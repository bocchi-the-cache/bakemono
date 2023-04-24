package bakemono

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"log"
	"os"
	"time"
)

type Offset uint64
type segId uint64

// TODO(meta): flush/rebuild to A/B alternately

var (
	HeaderSize = binary.Size(&VolHeaderFooter{})
	DirSize    = binary.Size(&Dir{})
)

// Vol is a volume represents a file on disk.
// structure: Meta_A(header, dirs, footer) + Meta_B(header, dirs, footer) + Data(Chunks)
// dirs are organized segment->bucket->dir logically.
type Vol struct {
	Path     string
	Fp       OffsetReaderWriterCloser
	Dm       *DirManager
	WritePos Offset

	Header *VolHeaderFooter

	SectorSize   uint32
	Length       Offset
	ChunkAvgSize Offset // average chunk size, adjusted by user.
	ChunksMaxNum Offset // max chunks num in this vol. calculated from ChunkAvgSize and Length

	HeaderAOffset Offset
	FooterAOffset Offset
	HeaderBOffset Offset
	FooterBOffset Offset
	DataOffset    Offset
	DirAOffset    Offset

	closeCh chan struct{}
	flushCh chan struct{}
}

// VolOptions to init a Vol.
// Note: do file open/truncate outside.
type VolOptions struct {
	Fp        OffsetReaderWriterCloser
	FileSize  Offset
	ChunkSize Offset

	FlushMetaInterval time.Duration
}

// NewVolOptionsWithFileTruncate creates a VolOptions with a file path.
// Note: It will create a file if not exists, and truncate it to the given sizeInternal.
func NewVolOptionsWithFileTruncate(path string, fileSize, chunkSize uint64) (*VolOptions, error) {
	log.Printf("creating vol options with file truncate, path: %s, fileSize: %d, chunkSize: %d", path, fileSize, chunkSize)
	fp, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	log.Printf("file opened, try to truncate to sizeInternal: %d", fileSize)
	err = fp.Truncate(int64(fileSize))
	if err != nil {
		return nil, err
	}
	return &VolOptions{
		Fp:                fp,
		FileSize:          Offset(fileSize),
		ChunkSize:         Offset(chunkSize),
		FlushMetaInterval: 60 * time.Second,
	}, nil
}

// Check checks if the VolOptions is valid.
func (cfg *VolOptions) Check() error {
	if cfg.Fp == nil {
		return errors.New("invalid config: Fp is nil")
	}
	if cfg.FileSize == 0 {
		return errors.New("invalid config: FileSize is 0")
	}
	if cfg.ChunkSize == 0 {
		return errors.New("invalid config: ChunkAvgSize is 0")
	}
	return nil
}

func (v *Vol) Init(cfg *VolOptions) (corrupted bool, err error) {
	log.Printf("initing vol, config: %+v", cfg)
	err = cfg.Check()
	if err != nil {
		return false, err
	}

	// channel init
	v.closeCh = make(chan struct{})
	v.flushCh = make(chan struct{})

	// storage interface
	v.Fp = cfg.Fp

	// dir manager size init. note: dir data setup in next step
	v.Dm = &DirManager{}
	expectedDirNum := (cfg.FileSize - 4*Offset(HeaderSize)) / (cfg.ChunkSize + 2*Offset(DirSize))
	v.ChunksMaxNum = v.Dm.Init(expectedDirNum)

	// calculate vol offsets
	v.prepareOffsets(cfg)

	err = v.buildMetaFromFp()
	if err != nil {
		log.Printf("warn: build meta from fp failed, file may corrupted, err: %v", err)
		corrupted = true
		v.initEmptyMeta()
	}

	// sync meta to vol, avoid mutex for header
	v.WritePos = v.DataOffset

	// start sync flush thread
	go v.SyncFlushLoop(cfg.FlushMetaInterval)

	log.Printf("init vol done, corrupted: %v, err: %v", corrupted, err)
	return corrupted, nil
}

// Close closes the Vol.
func (v *Vol) Close() error {
	close(v.closeCh)
	<-v.flushCh
	return v.Fp.Close()
}

// SyncFlushLoop flushes metadata to disk periodically.
func (v *Vol) SyncFlushLoop(interval time.Duration) {
	for {
		select {
		case <-v.closeCh:
			close(v.flushCh)
			return
		case <-time.After(interval):
			err := v.flushMetaToFp()
			if err != nil {
				log.Printf("error: flush meta to fp failed, err: %v", err)
			}
		}
	}
}

// prepareOffsets calculates offsets and block numbers before initing a Vol.
func (v *Vol) prepareOffsets(cfg *VolOptions) {
	v.ChunkAvgSize = cfg.ChunkSize
	v.SectorSize = 512
	v.Length = cfg.FileSize

	// calculate sizeInternal to allocate
	// Meta_A(header, dirs, footer) + Meta_B(header, dirs, footer) + Data(Chunks)
	HeaderFooterSize := Offset(HeaderSize)
	DirSize := Offset(binary.Size(&Dir{}))
	// TotalChunk init by DirManager
	//TotalChunks := (cfg.FileSize - 4*HeaderFooterSize) / (cfg.ChunkAvgSize + 2*DirSize)
	MetaSize := 2 * (2*HeaderFooterSize + v.ChunksMaxNum*DirSize)
	DataSize := cfg.FileSize - MetaSize
	log.Printf("initing vol: ChunksMaxNum: %d, MetaSize: %d, DataSize: %d, VolLength: %d", v.ChunksMaxNum, MetaSize, DataSize, v.Length)

	// calculate offsets
	v.HeaderAOffset = 0
	v.FooterAOffset = HeaderFooterSize + v.ChunksMaxNum*DirSize
	v.HeaderBOffset = v.FooterAOffset + HeaderFooterSize
	v.FooterBOffset = v.HeaderBOffset + HeaderFooterSize + v.ChunksMaxNum*DirSize
	v.DataOffset = MetaSize
	v.DirAOffset = v.HeaderAOffset + HeaderFooterSize

	log.Printf("initing vol: ActualLength: %d, ChunksMaxNum: %d", v.Length, v.ChunksMaxNum)
}

// buildMetaFromFp builds new empty metadata.
func (v *Vol) initEmptyMeta() {
	v.Header = &VolHeaderFooter{
		Magic:          MagicBocchi,
		CreateUnixTime: time.Now().Unix(),
		WritePos:       v.DataOffset,
		SyncSerial:     0,
		//WriteSerial:    0,
	}

	v.Dm.InitEmptyDirs()
}

// buildMetaFromFp builds metadata from io.
func (v *Vol) buildMetaFromFp() error {
	h := &VolHeaderFooter{}
	data := make([]byte, HeaderSize)
	_, err := v.Fp.ReadAt(data, int64(v.HeaderAOffset))
	if err != nil {
		return err
	}
	err = h.UnmarshalBinary(data)
	if err != nil {
		return err
	}
	v.Header = h

	DirSize := Offset(binary.Size(&Dir{})) * v.ChunksMaxNum
	dirsRaw := make([]byte, DirSize)
	_, err = v.Fp.ReadAt(dirsRaw, int64(v.DirAOffset))
	if err != nil {
		return err
	}

	DirsCheckSum := crc32.ChecksumIEEE(dirsRaw)
	log.Printf("DirsCheckSum: %d, v.Header.DirsChecksum: %d", DirsCheckSum, v.Header.DirsChecksum)
	if DirsCheckSum != v.Header.DirsChecksum {
		return errors.New("invalid dir checksum")
	}

	err = v.Dm.UnmarshalBinary(dirsRaw)
	if err != nil {
		return err
	}

	return nil
}

// flushMetaToFp flushes metadata to io.
func (v *Vol) flushMetaToFp() error {
	v.Header.Magic = MagicBocchi
	v.Header.MajorVersion = MajorVersion
	v.Header.MinorVersion = MinorVersion
	v.Header.WritePos = v.WritePos
	v.Header.SyncSerial++
	dirsRaw, err := v.Dm.MarshalBinary()
	if err != nil {
		return err
	}
	v.Header.DirsChecksum = crc32.ChecksumIEEE(dirsRaw)

	err = v.flushHeaderFooterToFp()
	if err != nil {
		return err
	}
	err = v.flushDirRawToFp(dirsRaw)
	if err != nil {
		return err
	}
	return nil
}

func (v *Vol) flushHeaderFooterToFp() error {
	data, err := v.Header.MarshalBinary()
	if err != nil {
		return err
	}
	offsets := []Offset{v.HeaderAOffset, v.FooterAOffset, v.HeaderBOffset, v.FooterBOffset}
	for _, off := range offsets {
		_, err = v.Fp.WriteAt(data, int64(off))
		if err != nil {
			return err
		}
	}
	return nil
}

func (v *Vol) flushDirRawToFp(data []byte) error {
	// check if data size is correct
	DirSize := Offset(binary.Size(&Dir{})) * v.ChunksMaxNum
	if DirSize < Offset(len(data)) {
		return errors.New("invalid dir data size")
	}
	_, err := v.Fp.WriteAt(data, int64(v.DirAOffset))
	if err != nil {
		return err
	}
	return nil
}
