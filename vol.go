package bakemono

import (
	"encoding/binary"
	"errors"
	"log"
	"os"
	"time"
)

type offset uint64
type segId uint64

//const (
//	CacheBlockShift = 9
//	CacheBlockSize  = 1 << CacheBlockShift
//)

const (
	MagicBocchi = 0x000b0cc1

	DirDepth = 4

	MaxSegmentSize = 1 << 16 / DirDepth
)

var (
	HeaderSize = binary.Size(&VolHeaderFooter{})
	DirSize    = binary.Size(&Dir{})
)

// Vol is a volume represents a file on disk.
// structure: Meta_A(header, dirs, footer) + Meta_B(header, dirs, footer) + Data(Chunks)
// dirs are organized segment->bucket->dir logically.
type Vol struct {
	Path string
	Fp   OffsetReaderWriterCloser

	Header *VolHeaderFooter
	// map segment id to dirs
	Dirs         map[segId][]*Dir
	DirFreeStart map[segId]uint16

	SectorSize uint32

	Length               offset
	ChunkSize            offset
	ChunksNum            offset
	SegmentsNum          offset
	BucketsNum           offset
	BucketsNumPerSegment offset

	HeaderAOffset offset
	FooterAOffset offset
	HeaderBOffset offset
	FooterBOffset offset
	DataOffset    offset
}

// VolOptions to init a Vol.
// Note: do file open/truncate outside.
type VolOptions struct {
	Fp        OffsetReaderWriterCloser
	FileSize  offset
	ChunkSize offset
}

// NewVolOptionsWithFileTruncate creates a VolOptions with a file path.
// Note: It will create a file if not exists, and truncate it to the given size.
func NewVolOptionsWithFileTruncate(path string, fileSize, chunkSize uint64) (*VolOptions, error) {
	log.Printf("creating vol options with file truncate, path: %s, fileSize: %d, chunkSize: %d", path, fileSize, chunkSize)
	fp, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	log.Printf("file opened, try to truncate to size: %d", fileSize)
	err = fp.Truncate(int64(fileSize))
	if err != nil {
		return nil, err
	}
	return &VolOptions{
		Fp:        fp,
		FileSize:  offset(fileSize),
		ChunkSize: offset(chunkSize),
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
		return errors.New("invalid config: ChunkSize is 0")
	}
	return nil
}

func (v *Vol) Init(cfg *VolOptions) (corrupted bool, err error) {
	log.Printf("initing vol, config: %+v", cfg)
	err = cfg.Check()
	if err != nil {
		return false, err
	}

	// storage interface
	v.Fp = cfg.Fp
	v.ChunkSize = cfg.ChunkSize

	// calculate vol offsets
	v.prepareOffsets(cfg)

	// validate disk file
	err = v.validateFp()
	if err != nil {
		log.Printf("validate file failed, cache file may be corrupted or not initialized, err: %v", err)

		v.initEmptyMeta()
		corrupted = true

		err = v.flushMetaToFp()
		if err != nil {
			log.Printf("init file failed, err: %v", err)
			return corrupted, err
		}
	}

	// rebuild from meta
	err = v.buildMetaFromFp()

	log.Printf("init vol done, corrupted: %v, err: %v", corrupted, err)
	return corrupted, nil
}

// prepareOffsets calculates offsets and block numbers before initing a Vol.
func (v *Vol) prepareOffsets(cfg *VolOptions) {
	v.SectorSize = 512

	// calculate size to allocate
	// Meta_A(header, dirs, footer) + Meta_B(header, dirs, footer) + Data(Chunks)
	HeaderFooterSize := offset(HeaderSize)
	DirSize := offset(binary.Size(&Dir{}))
	TotalChunks := (cfg.FileSize - 4*HeaderFooterSize) / (cfg.ChunkSize + 2*DirSize)
	MetaSize := 2 * (HeaderFooterSize + TotalChunks*DirSize)
	DataSize := TotalChunks * cfg.ChunkSize
	ActualFileSize := MetaSize + DataSize
	log.Printf("initing vol: TotalChunks: %d, MetaSize: %d, DataSize: %d, ActualFileSize: %d", TotalChunks, MetaSize, DataSize, ActualFileSize)

	// calculate offsets
	v.HeaderAOffset = 0
	v.FooterAOffset = HeaderFooterSize + TotalChunks*DirSize
	v.HeaderBOffset = v.FooterAOffset + HeaderFooterSize
	v.FooterBOffset = v.HeaderBOffset + HeaderFooterSize + TotalChunks*DirSize
	v.DataOffset = v.FooterBOffset + HeaderFooterSize

	// calculate block number
	v.Length = ActualFileSize
	v.ChunksNum = TotalChunks
	v.BucketsNum = TotalChunks / DirDepth
	v.SegmentsNum = (v.BucketsNum + MaxSegmentSize - 1) / MaxSegmentSize
	v.BucketsNumPerSegment = (v.BucketsNum + v.SegmentsNum - 1) / v.SegmentsNum
	log.Printf("initing vol: ActualLength: %d, ChunksNum: %d, BucketsNum: %d, SegmentsNum: %d, BucketsNumPerSegment: %d", v.Length, v.ChunksNum, v.BucketsNum, v.SegmentsNum, v.BucketsNumPerSegment)
}

// validateFp validates the io with metadata.
func (v *Vol) validateFp() error {
	// read header
	offsets := []offset{v.HeaderAOffset, v.FooterAOffset, v.HeaderBOffset, v.FooterBOffset}
	var headFooters []*VolHeaderFooter
	size := HeaderSize
	for _, off := range offsets {
		hf := &VolHeaderFooter{}
		data := make([]byte, size)
		_, err := v.Fp.ReadAt(data, int64(off))
		if err != nil {
			return err
		}
		err = hf.UnmarshalBinary(data)
		if err != nil {
			return err
		}
		headFooters = append(headFooters, hf)
	}

	// check magic
	for _, hf := range headFooters {
		if hf.Magic != MagicBocchi {
			return errors.New("invalid magic")
		}
	}
	return nil
}

// buildMetaFromFp builds new empty metadata.
func (v *Vol) initEmptyMeta() {
	v.Header = &VolHeaderFooter{
		Magic:          MagicBocchi,
		CreateUnixTime: time.Now().Unix(),
		WritePos:       v.DataOffset,
		SyncSerial:     0,
		WriteSerial:    0,
	}

	// init dirs
	v.Dirs = make(map[segId][]*Dir)
	v.DirFreeStart = make(map[segId]uint16)

	for seg := 0; seg < int(v.SegmentsNum); seg++ {
		ChunkNumPerSegment := v.BucketsNumPerSegment * DirDepth
		dirs := make([]*Dir, ChunkNumPerSegment)

		// first free chunk for conclusion
		v.DirFreeStart[segId(seg)] = 1

		// init all dirs as empty
		for i := 0; i < len(dirs); i++ {
			dirs[i] = &Dir{}
		}

		// link dirs with next chain
		for bucket := 0; bucket < int(v.BucketsNumPerSegment); bucket++ {
			for depth := 1; depth < DirDepth-1; depth++ {
				offset := bucket*DirDepth + depth
				dirs[offset].setNext(uint16(offset + 1))
			}
			if bucket != int(v.BucketsNumPerSegment)-1 {
				offset := bucket*DirDepth + DirDepth - 1
				dirs[offset].setNext(uint16(offset + 2))
			}
		}

		// link dirs with prev chain
		for bucket := 0; bucket < int(v.BucketsNumPerSegment); bucket++ {
			for depth := DirDepth - 1; depth > 1; depth-- {
				offset := bucket*DirDepth + depth
				dirs[offset].setPrev(uint16(offset - 1))
			}
			if bucket != 0 {
				offset := bucket*DirDepth + 1
				// first bucket - chunk 1 has no prev
				if offset != 1 {
					dirs[offset].setPrev(uint16(offset - 2))
				}
			}
		}
		v.Dirs[segId(seg)] = dirs
	}
}

// TODO(meta): meta checksum, meta version
// TODO(flush): flush to A/B alternately

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

	// read dirs
	v.Dirs = make(map[segId][]*Dir)
	v.DirFreeStart = make(map[segId]uint16)
	for seg := 0; seg < int(v.SegmentsNum); seg++ {
		dirs := make([]*Dir, v.BucketsNumPerSegment*DirDepth)
		for bucket := 0; bucket < int(v.BucketsNumPerSegment); bucket++ {
			for depth := 0; depth < DirDepth; depth++ {
				dirIndex := bucket*DirDepth + depth
				dirs[dirIndex] = &Dir{}
				data := make([]byte, binary.Size(dirs[dirIndex]))

				pos := v.HeaderAOffset + offset(HeaderSize) + offset(dirIndex*DirSize)
				_, err := v.Fp.ReadAt(data, int64(pos))
				if err != nil {
					return err
				}
				err = dirs[dirIndex].UnmarshalBinary(data)
				if err != nil {
					return err
				}
			}
		}
		v.Dirs[segId(seg)] = dirs
		// TODO: dump free start
		//v.DirFreeStart[segId(seg)] = uint16(seg)*uint16(v.BucketsNumPerSegment*DirDepth) + 1
	}
	return nil
}

// flushMetaToFp flushes metadata to io.
func (v *Vol) flushMetaToFp() error {
	err := v.flushHeaderFooterToFp()
	if err != nil {
		return err
	}
	err = v.flushDirsToFp()
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
	offsets := []offset{v.HeaderAOffset, v.FooterAOffset, v.HeaderBOffset, v.FooterBOffset}
	for _, off := range offsets {
		_, err = v.Fp.WriteAt(data, int64(off))
		if err != nil {
			return err
		}
	}
	return nil
}

func (v *Vol) flushDirsToFp() error {
	for seg := 0; seg < int(v.SegmentsNum); seg++ {
		dirs := v.Dirs[segId(seg)]
		for i := 0; i < len(dirs); i++ {
			data, err := dirs[i].MarshalBinary()
			if err != nil {
				return err
			}
			off := v.HeaderAOffset + offset(HeaderSize) + offset(i*DirSize)
			_, err = v.Fp.WriteAt(data, int64(off))
			if err != nil {
				return err
			}
		}
	}
	return nil
}
