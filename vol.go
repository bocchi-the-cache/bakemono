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

// Vol is a volume represents a file on disk.
// structure: Meta_A(header, dirs, footer) + Meta_B(header, dirs, footer) + Data(Chunks)
// dirs are organized segment->bucket->dir logically.
type Vol struct {
	Path string
	Fp   *os.File

	Header *VolHeaderFooter
	// map segment id to dirs
	Dirs         map[segId][]*Dir
	DirFreeStart map[segId]uint16

	SectorSize uint32

	Length               offset
	ChunksNum            offset
	SegmentsNum          offset
	BucketsNum           offset
	BucketsNumPerSegment offset

	HeaderAOffset offset
	HeaderBOffset offset
	FooterAOffset offset
	FooterBOffset offset
	DataOffset    offset
}

type VolConfig struct {
	Path      string
	FileSize  offset
	ChunkSize offset
}

func (cfg *VolConfig) Check() error {
	if cfg.Path == "" {
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

func (v *Vol) Init(cfg *VolConfig) error {
	log.Printf("initing vol, config: %+v", cfg)
	err := cfg.Check()
	if err != nil {
		return err
	}
	v.SectorSize = 512

	// calculate size to allocate
	// Meta_A(header, dirs, footer) + Meta_B(header, dirs, footer) + Data(Chunks)
	HeaderFooterSize := offset(binary.Size(&VolHeaderFooter{}))
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

	v.Length = ActualFileSize
	v.ChunksNum = TotalChunks
	v.BucketsNum = TotalChunks / DirDepth
	v.SegmentsNum = (v.BucketsNum + MaxSegmentSize - 1) / MaxSegmentSize
	v.BucketsNumPerSegment = (v.BucketsNum + v.SegmentsNum - 1) / v.SegmentsNum

	// open file
	v.Fp, err = os.OpenFile(cfg.Path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return err
	}

	// validate disk file
	err = v.validateFile()
	if err != nil {
		log.Printf("validate file failed, cache file may be corrupted or not initialized, err: %v", err)

		v.initEmptyMeta()
		err = v.initFile()
		if err != nil {
			log.Printf("init file failed, err: %v", err)
			return err
		}
	}

	// rebuild from meta
	err = v.buildMetaFromFile()
	return nil
}

func (v *Vol) validateFile() error {
	// read header
	offsets := []offset{v.HeaderAOffset, v.FooterAOffset, v.HeaderBOffset, v.FooterBOffset}
	var headFooters []*VolHeaderFooter
	size := binary.Size(&VolHeaderFooter{})
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

func (v *Vol) initFile() error {
	headerBinary, err := v.Header.MarshalBinary()
	offsets := []offset{v.HeaderAOffset, v.FooterAOffset, v.HeaderBOffset, v.FooterBOffset}
	for _, off := range offsets {
		_, err = v.Fp.WriteAt(headerBinary, int64(off))
		if err != nil {
			return err
		}
	}
	return nil
}

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

	for seg := 0; seg < int(v.SegmentsNum); seg++ {
		ChunkNumPerSegment := v.BucketsNumPerSegment * DirDepth
		dirs := make([]*Dir, ChunkNumPerSegment)

		// first free chunk for conclusion
		v.DirFreeStart[segId(seg)] = uint16(seg)*uint16(ChunkNumPerSegment) + 1

		// init all dirs as empty
		for i := 0; i < len(dirs); i++ {
			dirs[i] = &Dir{}
		}

		// link dirs with chain
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
	}

}
func (v *Vol) buildMetaFromFile() error {
	h := &VolHeaderFooter{}
	data := make([]byte, binary.Size(&VolHeaderFooter{}))
	_, err := v.Fp.ReadAt(data, int64(v.HeaderAOffset))
	if err != nil {
		return err
	}
	err = h.UnmarshalBinary(data)
	if err != nil {
		return err
	}
	v.Header = h
	return nil
}

func (v *Vol) flushMetaToFile() error {
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
