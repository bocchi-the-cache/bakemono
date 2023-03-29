package bakemono

import (
	"bytes"
	"encoding/binary"
	"os"
)

type offset uint64

//const (
//	CacheBlockShift = 9
//	CacheBlockSize  = 1 << CacheBlockShift
//)

type Vol struct {
	Path   string
	Fp     *os.File
	Header *VolHeaderFooter
	//Footer *VolHeaderFooter

	SectorSize uint32

	Length        offset
	SegmentsNum   offset
	BucketsNum    offset
	HeaderOffset  offset
	DataOffset    offset
	DataBlocksNum offset
}

type VolHeaderFooter struct {
	Magic         uint32
	CreatUnixTime uint64
	WritePos      offset
	SyncSerial    uint64
	WriteSerial   uint64
}

func (v *VolHeaderFooter) MarshalBinary() (data []byte, err error) {
	buf := &bytes.Buffer{}
	err = binary.Write(buf, binary.BigEndian, *v)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (v *VolHeaderFooter) UnmarshalBinary(data []byte) error {
	buf := bytes.NewBuffer(data)
	err := binary.Read(buf, binary.BigEndian, v)
	if err != nil {
		return err
	}
	return nil
}
