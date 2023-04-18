package bakemono

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// Dir is index unit in cache vol. Inspired by Traffic Server.
// use raw array to reduce memory allocation, especially for bare metal > 100TB.
// raw data format

type Dir struct {
	/*
	   unsigned int offset : 24;  // (0,1:0-7)
	   unsigned int bigInternal : 2;      // (1:8-9)
	   unsigned int sizeInternal : 6;     // (1:10-15)
	   unsigned int tag : 12;     // (2:0-11)
	   unsigned int phase : 1;    // (2:12)
	   unsigned int head : 1;     // (2:13)
	   unsigned int pinned : 1;   // (2:14)
	   unsigned int token : 1;    // (2:15)
	   unsigned int next : 16;    // (3)
	   unsigned int offset_high : 16;

	   if unused, raw[2] is `prev`, represents previous dir in freelist.

	   approx_size = sectorSize(512) * (2**3big) * sizeInternal
	*/
	raw [5]uint16

	// TODO: data range guard for every field
}

func (d *Dir) clear() {
	d.raw[0] = 0
	d.raw[1] = 0
	d.raw[2] = 0
	d.raw[3] = 0
	d.raw[4] = 0
}

func (d *Dir) MarshalBinary() ([]byte, error) {
	buf := &bytes.Buffer{}
	err := binary.Write(buf, binary.BigEndian, d.raw)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (d *Dir) UnmarshalBinary(data []byte) error {
	buf := bytes.NewBuffer(data)
	err := binary.Read(buf, binary.BigEndian, &d.raw)
	if err != nil {
		return err
	}
	return nil
}

func (d *Dir) offset() uint64 {
	return uint64(d.raw[0]) | uint64(d.raw[1]&0xff)<<16 | uint64(d.raw[4])<<24
}

func (d *Dir) setOffset(offset uint64) {
	d.raw[0] = uint16(offset)
	d.raw[1] = uint16(((offset >> 16) & 0xff) | (uint64(d.raw[1]) & 0xff00))
	d.raw[4] = uint16(offset >> 24)
}

func (d *Dir) setApproxSize(size uint64) {
	// Note: max sizeInternal 16MB
	if size > DirMaxDataSize {
		panic(fmt.Sprintf("dir setApproxSize %d is too bigInternal", size))
	}
	if size <= DirDataSizeLv0 {
		d.setBigInternal(0)
		d.setSizeInternal(uint8((size - 1) / DirDataSizeLv0))
	} else if size <= DirDataSizeLv1 {
		d.setBigInternal(1)
		d.setSizeInternal(uint8((size - 1) / DirDataSizeLv1))
	} else if size <= DirDataSizeLv2 {
		d.setBigInternal(2)
		d.setSizeInternal(uint8((size - 1) / DirDataSizeLv2))
	} else {
		d.setBigInternal(3)
		d.setSizeInternal(uint8((size - 1) / DirDataSizeLv3))
	}
}

func (d *Dir) approxSize() uint64 {
	big := d.bigInternal()
	size := d.sizeInternal()
	return (SectorSize << (big * 3)) * uint64(size+1)
}

func (d *Dir) prev() uint16 {
	return d.raw[2]
}

func (d *Dir) setPrev(prev uint16) {
	d.raw[2] = prev
}

func (d *Dir) bigInternal() uint8 {
	return uint8(d.raw[1] >> 8 & 0x3)
}

func (d *Dir) setBigInternal(big uint8) {
	d.raw[1] = (d.raw[1] & 0xfcff) | uint16(big)<<8
}

func (d *Dir) sizeInternal() uint8 {
	return uint8(d.raw[1] >> 10 & 0x3f)
}

func (d *Dir) setSizeInternal(size uint8) {
	d.raw[1] = (d.raw[1] & 0x83ff) | uint16(size)<<10
}

func (d *Dir) tag() uint16 {
	return d.raw[2] & 0xfff
}

func (d *Dir) setTag(tag uint16) {
	d.raw[2] = (d.raw[2] & 0xf000) | (tag & 0xfff)
}

func (d *Dir) phase() bool {
	return d.raw[2]>>12&0x1 == 1
}

func (d *Dir) setPhase(phase bool) {
	if phase {
		d.raw[2] |= 1 << 12
	} else {
		d.raw[2] &= 1 << 12
	}
}

func (d *Dir) head() bool {
	return d.raw[2]>>13&0x1 == 1
}

func (d *Dir) setHead(head bool) {
	if head {
		d.raw[2] |= 1 << 13
	} else {
		d.raw[2] &= 1 << 13
	}
}

func (d *Dir) pinned() bool {
	return d.raw[2]>>14&0x1 == 1
}

func (d *Dir) setPinned(pinned bool) {
	if pinned {
		d.raw[2] |= 1 << 14
	} else {
		d.raw[2] &= 1 << 14
	}
}

func (d *Dir) token() bool {
	return d.raw[2]>>15&0x1 == 1
}

func (d *Dir) setToken(token bool) {
	if token {
		d.raw[2] |= 1 << 15
	} else {
		d.raw[2] &= ^uint16(1 << 15)
	}
}

func (d *Dir) next() uint16 {
	return d.raw[3]
}

func (d *Dir) setNext(next uint16) {
	d.raw[3] = next
}
