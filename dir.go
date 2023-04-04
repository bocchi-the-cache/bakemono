package bakemono

import (
	"bytes"
	"encoding/binary"
)

// Dir is index unit in cache vol. Inspired by Traffic Server.
// use raw array to reduce memory allocation, especially for bare metal > 100TB.
// raw data format

type Dir struct {
	/*
	   unsigned int offset : 24;  // (0,1:0-7)
	   unsigned int big : 2;      // (1:8-9)
	   unsigned int size : 6;     // (1:10-15)
	   unsigned int tag : 12;     // (2:0-11)
	   unsigned int phase : 1;    // (2:12)
	   unsigned int head : 1;     // (2:13)
	   unsigned int pinned : 1;   // (2:14)
	   unsigned int token : 1;    // (2:15)
	   unsigned int next : 16;    // (3)
	   unsigned int offset_high : 16;
	*/
	raw [5]uint16
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

func (d *Dir) big() uint8 {
	return uint8(d.raw[1] >> 8 & 0x3)
}

func (d *Dir) setBig(big uint8) {
	d.raw[1] = (d.raw[1] & 0xfcff) | uint16(big)<<8
}

func (d *Dir) size() uint8 {
	return uint8(d.raw[1] >> 10 & 0x3f)
}

func (d *Dir) setSize(size uint8) {
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
