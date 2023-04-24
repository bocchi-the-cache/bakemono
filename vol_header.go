package bakemono

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

type VolHeaderFooter struct {
	Magic          uint32
	CreateUnixTime int64
	WritePos       Offset
	MajorVersion   uint32
	MinorVersion   uint32
	SyncSerial     uint64
	DirsChecksum   uint32

	Checksum uint32
}

func (v *VolHeaderFooter) GenerateChecksum() uint32 {
	return crc32.ChecksumIEEE([]byte(fmt.Sprintf("%v,%v,%v,%v,%v,%v", v.Magic, v.CreateUnixTime, v.WritePos, v.MajorVersion, v.MinorVersion, v.SyncSerial)))
}

func (v *VolHeaderFooter) MarshalBinary() (data []byte, err error) {
	buf := &bytes.Buffer{}
	v.Magic = MagicBocchi
	v.Checksum = v.GenerateChecksum()

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
	if v.Magic != MagicBocchi {
		return errors.New("invalid magic")
	}
	if v.Checksum != v.GenerateChecksum() {
		return errors.New("invalid checksum")
	}
	return nil
}
