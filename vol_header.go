package bakemono

import (
	"bytes"
	"encoding/binary"
	"errors"
)

type VolHeaderFooter struct {
	Magic          uint32
	CreateUnixTime int64
	WritePos       Offset
	SyncSerial     uint64
	WriteSerial    uint64
}

func (v *VolHeaderFooter) MarshalBinary() (data []byte, err error) {
	buf := &bytes.Buffer{}
	v.Magic = MagicBocchi
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
	return nil
}
