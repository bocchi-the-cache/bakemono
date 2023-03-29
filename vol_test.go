package bakemono

import (
	"reflect"
	"testing"
)

func TestVolHeaderFooterMarshal(t *testing.T) {
	v := VolHeaderFooter{
		Magic:         0x12345678,
		CreatUnixTime: 0x1ab27df24eaf0924,
		WritePos:      0xdf241ab27e924af0,
		SyncSerial:    0xab2df2eaf0924417,
		WriteSerial:   0x78934ab01256cdef,
	}
	b, err := v.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	t.Log(string(b))
}

func TestVolHeaderFooterUnmarshal(t *testing.T) {
	v := VolHeaderFooter{
		Magic:         0x12345678,
		CreatUnixTime: 0x1ab27df24eaf0924,
		WritePos:      0xdf241ab27e924af0,
		SyncSerial:    0xab2df2eaf0924417,
		WriteSerial:   0x78934ab01256cdef,
	}
	b, err := v.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	t.Log(string(b))
	var v2 VolHeaderFooter
	err = v2.UnmarshalBinary(b)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(v2)
	// check if v2 is equal using reflect.DeepEqual
	if !reflect.DeepEqual(v, v2) {
		t.Fatal("v2 is not equal to v")
	}
	t.Log("v2 is equal to v")
}
