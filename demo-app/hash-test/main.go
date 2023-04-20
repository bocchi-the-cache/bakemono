package main

import (
	"crypto/md5"
	"encoding/binary"
	"log"
)

func main() {
	strs := []string{"key-32523-16483", "key-32523-26"}
	for _, str := range strs {
		h := md5.New()
		h.Write([]byte(str))
		keyHashed := h.Sum(nil)
		keyInt64 := binary.BigEndian.Uint64(keyHashed)
		keyInt12 := uint16(keyInt64 >> 52) // u
		// print keyInt12 in hex
		log.Printf("keyInt12: %x", keyInt12)
	}
}
