package bakemono

import (
	"crypto/md5"
	"encoding/binary"
)

const MaxKeyLength = 4096

func (v *Vol) Set(key, value []byte) (err error) {
	err = v.checkSetRequest(key, value)
	if err != nil {
		return err
	}

	// TODO: if hash collision using md5
	h := md5.New()
	keyHashed := h.Sum(key)
	keyInt64 := binary.BigEndian.Uint64(keyHashed)
	keyInt16 := uint16(keyInt64 >> 48)
	// use high 32bit of md5 hash as segment id, low 32bit as bucket id
	segmentId := segId(keyInt64>>32) % segId(v.SegmentsNum)
	bucketId := offset(keyInt64&0xffffffff) % (v.BucketsNumPerSegment)

	// find if key exists. if exists, delete it.
	hit, dirOffset := v.dirProbe(keyInt16, segmentId, bucketId)
	if hit {
		v.dirClear(segmentId, dirOffset)
	}

	// setup dir
	v.Dirs[segmentId][dirOffset].setOffset(uint64(dirOffset * v.ChunkSize))
	v.Dirs[segmentId][dirOffset].setTag(keyInt16)
	v.Dirs[segmentId][dirOffset].setSize(uint8(len(value)))

	return nil
}

func (v *Vol) checkSetRequest(key, value []byte) (err error) {
	if len(key) > MaxKeyLength {
		return ErrChunkKeyTooLarge
	}
	if offset(len(value)) > v.ChunkSize {
		return ErrChunkDataTooLarge
	}
	return nil
}

func (v *Vol) Get(key []byte) (value []byte, err error) {
	return nil, nil
}
