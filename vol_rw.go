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
	keyInt12, segmentId, bucketId := calcDirHashPosition(key, v.SegmentsNum, v.BucketsNumPerSegment)

	// find if key exists. if exists, delete it.
	hit, dirOffset := v.dirProbe(keyInt12, segmentId, bucketId)
	if hit {
		v.dirClear(segmentId, dirOffset)
	} else {
		freeDirOffset := v.dirFindBucketNextFree(segmentId, bucketId)
		v.dirSetBucketNext(segmentId, bucketId, freeDirOffset)
		dirOffset = freeDirOffset
	}

	// setup dir
	v.Dirs[segmentId][dirOffset].setOffset(uint64(dirOffset * v.ChunkSize))
	v.Dirs[segmentId][dirOffset].setTag(keyInt12)
	v.Dirs[segmentId][dirOffset].setSize(uint8(len(value)))
	v.Dirs[segmentId][dirOffset].setApproxSize(uint64(len(value)))
	v.Dirs[segmentId][dirOffset].setHead(true)

	// write data
	writeOffset := getChunkDataOffset(v.DataOffset, segmentId, DirDepth,
		v.BucketsNumPerSegment, dirOffset, v.ChunkSize)

	ck := &Chunk{}
	err = ck.Set(key, value)
	if err != nil {
		return err
	}
	err = ck.WriteAt(v.Fp, int64(writeOffset))
	if err != nil {
		return err
	}
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
	err = v.checkGetRequest(key)
	if err != nil {
		return nil, err
	}

	keyInt12, segmentId, bucketId := calcDirHashPosition(key, v.SegmentsNum, v.BucketsNumPerSegment)

	// find if key exists. if exists, read it.
	hit, dirOffset := v.dirProbe(keyInt12, segmentId, bucketId)
	if !hit {
		return nil, ErrCacheMiss
	}

	// read data
	readOffset := getChunkDataOffset(v.DataOffset, segmentId, DirDepth,
		v.BucketsNumPerSegment, dirOffset, v.ChunkSize)
	approxSize := v.Dirs[segmentId][dirOffset].approxSize()
	ck := &Chunk{}
	err = ck.ReadAt(v.Fp, int64(readOffset), int64(approxSize))
	if err != nil {
		// data may be corrupted
		return nil, ErrCacheMiss
	}
	return ck.DataRaw, nil
}

func (v *Vol) checkGetRequest(key []byte) (err error) {
	if len(key) > MaxKeyLength {
		return ErrChunkKeyTooLarge
	}
	return nil
}

func calcDirHashPosition(key []byte, SegmentsNum, BucketsNumPerSegment offset) (keyInt12 uint16, segmentId segId, bucketId offset) {
	h := md5.New()
	h.Write(key)
	keyHashed := h.Sum(nil)
	keyInt64 := binary.BigEndian.Uint64(keyHashed)
	keyInt12 = uint16(keyInt64 >> 52) // use high 12bit of md5 hash as keyInt16
	// use high 32bit of md5 hash as segment id, low 32bit as bucket id
	segmentId = segId(keyInt64>>32) % segId(SegmentsNum)
	bucketId = offset(keyInt64&0xffffffff) % (BucketsNumPerSegment)
	return keyInt12, segmentId, bucketId
}

func getChunkDataOffset(segDataStartOffset offset, sid segId, depth, BucketsNumPerSegment, dirOffset, chunkSize offset) offset {
	return segDataStartOffset +
		offset(sid)*depth*BucketsNumPerSegment +
		dirOffset*chunkSize
}
