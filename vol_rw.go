package bakemono

const MaxKeyLength = 4096

func (v *Vol) Set(key, value []byte) (err error) {
	err = v.CheckSetRequest(key, value)
	if err != nil {
		return err
	}
	//h := fnv.New64a()
	//keyHashed := h.Sum(key)
	//keyInt64 := binary.BigEndian.Uint64(keyHashed)
	// use high 32bit of md5 hash as segment id, low 32bit as bucket id
	//segmentId := offset(keyInt64>>32) % (v.SegmentsNum)
	//bucketId := offset(keyInt64&0xffffffff) % (v.BucketsNumPerSegment)

	// find if key exists
	return nil
}

func (v *Vol) CheckSetRequest(key, value []byte) (err error) {
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
