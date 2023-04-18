package bakemono

import "log"

const MaxKeyLength = 4096

func (v *Vol) Set(key, value []byte) (err error) {
	log.Printf("DEBUG: set key: %s, value_len: %d", key, len(value))
	err = v.checkSetRequest(key, value)
	if err != nil {
		return err
	}

	// could set offset into dir, but we use dirOffset->chunkOffset for now
	dirOffset, err := v.Dm.Set(key, Offset(1), len(value))

	log.Printf("DEBUG: write data")
	// write data
	writeOffset := uint64(dirOffset * v.ChunkSize)

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
	if Offset(len(value)) > v.ChunkSize {
		return ErrChunkDataTooLarge
	}
	return nil
}

func (v *Vol) Get(key []byte) (hit bool, value []byte, err error) {
	log.Printf("DEBUG: get key: %s", key)
	err = v.checkGetRequest(key)
	if err != nil {
		return false, nil, err
	}

	hit, dirOffset, d := v.Dm.Get(key)

	if !hit {
		return false, nil, nil
	}

	// read data
	readOffset := uint64(dirOffset * v.ChunkSize)
	approxSize := d.approxSize()

	ck := &Chunk{}
	err = ck.ReadAt(v.Fp, int64(readOffset), int64(approxSize))
	if err != nil {
		return false, nil, err
	}
	return true, ck.DataRaw, nil
}

func (v *Vol) checkGetRequest(key []byte) (err error) {
	if len(key) > MaxKeyLength {
		return ErrChunkKeyTooLarge
	}
	return nil
}
