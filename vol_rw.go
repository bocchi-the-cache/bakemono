package bakemono

import "log"

const MaxKeyLength = 4096

func (v *Vol) Set(key, value []byte) (err error) {
	//log.Printf("DEBUG: set key: %s, value_len: %d", key, len(value))
	err = v.checkSetRequest(key, value)
	if err != nil {
		return err
	}

	// make data chunk
	ck := &Chunk{}
	err = ck.Set(key, value)
	if err != nil {
		return err
	}

	// process data write position
	binLenOnDisk := ck.GetBinaryLength()
	if v.WritePos+binLenOnDisk > v.Length {
		log.Printf("data write overflowed, start from dataOffset. set: writePos: %d, dataOffset: %d, len(value): %d", v.WritePos, v.DataOffset, len(value))
		v.WritePos = v.DataOffset
	}
	writeOffset := v.WritePos
	v.WritePos += binLenOnDisk

	// set dir
	_, err = v.Dm.Set(key, writeOffset, int(binLenOnDisk))

	// write to disk
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
	//log.Printf("DEBUG: get key: %s", key)
	err = v.checkGetRequest(key)
	if err != nil {
		return false, nil, err
	}

	hit, _, d := v.Dm.Get(key)

	if !hit {
		return false, nil, nil
	}

	// read data
	readOffset := d.offset()
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
