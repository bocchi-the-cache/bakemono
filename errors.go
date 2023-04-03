package bakemono

import "errors"

var ErrChunkVerifyFailed = errors.New("chunk verify failed")
var ErrChunkDataTooLarge = errors.New("chunk data too large")
var ErrChunkKeyTooLarge = errors.New("chunk key too large")

var ErrVolFileCorrupted = errors.New("vol file corrupted")
