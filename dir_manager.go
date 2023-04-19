package bakemono

import (
	"crypto/md5"
	"encoding/binary"
	"errors"
	"log"
	"math/rand"
)

// DirManager manages the dirs attached to a vol.
type DirManager struct {
	ChunksNum            Offset
	SegmentsNum          Offset
	BucketsNum           Offset
	BucketsNumPerSegment Offset

	// map segment id to dirs
	Dirs         map[segId][]*Dir
	DirFreeStart map[segId]uint16
}

func (dm *DirManager) Init(dirNum Offset) {
	dm.Dirs = make(map[segId][]*Dir)
	dm.DirFreeStart = make(map[segId]uint16)

	dm.BucketsNum = dirNum / DirDepth
	dm.SegmentsNum = (dm.BucketsNum + MaxBucketsPerSegment - 1) / MaxBucketsPerSegment
	dm.BucketsNumPerSegment = (dm.BucketsNum + dm.SegmentsNum - 1) / dm.SegmentsNum

	dm.ChunksNum = dm.BucketsNumPerSegment * DirDepth * dm.SegmentsNum
	log.Printf("initing dir manager: ChunksNum: %d, BucketsNum: %d, SegmentsNum: %d, BucketsNumPerSegment: %d", dm.ChunksNum, dm.BucketsNum, dm.SegmentsNum, dm.BucketsNumPerSegment)
}

// TODO: serialize and deserialize from bytes

// InitEmptyDirs initializes all dirs as empty, make chain.
func (dm *DirManager) InitEmptyDirs() {
	for seg := 0; seg < int(dm.SegmentsNum); seg++ {
		ChunkNumPerSegment := dm.BucketsNumPerSegment * DirDepth
		dirs := make([]*Dir, ChunkNumPerSegment)

		// first free chunk for conclusion
		dm.DirFreeStart[segId(seg)] = 1

		// init all dirs as empty
		for i := 0; i < len(dirs); i++ {
			dirs[i] = &Dir{}
		}

		// link dirs with next chain
		err := linkEmptyDirs(dirs)
		if err != nil {
			// should not happen
			log.Fatal(err)
		}

		dm.Dirs[segId(seg)] = dirs
	}
}

func linkEmptyDirs(dirs []*Dir) error {

	if len(dirs)%DirDepth != 0 {
		return errors.New("dirs length should be multiple of DirDepth")
	}

	buckets := len(dirs) / DirDepth

	// link dirs with next chain
	for bucket := 0; bucket < buckets; bucket++ {
		for depth := 1; depth < DirDepth-1; depth++ {
			offset := bucket*DirDepth + depth
			dirs[offset].setNext(uint16(offset + 1))
		}
		if bucket != int(buckets)-1 {
			offset := bucket*DirDepth + DirDepth - 1
			dirs[offset].setNext(uint16(offset + 2))
		}
	}

	// link dirs with prev chain
	for bucket := 0; bucket < buckets; bucket++ {
		for depth := DirDepth - 1; depth > 1; depth-- {
			offset := bucket*DirDepth + depth
			dirs[offset].setPrev(uint16(offset - 1))
		}
		if bucket != 0 {
			offset := bucket*DirDepth + 1
			// first bucket - chunk 1 has no prev
			if offset != 1 {
				dirs[offset].setPrev(uint16(offset - 2))
			}
		}
	}
	return nil
}

// freeChainDelete deletes a free dir from the chain
func (dm *DirManager) freeChainDelete(segmentId segId, dirOffset Offset) {
	isFirst, next := freeChainDelete(dm.Dirs[segmentId], dirOffset)
	if isFirst {
		dm.DirFreeStart[segmentId] = next
	}
	return
}

func freeChainDelete(dirs []*Dir, dirOffset Offset) (isFirst bool, freeListHead uint16) {
	if dirs[dirOffset].offset() != 0 {
		// TODO: remove panic once stable
		panic("dir is not empty")
	}
	prev := dirs[dirOffset].prev()
	if prev == 0 {
		isFirst = true
		freeListHead = dirs[dirOffset].next()
	} else {
		dirs[prev].setNext(dirs[dirOffset].next())
	}

	next := dirs[dirOffset].next()
	if next != 0 {
		dirs[next].setPrev(prev)
	}
	return
}

// Get returns
// HIT: the offset of the dir entry with the given key,
// MISS: the offset of last dir entry in the bucket
func (dm *DirManager) Get(key []byte) (hit bool, dirOffset Offset, d Dir) {
	keyInt12, segmentId, bucketId := calcDirHashPosition(key, dm.SegmentsNum, dm.BucketsNumPerSegment)
	return dirProbe(keyInt12, bucketId, dm.Dirs[segmentId])
}

func calcDirHashPosition(key []byte, SegmentsNum, BucketsNumPerSegment Offset) (keyInt12 uint16, segmentId segId, bucketId Offset) {
	h := md5.New()
	h.Write(key)
	keyHashed := h.Sum(nil)
	keyInt64 := binary.BigEndian.Uint64(keyHashed)
	keyInt12 = uint16(keyInt64 >> 52) // use high 12bit of md5 hash as keyInt16
	// use high 32bit of md5 hash as segment id, low 32bit as bucket id
	segmentId = segId(keyInt64>>32) % segId(SegmentsNum)
	bucketId = Offset(keyInt64&0xffffffff) % (BucketsNumPerSegment)
	return keyInt12, segmentId, bucketId
}

func dirProbe(key uint16, bucketId Offset, dirs []*Dir) (hit bool, dirOffset Offset, d Dir) {
	index := bucketId * DirDepth
	counter := 0

	// do...while
	for index != 0 || counter == 0 {
		counter++
		if counter > 10000 {
			log.Printf("dirProbe: counter: %d, index: %d", counter, index)
			panic("dirProbe: counter>10000")
		}
		if dirs[index].offset() == 0 {
			return false, index, d
		}
		dirKey := dirs[index].tag()
		if dirKey == key {
			return true, index, *dirs[index]
		}
		index = Offset(dirs[index].next())
	}

	return false, index, d
}

func (dm *DirManager) Set(key []byte, off Offset, size int) (dirOffset Offset, err error) {
	keyInt12, segmentId, bucketId := calcDirHashPosition(key, dm.SegmentsNum, dm.BucketsNumPerSegment)

	dir := Dir{}
	dir.setOffset(uint64(off))
	dir.setApproxSize(uint64(size))
	dir.setHead(true)
	dir.setTag(keyInt12)

	offset, err := dm.dirInsert(keyInt12, segmentId, bucketId, dir)
	if err != nil {
		return offset, err
	}
	return dm.BucketsNumPerSegment*DirDepth*Offset(segmentId) + offset, nil
}

func (dm *DirManager) dirInsert(key uint16, segmentId segId, bucketId Offset, dir Dir) (dirOffset Offset, err error) {
	hit, dirOffset, dOld := dirProbe(key, bucketId, dm.Dirs[segmentId])
	if hit {
		// Note: set manually is dangerous, need to keep the next chain
		dOld.setOffset(dir.offset())
		dOld.setApproxSize(dir.approxSize())
		dOld.setHead(true)
		dOld.setTag(dir.tag())

		dm.Dirs[segmentId][dirOffset] = &dOld
		return dirOffset, nil
	}

	// get a free dir
	_, freeDirOffset := dm.getFreeDir(segmentId, bucketId)

	// set dir
	dm.Dirs[segmentId][freeDirOffset] = &dir

	// link dir
	if freeDirOffset != bucketId*DirDepth {
		lastDirOfBucket := bucketId * DirDepth
		for dm.Dirs[segmentId][lastDirOfBucket].next() != 0 {
			lastDirOfBucket = Offset(dm.Dirs[segmentId][lastDirOfBucket].next())
		}
		dm.Dirs[segmentId][lastDirOfBucket].setNext(uint16(freeDirOffset))
	}

	return freeDirOffset, nil
}

func (dm *DirManager) getFreeDir(segmentId segId, bucketId Offset) (isSameBucket bool, freeDirOffset Offset) {
	index := bucketId * DirDepth
	// head of bucket
	if dm.Dirs[segmentId][index].offset() == 0 {
		return true, index
	}
	// same bucket
	for i := 1; i < DirDepth; i++ {
		if dm.Dirs[segmentId][index+Offset(i)].offset() == 0 {
			dm.freeChainDelete(segmentId, index+Offset(i))
			return true, index + Offset(i)
		}
	}
	// pop a free dir from free chain
	return false, dm.freeChainPop(segmentId, bucketId)
}

func (dm *DirManager) freeChainPop(segmentId segId, whileListBucketId Offset) (freeDirOffset Offset) {
	loop := 0
FindFreeDir:
	loop++
	if loop > 50 {
		// should not happen after many purge
		// TODO: remove panic once stable
		panic("freeChainPop: loop>50")
	}
	index := dm.DirFreeStart[segmentId]
	// no available free dir
	if index == 0 {
		// try to rebuild the free chain
		foundFreeDir := dm.freeChainRebuild(segmentId)
		log.Printf("dirFreeChainPop: no free dir, rebuild %d dirs", foundFreeDir)

		// purge some dirs
		if foundFreeDir == 0 {
			purgedNum := dm.purgeRandom10(segmentId, whileListBucketId)
			log.Printf("dirFreeChainPop: no free dir, purge %d dirs", purgedNum)
			goto FindFreeDir
		}
	}

	// pop the first free dir
	freeDirOffset = Offset(index)
	dm.freeChainDelete(segmentId, freeDirOffset)
	return
}

// freeChainRebuild rebuilds the free chain in a segment, try to reuse free dirs
func (dm *DirManager) freeChainRebuild(segmentId segId) Offset {
	dm.DirFreeStart[segmentId] = 0
	prev := Offset(0)
	counter := Offset(0)

	for b := 0; b < int(dm.BucketsNumPerSegment); b++ {
		index := Offset(b) * DirDepth
		for j := 1; j < DirDepth; j++ {
			thisNodeIndex := index + Offset(j)
			if dm.Dirs[segmentId][thisNodeIndex].offset() == 0 {
				counter++
				if dm.DirFreeStart[segmentId] == 0 {
					dm.DirFreeStart[segmentId] = uint16(thisNodeIndex)
				}
				dm.Dirs[segmentId][thisNodeIndex].clear()
				dm.Dirs[segmentId][thisNodeIndex].setPrev(uint16(prev))
				if prev != 0 {
					dm.Dirs[segmentId][prev].setNext(uint16(thisNodeIndex))
				}
				prev = thisNodeIndex
			}
		}
	}

	//if prev != 0 {
	//	dm.Dirs[segmentId][prev].setNext(0)
	//}
	return counter
}

// purgeRandom10 purges 10% dirs in the segment randomly.
// if bucketsNumPerSegment < 10, purge all dirs
func (dm *DirManager) purgeRandom10(segmentId segId, whileListBucketId Offset) Offset {
	randomIndex := rand.Intn(10)
	counter := 0
	for i := Offset(0); i < dm.BucketsNumPerSegment; i++ {
		if (dm.BucketsNumPerSegment > 10) && (i%10 != Offset(randomIndex)) {
			continue
		}
		if i == whileListBucketId {
			continue
		}
		// purge whole bucket
		index := Offset(i) * DirDepth
		c := 0 // do while
		for index != 0 || c == 0 {
			counter++
			c++

			next := Offset(dm.Dirs[segmentId][index].next())
			dm.Dirs[segmentId][index].clear()
			index = next
		}
	}

	dm.freeChainRebuild(segmentId)

	return Offset(counter)
}
