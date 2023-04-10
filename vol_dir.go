package bakemono

import (
	"log"
	"math/rand"
)

// dirProbe returns
// HIT: the offset of the dir entry with the given key,
// MISS: the offset of 0 if the key is not found.
func (v *Vol) dirProbe(key uint16, segmentId segId, bucketId offset) (hit bool, dirOffset offset) {
	index := bucketId * DirDepth

	for index != 0 {
		if v.Dirs[segmentId][index].offset() == 0 {
			return false, 0
		}
		dirKey := v.Dirs[segmentId][index].tag()
		if dirKey == key {
			return true, index
		}
		index = offset(v.Dirs[segmentId][index].next())
	}

	return false, 0
}

// dirFindBucketNextFree sets up the next free dir.
// delete the dir from the free list if it is in the free list.
// return the offset of the dir
func (v *Vol) dirFindBucketNextFree(segmentId segId, bucketId offset) offset {
	index := bucketId * DirDepth
	// depth #0
	if v.Dirs[segmentId][index].offset() == 0 {
		return index
	}
	// depth #1-#3
	for i := 1; i < DirDepth; i++ {
		if v.Dirs[segmentId][index+offset(i)].offset() == 0 {
			// delete from free list
			index = index + offset(i)
			v.dirChainDelete(segmentId, index)
			return index
		}
	}
	// find an available free dir
	index = v.dirFreeChainPop(segmentId)
	return index
}

// dirClear clears a dir
func (v *Vol) dirClear(segmentId segId, dirOffset offset) {
	v.Dirs[segmentId][dirOffset].clear()
}

// dirChainDelete deletes a dir from the chain
func (v *Vol) dirChainDelete(segmentId segId, dirOffset offset) {
	prev := v.Dirs[segmentId][dirOffset].prev()
	if prev == 0 {
		v.DirFreeStart[segmentId] = v.Dirs[segmentId][dirOffset].next()
	} else {
		v.Dirs[segmentId][prev].setNext(v.Dirs[segmentId][dirOffset].next())
	}

	next := v.Dirs[segmentId][dirOffset].next()
	if next != 0 {
		v.Dirs[segmentId][next].setPrev(prev)
	}
	return
}

// dirFreeChainPop pops a free dir from the free chain
func (v *Vol) dirFreeChainPop(segmentId segId) offset {
	loop := 0
FindFreeDir:
	loop++
	if loop > 10 {
		// should not happen after many purge
		log.Printf("dirFreeChainPop: loop > 10")
	}
	index := v.DirFreeStart[segmentId]
	// no available free dir
	if index == 0 {
		// try to rebuild the free chain
		foundFreeDir := v.dirFreeChainRebuild(segmentId)
		log.Printf("dirFreeChainPop: no free dir, rebuild %d dirs", foundFreeDir)

		// purge some dirs
		if foundFreeDir == 0 {
			purgedNum := v.dirPurgeRandomBatch(segmentId)
			log.Printf("dirFreeChainPop: no free dir, purge %d dirs", purgedNum)
			goto FindFreeDir
		}
	}
	v.DirFreeStart[segmentId] = v.Dirs[segmentId][index].next()
	return offset(index)
}

// dirFreeChainRebuild rebuilds the free chain, try to reuse free dirs
func (v *Vol) dirFreeChainRebuild(segmentId segId) offset {
	prev := offset(0)
	counter := offset(0)

	for i := offset(0); i < v.BucketsNumPerSegment; i++ {
		index := offset(i) * DirDepth
		for j := 1; j < DirDepth; j++ {
			if v.Dirs[segmentId][index+offset(j)].offset() == 0 {
				counter++
				v.Dirs[segmentId][index+offset(j)].setPrev(uint16(prev))
				if prev != 0 {
					v.Dirs[segmentId][prev].setNext(uint16(index + offset(j)))
				} else {
					v.DirFreeStart[segmentId] = uint16(prev)
				}
				prev = index + offset(j)
			}
		}
	}
	if prev != 0 {
		v.Dirs[segmentId][prev].setNext(0)
	}
	return counter
}

// dirPurgeRandomBatch purges 10% dirs in the segment randomly
func (v *Vol) dirPurgeRandomBatch(segmentId segId) offset {
	randomIndex := rand.Intn(10)
	counter := 0
	for i := offset(0); i < v.BucketsNumPerSegment; i++ {
		if i%10 == offset(randomIndex) {
			continue
		}
		// purge whole bucket
		counter++
		index := offset(i) * DirDepth
		for j := 0; j < DirDepth; j++ {
			v.dirClear(segmentId, index+offset(j))
		}
	}
	return offset(counter) * DirDepth
}

func (v *Vol) dirSetBucketNext(segmentId segId, bucketId offset, next offset) {
	index := bucketId * DirDepth
	if index == next {
		return
	}
	// found last dir not empty
	for index != 0 {
		if v.Dirs[segmentId][index].next() == 0 {
			break
		}
		index = offset(v.Dirs[segmentId][index].next())
	}
	v.Dirs[segmentId][index].setNext(uint16(next))
}
