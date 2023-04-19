package bakemono

import (
	"bytes"
	"fmt"
	"log"
)

func (dm *DirManager) DiagHangUsedDirs() (int, error) {
	// mark dirs used and linked from head node
	var counter int
	var mpDirsUsed []map[int]bool
	for seg := 0; seg < int(dm.SegmentsNum); seg++ {
		mp := make(map[int]bool)
		for buck := 0; buck < int(dm.BucketsNumPerSegment); buck++ {
			index := buck * DirDepth
			for dm.Dirs[segId(seg)][index].offset() != 0 {
				counter++
				mp[index] = true
				next := int(dm.Dirs[segId(seg)][index].next())
				if next == 0 {
					break
				}
				index = next
				if counter > 100000000 {
					panic("DiagHangUsedDirs: counter > 100000000")
				}
			}
		}
		mpDirsUsed = append(mpDirsUsed, mp)
	}

	// check if dir hang-up
	for seg := 0; seg < int(dm.SegmentsNum); seg++ {
		for buck := 0; buck < int(dm.BucketsNumPerSegment); buck++ {
			index := buck * DirDepth
			for i := 0; i < DirDepth; i++ {
				if dm.Dirs[segId(seg)][index+i].offset() != 0 {
					if !mpDirsUsed[seg][index+i] {
						return 0, fmt.Errorf("find a hang-up dir. seg: %d, buck: %d, index: %d", seg, buck, index+i)
					}
				}
			}
		}
	}
	return counter, nil
}

func (dm *DirManager) DiagHangFreeDirs() (int, error) {
	// mark dirs registered in free list
	var counter int
	var mpDirsInFreeList []map[uint16]bool
	for seg := 0; seg < int(dm.SegmentsNum); seg++ {
		mp := make(map[uint16]bool)
		index := dm.DirFreeStart[segId(seg)]

		for index != 0 {
			counter++
			mp[index] = true
			index = dm.Dirs[segId(seg)][index].next()
			if counter > 100000000 {
				panic("DiagHangFreeDirs: counter > 100000000")
			}
		}
		mpDirsInFreeList = append(mpDirsInFreeList, mp)
	}

	// check if dir hang-up
	for seg := 0; seg < int(dm.SegmentsNum); seg++ {
		for buck := 0; buck < int(dm.BucketsNumPerSegment); buck++ {
			index := buck * DirDepth
			for i := 1; i < DirDepth; i++ {
				if dm.Dirs[segId(seg)][index+i].offset() == 0 {
					if !mpDirsInFreeList[seg][uint16(index+i)] {
						return 0, fmt.Errorf("find a hang-up dir. seg: %d, buck: %d, index: %d", seg, buck, index+i)
					}
				}
			}
		}
	}
	return counter, nil
}

func (dm *DirManager) DiagDumpAllDirs() {
	log.Printf("Dump all dirs, segments: %d, buckets: %d", dm.SegmentsNum, dm.BucketsNumPerSegment)
	for seg := 0; seg < int(dm.SegmentsNum); seg++ {
		log.Printf("Segment %d", seg)
		for buck := 0; buck < int(dm.BucketsNumPerSegment); buck++ {
			log.Printf("Bucket %d", buck)
			index := buck * DirDepth
			for i := 0; i < DirDepth; i++ {
				log.Printf("Segment %d, \tBucket %d, \tDir %d, \toffset: %d, \tprev: %d, next: %d",
					seg, buck, index, dm.Dirs[segId(seg)][index].offset(), dm.Dirs[segId(seg)][index].prev(), dm.Dirs[segId(seg)][index].next())
				index++
			}
		}
	}
}

func (dm *DirManager) DiagDumpAllDirsToString() string {
	bf := bytes.NewBufferString("")
	bf.WriteString(fmt.Sprintf("Dump all dirs, segments: %d, buckets: %d\n", dm.SegmentsNum, dm.BucketsNumPerSegment))
	for seg := 0; seg < int(dm.SegmentsNum); seg++ {
		//log.Printf("Segment %d", seg)
		for buck := 0; buck < int(dm.BucketsNumPerSegment); buck++ {
			//log.Printf("Bucket %d", buck)
			index := buck * DirDepth
			for i := 0; i < DirDepth; i++ {
				bf.WriteString(fmt.Sprintf(
					"Segment %d, \tBucket %d, \tDir %d, \toffset: %d, \tprev: %d, next: %d\n",
					seg, buck, index, dm.Dirs[segId(seg)][index].offset(), dm.Dirs[segId(seg)][index].prev(), dm.Dirs[segId(seg)][index].next()))
				index++
			}
		}
	}
	return bf.String()
}

func (dm *DirManager) DiagPanicHangUpDirs() error {
	_, err := dm.DiagHangUsedDirs()
	if err != nil {
		log.Printf("dirInsert: DiagHangUsedDirs: %v", err)
		return err
	}
	return nil
}
