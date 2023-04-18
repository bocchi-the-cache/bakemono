package bakemono

import (
	"crypto/rand"
	"fmt"
	"testing"
)

func TestNewDirManager(t *testing.T) {
	dm := &DirManager{}
	dm.Init(123457)
	if dm.ChunksNum != 123456 {
		t.Error("ChunksNum should be 123456")
	}
	if dm.BucketsNum != 30864 {
		t.Error("BucketsNum should be 30864")
	}
	if dm.BucketsNumPerSegment != 15432 {
		t.Error("BucketsNumPerSegment should be 15432")
	}
	return
}

func countDirFreeInChain(dm *DirManager) (int, error) {
	// dir check
	var index uint16
	var counter1 int
	var counter2 int
	for seg := 0; seg < int(dm.SegmentsNum); seg++ {
		index = dm.DirFreeStart[segId(seg)]
		if index == 0 {
			return 0, fmt.Errorf("dir free start should not be 0")
		}
		for index != 0 {
			counter1++
			index = dm.Dirs[segId(seg)][index].next()
		}
	}
	//reverse counter
	for seg := 0; seg < int(dm.SegmentsNum); seg++ {
		index = dm.DirFreeStart[segId(seg)]
		for dm.Dirs[segId(seg)][index].next() != 0 {
			index = dm.Dirs[segId(seg)][index].next()
		}
		for index != 0 {
			counter2++
			index = dm.Dirs[segId(seg)][index].prev()
		}
	}
	if counter1 != counter2 {
		return 0, fmt.Errorf("dir free chain should be same")
	}
	return counter1, nil
}

func TestDirManager_InitEmptyDirs(t *testing.T) {
	dm := &DirManager{}
	dm.Init(123457)
	dm.InitEmptyDirs()

	counter, err := countDirFreeInChain(dm)
	if err != nil {
		t.Error(err)
	}
	t.Logf("dir free chain length: %d", counter)
	if Offset(counter) != (dm.SegmentsNum * dm.BucketsNumPerSegment * (DirDepth - 1)) {
		t.Error("dir free chain not match")
	}
	return
}

func TestDirManager_FreeChainDelete(t *testing.T) {
	// NOTE: only free dir has `prev`. Panic if `prev` is not 0.
	dm := &DirManager{}
	dm.Init(123457)
	dm.InitEmptyDirs()

	// 1. delete head
	{
		seg := segId(0)
		index := dm.DirFreeStart[seg]
		t.Logf("to delete index: %d", index)
		dm.freeChainDelete(seg, Offset(index))
		counter, err := countDirFreeInChain(dm)
		if err != nil {
			t.Error(err)
		}
		t.Logf("dir free chain length: %d", counter)
		if Offset(counter) != (dm.SegmentsNum*dm.BucketsNumPerSegment*(DirDepth-1) - 1) {
			t.Error("dir free chain not match")
		}
	}
	// 2. delete middle
	{
		seg := segId(0)
		index := dm.DirFreeStart[seg]
		index = dm.Dirs[seg][index].next()
		t.Logf("to delete index: %d", index)
		dm.freeChainDelete(seg, Offset(index))
		counter, err := countDirFreeInChain(dm)
		if err != nil {
			t.Error(err)
		}
		t.Logf("dir free chain length: %d", counter)
		if Offset(counter) != (dm.SegmentsNum*dm.BucketsNumPerSegment*(DirDepth-1) - 2) {
			t.Error("dir free chain not match")
		}
	}
	// 3. delete tail
	{
		seg := segId(0)
		index := dm.DirFreeStart[seg]
		for dm.Dirs[seg][index].next() != 0 {
			index = dm.Dirs[seg][index].next()
		}
		t.Logf("to delete index: %d", index)
		dm.freeChainDelete(seg, Offset(index))
		counter, err := countDirFreeInChain(dm)
		if err != nil {
			t.Error(err)
		}
		t.Logf("dir free chain length: %d", counter)
		if Offset(counter) != (dm.SegmentsNum*dm.BucketsNumPerSegment*(DirDepth-1) - 3) {
			t.Error("dir free chain not match")
		}
	}
}

func TestDirManager_FreeChainRebuild(t *testing.T) {
	// NOTE: only free dir has `prev`. Panic if `prev` is not 0.
	dm := &DirManager{}
	dm.Init(123457)
	dm.InitEmptyDirs()

	// 1. head
	{
		seg := segId(0)
		index := dm.DirFreeStart[seg]
		t.Logf("to pad index: %d", index)
		dm.Dirs[seg][index].setOffset(1)
		dm.freeChainRebuild(seg)

		counter, err := countDirFreeInChain(dm)
		if err != nil {
			t.Error(err)
		}
		t.Logf("dir free chain length: %d", counter)
		if Offset(counter) != (dm.SegmentsNum*dm.BucketsNumPerSegment*(DirDepth-1) - 1) {
			t.Error("dir free chain not match")
		}
	}
	// 2. middle
	{
		seg := segId(0)
		index := dm.DirFreeStart[seg]
		index = dm.Dirs[seg][index].next()
		t.Logf("to pad index: %d", index)
		dm.Dirs[seg][index].setOffset(1)
		dm.freeChainRebuild(seg)

		counter, err := countDirFreeInChain(dm)
		if err != nil {
			t.Error(err)
		}
		t.Logf("dir free chain length: %d", counter)
		if Offset(counter) != (dm.SegmentsNum*dm.BucketsNumPerSegment*(DirDepth-1) - 2) {
			t.Error("dir free chain not match")
		}
	}
	// 3. tail
	{
		seg := segId(0)
		index := dm.DirFreeStart[seg]
		for dm.Dirs[seg][index].next() != 0 {
			index = dm.Dirs[seg][index].next()
		}
		t.Logf("to pad index: %d", index)
		dm.Dirs[seg][index].setOffset(1)
		dm.freeChainRebuild(seg)

		counter, err := countDirFreeInChain(dm)
		if err != nil {
			t.Error(err)
		}
		t.Logf("dir free chain length: %d", counter)
		if Offset(counter) != (dm.SegmentsNum*dm.BucketsNumPerSegment*(DirDepth-1) - 3) {
			t.Error("dir free chain not match")
		}
	}
}

func TestDirManager_Probe(t *testing.T) {
	// 1 miss head bucket dir
	{
		dirs := make([]*Dir, 12)
		for i := 0; i < len(dirs); i++ {
			dirs[i] = &Dir{}
		}
		_ = linkEmptyDirs(dirs)

		hit, pos, _ := dirProbe(1, 0, dirs)
		if hit {
			t.Error("should not hit")
		}
		if pos != 0 {
			t.Error("pos should be 0")
		}

		hit, pos, _ = dirProbe(1, 1, dirs)
		if hit {
			t.Error("should not hit")
		}
		if pos != 4 {
			t.Error("pos should be 4")
		}
	}

	// 2 hit head bucket dir
	{
		dirs := make([]*Dir, 12)
		for i := 0; i < len(dirs); i++ {
			dirs[i] = &Dir{}
		}
		_ = linkEmptyDirs(dirs)

		dirs[0].setOffset(1)
		dirs[0].setTag(1)
		hit, pos, _ := dirProbe(1, 0, dirs)
		if !hit {
			t.Error("should hit")
		}
		if pos != 0 {
			t.Error("pos should be 0")
		}

		dirs[4].setOffset(1)
		dirs[4].setTag(1)
		hit, pos, _ = dirProbe(1, 1, dirs)
		if !hit {
			t.Error("should hit")
		}
		if pos != 4 {
			t.Error("pos should be 4")
		}
	}

	// 3 miss non-head bucket dir
	{
		dirs := make([]*Dir, 12)
		for i := 0; i < len(dirs); i++ {
			dirs[i] = &Dir{}
		}
		_ = linkEmptyDirs(dirs)

		dirs[0].setOffset(1)
		dirs[0].setTag(1)
		dirs[0].setNext(1)
		dirs[1].setOffset(1)
		dirs[1].setTag(2)
		dirs[1].setNext(0)

		hit, pos, _ := dirProbe(3, 0, dirs)
		if hit {
			t.Error("should not hit")
		}
		if pos != 0 {
			t.Error("pos should be 0")
		}
	}
	// 4 hit non-head bucket dir
	{
		dirs := make([]*Dir, 12)
		for i := 0; i < len(dirs); i++ {
			dirs[i] = &Dir{}
		}
		_ = linkEmptyDirs(dirs)

		dirs[0].setOffset(1)
		dirs[0].setTag(1)
		dirs[0].setNext(1)
		dirs[1].setOffset(1)
		dirs[1].setTag(2)
		dirs[1].setNext(0)
		hit, pos, _ := dirProbe(2, 0, dirs)
		if !hit {
			t.Error("should not hit")
		}
		if pos != 1 {
			t.Error("pos should be 0")
		}

		dirs[4].setOffset(1)
		dirs[4].setTag(1)
		dirs[4].setNext(5)
		dirs[5].setOffset(1)
		dirs[5].setTag(2)
		dirs[5].setNext(6)
		dirs[6].setOffset(1)
		dirs[6].setTag(3)
		dirs[6].setNext(0)
		hit, pos, _ = dirProbe(2, 1, dirs)
		if !hit {
			t.Error("should not hit")
		}
		if pos != 5 {
			t.Error("pos should be 5")
		}
	}
}

func TestDirManager_PurgeRandom10WhenFull(t *testing.T) {
	dm := &DirManager{}
	dm.Init(123457)
	dm.InitEmptyDirs()
	// set every dir to be used
	counter1 := 0
	for seg := segId(0); Offset(seg) < dm.SegmentsNum; seg++ {
		dm.DirFreeStart[seg] = 0
		for b := Offset(0); b < dm.BucketsNumPerSegment; b++ {
			index := b * DirDepth
			for i := Offset(0); i < DirDepth; i++ {
				counter1++
				dm.Dirs[seg][index+i].setNext(0)
				dm.Dirs[seg][index+i].setOffset(1)
				dm.Dirs[seg][index+i].setTag(1)
				if i != DirDepth-1 {
					dm.Dirs[seg][index+i].setNext(uint16(index + i + 1))
				}
			}
		}
		build := dm.freeChainRebuild(seg)
		t.Logf("seg %v free chain rebuild: %v", seg, build)
	}
	t.Logf("set every dir to be used, dir total: %d", counter1)

	// purge 10 random dirs
	for seg := segId(0); Offset(seg) < dm.SegmentsNum; seg++ {
		p := dm.purgeRandom10(seg)
		t.Logf("seg %v purge 10 random dirs: %v", seg, p)
	}

	counter, err := countDirFreeInChain(dm)
	if err != nil {
		t.Error(err)
	}
	t.Logf("after purge 10, dir free chain length: %d", counter)
	if Offset(counter) == 0 {
		t.Error("dir free chain empty")
	}
}

func TestDirManager_FreeChainPop(t *testing.T) {
	dm := &DirManager{}
	dm.Init(123457)
	dm.InitEmptyDirs()
	// set every dir to be used
	counter1 := 0
	for seg := segId(0); Offset(seg) < dm.SegmentsNum; seg++ {
		dm.DirFreeStart[seg] = 0
		for b := Offset(0); b < dm.BucketsNumPerSegment; b++ {
			index := b * DirDepth
			for i := Offset(0); i < DirDepth; i++ {
				counter1++
				dm.Dirs[seg][index+i].setNext(0)
				dm.Dirs[seg][index+i].setOffset(1)
				dm.Dirs[seg][index+i].setTag(1)
				if i != DirDepth-1 {
					dm.Dirs[seg][index+i].setNext(uint16(index + i + 1))
				}
			}
		}
		build := dm.freeChainRebuild(seg)
		t.Logf("seg %v free chain rebuild: %v", seg, build)
	}
	t.Logf("set every dir to be used, dir total: %d", counter1)

	// pop 10 random dirs
	for seg := segId(0); Offset(seg) < dm.SegmentsNum; seg++ {
		p := dm.freeChainPop(seg)
		t.Logf("seg %v pop 10 random dirs: %v", seg, p)
	}

	counter, err := countDirFreeInChain(dm)
	if err != nil {
		t.Error(err)
	}
	t.Logf("after pop 10, dir free chain length: %d", counter)
	if Offset(counter) == 0 {
		t.Error("dir free chain empty")
	}
}

func TestDirManager_GetSet(t *testing.T) {
	dm := &DirManager{}
	dm.Init(20)
	dm.InitEmptyDirs()

	// set same key
	{
		randomKey := make([]byte, 20)
		_, err := rand.Read(randomKey)

		pos, err := dm.Set(randomKey, 100, 200)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("pos: %v", pos)
		pos2, err := dm.Set(randomKey, 100, 200)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("pos: %v", pos2)
		if pos != pos2 {
			t.Error("pos should be same")
		}

		// get

		hit, pos3, d := dm.Get(randomKey)
		if !hit {
			t.Fatal("should hit")
		}
		if pos3 != pos {
			t.Error("pos should be same")
		}

		if d.offset() != 100 {
			t.Error("offset should be 100")
		}

		if d.approxSize() == 0 {
			t.Error("approxSize should be > 0")
		}
	}
}
