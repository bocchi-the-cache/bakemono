package bakemono

import (
	"os"
	"testing"
)

func CreateTestingVol(path string, fileSize, chunkSize uint64) (*Vol, bool, error) {
	cfg, err := NewVolOptionsWithFileTruncate(path, fileSize, chunkSize)
	if err != nil {
		panic(err)
	}
	v := &Vol{}
	corrupted, err := v.Init(cfg)
	if err != nil {
		panic(err)
	}
	return v, corrupted, err
}

func TestInitVol(t *testing.T) {
	_, _, err := CreateTestingVol("/tmp/bakemono-test.vol", 1024*1024*100, 1024*1024)
	defer func() {
		err := os.Remove("/tmp/bakemono-test.vol")
		if err != nil {
			t.Error(err)
		}
	}()
	if err != nil {
		t.Error(err)
	}
}

func TestVolWriteReadFile(t *testing.T) {
	v, _, err := CreateTestingVol("/tmp/bakemono-test.vol", 1024*1024*100, 1024*1024)
	defer func() {
		err := os.Remove("/tmp/bakemono-test.vol")
		if err != nil {
			t.Error(err)
		}
	}()
	if err != nil {
		t.Error(err)
	}
	err = v.flushMetaToFp()
	if err != nil {
		t.Error(err)
	}

	_, corrupted, err := CreateTestingVol("/tmp/bakemono-test.vol", 1024*1024*100, 1024*1024)
	if err != nil {
		t.Error(err)
	}
	if corrupted {
		t.Error("vol should not be corrupted")
	}
}

func TestVolBadRead(t *testing.T) {
	_, corrupted, err := CreateTestingVol("/tmp/bakemono-test-bad.vol", 1024*1024*100, 1024*1024)
	defer func() {
		err := os.Remove("/tmp/bakemono-test-bad.vol")
		if err != nil {
			t.Error(err)
		}
	}()
	if err != nil {
		t.Error(err)
	}
	if !corrupted {
		t.Error("vol should be corrupted")
	}
}

func TestVol_InitEmptyMeta(t *testing.T) {
	v, _, err := CreateTestingVol("/tmp/bakemono-test.vol", 1024*1024*100, 1024*1024)
	defer func() {
		err := os.Remove("/tmp/bakemono-test.vol")
		if err != nil {
			t.Error(err)
		}
	}()
	if err != nil {
		t.Error(err)
	}
	v.initEmptyMeta()

	// dir check
	var index uint16
	for seg := 0; seg < int(v.SegmentsNum); seg++ {
		index = v.DirFreeStart[segId(seg)]
		if index == 0 {
			t.Error("dir free start should not be 0")
		}
		var counter = 0
		for index != 0 {
			counter++
			index = v.Dirs[segId(seg)][index].next()
		}
		if counter != int(v.BucketsNumPerSegment*(DirDepth-1)) {
			t.Errorf("dir free start should be %d, but %d", v.BucketsNumPerSegment, counter)
		}
	}
}
