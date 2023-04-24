package bakemono

import (
	"os"
	"testing"
)

func CreateTestingVol(path string, fileSize, chunkSize uint64) (*Vol, bool, error) {
	cfg, err := NewDefaultVolOptions(path, fileSize, chunkSize)
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

func TestVolWriteReadFileWithClose(t *testing.T) {
	v, _, err := CreateTestingVol("/tmp/bakemono-test.vol", 1024*1024*100, 1024*1024)
	defer func() {
		err := os.Remove("/tmp/bakemono-test.vol")
		if err != nil {
			t.Error(err)
		}
	}()
	if err != nil {
		t.Fatal(err)
	}
	err = v.Set([]byte("key"), []byte("value"))
	if err != nil {
		t.Fatal(err)
	}
	hit, data, err := v.Get([]byte("key"))
	if err != nil {
		t.Fatal(err)
	}
	if !hit {
		t.Fatal("key should be hit")
	}
	if string(data) != "value" {
		t.Fatal("value should be 'value'")
	}

	err = v.flushMetaToFp()
	if err != nil {
		t.Fatal(err)
	}

	_, corrupted, err := CreateTestingVol("/tmp/bakemono-test.vol", 1024*1024*100, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	if corrupted {
		t.Fatal("vol should not be corrupted")
	}

	hit, data, err = v.Get([]byte("key"))
	if err != nil {
		t.Fatal(err)
	}
	if !hit {
		t.Fatal("key should be hit")
	}
	if string(data) != "value" {
		t.Fatal("value should be 'value'")
	}

	err = v.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestVolMetaRecover(t *testing.T) {
	v, _, err := CreateTestingVol("/tmp/bakemono-test1.vol", 1024*1024*100, 1024*1024)
	defer func() {
		err := os.Remove("/tmp/bakemono-test1.vol")
		if err != nil {
			t.Error(err)
		}
	}()
	if err != nil {
		t.Fatal(err)
	}
	err = v.Set([]byte("key"), []byte("value"))
	if err != nil {
		t.Fatal(err)
	}

	err = v.flushMetaToFp()
	if err != nil {
		t.Fatal(err)
	}

	hit, data, err := v.Get([]byte("key"))
	if err != nil {
		t.Fatal(err)
	}
	if !hit {
		t.Fatal("key should be hit")
	}
	if string(data) != "value" {
		t.Fatal("value should be 'value'")
	}

	err = v.Close()
	if err != nil {
		t.Fatal(err)
	}

	v2, corrupted, err := CreateTestingVol("/tmp/bakemono-test1.vol", 1024*1024*100, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	if corrupted {
		t.Fatal("vol should not be corrupted")
	}
	hit, data, err = v2.Get([]byte("key"))
	if err != nil {
		t.Fatal(err)
	}
	if !hit {
		t.Fatal("key should be hit")
	}
	if string(data) != "value" {
		t.Fatal("value should be 'value'")
	}
	v2.Close()
}

func TestVolBadRead(t *testing.T) {
	_, corrupted, err := CreateTestingVol("/tmp/bakemono-test-bad.vol", 1024*1024*100, 1024*1024)
	defer func() {
		err := os.Remove("/tmp/bakemono-test-bad.vol")
		if err != nil {
			t.Fatal(err)
		}
	}()
	if err != nil {
		t.Fatal(err)
	}
	if !corrupted {
		t.Fatal("vol should be corrupted")
	}
}
