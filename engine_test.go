package bakemono

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
)

func InitEngine(path string) (*Engine, error) {
	engine := NewEngine(path)
	err := engine.Init()
	if err != nil {
		return nil, err
	}
	return engine, nil
}

func TestEngineInit(t *testing.T) {
	path := "/tmp/bakemono_test.cache"
	eg, err := InitEngine(path)
	if err != nil {
		t.Fatal(err)
	}
	if eg == nil {
		t.Fatal("eg is nil")
	}
}

func TestEngineInitWithNonExistFile(t *testing.T) {
	randomSuffix := rand.Intn(1000000)
	randomPath := fmt.Sprintf("/tmp/bakemono_test_%d.cache", randomSuffix)
	// delete randomPath if exists
	if _, err := os.Stat(randomPath); err == nil {
		err := os.Remove(randomPath)
		if err != nil {
			t.Fatal(err)
		}
	}

	eg, err := InitEngine(randomPath)
	if err != nil {
		t.Fatal(err)
	}
	if eg == nil {
		t.Fatal("eg is nil")
	}
}

func TestEngineSet(t *testing.T) {
	path := "/tmp/bakemono_test.cache"
	engine := NewEngine(path)
	err := engine.Init()
	if err != nil {
		t.Fatal(err)
	}

	err = engine.Set([]byte("key"), []byte("value"))
	if err != nil {
		t.Fatal(err)
	}
}

func TestEngineGet(t *testing.T) {
	path := "/tmp/bakemono_test.cache"
	engine := NewEngine(path)
	err := engine.Init()
	if err != nil {
		t.Fatal(err)
	}

	_, err = engine.Get([]byte("key"))
	if err != nil {
		t.Fatal(err)
	}
}

func TestEngineDelete(t *testing.T) {
	path := "/tmp/bakemono_test.cache"
	engine := NewEngine(path)
	err := engine.Init()
	if err != nil {
		t.Fatal(err)
	}

	err = engine.Delete([]byte("key"))
	if err != nil {
		t.Fatal(err)
	}
}
