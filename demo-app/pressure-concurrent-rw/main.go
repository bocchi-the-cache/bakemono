package main

import (
	"fmt"
	"log"
	rand2 "math/rand"
	"os"
	"sync"
	"time"

	"github.com/bocchi-the-cache/bakemono"
)

const LOOP = 50000

func main() {
	_ = os.Remove("/tmp/bakemono-test.vol")

	cfg, err := bakemono.NewVolOptionsWithFileTruncate("/tmp/bakemono-test.vol", 1024*512*1000000, 1024*1024)
	if err != nil {
		panic(err)
	}
	v := &bakemono.Vol{}
	corrupted, err := v.Init(cfg)
	if err != nil {
		panic(err)
	}
	if corrupted {
		log.Printf("vol is corrupted, but fixed. ignore this if first time running.")
	}

	for i := 0; i < 100000; i++ {
		log.Printf(fmt.Sprintf("--------------------------------- start loop #%d", i))
		wg := &sync.WaitGroup{}
		wg.Add(2)

		rw := &RW{
			v:   v,
			key: rand2.Int63n(100000),
			mp:  &sync.Map{},
		}
		go func() {
			rw.WLoop()
			wg.Done()
		}()

		//wg.Wait()

		//wg.Add(1)

		go func() {
			rw.RLoop()
			wg.Done()
		}()

		wg.Wait()
	}
}

type RW struct {
	v   *bakemono.Vol
	mp  *sync.Map
	key int64
}

func (rw *RW) GetContent(key int64, size int) []byte {
	seed := key + int64(size) // 设置种子
	r := rand2.New(rand2.NewSource(seed))

	// 生成固定长度的随机字节数组
	bytes := make([]byte, size)
	if _, err := r.Read(bytes); err != nil {
		panic(err)
	}

	return bytes
}

func (rw *RW) GetContentSize(key int64, serial int) int {
	seed := key + int64(serial) // 设置种子
	r := rand2.New(rand2.NewSource(seed))

	return r.Intn(511) + 1
	//return 500
}

func (rw *RW) WLoop() {
	t := time.Now()
	randomKey := rw.key

	for i := 0; i < LOOP; i++ {
		randomSize := rw.GetContentSize(randomKey, i)
		randomData := rw.GetContent(randomKey, 1024*randomSize)
		if i%10000 == 0 {
			log.Printf("++ set key-%d-%d", randomKey, i)
		}
		rw.mp.Store(fmt.Sprintf("key-%d-%d", randomKey, i), randomSize)
		err := rw.v.Set([]byte(fmt.Sprintf("key-%d-%d", randomKey, i)), randomData)
		if err != nil {
			panic(err)
		}
	}
	log.Printf("set LOOP keys in %s", time.Since(t))
}

func (rw *RW) RLoop() {
	randomKey := rw.key
	counter := make(map[string]int)
	t := time.Now()

	for i := 0; i < LOOP*100; i++ {
		if i%100000 == 0 {
			log.Printf("get 100000 LOOP keys in %s", time.Since(t))
			log.Printf("hit: %d, miss: %d", counter["hit"], counter["miss"])
			counter = make(map[string]int)
			t = time.Now()

			log.Printf("-- get key times : %d", i)
		}

		serial := rand2.Intn(LOOP)

		hit, data, err := rw.v.Get([]byte(fmt.Sprintf("key-%d-%d", randomKey, serial)))
		if !hit {
			counter["miss"]++
		} else {
			counter["hit"]++
		}
		if err != nil {
			log.Printf("err: %v", err)
			continue
		}
		if !hit {
			continue
		}

		randomSize := rw.GetContentSize(randomKey, serial)

		randomData := rw.GetContent(randomKey, 1024*randomSize)

		if len(data) != 1024*randomSize {
			log.Printf("data len %v", len(data))
			log.Printf("random len %v", 1024*randomSize)
			//continue
			panic("data length is not 1024*randomSize")
		}
		dataS := string(data)
		randomS := string(randomData)
		if dataS != randomS {
			log.Printf("dataS %s", dataS)
			log.Printf("randomS %s", randomS)
			panic("data is not equal")
		}
	}
	log.Printf("get LOOP keys in %s", time.Since(t))
	log.Printf("hit: %d, miss: %d", counter["hit"], counter["miss"])
}
