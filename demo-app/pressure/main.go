package main

import (
	"crypto/rand"
	"fmt"
	"log"
	rand2 "math/rand"
	"os"
	"time"

	"github.com/bocchi-the-cache/bakemono"
)

const LOOP = 50000

func main() {
	_ = os.Remove("/tmp/bakemono-test.vol")

	cfg, err := bakemono.NewVolOptionsWithFileTruncate("/tmp/bakemono-test.vol", 1024*512*1000, 1024*1024)
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

	for i := 0; i < 1000; i++ {
		log.Printf(fmt.Sprintf("--------------------------------- start loop #%d", i))
		CacheRWLoop(v)
	}
}

func CacheRWLoop(v *bakemono.Vol) {
	// pad randomData with random data
	randomKey := rand2.Int63()
	randomSize := rand2.Intn(512)
	randomData := make([]byte, 1024*randomSize)
	rand.Read(randomData)

	t := time.Now()

	for i := 0; i < LOOP; i++ {
		if i%10000 == 0 {
			log.Printf("++ set key-%d-%d", randomKey, i)
		}
		err := v.Set([]byte(fmt.Sprintf("key-%d-%d", randomKey, i)), randomData)
		if err != nil {
			panic(err)
		}
		//loop := v.DirCheckNextLoop()
		//if loop {
		//	panic("loop should not be true")
		//}
		//v.DirDebugPrint()

	}
	log.Printf("set LOOP keys in %s", time.Since(t))

	counter := make(map[string]int)
	t = time.Now()
	for i := 0; i < LOOP; i++ {
		if i%10000 == 0 {
			log.Printf("-- get key-%d", i)
		}
		hit, data, err := v.Get([]byte(fmt.Sprintf("key-%d-%d", randomKey, i)))
		if !hit {
			counter["miss"]++
		} else {
			counter["hit"]++
		}
		if err != nil {
			panic(err)
		}

		if !hit {
			continue
		}

		if len(data) != 1024*randomSize {
			log.Printf("data len %v", len(data))
			log.Printf("random len %v", 1024*randomSize)
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
