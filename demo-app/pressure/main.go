package main

import (
	"crypto/rand"
	"fmt"
	"log"
	"time"

	"github.com/bocchi-the-cache/bakemono"
)

const LOOP = 500000

func main() {
	cfg, err := bakemono.NewVolOptionsWithFileTruncate("/tmp/bakemono-test.vol", 1024*1024*1000, 1024*1024)
	if err != nil {
		panic(err)
	}
	v := bakemono.Vol{}
	corrupted, err := v.Init(cfg)
	if err != nil {
		panic(err)
	}
	if corrupted {
		log.Printf("vol is corrupted, but fixed. ignore this if first time running.")
	}

	// pad randomData with random data
	randomData := make([]byte, 1024*892)
	rand.Read(randomData)

	t := time.Now()

	for i := 0; i < LOOP; i++ {
		if i%10000 == 0 {
			log.Printf("++ set key-%d", i)
		}
		err := v.Set([]byte(fmt.Sprintf("key-%d", i)), randomData)
		if err != nil {
			panic(err)
		}

	}
	log.Printf("set LOOP keys in %s", time.Since(t))

	counter := make(map[string]int)
	t = time.Now()
	for i := 0; i < LOOP; i++ {
		if i%10000 == 0 {
			log.Printf("-- get key-%d", i)
		}
		data, err := v.Get([]byte(fmt.Sprintf("key-%d", i)))
		if err != nil {
			if err == bakemono.ErrCacheMiss {
				counter["miss"]++
				continue
			} else {
				panic(err)
			}
		}
		counter["hit"]++
		if len(data) != 1024*892 {
			panic("data length is not 1024*892")
		}
		//if string(data) != string(randomData) {
		//	panic("data is not equal")
		//}
	}
	log.Printf("get LOOP keys in %s", time.Since(t))
	log.Printf("hit: %d, miss: %d", counter["hit"], counter["miss"])
}
