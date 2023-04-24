# bakemono

[![Go Reference](https://pkg.go.dev/badge/github.com/bocchi-the-cache/bakemono.svg)](https://pkg.go.dev/github.com/bocchi-the-cache/bakemono)
[![Go Report Card](https://goreportcard.com/badge/github.com/bocchi-the-cache/bakemono)](https://goreportcard.com/report/github.com/bocchi-the-cache/bakemono) 
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/gojp/goreportcard/blob/master/LICENSE)
[![ci-bakemono-tests](https://github.com/bocchi-the-cache/bakemono/actions/workflows/ci-bakemono-test.yml/badge.svg)](https://github.com/bocchi-the-cache/bakemono/actions/workflows/ci-bakemono-tests.yml)
[![codecov](https://codecov.io/gh/bocchi-the-cache/bakemono/branch/main/graph/badge.svg?token=ZQZQZQZQZQ)](https://codecov.io/gh/bocchi-the-cache/bakemono)

`bakemono`is a cache storage engine implemented in Go. 

Design goals:
- **Lightweight**: easy to embed in your project
- **High-performance**: high throughput and low latency
- **Code-readable**: simple but powerful storage design, easy to read and understand

It is highly inspired by [Apache Traffic Server], implemented for our cache-proxy project [hitori].

## Cache Storage Engine
What is a **cache storage engine**? 
What is the difference from an **embeddable k-v database**?

**Similarities**:
They both are:
- key-value storage
- embeddable
- persistent storage on SSD/HDD

**Differences**:
Cache storage are:
- allowed to drop data when conditions are met
- fault-tolerant (just return a `MISS` when disk failure happens)

Cache storage is common in CDN (Content Delivery Network). It is used to cache frequently accessed data to reduce the load of backend servers.

The size of cache data is usually `~100TiB` per bare-metal server.

## Usage

### Install
You can use `bakemono` as a pkg in your project. 
```bash
go get github.com/bocchi-the-cache/bakemono
```

### Init
Then simply import and init a `Vol` in your code:
```go
func main() {
	cfg, err := bakemono.NewDefaultVolOptions("/tmp/bakemono-test.vol", 1024*512*100000, 1024*1024)
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
	
	// ...
}
```

### Read/Write
```go
func main() {
    // ...
    
    // write
    err = v.Set([]byte("key"), []byte("value"))
    if err != nil {
        panic(err)
    }
    // read
	hit, data, err := v.Get([]byte("key"))
    if err != nil {
		// note: err can be not nil when disk failure happens
		// consider it as a MISS when err != nil, or log it to do further processing
        panic(err)
    }
    if !hit {
        panic("key should be hit")
    }
    if string(data) != "value" {
        panic("value should be 'value'")
    }
    log.Printf("value: %s", data)
	
	// close
    err = v.Close()
    if err != nil {
        panic(err)
    }
}
```

### Note

**Concurrency RW is supported**.

In this version, they are sharing several RWLocks. We will give more tuning options in the future.

**We highly recommend you to read tech design doc before using it in high-load scenarios.**


## Tech Design
TBD

### Data Structure
TBD

### Read/Write
TBD

### Metadata Persistence
TBD

## Performance
TBD


## Other Information
### Roadmap
- We are working on basic caching functions in this stage.
- When caching functions are stable, we will concentrate on performance tuning.

### Name Origin
**Bakemono** is a Japanese word meaning "monster". In Chinese, it is called "贵物". 

We wish it could be a **lightweight** but **high-performance** cache storage engine like a "bakemono"!

### Who are `bocchi-the-cache`?
We are a group of engineers who are interested in storage, networking and Go programming language.

We are excited to build projects using new technologies and share our experience with others.

[Apache Traffic Server]: https://trafficserver.apache.org/
[hitori]: https://github.com/bocchi-the-cache/hitori
