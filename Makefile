.PHONY: build clean

VERSION=0.0.1
BIN=bakemono
DIR_SRC=./
DOCKER_CMD=docker

GO_ENV=CGO_ENABLED=0
Revision=$(shell git rev-parse --short HEAD)
GO_FLAGS=-ldflags="-X main.Version=$(VERSION) -X 'main.Revision=$(Revision)' -X 'main.Time=`date`' -extldflags -static"
GO=$(GO_ENV) $(shell which go)

build:
	@$(GO_ENV) $(GO) build $(GO_FLAGS) -o $(BIN) $(DIR_SRC)

universal:
	@GOOS=darwin GOARCH=amd64 $(GO_ENV) $(GO) build $(GO_FLAGS) -o ${BIN}_amd64 $(DIR_SRC)
	@GOOS=darwin GOARCH=arm64 $(GO_ENV) $(GO) build $(GO_FLAGS) -o ${BIN}_arm64 $(DIR_SRC)
	@lipo -create -output ${BIN} ${BIN}_amd64 ${BIN}_arm64
	@rm -f ${BIN}_amd64 ${BIN}_arm64

test:
	@$(GO) test ./...

clean:
	@$(GO) clean ./...
	@rm -f $(BIN)

all: clean build