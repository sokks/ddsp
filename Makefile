GOPATH = $(shell pwd)

ROUTER   = router/router
NODE     = node/node
FRONTEND = frontend/frontend

STORAGE_PB = src/storage/pb
ROUTER_PB  = src/router/pb

all: build

gen:
	protoc -I $(STORAGE_PB) $(STORAGE_PB)/pb.proto --go_out=plugins=grpc:$(STORAGE_PB)
	protoc -I $(ROUTER_PB) $(ROUTER_PB)/pb.proto --go_out=plugins=grpc:$(ROUTER_PB)

build: gen
	go install node
	go install router
	go install frontend
	go install clikv

clean:
	find src -name 'pb.pb.go' -delete
	rm -rf bin/*

test-node:
	GOPATH="$(GOPATH)" go test $(NODE) -count=1
	GOPATH="$(GOPATH)" go test $(NODE) -count=1 -race

test-router:
	GOPATH="$(GOPATH)" go test $(ROUTER) -count=1
	GOPATH="$(GOPATH)" go test $(ROUTER) -count=1 -race

test-fe:
	GOPATH="$(GOPATH)" go test $(FRONTEND) -count=1
	GOPATH="$(GOPATH)" go test $(FRONTEND) -count=1 -race

test-integration:
	GOPATH="$(GOPATH)" go test integration_test -count=1

test: test-node test-router test-fe test-integration


.PHONY: build clean gen test test-node test-router test-fe
