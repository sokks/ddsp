GOPATH = $(shell pwd)

ROUTER   = router/router
NODE     = node/node
FRONTEND = frontend/frontend

STORAGE_PB = src/storage/pb
ROUTER_PB  = src/router/pb
STORAGE_PB_FILE = $(STORAGE_PB)/pb.pb.go
ROUTER_PB_FILE  = $(ROUTER_PB)/pb.pb.go

all: build

$(STORAGE_PB_FILE):
	protoc -I $(STORAGE_PB) $(STORAGE_PB)/pb.proto --go_out=plugins=grpc:$(STORAGE_PB)

$(ROUTER_PB_FILE):
	protoc -I $(ROUTER_PB) $(ROUTER_PB)/pb.proto --go_out=plugins=grpc:$(ROUTER_PB)

gen: $(STORAGE_PB_FILE) $(ROUTER_PB_FILE)

build: gen
	GOPATH="$(GOPATH)" go install node
	GOPATH="$(GOPATH)" go install router
	GOPATH="$(GOPATH)" go install frontend
	GOPATH="$(GOPATH)" go install clikv

clean:
	find src -name 'pb.pb.go' -delete
	rm -rf bin/*

test-node:
	GOPATH="$(GOPATH)" go test $(NODE) -count=1 -v
	GOPATH="$(GOPATH)" go test $(NODE) -count=1 -race -v

test-router:
	GOPATH="$(GOPATH)" go test $(ROUTER) -count=1 -v
	GOPATH="$(GOPATH)" go test $(ROUTER) -count=1 -race -v

test-fe:
	GOPATH="$(GOPATH)" go test $(FRONTEND) -count=1 -v
	GOPATH="$(GOPATH)" go test $(FRONTEND) -count=1 -race -v

test-integration:
	GOPATH="$(GOPATH)" go test integration_test -count=1 -v

test: test-node test-router test-fe test-integration


.PHONY: build clean gen test test-node test-router test-fe test-integration
