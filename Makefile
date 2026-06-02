.PHONY: build test vet clean install help

VERSION ?= $(shell git describe --tags --abbrev=0 2>/dev/null || echo "v0.0.0")
GIT_COMMIT := $(shell git rev-parse --short HEAD)

help:
	@echo "Valid targets:"
	@echo "  build   - Build the mpat binary"
	@echo "  test    - Run all tests"
	@echo "  vet     - Run go vet"
	@echo "  clean   - Remove build artifacts"
	@echo "  install - Install binary to GOPATH/bin"

build:
	go build -ldflags "-X main.Version=$(VERSION) -X main.GitCommit=$(GIT_COMMIT)" \
		-o mp main.go

test:
	go test ./...

vet:
	go vet ./...

clean:
	rm -f mp

install: build
	install ./mp "$(shell go env GOPATH)/bin/mp"
