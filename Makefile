.PHONY: count-go-lines count-total-go-lines test build help check-build check-vet check-all

VERSION ?= $(shell git describe --tags --abbrev=0 2>/dev/null || echo "v0.0.0")
GIT_COMMIT := $(shell git rev-parse --short HEAD)
BUILD_DATE := $(shell date -u '+%Y-%m-%dT%H:%M:%SZ')
GO_VERSION := $(shell go version | awk '{print $$3}')

help: ## Display this help message
	@echo "Makefile for MPAT (Measurement Platform Analysis Tool), the targets are listed below:\\n"
	@awk 'BEGIN {FS = ":.*?## "; OFS = " : ";} \
		/^[a-zA-Z_-]+:.*?##/ { \
			printf "\033[36m%-22s\033[0m%s\n", $$1, $$2; \
		}' $(MAKEFILE_LIST)
	@echo "\\n"

build: ## Compile the executable under build/mpat
	go build -ldflags "-X main.Version=$(VERSION) \
		-X main.GitCommit=$(GIT_COMMIT) \
		-X main.BuildDate=$(BUILD_DATE) \
		-X main.GoVersion=$(GO_VERSION)" \
		-o build/mpat cmd/mpat/main.go

test: ## Run go test for all of the packages
	go test ./...

check-build: ## Check for compilation issues
	go build ./...

vet: ## Check for linting issues
	go vet ./...

check-all: check-vet check-build ## Check for building and linting issues

clean: ## Run go clean command, also removes the mod cache
	go clean --modcache

count-go-lines: ## Return the number of lines for each go file
	@find . -name "*.go" -exec wc -l {} +

count-total-go-lines: ## Return the total number of lines written in go
	@make count-go-lines | grep -e total | awk '{print $$1}'

version: ## Get the version the binary will be compiled to
	@echo $(VERSION)
