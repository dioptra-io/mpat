.PHONY: count-go-lines count-total-go-lines test build help check-build check-vet check-all

# Version is received as an environment value
VERSION=$(shell git describe --tags --always)

help: ## Display this help message
	@echo "Makefile for MPAT (Measurement Platform Analysis Tool), the targets are listed below:\\n"
	@awk 'BEGIN {FS = ":.*?## "; OFS = " : ";} \
		/^[a-zA-Z_-]+:.*?##/ { \
			printf "\033[36m%-22s\033[0m%s\n", $$1, $$2; \
		}' $(MAKEFILE_LIST)
	@echo "\\n"

build: ## Compile the executable under build/mpat
	go build -ldflags "-X dioptra-io/ufuk-research/cmd/mpat.Version=8cc896c" -o build/mpat cmd/mpat/main.go

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
