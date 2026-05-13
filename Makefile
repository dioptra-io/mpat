APP_NAME := mp
BUILD_DIR := build

GO := go
GOFMT := gofmt
GOTEST := go test
GOBUILD := go build
GORUN := go run
GOMOD := go mod
SWAG := swag

GOBIN ?= $(shell go env GOBIN)
GOPATH_BIN := $(shell go env GOPATH)/bin

ifeq ($(GOBIN),)
GOBIN := $(GOPATH_BIN)
endif

VERSION ?= dev
COMMIT := $(shell git rev-parse --short HEAD)
DATE := $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")

LDFLAGS := -ldflags "\
-X 'github.com/dioptra-io/ufuk-research/cmd.version=$(VERSION)' \
-X 'github.com/dioptra-io/ufuk-research/cmd.commit=$(COMMIT)' \
-X 'github.com/dioptra-io/ufuk-research/cmd.date=$(DATE)'"

.DEFAULT_GOAL := help

.PHONY: help
help: ## help Display this help menu
	@awk 'BEGIN { \
		FS = ":.*## "; \
		printf "Help menu for MPAT: Measurement Platform Analysis Tool\n"; \
	} \
	/^[a-zA-Z0-9_\-]+:.*## / { \
		category = $$2; \
		sub(/ .*/, "", category); \
		description = $$2; \
		sub(/^[^ ]+ /, "", description); \
		targets[category] = targets[category] sprintf("  %-15s %s\n", $$1, description); \
	} \
	END { \
		printf "\n"; \
		for (category in targets) { \
			printf "%s\n", toupper(category); \
			printf "%s", targets[category]; \
			printf "\n"; \
		} \
	}' $(MAKEFILE_LIST)

.PHONY: fmt
fmt: ## make Format Go source files
	$(GOFMT) -w .

.PHONY: tidy
tidy: ## make Clean and synchronize go.mod
	$(GOMOD) tidy

.PHONY: vet
vet: ## make Run go vet
	$(GO) vet ./...

.PHONY: lint
lint: ## make Run golangci-lint
	golangci-lint run

.PHONY: test
test: ## execution Run tests
	$(GOTEST) ./...

.PHONY: build
build: fmt vet lint swag ## execution Run checks and build the MPAT binary
	@mkdir -p $(BUILD_DIR)
	$(GOBUILD) -o $(BUILD_DIR)/$(APP_NAME) .

.PHONY: install
install: build swag ## execution Build and install binary as mp
	@mkdir -p $(GOBIN)
	cp $(BUILD_DIR)/$(APP_NAME) $(GOBIN)/mp
	@echo "Installed to $(GOBIN)/mp"
	@echo Please run "source <(mp completion zsh)" for enabling completions.

.PHONY: run
run: fmt vet lint swag ## execution Run the application
	$(GORUN) .

.PHONY: serve
serve: fmt vet lint swag ## execution Run the MPAT server
	$(GORUN) . serve

.PHONY: swag
swag: vet lint ## make Generate the swago spec
	$(SWAG) init

.PHONY: deps
deps: ## make Download dependencies
	$(GOMOD) download

.PHONY: clean
clean: ## make Remove build artifacts
	rm -rf $(BUILD_DIR)
