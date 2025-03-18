.PHONY: count-go-lines count-total-go-lines test build

build:
	go build -o mpat cmd/mpat/main.go

test:
	go test ./...

clean:
	go clean --modcache

count-go-lines:
	@find . -name "*.go" -exec wc -l {} +

count-total-go-lines:
	@make count-go-lines | grep -e total | awk '{print $$1}'
