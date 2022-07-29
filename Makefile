.PHONY: build test

build:
	go build -o conduit-connector-oracle cmd/oracle/main.go

test:
	go test $(GOTEST_FLAGS) -race ./...

lint:
	golangci-lint run -c .golangci.yml --go=1.18

dep:
	go mod download
	go mod tidy

mockgen:
	mockgen -package mock -source destination/destination.go -destination destination/mock/destination.go