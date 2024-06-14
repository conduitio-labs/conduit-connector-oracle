VERSION=$(shell git describe --tags --dirty --always)

.PHONY: build
build:
	go build -ldflags "-X 'github.com/conduitio-labs/conduit-connector-oracle.version=${VERSION}'" -o conduit-connector-oracle cmd/connector/main.go

.PHONY: test
test:
	go test $(GOTEST_FLAGS) -count=1 -race ./...

.PHONY: lint
lint:
	golangci-lint run -v

.PHONY: generate
generate: mockgen
	go generate ./...

.PHONY: mockgen
mockgen:
	mockgen -package mock -source destination/destination.go -destination destination/mock/destination.go
	mockgen -package mock -source source/source.go -destination source/mock/source.go

.PHONY: install-tools
install-tools:
	@echo Installing tools from tools.go
	@go list -e -f '{{ join .Imports "\n" }}' tools.go | xargs -tI % go install %
	@go mod tidy
