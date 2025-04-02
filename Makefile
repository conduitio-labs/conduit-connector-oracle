VERSION=$(shell git describe --tags --dirty --always)

.PHONY: build
build:
	go build -ldflags "-X 'github.com/conduitio-labs/conduit-connector-oracle.version=${VERSION}'" -o conduit-connector-oracle cmd/connector/main.go

.PHONY: test
test:
	docker compose -f test/docker-compose.yml up --quiet-pull -d
	@echo "Waiting for Oracle to become healthy..."
	@while [ "$$(docker inspect -f '{{.State.Health.Status}}' test-oracle-1)" != "healthy" ]; do \
		echo "Waiting..."; \
		sleep 5; \
	done
	@echo "Oracle is healthy. Running tests..."
	go test $(GOTEST_FLAGS) -race ./...; ret=$$?; \
		docker compose -f test/docker-compose.yml down --volumes; \
		exit $$ret
	
.PHONY: generate
generate:
	go generate ./...

.PHONY: install-tools
install-tools:
	@echo Installing tools from tools/go.mod
	@go list -modfile=tools/go.mod tool | xargs -I % go list -modfile=tools/go.mod -f "%@{{.Module.Version}}" % | xargs -tI % go install %
	@go mod tidy

.PHONY: fmt
fmt:
	gofumpt -l -w .

.PHONY: lint
lint:
	golangci-lint run
