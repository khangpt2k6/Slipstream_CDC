# CDC pipeline task runner.

GO       ?= go
BIN_DIR  ?= bin

.PHONY: all build test lint run tidy clean proto

all: build test lint

## build: compile all packages and the worker binary
build:
	$(GO) build ./...
	$(GO) build -o $(BIN_DIR)/worker ./cmd/worker

## test: run the unit test suite
test:
	$(GO) test ./...

## lint: run go vet and golangci-lint
lint:
	$(GO) vet ./...
	golangci-lint run

## run: run the worker against the local stack
run:
	$(GO) run ./cmd/worker

## proto: lint the protobuf API and regenerate internal/gen
proto:
	buf lint
	buf generate

## tidy: tidy and verify the module graph
tidy:
	$(GO) mod tidy

## clean: remove build artifacts
clean:
	rm -rf $(BIN_DIR)
