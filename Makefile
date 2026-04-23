GO ?= go
BINARY ?= replay
BIN_DIR ?= bin
PKG ?= ./...
LOG ?= requests.log

.PHONY: test build tidy run run-sample clean docker-build

test:
	$(GO) test $(PKG) -count=1

build:
	mkdir -p $(BIN_DIR)
	$(GO) build -o $(BIN_DIR)/$(BINARY) ./cmd/replay

tidy:
	$(GO) mod tidy

run:
	$(GO) run ./cmd/replay

run-sample:
	$(GO) run ./cmd/replay -log ./$(LOG) -config ./config.yaml

clean:
	rm -rf $(BIN_DIR)

docker-build:
	docker build -t $(BINARY) .
