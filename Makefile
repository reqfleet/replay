GO ?= go
BINARY ?= replay
BIN_DIR ?= bin
IMG ?= $(BINARY)
PKG ?= ./...
LOG ?= requests.log

# Target OS and architecture for build
GOOS ?= $(shell $(GO) env GOOS)
GOARCH ?= $(shell $(GO) env GOARCH)

.PHONY: test build tidy run run-sample clean docker-build e2e e2e-server-start e2e-server-stop alltests staticcheck

test: staticcheck
	$(GO) test $(PKG) -count=1

staticcheck:
	$(GO) tool staticcheck ./...

e2e-server-start:
	@echo "Starting test server on localhost:6000..."
	@mkdir -p $(BIN_DIR)
	@$(GO) build -o $(BIN_DIR)/e2e_test_server ./e2e/test_server.go
	@$(BIN_DIR)/e2e_test_server > /dev/null 2>&1 & echo $$! > e2e_server.pid
	@for i in $$(seq 1 30); do nc -z localhost 6000 && echo "Test server started." && exit 0 || sleep 0.1; done; echo "Test server failed to start."; exit 1

e2e-server-stop:
	@if [ -f e2e_server.pid ]; then \
		echo "Stopping test server..."; \
		kill $$(cat e2e_server.pid) || true; \
		rm -f e2e_server.pid $(BIN_DIR)/e2e_test_server; \
	else \
		echo "No test server running."; \
	fi

e2e: e2e-server-start
	@echo "Running e2e tests..."
	@$(GO) run ./cmd/replay -log e2e/requests-ndjson.log -verbose || ($(MAKE) e2e-server-stop; exit 1)
	@echo "Running e2e tests with gzip..."
	@$(GO) run ./cmd/replay -log e2e/requests-ndjson.log.gz -gzip -verbose || ($(MAKE) e2e-server-stop; exit 1)
	@echo "Running e2e tests with zstd..."
	@$(GO) run ./cmd/replay -log e2e/requests-ndjson.log.zst -zstd -verbose || ($(MAKE) e2e-server-stop; exit 1)
	@$(MAKE) e2e-server-stop
	@echo "e2e tests passed."
alltests: test e2e

build:
	mkdir -p $(BIN_DIR)
	CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) $(GO) build -o $(BIN_DIR)/$(BINARY) ./cmd/replay

tidy:
	$(GO) mod tidy

run:
	$(GO) run ./cmd/replay

run-sample:
	$(GO) run ./cmd/replay -log ./$(LOG) -config ./config.yaml

clean:
	rm -rf $(BIN_DIR)

docker-build:
	$(MAKE) build GOOS=linux
	docker build --build-arg BIN_DIR=$(BIN_DIR) --build-arg BINARY=$(BINARY) -t $(IMG) .
