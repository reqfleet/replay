GO ?= go
BINARY ?= replay
BIN_DIR ?= bin
IMG ?= $(BINARY)
PKG ?= ./...
LOG ?= requests.log
DEVBOX_PROJECT_DIR ?= $(CURDIR)
E2E_REPLAY := $(BIN_DIR)/e2e_replay
E2E_GENERATED_LOG := $(BIN_DIR)/e2e-generated-body.ndjson

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
		rm -f e2e_server.pid $(BIN_DIR)/e2e_test_server $(E2E_REPLAY) $(E2E_GENERATED_LOG); \
	else \
		echo "No test server running."; \
	fi

e2e: e2e-server-start
	@$(GO) build -o $(E2E_REPLAY) ./cmd/replay || ($(MAKE) e2e-server-stop; exit 1)
	@echo "Running e2e tests..."
	@$(E2E_REPLAY) -log e2e/requests-ndjson.log -verbose || ($(MAKE) e2e-server-stop; exit 1)
	@echo "Running e2e tests with gzip..."
	@$(E2E_REPLAY) -log e2e/requests-ndjson.log.gz -gzip -verbose || ($(MAKE) e2e-server-stop; exit 1)
	@echo "Running e2e tests with zstd..."
	@$(E2E_REPLAY) -log e2e/requests-ndjson.log.zst -zstd -verbose || ($(MAKE) e2e-server-stop; exit 1)
	@echo "Running generated downstream-end request body e2e test..."
	@$(GO) run ./tools/generate_requests.go \
		-base http://localhost:6000 \
		-subpath e2e/request-body \
		-reqs 1 \
		-conns 1 \
		-status 200 \
		-header 'Content-Type: application/json' \
		-header 'X-E2E-Trace: one' \
		-header 'X-E2E-Trace: two' \
		-body '{"message":"hello"}' \
		-out $(E2E_GENERATED_LOG) || ($(MAKE) e2e-server-stop; exit 1)
	@$(E2E_REPLAY) -config e2e/response-validation.yaml -log $(E2E_GENERATED_LOG) -verbose || ($(MAKE) e2e-server-stop; exit 1)
	@echo "Running matching status, body, and header validation e2e test..."
	@$(E2E_REPLAY) -config e2e/response-validation.yaml -log e2e/response-validation-match.ndjson -verbose || ($(MAKE) e2e-server-stop; exit 1)
	@echo "Running mismatched response body validation e2e test..."
	@$(E2E_REPLAY) -config e2e/response-validation.yaml -log e2e/response-validation-body-mismatch.ndjson -verbose; status=$$?; \
		if [ $$status -ne 1 ]; then \
			echo "Mismatched response body exit code = $$status, want 1"; \
			$(MAKE) e2e-server-stop; \
			exit 1; \
		fi
	@echo "Running mismatched response header validation e2e test..."
	@$(E2E_REPLAY) -config e2e/response-validation.yaml -log e2e/response-validation-header-mismatch.ndjson -verbose; status=$$?; \
		if [ $$status -ne 1 ]; then \
			echo "Mismatched response header exit code = $$status, want 1"; \
			$(MAKE) e2e-server-stop; \
			exit 1; \
		fi
	@echo "Running mismatched response status validation e2e test..."
	@$(E2E_REPLAY) -config e2e/response-validation.yaml -log e2e/response-validation-status-mismatch.ndjson -verbose; status=$$?; \
		if [ $$status -ne 1 ]; then \
			echo "Mismatched response status exit code = $$status, want 1"; \
			$(MAKE) e2e-server-stop; \
			exit 1; \
		fi
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

.PHONY: devbox
devbox:
	@if limactl list replay --format '{{.Name}}' >/dev/null 2>&1; then \
		limactl start replay; \
	else \
		DEVBOX_PROJECT_DIR="$(DEVBOX_PROJECT_DIR)" \
			limactl start --name=replay \
			--set='.param.projectDir = env(DEVBOX_PROJECT_DIR)' lima.yaml; \
	fi

.PHONY: devbox-ssh
devbox-ssh: devbox
	limactl shell --workdir /workspace/replay replay

.PHONY: devbox-stop
devbox-stop:
	@if status="$$(limactl list replay --format '{{.Status}}' 2>/dev/null)"; then \
		if [ "$$status" = "Stopped" ]; then \
			echo 'Lima instance "replay" is already stopped.'; \
		else \
			limactl stop replay; \
		fi; \
	else \
		echo 'Lima instance "replay" does not exist.'; \
	fi
