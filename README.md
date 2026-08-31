# Replay Engine (MVP)

Go-based HTTP replay engine that consumes NDJSON traffic logs and exposes Prometheus metrics on `/metrics`.

## Inputs

- Recommended replay input: canonical `request`/`connection_close` NDJSON such
  as `canonical.ndjson`
- Recommended capture preparation: paired `DownstreamStart`/`DownstreamEnd`
  observations in `requests.log`, processed by `replay combine`
- Quick verification only: End-only NDJSON with explicit `DownstreamEnd` or no
  `type`
- Optional: `config.yaml` (timeouts, retry, target override, and metrics settings)

## Go library

The `validation` package validates and summarizes Replay streams without
exposing Replay's internal event model. Summarization also validates the input,
so callers that only need totals can omit the separate validation pass.

The `config` package exposes the runtime configuration schema, defaults,
parsing, loading, environment overrides, and validation for embedding
applications.

```go
package main

import (
	"bytes"

	replayconfig "github.com/reqfleet/replay/config"
	"github.com/reqfleet/replay/validation"
)

func inspect(data []byte, compressed bool) (validation.Summary, error) {
	format := validation.FormatNDJSON
	if compressed {
		format = validation.FormatZstd
	}

	if err := validation.ValidateStream(bytes.NewReader(data), format); err != nil {
		return validation.Summary{}, err
	}
	return validation.SummarizeStream(bytes.NewReader(data), format)
}

func loadConfig(path string) (replayconfig.Config, error) {
	return replayconfig.Load(path)
}
```

## Recording traffic

For replay fidelity, prepare canonical replay events from paired Envoy
access-log observations with `replay combine`. Replay also accepts End-only
completion logs directly for quick verification. The complete recording and
input contracts are defined in
[`specs.md`](specs.md#3-recording-and-replay-input).

### Basic capture with the Envoy example

[`example/envoy-standalone-proxy-full-headers.yaml`](example/envoy-standalone-proxy-full-headers.yaml)
deploys one Envoy reverse proxy and records paired `DownstreamStart` and
`DownstreamEnd` observations.

1. Copy the manifest and change the `test_cluster` endpoint from
   `testhttp:8080` to the application service being recorded.
2. Apply the manifest and wait for the proxy:

   ```bash
   kubectl apply -f ./example/envoy-standalone-proxy-full-headers.yaml
   kubectl rollout status deployment/envoy-recorder-proxy
   ```

3. Start collecting only new container logs:

   ```bash
   kubectl logs --follow deployment/envoy-recorder-proxy \
     --container envoy --tail=0 > requests.log
   ```

4. Route clients to `envoy-recorder-proxy:8080` instead of directly to the
   application. Stop the log command with `Ctrl-C` when the recording window
   ends.
5. Pair the observations into canonical replay input:

   ```bash
   go run ./cmd/replay combine \
     -log ./requests.log \
     -out ./canonical.ndjson
   ```

6. Parse the canonical NDJSON without sending requests:

   ```bash
   go run ./cmd/replay -log ./canonical.ndjson -dry-run
   ```

7. Replay the prepared capture:

   ```bash
   go run ./cmd/replay -log ./canonical.ndjson -config ./config.yaml
   ```

For a quick parser and engine check when only End records were captured, run:

```bash
go run ./cmd/replay -log ./downstream-end.ndjson -dry-run
```

Generate deterministic End-only data for local verification with:

```bash
go run ./tools/generate_requests.go \
  -base http://localhost:8080 \
  -downstream-end \
  -out ./downstream-end.ndjson
```

Direct completion input may use response-completion order. Use combined logs
when replay order matters. The example captures request headers and response
status, but not bodies or response headers, so it supports status validation
only.

Capture files can contain credentials, cookies, and personal data. Restrict
their storage and access, and redact fields before sharing them. Configuration
that drops sensitive headers during replay does not remove them from the
recorded file.

## Run

Recommended fidelity path:

```bash
go run ./cmd/replay -log ./canonical.ndjson -config ./config.yaml
```

End-only quick verification path:

```bash
go run ./cmd/replay -log ./downstream-end.ndjson -dry-run
```

**Request order is not guaranteed for direct End logs; use combined logs to
preserve replay fidelity.** If `-config` is omitted, safe defaults are used.

## Metrics

Metrics are exposed at `http://0.0.0.0:9102/metrics` by default. The endpoint,
namespace, common labels, label-cardinality limits, path templates, and graceful
termination period are configurable under `metrics` in
[`config.yaml`](config.yaml).

See the [metrics specification](specs.md#64-metrics-emission-and-scrape-endpoint)
for the metric catalog and exact label, path-template, and endpoint behavior.

## Outcome and exit status

Replay reports `success`, `partial_success`, or `failed`. `partial_success`
returns exit code `0` by default; set
`REPLAY_PARTIAL_SUCCESS_EXIT_ZERO=false` when it must return `1`.

See the [outcome specification](specs.md#63-replay-outcome-model) for request,
connection, and run outcome definitions.

## Configuration

[`config.yaml`](config.yaml) contains a ready-to-use example for replay safety,
retry, validation, pacing, sharding, checkpoints, and metrics. Configuration
precedence is CLI flags, environment variables, YAML, then built-in defaults.

Notable safety controls:

* `--dry-run` / `REPLAY_DRY_RUN`: parse input without sending requests.
* `--override-url` / `REPLAY_OVERRIDE_URL`: rewrite the target host and URL.
* `--disallow-recorded-targets` / `REPLAY_DISALLOW_RECORDED_TARGETS`: require an
  override instead of sending to captured destinations.

See the [runtime configuration specification](specs.md#65-runtime-configuration-yaml)
for the complete configuration contract and supported environment overrides.

## Operator checklist

* Run with `--dry-run` first to verify parsing and pacing without sending
  requests.
* For non-production targets, use `--override-url` and consider
  `--disallow-recorded-targets`.
* Verify the configured metrics endpoint before and during a run.
* Set `replay.checkpoint.file` for resumable runs. See
  [checkpoint persistence](specs.md#42-checkpoint-persistence) for durability
  and sharding behavior.

## Development

Replay can be developed directly on Linux with `make` and the Go version
declared in [`go.mod`](go.mod). Lima also supports Linux, but it is not required
for native Linux development.

### Linux host

Install Go and `make`, then run project targets from the repository root:

```bash
make build
make test
```

### Apple silicon macOS devbox

The repository also provides an Ubuntu VM managed by
[Lima](https://lima-vm.io/) for development on Apple silicon macOS. The bundled
configuration selects Apple's Virtualization framework and an ARM64 image, so
this devbox requires Lima 1.0 or newer and `make` on the macOS host.

From the repository root, run:

```bash
make devbox-ssh
```

This creates or starts the Lima instance named `replay`, mounts the current
checkout read-write at `/workspace/replay`, installs the Go version declared in
`go.mod` through GVM, and opens a shell in that directory. The initial start
downloads and provisions the VM, so it takes longer than subsequent starts.

Run project `make` targets and Go commands from this VM shell, not directly from
the macOS host:

```bash
# VM: /workspace/replay
make build
make test
```

Editing files and running repository commands such as `git` may still be done
on the host because the checkout is shared with the VM. Exit the shell with
`exit`; the VM continues running.

### Tests and checks

Run these commands directly on Linux or inside the VM on macOS:

| Command | Purpose |
| --- | --- |
| `make staticcheck` | Run Staticcheck across all Go packages. |
| `make test` | Run Staticcheck, then all Go tests with the test cache disabled. |
| `go test ./internal/parser -count=1` | Run one package while developing. |
| `make e2e` | Build Replay and exercise the bundled fixtures against a local test server. |
| `make alltests` | Run `make test` and `make e2e`; this is the CI check. |
| `make build` | Build `bin/replay` for the current OS and architecture. |
| `make tidy` | Update `go.mod` and `go.sum` after dependency changes. |

### VM lifecycle

Run lifecycle commands from the macOS host that owns the VM:

| Command | Purpose |
| --- | --- |
| `make devbox` | Create or start the VM without opening a shell. |
| `make devbox-ssh` | Create or start the VM and open a shell in `/workspace/replay`. |
| `make devbox-stop` | Stop the VM without deleting it. |
| `make devbox-recreate` | Delete and reprovision the VM. Host checkout files remain; guest-local data is removed. |

The current checkout is mounted by default. To mount a different checkout,
pass its absolute path while recreating the VM:

```bash
make DEVBOX_PROJECT_DIR=/absolute/path/to/replay devbox-recreate
```

Recreate the VM after changing `lima.yaml` or `DEVBOX_PROJECT_DIR`; an existing
instance retains the configuration and mount selected when it was created.
