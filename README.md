# Replay

Replay reconstructs HTTP traffic from Envoy access logs so capacity tests can
use representative production request mixes and connection behavior. By
default, Replay also reproduces recorded per-connection timing.

## Motivation and Design Goal

Most load tests use synthetic traffic. Tools such as JMeter and Locust can
generate large volumes of traffic, but synthetic workloads often lack a
critical property: fidelity to production traffic.

Production traffic patterns are visible in proxy and sidecar logs. Replay uses
those logs to reproduce real traffic. Envoy is one of the industry's most
widely used proxies, so it is Replay's initial capture source.

Simply put, Replay reproduces traffic from Envoy access logs.

### Replay fidelity

Replay is designed to reproduce the shape of production traffic as closely as
the recorded information allows.

Packet-level capture and replay generally requires packet captures such as
PCAP, together with tooling that can reproduce them. Collecting and handling
that data can be intrusive in production. Replay instead uses Envoy access
logs, which are already available in many deployments and provide enough HTTP
and connection information for high-fidelity replay. Configuring Envoy usually does not require admin (root) permission. Hence we try to find the balance between operation easness and true fidelity.

With paired `DownstreamStart` and `DownstreamEnd` logs, Replay preserves the
observed request order within each downstream connection. Requests that shared
the same client-to-Envoy connection remain grouped together during replay, and
recorded connection-close signals are used when available. HTTP/1.1 requests
remain sequential, while HTTP/2 traffic follows the configured serialized or
multiplexed mode.

Pacing is enabled by default: Replay uses the recorded time gaps between
requests on each connection. Pacing is subject to the configured maximum delay
and time already spent sending the preceding request.

Replay's event model supports request bodies for every HTTP method. When an
event is sent, Replay decodes its valid base64 `body` field and attaches the
payload to the outbound request. This representation supports both text and
binary payloads. For body-carrying requests such as `POST` and `PUT`, fidelity
therefore depends on the recorder including the original body. If log down the request/response body has security and other concerns, users can also attach artificial body data using scripts by following the specification.

For environments where capture size matters, Replay can also use
`DownstreamEnd` logs on their own. This reduces the amount of recorded data,
with the tradeoff that Replay sees response-completion order rather than the
original request-start order and cannot determine exactly when a connection
closed.

Replay fidelity ultimately depends on what the capture contains and how Replay
is configured. Target overrides, header rewrites, method safeguards, and
retries may intentionally change the resulting traffic. Replay focuses on HTTP
behavior rather than reproducing raw network packets, header ordering, TLS
handshakes, or exact server responses.

See the [replay semantics](specs.md#4-replay-semantics) and
[non-goals](specs.md#5-non-goals) for the detailed contract.

## Sample usage

Running replay will be looking like this:

```bash
go run ./cmd/replay -log ./log.ndjson
```

A more advanced usage that can protect the replay from targeting the production domain.

```bash
go run ./cmd/replay \
  -log ./canonical.ndjson \
  --override-url http://staging.example \
  --disallow-recorded-targets
```

## Recording traffic

Before we talk about traffic capture in detail, let's firstly explain the basic conceps of different access logs types in Envoy. We start from explaining the definition of downstream.

### Downstream

Envoy commonly runs as a reverse proxy. Downstream clients send requests to
Envoy, which forwards them to configured upstream servers:

```text
clients (downstream) -> Envoy -> (upstream) servers
```

Here, *downstream* describes the traffic between the client and Envoy.

### `DownstreamStart` access log

`DownstreamStart` is emitted for each HTTP request after Envoy has evaluated
the request headers and before it runs the HTTP filter chain. It records the
start of a request lifecycle.

### `DownstreamEnd` access log

`DownstreamEnd` is emitted after the response completes or the stream
terminates. It can contain the response code, duration, response flags, and
other response metadata when those fields are included in the configured log
format.

### Combine `DownstreamStart` and `DownstreamEnd`

By combining the two types of logs, we are able to capture the full lifecycle of the requests and be able to replay the requests with high fidelity.

### Basic capture with the Envoy example

[`example/envoy-standalone-proxy-full-headers.yaml`](example/envoy-standalone-proxy-full-headers.yaml)
deploys an Envoy reverse proxy that records paired `DownstreamStart` and
`DownstreamEnd` observations. It requires a plain HTTP upstream reachable from
the Envoy pod. The example expects that upstream at `testhttp:8080`.

The example uses Kubernetes for orchestration, but Replay itself does not
require Kubernetes. It records selected request headers and response
status/flags/duration. Replay supports request and response bodies, but this
example does not capture them or response headers. Body-dependent requests such
as `POST` and `PUT` need a recorder that populates the base64 `body` field;
otherwise, requests allowed by the method safeguards are sent without their
original payload. Capturing bodies can increase log volume, may require request
buffering, and can place sensitive application data in the capture. This
example therefore supports response-status validation only.

1. Copy the manifest and change `socket_address.address` and `port_value` under
   `test_cluster` from `testhttp` and `8080` to the HTTP server being recorded.
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

7. Replay the prepared capture against an explicitly selected target:

   ```bash
   go run ./cmd/replay \
     -log ./canonical.ndjson \
     -config ./config.yaml \
     --override-url http://staging.example \
     --disallow-recorded-targets
   ```

If reducing capture volume is more important than request-start ordering,
Replay can consume raw `DownstreamEnd` NDJSON without combining it with Start
observations. Direct End input preserves End append order and cannot reconstruct
safe connection-close placement, so connections remain active until EOF. End
records may appear in response-completion order: request A can arrive before B
but finish after B, producing replay order B, A instead of A, B.

## Replay outcome and metrics

### Outcome and exit status

Replay reports `success`, `partial_success`, or `failed`. `partial_success`
returns exit code `0` by default; set
`REPLAY_PARTIAL_SUCCESS_EXIT_ZERO=false` when it must return `1`.

See the [outcome specification](specs.md#63-replay-outcome-model) for request,
connection, and run outcome definitions.

### Metrics

By default, Replay listens for Prometheus scrapes on `0.0.0.0:9102` at
`/metrics`. Scrape it locally at `http://localhost:9102/metrics`, or use the
host or container address reachable by your monitoring system. Metrics can be
disabled, and the bind address, path, namespace, common labels,
label-cardinality limits, path templates, and graceful termination period are
configurable under `metrics` in [`config.yaml`](config.yaml).

See the [metrics specification](specs.md#64-metrics-emission-and-scrape-endpoint)
for the metric catalog and exact label, path-template, and endpoint behavior.

## Replay Configuration

[`config.yaml`](config.yaml) contains a ready-to-use example for replay safety,
retry, validation, pacing, sharding, checkpoints, and metrics. Configuration
precedence is CLI flags, environment variables, YAML, then built-in defaults.

Pacing is enabled by default, with `replay.pacing.max_sleep_delta: 30s` capping
each recorded per-connection gap. Set `replay.pacing.enabled: false` in YAML
to replay without waiting for recorded timing.

Notable safety controls:

* `--dry-run` / `REPLAY_DRY_RUN`: parse input without sending requests.
* `--override-url` / `REPLAY_OVERRIDE_URL`: rewrite the target host and URL.
* `--disallow-recorded-targets` / `REPLAY_DISALLOW_RECORDED_TARGETS`: require an
  override instead of sending to captured destinations.

See the [runtime configuration specification](specs.md#65-runtime-configuration-yaml)
for the complete configuration contract and supported environment overrides.

## Operator checklist

* Run with `--dry-run` first to verify the input without sending requests.
  Dry-run also exercises recorded timing unless pacing is explicitly disabled.
* Before a live run, set `--override-url` and use
  `--disallow-recorded-targets` to prevent fallback to destinations stored in
  the capture.
* The supplied `config.yaml` blocks `POST`, `PUT`, `PATCH`, and `DELETE` unless
  the effective request contains `idempotency-key` or `x-idempotency-key`.
  Adjust `replay.idempotency` for the target's side-effect policy.
* The supplied `config.yaml` enables resumable replay with
  `replay.checkpoint.file: "./checkpoint.json"`. Reusing that file skips
  sequences already recorded as complete; remove it or choose a new path for
  an independent run. See
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

### Generating request fixtures

Run the generator on Linux or inside the VM on macOS:

```bash
go run ./tools/generate_requests.go -method POST \
  -header 'Content-Type: application/json' \
  -body '{"message":"hello"}' -out requests.ndjson
```

`-method` defaults to `GET` and applies to canonical events, `-downstream-end`,
and `-observations` output. Supplying `-body` does not change the method; choose
`POST`, `PUT`, or another method explicitly when appropriate.

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

## Go library

The `validation` package validates and summarizes Replay streams without
exposing Replay's internal event model. Summarization also validates the input,
so callers that only need totals can omit the separate validation pass.

`ValidateStream`, `SummarizeStream`, and `SummarizeStreamWithSharding` accept
either of these record families:

- **Canonical replay events**: `request` and `connection_close` records,
  normally produced by running `replay combine` on a paired Envoy capture.
- **Direct End-only Envoy access logs**: a lower-volume capture of flat records
  whose `type` is `DownstreamEnd` or omitted. Records remain in End append
  order, which may be response-completion order rather than request-start
  order.

A stream must use one family throughout. Raw paired Envoy logs containing
`DownstreamStart` and `DownstreamEnd` observations are not valid validation
streams; process them with `replay combine` first. The `format` argument selects
only the byte encoding—plain NDJSON (`FormatNDJSON`) or zstd-compressed NDJSON
(`FormatZstd`)—not the record family.

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

### Add request bodies to combined logs

Combined logs are canonical NDJSON, so you can edit them with Go's standard
`encoding/json` package without importing Replay's internal event model. This
example replaces the body of every `POST`, `PUT`, and `PATCH` request with dummy
JSON. It preserves event order, connection-close records, and other fields.
Adapt the method/path selection and payload to your application.

Save this as `add-bodies.go`:

```go
package main

import (
	"encoding/base64"
	"encoding/json"
	"io"
	"log"
	"os"
	"strings"
)

func main() {
	decoder := json.NewDecoder(os.Stdin)
	decoder.UseNumber() // Preserve integer IDs without float64 rounding.
	encoder := json.NewEncoder(os.Stdout)

	payload := []byte(`{"message":"dummy replay body"}`)
	body := map[string]any{
		"encoding":   "base64",
		"content":    base64.StdEncoding.EncodeToString(payload),
		"size_bytes": len(payload), // Decoded byte count, not base64 length.
	}

	for {
		var event map[string]any
		if err := decoder.Decode(&event); err == io.EOF {
			break
		} else if err != nil {
			log.Fatal(err)
		}

		if event["type"] == "request" {
			switch event["method"] {
			case "POST", "PUT", "PATCH":
				event["body"] = body
				headers, ok := event["headers"].(map[string]any)
				if !ok {
					headers = make(map[string]any)
				}
				// Discard metadata for the old body; Replay computes its length.
				for name := range headers {
					switch strings.ToLower(name) {
					case "content-length", "content-encoding", "content-type":
						delete(headers, name)
					}
				}
				headers["content-type"] = []string{"application/json"}
				event["headers"] = headers
			}
		}

		if err := encoder.Encode(event); err != nil {
			log.Fatal(err)
		}
	}
}
```

Run it on the **uncompressed output of `replay combine`**, writing to a new
file so the original capture is not truncated:

```bash
go run add-bodies.go < canonical.ndjson > with-bodies.ndjson
go run ./cmd/replay -log ./with-bodies.ndjson -dry-run
go run ./cmd/replay \
  -log ./with-bodies.ndjson \
  -config ./config.yaml \
  --override-url http://staging.example \
  --disallow-recorded-targets
```

Replay decodes each `body.content` from base64 and sends those bytes as the
request body. The supplied `config.yaml` still requires an idempotency key for
these methods; use keys honored by your target or deliberately adjust
`replay.idempotency` for a safe test environment. Dummy bodies do not reproduce
the original payloads and may change responses, so review any configured
response validation.
