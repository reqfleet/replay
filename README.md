# Replay Engine (MVP)

Go-based HTTP replay engine that consumes NDJSON traffic logs and exposes Prometheus metrics on `/metrics`.

## Inputs

- Required: `requests.log` (NDJSON traffic file)
- Optional: `config.yaml` (timeouts, retry, target override, and metrics settings)

## Development

The development environment is an Ubuntu VM managed by
[Lima](https://lima-vm.io/). The bundled configuration uses Apple's
Virtualization framework and an ARM64 image, so it requires an Apple silicon
Mac, Lima 1.0 or newer, and `make` on the host.

### Start the VM and connect

From the repository root on the macOS host, run:

```bash
make devbox-ssh
```

This creates or starts the Lima instance named `replay`, mounts the current
checkout read-write at `/workspace/replay`, installs the Go version declared in
`go.mod` through GVM, and opens a shell in that directory. The initial start
downloads and provisions the VM, so it takes longer than subsequent starts.

Run project `make` targets and Go commands from this VM shell, not from the
macOS host:

```bash
# VM: /workspace/replay
make build
make test
```

Editing files and running repository commands such as `git` may still be done
on the host because the checkout is shared with the VM. Exit the shell with
`exit`; the VM continues running.

### Tests and checks

Run these commands inside the VM:

| Command | Purpose |
| --- | --- |
| `make staticcheck` | Run Staticcheck across all Go packages. |
| `make test` | Run Staticcheck, then all Go tests with the test cache disabled. |
| `go test ./internal/parser -count=1` | Run one package while developing. |
| `make e2e` | Build Replay and exercise the bundled fixtures against a local test server. |
| `make alltests` | Run `make test` and `make e2e`; this is the CI check. |
| `make build` | Build `bin/replay` for the VM's OS and architecture. |
| `make tidy` | Update `go.mod` and `go.sum` after dependency changes. |

### VM lifecycle

Run lifecycle commands from the macOS host:

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

## Recording traffic

Replay consumes canonical replay events prepared from paired Envoy access-log
observations. The bundled example shows the complete Kubernetes capture and
preprocessing workflow; [`specs.md`](specs.md#3-recording-and-replay-input)
defines both wire schemas.

### Basic capture with the Envoy example

[`example/envoy-standalone-proxy-full-headers.yaml`](example/envoy-standalone-proxy-full-headers.yaml)
deploys one Envoy reverse proxy. Its stdout contains a `DownstreamStart` and a
`DownstreamEnd` observation for every request, correlated by Envoy-generated
`request_id`.

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
   ends. Requests that bypass the proxy are not recorded.
5. Pair the mixed observations into canonical replay input:

   ```bash
   go run ./cmd/replay combine \
     -log ./requests.log \
     -out ./canonical.ndjson
   ```

6. Inspect the canonical NDJSON, then parse it without sending requests:

   ```bash
   go run ./cmd/replay -log ./canonical.ndjson -dry-run
   ```

7. Replay the prepared capture:

   ```bash
   go run ./cmd/replay -log ./canonical.ndjson -config ./config.yaml
   ```

`kubectl logs` can combine access logs with Envoy process logs. `combine`
ignores blank lines and lines that do not begin with `{`; a malformed JSON line
that does begin with `{` is fatal. `-gzip` and `-zstd` select compressed input.
Combined output is always plain NDJSON and is installed atomically only after
every Start/End pair validates.

The example captures request headers and the response status, but not request
or response bodies or response headers. Consequently, it supports status
validation but not recorded response-header or body validation.

### Capture log conformance

Raw observations from another Envoy configuration or capture tool must follow
the [recorder observation schema](specs.md#31-raw-recorder-observations):

- UTF-8 NDJSON containing exactly one explicit `DownstreamStart` and one
  explicit `DownstreamEnd` per original request.
- A nonempty, non-`-` `request_id` shared by the pair. `node`,
  `connection_id`, request descriptors, and start timestamp must also agree.
- `response_code` and Envoy's string `response_flags` on the End observation.
  `-` and an empty flag string mean no flags; other values are comma-separated
  exact tokens.

`replay -log` accepts only explicit canonical `request` and
`connection_close` events. Raw Start-only, End-only, omitted-type, and mixed
Envoy observation files are invalid engine input and must pass through
`replay combine`.

Canonical request records contain a nonempty `request_id`, `connection_id`,
`timestamp`, `method`, `authority`, `path`, `protocol`, and `response_code`.
Header values and `response_flags` are arrays of strings. `scheme`, `node`, and
`stream_id` are optional. Replay assigns `sequence` independently for each
`node` + `connection_id` when it is absent; supplied values must not decrease.
For HTTP/1.1, `stream_id` defaults to `1`, and any other value is rejected.

Capture files can contain credentials, cookies, and personal data. Restrict
their storage and access, and redact fields before sharing them. The sample
`config.yaml` drops `authorization` and `cookie` when replaying; it does not
remove those values from an already recorded file.

## Run

```bash
go run ./cmd/replay -log ./canonical.ndjson -config ./config.yaml
```

If `-config` is omitted, safe defaults are used.

## Metrics

By default, metrics are exposed at:

- `http://0.0.0.0:9102/metrics`

Metric names with the default `replay` namespace:

- `replay_latency_label_milliseconds`
- `replay_latency_label_milliseconds_bucket`
- `replay_latency_label_milliseconds_sum`
- `replay_latency_label_milliseconds_count`
- `replay_status_counter`
- `replay_egress_bytes_counter`
- `replay_threads_gauge`
- `replay_cpu_gauge`
- `replay_mem_gauge`

`replay_threads_gauge` reflects active virtual users. It increments when a replay
worker starts and decrements when that worker finishes.

`metrics.path_templates` reduces Prometheus label cardinality by replacing dynamic
request paths with configured templates. A path segment enclosed in braces is a
wildcard: with `/users/{id}`, a request to `/users/123` emits `/users/{id}` as
the metric `label`. Templates must have the same number of segments as the
request and match every literal segment; the first matching template wins.
Query strings are excluded before matching, and unmatched paths retain their
original path label.

Templates are validated when configuration loads. Each template must be an
absolute path with at most 64 non-empty segments and no trailing slash, dot
segments, query, or fragment. Wildcards must occupy an entire segment and use
a name of at most 64 bytes matching `[A-Za-z_][A-Za-z0-9_]*`; literal segments
must use RFC 3986 path characters, with non-ASCII bytes percent-encoded.
Duplicate templates are rejected. A configuration may contain at most 256
templates, 2 KiB each and 64 KiB in total.

## Outcome model

- Run outcomes: `success`, `partial_success`, `failed`
- Exit code:
  - `0` for `success`
  - `1` for `failed`
  - `0` for `partial_success` by default
  - set `REPLAY_PARTIAL_SUCCESS_EXIT_ZERO=false` to force `1` on `partial_success`

`replay_status_counter` includes both numeric HTTP response codes and synthetic transport statuses such as `timeout`, `connection_refused`, `connection_reset`, `tls`, `network`, and `send_error` when a request fails before any response is received.

## CLI & environment precedence

Configuration precedence (highest → lowest): CLI flags > environment variables > YAML config > defaults.

Notable CLI flags (also available via env):
- `--dry-run` / `REPLAY_DRY_RUN`: do not send network requests.
- `--override-url` / `REPLAY_OVERRIDE_URL`: rewrite target host and URL.
- `--disallow-recorded-targets` / `REPLAY_DISALLOW_RECORDED_TARGETS`: fail if no override is configured.
- `--config` path: YAML config file (loaded before env and CLI overrides).

Environment variables for metrics:
- `METRICS_ENABLED`, `METRICS_NAMESPACE`, `METRICS_LISTEN_ADDRESS`, `METRICS_PATH`, `METRICS_GRACEFUL_TERMINATION_PERIOD`

`metrics.graceful_termination_period` keeps the endpoint scrapeable for the full configured window after replay stops, including signal cancellation, then gracefully drains in-flight scrapes. The default is `5s`. Replay fails startup if the metrics listener cannot bind.

Common Prometheus labels are configured with `metrics.common_labels`:
- each entry has `name`, literal fallback `value`, and optional `env`
- if the configured env var is unset or empty, replay falls back to the literal `value`
- replay resolves these env-ref keys from the process environment

CLI flags are applied last and take highest precedence for safety-related settings.

## Operator checklist (safe run)

- Always run with `--dry-run` first to confirm traffic parsing and pacing without emitting network requests.
- When replaying against non-production targets, use `--override-url` and consider setting `--disallow-recorded-targets` in operator configs to prevent accidental traffic to recorded destinations.
- Verify the metrics endpoint (default `http://0.0.0.0:9102/metrics`) is reachable before and during runs.
- If you need resumable runs, set `checkpoint.file` in the YAML. Acknowledged sequences update in-memory progress per `node` + `connection_id`; dirty progress is durably synced at `checkpoint.sync_interval` (default `1s`) and flushed on orderly shutdown. An abrupt process or host failure can lose up to approximately one configured interval of progress, and persistence failures fail the run when observed or during shutdown.

## Programmatic outcome details

In addition to the summary printed to stdout, the engine maintains aggregate outcome types for programmatic consumption:

- `ConnectionResult` — fields: `node`, `connection_id`, `outcome` (`completed`/`aborted`), plus per-connection aggregate counters (`requests_sent`, `responses_received`, `send_errors`, `validation_failed`, `skipped`).
- `RequestResult` — retained as an API type for future bounded detail output; `Summary.RequestResults` and `ConnectionResult.Requests` are not populated during normal replay so large captures do not retain one result per request.

## Example config (replay/config.yaml)

See `config.yaml` in this directory for a ready-to-use example demonstrating safe defaults, metrics, and override usage.

## Canonical replay input

Replay accepts one canonical JSON object per line. `type` is required and must
be the exact, case-sensitive value `request` or `connection_close`.

A `request` requires `request_id`, `connection_id`, `timestamp`, `method`,
`authority`, `path`, `protocol`, and `response_code`. `response_flags`, when
present, must be an array of strings. A `connection_close` requires only
`connection_id`; `node` is optional and participates in the connection key.
Close events do not advance request sequence.

Malformed JSON, omitted types, raw `DownstreamStart` or `DownstreamEnd`
observations, `connection_open`, and every unknown type are rejected with the
input line number. Use `replay combine` to prepare raw Envoy observations.

## Retry and validation config

Use `config.yaml` to control retry, response validation, pacing, and idempotency safeguards.
Each `validation.status`, `validation.headers`, and `validation.body` field
directly enables that check; there is no aggregate validation toggle.
On canonical `request` events, `response_code`, `response_headers`, and
`response_body` provide response-validation expectations.

```yaml
replay:
  rampup_duration: 0s  # 0s disables ramp-up; 30s stages VUs over 30 seconds
  http2:
    mode: serialized  # serialized|multiplexed
  retry:
    max_attempts: 2
    backoff: exponential  # none|fixed|exponential
    retry_on_statuses: [429, 502, 503, 504]
    retry_on_errors: [timeout, connection_reset, network, tls]
  validation:
    status: true
    headers: false
    body: false
    ignore_headers: [x-request-id, date]
  pacing:
    enabled: false
    max_sleep_delta: 30s
  idempotency:
    enabled: true
    block_methods: [POST, PUT, PATCH, DELETE]
    require_header_for_allow: [idempotency-key, x-idempotency-key]
  sharding:
    shard_index: 0
    shard_count: 1
  checkpoint:
    file: "./checkpoint.json"
metrics:
  namespace: replay
  path_templates:
    - /users/{id}
    - /users/{id}/orders
  common_labels:
    - name: run_id
      value: unknown
      env: REPLAY_RUN_ID
    - name: worker_id
      value: "0"
      env: REPLAY_WORKER_ID
    - name: zone
      value: unknown
      env: REPLAY_ZONE
```

With pacing enabled, Replay maps increasing recorded timestamp deltas onto a
per-connection replay deadline. Time spent waiting for the preceding response
counts toward the next delta; Replay sleeps only for any remaining time instead
of adding the full recorded delta after the response completes.

When a canonical `request` contains captured response expectations,
enabled-check mismatches increment `validation_failed` and produce
`partial_success`.

When the target is unreachable or times out, the request is recorded as `send_error`, the run remains `partial_success`, and `replay_status_counter` is incremented with a synthetic transport status.

When idempotency safeguards are enabled, mutation methods are skipped unless one of the allow headers is present.
The combiner converts an exact `DC` response-flag token into one
`connection_close` after that connection's final Start-ordered request. Replay
waits for in-flight HTTP/2 work, closes the transport, and finalizes the
connection at that marker. Connections without `DC` retain their sequence and
transport state until EOF, where Replay finalizes them.

Per-instance memory therefore scales with the number of unique connection
identities assigned to that instance. For `N` replay processes, point every
process at the same complete capture, configure the same `shard_count: N`, and
give each process a unique `shard_index` in `[0, N)`. Replay hashes `node` +
`connection_id`, so one process handles every record for an identity in file
order. Each process still scans the complete input; sharding partitions replay
execution and retained state, not input-read I/O.

Do not split a connection using arbitrary byte offsets, line ranges, or
timestamp windows. Sharded checkpoint files are isolated with a
`.shard-<index>-of-<count>` suffix.

If `checkpoint.file` is set, completed sequences update an in-memory watermark
and are persisted at `checkpoint.sync_interval` (default `1s`). Orderly
shutdown flushes the latest watermark; after an abrupt termination, replay can
repeat requests completed since the last successful sync. Persisted sequences
are skipped on the next run using the same `node` + `connection_id` identity.

For HTTP/2 traffic, `serialized` replays requests sequentially, while
`multiplexed` dispatches requests concurrently on the shared per-connection
client and joins in-flight requests at `connection_close` or EOF. Canonical
records produced by `combine` are globally ordered by `DownstreamStart`
observation order, not response-completion order. Replay consumes that append
order and does not reorder by `timestamp`, `stream_id`, or `sequence`.
