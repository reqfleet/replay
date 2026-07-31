# Replay Engine (MVP)

Go-based HTTP replay engine that consumes NDJSON traffic logs and exposes Prometheus metrics on `/metrics`.

## Inputs

- Required: `requests.log` (NDJSON traffic file)
- Optional: `config.yaml` (timeouts, retry, target override, and metrics settings)

## Run

```bash
go run ./cmd/replay -log ./requests.log -config ./config.yaml
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

## Retry and validation config

Use `config.yaml` to control retry, response validation, pacing, lifecycle, and idempotency safeguards.
Each `validation.status`, `validation.headers`, and `validation.body` field
directly enables that check; there is no aggregate validation toggle.
Validation expectations are authoritative fields on `DownstreamEnd` request
events: `status`, `response_headers`, and `response_body`. Replay validates the
target response immediately and rejects separate `response` events.

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
  lifecycle:
    require_open: true
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

When a recorded response exists for a request (`connection_id` + `sequence`), mismatches increment `validation_failed` and produce `partial_success`.

When the target is unreachable or times out, the request is recorded as `send_error`, the run remains `partial_success`, and `replay_status_counter` is incremented with a synthetic transport status.

When idempotency safeguards are enabled, mutation methods are skipped unless one of the allow headers is present.
Lifecycle checks require `connection_open` before each replayed connection. `connection_close` is optional; EOF finalizes any still-open connections.
Round-robin workers may drive multiple recorded connections. Each recorded connection owns its HTTP transport until `connection_close` or EOF, preserving keep-alive reuse and socket isolation; `max_virtual_users_per_engine` bounds the worker count.

Sharding routes connections by `node` + `connection_id` hash (`shard_index` / `shard_count`) and only replays the local shard. With multiple shards, replay isolates checkpoint files using a `.shard-<index>-of-<count>` suffix.
If `checkpoint.file` is set, completed sequences update an in-memory watermark and are persisted at `checkpoint.sync_interval` (default `1s`). Orderly shutdown flushes the latest watermark; after an abrupt termination, replay can repeat requests completed since the last successful sync. Persisted sequences are skipped on the next run using the same `node` + `connection_id` identity.
For HTTP/2 traffic, `serialized` replays requests sequentially, while `multiplexed`
dispatches requests concurrently on the shared per-connection client and waits for them at `connection_close` or EOF.
