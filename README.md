# Replay Engine (MVP)

Go-based HTTP replay engine that consumes NDJSON traffic logs and exposes Prometheus metrics on `/metrics`.

## Inputs

- Required: `requests.log` (NDJSON traffic file)
- Optional: `config.yaml` (timeouts, retry, env vars, override target, metrics settings)

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
- replay resolves these env-ref keys from the process environment first and from the YAML `env:` map second

CLI flags are applied last and take highest precedence for safety-related settings.

## Operator checklist (safe run)

- Always run with `--dry-run` first to confirm traffic parsing and pacing without emitting network requests.
- When replaying against non-production targets, use `--override-url` and consider setting `--disallow-recorded-targets` in operator configs to prevent accidental traffic to recorded destinations.
- Verify the metrics endpoint (default `http://0.0.0.0:9102/metrics`) is reachable before and during runs.
- If you need resumable runs, set `checkpoint.file` in the YAML. Every acknowledged sequence is durably persisted per `node` + `connection_id`; persistence failures fail the run.

## Programmatic outcome details

In addition to the summary printed to stdout, the engine maintains aggregate outcome types for programmatic consumption:

- `ConnectionResult` — fields: `node`, `connection_id`, `outcome` (`completed`/`aborted`), plus per-connection aggregate counters (`requests_sent`, `responses_received`, `send_errors`, `validation_failed`, `skipped`).
- `RequestResult` — retained as an API type for future bounded detail output; `Summary.RequestResults` and `ConnectionResult.Requests` are not populated during normal replay so large captures do not retain one result per request.

## Migration note

When an NDJSON meta header is present, its format version and declared response-expectation capability are validated. Older logs without the optional capability retain legacy validation behavior.

## Recording response expectations

Format `1.0` recordings SHOULD declare their historical response shape in the
first `meta` event:

```json
{"type":"meta","format_version":"1.0","response_expectations":"none"}
```

Allowed capabilities are `none`, `request_status`, and `response_events`.
`none` is appropriate for request-start-only recordings such as
`DownstreamStart`: replay warns if response validation is configured, disables
historical validation for that run, and does not allocate response-validation
rendezvous maps. Target response status metrics, retries, transfer errors, body
draining, byte accounting, latency, and connection reuse continue unchanged.
Use `request_status` when each completed request event carries its expected
`status`, and `response_events` when the log contains separate `response` events.

## Example config (replay/config.yaml)

See `config.yaml` in this directory for a ready-to-use example demonstrating safe defaults, metrics, and override usage.

## Retry and validation config

Use `config.yaml` to control retry, response validation, pacing, lifecycle, and idempotency safeguards.

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
    enabled: true
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
If `checkpoint.file` is set, completed sequences are persisted and skipped on the next run using the same `node` + `connection_id` identity.
For HTTP/2 traffic, `serialized` replays requests sequentially, while `multiplexed`
dispatches requests concurrently on the shared per-connection client and waits for them at `connection_close` or EOF.
