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

Metric names:

- `shibuya_latency_label_milliseconds`
- `shibuya_latency_label_milliseconds_bucket`
- `shibuya_latency_label_milliseconds_sum`
- `shibuya_latency_label_milliseconds_count`
- `shibuya_status_counter`
- `shibuya_egress_bytes_counter`
- `shibuya_threads_gauge`
- `shibuya_cpu_gauge`
- `shibuya_mem_gauge`

`shibuya_threads_gauge` reflects the number of replay clients created for the
engine, not the live number of Go goroutines.

## Outcome model

- Run outcomes: `success`, `partial_success`, `failed`
- Exit code:
  - `0` for `success`
  - `1` for `failed`
  - `0` for `partial_success` by default
  - set `REPLAY_PARTIAL_SUCCESS_EXIT_ZERO=false` to force `1` on `partial_success`

`shibuya_status_counter` includes both numeric HTTP response codes and synthetic transport statuses such as `timeout`, `connection_refused`, `connection_reset`, `tls`, `network`, and `send_error` when a request fails before any response is received.

## CLI & environment precedence

Configuration precedence (highest → lowest): CLI flags > environment variables > YAML config > defaults.

Notable CLI flags (also available via env):
- `--dry-run` / `REPLAY_DRY_RUN`: do not send network requests.
- `--override-url` / `REPLAY_OVERRIDE_URL`: rewrite target host and URL.
- `--require-override` / `REPLAY_REQUIRE_OVERRIDE`: fail if override missing.
- `--config` path: YAML config file (loaded before env and CLI overrides).

Environment variables for metrics and labels:
- `METRICS_ENABLED`, `METRICS_LISTEN_ADDRESS`, `METRICS_PATH`, `METRICS_GRACEFUL_TERMINATION_PERIOD`

`metrics.graceful_termination_period` keeps the process alive after replay completes so the metrics endpoint remains scrapeable for a short window before exit. The default is `5s`.

## Ramp-up

`replay.rampup_duration` linearly stages virtual user activation over wall-clock time.

- `0s` disables ramp-up and preserves the current immediate startup behavior.
- `100` target VUs with `100s` ramp-up reaches the full `100` VUs by `100s`.
- Ramp-up only affects when VUs begin consuming replay jobs. `max_active_connections_per_engine`, HTTP/2 stream limits, and per-request pacing continue to behave independently.

All metric labels can also point at env vars directly from YAML:
- `labels.collection_id_env`, `labels.plan_id_env`, `labels.run_id_env`, `labels.engine_no_env`, `labels.zone_env`
- if the configured env var is unset or empty, replay falls back to the corresponding literal `labels.*` value
- replay resolves these env-ref keys from the process environment first and from the YAML `env:` map second

CLI flags are applied last and take highest precedence for safety-related settings.

## Operator checklist (safe run)

- Always run with `--dry-run` first to confirm traffic parsing and pacing without emitting network requests.
- When replaying against non-production targets, use `--override-url` to direct traffic and consider setting `--require-override` in operators' configs to avoid accidental live traffic.
- Verify the metrics endpoint (default `http://0.0.0.0:9102/metrics`) is reachable before and during runs.
- If you need resumable runs, set `checkpoint.file` in the YAML to persist completed sequences per `node` + `connection_id`.

## Programmatic outcome details

In addition to the summary printed to stdout, the engine maintains aggregate outcome types for programmatic consumption:

- `ConnectionResult` — fields: `node`, `connection_id`, `outcome` (`completed`/`aborted`), plus per-connection aggregate counters (`requests_sent`, `responses_received`, `send_errors`, `validation_failed`, `skipped`).
- `RequestResult` — retained as an API type for future bounded detail output; `Summary.RequestResults` and `ConnectionResult.Requests` are not populated during normal replay so large captures do not retain one result per request.

## Migration note

The parser is stricter now: the NDJSON meta header and format version are validated by default. Older logs that relied on permissive parsing may need normalizing or an intermediate conversion step.

## Example config (replay/config.yaml)

See `config.yaml` in this directory for a ready-to-use example demonstrating safe defaults, metrics, and override usage.

## Retry and validation config

Use `config.yaml` to control retry, response validation, pacing, lifecycle, and idempotency safeguards.

```yaml
replay:
  rampup_duration: 0s  # 0s disables ramp-up; 30s stages VUs over 30 seconds
  http2:
    mode: serialized  # serialized|multiplexed
    max_concurrent_streams: 16
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
    require_close: true
  idempotency:
    enabled: true
    block_methods: [POST, PUT, PATCH, DELETE]
    require_header_for_allow: [idempotency-key, x-idempotency-key]
  sharding:
    shard_index: 0
    shard_count: 1
  checkpoint:
    file: "./checkpoint.json"
```

When a recorded response exists for a request (`connection_id` + `sequence`), mismatches increment `validation_failed` and produce `partial_success`.

When the target is unreachable or times out, the request is recorded as `send_error`, the run remains `partial_success`, and `shibuya_status_counter` is incremented with a synthetic transport status.

When idempotency safeguards are enabled, mutation methods are skipped unless one of the allow headers is present.
Lifecycle checks require both `connection_open` and `connection_close` events per replayed connection.

Sharding routes connections by `node` + `connection_id` hash (`shard_index` / `shard_count`) and only replays the local shard.
If `checkpoint.file` is set, completed sequences are persisted and skipped on the next run using the same `node` + `connection_id` identity.
For HTTP/2 traffic, `serialized` replays requests sequentially, while `multiplexed`
replays by stream with bounded stream concurrency.
