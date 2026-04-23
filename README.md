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

## Outcome model

- Run outcomes: `success`, `partial_success`, `failed`
- Exit code:
  - `0` for `success`
  - `1` for `failed`
  - `1` for `partial_success` by default
  - set `REPLAY_PARTIAL_SUCCESS_EXIT_ZERO=1` to return `0` on `partial_success`

## CLI & environment precedence

Configuration precedence (highest → lowest): CLI flags > environment variables > YAML config > defaults.

Notable CLI flags (also available via env):
- `--dry-run` / `REPLAY_DRY_RUN`: do not send network requests.
- `--override-url` / `REPLAY_OVERRIDE_URL`: rewrite target host and URL.
- `--require-override` / `REPLAY_REQUIRE_OVERRIDE`: fail if override missing.
- `--config` path: YAML config file (loaded before env and CLI overrides).

Environment variables for metrics and labels:
- `METRICS_ENABLED`, `METRICS_LISTEN_ADDRESS`, `METRICS_PATH`
- `REPLAY_LABEL_COLLECTION_ID`, `REPLAY_LABEL_PLAN_ID`, `REPLAY_LABEL_RUN_ID`, `REPLAY_LABEL_ENGINE_NO`, `REPLAY_LABEL_ZONE`

CLI flags are applied last and take highest precedence for safety-related settings.

## Operator checklist (safe run)

- Always run with `--dry-run` first to confirm traffic parsing and pacing without emitting network requests.
- When replaying against non-production targets, use `--override-url` to direct traffic and consider setting `--require-override` in operators' configs to avoid accidental live traffic.
- Verify the metrics endpoint (default `http://0.0.0.0:9102/metrics`) is reachable before and during runs.
- If you need resumable runs, set `checkpoint.file` in the YAML to persist completed sequences.

## Programmatic outcome details

In addition to the summary printed to stdout, the engine maintains structured outcome types (used by SDKs and future outputs):

- `RequestResult` — fields: `connection_id`, `sequence`, `outcome` (`sent`, `send_error`, `response_received`, `validation_failed`, `skipped`), `status_code`, `latency_ms`, `error`, `validation_failed`, `skipped`.
- `ConnectionResult` — fields: `connection_id`, `outcome` (`completed`/`aborted`), `requests` (array of `RequestResult`).

These are aggregated into the engine `Summary` for programmatic consumption or future JSON output.

## Migration note

The parser is stricter now: the NDJSON meta header and format version are validated by default. Older logs that relied on permissive parsing may need normalizing or an intermediate conversion step.

## Example config (replay/config.yaml)

See `config.yaml` in this directory for a ready-to-use example demonstrating safe defaults, metrics, and override usage.

## Retry and validation config

Use `config.yaml` to control retry, response validation, pacing, lifecycle, and idempotency safeguards.

```yaml
replay:
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

When idempotency safeguards are enabled, mutation methods are skipped unless one of the allow headers is present.
Lifecycle checks require both `connection_open` and `connection_close` events per replayed connection.

Sharding routes connections by `connection_id` hash (`shard_index` / `shard_count`) and only replays the local shard.
If `checkpoint.file` is set, completed sequences are persisted and skipped on the next run.
For HTTP/2 traffic, `serialized` replays requests sequentially, while `multiplexed`
replays by stream with bounded stream concurrency.
