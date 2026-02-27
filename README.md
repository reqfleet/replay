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

## Retry and validation config

Use `config.yaml` to control retry and response validation behavior.

```yaml
replay:
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
```

When a recorded response exists for a request (`connection_id` + `sequence`), mismatches increment `validation_failed` and produce `partial_success`.
