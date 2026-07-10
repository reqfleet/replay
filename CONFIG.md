# Replay configuration reference

`config.yaml` controls replay execution, response validation, metrics, target safety, and outgoing request rewriting. The file is optional; omitted values use built-in defaults.

Configuration precedence, from highest to lowest, is:

1. CLI flags
2. Process environment variables
3. `config.yaml`
4. Built-in defaults

Durations use Go duration syntax, such as `750ms`, `3s`, or `2m`.

## `replay`

### Capacity and startup

| Field | Current value | Meaning and constraints |
| --- | --- | --- |
| `replay.max_virtual_users_per_engine` | `20` | Maximum number of replay workers active in this process. Connections are assigned to these workers while preserving per-connection ordering. Must be greater than zero. |
| `replay.rampup_duration` | `0s` | Spreads worker activation linearly across this duration. `0s` activates workers immediately; for example, `30s` stages them over 30 seconds. Must not be negative. |
| `replay.max_active_connections_per_engine` | `200` | Maximum number of replay connection states open concurrently. Additional connections wait for capacity. Must be greater than zero. |

### `replay.http2`

| Field | Current value | Meaning and constraints |
| --- | --- | --- |
| `replay.http2.mode` | `serialized` | Controls HTTP/2 request scheduling. `serialized` sends requests in recorded order, one at a time. `multiplexed` sends requests from multiple HTTP/2 streams concurrently over the connection and waits for them at `connection_close` or EOF. Allowed values: `serialized`, `multiplexed`. |

### `replay.timeout`

| Field | Current value | Meaning and constraints |
| --- | --- | --- |
| `replay.timeout.connect` | `3s` | Maximum time allowed to establish a network connection. Must be greater than zero. |
| `replay.timeout.request` | `30s` | Overall timeout for one outbound HTTP attempt, including receiving and reading the response. Must be greater than zero. |
| `replay.timeout.idle_connection` | `60s` | TCP keep-alive interval and maximum time an idle pooled connection remains open. Must be greater than zero. |

### `replay.retry`

| Field | Current value | Meaning and constraints |
| --- | --- | --- |
| `replay.retry.max_attempts` | `2` | Maximum total attempts for a request, including the first attempt. A value of `2` permits one retry. Must be greater than zero. |
| `replay.retry.backoff` | `exponential` | Delay strategy between attempts: `none`, `fixed`, or `exponential`. Fixed backoff is 100 ms; exponential backoff starts at 100 ms and is capped at 5 seconds. |
| `replay.retry.retry_on_statuses` | `[429, 502, 503, 504]` | HTTP response status codes that trigger another attempt when attempts remain. |
| `replay.retry.retry_on_errors` | `[timeout, connection_reset, network, tls]` | Transport error categories that trigger another attempt. `network` includes DNS, connection-refused, and dial failures. Matching is case-insensitive. |

### `replay.validation`

Validation compares replay-target responses with recorded responses matched by connection and sequence. A mismatch increments `validation_failed` and makes the run `partial_success`; it does not immediately abort replay. When no recorded response exists, there is nothing to compare.

| Field | Current value | Meaning and constraints |
| --- | --- | --- |
| `replay.validation.enabled` | `true` | Master switch for recorded-response validation. If false, all validation checks are skipped. |
| `replay.validation.status` | `true` | Compares the recorded HTTP status with the replay-target status. |
| `replay.validation.headers` | `false` | Compares recorded response headers and values. Every non-ignored recorded header must match; extra headers from the target are allowed. |
| `replay.validation.body` | `false` | Compares the decoded recorded response body with the target body byte-for-byte. |
| `replay.validation.ignore_headers` | `[x-request-id, date]` | Header names excluded from header comparison. Matching is case-insensitive and only applies when `headers` is true. |

With the current settings, validation checks only HTTP status codes.

### `replay.pacing`

| Field | Current value | Meaning and constraints |
| --- | --- | --- |
| `replay.pacing.enabled` | `false` | When true, waits between events according to their recorded timestamp deltas. Equal, decreasing, or invalid timestamps do not introduce a delay. |
| `replay.pacing.max_sleep_delta` | `30s` | Caps each pacing delay. `0s` means no cap. Must not be negative. |

### `replay.lifecycle`

| Field | Current value | Meaning and constraints |
| --- | --- | --- |
| `replay.lifecycle.require_open` | `true` | Requires a matching `connection_open` event before the first request for each `node` and `connection_id`. A missing open event fails replay. `connection_close` remains optional because EOF implicitly closes open connections. |

### `replay.idempotency`

| Field | Current value | Meaning and constraints |
| --- | --- | --- |
| `replay.idempotency.enabled` | `true` | Enables the mutation-request safety policy. |
| `replay.idempotency.block_methods` | `[POST, PUT, PATCH, DELETE]` | HTTP methods subject to the safety policy. Method matching is case-insensitive. |
| `replay.idempotency.require_header_for_allow` | `[idempotency-key, x-idempotency-key]` | A blocked method is sent only when at least one listed header exists with a non-empty value. Otherwise, the request is counted as skipped. Header matching is case-insensitive. An empty list blocks every method listed in `block_methods`. |

### `replay.sharding`

| Field | Current value | Meaning and constraints |
| --- | --- | --- |
| `replay.sharding.shard_index` | `0` | Zero-based index of the shard handled by this process. Must be in the range `[0, shard_count)`. |
| `replay.sharding.shard_count` | `1` | Total number of replay shards. Connections are assigned by a stable hash of `node` and `connection_id`, so all events for one connection remain together. Must be greater than zero. `1` disables distribution. |

### `replay.checkpoint`

| Field | Current value | Meaning and constraints |
| --- | --- | --- |
| `replay.checkpoint.file` | `./checkpoint.json` | File used to persist the last completed sequence for each `node` and `connection_id`. Completed sequences are skipped when a later run reuses this file. Updates are written atomically. An empty path disables checkpoint persistence. Dry-run requests are not checkpointed. |

## `metrics`

| Field | Current value | Meaning and constraints |
| --- | --- | --- |
| `metrics.enabled` | `true` | Starts the Prometheus HTTP endpoint when true. |
| `metrics.namespace` | `replay` | Prefix used for emitted Prometheus metric names. It must be a valid Prometheus name: letters or underscores first, followed by letters, digits, or underscores. Required when metrics are enabled. |
| `metrics.listen_address` | `0.0.0.0:9102` | Address and port on which the metrics HTTP server listens. Required when metrics are enabled. |
| `metrics.path` | `/metrics` | HTTP path serving Prometheus text exposition. Required when metrics are enabled. |
| `metrics.graceful_termination_period` | `5s` | Keeps the process alive for this duration after replay completes so Prometheus can perform a final scrape. `0s` disables the wait. Must not be negative. |
| `metrics.common_labels` | Three entries | Labels added to every replay metric. Label names must be unique valid Prometheus names and cannot be `label`, `status`, `le`, or start with `__`. |
| `metrics.common_labels[].name` | `run_id`, `worker_id`, `zone` | Prometheus label name. |
| `metrics.common_labels[].value` | `unknown`, `0`, `unknown` | Literal fallback used when the associated environment reference is unset or empty. Values are paired with entries in list order. |
| `metrics.common_labels[].env` | `REPLAY_RUN_ID`, `REPLAY_WORKER_ID`, `REPLAY_ZONE` | Optional environment-variable reference used to obtain the label value. A non-empty process environment value wins; otherwise the top-level YAML `env` map is checked, then `value` is used. |

## `env`

The top-level `env` mapping exports arbitrary key/value pairs into the replay process. The engine does not otherwise assign special meaning to the two sample keys. Entries can also provide fallback values for `metrics.common_labels[].env` references.

| Field | Current value | Meaning |
| --- | --- | --- |
| `env.TARGET_ENV` | `staging` | Exports `TARGET_ENV=staging`. This is an operator-defined value; replay does not interpret it directly. |
| `env.AUTH_MODE` | `token-rewrite` | Exports `AUTH_MODE=token-rewrite`. This is an operator-defined value; replay does not interpret it directly. |

## `target`

| Field | Current value | Meaning |
| --- | --- | --- |
| `target.override_url` | Empty string | Replaces the captured request scheme and authority while preserving its path and query. Replay also rewrites `Host` to the override host. An empty value disables the override. `REPLAY_OVERRIDE_URL` or `--override-url` can override this setting. |
| `target.require_override` | `false` | When true, startup fails unless an override URL is configured. Use this as a safety guard against accidentally replaying traffic to captured production destinations. `REPLAY_REQUIRE_OVERRIDE` or `--require-override` can enable it. |

## `header_rewrite`

Header rewriting is applied to each outbound request after recorded headers are loaded. Headers in `drop` are removed first, automatic target-host rewriting runs next, and entries in `set` run last.

| Field | Current value | Meaning |
| --- | --- | --- |
| `header_rewrite.drop` | `[authorization, cookie]` | Recorded request headers removed before sending. The current value prevents captured credentials and cookies from being replayed. Header matching is case-insensitive. |
| `header_rewrite.set` | `{}` | Map of headers to replace or add after dropping headers and applying the target override. The empty map performs no explicit additions. For example, `authorization: "Bearer replacement-token"` installs a replacement credential. |
