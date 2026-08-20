# Traffic Recording & Replay Specification

## 1. Overview

This document defines the specification for an HTTP traffic recording and
replay system using an Envoy proxy for capture and a Go-based replay engine.
Envoy may run as an application sidecar or as a standalone proxy.

Goals:

* Record HTTP/1.1 and HTTP/2 traffic
* Preserve per-connection request ordering and keep-alive reuse
* Support deterministic replay
* Support keep-alive behavior
* Support future schema evolution
* Enable optional response validation

File format: **NDJSON (newline-delimited JSON)**
Encoding: UTF-8
Body encoding: Base64
Ordering: Append-only, strictly chronological

---

## 2. Architecture

```
Client → Envoy Proxy → Application
             ↓
       Recorder Service
             ↓
       traffic.ndjson
             ↓
       Replay Engine (Go)
```

Responsibilities:

* Envoy (sidecar or standalone proxy): HTTP parsing and structured access-log emission
* Recorder Service: Convert structured logs into canonical NDJSON format
* Replay Engine: Reconstruct connections and replay requests deterministically

---

## 3. Envoy Access-Log Input

Each NDJSON line MUST be one flat Envoy access-log record. When present, the
`type` field is the dispatch key and MUST be one of the exact, case-sensitive
values below. If `type` is absent, replay MUST treat the record as
`DownstreamEnd` for compatibility with Envoy's default completion access logs.

| Type                         | Purpose                                                 |
| ---------------------------- | ------------------------------------------------------- |
| `DownstreamStart`            | Request captured when downstream request headers arrive |
| `DownstreamEnd` or omitted   | Completed request with a recorded response status        |

For each original HTTP request, a capture MUST contain exactly one replayable
record. It MUST NOT contain both types for the same request because replay does
not correlate start and end records and would replay the request twice.

Replay rejects unknown explicit types, including normalized `request` records,
`connection_open`, `connection_close`, periodic, upstream, TCP, and tunnel
events. It also rejects the previous nested `http` representation and the
aliases `start_time`, `status`, and `log_type`.

### 3.1 Flat Record Schema

```json
{
  "type": "DownstreamEnd",
  "node": "envoy-a",
  "connection_id": 42,
  "stream_id": 7,
  "sequence": 12,
  "timestamp": "2026-02-27T03:10:22.001Z",
  "method": "POST",
  "scheme": "https",
  "authority": "api.example.com",
  "path": "/api/v1/login?redirect=/home",
  "protocol": "HTTP/2",
  "response_code": 200,
  "duration_ms": 149,
  "headers": {
    "content-type": ["application/json"]
  },
  "body": {
    "encoding": "base64",
    "content": "eyJ1c2VybmFtZSI6ImFuZHkifQ==",
    "size_bytes": 28
  },
  "response_headers": {
    "content-type": ["application/json"]
  },
  "response_body": {
    "encoding": "base64",
    "content": "eyJ0b2tlbiI6ImFiYyJ9",
    "size_bytes": 19
  }
}
```

### 3.2 Field Requirements

`type` is optional; when present, it MUST be `DownstreamStart` or
`DownstreamEnd`.

Every record MUST contain:

* `connection_id`: integer logical connection identifier
* `timestamp`: RFC3339 timestamp
* `method`: HTTP method
* `authority`: request authority
* `path`: request path, including its query string
* `protocol`: HTTP protocol string

`scheme` is optional and defaults to `http` during replay. `node` is optional
and forms part of the logical connection identity when present. `stream_id` is
optional; replay assigns 1 when it is absent on HTTP/1.1, and rejects any other
HTTP/1.1 value.

`sequence` is optional. Replay derives a strictly increasing sequence from
record order independently for each `node` + `connection_id`. If the producer
provides `sequence`, replay preserves it and rejects a decrease.

Additional Envoy fields such as `response_flags`, `bytes_received`,
`bytes_sent`, and `upstream_host` MAY be present and are ignored.

### 3.3 Type-Specific Semantics

`DownstreamStart` represents the request at header arrival. It does not provide
final response expectations, so replay never performs inline response
validation from this type.

`DownstreamEnd` MUST contain `response_code`. It MAY also contain
`duration_ms`, `response_headers`, and `response_body`. Those fields describe
the recorded response and enabled validation checks compare them with the
replay target response.

### 3.4 Request Headers and Bodies

`headers` and `body` describe the request sent to the replay target. Header
values are arrays and names SHOULD be lowercase. If `headers` does not contain
`user-agent`, Replay copies a nonempty `user_agent` field into it.

`body` and `response_body` use base64 encoding:

```json
{
  "encoding": "base64",
  "content": "AAEC",
  "size_bytes": 3
}
```

This representation preserves binary payloads without encoding corruption.
`response_headers` has the same `map[string][]string` representation as
`headers`.

---

## 4. Replay Semantics

Replay engine MUST:

1. Read events in append order without buffering the full capture.
2. Route every event by `node` + `connection_id` to exactly one replay worker.
3. Preserve the parser's per-connection monotonic `sequence`; replay MUST NOT reorder events.
4. Open one replay connection state on the first record for each `node` + `connection_id`.
5. Replay requests in observed connection order for HTTP/1.1 and serialized HTTP/2.
6. In multiplexed HTTP/2 mode, dispatch request sends concurrently as request events arrive.
7. When pacing is enabled, schedule increasing timestamp deltas against a per-connection replay deadline.
8. Time elapsed while replaying a request MUST consume the corresponding timestamp delta; synchronous request latency MUST NOT be followed by another sleep for the full recorded delta.
9. When pacing timestamps move backward or stay equal, keep the existing pacing clock and do not sleep.
10. Close every connection state at EOF.

Native Envoy HTTP access logs do not contain TCP connection-close records.
Replay therefore retains sequence and HTTP transport state for every observed
`node` + `connection_id` until EOF. This preserves keep-alive behavior if a
connection appears again later in the capture, but makes retained state
proportional to the number of unique connection identities assigned to the
engine.

### HTTP/1.1

* Sequential replay per connection
* Preserve keep-alive behavior

### HTTP/2

Two supported modes:

1. Serialized mode, which sends requests one at a time in observed connection order.
2. Multiplexed mode, which sends HTTP/2 requests concurrently on the shared per-connection client and joins in-flight requests at EOF.

Multiplexed mode uses stream-aware checkpointing so a later completed request cannot advance the checkpoint past an earlier in-flight request.

Replay consumes HTTP/2 records in append order and does not reorder them by
`timestamp`, `stream_id`, or `sequence`. Envoy emits `DownstreamEnd` records as
streams complete, so multiplexed streams MAY appear in completion order rather
than their original request-start order. Serialized mode replays that observed
completion order.

An HTTP/2 capture that requires the original request-start order MUST use
`DownstreamStart` records. `DownstreamEnd` remains appropriate when inline
response validation is required and completion-order replay is acceptable.
Each request MUST still appear exactly once as required by Section 3.

### 4.1 Distributed Replay for Large Captures

When capture logs are too large for a single replay process, replay MAY be distributed across multiple replay engines.

Requirements:

1. Shard assignment MUST be derived from `node` + `connection_id` (for example, hash-based partitioning).
2. All events for a single `node` + `connection_id` MUST be handled by exactly one replay engine.
3. Per-connection ordering rules in Section 4 MUST still hold within each shard.
4. Sharding by byte offsets or naive timestamp windows MUST NOT split a single connection across shards.
5. Each shard MAY be replayed independently, but deterministic behavior is defined primarily per connection, not as a single global wall-clock schedule.
6. Shard sizing SHOULD bound the number of unique connection identities retained by each replay engine.

Recommended implementation pattern:

* Use a dispatcher to read NDJSON and route events to shard-specific queues/files by `node` + `connection_id`.
* Preserve append order within each shard output.
* Persist replay checkpoints per shard to support restart without duplicate sends.
* Apply capacity controls per replay engine (see Section 6.2).

---

## 5. Non-Goals

The system does NOT:

* Preserve raw TCP byte sequences
* Preserve header order
* Preserve header casing
* Preserve chunk boundaries
* Replay TLS handshakes exactly

The system operates at HTTP semantic level, not packet level.

---

## 6. Safety Considerations

Replay engine SHOULD support:

* Dry-run mode
* Target host override
* Header rewriting
* Authorization token replacement
* Idempotency safeguards

### 6.1 Target Override Semantics

To avoid replaying captured production traffic back into production, replay tooling SHOULD support explicit destination overrides.

Recommended behavior:

1. A runtime override target (for example, `https://staging.example.com`) replaces captured destination authority.
2. By default, replay SHOULD preserve original `path` and query string while overriding `scheme` and `authority`.
3. When override is enabled, `Host`/`:authority` headers MUST be rewritten to match the override target unless an explicit allowlist says otherwise.
4. Sensitive headers (for example, `authorization`, `cookie`) SHOULD be replaced, removed, or regenerated before send.
5. Replay SHOULD fail fast if override is required by policy but missing.

Example rewrite intent:

* Captured: `https://api.prod.example.com/api/v1/login?redirect=/home`
* Override target: `https://api.staging.example.com`
* Replayed URL: `https://api.staging.example.com/api/v1/login?redirect=/home`

### 6.2 Per-Engine Capacity Limits (VU Model)

Replay engines SHOULD expose virtual-user (VU) worker controls rather than treating VU count as a request-per-second or total-load throttle.

Definitions:

* `virtual_user` (VU): a logical replay worker that drives one or more recorded connections.
* `max_virtual_users_per_engine`: maximum number of VUs that may run concurrently in one replay engine.
* `max_active_connections_per_engine`: configured ceiling for simultaneously open replay connections. `0` means unlimited. This version preserves the setting for configuration compatibility but does not yet enforce positive values.

Normative behavior:

1. A replay engine MUST NOT exceed `max_virtual_users_per_engine` replay workers.
2. Connections MAY be assigned to VUs round-robin, but every event for one `node` + `connection_id` identity MUST remain on the same VU.
3. Each recorded connection SHOULD own its outbound transport until EOF so keep-alive reuse and socket isolation are preserved.
4. Implementations MUST preserve per-connection event ordering when a VU drives multiple connections.
5. The specification does not require `max_requests_per_second` and does not use it as a primary control.

The VU limit bounds replay workers only. It does not bound per-connection state
or transports retained until EOF, nor concurrent streams dispatched by
multiplexed HTTP/2 connections. Per-instance memory therefore depends on the
number of unique `node` + `connection_id` identities assigned to the engine;
request concurrency additionally depends on HTTP mode and target latency.

Distributed replay note:

* In multi-engine deployments, the VU limit applies per engine.
* The sum of per-engine worker limits is aggregate VU capacity, not an aggregate
  load ceiling. Operators SHOULD size and shard replay engines for each shard's
  unique connection count and multiplexed stream concurrency.

### 6.3 Replay Outcome Model

Replay execution MUST produce deterministic run outcomes at three levels: request, connection, and run.

Request outcome classes:

* `sent`: request was emitted to target.
* `send_error`: request failed before receiving an HTTP response (for example connect timeout, TLS error, network reset).
* `response_received`: response was received from target.
* `validation_failed`: response was received but did not match configured validation rules.
* `skipped`: request was intentionally not sent (for example policy guard or dry-run filter).

Connection outcome classes:

* `completed`: all scheduled requests for the connection reached terminal request outcomes and connection closed normally.
* `aborted`: connection terminated early due to unrecoverable error or policy stop.

Run outcome classes:

* `success`: no fatal engine errors and all non-skipped requests reached terminal outcomes.
* `partial_success`: replay completed with non-fatal send/validation failures.
* `failed`: replay stopped early due to fatal conditions (for example invalid input, backend unavailable, policy violation).

Exit status guidance:

* Engine SHOULD return exit code `0` for `success`.
* Engine SHOULD return non-zero exit code for `failed`.
* Engine SHOULD return exit code `0` for `partial_success` by default.
* Engine MAY make `partial_success` exit behavior configurable when operators need non-zero behavior in CI-style contexts.

### 6.4 Metrics Emission and Scrape Endpoint

Replay engines MUST expose Prometheus metrics over HTTP at `/metrics` for pull-based scraping.

Endpoint requirements:

1. Path MUST be `/metrics`.
2. Format MUST be Prometheus text exposition format.
3. Endpoint SHOULD be enabled by default and bind address/port MUST be configurable.
4. Endpoint MUST remain available during the full replay lifecycle, including startup and shutdown windows where feasible.

Metric catalog with the default `replay` namespace:

* `replay_latency_label_milliseconds`
* `replay_latency_label_milliseconds_bucket`
* `replay_latency_label_milliseconds_sum`
* `replay_latency_label_milliseconds_count`
* `replay_status_counter`
* `replay_egress_bytes_counter`
* `replay_threads_gauge`
* `replay_cpu_gauge`
* `replay_mem_gauge`

Engine-specific integrations MAY configure a different Prometheus namespace and common label set. Label conventions SHOULD include configurable common dimensions plus metric-specific labels such as `label`, `status`, and `le`.

For `replay_status_counter`, the `status` label MAY contain either a numeric HTTP status code or a synthetic transport status such as `timeout`, `connection_refused`, `connection_reset`, `tls`, `network`, or `send_error` when no HTTP response was received.

Engines SHOULD support a `metrics.path_templates` list to prevent dynamic path
segments from creating unbounded metric-label cardinality. A segment enclosed
in braces (for example, `{id}` in `/users/{id}`) is dynamic; all other segments
are literal. Matching MUST use the path without its query string, require the
same segment count, and compare literal segments exactly. The first matching
template MUST become the metric-specific `label`; when no template matches, the
path without its query string MUST remain the label.

### 6.5 Runtime Configuration (YAML)

Replay runtime behavior SHOULD be configurable via a YAML file.

Minimum configurable domains:

* Timeouts: connect timeout, request timeout, optional idle/keepalive timeout.
* HTTP/2 replay mode: serialized or multiplexed.
* Retry policy: max retries, retryable error classes/statuses, backoff strategy.
* Validation: status, header, body, and ignored-header controls.
* Pacing: optional timestamp-delta replay with a maximum sleep cap.
* Metrics server: listen address/port, endpoint enable toggle (default enabled), path (default `/metrics`).
* Capacity controls: `max_virtual_users_per_engine`, `max_active_connections_per_engine` (`0` means unlimited).

Configuration precedence (recommended):

1. Built-in defaults
2. YAML file
3. Environment variables
4. CLI flags (highest precedence)

Example:

```yaml
replay:
  max_virtual_users_per_engine: 20
  max_active_connections_per_engine: 0
  rampup_duration: 0s
  http2:
    mode: serialized
  timeout:
    connect: 3s
    request: 30s
    idle_connection: 60s
  retry:
    max_attempts: 2
    backoff: exponential
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
  enabled: true
  namespace: "replay"
  listen_address: "0.0.0.0:9102"
  path: "/metrics"
  path_templates:
    - "/users/{id}"
    - "/users/{id}/orders"
  common_labels:
    - name: "run_id"
      value: "unknown"
      env: "REPLAY_RUN_ID"
    - name: "worker_id"
      value: "0"
      env: "REPLAY_WORKER_ID"
    - name: "zone"
      value: "unknown"
      env: "REPLAY_ZONE"
```

Each `validation.status`, `validation.headers`, and `validation.body` field
directly enables that check; there is no aggregate validation toggle.

POST and mutation requests may cause side effects if replayed against production systems.

---

## 7. Future Extensions (Optional)

Example replay hints:

```json
"replay_hints": {
  "idempotent": false,
  "mutates_state": true,
  "requires_auth_refresh": true
}
```

Example tagging:

```json
"tags": ["oauth", "login-flow"]
```

---

## 8. Summary

This specification provides:

* Connection-aware recording
* Deterministic replay ordering
* HTTP/1.1 and HTTP/2 compatibility
* TLS metadata support
* Future-proof schema design
* Production-safe extensibility

This format is intended for long-term stability and production-grade traffic replay systems.
