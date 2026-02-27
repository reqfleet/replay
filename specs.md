# Traffic Recording & Replay Specification

## 1. Overview

This document defines the specification for an HTTP traffic recording and replay system using an Envoy sidecar for capture and a Go-based replay engine.

Goals:

* Record HTTP/1.1 and HTTP/2 traffic
* Preserve connection lifecycle semantics
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
Client → Envoy Sidecar → Application
              ↓
        Recorder Service
              ↓
        traffic.ndjson
              ↓
         Replay Engine (Go)
```

Responsibilities:

* Envoy: HTTP parsing, connection lifecycle tracking
* Recorder Service: Convert structured logs into canonical NDJSON format
* Replay Engine: Reconstruct connections and replay requests deterministically

---

## 3. Event Model

Each line in the NDJSON file represents one event.

### 3.1 Event Types

| Type             | Required               | Purpose                         |
| ---------------- | ---------------------- | ------------------------------- |
| meta             | Yes (first line)       | Format metadata                 |
| connection_open  | Yes                    | TCP lifecycle start             |
| request          | Yes                    | Replayable HTTP request         |
| response         | Optional (recommended) | Validation and latency analysis |
| connection_close | Yes                    | TCP lifecycle end               |

---

## 4. File Header (First Line Only)

```json
{
  "type": "meta",
  "format_version": "1.0",
  "generator": "envoy-sidecar-recorder",
  "created_at": "2026-02-27T03:10:21.123Z",
  "node": "pod-abc-123",
  "cluster": "prod-us-east-1"
}
```

Purpose:

* Enables schema evolution
* Identifies recording environment
* Prevents incompatible replay

---

## 5. Connection Open Event

Emitted when a downstream TCP connection is accepted.

```json
{
  "type": "connection_open",
  "connection_id": "c42",
  "timestamp": "2026-02-27T03:10:21.456Z",
  "downstream_remote_address": "10.1.2.15:53210",
  "downstream_local_address": "10.1.2.8:8080",
  "protocol": "HTTP/1.1",
  "tls": {
    "enabled": true,
    "sni": "api.example.com",
    "alpn": "h2",
    "version": "TLSv1.3"
  }
}
```

### Design Notes

* `connection_id` is a logical identifier assigned at TCP accept time.
* Must not be derived from IP:port.
* TLS block is optional when TLS is disabled.

---

## 6. HTTP Request Event

```json
{
  "type": "request",
  "connection_id": "c42",
  "stream_id": 7,
  "sequence": 3,
  "timestamp": "2026-02-27T03:10:22.001Z",
  "http": {
    "version": "HTTP/2",
    "method": "POST",
    "scheme": "https",
    "authority": "api.example.com",
    "path": "/api/v1/login?redirect=/home"
  },
  "headers": {
    "content-type": ["application/json"],
    "user-agent": ["curl/8.0.0"],
    "authorization": ["Bearer eyJhbGciOi..."]
  },
  "body": {
    "encoding": "base64",
    "content": "eyJ1c2VybmFtZSI6ImFuZHkifQ==",
    "size_bytes": 28
  }
}
```

### Field Requirements

* `connection_id`: Required
* `stream_id`: Required (HTTP/1.1 MUST use value 1)
* `sequence`: Strictly increasing per connection
* `timestamp`: RFC3339 format

### Headers

* Stored as `map[string][]string`
* Header names SHOULD be normalized to lowercase

### Body

Structure:

```json
"body": {
  "encoding": "base64",
  "content": "...",
  "size_bytes": 123
}
```

Reasons:

* Supports binary payloads
* Prevents encoding corruption
* Allows memory-aware replay

---

## 7. HTTP Response Event (Optional but Recommended)

```json
{
  "type": "response",
  "connection_id": "c42",
  "stream_id": 7,
  "sequence": 3,
  "timestamp": "2026-02-27T03:10:22.150Z",
  "status": 200,
  "headers": {
    "content-type": ["application/json"]
  },
  "body": {
    "encoding": "base64",
    "content": "eyJ0b2tlbiI6ImFiYyJ9",
    "size_bytes": 19
  },
  "duration_ms": 149
}
```

Benefits:

* Enables response validation
* Enables latency replay
* Enables mock server generation
* Enables regression testing

---

## 8. Connection Close Event

```json
{
  "type": "connection_close",
  "connection_id": "c42",
  "timestamp": "2026-02-27T03:15:45.900Z",
  "reason": "remote_close"
}
```

### Allowed Reasons

* remote_close
* local_close
* timeout
* drain
* error

---

## 9. Replay Semantics

Replay engine MUST:

1. Group events by `connection_id`
2. Sort by `sequence` within each connection
3. Open one TCP connection per `connection_id`
4. Replay requests in order
5. Optionally sleep based on timestamp delta
6. Close connection when `connection_close` event is encountered

### HTTP/1.1

* Sequential replay per connection
* Preserve keep-alive behavior

### HTTP/2

Two supported modes:

1. Serialized mode (simplified replay)
2. Multiplexed mode (advanced, preserves concurrency)

Multiplexed mode requires stream-aware replay logic.

### 9.1 Distributed Replay for Large Captures

When capture logs are too large for a single replay process, replay MAY be distributed across multiple replay engines.

Requirements:

1. Shard assignment MUST be derived from `connection_id` (for example, hash-based partitioning).
2. All events for a single `connection_id` MUST be handled by exactly one replay engine.
3. Per-connection ordering rules in Section 9 MUST still hold within each shard.
4. Sharding by byte offsets or naive timestamp windows MUST NOT split a single connection across shards.
5. Each shard MAY be replayed independently, but deterministic behavior is defined primarily per connection, not as a single global wall-clock schedule.

Recommended implementation pattern:

* Use a dispatcher to read NDJSON and route events to shard-specific queues/files by `connection_id`.
* Preserve append order within each shard output.
* Persist replay checkpoints per shard to support restart without duplicate sends.
* Apply capacity controls per replay engine (see Section 11.2).

---

## 10. Non-Goals

The system does NOT:

* Preserve raw TCP byte sequences
* Preserve header order
* Preserve header casing
* Preserve chunk boundaries
* Replay TLS handshakes exactly

The system operates at HTTP semantic level, not packet level.

---

## 11. Safety Considerations

Replay engine SHOULD support:

* Dry-run mode
* Target host override
* Header rewriting
* Authorization token replacement
* Idempotency safeguards

### 11.1 Target Override Semantics

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

### 11.2 Per-Engine Capacity Limits (VU/Connection Model)

Replay engines SHOULD expose per-engine capacity controls based on virtual users (VU) and connection counts, rather than request-per-second throttling.

Definitions:

* `virtual_user` (VU): a logical replay worker that drives one or more recorded connections.
* `max_virtual_users_per_engine`: maximum number of VUs that may run concurrently in one replay engine.
* `max_active_connections_per_engine`: maximum number of simultaneously open replay connections in one replay engine.

Normative behavior:

1. A replay engine MUST NOT exceed `max_virtual_users_per_engine`.
2. A replay engine MUST NOT exceed `max_active_connections_per_engine`.
3. When either limit is reached, additional work MUST be queued and started when capacity becomes available.
4. Implementations MUST preserve per-connection ordering and lifecycle semantics while queued.
5. The specification does not require `max_requests_per_second` and does not use it as a primary control.

Distributed replay note:

* In multi-engine deployments, these limits apply per engine.
* Aggregate load is the sum of all engine capacities and SHOULD be configured explicitly by operators.

### 11.3 Replay Outcome Model

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
* Engine MAY make `partial_success` exit behavior configurable, defaulting to non-zero in CI-style contexts.

### 11.4 Metrics Emission and Scrape Endpoint

Replay engines MUST expose Prometheus metrics over HTTP at `/metrics` for pull-based scraping.

Endpoint requirements:

1. Path MUST be `/metrics`.
2. Format MUST be Prometheus text exposition format.
3. Endpoint SHOULD be enabled by default and bind address/port MUST be configurable.
4. Endpoint MUST remain available during the full replay lifecycle, including startup and shutdown windows where feasible.

Metric catalog:

* `shibuya_latency_label_milliseconds`
* `shibuya_latency_label_milliseconds_bucket`
* `shibuya_latency_label_milliseconds_sum`
* `shibuya_latency_label_milliseconds_count`
* `shibuya_status_counter`
* `shibuya_egress_bytes_counter`
* `shibuya_threads_gauge`
* `shibuya_cpu_gauge`
* `shibuya_mem_gauge`

Label conventions SHOULD follow engine metric definitions, including dimensions such as `collection_id`, `plan_id`, `run_id`, `engine_no`, `label`, `zone`, and metric-specific labels like `status` and `le`.

### 11.5 Runtime Configuration (YAML)

Replay runtime behavior SHOULD be configurable via a YAML file.

Minimum configurable domains:

* Timeouts: connect timeout, request timeout, optional idle/keepalive timeout.
* Retry policy: max retries, retryable error classes/statuses, backoff strategy.
* Environment variables: key/value pairs injected into replay process/runtime.
* Metrics server: listen address/port, endpoint enable toggle (default enabled), path (default `/metrics`).
* Capacity controls: `max_virtual_users_per_engine`, `max_active_connections_per_engine`.

Configuration precedence (recommended):

1. Built-in defaults
2. YAML file
3. Environment variables
4. CLI flags (highest precedence)

Example:

```yaml
replay:
  max_virtual_users_per_engine: 20
  max_active_connections_per_engine: 200
  timeout:
    connect: 3s
    request: 30s
    idle_connection: 60s
  retry:
    max_attempts: 2
    backoff: exponential
    retry_on_statuses: [429, 502, 503, 504]
    retry_on_errors: [timeout, connection_reset]
metrics:
  enabled: true
  listen_address: "0.0.0.0:9102"
  path: "/metrics"
env:
  TARGET_ENV: "staging"
  AUTH_MODE: "token-rewrite"
```

POST and mutation requests may cause side effects if replayed against production systems.

---

## 12. Versioning Strategy

* `format_version` MUST follow semantic versioning
* Backward-incompatible changes require major version bump
* New optional fields may be added in minor versions

---

## 13. Future Extensions (Optional)

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

## 14. Summary

This specification provides:

* Connection-aware recording
* Deterministic replay ordering
* HTTP/1.1 and HTTP/2 compatibility
* TLS metadata support
* Future-proof schema design
* Production-safe extensibility

This format is intended for long-term stability and production-grade traffic replay systems.
