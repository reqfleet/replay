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

## 3. Recording and Replay Input

Recording observations and replay events are different wire contracts. Envoy
stdout is recorder input. The replay engine accepts only canonical output from
`replay combine`.

### 3.1 Raw Recorder Observations

Raw input is UTF-8 NDJSON. Each JSON-looking line MUST be one flat Envoy
observation with an explicit, case-sensitive `type`:

| Type              | Purpose                                                  |
| ----------------- | -------------------------------------------------------- |
| `DownstreamStart` | Request observed when downstream request headers arrive  |
| `DownstreamEnd`   | The same request after its response or stream termination |

Each original request MUST have exactly one observation of each type. Both
observations MUST contain:

* `request_id`: nonempty and not `-`
* `connection_id`: integer logical connection identifier
* `timestamp`: valid RFC3339 request-start timestamp
* `method`, `authority`, `path`, and `protocol`

`node`, `scheme`, and `stream_id` are optional. `DownstreamEnd` additionally
MUST contain `response_code`. Its `response_flags`, when present, MUST be an
Envoy string. `-` and the empty string mean no flags; every other value is
split on commas into exact, nonempty tokens.

The pair identity is `(node, connection_id, request_id)`. The combiner accepts
either observation order, but never pairs by timestamp, method, path, FIFO
position, or response-completion order. A pair MUST agree on `node`,
`connection_id`, `request_id`, `timestamp`, `method`, `authority`, `path`, and
`protocol`. Nonempty `scheme` and nonzero `stream_id` values MUST also agree;
a value omitted by one side is filled from the other.

Duplicate, conflicting, malformed, unsupported, missing-ID, and unmatched
observations are fatal. No partial output is installed. Raw Start-only,
End-only, omitted-type, and mixed observation files are invalid replay-engine
input.

### 3.2 Combine Semantics

Run:

```bash
replay combine -log mixed.ndjson -out canonical.ndjson
```

`-gzip` and `-zstd` select compressed input and are mutually exclusive. Output
is plain NDJSON.

The combiner emits exactly one canonical `request` per pair. Global output
order is the order of `DownstreamStart` observations, independent of End
arrival or completion order. The request uses Start identity, timestamp, and
request descriptors. Missing Start request headers or body are filled from
End. End supplies response code, duration, response headers, response body, and
tokenized response flags. Serialized `sequence` is omitted; the parser derives
it deterministically from canonical order for each connection key.

If any End for a connection contains the exact `DC` token, the combiner emits
one `connection_close` immediately after that connection's final canonical
request in global Start order. It does not close at the DC-bearing request's
position because later-started HTTP/2 streams may already belong to the same
recorded connection. A connection without `DC` has no synthetic close marker
and is finalized at EOF.

### 3.3 Canonical Replay Events

Canonical input uses explicit `request` and optional `connection_close` events:

```json
{"type":"request","node":"envoy-a","connection_id":42,"request_id":"opaque-id","timestamp":"2026-02-27T03:10:22.001Z","method":"POST","scheme":"https","authority":"api.example.com","path":"/api/v1/login","protocol":"HTTP/2","stream_id":7,"headers":{"content-type":["application/json"]},"body":{"encoding":"base64","content":"e30=","size_bytes":2},"response_code":200,"duration_ms":149,"response_headers":{"content-type":["application/json"]},"response_body":{"encoding":"base64","content":"e30=","size_bytes":2},"response_flags":["DC"]}
{"type":"connection_close","node":"envoy-a","connection_id":42}
```

A `request` MUST contain `request_id`, `connection_id`, `timestamp`, `method`,
`authority`, `path`, `protocol`, and `response_code`. `scheme` is optional and
defaults to `http` during replay. `node` is optional and forms part of the
logical connection identity. `stream_id` is optional; replay assigns `1` when
it is absent on HTTP/1.1 and rejects any other HTTP/1.1 value.

`response_flags`, when present, MUST be a JSON array containing only strings.
The replay parser does not accept Envoy's raw string representation.

`sequence` is optional. Replay derives a strictly increasing sequence from
canonical request order independently for each `node` + `connection_id`. If a
producer supplies `sequence`, replay preserves it and rejects a decrease.
`connection_close` requires only `connection_id` plus optional `node`, passes
directly to the engine, and does not advance request sequence.

`headers` and `response_headers` are maps from header names to arrays of
strings. Names SHOULD be lowercase. `body` and `response_body` use a base64
envelope:

```json
{"encoding":"base64","content":"AAEC","size_bytes":3}
```

Malformed JSON, omitted types, raw Envoy observations, `connection_open`, and
unknown event types are rejected with the input line number.

---

## 4. Replay Semantics

Replay engine MUST:

1. Read canonical events in append order without buffering the full capture.
2. Route every event by `node` + `connection_id` to exactly one replay worker.
3. Preserve the parser's per-connection monotonic `sequence`; replay MUST NOT reorder events.
4. Open one replay connection state on the first request for each `node` + `connection_id`.
5. Replay requests in observed connection order for HTTP/1.1 and serialized HTTP/2.
6. In multiplexed HTTP/2 mode, dispatch request sends concurrently as request events arrive.
7. When pacing is enabled, schedule increasing timestamp deltas against a per-connection replay deadline.
8. Time elapsed while replaying a request MUST consume the corresponding timestamp delta; synchronous request latency MUST NOT be followed by another sleep for the full recorded delta.
9. When pacing timestamps move backward or stay equal, keep the existing pacing clock and do not sleep.
10. On `connection_close`, wait for in-flight HTTP/2 work, close transport resources, and finalize the connection.
11. At EOF, perform the same finalization for every remaining connection.

DC-derived canonical close markers provide confirmed connection termination.
Connections without DC remain active until EOF because ordinary Envoy HTTP
access logs do not report a later clean idle close.

### HTTP/1.1

* Sequential replay per connection
* Preserve keep-alive behavior

### HTTP/2

Two supported modes:

1. Serialized mode, which sends requests one at a time in observed connection order.
2. Multiplexed mode, which sends HTTP/2 requests concurrently on the shared per-connection client and joins in-flight requests at EOF.

Multiplexed mode uses stream-aware checkpointing so a later completed request cannot advance the checkpoint past an earlier in-flight request.

Replay consumes HTTP/2 requests in canonical append order and does not reorder
them by `timestamp`, `stream_id`, or `sequence`. `replay combine` establishes
that order from `DownstreamStart` observations while merging response metadata
from the corresponding Ends. Multiplexed mode may execute requests
concurrently; a close marker waits for all admitted streams before finalizing
the connection.

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

Replay engines MUST expose Prometheus metrics over HTTP for pull-based scraping.

Endpoint requirements:

1. Path MUST be configurable and MUST default to `/metrics`.
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
* Capacity control: `max_virtual_users_per_engine`.

Configuration precedence (recommended):

1. Built-in defaults
2. YAML file
3. Environment variables
4. CLI flags (highest precedence)

Example:

```yaml
replay:
  max_virtual_users_per_engine: 20
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
