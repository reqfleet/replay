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
Raw observation order: observed append order; either pair member may appear first
Canonical request order: global `DownstreamStart` observation order

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

Recording observations and replay events are different wire contracts. The
recommended fidelity path sends paired Envoy stdout observations through
`replay combine` and replays its canonical output. End-only completion logs MAY
be replayed directly under the limited contract in Section 3.4.

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
* `method`, `authority`, `path`, and `protocol`: nonempty strings

`request_id` identifies one physical downstream request and MUST NOT be reused
by another request with the same `(node, connection_id)` during a capture.
Client-visible retries and fan-out calls are new physical requests and MUST use
new IDs; logical trace or transaction correlation belongs in a separate field.

`node`, `scheme`, and `stream_id` are optional. An HTTP/1.1 observation may
omit `stream_id` or use `1`; the combiner rejects any other value and omits the
field from canonical output. `DownstreamEnd` additionally MUST contain integer
`response_code` and string `response_flags`. `-` and the empty flag string mean
no flags; every other value is split on commas into exact, nonempty tokens.

Optional payload and response fields use these representations:

| Field              | Observation side | JSON representation                        | Combine rule                         |
| ------------------ | ---------------- | ------------------------------------------ | ------------------------------------ |
| `headers`          | Start or End     | Object mapping names to arrays of strings  | Start wins; End fills an omission    |
| `body`             | Start or End     | Base64 body envelope                       | Start wins; End fills an omission    |
| `duration_ms`      | End              | Number of milliseconds                     | End supplies canonical value         |
| `response_headers` | End              | Object mapping names to arrays of strings  | End supplies canonical value         |
| `response_body`    | End              | Base64 body envelope                       | End supplies canonical value         |

A base64 body envelope is an object such as
`{"encoding":"base64","content":"AAEC","size_bytes":3}`. Fields designated for
End do not contribute to canonical output when supplied only on Start.

The pair identity is `(node, connection_id, request_id)`. The combiner accepts
either observation order, but never pairs by timestamp, method, path, FIFO
position, or response-completion order. A pair MUST agree on `node`,
`connection_id`, `request_id`, `timestamp`, `method`, `authority`, `path`, and
`protocol`. Nonempty `scheme` values MUST also agree. For non-HTTP/1.1
observations, nonzero `stream_id` values MUST agree; a value omitted by one side
is filled from the other.

Duplicate, conflicting, malformed, unsupported, and missing-ID observations are
fatal. Unmatched observations at EOF are discarded instead of producing partial
canonical events. A Start and End with the same `request_id` but different
connection identities remain a fatal conflict. A file containing
`DownstreamStart` or both observation sides is combiner input and MUST NOT be
sent directly to replay.

### 3.2 Combine Semantics

Run:

```bash
replay combine -log mixed.ndjson -out canonical.ndjson
```

`-zstd` selects compressed input. Output is plain NDJSON.

Each encoded input or canonical output line is limited to 16 MiB. A complete
pair whose merged canonical record exceeds that limit is fatal, even when its
individual Start and End lines fit. The command does not install or replace
the output file after this validation failure.

When at least one unmatched observation is discarded, the command succeeds and
writes one warning to stderr:

```text
combine: warning: discarded unmatched observations starts=<N> ends=<N>
```

The counts cover discarded `DownstreamStart` and `DownstreamEnd` observations,
respectively. Complete pairs are still emitted normally.

The combiner emits exactly one canonical `request` per pair. Global output
order is the order of `DownstreamStart` observations, independent of End
arrival or completion order. The request uses Start identity, timestamp, and
request descriptors. Missing Start request headers or body are filled from
End. End supplies response code, duration, response headers, response body, and
tokenized response flags. Serialized `sequence` is omitted; the parser derives
it deterministically from canonical order for each connection key.

If any paired End for a connection contains the exact `DC` token, the combiner emits
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

A `request` MUST contain a nonempty `request_id`, a valid RFC3339 `timestamp`,
nonempty `method`, `authority`, `path`, and `protocol`, integer
`connection_id`, and integer `response_code`. `scheme` is optional and defaults
to `http` during replay. `node` is optional and forms part of the logical
connection identity. HTTP/1.1 requests MUST omit `stream_id`; replay uses
internal stream ID `1` for them. `stream_id` is optional for other protocols.

`response_flags`, when present on a canonical request, MUST be a JSON array
containing only strings. Canonical requests do not accept Envoy's raw string
representation.

`sequence` is optional. Replay derives a strictly increasing sequence from
canonical request order independently for each `node` + `connection_id`. If a
producer supplies `sequence`, replay preserves it and rejects a decrease.
`connection_close` requires only `connection_id` plus optional `node`, passes
directly to the engine, and does not advance request sequence.

Canonical `headers`, `body`, `response_headers`, and `response_body` use the
same header-map and base64-envelope representations as raw payload fields.

Within the canonical family, malformed JSON, absent or invalid explicit types,
raw Envoy observations, `connection_open`, and unknown event types are rejected
with the physical input line number.

### 3.4 Direct Completion Replay Input

Direct completion input is a quick-verification convenience, not the
recommended fidelity path. Every record MUST either use the exact,
case-sensitive `type: "DownstreamEnd"` or omit `type`. Explicit and untyped End
records MAY coexist.

Each record MUST contain integer `connection_id`, valid RFC3339 `timestamp`,
nonempty `method`, `authority`, `path`, and `protocol`, and integer
`response_code`. `request_id` is optional and is preserved when supplied.
`node`, `scheme`, `sequence`, `stream_id`, request metadata, and response
metadata are optional and use their existing event representations.

`response_flags` is optional. When present, it MUST be an Envoy string. `-` and
the empty string mean no flags; every other value is split on commas into exact,
nonempty tokens. Arrays, non-strings, and empty tokens are invalid. An HTTP/1.1
record MUST omit `stream_id` or use integer `1`; replay normalizes either form
to internal stream ID `1`. A nonempty, non-`-` `user_agent` is copied to
`headers["user-agent"]` only when no case-insensitive user-agent header exists.

The parser normalizes every accepted direct record to `request`. For each
connection, it derives sequence in file append order when `sequence` is omitted
or non-positive. A supplied positive sequence is preserved, and a decrease is
rejected. Replay MUST NOT sort by timestamp, duration, or stream ID. Completion
append order can be response-completion order rather than request-start order.

One input file MUST contain only canonical events or only direct completion
records. Crossing those families is fatal before the conflicting record is
forwarded; records already forwarded are not rolled back. Operators MUST run
`-dry-run` before sending traffic. `type: null`, empty, lowercase, or unknown
types, `DownstreamStart`, and `connection_open` are invalid.

After the first direct record passes validation and immediately before it is
forwarded, replay MUST log this warning exactly once per file:

> DownstreamEnd access logs are suitable only for quick verification because
> request order is not guaranteed; use combined logs to preserve replay
> fidelity

Direct input does not synthesize `connection_close`, including for the exact
`DC` flag. Safe marker placement requires Start order. Direct-input connections
therefore finalize only at EOF.

### 3.5 Capture Data Handling

Capture files can contain credentials, cookies, and personal data. Operators
MUST restrict their storage and access and SHOULD redact sensitive fields before
sharing a capture. Target overrides and header rewrite rules alter outbound
replay requests only; they do not redact values already stored in an input
file.

---

## 4. Replay Semantics

Replay engine MUST:

1. Read accepted input events in append order without buffering the full capture.
2. Route every event by `node` + `connection_id` to exactly one replay worker.
3. Preserve the parser's per-connection monotonic `sequence`; replay MUST NOT reorder events.
4. Open one replay connection state on the first request for each `node` + `connection_id`.
5. Replay requests in observed connection order for HTTP/1.1 and serialized HTTP/2.
6. In multiplexed HTTP/2 mode, dispatch request sends concurrently as request events arrive.
7. When pacing is enabled, schedule requests on a shared recorded timeline (Section 4.3), with pacing state per connection even when a worker owns multiple connections.
8. Time elapsed while replaying a request MUST consume the corresponding timestamp delta; synchronous request latency MUST NOT be followed by another sleep for the full recorded delta.
9. When pacing timestamps move backward or stay equal, keep the existing pacing clock and do not sleep.
10. On `connection_close`, wait for in-flight HTTP/2 work, close transport resources, and finalize the connection.
11. At EOF, perform the same finalization for every remaining connection.

DC-derived canonical close markers provide confirmed connection termination.
Canonical connections without DC and all direct-completion connections remain
active until EOF. Direct completion input cannot place a DC-derived close
safely because it lacks request-start order.

Replay MUST NOT automatically follow HTTP redirects (`301`, `302`, `303`,
`307`, or `308`), whether `Location` is relative or absolute. For redirects
with a syntactically valid `Location` URL reference, the original status,
headers, and body MUST remain available to configured response validation;
latency, status, and response-byte metrics MUST describe that response rather
than a redirect chain. Receiving such a redirect alone MUST NOT be treated
as a transport error or abort the connection. A follow-up request MUST be
sent only when its own captured request event is scheduled. Automatic
redirect following is not a configurable mode.

For redirect responses with a malformed `Location` URL reference, replay MUST
record a non-retryable request failure with synthetic status
`malformed_redirect`, discard and close that response, and continue with the
next scheduled request on the recorded connection. The response is not
available for validation or successful-response metrics. Such a failure MUST
NOT abort the recorded connection or clear an abort caused by another request;
it counts as a send error and makes the run `partial_success`. Reusing the
same underlying TCP connection is not guaranteed after discarding the response.

Replay retains `http.Client` timeouts, authentication, and error wrapping.
A wrapper around the standard transport identifies malformed redirect
locations using a typed error, not Go's error-message wording. Other transport
failures retain the existing retry and connection-abort behavior. Malformed
`Location` values on non-redirect responses are not interpreted.

Protocol fidelity is strict-only. Replay MUST trim outer whitespace and
normalize case, accepting only `HTTP/1.1`, `HTTP/2`, and `HTTP/2.0`; both HTTP/2
forms normalize to `HTTP/2.0`. Missing or unsupported protocols and inconsistent
normalized protocols within one `(node, connection_id)` MUST be rejected,
including during dry-run. Streaming rejection does not roll back requests
already forwarded. There is no protocol override or automatic fallback.

The captured protocol describes the client-to-Envoy downstream leg, not
Envoy-to-application upstream traffic. A destination override MUST NOT change
the recorded protocol; the new target must support it even if Envoy originally
translated between downstream HTTP/2 and upstream HTTP/1.1.

Replay MUST enforce the expected protocol on every response before treating it
as usable or applying optional status/header/body validation. A protocol
failure MUST NOT be retried, counted as a usable response, or reclassified as
an ordinary network failure. See Section 6.3 for run and diagnostic behavior.

### HTTP/1.1

* Sequential replay per connection
* Preserve keep-alive behavior
* MUST NOT negotiate or upgrade to HTTP/2.

### HTTP/2

HTTP/2 over TLS MUST advertise only `h2` in ALPN and require its negotiation.
Certificate verification remains enabled unless
`replay.tls.insecure_skip_verify` is explicitly configured; that setting MUST
NOT relax ALPN or response protocol enforcement. Cleartext HTTP/2 MUST use
prior-knowledge h2c, not HTTP/1 upgrade or fallback. An incompatible HTTP/1
server may expose the HTTP/2 preface (`PRI * HTTP/2.0`) to its request handler;
this is not a replayed application request over HTTP/1.

If Go's HTTP/2 implementation is disabled, including by `GODEBUG=http2client=0`
or the `nethttpomithttp2` build tag, sending a recorded HTTP/2 request MUST fail
as a non-retryable protocol failure before sending application bytes. This
applies to TLS and cleartext destinations alike.

Two supported modes:

1. Serialized mode, which sends requests one at a time in observed connection order. It intentionally changes recorded concurrency, not the HTTP/2 wire protocol.
2. Multiplexed mode, which sends HTTP/2 requests concurrently on the shared per-connection client and joins in-flight requests at EOF.

Checkpoint advancement in multiplexed mode follows Section 4.2.

Replay consumes HTTP/2 requests in input append order and does not reorder them
by `timestamp`, `stream_id`, or `sequence`. `replay combine` establishes
canonical order from `DownstreamStart` observations while merging response
metadata from the corresponding Ends. Direct completion input preserves End
append order instead. Multiplexed mode may execute requests concurrently; a
canonical close marker waits for all admitted streams before finalizing the
connection.

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
* Apply capacity controls per replay engine (see Section 6.2).

By default, engines reading the same complete input select the same capture
origin before shard filtering, but independently select their replay starts.
Pre-sharded files may additionally select different origins. The engine API
accepts a common origin/start pair (Section 4.3), but does not distribute
schedules, ensure engine readiness, or synchronize host clocks. The CLI uses
automatic timing and MUST NOT claim cross-shard burst fidelity. Coordinated
callers need the same origin/start pair across engines, comparable capture
timestamps, sufficiently synchronized host clocks, and engines ready in time.

### 4.2 Checkpoint Persistence

Setting `replay.checkpoint.file` enables resumable replay. The engine records a
monotonic completed-sequence watermark for each `node` + `connection_id` and
skips input at or below a loaded watermark. Multiplexed HTTP/2 MUST advance the
watermark only after every earlier admitted sequence reaches a terminal,
checkpointable state.

A malformed-redirect request failure is terminal and checkpointable even
though its response cannot be validated; resuming MUST NOT resend it once
its sequence is included in the persisted watermark.

Dirty progress MUST be persisted at `replay.checkpoint.sync_interval`, which
defaults to `1s`, and flushed during orderly shutdown. An abrupt process or host
failure can repeat requests completed since the last successful sync.
Checkpoint read, parse, version, and persistence failures MUST fail the run
when observed.

When `shard_count` is greater than one, each shard MUST use an isolated
checkpoint. Replay derives the path by appending
`.shard-<shard_index>-of-<shard_count>` to the configured file.

Checkpoint skipping happens after pacing. A resumed invocation uses its supplied
schedule or, by default, selects a fresh replay start and the origin of its input,
including skipped requests. It MUST NOT rebase each connection on its first
non-skipped request. With automatic timing, replaying the full file therefore
waits through the skipped prefix. Checkpoint data does not persist timing state
or provide a fast-forward clock.

### 4.3 Recorded Timing

`replay.pacing.enabled` defaults to `true` and is configured in YAML.
Disabling pacing MUST disable recorded-timing waits; it does not disable
ramp-up, retry backoff, or protocol ordering. Dry-run MUST exercise pacing
without sending requests.

`Engine.ReplayStream(ctx, events, schedule)` accepts an optional per-invocation
`*ReplaySchedule`:

```go
type ReplaySchedule struct {
    CaptureOrigin time.Time
    ReplayStart   time.Time
}
```

A nil schedule selects automatic timing. A supplied schedule MUST contain both
nonzero timestamps; an incomplete schedule MUST fail initialization. The engine
MUST copy the supplied pair at invocation entry and anchor its wall-clock
`ReplayStart` to the local monotonic clock once. Later wall-clock adjustments,
input arrival, and worker activation MUST NOT rebase that schedule. Past starts
are accepted: overdue requests proceed without extra timing waits. Supplying a
schedule MUST NOT override `replay.pacing.enabled: false`.

The CLI passes a nil schedule; there are no YAML or CLI schedule controls.

When pacing is enabled:

1. Without a supplied schedule, the capture origin MUST be the first valid
   request timestamp in input append order, selected before shard filtering or
   checkpoint/idempotency skipping. It is not necessarily the minimum timestamp;
   selecting it requires neither a pre-pass nor buffering the full input.
2. Without a supplied schedule, the replay start MUST be selected when the router
   observes that request. With a supplied schedule, both timestamps MUST come
   from that schedule, even if the input begins later than its capture origin.
   The timeline belongs to one invocation and is shared by all its workers;
   reusing an engine MUST NOT retain a previous invocation's timing state.
3. Each connection's first intended deadline MUST be
   `replay_start + (connection_first_timestamp - capture_origin)`, even when
   its worker processes that request late. For subsequent increasing timestamps,
   the deadline MUST be `replay_start + (recorded_timestamp - capture_origin)`.
4. Neither initial offsets nor subsequent gaps are capped, because
   connection-local capping can destroy cross-connection alignment.
5. A deadline at or before the current time is overdue and MUST NOT add a wait.
   Later equal/backward timestamps MUST retain the per-connection high-water
   timestamp and deadline without sleeping or reordering requests.
6. Worker ramp-up MUST NOT shift the shared timeline. Late requests proceed
   without an additional wait and MUST NOT rebase later deadlines. Operators
   SHOULD disable ramp-up when preserving burst timing.
7. Timing waits, including initial offsets, MUST be cancellable.

For A recorded at `[0s, 10s]` and B at `[9s, 10s]`, pacing MUST intend
A=`[0s, 10s]`, B=`[9s, 10s]`. Enough VUs reduce worker contention; the shared
clock preserves recorded offsets. These are separate requirements for faithful replay.

Intended deadlines are not guarantees of actual transmission time. A worker
sleeping or sending for one connection can block other connections assigned to
it; a full worker channel can block routing to other workers. HTTP/1.1 MUST
remain sequential, so slow responses can delay later requests. Input delivery,
HTTP/2 stream admission, transport behavior, target latency, and generator
capacity also affect achieved burst fidelity. Send-attempt lateness can be
measured using the optional histogram defined in Section 6.4; pacing does
not add a global timestamp sorter or change connection ownership.

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
6. Destination overrides MUST preserve the recorded HTTP protocol; no protocol-override setting is supported.

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
3. Each recorded connection SHOULD own its outbound transport until an explicit `connection_close` or EOF so keep-alive reuse and socket isolation are preserved.
4. Implementations MUST preserve per-connection event ordering when a VU drives multiple connections.
5. The specification does not require `max_requests_per_second` and does not use it as a primary control.

The VU limit bounds replay workers only. It does not bound per-connection state
or transports for currently active connections, nor concurrent streams
dispatched by multiplexed HTTP/2 connections. Active connection-state and
transport memory therefore depends on the number of simultaneously active
`node` + `connection_id` identities assigned to the engine.

Replay retains one aggregate `ConnectionResult` for every finalized connection
until the run summary is consumed. It contains `node`, `connection_id`,
`outcome`, and aggregate `requests_sent`, `responses_received`, `send_errors`,
`protocol_failed`, `validation_failed`, and `skipped` counters. Protocol-failed
`RequestResult` details are retained in `ConnectionResult.Requests`; successful
request details are not retained, and `Summary.RequestResults` is not populated.

Result memory therefore depends on total connections processed (including
finalized identities) and retained protocol failures, not all successful
requests. Request concurrency additionally depends on HTTP mode and target
latency.

Distributed replay note:

* In multi-engine deployments, the VU limit applies per engine.
* The sum of per-engine worker limits is aggregate VU capacity, not an aggregate
  load ceiling. Operators SHOULD size and shard replay engines for each shard's
  unique connection count and multiplexed stream concurrency.

### 6.3 Replay Outcome Model

Replay execution MUST produce deterministic run outcomes at three levels: request, connection, and run.

Request outcome classes:

* `sent`: request was emitted to target.
* `send_error`: request failed before receiving a usable HTTP response (for example connect timeout, TLS error, network reset, or malformed redirect location).
* `protocol_failed`: the recorded protocol could not be preserved, independently of response validation.
* `response_received`: response was received from target.
* `validation_failed`: response was received but did not match configured validation rules.
* `skipped`: request was intentionally not sent (for example policy guard or dry-run filter).

Connection outcome classes:

* `completed`: all scheduled requests for the connection reached terminal request outcomes and connection closed normally.
* `aborted`: connection terminated early due to unrecoverable error or policy stop.

Run outcome classes:

* `success`: no fatal engine errors or send/protocol/validation failures, and all non-skipped requests reached terminal outcomes.
* `partial_success`: replay completed with non-fatal send/validation failures.
* `failed`: a fatal condition occurred (for example invalid input, backend unavailable, policy violation), or any request failed protocol fidelity, even if replay otherwise completed.

Exit status guidance:

* Engine SHOULD return exit code `0` for `success`.
* Engine SHOULD return non-zero exit code for `failed`.
* Engine SHOULD return exit code `0` for `partial_success` by default.
* Engine MAY make `partial_success` exit behavior configurable when operators need non-zero behavior in CI-style contexts.

Protocol failures MUST have a separate `protocol_failed` run-summary counter
and MUST make the CLI exit nonzero, independently of response validation and
`partial_success_exit_zero`. Their diagnostics MUST identify the connection,
request, expected protocol, observed protocol when available, and destination.
Normal connection, DNS, timeout, and certificate failures remain ordinary send
errors rather than protocol failures.

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
* `replay_schedule_lateness_seconds_bucket`
* `replay_schedule_lateness_seconds_sum`
* `replay_schedule_lateness_seconds_count`
* `replay_status_counter`
* `replay_egress_bytes_counter`
* `replay_threads_gauge`
* `replay_cpu_gauge`
* `replay_mem_gauge`

`replay_threads_gauge` reports active virtual users. It increments when a replay
worker starts and decrements when that worker finishes.

Schedule-lateness collection is opt-in through the YAML setting
`metrics.schedule_lateness_enabled`, which defaults to `false`. The registry
MUST NOT construct or register the histogram unless both `metrics.enabled` and
`metrics.schedule_lateness_enabled` are true. Recording lateness through a
registry without the collector is a no-op. The engine MUST skip
lateness-specific label work when the collector is absent. This switch MUST NOT
change pacing or request execution behavior.

`replay_schedule_lateness_seconds` is a histogram of
`max(0, first_client_send_attempt - intended_deadline)` in seconds, with common
labels plus the bounded request-path `label`. With collection and pacing enabled
and a valid pacing deadline, the engine MUST observe it once per recorded request reaching
its first `http.Client.Do` call, including calls returning transport errors.
The timestamp is taken at the client-call boundary, not at wire transmission
or target receipt; transport queues, connection setup, and HTTP/2 admission can
add further delay. Retries MUST NOT add observations. Pacing-disabled sends,
dry-runs, policy/checkpoint skips, and failures before the client call MUST NOT
add observations.

Engine-specific integrations MAY configure a different Prometheus namespace and common label set. Label conventions SHOULD include configurable common dimensions plus metric-specific labels such as `label`, `status`, and `le`.

For `replay_status_counter`, the `status` label MAY contain either a numeric
HTTP status code or a synthetic error status such as `timeout`,
`connection_refused`, `connection_reset`, `tls`, `network`, `send_error`, or
`malformed_redirect`. A request that fails before receiving a usable HTTP
response MUST increment the counter with its synthetic error status.

Engines SHOULD support a `metrics.path_templates` list to prevent dynamic path
segments from creating unbounded metric-label cardinality. A segment enclosed
in braces (for example, `{id}` in `/users/{id}`) is dynamic; all other segments
are literal. Matching MUST use the path without its query string, require the
same segment count, and compare literal segments exactly. The first matching
template MUST become the metric-specific `label`; when no template matches, the
path without its query string MUST remain the label.

Every path template MUST be an absolute path with at most 64 nonempty segments
and no trailing slash, dot segment, query, or fragment. A wildcard MUST occupy
an entire segment and use a name of at most 64 bytes matching
`[A-Za-z_][A-Za-z0-9_]*`. Literal segments MUST use RFC 3986 path characters,
with non-ASCII bytes percent-encoded. Duplicate templates are invalid. A
configuration MUST contain at most 256 templates, at most 2 KiB per template,
and at most 64 KiB across all templates.

Each `metrics.common_labels` entry contains `name`, a literal fallback `value`,
and an optional environment variable name in `env`. When that variable is
unset or empty, the literal value remains in use.

`metrics.graceful_termination_period` defaults to `5s`. After replay stops, the
metrics endpoint MUST remain scrapeable for the configured period and then
gracefully drain in-flight scrapes. Failure to bind the configured metrics
listener MUST fail startup.

### 6.5 Runtime Configuration (YAML)

Replay runtime behavior SHOULD be configurable via a YAML file.

Minimum configurable domains:

* Timeouts: connect timeout, request timeout, optional idle/keepalive timeout.
* HTTP/2 replay mode: serialized or multiplexed.
* Retry policy: max retries, retryable error classes/statuses, backoff strategy.
* Validation: status, header, body, and ignored-header controls.
* Pacing: enabled by default for uncapped shared-origin timing. Set `replay.pacing.enabled: false` to disable recorded-timing waits. See Section 4.3.
* Metrics server: listen address/port, endpoint enable toggle (default enabled), path (default `/metrics`).
* Capacity control: `max_virtual_users_per_engine`.

Configuration precedence (recommended):

1. Built-in defaults
2. YAML file
3. Environment variables
4. CLI flags (highest precedence)

Replay recognizes these environment overrides:

* `REPLAY_DRY_RUN`
* `REPLAY_VERBOSE`
* `REPLAY_OVERRIDE_URL`
* `REPLAY_DISALLOW_RECORDED_TARGETS`
* `REPLAY_PARTIAL_SUCCESS_EXIT_ZERO`
* `METRICS_ENABLED`
* `METRICS_NAMESPACE`
* `METRICS_LISTEN_ADDRESS`
* `METRICS_PATH`
* `METRICS_MAX_LABELS`
* `METRICS_GRACEFUL_TERMINATION_PERIOD`

Configured common-label environment references follow the resolution rules in
Section 6.4.

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
    enabled: true
  idempotency:
    enabled: true
    block_methods: [POST, PUT, PATCH, DELETE]
    require_header_for_allow: [idempotency-key, x-idempotency-key]
  sharding:
    shard_index: 0
    shard_count: 1
  checkpoint:
    file: "./checkpoint.json"
    sync_interval: 1s
metrics:
  enabled: true
  schedule_lateness_enabled: false
  namespace: "replay"
  listen_address: "0.0.0.0:9102"
  path: "/metrics"
  max_labels: 20
  graceful_termination_period: 5s
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

When idempotency safeguards are enabled, configured mutation methods are
recorded as `skipped` unless an allow header is present.

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
