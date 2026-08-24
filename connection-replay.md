# Connection Replay and Capture Fidelity

## Purpose

Replay reconstructs HTTP work from canonical request events while preserving
connection identity, per-connection request order, protocol behavior, and
captured response expectations. Paired Envoy observations are preprocessing
input. End-only completion logs have a separate quick-verification path with
lower ordering and connection-close fidelity.

The authoritative fidelity workflow is:

```text
mixed Envoy DownstreamStart/DownstreamEnd NDJSON
  → replay combine
  → canonical request/connection_close NDJSON
  → replay -log
```

The alternate convenience workflow is:

```text
End-only DownstreamEnd NDJSON
  → replay -log -dry-run
```

Use that path only for quick verification. Combined logs remain authoritative
for replay fidelity.

## Current Connection Execution Model

The engine routes every event for one `(node, connection_id)` to the same
worker. Assignment is static and round-robin on a connection's first event.
This preserves connection affinity and per-connection event order. A configured
virtual user is worker capacity, not a promise that one worker owns exactly one
recorded downstream connection.

Static ownership is part of the current execution model. Log-fidelity work does
not require replacing it with an availability scheduler, a two-pass connection
index, or dynamic VU leasing. Those would be separate scheduling-policy
changes with different behavior and resource tradeoffs.

Each worker retains logical connection state and an HTTP transport until an
explicit `connection_close` or EOF. HTTP/1.1 requests execute synchronously per
connection. Multiplexed HTTP/2 requests share the connection client, execute
concurrently, and are joined before connection finalization.

## Preparing Envoy Logs

### Mixed raw capture

The bundled Envoy configuration emits two flat observations for every request:

```text
DownstreamStart
  → request identity and request-start order

DownstreamEnd
  → response status, duration, metadata, and response flags
```

Both sides carry `request_id`, `node`, `connection_id`, request start timestamp,
method, authority, path, and protocol. The example records `X-Request-ID` as
`request_id`; Envoy generates a value when the header is absent and may preserve
a caller-supplied value. The capture contract treats it as physical-request
identity, so every retry and fan-out request must use a value not reused on the
same connection during the capture. No Lua `connectionStreamInfo()` counter is
required.

Capture one append-only stdout stream, then prepare it explicitly:

```bash
kubectl logs --follow deployment/envoy-recorder-proxy \
  --container envoy --tail=0 > requests.log

go run ./cmd/replay combine \
  -log requests.log \
  -out canonical.ndjson
```

`combine` accepts plain, gzip, or zstd input. Output is always plain NDJSON and
is atomically installed only after the complete capture validates.

### Strict request pairing

The pair key is:

```text
(node, connection_id, request_id)
```

`request_id` establishes identity, not order. The combiner never infers a pair
from timestamps, methods, paths, FIFO position, or response completion order.
It accepts End before Start because collection can reorder observations, but it
rejects missing IDs, duplicate sides, shared-field conflicts, and unmatched
observations.

Node, connection ID, request ID, timestamp, method, authority, path, and
protocol must agree. Nonempty scheme and nonzero stream ID values must also
agree; one side may supply a value omitted by the other.

The combined request uses Start identity, timestamp, and immutable request
fields. It preserves Start request headers and body when present, otherwise
fills them from End. End supplies response code, duration, response headers,
response body, and response flags. Envoy's `response_flags` string is split
into exact tokens; `-` and the empty string mean no flags.

### Start-ordered canonical output

HTTP/2 streams complete independently. For example:

```text
A starts, then B starts
B ends, then A ends
```

End log order is `B, A`; canonical request order is `A, B`. The combiner queues
requests by global Start observation order and merges each exact End into its
request. It does not serialize `sequence`. The replay parser assigns a
monotonic sequence from canonical order independently for each connection key.

Canonical order does not impose global execution order across connections. The
engine preserves canonical order within each connection; independent workers
may execute different connections concurrently.

This provides original request-admission order together with End-side response
validation metadata. A file containing `DownstreamStart`, including a mixed
Start/End capture, must never be sent directly to the engine; use
`replay combine` so each request is emitted once in Start order.

### End-only quick verification

When a capture intentionally contains only completed requests, replay accepts
each record with exact `type: "DownstreamEnd"` or with no `type`. `request_id`
and Envoy string `response_flags` are optional:

```bash
go run ./cmd/replay -log downstream-end.ndjson -dry-run
```

Replay emits one warning for the file:

> DownstreamEnd access logs are suitable only for quick verification because
> request order is not guaranteed; use combined logs to preserve replay
> fidelity

The parser normalizes each End to a request. It derives per-connection sequence
in file append order when `sequence` is omitted or non-positive. A supplied
positive sequence is preserved, and a decrease is rejected. Replay does not
reorder by request timestamp, duration, or stream ID. For overlapping streams,
End append order can be `B, A` even when request-start order was `A, B`.

Direct End input cannot recover Start order, Start-preferred request headers or
body, strict Start/End pair validation, or safe placement of a `DC`-derived
connection close. It therefore retains EOF connection finalization. Files must
contain one family: canonical `request`/`connection_close` events or direct
explicit/untyped Ends, never both.

## Canonical Replay Input

A canonical request has explicit `type: "request"`, a nonempty `request_id`,
connection identity, request timestamp and descriptors, `response_code`, and
optional request/response metadata:

```json
{"type":"request","node":"envoy-a","connection_id":42,"request_id":"opaque-id","timestamp":"2026-08-21T10:00:00.100Z","method":"GET","scheme":"https","authority":"example.test","path":"/items","protocol":"HTTP/2","stream_id":7,"response_code":200,"duration_ms":50,"response_flags":["DC"]}
```

Canonical response flags are JSON string arrays; Envoy's string representation
belongs only to direct completion input. A canonical file does not contain raw
`DownstreamStart`, raw `DownstreamEnd`, omitted types, or `connection_open`.

Replay consumes canonical records in append order. It does not reorder by
timestamp, stream ID, response duration, or sequence. HTTP/1.1 canonical
records omit `stream_id`; replay uses internal stream ID `1` for those
requests.

## Downstream Connection Termination

Envoy's exact `DC` token means `DownstreamConnectionTermination` for the End
observation carrying it. It is evidence that the recorded downstream
connection ended while HTTP work was active.

The combiner translates this evidence into one canonical event:

```json
{"type":"connection_close","node":"envoy-a","connection_id":42}
```

The marker is placed after that connection's final request in global Start
order, not immediately after the request whose End contains `DC`. This matters
for overlapping HTTP/2 work: a later-started stream may already belong to the
same recorded connection even if an earlier request's End reports DC.

When the engine receives the marker, it stops extending that connection state,
waits for admitted HTTP/2 requests, collects results, closes transport
resources, and finalizes the connection. Exactly one marker is emitted per
DC-bearing connection even if multiple Ends contain DC.

DC does not describe a clean idle close that happens after the final request.
Connections without DC therefore have no synthetic marker and retain the
existing EOF finalization behavior.

Direct completion input never synthesizes this marker, even when an End
contains exact `DC`: without Start order, replay cannot know a safe close
position relative to overlapping requests. Every direct-input connection is
therefore finalized at EOF.

## Sharding and Checkpoints

Connection identity is always `(node, connection_id)`. Sharding hashes that
complete key so every event for a connection reaches one replay process in
input append order. Splitting a connection by byte offsets, line ranges, or
arbitrary timestamp windows is invalid.

Parser-assigned per-connection sequences drive checkpoints. Because combined
files omit sequence and preserve Start order, checkpoint watermarks are stable
across repeated parses of the same canonical file. A close event neither
advances nor resets request sequence.

For direct completion files, omitted or non-positive sequences are derived from
End append order. Supplied positive values become checkpoint sequences and must
remain monotonic within each connection.

## Fidelity Boundary

The combined workflow preserves exact pairing and recorded request-start order
when both observations carry the same stable request ID. It preserves
Start-preferred request metadata, End-side status, headers, body, duration, and
response flags. It confirms connection termination only when an End contains
exact `DC`; otherwise termination is inferred at EOF.

Missing, duplicate, conflicting, or unmatched paired observations are fatal.
There is no heuristic fallback and no partial canonical output.

The direct End-only workflow preserves only completion-record append order and
the fields available on each End. It cannot validate pairs, recover Start
order, select Start-preferred request metadata, or place a DC-derived close.
Use it for `-dry-run` and quick verification; use combined logs for traffic
replay fidelity.
