# Connection Replay and Capture Fidelity

## Purpose

Replay reconstructs HTTP work from canonical request events while preserving
connection identity, request-start order, protocol behavior, and captured
response expectations. Raw Envoy observations are preprocessing input, not
engine input.

The authoritative workflow is:

```text
mixed Envoy DownstreamStart/DownstreamEnd NDJSON
  → replay combine
  → canonical request/connection_close NDJSON
  → replay -log
```

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

Both sides carry an Envoy-generated `request_id`, `node`, `connection_id`,
request start timestamp, method, authority, path, and protocol. The selected
Envoy image remains `v1.28-latest`; no Lua `connectionStreamInfo()` counter is
required.

Capture one chronological stdout stream, then prepare it explicitly:

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

This provides original request-admission order together with End-side response
validation metadata. Raw Start and End observations must never be sent directly
to the engine: doing so is rejected rather than replaying a request twice.

## Canonical Replay Input

A canonical request has explicit `type: "request"`, a nonempty `request_id`,
connection identity, request timestamp and descriptors, `response_code`, and
optional request/response metadata:

```json
{"type":"request","node":"envoy-a","connection_id":42,"request_id":"opaque-id","timestamp":"2026-08-21T10:00:00.100Z","method":"GET","scheme":"https","authority":"example.test","path":"/items","protocol":"HTTP/2","stream_id":7,"response_code":200,"duration_ms":50,"response_flags":["DC"]}
```

Response flags are JSON string arrays in canonical input. The replay parser does
not accept Envoy's string representation, raw `DownstreamStart`, raw
`DownstreamEnd`, omitted types, or `connection_open`.

Replay consumes canonical records in append order. It does not reorder by
timestamp, stream ID, response duration, or sequence. HTTP/1.1 defaults a
missing stream ID to `1` and rejects any other value.

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

## Sharding and Checkpoints

Connection identity is always `(node, connection_id)`. Sharding hashes that
complete key so every event for a connection reaches one replay process in
canonical order. Splitting a connection by byte offsets, line ranges, or
arbitrary timestamp windows is invalid.

Parser-assigned per-connection sequences drive checkpoints. Because combined
files omit sequence and preserve Start order, checkpoint watermarks are stable
across repeated parses of the same canonical file. A close event neither
advances nor resets request sequence.

## Fidelity Boundary

The workflow preserves exact pairing and recorded request-start order when both
observations carry the same stable request ID. It preserves End-side status,
headers, body, duration, and response flags. It confirms connection termination
only when an End contains exact `DC`; otherwise termination is inferred at EOF.

Missing, duplicate, conflicting, or unmatched observations are fatal. There is
no heuristic fallback and no partial canonical output.
