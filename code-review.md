# Replay Code Review

## Findings

### 1. High: checkpoint flusher goroutine is never shut down

The replay entrypoint creates a checkpoint store in [internal/engine/engine.go#L120](internal/engine/engine.go#L120), but there is no matching `Close()` call before `ReplayStream` returns. The store starts a background flusher in [internal/engine/checkpoint.go#L31](internal/engine/checkpoint.go#L31) and only stops it through [internal/engine/checkpoint.go#L187](internal/engine/checkpoint.go#L187).

Impact: every run that enables `checkpoint.file` leaks a goroutine and skips the explicit final flush path unless the process happens to exit. This is especially risky if `ReplayStream` is reused from tests, libraries, or long-lived processes.

Recommendation: call `defer checkpoints.Close()` immediately after successful store creation in `ReplayStream`.

### 2. High: `override_url` corrupts recorded request paths that contain query strings

The spec explicitly records the full request target in `http.path`, including query strings such as `/api/v1/login?redirect=/home` in [specs.md#L123](specs.md#L123). When `override_url` is enabled, the engine assigns that raw value to `override.Path` in [internal/engine/engine.go#L963](internal/engine/engine.go#L963).

In Go's `net/url`, assigning a string containing `?` to `URL.Path` percent-encodes the question mark, so the replay target becomes `/api/v1/login%3Fredirect=/home` instead of preserving the original query.

Impact: any replay that relies on `override_url` silently sends the wrong endpoint for requests with query parameters.

Recommendation: parse `event.HTTP.Path` as a request URI and populate `Path` and `RawQuery` separately, or resolve it through `url.Parse` before merging it into the override URL.

### 3. High: connection replay continues after a send error instead of aborting the logical connection

The implementation marks the connection as aborted when `sendRequest` fails in [internal/engine/engine.go#L414](internal/engine/engine.go#L414), but then immediately `continue`s at [internal/engine/engine.go#L418](internal/engine/engine.go#L418). The spec requires replay to preserve per-connection behavior and keep-alive semantics in [specs.md#L236](specs.md#L236).

Impact: once a request on a logical connection fails, later requests from the same `connection_id` are still attempted. In practice that can replay traffic on a fresh socket after the original connection already failed, which no longer matches the recorded lifecycle.

Recommendation: treat transport-level send failures as terminal for that logical connection. Record the failure, mark the connection aborted, and stop replaying the remaining requests for that `connection_id`.

### 4. Medium: missing-method requests are interpreted inconsistently between safety checks and execution

The parser validates authority, path, and HTTP/1.1 stream rules in [internal/parser/ndjson.go#L81](internal/parser/ndjson.go#L81), but it never requires `http.method`. When the method is missing, the idempotency guard in [internal/engine/engine.go#L649](internal/engine/engine.go#L649) infers `POST` for body-bearing requests in [internal/engine/engine.go#L651](internal/engine/engine.go#L651), while the actual request execution path defaults the same event to `GET` in [internal/engine/engine.go#L750](internal/engine/engine.go#L750).

Impact: the same malformed recorded event can be classified as a blocked mutation by one path and executed as a safe `GET` by another. That makes replays non-deterministic and can hide or distort write traffic.

Recommendation: either reject request events without `http.method` during parsing, or centralize method inference so policy checks and actual execution use exactly the same resolved method.

## Validation

`go test ./...` passes in the current tree. The findings above are based on code-path inspection and one runtime check of the `override_url` path handling.