# Golang Code Review

## Critical Issues

### Concurrency: Goroutine deadlock on early return in `ReplayStream`

**Problem**: If `ReplayStream` encounters a parsing error (e.g., `parseErr != nil` due to missing `connection_open`), it stops reading from the `events` channel and returns early. However, `main.go` runs `ParseFileStream` in the main goroutine, which attempts to send to the unbuffered `eventsCh`. This causes the main goroutine to block forever waiting for a reader, resulting in a deadlock.

**Current code**:
```go
// In internal/engine/engine.go (ReplayStream)
if parseErr != nil {
	// stop scheduling more jobs
	close(jobs)
	// wait for workers to finish and drain results
	go func() { wg.Wait(); close(results) }()
	// collect any partial results (ignore aggregation semantics)
	for range results {
	}
	return Summary{Outcome: RunFailed}, parseErr
}
```

**Suggested fix**:
Drain the `events` channel in a background goroutine before returning from `ReplayStream` to unblock the producer, or pass a cancellation function.

```go
if parseErr != nil {
	// Drain events to unblock the producer in main.go
	go func() {
		for range events {}
	}()
	
	close(jobs)
	go func() { wg.Wait(); close(results) }()
	for range results {
	}
	return Summary{Outcome: RunFailed}, parseErr
}
```

## Important Issues

### Resource Management: Memory leak in NDJSON parser

**Problem**: The parser maintains a `lastSeq` map to track the sequence numbers of each connection to ensure monotonicity. However, it never removes connections from this map. For large log files with millions of connections, this map will grow indefinitely and consume excessive memory.

**Current code**:
```go
// In internal/parser/ndjson.go
case model.EventConnectionClose:
	if event.ConnectionID == "" {
		return fmt.Errorf("line %d: connection_close missing connection_id", line)
	}
	if event.Reason != "" {
		// ... validation ...
	}
	// Connection is closed but not removed from lastSeq map
```

**Suggested fix**:
Remove the connection from `lastSeq` when the connection is closed to free memory.

```go
case model.EventConnectionClose:
	if event.ConnectionID == "" {
		return fmt.Errorf("line %d: connection_close missing connection_id", line)
	}
	// ... validation ...
	delete(lastSeq, event.ConnectionID) // Free memory
```

### Function Design: Mixed abstraction levels in `ReplayStream`

**Problem**: The `ReplayStream` function is very long (>150 lines) and mixes multiple levels of abstraction: setting up worker pools, buffering streams by connection, implementing sharding logic, and aggregating results. According to Teetsh patterns, functions should operate at a single level of abstraction to make intent clear.

**Current code**:
`ReplayStream` is a monolithic function handling everything from context cancellation down to specific event type buffering logic.

**Suggested fix**:
Extract the event buffering and job generation logic into a separate helper method.

```go
func (e *Engine) ReplayStream(ctx context.Context, events <-chan model.Event) (Summary, error) {
	// 1. Setup workers
	jobs, results, wg := e.startWorkers(ctx, checkpoints)
	
	// 2. Buffer events and emit jobs
	parseErr := e.bufferAndSchedule(events, jobs)
	close(jobs)
	
	// 3. Aggregate results
	return e.aggregateResults(results, parseErr)
}
```

## Suggestions

### Code Clarity: Inefficient draining loop on context cancellation

**Problem**: In the worker pool loop in `ReplayStream`, when the context is canceled, it continues to read from the `jobs` channel and emit failures instead of breaking out early. While this drains the channel, it's an inefficient pattern for shutdown.

**Current code**:
```go
for job := range jobs {
	select {
	case <-ctx.Done():
		results <- Summary{Outcome: RunFailed}
		continue
	case connSem <- struct{}{}:
	}
	// ...
}
```

**Suggested fix**:
Since draining can be safely handled by `close(jobs)` unblocking other components, returning early is usually cleaner.

```go
for job := range jobs {
	select {
	case <-ctx.Done():
		return 
	case connSem <- struct{}{}:
	}
	// ...
}
```

## Positive Feedback

### Safe Resource Handling
The use of `io.LimitReader` when reading HTTP response bodies (`io.LimitReader(resp.Body, maxBodyRead)`) is a great defensive programming practice that protects against OOM errors from unexpectedly large payloads.

### Good Error Context
The codebase properly uses Go 1.13+ error wrapping (`fmt.Errorf("...: %w", err)`) in most places, preserving stack context and allowing for proper `errors.Is`/`errors.As` checks.
