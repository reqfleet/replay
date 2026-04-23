package engine

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/reqfleet/replay/internal/config"
	"github.com/reqfleet/replay/internal/metrics"
	"github.com/reqfleet/replay/internal/model"
)

type RunOutcome string

const (
	RunSuccess        RunOutcome = "success"
	RunPartialSuccess RunOutcome = "partial_success"
	RunFailed         RunOutcome = "failed"
)

type RequestOutcome string

const (
	RequestSent             RequestOutcome = "sent"
	RequestSendError        RequestOutcome = "send_error"
	RequestResponseReceived RequestOutcome = "response_received"
	RequestValidationFailed RequestOutcome = "validation_failed"
	RequestSkipped          RequestOutcome = "skipped"
)

type ConnectionOutcome string

const (
	ConnectionCompleted ConnectionOutcome = "completed"
	ConnectionAborted   ConnectionOutcome = "aborted"
)

type RequestResult struct {
	ConnectionID     int            `json:"connection_id"`
	Sequence         int            `json:"sequence"`
	Outcome          RequestOutcome `json:"outcome"`
	StatusCode       int            `json:"status_code,omitempty"`
	Error            string         `json:"error,omitempty"`
	LatencyMS        float64        `json:"latency_ms,omitempty"`
	ValidationFailed bool           `json:"validation_failed,omitempty"`
	Skipped          bool           `json:"skipped,omitempty"`
}

type ConnectionResult struct {
	ConnectionID int               `json:"connection_id"`
	Outcome      ConnectionOutcome `json:"outcome"`
	Requests     []RequestResult   `json:"requests"`
}

type Summary struct {
	RequestsSent       int64
	ResponsesReceived  int64
	SendErrors         int64
	ValidationFailed   int64
	Skipped            int64
	ConnectionsDone    int64
	ConnectionsAborted int64
	Outcome            RunOutcome
	RequestResults     []RequestResult
	ConnectionResults  []ConnectionResult
}

type Engine struct {
	cfg     config.Config
	metrics *metrics.Registry
	client  *http.Client
}

const maxBodyRead = 10 * 1024 * 1024 // 10 MiB

type requestExecution struct {
	latencyMS   float64
	statusCode  int
	egressBytes int64
	headers     map[string][]string
	body        []byte
}

func New(cfg config.Config, registry *metrics.Registry) *Engine {
	dialer := &net.Dialer{Timeout: cfg.Replay.Timeout.Connect, KeepAlive: cfg.Replay.Timeout.IdleConnection}
	transport := &http.Transport{
		DialContext:         dialer.DialContext,
		TLSClientConfig:     &tls.Config{MinVersion: tls.VersionTLS12},
		IdleConnTimeout:     cfg.Replay.Timeout.IdleConnection,
		MaxIdleConns:        cfg.Replay.MaxActiveConnectionsPerEngine,
		MaxIdleConnsPerHost: cfg.Replay.MaxActiveConnectionsPerEngine,
	}
	client := &http.Client{
		Timeout:   cfg.Replay.Timeout.Request,
		Transport: transport,
	}

	return &Engine{cfg: cfg, metrics: registry, client: client}
}

// ReplayStream processes events from the provided channel as they arrive.
// Connections are scheduled for replay when their corresponding
// connection_close event is observed or when the input channel is closed.
// This allows streaming large logs without holding all events in memory.
func (e *Engine) ReplayStream(ctx context.Context, events <-chan model.Event) (Summary, error) {
	checkpoints, err := newCheckpointStore(e.cfg.Replay.Checkpoint.File)
	if err != nil {
		return Summary{Outcome: RunFailed}, err
	}
	if checkpoints != nil {
		defer checkpoints.Close()
	}

	e.metrics.SeedEngineLabels(e.cfg.Labels)
	jobs := make(chan replayJob)
	results := make(chan Summary, 1024)

	var wg sync.WaitGroup
	e.startWorkers(ctx, &wg, jobs, results, checkpoints)

	parseErr := e.bufferAndSchedule(ctx, events, jobs)
	if parseErr != nil {
		// drain remaining events to unblock producer
		go func() {
			for range events {
			}
		}()
		// stop scheduling more jobs
		close(jobs)
		// wait for workers to finish and drain results
		go func() { wg.Wait(); close(results) }()
		for range results {
		}
		return Summary{Outcome: RunFailed}, parseErr
	}

	close(jobs)

	// close results when workers are done
	go func() {
		wg.Wait()
		close(results)
	}()

	return e.aggregateResults(results), nil
}

type connBuf struct {
	requests  []model.Event
	responses map[int]model.Event
	hasOpen   bool
	hasClose  bool
}

func (e *Engine) startWorkers(ctx context.Context, wg *sync.WaitGroup, jobs <-chan replayJob, results chan<- Summary, checkpoints *checkpointStore) {
	vus := e.cfg.Replay.MaxVirtualUsersPerEngine
	connSem := make(chan struct{}, e.cfg.Replay.MaxActiveConnectionsPerEngine)

	for i := 0; i < vus; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				select {
				case <-ctx.Done():
					return
				case connSem <- struct{}{}:
				}
				s := e.replayConnectionWithCheckpoint(ctx, job.requests, job.responsesBySequence, checkpoints)
				<-connSem
				results <- s
			}
		}()
	}
}

func (e *Engine) bufferAndSchedule(ctx context.Context, events <-chan model.Event, jobs chan<- replayJob) error {
	bufs := make(map[int]*connBuf)
	var parseErr error

	for ev := range events {
		if ev.Type == model.EventMeta {
			continue
		}
		id := ev.ConnectionID
		b := bufs[id]
		if b == nil {
			b = &connBuf{responses: make(map[int]model.Event)}
			bufs[id] = b
		}

		switch ev.Type {
		case model.EventRequest:
			b.requests = append(b.requests, ev)
		case model.EventResponse:
			if b.responses == nil {
				b.responses = make(map[int]model.Event)
			}
			b.responses[ev.Sequence] = ev
		case model.EventConnectionOpen:
			b.hasOpen = true
		case model.EventConnectionClose:
			b.hasClose = true
			if len(b.requests) == 0 {
				delete(bufs, id)
				continue
			}
			if e.cfg.Replay.Lifecycle.RequireOpen && !b.hasOpen {
				parseErr = fmt.Errorf("connection %q missing connection_open", id)
				break
			}
			if !connectionBelongsToShard(id, e.cfg.Replay.Sharding.ShardIndex, e.cfg.Replay.Sharding.ShardCount) {
				delete(bufs, id)
				continue
			}
			sort.Slice(b.requests, func(i, j int) bool { return b.requests[i].Sequence < b.requests[j].Sequence })
			select {
			case <-ctx.Done():
				return ctx.Err()
			case jobs <- replayJob{requests: b.requests, responsesBySequence: b.responses}:
			}
			delete(bufs, id)
		}
		if parseErr != nil {
			break
		}
	}

	if parseErr != nil {
		return parseErr
	}

	for id, b := range bufs {
		if len(b.requests) == 0 {
			continue
		}
		if !connectionBelongsToShard(id, e.cfg.Replay.Sharding.ShardIndex, e.cfg.Replay.Sharding.ShardCount) {
			continue
		}
		sort.Slice(b.requests, func(i, j int) bool { return b.requests[i].Sequence < b.requests[j].Sequence })
		select {
		case <-ctx.Done():
			return ctx.Err()
		case jobs <- replayJob{requests: b.requests, responsesBySequence: b.responses}:
		}
	}

	return nil
}

func (e *Engine) aggregateResults(results <-chan Summary) Summary {
	final := Summary{Outcome: RunSuccess}
	var allRequestResults []RequestResult
	for s := range results {
		final.RequestsSent += s.RequestsSent
		final.ResponsesReceived += s.ResponsesReceived
		final.SendErrors += s.SendErrors
		final.ValidationFailed += s.ValidationFailed
		final.Skipped += s.Skipped
		final.ConnectionsDone += s.ConnectionsDone
		final.ConnectionsAborted += s.ConnectionsAborted
		if len(s.RequestResults) > 0 {
			allRequestResults = append(allRequestResults, s.RequestResults...)
		}
		if len(s.ConnectionResults) > 0 {
			final.ConnectionResults = append(final.ConnectionResults, s.ConnectionResults...)
		}
	}
	final.RequestResults = allRequestResults
	if final.ConnectionsAborted > 0 {
		final.Outcome = RunPartialSuccess
	}
	if final.ValidationFailed > 0 && final.Outcome == RunSuccess {
		final.Outcome = RunPartialSuccess
	}
	if final.RequestsSent == 0 && final.Skipped == 0 {
		final.Outcome = RunFailed
	}
	return final
}

type replayJob struct {
	requests            []model.Event
	responsesBySequence map[int]model.Event
}

func (e *Engine) replayConnectionWithCheckpoint(ctx context.Context, requests []model.Event, responsesBySequence map[int]model.Event, checkpoints *checkpointStore) Summary {
	// Create a per-connection client/transport to ensure socket isolation per connection_id.
	client, transport := e.makePerConnectionClient()
	defer func() {
		if transport != nil {
			transport.CloseIdleConnections()
		}
	}()

	if e.shouldUseHTTP2MultiplexedMode(requests) {
		return e.replayConnectionHTTP2Multiplexed(ctx, client, requests, responsesBySequence, checkpoints)
	}
	return e.replayConnectionSerialized(ctx, client, requests, responsesBySequence, checkpoints)
}

func (e *Engine) makePerConnectionClient() (*http.Client, *http.Transport) {
	dialer := &net.Dialer{Timeout: e.cfg.Replay.Timeout.Connect, KeepAlive: e.cfg.Replay.Timeout.IdleConnection}
	tr := &http.Transport{
		DialContext:         dialer.DialContext,
		TLSClientConfig:     &tls.Config{MinVersion: tls.VersionTLS12},
		IdleConnTimeout:     e.cfg.Replay.Timeout.IdleConnection,
		MaxIdleConns:        2,
		MaxIdleConnsPerHost: 1,
	}
	client := &http.Client{
		Timeout:   e.cfg.Replay.Timeout.Request,
		Transport: tr,
	}
	return client, tr
}

func (e *Engine) replayConnectionSerialized(ctx context.Context, client *http.Client, requests []model.Event, responsesBySequence map[int]model.Event, checkpoints *checkpointStore) Summary {
	result := Summary{}
	if len(requests) == 0 {
		return result
	}

	connID := requests[0].ConnectionID
	connResult := ConnectionResult{ConnectionID: connID, Outcome: ConnectionCompleted, Requests: []RequestResult{}}

	var previousTimestamp time.Time
	previousTimestampSet := false

	for _, requestEvent := range requests {
		select {
		case <-ctx.Done():
			result.ConnectionsAborted++
			result.Outcome = RunFailed
			return result
		default:
		}

		if sleepErr := e.sleepForPacing(ctx, previousTimestamp, previousTimestampSet, requestEvent.Timestamp); sleepErr != nil {
			result.ConnectionsAborted++
			result.Outcome = RunFailed
			return result
		}
		if parsedTimestamp, ok := model.ParseTimestamp(requestEvent.Timestamp); ok {
			previousTimestamp = parsedTimestamp
			previousTimestampSet = true
		}

		reqRes := RequestResult{ConnectionID: requestEvent.ConnectionID, Sequence: requestEvent.Sequence}

		if checkpoints.alreadyProcessed(requestEvent.ConnectionID, requestEvent.Sequence) {
			result.Skipped++
			reqRes.Outcome = RequestSkipped
			reqRes.Skipped = true
			connResult.Requests = append(connResult.Requests, reqRes)
			result.RequestResults = append(result.RequestResults, reqRes)
			continue
		}

		if e.shouldSkipByIdempotencyPolicy(requestEvent) {
			result.Skipped++
			reqRes.Outcome = RequestSkipped
			reqRes.Skipped = true
			if err := checkpoints.markProcessed(requestEvent.ConnectionID, requestEvent.Sequence); err != nil {
				result.ConnectionsAborted++
				result.Outcome = RunFailed
				reqRes.Error = err.Error()
				connResult.Requests = append(connResult.Requests, reqRes)
				result.RequestResults = append(result.RequestResults, reqRes)
				return result
			}
			connResult.Requests = append(connResult.Requests, reqRes)
			result.RequestResults = append(result.RequestResults, reqRes)
			continue
		}

		// Dry-run: do not send network requests; count as skipped (do not persist checkpoint)
		if e.cfg.Replay.DryRun {
			result.Skipped++
			reqRes.Outcome = RequestSkipped
			reqRes.Skipped = true
			connResult.Requests = append(connResult.Requests, reqRes)
			result.RequestResults = append(result.RequestResults, reqRes)
			continue
		}

		exec, err := e.sendRequest(ctx, client, requestEvent)
		label := requestEvent.HTTP.Path
		if label == "" {
			label = "unknown"
		}

		if err != nil {
			result.SendErrors++
			result.ConnectionsAborted++
			if result.RequestsSent > 0 || result.Skipped > 0 {
				result.Outcome = RunPartialSuccess
			} else {
				result.Outcome = RunFailed
			}
			reqRes.Outcome = RequestSendError
			reqRes.Error = err.Error()
			connResult.Requests = append(connResult.Requests, reqRes)
			result.RequestResults = append(result.RequestResults, reqRes)
			connResult.Outcome = ConnectionAborted
			result.ConnectionResults = append(result.ConnectionResults, connResult)
			return result
		}

		result.RequestsSent++
		result.ResponsesReceived++
		reqRes.Outcome = RequestResponseReceived
		reqRes.StatusCode = exec.statusCode
		reqRes.LatencyMS = exec.latencyMS

		if err := checkpoints.markProcessed(requestEvent.ConnectionID, requestEvent.Sequence); err != nil {
			result.ConnectionsAborted++
			result.Outcome = RunFailed
			reqRes.Error = err.Error()
			connResult.Requests = append(connResult.Requests, reqRes)
			result.RequestResults = append(result.RequestResults, reqRes)
			return result
		}
		if expected, ok := responsesBySequence[requestEvent.Sequence]; ok {
			if e.responseValidationFailed(expected, exec) {
				result.ValidationFailed++
				reqRes.Outcome = RequestValidationFailed
				reqRes.ValidationFailed = true
				if result.Outcome == "" {
					result.Outcome = RunPartialSuccess
				}
			}
		}
		connResult.Requests = append(connResult.Requests, reqRes)
		result.RequestResults = append(result.RequestResults, reqRes)
		e.metrics.LabelLatencyHistogram.WithLabelValues(
			e.cfg.Labels.CollectionID,
			label,
			e.cfg.Labels.RunID,
			e.cfg.Labels.EngineNo,
			e.cfg.Labels.PlanID,
			e.cfg.Labels.Zone,
		).Observe(exec.latencyMS)
		e.metrics.StatusCounter.WithLabelValues(
			e.cfg.Labels.CollectionID,
			e.cfg.Labels.PlanID,
			e.cfg.Labels.RunID,
			e.cfg.Labels.EngineNo,
			label,
			e.cfg.Labels.Zone,
			fmt.Sprintf("%d", exec.statusCode),
		).Inc()
		e.metrics.EgressCounter.WithLabelValues(
			e.cfg.Labels.CollectionID,
			e.cfg.Labels.PlanID,
			e.cfg.Labels.RunID,
			e.cfg.Labels.EngineNo,
			label,
			e.cfg.Labels.Zone,
		).Add(float64(exec.egressBytes))
	}

	if result.ConnectionsAborted == 0 {
		result.ConnectionsDone = 1
		if result.ValidationFailed > 0 {
			result.Outcome = RunPartialSuccess
		} else if result.RequestsSent == 0 && result.Skipped > 0 {
			result.Outcome = RunSuccess
		} else if result.Outcome == "" {
			result.Outcome = RunSuccess
		}
	}
	// finalize connection result outcome
	if result.ConnectionsAborted > 0 {
		connResult.Outcome = ConnectionAborted
	} else {
		connResult.Outcome = ConnectionCompleted
	}
	result.ConnectionResults = append(result.ConnectionResults, connResult)
	return result
}

func (e *Engine) replayConnectionHTTP2Multiplexed(ctx context.Context, client *http.Client, requests []model.Event, responsesBySequence map[int]model.Event, checkpoints *checkpointStore) Summary {
	streams := groupRequestsByStream(requests)
	if len(streams) == 0 {
		return Summary{ConnectionsDone: 1, Outcome: RunSuccess}
	}

	streamSem := make(chan struct{}, e.cfg.Replay.HTTP2.MaxConcurrentStreams)
	results := make(chan Summary, len(streams))
	var wg sync.WaitGroup

	for _, streamRequests := range streams {
		streamRequests := streamRequests
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case <-ctx.Done():
				results <- Summary{ConnectionsAborted: 1, Outcome: RunFailed}
				return
			case streamSem <- struct{}{}:
			}
			defer func() { <-streamSem }()
			results <- e.replayConnectionSerialized(ctx, client, streamRequests, responsesBySequence, checkpoints)
		}()
	}

	wg.Wait()
	close(results)

	aggregated := Summary{Outcome: RunSuccess}
	var combinedRequests []RequestResult
	for streamResult := range results {
		aggregated.RequestsSent += streamResult.RequestsSent
		aggregated.ResponsesReceived += streamResult.ResponsesReceived
		aggregated.SendErrors += streamResult.SendErrors
		aggregated.ValidationFailed += streamResult.ValidationFailed
		aggregated.Skipped += streamResult.Skipped
		aggregated.ConnectionsAborted += streamResult.ConnectionsAborted
		if len(streamResult.RequestResults) > 0 {
			combinedRequests = append(combinedRequests, streamResult.RequestResults...)
		}
	}

	if aggregated.ConnectionsAborted > 0 {
		aggregated.Outcome = RunPartialSuccess
	}
	if aggregated.ValidationFailed > 0 && aggregated.Outcome == RunSuccess {
		aggregated.Outcome = RunPartialSuccess
	}
	if aggregated.RequestsSent == 0 && aggregated.Skipped == 0 {
		aggregated.Outcome = RunFailed
	}
	if aggregated.ConnectionsAborted == 0 {
		aggregated.ConnectionsDone = 1
	}
	// Single connection, collect connection-level result
	connID := 0
	if len(requests) > 0 {
		connID = requests[0].ConnectionID
	}
	connResult := ConnectionResult{ConnectionID: connID, Outcome: ConnectionCompleted, Requests: combinedRequests}
	if aggregated.ConnectionsAborted > 0 {
		connResult.Outcome = ConnectionAborted
	}
	aggregated.ConnectionResults = append(aggregated.ConnectionResults, connResult)
	aggregated.RequestResults = combinedRequests
	return aggregated
}

func groupRequestsByStream(requests []model.Event) map[int][]model.Event {
	grouped := make(map[int][]model.Event)
	for _, req := range requests {
		streamID := req.StreamID
		if streamID == 0 {
			streamID = 1
		}
		grouped[streamID] = append(grouped[streamID], req)
	}
	for streamID := range grouped {
		sort.Slice(grouped[streamID], func(i, j int) bool {
			return grouped[streamID][i].Sequence < grouped[streamID][j].Sequence
		})
	}
	return grouped
}

func (e *Engine) shouldUseHTTP2MultiplexedMode(requests []model.Event) bool {
	if !strings.EqualFold(e.cfg.Replay.HTTP2.Mode, "multiplexed") {
		return false
	}
	for _, req := range requests {
		if strings.Contains(strings.ToUpper(req.HTTP.Version), "HTTP/2") {
			return true
		}
	}
	return false
}

func (e *Engine) filterGroupsByShard(groups map[int][]model.Event) map[int][]model.Event {
	sharding := e.cfg.Replay.Sharding
	if sharding.ShardCount <= 1 {
		return groups
	}
	filtered := make(map[int][]model.Event)
	for connectionID, requests := range groups {
		if !connectionBelongsToShard(connectionID, sharding.ShardIndex, sharding.ShardCount) {
			continue
		}
		filtered[connectionID] = requests
	}
	return filtered
}

func connectionBelongsToShard(connectionID int, shardIndex, shardCount int) bool {
	if shardCount <= 1 {
		return true
	}
	hasher := fnv.New32a()
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(connectionID))
	_, _ = hasher.Write(buf[:])
	return int(hasher.Sum32()%uint32(shardCount)) == shardIndex
}

func (e *Engine) sleepForPacing(ctx context.Context, previous time.Time, previousSet bool, currentRaw string) error {
	if !e.cfg.Replay.Pacing.Enabled || !previousSet {
		return nil
	}
	current, ok := model.ParseTimestamp(currentRaw)
	if !ok || !current.After(previous) {
		return nil
	}

	delta := current.Sub(previous)
	if max := e.cfg.Replay.Pacing.MaxSleepDelta; max > 0 && delta > max {
		delta = max
	}

	timer := time.NewTimer(delta)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// timestamp parsing moved to internal/model/ for reuse across packages

func (e *Engine) shouldSkipByIdempotencyPolicy(request model.Event) bool {
	policy := e.cfg.Replay.Idempotency
	if !policy.Enabled {
		return false
	}

	method := resolvedRequestMethod(request)

	blockedMethods := make(map[string]struct{}, len(policy.BlockMethods))
	for _, m := range policy.BlockMethods {
		blockedMethods[strings.ToUpper(strings.TrimSpace(m))] = struct{}{}
	}
	if _, blocked := blockedMethods[method]; !blocked {
		return false
	}

	if len(policy.RequireHeaderForAllow) == 0 {
		return true
	}
	for _, headerName := range policy.RequireHeaderForAllow {
		if hasHeaderValue(request.Headers, headerName) {
			return false
		}
	}
	return true
}

func hasHeaderValue(headers map[string][]string, name string) bool {
	if len(headers) == 0 || name == "" {
		return false
	}
	target := strings.ToLower(name)
	for header, values := range headers {
		if strings.ToLower(header) != target {
			continue
		}
		for _, value := range values {
			if strings.TrimSpace(value) != "" {
				return true
			}
		}
	}
	return false
}

func (e *Engine) sendRequest(ctx context.Context, client *http.Client, requestEvent model.Event) (requestExecution, error) {
	maxAttempts := e.cfg.Replay.Retry.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 1
	}

	var lastExec requestExecution
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		exec, err := e.executeRequest(ctx, client, requestEvent)
		if err != nil {
			lastErr = err
			if attempt == maxAttempts || !e.shouldRetryError(err) {
				return requestExecution{}, err
			}
			if sleepErr := e.sleepBackoff(ctx, attempt); sleepErr != nil {
				return requestExecution{}, sleepErr
			}
			continue
		}

		lastExec = exec
		if attempt < maxAttempts && e.shouldRetryStatus(exec.statusCode) {
			if sleepErr := e.sleepBackoff(ctx, attempt); sleepErr != nil {
				return requestExecution{}, sleepErr
			}
			continue
		}
		return exec, nil
	}

	if lastErr != nil {
		return requestExecution{}, lastErr
	}
	return lastExec, nil
}

func (e *Engine) executeRequest(ctx context.Context, client *http.Client, requestEvent model.Event) (requestExecution, error) {
	requestURL, err := e.buildRequestURL(requestEvent)
	if err != nil {
		return requestExecution{}, err
	}

	var bodyReader io.Reader
	if requestEvent.Body.Content != "" {
		decoded, decodeErr := base64.StdEncoding.DecodeString(requestEvent.Body.Content)
		if decodeErr != nil {
			return requestExecution{}, fmt.Errorf("decode body: %w", decodeErr)
		}
		bodyReader = bytes.NewReader(decoded)
	}

	method := resolvedRequestMethod(requestEvent)
	req, err := http.NewRequestWithContext(ctx, method, requestURL, bodyReader)
	if err != nil {
		return requestExecution{}, err
	}

	for key, values := range requestEvent.Headers {
		for _, value := range values {
			req.Header.Add(key, value)
		}
	}
	for _, headerName := range e.cfg.Header.Drop {
		req.Header.Del(headerName)
	}

	// Automatic Host/:authority rewrite when override_url is set.
	if e.cfg.Target.OverrideURL != "" {
		if override, err := url.Parse(e.cfg.Target.OverrideURL); err == nil {
			// set request Host to override host (preserves path/query in URL)
			req.Host = override.Host
			// also set Host header explicitly for clarity
			req.Header.Set("Host", override.Host)
		}
	}

	for key, value := range e.cfg.Header.Set {
		req.Header.Set(key, value)
	}

	start := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		return requestExecution{}, err
	}
	defer resp.Body.Close()
	// Limit the amount of response body read to avoid unbounded memory growth.
	// This prevents OOMs when replaying unexpectedly large responses.
	lr := io.LimitReader(resp.Body, maxBodyRead)
	body, err := io.ReadAll(lr)
	if err != nil {
		return requestExecution{}, err
	}

	headers := make(map[string][]string, len(resp.Header))
	for key, values := range resp.Header {
		copied := make([]string, len(values))
		copy(copied, values)
		headers[strings.ToLower(key)] = copied
	}

	return requestExecution{
		latencyMS:   time.Since(start).Seconds() * 1000,
		statusCode:  resp.StatusCode,
		egressBytes: int64(len(body)),
		headers:     headers,
		body:        body,
	}, nil
}

func (e *Engine) shouldRetryStatus(statusCode int) bool {
	for _, retryStatus := range e.cfg.Replay.Retry.RetryOnStatuses {
		if retryStatus == statusCode {
			return true
		}
	}
	return false
}

func (e *Engine) shouldRetryError(err error) bool {
	if len(e.cfg.Replay.Retry.RetryOnErrors) == 0 {
		return false
	}
	category := retryErrorCategory(err)
	if category == "" {
		return false
	}
	for _, configured := range e.cfg.Replay.Retry.RetryOnErrors {
		if strings.EqualFold(configured, category) {
			return true
		}
	}
	return false
}

func retryErrorCategory(err error) string {
	if err == nil {
		return ""
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return "timeout"
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return "timeout"
	}
	lower := strings.ToLower(err.Error())
	switch {
	case strings.Contains(lower, "connection reset"):
		return "connection_reset"
	case strings.Contains(lower, "tls"):
		return "tls"
	case strings.Contains(lower, "dial tcp"), strings.Contains(lower, "no such host"), strings.Contains(lower, "connection refused"):
		return "network"
	default:
		return ""
	}
}

func (e *Engine) sleepBackoff(ctx context.Context, attempt int) error {
	d := backoffDuration(e.cfg.Replay.Retry.Backoff, attempt)
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func backoffDuration(strategy string, attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	const base = 100 * time.Millisecond
	switch strings.ToLower(strategy) {
	case "fixed":
		return base
	case "exponential":
		// Use a capped exponential backoff computed with floats to avoid
		// undefined behavior or overflow when shifting time.Duration.
		exp := attempt - 1
		if exp < 0 {
			exp = 0
		}
		if exp > 30 { // cap exponent to avoid absurd durations
			exp = 30
		}
		backoffNano := float64(base) * math.Pow(2, float64(exp))
		d := time.Duration(backoffNano)
		if d > 5*time.Second {
			return 5 * time.Second
		}
		return d
	default:
		return 0
	}
}

func (e *Engine) responseValidationFailed(expected model.Event, actual requestExecution) bool {
	validation := e.cfg.Replay.Validation
	if !validation.Enabled {
		return false
	}
	if validation.Status && expected.Status > 0 && expected.Status != actual.statusCode {
		return true
	}
	if validation.Headers && headersMismatch(expected.Headers, actual.headers, validation.IgnoreHeaders) {
		return true
	}
	if validation.Body && expected.Body.Content != "" {
		expectedBody, err := base64.StdEncoding.DecodeString(expected.Body.Content)
		if err != nil {
			return true
		}
		if !slices.Equal(expectedBody, actual.body) {
			return true
		}
	}
	return false
}

func headersMismatch(expected map[string][]string, actual map[string][]string, ignore []string) bool {
	if len(expected) == 0 {
		return false
	}
	ignoreSet := make(map[string]struct{}, len(ignore))
	for _, key := range ignore {
		ignoreSet[strings.ToLower(key)] = struct{}{}
	}

	for key, expectedValues := range expected {
		normalizedKey := strings.ToLower(key)
		if _, ignored := ignoreSet[normalizedKey]; ignored {
			continue
		}
		actualValues, ok := actual[normalizedKey]
		if !ok {
			return true
		}
		expectedCopy := append([]string(nil), expectedValues...)
		actualCopy := append([]string(nil), actualValues...)
		sort.Strings(expectedCopy)
		sort.Strings(actualCopy)
		if !slices.Equal(expectedCopy, actualCopy) {
			return true
		}
	}
	return false
}

func (e *Engine) buildRequestURL(event model.Event) (string, error) {
	if e.cfg.Target.OverrideURL != "" {
		override, err := url.Parse(e.cfg.Target.OverrideURL)
		if err != nil {
			return "", fmt.Errorf("parse override_url: %w", err)
		}
		requestURI := event.HTTP.Path
		parsedPath, err := url.ParseRequestURI(requestURI)
		if err != nil {
			return "", fmt.Errorf("parse request path: %w", err)
		}
		override = override.JoinPath(parsedPath.Path)
		override.RawQuery = parsedPath.RawQuery
		return override.String(), nil
	}

	scheme := event.HTTP.Scheme
	if scheme == "" {
		scheme = "http"
	}
	if event.HTTP.Authority == "" {
		return "", fmt.Errorf("request authority is required")
	}
	if event.HTTP.Path == "" {
		return "", fmt.Errorf("request path is required")
	}
	return fmt.Sprintf("%s://%s%s", scheme, event.HTTP.Authority, event.HTTP.Path), nil
}

func resolvedRequestMethod(request model.Event) string {
	method := strings.ToUpper(strings.TrimSpace(request.HTTP.Method))
	if method != "" {
		return method
	}

	// If the method is not recorded, infer the most likely method once and
	// reuse the same resolution for both policy checks and request execution.
	if request.Body.Content != "" || request.Body.SizeBytes > 0 || hasHeaderValue(request.Headers, "content-type") {
		return http.MethodPost
	}
	return http.MethodGet
}
