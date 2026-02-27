package engine

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
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

type Summary struct {
	RequestsSent       int64
	ResponsesReceived  int64
	SendErrors         int64
	ValidationFailed   int64
	Skipped            int64
	ConnectionsDone    int64
	ConnectionsAborted int64
	Outcome            RunOutcome
}

type Engine struct {
	cfg     config.Config
	metrics *metrics.Registry
	client  *http.Client
}

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

func (e *Engine) Replay(ctx context.Context, events []model.Event) (Summary, error) {
	groups := groupRequestEventsByConnection(events)
	if len(groups) == 0 {
		return Summary{Outcome: RunFailed}, fmt.Errorf("no request events found")
	}
	groups = e.filterGroupsByShard(groups)
	if len(groups) == 0 {
		return Summary{Outcome: RunSuccess}, nil
	}
	if err := e.validateLifecycleRequirements(groups, events); err != nil {
		return Summary{Outcome: RunFailed}, err
	}
	responseExpectations := groupResponseEventsByConnectionSequence(events)
	checkpoints, err := newCheckpointStore(e.cfg.Replay.Checkpoint.File)
	if err != nil {
		return Summary{Outcome: RunFailed}, err
	}

	e.metrics.SeedEngineLabels(e.cfg.Labels)
	vus := e.cfg.Replay.MaxVirtualUsersPerEngine
	connSem := make(chan struct{}, e.cfg.Replay.MaxActiveConnectionsPerEngine)
	jobs := make(chan replayJob)
	results := make(chan Summary, len(groups))

	var wg sync.WaitGroup
	for i := 0; i < vus; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				select {
				case <-ctx.Done():
					results <- Summary{Outcome: RunFailed}
					continue
				case connSem <- struct{}{}:
				}
				s := e.replayConnectionWithCheckpoint(ctx, job.requests, job.responsesBySequence, checkpoints)
				<-connSem
				results <- s
			}
		}()
	}

	for connID, reqs := range groups {
		jobs <- replayJob{requests: reqs, responsesBySequence: responseExpectations[connID]}
	}
	close(jobs)

	wg.Wait()
	close(results)

	final := Summary{Outcome: RunSuccess}
	for s := range results {
		final.RequestsSent += s.RequestsSent
		final.ResponsesReceived += s.ResponsesReceived
		final.SendErrors += s.SendErrors
		final.ValidationFailed += s.ValidationFailed
		final.Skipped += s.Skipped
		final.ConnectionsDone += s.ConnectionsDone
		final.ConnectionsAborted += s.ConnectionsAborted
	}
	if final.ConnectionsAborted > 0 {
		final.Outcome = RunPartialSuccess
	}
	if final.ValidationFailed > 0 && final.Outcome == RunSuccess {
		final.Outcome = RunPartialSuccess
	}
	if final.RequestsSent == 0 && final.Skipped == 0 {
		final.Outcome = RunFailed
	}
	return final, nil
}

type replayJob struct {
	requests            []model.Event
	responsesBySequence map[int]model.Event
}

func groupRequestEventsByConnection(events []model.Event) map[string][]model.Event {
	groups := make(map[string][]model.Event)
	for _, event := range events {
		if event.Type != model.EventRequest {
			continue
		}
		groups[event.ConnectionID] = append(groups[event.ConnectionID], event)
	}
	for id := range groups {
		sort.Slice(groups[id], func(i, j int) bool {
			return groups[id][i].Sequence < groups[id][j].Sequence
		})
	}
	return groups
}

func groupResponseEventsByConnectionSequence(events []model.Event) map[string]map[int]model.Event {
	grouped := make(map[string]map[int]model.Event)
	for _, event := range events {
		if event.Type != model.EventResponse {
			continue
		}
		if _, ok := grouped[event.ConnectionID]; !ok {
			grouped[event.ConnectionID] = make(map[int]model.Event)
		}
		grouped[event.ConnectionID][event.Sequence] = event
	}
	return grouped
}

func (e *Engine) replayConnection(ctx context.Context, requests []model.Event, responsesBySequence map[int]model.Event) Summary {
	return e.replayConnectionWithCheckpoint(ctx, requests, responsesBySequence, nil)
}

func (e *Engine) replayConnectionWithCheckpoint(ctx context.Context, requests []model.Event, responsesBySequence map[int]model.Event, checkpoints *checkpointStore) Summary {
	result := Summary{}
	if len(requests) == 0 {
		return result
	}

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
		if parsedTimestamp, ok := parseTimestamp(requestEvent.Timestamp); ok {
			previousTimestamp = parsedTimestamp
			previousTimestampSet = true
		}

		if checkpoints.alreadyProcessed(requestEvent.ConnectionID, requestEvent.Sequence) {
			result.Skipped++
			continue
		}

		if e.shouldSkipByIdempotencyPolicy(requestEvent) {
			result.Skipped++
			if err := checkpoints.markProcessed(requestEvent.ConnectionID, requestEvent.Sequence); err != nil {
				result.ConnectionsAborted++
				result.Outcome = RunFailed
				return result
			}
			continue
		}

		exec, err := e.sendRequest(ctx, requestEvent)
		label := requestEvent.HTTP.Path
		if label == "" {
			label = "unknown"
		}

		if err != nil {
			result.SendErrors++
			result.ConnectionsAborted++
			result.Outcome = RunPartialSuccess
			continue
		}

		result.RequestsSent++
		result.ResponsesReceived++
		if err := checkpoints.markProcessed(requestEvent.ConnectionID, requestEvent.Sequence); err != nil {
			result.ConnectionsAborted++
			result.Outcome = RunFailed
			return result
		}
		if expected, ok := responsesBySequence[requestEvent.Sequence]; ok {
			if e.responseValidationFailed(expected, exec) {
				result.ValidationFailed++
				if result.Outcome == "" {
					result.Outcome = RunPartialSuccess
				}
			}
		}
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
	return result
}

func (e *Engine) filterGroupsByShard(groups map[string][]model.Event) map[string][]model.Event {
	sharding := e.cfg.Replay.Sharding
	if sharding.ShardCount <= 1 {
		return groups
	}
	filtered := make(map[string][]model.Event)
	for connectionID, requests := range groups {
		if !connectionBelongsToShard(connectionID, sharding.ShardIndex, sharding.ShardCount) {
			continue
		}
		filtered[connectionID] = requests
	}
	return filtered
}

func connectionBelongsToShard(connectionID string, shardIndex, shardCount int) bool {
	if shardCount <= 1 {
		return true
	}
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(connectionID))
	return int(hasher.Sum32()%uint32(shardCount)) == shardIndex
}

func (e *Engine) validateLifecycleRequirements(requestGroups map[string][]model.Event, events []model.Event) error {
	lifecycles := collectLifecycleByConnection(events)
	for connectionID := range requestGroups {
		lifecycle := lifecycles[connectionID]
		if e.cfg.Replay.Lifecycle.RequireOpen && !lifecycle.hasOpen {
			return fmt.Errorf("connection %q missing connection_open event", connectionID)
		}
		if e.cfg.Replay.Lifecycle.RequireClose && !lifecycle.hasClose {
			return fmt.Errorf("connection %q missing connection_close event", connectionID)
		}
	}
	return nil
}

type connectionLifecycle struct {
	hasOpen  bool
	hasClose bool
}

func collectLifecycleByConnection(events []model.Event) map[string]connectionLifecycle {
	byConnection := make(map[string]connectionLifecycle)
	for _, event := range events {
		if event.ConnectionID == "" {
			continue
		}
		lifecycle := byConnection[event.ConnectionID]
		switch event.Type {
		case model.EventConnectionOpen:
			lifecycle.hasOpen = true
		case model.EventConnectionClose:
			lifecycle.hasClose = true
		default:
			continue
		}
		byConnection[event.ConnectionID] = lifecycle
	}
	return byConnection
}

func (e *Engine) sleepForPacing(ctx context.Context, previous time.Time, previousSet bool, currentRaw string) error {
	if !e.cfg.Replay.Pacing.Enabled || !previousSet {
		return nil
	}
	current, ok := parseTimestamp(currentRaw)
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

func parseTimestamp(raw string) (time.Time, bool) {
	if raw == "" {
		return time.Time{}, false
	}
	if parsed, err := time.Parse(time.RFC3339Nano, raw); err == nil {
		return parsed, true
	}
	parsed, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}, false
	}
	return parsed, true
}

func (e *Engine) shouldSkipByIdempotencyPolicy(request model.Event) bool {
	policy := e.cfg.Replay.Idempotency
	if !policy.Enabled {
		return false
	}

	method := strings.ToUpper(strings.TrimSpace(request.HTTP.Method))
	if method == "" {
		method = http.MethodGet
	}

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

func (e *Engine) sendRequest(ctx context.Context, requestEvent model.Event) (requestExecution, error) {
	maxAttempts := e.cfg.Replay.Retry.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 1
	}

	var lastExec requestExecution
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		exec, err := e.executeRequest(ctx, requestEvent)
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

func (e *Engine) executeRequest(ctx context.Context, requestEvent model.Event) (requestExecution, error) {
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
		bodyReader = strings.NewReader(string(decoded))
	}

	method := requestEvent.HTTP.Method
	if method == "" {
		method = http.MethodGet
	}
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
	for key, value := range e.cfg.Header.Set {
		req.Header.Set(key, value)
	}

	start := time.Now()
	resp, err := e.client.Do(req)
	if err != nil {
		return requestExecution{}, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
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
		latencyMS:   float64(time.Since(start).Milliseconds()),
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
		d := base << (attempt - 1)
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
		override.Path = event.HTTP.Path
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
