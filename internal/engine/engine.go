package engine

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
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
	responseExpectations := groupResponseEventsByConnectionSequence(events)

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
				s := e.replayConnection(ctx, job.requests, job.responsesBySequence)
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
	if final.RequestsSent == 0 {
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
	result := Summary{}
	if len(requests) == 0 {
		return result
	}

	for _, requestEvent := range requests {
		select {
		case <-ctx.Done():
			result.ConnectionsAborted++
			result.Outcome = RunFailed
			return result
		default:
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
		} else if result.Outcome == "" {
			result.Outcome = RunSuccess
		}
	}
	return result
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
