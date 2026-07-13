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
	"log/slog"
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
	Node             string         `json:"node,omitempty"`
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
	Node              string            `json:"node,omitempty"`
	ConnectionID      int               `json:"connection_id"`
	Outcome           ConnectionOutcome `json:"outcome"`
	RequestsSent      int64             `json:"requests_sent,omitempty"`
	ResponsesReceived int64             `json:"responses_received,omitempty"`
	SendErrors        int64             `json:"send_errors,omitempty"`
	ValidationFailed  int64             `json:"validation_failed,omitempty"`
	Skipped           int64             `json:"skipped,omitempty"`
	Requests          []RequestResult   `json:"requests,omitempty"`
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
	cfg                 config.Config
	metrics             *metrics.Registry
	metricLabelValues   []string
	parsedPathTemplates map[int][]PathTemplate
	parsedOverrideURL   *url.URL
	targetOverrideErr   error
}

const maxBodyRead = 10 * 1024 * 1024 // 10 MiB

type requestExecution struct {
	latencyMS   float64
	statusCode  int
	egressBytes int64
	attempted   bool
	headers     map[string][]string
	body        []byte
}

func responseHeaderBytes(headers http.Header) int64 {
	var n int64 = 2 // final CRLF terminating the header block
	for key, values := range headers {
		for _, value := range values {
			n += int64(len(key) + len(": ") + len(value) + len("\r\n"))
		}
	}
	return n
}

func New(cfg config.Config, registry *metrics.Registry) *Engine {
	parsedOverride, overrideErr := cfg.Target.ParseURL()
	if overrideErr != nil {
		slog.Error("failed to parse config target.override_url", "url", cfg.Target.OverrideURL, "error", overrideErr)
	}
	return &Engine{
		cfg:                 cfg,
		metrics:             registry,
		metricLabelValues:   cfg.Metrics.CommonLabelValues(),
		parsedPathTemplates: ParsePathTemplates(cfg.Metrics.PathTemplates),
		parsedOverrideURL:   parsedOverride,
		targetOverrideErr:   overrideErr,
	}
}

func drainEvents(events <-chan model.Event) {
	go func() {
		for range events {
		}
	}()
}

// ReplayStream processes events from the provided channel as they arrive.
// Events are routed to per-worker channels by connection assignment, providing
// bounded backpressure without buffering entire connections in memory.
// Each worker maintains per-connection state and replays HTTP/1.1 requests
// synchronously as they arrive. HTTP/2 multiplexed requests are dispatched
// concurrently on the shared per-connection client and joined at close/EOF.
func (e *Engine) ReplayStream(ctx context.Context, events <-chan model.Event) (Summary, error) {
	if e.targetOverrideErr != nil {
		drainEvents(events)
		return Summary{Outcome: RunFailed}, fmt.Errorf("validate target override: %w", e.targetOverrideErr)
	}
	checkpoints, err := newCheckpointStore(e.cfg.Replay.Checkpoint.File)
	if err != nil {
		drainEvents(events)
		return Summary{Outcome: RunFailed}, err
	}
	if checkpoints != nil {
		defer checkpoints.Close()
	}

	if e.metrics != nil {
		e.metrics.SeedEngineLabels(e.metricLabelValues)
	}

	replayCtx, cancelReplay := context.WithCancel(ctx)
	defer cancelReplay()

	vus := max(e.cfg.Replay.MaxVirtualUsersPerEngine, 1)

	workerChs := make([]chan model.Event, vus)
	for i := range workerChs {
		workerChs[i] = make(chan model.Event, eventChannelDepth)
	}

	results := make(chan Summary, vus)
	var wg sync.WaitGroup

	for i := range workerChs {
		activationDelay := workerActivationDelay(i, vus, e.cfg.Replay.RampupDuration)
		wg.Go(func() {
			results <- e.runEventWorker(replayCtx, workerChs[i], activationDelay, checkpoints)
		})
	}

	routeErr := e.routeEvents(replayCtx, events, workerChs)

	if routeErr != nil {
		cancelReplay()
		drainEvents(events)
	}

	for _, ch := range workerChs {
		close(ch)
	}

	wg.Wait()
	close(results)

	if routeErr != nil {
		return Summary{Outcome: RunFailed}, routeErr
	}

	return e.aggregateResults(results), nil
}

const eventChannelDepth = 256

// connState holds per-connection state within an event worker.
type connState struct {
	connKey     model.ConnectionKey
	client      *http.Client
	transport   *http.Transport
	http2       bool
	multiplexed bool
	detected    bool

	// Per-connection event processing state
	previousTimestamp     time.Time
	previousTimestampSet  bool
	pendingActual         map[int]requestExecution
	pendingExpected       map[int]model.Event
	pendingInlineExpected map[int]model.Event
	skippedValidation     map[int]struct{}
	sent                  int64
	responsesReceived     int64
	sendErrors            int64
	validationFailed      int64
	skipped               int64
	aborted               bool

	// Concurrent H/2 checkpointing advances the persisted watermark only after
	// every earlier observed request has reached a terminal checkpointable state.
	checkpointWatermark int
	checkpointOrder     []int
	checkpointCompleted map[int]struct{}
	// H/2 multiplexed requests run concurrently on the same http.Client.
	// h2Mu protects the shared result/rendezvous fields above while those
	// goroutines are in flight.
	h2Mu sync.Mutex
	h2WG sync.WaitGroup
}

func (e *Engine) newConnState(connKey model.ConnectionKey) *connState {
	return &connState{
		connKey:               connKey,
		pendingActual:         make(map[int]requestExecution),
		pendingExpected:       make(map[int]model.Event),
		pendingInlineExpected: make(map[int]model.Event),
	}
}

func (cs *connState) skipValidationForSequence(sequence int) {
	if sequence <= 0 {
		return
	}
	if cs.skippedValidation == nil {
		cs.skippedValidation = make(map[int]struct{})
	}
	cs.skippedValidation[sequence] = struct{}{}
	delete(cs.pendingExpected, sequence)
	delete(cs.pendingInlineExpected, sequence)
	delete(cs.pendingActual, sequence)
}

func (cs *connState) consumeSkippedValidation(sequence int) bool {
	if len(cs.skippedValidation) == 0 {
		return false
	}
	if _, ok := cs.skippedValidation[sequence]; !ok {
		return false
	}
	delete(cs.skippedValidation, sequence)
	return true
}

func (e *Engine) detectHTTP2ForConn(cs *connState, firstRequest model.Event) {
	cs.detected = true
	version := firstRequest.HTTP.Version
	if strings.Contains(version, "HTTP/2") || strings.Contains(version, "http/2") {
		cs.http2 = true
	}
	cs.multiplexed = strings.EqualFold(e.cfg.Replay.HTTP2.Mode, "multiplexed") && cs.http2
}

// routeEvents reads events and routes them to per-worker channels.
// Connections are assigned to workers round-robin on first appearance,
// ensuring even distribution. All events for a connection go to the same
// worker, preserving per-connection ordering.
func (e *Engine) routeEvents(ctx context.Context, events <-chan model.Event, workerChs []chan model.Event) error {
	opened := make(map[model.ConnectionKey]bool)
	connWorker := make(map[model.ConnectionKey]int)
	vus := len(workerChs)
	nextWorker := 0

	for ev := range events {
		if ev.Type == model.EventMeta {
			continue
		}

		connKey := model.ConnectionKey{Node: ev.Node, ConnectionID: ev.ConnectionID}
		if !connectionBelongsToShard(connKey, e.cfg.Replay.Sharding.ShardIndex, e.cfg.Replay.Sharding.ShardCount) {
			continue
		}

		switch ev.Type {
		case model.EventConnectionOpen:
			opened[connKey] = true
		case model.EventConnectionClose:
			// Cleanup happens after the close event is routed so it reaches the
			// worker that owns the connection state.
		case model.EventRequest:
			if e.cfg.Replay.Lifecycle.RequireOpen && !opened[connKey] {
				return fmt.Errorf("connection %v missing connection_open", connKey)
			}
		}

		workerIdx, ok := connWorker[connKey]
		if !ok {
			workerIdx = nextWorker % vus
			nextWorker++
			connWorker[connKey] = workerIdx
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case workerChs[workerIdx] <- ev:
		}

		if ev.Type == model.EventConnectionClose {
			delete(opened, connKey)
			delete(connWorker, connKey)
		}
	}

	return nil
}

// runEventWorker processes events from its channel using per-connection state.
// For HTTP/1.1, requests are processed synchronously as they arrive. For HTTP/2
// multiplexed mode, requests are sent concurrently on the same per-connection
// client and finalized when connection_close or EOF is observed.
func (e *Engine) runEventWorker(ctx context.Context, events <-chan model.Event, activationDelay time.Duration, checkpoints *checkpointStore) Summary {
	if err := waitForWorkerActivation(ctx, activationDelay); err != nil {
		return Summary{}
	}
	if e.metrics != nil {
		e.metrics.RecordActiveVirtualUserStarted(e.metricLabelValues)
		defer e.metrics.RecordActiveVirtualUserFinished(e.metricLabelValues)
	}

	conns := make(map[model.ConnectionKey]*connState)
	result := Summary{}

	for ev := range events {
		if ctx.Err() != nil {
			break
		}

		connKey := model.ConnectionKey{Node: ev.Node, ConnectionID: ev.ConnectionID}
		cs := conns[connKey]

		switch ev.Type {
		case model.EventConnectionOpen:
			if cs == nil {
				cs = e.newConnState(connKey)
				conns[connKey] = cs
			}

		case model.EventRequest:
			if cs == nil {
				cs = e.newConnState(connKey)
				conns[connKey] = cs
			}
			if !cs.detected {
				e.detectHTTP2ForConn(cs, ev)
				if cs.multiplexed {
					cs.checkpointWatermark = checkpoints.lastProcessed(connKey)
				}
				client, transport := e.makePerConnectionClient(cs.http2)
				cs.client = client
				cs.transport = transport
			}

			if cs.multiplexed {
				cs.h2Mu.Lock()
				aborted := cs.aborted
				cs.h2Mu.Unlock()
				if !aborted {
					e.processRequestConcurrent(ctx, cs, ev, checkpoints)
				}
			} else {
				if !cs.aborted {
					abort := e.processRequest(ctx, cs, ev, checkpoints)
					if abort {
						cs.aborted = true
						e.closeConnResources(cs)
					}
				}
			}

		case model.EventResponse:
			if cs == nil {
				continue
			}
			if cs.multiplexed {
				cs.h2Mu.Lock()
				e.handleResponseEvent(cs, ev)
				cs.h2Mu.Unlock()
			} else {
				e.handleResponseEvent(cs, ev)
			}

		case model.EventConnectionClose:
			if cs == nil {
				continue
			}
			e.finalizeConn(cs, &result)
			delete(conns, connKey)
		}
	}

	// EOF: finalize remaining connections
	for _, cs := range conns {
		e.finalizeConn(cs, &result)
	}

	return result
}

func (e *Engine) finalizeConn(cs *connState, result *Summary) {
	if !cs.detected {
		e.closeConnResources(cs)
		return
	}

	if cs.multiplexed {
		cs.h2WG.Wait()
	}
	e.validatePendingInlineResponses(cs)
	e.finalizePendingValidation(cs)
	e.collectConnResults(cs, result)
	e.closeConnResources(cs)
}

func (e *Engine) closeConnResources(cs *connState) {
	if cs.transport != nil {
		cs.transport.CloseIdleConnections()
		cs.transport = nil
	}
}

func (e *Engine) collectConnResults(cs *connState, result *Summary) {
	result.RequestsSent += cs.sent
	result.ResponsesReceived += cs.responsesReceived
	result.SendErrors += cs.sendErrors
	result.ValidationFailed += cs.validationFailed
	result.Skipped += cs.skipped
	if cs.aborted {
		result.ConnectionsAborted++
	} else {
		result.ConnectionsDone++
	}
	connResult := ConnectionResult{
		Node:              cs.connKey.Node,
		ConnectionID:      cs.connKey.ConnectionID,
		Outcome:           connOutcome(cs.aborted),
		RequestsSent:      cs.sent,
		ResponsesReceived: cs.responsesReceived,
		SendErrors:        cs.sendErrors,
		ValidationFailed:  cs.validationFailed,
		Skipped:           cs.skipped,
	}
	result.ConnectionResults = append(result.ConnectionResults, connResult)
}

func connectionResultFromSummary(connKey model.ConnectionKey, outcome ConnectionOutcome, result Summary) ConnectionResult {
	return ConnectionResult{
		Node:              connKey.Node,
		ConnectionID:      connKey.ConnectionID,
		Outcome:           outcome,
		RequestsSent:      result.RequestsSent,
		ResponsesReceived: result.ResponsesReceived,
		SendErrors:        result.SendErrors,
		ValidationFailed:  result.ValidationFailed,
		Skipped:           result.Skipped,
	}
}

func connOutcome(aborted bool) ConnectionOutcome {
	if aborted {
		return ConnectionAborted
	}
	return ConnectionCompleted
}

// processRequest handles a single HTTP/1.1 request event synchronously.
// Returns true if the connection should be aborted.
func (e *Engine) processRequest(ctx context.Context, cs *connState, requestEvent model.Event, checkpoints *checkpointStore) (abort bool) {
	if e.paceRequest(ctx, cs, requestEvent) != nil {
		cs.aborted = true
		return true
	}

	reqKey, handled, abort := e.prepareRequest(cs, requestEvent, checkpoints)
	if handled {
		return abort
	}

	exec, err := e.sendRequest(ctx, cs.client, requestEvent)
	if err != nil {
		e.finishRequestError(cs, requestEvent, err, exec)
		return true
	}

	abort = e.finishRequestSuccess(cs, requestEvent, exec, checkpoints.markProcessed(reqKey, requestEvent.Sequence))
	return abort
}

// processRequestConcurrent handles a single HTTP/2 multiplexed request by
// sending it on the shared per-connection client in its own goroutine. Go's
// http.Transport is safe for concurrent use and owns the actual H/2 stream
// multiplexing below this abstraction.
func (e *Engine) processRequestConcurrent(ctx context.Context, cs *connState, requestEvent model.Event, checkpoints *checkpointStore) {
	if e.paceRequest(ctx, cs, requestEvent) != nil {
		cs.h2Mu.Lock()
		cs.aborted = true
		cs.h2Mu.Unlock()
		return
	}

	cs.h2Mu.Lock()
	reqKey, handled, commitSequence := e.prepareConcurrentRequest(cs, requestEvent, checkpoints)
	cs.h2Mu.Unlock()
	if commitSequence > 0 {
		if err := checkpoints.markProcessed(reqKey, commitSequence); err != nil {
			cs.h2Mu.Lock()
			cs.aborted = true
			cs.h2Mu.Unlock()
		}
	}
	if handled {
		return
	}

	cs.h2WG.Go(func() {
		exec, err := e.sendRequest(ctx, cs.client, requestEvent)
		if err != nil {
			cs.h2Mu.Lock()
			e.finishRequestError(cs, requestEvent, err, exec)
			cs.h2Mu.Unlock()
			return
		}

		cs.h2Mu.Lock()
		abort := e.finishRequestSuccess(cs, requestEvent, exec, nil)
		commitSequence := 0
		if !abort {
			commitSequence = cs.completeCheckpointSequence(requestEvent.Sequence)
		}
		cs.h2Mu.Unlock()

		if abort {
			return
		}
		if commitSequence > 0 {
			if err := checkpoints.markProcessed(reqKey, commitSequence); err != nil {
				cs.h2Mu.Lock()
				cs.aborted = true
				cs.h2Mu.Unlock()
				return
			}
		}
	})
}

func (e *Engine) paceRequest(ctx context.Context, cs *connState, requestEvent model.Event) error {
	if err := e.sleepForPacing(ctx, cs.previousTimestamp, cs.previousTimestampSet, requestEvent.Timestamp); err != nil {
		return err
	}
	cs.previousTimestamp, cs.previousTimestampSet = advancePacingClock(
		cs.previousTimestamp,
		cs.previousTimestampSet,
		requestEvent.Timestamp,
	)
	return nil
}

func (e *Engine) prepareRequest(cs *connState, requestEvent model.Event, checkpoints *checkpointStore) (model.ConnectionKey, bool, bool) {
	reqKey := model.ConnectionKey{Node: requestEvent.Node, ConnectionID: requestEvent.ConnectionID}

	if checkpoints.alreadyProcessed(reqKey, requestEvent.Sequence) {
		cs.skipped++
		cs.skipValidationForSequence(requestEvent.Sequence)
		return reqKey, true, false
	}

	if e.shouldSkipByIdempotencyPolicy(requestEvent) {
		cs.skipped++
		cs.skipValidationForSequence(requestEvent.Sequence)
		if err := checkpoints.markProcessed(reqKey, requestEvent.Sequence); err != nil {
			cs.aborted = true
			return reqKey, true, true
		}
		return reqKey, true, false
	}

	if e.cfg.Replay.DryRun {
		cs.skipped++
		cs.skipValidationForSequence(requestEvent.Sequence)
		return reqKey, true, false
	}

	return reqKey, false, false
}

func (e *Engine) prepareConcurrentRequest(cs *connState, requestEvent model.Event, checkpoints *checkpointStore) (model.ConnectionKey, bool, int) {
	reqKey := model.ConnectionKey{Node: requestEvent.Node, ConnectionID: requestEvent.ConnectionID}

	if checkpoints.alreadyProcessed(reqKey, requestEvent.Sequence) {
		cs.skipped++
		cs.skipValidationForSequence(requestEvent.Sequence)
		return reqKey, true, 0
	}

	if e.shouldSkipByIdempotencyPolicy(requestEvent) {
		cs.skipped++
		cs.skipValidationForSequence(requestEvent.Sequence)
		cs.trackCheckpointSequence(requestEvent.Sequence)
		return reqKey, true, cs.completeCheckpointSequence(requestEvent.Sequence)
	}

	if e.cfg.Replay.DryRun {
		cs.skipped++
		cs.skipValidationForSequence(requestEvent.Sequence)
		return reqKey, true, 0
	}

	cs.trackCheckpointSequence(requestEvent.Sequence)
	return reqKey, false, 0
}

func (cs *connState) trackCheckpointSequence(sequence int) {
	if sequence <= cs.checkpointWatermark {
		return
	}
	cs.checkpointOrder = append(cs.checkpointOrder, sequence)
}

func (cs *connState) completeCheckpointSequence(sequence int) int {
	if sequence <= cs.checkpointWatermark {
		return 0
	}
	previous := cs.checkpointWatermark
	if cs.checkpointCompleted == nil {
		cs.checkpointCompleted = make(map[int]struct{})
	}
	cs.checkpointCompleted[sequence] = struct{}{}
	for len(cs.checkpointOrder) > 0 {
		next := cs.checkpointOrder[0]
		if _, ok := cs.checkpointCompleted[next]; !ok {
			break
		}
		delete(cs.checkpointCompleted, next)
		cs.checkpointWatermark = next
		cs.checkpointOrder = cs.checkpointOrder[1:]
	}
	if cs.checkpointWatermark == previous {
		return 0
	}
	return cs.checkpointWatermark
}

func (e *Engine) finishRequestError(cs *connState, requestEvent model.Event, err error, exec requestExecution) {
	cs.sendErrors++
	cs.aborted = true
	if !exec.attempted {
		e.recordStatusMetric(requestEvent, metricStatusForSendError(err))
	}
}

func (e *Engine) finishRequestSuccess(cs *connState, requestEvent model.Event, exec requestExecution, checkpointErr error) bool {
	cs.sent++
	cs.responsesReceived++

	if checkpointErr != nil {
		cs.aborted = true
		return true
	}

	if !e.shouldRetainResponseForValidation() {
		return false
	}
	if expected, ok := cs.pendingExpected[requestEvent.Sequence]; ok {
		if e.responseValidationFailed(expected, exec) {
			cs.validationFailed++
		}
		delete(cs.pendingExpected, requestEvent.Sequence)
	} else {
		cs.pendingActual[requestEvent.Sequence] = exec
		if expected, ok := e.expectedResponseFromRequestEvent(requestEvent); ok {
			cs.pendingInlineExpected[requestEvent.Sequence] = expected
		}
	}
	return false
}

func (e *Engine) validatePendingInlineResponses(cs *connState) {
	for sequence, expected := range cs.pendingInlineExpected {
		actual, ok := cs.pendingActual[sequence]
		if !ok {
			continue
		}
		if e.responseValidationFailed(expected, actual) {
			cs.validationFailed++
		}
		delete(cs.pendingInlineExpected, sequence)
		delete(cs.pendingActual, sequence)
	}
}

func (e *Engine) finalizePendingValidation(cs *connState) {
	if !e.shouldRetainResponseForValidation() {
		return
	}
	cs.validationFailed += int64(len(cs.pendingExpected))
	cs.validationFailed += int64(len(cs.pendingInlineExpected))
	clear(cs.pendingExpected)
	clear(cs.pendingInlineExpected)
	clear(cs.pendingActual)
}

func (e *Engine) expectedResponseFromRequestEvent(requestEvent model.Event) (model.Event, bool) {
	if !e.shouldRetainResponseForValidation() || requestEvent.AccessLogType == model.AccessLogTypeDownstreamStart || requestEvent.Status == 0 {
		return model.Event{}, false
	}
	return model.Event{
		Type:         model.EventResponse,
		Node:         requestEvent.Node,
		ConnectionID: requestEvent.ConnectionID,
		StreamID:     requestEvent.StreamID,
		Sequence:     requestEvent.Sequence,
		Status:       requestEvent.Status,
	}, true
}

// handleResponseEvent processes a response event for validation rendezvous.
// If the actual response (from the target) is already available, validation
// runs immediately. Otherwise, the expected response is stored for later.
func (e *Engine) handleResponseEvent(cs *connState, ev model.Event) {
	if !e.shouldRetainResponseForValidation() {
		return
	}
	if cs.consumeSkippedValidation(ev.Sequence) {
		return
	}
	if actual, ok := cs.pendingActual[ev.Sequence]; ok {
		if e.responseValidationFailed(ev, actual) {
			cs.validationFailed++
		}
		delete(cs.pendingActual, ev.Sequence)
		delete(cs.pendingInlineExpected, ev.Sequence)
	} else {
		cs.pendingExpected[ev.Sequence] = ev
	}
}

func (e *Engine) shouldRetainResponseForValidation() bool {
	validation := e.cfg.Replay.Validation
	return validation.Enabled && (validation.Status || validation.Headers || validation.Body)
}

func (e *Engine) recordAttemptMetrics(requestEvent model.Event, exec requestExecution, status string) {
	if !e.cfg.Metrics.Enabled || e.metrics == nil || !exec.attempted {
		return
	}
	safeLabel := e.metricLabelForRequest(requestEvent)
	e.metrics.RecordRequest(e.metricLabelValues, safeLabel, exec.latencyMS, status, exec.egressBytes)
}

func workerActivationDelay(workerIndex, totalWorkers int, rampup time.Duration) time.Duration {
	if workerIndex <= 0 || totalWorkers <= 1 || rampup <= 0 {
		return 0
	}
	return time.Duration(float64(rampup) * float64(workerIndex) / float64(totalWorkers-1))
}

func waitForWorkerActivation(ctx context.Context, delay time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (e *Engine) aggregateResults(results <-chan Summary) Summary {
	final := Summary{Outcome: RunSuccess}
	for s := range results {
		final.RequestsSent += s.RequestsSent
		final.ResponsesReceived += s.ResponsesReceived
		final.SendErrors += s.SendErrors
		final.ValidationFailed += s.ValidationFailed
		final.Skipped += s.Skipped
		final.ConnectionsDone += s.ConnectionsDone
		final.ConnectionsAborted += s.ConnectionsAborted
		if len(s.ConnectionResults) > 0 {
			final.ConnectionResults = append(final.ConnectionResults, s.ConnectionResults...)
		}
	}
	if final.ConnectionsAborted > 0 {
		final.Outcome = RunPartialSuccess
	}
	if final.ValidationFailed > 0 && final.Outcome == RunSuccess {
		final.Outcome = RunPartialSuccess
	}
	if final.RequestsSent == 0 && final.Skipped == 0 && final.SendErrors == 0 {
		final.Outcome = RunFailed
	}
	return final
}

func (e *Engine) replayConnectionWithCheckpoint(ctx context.Context, requests []model.Event, responsesBySequence map[int]model.Event, checkpoints *checkpointStore) Summary {
	// Create a per-connection client/transport to ensure socket isolation per connection_id.
	http2, multiplexed := e.detectHTTP2(requests)
	client, transport := e.makePerConnectionClient(http2)
	defer func() {
		if transport != nil {
			transport.CloseIdleConnections()
		}
	}()

	if multiplexed {
		return e.replayConnectionHTTP2Multiplexed(ctx, client, requests, responsesBySequence, checkpoints)
	}
	return e.replayConnectionSerialized(ctx, client, requests, responsesBySequence, checkpoints)
}

func (e *Engine) makePerConnectionClient(http2 bool) (*http.Client, *http.Transport) {
	dialer := &net.Dialer{Timeout: e.cfg.Replay.Timeout.Connect, KeepAlive: e.cfg.Replay.Timeout.IdleConnection}
	tlsConfig := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: e.cfg.Replay.TLS.InsecureSkipVerify,
	}
	if http2 {
		tlsConfig.NextProtos = []string{"h2", "http/1.1"}
	}
	tr := &http.Transport{
		DialContext:         dialer.DialContext,
		TLSClientConfig:     tlsConfig,
		IdleConnTimeout:     e.cfg.Replay.Timeout.IdleConnection,
		MaxIdleConns:        2,
		MaxIdleConnsPerHost: 1,
		MaxConnsPerHost:     1,
	}
	if http2 {
		tr.ForceAttemptHTTP2 = true
	} else {
		tr.TLSNextProto = make(map[string]func(string, *tls.Conn) http.RoundTripper)
	}
	client := &http.Client{
		Timeout:   e.cfg.Replay.Timeout.Request,
		Transport: tr,
	}
	return client, tr
}

// detectHTTP2 returns (http2, multiplexed) based on the connection requests in a single pass.
func (e *Engine) detectHTTP2(requests []model.Event) (http2 bool, multiplexed bool) {
	isMultiplexedMode := strings.EqualFold(e.cfg.Replay.HTTP2.Mode, "multiplexed")
	var streams map[int]struct{}
	if isMultiplexedMode {
		streams = make(map[int]struct{})
	}
	for _, req := range requests {
		if !http2 {
			version := req.HTTP.Version
			if strings.Contains(version, "HTTP/2") || strings.Contains(version, "http/2") {
				http2 = true
				if !isMultiplexedMode {
					break
				}
			}
		}
		if isMultiplexedMode {
			streamID := req.StreamID
			if streamID == 0 {
				streamID = 1
			}
			streams[streamID] = struct{}{}
		}
	}
	multiplexed = isMultiplexedMode && http2 && len(streams) > 1
	return http2, multiplexed
}

func cloneResponsesBySequence(responses map[int]model.Event) map[int]model.Event {
	if len(responses) == 0 {
		return nil
	}
	cloned := make(map[int]model.Event, len(responses))
	for sequence, response := range responses {
		cloned[sequence] = response
	}
	return cloned
}

func (e *Engine) replayConnectionSerialized(ctx context.Context, client *http.Client, requests []model.Event, responsesBySequence map[int]model.Event, checkpoints *checkpointStore) Summary {
	pendingResponses := cloneResponsesBySequence(responsesBySequence)
	result := Summary{}
	if len(requests) == 0 {
		return result
	}

	connID := requests[0].ConnectionID
	connKey := model.ConnectionKey{Node: requests[0].Node, ConnectionID: connID}
	var connResult ConnectionResult

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
		previousTimestamp, previousTimestampSet = advancePacingClock(
			previousTimestamp,
			previousTimestampSet,
			requestEvent.Timestamp,
		)

		requestKey := model.ConnectionKey{Node: requestEvent.Node, ConnectionID: requestEvent.ConnectionID}

		if checkpoints.alreadyProcessed(requestKey, requestEvent.Sequence) {
			result.Skipped++
			delete(pendingResponses, requestEvent.Sequence)
			continue
		}

		if e.shouldSkipByIdempotencyPolicy(requestEvent) {
			result.Skipped++
			delete(pendingResponses, requestEvent.Sequence)
			if err := checkpoints.markProcessed(requestKey, requestEvent.Sequence); err != nil {
				result.ConnectionsAborted++
				result.Outcome = RunFailed
				return result
			}
			continue
		}

		// Dry-run: do not send network requests; count as skipped (do not persist checkpoint)
		if e.cfg.Replay.DryRun {
			delete(pendingResponses, requestEvent.Sequence)
			result.Skipped++
			continue
		}

		exec, err := e.sendRequest(ctx, client, requestEvent)

		if err != nil {
			result.SendErrors++
			result.ConnectionsAborted++
			if result.RequestsSent > 0 || result.Skipped > 0 {
				result.Outcome = RunPartialSuccess
			} else {
				result.Outcome = RunFailed
			}
			if !exec.attempted {
				e.recordStatusMetric(requestEvent, metricStatusForSendError(err))
			}
			connResult = connectionResultFromSummary(connKey, ConnectionAborted, result)
			result.ConnectionResults = append(result.ConnectionResults, connResult)
			return result
		}

		result.RequestsSent++
		result.ResponsesReceived++

		if err := checkpoints.markProcessed(requestKey, requestEvent.Sequence); err != nil {
			result.ConnectionsAborted++
			result.Outcome = RunFailed
			return result
		}
		if expected, ok := pendingResponses[requestEvent.Sequence]; ok {
			delete(pendingResponses, requestEvent.Sequence)
			if e.responseValidationFailed(expected, exec) {
				if slog.Default().Enabled(ctx, slog.LevelDebug) {
					slog.DebugContext(ctx, "Validation failed",
						"node", requestEvent.Node,
						"conn", requestEvent.ConnectionID,
						"seq", requestEvent.Sequence,
						"status", exec.statusCode,
						"expected_status", expected.Status,
					)
					if len(exec.body) > 0 {
						slog.DebugContext(ctx, "Validation failed body", "body", string(exec.body))
					}
				}
				result.ValidationFailed++
				if result.Outcome == "" {
					result.Outcome = RunPartialSuccess
				}
			}
		} else if expected, ok := e.expectedResponseFromRequestEvent(requestEvent); ok {
			if e.responseValidationFailed(expected, exec) {
				result.ValidationFailed++
				if result.Outcome == "" {
					result.Outcome = RunPartialSuccess
				}
			}
		}

	}

	if e.shouldRetainResponseForValidation() && len(pendingResponses) > 0 {
		result.ValidationFailed += int64(len(pendingResponses))
		if result.Outcome == "" {
			result.Outcome = RunPartialSuccess
		}
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
	if result.ConnectionsAborted > 0 {
		connResult = connectionResultFromSummary(connKey, ConnectionAborted, result)
	} else {
		connResult = connectionResultFromSummary(connKey, ConnectionCompleted, result)
	}
	result.ConnectionResults = append(result.ConnectionResults, connResult)
	return result
}

func (e *Engine) replayConnectionHTTP2Multiplexed(ctx context.Context, client *http.Client, requests []model.Event, responsesBySequence map[int]model.Event, checkpoints *checkpointStore) Summary {
	streams := groupRequestsByStream(requests)
	if len(streams) == 0 {
		return Summary{ConnectionsDone: 1, Outcome: RunSuccess}
	}

	results := make(chan Summary, len(streams))
	var wg sync.WaitGroup

	responseStreams := groupResponsesByStream(responsesBySequence)
	for streamID, streamRequests := range streams {
		wg.Go(func() {
			if ctx.Err() != nil {
				results <- Summary{ConnectionsAborted: 1, Outcome: RunFailed}
				return
			}
			results <- e.replayConnectionSerialized(ctx, client, streamRequests, responseStreams[streamID], checkpoints)
		})
	}

	wg.Wait()
	close(results)

	aggregated := Summary{Outcome: RunSuccess}
	for streamResult := range results {
		aggregated.RequestsSent += streamResult.RequestsSent
		aggregated.ResponsesReceived += streamResult.ResponsesReceived
		aggregated.SendErrors += streamResult.SendErrors
		aggregated.ValidationFailed += streamResult.ValidationFailed
		aggregated.Skipped += streamResult.Skipped
		aggregated.ConnectionsAborted += streamResult.ConnectionsAborted
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
	connKey := model.ConnectionKey{}
	if len(requests) > 0 {
		connKey = model.ConnectionKey{Node: requests[0].Node, ConnectionID: requests[0].ConnectionID}
	}
	connResult := connectionResultFromSummary(connKey, ConnectionCompleted, aggregated)
	if aggregated.ConnectionsAborted > 0 {
		connResult.Outcome = ConnectionAborted
	}
	aggregated.ConnectionResults = append(aggregated.ConnectionResults, connResult)
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
	return grouped
}

func groupResponsesByStream(responses map[int]model.Event) map[int]map[int]model.Event {
	grouped := make(map[int]map[int]model.Event)
	for sequence, response := range responses {
		streamID := response.StreamID
		if streamID == 0 {
			streamID = 1
		}
		streamResponses := grouped[streamID]
		if streamResponses == nil {
			streamResponses = make(map[int]model.Event)
			grouped[streamID] = streamResponses
		}
		streamResponses[sequence] = response
	}
	return grouped
}

func connectionBelongsToShard(connectionKey model.ConnectionKey, shardIndex, shardCount int) bool {
	if shardCount <= 1 {
		return true
	}
	hasher := fnv.New32a()
	var buf [8]byte
	_, _ = hasher.Write([]byte(connectionKey.Node))
	_, _ = hasher.Write([]byte{0})
	binary.LittleEndian.PutUint64(buf[:], uint64(connectionKey.ConnectionID))
	_, _ = hasher.Write(buf[:])
	return int(hasher.Sum32()%uint32(shardCount)) == shardIndex
}

func advancePacingClock(previous time.Time, previousSet bool, currentRaw string) (time.Time, bool) {
	current, ok := model.ParseTimestamp(currentRaw)
	if !ok || (previousSet && !current.After(previous)) {
		return previous, previousSet
	}
	return current, true
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
	effectiveHeaders := e.effectiveRequestHeaders(request.Headers)
	for _, headerName := range policy.RequireHeaderForAllow {
		if hasHeaderValue(effectiveHeaders, headerName) {
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

func (e *Engine) effectiveRequestHeaders(recorded map[string][]string) http.Header {
	headers := make(http.Header, len(recorded)+len(e.cfg.Header.Set))
	for key, values := range recorded {
		if strings.HasPrefix(key, ":") {
			continue
		}
		for _, value := range values {
			headers.Add(key, value)
		}
	}
	for _, headerName := range e.cfg.Header.Drop {
		headers.Del(headerName)
	}
	if e.parsedOverrideURL != nil {
		headers.Set("Host", e.parsedOverrideURL.Host)
	}
	for key, value := range e.cfg.Header.Set {
		headers.Set(key, value)
	}
	return headers
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
			if exec.attempted {
				e.recordAttemptMetrics(requestEvent, exec, metricStatusForSendError(err))
			}
			slog.DebugContext(ctx, "Request failed",
				"conn", requestEvent.ConnectionID,
				"seq", requestEvent.Sequence,
				"path", requestEvent.HTTP.Path,
				"error", err,
			)
			if attempt == maxAttempts || !e.shouldRetryError(err) {
				return exec, err
			}
			if sleepErr := e.sleepBackoff(ctx, attempt); sleepErr != nil {
				return exec, sleepErr
			}
			continue
		}

		lastExec = exec
		e.recordAttemptMetrics(requestEvent, exec, fmt.Sprintf("%d", exec.statusCode))
		if attempt < maxAttempts && e.shouldRetryStatus(exec.statusCode) {
			slog.DebugContext(ctx, "Request retryable status",
				"conn", requestEvent.ConnectionID,
				"seq", requestEvent.Sequence,
				"path", requestEvent.HTTP.Path,
				"status", exec.statusCode,
			)
			if sleepErr := e.sleepBackoff(ctx, attempt); sleepErr != nil {
				return exec, sleepErr
			}
			continue
		}
		slog.DebugContext(ctx, "Request success",
			"conn", requestEvent.ConnectionID,
			"seq", requestEvent.Sequence,
			"path", requestEvent.HTTP.Path,
			"status", exec.statusCode,
		)
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

	req.Header = e.effectiveRequestHeaders(requestEvent.Headers)

	if host := req.Header.Get("Host"); host != "" {
		req.Host = host
	}

	start := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
		return requestExecution{
			latencyMS: time.Since(start).Seconds() * 1000,
			attempted: true,
		}, err
	}
	defer resp.Body.Close()
	// Retain a bounded response body for validation, then drain any remainder so
	// the transport can reuse the recorded connection's socket.
	lr := io.LimitReader(resp.Body, maxBodyRead)
	body, err := io.ReadAll(lr)
	var drainedBytes int64
	if err == nil {
		drainedBytes, err = io.Copy(io.Discard, resp.Body)
	}
	headerBytes := responseHeaderBytes(resp.Header)
	if err != nil {
		return requestExecution{
			latencyMS:   time.Since(start).Seconds() * 1000,
			statusCode:  resp.StatusCode,
			egressBytes: headerBytes + int64(len(body)) + drainedBytes,
			attempted:   true,
		}, err
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
		egressBytes: int64(len(body)) + drainedBytes + headerBytes,
		attempted:   true,
		headers:     headers,
		body:        body,
	}, nil
}

func (e *Engine) shouldRetryStatus(statusCode int) bool {
	return slices.Contains(e.cfg.Replay.Retry.RetryOnStatuses, statusCode)
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

func metricStatusForSendError(err error) string {
	if err == nil {
		return "send_error"
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
	case strings.Contains(lower, "connection refused"):
		return "connection_refused"
	case strings.Contains(lower, "connection reset"):
		return "connection_reset"
	case strings.Contains(lower, "tls"):
		return "tls"
	case strings.Contains(lower, "dial tcp"), strings.Contains(lower, "no such host"):
		return "network"
	default:
		return "send_error"
	}
}

func (e *Engine) metricLabelForRequest(requestEvent model.Event) string {
	label, _, _ := strings.Cut(requestEvent.HTTP.Path, "?")
	if len(e.parsedPathTemplates) > 0 {
		label = MatchPathTemplate(label, e.parsedPathTemplates)
	}
	if label == "" {
		label = "unknown"
	}
	if e.metrics != nil {
		return e.metrics.GetSafeLabel(label, e.cfg.Metrics.MaxLabels)
	}
	return label
}

func (e *Engine) recordStatusMetric(requestEvent model.Event, status string) {
	if !e.cfg.Metrics.Enabled || e.metrics == nil || status == "" {
		return
	}
	safeLabel := e.metricLabelForRequest(requestEvent)
	e.metrics.RecordStatus(e.metricLabelValues, safeLabel, status)
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
		exp := min(max(attempt-1, 0),
			// cap exponent to avoid absurd durations
			30)
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
		if strings.HasPrefix(key, ":") {
			continue
		}
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
	if e.parsedOverrideURL != nil {
		requestURI := event.HTTP.Path
		parsedPath, err := url.ParseRequestURI(requestURI)
		if err != nil {
			return "", fmt.Errorf("parse request path: %w", err)
		}
		u := e.parsedOverrideURL.JoinPath(parsedPath.Path)
		u.RawQuery = parsedPath.RawQuery
		return u.String(), nil
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
	return scheme + "://" + event.HTTP.Authority + event.HTTP.Path, nil
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
