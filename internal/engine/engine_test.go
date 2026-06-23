package engine

import (
	"context"
	"encoding/base64"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/reqfleet/replay/internal/config"
	"github.com/reqfleet/replay/internal/metrics"
	"github.com/reqfleet/replay/internal/model"
)

// runReplay streams the provided events into the engine's ReplayStream and
// waits for completion. This replaces the previous synchronous Replay API.
func runReplay(eng *Engine, events []model.Event) (Summary, error) {
	ctx := context.Background()
	ch := make(chan model.Event)
	var summary Summary
	var err error
	done := make(chan struct{})
	go func() {
		summary, err = eng.ReplayStream(ctx, ch)
		close(done)
	}()
	for _, e := range events {
		ch <- e
	}
	close(ch)
	<-done
	return summary, err
}

type replayResult struct {
	summary Summary
	err     error
}

func runReplayAsync(eng *Engine, events []model.Event) <-chan replayResult {
	resultCh := make(chan replayResult, 1)
	go func() {
		summary, err := runReplay(eng, events)
		resultCh <- replayResult{summary: summary, err: err}
	}()
	return resultCh
}

func waitForRequestStarts(t *testing.T, startCh <-chan struct{}, count int, timeout time.Duration) {
	t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()

	for range count {
		select {
		case <-startCh:
		case <-deadline.C:
			t.Fatalf("waitForRequestStarts(count=%d, timeout=%s) timed out", count, timeout)
		}
	}
}

func rampupTestEvents(target *url.URL, connections int) []model.Event {
	events := []model.Event{{Type: model.EventMeta}}
	for connectionID := 1; connectionID <= connections; connectionID++ {
		events = append(events,
			model.Event{Type: model.EventConnectionOpen, ConnectionID: connectionID},
			model.Event{
				Type:         model.EventRequest,
				ConnectionID: connectionID,
				Sequence:     1,
				HTTP: model.HTTPRequestMeta{
					Method:    http.MethodGet,
					Scheme:    target.Scheme,
					Authority: target.Host,
					Path:      "/",
				},
			},
			model.Event{Type: model.EventConnectionClose, ConnectionID: connectionID},
		)
	}
	return events
}

func TestReplayRetriesOnConfiguredStatus(t *testing.T) {
	var attempts int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		current := atomic.AddInt64(&attempts, 1)
		if current == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("retry"))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()

	target, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("url parse failed: %v", err)
	}

	cfg := config.Default()
	cfg.Replay.Retry.MaxAttempts = 2
	cfg.Replay.Retry.Backoff = "none"
	cfg.Replay.Retry.RetryOnStatuses = []int{http.StatusServiceUnavailable}

	eng := New(cfg, metrics.New())
	events := []model.Event{
		{Type: model.EventMeta},
		{Type: model.EventConnectionOpen, ConnectionID: 1},
		{
			Type:         model.EventRequest,
			ConnectionID: 1,
			Sequence:     1,
			HTTP: model.HTTPRequestMeta{
				Method:    http.MethodGet,
				Scheme:    target.Scheme,
				Authority: target.Host,
				Path:      "/",
			},
		},
		{Type: model.EventConnectionClose, ConnectionID: 1},
	}

	summary, err := runReplay(eng, events)
	if err != nil {
		t.Fatalf("replay failed: %v", err)
	}
	if summary.RequestsSent != 1 {
		t.Fatalf("summary.RequestsSent = %d, want 1", summary.RequestsSent)
	}
	if atomic.LoadInt64(&attempts) != 2 {
		t.Fatalf("attempt count = %d, want 2", attempts)
	}
}

func TestReplayMarksValidationFailedOnStatusMismatch(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("boom"))
	}))
	defer srv.Close()
	target, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("url parse failed: %v", err)
	}

	cfg := config.Default()
	cfg.Replay.Validation.Enabled = true
	cfg.Replay.Validation.Status = true

	eng := New(cfg, metrics.New())
	events := []model.Event{
		{Type: model.EventMeta},
		{Type: model.EventConnectionOpen, ConnectionID: 1},
		{
			Type:         model.EventRequest,
			ConnectionID: 1,
			Sequence:     1,
			HTTP: model.HTTPRequestMeta{
				Method:    http.MethodGet,
				Scheme:    target.Scheme,
				Authority: target.Host,
				Path:      "/",
			},
		},
		{
			Type:         model.EventResponse,
			ConnectionID: 1,
			Sequence:     1,
			Status:       http.StatusOK,
		},
		{Type: model.EventConnectionClose, ConnectionID: 1},
	}

	summary, err := runReplay(eng, events)
	if err != nil {
		t.Fatalf("replay failed: %v", err)
	}
	if summary.ValidationFailed != 1 {
		t.Fatalf("summary.ValidationFailed = %d, want 1", summary.ValidationFailed)
	}
	if summary.Outcome != RunPartialSuccess {
		t.Fatalf("summary.Outcome = %s, want %s", summary.Outcome, RunPartialSuccess)
	}
}

func TestReplayTreatsConnectionRefusedAsPartialSuccess(t *testing.T) {
	addr := closedLocalAddress(t)

	cfg := config.Default()
	eng := New(cfg, metrics.New())
	events := []model.Event{
		{Type: model.EventMeta},
		{Type: model.EventConnectionOpen, ConnectionID: 1},
		{
			Type:         model.EventRequest,
			ConnectionID: 1,
			Sequence:     1,
			HTTP: model.HTTPRequestMeta{
				Method:    http.MethodGet,
				Scheme:    "http",
				Authority: addr,
				Path:      "/transport",
			},
		},
		{Type: model.EventConnectionClose, ConnectionID: 1},
	}

	summary, err := runReplay(eng, events)
	if err != nil {
		t.Fatalf("runReplay(eng, events) error: %v", err)
	}
	if got, want := summary.Outcome, RunPartialSuccess; got != want {
		t.Fatalf("summary.Outcome = %s, want %s", got, want)
	}
	if got, want := summary.SendErrors, int64(1); got != want {
		t.Fatalf("summary.SendErrors = %d, want %d", got, want)
	}
	if got, want := summary.ConnectionsAborted, int64(1); got != want {
		t.Fatalf("summary.ConnectionsAborted = %d, want %d", got, want)
	}
}

func TestReplayEmitsSyntheticStatusForTransportSendErrors(t *testing.T) {
	tests := []struct {
		name       string
		configure  func(*testing.T, *config.Config) (authority string, cleanup func())
		wantStatus string
	}{
		{
			name: "connection_refused",
			configure: func(t *testing.T, cfg *config.Config) (string, func()) {
				t.Helper()
				cfg.Replay.Timeout.Connect = 100 * time.Millisecond
				return closedLocalAddress(t), func() {}
			},
			wantStatus: "connection_refused",
		},
		{
			name: "timeout",
			configure: func(t *testing.T, cfg *config.Config) (string, func()) {
				t.Helper()
				cfg.Replay.Timeout.Request = 20 * time.Millisecond
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					time.Sleep(100 * time.Millisecond)
					w.WriteHeader(http.StatusOK)
				}))
				target, err := url.Parse(srv.URL)
				if err != nil {
					t.Fatalf("url.Parse(%q) error: %v", srv.URL, err)
				}
				return target.Host, srv.Close
			},
			wantStatus: "timeout",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := config.Default()
			reg := metrics.New()
			authority, cleanup := tt.configure(t, &cfg)
			defer cleanup()

			eng := New(cfg, reg)
			events := []model.Event{
				{Type: model.EventMeta},
				{Type: model.EventConnectionOpen, ConnectionID: 1},
				{
					Type:         model.EventRequest,
					ConnectionID: 1,
					Sequence:     1,
					HTTP: model.HTTPRequestMeta{
						Method:    http.MethodGet,
						Scheme:    "http",
						Authority: authority,
						Path:      "/transport",
					},
				},
				{Type: model.EventConnectionClose, ConnectionID: 1},
			}

			summary, err := runReplay(eng, events)
			if err != nil {
				t.Fatalf("runReplay(eng, events) error: %v", err)
			}
			if got, want := summary.Outcome, RunPartialSuccess; got != want {
				t.Fatalf("summary.Outcome = %s, want %s", got, want)
			}
			counter := reg.StatusCounter.WithLabelValues(
				cfg.Labels.CollectionID,
				cfg.Labels.PlanID,
				cfg.Labels.RunID,
				cfg.Labels.EngineNo,
				"/transport",
				cfg.Labels.Zone,
				tt.wantStatus,
			)
			if got, want := testutil.ToFloat64(counter), float64(1); got != want {
				t.Fatalf("status counter for %s = %v, want %v", tt.wantStatus, got, want)
			}
		})
	}
}

func TestReplayHeaderValidationIgnoresConfiguredHeaders(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("x-request-id", "actual-req-id")
		w.Header().Set("content-type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	target, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("url parse failed: %v", err)
	}

	cfg := config.Default()
	cfg.Replay.Validation.Enabled = true
	cfg.Replay.Validation.Status = true
	cfg.Replay.Validation.Headers = true
	cfg.Replay.Validation.IgnoreHeaders = []string{"x-request-id"}

	eng := New(cfg, metrics.New())
	events := []model.Event{
		{Type: model.EventMeta},
		{Type: model.EventConnectionOpen, ConnectionID: 1},
		{
			Type:         model.EventRequest,
			ConnectionID: 1,
			Sequence:     1,
			HTTP: model.HTTPRequestMeta{
				Method:    http.MethodGet,
				Scheme:    target.Scheme,
				Authority: target.Host,
				Path:      "/",
			},
		},
		{
			Type:         model.EventResponse,
			ConnectionID: 1,
			Sequence:     1,
			Status:       http.StatusOK,
			Headers: map[string][]string{
				"content-type": {"application/json"},
				"x-request-id": {"expected-other-id"},
			},
		},
		{Type: model.EventConnectionClose, ConnectionID: 1},
	}

	summary, err := runReplay(eng, events)
	if err != nil {
		t.Fatalf("replay failed: %v", err)
	}
	if summary.ValidationFailed != 0 {
		t.Fatalf("summary.ValidationFailed = %d, want 0", summary.ValidationFailed)
	}
	if summary.Outcome != RunSuccess {
		t.Fatalf("summary.Outcome = %s, want %s", summary.Outcome, RunSuccess)
	}
}

func closedLocalAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen(127.0.0.1:0) error: %v", err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("listener.Close() error: %v", err)
	}
	return addr
}

func TestReplayBodyValidationMismatch(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("actual-body"))
	}))
	defer srv.Close()

	target, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("url parse failed: %v", err)
	}

	cfg := config.Default()
	cfg.Replay.Validation.Enabled = true
	cfg.Replay.Validation.Status = true
	cfg.Replay.Validation.Body = true

	eng := New(cfg, metrics.New())
	events := []model.Event{
		{Type: model.EventMeta},
		{Type: model.EventConnectionOpen, ConnectionID: 1},
		{
			Type:         model.EventRequest,
			ConnectionID: 1,
			Sequence:     1,
			HTTP: model.HTTPRequestMeta{
				Method:    http.MethodGet,
				Scheme:    target.Scheme,
				Authority: target.Host,
				Path:      "/",
			},
		},
		{
			Type:         model.EventResponse,
			ConnectionID: 1,
			Sequence:     1,
			Status:       http.StatusOK,
			Body: model.Body{
				Encoding: "base64",
				Content:  base64.StdEncoding.EncodeToString([]byte("expected-body")),
			},
		},
		{Type: model.EventConnectionClose, ConnectionID: 1},
	}

	summary, err := runReplay(eng, events)
	if err != nil {
		t.Fatalf("replay failed: %v", err)
	}
	if summary.ValidationFailed != 1 {
		t.Fatalf("summary.ValidationFailed = %d, want 1", summary.ValidationFailed)
	}
	if summary.Outcome != RunPartialSuccess {
		t.Fatalf("summary.Outcome = %s, want %s", summary.Outcome, RunPartialSuccess)
	}
}

func TestReplaySkipsMutationWithoutIdempotencyHeader(t *testing.T) {
	var attempts int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&attempts, 1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()

	target, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("url parse failed: %v", err)
	}

	cfg := config.Default()
	eng := New(cfg, metrics.New())
	events := []model.Event{
		{Type: model.EventMeta},
		{Type: model.EventConnectionOpen, ConnectionID: 1},
		{
			Type:         model.EventRequest,
			ConnectionID: 1,
			Sequence:     1,
			HTTP: model.HTTPRequestMeta{
				Method:    http.MethodPost,
				Scheme:    target.Scheme,
				Authority: target.Host,
				Path:      "/mutate",
			},
		},
		{Type: model.EventConnectionClose, ConnectionID: 1},
	}

	summary, err := runReplay(eng, events)
	if err != nil {
		t.Fatalf("replay failed: %v", err)
	}
	if summary.Skipped != 1 {
		t.Fatalf("summary.Skipped = %d, want 1", summary.Skipped)
	}
	if summary.RequestsSent != 0 {
		t.Fatalf("summary.RequestsSent = %d, want 0", summary.RequestsSent)
	}
	if summary.Outcome != RunSuccess {
		t.Fatalf("summary.Outcome = %s, want %s", summary.Outcome, RunSuccess)
	}
	if atomic.LoadInt64(&attempts) != 0 {
		t.Fatalf("attempt count = %d, want 0", attempts)
	}
}

func TestReplayAllowsImplicitLifecycleCloseAtEOF(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()

	target, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("url parse failed: %v", err)
	}

	cfg := config.Default()
	eng := New(cfg, metrics.New())
	events := []model.Event{
		{Type: model.EventMeta},
		{Type: model.EventConnectionOpen, ConnectionID: 1},
		{
			Type:         model.EventRequest,
			ConnectionID: 1,
			Sequence:     1,
			HTTP: model.HTTPRequestMeta{
				Method:    http.MethodGet,
				Scheme:    target.Scheme,
				Authority: target.Host,
				Path:      "/",
			},
		},
	}

	summary, err := runReplay(eng, events)
	if err != nil {
		t.Fatalf("runReplay() error = %v, want nil", err)
	}
	if summary.Outcome != RunSuccess {
		t.Errorf("runReplay() outcome = %v, want %v", summary.Outcome, RunSuccess)
	}
}

func TestReplayRespectsShardAssignment(t *testing.T) {
	var attempts int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&attempts, 1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()

	target, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("url parse failed: %v", err)
	}

	cfg := config.Default()
	cfg.Replay.Sharding.ShardCount = 2
	cfg.Replay.Sharding.ShardIndex = 0

	connections := []int{1, 2, 3, 4}
	events := []model.Event{{Type: model.EventMeta}}
	expectedSent := int64(0)
	for i, conn := range connections {
		events = append(events,
			model.Event{Type: model.EventConnectionOpen, ConnectionID: conn},
			model.Event{
				Type:         model.EventRequest,
				ConnectionID: conn,
				Sequence:     i + 1,
				HTTP: model.HTTPRequestMeta{
					Method:    http.MethodGet,
					Scheme:    target.Scheme,
					Authority: target.Host,
					Path:      "/",
				},
			},
			model.Event{Type: model.EventConnectionClose, ConnectionID: conn},
		)
		if connectionBelongsToShard(model.ConnectionKey{ConnectionID: conn}, cfg.Replay.Sharding.ShardIndex, cfg.Replay.Sharding.ShardCount) {
			expectedSent++
		}
	}

	eng := New(cfg, metrics.New())
	summary, err := runReplay(eng, events)
	if err != nil {
		t.Fatalf("replay failed: %v", err)
	}
	if summary.RequestsSent != expectedSent {
		t.Fatalf("summary.RequestsSent = %d, want %d", summary.RequestsSent, expectedSent)
	}
	if atomic.LoadInt64(&attempts) != expectedSent {
		t.Fatalf("attempt count = %d, want %d", attempts, expectedSent)
	}
}

func TestReplayZeroRampupStartsAllWorkersImmediately(t *testing.T) {
	startCh := make(chan struct{}, 3)
	releaseCh := make(chan struct{})
	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() {
			close(releaseCh)
		})
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		startCh <- struct{}{}
		<-releaseCh
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()
	t.Cleanup(release)

	target, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("url.Parse(%q) error: %v", srv.URL, err)
	}

	cfg := config.Default()
	cfg.Replay.MaxVirtualUsersPerEngine = 3
	cfg.Replay.MaxActiveConnectionsPerEngine = 3
	cfg.Replay.RampupDuration = 0
	cfg.Replay.Idempotency.Enabled = false

	eng := New(cfg, metrics.New())
	resultCh := runReplayAsync(eng, rampupTestEvents(target, 3))

	waitForRequestStarts(t, startCh, 3, 100*time.Millisecond)

	release()
	result := <-resultCh
	if result.err != nil {
		t.Fatalf("runReplay(eng, events) error: %v", result.err)
	}
	if got, want := result.summary.RequestsSent, int64(3); got != want {
		t.Fatalf("summary.RequestsSent = %d, want %d", got, want)
	}
}

func TestReplayRampupStagesWorkerActivation(t *testing.T) {
	startCh := make(chan struct{}, 3)
	releaseCh := make(chan struct{})
	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() {
			close(releaseCh)
		})
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		startCh <- struct{}{}
		<-releaseCh
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()
	t.Cleanup(release)

	target, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("url.Parse(%q) error: %v", srv.URL, err)
	}

	cfg := config.Default()
	cfg.Replay.MaxVirtualUsersPerEngine = 3
	cfg.Replay.MaxActiveConnectionsPerEngine = 3
	cfg.Replay.RampupDuration = 300 * time.Millisecond
	cfg.Replay.Idempotency.Enabled = false

	eng := New(cfg, metrics.New())
	resultCh := runReplayAsync(eng, rampupTestEvents(target, 3))

	waitForRequestStarts(t, startCh, 1, 150*time.Millisecond)
	waitForRequestStarts(t, startCh, 1, 250*time.Millisecond)
	waitForRequestStarts(t, startCh, 1, 350*time.Millisecond)

	release()
	result := <-resultCh
	if result.err != nil {
		t.Fatalf("runReplay(eng, events) error: %v", result.err)
	}
	if got, want := result.summary.RequestsSent, int64(3); got != want {
		t.Fatalf("summary.RequestsSent = %d, want %d", got, want)
	}
}

func TestConnectionBelongsToShardUsesNode(t *testing.T) {
	baseKey := model.ConnectionKey{ConnectionID: 1}
	baseShard := connectionBelongsToShard(baseKey, 0, 2)
	for i := range 256 {
		candidate := model.ConnectionKey{Node: fmt.Sprintf("envoy-%d", i), ConnectionID: 1}
		if connectionBelongsToShard(candidate, 0, 2) != baseShard {
			return
		}
	}
	t.Fatal("expected node to affect shard assignment")
}

func TestReplayGroupsSameConnectionIDByNode(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()

	target, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("url parse failed: %v", err)
	}

	cfg := config.Default()
	cfg.Replay.Idempotency.Enabled = false

	eng := New(cfg, metrics.New())
	events := []model.Event{
		{Type: model.EventMeta},
		{Type: model.EventConnectionOpen, Node: "envoy-a", ConnectionID: 1},
		{Type: model.EventConnectionOpen, Node: "envoy-b", ConnectionID: 1},
		{Type: model.EventRequest, Node: "envoy-a", ConnectionID: 1, Sequence: 1, HTTP: model.HTTPRequestMeta{Method: http.MethodGet, Scheme: target.Scheme, Authority: target.Host, Path: "/a"}},
		{Type: model.EventRequest, Node: "envoy-b", ConnectionID: 1, Sequence: 1, HTTP: model.HTTPRequestMeta{Method: http.MethodGet, Scheme: target.Scheme, Authority: target.Host, Path: "/b"}},
		{Type: model.EventConnectionClose, Node: "envoy-a", ConnectionID: 1},
		{Type: model.EventConnectionClose, Node: "envoy-b", ConnectionID: 1},
	}

	summary, err := runReplay(eng, events)
	if err != nil {
		t.Fatalf("replay failed: %v", err)
	}
	if summary.RequestsSent != 2 {
		t.Fatalf("summary.RequestsSent = %d, want 2", summary.RequestsSent)
	}
	if len(summary.ConnectionResults) != 2 {
		t.Fatalf("len(summary.ConnectionResults) = %d, want 2", len(summary.ConnectionResults))
	}
	nodes := make(map[string]struct{}, len(summary.ConnectionResults))
	for _, connectionResult := range summary.ConnectionResults {
		nodes[connectionResult.Node] = struct{}{}
	}
	if len(nodes) != 2 {
		t.Fatalf("expected distinct nodes in connection results, got %v", nodes)
	}
}

func TestReplaySkipsAlreadyCheckpointedSequence(t *testing.T) {
	var attempts int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&attempts, 1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()

	target, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("url parse failed: %v", err)
	}

	tmpDir := t.TempDir()
	checkpointPath := filepath.Join(tmpDir, "checkpoint.json")
	checkpointPayload := `{"version":1,"connections":{"1":1}}`
	if err := os.WriteFile(checkpointPath, []byte(checkpointPayload), 0o644); err != nil {
		t.Fatalf("write checkpoint failed: %v", err)
	}

	cfg := config.Default()
	cfg.Replay.Checkpoint.File = checkpointPath

	eng := New(cfg, metrics.New())
	events := []model.Event{
		{Type: model.EventMeta},
		{Type: model.EventConnectionOpen, ConnectionID: 1},
		{
			Type:         model.EventRequest,
			ConnectionID: 1,
			Sequence:     1,
			HTTP: model.HTTPRequestMeta{
				Method:    http.MethodGet,
				Scheme:    target.Scheme,
				Authority: target.Host,
				Path:      "/",
			},
		},
		{Type: model.EventConnectionClose, ConnectionID: 1},
	}

	summary, err := runReplay(eng, events)
	if err != nil {
		t.Fatalf("replay failed: %v", err)
	}
	if summary.Skipped != 1 {
		t.Fatalf("summary.Skipped = %d, want 1", summary.Skipped)
	}
	if summary.RequestsSent != 0 {
		t.Fatalf("summary.RequestsSent = %d, want 0", summary.RequestsSent)
	}
	if atomic.LoadInt64(&attempts) != 0 {
		t.Fatalf("attempt count = %d, want 0", attempts)
	}
}

func TestReplayHTTP2SerializedMode(t *testing.T) {
	var maxInFlight int64
	var inFlight atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		current := inFlight.Add(1)
		for {
			previous := atomic.LoadInt64(&maxInFlight)
			if current <= previous || atomic.CompareAndSwapInt64(&maxInFlight, previous, current) {
				break
			}
		}
		time.Sleep(80 * time.Millisecond)
		inFlight.Add(-1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()

	target, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("url parse failed: %v", err)
	}

	cfg := config.Default()
	cfg.Replay.HTTP2.Mode = "serialized"
	cfg.Replay.Idempotency.Enabled = false

	eng := New(cfg, metrics.New())
	events := []model.Event{
		{Type: model.EventMeta},
		{Type: model.EventConnectionOpen, ConnectionID: 1},
		{Type: model.EventRequest, ConnectionID: 1, StreamID: 1, Sequence: 1, HTTP: model.HTTPRequestMeta{Version: "HTTP/2", Method: http.MethodGet, Scheme: target.Scheme, Authority: target.Host, Path: "/a"}},
		{Type: model.EventRequest, ConnectionID: 1, StreamID: 3, Sequence: 2, HTTP: model.HTTPRequestMeta{Version: "HTTP/2", Method: http.MethodGet, Scheme: target.Scheme, Authority: target.Host, Path: "/b"}},
		{Type: model.EventConnectionClose, ConnectionID: 1},
	}

	_, err = runReplay(eng, events)
	if err != nil {
		t.Fatalf("replay failed: %v", err)
	}
	if atomic.LoadInt64(&maxInFlight) != 1 {
		t.Fatalf("max in-flight = %d, want 1 for serialized mode", maxInFlight)
	}
}

func TestReplayHTTP2MultiplexedMode(t *testing.T) {
	var maxInFlight int64
	var inFlight atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		current := inFlight.Add(1)
		for {
			previous := atomic.LoadInt64(&maxInFlight)
			if current <= previous || atomic.CompareAndSwapInt64(&maxInFlight, previous, current) {
				break
			}
		}
		time.Sleep(80 * time.Millisecond)
		inFlight.Add(-1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()

	target, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("url parse failed: %v", err)
	}

	cfg := config.Default()
	cfg.Replay.HTTP2.Mode = "multiplexed"
	cfg.Replay.HTTP2.MaxConcurrentStreams = 8
	cfg.Replay.Idempotency.Enabled = false

	eng := New(cfg, metrics.New())
	events := []model.Event{
		{Type: model.EventMeta},
		{Type: model.EventConnectionOpen, ConnectionID: 1},
		{Type: model.EventRequest, ConnectionID: 1, StreamID: 1, Sequence: 1, HTTP: model.HTTPRequestMeta{Version: "HTTP/2", Method: http.MethodGet, Scheme: target.Scheme, Authority: target.Host, Path: "/a"}},
		{Type: model.EventRequest, ConnectionID: 1, StreamID: 3, Sequence: 2, HTTP: model.HTTPRequestMeta{Version: "HTTP/2", Method: http.MethodGet, Scheme: target.Scheme, Authority: target.Host, Path: "/b"}},
		{Type: model.EventConnectionClose, ConnectionID: 1},
	}

	_, err = runReplay(eng, events)
	if err != nil {
		t.Fatalf("replay failed: %v", err)
	}
	// With MaxConnsPerHost: 1 unconditionally configured, the HTTP/1.1 fallback
	// forces concurrent streams to be serialized over a single TCP connection.
	// Therefore, the maximum concurrent in-flight requests must be exactly 1.
	if got := atomic.LoadInt64(&maxInFlight); got != 1 {
		t.Fatalf("max in-flight = %d, want exactly 1 due to strict single-connection HTTP/1.1 fallback", got)
	}
}

func TestPerConnectionSocketOwnership(t *testing.T) {
	var mu sync.Mutex
	addrs := make(map[string]struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		addrs[r.RemoteAddr] = struct{}{}
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()

	target, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("url parse failed: %v", err)
	}

	cfg := config.Default()
	cfg.Replay.Idempotency.Enabled = false

	eng := New(cfg, metrics.New())
	events := []model.Event{
		{Type: model.EventMeta},
		{Type: model.EventConnectionOpen, ConnectionID: 1},
		{Type: model.EventRequest, ConnectionID: 1, Sequence: 1, HTTP: model.HTTPRequestMeta{Method: http.MethodGet, Scheme: target.Scheme, Authority: target.Host, Path: "/"}},
		{Type: model.EventConnectionClose, ConnectionID: 1},
		{Type: model.EventConnectionOpen, ConnectionID: 2},
		{Type: model.EventRequest, ConnectionID: 2, Sequence: 1, HTTP: model.HTTPRequestMeta{Method: http.MethodGet, Scheme: target.Scheme, Authority: target.Host, Path: "/"}},
		{Type: model.EventConnectionClose, ConnectionID: 2},
	}

	summary, err := runReplay(eng, events)
	if err != nil {
		t.Fatalf("replay failed: %v", err)
	}
	if summary.RequestsSent != 2 {
		t.Fatalf("summary.RequestsSent = %d, want 2", summary.RequestsSent)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(addrs) != 2 {
		t.Fatalf("expected 2 distinct remote addresses, got %d", len(addrs))
	}
}

func TestDryRunNoNetwork(t *testing.T) {
	var attempts int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&attempts, 1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()

	target, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("url parse failed: %v", err)
	}

	cfg := config.Default()
	cfg.Replay.DryRun = true
	cfg.Replay.Idempotency.Enabled = false

	eng := New(cfg, metrics.New())
	events := []model.Event{
		{Type: model.EventMeta},
		{Type: model.EventConnectionOpen, ConnectionID: 1},
		{Type: model.EventRequest, ConnectionID: 1, Sequence: 1, HTTP: model.HTTPRequestMeta{Method: http.MethodGet, Scheme: target.Scheme, Authority: target.Host, Path: "/"}},
		{Type: model.EventConnectionClose, ConnectionID: 1},
	}

	summary, err := runReplay(eng, events)
	if err != nil {
		t.Fatalf("replay failed: %v", err)
	}
	if summary.RequestsSent != 0 {
		t.Fatalf("summary.RequestsSent = %d, want 0", summary.RequestsSent)
	}
	if summary.Skipped != 1 {
		t.Fatalf("summary.Skipped = %d, want 1", summary.Skipped)
	}
	if atomic.LoadInt64(&attempts) != 0 {
		t.Fatalf("attempt count = %d, want 0", attempts)
	}
}

func TestOverrideHostRewrite(t *testing.T) {
	var seenHost string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenHost = r.Host
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()

	// recorded event has a different authority than the override
	recordedAuthority := "api.prod.example.com"

	target, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("url parse failed: %v", err)
	}

	cfg := config.Default()
	cfg.Target.OverrideURL = srv.URL
	cfg.Replay.Idempotency.Enabled = false

	eng := New(cfg, metrics.New())
	events := []model.Event{
		{Type: model.EventMeta},
		{Type: model.EventConnectionOpen, ConnectionID: 1},
		{Type: model.EventRequest, ConnectionID: 1, Sequence: 1, HTTP: model.HTTPRequestMeta{Method: http.MethodGet, Scheme: "https", Authority: recordedAuthority, Path: "/"}, Headers: map[string][]string{"Host": {recordedAuthority}}},
		{Type: model.EventConnectionClose, ConnectionID: 1},
	}

	summary, err := runReplay(eng, events)
	if err != nil {
		t.Fatalf("replay failed: %v", err)
	}
	if summary.RequestsSent != 1 {
		t.Fatalf("summary.RequestsSent = %d, want 1", summary.RequestsSent)
	}
	// the server should observe the override host
	expectedHost := target.Host
	if seenHost != expectedHost {
		t.Fatalf("seen host = %q, want %q", seenHost, expectedHost)
	}
}

func TestOverrideURLPreservesQueryString(t *testing.T) {
	var seenPath string
	var seenRawQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenPath = r.URL.Path
		seenRawQuery = r.URL.RawQuery
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()

	cfg := config.Default()
	cfg.Target.OverrideURL = srv.URL
	cfg.Replay.Idempotency.Enabled = false

	eng := New(cfg, metrics.New())
	events := []model.Event{
		{Type: model.EventMeta},
		{Type: model.EventConnectionOpen, ConnectionID: 1},
		{
			Type:         model.EventRequest,
			ConnectionID: 1,
			Sequence:     1,
			HTTP: model.HTTPRequestMeta{
				Method:    http.MethodGet,
				Scheme:    "https",
				Authority: "api.prod.example.com",
				Path:      "/api/v1/login?redirect=/home",
			},
		},
		{Type: model.EventConnectionClose, ConnectionID: 1},
	}

	summary, err := runReplay(eng, events)
	if err != nil {
		t.Fatalf("replay failed: %v", err)
	}
	if summary.RequestsSent != 1 {
		t.Fatalf("summary.RequestsSent = %d, want 1", summary.RequestsSent)
	}
	if seenPath != "/api/v1/login" {
		t.Fatalf("seen path = %q, want %q", seenPath, "/api/v1/login")
	}
	if seenRawQuery != "redirect=/home" {
		t.Fatalf("seen raw query = %q, want %q", seenRawQuery, "redirect=/home")
	}
}

func TestReplayAbortsConnectionAfterSendError(t *testing.T) {
	var attempts int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&attempts, 1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()

	cfg := config.Default()
	cfg.Replay.Idempotency.Enabled = false
	cfg.Replay.Lifecycle.RequireOpen = false
	cfg.Replay.Lifecycle.RequireClose = false

	eng := New(cfg, metrics.New())
	events := []model.Event{
		{Type: model.EventMeta},
		{Type: model.EventConnectionOpen, ConnectionID: 1},
		{
			Type:         model.EventRequest,
			ConnectionID: 1,
			Sequence:     1,
			HTTP: model.HTTPRequestMeta{
				Method:    http.MethodGet,
				Scheme:    "http",
				Authority: "127.0.0.1:1",
				Path:      "/first",
			},
		},
		{
			Type:         model.EventRequest,
			ConnectionID: 1,
			Sequence:     2,
			HTTP: model.HTTPRequestMeta{
				Method:    http.MethodGet,
				Scheme:    "http",
				Authority: srv.Listener.Addr().String(),
				Path:      "/second",
			},
		},
		{Type: model.EventConnectionClose, ConnectionID: 1},
	}

	summary, err := runReplay(eng, events)
	if err != nil {
		t.Fatalf("replay failed: %v", err)
	}
	if summary.SendErrors != 1 {
		t.Fatalf("summary.SendErrors = %d, want 1", summary.SendErrors)
	}
	if summary.ConnectionsAborted != 1 {
		t.Fatalf("summary.ConnectionsAborted = %d, want 1", summary.ConnectionsAborted)
	}
	if summary.RequestsSent != 0 {
		t.Fatalf("summary.RequestsSent = %d, want 0", summary.RequestsSent)
	}
	if atomic.LoadInt64(&attempts) != 0 {
		t.Fatalf("second request attempt count = %d, want 0", attempts)
	}
}
