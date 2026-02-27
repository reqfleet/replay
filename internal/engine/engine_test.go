package engine

import (
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/reqfleet/replay/internal/config"
	"github.com/reqfleet/replay/internal/metrics"
	"github.com/reqfleet/replay/internal/model"
)

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
		{Type: model.EventConnectionOpen, ConnectionID: "c1"},
		{
			Type:         model.EventRequest,
			ConnectionID: "c1",
			Sequence:     1,
			HTTP: model.HTTPRequestMeta{
				Method:    http.MethodGet,
				Scheme:    target.Scheme,
				Authority: target.Host,
				Path:      "/",
			},
		},
		{Type: model.EventConnectionClose, ConnectionID: "c1"},
	}

	summary, err := eng.Replay(context.Background(), events)
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
		{Type: model.EventConnectionOpen, ConnectionID: "c1"},
		{
			Type:         model.EventRequest,
			ConnectionID: "c1",
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
			ConnectionID: "c1",
			Sequence:     1,
			Status:       http.StatusOK,
		},
		{Type: model.EventConnectionClose, ConnectionID: "c1"},
	}

	summary, err := eng.Replay(context.Background(), events)
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
		{Type: model.EventConnectionOpen, ConnectionID: "c1"},
		{
			Type:         model.EventRequest,
			ConnectionID: "c1",
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
			ConnectionID: "c1",
			Sequence:     1,
			Status:       http.StatusOK,
			Headers: map[string][]string{
				"content-type": {"application/json"},
				"x-request-id": {"expected-other-id"},
			},
		},
		{Type: model.EventConnectionClose, ConnectionID: "c1"},
	}

	summary, err := eng.Replay(context.Background(), events)
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
		{Type: model.EventConnectionOpen, ConnectionID: "c1"},
		{
			Type:         model.EventRequest,
			ConnectionID: "c1",
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
			ConnectionID: "c1",
			Sequence:     1,
			Status:       http.StatusOK,
			Body: model.Body{
				Encoding: "base64",
				Content:  base64.StdEncoding.EncodeToString([]byte("expected-body")),
			},
		},
		{Type: model.EventConnectionClose, ConnectionID: "c1"},
	}

	summary, err := eng.Replay(context.Background(), events)
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
		{Type: model.EventConnectionOpen, ConnectionID: "c1"},
		{
			Type:         model.EventRequest,
			ConnectionID: "c1",
			Sequence:     1,
			HTTP: model.HTTPRequestMeta{
				Method:    http.MethodPost,
				Scheme:    target.Scheme,
				Authority: target.Host,
				Path:      "/mutate",
			},
		},
		{Type: model.EventConnectionClose, ConnectionID: "c1"},
	}

	summary, err := eng.Replay(context.Background(), events)
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

func TestReplayFailsWhenLifecycleCloseMissing(t *testing.T) {
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
		{Type: model.EventConnectionOpen, ConnectionID: "c1"},
		{
			Type:         model.EventRequest,
			ConnectionID: "c1",
			Sequence:     1,
			HTTP: model.HTTPRequestMeta{
				Method:    http.MethodGet,
				Scheme:    target.Scheme,
				Authority: target.Host,
				Path:      "/",
			},
		},
	}

	_, err = eng.Replay(context.Background(), events)
	if err == nil {
		t.Fatal("expected lifecycle validation error")
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

	connections := []string{"c1", "c2", "c3", "c4"}
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
		if connectionBelongsToShard(conn, cfg.Replay.Sharding.ShardIndex, cfg.Replay.Sharding.ShardCount) {
			expectedSent++
		}
	}

	eng := New(cfg, metrics.New())
	summary, err := eng.Replay(context.Background(), events)
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
	checkpointPayload := `{"version":1,"connections":{"c1":1}}`
	if err := os.WriteFile(checkpointPath, []byte(checkpointPayload), 0o644); err != nil {
		t.Fatalf("write checkpoint failed: %v", err)
	}

	cfg := config.Default()
	cfg.Replay.Checkpoint.File = checkpointPath

	eng := New(cfg, metrics.New())
	events := []model.Event{
		{Type: model.EventMeta},
		{Type: model.EventConnectionOpen, ConnectionID: "c1"},
		{
			Type:         model.EventRequest,
			ConnectionID: "c1",
			Sequence:     1,
			HTTP: model.HTTPRequestMeta{
				Method:    http.MethodGet,
				Scheme:    target.Scheme,
				Authority: target.Host,
				Path:      "/",
			},
		},
		{Type: model.EventConnectionClose, ConnectionID: "c1"},
	}

	summary, err := eng.Replay(context.Background(), events)
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
	var inFlight int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		current := atomic.AddInt64(&inFlight, 1)
		for {
			previous := atomic.LoadInt64(&maxInFlight)
			if current <= previous || atomic.CompareAndSwapInt64(&maxInFlight, previous, current) {
				break
			}
		}
		time.Sleep(80 * time.Millisecond)
		atomic.AddInt64(&inFlight, -1)
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
		{Type: model.EventConnectionOpen, ConnectionID: "c1"},
		{Type: model.EventRequest, ConnectionID: "c1", StreamID: 1, Sequence: 1, HTTP: model.HTTPRequestMeta{Version: "HTTP/2", Method: http.MethodGet, Scheme: target.Scheme, Authority: target.Host, Path: "/a"}},
		{Type: model.EventRequest, ConnectionID: "c1", StreamID: 3, Sequence: 2, HTTP: model.HTTPRequestMeta{Version: "HTTP/2", Method: http.MethodGet, Scheme: target.Scheme, Authority: target.Host, Path: "/b"}},
		{Type: model.EventConnectionClose, ConnectionID: "c1"},
	}

	_, err = eng.Replay(context.Background(), events)
	if err != nil {
		t.Fatalf("replay failed: %v", err)
	}
	if atomic.LoadInt64(&maxInFlight) != 1 {
		t.Fatalf("max in-flight = %d, want 1 for serialized mode", maxInFlight)
	}
}

func TestReplayHTTP2MultiplexedMode(t *testing.T) {
	var maxInFlight int64
	var inFlight int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		current := atomic.AddInt64(&inFlight, 1)
		for {
			previous := atomic.LoadInt64(&maxInFlight)
			if current <= previous || atomic.CompareAndSwapInt64(&maxInFlight, previous, current) {
				break
			}
		}
		time.Sleep(80 * time.Millisecond)
		atomic.AddInt64(&inFlight, -1)
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
		{Type: model.EventConnectionOpen, ConnectionID: "c1"},
		{Type: model.EventRequest, ConnectionID: "c1", StreamID: 1, Sequence: 1, HTTP: model.HTTPRequestMeta{Version: "HTTP/2", Method: http.MethodGet, Scheme: target.Scheme, Authority: target.Host, Path: "/a"}},
		{Type: model.EventRequest, ConnectionID: "c1", StreamID: 3, Sequence: 2, HTTP: model.HTTPRequestMeta{Version: "HTTP/2", Method: http.MethodGet, Scheme: target.Scheme, Authority: target.Host, Path: "/b"}},
		{Type: model.EventConnectionClose, ConnectionID: "c1"},
	}

	_, err = eng.Replay(context.Background(), events)
	if err != nil {
		t.Fatalf("replay failed: %v", err)
	}
	if atomic.LoadInt64(&maxInFlight) < 2 {
		t.Fatalf("max in-flight = %d, want >= 2 for multiplexed mode", maxInFlight)
	}
}
