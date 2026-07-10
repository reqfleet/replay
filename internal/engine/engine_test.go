package engine

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
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

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
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

func startRawHTTPResponseServer(t *testing.T, response string) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen(tcp, 127.0.0.1:0) error: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
			t.Errorf("conn.SetReadDeadline(%v) error: %v", ln.Addr(), err)
			return
		}
		req, err := http.ReadRequest(bufio.NewReader(conn))
		if err != nil {
			t.Errorf("http.ReadRequest(%v) error: %v", ln.Addr(), err)
			return
		}
		_ = req.Body.Close()
		if err := conn.SetReadDeadline(time.Time{}); err != nil {
			t.Errorf("conn.SetReadDeadline(%v, zero) error: %v", ln.Addr(), err)
			return
		}

		_, _ = conn.Write([]byte(response))
	}()

	return ln.Addr().String()
}

func latencySampleCount(t *testing.T, reg *metrics.Registry, commonLabelValues []string, label string) uint64 {
	t.Helper()
	observer, err := reg.LabelLatencyHistogram.GetMetricWithLabelValues(append(commonLabelValues, label)...)
	if err != nil {
		t.Fatalf("LabelLatencyHistogram.GetMetricWithLabelValues(%q) error: %v", label, err)
	}
	histogram, ok := observer.(prometheus.Metric)
	if !ok {
		t.Fatalf("LabelLatencyHistogram.GetMetricWithLabelValues(%q) = %T, want prometheus.Metric", label, observer)
	}
	metric := &dto.Metric{}
	if err := histogram.Write(metric); err != nil {
		t.Fatalf("histogram.Write(%q) error: %v", label, err)
	}
	return metric.GetHistogram().GetSampleCount()
}

func TestResponseHeaderBytes(t *testing.T) {
	headers := http.Header{
		"X-Multi": {"one", "two"},
		"X-Test":  {"abc"},
	}
	want := int64(len("X-Multi: one\r\n") + len("X-Multi: two\r\n") + len("X-Test: abc\r\n") + len("\r\n"))

	if got := responseHeaderBytes(headers); got != want {
		t.Errorf("responseHeaderBytes(%v) = %d, want %d", headers, got, want)
	}
}

func TestExecuteRequestIncludesResponseHeadersInEgressBytes(t *testing.T) {
	const response = "HTTP/1.1 200 OK\r\nX-Multi: one\r\nX-Multi: two\r\nX-Test: abc\r\n\r\nok"
	authority := startRawHTTPResponseServer(t, response)
	cfg := config.Default()
	eng := New(cfg, metrics.New(cfg.Metrics))
	client, transport := eng.makePerConnectionClient(false)
	defer transport.CloseIdleConnections()

	exec, err := eng.executeRequest(context.Background(), client, model.Event{
		HTTP: model.HTTPRequestMeta{
			Method:    http.MethodGet,
			Scheme:    "http",
			Authority: authority,
			Path:      "/",
		},
	})
	if err != nil {
		t.Fatalf("executeRequest(raw response) error: %v", err)
	}

	want := int64(len("ok") + len("X-Multi: one\r\n") + len("X-Multi: two\r\n") + len("X-Test: abc\r\n") + len("\r\n"))
	if exec.egressBytes != want {
		t.Errorf("executeRequest(raw response).egressBytes = %d, want %d", exec.egressBytes, want)
	}
}

func TestExecuteRequestIncludesPartialBodyInEgressBytesOnReadError(t *testing.T) {
	const response = "HTTP/1.1 200 OK\r\nContent-Length: 5\r\nX-Test: abc\r\n\r\nabc"
	authority := startRawHTTPResponseServer(t, response)
	cfg := config.Default()
	eng := New(cfg, metrics.New(cfg.Metrics))
	client, transport := eng.makePerConnectionClient(false)
	defer transport.CloseIdleConnections()

	exec, err := eng.executeRequest(context.Background(), client, model.Event{
		HTTP: model.HTTPRequestMeta{
			Method:    http.MethodGet,
			Scheme:    "http",
			Authority: authority,
			Path:      "/",
		},
	})
	if err == nil {
		t.Fatal("executeRequest(partial body response) error = nil, want read error")
	}

	want := int64(len("abc") + len("Content-Length: 5\r\n") + len("X-Test: abc\r\n") + len("\r\n"))
	if exec.egressBytes != want {
		t.Errorf("executeRequest(partial body response).egressBytes = %d, want %d", exec.egressBytes, want)
	}
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

	reg := metrics.New(cfg.Metrics)
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
	commonLabelValues := cfg.Metrics.CommonLabelValues()
	status503 := reg.StatusCounter.WithLabelValues(append(commonLabelValues, "/", "503")...)
	if got, want := testutil.ToFloat64(status503), float64(1); got != want {
		t.Fatalf("503 status counter = %v, want %v", got, want)
	}
	status200 := reg.StatusCounter.WithLabelValues(append(commonLabelValues, "/", "200")...)
	if got, want := testutil.ToFloat64(status200), float64(1); got != want {
		t.Fatalf("200 status counter = %v, want %v", got, want)
	}
	if got, want := latencySampleCount(t, reg, commonLabelValues, "/"), uint64(2); got != want {
		t.Fatalf("latency sample count = %d, want %d", got, want)
	}
}

func TestSendRequestReturnsAttemptedExecutionWhenRetryBackoffCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cfg := config.Default()
	cfg.Replay.Retry.MaxAttempts = 2
	cfg.Replay.Retry.Backoff = "fixed"
	cfg.Replay.Retry.RetryOnStatuses = []int{http.StatusServiceUnavailable}

	reg := metrics.New(cfg.Metrics)
	eng := New(cfg, reg)
	client := &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			cancel()
			return &http.Response{
				StatusCode: http.StatusServiceUnavailable,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader("retry")),
				Request:    req,
			}, nil
		}),
	}

	exec, err := eng.sendRequest(ctx, client, model.Event{
		HTTP: model.HTTPRequestMeta{
			Method:    http.MethodGet,
			Scheme:    "http",
			Authority: "example.test",
			Path:      "/",
		},
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("sendRequest(canceled retry backoff) error = %v, want %v", err, context.Canceled)
	}
	if !exec.attempted {
		t.Fatal("sendRequest(canceled retry backoff).attempted = false, want true")
	}
	if got, want := exec.statusCode, http.StatusServiceUnavailable; got != want {
		t.Errorf("sendRequest(canceled retry backoff).statusCode = %d, want %d", got, want)
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

	eng := New(cfg, metrics.New(cfg.Metrics))
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

func TestDownstreamStartRequestSkipsInlineResponseValidation(t *testing.T) {
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
	eng := New(cfg, metrics.New(cfg.Metrics))
	events := []model.Event{
		{Type: model.EventMeta},
		{Type: model.EventConnectionOpen, ConnectionID: 1},
		{
			Type:          model.EventRequest,
			AccessLogType: model.AccessLogTypeDownstreamStart,
			ConnectionID:  1,
			Sequence:      1,
			Status:        http.StatusOK,
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
	if got, want := summary.ValidationFailed, int64(0); got != want {
		t.Fatalf("summary.ValidationFailed = %d, want %d", got, want)
	}
	if got, want := summary.Outcome, RunSuccess; got != want {
		t.Fatalf("summary.Outcome = %s, want %s", got, want)
	}
}

func TestDownstreamEndRequestValidatesInlineResponseStatus(t *testing.T) {
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
	eng := New(cfg, metrics.New(cfg.Metrics))
	events := []model.Event{
		{Type: model.EventMeta},
		{Type: model.EventConnectionOpen, ConnectionID: 1},
		{
			Type:          model.EventRequest,
			AccessLogType: model.AccessLogTypeDownstreamEnd,
			ConnectionID:  1,
			Sequence:      1,
			Status:        http.StatusOK,
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
	if got, want := summary.ValidationFailed, int64(1); got != want {
		t.Fatalf("summary.ValidationFailed = %d, want %d", got, want)
	}
	if got, want := summary.Outcome, RunPartialSuccess; got != want {
		t.Fatalf("summary.Outcome = %s, want %s", got, want)
	}
}

func TestDownstreamEndRequestPreservesLaterResponseValidation(t *testing.T) {
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
	cfg.Replay.Validation.Enabled = true
	cfg.Replay.Validation.Status = true
	eng := New(cfg, metrics.New(cfg.Metrics))
	events := []model.Event{
		{Type: model.EventMeta},
		{Type: model.EventConnectionOpen, ConnectionID: 1},
		{
			Type:          model.EventRequest,
			AccessLogType: model.AccessLogTypeDownstreamEnd,
			ConnectionID:  1,
			Sequence:      1,
			Status:        http.StatusOK,
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
			Status:       http.StatusInternalServerError,
		},
		{Type: model.EventConnectionClose, ConnectionID: 1},
	}

	summary, err := runReplay(eng, events)
	if err != nil {
		t.Fatalf("replay failed: %v", err)
	}
	if got, want := summary.ValidationFailed, int64(1); got != want {
		t.Fatalf("summary.ValidationFailed = %d, want %d", got, want)
	}
	if got, want := summary.Outcome, RunPartialSuccess; got != want {
		t.Fatalf("summary.Outcome = %s, want %s", got, want)
	}
}

func TestReplayMarksUnmatchedExpectedResponseAsValidationFailed(t *testing.T) {
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
	cfg.Replay.Validation.Enabled = true
	cfg.Replay.Validation.Status = true
	eng := New(cfg, metrics.New(cfg.Metrics))
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
			Sequence:     2,
			Status:       http.StatusOK,
		},
		{Type: model.EventConnectionClose, ConnectionID: 1},
	}

	summary, err := runReplay(eng, events)
	if err != nil {
		t.Fatalf("replay failed: %v", err)
	}
	if got, want := summary.ValidationFailed, int64(1); got != want {
		t.Fatalf("summary.ValidationFailed = %d, want %d", got, want)
	}
}

func TestReplaySkipsValidationForSkippedRequestResponse(t *testing.T) {
	cfg := config.Default()
	cfg.Replay.DryRun = true
	cfg.Replay.Validation.Enabled = true
	cfg.Replay.Validation.Status = true
	eng := New(cfg, metrics.New(cfg.Metrics))
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
				Authority: "example.invalid",
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
	if got, want := summary.Skipped, int64(1); got != want {
		t.Fatalf("summary.Skipped = %d, want %d", got, want)
	}
	if got, want := summary.ValidationFailed, int64(0); got != want {
		t.Fatalf("summary.ValidationFailed = %d, want %d", got, want)
	}
}

func TestReplayTreatsConnectionRefusedAsPartialSuccess(t *testing.T) {
	addr := closedLocalAddress(t)

	cfg := config.Default()
	eng := New(cfg, metrics.New(cfg.Metrics))
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
			reg := metrics.New(cfg.Metrics)
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
				append(cfg.Metrics.CommonLabelValues(), "/transport", tt.wantStatus)...,
			)
			if got, want := testutil.ToFloat64(counter), float64(1); got != want {
				t.Fatalf("status counter for %s = %v, want %v", tt.wantStatus, got, want)
			}
			if got, want := latencySampleCount(t, reg, cfg.Metrics.CommonLabelValues(), "/transport"), uint64(1); got != want {
				t.Fatalf("latency sample count for %s = %d, want %d", tt.wantStatus, got, want)
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

	eng := New(cfg, metrics.New(cfg.Metrics))
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

	eng := New(cfg, metrics.New(cfg.Metrics))
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

func TestFinishRequestSuccessDoesNotRetainDetailsWhenValidationDisabled(t *testing.T) {
	cfg := config.Default()
	cfg.Replay.Validation.Enabled = false
	eng := New(cfg, metrics.New(cfg.Metrics))
	cs := eng.newConnState(model.ConnectionKey{ConnectionID: 1})
	req := model.Event{Type: model.EventRequest, ConnectionID: 1, Sequence: 1}

	abort := eng.finishRequestSuccess(cs, req, requestExecution{statusCode: http.StatusOK, body: []byte("actual")}, nil)
	if abort {
		t.Fatal("finishRequestSuccess aborted unexpectedly")
	}
	if len(cs.pendingActual) != 0 {
		t.Fatalf("pendingActual len = %d, want 0", len(cs.pendingActual))
	}
	if got, want := cs.sent, int64(1); got != want {
		t.Fatalf("cs.sent = %d, want %d", got, want)
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
	eng := New(cfg, metrics.New(cfg.Metrics))
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

func TestReplayIdempotencyPolicyUsesRewrittenHeaders(t *testing.T) {
	t.Run("dropped allow header blocks mutation", func(t *testing.T) {
		var attempts atomic.Int64
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			attempts.Add(1)
			w.WriteHeader(http.StatusOK)
		}))
		defer srv.Close()

		target, err := url.Parse(srv.URL)
		if err != nil {
			t.Fatalf("url.Parse(%q) error: %v", srv.URL, err)
		}

		cfg := config.Default()
		cfg.Header.Drop = []string{"x-idempotency-key"}
		eng := New(cfg, metrics.New(cfg.Metrics))
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
				Headers: map[string][]string{"x-idempotency-key": {"recorded-key"}},
			},
			{Type: model.EventConnectionClose, ConnectionID: 1},
		}

		summary, err := runReplay(eng, events)
		if err != nil {
			t.Fatalf("runReplay() error: %v", err)
		}
		if got, want := summary.Skipped, int64(1); got != want {
			t.Fatalf("summary.Skipped = %d, want %d", got, want)
		}
		if got := attempts.Load(); got != 0 {
			t.Fatalf("request attempts = %d, want 0", got)
		}
	})

	t.Run("set allow header permits mutation", func(t *testing.T) {
		var seenKey string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			seenKey = r.Header.Get("x-idempotency-key")
			w.WriteHeader(http.StatusOK)
		}))
		defer srv.Close()

		target, err := url.Parse(srv.URL)
		if err != nil {
			t.Fatalf("url.Parse(%q) error: %v", srv.URL, err)
		}

		cfg := config.Default()
		cfg.Header.Set["x-idempotency-key"] = "replacement-key"
		eng := New(cfg, metrics.New(cfg.Metrics))
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
			t.Fatalf("runReplay() error: %v", err)
		}
		if got, want := summary.RequestsSent, int64(1); got != want {
			t.Fatalf("summary.RequestsSent = %d, want %d", got, want)
		}
		if got, want := seenKey, "replacement-key"; got != want {
			t.Fatalf("idempotency key = %q, want %q", got, want)
		}
	})

	t.Run("override host permits mutation after recorded host is dropped", func(t *testing.T) {
		var seenHost string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			seenHost = r.Host
			w.WriteHeader(http.StatusOK)
		}))
		defer srv.Close()

		target, err := url.Parse(srv.URL)
		if err != nil {
			t.Fatalf("url.Parse(%q) error: %v", srv.URL, err)
		}

		cfg := config.Default()
		cfg.Target.OverrideURL = srv.URL
		cfg.Header.Drop = []string{"Host"}
		cfg.Replay.Idempotency.RequireHeaderForAllow = []string{"Host"}
		eng := New(cfg, metrics.New(cfg.Metrics))
		events := []model.Event{
			{Type: model.EventMeta},
			{Type: model.EventConnectionOpen, ConnectionID: 1},
			{
				Type:         model.EventRequest,
				ConnectionID: 1,
				Sequence:     1,
				HTTP: model.HTTPRequestMeta{
					Method:    http.MethodPost,
					Scheme:    "https",
					Authority: "captured.example",
					Path:      "/mutate",
				},
				Headers: map[string][]string{"Host": {"captured.example"}},
			},
			{Type: model.EventConnectionClose, ConnectionID: 1},
		}

		summary, err := runReplay(eng, events)
		if err != nil {
			t.Fatalf("runReplay() error: %v", err)
		}
		if got, want := summary.RequestsSent, int64(1); got != want {
			t.Errorf("summary.RequestsSent = %d, want %d", got, want)
		}
		if got, want := summary.Skipped, int64(0); got != want {
			t.Errorf("summary.Skipped = %d, want %d", got, want)
		}
		if got, want := seenHost, target.Host; got != want {
			t.Errorf("request Host = %q, want %q", got, want)
		}
	})
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
	eng := New(cfg, metrics.New(cfg.Metrics))
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

	eng := New(cfg, metrics.New(cfg.Metrics))
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

func TestRouteEventsSkipsNonShardEventsBeforeLifecycleTracking(t *testing.T) {
	cfg := config.Default()
	cfg.Replay.Sharding.ShardCount = 2
	cfg.Replay.Sharding.ShardIndex = 0

	connKey := model.ConnectionKey{}
	for connectionID := 1; connectionID <= 128; connectionID++ {
		candidate := model.ConnectionKey{ConnectionID: connectionID}
		if !connectionBelongsToShard(candidate, cfg.Replay.Sharding.ShardIndex, cfg.Replay.Sharding.ShardCount) {
			connKey = candidate
			break
		}
	}
	if connKey == (model.ConnectionKey{}) {
		t.Fatal("setup failed: could not find connection outside shard")
	}

	eng := New(cfg, metrics.New(cfg.Metrics))
	events := make(chan model.Event, 1)
	events <- model.Event{
		Type:         model.EventRequest,
		ConnectionID: connKey.ConnectionID,
		Sequence:     1,
		HTTP: model.HTTPRequestMeta{
			Method:    http.MethodGet,
			Scheme:    "http",
			Authority: "example.test",
			Path:      "/",
		},
	}
	close(events)

	workerChs := []chan model.Event{make(chan model.Event, 1), make(chan model.Event, 1)}
	if err := eng.routeEvents(context.Background(), events, workerChs); err != nil {
		t.Fatalf("routeEvents(non-shard request without open) error = %v, want nil", err)
	}
	if got := len(workerChs[0]) + len(workerChs[1]); got != 0 {
		t.Fatalf("routeEvents(non-shard request) routed %d events, want 0", got)
	}
}

func TestRouteEventsSendsCloseToOwningWorker(t *testing.T) {
	cfg := config.Default()
	cfg.Replay.MaxVirtualUsersPerEngine = 2
	eng := New(cfg, metrics.New(cfg.Metrics))

	events := make(chan model.Event, 3)
	events <- model.Event{Type: model.EventConnectionOpen, ConnectionID: 1}
	events <- model.Event{Type: model.EventRequest, ConnectionID: 1, Sequence: 1, HTTP: model.HTTPRequestMeta{Method: http.MethodGet, Scheme: "http", Authority: "example.test", Path: "/"}}
	events <- model.Event{Type: model.EventConnectionClose, ConnectionID: 1}
	close(events)

	workerChs := []chan model.Event{make(chan model.Event, 3), make(chan model.Event, 3)}
	if err := eng.routeEvents(context.Background(), events, workerChs); err != nil {
		t.Fatalf("routeEvents() error = %v", err)
	}

	if got := len(workerChs[0]); got != 3 {
		t.Fatalf("worker 0 received %d events, want 3", got)
	}
	if got := len(workerChs[1]); got != 0 {
		t.Fatalf("worker 1 received %d events, want 0", got)
	}

	<-workerChs[0]
	<-workerChs[0]
	closeEvent := <-workerChs[0]
	if closeEvent.Type != model.EventConnectionClose {
		t.Fatalf("third worker-0 event = %s, want %s", closeEvent.Type, model.EventConnectionClose)
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

	eng := New(cfg, metrics.New(cfg.Metrics))
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

	eng := New(cfg, metrics.New(cfg.Metrics))
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

func TestReplayConnectionCapacityDoesNotBlockCloseEvents(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	target, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("url.Parse(%q) error: %v", srv.URL, err)
	}

	cfg := config.Default()
	cfg.Replay.MaxVirtualUsersPerEngine = 1
	cfg.Replay.MaxActiveConnectionsPerEngine = 1
	cfg.Replay.Idempotency.Enabled = false
	eng := New(cfg, metrics.New(cfg.Metrics))
	events := []model.Event{
		{Type: model.EventMeta},
		{Type: model.EventConnectionOpen, ConnectionID: 1},
		{Type: model.EventRequest, ConnectionID: 1, Sequence: 1, HTTP: model.HTTPRequestMeta{Method: http.MethodGet, Scheme: target.Scheme, Authority: target.Host, Path: "/one"}},
		{Type: model.EventConnectionOpen, ConnectionID: 2},
		{Type: model.EventRequest, ConnectionID: 2, Sequence: 1, HTTP: model.HTTPRequestMeta{Method: http.MethodGet, Scheme: target.Scheme, Authority: target.Host, Path: "/two"}},
		{Type: model.EventConnectionClose, ConnectionID: 1},
		{Type: model.EventConnectionClose, ConnectionID: 2},
	}

	select {
	case result := <-runReplayAsync(eng, events):
		if result.err != nil {
			t.Fatalf("runReplay() error: %v", result.err)
		}
		if got, want := result.summary.RequestsSent, int64(2); got != want {
			t.Fatalf("summary.RequestsSent = %d, want %d", got, want)
		}
	case <-time.After(time.Second):
		t.Fatal("replay deadlocked while waiting for connection capacity")
	}
}

func TestReplayConnectionCapacityLimitsConcurrentRequests(t *testing.T) {
	started := make(chan string, 2)
	releaseFirst := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseFirst) }) }
	t.Cleanup(release)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started <- r.URL.Path
		if r.URL.Path == "/one" {
			<-releaseFirst
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	target, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("url.Parse(%q) error: %v", srv.URL, err)
	}

	cfg := config.Default()
	cfg.Replay.MaxVirtualUsersPerEngine = 2
	cfg.Replay.MaxActiveConnectionsPerEngine = 1
	cfg.Replay.Idempotency.Enabled = false
	eng := New(cfg, metrics.New(cfg.Metrics))
	events := make(chan model.Event)
	resultCh := make(chan replayResult, 1)
	go func() {
		summary, replayErr := eng.ReplayStream(context.Background(), events)
		resultCh <- replayResult{summary: summary, err: replayErr}
	}()

	events <- model.Event{Type: model.EventMeta}
	events <- model.Event{Type: model.EventConnectionOpen, ConnectionID: 1}
	events <- model.Event{Type: model.EventRequest, ConnectionID: 1, Sequence: 1, HTTP: model.HTTPRequestMeta{Method: http.MethodGet, Scheme: target.Scheme, Authority: target.Host, Path: "/one"}}
	if got := <-started; got != "/one" {
		t.Fatalf("first request path = %q, want %q", got, "/one")
	}

	events <- model.Event{Type: model.EventConnectionOpen, ConnectionID: 2}
	events <- model.Event{Type: model.EventRequest, ConnectionID: 2, Sequence: 1, HTTP: model.HTTPRequestMeta{Method: http.MethodGet, Scheme: target.Scheme, Authority: target.Host, Path: "/two"}}
	events <- model.Event{Type: model.EventConnectionClose, ConnectionID: 1}
	events <- model.Event{Type: model.EventConnectionClose, ConnectionID: 2}
	close(events)

	select {
	case got := <-started:
		t.Fatalf("request %q started while the only connection lease was busy", got)
	case <-time.After(100 * time.Millisecond):
	}

	release()
	select {
	case got := <-started:
		if got != "/two" {
			t.Fatalf("second request path = %q, want %q", got, "/two")
		}
	case <-time.After(time.Second):
		t.Fatal("second request did not start after capacity became idle")
	}

	select {
	case result := <-resultCh:
		if result.err != nil {
			t.Fatalf("ReplayStream() error: %v", result.err)
		}
		if got, want := result.summary.RequestsSent, int64(2); got != want {
			t.Fatalf("summary.RequestsSent = %d, want %d", got, want)
		}
	case <-time.After(time.Second):
		t.Fatal("ReplayStream() did not finish after capacity handoff")
	}
}

func TestReplayConnectionCapacityRetainsKeepAliveWithoutPressure(t *testing.T) {
	var mu sync.Mutex
	remoteAddresses := make(map[string]struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		remoteAddresses[r.RemoteAddr] = struct{}{}
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	target, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("url.Parse(%q) error: %v", srv.URL, err)
	}

	cfg := config.Default()
	cfg.Replay.MaxActiveConnectionsPerEngine = 1
	cfg.Replay.Idempotency.Enabled = false
	eng := New(cfg, metrics.New(cfg.Metrics))
	events := []model.Event{
		{Type: model.EventMeta},
		{Type: model.EventConnectionOpen, ConnectionID: 1},
		{Type: model.EventRequest, ConnectionID: 1, Sequence: 1, HTTP: model.HTTPRequestMeta{Method: http.MethodGet, Scheme: target.Scheme, Authority: target.Host, Path: "/one"}},
		{Type: model.EventRequest, ConnectionID: 1, Sequence: 2, HTTP: model.HTTPRequestMeta{Method: http.MethodGet, Scheme: target.Scheme, Authority: target.Host, Path: "/two"}},
		{Type: model.EventConnectionClose, ConnectionID: 1},
	}

	summary, err := runReplay(eng, events)
	if err != nil {
		t.Fatalf("runReplay() error: %v", err)
	}
	if got, want := summary.RequestsSent, int64(2); got != want {
		t.Fatalf("summary.RequestsSent = %d, want %d", got, want)
	}
	mu.Lock()
	defer mu.Unlock()
	if got, want := len(remoteAddresses), 1; got != want {
		t.Fatalf("unique remote addresses = %d, want %d", got, want)
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

	eng := New(cfg, metrics.New(cfg.Metrics))
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

	eng := New(cfg, metrics.New(cfg.Metrics))
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

	eng := New(cfg, metrics.New(cfg.Metrics))
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
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		negotiatedProto := ""
		if r.TLS != nil {
			negotiatedProto = r.TLS.NegotiatedProtocol
		}
		t.Logf("Test server received request: %s Proto: %s NegotiatedProtocol: %q", r.URL.Path, r.Proto, negotiatedProto)
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
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()

	target, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("url parse failed: %v", err)
	}

	cfg := config.Default()
	cfg.Replay.HTTP2.Mode = "multiplexed"
	cfg.Replay.TLS.InsecureSkipVerify = true
	cfg.Replay.Idempotency.Enabled = false

	eng := New(cfg, metrics.New(cfg.Metrics))
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
	// With InsecureSkipVerify set to true and a TLS server, true HTTP/2 multiplexing
	// is enabled. Both requests will be processed concurrently over a single connection,
	// so the maximum concurrent in-flight requests should be exactly 2.
	if got := atomic.LoadInt64(&maxInFlight); got != 2 {
		t.Fatalf("max in-flight = %d, want exactly 2 for HTTP/2 multiplexed mode", got)
	}
}

func TestReplayHTTP2CheckpointWaitsForEarlierInFlightRequest(t *testing.T) {
	slowStarted := make(chan struct{}, 1)
	fastDone := make(chan struct{}, 1)
	releaseSlow := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseSlow) }) }
	t.Cleanup(release)

	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/slow":
			select {
			case slowStarted <- struct{}{}:
			default:
			}
			<-releaseSlow
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("slow"))
		case "/fast":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("fast"))
			select {
			case fastDone <- struct{}{}:
			default:
			}
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()

	target, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("url parse failed: %v", err)
	}

	tmpDir := t.TempDir()
	checkpointPath := filepath.Join(tmpDir, "checkpoint.json")
	cfg := config.Default()
	cfg.Replay.HTTP2.Mode = "multiplexed"
	cfg.Replay.TLS.InsecureSkipVerify = true
	cfg.Replay.Idempotency.Enabled = false
	cfg.Replay.Checkpoint.File = checkpointPath

	eng := New(cfg, metrics.New(cfg.Metrics))
	events := []model.Event{
		{Type: model.EventMeta},
		{Type: model.EventConnectionOpen, ConnectionID: 1},
		{Type: model.EventRequest, ConnectionID: 1, StreamID: 1, Sequence: 1, HTTP: model.HTTPRequestMeta{Version: "HTTP/2", Method: http.MethodGet, Scheme: target.Scheme, Authority: target.Host, Path: "/slow"}},
		{Type: model.EventRequest, ConnectionID: 1, StreamID: 3, Sequence: 2, HTTP: model.HTTPRequestMeta{Version: "HTTP/2", Method: http.MethodGet, Scheme: target.Scheme, Authority: target.Host, Path: "/fast"}},
		{Type: model.EventConnectionClose, ConnectionID: 1},
	}

	resultCh := runReplayAsync(eng, events)
	select {
	case <-slowStarted:
	case <-time.After(time.Second):
		t.Fatal("slow request did not start")
	}
	select {
	case <-fastDone:
	case <-time.After(time.Second):
		t.Fatal("fast request did not finish")
	}

	if b, readErr := os.ReadFile(checkpointPath); readErr == nil {
		var data struct {
			Connections map[model.ConnectionKey]int `json:"connections"`
		}
		if err := json.Unmarshal(b, &data); err != nil {
			t.Fatalf("unmarshal checkpoint before slow release: %v", err)
		}
		if got := data.Connections[model.ConnectionKey{ConnectionID: 1}]; got >= 2 {
			t.Fatalf("checkpoint advanced to %d while sequence 1 was still in flight", got)
		}
	} else if !os.IsNotExist(readErr) {
		t.Fatalf("read checkpoint before slow release: %v", readErr)
	}

	release()
	result := <-resultCh
	if result.err != nil {
		t.Fatalf("replay failed: %v", result.err)
	}
	if got, want := result.summary.RequestsSent, int64(2); got != want {
		t.Fatalf("summary.RequestsSent = %d, want %d", got, want)
	}

	b, err := os.ReadFile(checkpointPath)
	if err != nil {
		t.Fatalf("read checkpoint after replay: %v", err)
	}
	var data struct {
		Connections map[model.ConnectionKey]int `json:"connections"`
	}
	if err := json.Unmarshal(b, &data); err != nil {
		t.Fatalf("unmarshal checkpoint after replay: %v", err)
	}
	if got := data.Connections[model.ConnectionKey{ConnectionID: 1}]; got != 2 {
		t.Fatalf("checkpoint sequence = %d, want 2", got)
	}
}

func TestReplayHTTP2PacingUsesConnectionOrderWithUniqueStreamIDs(t *testing.T) {
	type startRecord struct {
		path string
		at   time.Time
	}
	starts := make(chan startRecord, 2)
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		starts <- startRecord{path: r.URL.Path, at: time.Now()}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()

	target, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("url parse failed: %v", err)
	}

	base := time.Date(2026, 2, 27, 3, 10, 21, 0, time.UTC)
	cfg := config.Default()
	cfg.Replay.HTTP2.Mode = "multiplexed"
	cfg.Replay.TLS.InsecureSkipVerify = true
	cfg.Replay.Idempotency.Enabled = false
	cfg.Replay.Pacing.Enabled = true
	cfg.Replay.Pacing.MaxSleepDelta = 200 * time.Millisecond

	eng := New(cfg, metrics.New(cfg.Metrics))
	events := []model.Event{
		{Type: model.EventMeta},
		{Type: model.EventConnectionOpen, ConnectionID: 1},
		{Type: model.EventRequest, ConnectionID: 1, StreamID: 1, Sequence: 1, Timestamp: base.Format(time.RFC3339Nano), HTTP: model.HTTPRequestMeta{Version: "HTTP/2", Method: http.MethodGet, Scheme: target.Scheme, Authority: target.Host, Path: "/a"}},
		{Type: model.EventRequest, ConnectionID: 1, StreamID: 3, Sequence: 2, Timestamp: base.Add(100 * time.Millisecond).Format(time.RFC3339Nano), HTTP: model.HTTPRequestMeta{Version: "HTTP/2", Method: http.MethodGet, Scheme: target.Scheme, Authority: target.Host, Path: "/b"}},
		{Type: model.EventConnectionClose, ConnectionID: 1},
	}

	summary, err := runReplay(eng, events)
	if err != nil {
		t.Fatalf("replay failed: %v", err)
	}
	if got, want := summary.RequestsSent, int64(2); got != want {
		t.Fatalf("summary.RequestsSent = %d, want %d", got, want)
	}

	first := <-starts
	second := <-starts
	if first.path != "/a" || second.path != "/b" {
		t.Fatalf("request order = %s then %s, want /a then /b", first.path, second.path)
	}
	if delta := second.at.Sub(first.at); delta < 80*time.Millisecond {
		t.Fatalf("request start delta = %s, want at least 80ms", delta)
	}
}

func TestPacingClockDoesNotRewindForNonIncreasingTimestamps(t *testing.T) {
	base := time.Date(2026, 2, 27, 3, 10, 21, 0, time.UTC)
	cfg := config.Default()
	cfg.Replay.Pacing.Enabled = true
	cfg.Replay.Pacing.MaxSleepDelta = 500 * time.Millisecond

	eng := New(cfg, metrics.New(cfg.Metrics))
	cs := eng.newConnState(model.ConnectionKey{ConnectionID: 1})

	start := time.Now()
	for _, ts := range []time.Time{
		base,
		base.Add(100 * time.Millisecond),
		base.Add(50 * time.Millisecond),
		base.Add(150 * time.Millisecond),
	} {
		if err := eng.paceRequest(context.Background(), cs, model.Event{Timestamp: ts.Format(time.RFC3339Nano)}); err != nil {
			t.Fatalf("paceRequest(%s) error: %v", ts.Format(time.RFC3339Nano), err)
		}
	}
	elapsed := time.Since(start)
	if elapsed < 125*time.Millisecond {
		t.Fatalf("paceRequest(non-increasing timestamps) elapsed = %s, want at least 125ms", elapsed)
	}
}

func TestPacingSleepDoesNotLogByDefault(t *testing.T) {
	var buf bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})))
	t.Cleanup(func() {
		slog.SetDefault(previousLogger)
	})

	base := time.Date(2026, 2, 27, 3, 10, 21, 0, time.UTC)
	cfg := config.Default()
	cfg.Replay.Pacing.Enabled = true
	cfg.Replay.Pacing.MaxSleepDelta = time.Millisecond

	eng := New(cfg, metrics.New(cfg.Metrics))
	if err := eng.sleepForPacing(context.Background(), base, true, base.Add(100*time.Millisecond).Format(time.RFC3339Nano)); err != nil {
		t.Fatalf("sleepForPacing() error: %v", err)
	}
	if got := buf.String(); got != "" {
		t.Fatalf("sleepForPacing() log output = %q, want empty", got)
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

	eng := New(cfg, metrics.New(cfg.Metrics))
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

	eng := New(cfg, metrics.New(cfg.Metrics))
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

	eng := New(cfg, metrics.New(cfg.Metrics))
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

	eng := New(cfg, metrics.New(cfg.Metrics))
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

func TestNewEngineAbsoluteURLValidation(t *testing.T) {
	tests := []struct {
		name        string
		overrideURL string
		expectValid bool
	}{
		{
			name:        "Valid absolute HTTP URL",
			overrideURL: "http://example.com",
			expectValid: true,
		},
		{
			name:        "Valid absolute HTTPS URL with path",
			overrideURL: "https://api.example.com/v1",
			expectValid: true,
		},
		{
			name:        "Invalid relative URL (missing scheme)",
			overrideURL: "example.com",
			expectValid: false,
		},
		{
			name:        "Invalid relative URL (missing host)",
			overrideURL: "/relative/path",
			expectValid: false,
		},
		{
			name:        "Empty string",
			overrideURL: "",
			expectValid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := config.Default()
			cfg.Target.OverrideURL = tt.overrideURL

			eng := New(cfg, metrics.New(cfg.Metrics))
			if tt.expectValid {
				if eng.parsedOverrideURL == nil {
					t.Errorf("expected parsedOverrideURL to be non-nil for URL %q", tt.overrideURL)
				} else if eng.parsedOverrideURL.String() != tt.overrideURL {
					t.Errorf("expected parsed URL %q, got %q", tt.overrideURL, eng.parsedOverrideURL.String())
				}
			} else {
				if eng.parsedOverrideURL != nil {
					t.Errorf("expected parsedOverrideURL to be nil for invalid URL %q", tt.overrideURL)
				}
			}
		})
	}
}

func TestReplayRejectsInvalidRequiredOverride(t *testing.T) {
	var attempts atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	target, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("url.Parse(%q) error: %v", srv.URL, err)
	}

	cfg := config.Default()
	cfg.Target.OverrideURL = "example.com"
	cfg.Target.Require = true
	cfg.Replay.Idempotency.Enabled = false
	eng := New(cfg, metrics.New(cfg.Metrics))
	events := make(chan model.Event, 4)
	events <- model.Event{Type: model.EventMeta}
	events <- model.Event{Type: model.EventConnectionOpen, ConnectionID: 1}
	events <- model.Event{Type: model.EventRequest, ConnectionID: 1, Sequence: 1, HTTP: model.HTTPRequestMeta{Method: http.MethodGet, Scheme: target.Scheme, Authority: target.Host, Path: "/"}}
	events <- model.Event{Type: model.EventConnectionClose, ConnectionID: 1}
	close(events)

	summary, err := eng.ReplayStream(context.Background(), events)
	if err == nil {
		t.Fatalf("runReplay() error = nil, summary=%+v", summary)
	}
	if got := attempts.Load(); got != 0 {
		t.Fatalf("original target requests = %d, want 0", got)
	}
}

func TestReplayStreamCancelsInFlightRequestOnRouteError(t *testing.T) {
	started := make(chan struct{})
	canceled := make(chan struct{})
	releaseCh := make(chan struct{})
	var startedOnce sync.Once
	var canceledOnce sync.Once
	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() {
			close(releaseCh)
		})
	}
	t.Cleanup(release)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		startedOnce.Do(func() {
			close(started)
		})
		select {
		case <-r.Context().Done():
			canceledOnce.Do(func() {
				close(canceled)
			})
		case <-releaseCh:
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
		}
	}))
	defer srv.Close()

	target, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("url parse failed: %v", err)
	}

	cfg := config.Default()
	cfg.Replay.Timeout.Request = 2 * time.Second
	cfg.Replay.Idempotency.Enabled = false
	eng := New(cfg, metrics.New(cfg.Metrics))

	events := make(chan model.Event)
	resultCh := make(chan replayResult, 1)
	go func() {
		summary, err := eng.ReplayStream(context.Background(), events)
		resultCh <- replayResult{summary: summary, err: err}
	}()

	events <- model.Event{Type: model.EventMeta}
	events <- model.Event{Type: model.EventConnectionOpen, ConnectionID: 1}
	events <- model.Event{
		Type:         model.EventRequest,
		ConnectionID: 1,
		Sequence:     1,
		HTTP: model.HTTPRequestMeta{
			Method:    http.MethodGet,
			Scheme:    target.Scheme,
			Authority: target.Host,
			Path:      "/slow",
		},
	}

	select {
	case <-started:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("request did not start")
	}

	events <- model.Event{
		Type:         model.EventRequest,
		ConnectionID: 2,
		Sequence:     1,
		HTTP: model.HTTPRequestMeta{
			Method:    http.MethodGet,
			Scheme:    target.Scheme,
			Authority: target.Host,
			Path:      "/missing-open",
		},
	}
	close(events)

	select {
	case result := <-resultCh:
		if result.err == nil {
			t.Fatal("ReplayStream() error = nil, want route error")
		}
		if result.summary.Outcome != RunFailed {
			t.Fatalf("summary.Outcome = %s, want %s", result.summary.Outcome, RunFailed)
		}
	case <-time.After(200 * time.Millisecond):
		release()
		result := <-resultCh
		t.Fatalf("ReplayStream did not cancel in-flight request before route error returned; summary=%+v err=%v", result.summary, result.err)
	}

	select {
	case <-canceled:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("in-flight request context was not canceled")
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

	eng := New(cfg, metrics.New(cfg.Metrics))
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
