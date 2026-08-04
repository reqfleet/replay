package engine

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"testing/iotest"
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

func intPointer(value int) *int {
	return &value
}

type replayResult struct {
	summary Summary
	err     error
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type readAfterEOFTrackingBody struct {
	reader        *bytes.Reader
	sawEOF        bool
	readsAfterEOF int
}

func (b *readAfterEOFTrackingBody) Read(p []byte) (int, error) {
	if b.sawEOF {
		b.readsAfterEOF++
	}
	n, err := b.reader.Read(p)
	if errors.Is(err, io.EOF) {
		b.sawEOF = true
	}
	return n, err
}

func (*readAfterEOFTrackingBody) Close() error {
	return nil
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
	events := []model.Event{}
	for connectionID := 1; connectionID <= connections; connectionID++ {
		events = append(events,
			model.Event{Type: model.EventConnectionOpen, ConnectionID: connectionID},
			model.Event{Type: model.EventRequest,
				ConnectionID: connectionID,
				Sequence:     1, Method: http.MethodGet,
				Scheme:    target.Scheme,
				Authority: target.Host,
				Path:      "/"},
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

func TestExecuteRequestRetainsResponseHeadersOnlyForHeaderValidation(t *testing.T) {
	tests := []struct {
		name             string
		statusValidation bool
		headerValidation bool
		wantHeaders      bool
	}{
		{
			name:             "all_checks_disabled",
			statusValidation: false,
			headerValidation: false,
			wantHeaders:      false,
		},
		{
			name:             "status_validation_only",
			statusValidation: true,
			headerValidation: false,
			wantHeaders:      false,
		},
		{
			name:             "header_validation",
			statusValidation: false,
			headerValidation: true,
			wantHeaders:      true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			responseHeaders := http.Header{"X-Test": {"one", "two"}}
			client := &http.Client{
				Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
					return &http.Response{
						StatusCode: http.StatusOK,
						Header:     responseHeaders,
						Body:       io.NopCloser(strings.NewReader("ok")),
						Request:    req,
					}, nil
				}),
			}
			cfg := config.Default()
			cfg.Replay.Validation.Status = tt.statusValidation
			cfg.Replay.Validation.Headers = tt.headerValidation
			eng := New(cfg, metrics.New(cfg.Metrics))
			requestEvent := model.Event{Method: http.MethodGet, Scheme: "http", Authority: "example.test", Path: "/"}

			exec, err := eng.executeRequest(
				context.Background(), client, requestEvent, eng.effectiveRequestHeaders(nil),
			)
			if err != nil {
				t.Fatalf("executeRequest(status=%t, headers=%t) error: %v", tt.statusValidation, tt.headerValidation, err)
			}
			wantEgressBytes := int64(len("ok")) + responseHeaderBytes(responseHeaders)
			if got := exec.egressBytes; got != wantEgressBytes {
				t.Errorf("executeRequest(status=%t, headers=%t).egressBytes = %d, want %d", tt.statusValidation, tt.headerValidation, got, wantEgressBytes)
			}
			if !tt.wantHeaders {
				if exec.headers != nil {
					t.Errorf("executeRequest(status=%t, headers=%t).headers = %v, want nil", tt.statusValidation, tt.headerValidation, exec.headers)
				}
				return
			}
			values, ok := exec.headers["x-test"]
			if !ok || len(values) != 2 || values[0] != "one" || values[1] != "two" {
				t.Errorf("executeRequest(status=%t, headers=%t).headers[x-test] = %v, want [one two]", tt.statusValidation, tt.headerValidation, values)
			}
			if _, ok := exec.headers["X-Test"]; ok {
				t.Errorf("executeRequest(status=%t, headers=%t).headers contains non-normalized key X-Test", tt.statusValidation, tt.headerValidation)
			}
			responseHeaders["X-Test"][0] = "changed"
			if got := exec.headers["x-test"][0]; got != "one" {
				t.Errorf("executeRequest(status=%t, headers=%t).headers[x-test][0] after source mutation = %q, want %q", tt.statusValidation, tt.headerValidation, got, "one")
			}
		})
	}
}

func TestExecuteRequestDoesNotReadAfterResponseEOF(t *testing.T) {
	tests := []struct {
		name string
		body []byte
	}{
		{
			name: "below_limit",
			body: []byte("response body"),
		},
		{
			name: "at_limit",
			body: bytes.Repeat([]byte("x"), maxBodyRead),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			trackedBody := &readAfterEOFTrackingBody{reader: bytes.NewReader(tt.body)}
			client := &http.Client{
				Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
					return &http.Response{
						StatusCode: http.StatusOK,
						Header:     make(http.Header),
						Body:       trackedBody,
						Request:    req,
					}, nil
				}),
			}
			cfg := config.Default()
			cfg.Replay.Validation.Body = true
			eng := New(cfg, metrics.New(cfg.Metrics))
			requestEvent := model.Event{Method: http.MethodGet, Scheme: "http", Authority: "example.test", Path: "/"}

			exec, err := eng.executeRequest(
				context.Background(), client, requestEvent, eng.effectiveRequestHeaders(nil),
			)
			if err != nil {
				t.Fatalf("executeRequest(body size %d) error: %v", len(tt.body), err)
			}
			if !trackedBody.sawEOF {
				t.Errorf("executeRequest(body size %d) did not read response EOF", len(tt.body))
			}
			if got := trackedBody.readsAfterEOF; got != 0 {
				t.Errorf("executeRequest(body size %d) reads after EOF = %d, want 0", len(tt.body), got)
			}
			if exec.bodyTruncated {
				t.Errorf("executeRequest(body size %d).bodyTruncated = true, want false", len(tt.body))
			}
			if got, want := exec.bodySize, int64(len(tt.body)); got != want {
				t.Errorf("executeRequest(body size %d).bodySize = %d, want %d", len(tt.body), got, want)
			}
			if !bytes.Equal(exec.body, tt.body) {
				t.Errorf("executeRequest(body size %d).body does not match response body", len(tt.body))
			}
			if got, want := exec.bodyDigest, sha256.Sum256(tt.body); got != want {
				t.Errorf("executeRequest(body size %d).bodyDigest = %x, want %x", len(tt.body), got, want)
			}
		})
	}
}

func TestExecuteRequestIncludesDrainedBodyInEgressBytesOnReadError(t *testing.T) {
	bodyErr := errors.New("body read failed")
	responseBody := bytes.Repeat([]byte("x"), maxBodyRead+32*1024)
	client := &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body: io.NopCloser(io.MultiReader(
					bytes.NewReader(responseBody),
					iotest.ErrReader(bodyErr),
				)),
				Request: req,
			}, nil
		}),
	}
	cfg := config.Default()
	cfg.Replay.Validation.Body = true
	eng := New(cfg, metrics.New(cfg.Metrics))
	requestEvent := model.Event{Method: http.MethodGet, Scheme: "http", Authority: "example.test", Path: "/"}

	exec, err := eng.executeRequest(
		context.Background(), client, requestEvent, eng.effectiveRequestHeaders(nil),
	)
	if !errors.Is(err, bodyErr) {
		t.Fatalf("executeRequest(truncated body with read error) error = %v, want %v", err, bodyErr)
	}
	wantEgressBytes := int64(len(responseBody)) + responseHeaderBytes(make(http.Header))
	if got := exec.egressBytes; got != wantEgressBytes {
		t.Errorf(
			"executeRequest(truncated body with read error).egressBytes = %d, want %d",
			got,
			wantEgressBytes,
		)
	}
}

func TestExecuteRequestIncludesResponseHeadersInEgressBytes(t *testing.T) {
	const response = "HTTP/1.1 200 OK\r\nX-Multi: one\r\nX-Multi: two\r\nX-Test: abc\r\n\r\nok"
	authority := startRawHTTPResponseServer(t, response)
	cfg := config.Default()
	eng := New(cfg, metrics.New(cfg.Metrics))
	client, transport := eng.makePerConnectionClient(false)
	defer transport.CloseIdleConnections()

	requestEvent := model.Event{Method: http.MethodGet,
		Scheme:    "http",
		Authority: authority,
		Path:      "/"}
	exec, err := eng.executeRequest(
		context.Background(),
		client,
		requestEvent,
		eng.effectiveRequestHeaders(requestEvent.Headers),
	)
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

	requestEvent := model.Event{Method: http.MethodGet,
		Scheme:    "http",
		Authority: authority,
		Path:      "/"}
	exec, err := eng.executeRequest(
		context.Background(),
		client,
		requestEvent,
		eng.effectiveRequestHeaders(requestEvent.Headers),
	)
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
		{Type: model.EventConnectionOpen, ConnectionID: 1},
		{
			Type:         model.EventRequest,
			ConnectionID: 1,
			Sequence:     1,
			Method:       http.MethodGet,
			Scheme:       target.Scheme,
			Authority:    target.Host,
			Path:         "/",
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

func TestExpectationFreeRecordingPreservesResponseProcessing(t *testing.T) {
	var attempts atomic.Int64
	var remoteMu sync.Mutex
	remoteAddresses := make(map[string]struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		remoteMu.Lock()
		remoteAddresses[r.RemoteAddr] = struct{}{}
		remoteMu.Unlock()

		current := attempts.Add(1)
		w.Header().Set("X-Test", "response")
		if current == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("retry"))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	t.Cleanup(srv.Close)
	target, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("url.Parse(%q) error: %v", srv.URL, err)
	}

	cfg := config.Default()
	cfg.Replay.Validation.Status = false
	cfg.Replay.Validation.Headers = false
	cfg.Replay.Validation.Body = false
	cfg.Replay.Retry.MaxAttempts = 2
	cfg.Replay.Retry.Backoff = "none"
	cfg.Replay.Retry.RetryOnStatuses = []int{http.StatusServiceUnavailable}
	reg := metrics.New(cfg.Metrics)
	eng := New(cfg, reg)
	requestEvent := func(sequence int) model.Event {
		return model.Event{
			Type:         model.AccessLogTypeDownstreamStart,
			ConnectionID: 1,
			Sequence:     sequence,
			Method:       http.MethodGet,
			Scheme:       target.Scheme,
			Authority:    target.Host,
			Path:         "/",
		}
	}
	events := []model.Event{
		{Type: model.EventConnectionOpen, ConnectionID: 1},
		requestEvent(1),
		requestEvent(2),
		{Type: model.EventConnectionClose, ConnectionID: 1},
	}

	summary, err := runReplay(eng, events)
	if err != nil {
		t.Fatalf("ReplayStream(expectation-free recording) error: %v", err)
	}
	if got, want := summary.RequestsSent, int64(2); got != want {
		t.Errorf("ReplayStream(expectation-free recording).RequestsSent = %d, want %d", got, want)
	}
	if got, want := summary.ResponsesReceived, int64(2); got != want {
		t.Errorf("ReplayStream(expectation-free recording).ResponsesReceived = %d, want %d", got, want)
	}
	if got := summary.ValidationFailed; got != 0 {
		t.Errorf("ReplayStream(expectation-free recording).ValidationFailed = %d, want 0", got)
	}
	if got, want := summary.Outcome, RunSuccess; got != want {
		t.Errorf("ReplayStream(expectation-free recording).Outcome = %q, want %q", got, want)
	}
	if got, want := attempts.Load(), int64(3); got != want {
		t.Errorf("ReplayStream(expectation-free recording) attempts = %d, want %d", got, want)
	}
	remoteMu.Lock()
	connectionCount := len(remoteAddresses)
	remoteMu.Unlock()
	if connectionCount != 1 {
		t.Errorf("ReplayStream(expectation-free recording) target connections = %d, want 1", connectionCount)
	}

	commonLabelValues := cfg.Metrics.CommonLabelValues()
	status503 := reg.StatusCounter.WithLabelValues(append(commonLabelValues, "/", "503")...)
	if got, want := testutil.ToFloat64(status503), float64(1); got != want {
		t.Errorf("expectation-free 503 status counter = %v, want %v", got, want)
	}
	status200 := reg.StatusCounter.WithLabelValues(append(commonLabelValues, "/", "200")...)
	if got, want := testutil.ToFloat64(status200), float64(2); got != want {
		t.Errorf("expectation-free 200 status counter = %v, want %v", got, want)
	}
	if got, want := latencySampleCount(t, reg, commonLabelValues, "/"), uint64(3); got != want {
		t.Errorf("expectation-free latency sample count = %d, want %d", got, want)
	}
	egress := reg.EgressCounter.WithLabelValues(append(commonLabelValues, "/")...)
	if got, minimum := testutil.ToFloat64(egress), float64(len("retryokok")); got <= minimum {
		t.Errorf("expectation-free egress counter = %v, want more than body bytes %v", got, minimum)
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

	requestEvent := model.Event{Method: http.MethodGet,
		Scheme:    "http",
		Authority: "example.test",
		Path:      "/"}
	exec, err := eng.sendRequest(ctx, client, requestEvent, eng.effectiveRequestHeaders(requestEvent.Headers))
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

func TestRetryErrorCategoryUsesStructuredErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "timeout",
			err:  context.DeadlineExceeded,
			want: "timeout",
		},
		{
			name: "connection reset",
			err: &net.OpError{
				Op:  "read",
				Net: "tcp",
				Err: syscall.ECONNRESET,
			},
			want: "connection_reset",
		},
		{
			name: "TLS",
			err:  tls.RecordHeaderError{Msg: "invalid record"},
			want: "tls",
		},
		{
			name: "network",
			err: &net.DNSError{
				Err:        "lookup failed",
				Name:       "example.test",
				IsNotFound: true,
			},
			want: "network",
		},
		{
			name: "unstructured message",
			err:  errors.New("connection reset during TLS dial tcp"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := retryErrorCategory(tt.err); got != tt.want {
				t.Errorf("retryErrorCategory(%T) = %q, want %q", tt.err, got, tt.want)
			}
		})
	}
}

func TestMetricStatusForSendErrorUsesStructuredErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "nil",
			want: "send_error",
		},
		{
			name: "timeout",
			err:  context.DeadlineExceeded,
			want: "timeout",
		},
		{
			name: "connection refused",
			err: &net.OpError{
				Op:  "dial",
				Net: "tcp",
				Err: syscall.ECONNREFUSED,
			},
			want: "connection_refused",
		},
		{
			name: "connection reset",
			err: &net.OpError{
				Op:  "read",
				Net: "tcp",
				Err: syscall.ECONNRESET,
			},
			want: "connection_reset",
		},
		{
			name: "TLS",
			err:  tls.RecordHeaderError{Msg: "invalid record"},
			want: "tls",
		},
		{
			name: "network",
			err: &net.DNSError{
				Err:        "lookup failed",
				Name:       "example.test",
				IsNotFound: true,
			},
			want: "network",
		},
		{
			name: "unstructured message",
			err:  errors.New("connection reset during TLS dial tcp"),
			want: "send_error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := metricStatusForSendError(tt.err); got != tt.want {
				t.Errorf("metricStatusForSendError(%T) = %q, want %q", tt.err, got, tt.want)
			}
		})
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

	cfg.Replay.Validation.Status = true
	eng := New(cfg, metrics.New(cfg.Metrics))
	events := []model.Event{
		{Type: model.EventConnectionOpen, ConnectionID: 1},
		{
			Type:         model.AccessLogTypeDownstreamStart,
			ConnectionID: 1,
			Sequence:     1,
			ResponseCode: intPointer(http.StatusOK),
			Method:       http.MethodGet,
			Scheme:       target.Scheme,
			Authority:    target.Host,
			Path:         "/",
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

	cfg.Replay.Validation.Status = true
	eng := New(cfg, metrics.New(cfg.Metrics))
	events := []model.Event{
		{Type: model.EventConnectionOpen, ConnectionID: 1},
		{
			Type:         model.AccessLogTypeDownstreamEnd,
			ConnectionID: 1,
			Sequence:     1,
			ResponseCode: intPointer(http.StatusOK),
			Method:       http.MethodGet,
			Scheme:       target.Scheme,
			Authority:    target.Host,
			Path:         "/",
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

func TestReplayRejectsResponseEvents(t *testing.T) {
	cfg := config.Default()

	eng := New(cfg, metrics.New(cfg.Metrics))
	events := []model.Event{{
		Type:         model.EventType("response"),
		ConnectionID: 1,
		Sequence:     1,
	}}

	if _, err := runReplay(eng, events); err == nil {
		t.Fatal("runReplay(response event) error = nil, want error")
	}
}

func TestReplayTreatsConnectionRefusedAsPartialSuccess(t *testing.T) {
	addr := closedLocalAddress(t)

	cfg := config.Default()
	eng := New(cfg, metrics.New(cfg.Metrics))
	events := []model.Event{
		{Type: model.EventConnectionOpen, ConnectionID: 1},
		{
			Type:         model.EventRequest,
			ConnectionID: 1,
			Sequence:     1,
			Method:       http.MethodGet,
			Scheme:       "http",
			Authority:    addr,
			Path:         "/transport",
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

func TestReplayDoesNotValidateResponseAfterTransportSendError(t *testing.T) {
	addr := closedLocalAddress(t)
	cfg := config.Default()

	cfg.Replay.Validation.Status = true
	eng := New(cfg, metrics.New(cfg.Metrics))
	events := []model.Event{
		{Type: model.EventConnectionOpen, ConnectionID: 1},
		{
			Type:         model.AccessLogTypeDownstreamEnd,
			ConnectionID: 1,
			Sequence:     1,
			ResponseCode: intPointer(http.StatusOK),
			Method:       http.MethodGet,
			Scheme:       "http",
			Authority:    addr,
			Path:         "/transport",
		},
		{Type: model.EventConnectionClose, ConnectionID: 1},
	}

	summary, err := runReplay(eng, events)
	if err != nil {
		t.Fatalf("runReplay() error: %v", err)
	}
	if got, want := summary.SendErrors, int64(1); got != want {
		t.Fatalf("summary.SendErrors = %d, want %d", got, want)
	}
	if got, want := summary.ValidationFailed, int64(0); got != want {
		t.Fatalf("summary.ValidationFailed = %d, want %d", got, want)
	}
	if got, want := summary.RequestsSent, int64(0); got != want {
		t.Fatalf("summary.RequestsSent = %d, want %d", got, want)
	}
	if got, want := summary.ResponsesReceived, int64(0); got != want {
		t.Fatalf("summary.ResponsesReceived = %d, want %d", got, want)
	}
	if got, want := len(summary.ConnectionResults), 1; got != want {
		t.Fatalf("len(summary.ConnectionResults) = %d, want %d", got, want)
	}
	connection := summary.ConnectionResults[0]
	if got, want := connection.SendErrors, int64(1); got != want {
		t.Fatalf("connection.SendErrors = %d, want %d", got, want)
	}
	if got, want := connection.ValidationFailed, int64(0); got != want {
		t.Fatalf("connection.ValidationFailed = %d, want %d", got, want)
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
				{Type: model.EventConnectionOpen, ConnectionID: 1},
				{
					Type:         model.EventRequest,
					ConnectionID: 1,
					Sequence:     1,
					Method:       http.MethodGet,
					Scheme:       "http",
					Authority:    authority,
					Path:         "/transport",
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

	cfg.Replay.Validation.Status = true
	cfg.Replay.Validation.Headers = true
	cfg.Replay.Validation.IgnoreHeaders = []string{"x-request-id"}

	eng := New(cfg, metrics.New(cfg.Metrics))
	events := []model.Event{
		{Type: model.EventConnectionOpen, ConnectionID: 1},
		{
			Type:         model.AccessLogTypeDownstreamEnd,
			ConnectionID: 1,
			Sequence:     1,
			ResponseCode: intPointer(http.StatusOK),
			ResponseHeaders: map[string][]string{
				"content-type": {"application/json"},
				"x-request-id": {"expected-other-id"},
			},
			Method:    http.MethodGet,
			Scheme:    target.Scheme,
			Authority: target.Host,
			Path:      "/",
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

	cfg.Replay.Validation.Status = true
	cfg.Replay.Validation.Body = true

	eng := New(cfg, metrics.New(cfg.Metrics))
	events := []model.Event{
		{Type: model.EventConnectionOpen, ConnectionID: 1},
		{
			Type:         model.AccessLogTypeDownstreamEnd,
			ConnectionID: 1,
			Sequence:     1,
			ResponseCode: intPointer(http.StatusOK),
			ResponseBody: &model.Body{
				Encoding: "base64",
				Content:  base64.StdEncoding.EncodeToString([]byte("expected-body")),
			},
			Method:    http.MethodGet,
			Scheme:    target.Scheme,
			Authority: target.Host,
			Path:      "/",
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

func TestReplayBodyValidationDistinguishesEmptyFromAbsent(t *testing.T) {
	tests := []struct {
		name         string
		actualBody   string
		expectedBody *model.Body
		wantFailures int64
	}{
		{
			name:         "explicit empty body rejects non-empty response",
			actualBody:   "unexpected",
			expectedBody: &model.Body{Encoding: "base64"},
			wantFailures: 1,
		},
		{
			name:         "explicit empty body accepts empty response",
			expectedBody: &model.Body{Encoding: "base64"},
		},
		{
			name:       "absent body does not assert emptiness",
			actualBody: "unexpected",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(tt.actualBody))
			}))
			defer srv.Close()

			target, err := url.Parse(srv.URL)
			if err != nil {
				t.Fatalf("url.Parse(%q) error: %v", srv.URL, err)
			}
			cfg := config.Default()

			cfg.Replay.Validation.Body = true
			eng := New(cfg, metrics.New(cfg.Metrics))
			events := []model.Event{
				{Type: model.EventConnectionOpen, ConnectionID: 1},
				{
					Type:         model.AccessLogTypeDownstreamEnd,
					ConnectionID: 1,
					Sequence:     1,
					ResponseCode: intPointer(http.StatusOK),
					ResponseBody: tt.expectedBody,
					Method:       http.MethodGet,
					Scheme:       target.Scheme,
					Authority:    target.Host,
					Path:         "/",
				},
				{Type: model.EventConnectionClose, ConnectionID: 1},
			}

			summary, err := runReplay(eng, events)
			if err != nil {
				t.Fatalf("runReplay() error: %v", err)
			}
			if got := summary.ValidationFailed; got != tt.wantFailures {
				t.Fatalf("summary.ValidationFailed = %d, want %d", got, tt.wantFailures)
			}
		})
	}
}

func TestResponseValidationComparesOversizedBodiesExactly(t *testing.T) {
	tests := []struct {
		name       string
		extraBytes int
	}{
		{
			name:       "one_byte_over_limit",
			extraBytes: 1,
		},
		{
			name:       "much_larger_than_limit",
			extraBytes: 1024 * 1024,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actualBody := bytes.Repeat([]byte("x"), maxBodyRead+tt.extraBytes)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write(actualBody)
			}))
			t.Cleanup(srv.Close)

			target, err := url.Parse(srv.URL)
			if err != nil {
				t.Fatalf("url.Parse(%q) error: %v", srv.URL, err)
			}
			cfg := config.Default()

			cfg.Replay.Validation.Body = true
			eng := New(cfg, metrics.New(cfg.Metrics))
			client, transport := eng.makePerConnectionClient(false)
			t.Cleanup(transport.CloseIdleConnections)
			requestEvent := model.Event{Method: http.MethodGet, Scheme: target.Scheme, Authority: target.Host, Path: "/"}

			exec, err := eng.executeRequest(
				context.Background(), client, requestEvent, eng.effectiveRequestHeaders(nil),
			)
			if err != nil {
				t.Fatalf("executeRequest(body size %d) error: %v", len(actualBody), err)
			}
			if !exec.bodyTruncated {
				t.Errorf("executeRequest(body size %d).bodyTruncated = false, want true", len(actualBody))
			}
			if got, want := exec.bodySize, int64(len(actualBody)); got != want {
				t.Errorf("executeRequest(body size %d).bodySize = %d, want %d", len(actualBody), got, want)
			}
			if !bytes.Equal(exec.body, actualBody[:maxBodyRead]) {
				t.Errorf("executeRequest(body size %d).body does not match retained prefix", len(actualBody))
			}
			if got, want := exec.bodyDigest, sha256.Sum256(actualBody); got != want {
				t.Errorf("executeRequest(body size %d).bodyDigest = %x, want %x", len(actualBody), got, want)
			}

			prefixOnly := model.Event{ResponseBody: &model.Body{
				Encoding: "base64",
				Content:  base64.StdEncoding.EncodeToString(actualBody[:maxBodyRead]),
			}}
			if !eng.responseValidationFailed(prefixOnly, exec) {
				t.Errorf("responseValidationFailed(prefix only, body size %d) = false, want true", len(actualBody))
			}
			exact := model.Event{ResponseBody: &model.Body{
				Encoding: "base64",
				Content:  base64.StdEncoding.EncodeToString(actualBody),
			}}
			if eng.responseValidationFailed(exact, exec) {
				t.Errorf("responseValidationFailed(exact body, body size %d) = true, want false", len(actualBody))
			}
		})
	}
}

func TestFinishRequestSuccessValidatesInlineImmediately(t *testing.T) {
	cfg := config.Default()
	cfg.Replay.Validation.Status = true
	eng := New(cfg, metrics.New(cfg.Metrics))
	cs := eng.newConnState(model.ConnectionKey{ConnectionID: 1})
	req := model.Event{
		Type:         model.AccessLogTypeDownstreamEnd,
		ConnectionID: 1,
		Sequence:     1,
		ResponseCode: intPointer(http.StatusOK),
	}

	abort := eng.finishRequestSuccess(cs, req, requestExecution{statusCode: http.StatusInternalServerError}, nil)
	if abort {
		t.Fatal("finishRequestSuccess aborted unexpectedly")
	}
	if got, want := cs.validationFailed, int64(1); got != want {
		t.Fatalf("finishRequestSuccess validation failures = %d, want %d", got, want)
	}
	if got, want := cs.sent, int64(1); got != want {
		t.Fatalf("finishRequestSuccess sent = %d, want %d", got, want)
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
		{Type: model.EventConnectionOpen, ConnectionID: 1},
		{
			Type:         model.EventRequest,
			ConnectionID: 1,
			Sequence:     1,
			Method:       http.MethodPost,
			Scheme:       target.Scheme,
			Authority:    target.Host,
			Path:         "/mutate",
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
	tests := []struct {
		name            string
		recordedHeaders map[string][]string
		dropHeaders     []string
		setHeaders      map[string]string
		wantSkipped     int64
		wantSent        int64
		wantAttempts    int64
		wantHeader      string
	}{
		{
			name:            "dropped required header blocks request",
			recordedHeaders: map[string][]string{"Idempotency-Key": {"recorded"}},
			dropHeaders:     []string{"idempotency-key"},
			wantSkipped:     1,
		},
		{
			name:         "set required header allows request",
			setHeaders:   map[string]string{"Idempotency-Key": "replacement"},
			wantSent:     1,
			wantAttempts: 1,
			wantHeader:   "replacement",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var attempts atomic.Int64
			var receivedHeader string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				attempts.Add(1)
				receivedHeader = r.Header.Get("Idempotency-Key")
				w.WriteHeader(http.StatusOK)
			}))
			defer srv.Close()

			target, err := url.Parse(srv.URL)
			if err != nil {
				t.Fatalf("url.Parse(%q) error: %v", srv.URL, err)
			}

			cfg := config.Default()
			cfg.Header.Drop = tt.dropHeaders
			cfg.Header.Set = tt.setHeaders
			eng := New(cfg, metrics.New(cfg.Metrics))
			events := []model.Event{
				{Type: model.EventConnectionOpen, ConnectionID: 1},
				{
					Type:         model.EventRequest,
					ConnectionID: 1,
					Sequence:     1,
					Headers:      tt.recordedHeaders,
					Method:       http.MethodPost,
					Scheme:       target.Scheme,
					Authority:    target.Host,
					Path:         "/mutate",
				},
				{Type: model.EventConnectionClose, ConnectionID: 1},
			}

			summary, err := runReplay(eng, events)
			if err != nil {
				t.Fatalf("runReplay() error: %v", err)
			}
			if got := summary.Skipped; got != tt.wantSkipped {
				t.Errorf("summary.Skipped = %d, want %d", got, tt.wantSkipped)
			}
			if got := summary.RequestsSent; got != tt.wantSent {
				t.Errorf("summary.RequestsSent = %d, want %d", got, tt.wantSent)
			}
			if got := attempts.Load(); got != tt.wantAttempts {
				t.Errorf("attempts = %d, want %d", got, tt.wantAttempts)
			}
			if receivedHeader != tt.wantHeader {
				t.Errorf("received Idempotency-Key = %q, want %q", receivedHeader, tt.wantHeader)
			}
		})
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
	eng := New(cfg, metrics.New(cfg.Metrics))
	events := []model.Event{
		{Type: model.EventConnectionOpen, ConnectionID: 1},
		{
			Type:         model.EventRequest,
			ConnectionID: 1,
			Sequence:     1,
			Method:       http.MethodGet,
			Scheme:       target.Scheme,
			Authority:    target.Host,
			Path:         "/",
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
	events := []model.Event{}
	expectedSent := int64(0)
	for i, conn := range connections {
		events = append(events,
			model.Event{Type: model.EventConnectionOpen, ConnectionID: conn},
			model.Event{Type: model.EventRequest,
				ConnectionID: conn,
				Sequence:     i + 1, Method: http.MethodGet,
				Scheme:    target.Scheme,
				Authority: target.Host,
				Path:      "/"},
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
	events <- model.Event{Type: model.EventRequest,
		ConnectionID: connKey.ConnectionID,
		Sequence:     1, Method: http.MethodGet,
		Scheme:    "http",
		Authority: "example.test",
		Path:      "/"}
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
	events <- model.Event{Type: model.EventRequest, ConnectionID: 1, Sequence: 1, Method: http.MethodGet, Scheme: "http", Authority: "example.test", Path: "/"}
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

func TestConnectionBelongsToShardSupportsFullHashSpace(t *testing.T) {
	if strconv.IntSize < 64 {
		t.Skip("int cannot represent the full 32-bit hash space")
	}

	const shardIndex = 1803821790
	shardCount := int(uint64(1) << 32)
	connectionKey := model.ConnectionKey{ConnectionID: 1}
	if !connectionBelongsToShard(connectionKey, shardIndex, shardCount) {
		t.Errorf(
			"connectionBelongsToShard(%v, %d, %d) = false, want true",
			connectionKey,
			shardIndex,
			shardCount,
		)
	}
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
		{Type: model.EventConnectionOpen, Node: "envoy-a", ConnectionID: 1},
		{Type: model.EventConnectionOpen, Node: "envoy-b", ConnectionID: 1},
		{Type: model.EventRequest, Node: "envoy-a", ConnectionID: 1, Sequence: 1, Method: http.MethodGet, Scheme: target.Scheme, Authority: target.Host, Path: "/a"},
		{Type: model.EventRequest, Node: "envoy-b", ConnectionID: 1, Sequence: 1, Method: http.MethodGet, Scheme: target.Scheme, Authority: target.Host, Path: "/b"},
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
		{Type: model.EventConnectionOpen, ConnectionID: 1},
		{
			Type:         model.EventRequest,
			ConnectionID: 1,
			Sequence:     1,
			Method:       http.MethodGet,
			Scheme:       target.Scheme,
			Authority:    target.Host,
			Path:         "/",
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
		{Type: model.EventConnectionOpen, ConnectionID: 1},
		{Type: model.EventRequest, ConnectionID: 1, StreamID: 1, Sequence: 1, Protocol: "HTTP/2", Method: http.MethodGet, Scheme: target.Scheme, Authority: target.Host, Path: "/a"},
		{Type: model.EventRequest, ConnectionID: 1, StreamID: 3, Sequence: 2, Protocol: "HTTP/2", Method: http.MethodGet, Scheme: target.Scheme, Authority: target.Host, Path: "/b"},
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
	var maxInFlight atomic.Int64
	var inFlight atomic.Int64
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		negotiatedProto := ""
		if r.TLS != nil {
			negotiatedProto = r.TLS.NegotiatedProtocol
		}
		t.Logf("Test server received request: %s Proto: %s NegotiatedProtocol: %q", r.URL.Path, r.Proto, negotiatedProto)
		current := inFlight.Add(1)
		for {
			previous := maxInFlight.Load()
			if current <= previous || maxInFlight.CompareAndSwap(previous, current) {
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
	cfg.Replay.Validation.Status = true

	eng := New(cfg, metrics.New(cfg.Metrics))
	events := []model.Event{
		{Type: model.EventConnectionOpen, ConnectionID: 1},
		{Type: model.AccessLogTypeDownstreamEnd, ConnectionID: 1, StreamID: 1, Sequence: 1, ResponseCode: intPointer(http.StatusOK), Protocol: "HTTP/2", Method: http.MethodGet, Scheme: target.Scheme, Authority: target.Host, Path: "/a"},
		{Type: model.AccessLogTypeDownstreamEnd, ConnectionID: 1, StreamID: 3, Sequence: 2, ResponseCode: intPointer(http.StatusCreated), Protocol: "HTTP/2", Method: http.MethodGet, Scheme: target.Scheme, Authority: target.Host, Path: "/b"},
		{Type: model.EventConnectionClose, ConnectionID: 1},
	}

	summary, err := runReplay(eng, events)
	if err != nil {
		t.Fatalf("replay failed: %v", err)
	}
	if got, want := summary.ValidationFailed, int64(1); got != want {
		t.Fatalf("summary.ValidationFailed = %d, want %d", got, want)
	}
	// With InsecureSkipVerify set to true and a TLS server, true HTTP/2 multiplexing
	// is enabled. Both requests will be processed concurrently over a single connection,
	// so the maximum concurrent in-flight requests should be exactly 2.
	if got := maxInFlight.Load(); got != 2 {
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
		{Type: model.EventConnectionOpen, ConnectionID: 1},
		{Type: model.EventRequest, ConnectionID: 1, StreamID: 1, Sequence: 1, Protocol: "HTTP/2", Method: http.MethodGet, Scheme: target.Scheme, Authority: target.Host, Path: "/slow"},
		{Type: model.EventRequest, ConnectionID: 1, StreamID: 3, Sequence: 2, Protocol: "HTTP/2", Method: http.MethodGet, Scheme: target.Scheme, Authority: target.Host, Path: "/fast"},
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
		{Type: model.EventConnectionOpen, ConnectionID: 1},
		{Type: model.EventRequest, ConnectionID: 1, StreamID: 1, Sequence: 1, Timestamp: base.Format(time.RFC3339Nano), Protocol: "HTTP/2", Method: http.MethodGet, Scheme: target.Scheme, Authority: target.Host, Path: "/a"},
		{Type: model.EventRequest, ConnectionID: 1, StreamID: 3, Sequence: 2, Timestamp: base.Add(100 * time.Millisecond).Format(time.RFC3339Nano), Protocol: "HTTP/2", Method: http.MethodGet, Scheme: target.Scheme, Authority: target.Host, Path: "/b"},
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

func TestPacingAccountsForElapsedRequestTime(t *testing.T) {
	base := time.Date(2026, 2, 27, 3, 10, 21, 0, time.UTC)
	cfg := config.Default()
	cfg.Replay.Pacing.Enabled = true
	cfg.Replay.Pacing.MaxSleepDelta = time.Second

	eng := New(cfg, metrics.New(cfg.Metrics))
	cs := eng.newConnState(model.ConnectionKey{ConnectionID: 1})
	if err := eng.paceRequest(context.Background(), cs, model.Event{Timestamp: base.Format(time.RFC3339Nano)}); err != nil {
		t.Fatalf("paceRequest(first request) error: %v", err)
	}

	time.Sleep(150 * time.Millisecond)
	start := time.Now()
	if err := eng.paceRequest(context.Background(), cs, model.Event{Timestamp: base.Add(200 * time.Millisecond).Format(time.RFC3339Nano)}); err != nil {
		t.Fatalf("paceRequest(second request) error: %v", err)
	}
	if got, maximum := time.Since(start), 125*time.Millisecond; got >= maximum {
		t.Fatalf("paceRequest(second request) additional delay = %s, want less than %s after 150ms elapsed request time", got, maximum)
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
	var pacing pacingClock
	for _, ts := range []time.Time{base, base.Add(100 * time.Millisecond)} {
		if err := eng.paceTimestamp(context.Background(), &pacing, ts.Format(time.RFC3339Nano)); err != nil {
			t.Fatalf("paceTimestamp(%s) error: %v", ts.Format(time.RFC3339Nano), err)
		}
	}
	if got := buf.String(); got != "" {
		t.Fatalf("paceTimestamp() log output = %q, want empty", got)
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
		{Type: model.EventConnectionOpen, ConnectionID: 1},
		{Type: model.EventRequest, ConnectionID: 1, Sequence: 1, Method: http.MethodGet, Scheme: target.Scheme, Authority: target.Host, Path: "/"},
		{Type: model.EventConnectionClose, ConnectionID: 1},
		{Type: model.EventConnectionOpen, ConnectionID: 2},
		{Type: model.EventRequest, ConnectionID: 2, Sequence: 1, Method: http.MethodGet, Scheme: target.Scheme, Authority: target.Host, Path: "/"},
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
		t.Fatalf("remote addresses = %v, want 2 distinct addresses for sequential recorded connections", addrs)
	}
}

func TestHTTP1RequestsReuseRecordedConnectionSocket(t *testing.T) {
	var mu sync.Mutex
	var addrs []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		addrs = append(addrs, r.RemoteAddr)
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
		{Type: model.EventConnectionOpen, ConnectionID: 1},
		{Type: model.EventRequest, ConnectionID: 1, Sequence: 1, Protocol: "HTTP/1.1", Method: http.MethodGet, Scheme: target.Scheme, Authority: target.Host, Path: "/first"},
		{Type: model.EventRequest, ConnectionID: 1, Sequence: 2, Protocol: "HTTP/1.1", Method: http.MethodGet, Scheme: target.Scheme, Authority: target.Host, Path: "/second"},
		{Type: model.EventConnectionClose, ConnectionID: 1},
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
		t.Fatalf("remote addresses = %v, want one observation for each of 2 requests", addrs)
	}
	if addrs[0] != addrs[1] {
		t.Fatalf("remote addresses = %v, want both requests on recorded connection 1 to reuse one socket", addrs)
	}
}

func TestHTTP1OversizedResponsePreservesRecordedConnectionSocket(t *testing.T) {
	var mu sync.Mutex
	var addrs []string
	payload := bytes.Repeat([]byte("x"), maxBodyRead+1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		addrs = append(addrs, r.RemoteAddr)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(payload)
	}))
	defer srv.Close()

	target, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("url.Parse(%q) error: %v", srv.URL, err)
	}

	cfg := config.Default()
	cfg.Replay.Idempotency.Enabled = false
	eng := New(cfg, metrics.New(cfg.Metrics))
	events := []model.Event{
		{Type: model.EventConnectionOpen, ConnectionID: 1},
		{Type: model.EventRequest, ConnectionID: 1, Sequence: 1, Protocol: "HTTP/1.1", Method: http.MethodGet, Scheme: target.Scheme, Authority: target.Host, Path: "/first"},
		{Type: model.EventRequest, ConnectionID: 1, Sequence: 2, Protocol: "HTTP/1.1", Method: http.MethodGet, Scheme: target.Scheme, Authority: target.Host, Path: "/second"},
		{Type: model.EventConnectionClose, ConnectionID: 1},
	}

	summary, err := runReplay(eng, events)
	if err != nil {
		t.Fatalf("runReplay() error: %v", err)
	}
	if got, want := summary.RequestsSent, int64(2); got != want {
		t.Errorf("summary.RequestsSent = %d, want %d", got, want)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(addrs) != 2 {
		t.Fatalf("remote addresses = %v, want one observation for each of 2 requests", addrs)
	}
	if addrs[0] != addrs[1] {
		t.Fatalf("remote addresses = %v, want oversized responses to preserve recorded connection 1 socket", addrs)
	}
}

func TestHTTP1InterleavedRecordedConnectionsHaveStableSockets(t *testing.T) {
	var mu sync.Mutex
	addrsByPath := make(map[string]string)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		addrsByPath[r.URL.Path] = r.RemoteAddr
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
	cfg.Replay.MaxVirtualUsersPerEngine = 1
	cfg.Replay.Idempotency.Enabled = false

	eng := New(cfg, metrics.New(cfg.Metrics))
	events := []model.Event{
		{Type: model.EventConnectionOpen, ConnectionID: 1},
		{Type: model.EventConnectionOpen, ConnectionID: 2},
		{Type: model.EventRequest, ConnectionID: 1, Sequence: 1, Protocol: "HTTP/1.1", Method: http.MethodGet, Scheme: target.Scheme, Authority: target.Host, Path: "/connection-1/first"},
		{Type: model.EventRequest, ConnectionID: 2, Sequence: 1, Protocol: "HTTP/1.1", Method: http.MethodGet, Scheme: target.Scheme, Authority: target.Host, Path: "/connection-2/first"},
		{Type: model.EventRequest, ConnectionID: 1, Sequence: 2, Protocol: "HTTP/1.1", Method: http.MethodGet, Scheme: target.Scheme, Authority: target.Host, Path: "/connection-1/second"},
		{Type: model.EventRequest, ConnectionID: 2, Sequence: 2, Protocol: "HTTP/1.1", Method: http.MethodGet, Scheme: target.Scheme, Authority: target.Host, Path: "/connection-2/second"},
		{Type: model.EventConnectionClose, ConnectionID: 1},
		{Type: model.EventConnectionClose, ConnectionID: 2},
	}

	summary, err := runReplay(eng, events)
	if err != nil {
		t.Fatalf("replay failed: %v", err)
	}
	if summary.RequestsSent != 4 {
		t.Fatalf("summary.RequestsSent = %d, want 4", summary.RequestsSent)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(addrsByPath) != 4 {
		t.Fatalf("remote addresses by path = %v, want observations for all 4 requests", addrsByPath)
	}
	connection1First := addrsByPath["/connection-1/first"]
	connection1Second := addrsByPath["/connection-1/second"]
	connection2First := addrsByPath["/connection-2/first"]
	connection2Second := addrsByPath["/connection-2/second"]
	if connection1First == "" || connection1Second == "" || connection2First == "" || connection2Second == "" {
		t.Fatalf("remote addresses by path = %v, want a non-empty address for every request", addrsByPath)
	}
	if connection1First != connection1Second {
		t.Fatalf("connection 1 remote addresses = [%q %q], want a stable socket across interleaved requests; all observations: %v", connection1First, connection1Second, addrsByPath)
	}
	if connection2First != connection2Second {
		t.Fatalf("connection 2 remote addresses = [%q %q], want a stable socket across interleaved requests; all observations: %v", connection2First, connection2Second, addrsByPath)
	}
	if connection1First == connection2First {
		t.Fatalf("remote addresses by path = %v, want simultaneously open recorded connections on distinct sockets with one virtual user", addrsByPath)
	}
}

func TestHTTP1ClosingRecordedConnectionPreservesOtherSocket(t *testing.T) {
	var mu sync.Mutex
	addrsByPath := make(map[string]string)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		addrsByPath[r.URL.Path] = r.RemoteAddr
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
	cfg.Replay.MaxVirtualUsersPerEngine = 1
	cfg.Replay.Idempotency.Enabled = false

	eng := New(cfg, metrics.New(cfg.Metrics))
	events := []model.Event{
		{Type: model.EventConnectionOpen, ConnectionID: 1},
		{Type: model.EventConnectionOpen, ConnectionID: 2},
		{Type: model.EventRequest, ConnectionID: 1, Sequence: 1, Protocol: "HTTP/1.1", Method: http.MethodGet, Scheme: target.Scheme, Authority: target.Host, Path: "/closing"},
		{Type: model.EventRequest, ConnectionID: 2, Sequence: 1, Protocol: "HTTP/1.1", Method: http.MethodGet, Scheme: target.Scheme, Authority: target.Host, Path: "/still-open/before"},
		{Type: model.EventConnectionClose, ConnectionID: 1},
		{Type: model.EventRequest, ConnectionID: 2, Sequence: 2, Protocol: "HTTP/1.1", Method: http.MethodGet, Scheme: target.Scheme, Authority: target.Host, Path: "/still-open/after"},
		{Type: model.EventConnectionClose, ConnectionID: 2},
	}

	summary, err := runReplay(eng, events)
	if err != nil {
		t.Fatalf("replay failed: %v", err)
	}
	if summary.RequestsSent != 3 {
		t.Fatalf("summary.RequestsSent = %d, want 3", summary.RequestsSent)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(addrsByPath) != 3 {
		t.Fatalf("remote addresses by path = %v, want observations for all 3 requests", addrsByPath)
	}
	closing := addrsByPath["/closing"]
	beforeClose := addrsByPath["/still-open/before"]
	afterClose := addrsByPath["/still-open/after"]
	if closing == "" || beforeClose == "" || afterClose == "" {
		t.Fatalf("remote addresses by path = %v, want a non-empty address for every request", addrsByPath)
	}
	if closing == beforeClose {
		t.Fatalf("remote addresses by path = %v, want simultaneously open recorded connections on distinct sockets", addrsByPath)
	}
	if beforeClose != afterClose {
		t.Fatalf("still-open connection remote address changed from %q to %q after the other recorded connection closed; all observations: %v", beforeClose, afterClose, addrsByPath)
	}
}

func TestHTTP2RecordedConnectionsOwnDistinctMultiplexedSockets(t *testing.T) {
	type observation struct {
		remoteAddr string
		proto      string
	}

	var mu sync.Mutex
	observations := make(map[string]observation)
	inFlight := make(map[string]int)
	maxInFlight := make(map[string]int)
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		connection, _, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/"), "/")
		mu.Lock()
		observations[r.URL.Path] = observation{remoteAddr: r.RemoteAddr, proto: r.Proto}
		inFlight[connection]++
		if inFlight[connection] > maxInFlight[connection] {
			maxInFlight[connection] = inFlight[connection]
		}
		mu.Unlock()

		time.Sleep(80 * time.Millisecond)

		mu.Lock()
		inFlight[connection]--
		mu.Unlock()
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
	cfg.Replay.MaxVirtualUsersPerEngine = 1
	cfg.Replay.HTTP2.Mode = "multiplexed"
	cfg.Replay.TLS.InsecureSkipVerify = true
	cfg.Replay.Idempotency.Enabled = false

	eng := New(cfg, metrics.New(cfg.Metrics))
	events := []model.Event{
		{Type: model.EventConnectionOpen, ConnectionID: 1},
		{Type: model.EventConnectionOpen, ConnectionID: 2},
		{Type: model.EventRequest, ConnectionID: 1, StreamID: 1, Sequence: 1, Protocol: "HTTP/2", Method: http.MethodGet, Scheme: target.Scheme, Authority: target.Host, Path: "/connection-1/first"},
		{Type: model.EventRequest, ConnectionID: 1, StreamID: 3, Sequence: 2, Protocol: "HTTP/2", Method: http.MethodGet, Scheme: target.Scheme, Authority: target.Host, Path: "/connection-1/second"},
		{Type: model.EventRequest, ConnectionID: 2, StreamID: 1, Sequence: 1, Protocol: "HTTP/2", Method: http.MethodGet, Scheme: target.Scheme, Authority: target.Host, Path: "/connection-2/first"},
		{Type: model.EventRequest, ConnectionID: 2, StreamID: 3, Sequence: 2, Protocol: "HTTP/2", Method: http.MethodGet, Scheme: target.Scheme, Authority: target.Host, Path: "/connection-2/second"},
		{Type: model.EventConnectionClose, ConnectionID: 1},
		{Type: model.EventConnectionClose, ConnectionID: 2},
	}

	summary, err := runReplay(eng, events)
	if err != nil {
		t.Fatalf("replay failed: %v", err)
	}
	if summary.RequestsSent != 4 {
		t.Fatalf("summary.RequestsSent = %d, want 4", summary.RequestsSent)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(observations) != 4 {
		t.Fatalf("HTTP/2 observations = %v, want all 4 request paths", observations)
	}
	connection1First := observations["/connection-1/first"]
	connection1Second := observations["/connection-1/second"]
	connection2First := observations["/connection-2/first"]
	connection2Second := observations["/connection-2/second"]
	for path, got := range observations {
		if got.proto != "HTTP/2.0" {
			t.Fatalf("request %q protocol = %q, want HTTP/2.0; all observations: %v", path, got.proto, observations)
		}
		if got.remoteAddr == "" {
			t.Fatalf("request %q remote address is empty; all observations: %v", path, observations)
		}
	}
	if connection1First.remoteAddr != connection1Second.remoteAddr {
		t.Fatalf("connection 1 HTTP/2 remote addresses = [%q %q], want streams multiplexed on one transport connection; all observations: %v", connection1First.remoteAddr, connection1Second.remoteAddr, observations)
	}
	if connection2First.remoteAddr != connection2Second.remoteAddr {
		t.Fatalf("connection 2 HTTP/2 remote addresses = [%q %q], want streams multiplexed on one transport connection; all observations: %v", connection2First.remoteAddr, connection2Second.remoteAddr, observations)
	}
	if connection1First.remoteAddr == connection2First.remoteAddr {
		t.Fatalf("HTTP/2 observations = %v, want separate recorded connections to own distinct transport connections", observations)
	}
	if maxInFlight["connection-1"] != 2 || maxInFlight["connection-2"] != 2 {
		t.Fatalf("maximum concurrent streams = %v, want 2 for each recorded HTTP/2 connection", maxInFlight)
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
		{Type: model.EventConnectionOpen, ConnectionID: 1},
		{Type: model.EventRequest, ConnectionID: 1, Sequence: 1, Method: http.MethodGet, Scheme: target.Scheme, Authority: target.Host, Path: "/"},
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
		{Type: model.EventConnectionOpen, ConnectionID: 1},
		{Type: model.EventRequest, ConnectionID: 1, Sequence: 1, Method: http.MethodGet, Scheme: "https", Authority: recordedAuthority, Path: "/", Headers: map[string][]string{"Host": {recordedAuthority}}},
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

func TestConfiguredHostRewrite(t *testing.T) {
	var seenHost string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenHost = r.Host
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	target, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("url.Parse(%q) error: %v", srv.URL, err)
	}

	cfg := config.Default()
	cfg.Target.OverrideURL = srv.URL
	cfg.Header.Set = map[string]string{"Host": "replay.example.com"}
	cfg.Replay.Idempotency.Enabled = false

	eng := New(cfg, metrics.New(cfg.Metrics))
	events := []model.Event{
		{Type: model.EventConnectionOpen, ConnectionID: 1},
		{
			Type:         model.EventRequest,
			ConnectionID: 1,
			Sequence:     1,
			Method:       http.MethodGet,
			Scheme:       target.Scheme,
			Authority:    target.Host,
			Path:         "/",
		},
		{Type: model.EventConnectionClose, ConnectionID: 1},
	}

	summary, err := runReplay(eng, events)
	if err != nil {
		t.Fatalf("runReplay() error: %v", err)
	}
	if got, want := summary.RequestsSent, int64(1); got != want {
		t.Errorf("runReplay().RequestsSent = %d, want %d", got, want)
	}
	if got, want := seenHost, "replay.example.com"; got != want {
		t.Errorf("server request Host = %q, want %q", got, want)
	}
}

func TestSpecialAuthorityHeaderRewrite(t *testing.T) {
	t.Run("normalizes set and drop", func(t *testing.T) {
		cfg := config.Default()
		cfg.Header.Set = map[string]string{":authority": "replay.example.com"}
		eng := New(cfg, metrics.New(cfg.Metrics))
		headers := eng.effectiveRequestHeaders(http.Header{"Host": {"captured.example.com"}})
		if got, want := headers.Get("Host"), "replay.example.com"; got != want {
			t.Fatalf("effective Host = %q, want %q", got, want)
		}
		if _, ok := headers[":authority"]; ok {
			t.Fatalf("effective headers contain ordinary :authority entry: %v", headers)
		}

		cfg.Header.Set = nil
		cfg.Header.Drop = []string{":authority"}
		eng = New(cfg, metrics.New(cfg.Metrics))
		headers = eng.effectiveRequestHeaders(http.Header{"Host": {"captured.example.com"}})
		if got := headers.Get("Host"); got != "" {
			t.Fatalf("effective Host after :authority drop = %q, want empty", got)
		}
	})

	t.Run("sets HTTP2 authority", func(t *testing.T) {
		type observation struct {
			host       string
			protoMajor int
		}
		observed := make(chan observation, 1)
		srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			observed <- observation{host: r.Host, protoMajor: r.ProtoMajor}
			w.WriteHeader(http.StatusOK)
		}))
		srv.EnableHTTP2 = true
		srv.StartTLS()
		defer srv.Close()
		target, err := url.Parse(srv.URL)
		if err != nil {
			t.Fatalf("url.Parse(%q) error: %v", srv.URL, err)
		}

		cfg := config.Default()
		cfg.Target.OverrideURL = srv.URL
		cfg.Header.Set = map[string]string{":authority": "replay.example.com"}
		cfg.Replay.TLS.InsecureSkipVerify = true
		eng := New(cfg, metrics.New(cfg.Metrics))
		client, transport := eng.makePerConnectionClient(true)
		defer transport.CloseIdleConnections()
		requestEvent := model.Event{Protocol: "HTTP/2", Method: http.MethodGet,
			Scheme: target.Scheme, Authority: target.Host, Path: "/"}
		if _, err := eng.executeRequest(
			context.Background(),
			client,
			requestEvent,
			eng.effectiveRequestHeaders(requestEvent.Headers),
		); err != nil {
			t.Fatalf("executeRequest() error: %v", err)
		}
		got := <-observed
		if got.protoMajor != 2 {
			t.Fatalf("server protocol major = %d, want 2", got.protoMajor)
		}
		if want := "replay.example.com"; got.host != want {
			t.Fatalf("server authority = %q, want %q", got.host, want)
		}
	})
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
		{Type: model.EventConnectionOpen, ConnectionID: 1},
		{
			Type:         model.EventRequest,
			ConnectionID: 1,
			Sequence:     1,
			Method:       http.MethodGet,
			Scheme:       "https",
			Authority:    "api.prod.example.com",
			Path:         "/api/v1/login?redirect=/home",
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

func TestBuildRequestURLOverridePreservesCapturedRequestTarget(t *testing.T) {
	tests := []struct {
		name        string
		overrideURL string
		requestPath string
		want        string
	}{
		{
			name:        "ignore override path query and fragment",
			overrideURL: "https://staging.example.com/base?override=1#override",
			requestPath: "/api/v1?captured=1",
			want:        "https://staging.example.com/api/v1?captured=1",
		},
		{
			name:        "preserve encoded path and query",
			overrideURL: "https://staging.example.com/base",
			requestPath: "/files/a%2Fb?next=%2F",
			want:        "https://staging.example.com/files/a%2Fb?next=%2F",
		},
		{
			name:        "preserve empty query marker",
			overrideURL: "https://staging.example.com/base",
			requestPath: "/path?",
			want:        "https://staging.example.com/path?",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := config.Default()
			cfg.Target.OverrideURL = tt.overrideURL
			eng := New(cfg, metrics.New(cfg.Metrics))
			got, err := eng.buildRequestURL(model.Event{Path: tt.requestPath})
			if err != nil {
				t.Fatalf("buildRequestURL(%q) error: %v", tt.requestPath, err)
			}
			if got != tt.want {
				t.Fatalf("buildRequestURL(%q) = %q, want %q", tt.requestPath, got, tt.want)
			}
		})
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

func TestReplayRejectsInvalidOverride(t *testing.T) {
	cfg := config.Default()
	cfg.Target.OverrideURL = "example.com"

	eng := New(cfg, metrics.New(cfg.Metrics))
	events := make(chan model.Event)
	close(events)

	summary, err := eng.ReplayStream(context.Background(), events)
	if err == nil {
		t.Fatal("ReplayStream() error = nil, want invalid target override error")
	}
	if got, want := summary.Outcome, RunFailed; got != want {
		t.Errorf("ReplayStream() outcome = %s, want %s", got, want)
	}
}

func TestReplayStreamDrainsEventsOnInitializationError(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*testing.T, *config.Config)
	}{
		{
			name: "invalid_override",
			configure: func(_ *testing.T, cfg *config.Config) {
				cfg.Target.OverrideURL = "example.com"
			},
		},
		{
			name: "invalid_checkpoint",
			configure: func(t *testing.T, cfg *config.Config) {
				t.Helper()
				checkpointPath := filepath.Join(t.TempDir(), "checkpoint.json")
				if err := os.WriteFile(checkpointPath, []byte("{"), 0o600); err != nil {
					t.Fatalf("os.WriteFile(%q) error: %v", checkpointPath, err)
				}
				cfg.Replay.Checkpoint.File = checkpointPath
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := config.Default()
			tt.configure(t, &cfg)
			eng := New(cfg, metrics.New(cfg.Metrics))

			events := make(chan model.Event)
			senderDone := make(chan struct{})
			go func() {

				close(events)
				close(senderDone)
			}()

			summary, err := eng.ReplayStream(context.Background(), events)
			if err == nil {
				t.Fatal("ReplayStream() error = nil, want initialization error")
			}
			if got, want := summary.Outcome, RunFailed; got != want {
				t.Errorf("ReplayStream() outcome = %s, want %s", got, want)
			}

			select {
			case <-senderDone:
			case <-time.After(time.Second):
				t.Fatal("event sender remained blocked after ReplayStream returned")
			}
		})
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

	events <- model.Event{Type: model.EventConnectionOpen, ConnectionID: 1}
	events <- model.Event{Type: model.EventRequest,
		ConnectionID: 1,
		Sequence:     1, Method: http.MethodGet,
		Scheme:    target.Scheme,
		Authority: target.Host,
		Path:      "/slow"}

	select {
	case <-started:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("request did not start")
	}

	events <- model.Event{Type: model.EventType("unsupported"), ConnectionID: 2}
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

	eng := New(cfg, metrics.New(cfg.Metrics))
	events := []model.Event{
		{Type: model.EventConnectionOpen, ConnectionID: 1},
		{
			Type:         model.EventRequest,
			ConnectionID: 1,
			Sequence:     1,
			Method:       http.MethodGet,
			Scheme:       "http",
			Authority:    "127.0.0.1:1",
			Path:         "/first",
		},
		{
			Type:         model.EventRequest,
			ConnectionID: 1,
			Sequence:     2,
			Method:       http.MethodGet,
			Scheme:       "http",
			Authority:    srv.Listener.Addr().String(),
			Path:         "/second",
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
