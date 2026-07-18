package engine

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/reqfleet/replay/internal/config"
	"github.com/reqfleet/replay/internal/metrics"
	"github.com/reqfleet/replay/internal/model"
)

func TestOutcomeAggregation_DryRunSerialized(t *testing.T) {
	cfg := config.Default()
	cfg.Replay.DryRun = true
	cfg.Replay.Lifecycle.RequireOpen = false

	reg := metrics.New(cfg.Metrics)
	e := New(cfg, reg)

	requests := []model.Event{
		{
			Type:         model.EventRequest,
			ConnectionID: 1,
			Sequence:     1,
			HTTP:         model.HTTPRequestMeta{Path: "/"},
		},
		{
			Type:         model.EventRequest,
			ConnectionID: 1,
			Sequence:     2,
			HTTP:         model.HTTPRequestMeta{Path: "/ok"},
		},
	}

	client, transport := e.makePerConnectionClient(false)
	defer transport.CloseIdleConnections()
	summary := e.replayConnectionSerialized(context.Background(), client, requests, nil, nil)

	if got, want := summary.Skipped, int64(2); got != want {
		t.Errorf("summary.Skipped = %v, want %v", got, want)
	}
	if got, want := summary.RequestsSent, int64(0); got != want {
		t.Errorf("summary.RequestsSent = %v, want %v", got, want)
	}
	if got := len(summary.RequestResults); got != 0 {
		t.Fatalf("len(summary.RequestResults) = %v, want 0", got)
	}
	if got, want := len(summary.ConnectionResults), 1; got != want {
		t.Fatalf("len(summary.ConnectionResults) = %v, want %v", got, want)
	}
	conn := summary.ConnectionResults[0]
	if conn.ConnectionID != 1 {
		t.Errorf("ConnectionResults[0].ConnectionID = %v, want %v", conn.ConnectionID, 1)
	}
	if got := len(conn.Requests); got != 0 {
		t.Errorf("conn.Requests len = %v, want 0", got)
	}
	if got, want := conn.Skipped, int64(2); got != want {
		t.Errorf("conn.Skipped = %v, want %v", got, want)
	}
	if conn.Outcome != ConnectionCompleted {
		t.Errorf("conn.Outcome = %v, want %v", conn.Outcome, ConnectionCompleted)
	}
}

func TestOutcomeAggregation_ValidationFailure(t *testing.T) {
	cfg := config.Default()
	cfg.Replay.Lifecycle.RequireOpen = false

	cfg.Replay.Validation.Status = true

	reg := metrics.New(cfg.Metrics)
	e := New(cfg, reg)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
		_, _ = w.Write([]byte("internal error"))
	}))
	defer srv.Close()
	u, _ := url.Parse(srv.URL)
	authority := u.Host

	requests := []model.Event{
		{
			Type:         model.EventRequest,
			ConnectionID: 1,
			Sequence:     1,
			HTTP:         model.HTTPRequestMeta{Scheme: "http", Authority: authority, Path: "/"},
		},
	}
	expected := map[int]model.Event{
		1: {Type: model.EventResponse, ConnectionID: 1, Sequence: 1, Status: 200},
	}

	client, transport := e.makePerConnectionClient(false)
	defer transport.CloseIdleConnections()
	summary := e.replayConnectionSerialized(context.Background(), client, requests, expected, nil)

	if got, want := summary.ValidationFailed, int64(1); got != want {
		t.Errorf("summary.ValidationFailed = %v, want %v", got, want)
	}
	if got := len(summary.RequestResults); got != 0 {
		t.Fatalf("len(summary.RequestResults) = %v, want 0", got)
	}
	if got, want := len(summary.ConnectionResults), 1; got != want {
		t.Fatalf("len(summary.ConnectionResults) = %v, want %v", got, want)
	}
	if got, want := summary.ConnectionResults[0].ValidationFailed, int64(1); got != want {
		t.Errorf("ConnectionResults[0].ValidationFailed = %v, want %v", got, want)
	}
}

func TestOutcomeAggregation_UnmatchedResponseSerialized(t *testing.T) {
	cfg := config.Default()
	cfg.Replay.Lifecycle.RequireOpen = false

	cfg.Replay.Validation.Status = true

	reg := metrics.New(cfg.Metrics)
	e := New(cfg, reg)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	u, _ := url.Parse(srv.URL)

	requests := []model.Event{
		{
			Type:         model.EventRequest,
			ConnectionID: 1,
			Sequence:     1,
			HTTP:         model.HTTPRequestMeta{Scheme: "http", Authority: u.Host, Path: "/"},
		},
	}
	expected := map[int]model.Event{
		2: {Type: model.EventResponse, ConnectionID: 1, Sequence: 2, Status: http.StatusOK},
	}

	client, transport := e.makePerConnectionClient(false)
	defer transport.CloseIdleConnections()
	summary := e.replayConnectionSerialized(context.Background(), client, requests, expected, nil)

	if got, want := summary.ValidationFailed, int64(1); got != want {
		t.Errorf("summary.ValidationFailed = %v, want %v", got, want)
	}
	if got, want := summary.Outcome, RunPartialSuccess; got != want {
		t.Errorf("summary.Outcome = %v, want %v", got, want)
	}
	if got, want := summary.ConnectionResults[0].ValidationFailed, int64(1); got != want {
		t.Errorf("ConnectionResults[0].ValidationFailed = %v, want %v", got, want)
	}
}

func TestOutcomeAggregation_SkippedSerializedResponseValidation(t *testing.T) {
	cfg := config.Default()
	cfg.Replay.DryRun = true
	cfg.Replay.Lifecycle.RequireOpen = false

	cfg.Replay.Validation.Status = true

	reg := metrics.New(cfg.Metrics)
	e := New(cfg, reg)

	requests := []model.Event{
		{
			Type:         model.EventRequest,
			ConnectionID: 1,
			Sequence:     1,
			HTTP:         model.HTTPRequestMeta{Scheme: "http", Authority: "example.invalid", Path: "/"},
		},
	}
	expected := map[int]model.Event{
		1: {Type: model.EventResponse, ConnectionID: 1, Sequence: 1, Status: http.StatusOK},
	}

	client, transport := e.makePerConnectionClient(false)
	defer transport.CloseIdleConnections()
	summary := e.replayConnectionSerialized(context.Background(), client, requests, expected, nil)

	if got, want := summary.Skipped, int64(1); got != want {
		t.Errorf("summary.Skipped = %v, want %v", got, want)
	}
	if got, want := summary.ValidationFailed, int64(0); got != want {
		t.Errorf("summary.ValidationFailed = %v, want %v", got, want)
	}
}

func TestOutcomeAggregation_SendError(t *testing.T) {
	cfg := config.Default()
	cfg.Replay.Lifecycle.RequireOpen = false

	reg := metrics.New(cfg.Metrics)
	e := New(cfg, reg)

	// Missing path will cause buildRequestURL to error and be treated as send error
	bad := model.Event{
		Type:         model.EventRequest,
		ConnectionID: 2,
		Sequence:     1,
		HTTP:         model.HTTPRequestMeta{Scheme: "http", Authority: "example.invalid", Path: ""},
	}

	client, transport := e.makePerConnectionClient(false)
	defer transport.CloseIdleConnections()
	summary := e.replayConnectionSerialized(context.Background(), client, []model.Event{bad}, nil, nil)

	if got, want := summary.SendErrors, int64(1); got != want {
		t.Errorf("summary.SendErrors = %v, want %v", got, want)
	}
	if got, want := summary.ConnectionsAborted, int64(1); got != want {
		t.Errorf("summary.ConnectionsAborted = %v, want %v", got, want)
	}
	if got := len(summary.RequestResults); got != 0 {
		t.Fatalf("len(summary.RequestResults) = %v, want 0", got)
	}
	if got, want := len(summary.ConnectionResults), 1; got != want {
		t.Fatalf("len(summary.ConnectionResults) = %v, want %v", got, want)
	}
	if got, want := summary.ConnectionResults[0].SendErrors, int64(1); got != want {
		t.Errorf("ConnectionResults[0].SendErrors = %v, want %v", got, want)
	}
}

func TestOutcomeAggregation_HTTP2Multiplexed(t *testing.T) {
	cfg := config.Default()
	cfg.Replay.Lifecycle.RequireOpen = false
	cfg.Replay.HTTP2.Mode = "multiplexed"

	reg := metrics.New(cfg.Metrics)
	e := New(cfg, reg)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()
	u, _ := url.Parse(srv.URL)
	authority := u.Host

	requests := []model.Event{
		{Type: model.EventRequest, ConnectionID: 3, StreamID: 1, Sequence: 1, HTTP: model.HTTPRequestMeta{Version: "HTTP/2", Scheme: "http", Authority: authority, Path: "/s1"}},
		{Type: model.EventRequest, ConnectionID: 3, StreamID: 2, Sequence: 1, HTTP: model.HTTPRequestMeta{Version: "HTTP/2", Scheme: "http", Authority: authority, Path: "/s2"}},
	}

	summary := e.replayConnectionWithCheckpoint(context.Background(), requests, nil, nil)

	if got, want := summary.RequestsSent, int64(2); got != want {
		t.Errorf("summary.RequestsSent = %v, want %v", got, want)
	}
	if got := len(summary.RequestResults); got != 0 {
		t.Fatalf("len(summary.RequestResults) = %v, want 0", got)
	}
	if len(summary.ConnectionResults) != 1 {
		t.Fatalf("len(summary.ConnectionResults) = %v, want %v", len(summary.ConnectionResults), 1)
	}
	if got := len(summary.ConnectionResults[0].Requests); got != 0 {
		t.Errorf("connection requests = %v, want 0", got)
	}
	if got, want := summary.ConnectionResults[0].RequestsSent, int64(2); got != want {
		t.Errorf("ConnectionResults[0].RequestsSent = %v, want %v", got, want)
	}
}
