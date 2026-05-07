package engine

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/reqfleet/replay/internal/config"
	"github.com/reqfleet/replay/internal/metrics"
	"github.com/reqfleet/replay/internal/model"
)

func TestCheckpointWrittenOnSuccess(t *testing.T) {
	cfg := config.Default()
	cfg.Replay.Lifecycle.RequireOpen = false
	cfg.Replay.Lifecycle.RequireClose = false

	reg := metrics.New()
	e := New(cfg, reg)

	srv := startOKServer()
	defer srv.Close()

	u := srv.URL
	// extract host part
	host := u[len("http://"):]

	req := model.Event{Type: model.EventRequest, Node: "envoy-a", ConnectionID: 1, Sequence: 1, HTTP: model.HTTPRequestMeta{Scheme: "http", Authority: host, Path: "/"}}

	tmp := t.TempDir()
	ckPath := filepath.Join(tmp, "checkpoint.json")
	store, err := newCheckpointStore(ckPath)
	if err != nil {
		t.Fatalf("new checkpoint store: %v", err)
	}

	summary := e.replayConnectionSerialized(context.Background(), e.client, []model.Event{req}, nil, store)
	if summary.RequestsSent != 1 {
		t.Fatalf("expect 1 request sent, got %d", summary.RequestsSent)
	}

	b, err := os.ReadFile(ckPath)
	if err != nil {
		t.Fatalf("read checkpoint: %v", err)
	}
	var data struct {
		Connections map[model.ConnectionKey]int `json:"connections"`
	}
	if err := json.Unmarshal(b, &data); err != nil {
		t.Fatalf("unmarshal checkpoint: %v", err)
	}
	if got := data.Connections[model.ConnectionKey{Node: "envoy-a", ConnectionID: 1}]; got != 1 {
		t.Fatalf("checkpoint for envoy-a/1 = %v, want 1", got)
	}
}

func TestCheckpointNotWrittenInDryRun(t *testing.T) {
	cfg := config.Default()
	cfg.Replay.Lifecycle.RequireOpen = false
	cfg.Replay.Lifecycle.RequireClose = false
	cfg.Replay.DryRun = true

	reg := metrics.New()
	e := New(cfg, reg)

	srv := startOKServer()
	defer srv.Close()
	u := srv.URL
	host := u[len("http://"):]

	req := model.Event{Type: model.EventRequest, Node: "envoy-b", ConnectionID: 2, Sequence: 1, HTTP: model.HTTPRequestMeta{Scheme: "http", Authority: host, Path: "/"}}

	tmp := t.TempDir()
	ckPath := filepath.Join(tmp, "checkpoint.json")
	store, err := newCheckpointStore(ckPath)
	if err != nil {
		t.Fatalf("new checkpoint store: %v", err)
	}

	summary := e.replayConnectionSerialized(context.Background(), e.client, []model.Event{req}, nil, store)
	if summary.Skipped == 0 {
		t.Fatalf("expected skipped > 0 for dry run")
	}
	if _, err := os.Stat(ckPath); !os.IsNotExist(err) {
		if err == nil {
			t.Fatalf("expected no checkpoint file written in dry run")
		}
		t.Fatalf("unexpected stat error: %v", err)
	}
}

func TestCheckpointWrittenOnIdempotencySkip(t *testing.T) {
	cfg := config.Default()
	cfg.Replay.Lifecycle.RequireOpen = false
	cfg.Replay.Lifecycle.RequireClose = false
	cfg.Replay.Idempotency.Enabled = true
	cfg.Replay.Idempotency.BlockMethods = []string{"POST"}
	cfg.Replay.Idempotency.RequireHeaderForAllow = []string{"x-idempotency-key"}

	reg := metrics.New()
	e := New(cfg, reg)

	// no server needed because idempotency skip happens before network
	req := model.Event{Type: model.EventRequest, Node: "envoy-b", ConnectionID: 42, Sequence: 42, HTTP: model.HTTPRequestMeta{Scheme: "http", Authority: "example.invalid", Path: "/"}, Headers: map[string][]string{"content-type": {"text/plain"}}}

	tmp := t.TempDir()
	ckPath := filepath.Join(tmp, "checkpoint.json")
	store, err := newCheckpointStore(ckPath)
	if err != nil {
		t.Fatalf("new checkpoint store: %v", err)
	}

	summary := e.replayConnectionSerialized(context.Background(), e.client, []model.Event{req}, nil, store)
	if summary.Skipped != 1 {
		t.Fatalf("expected 1 skipped, got %d", summary.Skipped)
	}
	b, err := os.ReadFile(ckPath)
	if err != nil {
		t.Fatalf("read checkpoint: %v", err)
	}
	var data struct {
		Connections map[model.ConnectionKey]int `json:"connections"`
	}
	if err := json.Unmarshal(b, &data); err != nil {
		t.Fatalf("unmarshal checkpoint: %v", err)
	}
	if got := data.Connections[model.ConnectionKey{Node: "envoy-b", ConnectionID: 42}]; got != 42 {
		t.Fatalf("checkpoint for envoy-b/42 = %v, want 42", got)
	}
}

func TestRuntimeMetricsCollector(t *testing.T) {
	reg := metrics.New()
	labels := config.Default().Labels
	reg.SeedEngineLabels(labels)
	stop := reg.StartRuntimeCollection(labels, 50*time.Millisecond)
	defer stop()

	time.Sleep(150 * time.Millisecond)

	g := reg.ThreadsGauge.WithLabelValues(labels.CollectionID, labels.PlanID, labels.RunID, labels.EngineNo, labels.Zone)
	v := testutil.ToFloat64(g)
	if v <= 0 {
		t.Fatalf("threads gauge = %v, want > 0", v)
	}
}

// startOKServer returns an httptest server that returns 200 OK on any request.
func startOKServer() *httptest.Server {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte("ok"))
	}))
	return srv
}
