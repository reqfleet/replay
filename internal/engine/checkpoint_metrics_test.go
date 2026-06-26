package engine

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
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

	reg := metrics.New(cfg.Metrics)
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

	client, transport := e.makePerConnectionClient(false)
	defer transport.CloseIdleConnections()
	summary := e.replayConnectionSerialized(context.Background(), client, []model.Event{req}, nil, store)
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

func TestCheckpointFlushBatchesUntilClose(t *testing.T) {
	tmp := t.TempDir()
	ckPath := filepath.Join(tmp, "checkpoint.json")
	store := newCheckpointStoreWithInterval(ckPath, 24*time.Hour)
	store.wg.Go(store.flusher)

	key := model.ConnectionKey{Node: "envoy-a", ConnectionID: 1}
	if err := store.markProcessed(key, 1); err != nil {
		t.Fatalf("mark initial checkpoint: %v", err)
	}
	if got := readCheckpointSequence(t, ckPath, key); got != 1 {
		t.Fatalf("checkpoint sequence after initial write = %d, want 1", got)
	}

	if err := store.markProcessed(key, 2); err != nil {
		t.Fatalf("mark second checkpoint: %v", err)
	}
	if err := store.markProcessed(key, 3); err != nil {
		t.Fatalf("mark third checkpoint: %v", err)
	}
	if got := readCheckpointSequence(t, ckPath, key); got != 1 {
		t.Fatalf("checkpoint sequence before batch flush = %d, want 1", got)
	}

	if err := store.Close(); err != nil {
		t.Fatalf("close checkpoint store: %v", err)
	}
	if got := readCheckpointSequence(t, ckPath, key); got != 3 {
		t.Fatalf("checkpoint sequence after final flush = %d, want 3", got)
	}
}

func readCheckpointSequence(t *testing.T, path string, key model.ConnectionKey) int {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read checkpoint: %v", err)
	}
	var data struct {
		Connections map[model.ConnectionKey]int `json:"connections"`
	}
	if err := json.Unmarshal(b, &data); err != nil {
		t.Fatalf("unmarshal checkpoint: %v", err)
	}
	return data.Connections[key]
}

func TestCheckpointNotWrittenInDryRun(t *testing.T) {
	cfg := config.Default()
	cfg.Replay.Lifecycle.RequireOpen = false
	cfg.Replay.DryRun = true

	reg := metrics.New(cfg.Metrics)
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

	client, transport := e.makePerConnectionClient(false)
	defer transport.CloseIdleConnections()
	summary := e.replayConnectionSerialized(context.Background(), client, []model.Event{req}, nil, store)
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
	cfg.Replay.Idempotency.Enabled = true
	cfg.Replay.Idempotency.BlockMethods = []string{"POST"}
	cfg.Replay.Idempotency.RequireHeaderForAllow = []string{"x-idempotency-key"}

	reg := metrics.New(cfg.Metrics)
	e := New(cfg, reg)

	// no server needed because idempotency skip happens before network
	req := model.Event{Type: model.EventRequest, Node: "envoy-b", ConnectionID: 42, Sequence: 42, HTTP: model.HTTPRequestMeta{Scheme: "http", Authority: "example.invalid", Path: "/"}, Headers: map[string][]string{"content-type": {"text/plain"}}}

	tmp := t.TempDir()
	ckPath := filepath.Join(tmp, "checkpoint.json")
	store, err := newCheckpointStore(ckPath)
	if err != nil {
		t.Fatalf("new checkpoint store: %v", err)
	}

	client, transport := e.makePerConnectionClient(false)
	defer transport.CloseIdleConnections()
	summary := e.replayConnectionSerialized(context.Background(), client, []model.Event{req}, nil, store)
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

func TestWorkerClientCreationUpdatesThreadsGauge(t *testing.T) {
	cfg := config.Default()
	cfg.Replay.MaxVirtualUsersPerEngine = 3
	reg := metrics.New(cfg.Metrics)
	metricLabelValues := cfg.Metrics.CommonLabelValues()
	reg.SeedEngineLabels(metricLabelValues)

	eng := New(cfg, reg)
	ctx := t.Context()

	vus := cfg.Replay.MaxVirtualUsersPerEngine
	workerChs := make([]chan model.Event, vus)
	for i := range workerChs {
		workerChs[i] = make(chan model.Event, 1)
	}
	connSem := make(chan struct{}, cfg.Replay.MaxActiveConnectionsPerEngine)
	results := make(chan Summary, vus)
	var wg sync.WaitGroup
	for i := range workerChs {
		ch := workerChs[i]
		wg.Go(func() {
			results <- eng.runEventWorker(ctx, ch, 0, nil, connSem)
		})
	}

	want := float64(cfg.Replay.MaxVirtualUsersPerEngine)
	deadline := time.After(time.Second)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		g := reg.ThreadsGauge.WithLabelValues(metricLabelValues...)
		if v := testutil.ToFloat64(g); v == want {
			break
		}

		select {
		case <-deadline:
			t.Fatalf("threads gauge did not reach %v", want)
		case <-ticker.C:
		}
	}

	for _, ch := range workerChs {
		close(ch)
	}
	wg.Wait()
}

// startOKServer returns an httptest server that returns 200 OK on any request.
func startOKServer() *httptest.Server {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte("ok"))
	}))
	return srv
}
