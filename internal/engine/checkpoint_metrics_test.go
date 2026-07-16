package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
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

func TestCheckpointPersistsEveryAdvance(t *testing.T) {
	tmp := t.TempDir()
	ckPath := filepath.Join(tmp, "checkpoint.json")
	legacyTmpPath := ckPath + ".tmp"
	if err := os.WriteFile(legacyTmpPath, []byte("sentinel"), 0o644); err != nil {
		t.Fatalf("os.WriteFile(%q) error: %v", legacyTmpPath, err)
	}
	store, err := newCheckpointStore(ckPath)
	if err != nil {
		t.Fatalf("newCheckpointStore(%q) error: %v", ckPath, err)
	}

	key := model.ConnectionKey{Node: "envoy-a", ConnectionID: 1}
	for sequence := 1; sequence <= 3; sequence++ {
		if err := store.markProcessed(key, sequence); err != nil {
			t.Fatalf("markProcessed(sequence=%d) error: %v", sequence, err)
		}
		if got := readCheckpointSequence(t, ckPath, key); got != sequence {
			t.Fatalf("checkpoint sequence after mark %d = %d", sequence, got)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatalf("store.Close() error: %v", err)
	}

	reloaded, err := newCheckpointStore(ckPath)
	if err != nil {
		t.Fatalf("reload checkpoint: %v", err)
	}
	if !reloaded.alreadyProcessed(key, 3) {
		t.Fatal("reloaded checkpoint does not contain acknowledged sequence 3")
	}
	if err := reloaded.Close(); err != nil {
		t.Fatalf("reloaded.Close() error: %v", err)
	}
	if got, err := os.ReadFile(legacyTmpPath); err != nil || string(got) != "sentinel" {
		t.Fatalf("legacy temp sentinel = %q, %v; want untouched", got, err)
	}
	entries, err := os.ReadDir(tmp)
	if err != nil {
		t.Fatalf("os.ReadDir(%q) error: %v", tmp, err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".checkpoint.json.tmp-") {
			t.Fatalf("temporary checkpoint file was not cleaned up: %s", entry.Name())
		}
	}
}

func TestCheckpointConcurrentMarksAreDurableOnReturn(t *testing.T) {
	const marks = 32

	path := filepath.Join(t.TempDir(), "checkpoint.json")
	store, err := newCheckpointStore(path)
	if err != nil {
		t.Fatalf("newCheckpointStore(%q) error: %v", path, err)
	}

	start := make(chan struct{})
	results := make(chan error, marks)
	var wg sync.WaitGroup
	wg.Add(marks)
	for connectionID := 1; connectionID <= marks; connectionID++ {
		go func() {
			defer wg.Done()
			<-start

			key := model.ConnectionKey{Node: "envoy-a", ConnectionID: connectionID}
			if err := store.markProcessed(key, 1); err != nil {
				results <- fmt.Errorf("markProcessed(%v, 1): %w", key, err)
				return
			}
			payload, err := os.ReadFile(path)
			if err != nil {
				results <- fmt.Errorf("os.ReadFile(%q) after markProcessed(%v, 1): %w", path, key, err)
				return
			}
			var data checkpointData
			if err := json.Unmarshal(payload, &data); err != nil {
				results <- fmt.Errorf("json.Unmarshal(checkpoint) after markProcessed(%v, 1): %w", key, err)
				return
			}
			if got := data.Connections[key]; got != 1 {
				results <- fmt.Errorf("checkpoint sequence after markProcessed(%v, 1) = %d, want 1", key, got)
				return
			}
			results <- nil
		}()
	}
	close(start)
	wg.Wait()
	close(results)

	for err := range results {
		if err != nil {
			t.Error(err)
		}
	}
	if err := store.Close(); err != nil {
		t.Errorf("store.Close() error: %v", err)
	}
}

func TestCheckpointRejectsUnsupportedVersions(t *testing.T) {
	tests := []struct {
		name    string
		version int
		wantErr bool
	}{
		{name: "current", version: currentCheckpointVersion},
		{name: "missing", version: 0, wantErr: true},
		{name: "future", version: currentCheckpointVersion + 1, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "checkpoint.json")
			payload := fmt.Sprintf(`{"version":%d,"connections":{}}`, tt.version)
			if err := os.WriteFile(path, []byte(payload), 0o644); err != nil {
				t.Fatalf("os.WriteFile(%q) error: %v", path, err)
			}
			store, err := newCheckpointStore(path)
			if tt.wantErr {
				if err == nil || !strings.Contains(err.Error(), "unsupported checkpoint version") {
					t.Fatalf("newCheckpointStore(version=%d) error = %v", tt.version, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("newCheckpointStore(version=%d) error: %v", tt.version, err)
			}
			if err := store.Close(); err != nil {
				t.Fatalf("store.Close() error: %v", err)
			}
		})
	}
}

func TestCheckpointCloseReturnsPersistenceFailure(t *testing.T) {
	tmp := t.TempDir()
	parent := filepath.Join(tmp, "not-a-directory")
	path := filepath.Join(parent, "checkpoint.json")
	store, err := newCheckpointStore(path)
	if err != nil {
		t.Fatalf("newCheckpointStore(%q) error: %v", path, err)
	}
	if err := os.WriteFile(parent, []byte("blocking file"), 0o644); err != nil {
		t.Fatalf("os.WriteFile(%q) error: %v", parent, err)
	}

	key := model.ConnectionKey{Node: "envoy-a", ConnectionID: 1}
	markErr := store.markProcessed(key, 1)
	if markErr == nil {
		t.Fatal("markProcessed() error = nil, want persistence failure")
	}
	closeErr := store.Close()
	if closeErr == nil || closeErr.Error() != markErr.Error() {
		t.Fatalf("store.Close() error = %v, want first error %v", closeErr, markErr)
	}
	if secondErr := store.Close(); secondErr == nil || secondErr.Error() != markErr.Error() {
		t.Fatalf("second store.Close() error = %v, want first error %v", secondErr, markErr)
	}
}

func TestCheckpointPathsAreIsolatedByShard(t *testing.T) {
	base := filepath.Join(t.TempDir(), "checkpoint.json")
	firstPath := checkpointPath(base, 0, 2)
	secondPath := checkpointPath(base, 1, 2)
	if firstPath == secondPath {
		t.Fatalf("checkpoint paths are equal: %q", firstPath)
	}
	if !strings.Contains(firstPath, "shard-0-of-2") || !strings.Contains(secondPath, "shard-1-of-2") {
		t.Fatalf("shard checkpoint paths = %q, %q", firstPath, secondPath)
	}

	first, err := newCheckpointStore(firstPath)
	if err != nil {
		t.Fatalf("newCheckpointStore(first) error: %v", err)
	}
	second, err := newCheckpointStore(secondPath)
	if err != nil {
		t.Fatalf("newCheckpointStore(second) error: %v", err)
	}
	firstKey := model.ConnectionKey{Node: "envoy-a", ConnectionID: 1}
	secondKey := model.ConnectionKey{Node: "envoy-b", ConnectionID: 2}
	if err := first.markProcessed(firstKey, 3); err != nil {
		t.Fatalf("first.markProcessed() error: %v", err)
	}
	if err := second.markProcessed(secondKey, 7); err != nil {
		t.Fatalf("second.markProcessed() error: %v", err)
	}
	if got := readCheckpointSequence(t, firstPath, firstKey); got != 3 {
		t.Fatalf("first shard sequence = %d, want 3", got)
	}
	if got := readCheckpointSequence(t, secondPath, secondKey); got != 7 {
		t.Fatalf("second shard sequence = %d, want 7", got)
	}
}

func TestReplayStreamReturnsCheckpointPersistenceFailure(t *testing.T) {
	checkpointDir := filepath.Join(t.TempDir(), "checkpoint")
	if err := os.MkdirAll(checkpointDir, 0o755); err != nil {
		t.Fatalf("os.MkdirAll(%q) error: %v", checkpointDir, err)
	}
	sabotageErr := make(chan error, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := os.RemoveAll(checkpointDir); err != nil {
			sabotageErr <- err
			return
		}
		if err := os.WriteFile(checkpointDir, []byte("blocking file"), 0o644); err != nil {
			sabotageErr <- err
			return
		}
		sabotageErr <- nil
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	target, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("url.Parse(%q) error: %v", srv.URL, err)
	}

	cfg := config.Default()
	cfg.Replay.Checkpoint.File = filepath.Join(checkpointDir, "state.json")
	eng := New(cfg, metrics.New(cfg.Metrics))
	events := []model.Event{
		{Type: model.EventMeta},
		{Type: model.EventConnectionOpen, ConnectionID: 1},
		{
			Type:         model.EventRequest,
			ConnectionID: 1,
			Sequence:     1,
			HTTP: model.HTTPRequestMeta{
				Method: http.MethodGet, Scheme: target.Scheme, Authority: target.Host, Path: "/",
			},
		},
		{Type: model.EventConnectionClose, ConnectionID: 1},
	}
	summary, replayErr := runReplay(eng, events)
	if err := <-sabotageErr; err != nil {
		t.Fatalf("checkpoint sabotage error: %v", err)
	}
	if replayErr == nil || !strings.Contains(replayErr.Error(), "persist checkpoint") {
		t.Fatalf("runReplay() error = %v, want checkpoint persistence error", replayErr)
	}
	if got, want := summary.Outcome, RunFailed; got != want {
		t.Fatalf("summary.Outcome = %s, want %s", got, want)
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

func TestActiveVirtualUserGaugeTracksWorkerLifecycle(t *testing.T) {
	cfg := config.Default()
	cfg.Replay.MaxVirtualUsersPerEngine = 3
	reg := metrics.New(cfg.Metrics)
	metricLabelValues := cfg.Metrics.CommonLabelValues()
	reg.SeedEngineLabels(metricLabelValues)

	requestStarted := make(chan struct{}, 3)
	releaseResponse := make(chan struct{})
	var releaseResponseOnce sync.Once
	release := func() {
		releaseResponseOnce.Do(func() {
			close(releaseResponse)
		})
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestStarted <- struct{}{}
		<-releaseResponse
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer func() {
		release()
		srv.Close()
	}()

	target, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("url.Parse(%q) error: %v", srv.URL, err)
	}

	eng := New(cfg, reg)
	resultCh := runReplayAsync(eng, rampupTestEvents(target, 3))
	waitForRequestStarts(t, requestStarted, 3, time.Second)
	waitForThreadsGauge(t, reg, metricLabelValues, 3)

	release()
	select {
	case result := <-resultCh:
		if result.err != nil {
			t.Fatalf("ReplayStream(active VU gauge test) error: %v", result.err)
		}
		if got, want := result.summary.ConnectionsDone, int64(3); got != want {
			t.Errorf("ReplayStream(active VU gauge test).ConnectionsDone = %d, want %d", got, want)
		}
	case <-time.After(time.Second):
		t.Fatal("ReplayStream(active VU gauge test) did not finish")
	}
	waitForThreadsGauge(t, reg, metricLabelValues, 0)
}

func waitForThreadsGauge(t *testing.T, reg *metrics.Registry, commonLabelValues []string, want float64) {
	t.Helper()

	deadline := time.After(time.Second)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	var got float64
	for {
		g := reg.ThreadsGauge.WithLabelValues(commonLabelValues...)
		got = testutil.ToFloat64(g)
		if got == want {
			return
		}

		select {
		case <-deadline:
			t.Fatalf("threads gauge = %v, want %v", got, want)
		case <-ticker.C:
		}
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
