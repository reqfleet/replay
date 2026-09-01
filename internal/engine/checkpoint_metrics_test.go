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
	"github.com/reqfleet/replay/config"
	"github.com/reqfleet/replay/internal/metrics"
	"github.com/reqfleet/replay/internal/model"
)

func TestCheckpointPersistsLatestProgressOnInterval(t *testing.T) {
	tmp := t.TempDir()
	ckPath := filepath.Join(tmp, "checkpoint.json")
	legacyTmpPath := ckPath + ".tmp"
	if err := os.WriteFile(legacyTmpPath, []byte("sentinel"), 0o644); err != nil {
		t.Fatalf("os.WriteFile(%q) error: %v", legacyTmpPath, err)
	}
	store, err := newCheckpointStore(ckPath, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("newCheckpointStore(%q) error: %v", ckPath, err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})

	key := model.ConnectionKey{Node: "envoy-a", ConnectionID: 1}
	for sequence := 1; sequence <= 3; sequence++ {
		if err := store.markProcessed(key, sequence); err != nil {
			t.Fatalf("markProcessed(sequence=%d) error: %v", sequence, err)
		}
	}
	waitForCheckpointSequence(t, ckPath, key, 3, time.Second)
	if err := store.Close(); err != nil {
		t.Fatalf("store.Close() error: %v", err)
	}

	reloaded, err := newCheckpointStore(ckPath, time.Hour)
	if err != nil {
		t.Fatalf("reload checkpoint: %v", err)
	}
	t.Cleanup(func() {
		_ = reloaded.Close()
	})
	if !reloaded.alreadyProcessed(key, 3) {
		t.Fatal("reloaded checkpoint does not contain sequence 3")
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

func TestReplayUsesConfiguredCheckpointSyncInterval(t *testing.T) {
	srv := startOKServer()
	defer srv.Close()
	target, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("url.Parse(%q) error: %v", srv.URL, err)
	}

	path := filepath.Join(t.TempDir(), "checkpoint.json")
	cfg := config.Default()

	cfg.Replay.Checkpoint.File = path
	cfg.Replay.Checkpoint.SyncInterval = 10 * time.Millisecond
	eng := New(cfg, metrics.New(cfg.Metrics))

	type streamResult struct {
		summary Summary
		err     error
	}
	events := make(chan model.Event)
	resultCh := make(chan streamResult, 1)
	go func() {
		summary, replayErr := eng.ReplayStream(context.Background(), events)
		resultCh <- streamResult{summary: summary, err: replayErr}
	}()
	var closeEventsOnce sync.Once
	closeEvents := func() {
		closeEventsOnce.Do(func() {
			close(events)
		})
	}
	t.Cleanup(closeEvents)

	key := model.ConnectionKey{Node: "envoy-a", ConnectionID: 1}
	events <- model.Event{Type: model.EventRequest,
		Node:         key.Node,
		ConnectionID: key.ConnectionID,
		Sequence:     1, Method: http.MethodGet,
		Scheme:    target.Scheme,
		Authority: target.Host,
		Path:      "/"}
	waitForCheckpointSequence(t, path, key, 1, time.Second)
	closeEvents()

	select {
	case result := <-resultCh:
		if result.err != nil {
			t.Fatalf("ReplayStream(configured checkpoint interval) error: %v", result.err)
		}
		if got, want := result.summary.RequestsSent, int64(1); got != want {
			t.Errorf("ReplayStream(configured checkpoint interval).RequestsSent = %d, want %d", got, want)
		}
	case <-time.After(time.Second):
		t.Fatal("ReplayStream(configured checkpoint interval) did not finish")
	}
}

func TestCheckpointCloseFlushesLatestInMemoryProgress(t *testing.T) {
	path := filepath.Join(t.TempDir(), "checkpoint.json")
	store, err := newCheckpointStore(path, time.Hour)
	if err != nil {
		t.Fatalf("newCheckpointStore(%q) error: %v", path, err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})

	key := model.ConnectionKey{Node: "envoy-a", ConnectionID: 1}
	if err := store.markProcessed(key, 3); err != nil {
		t.Fatalf("markProcessed(%v, 3) error: %v", key, err)
	}
	if !store.alreadyProcessed(key, 3) {
		t.Fatalf("alreadyProcessed(%v, 3) = false, want true", key)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("os.Stat(%q) error = %v, want not-exist error before periodic sync", path, err)
	}

	if err := store.Close(); err != nil {
		t.Fatalf("store.Close() error: %v", err)
	}
	if got := readCheckpointSequence(t, path, key); got != 3 {
		t.Fatalf("checkpoint sequence after Close() = %d, want 3", got)
	}
}

func TestCheckpointConcurrentMarksPersistOnClose(t *testing.T) {
	const marks = 32

	path := filepath.Join(t.TempDir(), "checkpoint.json")
	store, err := newCheckpointStore(path, time.Hour)
	if err != nil {
		t.Fatalf("newCheckpointStore(%q) error: %v", path, err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})

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
			if !store.alreadyProcessed(key, 1) {
				results <- fmt.Errorf("alreadyProcessed(%v, 1) = false, want true", key)
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
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("os.Stat(%q) error = %v, want not-exist error before Close()", path, err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("store.Close() error: %v", err)
	}
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("os.ReadFile(%q) error: %v", path, err)
	}
	var data checkpointData
	if err := json.Unmarshal(payload, &data); err != nil {
		t.Fatalf("json.Unmarshal(checkpoint) error: %v", err)
	}
	for connectionID := 1; connectionID <= marks; connectionID++ {
		key := model.ConnectionKey{Node: "envoy-a", ConnectionID: connectionID}
		if got := data.Connections[key]; got != 1 {
			t.Errorf("checkpoint sequence for %v = %d, want 1", key, got)
		}
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
			store, err := newCheckpointStore(path, time.Second)
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
	store, err := newCheckpointStore(path, time.Hour)
	if err != nil {
		t.Fatalf("newCheckpointStore(%q) error: %v", path, err)
	}
	if err := os.WriteFile(parent, []byte("blocking file"), 0o644); err != nil {
		t.Fatalf("os.WriteFile(%q) error: %v", parent, err)
	}

	key := model.ConnectionKey{Node: "envoy-a", ConnectionID: 1}
	if err := store.markProcessed(key, 1); err != nil {
		t.Fatalf("markProcessed(%v, 1) error: %v", key, err)
	}
	closeErr := store.Close()
	if closeErr == nil {
		t.Fatal("store.Close() error = nil, want persistence failure")
	}
	if secondErr := store.Close(); secondErr == nil || secondErr.Error() != closeErr.Error() {
		t.Fatalf("second store.Close() error = %v, want first error %v", secondErr, closeErr)
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

	first, err := newCheckpointStore(firstPath, time.Hour)
	if err != nil {
		t.Fatalf("newCheckpointStore(first) error: %v", err)
	}
	t.Cleanup(func() {
		_ = first.Close()
	})
	second, err := newCheckpointStore(secondPath, time.Hour)
	if err != nil {
		t.Fatalf("newCheckpointStore(second) error: %v", err)
	}
	t.Cleanup(func() {
		_ = second.Close()
	})
	firstKey := model.ConnectionKey{Node: "envoy-a", ConnectionID: 1}
	secondKey := model.ConnectionKey{Node: "envoy-b", ConnectionID: 2}
	if err := first.markProcessed(firstKey, 3); err != nil {
		t.Fatalf("first.markProcessed() error: %v", err)
	}
	if err := second.markProcessed(secondKey, 7); err != nil {
		t.Fatalf("second.markProcessed() error: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("first.Close() error: %v", err)
	}
	if err := second.Close(); err != nil {
		t.Fatalf("second.Close() error: %v", err)
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

func waitForCheckpointSequence(t *testing.T, path string, key model.ConnectionKey, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var got int
	for time.Now().Before(deadline) {
		payload, err := os.ReadFile(path)
		if err == nil {
			var data checkpointData
			if err := json.Unmarshal(payload, &data); err != nil {
				t.Fatalf("json.Unmarshal(%q) error: %v", path, err)
			}
			got = data.Connections[key]
			if got == want {
				return
			}
		} else if !os.IsNotExist(err) {
			t.Fatalf("os.ReadFile(%q) error: %v", path, err)
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("checkpoint sequence after %s = %d, want %d", timeout, got, want)
}

func TestCheckpointNotWrittenInDryRun(t *testing.T) {
	ckPath := filepath.Join(t.TempDir(), "checkpoint.json")
	cfg := config.Default()
	cfg.Replay.DryRun = true
	cfg.Replay.Checkpoint.File = ckPath
	e := New(cfg, metrics.New(cfg.Metrics))

	req := model.Event{
		Type:         model.EventRequest,
		Node:         "envoy-b",
		ConnectionID: 2,
		Sequence:     1,
		Method:       http.MethodGet,
		Scheme:       "http",
		Authority:    "example.invalid",
		Path:         "/",
	}
	summary, err := runReplay(e, []model.Event{req})
	if err != nil {
		t.Fatalf("runReplay(dry run) error: %v", err)
	}
	if got, want := summary.Skipped, int64(1); got != want {
		t.Fatalf("summary.Skipped = %d, want %d", got, want)
	}
	if _, err := os.Stat(ckPath); !os.IsNotExist(err) {
		t.Fatalf("os.Stat(%q) error = %v, want not-exist error", ckPath, err)
	}
}

func TestCheckpointWrittenOnIdempotencySkip(t *testing.T) {
	ckPath := filepath.Join(t.TempDir(), "checkpoint.json")
	cfg := config.Default()
	cfg.Replay.Idempotency.Enabled = true
	cfg.Replay.Idempotency.BlockMethods = []string{"POST"}
	cfg.Replay.Idempotency.RequireHeaderForAllow = []string{"x-idempotency-key"}
	cfg.Replay.Checkpoint.File = ckPath
	e := New(cfg, metrics.New(cfg.Metrics))

	req := model.Event{
		Type:         model.EventRequest,
		Node:         "envoy-b",
		ConnectionID: 42,
		Sequence:     42,
		Scheme:       "http",
		Authority:    "example.invalid",
		Path:         "/",
		Headers:      map[string][]string{"content-type": {"text/plain"}},
	}
	summary, err := runReplay(e, []model.Event{req})
	if err != nil {
		t.Fatalf("runReplay(idempotency skip) error: %v", err)
	}
	if got, want := summary.Skipped, int64(1); got != want {
		t.Fatalf("summary.Skipped = %d, want %d", got, want)
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
