package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/reqfleet/replay/internal/config"
	"github.com/reqfleet/replay/internal/engine"
	"github.com/reqfleet/replay/internal/metrics"
)

func TestExitCodeForSummary(t *testing.T) {
	tests := []struct {
		name    string
		outcome engine.RunOutcome
		adjust  func(*config.Config)
		want    int
	}{
		{
			name:    "success",
			outcome: engine.RunSuccess,
			want:    0,
		},
		{
			name:    "partial_success_default_zero",
			outcome: engine.RunPartialSuccess,
			want:    0,
		},
		{
			name:    "partial_success_override_non_zero",
			outcome: engine.RunPartialSuccess,
			adjust: func(cfg *config.Config) {
				cfg.Replay.PartialSuccessExitZero = false
			},
			want: 1,
		},
		{
			name:    "failed",
			outcome: engine.RunFailed,
			want:    1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := config.Default()
			if tt.adjust != nil {
				tt.adjust(&cfg)
			}
			if got, want := exitCodeForSummary(engine.Summary{Outcome: tt.outcome}, cfg), tt.want; got != want {
				t.Fatalf("exitCodeForSummary(%s) = %d, want %d", tt.outcome, got, want)
			}
		})
	}
}

func TestRunReplayFromFileAcceptsCanonicalRequestWithoutConnectionOpen(t *testing.T) {
	cfg := config.Default()
	cfg.Replay.DryRun = true
	logPath := filepath.Join(t.TempDir(), "envoy.ndjson")
	content := `{"type":"request","request_id":"request-7-1","connection_id":7,"timestamp":"2026-08-03T01:11:06.531Z","method":"GET","authority":"envoy-recorder-proxy:8080","path":"/","protocol":"HTTP/1.1","response_code":200}` + "\n"
	if err := os.WriteFile(logPath, []byte(content), 0o644); err != nil {
		t.Fatalf("os.WriteFile(%q) error: %v", logPath, err)
	}

	registry := metrics.New(cfg.Metrics)
	summary, err := runReplayFromFile(context.Background(), cfg, registry, logPath, "")
	if err != nil {
		t.Fatalf("runReplayFromFile(%q) error: %v", logPath, err)
	}
	if got, want := summary.Outcome, engine.RunSuccess; got != want {
		t.Errorf("runReplayFromFile(%q) outcome = %s, want %s", logPath, got, want)
	}
	if got, want := summary.Skipped, int64(1); got != want {
		t.Errorf("runReplayFromFile(%q) skipped = %d, want %d", logPath, got, want)
	}
	if got, want := summary.ConnectionsDone, int64(1); got != want {
		t.Errorf("runReplayFromFile(%q) completed connections = %d, want %d", logPath, got, want)
	}
}

func TestRunReplayFromFile_TransportFailures(t *testing.T) {
	tests := []struct {
		name            string
		configureTarget func(*testing.T, *config.Config) (authority string, cleanup func())
		wantStatus      string
	}{
		{
			name: "connection_refused",
			configureTarget: func(t *testing.T, cfg *config.Config) (string, func()) {
				t.Helper()
				cfg.Replay.Timeout.Connect = 100 * time.Millisecond
				return closedLocalAddress(t), func() {}
			},
			wantStatus: "connection_refused",
		},
		{
			name: "timeout",
			configureTarget: func(t *testing.T, cfg *config.Config) (string, func()) {
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
			authority, cleanup := tt.configureTarget(t, &cfg)
			defer cleanup()

			logPath := writeReplayLog(t, authority)
			registry := metrics.New(cfg.Metrics)
			registry.SeedEngineLabels(cfg.Metrics.CommonLabelValues())

			summary, err := runReplayFromFile(context.Background(), cfg, registry, logPath, "")
			if err != nil {
				t.Fatalf("runReplayFromFile(...) error: %v", err)
			}
			if got, want := summary.Outcome, engine.RunPartialSuccess; got != want {
				t.Fatalf("summary.Outcome = %s, want %s", got, want)
			}
			if got, want := summary.SendErrors, int64(1); got != want {
				t.Fatalf("summary.SendErrors = %d, want %d", got, want)
			}
			if got, want := exitCodeForSummary(summary, cfg), 0; got != want {
				t.Fatalf("exitCodeForSummary(summary) = %d, want %d", got, want)
			}

			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodGet, cfg.Metrics.Path, nil)
			registry.Handler().ServeHTTP(recorder, request)
			if got, want := recorder.Code, http.StatusOK; got != want {
				t.Fatalf("metrics handler status = %d, want %d", got, want)
			}
			body := recorder.Body.String()
			wantMetricFragment := fmt.Sprintf("status=%q", tt.wantStatus)
			if !strings.Contains(body, wantMetricFragment) {
				t.Fatalf("metrics output missing %s:\n%s", wantMetricFragment, body)
			}
		})
	}
}

func TestStartMetricsServer(t *testing.T) {
	t.Run("returns bind failure synchronously", func(t *testing.T) {
		occupied, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("net.Listen() error: %v", err)
		}
		defer occupied.Close()

		started, err := startMetricsServer(occupied.Addr().String(), http.NotFoundHandler())
		if err == nil {
			if started != nil {
				_ = shutdownMetricsServer(started)
			}
			t.Fatal("startMetricsServer(occupied address) error = nil, want bind error")
		}
		if started != nil {
			t.Fatalf("startMetricsServer(occupied address) server = %v, want nil", started)
		}
	})

	t.Run("returns bound server", func(t *testing.T) {
		started, err := startMetricsServer("127.0.0.1:0", http.NotFoundHandler())
		if err != nil {
			t.Fatalf("startMetricsServer() error: %v", err)
		}
		if started == nil || started.listener == nil {
			t.Fatal("startMetricsServer() did not return a bound server")
		}
		if err := shutdownMetricsServer(started); err != nil {
			t.Fatalf("shutdownMetricsServer() error: %v", err)
		}
	})
}

func TestWaitForMetricsGracePeriod(t *testing.T) {
	start := time.Now()
	waitForMetricsGracePeriod(40 * time.Millisecond)
	if elapsed := time.Since(start); elapsed < 35*time.Millisecond {
		t.Fatalf("waitForMetricsGracePeriod returned too early: %v", elapsed)
	}
}

func TestRunWithMetricsLifecycleHonorsGraceAfterCancellation(t *testing.T) {
	started, err := startMetricsServer("127.0.0.1:0", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	if err != nil {
		t.Fatalf("startMetricsServer() error: %v", err)
	}
	replayStarted := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	type lifecycleResult struct {
		summary engine.Summary
		err     error
	}
	resultCh := make(chan lifecycleResult, 1)
	go func() {
		summary, runErr := runWithMetricsLifecycle(
			ctx,
			func(replayCtx context.Context) (engine.Summary, error) {
				close(replayStarted)
				<-replayCtx.Done()
				return engine.Summary{Outcome: engine.RunFailed}, replayCtx.Err()
			},
			started,
			40*time.Millisecond,
		)
		resultCh <- lifecycleResult{summary: summary, err: runErr}
	}()
	<-replayStarted
	cancelledAt := time.Now()
	cancel()
	time.Sleep(10 * time.Millisecond)

	resp, err := http.Get("http://" + started.listener.Addr().String())
	if err != nil {
		t.Fatalf("GET metrics endpoint during grace period error: %v", err)
	}
	_ = resp.Body.Close()
	if got, want := resp.StatusCode, http.StatusNoContent; got != want {
		t.Fatalf("metrics endpoint status during grace = %d, want %d", got, want)
	}

	result := <-resultCh
	if !errors.Is(result.err, context.Canceled) {
		t.Fatalf("runWithMetricsLifecycle() error = %v, want context cancellation", result.err)
	}
	if elapsed := time.Since(cancelledAt); elapsed < 35*time.Millisecond {
		t.Fatalf("metrics lifecycle returned before full grace period: %v", elapsed)
	}
	conn, err := net.DialTimeout("tcp", started.listener.Addr().String(), 50*time.Millisecond)
	if err == nil {
		_ = conn.Close()
		t.Fatal("metrics listener still accepts connections after lifecycle shutdown")
	}
}

func TestShutdownMetricsServerDrainsInFlightRequest(t *testing.T) {
	requestStarted := make(chan struct{})
	releaseRequest := make(chan struct{})
	released := false
	defer func() {
		if !released {
			close(releaseRequest)
		}
	}()
	started, err := startMetricsServer("127.0.0.1:0", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(requestStarted)
		<-releaseRequest
		w.WriteHeader(http.StatusNoContent)
	}))
	if err != nil {
		t.Fatalf("startMetricsServer() error: %v", err)
	}
	requestResult := make(chan error, 1)
	go func() {
		resp, requestErr := http.Get("http://" + started.listener.Addr().String())
		if requestErr == nil {
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusNoContent {
				requestErr = fmt.Errorf("metrics response status = %d", resp.StatusCode)
			}
		}
		requestResult <- requestErr
	}()
	<-requestStarted
	shutdownResult := make(chan error, 1)
	go func() {
		shutdownResult <- shutdownMetricsServer(started)
	}()
	select {
	case err := <-shutdownResult:
		t.Fatalf("shutdown returned before in-flight request completed: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(releaseRequest)
	released = true
	if err := <-requestResult; err != nil {
		t.Fatalf("in-flight metrics request error: %v", err)
	}
	if err := <-shutdownResult; err != nil {
		t.Fatalf("shutdownMetricsServer() error: %v", err)
	}
}

func writeReplayLog(t *testing.T, authority string) string {
	t.Helper()
	content := fmt.Sprintf(
		"{\"type\":\"request\",\"request_id\":\"request-1-1\",\"connection_id\":1,\"timestamp\":\"2026-08-03T01:11:06.531Z\",\"method\":\"GET\",\"scheme\":\"http\",\"authority\":%q,\"path\":\"/transport\",\"protocol\":\"HTTP/1.1\",\"response_code\":200}\n",
		authority,
	)

	dir := t.TempDir()
	path := filepath.Join(dir, "requests.ndjson")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("os.WriteFile(%q) error: %v", path, err)
	}
	return path
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
