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
	"sync/atomic"
	"testing"
	"time"

	"github.com/reqfleet/replay/config"
	"github.com/reqfleet/replay/internal/engine"
	"github.com/reqfleet/replay/internal/metrics"
)

func TestResolveConfigAppliesTargetOverridesBeforeValidation(t *testing.T) {
	tests := []struct {
		name            string
		yaml            string
		envOverrideURL  string
		envDisallow     string
		overrides       runtimeOverrides
		wantOverrideURL string
		wantDisallow    bool
	}{
		{
			name:            "yaml_disallow_with_environment_override",
			yaml:            "target:\n  disallow_recorded_targets: true\n",
			envOverrideURL:  "https://env.example.test",
			wantOverrideURL: "https://env.example.test",
			wantDisallow:    true,
		},
		{
			name:        "environment_disallow_with_cli_override",
			envDisallow: "true",
			overrides: runtimeOverrides{
				overrideURL: "https://cli.example.test",
			},
			wantOverrideURL: "https://cli.example.test",
			wantDisallow:    true,
		},
		{
			name: "invalid_yaml_override_replaced_by_cli",
			yaml: "target:\n  override_url: ftp://yaml.example.test\n",
			overrides: runtimeOverrides{
				overrideURL: "https://cli.example.test",
			},
			wantOverrideURL: "https://cli.example.test",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("REPLAY_OVERRIDE_URL", test.envOverrideURL)
			t.Setenv("REPLAY_DISALLOW_RECORDED_TARGETS", test.envDisallow)

			configPath := ""
			if test.yaml != "" {
				configPath = filepath.Join(t.TempDir(), "config.yaml")
				if err := os.WriteFile(configPath, []byte(test.yaml), 0o644); err != nil {
					t.Fatalf("os.WriteFile(%q) error: %v", configPath, err)
				}
			}

			cfg, err := resolveConfig(configPath, test.overrides)
			if err != nil {
				t.Fatalf("resolveConfig(%s) error: %v", test.name, err)
			}
			if got := cfg.Target.OverrideURL; got != test.wantOverrideURL {
				t.Errorf("resolveConfig(%s).Target.OverrideURL = %q, want %q", test.name, got, test.wantOverrideURL)
			}
			if got := cfg.Target.DisallowRecordedTargets; got != test.wantDisallow {
				t.Errorf("resolveConfig(%s).Target.DisallowRecordedTargets = %t, want %t", test.name, got, test.wantDisallow)
			}
		})
	}
}

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

func TestRunReplayFromFileAcceptsDownstreamEndForQuickVerification(t *testing.T) {
	cfg := config.Default()
	cfg.Replay.DryRun = true
	logPath := filepath.Join(t.TempDir(), "downstream-end.ndjson")
	content := `{"type":"DownstreamEnd","connection_id":7,"stream_id":2,"timestamp":"2026-08-03T01:11:07.531Z","method":"GET","authority":"envoy-recorder-proxy:8080","path":"/b","protocol":"HTTP/2","response_code":200,"response_flags":"DC"}` + "\n" +
		`{"connection_id":7,"stream_id":1,"timestamp":"2026-08-03T01:11:06.531Z","method":"GET","authority":"envoy-recorder-proxy:8080","path":"/a","protocol":"HTTP/2","response_code":201,"response_flags":"-"}` + "\n"
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
	if got, want := summary.Skipped, int64(2); got != want {
		t.Errorf("runReplayFromFile(%q) skipped = %d, want %d", logPath, got, want)
	}
	if got, want := summary.ConnectionsDone, int64(1); got != want {
		t.Errorf("runReplayFromFile(%q) completed connections = %d, want %d", logPath, got, want)
	}
}

func TestRunReplayFromFileProtocolFailureWithoutValidation(t *testing.T) {
	var applicationRequests atomic.Int64
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// An HTTP/1 server may expose the HTTP/2 preface as PRI *.
		if r.Method == http.MethodGet && r.URL.Path == "/protocol" {
			applicationRequests.Add(1)
		}
		w.WriteHeader(http.StatusOK)
	}))
	srv.Start()
	t.Cleanup(srv.Close)

	cfg := config.Default()
	cfg.Target.OverrideURL = srv.URL
	cfg.Replay.Pacing.Enabled = false
	cfg.Replay.Validation.Status = false
	cfg.Replay.Validation.Headers = false
	cfg.Replay.Validation.Body = false
	cfg.Replay.Timeout.Request = time.Second
	cfg.Replay.PartialSuccessExitZero = true

	logPath := filepath.Join(t.TempDir(), "requests.ndjson")
	content := `{"type":"request","request_id":"request-1-1","connection_id":1,"timestamp":"2026-08-03T01:11:06.531Z","method":"GET","scheme":"https","authority":"recorded.example","path":"/protocol","protocol":"HTTP/2","stream_id":1,"response_code":200}` + "\n"
	if err := os.WriteFile(logPath, []byte(content), 0o644); err != nil {
		t.Fatalf("os.WriteFile(%q) error: %v", logPath, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	summary, err := runReplayFromFile(ctx, cfg, metrics.New(cfg.Metrics), logPath, "")
	if err != nil {
		t.Fatalf("runReplayFromFile(HTTP/2 capture, HTTP/1 target) error: %v", err)
	}
	if got, want := summary.Outcome, engine.RunFailed; got != want {
		t.Errorf("summary.Outcome = %s, want %s", got, want)
	}
	if got, want := summary.ProtocolFailed, int64(1); got != want {
		t.Errorf("summary.ProtocolFailed = %d, want %d", got, want)
	}
	if got := summary.ValidationFailed; got != 0 {
		t.Errorf("summary.ValidationFailed = %d, want 0 with validation disabled", got)
	}
	if got := summary.ResponsesReceived; got != 0 {
		t.Errorf("summary.ResponsesReceived = %d, want 0 usable HTTP/2 responses", got)
	}
	if got, want := exitCodeForSummary(summary, cfg), 1; got != want {
		t.Errorf("exitCodeForSummary(protocol failure, validation disabled) = %d, want %d", got, want)
	}
	if got := applicationRequests.Load(); got != 0 {
		t.Errorf("HTTP/1 application requests = %d, want 0 (no fallback)", got)
	}
}

func TestRunReplayFromFileDisabledHTTP2WithoutValidation(t *testing.T) {
	godebug := "http2client=0"
	if current := os.Getenv("GODEBUG"); current != "" {
		godebug = current + "," + godebug
	}
	t.Setenv("GODEBUG", godebug)

	for _, scheme := range []string{"http", "https"} {
		t.Run(scheme, func(t *testing.T) {
			var applicationRequests atomic.Int64
			srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				applicationRequests.Add(1)
				w.WriteHeader(http.StatusOK)
			}))
			srv.Config.Protocols = new(http.Protocols)
			srv.Config.Protocols.SetHTTP1(true)
			srv.Config.Protocols.SetHTTP2(true)
			srv.Config.Protocols.SetUnencryptedHTTP2(true)
			if scheme == "https" {
				srv.EnableHTTP2 = true
				srv.StartTLS()
			} else {
				srv.Start()
			}
			t.Cleanup(srv.Close)

			cfg := config.Default()
			cfg.Target.OverrideURL = srv.URL
			cfg.Replay.TLS.InsecureSkipVerify = true
			cfg.Replay.Pacing.Enabled = false
			cfg.Replay.Validation.Status = false
			cfg.Replay.Validation.Headers = false
			cfg.Replay.Validation.Body = false
			cfg.Replay.PartialSuccessExitZero = true
			cfg.Replay.Timeout.Request = time.Second

			logPath := filepath.Join(t.TempDir(), "requests.ndjson")
			content := `{"type":"request","request_id":"request-1","connection_id":1,"timestamp":"2026-09-09T00:00:00Z","method":"GET","authority":"recorded.example","path":"/protocol","protocol":"HTTP/2","stream_id":1,"response_code":200}` + "\n"
			if err := os.WriteFile(logPath, []byte(content), 0o644); err != nil {
				t.Fatalf("os.WriteFile(%q) error: %v", logPath, err)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			summary, err := runReplayFromFile(ctx, cfg, metrics.New(cfg.Metrics), logPath, "")
			if err != nil {
				t.Fatalf("runReplayFromFile(HTTP/2 capture, HTTP/2 disabled) error: %v", err)
			}
			if got, want := summary.Outcome, engine.RunFailed; got != want {
				t.Errorf("summary.Outcome = %s, want %s", got, want)
			}
			if got, want := summary.ProtocolFailed, int64(1); got != want {
				t.Errorf("summary.ProtocolFailed = %d, want %d", got, want)
			}
			if got := summary.SendErrors; got != 0 {
				t.Errorf("summary.SendErrors = %d, want 0 ordinary network failures", got)
			}
			if got := exitCodeForSummary(summary, cfg); got != 1 {
				t.Errorf("exitCodeForSummary(HTTP/2 disabled) = %d, want 1 despite partial-success exit zero", got)
			}
			if got := applicationRequests.Load(); got != 0 {
				t.Errorf("target application requests = %d, want 0 (no HTTP/1 fallback)", got)
			}
		})
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
