package main

import (
	"context"
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

		listener, err := startMetricsServer(occupied.Addr().String(), http.NotFoundHandler())
		if err == nil {
			if listener != nil {
				_ = listener.Close()
			}
			t.Fatal("startMetricsServer(occupied address) error = nil, want bind error")
		}
		if listener != nil {
			t.Fatalf("startMetricsServer(occupied address) listener = %v, want nil", listener)
		}
	})

	t.Run("returns bound listener", func(t *testing.T) {
		listener, err := startMetricsServer("127.0.0.1:0", http.NotFoundHandler())
		if err != nil {
			t.Fatalf("startMetricsServer() error: %v", err)
		}
		if listener == nil {
			t.Fatal("startMetricsServer() listener = nil")
		}
		if err := listener.Close(); err != nil {
			t.Fatalf("listener.Close() error: %v", err)
		}
	})
}

func TestWaitForMetricsGracePeriod(t *testing.T) {
	t.Run("waits for configured period", func(t *testing.T) {
		ctx := context.Background()
		start := time.Now()
		waitForMetricsGracePeriod(ctx, 40*time.Millisecond)
		if elapsed := time.Since(start); elapsed < 35*time.Millisecond {
			t.Fatalf("waitForMetricsGracePeriod returned too early: %v", elapsed)
		}
	})

	t.Run("returns on context cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		start := time.Now()
		waitForMetricsGracePeriod(ctx, time.Second)
		if elapsed := time.Since(start); elapsed > 20*time.Millisecond {
			t.Fatalf("waitForMetricsGracePeriod ignored cancellation: %v", elapsed)
		}
	})
}

func writeReplayLog(t *testing.T, authority string) string {
	t.Helper()
	content := strings.Join([]string{
		`{"type":"meta","format_version":"1.0"}`,
		`{"type":"connection_open","connection_id":1}`,
		fmt.Sprintf(`{"type":"request","connection_id":1,"http":{"method":"GET","scheme":"http","authority":%q,"path":"/transport"}}`, authority),
		`{"type":"connection_close","connection_id":1,"reason":"remote_close"}`,
	}, "\n") + "\n"

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
