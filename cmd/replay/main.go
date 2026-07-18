package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/reqfleet/replay/internal/config"
	"github.com/reqfleet/replay/internal/engine"
	"github.com/reqfleet/replay/internal/metrics"
	"github.com/reqfleet/replay/internal/model"
	"github.com/reqfleet/replay/internal/parser"
)

func exitCodeForSummary(summary engine.Summary, cfg config.Config) int {
	switch summary.Outcome {
	case engine.RunSuccess:
		return 0
	case engine.RunPartialSuccess:
		if cfg.Replay.PartialSuccessExitZero {
			return 0
		}
		return 1
	default:
		return 1
	}
}

func runReplayFromFile(ctx context.Context, cfg config.Config, registry *metrics.Registry, logPath, format string) (engine.Summary, error) {
	replayEngine := engine.New(cfg, registry)
	eventsCh := make(chan model.Event)
	var summary engine.Summary
	var replayErr error
	done := make(chan struct{})
	go func() {
		summary, replayErr = replayEngine.ReplayStream(ctx, eventsCh)
		close(done)
	}()

	if err := parser.ParseFileStream(logPath, format, func(e model.Event) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case eventsCh <- e:
			return nil
		}
	}); err != nil {
		close(eventsCh)
		<-done
		return engine.Summary{}, err
	}
	close(eventsCh)
	<-done
	if replayErr != nil {
		return engine.Summary{}, replayErr
	}
	return summary, nil
}

func waitForMetricsGracePeriod(period time.Duration) {
	if period <= 0 {
		return
	}
	timer := time.NewTimer(period)
	defer timer.Stop()
	<-timer.C
}

const metricsShutdownTimeout = 5 * time.Second

type metricsHTTPServer struct {
	server   *http.Server
	listener net.Listener
	done     chan error
}

func startMetricsServer(address string, handler http.Handler) (*metricsHTTPServer, error) {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("listen for metrics: %w", err)
	}
	server := &http.Server{Handler: handler}
	started := &metricsHTTPServer{
		server:   server,
		listener: listener,
		done:     make(chan error, 1),
	}
	go func() {
		started.done <- server.Serve(listener)
	}()
	return started, nil
}

func shutdownMetricsServer(started *metricsHTTPServer) error {
	if started == nil {
		return nil
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), metricsShutdownTimeout)
	defer cancel()
	shutdownErr := started.server.Shutdown(shutdownCtx)
	if shutdownErr != nil {
		shutdownErr = errors.Join(shutdownErr, started.server.Close())
	}
	serveErr := <-started.done
	if errors.Is(serveErr, http.ErrServerClosed) || errors.Is(serveErr, net.ErrClosed) {
		serveErr = nil
	}
	return errors.Join(shutdownErr, serveErr)
}

func runWithMetricsLifecycle(
	ctx context.Context,
	replay func(context.Context) (engine.Summary, error),
	started *metricsHTTPServer,
	gracePeriod time.Duration,
) (engine.Summary, error) {
	summary, replayErr := replay(ctx)
	if started == nil {
		return summary, replayErr
	}
	if gracePeriod > 0 {
		slog.Info("metrics graceful termination period", "period", gracePeriod)
	}
	waitForMetricsGracePeriod(gracePeriod)
	if shutdownErr := shutdownMetricsServer(started); shutdownErr != nil {
		replayErr = errors.Join(replayErr, fmt.Errorf("shutdown metrics server: %w", shutdownErr))
	}
	return summary, replayErr
}

func main() {
	os.Exit(run())
}

func run() int {
	configPath := flag.String("config", "", "path to config.yaml (optional)")
	logPath := flag.String("log", "", "path to requests.log NDJSON file")
	zstdFlag := flag.Bool("zstd", false, "read log file compressed with zstd")
	gzipFlag := flag.Bool("gzip", false, "read log file compressed with gzip")
	dryRunFlag := flag.Bool("dry-run", false, "dry run mode: do not send network requests")
	overrideFlag := flag.String("override-url", "", "override target URL (overrides config)")
	disallowRecordedTargets := flag.Bool("disallow-recorded-targets", false, "fail if no target override is configured")
	verboseFlag := flag.Bool("verbose", false, "enable verbose output (e.g. log response errors)")
	flag.Parse()

	// Initialize slog default logger
	logLevel := slog.LevelInfo
	if *verboseFlag {
		logLevel = slog.LevelDebug
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: logLevel,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if a.Key == slog.LevelKey {
				a.Value = slog.StringValue("[" + a.Value.String() + "]")
			}
			return a
		},
	})))

	if *logPath == "" {
		slog.Error("-log is required")
		return 2
	}
	if *zstdFlag && *gzipFlag {
		slog.Error("cannot specify both -zstd and -gzip")
		return 2
	}
	cfg, err := config.Load(*configPath)
	if err != nil {
		slog.Error("load config", "error", err)
		return 2
	}
	// Apply environment overrides (env > YAML)
	cfg.ApplyEnv()
	if err := cfg.Validate(); err != nil {
		slog.Error("validate config", "error", err)
		return 2
	}
	slog.Info("resolved metric labels", cfg.Metrics.CommonLabelAttrs()...)
	// Apply CLI overrides with higher precedence for safety-related flags
	if *dryRunFlag {
		cfg.Replay.DryRun = true
	}
	if *overrideFlag != "" {
		cfg.Target.OverrideURL = *overrideFlag
	}
	if *disallowRecordedTargets {
		cfg.Target.DisallowRecordedTargets = true
	}
	if *verboseFlag {
		cfg.Replay.Verbose = true
	}

	if _, err := cfg.Target.ParseURL(); err != nil {
		slog.Error("validate target override", "error", err)
		return 2
	}

	registry := metrics.New(cfg.Metrics)
	metricLabelValues := cfg.Metrics.CommonLabelValues()
	registry.SeedEngineLabels(metricLabelValues)
	var startedMetrics *metricsHTTPServer
	if cfg.Metrics.Enabled {
		stopMetrics := registry.StartRuntimeCollection(metricLabelValues, 2*time.Second)
		if stopMetrics != nil {
			defer stopMetrics()
		}
		mux := http.NewServeMux()
		mux.Handle(cfg.Metrics.Path, registry.Handler())
		startedMetrics, err = startMetricsServer(cfg.Metrics.ListenAddress, mux)
		if err != nil {
			slog.Error("start metrics server", "error", err)
			return 2
		}
		slog.Info(
			"metrics endpoint ready",
			"url", fmt.Sprintf("http://%s%s", startedMetrics.listener.Addr(), cfg.Metrics.Path),
		)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	var format string
	if *gzipFlag {
		format = "gzip"
	} else if *zstdFlag {
		format = "zstd"
	}

	summary, err := runWithMetricsLifecycle(
		ctx,
		func(replayCtx context.Context) (engine.Summary, error) {
			return runReplayFromFile(replayCtx, cfg, registry, *logPath, format)
		},
		startedMetrics,
		cfg.Metrics.GracefulTerminationPeriod,
	)
	if err != nil {
		slog.Error("replay failed", "error", err)
		return 2
	}

	slog.Info("replay finished",
		"outcome", summary.Outcome,
		"sent", summary.RequestsSent,
		"responses", summary.ResponsesReceived,
		"send_errors", summary.SendErrors,
		"validation_failed", summary.ValidationFailed,
		"skipped", summary.Skipped,
		"conn_done", summary.ConnectionsDone,
		"conn_aborted", summary.ConnectionsAborted,
	)

	exitCode := exitCodeForSummary(summary, cfg)
	if exitCode != 0 && summary.Outcome == engine.RunFailed {
		fmt.Fprintln(os.Stderr, "replay failed")
	}
	return exitCode
}
