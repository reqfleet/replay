package main

import (
	"context"
	"flag"
	"fmt"
	"log"
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

func main() {
	configPath := flag.String("config", "", "path to config.yaml (optional)")
	logPath := flag.String("log", "", "path to requests.log NDJSON file")
	zstdFlag := flag.Bool("zstd", false, "read log file compressed with zstd")
	gzipFlag := flag.Bool("gzip", false, "read log file compressed with gzip")
	dryRunFlag := flag.Bool("dry-run", false, "dry run mode: do not send network requests")
	overrideFlag := flag.String("override-url", "", "override target URL (overrides config)")
	requireOverride := flag.Bool("require-override", false, "fail if override-url is required but missing")
	verboseFlag := flag.Bool("verbose", false, "enable verbose output (e.g. log response errors)")
	flag.Parse()
	if *logPath == "" {
		log.Println("-log is required")
		os.Exit(2)
	}
	if *zstdFlag && *gzipFlag {
		log.Println("cannot specify both -zstd and -gzip")
		os.Exit(2)
	}
	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Printf("load config: %v", err)
		os.Exit(2)
	}
	// Apply environment overrides (env > YAML)
	cfg.ApplyEnv()
	// Apply CLI overrides with higher precedence for safety-related flags
	if *dryRunFlag {
		cfg.Replay.DryRun = true
	}
	if *overrideFlag != "" {
		cfg.Target.OverrideURL = *overrideFlag
	}
	if *requireOverride {
		cfg.Target.Require = true
	}
	if *verboseFlag {
		cfg.Replay.Verbose = true
	}

	if cfg.Target.Require && cfg.Target.OverrideURL == "" {
		log.Println("target override required but missing; aborting")
		os.Exit(2)
	}
	for key, value := range cfg.Env {
		if setErr := os.Setenv(key, value); setErr != nil {
			log.Printf("set env %s: %v", key, setErr)
			os.Exit(2)
		}
	}

	// We'll stream the parsed events into the engine to avoid building
	// a large in-memory slice for big log files.

	registry := metrics.New()
	// seed labels early so collectors and the server can report immediately
	registry.SeedEngineLabels(cfg.Labels)
	var stopMetrics func()
	if cfg.Metrics.Enabled {
		// start runtime collectors (threads/memory/CPU)
		stopMetrics = registry.StartRuntimeCollection(cfg.Labels, 5*time.Second)
		if stopMetrics != nil {
			defer stopMetrics()
		}
		mux := http.NewServeMux()
		mux.Handle(cfg.Metrics.Path, registry.Handler())
		go func() {
			if serveErr := http.ListenAndServe(cfg.Metrics.ListenAddress, mux); serveErr != nil {
				log.Printf("metrics server stopped: %v", serveErr)
			}
		}()
		log.Printf("metrics endpoint ready at http://%s%s", cfg.Metrics.ListenAddress, cfg.Metrics.Path)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	var format string
	if *gzipFlag {
		format = "gzip"
	} else if *zstdFlag {
		format = "zstd"
	}

	summary, err := runReplayFromFile(ctx, cfg, registry, *logPath, format)
	if err != nil {
		log.Printf("parse log file: %v", err)
		os.Exit(2)
	}

	log.Printf(
		"replay outcome=%s sent=%d responses=%d send_errors=%d validation_failed=%d skipped=%d conn_done=%d conn_aborted=%d",
		summary.Outcome,
		summary.RequestsSent,
		summary.ResponsesReceived,
		summary.SendErrors,
		summary.ValidationFailed,
		summary.Skipped,
		summary.ConnectionsDone,
		summary.ConnectionsAborted,
	)

	exitCode := exitCodeForSummary(summary, cfg)
	if exitCode != 0 && summary.Outcome == engine.RunFailed {
		fmt.Fprintln(os.Stderr, "replay failed")
	}
	os.Exit(exitCode)
}
