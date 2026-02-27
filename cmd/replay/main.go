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

	"github.com/reqfleet/replay/internal/config"
	"github.com/reqfleet/replay/internal/engine"
	"github.com/reqfleet/replay/internal/metrics"
	"github.com/reqfleet/replay/internal/parser"
)

func main() {
	configPath := flag.String("config", "", "path to config.yaml (optional)")
	logPath := flag.String("log", "", "path to requests.log NDJSON file")
	flag.Parse()

	if *logPath == "" {
		log.Println("-log is required")
		os.Exit(2)
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Printf("load config: %v", err)
		os.Exit(2)
	}
	for key, value := range cfg.Env {
		if setErr := os.Setenv(key, value); setErr != nil {
			log.Printf("set env %s: %v", key, setErr)
			os.Exit(2)
		}
	}

	events, err := parser.ParseFile(*logPath)
	if err != nil {
		log.Printf("parse log file: %v", err)
		os.Exit(2)
	}

	registry := metrics.New()
	if cfg.Metrics.Enabled {
		mux := http.NewServeMux()
		mux.Handle(cfg.Metrics.Path, registry.Handler())
		go func() {
			if serveErr := http.ListenAndServe(cfg.Metrics.ListenAddress, mux); serveErr != nil {
				log.Printf("metrics server stopped: %v", serveErr)
			}
		}()
		log.Printf("metrics endpoint ready at http://%s%s", cfg.Metrics.ListenAddress, cfg.Metrics.Path)
	}

	replayEngine := engine.New(cfg, registry)
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	summary, err := replayEngine.Replay(ctx, events)
	if err != nil {
		log.Printf("replay failed: %v", err)
		os.Exit(1)
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

	switch summary.Outcome {
	case engine.RunSuccess:
		os.Exit(0)
	case engine.RunPartialSuccess:
		if os.Getenv("REPLAY_PARTIAL_SUCCESS_EXIT_ZERO") == "1" {
			os.Exit(0)
		}
		os.Exit(1)
	default:
		fmt.Fprintln(os.Stderr, "replay failed")
		os.Exit(1)
	}
}
