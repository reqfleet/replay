package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/reqfleet/replay/config"
	"github.com/reqfleet/replay/internal/metrics"
	"github.com/reqfleet/replay/internal/model"
	"github.com/reqfleet/replay/internal/sharding"
)

func TestReplaySharedPacingInitialOffsets(t *testing.T) {
	for _, workers := range []int{1, 2} {
		t.Run(fmt.Sprintf("%d_workers", workers), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cfg, err := config.Parse([]byte("replay:\n  dry_run: true\n"))
				if err != nil {
					t.Fatal(err)
				}
				cfg.Replay.MaxVirtualUsersPerEngine = workers
				base := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)
				events := make(chan model.Event, 2)
				events <- model.Event{Type: model.EventRequest, ConnectionID: 1, Sequence: 1, Method: "GET", Timestamp: base.Format(time.RFC3339Nano)}
				events <- model.Event{Type: model.EventRequest, ConnectionID: 2, Sequence: 1, Method: "GET", Timestamp: base.Add(9 * time.Second).Format(time.RFC3339Nano)}
				close(events)
				start := time.Now()
				summary, err := New(cfg, nil).ReplayStream(context.Background(), events, nil)
				if err != nil {
					t.Fatal(err)
				}
				if got := time.Since(start); got != 9*time.Second {
					t.Errorf("shared replay with %d workers elapsed = %s, want 9s", workers, got)
				}
				if summary.Skipped != 2 {
					t.Errorf("dry-run skipped = %d, want 2", summary.Skipped)
				}
			})
		})
	}
}

func TestSharedPacingBurstDeadlines(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := config.Default()

		eng := New(cfg, nil)
		start := time.Now()
		origin := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)
		timeline := &replayTimeline{captureOrigin: origin, replayStart: start}
		a, b := pacingClock{timeline: timeline}, pacingClock{timeline: timeline}
		for _, request := range []struct {
			connection string
			clock      *pacingClock
			offset     time.Duration
		}{
			{"A", &a, 0},
			{"B", &b, 9 * time.Second},
			{"A", &a, 10 * time.Second},
			{"B", &b, 10 * time.Second},
		} {
			if err := eng.paceTimestamp(context.Background(), request.clock, origin.Add(request.offset).Format(time.RFC3339Nano)); err != nil {
				t.Fatal(err)
			}
			if got := request.clock.nextRequestAt.Sub(start); got != request.offset {
				t.Errorf("%s intended deadline = %s, want %s", request.connection, got, request.offset)
			}
			if got := time.Since(start); got != request.offset {
				t.Errorf("%s paced at = %s, want %s", request.connection, got, request.offset)
			}
		}
	})
}

func TestSharedPacingLateFirstRequestRetainsAnchor(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := config.Default()

		eng := New(cfg, nil)
		start := time.Now()
		origin := start.Add(-time.Hour)
		clock := pacingClock{timeline: &replayTimeline{captureOrigin: origin, replayStart: start}}
		time.Sleep(12 * time.Second)
		for _, offset := range []time.Duration{9 * time.Second, 10 * time.Second} {
			if err := eng.paceTimestamp(context.Background(), &clock, origin.Add(offset).Format(time.RFC3339Nano)); err != nil {
				t.Fatal(err)
			}
			if got := clock.nextRequestAt.Sub(start); got != offset {
				t.Errorf("late request intended deadline = %s, want %s", got, offset)
			}
			if got := time.Since(start); got != 12*time.Second {
				t.Errorf("late request advanced time to %s, want 12s", got)
			}
		}
	})
}

func TestSharedPacingBackwardTimestamps(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := config.Default()

		eng := New(cfg, nil)
		start := time.Now()
		clock := pacingClock{timeline: &replayTimeline{captureOrigin: start, replayStart: start}}
		for _, step := range []struct {
			offset, want time.Duration
		}{
			{-time.Second, -time.Second}, // A connection may first appear before the input origin.
			{2 * time.Second, 2 * time.Second},
			{time.Second, 2 * time.Second},
			{2 * time.Second, 2 * time.Second},
			{3 * time.Second, 3 * time.Second},
		} {
			if err := eng.paceTimestamp(context.Background(), &clock, start.Add(step.offset).Format(time.RFC3339Nano)); err != nil {
				t.Fatal(err)
			}
			if got := clock.nextRequestAt.Sub(start); got != step.want {
				t.Errorf("timestamp offset %s deadline = %s, want %s", step.offset, got, step.want)
			}
			if got, want := time.Since(start), max(step.want, 0); got != want {
				t.Errorf("timestamp offset %s elapsed = %s, want %s", step.offset, got, want)
			}
		}
	})
}

func TestReplayPacingAndRampup(t *testing.T) {
	for _, tt := range []struct {
		name    string
		enabled bool
		ramp    time.Duration
		want    time.Duration
	}{
		{"enabled", true, 0, 10 * time.Second},
		{"disabled", false, 0, 0},
		{"late_activation", true, 15 * time.Second, 15 * time.Second},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cfg := config.Default()

				cfg.Replay.Pacing.Enabled = tt.enabled
				cfg.Replay.DryRun = true
				cfg.Replay.MaxVirtualUsersPerEngine = 2
				cfg.Replay.RampupDuration = tt.ramp
				eng := New(cfg, nil)
				origin := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)
				for range 2 { // Each run needs a fresh origin/start, even on the same Engine.
					start := time.Now()
					events := []model.Event{
						{Type: model.EventRequest, ConnectionID: 1, Sequence: 1, Method: "GET", Timestamp: origin.Format(time.RFC3339Nano)},
						{Type: model.EventRequest, ConnectionID: 2, Sequence: 1, Method: "GET", Timestamp: origin.Add(9 * time.Second).Format(time.RFC3339Nano)},
						{Type: model.EventRequest, ConnectionID: 2, Sequence: 2, Method: "GET", Timestamp: origin.Add(10 * time.Second).Format(time.RFC3339Nano)},
					}
					summary, err := runReplay(eng, events)
					if err != nil {
						t.Fatal(err)
					}
					if got := time.Since(start); got != tt.want {
						t.Errorf("replay elapsed = %s, want %s", got, tt.want)
					}
					if summary.Skipped != 3 {
						t.Errorf("dry-run skipped = %d, want 3", summary.Skipped)
					}
				}
			})
		})
	}
}

func TestReplaySuppliedSchedule(t *testing.T) {
	for _, tt := range []struct {
		name       string
		enabled    bool
		startDelay time.Duration
		want       time.Duration
	}{
		{name: "future_start", enabled: true, startDelay: 5 * time.Second, want: 15 * time.Second},
		{name: "late_start", enabled: true, startDelay: -9 * time.Second, want: time.Second},
		{name: "overdue", enabled: true, startDelay: -15 * time.Second, want: 0},
		{name: "disabled", enabled: false, startDelay: 5 * time.Second, want: 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cfg := config.Default()
				cfg.Replay.Pacing.Enabled = tt.enabled
				cfg.Replay.DryRun = true
				eng := New(cfg, nil)
				start := time.Now()
				schedule := ReplaySchedule{
					CaptureOrigin: time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC),
					ReplayStart:   start.Add(tt.startDelay).Round(0),
				}
				// This input lacks the capture origin, as a pre-sharded file may.
				input := []model.Event{
					{Type: model.EventRequest, ConnectionID: 1, Sequence: 1, Method: "GET", Timestamp: schedule.CaptureOrigin.Add(9 * time.Second).Format(time.RFC3339Nano)},
					{Type: model.EventRequest, ConnectionID: 2, Sequence: 1, Method: "GET", Timestamp: schedule.CaptureOrigin.Add(10 * time.Second).Format(time.RFC3339Nano)},
				}
				events := make(chan model.Event, len(input))
				for _, event := range input {
					events <- event
				}
				close(events)
				summary, err := eng.ReplayStream(context.Background(), events, &schedule)
				if err != nil {
					t.Fatal(err)
				}
				if got := time.Since(start); got != tt.want {
					t.Errorf("ReplayStream(supplied schedule) elapsed = %s, want %s", got, tt.want)
				}
				if summary.Skipped != 2 {
					t.Errorf("ReplayStream(supplied schedule) skipped = %d, want 2", summary.Skipped)
				}

				// A later automatic invocation must not retain the supplied pair.
				start = time.Now()
				if _, err := runReplay(eng, input); err != nil {
					t.Fatal(err)
				}
				wantAutomatic := time.Duration(0)
				if tt.enabled {
					wantAutomatic = time.Second
				}
				if got := time.Since(start); got != wantAutomatic {
					t.Errorf("ReplayStream(nil schedule) elapsed = %s, want %s", got, wantAutomatic)
				}
			})
		})
	}
}

func TestReplaySharedOriginBeforeShardFiltering(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := config.Default()

		cfg.Replay.DryRun = true
		cfg.Replay.Sharding.ShardCount = 2
		keys := make([]model.ConnectionKey, 2)
		found := [2]bool{}
		for id := 1; !found[0] || !found[1]; id++ {
			key := model.ConnectionKey{ConnectionID: id}
			shard := 1
			if sharding.ConnectionBelongsToShard(key, 0, 2) {
				shard = 0
			}
			keys[shard], found[shard] = key, true
		}
		start := time.Now()
		summary, err := runReplay(New(cfg, nil), []model.Event{
			{Type: model.EventRequest, ConnectionID: keys[1].ConnectionID, Timestamp: start.Format(time.RFC3339Nano)},
			{Type: model.EventRequest, ConnectionID: keys[0].ConnectionID, Method: "GET", Timestamp: start.Add(9 * time.Second).Format(time.RFC3339Nano)},
		})
		if err != nil {
			t.Fatal(err)
		}
		if got := time.Since(start); got != 9*time.Second {
			t.Errorf("sharded replay elapsed = %s, want 9s from unfiltered origin", got)
		}
		if summary.Skipped != 1 {
			t.Errorf("sharded dry-run skipped = %d, want 1", summary.Skipped)
		}
	})
}

func TestSharedPacingResumeRetainsSkippedTimeline(t *testing.T) {
	path := filepath.Join(t.TempDir(), "checkpoint.json")
	store, err := newCheckpointStore(path, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []int{1, 2} {
		if err := store.markProcessed(model.ConnectionKey{ConnectionID: id}, 1); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	synctest.Test(t, func(t *testing.T) {
		cfg := config.Default()

		cfg.Replay.Checkpoint.File = path
		start := time.Now()
		summary, err := runReplay(New(cfg, nil), []model.Event{
			{Type: model.EventRequest, ConnectionID: 1, Sequence: 1, Timestamp: start.Format(time.RFC3339Nano)},
			{Type: model.EventRequest, ConnectionID: 2, Sequence: 1, Timestamp: start.Add(9 * time.Second).Format(time.RFC3339Nano)},
		})
		if err != nil {
			t.Fatal(err)
		}
		if got := time.Since(start); got != 9*time.Second {
			t.Errorf("resumed replay elapsed = %s, want 9s including skipped requests", got)
		}
		if summary.Skipped != 2 || summary.RequestsSent != 0 {
			t.Errorf("resumed summary = %+v, want two skipped and no sends", summary)
		}
	})
}

func TestSharedPacingInitialWaitCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := config.Default()

		eng := New(cfg, nil)
		start := time.Now()
		clock := pacingClock{timeline: &replayTimeline{captureOrigin: start, replayStart: start}}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		err := eng.paceTimestamp(ctx, &clock, start.Add(9*time.Second).Format(time.RFC3339Nano))
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("initial wait error = %v, want deadline exceeded", err)
		}
		if got := time.Since(start); got != time.Second {
			t.Errorf("cancelled wait elapsed = %s, want 1s", got)
		}
	})
}

func TestSharedPacingSequentialSendsAndLateness(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := config.Default()

		cfg.Metrics.ScheduleLatenessEnabled = true
		reg := metrics.New(cfg.Metrics)
		eng := New(cfg, reg)
		start := time.Now()
		var sends []time.Duration
		cs := eng.newConnState(model.ConnectionKey{ConnectionID: 1})
		cs.pacing.timeline = &replayTimeline{captureOrigin: start, replayStart: start}
		cs.client = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			sends = append(sends, time.Since(start))
			if len(sends) == 1 {
				time.Sleep(12 * time.Second)
			}
			return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("")), Request: req}, nil
		})}
		for index, offset := range []time.Duration{0, 10 * time.Second} {
			event := model.Event{ConnectionID: 1, Sequence: index + 1, Timestamp: start.Add(offset).Format(time.RFC3339Nano), Method: "GET", Scheme: "http", Authority: "example.test", Path: "/"}
			if abort := eng.processRequest(context.Background(), cs, event, nil); abort {
				t.Fatalf("request %d aborted", index+1)
			}
		}
		if want := []time.Duration{0, 12 * time.Second}; !slices.Equal(sends, want) {
			t.Errorf("sequential send times = %v, want %v", sends, want)
		}
		observer := reg.ScheduleLatenessHistogram.WithLabelValues(append(cfg.Metrics.CommonLabelValues(), "/")...)
		sample := &dto.Metric{}
		if err := observer.(prometheus.Metric).Write(sample); err != nil {
			t.Fatal(err)
		}
		if got := sample.GetHistogram(); got.GetSampleCount() != 2 || got.GetSampleSum() != 2 {
			t.Errorf("lateness count/sum = %d/%g, want 2/2 seconds", got.GetSampleCount(), got.GetSampleSum())
		}
	})
}

func TestSharedPacingHTTP2CapturesEachDispatchDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := config.Default()

		cfg.Metrics.ScheduleLatenessEnabled = true
		reg := metrics.New(cfg.Metrics)
		eng := New(cfg, reg)
		start := time.Now()
		cs := eng.newConnState(model.ConnectionKey{ConnectionID: 1})
		cs.pacing.timeline = &replayTimeline{captureOrigin: start, replayStart: start}
		cs.client = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 200, Header: make(http.Header), Body: http.NoBody, Request: req}, nil
		})}
		time.Sleep(12 * time.Second)
		for index, offset := range []time.Duration{9 * time.Second, 10 * time.Second} {
			eng.processRequestConcurrent(context.Background(), cs, model.Event{
				ConnectionID: 1, Sequence: index + 1, StreamID: 2*index + 1, Protocol: "HTTP/2",
				Timestamp: start.Add(offset).Format(time.RFC3339Nano),
				Method:    "GET", Scheme: "http", Authority: "example.test", Path: "/",
			}, nil)
		}
		cs.h2WG.Wait()
		if cs.sent != 2 || cs.aborted {
			t.Fatalf("multiplexed sends/aborted = %d/%t, want 2/false", cs.sent, cs.aborted)
		}
		observer := reg.ScheduleLatenessHistogram.WithLabelValues(append(cfg.Metrics.CommonLabelValues(), "/")...)
		sample := &dto.Metric{}
		if err := observer.(prometheus.Metric).Write(sample); err != nil {
			t.Fatal(err)
		}
		if got := sample.GetHistogram(); got.GetSampleCount() != 2 || got.GetSampleSum() != 5 {
			t.Errorf("multiplexed lateness count/sum = %d/%g, want 2/5 seconds", got.GetSampleCount(), got.GetSampleSum())
		}
	})
}

func TestScheduleLatenessExcludesRetriesAndUnpacedSends(t *testing.T) {
	for _, tt := range []struct {
		name            string
		pacingEnabled   bool
		metricsEnabled  bool
		latenessEnabled bool
		wantCount       uint64
		wantSum         float64
	}{
		{name: "default_off", pacingEnabled: true, metricsEnabled: true},
		{name: "unpaced", metricsEnabled: true, latenessEnabled: true},
		{name: "paced_retries", pacingEnabled: true, metricsEnabled: true, latenessEnabled: true, wantCount: 1, wantSum: 2},
		{name: "disabled_metrics", pacingEnabled: true, latenessEnabled: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cfg := config.Default()

				cfg.Replay.Pacing.Enabled = tt.pacingEnabled
				cfg.Metrics.Enabled = tt.metricsEnabled
				cfg.Metrics.ScheduleLatenessEnabled = tt.latenessEnabled
				cfg.Replay.Retry.MaxAttempts = 2
				cfg.Replay.Retry.RetryOnStatuses = []int{503}
				reg := metrics.New(cfg.Metrics)
				eng := New(cfg, reg)
				start := time.Now()
				cs := eng.newConnState(model.ConnectionKey{ConnectionID: 1})
				cs.pacing.timeline = &replayTimeline{captureOrigin: start, replayStart: start}
				attempts := 0
				cs.client = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
					attempts++
					status := 200
					if attempts == 1 {
						status = 503
					}
					return &http.Response{StatusCode: status, Header: make(http.Header), Body: http.NoBody, Request: req}, nil
				})}
				time.Sleep(2 * time.Second)
				if abort := eng.processRequest(context.Background(), cs, model.Event{
					ConnectionID: 1, Sequence: 1, Timestamp: start.Format(time.RFC3339Nano),
					Method: "GET", Scheme: "http", Authority: "example.test", Path: "/",
				}, nil); abort {
					t.Fatal("retryable request aborted")
				}
				if attempts != 2 {
					t.Errorf("HTTP attempts = %d, want 2", attempts)
				}
				if tt.wantCount == 0 {
					recorder := httptest.NewRecorder()
					reg.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
					if strings.Contains(recorder.Body.String(), "replay_schedule_lateness_seconds") {
						t.Errorf("unexpected lateness observations:\n%s", recorder.Body.String())
					}
					return
				}
				observer := reg.ScheduleLatenessHistogram.WithLabelValues(append(cfg.Metrics.CommonLabelValues(), "/")...)
				sample := &dto.Metric{}
				if err := observer.(prometheus.Metric).Write(sample); err != nil {
					t.Fatal(err)
				}
				if got := sample.GetHistogram(); got.GetSampleCount() != tt.wantCount || got.GetSampleSum() != tt.wantSum {
					t.Errorf("lateness count/sum = %d/%g, want %d/%g seconds", got.GetSampleCount(), got.GetSampleSum(), tt.wantCount, tt.wantSum)
				}
			})
		})
	}
}
