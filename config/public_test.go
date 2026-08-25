package config_test

import (
	"testing"
	"time"

	"github.com/reqfleet/replay/config"
)

func TestParseOverlaysReplayDefaults(t *testing.T) {
	content := []byte(`
replay:
  rampup_duration: 2s
metrics:
  namespace: embedded
`)

	got, err := config.Parse(content)
	if err != nil {
		t.Fatalf("config.Parse(content) error: %v", err)
	}
	if got.Replay.RampupDuration != 2*time.Second {
		t.Errorf("config.Parse(content).Replay.RampupDuration = %v, want %v", got.Replay.RampupDuration, 2*time.Second)
	}
	if got.Replay.Timeout != config.Default().Replay.Timeout {
		t.Errorf("config.Parse(content).Replay.Timeout = %+v, want default %+v", got.Replay.Timeout, config.Default().Replay.Timeout)
	}
	if got.Metrics.Namespace != "embedded" {
		t.Errorf("config.Parse(content).Metrics.Namespace = %q, want %q", got.Metrics.Namespace, "embedded")
	}
}
