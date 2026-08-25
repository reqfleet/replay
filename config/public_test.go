package config_test

import (
	"reflect"
	"strings"
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

func TestParseEmptyContentUsesDefaults(t *testing.T) {
	got, err := config.Parse(nil)
	if err != nil {
		t.Fatalf("config.Parse(nil) error: %v", err)
	}
	want := config.Default()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("config.Parse(nil) = %+v, want Default() %+v", got, want)
	}
}

func TestParseWithOverridesAppliesBeforeValidation(t *testing.T) {
	content := []byte("target:\n  disallow_recorded_targets: true\n")
	got, err := config.ParseWithOverrides(content, func(cfg *config.Config) {
		cfg.Target.OverrideURL = "https://override.example.test"
	})
	if err != nil {
		t.Fatalf("config.ParseWithOverrides(content, apply) error: %v", err)
	}
	if got.Target.OverrideURL != "https://override.example.test" {
		t.Errorf("config.ParseWithOverrides(content, apply).Target.OverrideURL = %q, want %q", got.Target.OverrideURL, "https://override.example.test")
	}
	if !got.Target.DisallowRecordedTargets {
		t.Error("config.ParseWithOverrides(content, apply).Target.DisallowRecordedTargets = false, want true")
	}
}

func TestParseRejectsInvalidTargetOverride(t *testing.T) {
	tests := []struct {
		name    string
		content string
		want    string
	}{
		{
			name:    "unsupported_scheme",
			content: "target:\n  override_url: ftp://example.test\n",
			want:    "scheme must be http or https",
		},
		{
			name:    "missing_hostname",
			content: "target:\n  override_url: https:///base\n",
			want:    "must include a hostname",
		},
		{
			name:    "disallowed_recorded_target_without_override",
			content: "target:\n  disallow_recorded_targets: true\n",
			want:    "recorded targets are disallowed",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := config.Parse([]byte(test.content))
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Errorf("config.Parse(%s) error = %v, want error containing %q", test.name, err, test.want)
			}
		})
	}
}
