package config

import (
	"os"
	"reflect"
	"testing"
	"time"
)

func TestDefaultValidate(t *testing.T) {
	cfg := Default()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("default config invalid: %v", err)
	}
}

func TestDefaultMetricsGracefulTerminationPeriod(t *testing.T) {
	cfg := Default()
	if got, want := cfg.Metrics.GracefulTerminationPeriod, 5*time.Second; got != want {
		t.Fatalf("Default().Metrics.GracefulTerminationPeriod = %v, want %v", got, want)
	}
}

func TestDefaultRampupDuration(t *testing.T) {
	cfg := Default()
	if got, want := cfg.Replay.RampupDuration, time.Duration(0); got != want {
		t.Fatalf("Default().Replay.RampupDuration = %v, want %v", got, want)
	}
}

func TestValidateRejectsZeroLimits(t *testing.T) {
	cfg := Default()
	cfg.Replay.MaxVirtualUsersPerEngine = 0
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error for zero virtual users")
	}
}

func TestConfigPrecedence(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/cfg.yaml"
	content := "replay:\n" +
		"  dry_run: false\n" +
		"target:\n" +
		"  override_url: \"http://fromyaml\"\n" +
		"metrics:\n" +
		"  enabled: false\n" +
		"  namespace: \"custom\"\n" +
		"  listen_address: \"127.0.0.1:11111\"\n" +
		"  path: \"/m\"\n" +
		"  common_labels:\n" +
		"    - name: \"tenant_id\"\n" +
		"      value: \"tenant-yaml\"\n" +
		"      env: \"TENANT_ID_FROM_ENV\"\n" +
		"    - name: \"run_id\"\n" +
		"      value: \"run-yaml\"\n" +
		"      env: \"RUN_ID_FROM_ENV\"\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	// env should override YAML
	t.Setenv("REPLAY_OVERRIDE_URL", "http://fromenv")
	t.Setenv("REPLAY_DRY_RUN", "true")
	t.Setenv("METRICS_ENABLED", "true")
	t.Setenv("TENANT_ID_FROM_ENV", "tenant-from-config-env")
	t.Setenv("RUN_ID_FROM_ENV", "run-from-config-env")
	t.Setenv("METRICS_NAMESPACE", "envnamespace")
	t.Setenv("METRICS_GRACEFUL_TERMINATION_PERIOD", "2s")

	cfg.ApplyEnv()

	if cfg.Target.OverrideURL != "http://fromenv" {
		t.Fatalf("override url not taken from env: %v", cfg.Target.OverrideURL)
	}
	if cfg.Replay.DryRun != true {
		t.Fatalf("dry run not taken from env: %v", cfg.Replay.DryRun)
	}
	if cfg.Metrics.Enabled != true {
		t.Fatalf("metrics enabled not taken from env: %v", cfg.Metrics.Enabled)
	}
	if got, want := cfg.Metrics.GracefulTerminationPeriod, 2*time.Second; got != want {
		t.Fatalf("cfg.Metrics.GracefulTerminationPeriod = %v, want %v", got, want)
	}
	if got, want := cfg.Metrics.Namespace, "envnamespace"; got != want {
		t.Fatalf("cfg.Metrics.Namespace = %q, want %q", got, want)
	}
	wantLabels := []MetricLabel{
		{Name: "tenant_id", Value: "tenant-from-config-env", Env: "TENANT_ID_FROM_ENV"},
		{Name: "run_id", Value: "run-from-config-env", Env: "RUN_ID_FROM_ENV"},
	}
	if !reflect.DeepEqual(cfg.Metrics.CommonLabels, wantLabels) {
		t.Fatalf("cfg.Metrics.CommonLabels = %+v, want %+v", cfg.Metrics.CommonLabels, wantLabels)
	}

	// CLI (applied after ApplyEnv) should be highest precedence
	cfg.Target.OverrideURL = "http://cli"
	if cfg.Target.OverrideURL != "http://cli" {
		t.Fatalf("cli override not applied: %v", cfg.Target.OverrideURL)
	}
}

func TestLoadDefersTargetOverrideValidationUntilResolved(t *testing.T) {
	tests := []struct {
		name        string
		overrideURL string
	}{
		{name: "empty override", overrideURL: ""},
		{name: "malformed override", overrideURL: "http://[::1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			path := dir + "/cfg.yaml"
			content := "target:\n" +
				"  require_override: true\n" +
				"  override_url: \"" + tt.overrideURL + "\"\n"
			if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
				t.Fatalf("os.WriteFile(%q) error = %v, want nil", path, err)
			}

			cfg, err := Load(path)
			if err != nil {
				t.Fatalf("Load(%q) error = %v, want nil", path, err)
			}
			if got, want := cfg.Target.Require, true; got != want {
				t.Errorf("Load(%q).Target.Require = %t, want %t", path, got, want)
			}
			if got, want := cfg.Target.OverrideURL, tt.overrideURL; got != want {
				t.Errorf("Load(%q).Target.OverrideURL = %q, want %q", path, got, want)
			}
			if err := cfg.Validate(); err == nil {
				t.Fatalf("Load(%q).Validate() with unresolved target.override_url %q error = nil, want error", path, tt.overrideURL)
			}

			const effectiveOverrideURL = "https://effective.example"
			t.Setenv("REPLAY_OVERRIDE_URL", effectiveOverrideURL)
			cfg.ApplyEnv()
			if got, want := cfg.Target.OverrideURL, effectiveOverrideURL; got != want {
				t.Errorf("ApplyEnv().Target.OverrideURL = %q, want %q", got, want)
			}
			if err := cfg.Validate(); err != nil {
				t.Fatalf("Validate() after REPLAY_OVERRIDE_URL=%q error = %v, want nil", effectiveOverrideURL, err)
			}
		})
	}
}

func TestApplyEnvMetricLabelRefsFallback(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*Config)
		want      []MetricLabel
	}{
		{
			name: "falls back to literal yaml values when configured env vars are missing",
			configure: func(cfg *Config) {
				cfg.Metrics.CommonLabels = []MetricLabel{
					{Name: "tenant_id", Value: "tenant-yaml", Env: "MISSING_TENANT_ID"},
					{Name: "run_id", Value: "run-yaml", Env: "MISSING_RUN_ID"},
				}
			},
			want: []MetricLabel{
				{Name: "tenant_id", Value: "tenant-yaml", Env: "MISSING_TENANT_ID"},
				{Name: "run_id", Value: "run-yaml", Env: "MISSING_RUN_ID"},
			},
		},
		{
			name: "uses cfg env values before they are exported into process env",
			configure: func(cfg *Config) {
				cfg.Metrics.CommonLabels = []MetricLabel{
					{Name: "tenant_id", Value: "tenant-yaml", Env: "TENANT_ID_FROM_CFG_ENV"},
					{Name: "run_id", Value: "run-yaml", Env: "RUN_ID_FROM_CFG_ENV"},
				}
				cfg.Env["TENANT_ID_FROM_CFG_ENV"] = "tenant-from-cfg-env"
				cfg.Env["RUN_ID_FROM_CFG_ENV"] = "run-from-cfg-env"
			},
			want: []MetricLabel{
				{Name: "tenant_id", Value: "tenant-from-cfg-env", Env: "TENANT_ID_FROM_CFG_ENV"},
				{Name: "run_id", Value: "run-from-cfg-env", Env: "RUN_ID_FROM_CFG_ENV"},
			},
		},
		{
			name: "falls back to defaults when no literal override is provided",
			configure: func(cfg *Config) {
				cfg.Metrics.CommonLabels = []MetricLabel{
					{Name: "run_id", Value: "unknown", Env: "MISSING_RUN_ID"},
					{Name: "worker_id", Value: "0", Env: "MISSING_WORKER_ID"},
					{Name: "zone", Value: "unknown", Env: "MISSING_ZONE"},
				}
			},
			want: []MetricLabel{
				{Name: "run_id", Value: "unknown", Env: "MISSING_RUN_ID"},
				{Name: "worker_id", Value: "0", Env: "MISSING_WORKER_ID"},
				{Name: "zone", Value: "unknown", Env: "MISSING_ZONE"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Default()
			tt.configure(&cfg)

			cfg.ApplyEnv()

			if !reflect.DeepEqual(cfg.Metrics.CommonLabels, tt.want) {
				t.Fatalf("cfg.Metrics.CommonLabels = %+v, want %+v", cfg.Metrics.CommonLabels, tt.want)
			}
		})
	}
}

func TestDefaultPartialSuccessExitZero(t *testing.T) {
	cfg := Default()
	if got, want := cfg.Replay.PartialSuccessExitZero, true; got != want {
		t.Fatalf("Default().Replay.PartialSuccessExitZero = %v, want %v", got, want)
	}
}

func TestApplyEnvPartialSuccessExitZero(t *testing.T) {
	t.Setenv("REPLAY_PARTIAL_SUCCESS_EXIT_ZERO", "false")
	cfg := Default()
	cfg.ApplyEnv()
	if got, want := cfg.Replay.PartialSuccessExitZero, false; got != want {
		t.Fatalf("cfg.Replay.PartialSuccessExitZero = %v, want %v", got, want)
	}
}

func TestApplyEnvMetricsGracefulTerminationPeriod(t *testing.T) {
	t.Setenv("METRICS_GRACEFUL_TERMINATION_PERIOD", "750ms")
	cfg := Default()
	cfg.ApplyEnv()
	if got, want := cfg.Metrics.GracefulTerminationPeriod, 750*time.Millisecond; got != want {
		t.Fatalf("cfg.Metrics.GracefulTerminationPeriod = %v, want %v", got, want)
	}
}

func TestLoadParsesRampupDuration(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/cfg.yaml"
	content := "replay:\n" +
		"  rampup_duration: 3s\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load(%q) error: %v", path, err)
	}
	if got, want := cfg.Replay.RampupDuration, 3*time.Second; got != want {
		t.Fatalf("cfg.Replay.RampupDuration = %v, want %v", got, want)
	}
}

func TestSampleConfigLoads(t *testing.T) {
	if _, err := Load("../../config.yaml"); err != nil {
		t.Fatalf("Load sample config: %v", err)
	}
}

func TestValidateRejectsNegativeRampupDuration(t *testing.T) {
	cfg := Default()
	cfg.Replay.RampupDuration = -1 * time.Second
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error for negative rampup duration")
	}
}

func TestValidateRejectsNegativeMetricsGracefulTerminationPeriod(t *testing.T) {
	cfg := Default()
	cfg.Metrics.GracefulTerminationPeriod = -1 * time.Second
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error for negative metrics graceful termination period")
	}
}

func TestValidateRejectsInvalidMetricCommonLabels(t *testing.T) {
	tests := []struct {
		name   string
		labels []MetricLabel
	}{
		{
			name:   "empty",
			labels: []MetricLabel{{Name: "", Value: "x"}},
		},
		{
			name:   "reserved",
			labels: []MetricLabel{{Name: "status", Value: "x"}},
		},
		{
			name:   "reserved_prefix",
			labels: []MetricLabel{{Name: "__tenant_id", Value: "x"}},
		},
		{
			name:   "duplicate",
			labels: []MetricLabel{{Name: "tenant_id", Value: "a"}, {Name: "tenant_id", Value: "b"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Default()
			cfg.Metrics.CommonLabels = tt.labels
			if err := cfg.Validate(); err == nil {
				t.Fatalf("Validate() with labels %+v error = nil, want error", tt.labels)
			}
		})
	}
}

func TestValidateRejectsInvalidMetricsNamespace(t *testing.T) {
	cfg := Default()
	cfg.Metrics.Namespace = "bad-namespace"
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() with invalid metrics namespace error = nil, want error")
	}
}

func TestApplyEnvInvalidMetricsNamespaceFailsValidation(t *testing.T) {
	t.Setenv("METRICS_NAMESPACE", "bad-namespace")
	cfg := Default()
	cfg.ApplyEnv()
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() after invalid METRICS_NAMESPACE error = nil, want error")
	}
}

func TestValidateTargetOverride(t *testing.T) {
	tests := []struct {
		name        string
		overrideURL string
		require     bool
		wantErr     bool
	}{
		{name: "optional and empty", wantErr: false},
		{name: "required and empty", require: true, wantErr: true},
		{name: "relative URL", overrideURL: "example.com", wantErr: true},
		{name: "unsupported scheme", overrideURL: "file://example.com/path", wantErr: true},
		{name: "missing hostname", overrideURL: "http://:8080", require: true, wantErr: true},
		{name: "absolute HTTP URL", overrideURL: "http://example.com", require: true, wantErr: false},
		{name: "absolute HTTPS URL", overrideURL: "https://example.com", require: true, wantErr: false},
		{name: "uppercase HTTPS URL", overrideURL: "HTTPS://example.com", require: true, wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Default()
			cfg.Target.OverrideURL = tt.overrideURL
			cfg.Target.Require = tt.require
			err := cfg.Validate()
			if tt.wantErr && err == nil {
				t.Fatal("Validate() error = nil, want error")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("Validate() error = %v, want nil", err)
			}
		})
	}
}

func TestValidateRejectsCaseInsensitiveDuplicateSetHeaders(t *testing.T) {
	cfg := Default()
	cfg.Header.Set = map[string]string{
		"Idempotency-Key": "first",
		"idempotency-key": "second",
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() error = nil, want duplicate header error")
	}
}
