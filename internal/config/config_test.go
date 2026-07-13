package config

import (
	"net/http"
	"os"
	"reflect"
	"strings"
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

func TestDefaultMaxActiveConnectionsIsUnlimited(t *testing.T) {
	cfg := Default()
	if got, want := cfg.Replay.MaxActiveConnectionsPerEngine, 0; got != want {
		t.Fatalf("Default().Replay.MaxActiveConnectionsPerEngine = %d, want %d", got, want)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() with unlimited active connections error: %v", err)
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
	t.Setenv("REPLAY_DISALLOW_RECORDED_TARGETS", "true")
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
	if !cfg.Target.DisallowRecordedTargets {
		t.Fatal("cfg.Target.DisallowRecordedTargets = false, want true")
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

func TestLoadParsesMaxActiveConnectionsPerEngine(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/cfg.yaml"
	content := "replay:\n" +
		"  max_active_connections_per_engine: 1000\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load(%q) error: %v", path, err)
	}
	if got, want := cfg.Replay.MaxActiveConnectionsPerEngine, 1000; got != want {
		t.Fatalf("cfg.Replay.MaxActiveConnectionsPerEngine = %d, want %d", got, want)
	}
}

func TestLoadRejectsEmptyHTTP2Mode(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/cfg.yaml"
	content := "replay:\n" +
		"  http2:\n" +
		"    mode: \"\"\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	if _, err := Load(path); err == nil {
		t.Fatalf("Load(%q) error = nil, want replay.http2.mode validation error", path)
	}
}

func TestSampleConfigLoads(t *testing.T) {
	if _, err := Load("../../config.yaml"); err != nil {
		t.Fatalf("Load sample config: %v", err)
	}
}

func TestValidateRetryConfiguration(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr bool
	}{
		{
			name: "known values",
			mutate: func(cfg *Config) {
				cfg.Replay.Retry.Backoff = "EXPONENTIAL"
				cfg.Replay.Retry.RetryOnStatuses = []int{100, 599}
				cfg.Replay.Retry.RetryOnErrors = []string{"TIMEOUT", "connection_reset", "network", "tls"}
			},
		},
		{
			name: "unknown backoff",
			mutate: func(cfg *Config) {
				cfg.Replay.Retry.Backoff = "immediate"
			},
			wantErr: true,
		},
		{
			name: "status below HTTP range",
			mutate: func(cfg *Config) {
				cfg.Replay.Retry.RetryOnStatuses = []int{99}
			},
			wantErr: true,
		},
		{
			name: "status above HTTP range",
			mutate: func(cfg *Config) {
				cfg.Replay.Retry.RetryOnStatuses = []int{600}
			},
			wantErr: true,
		},
		{
			name: "unknown error category",
			mutate: func(cfg *Config) {
				cfg.Replay.Retry.RetryOnErrors = []string{"server_busy"}
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Default()
			tt.mutate(&cfg)
			err := cfg.Validate()
			if gotErr := err != nil; gotErr != tt.wantErr {
				t.Errorf("Config.Validate() error = %v, want error = %t", err, tt.wantErr)
			}
		})
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

func TestValidateMetricsPath(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		wantErr bool
	}{
		{name: "default", path: "/metrics"},
		{name: "nested", path: "/custom/metrics"},
		{name: "encoded braces", path: "/metrics/%7Btenant%7D"},
		{name: "missing leading slash", path: "metrics", wantErr: true},
		{name: "space", path: "/metrics bad", wantErr: true},
		{name: "tab", path: "/metrics\tbad", wantErr: true},
		{name: "wildcard start", path: "/metrics/{", wantErr: true},
		{name: "wildcard", path: "/metrics/{tenant}", wantErr: true},
		{name: "duplicate wildcard", path: "/metrics/{tenant}/{tenant}", wantErr: true},
		{name: "query", path: "/metrics?format=openmetrics", wantErr: true},
		{name: "empty query", path: "/metrics?", wantErr: true},
		{name: "fragment", path: "/metrics#fragment", wantErr: true},
		{name: "bad escape", path: "/metrics%zz", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Default()
			cfg.Metrics.Path = tt.path
			err := cfg.Validate()
			if tt.wantErr && (err == nil || !strings.Contains(err.Error(), "metrics.path")) {
				t.Fatalf("Validate(metrics.path=%q) error = %v, want metrics.path error", tt.path, err)
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("Validate(metrics.path=%q) error: %v", tt.path, err)
			}
			if !tt.wantErr {
				mux := http.NewServeMux()
				mux.Handle(tt.path, http.NotFoundHandler())
			}
		})
	}

	cfg := Default()
	cfg.Metrics.Enabled = false
	cfg.Metrics.Path = "metrics"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() with disabled metrics and malformed path error: %v", err)
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

func TestValidateRejectsCaseInsensitiveDuplicateSetHeaders(t *testing.T) {
	cfg := Default()
	cfg.Header.Set = map[string]string{
		"Idempotency-Key": "first",
		"idempotency-key": "second",
	}

	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() with case-insensitive duplicate header_rewrite.set keys error = nil, want error")
	}
}

func TestValidateHeaderRewrite(t *testing.T) {
	tests := []struct {
		name    string
		rewrite HeaderRewriteConfig
		wantErr bool
	}{
		{name: "ordinary token name", rewrite: HeaderRewriteConfig{Set: map[string]string{"X-!#$%&'*+-.^_`|~": "value"}}},
		{name: "host", rewrite: HeaderRewriteConfig{Set: map[string]string{"Host": "replay.example.com"}}},
		{name: "authority", rewrite: HeaderRewriteConfig{Set: map[string]string{":authority": "replay.example.com"}}},
		{name: "horizontal tab value", rewrite: HeaderRewriteConfig{Set: map[string]string{"X-Test": "one\ttwo"}}},
		{name: "non ASCII value", rewrite: HeaderRewriteConfig{Set: map[string]string{"X-Test": "café"}}},
		{name: "empty drop name", rewrite: HeaderRewriteConfig{Drop: []string{""}}, wantErr: true},
		{name: "space in drop name", rewrite: HeaderRewriteConfig{Drop: []string{"Bad Name"}}, wantErr: true},
		{name: "unsupported pseudo header", rewrite: HeaderRewriteConfig{Drop: []string{":path"}}, wantErr: true},
		{name: "space in set name", rewrite: HeaderRewriteConfig{Set: map[string]string{"Bad Name": "value"}}, wantErr: true},
		{name: "newline in set name", rewrite: HeaderRewriteConfig{Set: map[string]string{"Bad\nName": "value"}}, wantErr: true},
		{name: "CRLF value", rewrite: HeaderRewriteConfig{Set: map[string]string{"X-Test": "one\r\nInjected: true"}}, wantErr: true},
		{name: "NUL value", rewrite: HeaderRewriteConfig{Set: map[string]string{"X-Test": "one\x00two"}}, wantErr: true},
		{name: "control value", rewrite: HeaderRewriteConfig{Set: map[string]string{"X-Test": "one\x01two"}}, wantErr: true},
		{name: "DEL value", rewrite: HeaderRewriteConfig{Set: map[string]string{"X-Test": "one\x7ftwo"}}, wantErr: true},
		{
			name: "host and authority alias",
			rewrite: HeaderRewriteConfig{Set: map[string]string{
				"Host":       "first.example.com",
				":authority": "second.example.com",
			}},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Default()
			cfg.Header = tt.rewrite
			err := cfg.Validate()
			if tt.wantErr && (err == nil || !strings.Contains(err.Error(), "header_rewrite")) {
				t.Fatalf("Validate(%+v) error = %v, want header_rewrite error", tt.rewrite, err)
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("Validate(%+v) error: %v", tt.rewrite, err)
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
		name    string
		target  TargetOverrideConfig
		want    string
		wantErr bool
	}{
		{
			name:   "valid_absolute_url",
			target: TargetOverrideConfig{OverrideURL: "https://staging.example.com/base", DisallowRecordedTargets: true},
			want:   "https://staging.example.com/base",
		},
		{
			name:    "invalid_url",
			target:  TargetOverrideConfig{OverrideURL: "example.com"},
			wantErr: true,
		},
		{
			name:    "unsupported_scheme",
			target:  TargetOverrideConfig{OverrideURL: "ftp://staging.example.com"},
			wantErr: true,
		},
		{
			name:    "missing_hostname",
			target:  TargetOverrideConfig{OverrideURL: "https:///base"},
			wantErr: true,
		},
		{
			name:    "recorded_targets_disallowed_without_override",
			target:  TargetOverrideConfig{DisallowRecordedTargets: true},
			wantErr: true,
		},
		{
			name:   "optional_missing_url",
			target: TargetOverrideConfig{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.target.ParseURL()
			if gotErr := err != nil; gotErr != tt.wantErr {
				t.Fatalf("TargetOverrideConfig.ParseURL() error = %v, want error presence = %t", err, tt.wantErr)
			}
			if err != nil {
				return
			}
			if got == nil {
				if tt.want != "" {
					t.Fatalf("TargetOverrideConfig.ParseURL() = nil, want %q", tt.want)
				}
				return
			}
			if got.String() != tt.want {
				t.Errorf("TargetOverrideConfig.ParseURL() = %q, want %q", got, tt.want)
			}
		})
	}
}
