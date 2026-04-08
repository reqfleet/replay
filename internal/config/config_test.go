package config

import (
	"os"
	"testing"
)

func TestDefaultValidate(t *testing.T) {
	cfg := Default()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("default config invalid: %v", err)
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
	content := `replay:
  dry_run: false
target:
  override_url: "http://fromyaml"
metrics:
  enabled: false
  listen_address: "127.0.0.1:11111"
  path: "/m"
labels:
  collection_id: "col-yaml"
  plan_id: "plan-yaml"
  run_id: "run-yaml"
  engine_no: "1"
  zone: "zone-yaml"
`
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
	t.Setenv("REPLAY_LABEL_COLLECTION_ID", "col-env")

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
	if cfg.Labels.CollectionID != "col-env" {
		t.Fatalf("label collection id not taken from env: %v", cfg.Labels.CollectionID)
	}

	// CLI (applied after ApplyEnv) should be highest precedence
	cfg.Target.OverrideURL = "http://cli"
	if cfg.Target.OverrideURL != "http://cli" {
		t.Fatalf("cli override not applied: %v", cfg.Target.OverrideURL)
	}
}
