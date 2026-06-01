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
	content := "replay:\n" +
		"  dry_run: false\n" +
		"target:\n" +
		"  override_url: \"http://fromyaml\"\n" +
		"metrics:\n" +
		"  enabled: false\n" +
		"  listen_address: \"127.0.0.1:11111\"\n" +
		"  path: \"/m\"\n" +
		"labels:\n" +
		"  collection_id: \"col-yaml\"\n" +
		"  collection_id_env: \"COLLECTION_ID_FROM_ENV\"\n" +
		"  plan_id: \"plan-yaml\"\n" +
		"  plan_id_env: \"PLAN_ID_FROM_ENV\"\n" +
		"  run_id: \"run-yaml\"\n" +
		"  run_id_env: \"RUN_ID_FROM_ENV\"\n" +
		"  engine_no: \"1\"\n" +
		"  engine_no_env: \"ENGINE_NO_FROM_ENV\"\n" +
		"  zone: \"zone-yaml\"\n" +
		"  zone_env: \"ZONE_FROM_ENV\"\n"
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
	t.Setenv("COLLECTION_ID_FROM_ENV", "col-from-config-env")
	t.Setenv("PLAN_ID_FROM_ENV", "plan-from-config-env")
	t.Setenv("RUN_ID_FROM_ENV", "run-from-config-env")
	t.Setenv("ENGINE_NO_FROM_ENV", "17")
	t.Setenv("ZONE_FROM_ENV", "zone-from-config-env")

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
	if cfg.Labels.CollectionID != "col-from-config-env" {
		t.Fatalf("cfg.Labels.CollectionID = %q, want %q", cfg.Labels.CollectionID, "col-from-config-env")
	}
	if cfg.Labels.PlanID != "plan-from-config-env" {
		t.Fatalf("cfg.Labels.PlanID = %q, want %q", cfg.Labels.PlanID, "plan-from-config-env")
	}
	if cfg.Labels.RunID != "run-from-config-env" {
		t.Fatalf("cfg.Labels.RunID = %q, want %q", cfg.Labels.RunID, "run-from-config-env")
	}
	if cfg.Labels.EngineNo != "17" {
		t.Fatalf("cfg.Labels.EngineNo = %q, want %q", cfg.Labels.EngineNo, "17")
	}
	if cfg.Labels.Zone != "zone-from-config-env" {
		t.Fatalf("cfg.Labels.Zone = %q, want %q", cfg.Labels.Zone, "zone-from-config-env")
	}

	// CLI (applied after ApplyEnv) should be highest precedence
	cfg.Target.OverrideURL = "http://cli"
	if cfg.Target.OverrideURL != "http://cli" {
		t.Fatalf("cli override not applied: %v", cfg.Target.OverrideURL)
	}
}

func TestApplyEnvLabelRefsFallback(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*Config)
		want      CommonMetricLabelSet
	}{
		{
			name: "falls back to literal yaml values when configured env vars are missing",
			configure: func(cfg *Config) {
				cfg.Labels.CollectionID = "col-yaml"
				cfg.Labels.CollectionIDEnv = "MISSING_COLLECTION_ID"
				cfg.Labels.PlanID = "plan-yaml"
				cfg.Labels.PlanIDEnv = "MISSING_PLAN_ID"
				cfg.Labels.RunID = "run-yaml"
				cfg.Labels.RunIDEnv = "MISSING_RUN_ID"
				cfg.Labels.EngineNo = "1"
				cfg.Labels.EngineNoEnv = "MISSING_ENGINE_NO"
				cfg.Labels.Zone = "zone-yaml"
				cfg.Labels.ZoneEnv = "MISSING_ZONE"
			},
			want: CommonMetricLabelSet{
				CollectionID: "col-yaml",
				PlanID:       "plan-yaml",
				RunID:        "run-yaml",
				EngineNo:     "1",
				Zone:         "zone-yaml",
			},
		},
		{
			name: "uses cfg env values before they are exported into process env",
			configure: func(cfg *Config) {
				cfg.Labels.CollectionID = "col-yaml"
				cfg.Labels.CollectionIDEnv = "COLLECTION_ID_FROM_CFG_ENV"
				cfg.Labels.PlanID = "plan-yaml"
				cfg.Labels.PlanIDEnv = "PLAN_ID_FROM_CFG_ENV"
				cfg.Labels.RunID = "run-yaml"
				cfg.Labels.RunIDEnv = "RUN_ID_FROM_CFG_ENV"
				cfg.Labels.EngineNo = "1"
				cfg.Labels.EngineNoEnv = "ENGINE_NO_FROM_CFG_ENV"
				cfg.Labels.Zone = "zone-yaml"
				cfg.Labels.ZoneEnv = "ZONE_FROM_CFG_ENV"
				cfg.Env["COLLECTION_ID_FROM_CFG_ENV"] = "col-from-cfg-env"
				cfg.Env["PLAN_ID_FROM_CFG_ENV"] = "plan-from-cfg-env"
				cfg.Env["RUN_ID_FROM_CFG_ENV"] = "run-from-cfg-env"
				cfg.Env["ENGINE_NO_FROM_CFG_ENV"] = "9"
				cfg.Env["ZONE_FROM_CFG_ENV"] = "zone-from-cfg-env"
			},
			want: CommonMetricLabelSet{
				CollectionID: "col-from-cfg-env",
				PlanID:       "plan-from-cfg-env",
				RunID:        "run-from-cfg-env",
				EngineNo:     "9",
				Zone:         "zone-from-cfg-env",
			},
		},
		{
			name: "falls back to defaults when no literal override is provided",
			configure: func(cfg *Config) {
				cfg.Labels.CollectionIDEnv = "MISSING_COLLECTION_ID"
				cfg.Labels.PlanIDEnv = "MISSING_PLAN_ID"
				cfg.Labels.RunIDEnv = "MISSING_RUN_ID"
				cfg.Labels.EngineNoEnv = "MISSING_ENGINE_NO"
				cfg.Labels.ZoneEnv = "MISSING_ZONE"
			},
			want: CommonMetricLabelSet{
				CollectionID: "unknown",
				PlanID:       "unknown",
				RunID:        "unknown",
				EngineNo:     "0",
				Zone:         "unknown",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Default()
			tt.configure(&cfg)

			cfg.ApplyEnv()

			if got := cfg.Labels.CollectionID; got != tt.want.CollectionID {
				t.Fatalf("cfg.Labels.CollectionID = %q, want %q", got, tt.want.CollectionID)
			}
			if got := cfg.Labels.PlanID; got != tt.want.PlanID {
				t.Fatalf("cfg.Labels.PlanID = %q, want %q", got, tt.want.PlanID)
			}
			if got := cfg.Labels.RunID; got != tt.want.RunID {
				t.Fatalf("cfg.Labels.RunID = %q, want %q", got, tt.want.RunID)
			}
			if got := cfg.Labels.EngineNo; got != tt.want.EngineNo {
				t.Fatalf("cfg.Labels.EngineNo = %q, want %q", got, tt.want.EngineNo)
			}
			if got := cfg.Labels.Zone; got != tt.want.Zone {
				t.Fatalf("cfg.Labels.Zone = %q, want %q", got, tt.want.Zone)
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
