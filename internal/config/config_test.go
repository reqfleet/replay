package config

import "testing"

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
