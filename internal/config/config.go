package config

import (
	"errors"
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Replay  ReplayConfig         `yaml:"replay"`
	Metrics MetricsConfig        `yaml:"metrics"`
	Env     map[string]string    `yaml:"env"`
	Labels  CommonMetricLabelSet `yaml:"labels"`
	Target  TargetOverrideConfig `yaml:"target"`
	Header  HeaderRewriteConfig  `yaml:"header_rewrite"`
}

type ReplayConfig struct {
	MaxVirtualUsersPerEngine      int              `yaml:"max_virtual_users_per_engine"`
	MaxActiveConnectionsPerEngine int              `yaml:"max_active_connections_per_engine"`
	Timeout                       TimeoutConfig    `yaml:"timeout"`
	Retry                         RetryConfig      `yaml:"retry"`
	Validation                    ValidationConfig `yaml:"validation"`
}

type TimeoutConfig struct {
	Connect        time.Duration `yaml:"connect"`
	Request        time.Duration `yaml:"request"`
	IdleConnection time.Duration `yaml:"idle_connection"`
}

type RetryConfig struct {
	MaxAttempts     int      `yaml:"max_attempts"`
	Backoff         string   `yaml:"backoff"`
	RetryOnStatuses []int    `yaml:"retry_on_statuses"`
	RetryOnErrors   []string `yaml:"retry_on_errors"`
}

type ValidationConfig struct {
	Enabled       bool     `yaml:"enabled"`
	Status        bool     `yaml:"status"`
	Headers       bool     `yaml:"headers"`
	Body          bool     `yaml:"body"`
	IgnoreHeaders []string `yaml:"ignore_headers"`
}

type MetricsConfig struct {
	Enabled       bool   `yaml:"enabled"`
	ListenAddress string `yaml:"listen_address"`
	Path          string `yaml:"path"`
}

type CommonMetricLabelSet struct {
	CollectionID string `yaml:"collection_id"`
	PlanID       string `yaml:"plan_id"`
	RunID        string `yaml:"run_id"`
	EngineNo     string `yaml:"engine_no"`
	Zone         string `yaml:"zone"`
}

type TargetOverrideConfig struct {
	OverrideURL string `yaml:"override_url"`
}

type HeaderRewriteConfig struct {
	Drop []string          `yaml:"drop"`
	Set  map[string]string `yaml:"set"`
}

func Default() Config {
	return Config{
		Replay: ReplayConfig{
			MaxVirtualUsersPerEngine:      20,
			MaxActiveConnectionsPerEngine: 200,
			Timeout: TimeoutConfig{
				Connect:        3 * time.Second,
				Request:        30 * time.Second,
				IdleConnection: 60 * time.Second,
			},
			Retry: RetryConfig{
				MaxAttempts: 1,
				Backoff:     "none",
			},
			Validation: ValidationConfig{
				Enabled: true,
				Status:  true,
			},
		},
		Metrics: MetricsConfig{
			Enabled:       true,
			ListenAddress: "0.0.0.0:9102",
			Path:          "/metrics",
		},
		Env: map[string]string{},
		Labels: CommonMetricLabelSet{
			CollectionID: "unknown",
			PlanID:       "unknown",
			RunID:        "unknown",
			EngineNo:     "0",
			Zone:         "unknown",
		},
		Target: TargetOverrideConfig{},
		Header: HeaderRewriteConfig{Set: map[string]string{}},
	}
}

func Load(path string) (Config, error) {
	if path == "" {
		return Default(), nil
	}

	base := Default()
	b, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config: %w", err)
	}
	if err := yaml.Unmarshal(b, &base); err != nil {
		return Config{}, fmt.Errorf("parse yaml: %w", err)
	}
	if err := base.Validate(); err != nil {
		return Config{}, err
	}
	return base, nil
}

func (c Config) Validate() error {
	if c.Replay.MaxVirtualUsersPerEngine <= 0 {
		return errors.New("replay.max_virtual_users_per_engine must be > 0")
	}
	if c.Replay.MaxActiveConnectionsPerEngine <= 0 {
		return errors.New("replay.max_active_connections_per_engine must be > 0")
	}
	if c.Replay.Timeout.Connect <= 0 || c.Replay.Timeout.Request <= 0 || c.Replay.Timeout.IdleConnection <= 0 {
		return errors.New("replay.timeout values must be > 0")
	}
	if c.Replay.Retry.MaxAttempts <= 0 {
		return errors.New("replay.retry.max_attempts must be > 0")
	}
	if c.Metrics.Path == "" {
		return errors.New("metrics.path is required")
	}
	if c.Metrics.ListenAddress == "" {
		return errors.New("metrics.listen_address is required")
	}
	return nil
}
