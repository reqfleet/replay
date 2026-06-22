package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/goccy/go-yaml"
)

type Config struct {
	Replay  ReplayConfig         `yaml:"replay"`
	Metrics MetricsConfig        `yaml:"metrics"`
	Target  TargetOverrideConfig `yaml:"target"`

	Labels CommonMetricLabelSet `yaml:"labels"`
	Env    map[string]string    `yaml:"env"`

	Header HeaderRewriteConfig `yaml:"header_rewrite"`
}

type ReplayConfig struct {
	Retry       RetryConfig       `yaml:"retry"`
	Timeout     TimeoutConfig     `yaml:"timeout"`
	HTTP2       HTTP2Config       `yaml:"http2"`
	Idempotency IdempotencyConfig `yaml:"idempotency"`

	Checkpoint CheckpointConfig `yaml:"checkpoint"`
	Pacing     PacingConfig     `yaml:"pacing"`
	Sharding   ShardingConfig   `yaml:"sharding"`
	Validation ValidationConfig `yaml:"validation"`

	MaxVirtualUsersPerEngine      int           `yaml:"max_virtual_users_per_engine"`
	MaxActiveConnectionsPerEngine int           `yaml:"max_active_connections_per_engine"`
	RampupDuration                time.Duration `yaml:"rampup_duration"`

	Lifecycle LifecycleConfig `yaml:"lifecycle"`

	DryRun                 bool `yaml:"dry_run"`
	Verbose                bool `yaml:"verbose"`
	PartialSuccessExitZero bool `yaml:"partial_success_exit_zero"`
}

type TimeoutConfig struct {
	Connect        time.Duration `yaml:"connect"`
	Request        time.Duration `yaml:"request"`
	IdleConnection time.Duration `yaml:"idle_connection"`
}

type HTTP2Config struct {
	Mode                 string `yaml:"mode"`
	MaxConcurrentStreams int    `yaml:"max_concurrent_streams"`
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

type PacingConfig struct {
	Enabled       bool          `yaml:"enabled"`
	MaxSleepDelta time.Duration `yaml:"max_sleep_delta"`
}

type LifecycleConfig struct {
	RequireOpen  bool `yaml:"require_open"`
	RequireClose bool `yaml:"require_close"`
}

type IdempotencyConfig struct {
	Enabled               bool     `yaml:"enabled"`
	BlockMethods          []string `yaml:"block_methods"`
	RequireHeaderForAllow []string `yaml:"require_header_for_allow"`
}

type ShardingConfig struct {
	ShardIndex int `yaml:"shard_index"`
	ShardCount int `yaml:"shard_count"`
}

type CheckpointConfig struct {
	File string `yaml:"file"`
}

type MetricsConfig struct {
	Enabled                   bool          `yaml:"enabled"`
	ListenAddress             string        `yaml:"listen_address"`
	Path                      string        `yaml:"path"`
	PathTemplates             []string      `yaml:"path_templates"`
	MaxLabels                 int           `yaml:"max_labels"`
	GracefulTerminationPeriod time.Duration `yaml:"graceful_termination_period"`
}

type CommonMetricLabelSet struct {
	CollectionID    string `yaml:"collection_id"`
	CollectionIDEnv string `yaml:"collection_id_env"`
	PlanID          string `yaml:"plan_id"`
	PlanIDEnv       string `yaml:"plan_id_env"`
	RunID           string `yaml:"run_id"`
	RunIDEnv        string `yaml:"run_id_env"`
	EngineNo        string `yaml:"engine_no"`
	EngineNoEnv     string `yaml:"engine_no_env"`
	Zone            string `yaml:"zone"`
	ZoneEnv         string `yaml:"zone_env"`
}

type labelEnvSpec struct {
	value     *string
	configEnv string
}

type TargetOverrideConfig struct {
	OverrideURL string `yaml:"override_url"`
	Require     bool   `yaml:"require_override"`
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
			RampupDuration:                0,
			HTTP2: HTTP2Config{
				Mode:                 "serialized",
				MaxConcurrentStreams: 16,
			},
			DryRun:                 false,
			PartialSuccessExitZero: true,
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
			Pacing: PacingConfig{
				Enabled:       false,
				MaxSleepDelta: 30 * time.Second,
			},
			Lifecycle: LifecycleConfig{
				RequireOpen:  true,
				RequireClose: true,
			},
			Idempotency: IdempotencyConfig{
				Enabled:               true,
				BlockMethods:          []string{"POST", "PUT", "PATCH", "DELETE"},
				RequireHeaderForAllow: []string{"idempotency-key", "x-idempotency-key"},
			},
			Sharding: ShardingConfig{
				ShardIndex: 0,
				ShardCount: 1,
			},
			Checkpoint: CheckpointConfig{},
		},
		Metrics: MetricsConfig{
			Enabled:                   true,
			ListenAddress:             "0.0.0.0:9102",
			Path:                      "/metrics",
			PathTemplates:             []string{},
			MaxLabels:                 20,
			GracefulTerminationPeriod: 5 * time.Second,
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
	if c.Replay.RampupDuration < 0 {
		return errors.New("replay.rampup_duration must be >= 0")
	}
	if c.Replay.Timeout.Connect <= 0 || c.Replay.Timeout.Request <= 0 || c.Replay.Timeout.IdleConnection <= 0 {
		return errors.New("replay.timeout values must be > 0")
	}
	if c.Replay.Retry.MaxAttempts <= 0 {
		return errors.New("replay.retry.max_attempts must be > 0")
	}
	switch c.Replay.HTTP2.Mode {
	case "", "serialized", "multiplexed":
	default:
		return errors.New("replay.http2.mode must be one of: serialized, multiplexed")
	}
	if c.Replay.HTTP2.MaxConcurrentStreams <= 0 {
		return errors.New("replay.http2.max_concurrent_streams must be > 0")
	}
	if c.Replay.Pacing.MaxSleepDelta < 0 {
		return errors.New("replay.pacing.max_sleep_delta must be >= 0")
	}
	if c.Replay.Sharding.ShardCount <= 0 {
		return errors.New("replay.sharding.shard_count must be > 0")
	}
	if c.Replay.Sharding.ShardIndex < 0 || c.Replay.Sharding.ShardIndex >= c.Replay.Sharding.ShardCount {
		return errors.New("replay.sharding.shard_index must be within [0, shard_count)")
	}
	if c.Metrics.GracefulTerminationPeriod < 0 {
		return errors.New("metrics.graceful_termination_period must be >= 0")
	}

	if c.Metrics.Enabled {
		if c.Metrics.Path == "" {
			return errors.New("metrics.path is required")
		}
		if c.Metrics.ListenAddress == "" {
			return errors.New("metrics.listen_address is required")
		}
		if c.Metrics.MaxLabels < 0 {
			return errors.New("metrics.max_labels must be >= 0")
		}
	}
	return nil
}

// ApplyEnv applies well-known environment variable overrides to the config.
// Precedence order is: defaults -> YAML -> environment -> CLI (applied by caller).
func (c *Config) ApplyEnv() {
	c.applyLabelOverrides()

	if v, ok := os.LookupEnv("REPLAY_DRY_RUN"); ok {
		if b, err := strconv.ParseBool(v); err == nil {
			c.Replay.DryRun = b
		}
	}
	if v, ok := os.LookupEnv("REPLAY_VERBOSE"); ok {
		if b, err := strconv.ParseBool(v); err == nil {
			c.Replay.Verbose = b
		}
	}
	if v, ok := os.LookupEnv("REPLAY_OVERRIDE_URL"); ok && v != "" {
		c.Target.OverrideURL = v
	}
	if v, ok := os.LookupEnv("REPLAY_REQUIRE_OVERRIDE"); ok {
		if b, err := strconv.ParseBool(v); err == nil {
			c.Target.Require = b
		}
	}
	if v, ok := os.LookupEnv("METRICS_ENABLED"); ok {
		if b, err := strconv.ParseBool(v); err == nil {
			c.Metrics.Enabled = b
		}
	}
	if v, ok := os.LookupEnv("METRICS_LISTEN_ADDRESS"); ok && v != "" {
		c.Metrics.ListenAddress = v
	}
	if v, ok := os.LookupEnv("METRICS_PATH"); ok && v != "" {
		c.Metrics.Path = v
	}
	if v, ok := os.LookupEnv("METRICS_MAX_LABELS"); ok {
		if i, err := strconv.Atoi(v); err == nil {
			c.Metrics.MaxLabels = i
		}
	}
	if v, ok := os.LookupEnv("METRICS_GRACEFUL_TERMINATION_PERIOD"); ok && v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			c.Metrics.GracefulTerminationPeriod = d
		}
	}

	if v, ok := os.LookupEnv("REPLAY_PARTIAL_SUCCESS_EXIT_ZERO"); ok {
		if b, err := strconv.ParseBool(v); err == nil {
			c.Replay.PartialSuccessExitZero = b
		}
	}
}

func (c *Config) applyLabelOverrides() {
	for _, spec := range c.labelEnvSpecs() {
		*spec.value = c.resolveLabelValue(*spec.value, spec.configEnv)
	}
}

func (c *Config) labelEnvSpecs() []labelEnvSpec {
	return []labelEnvSpec{
		{value: &c.Labels.CollectionID, configEnv: c.Labels.CollectionIDEnv},
		{value: &c.Labels.PlanID, configEnv: c.Labels.PlanIDEnv},
		{value: &c.Labels.RunID, configEnv: c.Labels.RunIDEnv},
		{value: &c.Labels.EngineNo, configEnv: c.Labels.EngineNoEnv},
		{value: &c.Labels.Zone, configEnv: c.Labels.ZoneEnv},
	}
}

func (c *Config) resolveLabelValue(currentValue, envKey string) string {
	if envKey == "" {
		return currentValue
	}
	if value, ok := lookupEnvValue(envKey, c.Env); ok {
		return value
	}
	return currentValue
}

func lookupEnvValue(key string, fallbacks map[string]string) (string, bool) {
	if value, ok := os.LookupEnv(key); ok && value != "" {
		return value, true
	}
	if value, ok := fallbacks[key]; ok && value != "" {
		return value, true
	}
	return "", false
}
