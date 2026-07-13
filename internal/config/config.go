// Package config loads and validates replay runtime configuration.
package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/goccy/go-yaml"
)

type Config struct {
	Replay  ReplayConfig         `yaml:"replay"`
	Metrics MetricsConfig        `yaml:"metrics"`
	Target  TargetOverrideConfig `yaml:"target"`

	Env map[string]string `yaml:"env"`

	Header HeaderRewriteConfig `yaml:"header_rewrite"`
}

type ReplayConfig struct {
	Retry       RetryConfig       `yaml:"retry"`
	Timeout     TimeoutConfig     `yaml:"timeout"`
	HTTP2       HTTP2Config       `yaml:"http2"`
	TLS         TLSConfig         `yaml:"tls"`
	Idempotency IdempotencyConfig `yaml:"idempotency"`

	Checkpoint CheckpointConfig `yaml:"checkpoint"`
	Pacing     PacingConfig     `yaml:"pacing"`
	Sharding   ShardingConfig   `yaml:"sharding"`
	Validation ValidationConfig `yaml:"validation"`

	MaxVirtualUsersPerEngine int `yaml:"max_virtual_users_per_engine"`
	// MaxActiveConnectionsPerEngine is reserved for connection admission; 0 means unlimited.
	MaxActiveConnectionsPerEngine int           `yaml:"max_active_connections_per_engine"`
	RampupDuration                time.Duration `yaml:"rampup_duration"`

	Lifecycle LifecycleConfig `yaml:"lifecycle"`

	DryRun                 bool `yaml:"dry_run"`
	Verbose                bool `yaml:"verbose"`
	PartialSuccessExitZero bool `yaml:"partial_success_exit_zero"`
}

type TLSConfig struct {
	InsecureSkipVerify bool `yaml:"insecure_skip_verify"`
}

type TimeoutConfig struct {
	Connect        time.Duration `yaml:"connect"`
	Request        time.Duration `yaml:"request"`
	IdleConnection time.Duration `yaml:"idle_connection"`
}

type HTTP2Config struct {
	Mode string `yaml:"mode"`
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
	RequireOpen bool `yaml:"require_open"`
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
	Namespace                 string        `yaml:"namespace"`
	ListenAddress             string        `yaml:"listen_address"`
	Path                      string        `yaml:"path"`
	PathTemplates             []string      `yaml:"path_templates"`
	MaxLabels                 int           `yaml:"max_labels"`
	CommonLabels              []MetricLabel `yaml:"common_labels"`
	GracefulTerminationPeriod time.Duration `yaml:"graceful_termination_period"`
}

type MetricLabel struct {
	Name  string `yaml:"name"`
	Value string `yaml:"value"`
	Env   string `yaml:"env"`
}

type TargetOverrideConfig struct {
	OverrideURL             string `yaml:"override_url"`
	DisallowRecordedTargets bool   `yaml:"disallow_recorded_targets"`
}

// ParseURL validates and parses the configured target override.
func (c TargetOverrideConfig) ParseURL() (*url.URL, error) {
	if c.OverrideURL == "" {
		if c.DisallowRecordedTargets {
			return nil, errors.New("recorded targets are disallowed but target.override_url is empty")
		}
		return nil, nil
	}

	parsed, err := url.Parse(c.OverrideURL)
	if err != nil {
		return nil, fmt.Errorf("parse target.override_url: %w", err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, errors.New("target.override_url scheme must be http or https")
	}
	if parsed.Hostname() == "" {
		return nil, errors.New("target.override_url must include a hostname")
	}
	return parsed, nil
}

type HeaderRewriteConfig struct {
	Drop []string          `yaml:"drop"`
	Set  map[string]string `yaml:"set"`
}

func Default() Config {
	return Config{
		Replay: ReplayConfig{
			MaxVirtualUsersPerEngine:      20,
			MaxActiveConnectionsPerEngine: 0,
			RampupDuration:                0,
			HTTP2: HTTP2Config{
				Mode: "serialized",
			},
			TLS: TLSConfig{
				InsecureSkipVerify: false,
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
				RequireOpen: true,
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
			Enabled:       true,
			Namespace:     "replay",
			ListenAddress: "0.0.0.0:9102",
			Path:          "/metrics",
			PathTemplates: []string{},
			MaxLabels:     20,
			CommonLabels: []MetricLabel{
				{Name: "run_id", Value: "unknown", Env: "REPLAY_RUN_ID"},
				{Name: "worker_id", Value: "0", Env: "REPLAY_WORKER_ID"},
				{Name: "zone", Value: "unknown", Env: "REPLAY_ZONE"},
			},
			GracefulTerminationPeriod: 5 * time.Second,
		},
		Env:    map[string]string{},
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
	if c.Replay.RampupDuration < 0 {
		return errors.New("replay.rampup_duration must be >= 0")
	}
	if c.Replay.Timeout.Connect <= 0 || c.Replay.Timeout.Request <= 0 || c.Replay.Timeout.IdleConnection <= 0 {
		return errors.New("replay.timeout values must be > 0")
	}
	if c.Replay.Retry.MaxAttempts <= 0 {
		return errors.New("replay.retry.max_attempts must be > 0")
	}
	if err := validateRetryConfig(c.Replay.Retry); err != nil {
		return err
	}
	switch c.Replay.HTTP2.Mode {
	case "serialized", "multiplexed":
	default:
		return errors.New("replay.http2.mode must be one of: serialized, multiplexed")
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
	if c.Metrics.MaxLabels < 0 {
		return errors.New("metrics.max_labels must be >= 0")
	}
	if err := validateHeaderRewrite(c.Header); err != nil {
		return err
	}
	if c.Metrics.Namespace != "" && !isValidMetricLabelName(c.Metrics.Namespace) {
		return errors.New("metrics.namespace must be a valid Prometheus metric namespace")
	}
	if err := validateMetricLabels(c.Metrics.CommonLabels); err != nil {
		return err
	}
	if c.Metrics.Enabled {
		if c.Metrics.Namespace == "" {
			return errors.New("metrics.namespace is required")
		}
		if err := validateMetricsPath(c.Metrics.Path); err != nil {
			return err
		}
		if c.Metrics.ListenAddress == "" {
			return errors.New("metrics.listen_address is required")
		}
	}
	return nil
}

func validateRetryConfig(retry RetryConfig) error {
	if !strings.EqualFold(retry.Backoff, "none") &&
		!strings.EqualFold(retry.Backoff, "fixed") &&
		!strings.EqualFold(retry.Backoff, "exponential") {
		return fmt.Errorf("replay.retry.backoff %q must be one of: none, fixed, exponential", retry.Backoff)
	}
	for _, status := range retry.RetryOnStatuses {
		if status < 100 || status > 599 {
			return fmt.Errorf("replay.retry.retry_on_statuses contains invalid HTTP status %d", status)
		}
	}
	for _, category := range retry.RetryOnErrors {
		if !strings.EqualFold(category, "timeout") &&
			!strings.EqualFold(category, "connection_reset") &&
			!strings.EqualFold(category, "network") &&
			!strings.EqualFold(category, "tls") {
			return fmt.Errorf(
				"replay.retry.retry_on_errors category %q must be one of: timeout, connection_reset, network, tls",
				category,
			)
		}
	}
	return nil
}

// ApplyEnv applies well-known environment variable overrides to the config.
// Precedence order is: defaults -> YAML -> environment -> CLI (applied by caller).
func (c *Config) ApplyEnv() {
	c.applyMetricLabelOverrides()
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
	if v, ok := os.LookupEnv("REPLAY_DISALLOW_RECORDED_TARGETS"); ok {
		if b, err := strconv.ParseBool(v); err == nil {
			c.Target.DisallowRecordedTargets = b
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
	if v, ok := os.LookupEnv("METRICS_NAMESPACE"); ok && v != "" {
		c.Metrics.Namespace = v
	}

	if v, ok := os.LookupEnv("REPLAY_PARTIAL_SUCCESS_EXIT_ZERO"); ok {
		if b, err := strconv.ParseBool(v); err == nil {
			c.Replay.PartialSuccessExitZero = b
		}
	}
}

func (c *Config) applyMetricLabelOverrides() {
	for i := range c.Metrics.CommonLabels {
		label := &c.Metrics.CommonLabels[i]
		label.Value = c.resolveLabelValue(label.Value, label.Env)
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

func (m MetricsConfig) CommonLabelNames() []string {
	names := make([]string, len(m.CommonLabels))
	for i, label := range m.CommonLabels {
		names[i] = label.Name
	}
	return names
}

func (m MetricsConfig) CommonLabelValues() []string {
	values := make([]string, len(m.CommonLabels))
	for i, label := range m.CommonLabels {
		values[i] = label.Value
	}
	return values
}

func (m MetricsConfig) CommonLabelAttrs() []any {
	attrs := make([]any, 0, len(m.CommonLabels)*2)
	for _, label := range m.CommonLabels {
		attrs = append(attrs, label.Name, label.Value)
	}
	return attrs
}

func validateMetricLabels(labels []MetricLabel) error {
	seen := make(map[string]struct{}, len(labels))
	for _, label := range labels {
		if !isValidMetricLabelName(label.Name) {
			return fmt.Errorf("metrics.common_labels contains invalid label name %q", label.Name)
		}
		if isReservedMetricLabel(label.Name) {
			return fmt.Errorf("metrics.common_labels contains reserved label name %q", label.Name)
		}
		if _, ok := seen[label.Name]; ok {
			return fmt.Errorf("metrics.common_labels contains duplicate label name %q", label.Name)
		}
		seen[label.Name] = struct{}{}
	}
	return nil
}

func validateMetricsPath(path string) error {
	if path == "" {
		return errors.New("metrics.path is required")
	}
	if !strings.HasPrefix(path, "/") {
		return errors.New("metrics.path must be an absolute URL path")
	}
	if strings.ContainsAny(path, "{}") {
		return errors.New("metrics.path must be a literal URL path without wildcards")
	}
	if strings.ContainsAny(path, "?#") {
		return errors.New("metrics.path must not contain a query or fragment")
	}
	if strings.IndexFunc(path, func(r rune) bool { return r <= ' ' || r == '\x7f' }) >= 0 {
		return errors.New("metrics.path must not contain spaces or control characters")
	}
	parsed, err := url.ParseRequestURI(path)
	if err != nil {
		return fmt.Errorf("metrics.path must be a valid URL path: %w", err)
	}
	if parsed.Scheme != "" || parsed.Host != "" || parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" {
		return errors.New("metrics.path must not contain a scheme, host, query, or fragment")
	}
	return nil
}

func validateHeaderRewrite(rewrite HeaderRewriteConfig) error {
	for _, name := range rewrite.Drop {
		if !isValidRewriteHeaderName(name) {
			return fmt.Errorf("header_rewrite.drop contains invalid header name %q", name)
		}
	}

	seen := make(map[string]string, len(rewrite.Set))
	for name, value := range rewrite.Set {
		if !isValidRewriteHeaderName(name) {
			return fmt.Errorf("header_rewrite.set contains invalid header name %q", name)
		}
		identity := rewriteHeaderIdentity(name)
		if previous, ok := seen[identity]; ok {
			return fmt.Errorf("header_rewrite.set contains duplicate header names %q and %q", previous, name)
		}
		seen[identity] = name
		if !isValidHeaderFieldValue(value) {
			return fmt.Errorf("header_rewrite.set contains invalid value for header %q", name)
		}
	}
	return nil
}

func rewriteHeaderIdentity(name string) string {
	if strings.EqualFold(name, "host") || strings.EqualFold(name, ":authority") {
		return "host"
	}
	return strings.ToLower(name)
}

func isValidRewriteHeaderName(name string) bool {
	if strings.EqualFold(name, ":authority") {
		return true
	}
	if name == "" || strings.HasPrefix(name, ":") {
		return false
	}
	for i := 0; i < len(name); i++ {
		if !isHeaderTokenByte(name[i]) {
			return false
		}
	}
	return true
}

func isHeaderTokenByte(b byte) bool {
	if b >= '0' && b <= '9' || b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' {
		return true
	}
	switch b {
	case '!', '#', '$', '%', '&', '\'', '*', '+', '-', '.', '^', '_', '`', '|', '~':
		return true
	default:
		return false
	}
}

func isValidHeaderFieldValue(value string) bool {
	for i := 0; i < len(value); i++ {
		b := value[i]
		if b == '\x7f' || b < ' ' && b != '\t' {
			return false
		}
	}
	return true
}

func isReservedMetricLabel(name string) bool {
	if strings.HasPrefix(name, "__") {
		return true
	}
	switch name {
	case "label", "status", "le":
		return true
	default:
		return false
	}
}

func isValidMetricLabelName(name string) bool {
	if name == "" {
		return false
	}
	for i, r := range name {
		if r == '_' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' {
			continue
		}
		if i > 0 && r >= '0' && r <= '9' {
			continue
		}
		return false
	}
	return true
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
