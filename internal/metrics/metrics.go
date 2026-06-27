package metrics

import (
	"net/http"
	"os"
	"runtime"
	runtimemetrics "runtime/metrics"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/reqfleet/containerstats"
	"github.com/reqfleet/replay/internal/config"
)

const (
	cgroupV2ControllersPath = "/sys/fs/cgroup/cgroup.controllers"
	hostCPUTotalMetric      = "/cpu/classes/total:cpu-seconds"
	hostCPUIdleMetric       = "/cpu/classes/idle:cpu-seconds"
)

type Registry struct {
	LabelLatencyHistogram *prometheus.HistogramVec
	StatusCounter         *prometheus.CounterVec
	EgressCounter         *prometheus.CounterVec
	ThreadsGauge          *prometheus.GaugeVec
	CPU                   *prometheus.GaugeVec
	Mem                   *prometheus.GaugeVec

	r *prometheus.Registry

	seenLabels map[string]struct{}
	labelsMu   sync.RWMutex
}

func New(cfg config.MetricsConfig) *Registry {
	r := prometheus.NewRegistry()
	buckets := prometheus.DefBuckets
	commonLabels := cfg.CommonLabelNames()

	out := &Registry{
		LabelLatencyHistogram: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: cfg.Namespace,
			Name:      "latency_label_milliseconds",
			Buckets:   toMillisecondsBuckets(buckets),
		}, appendLabels(commonLabels, "label")),
		StatusCounter: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: cfg.Namespace,
			Name:      "status_counter",
			Help:      "HTTP status distribution counter",
		}, appendLabels(commonLabels, "label", "status")),
		EgressCounter: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: cfg.Namespace,
			Name:      "egress_bytes_counter",
			Help:      "Total egress bytes used by engine",
		}, appendLabels(commonLabels, "label")),
		ThreadsGauge: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: cfg.Namespace,
			Name:      "threads_gauge",
			Help:      "Number of replay clients created for the engine",
		}, commonLabels),
		CPU: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: cfg.Namespace,
			Name:      "cpu_gauge",
			Help:      "CPU cores used by the container when running in a cgroup; process CPU cores used otherwise",
		}, commonLabels),
		Mem: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: cfg.Namespace,
			Name:      "mem_gauge",
			Help:      "Container memory usage when running in a cgroup; Go heap allocation otherwise",
		}, commonLabels),
		r:          r,
		seenLabels: make(map[string]struct{}),
	}

	r.MustRegister(
		out.LabelLatencyHistogram,
		out.StatusCounter,
		out.EgressCounter,
		out.ThreadsGauge,
		out.CPU,
		out.Mem,
	)
	return out
}

// GetSafeLabel returns the label if it can be tracked based on the configured MaxLabels limit.
// If the limit is reached and it's a new label, it returns a fallback label ("_other_") to ensure aggregate metrics remain accurate.
// If max <= 0, no limit is enforced.
func (r *Registry) GetSafeLabel(label string, max int) string {
	if max <= 0 {
		return label
	}

	r.labelsMu.RLock()
	_, exists := r.seenLabels[label]
	full := len(r.seenLabels) >= max
	r.labelsMu.RUnlock()

	if exists {
		return label
	}
	if full {
		return "_other_"
	}

	r.labelsMu.Lock()
	defer r.labelsMu.Unlock()
	// Check again in case another goroutine inserted it
	if _, exists := r.seenLabels[label]; exists {
		return label
	}
	if len(r.seenLabels) >= max {
		return "_other_"
	}
	r.seenLabels[label] = struct{}{}
	return label
}

func (r *Registry) Handler() http.Handler {
	return promhttp.HandlerFor(r.r, promhttp.HandlerOpts{})
}

func (r *Registry) SeedEngineLabels(commonLabelValues []string) {
	r.ThreadsGauge.WithLabelValues(commonLabelValues...).Set(0)
	r.CPU.WithLabelValues(commonLabelValues...).Set(0)
	r.Mem.WithLabelValues(commonLabelValues...).Set(0)
}

func (r *Registry) RecordClientCreated(commonLabelValues []string) {
	r.ThreadsGauge.WithLabelValues(commonLabelValues...).Inc()
}

func (r *Registry) RecordRequest(commonLabelValues []string, label string, latencyMS float64, status string, egressBytes int64) {
	r.LabelLatencyHistogram.WithLabelValues(appendLabelValues(commonLabelValues, label)...).Observe(latencyMS)
	r.StatusCounter.WithLabelValues(appendLabelValues(commonLabelValues, label, status)...).Inc()
	r.EgressCounter.WithLabelValues(appendLabelValues(commonLabelValues, label)...).Add(float64(egressBytes))
}

func (r *Registry) RecordStatus(commonLabelValues []string, label string, status string) {
	r.StatusCounter.WithLabelValues(appendLabelValues(commonLabelValues, label, status)...).Inc()
}

// StartRuntimeCollection starts a background goroutine that updates runtime
// metrics for the provided labels at the given interval. It returns a stop
// function that cancels the collector.
func (r *Registry) StartRuntimeCollection(commonLabelValues []string, interval time.Duration) func() {
	if interval <= 0 {
		interval = time.Second * 5
	}
	collector := newRuntimeMetricsCollector(interval)
	ticker := time.NewTicker(interval)
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-ticker.C:
				metrics := collector.snapshot()
				if metrics.setCPU {
					r.CPU.WithLabelValues(commonLabelValues...).Set(metrics.cpu)
				}
				r.Mem.WithLabelValues(commonLabelValues...).Set(metrics.memory)
			case <-done:
				ticker.Stop()
				return
			}
		}
	}()
	return func() { close(done) }
}

type runtimeStatReader func() (uint64, error)

type runtimeMetricsCollector struct {
	readContainerCPU     runtimeStatReader
	readContainerMemory  runtimeStatReader
	readHostMemory       func() uint64
	readHostCPU          func() (float64, bool)
	interval             time.Duration
	cpuUsageUnit         time.Duration
	previousCPUUsage     uint64
	cpuInitialized       bool
	previousHostCPUUsage float64
	hostCPUInitialized   bool
}

type runtimeMetrics struct {
	cpu    float64
	memory float64
	setCPU bool
}

func newRuntimeMetricsCollector(interval time.Duration) *runtimeMetricsCollector {
	return &runtimeMetricsCollector{
		readContainerCPU:    containerstats.ReadCPUUsage,
		readContainerMemory: containerstats.ReadMemoryUsage,
		readHostMemory:      readHostMemoryAlloc,
		readHostCPU:         readHostCPUSeconds,
		interval:            interval,
		cpuUsageUnit:        detectContainerCPUUsageUnit(),
	}
}

func (c *runtimeMetricsCollector) snapshot() runtimeMetrics {
	cpu, setCPU := c.cpuUsage()
	return runtimeMetrics{
		cpu:    cpu,
		memory: float64(c.memoryUsage()),
		setCPU: setCPU,
	}
}

func (c *runtimeMetricsCollector) cpuUsage() (float64, bool) {
	cpuUsage, err := c.readContainerCPU()
	if err != nil {
		c.cpuInitialized = false
		return c.hostCPUUsage()
	}
	if !c.cpuInitialized || cpuUsage < c.previousCPUUsage {
		c.previousCPUUsage = cpuUsage
		c.cpuInitialized = true
		return 0, false
	}

	used := calculateCPUUsage(cpuUsage, c.previousCPUUsage, c.interval, c.cpuUsageUnit)
	c.previousCPUUsage = cpuUsage
	return used, true
}

func (c *runtimeMetricsCollector) hostCPUUsage() (float64, bool) {
	cpuUsage, ok := c.readHostCPU()
	if !ok {
		c.hostCPUInitialized = false
		return 0, false
	}
	if !c.hostCPUInitialized || cpuUsage < c.previousHostCPUUsage {
		c.previousHostCPUUsage = cpuUsage
		c.hostCPUInitialized = true
		return 0, false
	}

	used := calculateCPUUsageSeconds(cpuUsage, c.previousHostCPUUsage, c.interval)
	c.previousHostCPUUsage = cpuUsage
	return used, true
}

func (c *runtimeMetricsCollector) memoryUsage() uint64 {
	memory, err := c.readContainerMemory()
	if err != nil {
		return c.readHostMemory()
	}
	return memory
}

func calculateCPUUsage(cpuUsage, previousCPUUsage uint64, interval, usageUnit time.Duration) float64 {
	if interval <= 0 {
		interval = time.Second
	}
	if usageUnit <= 0 {
		usageUnit = time.Microsecond
	}
	return calculateCPUUsageDuration(time.Duration(cpuUsage-previousCPUUsage)*usageUnit, 0, interval)
}

func calculateCPUUsageDuration(cpuUsage, previousCPUUsage, interval time.Duration) float64 {
	if interval <= 0 {
		interval = time.Second
	}
	return float64(cpuUsage-previousCPUUsage) / float64(interval)
}

func calculateCPUUsageSeconds(cpuUsage, previousCPUUsage float64, interval time.Duration) float64 {
	if interval <= 0 {
		interval = time.Second
	}
	return (cpuUsage - previousCPUUsage) / interval.Seconds()
}

func detectContainerCPUUsageUnit() time.Duration {
	if _, err := os.Stat(cgroupV2ControllersPath); err == nil {
		return time.Microsecond
	}
	return time.Nanosecond
}

func readHostMemoryAlloc() uint64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.Alloc
}

func readHostCPUSeconds() (float64, bool) {
	samples := []runtimemetrics.Sample{
		{Name: hostCPUTotalMetric},
		{Name: hostCPUIdleMetric},
	}
	runtimemetrics.Read(samples)

	total, ok := runtimeMetricFloat64(samples[0])
	if !ok {
		return 0, false
	}
	idle, ok := runtimeMetricFloat64(samples[1])
	if !ok || idle > total {
		return 0, false
	}
	return total - idle, true
}

func runtimeMetricFloat64(sample runtimemetrics.Sample) (float64, bool) {
	if sample.Value.Kind() != runtimemetrics.KindFloat64 {
		return 0, false
	}
	return sample.Value.Float64(), true
}

func appendLabels(commonLabels []string, names ...string) []string {
	out := make([]string, 0, len(commonLabels)+len(names))
	out = append(out, commonLabels...)
	out = append(out, names...)
	return out
}

func appendLabelValues(commonLabelValues []string, values ...string) []string {
	out := make([]string, 0, len(commonLabelValues)+len(values))
	out = append(out, commonLabelValues...)
	out = append(out, values...)
	return out
}

func toMillisecondsBuckets(b []float64) []float64 {
	out := make([]float64, len(b))
	for i, v := range b {
		out[i] = v * 1000
	}
	return out
}
