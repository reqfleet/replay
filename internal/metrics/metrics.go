package metrics

import (
	"net/http"
	"runtime"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/reqfleet/replay/internal/config"
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
			Help:      "Logical CPUs available to the process (not CPU utilization)",
		}, commonLabels),
		Mem: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: cfg.Namespace,
			Name:      "mem_gauge",
			Help:      "Memory used by engine",
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
// metrics (memory and a CPU proxy) for the provided labels at the given
// interval. It returns a stop function that cancels the collector.
func (r *Registry) StartRuntimeCollection(commonLabelValues []string, interval time.Duration) func() {
	if interval <= 0 {
		interval = time.Second * 5
	}
	ticker := time.NewTicker(interval)
	done := make(chan struct{})
	go func() {
		var m runtime.MemStats
		for {
			select {
			case <-ticker.C:
				runtime.ReadMemStats(&m)
				r.Mem.WithLabelValues(commonLabelValues...).Set(float64(m.Alloc))
				// CPU: report number of CPUs as a proxy to avoid OS-specific code.
				r.CPU.WithLabelValues(commonLabelValues...).Set(float64(runtime.NumCPU()))
			case <-done:
				ticker.Stop()
				return
			}
		}
	}()
	return func() { close(done) }
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
