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

func New() *Registry {
	r := prometheus.NewRegistry()
	buckets := prometheus.DefBuckets

	out := &Registry{
		LabelLatencyHistogram: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "shibuya",
			Name:      "latency_label_milliseconds",
			Buckets:   toMillisecondsBuckets(buckets),
		}, []string{"collection_id", "label", "run_id", "engine_no", "plan_id", "zone"}),
		StatusCounter: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "shibuya",
			Name:      "status_counter",
			Help:      "HTTP status distribution counter",
		}, []string{"collection_id", "plan_id", "run_id", "engine_no", "label", "zone", "status"}),
		EgressCounter: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "shibuya",
			Name:      "egress_bytes_counter",
			Help:      "Total egress bytes used by engine",
		}, []string{"collection_id", "plan_id", "run_id", "engine_no", "label", "zone"}),
		ThreadsGauge: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "shibuya",
			Name:      "threads_gauge",
			Help:      "Current number of threads running",
		}, []string{"collection_id", "plan_id", "run_id", "engine_no", "zone"}),
		CPU: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "shibuya",
			Name:      "cpu_gauge",
			Help:      "Logical CPUs available to the process (not CPU utilization)",
		}, []string{"collection_id", "plan_id", "run_id", "engine_no", "zone"}),
		Mem: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "shibuya",
			Name:      "mem_gauge",
			Help:      "Memory used by engine",
		}, []string{"collection_id", "plan_id", "run_id", "engine_no", "zone"}),
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

func (r *Registry) SeedEngineLabels(labels config.CommonMetricLabelSet) {
	r.ThreadsGauge.WithLabelValues(labels.CollectionID, labels.PlanID, labels.RunID, labels.EngineNo, labels.Zone).Set(0)
	r.CPU.WithLabelValues(labels.CollectionID, labels.PlanID, labels.RunID, labels.EngineNo, labels.Zone).Set(0)
	r.Mem.WithLabelValues(labels.CollectionID, labels.PlanID, labels.RunID, labels.EngineNo, labels.Zone).Set(0)
}

// StartRuntimeCollection starts a background goroutine that updates runtime
// metrics (goroutines, memory, and a CPU proxy) for the provided labels at the
// given interval. It returns a stop function that cancels the collector.
func (r *Registry) StartRuntimeCollection(labels config.CommonMetricLabelSet, interval time.Duration) func() {
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
				r.ThreadsGauge.WithLabelValues(labels.CollectionID, labels.PlanID, labels.RunID, labels.EngineNo, labels.Zone).Set(float64(runtime.NumGoroutine()))
				runtime.ReadMemStats(&m)
				r.Mem.WithLabelValues(labels.CollectionID, labels.PlanID, labels.RunID, labels.EngineNo, labels.Zone).Set(float64(m.Alloc))
				// CPU: report number of CPUs as a proxy to avoid OS-specific code.
				r.CPU.WithLabelValues(labels.CollectionID, labels.PlanID, labels.RunID, labels.EngineNo, labels.Zone).Set(float64(runtime.NumCPU()))
			case <-done:
				ticker.Stop()
				return
			}
		}
	}()
	return func() { close(done) }
}

func toMillisecondsBuckets(b []float64) []float64 {
	out := make([]float64, len(b))
	for i, v := range b {
		out[i] = v * 1000
	}
	return out
}
