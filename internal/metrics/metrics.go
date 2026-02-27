package metrics

import (
	"net/http"

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
			Help:      "CPU used by engine",
		}, []string{"collection_id", "plan_id", "run_id", "zone", "engine_no"}),
		Mem: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "shibuya",
			Name:      "mem_gauge",
			Help:      "Memory used by engine",
		}, []string{"collection_id", "plan_id", "run_id", "zone", "engine_no"}),
		r: r,
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

func (r *Registry) Handler() http.Handler {
	return promhttp.HandlerFor(r.r, promhttp.HandlerOpts{})
}

func (r *Registry) SeedEngineLabels(labels config.CommonMetricLabelSet) {
	r.ThreadsGauge.WithLabelValues(labels.CollectionID, labels.PlanID, labels.RunID, labels.EngineNo, labels.Zone).Set(0)
	r.CPU.WithLabelValues(labels.CollectionID, labels.PlanID, labels.RunID, labels.Zone, labels.EngineNo).Set(0)
	r.Mem.WithLabelValues(labels.CollectionID, labels.PlanID, labels.RunID, labels.Zone, labels.EngineNo).Set(0)
}

func toMillisecondsBuckets(b []float64) []float64 {
	out := make([]float64, len(b))
	for i, v := range b {
		out[i] = v * 1000
	}
	return out
}
