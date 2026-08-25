package metrics

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/reqfleet/replay/config"
)

func TestGetSafeLabel(t *testing.T) {
	r := New(config.Default().Metrics)

	if l := r.GetSafeLabel("label1", 2); l != "label1" {
		t.Errorf("Expected to get label1, got %s", l)
	}
	if l := r.GetSafeLabel("label2", 2); l != "label2" {
		t.Errorf("Expected to get label2, got %s", l)
	}
	if l := r.GetSafeLabel("label3", 2); l != "_other_" {
		t.Errorf("Expected to get _other_, got %s", l)
	}

	// Should still be able to get already seen labels
	if l := r.GetSafeLabel("label1", 2); l != "label1" {
		t.Errorf("Expected to get label1 again, got %s", l)
	}

	// Should emit all if max <= 0
	if l := r.GetSafeLabel("label4", 0); l != "label4" {
		t.Errorf("Expected to get label4 with max=0, got %s", l)
	}
}

func TestRegistryUsesConfiguredNamespaceAndCommonLabels(t *testing.T) {
	cfg := config.Default().Metrics
	cfg.Namespace = "custom"
	cfg.CommonLabels = []config.MetricLabel{
		{Name: "tenant_id", Value: "tenant-1"},
		{Name: "worker_id", Value: "worker-2"},
	}
	r := New(cfg)

	commonLabelValues := cfg.CommonLabelValues()
	r.SeedEngineLabels(commonLabelValues)
	r.RecordRequest(commonLabelValues, "/users", 12, "200", 34)

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	r.Handler().ServeHTTP(recorder, request)

	body := recorder.Body.String()
	for _, want := range []string{
		"custom_latency_label_milliseconds",
		`tenant_id="tenant-1"`,
		`worker_id="worker-2"`,
		`label="/users"`,
		`status="200"`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics body missing %q:\n%s", want, body)
		}
	}
}

func TestStartRuntimeCollectionStopIsIdempotent(t *testing.T) {
	cfg := config.Default().Metrics
	r := New(cfg)
	stop := r.StartRuntimeCollection(cfg.CommonLabelValues(), time.Hour)

	stop()
	stop()
}

func TestStartRuntimeCollectionRecordsInitialSnapshot(t *testing.T) {
	cfg := config.Default().Metrics
	r := New(cfg)
	commonLabelValues := cfg.CommonLabelValues()
	cpuReads := 0
	collector := &runtimeMetricsCollector{
		readContainerCPU: func() (uint64, error) {
			defer func() { cpuReads++ }()
			if cpuReads == 0 {
				return 1_000, nil
			}
			return 36_000, nil
		},
		readContainerMemory: func() (uint64, error) {
			return 2048, nil
		},
		readHostMemory: func() uint64 {
			return 1024
		},
		readHostCPU: func() (float64, bool) {
			return 0, true
		},
		cpuUsageUnit: time.Microsecond,
	}

	stop := r.startRuntimeCollection(commonLabelValues, 10*time.Millisecond, collector)
	defer stop()

	if got, want := testutil.ToFloat64(r.Mem.WithLabelValues(commonLabelValues...)), float64(2048); got != want {
		t.Fatalf("Mem after StartRuntimeCollection() = %v, want %v", got, want)
	}

	deadline := time.After(time.Second)
	for {
		if got := testutil.ToFloat64(r.CPU.WithLabelValues(commonLabelValues...)); got == 3.5 {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("CPU after first StartRuntimeCollection() tick = %v, want 3.5", testutil.ToFloat64(r.CPU.WithLabelValues(commonLabelValues...)))
		case <-time.After(time.Millisecond):
		}
	}
}

func TestRuntimeMetricsCollectorSnapshot(t *testing.T) {
	errCgroupUnavailable := errors.New("cgroup unavailable")
	tests := []struct {
		name                string
		containerCPU        []uint64
		containerCPUErr     []error
		containerMemory     uint64
		containerMemoryErr  error
		hostCPU             []float64
		hostCPUOK           []bool
		hostMemory          uint64
		cpuUsageUnit        time.Duration
		wantCPU             []float64
		wantSetCPU          []bool
		wantMemory          float64
		wantHostCPUCalls    int
		wantHostMemoryCalls int
	}{
		{
			name:            "container_stats_available",
			containerCPU:    []uint64{1000, 3_501_000},
			containerMemory: 34,
			cpuUsageUnit:    time.Microsecond,
			hostCPU:         []float64{8},
			hostMemory:      1024,
			wantCPU:         []float64{0, 3.5},
			wantSetCPU:      []bool{false, true},
			wantMemory:      34,
		},
		{
			name:            "initial_zero_cpu_usage",
			containerCPU:    []uint64{0, 3_500_000},
			containerMemory: 34,
			cpuUsageUnit:    time.Microsecond,
			hostCPU:         []float64{8},
			hostMemory:      1024,
			wantCPU:         []float64{0, 3.5},
			wantSetCPU:      []bool{false, true},
			wantMemory:      34,
		},
		{
			name:                "cgroup_unavailable",
			containerCPUErr:     []error{errCgroupUnavailable, errCgroupUnavailable},
			containerMemoryErr:  errCgroupUnavailable,
			hostCPU:             []float64{0, 3.5},
			hostMemory:          1024,
			wantCPU:             []float64{0, 3.5},
			wantSetCPU:          []bool{false, true},
			wantMemory:          1024,
			wantHostCPUCalls:    2,
			wantHostMemoryCalls: 2,
		},
		{
			name:             "cpu_cgroup_unavailable",
			containerCPUErr:  []error{errCgroupUnavailable, errCgroupUnavailable},
			containerMemory:  2048,
			hostCPU:          []float64{1, 4.5},
			hostMemory:       1024,
			wantCPU:          []float64{0, 3.5},
			wantSetCPU:       []bool{false, true},
			wantMemory:       2048,
			wantHostCPUCalls: 2,
		},
		{
			name:             "container_cpu_recovers_after_error",
			containerCPU:     []uint64{1_000, 3_501_000, 0, 10_501_000},
			containerCPUErr:  []error{nil, nil, errCgroupUnavailable, nil},
			containerMemory:  2048,
			cpuUsageUnit:     time.Microsecond,
			hostCPU:          []float64{0},
			hostMemory:       1024,
			wantCPU:          []float64{0, 3.5, 0, 0},
			wantSetCPU:       []bool{false, true, false, false},
			wantMemory:       2048,
			wantHostCPUCalls: 1,
		},
		{
			name:             "host_cpu_recovers_after_error",
			containerCPUErr:  []error{errCgroupUnavailable, errCgroupUnavailable, errCgroupUnavailable, errCgroupUnavailable},
			containerMemory:  2048,
			hostCPU:          []float64{0, 3.5, 0, 10.5},
			hostCPUOK:        []bool{true, true, false, true},
			hostMemory:       1024,
			wantCPU:          []float64{0, 3.5, 0, 0},
			wantSetCPU:       []bool{false, true, false, false},
			wantMemory:       2048,
			wantHostCPUCalls: 4,
		},
		{
			name:             "host_cpu_reinitializes_after_container_success",
			containerCPU:     []uint64{0, 0, 10_000, 0},
			containerCPUErr:  []error{errCgroupUnavailable, errCgroupUnavailable, nil, errCgroupUnavailable},
			containerMemory:  2048,
			cpuUsageUnit:     time.Microsecond,
			hostCPU:          []float64{0, 3.5, 10.5},
			hostMemory:       1024,
			wantCPU:          []float64{0, 3.5, 0, 0},
			wantSetCPU:       []bool{false, true, false, false},
			wantMemory:       2048,
			wantHostCPUCalls: 3,
		},
		{
			name:                "memory_cgroup_unavailable",
			containerCPU:        []uint64{99},
			containerMemoryErr:  errCgroupUnavailable,
			hostCPU:             []float64{6},
			hostMemory:          4096,
			wantCPU:             []float64{0},
			wantSetCPU:          []bool{false},
			wantMemory:          4096,
			wantHostMemoryCalls: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hostCPUCalls := 0
			hostMemoryCalls := 0
			cpuReads := 0

			collector := &runtimeMetricsCollector{
				readContainerCPU: func() (uint64, error) {
					defer func() { cpuReads++ }()
					var value uint64
					if cpuReads < len(tt.containerCPU) {
						value = tt.containerCPU[cpuReads]
					}
					if cpuReads < len(tt.containerCPUErr) {
						return value, tt.containerCPUErr[cpuReads]
					}
					return value, nil
				},
				readContainerMemory: func() (uint64, error) {
					return tt.containerMemory, tt.containerMemoryErr
				},
				readHostMemory: func() uint64 {
					hostMemoryCalls++
					return tt.hostMemory
				},
				readHostCPU: func() (float64, bool) {
					defer func() { hostCPUCalls++ }()
					var value float64
					if hostCPUCalls < len(tt.hostCPU) {
						value = tt.hostCPU[hostCPUCalls]
					}
					if hostCPUCalls < len(tt.hostCPUOK) {
						return value, tt.hostCPUOK[hostCPUCalls]
					}
					return value, true
				},
				interval:     time.Second,
				cpuUsageUnit: tt.cpuUsageUnit,
			}

			for i := range tt.wantCPU {
				got := collector.snapshot()
				if got.cpu != tt.wantCPU[i] {
					t.Errorf("runtimeMetricsCollector.snapshot(%s, tick %d) CPU = %v, want %v", tt.name, i, got.cpu, tt.wantCPU[i])
				}
				if got.setCPU != tt.wantSetCPU[i] {
					t.Errorf("runtimeMetricsCollector.snapshot(%s, tick %d) setCPU = %t, want %t", tt.name, i, got.setCPU, tt.wantSetCPU[i])
				}
				if got.memory != tt.wantMemory {
					t.Errorf("runtimeMetricsCollector.snapshot(%s, tick %d) memory = %v, want %v", tt.name, i, got.memory, tt.wantMemory)
				}
			}
			if hostCPUCalls != tt.wantHostCPUCalls {
				t.Errorf("runtimeMetricsCollector.snapshot(%s) host CPU calls = %d, want %d", tt.name, hostCPUCalls, tt.wantHostCPUCalls)
			}
			if hostMemoryCalls != tt.wantHostMemoryCalls {
				t.Errorf("runtimeMetricsCollector.snapshot(%s) host memory calls = %d, want %d", tt.name, hostMemoryCalls, tt.wantHostMemoryCalls)
			}
		})
	}
}

func TestCalculateCPUUsage(t *testing.T) {
	tests := []struct {
		name             string
		cpuUsage         uint64
		previousCPUUsage uint64
		interval         time.Duration
		usageUnit        time.Duration
		want             float64
	}{
		{
			name:             "cgroup_v2_microseconds",
			cpuUsage:         3_501_000,
			previousCPUUsage: 1_000,
			interval:         time.Second,
			usageUnit:        time.Microsecond,
			want:             3.5,
		},
		{
			name:             "cgroup_v1_nanoseconds",
			cpuUsage:         3_501_000_000,
			previousCPUUsage: 1_000_000,
			interval:         time.Second,
			usageUnit:        time.Nanosecond,
			want:             3.5,
		},
		{
			name:             "longer_interval",
			cpuUsage:         10_000_000,
			previousCPUUsage: 0,
			interval:         5 * time.Second,
			usageUnit:        time.Microsecond,
			want:             2,
		},
		{
			name:             "counter_underflow",
			cpuUsage:         1_000,
			previousCPUUsage: 3_501_000,
			interval:         time.Second,
			usageUnit:        time.Microsecond,
			want:             0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := calculateCPUUsage(tt.cpuUsage, tt.previousCPUUsage, tt.interval, tt.usageUnit)
			if got != tt.want {
				t.Errorf("calculateCPUUsage(%d, %d, %s, %s) = %v, want %v", tt.cpuUsage, tt.previousCPUUsage, tt.interval, tt.usageUnit, got, tt.want)
			}
		})
	}
}

func TestCalculateCPUUsageSeconds(t *testing.T) {
	got := calculateCPUUsageSeconds(4.5, 1, time.Second)
	if got != 3.5 {
		t.Errorf("calculateCPUUsageSeconds(4.5, 1, 1s) = %v, want 3.5", got)
	}
}
