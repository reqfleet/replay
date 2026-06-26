package metrics

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/reqfleet/replay/internal/config"
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

func TestRuntimeMetricsCollectorSnapshot(t *testing.T) {
	errCgroupUnavailable := errors.New("cgroup unavailable")
	tests := []struct {
		name                string
		containerCPU        []uint64
		containerCPUErr     []error
		containerMemory     uint64
		containerMemoryErr  error
		hostCPU             int
		hostMemory          uint64
		wantCPU             []float64
		wantSetCPU          []bool
		wantMemory          float64
		wantHostCPUCalls    int
		wantHostMemoryCalls int
	}{
		{
			name:            "container_stats_available",
			containerCPU:    []uint64{1000, 11_000},
			containerMemory: 34,
			hostCPU:         8,
			hostMemory:      1024,
			wantCPU:         []float64{0, 10},
			wantSetCPU:      []bool{false, true},
			wantMemory:      34,
		},
		{
			name:            "initial_zero_cpu_usage",
			containerCPU:    []uint64{0, 10_000},
			containerMemory: 34,
			hostCPU:         8,
			hostMemory:      1024,
			wantCPU:         []float64{0, 10},
			wantSetCPU:      []bool{false, true},
			wantMemory:      34,
		},
		{
			name:                "cgroup_unavailable",
			containerCPUErr:     []error{errCgroupUnavailable},
			containerMemoryErr:  errCgroupUnavailable,
			hostCPU:             8,
			hostMemory:          1024,
			wantCPU:             []float64{8},
			wantSetCPU:          []bool{true},
			wantMemory:          1024,
			wantHostCPUCalls:    1,
			wantHostMemoryCalls: 1,
		},
		{
			name:             "cpu_cgroup_unavailable",
			containerCPUErr:  []error{errCgroupUnavailable},
			containerMemory:  2048,
			hostCPU:          6,
			hostMemory:       1024,
			wantCPU:          []float64{6},
			wantSetCPU:       []bool{true},
			wantMemory:       2048,
			wantHostCPUCalls: 1,
		},
		{
			name:                "memory_cgroup_unavailable",
			containerCPU:        []uint64{99},
			containerMemoryErr:  errCgroupUnavailable,
			hostCPU:             6,
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
				readHostCPU: func() int {
					hostCPUCalls++
					return tt.hostCPU
				},
				interval: time.Second,
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
	got := calculateCPUUsage(51_000, 1_000, 5*time.Second)
	if got != 10 {
		t.Errorf("calculateCPUUsage(51000, 1000, 5s) = %d, want 10", got)
	}
}
