package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

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
