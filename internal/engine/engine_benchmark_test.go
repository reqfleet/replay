package engine

import (
	"net/http"
	"strings"
	"testing"

	"github.com/reqfleet/replay/config"
)

var benchmarkHeadersSink http.Header

func BenchmarkEffectiveRequestHeaders(b *testing.B) {
	recorded := map[string][]string{
		"x-api-key":          {"key"},
		"x-forwarded-proto":  {"http"},
		"x-forwarded-scheme": {"http"},
		"x-real-ip":          {"172.18.0.1"},
		"x-forwarded-host":   {"localhost:6000"},
		"x-forwarded-port":   {"6000"},
		"x-trace":            {"one", "two"},
	}
	engine := New(config.Default(), nil)

	b.Run("single_pass", func(b *testing.B) {
		b.ReportAllocs()
		var headers http.Header
		for b.Loop() {
			headers = engine.effectiveRequestHeaders(recorded)
		}
		benchmarkHeadersSink = headers
	})
	b.Run("header_add", func(b *testing.B) {
		b.ReportAllocs()
		var headers http.Header
		for b.Loop() {
			headers = benchmarkEffectiveRequestHeadersAdd(recorded)
		}
		benchmarkHeadersSink = headers
	})
}

func benchmarkEffectiveRequestHeadersAdd(recorded map[string][]string) http.Header {
	headers := make(http.Header, len(recorded))
	for key, values := range recorded {
		if strings.HasPrefix(key, ":") {
			continue
		}
		for _, value := range values {
			headers.Add(key, value)
		}
	}
	return headers
}
