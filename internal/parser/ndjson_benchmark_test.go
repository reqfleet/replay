package parser

import (
	"encoding/json"
	"testing"
)

const benchmarkCanonicalRequest = `{"type":"request","request_id":"request-a","node":"envoy-a","connection_id":7,"timestamp":"2026-02-27T03:10:22.001Z","method":"GET","scheme":"http","authority":"localhost:6000","path":"/profile","protocol":"HTTP/1.1","response_code":200,"duration_ms":16,"headers":{"x-api-key":["key"],"x-forwarded-proto":["http"],"x-forwarded-scheme":["http"],"x-real-ip":["172.18.0.1"],"x-forwarded-host":["localhost:6000"],"x-forwarded-port":["6000"]}}`

const benchmarkDownstreamEnd = `{"type":"DownstreamEnd","request_id":"request-a","node":"envoy-a","connection_id":7,"stream_id":3,"timestamp":"2026-02-27T03:10:22.001Z","method":"POST","scheme":"https","authority":"example.com","path":"/items","protocol":"HTTP/2","response_code":503,"duration_ms":16,"user_agent":"profile-agent","headers":{"content-type":["application/json"]},"body":{"encoding":"base64","content":"e30=","size_bytes":2},"response_headers":{"x-result":["one","two"]},"response_body":{"encoding":"base64","content":"b2s=","size_bytes":2},"response_flags":"DC,UR"}`

var benchmarkWireSink canonicalWireEvent

func BenchmarkCanonicalWireDecoder(b *testing.B) {
	for _, input := range []struct {
		name string
		data []byte
	}{
		{name: "canonical", data: []byte(benchmarkCanonicalRequest)},
		{name: "downstream_end", data: []byte(benchmarkDownstreamEnd)},
	} {
		b.Run(input.name, func(b *testing.B) {
			b.ReportAllocs()
			var raw canonicalWireEvent
			for b.Loop() {
				raw = canonicalWireEvent{}
				if err := json.Unmarshal(input.data, &raw); err != nil {
					b.Fatalf("json.Unmarshal() error: %v", err)
				}
			}
			benchmarkWireSink = raw
		})
	}
}
