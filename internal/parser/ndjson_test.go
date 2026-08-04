package parser

import (
	"strings"
	"testing"

	"github.com/reqfleet/replay/internal/model"
)

func TestParseFlatDownstreamStart(t *testing.T) {
	input := strings.NewReader(
		`{"type":"DownstreamStart","node":"envoy-a","connection_id":7,` +
			`"start_time":"2026-02-27T03:10:22.001Z","method":"GET","scheme":"https",` +
			`"authority":"example.com","path":"/start","protocol":"HTTP/1.1","response_code":503,"user_agent":"curl/8.0.0"}` + "\n")

	events := parseEvents(t, input)
	if got, want := len(events), 1; got != want {
		t.Fatalf("len(events) = %d, want %d", got, want)
	}
	got := events[0]
	if got.Type != model.AccessLogTypeDownstreamStart {
		t.Errorf("event.Type = %q, want %q", got.Type, model.AccessLogTypeDownstreamStart)
	}
	if got.Node != "envoy-a" || got.ConnectionID != 7 {
		t.Errorf("event connection = %q/%d, want envoy-a/7", got.Node, got.ConnectionID)
	}
	if got.Sequence != 1 {
		t.Errorf("event.Sequence = %d, want 1", got.Sequence)
	}
	if got.StreamID != 1 {
		t.Errorf("event.StreamID = %d, want 1", got.StreamID)
	}
	if got.StartTime != "2026-02-27T03:10:22.001Z" {
		t.Errorf("event.StartTime = %q, want input start_time", got.StartTime)
	}
	if got.Protocol != "HTTP/1.1" || got.Method != "GET" ||
		got.Scheme != "https" || got.Authority != "example.com" ||
		got.Path != "/start" {
		t.Errorf("event request = %s %s://%s%s %s, want GET https://example.com/start HTTP/1.1",
			got.Method, got.Scheme, got.Authority, got.Path, got.Protocol)
	}
	if got.ResponseCode == nil || *got.ResponseCode != 503 {
		t.Errorf("event.ResponseCode = %v, want 503", got.ResponseCode)
	}
	if values := got.Headers["user-agent"]; len(values) != 1 || values[0] != "curl/8.0.0" {
		t.Errorf("event.Headers[user-agent] = %v, want [curl/8.0.0]", values)
	}
}

func TestParseFlatDownstreamEnd(t *testing.T) {
	input := strings.NewReader(
		`{"type":"DownstreamEnd","connection_id":7,` +
			`"start_time":"2026-02-27T03:10:22.101Z","method":"POST","scheme":"https",` +
			`"authority":"example.com","path":"/end","protocol":"HTTP/2","response_code":503,` +
			`"duration_ms":16,"user_agent":"flat-user-agent",` +
			`"headers":{"content-type":["application/json"],"User-Agent":["recorded-user-agent"]},` +
			`"body":{"encoding":"base64","content":"e30=","size_bytes":2},` +
			`"response_headers":{"content-type":["application/json"]},` +
			`"response_body":{"encoding":"base64","content":"b2s=","size_bytes":2}}` + "\n")

	events := parseEvents(t, input)
	if got, want := len(events), 1; got != want {
		t.Fatalf("len(events) = %d, want %d", got, want)
	}
	got := events[0]
	if got.Type != model.AccessLogTypeDownstreamEnd {
		t.Errorf("event.Type = %q, want %q", got.Type, model.AccessLogTypeDownstreamEnd)
	}
	if got.ResponseCode == nil || *got.ResponseCode != 503 {
		t.Errorf("event.ResponseCode = %v, want 503", got.ResponseCode)
	}
	if got.DurationMS != 16 {
		t.Errorf("event.DurationMS = %v, want 16", got.DurationMS)
	}
	if got.Protocol != "HTTP/2" || got.Method != "POST" ||
		got.Authority != "example.com" || got.Path != "/end" {
		t.Errorf("event request = %s %s://%s%s %s, want POST https://example.com/end HTTP/2",
			got.Method, got.Scheme, got.Authority, got.Path, got.Protocol)
	}
	if values := got.Headers["User-Agent"]; len(values) != 1 || values[0] != "recorded-user-agent" {
		t.Errorf("event.Headers[User-Agent] = %v, want [recorded-user-agent]", values)
	}
	if values, exists := got.Headers["user-agent"]; exists {
		t.Errorf("event.Headers[user-agent] = %v, want key absent", values)
	}
	if got.Body == nil || got.Body.Content != "e30=" {
		t.Errorf("event.Body = %#v, want request body e30=", got.Body)
	}
	if values := got.ResponseHeaders["content-type"]; len(values) != 1 || values[0] != "application/json" {
		t.Errorf("event.ResponseHeaders[content-type] = %v, want [application/json]", values)
	}
	if got.ResponseBody == nil || got.ResponseBody.Content != "b2s=" {
		t.Errorf("event.ResponseBody = %#v, want response body b2s=", got.ResponseBody)
	}
}

func TestParseFlatWithoutTypeDefaultsToDownstreamEnd(t *testing.T) {
	input := strings.NewReader(
		`{"connection_id":7,"start_time":"2026-02-27T03:10:22.101Z","method":"GET",` +
			`"authority":"example.com","path":"/end","protocol":"HTTP/1.1","response_code":201}` + "\n")

	events := parseEvents(t, input)
	if got, want := len(events), 1; got != want {
		t.Fatalf("len(events) = %d, want %d", got, want)
	}
	got := events[0]
	if got.Type != model.AccessLogTypeDownstreamEnd {
		t.Errorf("event.Type = %q, want %q", got.Type, model.AccessLogTypeDownstreamEnd)
	}
	if got.ResponseCode == nil || *got.ResponseCode != 201 {
		t.Errorf("event.ResponseCode = %v, want 201", got.ResponseCode)
	}
}

func TestParseRejectsUnsupportedInputShapes(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "empty_type",
			input: `{"type":"","connection_id":1,"start_time":"2026-02-27T03:10:22Z","method":"GET","authority":"example.com","path":"/","protocol":"HTTP/1.1","response_code":200}`,
		},
		{
			name:  "noncanonical_type",
			input: `{"type":"downstreamend","connection_id":1,"start_time":"2026-02-27T03:10:22Z","method":"GET","authority":"example.com","path":"/","protocol":"HTTP/1.1","response_code":200}`,
		},
		{
			name:  "legacy_request",
			input: `{"type":"request","connection_id":1,"timestamp":"2026-02-27T03:10:22Z","http":{"method":"GET","authority":"example.com","path":"/"}}`,
		},
		{
			name:  "connection_lifecycle",
			input: `{"type":"connection_open","connection_id":1}`,
		},
		{
			name:  "nested_http",
			input: `{"type":"DownstreamEnd","connection_id":1,"start_time":"2026-02-27T03:10:22Z","method":"GET","authority":"example.com","path":"/","protocol":"HTTP/1.1","response_code":200,"http":{"method":"GET","authority":"example.com","path":"/"}}`,
		},
		{
			name:  "legacy_timestamp",
			input: `{"type":"DownstreamStart","connection_id":1,"timestamp":"2026-02-27T03:10:22Z","start_time":"2026-02-27T03:10:22Z","method":"GET","authority":"example.com","path":"/","protocol":"HTTP/1.1"}`,
		},
		{
			name:  "missing_connection_id",
			input: `{"type":"DownstreamStart","start_time":"2026-02-27T03:10:22Z","method":"GET","authority":"example.com","path":"/","protocol":"HTTP/1.1"}`,
		},
		{
			name:  "missing_start_time",
			input: `{"type":"DownstreamStart","connection_id":1,"method":"GET","authority":"example.com","path":"/","protocol":"HTTP/1.1"}`,
		},
		{
			name:  "invalid_start_time",
			input: `{"type":"DownstreamStart","connection_id":1,"start_time":"not-a-time","method":"GET","authority":"example.com","path":"/","protocol":"HTTP/1.1"}`,
		},
		{
			name:  "missing_method",
			input: `{"type":"DownstreamStart","connection_id":1,"start_time":"2026-02-27T03:10:22Z","authority":"example.com","path":"/","protocol":"HTTP/1.1"}`,
		},
		{
			name:  "missing_protocol",
			input: `{"type":"DownstreamStart","connection_id":1,"start_time":"2026-02-27T03:10:22Z","method":"GET","authority":"example.com","path":"/"}`,
		},
		{
			name:  "missing_authority",
			input: `{"type":"DownstreamStart","connection_id":1,"start_time":"2026-02-27T03:10:22Z","method":"GET","path":"/","protocol":"HTTP/1.1"}`,
		},
		{
			name:  "missing_path",
			input: `{"type":"DownstreamStart","connection_id":1,"start_time":"2026-02-27T03:10:22Z","method":"GET","authority":"example.com","protocol":"HTTP/1.1"}`,
		},
		{
			name:  "completion_missing_response_code",
			input: `{"type":"DownstreamEnd","connection_id":1,"start_time":"2026-02-27T03:10:22Z","method":"GET","authority":"example.com","path":"/","protocol":"HTTP/1.1"}`,
		},
		{
			name:  "untyped_completion_missing_response_code",
			input: `{"connection_id":1,"start_time":"2026-02-27T03:10:22Z","method":"GET","authority":"example.com","path":"/","protocol":"HTTP/1.1"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ParseStream(strings.NewReader(tt.input+"\n"), func(model.Event) error { return nil })
			if err == nil {
				t.Fatalf("ParseStream(%s) error = nil, want error", tt.input)
			}
		})
	}
}

func TestParseSkipsNonJSONLines(t *testing.T) {
	input := strings.NewReader(
		"[2026-04-23 07:12:34.566][1][info][main] shutting down parent after drain\n" +
			`{"type":"DownstreamStart","connection_id":1,"start_time":"2026-02-27T03:10:22Z","method":"GET","authority":"example.com","path":"/","protocol":"HTTP/1.1"}` + "\n")

	events := parseEvents(t, input)
	if got, want := len(events), 1; got != want {
		t.Fatalf("len(events) = %d, want %d", got, want)
	}
}

func TestParseRejectsHTTP11StreamIDNotOne(t *testing.T) {
	input := strings.NewReader(
		`{"type":"DownstreamStart","connection_id":1,"stream_id":2,` +
			`"start_time":"2026-02-27T03:10:22Z","method":"GET","authority":"example.com",` +
			`"path":"/","protocol":"HTTP/1.1"}` + "\n")

	err := ParseStream(input, func(model.Event) error { return nil })
	if err == nil {
		t.Fatal("ParseStream(HTTP/1.1 stream_id=2) error = nil, want error")
	}
}

func TestParseRejectsNonMonotonicSequence(t *testing.T) {
	input := strings.NewReader(
		`{"type":"DownstreamStart","connection_id":1,"sequence":2,` +
			`"start_time":"2026-02-27T03:10:22Z","method":"GET","authority":"example.com",` +
			`"path":"/a","protocol":"HTTP/1.1"}` + "\n" +
			`{"type":"DownstreamStart","connection_id":1,"sequence":1,` +
			`"start_time":"2026-02-27T03:10:23Z","method":"GET","authority":"example.com",` +
			`"path":"/b","protocol":"HTTP/1.1"}` + "\n")

	err := ParseStream(input, func(model.Event) error { return nil })
	if err == nil {
		t.Fatal("ParseStream(non-monotonic sequence) error = nil, want error")
	}
}

func TestParseIsolatesSequenceByNode(t *testing.T) {
	input := strings.NewReader(
		`{"type":"DownstreamStart","node":"envoy-a","connection_id":1,` +
			`"start_time":"2026-02-27T03:10:22Z","method":"GET","authority":"example.com",` +
			`"path":"/a","protocol":"HTTP/1.1"}` + "\n" +
			`{"type":"DownstreamStart","node":"envoy-b","connection_id":1,` +
			`"start_time":"2026-02-27T03:10:23Z","method":"GET","authority":"example.com",` +
			`"path":"/b","protocol":"HTTP/1.1"}` + "\n")

	events := parseEvents(t, input)
	if got, want := len(events), 2; got != want {
		t.Fatalf("len(events) = %d, want %d", got, want)
	}
	if events[0].Sequence != 1 || events[1].Sequence != 1 {
		t.Errorf("event sequences = [%d %d], want [1 1]", events[0].Sequence, events[1].Sequence)
	}
}

func parseEvents(t *testing.T, input *strings.Reader) []model.Event {
	t.Helper()
	var events []model.Event
	if err := ParseStream(input, func(event model.Event) error {
		events = append(events, event)
		return nil
	}); err != nil {
		t.Fatalf("ParseStream() error: %v", err)
	}
	return events
}
