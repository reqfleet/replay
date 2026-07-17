package parser

import (
	"strings"
	"testing"

	"github.com/reqfleet/replay/internal/model"
)

func TestParseSuccess(t *testing.T) {
	input := strings.NewReader("{" +
		`"type":"meta","format_version":"1.0"` +
		"}\n" +
		"{" +
		`"type":"request","connection_id":1,"http":{"method":"GET","scheme":"http","authority":"example.com","path":"/"}` +
		"}\n")

	events := make([]model.Event, 0)
	if err := ParseStream(input, func(e model.Event) error {
		events = append(events, e)
		return nil
	}); err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}
	if got, want := events[1].Sequence, 1; got != want {
		t.Errorf("ParseStream(request without sequence) = sequence %d, want %d", got, want)
	}
}

func TestParseResponseExpectationModes(t *testing.T) {
	modes := []model.ResponseExpectationMode{
		model.ResponseExpectationsNone,
		model.ResponseExpectationsRequestStatus,
		model.ResponseExpectationsResponseEvents,
	}
	for _, want := range modes {
		t.Run(string(want), func(t *testing.T) {
			input := strings.NewReader(
				`{"type":"meta","format_version":"1.0","response_expectations":"` +
					string(want) +
					`"}` + "\n",
			)
			var events []model.Event
			if err := ParseStream(input, func(event model.Event) error {
				events = append(events, event)
				return nil
			}); err != nil {
				t.Fatalf("ParseStream(response_expectations=%q) error: %v", want, err)
			}
			if got := events[0].ResponseExpectations; got != want {
				t.Errorf("ParseStream(response_expectations=%q) = %q, want %q", want, got, want)
			}
		})
	}
}

func TestParseRejectsInvalidResponseExpectations(t *testing.T) {
	input := strings.NewReader(
		`{"type":"meta","format_version":"1.0","response_expectations":"sideways"}` + "\n",
	)
	err := ParseStream(input, func(model.Event) error { return nil })
	if err == nil {
		t.Fatal("ParseStream(response_expectations=\"sideways\") error = nil, want error")
	}
	if !strings.Contains(err.Error(), `invalid response_expectations: "sideways"`) {
		t.Errorf("ParseStream(response_expectations=\"sideways\") error = %q, want invalid response_expectations", err)
	}
}

func TestParseDownstreamStartAccessLogAsRequest(t *testing.T) {
	input := strings.NewReader("{" +
		`"type":"DownstreamStart","connection_id":1,"timestamp":"2026-02-27T03:10:22.001Z","http":{"method":"GET","authority":"example.com","path":"/start"}` +
		"}\n")

	events := make([]model.Event, 0)
	if err := ParseStream(input, func(e model.Event) error {
		events = append(events, e)
		return nil
	}); err != nil {
		t.Fatalf("ParseStream(DownstreamStart) error: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("len(events) = %d, want 1", len(events))
	}
	if got, want := events[0].Type, model.EventRequest; got != want {
		t.Fatalf("events[0].Type = %q, want %q", got, want)
	}
	if got, want := events[0].AccessLogType, model.AccessLogTypeDownstreamStart; got != want {
		t.Fatalf("events[0].AccessLogType = %q, want %q", got, want)
	}
	if got, want := events[0].Sequence, 1; got != want {
		t.Fatalf("events[0].Sequence = %d, want %d", got, want)
	}
}

func TestParseSkipsMalformedDownstreamStartAccessLog(t *testing.T) {
	input := strings.NewReader("{" +
		`"type":"DownstreamStart","connection_id":1,"timestamp":"2026-02-27T03:10:22.001Z"` +
		"}\n" +
		"{" +
		`"type":"DownstreamStart","connection_id":1,"timestamp":"2026-02-27T03:10:22.002Z","http":{"method":"GET","authority":"example.com","path":"/ok"}` +
		"}\n")

	events := make([]model.Event, 0)
	if err := ParseStream(input, func(e model.Event) error {
		events = append(events, e)
		return nil
	}); err != nil {
		t.Fatalf("ParseStream(malformed DownstreamStart) error: %v", err)
	}
	if got, want := len(events), 1; got != want {
		t.Fatalf("len(events) = %d, want %d", got, want)
	}
	if got, want := events[0].Sequence, 1; got != want {
		t.Fatalf("events[0].Sequence = %d, want %d", got, want)
	}
}

func TestParseFlatEnvoyAccessLogAsInlineValidatedRequest(t *testing.T) {
	input := strings.NewReader("{" +
		`"connection_id":7,` +
		`"start_time":"2026-02-27T03:10:22.101Z",` +
		`"method":"GET",` +
		`"path":"/start",` +
		`"protocol":"HTTP/1.1",` +
		`"authority":"example.com",` +
		`"request_id":"req-1",` +
		`"response_code":503,` +
		`"response_flags":"-",` +
		`"duration_ms":16,` +
		`"bytes_received":0,` +
		`"bytes_sent":5,` +
		`"downstream_remote_address":"10.1.2.15:53210",` +
		`"upstream_host":"10.2.3.4:8080",` +
		`"user_agent":"curl/8.0.0"` +
		"}\n")

	events := make([]model.Event, 0)
	if err := ParseStream(input, func(e model.Event) error {
		events = append(events, e)
		return nil
	}); err != nil {
		t.Fatalf("ParseStream(flat Envoy access log) error: %v", err)
	}
	if got, want := len(events), 1; got != want {
		t.Fatalf("len(events) = %d, want %d", got, want)
	}
	got := events[0]
	if got.Type != model.EventRequest {
		t.Fatalf("events[0].Type = %q, want %q", got.Type, model.EventRequest)
	}
	if got.AccessLogType != model.AccessLogTypeDownstreamEnd {
		t.Fatalf("events[0].AccessLogType = %q, want %q", got.AccessLogType, model.AccessLogTypeDownstreamEnd)
	}
	if got, want := got.ConnectionID, 7; got != want {
		t.Fatalf("events[0].ConnectionID = %d, want %d", got, want)
	}
	if got.Sequence != 1 {
		t.Fatalf("events[0].Sequence = %d, want 1", got.Sequence)
	}
	if got.Status != 503 {
		t.Fatalf("events[0].Status = %d, want 503", got.Status)
	}
	if got.HTTP.Method != "GET" || got.HTTP.Authority != "example.com" || got.HTTP.Path != "/start" {
		t.Fatalf("events[0].HTTP = %+v, want method GET authority example.com path /start", got.HTTP)
	}
	if got.Headers["user-agent"][0] != "curl/8.0.0" {
		t.Fatalf("events[0].Headers[user-agent] = %v, want curl/8.0.0", got.Headers["user-agent"])
	}
}

func TestParseFlatEnvoyAccessLogRequiresConnectionID(t *testing.T) {
	input := strings.NewReader("{" +
		`"start_time":"2026-02-27T03:10:22.101Z",` +
		`"method":"GET",` +
		`"path":"/start",` +
		`"protocol":"HTTP/1.1",` +
		`"authority":"example.com",` +
		`"response_code":503,` +
		`"duration_ms":16` +
		"}\n")

	err := ParseStream(input, func(e model.Event) error { return nil })
	if err == nil {
		t.Fatal("ParseStream(flat Envoy access log without connection_id) error = nil, want error")
	}
}

func TestParseRejectsCanonicalDownstreamEndAccessLogType(t *testing.T) {
	input := strings.NewReader("{" +
		`"type":"DownstreamEnd","connection_id":1,"timestamp":"2026-02-27T03:10:22.101Z","status":503` +
		"}\n")

	err := ParseStream(input, func(e model.Event) error { return nil })
	if err == nil {
		t.Fatal("ParseStream(DownstreamEnd) error = nil, want error")
	}
}

func TestParseRejectsNonCanonicalDownstreamAccessLogType(t *testing.T) {
	input := strings.NewReader("{" +
		`"type":"downstream-start","connection_id":1,"timestamp":"2026-02-27T03:10:22.001Z","http":{"method":"GET","authority":"example.com","path":"/start"}` +
		"}\n")

	err := ParseStream(input, func(e model.Event) error { return nil })
	if err == nil {
		t.Fatal("ParseStream(non-canonical downstream-start) error = nil, want error")
	}
}

func TestParseDefaultsHTTP11StreamID(t *testing.T) {
	input := strings.NewReader("{" +
		`"type":"meta","format_version":"1.0"` +
		"}\n" +
		"{" +
		`"type":"request","connection_id":1,"http":{"version":"HTTP/1.1","method":"GET","scheme":"http","authority":"example.com","path":"/"}` +
		"}\n")

	events := make([]model.Event, 0)
	if err := ParseStream(input, func(e model.Event) error {
		events = append(events, e)
		return nil
	}); err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if got, want := len(events), 2; got != want {
		t.Fatalf("ParseStream() event count = %d, want %d", got, want)
	}
	if got, want := events[1].StreamID, 1; got != want {
		t.Errorf("ParseStream(HTTP/1.1 stream_id) = %d, want %d", got, want)
	}
	if got, want := events[1].Sequence, 1; got != want {
		t.Errorf("ParseStream(HTTP/1.1 sequence) = %d, want %d", got, want)
	}
}

func TestParseSkipsNonJSONLines(t *testing.T) {
	input := strings.NewReader("{" +
		`"type":"meta","format_version":"1.0"` +
		"}\n" +
		"[2026-04-23 07:12:34.566][1][info][main] shutting down parent after drain\n" +
		"{" +
		`"type":"request","connection_id":1,"http":{"version":"HTTP/1.1","method":"GET","scheme":"http","authority":"example.com","path":"/"}` +
		"}\n")

	events := make([]model.Event, 0)
	if err := ParseStream(input, func(e model.Event) error {
		events = append(events, e)
		return nil
	}); err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if got, want := len(events), 2; got != want {
		t.Fatalf("ParseStream() event count = %d, want %d", got, want)
	}
	if got, want := events[1].Sequence, 1; got != want {
		t.Errorf("ParseStream(request sequence) = %d, want %d", got, want)
	}
}

func TestParseAcceptsNoMetaFirst(t *testing.T) {
	input := strings.NewReader("{" +
		`"type":"request","connection_id":1,"http":{"method":"GET","scheme":"http","authority":"example.com","path":"/"}` +
		"}\n")

	var events []model.Event
	err := ParseStream(input, func(e model.Event) error {
		events = append(events, e)
		return nil
	})
	if err != nil {
		t.Fatalf("expected success when first line is not meta, got error: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
}

func TestParseRejectsUnsupportedFormatVersion(t *testing.T) {
	input := strings.NewReader("{" +
		`"type":"meta","format_version":"2.0"` +
		"}\n" +
		"{" +
		`"type":"request","connection_id":1,"http":{"method":"GET","scheme":"http","authority":"example.com","path":"/"}` +
		"}\n")

	var events []model.Event
	err := ParseStream(input, func(e model.Event) error {
		events = append(events, e)
		return nil
	})
	if err == nil {
		t.Fatal("expected error for unsupported format_version")
	}
}

func TestParseRejectsHTTP11StreamIDNotOne(t *testing.T) {
	input := strings.NewReader("{" +
		`"type":"meta","format_version":"1.0"` +
		"}\n" +
		"{" +
		`"type":"request","connection_id":1,"stream_id":2,"http":{"version":"HTTP/1.1","method":"GET","scheme":"http","authority":"example.com","path":"/"}` +
		"}\n")

	var events []model.Event
	err := ParseStream(input, func(e model.Event) error {
		events = append(events, e)
		return nil
	})
	if err == nil {
		t.Fatal("expected error for HTTP/1.1 with stream_id != 1")
	}
}

func TestParseRejectsNonMonotonicSequence(t *testing.T) {
	input := strings.NewReader("{" +
		`"type":"meta","format_version":"1.0"` +
		"}\n" +
		"{" +
		`"type":"request","connection_id":1,"sequence":2,"http":{"method":"GET","scheme":"http","authority":"example.com","path":"/a"}` +
		"}\n" +
		"{" +
		`"type":"request","connection_id":1,"sequence":1,"http":{"method":"GET","scheme":"http","authority":"example.com","path":"/b"}` +
		"}\n")

	var events []model.Event
	err := ParseStream(input, func(e model.Event) error {
		events = append(events, e)
		return nil
	})
	if err == nil {
		t.Fatal("expected error for non-monotonic sequence")
	}
}

func TestParseAssignsResponseSequenceFromRequest(t *testing.T) {
	input := strings.NewReader("{" +
		`"type":"meta","format_version":"1.0"` +
		"}\n" +
		"{" +
		`"type":"request","connection_id":1,"stream_id":7,"http":{"method":"GET","scheme":"http","authority":"example.com","path":"/a"}` +
		"}\n" +
		"{" +
		`"type":"response","connection_id":1,"stream_id":7,"status":200}` +
		"\n")

	events := make([]model.Event, 0)
	if err := ParseStream(input, func(e model.Event) error {
		events = append(events, e)
		return nil
	}); err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if got, want := len(events), 3; got != want {
		t.Fatalf("ParseStream() event count = %d, want %d", got, want)
	}
	if got, want := events[1].Sequence, 1; got != want {
		t.Errorf("ParseStream(request sequence) = %d, want %d", got, want)
	}
	if got, want := events[2].Sequence, 1; got != want {
		t.Errorf("ParseStream(response sequence) = %d, want %d", got, want)
	}
}

func TestParseRejectsMalformedTimestamp(t *testing.T) {
	input := strings.NewReader("{" +
		`"type":"meta","format_version":"1.0"` +
		"}\n" +
		"{" +
		`"type":"request","connection_id":1,"timestamp":"not-a-time","http":{"method":"GET","scheme":"http","authority":"example.com","path":"/"}` +
		"}\n")

	var events []model.Event
	err := ParseStream(input, func(e model.Event) error {
		events = append(events, e)
		return nil
	})
	if err == nil {
		t.Fatal("expected error for malformed timestamp")
	}
}

func TestParseConnectionCloseCleansUpState(t *testing.T) {
	input := strings.NewReader("{" +
		`"type":"meta","format_version":"1.0"` +
		"}\n" +
		"{" +
		`"type":"request","connection_id":1,"http":{"authority":"example.com","path":"/"}` +
		"}\n" +
		"{" +
		`"type":"connection_close","connection_id":1,"reason":"remote_close"` +
		"}\n" +
		"{" +
		`"type":"request","connection_id":1,"http":{"authority":"example.com","path":"/"}` +
		"}\n")

	events := make([]model.Event, 0)
	if err := ParseStream(input, func(e model.Event) error {
		events = append(events, e)
		return nil
	}); err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if len(events) != 4 {
		t.Fatalf("expected 4 events, got %d", len(events))
	}
	if got, want := events[3].Sequence, 1; got != want {
		t.Errorf("Sequence after connection_close = %d, want %d (state not cleaned up)", got, want)
	}
}

func TestParseIsolatesStateByNode(t *testing.T) {
	input := strings.NewReader("{" +
		`"type":"meta","format_version":"1.0"` +
		"}\n" +
		"{" +
		`"type":"request","node":"envoy-a","connection_id":1,"http":{"authority":"example.com","path":"/a"}` +
		"}\n" +
		"{" +
		`"type":"request","node":"envoy-b","connection_id":1,"http":{"authority":"example.com","path":"/b"}` +
		"}\n" +
		"{" +
		`"type":"connection_close","node":"envoy-a","connection_id":1,"reason":"remote_close"` +
		"}\n" +
		"{" +
		`"type":"connection_close","node":"envoy-b","connection_id":1,"reason":"remote_close"` +
		"}\n")

	events := make([]model.Event, 0)
	if err := ParseStream(input, func(e model.Event) error {
		events = append(events, e)
		return nil
	}); err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if len(events) != 5 {
		t.Fatalf("expected 5 events, got %d", len(events))
	}
	if got, want := events[1].Sequence, 1; got != want {
		t.Errorf("envoy-a sequence = %d, want %d", got, want)
	}
	if got, want := events[2].Sequence, 1; got != want {
		t.Errorf("envoy-b sequence = %d, want %d", got, want)
	}
}

func TestParseConnectionCloseInvalidReason(t *testing.T) {
	input := strings.NewReader("{" +
		`"type":"connection_close","connection_id":1,"reason":"invalid_reason"` +
		"}\n")

	err := ParseStream(input, func(e model.Event) error { return nil })
	if err == nil {
		t.Fatal("expected error for invalid connection_close reason")
	}
}

func TestParseResponseDefaultsHTTP11StreamID(t *testing.T) {
	input := strings.NewReader("{" +
		`"type":"meta","format_version":"1.0"` +
		"}\n" +
		"{" +
		`"type":"request","connection_id":1,"stream_id":1,"http":{"version":"HTTP/1.1","method":"GET","scheme":"http","authority":"example.com","path":"/"}` +
		"}\n" +
		"{" +
		`"type":"response","connection_id":1,"http":{"version":"HTTP/1.1"},"status":200` +
		"}\n")

	events := make([]model.Event, 0)
	if err := ParseStream(input, func(e model.Event) error {
		events = append(events, e)
		return nil
	}); err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if len(events) != 3 {
		t.Fatalf("expected 3 events, got %d", len(events))
	}
	if got, want := events[2].StreamID, 1; got != want {
		t.Errorf("ParseStream(response HTTP/1.1 stream_id) = %d, want %d", got, want)
	}
}
