package parser

import (
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/reqfleet/replay/internal/model"
)

func TestParseCanonicalRequest(t *testing.T) {
	input := strings.NewReader(
		`{"type":"request","node":"envoy-a","connection_id":7,"request_id":"request-a",` +
			`"timestamp":"2026-02-27T03:10:22.001Z","method":"POST","scheme":"https",` +
			`"authority":"example.com","path":"/items","protocol":"HTTP/1.1","response_code":503,` +
			`"duration_ms":16,"headers":{"content-type":["application/json"]},` +
			`"body":{"encoding":"base64","content":"e30=","size_bytes":2},` +
			`"response_headers":{"x-result":["one","two"]},` +
			`"response_body":{"encoding":"base64","content":"b2s=","size_bytes":2},` +
			`"response_flags":["DC","UR"]}` + "\n")

	events := parseEvents(t, input)
	if got, want := len(events), 1; got != want {
		t.Fatalf("len(events) = %d, want %d", got, want)
	}
	event := events[0]
	if event.Type != model.EventRequest || event.RequestID != "request-a" {
		t.Errorf("parsed identity = type %q request_id %q, want request/request-a", event.Type, event.RequestID)
	}
	if event.Node != "envoy-a" || event.ConnectionID != 7 || event.Sequence != 1 || event.StreamID != 1 {
		t.Errorf("parsed connection = %q/%d sequence=%d stream=%d, want envoy-a/7 sequence=1 stream=1", event.Node, event.ConnectionID, event.Sequence, event.StreamID)
	}
	if event.ResponseCode == nil || *event.ResponseCode != 503 {
		t.Errorf("event.ResponseCode = %v, want 503", event.ResponseCode)
	}
	if !reflect.DeepEqual(event.ResponseFlags, []string{"DC", "UR"}) {
		t.Errorf("event.ResponseFlags = %v, want [DC UR]", event.ResponseFlags)
	}
	if event.Body == nil || event.Body.Content != "e30=" || event.ResponseBody == nil || event.ResponseBody.Content != "b2s=" {
		t.Errorf("parsed bodies = request %#v response %#v, want preserved bodies", event.Body, event.ResponseBody)
	}
	if got, want := event.ResponseHeaders["x-result"], []string{"one", "two"}; !reflect.DeepEqual(got, want) {
		t.Errorf("event.ResponseHeaders[x-result] = %v, want %v", got, want)
	}
}

func TestParseConnectionCloseDoesNotAdvanceSequence(t *testing.T) {
	input := strings.NewReader(
		`{"type":"request","request_id":"a","connection_id":1,"timestamp":"2026-02-27T03:10:22Z","method":"GET","authority":"example.com","path":"/a","protocol":"HTTP/2","response_code":200}` + "\n" +
			`{"type":"connection_close","node":"envoy-a","connection_id":9}` + "\n" +
			`{"type":"request","request_id":"b","connection_id":1,"timestamp":"2026-02-27T03:10:23Z","method":"GET","authority":"example.com","path":"/b","protocol":"HTTP/2","response_code":201}` + "\n")

	events := parseEvents(t, input)
	if got, want := len(events), 3; got != want {
		t.Fatalf("len(events) = %d, want %d", got, want)
	}
	if events[0].Sequence != 1 || events[2].Sequence != 2 {
		t.Errorf("request sequences = [%d %d], want [1 2]", events[0].Sequence, events[2].Sequence)
	}
	closeEvent := events[1]
	if closeEvent.Type != model.EventConnectionClose || closeEvent.Node != "envoy-a" || closeEvent.ConnectionID != 9 || closeEvent.Sequence != 0 {
		t.Errorf("close event = %+v, want envoy-a/9 with zero sequence", closeEvent)
	}
}

func TestParseRejectsNonCanonicalInput(t *testing.T) {
	validFields := `"request_id":"request-a","connection_id":1,"timestamp":"2026-02-27T03:10:22Z","method":"GET","authority":"example.com","path":"/","protocol":"HTTP/1.1","response_code":200`
	tests := []struct {
		name  string
		input string
	}{
		{name: "raw_start", input: `{"type":"DownstreamStart",` + validFields + `}`},
		{name: "raw_end", input: `{"type":"DownstreamEnd",` + validFields + `}`},
		{name: "omitted_type", input: `{` + validFields + `}`},
		{name: "connection_open", input: `{"type":"connection_open","connection_id":1}`},
		{name: "unknown_type", input: `{"type":"response",` + validFields + `}`},
		{name: "string_response_flags", input: `{"type":"request",` + validFields + `,"response_flags":"DC"}`},
		{name: "null_response_flags", input: `{"type":"request",` + validFields + `,"response_flags":null}`},
		{name: "nonstring_response_flag", input: `{"type":"request",` + validFields + `,"response_flags":["DC",1]}`},
		{name: "null_response_flag_element", input: `{"type":"request",` + validFields + `,"response_flags":["DC",null]}`},
		{name: "missing_request_id", input: `{"type":"request","connection_id":1,"timestamp":"2026-02-27T03:10:22Z","method":"GET","authority":"example.com","path":"/","protocol":"HTTP/1.1","response_code":200}`},
		{name: "missing_connection_id", input: `{"type":"request","request_id":"a","timestamp":"2026-02-27T03:10:22Z","method":"GET","authority":"example.com","path":"/","protocol":"HTTP/1.1","response_code":200}`},
		{name: "missing_timestamp", input: `{"type":"request","request_id":"a","connection_id":1,"method":"GET","authority":"example.com","path":"/","protocol":"HTTP/1.1","response_code":200}`},
		{name: "invalid_timestamp", input: `{"type":"request","request_id":"a","connection_id":1,"timestamp":"invalid","method":"GET","authority":"example.com","path":"/","protocol":"HTTP/1.1","response_code":200}`},
		{name: "missing_method", input: `{"type":"request","request_id":"a","connection_id":1,"timestamp":"2026-02-27T03:10:22Z","authority":"example.com","path":"/","protocol":"HTTP/1.1","response_code":200}`},
		{name: "missing_authority", input: `{"type":"request","request_id":"a","connection_id":1,"timestamp":"2026-02-27T03:10:22Z","method":"GET","path":"/","protocol":"HTTP/1.1","response_code":200}`},
		{name: "missing_path", input: `{"type":"request","request_id":"a","connection_id":1,"timestamp":"2026-02-27T03:10:22Z","method":"GET","authority":"example.com","protocol":"HTTP/1.1","response_code":200}`},
		{name: "missing_protocol", input: `{"type":"request","request_id":"a","connection_id":1,"timestamp":"2026-02-27T03:10:22Z","method":"GET","authority":"example.com","path":"/","response_code":200}`},
		{name: "missing_response_code", input: `{"type":"request","request_id":"a","connection_id":1,"timestamp":"2026-02-27T03:10:22Z","method":"GET","authority":"example.com","path":"/","protocol":"HTTP/1.1"}`},
		{name: "malformed_json", input: `{"type":"request"`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := ParseStream(strings.NewReader(test.input+"\n"), func(model.Event) error { return nil })
			if err == nil {
				t.Fatalf("ParseStream(%s) error = nil, want error", test.input)
			}
			if !strings.Contains(err.Error(), "line 1") {
				t.Errorf("ParseStream(%s) error = %q, want line number", test.input, err)
			}
		})
	}
}

func TestParseSkipsProcessLines(t *testing.T) {
	input := strings.NewReader(
		"\n[2026-04-23 07:12:34.566][info] shutting down\n" +
			`{"type":"request","request_id":"a","connection_id":1,"timestamp":"2026-02-27T03:10:22Z","method":"GET","authority":"example.com","path":"/","protocol":"HTTP/1.1","response_code":200}` + "\n")
	if got, want := len(parseEvents(t, input)), 1; got != want {
		t.Errorf("len(parseEvents(process lines)) = %d, want %d", got, want)
	}
}

func TestParseRejectsHTTP11StreamIDNotOne(t *testing.T) {
	input := strings.NewReader(`{"type":"request","request_id":"a","connection_id":1,"stream_id":2,"timestamp":"2026-02-27T03:10:22Z","method":"GET","authority":"example.com","path":"/","protocol":"HTTP/1.1","response_code":200}` + "\n")
	if err := ParseStream(input, func(model.Event) error { return nil }); err == nil {
		t.Fatal("ParseStream(HTTP/1.1 stream_id=2) error = nil, want error")
	}
}

func TestParseRejectsNonMonotonicSequence(t *testing.T) {
	input := strings.NewReader(
		`{"type":"request","request_id":"a","connection_id":1,"sequence":2,"timestamp":"2026-02-27T03:10:22Z","method":"GET","authority":"example.com","path":"/a","protocol":"HTTP/1.1","response_code":200}` + "\n" +
			`{"type":"request","request_id":"b","connection_id":1,"sequence":1,"timestamp":"2026-02-27T03:10:23Z","method":"GET","authority":"example.com","path":"/b","protocol":"HTTP/1.1","response_code":200}` + "\n")
	if err := ParseStream(input, func(model.Event) error { return nil }); err == nil {
		t.Fatal("ParseStream(non-monotonic sequence) error = nil, want error")
	}
}

func TestParseIsolatesSequenceByNode(t *testing.T) {
	input := strings.NewReader(
		`{"type":"request","node":"envoy-a","request_id":"a","connection_id":1,"timestamp":"2026-02-27T03:10:22Z","method":"GET","authority":"example.com","path":"/a","protocol":"HTTP/1.1","response_code":200}` + "\n" +
			`{"type":"request","node":"envoy-b","request_id":"b","connection_id":1,"timestamp":"2026-02-27T03:10:23Z","method":"GET","authority":"example.com","path":"/b","protocol":"HTTP/1.1","response_code":200}` + "\n")
	events := parseEvents(t, input)
	if events[0].Sequence != 1 || events[1].Sequence != 1 {
		t.Errorf("event sequences = [%d %d], want [1 1]", events[0].Sequence, events[1].Sequence)
	}
}

func TestScanObjectsReportsPhysicalLineNumbers(t *testing.T) {
	input := strings.NewReader("process line\n\n  {\"value\":1}\n")
	var lines []int
	if err := ScanObjects(input, func(line int, object []byte) error {
		lines = append(lines, line)
		return nil
	}); err != nil {
		t.Fatalf("ScanObjects() error: %v", err)
	}
	if !reflect.DeepEqual(lines, []int{3}) {
		t.Errorf("ScanObjects() lines = %v, want [3]", lines)
	}
}

func parseEvents(t *testing.T, input io.Reader) []model.Event {
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
