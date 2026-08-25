package parser

import (
	"bytes"
	"io"
	"log/slog"
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

func TestParseDownstreamEnd(t *testing.T) {
	input := strings.NewReader(
		`{"type":"DownstreamEnd","node":"envoy-a","connection_id":7,"request_id":"request-a","unknown_envoy_field":{"value":1},` +
			`"timestamp":"2026-02-27T03:10:22.001Z","method":"POST","scheme":"https",` +
			`"authority":"example.com","path":"/items","protocol":"HTTP/1.1","response_code":503,` +
			`"duration_ms":16,"user_agent":"field-agent","headers":{"content-type":["application/json"]},` +
			`"body":{"encoding":"base64","content":"e30=","size_bytes":2},` +
			`"response_headers":{"x-result":["one","two"]},` +
			`"response_body":{"encoding":"base64","content":"b2s=","size_bytes":2},` +
			`"response_flags":"DC,UR"}` + "\n" +
			`{"node":"envoy-a","connection_id":7,"stream_id":1,"timestamp":"2026-02-27T03:10:23.001Z",` +
			`"method":"GET","authority":"example.com","path":"/next","protocol":"HTTP/1.1",` +
			`"response_code":201,"user_agent":"field-agent","headers":{"User-Agent":["header-agent"]}}` + "\n")

	events := parseEvents(t, input)
	if got, want := len(events), 2; got != want {
		t.Fatalf("len(events) = %d, want %d", got, want)
	}

	explicit := events[0]
	if explicit.Type != model.EventRequest || explicit.RequestID != "request-a" {
		t.Errorf("explicit End identity = type %q request_id %q, want request/request-a", explicit.Type, explicit.RequestID)
	}
	if explicit.Node != "envoy-a" || explicit.ConnectionID != 7 || explicit.Sequence != 1 || explicit.StreamID != 1 {
		t.Errorf("explicit End connection = %q/%d sequence=%d stream=%d, want envoy-a/7 sequence=1 stream=1", explicit.Node, explicit.ConnectionID, explicit.Sequence, explicit.StreamID)
	}
	if explicit.Scheme != "https" || explicit.DurationMS != 16 {
		t.Errorf("explicit End metadata = scheme %q duration %v, want https/16", explicit.Scheme, explicit.DurationMS)
	}
	if !reflect.DeepEqual(explicit.ResponseFlags, []string{"DC", "UR"}) {
		t.Errorf("explicit End response flags = %v, want [DC UR]", explicit.ResponseFlags)
	}
	if got, want := explicit.Headers["user-agent"], []string{"field-agent"}; !reflect.DeepEqual(got, want) {
		t.Errorf("explicit End user-agent header = %v, want %v", got, want)
	}
	if explicit.Body == nil || explicit.Body.Content != "e30=" || explicit.ResponseBody == nil || explicit.ResponseBody.Content != "b2s=" {
		t.Errorf("explicit End bodies = request %#v response %#v, want preserved bodies", explicit.Body, explicit.ResponseBody)
	}
	if got, want := explicit.ResponseHeaders["x-result"], []string{"one", "two"}; !reflect.DeepEqual(got, want) {
		t.Errorf("explicit End response headers = %v, want %v", got, want)
	}

	untyped := events[1]
	if untyped.Type != model.EventRequest || untyped.RequestID != "" || untyped.Sequence != 2 || untyped.StreamID != 1 {
		t.Errorf("untyped End identity = type %q request_id %q sequence=%d stream=%d, want request/empty/2/1", untyped.Type, untyped.RequestID, untyped.Sequence, untyped.StreamID)
	}
	if untyped.ResponseFlags != nil {
		t.Errorf("untyped End response flags = %v, want nil", untyped.ResponseFlags)
	}
	if got, want := untyped.Headers["User-Agent"], []string{"header-agent"}; !reflect.DeepEqual(got, want) {
		t.Errorf("untyped End User-Agent header = %v, want %v", got, want)
	}
	if got := untyped.Headers["user-agent"]; got != nil {
		t.Errorf("untyped End synthesized user-agent header = %v, want nil", got)
	}
}

func TestParseDownstreamEndResponseFlags(t *testing.T) {
	const fields = `"connection_id":1,"timestamp":"2026-02-27T03:10:22Z","method":"GET","authority":"example.com","path":"/","protocol":"HTTP/2","response_code":200`
	tests := []struct {
		name   string
		suffix string
		want   []string
	}{
		{name: "omitted"},
		{name: "empty", suffix: `,"response_flags":""`},
		{name: "hyphen", suffix: `,"response_flags":"-"`},
		{name: "tokens", suffix: `,"response_flags":"DC,UR"`, want: []string{"DC", "UR"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			events := parseEvents(t, strings.NewReader(`{"type":"DownstreamEnd",`+fields+test.suffix+"}\n"))
			if got := events[0].ResponseFlags; !reflect.DeepEqual(got, test.want) {
				t.Errorf("ParseStream(response_flags %s) = %v, want %v", test.name, got, test.want)
			}
		})
	}
}

func TestParseDownstreamEndPreservesAppendOrder(t *testing.T) {
	input := strings.NewReader(
		`{"type":"DownstreamEnd","connection_id":1,"request_id":"B","stream_id":2,"timestamp":"2026-02-27T03:10:23Z","method":"GET","authority":"example.com","path":"/b","protocol":"HTTP/2","response_code":200}` + "\n" +
			`{"connection_id":1,"request_id":"A","stream_id":1,"timestamp":"2026-02-27T03:10:22Z","method":"GET","authority":"example.com","path":"/a","protocol":"HTTP/2","response_code":200}` + "\n")

	events := parseEvents(t, input)
	if got, want := []string{events[0].RequestID, events[1].RequestID}, []string{"B", "A"}; !reflect.DeepEqual(got, want) {
		t.Errorf("ParseStream(B,A) request order = %v, want %v", got, want)
	}
	if got, want := []int{events[0].Sequence, events[1].Sequence}, []int{1, 2}; !reflect.DeepEqual(got, want) {
		t.Errorf("ParseStream(B,A) sequences = %v, want %v", got, want)
	}
}

func TestParseDownstreamEndWarning(t *testing.T) {
	const directFields = `"connection_id":1,"timestamp":"2026-02-27T03:10:22Z","method":"GET","authority":"example.com","path":"/","protocol":"HTTP/2","response_code":200`
	const canonicalFields = `"request_id":"request-a","connection_id":1,"timestamp":"2026-02-27T03:10:22Z","method":"GET","authority":"example.com","path":"/","protocol":"HTTP/2","response_code":200`
	tests := []struct {
		name         string
		input        string
		wantWarnings int
		wantError    bool
	}{
		{
			name:         "multi_record_direct",
			input:        `{"type":"DownstreamEnd",` + directFields + "}\n{" + directFields + "}\n",
			wantWarnings: 1,
		},
		{
			name:  "canonical_only",
			input: `{"type":"request",` + canonicalFields + "}\n",
		},
		{
			name:      "invalid_direct",
			input:     `{"type":"DownstreamEnd","connection_id":1}` + "\n",
			wantError: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var logs bytes.Buffer
			previousLogger := slog.Default()
			slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
			t.Cleanup(func() {
				slog.SetDefault(previousLogger)
			})

			err := ParseStream(strings.NewReader(test.input), func(model.Event) error { return nil })
			if got := err != nil; got != test.wantError {
				t.Errorf("ParseStream(%s) error presence = %t, want %t; error=%v", test.name, got, test.wantError, err)
			}
			if got := strings.Count(logs.String(), downstreamEndWarning); got != test.wantWarnings {
				t.Errorf("ParseStream(%s) warning count = %d, want %d; logs=%q", test.name, got, test.wantWarnings, logs.String())
			}
		})
	}
}

func TestParseDownstreamEndRejectsHTTP11StreamID(t *testing.T) {
	for _, streamID := range []string{"0", "2", "null"} {
		t.Run("stream_id_"+streamID, func(t *testing.T) {
			input := strings.NewReader(`{"type":"DownstreamEnd","connection_id":1,"stream_id":` + streamID + `,"timestamp":"2026-02-27T03:10:22Z","method":"GET","authority":"example.com","path":"/","protocol":"HTTP/1.1","response_code":200}` + "\n")
			err := ParseStream(input, func(model.Event) error { return nil })
			if err == nil {
				t.Fatalf("ParseStream(DownstreamEnd HTTP/1.1 stream_id=%s) error = nil, want error", streamID)
			}
			if !strings.Contains(err.Error(), "must omit stream_id or use stream_id=1") {
				t.Errorf("ParseStream(DownstreamEnd HTTP/1.1 stream_id=%s) error = %q, want stream ID error", streamID, err)
			}
		})
	}
}

func TestParseDownstreamEndRejectsLegacySchema(t *testing.T) {
	const fields = `"connection_id":1,"timestamp":"2026-02-27T03:10:22Z","method":"GET","authority":"example.com","path":"/","protocol":"HTTP/2","response_code":200`
	tests := []struct {
		name  string
		input string
	}{
		{name: "nested_http_untyped", input: `{` + fields + `,"http":{"method":"GET"}}`},
		{name: "start_time", input: `{"type":"DownstreamEnd",` + fields + `,"start_time":"2026-02-27T03:10:22Z"}`},
		{name: "status", input: `{"type":"DownstreamEnd",` + fields + `,"status":200}`},
		{name: "log_type", input: `{"type":"DownstreamEnd",` + fields + `,"log_type":"response"}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := ParseStream(strings.NewReader(test.input+"\n"), func(model.Event) error { return nil })
			const wantError = "line 1: access log must use the flat Envoy schema"
			if err == nil || err.Error() != wantError {
				t.Errorf("ParseStream(%s) error = %v, want %q", test.name, err, wantError)
			}
		})
	}
}

func TestParseRejectsMixedInputFamilies(t *testing.T) {
	const canonical = `{"type":"request","request_id":"request-a","connection_id":1,"timestamp":"2026-02-27T03:10:22Z","method":"GET","authority":"example.com","path":"/","protocol":"HTTP/2","response_code":200}`
	const direct = `{"type":"DownstreamEnd","connection_id":1,"timestamp":"2026-02-27T03:10:23Z","method":"GET","authority":"example.com","path":"/","protocol":"HTTP/2","response_code":200}`
	canonicalSequence1 := strings.Replace(canonical, `"connection_id":1`, `"connection_id":1,"sequence":1`, 1)
	canonicalSequence2 := strings.Replace(canonical, `"connection_id":1`, `"connection_id":1,"sequence":2`, 1)
	directSequence1 := strings.Replace(direct, `"connection_id":1`, `"connection_id":1,"sequence":1`, 1)
	directSequence2 := strings.Replace(direct, `"connection_id":1`, `"connection_id":1,"sequence":2`, 1)
	tests := []struct {
		name      string
		input     string
		wantError string
	}{
		{
			name:      "canonical_to_direct",
			input:     canonical + "\nprocess line\n" + direct + "\n",
			wantError: "line 3: cannot mix canonical replay events with DownstreamEnd access logs",
		},
		{
			name:      "direct_to_canonical",
			input:     direct + "\n\n" + canonical + "\n",
			wantError: "line 3: cannot mix canonical replay events with DownstreamEnd access logs",
		},
		{
			name:      "canonical_to_direct_with_decreasing_sequence",
			input:     canonicalSequence2 + "\n" + directSequence1 + "\n",
			wantError: "line 2: cannot mix canonical replay events with DownstreamEnd access logs",
		},
		{
			name:      "direct_to_canonical_with_decreasing_sequence",
			input:     directSequence2 + "\n" + canonicalSequence1 + "\n",
			wantError: "line 2: cannot mix canonical replay events with DownstreamEnd access logs",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			handled := 0
			err := ParseStream(strings.NewReader(test.input), func(model.Event) error {
				handled++
				return nil
			})
			if err == nil || err.Error() != test.wantError {
				t.Errorf("ParseStream(%s) error = %v, want %q", test.name, err, test.wantError)
			}
			if handled != 1 {
				t.Errorf("ParseStream(%s) handled events = %d, want 1", test.name, handled)
			}
		})
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
		{name: "null_type", input: `{"type":null,` + validFields + `}`},
		{name: "empty_type", input: `{"type":"",` + validFields + `}`},
		{name: "lowercase_end", input: `{"type":"downstreamend",` + validFields + `}`},
		{name: "connection_open", input: `{"type":"connection_open","connection_id":1}`},
		{name: "unknown_type", input: `{"type":"response",` + validFields + `}`},
		{name: "string_response_flags", input: `{"type":"request",` + validFields + `,"response_flags":"DC"}`},
		{name: "null_response_flags", input: `{"type":"request",` + validFields + `,"response_flags":null}`},
		{name: "nonstring_response_flag", input: `{"type":"request",` + validFields + `,"response_flags":["DC",1]}`},
		{name: "null_response_flag_element", input: `{"type":"request",` + validFields + `,"response_flags":["DC",null]}`},
		{name: "direct_array_response_flags", input: `{"type":"DownstreamEnd",` + validFields + `,"response_flags":["DC"]}`},
		{name: "direct_null_response_flags", input: `{"type":"DownstreamEnd",` + validFields + `,"response_flags":null}`},
		{name: "direct_empty_response_flag_token", input: `{"type":"DownstreamEnd",` + validFields + `,"response_flags":"DC,"}`},
		{name: "missing_request_id", input: `{"type":"request","connection_id":1,"timestamp":"2026-02-27T03:10:22Z","method":"GET","authority":"example.com","path":"/","protocol":"HTTP/1.1","response_code":200}`},
		{name: "missing_connection_id", input: `{"type":"request","request_id":"a","timestamp":"2026-02-27T03:10:22Z","method":"GET","authority":"example.com","path":"/","protocol":"HTTP/1.1","response_code":200}`},
		{name: "missing_timestamp", input: `{"type":"request","request_id":"a","connection_id":1,"method":"GET","authority":"example.com","path":"/","protocol":"HTTP/1.1","response_code":200}`},
		{name: "invalid_timestamp", input: `{"type":"request","request_id":"a","connection_id":1,"timestamp":"invalid","method":"GET","authority":"example.com","path":"/","protocol":"HTTP/1.1","response_code":200}`},
		{name: "missing_method", input: `{"type":"request","request_id":"a","connection_id":1,"timestamp":"2026-02-27T03:10:22Z","authority":"example.com","path":"/","protocol":"HTTP/1.1","response_code":200}`},
		{name: "missing_authority", input: `{"type":"request","request_id":"a","connection_id":1,"timestamp":"2026-02-27T03:10:22Z","method":"GET","path":"/","protocol":"HTTP/1.1","response_code":200}`},
		{name: "missing_path", input: `{"type":"request","request_id":"a","connection_id":1,"timestamp":"2026-02-27T03:10:22Z","method":"GET","authority":"example.com","protocol":"HTTP/1.1","response_code":200}`},
		{name: "missing_protocol", input: `{"type":"request","request_id":"a","connection_id":1,"timestamp":"2026-02-27T03:10:22Z","method":"GET","authority":"example.com","path":"/","response_code":200}`},
		{name: "missing_response_code", input: `{"type":"request","request_id":"a","connection_id":1,"timestamp":"2026-02-27T03:10:22Z","method":"GET","authority":"example.com","path":"/","protocol":"HTTP/1.1"}`},
		{name: "direct_missing_connection_id", input: `{"type":"DownstreamEnd","timestamp":"2026-02-27T03:10:22Z","method":"GET","authority":"example.com","path":"/","protocol":"HTTP/1.1","response_code":200}`},
		{name: "direct_missing_timestamp", input: `{"type":"DownstreamEnd","connection_id":1,"method":"GET","authority":"example.com","path":"/","protocol":"HTTP/1.1","response_code":200}`},
		{name: "direct_invalid_timestamp", input: `{"type":"DownstreamEnd","connection_id":1,"timestamp":"invalid","method":"GET","authority":"example.com","path":"/","protocol":"HTTP/1.1","response_code":200}`},
		{name: "direct_missing_method", input: `{"type":"DownstreamEnd","connection_id":1,"timestamp":"2026-02-27T03:10:22Z","authority":"example.com","path":"/","protocol":"HTTP/1.1","response_code":200}`},
		{name: "direct_missing_authority", input: `{"type":"DownstreamEnd","connection_id":1,"timestamp":"2026-02-27T03:10:22Z","method":"GET","path":"/","protocol":"HTTP/1.1","response_code":200}`},
		{name: "direct_missing_path", input: `{"type":"DownstreamEnd","connection_id":1,"timestamp":"2026-02-27T03:10:22Z","method":"GET","authority":"example.com","protocol":"HTTP/1.1","response_code":200}`},
		{name: "direct_missing_protocol", input: `{"type":"DownstreamEnd","connection_id":1,"timestamp":"2026-02-27T03:10:22Z","method":"GET","authority":"example.com","path":"/","response_code":200}`},
		{name: "direct_missing_response_code", input: `{"type":"DownstreamEnd","connection_id":1,"timestamp":"2026-02-27T03:10:22Z","method":"GET","authority":"example.com","path":"/","protocol":"HTTP/1.1"}`},
		{name: "direct_noninteger_response_code", input: `{"type":"DownstreamEnd","connection_id":1,"timestamp":"2026-02-27T03:10:22Z","method":"GET","authority":"example.com","path":"/","protocol":"HTTP/1.1","response_code":"200"}`},
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

func TestParseRejectsHTTP11StreamID(t *testing.T) {
	for _, streamID := range []string{"0", "1", "2", "null"} {
		t.Run("stream_id_"+streamID, func(t *testing.T) {
			input := strings.NewReader(`{"type":"request","request_id":"a","connection_id":1,"stream_id":` + streamID + `,"timestamp":"2026-02-27T03:10:22Z","method":"GET","authority":"example.com","path":"/","protocol":"HTTP/1.1","response_code":200}` + "\n")
			err := ParseStream(input, func(model.Event) error { return nil })
			if err == nil {
				t.Fatalf("ParseStream(HTTP/1.1 stream_id=%s) error = nil, want error", streamID)
			}
			if !strings.Contains(err.Error(), "HTTP/1.1 requests must omit stream_id") {
				t.Errorf("ParseStream(HTTP/1.1 stream_id=%s) error = %q, want omitted-stream error", streamID, err)
			}
		})
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
