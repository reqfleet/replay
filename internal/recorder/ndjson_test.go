package recorder

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/reqfleet/replay/internal/model"
	"github.com/reqfleet/replay/internal/parser"
)

type fixtureObservation struct {
	Type            string              `json:"type,omitempty"`
	Node            string              `json:"node,omitempty"`
	ConnectionID    *int                `json:"connection_id,omitempty"`
	RequestID       string              `json:"request_id,omitempty"`
	Timestamp       string              `json:"timestamp,omitempty"`
	Method          string              `json:"method,omitempty"`
	Scheme          string              `json:"scheme,omitempty"`
	Authority       string              `json:"authority,omitempty"`
	Path            string              `json:"path,omitempty"`
	Protocol        string              `json:"protocol,omitempty"`
	UserAgent       string              `json:"user_agent,omitempty"`
	StreamID        int                 `json:"stream_id,omitempty"`
	Headers         map[string][]string `json:"headers,omitempty"`
	Body            *model.Body         `json:"body,omitempty"`
	ResponseCode    *int                `json:"response_code,omitempty"`
	DurationMS      float64             `json:"duration_ms,omitempty"`
	ResponseHeaders map[string][]string `json:"response_headers,omitempty"`
	ResponseBody    *model.Body         `json:"response_body,omitempty"`
	ResponseFlags   any                 `json:"response_flags,omitempty"`
}

func fixture(kind string, connectionID int, requestID, path string) fixtureObservation {
	status := 200
	observation := fixtureObservation{
		Type:         kind,
		Node:         "envoy-a",
		ConnectionID: &connectionID,
		RequestID:    requestID,
		Timestamp:    "2026-08-21T10:00:00Z",
		Method:       "GET",
		Scheme:       "https",
		Authority:    "example.test",
		Path:         path,
		Protocol:     "HTTP/2",
		StreamID:     1,
	}
	if kind == downstreamEnd {
		observation.ResponseCode = &status
		observation.ResponseFlags = "-"
	}
	return observation
}

func observationsNDJSON(t *testing.T, observations ...fixtureObservation) string {
	t.Helper()
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	for _, observation := range observations {
		if err := encoder.Encode(observation); err != nil {
			t.Fatalf("json.Encoder.Encode(%+v) error: %v", observation, err)
		}
	}
	return buffer.String()
}

func observationWithRawField(t *testing.T, observation fixtureObservation, field string) string {
	t.Helper()
	data, err := json.Marshal(observation)
	if err != nil {
		t.Fatalf("json.Marshal(%+v) error: %v", observation, err)
	}
	return strings.TrimSuffix(string(data), "}") + "," + field + "}\n"
}

func combineCapture(t *testing.T, input string) (CombineSummary, string) {
	t.Helper()
	var output bytes.Buffer
	summary, err := CombineStream(strings.NewReader(input), &output)
	if err != nil {
		t.Fatalf("CombineStream() error: %v", err)
	}
	return summary, output.String()
}

func TestCombineStreamPreservesStartOrderAndRoundTrips(t *testing.T) {
	startA := fixture(downstreamStart, 7, "request-a", "/a")
	startA.StreamID = 1
	startA.Headers = map[string][]string{"x-start": {"a"}}
	startA.Body = &model.Body{Encoding: "base64", Content: "YQ==", SizeBytes: 1}
	startResponseCode := 599
	startA.ResponseCode = &startResponseCode
	startA.DurationMS = 999
	startA.ResponseFlags = "UF"
	startA.ResponseHeaders = map[string][]string{"x-response": {"start"}}
	startA.ResponseBody = &model.Body{Encoding: "base64", Content: "U1RBUlQ=", SizeBytes: 5}
	startB := fixture(downstreamStart, 7, "request-b", "/b")
	startB.Timestamp = "2026-08-21T10:00:00.010Z"
	startB.StreamID = 3

	endB := fixture(downstreamEnd, 7, "request-b", "/b")
	endB.Timestamp = startB.Timestamp
	endB.StreamID = 3
	statusB := 202
	endB.ResponseCode = &statusB
	endB.DurationMS = 20
	endB.Headers = map[string][]string{"x-from-end": {"b"}}
	endB.ResponseHeaders = map[string][]string{"x-response": {"b"}}
	endB.ResponseBody = &model.Body{Encoding: "base64", Content: "Qg==", SizeBytes: 1}

	endA := fixture(downstreamEnd, 7, "request-a", "/a")
	endA.StreamID = 1
	statusA := 201
	endA.ResponseCode = &statusA
	endA.DurationMS = 100
	endA.ResponseFlags = "DC,UR"
	endA.ResponseHeaders = map[string][]string{"x-response": {"a"}}
	endA.ResponseBody = &model.Body{Encoding: "base64", Content: "QQ==", SizeBytes: 1}

	input := "process log line\n\n" + observationsNDJSON(t, startA, startB, endB, endA)
	summary, output := combineCapture(t, input)
	wantSummary := CombineSummary{Starts: 2, Ends: 2, Records: 2, ConnectionsClosed: 1}
	if summary != wantSummary {
		t.Errorf("CombineStream() summary = %+v, want %+v", summary, wantSummary)
	}

	lines := strings.Split(strings.TrimSpace(output), "\n")
	if got, want := len(lines), 3; got != want {
		t.Fatalf("combined line count = %d, want %d; output=%s", got, want, output)
	}
	for index := range 2 {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal([]byte(lines[index]), &fields); err != nil {
			t.Fatalf("json.Unmarshal(output line %d) error: %v", index+1, err)
		}
		if _, ok := fields["sequence"]; ok {
			t.Errorf("output line %d contains sequence, want parser-derived sequence", index+1)
		}
	}

	var events []model.Event
	if err := parser.ParseStream(strings.NewReader(output), func(event model.Event) error {
		events = append(events, event)
		return nil
	}); err != nil {
		t.Fatalf("parser.ParseStream(combined output) error: %v", err)
	}
	if got, want := len(events), 3; got != want {
		t.Fatalf("len(round-trip events) = %d, want %d", got, want)
	}
	if events[0].RequestID != "request-a" || events[1].RequestID != "request-b" || events[0].Sequence != 1 || events[1].Sequence != 2 {
		t.Errorf("round-trip order = [%s/%d %s/%d], want [request-a/1 request-b/2]", events[0].RequestID, events[0].Sequence, events[1].RequestID, events[1].Sequence)
	}
	if events[0].ResponseCode == nil || *events[0].ResponseCode != 201 || events[1].ResponseCode == nil || *events[1].ResponseCode != 202 {
		t.Errorf("round-trip response codes = [%v %v], want [201 202]", events[0].ResponseCode, events[1].ResponseCode)
	}
	if !reflect.DeepEqual(events[0].ResponseFlags, []string{"DC", "UR"}) {
		t.Errorf("request-a flags = %v, want [DC UR]", events[0].ResponseFlags)
	}
	if got := events[0].Headers["x-start"]; !reflect.DeepEqual(got, []string{"a"}) || events[0].Body == nil || events[0].Body.Content != "YQ==" {
		t.Errorf("request-a request metadata = headers %v body %#v, want Start metadata", got, events[0].Body)
	}
	if got := events[1].Headers["x-from-end"]; !reflect.DeepEqual(got, []string{"b"}) {
		t.Errorf("request-b headers = %v, want End fallback [b]", got)
	}
	if events[1].ResponseBody == nil || events[1].ResponseBody.Content != "Qg==" || !reflect.DeepEqual(events[1].ResponseHeaders["x-response"], []string{"b"}) {
		t.Errorf("request-b response metadata = headers %v body %#v, want End metadata", events[1].ResponseHeaders, events[1].ResponseBody)
	}
	if events[2].Type != model.EventConnectionClose || events[2].ConnectionID != 7 {
		t.Errorf("final event = %+v, want connection_close for 7", events[2])
	}
}

func TestCombineStreamOmitsHTTP11StreamID(t *testing.T) {
	start := fixture(downstreamStart, 1, "request-a", "/")
	start.Protocol = "HTTP/1.1"
	end := fixture(downstreamEnd, 1, "request-a", "/")
	end.Protocol = "HTTP/1.1"

	_, output := combineCapture(t, observationsNDJSON(t, start, end))
	if strings.Contains(output, `"stream_id"`) {
		t.Errorf("CombineStream(HTTP/1.1 stream_id=1) output = %q, want stream_id omitted", output)
	}

	var events []model.Event
	if err := parser.ParseStream(strings.NewReader(output), func(event model.Event) error {
		events = append(events, event)
		return nil
	}); err != nil {
		t.Fatalf("parser.ParseStream(combined HTTP/1.1 output) error: %v", err)
	}
	if got, want := len(events), 1; got != want {
		t.Fatalf("len(combined HTTP/1.1 events) = %d, want %d", got, want)
	}
	if got, want := events[0].StreamID, 1; got != want {
		t.Errorf("combined HTTP/1.1 internal stream ID = %d, want %d", got, want)
	}
}

func TestCombineStreamAcceptsEndBeforeStart(t *testing.T) {
	end := fixture(downstreamEnd, 1, "request-a", "/a")
	start := fixture(downstreamStart, 1, "request-a", "/a")
	summary, output := combineCapture(t, observationsNDJSON(t, end, start))
	if summary != (CombineSummary{Starts: 1, Ends: 1, Records: 1}) {
		t.Errorf("CombineStream(End before Start) summary = %+v, want 1/1/1", summary)
	}
	if !strings.Contains(output, `"request_id":"request-a"`) {
		t.Errorf("CombineStream(End before Start) output = %q, want request-a", output)
	}
}

func TestCombineStreamStartUserAgentDoesNotSuppressEndHeaders(t *testing.T) {
	start := fixture(downstreamStart, 1, "request-a", "/a")
	start.UserAgent = "start-agent"
	end := fixture(downstreamEnd, 1, "request-a", "/a")
	end.UserAgent = "end-agent"
	end.Headers = map[string][]string{
		"authorization": {"Bearer token"},
		"x-route":       {"orders"},
	}

	_, output := combineCapture(t, observationsNDJSON(t, start, end))
	var events []model.Event
	if err := parser.ParseStream(strings.NewReader(output), func(event model.Event) error {
		events = append(events, event)
		return nil
	}); err != nil {
		t.Fatalf("parser.ParseStream(combined output) error: %v", err)
	}
	if got, want := len(events), 1; got != want {
		t.Fatalf("len(events) = %d, want %d", got, want)
	}
	if got, want := events[0].Headers["authorization"], []string{"Bearer token"}; !reflect.DeepEqual(got, want) {
		t.Errorf("event.Headers[authorization] = %v, want %v", got, want)
	}
	if got, want := events[0].Headers["x-route"], []string{"orders"}; !reflect.DeepEqual(got, want) {
		t.Errorf("event.Headers[x-route] = %v, want %v", got, want)
	}
	if got, want := events[0].Headers["user-agent"], []string{"start-agent"}; !reflect.DeepEqual(got, want) {
		t.Errorf("event.Headers[user-agent] = %v, want %v", got, want)
	}
	if got, want := events[0].UserAgent, "start-agent"; got != want {
		t.Errorf("event.UserAgent = %q, want %q", got, want)
	}
}

func TestCombineStreamScopesSameRequestIDByNodeAndConnection(t *testing.T) {
	startOne := fixture(downstreamStart, 1, "shared", "/one")
	startTwo := fixture(downstreamStart, 2, "shared", "/two")
	startOtherNode := fixture(downstreamStart, 1, "shared", "/other-node")
	startOtherNode.Node = "envoy-b"
	endOtherNode := fixture(downstreamEnd, 1, "shared", "/other-node")
	endOtherNode.Node = "envoy-b"
	endTwo := fixture(downstreamEnd, 2, "shared", "/two")
	endOne := fixture(downstreamEnd, 1, "shared", "/one")
	summary, output := combineCapture(t, observationsNDJSON(t, startOne, startTwo, startOtherNode, endOtherNode, endTwo, endOne))
	if summary.Records != 3 {
		t.Errorf("CombineStream(scoped IDs).Records = %d, want 3", summary.Records)
	}
	var events []model.Event
	if err := parser.ParseStream(strings.NewReader(output), func(event model.Event) error {
		events = append(events, event)
		return nil
	}); err != nil {
		t.Fatalf("parser.ParseStream(scoped IDs) error: %v", err)
	}
	if events[0].Node != "envoy-a" || events[0].ConnectionID != 1 ||
		events[1].Node != "envoy-a" || events[1].ConnectionID != 2 ||
		events[2].Node != "envoy-b" || events[2].ConnectionID != 1 ||
		events[0].Sequence != 1 || events[1].Sequence != 1 || events[2].Sequence != 1 {
		t.Errorf("scoped events = [%s/%d/%d %s/%d/%d %s/%d/%d], want [envoy-a/1/1 envoy-a/2/1 envoy-b/1/1]",
			events[0].Node, events[0].ConnectionID, events[0].Sequence,
			events[1].Node, events[1].ConnectionID, events[1].Sequence,
			events[2].Node, events[2].ConnectionID, events[2].Sequence)
	}
}

func TestCombineStreamNormalizesResponseFlags(t *testing.T) {
	tests := []struct {
		name  string
		raw   string
		want  []string
		close int64
	}{
		{name: "dash", raw: "-", want: nil},
		{name: "empty", raw: "", want: nil},
		{name: "other", raw: "UR", want: []string{"UR"}},
		{name: "comma_separated", raw: "DC,UR", want: []string{"DC", "UR"}, close: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			start := fixture(downstreamStart, 1, "request-a", "/")
			end := fixture(downstreamEnd, 1, "request-a", "/")
			end.ResponseFlags = test.raw
			summary, output := combineCapture(t, observationsNDJSON(t, start, end))
			if summary.ConnectionsClosed != test.close {
				t.Errorf("CombineStream(flags %q).ConnectionsClosed = %d, want %d", test.raw, summary.ConnectionsClosed, test.close)
			}
			var events []model.Event
			if err := parser.ParseStream(strings.NewReader(output), func(event model.Event) error {
				events = append(events, event)
				return nil
			}); err != nil {
				t.Fatalf("parser.ParseStream(flags %q) error: %v", test.raw, err)
			}
			if !reflect.DeepEqual(events[0].ResponseFlags, test.want) {
				t.Errorf("flags %q parsed as %v, want %v", test.raw, events[0].ResponseFlags, test.want)
			}
		})
	}
}

func TestCombineStreamEmitsOneCloseForMultipleDCEnds(t *testing.T) {
	startA := fixture(downstreamStart, 1, "a", "/a")
	startB := fixture(downstreamStart, 1, "b", "/b")
	endA := fixture(downstreamEnd, 1, "a", "/a")
	endA.ResponseFlags = "DC"
	endB := fixture(downstreamEnd, 1, "b", "/b")
	endB.ResponseFlags = "DC"
	summary, output := combineCapture(t, observationsNDJSON(t, startA, startB, endA, endB))
	if summary.ConnectionsClosed != 1 || strings.Count(output, `"type":"connection_close"`) != 1 {
		t.Errorf("multiple DC summary/output = %+v / %q, want one close", summary, output)
	}
	if !strings.HasSuffix(strings.TrimSpace(output), `{"type":"connection_close","node":"envoy-a","connection_id":1}`) {
		t.Errorf("close placement output = %q, want close after final request", output)
	}
}

func TestCombineStreamRejectsInvalidPayloads(t *testing.T) {
	validStart := fixture(downstreamStart, 1, "request-a", "/")
	validEnd := fixture(downstreamEnd, 1, "request-a", "/")
	start := observationsNDJSON(t, validStart)
	end := observationsNDJSON(t, validEnd)
	tests := []struct {
		name, input, wantError string
	}{
		{
			name:      "request_header_null_values",
			input:     observationWithRawField(t, validStart, `"headers":{"x-required":null}`) + end,
			wantError: `header "x-required" values must be an array of strings`,
		},
		{
			name:      "request_header_null_element",
			input:     observationWithRawField(t, validStart, `"headers":{"x-required":[null]}`) + end,
			wantError: `header "x-required" value 0 must be a string`,
		},
		{
			name:      "request_body_encoding",
			input:     observationWithRawField(t, validStart, `"body":{"encoding":"plain","content":"YQ==","size_bytes":1}`) + end,
			wantError: `body encoding must be "base64"`,
		},
		{
			name:      "request_body_content",
			input:     observationWithRawField(t, validStart, `"body":{"encoding":"base64","content":"%%%","size_bytes":3}`) + end,
			wantError: "decode body content",
		},
		{
			name:      "request_body_size",
			input:     observationWithRawField(t, validStart, `"body":{"encoding":"base64","content":"YQ==","size_bytes":2}`) + end,
			wantError: "body size_bytes is 2, decoded content is 1 bytes",
		},
		{
			name:      "response_header_null_values",
			input:     start + observationWithRawField(t, validEnd, `"response_headers":{"x-required":null}`),
			wantError: `header "x-required" values must be an array of strings`,
		},
		{
			name:      "response_body_content",
			input:     start + observationWithRawField(t, validEnd, `"response_body":{"encoding":"base64","content":"%%%","size_bytes":3}`),
			wantError: "decode response_body content",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var output bytes.Buffer
			summary, err := CombineStream(strings.NewReader(test.input), &output)
			if err == nil {
				t.Fatalf("CombineStream(%s) error = nil, want error", test.name)
			}
			if !strings.Contains(err.Error(), test.wantError) {
				t.Errorf("CombineStream(%s) error = %q, want substring %q", test.name, err, test.wantError)
			}
			if summary != (CombineSummary{}) {
				t.Errorf("CombineStream(%s) summary = %+v, want zero", test.name, summary)
			}
			if output.Len() != 0 {
				t.Errorf("CombineStream(%s) output = %q, want empty", test.name, output.String())
			}
		})
	}
}

func TestCombineStreamDiscardsUnmatchedObservations(t *testing.T) {
	unmatchedEnd := fixture(downstreamEnd, 1, "unmatched-end", "/unmatched-end")
	unmatchedEnd.ResponseFlags = "DC"
	completeStart := fixture(downstreamStart, 1, "complete", "/complete")
	unmatchedStart := fixture(downstreamStart, 1, "unmatched-start", "/unmatched-start")
	completeEnd := fixture(downstreamEnd, 1, "complete", "/complete")

	summary, output := combineCapture(t, observationsNDJSON(
		t,
		unmatchedEnd,
		completeStart,
		unmatchedStart,
		completeEnd,
	))
	wantSummary := CombineSummary{
		Starts:          2,
		Ends:            2,
		Records:         1,
		DiscardedStarts: 1,
		DiscardedEnds:   1,
	}
	if summary != wantSummary {
		t.Errorf("CombineStream(unmatched observations) summary = %+v, want %+v", summary, wantSummary)
	}
	if got := strings.Count(output, `"type":"request"`); got != 1 {
		t.Errorf("CombineStream(unmatched observations) request count = %d, want 1; output=%q", got, output)
	}
	if !strings.Contains(output, `"request_id":"complete"`) {
		t.Errorf("CombineStream(unmatched observations) output = %q, want complete request", output)
	}
	if strings.Contains(output, "unmatched-") {
		t.Errorf("CombineStream(unmatched observations) output = %q, want unmatched observations discarded", output)
	}
	if strings.Contains(output, `"type":"connection_close"`) {
		t.Errorf("CombineStream(unmatched DC End) output = %q, want no connection close", output)
	}
}

func TestCombineStreamRejectsInvalidObservations(t *testing.T) {
	validStart := fixture(downstreamStart, 1, "request-a", "/")
	validEnd := fixture(downstreamEnd, 1, "request-a", "/")

	missingID := validStart
	missingID.RequestID = "-"
	duplicateStart := observationsNDJSON(t, validStart, validStart, validEnd)
	duplicateEnd := observationsNDJSON(t, validEnd, validEnd, validStart)
	reusedStart := observationsNDJSON(t, validStart, validEnd, validStart)
	mismatchEnd := validEnd
	mismatchEnd.Method = "POST"
	otherConnectionEnd := fixture(downstreamEnd, 2, "request-a", "/")
	emptyFlagEnd := validEnd
	emptyFlagEnd.ResponseFlags = "DC,"
	arrayFlagEnd := validEnd
	arrayFlagEnd.ResponseFlags = []string{"DC"}
	missingFlagEnd := validEnd
	missingFlagEnd.ResponseFlags = nil
	unsupported := validStart
	unsupported.Type = "request"
	missingType := validStart
	missingType.Type = ""
	invalidHTTP11Start := validStart
	invalidHTTP11Start.Protocol = "HTTP/1.1"
	invalidHTTP11Start.StreamID = 2
	invalidHTTP11End := validEnd
	invalidHTTP11End.Protocol = "HTTP/1.1"
	invalidHTTP11End.StreamID = 2

	tests := []struct {
		name  string
		input string
	}{
		{name: "malformed_json", input: "{not json\n"},
		{name: "missing_request_id", input: observationsNDJSON(t, missingID)},
		{name: "duplicate_start", input: duplicateStart},
		{name: "duplicate_end", input: duplicateEnd},
		{name: "reused_start", input: reusedStart},
		{name: "shared_field_mismatch", input: observationsNDJSON(t, validStart, mismatchEnd)},
		{name: "connection_identity_mismatch", input: observationsNDJSON(t, validStart, otherConnectionEnd)},
		{name: "empty_flag_token", input: observationsNDJSON(t, validStart, emptyFlagEnd)},
		{name: "array_flags", input: observationsNDJSON(t, validStart, arrayFlagEnd)},
		{name: "missing_response_flags", input: observationsNDJSON(t, validStart, missingFlagEnd)},
		{name: "unsupported_type", input: observationsNDJSON(t, unsupported)},
		{name: "omitted_type", input: observationsNDJSON(t, missingType)},
		{name: "invalid_http11_stream_id", input: observationsNDJSON(t, invalidHTTP11Start, invalidHTTP11End)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var output bytes.Buffer
			summary, err := CombineStream(strings.NewReader(test.input), &output)
			if err == nil {
				t.Fatalf("CombineStream(%s) error = nil, want error", test.name)
			}
			if summary != (CombineSummary{}) {
				t.Errorf("CombineStream(%s) summary = %+v, want zero", test.name, summary)
			}
			if output.Len() != 0 {
				t.Errorf("CombineStream(%s) output = %q, want empty", test.name, output.String())
			}
			if !strings.Contains(err.Error(), "line ") {
				t.Errorf("CombineStream(%s) error = %q, want line context", test.name, err)
			}
		})
	}
}

type failingWriter struct {
	remaining int
}

func (w *failingWriter) Write(data []byte) (int, error) {
	if w.remaining <= 0 {
		return 0, errors.New("injected output failure")
	}
	if len(data) <= w.remaining {
		w.remaining -= len(data)
		return len(data), nil
	}
	written := w.remaining
	w.remaining = 0
	return written, errors.New("injected output failure")
}

func TestCombineStreamReturnsZeroSummaryOnOutputFailure(t *testing.T) {
	start := fixture(downstreamStart, 1, "request-a", "/")
	end := fixture(downstreamEnd, 1, "request-a", "/")
	summary, err := CombineStream(strings.NewReader(observationsNDJSON(t, start, end)), &failingWriter{remaining: 8})
	if err == nil || !strings.Contains(err.Error(), "write combined request") {
		t.Fatalf("CombineStream(failing writer) error = %v, want write error", err)
	}
	if summary != (CombineSummary{}) {
		t.Errorf("CombineStream(failing writer) summary = %+v, want zero", summary)
	}
}

func TestCombineStreamReturnsZeroSummaryOnSpoolFailure(t *testing.T) {
	missingTempDir := filepath.Join(t.TempDir(), "missing")
	t.Setenv("TMPDIR", missingTempDir)
	start := fixture(downstreamStart, 1, "request-a", "/")
	end := fixture(downstreamEnd, 1, "request-a", "/")
	summary, err := CombineStream(strings.NewReader(observationsNDJSON(t, start, end)), &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "create spool") {
		t.Fatalf("CombineStream(invalid TMPDIR=%q) error = %v, want spool creation error", missingTempDir, err)
	}
	if summary != (CombineSummary{}) {
		t.Errorf("CombineStream(spool failure) summary = %+v, want zero", summary)
	}
	if _, statErr := os.Stat(missingTempDir); !errors.Is(statErr, os.ErrNotExist) {
		t.Errorf("os.Stat(%q) error = %v, want not exist", missingTempDir, statErr)
	}
}

func TestObservationErrorIncludesOriginalLine(t *testing.T) {
	start := fixture(downstreamStart, 1, "request-a", "/")
	start.RequestID = "-"
	input := fmt.Sprintf("process\n\n%s", observationsNDJSON(t, start))
	_, err := CombineStream(strings.NewReader(input), &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "line 3") {
		t.Errorf("CombineStream(process lines) error = %v, want original line 3", err)
	}
}
