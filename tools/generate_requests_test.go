package main

import (
	"bytes"
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/reqfleet/replay/internal/model"
	"github.com/reqfleet/replay/internal/parser"
	"github.com/reqfleet/replay/internal/recorder"
)

func TestParseGeneratedHeader(t *testing.T) {
	tests := []struct {
		input     string
		wantName  string
		wantValue string
	}{
		{input: "X-Test: value", wantName: "x-test", wantValue: "value"},
		{input: "authorization:Bearer token", wantName: "authorization", wantValue: "Bearer token"},
		{input: "x-empty:", wantName: "x-empty", wantValue: ""},
	}
	for _, test := range tests {
		name, value, err := parseGeneratedHeader(test.input)
		if err != nil {
			t.Errorf("parseGeneratedHeader(%q) error: %v", test.input, err)
			continue
		}
		if name != test.wantName || value != test.wantValue {
			t.Errorf("parseGeneratedHeader(%q) = (%q, %q), want (%q, %q)", test.input, name, value, test.wantName, test.wantValue)
		}
	}
}

func TestParseGeneratedHeaderRejectsMalformedValue(t *testing.T) {
	for _, input := range []string{"missing-separator", ": missing-name"} {
		if _, _, err := parseGeneratedHeader(input); err == nil {
			t.Errorf("parseGeneratedHeader(%q) error = nil, want error", input)
		}
	}
}

func TestGeneratedRequestIncludesCanonicalIdentityAndResponse(t *testing.T) {
	request := generatedRequestEvent(7, 3, time.Unix(100, 0).UTC(), generatedRequestOptions{
		authority:   "example.com",
		scheme:      "http",
		port:        "80",
		apiKey:      "key",
		requestPath: "/resource",
		status:      201,
		durationMS:  12.5,
		extraHeaders: map[string][]string{
			"content-type": {"application/json"},
			"x-real-ip":    {"198.51.100.7"},
			"x-trace":      {"one", "two"},
		},
		body: generatedRequestBody("hello"),
	})

	if got, want := request.Type, model.EventRequest; got != want {
		t.Errorf("generatedRequestEvent().Type = %q, want %q", got, want)
	}
	if got, want := request.RequestID, "connection-7-request-3"; got != want {
		t.Errorf("generatedRequestEvent().RequestID = %q, want %q", got, want)
	}
	if request.ResponseCode == nil || *request.ResponseCode != 201 {
		t.Errorf("generatedRequestEvent().ResponseCode = %v, want 201", request.ResponseCode)
	}
	if got, want := request.DurationMS, 12.5; got != want {
		t.Errorf("generatedRequestEvent().DurationMS = %v, want %v", got, want)
	}
	if got, want := request.Headers["x-trace"], []string{"one", "two"}; !slices.Equal(got, want) {
		t.Errorf("generatedRequestEvent().Headers[x-trace] = %v, want %v", got, want)
	}
	if request.Body == nil || request.Body.Content != "aGVsbG8=" || request.Body.SizeBytes != 5 {
		t.Errorf("generatedRequestEvent().Body = %+v, want base64 hello body", request.Body)
	}
}

func TestGeneratedRequestSerializesZeroResponseCode(t *testing.T) {
	request := generatedRequestEvent(1, 1, time.Unix(100, 0).UTC(), generatedRequestOptions{
		authority: "example.com",
		scheme:    "http",
		port:      "80",
		status:    0,
	})
	encoded, err := json.Marshal(request)
	if err != nil {
		t.Fatalf("json.Marshal(generatedRequestEvent()) error: %v", err)
	}
	var fields map[string]any
	if err := json.Unmarshal(encoded, &fields); err != nil {
		t.Fatalf("json.Unmarshal(%q) error: %v", encoded, err)
	}
	if got, ok := fields["response_code"]; !ok || got != float64(0) {
		t.Errorf("serialized response_code = %v, present=%t, want 0 and present", got, ok)
	}
}

func TestGeneratedEventsInterleaveConnectionsByRequestStep(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	events := generatedEvents(2, 2, now, generatedRequestOptions{
		authority:   "example.com",
		scheme:      "http",
		port:        "80",
		requestPath: "/base",
		status:      200,
	})
	if got, want := len(events), 4; got != want {
		t.Fatalf("len(generatedEvents(2, 2)) = %d, want %d", got, want)
	}
	wantConnections := []int{1, 2, 1, 2}
	wantIDs := []string{
		"connection-1-request-1",
		"connection-2-request-1",
		"connection-1-request-2",
		"connection-2-request-2",
	}
	wantPaths := []string{"/base", "/base", "/base/2", "/base/2"}
	for i, event := range events {
		if event.ConnectionID != wantConnections[i] || event.RequestID != wantIDs[i] || event.Path != wantPaths[i] {
			t.Errorf("generatedEvents()[%d] identity = connection %d request_id %q path %q, want connection %d request_id %q path %q",
				i, event.ConnectionID, event.RequestID, event.Path, wantConnections[i], wantIDs[i], wantPaths[i])
		}
		if event.Type != model.EventRequest || event.ResponseCode == nil || *event.ResponseCode != 200 {
			t.Errorf("generatedEvents()[%d] canonical response metadata = type %q response_code %v, want request/200", i, event.Type, event.ResponseCode)
		}
	}
}

func TestGeneratedDownstreamEndsAreDirectReplayInput(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	var observations []generatedObservation
	err := emitGeneratedDownstreamEnds(2, 2, now, generatedRequestOptions{
		authority:   "example.com",
		scheme:      "http",
		port:        "80",
		requestPath: "/base",
		status:      201,
		durationMS:  12.5,
		extraHeaders: map[string][]string{
			"content-type": {"application/json"},
		},
		body: generatedRequestBody("hello"),
	}, func(observation generatedObservation) error {
		observations = append(observations, observation)
		return nil
	})
	if err != nil {
		t.Fatalf("emitGeneratedDownstreamEnds() error: %v", err)
	}
	if got, want := len(observations), 4; got != want {
		t.Fatalf("len(DownstreamEnd observations) = %d, want %d", got, want)
	}

	wantIDs := []string{
		"connection-1-request-1",
		"connection-2-request-1",
		"connection-1-request-2",
		"connection-2-request-2",
	}
	var raw bytes.Buffer
	encoder := json.NewEncoder(&raw)
	for i, observation := range observations {
		if observation.Type != generatedDownstreamEnd ||
			observation.RequestID != wantIDs[i] ||
			observation.Protocol != "HTTP/1.1" ||
			observation.ResponseCode == nil ||
			*observation.ResponseCode != 201 ||
			observation.ResponseFlags != "-" {
			t.Errorf("DownstreamEnd observations[%d] = %+v, want ID %q with HTTP/1.1 response 201 and flags -",
				i, observation, wantIDs[i])
		}
		if err := encoder.Encode(observation); err != nil {
			t.Fatalf("json.Encoder.Encode(%+v) error: %v", observation, err)
		}
	}

	var events []model.Event
	if err := parser.ParseStream(&raw, func(event model.Event) error {
		events = append(events, event)
		return nil
	}); err != nil {
		t.Fatalf("parser.ParseStream(generated DownstreamEnd input) error: %v", err)
	}
	if got, want := len(events), 4; got != want {
		t.Fatalf("len(parsed DownstreamEnd events) = %d, want %d", got, want)
	}
	wantSequences := []int{1, 1, 2, 2}
	for i, event := range events {
		if event.Type != model.EventRequest ||
			event.RequestID != wantIDs[i] ||
			event.Sequence != wantSequences[i] {
			t.Errorf("parsed DownstreamEnd events[%d] = type %q request_id %q sequence %d, want request/%q/%d",
				i, event.Type, event.RequestID, event.Sequence, wantIDs[i], wantSequences[i])
		}
	}
}

func TestGeneratedObservationsReverseCompletionAndCombineInStartOrder(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	var observations []generatedObservation
	err := emitGeneratedObservations(2, 1, now, generatedRequestOptions{
		authority:   "example.com",
		scheme:      "https",
		port:        "443",
		requestPath: "/base",
		status:      200,
		durationMS:  16,
	}, func(observation generatedObservation) error {
		observations = append(observations, observation)
		return nil
	})
	if err != nil {
		t.Fatalf("emitGeneratedObservations() error: %v", err)
	}
	if got, want := len(observations), 4; got != want {
		t.Fatalf("len(observations) = %d, want %d", got, want)
	}

	wantTypes := []string{
		generatedDownstreamStart,
		generatedDownstreamStart,
		generatedDownstreamEnd,
		generatedDownstreamEnd,
	}
	wantIDs := []string{
		"connection-1-request-1",
		"connection-1-request-2",
		"connection-1-request-2",
		"connection-1-request-1",
	}
	for i, observation := range observations {
		if observation.Type != wantTypes[i] || observation.RequestID != wantIDs[i] {
			t.Errorf("observations[%d] = type %q request_id %q, want type %q request_id %q",
				i, observation.Type, observation.RequestID, wantTypes[i], wantIDs[i])
		}
	}
	if observations[2].ResponseFlags != "-" || observations[3].ResponseFlags != "DC" {
		t.Errorf("End response_flags = [%q %q], want [- DC]",
			observations[2].ResponseFlags, observations[3].ResponseFlags)
	}
	completionTime := func(observation generatedObservation) time.Time {
		start, err := time.Parse(time.RFC3339Nano, observation.Timestamp)
		if err != nil {
			t.Fatalf("time.Parse(%q) error: %v", observation.Timestamp, err)
		}
		return start.Add(time.Duration(observation.DurationMS * float64(time.Millisecond)))
	}
	if secondEnd, firstEnd := completionTime(observations[2]), completionTime(observations[3]); !secondEnd.Before(firstEnd) {
		t.Errorf("completion times = request 2 %v, request 1 %v; want request 2 before request 1", secondEnd, firstEnd)
	}

	var raw bytes.Buffer
	encoder := json.NewEncoder(&raw)
	for _, observation := range observations {
		if err := encoder.Encode(observation); err != nil {
			t.Fatalf("json.Encoder.Encode(%+v) error: %v", observation, err)
		}
	}
	var canonical bytes.Buffer
	summary, err := recorder.CombineStream(&raw, &canonical)
	if err != nil {
		t.Fatalf("recorder.CombineStream() error: %v", err)
	}
	if summary.Records != 2 || summary.ConnectionsClosed != 1 {
		t.Errorf("recorder.CombineStream() summary = %+v, want 2 records and 1 closed connection", summary)
	}

	var events []model.Event
	if err := parser.ParseStream(strings.NewReader(canonical.String()), func(event model.Event) error {
		events = append(events, event)
		return nil
	}); err != nil {
		t.Fatalf("parser.ParseStream() error: %v", err)
	}
	if got, want := len(events), 3; got != want {
		t.Fatalf("len(canonical events) = %d, want %d", got, want)
	}
	if events[0].RequestID != "connection-1-request-1" ||
		events[1].RequestID != "connection-1-request-2" ||
		events[2].Type != model.EventConnectionClose {
		t.Errorf("canonical event order = [%q %q %q], want [request-1 request-2 connection_close]",
			events[0].RequestID, events[1].RequestID, events[2].Type)
	}
}
