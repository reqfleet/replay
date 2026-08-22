package main

import (
	"encoding/json"
	"slices"
	"testing"
	"time"

	"github.com/reqfleet/replay/internal/model"
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
