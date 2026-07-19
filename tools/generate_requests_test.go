package main

import (
	"testing"
	"time"

	"github.com/reqfleet/replay/internal/model"
)

func TestGeneratedAccessLogType(t *testing.T) {
	tests := []struct {
		input string
		want  model.AccessLogType
	}{
		{input: "downstream-start", want: model.AccessLogTypeDownstreamStart},
		{input: "downstream_start", want: model.AccessLogTypeDownstreamStart},
		{input: "DownstreamStart", want: model.AccessLogTypeDownstreamStart},
		{input: "downstream-end", want: model.AccessLogTypeDownstreamEnd},
		{input: "downstream_end", want: model.AccessLogTypeDownstreamEnd},
		{input: "DownstreamEnd", want: model.AccessLogTypeDownstreamEnd},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := generatedAccessLogType(tt.input)
			if err != nil {
				t.Fatalf("generatedAccessLogType(%q) error: %v", tt.input, err)
			}
			if got != tt.want {
				t.Fatalf("generatedAccessLogType(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestGeneratedAccessLogTypeRejectsUnknown(t *testing.T) {
	if _, err := generatedAccessLogType("sideways"); err == nil {
		t.Fatal("generatedAccessLogType(\"sideways\") error = nil, want error")
	}
}

func TestParseGeneratedHeader(t *testing.T) {
	tests := []struct {
		input     string
		wantName  string
		wantValue string
	}{
		{input: "Content-Type: application/json", wantName: "content-type", wantValue: "application/json"},
		{input: "x-note:value:with:colons", wantName: "x-note", wantValue: "value:with:colons"},
		{input: "x-empty:", wantName: "x-empty", wantValue: ""},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			gotName, gotValue, err := parseGeneratedHeader(tt.input)
			if err != nil {
				t.Fatalf("parseGeneratedHeader(%q) error: %v", tt.input, err)
			}
			if gotName != tt.wantName {
				t.Errorf("parseGeneratedHeader(%q) name = %q, want %q", tt.input, gotName, tt.wantName)
			}
			if gotValue != tt.wantValue {
				t.Errorf("parseGeneratedHeader(%q) value = %q, want %q", tt.input, gotValue, tt.wantValue)
			}
		})
	}
}

func TestParseGeneratedHeaderRejectsMalformedValue(t *testing.T) {
	for _, input := range []string{"missing-colon", ": value"} {
		t.Run(input, func(t *testing.T) {
			if _, _, err := parseGeneratedHeader(input); err == nil {
				t.Errorf("parseGeneratedHeader(%q) error = nil, want error", input)
			}
		})
	}
}

func TestGeneratedDownstreamStartRequestOmitsResponseFields(t *testing.T) {
	req := generatedRequestEvent(model.AccessLogTypeDownstreamStart, 7, time.Unix(100, 0).UTC(), generatedRequestOptions{
		authority:   "example.com",
		scheme:      "http",
		port:        "80",
		apiKey:      "key",
		requestPath: "/resource",
		status:      503,
		durationMS:  16,
		body:        generatedRequestBody("hello"),
	})

	if got, want := req.AccessLogType, model.AccessLogTypeDownstreamStart; got != want {
		t.Fatalf("req.AccessLogType = %q, want %q", got, want)
	}
	if got, want := req.Status, 0; got != want {
		t.Fatalf("req.Status = %d, want %d", got, want)
	}
	if got, want := req.DurationMS, float64(0); got != want {
		t.Fatalf("req.DurationMS = %v, want %v", got, want)
	}
	if got, want := req.HTTP.Scheme, "http"; got != want {
		t.Fatalf("req.HTTP.Scheme = %q, want %q", got, want)
	}
	if req.Body != nil {
		t.Errorf("generatedRequestEvent(DownstreamStart).Body = %+v, want nil", req.Body)
	}
}

func TestGeneratedDownstreamEndRequestIncludesRequestAndResponseFields(t *testing.T) {
	req := generatedRequestEvent(model.AccessLogTypeDownstreamEnd, 7, time.Unix(100, 0).UTC(), generatedRequestOptions{
		authority:   "example.com",
		scheme:      "http",
		port:        "80",
		apiKey:      "key",
		requestPath: "/resource",
		status:      503,
		durationMS:  16,
		extraHeaders: map[string][]string{
			"content-type": {"application/json"},
			"x-real-ip":    {"198.51.100.7"},
			"x-trace":      {"one", "two"},
		},
		body: generatedRequestBody("hello"),
	})

	if got, want := req.AccessLogType, model.AccessLogTypeDownstreamEnd; got != want {
		t.Fatalf("req.AccessLogType = %q, want %q", got, want)
	}
	if got, want := req.Status, 503; got != want {
		t.Fatalf("req.Status = %d, want %d", got, want)
	}
	if got, want := req.DurationMS, float64(16); got != want {
		t.Fatalf("req.DurationMS = %v, want %v", got, want)
	}
	if got, want := req.Headers["content-type"], "application/json"; len(got) != 1 || got[0] != want {
		t.Errorf("generatedRequestEvent(DownstreamEnd).Headers[content-type] = %v, want [%q]", got, want)
	}
	if got, want := req.Headers["x-real-ip"], "198.51.100.7"; len(got) != 1 || got[0] != want {
		t.Errorf("generatedRequestEvent(DownstreamEnd).Headers[x-real-ip] = %v, want [%q]", got, want)
	}
	if got, want := req.Headers["x-trace"], []string{"one", "two"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("generatedRequestEvent(DownstreamEnd).Headers[x-trace] = %v, want %v", got, want)
	}
	if got, want := req.Headers["x-api-key"], "key"; len(got) != 1 || got[0] != want {
		t.Errorf("generatedRequestEvent(DownstreamEnd).Headers[x-api-key] = %v, want [%q]", got, want)
	}
	if req.Body == nil {
		t.Fatal("generatedRequestEvent(DownstreamEnd).Body = nil, want body")
	}
	if got, want := req.Body.Encoding, "base64"; got != want {
		t.Errorf("generatedRequestEvent(DownstreamEnd).Body.Encoding = %q, want %q", got, want)
	}
	if got, want := req.Body.Content, "aGVsbG8="; got != want {
		t.Errorf("generatedRequestEvent(DownstreamEnd).Body.Content = %q, want %q", got, want)
	}
	if got, want := req.Body.SizeBytes, int64(5); got != want {
		t.Errorf("generatedRequestEvent(DownstreamEnd).Body.SizeBytes = %d, want %d", got, want)
	}
}

func TestGeneratedRequestUsesBaseScheme(t *testing.T) {
	req := generatedRequestEvent(model.AccessLogTypeDownstreamStart, 7, time.Unix(100, 0).UTC(), generatedRequestOptions{
		authority:   "example.com",
		scheme:      "https",
		port:        "443",
		apiKey:      "key",
		requestPath: "/resource",
		status:      503,
		durationMS:  16,
	})

	if got, want := req.HTTP.Scheme, "https"; got != want {
		t.Fatalf("req.HTTP.Scheme = %q, want %q", got, want)
	}
}

func TestGeneratedEventsInterleaveConnectionsByRequestStep(t *testing.T) {
	now := time.Date(2026, 6, 29, 3, 11, 15, 0, time.UTC)
	events := generatedEvents(model.AccessLogTypeDownstreamStart, 2, 3, now, generatedRequestOptions{
		authority:   "example.com",
		scheme:      "http",
		port:        "80",
		apiKey:      "key",
		requestPath: "api/v1/resource",
		status:      200,
		durationMS:  16,
	})

	wantTypes := []model.EventType{
		model.EventConnectionOpen,
		model.EventConnectionOpen,
		model.EventConnectionOpen,
		model.EventRequest,
		model.EventRequest,
		model.EventRequest,
		model.EventRequest,
		model.EventRequest,
		model.EventRequest,
		model.EventConnectionClose,
		model.EventConnectionClose,
		model.EventConnectionClose,
	}
	if len(events) != len(wantTypes) {
		t.Fatalf("len(generatedEvents()) = %d, want %d", len(events), len(wantTypes))
	}
	for i, wantType := range wantTypes {
		if got := events[i].Type; got != wantType {
			t.Fatalf("events[%d].Type = %q, want %q", i, got, wantType)
		}
	}

	requestConnectionIDs := []int{}
	requestTimestamps := []string{}
	for _, event := range events {
		if event.Type != model.EventRequest {
			continue
		}
		requestConnectionIDs = append(requestConnectionIDs, event.ConnectionID)
		requestTimestamps = append(requestTimestamps, event.Timestamp)
	}
	wantConnectionIDs := []int{1, 2, 3, 1, 2, 3}
	for i, want := range wantConnectionIDs {
		if got := requestConnectionIDs[i]; got != want {
			t.Fatalf("requestConnectionIDs[%d] = %d, want %d", i, got, want)
		}
	}
	if requestTimestamps[0] != requestTimestamps[1] || requestTimestamps[1] != requestTimestamps[2] {
		t.Fatalf("first request batch timestamps = %v, want identical timestamps", requestTimestamps[:3])
	}
	if requestTimestamps[2] == requestTimestamps[3] {
		t.Fatalf("requestTimestamps[2] = requestTimestamps[3] = %q, want later second batch", requestTimestamps[2])
	}
}
