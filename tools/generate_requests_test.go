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

func TestGeneratedEventsDeclareResponseExpectations(t *testing.T) {
	tests := []struct {
		name    string
		logType model.AccessLogType
		want    model.ResponseExpectationMode
	}{
		{
			name:    "downstream_start",
			logType: model.AccessLogTypeDownstreamStart,
			want:    model.ResponseExpectationsNone,
		},
		{
			name:    "downstream_end",
			logType: model.AccessLogTypeDownstreamEnd,
			want:    model.ResponseExpectationsRequestStatus,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			events := generatedEvents(tt.logType, 1, 1, time.Unix(100, 0).UTC(), "example.com", "http", "80", "key", "/resource", 200, 16)
			if got, want := events[0].FormatVersion, "1.0"; got != want {
				t.Errorf("generatedEvents(%q)[0].FormatVersion = %q, want %q", tt.logType, got, want)
			}
			if got := events[0].ResponseExpectations; got != tt.want {
				t.Errorf("generatedEvents(%q)[0].ResponseExpectations = %q, want %q", tt.logType, got, tt.want)
			}
		})
	}
}

func TestGeneratedDownstreamStartRequestOmitsResponseFields(t *testing.T) {
	req := generatedRequestEvent(model.AccessLogTypeDownstreamStart, 7, time.Unix(100, 0).UTC(), "example.com", "http", "80", "key", "/resource", 503, 16)

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
}

func TestGeneratedDownstreamEndRequestIncludesResponseFields(t *testing.T) {
	req := generatedRequestEvent(model.AccessLogTypeDownstreamEnd, 7, time.Unix(100, 0).UTC(), "example.com", "http", "80", "key", "/resource", 503, 16)

	if got, want := req.AccessLogType, model.AccessLogTypeDownstreamEnd; got != want {
		t.Fatalf("req.AccessLogType = %q, want %q", got, want)
	}
	if got, want := req.Status, 503; got != want {
		t.Fatalf("req.Status = %d, want %d", got, want)
	}
	if got, want := req.DurationMS, float64(16); got != want {
		t.Fatalf("req.DurationMS = %v, want %v", got, want)
	}
}

func TestGeneratedRequestUsesBaseScheme(t *testing.T) {
	req := generatedRequestEvent(model.AccessLogTypeDownstreamStart, 7, time.Unix(100, 0).UTC(), "example.com", "https", "443", "key", "/resource", 503, 16)

	if got, want := req.HTTP.Scheme, "https"; got != want {
		t.Fatalf("req.HTTP.Scheme = %q, want %q", got, want)
	}
}

func TestGeneratedEventsInterleaveConnectionsByRequestStep(t *testing.T) {
	now := time.Date(2026, 6, 29, 3, 11, 15, 0, time.UTC)
	events := generatedEvents(model.AccessLogTypeDownstreamStart, 2, 3, now, "example.com", "http", "80", "key", "api/v1/resource", 200, 16)

	wantTypes := []model.EventType{
		model.EventMeta,
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
