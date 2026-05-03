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
