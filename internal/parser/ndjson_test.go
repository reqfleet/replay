package parser

import (
	"strings"
	"testing"
)

func TestParseSuccess(t *testing.T) {
	input := strings.NewReader("{" +
		`"type":"meta","format_version":"1.0"` +
		"}\n" +
		"{" +
		`"type":"request","connection_id":"c1","sequence":1,"http":{"method":"GET","scheme":"http","authority":"example.com","path":"/"}` +
		"}\n")

	events, err := Parse(input)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}
}

func TestParseRejectsNoMetaFirst(t *testing.T) {
	input := strings.NewReader("{" +
		`"type":"request","connection_id":"c1"` +
		"}\n")

	_, err := Parse(input)
	if err == nil {
		t.Fatal("expected error when first line is not meta")
	}
}

func TestParseRejectsUnsupportedFormatVersion(t *testing.T) {
	input := strings.NewReader("{" +
		`"type":"meta","format_version":"2.0"` +
		"}\n" +
		"{" +
		`"type":"request","connection_id":"c1","sequence":1,"http":{"method":"GET","scheme":"http","authority":"example.com","path":"/"}` +
		"}\n")

	_, err := Parse(input)
	if err == nil {
		t.Fatal("expected error for unsupported format_version")
	}
}

func TestParseRejectsHTTP11StreamIDNotOne(t *testing.T) {
	input := strings.NewReader("{" +
		`"type":"meta","format_version":"1.0"` +
		"}\n" +
		"{" +
		`"type":"request","connection_id":"c1","sequence":1,"stream_id":2,"http":{"version":"HTTP/1.1","method":"GET","scheme":"http","authority":"example.com","path":"/"}` +
		"}\n")

	_, err := Parse(input)
	if err == nil {
		t.Fatal("expected error for HTTP/1.1 with stream_id != 1")
	}
}

func TestParseRejectsNonMonotonicSequence(t *testing.T) {
	input := strings.NewReader("{" +
		`"type":"meta","format_version":"1.0"` +
		"}\n" +
		"{" +
		`"type":"request","connection_id":"c1","sequence":2,"http":{"method":"GET","scheme":"http","authority":"example.com","path":"/a"}` +
		"}\n" +
		"{" +
		`"type":"request","connection_id":"c1","sequence":1,"http":{"method":"GET","scheme":"http","authority":"example.com","path":"/b"}` +
		"}\n")

	_, err := Parse(input)
	if err == nil {
		t.Fatal("expected error for non-monotonic sequence")
	}
}

func TestParseRejectsMalformedTimestamp(t *testing.T) {
	input := strings.NewReader("{" +
		`"type":"meta","format_version":"1.0"` +
		"}\n" +
		"{" +
		`"type":"request","connection_id":"c1","sequence":1,"timestamp":"not-a-time","http":{"method":"GET","scheme":"http","authority":"example.com","path":"/"}` +
		"}\n")

	_, err := Parse(input)
	if err == nil {
		t.Fatal("expected error for malformed timestamp")
	}
}
