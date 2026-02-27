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
