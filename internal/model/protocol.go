package model

import (
	"fmt"
	"strings"
)

const (
	// ProtocolHTTP11 is the canonical HTTP/1.1 protocol name.
	ProtocolHTTP11 = "HTTP/1.1"
	// ProtocolHTTP2 is the canonical HTTP/2 protocol name.
	ProtocolHTTP2 = "HTTP/2.0"
)

// NormalizeProtocol returns the supported HTTP wire version for capture metadata.
// Missing versions are invalid: guessing HTTP/1.1 can silently serialize HTTP/2.
func NormalizeProtocol(protocol string) (string, error) {
	version := strings.TrimSpace(protocol)
	switch {
	case strings.EqualFold(version, ProtocolHTTP11):
		return ProtocolHTTP11, nil
	case strings.EqualFold(version, "HTTP/2"), strings.EqualFold(version, ProtocolHTTP2):
		return ProtocolHTTP2, nil
	default:
		return "", fmt.Errorf("unsupported recorded HTTP protocol %q (expected HTTP/1.1 or HTTP/2)", protocol)
	}
}
