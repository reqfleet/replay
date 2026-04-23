package model

import "time"

// ParseTimestamp parses RFC3339 / RFC3339Nano timestamps used in NDJSON events.
// Returns parsed time and true on success, zero time and false otherwise.
func ParseTimestamp(raw string) (time.Time, bool) {
	if raw == "" {
		return time.Time{}, false
	}
	if parsed, err := time.Parse(time.RFC3339Nano, raw); err == nil {
		return parsed, true
	}
	parsed, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}, false
	}
	return parsed, true
}
