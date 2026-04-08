package parser

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/reqfleet/replay/internal/model"
)

var allowedCloseReasons = map[string]struct{}{
	"remote_close": {},
	"local_close":  {},
	"timeout":      {},
	"drain":        {},
	"error":        {},
}

func ParseFile(path string) ([]model.Event, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open log file: %w", err)
	}
	defer f.Close()
	return Parse(f)
}

func Parse(r io.Reader) ([]model.Event, error) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 1024*1024), 16*1024*1024)

	line := 0
	events := make([]model.Event, 0)
	// track last seen sequence per connection for monotonicity
	lastSeq := make(map[string]int)

	for scanner.Scan() {
		line++
		raw := scanner.Bytes()

		// For the first line, validate meta and format_version
		if line == 1 {
			var meta map[string]interface{}
			if err := json.Unmarshal(raw, &meta); err != nil {
				return nil, fmt.Errorf("line %d: invalid json: %w", line, err)
			}
			if t, ok := meta["type"].(string); !ok || t != string(model.EventMeta) {
				return nil, fmt.Errorf("line 1 must be meta event")
			}
			fv, ok := meta["format_version"].(string)
			if !ok || fv == "" {
				return nil, fmt.Errorf("line 1: missing format_version")
			}
			// require major version 1 for now
			parts := strings.SplitN(fv, ".", 2)
			if parts[0] != "1" {
				return nil, fmt.Errorf("unsupported format_version: %s", fv)
			}
		}

		var event model.Event
		if err := json.Unmarshal(raw, &event); err != nil {
			return nil, fmt.Errorf("line %d: invalid json: %w", line, err)
		}

		// Basic timestamp validation when present
		if event.Timestamp != "" {
			if _, ok := parseTimestamp(event.Timestamp); !ok {
				return nil, fmt.Errorf("line %d: invalid timestamp", line)
			}
		}

		switch event.Type {
		case model.EventRequest:
			if event.ConnectionID == "" {
				return nil, fmt.Errorf("line %d: request missing connection_id", line)
			}
			if event.Sequence <= 0 {
				return nil, fmt.Errorf("line %d: request missing sequence", line)
			}
			if event.HTTP.Authority == "" {
				return nil, fmt.Errorf("line %d: request missing http.authority", line)
			}
			if event.HTTP.Path == "" {
				return nil, fmt.Errorf("line %d: request missing http.path", line)
			}
			if strings.Contains(strings.ToUpper(event.HTTP.Version), "HTTP/1.1") && event.StreamID != 1 {
				return nil, fmt.Errorf("line %d: HTTP/1.1 requests must use stream_id=1", line)
			}
			if last, ok := lastSeq[event.ConnectionID]; ok {
				if event.Sequence <= last {
					return nil, fmt.Errorf("line %d: non-monotonic sequence for connection %s", line, event.ConnectionID)
				}
			}
			lastSeq[event.ConnectionID] = event.Sequence

		case model.EventResponse:
			if event.ConnectionID == "" {
				return nil, fmt.Errorf("line %d: response missing connection_id", line)
			}
			if event.Sequence <= 0 {
				return nil, fmt.Errorf("line %d: response missing sequence", line)
			}

		case model.EventConnectionOpen:
			if event.ConnectionID == "" {
				return nil, fmt.Errorf("line %d: connection_open missing connection_id", line)
			}

		case model.EventConnectionClose:
			if event.ConnectionID == "" {
				return nil, fmt.Errorf("line %d: connection_close missing connection_id", line)
			}
			if event.Reason != "" {
				if _, ok := allowedCloseReasons[event.Reason]; !ok {
					return nil, fmt.Errorf("line %d: invalid connection_close reason: %s", line, event.Reason)
				}
			}

		case model.EventMeta:
			// already validated above

		default:
			return nil, fmt.Errorf("line %d: unknown event type: %s", line, event.Type)
		}

		events = append(events, event)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan ndjson: %w", err)
	}
	if len(events) == 0 {
		return nil, fmt.Errorf("empty log")
	}
	return events, nil
}

func parseTimestamp(raw string) (time.Time, bool) {
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
