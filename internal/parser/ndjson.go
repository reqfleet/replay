package parser

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/reqfleet/replay/internal/model"
)

var allowedCloseReasons = map[string]struct{}{
	"remote_close": {},
	"local_close":  {},
	"timeout":      {},
	"drain":        {},
	"error":        {},
}

// ParseFileStream opens the given path and streams parsed events to handler.
// The handler is invoked for each parsed event and may return an error to
// stop processing early.
func ParseFileStream(path string, handler func(model.Event) error) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open log file: %w", err)
	}
	defer f.Close()
	return ParseStream(f, handler)
}

// ParseStream reads NDJSON events from r and invokes handler for each parsed event.
// The handler may return an error to stop processing early.
func ParseStream(r io.Reader, handler func(model.Event) error) error {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 1024*1024), 16*1024*1024)

	line := 0
	// track last seen sequence per connection for monotonicity
	lastSeq := make(map[string]int)

	for scanner.Scan() {
		line++
		raw := scanner.Bytes()

		// For the first line, validate meta and format_version
		if line == 1 {
			var meta map[string]interface{}
			if err := json.Unmarshal(raw, &meta); err != nil {
				return fmt.Errorf("line %d: invalid json: %w", line, err)
			}
			if t, ok := meta["type"].(string); !ok || t != string(model.EventMeta) {
				return fmt.Errorf("line 1 must be meta event")
			}
			fv, ok := meta["format_version"].(string)
			if !ok || fv == "" {
				return fmt.Errorf("line 1: missing format_version")
			}
			// require major version 1 for now
			parts := strings.SplitN(fv, ".", 2)
			if parts[0] != "1" {
				return fmt.Errorf("unsupported format_version: %s", fv)
			}
		}

		var event model.Event
		if err := json.Unmarshal(raw, &event); err != nil {
			return fmt.Errorf("line %d: invalid json: %w", line, err)
		}

		// Basic timestamp validation when present
		if event.Timestamp != "" {
			if _, ok := model.ParseTimestamp(event.Timestamp); !ok {
				return fmt.Errorf("line %d: invalid timestamp", line)
			}
		}

		switch event.Type {
		case model.EventRequest:
			if event.ConnectionID == "" {
				return fmt.Errorf("line %d: request missing connection_id", line)
			}
			if event.Sequence <= 0 {
				return fmt.Errorf("line %d: request missing sequence", line)
			}
			if event.HTTP.Authority == "" {
				return fmt.Errorf("line %d: request missing http.authority", line)
			}
			if event.HTTP.Path == "" {
				return fmt.Errorf("line %d: request missing http.path", line)
			}
			if strings.Contains(strings.ToUpper(event.HTTP.Version), "HTTP/1.1") && event.StreamID != 1 {
				return fmt.Errorf("line %d: HTTP/1.1 requests must use stream_id=1", line)
			}
			if last, ok := lastSeq[event.ConnectionID]; ok {
				if event.Sequence <= last {
					return fmt.Errorf("line %d: non-monotonic sequence for connection %s", line, event.ConnectionID)
				}
			}
			lastSeq[event.ConnectionID] = event.Sequence

		case model.EventResponse:
			if event.ConnectionID == "" {
				return fmt.Errorf("line %d: response missing connection_id", line)
			}
			if event.Sequence <= 0 {
				return fmt.Errorf("line %d: response missing sequence", line)
			}

		case model.EventConnectionOpen:
			if event.ConnectionID == "" {
				return fmt.Errorf("line %d: connection_open missing connection_id", line)
			}

		case model.EventConnectionClose:
			if event.ConnectionID == "" {
				return fmt.Errorf("line %d: connection_close missing connection_id", line)
			}
			if event.Reason != "" {
				if _, ok := allowedCloseReasons[event.Reason]; !ok {
					return fmt.Errorf("line %d: invalid connection_close reason: %s", line, event.Reason)
				}
			}

		case model.EventMeta:
			// already validated above

		default:
			return fmt.Errorf("line %d: unknown event type: %s", line, event.Type)
		}

		if err := handler(event); err != nil {
			return err
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan ndjson: %w", err)
	}
	return nil
}

// Note: old buffered Parse API removed in favor of streaming ParseStream.

// timestamp parsing moved to internal/model/ for reuse across packages
