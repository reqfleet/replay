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

type connectionSequenceState struct {
	nextRequestSequence     int
	lastRequestSequence     int
	requestSequenceByStream map[int]int
}

func (s *connectionSequenceState) recordRequest(streamID, provided int) (int, error) {
	if provided > 0 {
		if s.nextRequestSequence > 0 && provided < s.nextRequestSequence {
			return 0, fmt.Errorf("non-monotonic sequence")
		}
		if provided >= s.nextRequestSequence {
			s.nextRequestSequence = provided + 1
		}
		s.lastRequestSequence = provided
		if streamID > 0 {
			if s.requestSequenceByStream == nil {
				s.requestSequenceByStream = make(map[int]int)
			}
			s.requestSequenceByStream[streamID] = provided
		}
		return provided, nil
	}

	if s.nextRequestSequence == 0 {
		s.nextRequestSequence = 1
	}
	sequence := s.nextRequestSequence
	s.nextRequestSequence++
	s.lastRequestSequence = sequence
	if streamID > 0 {
		if s.requestSequenceByStream == nil {
			s.requestSequenceByStream = make(map[int]int)
		}
		s.requestSequenceByStream[streamID] = sequence
	}
	return sequence, nil
}

func (s *connectionSequenceState) recordResponse(streamID, provided int) (int, error) {
	if provided > 0 {
		return provided, nil
	}
	if streamID > 0 && s.requestSequenceByStream != nil {
		if sequence, ok := s.requestSequenceByStream[streamID]; ok {
			return sequence, nil
		}
	}
	if s.lastRequestSequence > 0 {
		return s.lastRequestSequence, nil
	}
	return 0, fmt.Errorf("response missing sequence")
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
	parsedEvents := 0
	states := make(map[int]*connectionSequenceState)
	stateForConnection := func(connectionID int) *connectionSequenceState {
		state := states[connectionID]
		if state == nil {
			state = &connectionSequenceState{}
			states[connectionID] = state
		}
		return state
	}

	for scanner.Scan() {
		line++
		raw := scanner.Bytes()
		trimmed := strings.TrimSpace(string(raw))
		if trimmed == "" || !strings.HasPrefix(trimmed, "{") {
			continue
		}
		parsedEvents++

		var rawEvent struct {
			model.Event
			ConnectionID *int `json:"connection_id"`
		}
		if err := json.Unmarshal(raw, &rawEvent); err != nil {
			return fmt.Errorf("line %d: invalid json: %w", line, err)
		}
		event := rawEvent.Event
		if rawEvent.ConnectionID != nil {
			event.ConnectionID = *rawEvent.ConnectionID
		}
		hasConnectionID := rawEvent.ConnectionID != nil

		// For the first parsed event, validate meta and format_version
		if parsedEvents == 1 {
			if event.Type != model.EventMeta {
				return fmt.Errorf("line %d: must be meta event", line)
			}
			fv := event.FormatVersion
			if fv == "" {
				return fmt.Errorf("line %d: missing format_version", line)
			}
			// require major version 1 for now
			parts := strings.SplitN(fv, ".", 2)
			if parts[0] != "1" {
				return fmt.Errorf("line %d: unsupported format_version: %s", line, fv)
			}
		}

		// Basic timestamp validation when present
		if event.Timestamp != "" {
			if _, ok := model.ParseTimestamp(event.Timestamp); !ok {
				return fmt.Errorf("line %d: invalid timestamp", line)
			}
		}

		switch event.Type {
		case model.EventRequest:
			if !hasConnectionID {
				return fmt.Errorf("line %d: request missing connection_id", line)
			}
			if strings.Contains(strings.ToUpper(event.HTTP.Version), "HTTP/1.1") && event.StreamID == 0 {
				event.StreamID = 1
			}
			sequence, err := stateForConnection(event.ConnectionID).recordRequest(event.StreamID, event.Sequence)
			if err != nil {
				return fmt.Errorf("line %d: %w", line, err)
			}
			event.Sequence = sequence
			if event.HTTP.Authority == "" {
				return fmt.Errorf("line %d: request missing http.authority", line)
			}
			if event.HTTP.Path == "" {
				return fmt.Errorf("line %d: request missing http.path", line)
			}
			if strings.Contains(strings.ToUpper(event.HTTP.Version), "HTTP/1.1") && event.StreamID != 1 {
				return fmt.Errorf("line %d: HTTP/1.1 requests must use stream_id=1", line)
			}

		case model.EventResponse:
			if !hasConnectionID {
				return fmt.Errorf("line %d: response missing connection_id", line)
			}
			if strings.Contains(strings.ToUpper(event.HTTP.Version), "HTTP/1.1") && event.StreamID == 0 {
				event.StreamID = 1
			}
			sequence, err := stateForConnection(event.ConnectionID).recordResponse(event.StreamID, event.Sequence)
			if err != nil {
				return fmt.Errorf("line %d: %w", line, err)
			}
			event.Sequence = sequence

		case model.EventConnectionOpen:
			if !hasConnectionID {
				return fmt.Errorf("line %d: connection_open missing connection_id", line)
			}

		case model.EventConnectionClose:
			if !hasConnectionID {
				return fmt.Errorf("line %d: connection_close missing connection_id", line)
			}
			if event.Reason != "" {
				if _, ok := allowedCloseReasons[event.Reason]; !ok {
					return fmt.Errorf("line %d: invalid connection_close reason: %s", line, event.Reason)
				}
			}
			delete(states, event.ConnectionID)

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
