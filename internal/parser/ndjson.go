package parser

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/klauspost/compress/zstd"
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

var readers = map[string]func(io.Reader) (io.ReadCloser, error){
	"gzip": func(r io.Reader) (io.ReadCloser, error) {
		return gzip.NewReader(r)
	},
	"zstd": func(r io.Reader) (io.ReadCloser, error) {
		zr, err := zstd.NewReader(r)
		if err != nil {
			return nil, err
		}
		return zr.IOReadCloser(), nil
	},
	"": func(r io.Reader) (io.ReadCloser, error) {
		return io.NopCloser(r), nil
	},
}

// ParseFileStream opens the given path and streams parsed events to handler.
// The handler is invoked for each parsed event and may return an error to
// stop processing early.
func ParseFileStream(path string, format string, handler func(model.Event) error) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open log file: %w", err)
	}
	defer f.Close()

	newReader, ok := readers[format]
	if !ok {
		return fmt.Errorf("unsupported format: %s", format)
	}

	rc, err := newReader(f)
	if err != nil {
		return fmt.Errorf("%s reader: %w", format, err)
	}
	defer rc.Close()

	return ParseStream(rc, handler)
}

// ParseStream reads NDJSON events from r and invokes handler for each parsed event.
// The handler may return an error to stop processing early.
func ParseStream(r io.Reader, handler func(model.Event) error) error {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 1024*1024), 16*1024*1024)

	line := 0
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
		trimmed := bytes.TrimSpace(raw)
		if len(trimmed) == 0 || trimmed[0] != '{' {
			continue
		}

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

		// Optional meta event validation
		if event.Type == model.EventMeta {
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

		var isHTTP11 bool
		if event.Type == model.EventRequest || event.Type == model.EventResponse {
			isHTTP11 = len(event.HTTP.Version) >= 8 && strings.EqualFold(event.HTTP.Version[:8], "HTTP/1.1")
			if isHTTP11 && event.StreamID == 0 {
				event.StreamID = 1
			}
		}

		switch event.Type {
		case model.EventRequest:
			if !hasConnectionID {
				return fmt.Errorf("line %d: request missing connection_id", line)
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
			if isHTTP11 && event.StreamID != 1 {
				return fmt.Errorf("line %d: HTTP/1.1 requests must use stream_id=1", line)
			}

		case model.EventResponse:
			if !hasConnectionID {
				return fmt.Errorf("line %d: response missing connection_id", line)
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
