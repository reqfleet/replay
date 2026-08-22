package parser

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/klauspost/compress/zstd"
	"github.com/reqfleet/replay/internal/model"
)

const maxNDJSONLineBytes = 16 * 1024 * 1024

type connectionSequenceState struct {
	nextRequestSequence int
}

func (s *connectionSequenceState) recordRequest(provided int) (int, error) {
	if provided > 0 {
		if s.nextRequestSequence > 0 && provided < s.nextRequestSequence {
			return 0, fmt.Errorf("non-monotonic sequence")
		}
		if provided >= s.nextRequestSequence {
			s.nextRequestSequence = provided + 1
		}
		return provided, nil
	}

	if s.nextRequestSequence == 0 {
		s.nextRequestSequence = 1
	}
	sequence := s.nextRequestSequence
	s.nextRequestSequence++
	return sequence, nil
}

type compressedReadCloser struct {
	io.Reader
	compressed io.Closer
	file       *os.File
}

func (r *compressedReadCloser) Close() error {
	return errors.Join(r.compressed.Close(), r.file.Close())
}

// OpenFile opens path using the requested input compression format.
func OpenFile(path, format string) (io.ReadCloser, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open log file: %w", err)
	}

	switch format {
	case "":
		return file, nil
	case "gzip":
		reader, err := gzip.NewReader(file)
		if err != nil {
			_ = file.Close()
			return nil, fmt.Errorf("gzip reader: %w", err)
		}
		return &compressedReadCloser{Reader: reader, compressed: reader, file: file}, nil
	case "zstd":
		reader, err := zstd.NewReader(file)
		if err != nil {
			_ = file.Close()
			return nil, fmt.Errorf("zstd reader: %w", err)
		}
		readCloser := reader.IOReadCloser()
		return &compressedReadCloser{Reader: readCloser, compressed: readCloser, file: file}, nil
	default:
		_ = file.Close()
		return nil, fmt.Errorf("unsupported format: %s", format)
	}
}

// ParseFileStream opens the given path and streams parsed events to handler.
// The handler is invoked for each parsed event and may return an error to
// stop processing early.
func ParseFileStream(path string, format string, handler func(model.Event) error) error {
	reader, err := OpenFile(path, format)
	if err != nil {
		return err
	}
	defer reader.Close()

	return ParseStream(reader, handler)
}

// ScanObjects frames JSON-looking NDJSON objects and passes them to handler.
// Blank lines and lines whose first non-space byte is not an object opener are
// ignored. Object bytes are valid only until handler returns.
func ScanObjects(r io.Reader, handler func(line int, object []byte) error) error {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 1024*1024), maxNDJSONLineBytes)

	line := 0
	for scanner.Scan() {
		line++
		object := scanner.Bytes()
		trimmed := bytes.TrimSpace(object)
		if len(trimmed) == 0 || trimmed[0] != '{' {
			continue
		}
		if err := handler(line, object); err != nil {
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan ndjson: %w", err)
	}
	return nil
}

type canonicalWireEvent struct {
	model.Event
	Type          *model.EventType `json:"type"`
	ConnectionID  *int             `json:"connection_id"`
	ResponseFlags json.RawMessage  `json:"response_flags"`
}

func (raw canonicalWireEvent) event(line int) (model.Event, error) {
	if raw.Type == nil {
		return model.Event{}, fmt.Errorf("line %d: event missing type", line)
	}
	if raw.ConnectionID == nil {
		return model.Event{}, fmt.Errorf("line %d: event missing connection_id", line)
	}

	event := raw.Event
	event.Type = *raw.Type
	event.ConnectionID = *raw.ConnectionID
	if len(raw.ResponseFlags) != 0 {
		flagsJSON := bytes.TrimSpace(raw.ResponseFlags)
		if len(flagsJSON) == 0 || flagsJSON[0] != '[' {
			return model.Event{}, fmt.Errorf("line %d: response_flags must be an array of strings", line)
		}
		var tokens []json.RawMessage
		if err := json.Unmarshal(flagsJSON, &tokens); err != nil {
			return model.Event{}, fmt.Errorf("line %d: response_flags must be an array of strings: %w", line, err)
		}
		event.ResponseFlags = make([]string, len(tokens))
		for index, token := range tokens {
			token = bytes.TrimSpace(token)
			if len(token) == 0 || token[0] != '"' {
				return model.Event{}, fmt.Errorf("line %d: response_flags[%d] must be a string", line, index)
			}
			if err := json.Unmarshal(token, &event.ResponseFlags[index]); err != nil {
				return model.Event{}, fmt.Errorf("line %d: response_flags[%d] must be a string: %w", line, index, err)
			}
		}
	}
	return event, nil
}

func validateRequest(line int, event model.Event) error {
	if event.RequestID == "" {
		return fmt.Errorf("line %d: request missing request_id", line)
	}
	if event.Timestamp == "" {
		return fmt.Errorf("line %d: request missing timestamp", line)
	}
	if _, ok := model.ParseTimestamp(event.Timestamp); !ok {
		return fmt.Errorf("line %d: invalid timestamp", line)
	}
	if event.Method == "" {
		return fmt.Errorf("line %d: request missing method", line)
	}
	if event.Authority == "" {
		return fmt.Errorf("line %d: request missing authority", line)
	}
	if event.Path == "" {
		return fmt.Errorf("line %d: request missing path", line)
	}
	if event.Protocol == "" {
		return fmt.Errorf("line %d: request missing protocol", line)
	}
	if event.ResponseCode == nil {
		return fmt.Errorf("line %d: request missing response_code", line)
	}
	return nil
}

// ParseStream reads canonical NDJSON events from r and invokes handler for each
// parsed event. The handler may return an error to stop processing early.
func ParseStream(r io.Reader, handler func(model.Event) error) error {
	states := make(map[model.ConnectionKey]*connectionSequenceState)
	stateForConnection := func(connectionKey model.ConnectionKey) *connectionSequenceState {
		state := states[connectionKey]
		if state == nil {
			state = &connectionSequenceState{}
			states[connectionKey] = state
		}
		return state
	}

	return ScanObjects(r, func(line int, object []byte) error {
		var raw canonicalWireEvent
		if err := json.Unmarshal(object, &raw); err != nil {
			return fmt.Errorf("line %d: invalid json: %w", line, err)
		}
		event, err := raw.event(line)
		if err != nil {
			return err
		}

		switch event.Type {
		case model.EventConnectionClose:
			return handler(event)
		case model.EventRequest:
			if err := validateRequest(line, event); err != nil {
				return err
			}
		default:
			return fmt.Errorf("line %d: unsupported event type %q", line, event.Type)
		}

		connectionKey := model.ConnectionKey{Node: event.Node, ConnectionID: event.ConnectionID}
		sequence, err := stateForConnection(connectionKey).recordRequest(event.Sequence)
		if err != nil {
			return fmt.Errorf("line %d: %w", line, err)
		}
		event.Sequence = sequence

		isHTTP11 := len(event.Protocol) >= 8 && strings.EqualFold(event.Protocol[:8], "HTTP/1.1")
		if isHTTP11 && event.StreamID == 0 {
			event.StreamID = 1
		}
		if isHTTP11 && event.StreamID != 1 {
			return fmt.Errorf("line %d: HTTP/1.1 requests must use stream_id=1", line)
		}

		return handler(event)
	})
}

// Note: old buffered Parse API removed in favor of streaming ParseStream.

// timestamp parsing moved to internal/model/ for reuse across packages
