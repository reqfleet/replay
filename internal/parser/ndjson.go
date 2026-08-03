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

type rawEvent struct {
	model.Event
	Type            *model.EventType `json:"type"`
	ConnectionID    *int             `json:"connection_id"`
	NestedHTTP      json.RawMessage  `json:"http"`
	LegacyTimestamp *string          `json:"timestamp"`
	LegacyStatus    *int             `json:"status"`
	LegacyLogType   model.EventType  `json:"log_type"`
}

func (ev rawEvent) normalize(line int) (model.Event, error) {
	eventType := model.AccessLogTypeDownstreamEnd
	if ev.Type != nil {
		eventType = *ev.Type
	}
	switch eventType {
	case model.AccessLogTypeDownstreamStart, model.AccessLogTypeDownstreamEnd:
	default:
		return model.Event{}, fmt.Errorf("line %d: unsupported access log type %q", line, eventType)
	}
	if len(ev.NestedHTTP) != 0 || ev.LegacyTimestamp != nil || ev.LegacyStatus != nil || ev.LegacyLogType != "" {
		return model.Event{}, fmt.Errorf("line %d: access log must use the flat Envoy schema", line)
	}
	if ev.ConnectionID == nil {
		return model.Event{}, fmt.Errorf("line %d: access log missing connection_id", line)
	}
	if ev.StartTime == "" {
		return model.Event{}, fmt.Errorf("line %d: access log missing start_time", line)
	}
	if _, ok := model.ParseTimestamp(ev.StartTime); !ok {
		return model.Event{}, fmt.Errorf("line %d: invalid start_time", line)
	}
	if ev.Method == "" {
		return model.Event{}, fmt.Errorf("line %d: access log missing method", line)
	}
	if ev.Protocol == "" {
		return model.Event{}, fmt.Errorf("line %d: access log missing protocol", line)
	}
	if ev.Authority == "" {
		return model.Event{}, fmt.Errorf("line %d: access log missing authority", line)
	}
	if ev.Path == "" {
		return model.Event{}, fmt.Errorf("line %d: access log missing path", line)
	}
	if eventType == model.AccessLogTypeDownstreamEnd && ev.ResponseCode == nil {
		return model.Event{}, fmt.Errorf("line %d: DownstreamEnd access log missing response_code", line)
	}

	headers := ev.Headers
	if ev.UserAgent != "" && ev.UserAgent != "-" {
		hasUserAgentHeader := false
		for name := range headers {
			if strings.EqualFold(name, "user-agent") {
				hasUserAgentHeader = true
				break
			}
		}
		if !hasUserAgentHeader {
			if headers == nil {
				headers = make(map[string][]string, 1)
			}
			headers["user-agent"] = []string{ev.UserAgent}
		}
	}

	event := ev.Event
	event.Type = eventType
	event.ConnectionID = *ev.ConnectionID
	event.Headers = headers
	return event, nil
}

// ParseStream reads NDJSON events from r and invokes handler for each parsed event.
// The handler may return an error to stop processing early.
func ParseStream(r io.Reader, handler func(model.Event) error) error {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 1024*1024), 16*1024*1024)

	line := 0
	states := make(map[model.ConnectionKey]*connectionSequenceState)
	stateForConnection := func(connectionKey model.ConnectionKey) *connectionSequenceState {
		state := states[connectionKey]
		if state == nil {
			state = &connectionSequenceState{}
			states[connectionKey] = state
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

		var ev rawEvent
		if err := json.Unmarshal(raw, &ev); err != nil {
			return fmt.Errorf("line %d: invalid json: %w", line, err)
		}
		event, err := ev.normalize(line)
		if err != nil {
			return err
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
