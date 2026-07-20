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
	ConnectionID            *int    `json:"connection_id"`
	StartTime               string  `json:"start_time"`
	Method                  string  `json:"method"`
	Path                    string  `json:"path"`
	Protocol                string  `json:"protocol"`
	Authority               string  `json:"authority"`
	ResponseCode            int     `json:"response_code"`
	DurationMillis          float64 `json:"duration_ms"`
	DownstreamRemoteAddress string  `json:"downstream_remote_address"`
	UserAgent               string  `json:"user_agent"`
}

func isMalformedDownstreamStartRequest(event model.Event) bool {
	return event.AccessLogType == model.AccessLogTypeDownstreamStart &&
		(event.HTTP.Authority == "" || event.HTTP.Path == "")
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
		event := ev.Event
		if ev.ConnectionID != nil {
			event.ConnectionID = *ev.ConnectionID
		}
		hasConnectionID := ev.ConnectionID != nil
		if event.Type == "" && ev.StartTime != "" && ev.Method != "" && ev.Path != "" {
			event.Type = model.EventRequest
			event.AccessLogType = model.AccessLogTypeDownstreamEnd
			event.Timestamp = ev.StartTime
			event.Status = ev.ResponseCode
			event.DurationMS = ev.DurationMillis
			event.DownstreamRemoteAddress = ev.DownstreamRemoteAddress
			event.HTTP.Version = ev.Protocol
			event.HTTP.Method = ev.Method
			event.HTTP.Authority = ev.Authority
			event.HTTP.Path = ev.Path
			if ev.UserAgent != "" && ev.UserAgent != "-" {
				event.Headers = map[string][]string{"user-agent": {ev.UserAgent}}
			}
		}
		switch event.Type {
		case model.EventType(model.AccessLogTypeDownstreamStart):
			event.Type = model.EventRequest
			event.AccessLogType = model.AccessLogTypeDownstreamStart
		}

		// Basic timestamp validation when present
		if event.Timestamp != "" {
			if _, ok := model.ParseTimestamp(event.Timestamp); !ok {
				return fmt.Errorf("line %d: invalid timestamp", line)
			}
		}

		var isHTTP11 bool
		if event.Type == model.EventRequest {
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
			if isMalformedDownstreamStartRequest(event) {
				continue
			}
			connectionKey := model.ConnectionKey{Node: event.Node, ConnectionID: event.ConnectionID}
			sequence, err := stateForConnection(connectionKey).recordRequest(event.Sequence)
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

		case model.EventConnectionOpen:
			if !hasConnectionID {
				return fmt.Errorf("line %d: connection_open missing connection_id", line)
			}
			connectionKey := model.ConnectionKey{Node: event.Node, ConnectionID: event.ConnectionID}
			stateForConnection(connectionKey)

		case model.EventConnectionClose:
			if !hasConnectionID {
				return fmt.Errorf("line %d: connection_close missing connection_id", line)
			}
			if event.Reason != "" {
				if _, ok := allowedCloseReasons[event.Reason]; !ok {
					return fmt.Errorf("line %d: invalid connection_close reason: %s", line, event.Reason)
				}
			}
			connectionKey := model.ConnectionKey{Node: event.Node, ConnectionID: event.ConnectionID}
			delete(states, connectionKey)

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
