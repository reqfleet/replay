package recorder

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/reqfleet/replay/internal/model"
	"github.com/reqfleet/replay/internal/parser"
)

const (
	downstreamStart = "DownstreamStart"
	downstreamEnd   = "DownstreamEnd"
)

// CombineSummary describes a successfully combined capture.
type CombineSummary struct {
	Starts            int64
	Ends              int64
	Records           int64
	ConnectionsClosed int64
}

type strictHeaders map[string][]string

func (headers *strictHeaders) UnmarshalJSON(data []byte) error {
	data = bytes.TrimSpace(data)
	if len(data) == 0 || data[0] != '{' {
		return errors.New("headers must be an object mapping names to arrays of strings")
	}
	var raw map[string][]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("decode headers: %w", err)
	}
	decoded := make(strictHeaders, len(raw))
	for name, tokens := range raw {
		if tokens == nil {
			return fmt.Errorf("header %q values must be an array of strings", name)
		}
		values := make([]string, len(tokens))
		for index, token := range tokens {
			token = bytes.TrimSpace(token)
			if len(token) == 0 || token[0] != '"' {
				return fmt.Errorf("header %q value %d must be a string", name, index)
			}
			if err := json.Unmarshal(token, &values[index]); err != nil {
				return fmt.Errorf("decode header %q value %d: %w", name, index, err)
			}
		}
		decoded[name] = values
	}
	*headers = decoded
	return nil
}

type rawObservation struct {
	ResponseFlags json.RawMessage `json:"response_flags"`
	NestedHTTP    json.RawMessage `json:"http"`

	Type                    string `json:"type"`
	Node                    string `json:"node"`
	RequestID               string `json:"request_id"`
	Timestamp               string `json:"timestamp"`
	Method                  string `json:"method"`
	Scheme                  string `json:"scheme"`
	Authority               string `json:"authority"`
	Path                    string `json:"path"`
	Protocol                string `json:"protocol"`
	DownstreamRemoteAddress string `json:"downstream_remote_address"`
	UserAgent               string `json:"user_agent"`
	LegacyLogType           string `json:"log_type"`

	ConnectionID    *int            `json:"connection_id"`
	TLS             *model.TLSInfo  `json:"tls"`
	Headers         strictHeaders   `json:"headers"`
	Body            json.RawMessage `json:"body"`
	ResponseCode    *int            `json:"response_code"`
	ResponseHeaders strictHeaders   `json:"response_headers"`
	ResponseBody    json.RawMessage `json:"response_body"`
	LegacyStartTime *string         `json:"start_time"`
	LegacyStatus    *int            `json:"status"`
	StreamID        int             `json:"stream_id"`
	DurationMS      float64         `json:"duration_ms"`
}

type observation struct {
	Event model.Event `json:"event"`
}

type pairKey struct {
	Connection model.ConnectionKey
	RequestID  string
}

type spoolRef struct {
	Offset int64
	Length int64
}

type pairState struct {
	startSeen bool
	endSeen   bool
	complete  bool
	startLine int
	endLine   int
	startRef  spoolRef
	endRef    spoolRef
	ordinal   int
}

type outputRecord struct {
	key    pairKey
	record spoolRef
}

type spoolCodec struct {
	file *os.File
}

func (s spoolCodec) write(value any) (spoolRef, error) {
	offset, err := s.file.Seek(0, io.SeekEnd)
	if err != nil {
		return spoolRef{}, fmt.Errorf("seek spool for write: %w", err)
	}
	if err := json.NewEncoder(s.file).Encode(value); err != nil {
		return spoolRef{}, fmt.Errorf("encode spool record: %w", err)
	}
	end, err := s.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return spoolRef{}, fmt.Errorf("measure spool record: %w", err)
	}
	return spoolRef{Offset: offset, Length: end - offset}, nil
}

func (s spoolCodec) read(ref spoolRef, value any) error {
	reader := io.NewSectionReader(s.file, ref.Offset, ref.Length)
	if err := json.NewDecoder(reader).Decode(value); err != nil {
		return fmt.Errorf("decode spool record: %w", err)
	}
	return nil
}

func responseFlags(line int, raw json.RawMessage) ([]string, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	valueJSON := bytes.TrimSpace(raw)
	if len(valueJSON) == 0 || valueJSON[0] != '"' {
		return nil, fmt.Errorf("line %d: response_flags must be a string", line)
	}
	var value string
	if err := json.Unmarshal(valueJSON, &value); err != nil {
		return nil, fmt.Errorf("line %d: invalid response_flags: %w", line, err)
	}
	if value == "" || value == "-" {
		return nil, nil
	}
	flags := strings.Split(value, ",")
	for _, flag := range flags {
		if flag == "" {
			return nil, fmt.Errorf("line %d: response_flags contains an empty token", line)
		}
	}
	return flags, nil
}

func decodeBody(line int, field string, raw json.RawMessage) (*model.Body, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || raw[0] != '{' {
		return nil, fmt.Errorf("line %d: %s must be a base64 body object", line, field)
	}
	var body model.Body
	if err := json.Unmarshal(raw, &body); err != nil {
		return nil, fmt.Errorf("line %d: decode %s: %w", line, field, err)
	}
	if body.Encoding != "base64" {
		return nil, fmt.Errorf("line %d: %s encoding must be %q", line, field, "base64")
	}
	decodedSize, err := io.Copy(
		io.Discard,
		base64.NewDecoder(base64.StdEncoding, strings.NewReader(body.Content)),
	)
	if err != nil {
		return nil, fmt.Errorf("line %d: decode %s content: %w", line, field, err)
	}
	if body.SizeBytes != decodedSize {
		return nil, fmt.Errorf(
			"line %d: %s size_bytes is %d, decoded content is %d bytes",
			line,
			field,
			body.SizeBytes,
			decodedSize,
		)
	}
	return &body, nil
}

func normalizeObservation(line int, raw rawObservation) (string, observation, error) {
	if raw.Type != downstreamStart && raw.Type != downstreamEnd {
		return "", observation{}, fmt.Errorf("line %d: unsupported observation type %q", line, raw.Type)
	}
	if len(raw.NestedHTTP) != 0 || raw.LegacyStartTime != nil || raw.LegacyStatus != nil || raw.LegacyLogType != "" {
		return "", observation{}, fmt.Errorf("line %d: observation must use the flat Envoy schema", line)
	}
	if raw.ConnectionID == nil {
		return "", observation{}, fmt.Errorf("line %d: observation missing connection_id", line)
	}
	if raw.RequestID == "" || raw.RequestID == "-" {
		return "", observation{}, fmt.Errorf("line %d: observation missing request_id", line)
	}
	if raw.Timestamp == "" {
		return "", observation{}, fmt.Errorf("line %d: observation missing timestamp", line)
	}
	if _, ok := model.ParseTimestamp(raw.Timestamp); !ok {
		return "", observation{}, fmt.Errorf("line %d: invalid timestamp", line)
	}
	if raw.Method == "" {
		return "", observation{}, fmt.Errorf("line %d: observation missing method", line)
	}
	if raw.Authority == "" {
		return "", observation{}, fmt.Errorf("line %d: observation missing authority", line)
	}
	if raw.Path == "" {
		return "", observation{}, fmt.Errorf("line %d: observation missing path", line)
	}
	if raw.Protocol == "" {
		return "", observation{}, fmt.Errorf("line %d: observation missing protocol", line)
	}
	if len(raw.Protocol) >= 8 && strings.EqualFold(raw.Protocol[:8], "HTTP/1.1") {
		if raw.StreamID != 0 && raw.StreamID != 1 {
			return "", observation{}, fmt.Errorf("line %d: HTTP/1.1 observations must omit stream_id or use stream_id=1", line)
		}
		raw.StreamID = 0
	}
	if raw.Type == downstreamEnd && raw.ResponseCode == nil {
		return "", observation{}, fmt.Errorf("line %d: DownstreamEnd observation missing response_code", line)
	}
	if raw.Type == downstreamEnd && len(raw.ResponseFlags) == 0 {
		return "", observation{}, fmt.Errorf("line %d: DownstreamEnd observation missing response_flags", line)
	}

	flags, err := responseFlags(line, raw.ResponseFlags)
	if err != nil {
		return "", observation{}, err
	}

	body, err := decodeBody(line, "body", raw.Body)
	if err != nil {
		return "", observation{}, err
	}
	responseBody, err := decodeBody(line, "response_body", raw.ResponseBody)
	if err != nil {
		return "", observation{}, err
	}

	return raw.Type, observation{Event: model.Event{
		RequestID:               raw.RequestID,
		ResponseFlags:           flags,
		Node:                    raw.Node,
		ConnectionID:            *raw.ConnectionID,
		Timestamp:               raw.Timestamp,
		Method:                  raw.Method,
		Scheme:                  raw.Scheme,
		Authority:               raw.Authority,
		Path:                    raw.Path,
		Protocol:                raw.Protocol,
		StreamID:                raw.StreamID,
		DownstreamRemoteAddress: raw.DownstreamRemoteAddress,
		UserAgent:               raw.UserAgent,
		TLS:                     raw.TLS,
		Headers:                 map[string][]string(raw.Headers),
		Body:                    body,
		ResponseCode:            raw.ResponseCode,
		DurationMS:              raw.DurationMS,
		ResponseHeaders:         map[string][]string(raw.ResponseHeaders),
		ResponseBody:            responseBody,
	}}, nil
}

func containsHeader(headers map[string][]string, name string) bool {
	for candidate := range headers {
		if strings.EqualFold(candidate, name) {
			return true
		}
	}
	return false
}

func observationKey(event model.Event) pairKey {
	return pairKey{
		Connection: model.ConnectionKey{Node: event.Node, ConnectionID: event.ConnectionID},
		RequestID:  event.RequestID,
	}
}

func sharedFieldConflict(start, end model.Event) string {
	fields := []struct {
		name       string
		start, end any
	}{
		{name: "node", start: start.Node, end: end.Node},
		{name: "connection_id", start: start.ConnectionID, end: end.ConnectionID},
		{name: "request_id", start: start.RequestID, end: end.RequestID},
		{name: "timestamp", start: start.Timestamp, end: end.Timestamp},
		{name: "method", start: start.Method, end: end.Method},
		{name: "authority", start: start.Authority, end: end.Authority},
		{name: "path", start: start.Path, end: end.Path},
		{name: "protocol", start: start.Protocol, end: end.Protocol},
	}
	for _, field := range fields {
		if field.start != field.end {
			return field.name
		}
	}
	if start.Scheme != "" && end.Scheme != "" && start.Scheme != end.Scheme {
		return "scheme"
	}
	if start.StreamID != 0 && end.StreamID != 0 && start.StreamID != end.StreamID {
		return "stream_id"
	}
	return ""
}

func mergeObservations(start observation, startLine int, end observation, endLine int) (model.Event, error) {
	if field := sharedFieldConflict(start.Event, end.Event); field != "" {
		line, otherLine := endLine, startLine
		if startLine > endLine {
			line, otherLine = startLine, endLine
		}
		return model.Event{}, fmt.Errorf("line %d: %s conflicts with paired observation on line %d", line, field, otherLine)
	}

	combined := start.Event
	combined.Type = model.EventRequest
	combined.Sequence = 0
	if combined.Scheme == "" {
		combined.Scheme = end.Event.Scheme
	}
	if combined.StreamID == 0 {
		combined.StreamID = end.Event.StreamID
	}
	if combined.Headers == nil {
		combined.Headers = end.Event.Headers
	}
	userAgent := combined.UserAgent
	if userAgent == "" || userAgent == "-" {
		userAgent = end.Event.UserAgent
	}
	if userAgent == "-" {
		userAgent = ""
	}
	combined.UserAgent = userAgent
	if userAgent != "" && !containsHeader(combined.Headers, "user-agent") {
		if combined.Headers == nil {
			combined.Headers = make(map[string][]string, 1)
		}
		combined.Headers["user-agent"] = []string{userAgent}
	}
	if combined.Body == nil {
		combined.Body = end.Event.Body
	}
	combined.ResponseCode = end.Event.ResponseCode
	combined.DurationMS = end.Event.DurationMS
	combined.ResponseHeaders = end.Event.ResponseHeaders
	combined.ResponseBody = end.Event.ResponseBody
	combined.ResponseFlags = end.Event.ResponseFlags
	return combined, nil
}

func containsExactFlag(flags []string, wanted string) bool {
	for _, flag := range flags {
		if flag == wanted {
			return true
		}
	}
	return false
}

func completePair(spool spoolCodec, state *pairState, output []outputRecord, start observation, end observation) error {
	combined, err := mergeObservations(start, state.startLine, end, state.endLine)
	if err != nil {
		return err
	}
	record, err := spool.write(combined)
	if err != nil {
		return fmt.Errorf("spool combined request %q: %w", combined.RequestID, err)
	}
	output[state.ordinal].record = record
	state.complete = true
	return nil
}

func unmatchedError(order []pairKey, pairs map[pairKey]*pairState) error {
	for _, key := range order {
		state := pairs[key]
		if state.startSeen == state.endSeen {
			continue
		}
		for _, otherKey := range order {
			if otherKey.RequestID != key.RequestID || otherKey.Connection == key.Connection {
				continue
			}
			other := pairs[otherKey]
			if state.startSeen && !state.endSeen && !other.startSeen && other.endSeen {
				return fmt.Errorf("line %d: connection identity for request_id %q conflicts with DownstreamStart on line %d", other.endLine, key.RequestID, state.startLine)
			}
			if state.endSeen && !state.startSeen && other.startSeen && !other.endSeen {
				return fmt.Errorf("line %d: connection identity for request_id %q conflicts with DownstreamEnd on line %d", other.startLine, key.RequestID, state.endLine)
			}
		}
		if state.startSeen {
			return fmt.Errorf("line %d: unmatched DownstreamStart for request_id %q", state.startLine, key.RequestID)
		}
		return fmt.Errorf("line %d: unmatched DownstreamEnd for request_id %q", state.endLine, key.RequestID)
	}
	return nil
}

func combineWithSpool(r io.Reader, w io.Writer, spool spoolCodec) (CombineSummary, error) {
	pairs := make(map[pairKey]*pairState)
	pairOrder := make([]pairKey, 0)
	output := make([]outputRecord, 0)
	closedConnections := make(map[model.ConnectionKey]struct{})
	summary := CombineSummary{}

	err := parser.ScanObjects(r, func(line int, object []byte) error {
		var raw rawObservation
		if err := json.Unmarshal(object, &raw); err != nil {
			return fmt.Errorf("line %d: invalid json: %w", line, err)
		}
		kind, current, err := normalizeObservation(line, raw)
		if err != nil {
			return err
		}
		key := observationKey(current.Event)
		state := pairs[key]
		if state == nil {
			state = &pairState{ordinal: -1}
			pairs[key] = state
			pairOrder = append(pairOrder, key)
		}

		switch kind {
		case downstreamStart:
			if state.startSeen {
				return fmt.Errorf("line %d: duplicate DownstreamStart for request_id %q (first on line %d)", line, key.RequestID, state.startLine)
			}
			state.startSeen = true
			state.startLine = line
			state.ordinal = len(output)
			output = append(output, outputRecord{key: key})
			summary.Starts++
			if state.endSeen {
				var end observation
				if err := spool.read(state.endRef, &end); err != nil {
					return fmt.Errorf("read DownstreamEnd for request_id %q from spool: %w", key.RequestID, err)
				}
				return completePair(spool, state, output, current, end)
			}
			state.startRef, err = spool.write(current)
			if err != nil {
				return fmt.Errorf("spool DownstreamStart for request_id %q: %w", key.RequestID, err)
			}
			return nil

		case downstreamEnd:
			if state.endSeen {
				return fmt.Errorf("line %d: duplicate DownstreamEnd for request_id %q (first on line %d)", line, key.RequestID, state.endLine)
			}
			state.endSeen = true
			state.endLine = line
			summary.Ends++
			if containsExactFlag(current.Event.ResponseFlags, "DC") {
				closedConnections[key.Connection] = struct{}{}
			}
			if state.startSeen {
				var start observation
				if err := spool.read(state.startRef, &start); err != nil {
					return fmt.Errorf("read DownstreamStart for request_id %q from spool: %w", key.RequestID, err)
				}
				return completePair(spool, state, output, start, current)
			}
			state.endRef, err = spool.write(current)
			if err != nil {
				return fmt.Errorf("spool DownstreamEnd for request_id %q: %w", key.RequestID, err)
			}
			return nil
		}
		panic("unreachable observation type")
	})
	if err != nil {
		return CombineSummary{}, err
	}
	if err := unmatchedError(pairOrder, pairs); err != nil {
		return CombineSummary{}, err
	}
	if summary.Starts != summary.Ends || summary.Starts != int64(len(output)) {
		return CombineSummary{}, fmt.Errorf("combine invariant violated: starts=%d ends=%d records=%d", summary.Starts, summary.Ends, len(output))
	}

	lastOrdinal := make(map[model.ConnectionKey]int, len(closedConnections))
	for ordinal, item := range output {
		if _, ok := closedConnections[item.key.Connection]; ok {
			lastOrdinal[item.key.Connection] = ordinal
		}
	}
	encoder := json.NewEncoder(w)
	for ordinal, item := range output {
		if !pairs[item.key].complete {
			return CombineSummary{}, fmt.Errorf("combine invariant violated: request_id %q is incomplete", item.key.RequestID)
		}
		if _, err := io.CopyN(w, io.NewSectionReader(spool.file, item.record.Offset, item.record.Length), item.record.Length); err != nil {
			return CombineSummary{}, fmt.Errorf("write combined request %q: %w", item.key.RequestID, err)
		}
		if last, ok := lastOrdinal[item.key.Connection]; ok && last == ordinal {
			closeEvent := model.Event{
				Type:         model.EventConnectionClose,
				Node:         item.key.Connection.Node,
				ConnectionID: item.key.Connection.ConnectionID,
			}
			if err := encoder.Encode(closeEvent); err != nil {
				return CombineSummary{}, fmt.Errorf("write connection close for %s: %w", item.key.Connection, err)
			}
		}
	}

	summary.Records = int64(len(output))
	summary.ConnectionsClosed = int64(len(closedConnections))
	return summary, nil
}

// CombineStream joins mixed Envoy DownstreamStart and DownstreamEnd observations
// into canonical replay events ordered by the Start observations.
func CombineStream(r io.Reader, w io.Writer) (summary CombineSummary, returnErr error) {
	spoolFile, err := os.CreateTemp("", "replay-combine-*.ndjson")
	if err != nil {
		return CombineSummary{}, fmt.Errorf("create spool: %w", err)
	}
	spoolPath := spoolFile.Name()
	defer func() {
		var cleanupErr error
		if err := spoolFile.Close(); err != nil {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("close spool: %w", err))
		}
		if err := os.Remove(spoolPath); err != nil {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("remove spool: %w", err))
		}
		if cleanupErr != nil {
			summary = CombineSummary{}
			returnErr = errors.Join(returnErr, cleanupErr)
		}
	}()

	summary, err = combineWithSpool(r, w, spoolCodec{file: spoolFile})
	if err != nil {
		return CombineSummary{}, err
	}
	return summary, nil
}
