package parser

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/reqfleet/replay/internal/model"
)

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
	for scanner.Scan() {
		line++
		raw := scanner.Bytes()
		var event model.Event
		if err := json.Unmarshal(raw, &event); err != nil {
			return nil, fmt.Errorf("line %d: invalid json: %w", line, err)
		}
		if line == 1 && event.Type != model.EventMeta {
			return nil, fmt.Errorf("line 1 must be meta event")
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
