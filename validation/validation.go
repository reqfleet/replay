// Package validation validates and summarizes Replay input streams.
package validation

import (
	"errors"
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"
	"github.com/reqfleet/replay/internal/model"
	"github.com/reqfleet/replay/internal/parser"
	"github.com/reqfleet/replay/internal/sharding"
)

// InputFormat identifies a supported Replay input encoding.
type InputFormat uint8

const (
	// FormatNDJSON identifies uncompressed NDJSON input.
	FormatNDJSON InputFormat = iota
	// FormatZstd identifies zstd-compressed NDJSON input.
	FormatZstd
)

// Summary contains request and connection totals for a Replay input stream.
type Summary struct {
	TotalRequests    int64
	TotalConnections int64
}

// ValidateStream validates all records in r.
func ValidateStream(r io.Reader, format InputFormat) error {
	return parseStream(r, format, func(model.Event) error { return nil })
}

// SummarizeStream validates r and returns its unsharded request and connection
// totals.
func SummarizeStream(r io.Reader, format InputFormat) (Summary, error) {
	return summarizeStream(r, format, 0, 1)
}

// SummarizeStreamWithSharding validates r and returns request and connection
// totals assigned to shardIndex among shardCount shards.
func SummarizeStreamWithSharding(r io.Reader, format InputFormat, shardIndex, shardCount int) (Summary, error) {
	if err := validateInputFormat(format); err != nil {
		return Summary{}, err
	}
	if shardCount <= 0 {
		return Summary{}, fmt.Errorf("invalid shardCount: %d (must be > 0)", shardCount)
	}
	if uint64(shardCount) > sharding.MaxShardCount {
		return Summary{}, fmt.Errorf(
			"invalid shardCount: %d (must be <= %d)",
			shardCount,
			sharding.MaxShardCount,
		)
	}
	if shardIndex < 0 || shardIndex >= shardCount {
		return Summary{}, fmt.Errorf("invalid shardIndex: %d (must be within [0, %d))", shardIndex, shardCount)
	}
	return summarizeStream(r, format, shardIndex, shardCount)
}

func summarizeStream(r io.Reader, format InputFormat, shardIndex, shardCount int) (Summary, error) {
	var summary Summary
	connections := make(map[model.ConnectionKey]struct{})
	err := parseStream(r, format, func(event model.Event) error {
		if event.Type != model.EventRequest {
			return nil
		}

		connectionKey := model.ConnectionKey{Node: event.Node, ConnectionID: event.ConnectionID}
		if !sharding.ConnectionBelongsToShard(connectionKey, shardIndex, shardCount) {
			return nil
		}

		summary.TotalRequests++
		if _, ok := connections[connectionKey]; !ok {
			connections[connectionKey] = struct{}{}
			summary.TotalConnections++
		}
		return nil
	})
	if err != nil {
		return Summary{}, err
	}
	return summary, nil
}

func parseStream(r io.Reader, format InputFormat, handler func(model.Event) error) error {
	reader, err := streamReader(r, format)
	if err != nil {
		return err
	}
	return parseSelectedStream(reader, handler)
}

func parseSelectedStream(reader io.ReadCloser, handler func(model.Event) error) error {
	parseErr := parser.ParseStreamWithOptions(reader, parser.StreamOptions{}, handler)
	closeErr := reader.Close()
	if parseErr != nil && closeErr != nil {
		return errors.Join(parseErr, fmt.Errorf("close input: %w", closeErr))
	}
	if parseErr != nil {
		return parseErr
	}
	if closeErr != nil {
		return fmt.Errorf("close input: %w", closeErr)
	}
	return nil
}

func streamReader(r io.Reader, format InputFormat) (io.ReadCloser, error) {
	if err := validateInputFormat(format); err != nil {
		return nil, err
	}

	switch format {
	case FormatNDJSON:
		return io.NopCloser(r), nil
	case FormatZstd:
		decoder, err := zstd.NewReader(r)
		if err != nil {
			return nil, fmt.Errorf("zstd reader: %w", err)
		}
		return decoder.IOReadCloser(), nil
	default:
		return nil, fmt.Errorf("unsupported input format %d", format)
	}
}

func validateInputFormat(format InputFormat) error {
	switch format {
	case FormatNDJSON, FormatZstd:
		return nil
	default:
		return fmt.Errorf("unsupported input format %d", format)
	}
}
