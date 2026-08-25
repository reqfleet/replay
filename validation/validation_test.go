package validation_test

import (
	"bytes"
	"errors"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/reqfleet/replay/validation"
)

const canonicalRequest = `{"type":"request","request_id":"request-a","node":"envoy-a","connection_id":1,"timestamp":"2026-02-27T03:10:22Z","method":"GET","authority":"example.com","path":"/a","protocol":"HTTP/2","response_code":200,"response_flags":["DC","UR"]}` + "\n"

const directDownstreamEnd = `{"type":"DownstreamEnd","connection_id":1,"timestamp":"2026-02-27T03:10:22Z","method":"GET","authority":"example.com","path":"/","protocol":"HTTP/2","response_code":200,"response_flags":"DC,UR"}` + "\n" +
	`{"connection_id":2,"timestamp":"2026-02-27T03:10:23Z","method":"GET","authority":"example.com","path":"/","protocol":"HTTP/2","response_code":200,"response_flags":"DC,UR"}` + "\n"

func TestValidateStreamSupportedInputs(t *testing.T) {
	var logs bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() {
		slog.SetDefault(previousLogger)
	})

	canonicalZstd := compressZstd(t, []byte(canonicalRequest))
	tests := []struct {
		name   string
		input  []byte
		format validation.InputFormat
	}{
		{
			name:   "canonical_ndjson",
			input:  []byte(canonicalRequest),
			format: validation.FormatNDJSON,
		},
		{
			name:   "canonical_zstd",
			input:  canonicalZstd,
			format: validation.FormatZstd,
		},
		{
			name:   "explicit_and_omitted_downstream_end_type",
			input:  []byte(directDownstreamEnd),
			format: validation.FormatNDJSON,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := validation.ValidateStream(bytes.NewReader(test.input), test.format); err != nil {
				t.Errorf("ValidateStream(%s) error: %v", test.name, err)
			}
		})
	}
	if got := logs.String(); got != "" {
		t.Errorf("ValidateStream() logs = %q, want no logger output", got)
	}
}

func TestValidateStreamRejectsMixedInputFamilies(t *testing.T) {
	input := canonicalRequest + directDownstreamEnd
	err := validation.ValidateStream(strings.NewReader(input), validation.FormatNDJSON)
	if err == nil || !strings.Contains(err.Error(), "cannot mix canonical replay events with DownstreamEnd access logs") {
		t.Errorf("ValidateStream(mixed families) error = %v, want mixed-family error", err)
	}
}

func TestValidateStreamRejectsInvalidBodyEncoding(t *testing.T) {
	canonicalPrefix := strings.TrimSuffix(canonicalRequest, "}\n")
	tests := []struct {
		name   string
		suffix string
		want   string
	}{
		{
			name:   "request_encoding",
			suffix: `,"body":{"encoding":"plain","content":"YQ==","size_bytes":1}`,
			want:   `body encoding must be "base64"`,
		},
		{
			name:   "request_content",
			suffix: `,"body":{"encoding":"base64","content":"%%%","size_bytes":3}`,
			want:   "decode body content",
		},
		{
			name:   "response_encoding",
			suffix: `,"response_body":{"encoding":"plain","content":"YQ==","size_bytes":1}`,
			want:   `response_body encoding must be "base64"`,
		},
		{
			name:   "response_content",
			suffix: `,"response_body":{"encoding":"base64","content":"%%%","size_bytes":3}`,
			want:   "decode response_body content",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := canonicalPrefix + test.suffix + "}\n"
			err := validation.ValidateStream(strings.NewReader(input), validation.FormatNDJSON)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Errorf("ValidateStream(%s) error = %v, want error containing %q", test.name, err, test.want)
			}
		})
	}
}

func TestUnsupportedInputFormatDoesNotRead(t *testing.T) {
	const format = validation.InputFormat(255)
	tests := []struct {
		name string
		call func(io.Reader) error
	}{
		{
			name: "validate",
			call: func(r io.Reader) error {
				return validation.ValidateStream(r, format)
			},
		},
		{
			name: "summarize",
			call: func(r io.Reader) error {
				_, err := validation.SummarizeStream(r, format)
				return err
			},
		},
		{
			name: "summarize_with_sharding",
			call: func(r io.Reader) error {
				_, err := validation.SummarizeStreamWithSharding(r, format, 0, 1)
				return err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reader := &trackingReader{}
			err := test.call(reader)
			const want = "unsupported input format 255"
			if err == nil || err.Error() != want {
				t.Errorf("%s(unsupported format) error = %v, want %q", test.name, err, want)
			}
			if reader.reads != 0 {
				t.Errorf("%s(unsupported format) reads = %d, want 0", test.name, reader.reads)
			}
		})
	}
}

func TestSummarizeStreamWithShardingRejectsInvalidParametersBeforeReading(t *testing.T) {
	tests := []struct {
		name                   string
		shardIndex, shardCount int
		want                   string
	}{
		{
			name:       "zero_shard_count",
			shardCount: 0,
			want:       "invalid shardCount: 0 (must be > 0)",
		},
		{
			name:       "negative_shard_count",
			shardCount: -1,
			want:       "invalid shardCount: -1 (must be > 0)",
		},
		{
			name:       "negative_shard_index",
			shardIndex: -1,
			shardCount: 2,
			want:       "invalid shardIndex: -1 (must be within [0, 2))",
		},
		{
			name:       "shard_index_at_count",
			shardIndex: 2,
			shardCount: 2,
			want:       "invalid shardIndex: 2 (must be within [0, 2))",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reader := &trackingReader{}
			_, err := validation.SummarizeStreamWithSharding(
				reader,
				validation.FormatNDJSON,
				test.shardIndex,
				test.shardCount,
			)
			if err == nil || err.Error() != test.want {
				t.Errorf(
					"SummarizeStreamWithSharding(index=%d, count=%d) error = %v, want %q",
					test.shardIndex,
					test.shardCount,
					err,
					test.want,
				)
			}
			if reader.reads != 0 {
				t.Errorf(
					"SummarizeStreamWithSharding(index=%d, count=%d) reads = %d, want 0",
					test.shardIndex,
					test.shardCount,
					reader.reads,
				)
			}
		})
	}
}

func TestSummarizeStreamWithShardingRejectsShardCountAboveHashSpace(t *testing.T) {
	if strconv.IntSize < 64 {
		t.Skip("int cannot represent a shard count above the FNV-32 hash space")
	}

	const maxShardCount = uint64(1) << 32
	shardCount := int(maxShardCount + 1)
	reader := &trackingReader{}
	_, err := validation.SummarizeStreamWithSharding(reader, validation.FormatNDJSON, 0, shardCount)
	want := "invalid shardCount: 4294967297 (must be <= 4294967296)"
	if err == nil || err.Error() != want {
		t.Errorf("SummarizeStreamWithSharding(index=0, count=%d) error = %v, want %q", shardCount, err, want)
	}
	if reader.reads != 0 {
		t.Errorf("SummarizeStreamWithSharding(index=0, count=%d) reads = %d, want 0", shardCount, reader.reads)
	}
}

func TestSummarizeStreamCountsRequestsAndConnections(t *testing.T) {
	const input = `{"type":"request","request_id":"a-1","node":"envoy-a","connection_id":1,"timestamp":"2026-02-27T03:10:22Z","method":"GET","authority":"example.com","path":"/1","protocol":"HTTP/2","response_code":200}` + "\n" +
		`{"type":"request","request_id":"a-2","node":"envoy-a","connection_id":1,"timestamp":"2026-02-27T03:10:23Z","method":"GET","authority":"example.com","path":"/2","protocol":"HTTP/2","response_code":200}` + "\n" +
		`{"type":"connection_close","node":"envoy-a","connection_id":1}` + "\n" +
		`{"type":"request","request_id":"b-1","node":"envoy-b","connection_id":1,"timestamp":"2026-02-27T03:10:24Z","method":"GET","authority":"example.com","path":"/3","protocol":"HTTP/2","response_code":200}` + "\n" +
		`{"type":"connection_close","node":"envoy-b","connection_id":1}` + "\n" +
		`{"type":"request","request_id":"a-3","node":"envoy-a","connection_id":2,"timestamp":"2026-02-27T03:10:25Z","method":"GET","authority":"example.com","path":"/4","protocol":"HTTP/2","response_code":200}` + "\n"

	got, err := validation.SummarizeStream(strings.NewReader(input), validation.FormatNDJSON)
	if err != nil {
		t.Fatalf("SummarizeStream(multi-node fixture) error: %v", err)
	}
	want := validation.Summary{TotalRequests: 4, TotalConnections: 3}
	if got != want {
		t.Errorf("SummarizeStream(multi-node fixture) = %+v, want %+v", got, want)
	}

	var shardedTotal validation.Summary
	for shardIndex := range 2 {
		shard, err := validation.SummarizeStreamWithSharding(
			strings.NewReader(input),
			validation.FormatNDJSON,
			shardIndex,
			2,
		)
		if err != nil {
			t.Fatalf("SummarizeStreamWithSharding(index=%d, count=2) error: %v", shardIndex, err)
		}
		shardedTotal.TotalRequests += shard.TotalRequests
		shardedTotal.TotalConnections += shard.TotalConnections
	}
	if shardedTotal != got {
		t.Errorf("sum of sharded summaries = %+v, want unsharded summary %+v", shardedTotal, got)
	}
}

func TestSummarizeStreamCountsDirectDownstreamEndConnectionsByNode(t *testing.T) {
	const input = `{"type":"DownstreamEnd","node":"envoy-a","connection_id":7,"timestamp":"2026-02-27T03:10:22Z","method":"GET","authority":"example.com","path":"/1","protocol":"HTTP/2","response_code":200,"response_flags":"-"}` + "\n" +
		`{"node":"envoy-a","connection_id":7,"timestamp":"2026-02-27T03:10:23Z","method":"GET","authority":"example.com","path":"/2","protocol":"HTTP/2","response_code":200,"response_flags":"DC,UR"}` + "\n" +
		`{"node":"envoy-b","connection_id":7,"timestamp":"2026-02-27T03:10:24Z","method":"GET","authority":"example.com","path":"/3","protocol":"HTTP/2","response_code":200,"response_flags":"-"}` + "\n"

	got, err := validation.SummarizeStream(strings.NewReader(input), validation.FormatNDJSON)
	if err != nil {
		t.Fatalf("SummarizeStream(DownstreamEnd fixture) error: %v", err)
	}
	want := validation.Summary{TotalRequests: 3, TotalConnections: 2}
	if got != want {
		t.Errorf("SummarizeStream(DownstreamEnd fixture) = %+v, want %+v", got, want)
	}
}

type trackingReader struct {
	reads int
}

func (r *trackingReader) Read([]byte) (int, error) {
	r.reads++
	return 0, errors.New("unexpected read")
}

func compressZstd(t *testing.T, input []byte) []byte {
	t.Helper()

	var compressed bytes.Buffer
	writer, err := zstd.NewWriter(&compressed)
	if err != nil {
		t.Fatalf("zstd.NewWriter() error: %v", err)
	}
	if _, err := writer.Write(input); err != nil {
		t.Fatalf("zstd writer Write() error: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("zstd writer Close() error: %v", err)
	}
	return compressed.Bytes()
}
