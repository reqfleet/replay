package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/reqfleet/replay/internal/config"
	"github.com/reqfleet/replay/internal/engine"
	"github.com/reqfleet/replay/internal/metrics"
	"github.com/reqfleet/replay/internal/parser"
)

func mixedCapture() string {
	return strings.Join([]string{
		`{"type":"DownstreamStart","node":"envoy-a","connection_id":7,"request_id":"request-a","timestamp":"2026-08-21T10:00:00Z","method":"GET","scheme":"https","authority":"example.test","path":"/a","protocol":"HTTP/2","stream_id":1}`,
		`{"type":"DownstreamStart","node":"envoy-a","connection_id":7,"request_id":"request-b","timestamp":"2026-08-21T10:00:00.010Z","method":"GET","scheme":"https","authority":"example.test","path":"/b","protocol":"HTTP/2","stream_id":3}`,
		`{"type":"DownstreamEnd","node":"envoy-a","connection_id":7,"request_id":"request-b","timestamp":"2026-08-21T10:00:00.010Z","method":"GET","scheme":"https","authority":"example.test","path":"/b","protocol":"HTTP/2","stream_id":3,"response_code":202,"duration_ms":20,"response_flags":"-"}`,
		`{"type":"DownstreamEnd","node":"envoy-a","connection_id":7,"request_id":"request-a","timestamp":"2026-08-21T10:00:00Z","method":"GET","scheme":"https","authority":"example.test","path":"/a","protocol":"HTTP/2","stream_id":1,"response_code":201,"duration_ms":100,"response_flags":"DC"}`,
	}, "\n") + "\n"
}

func TestRunCombineHelp(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if got, want := runCombine([]string{"-help"}, &stdout, &stderr), 0; got != want {
		t.Errorf("runCombine(-help) = %d, want %d", got, want)
	}
	if !strings.Contains(stdout.String(), "usage: replay combine") {
		t.Errorf("runCombine(-help) stdout = %q, want usage", stdout.String())
	}
	if stderr.Len() != 0 {
		t.Errorf("runCombine(-help) stderr = %q, want empty", stderr.String())
	}
}

func TestRunCombineRejectsInvalidArguments(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{name: "missing_log", args: nil},
		{name: "missing_out", args: []string{"-log", "input.ndjson"}},
		{name: "compression_conflict", args: []string{"-log", "input.ndjson", "-out", "output.ndjson", "-gzip", "-zstd"}},
		{name: "trailing_argument", args: []string{"-log", "input.ndjson", "-out", "output.ndjson", "extra"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			if got, want := runCombine(test.args, &stdout, &stderr), 2; got != want {
				t.Errorf("runCombine(%v) = %d, want %d", test.args, got, want)
			}
			if stdout.Len() != 0 {
				t.Errorf("runCombine(%v) stdout = %q, want empty", test.args, stdout.String())
			}
			if got := strings.Count(stderr.String(), "\n"); got != 1 || !strings.HasPrefix(stderr.String(), "combine: ") {
				t.Errorf("runCombine(%v) stderr = %q, want one combine line", test.args, stderr.String())
			}
		})
	}
}

func TestRunCombineRejectsSameInputAndOutput(t *testing.T) {
	path := filepath.Join(t.TempDir(), "capture.ndjson")
	if err := os.WriteFile(path, []byte(mixedCapture()), 0o600); err != nil {
		t.Fatalf("os.WriteFile(%q) error: %v", path, err)
	}
	var stdout, stderr bytes.Buffer
	if got, want := runCombine([]string{"-log", path, "-out", path}, &stdout, &stderr), 2; got != want {
		t.Errorf("runCombine(same path) = %d, want %d", got, want)
	}
	if !strings.Contains(stderr.String(), "input and output paths are identical") {
		t.Errorf("runCombine(same path) stderr = %q, want identical-path error", stderr.String())
	}
	if got := string(mustReadFile(t, path)); got != mixedCapture() {
		t.Errorf("same input/output contents changed to %q", got)
	}
}

func TestRunCombineInstallsCanonicalOutputAtomically(t *testing.T) {
	dir := t.TempDir()
	inputPath := filepath.Join(dir, "mixed.ndjson")
	outputPath := filepath.Join(dir, "canonical.ndjson")
	if err := os.WriteFile(inputPath, []byte(mixedCapture()), 0o600); err != nil {
		t.Fatalf("os.WriteFile(%q) error: %v", inputPath, err)
	}
	if err := os.WriteFile(outputPath, []byte("old output\n"), 0o644); err != nil {
		t.Fatalf("os.WriteFile(%q) error: %v", outputPath, err)
	}

	var stdout, stderr bytes.Buffer
	if got, want := runCombine([]string{"-log", inputPath, "-out", outputPath}, &stdout, &stderr), 0; got != want {
		t.Fatalf("runCombine(success) = %d, want %d; stderr=%q", got, want, stderr.String())
	}
	wantSummary := "combine complete starts=2 ends=2 records=2 connections_closed=1\n"
	if stdout.String() != wantSummary {
		t.Errorf("runCombine(success) stdout = %q, want %q", stdout.String(), wantSummary)
	}
	if stderr.Len() != 0 {
		t.Errorf("runCombine(success) stderr = %q, want empty", stderr.String())
	}
	output := string(mustReadFile(t, outputPath))
	if strings.Count(output, `"type":"request"`) != 2 || strings.Count(output, `"type":"connection_close"`) != 1 {
		t.Errorf("canonical output = %q, want two requests and one close", output)
	}
	if strings.Contains(output, `"sequence"`) {
		t.Errorf("canonical output = %q, want no serialized sequence", output)
	}
	info, err := os.Stat(outputPath)
	if err != nil {
		t.Fatalf("os.Stat(%q) error: %v", outputPath, err)
	}
	if got, want := info.Mode().Perm(), os.FileMode(0o600); got != want {
		t.Errorf("canonical output mode = %o, want %o", got, want)
	}
	matches, err := filepath.Glob(filepath.Join(dir, ".replay-combine-*"))
	if err != nil {
		t.Fatalf("filepath.Glob(temporary outputs) error: %v", err)
	}
	if len(matches) != 0 {
		t.Errorf("temporary outputs after success = %v, want none", matches)
	}
}

func TestRunCombineWarnsWhenDiscardingUnmatchedObservations(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"DownstreamEnd","node":"envoy-a","connection_id":7,"request_id":"unmatched-end","timestamp":"2026-08-21T10:00:00Z","method":"GET","scheme":"https","authority":"example.test","path":"/end","protocol":"HTTP/2","stream_id":1,"response_code":200,"response_flags":"DC"}`,
		`{"type":"DownstreamStart","node":"envoy-a","connection_id":7,"request_id":"unmatched-start","timestamp":"2026-08-21T10:00:00Z","method":"GET","scheme":"https","authority":"example.test","path":"/start","protocol":"HTTP/2","stream_id":3}`,
	}, "\n") + "\n"
	dir := t.TempDir()
	inputPath := filepath.Join(dir, "mixed.ndjson")
	outputPath := filepath.Join(dir, "canonical.ndjson")
	if err := os.WriteFile(inputPath, []byte(input), 0o600); err != nil {
		t.Fatalf("os.WriteFile(%q) error: %v", inputPath, err)
	}

	var stdout, stderr bytes.Buffer
	if got, want := runCombine([]string{"-log", inputPath, "-out", outputPath}, &stdout, &stderr), 0; got != want {
		t.Fatalf("runCombine(unmatched observations) = %d, want %d; stderr=%q", got, want, stderr.String())
	}
	wantStdout := "combine complete starts=1 ends=1 records=0 connections_closed=0\n"
	if stdout.String() != wantStdout {
		t.Errorf("runCombine(unmatched observations) stdout = %q, want %q", stdout.String(), wantStdout)
	}
	wantStderr := "combine: warning: discarded unmatched observations starts=1 ends=1\n"
	if stderr.String() != wantStderr {
		t.Errorf("runCombine(unmatched observations) stderr = %q, want %q", stderr.String(), wantStderr)
	}
	if output := mustReadFile(t, outputPath); len(output) != 0 {
		t.Errorf("runCombine(unmatched observations) output = %q, want empty", output)
	}
}

func TestRunCombineSupportsLongOutputBasename(t *testing.T) {
	dir := t.TempDir()
	inputPath := filepath.Join(dir, "mixed.ndjson")
	outputPath := filepath.Join(dir, strings.Repeat("a", 240))
	if err := os.WriteFile(inputPath, []byte(mixedCapture()), 0o600); err != nil {
		t.Fatalf("os.WriteFile(%q) error: %v", inputPath, err)
	}

	var stdout, stderr bytes.Buffer
	args := []string{"-log", inputPath, "-out", outputPath}
	if got, want := runCombine(args, &stdout, &stderr), 0; got != want {
		t.Fatalf("runCombine(long output basename) = %d, want %d; stderr=%q", got, want, stderr.String())
	}
	if got := string(mustReadFile(t, outputPath)); !strings.Contains(got, `"type":"request"`) {
		t.Errorf("runCombine(long output basename) output = %q, want canonical request", got)
	}
}

func TestCombineFilesCancellationWhileOpeningInput(t *testing.T) {
	tests := []struct {
		name       string
		format     string
		openWriter bool
	}{
		{name: "fifo_without_writer"},
		{name: "gzip_without_header", format: "gzip", openWriter: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			inputPath := filepath.Join(dir, "mixed.fifo")
			outputPath := filepath.Join(dir, "canonical.ndjson")
			if err := syscall.Mkfifo(inputPath, 0o600); err != nil {
				t.Fatalf("syscall.Mkfifo(%q) error: %v", inputPath, err)
			}

			var holder *os.File
			if test.openWriter {
				var err error
				holder, err = os.OpenFile(inputPath, os.O_RDWR, 0o600)
				if err != nil {
					t.Fatalf("os.OpenFile(%q) error: %v", inputPath, err)
				}
			}

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			result := make(chan error, 1)
			go func() {
				_, err := combineFiles(ctx, inputPath, outputPath, test.format)
				result <- err
			}()

			select {
			case err := <-result:
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Errorf("combineFiles(%s) error = %v, want context.DeadlineExceeded", test.name, err)
				}
			case <-time.After(2 * time.Second):
				t.Errorf("combineFiles(%s) did not return within 2s after cancellation", test.name)
			}

			if holder == nil {
				var err error
				holder, err = os.OpenFile(inputPath, os.O_RDWR, 0o600)
				if err != nil {
					t.Fatalf("os.OpenFile(%q) error: %v", inputPath, err)
				}
			}
			if err := holder.Close(); err != nil {
				t.Errorf("holder.Close() error: %v", err)
			}
			if _, err := os.Stat(outputPath); !errors.Is(err, os.ErrNotExist) {
				t.Errorf("os.Stat(%q) error = %v, want os.ErrNotExist", outputPath, err)
			}
		})
	}
}

func TestCombineFilesCancellationCleansTemporaryFiles(t *testing.T) {
	tests := []struct {
		name, format string
		inputPrefix  []byte
	}{
		{name: "plain"},
		{
			name:        "gzip",
			format:      "gzip",
			inputPrefix: []byte{0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
		},
		{name: "zstd", format: "zstd"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			t.Setenv("TMPDIR", dir)
			inputPath := filepath.Join(dir, "mixed.fifo")
			outputPath := filepath.Join(dir, "canonical.ndjson")
			if err := syscall.Mkfifo(inputPath, 0o600); err != nil {
				t.Fatalf("syscall.Mkfifo(%q) error: %v", inputPath, err)
			}
			holder, err := os.OpenFile(inputPath, os.O_RDWR, 0o600)
			if err != nil {
				t.Fatalf("os.OpenFile(%q) error: %v", inputPath, err)
			}
			if len(test.inputPrefix) > 0 {
				if _, err := holder.Write(test.inputPrefix); err != nil {
					t.Fatalf("holder.Write(%s prefix) error: %v", test.name, err)
				}
			}

			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(func() {
				cancel()
				if err := holder.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
					t.Errorf("holder.Close() error: %v", err)
				}
			})
			result := make(chan error, 1)
			go func() {
				_, err := combineFiles(ctx, inputPath, outputPath, test.format)
				result <- err
			}()

			deadline := time.Now().Add(5 * time.Second)
			for {
				entries, err := os.ReadDir(dir)
				if err != nil {
					t.Fatalf("os.ReadDir(%q) error: %v", dir, err)
				}
				var outputTemporary, spool bool
				for _, entry := range entries {
					outputTemporary = outputTemporary || strings.HasPrefix(entry.Name(), ".replay-combine-")
					spool = spool || strings.HasPrefix(entry.Name(), "replay-combine-")
				}
				if outputTemporary && spool {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("combineFiles(%s) temporary files not created within 5s; entries=%v", test.name, entries)
				}
				time.Sleep(10 * time.Millisecond)
			}

			cancel()
			select {
			case err := <-result:
				if !errors.Is(err, context.Canceled) {
					t.Errorf("combineFiles(%s) error = %v, want context.Canceled", test.name, err)
				}
			case <-time.After(5 * time.Second):
				t.Fatalf("combineFiles(%s) did not return within 5s after cancellation", test.name)
			}

			entries, err := os.ReadDir(dir)
			if err != nil {
				t.Fatalf("os.ReadDir(%q) error: %v", dir, err)
			}
			var leftovers []string
			for _, entry := range entries {
				if entry.Name() != filepath.Base(inputPath) {
					leftovers = append(leftovers, entry.Name())
				}
			}
			if len(leftovers) != 0 {
				t.Errorf("combineFiles(%s) files after cancellation = %v, want only %q", test.name, leftovers, filepath.Base(inputPath))
			}
		})
	}
}

func TestRunCombineErrorPreservesExistingOutput(t *testing.T) {
	invalidHTTP11StreamID := strings.Join([]string{
		`{"type":"DownstreamStart","node":"envoy-a","connection_id":7,"request_id":"request-a","timestamp":"2026-08-21T10:00:00Z","method":"GET","scheme":"https","authority":"example.test","path":"/","protocol":"HTTP/1.1","stream_id":2}`,
		`{"type":"DownstreamEnd","node":"envoy-a","connection_id":7,"request_id":"request-a","timestamp":"2026-08-21T10:00:00Z","method":"GET","scheme":"https","authority":"example.test","path":"/","protocol":"HTTP/1.1","stream_id":2,"response_code":200,"response_flags":"-"}`,
	}, "\n") + "\n"
	start := strings.Split(mixedCapture(), "\n")[0]
	duplicateStart := start + "\n" + start + "\n"
	tests := []struct {
		name, input, wantError string
	}{
		{name: "duplicate_start", input: duplicateStart, wantError: "duplicate DownstreamStart"},
		{name: "invalid_http11_stream_id", input: invalidHTTP11StreamID, wantError: "HTTP/1.1 observations must omit stream_id"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			inputPath := filepath.Join(dir, "input.ndjson")
			outputPath := filepath.Join(dir, "canonical.ndjson")
			if err := os.WriteFile(inputPath, []byte(test.input), 0o600); err != nil {
				t.Fatalf("os.WriteFile(%q) error: %v", inputPath, err)
			}
			if err := os.WriteFile(outputPath, []byte("existing output\n"), 0o600); err != nil {
				t.Fatalf("os.WriteFile(%q) error: %v", outputPath, err)
			}

			var stdout, stderr bytes.Buffer
			if got, want := runCombine([]string{"-log", inputPath, "-out", outputPath}, &stdout, &stderr), 2; got != want {
				t.Errorf("runCombine(%s) = %d, want %d", test.name, got, want)
			}
			if got, want := string(mustReadFile(t, outputPath)), "existing output\n"; got != want {
				t.Errorf("output after %s failure = %q, want %q", test.name, got, want)
			}
			if !strings.Contains(stderr.String(), test.wantError) {
				t.Errorf("runCombine(%s) stderr = %q, want %q", test.name, stderr.String(), test.wantError)
			}
		})
	}
}

func TestRunCombineRejectsCanonicalRecordOverParserLimit(t *testing.T) {
	bodyContent := strings.Repeat("AAAA", parser.MaxNDJSONLineBytes/8)
	decodedBodyBytes := len(bodyContent) / 4 * 3
	start := fmt.Sprintf(
		`{"type":"DownstreamStart","node":"envoy-a","connection_id":7,"request_id":"request-a","timestamp":"2026-08-21T10:00:00Z","method":"POST","scheme":"https","authority":"example.test","path":"/","protocol":"HTTP/2","stream_id":1,"body":{"encoding":"base64","content":"%s","size_bytes":%d}}`,
		bodyContent,
		decodedBodyBytes,
	)
	end := fmt.Sprintf(
		`{"type":"DownstreamEnd","node":"envoy-a","connection_id":7,"request_id":"request-a","timestamp":"2026-08-21T10:00:00Z","method":"POST","scheme":"https","authority":"example.test","path":"/","protocol":"HTTP/2","stream_id":1,"response_code":200,"response_flags":"-","response_body":{"encoding":"base64","content":"%s","size_bytes":%d}}`,
		bodyContent,
		decodedBodyBytes,
	)
	for name, line := range map[string]string{"start": start, "end": end} {
		if got := len(line) + 1; got > parser.MaxNDJSONLineBytes {
			t.Fatalf("%s observation line size = %d, want at most %d", name, got, parser.MaxNDJSONLineBytes)
		}
	}

	dir := t.TempDir()
	inputPath := filepath.Join(dir, "input.ndjson")
	outputPath := filepath.Join(dir, "canonical.ndjson")
	if err := os.WriteFile(inputPath, []byte(start+"\n"+end+"\n"), 0o600); err != nil {
		t.Fatalf("os.WriteFile(%q) error: %v", inputPath, err)
	}
	const existingOutput = "existing output\n"
	if err := os.WriteFile(outputPath, []byte(existingOutput), 0o600); err != nil {
		t.Fatalf("os.WriteFile(%q) error: %v", outputPath, err)
	}

	var stdout, stderr bytes.Buffer
	if got, want := runCombine([]string{"-log", inputPath, "-out", outputPath}, &stdout, &stderr), 2; got != want {
		t.Errorf("runCombine(oversized canonical record) = %d, want %d", got, want)
	}
	if stdout.Len() != 0 {
		t.Errorf("runCombine(oversized canonical record) stdout = %q, want empty", stdout.String())
	}
	wantError := fmt.Sprintf("exceeds %d-byte canonical record limit", parser.MaxNDJSONLineBytes)
	if !strings.Contains(stderr.String(), wantError) {
		t.Errorf("runCombine(oversized canonical record) stderr = %q, want %q", stderr.String(), wantError)
	}
	if got := mustReadFile(t, outputPath); !bytes.Equal(got, []byte(existingOutput)) {
		t.Errorf("output after oversized canonical record changed: size=%d, want preserved %q", len(got), existingOutput)
	}
}

func TestRunCombineCompressedInput(t *testing.T) {
	tests := []struct {
		name   string
		flag   string
		encode func(*testing.T, []byte) []byte
	}{
		{name: "gzip", flag: "-gzip", encode: gzipCapture},
		{name: "zstd", flag: "-zstd", encode: zstdCapture},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			inputPath := filepath.Join(dir, "mixed.bin")
			outputPath := filepath.Join(dir, "canonical.ndjson")
			if err := os.WriteFile(inputPath, test.encode(t, []byte(mixedCapture())), 0o600); err != nil {
				t.Fatalf("os.WriteFile(%q) error: %v", inputPath, err)
			}
			var stdout, stderr bytes.Buffer
			args := []string{"-log", inputPath, "-out", outputPath, test.flag}
			if got, want := runCombine(args, &stdout, &stderr), 0; got != want {
				t.Fatalf("runCombine(%s input) = %d, want %d; stderr=%q", test.name, got, want, stderr.String())
			}
			if !strings.Contains(string(mustReadFile(t, outputPath)), `"request_id":"request-a"`) {
				t.Errorf("runCombine(%s input) output missing request-a", test.name)
			}
		})
	}
}

func TestRunCombineReportsOutputCreationAndInstallErrors(t *testing.T) {
	inputPath := filepath.Join(t.TempDir(), "mixed.ndjson")
	if err := os.WriteFile(inputPath, []byte(mixedCapture()), 0o600); err != nil {
		t.Fatalf("os.WriteFile(%q) error: %v", inputPath, err)
	}

	tests := []struct {
		name       string
		outputPath string
		want       string
	}{
		{name: "create", outputPath: filepath.Join(t.TempDir(), "missing", "canonical.ndjson"), want: "create temporary output"},
		{name: "install", outputPath: t.TempDir(), want: "install output"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			if got, want := runCombine([]string{"-log", inputPath, "-out", test.outputPath}, &stdout, &stderr), 2; got != want {
				t.Errorf("runCombine(%s error) = %d, want %d", test.name, got, want)
			}
			if !strings.Contains(stderr.String(), test.want) {
				t.Errorf("runCombine(%s error) stderr = %q, want %q", test.name, stderr.String(), test.want)
			}
		})
	}
}

func TestCombinedCaptureReplaysExactlyOneRequestPerPair(t *testing.T) {
	dir := t.TempDir()
	inputPath := filepath.Join(dir, "mixed.ndjson")
	outputPath := filepath.Join(dir, "canonical.ndjson")
	if err := os.WriteFile(inputPath, []byte(mixedCapture()), 0o600); err != nil {
		t.Fatalf("os.WriteFile(%q) error: %v", inputPath, err)
	}
	var stdout, stderr bytes.Buffer
	if code := runCombine([]string{"-log", inputPath, "-out", outputPath}, &stdout, &stderr); code != 0 {
		t.Fatalf("runCombine() = %d, want 0; stderr=%q", code, stderr.String())
	}
	output := string(mustReadFile(t, outputPath))
	if !strings.HasSuffix(strings.TrimSpace(output), `{"type":"connection_close","node":"envoy-a","connection_id":7}`) {
		t.Fatalf("combined output = %q, want final DC-derived close marker", output)
	}

	cfg := config.Default()
	cfg.Replay.DryRun = true
	summary, err := runReplayFromFile(context.Background(), cfg, metrics.New(cfg.Metrics), outputPath, "")
	if err != nil {
		t.Fatalf("runReplayFromFile(combined output) error: %v", err)
	}
	if summary.Outcome != engine.RunSuccess || summary.Skipped != 2 || summary.ConnectionsDone != 1 || summary.RequestsSent != 0 {
		t.Errorf("runReplayFromFile(combined output) summary = %+v, want success skipped=2 connections_done=1 sent=0", summary)
	}
}

func gzipCapture(t *testing.T, data []byte) []byte {
	t.Helper()
	var buffer bytes.Buffer
	writer := gzip.NewWriter(&buffer)
	if _, err := writer.Write(data); err != nil {
		t.Fatalf("gzip.Writer.Write() error: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("gzip.Writer.Close() error: %v", err)
	}
	return buffer.Bytes()
}

func zstdCapture(t *testing.T, data []byte) []byte {
	t.Helper()
	var buffer bytes.Buffer
	writer, err := zstd.NewWriter(&buffer)
	if err != nil {
		t.Fatalf("zstd.NewWriter() error: %v", err)
	}
	if _, err := writer.Write(data); err != nil {
		t.Fatalf("zstd.Writer.Write() error: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("zstd.Writer.Close() error: %v", err)
	}
	return buffer.Bytes()
}

func mustReadFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("os.ReadFile(%q) error: %v", path, err)
	}
	return data
}

func TestJoinCleanupErrorPreservesCauses(t *testing.T) {
	primary := context.Canceled
	cleanup := errors.New("remove temporary output")
	got := joinCleanupError(primary, cleanup)
	if !errors.Is(got, primary) {
		t.Errorf("errors.Is(joinCleanupError(), primary) = false, want true; error=%v", got)
	}
	if !errors.Is(got, cleanup) {
		t.Errorf("errors.Is(joinCleanupError(), cleanup) = false, want true; error=%v", got)
	}
	if strings.Contains(got.Error(), "\n") {
		t.Errorf("joinCleanupError() = %q, want single-line error", got)
	}
	if got := joinCleanupError(nil, cleanup); got != cleanup {
		t.Errorf("joinCleanupError(nil, cleanup) = %v, want original cleanup error", got)
	}
}

func TestRunCombineParseErrorIsSingleLine(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := runCombine([]string{"-unknown"}, &stdout, &stderr)
	if code != 2 || strings.Count(stderr.String(), "\n") != 1 {
		t.Errorf("runCombine(-unknown) = code %d stderr %q, want code 2 and one error line", code, stderr.String())
	}
	if got := fmt.Sprint(stdout.String()); got != "" {
		t.Errorf("runCombine(-unknown) stdout = %q, want empty", got)
	}
}
