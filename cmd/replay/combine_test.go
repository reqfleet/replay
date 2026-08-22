package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/reqfleet/replay/internal/config"
	"github.com/reqfleet/replay/internal/engine"
	"github.com/reqfleet/replay/internal/metrics"
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
	matches, err := filepath.Glob(filepath.Join(dir, ".canonical.ndjson.tmp-*"))
	if err != nil {
		t.Fatalf("filepath.Glob(temporary outputs) error: %v", err)
	}
	if len(matches) != 0 {
		t.Errorf("temporary outputs after success = %v, want none", matches)
	}
}

func TestRunCombinePairingErrorPreservesExistingOutput(t *testing.T) {
	dir := t.TempDir()
	inputPath := filepath.Join(dir, "unmatched.ndjson")
	outputPath := filepath.Join(dir, "canonical.ndjson")
	unmatched := strings.Split(mixedCapture(), "\n")[0] + "\n"
	if err := os.WriteFile(inputPath, []byte(unmatched), 0o600); err != nil {
		t.Fatalf("os.WriteFile(%q) error: %v", inputPath, err)
	}
	if err := os.WriteFile(outputPath, []byte("existing output\n"), 0o600); err != nil {
		t.Fatalf("os.WriteFile(%q) error: %v", outputPath, err)
	}

	var stdout, stderr bytes.Buffer
	if got, want := runCombine([]string{"-log", inputPath, "-out", outputPath}, &stdout, &stderr), 2; got != want {
		t.Errorf("runCombine(unmatched) = %d, want %d", got, want)
	}
	if got, want := string(mustReadFile(t, outputPath)), "existing output\n"; got != want {
		t.Errorf("output after pairing failure = %q, want %q", got, want)
	}
	if !strings.Contains(stderr.String(), "unmatched DownstreamStart") {
		t.Errorf("runCombine(unmatched) stderr = %q, want pairing error", stderr.String())
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
