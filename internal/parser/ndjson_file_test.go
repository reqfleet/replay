package parser

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/reqfleet/replay/internal/model"
)

func TestOpenFileAndParseFileStream(t *testing.T) {
	content := []byte("{\"type\":\"request\",\"request_id\":\"request-1\",\"connection_id\":1,\"timestamp\":\"2026-02-27T03:10:22Z\",\"method\":\"GET\",\"scheme\":\"http\",\"authority\":\"example.com\",\"path\":\"/\",\"protocol\":\"HTTP/1.1\",\"response_code\":200}\n")

	tests := []struct {
		name   string
		format string
		encode func(*testing.T, []byte) []byte
	}{
		{name: "plain", format: "", encode: func(_ *testing.T, data []byte) []byte { return data }},
		{name: "zstd", format: "zstd", encode: zstdBytes},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), test.name+".log")
			if err := os.WriteFile(path, test.encode(t, content), 0o644); err != nil {
				t.Fatalf("os.WriteFile(%q) error: %v", path, err)
			}

			reader, err := OpenFile(path, test.format)
			if err != nil {
				t.Fatalf("OpenFile(%q, %q) error: %v", path, test.format, err)
			}
			opened, err := io.ReadAll(reader)
			if err != nil {
				t.Fatalf("io.ReadAll(OpenFile(%q, %q)) error: %v", path, test.format, err)
			}
			if err := reader.Close(); err != nil {
				t.Fatalf("OpenFile(%q, %q).Close() error: %v", path, test.format, err)
			}
			if !bytes.Equal(opened, content) {
				t.Errorf("OpenFile(%q, %q) bytes = %q, want %q", path, test.format, opened, content)
			}

			var events []model.Event
			if err := ParseFileStream(path, test.format, func(event model.Event) error {
				events = append(events, event)
				return nil
			}); err != nil {
				t.Fatalf("ParseFileStream(%q, %q) error: %v", path, test.format, err)
			}
			if len(events) != 1 || events[0].Type != model.EventRequest || events[0].RequestID != "request-1" {
				t.Errorf("ParseFileStream(%q, %q) events = %+v, want one canonical request", path, test.format, events)
			}
		})
	}
}

func TestOpenFileCloseUnblocksZstdDecoder(t *testing.T) {
	inputPath := filepath.Join(t.TempDir(), "mixed.fifo")
	if err := syscall.Mkfifo(inputPath, 0o600); err != nil {
		t.Fatalf("syscall.Mkfifo(%q) error: %v", inputPath, err)
	}
	holder, err := os.OpenFile(inputPath, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatalf("os.OpenFile(%q) error: %v", inputPath, err)
	}
	t.Cleanup(func() {
		if err := holder.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
			t.Errorf("holder.Close() error: %v", err)
		}
	})

	reader, err := OpenFile(inputPath, "zstd")
	if err != nil {
		t.Fatalf("OpenFile(%q, %q) error: %v", inputPath, "zstd", err)
	}
	closed := make(chan error, 1)
	go func() {
		closed <- reader.Close()
	}()
	select {
	case err := <-closed:
		if err != nil {
			t.Errorf("OpenFile(%q, %q).Close() error: %v", inputPath, "zstd", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("OpenFile(zstd FIFO).Close() did not return within 2s")
	}
}

func zstdBytes(t *testing.T, data []byte) []byte {
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
