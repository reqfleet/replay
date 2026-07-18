package parser

import (
	"bytes"
	"compress/gzip"
	"os"
	"path/filepath"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/reqfleet/replay/internal/model"
)

func TestParseFileStream(t *testing.T) {
	content := []byte("{\"type\":\"request\",\"connection_id\":1,\"http\":{\"method\":\"GET\",\"scheme\":\"http\",\"authority\":\"example.com\",\"path\":\"/\"}}\n")

	t.Run("plain", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "plain.log")
		if err := os.WriteFile(path, content, 0644); err != nil {
			t.Fatal(err)
		}
		var events []model.Event
		err := ParseFileStream(path, "", func(e model.Event) error {
			events = append(events, e)
			return nil
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(events) != 1 {
			t.Fatalf("expected 1 event, got %d", len(events))
		}
	})

	t.Run("gzip", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "gzip.log")

		var buf bytes.Buffer
		gw := gzip.NewWriter(&buf)
		gw.Write(content)
		gw.Close()

		if err := os.WriteFile(path, buf.Bytes(), 0644); err != nil {
			t.Fatal(err)
		}
		var events []model.Event
		err := ParseFileStream(path, "gzip", func(e model.Event) error {
			events = append(events, e)
			return nil
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(events) != 1 {
			t.Fatalf("expected 1 event, got %d", len(events))
		}
	})

	t.Run("zstd", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "zstd.log")

		var buf bytes.Buffer
		zw, _ := zstd.NewWriter(&buf)
		zw.Write(content)
		zw.Close()

		if err := os.WriteFile(path, buf.Bytes(), 0644); err != nil {
			t.Fatal(err)
		}
		var events []model.Event
		err := ParseFileStream(path, "zstd", func(e model.Event) error {
			events = append(events, e)
			return nil
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(events) != 1 {
			t.Fatalf("expected 1 event, got %d", len(events))
		}
	})
}
