package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/url"
	"os"
	"path"
	"time"
)

type MetaEvent struct {
	Type          string `json:"type"`
	FormatVersion string `json:"format_version,omitempty"`
	Generator     string `json:"generator,omitempty"`
	CreatedAt     string `json:"created_at,omitempty"`
}

type HTTPRequestMeta struct {
	Version   string `json:"version,omitempty"`
	Method    string `json:"method,omitempty"`
	Scheme    string `json:"scheme,omitempty"`
	Authority string `json:"authority,omitempty"`
	Path      string `json:"path,omitempty"`
}

type Body struct {
	Encoding  string `json:"encoding,omitempty"`
	Content   string `json:"content,omitempty"`
	SizeBytes int64  `json:"size_bytes,omitempty"`
}

type RequestEvent struct {
	Type         string              `json:"type"`
	ConnectionID string              `json:"connection_id,omitempty"`
	StreamID     int                 `json:"stream_id,omitempty"`
	Sequence     int                 `json:"sequence,omitempty"`
	Timestamp    string              `json:"timestamp,omitempty"`
	HTTP         HTTPRequestMeta     `json:"http,omitempty"`
	Headers      map[string][]string `json:"headers,omitempty"`
	Body         Body                `json:"body,omitempty"`
}

type GenericEvent map[string]interface{}

func writeJSONLine(f *os.File, v interface{}) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		return err
	}
	if _, err := f.Write([]byte("\n")); err != nil {
		return err
	}
	return nil
}

func main() {
	outPath := flag.String("out", "requests.log", "output NDJSON file")
	urlStr := flag.String("url", "http://localhost:8080", "base URL to use in generated logs (scheme+authority) e.g. http://localhost:8080/basepath")
	subPath := flag.String("path", "", "subpath to use in generated request paths, e.g. /api")
	conns := flag.Int("conns", 1, "number of distinct connection_ids to emit")
	reqs := flag.Int("reqs", 5, "number of requests per connection")
	format := flag.String("format", "1.0", "format_version to write in meta header")
	generator := flag.String("generator", "replay-loggen", "meta.generator field")
	flag.Parse()

	u, err := url.Parse(*urlStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid url: %v\n", err)
		os.Exit(2)
	}
	scheme := u.Scheme
	if scheme == "" {
		scheme = "http"
	}
	authority := u.Host
	basePath := u.Path
	if basePath == "" {
		basePath = "/"
	}

	f, err := os.Create(*outPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "create out: %v\n", err)
		os.Exit(2)
	}
	defer f.Close()

	// write meta
	meta := MetaEvent{Type: "meta", FormatVersion: *format, Generator: *generator, CreatedAt: time.Now().UTC().Format(time.RFC3339Nano)}
	if err := writeJSONLine(f, meta); err != nil {
		fmt.Fprintf(os.Stderr, "write meta: %v\n", err)
		os.Exit(2)
	}

	now := time.Now().UTC()
	for ci := 1; ci <= *conns; ci++ {
		connID := fmt.Sprintf("conn-%d", ci)
		// connection_open
		open := GenericEvent{
			"type":                      "connection_open",
			"connection_id":             connID,
			"timestamp":                 now.Format(time.RFC3339Nano),
			"downstream_remote_address": "127.0.0.1:0",
			"downstream_local_address":  "127.0.0.1:0",
			"protocol":                  "HTTP/1.1",
		}
		if err := writeJSONLine(f, open); err != nil {
			fmt.Fprintf(os.Stderr, "write open: %v\n", err)
			os.Exit(2)
		}

		for r := 1; r <= *reqs; r++ {
			ts := now.Add(time.Duration((ci-1)**reqs+r) * time.Second)
			p := path.Join(basePath, *subPath)
			req := RequestEvent{
				Type:         "request",
				ConnectionID: connID,
				StreamID:     1,
				Sequence:     r,
				Timestamp:    ts.Format(time.RFC3339Nano),
				HTTP: HTTPRequestMeta{
					Version:   "HTTP/1.1",
					Method:    "GET",
					Scheme:    scheme,
					Authority: authority,
					Path:      p,
				},
				Headers: map[string][]string{"User-Agent": {"replay-gen"}, "Accept": {"*/*"}},
				Body:    Body{Encoding: "base64", Content: "", SizeBytes: 0},
			}
			if err := writeJSONLine(f, req); err != nil {
				fmt.Fprintf(os.Stderr, "write req: %v\n", err)
				os.Exit(2)
			}

			// optional response event to match expected responses
			resp := GenericEvent{
				"type":          "response",
				"connection_id": connID,
				"sequence":      r,
				"timestamp":     ts.Add(10 * time.Millisecond).Format(time.RFC3339Nano),
				"status":        200,
				"duration_ms":   10.0,
			}
			if err := writeJSONLine(f, resp); err != nil {
				fmt.Fprintf(os.Stderr, "write resp: %v\n", err)
				os.Exit(2)
			}
		}

		// connection_close
		closeEvt := GenericEvent{
			"type":          "connection_close",
			"connection_id": connID,
			"timestamp":     now.Add(time.Duration(*reqs+1) * time.Second).Format(time.RFC3339Nano),
			"reason":        "remote_close",
		}
		if err := writeJSONLine(f, closeEvt); err != nil {
			fmt.Fprintf(os.Stderr, "write close: %v\n", err)
			os.Exit(2)
		}
	}

	fmt.Fprintf(os.Stdout, "wrote %s\n", *outPath)
}
