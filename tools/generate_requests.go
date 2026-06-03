package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/url"
	"os"
	"path"
	"time"

	"github.com/reqfleet/replay/internal/model"
)

func writeJSONLine(f *os.File, v any) error {
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
	var (
		baseURL     = flag.String("base", "http://localhost:8080", "Base URL to generate requests for")
		requestPath = flag.String("subpath", "api/v1/resource", "Subpath for generated requests")
		reqs        = flag.Int("reqs", 5, "Number of requests per connection")
		conns       = flag.Int("conns", 1, "Number of simulated connections")
		out         = flag.String("out", "requests.ndjson", "Output file path")
		status      = flag.Int("status", 200, "HTTP response status code to simulate")
		dur         = flag.Float64("duration", 16.0, "Request duration in milliseconds")
		apiKey      = flag.String("apikey", "rqt_api_dummy-apikey-local", "API key header value")
	)
	flag.StringVar(baseURL, "url", *baseURL, "Alias for -base")
	flag.StringVar(requestPath, "path", *requestPath, "Alias for -subpath")
	flag.Parse()

	u, err := url.Parse(*baseURL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid base URL: %v\n", err)
		os.Exit(1)
	}
	authority := u.Host
	scheme := u.Scheme
	port := u.Port()
	if port == "" {
		if scheme == "https" {
			port = "443"
		} else {
			port = "80"
		}
	}

	f, err := os.Create(*out)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create output file: %v\n", err)
		os.Exit(1)
	}
	defer f.Close()

	now := time.Now().UTC()

	meta := model.Event{
		Type:          model.EventMeta,
		FormatVersion: "1.0",
	}
	if err := writeJSONLine(f, meta); err != nil {
		fmt.Fprintf(os.Stderr, "write meta: %v\n", err)
		os.Exit(2)
	}

	for c := 1; c <= *conns; c++ {
		connID := c
		connStart := now.Add(time.Duration(c-1) * 50 * time.Millisecond)

		openEvt := model.Event{
			Type:                    model.EventConnectionOpen,
			ConnectionID:            connID,
			Timestamp:               connStart.Format(time.RFC3339Nano),
			DownstreamRemoteAddress: "172.18.0.1:45398",
		}
		if err := writeJSONLine(f, openEvt); err != nil {
			fmt.Fprintf(os.Stderr, "write open: %v\n", err)
			os.Exit(2)
		}

		for r := 1; r <= *reqs; r++ {
			p := path.Join("/", *requestPath)
			if r > 1 {
				p = path.Join(p, fmt.Sprintf("%d", r))
			}
			ts := connStart.Add(time.Duration(r) * 100 * time.Millisecond)

			req := model.Event{
				Type:         model.EventRequest,
				Status:       *status,
				ConnectionID: connID,
				Headers: map[string][]string{
					"x-api-key":          {*apiKey},
					"x-forwarded-proto":  {scheme},
					"x-forwarded-scheme": {scheme},
					"x-real-ip":          {"172.18.0.1"},
					"x-forwarded-host":   {authority},
					"x-forwarded-port":   {port},
				},
				HTTP: model.HTTPRequestMeta{
					Version:   "HTTP/1.1",
					Authority: authority,
					Method:    "GET",
					Path:      p,
				},
				DurationMS: *dur,
				Timestamp:  ts.Format(time.RFC3339Nano),
			}
			if err := writeJSONLine(f, req); err != nil {
				fmt.Fprintf(os.Stderr, "write req: %v\n", err)
				os.Exit(2)
			}
		}

		closeEvt := model.Event{
			Type:         model.EventConnectionClose,
			ConnectionID: connID,
			Timestamp:    connStart.Add(time.Duration(*reqs+1) * time.Second).Format(time.RFC3339Nano),
			Reason:       "remote_close",
		}
		if err := writeJSONLine(f, closeEvt); err != nil {
			fmt.Fprintf(os.Stderr, "write close: %v\n", err)
			os.Exit(2)
		}
	}

	fmt.Printf("Generated %d connections with %d requests each to %s\n", *conns, *reqs, *out)
}
