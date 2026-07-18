package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/url"
	"os"
	"path"
	"strings"
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

func generatedAccessLogType(raw string) (model.AccessLogType, error) {
	switch strings.ToLower(raw) {
	case "downstream_start", "downstreamstart", "downstream-start":
		return model.AccessLogTypeDownstreamStart, nil
	case "downstream_end", "downstreamend", "downstream-end":
		return model.AccessLogTypeDownstreamEnd, nil
	default:
		return "", fmt.Errorf("unsupported access log type %q", raw)
	}
}

func generatedRequestEvent(logType model.AccessLogType, connID int, ts time.Time, authority, scheme, port, apiKey, requestPath string, status int, durationMS float64) model.Event {
	req := model.Event{
		Type:          model.EventRequest,
		AccessLogType: logType,
		ConnectionID:  connID,
		Headers: map[string][]string{
			"x-api-key":          {apiKey},
			"x-forwarded-proto":  {scheme},
			"x-forwarded-scheme": {scheme},
			"x-real-ip":          {"172.18.0.1"},
			"x-forwarded-host":   {authority},
			"x-forwarded-port":   {port},
		},
		HTTP: model.HTTPRequestMeta{
			Version:   "HTTP/1.1",
			Scheme:    scheme,
			Authority: authority,
			Method:    "GET",
			Path:      requestPath,
		},
		Timestamp: ts.Format(time.RFC3339Nano),
	}
	if logType == model.AccessLogTypeDownstreamEnd {
		req.Status = status
		req.DurationMS = durationMS
	}
	return req
}

func emitGeneratedEvents(logType model.AccessLogType, reqs, conns int, now time.Time, authority, scheme, port, apiKey, requestPath string, status int, durationMS float64, emit func(model.Event) error) error {

	for c := 1; c <= conns; c++ {
		if err := emit(model.Event{
			Type:                    model.EventConnectionOpen,
			ConnectionID:            c,
			Timestamp:               now.Format(time.RFC3339Nano),
			DownstreamRemoteAddress: "172.18.0.1:45398",
		}); err != nil {
			return err
		}
	}

	for r := 1; r <= reqs; r++ {
		p := path.Join("/", requestPath)
		if r > 1 {
			p = path.Join(p, fmt.Sprintf("%d", r))
		}
		ts := now.Add(time.Duration(r) * 100 * time.Millisecond)
		for c := 1; c <= conns; c++ {
			if err := emit(generatedRequestEvent(logType, c, ts, authority, scheme, port, apiKey, p, status, durationMS)); err != nil {
				return err
			}
		}
	}

	closeTimestamp := now.Add(time.Duration(reqs+1) * 100 * time.Millisecond).Format(time.RFC3339Nano)
	for c := 1; c <= conns; c++ {
		if err := emit(model.Event{
			Type:         model.EventConnectionClose,
			ConnectionID: c,
			Timestamp:    closeTimestamp,
			Reason:       "remote_close",
		}); err != nil {
			return err
		}
	}

	return nil
}

func generatedEvents(logType model.AccessLogType, reqs, conns int, now time.Time, authority, scheme, port, apiKey, requestPath string, status int, durationMS float64) []model.Event {
	events := []model.Event{}
	if err := emitGeneratedEvents(logType, reqs, conns, now, authority, scheme, port, apiKey, requestPath, status, durationMS, func(event model.Event) error {
		events = append(events, event)
		return nil
	}); err != nil {
		panic(fmt.Sprintf("unexpected error generating events: %v", err))
	}
	return events
}

func main() {
	var (
		baseURL       = flag.String("base", "http://localhost:8080", "Base URL to generate requests for")
		requestPath   = flag.String("subpath", "api/v1/resource", "Subpath for generated requests")
		reqs          = flag.Int("reqs", 5, "Number of requests per connection")
		conns         = flag.Int("conns", 1, "Number of simulated connections")
		out           = flag.String("out", "requests.ndjson", "Output file path")
		status        = flag.Int("status", 200, "HTTP response status code to simulate")
		dur           = flag.Float64("duration", 16.0, "Request duration in milliseconds")
		apiKey        = flag.String("apikey", "rqt_api_dummy-apikey-local", "API key header value")
		accessLogType = flag.String("access-log-type", "downstream-end", "Envoy access log type to simulate: downstream-start or downstream-end")
	)
	flag.StringVar(baseURL, "url", *baseURL, "Alias for -base")
	flag.StringVar(requestPath, "path", *requestPath, "Alias for -subpath")
	flag.StringVar(accessLogType, "log-type", *accessLogType, "Alias for -access-log-type")
	flag.Parse()

	logType, err := generatedAccessLogType(*accessLogType)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

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

	if err := emitGeneratedEvents(logType, *reqs, *conns, now, authority, scheme, port, *apiKey, *requestPath, *status, *dur, func(event model.Event) error {
		return writeJSONLine(f, event)
	}); err != nil {
		fmt.Fprintf(os.Stderr, "write event: %v\n", err)
		os.Exit(2)
	}

	fmt.Printf("Generated %d connections with %d requests each to %s\n", *conns, *reqs, *out)
}
