package main

import (
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"net/url"
	"os"
	"path"
	"slices"
	"strings"
	"time"

	"github.com/reqfleet/replay/internal/model"
)

const (
	generatedDownstreamStart = "DownstreamStart"
	generatedDownstreamEnd   = "DownstreamEnd"
	generatedRequestInterval = 100 * time.Millisecond
	reverseCompletionStepMS  = 200.0
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

type generatedRequestOptions struct {
	authority    string
	scheme       string
	port         string
	apiKey       string
	requestPath  string
	status       int
	durationMS   float64
	extraHeaders map[string][]string
	body         *model.Body
}
type generatedObservation struct {
	Type          string `json:"type"`
	RequestID     string `json:"request_id"`
	Timestamp     string `json:"timestamp"`
	Method        string `json:"method"`
	Scheme        string `json:"scheme,omitempty"`
	Authority     string `json:"authority"`
	Path          string `json:"path"`
	Protocol      string `json:"protocol"`
	ResponseFlags string `json:"response_flags,omitempty"`

	ConnectionID int                 `json:"connection_id"`
	StreamID     int                 `json:"stream_id,omitempty"`
	Headers      map[string][]string `json:"headers,omitempty"`
	Body         *model.Body         `json:"body,omitempty"`
	ResponseCode *int                `json:"response_code,omitempty"`
	DurationMS   float64             `json:"duration_ms,omitempty"`
}

func parseGeneratedHeader(raw string) (string, string, error) {
	name, value, ok := strings.Cut(raw, ":")
	name = strings.ToLower(strings.TrimSpace(name))
	if !ok || name == "" {
		return "", "", fmt.Errorf("header %q must use the format name:value", raw)
	}
	return name, strings.TrimSpace(value), nil
}

func generatedRequestBody(raw string) *model.Body {
	if raw == "" {
		return nil
	}
	body := []byte(raw)
	return &model.Body{
		Encoding:  "base64",
		Content:   base64.StdEncoding.EncodeToString(body),
		SizeBytes: int64(len(body)),
	}
}

func generatedRequestEvent(connID, requestOrdinal int, ts time.Time, options generatedRequestOptions) model.Event {
	headers := map[string][]string{
		"x-api-key":          {options.apiKey},
		"x-forwarded-proto":  {options.scheme},
		"x-forwarded-scheme": {options.scheme},
		"x-real-ip":          {"172.18.0.1"},
		"x-forwarded-host":   {options.authority},
		"x-forwarded-port":   {options.port},
	}
	for name, values := range options.extraHeaders {
		headers[name] = slices.Clone(values)
	}

	responseCode := options.status
	request := model.Event{
		Type:         model.EventRequest,
		RequestID:    fmt.Sprintf("connection-%d-request-%d", connID, requestOrdinal),
		ConnectionID: connID,
		Timestamp:    ts.Format(time.RFC3339Nano),
		Method:       "GET",
		Scheme:       options.scheme,
		Authority:    options.authority,
		Path:         options.requestPath,
		Protocol:     "HTTP/1.1",
		Headers:      headers,
		Body:         options.body,
		ResponseCode: &responseCode,
		DurationMS:   options.durationMS,
	}
	return request
}
func generatedRequestPath(base string, requestOrdinal int) string {
	requestPath := path.Join("/", base)
	if requestOrdinal > 1 {
		requestPath = path.Join(requestPath, fmt.Sprintf("%d", requestOrdinal))
	}
	return requestPath
}

func generatedObservationForRequest(kind string, request model.Event, durationMS float64, responseFlags string) generatedObservation {
	observation := generatedObservation{
		Type:          kind,
		RequestID:     request.RequestID,
		ConnectionID:  request.ConnectionID,
		Timestamp:     request.Timestamp,
		Method:        request.Method,
		Scheme:        request.Scheme,
		Authority:     request.Authority,
		Path:          request.Path,
		Protocol:      request.Protocol,
		StreamID:      request.StreamID,
		ResponseFlags: responseFlags,
	}
	if kind == generatedDownstreamEnd {
		observation.Headers = request.Headers
		observation.Body = request.Body
		observation.ResponseCode = request.ResponseCode
		observation.DurationMS = durationMS
	}
	return observation
}

func emitGeneratedObservations(reqs, conns int, now time.Time, options generatedRequestOptions, emit func(generatedObservation) error) error {
	emitObservation := func(kind string, requestOrdinal, connectionID int, durationMS float64, responseFlags string) error {
		requestOptions := options
		requestOptions.requestPath = generatedRequestPath(options.requestPath, requestOrdinal)
		timestamp := now.Add(time.Duration(requestOrdinal) * generatedRequestInterval)
		request := generatedRequestEvent(connectionID, requestOrdinal, timestamp, requestOptions)
		request.Protocol = "HTTP/2"
		request.StreamID = requestOrdinal*2 - 1
		return emit(generatedObservationForRequest(kind, request, durationMS, responseFlags))
	}

	for requestOrdinal := 1; requestOrdinal <= reqs; requestOrdinal++ {
		for connectionID := 1; connectionID <= conns; connectionID++ {
			if err := emitObservation(generatedDownstreamStart, requestOrdinal, connectionID, 0, ""); err != nil {
				return err
			}
		}
	}
	for requestOrdinal := reqs; requestOrdinal >= 1; requestOrdinal-- {
		durationMS := options.durationMS + float64(reqs-requestOrdinal)*reverseCompletionStepMS
		responseFlags := "-"
		if requestOrdinal == 1 {
			responseFlags = "DC"
		}
		for connectionID := 1; connectionID <= conns; connectionID++ {
			if err := emitObservation(generatedDownstreamEnd, requestOrdinal, connectionID, durationMS, responseFlags); err != nil {
				return err
			}
		}
	}
	return nil
}

func emitGeneratedEvents(reqs, conns int, now time.Time, options generatedRequestOptions, emit func(model.Event) error) error {
	for requestOrdinal := 1; requestOrdinal <= reqs; requestOrdinal++ {
		requestPath := generatedRequestPath(options.requestPath, requestOrdinal)
		timestamp := now.Add(time.Duration(requestOrdinal) * generatedRequestInterval)
		for connectionID := 1; connectionID <= conns; connectionID++ {
			requestOptions := options
			requestOptions.requestPath = requestPath
			if err := emit(generatedRequestEvent(connectionID, requestOrdinal, timestamp, requestOptions)); err != nil {
				return err
			}
		}
	}
	return nil
}

func generatedEvents(reqs, conns int, now time.Time, options generatedRequestOptions) []model.Event {
	events := []model.Event{}
	if err := emitGeneratedEvents(reqs, conns, now, options, func(event model.Event) error {
		events = append(events, event)
		return nil
	}); err != nil {
		panic(fmt.Sprintf("unexpected error generating events: %v", err))
	}
	return events
}

func main() {
	var (
		baseURL      = flag.String("base", "http://localhost:8080", "Base URL to generate requests for")
		requestPath  = flag.String("subpath", "api/v1/resource", "Subpath for generated requests")
		reqs         = flag.Int("reqs", 5, "Number of requests per connection")
		conns        = flag.Int("conns", 1, "Number of simulated connections")
		out          = flag.String("out", "requests.ndjson", "Output file path")
		status       = flag.Int("status", 200, "HTTP response status code to simulate")
		dur          = flag.Float64("duration", 16.0, "Request duration in milliseconds")
		apiKey       = flag.String("apikey", "rqt_api_dummy-apikey-local", "API key header value")
		body         = flag.String("body", "", "Request body to include")
		observations = flag.Bool("observations", false, "Emit mixed HTTP/2 DownstreamStart/DownstreamEnd observations in reverse completion order")
	)
	extraHeaders := make(map[string][]string)
	flag.Func("header", "Request header in name:value format; repeatable and replaces generated values for the same name", func(raw string) error {
		name, value, err := parseGeneratedHeader(raw)
		if err != nil {
			return err
		}
		extraHeaders[name] = append(extraHeaders[name], value)
		return nil
	})
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

	file, err := os.Create(*out)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create output file: %v\n", err)
		os.Exit(1)
	}
	defer file.Close()

	options := generatedRequestOptions{
		authority:    authority,
		scheme:       scheme,
		port:         port,
		apiKey:       *apiKey,
		requestPath:  *requestPath,
		status:       *status,
		durationMS:   *dur,
		extraHeaders: extraHeaders,
		body:         generatedRequestBody(*body),
	}
	if *observations {
		if err := emitGeneratedObservations(*reqs, *conns, time.Now().UTC(), options, func(observation generatedObservation) error {
			return writeJSONLine(file, observation)
		}); err != nil {
			fmt.Fprintf(os.Stderr, "write observation: %v\n", err)
			os.Exit(2)
		}
		fmt.Printf("Generated %d connections with %d observation pairs each to %s\n", *conns, *reqs, *out)
		return
	}
	if err := emitGeneratedEvents(*reqs, *conns, time.Now().UTC(), options, func(event model.Event) error {
		return writeJSONLine(file, event)
	}); err != nil {
		fmt.Fprintf(os.Stderr, "write event: %v\n", err)
		os.Exit(2)
	}

	fmt.Printf("Generated %d connections with %d requests each to %s\n", *conns, *reqs, *out)
}
