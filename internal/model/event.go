package model

type EventType string

const (
	EventMeta            EventType = "meta"
	EventConnectionOpen  EventType = "connection_open"
	EventRequest         EventType = "request"
	EventResponse        EventType = "response"
	EventConnectionClose EventType = "connection_close"
)

type Event struct {
	Type         EventType           `json:"type"`
	ConnectionID string              `json:"connection_id,omitempty"`
	StreamID     int                 `json:"stream_id,omitempty"`
	Sequence     int                 `json:"sequence,omitempty"`
	Timestamp    string              `json:"timestamp,omitempty"`
	Status       int                 `json:"status,omitempty"`
	DurationMS   float64             `json:"duration_ms,omitempty"`
	Reason       string              `json:"reason,omitempty"`
	HTTP         HTTPRequestMeta     `json:"http,omitempty"`
	Headers      map[string][]string `json:"headers,omitempty"`
	Body         Body                `json:"body,omitempty"`
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
