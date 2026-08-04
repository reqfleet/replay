package model

type EventType string

const (
	EventConnectionOpen          EventType = "connection_open"
	EventRequest                 EventType = "request"
	EventConnectionClose         EventType = "connection_close"
	AccessLogTypeDownstreamStart EventType = "DownstreamStart"
	AccessLogTypeDownstreamEnd   EventType = "DownstreamEnd"
)

type Event struct {
	ResponseCode    *int                `json:"response_code,omitempty"`
	TLS             *TLSInfo            `json:"tls,omitempty"`
	Headers         map[string][]string `json:"headers,omitempty"`
	Body            *Body               `json:"body,omitempty"`
	ResponseHeaders map[string][]string `json:"response_headers,omitempty"`
	ResponseBody    *Body               `json:"response_body,omitempty"`

	Type                    EventType `json:"type"`
	Node                    string    `json:"node,omitempty"`
	StartTime               string    `json:"start_time"`
	Method                  string    `json:"method"`
	Scheme                  string    `json:"scheme,omitempty"`
	Authority               string    `json:"authority"`
	Path                    string    `json:"path"`
	Protocol                string    `json:"protocol"`
	DownstreamRemoteAddress string    `json:"downstream_remote_address,omitempty"`
	UserAgent               string    `json:"user_agent,omitempty"`

	ConnectionID int     `json:"connection_id"`
	StreamID     int     `json:"stream_id,omitempty"`
	Sequence     int     `json:"sequence,omitempty"`
	DurationMS   float64 `json:"duration_ms,omitempty"`
}

type Body struct {
	Encoding  string `json:"encoding,omitempty"`
	Content   string `json:"content,omitempty"`
	SizeBytes int64  `json:"size_bytes,omitempty"`
}

type TLSInfo struct {
	SNI     string `json:"sni,omitempty"`
	ALPN    string `json:"alpn,omitempty"`
	Version string `json:"version,omitempty"`
	Enabled bool   `json:"enabled,omitempty"`
}
