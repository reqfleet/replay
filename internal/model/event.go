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
	Type          EventType `json:"type"`
	FormatVersion string    `json:"format_version,omitempty"`
	Generator     string    `json:"generator,omitempty"`
	CreatedAt     string    `json:"created_at,omitempty"`
	Node          string    `json:"node,omitempty"`
	Cluster       string    `json:"cluster,omitempty"`
	ConnectionID  string    `json:"connection_id,omitempty"`
	StreamID      int       `json:"stream_id,omitempty"`
	Sequence      int       `json:"sequence,omitempty"`
	Timestamp     string    `json:"timestamp,omitempty"`
	Status        int       `json:"status,omitempty"`
	DurationMS    float64   `json:"duration_ms,omitempty"`
	Reason        string    `json:"reason,omitempty"`
	// Connection open metadata
	DownstreamRemoteAddress string              `json:"downstream_remote_address,omitempty"`
	DownstreamLocalAddress  string              `json:"downstream_local_address,omitempty"`
	Protocol                string              `json:"protocol,omitempty"`
	TLS                     *TLSInfo            `json:"tls,omitempty"`
	HTTP                    HTTPRequestMeta     `json:"http,omitempty"`
	Headers                 map[string][]string `json:"headers,omitempty"`
	Body                    Body                `json:"body,omitempty"`
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

type TLSInfo struct {
	Enabled bool   `json:"enabled,omitempty"`
	SNI     string `json:"sni,omitempty"`
	ALPN    string `json:"alpn,omitempty"`
	Version string `json:"version,omitempty"`
}
