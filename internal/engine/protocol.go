package engine

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"

	"github.com/reqfleet/replay/internal/model"
)

// protocolFidelityError is separate from optional response validation and from
// ordinary reachability failures. It is never eligible for request retries.
type protocolFidelityError struct {
	expected    string
	observed    string
	destination string
	err         error
}

func (e *protocolFidelityError) Error() string {
	message := fmt.Sprintf("HTTP protocol fidelity: destination %q expected %q, observed %q", e.destination, e.expected, e.observed)
	if e.err != nil {
		return message + ": " + e.err.Error()
	}
	return message
}

func (e *protocolFidelityError) Unwrap() error { return e.err }

type h2cProtocolTraceKey struct{}

// h2cProtocolConn retains an HTTP/1 response prefix before Go's HTTP/2 read
// loop can replace its error with "client conn could not be established".
// Bytes pass through unchanged; only the first nine bytes are inspected.
type h2cProtocolConn struct {
	net.Conn
	// The transport's single read loop owns the prefix; request goroutines
	// may inspect rejection concurrently after their RoundTrip fails.
	prefix    [9]byte
	prefixLen int
	rejection atomic.Pointer[protocolFidelityError]
}

func (c *h2cProtocolConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if c.prefixLen < len(c.prefix) {
		c.prefixLen += copy(c.prefix[c.prefixLen:], p[:n])
		if c.prefixLen == len(c.prefix) {
			switch string(c.prefix[:]) {
			case "HTTP/1.0 ", "HTTP/1.1 ":
				c.rejection.Store(&protocolFidelityError{
					expected: model.ProtocolHTTP2,
					observed: string(c.prefix[:8]),
				})
			}
		}
	}
	return n, err
}

func verifyResponseProtocol(resp *http.Response, expected string, destination *url.URL) error {
	major, minor := 1, 1
	if expected == model.ProtocolHTTP2 {
		major, minor = 2, 0
	}
	if resp.ProtoMajor == major && resp.ProtoMinor == minor {
		return nil
	}
	return &protocolFidelityError{
		expected: expected, observed: fmt.Sprintf("HTTP/%d.%d", resp.ProtoMajor, resp.ProtoMinor), destination: destination.Redacted(),
	}
}

func classifyProtocolError(err error, expected, destination string) error {
	var fidelityErr *protocolFidelityError
	if errors.As(err, &fidelityErr) {
		// TLS verification and the h2c observer have no request URL. Copy
		// rather than mutate an error shared by concurrent HTTP/2 streams.
		return &protocolFidelityError{
			expected: fidelityErr.expected, observed: fidelityErr.observed,
			destination: destination, err: fidelityErr.err,
		}
	}
	// crypto/tls's TCP alert type and net/http's bundled HTTP/2 errors
	// are unexported. Match only explicit protocol rejection or support diagnostics;
	// timeouts, EOF, certificate errors, and other network failures stay distinct.
	observed := ""
	for cause := err; cause != nil; cause = errors.Unwrap(cause) {
		switch {
		case cause.Error() == "tls: no application protocol":
			observed = "ALPN rejected"
		case cause.Error() == "tls: server selected unadvertised ALPN protocol":
			observed = "unadvertised ALPN protocol"
		case expected == model.ProtocolHTTP2 && cause.Error() == "http: Transport does not support unencrypted HTTP/2":
			observed = "HTTP/2 unavailable"
		case expected == model.ProtocolHTTP2 && strings.Contains(cause.Error(), "frame header looked like an HTTP/1.1 header"):
			observed = model.ProtocolHTTP11
		case expected == model.ProtocolHTTP2 && cause.Error() == "http2: frame too large" && observed == "":
			observed = "invalid HTTP/2 frame"
		}
	}
	if observed == "" {
		return err
	}
	return &protocolFidelityError{expected: expected, observed: observed, destination: destination, err: err}
}
