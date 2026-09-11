package engine

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/reqfleet/replay/config"
	"github.com/reqfleet/replay/internal/model"
)

func startProtocolServer(t *testing.T, encrypted bool, handler http.Handler) *httptest.Server {
	t.Helper()
	srv := httptest.NewUnstartedServer(handler)
	srv.Config.Protocols = new(http.Protocols)
	srv.Config.Protocols.SetHTTP1(true)
	if encrypted {
		srv.EnableHTTP2 = true
		srv.Config.Protocols.SetHTTP2(true)
		srv.StartTLS()
	} else {
		srv.Config.Protocols.SetUnencryptedHTTP2(true)
		srv.Start()
	}
	t.Cleanup(srv.Close)
	return srv
}

func protocolReplayEngine() *Engine {
	cfg := config.Default()
	cfg.Replay.TLS.InsecureSkipVerify = true
	cfg.Replay.Pacing.Enabled = false
	cfg.Replay.Idempotency.Enabled = false
	cfg.Replay.MaxVirtualUsersPerEngine = 1
	cfg.Replay.Timeout.Request = 2 * time.Second
	cfg.Replay.Retry.MaxAttempts = 1
	cfg.Replay.Validation.Status = false
	cfg.Replay.Validation.Headers = false
	cfg.Replay.Validation.Body = false
	return New(cfg, nil)
}

func protocolRequest(t *testing.T, target, protocol string, connection, sequence int, path string) model.Event {
	t.Helper()
	u, err := url.Parse(target)
	if err != nil {
		t.Fatalf("url.Parse(%q) error: %v", target, err)
	}
	return model.Event{
		Type: model.EventRequest, Node: "protocol-test", ConnectionID: connection,
		Sequence: sequence, StreamID: 2*sequence - 1, Protocol: protocol,
		Method: http.MethodGet, Scheme: u.Scheme, Authority: u.Host, Path: path,
	}
}

func TestRecordedProtocolClientAndReplay(t *testing.T) {
	for _, tt := range []struct {
		name      string
		encrypted bool
		http2     bool
		protocol  string
	}{
		{name: "http2_tls", encrypted: true, http2: true, protocol: "HTTP/2.0"},
		{name: "http2_prior_knowledge", http2: true, protocol: "HTTP/2.0"},
		{name: "http1_dual_tls", encrypted: true, protocol: "HTTP/1.1"},
		{name: "http1_dual_cleartext", protocol: "HTTP/1.1"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			observed := make(chan string, 2)
			srv := startProtocolServer(t, tt.encrypted, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				observed <- r.Proto
				w.WriteHeader(http.StatusNoContent)
			}))
			eng := protocolReplayEngine()
			client, transport := eng.makePerConnectionClient(tt.http2)
			t.Cleanup(transport.CloseIdleConnections)
			resp, err := client.Get(srv.URL + "/direct")
			if err != nil {
				t.Fatalf("recorded %s client.Get() error: %v", tt.protocol, err)
			}
			if err := resp.Body.Close(); err != nil {
				t.Errorf("response body Close() error: %v", err)
			}
			if resp.Proto != tt.protocol {
				t.Errorf("client response protocol = %q, want %q", resp.Proto, tt.protocol)
			}
			if tt.encrypted && tt.http2 && (resp.TLS == nil || resp.TLS.NegotiatedProtocol != "h2") {
				t.Errorf("HTTP/2 TLS response state = %+v, want negotiated h2", resp.TLS)
			}
			summary, err := runReplay(eng, []model.Event{protocolRequest(t, srv.URL, tt.protocol, 1, 1, "/replay")})
			if err != nil {
				t.Fatalf("ReplayStream(%s) error: %v", tt.protocol, err)
			}
			if summary.Outcome != RunSuccess || summary.ResponsesReceived != 1 {
				t.Fatalf("ReplayStream(%s) summary = %+v, want success and one response", tt.protocol, summary)
			}
			for range 2 {
				select {
				case got := <-observed:
					if got != tt.protocol {
						t.Errorf("server request protocol = %q, want %q", got, tt.protocol)
					}
				case <-time.After(time.Second):
					t.Fatal("server did not observe direct and replay requests")
				}
			}
		})
	}
}

func TestRecordedHTTP2RejectsHTTP1WithoutFallback(t *testing.T) {
	for _, mode := range []string{"http1_alpn", "no_alpn", "cleartext_http1"} {
		t.Run(mode, func(t *testing.T) {
			var intendedRequests atomic.Int64
			srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodGet && r.URL.Path == "/intended" {
					intendedRequests.Add(1)
				}
				w.WriteHeader(http.StatusOK)
			}))
			if mode == "cleartext_http1" {
				srv.Start()
			} else {
				srv.TLS = &tls.Config{MinVersion: tls.VersionTLS12, NextProtos: []string{"http/1.1"}}
				if mode == "no_alpn" {
					srv.TLS.NextProtos = []string{}
				}
				srv.StartTLS()
			}
			t.Cleanup(srv.Close)
			eng := protocolReplayEngine()
			client, transport := eng.makePerConnectionClient(true)
			t.Cleanup(transport.CloseIdleConnections)
			resp, err := client.Get(srv.URL + "/intended")
			if resp != nil {
				if closeErr := resp.Body.Close(); closeErr != nil {
					t.Errorf("response body Close() error: %v", closeErr)
				}
			}
			if err == nil {
				t.Error("recorded HTTP/2 client.Get(HTTP/1-only server) succeeded, want protocol rejection")
			}
			summary, err := runReplay(eng, []model.Event{protocolRequest(t, srv.URL, "HTTP/2", 1, 1, "/intended")})
			if err != nil {
				t.Fatalf("ReplayStream(HTTP/1-only server) error: %v", err)
			}
			if summary.Outcome != RunFailed || summary.ResponsesReceived != 0 {
				t.Errorf("ReplayStream(HTTP/1-only server) summary = %+v, want failed and no responses", summary)
			}
			if got := intendedRequests.Load(); got != 0 {
				t.Errorf("HTTP/1 application GET requests = %d, want 0 (no fallback)", got)
			}
		})
	}
}

func TestRecordedHTTP2MultiplexingOverlapsOnOneSocket(t *testing.T) {
	for _, encrypted := range []bool{true, false} {
		name := "h2c"
		if encrypted {
			name = "tls"
		}
		t.Run(name, func(t *testing.T) {
			type observation struct{ protocol, remote string }
			started := make(chan observation, 2)
			release := make(chan struct{})
			srv := startProtocolServer(t, encrypted, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				started <- observation{r.Proto, r.RemoteAddr}
				select {
				case <-release:
				case <-r.Context().Done():
				}
				w.WriteHeader(http.StatusNoContent)
			}))
			// Release handlers before the server cleanup, including on assertion failure.
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			t.Cleanup(unblock)
			eng := protocolReplayEngine()
			eng.cfg.Replay.HTTP2.Mode = "multiplexed"
			result := runReplayAsync(eng, []model.Event{
				protocolRequest(t, srv.URL, "HTTP/2", 1, 1, "/first"),
				protocolRequest(t, srv.URL, "HTTP/2.0", 1, 2, "/second"),
			})
			var socket string
			for range 2 {
				select {
				case got := <-started:
					if got.protocol != "HTTP/2.0" {
						t.Errorf("overlapping request protocol = %q, want HTTP/2.0", got.protocol)
					}
					if socket != "" && got.remote != socket {
						t.Errorf("overlapping stream socket = %q, want shared socket %q", got.remote, socket)
					}
					socket = got.remote
				case <-time.After(time.Second):
					t.Fatal("second HTTP/2 stream did not arrive while first handler was blocked")
				}
			}
			unblock()
			got := <-result
			if got.err != nil || got.summary.Outcome != RunSuccess || got.summary.ResponsesReceived != 2 {
				t.Errorf("multiplexed ReplayStream() = %+v, error %v; want success and two responses", got.summary, got.err)
			}
		})
	}
}

func TestRecordedMixedProtocolsKeepSeparateSockets(t *testing.T) {
	type observation struct{ protocol, remote string }
	var mu sync.Mutex
	observed := make(map[string]observation)
	srv := startProtocolServer(t, true, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		observed[r.URL.Path] = observation{r.Proto, r.RemoteAddr}
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	eng := protocolReplayEngine()
	summary, err := runReplay(eng, []model.Event{
		protocolRequest(t, srv.URL, "HTTP/1.1", 1, 1, "/h1/first"),
		protocolRequest(t, srv.URL, "HTTP/2", 2, 1, "/h2/first"),
		protocolRequest(t, srv.URL, "HTTP/1.1", 1, 2, "/h1/second"),
		protocolRequest(t, srv.URL, "HTTP/2.0", 2, 2, "/h2/second"),
	})
	if err != nil || summary.Outcome != RunSuccess || summary.ResponsesReceived != 4 {
		t.Fatalf("mixed ReplayStream() = %+v, error %v; want success and four responses", summary, err)
	}
	mu.Lock()
	defer mu.Unlock()
	for _, pair := range []struct{ prefix, protocol string }{{"/h1", "HTTP/1.1"}, {"/h2", "HTTP/2.0"}} {
		first, second := observed[pair.prefix+"/first"], observed[pair.prefix+"/second"]
		if first.protocol != pair.protocol || second.protocol != pair.protocol {
			t.Errorf("%s protocols = %q, %q; want %s for both requests", pair.prefix, first.protocol, second.protocol, pair.protocol)
		}
		if first.remote == "" || first.remote != second.remote {
			t.Errorf("%s sockets = %q, %q; want one stable socket", pair.prefix, first.remote, second.remote)
		}
	}
	if observed["/h1/first"].remote == observed["/h2/first"].remote {
		t.Errorf("mixed observations = %v, want separate sockets for recorded connections", observed)
	}
}

func TestRecordedProtocolMetadataNormalization(t *testing.T) {
	for _, tt := range []struct{ recorded, canonical string }{
		{" \thTtP/1.1 \n", "HTTP/1.1"},
		{" hTtP/2 ", "HTTP/2.0"},
		{"\tHTTP/2.0\n", "HTTP/2.0"},
	} {
		t.Run(tt.recorded, func(t *testing.T) {
			observed := make(chan string, 2)
			srv := startProtocolServer(t, false, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				observed <- r.Proto
				w.WriteHeader(http.StatusNoContent)
			}))
			summary, err := runReplay(protocolReplayEngine(), []model.Event{
				protocolRequest(t, srv.URL, tt.recorded, 1, 1, "/normalized"),
				protocolRequest(t, srv.URL, tt.canonical, 1, 2, "/canonical"),
			})
			if err != nil || summary.Outcome != RunSuccess || summary.ResponsesReceived != 2 {
				t.Fatalf("ReplayStream(%q then %q) = %+v, error %v; want two successful responses", tt.recorded, tt.canonical, summary, err)
			}
			for range 2 {
				select {
				case got := <-observed:
					if got != tt.canonical {
						t.Errorf("server protocol for %q = %q, want %q", tt.recorded, got, tt.canonical)
					}
				default:
					t.Fatal("successful replay did not reach both intended handlers")
				}
			}
		})
	}
}

func TestRecordedProtocolMetadataFailureIsolatedFromOtherConnections(t *testing.T) {
	for _, invalid := range []string{"", "HTTP/1.0", "HTTP/3", "prefixHTTP/2", "HTTP/2.1"} {
		t.Run(invalid, func(t *testing.T) {
			var rejectedRequests, healthyRequests atomic.Int64
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/healthy" {
					healthyRequests.Add(1)
				} else {
					rejectedRequests.Add(1)
				}
				w.WriteHeader(http.StatusNoContent)
			}))
			t.Cleanup(srv.Close)
			summary, err := runReplay(protocolReplayEngine(), []model.Event{
				protocolRequest(t, srv.URL, invalid, 1, 1, "/invalid"),
				protocolRequest(t, srv.URL, "HTTP/1.1", 1, 2, "/aborted"),
				protocolRequest(t, srv.URL, "HTTP/1.1", 2, 1, "/healthy"),
			})
			if err != nil {
				t.Fatalf("ReplayStream(protocol %q) error: %v", invalid, err)
			}
			if summary.Outcome != RunFailed || summary.ProtocolFailed != 1 || summary.SendErrors != 0 || summary.ValidationFailed != 0 || summary.ResponsesReceived != 1 {
				t.Errorf("ReplayStream(protocol %q) summary = %+v, want one protocol failure and one healthy response", invalid, summary)
			}
			if rejectedRequests.Load() != 0 || healthyRequests.Load() != 1 {
				t.Errorf("application invalid/healthy requests = %d/%d, want 0/1", rejectedRequests.Load(), healthyRequests.Load())
			}
			var failure *ConnectionResult
			for i := range summary.ConnectionResults {
				if summary.ConnectionResults[i].ConnectionID == 1 {
					failure = &summary.ConnectionResults[i]
				}
			}
			if failure == nil || failure.ProtocolFailed != 1 || len(failure.Requests) != 1 {
				t.Fatalf("failed connection result = %+v, want one retained protocol failure", failure)
			}
			got := failure.Requests[0]
			if got.Node != "protocol-test" || got.ConnectionID != 1 || got.Sequence != 1 || got.Outcome != RequestProtocolFailed {
				t.Errorf("retained failure identity/outcome = %+v, want protocol-test connection 1 sequence 1 protocol_failed", got)
			}
			if got.Destination != srv.URL+"/invalid" || got.ObservedProtocol != invalid || got.ExpectedProtocol == "" || got.Error == "" {
				t.Errorf("retained failure diagnostic = %+v, want destination, expected protocol, rejected metadata %q and error", got, invalid)
			}
		})
	}
}

func TestRecordedProtocolChangeAbortsBeforeSendingInconsistentRequest(t *testing.T) {
	var requests atomic.Int64
	srv := startProtocolServer(t, true, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.URL.Path != "/first" || r.Proto != "HTTP/1.1" {
			t.Errorf("application received %s over %s, want only /first over HTTP/1.1", r.URL.Path, r.Proto)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	summary, err := runReplay(protocolReplayEngine(), []model.Event{
		protocolRequest(t, srv.URL, "HTTP/1.1", 1, 1, "/first"),
		protocolRequest(t, srv.URL, "HTTP/2", 1, 2, "/inconsistent"),
		protocolRequest(t, srv.URL, "HTTP/1.1", 1, 3, "/after-abort"),
	})
	if err != nil || summary.Outcome != RunFailed || summary.ProtocolFailed != 1 || summary.ResponsesReceived != 1 || requests.Load() != 1 {
		t.Fatalf("inconsistent ReplayStream() = %+v, error %v, application requests %d; want one response then protocol failure", summary, err, requests.Load())
	}
	if len(summary.ConnectionResults) != 1 || len(summary.ConnectionResults[0].Requests) != 1 {
		t.Fatalf("inconsistent connection results = %+v, want one retained failed request", summary.ConnectionResults)
	}
	got := summary.ConnectionResults[0].Requests[0]
	if got.Sequence != 2 || got.ExpectedProtocol != "HTTP/1.1" || got.Destination != srv.URL+"/inconsistent" {
		t.Errorf("inconsistent request diagnostic = %+v, want sequence 2 expected HTTP/1.1 at inconsistent destination", got)
	}
}

func TestRecordedHTTP2NetworkFailureRemainsSendError(t *testing.T) {
	for _, scheme := range []string{"http", "https"} {
		t.Run(scheme, func(t *testing.T) {
			target := scheme + "://" + closedLocalAddress(t)
			summary, err := runReplay(protocolReplayEngine(), []model.Event{protocolRequest(t, target, "HTTP/2", 1, 1, "/unreachable")})
			if err != nil || summary.Outcome != RunPartialSuccess || summary.SendErrors != 1 || summary.ProtocolFailed != 0 {
				t.Errorf("ReplayStream(unreachable %s HTTP/2 target) = %+v, error %v; want ordinary send error, not protocol failure", scheme, summary, err)
			}
		})
	}
}

type protocolResponseBody struct {
	io.Reader
	closed bool
}

func (b *protocolResponseBody) Close() error {
	b.closed = true
	return nil
}

func TestRecordedProtocolGuardChecksReplacementResponses(t *testing.T) {
	eng := protocolReplayEngine()
	client, transport := eng.makePerConnectionClient(true)
	t.Cleanup(transport.CloseIdleConnections)
	var attempts int
	replacedBody := &protocolResponseBody{Reader: strings.NewReader("unexpected HTTP/1 response")}
	guard := client.Transport.(*redirectCheckingTransport)
	guard.RoundTripper = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		attempts++
		if attempts == 1 {
			return &http.Response{StatusCode: http.StatusOK, Proto: "HTTP/2.0", ProtoMajor: 2, ProtoMinor: 0, Header: make(http.Header), Body: http.NoBody, Request: req}, nil
		}
		return &http.Response{StatusCode: http.StatusOK, Proto: "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1, Header: make(http.Header), Body: replacedBody, Request: req}, nil
	})
	resp, err := client.Get("http://replacement.test/first")
	if err != nil {
		t.Fatalf("initial HTTP/2 response error: %v", err)
	}
	if err := resp.Body.Close(); err != nil {
		t.Errorf("initial response Close() error: %v", err)
	}
	resp, err = client.Get("http://replacement.test/second")
	if resp != nil {
		if closeErr := resp.Body.Close(); closeErr != nil {
			t.Errorf("replacement response Close() error: %v", closeErr)
		}
		t.Errorf("replacement HTTP/1 response = %+v, want no response", resp)
	}
	var fidelityErr *protocolFidelityError
	if !errors.As(err, &fidelityErr) {
		t.Fatalf("replacement response error = %v, want protocol fidelity error", err)
	}
	if fidelityErr.expected != "HTTP/2.0" || fidelityErr.observed != "HTTP/1.1" || fidelityErr.destination != "http://replacement.test/second" {
		t.Errorf("replacement protocol diagnostic = %+v, want HTTP/2.0 expected and HTTP/1.1 observed at second request", fidelityErr)
	}
	if !replacedBody.closed {
		t.Error("mismatched replacement response body was not closed")
	}
}

func startEarlyH2CFailure(t *testing.T, reply string) (string, context.Context, *atomic.Int64) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen() error: %v", err)
	}
	var attempts atomic.Int64
	clientClosed := make(chan struct{}, 3)
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			attempts.Add(1)
			if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
				t.Errorf("accepted connection SetDeadline() error: %v", err)
				_ = conn.Close()
				return
			}
			var preface [24]byte
			if _, err := io.ReadFull(conn, preface[:]); err != nil {
				t.Errorf("read HTTP/2 preface: %v", err)
				_ = conn.Close()
				return
			}
			if got, want := string(preface[:]), "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"; got != want {
				t.Errorf("client preface = %q, want %q (no HTTP/1 application request)", got, want)
			}
			if _, err := io.WriteString(conn, reply); err != nil {
				t.Errorf("write early server reply: %v", err)
				_ = conn.Close()
				return
			}
			if err := conn.(*net.TCPConn).CloseWrite(); err != nil {
				t.Errorf("close server write side: %v", err)
			}
			// EOF proves the HTTP/2 read loop processed the reply (or EOF)
			// and closed the socket before GotConn allows the first stream.
			if _, err := io.Copy(io.Discard, conn); err != nil {
				t.Errorf("wait for failed client connection to close: %v", err)
			}
			_ = conn.Close()
			clientClosed <- struct{}{}
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		<-serverDone
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
		GotConn: func(httptrace.GotConnInfo) {
			select {
			case <-clientClosed:
			case <-ctx.Done():
				t.Errorf("wait for early HTTP/2 connection failure: %v", ctx.Err())
			}
		},
	})
	return "http://" + listener.Addr().String(), ctx, &attempts
}

func TestRecordedHTTP2EarlyCleartextRejectionFailsRun(t *testing.T) {
	target, ctx, attempts := startEarlyH2CFailure(t, "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
	eng := protocolReplayEngine()
	eng.cfg.Replay.Retry.MaxAttempts = 3
	eng.cfg.Replay.Retry.RetryOnErrors = []string{"network"}
	events := make(chan model.Event, 1)
	events <- protocolRequest(t, target, "HTTP/2", 1, 1, "/intended")
	close(events)
	summary, err := eng.ReplayStream(ctx, events, nil)
	if err != nil {
		t.Fatalf("ReplayStream(early HTTP/1 rejection) error: %v", err)
	}
	if summary.Outcome != RunFailed || summary.ProtocolFailed != 1 || summary.SendErrors != 0 || summary.ResponsesReceived != 0 {
		t.Errorf("ReplayStream(early HTTP/1 rejection) = %+v, want failed with one protocol failure and no send errors or responses", summary)
	}
	if got := attempts.Load(); got != 1 {
		t.Errorf("HTTP/2 connection attempts = %d, want 1 despite configured network retries", got)
	}
	if len(summary.ConnectionResults) != 1 || len(summary.ConnectionResults[0].Requests) != 1 {
		t.Fatalf("connection results = %+v, want one retained protocol failure", summary.ConnectionResults)
	}
	failure := summary.ConnectionResults[0].Requests[0]
	if failure.ExpectedProtocol != model.ProtocolHTTP2 || failure.ObservedProtocol != model.ProtocolHTTP11 || failure.Destination != target+"/intended" {
		t.Errorf("protocol failure = %+v, want HTTP/2.0 expected and HTTP/1.1 observed at %s/intended", failure, target)
	}
}

func TestRecordedHTTP2EarlyCleartextEOFRemainsSendError(t *testing.T) {
	target, ctx, attempts := startEarlyH2CFailure(t, "")
	eng := protocolReplayEngine()
	eng.cfg.Replay.Retry.MaxAttempts = 3
	eng.cfg.Replay.Retry.RetryOnErrors = []string{"network"}
	events := make(chan model.Event, 1)
	events <- protocolRequest(t, target, "HTTP/2", 1, 1, "/intended")
	close(events)
	summary, err := eng.ReplayStream(ctx, events, nil)
	if err != nil {
		t.Fatalf("ReplayStream(early EOF without protocol evidence) error: %v", err)
	}
	if summary.Outcome != RunPartialSuccess || summary.SendErrors != 1 || summary.ProtocolFailed != 0 {
		t.Errorf("ReplayStream(early EOF without protocol evidence) = %+v, want an ordinary send error, not a protocol failure", summary)
	}
	if got := attempts.Load(); got != 3 {
		t.Errorf("early EOF connection attempts = %d, want 3 configured network attempts", got)
	}
}
