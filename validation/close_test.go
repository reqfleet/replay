package validation

import (
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/reqfleet/replay/internal/model"
)

const validCanonicalRecord = `{"type":"request","request_id":"request-a","connection_id":1,"timestamp":"2026-02-27T03:10:22Z","method":"GET","authority":"example.com","path":"/","protocol":"HTTP/2","response_code":200}` + "\n"

func TestParseSelectedStreamReturnsCloseError(t *testing.T) {
	closeErr := errors.New("close failed")
	reader := errorReadCloser{
		Reader:   strings.NewReader(validCanonicalRecord),
		closeErr: closeErr,
	}

	err := parseSelectedStream(reader, func(model.Event) error { return nil })
	if !errors.Is(err, closeErr) {
		t.Errorf("parseSelectedStream(valid input) error = %v, want close error", err)
	}
}

func TestParseSelectedStreamJoinsProcessingAndCloseErrors(t *testing.T) {
	processErr := errors.New("processing failed")
	closeErr := errors.New("close failed")
	reader := errorReadCloser{
		Reader:   strings.NewReader(validCanonicalRecord),
		closeErr: closeErr,
	}

	err := parseSelectedStream(reader, func(model.Event) error { return processErr })
	if !errors.Is(err, processErr) {
		t.Errorf("parseSelectedStream(handler error) error = %v, want processing error", err)
	}
	if !errors.Is(err, closeErr) {
		t.Errorf("parseSelectedStream(handler error) error = %v, want close error", err)
	}
}

type errorReadCloser struct {
	io.Reader
	closeErr error
}

func (r errorReadCloser) Close() error {
	return r.closeErr
}
