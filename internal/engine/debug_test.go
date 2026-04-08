package engine

import (
	"net/http"
	"strings"
	"testing"

	"github.com/reqfleet/replay/internal/config"
	"github.com/reqfleet/replay/internal/model"
)

func TestDebugIdempotencyMethod(t *testing.T) {
	cfg := config.Default()
	cfg.Replay.Idempotency.Enabled = true
	cfg.Replay.Idempotency.BlockMethods = []string{"POST"}
	cfg.Replay.Idempotency.RequireHeaderForAllow = []string{"x-idempotency-key"}

	req := model.Event{Type: model.EventRequest, ConnectionID: "ck-id", Sequence: 42, HTTP: model.HTTPRequestMeta{Scheme: "http", Authority: "example.invalid", Path: "/"}, Headers: map[string][]string{"content-type": {"text/plain"}}}

	method := strings.ToUpper(strings.TrimSpace(req.HTTP.Method))
	if method == "" {
		method = http.MethodGet
	}
	t.Logf("computed method=%q", method)

	// replicate shouldSkipByIdempotencyPolicy logic
	policy := cfg.Replay.Idempotency
	if !policy.Enabled {
		t.Fatalf("policy not enabled")
	}
	blockedMethods := make(map[string]struct{}, len(policy.BlockMethods))
	for _, m := range policy.BlockMethods {
		blockedMethods[strings.ToUpper(strings.TrimSpace(m))] = struct{}{}
	}
	if _, blocked := blockedMethods[method]; blocked {
		// check allow headers
		allow := false
		for _, headerName := range policy.RequireHeaderForAllow {
			for h := range req.Headers {
				if strings.ToLower(h) == strings.ToLower(headerName) {
					allow = true
				}
			}
		}
		t.Logf("blocked=true allow=%v", allow)
	} else {
		t.Logf("blocked=false")
	}
}
