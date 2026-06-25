package engine

import (
	"testing"

	"github.com/reqfleet/replay/internal/config"
	"github.com/reqfleet/replay/internal/model"
)

func TestIdempotencyPolicyInfersPostForBodyHeaders(t *testing.T) {
	cfg := config.Default()
	cfg.Replay.Idempotency.Enabled = true
	cfg.Replay.Idempotency.BlockMethods = []string{"POST"}
	cfg.Replay.Idempotency.RequireHeaderForAllow = []string{"x-idempotency-key"}

	eng := New(cfg, nil)
	req := model.Event{
		HTTP: model.HTTPRequestMeta{
			Scheme:    "http",
			Authority: "example.invalid",
			Path:      "/",
		},
		Headers: map[string][]string{"content-type": {"text/plain"}},
	}

	if got := eng.shouldSkipByIdempotencyPolicy(req); !got {
		t.Fatalf("shouldSkipByIdempotencyPolicy(%+v) = %t, want true", req, got)
	}
}
