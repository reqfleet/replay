---
name: address-pr-comments
description: Guides fetching, triaging, and addressing PR review comments for the reqfleet/replay repository.
---

# Address PR Comments

Use when: you need to fetch PR review comments for `reqfleet/replay`, triage them, apply fixes, and create commits that address reviewer feedback.

Prerequisites
- `gh` CLI installed and authenticated (access to `reqfleet/replay`).
- `jq` installed for JSON parsing.
- You have a local clone of `reqfleet/replay`.

Quick fetch
- Fetch the PR review JSON (returns a JSON structure containing `reviews` and `comments`):

```bash
gh pr-review review view {pr_number} --repo reqfleet/replay > pr_review.json
```

Useful `jq` snippets
- List review summaries (id, state, author, submitted_at, body):

```bash
jq -r '.reviews[] | "REVIEW: \(.id) \(.state) by \(.author_login) at \(.submitted_at)\n\(.body)\n"' pr_review.json
```

- List file-level comments (path, line, author, body):

```bash
jq -r '.reviews[].comments[] | "\(.path):\(.line) \(.author_login): \(.body)"' pr_review.json
```

- Filter comments flagged as high-priority (markdown contains `![high]`):

```bash
jq -r '.reviews[].comments[] | select(.body|test("!\\[high\\]")) | "\(.path):\(.line) \(.author_login): \(.body)"' pr_review.json
```

Triage rules (suggested)
- Bug / Crash / OOM => `fix`
- Performance / memory / goroutine leaks => `perf`
- Incorrect API / config behavior => `fix` + tests
- Style / lint / formatting => `style`
- Missing or wrong tests => `test`
- Documentation / README => `docs`
- Discussion / clarification => `follow-up`

Branch & workflow
1. Stay on the current branch unless you specifically need to isolate the work elsewhere; there is no need to create a new branch just to address PR comments.

2. For each comment, create a small, focused commit addressing that thread. Use conventional commit messages that reference the PR and thread when helpful, e.g.:

```bash
git add path/to/file.go
git commit -m "fix(engine): limit response body reads to avoid OOM (addresses PR #123)"
```

Example suggested code changes (from the sample review)

- OOM risk when reading response bodies (`io.ReadAll`):

Replace unbounded reads with a limited reader (or skip reading when not needed):

```go
const maxBodyRead = 10 * 1024 * 1024 // 10 MiB
lr := io.LimitReader(resp.Body, maxBodyRead)
respBody, err := io.ReadAll(lr)
if err != nil {
    // handle error
}
// consider logging/trimming if body truncated
```

- Centralize environment/config handling (`REPLAY_PARTIAL_SUCCESS_EXIT_ZERO`):

Add the flag to the `Config` struct and parse it in `ApplyEnv` so all env overrides follow the same flow (example file: `cmd/replay/main.go` + `internal/config`):

```go
type Config struct {
    // ... existing fields
    PartialSuccessExitZero bool `env:"REPLAY_PARTIAL_SUCCESS_EXIT_ZERO"`
}

func (c *Config) ApplyEnv() error {
    // parse and validate env overrides in one place
}
```

- Avoid creating a new `http.Transport` per connection:

Consider reusing transports (a single shared transport or a `sync.Pool` of transports) to reduce goroutine and memory overhead. If per-connection isolation is required, document the trade-off and measure.

- Deduplicate `parseTimestamp`:

Move `parseTimestamp` into a shared package (e.g., `internal/model` or `internal/util/time.go`) and use it across `internal/parser/ndjson.go` and `internal/engine/engine.go`.

- Preserve latency precision:

Use a `float64` value in milliseconds (or seconds) to avoid integer truncation:

```go
latencyMS := time.Since(start).Seconds() * 1000 // float64
```

- Avoid overflow in exponential backoff shifting:

Cap the shift or use `math.Pow` to compute backoff safely:

```go
exp := attempt - 1
if exp > 30 { exp = 30 }
backoff := base << uint(exp)
```

or

```go
backoff := time.Duration(float64(base) * math.Pow(2, float64(attempt-1)))
```

- CPU metric accuracy:

`runtime.NumCPU()` reports logical CPUs, not runtime usage. Either remove the metric, rename it to `logical_cpus_available`, or replace with real process CPU usage via a process metrics library (e.g., `gopsutil` or `go:runtime/metrics`).

Automation helpers (script skeleton)

```bash
#!/usr/bin/env bash
set -euo pipefail
PR=$1
gh pr-review review view "$PR" --repo reqfleet/replay > pr_review.json
jq -r '.reviews[].comments[] | "\(.path):\(.line) \(.author_login): \(.body)"' pr_review.json
# Manually inspect each thread, implement fixes, run tests, commit, push.
```

Testing & verification
- Run tests and linters before committing:

```bash
go test ./...
gofmt -w .
golangci-lint run ./...
```

Committing and pushing
- Follow your project's commit conventions. Prefer small, focused commits that reference the PR and the review thread.
- If you need to share the work, push the current branch or otherwise use the repository's existing PR flow:

```bash
git push
gh pr comment {pr_number} --body "Addressed review comments. PTAL."
```

Notes and limits
- This skill provides a reproducible human-in-the-loop workflow. Fully automated code edits are risky; prefer small, reviewer-visible commits and run CI locally before pushing.
- Marking review threads as resolved typically requires the GitHub UI or API actions that are best done intentionally. Use `gh` or the web UI to resolve threads after pushing fixes.

Questions to ask the user when invoked
- Which PR number should I fetch and triage? (required)
- Do you want me to commit changes for you, or produce suggested patches for manual review?

Examples to try
- `gh pr-review review view 123 --repo reqfleet/replay > pr_review.json` then `jq -r ...` to inspect comments.

Reference: use `agent-customization` skill templates and the repo's testing and commit conventions before creating commits.
