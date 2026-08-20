---
name: address-pr-comments
description: Guides fetching, triaging, and addressing PR review comments for the reqfleet/replay repository.
---

# Address PR Comments

Use when: you need to fetch PR review comments for `reqfleet/replay`, triage them, apply fixes, and create commits that address reviewer feedback.

Prerequisites
- `gh` CLI installed and authenticated (access to `reqfleet/replay`).
- `jq` installed for JSON parsing.

Execution environment
- From the host repository, run `make devbox-ssh` before any project `make` target or `go test` command.
- Run those commands only in the resulting VM shell at `/workspace/replay`. The `make devbox`, `make devbox-ssh`, and `make devbox-stop` lifecycle targets are host-only.

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

2. Skip comments that are already resolved; focus only on unresolved feedback that still needs action.

3. For each remaining comment, create a small, focused commit addressing that thread. Use conventional commit messages that reference the PR and thread when helpful, e.g.:

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

Testing & verification
- Run tests and linters before committing:

```bash
# Host: enter the replay development VM.
make devbox-ssh

# VM (/workspace/replay): run verification here.
go test ./...
gofmt -w .
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
