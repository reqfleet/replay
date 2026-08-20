---
name: issue-to-development
description: "Fetch a GitHub issue and start a development branch to address it. Use when the user asks the agent to fetch an issue, create a branch named with the issue number, and begin implementation."
---

# Issue → Development

Use when: the user asks the agent to pick up or start work on a `reqfleet/shibuya` issue (e.g. "work on issue 745", "start issue #745").

## Summary

This skill fetches a GitHub issue (title, body, comments), ensures the local repository is on `main` (asking for confirmation if it is not), checks out a new branch named with the issue number, and guides the agent through starting development. Commits must be created via the `commit` skill — do not create PRs automatically.

Note: Shibuya is a monorepo; it contains `gui` (a Next.js project) and the control plane implementation written in Go.

## Commands (examples)

- Fetch issue JSON:

	gh issue view --repo reqfleet/shibuya {issue_number} --json title,body,comments

- Check current branch:

	git rev-parse --abbrev-ref HEAD

- Switch to main (ask first if not on main):

	git checkout main && git pull --ff-only

- Create a new branch named with the issue number:

	git checkout -b <type>-issue-{issue_number}

## Workflow

1. If no `issue_number` parameter is provided, ask the user: "Which `reqfleet/shibuya` issue number should I work on?"
2. Run the GH command above to fetch `title`, `body`, and `comments`. If the `gh` CLI returns an error (not installed or not authenticated), report the error and ask the user to authenticate with `gh auth login`.
3. Summarize the issue (title, brief body, and top-level comments) to the user and ask for confirmation to proceed.
4. Check the current git branch using `git rev-parse --abbrev-ref HEAD`:
	 - If already on `main`, continue.
	 - If not on `main`, ask: "Current branch is `{branch}`. Should I switch to `main` and pull the latest?" Do not switch without explicit agreement.
5. If the user agrees, run `git checkout main` and `git pull --ff-only`.
6. Create and switch to a new branch following the <type>-issue-{issue_number} pattern (e.g., feat-issue-{issue_number}). Determine <type> from issue labels (e.g., bug -> fix, enhancement -> feat). If a branch with that name already exists, ask the user if they want to resume work on it or create a new one with a unique suffix.
7. Begin development: open the relevant files, implement changes, and run project-specific checks (linters, static type checks, unit tests). Examples:
	 - Go: From the host repository, run `make devbox-ssh`, then run all project `make` targets and `go test` commands inside the VM at `/workspace/replay`. Do not run them directly on the host. Consult the replay `Makefile` for repository-specific checks such as `make test`, `make e2e`, `make alltests`, or `make build`.
	 - Frontend: `npm run lint`, `npx tsc --noEmit`
	 Adapt checks to the repository or ask the user which checks they prefer.
8. When ready to save progress, call the `commit` skill to create a commit. Suggested commit message pattern:
	 - <type>(issue-{issue_number}): short description
	 - Include `Refs #{issue_number}` in the commit body.
	 Always use the `commit` skill (do not run `git commit` directly) so commits follow repository conventions.
9. Stop after committing. Creating a PR is a separate action and should use the `create-pr` skill when requested.

## Decision Points & Edge Cases

- If the GitHub issue references a different repo, ask the user whether to continue with that repo.
- If `gh` CLI is unavailable or the user is not authenticated, prompt them to run `gh auth login` and provide guidance.
- If tests fail after changes, summarize failures and ask whether to fix them now or commit partial work.

## Example user prompts

- "Start work on issue 745"
- "Please fetch issue 745 and create branch 745"
- "I want you to pick an open issue — which one should I work on?"

## Notes

- This file follows `agent-customization` rules: short `description` frontmatter and clear `Use when` triggers.
- The skill is workspace-scoped to the `shibuya` repository. Adjust repo name in the GH command if invoked for a different repo.
