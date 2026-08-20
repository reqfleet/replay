# Development environment

Replay development commands run inside the Lima VM.

- From the host repository, run `make devbox-ssh` before running any project `make` target or `go test` command.
- Run project `make ...` and `go test ...` commands only from the resulting VM shell in `/workspace/replay`; never run them directly on the macOS host.
- `make devbox` and `make devbox-ssh` are host-only exceptions because they start and enter the VM.
- Host-side repository operations such as editing files and running `git` or `gh` do not require the VM.
