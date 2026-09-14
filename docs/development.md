# Development

[README](../README.md) · [Replay guide](guide.md) · [Specification](../specs.md)

Replay can be developed directly on Linux with `make` and the Go version
declared in [`go.mod`](../go.mod). Lima also supports Linux, but it is not required
for native Linux development.

## Linux host

Install Go and `make`, then run project targets from the repository root:

```bash
make build
make test
```

## Apple silicon macOS devbox

The repository also provides an Ubuntu VM managed by
[Lima](https://lima-vm.io/) for development on Apple silicon macOS. The bundled
configuration selects Apple's Virtualization framework and an ARM64 image, so
this devbox requires Lima 1.0 or newer and `make` on the macOS host.

From the repository root, run:

```bash
make devbox-ssh
```

This creates or starts the Lima instance named `replay`, mounts the current
checkout read-write at `/workspace/replay`, installs the Go version declared in
`go.mod` through GVM, and opens a shell in that directory. The initial start
downloads and provisions the VM, so it takes longer than subsequent starts.

Run project `make` targets and Go commands from this VM shell, not directly from
the macOS host:

```bash
# VM: /workspace/replay
make build
make test
```

Editing files and running repository commands such as `git` may still be done
on the host because the checkout is shared with the VM. Exit the shell with
`exit`; the VM continues running.

## Tests and checks

Run these commands directly on Linux or inside the VM on macOS:

| Command | Purpose |
| --- | --- |
| `make staticcheck` | Run Staticcheck across all Go packages. |
| `make test` | Run Staticcheck, then all Go tests with the test cache disabled. |
| `go test ./internal/parser -count=1` | Run one package while developing. |
| `make e2e` | Build Replay and exercise the bundled fixtures against a local test server on `localhost:6001`. |
| `make alltests` | Run `make test` and `make e2e`; this is the CI check. |
| `make build` | Build `bin/replay` for the current OS and architecture. |
| `make tidy` | Update `go.mod` and `go.sum` after dependency changes. |

## Generating request fixtures

Run the generator on Linux or inside the VM on macOS:

```bash
go run ./tools/generate_requests.go -method POST \
  -header 'Content-Type: application/json' \
  -body '{"message":"hello"}' -out requests.ndjson
```

`-method` defaults to `GET` and applies to canonical events, `-downstream-end`,
and `-observations` output. Supplying `-body` does not change the method; choose
`POST`, `PUT`, or another method explicitly when appropriate.

## VM lifecycle

Run lifecycle commands from the macOS host that owns the VM:

| Command | Purpose |
| --- | --- |
| `make devbox` | Create or start the VM without opening a shell. |
| `make devbox-ssh` | Create or start the VM and open a shell in `/workspace/replay`. |
| `make devbox-stop` | Stop the VM without deleting it. |
| `make devbox-recreate` | Delete and reprovision the VM. Host checkout files remain; guest-local data is removed. |

The current checkout is mounted by default. To mount a different checkout,
pass its absolute path while recreating the VM:

```bash
make DEVBOX_PROJECT_DIR=/absolute/path/to/replay devbox-recreate
```

Recreate the VM after changing `lima.yaml` or `DEVBOX_PROJECT_DIR`; an existing
instance retains the configuration and mount selected when it was created.

