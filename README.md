# Replay

Replay reconstructs HTTP traffic from Envoy access logs so capacity tests can
use representative production request mixes and connection behavior. By
default, Replay schedules requests on a shared recorded timeline within each
replay process.

## Motivation and Design Goal

Most load tests use synthetic traffic. Tools such as JMeter and Locust can
generate large volumes of traffic, but synthetic workloads often lack a
critical property: fidelity to production traffic.

Production traffic patterns are visible in proxy and sidecar logs. Replay uses
those logs to reproduce real traffic. Envoy is one of the industry's most
widely used proxies, so it is Replay's initial capture source.

Simply put, Replay reproduces traffic from Envoy access logs.

### Replay fidelity

Replay uses existing Envoy access logs rather than packet captures, balancing
capture overhead with fidelity to production HTTP traffic.

* Paired `DownstreamStart` and `DownstreamEnd` observations preserve recorded
  request-start order, downstream connection grouping, and connection-close
  signals when available.
* Recorded HTTP/1.1 and HTTP/2 are preserved without protocol fallback.
  Serialized HTTP/2 changes concurrency, not the wire protocol.
* Pacing is enabled by default, preserving start offsets between connections
  and request gaps within each connection on one replay engine.
* Request bodies are replayed when captured. Body-dependent requests need
  those payloads, or deliberately substituted artificial bodies.
* End-only logs are supported with lower fidelity: they preserve completion
  order rather than request-start order and cannot determine exact connection
  close timing.

Replay focuses on HTTP behavior, not raw packets, header ordering, or exact TLS
handshakes and server responses. See the [replay guide](docs/guide.md#replay-fidelity)
for capture limitations, protocol requirements, and timing behavior.

## Download

### Replay binary

Download the binary for your operating system and architecture from the
[latest GitHub release](https://github.com/reqfleet/replay/releases/latest):

* Linux: `replay-linux-amd64` or `replay-linux-arm64`.
* Windows: `replay-windows-amd64.exe` or `replay-windows-arm64.exe`.
* macOS (Apple Silicon): `replay-darwin-arm64`.

For example, download and run the Linux `amd64` binary:

```bash
curl -fL https://github.com/reqfleet/replay/releases/latest/download/replay-linux-amd64 -o replay
chmod +x replay
./replay -help
```

### Docker image

Pull an image from GitHub Container Registry using a release tag, for example:

```bash
docker pull ghcr.io/reqfleet/replay:v0.1.1
```

Images support Linux on `amd64` and `arm64`. Available versions are listed on
the [releases page](https://github.com/reqfleet/replay/releases).

## Quickstart

The commands below assume the downloaded binary is saved as `./replay`.
Copy the supplied [`config.yaml`](config.yaml) into the working directory and
review it for your target's safety policy.

1. [Capture paired Envoy access logs](docs/guide.md#recording-traffic) as
   `requests.log`, then combine them into canonical replay input:

   ```bash
   ./replay combine \
     -log ./requests.log \
     -out ./canonical.ndjson
   ```

2. Choose an isolated staging target and validate the input without sending
   requests:

   ```bash
   ./replay \
     -log ./canonical.ndjson \
     -config ./config.yaml \
     --override-url http://staging.example \
     --disallow-recorded-targets \
     --dry-run
   ```

3. Replay against the same target:

   ```bash
   ./replay \
     -log ./canonical.ndjson \
     -config ./config.yaml \
     --override-url http://staging.example \
     --disallow-recorded-targets
   ```

The target must support the recorded HTTP version. `--override-url` changes
the destination and URL scheme, not the HTTP version. Envoy's downstream and
upstream connections may use different protocols, including HTTPS terminated
at Envoy and plaintext HTTP to the application. See
[Choosing a replay target](docs/guide.md#choosing-a-replay-target) before
replaying directly against an application.

### Before a live run

* Keep `--override-url` and `--disallow-recorded-targets` set so replay cannot
  fall back to destinations stored in the capture.
* Run with `--dry-run` first. It parses input without sending requests but
  still exercises recorded timing unless pacing is disabled.
* The supplied configuration blocks `POST`, `PUT`, `PATCH`, and `DELETE`
  unless the effective request contains `idempotency-key` or `x-idempotency-key`.
  Use keys honored by the target; adjust safeguards only for a safe test
  environment. Replay can mutate application state.
* The supplied configuration uses `./checkpoint.json` for resumable replay.
  Reusing it skips sequences already recorded as complete. Remove it or
  choose a new checkpoint path for an independent run.

See the [operator checklist](docs/guide.md#operator-checklist) for details.

## Documentation

| Document | Contents |
| --- | --- |
| [Replay guide](docs/guide.md) | Recording, target compatibility, configuration, safety, outcomes, metrics, Go library usage, and request-body recipes. |
| [Development](docs/development.md) | Linux and macOS setup, Lima lifecycle, tests, and fixture generation. |
| [Specification](specs.md) | Normative input formats, replay semantics, and configuration contract. |
| [Example configuration](config.yaml) | Ready-to-use replay safety, timing, retry, validation, and metrics settings. |
