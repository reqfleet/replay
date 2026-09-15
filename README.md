# Replay

Replay helps engineers to reproduce production traffic patterns. As a start, Replay replays the traffic based on Envoy logs.

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
* Pacing is enabled by default, preserving start offsets between connections
  and request gaps within each connection on one replay engine.
* Request bodies are replayed when captured. If body capture is a performance concern, the user can modify the generated logs with an artificial body.
* If you do not want to add the `DownstreamStart` logs to your Envoy access logs and want to keep the changes to the log configuration to a minimum, you can replay with the `DownstreamEnd`-only logs. However, this will reduce the fidelity of the replay, for example, by losing the correct order of requests arriving at Envoy.

More about fidelity can be read in the [replay guide](docs/guide.md#replay-fidelity). Topics
such as capture limitations, protocol requirements, and timing behavior will be discussed in detail.

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

### Docker

Pull an image from GitHub Container Registry using a release tag, for example:

```bash
docker pull ghcr.io/reqfleet/replay:v0.1.1
```

Images support Linux on `amd64` and `arm64`. Available versions are listed on
the [releases page](https://github.com/reqfleet/replay/releases).

## Quickstart

The commands below assume the downloaded binary is saved as `./replay`.
No configuration file is required; these commands use Replay's built-in defaults.

1. [Capture paired Envoy access logs](docs/guide.md#recording-traffic) as
   `requests.log`, then combine them into canonical replay input:

   ```bash
   ./replay combine \
     -log ./requests.log \
     -out ./canonical.ndjson
   ```

2. Replay supports dry runs. It will go through the logs without sending the requests. This is optional for operational safety:

   ```bash
   ./replay \
     -log ./canonical.ndjson \
     --dry-run
   ```

3. Replay by sending the requests out:

   ```bash
   ./replay \
     -log ./canonical.ndjson
   ```


### Before a live run

* If you do not want to send the traffic to the targets in the recorded logs, you can replace them with the following flags: ` --override-url http://staging.example --disallow-recorded-targets`. Replay will send the requests to the staging example instead.
* The default configuration blocks `POST`, `PUT`, `PATCH`, and `DELETE`
  unless the effective request contains `idempotency-key` or `x-idempotency-key`. The idempotency protection could also be turned off using a [customised configuration file](/docs/guide.md#replay-configuration).
* Recorded `authorization` and `cookie` headers are removed by default. Use
  [header rewrite settings](docs/guide.md#replay-configuration) in an optional
  configuration file to supply credentials for the replay target.


See the [operator checklist](docs/guide.md#operator-checklist) for details.

## Documentation

| Document | Contents |
| --- | --- |
| [Replay guide](docs/guide.md) | Recording, target compatibility, configuration, safety, outcomes, metrics, Go library usage, and request-body recipes. |
| [Development](docs/development.md) | Linux and macOS setup, Lima lifecycle, tests, and fixture generation. |
| [Specification](specs.md) | Normative input formats, replay semantics, and configuration contract. |
| [Example configuration](config.yaml) | Ready-to-use replay safety, timing, retry, validation, and metrics settings. |
