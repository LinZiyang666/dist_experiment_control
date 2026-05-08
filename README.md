# tether

Distributed node control: an "SSH + 端口暴露" control plane for nodes behind
NAT, with a single public broker. Designed in `docs/architecture.md`.

## Status

Pre-alpha (phase **P0** — scaffold). No runtime functionality yet.

## Build

```bash
make build
./bin/tether version
```

Requires Go 1.23+.

## Development tools

`make lint` requires [`golangci-lint`](https://github.com/golangci/golangci-lint).
Install the version pinned by CI:

```bash
make tools         # go install ...@v1.62.2
make lint
```
