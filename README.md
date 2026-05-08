# tether

Distributed node control: an "SSH + 端口暴露" control plane for nodes behind
NAT, with a single public broker. Designed in `docs/architecture.md`.

## Status

Pre-alpha (phase **P3** — sessions + login). On top of P2's heartbeat loop,
adds session CRUD (`tether session create / ls / rm`), per-user nkey
identity (`tether login`), and the broker handlers for
`ctrl.by.<actor>.session.*.req`. Multi-session isolation is per CLI shell
via the `TETHER_SESSION` env var.

Note: NATS-level JWT permission enforcement (auth_callout, architecture
B.2 / E.2) is staged for a P3 follow-up — clients connect anonymously to
NATS today; the actor token in subjects is a routing label, not yet proof
of identity. Application-layer owner / member / PIN checks ARE enforced
by the broker.

Full control plane (`run` / `exec` / `expose`) lands in P4+.

## Build

```bash
make build
./bin/tether version
```

Requires Go 1.25+ (pinned by `go.mod` because `github.com/nats-io/jwt/v2` ≥ v2.8.1 needs it).

## Development tools

`make lint` requires [`golangci-lint`](https://github.com/golangci/golangci-lint).
Install the version pinned by CI:

```bash
make tools         # go install ...@v1.62.2
make lint
```

## P2 quick-start (manual)

```bash
make nats-server-install        # one-time install of nats-server binary
make nats-dev                   # terminal A: starts nats-server -js

make build && ./bin/tether serve --db ./tether.db          # terminal B
./bin/tether agent --session lab --nid lab-1               # terminal C
./bin/tether admin nodes --db ./tether.db                  # terminal D

# kill agent in C → wait 5s → admin nodes shows STALE
# wait 60s total → admin nodes shows OFFLINE
```

The Go test suite does **not** require an external `nats-server`; it embeds
one via `nats-server/v2/test`.
