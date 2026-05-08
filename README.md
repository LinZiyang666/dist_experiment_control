# tether

Distributed node control: an "SSH + 端口暴露" control plane for nodes behind
NAT, with a single public broker. Designed in `docs/architecture.md`.

## Status

Pre-alpha (phase **P4 complete**). Adds the non-interactive control
plane on top of P3:

- `tether exec <node> -- <argv>` runs argv on the named agent node,
  streaming stdout/stderr back via the request's reply inbox, and
  propagates the remote exit code as the local exit code.
- `tether ps [-a]` lists processes in the active session (RUNNING by
  default; `-a` includes EXITED).
- broker forwards `cmd.by.<actor>.node.<nid>.exec.req` →
  `cmd.node.<nid>.exec.req.forwarded` while preserving the original
  reply inbox; agent only subscribes to `.forwarded` (architecture C.4).
- `internal/proc` owns the SQLite process row, written from agent's
  `ev.proc.<pid>.{started,exit}` events. broker also writes
  `audit.call` / `audit.proc` (single-writer rule, C.1 §4).

P3 features still apply:

- ctl CONNECTs use `nats.Nkey` + signed challenge by default; broker
  subscribes to `$SYS.REQ.USER.AUTH` and issues per-connection user
  JWTs that pin `by.<actor>`.
- Multi-session isolation per CLI shell via `TETHER_SESSION`.
- Agent role in `auth_callout` is hard-denied until P4 ships real
  agent provisioning (architecture K.1) — not yet on this commit.

Local laptop demo: set `TETHER_DEV_NO_AUTH=1` (CLI-side only) to
connect anonymously to a vanilla `nats-server`. NATS-level identity
enforcement is bypassed in that mode; never use it in production.

PTY mode (`tether run` for vim/htop/progress bars) lands in P5.

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
