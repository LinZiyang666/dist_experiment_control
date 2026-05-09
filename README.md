# tether

Distributed node control: an "SSH + 端口暴露" control plane for nodes behind
NAT, with a single public broker. Designed in `docs/architecture.md`.

## Status

Pre-alpha (phase **P5 complete**). Adds interactive PTY mode on top of P4:

- `tether run <node> -- <argv>` runs argv interactively on the named
  agent node with a PTY allocated. Local terminal goes into raw mode
  so keys / Ctrl sequences / cursor moves flow through; SIGWINCH
  resizes are forwarded; local Ctrl-C is delivered as SIGINT to the
  remote process group.
- Two-phase attach handshake (architecture C.5.1): agent allocates a
  PTY without exec, replies `RunChunk{Kind:ready}`; ctl subscribes
  `pty.<pid>.out` and publishes `pty.<pid>.attach`; only then agent
  forks+execs. First byte of remote output cannot be lost.
- 3-second attach deadline: agent that doesn't see attach in time
  publishes `pty.<pid>.failed{reason:attach_timeout}`, releases the
  PTY, and replies `RunChunk{Kind:failed}` to ctl. broker writes
  `audit.proc{kind:attach_timeout}`.
- `tether exec <node> -- <argv>` (P4) for non-interactive commands;
  streams stdout/stderr and propagates the remote exit code.
- `tether ps [-a]` lists processes in the active session (RUNNING by
  default; `-a` includes EXITED).
- broker forwards `cmd.by.<actor>.node.<nid>.<verb>.req` →
  `cmd.node.<nid>.<verb>.req.forwarded` for verb ∈ {exec, run, kill}
  while preserving the original reply inbox; agent only subscribes to
  `.forwarded` (architecture C.4).
- `internal/proc` owns the SQLite process row, written from agent's
  `ev.proc.<pid>.{started,exit}` events. broker also writes
  `audit.call` / `audit.proc` (single-writer rule, C.1 §4).

P3 features still apply:

- ctl CONNECTs use `nats.Nkey` + signed challenge by default; broker
  subscribes to `$SYS.REQ.USER.AUTH` and issues per-connection user
  JWTs that pin `by.<actor>`.
- Multi-session isolation per CLI shell via `TETHER_SESSION`.

P4 review F1 closed: agents own their per-`(machine, sid)` nkey at
`~/.tether/agent/<sid>/keys/agent.nk` (architecture K.1) and connect
via `nats.Nkey` + Name `tether-agent:<sid>:<nid>`. The first connect
supplies `--pin` to bind the agent fp into `agent_provisioning`;
subsequent connects validate against that binding. The `roleAgent`
auth_callout branch is no longer hard-denied. `tether serve
--auth-callout-seeds-dir <dir>` enables the secure path on the daemon
side (loads `broker.nk` + `account.nk`).

Local laptop demo: set `TETHER_DEV_NO_AUTH=1` (CLI-side env, applies
to both `tether agent` and the ctl commands) to connect anonymously
to a vanilla `nats-server`. NATS-level identity enforcement is
bypassed in that mode; never use it in production.

Port `expose` (frp data plane for jupyter / tensorboard) lands in P6.

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
