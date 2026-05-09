# tether

Distributed node control: an "SSH + 端口暴露" control plane for nodes behind
NAT, with a single public broker. Designed in `docs/architecture.md`.

## Status

Pre-alpha (phase **P7 complete**). Adds JetStream-backed audit + history
+ 3-phase session rm on top of P6:

- All `audit.{call,proc,port}` events are now `js.Publish` to a
  per-session `history-<sid>` JetStream stream (architecture H.1 / H.5).
  When the underlying nats-server has JetStream enabled the broker
  auto-detects it and switches; without JS it falls back to core
  publish (P4-P6 behavior, no test regressions).
- `tether history [-n N] [--follow] [--kind call|proc|port]` replays
  audit history via an ephemeral consumer. -n is bounded last-N,
  --follow tails new entries, --kind filters call/proc/port.
- `session rm` runs the full architecture H.3 three-phase: ① SQLite
  tombstone (state=DELETING, C.1 §6 starts rejecting new req
  immediately) → ② JetStream DELETE history-`<sid>` → ③ SQLite
  cascade-delete dependent rows → ④ pub `sys.events{session_destroyed}`.
- Broker boot reconciles: re-runs phases ②③ for sessions stuck in
  DELETING (crash recovery), creates missing `history-<sid>` for
  ACTIVE sessions, deletes orphan `history-*` streams whose SQLite
  session is gone.
- Global `events` JetStream stream (30d / 1 GiB / discard=old) carries
  `sys.events`. P7 emits `session_created` / `session_destroyed` so
  far; member_joined / pin_failed / disk_pressure plug in at the
  same `pubSysEvent` helper when their feature lands.

P6 features still apply:

- `tether expose <node> --local 8888 --name jupyter` allocates a
  public port from the broker's [14000-14999] band, generates a
  one-time token, plumbs it to the agent, and prints the URL
  (`http://<broker>:14022`). The agent's `internal/tunnel` client opens
  a yamux session to the broker over the configured tunnel control
  port (default `:7000`); when external traffic hits 14022, broker
  multiplexes a new stream over the session and the agent bridges it
  to its local 8888.

- `tether expose <node> --local 8888 --name jupyter` allocates a
  public port from the broker's [14000-14999] band, generates a
  one-time token, plumbs it to the agent, and prints the URL
  (`http://<broker>:14022`). The agent's `internal/tunnel` client opens
  a yamux session to the broker over the configured tunnel control
  port (default `:7000`); when external traffic hits 14022, broker
  multiplexes a new stream over the session and the agent bridges it
  to its local 8888.
- `tether expose rm <node> --name jupyter` immediately tears down the
  proxy and returns the public port number to the pool.
- `tether ps` upgrades to the F.8 unified view (PROCESSES + PORTS in
  one screen).
- `internal/port` owns the `port_allocations` table; broker-side
  reconciler tick promotes `ALLOCATED → REVOKED` for ports owned by
  long-OFFLINE nodes (architecture D.4 / F.6, default 15min).
- Agent persists `(name, port, local_port, token)` rows to
  `~/.tether/agent/<sid>/state.json` (architecture I.2 / K.1, mode
  0600, atomic tmp+rename) so the tunnel auto-reconnects on agent
  restart without a re-expose.
- Architecture deviation: spec calls for embedding the `frp` Go
  library and shipping `frpc` as a subprocess; we use a minimal
  in-process yamux-over-TCP tunnel (`internal/tunnel`, ~300 LOC) for
  identical behavior with vastly smaller dep footprint. Swappable if
  frp's wider feature set (HTTP vhost, kcp, web admin) is ever needed.

P5 features still apply:

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
  `cmd.node.<nid>.<verb>.req.forwarded` for verb ∈ {exec, run, kill,
  expose, expose-rm} while preserving the original reply inbox; agent
  only subscribes to `.forwarded` (architecture C.4).
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

Reconciliation (G.1 / G.2 — agent reconnect snapshot, broker restart
DELETING resume already in P7, process LOST detection) lands in P8.

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
