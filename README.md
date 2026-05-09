# tether

Distributed node control: an "SSH + 端口暴露" control plane for
nodes behind NAT, with a single public broker. Designed in
`docs/architecture.md`.

Single static binary `tether` covers all three roles (broker, agent,
ctl); subcommand selects the mode. Single tarball per (os, arch) +
one `install.sh` does all three deployments. Architecture K.

## Status

Pre-alpha. P0–P11 complete; not publicly released. v0.1.0 release
artifacts (goreleaser-built tarballs + install.sh) are produced on
demand; no public GitHub Release tag has been cut yet.

Implementation phases (architecture Part II):

| Phase | What |
|---|---|
| P0–P3 | scaffolding, NATS auth_callout, sessions, nkey-PKI |
| P4–P5 | `exec` + `run` (PTY) + `ps` + audit |
| P6 | `expose` reverse-TCP tunnel via in-process yamux mux |
| P7 | JetStream-backed audit/history + 3-phase `session rm` |
| P8 | reconciliation (G.1 agent reconnect, G.2 broker restart) |
| P9 | broker.yaml + admin Unix socket (`tether admin *`) |
| P10 | install.sh + goreleaser + `tether node upgrade` (J.4) |
| P11 | release hardening (CI nightly e2e, error-hint walk, this README) |

## Build

```bash
make build
./bin/tether version
```

Requires Go 1.25+ (pinned by `go.mod` because
`github.com/nats-io/jwt/v2` ≥ v2.8.1 needs it).

## Development tools

`make lint` requires [`golangci-lint`](https://github.com/golangci/golangci-lint).
Install the pinned version (Makefile is the single source of truth;
CI installs the same one via `make tools`):

```bash
make tools         # installs golangci-lint v2.5.0 to $GOPATH/bin
make lint
make test          # unit + per-package tests, ~30s
make e2e           # P1-P10 e2e matrix, sequential, ~80s
```

## Install (architecture K)

`scripts/install.sh` is the single entry point for all three roles.
**`install.sh` only lays down files; it never starts anything** —
the operator must run the documented start command afterwards.

### ctl (使用者笔记本)

```bash
curl -fsSL https://<broker>/install.sh | sh
# defaults: --role ctl, drops to /usr/local/bin/tether or
# ~/.local/bin/tether (PATH note printed).
tether login --broker wss://<broker>:443 --session lab --pin <pin>
```

### agent (实验机器)

```bash
curl -fsSL https://<broker>/install.sh | sh -s -- \
  --role agent \
  --broker wss://<broker>:443 \
  --session lab \
  --pin <pin> \
  --nid lab-1
# writes ~/.local/bin/tether and ~/.tether/agent/lab/agent.yaml
# (broker_url + tunnel_addr + session + nid).

# Then start (install does NOT auto-start):
setsid nohup ~/.local/bin/tether agent --session lab --pin <pin> \
  >> ~/.tether/agent/lab/agent.log 2>&1 &

# Or, for systemd --user:
~/.local/bin/tether agent --install-user-service --session lab --nid lab-1
systemctl --user daemon-reload
systemctl --user enable --now tether-agent@lab.service
loginctl enable-linger $USER
```

The agent reads `agent.yaml` for broker URL + tunnel addr — the
`tether agent --session lab` command after install needs no extra
flags.

### broker (运维侧, 需 sudo)

```bash
curl -fsSL https://<broker>/install.sh | sudo sh -s -- \
  --role broker \
  --domain <broker-domain> \
  --acme-email <email>
# installs:
#   /usr/local/bin/tether
#   /usr/local/bin/nats-server (v2.10.22, sha256-verified)
#   /usr/local/bin/caddy       (v2.7.6,  sha256-verified)
#   /etc/tether/{broker.yaml, Caddyfile}
#   /etc/systemd/system/{nats-server, tether-broker, caddy}.service
#   /var/{lib,log,run}/tether (owned by 'tether' user)

sudo systemctl daemon-reload
sudo systemctl enable --now nats-server tether-broker caddy
```

## Daily commands

| Command | Purpose |
|---|---|
| `tether login -s <sid> --pin <pin>` | Bootstrap CLI identity, activate session |
| `tether session create --name <name> --pin <pin>` | New session (you become owner) |
| `tether session list` | Sessions you can see |
| `tether session rm <sid>` | Tombstone + cascade-delete (owner only) |
| `tether ps` | Processes + ports in active session |
| `tether exec <node> -- <argv>` | Non-interactive remote command (streams stdout/stderr, propagates exit code) |
| `tether run <node> -- <argv>` | Interactive PTY remote command (raw mode + SIGWINCH + Ctrl-C) |
| `tether expose <node> --local <port> --name <n>` | Allocate broker public port → agent local port |
| `tether expose rm <node> --name <n>` | Release the public port |
| `tether history -n N [--kind call\|proc\|port] [--follow]` | Tail JetStream audit history |
| `tether node upgrade <nid> --url ... --sha256 ...` | Owner-only agent binary upgrade (J.4) |
| `tether node upgrade --all --url ... --sha256 ...` | Same, fan out to every ONLINE node |
| `tether admin sessions \| nodes \| audit <sid> \| evict <sid> <nid>` | Local-only broker admin (Unix socket; broker host) |

## Local laptop demo (no auth)

For quick experimentation against a vanilla `nats-server`, set
`TETHER_DEV_NO_AUTH=1` and the broker/agent connect anonymously
(NATS-level identity enforcement is bypassed; never use this in
production):

```bash
make nats-server-install                                # one-time
make nats-dev                                           # term A: nats-server -js

TETHER_DEV_NO_AUTH=1 ./bin/tether serve \
  --db ./tether.db --admin-socket /tmp/tether-admin.sock          # term B

TETHER_DEV_NO_AUTH=1 ./bin/tether agent --session lab --nid lab-1 # term C

./bin/tether admin --socket /tmp/tether-admin.sock nodes          # term D
```

The Go test suite does NOT require an external `nats-server` — it
embeds one via `nats-server/v2/test`.

## Troubleshooting

**`exec: cannot reach broker at nats://127.0.0.1:4222: …`** —
the CLI couldn't reach NATS. Check `--nats-url` (or
`~/.tether/config.toml`'s broker entry), and that the broker /
nats-server are actually running. The broker logs to stderr by
default; under systemd see `journalctl -u tether-broker`.

**`agent: register rejected (code=session_not_found_or_deleting)`** —
the session you're targeting either doesn't exist or is being
deleted. Run `tether session list` to confirm; create with
`tether session create --name <X> --pin <pin>` first.

**`agent: register rejected (code=proto_mismatch …)`** — broker
and agent are on different `ProtoVersion`s. This needs a full
reinstall (architecture J.3): re-run `install.sh --role agent` on
the agent host with a binary matching the broker's release.

**`run failed: agent allocated the PTY but ctl didn't subscribe in
time (3s) … (attach_timeout)`** — usually a slow client or NATS
hiccup; just retry. If persistent, check for clock skew or NATS
connectivity issues between ctl and broker.

**`upgrade … failed: the broker hasn't whitelisted that URL prefix
… (url_not_allowed)`** — ask the broker operator to add the URL
prefix under `broker.upgrade.url_allow` in `broker.yaml`. The
broker requires explicit whitelisting; there is no implicit default.

**`tether admin …` connection refused** — admin commands talk to a
local Unix socket (`/var/run/tether/admin.sock`). They MUST run on
the broker host as a user with read access (mode 0600, owned by
`tether`). Use `sudo -u tether tether admin ...` or run as root.

**`make lint` complains about Go version / "language version is
lower than the targeted Go version"** — golangci-lint v1.x can't
lint Go 1.25 modules. Run `make tools` to install the pinned v2.5.0.

**Agent OFFLINE in `tether ps` even though it's running** — the
agent's NATS connection might be down. Check the agent log
(`~/.tether/agent/<sid>/agent.log` or `journalctl --user -u
tether-agent@<sid>`). After re-register the broker runs G.1
reconciliation and re-derives RUNNING / EXITED / LOST status.

**Disk pressure warnings in broker logs (`disk_pressure`
sys.events)** — JetStream store dir crossed the 80% threshold
(architecture H.4). Free space, or rotate older `history-<sid>`
streams via `session rm`.

## Architecture deep-dive

See `docs/architecture.md` for the design rationale. Notable
deviations from the spec:

- P6 / F.1: spec calls for embedding the `frp` Go library and
  shipping `frpc` as a subprocess. We use a minimal in-process
  yamux-over-TCP tunnel (`internal/tunnel`, ~300 LOC) with
  identical behavior and a vastly smaller dep footprint. Swappable
  if frp's wider feature set (HTTP vhost, kcp, web admin) is ever
  needed.

- P11: no public v0.1.0 release tag. The `goreleaser` config is in
  `build/goreleaser.yaml` and produces correct artifacts in
  snapshot mode; tagging is deferred until external publication is
  ready.

`docs/requirements.md` is a v1-pre design draft. Concepts there
that don't appear in `architecture.md` (notably `push` / `pull`
file transfer) are deferred to v2; the active v1 contract is in
`architecture.md` only.
