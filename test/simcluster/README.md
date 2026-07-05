# `test/simcluster/` — Docker Simulated-Cluster Dev Tool

A **persistent, parameterized, Docker multi-container simulated cluster** that runs a real HA `tether`
fleet on one Linux host. Each container is one node running **real systemd (PID1) + real `sshd` + a real
out-of-process `nats-server` + the real `tether` binary**, on a docker bridge with per-node hostnames and
persistent named volumes. It exercises the **cross-process / on-disk / nats.conf-drift / install-path**
bug class that the hermetic `make test` and the in-process `d*_integration` suites structurally cannot
reach (the 21 real-fleet failures in `docs/v0.4.5-ha-grow-ops-gotchas.md`).

**Plan of record:** `docs/reviews/simcluster-plan.md`.

## What it is (and is NOT)

- A **dev tool**, not the product. **Zero changes to the `tether` binary**; it orchestrates the *real*
  `tether` CLI + `docker` + in-container `systemctl`, exactly as an operator would.
- **NOT part of `make test` / `make e2e`.** No Go `_test.go` here, no build tag — `go test ./...` never
  sees it. It is a separate, slower, deploy-tier gate you run before a release touches production.
- It complements — does not replace — the fast hermetic suites. The `d*_integration` suites run raft
  in-process with an embedded NATS; this runs the *real* out-of-process stack. It **replaces** "SSH to the
  real fleet and pray."

## Server

Runs on a dedicated Ubuntu host (see `docs/devices-ops.local.md §6` for the credentials). Requirements:
Linux with cgroup v2 + Docker (systemd-in-docker needs `--privileged --cgroupns=host`). The `tether`,
`nats-server` (pinned to the `install.sh` version), `nats`, and `nk` binaries are built/staged on the WSL
dev box (the server has no Go) and rsync'd over; see `remote.sh`.

## Quickstart

```sh
# from the WSL dev box (builds tether + stages vendor binaries + rsyncs the tree + runs on the server):
./remote.sh --build build                       # build the tether-sim image (once, or after a code change)
./remote.sh up --brokers 3 --agents 1 --ctl 1   # boot + provision (does not cluster)
./remote.sh init brk1                            # standalone → N=1 cluster
./remote.sh grow brk2 && ./remote.sh grow brk3   # → N=3 (real cross-process join + mesh)
./remote.sh session lab --pin 135790             # create a session + ctl login
./remote.sh agent-join agt1 --session lab --pin 135790
./remote.sh ctl -- node ls                       # run tether from the ctl container
./remote.sh drill 10-grow-to-3                    # run a drill on an isolated throwaway instance
./remote.sh shell brk1                            # or: ssh brk1 (real sshd) — robust debug
```

`--instance <name>` (env `INSTANCE=`) namespaces the containers/volumes/network so **destructive drills
run on isolated throwaway instances** and never wedge the persistent cluster. `remote.sh <verb>` runs on
the server after an rsync; edit files locally and re-run. Baked files (`Dockerfile`, `image/*`) need
`build`; the control scripts (`simcluster`, `lib/*.sh`, `drills/*`) take effect via rsync alone.

## Verbs

`build · up · init · grow · force-single · session · agent-join · ctl · status · exec · shell · ssh ·
logs · nats-conf · drill · down [-v] · nuke · doctor`. See `./simcluster` (no args) for the summary.

## Drills

Run on an isolated `--instance`; each prints RED/GREEN. GREEN = an invariant that must hold; a **RED
drill** asserts a *currently-broken* behavior that has **no green code path today** (open backlog),
signature-guarded (`lib/assert.sh`) so it can only pass for the DOCUMENTED cause — flip it to a plain
GREEN regression the day the product fix lands.

**Run drills ONE AT A TIME.** The heavy clustered-JetStream drills starve each other's meta-group
formation when several run back-to-back on one host (the same contention that keeps `make e2e` serial;
see `Makefile`) — `grow` / `node ls` then flake. Run `simcluster drill <name>` singly (each nukes its
isolated instance on exit); do not loop all drills in one shot on a busy box.

| Drill | Proves |
|---|---|
| `00-skeleton` | GREEN acceptance gate: N=1 cutover + `agent join` + tier-A/B push/pull round-trip |
| `10-grow-to-3` | GREEN: real N=1→2→3 grow, 3 VOTER + streams R=3 + 3-node JS meta + follower-kill quorum proof |
| `20-forcesingle-natsconf` | RED (#20): force-single leaves nats.conf clustered → tier-B JS 503-rots |
| `21-smalldisk-tierb` | RED (#21): 8 GiB OBJ_xfer reservation denies tier-B on a small (tmpfs-capped) store |
| `12-ghost-voter` | RED (#12): force-single ghost VOTER — all three online removal paths refuse |

## Real `tether` findings surfaced (2026-07-05)

This tool caught cross-process deployment defects unreachable by the in-process suites; the confirmed
ones are logged in `docs/v0.4.5-ha-grow-ops-gotchas.md` (#22 install.sh doesn't chown `/etc/tether` →
in-broker reconciler can't converge topology; #23 broker clean-exits on nats loss + `Restart=on-failure`
→ stays down; #24 route-cert needs a SAN for the nats route mesh). One open item (not yet attributed to
tether): after a grow the health label lingers `DEGRADED-WRITABLE` because `nats-server --signal reload`
does not advance `/varz config_load_time`, so the topology observer can't confirm the load — the cluster
is functionally HA (routes/JS/streams all converged).

## NON-GOALS

Single host (cross-process, not cross-host WAN raft); no ACME/Caddy/real domains (plain `nats://` on the
trusted bridge, route mTLS is real); v2/`main` only; not a load/perf/fuzz tool; Linux/amd64 only; not a
CI gate and not a substitute for the real multi-host staging drill.
