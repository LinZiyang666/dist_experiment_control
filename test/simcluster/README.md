# `test/simcluster/` — Docker Simulated-Cluster Dev Tool

A **persistent, parameterized, Docker multi-container simulated cluster** that runs a real HA `tether`
fleet on one Linux host. Each container is one node running **real systemd (PID1) + real `sshd` + a real
out-of-process `nats-server` + the real `tether` binary**, on a docker bridge with per-node hostnames and
persistent named volumes. It exercises the **cross-process / on-disk / nats.conf-drift / install-path**
bug class that the hermetic `make test` and the in-process `d*_integration` suites structurally cannot
reach (the 21 real-fleet failures in `docs/v0.4.5-ha-grow-ops-gotchas.md`).

**Plan of record:** `docs/reviews/simcluster-plan.md`.

## Mandate — reproduce reality, expose defects, never compensate

This tool exists to **surface `tether`'s defects, not to hide them.** Its one and only job is to
faithfully reproduce a **real production server-cluster deployment environment** — real systemd, a real
out-of-process `nats-server`, real cross-process / cross-host route mTLS, real persistent disk, the real
`install.sh` install paths — and to let **every defect `tether` has under real deployment show itself.**
It is bound by the following, without exception:

1. **Never accommodate `tether`'s broken design.** The environment is built the way real production is
   (e.g. `install.sh` leaves `/etc/tether` root-owned) and is **never** altered to dodge a `tether`
   defect — no chowning, no patching, no pre-staged workarounds. If `tether` would trip over something in
   the real world, the sim lets it trip.

2. **Expose the gap; do not fill it for `tether`.** Any cluster operation that *should* be a few
   automated `tether` commands but today requires manual toil (grow / shrink / upgrade / de-cluster …)
   must be **presented as the gap it is and asserted** — never scripted away so `tether` looks more
   capable than it is. Every unavoidable manual workaround is labeled a `tether` defect (`[GAP #N]`) and,
   wherever possible, pinned by a signature-guarded assertion, to be flipped to a plain GREEN regression
   the day the product fix lands.

3. **A hard line: the environment's job vs. `tether`'s job.** Provisioning machines (install.sh,
   minting/placing secrets, booting containers) is the **sim's** job. Cluster *operations* (init / grow /
   retire / force-single / de-cluster …) are **`tether`'s** job — where `tether` cannot perform them, or
   cannot perform them cleanly, the sim **only exposes that**; it does not do the work on `tether`'s behalf.

4. **Inverted success criterion.** If an operation "succeeds" only because the sim wrapped it in elaborate
   bash, that is not the sim's achievement — it is `tether`'s failure being masked, and it counts as a
   **defect to be re-exposed.** The more effortless the sim makes a broken operation look, the more
   suspect it is.

## What it is (and is NOT)

- A **dev tool**, not the product. **Zero changes to the `tether` binary**; it orchestrates the *real*
  `tether` CLI + `docker` + in-container `systemctl`, exactly as an operator would.
- **NOT part of `make test` / `make e2e`.** No Go `_test.go` here, no build tag — `go test ./...` never
  sees it. It is a separate, slower, deploy-tier gate you run before a release touches production.
- It complements — does not replace — the fast hermetic suites. The `d*_integration` suites run raft
  in-process with an embedded NATS; this runs the *real* out-of-process stack. It **replaces** "SSH to the
  real fleet and pray."

## Architecture

Two layers — a **driver on your WSL dev box** and a **brain on the sim server** — because the server has
no Go toolchain and WSL's docker+systemd is awkward:

```
  WSL dev box                          dedicated Ubuntu server
  -----------                          -----------------------
  remote.sh  ──build tether────────>   docker build the image
             ──stage vendor/ + rsync>  (nats-server / nats / nk / install.sh)
             ──ssh: run simcluster──>   simcluster orchestrates N containers.

     each container = one node (brk1, brk2, brk3, agt1, ctl1, …) on a docker
     bridge where container-name == hostname == node_id:

        +- brk1 ------------------------------------------+
        |  systemd (PID1)  +  sshd                        |
        |  nats-server  (real, SEPARATE service, :6222)   |   <- raft   :7400
        |  tether-broker (real binary, cluster mode)      |   <- client :4222
        |  volumes: /etc/tether  +  /var/lib/tether       |   (persistent)
        +-------------------------------------------------+
        (brk2, brk3 identical brokers; agt1 = agent; ctl1 = ctl role)
```

- **One container = one node = one machine.** Real systemd is PID1 (manages services just like a real
  box); `nats-server` and `tether-broker` are TWO SEPARATE systemd services — exactly like production, NOT
  an embedded in-process NATS. That is the whole point: deploy-layer bugs (install paths, nats.conf
  ownership, unit behavior on nats loss, cross-process route mTLS) only exist with the real stack.
- **Addressing = hostname.** `node_id == hostname == server_name == raft ServerID == cert CN/SAN`
  (`brk1:7400` raft, `nats://brk1:6222` route, `brk1:4222` client) — no IP bookkeeping.
- **Persistent.** Each node mounts two docker named volumes (`/etc/tether` config, `/var/lib/tether`
  data), so state survives restart/upgrade — that is how "upgrade like real usage" gets tested.

### File map

| File | Runs where | Job |
|---|---|---|
| `remote.sh` | **your WSL** | The driver you actually type. Compiles `tether` + stages `vendor/` binaries (server has no Go) + rsyncs the tree + ssh-runs `simcluster` on the server. |
| `simcluster` | server | The brain: ~19 verbs. Orchestrates docker + in-container `systemctl` + the real `tether` CLI. |
| `lib/log.sh` | server | logging + `poll_until` (wait-for-condition, never a fixed sleep) |
| `lib/docker.sh` | server | docker primitives (`dexec`, `ctr_name`, `run_node`, hostname addressing) |
| `lib/tether.sh` | server | tether helpers (`leader_node`, `wait_phase`, `cluster_status_json`; defaults to `User=tether`) |
| `lib/secrets.sh` | server | mints the §15 secrets with host `openssl` (ed25519) + `nk` (CA, account, route/tunnel certs WITH SANs, node-ident, broker nkey) + distributes them into containers |
| `lib/assert.sh` | server | the RED/GREEN drill harness: `assert_ok` / `assert_refuses` / `assert_bug` (signature-guarded so a bug only "passes" for its DOCUMENTED cause) |
| `Dockerfile` | build | base image: ubuntu:24.04 + systemd + sshd + baked binaries |
| `image/provision-node.sh` | in container | lays down a node's disk tree via the REAL `install.sh` (`--skip-download`), exactly as a real machine installs; keeps `/etc/tether` root-owned (the Option B invariant post-G1 — install.sh makes `/etc/tether/nats.d/` tether-owned; a tether-owned `/etc/tether` would be a tether→root privesc) |
| `image/pty-confirm.py` | in container | feeds the typed confirm to interactive `tether` commands (e.g. `cluster init` demands a TTY) |
| `image/units/tether-agent.service` | in container | the agent systemd unit |
| `drills/*.sh` | server | one reproducible deploy scenario each (acceptance/regression); see the Drills table |

**Edit-loop:** the control scripts (`simcluster`, `lib/*`, `drills/*`) take effect via rsync alone — just
re-run `remote.sh`. The baked files (`Dockerfile`, `image/*`, the vendored binaries) need `remote.sh build`.

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

## Walkthrough (what each step does + how to debug)

The Quickstart, explained. Everything is typed on your WSL box; `remote.sh` ships it to the server.

1. `remote.sh --build build` — compile `tether`, fetch/cache `nats-server` (pinned to install.sh's
   version), stage `vendor/`, rsync the tree, and `docker build` the image on the server. Run once, and
   again after you change `tether` source or `Dockerfile`/`image/*`. (Changed only a control script or
   drill? Drop `--build` — any verb re-rsyncs.)
2. `remote.sh up --brokers 3 --agents 1 --ctl 1` — create + boot + provision the containers (real systemd
   + install.sh per node). Does NOT form a cluster yet — every broker is a lone standalone.
3. `remote.sh init brk1` — cut brk1 from standalone to a **single-voter cluster** (`cluster init
   --from-existing` + the nats.conf takeover + start in cluster mode). brk1 is now the N=1 leader.
4. `remote.sh grow brk2 && remote.sh grow brk3` — add each as a voter via the REAL cross-process join
   (secrets → joiner `cluster init` → `join prepare`/`approve` → render the route mesh → catch up →
   VOTER). This is the honest grow: it runs tether's real commands, labels every workaround `[GAP #N]`,
   and prints a `GREW-VIA-WORKAROUNDS:` trailer (see the Mandate).
5. `remote.sh session lab --pin 135790` — create a session on a broker + log the ctl node in.
6. `remote.sh agent-join agt1 --session lab --pin 135790` — bind agt1's nkey + start its persistent agent
   unit (reconnects as a bound member).
7. `remote.sh ctl -- node ls` — run any `tether` subcommand from the ctl container (its session +
   broker URL persist).

**Debugging (like ssh-ing into a machine):** `remote.sh shell brk1` (or `ssh brk1`) for a shell inside a
node · `remote.sh logs brk1 tether-broker` for journalctl · `remote.sh status` for the node table + leader
view · `remote.sh nats-conf brk1` to inspect nats.conf (the #20 probe) · `remote.sh exec brk1 -- <cmd>`
for any command · `remote.sh doctor` for PID1 + ownership drift checks (the #22 tripwire).

**Teardown:** `remote.sh down` (stop, keep data) · `remote.sh down -v` (remove volumes) · `remote.sh nuke`
(delete everything — containers + volumes + secrets — for a clean slate).

**Isolation:** `--instance <name>` (or env `INSTANCE=`) namespaces containers/volumes/network, so
destructive `drill`s run on throwaway `drill-*` instances that never touch your persistent cluster (each
drill nukes its own instance on exit).

## Verbs (reference)

| Verb | Does |
|---|---|
| `build` | build the tether-sim image from `vendor/` + install.sh |
| `up --brokers N --agents M --ctl K` | create + boot + provision nodes (does not cluster) |
| `init <brk>` | standalone → N=1 cluster |
| `grow <brk>` | add a fresh broker as a voter (honest, gap-labeled) |
| `force-single <brk> --dead <n>` | online quorum-loss escape (the #20/#12 setup) |
| `session <name> --pin <p>` | create a session + ctl login |
| `agent-join <agt> --session <s> --pin <p>` | bind + start a persistent agent |
| `ctl -- <tether args…>` | run tether from the ctl container |
| `status [--json]` | node table + the leader's cluster status |
| `exec <node> -- <cmd…>` | docker exec inside a node |
| `shell <node>` / `ssh <node>` | interactive shell (docker exec / real sshd) |
| `logs <node> [unit]` | journalctl inside a node |
| `nats-conf <node>` | inspect `/etc/tether/nats.d/nats.conf` (the #20 probe) |
| `drill <name>` | run `drills/<name>.sh` on an isolated throwaway instance |
| `down [-v]` · `nuke` | stop (keep volumes; `-v` removes) · full clean slate |
| `doctor` | PID1 + provisioning + ownership drift checks |

`./simcluster` with no args prints the same summary.

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
| `11-grow-gaps` | RED (#8/#I1): `grow` reaches VOTER only via labeled workarounds — `join approve --wait` stalls pre-mesh (#8, tether's own "still in flight"); a fresh joiner without `cluster init` can't serve (#I1, `serve` refuses "no raft state"); asserts the `GREW-VIA-WORKAROUNDS` trailer |
| `13-inbroker-reconcile-perm` | GREEN (#22 FIXED, G1 Option B): the reconciler's nats.conf now lives in tether-owned `/etc/tether/nats.d/` (`/etc/tether` STAYS root-owned — Caddyfile safe), so the `User=tether` in-broker reconciler writes it + auto-converges. Asserts `/etc/tether` root-owned + `nats.d/` tether-owned + every voter's `nats_conf_path` at nats.d/ + a `User=tether` write into nats.d/ succeeds + grow drops the #22 token |
| `20-forcesingle-natsconf` | RED (#20): force-single leaves nats.conf clustered → tier-B JS 503-rots |
| `21-smalldisk-tierb` | RED (#21): 8 GiB OBJ_xfer reservation denies tier-B on a small (tmpfs-capped) store |
| `12-ghost-voter` | RED (#12): force-single ghost VOTER — all three online removal paths refuse |

## Real `tether` findings surfaced (2026-07-05)

This tool caught cross-process deployment defects unreachable by the in-process suites, logged in
`docs/v0.4.5-ha-grow-ops-gotchas.md`:

- **#22 (FIXED in G1, 2026-07-05 — Option B).** Pre-G1: install.sh left the reconciler's nats.conf in the
  root-owned `/etc/tether`, so a `User=tether` in-broker C3 reconciler write (temp/`.bak` + atomic rename,
  which need DIRECTORY write) perm-denied → topology never auto-converged. **G1 fix = Option B**: relocate
  ONLY the reconciler's nats.conf into a tether-owned `/etc/tether/nats.d/` subdir; `/etc/tether` ITSELF
  STAYS root-owned (the root-run caddy reads `/etc/tether/Caddyfile` — a tether-owned `/etc/tether` would be
  a tether→root local privesc, so **chowning ETC was REJECTED**). drill `13` is now a **GREEN regression**
  (driven by the REAL vendored install.sh): it asserts `/etc/tether` root-owned + `/etc/tether/nats.d`
  tether-owned + every voter's broker.yaml pins `nats_conf_path` at nats.d/ + the second grow's `reconcile
  nats --all` has NO natsconf permission-denied reason + a `User=tether` write into nats.d/ SUCCEEDS + grow
  drops the #22 trailer token. `doctor` asserts `/etc/tether` STAYS root-owned (errs if hand-chowned) AND
  nats.d/ is tether-owned. (History: an earlier BAKED image once masked #22 via a stale provision that
  chowned ETC to tether; that chown was removed. `provision-node.sh` adds NO chown — nats.d/'s tether
  ownership comes from the real install.sh.)
- **#I1** (new, 2026-07-05) cluster-mode `serve` fail-closed refuses without pre-existing raft state, so a
  fresh joiner MUST `cluster init` before it can serve — yet the `join` flow does NOT bootstrap the
  joiner's raft state and the runbook §1 joiner path omits the init step (drill `11`, `assert_refuses` —
  the serve refusal is a KEPT invariant; the gap is that join leaves no raft behind).
- **#24** route-cert needs a SAN for the nats route mesh (worked around in `lib/secrets.sh`; grow
  trailer-names #24; a CN-only-cert RED drill is deferred).
- **#23** broker clean-exits on nats loss + `Restart=on-failure` → stays down. The clean-exit trigger is
  unlocated (per gotcha #23). Two recipes were tried on the server (2026-07-05) and NEITHER reproduces the
  clean-exit-0 strand: (a) restarting a healthy follower's nats → an exit-70 *crash* that
  `Restart=on-failure` revives; (b) a long N=1 outage (`stop nats; dwell 8s`) → the broker stays *active*
  (`MaxReconnects(-1)` reconnects). So there is no deterministic RED drill — `grow` labels + recovers the
  strand it does hit (the #10-adjacent crash-loop during the JS-meta-not-formed window, via
  `reset-failed`+`start`), tagged `[workaround #23]`.

### Gaps LABELED by grow but NOT yet signature-pinned (regression-unprotected)

The `GREW-VIA-WORKAROUNDS` trailer is the interim regression guard for these (drill `11` asserts the full
ordinal set, so dropping any label flips it), but none has a dedicated RED drill yet: **#3** (auto
`reconcile nats --all` can't harvest route mTLS on the first grow), **#4** (standalone JS doesn't migrate
into the clustered meta), **#10** (clustered-alone joiner exit-70 — the mesh-before-joiner ordering that
dodges it is the standing evidence), **#5** (init has no machine-escape confirm), **#24** (CN-only cert),
**#23** (see above). Deliberately out of scope: **#6** (root-owned `tether.lock`) — the sim standardizes on
`User=tether` for `cluster init` (which produces the correct post-fix ownership), so it does not reproduce
#6; a root-init drill would be needed to surface it.

One item to catalogue as a tether OBSERVABILITY gap (candidate gotcha): after a grow the health label
lingers `DEGRADED-WRITABLE` because the topology observer keys convergence on `/varz config_load_time`,
which `nats-server --signal reload` never advances on a byte-identical reload — so tether can't confirm
the load even though routes/JS/streams are fully converged (this is what made the fleet's post-grow
DEGRADED look like #22).

## NON-GOALS

Single host (cross-process, not cross-host WAN raft); no ACME/Caddy/real domains (plain `nats://` on the
trusted bridge, route mTLS is real); v2/`main` only; not a load/perf/fuzz tool; Linux/amd64 only; not a
CI gate and not a substitute for the real multi-host staging drill.
