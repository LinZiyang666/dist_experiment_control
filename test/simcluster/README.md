# `test/simcluster/` — Docker Simulated-Cluster Dev Tool

A **persistent, parameterized, Docker multi-container simulated cluster** that runs a real HA `tether`
fleet on one Linux host. Each container is one node running **real systemd (PID1) + real `sshd` + a real
out-of-process `nats-server` + the real `tether` binary**, on a docker bridge with per-node hostnames and
persistent named volumes. It exercises the **cross-process / on-disk / nats.conf-drift / install-path**
bug class that the hermetic `make test` and the in-process `d*_integration` suites structurally cannot
reach (the 21 real-fleet failures in `docs/reviews/v0.4.5-ha-grow-ops-gotchas.md`).

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

**The primary home is `weilandserver` itself** — it is both the main development box and the sim host,
so the normal path is to work in the checkout on that machine and drive `./simcluster` directly. Check
where you are first: `hostname` is `weilandserver` (note `/etc/hostname` says `weiland_server`), or
`hostname -I` contains `192.168.1.150`.

`remote.sh` remains for the *optional* case of driving from a different box (build elsewhere, rsync in,
ssh-run). On `weilandserver` itself, skip it — it would rsync the machine onto itself.

```
  on weilandserver (normal)            from another box (optional, remote.sh)
  -------------------------            --------------------------------------
  ./simcluster <verb>  ────────────>   build tether ──> stage vendor/ ──> rsync
                                       ──ssh: run simcluster on the server──>

     simcluster orchestrates N containers.

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
| `local.sh` | **`weilandserver` (normal)** | The on-host driver. Same vendor staging as `remote.sh`, minus rsync/ssh — builds in place and execs `./simcluster`. Refuses to run off-host (escape hatch: `SIM_ALLOW_ANY_HOST=1`). |
| `lib/stage.sh` | both drivers | Shared vendor staging (tether + tether-next + pinned `nats-server` + `nats`/`nk` + `install.sh`) and the `sim_is_sim_host` probe, so the two drivers cannot drift. |
| `remote.sh` | **an external driver box (optional)** | Only needed when you are *not* on `weilandserver`. Compiles `tether` + stages `vendor/` binaries + rsyncs the tree + ssh-runs `simcluster` on the server. On the server itself, call `./simcluster` directly. |
| `simcluster` | server | The brain: ~19 verbs. Orchestrates docker + in-container `systemctl` + the real `tether` CLI. |
| `lib/log.sh` | server | logging + `poll_until` (wait-for-condition, never a fixed sleep) |
| `lib/docker.sh` | server | docker primitives (`dexec`, `ctr_name`, `run_node`, hostname addressing) |
| `lib/tether.sh` | server | tether helpers (`leader_node`, `wait_phase`, `cluster_status_json`; defaults to `User=tether`) |
| `lib/secrets.sh` | server | mints the §15 secrets with host `openssl` (ed25519) + `nk` (CA, account, route/tunnel certs WITH SANs, node-ident, broker nkey) + distributes them into containers |
| `lib/assert.sh` | server | the drill harness + the **5-verdict contract** (SSOT: the header truth-table): `assert_ok` / `assert_refuses` / `assert_bug` / `assert_setup` / `setup_fail` / `product_red` / `not_covered`. A drill lands in exactly one verdict — GREEN / PRODUCT-RED (a signature-guarded known defect reproduced — the harness working, a distinct non-green EXPECTED state) / INCOMPLETE (a recorded coverage gap) / SETUP-RED / ASSERT-FAIL — emitted as a machine-parseable `DRILL-VERDICT` line. Pinned by `tests/verdict-contract-test.sh` + `tests/lint-drills.sh` |
| `Dockerfile` | build | base image: ubuntu:24.04 + systemd + sshd + baked binaries |
| `image/provision-node.sh` | in container | lays down a node's disk tree via the REAL `install.sh` (`--skip-download`), exactly as a real machine installs; keeps `/etc/tether` root-owned (the Option B invariant post-G1 — install.sh makes `/etc/tether/nats.d/` tether-owned; a tether-owned `/etc/tether` would be a tether→root privesc) |
| `image/pty-confirm.py` | in container | feeds the typed confirm to interactive `tether` commands (e.g. `cluster init` demands a TTY) |
| `image/units/tether-agent.service` | in container | the agent systemd unit |
| `drills/lib/colocated.sh` | server | **OQ-6 SUPPLY (R9-D)**: provisions a tether AGENT co-located on a BROKER host — declares it in broker.yaml (`broker.cluster.colocated_agent_nid`), binds it into the session via the `--pin` first-connect, and runs it as a system unit out of the ROOT-owned `/usr/local/bin/tether` (the re-exec-only path `cluster upgrade`'s agent leg uses; NOT the `~/.local/bin` copy the standalone agent role needs for `node upgrade`'s atomic replace). This is machine SUPPLY, per Mandate ③ — the sim builds the host shape, every upgrade action stays tether's. Without it drill 30's whole-host (broker + agent dual-version) leg was structurally uncoverable |
| `drills/*.sh` | server | one reproducible deploy scenario each (acceptance/regression); see the Drills table |
| `tests/r9d-nonvacuity.sh` | dev box | **the NON-VACUITY gate (R9-D)**: extracts each drill oracle VERBATIM from its drill file and drives it with deliberately BAD input, proving it can go RED. Running the drills cannot catch a permanently-true oracle — it passes every run, forever (H1: a jq path matching nothing; H12: an empty grep needle; and, found BY this harness, a `.assumed // empty` whose jq `//` swallows `false`, making `assumed=false` permanently unassertable). Wired into `tests/run-all.sh` |
| `run-drills.sh` | server | run the WHOLE drill suite in PARALLEL — inotify preflight + concurrency cap (`-j`) + infra-flake re-run; the preferred way to run all drills (see Drills) |

**Edit-loop:** the control scripts (`simcluster`, `lib/*`, `drills/*`) take effect via rsync alone — just
re-run `remote.sh`. The baked files (`Dockerfile`, `image/*`, the vendored binaries) need `remote.sh build`.

## Server

Runs on `weilandserver`, the main development box (see `docs/devices-ops.local.md §6`). Requirements:
Linux with cgroup v2 + Docker (systemd-in-docker needs `--privileged --cgroupns=host`), plus a Go
toolchain to build `tether` in place. The `nats-server` (pinned to the `install.sh` version), `nats`
and `nk` binaries live in `vendor/` and are staged by `./simcluster build`. When driving from an
external box instead, `remote.sh` builds and rsyncs them over.

## Quickstart

```sh
# On weilandserver:      ./local.sh  [--build] <verb>     (builds in place, no rsync/ssh)
# From another box:      ./remote.sh [--build] <verb>     (builds + rsyncs + ssh, shown below)
# The two take identical verbs; each refuses to run in the other's situation.
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

The Quickstart, explained. On `weilandserver` these are `./simcluster <verb>`; from an external box
`remote.sh` wraps each one with a build + rsync first.

1. `remote.sh --build build` — compile `tether`, fetch/cache `nats-server` (pinned to install.sh's
   version), stage `vendor/`, rsync the tree, and `docker build` the image on the server. Run once, and
   again after you change `tether` source or `Dockerfile`/`image/*`. (Changed only a control script or
   drill? Drop `--build` — any verb re-rsyncs.)
2. `remote.sh up --brokers 3 --agents 1 --ctl 1` — create + boot + provision the containers (real systemd
   + install.sh per node). Does NOT form a cluster yet — every broker is a lone standalone.
3. `remote.sh init brk1` — cut brk1 from standalone to a **single-voter cluster** (`cluster init
   --from-existing` + the nats.conf takeover + start in cluster mode). brk1 is now the N=1 leader.
4. `remote.sh grow brk2 && remote.sh grow brk3` — add each as a voter by driving tether's REAL
   `cluster add` (offline init → approve-join → former-N1 JS reset → clustered cutover → catch up →
   VOTER). Since G4 this is end-to-end `tether cluster add` (no hand cluster-lifecycle steps left), so it
   prints a `GREW-VIA-TETHER-CLUSTER-ADD:` trailer; the only `[env]` steps are provisioning
   (secrets/first-boot/daemon-start) per the Mandate.
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
| `grow <brk>` | add a fresh broker as a voter (honest; drives `tether cluster add`, `GREW-VIA-TETHER-CLUSTER-ADD` trailer) |
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

Run on an isolated `--instance`. Each drill lands in exactly ONE of five verdicts (SSOT: the
`lib/assert.sh` header truth-table; pinned by `tests/verdict-contract-test.sh`), emitted as a
machine-parseable `DRILL-VERDICT verdict=<ENUM> …` line that `run-drills.sh` classifies by:

| verdict | meaning | suite disposition |
|---|---|---|
| **GREEN** | every assertion a KEPT invariant, no gaps, no defects | clean |
| **PRODUCT-RED** | a signature-guarded KNOWN defect reproduced for its documented reason (`assert_bug`/`product_red`) — the harness worked, but the product is still defective | **BLOCKER by default**; only explicit `--allow-product-red` waives it for that run |
| **INCOMPLETE** | a first-class `not_covered()` coverage gap recorded (explore→pin follow-up) | **BLOCKER by default**; only explicit `--allow-incomplete` waives it for that run |
| **SETUP-RED** | a prerequisite / fixture / provisioning step failed, or a harness misuse | **release BLOCKER** |
| **ASSERT-FAIL** | a KEPT invariant broke, or a signature guard tripped (broke for an UNDOCUMENTED reason / a known bug "APPEARS FIXED") | **release BLOCKER** |

The mandate is to **EXPOSE tether's defects, not chase all-green**: PRODUCT-RED and INCOMPLETE are useful,
honest RED evidence, not permission to release. All non-GREEN states block by default. Owner waivers are
explicit command-line policy and print `WAIVED NON-GREEN`; a waived run is never called all-green. The runner
requires exactly one anchored verdict line and cross-checks enum, counters, line `rc=` and process rc;
missing verdicts are INFRA-ABORT, malformed/duplicate/contradictory verdicts are CONTRACT-ERROR, and neither
is retryable. A PRODUCT-RED drill flips to GREEN only after the product fix makes its positive invariant pass.

**Running the whole suite — PREFER `./run-drills.sh` (parallel).** The drills are docker-isolated (own
network/volumes/containers per instance) and parallelize freely; the host has ~20× the capacity the
full suite needs (measured 2026-07-09: ~600 concurrent systemd containers before a CPU boot-storm
ceiling; the 7 drills are ~25 containers). What *used* to force serial runs was a single misdiagnosed
kernel limit — **`fs.inotify.max_user_instances` (default 128, a PER-UID cap shared by ALL privileged
containers)**. Every systemd container opens several inotify instances under host uid 0; parallel
drills exhaust the 128 and the next container's systemd PID1 dies (exit 255 → "container not running"
→ `up`'s `wait_sysd` times out at 60s). It *looks* like a boot/IO storm but is not — CPU/mem/io stay
near idle; it is a counter, not saturation (decisive test: 40 containers → 13 fail at 128, 0 at 8192,
12 again back at 128). Raise it once and the suite parallelizes cleanly:

```sh
echo 'fs.inotify.max_user_instances=8192' | sudo tee /etc/sysctl.d/99-simcluster.conf && sudo sysctl --system
```

Then run the whole suite in parallel — **this is the preferred path**:

```sh
./run-drills.sh                 # ALL drills, full parallel; preflights the inotify cap first
./run-drills.sh -j 4            # cap concurrency on a smaller host
./run-drills.sh 10-grow-to-3    # a named subset
```

`run-drills.sh` preflights `max_user_instances` (raising it if it has passwordless sudo, else printing
the fix), fires every drill concurrently on its own throwaway instance, then serially re-runs any infra
flake. A single drill by hand is still fine: `simcluster drill <name>`.

**CAVEAT (grow-concurrency, INDEPENDENT of inotify).** The heaviest clustered-JetStream grow
(`10-grow-to-3`: two grows + follower-kill) can still time out its VOTER promotion (150s) when ALL
drills grow at the same instant — raft/JS-meta formation is timing-sensitive at peak concurrency. If it
goes RED alone in an otherwise-green full parallel run, re-run it singly or cap with `-j`.

**OQ-8 whole-suite policy (instituted from S1; CORRECTED by the S1 internal review).** `-j N` is a pure
global concurrency throttle with **no family awareness** (`run-drills.sh:144`), and drills launch in
glob/alpha order: `00,10,11,12,13,20,21,60,61,62`. So `-j 6` is **NOT** a "family-wave cap": its first
6-wide wave is `00,10,11,12,13,20` — **all five heavy grow / force-single drills at once** (`10/11/13` grow
to N=3; `12/20` grow-then-force-single via `drills/lib/setup-forcesingle.sh`), front-loading the exact
peak-grow concurrency the CAVEAT above says blows the 150s VOTER window, and deferring the cheap N=1 drills
(`21/60/61/62`) to wave 2 — the opposite of separating families. `-j 6` also cannot bound concurrent grows
below 5 (5 grows, 6 > 5), so it yields **zero** reduction vs. full parallel; and a VOTER-timeout log line
(`… waiting for: brkN reaches VOTER`) is **deliberately NOT** a `FLAKE_SIG` signature (external-review
round-2 R2-F1): `is_flake` has no concurrency input, so auto-retrying a VOTER timeout would misclassify a
`-j 1` SOLO timeout — which is a REAL regression per the CAVEAT — as retryable and overwrite its evidence.
A grow-timing timeout therefore shows RED and is **re-run SINGLY by hand** per the CAVEAT; the two-wave
family split below keeps the flake from arising at all. (Genuine INFRA flakes — systemd PID1 dying under
inotify-cap exhaustion, container-not-running — ARE auto-retried, and their first-run log is preserved as
`<name>.attempt1.log`, never silently truncated.)
**Correct wrap policy — schedule BY FAMILY in two passes:** run the grow / force-single family serially or
`-j 2` (`./run-drills.sh -j 2 10-grow-to-3 11-grow-gaps 12-ghost-voter 13-inbroker-reconcile-perm
20-forcesingle-natsconf`), then the N=1 family at full parallel (`./run-drills.sh 00-skeleton
21-smalldisk-tierb 60-user-journey 61-transfer-edges 62-remote-fs-safe`). `run-drills.sh`'s infra-flake
re-run still only re-runs the documented `FLAKE_SIG` signatures (a grow-timing RED is re-run singly per the
CAVEAT, never auto-swallowed).

**Numbering families (tens digit = scenario family; established by the S-series coverage roadmap).**
`0x` skeleton · `1x` grow · `2x` force-single·capacity · `3x` upgrade · `4x` shrink·regression·cutover ·
`5x` backup·DR·rotation · `6x` user-plane · `7x` expose·proxy dataplane · `8x` session·security·onboarding ·
`9x` observability·client-view·chaos. Historical exceptions kept as-is: **12/13/20/21** (their numbers came
from gotcha ordinals — 12/20/21 off gotcha ids, 13 a run-on).

| Drill | Proves |
|---|---|
| `00-skeleton` | GREEN acceptance gate: N=1 cutover + `agent join` + tier-A/B push/pull round-trip |
| `10-grow-to-3` | GREEN: real N=1→2→3 grow, 3 VOTER + streams R=3 + 3-node JS meta + follower-kill quorum proof |
| `11-grow-gaps` | GREEN (G4-inverted): `grow` drives `tether cluster add` end-to-end (init/approve/mesh-render/former-N1-JS-reset/catch-up), so the old manual-workaround signatures (#3/#4/#8) are now asserted **ABSENT** and the trailer is `GREW-VIA-TETHER-CLUSTER-ADD`. The **#I1** serve fail-closed invariant STAYS (a fresh joiner with no raft state must NOT auto-bootstrap a cluster — `assert_refuses` `no raft state exists|never auto-bootstraps`; #I* family closed, see `docs/deploy-tier-gotchas.md`). Structural gate A asserts cmd_grow runs no hand cluster-lifecycle |
| `13-inbroker-reconcile-perm` | GREEN (#22 FIXED, G1 Option B): the reconciler's nats.conf now lives in tether-owned `/etc/tether/nats.d/` (`/etc/tether` STAYS root-owned — Caddyfile safe), so the `User=tether` in-broker reconciler writes it + auto-converges. Asserts `/etc/tether` root-owned + `nats.d/` tether-owned + every voter's `nats_conf_path` at nats.d/ + a `User=tether` write into nats.d/ succeeds + grow drops the #22 token |
| `20-forcesingle-natsconf` | GREEN (#20/#12 FIXED, G2): OFFLINE force-single auto-de-clusters the survivor's nats.conf to standalone (identity from cluster_nodes) + prunes the abandoned peer → after a JS-store reset + restart, tier-B WORKS at N=1 (14 assertions). Was RED: force-single left the conf clustered → silent JS 503-rot |
| `21-smalldisk-tierb` | GREEN (#21 FIXED, G6): disk-aware OBJ_xfer MaxBytes (sized off the JS store ceiling, not a hardcoded 8 GiB) fits under a 4g tmpfs store → tier-B push works + file lands (8 assertions). Was RED: 8 GiB reservation → JS storage admission 10047 |
| `12-ghost-voter` | GREEN (#12 FIXED, G2): force-single now AUTO-PRUNES the abandoned peer → no phase==VOTER ghost, so the old three-non deadlock premise is gone; the removal path cleanly reports 'no such roster node' (13 assertions, OFFLINE force-single). Upgrade-leftover ghost passthrough covered hermetically (`TestG2RemoveNodeGhostPassthrough` — the deploy tier has no sqlite3 / old binary to manufacture that legacy ghost). Was RED: force-single left the ejected peer phase==VOTER, all three online removal paths deadlocked |
| `60-user-journey` | GREEN (S1, N=1 + 2 agents): the first-day user journey on the real auth_callout stack — login/ctx/logout + **G.3** (logout REFUSES; a mid-window agt2 stop is proven broker-side via the admin socket — no ctl session — BEFORE re-login, then the FIRST post-login read already reflects it: reconnected read = current state, not a login-time snapshot) · `node ls` real columns + `-a` OFFLINE view · `exec` exit-0 / exact non-zero / 256 KiB stream / `--cwd` / signal→**flat 128** · `run` over a **real cross-container PTY** (via `image/pty-run.py`): `stty size` 40×132, interactive round-trip, SIGWINCH resize, **Ctrl-C** interrupts the remote pgroup, no orphan · `ps`/`ps -a` RUNNING→EXITED + PORTS (exact 6-col header, no HOME) · `history -n`/`--kind`/`--follow` · version/completion (38 assertions) |
| `61-transfer-edges` | GREEN (S1, N=1): push/pull deploy edges — a **real daemon loading a real `agent.yaml`** (via `drills/lib/agentyaml.sh`, flagless unit so yaml `broker_url` is authoritative) enforcing the `allow_roots` tri-state (open/narrow/disabled) across a real restart; both-direction walls (`path_parent_missing`[push]/`path_not_found`[pull]/`path_outside_roots`/`not_a_regular_file`/`transfer_disabled`/`dst_exists`→`--force`/`too_large`>2 GiB via pull); **tier boundary** set by the real nats `max_payload` (1 MiB→tier-B); `history --kind transfer` complete+failed pairing; every policy refusal `! io_error`-guarded; a G0 malformed-yaml guard self-test; **cross-node SHA-256** oracles on `--force` overwrite + restore round-trip (41 assertions) |
| `62-remote-fs-safe` | GREEN (S1 spike, FUSE-approx): a real `fuse.hangfs` mount (`image/hangfs.py`) reproduces a hung network mount; `remote_fs` **auto** fast-fails `remote_fs_unsafe_cwd`/`remote_fs_unhealthy` (bounded — no agent wedge), **off+`--safe`** escalates to a fast-fail. A `/proc`-state + kill-9 discriminator proves it's a REAPABLE (T/S) approximation → true uninterruptible-D and the mode:off-without-`--safe` legacy hang are **NOT-COVERED** (shared-host wedge hazard; `docs/deploy-tier-gotchas.md` OQ-2) (23 assertions) |
| `80-session-isolation` | GREEN (S2, N=1 + 2 agents): dual-tenant session isolation on the real auth_callout stack — non-member activation refused at CONNECT + `current_session` NOT persisted; **bidirectional pub+sub cross-session ACL denial** (bare-`nats` impersonation with the tenant's nkey + `tether-cli:<sid>` name → NATS **protocol-layer** `Permissions Violation`, verified live) + app-layer node isolation (`node_not_found` across tenants) + in-session non-owner `not_owner` (session rm / node upgrade); wrong-PIN→correct-PIN neg→pos with `pin_failed`/`member_joined` sys.events (fresh-identity, post-sub, ordered so Arm R can't pollute the capture); **PIN rate-limit probe — 探索→定格 gotcha #25** (10 same-IP **WRONG**-PIN attempts, ≥10 `pin_failed` events captured = the §E.6 limiter's real Argon2-failure trigger fired; then the 11th same-IP **CORRECT**-PIN join STILL SUCCEEDS = §E.6 «≤10 attempts/IP/min» unimplemented, no source block; INVERTED `assert_ok`, FLIP on fix); `TETHER_SESSION` dual-shell no-crosstalk (42 assertions) |
| `81-admin-evict-session-rm` | GREEN (S2, N=1): broker admin unix socket (0600/0700 **OS-level EACCES** for a non-authorized user, no app string) + agent evict (~1s cross-process self-exit via `sys.events{agent_evicted}` + reconnect refused with an agent-specific auth string) + **evict-cleanup GAP — 探索→定格 gotcha #26** (a managed OS child LEAKS under a setsid-nohup deploy while the broker DB cascade-removes its row = divergence; under systemd the cgroup reaps it — **counter-proved as NOT tether's doing**; the public expose port is cleaned incidentally by the tunnel drop; INVERTED `assert_ok`) + evicted-nkey re-join with a **byte-identical** nkey (md5==D0; DOC-6 eviction≠revocation) + session rm 3-phase (history-`<sid>` JS stream delete / SQLite rows gone / `session_destroyed` sys.event, all RESULT-polled) + `session_deleting` probe (DOC-11: post-rm the refusal is a broker-side auth_callout CONNECT-deny, no agent broadcast; the app-layer IsActive gate is hermetic-only) (40 assertions) |
| `82-agent-onboarding-invite` | GREEN (S2, N=2 + fresh agent + S0-ingress): the real C2 onboarding journey — **gotcha #27** (well-known `manifest_listen` unbound after bare `cluster init` → C2 discovery leg dead) asserted FIRST, then a **labeled** operator enable; **S0-ingress** = per-broker **same-netns** python-stdlib HTTPS reverse-proxy sidecar (`image/ingress-proxy.py`, `--network container:<brk>`) + instance test-CA (reused from `lib/secrets.sh`) + trust injection; `cluster seeds publish --sid` mints the agent-join invite → **fresh** `agent join --start` → ONLINE + agent.yaml at the real 0600/0700 path + roster-cache pre-warmed via a **real HTTPS manifest fetch + AdoptDecision** + `config refresh` (real Go-x509 verify through the TLS front) + doctor; **wrong-SAN / untrusted-CA negatives** (real x509 hostname + issuer verify); grow → `roster_gen` convergence (NOT `agent_roster_stale` — 6-min grace, NOT-COVERED); forged/tampered invite refused before write (T1 no-residue; T2 pins the EXACT residue reality → DOC-7); user-service spike → NOT-COVERED (no container `systemd --user`) (29 assertions) |
| `70-expose-journey` | GREEN (G-A/S3, N=1 cluster + agent): the full expose data-plane journey on the real reverse tunnel — expose --local + **curl the public port cross-container returns the EXACT sentinel** (end-to-end tunnel, not a TCP connect) + ps PORTS (has HOME=brk1 — explore→pin overrode the plan's "no HOME" from usage.md, since `init` makes N=1 a CLUSTER) + explain (rebuild on, B5 footer) + --remote-port/port_taken/port_out_of_band/name_taken + **--on-broker self SUCCEEDS** (eligible voter, not on_broker_single_mode) + expose rm → connection-refused (exit 7, not `!curl -sf`) + immediate port reuse + **agent restart → state.json token reconnect** (P6: STOP→refused→START, same port ∧ moved==false; explain has no epoch → `.moved` oracle) (28 assertions) |
| `71-expose-rehome-failover` | **P1/R8 VERIFIER (G-A/S3, N=3)** — since R9-D arm B is a POSITIVE assertion, not a RED-exposed wall. R8 gave `cluster drain` data-plane rc semantics (rc=0 only once every migrated expose's agent ACKed the new home epoch; otherwise exit 75 `dataplane_not_converged`) AND made the home directive an ACTIVE, re-delivered push instead of something only a NATS reconnect could carry. Arm B is that fix's deploy-tier verifier, with three separated oracles: **B-cmd** (the rc is honest — rc=0, or rc=75 with the EXACT authored string; the pre-R9-D drill classified the product's own fix as "a wrong refusal" and went RED on it), **B-migrate** (the TERMINAL data plane — the expose is homed on a SURVIVOR voter and that broker's public port returns the exact sentinel over the real tunnel; never an intermediate "a directive was published" artifact, since a directive nobody applied IS the P1 bug), and **B-silent** (the P1 PREMISE — agt1's MainPID is unchanged AND its own journal for the drain window shows >=1 active home-delivery line and ZERO re-register/rebuild lines; without it one incidental reconnect would let B-migrate pass in BOTH the fixed and the unfixed world). NB arm B deliberately drops `--now`: `--now` collapses the convergence deadline to `now` (clusterstatus.go:776-779) so rc=75 becomes the only reachable outcome and the drill would be measuring its own flag. Measured 2026-07-19: drain returned **rc=0**, the expose migrated + served, zero agent re-registrations. If a lingering #31 membership op fences the drain instead, that is a signature-guarded PRODUCT-RED plus a gap — the drill refuses to invent a migration verdict for a command that never ran. Still pinned unchanged: **gotcha #29** — a cluster expose's home is NOT deliverable to a non-tunnel broker (net effect = tunnel-coupling, but the MECHANISM is un-homed fallback, R5-M3): `homeForExpose` (home.go:96-113) returns nil for a `--on-broker <non-tunnel voter>` → the agent's AddProxy falls back to its FIXED tunnel broker (tunnel_adapter.go:76-77) → REGISTER denied `token_unknown_or_revoked`; and a crashed home STRANDS a regular expose cluster-wide (rehome_events.go:52-53). **FIXTURE gate (HARD, R6-M2)**: agt→brk3 tunnel is INTERMITTENT — a HARD assertion RED-exposes it when it doesn't establish in 200s (never a silent GREEN NOT-COVERED). **Combined crash (Arm C rebuild-ON + Arm D rebuild-OFF, ONE injection)**: `node_kill brk3` → BOTH dead on EVERY live voter (curl exit 7, F5) + epoch UNCHANGED → `node_start brk3` → BOTH recover on the SAME port/epoch. **Arm A**: `on_broker_unknown` negative + no row. **Arm E (rebuild-OFF drain refusal)** is directly executed with the exact `will NOT be auto-migrated` signature (clusterdrain.go:665). G/F remain registered gaps (G needs a drained-then-returned home, F an N=1-eligible topology). **Hermetic-coverage (R8-M3)**: the rebuild-OFF refusal IS hermetic-tested (`test/d7/integration_test.go testD7DrainRefusesRebuildOff`) and R8's own invariant test proves the silent-agent claim against a fake agent; what drill 71 uniquely owns is the same claim on the REAL stack. Raw sys.events have no operator reader (owner-decisions D2) |
| `72-proxy-subscription` | GREEN (G-A/S4, N=1 cluster + 2 agents): the proxy subscription journey — owner-only `proxy on` (--yes gate; member `not_owner`) + member-readable no-secret status + sub create (URL once, dup-name conflict) + /sub Clash YAML (loopback verify + cross-container via S0-ingress) + forged-token 404 + **real Shadowsocks dual-leg** (agt1 `allow_private` POSITIVE + agt2 default-deny NEGATIVE via journald blocked-dest + wrong-PSK AEAD discriminator via agt1-NO-sink-bytes — Stage-C harness-safety-2: on the allow_private exit, no bytes = pure AEAD since it never dest-blocks; agt2-journal-distinction proved unreliable) + cross-container-direct-to-brk1:8090→000 loopback-only NEGATIVE (Stage-C dp-3) + external-review M3 DATA-PLANE `sub revoke` (each sub has its OWN PSK via activeProxyKeys → revoke drops alice's PSK from the agent keyset while bob's stays): alice+bob SS legs both flow before revoke → after `revoke alice`, **alice's SS leg BLACK-HOLES while bob's STILL flows** (per-sub cut, not a blanket outage) + alice2's new PSK RECOVERS the data plane (not merely /sub 200); + `proxy off` **DATA-PLANE** cut (the alice2 SS leg black-holes — exit torn down) + `_off_semantics` now HTTP-200 GATED (a 404/empty fetch can no longer vacuously satisfy `! grep type: ss`) + token/PSK/YAML secret-logging removed. **R5-M1 NOW-COVERED**: BYTE-OBSERVED in-flight force-close — a held-open THREADING-slow-sink SS stream via alice whose received-byte count is proven STRICTLY GROWING pre-revoke (REV-hold-base, so a stalled/never-connected curl can't false-green it) is FORCE-CLOSED on `revoke alice` (its curl exits early with bytes frozen) WHILE bob's held stream KEEPS transferring (still growing); + `__proxy__` reclaim after `proxy off` checks the OS listener (OFF-port-reclaim, `ss -ltn`) AND the port_allocations SOURCE dropping to 0 (OFF-alloc-reclaim, sqlite3) AND SAFE same-port reuse (OFF-reuse serves a sentinel through the reclaimed port). Each FAILS (exposes the gap) if tether does not force-close a transferring stream / does not release the allocation / leaks the port |
| `73-proxy-cluster-ha` | GREEN (G-A/S4, N=3 + 2 agents + ctl, 40 assertions): proxy cluster HA — CENTRAL finding: a proxy exit is **STRUCTURALLY IMMUNE to expose's #29**. `__proxy__` home is LOAD-SPREAD across Eligible voters (`proxy_reconcile.go:314` fewest-homes, NOT tunnel-tracking), YET SS egress stays LIVE through an exit homed OFF the tunnel broker, because the proxy Home directive carries HomeBrokerAddr+CertPins and the agent ACTIVELY DIALS its home (`tunnel_adapter.go:80` OpenHome) — exactly the delivered-named-home case that an EXPOSE never reaches (#29 is un-homed fallback, drill 71). Proven steady-state (SS via a non-tunnel-homed exit → curl RFC1918 sink returns bytes). **gotcha #33 — MEASURE-AND-RECORD (no die, no inverted assertion, no fixed lag, R5-M7)**: after killing an ESTABLISHED exit's home broker (quorum kept 2/3), `[#33-a]` the crash DETERMINISTICALLY severs the tunnel so the SAME pre-crash client black-holes; `[CONTROL]` home leaves the dead broker + exit reaches ready (die-gated); `[#33]` a 180s poll MEASURES whether the SS DATA plane auto-recovers and records **AUTO-RECOVERED (with the lag) or STRANDED — BOTH accepted** (the QUORUM `proxy off; proxy on` heal proves manual recovery when STRANDED). No claim of any fixed lag / eventual-recovery promptness / root cause (the round-3 readiness oracle and round-4 240s-inverted-gate wordings are both RETRACTED; ApplyHome→OpenHome is an atomic session replace+redial). + readable `proxy status --cluster` (member, no secret) + sub create / loopback-200 / cross-container-ingress / forged-404 + `sub revoke` (carol 404 WHILE alice 200) + **quorum-loss freeze DATA-PLANE separation, R5-M6 causally gated**: `cluster rebalance proxy`+poll DETERMINISTICALLY builds a 1+1 spread over the 2 live voters; the dead-homed exit's /sub-VENDED server is cross-checked == the broker about to be killed (Q-xcheck), the killed broker is proven genuinely DOWN (Q-dead), the dead SS leg's black-hole is CAPTURED as a HARD PREREQUISITE, and the SEPARATION claim (/sub still 200 WHILE the dead leg is DEAD) is a COMPOUND gate on that black-hole actually holding — a dead leg that kept serving FAILS (RED), never a false "DEAD while 200" conjunction (the solo2 accumulation bug); a non-black-holing leg is DIAGNOSED (tunnel-survival vs teardown). + control-WRITE fence. #30 (cluster-revoke emits no `proxy_keyset_changed`) NOT-COVERED — sys.events has no operator reader (owner-decisions D2) |
| `74-rebalance-on-return` | **RED-EXPOSES (G-A/S4, N=3 + 3 agents + ctl; g7-review m11 sim leg)** the proxy-distribution instability (**gotcha #34**: 1/1/1 doesn't hold — non-tunnel voters' proxy-eligibility is unstable, exits re-pile on the tunnel broker) + moved-exit data-plane (#33) + auto-rebalance gaps as release-blocking (R6-M1). Round-5's measure-and-record masked ALL of this as GREEN — reverted to HARD assertions. **RED, per-run (the failed COUNT is intentionally branch-dependent — do NOT read one number as the stable verdict)**: observed 1–7 failed across runs (clean retry = `RED 1/35`, only C-auto; noisier runs `RED 3/33`, `5/31`, `7/29` when transient setup-SS / reconstruct legs also RED). The STABLE release-blocking REDs are B-dp (#33 moved-exit strand) + C-auto (#31 fire-gate); the total varies with how many transient setup/eligibility legs also fail that run (each such RED is itself the #34 instability exposed). Runs through (no die-abort): `cluster rebalance proxy` __proxy__ home distribution with a **FAIL-CLOSED validated snapshot (R5-M2 + R6-M1: 3 DISTINCT nids)** — every per-broker home COUNT comes from ONE `proxy status --json` document validated for command-rc / JSON-validity / exactly-3-rows / all-homes-a-real-voter; an empty/malformed/partially-un-homed status returns a unique invalid sentinel so spread/stability/dry-run can NEVER certify balanced/stable/zero-change (the old code counted an empty list as spread 0 = false GREEN). **LOCKED 1/1/1 baseline**: the non-deterministic initial reconcile is rebalanced to one-per-voter (spread==0) + an SS leg is proven FLOWING before any skew. Arms: kill the home-heaviest non-leader → its exits rehome AWAY (count→0) + `node_start` RETURN → rejoins VOTER + **default-off** stays at 0 homes (dry-run proves the returned voter is ELIGIBLE, so it's a real "won't auto-move-back", not "not yet") + `--dry-run` ZERO-change (byte-identical, settle-gated) + **real rebalance evens** (spread≤1, exits move BACK onto the returned voter) + **B-dp DATA-PLANE (HARD, R6-M1)**: an SS leg MUST flow bytes through the rebalance-MOVED exit within 240s — RED when it STRANDS, EXPOSING the #33-family moved-exit stranding as release-blocking (round-5's measure-and-record accept-both was a unilateral acceptance change, reverted; the DISTRIBUTION is separately hard-proven by B-real + all-three steady-state SS baselines, driven HERE not delegated to 73) + **B-negctrl NEGATIVE CONTROL**: a co-homed ordinary expose is NOT moved by the __proxy__-only rebalance + no-agent-left-unhomed. **Arm C (HARD, R6-M1)** TETHER_AUTO_REBALANCE=on auto EFFECT — the distribution MUST auto-even within the locked 180s window; RED when the auto path does NOT fire, EXPOSING the auto-rebalance-on-return gap as release-blocking (the EFFECT is NOT carved out; only the raw EVENT is). Also fixed: snapshot requires 3 DISTINCT nids; dry-run/real rebalance require the command rc=0; the negative control is non-vacuous (created+validly-homed+serving, no `single` fallback). **NOT-COVERED (ONLY the raw EVENT)**: the `proxy_auto_rebalanced` count==1 EVENT — sys.events has NO operator reader (owner-decisions D2, raw-event only), bound to the readable one-per-voter distribution + Arm-C data plane |
| `31-node-upgrade-fleet` | **PRODUCT-RED** (G-A/S5, N=1 cluster + 2 agents) — under the 5-verdict contract this drill lands PRODUCT-RED because its `assert_bug` reproduces **gotcha #28** (a known defect surfaced = the harness working, a distinct non-green EXPECTED state; NOT GREEN). The agent `node upgrade` surface — **gotcha #28** (a broker-allowlisted self-hosted mirror URL REFUSED by the agent's hardcoded local allowlist `url_not_allowed_local`, 3-point discriminator [agent can fetch+trust / broker whitelisted / agt1 ONLINE-owner] makes it the ONLY possible wall; the refusal msg literally says "check the agent's --upgrade-url-allow flag" — a flag that doesn't exist = **DOC-3**; `assert_bug`) + GREEN broker negatives (`url_not_allowed`/`sha256_invalid`/`not_owner`) + S0-artifact (https tether-next tarball + instance CA). url_allow config is a non-destructive broker.yaml append BEFORE session (a broker restart after ctl login breaks the session). + external-review M4 FLEET arms (independent of #28's blocked success): `node upgrade --all` ONLINE enumeration (both agents in the target set) → OFFLINE-enumeration EXCLUSION (stop agt2 → it drops out of `listOnlineNIDs`, the exact filter --all uses) → `--all` DISPATCHES to the ONLINE agt1 + config-ABORT on the #28 `url_not_allowed_local` (never dispatched to the OFFLINE agt2) → `--all --timeout 0` transient-SKIP-continue + skip summary (--timeout threaded). A SUCCESSFUL fleet upgrade (PID/version) stays NOT-COVERED (walled by #28) — honestly, not faked (28 assertions) |
| `32-install-lifecycle` | GREEN (G-A/S5, fresh un-provisioned container): install.sh lifecycle — `--dry-run` zero-write (3 roles, md5-stable tree) + never-start invariant (`! pgrep tether`) + `--uninstall` → units gone + reinstall idempotent + ownership (/etc/tether root, nats.d tether) + agent `BIN_DIR=~/.local/bin` (S0-layout, node-upgrade atomic-replace target). + external-review M5: the zero-write oracle is a CONTENT+METADATA manifest (per-file type/mode/**numeric uid/gid**/size/sha256/link-target + symlink own-mode/owner + an other-type catch-all, not just path names — a self-test proves mutating one byte MOVES the digest) + ctl/agent dry-runs run as the SIM user (not root). **round-4 R4-M4 (GREEN, 17 assertions)**: manifest hardened to numeric `%u|%g` + `while IFS= read -r` (embedded-newline paths a noted residual — dash `read` has no `-d ''`) + **fail-CLOSED** (an empty / stat-error / hash-error manifest can no longer certify zero-write — it returns a sentinel that never matches a valid snap) + a **byte-exact self-test restore** (cp -a backup/restore, not a sed line-delete). **REAL agent binary install NOW-COVERED**: a real S0-artifact https tarball → install.sh downloads + verifies the sha + place_binary EXTRACTS + atomic-places the binary at ~/.local/bin/tether; it RUNS + never-starts + uninstall removes it (NOT --skip-download, which skips place_binary). **R5-M4 NOW-COVERED**: (1) the manifest FAILS CLOSED on a partial/permission-failed `find` traversal (MANIFEST-FIND-ERR — the old `find|grep|sort` lost find's rc); (2) ONE combined EXIT trap (the two traps overwrote each other, leaking the artifact container on early failure); (3) REAL **ctl** binary boundary — its OWN place_binary/run/never-start/uninstall (not "same as agent"); (4) **§8.4 single-broker manual upgrade IMPLEMENTED** — a live N=1 broker: stop tether-broker → swap the binary (privileged step) → `sqlite3 PRAGMA integrity_check == ok` (sqlite3 added to the image) → start → G.2 business convergence (the original expose serves the sentinel AGAIN + a NEW post-upgrade expose serves — real read-write) with version-flip ∧ MainPID-changed guards. §8.4 is the OPERATOR manual path (systemctl+install), NOT the #31/#28-blocked `cluster upgrade`/`node upgrade` verbs |
| `30-rolling-upgrade` | **GREEN-on-mechanism (G-A/S5, N=3 cluster + ctl; G5 #8)** — REWRITTEN R9-D, after the 2026-07-18 full-suite review found this drill's G5 coverage was NET ZERO (H1: a version oracle querying a schema the product never emitted ⇒ permanently empty; H3: a discarded roll rc and a roll.log that never reached the operator). R4 fixed the oracle; R9-D added the mechanism. **OQ-6 supply (`drills/lib/colocated.sh`)**: the sim now provisions a tether AGENT co-located on a broker host, which had never existed, so the whole-host leg was structurally uncoverable and stood as a permanent `not_covered`. All THREE P3 presence states are built in ONE cluster: **(a) DECLARED** (broker.yaml `colocated_agent_nid=colo-<host>`, nid != node_id so `assumed:false` proves the declaration — not the convention — was consumed), **(b) OBSERVED** (nid == node_id, undeclared ⇒ `assumed:true`), **(c) ABSENT** (the leader runs no agent ⇒ its plan step must read `[broker-only]`). **The (b)-state HALT**: the declared agent is STOPPED and the roll must HALT loudly on it (P3: a declared agent stays PRESENT while down) — reproducing `HALTED at <host>: agent re-exec refused: agent_no_responders` verbatim. A roll that SUCCEEDS there is an ASSERT-FAIL: silently skipping a declared-but-down agent is the P3 bug. Because the HALT is aimed at the FIRST hop, the partial state is exactly checkable — **per-hop advance** (only that host's broker moved; the other two untouched; leader-last held on the failure path) and **skew=true** on the half-upgraded host, then skew CLEARED after the repair. **The way out**: a HALT deliberately leaves the roll lock held, so `tether cluster unlock` (R7b; drill-coverage owner R9) is exercised for real — the default clear REFUSES a lease still in the future, `--force` clears AND confirms, and the TERMINAL proof is that the NEXT roll gets past acquire-lock (not the unlock command's own success message). **Mechanism, not just end state**: the roll's own log must show it re-exec'd the OBSERVED host's agent and declared that host `(broker+agent) at <target>`, and must show the repaired host SKIPPED as `already at target` (idempotent resume) — an end-state-only drill would credit `cluster upgrade` for an advance the operator's daemon restart performed. #31 is now PROBED directly (`cluster unlock --grow --dry-run`, zero mutation) BEFORE the roll instead of being inferred from a roll that trips over it: a held marker is a PRODUCT-RED plus tether's own `cluster unlock --grow --force` remedy asserted to confirm the clear; a clean marker is asserted as the KEPT R7a invariant. Retained: privileged staging (labeled, NOT automatic) + `--expect-sha256` anchor + broker-local ctl login + `--account-seed`/`--backup-taken` refuses + ordered-roll dry-run + PID-preserving re-exec + a raft write probe run over BOTH the failed and the completing roll. **Harness bug fixed in the same batch**: the write probe used to run under the ctl's own `$HOME`, and `session create` rewrites that HOME's active-session pointer (session.go:69) — so every session-scoped read after the probe started was answered for an empty throwaway session (`node ls` printed "(no nodes)", the AGENT half of `node ls --brokers` read `?`/skew=false on a genuinely skewed host). The probe now runs in an isolated HOME and the ctl's session pointer is asserted intact. Remaining stated-reason gaps: (b) the 30-C N=2 write-fence NEGATIVE control, (c) a LEADER-hop HALT/resume |

**S6-S8 batch (G-B) — historical round-3 snapshot, superseded by strict round-4.** The table below records
the coverage state that round-4 reviewed; it is not an acceptance target. Round-4 removed the listed
`not_covered()` theme gaps, strengthened the positive oracles, and changed the runner so PRODUCT-RED and
INCOMPLETE block by default. A current run is judged solely by its actual unique verdict; no drill is
"expected non-green" in a way that permits release.

| drill | expected verdict | scenario |
|---|---|---|
| `22-forcesingle-online` | **INCOMPLETE** (or PRODUCT-RED if #35 reproduces) | S6, N=2: the ONLINE force-single escape hatch — dwell + refusal gates (quorum_not_lost / peer_alive / #36 online-`--yes`) + protected-mode set-raft-addr + a total-function Arm-0 (POSITIVE commit via MainPID-unchanged in-process, or #35 unreachable-dwell). #35 downgraded to a CANDIDATE (reproduced → PRODUCT-RED via a PROVEN survivor-restart MainPID change; else recorded INCOMPLETE). GATE-d replay + TAMED fidelity are `not_covered()` |
| `40-drain-retire` | **INCOMPLETE** (or PRODUCT-RED if #31/#38) | S6, N=3: topology shrink via drain/retire on the admin-socket membership plane — drain round-trip (DRAINING observed) + ops-schema + reconcile-`--plan` refusal/zero-write + Tier-2/machine-confirm negatives. The retire SPINE is `#31`-intermittent: `product_red` when the grow-lock leak blocks retire, or `#38` (retire stalls at NATS_ROLLED_OUT) via `assert_bug`; a converged retire is GREEN. OPS-ABORT/ADD-dryrun are `not_covered()` |
| `41-shrink-to-standalone` | **GREEN** if shrink converges, else **PRODUCT-RED** (#31/#38) | S6, N=3→1: shrink to a lone standalone — peer-present refuse + set-raft-addr rebind + before/after voter count + a raft-replicated SESSION survival oracle + JS-reset-then-broker-active + 3-way to-standalone (de-cluster + tier-B at N=1 + R3-persist). `product_red` if #31 grow-lock or #38 stall blocks the retires |
| `42-rejoin-returning` | **PRODUCT-RED (#47)** | S6, N=2: the returning-node recovery surface — diagnose pos/neg (alive-refuse + dead-peer pasteable) + rejoin-prepare O_EXCL WIPE-REFUSED on an intact DB + resnapshot single-voter + Tier-2/machine-confirm. **R9-D**: H8's `--ack-alerts` fix (the fixture push was being blocked, correctly, by the `force_single_active` severe gate — a KEPT design guard, now PINNED by its own assert_refuses) moved the failure point from assertion 27 to 37, which finally let the whole rejoin/resnapshot SPINE execute — audit-window refusal, explicit `--accept-audit-loss`, no stale-peer resurrection from the raft tail, identity-only manifest, `init --from-manifest`. **All of it is now GREEN (42 assertions).** The one remaining failure is the TERMINUS: the returning node cannot re-grow. It is no longer a blanket SETUP-RED (which claimed the drill's own fixture broke, when every fixture step is asserted green above) — the grow now runs OUTSIDE the assertion with its full output as first-class log lines, and the outcome is CLASSIFIED: a CATCHING_UP signature lands PRODUCT-RED against **#47** (owner: R9 product side — R9's own risk 2 named this drill as the honest verifier, since the AddVoter/isVoter branch is unreachable in a hermetic single-node raft), a force-single signature lands PRODUCT-RED against the one-way-escape-hatch finding, anything else is an honest ASSERT-FAIL. The downstream post-rejoin arms are recorded `not_covered` rather than run over a node that never rejoined (a push that happened to succeed through the surviving N=1 broker would read as a post-rejoin success it is not). **#49** (resnapshot resurrecting a pruned peer) is re-verified GREEN here |
| `43-migrate-live-data` | **INCOMPLETE** | S6, N=1→cluster: the runbook §4 standalone→cluster cutover safety surface — init `--check` zero-write (byte-exact) + `--yes`/machine-confirm negatives + from-existing cutover + **3-way rollback** (DB byte==bak + cluster-mode off + bootable standalone). Business-row SURVIVAL + the E cluster-ization candidate-gap are `not_covered()` (a bare P2 broker doesn't serve NATS sessions — SB-43) |
| `90-alerts-lifecycle` | **INCOMPLETE** | S8, N=3: the operator alert lifecycle on the store-backed plane — manual raise→member ls→severe banner→ack(≠clear)→clear(idempotent) + real broker_down (kill a follower) + quorum_lost-ack refuse. Absence predicates fail-CLOSED (valid-JSON guarded). M6 disk_pressure (#39 fixed 5-min interval) + below_quorum/raft_lag are `not_covered()` |
| `91-client-converge` | **INCOMPLETE** (or PRODUCT-RED if #G3) | S8, N=1→2→3: the client-view auto-convergence journey — A1 publish + A2 grow-auto-include + C offline force-single survivor-only. `#G3` = `product_red` when brk3 reaches VOTER but never appears in `seeds show` (a real auto-converge omission). D cli-failover + A3 retire-drop are `not_covered()` |
| `92-js503-remote-alert` | **INCOMPLETE** (or PRODUCT-RED if #42) | S8, N=2: the `cluster status --remote` quorum-loss / force-single surface — (a) quorum-loss READ-ONLY self-correct (`#42` `product_red` if it never self-corrects) + `session rm --ack-alerts` proven to REACH THE WRITE PATH (not the gate, not a connect/auth error) + a 12 MiB tier-B corroboration. (b) online force-single JS-503 banner + recovery are `not_covered()` when the sustained-503 doesn't arise |
| `93-metrics-observability` | **INCOMPLETE** | S8, N=3: the observability surface — /metrics true values (leader-gated peer rows) + /healthz & /readyz (HTTP status + body, `ready` excludes `not ready`) + the alert webhook (raised + cleared transitions, no-secret) + --card/JSON. LOGJSON seam + --watch (needs a container PTY) + all-down ROSTER_UNREACHABLE + READYZ-503 are `not_covered()` |
| `50-backup-restore` | **PRODUCT-RED** (#50/#64/DOC-27) | S7/G-C, N=2: `cluster backup` (leader/follower/offline) + the `recovery restore` gate family (13 gates incl. the foreign-bundle anchor that MUST pin gate 10 not gate 9) + `doctor --offline` + incident export. Pins **#50** (doctor `--db <nonexistent>` reports 0 fatal — lazy `sql.Open`), **#64** (restore prunes to a lone voter but leaves nats.conf clustered → crash-loop, self-heals only via the surviving peer), **DOC-27** (runbook backup example fails on stock install). 68 assertions |
| `51-full-dr` | **PRODUCT-RED** (#51) | S7/G-C, N=3→total-loss→1→2: runbook §5.2 full-cluster DR executed literally, with a DR-STEP-LEDGER quantifying the undocumented steps. Pins **#51** (`recovery restore` cannot apply the broker.yaml cluster seam — no `--config` flag — so the documented "start the daemon" step FATALs on a fresh DR box). The DR-completion tail is `not_covered()` (the manual gap-clears do not compose into a working broker on the real stack — itself the operational consequence of #51/#52). S0-backup-vault survives the volume disaster (sha256-verified) |
| `52-credential-rotation` | **PRODUCT-RED** (#54/#56/DOC-23) | S7/G-C, N=2: `rotate-tunnel-cert` + the account.nk/CA rotation runbook + `cluster keygen` + the C7 guided-rotation lifecycle. Pins **#54** (account.nk rotation on a running cluster cannot proceed — reconcile goes UNREACHABLE/false-all-clear, and doctor is blind to the skew), **#56** (rotate-tunnel-cert's circular follower↔leader advice), **DOC-23** (the bricked-broker remedy is unreachable). The live-rotation completion + C7-on-broken-auth tails are `not_covered()` |
| `94-agent-reconcile` | **GREEN** | S9/G-C, N=1 + 2 agents: G.1 reconciliation — the missed-exit direction (foreground exec → `docker kill` agent → `EXITED(-1)` + `reconciled_closed` audit, node-scoped) + the ORPHAN direction manufactured entirely through product paths (backup → `run` PTY process → restore an older bundle → `restart nats-server` → re-register → `killed_orphan` with no rc) + `ps` LOST + the G.5 port-reconcile face. 51 assertions, all invariants |
| `95-broker-selfheal` | **INCOMPLETE** (GREEN body) | S9/G-C, N=2: G.2 restart reconciliation + the FIRST behavioural proof of **#23** (a SIGTERM to MainPID exits CLEAN → `Deactivated successfully` ∧ NOT `code=`, and `Restart=always` revives it; a kill-9 exits `code=killed, status=9` — the two journal wordings are each other's discriminator, and with `SuccessExitStatus` unset only exit-0 counts as success so `on-failure` could not have revived it). T3 nats-restart survival is a hard claim + recorded mechanism. DELETING boot-resume is `not_covered()` (raft + JS share fate at N=2, so the middle state cannot be established in-sim) |
| `96-mid-flight-chaos` | **PRODUCT-RED** (#58; #65 non-deterministic candidate) | S9/G-C, N=3: failures injected MID-FLIGHT via the S0 partition primitive (iptables silent DROP, self-proven by the 124-hang discriminator with 4222 deliberately left up as the selective control). Pins **#58** (orphaned transfer objects never reaped on a non-leader home broker — **LIVE-CONFIRMED across 3 fresh-instance re-runs** via the external-review-corrected /jsz **object-count** oracle: it counts `state.messages` in the OBJ_xfer bucket with a clean-transfer baseline, not the persistent stream's mere existence, so `baseline=1 → orphan=2 → still 2 after the reaper ran` is a real orphan, not the stream-existence false-positive the old `grep -c OBJ_xfer` gave). Surfaces **#65** (a partitioned-minority stale-leader write **sometimes** survives to majority-visibility after heal — the D6b per-broker RAW-ARTIFACT readback shows it durable in 2/3 re-runs, rolled back in 1/3, so it is a **non-deterministic raft-safety candidate owed to product-side root-cause**, NOT a deterministic PRODUCT-RED — this is the honest resolution of the reviewer's B10 "durable vs rolled-back" contradiction: the phenomenon itself is non-deterministic). The flagship leader-partition arm is GREEN (partition self-proven rc=124 + brk1 alive/stable + survivors elect + majority commits + no split-brain-LOSS at the RESULT level after heal). **#57** (in-flight transfer audit) is `not_covered` in-sim (loopback-speed transfers finish or dangle unreadably before catch; mechanism source-certain, hermetic-owned); the run-PTY / expose-crash arms are `not_covered` (#29 territory / DOC-28); the double-fault arm gates `not_covered` unless the cluster fully recovers from the partition arm first (cross-arm isolation) |
| `97-soak-cycles` | **GREEN** (thresholds UNCALIBRATED) | S9/G-C, N=3, `SOAK_CYCLES=6`: the parameterised chaos soak — four rotating injections (agent kill / broker restart / partition / transfer-concurrency) with a fd/RSS/Threads leak oracle (bounded high-water + slope, the measured process never a victim, PID-generation guarded) + an orthogonal panic/FK/corruption scan. Each injection self-proves its own effect; the transfer-concurrency type proves BOTH the restart (brk3 MainPID change) AND that the transfer entered the product path (a `start` history row) — a restart-disrupted pull that never registers records that half `not_covered` rather than passing on the restart alone (ext-review round-2 R2-F2). goroutines + P8's 24h parity are registered at batch level (no product observation port; owed to staging) |

> Pre-S6 drills above still carry their pre-contract verdict labels; the runner's rc cross-check and
> `tests/lint-drills.sh --all` advisory track their migration to the verdict contract as a follow-up.

**Strict round-4 evidence (2026-07-16, `-j1 --no-retry`).** The first post-remediation run landed
22/43/92 GREEN; 40/90/93 SETUP-RED; 41/42/91 ASSERT-FAIL. Those REDs are evidence, not results to retry
away. Fixture defects in 90/93 and one invalid audit oracle in 42 were corrected, while the product
findings they exposed were fixed behind independent regressions. Post-fix remote reruns are still pending
because the execution platform rejected further remote use on quota; therefore the batch remains **Fail**,
not “all green.” See `docs/reviews/s6-s8-external-review-round4-implementation.md`.

## Real `tether` findings surfaced (2026-07-05)

This tool caught cross-process deployment defects unreachable by the in-process suites, logged in
`docs/reviews/v0.4.5-ha-grow-ops-gotchas.md`:

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

### Grow gaps — status after G4 (`cluster add` drives grow)

Since G4, `grow` drives `tether cluster add` end-to-end, which reclassifies the old grow gaps:
- **#3** (auto `reconcile nats --all` can't harvest route mTLS on the first grow) and **#4** (standalone JS
  doesn't migrate into the clustered meta): now handled INSIDE `cluster add`; drill `11` asserts their old
  workaround signatures are **ABSENT** (a regression would re-introduce the signature and flip the drill).
- **#5** (init has no machine-escape confirm) and **#10** (clustered-alone joiner exit-70 — the
  mesh-before-joiner ordering dodges it): re-anchored as covered by drill `11`'s **structural gate A**
  (`cmd_grow` runs no hand `cluster init/join/reconcile-nats`/JS-reset), NOT by a behavior signature —
  kept labeled so the coverage gap stays visible, not deleted.
- **#24** (CN-only cert) and **#23** (clean-exit-on-nats-loss — see above): still the honest,
  signature-unpinned backlog; #23 has a `doctor` `Restart=always` drift guard but no deterministic RED drill.
- Deliberately out of scope: **#6** (root-owned `tether.lock`) — the sim standardizes on `User=tether` for
  `cluster init` (which produces the correct post-fix ownership), so it does not reproduce #6; a root-init
  drill would be needed to surface it.

One item to catalogue as a tether OBSERVABILITY gap (candidate gotcha): after a grow the health label
lingers `DEGRADED-WRITABLE` because the topology observer keys convergence on `/varz config_load_time`,
which `nats-server --signal reload` never advances on a byte-identical reload — so tether can't confirm
the load even though routes/JS/streams are fully converged (this is what made the fleet's post-grow
DEGRADED look like #22).

## NON-GOALS

Single host (cross-process, not cross-host WAN raft); no ACME/Caddy/real domains (plain `nats://` on the
trusted bridge, route mTLS is real); v2/`main` only; not a load/perf/fuzz tool; Linux/amd64 only; not a
CI gate and not a substitute for the real multi-host staging drill.
