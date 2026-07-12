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
| `run-drills.sh` | server | run the WHOLE drill suite in PARALLEL — inotify preflight + concurrency cap (`-j`) + infra-flake re-run; the preferred way to run all drills (see Drills) |

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

Run on an isolated `--instance`; each prints RED/GREEN. GREEN = an invariant that must hold; a **RED
drill** asserts a *currently-broken* behavior that has **no green code path today** (open backlog),
signature-guarded (`lib/assert.sh`) so it can only pass for the DOCUMENTED cause — flip it to a plain
GREEN regression the day the product fix lands.

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
