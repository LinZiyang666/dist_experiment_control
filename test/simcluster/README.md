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
- **NOT part of `make test` / `make e2e-parallel`.** No Go `_test.go` here, no build tag — `go test ./...` never
  sees it. It is a separate, slower, deploy-tier gate you run before a release touches production.
- It complements — does not replace — the fast hermetic suites. The `d*_integration` suites run raft
  in-process with an embedded NATS; this runs the *real* out-of-process stack. It **replaces** "SSH to the
  real fleet and pray."

## Architecture

**The primary home is `weilandserver` itself** — it is both the main development box and the sim host,
so the normal path is to work in the checkout on that machine and drive `./simcluster` directly. Check
where you are first: `hostname` is `weilandserver` (note `/etc/hostname` says `weiland_server`), or
`hostname -I` contains `192.168.0.200`.

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
| `lib/stage.sh` | both drivers | Shared vendor staging (tether + tether-next + **tether-prev** + pinned `nats-server` + verified `caddy` + `nats`/`nk` + `install.sh`) and the `sim_is_sim_host` probe, so the two drivers cannot drift. `vendor/tether-prev` is the REAL previous release (hot-update S1): `sim_prev_tag` resolves the newest `v*` tag reachable from HEAD (or the next older one when HEAD sits exactly on a tag — never a literal, which would turn into "same version" on release day), `sim_stage_prev` builds it in a detached worktree with that tag's own Makefile/go.mod (fail-closed: a build failure never substitutes tether-next), and `vendor/tether-prev-source.json` `{tag, commit, binary_sha256, toolchain, built_at}` is the receipt `sim_prev_source_ok` checks. `vendor/tether-source.json` binds every staged tether binary to HEAD/dirty-count/sha for evidence. tether-prev is NOT baked into the image (the COPY list is image-identity); drills `d cp` it in. Hermetic controls: `tests/prev-supply-test.sh`. |
| `lib/supervisor.sh` | both drivers + `tests/run-all.sh` | The adapter the S8 supply path requires as `SIM_UPGRADE_SUPERVISOR` (`-- argv...`, caller's stdio/cwd/env, native child status): `sim_supervisor_build` compiles the repository's FIX-ORPHAN test launcher `test/fullstackharness/cmd/supervise` (over `internal/testharness.StartProcess`) from the caller's own tree; `sim_supervisor_provision` keeps an exported adapter or builds one for `--build`. Exit 125 = descendants survived the child, 126 = launcher setup failure — never success. Linux only. |
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
| `tests/hot-update-transfer-drill-test.py` | dev box | The r9d arm controls for drills 63/64 (`--arm <ARM>`; r9d fans `--arm all --results DIR` out once and gives every arm its own section). Runs the REAL drill file, driver and `transfer_mutations.py` in a private sandbox where only the deployment boundary is a recording double (`TRANSFER_PROCESS_SUPERVISOR` protocol; docker/ssh/systemctl/tether poisoned): each arm must reach its expected native verdict with every scheduled cell cut and be driven off it by its own fault; each control's kill is two-sided over its owner, roles, 120s window (M02/M12B) and native ASSERT-FAIL. No transfer, cut, kill or deploy receipt is claimed. |
| `tests/hot-update-rollback-drill-test.py` | dev box | The r9d arm controls for drill 38 (`--arm <ARM>\|all [--results DIR] [--jobs N]`; r9d fans `--arm all --results DIR` out once and gives every arm its own section). Runs the REAL drill file, its PY38 program and `upgrade38_native` mapping against the arm's REAL row in `drills/lib/upgrade_tx_arms.tsv` and `upgrade_fault_cases.tsv` under the shared FIX-ORPHAN supervisor (`SIM_UPGRADE_SUPERVISOR`; rc 2 without it or for an unknown arm). Only the deployment boundary is a recording double (`tests/hot_update_rollback_{fixture,faults,clock}_double.py`: the P26 fixture's live verbs, the supervisor's command execution, the fault injector's effect, the mutation build supplier and the calibration clock; docker/ssh/sudo/systemctl/journalctl/tether poisoned): each arm must reach its expected native verdict with a trace of exactly its declared operations, and bad input aimed at its own obligation (last CLI rc, proof set, R checkpoint/reverse receipts, W injector hit, A calibration interval, H reverse step, M causal kill receipt, I judge binding) must drive it to one specific other native status. Zero tests or any skip is red. No cut, rollback, kill or deploy receipt is claimed. |
| `tests/hot-update-compat-drill-test.py` | dev box | The r9d arm controls for drill 39's S8 compatibility arms (`--arm <ARM>\|all [--results DIR] [--jobs N]`, same fan-out). Runs the REAL `drills/39-hot-update-legacy-n1.sh` dispatch (`_upgrade_39_new_arm` → the REAL `drills/lib/hot_update_broker.sh` `upgrade_39_compat` → its embedded PYCOMPAT driver → `broker_native` → `drill_end`) under the shared supervisor. Doubles: `SIM_UPGRADE_COMPAT_SUPPLIER` is a recorder speaking the P24/P22 port protocol, every product command is a finite local actor exiting with the scenario status, and for ABI-adjacent alone `drills/lib/upgrade_tx_fixture.py` is replaced by a double of the P26 live-fixture interface (the real P26 judge needs a live cohort); wire plans come from the REAL `test/hotupdate/testdata/upgrade_wire_argv.json`. Each arm must reach its expected native verdict with its own CASES39 CLI statuses, and an independent per-arm pin of PYCOMPAT's checks, wire checks and config obligations goes red if the drill drops one. No compatibility, migration or deploy receipt is claimed. |
| `tests/hot-update-infra-arm-test.py` | dev box | The r9d arm controls for drills 85/86 and 39.NI/CI (`--arm <85.A\|86.R\|39.NI\|...\|all>`, same fan-out). Runs the REAL drill files (case branch, every `infra_drill_*` helper, `drills/lib/cluster.sh` grow fixtures, `drills/lib/logs.sh` `sim_log_snapshot`, `lib/assert.sh`, the FIX-ORPHAN supervisor exec). Recording doubles replace `$SIM`, the vendor supply verifiers, the release-source supplier behind `sim_prev_source_ok`, `$DOCKER` and the whole P32 `drills/lib/hot_update_infra.sh` seam, whose landed verbs differ from the ones 85/86 call (reported, not repaired). Each arm's recorded trace must equal its own obligation sequence (topology, rounds, hops, refusal/heal/halt/recover, reload/support steps, CLI binary and verb, the CLI rc forwarded to each judge call), and faults aimed at its distinctive obligation must reach an exact other native verdict. No infra cut, roll, refusal or deploy receipt is claimed. |
| `tests/hot-update-fullstack-arm-test.py` | dev box | The r9d arm controls for drills 99/100 (`--arm <99.ARM\|100.ARM>\|all`, same fan-out). Runs the REAL drill shell (supervisor re-exec, native case mapping, `drill_end`), `drills/lib/fullstack_fixture.py run` and `drills/lib/fullstack_mutations.py run-arm`; only the deployment boundary is a double (`tests/hot_update_fullstack_arm_boundary.py`: the P25 source-pinned tool rebuild, the P10 source archive, the Go assembly host, the P01 judge and the P19/P25 mutation host), and failed `require` criteria are recorded so each negative proves which guard fired. A 99 arm must reach GREEN on its own sealed profile and go SETUP-RED for a missing step or execution of its own runtime, a flipped topology, another arm's profile, a GREEN judge with unmet criteria, foreign running bytes or an out-of-vocabulary host status. A 100 arm must reach its 0→1→0 ASSERT-FAIL kill on its own catalog row and go SETUP-RED for a comment-only or unbound patch, a surviving or translated mutant, a red outside its window or at another assertion, an unclassified driver crash (Python's default exit 1 is the kill status) and its row-specific tier/cell/overlay/restore obligation; 100.M10C stays INCOMPLETE until P19/P25 supplies its targeted-cut probe sender. No fullstack deployment, cut, kill or judge receipt is claimed. |
| `tests/hot-update-process-probe-test.sh` | Hermetic process/image oracle controls for drills 34/35/39: synthetic positive and negative judges, real local pipes and FD census, pidfd cleanup, extracted shell predicates, artifact packing and Go source literal pins. S1 loss remains a negative of the S3 judge. Never runs tether or Docker. |
| `drills/lib/hot_update_process_probe.py` | Independent producer, process identity, FD census, continuity/image/legacy judges, freeze samples and pidfd cleanup. |
| `drills/lib/hot_update_budget.sh` | Deploy half of the single-source upgrade budgets (`internal/upgradebudget`): sourced by drill 34; rate is fixed, freeze/request samples are calibration only. |
| `drills/lib/hot_update_mixed_probe.py` | Mixed read/write/short request sensor for 34.P (MIXEDPROBE package); calibrated traffic, source-ledger side-effect judge and latency distributions. |
| `tests/prev-supply-test.sh` | dev box | Hermetic controls for the `vendor/tether-prev` supply: `sim_prev_tag` on a scratch git repo (HEAD past a tag / exactly on a tag / on the oldest tag / non-`v*` tags ignored), `sim_prev_source_ok` two-sided (wrong tag, swapped binary, missing/partial source.json refused), `sim_stage_prev` fail-closed (a failing build leaves nothing behind and removes its worktree; a matching cache triple never rebuilds) and `sim_write_tether_source`. Wired into `tests/run-all.sh`. |
| `run-drills.sh` | server | run the WHOLE drill suite in PARALLEL — inotify preflight + concurrency cap (`-j`) + infra-flake re-run; the preferred way to run all drills (see Drills) |

**Edit-loop:** the control scripts (`simcluster`, `lib/*`, `drills/*`) take effect via rsync alone — just
re-run `remote.sh`. The baked files (`Dockerfile`, `image/*`, the vendored binaries) need `remote.sh build`.

## Server

Runs on `weilandserver`, the main development box (see `docs/devices-ops.local.md §6`). Requirements:
Linux with cgroup v2 + Docker (systemd-in-docker needs `--privileged --cgroupns=host`), plus a Go
toolchain to build `tether` in place. The `nats-server` (pinned to the `install.sh` version), `nats`
and `nk` binaries live in `vendor/` and are staged by `./simcluster build`. When driving from an
external box instead, `remote.sh` builds and rsyncs them over.

Caddy provisioning requires Python 3.9+ and curl on the staging driver and Python 3.9+
on the image-build host. The entire image profile remains **Linux amd64**; Caddy
staging refuses another `SIM_TARGET_OS` / `SIM_TARGET_ARCH`. `lib/stage-caddy.sh`
reads the exact `CADDY_VERSION` default from `scripts/install.sh` (an explicit numeric
`TETHER_CADDY_VERSION` override is forwarded by `remote.sh`). It downloads the official
release archive and its checksum list on every stage; **there is no Caddy download cache**.
The archive name must have exactly one checksum entry (SHA256/SHA512, matching the
installer's policy). The receipt records both that official checksum and the archive /
binary / installer SHA256, release, source URL and target platform.

Serialize vendor staging, rsync and image build. Files are prepared in the same vendor
directory, fsynced and atomically renamed; `caddy-source.json` is published last. This
is not an atomic two-file transaction: interruption between renames leaves a pair that
`simcluster build` rejects before Docker is invoked. The Dockerfile repeats verification
on its copied files, and build checks the runnable Caddy version inside the target image.
A failed or interrupted stage requires re-staging before build; do not hand-edit the receipt.

`drills/lib/caddy.sh` configures the baked real Caddy through the production installer's
`caddy.service` during initial drill setup. The instance CA is reused; inject trust into
ctl/agents before those processes start. `/sub/*` routes to 8090; the remaining HTTPS/WSS
traffic routes to NATS WebSocket 8222. `caddy_probe_wss` requires an authenticated request
through `wss://<broker>:443/nats`; merely active systemd state is insufficient. Control WSS
and tunnel TLS (`<broker>:7000`) are independent addresses: pass the latter as argument six
to `agent_provision_yaml`. The helper refuses to replace an existing Python ingress sidecar.
Fresh nodes use the installer's public `--no-enable`: the broker start command enables
NATS/broker, while `caddy_start` enables Caddy for the explicit profile. This keeps Caddy
disabled across reboot in plain/Python-ingress profiles. Existing provisioned nodes are
not retroactively changed; use fresh isolated containers for these profile checks.
These helpers do not by themselves constitute an H0 continuity drill: it must keep one
original external SSH/TCP stream and audit all discovered endpoints before declaring READY.

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
node · `remote.sh logs brk1 tether-broker` for the journal **and** the daemon's own slog (see below) ·
`remote.sh status` for the node table + leader
view · `remote.sh nats-conf brk1` to inspect nats.conf (the #20 probe) · `remote.sh exec brk1 -- <cmd>`
for any command · `remote.sh doctor` for PID1 + ownership drift checks (the #22 tripwire).

**Teardown:** `remote.sh down` (stop, keep data) · `remote.sh down -v` (remove volumes) · `remote.sh nuke`
(delete everything — containers + volumes + secrets — for a clean slate).

**Isolation:** `--instance <name>` (or env `INSTANCE=`) namespaces containers/volumes/network, so
destructive `drill`s run on throwaway `drill-*` instances that never touch your persistent cluster (each
drill nukes its own instance on exit).

## Where the logs are (four streams, and why they must stay apart)

tether's daemons do NOT put their application log in journald. Each writes its own size-capped file,
and journald is left carrying panics and pre-logger boot output:

| Stream | Lives in |
|---|---|
| broker slog | `/var/log/tether/broker.log` (named by broker.yaml's `log_file:`; `broker.err` on pre-h1 hosts) |
| broker panic / stacktrace | journald — `journalctl -u tether-broker` |
| agent slog | `$HOME/.tether/agent/<sid>/agent.log` (the agent binary's default sink) |
| agent panic / stacktrace | `$HOME/.tether/agent/<sid>/agent.boot.err` (fd 2 is dup2'd there) |

**Drills must read these through `drills/lib/logs.sh` — never inline the paths.** When h1 moved two of
the four streams, roughly a dozen drills each held their own copy of the mapping, and the move turned
working oracles into ASSERTION FAILURES against a product that was behaving correctly. A harness whose
job is to expose real defects faithfully had started manufacturing them. `sim_broker_slog`,
`sim_broker_panic_journal`, `sim_agent_slog_grep`, `sim_agent_panic_sink` and friends keep the streams
distinct on purpose: merged into one "grep the logs" helper, a panic could satisfy a slog assertion.
`test/architecture/simcluster_log_oracle_test.go` holds the line in `make gates`.

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
| `logs <node> [unit]` | journalctl inside a node, **plus** that unit's process-owned slog (see “Where the logs are”) |
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

### Units, arms, lanes and the contention registry (simcluster-speed, 2026-09)

The runner's unit of work is a **unit**: a whole drill, or one **arm** of an arm-split drill. A drill
declares arms in a comment-block manifest at its top (parser: `lib/manifest.sh`, ONE parser shared by
`simcluster drill --arm`, the runner and `tests/arm-manifest-lint.sh`):

```sh
# arms: A D F
# fixture: A=N3-live D=N3-live F=N3-live      # closed vocabulary: N1|install|N2-live|N3-live|FS-N2|N2-cap3g|grow-claim
# grows: A=2 D=2 F=2                          # `tether cluster add` grows the arm performs (lane release count)
# worst: A=1350 D=1500 F=800                  # declared worst case, seconds; the unit is bounded at 2×worst
# forgoes: A=- D="71-minority-commit: post-A context no longer exists" F=-
case "${ARM:?}" in A) … ;; D) … ;; F) … ;; *) setup_fail "unknown arm" ;; esac
```

`simcluster drill <name> --arm <A>` runs exactly one arm on instance `drill-<name>-<A>`; `drill_end`
emits `DRILL-ARM arm=<A> of=<name>` beside the verdict line and the runner cross-checks it — the line is
printed from the exported `ARM`, so it proves the arm the runner asked for reached `drill_end` (a swapped or
missing line is CONTRACT-ERROR, never a verdict of record); that the arm's CODE is the branch it names is
`arm-manifest-lint`'s R4 (case labels == manifest arms), not a runtime check. Rollup rows are keyed
`<drill>.<arm>`; an arm-split drill also gets one `ARMS` row — the precedence join of its arms
(ASSERT-FAIL > SETUP-RED > PRODUCT-RED > INCOMPLETE > GREEN), the summed counters, and `arms_missing`
(arms with no verdict, reported beside the join, never folded into it: an early INFRA-ABORT can no
longer hide a later real red). The exit code counts **drills** with a blocker, so splitting a drill never
changes it. `expected-verdicts.tsv` carries one child row per arm (`<drill>.<arm>`, written from the
parent's claim ownership BEFORE the first split sweep — `tests/validate-verdicts.sh` rules ①–⑧ keep
the parent row derived: bands `-`, expected = join, nc_gap = Σ). Two levers the sweep runs under:

- **per-unit ceiling** — `2 × worst`, capped by `--drill-timeout`; a unit with no `# worst:` keeps the
  global ceiling (fail-open only in the LONGER direction). A ceiling kill is INFRA-ABORT, named by its
  basis in the log, never retried.
- **grow lane** — units whose CODE contains a grow token (`grow_to_3|grow_to_2|setup_forcesingle_n2|
  "$SIM" grow`) are grow-lane units; at most `--grow-cap` (default 5) are inside their grow phase at once
  (a unit leaves the lane when its declared `# grows:` markers are written, or when it exits; a fixture
  with an in-unit nuke+retry — `grow_to_3` / `grow_to_2` — writes its markers itself, once, after its
  post-check, so a retry never releases the slot early). The lane never holds the head of the queue: a
  free job slot takes the first eligible unit in cost order, so N1 units keep flowing while the lane is
  full. `--live-grow` (cap 0) is the CONTENTION regime; the run's regime is recorded once in
  `regime.tsv` and copied into the rollup's `REGIME` row — `--replay` reads it back and never rewrites
  it (a missing or invalid `regime.tsv` reads `unknown`), and refuses the regime flags outright.
  Metadata must contain exactly one four-column row with a known mode and non-negative integer
  cap/stagger values; truncated, conflicting or malformed records do not establish the original regime.

**C1 — the contention registry (`contention-sensors.tsv`).** Every load-discovered product defect this
suite has produced came from UNENGINEERED contention (#67 under saturation, #66 at `-j3`, 52's rc=77 at
`-j6`, #70 whenever five grows start together). Each speed lever reduces contention, so each of those
sensors can go dark without any drill turning red. The registry names every such sensor, the regime it
fires under (`default` / `live-grow` / `none-after-split` / `H-dependent`) and the units that still
sample it; an arm that stops sampling one says so in `# forgoes:`, and `tests/contention-registry-check.sh`
reconciles both ways (ids must exist, units must resolve, a live regime must name a unit). Obligation:
**`./run-drills.sh --live-grow` is run before a release and whenever the grow path changes** — by hand,
like every drill here (CLAUDE.md §5: on demand, never in `go test` / CI; nothing in the repo schedules it,
and an earlier revision of this line that said "weekly" described a mechanism that does not exist —
round-2 review R6-2). It may mint a gotcha; it may not change an expectation — a red under `--live-grow`
is a finding to attribute, not a band to add.

**CAVEAT (grow-concurrency, INDEPENDENT of inotify).** The heaviest clustered-JetStream grow
(`10-grow-to-3`: two grows + follower-kill) can still time out its VOTER promotion (150s) when ALL
drills grow at the same instant — raft/JS-meta formation is timing-sensitive at peak concurrency. The
grow lane above bounds that by default; under `--live-grow` (or a raised `--grow-cap`) the window is
back, deliberately — that is the #70 sensor. If it goes RED alone in an otherwise-green run, re-run it
singly.

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
| `78-proxy-dial-backoff` | **INCOMPLETE** (g75-g78, N=1 broker + 1 agent + ctl): the **gotcha #78** deploy-tier regression — proxy first-dial exponential backoff + broker read-REGISTER WARN damping + `proxy.participate` opt-out. **Arm A** (GREEN): under a REJECTing tunnel port (`fault_reject_on`), total dial attempts (via the **netfilter packet counter**, not product logs) must fall well below the no-backoff ~39 baseline (≤20) AND show a decreasing per-bucket trend — fault-speed-decoupled (an absolute count would couple to REJECT's fast-fail vs a slow half-open timeout). **Arm B** (GREEN): an operator `proxy off`/`on` mints a new epoch and establishes within 60s — no backoff hangover. **Arm C** (`not_covered` gap): the broker WARN-damping face is NOT deploy-tier-constructable — the `read REGISTER` WARN site is post-TLS (`tls.NewListener`), so a raw junk-TCP storm fails at the handshake and hits the `accept` site, never reaching it (measured WARN delta 0); the half-open TLS-completed-then-EOF shape needs a real TLS middlebox this sim doesn't front. Hermetically covered by `internal/tunnel/register_log_damping_test.go` (per-class damping under interleave, no-recovery-on-unauthorized-line, per-Cap reaffirmation). **Arm D** (GREEN, strongest #78 evidence): `proxy.participate: false` → zero dials across a live repair loop + the __proxy__ allocation freed + `proxy status` shows `opted-out`, then flip back re-enters the pool and serves. Mandate-honest: arm C exposes the constructability limit rather than faking it with a raw-TCP storm that would false-GREEN or false-RED. |
| `31-node-upgrade-fleet` | **INCOMPLETE** (G-A/S5, N=1 cluster + 2 agents): **#28-FIXED** — configured agent `upgrade.url_allow` is proven to admit the self-hosted artifact, while the negative local-allowlist discriminator remains non-vacuous. The drill covers broker negatives plus the fleet CONTROL surface: ONLINE enumeration, OFFLINE exclusion, dispatch/config-abort, and `--timeout 0` under the upgrade-safety CANARY contract (a canary transient failure aborts with the rest untouched). Single-node success/rollback/`--wait` is owned by drill 33; the remaining nc_gap is only the fleet-wide `--all` fan-out after a committed canary. |
| `33-node-upgrade-success` | **INCOMPLETE** (S5 follow-up, N=1 cluster + 1 agent container with TWO agent units + ctl, ≈9 min incl. one rigid 120s watchdog window): the REAL `node upgrade` success/rollback/upgrade-domain paths — B: a healthy tether-next artifact under a SYN-only egress block (`fault_synblock_on`, the weak-NAT shape of broker-ops §8.7: established registration lives, the re-exec'd process can never register) → staged line → pending whole-conjunction (marker `boot_count>=1` ∧ dst flip ∧ same MainPID ∧ `/proc/exe`==next) → 120s watchdog rollback whole-conjunction (Detail phrase-pinned to the watchdog leg, `.prev` consumed, running image back to OLD) → heal: product-only re-register at OLD + broker `agent-reported upgrade outcome rolled_back` + delivered marker removed; C (nested in B's window): shared-binary sibling `agt1b` refused `upgrade_in_progress` at the MARKER-gate phrase; A: real success — same PID through THREE execs (`ExecMainStartTimestamp` unchanged) + running-image sha==next + `node ls` RELEASE flip + ctl COMMITTED + marker committed interlock; C2: domain release — the sibling's own upgrade commits (⚠ wholesale-replaces the shared marker; nothing reads agt1's marker after it). nc_gap: gotcha **#73** (a non-tether artifact past the smoke gate has no boot shim — deliberately not staged; it would wedge the node). Plan: `docs/reviews/upgrade-success-drill-plan.md`. |
| `34-hot-update-process` | **S3 acceptance: GREEN ×7 arms** (observed 2026-10-02, image `4fe3646b…`, `run-drills.sh` -j4: A 63 / G 77 / U 68 / N 72 / P 102 / W 217 / F 81 s, 220 s wall, NO DEVIATIONS; receipts in `docs/reviews/hot-update-evidence.md §14`). N1 per arm: A one same-PID image switch and original ctl rc23; G one producer across tether → tether-next → tether; U product `agentUserUnitBody` rendered with `User=sim` as a SYSTEM unit (does **not** prove `systemctl --user`; 82 U3 owns that gap); N setsid/nohup, nonce pidfile and stable fd2 inode; P calibrated mixed traffic at 10 requests/s, a start handshake and observed cut/activation marks, continuing through activation plus 10 seconds (180-second maximum); successful read/write/short work, ≥9 requests/s and a successful write overlapping the switch are required; refusals outside the cut, refusal side effects and unrequested ledger tokens fail; reliable/legacy exec counts are recorded; W SYN blackhole and watchdog redispatch to the previous image, LOST→RUNNING and EXITED(23); F healthy hangfs mounted before the final setup restart and before business work, then a wedged probe proven by refusal, retained census and agent slog; disk untouched, then healed retry. Child identity, exact output, final process state, image generations, audit counts and FD ownership are independent oracles. Freeze and request latency distributions are recorded calibration. The historical S1 PRODUCT-RED receipt is preserved in expected-verdicts-log §34. No PTY/SSH/transfer/full-stack continuity claim. |
| `35-hot-update-exec-carrier` | **GREEN ×3 arms** (observed 2026-09-21, first real run, image `ea43e478…`, `run-drills.sh` -j3: A 90 s / B 51 s / C 84 s, pass 29/21/40, NO DEVIATIONS; receipts in `docs/reviews/hot-update-evidence.md §12.2`) (hot-update S1, `docs/reviews/hot-update-reliable-exec-plan.md` §5.2; N=1 + agent + ctl per arm, no upgrade, no artifact server): a reliable `tether exec` survives its CARRIER being replaced under it — **A** nats-server restart then a 25 s nats-server outage (> `redialAfter` 20 s: the agent rebuilds its whole NATS session, asserted from its slog); **B** tether-broker restart (auth_callout black window included; `B-new` records a new exec's rc/path/wall, never judged); **C** the N-1 window against the REAL previous release `vendor/tether-prev` behind a die-gate — C1 the old ctl vs today's agent under the A2 fault (claim: legacy path taken, executed once, discontinuity recorded honestly in `legacy-outcome.json`; a fully continuous old ctl REFUSES the judge; C1 is also the old-binary NEGATIVE control of the A/B oracle), C2 today's ctl vs the old agent (probe reaches it — `unknown forwarded verb` — ctl falls back to legacy after the 2 s tax, run complete). GREEN conjunction (probe.py `carrier`): original child alive under the same PID triple/parent, agent MainPID + ExecMainStartTimestamp unchanged, both transcripts == source, no pipe_error, ctl rc 23 on its own, exactly one `agent: exec … reliable=true` for the nonce, exactly one `ps -a` row EXITED(23), ≥ 20 source records inside every fault window (`carrier.json`). Non-vacuity: `_stream_advancing` gates every injection; the shell gates are proved two-sided in `tests/r9d-nonvacuity.sh`, the judges in the probe selftest. Every log read via `drills/lib/logs.sh`. |
| `36-hot-update-pty` | **S4：NOT_RUN**，验收目标 36.A/W/C/L 及父 36 全 GREEN（非已跑结果）。每臂 fresh N1、原 ctl 真实 controlling tty + 独立远端 PTY；A 不同 SHA 升级、原长前台双工/resize、strict mixed 10/s（≥9/s）、新 run≤5s、业务停顿≤5s；W 新 KA 收据后 SYN 黑洞、真实 120s watchdog 回 prev，原 handle 与当前 A/Q/output 继续，KA 绝对期限不重授；C server restart+25s outage/session rebuild，并覆盖 leader 退出后 >200ms 尾流和 FIN/canonical/raw；L 真实旧 ctl 的 legacy 会话使升级 legacy_session_busy、盘面不变，新 ctl 对旧端仅 create 前 fallback。逐字 bytes、原 master/fg/child identity、同 agent PID 与 running SHA、marker generation、唯一 spawn/courier exit、原 ctl waitpid rc23、最终 FD/zombie 均独立断言。四日志流经 logs.sh 新鲜完整 cursor；500ms 全冻结窗只校准、保留最大值/超标。Σ declared worst=1380s、串行 ceiling=2760s 是预算算术，actual 成本 NOT_RUN；34 七臂、33 A/C/C2、35 全回归另存同 SHA 收据。 |
| `39-hot-update-legacy-n1` | **S3 N-1 boundary: X GREEN / Y PRODUCT-RED (owner S8), parent PRODUCT-RED** (observed 2026-10-02, image `4fe3646b…`, -j2: X 64 s / Y 117 s, NO DEVIATIONS; the predicted `idle_timeout_rc70` signature matched verbatim, no re-pin). X GREEN: live reliable task refuses the real previous-release target with `image_abi_unsupported` (64), disk untouched and original ctl rc23. Y PRODUCT-RED: running `vendor/tether-prev` exports via bare exec; dev ctl falls back to legacy, both original pipes EPIPE and ps EXITED(-1). `idle_timeout_rc70` was predicted from the ctl code before the first real run and measured verbatim on it. Parent is the PRODUCT-RED join. Real previous tag/source/SHA/absent ABI die-gate; no fake executable. |
| `39.P`（`39-hot-update-legacy-n1` 的 S4 新臂） | **S4：NOT_RUN**，目标原生 PRODUCT-RED，不能写 GREEN。真正 `vendor/tether-prev` old exporter + old ctl，有 source/version/SHA die-gate，与 36.A 相同 HUP 敏感原 PTY fixture、升级故障及 continuity judge；故障后 child 可已死，不能用存活 setup 门掩盖缺陷。先证有效供给/故障/可读日志，再以真实产品单一精确签名重钉，不猜 rc70。三层分存：continuity oracle rc=1；native DRILL-VERDICT PRODUCT-RED rc=3、assert_fail=setup_red=not_covered=nc_gap=0；sweep 无 waiver 阻塞，--allow-product-red 只放行 sweep、绝不改 native。39 父按 X/Y/P join 仍 PRODUCT-RED；保留 H0 owner 子串并追加 H1 legacy PTY exporter / S8 legacy-to-capable first migration。三向 selftest 必须证正输入过、旧输入失败、删检查后旧输入错误通过；坏 setup/空窗口/缺 waitpid 不算 PRODUCT-RED。estimate 150s / declared worst 300s，actual NOT_RUN；证据见 hot-update-evidence §15。 |
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
| `98-stuck-redial-recovery` | **INCOMPLETE** (gotcha #72 fix regression, N=3 + 1 agent + ctl, ≈11 min): the POST-FIX bounded-teardown recovery contract over nats:// — silent-DROP the agent's egress to the broker `/connz` says actually holds its connection (self-proved black-hole + a survivor reachability probe; the DROP stays armed) → the connection must be OBSERVED to leave that broker, then the heartbeat must advance past a POST-IMPACT watermark on another voter, all inside ONE shared budget (330s: up to ~4min nats.go ping detection under a silent DROP — tether sets no PingInterval — plus redialAfter 20s + closeBudget 10s + poisonGrace 10s + reconnect; the product's ≤60s bound covers only the part AFTER nats.go declares the disconnect) → recovery path classified by MainPID (same-PID in-process rebuild / same-PID escalate self-exec / supervisor restart — recorded, never banded silently) → heal + no-flap steady state. nc_gap: **[GAP #72]** — the live incident's actual wss:// half-dead-handshake shape is NOT constructable (simcluster fronts no wss listener); that arm is the ledger's flip condition. Plan: `docs/reviews/gotcha72-teardown-plan.md`. |
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

Single host (cross-process, not cross-host WAN raft); no public ACME/real domains. Existing drills use
plain `nats://` on the trusted bridge (route mTLS is real); the explicit Caddy profile uses
real Caddy TLS/WSS with the instance test CA. v2/`main` only; not a load/perf/fuzz tool; Linux/amd64 only; not a
CI gate and not a substitute for the real multi-host staging drill.

## S5 flow/SS validation contract (historical contract; final receipts linked below)

The [binding S5 plan](../../docs/reviews/hot-update-flow-carrier-plan.md) §9 defines the following
validation contract; [raw receipt index](../../docs/reviews/hot-update-evidence.md#16-s5-flow-carrier-ss-receipt-index)
also links the [S5 final receipts](../../docs/reviews/hot-update-evidence.md#17-s5-flow-carrier-ss-最终收尾收据2026-10-11). The following records the original contract, not a new receipt. P24/P25/P27 own probes, scripts,
supply and verdict/cost registries. Keep this contract distinct from historical drill rows above.

`75-hot-update-flows.sh` has exactly five manifest headers and one top-level ARM dispatch:

```sh
# arms: A C R B W L
# fixture: A=N1 C=N3-live R=N1 B=N1 W=N1 L=N1
# grows: A=0 C=2 R=0 B=0 W=0 L=0
# worst: A=420 C=600 R=420 B=540 W=660 L=420
# forgoes: A=- C=- R=- B=- W=- L=-
```

| Unit | Required native verdict | Nonempty evidence |
|---|---|---|
|75.A|GREEN/rc0, nc_gap0|Same original SSH, four duplex TCP, actual EINPROGRESS OPEN, half-close and bob/alice SS cohort through upgrade, in-window alice revoke and forced carrier rebuild;≥20 samples each stage plus10/s new flows.|
|75.C|GREEN/rc0, nc_gap0|N3-live,32active and declared slow flow; healthy home/epoch/port unchanged, reaper running and SQL/event evidence.|
|75.R|GREEN/rc0, nc_gap0|PREPARE/final cut/RESUME revoke windows; silent alice terminated by product with originals/dup/public FD/replay retirement, bob survives, fresh AppliedRevision grant.|
|75.B|GREEN/rc0, nc_gap0|Two real communicable artifacts A→B→A, both commits; B advances offsets/nonces/retirement, two current cuts.|
|75.W|GREEN/rc0, nc_gap0|First real commit marker filesystem write fails after ready; restore filesystem access, keep NATS and default reaper alive, product120s watchdog takes second current cut.|
|75.L|GREEN/rc0, nc_gap0|True v0.6.0 broker runs before first business, old /proc exe SHA and empty ALPN/raw first byte; new agent raw interoperability, safe legacy_session_busy75 preserves original business; no continuity claim.|
|75 parent|GREEN/rc0, nc_gap0|All six children joined natively; never aggregate cohorts from separate phases into75.A.|
|early-close / wrong-offset|ASSERT-FAIL/rc1 each|Same independent continuity judge; actual early socket close and product A+1 or removed dedupe mutant; A−1 correct overlap is a positive control.|
|39.F|PRODUCT-RED/rc3|Real v0.6.0 exporter/old business before bare-exec fault, valid_fault1, shared continuity judge rc1, earlyEOF/RST signature, then assert_bug native3; dispatch before common exec-only setup. Safe busy is an ability gap, not this receipt.|

75.W fault setup holds first-register response until candidate boot_count advances, chmods the non-root
agent binary directory0555 only for the first CreateTemp/rename attempt, observes real commit failure with
pending/ready/watchdog armed, then restores mode. An ineffective root permission fault or second full-register
commit is SETUP-RED and must rebuild the fixture. Do not disconnect NATS for120s, pause reaper, or let the
fixture call resume/rollback on the product's behalf. B and W are separate receipts.

Use independent directional seq/hash ledgers, original kernel/application identities and predeclared final
sequence/deadline. Judge trailing silence, sticky EOF/RST, original half-close reverse tail and genuine
short Write0<n<attemptLen; no application reconnect. Normal max stall/new request≤5s is hard, p95 alone
is insufficient; full required-domain freeze500ms is calibration with all samples/max/excesses retained.
Source/artifact/running SHA must prove actual exec. Every negative product mutant is restaged with source
and running SHA; restore and revalidate the final candidate. Legacy fixed v0.6.0/a3431a1 supply and dynamic
adjacent-release N-1 are separate: record peeled tag commit, flags, checksum and actual broker ExecStart/SHA.
An old ctl against a new broker cannot sign the old-broker quadrant; TLS/pins remain enforced.

`expected-verdicts.tsv` is exactly six columns: drill, expected, expected_nc_gap, bands, owner, note-ref.
75/its six children use GREEN,0,-,- and full unit note-ref.39.F uses PRODUCT-RED,0,- and owner
`hot-update-plan H2 legacy flow exporter; S8 legacy-to-capable first migration`, note-ref
`39-hot-update-legacy-n1.F`; append that full owner to39's existing parent owner preserving H0/S4 H1.
Parent verdict=child join, gap=sum, child owner is parent substring. Priority ASSERT-FAIL>SETUP-RED>
PRODUCT-RED>INCOMPLETE>GREEN; the inherited harness uses native rc1/2/3/4/0. S5 plan §9.4's
INCOMPLETE75 differs from `lib/assert.sh` and the validator/runner, which plan L15 preserves.
S6 lead L4 confirms native INCOMPLETE4; safe-busy command75 remains a separate business result.
Waivers never rewrite native results;
save subshell rc before pipes/cleanup. Native drill INCOMPLETE4 and a business command's safe-busy75
are separate fields. Headers/worst/expected are declarations, not actual wall receipts.

On the same final candidate preserve36.A/W/C/L GREEN and39.P PRODUCT-RED,34 seven arms,35 three arms,
33 A/C/C2 plus its INCOMPLETE boundary;70/72 full verdicts.70 verifies rm immediate reuse separately
from Stop≤15s refused/restart.72 verifies wrong PSK zero SINKBYTES on allow-private sink, growing and
silent alice revoke/bob continuity, off OS-listener+allocation retirement and same-port reuse.
A fixture closing the victim cannot prove product cleanup. Four log streams still use only logs.sh.

Estimates (not measured):75 A/C/R/B/W/L=180/240/180/240/300/150s, total1290s; declared worst3060s.
Per-arm2×worst ceilings840/1200/840/1080/1320/840s within runner global ceiling.
39.F estimate180–300s, worst420s; mutants5–10min, S4/exec regression25–40min,70/72≈6min;
reserve60–90min for serial deploy, failure reconstruction extra. S5 rows in `drill-costs.tsv`
are explicit estimate / declared / NOT_RUN placeholders for launch ordering; the lead replaces each
with COMPLETE actual wall/source/date/concurrency/regime; avoid counting parent+arm wall twice. Each raw receipt stores
source_head/dirty_patch_sha(actual content, including untracked inputs)/build_flags/artifact_sha/running_sha/
role/command/native_rc/verdict/wall_s/oracle_inputs_sha/evidence_path/status and raw byte/crypto/SQL/FD/budget logs.
P28 does not edit mechanical verdict/cost ledgers or claim any of these drills ran.

Current deploy prerequisite: `drills/lib/flow.sh` loads the live `flow_fixture` and independent judge.
The fixture owns original SSH/TCP/SS workloads, sealed client custody, actual barrier observations
and raw collection. Product Seal attempts/short writes, private flow bindings, domain reservations
and legacy ALPN/raw-prefix observations still need product owner interfaces. Missing priors return
native SETUP-RED2; hermetic passes cannot certify deployment continuity.
The former `tests/flow-fixture-report.md` process report is absent from this tree (NOT_RECORDED);
use the reviewed [S5 evidence](../../docs/reviews/hot-update-evidence.md#17-s5-flow-carrier-ss-最终收尾收据2026-10-11)
for scoped final receipts rather than a dead process-file link.
39.F also requires `FLOW_LEGACY_EXPECTED_LOSS=early_eof` or `reset`, selected only after the
lead reviews its actual raw historical loss. An unpinned run retains the oracle observation and
returns SETUP-RED; it cannot manufacture a PRODUCT-RED receipt from an EOF/RST alternation.
The former `DRILL-PREP-REPORT.md` process report is absent (NOT_RECORDED); current executable
fixture/driver declarations and their independent NOT_RUN status are documented below.

## S6 broker-image deploy contract (NOT_RUN pending P29/P35)

Use the [frozen plan](../../docs/reviews/hot-update-broker-image-plan.md) §10 and
[unchecked claim index](../../docs/reviews/hot-update-tasklist.md#s6-broker-image-receipt-boundary).
The mechanical `hot-update-broker-ledger.tsv` currently records NOT_RUN for every S6 claim; P33 owns its
updates. Existing fixture scripts and manifest declarations do not prove a product run. P29 owns real live
fixture/collector and its selftests; do not invent a fixture command until its entry is landed and verified.
P30 owns deploy arm wiring/native verdict controls. P35 archives final native runs, legacy pin and measured costs.

| Unit | Required native verdict | Independent evidence |
|---|---|---|
|37.A|GREEN0|Default single, same broker PID/new running SHA, original SSH/TCP/SS and reliable child; current SQLite ACK prefix/flock/listener continuity.|
|37.A1|GREEN0|Real sole-voter configuration and production timers; first new write ≤5s, no CodeIsLeader.|
|37.B|GREEN0|N3 fresh leadership transfer before each cut; explicit broker-only scope leaves real co-located agent PID/SHA unchanged; home/epoch/port/pins and dwell stable.|
|37.T|GREEN0|S7 capable live transfers migrate in single and N3 lanes, both legs before default reaper; nonempty original ID/version/PID/OFD, postcut progress and exact history. Independent legacy/unknown responsibility still safely defers.|
|37.R|GREEN0|Valid candidate reaches partial Take with complete salvage, then separate ACTIVE new ACK/offset state takes generation+1 current cut to previous reader.|
|37 parent|GREEN0|All five native child receipts on one candidate/run; no translated expected-red success.|
|39.BR|PRODUCT-RED3|Genuine previous-release N3 broker Run-unwind; peeled tag/source/binary/running SHA, capability and valid original cohort; independent first-run loss signature signed by lead.|

Originals must survive without application reconnect, target Accept=1 and exact directional bytes/EOF;
include real pending OPEN, slow/half-close tail and independent SS nonce oracle. Every10/s scheduled request
slot and attempt, CONNECT versus request failure, true PID start/effects and trailing silence is retained.
Measure actual auth-callout blackout from usable-to-usable, not Flush/log timestamps. Retain unchanged external
JS object/chunk/config/consumer/audit witnesses. Do not pause reaper, pre-authenticate away the window,
reset JS or start a second NATS server. A capable T preflight refusal leaves S7 coverage incomplete. R refusal at preflight does not sign its downstream branches.

Normal flow/control/write max≤5s is hard; full required-domain freeze500ms is calibration with all
samples/max/excesses. INCOMPLETE4, ASSERT-FAIL1, SETUP-RED2, PRODUCT-RED3 and GREEN0 remain native;
retryable business refusal75 is separate. Preserve39 X/Y/P/F owners and verdicts when appending BR.
Declared worst/estimate values are not measured wall; S6 package/lead gate history is in evidence §18, while S6 deploy drills remain NOT_RUN. S7/S8/S9/S10
final evidence and ENDPOINT-RESUME carry reconciliation remain unsigned until reviewed. Historical red
receipts are preserved; administrative stage closure is not a deployment pass.

## S7 transfer-continuation driver (NOT_RUN)

The frozen [S7 plan](../../docs/reviews/hot-update-transfer-continuation-plan.md)
§10 owns the acceptance profile. `63-hot-update-transfer` has exactly eleven
positive arms: A A1 B C1 C2 C3 R1 R2 X W1 W2. R1 is pristine and R2 is ACTIVE
current-state rollback. `64-hot-update-transfer-controls` has23 independent arms:
M01 M02 M03 M04 M05 M06 M07 M08A M08B M08C M09 M10A M10B M11A M11B M11C
M12A M12B M12C M13A M13B M13C M14. Each arm is a fresh launch unit. Existing
`run-drills.sh` arm splitting and missing-child checks own the parents; the driver
never serializes all arms into one2700s launch. The positive parent requires every
child GREEN0; the control parent preserves the children's native ASSERT-FAIL1.

| Unit | Frozen coverage | Estimate / declared worst / runner ceiling |
|---|---|---|
|63.A / A1 / B|Four independent sessions/buckets/installations; both legs, agent A→B, broker A→B, agent and broker current-state B→A; N3 has three individually witnessed hops.|600 /650 /750s estimates;1200s worst each;2400s ceiling|
|63.C1 / C2 / C3|Distinct carrier replacement / transfer data connection rebuild / same-binary same-store JS restart; all four cohorts both legs have pending Publish/Get.|570s each estimate;1000s worst;2000s ceiling|
|63.R1 / R2|Tier A/B × push/pull × agent/broker NewStore/broker NewControl import failures; ACTIVE requires new progress before current cut.|650s each estimate;1200s worst;2400s ceiling|
|63.X|Independent Ctrl-C/session-rm victims before reserve; four successful survivors have separate sessions/buckets. No-force-existing refusal is independent.|580s estimate;1100s worst;2200s ceiling|
|63.W1 / W2|One real120s outage per original call: W1 leg1 pull sender, W2 leg2 push receiver; actual byte attachment stalled, original budget and automatic current cut.|450s each estimate;1100s worst;2200s ceiling|
|64,23 children|Exact SHA-bound production predicate patches; M08=3, M10=2, M11/M12/M13=3 each; original native judge kills each independently.|About5000s total estimate;900s worst per child;1800s ceiling|
|61 /37.T /39.BR|All41 transfer edge assertions, unlimited baseline and TTL List; T single/N3 capable live cut with legacy/unknown safe defer; independent reviewed old N3 native PRODUCT-RED3.|Original budgets;37.T now1200s worst;2400s ceiling|

All durations above are ESTIMATE / NOT_MEASURED. About13170s serial marginal
cost, two lanes and grow concurrency1, with shared builds/install serialized;
allow2–3h plus original regressions/gates. P37 records actual per-launch wall,
direct poll wait, baseline versus marginal, peak FD/RSS/JSbytes. A runner timeout
without drill_end is INFRA-ABORT. It cannot be renamed INCOMPLETE by cleanup.

The four labels are `small-push`, `small-pull`, `large-push`, `large-pull`.
Small is66,549,440B at256KiB/s; large is1GiB at4MiB/s. W predeclares1MiB/s and
8MiB/s respectively, with only one120s outage per invocation. Production budgets
remain568s small /1084s large in the baseline and300s small /1084s large for W.
The large4MiB/s profile does not change the product budget's2MiB/s effective-rate
cap. Each leg cuts agent by30s, broker by110s, rollback by190s relative to its first
durable receipt. Sender needs0<acked bytes<size; receiver needs0<Sync bytes<size.
Prepared/courier responsibility is counted separately. Healthy progress is10s,
control slots10/s with5s logical budget, maintenance8s, watchdog120s, default
reaper. Rate/IDs/deadlines never change after the manifest is sealed.

Driver prerequisites and handoff to P29/P32 (currently NOT_RUN):

- `TRANSFER_DEPLOYMENT_MANIFEST` is a read-only JSON deployment description.
  `cohorts` are ordered by the four labels, with `sid,bucket,nid,agent_node`,
  isolated absolute `binary_path,home,source,destination`, `verb,size,sha256`,
  `rate_limit_bps,force`. SID/bucket/NID and agent installation pairs are unique.
  Force/no-force both occur in each direction. `transport` is `caddy-wss`.
  `artifacts.A/B` carry absolute prebuilt `binary`, HTTPS `url`, `sha256`, `version`;
  both are real capable images with distinct SHA and `transfer_version=transfer-v1`. `brokers` carry `node,socket`.
  R artifacts are additionally keyed by the exact schedule cell ID; P32 provides
  actual import-barrier builds, including separate NewStore and NewControl cells.
  X has separate `victims` with `kind,sid,bucket`. T also requires separate
  `refusal_lanes` ordered legacy/unknown, each with `kind,node,socket,artifact_key,reason`;
  P29/P32 supplies real retained legacy and unknown-census windows. The driver
  issues reload itself, requires native75, and the fixture checks unchanged
  running identity and zero cut effects before those original responsibilities retire. P29 must check live isolation,
  source hashes, original images and zero business side effects before start.
  `fixture` contains the observation owner's schema=1 live manifest: `processes`,
  `transfers`, `cuts`, `nats`, and `control.logical_ids`. Each transfer supplies
  its original transfer ID/nonce/object version, canonical start/terminal bytes,
  streams/subjects, custody producer/receipt and required OFD schedule. Its
  session, bucket, target, size, hash and rate must match its driver cohort.
  Agent home/binary pairs must match the four installations. These inputs are
  required; the driver cannot infer canonical product identities from node IDs.
  Existing flat deployment manifests may supply these fixture fields at the
  top level with `mode=live`; the projection removes driver-only fields and
  restores schema=1 after the planning manifest replaces its schema.
- `TRANSFER_PROCESS_SUPERVISOR` is FIX-ORPHAN's absolute shared executable,
  called as `--log PATH --receipt PATH -- ARGV...`. It preserves native status
  and owns descendants through timeout/panic/SIGKILL. Missing helper is SETUP2;
  this driver has no private Pdeathsig implementation.
  The in-repo Go launcher now implements these flags: it tees inherited stdout
  and stderr to the log and writes the final decimal status after descendant
  cleanup. `SIM_UPGRADE_SUPERVISOR` is also accepted as the shared launcher.
- The fixture's frozen CLI remains `transfer_fixture.py
  start|sample|drain|judge|stop --manifest PATH --evidence DIR`. It launches the
  keeper in the foreground and returns a flushed startup custody receipt while
  serving its control socket. The driver retains its supervisor until stop and
  joins it on failure too. Product custody/launch integration supplies original
  CLI push/pull using existing positional arguments plus the predeclared
  `--rate-limit`; every original PID/ID must be retained. The driver only
  invokes node upgrade, broker reload and cluster broker-only upgrade itself.
  The fixture cannot issue upgrades, change product ledgers or resume for ctl.
- `manifest.json` is the generated `transfer-driver-v1` profile. P29 sample writes
  `sample.json`, bound by `manifest_sha=SHA256(sorted-key JSON without newline)`,
  with positive `sequence` and `windows[requested-step]` (the step ID is in the
  evidence file `requested-step`). Each ordered window records label, leg,
  ready, elapsed_s, role, complete=false and durable_bytes/size or positive
  pending_responsibilities. Expired windows set `window_expired=true`. Sample
  remains read-only. Connection fault mapping is `connections.carrier/data`
  arrays of node/peer/port; the native judge independently verifies all affected
  attachments, connection/server/stream identities and postfault progress.
  W records watchdog current_cut_observed/elapsed_s/pending_data before release;
  X records original cancel ctl_pid. P29 owns schema/events/poison/OFD and all
  target completeness, history, resource and end-of-run oracles.
  `drills/lib/transfer_fixture_manifest.py` projects `fixture-manifest.json`
  using the frozen schema=1 reader after control artifact binding. Only this
  manifest is passed to all five fixture verbs. The driver planning manifest
  remains the scheduler's input. Hermetic real-start controls are part of
  `tests/transfer-fixture-selftest.sh`; they exercise the actual keeper and Go
  launcher against loopback protocol peers, without product/deploy claims.
- Drain joins every courier/finalizer/watchdog and retirement/consumer owner,
  gets fresh ACK plus JS scan, waits predeclared quiet>=2s, repeats fresh ACK/scan.
  The original judge log and rc are saved before stop or EXIT cleanup. Missing
  custody/ACK/channel tail is SETUP2; early finish/missed matrix is INCOMPLETE4.

Production patch handoff: `TRANSFER_MUTATION_PATCHES/<arm>.json` uses schema
`transfer-production-patch-v1`, exact `mutation_id`, catalog `defect,roles`, and
nonempty `changes` with production `path,source_sha256,function,before,after`.
The path must belong to that mutation's data owner; tests, contracts, stubs,
source drift, ambiguous needles and comment-only patches are rejected. Exact
owner patches are required after those implementations land. Missing patches
never build a synthetic "mutant". Existing GOFLAGS overlays are merged without
collisions and the new overlay is exported to all nested builds before business.
Each control builds two real capable images from the same production patch:
A with trimpath and B with trimpath=false, retaining distinct artifact SHA.
The deployment predeclares `mutation_artifact_urls.A/B` HTTPS URLs; before start
the driver binds the built binaries and hashes into the manifest. P29 stages
these URLs and launches the affected original roles from the patched artifacts.
This keeps ctl mutations on the original invocation and import mutations in the
candidate across cuts. No build runs while business is active.
P29 archives `mutation-runtime.json` with build_selector and actual role PID/
process_start/boot_id/artifact_sha/running_sha, plus `mutation-witness.json` with
mutation_id, exercised criterion, native_rc, raw_path/raw_sha,
oracle_input_path/oracle_input_sha and reader_digest. M02/M12B additionally
record default_dedup_ns=120000000000 and actual elapsed_s>=125. These bind the real source
patch to actual running bytes and the original judge inputs. Only exercised
native1 emits `control-receipt.json`; setup/compile/incomplete red never kills.
The runner marks a 64 control as DEVIATION when setup_red is nonzero or its
receipt is missing/invalid. A matching ASSERT-FAIL enum alone is insufficient:
the receipt must reconstruct the source/build/runtime/witness chain in this
invocation's announced evidence directory. Expected red still remains a blocker.

Hermetic driver validation uses the source-only T43/T44 architecture tests and
`python3 -I -B drills/lib/transfer_mutations.py selftest`. These prove contract
and parser controls; they provide no live acceptance receipt. Live exits await
P29/P32/P33 product/fixture integration, P34/P35 inventories and P37's same-candidate
native runs. No receipt or cost in this section is claimed measured.

| Ledger scope | Registered arms and source | Evidence and cost status |
|---|---|---|
| S8 transaction / rollback | `38-hot-update-rollback-compat`: 71 child IDs from `drills/lib/upgrade_tx_arms.tsv`; 50 positive cases and 21 independent mutation arms. `tests/drill38-contract-test.sh` is in the hermetic runner. | Declared / NOT_RUN; shell `ASSERT-FAIL` and S8 receipt `ASSERT-RED` both mean native rc1. Per-child declared worst is a conservative launch estimate, NOT_MEASURED; no parent launch cost. |
| S8 compatibility / historical boundaries | `39-hot-update-legacy-n1`: X/Y/P/F/BR retained, all 56 landed S8 additions plus NI/CI; `tests/drill39-contract-test.sh`. 31/33 V1 retains every historical assertion; V2 uses `tests/upgrade-regression-test.sh`. | New cells declared / NOT_RUN. Existing 39 Y/P/F/BR PRODUCT-RED and parent ownership retained. Historical V1 costs retain their original measured samples; new cells use estimates only. |
| S9 infrastructure | `85-hot-update-nats`: A/A1/B/D/Q/L/S/J/F; `86-hot-update-caddy`: R/B/I/H/C/K/F/U; independent 39 NI/CI capability refusals. Source: both drivers and `hot-update-infra-ledger.tsv`. | 21 declared claims / NOT_RUN; native rc, candidate/artifact/running SHA, raw oracle and marginal wall remain unset. Child costs are launch estimates from actual worst headers, never measured marginal costs. |
| S10 positive acceptance | `99-hot-update-fullstack`: C1/C3 calibration; A/B full rollout; S1/S3/F1/F3 prefix; K fixed-survivor soak. Sources: `drills/lib/fullstack_manifest.json`, `drills/lib/fullstack_fixture.py`, `test/fullstackharness/fixture.go`. | Nine children declared / NOT_RUN. C1/C3 estimates 2200s, A/B 4800s, S1/S3/F1/F3 2000s, K 1500s; exact frozen worst/ceiling map remains in the manifest. Same-candidate quiet/default/live-grow regimes, no retry or attribution, original cohorts and native child Join required. |
| S10 independent controls | `100-hot-update-fullstack-controls`: M01A/M01B; M02E/M02P/M02F/M02X; M03E/M03P/M03F/M03X; M04A/M04B/M04P; M05E/M05P/M05X/M05R; M06A/M06B/M06R/M06F; M07A/M07B/M07N/M07C; M08E/M08P/M08A/M08B/M08D/M08X/M08C/M08O; M09A/M09B; M10S/M10C; M11W/M11I/M11O/M11P/M11Q/M11F/M11S/M11X/M11D/M11C/M11L/M11U/M11K. Source: `drills/lib/fullstack_mutations.json` and `hot-update-fullstack-ledger.tsv`. | 50 exact keys / 77 topology cells / NOT_RUN. Independent baseline0→mutant1→restore0 archives, exact nonempty hit windows and P05 original supervised waiter required. Estimated worst600s/ceiling1200s per child; controls stay outside 99. |
| S10 supply / provisioning intake | P10 `lib/fullstack-artifacts-test.py`: `FullstackArtifactSupplyTest` (19 hermetic tests in its package receipt); P11 `drills/lib/fullstack_provision_test.py`: `FullstackProvisionTest`; four runtimes a0/a1/a2/a3 retain s10-small-push/s10-small-pull/s10-large-push/s10-large-pull. Both selftests are registered in the fullstack wrapper. | Package selftest receipts are hermetic-only. Full source→COPY→product-stage→executing evidence, real provisioning and live acceptance remain NOT_RUN; these rows certify no deployed generation. |

Documentation status is separate from live execution: the integrated tree contains S8 transaction and
S9 infra CLI/routes plus S10 fixture/observer declarations; 38/39/85/86/99/100 live runs, three hard gates
on the final candidate and Darwin native remain NOT_RUN/PENDING. S9 P39 supplies final infra receipts;
S10 P34/P35 supplies leaf coverage, carry closure and final candidate validation. No S10 A–H total is
signed here. S9 hot support remains linux/amd64 + systemd/pidfd/cgroup/nft, with explicit before-effect
unsupported/defer boundaries. Package workers use bounded hermetic/source checks; lead reports cost
before full default/live-grow runs and never overlaps deploy drills with go test.

The S10 declaration crosscheck runs through `tests/ledger-crosscheck.sh` and
`test/architecture/fullstack_drill_ledger_test.go`. The launch catalog in
`hot-update-fullstack-ledger.tsv` enumerates every on-disk launch unit for
`default` and `live-grow`, plus nine quiet 99 arms and fifty control 100 arms.
Parents only join children and have no launch cost. Each control row names its
exact applicable topology set; all 77 per-topology native chains remain separate
in the preceding mutation table. Neither this catalog nor a matching expected
verdict certifies a live run. Upstream artifact/cell provenance remains pending
its manifest owner and S10/P27; S10/P19/P20/P26/P32/P35 own the final S10 launch
profile, assembly and execution receipts.

`drill-costs.tsv` is functional input. The frozen S10 plan section 9.2 estimates
180s per control child, worst 600s and ceiling 1200s; this replaces the earlier
conservative 600s launch estimate before candidate sealing. Nine positive arms
sum to 23500s and fifty controls to 9000s. These are estimates, NOT_MEASURED and
NOT_RUN, and do not include builds, gates, platform checks or fuzz overhead.
Changing costs, expected verdicts, topology sets, schedules or ceilings requires
a candidate re-seal and scheduling review. Such edits are outside the receipt
whitelist. S10/P35 may populate independent measured results only after retaining
the original candidate, native rc, raw evidence and resource peaks; missing live
evidence stays NOT_RUN. Full-directory totals await that intake, and historical
samples never sign the current candidate. No deployment run is launched by this
crosscheck.
