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
| `11-grow-gaps` | RED (#8/#I1): `grow` reaches VOTER only via labeled workarounds — `join approve --wait` stalls pre-mesh (#8, tether's own "still in flight"); a fresh joiner without `cluster init` can't serve (#I1, `serve` refuses "no raft state"); asserts the `GREW-VIA-WORKAROUNDS` trailer |
| `13-inbroker-reconcile-perm` | RED (#22): the in-broker reconciler (User=tether) can't `CreateTemp` in the root-owned `/etc/tether` — auto-reconcile is broken, so the operator must hand-render as root (why `grow`'s mesh render is `[workaround #22/#3]`) |
| `20-forcesingle-natsconf` | RED (#20): force-single leaves nats.conf clustered → tier-B JS 503-rots |
| `21-smalldisk-tierb` | RED (#21): 8 GiB OBJ_xfer reservation denies tier-B on a small (tmpfs-capped) store |
| `12-ghost-voter` | RED (#12): force-single ghost VOTER — all three online removal paths refuse |

## Real `tether` findings surfaced (2026-07-05)

This tool caught cross-process deployment defects unreachable by the in-process suites, logged in
`docs/v0.4.5-ha-grow-ops-gotchas.md`:

- **#22** install.sh leaves `/etc/tether` root-owned → a `User=tether` nats.conf write (the in-broker C3
  reconciler's path) can't create its temp/`.bak` there → topology never auto-converges (drill `13`
  exercises this WRITE via a manual `reconcile nats --manual` as tether — it does NOT observe the auto
  reconciler firing, which grow's root render front-runs). `/etc/tether` is NATURALLY root-owned here (a
  fresh docker named volume mounts root:root + install.sh never chowns ETC), so the sim reproduces #22
  without any sim chown. **Correction (2026-07-05):** an earlier BAKED image had once masked #22 (a stale
  provision that chowned ETC to tether, from before that chown was removed but not rebuilt); `doctor`
  tripwires a tether-owned `/etc/tether`. `provision-node.sh` deliberately does NOT force the owner, so a
  future install.sh #22 fix (chowning ETC → tether) makes drill 13 flip RED-for-promotion — its
  "`/etc/tether` DIRECTORY is root-owned" control fails AND the write succeeds (`assert_bug` "APPEARS
  FIXED") — signalling "promote to a plain regression".
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
