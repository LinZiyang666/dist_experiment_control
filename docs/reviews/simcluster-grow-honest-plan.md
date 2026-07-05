# simcluster grow rework — "expose, never compensate" (option B) — FINALIZED (post Stage-A)

> Rework `test/simcluster/simcluster` `cmd_grow` (and the reload/recovery/init it does) to obey the sim
> **Mandate** (`test/simcluster/README.md` top / `CLAUDE.md` §5 「模拟集群定位铁律」): run `tether`'s REAL
> grow commands + documented manual steps, **label every step** `[env]` / `[tether]` /
> `[workaround #N]` / `[GAP #N]`, and **assert the gaps** by their REAL signatures — instead of silently
> scripting around `tether`'s shortcomings. Option **B** (user-chosen): runbook-faithful — real commands,
> gaps presented, still reach N=3 so other drills run.
>
> **This is the finalized plan.** It supersedes the draft after a Stage-A 8-expert adversarial workflow
> (`wf_963b9a43-a99`, all Opus 4.8) found the draft's drill signatures were mechanically impossible
> against the real `tether` code. Every drift is corrected below with a code citation. Adjudication log at
> the end.

## 0. The decisive code facts Stage-A established (these reshape everything)

- **`reconcile nats --all --wait` (the auto path) is a PASSIVE STATUS POLLER** — `cmd/tether/
  cluster_reconcile.go:80-107` only calls `fetchClusterStatusReport`+`topoLaggards` in a loop. It never
  renders/harvests/writes `/etc/tether`. Its OWN stderr is only: exit-0 `no topology generation is being
  managed yet (nothing to converge)` / `all voters converged`, **or** non-zero `reconcile nats: timed out
  after <t>; not converged: <laggards>`. The #3/#22 cause token lives in the laggard's
  `TopoReconcileReason` (embedded in the laggard tuple, `cluster_reconcile.go:128`) and in the in-broker
  reconciler's journal — **never on the CLI's own stderr.** `topoLaggards` counts only VOTER-phase nodes,
  so a freshly-added non-voter joiner is not a laggard.
- **`join approve` never AddVoters.** `StartJoinOperation` verifies PoP + mints a deterministic opID +
  INSERTs the op row + commits the roster upsert; the leader controller loop `driveJoin` does
  AddNonvoter/AddVoter **asynchronously** (`cluster_operation_controller.go:490,528`). So a killed
  `--wait` staged nothing extra, and a re-`approve` of the same bundle is a silent idempotent no-op
  (same opID → attach) while in-flight, or a `nonce already consumed` refusal after terminal. The unblock
  path for a BLOCKED join is `cluster ops confirm <opid>`, **never** a second approve.
- **#3 and #22 are mutually exclusive per grow.** In the in-broker reconciler the mTLS harvest (#3,
  `internal/natsreconcile/reconcile.go:116-123` → `natsconf/preflight.go:243` "no cluster{} block to
  harvest routes mTLS") returns BEFORE any write; the CreateTemp perm-denied (#22, `natsconf/
  takeover.go:190`) is only reached AFTER a successful harvest AND when `merged != current` (a real
  write). First grow: former-N1 conf is standalone → harvest fails (#3), any user. Later grow: conf
  already clustered → harvest ok → CreateTemp perm-denies (#22), only as `User=tether` vs a root-owned
  `/etc/tether` DIRECTORY. A merged==current fast-path (`reconcile.go:130-143`) writes nothing → no
  perm-deny → so the grow's own root pre-render MASKS #22 unless the assertion forces a gen ahead of disk.
- **`systemctl restart nats-server` exits 0.** `assert_bug` (`test/simcluster/lib/assert.sh:45`) treats
  exit-0 as "APPEARS FIXED → drill FAILS". So #23 must be asserted on the SIDE EFFECT (broker stranded),
  not on the restart command.
- **assert_bug matches a signature in the FAILING command's combined stdout+stderr, and a clean exit is
  scored APPEARS-FIXED.** Every gap assertion must therefore (a) exit non-zero on the reproduced bug, and
  (b) carry a signature that appears ONLY on the failure path and ONLY for THIS gap.

## 1. cmd_grow rework (the heart of B)

Every step logs a tag: `[env]` = sim provisioning (its job) · `[tether]` = a real tether command an
operator runs · `[workaround #N]` = the sim doing the documented manual step because tether can't (the
gap is real, the workaround is what an operator does today) · `[GAP #N]` = a tether defect surfaced live.
Every attempt-real-first step that is EXPECTED to fail is `capture … || true` (NEVER `|| die`), with an
explicit short `--timeout` (never the 60s/5m defaults), so `grow` still exits 0 and `10-grow-to-3` stays
the reach-N=3 guard.

### I1 — the joiner `init --from-existing` audit — **RESOLVED on the server (2026-07-05)**

Experiment (instance `i1exp`): removed the standalone-boot + `init --from-existing`, ran grow with a
truly fresh joiner (empty `raft/`, no `tether.db`, only the broker.yaml cluster seam +
keygen/prepare/approve/mesh/start). **Result: brk2 crash-looped exit-70 and never reached VOTER**, with
the exact error (`internal/broker/cutover.go:60`):
`broker: broker.cluster.data_dir="…" is set but no raft state exists — run `tether cluster init
[--from-existing]` first (the daemon never auto-bootstraps a cluster onto a live DB)`.

**Verdict: the joiner `init --from-existing` is REQUIRED, not a removable compensation.** Cluster-mode
`serve` hard-refuses without pre-existing raft state; a joiner MUST bootstrap its raft state
(`cluster init --from-existing` migrates a DB, or `--from-manifest` reconstructs from the C2 manifest),
which makes it a single-voter cluster; the leader's `approve`→AddVoter→InstallSnapshot then absorbs it
(the 32b28e9 identity-preservation path). So:
- **KEEP** the joiner `init --from-existing` — label it `[tether]` (a real, required command). The
  standalone-boot that creates the `tether.db` it migrates is `[env]` (a fresh broker runs standalone
  first, exactly as a real operator provisions a new broker host before joining it).
- **The honest gap surfaced (NEW, not previously catalogued):** the `join` flow (prepare/approve) does
  NOT bootstrap the joiner's raft state, and the runbook §1 joiner path (`docs/cluster-runbook.md:82-95`,
  keygen→prepare→approve) **omits the required joiner `cluster init` step entirely** — its §3a
  "migrated by init --from-existing" refers to the former-N1, not the joiner. A fresh operator following
  §1 literally hits the exit-70 above. This is exactly what the aspirational `tether cluster add` (§B)
  must fold in. Pin it: `assert_bug 'cluster-mode serve refuses a fresh joiner (join does not bootstrap
  raft state; runbook §1 omits the joiner init)' '#I1' 'no raft state exists|never auto-bootstraps' <serve
  on a fresh joiner with the cluster seam but no init>` (RED today; flips GREEN the day join self-bootstraps
  the joiner). This is the cleanest new finding of the whole rework.

### Steps

1. `[env]` mint + distribute secrets (route/tunnel leaves WITH `subjectAltName=DNS:<node>`). The minting
   is the environment's job; it *works around* `[GAP #24]` (tether has no SAN cert tooling — `cluster
   keygen` mints only nkeys, and `transport.go`'s InsecureSkipVerify comment misleads toward CN-only).
2. `[tether]` joiner `cluster init --from-existing` **OR** the I1 replacement, via pty-confirm — the pty
   feed is `[GAP #5]` (init has no machine-escape confirm).
3. `[tether]` joiner `cluster join prepare` → self-signed bundle.
4. `[tether]` leader `cluster join approve <bundle> --wait --timeout 30s`, captured `|| true`. 30s < the
   2-min catch-up deadline (`controller.go:281,452`) so the op is still CATCHING_UP and the CLI prints
   the stable give-up line `operation <id> still in flight (<state>) after 30s` (exit 75) — that IS the
   `[GAP #8]` surface (catch-up can't complete before the mesh + joiner NATS exist). Do **NOT** re-approve.
5. `[workaround #10]` render the full route mesh on **every** broker INCLUDING the joiner (drop the joiner
   and it boots clustered-but-alone → #10 exit-70 crash-loop). Before rendering, `[tether]` attempt
   `cluster reconcile nats --all --wait --timeout 25s` (captured `|| true`) so the `[GAP #3]` (first grow)
   / `[GAP #22]` (later grow) failure is on the record; then do the `[workaround #22/#3]`
   `reconcile nats --manual --secrets-dir --peer …` per broker as root (the gotcha-§A#3 escape — the
   runbook has NO documented recovery when `--all` fails). **Remove** the `chown tether:tether
   /etc/tether/nats.conf` (Mandate ①; it doesn't help — CreateTemp needs DIR write and the dir stays
   root-owned; nats-server reads a root-owned 0644 conf fine).
6. `[workaround #4]` former-N1 JS reset (`mv jetstream → jetstream.grow-bak.<ts>`) on the FIRST grow only
   — standalone JS does not migrate into the clustered meta (tether should auto reset/backup-restore).
7. `[workaround #23/#10]` reload existing voters' nats via SIGHUP (NOT "legitimate ops" — the documented
   mesh-pickup is `systemctl restart nats-server`, which clean-exits the running broker (#23) and bounces
   an in-mesh voter to CATCHING_UP (#10); the sim SIGHUPs to dodge both). Start the joiner nats + joiner
   `tether-broker` (cluster mode); the already-driving op auto-promotes it to VOTER. If it hits BLOCKED,
   recover with `[tether] cluster ops confirm <opid>`.
8. `[workaround #23]` former-N1 broker recovery (`reset-failed` + `start`) — its nats was stopped for the
   JS reset, so its broker can strand under `Restart=on-failure`.
9. wait joiner → VOTER. Emit a machine-parseable trailer on stdout:
   `GREW-VIA-WORKAROUNDS: #3,#4,#8,#10,#22,#23` (only the gaps this grow actually hand-fixed). grow exits
   0. The day a real fix removes a workaround, the trailer changes and `11-grow-gaps` (below) flips.

## 2. Drills — signature-guarded, REAL strings only

`assert_bug <desc> <sig-id> <ERE-signature> <cmd…>` reproduces a bug iff `<cmd>` exits non-zero AND its
combined output matches `<ERE-signature>`; a clean exit ⇒ APPEARS-FIXED fail; a non-matching failure ⇒
HARD-FAIL. All signatures below are REAL tether/nats/systemd strings (code-cited), never a sim-echoed
token (except where a composite wrapper must emit its own failure marker). Each drill is independently
runnable (filename auto-discovery, `simcluster:498`); add each to the README table + the run-one-at-a-time
note.

### `11-grow-gaps.sh` — the in-grow gaps (runs a real grow, asserts what it had to hand-fix)

- **#8** — `dexec -u tether $L -- tether cluster join approve $BUNDLE --wait --timeout 30s`; sig
  `still in flight.*after|is BLOCKED|catch-up exceeded|cluster ops confirm`. Controls FIRST (pin to #8,
  not #7 slow-snapshot): leader status shows joiner phase ∈ {JOIN_VERIFIED_PENDING_VOTER,CATCHING_UP}
  AND `reachable==false`. Flips to `assert_ok`(exit 0) the day mesh-before-catchup is fixed.
- **#3** (first grow) — after the joiner is CATCHING_UP (TopoDesired>0), `dexec -u tether $L -- tether
  cluster reconcile nats --all --wait --timeout 25s`; CLI sig `reconcile nats: timed out .*not
  converged`; PLUS a separate `assert_ok` cause-probe on the former-N1 laggard:
  `tether cluster status --json | jq -r '.nodes[]|select(...==$L).topo_reconcile_reason'` matches
  `no cluster.{0,4}block to harvest|could not be assembled`.
- **#10** — start a broker with a clustered conf + NO peers; sig from `journalctl -u tether-broker`:
  `JetStream.*UNAVAILABLE|cluster mode requires|exit .*70`.
- **#4** — first grow, restart former-N1 clustered WITHOUT the mv reset; sig from `nats stream ls` /
  `nats str info`: `orphan|stream .* not found|no responders|0 streams` (the standalone streams are not
  adopted by the clustered meta).
- **trailer** — `assert_ok 'grow limped via documented workarounds' sh -c "$SIM grow brkN 2>&1 | grep -q
  'GREW-VIA-WORKAROUNDS: .*#8'"` — proves grow's success is NOT clean; flips when a fix drops a workaround.

### `13-inbroker-reconcile-perm.sh` — #22 (the in-broker reconciler can't write /etc/tether)

Setup: healthy clustered N≥2 (confs already clustered so harvest succeeds; `/etc/tether` DIR root-owned);
bump the topology generation via a self-only op that forces a write ahead of disk WITHOUT a preceding root
manual reconcile of that gen (e.g. `tether cluster set-raft-addr <self>:7400 --route …`, which bumps
`topology_generation`). Then `dexec -u tether $L -- tether cluster reconcile nats --all --wait
--timeout 25s`; CLI sig `reconcile nats: timed out .*not converged` + `assert_ok` cause-probe on the
laggard's `topo_reconcile_reason` / journal matching `natsconf: temp:.*permission denied|/etc/tether/\.nats\.conf\.[A-Za-z0-9]+.*permission denied` (the discriminating path token — NOT bare `permission
denied`, which also matches gotcha #6's root-owned `tether.lock`). Control: `[ "$(… stat -c %U
/etc/tether)" = root ]` (the DIRECTORY, not nats.conf).

### `14-nats-restart-strands-broker.sh` — #23 (running broker clean-exits on nats restart, never revives)

Run on a healthy multi-node cluster; restart a **FOLLOWER's** nats (its JS meta re-forms via the other
voters → excludes #10). `assert_ok` the restart; then `assert_bug` a survival predicate that exits
non-zero when stranded: sig `inactive|broker: shutting down`, cmd = `sh -c 'sleep 8; is-active
tether-broker | grep -qx active || { journalctl -u tether-broker | grep -i "broker: shutting down"; echo
inactive; exit 1; }'`. Controls to pin the CLEAN exit (exclude a #10 crash): `systemctl show
tether-broker -p Result --value` == `success` AND `-p NRestarts --value` == `0`. **Flake caveat** (header
it): the broker sets `nats.MaxReconnects(-1)`; the clean-exit is an unlocated loop-return, so a fast
restart may let it reconnect and survive → non-deterministic. Prefer journal-evidence of the clean exit +
`NRestarts==0` over an is-active snapshot; if flaky, lengthen the outage (stop, dwell ≥6s, start).

### Follow-on drills (lower priority; land after the core)

- `15-cnonly-cert.sh` — **#24** (MANDATORY, not optional — a green SAN grow proves SANs work, NOT that
  CN-only fails). Mint a CN-only route cert; sig from the NATS-SERVER surface (`journalctl -u nats-server`
  + `curl -s localhost:8223/routez`): `certificate relies on legacy Common Name|use SANs instead|num_routes.{0,3}0`.
- `16-init-no-confirm.sh` — **#5**. `dexec -u tether $B -- sh -c 'tether cluster init --from-existing …
  </dev/null'` (no TTY); sig `not a terminal|confirm|node.?id`.

## 3. What does NOT change (Mandate ③ — the environment's job stays the sim's job)

- `up` / install.sh provisioning, secrets placement, `/etc/tether` left **root-owned** (reproduces #22),
  the SAN-bearing certs, the broker.yaml cluster seam (a daemon-config step; note in the table that
  `cluster init` arguably should emit it). Untouched.
- `10-grow-to-3.sh` stays **GREEN** (grow reaches functional HA N=3 via the labeled workarounds). Reframe
  its "#23 guard" comment: asserting every broker is ACTIVE after grow certifies the sim's RECOVERY ran
  (a reach-N=3 control), NOT that #23 exists — the real #23 signal lives in `14-*`. Add a postcondition
  that the joiner's `/etc/tether/nats.conf` is clustered BEFORE its broker starts, so a dropped-joiner
  regression fails loud instead of hanging 150s.
- `force-single` / `#20` / `#12` / `#21`: force-single already faithfully leaves nats.conf clustered
  (EXPOSES #20 by design) — no compensation. Left as-is; re-audit later.

## 4. Invariants for the implementation

1. Every attempt-real-first step EXPECTED to fail is `capture … || true`, never `|| die`; grow still
   exits 0; `10-grow-to-3` is the reach-N=3 guard.
2. Always pass an explicit short `--timeout` (`join approve --wait --timeout 30s`, `reconcile nats --all
   --wait --timeout 25s`); never the 60s/5m defaults (they inflate wall-clock on the JS-starving box).
3. Keep the joiner in the mesh render set (step 5); dropping it → #10 hang.
4. Never re-`approve`; let the controller drive; `ops confirm` for BLOCKED.
5. `dexec -u tether` for any assertion whose gap is the tether-user path (#22, #8, #5) — `simcluster
   exec` runs as root and does not forward `-u`.
6. Signatures are REAL command output, code-cited; split #3 vs #22 (never OR); pin #22 to the
   `/etc/tether/.nats.conf.*` path token; pin #23 to `Result=success` + `broker: shutting down`.

## 5. Acceptance

- `simcluster grow brkN` transcript shows `[tether]` real commands + `[workaround #N]`/`[GAP #N]` labels,
  with the `[GAP #8]` `still in flight` line and the `[GAP #3/#22]` `--all` timeout PRINTED live, and
  still reaches VOTER + emits the `GREW-VIA-WORKAROUNDS` trailer.
- `10-grow-to-3` still GREEN on the server (reach-N=3; hollow-voter parity intact; joiner-conf-clustered
  postcondition added).
- `11-grow-gaps` RED-reproduces #8 + #3 + #10 + #4 and asserts the trailer, each by its REAL signature
  (HARD-FAILs on signature drift, forcing human re-attribution). `13-*` reproduces #22; `14-*` #23.
- I1 resolved on the server (init removed, or labeled `[GAP #I1]` + asserted).
- Stage-C adversarial review (multi-expert, all Opus 4.8) on the rework before done. **STOP at the
  external-review gate** (goal: 按流程前进于外审门停止).

## 6. Stage-A adjudication log (wf_963b9a43-a99, 8×Opus, all "plan-needs-material-changes")

ADOPTED (blocker): `--all` is a poller not a writer → assert the timeout wrapper + probe the laggard
reason (not the CLI stderr); split #3/#22 by grow ordinal with distinct real signatures; #8 via tether's
own `--wait --timeout` not shell `timeout(1)`; drop the second `approve` (controller drives; `ops
confirm` for BLOCKED); #23 assert the side effect not the restart; #10 is an unlabeled workaround → label
+ assert; SIGHUP is a #23/#10 dodge not "legitimate"; I1 — audit/remove the joiner `init --from-existing`
(runbook has no init; it seeds the 32b28e9 hazard).
ADOPTED (major): remove the `chown` (Mandate ①); #4 and #5 must be asserted, not just narrated; #24
mandatory + assert the nats-server surface; relabel SAN-mint as `[env]` working-around-#24; grow trailer
so smoothness can't be consumed silently; per-grow gating so a fixed gap flips; `/etc/tether` DIR-owner
control; #22 path-token signature (not bare perm-denied, which collides with gotcha #6); #23 flake caveat
+ Result=success/NRestarts controls.
ADOPTED (minor): reword `--manual --peer` provenance (gotcha §A#3 escape, not a runbook step); add
11-grow-gaps to the README table + single-run note; `--timeout 25s` window sizing; broker.yaml cluster
seam added to the compensation audit.
DEFERRED: `15-*` (#24) and `16-*` (#5) land after the core grow rework + `11/13/14`.

## 7. Stage-B implementation log (2026-07-05, on the server)

What was actually built + the discoveries that shifted the plan:

- **cmd_grow reworked + validated (N=1→2→3, GREEN):** every step labeled `[env]`/`[tether]`/`[workaround
  #N]`/`[GAP #N]`; `join approve --wait --timeout 30s` surfaces #8 live (tether's own "still in flight");
  the two forbidden `chown tether:tether /etc/tether/nats.conf` lines REMOVED (conf stays root:root 0644,
  nats-server reads it fine — verified all brokers active + conf owner root); emits
  `GREW-VIA-WORKAROUNDS: …` trailer. No re-approve (controller drives the staged op).
- **I1 RESOLVED (see §1): KEEP the joiner `cluster init --from-existing`** — it is REQUIRED (cluster-mode
  `serve` hard-refuses without raft state, exit-70 "no raft state exists"), not a removable compensation.
  The gap is that `join` doesn't bootstrap the joiner + the runbook §1 omits the init step (`#I1`).
- **#22 fidelity bug DISCOVERED + FIXED (the biggest Stage-B finding):** the sim had NEVER actually
  reproduced #22. `/etc/tether` is a docker named volume initialized **tether-owned (uid 999)**, and
  install.sh's idempotent `mkdir -p` never re-owns it → the in-broker reconciler could write → #22 never
  fired (`doctor` was warning "MASKS #22", unnoticed; the DEGRADED health was the SIGHUP/config_load_time
  issue misattributed to a perm-denial). Fix: `provision-node.sh` now `chown root:root /etc/tether`
  (correct the docker artifact → real-fleet faithful; Mandate = reproduce reality, not accommodate
  tether). Verified: root-owned ETC → grow still reaches VOTER; #22 now reproduces as tether via
  `reconcile nats --manual` → `natsconf: temp: open /etc/tether/.nats.conf.*: permission denied`.
- **#23 DEFERRED (no reliable RED drill):** empirically, restarting a healthy follower's nats yields an
  exit-70 CRASH that `Restart=on-failure` REVIVES — not the documented clean-exit-0 strand (whose trigger
  gotcha #23 itself says is unlocated). So #23 is not deterministically reproducible; `grow` labels +
  recovers it (`[workaround #23]`) and the README documents why there is no drill.
- **Drills built:** `11-grow-gaps.sh` (#8 real stall + #I1 fresh-joiner exit-70 + option-B VOTER + the
  `GREW-VIA-WORKAROUNDS` trailer) and `13-inbroker-reconcile-perm.sh` (#22, signature pinned to
  `natsconf: *temp:.*permission denied`, with a DIR-root-owned control). Both signatures captured
  empirically on the server, never guessed.
- **Deferred (bespoke mid-grow setups; grow already LABELS them):** #10, #4, #24, #5, #23.
