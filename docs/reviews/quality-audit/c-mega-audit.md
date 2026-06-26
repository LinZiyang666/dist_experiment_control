# C1–C8 v2-usability epic — ≥20-expert mega-audit (final hardening before external review)

> Workflow: 22 expert lenses (per-lens audit → per-lens adversarial verify) + synth = **45 Opus 4.8 agents** (raw tasks/wettiwgb1.output). Verdict: **CONDITIONAL PASS — 0 BLOCKER + 12 confirmed MAJOR + many MINOR**. Requirement-fidelity scorecard: all 5 rejections REFUSED (#5 had one windowed false-safe = MAJ-5, now fixed); 建议1–7 each PASS or partial-on-a-finding.
>
> **CLOSURE STATUS (post-fix, INDEPENDENT verification PASS): ALL 12 MAJORs CLOSED; no fix introduced a new defect (the subtle concurrency fixes MAJ-2/3/4 adversarially re-checked for strand/leak/spin/misfire/deadlock — clean); make test + make lint + make e2e all green.**
>
> Main-process dispositions — ALL 12 ACCEPTED (10 fixed in code, MAJ-11/12 = honest tracked follow-ups):
> - **MAJ-1** (BLOCKER-adjacent: silent roster/raft fork) — `AbortDrain` now `assertNoActiveOp` first (clusterdrain.go); a `drain --abort` of an active retire op is refused (use `ops abort`), a plain drain still aborts.
> - **MAJ-2** (C5 broken in multi-broker: proxy sub create/revoke wildcard SUB → queue-group → follower silent-drop) — register the CONCRETE `.proxy.sub.{create,revoke,list}.req` leaves (broker.go); create/revoke → broadcast+leader-only, list → queue-grouped RODB. Pinned TestMegaAuditMAJ2.
> - **MAJ-3** (C5 reaper rotation can't rebuild a node@current-keyset → /sub black hole) — `portRebuild` predicate gates the re-ACK + proxyNewer drop so a NEW-port token@same-keyset routes to the rebuild arm (agent/proxy.go). Verified no misfire on rehome (same port) / keyset-only push (no token).
> - **MAJ-4** (C1 rehome want orphaned on canceled per-session ctx) — applyOneHome defer restarts the want on the LIVE successor session ctx (a.loadRunCtx) instead of waiting for the next full register (agent.go); single-loop invariant + 2min bound preserved.
> - **MAJ-5** (C7 require-credential-rotation raise-failure = durable false-safe, rejection #5) — bounded-retry the raise (3×), pass the TRUE raised flag to the guide, exit NON-ZERO on persistent failure (cluster_retire.go). Pinned TestMegaAuditMAJ5.
> - **MAJ-6** (C3 SIGHUP reload storm + perma-DEGRADED with no http monitor) — gate the reload on the conf mtime advancing since the last reload (topology_reconcile.go); a real swap reloads once, a no-monitor fast-path no-ops after the first.
> - **MAJ-7** (proxy quorum codes → exit 70) — map proxy_disabled_no_quorum/proxy_frozen_readonly → 75 (transient), ha_policy_invalid → 64 (error_hints.go). Pinned TestMegaAuditMAJ7.
> - **MAJ-8** (leaderRedirect election → exit 77) — leaderRedirect returns a transient (75) for an election bounce (LeaderHost==""), permission (77) for a named-leader bounce; 13 call sites updated. Pinned TestMegaAuditMAJ8.
> - **MAJ-9** (status F==0 next-step names deleted `cluster add`) — → join prepare/approve (clusterstatus.go). TestD7ComputeHealth updated.
> - **MAJ-10** (proxysub Plan bakes raw local-zone `now` → DIFF-1 ordering break) — bake LitTime(now.UTC()) at created_at + revoked_at (proxysub/plan.go).
> - **MAJ-11** (OpPortReassignHome FSM fail-stop on a 0017-less member; join gate no schema floor) — TRACKED FOLLOW-UP: this is the standing reinstall-not-upgrade invariant (CLAUDE.md §5), shared by every column-adding migration 0008–0016, NOT a C-epic regression. Documented explicitly at versionSkewResponse; an in-band schema/migration-version floor in the join handshake is a v2.x follow-up.
> - **MAJ-12** (test-fidelity: 1 FALSE coverage comment = 阳奉阴违 + headline acceptances token-scan-only) — corrected the false `reconcile_test.go` comment NOW; the named gated drills (test/c3/c4/c5/c6 + the C4 resume battery) are TRACKED follow-ups (consolidated with the C5/C6/C7 drill follow-ups).
> - **MINORs** (dozens, all CONFIRMED) — a high-value subset fixed inline (QUORUM_LOST next-step → recovery force-single); the remainder (exit-code outliers, C7 HEALTHY-while-severe-manual-alert, C5/C6 event change-keying, C2 SSRF defense-in-depth, wire-proto golden coverage, etc.) recorded here for the external review + the post-GA hardening backlog — none is a security-boundary or correctness BLOCKER.
> - **Tracked follow-ups (carried into the unified external review):** in-band schema-floor join gate (MAJ-11); the gated test/c3·c4·c5·c6·c7 integration drills + the C4 resume battery + e2e registration (MAJ-12); the C7 computeHealth-reads-active-manual-alert surface; the MINOR backlog list below.
>
> False-positives screened by adversarial verification (excluded): handleAdd dual-surface (CLI-unreachable latent, OVERSTATED→MINOR), c5 stale-read-drops-token (REFUTED — Propose/Apply commit-synchronous under applyMu), c3 reload cluster-wide-raft-storm (OVERSTATED→per-broker-local SIGHUP), c1 counter-rollback-on-backward-clock (effectively unreachable).

---

## Verdict

**CONDITIONAL PASS** — 0 confirmed BLOCKERs (build/`go vet`/cheap suites green; no FSM panic, raft split-brain, dual-write, wire-incompat, or unconditional rejection-table breach on any honest path), but **12 confirmed MAJORs** (one at the rejection-#5 boundary, the rest availability / retry-automation / operator-footgun / test-fidelity). Not clean — resolve or explicitly adjudicate the MAJORs before the unified external review.

Scope note on severity: per the project rubric these MAJORs are real but none crosses into BLOCKER — the closest is C7-FALSESAFE (windowed durable-surface false-safe of rejection #5); external review may elect to elevate it.

---

## Confirmed BLOCKERs

**None.** Every candidate that could have been a BLOCKER was either confirmed non-reachable on the honest path or downgraded by adversarial verification (see *False-positives screened*). The PSK-in-raft boundary, FSM determinism (all-literal leader-baked SQL, poison-safe custom applier), single-WAL-owner write routing, and rejections #2/#3/#5 (normal path) all hold with enforcing tests.

---

## Confirmed MAJORs

Ordered by operational risk.

### MAJ-1 · C4 `AbortDrain` missing `assertNoActiveOp` → silent roster/raft fork
- file:line: `internal/broker/clusterdrain.go:269` (no guard) vs guarded siblings `DrainNode` `:83`, `RemoveNode` `:192`, `AddNode` `internal/broker/clusteradmin.go:187`. Live-reachable: flag `cmd/tether/cluster.go:510`, dispatch `internal/broker/clusterstatus.go:669-670`.
- proof: With a retire op holding phase `DRAINING` (`cluster_operation_controller.go:486-490`), `cluster drain X --abort` flips phase→VOTER (`clusterdrain.go:270`) with no active-op check. Next `RAFT_REMOVED` tick: `setPhase RETIRING` is gated `phase==DRAINING` → skipped; `RemoveServer` (`controller:538`, gated only on `inRaft`) removes the voter; `PlanClusterNodeRemove` deletes only phase∈{VOTER_ADD_FAILED,RETIRING} (`membership_ops.go:290-291`) → RowsAffected==0 → **raft voter removed, roster row stuck VOTER**. Reachable unconfirmed at N≥4 (`ProjectQuorum(4,true).FaultTolerance==1`); at N=3 the op already carries `Confirmed`. Recovery is poor (`ReconcileMembershipOnLeadership` only logs INCONSISTENT, `clusteradmin.go:324-326`; `RemoveNode` refuses a VOTER-phase node).
- invariant: c4-plan §6 "raw drain-family mutators REFUSE when an active op exists"; §8.1 RETIRING-before-RemoveServer.
- fix: `assertNoActiveOp(nodeID)` as the first line of `AbortDrain` (verified not to break plain `drain` or post-`ops abort` cleanup).

### MAJ-2 · Proxy `sub create`/`sub revoke` wildcard SUB becomes a queue group → silent-drop on followers
- file:line: `internal/broker/broker.go:798,816`; leaf list `internal/broker/clusterwrite.go:70`; follower silent-return `internal/broker/proxy_cluster_wire.go:93,111`; ctl single-shot `cmd/tether/proxy.go:81-83`.
- proof: The control plane registers the wildcard `…proxy.sub.*.req` (one SUB), and `isBroadcastClusterSubject("…proxy.sub.*.req")` is **false** (`HasSuffix` vs the concrete `.proxy.sub.create.req`/`.revoke.req` leaves), so the whole subscription is queue-grouped (reproduced: `proxy.set.req`→BROADCAST, `proxy.sub.*.req`→QUEUE-GROUP). In an N-broker cluster the request lands on a follower ~(N-1)/N of the time → `isClusterFollower()→return` with no reply → ctl 15 s timeout, no retry. C5 subscriber create/revoke is intermittently non-functional in the exact multi-broker mode C5 exists for. Availability-only (follower returns before `MintSubscriber`; single-mode byte-equivalent). It is the *unique* broken instance among all 8 silent-follower handlers (register/heartbeat use separate plain `Subscribe`).
- invariant: C5 broadcast+leader-only contract (`proxy_cluster_wire.go:14-18`) silently defeated by the wildcard.
- fix: register `.proxy.sub.create.req`/`.revoke.req` as their own broadcast subs (keep `.list.req` queue-grouped), **or** forward-to-leader in the two cluster handlers. Do NOT broadcast the wildcard (`sub.list` has no follower gate → duplicate replies).

### MAJ-3 · C5 reaper rotation cannot rebuild a node already at the current keyset epoch → data-plane strand + `/sub` black hole
- file:line: `internal/broker/proxy_reconcile.go:149-164` (rotation branch), `internal/broker/proxy.go:172,195` (agent-side drop gates), `internal/agent/proxy.go:133`; cluster keeps `d.Generation==0` (`proxy.go:897-904`).
- proof: A reaper rotation mints a NEW port+token but pushes `{Token:NEW, PublicPort:NEW, Epoch:<current keyset>, Generation:0, Home.Epoch:0}` — `PlanAllocateProxy` bumps only the home axis, never `sessions.proxy_epoch` (`port/plan.go:227-235`). For a steady-state agent serving at `p.gen=0, p.epoch=keyset, p.srv!=nil`: rehome branch is N1-scoped by `Home.PublicPort==p.publicPort` (NEW≠OLD → skip); exact-equal re-ACK fires (`proxy.go:172`) → `pubProxyReady(true);return`; `!proxyNewer(0,keyset,0,keyset)` drops. The rotation frees the old port row (old tunnel can't re-REGISTER) and `/sub` renders a port the agent never opened — **no auto-recovery** without keyset bump / `proxy off/on` / restart. Triggered by ≥`proxyRehomeDwell`≈15 s `proxy_ready=false` while home stays leader-reachable (tunnel partition, or a crash-failover whose new-home tunnel takes >15 s). Heals only `p.epoch < keyset` nodes; every serving agent is `p.epoch == keyset`, so the M3 self-heal comment (`proxy_reconcile.go:148`) overpromises broadly.
- invariant: 建议4 "auto-switches WITHOUT proxy off/on".
- fix: gate the re-ACK (`proxy.go:172`) and `proxyNewer` drop (`:195`) with `d.Token=="" || d.PublicPort==p.publicPort` so a NEW-port token directive routes to the rebuild branch; or bump the keyset epoch on rotation.

### MAJ-4 · C1 rebuild orphans a rehome `want` when the dying session's per-port loop is on the canceled ctx
- file:line: `internal/agent/agent.go:1188-1191` (no-spawn hand-off) + `:1226-1228` (cleanup `else` drops the want).
- proof: Loops run on the per-session `runCtx` canceled by `cancelRun()` before `session()` returns. Session A's `applyOneHome` is typically blocked in the ctx-unaware blocking `ApplyHome` dial to the dead old home (`internal/agent/tunnel.go:935`). Session B's full-register `applyHomeDirectives` records the new directive, sees `rehomeRunning[port]==true`, and `continue`s without spawning. When A's `ApplyHome` returns, the top-of-loop `ctx.Err()!=nil` returns and cleanup hits the `else` branch (`rehomeSeq!=lastSeq && ctx.Err()!=nil`) → `delete(rehomeRunning)`, no restart, want abandoned. `applyHomeDirectives` has one production caller (full register); `RosterRefreshOnly` short-circuits before `homeForRegister` — so a regular expose's reverse tunnel stays pinned to the dead/draining home for the **life of session B**. Self-heals only on the next full register. MAJOR not BLOCKER (`:1227` deletes the flag; no permanent wedge / no leak).
- invariant: D6/C5 epoch-ordered rehome convergence.
- fix: decouple `rehomeWant` from the per-session ctx — leave a respawn marker honored on a fresh ctx, or track per-port loop generation and respawn on mismatch, or run rehome loops off agent-lifetime ctx.

### MAJ-5 · `--require-credential-rotation` raise-failure is a durable-surface false-safe (rejection #5) + misleading guide, exit 0
- file:line: `cmd/tether/cluster_retire.go:62-66` (best-effort raise, WARNING-only), `:65` (hardcoded `true /* alert raised */`), guide text `cmd/tether/cluster_rotation.go:58-60,105-107`.
- proof: After the retire op commits (`cluster_retire.go:50`), a leadership loss before the *separate* `raiseCredRotationAlert` Propose drops the durable `manual:credrot:<node>` row; only a stderr WARNING prints and control returns `nil` → **exit 0**. `computeHealth` never reads `alerts` (`clusterstatus.go:388-481`), so health/`alert ls`/severe banner all read clean — the durable surfaces default to "safe restored," the exact rejection-#5 hazard (`docs/v2-usability-proposals.md:306`). The guide still prints "NOT-SAFE (severe alert manual:credrot:<node>) … `tether alert clear manual:credrot:<node>`" pointing at a non-existent alert. Re-running hits `assertNoActiveOp` and returns before re-raising (`:57-58`). Runtime WARNING + NOT-SAFE banner DO print, and the rejection holds in the common path — so windowed, but the durable-surface false-safe is real.
- invariant: rejection #5 (never default to safety restored; the persistent alert IS the mechanism).
- fix: pass `raised := (aerr==nil)` into the guide; treat a failed raise as a non-zero exit and/or bounded retry/local marker for `--require-credential-rotation`.

### MAJ-6 · C3 topology reconciler fast-path SIGHUP storm + permanent DEGRADED when no http monitor
- file:line: `internal/natsreconcile/reconcile.go:121-134` (unconditional `reload()` when `observedConfirmed==false`); probe `internal/broker/topology_reconcile.go:141-144`; mislabel `internal/broker/clusterstatus.go:429,433-435`.
- proof: The reconciler harvests but never hot-adds the http monitor (`takeover.go:77-81`, cfg leaves `MonitorListen` unset). `install.sh:683-697` writes a no-monitor default conf. A cluster broker with no `http:` (relied on auto-reconcile, or pre-restart after `--manual`) probes `127.0.0.1:8223/varz` → connection-refused → `observedConfirmed` false → `reload()` every `topoReconcileInterval`=5 s **permanently** (~17k SIGHUP/day/broker), and `SwappedReloadPending` matches no STUCK substring → mislabeled `topoBehind` + stays DEGRADED forever. Per-broker-local SIGHUP (no per-tick raft write — bus_nkey backfill is idempotent-guarded), not a cluster-wide raft storm.
- invariant: C3 idle/only-real-transition steady-state side-effect contract.
- fix: track last-reloaded conf mtime and reload only on advance; or back off / cap; or when no monitor exists, return `SwappedReloadPending` without re-signaling.

### MAJ-7 · C5 proxy quorum-loss codes unmapped → exit 70 (internal) instead of 75 (transient)
- file:line: minted at `internal/broker/proxy_cluster_wire.go:40,42,52`; absent from `cmd/tether/error_hints.go:73-96` `brokerCodeExitClasses`/`brokerCodeHints`.
- proof: `proxy_disabled_no_quorum`/`proxy_frozen_readonly` carry the message "…lost the leader); retry" but fall through `brokerCodeExitClass` to the default `exitInternal=70`. A monitor keyed on the documented 75 won't retry a designed self-healing transient; one treating 70 as a tether bug mispages. (`ha_policy_invalid` similarly →70, but ctl-pre-empted.)
- invariant: B2 taxonomy "positively self-healing transients → 75" (`error_hints.go:70-72`).
- fix: map both codes → `exitTransient`, `ha_policy_invalid` → `exitUsage`.

### MAJ-8 · `leaderRedirect` "no leader (election in progress)" → exit 77 (noperm) instead of 75 (transient)
- file:line: `cmd/tether/cluster.go:819-829` (returns true for both wrong-host and election); broker sets `NotLeader, LeaderHost:"", "…retry"` at `internal/broker/clusterstatus.go:575-582`. 13 leader-gated verbs return `errNonLeader=permErr("not leader")` → 77.
- proof: The CLI never branches on the `LeaderHost==""` electing discriminator; a routine failover makes `cluster join approve --wait`, `retire`, `ops confirm/abort`, `seeds publish`, `drain`, `transfer-leader`, `rotate-tunnel-cert`, `alert raise/ack` print "retry" but exit 77 (terminal permission). The *agent*-facing identical condition classes transient (`error_hints.go:77-78`) — operator/agent asymmetry.
- fix: when `LeaderHost==""` return an `exitTransient`-classed sentinel.

### MAJ-9 · `cluster status` F==0 next-step names the C8-deleted `cluster add` + `--join-token` + `<node-pub>`
- file:line: `internal/broker/clusterstatus.go:463` (rendered live at `cmd/tether/cluster.go:458-460`).
- proof: For `proj.FaultTolerance==0` (every N=1 — i.e. the exact state after `cluster init --from-existing` — and N=2) the authoritative `next:` line is "cluster add <node-id> <host:7400> <node-pub>, then re-run with --join-token…". `cluster add`/`<node-pub>`/the nonce dance were deleted in C8 (`TestC8DeletedCommandsGone`; grep `OpClusterAdd` over `cmd/`=empty). It contradicts `init`'s own success output (`cmd/tether/cluster.go:744-748`, correctly `join prepare/approve`) and tells the operator to run `unknown command`. No test locks the string.
- invariant: `docs/v2-cli-consolidation-proposal.md` §6/§7 (hints name only existing commands; no resurrection of deleted surface).
- fix: replace with `cluster join prepare … (new broker)` + `cluster join approve <bundle> --wait (leader)`.

### MAJ-10 · DIFF-1 break: proxysub cluster Plan bakes raw `now` (local zone + monotonic), live mutator binds `now.UTC()`
- file:line: `internal/proxysub/plan.go:49` (`created_at`), `:65` (`revoked_at`) bake `LitTime(now)`; live mutators bind `now.UTC()` (`internal/proxysub/proxysub.go:83,104`); caller feeds `b.cfg.Now()`=`time.Now` (`broker.go:544`).
- proof: Same logical create produces `'…-0500 CDT m=+0.00…'` (cluster) vs `'…+0000 UTC'` (single), violating the `LitTime` contract (`internal/cluster/sqlbake.go:78-84`: caller must apply `.UTC()` iff the live mutator does). proxysub is the **sole** UTC-group anomaly (port/node/agentprov/alert siblings all bake `.UTC()`). Not a BLOCKER — leader bakes the literal once, replicas converge (no FSM divergence); impact is a mixed-zone `ORDER BY created_at` mis-sort across a D9 `--from-existing` cutover boundary (display/ordering only). Escaped review because `plan_test.go:14` seeds a UTC `time.Date`.
- fix: bake `LitTime(now.UTC())` in both sites; add a DIFF-1 test feeding a local-zone monotonic clock.

### MAJ-11 · `OpPortReassignHome` (C6 `last_rehome_at`) is an FSM fail-stop for a 0017-less member; join gate has no schema floor
- file:line: bakes `last_rehome_at` literal `internal/port/plan.go:290-294`; missing-column exec error → `internal/cluster/fsm.go:239` → retry → `panic` `:133-142`; join gate proto-only `internal/broker/clusterstatus.go:768-782`.
- proof: A member on an older same-proto-v2 release predating migration 0017 panics on the first committed `OpPortReassignHome` (missing column → plain error, not `errAppliedRejected`, so no poison-skip). `versionSkewResponse` waves release skew through with a Warn — no migration/schema floor exists (grep across storage/cluster/broker = none). The panic mechanism is the **standing reinstall-not-upgrade invariant** shared by every column-adding migration 0008–0016 (not a C6-introduced class — see screened). The actionable delta: the version-skew gate advertises rolling-release mixing while a column-adding migration makes it unsafe once leadership moves to a newer-release node.
- invariant: enforcement/doc gap — gate promises safety the migration model doesn't deliver.
- fix: gate joins on a schema/migration floor (not just proto), converting runbook discipline to in-band enforcement.

### MAJ-12 · Test-fidelity: three headline acceptances enforced only by token-scans/logic stand-ins + one FALSE coverage comment
- file:line: false comment `internal/natsreconcile/reconcile_test.go:12-15` ("proven against a REAL nats-server…the codified spike") — no such test exists (`test/c3` absent; `c3-review.md` records it deferred). C5 reaper: `driveProxyReconcile`/`reconcileProxySession`/`proxyHomeHealthy`/`homeReachable`/`pickProxyRehomeTarget`/`bumpProxyDwell`/`rehomeProxy`/`reconcileProxyTeardown` have **0** `_test.go` hits each (only token-scans `proxy_cluster_guard_test.go:54`). C6 `buildHomesReport` (`internal/broker/homes.go:15`) is never *called* by any test. C4 resume battery absent (`test/c4` does not exist; 9 plan-§9 tests absent; not in e2e matrix).
- proof: The C3 swap/reload happy path, C5 kill-home auto-switch (建议4 headline, production-wired at `observability.go:246`), C6 `--homes` aggregate, and C4 resume/concurrency would not fail on regression — MAJ-1 ships undetected precisely because of the C4 gap. The `reconcile_test.go` comment is genuine 阳奉阴违 (claims coverage the review doc admits was deferred).
- fix: correct the false comment; deliver the named gated drills (see *Suggested tests*).

---

## MINORs (grouped, all CONFIRMED)

**Exit-code taxonomy** (`cmd/tether`): `reconcile nats --all --wait` timeout → 70 not 75 (`cluster_reconcile.go:95`, lone wait-verb outlier); usage errors via bare `fmt.Errorf` → 70 not 64 (`cluster_seeds.go:37`, `agent_join.go:41,44,68`, `cluster_natsconf.go:24,27,30,89,94,104,243`); `waitForOp` BLOCKED→75 / terminal failed/aborted→70 (`cluster_join.go:162,165-166`); `proxyRequest` no-responders drops `%w` → 70 not 69 (`proxy.go:88`, asymmetric with `:90`).

**Deleted-verb drift / latent live surfaces** (CLI §6): `handleAdd`/`OpClusterAdd` is CLI-unreachable but a fully-functional alternate voter-admission path bypassing the C4 controller (`clusterstatus.go:598-599,696-758`; production backend is non-nil so the `caughtUp==nil` self-refusal isn't taken) — team-accepted latent, identity gate intact leader+replica → MINOR; its hint strings name deleted `sign-join`/`add` (`:705,727,775,778`); `OpClusterDrain` + `Retire:true` still honors the pre-C4 inline retire (`clusterstatus.go:680-683` → `clusterdrain.go:81`); `logger.Warn` names `cluster add` in recovery (`internal/clusteroffline/offline.go:258`, `restore.go:210`); stale comment `error_hints.go:94`; `--node-id` (`cluster_join.go:96`) vs `--self-id` (`cluster.go:760`, `cluster_recovery.go:45`) inconsistency; QUORUM_LOST next-step uses deprecated `cluster force-single` (`clusterstatus.go:404`).

**C1 roster**: spurious `agent_roster_stale` on *any* remove from a stable cluster (derived `lastChangeNano` regresses while the counter jumps, `roster_stale.go:44-49` vs `cluster_roster.go:98-128`); refresh loop dormant after a single→cluster sub-20 s reconnect (`onNATSReconnect` adopts but never starts the loop, `proxy.go:444-480` vs `agent.go:756-758`); non-atomic (membership, counter) two-read snapshot in `buildSignedRoster` (benign, self-healing).

**C2 invite**: redirect SSRF — `CheckRedirect` lacks loopback/RFC1918 guard (`internal/clusterroster/fetch.go:39-44`), blind only; `requireHTTPScheme` accepts plaintext `http://` unconditionally vs dev-gated `validateBootstrapURL` (`fetch.go:72-84`); `ParseInvite` skips `proto.ValidateSID` (`invite.go:93-95`, FS sinks gated downstream).

**C3 topo**: render fixpoint has no test; `topoLaggards` flags `!TopoReported` opt-out VOTERs that `computeHealth` does not (`cluster_reconcile.go:117-118` vs `clusterstatus.go:429`) → `--all --wait` hangs forever; Apply-failure (ENOSPC/EPERM) mislabeled `topoBehind` (`reconcile.go:144` vs `clusterstatus.go:433-435`); "take over manually" dead end (`preflight.go:162-166`); mid-join unbackfilled bus_nkey freezes every broker's observed (accept-as-is fail-closed); unbound `NodeID` in `VerbBusNkeySet` malicious-voter thrash (`cluster_forward.go:584-593`, accept-as-is security-pragmatic).

**C4 op**: dead `confirmed_ft` column (written, never read); `AbortOp` over-promises "reconcile/doctor heals" + `broker_draining` marker never auto-expires (`cluster_operation_controller.go:183-184`, `alert_read.go:96-97`); independent concurrent retires can over-shrink N=3→1 (`controller:573-591`, last-voter guard still holds); no terminal-op GC; stall-classification gaps (MINOR-3/4/5/7/9 — silent JOIN stall, no NATS_ROLLED_OUT deadline, legacy sync surfaces, abort consumes nonce, flapping-error idle-write hole).

**C5 proxy**: missing encoded-`Command` secret-boundary regression test for `OpProxyAllocate`/`OpProxySubCreate`; subscriber `name` passed into `pubAuditCall`'s `nid` positional at two sites (`proxy_cluster_wire.go:102,137`); `homeForRegister` doesn't exclude `__proxy__` → benign dual rehome channel (`home.go:136-139`); leak-once create lost-reply retry returns `sub_name_taken` with an unrecoverable token (`proxy_cluster_wire.go:131-135`); `PlanProxySetEnabled` bumps the keyset epoch on an HA-policy-only change → fleet `keyset_stale` (`session/proxy_plan.go:40-43`); `proxyTunnelUp` lost-update publishes transient `ProxyBound=true` over a just-dropped tunnel (`agent/proxy.go:296-316`).

**C6 observability**: drain/retire rehome events not change-keyed → re-emit every 5 s tick on a stuck retire (`clusterdrain.go:407-413,430-436`); `broker_down_rehome_summary` re-fires if the RAISE keeps failing (`observability.go:163-165`); `--homes` PublicURL (`homes.go:72`, full `Ready`) vs `proxy status --cluster` PublicHost (`proxy.go:410`, `proxyHomeHealthy` alone) gate mismatch; raw `err.Error()` into wire `Errors[]` unguarded for secrets (`homes.go:44,51`); BD10/BD14/HR1 byte-stability/secret-free/view-agreement guards never landed; `proxy_node_ready` event never emitted (documented descope, `c6-plan.md:15`).

**C7 keys**: `derivePublicKey` leaves the raw seed `[]byte` un-zeroed in heap (`cmd/tether/cluster_secrets.go:73-82`, hygiene only, no exfil); structural exfil guard scoped only to `cluster_rotation.go` (`cluster_rotation_guard_test.go:14`); `SecretsPreflight` doc drift — never called anywhere in `cmd/tether` (`cluster_rotation.go:19`).

**C7 false-safe (beyond MAJ-5)**: `cluster status` reads HEALTHY-HA/exit 0 while a severe `manual:credrot` alert is ACTIVE (`computeHealth` never reads `alerts`; asymmetric with `replication_degraded`); no test pins "reconciler PRESERVES a manual alert"; `test/c7/drill_test.go:23-27` is a `t.Skip` stub.

**FSM**: 0016 `proxy_ha_policy` CHECK on a `genericExecApplier` op (`0016_proxy_cluster.sql:18-19`) — honest-path-safe (`ValidProxyHAPolicy` + `MustLitText`), precedent-consistent with 0009 alerts; borderline-informational; diverges from sibling 0015's deliberate no-CHECK choice.

**Wire-proto**: entire C-epic type surface absent from `allRoundtripCases`/`TestUnknownFieldsIgnored`/`goldenFixtures` (`proto_invariants_test.go:40,183`, `golden_test.go:38`); `HomeDirective.CertPins` has `omitempty` on a struct value (no-op → always emits `{}`, `internal/proto/messages.go:177`, cosmetic).

**Auth**: public `/.well-known/tether/cluster.json` reuses the full signed roster (`cluster_manifest.go:80` → `cluster_roster.go:99-110`) disclosing internal `nats_route`/`tunnel_addr`/`cert_fp` the client consumer discards — defense-in-depth (mTLS-gated, no secrets) vs 建议5 脱敏.

**C8 gates / drills**: `--manual` gate verified structurally but not behaviorally through the `recovery`-tree root (`cluster_c8_test.go:144` uses the bare constructor); only `restore` of 5 aliases has a stderr-not-stdout assertion; `test/c4`/`test/c5`/`test/c6` drill stubs absent (only `test/c7`); no C-phase registered in `test/e2e/all_phases_test.go`.

---

## Requirement-fidelity scorecard

**docs/v2-usability-proposals.md (建议1–7 + rejections)**
- 建议1 (auto NATS topology convergence / C3): **partial** — MAJ-6 (reload storm), MAJ-12 (false coverage comment + happy path untested).
- 建议2 (invite/bootstrap-URL join / C2): **PASS** (3 MINOR defense-in-depth only).
- 建议3 (roster auto-refresh ≤5 min / C1): **partial** — refresh loop dormant on sub-20 s single→cluster reconnect (MINOR).
- 建议4 (proxy cluster-ization, kill-home auto-switch / C5): **partial** — MAJ-2 (sub create/revoke broken in cluster), MAJ-3 (rotation black hole, no auto-recovery), MAJ-12 (zero behavioral coverage).
- 建议5 (observability `--homes`/events/脱敏 / C6): **partial** — MAJ-12 (`--homes` never invoked); MINORs (drain events not change-keyed, gate mismatch, `Errors[]` unguarded, `proxy_node_ready` descope); auth MINOR (manifest over-disclosure vs 脱敏).
- 建议6 (status vocab + 5-state health): **partial** — MAJ-9 (status names deleted verb); MINOR (HEALTHY-HA while severe manual alert active).
- 建议7 (compromised-credential rotation / C7): **partial** — MAJ-5 (require-credential-rotation windowed false-safe).

**The 5 rejections (MUST stay refused):**
- #2 NEVER auto-copy CA/account private key: **REFUSED** — full rotation/retire/seed-bundle/join-bundle graph exfil-free (c7-keys, auth, idempotency lenses), structural + real-seed-canary guards pass; only MINOR heap-hygiene (un-zeroed seed slice, no exfil).
- #3 reject sig-fail / gen-rollback (no auto-elect of forged discovery): **REFUSED** — `AdoptDecision` monotone `>=` + pinned-account `VerifyAt`, enforcing tests present.
- #5 retire is not a security boundary / never signal safe: **REFUSED in the common path**, but MAJ-5 is a windowed durable-surface false-safe (raise-failure → exit 0 + clean health/alert/banner) — the one rejection wrinkle; flag for external elevation.
- (Remaining rejection items — never `--yes` on force-single, no theoretical-attack complexity — verified refused: `cluster_machineconfirm_test.go`, gate-preservation lens.) test-adequacy lens confirms all 5 carry real enforcing tests.

**docs/v2-cli-consolidation-proposal.md (§6 discipline):** **partial** — MAJ-9 (status names deleted `cluster add`); MINORs (latent live `handleAdd`/`OpClusterAdd` + `OpClusterDrain Retire:true`, deleted-verb hint strings). CLI *surface* genuinely consolidated (5 relocated escapes = hidden alias + visible child sharing one constructor; deleted constructors absent; gates byte-preserved; deprecation→stderr) — the residue is broker-layer dead-code + stale strings, not a live user-facing dual surface.

**docs/v2-usability-proposals-gap.md / v2-automation-program.md:** decomposition implemented end-to-end (every op proposed, every loop production-gated, every CLI surface registered — deadcode-wiring PASS); fidelity gaps are the test-coverage MAJ-12 and the `proxy_node_ready` alias row (should be recorded as alias+doc-mapping, not ✅-unqualified).

---

## Coverage map (22 lenses)

**Ran clean — PASS-FOR-LENS, MINOR/screened only (9):** c2-invite-bootstrap, c5-proxy-psk-security, c7-rejection-2-keys, c8-gate-preservation, fsm-determinism, idempotency-dedup, wire-proto-compat, auth-acl-security, deadcode-wiring.

**MINOR-only (2):** c1-roster-monotonicity, c8-dual-surface.

**Found confirmed MAJOR (11):** c3-topology-reconciler (MAJ-6), c4-operation-resume (MAJ-1 + C4-resume-battery in MAJ-12), c5-proxy-reaper-epochs (MAJ-3), c6-observability-stability (MAJ-11), c7-rejection-5-falsesafe (MAJ-5), raft-write-path (MAJ-2), concurrency-race-leak (MAJ-4), sql-migrations (MAJ-10), error-exit-codes (MAJ-7, MAJ-8), cli-ux-consistency (MAJ-9), test-adequacy-fidelity (MAJ-12).

Core security/correctness invariants (PSK-in-raft, FSM determinism/poison-safety, single-WAL-owner, byte-equivalence, session ACL, proto SSOT) independently re-verified clean across the relevant lenses.

---

## False-positives screened (notable)

- **c8-dual-surface "MAJOR §6.1 violation" (handleAdd):** OVERSTATED → MINOR. Identity gate intact leader-side (`PlanClusterNodeUpsert` re-verifies PoP) and replica-side (poison-skip), no two-writer hole (`assertNoActiveOp`), owner-only 0600 socket, team-accepted latent (`cluster_c8_hints_test.go:13`).
- **c6 last_rehome_at as a *C6-introduced* FSM defect:** the panic-on-missing-column is the standing reinstall-not-upgrade invariant shared by migrations 0008–0016, not a new class — kept only as the enforcement-gap framing (MAJ-11), not a fresh panic surface.
- **c1 Finding 3 (counter==0 + remove-first + backward clock skew → gen_rollback):** effectively unreachable non-bug (needs NTP-scale backward skew exceeding inter-stamp gap inside the pre-first-op window); self-heals on the next forward-clock op.
- **c5 "stale read drops a good token" on create read-back:** REFUTED — `Node.Propose`/`Apply` is commit-synchronous under `applyMu`, so the read-back sees committed WAL state.
- **c3 reload-storm "cluster-wide raft/write storm":** OVERSTATED — per-broker-local SIGHUP; bus_nkey backfill is idempotent-guarded, no per-tick raft write (severity still MAJOR-operational, MAJ-6).
- **test-adequacy MINOR-2 (`reconnects == epoch` aliasing):** OVERSTATED — already documented at `internal/adminsock/protocol.go:366`.

---

## Suggested tests (named)

- **C4 (MAJ-1 + battery):** `TestC4AbortDrainRefusesActiveOp`; `TestC4JoinResumeEachState`, `TestC4RetireResumeEachState`, `TestC4RetireGatesReRunOnResume`, `TestC4TopoConvergedFailClosed`, `TestC4OpFSMDeterminism`, `TestC4OpsAbortFreesSlot`, `TestC4NonceReplayAfterRetire`, `TestC4OpsLsSchemaStable`, `TestC4ProductionWiresNoController`; register gated `test/c4` + add to e2e matrix.
- **raft-write-path (MAJ-2):** `TestC5ProxySubCreateOnFollowerRouted` — issue `proxy sub create` against a node connected to a follower, assert non-timeout `ok` + leak-once URL (gated multi-broker `test/c5`).
- **C5 reaper (MAJ-3, MAJ-12):** `TestC5RotationRebuildsServingNode` (serve at keyset E, deliver a full directive on a NEW port at the SAME E with `p.srv!=nil`, assert rebuild not false re-ACK); gated `test/c5` `KillHomeRehome` / `RehomeFlapHysteresis` / `KeysetRotateUnderPartition` / `RehomeCASSingleWinner`; unit-test `pickProxyRehomeTarget`/`proxyHomeHealthy`/dwell.
- **concurrency (MAJ-4):** `TestRehomeWantAppliedAfterRebuildCancelsCtx` — block `ApplyHome`, cancel its ctx, deliver a higher-epoch want on a new ctx, assert eventual application.
- **C7 (MAJ-5):** `TestRequireCredRotationRaiseFailureNonZeroExitAndGuideRaisedFalse`; `TestReconcilePreservesManualAlert` (seed ACTIVE `manual:credrot`, assert stays ACTIVE + zero clear proposals); un-skip the `test/c7` temporal-monotone status drill.
- **C3 (MAJ-6, MAJ-12):** correct the false comment at `reconcile_test.go:12-15`; `TestReconcilerRenderFixpoint` (`BuildMergedConf∘Preflight∘BuildMergedConf == BuildMergedConf`); deliver the deferred `test/c3` reload smoke (seam-injected `reconcileTopologyOnce` or subprocess).
- **C6 (MAJ-12 + MINORs):** `TestHomesProxyReasonMatchesProxyStatus`, `TestHomesReportErrorsSecretFree`, `TestHomesPublicURLEmptyWhenHomeDown`, `TestHomesTableShowsNameAndPort`; `TestRehomeEventsNeverLeakSecret`; `TestJSONSchemaStableHealthLabel` / `TestExitCodeContractUnchangedAcross5States`.
- **C5 secret-boundary (MINOR):** encoded-`Command` byte-scan for `OpProxyAllocate` (`!Contains(enc, alloc.Token)` + `Contains(enc, tokenHash)`) and `OpProxySubCreate` (`!Contains(enc, rawToken)` + `Contains(enc, psk)`).
- **sql-migrations (MAJ-10):** DIFF-1 test feeding a local-zone monotonic `time.Now()`, assert cluster-baked `created_at`/`revoked_at` literal == single-mode-bound string.
- **wire-proto (MINOR):** `roundtripCase` + golden fixture per new C type (≥ one `ProxyDirective` with `Home`, one `ClusterHealthResp` with `Topo*`).
- **error-exit (MAJ-7/8):** classification tests asserting proxy quorum-loss codes → 75 and leader-election (`LeaderHost==""`) → 75.
- **cli-ux (MAJ-9):** unit test scanning every `computeHealth` NextStep/Banner string for command tokens absent from the live cobra tree.
- **C1 (MINOR):** integration test exercising the `readRosterBrokers`-after-remove stale path; assert single-snapshot (membership, counter) read.
- **C8 (MINOR):** table-drive the deprecation-on-stderr assertion over all 5 aliases; drive the `--manual` and never-escapable gates through the `recovery`-tree root.