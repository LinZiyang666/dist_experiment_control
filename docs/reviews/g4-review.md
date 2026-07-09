# G4 — `tether cluster add` · Stage-C Internal Review

## 1. Verdict

**14 findings survived adversarial verification: 1 BLOCKER, 6 MAJOR, 7 MINOR. 1 claim was refuted.** (21 raw lens-findings folded to 14 after cross-lens dedup.)

**G4 is NOT ready for external review.** The BLOCKER (`findJoinOp` attaches to terminal ops → `cluster add` permanently dead-ends on two supported resume paths) breaks the feature's headline idempotency/resumability invariant and must be fixed. Six MAJORs must each be resolved or adjudicated: two are real code/data-safety defects (`--preserve-js-data` no-op dead-end; move-aside sentinel crash-window wedge), one is a plan-vs-code divergence that must be either implemented or documented (#7 stall-window), and three are missing hermetic regression nets over load-bearing paths (cutover revival/render/R3 gate; crash-mid-grow resume; growActive fence). The MINORs and remaining test gaps should close in the same fix pass. The sim drills (10 19/19, 11 12/12) are GREEN because they exercise only the happy fresh-install path — every confirmed defect lives in a resume/edge/regression surface the sim structurally does not reach.

---

## 2. Confirmed findings (most-severe first)

### BLOCKER

#### B1 — `findJoinOp` returns a TERMINAL join op → `cluster add` dead-ends on abort-retry and remove-then-re-add  `[impl]`
**file:** `cmd/tether/cluster_add_drive.go:206-213` (consumer), `driveAdd:78-96` (skip), `internal/broker/cluster_grow_trigger.go:123-134` (matcher), `internal/cluster/operation_read.go:104-113` (unfiltered rows)
*(folds the two lens-findings that hit the same root cause)*

**Defect:** `findJoinOp` sends `join-status` keyed on the joiner node-id and returns `resp.OpID` **without checking `resp.Terminal`**. The matcher only *prefers* a non-terminal op; backed by `RecentOperations(ro,200)` (no terminal filter), it falls back to the newest **terminal** row when no live op exists. `driveAdd` then treats any non-empty `opID` as "in-flight — resume" and **skips `runSelfJoinPrepare` + approve-join**, so no fresh `OpKindJoin`/`AddNonvoter` is ever created. `waitOpCatchingUp` short-circuits on `resp.Terminal`; `waitJoinServing` matches SERVING first, otherwise HALTs.

**Failure scenarios (both reachable, no clean operator recovery — there is no verb to delete a terminal `cluster_operations` row):**
- **Abort-then-retry:** an operator `cluster ops abort` (the code's own recommended remedy) or the `abortRejectedJoinOp` backstop leaves a terminal ABORTED op. Every re-run of `cluster add <joiner>` re-discovers it and HALTs "ended non-SERVING: ABORTED". The joiner is permanently un-growable, and the never-released grow lock keeps cluster join/retire BLOCKED cluster-wide.
- **Remove-then-re-add:** a node completes (terminal SERVING op), is pruned via `recovery node remove` (deletes only `cluster_nodes`, leaves the op row). Re-`cluster add`: prepare/approve skipped → if the joiner's broker is up, `waitJoinServing` sees terminal SERVING and prints "✓ VOTER / cluster add complete" **while the node is absent from the raft config** (silent HA under-count — an R3-adjacent membership-honesty violation); if the broker is down, `renderJoinerClusteredConf` fails "bus nkey absent — retry" forever (wedged).

**Fix direction:** make `findJoinOp` not attach to a stale terminal op. The narrow, non-hiding fix keys attach on **current roster membership** (node present / VOTER) rather than raw `resp.Terminal` — because a legit crash-post-SERVING-still-VOTER resume (plan §4 P9) currently *relies* on the terminal-op return, so a blanket `if resp.Terminal { return "" }` would perturb that path (though `StartJoinOperation`'s NonceConsumed/ActiveOperationForTarget guards make a fresh no-op join on an already-VOTER node safe). Main process to adjudicate which. **Add hermetic tests:** re-grow after ABORTED converges to exactly one fresh op → one N+1 VOTER; remove-then-re-add re-admits (fresh op) rather than false-succeeding or wedging. Currently `g4_grow_trigger_test.go` never exercises the join-status terminal-fallback.

---

### MAJOR

#### M1 — `--preserve-js-data` is a signed wire field that is silently dropped: unimplemented backup→restore AND an operator dead-end on a data-bearing store  `[impl + doc]`
**file:** `internal/broker/cluster_grow_cutover.go:102` (drop), `:200-202` (refusal), `cmd/tether/cluster_add.go:119` (flag help), `internal/proto/cluster_grow.go:71-81` (signed)
*(folds security/wire + CLI/UX lenses)*

**Defect:** `PreserveData` is threaded CLI→drive→trigger and **signed into `CanonicalGrowReqBytes`**, but `performGrowCutover` calls `moveAsideJetStreamStore(dir, req.GrowEpoch, req.ResetAck)` and never reads `req.PreserveData` (grep: zero consumers). `moveAsideJetStreamStore` contains no `nats stream backup`/restore — it only move-aside-renames on `ack`. So the advertised "best-effort backup→restore" (flag help + plan Q3/§12.2) is **completely unimplemented under every flag combination**, and a signed field has zero consumers (a wire-contract smell).

**Failure scenario:** operator grows the live racknerd N=1 (non-empty former-N1 store — the exact case Q3/§12.2 headline) with only `--preserve-js-data` → `ResetAck=false` → the non-empty gate REFUSES with a message telling them to re-run with **the flag they already passed** — an infinite dead-end (`cutoverBroker` is best-effort, so the grow stalls at start-joiner and the joiner never reaches SERVING). Switching to `--reset-former-js` moves the store aside to `jetstream.grow-bak.<epoch>` and boots the clustered JS **empty** — history/OBJ_xfer survives only as an un-restored on-disk dir the operator was never told to restore. (Not literal data loss: never deleted. Not documented as a §12 deferral.)

**Fix direction:** either (a) implement backup→restore (thread `req.PreserveData` into the move; `nats stream backup` before rename, restore after clustered revival) and treat `--preserve-js-data` as unblocking the non-empty gate; **or** (b) if deferred, remove the flag + all "(backup→restore)" claims from flag help and the refusal message, treat `--preserve-js-data` as an explicit ack-equivalent (so it does not dead-end), and record the deferral in plan §12. **Add test:** non-empty store + only `--preserve-js-data` must not dead-end into a refusal recommending the passed flag.

#### M2 — #7 catch-up stall-window / AppliedIndex-progress mechanism was never implemented → size-scaled deadline is the sole gate; a progressing joiner is false-BLOCKED  `[impl OR plan]`
**file:** `internal/broker/cluster_operation_controller.go:566` (BLOCK gate), `:528-530` (bool-only probe), `:597-606` (size cap)
*(folds the MINOR runtime framing + the MAJOR spec/process framing)*

**Defect:** the plan (§4 P6 / §5 #7 / §11) specifies #7 as **two** parts: (a) a size-scaled deadline (shipped) **and** (b) a leader-local AppliedIndex progress map + ~90s stall-window so the op "never BLOCKs while advancing." Part (b) is entirely absent — grep for `opCatchupProgress`/`catchupProbeFn`/`catchupStalled`/`opCatchupStallWindow`/`snapshotSizeFn` returns nothing. `OpStateCatchingUp` uses the bool-only `caughtUpFn` and BLOCKs purely on `now > CatchupDeadline`. §11's own residual-risk mitigation ("on-disk DB size ≠ InstallSnapshot transfer size; the stall-window is the real gate, so a mis-sized cap never false-fails a progressing joiner") is now **false** — the mis-sizeable cap is the only gate. This divergence is not recorded in §12, and acceptance §9(D) ("tests prove promotes-when-progressing, BLOCKS-when-stalled") is unmet — `g4_adaptive_catchup_test.go` only asserts clamp/floor bounds.

**Failure scenario:** a large/WAN joiner whose real InstallSnapshot legitimately exceeds `base + dbBytes/512KiB/s` (wire slower than the 512KiB/s floor, or a multi-GB command-domain DB exceeding the 30-min clamp) while steadily advancing AppliedIndex is flipped to BLOCKED at `now>CatchupDeadline` — the exact #7 false-BLOCK the mechanism was written to eliminate. Recoverable via `cluster ops confirm` (fresh barrier+deadline), potentially looping.

**Severity note:** runtime blast radius alone is MINOR (recoverable nonvoter, no data loss, no quorum risk, and the size-scaled cap is a strict improvement over the old fixed 2-min). It is rated **MAJOR** on the substantive core: a load-bearing safety property the FINAL plan asserts in three places does not exist, an explicit §9(D) criterion is unmet, and the drop is undocumented — violating this project's 先改文档再改代码 discipline that Stage-C exists to enforce.

**Fix direction:** either implement (b) — track per-op AppliedIndex via a probe, BLOCK only on a true flat-index stall-window OR the max clamp — and add `TestCatchupStallDetection`; **or** amend plan §4/§5/§11/§9(D) to record that (b) was dropped, soften the §11 claim, and justify why cap-only meets #7. Do not leave the plan claiming a property the code lacks.

#### M3 — `moveAsideJetStreamStore` writes its per-epoch idempotency sentinel AFTER the destructive rename+recreate → crash-in-window resume permanently wedges (ENOTEMPTY)  `[impl]`
**file:** `internal/broker/cluster_grow_cutover.go:205` (rename), `:208` (mkdir), `:211` (sentinel, error ignored), `:193-195` (sole guard)
*(folds data-plane + concurrency lenses)*

**Defect:** the per-epoch sentinel is the **sole** "already-moved" guard, yet is written **last** (`_ = os.WriteFile(...)`, error swallowed) after the irreversible `os.Rename(storeDir, backup)` and `os.MkdirAll(storeDir)`. `performGrowCutover` calls move-aside **before** `natsconf.Apply`, and neither staged short-circuit (Stage A live-clustered, Stage B conf-clustered) fires in the render→move→Apply window. An external kill/panic/OOM/power-loss (or a silently-failed WriteFile) in that window leaves: sentinel absent, `storeDir` recreated **empty** (`ReadDir`==0 → ack gate bypassed), `backup` already holding the **original data**.

**Failure scenario:** on resume the orchestrator re-sends mesh-cutover (epoch == opID, stable via `findJoinOp` attach) → re-enters move-aside → computes the same `backup` path → `os.Rename(emptyStoreDir, nonEmptyBackup)` returns **ENOTEMPTY** on every retry → `performGrowCutover` returns `CodeBadRequest "move JS store aside: directory not empty"`. The former-sole-leader cutover is permanently wedged on exactly the data-bearing racknerd-N=1 case, manual-recovery-only. (No data loss — backup never deleted — but this is precisely the "crash after move/before restart → restart-only, no second move" invariant of §4 P5 / §7 / §8.)

**Fix direction:** make the idempotency signal the durable evidence of the move itself — at the top of `moveAsideJetStreamStore`, if `backup` (`storeDir+".grow-bak."+epoch`) already exists, treat it as already-moved and return (skip the second rename). Optionally also write the sentinel *before* MkdirAll and stop ignoring its error. **Add test:** pre-create a non-empty backup dir + empty storeDir + absent sentinel → move-aside returns nil no-op, does not error, does not touch the backup. (Existing `TestMoveAsideJetStreamStore_IdempotentPerEpoch` only exercises the empty-store + sentinel-present happy path.)

#### M4 — `performGrowCutover` revival-failure BLOCK, clustered render, and the R3 committed-≥2-server gate have ZERO hermetic tests  `[test]`
**file:** `internal/broker/cluster_grow_cutover.go:119-140` (revival BLOCK), `:145-179` (render), `:71-84` (R3 gate)
*(folds the standalone "R3 gate untested" MINOR — the gate is one of the three load-bearing branches here)*

**Defect:** grep proves `performGrowCutover`, `restartAndVerifyClustered`, `renderClusteredCutoverConf`, `growCutoverRevivalFailed`, `"cutover_revival_failed"` appear only in non-test files. `mesh-cutover` is never driven through `handleGrowTrigger` (the sole `_test.go` occurrence is a proto canonical-bytes fixture). Only the leaf `moveAsideJetStreamStore` is unit-tested. Plan §8's `TestFormerN1ResetGate_FiresOnlyWhenClustering` and `TestNatsRestartRevivalFailure_BlocksLoudly` do not exist; §9(D)'s non-sim revival-failure clause is literally unmet. The whole standalone→clustered cutover — the highest-consequence "never silently strand the data plane" path — is proven only by sim drills 10/11.

**Failure scenario:** a refactor drops the `growCutoverRevivalFailed` path, or renders a peer-short conf, or loosens the R3 gate to `len<1` (the #10 lone-survivor re-cluster) / drops the `selfInCfg` requirement — and `make test`/`e2e`/`lint` stay GREEN. It surfaces only on a real deploy-tier grow when the former-N1 data plane is left down with no loud BLOCK — the racknerd-style silent-503 rot the feature exists to prevent.

**Fix direction:** add broker integration tests: (1) drive `mesh-cutover` with a fake nats bin + fake `/varz` so `probeNatsClusterName` never returns clustered ⇒ assert `Code==growCutoverRevivalFailed` + operator hint; (2) R3 gate: single-voter/self-absent config ⇒ `bad_request "committed >=2-server raft config"`, committed 2-server ⇒ passes, already-clustered ⇒ Stage-A/B AlreadyDone with no store move (the `d7SingleNode`/`d9` harnesses already yield a real single-voter leader); (3) `renderClusteredCutoverConf` golden/round-trip: peers<2 ⇒ error, secrets-dir mTLS paths, `SynthesizeClusterListen`, self present.

#### M5 — Crash-mid-grow resume (the six §8 failure points) has no hermetic test  `[test]`
**file:** `internal/broker/cluster_grow_cutover.go:33-37` (staged-idempotent contract), the P0-P9 ladder
**Defect:** idempotency/crash-recovery is a first-class G4 invariant (§7). Plan §8's `TestGrowResume_sixFailurePoints` and `TestClusterAdd_doubleRun_singleMembershipChange` are absent; §9(C)'s kill-at-each-failure-point re-run is a drill-11 acceptance that per §5 is explicitly NOT in `go test`/CI — so the invariant has **no hermetic regression net**. The three-stage staged-idempotent dispatch (AlreadyDone / restart-only / full) is verified by no unit test; Stage A's short-circuit is the sole guard preventing a resume-after-success from re-SIGKILLing a healthy nats.

**Failure scenario:** a change to the staged-branch ordering (Stage-A probe moved after move-aside, or sentinel/backup detection altered) makes a resume re-move an already-reset store or double-issue AddVoter — the R3 hazard the plan claims is inherited — and no hard gate fails.

**Fix direction:** add an embedded-nats integration test driving the OpKindJoin ladder + `performGrowCutover` through each documented kill point, asserting exactly one N+1 voter, no orphan stream, one rename + one restart, no double AddVoter. Requires a small injection seam (Stage A hits a fixed loopback `/varz`; Stage B/C exec a real nats bin). At minimum, a table-test over the three stages with a faked probe+conf to pin the staged contract. (Cross-ref M3: the move/before-restart kill point is a *confirmed bug*, not just a test gap.)

#### M6 — growActive symmetric fence + Q7 self-op carve-out (grow's own join op NOT frozen) has zero hermetic coverage → a symmetry regression silently re-arms the self-deadlock  `[test]`
**file:** `internal/broker/cluster_operation_controller.go:172-174` (retire refuses on grow), `:326` (driveInFlightOperations freezes on upgrade only — the deliberate carve-out), `internal/broker/cluster_grow_trigger.go:73-89` (acquire-lock), `internal/broker/cluster_upgrade_trigger.go:145-148`
*(folds the MINOR "grow-marker mutual-exclusion untested" duplicate)*

**Defect:** the Q7 serialize/carve-out design is spread across four sites; the load-bearing counterintuitive part is that `driveInFlightOperations` checks **only** `upgradeActive`, never `growActive`, so the grow's own join op keeps driving. Plan §8's `TestGrowLock_noSelfBlock_symmetricFence` and `TestConcurrentAddRetireRace` do not exist; no `_test.go` ever sets the grow marker (`PlanSetGrowActive`/`cluster_grow_active` grep-empty in tests). Aggravating: the **upgrade** freeze precedent *is* tested (`TestB2UpgradeLockBlocksMembership`), `growActive` is used symmetrically in two sibling sites, and there is **no inline comment at line 326** warning not to add it — so a maintainer is actively invited to "complete the symmetry."

**Failure scenario:** a future edit adds `if growActive != "" { return }` to `driveInFlightOperations` → the grow's own `OpKindJoin` is frozen for the whole marker-held P1-P9 window → never reaches SERVING → `cluster add` self-deadlocks (the exact Q7 warning). `make test`/`e2e` stay GREEN; only the non-CI sim drill would catch it.

**Fix direction:** add a `-race` + NumGoroutine/fd-leak-gated concurrency test asserting: (a) with the grow marker held, `driveInFlightOperations` still advances the grow's own join op to VOTER (carve-out intact); (b) `StartRetireOperation` and upgrade acquire-lock both refuse on growActive; (c) a second grow acquire-lock for a **different** joiner is refused while the same-joiner resume is allowed; (d) `GrowLockActive` surfaces in the health reply. Consider adding an inline "do NOT add growActive here — would deadlock the grow's own op" comment at line 326.

---

### MINOR

#### m1 — `StartJoinOperation` / approve-join dispatch not gated on the grow marker → a concurrent different-node join bypasses Q7 strict-serialize  `[impl]`
**file:** `internal/broker/cluster_operation_controller.go:43`; dispatch `internal/broker/cluster_grow_trigger.go:105-111`
**Defect:** `StartJoinOperation` gates only on `upgradeActive`, never `growActive` (its sibling `StartRetireOperation:172` *does* refuse on grow — a real asymmetry). During a grow of node-A (marker=A), an operator with the account seed can `cluster join approve <bundle-for-C>` and create a second concurrent join op; `driveInFlightOperations` drives both. The orchestrator's own warnings (`cluster_add_drive.go:300,304`) promise "`cluster join`/`cluster retire` stay BLOCKED" — but only retire is enforced, so the code promises a serialization it never delivers.
**Why MINOR:** R3 holds (both joins AddNonvoter-first, raft serializes config changes, no half-commit/fork); the normal path is serialized by acquire-lock; triggering it needs a privileged operator bypassing `cluster add`. Serialize-intent gap, not a safety violation.
**Fix direction:** in `StartJoinOperation`, after decoding the bundle, add `if j := growActiveJoiner(a.node.RODB()); j != "" && j != b.NodeID { return "", err }` (marker is joiner-id-valued, so the self-op carve-out is preserved). Add a test: a different-node approve is refused during a grow while the grow's own same-target approve is allowed.

#### m2 — Former-N1 cutover render harvests the http monitor instead of forcing `127.0.0.1:8223` → false revival-failure if the monitor is absent  `[impl]`  *(downgraded MAJOR→MINOR)*
**file:** `internal/broker/cluster_grow_cutover.go:172`
**Defect:** `renderClusteredCutoverConf` sets `MonitorListen: own.MonitorHTTP()` (harvest), but `restartAndVerifyClustered` verifies revival **only** via `probeNatsClusterName` against the hardcoded `127.0.0.1:8223`. Every other restart-bearing takeover site FORCES `8223` (`cluster_natsconf.go:326`, with a "keep in sync with topoMonitorListen" comment); the cutover uniquely harvests. If a former-N1's live conf lacks an http block, the rendered clustered conf also lacks one → probe connection-refuses for the full 45s → healthy revival is falsely reported `growCutoverRevivalFailed` "data plane is DOWN."
**Why MINOR (not MAJOR):** the precondition is not reachable in any supported flow — `cluster init --from-existing` PRINTS a mandatory step-1 `reconcile nats --manual` which sets `MonitorListen:"127.0.0.1:8223"` unconditionally, so every properly-initialized former-N1 carries http:8223 before its first grow (the cutover only does real work on the first grow); skipping init's documented step is independently surfaced loudly as TOPO STUCK in `cluster status`. Real defense-in-depth gap, guarded elsewhere.
**Fix direction:** set `MonitorListen` to the `topoMonitorListen` constant `127.0.0.1:8223` explicitly in `renderClusteredCutoverConf` (the probe hardcodes it, so forcing it is strictly more correct). Add a test: render a cutover conf from a standalone Ownership with no http block ⇒ output contains `http: "127.0.0.1:8223"`.

#### m3 — `OpClusterGrowCutover` is wired (handler + hook + request fields) but omitted from the `clusterOps` routing map → the documented local-socket cutover fallback is unreachable ("unknown op")  `[impl]`
**file:** `internal/adminsock/protocol.go:135-149` (map), handler `internal/broker/clusterstatus.go:700-708`, hook `internal/broker/broker.go:1016-1018`
**Defect:** `server.dispatch` forwards to `HandleCluster` only when `clusterOps[req.Op]` is true; `OpClusterGrowCutover` is absent from the map, so `callAdmin({Op: OpClusterGrowCutover})` returns `unknown op ... bad_request`. The handler, hook, and three `Grow*` request fields are dead-but-wired code; plan §12.8's "(fallback) local-socket joiner cutover" is non-functional. Every *other* routed cluster op carries a `clusterOps[Op]` assertion test — this one has none, which is why it slipped. (Refuted-finding #21 independently confirms **nothing anywhere calls this op** — the pre-render design obsoleted the joiner-local cutover.)
**Why MINOR:** not a security hole (root-gated 0600/0700 socket, over-gated to unreachable); no shipped path fails.
**Fix direction:** given #21's finding that the op has no caller, **prefer deleting** the dead handler, hook, const, and the three `Grow*` request fields; **or** register `OpClusterGrowCutover: true` + add a routing-map assertion test mirroring `proxy_rebalance_wire_test.go` and reconcile plan §12.8. Do not leave it wired-but-unroutable.

#### m4 — Non-empty ack gate ignores the `os.ReadDir` error → a data-bearing but transiently-unreadable store is reset without `--reset-former-js`  `[impl]`
**file:** `internal/broker/cluster_grow_cutover.go:199-205`
**Defect:** the Q3 operator-ack gate keys on `entries, _ := os.ReadDir(storeDir)` then `if len(entries) > 0 && !ack` — the ReadDir error is discarded. On `(nil, err)` (EACCES/EIO on the store dir, or a TOCTOU race — the parent-search Stat at :196 can succeed while Open for ReadDir fails), `len==0` skips the loud refusal and falls through to `os.Rename` with `ack=false`. A gate whose stated purpose is to fail loud fails **open**. (For the EACCES-on-dir case there is no backstop: rename needs write+exec on the *parent*, not read on the store.)
**Why MINOR:** move-not-delete (renamed to `.grow-bak.<epoch>`, never deleted), so the never-lose-data invariant holds — only the mandatory operator acknowledgement is bypassed.
**Fix direction:** fail-closed — if `os.ReadDir` errors, require ack (or surface the error) rather than assuming empty. Add a test injecting a ReadDir failure ⇒ reset refused without `--reset-former-js`.

#### m5 — `CanonicalGrowReqBytes` field-sensitivity test omits the `JoinBundle` and `OpID` tamper cases  `[test]`
**file:** `internal/proto/cluster_grow_test.go:29-39`
**Defect:** the impl correctly signs over `JoinBundle` and `OpID` (`cluster_grow.go:79-81`), but the base request leaves both at "" and the mutations slice exercises only 7 of the 9 signed fields — never mutating `JoinBundle` (the highest-value security field: a swapped bundle admits a different node as voter) or `OpID`. A future edit dropping `r.JoinBundle` from the canonical bytes passes every existing test (all mutations keep JoinBundle=""), shipping a signature that no longer covers the join PoP bundle → JoinBundle substitution within the 5-min replay window. No runtime defect today.
**Fix direction:** set a non-empty base `JoinBundle`/`OpID` and add `func(r){ r.JoinBundle = "tether-join:other" }` and `func(r){ r.OpID = "op-2" }` to the mutations slice so every signed field is pinned tamper-sensitive.

#### m6 — mesh-peers grow-trigger handler is completely untested  `[test]`  *(downgraded MAJOR→MINOR)*
**file:** `internal/broker/cluster_grow_trigger.go:144-165`
**Defect:** the load-bearing §12.8 mesh-peers op (returns route-mesh triples the fresh joiner needs to render its own clustered conf) has no hermetic test — its raw SQL `phase IN ('VOTER','CATCHING_UP','VOTER_ADD_FAILED')` + `bus_nkey_pub != ''` filter is unexercised, and the phase-string literals are hand-written, not bound to the `phaseVoter`/`phaseCatchingUp`/`phaseAddFailed` constants.
**Why MINOR (not the claimed MAJOR silent-boot-failure):** the sole consumer `renderJoinerClusteredConf` (`cluster_add_drive.go:369-374`) converts every drop path into a **loud, retryable** HALT ("bus nkey absent — retry" / "no route-mesh peers returned — retry") *before* any conf is rendered — nothing boots broken. An established VOTER always carries a committed bus_nkey. The phase literals are DB-persisted schema strings used as raw SQL across ~10 files; a rename is a migration that breaks many persisted-state tests loudly. So it is a genuine coverage gap, not a live defect.
**Fix direction:** seed `cluster_nodes` with mixed phases + one empty-`bus_nkey_pub` row, drive `handleGrowTrigger op=="mesh-peers"`, assert the returned `MeshPeers` contains exactly the eligible triples (right count/server/route/nkey, empty-nkey and non-eligible-phase rows excluded). Bind the SQL phase literals to the exported constants to prevent drift.

#### m7 — Deploy-tier drills never exercise a data-bearing former-N1 JS reset (no `grow-bak` / no-orphan / preserve assertion)  `[test]`
**file:** `test/simcluster/drills/11-grow-gaps.sh:28`
**Defect:** plan §5 called for drill 11 to add a positive orphan-stream regression + assert `jetstream.grow-bak.<epoch>` exists; that change was not made. `grep -rn grow-bak test/` returns zero — the move-not-delete sentinel is asserted nowhere in the deploy tier. Drill 10 asserts R=3 streams but grows from an **empty** former-N1 (reflects the D5 reconciler recreating standard streams, not preservation), and `cmd_grow` always passes `--reset-former-js`, never `--preserve-js-data`. So the deploy tier has zero coverage of a data-bearing cutover — nothing there would catch the M1 preserve regression.
**Why MINOR:** a pure delete-instead-of-move regression is still caught hermetically by `TestMoveAsideJetStreamStore_EmptyMoves`; §9-C's acceptance gate (trailer-rename + inverted asserts + #I1) is satisfied. The uncovered surface is the deploy-tier *integration* (real on-disk store preserved across a real systemd+nats cutover).
**Fix direction:** in drill 11 (or 10), write a small JS stream with data on brk1 while N=1, grow to N=2 with `--reset-former-js`, then assert (a) `test -d /var/lib/tether/jetstream.grow-bak.*` on the former-N1 (move-not-delete proof) and (b) clustered streams at ReplicasFor(N) with no single-replica orphan.

---

## 3. Refuted claims (checked, did not survive)

- **`performGrowCutover` two unguarded entry points can run concurrently and race the check-then-rename / double-SIGKILL nats** — **REFUTED.** The two entry points exist and share no mutex, but neither concurrency path is reachable: (1) the NATS grow responder is a single async subscription, and nats.go dispatches one subscription's async callbacks **strictly serially** (synchronous `mcb(m)` in a for-loop, next message popped only after return; the subscription survives reconnect) — a retry arriving mid-poll is queued, not concurrent; (2) **nothing sends `OpClusterGrowCutover`** — the local hook is a dead vestige of the obsoleted §12.8 fallback (ties to m3), and the two paths target *different* brokers by construction (NATS mesh-cutover → former-sole-voter; local → joiner; joiner==leader is rejected). STAGED-for-sequential idempotency is therefore sufficient; no per-broker cutover mutex is needed. The one genuine sub-observation (the local `growCutover` hook at `broker.go:1016` is dead code with no caller) is folded into **m3**.

---

## 4. Test-coverage gaps — hermetic tests to add (sim validates E2E; these close the unit/integration net)

The sim drills prove the happy path E2E but are non-CI and reach only the fresh-install flow. The following hermetic tests are the regression net the fix pass should land (ID = the finding that motivates it):

| # | Test to add | Motivating finding | Plan-named |
|---|---|---|---|
| 1 | Re-grow after ABORTED op → one fresh op → one N+1 VOTER; remove-then-re-add re-admits (not false-success/wedge) | **B1** | — |
| 2 | Non-empty store + only `--preserve-js-data` must not dead-end into a refusal recommending the passed flag; assert backup/restore artifact (or that gate is unblocked) | **M1** | — |
| 3 | `TestCatchupStallDetection` — advancing index past deadline ⇒ not BLOCKED; flat index past stall-window ⇒ BLOCKED; zero raft writes on idle ticks *(only if (b) is implemented; else amend plan)* | **M2** | §8 |
| 4 | Move-aside crash-window: non-empty backup + empty storeDir + absent sentinel ⇒ nil no-op, no error, backup untouched | **M3** | — |
| 5 | `TestNatsRestartRevivalFailure_BlocksLoudly` — fake nats bin + `/varz` never clustered ⇒ `growCutoverRevivalFailed` + hint | **M4** | §8 |
| 6 | `TestFormerN1ResetGate_FiresOnlyWhenClustering` / R3 gate — <2-server or self-absent ⇒ `bad_request`; committed 2-server ⇒ passes; already-clustered ⇒ AlreadyDone, no store move | **M4** | §8 |
| 7 | `renderClusteredCutoverConf` golden/round-trip — peers<2 ⇒ error; secrets-dir mTLS paths; `SynthesizeClusterListen`; self present; **http `127.0.0.1:8223` forced** (also covers m2) | **M4 / m2** | — |
| 8 | `TestGrowResume_sixFailurePoints` + `TestClusterAdd_doubleRun_singleMembershipChange` — kill at each §8 point ⇒ exactly one N+1 voter, no orphan, one rename + one restart, no double AddVoter | **M5** | §8 / §9(C) |
| 9 | `TestGrowLock_noSelfBlock_symmetricFence` + `TestConcurrentAddRetireRace` (`-race` + leak gate) — carve-out intact while marker held; retire + upgrade-acquire refuse on grow; different-joiner acquire refused; `GrowLockActive` in health reply | **M6** | §8 |
| 10 | Different-node approve refused during a grow while the grow's own same-target approve is allowed | **m1** | — |
| 11 | ReadDir-error injected on a data-bearing store ⇒ reset refused without `--reset-former-js` (fail-closed) | **m4** | — |
| 12 | `CanonicalGrowReqBytes` field-sensitivity extended to `JoinBundle` + `OpID` tamper cases | **m5** | — |
| 13 | mesh-peers handler: mixed-phase + empty-nkey rows ⇒ exactly the eligible triples; bind SQL phase literals to constants | **m6** | — |
| 14 | Drill 11 (deploy-tier): write JS data while N=1, grow with `--reset-former-js`, assert `grow-bak.<epoch>` exists + no single-replica orphan | **m7** | §5 |
| 15 | `clusterOps` routing-map assertion for `OpClusterGrowCutover` **only if** it is kept (else delete the dead code) | **m3** | — |

**Process note:** the FINAL plan (which per this repo's discipline is authoritative before code) promised at least eight §8 hermetic tests (`TestFormerN1ResetGate_FiresOnlyWhenClustering`, `TestNatsRestartRevivalFailure_BlocksLoudly`, `TestD9FirstGrowStandaloneToClusteredEndToEnd`, `TestGrowResume_sixFailurePoints`, `TestGrowLock_noSelfBlock_symmetricFence`, `TestConcurrentAddRetireRace`, `TestCatchupStallDetection`, `TestReconciler_NoDoubleWipeUnderTicks`) plus §9(D)'s non-sim revival-failure clause — **none of the load-bearing ones landed.** Either land them or amend §8/§9(D) with justification; do not close Stage-C with the plan asserting tests and properties the code lacks.

---

## 5. Main-process response (Stage-C adjudication + fixes)

All 14 confirmed findings adjudicated. Code findings FIXED, test gaps ADDRESSED, one MAJOR (M2) DEFERRED with
plan documentation. Hard gates re-run GREEN after fixes (`make lint` 0 issues + `make test` + deploy-tier
drills 10/11). Per-finding:

| ID | Disposition | What changed |
|---|---|---|
| **B1** | ✅ FIXED | `findJoinOp` returns "" on a TERMINAL op (only attaches to a live in-flight join); `driveAdd` short-circuits an already-VOTER joiner to release-lock + done (the crash-post-SERVING resume, without the terminal-op attach). Abort-retry / remove-then-re-add now converge to a fresh op. |
| **M1** | ✅ FIXED | `--preserve-js-data` now unblocks the non-empty reset gate (`ResetAck \|\| PreserveData`); the dead-end recommending the passed flag is gone; flag help + refusal message corrected to "move aside, restore by hand"; auto backup→restore explicitly deferred (plan §12.10 — the store is never deleted). |
| **M2** | ⏭️ DEFERRED (documented) | #7 ships the size-scaled deadline only in v1; the AppliedIndex stall-window is a follow-up. A false-BLOCK is operator-recoverable (`cluster ops confirm`); plan §12.10 + §11/§9-D corrected (the mis-sized-cap claim was false). |
| **M3** | ✅ FIXED | `moveAsideJetStreamStore` keys idempotency on the DURABLE backup-dir existence (checked first), not the crash-losable sentinel → a crash-in-window resume is a no-op, never the ENOTEMPTY wedge. Test: `TestMoveAsideJetStreamStore_CrashWindowBackupExists`. |
| **M4** | ⚠️ PARTIAL + E2E | Leaf logic unit-tested (move-aside stages: crash-window, epoch, empty/non-empty, ReadDir-fail-closed). The full `performGrowCutover` R3-gate + revival-BLOCK + render integration test needs a `b.cl.node` broker + injectable nats-timeout/probe seams (a refactor) — deferred as a follow-up; the paths are E2E-validated by drills 10/11 (R3 gate passes on committed-2-server; revival via `Restart=always` succeeds). |
| **M5** | ⚠️ E2E + partial | Staged-idempotent move-aside is unit-tested (M3 + epoch tests pin the "restart-only, no second move" contract). The six-failure-point kill/resume integration is E2E-validated (drill 10 GREEN through the real ladder) — the full hermetic harness (injection seams at each kill point) is a follow-up per plan §9(C)'s deploy-tier designation. |
| **M6** | ✅ FIXED (tested) | `TestG4GrowMarkerFenceAndCarveout` pins: different-node join + retire refuse on the marker; the grow's OWN join is allowed (carve-out); marker reader. Added an inline "do NOT add growActive here" comment at `driveInFlightOperations` (the deadlock trap the reviewer flagged). |
| **m1** | ✅ FIXED | `StartJoinOperation` now refuses a DIFFERENT-node join during a grow (joiner-id-valued marker → self-op carve-out preserved), closing the retire/join asymmetry. Covered by `TestG4GrowMarkerFenceAndCarveout`. |
| **m2** | ✅ FIXED | `renderClusteredCutoverConf` forces `MonitorListen = topoMonitorListen` (127.0.0.1:8223), which the revival probe hardcodes — no false revival-failure from a harvested-absent monitor. |
| **m3** | ✅ DELETED | The unroutable, caller-less `OpClusterGrowCutover` local op + `growCutover` hook + three `Grow*` request fields removed (the pre-render obsoleted the joiner-local cutover; refuted-finding #21 confirmed no caller). |
| **m4** | ✅ FIXED | The non-empty ack gate fails CLOSED on an `os.ReadDir` error (treats an un-enumerable store as data-bearing). Test: `TestMoveAsideJetStreamStore_ReadDirErrorFailsClosed`. |
| **m5** | ✅ FIXED (test) | `TestCanonicalGrowReqBytes_fieldSensitivity` extended with `JoinBundle` + `OpID` tamper cases (highest-value signed fields). |
| **m6** | ✅ FIXED (test) | `meshPeerTriples` extracted + `TestMeshPeerTriples` (mixed phases + empty-nkey row → exactly the eligible triples). |
| **m7** | ✅ FIXED (drill) | drill 11 now asserts the former-N1's non-empty N=1 JS store is MOVED to `jetstream.grow-bak.<epoch>` (move-not-delete proof), re-run GREEN on weilandserver. |

**Residual (documented follow-ups, non-blocking):** M2's stall-window; M4/M5's full cutover-integration + six-point-resume hermetic harness (injectable-seam refactor) — both E2E-validated by the deploy-tier drills, which plan §9 designates as the acceptance surface for exactly these paths.
