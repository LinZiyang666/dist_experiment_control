# R16 — grow-onto-recovered deep defect family + transfer #57 / #58-split-home — PLAN

> Status: **Stage A finalized** (main process is the sole finalizer). Drafted by a 6-expert Fable-5
> adversarial workflow (6 draft → 6 critique → 1 synth) over a 4-agent recon; the main process
> verified the load-bearing roots against source and made the finalizer decisions recorded in §0.
> HA-critical path — deliberately not rushed. Follows CLAUDE.md §3 (Stage B implement → Stage C
> internal review → external review gate). Recon bundle: job tmp `r16-context-bundle.md`.
>
> Base: main HEAD `b602fc7`. Handoff context: `docs/reviews/release-readiness-followups.md` §3/§8
> (path B). Roadmap lineage: G4 (`tether cluster add`, commit d93d842) is DONE; R16 fixes the
> recovered-survivor case G4 explicitly left as a deferred deep-HA defect. Gotcha SSOT:
> `docs/reviews/v0.4.5-ha-grow-ops-gotchas.md` (#3/#4/#20) + `docs/deploy-tier-gotchas.md`
> (#GROW-ONTO-FORCE-SINGLE / #GROW-ONTO-RECOVERED / #57 / #58-split-home).

## 0. Finalizer decisions (binding for Stage B)

1. **Drill 51's root is ADJUDICATED BY A STEP-0 RED-FIRST REPRO, not pre-committed.** Source
   adjudication (main process) NARROWED it: `normalizeRestoreStaging` (`restore.go:343-361`) ALREADY
   clears non-terminal `cluster_operations` + `cluster_grow_active`/`cluster_upgrade_active` locks in the
   restore txn (the R15 #31/#45/#51 fix, pinned by `restore_test.go:180-239`), so the bundle-stale-op and
   grow-lock candidates are **R15-CLOSED**. The remaining 51 `NATS_ROLLED_OUT | already in flight`
   signature therefore comes from **THIS grow's own join op stalling at the C3 topology/NATS convergence
   phase** (not a bundle residue), and re-run then sees it as "already in flight". Step 0 reproduces 51
   hermetically and adjudicates the two remaining candidates: **(c) restore-not-grow-ready raft** — a
   DR-restored survivor's raft has `FirstIndex=1` (no snapshot; init.go:242 calls `GrowReadySnapshot`,
   restore.go:207 does NOT), so the fresh joiner replays a log that never carried the direct-installed
   rows (pc732 hollow-voter/FK class) → the op cannot converge; vs **(d) a topology/JS-meta convergence
   wedge** independent of the snapshot. **The A2 fix is aimed only after the RED pin identifies which.**
   §2 Lane A2 documents the candidate remedies; exactly one (or a minimal combination) ships.

2. **Drill 42's root is VERIFIED (joiner-side stale JS store).** `Recover` (rejoin-prepare engine,
   `internal/clusteroffline/offline.go:702-740`) wipes only `raft/` + `tether.db`, NOT the JetStream
   store; the drill's `_reset_after_force_single` (42:36-44) `mv`s only the SURVIVOR's store, never the
   returning joiner's. So brk2 boots `cluster add`'s clustered conf onto a dead-epoch clustered JS meta
   → `n1ClusteredJetStreamFatal` (broker.go:731/1083) → CATCHING_UP stall. **A1 (joiner P5 reset) is
   the direct flip; A3 (offline force-single `--reset-js`) retires the drill's Mandate-④ survivor
   compensation.**

3. **A4 (`reconcile nats --to-standalone --reset-js`) is IN.** It completes the de-cluster verb every
   remedy text already points at and retires the product's last standing raw-`rm -rf` instruction
   (`warnClusteredJSShrink`, `cluster_natsconf.go:92-104`) — escape-hatch completion, not
   online-force-single remediation machinery. Low cost, aligns with the "no delete outside a gate"
   invariant.

4. **Joiner ack model = REUSE the two existing grow flags** (`--reset-former-js` / `--preserve-js-data`)
   with help + refusal text widened to name "any pre-existing JS store on EITHER end of the grow." One
   grow, one ack model; no third flag.

5. **A5 (meta-aware auto-reset cutover + `/jsz` meta-health verdict machine) is DEFERRED (record-only).**
   Rationale (security-pragmatism + minimal-correct-scope): NO drill exercises grow-onto-a-still-clustered
   survivor (drill 22 online-force-singles but rebuilds a FRESH N=2, never re-grows the residue; 42/51
   survivors are standalone at grow time). The online-force-single / racknerd clustered-residue survivor
   already has a DOCUMENTED remedy which A4 now COMPLETES: `reconcile nats --to-standalone --confirm-single
   --reset-js` before re-growing. Auto-detecting a stale-vs-healthy live meta robustly needs a new `/jsz`
   probe + verdict state machine — over-scope for an uncovered case. **R16 ships the LOUD-SAFE minimum
   instead (A5-min, §2):** the cutover must never SILENTLY classify a clustered-residue survivor as
   `AlreadyDone`; the grow-side auto-reset + meta-health probe is a recorded follow-up.

## 1. Root cause

Every recovery operation rebuilds a node's raft identity and abandons ≥1 other piece of epoch-bound
state, while every grow-side protective branch keys on the one shape recovery never produces —
`own.IsStandaloneJetStream()` (`cluster_grow_cutover.go:74`, `natsreconcile/reconcile.go:135`). Three
independent residues, each a required flip:

1. **Returning joiner's JS store (drill 42).** rejoin-prepare wipes db+raft only; the returning node's
   dead-epoch clustered JS meta stays on disk; `renderJoinerClusteredConf` rewrites the conf but resets
   no store → the joiner boots a meta naming dead peers → `n1ClusteredJetStreamFatal` → CATCHING_UP
   stall. (Survivor-side store reset is a real gap too, today MASKED by the drill's unlabeled `mv` =
   Mandate-④ violation; A3 retires it.)
2. **Restore leaves a membership-op fence and/or is not grow-ready (drill 51).** The `already in flight |
   NATS_ROLLED_OUT` signature is the #45/#51 stalled-op fence; the exact mechanism is adjudicated in step 0
   (finalizer decision #1).
3. **Clustered-residue survivor (online-force-single / racknerd shape).** Cutover Stage A returns
   `AlreadyDone` on any live cluster name (`:59-61`), Stage B restart-only on any clustered conf
   (`:70-72`); the only reset is unreachable behind the standalone guard (`:74`). A stale lone/dead-peer
   meta is silently "done" and the 1→2 meta wedges. No drill flips here; R16 makes it LOUD not silent
   (A5-min), defers the auto-reset.

Transfer lanes are the same shape one layer up: terminal audit needs a RAM-only tracker entry
(`claimFinalize`, transfer.go:160-169) + a `runCtx`-bound watchdog (`:399-441`) → home crash strands
`start` rows forever (#57); reap authority needs one broker to home EVERY session node
(`homeOwnsXferBucket`, transfer_home.go:78-111) → split-home buckets are immortal, gauge-only (#58).

**Recovery-side vs grow-side vs both: BOTH, split by residue.** Recovery-side where the recovery verb is
the natural owner and a drill compensation must be retired (A3 offline force-single `--reset-js`; A2
restore fix); grow-side where the residue is only provably stale INSIDE a committed grow (A1 joiner store
at P5; A5-min survivor loud-detect). NO online-force-single auto-de-cluster executor (out of scope, no
acceptance net); NO rejoin-prepare-time store move (the returning node's `Restart=always` nats re-dirties
state in the window — correctness-critique hard veto).

## 2. Fix design

### Lane A — grow-onto-recovered

**A0. Shared move-aside helper (prerequisite).** Extract `moveAsideJetStreamStore`
(`cluster_grow_cutover.go:256-302`) into a package-level `natsconf.MoveAsideJSStore(storeDir, backupPath,
sentinelPath string, ack bool) (backup string, err error)`, preserving VERBATIM: backup-dir-first durable
evidence (M3), sentinel-last advisory, m4 fail-closed on ReadDir error, non-empty→refuse-without-ack. The
broker method becomes a thin delegate (behavior-frozen; existing `cluster_grow_cutover` tests stay green).
NEW behavior: an EACCES on the rename fails LOUD with the broker-ops #6 chown hint (the drill-51 G-own
class), never skip-and-proceed.

**A1. Joiner-side stale-store reset at grow P5 — the drill-42 flip.** In `driveAdd`, inside the
`!joinerBrokerUpLocal` block right after `renderJoinerClusteredConf` (`cluster_add_drive.go:171-176`) and
BEFORE the P3b `startJoinerHint` HALT (`:190-193`): preflight the joiner's local conf; if `JSStoreDir()`
is non-empty on disk:
- unacked → HALT with an M2-stable refusal naming the JOINER's store, the backup destination, and the
  exact re-run;
- acked (`--reset-former-js`/`--preserve-js-data`) → `MoveAsideJSStore` to `<store>.grow-bak.<opID>`,
  sentinel keyed on opID in the joiner's data dir;
- empty/absent → no-op (fresh joiners; drills 10/11 byte-equivalent, pinned by a test).
Placement rationale: P5 is the only moment "any local JS state is prior-life residue" is provable (raft
freshly `{self}`, broker not up, opID known); rename semantics bound the live-old-nats window (open fds
follow the renamed backup; the operator's `restart nats-server` is the very next printed step).

**A2. Restore fix — the drill-51 flip (mechanism from step 0).** Aim ONLY after the step-0 RED pin
(finalizer decision #1). Candidates (a) bundle-stale-op fence and (b) grow-lock residue are **R15-CLOSED**
(`normalizeRestoreStaging` already clears them, `restore.go:343-361`). The remaining candidates:
- (c) **restore grow-ready** — add the `init.go:236-242` mirror `cluster.GrowReadySnapshot(DataDir, DBPath,
  m.SelfID, effectiveRaftAddr, logger)` after `BootstrapSingleNode` (`restore.go:207`) and before
  `clearRestoreInProgress` (`:215`), so `FirstIndex>1` and a fresh joiner installs the snapshot instead of
  replaying a log that never carried the direct-installed rows (pc732 hollow-voter/FK). Reorder
  `clearRestoreInProgress` AFTER the snapshot (crash between bootstrap and snapshot leaves the
  daemon-refusing marker; re-run forward-completes). NOTE the `applied_index=0` × snapshot-index interplay
  is a known unknown — the step-0 test decides and pins it before lane code. Ships iff the repro shows a
  hollow-voter/FK catch-up (or a convergence stall the snapshot resolves).
- (d) **topology/JS-meta convergence** — if the repro shows the joiner catching up fine (raft) but the op
  stalling at C3/NATS convergence for a mesh/JS-meta reason, aim there instead (compare against the
  working drill-10/11 fresh-grow path; the only survivor-state delta is the missing grow-ready snapshot,
  so (c) and (d) may be the same root — the repro decides).
Whichever ships, assert (don't assume) zero unpublished audit ops in the resulting log.

**A3. Offline force-single `--reset-js` — retires drill-42's survivor compensation.** New `--reset-js`
flag on `cluster recovery force-single`. In the CLI de-cluster step (`cmd/tether/cluster_offline.go`, at
the `warnClusteredJSShrink` call `:225`, after `deClusterStandaloneConf` applied the standalone conf):
store non-empty ∧ no `--reset-js` → refuse LOUD naming the exact re-run (the force-single journal's
forward-completion makes the re-run converge); with it → `MoveAsideJSStore` to
`jetstream.force-single-bak.<epoch>` where `<epoch>` is recorded in the force-single journal on FIRST run
(stable naming across resumes), then print backup path + `nats stream restore` inverse + the restart
instruction. New journalled phase after `phaseRaftRebuilt`. The `rm -rf` advisory is retired here.

**A4. `reconcile nats --to-standalone --reset-js`.** Same helper, same gate, in
`runReconcileToStandalone` (`cluster_natsconf.go:118-239`): with the flag, move-aside
(`jetstream.standalone-bak.<ts>`) after the post-apply standalone proof; without it, the warning names
"re-run with `--reset-js`" instead of printing `rm -rf`. (Finalizer decision #3.)

**A5-min. Loud-safe clustered-residue detection in the cutover (belt-and-suspenders).** In
`performGrowCutover`, the Stage A `AlreadyDone` (`:59-61`) and Stage B restart-only (`:70-72`)
short-circuits must NOT silently absorb a clustered survivor whose clustered state PREDATES this grow.
Minimal rule using the durable evidence that already exists (NO new `/jsz` probe): when the live/on-disk
state is clustered but there is NO `grow-bak.<GrowEpoch>` evidence for THIS grow epoch AND the survivor
entered clustered (not moved-aside by this cutover), emit a LOUD `grow_cutover_clustered_residue`
sys.event + return a refusal (`CodeBadRequest`) whose text names the remedy `reconcile nats --to-standalone
--confirm-single --reset-js` then re-run `cluster add`. Never a silent `AlreadyDone` on unproven-healthy
clustered residue. **Deferred (record-only):** the full auto-reset of the residue in-cutover + a `/jsz`
meta-health verdict machine (`grow_cutover_meta_ambiguous`) — recorded as a gotcha follow-up.

**Deliberately NOT built:** online force-single auto-de-cluster executor; rejoin-prepare store move;
joiner boot grace window / TTL sentinels (unneeded once A1 empties the store and P5-before-P3b ordering
gives the joiner a live clustered peer — the fatal's message gains one mid-grow hint line, no machinery);
full A5 auto-reset + `/jsz` verdict machine.

### Lane B — transfer #57 (finalize-on-recovery)

**Node-local durable in-flight ledger + home-broker finalizer** (the ONLY wire-safe mechanism —
`OpTransferAudit` is documented apply-inert/empty-Body, `internal/xferaudit/plan.go`; `cluster_forward.go`
discards `env.ReqID` for `VerbTransferAudit`; audit-stream tail-scan loses exactly the crash-under-load
rows #57 exists for; a mutating applier / baked SQL / minted reqID are all rejected as factually
impossible or wire-hazardous):
- At the tracker `put()` point in `transfer.go` (before the prepare is forwarded, ordering preserved),
  when `ClusterDataDir` is set: write `<ClusterDataDir>/xfer-inflight/<transferID>.json` (0600; immutable
  fields + `startedAt`). Remove it at EVERY terminal path: `handleEvTransfer` (:849-861),
  `handleFinalizeReq` (:1114-1126), watchdog (:426-436), `cleanupEntry`.
- On boot + a periodic pass (existing reconcile cadence, no new goroutine surface): for each ledger file
  older than `tierTimeout(tier) + slack(60s)` with no live tracker entry, emit a DETERMINISTIC synthetic
  terminal via `emitTransferAudit`: `Kind:"failed"`, `Code:"home_broker_restart"`, `Ts = startedAt +
  tierTimeout` — so `TransferRecordReqID` (content hash incl. Ts) collapses every re-emit/crash-window
  duplicate in the 0011 ledger, zero forward/propose changes. Then best-effort `deleteXferObject`, then
  remove the file (idempotent).
- Single-broker mode: record-only defer (its audit publish is already best-effort).
- **Residual (documented, deferred):** a PERMANENTLY dead home leaves its node-local ledger unreadable →
  the audit row stays dangling; the bytes are reclaimed by Lane C. A replicated ledger would fix it but is
  the exact wire hazard we refuse.

### Lane C — transfer #58-split-home (leader cross-home GC)

Unanimous mechanism; object-key/owner renaming rejected (agent/ctl wire surface for zero gain). In
`reconcileXferObjects` (`transfer_reconcile.go:82-97`), when `homeOwnsXferBucket(sid)` is false, add: if
`reaperMayDelete()` (caught-up LEADER) ∧ `xferBucketOrphanedEverywhere(sid)` (transfer_home.go:122-157,
already shipped, N-6b) ∧ bucket ∉ `activeOBJStreams()` → delete non-deleted objects with `age ≥
xferCrossHomeReapAge`. Age floor = a DERIVED expression `3 × transferTimeoutTierB` (15m — enough
cross-node ModTime/clock margin), pinned by a compile-adjacent test so the relation can't drift. Add a
serveconf seam mirroring `xfer_reap_interval` so drill 96 can compress it (production default = the
derived constant; no operator tuning story). The N-6b `xferUnreapableBuckets` gauge then decays to 0 (the
drill-observable signal). Fast home-partitioned path and `reaperCaughtUp` untouched; the
`transfer_reconcile.go:86-93` TODOs flip to "retired by R16".

## 3. R3 + data-loss gating

Zero autonomous de-cluster paths added; `natsreconcile` unchanged (R3 posture `reconcile.go:109-116` +
the cutover withhold `:179-187` stay byte-identical). Every store reset is MOVE-ASIDE, never delete,
through the ONE helper, behind an explicit DATA ack whose text names the data:

| Site | Trigger | Ack | Backup |
|---|---|---|---|
| Joiner store (A1) | non-empty local store at P5 | `--reset-former-js` / `--preserve-js-data` (help+refusal name BOTH ends) | `jetstream.grow-bak.<opID>` |
| Offline force-single (A3) | conf de-clustered, store non-empty | `--reset-js` (typed node-id confirm proves TARGET intent only — never rides it) | `jetstream.force-single-bak.<journal-epoch>` |
| `--to-standalone` (A4) | `--confirm-single` + F2 N=1 machine gate | `--reset-js` | `jetstream.standalone-bak.<ts>` |

`warnClusteredJSShrink`'s `rm -rf` and drill 42's `_reset_after_force_single` — the two real deletions
outside any product gate — are both retired. Every refusal/move prints the backup path + the `nats stream
backup/restore` route (auto backup→restore stays NOT implemented, g4 §12). A5-min never auto-resets and
never silently absorbs residue. #57 writes only additive audit; #58 deletes only leader-gated,
orphaned-everywhere, ≥15m-aged transfer temp objects (existing reaper posture, per-reap logged).

## 4. Idempotency / crash-recovery

- **Joiner move (A1)**: keyed on `grow-bak.<opID>` backup-dir-first; resume with backup present never
  re-moves (protects the fresh new-conf meta after the operator's restart); `joinerBrokerUpLocal`
  short-circuits once booted.
- **Restore (A2)**: staging→install→bootstrap→(snapshot, if A2c)→clear-marker; crash anywhere pre-clear
  ⇒ daemon refuses start, re-run forward-completes (existing kill-9-idempotent staging). Stale-op fence
  (A2a) is a pure DB UPDATE inside the staged transaction — atomic.
- **Offline force-single (A3)**: `--reset-js` is a journalled phase after `phaseRaftRebuilt` with the
  epoch persisted at first run; interrupted runs resume there; refusal-then-re-run-with-flag converges.
- **A5-min**: pure detection + refusal, no state change → trivially idempotent.
- **#57**: ledger write before forward (a crash between put and write loses only what today loses);
  emit-then-remove crash re-emits an IDENTICAL record → same content reqID → deduped; file removal
  idempotent.
- **#58**: deletes tolerate `ErrObjectNotFound`; leadership flap delays one interval.
- No raft membership ops anywhere in the cutover file → no double raft change by construction.

## 5. Test matrix

**Step 0 — RED pins BEFORE any lane code (`test/d9`):**
1. `testGrowClusteredSingleWedgesJS` — the clustered-single sibling of `GrowStandaloneRestartWedgesJS`
   (same `t.Skip` flip-marker for future NATS bumps); real-nats constraint pin proving the joiner-store
   wedge (drill 42's mechanism).
2. `RestoreThenGrowAdjudication` — restore a data-bearing bundle (incl. a non-terminal op) → attempt a
   grow (pattern of `grow_migrated_snapshot_test.go` / `grow_migrated_leader_e2e_test.go`): RED today,
   and ADJUDICATES 51's root among stale-op / grow-lock / grow-ready / JS-meta (finalizer decision #1).
   Decides the A2 remedy + (if A2c) the snapshot/applied_index pairing.
3. Confirm-in-code: `force_single_active` clear at join-SERVING is already wired
   (`cluster_operation_controller.go:609-612`) — expect no work.

**Hermetic d9 (fix pins):** `testGrowRecoveredResetThenWorks` (reset → meta forms at R=2);
`ReturningJoinerStoreMoveAside` (non-empty⇒refuse/ack/move; empty⇒no-op — the drills-10/11 equivalence
guard); the 51 fix pin for whichever A2 remedy ships (+ `RestoreCrashBetween...` resume if A2c);
`OfflineForceSingleResetJS` (+ journal kill-9 resume); `ToStandaloneResetJS`; `A5MinRefusesResidueLoudly`
(clustered + no this-epoch evidence ⇒ loud refusal, not AlreadyDone).

**Package tests:** `natsconf.MoveAsideJSStore` table (ported + ENOTEMPTY resume + EACCES-loud-with-chown-
hint); #57 ledger lifecycle (write-at-put ordering; removal on all four terminal paths; deterministic
re-emit → content-reqID dedup; young/live-tracker skips) with `-race` + NumGoroutine/fd leak gates; #58
reap ladder extending `reaper_gate_test.go` (leader+aged+split-home reaps; follower never; under-floor
never; busy-bucket skip; other-homed untouched; gauge decays; `xferCrossHomeReapAge ≥ 3*transferTimeoutTierB`
derivation pin); `cmd/tether` golden command-tree for the new flags.

**Drills (acceptance, weilandserver via `remote.sh --build`):**
- **42**: replace `_reset_after_force_single`'s `mv` with the product verb (`force-single … --reset-js`
  via pty-confirm) + file-postconditions (standalone conf, `force-single-bak.*` present, both services
  active after the SIM's systemctl restart — systemctl stays legitimately sim-side); terminus flips
  through the existing rc=0 branch (42:209) + VOTER + tier-A round-trip; the `#GROW-ONTO-FORCE-SINGLE` /
  `#47` product_red branches stay as regression traps; the guard's survivor-blaming prose is corrected
  to the joiner-store root.
- **51**: arm I flips through the existing `_as_pass` branch (51:494-497); `[#GROW-ONTO-RECOVERED]`
  product_red stays as a trap; `[GAP #6-chown]` UNTOUCHED (orthogonal). Honest tsv: 51 may land
  INCOMPLETE if orthogonal gaps (#53-scope WONTFIX / H1a) remain — never laundered GREEN.
- **96**: (i) make the brk2 restart + post-restart #57 judgment UNCONDITIONAL (today coupled inside the
  #58 arm's conditional — cross-arm coupling banned); after restart poll `history --kind transfer` for a
  terminal whose Code is the synthetic `home_broker_restart` (a bare `complete|failed` grep would misread
  it); found ⇒ #57 FIXED GREEN, absent-after-window ⇒ product_red; the in-window R-EXHAUST branches stay
  as the honest pre-restart truth. (ii) #58: set the cross-home-age seam low in setup (like
  `xfer_reap_interval=8s`); flip the 96:447 gap branch to a GREEN drain-to-tombstone-floor assertion
  accepting the leader-GC signature; the REGRESSION branch (96:444-445) stays.
- **Regression**: 10 + 11 (fresh-grow byte-equivalence), 30 (rolling upgrade — wire lane), 50 (restore
  surface), 20/41 (force-single surface); one concurrent `run-drills.sh` pass at the end. 22/82: tsv
  note-text re-pointing only.

**Hard gates:** `make test` + `make e2e` + `make lint`, `-race` + built-in leak gates on all touched
concurrency surfaces. Any gate red = not done.

## 6. Scope boundary

**Fix now:** A0–A4, A5-min, Lane B (#57 node-local ledger + finalizer), Lane C (#58 leader GC + seam),
drill flips 42/51/96, docs (runbook §2.2/§5.2 step-lists, cluster.md + broker-ops flag docs,
deploy-tier-gotchas ledger flips for #GROW-ONTO-FORCE-SINGLE / #GROW-ONTO-RECOVERED / #57 / #58-split-home,
this plan).

**Defer, record-only** (each a gotchas line): online force-single auto-de-cluster/executor (steady-state
N=1 503 keeps the now-complete documented remedy `reconcile nats --to-standalone --confirm-single
--reset-js`); the full A5 auto-reset + `/jsz` meta-health verdict machine; rejoin-prepare-time store
handling; joiner boot grace/sentinel machinery; #3-residual cluster-name cross-check; auto `nats stream
backup→restore`; per-object owner encoding; single-mode #57; permanent-home-loss audit dangling;
backup-dir accumulation pruning; `[GAP #6-chown]`.

**Explicitly excluded:** drills 22/82 code (band labels only); **#65**; any re-touch of G4 `cluster add`
phase machinery beyond the named insertion points; any `ProtoVersion` bump (all changes additive: new
audit Code string, new CLI flags, reuse of on-wire `ResetAck`/`PreserveData`).

## 7. Implementation order

1. **Step 0**: RED pins (d9 tests 1–2) + confirm `force_single_active` clearing + ADJUDICATE 51's root
   → fix the A2 aim. Do NOT proceed to A2 code before the RED pin.
2. **A0** helper extraction (behavior-frozen).
3. **A2** restore fix (mechanism from step 0) + tests (smallest diff, unlocks 51).
4. **A1** joiner P5 move + tests (flips 42's live blocker).
5. **A3** offline force-single `--reset-js` + journal phase; **A4** `--to-standalone --reset-js` (retire
   the compensation + the `rm -rf`).
6. **A5-min** loud clustered-residue detection + test.
7. **Lane B** #57 ledger + finalizer + tests.
8. **Lane C** #58 GC + seam + tests.
9. **Drill edits** 42/51/96; weilandserver runs (42/51/96 + regression set).
10. **Docs + ledger flips**; hard gates; stop for internal review → external review.

## 8. Risks + open questions

1. **51's root is adjudicated, not yet proven** — step 0 is a hard gate; budget one diagnosis loop; do
   not ship any 51 flip claim without the RED pin.
2. **A5-min honesty**: the accepted failure mode is a LOUD refusal (operator runs the documented
   `--to-standalone --reset-js` then re-grows), never a silent wedge. If a future need arises, the full
   auto-reset is the recorded follow-up.
3. **Joiner move under a live old-conf nats**: rename sends the old process's writes into the backup via
   open fds; path-created files could land in the fresh dir until the operator's restart (window is
   operator-bounded, the hint orders the restart next — parity with the shipped survivor cutover). If
   drill 42 shows re-dirtying, mitigate by re-checking emptiness on resume ONLY when local nats is not
   yet serving the new cluster name — decide on evidence.
4. **#57 duplicate-terminal edge**: a genuinely-late real terminal after the synthetic yields two
   terminals for one tid — bounded, visible, documented ("first terminal wins" for consumers); strictly
   better than a permanent dangling start.
5. **#58 floor soundness** rests on "no transfer outlives `transferTimeoutTierB`"; the derivation pin
   test breaks loudly if a future tier edits the relationship. Audit `cleanupEntry`/forward-failure
   paths for a route that drops the tracker entry while leaving a resumable upload (believed none;
   add the mutation test).
6. **Mixed-version fleet**: no ProtoVersion bump, no FSM/op changes, ledger is node-local → old/new
   brokers interoperate; new CLI flags are new-verb-gated. Drill 30 in the regression set is the check.
7. **Drill honesty**: 42's GREEN rests on product verbs + sim systemctl only; 51 stays INCOMPLETE if
   orthogonal gaps remain; 96's #57 flip is never judged while the home is down. Any flip that can't
   fire through the drill's real control flow is a false GREEN — worse than the RED it replaces.
