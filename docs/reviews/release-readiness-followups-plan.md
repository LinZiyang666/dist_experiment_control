# §6 release-readiness follow-ups — implementation plan (finalized)

> Phase type: **post-1.0 leaf increment** (not on the linear P-series). Scope = **all of §6** of
> `docs/reviews/release-readiness-followups.md` — the follow-up technical debt the external reviewers
> (claude + codex, 3 rounds → Pass) explicitly labeled **non-blocking**. OUT OF SCOPE: §3 R16
> (grow-onto-recovered, its own phase), §4 release mechanism (user decisions + live fleet), §5 macOS gate.
>
> Process: CLAUDE.md §3 three-stage / seven-step. Stage A drafting used a 4-lens adversarial Workflow
> (Fable 5 drafters + Fable 5 cross-critics, per the one-time model exception). This file is the **sole
> finalized plan**; the main process is the only author. Stop at the external-review gate (step 6).
>
> Baseline: main HEAD `55b1451`. All line numbers verified against that tree.

> **⚠ UPGRADE NOTE for the next version tag (external review L-4)**: this batch adds an UPPER clamp
> (24h) to `broker.cluster.xfer_reap_interval`, `broker.storage.proc_gc_interval`, and
> `broker.observability.disk_check_interval` — a value **> 24h now fails `Load()` at boot** (previously
> it silently disabled the reaper/monitor). A deployment whose `broker.yaml` carries a `>24h` value for
> any of these will **refuse to start after upgrade**. The live v2 fleet (per memory) sets none of these
> knobs (all default), so real risk ≈ 0; still, put "check broker.yaml for >24h reap/gc/disk intervals
> before upgrading" in the release notes.

---

## 0. Finalization ledger (decision per item, with the adversarial adjustments folded in)

| Item | Decision | Key adversarial adjustment adopted |
|---|---|---|
| A1 CommitIndex()>0 | **DO** — extract `Node.CaughtUp()`, apply to BOTH reaper predicates | comment honesty: it means *dispatched-to-FSM*, not *applied*; boot-zero is really snapshot-restart applied=S>0/commit=0 |
| A2 M-2 behavioral | **RECORD-ONLY** (Barrier rejected on discovery) | drafted conn-hold test is DOA (raft AppliedIndex advances at dispatch); **Barrier ALSO rejected** — `raft.Barrier(timeout).Error()` has NO apply deadline, it HANGS the reconcile goroutine on a wedged FSM (verified empirically), strictly worse than the µs-scale backstopped residual; keep grep pin + honest comments |
| A3 M-3 wiring test | **DO** | `>0`-before-equality assert ordering is load-bearing |
| A4 RESIDUAL-1 | **RECORD-ONLY** | skew-subtraction re-admits unrelated stranded rows → re-wedge; strictly worse |
| B1 N-3 issuer fail-open | **DO** unverified-signal + doctor ADVISORY + reconcile WARNING (never refuse) | stock install.sh conf has no `authorization{}` → confIssuer=="" is the mainline; refuse would break init/restore |
| B1-adjacent online-doctor skips skew | **RECORD** (not in-phase) | fixing adds permanent advisory noise on every custom-conf online doctor |
| B2 N-4b fresh-box remediation | **DO** honest 2-step + install.sh-clobber warning | distinguish broken-vs-missing conf; execution pin needs REAL keys |
| B3 N-4c seam nats_conf_path | **DO** thread `--nats-conf`; join-flow = record-only | no-thrash idempotency verified; 8 test call sites |
| DOC-27 CLI help (cluster_backup.go:33/35/92) | **DO** (lane B batch) | DOC-27 not closed while `--help` prints the broken path |
| C1 N-5a events truncated flag | **DO** across broker+adminsock+cmd | distinguish scan-cap / ctx-deadline (truncated) from cutoff / n-satisfied (complete) |
| C2 N-5b interval upper clamp | **DO** reject (not clamp) at 24h | fail-closed at Load(), matches lower-bound idiom |
| C3a N-6 Threads comment | **DO** (comment sweep incl. adminsock field doc + broker-ops.md) | ever-created/monotone, not a live count |
| C3b N-6 unreapable-bucket counter | **DO** observability-only gauge | orphaned-*everywhere* + has-aged-objects predicate (else N-1/N noise); never softens #58/drill-96 |
| C4 N-9 attempts prune | **DO** live-set prune (not TTL) | mirrors pruneHomeApplied; backoff-preservation guard test |
| D1 drill 96 registry sync | **DO** | purge the stale "454 orphans PINS LIVE=PRODUCT-RED" sentence; keep the `#58` token (crosscheck pin) |
| D2 drill 30 adjudication | **DO** scene-capture + weilandserver experiment + numbered decision tree | numbered `### #NN` gotcha IDs (crosscheck-visible); 4th "no-repro" branch; permanent watcher; +22 attribution |
| D3 kept-sites | **DO** ratchet→1398 + provenance header + quote-mask + committed self-test | `\047` literal; regenerate baseline LAST |
| D4 M-5 lint gate D | **WITHDRAW** (documented, names both replacement gates) | prose-keyed lint is gameable; withdrawal precedent exists |
| D5 DOC-27 runbook | **DO** 9 lines→/var/lib/tether/backups + off-node caveat + drill 50 arm C + weilandserver run + registry flip | off-node caveat (backup on data volume dies with it) |
| D6 H2 N≥3 drill | **RECORD-ONLY** (coverage-boundary §3) | per-broker is the v1 contract |

---

## 1. Lane A — concurrency / raft (internal/cluster, internal/broker)

### A1 — `Node.CaughtUp()` freshness bound
- **New** `internal/cluster/read.go` after `RaftAppliedIndex` (:114): `func (n *Node) CaughtUp() bool { commit := n.CommitIndex(); return commit > 0 && n.RaftAppliedIndex() >= commit }`.
  - Comment MUST be honest: (a) "synced with a leader at least once (commit>0)"; (b) the `>=` half means **dispatched to the FSM**, not durably applied to SQLite (raft `AppliedIndex` advances when a batch is enqueued to `fsmMutateCh`, before `fsm.Apply` returns) — so a committed-but-not-yet-SQLite-applied tail (up to ~the FSM dispatch-channel capacity) still reads caught-up — a RECORDED residual (A2's Barrier was REJECTED, see A2, because raft `Barrier().Error()` has no apply deadline and hangs a wedged FSM); (c) the boot-zero case is really the **snapshot-restart** case (applied=S>0, commit=0 until first post-restart commit) as well as the never-elected case; (d) HONEST LIMIT: an islanded follower's commit froze at a positive value and applied catches it → still true; fenced by the write path (a quorum-less Propose can't commit; a JS delete fails once the colocated JS meta loses quorum), not by this predicate.
- **Edit** `internal/broker/clusterwrite.go:509` (reaperMayDelete) and `:543` (reaperCaughtUp): `RaftAppliedIndex() >= CommitIndex()` → `b.cl.node.CaughtUp()`. Applying to **both** is a documented judgment extension (the reviewer named only `reaperCaughtUp`; symmetric + zero-cost + the snapshot-restart boot window also hits `reaperMayDelete`'s `audit.go:191` boot orphan reap). Update the two "Raft-free (L-2)" accessor lists (:493-494, :535) to name `CaughtUp`, and amend the split-view argument (:529-533) with the frozen-commit caveat + write-path backstop.
- **Tests** (`internal/cluster/reaper_predicate_test.go`):
  1. `TestCaughtUpRequiresFirstLeaderSync`: 2-voter bootstrap whose peer inmem transport is never `Connect`ed (can never win) → `CommitIndex()==0` forever; assert `!n.CaughtUp()` AND assert the OLD bare predicate `RaftAppliedIndex()>=CommitIndex()` IS true (vacuity/discrimination guard). Mutation: drop `commit>0` → red.
  2. Extend `TestRaftAppliedIndexCatchesUpToCommitOnLeader`: also assert `n.CaughtUp()` in the caught-up branch (ties the hand-transcribed predicate to the shipped method — no drift).
  3. `TestCaughtUpIslandedFollowerFrozenCommitResidual` (honesty pin): 2-node cluster, propose once (commit>0 everywhere), Shutdown leader, wait survivor `!IsLeader()`, assert `CaughtUp()==true` with a comment marking it a KNOWN residual (flips deliberately if a leader-contact bound is ever added).
- **N=1 force-single safety**: commit index is monotone in-process; only the pre-first-commit instant is suppressed; xfer pass is 5 min ≫ election ms. `reaper_predicate_test.go:24` already polls with the `>0` bound and passes.

### A2 — M-2 lagging-leader: RECORD-ONLY (both drafted approaches proved unsound)
- **Why not the drafted conn-hold behavioral test**: raft `AppliedIndex()` advances at **dispatch** (batch enqueued to the 128-deep `fsmMutateCh`), not at `fsm.Apply` return. Holding the sole SQLite write conn stalls `Apply` but NOT `RaftAppliedIndex`, so a lagging leader with `RaftAppliedIndex() < CommitIndex()` is unreachable that way. Verified against `hashicorp/raft@v1.7.3`.
- **Why the Barrier-before-clear was ALSO rejected (discovered during implementation)**: `raft.Barrier(timeout)` — its `timeout` bounds only ENQUEUE; `Barrier().Error()` then blocks until the barrier is APPLIED with **no deadline**. On a wedged/stalled FSM (or the test's held-conn scenario) it **hangs the reconcile goroutine indefinitely** (empirically confirmed — the prototype test hung). A primitive that can wedge the reconcile loop to close a µs-scale, already-backstopped residual violates security-pragmatism — strictly worse than the residual.
- **Decision**: keep `reaperCaughtUp()` as-is (it DOES close the reviewer's original concern — a just-elected leader replaying a LONG committed tail has `RaftAppliedIndex < CommitIndex` under dispatch-channel backpressure). Document the narrower single-committed-but-unapplied-renewal residual honestly in `reconcile_upgrade_lock.go` / `reconcile_grow_lock.go` / `Node.CaughtUp` (the µs–ms window, why a Barrier was rejected, and the backstops: leader-only pass + clear-Propose needs fresh quorum + orchestrator keeper latch). Keep the source-grep pin `TestMembershipLockReapsGateOnCatchUp` (sole coverage of both lock files). No DOA behavioral test shipped. The M-2 "behavioral test" ask is honestly answered as **infeasible-as-specced** (the residual is a µs-scale dispatch window that no hermetic fixture can build and no safe primitive can bound).

### A3 — M-3 grace wiring test
- `TestXferReapGraceIsWiredInProduction` beside `TestXferReapShieldsFreshObjects` (`reconcile_passes_test.go`): build via `New(...)`, assert `b.xferReapMinAge > 0` **first** (load-bearing — red under both "drop the New() wiring" and "zero the const"), then `== xferReapMinObjectAge`. `New` does not dial NATS (garbage URL fine).

### A4 — RESIDUAL-1 / F3: RECORD-ONLY
- No behavior change, no test. Extend the SCOPING comment in `clusterdrain.go:848-864` (~3 lines): name the residual (cross-leader clock rollback > create→migrate elapsed can age a still-stranded row out → fail-open) and record WHY the skew-subtraction is declined — subtracting `clockSkewTolerance` from `op.CreatedAt` re-admits every unconfirmed row rehomed within (CreatedAt−tol, CreatedAt) into `pendingRetireConvergence`, re-introducing the unrelated-stranded-row membership wedge the fixed origin exists to eliminate (back-to-back retires within tolerance would spuriously BLOCK); fixed origin remains strictly better than the discarded sliding window. Existing `TestPendingRetireConvergenceRecencyWindow` + fail-closed `rehomedRecently` stand.

---

## 2. Lane B — security / seam (cmd/tether). Order: **B3 → B1 → B2** (+ DOC-27 help).

### B3 — N-4c: thread `--nats-conf` through `applyClusterSeam`
- `cluster.go:913` `applyClusterSeam(configPath, dataDir, raftAddr, secretsDir, natsConfPath string)`; guard `natsConfPath==""` → error (no silent default). Use it at compare :930, error :935, seam write :938-939.
- `cluster init`: add `--nats-conf` (default `defaultNatsConfPath`); wire :771 `ConfPath`, :832 call, :848 `readClusterPublicIdentities`, :872 print the real value, and add `--conf <natsConfPath>` to the printed step-1 command at :858.
- `cluster_backup.go` `applyRestoreClusterSeam(..., natsConfPath)`: pass at :136/:195; print `natsConfPath` at :203.
- **Idempotency (no-thrash)**: pre-fix seams carry the default path → default-flag re-run compares equal → `(false,nil)`. `--nats-conf /custom` against an old default-path seam → the existing complete+matching rule HARD-errors naming have/want (loud, correct). `cluster add`'s `runSelfInit`/`renderJoinerClusteredConf` inherit the default (record-only comment; threading a custom path through join bundles is scope creep).
- **Tests**: `TestApplyClusterSeamThreadsNatsConfPath` (fresh+/custom→NatsConfPath=="/custom"; re-run→(false,nil); pre-fix seam+default→(false,nil); pre-fix seam+/custom→error naming have/want); `TestRestoreSeamHonorsNatsConfFlag`. Update all **8** existing call sites (`cluster_seam_test.go:21,46,60`; `g4_external_review_fixes_test.go:47,108,124,132`; `g4_external_review_test.go:19`) + 2 production sites.

### B1 — N-3: issuer-verification fail-open → loud, never refuse
- `cluster_secrets.go`: add `IssuerUnverified bool` + `IssuerUnverifiedReason string` to `clusterPublicIdentities`. Capture the discarded Preflight error (:50-52). Split the `default:` case (:61-62): still substitute `AccountIssuer=acct`, but when `confIssuer==""` set `IssuerUnverified=true` + a reason (Preflight error vs "no auth_callout issuer rendered yet (pre-cutover conf)"). Note text is a SEPARATE sentence ("account-issuer substituted UNVERIFIED — <reason>; verify with `cluster doctor` once the conf is rendered.") — must NOT reuse the "NOT auto-substituted" wording. **No `BrokerUnverified`** (multi-user conf → confBroker=="" is documented-normal; would be a permanent false alarm).
- `clusterAuthIssuerSkewChecks`: append an `auth-issuer-unverified` **ADVISORY** DoctorCheck when `IssuerUnverified` (NOT Fatal — a stock pre-init box would else fail doctor on its mainline). Update the `:102-103` doc comment to state the deliberate error-vs-checks asymmetry (unverified is loud-advisory, not a hard error, to avoid breaking init/restore mainline).
- `cluster_reconcile.go`: add helper `clusterIssuerUnverifiedReason(secretsDir, confPath)`; at :92 and :116 print a stderr WARNING when unverified; change the :119 converged line to append "(issuer verification SKIPPED — see warning above)" while KEEPING the leading `all voters converged` substring (drill greps). `clusterAuthIssuerSkewError` untouched (stays skew-only).
- `cluster.go:860-863`: print "verified against nats.conf" ONLY when `!ids.IssuerUnverified`; else print `ids.Note` (closes the current false claim on stock confs).
- **Tests** (extend `r11_issuer_skew_test.go`, `skewFixture`): `TestReadClusterPublicIdentitiesUnverifiedIssuer` (missing conf / stock-no-authorization conf / matching conf → substituted+Unverified / substituted+Unverified / clear; **plus** matching-issuer multi-user conf `confBroker=="" ` → zero advisory — pins no-BrokerUnverified, F5). `TestClusterDoctorAdvisoryWhenIssuerCheckSkipped` — **pin the row status directly via `clusterAuthIssuerSkewChecks` (returns Advisory row) + a --json summary assert that fatal count is unchanged**, NOT via full `Doctor()` (which FATALs on skewFixture's incomplete secret set — F1). `TestReconcileNatsWarnsNotRefusesOnSkippedVerification` (missing conf, no --wait → err==nil + stderr "verification SKIPPED"; control matching → no warning). `TestReconcileConvergedPrintIsQualifiedWhenUnverified` using the existing `fetchClusterStatusReport` stub (`cluster.go:268` + `stubFetch`, F2): converged report + missing conf + valid seeds → exit 0 + "all voters converged to topology generation" + the SKIPPED parenthetical + stderr warning.
- **ADJACENT (RECORD, not fix)**: the ONLINE doctor path (`cluster_natsconf.go:450-453`) never runs the skew/unverified checks. Fixing it in-phase would emit a permanent advisory on every custom-conf online doctor unless `--conf` is passed (noise regression). Record it as a follow-up (a note in this plan + a `// TODO(n3-online-doctor)` at the online branch); do NOT claim it closed.

### B2 — N-4b: honest fresh-box remediation
- `printRestoreNextSteps` (`cluster_backup.go:242-261`): capture the Preflight error; split the `default` branch by `os.Stat`: **exists-but-unreadable** → "cannot be taken over as-is (<perr>). Fix/replace it FIRST (restore from your config backup), THEN render"; **missing** → "MISSING (fresh DR box). The render below reads the existing conf (listen addr, JetStream store_dir), so it cannot run yet. FIRST create the base conf (restore from backup, or write the minimal stock host/port + jetstream.store_dir + websocket conf). Do NOT re-run install.sh now: it OVERWRITES broker.yaml and removes the broker.cluster seam this restore just applied. THEN render...". The existing command print (:260) becomes step 1b.
- **Tests** (extend `r10_restore_test.go`): `TestRestoreNextStepsFreshBoxIsAnHonestTwoStep` — fresh box → the "cannot run yet"/base-conf/install.sh-clobber text; then an EXECUTION-PREMISE pin (`natsconf.Preflight(b.natsConf)` on the missing conf MUST error) proving the printed render command is genuinely unrunnable on a fresh box. `TestRestoreNextStepsDistinguishBrokenConfFromMissing` (conf with `include` → "cannot be taken over as-is", not "MISSING"). Update the existing `B-fresh-box-no-conf` case's `wantWhy`.
  - **DEVIATION (recorded, Stage-C review E-4/F3)**: the plan's original F3 execution pin had a SECOND half — write the stock conf + `--nats-server <exit-0 stub>` → run the printed command → err==nil + standalone conf. Only the FIRST (unrunnable-premise) half shipped; the recipe-WORKS half is **not** shipped — it needs a full render fixture (real keypairs + a nats-server dry-run stub + JS store_dir harvest), disproportionate to the LOW finding whose critical half (the bug: the bare one-liner cannot run) IS pinned. Recorded here rather than dropped silently; a future hardening can add the render half.
- **Render-from-flags bootstrap = REJECTED/record-only** (the ClientListen/JS-disable refuses exist to block guessed defaults; runbook §5.2 install-first order already covers the real DR flow).

### DOC-27 CLI help (in the B batch)
- `cmd/tether/cluster_backup.go:33, :35, :92` Example strings still print `/var/backups/...` → change to `/var/lib/tether/backups/...` (same file lane B already edits; both critics flagged DOC-27 is not closed while `--help` teaches the broken path). D5's ledger close cites these.

---

## 3. Lane C — observability / config / CLI (internal/broker, internal/adminsock, internal/serveconf, internal/brokermetrics, cmd/tether). Sequence AFTER lane A's `transfer_reconcile.go` comment edits.

### C1 — N-5a: `admin events` truncation marker
- `internal/broker/admin.go` `adminEventsTail` → return `([]adminsock.AuditEntry, bool, error)`. `truncated=true` when breaking at `scanned>=eventsMaxScan` (:146) OR when a `GetMsg` error coincides with `ctx.Err()!=nil` (deadline mid-scan — break, stop burning budget); a benign retention gap (`ctx.Err()==nil`) stays `continue`. The `--since` cutoff break and the `len(out)>=n` break stay `truncated=false` (complete by definition).
- Wire: `adminsock/server.go:37` hook signature + `handleEvents` (:393-397) → `Response{..., Truncated: truncated}`; add `Truncated bool json:"truncated,omitempty"` to `Response` (`protocol.go:286`, additive+omitempty, no schema bump); `cmd/tether/admin.go` renderer prints "(truncated: only the newest N examined — scan cap or deadline)"; `cmd/tether/jsonout.go` `adminEventsJSON` gains `Truncated bool`. Update the two func-literal mocks in `adminsock/events_test.go:24,49`.
- **Tests**: `TestEventsTruncatedFlagPropagates` (mock returns truncated=true → `resp.Truncated` && `OK==true`, not an error). `TestAdminEventsTailTruncatesAtScanCap` (publish >5000 with a `--kind` matching none of the newest 5000 → truncated=true; no filter n=50 → false). `TestAdminEventsTailReportsDeadlineTruncation` (already-cancelled ctx → truncated=true). Mutations named per test.
- **Trap**: never emit a synthetic in-band "truncated" AuditEntry (pollutes `--json`); never make truncation an ERROR (breaks scripts on busy streams). `adminAuditTail` NOT touched (operator-bounded window; deliberate asymmetry, note in commit).

### C2 — N-5b: interval upper clamp (reject, not clamp)
- `serveconf.go`: after the `<1s` rejects in `XferReapIntervalDuration` (:190) and `DiskCheckIntervalDuration` (:252), add `if d > 24*time.Hour { return 0, fmt.Errorf(... "> 24h (an effectively-disabled reaper/monitor lets orphan objects/disk fill; must run at least daily)") }`. Also `ProcGCIntervalDuration` (:232) — same class (unbounded GC = unbounded row growth), same idiom, same file; closing 2 of 3 siblings is half-done. `ProcRetention` gets NO upper clamp (long retention is legitimate).
- 24h justification: the floor duty is "garbage/disk-pressure must not be immortal"; one run/day bounds worst-case lifetime to ~1 day; any legit slow-reap motive is satisfied far below 24h; the clamp catches "10000h=silently disabled", not taste.
- **Tests** (extend `serveconf_test.go`): per knob `"10000h"`→error, `"24h"`→passes, `"24h1s"`→error; one `Load()`-level case (yaml with `10000h` fails Load). Mutations: remove the clamp / `>=` instead of `>`.

### C3a — N-6 Threads comment sweep (comment-only)
- `runtime_introspect.go:45` inline + `:18-23` header + `adminsock/protocol.go:351-353` (RuntimeReport.Threads doc) + `docs/broker-ops.md` (threads description): correct to "OS threads **ever created** (threadcreate profile) — **monotone, never falls** as threads exit; a CLIMBING value signals thread churn; NOT a live thread count and NOT a goroutine proxy". Do NOT change the measurement.

### C3b — N-6 unreapable-bucket counter (observability-only gauge)
- New `atomic.Int64 xferUnreapableBuckets` on `Broker` (~`broker.go:482`). New helper `xferBucketOrphanedEverywhere(sid)` in `transfer_home.go` (reuse the nids collect-then-close discipline): true when the session has zero bound nodes OR its nodes resolve to ≥2 distinct homes / any unresolved (= no broker can ever reap it). New `xferBucketHasAgedObjects(ctx, name)` in `transfer_reconcile.go` (true iff ≥1 non-deleted object past the grace). In `reconcileXferObjects`, at the `!homeOwnsXferBucket` skip (:80): if `selfID!="" && xferBucketOrphanedEverywhere(sid) && xferBucketHasAgedObjects(...)` → `unreapable++` + a per-pass `Warn`. `Store(unreapable)` at pass end. Surface via `brokermetrics.Snapshot.XferUnreapableBuckets` (metrics.go + Render emit-0-never-omit) + `metrics_wire.go` `metricsSnapshot` cluster branch. **Zero reap-behavior change.**
- Predicate precision (avoid noise): counting every non-home skip is ~always nonzero on a healthy N-broker cluster → useless. Only "unreapable by EVERY broker AND holding aged garbage" is signal.
- **Mandate**: raises visibility of the #58/racknerd class; MUST NOT be cited to soften drill 96 `nc_gap` or #58's ledger status (D1/N-6 wording coordination).
- **Tests** (`reconcile_passes_test.go`, cluster-mode `selfID!=""`): split-home + orphan → counts; single-home-elsewhere + orphan → NOT counted (noise guard; mutation: replace predicate with plain not-home → red); zero-node → counts; split-home empty bucket → NOT counted (mutation: drop aged-objects check → red); fresh object in split-home (Now=time.Now, grace=1h) → NOT counted; gauge falls after topology heals (Store not Add). `brokermetrics` Render test (ClusterMode gauge present at 0; single mode omits).

### C4 — N-9 attempts-map live-set prune
- `home_delivery.go`: `homeDeliveryTargets` returns the already-computed `seen` set too (`([]homeDeliveryTarget, map[int]struct{}, map[string]struct{}, error)`) — same `sid+"|"+nid` key, no drift. New `pruneHomeAttempts(liveKeys)` beside `pruneHomeApplied` (delete `attempts` keys not in `liveKeys`, under `hd.mu`). Call it in `reconcileHomeDelivery` next to the two existing prunes. **Live-set, NOT TTL** (a TTL on `nextAt` would evict a still-live node sitting in max backoff — reset its earned backoff → push-storm).
- **Tests** (`r8_home_delivery_test.go`): `TestHomeDeliveryPrunesAttemptsForDepartedTargets` (ghost `attempts["gone|gone"]` + a live target's real entry → pass prunes ghost, keeps live). Backoff-preservation guard (a live target mid-backoff, not due this tick, survives with backoff/nextAt unmutated — mutation: prune "not pushed this tick" instead of "not enumerated" → red). Unconverged-then-released end-to-end (delete the `port_allocations` row mid-backoff → next pass empties `attempts`). `-race`; leak is map-growth (the direct map assertions ARE the leak test; NumGoroutine/fd gate can't see it).

---

## 4. Lane D — deploy-tier harness / registry / docs (test/simcluster, docs)

### D1 — drill 96 registry sync (doc-lag; drill code already correct at `96-mid-flight-chaos.sh:447`)
- `expected-verdicts.tsv:38` owner cell: `#58 false-REGRESSION now runtime-guard` → `#58-split-home reclassified runtime-guard→gap (DEFECT-tied: homeOwnsXferBucket requires ALL session nodes homed to one broker, structurally false on the split-home session; retires at per-transfer-owner refinement — r15-finalization §8)`. **Keep the literal `#58` token** (crosscheck pins `#58` solely via this cell). Note cell: (a) add `#58 split-home (drill :447 — deterministic structural cause, defect-tied)` to the `Reclassified runtime-guard→gap:` list; (b) retie `nc_guard=0` to "#58-split-home now counts under nc_gap (nc_gap=5 nc_guard=0)"; **(c) PURGE the now-contradictory `#58 orphaned-object relapse PINS LIVE = PRODUCT-RED (454 orphans > baseline 1)` sentence** (the current PRODUCT-RED driver is #57 per followups §1 — leaving it makes the row say both runtime-guard and PINS-LIVE).
- `r15-finalization.md` (dated report — append bracketed corrections, don't rewrite): `:94` `[POST-EXTERNAL-REVIEW 2026-07-21: reclassified runtime-guard→DEFECT-tied gap, server-measured nc_guard=0; split-home cause deterministic, retires with per-transfer-owner refinement]`; `:65` `[update: landing = PRODUCT-RED nc_gap=5 nc_guard=0]`; `:29` `[verified: A2 8186→2 single-home; split-home residual = gap, §8]`.
- **Test**: `sh tests/ledger-crosscheck.sh` stays OK (the `#58` token loss is the mutation guard). No drill code change → kept-sites unchanged.

### D2 — drill 30 HALT-window adjudication (scene-capture + weilandserver experiment)
- **(a) Scene-capture (observation-only, predicate untouched, MANDATE-clean)**: new host-side `_start_scene_watcher <tag>` next to `_start_write_probe` (:171): every 0.5s grep `/tmp/wp-<tag>.log` for the failure signatures; on FIRST hit capture (host file) UTC ts + matched line + `sim_leader` + per-broker `cluster status` + `journalctl -u tether-broker -n 80` + `systemctl show ...MainPID,ExecMainStartTimestamp` (all best-effort — a `cluster status` that itself fails mid-roll is evidence, not an error). `_stop_write_probe` (:191) AND the non-HALT else-branch (~:390) kill the watcher. `_probe_clean` failure replays the scene into drill output. Plain `while` (no `poll_until` reentrancy). **Commit the watcher permanently** (standing forensic asset).
- **(b) Experiment (weilandserver, per `docs/devices-ops.local.md §6`)**: ① 3× serial `./remote.sh drill 30` → expect CLEAN + empty scene. ② 3× `run-drills.sh -j3` on subset {30, 96, 51} → reproduce the transient with a scene each. NOT the full 37-suite.
- **(c) Decision tree — numbered `### #NN` gotcha IDs in ALL defect branches (crosscheck-visible; `#ROLL-WRITE-WINDOW` is NOT enforceable)**, none lands silently GREEN:
  1. Leader transition coincides in PHASE-1 (follower-hop) → real product defect: new `### #NN — follower reload triggers leader election during roll` in `deploy-tier-gotchas.md`; tsv 30 → `PRODUCT-RED` owner `#NN`.
  2. Leader handoff PHASE-2 only (leader reload) → bounded write-unavailability window under contention: new numbered `### #NN — leader-reload write-availability window (roll)`; tsv 30 stays `INCOMPLETE`, owner append `+ #NN p2 leader-hop band (serial=CLEAN authority; scene-captured <date>)`; predicate at :387 stays strict.
  3. No leader change, hits are `no.responder`/timeout with journal IO-stall → parallel-sweep infra artifact: tsv 30 note append `intermittent -j3 band = host IO-contention infra-flake (scene-verified leader stable <date>); authority = serial/-j2`; finalization §9.2 `[RESOLVED: scene-verified infra]`. Not a predicate change, not GREEN (expected stays INCOMPLETE for the structural b/c gaps).
  4. **Does-NOT-reproduce under ×3 targeted contention** → tsv 30 note append `not reproduced under targeted -j3 ×3 (<date>); watcher committed as PERMANENT instrumentation → any future band hit self-captures; band stays N1-owned, serial=authority`.
- **(d) 82/22 attribution (landable now, no experiment)**: `expected-verdicts.tsv:31` (82) owner append `+ intermittent C1-grow(N=2) ASSERT-FAIL band = #GROW-ONTO-RECOVERED family (R16; r15-finalization §9.1)`; **also** tsv:10 (22) note append the same band sentence (note-only, expected stays GREEN — half-registration is the disease D2 treats). `#GROW-ONTO-RECOVERED` is crosscheck-safe (doesn't match `#[0-9]+`; family pinned by 42/51 PRODUCT-RED).
- **Test**: mutation proof-run — inject `echo not_leader >> /tmp/wp-p1.log` mid-roll → drill RED at :355 + populated scene block (proves capture fires + predicate still bites). Serial run ① must stay CLEAN with the watcher active (perturbation guard).

### D3 — kept-sites provenance + ratchet + quote-mask + committed self-test
- **Ratchet** `tests/kept-sites.baseline.tsv` to current per-drill counts (sum **1398**, closing the 132-site silent-deletion headroom, e.g. drill 30 floor 16→63) + a provenance header reconciling 1247 (r1-plan.md:74, prose-only) / 1274 (r2-plan.md:82) / 1266 (first committed @ 55b1451, single-commit history → earlier states unreconstructable) / 1398 (re-derived @ HEAD, floor=live) + a POLICY line ("intentional lowering is a coverage TRADE — edit the row WITH a note").
- **Quote-mask** in `count_sites` between comment-strip and separator-gsub: `gsub(/\047[^\047]*\047|"(\\.|[^"\\])*"/, "Q", line)` — **`\047` literal, NOT a raw `'`** (raw `'` breaks the single-quoted awk program). Verified: identical per-drill counts on all 37 drills (any future divergence = a real miscount to investigate).
- **Committed self-test** (must-fix 5, unconditional) in `tests/` wired into `run-all.sh`: scratch fixture — a deleted `assert_ok` REDs `--check`; an `assert_ok` inside a quoted description does NOT move the count.
- `lint-install.sh` quote-awareness = **record-only** (loud failure mode; a correct fix needs opener-own-quote vs enclosing-quote position tracking — disproportionate). 3-line comment at `:47` naming the limitation + BOTH drift directions.
- **Sequencing**: regenerate the baseline **LAST** (after D5's arm-C `+1 assert_ok`).

### D4 — M-5 lint gate D: WITHDRAW (documented)
- Replace `lint-drills.sh:98-103` with an explicit withdrawal naming both replacement halves: REGISTERED-defect-in-GREEN-drill → `ledger-crosscheck.sh` (executing, fail-closed); UNREGISTERED-inverted-false-green → not statically checkable, owned by the Stage-C non-vacuity requirement (accepted residual, recorded so nobody re-invents a prose-keyed lint). Precedent: `ledger-crosscheck.sh:4-11` (a static sibling written, measured, withdrawn: 10 hits, 3 real). tsv/followups M-5 row gets "withdrawn, replacement named".

### D5 — DOC-27 runbook + drill 50 arm C + registry flip
- `docs/cluster-runbook.md` all 9 `/var/backups` (:147,149,579,583,603,605,635,668,701) → `/var/lib/tether/backups/...`. Add the **off-node caveat** next to :579: "Copy the bundle off-node immediately — a backup under /var/lib/tether lives on the data volume and dies with it (§7 disaster flow assumes the bundle survives the disk)". `broker-ops.md`: zero occurrences (no edit).
- Drill 50 arm C (`50-backup-restore.sh:240-251`): track the corrected example (`--out /var/lib/tether/backups/tether-$(date +%F)-$$`); flip to a POSITIVE regression: success → `assert_ok "DOC-27 CLOSED: the runbook §5 ONLINE-backup literal example runs as User=tether on a stock install"`; expected-perm-failure signature → `product_red "DOC-27 REGRESSION ..."`; other → `_as_fail`. Fix stale line refs.
- **Registry flip (ONE commit, after a weilandserver `./remote.sh drill 50` GREEN)**: `deploy-tier-gotchas.md:640 ### DOC-27` → CLOSED marker (within 3 lines of the heading) AND `expected-verdicts.tsv:18` row 50 → GREEN owner `-` note with close evidence. Flipping tsv before the ledger close = crosscheck red (that's the gate working; don't split).
- **Test**: the 3-branch signature guard IS the adversarial test (mutation: revert arm-C path → PRODUCT-RED). Deploy-tier run in-scope (doc/deploy face touched).

### D6 — H2 N≥3 drill: RECORD-ONLY
- `docs/reviews/simcluster-coverage-boundary.md §3`: `### H2 · N≥3 分布式 PIN 限速 drill — DEFERRED (v2 acceptance)` (per-broker is the v1 contract, architecture §E.6; becomes the acceptance gate if/when cluster-consistent limiting ships). followups §6.4 H2 row gets `[RECORDED → coverage-boundary §3]`. No tsv row (no drill exists).

---

## 5. Sequencing, hard gate, and deploy-tier

1. **Implement** (single continuous block, §3 step 3), coordinating the two shared files (I am the sole merger): lane A `clusterwrite.go`/`read.go`/lock-reaps/comments + `transfer_reconcile.go` comments → then lane C `transfer_reconcile.go` counter; lane B `cmd/tether` (B3→B1→B2 + DOC-27 help); lane D docs/simcluster (D1/D3-header/D4/D6/82+22 now; D2/D5 code now).
2. **Targeted tests as I go** (§5 discipline): `go test -race ./internal/cluster/ ./internal/broker/`, `go test ./internal/serveconf/ ./internal/adminsock/ ./internal/brokermetrics/ ./cmd/tether/`, `go test ./test/p9/...`; `sh test/simcluster/tests/run-all.sh` (kept-sites/lint/crosscheck, local, no Docker).
3. **Deploy-tier (weilandserver via remote.sh — doc/deploy face touched)**: D2 drill-30 experiment (serial ×3 + -j3 subset ×3, scene-capture) → adjudicate → register per the decision tree; D5 drill-50 GREEN → registry flip. D3 baseline regenerated LAST (after D5 arm-C).
   - **✅ DONE (2026-07-21, weilandserver reachable again — image rebuilt with the new binary, tree rsynced)**:
     - **D5 drill-50 = GREEN** (pass=87, 0 gaps): the `DOC-27 CLOSED: …runs as User=tether` arm-C positive regression PASSED. → **registry flipped in one edit**: `expected-verdicts.tsv` row 50 → GREEN owner `-`; `deploy-tier-gotchas.md` DOC-27 → CLOSED. `ledger-crosscheck` OK (DOC-27 dropped from the open set; 10→now includes #66 below).
     - **D2 drill-30 adjudicated** (serial ×2 + -j3 ×1 = **1 FIRE / 2 CLEAN**): PHASE-1 CONTINUITY always clean; PHASE-2 intermittently RED with a `WRITEFAIL`. The scene-capture watcher (my BLOCKER fix **verified on the deploy tier** — probe started without hanging on EVERY run) captured the failure instant on serial #1: leader **transitioned brk1→brk2**, all 3 brokers VOTER/LAG=0/STREAMS 3/3/TOPO✓/reachable — a healthy re-converged cluster, NOT infra/wedge/IO-stall. **Verdict: decision-tree branch 2** — a bounded **phase-2 leader-hop write-availability window** (leader-last roll re-execs the leader → raft restart → step-down → re-election; a `session create` landing in that ~sub-second window fails, and the CLI does not auto-retry, architecture C.3). Intermittent (probe-timing dependent), fires in serial too (NOT a parallel artifact). → **Registered `### #66`** in `deploy-tier-gotchas.md` (LIVE-CONFIRMED, scene-proven; candidate remedy = bounded not_leader retry in the CLI raft-write path, product work for a later phase) + `expected-verdicts.tsv` row 30 owner append `+ #66 p2 leader-hop write-availability window`. **Predicate stays STRICT** (mandate: the gap carries the truth, not a loosened grep). `ledger-crosscheck` OK (#66 pinned by row 30).
     - **D1 validated**: drill-96 (serial) fires the `#58-SPLIT-HOME RESIDUAL (a DEFECT-tied gap … reclassified runtime-guard→gap)` exactly as the synced ledger says (count 436 > floor, structurally split-home). 96's overall verdict was ASSERT-FAIL from a SEPARATE intermittent flake in the D3/Q4 majority-survivor-write arm (the pre-existing "96 unstable band", NOT this phase's subject and NOT touched).
     - **51 confirmed PRODUCT-RED** (grow-onto-recovered R16 family) — matches the registry.
4. **Hard gate (pre-commit)**: `make test` + `make e2e` + `make lint` all green; `-race` + built-in NumGoroutine/fd leak gate for the concurrency-touching changes (A2 test, C4). Any not-green ≠ done.
5. **Stage C**: adversarial multi-expert review Workflow (Fable 5 per the exception) → main-process fix/integrate → **STOP at the external-review gate (step 6)**.

### 5.1 Stage-C review outcome (5 lens reviewers + 5 cross verifiers, Fable 5 — DONE)
All 5 lanes returned **APPROVE-WITH-CHANGES** (no lane REWORK; no shipped-behavior defect in A/B/C/D). Adopted fixes:
- **BLOCKER (E-1/D-1) — FIXED + hermetically verified**: the D2 scene watcher `( ) &` inherited `assert_ok`/`_as_capture`'s `$(...)` pipe write-end → the command substitution blocked on EOF → drill 30 would WEDGE at "start probe" on EVERY run. Fix: detached the watcher stdio (`</dev/null >/dev/null 2>&1 &`) + bounded the loop (~20min) + moved `_replay_scene` to the drill body (uncaptured) + per-broker scene capture + `$LDR` label. Verified with a docker-free stub: broken shape hangs, fixed shape returns promptly.
- **MEDIUM — closed**: C3b gauge Store-not-Add + heal-to-1 test (kills Add mutation); brokermetrics Render test (cluster gauge present=2, single-mode forbidden); N-5a ctx-deadline branch — corrected the false "pre-cancelled ctx" comment + recorded the mid-scan test as infeasible-as-specced (mirrors A2); B2 execution-recipe-works half recorded as a deviation (the unrunnable-premise half IS pinned).
- **LOW/NIT — closed**: `transfer_reconcile.go` reaperCaughtUp comment (CaughtUp not the bare predicate); `clusterwrite.go:493-494` stale duplicate L-2 accessor list removed; M-2 residual comment reworded (≤~8192-entry dispatch tail, not "single entry"); plan §1-A2 self-contradiction (Barrier) fixed; kept-sites-selftest now pins BOTH quote-mask arms (single + double); M-5 followups annotation + DOC-27 gotcha stale-while-OPEN note (crosscheck-safe, no auto-close token); drill-50 install.sh:491→:500 line-refs; CJK comment tags anglicized (`lock-reap`, tidy-it-up); gauge-staleness comment; N-5b field-doc ceiling; restore/join-flow `--nats-conf` notes; gofmt-clean.
- **Verifier note**: the Verify phase received un-interpolated `${r.text}` placeholders (a script backtick-escaping bug), so verifiers did INDEPENDENT audits — which corroborated the reviews finding-for-finding (no contradiction), so the panel's conclusions stand.

**Post-fix hard gate re-confirmed**: `make lint` 0 · `make test` rc=0 (0 FAIL) · `-race`+leak gate rc=0 · `make e2e` rc=0 (614s, behavior unchanged since — comments/tests only) · `run-all.sh` ALL PASS (incl. ledger-crosscheck: DOC-27 stays OPEN, 10 pinned; kept-sites-selftest).

**Single commit at step 7** (after external review passes), author = user only, no AI attribution. Do not split (D5's ledger+tsv must be atomic).

## 6. Cross-cutting guards carried from the adversarial review
- No file collisions across lanes except the two coordinated shared files (`transfer_reconcile.go` A-comments before C-counter; `cluster_backup.go` B owns, D5 cites the help edits).
- `ledger-crosscheck.sh` token discipline: every owner-cell reword keeps/creates a `#NN|DOC-NN` on non-GREEN rows; re-run `tests/run-all.sh` after any wording change.
- N-6 counter (C) and #58 registration (D1) describe the same stranded-bucket family — the counter is *added visibility*, never a retirement of the gap.
- A2's Barrier design + A2 fixture's raft-pool assumptions are the highest-risk technical items — verify raft semantics before committing; fall back to record-only if unsafe.
- B1's new ADVISORY row: grep drills 20/50/51/52 for doctor `0 fatal`/advisory-count assumptions before the drill-50 gating run (which doubles as B's deploy-face gate).
