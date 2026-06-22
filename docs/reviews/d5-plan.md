# D5 Plan — Re-derivable Audit Publish (single-writer + dedup + post-election sweep) + JS per-session Stream Replica Reconfig

> **Plan-of-record.** Stage A (CLAUDE.md §3): 5 adversarial drafters → 5 adversarial critics → 1 synthesizer (workflow `wf_4a8fc738-4d6`, 11× Opus 4.8, ~988k tok), then **finalized by the main process** (sole definer). The synthesis candidate is adopted with the §0bis main-process rulings layered on top (6 open-question resolutions + the threading/config divergences). **Every load-bearing repo/dep fact was re-verified firsthand before adoption** (see Verification basis); the experts' cited line numbers all checked out.

D5 closes the §19-D5 exit: any new leader re-derives committed-but-unpublished audit from the **replicated** log (not the dead leader's memory) and re-publishes it idempotently; per-session JS streams reconfigure their replica factor toward `replicasFor(nVoters)` and a canonical `AllAtTarget` predicate gates D7's retire. **Two orthogonal mechanisms**, all **build-and-prove** (cutover = D9), `cmd/tether/serve.go` byte-untouched, production `Broker` constructs no `cluster.Node` and never starts the publisher loop.

---

## Verification basis (re-read live before finalizing)

`internal/cluster/{fsm,node,command,clustermeta,read}.go`; `internal/proc/plan.go`; `internal/broker/{broker,audit,exec,reconcile,transfer,cluster_forward,testhooks}.go`; `internal/jsstream/jsstream.go`; `internal/proto/subjects.go`; `internal/schema/audit.go`; `test/d4/{setup,forward,regression}_test.go`; `test/e2e/all_phases_test.go`; architecture §4.1/§4.4/§5/§6.3/§6.4/§9/§16/§17/§19. Dep facts (Go module cache):

- **`func (r *Raft) CommitIndex() uint64` IS exported** — `hashicorp/raft@v1.7.3/api.go:1234` (also `LastIndex` :1227, `AppliedIndex` :1245, `GetConfiguration` :897, `Suffrage==Voter` configuration.go:15). **R-4 is sound; the drafts that claimed CommitIndex was unexported were factually wrong.**
- **`DefaultConfig()`: `TrailingLogs=10240`, `SnapshotThreshold=8192`, `SnapshotInterval=120s`** (config.go). `raftConfig` (node.go:216) overrides only the four timeouts, so these stay at defaults → R-6/R-7 reference real values.
- **`metaTestKeyPrefix="t:"`** (command.go:46); `newClusterMetaSet` hard-rejects any non-`t:` key (clustermeta.go:40-41) → `ApplyMetaSet("audit_published_index",…)` proposes nothing → **R-2 (new op) is mandatory.**
- **`ReconcileReqID = hex(SHA256("reconcile"||sid||nid||bootID||digest(sorted procs++ports)))[:32]`** (cluster_forward.go:166-170); the comment at :161 says bootID "protects D5's future audit-publish from re-sending the same register" → **R-10 reqID-keyed dedup is exactly what D4 staged.**
- **`Node.LeaderContactStale(now)`** stateless, no goroutine (read.go:41); house style explicitly rejected a `LeaderCh` goroutine (read.go:30) → **R-13/R-15 sound.**
- **nats.go@v1.52.0**: `StreamConfig.Duplicates time.Duration`; `jetstream.WithMsgID(id) PublishOpt`; `UpdateStream(ctx,cfg)` + `UpdateObjectStore(ctx,cfg)` exist; `ClusterInfo{Leader string; Replicas []*PeerInfo}`, `PeerInfo{Name string; Current bool; Offline bool; Active; Lag}`; `StreamInfo.Cluster *ClusterInfo` → **R-17/R-20 `metaGroupCanHost`/`actual` computation sound.** Broker is a JS **client** (no `*server.Server`), so the only client-visible peer signal is `StreamInfo.Cluster` — confirmed.
- Three `Replicas:1` literals: `jsstream.go:74` (events), `jsstream.go:103` (history), `transfer.go:202` (object store). Five live call sites to thread: `EnsureHistoryStream` at `audit.go:151`+`sessions.go:68`; `EnsureEventsStream` at `broker.go:543`; `ensureXferBucket` at `transfer.go:375/475`.
- `PlanReconcileBatch` sets `Aux` independent of `Body` (proc/plan.go:152-203) → an audit-only reconcile (empty Body, non-empty Aux) commits an entry precisely so D5 can replay it (R-9/R-11). `ReplayReconcileAudit` (proc/plan.go:214) is pure, threads no raftIndex, computes no dedup key ("that is D5") — consumed UNCHANGED.
- No persistent audit table (audit lives only in JS `history-<sid>`); `schema.AuditSchemaVersion=1`. `publishAudit` (broker.go:305) routes JS-or-core, sets **no** Nats-Msg-Id, has the `auditTapForTest` seam.

---

## §0 RULINGS (adopted from synthesis; main-process edits flagged inline)

### Checkpoint mechanism

**R-1 — The publish checkpoint is a REPLICATED raft write, not leader-local.** A leader-local cursor is the dead leader's memory; a new leader reads 0, defeating §6.3 re-derivability and forcing an unbounded scan every election, and it races the FSM on the `SetMaxOpenConns(1)` pool. Rides the existing `cluster_meta` table (no migration), advanced via raft.

**R-2 — A NEW op `OpAuditCheckpointSet` writes the checkpoint; do NOT reuse `ApplyMetaSet`.** The `t:` prefix guard (clustermeta.go:40) rejects `audit_published_index`. The new op rides `genericExecApplier` with FSM-baked monotonic-guard SQL. Driven through `Propose` (leader-gated, node.go:339), not `Apply` (ungated, node.go:265).

**R-3 — Monotonicity enforced in the FSM-baked SQL, not leader-side `max`.** A deposed-but-in-lease leader computing `max(old,new)` against its stale local view could blind-UPSERT a lower literal onto every replica. The guard is unconditional, in the baked statement:
```sql
INSERT INTO cluster_meta(key,value) VALUES('audit_published_index', <litUint>)
ON CONFLICT(key) DO UPDATE SET value = excluded.value
  WHERE CAST(cluster_meta.value AS INTEGER) < CAST(excluded.value AS INTEGER)
```
First write = INSERT branch; regress = conflict-update whose WHERE is false → 0 rows, **no error** (`genericExecApplier` must not assert rows-affected — confirmed D2 behavior). Value baked as a validated decimal-uint literal (no `Args`; D2 sqlbake NUL/UTF-8 rules; reuse `sqlbake.LitInt`).

**R-4 — Publish ceiling = `Node.CommitIndex()` (exported, verified), not `AppliedIndex()` and never `LastIndex()`.** A just-elected leader's `AppliedIndex` lags `CommitIndex` during FSM catch-up → an `AppliedIndex` ceiling needlessly delays the sweep. Safety: a committed entry is durable on a majority and present in the leader's local log, so `GetLog(idx)` for `idx ≤ CommitIndex` succeeds unless snapshot-truncated (R-6); we never publish an uncommitted `LastIndex` tail a new leader could overwrite. Audit derives purely from the committed entry's bytes, not a locally-applied SQL row, so `AppliedIndex` is not needed for durability. Adds the one-line primitive `Node.CommitIndex()`.

**R-5 — Checkpoint advance is BATCHED (one raft write per drained batch, cap 256) and SKIPPED when nothing advanced (`highWater == cursor`).** Per-entry raft writes self-DoS a large post-election sweep and inflate the log → faster snapshots → worsen the truncation race. An **idle loop issues ZERO raft writes** (an FSM no-op still costs a committed entry). The checkpoint op derives zero audit (R-9 skip-set) so it never begets more checkpoints.

### Publisher loop + dedup id

**R-6 — Sweep floor = `max(replicatedCheckpoint, LogFirstIndex())`, re-clamped each iteration; a truncated unpublished entry is a LOUD accepted-loss, never a silent skip.** The snapshot is a full SQLite online-backup, so `cluster_meta` (the checkpoint) survives snapshot+restore, but log entries it points past are **gone** when `LogFirstIndex()` advances. "Truncated ⟹ already published" is FALSE (a slow publisher + a commit burst past `SnapshotThreshold` truncates unpublished audit). The truncation branch must: (a) not panic (typed `ErrLogTruncated`), (b) surface the loss via a counter + structured log (the §16/§17 bounded-approximate-loss boundary), (c) **advance the checkpoint past the gap so the loop does not wedge/livelock**, (d) keep publishing entries `> LogFirstIndex()`. Re-clamped per iteration to handle a snapshot firing mid-loop.

**R-7 — Asserted invariant `CommitIndex() - checkpoint < TrailingLogs` (10240, with margin).** Back-pressure is via the predicate path, NOT by blocking raft's snapshot loop (cannot be done cleanly; arguably D9 — see OQ-2). The bound makes the approximate-loss "bounded by snapshot cadence," not unbounded. The assertion reads the real `raft.DefaultConfig().TrailingLogs` by reference.

**R-8 — Dedup id grammar `r<raftIndex>:<kind>:<seq>` with `kind ∈ {proc,port}` ONLY.** `seq` = 0-based position in `ReplayReconcileAudit`'s returned slice (total-ordered by `(PID,Kind)` for proc, `(Port,LocalPort,Name)` for port — verified deterministic). Two leaders decoding the same committed bytes → identical slices → identical ids. `kind` MUST partition proc/port distinctly (a coarse `kind="audit"` collides `(idx,audit,0)` between a proc and a port record → silent loss). Grammar is **closed** for D5: `call`/`event` are NOT replayable (R-11), no dedup grammar until D8+. One `aux.SID` per `ReconcileBatch` → all records share one `history-<sid>` stream → no cross-session seq collision.

**R-9 — The publisher derives audit by OP TYPE (`cmd.Op == OpReconcileBatch`), never by Body-emptiness; skips `OpAuditCheckpointSet` and all non-reconcile ops.** An audit-only reconcile commits an empty-Body entry with non-empty Aux precisely so D5 replays it; a Body-gated publisher loses orphan-kill audit. Op-based skip-set; `OpAuditCheckpointSet` derives zero audit (prevents the checkpoint-begets-checkpoint loop).

**R-10 — Forwarded-reconcile double-publish via distinct raft indices → RESOLVED (the bug none of the 5 drafts caught).** A D4 cross-retry of the same reconcile reuses the deterministic `ReconcileReqID`; an ack-lost retry commits at a **new** raft index but hits `appliedDedup` (op SKIPPED, applied_index advanced, NOT a rollback). The dedup'd entry **still carries Aux**, so a raft-index-keyed publisher would re-derive it with a different `r<newIdx>:proc:0` id → **double-publish** (JS only collapses identical ids). **Resolution:** for `OpReconcileBatch` entries carrying a non-empty `cmd.ReqID`, the dedup id keys on the **ReqID** (cross-retry-stable), grammar `q<reqID>:<kind>:<seq>`; original commit and dedup'd retry then emit **identical** ids → JS collapses. Empty-ReqID reconcile (not expected for forwarded reconcile; defensive) falls back to `r<raftIndex>:…`. Pinned by E-A8; recorded in doc D-1.

**R-11 — Replayable surface is EXACTLY `OpReconcileBatch`.** `audit.call` (exec), inline `audit.proc {start,exit}`, `audit.port` live-mutator, and `sys.events` stay best-effort leader-local and are NOT replayed (the §2.3/§17 honesty caveat made mechanical). The live inline paths continue via the byte-unchanged `publishAudit`.

### Single-writer / lifecycle / split-brain

**R-12 — Single-writer = "identical-id collapse across an election," NOT "exactly one process ever publishes."** Across an election two leaders may briefly both publish; because the id is a pure function of the committed entry (R-8/R-10) they emit byte-identical ids that JS collapses. Followers emit nothing — the publisher loop is structurally absent on them (it lives in `internal/broker`, unreachable from the FSM; L-2).

**R-13 — ONE long-lived broker-owned goroutine that polls `node.IsLeader()` each tick and no-ops when false. No `OnLeaderChange`, no per-flap spawn/join.** House style (read.go:30 rejected a `LeaderCh` goroutine; the D4 responder is one long-lived subscription with the leader-check inside the callback). One loop = constant goroutine count across N flaps = trivially passes the leak gate. A start/stop-per-flap design back-pressures `raft.LeaderCh` consumers when `stop()` joins a goroutine blocked on an in-flight publish.

**R-14 — The per-publish ctx is a CHILD of the loop ctx; every leak assertion follows `cancel()` + loop-done join.** An unparented publish ctx after leadership loss holds a reply-inbox subscription (an fd) for its full timeout, blowing the fd leak gate. `pubCtx, cancel := context.WithTimeout(loopCtx, publishTimeout)`; loop-ctx cancellation aborts in-flight publishes. `publishTimeout = 5s` (the d4 `ApplyTimeout`) under `-race` + JS cluster.

**R-15 — Publish gate = `IsLeader() && !LeaderContactStale(now)`; the `CommitIndex` ceiling is the load-bearing safety, the fence is hardening. NO leader-epoch header in the record body.** A deposed-but-in-lease leader keeps `IsLeader()==true` but cannot advance `CommitIndex()` (no quorum), so it only re-publishes already-committed entries the new leader also has → dedup-collapse. The stateless `LeaderContactStale` predicate is belt-and-suspenders. `raft_index` is already in the msg-id; adding node_id/term to the record **body** is a schema change breaking the byte-unchanged lock. Consumer-side stale-leader rejection is D7/D8.

### Mechanism B

**R-16 — `replicasFor(nVoters)`: `n≤1→1; n==2→2; n≥3→3`. Cap at 3 (NOT 5). Lives in `internal/jsstream` (pure `int→int`, no `internal/cluster` import).** Architecture pins R≤3. N=2→R2 is the only monotone choice (1→2→3, which D7's retire-gate wants) that also satisfies "never request more replicas than servers"; **and** it makes the kill-during-expand test's no-loss property reachable at the 2-voter waypoint (the data already lives on the 2nd node). Test-asserted properties (E-B1): monotone non-decreasing AND `replicasFor(n) ≤ n` AND cap 3. Honesty caveat registered in §17 (OQ-1): "RF2 = read-survivable, zero write fault-tolerance."

**R-17 — Target = `replicasFor(Node.NumVoters())` (raft voter count = authoritative intent); the `UpdateStream` is GATED on a client-visible `metaGroupCanHost(target)` readiness predicate, NOT on `replicasFor(nJSPeers)`.** The broker is a JS client without a `*server.Server`, so it cannot read `JetStreamClusterPeers()`; the only client-visible signal is per-stream `StreamInfo.Cluster`. Feeding `nJSPeers` into `replicasFor` conflates intent with readiness → target computed too low when JS-meta lags voter-join → never converges. Bootstrap (no stream yet at target): expand `events` first; treat an `UpdateStream` rejection ("no suitable peers"/"insufficient resources") as typed `ErrMetaGroupNotReady` retry — the retry-on-reject IS the gate; the per-stream `StreamInfo.Cluster` probe is post-hoc confirmation.

**R-18 — `ensureStream` already-in-use branch: raise-only (`current < target → UpdateStream`); never auto-shrink (shrink is D7 retire, gated). All THREE families reconfigured.** Verified three `Replicas:1` literals (events, history, object store — the last via `UpdateObjectStore`). All three reconfig; all three enumerated in `AllAtTarget` (else the retire gate false-greens on un-replicated tier-B).

**R-19 — D5 ships the `AllAtTarget`/`Degraded` PREDICATE only. NO alert-row write (D8b's writer), NO status CLI (D7).** `replication_degraded` is replication-store-backed (migration 0009 CHECK), its writer is D8b, it is NOT client-synthesized (the client-synthesized pair is `quorum_lost`/`force_single_active`). D5 delivers a pure predicate over the full enumerated stream set; D7 consumes `AllAtTarget` as its retire gate; D8b writes the alert row.

**R-20 — `AllAtTarget` = a CANONICAL single authoritative pass over the FULL set `{events} ∪ {history-<sid> ∀ active session} ∪ {OBJ_xfer-<sid>}`; fail-closed on any list/info error; `actual` excludes lagging (`!Current` or `Offline`) peers; empty set → false.** §6.4 forbids sampling. A sampler / a `Config.Replicas`(target)-instead-of-`Cluster.Replicas`(actual) read / a skipped erroring stream all false-green the retire gate → D7 retires a node a stream still needs → loss. `actual = 1 (leader) + count(p in Cluster.Replicas where p.Current && !p.Offline)`; a present-but-lagging peer counts as NOT actual (conservative). Empty (no sessions) → false (a fresh cluster must not permit retire). `listSIDs` reads the leader's local committed SQLite (authoritative for the session set; the loop is leader-only).

**R-21 — Mechanism A and B run in ONE merged leader-only goroutine (tick: gate → `publishOnce` drain → `reconcileOnce` pass); reconfig fan-out is bounded (`maxParallel=8`) and JOINED before the pass returns.** Two independent loops double the flap-leak surface and race on the same JS (publish to `history-<sid>` concurrent with `UpdateStream` on it). One goroutine = one lifecycle + a happens-before between a reconfig pass and the next publish drain. Fan-out workers (errgroup+semaphore) joined before the pass returns so they never outlive the loop.

**R-22 — During reconfig, audit QUEUES at the leader = the publish checkpoint does not advance past an un-ACKed publish; the queue-not-drop test asserts cursor-non-advance-during-stall (not just eventual-presence).** "All records eventually present" cannot distinguish queue-not-drop from drop-then-sweep-backfill. The non-vacuous assertion: `audit_published_index` stays put during the stall and advances after recovery, read via the checkpoint, not the stream depth.

### Guards / harness

**R-23 — THREE guard families ship as named deliverables: (1) D5 build-and-prove token-scan + self-check, (2) `internal/cluster` no-NATS import-set guard, (3) `internal/jsstream` no-`internal/cluster` import-set guard.** No import-graph guard exists today for either L-2 edge (`deps_test.go` is a compile-reference pin; `apply_reachability_test.go` is an Apply→mutator CHA lint). D5 is the first phase to put a `cluster.Node` reader + JS reconfig in one PR, so these are the most important new locks. "Live `publishAudit` byte-unchanged" is made structural by a **separate** `publishAuditWithID` method (live `publishAudit` gets zero diff); the token-scan asserts `serve.go`/live path never call the new symbols.

---

## §0bis MAIN-PROCESS FINALIZATION (open-question resolutions + divergences from synthesis)

### Open-question resolutions (I am the sole finalizer; external review may override)

- **OQ-1 `replicasFor(2)` → R2 (ADOPT).** R2 is monotone (1→2→3, what D7's retire wants) **and** strictly better for the kill-during-expand no-loss property: at the 2-voter waypoint the data already lives on the 2nd node, so killing the original survives for reads. Register the §17 honesty caveat "RF2 = read-survivable, zero write fault-tolerance"; external review can flip to R1 with a one-line change (it is an HA-honesty call, not an invariant).
- **OQ-2 snapshot back-pressure → ACCEPT + BOUND (ADOPT).** Assert `CommitIndex - checkpoint < TrailingLogs`, sweep-first on election, loud-loss on the rare truncation. Control-plane write rate is low + `TrailingLogs=10240` + 100ms poll ⇒ the publisher essentially never falls behind. Forcing raft snapshot back-pressure is fragile and arguably D9.
- **OQ-3 dedup-id helper placement → `internal/broker` (ADOPT).** R-10's reqID-keyed branch needs the forward-envelope ReqID (a broker/cluster_forward concept). `proc.ReplayReconcileAudit` stays pure + id-free; its `seq` ordering remains the SSOT for the seq value.
- **OQ-4 `metaGroupCanHost` API → `StreamInfo.Cluster` + retry-on-reject (ADOPT).** Only client-visible signal; no `$SYS…JSZ` probe (needs a broker ACL perm that does not exist → scope creep into D7). Classify the `UpdateStream` rejection → `ErrMetaGroupNotReady` during impl.
- **OQ-5 JS-cluster leak-gate → do NOT bump the global `+2`/`+4` tolerance (ADOPT).** Use the bracketing discipline (baseline AFTER JS + streams settle; assert only the loop-lifecycle delta). For follower-silent assertions prefer the deterministic `onPublish`-tap counter over goroutine-count.
- **OQ-6 object-store kill depth → ACCEPT Draft-3-Q5 (ADOPT).** The no-audit-loss-under-kill proof (E-J1) targets `history-<sid>`; OBJ_xfer gets E-B8 (expands to target) + predicate inclusion (E-B4). Tier-B no-loss-under-kill is out of D5.

### Main-process divergences from the synthesis (binding for Stage B)

- **MP-1 — Live stream callers pass the literal `1`; NO `b.node`/`b.targetReplicas()` field is added to the production `Broker`.** The synthesis put a `targetReplicas()` method on `Broker` reading `b.node` (returning 1 when nil). I diverge: keep the production `Broker` free of any `cluster.Node`-shaped field (cleaner build-and-prove; the guard test then need not police a latent field). The reconfig MECHANISM (`target = ReplicasFor(NumVoters())` + `UpdateStream`) is driven by the standalone `auditPublisher`/reconciler that holds the node directly and is **constructed only in `test/d5`**. The jsstream signatures still gain a `targetReplicas int` param; the five live call sites pass `1` (named `jsstream.ReplicasSingle = 1` with a comment). Production behavior byte-equivalent (R=1, actual==target==1, no `UpdateStream` fires).
- **MP-2 — `StreamConfig.Duplicates = AuditDedupWindow` is set UNCONDITIONALLY at stream creation (both R=1 and R>1), and is INERT in production.** The live `publishAudit` sets no Nats-Msg-Id, so a dedup window has no effect in production; it pre-stages the D9 cutover with one code path. For an EXISTING production stream it is never applied (already-in-use + target=1 → no `UpdateStream`); for a NEW one it is inert. Documented as behavior-preserving in D-1; the guard test ensures the live `publishAudit` is never changed to set a msg-id. *(If external review prefers strict live-stream-config-unchanged, gate `Duplicates` behind `targetReplicas > 1` — a one-line change.)*
- **MP-3 — `AuditDedupWindow = 2 * time.Minute`** is the SSOT constant in `internal/jsstream` shared by the stream `Duplicates` config and Mechanism A's window assertion. Rationale: it must exceed `(election + tail-drain)` worst case; with `MultinodeElectionTimeout=1s` + a bounded post-election sweep, 2m is ~100× margin. The assertion (E-A3) reads the election constant + the sweep bound **by reference**, not hardcoded on both sides.
- **MP-4 — Test consolidation.** The 30 enumerated cases (16 A + 9 B + 1 joint + 4 guard) are the coverage contract; where natural they collapse into table-driven subtests (e.g. E-B1 is one table). The non-vacuity hook of each is mandatory and survives consolidation.
- **MP-5 — `metaGroupCanHost` is NOT the raise pre-gate (Stage-B correction to synthesis R-17).** An R1 stream's `StreamInfo.Cluster` lists 0 peers regardless of the meta-group size, so using it as a pre-check would PERMANENTLY block the R1→R3 raise it is meant to enable. The raise gate is the `UpdateStream` rejection classified to `ErrMetaGroupNotReady` (`IsMetaGroupNotReady`); `ActualReplicas`(via `Cluster.Replicas`) is used ONLY for the `AllAtTarget` actual count.
- **MP-6 — No `publishAuditWithID` Broker method (Stage-B simplification of synthesis C).** The standalone `AuditPublisher` (constructed only in test/d5) publishes directly via its own JS client with `jetstream.WithMsgID`, so the live `publishAudit` is byte-unchanged WITHOUT a sibling method — "live byte-unchanged" holds better. The `publishAuditWithID(` token is therefore correctly absent from the guard's banned set (it would be a dead token).
- **MP-7 — the heavy clustered-JS `test/d5` integration suite is gated behind `//go:build d5_integration`** (post-internal-review). It runs ONLY in the dedicated `TestD5Matrix` subprocess (`-tags d5_integration`, uncontended), NOT in the parallel `make test` where ≈30 concurrent package binaries starve the embedded JS clusters into timeouts (each flaked test passes in isolation + `-race`). Same precedent as the `e2e_matrix` gate. The CHEAP d5 tests (build-and-prove + L-2 import guards, dedup-window assertion — no JS cluster) and the `internal/{cluster,broker,jsstream}` D5 unit tests stay in `make test`. Internal-review adjudication + all fixes: `docs/reviews/d5-review.md`.

---

## §1 File-level work plan

> **⚠ SUPERSEDED-IN-PART (read §0bis first).** §1/§3 below are the pre-implementation draft. Two
> items were corrected during Stage B and are BINDING per §0bis MP-5/MP-6 + the external review:
> (a) there is **no `publishAuditWithID` method** — the standalone `AuditPublisher` publishes
> directly via its JS client with `jetstream.WithMsgID` (MP-6); (b) the replica-raise gate is the
> **`UpdateStream`/`UpdateObjectStore` rejection classified to `ErrMetaGroupNotReady`**, NOT a
> `metaGroupCanHost` pre-gate (MP-5 — an R1 stream's `Cluster` lists 0 peers and would falsely
> block the raise); `StreamInfo.Cluster`/`ActualReplicas` is used ONLY for the `AllAtTarget` actual
> count. Wherever §1/§3 say otherwise, §0bis wins. (Also external F1: `OpAuditCheckpointSet` is a
> hard publisher-cursor skip — see `d5-review.md` + the §6.3 doc block.)

### New `cluster.Node` primitives — `internal/cluster/read.go` (raft-free, NATS-free; L-2)
```go
func (n *Node) CommitIndex() uint64                      // n.raft.CommitIndex() (R-4)
func (n *Node) LogFirstIndex() (uint64, error)           // n.store.FirstIndex() (R-6)
func (n *Node) LogLastIndex()  (uint64, error)           // n.store.LastIndex()
func (n *Node) CommittedCommandAt(idx uint64) (*Command, error) // decode via the SAME unexported decodeCommand
//   ErrLogTruncated  idx < FirstIndex() (snapshotted away — R-6 loud-loss branch)
//   ErrLogNonCommand raft config/noop  (skip; no audit)
//   ErrLogPoison     decode failed      (skip; FSM advanced applied_index as no-op)
func (n *Node) AuditPublishedIndex() (uint64, error)     // replicated checkpoint KV; missing→0 (off n.db)
func (n *Node) AdvanceAuditPublished(to uint64) error    // OpAuditCheckpointSet via Propose; batched; skip when to<=current (R-5)
func (n *Node) NumVoters() (int, error)                  // count Suffrage==Voter in committed Configuration (R-16/R-17)
```
Sentinels in `internal/cluster/command.go`: `ErrLogTruncated`, `ErrLogNonCommand`, `ErrLogPoison`.
**Dropped from drafts:** `OnLeaderChange` (R-13), `AppliedIndex`-as-ceiling (R-4), `ReadCommand`'s `(term,logtype)` extras (R-15), leader-local cursor methods (R-1).

### New op `OpAuditCheckpointSet` — `internal/cluster/auditcursor.go`
- Planner bakes the R-3 monotonic-guard UPSERT with a validated decimal-uint literal (reuse `sqlbake.LitInt`; no `Args`); `const metaKeyAuditPublishedIndex = "audit_published_index"`.
- Register in `knownOps` + `defaultAppliers` (rides `genericExecApplier`). **No migration** (rides `cluster_meta` 0009; KV reads-as-0 when absent like `applied_index`). `test/determinism/apply_reachability_test.go` stays green (new op rides the existing `genericExecApplier.ApplyTx` edge, no new mutator edge).

### `internal/jsstream/replicas.go` (new) + `jsstream.go`
- `func ReplicasFor(nVoters int) int` (R-16); `const ReplicasSingle = 1`; `const AuditDedupWindow = 2*time.Minute` (MP-3); `var ErrMetaGroupNotReady = errors.New(...)`.
- `EnsureEventsStream(ctx, js, targetReplicas int)` / `EnsureHistoryStream(ctx, js, sid string, targetReplicas int)` — set `cfg.Replicas = targetReplicas` (replacing the `1` literal) + `cfg.Duplicates = AuditDedupWindow` (MP-2), route through reworked `ensureStream`.
- `ensureStream`: create; on `ErrStreamNameAlreadyInUse` → `reconcileReplicas`: fetch `StreamInfo`; `current >= target` → nil (never shrink, R-18); `!metaGroupCanHost(info,target)` → `ErrMetaGroupNotReady`; else `UpdateStream(Replicas=target)`.
- `func metaGroupCanHost(info, target)`: `live := 1; for p := range info.Cluster.Replicas { if p.Current && !p.Offline { live++ } }; return live >= target`.
- `type StreamReplicaState struct { Name string; Target, Actual int; Ready bool }` + an `ActualReplicas(info) int` helper (R-20).

### `internal/broker/transfer.go`
- `ensureXferBucket(ctx, sid string, targetReplicas int)` — `ObjectStoreConfig{Replicas: targetReplicas}`; on `ErrBucketExists`/`ErrStreamNameAlreadyInUse` → `reconcileXferReplicas` (read object-store `Status().Cluster`; `UpdateObjectStore(Replicas)` when short + meta-ready, R-18). Live callers (`transfer.go:375/475`) pass `jsstream.ReplicasSingle`.

### `internal/broker/broker.go` + `audit.go` + `sessions.go` (live caller threading + new method)
- Thread `jsstream.ReplicasSingle` into the three live `EnsureHistoryStream`/`EnsureEventsStream` call sites (MP-1: NO `b.node` field).
- `func (b *Broker) publishAuditWithID(subject string, payload []byte, msgID string) error` — sibling of `publishAudit`, adds `jetstream.WithMsgID(msgID)` on the JS path, preserves the `auditTapForTest` seam. **`publishAudit` gets zero diff** (R-23).

### New `internal/broker/audit_publisher.go` — the merged leader-only loop (constructed ONLY in test/d5)
```go
type clusterReader interface {           // narrow iface over *cluster.Node (mockable)
    IsLeader() bool
    LeaderContactStale(now time.Time) bool
    CommitIndex() uint64
    LogFirstIndex() (uint64, error)
    CommittedCommandAt(idx uint64) (*cluster.Command, error)
    AuditPublishedIndex() (uint64, error)
    AdvanceAuditPublished(to uint64) error
    NumVoters() (int, error)
}
type auditPublisher struct {
    node       clusterReader
    js         jetstream.JetStream
    now        func() time.Time
    log        *slog.Logger
    batch, maxPar int            // 256 (R-5), 8 (R-21)
    poll       time.Duration     // 100ms
    onPublish  func(subject, msgID string)              // test tap, fires AFTER ack
    listSIDs   func(ctx context.Context) ([]string, error)
    xferState  func(ctx context.Context, sid string, target int) (jsstream.StreamReplicaState, error)
}
func (p *auditPublisher) Run(ctx context.Context)                       // R-13/R-14: one goroutine, loopCtx-tied
func (p *auditPublisher) publishOnce(ctx context.Context) (advancedTo uint64, err error) // R-4/R-5/R-6/R-8/R-9/R-10
func (p *auditPublisher) reconcileOnce(ctx context.Context) (ReplicaReport, error)        // R-17/R-18/R-20/R-21
func auditMsgID(raftIndex uint64, reqID, kind string, seq int) string   // R-8/R-10: q<reqID>:k:s | r<idx>:k:s

type ReplicaReport struct { Streams []jsstream.StreamReplicaState; Observed bool }
func (r ReplicaReport) AllAtTarget() bool // !Observed||empty→false; all Ready (R-19/R-20)
func (r ReplicaReport) Degraded() bool    // Observed && non-empty && !AllAtTarget
```

### `internal/proc/plan.go`, `cmd/tether/serve.go`, `internal/broker/authcallout.go`
UNCHANGED. `ReplayReconcileAudit` consumed as-is. Production constructs no `cluster.Node`, never starts `auditPublisher` (R-23 guard-locked).

---

## §2 Doc-first amendments (`docs/distributed-broker-architecture.md`, BEFORE code; written as `> D5 实现裁定` blocks in the existing R2-major rulings, D2/D3/D4 style)

- **D-1 §6.3** — replicated `audit_published_index` via **new op `OpAuditCheckpointSet`** (NOT `ApplyMetaSet` — `t:` guard; NOT leader-local), FSM-baked monotonic `WHERE CAST(value AS INTEGER) < ?`, batched advance, idle = zero raft writes. Ceiling = **`CommitIndex()`** (IS exported in raft v1.7.3 — correct the SSOT). Floor = `max(checkpoint, LogFirstIndex())` re-clamped each iteration. Dedup id `r<idx>:<kind>:<seq>` (`kind∈{proc,port}`, closed for D5), seq = position in `ReplayReconcileAudit` order; **reqID-keyed `q<reqID>:…` for reqID-bearing reconcile** to collapse the D4 `appliedDedup` retry double-publish (R-10). `CommitIndex-checkpoint < TrailingLogs` asserted; snapshot-truncated unpublished audit = bounded loud accepted-loss (tightens §16/§17 approximate-0-loss to "bounded by snapshot cadence"). Single-writer = identical-id collapse. Live `publishAudit` byte-unchanged; new `publishAuditWithID` adds the msg-id; streams carry an inert `Duplicates` window in production (MP-2).
- **D-2 §6.4 / §9** — `replicasFor(nVoters)`: `1→1, 2→2, 3→3, cap 3` in `internal/jsstream` (L-2: `int` param). `ensureStream` already-in-use → raise-only `UpdateStream`; `ensureXferBucket` → `UpdateObjectStore`; **all THREE families**. Target = `replicasFor(nVoters)` (raft authoritative); gate = client-visible `metaGroupCanHost` from `StreamInfo.Cluster` (broker is a JS client); expand `events` first; `ErrMetaGroupNotReady` typed-retry IS the gate. `actual` excludes `!Current`/`Offline`. `AllAtTarget` canonical (full set, fail-closed, empty→false). Queue-not-drop = checkpoint-non-advance (the §6.3 non-advance IS the §6.4 queue; no second queue).
- **D-3 §16 / §10.4** — correct any text implying `replication_degraded` is client-synthesized: it is replication-store-backed (0009 CHECK), its writer is D8b, D5 ships only the predicate. Client-synthesized pair = `quorum_lost`/`force_single_active`.
- **D-4 §17** — confirm "审计 0 丢失 = 近似 (D5 sweep)" + "severe 非全部硬闸 destructive" (keep `replication_degraded` severe + gating-retire + non-write-blocking); tighten loss bound to "bounded by snapshot cadence + dedup window"; add "RF2 (N=2) = read-survivable, zero write fault-tolerance" (OQ-1).
- **D-5 §5** — register `OpAuditCheckpointSet` (NOT a relaxation of the `t:` guard); derives zero audit; in the publisher skip-set.
- **D-6 §19-D5** — full 做/测试/出口 box mirroring D1–D4: build-and-prove, serve.go uncut, cutover=D9; lists the new Node primitives, the new op, the jsstream/transfer reconfig, the merged publisher loop, the `test/d5` clustered-JS harness, and exit assertions. **Hard non-goals:** any cluster status/create/online/drain/retire COMMAND (D7), data-plane rehome (D6), serve.go `cluster.Node` (D9), in-body epoch header (D8), alert-row write (D8b).

---

## §3 Adversarial test plan

**Harness `test/d5/setup_test.go`** — extends the d4 combined harness by enabling **clustered JetStream**: unique non-empty `o.ServerName="d5-<i>"` per server (d4 omits it; required or JS meta never forms), per-server `o.StoreDir=t.TempDir()/js-<i>`, `o.JetStream=true`, bounded `JetStreamMaxStore/Memory`, n=3, retain `*server.Server` handles as ground truth (`JetStreamIsStreamCurrent`/cluster peers) to cross-check client-derived `actual`. Wait helpers (no `time.Sleep`): `waitForJSMeta` (15s), `waitForStreamReplicas` (polls `Cluster.Replicas` actual, not the API return), `drainPublisher` (publishOnce to fixpoint: returns 0 advance AND cursor==CommitIndex). Leak gates copied from d4 (`assertNoGoroutineLeak`/`assertNoFDLeak`; goleak NOT used). **Bracketing (OQ-5):** baseline AFTER JS+streams settle; bracket only the loop lifecycle (start → flap → cancel+join).

### Mechanism A — `test/d5/publisher_test.go` + unit tests
| Test | Adversarial scenario | Non-vacuity hook |
|---|---|---|
| E-A1 LeaderDiesBeforeSweep | kill leader before its first `publishOnce`; new leader sweeps the gap | stream-empty-before-election baseline (old leader published 0 via tap) → full post-election proves the SWEEP did it |
| E-A2 DedupWithinWindow (two-arm) | old leader publishes E; `TransferLeadership`; new leader re-publishes E | Arm A: `Duplicates=2m`, `State.Msgs` grows by **0**. Arm B: mutated id (seq+1) OR shrunk window → grows by exactly len(records). Both arms (without B it is vacuous) |
| E-A3 DedupWindowAssertionFires | the asserted inequality | reads real `MultinodeElectionTimeout` + sweep bound BY REFERENCE; shrink override → red |
| E-A4 MsgIDStableAcrossLeaders | two independent decodes of the same committed bytes | id lists byte-identical; control: hand-reordered Aux → different ids (seq is position-sensitive) |
| E-A5 DedupIDDistinctAcrossKinds | an entry whose `proc[0]` and `port[0]` would collide under a coarse kind | both records survive (depth==2 exact) — pins the coarse-key silent-loss bug |
| E-A6 SweepHitsTruncatedLog | advance checkpoint, `Snapshot()`, push no-ops so `LogFirstIndex() > checkpoint+1` | (a) no panic, (b) loss surfaced for exact range, (c) checkpoint advances to CommitIndex (NO WEDGE), (d) entries `> LogFirstIndex` still publish |
| E-A7 CheckpointSnapshotRaceMidLoop | inject `Snapshot()` between two `CommittedCommandAt` in one pass (a `midLoopGate` seam) | the mid-loop-truncated entry takes the loud-loss branch + checkpoint reflects reality |
| **E-A8 ForwardedReconcileNotDoublePublished** | commit `OpReconcileBatch` at idx N (ReqID R), then an ack-lost retry of the SAME reconcile commits at idx M → `appliedDedup` | both entries carry Aux; records appear **exactly once** (id keyed on ReqID R) — pins R-10 |
| E-A9 CheckpointMonotonicNoRewind | a deposed leader's `Propose(lower)` racing a new leader's higher committed value | FSM `WHERE`-guard makes the lower a no-op on every replica; drive the lower via the deposed-leader Propose path |
| E-A10 PublishCeilingIsCommitNotLast | append an uncommitted entry to a partitioned leader's log; heal so it's overwritten | publisher does NOT publish the doomed entry's audit; no orphan audit after heal |
| E-A11 AuditOnlyReconcilePublished | `OpReconcileBatch`, empty Body, 2 killed_orphan + 1 reconciled port in Aux | all 3 publish (killed_orphan nil rc); a Body-gated publisher emits 0 |
| E-A12 LeaderLocalOnlyAuditNotReplayed | a committed non-reconcile op + an inline `audit.call` | publisher emits 0 for them (R-11 honesty boundary) |
| E-A13 StaleLeaderNoSplitBrain | partition the leader, let B win, both run the loop | A advances NO checkpoint (Propose→ErrNotLeader once quorum lost); every id once; A stops within `TFence` |
| E-A14 IdleLoopZeroRaftWrites | idle loop K ticks (cursor==CommitIndex) | **zero** new committed `OpAuditCheckpointSet` entries (R-5) |
| E-A15 PublisherNoGoroutineLeak | flap leadership 50× (`TransferLeadership`), cancel loop ctx, join | `assertNoGoroutineLeak`+`assertNoFDLeak` green; in-flight publish ctx is loop-ctx child |
| E-A16 FollowerNeverPublishes | a non-leader runs the loop | the `onPublish` tap fires **zero** times AND stream empty |

### Mechanism B — `test/d5/replicas_test.go`
| Test | Scenario | Non-vacuity hook |
|---|---|---|
| E-B1 ReplicasForProperties | table N∈{0,1,2,3,4,5,100} | exact values + monotone non-decreasing + `replicasFor(n)≤n` + cap 3 (N=4/100→3 catches a `min(n,5)` regression) |
| E-B2 StreamExpandsR1toR3 | create at R1 on a 3-node JS cluster, `EnsureHistoryStream(...,3)` | `waitForStreamReplicas(3)` + pre-expand messages still readable; a second ensure = 0 `UpdateStream` calls (counting JS wrapper) |
| E-B3 ReconfigGatedOnJSMetaJoin | 3 raft voters but only 1–2 in JS meta-group | `UpdateStream` NOT called for target 3, `ErrMetaGroupNotReady`, `Degraded()==true`, `AllAtTarget()==false` (raft-voter ≠ JS-member) |
| E-B4 AllAtTargetCanonicalNotSampling | 50 streams, the short one pinned LAST | `AllAtTarget()==false` + the short stream named (laggard-last defeats a first-K sampler); variant: transiently-erroring `ListStreams` → false (fail-closed) |
| E-B5 DegradedRaisesAndClearsFromLiveStream | one LIVE stream short mid-expand, then caught up | derive `actual` from live `StreamInfo.Cluster` (not a fixture); raise then clear edges; plus a stuck-peer-never-clears variant |
| E-B6 RetireGateEmptyIsFalse | zero sessions/streams | `AllAtTarget()==false` |
| E-B7 ShrinkIsNoop | target computed lower than current | NO `UpdateStream` shrink (shrink is D7) |
| E-B8 AllThreeFamiliesReconfigured | events + history + OBJ_xfer at R1 → scale to 3 | ALL THREE reach actual=3 (object store via `UpdateObjectStore`) |
| E-B9 BoundedFanout | 200 sessions | in-flight JS calls never exceed `maxParallel` (counting wrapper high-water) |

### Joint A+B — `test/d5/joint_test.go`
| Test | Scenario | Non-vacuity hook |
|---|---|---|
| E-J1 KillDuringExpandNoLossBoundedStall | start R1→R3 expand, **kill gated on reconfig-in-flight** (poll until a peer shows `Current==false`, THEN kill), concurrently commit ReconcileBatch | (a) zero audit loss (every record once after recovery), (b) `0 < maxStall < bound` (a 0 stall means the kill missed the window → setup fails), (c) checkpoint did NOT advance during the stall then advanced after (queue-not-drop) |

### Guards + matrix — `test/d5/regression_test.go`, `test/e2e/all_phases_test.go`
- **E-G1 ProductionWiresNoClusterNode** — token-scan over `serve.go`+`broker.go`+`authcallout.go` for `auditPublisher`/`publishOnce(`/`reconcileOnce(`/`AdvanceAuditPublished(`/`AuditPublishedIndex(`/`CommittedCommandAt(`/`CommitIndex(`/`NumVoters(`/`ReplicasFor(`/`publishAuditWithID(`/`ReplicaReport` + a self-check (mirror `TestD4ProductionGuardSelfCheck`). SSOT banned-token slice.
- **E-G2 ClusterNoNATSImport** — `internal/cluster` transitive import-set (via `go list -deps`) excludes `nats.go`/`jetstream`/`internal/broker`/`internal/jsstream` + a self-check.
- **E-G3 JsstreamNoClusterImport** — `internal/jsstream` import-set excludes `internal/cluster` (locks `ReplicasFor` as an `int` param).
- **E-G4 TestD5Matrix** — `-race -count=1 -timeout 300s` subtest over `./internal/cluster/... ./internal/proc/... ./internal/jsstream/... ./internal/broker/... ./test/d5/...`, driving E-A1 + E-A8 + E-A13 + E-B3 + E-B4 + E-J1 end-to-end.

---

## §4 Execution checklist (3-stage · 7-step)

1. **Stage A done** — this plan-of-record (synth + §0bis finalization). ✅
2. **Doc-first** — apply D-1..D-6 to `docs/distributed-broker-architecture.md`. ⛔ before any code.
3. **Stage B** — implement §1 in order: (a) `cluster` read primitives + `OpAuditCheckpointSet`; (b) `jsstream` `ReplicasFor`/`Duplicates`/reconcile + `transfer` xfer reconcile; (c) `broker` `publishAuditWithID` + `audit_publisher.go` merged loop + predicate; (d) live-caller threading (pass `1`); (e) tests E-A*/E-B*/E-J1/E-G* + harness; (f) e2e `TestD5Matrix`.
4. **Stage C internal review** — fresh adversarial workflow (≥5+1 Opus 4.8), report `docs/reviews/d5-review.md`.
5. **Adjudicate + fix** — main process; integrate experts' added tests; only main process edits implementation.
6. **External review (user)** — **STOP HERE per the goal.** Hand off; reply in `docs/reviews/d5-external-review.md`.
7. *(After external PASS)* commit + push to main, flip CLAUDE.md §7 + memory.

**Hard gate (before claiming Stage B done):** `make test` + `make e2e` + `make lint` all green; `-race` + NumGoroutine/fd leak gates on `test/d5`; the three guard families green.
