# D4 Plan — Write-Forwarding `apply.*` (follower→leader)

> **Plan-of-record.** Stage A (CLAUDE.md §3): 4 adversarial drafters → 4 adversarial critics → 1 synthesizer (workflow `wf_13a45d2b-d37`, 9× Opus 4.8), then **finalized by the main process** (sole definer). The synthesis candidate is adopted with the main-process rulings in §0bis layered on top (one material correction + 6 open-question rulings). All load-bearing repo facts re-verified firsthand before adoption.

D4 closes the §19-D4 exit: a follower receiving a session-control write lands it through the leader; an old/deposed leader rejects the write (`ErrLeadershipLost` → fail-closed). It builds **two orthogonal mechanisms** — (1) content-addressed-`ReqID` write-forwarding follower→leader, (2) a same-Apply-txn dedup ledger — plus a **self-sufficient `ReconcileBatch`** entry that any new leader can byte-identically replay. All **build-and-prove** (cutover = D9), `cmd/tether/serve.go` byte-untouched.

---

## §0 RULINGS (resolution of every contested / must-fix point)

**Verification basis.** Re-read live: `internal/cluster/{fsm,command,node}.go`, `internal/broker/{reconcile,exec,broker}.go`, `internal/proc/plan.go`, `internal/agentprov/{plan,agentprov}.go`, `internal/session/{session,plan}.go`, `internal/authcallout/handler.go`, `internal/cluster/testhooks.go`, `internal/proto/subjects.go`, `internal/auth/permissions.go`, `test/d3/*`; architecture §3.4/§3.7/§3.8/§4.1/§5/§6.2/§6.3/§19. Confirmed: PIN-join writes are `INSERT OR IGNORE` (`agentprov/plan.go:54`, `session/session.go:344`) → SQL-idempotent; `pubAuditProc` gates `RC` only for `exit`/`reconciled_closed` (`exec.go:433`) → `killed_orphan` carries no `rc`; no persistent audit table (`0001_init.sql:3` "audit_log NOT in SQLite"); no golden `"v":1` fixtures in cluster tests → `commandVersion` bump is safe; `0008–0010` are D0 → `0011` is the next number.

### R-1 — Cutover stance: BUILD-AND-PROVE, zero production wire. *(uncontested)*
D4 wires nothing new into `cmd/tether/serve.go`. Production constructs no `cluster.Node`; the authcallout `JoinMemberWrite`/`ProvisionAgentWrite` seams stay nil (= today's direct `ProvisionWithPIN`/`AddMember`, byte-identical); the forward responder is never subscribed. Forwarder + responder + ReqID dedup + self-sufficient `ReconcileBatch` are library code under `internal/cluster` (+ a broker-owned bus adapter), proven only by a real ≥2-node `test/d4` harness. **Why:** §19-D4 做 lists 转 leader + not_leader fail-closed + 稳定幂等键 + ReconcileBatch leader 权威 — it does NOT list `broker.New` embedding a Node. Production cutover needs the single-WAL merge + DB-header flip §3.8 pins to D9; doing it now violates 先父后子. A standing guard test (`TestD4ProductionWiresNoClusterNode`) extends the D3 source-scan so this cannot drift.

### R-2 — ReqID dedup-ledger STORAGE: dedicated table via **migration 0011** (NOT a `cluster_meta` "r:" prefix).
"`r:ReqID` 取该键，§5" refers to the **Command envelope `r` field** (`command.go:70` `json:"r"`), NOT a KV storage prefix — the doc is silent on storage, so this is a free engineering decision (doc-first satisfied by amending §4.1/§5). Choose `cluster_reqid_ledger(req_id TEXT PRIMARY KEY, raft_index INTEGER NOT NULL)` + index on `raft_index` because: (a) the PK is the dedup-uniqueness invariant as a hard DB constraint (a double-insert backstop that fail-stops); (b) the `raft_index` index makes the deterministic GC range-delete bounded instead of a full prefix-scan parsing every KV value on the `SetMaxOpenConns(1)` pool; (c) it leaves the D1 `applied_index`/`applied_term` cursor KV semantics byte-untouched. Lives in the cluster WAL FSM DB → zero blast radius to the frozen broker DB until D9.

### R-3 — ReqID MINT OWNER + KEY DERIVATION: originating broker, **content-addressed per-verb digest**. *(see §0bis-A for the reconcile correction; **SUPERSEDED for provision/join by §0bis-H + external-review RF1**)*
> **SUPERSEDED (external F1/RF1):** provision and join carry **NO** forwarding ReqID — their writes are `INSERT OR IGNORE` (idempotent) and their binding is operator-deletable, so a content key would falsely dedup-skip a legitimate post-evict re-provision. The `cluster.apply` responder REJECTS a non-empty ReqID on provision/join (`ErrReqIDNotAllowed`). **Only `reconcile` carries a key.** The per-verb-digest derivation below applied only to the original (rejected) design; keep it as the historical record. See §0bis-H.
The ORIGINATING (forwarding) broker mints, NEVER the leader (§4.1: `ErrLeadershipLost` = "committed but ack lost"; the retry hits the NEW leader; a per-leader re-mint cannot dedup the already-committed entry). The **random-nonce-cached-per-request scheme is REJECTED**: `authcallout/handler.go` invokes the seams fresh on every `Handle()`, and D3-R3 has the client treat a deny as terminal → it RECONNECTS, producing a new `Handle()` with no surviving object to cache a nonce on; a forwarder crash has the same effect. Only a key **re-derivable from request-identity inputs the seam re-sees on retry** survives. Derivation (domain-separated SHA-256, 128-bit hex prefix):
- PIN-provision: ~~`hex(SHA256("prov\x00" || sid || "\x00" || nid || "\x00" || fp))[:32]`~~ — SUPERSEDED, no key (RF1)
- PIN-join (member): ~~`hex(SHA256("join\x00" || sid || "\x00" || fp))[:32]`~~ — SUPERSEDED, no key (RF1)
- reconcile: **see §0bis-A (corrected)** — keyed on the RAW forwarded request, not resolved decisions.

Domain separation makes distinct intents un-collidable; SHA-256 makes adversarial collision infeasible; the 128-bit prefix makes accidental collision negligible.

### R-4 — Dedup-ledger atomicity + FSM dedup SEMANTICS + sentinel + GC.
- **Same-txn atomicity:** op SQL + the `cluster_reqid_ledger` INSERT + the `applied_index`/`applied_term` UPSERT commit in the ONE existing `applyCommand` txn (`fsm.go:99-159`). Crash before commit ⇒ none persist; raft re-delivers and the index-skip handles it. Proven by an `applyFailHook` injection test.
- **Gate ordering (MANDATORY):** the existing index-skip (`fsm.go:116` `l.Index <= applied → appliedNoOp` rollback) runs FIRST; the ReqID-skip runs only when `cmd.ReqID != "" && l.Index > applied`. Raft re-delivery of the *same* index stays on the cheap index path (`reapplyCount`); the dedup branch fires *only* for a forwarder-retry at a *new* index (`dedupCount`).
- **New sentinel `appliedDedup{index}` — MUST NOT reuse `appliedNoOp`.** `appliedNoOp` *rolls back* (valid only at `l.Index <= applied`). The dedup path arrives at a HIGHER index and MUST advance `applied_index`+`applied_term` and COMMIT (raft advances `lastApplied` regardless of return value), else the FSM wedges. `appliedDedup` = {skip op SQL, do NOT re-INSERT the ledger row, write `applied_index`, commit}. `Node.Apply` maps `appliedDedup` → nil (success), like `appliedOK`. New `dedupCount atomic.Uint64` sibling to `reapplyCount` for non-vacuity.
- **GC: in-Apply-txn deterministic range-delete keyed on `applied_index`.** REJECT any leader-local periodic prune — a non-replicated DELETE to a *replicated* table diverges follower ledgers and breaks logical equivalence. On a NEW-ReqID apply at index `N`, the same txn also runs `DELETE FROM cluster_reqid_ledger WHERE raft_index < N - reqIDRetentionWindow`. Every replica deletes the identical set at the identical index (no wall-clock). On the dedup-HIT path: no re-INSERT, no GC advance (the original row must age out by index math).
- **Retention window:** `reqIDRetentionWindow uint64 = 1 << 20` — see §0bis-D for the anchoring.

### R-5 — Idempotency proof FRAMING: assert dedup-branch FIRED, NOT "one row".
The in-scope PIN writes are SQL-idempotent (`INSERT OR IGNORE`), so "exactly one row" is VACUOUS (passes even with the ledger removed) and a "two rows without the ledger" RED control is falsely constructed for these verbs. **Ruling:** non-vacuity is proven by (a) `dedupCount == 1` (the branch demonstrably fired) AND (b) the op applier observably ran exactly once — via an **injected counting applier** in the FSM-unit test (the real RED control; see §0bis-B). D4 must NOT assert "audit emitted once" — that pulls D5 forward.

### R-6 — ReconcileBatch: what-gets-baked + replay determinism + D4↔D5 boundary.
- **Baked content:** `OpReconcileBatch` keeps the executable `MarkExited` UPDATEs in `Body` (the only replicated DB mutation, unchanged) AND carries a NEW apply-inert structured field of ordered audit tuples. Tuple = `{Kind, NID, PID string; Port int; Name string; LocalPort int; RC *int; Ts time.Time}`. The fields in NO persistent row — `name`/`local_port` (from `req.LocalPorts`), `rc` (from `req.LocalProcesses`) — are captured at Plan time by the leader's resolver and baked. Self-sufficient per the §4.1 R3-blocker.
- **Encoding = structured field, NOT SQL into an audit table** (confirmed: no persistent audit table; audit lives only in JS `history-<sid>` → inventing one drifts into D5/D8). The tuples ride a new `Command` field that `genericExecApplier` ignores. **`commandVersion` bumps 1→2**; safe (no persisted v1 logs; fresh harness DBs; no golden v1 fixtures). A poison test asserts a malformed v1-shaped/blob ReconcileBatch advances `applied_index` and logs loudly (never wedges).
- **`killed_orphan` carries NO `rc`** (`exec.go:433` gates `RC` to `exit`/`reconciled_closed`): the tuple stores `RC *int` and the replay reproduces the exact kind-gating (orphan → `RC=nil`), or the replayed JSON gains a spurious `"rc":0` and diverges.
- **Timestamp serialization:** `markExitedSQL` bakes time via `LitTime = t.String()` (may carry the monotonic reading); `pubAuditProc/Port` json-marshal `Ts time.Time` as RFC3339 (monotonic stripped). The baked tuple `Ts` MUST be monotonic-stripped (`now.Round(0)` before baking) so replayed audit JSON byte-matches the live record. The `MarkExited` UPDATE keeps its existing `LitTime` rendering.
- **Replay function:** `ReplayReconcileAudit(body ReconcileBatchBody) []schema.AuditRecord` — PURE, deterministic, reads ONLY the decoded op (no live request, no leader-local map, no DB). Does NOT thread `raftIndex`, computes NO dedup key (that is D5's `raft_index:kind:seq` — threading it now leaks D5 design). Returns tuples in the baked total order.
- **D4↔D5 boundary (pinned):** D4 = entry self-sufficient (bakes ordered tuples) + pure `ReplayReconcileAudit` + byte-identical-replay proof. D4 **publishes NOTHING**. D5 owns the post-commit single-writer publish, the `raft_index:kind:seq` dedup window, and the post-election sweep. Live `reconcileOnRegister` keeps its inline `pubAuditProc/pubAuditPort` exactly as today.

### R-7 — Total order + the LIVE-PATH audit-order contradiction.
Op-path total order: proc tuples (`reconciled_closed` + `killed_orphan` merged) by **PID-ULID ASC**, port tuples by **port ASC**, covering both nondeterministic map loops + the orphan list (`reconcile.go:126/197/217`). But LIVE `reconcileOnRegister` emits `killed_orphan` in Go-map order (`reconcile.go:197`), so asserting "replay byte-identical to the live *sequence*" is unimplementable without reordering the live path — a behavior change in a zero-regression phase. **RESOLUTION:** the live-vs-op equivalence oracle compares audit records as a **SORTED SET / multiset**, never the raw live emission order; cross-node **replay-vs-replay is compared byte-identical** (both read the same baked total-ordered entry). This proves self-sufficiency + cross-leader determinism WITHOUT touching the live path. Live `reconcileOnRegister` stays byte-identical (DB rows + inline audit calls) and remains the differential golden arm. The shared classifier is promoted (pure, side-effect-free); the live path is NOT cut to the op path (that is D9).

### R-8 — not_leader / fenced / timeout TAXONOMY + client/PIN contract.
Typed reply envelope `{Status ∈ {ok, not_leader, error}, ErrKind, ErrMsg string}`:
- `ok` = entry committed (or dedup-skipped — indistinguishable, correct).
- `not_leader` = answerer was follower / lost leadership (`cluster.IsNotLeader(err)` ⇒ `raft.ErrNotLeader ∨ ErrLeadershipLost`). RETRIABLE — forwarder re-requests the broadcast bus (§0bis-H), never the deposed broker, never an ambiguous timeout-as-success.
- `error` = a PERMANENT typed business error from Plan/Apply (`ErrInvalidPIN`, `ErrSessionMissing`); `ErrKind` lets the broker re-map to the exact authcallout deny. NOT retriable.
- **NATS request TIMEOUT = RETRIABLE-with-the-SAME ReqID, NEVER a fresh write** (a timeout is not proof of non-commit; the stable ReqID lets the leader dedup). Test T-T.
- **`ErrFenced` / quorum-loss:** if the forward reaches a leader that cannot commit (quorum lost), `Node.Apply` blocks then `applyTimeout` fires → forwarder surfaces a transient retriable deny within bounded time (no hang, no un-replicated local write). The forwarded-PIN-write-during-quorum-loss path maps to a retriable deny. Test T-Q.
- **Client contract:** agent `isAuthFailure` is UNCHANGED (broadening would flap a genuine bad PIN forever, D3-R3). The improvement is BROKER-INTERNAL transparent forwarding: a healthy follower forwards the PIN write to the leader so the agent never sees `not_leader` on the happy path. The residual `not_leader` (election race / quorum loss) stays transient; the agent treats it terminal-then-reconnect, and the reconnect-driven re-provision is idempotent (INSERT OR IGNORE + already-provisioned fast-path; provision/join carry no ReqID — §0bis-H/RF1).

### R-9 — Forwarder/responder LOCATION + L-2.
Keep `internal/cluster` **NATS-FREE** (it imports zero nats today; a nats subscription there inverts the broker→cluster dependency). The forwarder + leader responder live in a broker-owned `internal/broker/cluster_forward.go` importing both `nats` and `cluster`, translating `cluster.IsNotLeader(err)` → typed reply / `authcallout.ErrNotLeader`. `internal/cluster` exposes only raft-free primitives: `ProposeWithReqID` (see §0bis-F) + a new `cluster.ErrForwardNotLeader` sentinel. `authcallout` stays raft-free. `TestRaftConfinedToClusterPackage` stays green.

### R-10 — Verb set: wire+test only `provision` / `join` / `reconcile`. *(see §0bis-E)*
Build the GENERIC forwarder primitive, but WIRE + TEST only the §13.7-named writes: `provision`, `join` (PIN), `reconcile` (G.1). Forwarding `exec/run/expose/kill/push/pull` and the other mutators is dead code in D4 (production forwarder nil until D9) AND would imply idempotency coverage for the token-minting `PortAllocate` that no test provides. Subjects via `proto.SubjClusterApply(verb)` (SSOT).

### R-11 — A NEW combined real-routed-NATS + real-mTLS-raft harness must be BUILT.
In D3 no test combines routed NATS with raft writes (`cross_server_test.go` = routed NATS w/o raft; `handler_node_test.go`/`follower_pin_review_test.go` = raft over `InmemTransport`). D4's forward wire is a NATS request/reply on the routed bus while raft replication rides the mTLS `NetworkTransport`. That combined harness DOES NOT EXIST — D4 budgets building it in `test/d4`: N≥2 brokers each with (a) routed NATS (`startRouted`/`authClusterOpts`), (b) a real mTLS raft `NetworkTransport` Node (`handler_node_test.go` pattern), (c) the forward responder broadcast-Subscribed (leader-only-reply, §0bis-H) on the real bus. **Real build cost.**

### R-12 — Post-commit `ErrLeadershipLost` test mechanism. *(see §0bis-C)*
`applyCommitGate`/`applyFailHook` are SQLite-layer; neither produces "raft committed + replicated + proposer observes failure". The headline §13.7 #2 gate needs a deterministic way to land a retry under a NEW leader after the entry committed under the old one — resolved in §0bis-C (forwarder-level ack-drop + `raft.LeadershipTransfer()`), opened with a de-risk spike.

### R-13 — Misc must-fixes locked.
- **Leak gate not importable:** `assertNoGoroutineLeak` is package-private in `test/concurrency/helpers_test.go`. D4 copies the poll-with-tolerance `NumGoroutine` + fd-baseline helper into `test/d4` (goleak forbidden). Forwarder/responder Subscribe/Unsubscribe + reply-inbox churn is the leak-prone surface.
- **ReqID charset guard:** `decodeCommand` validates `ReqID` is empty OR hex/bounded-length (≤64, no NUL, valid UTF-8) — mirroring `sqlbake.LitText` fail-close — so a malicious ReqID cannot round-trip raft-log JSON differently per node (split-brain ledger). Empty ReqID = today's behavior (D1/D2 ops byte-unchanged).
- **Deterministic follower-answers injection:** the not_leader-bounce path needs a seam (temporarily Unsubscribe the leader's responder, or a test responder asserting received-as-follower) so it is exercised, not happy-path.
- **Guard test extension:** the D4 guard adds the new banned tokens (`NewForwarder(`, `SubscribeClusterApply(`, `NewProvisionSeam(`, `NewJoinSeam(`) to the D3 source-scan over `serve.go`/`authcallout.go`/`broker.go` ONLY. **NOT `cluster_forward.go`** — it DEFINES those constructors, so scanning it would fail trivially (corrected from the draft; see §0bis-H).
- **Stale-leader-in-lease forwarded-PIN:** test T-Q2 asserts a stale-but-in-lease leader answering a forwarded PIN does not allow a join into a session tombstoned on the real leader (bounded by `MultinodeLeaderLeaseTimeout=500ms`, the §3.2 fence D3 established).

---

## §0bis — MAIN-PROCESS DEFINER RULINGS (corrections + open-question dispositions)

### A. **[MATERIAL CORRECTION to R-3]** reconcile ReqID derives from the RAW forwarded request, NOT resolved decisions.
§4.1 is "home follower 转 **agent 清单** → leader 把整个 reconcileOnRegister **结果算成**一条 entry": the **follower forwards the raw agent list; only the leader resolves** (against the leader DB). Therefore the forwarder — which mints the key BEFORE the leader resolves and re-mints it on retry — **cannot** key on "resolved decisions" (it has none). It must key on what it re-sees on retry: the raw request.

```
reconcile ReqID = hex(SHA256(
    "reconcile\x00" || sid || "\x00" || nid || "\x00" || bootID || "\x00" ||
    digest(sorted(req.LocalProcesses by PID) ++ sorted(req.LocalPorts by Port))
))[:32]
```

`bootID` is the natural epoch: an agent **restart** → new `bootID` → new key → a fresh, correct reconcile; an **ack-lost retry of the same register** → identical `bootID`+list → same key → dedupes. The leader still resolves against its own replicated DB and bakes the batch; two retries of one logical register dedupe to the FIRST committed resolution (correct exactly-once for that register event). Leader DB drift inside the seconds-scale retry window is absorbed (the retry skips re-resolution; the DB writes are `WHERE status='RUNNING'`-idempotent anyway). **The leader does NOT pre-check the ledger before proposing** — it always resolves + proposes; dedup is authoritative only inside Apply (replicated, same-txn). (A bounded-stale pre-check optimization is explicitly out of D4 scope.)

### B. **[OQ2]** No new production op; non-vacuous RED control via an injected counting applier.
Adding a probe op widens production `knownOps`/`defaultAppliers` with dead code; extending the `t:` `ClusterMetaSet` cannot express a non-idempotent counter (it bakes a literal, not an expression). Instead: the FSM-unit test (`TestD4FSMReqIDDedupSemantics`) constructs the fsm directly with a **counting applier** injected into `f.appliers` and asserts the applier ran **exactly once** across two same-ReqID applies at different indices (`dedupCount==1`); the RED control is a same-test variant with **distinct** ReqIDs (or empty) that runs the applier twice. The end-to-end forward test additionally asserts `dedupCount==1` (exposed via the Node) + the idempotent-row invariant. Production op set untouched.

### C. **[OQ3]** Headline #2 test = forwarder ack-drop + `raft.LeadershipTransfer()`, opened by a de-risk spike.
The raft-API ambiguity is real but the loss is usually at the **NATS-ack** layer (leader commits, `fut.Error()==nil`, the reply is lost) OR at the raft layer (`ErrLeadershipLost` after a possible commit). Rather than racing to catch the exact `ErrLeadershipLost` instant, the headline test **decouples** the two facts it must combine: (1) entry **committed + replicated** — asserted deterministically by polling a follower's `applied_index`; (2) the proposer **did not get the ack** — simulated by the forwarder ignoring/dropping the first reply; then `raft.LeadershipTransfer()` moves leadership; the retry with the **same** ReqID lands under the new leader → `dedupCount==1`, op ran once. **Implementation opens with a de-risk spike** (mirroring D3-R0) to confirm `LeadershipTransfer` timing in raft v1.7.3 and whether a Node-level post-commit hook in `testhooks.go` is also warranted; the spike's result is recorded before the test is finalized. A pure FSM-unit test (§B) proves the dedup mechanism in isolation regardless.

### D. **[OQ4]** `reqIDRetentionWindow = 1 << 20` indices, accepted with anchoring.
The window is in **raft-index** units and must exceed the max forwarder-retry horizon — bounded by agent NATS reconnect backoff + the not_leader bounce loop (**seconds-scale**) expressed in indices at the control-plane write rate (**low**: session-control writes, not data-plane). `1<<20` ≈ 1M indices = months of operation before eviction — bounded growth, never evicting a live retry. **D5's audit dedup window (`raft_index:kind:seq`) is a SEPARATE concern** and sizes itself. Vacuity test T-G2 sets a tiny window and asserts double-execution, proving the window is load-bearing.

### E. **[OQ5]** No `proc.create` wiring — strict three verbs.
The three §13.7 verbs already exercise three distinct shapes through the generic forwarder: `provision` (PIN write that can return a permanent business error), `join` (member `INSERT OR IGNORE`), `reconcile` (leader-side resolution against the leader DB + batch bake). That is sufficient genericity proof. `proc.create` has no production caller until D9 → pure dead wire; excluded.

### F. **[OQ6]** Add `ProposeWithReqID(reqID, plan)`; `Propose(plan)` delegates with `""`.
Non-breaking and additive — the D3 external-review F1 leader-gate seam (`Propose` checks `n.raft.State() != raft.Leader` before planning) is preserved byte-unchanged, and its guard tests need no edits. `ProposeWithReqID` stamps `reqID` into the Command (via `NewCommandReqID`) inside the SAME `applyMu` window before `Apply`. `Propose` becomes `return n.ProposeWithReqID("", plan)`.

### G. Adopt R-1…R-13 as written (all verified). `commandVersion` bump 1→2 confirmed safe (no golden v1 fixtures; build-and-prove fresh DBs).

### H. Implementation + internal-review + external-review corrections (recorded post-hoc; full reports `docs/reviews/d4-review.md` + `d4-external-review.md`)
- **External F1/RF1 — provision/join carry NO forwarding ReqID, enforced at the WIRE BOUNDARY.** Their writes are `INSERT OR IGNORE` (idempotent) and the binding is operator-deletable (node-evict), so a content key would falsely dedup-skip a legitimate post-evict re-provision (return ok, leave `agent_provisioning` absent → D9 false-allow). The agent has no per-attempt epoch (D3-R3). Fix: the production seams (`NewProvisionSeam`/`NewJoinSeam`) carry no key (`node.Propose` / `Forward(verb,"",…)`) AND — because the seam is only one caller — `dispatchForward` REJECTS a non-empty ReqID on provision/join with the permanent `ErrReqIDNotAllowed` (RF1: a broker-bus caller / generic `Forwarder.Forward` / future seam regression cannot reintroduce the stale-ledger false-success). Only `reconcile` carries a key (bootID epoch + protects D5 publish). Regressions: `TestD4Provision{ReqIDMustNotDedupAcrossEvict,NonEmptyReqIDMustNotFalseSuccessAfterEvict}_Review`.
- **External F2 — invalid forwarded ReqID fails closed BEFORE raft.** `ProposeWithReqID` validates a non-empty reqID (`validReqID`) before proposing → permanent error, never a poison entry surfaced to the caller as ok. Regression: `TestD4ForwardInvalidReqIDMustNotReturnOK_Review`.
- **Routing = broadcast `Subscribe` + leader-only-reply (NOT queue-group).** §2.4's "queue-group" was corrected during implementation: a queue group has no leader affinity (leader-address advertisement is D7), and a follower replying `not_leader` could race ahead of the leader's commit-round-trip `ok` → spurious retry. So the responder uses a broadcast `Subscribe` and ONLY the believed-leader replies; followers stay silent; election ⇒ no reply ⇒ timeout ⇒ retriable. Panel-confirmed sound (no double-commit / lost-write / false-allow). Doc §4.1 updated (doc-first).
- **Guard scan list excludes `cluster_forward.go`** (it defines the constructors). Guard tokens are `NewForwarder(`/`SubscribeClusterApply(`/`NewProvisionSeam(`/`NewJoinSeam(` + the D3 set, scanned over `serve.go`/`authcallout.go`/`broker.go`; a self-check proves discriminating power.
- **B1 (review) — forwarded business error keeps typed identity:** `ForwardBusinessError.Is` + a stable `forwardErrKind` registry (NOT `%T`) so `errors.Is(fwdErr, agentprov.ErrInvalidPIN)` holds and the forwarded bad-PIN path emits the same `pin_failed` + canonical deny as the local path.
- **M1 (review) — leader-local seam maps raft errors too** (mirrors the forward branch): `cluster.IsNotLeader → authcallout.ErrNotLeader`.
- **M5 (review) — `ReconcileReqID` uses `sort.SliceStable` + full-field tiebreak** so duplicate-key agent lists derive a stable key (§0bis-A refined).
- **M2 (review) — leadership-transfer idempotency gate asserts the NEW leader's `DedupCount==1` + per-replica row-count==1**, NOT the racy cross-node sum.
- **Reconcile dedup is doubly-idempotent** (review M4 adjudication): once committed, the re-resolve is empty (proc already EXITED) → nil command → the FSM ReqID-dedup branch is short-circuited by that natural no-op, so a synchronous reconcile re-forward shows `DedupCount==0` (correct). The op-agnostic dedup branch is proven by `TestD4FSMReqIDDedupSemantics`; the reconcile forward proves observable no-double-execute.
- **DEFERRED with rationale (NOT silently dropped):**
  - **M6 full stale-leader-in-lease-vs-new-majority gate** needs a transport-partition seam over the mTLS `NetworkTransport` (not built; D3 didn't build one). The achievable half — a leader that lost quorum fails closed (retriable, no commit) — IS tested (`TestD4ForwardLeaderLosesQuorumFailsClosed`, 3-node kill-2). The partitioned-stale-leader mechanism is panel-confirmed sound (its `Apply` blocks→`ErrLeadershipLost`→not_leader→retriable, no false-allow because commit needs quorum). Full partition-seam test deferred to a D-phase that builds the seam (D7 membership / D9 mass-reconnect e2e).
  - **m3 cross-node raw-log-read replay:** byte-identical replay is proven by an in-process `cluster.Command` JSON round-trip (a faithful proxy — raft replicates exact bytes, so node B's decode == node A's encode) PLUS the Body-replicates-to-follower test. A per-node raft-log-read seam (to decode the committed `Aux` off a second node) needs node-distinguishing test infra; deferred as marginal-confidence-at-infra-cost.

---

## 1. Scope

### 1.1 In scope
1. **Idempotency core:** activate `Command.ReqID` (drop the "RESERVED+INERT; that is D4" comment); originating-broker content-addressed per-verb key (R-3 + §0bis-A); FSM dedup via `cluster_reqid_ledger` in the same Apply txn (R-2/R-4); `appliedDedup` sentinel + `dedupCount` (R-4); decode-time ReqID charset guard (R-13).
2. **migration 0011** `cluster_reqid_ledger` (R-2).
3. **Forward wire:** broker-owned `internal/broker/cluster_forward.go` (forwarder + leader responder) over `proto.SubjClusterApply(verb)`; typed `{ok,not_leader,error}` reply; **broadcast `Subscribe` + leader-only-reply routing** (corrected from "queue-group" — §0bis-H); timeout = retriable-same-ReqID (R-8/R-9/R-10). `internal/cluster` stays nats-free; `ProposeWithReqID` (§0bis-F).
4. **PIN transparent forwarding:** broker-injected `JoinMemberWrite`/`ProvisionAgentWrite` impl forwards follower→leader when not leader (nil in production); proves D3-R3's deferred transparent path.
5. **Self-sufficient `ReconcileBatch`:** promote `resolveReconcileMarks`→pure full-classifier (`resolveReconcile`) emitting proc + orphan + port tuples; `PlanReconcileBatch` bakes the apply-inert ordered tuple field + the existing UPDATEs; bump `commandVersion`→2; pure `ReplayReconcileAudit`; live path byte-unchanged (R-6/R-7).
6. **Harness + tests:** new combined routed-NATS + mTLS-raft `test/d4` harness (R-11); post-commit-`ErrLeadershipLost` mechanism (§0bis-C); follower-answers + reply-drop injection (R-13); copied leak helper; `TestD4Matrix` into `test/e2e/all_phases_test.go`.
7. **Doc-first** amendments to §4.1/§5/§3.7/§6.3-boundary/§19-D4 (§4 below) BEFORE code.

### 1.2 Deferred (with reason)
- **D5:** post-commit single-writer publish of `ev.*/audit.*/sys.events`, `raft_index:kind:seq` dedup window, post-election sweep, JS `Replicas` reconfig. D4 bakes the replayable entry + proves replay; publishes nothing.
- **D6:** `server_id` home bridge, per-expose `home_broker`/epoch, REGISTER 6th field, `home_catching_up`, rehome. D4 forwards register's RECONCILE write only; assigns no home, changes no tunnel/REGISTER wire.
- **D7:** dynamic membership (`raft.AddVoter`/join-PoP), `cluster_nodes` writes, `ClusterNode*` ops, leader-address advertisement. D4 uses D3's static `BootstrapPeers` + broadcast leader-only-reply routing (§0bis-H).
- **D9:** production cutover — `broker.New` embeds `cluster.Node`, delete startup direct writes, single-WAL merge, `nats.conf` takeover, `--from-existing`. D4 seams stay nil in `serve.go`.

---

## 2. Component-by-component design

### 2.1 `Command.ReqID` activation + decode guard — `internal/cluster/command.go`
- Re-document `ReqID` as the cross-retry-stable, originating-broker-minted idempotency key (drop the inert comment).
- `NewCommandReqID(op, reqID, body...)` stamps it before `encode()`; `NewCommand` stays for empty-ReqID ops (D1/D2 unchanged).
- `decodeCommand`: after version/known-op checks, validate `c.ReqID == "" || isHexBounded(c.ReqID)` (≤64 hex, no NUL, valid UTF-8); a bad ReqID is POISON (`appliedPoison` advance, never honored).
- Bump `commandVersion` 1→2; add the apply-inert audit-tuple field to the `OpReconcileBatch` body shape (§2.5).

### 2.2 FSM dedup — `internal/cluster/fsm.go`
In `applyCommand`, after the `l.Index <= applied` index-skip and before the applier:
```go
if cmd != nil && cmd.ReqID != "" {
    seen, err := reqIDSeenTx(tx, cmd.ReqID)   // SELECT 1 FROM cluster_reqid_ledger WHERE req_id=?
    if err != nil { return nil, err }
    if seen {                                  // committed-but-ack-lost retry at a NEW index
        f.dedupCount.Add(1)
        if err := writeAppliedIndexTx(tx, l.Index, l.Term); err != nil { return nil, err }
        // applyFailHook / applyCommitGate seams remain reachable here
        if err := tx.Commit(); err != nil { return nil, err }
        committed = true
        return appliedDedup{l.Index}, nil      // op SQL skipped, ledger NOT re-inserted, cursor advanced
    }
}
```
On the NEW-ReqID path: run the applier, then `INSERT INTO cluster_reqid_ledger(req_id, raft_index) VALUES(?, ?)`, then GC `DELETE FROM cluster_reqid_ledger WHERE raft_index < ? - reqIDRetentionWindow`, then the existing `writeAppliedIndexTx` + commit — all one txn.
- New `type appliedDedup struct{ index uint64 }`; `Node.Apply` maps it to nil (success).
- New `dedupCount atomic.Uint64` (sibling to `reapplyCount`), read by tests + exposed via the Node for the e2e assertion.
- `reqIDRetentionWindow uint64 = 1 << 20` with the §0bis-D comment.

### 2.3 migration 0011 — `internal/storage/migrations/0011_cluster_reqid_ledger.sql`
```sql
-- Migration 0011 — cluster_reqid_ledger (distributed-broker D4 §4.1 cross-retry idempotency).
-- FSM-owned, written ONLY by FSM.Apply in the op txn. No CURRENT_TIMESTAMP (determinism §3.4).
-- Cluster WAL FSM DB only; zero blast radius to the frozen broker DB until the D9 merge.
CREATE TABLE IF NOT EXISTS cluster_reqid_ledger (
    req_id     TEXT    PRIMARY KEY,
    raft_index INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_reqid_ledger_index ON cluster_reqid_ledger(raft_index);
```
Pure CREATE, idempotent re-run (engine runs each file once by name); applied by the cluster DB open path that already runs `storage.ApplyMigrations`.

### 2.4 Forward wire — `internal/broker/cluster_forward.go` (new) + `internal/cluster/node.go`
- `cluster.ProposeWithReqID(reqID string, plan func(db *sql.DB)(*Command,error)) error` (§0bis-F): leader gate + `applyMu` unchanged; stamps `reqID` via `NewCommandReqID` before `Apply`. `Propose` delegates with `""`. New raft-free `cluster.ErrForwardNotLeader` sentinel.
- `cluster_forward.go`:
  - `type forwardEnvelope struct { ReqID, Verb string; Payload json.RawMessage }`; `type forwardReply struct { Status, ErrKind, ErrMsg string }`.
  - `Forwarder.Forward(ctx, verb, reqID string, payload []byte) (forwardReply, error)`: `Request` on `proto.SubjClusterApply(verb)` with a fresh inbox + the broker bus nkey, bounded timeout; decode reply; on `not_leader` / transport-timeout return `cluster.ErrForwardNotLeader` (retriable, SAME reqID on re-call); on `error` return the typed business error; on `ok` return nil.
  - leader responder: **broadcast `Subscribe(proto.SubjClusterApplyWildcard)` — only the believed-leader replies, followers stay SILENT (§0bis-H; NOT a queue group)** — decode envelope, build the verb's Plan closure from the payload, call `node.ProposeWithReqID(reqID, plan)`, map result → reply (`cluster.IsNotLeader(err)`→`not_leader`; nil→`ok`; else `error`+`ErrKind`). A non-leader answerer stays silent (never runs Plan on a stale replica — D3-R3/F1).
- Constructed only when a `cluster.Node` is injected; nil in production.

### 2.5 Self-sufficient ReconcileBatch — `internal/proc/plan.go` + `internal/broker/reconcile.go` + `internal/cluster/command.go`
- New `proc.ReconAuditTuple struct { Kind, NID, PID string; Port int; Name string; LocalPort int; RC *int; Ts time.Time }` and `proc.ReconcileBatchBody struct { Marks []ExitMark; Audit []ReconAuditTuple }`.
- Promote `resolveReconcileMarks`→`resolveReconcile(nid, req, procs, portRows, now) ReconcileBatchBody` — pure, no DB write, no pub; absorbs the orphan loop + port loop classification; captures `name/local_port/rc` into tuples; reproduces kind-gating (orphan→`RC=nil`); `Ts = now.Round(0)`.
- `PlanReconcileBatch(body)`: sort `Marks` by PID ASC (unchanged) via `markExitedSQL`; sort `Audit` proc tuples by PID-ULID ASC + port tuples by port ASC (R-7); bake `Marks`→`Body` SQL + `Audit`→the new inert field. Empty body → nil command (no-op).
- `ReplayReconcileAudit(body) []schema.AuditRecord` — pure decode in total order; reproduces `pubAuditProc`/`pubAuditPort` record shapes exactly (incl `RC=nil` orphan, RFC3339 `Ts`).
- LIVE `reconcileOnRegister` (`reconcile.go:101`): adopt `resolveReconcile` as the shared CLASSIFIER but keep applying inline `MarkExited` + inline `pubAudit*` + directive arrays UNCHANGED (byte-identical live behavior). NOT cut to the op path (D9).

### 2.6 PIN transparent forwarding — broker seam impl (`cluster_forward.go`) + `internal/authcallout/handler.go` (unchanged)
- The handler is unchanged (already has the seams + `ErrNotLeader`/`ErrFenced` + raft-free classification).
- Broker-injected `JoinMemberWrite`/`ProvisionAgentWrite` impl (external F1/RF1 — NO ReqID): if leader → `node.Propose(PlanJoin/PlanProvision)` locally (no key); else → `Forwarder.Forward("join"/"provision", "", payload)` (empty key); map `cluster.ErrForwardNotLeader`→`authcallout.ErrNotLeader`. Nil in production. The `cluster.apply` responder additionally REJECTS a non-empty ReqID on these verbs at the wire boundary (`ErrReqIDNotAllowed`, RF1), so no caller can reintroduce the stale-ledger false-success.

### 2.7 Doc + guards — `docs/distributed-broker-architecture.md`, `internal/cluster/testhooks.go`, `test/d4/`
- Doc amendments (§4) BEFORE code.
- `testhooks.go`: add whatever the §0bis-C de-risk spike concludes is needed (raft-confined post-commit seam if `LeadershipTransfer` alone is insufficient).
- `test/d4/regression_test.go`: extend the D3 source-scan with the D4 banned tokens (R-13).

---

## 3. §13.7 adversarial test plan (each gate → named test)

| Gate (§13.7 / EXIT) | Test (`test/d4` unless noted) | What it proves / vacuity control |
|---|---|---|
| #1 PIN-join forwarded follower→leader | **TestD4PINForwardedFollowerReachesLeader** | PIN forwarded commits exactly one row on every replica; agent sees ALLOW, never `not_leader`. |
| #1 election race | **TestD4PINJoinRacingElection** | Election in-flight (all answer `not_leader`) → retriable deny, NO `agent_provisioning` row on any replica. RED: a responder answering `ok`-on-`not_leader` goes red. |
| #1 stale-leader-in-lease | **TestD4PINStaleLeaderInLeaseNoFalseAllow** (R-13) | Stale-but-in-lease leader answering a forwarded PIN does NOT allow a join into a session tombstoned on the real leader; bounded by `MultinodeLeaderLeaseTimeout`. |
| #1 quorum loss | **TestD4PINJoinQuorumLossFailsClosed** (R-8) | Leader exists but cannot commit → transient deny within bounded time; no hang, no un-replicated local write. |
| #2 forwarding idempotency (post-commit `ErrLeadershipLost` retry ≠ double-execute) | **TestD4ForwardToDeposedLeaderNoDoubleExecute** (§0bis-C, R-5) | Forwarder ack-drop + `raft.LeadershipTransfer()`: entry commits+replicates (follower `applied_index` polled), retry with SAME ReqID hits new leader → `dedupCount==1`. RED: leader-minted/random-key variant double-executes. |
| #2 FSM unit | **TestD4FSMReqIDDedupSemantics** (`internal/cluster`, §0bis-B) | Injected counting applier: (a) new ReqID execs + inserts ledger + advances; (b) seen ReqID at new index → `appliedDedup`, applier NOT called, cursor advanced, success; (c) restart-replay same index → index-skip first, `dedupCount==0`; (d) empty ReqID = today's path byte-unchanged. RED: distinct-ReqID variant runs applier twice. |
| #2 atomicity | **TestD4DedupLedgerSameTxnAtomicity** (`internal/cluster`) | `applyFailHook` between op and commit → NEITHER op effect NOR ledger row survives rollback. |
| #2 timeout | **TestD4ForwardTimeoutIsRetriableNotFreshWrite** (R-8, R-13) | Reply-drop injection: forwarder re-forwards SAME ReqID; op executes once; timeout never treated as success or fresh write. |
| #2 GC bound | **TestD4ReqIDLedgerGCDeterministicAndBounded** (R-4) | Same-txn range-delete prunes the identical set on every replica; ledger bounded. T-G2: tiny window double-executes. |
| #3 ReconcileBatch byte-identical replay (read-only) | **TestD4ReconcileBatchByteIdenticalReplay** (`internal/proc` + `test/d4`) | Commit one entry through real ≥2-node raft; on a second node read the entry FROM THE RAFT LOG (live request gone), `ReplayReconcileAudit`→byte-identical across nodes. Vacuity: shuffle resolver input → same bytes; drop the audit blob → FAILS. |
| #3 live equivalence | **TestD4ReconcileEquivalence** (extend `reconcile_marks_test.go`) | LIVE `reconcileOnRegister` vs `resolveReconcile`→`PlanReconcileBatch`→replay on identically-seeded DBs: identical processes table AND identical audit record **SET/multiset** (R-7), incl killed_orphan-no-rc + PID-reuse-also-orphan. In-process audit sink captures live emission. |
| EXIT (no false-allow on deposed) | **TestD4DeposedLeaderRejectsWrite** | Write on deposed leader rejected (`ErrLeadershipLost`→`not_leader`), retry lands through new leader; ZERO rows on BOTH replicas when answered `not_leader`. |
| not_leader bounce | **TestD4FollowerAnswersNotLeader** (R-13) | Deterministic follower-answers injection (unsubscribe leader responder) exercises the bounce + bounded re-request. |
| build-and-prove invariant | **TestD4ProductionWiresNoClusterNode** (+ self-check, extend D3 guard, R-1/R-13) | `serve.go`/`authcallout.go`/`broker.go` (NOT cluster_forward.go — §0bis-H) wire no `cluster.New(`, no PIN seam, no `SubscribeClusterApply(`, no `NewForwarder(`/`NewProvisionSeam(`/`NewJoinSeam(`. |
| L-2 | **TestRaftConfinedToClusterPackage** (existing) | raft stays in `internal/cluster`; `cluster_forward.go` imports cluster (not raft); authcallout raft-free. |
| leak/-race | all `test/d4` behavioral tests | copied NumGoroutine poll-with-tolerance + fd-baseline gate (R-13); `-race`. |
| e2e regression net | **TestD4Matrix** (`test/e2e/all_phases_test.go`) | Subprocess (≥300s, mirror TestD3Matrix): forward happy path + idempotency + reconcile replay + not_leader smoke over the real combined harness. |

**Pre-gate (blocking):** re-run D1 kill-9 + D2 DIFF-1 + D3 §13.8 GREEN **before AND after** the `ExitMark`→tuple / `commandVersion` bump refactor (`PlanReconcileBatch` + the differential test are shared D2/D3 surfaces).

---

## 4. Doc-first amendments (architecture §0-§18, BEFORE code)

1. **§4.1** — append a "D4 实现定稿" block: (a) dedup ledger = dedicated `cluster_reqid_ledger` table via migration 0011 in the cluster WAL FSM DB (clarify "`r:ReqID` 取该键" = the envelope `r` field per §5, not a KV prefix); (b) ReqID = originating-broker content-addressed per-verb SHA-256 digest; **reconcile keys on the RAW forwarded request (sid/nid/bootID/sorted lists), NOT resolved decisions** (§0bis-A); (c) `appliedDedup` sentinel advances `applied_index` while skipping the op (distinct from rollback `appliedNoOp`); (d) in-Apply-txn deterministic GC keyed on `applied_index`, `reqIDRetentionWindow` sized to the reconnect horizon; (e) forwarder timeout = retriable-same-ReqID; (f) forwarder/responder = broker-owned (`internal/cluster` stays nats-free).
2. **§4.1 (G.1/ReconcileBatch)** — tuples baked as an apply-inert structured Command field (no audit table; audit lives only in JS `history-<sid>`); `killed_orphan` carries no `rc`; `Ts` monotonic-stripped to byte-match the live RFC3339 audit; total order proc PID-ASC + port port-ASC; pure `ReplayReconcileAudit`; D4↔D5 boundary (D4 bakes+proves replay, D5 publishes; D4 threads no `raft_index`, computes no dedup key); live-vs-op equivalence compared as a SET/multiset (live emission order unchanged).
3. **§5** — `commandVersion` 1→2 for the inert audit-tuple field (decoupled from proto v2); `ReqID` now live with the decode-time charset guard.
4. **§3.7** — the second idempotency mechanism (ReqID ledger, cross-retry) is complementary to the index-skip (raft re-delivery); gate ordering index-skip-first.
5. **§19-D4** — add a "D4 范围定稿（先改正文）" status paragraph mirroring D2/D3: build-and-prove (cutover-ready), NOT production cutover; cutover=D9; `serve.go` byte-untouched, seams nil, guard test locks it; verbs scoped to provision/join/reconcile.

---

## 5. Implementation order (single connected block)

0. **De-risk spike (§0bis-C):** confirm `raft.LeadershipTransfer()` timing + whether a post-commit `testhooks.go` seam is needed; record the result. *(blocking gate for the #2 test design only)*
1. **Doc-first** §4 amendments to `docs/distributed-broker-architecture.md`.
2. migration 0011 + `cluster_reqid_ledger` open-path wiring.
3. `Command.ReqID` activation + decode charset guard + `commandVersion`→2 + `NewCommandReqID`.
4. FSM dedup (`appliedDedup`, `dedupCount`, in-txn GC) + `ProposeWithReqID`.
5. `ReconcileBatch` self-sufficiency (`ReconAuditTuple`/`ReconcileBatchBody`, `resolveReconcile`, `PlanReconcileBatch`, `ReplayReconcileAudit`); keep live path byte-unchanged.
6. `internal/broker/cluster_forward.go` (forwarder + responder + PIN seam impl).
7. `test/d4` harness (combined routed-NATS + mTLS-raft) + leak helper + the §3 suite + guard.
8. `TestD4Matrix` into `test/e2e/all_phases_test.go`.
9. Pre-gate + full gates: `make test` + `make e2e` + `make lint` + `-race` + built-in NumGoroutine/fd leak gate (NOT goleak). Any not-green ≠ done.

---

## 6. Residual judgment calls already ruled (recorded, not re-opened)
- reconcile ReqID derivation **corrected** to raw-request keying (§0bis-A) — the synthesis's "resolved decisions" was unimplementable given follower-forwards/leader-resolves.
- non-vacuity via injected counting applier, **no new production op** (§0bis-B).
- #2 test via ack-drop + `LeadershipTransfer`, **de-risk spike first** (§0bis-C).
- `reqIDRetentionWindow = 1<<20` accepted (§0bis-D); D5 window separate.
- strict three verbs, **no `proc.create`** (§0bis-E).
- `ProposeWithReqID` additive, **`Propose` signature unchanged** (§0bis-F).
