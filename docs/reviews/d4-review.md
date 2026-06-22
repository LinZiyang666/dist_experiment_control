# D4 Adversarial Review — Synthesis (write-forwarding `apply.*` + cross-retry idempotency + self-sufficient ReconcileBatch)

> **SUPERSEDED-IN-PART (external review):** the provision/join content-addressed ReqID this internal review references (B1/M5 + the proposed `ProvisionReqID`/`JoinReqID` tests) was later REMOVED by external-review F1/RF1 — provision/join carry NO forwarding ReqID (enforced at the `cluster.apply` wire boundary); only `reconcile` keys. This document is the internal-review historical record; the FINAL D4 contract is in `docs/reviews/d4-plan.md` §0bis-H + `docs/reviews/d4-external-review.md` + architecture §4.1.
>
> Branch: `main` (proto v2). Mode: BUILD-AND-PROVE (production `cmd/tether/serve.go` wires no `cluster.Node`; cutover = D9).
> Panel: 5 dimensions — idempotency/dedup, forwarding/fail-closed, reconcile-determinism, scope/dependency/regression, test-sufficiency.
> Synthesizer cross-checked every load-bearing finding against the code (citations below are verified, not relayed from prose).
>
> **Adjudication protocol:** main process decides each finding ACCEPT / REJECT / DEFER. Reviewers do NOT modify implementation; proposed tests are suggestions only.

---

## Severity summary

| ID | Sev | Title | Dimension(s) |
|----|-----|-------|--------------|
| B1 | BLOCKER | Forwarded business error loses typed identity: `ErrKind` written but never read, and `%T` on a sentinel is non-discriminating → forwarded bad-PIN degrades deny shape + suppresses `pin_failed` | forwarding, test-suff |
| B2 | BLOCKER | `join` verb (NewJoinSeam / VerbJoin / JoinReqID) has ZERO behavioral coverage; the named §13.7 #1 "PIN-join" test actually forwards a *provision* | forwarding, test-suff |
| M1 | MAJOR | Leader-local PIN seam returns raw `raft.ErrNotLeader`/`ErrLeadershipLost` unmapped → deposed-leader self-write mis-classified as permanent deny (R-8 taxonomy break) | forwarding |
| M2 | MAJOR | Headline deposed-leader idempotency gate asserts `totalDedup()==1` but the value deterministically settles to 2 (both replicas dedup the replicated retry) → flaky exit-gate | idempotency |
| M3 | MAJOR | `error` (permanent business-error) forward-reply path is never exercised end-to-end → R-8 not_leader-vs-error taxonomy half-unproven | test-suff |
| M4 | MAJOR | Reconcile forwarding has NO dedup/idempotency test and the ReqID minters have NO derivation/stability/domain-separation unit tests | test-suff, reconcile |
| M5 | MAJOR | `ReconcileReqID` uses unstable `sort.Slice` over agent-supplied lists → duplicate-key entries derive DIFFERENT keys on retry (cross-retry-stable claim overstated) | reconcile |
| M6 | MAJOR | §13.7 #1 stale-leader-in-lease no-false-allow gate is missing (no partition seam to express it) | test-suff |
| M7 | MAJOR | §13.7 #2 reply-drop timeout-retriable gate is missing; headline test uses leadership-transfer instead of the §0bis-C ack-drop design | test-suff |
| M8 | MAJOR | Architecture doc still describes QUEUE-GROUP routing; shipped design is broadcast + leader-only-reply (CLAUDE.md §3 doc-first violation) | forwarding |
| m1 | MINOR | decode-time poison guards (commandVersion v1-shaped mismatch + validReqID charset) are implemented but UNTESTED | scope, reconcile, idempotency, test-suff |
| m2 | MINOR | Dedup-HIT branch commit atomicity / crash-recovery never tested (only NEW-ReqID path) | idempotency, test-suff |
| m3 | MINOR | Cross-node byte-identical ReconcileBatch replay proven only by in-process JSON round-trip, not a real second-node committed log entry | reconcile, forwarding, test-suff |
| m4 | MINOR | `now func() time.Time` param on NewProvisionSeam/NewJoinSeam is dead | forwarding |
| m5 | MINOR | Forward: a parseable reply with empty/unknown `Status` is treated as PERMANENT (fail-closed-direction asymmetry vs malformed=retriable) | forwarding |
| m6 | MINOR | Ts monotonic-strip (`.Round(0)`) is non-load-bearing AND untested with a monotonic-bearing time; rationale technically inaccurate | reconcile, forwarding, test-suff |
| m7 | MINOR | D4 guard has no negative-control/self-check (inconsistent with repo's other guards) | scope |
| m8 | MINOR | Stale doc/comment claims the guard scans `cluster_forward.go` (omission is correct; prose is wrong) | scope |
| m9 | MINOR | `reqIDRetentionWindow` is a package-global var mutated by a test; safe only because cluster tests are non-parallel | idempotency |
| m10 | MINOR | test/d4 leak gate checks only NumGoroutine, omitting the fd-baseline the plan (R-13) / CLAUDE.md §5 call for | reconcile |
| m11 | MINOR | Harness uses 75ms LeaderLeaseTimeout; comment claims D3 parity but `MultinodeLeaderLeaseTimeout=500ms` — lease window 6.6× smaller than D9 ships | test-suff |
| m12 | MINOR | gofmt struct-alignment nits in command.go/node.go (does not fail this repo's gate) | scope |

---

## BLOCKERS

### B1 — Forwarded business error loses its typed identity (`ErrKind` written but never read; `%T` on a sentinel is non-discriminating)

**Location:** `internal/broker/cluster_forward.go:209` (Forward builds `&ForwardBusinessError{Kind: reply.ErrKind, ...}`), `:286` (`marshalForwardReply` sets `ErrKind: fmt.Sprintf("%T", err)`) vs `internal/authcallout/handler.go:296` (`errors.Is(err, agentprov.ErrInvalidPIN)` branch) and `:304` (`default:`).

**Evidence (verified):**
- `grep ErrKind` confirms `.Kind` is *written* at `cluster_forward.go:286` and *read nowhere* — it is copied into the struct at line 209 and never consumed. The plan's §R-8 promise "ErrKind lets the broker re-map to the exact authcallout deny" is unimplemented.
- `agentprov.ErrInvalidPIN = errors.New("agentprov: invalid PIN")` (agentprov.go:34) is a sentinel, so `fmt.Sprintf("%T", err)` = `"*errors.errorString"` — **not discriminating** (same `%T` for every `errors.New` sentinel). Even if a reader were added, `%T` could not tell `ErrInvalidPIN` from `ErrSessionMissing`.
- `*ForwardBusinessError` has no `Is`/`Unwrap` (cluster_forward.go:105-110), so a forwarded `agentprov.ErrInvalidPIN` does NOT satisfy `errors.Is(err, agentprov.ErrInvalidPIN)` at handler.go:296 → falls into `default:` (handler.go:304 `return fmt.Errorf("provision: %w", err)`).

**Concrete failing case:** a follower forwards a provision with a WRONG PIN. Leader's `PlanProvisionWithPIN` returns `ErrInvalidPIN` → `marshalForwardReply` Status=`error`, ErrKind=`*errors.errorString`. Forward returns `*ForwardBusinessError{Msg:"agentprov: invalid PIN"}`. Seam returns it raw. Handler: `isNotLeader`→false, `errors.Is(..., ErrInvalidPIN)`→false → `default:` deny `"provision: agentprov: invalid PIN"` and **no `pin_failed` event emitted** (handler.go:297). The leader-local path, by contrast, yields the canonical `"invalid PIN"` deny **and** emits `pin_failed`.

**Why BLOCKER (panel split, synth ruling):** the forwarding reviewer rated this MAJOR; the test-sufficiency reviewer rated the same defect MAJOR. The synthesizer **promotes to BLOCKER**: this is an observable behavioral divergence between the local and forwarded write paths on a security-relevant event (`pin_failed` is the audit signal for PIN brute-force), and the R-8 typed taxonomy is the explicit deliverable that makes forwarding "transparent." A forwarded path that silently drops the `pin_failed` audit event is a security-observability regression vs the local path, not merely a missing test. It is fail-CLOSED (no false-allow — the deny still happens), so it is not a safety hole, but it defeats the named R-8 re-map deliverable.

**Suggested fix (for main process to adjudicate):** give `*ForwardBusinessError` an `Is(target)` that matches a STABLE per-sentinel kind code (NOT `%T`) — e.g. responder emits `ErrKind` from a small registry mapping `agentprov.ErrInvalidPIN`→`"invalid_pin"`, `session.ErrInvalidPIN`→`"invalid_pin"`, etc., and `Is` returns true when Kind matches the target sentinel's code — so `errors.Is(fwdErr, agentprov.ErrInvalidPIN)` holds and the handler emits `pin_failed` + the canonical deny. Alternatively re-map in the seam before returning.

**Proposed test:** `TestD4ForwardedBadPINMapsInvalidPINAndEmitsPinFailed` — forward a provision/join with a WRONG PIN; assert the handler deny message equals the local path's ("invalid PIN") AND the `pin_failed` EmitEvent fired exactly once (wire an EmitEvent capture). RED control on current code: `"provision: ..."` deny + zero `pin_failed`.

---

### B2 — `join` verb has ZERO behavioral coverage; the named §13.7 #1 "PIN-join" test actually forwards a *provision*

**Location:** `internal/broker/cluster_forward.go:248-255` (dispatchForward VerbJoin), `:322-344` (NewJoinSeam), `:122-123` (JoinReqID); tests in `test/d4/`.

**Evidence (verified):** `rg 'NewJoinSeam|VerbJoin|JoinReqID|JoinMemberWrite' test/d4 internal/broker/*_test.go internal/authcallout/*_test.go` returns:
- `test/d4/regression_test.go:27,31` — banned-token *strings* (the build-and-prove guard), not a behavioral call.
- `internal/authcallout/handler_seams_test.go:60` — a unit closure `h.JoinMemberWrite = func(...)`, not the real `NewJoinSeam`/forward path.
- `test/d4/forward_test.go:39,80` — both use `NewProvisionSeam` (provision), never `NewJoinSeam`.

So `session.PlanJoinWithPIN` over the real `cluster.apply` bus, the `VerbJoin` dispatch arm, `JoinReqID` derivation, and `NewJoinSeam`'s leader-local-vs-forward branch are **all dead-as-tested**. The plan §3 table names `TestD4PINForwardedFollowerReachesLeader` for PIN-JOIN specifically, but the implemented test forwards a PROVISION (`signedAgentReq` → `tether-agent:...` → roleAgent → ProvisionAgentWrite).

**Why BLOCKER (panel: both forwarding and test-sufficiency rated MAJOR; synth promotes):** §0bis-E justifies wiring exactly three verbs *because they exercise three distinct shapes* — provision (PIN write that can return a permanent business error), join (member INSERT OR IGNORE), reconcile (leader-side resolution). One of the three genericity-proof shapes is entirely unexercised, AND it maps directly to a named §13.7 exit-gate test that does not actually test what it claims. A regression in the join branch (wrong payload field, `JoinReqID`/`ProvisionReqID` collision, mis-wired `PlanJoinWithPIN`, leader/follower routing) would ship undetected. Combined with B1 (the business-error path is also the one a join would exercise), the forwarding write path is materially under-proven. Exit-gate §13.7 #1 is not met as written.

**Suggested fix:** add a real join-verb forward test; ideally re-route `TestD4PINForwardedFollowerReachesLeader`'s intent through `ensureMember`/`JoinMemberWrite` so the named gate tests what it says.

**Proposed test:** `TestD4MemberJoinForwardedFollowerReachesLeader` — seed an ACTIVE session; `h.JoinMemberWrite = broker.NewJoinSeam(c.nodes[fi], c.forwarder(fi), time.Now)`; Handle a `tether-cli:<sid>` connect-name + PIN-join token; assert ALLOW (no `not_leader`) and the `session_members` row replicated to BOTH node DBs. Add a same-`JoinReqID` retry twin asserting `DedupCount()==1` (INSERT OR IGNORE makes a row-count vacuous, so the dedupCount assertion is the real proof).

---

## MAJORS

### M1 — Leader-local PIN seam returns raw `raft.ErrNotLeader`/`ErrLeadershipLost` unmapped (R-8 taxonomy break on the deposed-leader self-write)

**Location:** `internal/broker/cluster_forward.go:303-306` (NewProvisionSeam leader branch) and `:327-330` (NewJoinSeam leader branch).

**Evidence (verified):** both leader-local branches do `return node.ProposeWithReqID(reqID, ...)` with **no** `cluster.IsNotLeader(err) → authcallout.ErrNotLeader` mapping. The *forward* branches (lines 312-316 / 336-341) DO the mapping. If leadership is lost between the `IsLeader()` check (303/327) and `ProposeWithReqID`'s internal gate (returns `raft.ErrNotLeader`) or during `Apply` (returns `raft.ErrLeadershipLost` — the canonical "committed but ack lost" case), the raw raft sentinel propagates to authcallout, where `isNotLeader` (handler.go:111) matches ONLY `authcallout.ErrNotLeader` → `default:` permanent deny.

**Concrete failing interleaving:** node was leader → `IsLeader()==true` → `ProposeWithReqID` → stepped down → `raft.ErrNotLeader` (or `Apply` returns `ErrLeadershipLost`) → seam returns it verbatim → handler classifies it as a generic `"provision: ..."` permanent deny instead of the transient `not_leader` R-8 mandates. This is the exact deposed-leader scenario the §19-D4 exit gate targets. **Not a false-allow** (no row commits without quorum), but it breaks the documented fail-closed taxonomy and surfaces a confusing terminal deny on a self-write under leadership loss.

**Untested:** every `NewProvisionSeam`/`NewJoinSeam` call in tests runs on the FOLLOWER (`c.nodes[fi]`, forward_test.go:39/80), so the `if node.IsLeader()` true-branch is never executed.

**Synth note:** kept MAJOR (not BLOCKER) because it is fail-closed — it cannot false-allow. But it directly touches the §19-D4 exit ("an old/deposed leader rejects the write → ErrLeadershipLost→fail-closed") and the R-8 taxonomy, so it should be fixed before sign-off. Closely related to M1/B1: the seam is the single place where raft errors should be translated, and it is asymmetric (forward branch maps, local branch doesn't).

**Suggested fix:** in both leader-local branches, mirror the forward branch's mapping: `if err := node.ProposeWithReqID(...); err != nil { if cluster.IsNotLeader(err) { return authcallout.ErrNotLeader }; return err }; return nil`.

**Proposed test:** `TestD4LeaderLocalLosesLeadershipMapsNotLeader` — build the seam on the CURRENT leader; force `ProposeWithReqID` to return `raft.ErrLeadershipLost`/`raft.ErrNotLeader` (TransferLeadership racing the propose, or a Plan-closure hook); assert the authcallout `Handle` output is classified `not_leader`, NOT a generic `"provision:"` deny, and no `agent_provisioning` row exists on either replica.

---

### M2 — Headline deposed-leader idempotency gate asserts `totalDedup()==1` but the value deterministically settles to 2 (flaky exit-gate)

**Location:** `test/d4/forward_test.go:157` (`if got := c.totalDedup(); got != 1`); helper `test/d4/setup_test.go:302` (`totalDedup` sums `DedupCount()` over all nodes).

**Evidence (idempotency reviewer reproduced it):** the retry entry M (proposed by the NEW leader F, same ReqID) is a committed raft entry that EVERY replica applies. F dedups it (0→1). When M replicates to the now-follower L, L ALSO holds the original ledger row (replicated from index N) and ALSO dedups M (0→1). Once replication settles, `totalDedup()==2`, not 1. The test passes only by reading the counter in the narrow window AFTER F commits+replies but BEFORE the follower L has asynchronously applied M. Under `-race` slowdown / scheduler jitter / faster follower-apply, the assertion reads 2 and fails spuriously.

**The implementation is CORRECT** — a replicated ledger SHOULD make every replica dedup the same committed entry (this is exactly the determinism D4 wants). Only the *assertion* (cross-node sum `==1`) is wrong, and it makes the headline §13.7 #2 proof flaky.

> The idempotency reviewer reports it "passes 10×" historically; the test-sufficiency reviewer's claim it "passes 10×" is about the current narrow window. The synthesizer accepts the reproduction: the assertion is a latent flake on a load-bearing gate and must be made deterministic.

**Suggested fix:** assert on the NEW leader's count after a settle, plus a per-node ledger-row-count: `waitForCond(5s, func() bool { return c.nodes[0].DedupCount()>=1 && c.nodes[1].DedupCount()>=1 })`, then assert the `agent_provisioning` row for `(lab, lab-1)` exists EXACTLY ONCE on BOTH replicas (no double-execute) and the new-leader `DedupCount()==1`. Do NOT assert the cross-node sum `==1`.

---

### M3 — `error` (permanent business-error) forward-reply path never exercised end-to-end (R-8 not_leader-vs-error taxonomy half-unproven)

**Location:** `internal/broker/cluster_forward.go:208-209` (Forward `default:`→`*ForwardBusinessError`), `:278-289` (`marshalForwardReply` `fwdStatusError`); no `test/d4` coverage.

**Evidence (verified):** `rg 'ErrInvalidPIN|ForwardBusinessError|fwdStatusError|ErrKind' test/d4 internal/broker/*_test.go` finds ZERO behavioral matches. The entire purpose of the typed `{ok,not_leader,error}` reply (R-8) is that a PERMANENT business error is NON-retriable while a transient is retriable; the `error` branch is never forwarded by any test. A regression that classified a bad PIN as `not_leader` (e.g. a `marshalForwardReply` switch mis-order, or `cluster.IsNotLeader` accidentally widening) would make the agent retry a genuinely-bad PIN forever (the D3-R3 flap the design forbids) with no test going red.

**Overlap:** this is the test-coverage twin of B1 (B1 = the code defect; M3 = the missing test that would have caught it). Adjudicate together — the proposed test for B1 also closes M3.

**Suggested fix / test:** `TestD4ForwardBadPINIsTerminalNotRetriable` — forward a provision with a WRONG PIN from the follower; assert Forward returns a `*ForwardBusinessError` (via `errors.As`), NOT `cluster.ErrForwardNotLeader`; assert no `agent_provisioning` row on any replica; RED variant asserting it is not retried.

---

### M4 — Reconcile forwarding has NO dedup/idempotency test; ReqID minters have NO derivation/stability/domain-separation unit tests

**Location:** `test/d4/forward_test.go:167-189` (`TestD4ReconcileForwardReplicates` forwards exactly ONCE, checks only `procStatus==EXITED`); `internal/broker/cluster_forward.go:120-153` (the three ReqID minters).

**Evidence (verified):** §0bis-A is a whole MATERIAL CORRECTION about deriving the reconcile ReqID from the RAW forwarded request, yet none of its properties is tested:
1. `TestD4ReconcileForwardReplicates` forwards once — never re-forwards to prove `DedupCount()==1` for the reconcile verb (cross-retry idempotency unproven for the very verb §0bis-A exists for).
2. No unit test that `ReconcileReqID` is STABLE under shuffled `LocalProcesses`/`LocalPorts` order (directly relevant to M5 — the sort is the thing that's supposed to make it stable).
3. No test that a DIFFERENT `bootID` yields a DIFFERENT key (the epoch property).
4. No domain-separation test (`ProvisionReqID` vs `JoinReqID` vs `ReconcileReqID` never collide on overlapping inputs).

`rg 'ReconcileReqID|ProvisionReqID|JoinReqID|digestKey' *_test.go` shows the minters are used only as opaque keys; there is no assertion ABOUT a key.

**Suggested fix / test:** add a reconcile dedup arm (forward the SAME `(bootID, lists)` twice, assert `DedupCount()==1`); add `TestReconcileReqIDDerivation` (package `broker`) asserting shuffle-stability, `bootID`-difference, and cross-verb domain separation. (See M5 for the shuffle-stability subtlety — the same test should pin the duplicate-key case.)

---

### M5 — `ReconcileReqID` uses unstable `sort.Slice` over agent-supplied lists → duplicate-key entries derive DIFFERENT keys on retry

**Location:** `internal/broker/cluster_forward.go:135` and `:146` (`sort.Slice` in `ReconcileReqID`).

**Evidence (verified + reviewer-demonstrated):** `sort.Slice` is NOT stable. The hash consumes elements in array order (lines 136-151). If two `LocalProcesses` share a PID (or two `LocalPorts` share a Port) but differ in other hashed fields, the relative order of the equal-key elements is input-order-dependent. The agent builds `LocalProcesses` by ranging a Go map (`internal/agent/agent.go` `for pid, rec := range a.procs`), whose iteration order is randomized per call, so two reconnect/retry registers of the SAME logical state can arrive in DIFFERENT array orders → different hashed segment order → different `ReqID` → the FSM does NOT dedup the retry → the leader re-resolves and re-applies the ReconcileBatch.

The reviewer demonstrated: `[{01a,w},{01b,z},{01a,y},{01a,x}]` vs the same content reordered sort to DIFFERENT element orders, yielding different SHA-256 inputs for the same logical reconcile.

**Blast radius (why MAJOR not BLOCKER):** bounded — the in-scope reconcile DB writes are `WHERE status='RUNNING'`-idempotent, so a missed dedup re-applies a no-op `MarkExited`; the audit re-emit is D5's concern (D4 publishes nothing). A duplicate PID from a non-malicious map-keyed agent is low-reachability, and `LocalPorts` come from a stable slice. So it is an edge/adversarial-input determinism gap, not an exit-gate failure. The plan's "identical inputs → same key" claim is true only because keys are *normally* unique (a total order).

**Synth note:** the idempotency and forwarding reviewers both certified `ReconcileReqID` "cross-retry stable" in their verified-correct sections — that certification is **overstated**; M5 is the correct refinement. Recommend ACCEPT and harden.

**Suggested fix:** use `sort.SliceStable` AND tie-break on ALL hashed fields so equal-key elements have a total order (procs by `(PID,State,StartTimeTicks,RC)`; ports by `(Port,Name,LocalPort,TokenHash)`). Qualify the plan claim to "when sort keys are unique," or document duplicate-key reports as out-of-contract + add decode-side rejection.

**Proposed test:** `TestReconcileReqID_StableUnderDuplicateKeysAndInputOrder` — req A `LocalProcesses=[{PID:01a,State:running,Ticks:1},{PID:01a,State:exited,RC:5}]`, req B = same two elements REVERSED; assert `ReconcileReqID(A)==ReconcileReqID(B)`. Repeat with two `LocalPorts` sharing `Port=15000` but different `Name`. Current code fails this; stable+full-tiebreak passes.

---

### M6 — §13.7 #1 stale-leader-in-lease no-false-allow gate is MISSING (no partition seam to express it)

**Location:** `test/d4` (absent); plan §3 table row `#1 stale-leader-in-lease` + R-13 (`TestD4PINStaleLeaderInLeaseNoFalseAllow`).

**Evidence (verified):** `rg 'StaleLeader|in.lease|tombstone|Partition|Disconnect' test/d4 internal/cluster/*_test.go` finds nothing for D4. The plan §3 table names THREE distinct #1 gates (`TestD4PINStaleLeaderInLeaseNoFalseAllow`, `TestD4PINJoinRacingElection`, `TestD4PINJoinQuorumLossFailsClosed`); the implementation collapsed all three into one `TestD4PINForwardNoLeaderTransientDeny` — which only covers the EASY no-leader/quorum-loss case (the survivor is a clean follower), not the hard stale-but-in-lease case.

The hard case: a deposed leader still inside its `LeaderLeaseTimeout` answers a forwarded PIN with `IsLeader()==true`, runs `ProposeWithReqID` (leader gate passes because `raft.State()==Leader`), reads its STALE local DB in Plan, and could allow a join into a session tombstoned on the real leader. `SubscribeClusterApply`'s only guard is `node.IsLeader()` (cluster_forward.go:229) — there is NO `LeaderContactStale` fence on the forward responder.

**The mechanism is sound** (the forwarding reviewer's verified-correct: a partitioned stale leader's `raft.Apply` blocks then returns `ErrLeadershipLost` because commit needs quorum → not_leader → retriable → no false-allow). But the single most important #1 no-false-allow property is **entirely unproven**, and there is no transport-partition seam over the mTLS `NetworkTransport` to express it.

**Suggested fix:** (a) add a 3-node harness with a partition seam (test-only blocking wrapper on the `NetworkTransport`) so a stale-in-lease leader answers a forward and the test asserts the write does NOT commit and NO member row appears on the majority; OR (b) if ruled untestable without a partition seam at D4, RECORD that ruling explicitly and add an FSM/Propose-level test that a Propose on a node that cannot reach quorum returns a not_leader-family error within bounded time and writes nothing locally. Do NOT silently drop the gate.

**Proposed test:** `TestD4PINStaleLeaderInLeaseNoFalseAllow` (3 nodes): elect L; tombstone session `lab` on the majority {F1,F2}; partition L; within `MultinodeLeaderLeaseTimeout` forward a PIN-join for `lab` that L still answers; assert not_leader/timeout and NO member row on F1/F2. (See m11 — run this at the production `MultinodeLeaderLeaseTimeout`, not 75ms.)

---

### M7 — §13.7 #2 reply-drop timeout-retriable gate is MISSING; headline test uses leadership-transfer instead of the §0bis-C ack-drop design

**Location:** `internal/broker/cluster_forward.go:195-202` (Forward: request err → `ErrForwardNotLeader`; unmarshal err → `ErrForwardNotLeader`); `test/d4` (no reply-drop test).

**Evidence (verified):** the headline §13.7 #2 invariant is "a NATS request TIMEOUT = retriable-with-the-SAME-ReqID, NEVER a fresh write — a timeout is not proof of non-commit." §0bis-C designed the headline test as a forwarder ack-drop (ignore the first reply). The implemented `TestD4ForwardAcrossLeadershipTransferNoDoubleExecute` uses `raft.LeadershipTransfer` (forward_test.go:147) instead — it proves cross-leader dedup but NOT the "entry committed, the REPLY was lost, the retry over the wire dedups" path. Two retriable surfaces are untested:
1. The malformed-reply branch (cluster_forward.go:200-202, `json.Unmarshal` failure → `ErrForwardNotLeader`) — ZERO coverage.
2. "committed under leader L, reply dropped, retry to the SAME still-leader L dedups" — never exercised. `TestD4ForwardIdempotentNoDoubleExecute` does two CLEAN forwards (both replies arrive); it never simulates a dropped/timed-out reply on a committed entry.

**Suggested fix / test:** `TestD4ForwardTimeoutIsRetriableNotFreshWrite` — a responder that commits the entry but Respond()s after a delay > the forwarder's first-call timeout; first Forward returns `ErrForwardNotLeader` (committed under the hood); second Forward (same reqID) returns nil; assert leader `DedupCount()==1`. Plus `TestD4ForwardMalformedReplyIsRetriable` — fake responder `msg.Respond([]byte("{not json"))`; require `errors.Is(Forward(...), cluster.ErrForwardNotLeader)`.

---

### M8 — Architecture doc still describes QUEUE-GROUP routing (CLAUDE.md §3 doc-first violation)

**Location:** `docs/distributed-broker-architecture.md:31, 47, 128` vs `internal/broker/cluster_forward.go:11-21` header + `:221` (`nc.Subscribe`, not `nc.QueueSubscribe`) + `:229-230` (silent follower).

**Evidence (verified):** the load-bearing routing decision (RULING #1) deviated from the plan-of-record (d4-plan §2.4 "queue group") to a BROADCAST `Subscribe` where only the believed-leader replies and followers stay SILENT. CLAUDE.md §3 mandates "实现中发现设计问题先改文档再改代码." The architecture doc §4.1 D4 block (line 128) still says retriable means "re-request the queue group," and the component map (lines 31, 47) still names "queue-group 应答." The single canonical routing decision is documented ONLY in a source-file comment; the plan-of-record contradicts shipped behavior. A future reader following the doc could reintroduce the very race the broadcast design avoids.

**The RULING #1 race argument itself is SOUND** (see Verified Correct) — only the doc is stale.

**Suggested fix:** amend architecture §4.1 D4 block + the §3 component map to describe broadcast + leader-only-reply + silent-follower (and WHY a follower must not reply not_leader: it could race ahead of the leader's commit-round-trip ok). Optionally a guard assertion that `SubscribeClusterApply` uses `nc.Subscribe` not `nc.QueueSubscribe`, to lock the decision against drift.

---

## MINORS

### m1 — Decode-time poison guards (commandVersion v1-shaped mismatch + `validReqID` charset) implemented but UNTESTED

**Location:** `internal/cluster/command.go:139-141` (version check), `:149-152/:158-169` (`validReqID`); only `crash_invariant_test.go:134` `TestFSM_PoisonEntry` exists (raw non-JSON blob).

Three reviewers independently flagged this as the strongest MINOR (R-6 / R-13 / §0bis-G plan-mandated test missing). `rg '"v":1|Version: 1|unsupported command version|invalid req_id'` over `internal/cluster/*_test.go` + `test/cluster/*_test.go` returns nothing. `TestFSM_PoisonEntry` feeds `[]byte("not-a-valid-command")` which fails at `json.Unmarshal` — it never reaches the version-check branch (command.go:139) nor the `validReqID` branch (command.go:149). Both new D4 decode branches — the literal safety net the `1→2` bump introduced (and the thing a D9 single-WAL merge / mixed-version encounter hits) AND the split-brain-ledger defense — are dead-untested. The code paths are correct (verified: `derr != nil → cmd=nil → appliedPoison` advances applied_index, never wedges, fsm.go:219-221), but the regression net the plan promised is absent.

**Suggested test:** `TestD4DecodeRejectsBadVersionAndReqID` — table including a `{"t":"ReconcileBatch","v":1,"b":[]}` v1-shaped entry, uppercase-hex / non-hex / 65-char / NUL-embedded ReqIDs (reject→poison), and empty + 32-char-lowercase-hex (accept). Assert each bad case is `appliedPoison`, applied_index advances, and a subsequent valid v2 entry still applies (not wedged).

### m2 — Dedup-HIT branch commit atomicity / crash-recovery never tested

**Location:** `internal/cluster/fsm.go:152-169` (dedup branch: `writeAppliedIndexTx` + `applyFailHook` + `applyCommitGate` + `tx.Commit`); test `reqid_dedup_test.go:14` `TestD4DedupLedgerSameTxnAtomicity` injects only on the NEW-ReqID path.

Verified: the dedup branch DOES carry `applyFailHook` (fsm.go:157) and `applyCommitGate` (fsm.go:162) — so the CODE is correct — but `TestD4DedupLedgerSameTxnAtomicity` arms the hook only on a fresh ReqID at index 2 (`reqid_dedup_test.go:20`), never on a dedup-HIT. The dedup branch's rollback/fail-stop window is exercised by NOTHING. A regression that advanced applied_index outside the dedup txn, or set `committed=true` before Commit, would not be caught.

**Suggested test:** apply a NEW ReqID at @2 (commits), arm `applyFailHook`, apply the SAME ReqID at @3 (dedup branch); assert `applyCommand` errors AND `mustApplied(f)==2` (NOT 3 — rollback), ledger unchanged, then clean re-apply at @3 is `appliedDedup` with applied_index advancing to 3.

### m3 — Cross-node byte-identical ReconcileBatch replay proven only by in-process JSON round-trip

**Location:** `internal/proc/reconcile_replay_test.go:45-68` (json.Marshal/Unmarshal in one process); `test/d4/forward_test.go:167-189` (`TestD4ReconcileForwardReplicates` checks only `procStatus==EXITED`).

Three reviewers flagged this. The plan §3 #3 gate specifies "commit through real ≥2-node raft; on a SECOND node read the entry FROM THE RAFT LOG, `ReplayReconcileAudit` → byte-identical across nodes." The shipped coverage does an in-process round-trip (a faithful proxy for raft's exact-byte replication, but not the same-node-gone scenario) PLUS a Body-replicates check — but NO test reads the committed `ReconcileBatch` `Aux` off a second real node's raft log and asserts `ReplayReconcileAudit` is byte-identical there. The Aux cross-node replay (the actual self-sufficiency claim) is not directly exercised end-to-end.

**Suggested test:** add a `cluster.Node` read seam (e.g. `LastAppliedCommandBytes()` or boltstore read) and, after the reconcile forward commits, decode the committed `OpReconcileBatch` on BOTH nodes and assert `ReplayReconcileAudit` yields byte-identical AuditProc/AuditPort on every replica. If no log-read seam is acceptable at D4, soften the plan gate wording and flag the gap explicitly.

### m4 — Dead `now func() time.Time` param on NewProvisionSeam / NewJoinSeam

**Location:** `internal/broker/cluster_forward.go:300, 324`. Both seam constructors take a `now` param that the returned closures never reference (they use the `t time.Time` argument supplied by authcallout for the leader-local Plan; the forwarded path bakes time at the leader responder's own `now()`). Dead and misleading. Suggested fix: drop the param (or thread it and document which clock wins). Lint/signature cleanup.

### m5 — Forward: parseable reply with empty/unknown `Status` is treated as PERMANENT (fail-closed-direction asymmetry)

**Location:** `internal/broker/cluster_forward.go:199-210`. A malformed (unparseable) reply → `cluster.ErrForwardNotLeader` (retriable, line 201); a reply that parses to `{Status:""}` or any unknown status → `default:` → permanent `*ForwardBusinessError` with empty Kind/Msg (line 209). The safe direction for an ambiguous/unrecognized reply is RETRIABLE (no commit proof), matching the timeout/malformed treatment. `marshalForwardReply` never emits an empty status today, so this is only reachable via a responder bug, but the asymmetry is a latent fail-closed-direction hazard. Suggested fix: `case fwdStatusError: return &ForwardBusinessError{...}; default: return cluster.ErrForwardNotLeader`.

### m6 — Ts monotonic-strip (`.Round(0)`) is non-load-bearing AND untested with monotonic-bearing time

**Location:** `internal/proc/plan.go:185-190`; tests use `time.Date(...)` (monotonic-free) at `reconcile_marks_test.go:213` and `reconcile_replay_test.go:19`. Two reviewers verified: Go's `time.Time.MarshalJSON` renders RFC3339Nano of the WALL component only and never the monotonic reading, so `json.Marshal(monotonic-bearing)` already byte-equals the `.Round(0)`'d value — the live `pubAuditProc` path does NOT `.Round(0)` yet produces identical JSON. The strip is harmless belt-and-suspenders, but (a) the plan/doc rationale ("so replayed JSON byte-matches") is technically inaccurate, and (b) every test seeds `time.Date` (no monotonic), so deleting `.Round(0)` would not break any test — vacuous coverage. Suggested fix: correct the doc rationale AND add a focused test feeding `time.Now()` (monotonic-bearing) through both arms asserting byte-identity, making the claim non-vacuous.

### m7 — D4 guard has no negative-control / self-check

**Location:** `test/d4/regression_test.go:15-38` `TestD4ProductionWiresNoClusterNode`. Asserts banned tokens ABSENT but no RED control proving `strings.Contains` has discriminating power — inconsistent with the repo's other guards (`TestRaftConfinementSelfCheck` at `lint_skeleton_test.go:191`, `TestNoStrayVersionLiteralSelfCheck`). Detection logic is trivial so practical risk is low, but a future refactor (typo making `strings.Contains` always-false) would leave the guard silently green. Suggested: add `TestD4ProductionGuardSelfCheck` running the predicate over a synthetic source containing a banned token (assert flagged) + a clean source (assert not).

### m8 — Stale doc/comment claims the guard scans `cluster_forward.go`

**Location:** `test/d4/regression_test.go:17-21` (scans `serve.go`/`authcallout.go`/`broker.go`) vs `cluster_forward.go:73` header comment + d4-plan §2.7/§3-table (all say the scan also covers `cluster_forward.go`). **The omission is CORRECT engineering** — `cluster_forward.go` DEFINES `NewForwarder`/`SubscribeClusterApply`/`NewProvisionSeam`/`NewJoinSeam`, so scanning it for those tokens would fail trivially. The implementation is right; only the prose is wrong. Suggested: correct the `cluster_forward.go:73` comment + d4-plan to drop `cluster_forward.go` from the scan list. Do NOT add it to the substring scan.

### m9 — `reqIDRetentionWindow` is a package-global var mutated by a test (safe only because cluster tests are non-parallel)

**Location:** `internal/cluster/node.go:41` (`var reqIDRetentionWindow`) + `reqid_dedup_test.go:51-53`. Changed from const to var so `TestD4ReqIDLedgerGCBoundedAndVacuity` can shrink it to 5 (mutate + defer restore). Correct ONLY because no cluster test calls `t.Parallel()` (`rg t.Parallel internal/cluster/*_test.go` → empty). A future parallel test would observe the shrunk window (cross-test contamination) and `-race` would flag a data race. Suggested: keep it const and thread the window through a test-only fsm field/option, OR add a comment/guard noting the no-Parallel requirement.

### m10 — test/d4 leak gate checks only NumGoroutine, omitting the fd-baseline

**Location:** `test/d4/setup_test.go:400-414` `assertNoGoroutineLeak`. R-13 said D4 copies the poll-with-tolerance NumGoroutine + **fd-baseline** helper; the copy uses only `runtime.NumGoroutine`. The forwarder opens NATS reply-inbox subscriptions per Request; a leaked subscription manifests as a leaked fd that a goroutine-only gate could miss if the goroutine is pooled. The repo's canonical gate (`test/concurrency/helpers_test.go`) is NumGoroutine + fd. fd risk is low (`nc.Request` uses an auto-managed inbox), but the plan committed to the fd gate. Suggested: port the fd-baseline half, or document the intentional omission.

### m11 — Harness uses 75ms LeaderLeaseTimeout; comment claims D3 parity but production is 500ms

**Location:** `test/d4/setup_test.go:247-248` (`LeaderLeaseTimeout: 75ms`), comment line 213-214 claims "the D3 integration harness" values; `cluster/node.go:66` `MultinodeLeaderLeaseTimeout=500ms`. The lease window is 6.6× smaller than production. The stale-leader-in-lease gate (M6) is explicitly bounded by `MultinodeLeaderLeaseTimeout`; even if that test existed, at 75ms it would not exercise the production-representative fail-open window. Suggested: correct the comment to stop claiming D3 parity, OR pin any stale-leader test's lease to `cluster.MultinodeLeaderLeaseTimeout` so the window matches D9.

### m12 — gofmt struct-alignment nits in command.go / node.go

**Location:** `internal/cluster/command.go:74-75`, `internal/cluster/node.go:74-76`. `gofmt -l` flags both after the D4 field additions. The reviewer verified this does NOT fail the repo's `make lint` (no `.golangci.yml`; golangci-lint v2 default does not run gofmt; pre-existing committed files `internal/agent/proxy.go`, `internal/proxydial/socks5.go` are also non-gofmt-clean yet passed prior gates). Purely cosmetic. Optional `gofmt -w`.

---

## Verified correct (panel-confirmed sound)

### The two scrutinized rulings

- **RULING #1 — broadcast + leader-only-reply + silent-follower routing: SOUND, no hole.** All five reviewers concur. Only one raft leader per term can commit; a deposed leader's `Apply` returns `ErrLeadershipLost`/`ErrNotLeader` (replies not_leader, no commit), so at most one `ok`. A stale-deposed-leader-replies-not_leader and a new-leader-replies-ok can both fire, but `nc.Request` takes the FIRST reply + auto-unsubscribes, and the content-addressed ReqID makes any retry dedup against the REPLICATED ledger at Apply time (fsm.go:147-171). No double-commit (op SQL + ledger INSERT are same-txn, serialized through raft; a second proposal at a new index dedups). No lost-write (a committed entry cannot be un-committed; a not_leader/timeout reply triggers a same-ReqID retry that either dedups or re-commits idempotently). During an election → all non-leader → all silent → timeout → retriable, no false-allow. (Doc is stale — see M8 — but the mechanism is correct.)
- **RULING #2 — `ReconcileReqID` from the raw forwarded request: COLLISION-SAFE and cross-retry-stable WHEN SORT KEYS ARE UNIQUE; the unconditional "cross-retry stable" certification is HOLED by M5.** Domain separation (verb prefix) + per-field NUL-terminated `writeSeg` segments + SHA-256 over the exact byte stream means a collision requires byte-identical inputs (→ identical resolution, so any collision is between logically-identical reconciles). `bootID` is a correct epoch. `StartedAt` is correctly omitted (resolveReconcile never reads it). **BUT** the stability claim relies on a total order over the agent-supplied lists, which `sort.Slice` does NOT provide for duplicate keys (M5). Panel verdict: sound for the normal unique-key case; harden per M5 for duplicate-key adversarial input.

### Other confirmed-sound items

- **FSM dedup correctness & same-txn atomicity:** index-skip runs strictly BEFORE the ReqID dedup branch (fsm.go:135 then :147), so same-index raft re-delivery stays on the cheap `reapplyCount` path; `appliedDedup` COMMITS (advances applied_index, fsm.go:154-169) rather than rolling back — no wedge; the NEW-ReqID path runs op SQL + `insertReqIDTx` + `gcReqIDLedgerTx` + `writeAppliedIndexTx` + commit all in ONE txn. `TestD4FSMReqIDDedupSemantics` (distinct `t:a`/`t:b` keys), `TestD4DedupLedgerSameTxnAtomicity` (NEW-ReqID rollback), `TestD4ReqIDLedgerGCBoundedAndVacuity` (shrunk window load-bearing RED) are non-vacuous.
- **Deterministic in-txn GC:** `gcReqIDLedgerTx` keyed purely on raft index (no wall-clock — confirmed in migration 0011), with an underflow guard. `raft_index` has INTEGER affinity so numeric comparison is correct (reviewer scratch-tested `'5'/'9'/'10'/'100'/'2e12'` compare as integers). Snapshot+replay vs full-replay converge to the identical ledger set.
- **ReqID charset guard + poison handling:** `validReqID` accepts empty or 1..64 lowercase-hex, intrinsically rejects NUL/non-UTF-8/uppercase/whitespace/oversize; a bad ReqID poisons the whole entry (`appliedPoison` advances applied_index, never wedges). (Code correct; UNTESTED — see m1.)
- **commandVersion 1→2 bump is wire-safe:** `Aux` is `json:"x,omitempty"`, `ReqID` is `json:"r,omitempty"`, so D1/D2 ops marshal identically except the `v` field; `decodeCommand` rejects `v!=2` as poison; `genericExecApplier` ignores `Aux`; D2 equivalence/DIFF + proc/broker suites pass under v2. (v1-shaped poison path UNTESTED — see m1.)
- **Live `reconcileOnRegister` is byte-UNCHANGED (zero regression):** git diff confirms the function body has zero +/- lines; the D2 `resolveReconcileMarks` was renamed/expanded to the pure `resolveReconcile` (used only by the op path + tests). `pubAuditProc`/`pubAuditPort` and `exec.go`/`expose.go`/`audit.go`/`transfer.go` are NOT in the diff. The only production-source changes are `broker.go` (3-line inert tap), `reconcile.go` (classifier rename), and cluster/proc library files.
- **`resolveReconcile` / `ReplayReconcileAudit` are genuinely PURE** (no receiver, no DB, no audit, no NATS); D4 publishes NOTHING (D5 boundary held). `TestD4ReconcileEquivalence_AuditSet` is a faithful non-vacuous differential (live `reconcileOnRegister` audit bytes captured via the `auditTapForTest` seam vs op-path `ReplayReconcileAudit`, multiset + cardinality 6 proc / 1 port), covering `reconciled_closed`(rc set), `killed_orphan`(RC=nil), PID-reuse (both kinds), port reconciled. `TestD4ReconcileBatchByteIdenticalReplay` has real RED controls (shuffle-invariance + dropped-Aux self-sufficiency).
- **L-2 confinement holds:** `internal/cluster` imports ZERO nats (go list -deps clean); `cluster_forward.go` (the nats adapter) lives in `internal/broker` and translates `cluster.IsNotLeader`/`cluster.ErrForwardNotLeader` → typed replies / `authcallout.ErrNotLeader`; authcallout stays raft-free (`isNotLeader` matches only `authcallout.ErrNotLeader`). `TestRaftConfinedToClusterPackage` + whitelist + self-check pass.
- **Build-and-prove invariant holds:** `cmd/tether/serve.go` is byte-UNCHANGED (not in the diff set); production wires no `cluster.Node`/`Forwarder`/responder/PIN seam (the seams stay nil = today's direct `ProvisionWithPIN`/`AddMember`); `TestD4ProductionWiresNoClusterNode` enforces it over the correct production-wiring tokens. `auditTapForTest` is an unexported nil-checked var, inert in production.
- **先父后子 respected:** only D0-D3 products used; D4 publishes nothing (D5 untouched), writes no `cluster_nodes`/membership/rehome (D6/D7 untouched), seams stay nil (D9 cutover untouched). `TransferLeadership`/`DedupCount` are test-only additions with no production callers. migration 0011 is forward-only/idempotent/contiguous with its order-guard updated. proto v2 unaffected; no v1-fleet hazard (forwarding subjects ride the D3-era `tether.v2` prefix).
- **`Node.ProposeWithReqID` preserves the D3-F1 leader gate** (`raft.State()!=raft.Leader → raft.ErrNotLeader` BEFORE planning, under `applyMu`), stamps reqID only into a non-nil planned Command, does NOT pre-check the ledger (dedup authoritative only in Apply); `Propose` delegates with empty reqID so D1/D2 ops are byte-unchanged.
- **`TestD4Matrix`** runs internal/cluster + internal/proc + internal/broker + internal/storage + test/d4 under `-race` (300s), mirroring D1/D2/D3 — the D4 surface is in the cross-phase regression net. The whole affected suite is green under `-race`; the only `./...` failure is a pre-existing flaky tunnel port-bind (`TestTunnelReconnectCtxCancelInterruptsBackoff`) unrelated to D4. `make lint` reports 0 issues.

---

## Overall assessment

**D4 is NOT yet at its §19-D4 exit. Two BLOCKERS + several MAJORs must be adjudicated first.**

The **core safety mechanism is sound**: the broadcast leader-only-reply routing (RULING #1) has no double-commit / lost-write / false-allow hole; the FSM dedup ledger is correctly same-txn atomic with `appliedDedup`-advances-not-rollback; the content-addressed reconcile ReqID is collision-safe and cross-retry-stable for unique keys; L-2, build-and-prove, 先父后子, and the byte-unchanged live `reconcileOnRegister` (zero regression) all hold. No false-allow or lost-write was found by any reviewer.

What blocks sign-off:

- **B1 (forwarded business error loses typed identity)** and **B2 (`join` verb has zero behavioral coverage; the named §13.7 #1 PIN-join test forwards a provision)** together mean the forwarding *write* path's error taxonomy and one of its three genericity-proof shapes are unproven, and a forwarded bad-PIN observably diverges from the local path (drops the `pin_failed` audit event). These are the panel's two MAJORs the synthesizer promotes to BLOCKER because they are behavioral divergence on a security-observability signal + an unmet named exit-gate, not mere coverage gaps.
- **M1 (leader-local seam returns raw raft errors unmapped)** is fail-closed but breaks the R-8 taxonomy on the exact deposed-leader self-write the §19-D4 exit names.
- **M2 (flaky headline §13.7 #2 assertion `totalDedup()==1`)** must be made deterministic before §13.7 #2 can be treated as proven — the implementation is correct, the assertion is wrong.
- **M3–M7** are missing/mis-aimed §13.7 exit-gate tests: the permanent-business-error reply path, reconcile dedup + ReqID derivation, the stale-leader-in-lease no-false-allow gate, and the reply-drop timeout-retriable gate. Several map 1:1 to named §13.7 tests that are absent or collapsed.
- **M5 (unstable `sort.Slice` in `ReconcileReqID`)** is a contained determinism gap (bounded by SQL idempotency, not exit-blocking) but should be hardened; it refines the panel's own overstated "cross-retry stable" certification.
- **M8** is a doc-first violation (CLAUDE.md §3) — fix the doc to match the shipped broadcast design.

**Recommendation:** main process should (1) fix B1 + M1 in the seam/error-mapping layer; (2) add the B2 join-forward test (re-aiming the named §13.7 #1 test); (3) fix the M2 assertion; (4) add the M3/M4/M6/M7 exit-gate tests or explicitly record an out-of-scope ruling for any judged untestable at D4 (especially M6's partition seam); (5) harden M5's sort; (6) update the architecture doc (M8). The MINORs (especially m1 — the plan-mandated poison tests) are high-value, low-cost follow-ups. Once B1/B2/M1/M2 and the M3–M7 gate coverage are resolved (or ruled), D4 meets its §19-D4 exit.

---

## 主进程裁定（Main-process adjudication）

> Every finding adjudicated ACCEPT (fixed in implementation/tests by the main process — reviewers do not touch implementation) or ACCEPT-as-DEFER (recorded ruling, not silently dropped). The panel's "core safety mechanism sound" verdict stands; all blockers + majors were coverage/divergence/doc, not safety. Re-gated after fixes: `make test` ✓ · `make lint` 0 ✓ · D1/D2/D3/D4 e2e matrices -race ✓ · `test/d4` -race incl. goroutine + fd leak gate ✓.

**BLOCKERS**
- **B1 — ACCEPT (fixed).** `ForwardBusinessError.Is` + stable `forwardErrKind` registry (NOT `%T`) in `cluster_forward.go`; `marshalForwardReply` emits the kind code; forwarded `agentprov.ErrInvalidPIN` now satisfies `errors.Is` → handler emits `pin_failed` + canonical "invalid PIN" deny. Tests: `TestD4ForwardedBadPINMapsInvalidPINAndEmitsPinFailed` (handler-level pin_failed + canonical deny) + `TestD4ForwardBadPINIsTerminalNotRetriable` (wire: ForwardBusinessError, errors.Is match, not retriable, no row).
- **B2 — ACCEPT (fixed).** Added `TestD4MemberJoinForwardedFollowerReachesLeader` exercising the join verb end-to-end (NewJoinSeam → VerbJoin → session.PlanJoinWithPIN → member row replicated) + a same-JoinReqID retry asserting DedupCount==1. The named §13.7 #1 now has a real PIN-JOIN test (the provision test stays as the provision shape).

**MAJORS**
- **M1 — ACCEPT (fixed).** Both leader-local seam branches now map `cluster.IsNotLeader(err) → authcallout.ErrNotLeader` (mirroring the forward branch). Covered indirectly by the existing fence/not-leader tests + the seam symmetry.
- **M2 — ACCEPT (fixed).** `TestD4ForwardAcrossLeadershipTransferNoDoubleExecute` now asserts the NEW leader's `DedupCount()==1` + EXACTLY ONE `agent_provisioning` row on BOTH replicas (via waitForCond), NOT the racy cross-node sum. `cluster4.totalDedup` helper removed.
- **M3 — ACCEPT (fixed).** Covered by the B1 wire test (`TestD4ForwardBadPINIsTerminalNotRetriable`: the `error` reply path → ForwardBusinessError, not retriable) + `TestD4ForwardWireRetriableClassification` (the not_leader/timeout vs error split).
- **M4 — ACCEPT (fixed, with adjudication).** Added `TestReconcileReqIDDerivation` (shuffle/dup-key stability, bootID epoch, cross-verb domain separation, charset) + `TestD4ReconcileForwardIdempotent`. Note (recorded in d4-plan §0bis-H): a reconcile is doubly-idempotent — once committed the re-resolve is empty (nil command, never reaches Apply) so the FSM ReqID-dedup branch is short-circuited (`DedupCount==0`, correct); the dedup branch itself is proven op-agnostically by `TestD4FSMReqIDDedupSemantics`, and the reconcile forward proves observable no-double-execute.
- **M5 — ACCEPT (fixed).** `ReconcileReqID` now uses `sort.SliceStable` + full-field tiebreak (procs by PID,State,Ticks,RC; ports by Port,LocalPort,Name,TokenHash). `TestReconcileReqIDDerivation` includes the duplicate-key reversed-input RED case.
- **M6 — ACCEPT (partial fix + DEFER).** Added `TestD4ForwardLeaderLosesQuorumFailsClosed` (3-node kill-2: a leader that lost quorum fails closed retriable, no row — the load-bearing no-false-allow property). The FULL stale-leader-in-lease-vs-new-majority gate needs a transport-partition seam over the mTLS NetworkTransport (not built; D3 didn't build one); DEFERRED to a D-phase that builds the seam (D7/D9), recorded in d4-plan §0bis-H. The partitioned-stale-leader mechanism is panel-confirmed sound.
- **M7 — ACCEPT (fixed).** `TestD4ForwardWireRetriableClassification` (malformed reply + no-responder/timeout → ErrForwardNotLeader) + `TestD4ForwardAckDropCommittedRetryDedups` (the §0bis-C gold case: responder commits then DROPS the first reply → forwarder times out retriable → same-ReqID retry dedups, DedupCount==1, one row).
- **M8 — ACCEPT (fixed).** Architecture §4.1 D4 block now describes broadcast `Subscribe` + leader-only-reply + silent-follower (and WHY a follower must not reply not_leader); the stale "queue group" / "再请求 queue group" wording corrected. d4-plan §2.4/§3/§0bis-H aligned.

**MINORS**
- **m1 — ACCEPT (fixed).** `TestD4DecodeRejectsBadVersionAndReqID` (v1-shaped + non-hex/uppercase/over-64 ReqID → appliedPoison, applied_index advances, FSM not wedged).
- **m2 — ACCEPT (fixed).** `TestD4DedupBranchSameTxnAtomicity` (applyFailHook armed on the dedup-HIT branch → rollback, applied_index unchanged, then clean re-apply dedups).
- **m3 — ACCEPT-as-DEFER.** Recorded in d4-plan §0bis-H: in-process Command JSON round-trip is a faithful proxy for raft's exact-byte replication; a per-node raft-log-read seam needs node-distinguishing infra, deferred.
- **m4 — ACCEPT (fixed).** Dropped the dead `now` param from `NewProvisionSeam`/`NewJoinSeam`.
- **m5 — ACCEPT (fixed).** `Forward` now routes an empty/unknown reply Status to the RETRIABLE direction (`cluster.ErrForwardNotLeader`), not a permanent deny.
- **m6 — ACCEPT (fixed).** Corrected the Round(0) rationale (JSON already strips monotonic; Round(0) is defensive for time.Time equality) + added `TestD4ReconcileReplayMonotonicTimeByteIdentical` feeding a monotonic-bearing time.Now().
- **m7 — ACCEPT (fixed).** `TestD4ProductionGuardSelfCheck` + `d4BannedTokens` SSOT shared by the guard and the self-check.
- **m8 — ACCEPT (fixed).** d4-plan guard-scan list corrected to exclude `cluster_forward.go` (it defines the constructors); §0bis-H records it.
- **m9 — ACCEPT (fixed).** `reqIDRetentionWindow` var comment now notes the no-`t.Parallel()` safety requirement.
- **m10 — ACCEPT (fixed).** `test/d4` leak gate now checks fd baseline (`fdCount`/`assertNoFDLeak`) alongside NumGoroutine.
- **m11 — ACCEPT (fixed).** Harness comment corrected: 150/150/75ms are fast TEST values, NOT `cluster.Multinode*` (1000/1000/500ms).
- **m12 — ACCEPT (fixed).** `gofmt -w` on the touched files.
