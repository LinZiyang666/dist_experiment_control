# D5 Internal Review — Re-derivable Audit Publish + JS Stream Replica Reconfig

> **Stage C (CLAUDE.md §3 step 4–5).** Adversarial internal review by 5 Opus 4.8 reviewers (distinct dimensions: correctness/consistency · concurrency/leak/lifecycle · architecture-invariants · test-rigor/vacuity · security/edge) + 1 synthesizer (workflow `wf_9c5d44ca-118`, 6 agents, ~623k tok). **Adjudicated + fixed by the main process** (sole implementer). Reviewers read the implementation and added tests; only the main process edited implementation.

## Reviewer verdict: **CONDITIONAL PASS**

The D5 *mechanism* is correct on the happy + adversarial paths (at-least-once + R-22 queue-not-drop hold in code; the `q<reqID>`/`r<idx>` dedup-id grammar never collides; `seq` is deterministic across leaders; the FSM-baked monotonic guard is a clean 0-row no-op on regress; the L-2 import guards are sound; `serve.go`/`authcallout.go`/live `publishAudit` are byte-unchanged; `ReplicasFor`/`ActualReplicas` correct). The CONDITIONAL findings are correctness/lock defects + load-bearing test vacuities — none a data-corruption BLOCKER at N≤3 today, but exit-criteria-critical (the suite stayed green even if JS dedup silently failed, or the publisher were wired into prod via a struct literal).

## Main-process adjudication summary

**All findings ADJUDICATED; all real ones FIXED. Verdict after fixes: PASS (pending external review).** Disposition:

| # | Sev | Finding | Disposition |
|---|-----|---------|-------------|
| **B1** | BLOCKER | R-12 single-writer "identical-id collapse across an election" proven by NO non-vacuous test (the sweep tests are structurally satisfied by the checkpoint-skip) | **ACCEPTED + FIXED.** Replaced with `TestD5DedupCollapsesIdenticalID`: the publisher publishes (count 0→1, non-vacuous), a re-publish of the publisher's EXACT id COLLAPSES (stays 1), a DIFFERENT id does NOT (grows to 2 — the control arm). Proves the Duplicates-window + msg-id-grammar wire mechanism directly. |
| **M1** | MAJOR | `IsMetaGroupNotReady` misclassifies the PERMANENT JS-10074 "non-clustered mode" error as retriable → reconcile spins forever, masked by `Degraded()` | **ACCEPTED + FIXED** (`internal/jsstream/replicas.go`): exclude `"non-clustered"`/`"not supported"` as permanent; drop the over-broad `"replicas > "`/bare `"peers"`; keep `"no suitable peer"`/`"no peers"`/`"insufficient"`/`"not enough"`. Test updated with the 10074 + `"lost connection to peers"` negatives. |
| **M2** | MAJOR | R-7's named invariant `CommitIndex − checkpoint < TrailingLogs` is unimplemented (the whole justification for the bounded accepted-loss) | **ACCEPTED + FIXED.** Added `cluster.TrailingLogs` const (+ `TestD5TrailingLogsMatchesRaft` drift guard) + a lag-bound check in `PublishOnce` (`LagExceededCount` + loud Warn, `MaxLag` defaults to `cluster.TrailingLogs`) + `TestD5PublishLagBoundFires`. |
| **M3** | MAJOR | R-22 queue-not-drop asserted only by eventual stream-presence (not the mandated checkpoint-non-advance); joint test is steady-state not kill-during-expand | **ACCEPTED + FIXED.** New deterministic `TestD5QueueNotDropCheckpointStaysOnFailure` (publish to a not-yet-created stream → checkpoint STUCK + re-run still stuck → create stream → advances + exactly-once, no loss). Joint test restructured to a reconfig-in-flight kill + a `cpAfter > cpBefore` checkpoint assertion. |
| **M4** | MAJOR | The build-and-prove guard's primary token `"auditPublisher"` is DEAD (case-mismatch vs exported `AuditPublisher`); a struct-literal cutover evades; only 3 files scanned | **ACCEPTED + FIXED** (`test/d5/regression_test.go`): ban `"AuditPublisher"` (catches the type, `NewAuditPublisher(`, `&AuditPublisher{`) + `"AuditPublisherConfig"`; widen the scan to ALL production `internal/broker/*.go` (minus `audit_publisher.go`/`_test.go`) + serve.go; self-test now asserts a struct-literal cutover is flagged. |
| **M5** | MAJOR | R-5 batching (cap 256) entirely untested; the mid-drain `flush()` branch is dead-untested | **ACCEPTED + FIXED.** `TestD5PublishBatchedCheckpoint` (commit=1000, Batch=256 → `advanceCalls==4`, not 1, not 1000). |
| **M6** | MAJOR | Missing A-cases: E-A5 (proc+port same-seq both survive), E-A16 (follower-silent over the wire), E-A7/A11 | **ACCEPTED (high-value) + FIXED.** `TestD5ProcPortSameSeqBothSurvive` (depth==2) + `TestD5FollowerNeverPublishes` (Run on a follower, `OnPublish` tap stays 0). E-A7 (real mid-loop snapshot) / E-A11 (killed_orphan nil-rc distinguished) DEFERRED — the `fakeReader` mid-loop re-clamp is covered by the m1 unit test + D4's byte-identical replay covers killed_orphan nil-rc; logged here as a documented follow-up. |
| **m1** | MINOR | Truncation branch overshoots the captured `CommitIndex` ceiling (missing `min(first-1, hi)` clamp) | **ACCEPTED + FIXED** (`PublishOnce`: clamp `lossTo` to `hi` + `break` at ceiling) + `TestD5TruncationNeverExceedsCommit`. |
| **m2** | (clean) | `AdvanceAuditPublished` pre-read skip race is benign | **NO ACTION** (confirmed benign: single-writer + the FSM WHERE-guard no-ops a stale-low re-propose). |
| **m3** | MINOR | `zz_castprobe_test.go` is an assertion-free scratch probe | **REJECTED — no such file exists** (the reviewer hallucinated it; `git status` confirms it was never created). |
| **m4** | MINOR | Monotonic-guard `CAST` saturates at 2^63; comment over-claims | **ACCEPTED (doc) + FIXED** — narrowed the `auditcursor.go` comment to "valid for indices < 2^63 (the practical raft range)". Physically unreachable; no code change. |
| **m5** | MINOR | `aux.SID` is an unvalidated trust boundary into the JS subject | **ACCEPTED (defense-in-depth) + FIXED** — `publishReconcile` now `proto.ValidateSID`-guards the sid and loud-skips a malformed one (subject-injection guard). |
| **m6** | MINOR | Loop-exit latency bounded by `ApplyTimeout` (the R-14 comment over-sells); sem-acquire not ctx-aware | **ACCEPTED (comment) + FIXED** — tightened the `publishTimeout` comment to acknowledge the checkpoint-Apply exit tail; widened `TestD5RunPublishesAndNoLeak`'s exit window. The sem-acquire is safe (every worker JS op honors ctx); documented. |
| **m7** | MINOR | `kDrain=10` hardcoded in the window assertion | **ACCEPTED (as-is)** — the window/election sides ARE by-reference (non-vacuous); `kDrain` mirrors the `kFence` k-factor discipline (documented in the test). |
| **T-fanout** | — | E-B9 bounded fan-out shipped by a reviewer as `fanout_review_test.go` but untracked | **ADOPTED** — promoted into the committed suite (shrunk to 20 sessions to fit the 32 GiB JS store quota). Asserts in-flight high-water ≤ MaxPar (>0). |
| **harness** | — | Reviewers left cosmetic gofmt churn in unrelated D1/D4 test files (`deps_test.go`, `snapshot_test.go`, `reconcile_marks_test.go`) | **REVERTED** — pure whitespace noise, no D5 relevance. |

### Main-process post-review divergence (binding)

- **MP-7 — the heavy clustered-JetStream `test/d5` integration suite is gated behind `//go:build d5_integration`** and runs ONLY in the dedicated `TestD5Matrix` subprocess (which passes `-tags d5_integration`), NOT in the parallel `make test`. Reason: a 3-node embedded clustered-JS + mTLS-raft cluster per test is heavy; under `go test ./...` (≈30 concurrent package binaries) the JS clusters starve into "no response from stream" / meta-formation timeouts (confirmed: every flaked test passes in isolation + under `-race` alone). Same precedent as the `e2e_matrix`-gated suite. The **cheap** d5 tests (the build-and-prove + L-2 import guards, the dedup-window assertion — no JS cluster) are NOT gated and run in `make test`. The internal/{cluster,broker,jsstream} D5 UNIT tests are likewise in `make test`.

---

## Gates after fixes (all green)

- `make test` ✓ (the heavy d5 integration suite gated out; the cheap d5 guards + the cluster/broker/jsstream D5 unit tests run).
- `make lint` ✓ (0 issues).
- `make e2e` → `TestD5Matrix` ✓ under `-race` (the full clustered-JS behavioral suite, ~45s) + all other phase matrices.
- `test/d5 -tags d5_integration -race` ✓ in isolation (incl. the NumGoroutine + fd leak gate with warm-up bracketing).

## Genuinely solid (do NOT touch — per reviewers)

The cursor/sweep/dedup mechanism (at-least-once + R-22 in code, no id collision, deterministic seq, monotonic guard with no regress hole); `TestD5ForwardedReconcileNoDoublePublish` (E-A8, genuinely adversarial — `DedupCount==1` proves the 2nd entry, reqID-keying collapses); concurrency/lifecycle (one loop, constant goroutines across flaps, child publish-ctx, bounded joined fan-out, race-clean); the L-2 import guards + the new op riding `genericExecApplier` without breaking the apply-reachability lint; the msg-id deriving only from trusted data (no agent field reaches the subject/id); the SQL literal bake injection-safe.
