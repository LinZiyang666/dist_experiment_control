# R16 + G67 + G69 external review tasklist

Date: 2026-07-22
Reviewer role: independent external reviewer
Base: `b602fc7`
Initial review boundary: 71 staged files, approximately `+6515/-325`; no unstaged implementation
change existed when the review began

The working tree combines three release-significant increments: R16 recovery/grow and transfer
repair, G67 transient JetStream provisioning handling, and G69's bounded JS-placement gate for
`cluster add`. Internal plans, internal-review claims, recorded test output, and the pre-written
`r16-external-review-tasklist.md` are context and leads only. They are not accepted as evidence.

## Boundary, contracts, and review method

- [x] Read `CLAUDE.md`, the requirements/architecture baselines, distributed-broker contracts,
  relevant operator docs, the R16/G67/G69 plans, simcluster mandate, device/server notes, and
  representative prior external-review reports/tasklists.
- [x] Freeze and record the actual Git boundary: all 71 candidate files were staged relative to
  `HEAD=b602fc7`; `git diff` was empty despite the request describing an unstaged boundary.
- [x] Rough-read and group the modification map before detailed review: recovery and JS reset,
  restore snapshot, grow sequencer, transfer durability/GC, provisioning retry and CLI
  classification, join placement gating, tests, drills, verdict ledger, and docs.
- [x] Reconcile every shipped claim and deliberate deferral with the governing requirements,
  architecture, plans, gotcha ledger, CLI help, and runbooks; report contradictions or claims
  stronger than the implementation/evidence.
- [x] Preserve reviewer independence: derive findings from source and independent tests/probes;
  do not treat internal verdicts or recorded green runs as proof.

## R16: JetStream store reset and recovery safety

- [x] Audit `JSStoreHasData` and `MoveAsideJSStore` for fail-closed behavior, empty/skeleton
  classification, symlink and special-file handling, permissions/ownership, rename atomicity,
  cross-filesystem behavior, backup collisions, sentinel ordering, fsync/durability, directory
  recreation, crash points, and repeated incidents.
- [x] Trace all four move-aside callers (former-N1 cutover, returning joiner, offline
  force-single, and `reconcile nats --to-standalone`) for consistent acknowledgement semantics,
  exact target/store discovery, stable backup naming, no implicit deletion, honest data-impact
  text, safe resume, and no accidental reset of a fresh/live store.
- [x] Audit force-single journal evolution and the new JS-reset epoch across first run, old-format
  journals, interrupted raft rebuild, refusal then retry with `--reset-js`, repeated incident,
  journal corruption/symlink, phase completion, and CLI exit status.
- [x] Audit restore's new `GrowReadySnapshot` ordering against Raft/SQLite applied indices,
  audit-published index, snapshot/log truncation, direct-installed rows, existing snapshots,
  kill-9 forward completion, marker clearing, and failure cleanup.
- [x] Audit `cluster add` returning-joiner reset and boot-grace loop for correct local config path,
  correct data directory, operation-ID binding, old nats process writes, context cancellation,
  timer/goroutine leaks, single-mode false positives, and bounded truthful failure.
- [x] Verify remedies/help/runbooks use commands that are actually executable in the stated online
  or offline state and that all copies include the `--reset-js` gate where required.

## R16: durable transfer finalization and cross-home GC

- [x] Trace in-flight ledger creation from both push and pull start paths through every terminal,
  forward-failure, shutdown, watchdog, and cleanup path; verify write-before-forward ordering,
  mode/ownership, atomic replacement, fsync semantics, stable immutable fields, path safety, and
  behavior on partial/corrupt/unreadable files.
- [x] Audit boot/periodic finalization for home ownership, tracker races, age computation,
  deterministic audit identity, duplicate/contradictory terminal handling, JS object deletion
  order, retry after partial failure, leadership changes, and permanent-home-loss disclosure.
- [x] Audit cross-home GC's full deletion ladder: caught-up leader gate, split/zero-home proof,
  active-object-store exclusion, per-object age, deleted-object handling, clock skew, JS list
  errors, concurrent new transfers, rehome races, leadership loss, and gauge convergence.
- [x] Verify `xfer_cross_home_reap_age` parsing/wiring/default relation to tier-B timeout, upper/lower
  bounds, production documentation, and drill-only compression without creating a dangerous
  operator tuning escape.
- [x] Inspect all R16 transfer tests for non-vacuity and mutation strength, especially the three
  terminal trailers not directly covered, deterministic dedup, permanently dead home, active
  bucket races, and drill 96's admitted non-exercise of #57/#58.

## G67: transient provisioning and client capability decisions

- [x] Audit `IsTransientProvisionErr` permanent-first ordering against the pinned nats-server API
  codes and wrapped error shapes; verify cancellation, deadlines, no responders, already-created
  races, unsupported/non-clustered/storage errors, and fail-permanent default.
- [x] Audit `provisionXferBucket` sizing and create contexts, total budget, retry count/backoff,
  timer cancellation, shutdown behavior, parent deadline, attempt accounting, logging, and
  idempotent create/replica-raise behavior.
- [x] Quantify and judge head-of-line impact on broker NATS subscriptions; verify one slow
  provisioning request cannot starve unrelated transfer/control traffic beyond documented bounds.
- [x] Verify push and pull run provisioning entirely before tracker/ledger/audit/data-plane side
  effects and retain all pre-existing admission and cleanup invariants.
- [x] Audit classified capability probing and optimistic tier-B fallback for malformed, refused,
  timed-out, absent-JS, non-member, and small-`max_payload` cases; verify tier-A ceiling never
  expands on missing measurements and pull/push remain semantically aligned.
- [x] Verify all transfer refusal sites preserve text/wire compatibility while returning the
  documented exit class; cross-check `docs/usage.md`, hints, proto comments, golden command tree,
  and old-client/mixed-version behavior.

## G69: join JS-placement gate and membership liveness

- [x] Audit `AssignedReplicas` for every possible `StreamInfo` shape, nil peers, offline/stale
  assignments, standalone streams, configured-vs-assigned mismatch, and N≥4 capped-replica
  semantics without changing `ActualReplicas`, `Ready`, or retire safety.
- [x] Audit `clusterJSPlaceable` and its production wiring for correct voter count, events stream
  selection, timeout/cancellation, nil seam behavior, observation error detail, and no hidden
  dependence on a stale/corpse assignment.
- [x] Drive `jsPlacementAdvance` through success, repeated false, probe error, zero deadline,
  already-expired deadline, reserve boundary, clock jump, leader change, crash/resume, and
  force-single recovery-grow; prove it cannot wedge or prematurely unblock the membership plane.
- [x] Re-check interaction with the preceding fail-closed `topoAdvance`: reserve arithmetic versus
  observe cadence and probe latency, deadline ownership, stale timeline writes, BLOCKED behavior,
  and whether load correlation can still produce the wrong terminal cause.
- [x] Verify the gate is join-only, creates no idle Raft-write churn, does not move
  `PlanClearGrowActive`/`PlanClearForceSingle` unsafely, and leaves retire/N=1 behavior unchanged.
- [x] Judge whether "events assigned at target" is sufficient for the actual claim made; distinguish
  structural placement evidence from CreateObjectStore liveness, disk constraints, and current
  peer health in the final report.

## Cross-cutting correctness, security, concurrency, and compatibility

- [x] Audit new filesystem operations for root/service-user boundary violations, symlink/TOCTOU,
  unsafe permissions, secret/path disclosure, unbounded files, and attacker-controlled names.
- [x] Audit new clocks/deadlines/retry loops for deterministic tests, overflow, negative/zero
  duration, cancellation, leak-free timers, lock ordering, and bounded resource use.
- [x] Audit all changed public flags/config fields/wire-adjacent codes for backward compatibility,
  strict same-version policy, dry-run behavior, stdout/stderr discipline, and automation-safe
  nonzero exits.
- [x] Run static searches for ignored errors, swallowed contexts, stale TODOs, dead helpers,
  unsafe `time.After` loops, direct filesystem deletion, and copy/pasted remedy drift in the
  touched call graph.

## Tests, simcluster truthfulness, and independent evidence

- [x] Inspect every changed drill/helper and `expected-verdicts.tsv` row against the simcluster
  mandate: no product workaround, weakened oracle, hidden manual lifecycle step, retroactive
  assertion widening, false GREEN, vacuous arm, or incorrect SETUP/PRODUCT/ASSERT/INCOMPLETE class.
- [x] Validate shell syntax and run the hermetic simcluster self-tests/ledger checks relevant to the
  edited drills and expected verdicts.
- [x] Add reviewer-owned tests for any high-risk branch not independently proven by existing tests;
  use mutations/counterexamples where practical and do not modify product implementation.
- [x] Run focused Go tests for `natsconf`, `clusteroffline`, `cluster`, `broker`, `jsstream`,
  `serveconf`, and `cmd/tether`; repeat timing-sensitive tests and run affected concurrency
  surfaces under `-race` plus available leak checks.
- [x] Read current simcluster server state and run the smallest relevant deploy-tier set needed to
  validate disputed/high-risk behavior, including loaded grow/placement and recovery/reset paths;
  preserve exact run IDs, verdicts, non-vacuity counters, and limitations.
- [x] Run formatting/diff checks, vet/static build, `make lint`, `make test`, and `make e2e` once as
  final release gates; classify environment failures separately and never infer a pass from
  internally recorded output.

## Deliverable and staging

- [x] Write `docs/reviews/r16-g67-g69-external-review.md` beginning with `Fail` or `Pass`, then scope,
  severity-ranked findings, doubts/questions, recommendations, independent verification,
  deploy-tier evidence, and explicit residual coverage limits.
- [x] Complete every task above or mark it blocked with a concrete reason; ensure no product
  implementation was changed by the reviewer.
- [x] Stage all files at the end, including reviewer tasklist/report/tests, and verify the final
  index/worktree status and review boundary.

## Completion record

All review items were completed; none was blocked. The independent report records 1 Blocker,
4 Major, 2 Medium, and 2 Low findings. Reviewer-owned evidence consists of 7 Go counterexample
tests plus 1 shell-oracle test; no product implementation was changed outside the candidate index.

Verification summary:

- candidate tests excluding the deliberate review reds passed; the final unfiltered `make test`
  failed only the 7 counterexamples;
- focused affected-package `-race`, `go vet`, lint, and simcluster hermetic self-tests passed;
- full tagged E2E passed every outer matrix except D4/D5, whose child runs hit the deliberate
  F1/F3 counterexamples;
- rebuilt deploy-tier drills: 42 `GREEN` (49/0 gaps), 67 `INCOMPLETE` (18 pass, one declared
  coverage gap), 51 `INCOMPLETE` (72 pass, two declared gaps); no product/setup/assert red;
- final simcluster status was empty;
- `git diff --check` remains red only for the three candidate trailing-whitespace lines reported
  as F9.

The final staging/status verification was performed after this record was written.
