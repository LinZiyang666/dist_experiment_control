# Batch C external review tasklist

> Reviewer role: independent external reviewer. Internal plans, reviews, dispositions, and tests are
> evidence leads only; none are accepted without reconstruction from the binding contracts, production
> paths, and fresh execution.
>
> Review target: every path outside the index at intake, including untracked files. The review may add
> unit/integration tests, but must not repair production implementation. Final deliverables are a
> Pass/Fail report and a completely staged worktree.

## A. Intake, authority, and change map

- [x] A1. Record branch, HEAD, empty/non-empty index, unstaged/untracked path inventory, diff statistics,
  and an intake patch hash so the reviewed layer is reproducible.
- [x] A2. Read `CLAUDE.md` and identify the authority chain, external-review role boundary, required
  quality gates, test naming rule, and the simcluster trigger/host rules.
- [x] A3. Read the binding portions of `docs/requirements.md`,
  `docs/distributed-broker-architecture.md`, and `docs/deploy-tier-gotchas.md`; use
  `docs/architecture.md` only for still-valid phase/dependency/checklist context.
- [x] A4. Read the cluster, operations, usage, testing, and simcluster documentation touched or relied
  on by this delta; identify every user-visible or deploy-tier contract that Batch C changes.
- [x] A5. Read representative previous external-review tasklists/reports to reproduce their evidence,
  severity, doubt, recommendation, test, and staging conventions.
- [x] A6. Reconstruct Batch C scope from the actual diff before consulting the internal verdict:
  C1 durable force-single finalization and drain-marker healing; C2 size-derived transfer budgets;
  C3 topology action propagation/classification/rendering.
- [x] A7. Treat every claim in `batch-c-plan.md` and `batch-c-review.md` as untrusted. Build a disposition
  ledger mapping each delivered item and internal finding to independent source/test evidence.

## B. C1 — force-single finalize state machine

- [x] B1. Trace the online force-single commit path from arm/commit through raft rewrite, leader wait,
  durable marker/epoch, roster prune, seed convergence, response serialization, and CLI output.
- [x] B2. Prove the unchanged success path performs synchronous prune, creates no finalize operation,
  reports completion honestly, and leaves the documented de-cluster step immediately usable.
- [x] B3. Enumerate failure/crash windows after raft rewrite: before leader, marker, epoch, prune,
  operation creation, operation observation, terminal transition, seed convergence, and CLI reply.
- [x] B4. Audit `OpKindForceSingleFinalize` schema/states/validity/rendering for additive compatibility,
  terminal-state correctness, unknown-kind rollback behavior, and absence of migration assumptions.
- [x] B5. Audit operation identity and uniqueness: repeated recovery for the same ghost set, prior
  terminal rows, duplicate concurrent starters, leader changes, process restarts, and ID collision.
- [x] B6. Verify start/resume predicates require durable force-single intent, a one-voter raft config,
  VOTER roster ghosts outside raft, and no active finalize op; reject join/rejoin and self-deletion.
- [x] B7. Verify parameters permanently capture the exact abandoned set, epoch, marker time, and deadline;
  partial prune must not shrink the remaining work or mint unstable recovery identities.
- [x] B8. Verify every destructive prune re-reads current raft configuration and current roster phase
  immediately before proposal, deletes only captured ghosts, and cannot race a rejoin into deletion.
- [x] B9. Verify advance-after-observe: proposal success is not completion; zero matching roster rows is
  completion; proposal errors, zero-row CAS, stale reads, and leader turnover remain retryable.
- [x] B10. Verify the replicated deadline semantics across leader/process changes, including zero/malformed
  deadlines, deadline-edge prune success, truthful ghost enumeration, and guaranteed terminalization.
- [x] B11. Prove budget exhaustion ends in `FS_GHOST_LEFT`, never `BLOCKED`, preserves actionable
  `last_error`, and does not discard the final timeline/error evidence.
- [x] B12. Verify upgrade-lock bypass is limited to finalize operations while join/retire remain frozen;
  inspect other global gates, target-node fences, and command interactions such as add/readdr/remove.
- [x] B13. Verify `ConfirmOp`, `AbortOp`, and unknown-kind handling cannot reset/reopen the finalize
  budget, strand an unfamiliar state, or destroy the manual escape hatch after rollback.
- [x] B14. Verify the response has an unambiguous three-way truth table:
  prune complete / incomplete with retry op / incomplete without retry op; no consumer may infer
  completion from `FinalizeOpID == ""` or from `Abandoned`.
- [x] B15. Inspect CLI/runbook/error strings for every outcome, including non-TTY syntax, required
  `--manual`, `--confirm-node-id`, exact op commands, and prohibition on premature `--to-standalone`.
- [x] B16. Audit drain-marker healing predicates and authority: delete only markers whose roster row is
  absent, preserve half-finished drains, report DRAINING-without-marker inconsistency, and avoid racing
  active raw drain/retire/abort operations.
- [x] B17. Verify pass registration, cadence, caught-up gate, leader-only writes, idempotence, error
  observability, shutdown behavior, and configuration default/override plumbing.
- [x] B18. Trace every `Inconsistent` renderer/doctor consumer after adding the third meaning and confirm
  the operator receives a correct diagnosis and recovery action.
- [x] B19. Re-run and adversarially mutate/extend direct behavior tests for start, drive, resume, budget,
  deadline-edge, partial prune, rejoin race, upgrade lock, unknown kind, marker healing, and reporting.
- [x] B20. Inspect drills 12 and 22 for non-vacuous assertions, fail-open shell constructs, correct
  injection cleanup, success/no-op assertions, and fidelity to the real online recovery path.

## C. C2 — transfer budget, garbage collection, and audit truth

- [x] C1. Reconstruct the complete push and pull lifecycles across ctl, broker, object store, agent,
  receiver event, watchdog, inflight ledger, crash recovery, cross-home GC, and terminal audit.
- [x] C2. Independently derive the budget formula, units, rounding, leg counts, floor/ceiling behavior,
  max-size value, minimum throughput, slack, and overflow behavior for negative/zero/huge sizes.
- [x] C3. Verify tier A remains behaviorally unchanged and tier B uses the same constants/formula at all
  intended producers and consumers; scan for stale hand-copied sizes, timeouts, or formulas.
- [x] C4. Verify broker admission bounds every size used to derive a budget and that agent/ctl limits,
  flag defaults, help text, config defaults, and runtime contexts cannot terminate earlier.
- [x] C5. Verify push covers both object-store crossings; determine and test the intentionally different
  pull budget contract so broker and endpoints cannot disagree or delete a live transfer.
- [x] C6. Audit prep-entry TTL derivation and lower bounds for all tier-B sizes; it must outlive the
  complete upload/finalization wait and preserve pre-change behavior for smaller objects.
- [x] C7. Audit live watchdog arming at the production call site, cancellation/removal races, duplicate
  terminal events, nil/unknown verbs, and attribution/exit-code correctness.
- [x] C8. Audit late receiver completion tracking for clock consistency, bounded memory, ID reuse,
  broker restart, logging sensitivity, and both push and pull semantics.
- [x] C9. Audit inflight ledger compatibility and both writers/readers of `Size`; old records must have a
  defined safe policy and synthetic terminal timestamps/dedup identities must remain stable.
- [x] C10. Verify stranded detection uses the same live budget while terminal timestamp/duration use a
  deliberately stable floor; prove this cannot create contradictory or duplicate audit terminals.
- [x] C11. Derive the cross-home GC inequality per object. Verify object metadata size is trustworthy,
  overflow-safe, and extended only where required; explicit operator floors and compressed drills
  must retain their intended effect.
- [x] C12. Audit home-owned reaping and the documented restart/orphan limitation: no new claim may imply
  all live-object deletion paths are fixed, and the executable limitation guard must reflect production.
- [x] C13. Check `transfer_budget_exceeded` wire stability, hint and exit taxonomy, logging, metrics,
  audit schema behavior, and preservation of genuine `agent_no_responders` emitters.
- [x] C14. Audit tracker-slot exhaustion window, context/goroutine/timer cleanup, race safety, file
  descriptor/object-store cleanup, and denial-of-service implications of the longer maximum budget.
- [x] C15. Re-run direct helper and production call-site tests with boundary tables and targeted
  mutations so changing legs, omitting size, shortening TTL, or dropping the GC increment fails.
- [x] C16. Search all docs, source comments, examples, tests, config templates, and errors for stale
  `2 GiB`, `5m`, `10m`, fixed-timeout, and cross-home-reap claims; classify intentional historical text.

## D. C3 — topology action wire path and shared classifier

- [x] D1. Enumerate every topology `Action` producer from `ReconcileOnce`, including all exit branches,
  and verify `AllActions` is genuinely exhaustive rather than a hand-maintained false guard.
- [x] D2. Trace `TopoAction` end-to-end: reconcile self-report, health response, peer aggregation, self
  overwrite, admin-socket JSON, CLI renderers, schema-version ledger, and mixed-version omission.
- [x] D3. Verify the additive field requires no `ProtoVersion` bump and older readers/writers have a
  deterministic legacy fallback with no health polarity regression.
- [x] D4. Audit `ClassifyTopo` for every action × reported flag × desired/observed relation × legacy
  reason: Converged, Behind, Held, Stuck, UnknownAction, and Unreported must match binding semantics.
- [x] D5. Prove `ActionRejected` and `ActionUnknownDirective` are unconditionally stuck;
  awaiting-clustered-cutover is held; transient reload/unresolvable states preserve legacy generation
  polarity where promised; unknown future actions fail visibly.
- [x] D6. Verify severity order and every `Cell`, `String`, `Banner`, `NextStep`, and `Degrades` mapping;
  no state may recommend unsafe manual reconcile or omit the coordinated action it requires.
- [x] D7. Compare all decision/render mirrors: broker health, status TOPO cell and legend, status card,
  doctor, `reconcile nats --wait`, operation convergence, JSON, metrics, and deployment scripts.
- [x] D8. Audit reachability filters and voter/phase filters for consistency; self, nats-health,
  unreachable, non-voter, old broker, and malformed rows must not produce contradictory views.
- [x] D9. Verify broker health precedence and the stated N<=2 limitation; distinguish deliberate
  fault-tolerance precedence from accidental invisibility or false HEALTHY_HA.
- [x] D10. Verify status card folds all nodes by severity, handles Behind and UnknownAction, and never
  contradicts its banner or falls through to a false generic explanation.
- [x] D11. Verify doctor emits FATAL/ADVISORY/PASS for all relevant states, especially Behind,
  UnknownAction, missing reports, unreachable voters, and mixed-version nodes.
- [x] D12. Verify `reconcile nats --wait` has correct exit taxonomy, an adequate dwell against one-tick
  transients, state-specific remedies, deadline behavior, and no false green or infinite retry.
- [x] D13. Confirm the intentional divergence of operation convergence remains fail-closed and is
  documented/tested without accidentally importing the presentation classifier into safety logic.
- [x] D14. Drive real `ReconcileOnce` outcomes into the classifier/propagation tests so prose changes,
  new actions, and producer rewiring cannot leave pure classifier unit tests green.
- [x] D15. Inspect drill 93's converged and failure-polarity assertions for correct table fields,
  real action propagation, shell robustness, restoration, and proof that hard-coding `"noop"` fails.

## E. Cross-cutting implementation quality and documentation

- [x] E1. Review every changed production line and new file for unchecked errors, stale-state reads,
  TOCTOU, races, lock ordering, goroutine/timer leaks, integer overflow, nil behavior, and dead code.
- [x] E2. Check public/internal API compatibility, JSON `omitempty` behavior, stable codes/subjects,
  database schema assumptions, rollback/forward-version behavior, and static-build constraints.
- [x] E3. Inspect tests for vacuity, duplicated production logic, weak assertions, false mutation claims,
  forbidden process-named files, timing flakes, global-state contamination, and cleanup leaks.
- [x] E4. Compare actual implementation against every C1/C2/C3 delivery row, internal accepted finding,
  rejected finding, permanent non-goal, and claim that a drill/test was freshly executed.
- [x] E5. Verify binding docs describe only implemented behavior, use safe operator commands, state
  known limitations honestly, and do not promote internal-review assertions into architecture facts.
- [x] E6. Run formatting and static diff checks; inspect generated/config/shell changes and confirm no
  unrelated user changes are overwritten.

## F. Independent execution and release decision

- [x] F1. Run focused package tests for changed cmd, proto, natsconf, agent, broker, cluster, and
  determinism surfaces; repeat race-sensitive and state-machine cases under `-race`/`-count`.
- [x] F2. Add independent regression tests under unit-based filenames with `origin:` comments whenever
  a reachable invariant lacks a non-vacuous guard; do not modify production implementation.
- [x] F3. Run all relevant build-tag compile/vet slices so additive fields and new symbols are checked
  in code hidden from the default package graph.
- [x] F4. Run `make test`, `make lint`, `CGO_ENABLED=0 go build ./...`, and the sole complete matrix
  `make e2e-parallel`; investigate every failure and never substitute a serial all-suite run.
- [x] F5. Because deploy-tier drills changed, validate simcluster harness/ledger checks, rebuild the
  current image as required, run drills 12, 22, and 93 locally on `weilandserver`, inspect raw evidence,
  and prove cleanup.
- [x] F6. Record findings with severity, exact evidence, impact, and required correction. Explicitly
  list unresolved doubts, accepted limitations, and non-blocking recommendations.
- [x] F7. Write `docs/reviews/batch-c-external-review.md` with `Fail` or `Pass` as its first line and
  include intake, scope, findings, evidence, verification, doubts, recommendations, and conclusion.
- [x] F8. Mark every task above complete or explain any blocked item in the report; no silent omissions.
- [x] F9. Add every file to the index, run cached diff checks, record the cached patch hash, and prove
  the worktree outside the index is clean.
