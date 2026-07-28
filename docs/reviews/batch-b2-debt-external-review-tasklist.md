# batch B2 debt cleanup — external review tasklist

> Reviewer: independent external reviewer
> Date: 2026-07-28
> Review boundary at intake: empty index; 14 modified paths + 2 untracked paths;
> unstaged patch SHA-256 `ae4cf0c9f328609ea3b4a471d50a743b7db9bea6096493f472204693dc3b8fd5`.
>
> Rule: internal review conclusions are leads, not evidence. Finish every item below before staging the
> review phase. Any reviewer-authored implementation fix happens only after that staging boundary and
> remains unstaged with the final report.

## A. Boundary and contract reconstruction

- [x] A1. Reconstruct the exact developer delta from Git status, diff, new files, plan and debt-review
  response; confirm no staged or ignored review input is being missed.
- [x] A2. Re-read the binding requirements/architecture/testing/simcluster contracts touched by the
  delta, including fail-closed status semantics, leader/follower behavior and drill honesty rules.
- [x] A3. Map every claimed debt item and every internal-review response to concrete code, test and
  persistent-ledger sites; flag claims that are documentation-only or unsupported by executable proof.

## B. Replica-observation budget

- [x] B1. Audit the production dataflow end to end: stream enumeration, timeout sizing, cache writer,
  lock discipline, zero/unobserved behavior, leadership transitions, growth/shrink transitions and
  integer/duration bounds.
- [x] B2. Independently recompute the latency model and boundary conditions; test whether the revised
  budget is monotonic and whether stale/partial counts can under-budget a future observation.
- [x] B3. Review tests for causal strength rather than line coverage: make sure events/history/OBJ
  streams, first observation, follower lifetime, new leader, concurrent readers and failed observations
  are distinguished.
- [x] B4. Run focused unit/race tests and, where useful, mutation-style checks in an isolated copy to
  prove the new assertions fail for the intended reason.

## C. Stable exemption-site keys

- [x] C1. Audit all affected AST scanners, not only edited maps: traversal boundaries, nested
  declarations/literals, receiver formatting, aliases/generics, multi-site expressions, ordinal scope,
  duplicate keys, stale-entry detection and exact-source coverage.
- [x] C2. Verify all migrations preserve a strict bijection between old exemptions and physical sites,
  with reasons still describing the site now covered; check the claimed third raft timing site.
- [x] C3. Challenge the stability claim with realistic edits (insert/delete/reorder/same-count
  replacement) and decide which blind spots are acceptable versus falsely documented.
- [x] C4. Run the guards directly and the broader packages that exercise them; add an independent
  regression test if a structurally reachable hole is not already pinned.

## D. Agent proactive re-home

- [x] D1. Trace `refreshRosterOnce` through roster adoption, generation rejection,
  `rosterRequiresReconnect`, connection identity, rebuild scheduling and concurrent flags.
- [x] D2. Audit the new hermetic fixture for realism and non-vacuity: signed roster identity, actual
  connected URL, request/reply path, previous/current roster distinction, LEAVING/REMOVED arms,
  rejection arm and negative controls.
- [x] D3. Decide whether the two “known limitation” tests are safe executable documentation or tests
  that freeze undesirable behavior; verify they self-destruct when behavior is fixed and do not make
  incorrect production-reachability claims.
- [x] D4. Run focused tests under race and repetition; use targeted mutation/adversarial rows where they
  can distinguish wiring from pure predicate behavior.

## E. Drill 41 and evidence honesty

- [x] E1. Audit the shell assertions for quoting, fail-open behavior, paired-claim independence,
  anti-deletion accounting and whether `not_covered` text matches what the script actually observes.
- [x] E2. Cross-check all three persistent records (drill, TSV, verdict log) plus plan/review prose for
  exact verdict/gap-count/owner/evidence consistency and removal of superseded claims.
- [x] E3. Validate the timing argument against the actual jitter/rebuild implementation; separate
  measured facts, inference and hypothesis, and look for denominator/run-count contradictions.
- [x] E4. Run drill linters/ledger validators. Because this delta changes a deploy-tier cluster lifecycle
  drill, rebuild the current image and run drill 41 locally on the simcluster; inspect raw evidence and
  leave the cluster clean.

## F. Whole-change regression and release decision

- [x] F1. Review all changed production code for data races, lock ordering, hidden API/behavior changes,
  error handling, observability loss, code smell and misleading comments.
- [x] F2. Run `make test`, `make lint`, the required race slices, all-tag compile/vet checks and
  `make e2e-parallel`; investigate every failure rather than accepting an internal classification.
- [x] F3. Record every finding with severity, evidence, impact and disposition; list residual doubts and
  non-blocking recommendations explicitly.
- [x] F4. Write the pre-fix external verdict, stage the entire reviewed layer, record its cached patch
  hash and prove the worktree is clean.
- [x] F5. Only after F4, use the user's authorization to fix confirmed code problems; re-run
  proportionate verification, update the final report, and prove all reviewer fixes/report edits remain
  outside the index.
