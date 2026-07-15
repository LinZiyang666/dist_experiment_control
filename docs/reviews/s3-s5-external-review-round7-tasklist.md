# S3–S5 (G-A) external re-review round 7 tasklist

Scope: independently review the developer's eight-file unstaged response over the staged round-6 Fail baseline.
The developer's appended response, claimed server runs, root-cause labels, and documentation are evidence to
verify, not authority. Correct RED is acceptable; false GREEN, vacuous RED attribution, or release documentation
that claims completion despite a failing locked gate remains blocking.

## Boundary and response

- [x] Reconstruct the exact staged-to-worktree delta, untracked/deleted state, executable/image inputs, and the
  complete developer response to R6-M1/M2 and questions 1–3.
- [x] Build an R6 closure matrix covering executable behavior, verdict propagation, documentation, and remaining
  NOT-COVERED arms; distinguish a correctly failing product gate from implemented acceptance coverage.
- [x] Verify the revised owner-decisions file removes the unauthenticated quote/rescope and does not introduce a
  new implied authorization beyond the raw sys.events observability limitation.

## Drill 74 / R6-M1

- [x] Audit the validated snapshot for command/JQ rc, exact and distinct nids, allowed homes, readiness/identity,
  sentinel uniqueness, and every downstream consumer.
- [x] Verify all three pre-skew SS legs are independently established, remain non-vacuous, use distinct exits and
  ports, and foundational failures cannot continue into misleading destructive arms.
- [x] Audit dry-run and real rebalance rc propagation, exact before/after moved-exit identification, natural-drift
  exclusion, and the hard post-move data-plane assertion.
- [x] Audit the ordinary-expose negative control for sentinel creation, create/explain rc, real home, serving
  baseline, successful rebalance injection, unchanged home, continued serving, and cleanup.
- [x] Audit Arm C construction and auto-effect hard failure for a real return edge, live env, skew non-vacuity,
  all required SS baselines/closures, and attribution versus unrelated in-flight operations.
- [x] Re-run the former empty/duplicate/failed-command/empty-explain adversarial probes and add new probes for
  duplicate exit selection, stale globals, natural convergence, and failed setup continuing after assert_ok.

## Drill 71 / R6-M2

- [x] Audit the fixture hard failure and branch control so a missing fixture cannot produce misleading crash/drain
  claims, and an established fixture crosses valid live-nonleader injections.
- [x] Audit Arm B drain command rc/signature, exact old/new home, epoch/moved state, survivor endpoint and bytes,
  return behavior, and whether the predicate can pass on stale/missing explain output.
- [x] Determine whether E/G/F are executable, explicitly failing at their own required semantics, or merely
  described as unreachable; reconcile that disposition with the locked plan and release verdict.
- [x] Check the proposed #34/systemic root-cause claim against executable evidence: distinguish correlation with a
  lingering op from proof that the same op blocks drain, upgrade, and auto-rebalance.

## Shared harness, documents, and tests

- [x] Audit the shared grow helper change and individual grow rc handling; ensure concurrent-test mode and internal
  setup retry accounting do not erase a product failure.
- [x] Reconcile README, gotcha ledger, inventory, owner decisions, round-6 response, locked plan, and executable
  behavior for current GREEN/RED/NOT-COVERED/completion language.
- [x] Run `sh -n`, cached/uncached whitespace checks, ShellCheck if available, and focused local adversarial probes.
- [x] Via tether CLI only, stage/verify current hashes on the simcluster server and run fresh isolated relevant
  drills concurrently (not one-by-one); retain per-instance logs/rc/assertion counts and do not retry away RED.
- [x] Scan complete logs for skipped branches, false attribution, internal grow attempts, contradictory
  PASS/FAIL/NOT-COVERED lines, stale image inputs, and secrets.
- [x] Write a round-7 report beginning with Pass or Fail, listing findings, verified closures, doubts/questions,
  exact tests/hashes/logs, and release disposition.
- [x] Stage every file with `git add -A`; verify no unstaged/untracked content and both cached/uncached whitespace
  checks. Do not commit or push.
