# S6–S8 external re-review round 5 tasklist

Review boundary: only work outside the index at review start (`git diff` plus untracked files), while
using the staged S6–S8 implementation as context. Claude's round-5 reply is evidence to check, not an
authority. The release bar is pragmatic: block only reproducible correctness, durability, security,
portability, or materially misleading-test defects; record lower-risk hardening as advice.

## A. Boundary and governing contracts

- [x] Freeze the exact unstaged/untracked file set and distinguish it from the staged baseline.
- [x] Read `CLAUDE.md`, requirements/architecture, cluster runbook, simcluster mandate, and prior S6–S8
      external-review workflow/reports relevant to this delta.
- [x] Reconcile every claim in `s6-s8-round5-review.md` against source or executable evidence; do not
      inherit its severity or conclusion.

## B. Offline Raft rebuild and directory replacement

- [x] Prove Linux and non-Linux build portability of the new exchange abstraction.
- [x] Audit `RENAME_EXCHANGE` error classification and the fallback swap's success, rollback, cleanup,
      path-collision, and crash/interruption states.
- [x] Audit ownership/mode mirroring for root-run recovery, files vs directories, symlinks, special
      entries, privilege failures, and whether the resulting Bolt/snapshot stores remain writable.
- [x] Check durability ordering (`Sync` boundaries), staged/live cleanup, and error messages/runbook
      recoverability after every partial failure.
- [x] Independently evaluate whether a concurrently revived daemon can open/write either old or new
      store during rebuild/swap, and whether any acknowledged write can be deleted.
- [x] Independently evaluate force-single/resnapshot interruption and retry/idempotency, including the
      interval between roster mutation, Raft rebuild, and NATS de-clustering.
- [x] Check snapshot/index/term reconstruction against live Raft high-water semantics; separate
      pre-existing risk from regressions caused by this delta.

## C. Cluster operation controller

- [x] Trace `blockAfterAttempts`/attempt-counter semantics for join seed convergence, grow-lock release,
      and retire seed withdrawal: first failure, retry ceiling, restart, confirm, abort, and success reset.
- [x] Verify error text/state transitions remain actionable and do not mark a physically incomplete
      operation terminal.
- [x] Recheck grow-lock lifetime around the terminal join path and downstream rebalance/release phases;
      classify only a concrete concurrency violation as blocking.

## D. Simcluster harness and drills

- [x] Validate `out_matches`: command runs once to completion, command failure cannot match as success,
      stdout/stderr and regex behavior are intentional, and no SIGPIPE is reintroduced.
- [x] Audit drills 42/91/92 for truthful setup vs product assertions, settled-service liveness,
      force-single file postconditions, quoting/word-splitting, and exact five-verdict behavior.
- [x] Audit the lint rule for meaningful detection plus false positives/obvious false negatives; ensure it
      does not ban safe captured-output matching or create fake green coverage.
- [x] Search all drills for remaining mutating `tether ... | grep -q` truncation patterns and distinguish
      destructive writers from read-only/dry-run calls.

## E. Executable evidence

- [x] Run focused Go tests for cluster offline/controller behavior and add a small independent regression
      only if it exposes a material untested failure.
- [x] Run race-enabled focused tests for touched concurrent/state-machine packages where practical.
- [x] Run shell syntax, verdict-contract, and drill-lint tests, including adversarial fixtures for the new
      lint/helper behavior.
- [x] Build `cmd/tether` for Linux and shipped Darwin targets.
- [x] Inspect sim-cluster server guidance/state and run the narrow remote drill needed to adjudicate a
      remaining deploy-tier question; accept honest PRODUCT-RED/ASSERT-FAIL as evidence, never force green.

## F. Closure

- [x] Write an external report whose first line is `Pass` or `Fail`, with evidence, doubts, findings,
      suggestions, test results, and an explicit disposition of Claude's reply.
- [x] Mark every task above complete (or explicitly blocked with reason), review the final diff, and add
      all files to the index as requested.
- [x] Stop after this review stage; do not begin implementation of reported product findings.
