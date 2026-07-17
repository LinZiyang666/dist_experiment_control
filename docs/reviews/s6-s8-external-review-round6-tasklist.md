# S6–S8 external re-review round 6 tasklist

Boundary: only the post-round-5 unstaged changes that claim to close B1/B2/B3 and the `out_matches`
advisory. Prior staged work is context, not reopened scope. Pass if no reproducible major correctness,
durability, interlock, or false-green defect remains.

## A. Boundary and claim mapping

- [x] Freeze the unstaged/untracked file set and map each edit/test to round-5 B1, B2, B3, or advisory.
- [x] Read the appended developer response; verify every closure claim against actual call ordering.

## B. B1 recovery journal and CLI completion

- [x] Audit journal durability, validation, permissions, replacement, cleanup, corrupt/torn handling, and
      behavior at every interruption boundary before/after RecoverCluster, prune, rebuild, de-cluster,
      and journal removal.
- [x] Trace first-run and resume CLI validation, typed confirmation, peer liveness checks, journal identity/
      address binding, phase handling, and idempotent forward completion.
- [x] Verify de-cluster failure is non-zero and a documented/reachable rerun can actually complete it.
- [x] Check whether startup/status diagnostics really consume `InterruptedForceSingle`, rather than only
      exporting an unused helper.

## C. B2 atomic exchange precondition

- [x] Verify capability probing occurs before every mutation and on the same filesystem as live/staged
      stores; audit cleanup and unexpected-error behavior.
- [x] Verify the non-atomic fallback and stale `.pre-rebuild` deletion are fully removed.
- [x] Test supported Linux behavior and unsupported/non-Linux compile/refusal behavior.

## D. B3 continuous daemon/offline interlock

- [x] Trace broker construction/startup ordering to prove no DB/Raft access or write occurs before the
      lifetime lock is acquired.
- [x] Verify every relevant daemon entry point and offline tool uses the same lock, with correct lifetime,
      permissions, error handling, and no self-deadlock for online operations.
- [x] Exercise two-holder exclusion and release/restart behavior, including race-enabled tests.

## E. Harness and regression quality

- [x] Re-run the failing-rc `out_matches` adversarial case and normal-match/no-match cases.
- [x] Review new regressions for vacuity and mutation sensitivity; add an independent regression only if
      needed to expose a material gap.
- [x] Run focused Go, race, shell contract/lint, diff checks, and Linux/Darwin builds.
- [x] Run a narrow sim-cluster check only if hermetic/source evidence cannot adjudicate a deploy seam.

## F. Closure

- [x] Write a round-6 report beginning with `Pass` or `Fail`, including findings, doubts, evidence, and
      explicit B1/B2/B3 disposition.
- [x] Complete this checklist, review final diff, stage every file, verify no unstaged/untracked residue,
      and stop.
