# Simcluster Coverage Roadmap External Review Round 5 Tasklist

Scope: independent re-review of unstaged rev6 against the staged rev5/round-4 baseline. Maintainer
responses are claim indexes only. Roadmap/inventory only; no S0-S9 implementation exists yet.

## Boundary and round-4 closure

- [x] Rebuild staged-vs-unstaged scope and rough-read the complete rev5→rev6 diff and response.
- [x] Verify every R4-F1 named command, behavior flag, Hidden command/flag and safety gate is present,
  correctly owned, and has a meaningful assertion or explicit NOT-COVERED rationale.
- [x] Independently enumerate the rest of the constructed Cobra tree, local/inherited flags and Hidden
  bits to detect omissions outside the maintainer's named fix list.
- [x] Verify R4-F2 `/sub` values are consistently modeled as `pubSysEvent` kinds with correct payload.
- [x] Verify R4-F3 source references, post-disconnect re-register proof, independent timeouts and final
  orphan reconciliation oracles.

## Regression and decision surface

- [x] Check new safety-negative drill assignments for executable setup, correct command paths, non-duplicated
  ownership and false-green resistance.
- [x] Recheck inventory generation ownership: what is proven in rev6 versus deferred to S0/S1, and whether
  any deferral leaves a batch-reordering gap.
- [x] Re-read all rev6 roadmap/inventory edits for accidental contradictions, security weakening, cleanup
  omissions or unresolved candidates.
- [x] Classify residual issues by release impact and record doubts/recommendations separately.

## Verification and handoff

- [x] Run proportional focused tests and Markdown/diff checks; document why live simcluster is or is not
  useful for this roadmap-only revision.
- [x] Write a round-5 report beginning with Pass or Fail, including closure matrix, findings, doubts,
  verification and explicit release recommendation.
- [x] Re-read round-5 artifacts, close boxes truthfully, stage every file with `git add -A`, and verify no
  unstaged changes plus documentation-only cached scope.
