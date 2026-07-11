# Simcluster Coverage Roadmap External Review Round 6 Tasklist

Scope: independent re-review of unstaged rev7 against the staged rev6/round-5 baseline. Maintainer
responses are claim indexes only. Roadmap/inventory only; temporary diagnostics must leave no code residue.

## Boundary and round-5 closure

- [x] Rebuild staged-vs-unstaged scope and rough-read the complete rev6→rev7 diff and response.
- [x] Reproduce the constructed Cobra tree path/flag/Hidden enumeration and compare it against the rebuilt
  inventory, including the runtime completion-node convention and the claimed path count.
- [x] Verify the four-name exclusion rule cannot hide deployment behavior and its two exceptions are complete.
- [x] Verify every R5-F1 named missing flag is now explicit, correctly classified and assigned to a meaningful
  drill or NOT-COVERED rationale.
- [x] Verify R5-F2 restore is excluded from machine-confirm and its three never-escapable assertions match code.

## Regression and release surface

- [x] Check new high-risk assignments (`accept-audit-loss`, restore raft override, doctor wrong-input,
  incident O_EXCL, join identity/provenance) for executable setup and false-green resistance.
- [x] Recheck generator ownership under reordered first batch and persistent/inherited/local de-dup semantics.
- [x] Re-read all rev7 edits for contradictions, omitted cleanup, source inaccuracies or unresolved candidates.
- [x] Separate release-blocking defects from leaf-plan-safe detail and record residual recommendations.

## Verification and handoff

- [x] Run proportional focused tests and Markdown/diff checks; document live-simcluster decision.
- [x] Write a round-6 report beginning with Pass or Fail, including closure matrix, findings, doubts,
  verification and explicit release recommendation.
- [x] Re-read round-6 artifacts, close boxes truthfully, stage all files with `git add -A`, and verify no
  unstaged changes plus documentation-only cached scope.
