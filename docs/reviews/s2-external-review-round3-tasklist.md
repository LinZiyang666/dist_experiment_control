# S2 External Re-review Round 3 Tasklist

Scope: narrow closure review of the unstaged response/fixes for round-2 R2-F1/R2-F2 and residual notes,
against the staged round-2 baseline.

- [x] Rebuild the staged-vs-unstaged boundary and confirm the six-file docs/test-only scope.
- [x] Prove VOTER success, timeout, and INCOMPLETE strings are all excluded from automatic flake retry while
  genuine systemd/container infrastructure signatures remain retryable.
- [x] Verify retry preserves first-run `.log` and `.rc`, exposes the retry in summary output, and cannot silently
  erase evidence or recategorize a product failure.
- [x] Mechanically verify 80/81/82 static assertion counts, conditional 82 discrepancy, and README/inventory/live
  consistency at 42/40/29.
- [x] Verify Arm R event count binds type+sid+role on one event line and the plan no longer retains executable
  correct-PIN warmup rows.
- [x] Run shell/Python syntax, whitespace, focused Go/lint checks, and synthetic flake/retry controls.
- [x] Run live drill 80 with automatic retry disabled; inspect load-bearing assertions and clean server residue.
- [x] Write a round-3 report beginning with Pass or Fail, including closure matrix, residual notes, verification,
  and explicit release recommendation.
- [x] Mark every item truthfully, stage all files with `git add -A`, and verify no unstaged/untracked residue and
  a clean cached-diff check.
