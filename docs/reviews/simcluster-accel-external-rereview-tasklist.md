# simcluster acceleration external re-review tasklist

Scope: developer response and all unstaged changes layered on top of the first external-review index.
The first review's findings remain the acceptance checklist; a response is evidence only after code,
tests, and deploy-tier artifacts independently agree.

## A. Major findings

- [x] Reproduce both cross-mode nesting directions and verify mode is restored from each frame.
- [x] Re-audit every newly fixed/effectful poll site and test whether the mechanical backstop is
  non-vacuous, portable, and complete enough for its stated guarantee.
- [x] Mutate owner, defect ID, duplicate row, signature definition/count/ERE, and slug metacharacters;
  confirm validator and runtime share one exact grammar.
- [x] Replay the real 52/74 failures and prove each new band distinguishes the recorded root cause from
  a different failure at the same assertion.
- [x] Reassess 30/50/52/74/96 dispositions against the original no-laundering rule; do not treat an
  amended criterion or an unresolved investigation as proof that speed levers are clean.
- [x] Reconcile M3/M5/V4/H3 code and plan status, including telemetry field semantics and signal paths.

## B. Medium/Low findings

- [x] Try to forge the new image-precheck mechanism without first checking the image.
- [x] Verify log/evidence modes and redaction against spaced/equal/quoted secrets and multiline output.
- [x] Exercise logdir traversal, symlink, relative, root/home/source aliases, live cleanup, and replay.
- [x] Exercise provisioning temp cleanup on success, failure, INT/TERM, and unusual `TMPDIR`.

## C. Regression and release gates

- [x] Run the first review's independent gate, the full simcluster hermetic suite, shell syntax/static
  checks, and focused mutations added during re-review.
- [x] Run `make lint`, `make test`, and `make e2e`; distinguish repeatable findings from load flakes.
- [x] Inspect or replay cited server archives where a classification claim depends on real evidence.
- [x] Write a re-review report beginning with `Pass` or `Fail`, list doubts and residuals, then stage all
  files and verify no unstaged/untracked content remains.
