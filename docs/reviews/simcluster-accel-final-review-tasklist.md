# simcluster acceleration final external review tasklist

Scope: the developer's round-2 response and every unstaged change layered over the prior external
re-review index. This is the final review round. Findings are accepted only when implementation,
adversarial tests, documentation, and deploy-tier evidence agree.

## A. Re-open every prior blocker

- [x] Prove cause-context bands cannot match the assertion title, stale diagnostics from an earlier
  operation, a diagnostic after the failure, or a different failure sharing the same rc/message.
- [x] Exercise INT, TERM, and HUP while drills and telemetry are active; prove children are reaped,
  exit status is signal-derived, and no completion sentinel is written.
- [x] Verify `running_drills` from launch through completion/retry and ensure telemetry reads shared
  state rather than a subshell snapshot.
- [x] Attempt every public/legacy image-precheck bypass and prove each direct drill invocation performs
  a real image-versus-vendor check.
- [x] Exercise new, existing-owned, existing-unowned, relative, absolute, symlink, `..`, root/home/source,
  replay, and interrupted log directories without deleting unrelated artifacts.
- [x] Mutate signature slugs and definitions so validator and runtime are proven to use one literal
  grammar and one cause-input contract.
- [x] Exercise `cmd_up` success, partial failure, INT/TERM, background-child cleanup, unusual `TMPDIR`,
  and redaction of flags, assignments, query parameters, quoting, and multiline output.

## B. Acceptance and evidence

- [x] Independently inspect both corrected-tree `-j 6` archives and the solo drill-50 run cited by the
  response; verify archive completion, code identity, deviation sets, M4 classifications, and raw causes.
- [x] Trace drill 96's pre-heal committer oracle through the product source and fixture; prove it cannot
  confuse pre-existing, delayed, post-heal, or log-residue events.
- [x] Trace drill 50's reader-recovery diagnosis and determine whether “host contention” is demonstrated
  or merely inferred from a solo rerun.
- [x] Reconcile the final dispositions, ledger, expected-verdict table, plan status, and response without
  upgrading unresolved product gaps or load sensitivity into release-clean evidence.

## C. Gates, remediation, and handoff

- [x] Run both prior independent gates, the complete simcluster hermetic suite, shell syntax/static
  checks, `make lint`, `make test`, and `make e2e`.
- [x] Add focused adversarial tests for any newly found authority, signal, cleanup, or evidence defect.
- [x] Write the review conclusion, then stage every developer/review-baseline file and verify the index
  has no unstaged/untracked residue.
- [x] Directly fix all remaining in-scope defects, rerun proportionate gates, and leave those fixes plus
  the final report outside the index with an explicit staged/unstaged inventory.
