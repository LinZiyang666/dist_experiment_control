# S3–S5 (G-A) external re-review round 11 tasklist

Scope: final re-review of the round-10 response in drill 74; 71 is unchanged and its success/failure recovery gates
were already exercised in round 10.

- [x] Verify the B fresh-snapshot branch checks helper rc/nonempty before every mutation and preserves NEG_OK/cleanup
  on both success and failure paths; inspect nesting and set-u safety.
- [x] Verify C captures all pre-proven nids, then reselects a positive non-leader/non-tunnel target from one fresh
  validated snapshot adjacent to kill; no stale or vacuous edge may reach `_skew`.
- [x] Verify the reselected target remains compatible with exact auto-move attribution and ordinary-expose control.
- [x] Run syntax/whitespace and focused failure/success probes for B snapshot, C no-target, C reselected-target and
  C-before-auto snapshot failure.
- [x] Via tether CLI only, sync/verify the final 74 and run a unique-instance fresh drill without colliding with other
  reviewers; retry a noisy failure where useful and scan logs/cleanup.
- [x] Write the round-11 Pass/Fail report with separate harness and product dispositions; stage every file and verify
  no unstaged/untracked content.
