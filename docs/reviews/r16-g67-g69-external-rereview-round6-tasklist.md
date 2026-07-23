# R16 + G67 + G69 external re-review round 6 tasklist

Date: 2026-07-23
Reviewer: independent external reviewer
Base: staged 90-file round-5 candidate on `HEAD=b602fc7`
Prior verdict: `Fail` (`r16-g67-g69-external-rereview-round5.md`)
Developer response boundary: 6 tracked unstaged files, approximately `+481/-108`,
initial diff SHA-256
`bbbcdfdaa860ca85e34349e7fb72ae81340497cc4e90da1c6083ee252cad94b3`.

## Boundary and response verification

- [x] Re-read `CLAUDE.md`, freeze the unstaged developer response separately from the staged candidate,
  and record the initial diff hash.
- [x] Read the complete response appended to the round-5 report and map every claimed fix/test/gate to
  executable changes.
- [x] Verify the implementation boundary remains stable before final certification.

## R5-F1/R5-F2 two-directory terminal state machine

- [x] Re-run all round-5 fallback replay, primary-unavailable, callback-cleanup, repeated-pass, commit
  lookup, and staging-failure counterexamples.
- [x] Verify the new outbox-first recovery when primary remains unavailable and after primary returns.
- [x] Test the inverse partial failure: an unreadable outbox must not let a primary start-only row
  synthesize a terminal whose exact terminal may still exist in that outbox.
- [x] Inspect every legal/illegal primary × outbox row combination, including different terminal bytes,
  live tracker races, corrupt/temp records, age boundaries, and repeated passes.
- [x] Verify `consumeXferLedgerRow` does not retire the only exact terminal until primary unlink and its
  directory durability are confirmed; inspect sync errors and crash points.
- [x] Verify one-source scan errors make safe progress without being silently normalized into a false
  successful reconciliation.

## R5-F3/R5-F4 placement canary

- [x] Re-run non-empty stream, markerless stream, authored lookalike, durable-consumer, and marked-residue
  preservation/reclaim tests.
- [x] Verify pre-delete failure is fatal and cannot fall through to idempotent create.
- [x] Inspect lookup/create/verify/delete TOCTOU, marker ownership, message/consumer state before both
  deletion points, requested versus actual replicas, cleanup observability, and context/error handling.
- [x] Add an independent counterexample for any destructive or false-placement seam not covered by the
  developer tests.

## Documentation, drill, and unexplained-red audit

- [x] Verify state-table claims match implementation and user-visible memory-canary wording remains
  narrower than File/ObjectStore disk-budget guarantees.
- [x] Verify Drill 96 still records #58 exactly once and retains the documented deploy-tier gaps.
- [x] Investigate the developer's unclassified E2E failure with complete logs; do not erase it merely
  because later runs pass.
- [x] Investigate Drill 67's healthy-control timeout, attempt/budget accounting, and sim-server hygiene;
  classify it or retain it explicitly as release uncertainty.

## Gates and deliverables

- [x] Run focused reviewer/developer tests and classify every red.
- [x] Run `git diff --check`, vet, lint, full tests, and affected-package race/leak gates.
- [x] Run tagged E2E with complete retained output and the relevant hermetic/simcluster gates.
- [x] Write the round-6 report beginning with `Fail` or `Pass`, including findings, doubts, suggestions,
  and all unclassified evidence.
- [x] Stage all files and verify no unstaged or untracked files remain.

## User-authorized post-review remediation (must remain unstaged)

- [x] Confirm the complete external-review snapshot above is staged before touching implementation.
- [x] Fix unreadable-outbox synthesis authorization and missing-primary-path cleanup semantics.
- [x] Propagate directory-sync failures before retiring the exact terminal.
- [x] Fix canary lookup-error handling and post-create state verification.
- [x] Fix the session test helper's broker-subscription readiness race found by complete-log E2E.
- [x] Turn every independent red test green and re-run full release gates.
- [x] Append the remediation result and final release conclusion without staging the remediation delta.
