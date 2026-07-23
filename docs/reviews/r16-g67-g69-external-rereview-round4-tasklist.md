# R16 + G67 + G69 external re-review round 4 tasklist

Date: 2026-07-23
Reviewer: independent external reviewer
Base: `b602fc7`
Prior verdict: `Fail` (`r16-g67-g69-external-rereview-round3.md`)
Developer response boundary: 16 unstaged files, `+425/-36`, diff SHA-256
`3f8333fc7639a4f88248a52046ad98b3a263e21d539d69b7690a198bb7b59972`, on top of the
previously staged 85-file candidate. The developer updated this boundary once during review; all
earlier test results are treated as exploratory rather than release evidence.

## Boundary and response verification

- [x] Freeze the latest 16-file developer response without trusting its appended conclusion.
- [x] Map changes to round-3 R3-F1/R3-F2/R3-F3 and the additional G69 canary work.
- [x] Compare claimed Drill 96, TSV, gotcha, comment, and canary changes with the effective diff.

## Round-3 blocker rechecks

- [x] Re-run the ReqID lookup-error outbox-retention counterexample.
- [x] Re-run the terminal-stage failure contradiction counterexample.
- [x] Inspect the new `(staged, applicable)` contract across every caller and every missing-ledger
  state, not only the developer's existing-ledger fixture.
- [x] Add a persistent-filesystem-failure counterexample covering failure of both the initial
  best-effort start ledger and terminal staging.
- [x] Add a counterexample where a once-durable start ledger is lost before terminal staging; an
  in-memory "was written" bit must not be mistaken for currently recoverable evidence.

## Placement canary

- [x] Inspect name/subject ownership, create/delete ambiguity, concurrency, cleanup, storage class,
  timeouts, resource limits, and false-positive/false-negative behavior.
- [x] Run the developer's real JetStream positive/negative test.
- [x] Add an independent pre-existing-stream ownership/data-preservation counterexample.
- [x] Strengthen ownership coverage with an exact-config collision containing operator data; a
  configuration fingerprint is not an ownership token.

## Drill and documentation

- [x] Verify #57 is judged only after the crashed home and recovery finalizer can run.
- [x] Verify all obsolete 5-second #58 judges are removed and current TSV/gotcha statements match.
- [x] Check that the structural #58 gap is recorded exactly once and no stale branch double-counts it.

## Gates and deliverables

- [x] Run all reviewer tests and classify expected reds.
- [x] Run filtered full tests, affected-package race, vet, lint, diff check, and tagged E2E.
- [x] Run simcluster hermetic gates and verify the sim server is clean.
- [x] Write the round-4 report beginning with `Fail` or `Pass`.
- [x] Stage all files and verify no unstaged or untracked files remain.
