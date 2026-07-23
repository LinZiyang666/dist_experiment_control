# R16 + G67 + G69 external re-review round 5 tasklist

Date: 2026-07-23
Reviewer: independent external reviewer
Base: staged 87-file round-4 candidate on `HEAD=b602fc7`
Prior verdict: `Fail` (`r16-g67-g69-external-rereview-round4.md`)
Developer response boundary: 11 tracked unstaged files plus one untracked test file,
approximately `+487/-154`, initial diff SHA-256
`80359f40454f868effa0f0b67df10481eed09030aa6da6b0ff37d92e1e63b7ef`.

## Boundary and response verification

- [x] Freeze the latest developer response separately from the staged round-4 candidate.
- [x] Read the response appended to the round-4 report and map every claim to executable diff.
- [x] Verify the implementation boundary remains stable before final certification.

## R4-F1 terminal durability

- [x] Re-run every prior commit/delete, lookup-error, staging-failure, missing-ledger, and
  lost-ledger counterexample.
- [x] Inspect the new primary/outbox two-directory state machine across partial directory failure,
  duplicate records, different record contents, cleanup, unknown commit state, and repeated passes.
- [x] Add an independent canonical fallback test: an outbox terminal replay must also dispose of the
  surviving primary start-only ledger, or the next pass synthesizes a contradictory terminal.
- [x] Verify failure of either optional directory does not prevent recovery from the other.

## R4-F2 placement canary ownership

- [x] Re-run the non-empty exact-config preservation counterexample and developer ownership tests.
- [x] Inspect both pre-create and post-create deletion points, delete-error handling, TOCTOU,
  metadata compatibility, messages, consumers, and false placement proof.
- [x] Add an independent empty-stream-with-durable-consumer preservation counterexample; zero messages
  is not equivalent to an ownerless/disposable stream.

## Drill, contract, and documentation

- [x] Verify the #58 structural gap appears exactly once and expected-verdict text matches.
- [x] Verify user-visible placement wording is limited to memory-backed meta assignment.
- [x] Verify comments and plan retain the real File/ObjectStore and 3->2->3 evidence gaps.

## Gates and deliverables

- [x] Run reviewer tests and classify every red.
- [x] Run full tests, affected-package race, vet, lint, diff check, and tagged E2E.
- [x] Run simcluster hermetic gates and verify the sim server is clean.
- [x] Write the round-5 report beginning with `Fail` or `Pass`.
- [x] Stage all files and verify no unstaged or untracked files remain.
