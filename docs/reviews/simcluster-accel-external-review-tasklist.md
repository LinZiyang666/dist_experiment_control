# simcluster acceleration external review tasklist

Scope: all unstaged and untracked changes present at review start (2026-07-24), centered on
`test/simcluster/` acceleration, observability, expected-verdict classification, replay/attribution,
parallel provisioning, poll modes, and the accompanying plan/review/disposition documents.

Role and evidence rule: independent external review. Internal review reports and maintainer
dispositions are indexes only; every material claim must be rebuilt from code, tests, archived/live
evidence, or a clearly identified limitation. Product implementation is out of scope, but independent
review tests and review documents may be added.

## A. Boundary, requirements, and invariants

- [x] Read `CLAUDE.md`, the simcluster Mandate/README, relevant requirements/architecture/coverage
  documents, server operations notes, the acceleration plan, internal reviews, dispositions, and prior
  external-review/tasklist conventions.
- [x] Reconstruct the exact unstaged/untracked boundary and rough-read every changed file before fixing
  the review surface.
- [x] Check that the increment remains a dev-tool-only leaf and does not compensate for product defects,
  weaken the five-verdict contract, silently turn non-GREEN into success, or violate the documented
  deploy-tier/runtime-sudo boundaries.
- [x] Audit documentation for contradictions, stale status claims, unsupported causality, misleading
  release language, and mismatches with executable behavior.

## B. Expected-verdict table and validation

- [x] Verify the TSV split preserves every drill row, expected verdict, expected `nc_gap`, owner, band,
  and prose history; independently compare old and new data rather than trusting migration comments.
- [x] Audit `validate-verdicts.sh` parsing and fail-closed behavior for field counts, duplicate/missing
  drills, enums, numeric gaps, note refs, owners, bands, signature syntax, open/closed gotcha lookup, and
  adversarial whitespace/newline/regex/metacharacter cases.
- [x] Assess whether a band can launder a different first failure, a closed/unknown defect, an
  ownerless red, or an incorrect `nc_gap`.
- [x] Review both migrated consumers and search the whole repository for stale assumptions about the
  old five-column/prose TSV.
- [x] Run validator mutation/self-tests and add focused independent cases for any untested boundary.

## C. Runner, replay, and classification authority

- [x] Trace `run-drills.sh` end to end: option parsing, drill discovery/filtering, LPT ordering,
  concurrency slots, preflight/image gate, first run, retry, rollup writer, deviation match, waiver,
  attribution, progress sentinel, cleanup, signal/error paths, and final exit-code law.
- [x] Verify exactly one validated verdict remains authoritative; malformed/missing/duplicate verdicts,
  rc mismatches, replay artifacts, and unknown expectations must fail visibly without accidental retry
  or success.
- [x] Verify `MATCH`, `MATCH-BAND`, `DEVIATION`, and `NO-EXPECTATION` against verdict, `nc_gap`, and
  first-failure signature, including shell/regex escaping and absent failure lines.
- [x] Verify attribution is strictly additive: it never overwrites first-run logs/rollup/exit status,
  never labels a non-equivalent solo result `REGRESSION`, and respects time budget/order.
- [x] Verify replay is read-only, cannot forge completion, handles partial/legacy archives honestly,
  does not run live preflight/image checks, and behaves correctly with custom log directories/subsets.
- [x] Verify progress/rollup writes are concurrency-safe and machine-readable; durations, attempts,
  first verdict, expected values, match labels, evidence links, telemetry, and completion sentinel
  cannot be corrupted by parallel workers or stale files.
- [x] Exercise synthetic runner cases (including interrupts/partial artifacts where practical) and add
  independent regressions for uncovered authority/cleanup defects.

## D. Failure evidence and polling semantics

- [x] Audit `lib/assert.sh` state lifetime, quoting, ordinals, `_AS_OUT`/rc freshness, all failure
  entrypoints, evidence naming, telemetry timeouts, unwritable/full destinations, secret exposure, and
  guarantee that recording cannot affect counters/verdict/rc.
- [x] Audit `lib/log.sh` nested frame behavior under `dash` and Bash, timeout/deadline arithmetic,
  fast-start/fixed grids, interval edge cases, command argv preservation, return status, and
  `POLL-WAIT` formatting.
- [x] Independently enumerate every changed or newly fast `poll_until` predicate and classify it as
  read-only onset, effectful, stability-window, or deliberate settle; verify the exemption gate is
  complete and non-vacuous rather than merely matching a hand-maintained allowlist.
- [x] Check drill-10 open-coded poll conversions for timeout/sample-budget equivalence and side effects;
  check fixed-mode changes in drills 32/73/74/93 and settle timers for semantic preservation.
- [x] Run shell syntax/static checks plus poll reentrancy/mode/contract tests under all available shells;
  add focused independent tests for any discovered edge.

## E. Parallel provisioning and image gate

- [x] Audit `simcluster cmd_up` two-phase parallel bring-up for `set -e` behavior, background PID/wait
  handling, missing rc files, temp-path safety, concurrent invocation collisions, node-name/path
  injection, partial failure cleanup, retry diagnostics, existing-node idempotency, and parity with the
  prior serial path.
- [x] Verify `check-image` hoisting cannot be bypassed by user-controlled environment, replay, direct
  drill calls, nested runner calls, early failures, or stale vendor/image state; confirm the check runs
  exactly once per live sweep and never during replay.
- [x] Review host preflight/kernel-cap checks for portability, arithmetic, false refusal/false pass,
  permission behavior, and fidelity to actual privileged-container uid/resource usage.
- [x] Use hermetic stubs and, if still material after local review, live simcluster drills to validate
  parallel provisioning, image checking, and failure cleanup.

## F. Drill-level oracle preservation

- [x] Review every changed drill and prove no `assert_*` call was deleted or weakened, no product action
  was replaced by sim-side compensation, and polling/batching preserves the product path.
- [x] Check drills 20/91 `--reset-js` call-site changes and the new store-move oracle against real
  force-single semantics; ensure no manual reset still masks a product regression.
- [x] Check drill 90 batching preserves all 72 invocations, quoting, per-call failure visibility,
  ordering, actor identity, and aggregate assertion behavior.
- [x] Check drill 74 stage classification/band signature cannot relabel harness/setup failures as #34;
  inspect drills 73/74 stability/effectful fixed polls and changed quiet windows.
- [x] Check smaller poll-mode edits in drills 32/93/95/97 and any ledger expectation changes for
  behavioral or accounting drift.
- [x] Independently compare `assert_*`, `product_red`, `not_covered`, and setup assertion call-site
  inventories before/after.

## G. Verification, live evidence, and handoff

- [x] Run `sh test/simcluster/tests/run-all.sh` and each new gate directly with fresh temp state; run
  relevant shellcheck/lint or explain tool availability.
- [x] Run project hard gates proportionate to this dev-tool-only change (`make test`, `make e2e`,
  `make lint`) and distinguish unrelated/environment failures from review findings.
- [x] Inspect the archived deploy-tier artifacts cited by the implementation; cross-check rollups,
  durations, deviation sets, attribution labels, verdicts, and evidence against the written claims.
- [x] Inspect current simcluster server state. Run focused live drills/synthetic failure exercises where
  they materially validate modified deploy-tier behavior; avoid a redundant full sweep unless evidence
  gaps require it.
- [x] Record every confirmed issue with severity, exact location, consequence, reproduction/evidence,
  and remediation direction; separately list doubts and non-blocking recommendations.
- [x] Write an external-review report beginning with `Pass` or `Fail`, re-read it against this checklist,
  complete boxes truthfully, stage every file with `git add -A`, and verify there are no unstaged or
  untracked files.

## Execution record

- Scope reconstructed from an initially empty index: 19 modified files plus 10 untracked files. The
  review added only this tasklist, the external report, and
  `tests/simcluster-accel-external-review-test.sh`.
- Migration cross-check: 38 old rows and 38 new rows; expected verdict and owner are unchanged for every
  row, every former prose note occurs verbatim as a line in the new log, and all 38 note headings exist.
- Call-site inventory: no aggregate assertion-family deletion (`assert_ok` 1114→1116,
  `assert_setup` 176→177, `assert_refuses` 98→98, `assert_bug` 4→4, `product_red` 28→28,
  `not_covered` 92→93). Drill 90 retains 72 product CLI invocations.
- Hermetic gates: `sh test/simcluster/tests/run-all.sh` passed all 12 shipped gates plus kept-sites.
  POSIX scripts passed `sh -n` and `dash -n`; Bash runners passed `bash -n`. `shellcheck` is not installed.
- Independent RED test: all five added adversarial cases fail against the reviewed tree (cross-mode
  nested poll frame, ownerless band, unknown defect, duplicate row, prose-only signature mention).
- Project gates: `make lint` passed. The first unsandboxed `make test` failed only
  `TestRebuildNoGoroutineLeak`; its focused `-count=3` rerun passed and a second `make test` passed.
  `make e2e` failed p13 once (`lab-1` not ONLINE within 3 s); that exact test passed three focused
  reruns. The release command itself was not green.
- Deploy tier: `weilandserver` was initially offline, later ONLINE. No redundant fourth full sweep was
  launched: the two cited 38-drill `-j 6` archives were inspected directly over `tether exec`.
  Their rollups independently confirm deviation sets `{30,52,74,96}` and `{30,50,52,96}`; the run-2
  drill-50 log confirms a 180-second reader-recovery runtime-guard gap.
