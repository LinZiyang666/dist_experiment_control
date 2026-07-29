# Batch C external re-review tasklist

> Scope: the 27 developer-modified paths outside the index after the first external review, plus the
> previously staged Batch C implementation where a new fix changes an invariant. Developer responses
> and claimed green gates are evidence leads only. The reviewer is authorized to repair confirmed
> production defects after the independent review boundary has been recorded and staged.

## A. Intake and claim reconstruction

- [x] A1. Record branch, HEAD, staged baseline, unstaged path inventory/stat/hash, and diff hygiene.
- [x] A2. Re-read `CLAUDE.md`, binding architecture/usage sections, prior external findings, and the
  developer's inline disposition without treating the disposition as proof.
- [x] A3. Map every unstaged change to B1/B2/M1–M4/m1/T1 and identify unrelated or incomplete changes.
- [x] A4. Verify prior reviewer tests were not weakened and add adversarial cases for newly introduced
  recovery, ledger, classifier, budget, compatibility, and deploy-tier paths.

## B. B1 and T1 — durable force-single intent

- [x] B1. Audit the on-disk format, permissions, symlink/regular-file rules, atomic replace, parent/file
  fsync, malformed/partial/legacy reads, stable epoch, scope fields, and cleanup semantics.
- [x] B2. Trace write-before-rewrite and every error/crash window through apply, marker, epoch, prune,
  finalize-op creation, leadership resume, response, restart, and second recovery.
- [x] B3. Prove a stale/cross-node intent cannot mutate another node, cannot prune a rejoined member,
  cannot overwrite a newer epoch, and cannot be cleared until every durable fact is observed.
- [x] B4. Verify the fallback marker path remains safe for upgrades while the intent path does not
  recreate work after completion or bypass the exact ghost-shape/rejoin guards.
- [x] B5. Verify journal access cannot be influenced by an untrusted node ID/path and that every broker
  configuration supplies a valid, persistent ClusterDataDir.
- [x] B6. Inspect unit tests for production-path fidelity and mutation strength; run focused race tests.
- [x] B7. Inspect drills 20/22 for non-vacuous injection, real restart, cleanup, correct verdict class,
  and fidelity to online rather than merely offline recovery.

## C. B2 — durable-ledger protection

- [x] C1. Trace every inflight ledger writer/reader and object-name/bucket identity across push/pull,
  home/cross-home, terminalization, restart, old rows, duplicate IDs, and clock skew.
- [x] C2. Audit `ledgerProtectedObjects` query/error handling, malformed sizes/timestamps/verbs/tiers,
  overflow, terminal semantics, stable now, budget+slack derivation, and fail-closed behavior.
- [x] C3. Verify protection is scoped to the correct bucket and object, cannot cross-protect unrelated
  objects, and cannot retain objects forever after terminal or expired ledger rows.
- [x] C4. Prove all deletion call sites receive the guard and no alternate reaper path bypasses it.
- [x] C5. Run the original real JetStream restart regression plus focused negative/expiry/error cases.

## D. M1–M4 and m1

- [x] D1. Verify pull is consistently documented as fixed five minutes at ctl, broker, agent, usage,
  help, examples, and timeout validation; no claim implies the ctl flag extends internal watchdogs.
- [x] D2. Independently rederive the transfer formula with overhead, max value, CLI/agent slack, ledger
  expiry, GC inequality, integer/rounding bounds, and all literal/config consumers.
- [x] D3. Verify doctor and `--wait` consume every topology state with correct polarity, severity,
  next-step and exit taxonomy; the exhaustiveness guard must fail for a newly added state.
- [x] D4. Audit `AllTopoStates`/classifier API design for duplicates, mutable shared slices, impossible
  synthetic states, future enum behavior, and compatibility.
- [x] D5. Verify unknown operation kinds remain byte-for-byte unmodified across repeated drives and
  leader changes while operator abort remains enum-independent and observable.
- [x] D6. Audit additive inconsistency reason production, wire omission, legacy fallback, precedence,
  all renderers, JSON schema, docs, and remedies for correctness and compatibility.

## E. Cross-cutting implementation review and repair

- [x] E1. Review all 27 unstaged paths for unchecked errors, TOCTOU, races, stale reads, path traversal,
  nil/zero values, timer/goroutine/resource leaks, integer overflow, and unsafe defaults.
- [x] E2. Check test naming/origin discipline, non-vacuity, production-path use, cleanup, timing
  stability, duplicated formulas, and forbidden implementation-presence assertions.
- [x] E3. Run gofmt/diff check, focused tests, build, vet/tag slices, lint, `make test`, targeted race,
  and the sole full matrix `make e2e-parallel`.
- [x] E4. Rebuild the simcluster image if production binaries changed; run only the affected drills and
  inspect raw logs, unique verdicts, not-covered rows, restart counters, and Docker cleanup.
- [x] E5. Record the independent review boundary and stage it before production repair.
- [x] E6. Repair every confirmed code/document/test defect within scope, add durable regression guards,
  and rerun the proportional gates.
- [x] E7. Write a new final report whose first line is `Pass` only if no blocker/major remains; include
  findings fixed by the reviewer, residual doubts/limitations, evidence, and exact drill outcomes.
- [x] E8. Mark all tasks complete or explain a blocker; stage every file, run cached diff checks, record
  the final cached patch hash, and prove no content remains outside the index.
