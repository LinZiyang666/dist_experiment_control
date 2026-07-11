# Simcluster Coverage Roadmap External Review Round 4 Tasklist

Scope: independent re-review of unstaged rev5 against the staged rev4/round-3 baseline. Maintainer
responses are claim indexes only. This remains roadmap-only; no product or harness implementation.

## Boundary and closure

- [x] Rebuild staged-vs-unstaged scope and rough-read the complete rev4→rev5 diff.
- [x] Verify R3-F1's revised orphan sequence against actual agent/NATS/systemd control flow and its
  precondition, trigger, observation and cleanup oracles.
- [x] Verify R3-F2 by independently enumerating the reachable Cobra tree, Hidden construction paths,
  deployment-behavior flags, event/reason/action sets and alert kind/dedup fields; compare every result
  with the claimed full inventory rather than trusting its count.
- [x] Verify R3-F3's backup lifecycle wording is consistent at S0, S7 drill and harness locations.
- [x] Recheck both prior doubts: CA ownership/trust stability and source-reference correctness.

## Roadmap quality and regression surface

- [x] Check rev5 polish for accidental semantic changes, contradictory ownership, false-green assertions,
  impossible triggers, missing cleanup, and roadmap/inventory drift.
- [x] Reapply the destructive five-element discipline to restore/reconnect and verify restarting NATS does
  not invalidate the preserved-process premise or destroy an unrelated required state.
- [x] Distinguish blocking defects from leaf-plan-safe precision; record residual doubts and recommendations.

## Verification and handoff

- [x] Run focused source/tests and Markdown/diff checks proportional to this documentation-only change;
  explain why live simcluster is or is not useful before implementation.
- [x] Write a round-4 report beginning with Pass or Fail, including closure matrix, findings, doubts,
  verification and release recommendation.
- [x] Re-read all rev5 artifacts and round-4 outputs, close boxes truthfully, stage every file with
  `git add -A`, and verify there are no unstaged changes and cached scope is documentation-only.
