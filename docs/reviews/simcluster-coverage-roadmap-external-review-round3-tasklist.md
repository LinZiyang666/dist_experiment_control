# Simcluster Coverage Roadmap External Review Round 3 Tasklist

Scope: independent re-review of unstaged rev4 relative to the staged rev3 baseline. The round-2
maintainer response is a claim index only. This remains roadmap-only: no S0-S9/product implementation.

## Boundary and accounting

- [x] Rebuild staged-vs-unstaged scope and confirm the only pre-review mutations are roadmap/review
  responses plus the new coverage inventory.
- [x] Rough-read the rev3→rev4 diff, round-2 response, and complete new inventory before detailed review.
- [x] Build a closure matrix for R2-F1-F8 and questions Q1-Q3 with exact roadmap/inventory evidence.
- [x] Re-read the complete rev4 roadmap and both changed response tails for internal consistency.

## Load-bearing remediations

- [x] R2-F1/Q1: verify per-broker same-netns HTTPS ingress is addressable on the instance bridge,
  reaches only loopback upstreams, has an executable CA/SAN/trust lifecycle under every batch reorder,
  forbids plaintext bypass, carries positive/negative TLS controls, and cleans up with the instance.
- [x] R2-F2/Q2: reproduce a complete source inventory of commands/Hidden bits/behavior flags,
  `pubSysEvent`/auth events, dedicated reasons/events, architecture-promised kinds, and store-backed
  alert kinds; prove the new appendix is actually complete, uniquely authoritative, and not deferred.
- [x] R2-F3: verify the one S0 ownership rule, per-batch dependency lines, status transitions,
  default/first owner, lifecycle tuple, S0-CA sharing, tunnel/PTY consumers, and reordered first batch.
- [x] R2-F4: validate the correct-PIN eleven-identity same-IP throttle oracle, second-IP control,
  window recovery, source-IP visibility, actor freshness, membership/event counts, and RED semantics.
- [x] R2-F5: verify off-cluster backup survives node-volume destruction but is instance-namespaced,
  fresh, permissioned, trap-cleaned, and removed by final nuke together with original secrets.
- [x] R2-F6/Q3: trace actual online-backup/offline-restore preconditions, required target cleanup,
  identity/Raft reset, surviving agent/process behavior, reconnect authorization, drop directive and
  no-RC audit; ensure the product-path orphan sequence is executable without omitted destructive steps.
- [x] R2-F7: verify Git wording now delegates exactly to `CLAUDE.md §6` and response evidence lines are correct.
- [x] R2-F8: verify the soak oracle no longer implies unavailable goroutine telemetry and the explicit
  NOT-COVERED decision is routed without weakening FD/RSS/thread leak gates.

## Inventory and roadmap coherence

- [x] Validate inventory scope statement vs §§1.1-1.5/§2: exact kind names, writer status, channel
  classification, deterministic trigger, observation source, drill ownership, NOT-COVERED/probe status.
- [x] Validate the command inventory is a full command/flag list now—not merely examples or work
  deferred to S1—and every row maps to roadmap §3/§4 or an explicit reason.
- [x] Check all ~30 new/~37 total drills, topologies, dependencies, S0 statuses, acceptance exits,
  gotcha/DOC routing, and source references after the rev4 edits.
- [x] Reapply Mandate ①-④ and destructive five-element discipline to ingress, backup/restore, PIN,
  orphan, partition and soak changes; look for false-green controls and cleanup leaks.

## Independent verification and closure

- [x] Run source enumerations, command-tree inspection, exact restore/inventory searches, focused tests,
  Markdown/diff checks, and document why live simcluster remains necessary or unnecessary.
- [x] Record any residual doubts/recommendations separately; distinguish blocking roadmap defects from
  leaf-plan precision that is safely delegated.
- [x] Write a round-3 report beginning with `Pass` or `Fail`, including closure matrix, findings,
  verification, and explicit release recommendation.
- [x] Re-read roadmap/inventory/responses/report/tasklist; close boxes truthfully, stage everything with
  `git add -A`, and verify cached diff contains only review-stage documentation.
