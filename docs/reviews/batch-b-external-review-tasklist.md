# Batch B independent external review tasklist

Date: 2026-07-27
Reviewer: independent external reviewer
Base: unstaged and untracked working tree on `HEAD=52d3b80` (`main`)
Initial boundary: 41 tracked modified files (`+1194/-604`) plus 22 untracked files
(including the Batch B plan/review, 19 Go/test files, and `tools/rescue.py`).

## Boundary and authority

- [x] Freeze and inventory every unstaged/untracked file; do not accept the internal review's scope or verdict as authority.
- [x] Read `CLAUDE.md`, the WHAT/HOW authority chain, testing rules, cluster operations docs, and simcluster mandate relevant to the delta.
- [x] Map the final implementation to Batch B's stated scope, deferred/cut items, later scope expansion, and user-visible contract changes.
- [x] Re-check the implementation boundary before final certification and record any reviewer-only additions.

## B1 admission and authentication boundary

- [x] Audit all three subject parsers, every `verbSpec`/`ctrlVerbSpec`, handler ordering, follower short-circuits, role/session/node-state decisions, error/audit/log behavior, and fail-closed zero values.
- [x] Independently verify the subscription-to-gate reconciliation and every ungated exemption against real subscription and handler semantics.
- [x] Verify store/storage details no longer cross untrusted wire boundaries while useful detail reaches the correct logs.
- [x] Audit the new cluster-mode seams in authcallout/adminsock and production wiring; prove missing seams fail closed without consuming PIN budgets or leaking internals.

## B2 forwarding, allocation identity, and metrics

- [x] Audit `writeVerbs` dispatch equivalence for all verbs/payloads, ReqID-before-lookup ordering, unknown-verb behavior, JSON/wire compatibility, and rolling-version safety.
- [x] Verify allocation narrowing is derived (not name-based), identical on leader/follower/single-node paths, and stays aligned with every SQL fence column.
- [x] Audit forward outcome classification, zero-value counter safety, cardinality/concurrency behavior, exposition, and all intentionally silent paths.

## B3 database-role enforcement

- [x] Audit `readDB`, `singleWriter`, liveness writes, proc-GC behavior, and every remaining direct `Config.DB` site for role correctness and fail-closed behavior.
- [x] Adversarially validate the AST ratchet, its baseline completeness, self-checks, line/function identity, and resistance to aliasing or helper-based bypass.
- [x] Check the documented liveness-column contract against actual SQL writes and current architecture.

## B4 join/version and cluster status honesty

- [x] Audit join-bundle compatibility, signing/PoP boundary, declared version stamping, live-path gate ordering, typed error/exit classification, replay/nonces, and zero-raft-write rejection.
- [x] Audit ACCT.NK collection and three-state rendering for self/peer/orphan/offline/pre-v6/missing-key/skew cases; verify advisory, health, and schema compatibility.
- [x] Audit online doctor nats.conf selection and issuer cross-check for explicit/default/reported/old-broker/custom-path cases.
- [x] Verify cluster health schema/call-site changes across all tagged builds and user/runbook documentation.

## Wire, scanners, and test quality

- [x] Adversarially inspect wire-freeze and canonical-signature guards for vacuity, reflection/AST blind spots, nested/map/interface/embedded types, aliases, new verbs, and new fields.
- [x] Inspect error-code and ACL site-scoped exemptions for live anchors, accurate reasons, complete emitters, and correct retry classification.
- [x] Mutation-test load-bearing new guards and independently add focused regression/counterexample tests where existing tests only restate implementation.
- [x] Review new tests for races, ordering assumptions, resource leaks, false goldens, soft-pass branches, brittle source-name coupling, and excessive runtime.

## Out-of-band rescue tool

- [x] Treat `tools/rescue.py` as an independent high-risk root-shell product surface despite Batch B's scope note; check syntax, defaults, authentication, confidentiality/integrity, replay/session binding, lifecycle, daemon/supervisor behavior, and documentation consistency.
- [x] Reconcile it with `docs/testing-standards.md`'s explicit security finding about this exact file and determine whether it can be staged/released at all.

## Verification and deploy tier

- [x] Run formatting/diff/static checks and focused package tests; classify every red rather than assuming flake.
- [x] Run affected concurrency paths with `-race` and relevant leak/resource gates.
- [x] Run `make test`, `make lint`, and the sole full-matrix gate `make e2e-parallel` on a stable tree.
- [x] Read simcluster server instructions, verify host/server state, and run only the deploy-tier drill(s) materially required by join/status changes; preserve exact verdict/gap evidence.

## Deliverables

- [x] Write an external review report whose first line is `Fail` or `Pass`, with findings, doubts, recommendations, scope, and exact verification evidence.
- [x] Stage every file, including reviewer tests/tasklist/report, and verify no unstaged or untracked files remain.

## Completion evidence

- Reviewer-only additions: this tasklist, `internal/broker/batch_b_external_review_test.go`, and
  `docs/reviews/batch-b-external-review.md`. No product implementation was changed by the reviewer.
- The counterexample test proves the disk-alert forwarding outcome is absent from the exported
  counter snapshot; it is intentionally RED until the product path is fixed.
- `make test` failed only on that counterexample. `make e2e-parallel` represented all 15/15
  top-level matrices and failed three of 99 units: two copies of the counterexample plus one loaded
  leader-election timing failure. The latter passed an isolated `-race -count=10` rerun and remains
  an unresolved gate-stability defect rather than being dismissed as noise.
- `make lint` passed with zero issues; independent `gofmt -l` still found four changed files.
- Deploy tier: current-source image build passed; isolated `10-grow-to-3` was GREEN with 19
  assertions and zero gaps.
