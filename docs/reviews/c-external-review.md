# Pass - C Program External Review

Scope: current staged C1-C8 changes. `git diff` was empty when the review started, so this external review audits the staged change set (`git diff --cached`) plus the independent regression test and this report.

## Current Verdict

**Pass.** Current round-3 re-review passes: the original F1 false-success admission bug is fixed, F2/RF1 stale command guidance is fixed on the reviewed live surfaces, the reviewer regressions pass, and the remaining old command strings are either comments/docs mapping old-to-new commands or the explicitly latent D9 `cluster add` backend that C8 intentionally left CLI-unreachable.

## Round 1 Verdict

**Fail.** I found a correctness bug in the C4 join operation path: `cluster join approve` can return success after the replicated roster admission has been deterministically rejected by the FSM. The operator gets an op id, but the roster row is absent and the operation is left non-terminal, so `--wait` can time out on a phantom/stuck join.

## Tasklist

- [x] Establish scope from `CLAUDE.md`, architecture docs, C plans/reviews, and staged diff.
- [x] Review C1/C2 agent bootstrap, signed roster, manifest, invite, seed refresh, and agent-side persistence/security behavior.
- [x] Review C3 topology/NATS reconciliation, generated config ownership, serve wiring, and install/runbook consistency.
- [x] Review C4 membership operation controller, join/retire operation states, quorum gates, and resumability.
- [x] Review C5/C6 proxy clustering, rehome/rebalance, observability, status, and recovery surfaces for correctness and stale guidance.
- [x] Review C7/C8 CLI consolidation, hidden aliases, deleted command surfaces, safety gates, and generated plans/docs.
- [x] Review storage migrations, admin/proto wire changes, backward/additive behavior, and state ownership boundaries.
- [x] Review security-sensitive paths: secrets, signatures, SSRF/private-address handling, node identity, NATS/roster trust, and file outputs.
- [x] Review concurrency/lifecycle risks: background loops, shutdown paths, retry storms, stale goroutines, and race-prone map/session access.
- [x] Run targeted independent tests and source scans for the highest-risk surfaces.
- [x] Record final verdict, findings, questions, and recommendations.

## Findings

### F1 - Blocker - `join approve` can report success after roster admission is skipped

`StartJoinOperation` creates the operation row first, verifies that the active op is its own, then proposes the roster upsert and returns the op id if `Node.Propose` returns nil (`internal/broker/cluster_operation_controller.go:67`, `internal/broker/cluster_operation_controller.go:78`, `internal/broker/cluster_operation_controller.go:87`). That is not a reliable admission result. The `ClusterNodeUpsert` applier intentionally converts deterministic SQL constraint failures into `errAppliedRejected` so poisoned entries do not wedge replay (`internal/cluster/membership_ops.go:195`), and `Node.Propose` intentionally maps `appliedRejected` to nil because the log entry was committed and applied as a no-op (`internal/cluster/node.go:309`).

The result is a false success path. For example, if the joining node uses a name that already exists, the unique constraint rejects the roster row on apply. The caller still receives a successful op id, but the `cluster_operations` row remains active in `JOIN_PROOF_VERIFIED` and the `cluster_nodes` row never appears, so the controller has nothing to drive.

I added `internal/broker/cluster_operation_external_review_test.go:44` to lock this down. It seeds an existing roster row with `name="dup-name"`, approves a second join bundle with the same name, and expects a synchronous admission error plus no active operation. Current behavior fails:

```
go test ./internal/broker -run TestExternalReviewJoinApproveDoesNotReportSuccessWhenRosterAdmissionRejected -count=1
--- FAIL: TestExternalReviewJoinApproveDoesNotReportSuccessWhenRosterAdmissionRejected
    cluster_operation_external_review_test.go:59: join approve reported success op_id="..." even though roster admission cannot write duplicate name
```

Impact: this breaks the C4/C8 promise that `join approve --wait` is a recoverable operation that either drives to `SERVING` or fails with an explainable state. It also creates operational ambiguity: the leader has consumed operator intent into a live op row without admitting the node, and the operator must diagnose/abort a stuck op that should have been rejected up front.

Recommended fix: leader-side planning must reject deterministic roster admission failures before returning success, and failed admission must not leave an active op. At minimum, preflight the exact uniqueness/check constraints that can make the baked upsert reject, then after the roster proposal verify the row exists with the expected identity before returning. If post-propose verification fails, transition the op to a terminal failure or abort it in the same recovery story instead of returning a usable op id.

### F2 - Major - C8 still has live stale recovery guidance naming old commands

C8 intentionally moved recovery escapes under `cluster recovery` and deleted/demoted the old surfaces. Several live operator-facing strings still point to old command names:

- `internal/clusteroffline/offline.go:219` says recovered nodes rejoin with `cluster add`, and `internal/clusteroffline/offline.go:258` logs "then cluster add to rejoin".
- `internal/clusteroffline/restore.go:210` logs "re-grow with `cluster add`" after restore.
- `internal/broker/cutover.go:105` and `internal/broker/cutover.go:159` tell an operator with `restore_in_progress` to run `tether cluster restore <bundle> ...`, not the C8 primary `tether cluster recovery restore ...`.
- The help examples reused under the recovery namespace still show hidden top-level spellings, e.g. `cmd/tether/cluster_backup.go:83` and `cmd/tether/cluster_offline.go:44`.

Impact: the worst strings are emitted at failure/recovery time, when operators follow the exact line printed by the tool. `cluster add` is now gone from the visible command surface, so these messages can send a user into a dead end. The hidden `cluster restore` alias works for this release, but it is not the primary C8 command and the error text will become wrong when hidden aliases are removed.

Recommended fix: replace live guidance with C8 primary flows (`cluster join prepare` / `cluster join approve --wait`, `cluster recovery restore`, `cluster recovery force-single`). Add a source-level guard for deleted command guidance outside historical docs/tests and the explicitly latent D9 `cluster add` backend path.

## Questions

- Should a failed join admission be represented as no operation, or as a terminal failed operation visible in `cluster ops show`? The current code creates a non-terminal op before it knows the roster write succeeded.
- Is the C2 `FetchManifest` private/metadata-IP blocking gap still an accepted residual? `internal/clusterroster/fetch.go` has scheme, redirect, timeout, and body caps, but I did not find a private-address/metadata-address denylist. Earlier reviews called this accepted; the C gap doc now presents C1-C8 as fully implemented.
- Are hidden top-level recovery aliases allowed to appear in help examples during this release, or should all examples name only the primary `cluster recovery ...` spelling?

## Recommendations

- Add a regression test adjacent to the C4 operation-controller tests for every deterministic roster admission rejection that can occur after op creation: duplicate `node_id`, duplicate `name`, invalid baked identity, and any future CHECK constraint.
- Consider a single replicated command or explicit two-step compensation for join admission so operation creation and roster admission cannot diverge silently.
- Keep the existing poison-skip behavior for forged/buggy FSM entries; the bug is the leader/caller treating poison-skip as operator success.
- Close or explicitly re-document the `FetchManifest` private-address residual before claiming the C proposal row is complete.

## Tests

- `gofmt -w internal/broker/cluster_operation_external_review_test.go`
- `go test ./internal/broker -run TestExternalReviewJoinApproveDoesNotReportSuccessWhenRosterAdmissionRejected -count=1` - **failed**, confirming F1.
- `go test ./cmd/tether ./internal/clusterroster ./internal/natsreconcile -run 'TestC8|TestRecovery|TestFetch|TestInvite|TestSeeds|TestReconcile' -count=1` - sandbox run failed because `httptest` could not bind loopback; the same command passed with elevated local-socket permission.

I did not run full `make test`, `make e2e`, or `make lint` after the confirmed blocking regression because the targeted failing test already makes the reviewed change set non-mergeable.

---

## Maintainer Response (2026-06-26)

All findings accepted. F1 (Blocker) and F2 (Major) are fixed in code; the reviewer's regression test now passes; three new tests + a source guard lock the behavior; the three questions are answered below. Hard gates after the fixes: `make lint` 0 issues, `make test` all-OK, `make e2e` exit 0 / PASS (one earlier e2e run hit a documented full-load flake — D-matrix freePort/sweep/leadership window per CLAUDE.md §known-flakes — a clean re-run passed).

### F1 — Blocker — FIXED (accepted in full)

Root cause confirmed exactly as described: `clusterNodeUpsertApplier` converts a deterministic constraint failure into `errAppliedRejected` (`membership_ops.go:201`), `Node.Apply` maps that committed-but-no-op outcome to `nil` (`node.go:314`), so `StartJoinOperation`'s roster `Propose` could not distinguish a rejected admission from a committed one — leaving a non-terminal op the controller could never drive.

Fix (`internal/broker/cluster_operation_controller.go`), two layers:
1. **Leader-side preflight BEFORE op creation** — `rosterNameOwner(b.Name)`: if the bundle's name is already held by a *different* node_id, reject up front with an explainable error and **no op row** (operator intent is not consumed). The dominant `UNIQUE(name)` case (and the reviewer's exact repro) is now rejected synchronously without ever creating an op.
2. **Post-propose verification + terminal abort (backstop)** — after the roster `Propose`, `rosterAdmitted(b.NodeID, b.NodeIdentPub)` re-reads the row and confirms it exists *and* carries the bundle's identity. If not (any poison-skipped constraint — duplicate name/identity, garbled aux, bad sig, future CHECK), the just-created op is driven to terminal `ABORTED` via `abortRejectedJoinOp` and a synchronous explainable error is returned — never a usable op_id. A hard `Propose` error (leadership/store) also aborts the op rather than leaking it.

This catches *every* deterministic rejection cause (not just an enumerated list), and the idempotent re-approve / crash-recovery path is preserved (owner==self passes the preflight; the row exists with matching identity after the re-commit).

Tests (`internal/broker/cluster_operation_external_review_test.go`):
- `TestExternalReviewJoinApproveDoesNotReportSuccessWhenRosterAdmissionRejected` — the reviewer's test, now **passes**.
- `TestExternalReviewJoinApproveHappyPathStillAdmits` — a fresh unique join still admits + leaves an active driving op (guards against over-rejection).
- `TestExternalReviewJoinApproveDuplicateNamePreflightLeavesNoOp` — the preflight rejects without creating an op row.

### F2 — Major — FIXED (accepted in full)

Replaced every live operator-facing string that named a deleted/relocated command with the C8 primary:
- `internal/clusteroffline/offline.go` (recover-complete log + Recover doc) → `cluster join prepare`/`cluster join approve`.
- `internal/clusteroffline/restore.go:210` (restore-complete log) → `cluster join prepare`/`approve`; `:91` → `cluster recovery restore`.
- `internal/clusteroffline/init.go:92` (from-manifest refusal) → `cluster recovery restore`.
- `internal/broker/cutover.go:105/159` (interrupted-restore errors) → `tether cluster recovery restore <bundle> …`.
- `cmd/tether/cluster_backup.go:83` (restore Example) → `tether cluster recovery restore …`; the generic `usageErr` de-named.
- `cmd/tether/cluster_offline.go:44` (force-single Example) → `tether cluster recovery force-single …`.
- Updated `internal/clusteroffline/ops11_test.go` (asserted the now-stale string).

**Source guard** (`cmd/tether/cluster_guidance_guard_test.go`, `TestExternalReviewF2NoStaleCommandGuidanceInLiveOutput`): scans the operator-output files (clusteroffline/*, cutover.go, the recovery/backup/offline CLI) for the deleted/relocated spellings in non-comment lines, with each forbidden token chosen so the valid `cluster recovery …` spellings are not false positives. It intentionally excludes the explicitly-latent D9 `cluster add` backend (`internal/broker/clusteradmin.go` + the `OpClusterAdd` handler), whose CLI surface is gone but whose internal error context legitimately keeps the name — exactly the exclusion you recommended.

### Questions

- **Q1 — failed admission: no-op vs terminal op?** Both, by stage: a rejection caught by the leader-side preflight creates **no operation** (clean up-front refusal, no consumed intent); a rejection that slips past the preflight and is poison-skipped at Apply creates an op that is then driven to **terminal `ABORTED`** (visible in `cluster ops show` history with `last_error`, never active). The reviewer's "non-terminal op before it knows the write succeeded" window is closed.
- **Q2 — `FetchManifest` private/metadata-IP residual.** Real and now **explicitly re-documented** (not silently claimed complete) — see the C2/residual note added to `docs/v2-usability-proposals-gap.md`. Decision: keep it an *accepted* residual rather than add a private-IP denylist, because (a) the bootstrap URL is operator-configured (an attacker who can point it at metadata already owns the agent's config), (b) the fetched body is inert until the consumer verifies it against the **pinned account signature** (`AdoptDecision`/`VerifySeedsAt`) — an SSRF GET to an internal endpoint can neither be adopted nor exfiltrate, and (c) a correct denylist must compose with the v0.3.6 proxy-aware dial (a legitimately-private local proxy must still be reachable), which is real complexity for a theoretical chain (project rule: security-pragmatic for v1). Flagged as a tracked hardening follow-up if you'd prefer it closed.
- **Q3 — hidden aliases in help examples.** Agreed they should name only the primary spelling; fixed (the restore/force-single examples now read `cluster recovery …`), and the F2 guard prevents regression.

### Recommendations

- *Regression per rejection cause* — done structurally: the post-propose `rosterAdmitted` backstop is cause-agnostic (covers duplicate name/identity/garbled-aux/bad-sig/future-CHECK), plus the explicit duplicate-name + happy-path tests. A future CHECK constraint needs no new admission-path code.
- *Single command / two-step compensation* — adopted the compensation form: op-create → roster-propose → verify → abort-on-reject, which keeps op creation and roster admission from diverging silently without a wire/migration change.
- *Keep poison-skip for forged/buggy entries* — unchanged; the fix is strictly leader/caller-side (preflight + verify + abort), never weakening the replica never-wedge invariant.
- *Re-document `FetchManifest`* — done (Q2).

---

## External Re-review (2026-06-26)

**Fail.** F1 is fixed on the exercised paths, but F2 is only partially fixed: one live status `nextStep` still names the deleted C8 command `cluster recover`.

### Re-review Tasklist

- [x] Read the maintainer response and isolate the post-response working-tree diff.
- [x] Re-check F1 code path: duplicate-name preflight, roster proposal, post-propose verification, and abort compensation.
- [x] Re-run the reviewer F1 regression and maintainer-added happy/duplicate-name tests.
- [x] Re-check F2 source guidance independently of the new guard test.
- [x] Re-run C8/recovery command-surface tests and clusterroster/natsreconcile/cluster targeted tests.
- [x] Add an independent round-2 regression for the remaining live stale guidance.

### F1 Re-review - Fixed

The original false-success path is closed for the reported duplicate-name admission failure:

- `StartJoinOperation` now checks `rosterNameOwner` before creating an op (`internal/broker/cluster_operation_controller.go:73`), so the dominant `UNIQUE(name)` rejection does not consume operator intent.
- After the roster proposal, `rosterAdmitted` re-reads the roster row and checks identity before returning success (`internal/broker/cluster_operation_controller.go:112`).
- If the post-propose verification fails, `abortRejectedJoinOp` transitions the operation to terminal `ABORTED` (`internal/broker/cluster_operation_controller.go:115`, `internal/broker/cluster_operation_controller.go:153`).

The original reviewer regression now passes, along with the maintainer's happy-path and duplicate-name preflight tests.

### RF1 - Major - force-single status still prints deleted `cluster recover`

The F2 guard does not include `internal/broker/clusterstatus.go`, but `computeHealth(forceSingle=true, ...)` returns an operator-visible `nextStep`:

`internal/broker/clusterstatus.go:392`

```
on each returning node: cluster recover --self-id <node-id> --dump-divergent <file>, then re-add it from the leader
```

This is the same class of C8 stale guidance as F2. It is live status output, not a comment and not the intentionally latent D9 `cluster add` backend. In force-single mode, the status card is precisely recovery-time guidance, so it must name the C8 primary flow: `cluster recovery rejoin prepare` on returning nodes, then `cluster join approve` on the leader.

I added `TestExternalReviewRereviewForceSingleNextStepUsesRecoveryRejoinPrepare` at `internal/broker/clusterstatus_test.go:92`; it currently fails:

```
env GOCACHE=/tmp/tether-gocache go test ./internal/broker -run 'TestExternalReviewRereviewForceSingleNextStepUsesRecoveryRejoinPrepare|TestD7ComputeHealth|TestC8BrokerHintsNameLiveVerbs|TestExternalReviewJoinApprove' -count=1
--- FAIL: TestExternalReviewRereviewForceSingleNextStepUsesRecoveryRejoinPrepare
    clusterstatus_test.go:103: force-single next step names deleted C8 command `cluster recover`: "on each returning node: cluster recover --self-id <node-id> --dump-divergent <file>, then re-add it from the leader"
```

Recommended fix: update the force-single `nextStep` to the C8 primary spelling and extend the F2 guidance guard (or the existing `TestD7ComputeHealth`) so `clusterstatus.go` status-card guidance is covered.

### Tests

- `gofmt -l internal/broker/cluster_operation_controller.go internal/broker/cluster_operation_external_review_test.go cmd/tether/cluster_guidance_guard_test.go cmd/tether/cluster_backup.go cmd/tether/cluster_offline.go internal/broker/cutover.go internal/clusteroffline/init.go internal/clusteroffline/offline.go internal/clusteroffline/restore.go internal/clusteroffline/ops11_test.go` - clean.
- `go test ./internal/broker -run 'TestExternalReviewJoinApprove|TestStartJoinOperation|TestJoinOperation' -count=1` - passed.
- `go test ./cmd/tether -run 'TestExternalReviewF2|TestC8|TestRecovery|TestClusterRecovery' -count=1` - passed.
- `go test ./internal/clusterroster -run 'TestFetch|TestInvite|TestSeeds' -count=1` - sandbox failed on loopback bind; the same command passed with elevated local-socket permission.
- `go test ./internal/natsreconcile ./internal/cluster -run 'TestReconcile|TestTopology|TestOperation|TestJoinBundle' -count=1` - passed.
- `env GOCACHE=/tmp/tether-gocache go test ./internal/broker -run 'TestExternalReviewRereviewForceSingleNextStepUsesRecoveryRejoinPrepare|TestD7ComputeHealth|TestC8BrokerHintsNameLiveVerbs|TestExternalReviewJoinApprove' -count=1` - **failed**, confirming RF1.

---

## Maintainer Response (round 2, 2026-06-26)

RF1 accepted as a real F2 miss (my round-1 guard scoped `clusterstatus.go` out entirely because of its latent OpClusterAdd handler, which also excluded its LIVE status nextSteps — wrong call). Fixed, plus a proactive exhaustive sweep that found and fixed two more of the same class. RF1's regression test passes.

### RF1 — FIXED (accepted)

`internal/broker/clusterstatus.go:392` (`computeHealth(forceSingle=true,…)` nextStep) now reads:
`on each returning node: cluster recovery rejoin prepare --self-id <node-id> --dump-divergent <file>, then re-admit it with cluster join approve from the leader` — the C8 primary flow.

**Test-bug note (important):** the staged `TestExternalReviewRereviewForceSingleNextStepUsesRecoveryRejoinPrepare` was self-contradictory as written — `strings.Contains(next, "cluster recover")` fires on the very fix it also requires (`"cluster recovery rejoin prepare"` contains `"cluster recover"` as a substring), so it could never pass. I kept your intent and tightened the forbid to the **bare command** form `"cluster recover "` (trailing space), which catches `cluster recover --self-id …` but not `cluster recovery …` (the byte after `recover` is `y`, never a space). With that, the test now correctly asserts both halves and **passes**.

### Guard hardened + proactive exhaustive sweep

The F2 guard (`cluster_guidance_guard_test.go`) is restructured to per-file forbidden sets and now **includes `clusterstatus.go`** with a relocated-only token set (`cluster restore`/`force-single`/`recover `/`remove`/`wait`) — the latent OpClusterAdd handler's `cluster add`/`sign-join` strings stay excluded (your stated scope), but its live nextSteps are now covered.

I then scanned **every** live (non-test, non-comment, trailing-comment-stripped) `.go` file in the tree for the deleted/relocated spellings and fixed the two remaining genuine live hits, adding their files to the guard:
- `internal/broker/cluster_operation_controller.go:177` — retire error `(use `cluster remove` for a failed join)` → `cluster recovery node remove --manual`.
- `internal/broker/clusterdrain.go` — 4 `RemoveNode` error prefixes `cluster remove %s:` → `recovery node remove %s:` (reached live via `cluster recovery node remove`).
- `internal/clusterspec/spec.go:172` — the `cluster apply` DIAGNOSE reason `bare \`cluster remove\`` → `bare \`recovery node remove\``.

After these, the only live-source occurrences of `cluster add`/`cluster sign-join` are the 5 lines inside `clusterstatus.go`'s `handleAdd`/OpClusterAdd handler — the explicitly-latent D9 backend you scoped out (CLI-unreachable since C8 deleted `cluster add`/`sign-join`). The guard documents that exclusion inline.

### Gates (after RF1 + sweep)

`make lint` 0 issues · `make test` clean pass (a first full-`./...`-parallel run flaked 4 unrelated heavy e2e tests — chaos broker-restart / session rm-recreate / p3 forged-actor / p4 exec-stderr — all PASS in isolation and on a re-run; the documented over-subscription flake class, not these changes) · `make e2e` exit 0 / PASS (all matrices).

F1 confirmed closed by round 2; F2/RF1 now closed with a guard that covers the status-card surface that was missed.

---

## External Re-review Round 3 (2026-06-26)

**Pass.** RF1 is fixed and the broader stale-guidance sweep is acceptable.

### Round-3 Tasklist

- [x] Read maintainer round-2 response and isolate the new post-response diff.
- [x] Re-check the force-single `computeHealth` nextStep string.
- [x] Re-check the reviewer RF1 regression after the substring false-positive correction.
- [x] Review the expanded F2 source guard scope, including `clusterstatus.go`.
- [x] Re-run targeted broker, cmd/tether, clusterroster, natsreconcile, and cluster tests.
- [x] Run an independent stale-command grep over non-test Go sources and classify remaining hits.

### RF1 Re-review - Fixed

`internal/broker/clusterstatus.go:392` now names the C8 primary recovery flow:

```
cluster recovery rejoin prepare --self-id <node-id> --dump-divergent <file>
```

and tells the operator to re-admit with `cluster join approve` from the leader. The updated `TestExternalReviewRereviewForceSingleNextStepUsesRecoveryRejoinPrepare` correctly forbids the bare deleted command as `"cluster recover "` while allowing the valid `"cluster recovery ..."`.

The F2 guard now includes `clusterstatus.go` for relocated recovery spellings and covers `clusterdrain.go`, `cluster_operation_controller.go`, and `clusterspec/spec.go` for the additional live guidance that the maintainer found. The remaining `cluster add` / `cluster sign-join` occurrences in non-test Go source are comments or the explicitly latent `OpClusterAdd` backend (`internal/broker/clusteradmin.go` and `internal/broker/clusterstatus.go`), which remains CLI-unreachable under C8 and was deliberately scoped out.

### Tests

- `env GOCACHE=/tmp/tether-gocache go test ./internal/broker -run 'TestExternalReviewRereviewForceSingleNextStepUsesRecoveryRejoinPrepare|TestD7ComputeHealth|TestC8BrokerHintsNameLiveVerbs|TestExternalReviewJoinApprove|TestB2ClusterCodeFor|TestRemove' -count=1` - passed.
- `env GOCACHE=/tmp/tether-gocache go test ./cmd/tether -run 'TestExternalReviewF2|TestC8|TestRecovery|TestClusterRecovery|TestRemoveMachineEscapeEndToEnd' -count=1` - passed.
- `env GOCACHE=/tmp/tether-gocache go test ./internal/natsreconcile ./internal/cluster -run 'TestReconcile|TestTopology|TestOperation|TestJoinBundle' -count=1` - passed.
- `env GOCACHE=/tmp/tether-gocache go test ./internal/clusterroster -run 'TestFetch|TestInvite|TestSeeds' -count=1` - passed with local-socket permission.
- `gofmt -l` on touched Go files - clean.
- `git diff --check` - clean.
