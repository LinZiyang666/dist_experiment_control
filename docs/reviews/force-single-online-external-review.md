# Pass - online force-single external review (round 2 final)

Reviewer role: external reviewer. Scope: round 2 changes for online force-single, including the
main-process fixes to the previous four findings, new handler/CLI/mTLS coverage, docs/runbook updates,
and the two reviewer-added round 2 regressions fixed directly during this pass.

Conclusion: Pass. The round 1 blockers/high-risk issues are addressed, and the two additional round 2
gaps I found during independent review were fixed before this final conclusion:

- Broker-side self-id validation now requires a non-empty `Request.NodeID` and exact equality with
  `node.SelfID()` for both arm and commit; old or hand-written local socket clients can no longer
  bypass the typed self-id boundary.
- The shared offline/online peer probe now strips the canonical `nats://` scheme before dialing
  `cluster_nodes.nats_route`, so an alive NATS route listener is hard-refused the same way as raft or
  tunnel listeners.

I found no remaining ship-blocking issue in the reviewed surface after those fixes.

## Round 2 tasklist

- [x] Re-read `CLAUDE.md`, the architecture/runbook/usage updates, and the previous review style.
- [x] Census the round 2 diff, including newly added tests and unstaged code/docs.
- [x] Re-check F1 marker/epoch success boundary and the functional persistence proof.
- [x] Re-check F2 CLI-to-broker self-id propagation, prompt rendering, and handler validation.
- [x] Re-check F3 docs/runbook wording for online-preferred vs offline-floor recovery.
- [x] Re-check F4 scope reduction, mTLS transport rebuild coverage, handler-with-roster coverage, and CLI behavior coverage.
- [x] Add independent round 2 regressions for empty self-id bypass and canonical `nats://` peer probing.
- [x] Fix the two round 2 regressions directly.
- [x] Run focused regressions, full `make test`, `make lint`, and `git diff --check`.
- [x] Update this external-review report and stage all files.

## Round 2 findings

No open findings.

Fixed during review:

- `internal/broker/force_single_online.go`: `fsSelfIDMismatch` now treats empty `NodeID` as
  `CodeBadRequest`, not as a compatibility path. Regression:
  `internal/broker/force_single_round2_external_review_test.go`.
- `internal/clusteroffline/offline.go`: `probePeer` now dials the normalized NATS route address
  (`stripScheme(p.NatsRoute)`) while preserving the shared hard-refuse path used by online and offline
  force-single. Regression: `internal/clusteroffline/force_single_round2_external_review_test.go`.

## Questions / residual risk

- The future cross-node split-brain detector is still scope-reduced rather than implemented. That is
  acceptable for this pass because the docs now say so explicitly, the recovery epoch is durably
  persisted as future detector input, and online force-single remains at least as guarded as the
  existing offline floor plus a commit-time live-peer re-probe.
- The first full `make test` attempt hit two unrelated local timing/port failures
  (`internal/agent TestRebuildNoGoroutineLeak`, `internal/tunnel TestTunnelReconnectStopsOnTerminalDeny`).
  Both failed tests passed on direct rerun, and a second full `make test` passed.

## Round 2 verification

Passing:

- `env GOCACHE=/tmp/tether-gocache go test ./internal/broker -run 'TestOnlineForceSingleExternalReview|TestForceSingleHandler|TestOnlineForceSingleRound2Review' -count=1`
- `env GOCACHE=/tmp/tether-gocache go test ./internal/clusteroffline -run 'TestForceSingleRound2ReviewNatsRouteURLIsProbed|TestForceSingle|TestCheckPeers|TestReadRoster|TestRecover' -count=1`
- `env GOCACHE=/tmp/tether-gocache go test ./cmd/tether -run 'TestOnlineForceSingleExternalReview|TestOnlineForceSingleCLI' -count=1`
- `env GOCACHE=/tmp/tether-gocache go test ./internal/cluster -run 'TestRecoverToSelfOnline' -count=1`
- `env GOCACHE=/tmp/tether-gocache go test ./internal/agent -run TestRebuildNoGoroutineLeak -count=1 -v`
- `env GOCACHE=/tmp/tether-gocache go test ./internal/tunnel -run TestTunnelReconnectStopsOnTerminalDeny -count=1 -v`
- `env GOCACHE=/tmp/tether-gocache make test` (second full run)
- `make lint`
- `git diff --check`

---

## Round 1 record - Fail

Reviewer role: external reviewer. Scope: all unstaged / untracked online force-single
changes outside staging, including `cmd/tether/cluster_offline.go`,
`internal/{adminsock,broker,cluster,clusteroffline}`, the new tests, and
`docs/reviews/force-single-online-plan.md`.

Conclusion: Fail. The core `cluster.Node.RecoverToSelfOnline` mechanics are a plausible direction
and its focused unit tests pass, but the operator-facing feature is not safe to ship yet. The online
commit can report success while failing to persist `force_single_active` / epoch, the CLI typed
confirm is not validated by the broker, the runbook still says offline is the only recovery path, and
the plan's split-brain detector / production integration tests are explicitly deferred.

## Tasklist

- [x] Scope census: enumerated tracked modified files and untracked force-single-online additions outside staging.
- [x] Process/docs alignment: read `CLAUDE.md`, architecture/requirements/runbook, the online force-single plan, and prior external-review style.
- [x] Raft hot-swap review: checked atomic raft pointer conversion, `RecoverToSelfOnline`, transport/store/FSM ownership, and failure modes.
- [x] Adminsock/CLI safety review: checked arm/commit gates, dwell, peer-dead confirmation, token flow, local socket surface, dry-run, and typed confirm.
- [x] Marker/status/alert review: checked `force_single_active`, recovery epoch, cluster-health broadcast, status/destructive-gate consequences, and detector wiring.
- [x] Offline parity review: checked exported `ReadRoster` / `CheckPeersDead` and ran focused offline tests.
- [x] Independent tests: added reviewer regressions under `cmd/tether` and `internal/broker`.
- [x] Verification: ran focused passing tests, failing reviewer regressions, and `git diff --check`.
- [x] Report: this report written as `docs/reviews/force-single-online-external-review.md`.

## Findings

### F1 - Blocker: online commit can succeed without the force-single marker or epoch

Locations:
- `internal/broker/force_single_online.go:197`-`211`
- `internal/broker/cluster_health.go:43`-`47`
- Reviewer repro: `internal/broker/force_single_online_external_review_test.go`

Why this fails:

After `RecoverToSelfOnline` rewrites the raft stores to `{self}`, the handler ignores
`WaitForLeader` and both `Propose` errors:

```go
_ = b.admin.node.WaitForLeader(5 * time.Second)
_ = b.admin.node.Propose(...)
_ = b.admin.node.Propose(...)
return OK
```

`force_single_active` is the only persisted fact that makes status exit 3 and makes ctl destructive
gates see the emergency (`cluster_health.go` only reports `ForceSingleActive` by reading that
marker). Reporting success without that row means the system is writable on a single emergency broker
while users may not see the hard gate or banner. The epoch write is also silently optional, so the
planned detector has no durable input guarantee.

Expected fix direction:

Treat marker and epoch persistence as part of the operation success boundary. After the raft rewrite,
wait for leader and propose both rows with checked errors; if either fails, return a loud failed
response that tells the operator to rerun/repair before using the cluster. Add a regression that
forces marker proposal failure and asserts the commit does not return OK.

### F2 - High: `--self-id` typed confirmation is never checked by the broker

Locations:
- `cmd/tether/cluster_offline.go:112`-`146`
- `internal/broker/force_single_online.go:142` and `:180`
- Reviewer repro: `cmd/tether/force_single_online_external_review_test.go`

Why this fails:

The CLI requires `--self-id`, but uses it only for local prompt text. The arm and commit requests omit
that value, while the broker simply uses `b.admin.node.SelfID()`. If an operator on broker `brk-a`
mistypes `--self-id brk-b`, the prompt asks them to type `brk-b`, then the socket still force-singles
`brk-a`. That defeats the purpose of typed node-id confirmation: proving the operator is acting on
the intended node.

Expected fix direction:

Send the operator-confirmed self id to the broker, preferably in `Request.NodeID` or a dedicated
field, and have both arm and commit fail if it does not exactly match `node.SelfID()`. Also include
the broker self id in the arm report so the CLI can render a precise prompt before commit.

### F3 - Major: operator docs still say force-single is offline-only

Locations:
- `docs/cluster-runbook.md:271`-`305`
- `docs/usage.md:902`-`906`
- `docs/distributed-broker-architecture.md:294`-`305`
- Reviewer repro: `internal/broker/force_single_online_external_review_test.go`

Why this fails:

The feature's goal is to avoid manufacturing a second outage when the survivor broker is running, but
the runbook still says `recovery force-single` is OFFLINE and "the ONLY operable path". The procedure
still tells operators to mask and stop the daemon. `usage.md` also documents only daemon-stopped
flags, with no `--online` / `--dry-run` contract.

Expected fix direction:

Update the runbook and usage docs so the running-broker path is the preferred path when the admin
socket is reachable, and offline disk surgery remains the floor when the broker cannot start. Document
the two-step arm/commit flow, dwell, peer probe, dry-run behavior, and the exact fallback command.

### F4 - Major: the chosen split-brain detector and production integration coverage are deferred

Locations:
- `docs/reviews/force-single-online-plan.md:20`-`27`
- `docs/reviews/force-single-online-plan.md:111`-`120`
- `internal/broker/cluster_health.go:43`-`50`

Why this fails:

The plan explicitly selects an epoch-tagged split-brain detector as a mitigation for the irreducible
total-partition case, but the implementation status says it is not wired. The health responder does
not broadcast the epoch, and the observe tick does not compare peer epochs or raise a severe alert.
The same status section defers the real mTLS transport rebuild integration test, handler-with-roster
tests, and CLI tests.

Expected fix direction:

Either implement the detector now or explicitly reduce the feature scope and document why online
force-single is acceptable without it. At minimum, add gated coverage that exercises a real production
`NewMTLSTransport` rebuild and handler-level peer-alive / dry-run / commit paths with roster rows.

## Questions / concerns

- Should `--dry-run` on a healthy cluster return OK with a report or return `quorum_not_lost`? The plan says it is runnable on a healthy cluster; the current implementation treats healthy as a gate refusal.
- `ForceSingleReport` has `Alive` / `OnPort`, but `fsBuildReport` never fills them; live-peer detail is only in the error string from `CheckPeersDead`.
- `newRecoveryEpoch` and arm-token minting ignore `rand.Read` errors. That is unlikely in practice, but this is an emergency safety path and should fail closed.
- `internal/cluster/d7_membership_test.go` gained a Unicode quote in an English code comment; that is minor, but it violates the repo's ASCII/code-comment convention.

## Confirmed clean / lower-risk areas

- Mechanical scan found no bare `n.raft` dereference outside `Load` / `Store` in `internal/cluster`.
- Existing focused `RecoverToSelfOnline` tests pass, including writable-after-swap, failed transport rebuild floor, concurrent propose, nil factory, and marker plan commands.
- The admin surface remains the local Unix admin socket path; I did not find a NATS subject exposing these new force-single ops.
- Exporting `ReadRoster` / `CheckPeersDead` did not break the focused offline tests I ran.

## Verification

Passing:

- `env GOCACHE=/tmp/tether-gocache go test ./internal/cluster -run 'TestRecoverToSelfOnline' -count=1`
- `env GOCACHE=/tmp/tether-gocache go test ./internal/broker -run 'TestForceSingleArm' -count=1`
- `env GOCACHE=/tmp/tether-gocache go test ./internal/clusteroffline -run 'TestForceSingle|TestCheckPeers|TestReadRoster|TestRecover' -count=1`
- `env GOCACHE=/tmp/tether-gocache go test ./internal/adminsock -run 'TestD7|TestCluster|TestAdmin' -count=1`
- `git diff --check`

Failing reviewer regressions:

- `env GOCACHE=/tmp/tether-gocache go test ./cmd/tether -run 'TestOnlineForceSingleExternalReviewCLITransmitsConfirmedSelfID' -count=1`
- `env GOCACHE=/tmp/tether-gocache go test ./internal/broker -run 'TestOnlineForceSingleExternalReview' -count=1`

Not run:

- Full `make test`, `make e2e`, and `make lint`, because deterministic reviewer regressions already make this external review a Fail.

---

## Main-process responses (post-fix) — all findings ADOPTED

> Per the §3 workflow, the main process evaluated each finding, fixed it, and integrated the reviewer's
> regressions. All four findings + every "Questions / concerns" item are addressed. Gates re-run green:
> `make lint` 0 issues, `make test` all packages ok (p13 included this run), `-race` clean on the swap
> + handler tests. The two reviewer regressions now PASS.

### F1 (Blocker) — ADOPTED & FIXED: marker/epoch are now the success boundary

`handleForceSingleCommit` no longer ignores the post-recover writes. After `RecoverToSelfOnline`:
`WaitForLeader`, the `PlanSetForceSingle` propose, the epoch generation, and the `PlanForceSingleEpoch`
propose are EACH error-checked; any failure returns a LOUD `CodeStoreError` naming the repair
(re-run to re-arm + retry — `RecoverToSelfOnline` is idempotent on an already-`{self}` node), never a
false OK. `internal/broker/force_single_online.go:handleForceSingleCommit`.
- Reviewer regression `TestOnlineForceSingleExternalReviewMarkerErrorsAreFatal` (no `_ = …Propose`) now PASSES.
- New functional proof `TestForceSingleHandlerArmCommitPersistsMarkerAndEpoch`: a full arm→commit drives
  the in-process recover through a real `cluster.Node` and asserts `force_single_active` + the epoch are
  in `cluster_meta` after the reported success.

### F2 (High) — ADOPTED & FIXED: the typed --self-id is validated by the broker

The CLI now SENDS the operator-confirmed id (`Request.NodeID = selfID`) on BOTH arm and commit; the
broker refuses with `CodeBadRequest` when `req.NodeID != node.SelfID()` (`fsSelfIDMismatch`). The arm
report carries `BrokerSelfID`, and the CLI renders the TTY prompt from THAT value (not the local
`--self-id`), so a mistyped id can never quietly confirm the wrong node.
`cmd/tether/cluster_offline.go:runForceSingleOnline`, `internal/broker/force_single_online.go`,
`internal/adminsock/protocol.go:ForceSingleReport.BrokerSelfID`.
- Reviewer regression `TestOnlineForceSingleExternalReviewCLITransmitsConfirmedSelfID` now PASSES.
- New `TestForceSingleHandlerSelfIDMismatchRefused` (arm + commit both refuse a mismatched id).

### F3 (Major) — ADOPTED & FIXED: docs teach the online path as preferred

- `docs/cluster-runbook.md`: §3 now leads with **§3.0 ONLINE (preferred — broker keeps RUNNING)**
  (arm→commit, dwell, peer probe, `--dry-run`) and demotes the disk surgery to **§3.1 OFFLINE (the
  floor)**; the §2.3 table row + protected-mode paragraph updated; the "the ONLY operable path" / "only
  `cluster recovery force-single` (OFFLINE) can recover" phrasings are gone.
- `docs/usage.md`: added the `--online` / `--dry-run` rows + the `--self-id`-is-validated note.
- `docs/distributed-broker-architecture.md` §8.4: a post-D9 note documents the in-process mechanism +
  gates + the success-boundary marker/epoch + the detector scope-reduction.
- Reviewer regression `TestOnlineForceSingleExternalReviewRunbookDocumentsOnlinePath` now PASSES.

### F4 (Major) — ADOPTED: detector explicitly scope-reduced + the named gated coverage added

- **Detector**: scope-reduced (documented in the architecture note + the plan). The recovery epoch is
  now persisted WITH CHECKED ERRORS as the durable input a future cross-node detector needs; the
  cross-node compare + SEVERE alert is a follow-on. Rationale (documented): online-without-detector is
  ≥ the offline floor (which has NO detector), and the irreducible total-partition residual is already
  narrowed by the commit-time peer-liveness RE-PROBE.
- **Real mTLS rebuild** (the "at minimum"): `TestRecoverToSelfOnlineRealMTLSTransportRebuild` drives the
  in-process swap with an actual `NewMTLSTransport` rebuild (not inmem), via the new declarative
  `Config.TransportFactory` seam (NewProduction now wires it through Config).
- **Handler-with-roster**: `internal/broker/force_single_handler_test.go` exercises dry-run-on-healthy,
  self-id mismatch, peer-alive HARD-REFUSE, commit-without-token, and the full arm→commit through a real
  `cluster.Node` with `cluster_nodes` rows.
- **CLI behavior**: `cmd/tether/force_single_cli_test.go` (dry-run transmits NodeID+DryRun and never
  commits; a non-TTY real recover refuses at the confirm without committing).

### Questions / concerns — all ADOPTED

- **dry-run on a healthy cluster**: FIXED. A `--dry-run` arm now ALWAYS returns OK with the gate verdict
  (`WouldProceed` + `Reason`) instead of erroring `quorum_not_lost`, so it is genuinely runnable as a
  drill on a healthy cluster (`TestForceSingleHandlerDryRunOnHealthyCluster`).
- **`Alive` / `OnPort` never filled**: FIXED. `ProbePeers` (sharing the `probePeer` primitive with the
  authoritative `CheckPeersDead`) populates per-peer `Alive` / `OnPort`; the CLI renders them. Asserted
  by `TestForceSingleHandlerPeerAliveRefused`.
- **`rand.Read` ignored**: FIXED. `mint` and `newRecoveryEpoch` now FAIL CLOSED on a CSPRNG error (no
  token / no commit; no empty epoch) instead of swallowing it.
- **Unicode quote in `d7_membership_test.go`**: FIXED (ASCII `""`).
