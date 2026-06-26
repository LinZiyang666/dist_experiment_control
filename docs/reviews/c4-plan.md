# C4 Plan (FINAL) — Cluster Operation Controller (recoverable join/retire)

> Stage-A output. 11-agent adversarial workflow (5 lens drafters → 5 critics → 1 synth; full raw `tasks/w2j0wrkrp.output`). Main process sole finalizer. Synth verified the tree: `cluster_reconcile.go`/`topoLaggards` do NOT exist (the real file is `topology_reconcile.go`, predicate inlined at `clusterstatus.go:404`); `clusterNodeUpsertApplier` asserts `len(cmd.Body)==1`; `genericExecApplier` constraint failure → `fsm.go:142` retry→**panic** (poison-skip is custom-applier-only); `DrainNode` transfers-leader-first then bails; `issuedNonces` is a leader-local map; D9 probes `clusterCaughtUp`/`clusterStreamsReady` already wired.

## 0. Finalization decisions (binding)

| # | Decision | Rationale |
|---|---|---|
| **SSOT split** | **substrate** (`cluster_nodes.phase` + raft config + `topology_generation` + `broker_draining`) = the membership SSOT (unchanged; all D7 gates + `cluster status` read it). **`cluster_operations`** = the SSOT for the named workflow cursor + the operator-confirm bit + captured barrier/deadline + `last_error` + the durable timeline. The two NEVER overlap as authorities. | Avoids a two-SSOT divergence. Membership-window display states (PENDING↔ROSTER_COMMITTED, CATCHING_UP, DRAINING↔DRAIN_REQUESTED) are PROJECTED live from substrate in `ops show`, never stored as a competing copy. |
| **advance-after-observe** | The controller advances the op cursor ONLY after the substrate fact is committed → the op row is a monotone LOWER BOUND on real progress. On resume the cursor is re-derived from the substrate (substrate wins); a one-step kill-9 lag self-heals. | The only model robust across every kill-9 window + feasible at the PoP-gated admit step. |
| **separate Command** | The op row is a SEPARATE raft `Command`, NOT bundled into the membership `Command`. | `clusterNodeUpsertApplier` asserts `len(Body)==1` (can't carry an op stmt); `PlanClusterNodePhase`'s gen-bumps are `changes()`-gated on the prior stmt's RowsAffected (splicing corrupts roster/topology gen). Consistency = advance-after-observe + resume re-derive, not atomic bundling. |
| **no CHECK / no partial-UNIQUE** | migration 0015 has NO sql `CHECK(kind/op_state)` and NO partial-UNIQUE. Enum validity validated in Go (bake surface); single-active enforced by a baked `INSERT … SELECT … WHERE NOT EXISTS(active op for target) AND NOT EXISTS(op_id)` → RowsAffected guard. | These ops ride `genericExecApplier`; an Apply-time constraint failure is a plain error → `fsm.go:142` retry→**panic on every replica = cluster brick** (poison-skip is custom-applier-only). |
| **prepare joiner-local** | `cluster join prepare` runs on the NEW machine (not yet a member, cannot call the leader): derive identity → local preflight → SELF-MINT a nonce → `SignWithSeed(JoinSignBytes(node_id, ident_pub, nonce))` → emit one base64 BUNDLE. The replicated ladder begins at `approve`. | This is the ONLY way `one prepare + one approve` is real (success metric #1). `PREPARED`/`PREFLIGHT_OK` live in the bundle (no raft write); `approve` backfills their timeline entries. |
| **joiner-minted nonce** | The signed message bytes are UNCHANGED (`domain‖node_id‖ident_pub‖nonce`) so the cross-replica re-verify is byte-identical. Per §18.2.4 the nonce is a CONSISTENCY property (not a security boundary); `approve` requires admin-socket access (the trust boundary). Remove the leader-local `issuedNonces` map. Replay-after-retire deduped by a consumed-nonce content check against retained terminal op rows `(target_node, join_nonce)`, bounded by the `reqIDRetentionWindow` idiom. | Update the `JoinSignBytes` comment in lockstep. REJECTED (over-engineering): a replicated consumed-nonce ledger beyond op rows; binding cluster-id into the signed message (forces the joiner to know the cluster-id at offline prepare). |
| **topoConvergedForOp** | A NEW broker helper over the existing `StatusReport`/`pollClusterHealth(SubjClusterCursor)` scatter-gather. Converged iff FOR EVERY current raft voter (retire: every REMAINING voter) the voter is `reached` AND `TopoReported` AND `TopoObserved >= op.topo_target_gen`. An unreachable/UNKNOWN voter counts as NOT converged (fail-closed; the op stalls loud, never false-greens SERVING/RETIRED). | Deliberately DIFFERS from `computeHealth`'s inlined predicate (`clusterstatus.go:404`), which excludes unreachable voters via `reached` and would false-green. `op.topo_target_gen` = the `topology_generation` this op's membership change bumped. |

## 1. What C4 closes (建议 1+2 acceptance, verbatim)

> adding a broker = ONE `prepare` + ONE `approve`; retire is a RECOVERABLE operation (not a one-shot command block); ALL safety boundaries preserved. Closes success metric #1 "one prepare + one approve".

Gap rows: `cluster plan add` ❌→✅; `cluster apply <plan-id> --wait` 🟡→✅; `cluster ops show` real log 🟡→✅; `cluster join prepare`/`approve` ❌→✅; join machine PREPARED→SERVING 🟡→✅; `cluster retire --wait` op-ize 🟡→✅; retire machine DRAIN_REQUESTED→RETIRED 🟡→✅.

## 2. Migration `0015_cluster_operations.sql` (additive, FSM-owned)

`cluster_operations(op_id PK [leader-baked sha256(node_id|epoch)], kind, target_node, op_state, confirmed, confirmed_ft, barrier, catchup_deadline, topo_target_gen, join_nonce, params JSON [no secrets], last_error, timeline JSON [capped in-row], terminal, created_at, updated_at)` + non-unique indexes `(target_node, terminal)` + `(terminal)`. No `CURRENT_TIMESTAMP` (leader-baked literals, §3.4). Non-cluster broker never reads/writes it.

## 3. Join state machine (state → existing function)

`PREPARED`/`PREFLIGHT_OK` (in-bundle, joiner-local) → **`JOIN_PROOF_VERIFIED`** (leader pre-verify PoP from bundle + charset + version-skew + replay-dedup) → **`ROSTER_COMMITTED`** (`PlanClusterNodeUpsert` VERBATIM — PoP re-verified on EVERY replica, unchanged) → **`RAFT_ADDING`** (capture barrier under VerifyLeaderRead → persist in op → `AddVoter(node_id, cluster_nodes.raft_addr)` — raft_addr from SUBSTRATE) → **`CATCHING_UP`** (`setPhase→CATCHING_UP`; poll `caughtUp(op.barrier)` against the PERSISTED barrier; past `op.catchup_deadline` → bounded retry → BLOCKED+`last_error`) → **`NATS_ROLLED_OUT`** (`topoConvergedForOp(joining=true)`) → **`SERVING`** (clear `force_single_active` if NumVoters>1; terminal). `AddNode` internals decomposed into idempotent step methods (one-per-tick + resume primitives). VOTER_ADD_FAILED → bounded-retry re-issue `AddVoter`, else BLOCKED+hint (no infinite raft-config spam).

## 4. Retire state machine (gate→state preservation)

`DrainNode`'s gates extracted into shared helpers (`preflightRetireGates`, `streamsReadyGate`, `rebuildOffEnumerate`) called by BOTH legacy `DrainNode` (kept for non-retire `drain`, NO logic change) and the controller. Op created+committed at **DRAIN_REQUESTED while target is still leader** (so LEADER_TRANSFERRED is a late step + a crash hands off cleanly). Sub-orderings preserved: marker-before-migrate, all gates strictly before `RemoveServer`.

| op_state | side-effect | gate (re-run every drive) |
|---|---|---|
| `DRAIN_REQUESTED` | create op | **F==0 typed-confirm** (`ErrQuorumConfirmRequired`, never `--yes`) + **refuse-last-voter** |
| `NO_NEW_HOME` | raise `broker_draining` (`PlanClusterDrainSet`) | ordering precondition (monotone UPSERT idempotent) |
| `REHOME_EXPOSES` | `migrateExposes` → `setPhase VOTER→DRAINING` | **rebuild-OFF enumeration refusal** + `ErrNoMigrationTarget` (re-enumerated each drive) |
| `STREAMS_AT_TARGET` | `streamsReady()` (fail-closed) | **replica-target gate** (re-probed each drive AND again before RemoveServer) |
| `SEED_WITHDRAWN` | **advisory event only** (demoted — C2 seeds are free-form, not roster-keyed) | non-blocking |
| `LEADER_TRANSFERRED` | if target==leader → `transferLeadershipOff`; new leader resumes | **transfer-leader-first** re-asserted |
| `RAFT_REMOVED` | re-assert streams ∧ ProjectQuorum; `setPhase RETIRING`; `RemoveServer`; `PlanClusterNodeRemove` | **final re-check before the irreversible removal** |
| `NATS_ROLLED_OUT` | `topoConvergedForOp(joining=false)` over REMAINING voters | C3 convergence fail-closed |
| `RETIRED` | clear marker; terminal | resume special-case: RAFT_REMOVED ∧ target absent → commit terminal RETIRED |

**F==0 confirm across resume:** `confirmed`+`confirmed_ft` replicated (unattended resume not re-prompted), BUT every drive re-runs `ProjectQuorum(NumVoters_now, retire=true)` + refuse-last-voter from CURRENT ground truth. Worsened-to-last-voter → hard terminal `RETIRE_FAILED` (loud). Degraded to F==0 strictly worse than `confirmed_ft` → BLOCKED awaiting `cluster ops confirm <op-id>` (new `OpClusterOpConfirm`) — neither auto-proceeds nor wedges.

## 5. CLI (two front-ends → one op record)

- **Ergonomic:** `cluster join prepare` (joiner-local bundle) + `cluster join approve <bundle> --wait` (leader) = one+one.
- **Auditable:** `cluster plan add b4 <host> …` → DRAFT op (`PLAN_DRAFTED`, config in `params`, no membership change), prints plan-id (=op_id); `cluster apply <plan-id> --bundle|--token --wait` drives it (re-checks gates). `cluster apply -f roster.yaml` stays plan-only (dispatch on arg-vs-flag).
- **Retire:** `cluster retire <node> --wait` (F==0 typed-confirm round-trip, never `--yes`). `cluster drain --retire` → deprecated alias creating the same op. Plain `cluster drain` (reversible) stays one-shot but active-op-guarded.
- **`cluster ops`:** `ls [--json] [--active]` + `show <op-id|node> [--json]` read the REAL `cluster_operations` (RODB, leader-agnostic) + durable timeline; `opFromPhase` fallback for legacy rows. **`cluster ops abort <op-id>`** (predecessor-CAS → ABORTED, frees the active slot WITHOUT touching substrate) = the stuck-op escape hatch. **`cluster ops confirm <op-id>`** = mid-flight re-confirm.
- **`--wait`:** reuse `waitForConverge` repredicated on the op row — terminal SERVING/RETIRED = exit 0; RETIRE_FAILED/ABORTED/last_error = loud non-zero; BLOCKED/timeout = exit 75 (never a silent hang).
- **adminsock (additive/omitempty):** verbs `OpClusterJoinApprove/PlanAdd/Apply/Retire/OpConfirm/OpAbort`; `OpClusterOps` read leader-agnostic + `OpsOpID`; `ClusterOpEntry` += `OpID/TargetNode/OpState/Terminal/CreatedAt/Timeline[]` — **keep v1 `State` mapped to its documented vocab** (`in_progress|done|failed|stalled|draining|retiring`), rich state in new `OpState`, bump `cluster_ops` schema_version 1→2.

## 6. Safety + recoverability

kill-9/leadership-change → advance-after-observe + per-step substrate-read-then-skip → new leader re-derives + drives forward (no double AddVoter/RemoveServer/rehome; persisted barrier/deadline = fixed goalpost). retire-the-leader → op at DRAIN_REQUESTED before any transfer → resumes to RETIRED zero re-issue. gate-fires-mid-op → shared helpers re-invoked every drive → block/fail loud before RemoveServer. concurrent ops → `WHERE NOT EXISTS(active for target)` no-op; **raw `AddNode`/`DrainNode`/`RemoveNode`/plain `drain` REFUSE when an active op exists** (no two-writer hole); legacy add/drain re-backed as create+drive shims. no-silent-stall → `last_error` + non-terminal/BLOCKED + resume hint + `--wait` exit 75; `cluster ops abort` escape. replay/forgery → admission stays the separate PoP-verified `OpClusterNodeUpsert` (cross-replica re-verify, poison-skip); the op row carries NO PoP + never gates membership.

## 7. Determinism / byte-equiv

All op SQL all-literal leader-baked (`LitText`/`LitTime(UTC)`/`LitInt`), `genericExecApplier`, no Apply-side reads/clocks; op_id/barrier/deadline/timeline/seq are leader-read-then-baked literals (epoch-CAS idiom, DIFF-1). Migration additive; non-cluster broker constructs no controller + never touches `cluster_operations`; new wire fields omitempty. Guard test (à la `TestD9ProductionWiresNoCluster`) asserts single-mode builds no controller.

## 8. File-level change list

**New:** `internal/storage/migrations/0015_cluster_operations.sql`; `internal/cluster/operation_ops.go` (`PlanClusterOpStart/Transition/Confirm`, `ValidOpState`/`ValidOpKind`, capped-timeline helper); `internal/cluster/operation_read.go` (`NonTerminalOperations`, `ActiveOperationForTarget`, `OperationByID`, `RecentOperations`, `NonceConsumed` — pure RODB L-2); `internal/broker/cluster_operation_controller.go` (`driveInFlightOperations`/`driveOne`/`nextJoinTransition`/`nextRetireTransition`/`readSubstrate`/`topoConvergedForOp`, per-op mutex, deterministic op_id); `cmd/tether/{cluster_join,cluster_plan,cluster_retire}.go`; `test/c4/` gated (`//go:build c4_integration`).
**Edited:** `command.go` (knownOps + op consts), `clustermeta.go` (defaultAppliers), `membership_ops.go` (JoinSignBytes comment only); `clusteradmin.go` (decompose `AddNode` into idempotent steps; remove `issuedNonces`; `ReconcileMembershipOnLeadership` fallback; active-op guard); `clusterdrain.go` (extract gate helpers + idempotent steps; `DrainNode` via them, no logic change); `clusterops.go` (real log + `opFromPhase` fallback + `deriveDisplayState`); `observability.go` (fold `driveInFlightOperations` into leader-edge + tick + kick); `clusterstatus.go` (dispatch new verbs); `adminsock/protocol.go` (verbs + fields + schema bump); `cmd/tether/{cluster_apply,cluster_ops,cluster_wait,cluster}.go`; `test/e2e/all_phases_test.go` (TestC4Matrix serial); runbook + usage.

## 9. Adversarial tests (named)

`TestC4JoinResumeEachState` (kill-9 at each join state → resume drives to SERVING, no double AddVoter), `TestC4RetireResumeEachState` (incl. retire-the-leader hand-off), `TestC4RetireGatesReRunOnResume` (F worsens / stream regresses mid-op → RETIRE_FAILED/BLOCKED before RemoveServer), `TestC4SingleActiveOpGuard` (2nd start no-op + raw mutators refuse), `TestC4TopoConvergedFailClosed` (unreachable voter ≠ converged), `TestC4OpFSMDeterminism` (multi-FSM byte-identical), `TestC4OpsAbortFreesSlot`, `TestC4NonceReplayAfterRetire`, `TestC4OpsLsSchemaStable` (v1 State vocab preserved), guard `TestC4ProductionWiresNoController`.

## 10. Risks / open questions

The nonce-model shift (leader→joiner minted) is an external-review decision item (signed bytes unchanged; security-pragmatic per §18.2.4). The `RETIRED` terminal lives only in the op row (no substrate) — the resume special-case + retention window are load-bearing. Decomposing `AddNode`/`DrainNode` without weakening a gate is the highest-risk refactor — the legacy paths stay byte-equivalent via the shared helpers, guard-tested. `test/c4` is gated + serial (the clustered-JS flake constraint).
