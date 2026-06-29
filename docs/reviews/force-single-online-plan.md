# Online in-process force-single — finalized plan

> Open-ended Stage-A multi-expert design workflow (8 agents, wf_a601e038-da7) → main-process finalized.
> Fixes the design flaw: force-single (the quorum-loss escape hatch) is OFFLINE-only — it requires STOPPING
> the surviving broker to run raft.RecoverCluster, i.e. recovering from a disaster needs manufacturing ANOTHER
> outage, and it cannot be drilled on a healthy cluster.
>
> Process (per user 2026-06-28): Stage A design (this) → Stage B implement + adversarial tests + green gates
> → **skip internal review, go straight to external review (user)**.

## APPROACH (chosen)

Add an **online, operator-driven, in-process** recovery that hot-swaps **only the raft instance** inside the
running `*cluster.Node` — the process never stops, the data plane keeps serving reads. Lifecycle:
`raft.Shutdown` (old instance, frees the stores) → `raft.RecoverCluster({self})` on the LIVE stores → rebuild
the mTLS transport → `raft.NewRaft` with a **fresh FSM** → **`atomic.Pointer` Store of the new instance ONLY on
full success**. On any failure the field keeps the **old Shutdown instance** (never nil) → `Propose`'s existing
`State()!=Leader → ErrNotLeader` gate returns retriable, reads keep working, no nil-panic, no brick.

Invoked over the **local root-only admin socket** as a two-step **`arm`→`commit`** flow gated by:
1. **CLI TTY-typed-node_id confirm** (unchanged `confirmTypedNodeID(..., false, "")` — non-TTY HARD-REFUSE, `--yes`/env rejected).
2. **Sustained-quorum-loss dwell** (`T_dwell`) — refuse unless continuously leaderless ≥ T_dwell (transient-partition defense).
3. **Multi-port peer-liveness HARD-REFUSE** (the exact offline `CheckPeersDead`) — re-checked at BOTH arm and commit.
4. **Broker-minted arm token** (in-mem, 60s TTL, single-shot) — a stray/lone `commit` is refused; anti-fat-finger/anti-stray-automation interlock.
5. **`--dry-run`** arm → zero-mutation drill, runnable on a healthy cluster.
6. **Post-recover epoch-tagged split-brain DETECTOR** — for the irreducible total-partition residual: if a peer is
   ever heard advertising `force_single_active` with a DIFFERENT epoch → SEVERE alert (converts silent divergence to a loud alarm).

**Rejected:** `n.raft=nil` window (nil-panic kills the only survivor); self-restart (reintroduces the outage,
bricks non-systemd hosts like a100); reusing the post-Shutdown NetworkTransport (unproven → rebuild); reusing
`n.fsm` (violates RecoverCluster's discard-FSM contract → fresh fsm); ANY automatic trigger (a transient total
partition is indistinguishable from death → auto-fire would split-brain on heal); leader-gated/NATS-routable
(there is no leader; the most dangerous op stays local-socket-only, out of `routableClusterOps`).

## MECHANISM

`Node.RecoverToSelfOnline(selfRaftAddr)` under `applyMu` (serializes vs Propose's gate→Plan→Apply → no in-flight
Apply; liveness writes hit `n.db` directly, serialize on `MaxOpenConns(1)`, disjoint columns):
`old := n.raft.Load()` → `old.Shutdown()` → fresh `&fsm{db:n.db, ro:n.ro,...}` → `raft.RecoverCluster(rc, f,
n.store, n.store, n.snaps, throwaway-inmem, {self})` → close old transport → `transportFactory()` → `raft.NewRaft`
→ `n.fsm=f; n.transport=newTrans; n.raft.Store(r)` (commit on full success). RecoverCluster internally restores
the snapshot in-place (same inode → `n.ro`/`n.db` stay valid), replays the idempotent (`applied_index`-self-skip)
tail, writes a {self} snapshot+config, truncates the log — mechanically identical to the proven offline
`recoverClusterToSelf`, only the stores are live + the FSM is fresh.

## FILES TO ADD / MODIFY
- `internal/cluster/node.go` — `raft` → `atomic.Pointer[raft.Raft]` (all in-pkg derefs → `.Load()`); retain
  `snaps`/`dataDir`/`cfg` + add `transportFactory func() (raft.Transport, error)`; add `RecoverToSelfOnline`.
- `internal/cluster/read.go`, `membership.go` — mechanical `n.raft` → `n.raft.Load()`.
- `internal/cluster/membership_ops.go` — `PlanSetForceSingle(now)`, `PlanForceSingleEpoch(epoch)`,
  `MetaKeyForceSingleEpoch` (reuse `OpClusterMetaSet`; NO new wire op, NO proto bump).
- `internal/cluster/production.go` — `NewProduction` sets `transportFactory` (re-run `NewMTLSTransport`), `snaps`, `dataDir`, `cfg`.
- `internal/clusteroffline/offline.go` — export `CheckPeersDead`, add `ReadRoster(ro, selfID)`; internal `ForceSingle` delegates (byte-unchanged).
- `internal/adminsock/protocol.go` — `OpClusterForceSingleArm`/`Commit`; `Request.{ConfirmPeersDead,ArmToken,DryRun}`;
  `Response.ForceSingle *ForceSingleReport`; codes `CodePeerAlive`/`CodeQuorumNotLost`/`CodeForceSingleRefused`/`CodeArmExpired`. **NOT in `routableClusterOps`.**
- `internal/broker/force_single_online.go` (new) — `handleForceSingleArm`/`handleForceSingleCommit`; dwell +
  `CheckPeersDead` gates; in-mem arm-token store; `RecoverToSelfOnline` call; marker+epoch `Propose`.
- `internal/broker/clusterstatus.go` — dispatch both ops BEFORE the `!IsLeader()` gate.
- `internal/broker/observability.go` — maintain `leaderlessSince` (non-leader tick); epoch split-brain detector each tick.
- `internal/broker/cluster_health.go` — recovery `epoch` on the broadcast responder; detector comparison.
- `internal/broker/clusterwrite.go`/`broker.go` — `wireClusterLate` injects the dwell getter + arm-token store.
- `cmd/tether/cluster_offline.go` — `--online` + `--dry-run` (arm→TTY-confirm→commit via `callAdmin`); offline fallback if socket dead; default = offline verbatim.
- docs: runbook + architecture §8.4.

## KEEP-OR-REPLACE offline
**KEEP offline unchanged as the floor** (a crash-looping/won't-start broker can't serve its socket → offline disk
surgery is the last resort; `RecoverToSelfOnline`'s own post-RecoverCluster failures degrade TO a plain restart on
the `{self}` stores). Online is the preferred default-when-up. Both share `CheckPeersDead`, the `{self}`
RecoverCluster rewrite, the `force_single_active` marker → `test/d7` offline drill + `internal/cluster`/`clusteroffline` tests stay green.

## DECISIONS (R1–R5)
- **R1 transport rebind** — rebuild by default (Go `SO_REUSEADDR`; sub-second `:7400` rebind, harmless at N=1 with dead peers); validated by tests. ADOPT.
- **R2 atomic.Pointer completeness** — convert all derefs to `.Load()` + a review grep guard (no bare `n.raft.` except `.Load()/.Store()`); `-race` test backstop. ADOPT.
- **R3 discard-FSM carried state** — fresh fsm resets in-mem counters (e.g. dedupCount; externalized state is in SQLite); document the reset. ADOPT.
- **R4 T_dwell** — `max(15s, 10×ElectionTimeout)`; a normal election (~1s) never trips it; a quorum-lost node legitimately re-waits T_dwell after a restart (intentional, documented). ADOPT.
- **R5 liveness writes during swap** — NOT under applyMu; serialize on `MaxOpenConns(1)` vs RecoverCluster's replay, disjoint columns → safe (brief stall at most); assert no deadlock under load in the `-race` test. ADOPT.

## ADVERSARIAL TESTS (each → what it kills)
PartitionedPeerAlive_TCPRefuses (raft-partitioned-but-alive split-brain) · IncompleteConfirmList_Refuses
(missed-peer) · TransientQuorumLoss_DwellRefuses (fired during election) · HealthyCluster_DryRunNoMutation
(drill mutates / drillability) · RecoversWritable_InProcess (core feature + data loss across swap; no restart) ·
RODBStableAcrossRecover (inode-swap/handle invalidation) · ConcurrentProposeRace (`-race`+leak gate:
atomic-pointer race / torn Apply / raft leak) · NewRaftFailureLeavesReadOnlySurvivor (mid-way brick; floor) ·
FreshFSMForNewRaft (discard-FSM contract) · MarkerViaRaft (single-writer invariant) · CLINotAutomatable
(automation hole) · ArmCommitTokenRequired (stray-socket bypass) · ArmThenPeerRevivesBeforeCommit (arm-window
revival) · SplitBrainDetectorAlerts (silent divergence) · MidRecoveryCrash_NoCorruption (crash-corruption) ·
NotBusRoutable (remote-trigger surface) · OfflineFloorIntact (regression to floor).
Heavy multi-node ones gate `//go:build d7_integration` under `TestD7Matrix -race`; swap-touching ones under `-race` + NumGoroutine/fd leak gate.

## STAGE B — IMPLEMENTATION STATUS (for external review)

**Implemented + gates green** (`make lint` 0 issues; `make test` green — the one `test/p13` failure was a known
proxy-reconnect load flake, passes alone, untouched by this change; `-race` clean on the swap):
- `internal/cluster/node.go`: `raft` → `atomic.Pointer[raft.Raft]` (26 derefs → `.Load()`); `RecoverToSelfOnline`
  (Shutdown old → RecoverCluster on live stores → rebuild transport → NewRaft fresh-FSM → atomic Store on full
  success; old Shutdown instance kept on failure, never nil/bricked); retained `snaps`/`cfg`/`transportFactory`.
- `internal/cluster/membership_ops.go`: `PlanSetForceSingle`/`PlanForceSingleEpoch`/`MetaKeyForceSingleEpoch`.
- `internal/cluster/production.go`: `NewProduction` wires the mTLS `transportFactory`.
- `internal/clusteroffline/offline.go`: exported `CheckPeersDead` + `ReadRoster` (offline path delegates → parity).
- `internal/adminsock/protocol.go`: Arm/Commit ops + `ConfirmPeersDead`/`ArmToken`/`ForceSingleReport` + codes;
  routed to the backend (local Unix socket only — no NATS path dispatches admin ops).
- `internal/broker/force_single_online.go`: `handleForceSingleArm`/`handleForceSingleCommit` + `forceSingleArm`
  (sustained-quorum-loss dwell + single-shot TTL arm token); peer-liveness HARD-REFUSE re-checked at commit;
  RecoverToSelfOnline + marker+epoch Propose. Dispatched before the leader gate; dwell fed by the (non-leader-gated)
  observe tick.
- `cmd/tether/cluster_offline.go`: `--online` + `--dry-run` (arm → unchanged TTY-typed confirm → commit; offline
  floor printed if the socket is unreachable; default = offline verbatim).
- Tests: cluster `RecoverToSelfOnline` (recovers-writable in-process; failure-leaves-read-only + restart-floor;
  concurrent-Propose `-race`; nil-factory refuse; marker-via-raft) + broker `forceSingleArm` (dwell, token).

**External-review round-1 = Fail (F1–F4) → ALL ADOPTED + FIXED** (see
`force-single-online-external-review.md` → "Main-process responses"). Post-fix the two reviewer
regressions PASS; gates re-run green (`make lint` 0, `make test` all-ok incl. p13, `-race` clean):
- **F1** (marker/epoch were best-effort): now the COMMIT SUCCESS BOUNDARY — WaitForLeader + both
  Proposes error-checked; any failure returns a loud `CodeStoreError` naming the repair. Functional
  proof `TestForceSingleHandlerArmCommitPersistsMarkerAndEpoch`.
- **F2** (typed `--self-id` not validated): CLI sends `Request.NodeID`; broker refuses a mismatch
  (`fsSelfIDMismatch`); report carries `BrokerSelfID` and the CLI prompts from it.
- **F3** (docs said offline-only): runbook §3.0 ONLINE (preferred) / §3.1 OFFLINE (floor); usage
  `--online`/`--dry-run`; architecture §8.4 post-D9 note.
- **F4** (detector + integration coverage): detector explicitly **scope-reduced** (epoch persisted as
  durable input; cross-node compare is the follow-on; rationale = online ≥ offline floor, residual
  narrowed by commit-time peer re-probe). Added the named gated coverage: real `NewMTLSTransport`
  rebuild (`TestRecoverToSelfOnlineRealMTLSTransportRebuild` via the new declarative
  `Config.TransportFactory`), handler-with-roster (`force_single_handler_test.go`), CLI behavior
  (`force_single_cli_test.go`).
- **Concerns** all fixed: dry-run-on-healthy returns OK+verdict (not an error); `Alive`/`OnPort` filled
  via `ProbePeers`; `mint`/`newRecoveryEpoch` fail closed on a CSPRNG error; ASCII comment fix.

**Still deferred (follow-on, documented; ≥ offline-floor safety):**
- **Epoch split-brain DETECTOR cross-node comparison + SEVERE alert** — the epoch is persisted; the
  broadcast-and-compare is the follow-on (offline floor has no detector either).
- **Multi-broker kill-peer end-to-end drill** (2-broker `test/d6`/`d7`-style): the in-process swap is
  covered at the node level with a real mTLS rebuild + at the handler level with roster rows, but a
  full multi-broker online-recover-after-real-partition e2e is a later matrix addition.
