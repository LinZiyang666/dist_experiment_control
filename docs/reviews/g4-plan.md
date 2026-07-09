# G4 — Grow Orchestration `tether cluster add` — Implementation Plan (FINAL)

> Status: **FINAL** (main-process finalized; supersedes the workflow candidate). G4 is the
> **largest and last** leaf of the HA grow/force-single/deploy remediation roadmap
> (`docs/reviews/ha-grow-ops-remediation-roadmap.md §G4`); siblings G1/G2/G3/G5/G6/G7 are shipped.
> Source-of-truth for scope: gotchas `#3/#4/#5/#7/#8` + folds `#10/#23` + §B "cluster add" ultimate goal
> (`docs/v0.4.5-ha-grow-ops-gotchas.md`). Depends on **G1** (nats.d/ tether-owned, Restart=always broker,
> route-SAN) — satisfied.
>
> This plan was drafted by an adversarial multi-expert workflow (6 lenses → 3 critics → synthesis) and then
> **finalized by the main process**: the finalizer verified every load-bearing code fact against the tree,
> resolved a real architecture-boundary conflict the candidate glossed (systemctl orchestration), and turned
> the candidate's 7 open questions into decisions (§3). Traceability of finalizer divergences is in §12.

---

## 1. One-line goal

Collapse the ~15-step manual grow sequence (leader resnapshot-if-needed → joiner `init` → render mesh →
reset former-N1 JS → `join` → catch-up → `AddVoter` → publish seed → rebalance proxy) — today a hand-run
pile of `reconcile nats --manual`, `mv jetstream`, pty-fed confirms, `systemctl restart`, and non-blocking
approve retries — into **one idempotent, resumable, crash-recoverable `tether cluster add`** that performs
each step **as real tether code**, so `test/simcluster/simcluster cmd_grow` can invoke it with **no
compensating bash**.

## 2. Chosen architecture

**G4 is an EXTERNAL ctl-side drive-orchestrator that COMPOSES the already-shipped leader-driven
`OpKindJoin`/`driveJoin` ladder for every raft mutation. It NEVER introduces a new `OpKind` and NEVER calls
`AddNonvoter`/`AddVoter`/`RemoveServer` itself.** This is unanimous across all six lenses and both
architecture critics, and it is *why R3 is inherited rather than re-proven*: `driveJoin`
(`internal/broker/cluster_operation_controller.go:440-573`) already ships
`AddNonvoter` → persisted catch-up barrier → `AddVoter` → `NATS_ROLLED_OUT` → `SERVING` with R1 `opStillLive`
TOCTOU guards, predecessor-CAS phase gates, and idempotent `!inRaft`/`!isVoter` guards. Rebuilding it would
re-prove R3 from scratch; composing it inherits R3 for free. The orchestrator, being external to all
brokers, survives every restart the grow itself causes (leader/former-N1/joiner all bounce) — the exact
resumability property `driveUpgrade` (G5) established.

The work splits by **reachability**:

- **LOCAL, offline-disk half** (runs on the joiner host): `cluster init --from-existing` writes root-owned
  `raft/` with the daemon DOWN (`cutover.go:60` — "the daemon never auto-bootstraps a cluster onto a live
  DB"); `join prepare` reads joiner-local secrets (`cluster_join.go:93-107`); the joiner's own `nats.conf`
  is rendered. These cannot be reached over NATS (the joiner is not yet a member and its broker is down).
- **REMOTE, over-NATS half** (driven from the joiner via ONE new **account-seed-signed** trigger, mirroring
  G5): acquire/release the grow lock, non-blocking `approve-join` + op-status poll, the **former-N1
  clustered-cutover + JS-reset**, seed convergence assertion, and proxy rebalance.

Durable envelope state is a replicated `cluster_meta` key **`cluster_grow_active`** (value = joiner id) via
`PlanSetGrowActive`/`PlanClearGrowActive`, riding the existing `OpClusterMetaSet`/`OpClusterMetaClear` raft
ops (no new op-log command — a mixed-version fleet cannot decode-poison it), exactly mirroring
`cluster_upgrade_active` (G5) and `force_single_active` (G2).

### 2.1 The systemctl-boundary resolution (finalizer's key correction to the candidate)

tether has a **hard architecture boundary: it does NOT orchestrate systemctl** (`cluster.go:782-784`
"tether does NOT orchestrate systemctl; d9-plan OQ-3" — `cluster init` HALT-and-prints `systemctl restart
nats-server` / `systemctl start tether-broker` as **operator** steps). The workflow candidate had `cluster
add` locally `systemctl start` the joiner's daemons; that **violates the boundary**. Finalized resolution:

1. **`cluster add` NEVER calls systemctl.** On the joiner it is privileged only for **offline-disk**
   operations (init writes `raft/`, render writes `nats.conf`, JS-store move-aside is an `os.Rename`).
2. **The joiner's daemon lifecycle is provisioning/operator's job** (mandate ③: provisioning is the sim's
   job, not tether's). The joiner is provisioned "cluster-ready but not yet serving"; init runs with the
   broker down; the operator/provisioning then `systemctl start`s nats + broker. `cluster add` **brackets**
   this: at each provisioning boundary it HALTs with the exact command + "re-run to resume." Because every
   phase is a re-observable postcondition (§4), a single re-run seamlessly resumes — this unifies the
   candidate's resumable-phase model with the operator-bracket model (cli-ux lens) and the sim mandate.
3. **The ONE lifecycle restart tether MUST own — the former-N1's standalone→clustered nats bounce — uses
   same-uid `SIGKILL` + systemd `Restart=always` revive, NOT systemctl.** Both `nats-server` and
   `tether-broker` run `User=tether` (`install.sh:715/732`), so the broker can `SIGKILL` its co-located
   nats with no privilege escalation; systemd revives it with the already-rendered clustered conf. This is
   why **`nats-server.service` must flip `Restart=on-failure`→`Restart=always`** (`install.sh:717`;
   `tether-broker.service` is already `Restart=always` at :748 for exactly this #23 reason). SIGKILL alone
   already revives under `on-failure` (non-zero exit), but `always` makes the deliberate restart
   deterministic and closes the #23 clean-exit stranding. **This makes G4 a deploy-tier batch** (a
   simcluster drill re-run is a hard gate).

## 3. Finalizer decisions (the candidate's open questions, decided)

| # | Question | **Decision** |
|---|---|---|
| Q1 | Run location | **On the joiner host**, privileged for offline-disk ops only; NEVER calls systemctl; brackets the operator/provisioning daemon lifecycle; drives the remote half via account-signed triggers. (§2.1) |
| Q2 | `Restart=always` flip | **YES** — flip only `nats-server.service` (`install.sh:717`). Completes #23 hardening; makes the former-N1 cutover restart deterministic; required for the tether-owned SIGKILL-revive mechanism. |
| Q3 | #4 data preservation | **Move-aside, never delete** (`jetstream.grow-bak.<growEpoch>`, rollback-able). If the former-N1 store is **non-empty (data-bearing)**, the reset is **LOUD + operator-gated**: HALT unless `--reset-former-js` OR `--preserve-js-data` is passed. **v1 shipped semantics (external review m1):** BOTH flags merely ACKNOWLEDGE the move-aside (the store is renamed to `jetstream.grow-bak.<epoch>`, never deleted); auto `nats stream backup`→restore is NOT implemented — the moved-aside dir is the operator's manual restore source. Never silently wipes data (the live racknerd N=1 carries real events/history/OBJ_xfer). |
| Q4 | BLOCKED auto-confirm | **NO auto-confirm by default.** #7's adaptive deadline makes a BLOCKED op a *real* stall worth operator attention → surface it (exit transient) with the `cluster ops confirm` command. Optional bounded `--auto-confirm-catchup N` (default **0**) for unattended runs. |
| Q5 | #7 tunables | **Hardcoded constants for v1** (`opCatchupMaxWindow≈30m`, `opCatchupStallWindow≈90s`, `opCatchupBytesPerSec` floor). `broker.yaml`-overridable deferred (security-pragmatic: no config surface until the WAN path needs it). |
| Q6 | Legacy quarantine | **REVISED — do NOT delete.** Implementation-time ground truth (see §12.7): `AddNode` is NOT dead code — it is the **test-harness synchronous-add primitive** that ~10 suites depend on to bootstrap cluster members (`clusterwrite.go:103` "kept so the d9_integration harness can drive AddNode directly"), and `OpClusterAdd` is the **CLI-unreachable proto-version-skew REJECT path** (a safety feature; `cluster_c8_hints_test.go:13` "intentionally excluded"). The `add` verb is already reclaimed (C8 deleted the CLI verb; `newClusterAddCmd` is a fresh command, no alias collision). Deleting would break a large test surface for zero product-safety gain. Keep both. |
| Q7 | Grow-marker granularity | **Strict cluster-wide serialize** (`cluster_grow_active`, joiner-id-valued) + **symmetric grow↔upgrade fence**. The roadmap needs no concurrent grows; concurrent grows to F-fragile topologies dip fault tolerance. **Self-op carve-outs are mandatory**: do NOT add `growActive` to the `driveInFlightOperations` freeze and do NOT gate same-target `StartJoinOperation` (either would deadlock the grow's own join op). |

## 4. Ordered grow sequence (phased state machine + resume behavior)

A resumable, HALT-on-refusal ctl-side sequencer. Each phase checks its own postcondition and skips if
satisfied, so a crash/re-run of `cluster add` converges by re-observing state; it never double-applies a
raft change and never de-clusters (**HALT ≠ rollback** — a HALT leaves the joiner at-most a nonvoter,
online-`RemoveServer`-able, or a full voter; never an in-between that forks quorum).

| Phase | Action | Reachability | Resume / idempotency |
|---|---|---|---|
| **P0 PREFLIGHT** | Resolve writable leader (`currentLeader`). `RouteCertSANMatches` on the joiner route-cert (fail-fast #24 with an actionable error, not a mystery catch-up timeout). Verify secrets-dir has cluster-ca/route-cert/route-key. Refuse if `upgrade_active`, if `grow_active` names a **different** joiner, or if a non-terminal op targets a **different** node. | remote reads + local files | Pure reads; always safe. |
| **P1 ACQUIRE LOCK** | Signed `acquire-lock` → leader `Propose(PlanSetGrowActive(joiner))`, **before** the join op is created (closes the empty-`NonTerminalOperations` window). | remote signed | Idempotent UPSERT; no-op if already ours; refuses a different joiner. |
| **P2 LOCAL OFFLINE INIT** | If the joiner broker is running → **HALT** "stop tether-broker on the joiner, then re-run" (init needs the DB quiescent). `cluster init --from-existing --confirm-node-id <id>` (machine-escape #5, no pty) — now **APPLIES** the `broker.yaml` cluster seam (it currently only prints it, cluster.go:814). If the joiner carries a standalone JS store, `os.Rename` it aside locally (it will boot clustered). | local (offline disk) | Skip if `raft/` present with matching self_id AND seam present. |
| **P3 RENDER + PROVISION-START JOINER** | Render the joiner's own `nats.conf` clustered with all peers via **secrets-dir fallback (#3)** + `MonitorListen`. Then **HALT** "on the joiner: `systemctl start nats-server && systemctl start tether-broker`, then re-run" — the cold daemon start is provisioning's job (§2.1). (The sim/operator performs it; `cluster add` does not.) | local render; provisioning start | Skip if joiner nats clustered+up AND broker raft node up. |
| **P4 APPROVE-JOIN (non-blocking, #8)** | Local `join prepare` → PoP bundle. Signed `approve-join` → leader routes to the **existing** `OpClusterJoinApprove` admin op → creates `OpKindJoin` → `driveJoin`: `ROSTER_COMMITTED` (persist barrier+`catchup_deadline`) → `RAFT_ADDING`/`AddNonvoter` commits (former-N1 now in a committed ≥2-server config) → `CATCHING_UP` (bumps `topology_generation`). Returns op_id immediately; orchestrator polls (NO blocking `--wait`). | remote signed | If an op exists for the joiner: do NOT re-prepare (fresh nonce → different op_id → refused); attach + resume polling. |
| **P5 FORMER-N1 CUTOVER** (only when prior voter count == 1) | Signed `mesh-cutover` → former-N1's new `cluster_grow_cutover`: render clustered (secrets-dir fallback + `MonitorListen`), **move-aside** JS store to `jetstream.grow-bak.<growEpoch>` (gated: `IsStandaloneJetStream()` AND self in committed ≥2-server raft config AND per-epoch sentinel absent AND — if non-empty — operator ack per Q3), **SIGKILL nats → Restart=always revive clustered**. Returns only after `/varz` confirms clustered + JS-meta serving; else **BLOCKS loudly** with a `reset-failed` hint (never silently strands the data plane). For prior voters ≥2 this is a **no-op** (existing voters SIGHUP-reload the new route autonomously; only the fresh joiner boots clustered). | remote signed | Skip if former-N1 already `IsClusteredJetStream()` / sentinel present. Crash after move/before restart → restart-only (no second move). |
| **P6 CATCH-UP + PROMOTE (#7)** | Mesh up → leader probes joiner cursor (`SubjClusterCursor`). `driveJoin CATCHING_UP` BLOCKS on the size-scaled `catchup_deadline` cap. **(v1 ships ONLY the size-scaled cap; the leader-local `AppliedIndex` progress-map + ~90s stall-window described here is a §12.10 follow-up — NOT shipped.)** Caught-up → `AddVoter` → `VOTER` → `NATS_ROLLED_OUT` → `SERVING`. Orchestrator's own poll timeout is likewise adaptive (sized from DB bytes via cluster-health) so #8's poll never false-fails a slow-but-healthy joiner. On BLOCKED → surface (Q4: no auto-confirm by default). | leader-driven + remote poll | Read op state; converge. |
| **P7 SEED CONVERGE** | `driveJoin.toServing` now calls `deriveAndConvergeSeedsFromRoster` (**NEW hook** — verified the async path does NOT converge seeds today: `cluster_operation_controller.go:572` transitions without it; only the dead sync `AddNode:271` + the leadership-edge backstop:356 do). Orchestrator asserts joiner ∈ seeds. | leader | Observe `seed_generation`; monotonic + advisory. |
| **P8 REBALANCE** | Signed `rebalance-proxy` → existing `OpClusterRebalanceProxy`. | remote signed | Converges to even; idempotent. |
| **P9 RELEASE LOCK** | On CLEAN completion: signed `release-lock` → `Propose(PlanClearGrowActive)`. Crash-post-SERVING: op is terminal, so derive "already done" from the marker's joiner-id + roster showing VOTER, then release. A HALT/crash before release leaves the marker held → a re-run resumes. | remote signed | Self-heals like the upgrade lock. |

## 5. File-by-file change list (grouped by subsystem)

### Wire / proto / subjects / ACL (all additive, ProtoVersion stays 2)
- **`internal/proto/cluster_grow.go` (NEW)** — mirror `cluster_upgrade.go`. `ClusterGrowReq{Op, TargetNode,
  JoinBundle?, OpID?, JoinerNode?, GrowEpoch?, PreserveData?, ResetAck?, IssuedAt, Sig}`,
  `ClusterGrowResp{OK?, Code?, Error?, OpID?, OpState?, Terminal?, LastError?, AlreadyDone?, BackupPath?}`
  (all `json:",omitempty"` except required), `ClusterGrowSchemaVersion=1`, and `CanonicalGrowReqBytes(r)`
  with a **domain-separation prefix `"tether-cluster-grow-v2\n"`** (excludes `Sig`, deterministic field
  order) so a grow signature can never verify as an upgrade/join signature.
- **`internal/proto/subjects.go`** — `SubjCtrlClusterGrow(actor)` =
  `<prefix>.ctrl.by.<actor>.cluster-grow.req` + `SubjCtrlClusterGrowWildcard` (**hyphen-leaf**
  `cluster-grow.`, NOT `.cluster.`, to keep the §13.8 member-denied `cluster.*` negative test green;
  derived from `SubjectPrefix`).
- **`internal/proto/alerts.go`** — bump `ClusterHealthSchemaVersion` (currently 3) → 4; add
  `GrowLockActive bool json:",omitempty"` to `ClusterHealthResp` (older brokers omit → false; lets the
  orchestrator self-heal a stale lock, mirroring G5's `UpgradeLockActive`).
- **`internal/auth/permissions.go`** — ONE line: grant `PermissionsForActivatedMember` Pub
  `<prefix>.ctrl.by.<actor>.cluster-grow.req` (next to the cluster-upgrade grant). Broker Sub
  `ctrl.by.*.>` + `_INBOX.>` reply already cover it. The Pub ACL is defence-in-depth; the **gate** is the
  broker verifying the account signature (operator root authority) + `TargetNode==self` + replay skew.

### Cluster (marker + adaptive catch-up)
- **`internal/cluster/membership_ops.go`** — `MetaKeyGrowActive="cluster_grow_active"` +
  `PlanSetGrowActive(joinerID)` / `PlanClearGrowActive()` (over existing `OpClusterMetaSet`/`Clear` — no new
  raft op) + `growActive(db)` reader returning the stored joiner id.

### Broker (trigger + cutover + driveJoin fixes + mutual-exclusion + legacy quarantine)
- **`internal/broker/cluster_grow_trigger.go` (NEW)** — `SubscribeClusterGrowTrigger` + `handleGrowTrigger`:
  `TargetNode==selfID` (else silent nil), account-sig verify vs pinned `account_pub`, 5-min `IssuedAt`
  replay skew — all copied from `cluster_upgrade_trigger.go`. Dispatch:
  `acquire-lock`/`release-lock`→leader `Propose`; `approve-join`→`OpClusterJoinApprove` (returns op_id);
  `join-status`/`confirm-op`→existing op admin ops; `mesh-cutover`→`cluster_grow_cutover`;
  `rebalance-proxy`→`OpClusterRebalanceProxy`. Retriable `cluster_not_ready` for the just-restarted window.
- **`internal/broker/cluster_grow_cutover.go` (NEW)** — the former-N1 standalone→clustered cutover:
  `renderClusteredCutoverConf` (natscluster.Render + secrets-dir fallback + `MonitorListen`), DryRun,
  `RouteCertSANMatches` preflight, `natsconf.Apply`; `resetLocalJetStreamForClustering` (`os.Rename` store →
  `jetstream.grow-bak.<growEpoch>`, gated on committed ≥2-server membership + per-epoch sentinel absent +
  Q3 non-empty ack; **move-not-delete**); `restartNatsServerHard` (same-uid **SIGKILL** → systemd revive;
  verify clustered via `/varz`; **BLOCK** with actionable message on revival failure / StartLimit). `--preserve-js-data`
  (v1) only ACKNOWLEDGES the move-aside (keeps the moved-aside dir for a manual restore); no auto backup is taken.
- **`internal/broker/cluster_operation_controller.go`** —
  - **#7**: add `opCatchupMaxWindow`(~30m) / `opCatchupStallWindow`(~90s) / `opCatchupBytesPerSec` floor;
    `catchupProbeFn(nodeID,barrier)→(applied,caught,err)`, `snapshotSizeFn()→(bytes,err)`, leader-local
    `opCatchupProgress` map (+mutex, mirrors `opAttempts`). At `OpStateRosterCommitted` set the persisted
    `catchup_deadline` = `now + clamp(base + dbBytes/rate, base, maxWindow)`; at `OpStateCatchingUp`
    transition to BLOCKED only on `catchupStalled(opID)` OR `now > CatchupDeadline`; clear progress on
    promote/BLOCKED. **No per-tick raft write, no schema migration** (reuses the durable `catchup_deadline`
    column, verified at :465-469). Fall back to the fixed 2-min `caughtUpFn` when `catchupProbeFn` is nil.
  - **marker mutual-exclusion**: `growActive` refusal in `StartRetireOperation`; **do NOT** add `growActive`
    to the `driveInFlightOperations` freeze and **do NOT** gate same-target `StartJoinOperation` (Q7
    carve-outs — either would deadlock the grow's own op).
  - **`toServing`**: add `a.deriveAndConvergeSeedsFromRoster()` at `NATS_ROLLED_OUT→SERVING` (best-effort,
    logged) so the **async** grow path converges seeds per-commit (P7).
- **`internal/broker/clusteradmin.go` / `clusterwrite.go`** — wire `catchupProbeFn` = a `clusterCatchupProbe`
  (same `SubjClusterCursor` scatter-gather as `clusterCaughtUp`, returns `AppliedIndex`) + `snapshotSizeFn` =
  stat the command-domain SQLite DB; keep `caughtUpFn` as the fallback.
- **`internal/broker/cluster_upgrade_trigger.go`** — in `acquire-lock`, refuse if `growActive` set
  (symmetric fence, Q7).
- **Legacy quarantine (Q6) — DROPPED** (see §3 Q6 + §12.7). `AddNode`/`OpClusterAdd` are the test-harness
  synchronous-add primitive + the CLI-unreachable proto-skew reject, not deletable dead code. No change.

### natsreconcile / natsconf (#3 fallback + withhold-swap + SAN)
- **`internal/natsreconcile/reconcile.go`** — add `SecretsDir` to `Inputs`. In `ReconcileOnce`, when
  rendering clustered over a **live standalone-JS** conf with `SecretsDir!=""`, derive `CAFile/CertFile/KeyFile`
  from the secrets-dir names + synthesize `ClusterListen` from the self route-URL port (**#3 fallback**),
  bypassing the `ClusterMTLS()` harvest hard-fail (`preflight.go:250-253`) WITHOUT touching the already-clustered
  harvest path. **Add `ActionAwaitingClusteredCutover`: for the standalone→clustered delta, render +
  DryRun-validate but return WITHOUT Apply (withhold the swap).** This is the load-bearing safety the
  sim-acceptance critic flagged: making the render *succeed* without withholding would let the autonomous C3
  reconciler swap a clustered conf onto a running-standalone former-N1 and SIGHUP it — re-arming #10/#4. The
  orchestrated cutover (P5) owns the apply. Route-add on an already-clustered live conf keeps the existing
  swap+SIGHUP path (#23).
- **`internal/broker/topology_reconcile.go`** — plumb `SecretsDir: b.cfg.ClusterSecretsDir` into
  `buildTopologyInputs`. **No change to `reloadNatsServer`** (SIGHUP stays the reconciler's only nats side
  effect; §11(h) invariant intact — verified topology_reconcile.go:19-23).
- **`internal/natsconf/preflight.go`** — add `HasClusterTLS()` predicate so harvest-vs-fallback is explicit
  + fail-closed on a malformed `cluster{}` without tls. Reuse existing
  `IsStandaloneJetStream`/`IsClusteredJetStream`/`JSStoreDir`.
- **`internal/cluster/route_san.go` (NEW, or into clusteroffline preflight)** —
  `RouteCertSANMatches(secretsDir, routeHost)` reusing the `leaf.Verify` pattern proven in
  `internal/cluster/g1_route_san_test.go` (#24 fail-fast; DryRun does NOT catch a missing SAN — it surfaces
  only at the route handshake). **Note: this does NOT clear #24's trailer token** (no cert-minting tool —
  G1-deferred); it only turns a silent mesh failure into an actionable up-front error.

### cmd/tether (the command + #5 + seam apply)
- **`cmd/tether/cluster_add.go` (NEW)** — `newClusterAddCmd`, run on the joiner host. Flags: `--confirm-node-id`
  + `$TETHER_CONFIRM_NODE_ID` (#5), `--account-seed` (signs triggers), joiner provisioning flags
  (`--self-id/--raft-addr/--nats-route/--tunnel-addr/--public-host/--secrets-dir/--node-ident-pub`),
  `--nats-url` (a live broker), `--reset-former-js` / `--preserve-js-data` (Q3), `--auto-confirm-catchup`
  (default 0, Q4), `--dry-run`, `--timeout`, `--notify-webhook`. Registered in `newClusterCmd()` online
  group. `registerYesRejector` (no `--yes` escape).
- **`cmd/tether/cluster_add_drive.go` (NEW)** — the P0–P9 sequencer; reuses
  `currentLeader`/`waitLeader`/`pollUntil`/HALT helpers from `cluster_upgrade_drive.go`;
  `signGrowTrigger`/`sendGrowTrigger` over `CanonicalGrowReqBytes`. **HALTs (never de-clusters) at every
  provisioning boundary with the exact command + "re-run to resume."**
- **`cmd/tether/cluster.go` (`newClusterInitCmd`, line 768)** — change
  `confirmTypedNodeID(cmd, selfID, "", false, "")` to `allowMachineEscape=true` + a `--confirm-node-id` flag
  gated on BOTH the flag AND `$TETHER_CONFIRM_NODE_ID == selfID` (same guard as `cluster remove`, cluster.go:551).
  Make init **APPLY** the `broker.yaml` cluster seam (currently only printed as NEXT-step 3, cluster.go:814).

### install.sh (deploy-tier)
- **`scripts/install.sh` (line 717)** — flip `nats-server.service` `Restart=on-failure`→`Restart=always`
  (Q2). Justified: completes the #23 clean-exit hardening the broker unit already uses (:737-748); makes the
  cutover restart deterministic. **Makes G4 a deploy-tier batch → simcluster drill 10/11 re-run is a hard gate.**

### test/simcluster (rewrite + drill inversion)
- **`test/simcluster/simcluster` (`cmd_grow`)** — rewrite to invoke `tether cluster add`. Delete the
  `[workaround #3]` `_reconcile_clustered` render loop, the `[workaround #4]` `mv jetstream` +
  `sctl restart nats-server`, the `[workaround #5]` pty-confirm feed + hand `broker.yaml` seam append, and
  the manual SIGHUP-reload loop. Only `[env]` secret-mint (#24 out of scope) + container/systemd
  **provisioning** (the cold daemon start `cluster add` brackets) may remain.
- **`test/simcluster/drills/11-grow-gaps.sh`** — invert the string-keyed asserts (the `#8` positive grep for
  "still in flight|is BLOCKED", the `#3` grep for "no cluster block to harvest"/"reconcile nats … not
  converged") to **assert-ABSENT**; drop #3/#4/#5/#8/#10 from the `GREW-VIA-WORKAROUNDS` trailer; keep the
  #I1 serve-refusal invariant GREEN while dropping the #I1 workaround token; add a positive orphan-stream
  regression (no single-replica orphan; events/history at `ReplicasFor(N)`; `jetstream.grow-bak.<epoch>`
  exists). Preferred: convert to a `cluster add` idempotent-regression.

## 6. Gotcha coverage table

| # | Gotcha | Exactly how G4 addresses it |
|---|---|---|
| **#3** | First standalone→clustered grow: `reconcile nats` can't harvest route mTLS | `natsreconcile.ReconcileOnce` secrets-dir fallback derives CA/cert/key + synthesizes `ClusterListen`; the reconciler **withholds** the swap (`ActionAwaitingClusteredCutover`) so the fix does not arm #10/#4; the orchestrated cutover owns the apply. |
| **#4** | Former-N1 standalone JS store orphans streams on clustered restart | `cluster_grow_cutover.resetLocalJetStreamForClustering` **moves** (not deletes) the store to `jetstream.grow-bak.<growEpoch>`, gated on committed ≥2-server membership + per-epoch sentinel (+ Q3 non-empty ack); D5 replica reconciler re-creates events/history at clustered R. Driven as a **signed remote trigger** (same-uid restart), never sim bash. |
| **#5** | `cluster init` needs a machine-escapable confirm + only prints the seam | `confirmTypedNodeID(...allowMachineEscape=true...)` + `--confirm-node-id`/`$TETHER_CONFIRM_NODE_ID` at cluster.go:768; init now **applies** the seam. |
| **#7** | Fixed 2-min catch-up deadline false-BLOCKs a slow-but-healthy joiner | **v1 shipped:** size-scaled durable `catchup_deadline` cap (reuses the existing column, no migration). **§12.10 follow-up (NOT shipped):** leader-local `AppliedIndex` progress + ~90s stall-window (never BLOCKs while advancing). |
| **#8** | Join-before-reconcile chicken-and-egg; `join approve --wait` blocks pre-mesh | Already correct inside `driveJoin`; G4 issues `approve-join` **non-blocking** + polls; the #3 render is what lets the existing catch-up gate pass. |
| **#10** | N=1-clustered-JS trap / mesh-before-joiner ordering | Orchestrator pre-renders the joiner's clustered nats (waiting as a route peer) FIRST, THEN cuts over the former-N1 → the former-N1's revived clustered nats immediately forms a 2-node mesh + JS meta at quorum, never clustered-alone (no exit-70). |
| **#23** | SIGHUP-reload vs full restart after mesh render | Split by delta: route-add on an already-clustered node → reconciler autonomous SIGHUP; standalone→clustered transition → orchestrated **full restart** (SIGKILL → `Restart=always` revive), never a SIGHUP (can't open a cluster port). |
| *(#24)* | *CN-only route-cert silently kills the mesh* | `RouteCertSANMatches` preflight fails the op up front with an actionable message. **Does NOT clear #24's trailer token** (no cert-minting tool). |

## 7. Invariant argument

- **R3 — no silent de-cluster / no half-committed membership.** The orchestrator NEVER calls
  `AddNonvoter`/`AddVoter`/`RemoveServer`; every raft config change flows through the one leader-driven
  `OpKindJoin` controller (AddNonvoter-first so an unreachable nonvoter can never dip quorum; promotion only
  after a persisted barrier is provably reached; predecessor-CAS phase guards). A HALT/crash leaves the
  joiner at-most a nonvoter or a full voter — never an in-between. The former-N1 store reset is gated on
  **committed** ≥2-server membership (data-plane follows committed control-plane), and the reverse
  (clustered→standalone) reset is explicitly NOT triggered here — that de-cluster path stays G2's explicit
  `--to-standalone`.
- **Idempotency / crash-recovery.** No raft change is double-applied (`!inRaft`/`!isVoter` guards +
  substrate re-derivation every drive). Durable state = the replicated `OpKindJoin` op row +
  `cluster_grow_active`; all joiner-host facts (`raft/`, conf `cluster{}`, broker up, isVoter, seam) are
  directly observable → re-running re-derives each phase and skips completed ones (no separate orchestrator
  FSM to corrupt). The JS move-aside is deterministic per-epoch (never double-wipes a freshly-clustered
  store); the full restart is repeatable. #7 progress is leader-local (lost-on-failover is the SAFE
  direction — a new leader re-observes and re-arms, never false-promotes); the size-scaled cap is durable.
- **Control/data-plane separation.** Raft membership (control plane, :7400) is mutated solely by
  `driveJoin`; the JS reset + mesh render (data plane) are decoupled. We deliberately do NOT add a
  `jsMetaHealthy` gate inside `driveJoin`'s AddVoter transition (which would invert plane separation) —
  instead **ordering** guarantees the cutover/JS-reset completes DURING `CATCHING_UP` (the mesh must form
  before the joiner cursor is even probeable), so promotion naturally follows data-plane convergence, with
  the orchestrator asserting JS-meta health as its own phase gate. ProtoVersion stays 2; all additions are
  additive/omitempty; **先父后子** holds — G4 composes only shipped subsystems (OpKindJoin, G5 signed-trigger
  transport, C3 reconciler, D5 replica reconciler, `InitFromExisting`, `OpClusterJoinApprove`, G1
  nats.d/Restart/route-SAN).

## 8. Test plan (adversarial; new phase tests join `make e2e` as the cross-phase regression net)

**Unit (broker/proto/reconcile):**
- `TestCanonicalGrowReqBytes_domainSeparation` — a grow signature never verifies as upgrade/join (& vice-versa).
- `TestClusterGrowReq_additiveOmitemptyRoundtrip` — required-only marshal omits optionals; unknown-field
  decode → zero-value; unknown op → `bad_request`.
- `TestHandleGrowTrigger_verifyAndDispatch` — wrong TargetNode → silent nil; tampered/absent sig →
  unauthorized; stale `IssuedAt` → replay-refused; `mesh-cutover` refused when NOT a standalone former-sole-
  voter; valid `approve-join` → deterministic op_id.
- `TestReconcileSecretsDirFallbackFirstGrow` + `TestReconcileAlreadyClusteredHarvestUnchanged` +
  `TestReconcileWithholdsClusteredCutoverSwap` — #3 render succeeds via fallback (golden conf); 2nd-grow
  harvest byte-identical to today; standalone→clustered delta renders+DryRuns but does NOT write.
- `TestAdaptiveCatchupDeadline_sizeScaledClamp` + `TestCatchupStallDetection` — nil `snapshotSizeFn` ⇒
  exactly today's 2 min; advancing index past a size-scaled deadline ⇒ never BLOCKED; flat index >
  stall-window ⇒ BLOCKED; **assert zero raft writes on idle ticks**.
- `TestFormerN1ResetGate_FiresOnlyWhenClustering` + `TestFormerN1Reset_IdempotentResume` +
  `TestFormerN1Reset_RefusesNonEmptyWithoutAck` (Q3) — gate true only for standalone + committed ≥2-server +
  no sentinel; crash after move/before restart ⇒ restart-only, no second move; a non-empty store without
  `--reset-former-js`/`--preserve-js-data` ⇒ HALT, never wipes.
- `TestRouteCertSANPreflight` — CN-only rejected with actionable error; DNS/IP-SAN accepted.

**Integration (embedded-nats):**
- `TestD9FirstGrowStandaloneToClusteredEndToEnd` — N=1 former-N1 + fresh joiner: approve → AddNonvoter →
  topo bump → withheld reconciler → orchestrated cutover (render+reset+SIGKILL-restart) → mesh forms →
  catch-up → AddVoter → both VOTER, stream R=2, no orphan (proves #3+#4+#8+#10+#23 with no manual reconcile).
- `TestGrowResume_sixFailurePoints` — kill/re-run at: after AddNonvoter/before mesh; after mesh/before
  AddVoter; catch-up stall; JS-reset interrupted (after move/before restart, after success); seed-publish
  failure; rebalance failure. Assert convergence to exactly one N+1 VOTER, no double AddVoter, no orphan,
  no bricked joiner.
- `TestClusterAdd_doubleRun_singleMembershipChange` — two concurrent adds for the same joiner attach to one
  op_id → exactly one voter; a fresh-bundle second run while active is refused.
- `TestNatsRestartRevivalFailure_BlocksLoudly` — simulate systemd NOT reviving nats (StartLimit / clean
  exit) → cutover BLOCKs with an actionable `reset-failed` hint, never silently strands the data plane.
- `TestSeedConvergeOnAsyncJoin` — `driveJoin.toServing` publishes seeds containing the joiner with no
  leadership edge.

**Concurrency (`-race` + repo NumGoroutine/fd leak gate — NOT goleak):**
- `TestGrowLock_noSelfBlock_symmetricFence` — `cluster_grow_active` held: `driveInFlightOperations` still
  drives the grow's own join op (not frozen); `StartRetireOperation` + upgrade acquire-lock refused; second
  add for a different joiner refused.
- `TestConcurrentAddRetireRace` — concurrent add + retire serialize via the marker; no interleaved
  AddVoter+RemoveServer half-commit; leak gate clean.
- `TestReconciler_NoDoubleWipeUnderTicks` — repeated `reconcileTopologyOnce` across the transition: exactly
  one store rename + one restart; no wipe once conf is clustered; no goroutine/fd leak.

**Sim drill (deploy-tier gate):** see §9.

## 9. Sim acceptance gate (authoritative — `docs/reviews/g4-plan.md §Acceptance`)

G4 is DONE only when ALL hold:
- **(A) TETHER-DOES-IT gate.** `test/simcluster/simcluster cmd_grow` is rewritten to invoke `tether cluster
  add` and contains **NO residual cluster-lifecycle `sctl`/`dexec` step** — a grep-assert confirms the only
  remaining `dexec/sctl` are `[env]` secret-mint (#24, out of scope) + container/systemd **provisioning**
  (the cold daemon start `cluster add` brackets, §2.1). init(#5)/seam/mesh-render(#3)/former-N1-JS-reset(#4)/
  nats-restart(#23)/non-blocking-approve(#8)/mesh-before-joiner(#10) are each performed BY tether, proven by
  tether's own logs, not sim bash.
- **(B)** `drill-10-grow-to-3` stays GREEN end-to-end via `tether cluster add`, including "every voter's
  tether-broker active after grow" and the follower-kill write-commit at 2/3.
- **(C)** `drill-11-grow-gaps` `GREW-VIA-WORKAROUNDS` trailer no longer names **#3/#4/#5/#8/#10**; its
  string-keyed asserts are inverted to assert-ABSENT; the #I1 workaround token drops while the serve-refusal
  invariant stays GREEN. Preferred: drill 11 becomes a `cluster add` idempotent-regression (run twice, kill
  mid-grow at each documented failure point, re-run → exactly one N+1 VOTER, no FK-panic/hollow-voter, empty
  trailer).
- **(D)** Non-sim in-process tests prove #7 (stalled/large InstallSnapshot promotes-when-progressing,
  BLOCKS-when-stalled) and the nats-restart revival-failure path (BLOCKs loudly, never strands). (drill 10 is
  same-host and cannot exercise the >2-min WAN InstallSnapshot.)
- **(E)** `install.sh nats-server.service Restart=always` landed and the deploy-tier drill re-run confirms
  deterministic revival.

Any step that still needs a manual bash workaround after this is a remaining tether GAP to EXPOSE
(`[GAP #N]` + trailer), never to script around.

## 10. Implementation scope (one continuous unit — NOT staged)

G4 is implemented as **ONE continuous body of work** — one plan → implement everything → one adversarial
internal review → one external review → one commit. There is deliberately **no internal a/b/c phasing**: all
of it lands together (wire + #5 + marker carve-outs + #3 fallback/withhold + `RouteCertSANMatches` + seed hook
+ `cluster_grow_cutover.go` SIGKILL-restart + `cluster_grow_trigger.go` + `install.sh Restart=always` +
`cluster_add.go`/`cluster_add_drive.go` orchestrator + #7 adaptive catch-up + sim `cmd_grow` rewrite +
drill-11 inversion), gated as a whole by the §9 acceptance gate + the hard gates (`make test`/`make e2e`/
`make lint` + touched-package `-race`/leak). The full file list is §5; the ordering within the continuous
push is a build-convenience detail, not a delivery boundary.

## 11. Residual risks

- **Intrinsic availability dip on 1→2.** The former-N1 IS the sole voter+leader and the cutover-restart
  target; you cannot transfer leadership to a not-yet-caught-up nonvoter, so bouncing its nats necessarily
  bounces the only leader. Bounded to seconds (raft state persists; the broker reconnects — `MaxReconnects(-1)`
  — or, if it clean-exits on nats loss, its own `Restart=always` revives it; nats revives via the new
  `Restart=always`). Documented + drill-validated (drill-10's "every voter tether-broker active after grow"
  guards it), not eliminable.
- **StartLimit deadlock.** A single deliberate SIGKILL is 1 failure (well under `StartLimitBurst`), so it
  should not trip; a resume that restarts repeatedly could. Mitigation: the cutover verifies `/varz` clustered
  before declaring success and BLOCKs with a `reset-failed` hint on revival failure (test D).
- **Legacy custom cluster name.** The secrets-dir fallback defaults the cluster name to `tether`; a
  historical custom-named cluster would split-mesh. Mitigation: a unit test pins fallback-name == harvest-name;
  flag any legacy custom name as an operator-confirmable assertion.
- **Adaptive-cap approximation.** On-disk DB size ≠ InstallSnapshot transfer size. Mitigation in the DESIGN was
  that the stall-window is the real gate so a mis-sized cap never false-fails a progressing joiner — **but the
  stall-window is a §12.10 follow-up NOT shipped in v1; v1's mis-size mitigation is the generous size-scaled cap
  alone** (a mis-size is a recoverable annoyance, re-run to resume, never data loss). Surface the computed
  deadline in `cluster ops show`.

## 12. Finalizer divergences from the workflow candidate (traceability)

1. **systemctl boundary (§2.1).** The candidate had `cluster add` locally `systemctl start` the joiner
   daemons; this violates the `cluster.go:782-784` "tether does NOT orchestrate systemctl" invariant.
   Finalized: `cluster add` never calls systemctl; the joiner cold-start is provisioning/operator (bracketed,
   resumable re-run); only the former-N1 cutover restart is tether-owned via SIGKILL + `Restart=always` revive.
2. **Q3 data safety.** Strengthened from "move-aside default" to "move-aside default **+ LOUD operator-gated
   on a non-empty store**" (`--reset-former-js`/`--preserve-js-data`) — the live racknerd N=1 carries real
   data; a silent-loss default is unacceptable (feedback: tests/security pragmatic, expose don't mask).
3. **Q4 auto-confirm.** Changed from the candidate's "bounded auto-confirm" default to **no auto-confirm by
   default** (the sim-acceptance critic's position): #7 makes a BLOCKED op a real stall → surface it; opt-in
   `--auto-confirm-catchup N` (default 0) only.
4. **Q7 marker.** Adopted **strict cluster-wide serialize** (candidate leaned strict; cli-ux leaned per-target)
   — plus the mandatory self-op carve-outs so the grow's own join op is never frozen.
5. **#24 SAN preflight scope.** Clarified `RouteCertSANMatches` is fail-fast only and does **not** clear #24's
   trailer token (no cert-minting tool ships in G4).
6. All load-bearing code facts (install.sh Restart lines, async-seed-converge gap at controller.go:572,
   legacy AddNode at clusteradmin.go:198, init confirm/seam at cluster.go:768/814, reconciler SIGHUP-only at
   topology_reconcile.go:19-23) were **independently re-verified against the tree** before finalization.
8. **Deploy-tier iteration refined the orchestrator design (8 `weilandserver` drill runs).** The candidate's
   joiner flow (boot standalone → post-start cutover) did NOT survive real systemd + separate nats-servers +
   root/tether ownership. Each run surfaced one structural bug the hermetic suite cannot reach; the final,
   sim-GREEN (`drill 10-grow-to-3` 19/19 + `drill 11-grow-gaps` 12/12) design is:
   - **Joiner clustered-conf PRE-RENDER (the load-bearing correction).** A fresh joiner CANNOT boot cluster-mode
     with a standalone nats.conf (its broker connects with cluster nkeys the standalone conf lacks →
     "nkeys not supported"; a clustered-alone JS meta cannot form). So the orchestrator renders the joiner's
     OWN clustered nats.conf BEFORE its daemons start, via a NEW **`mesh-peers` grow-trigger op** (the leader
     returns the `server,route,bus-nkey` triples from cluster_nodes — the roster does NOT carry bus-nkeys) fed
     to `reconcile nats --manual`. BOTH brokers must be clustered when the joiner boots.
   - **Local-socket joiner ops.** An unmeshed joiner is unreachable over NATS, so joiner liveness + (fallback)
     cutover go through the joiner's LOCAL admin socket (`OpClusterGrowCutover`); the former-N1 cutover stays
     the over-NATS signed trigger (the leader IS reachable).
   - **Ordering.** invocation-1: init → approve (non-blocking) → wait CATCHING_UP → render joiner conf →
     former-N1 cutover → HALT at start-joiner. provisioning: **restart** joiner nats (load the clustered conf —
     a bare `start` is a no-op on the already-running standalone nats) + start broker. invocation-2: resume →
     catch-up → VOTER → rebalance → release.
   - **Seam-apply is root-owned config.** `init`'s `applyClusterSeam` works only when init runs as root (the
     real `sudo cluster init`; hermetic-tested); `/etc/tether/broker.yaml` is root-owned (G1), so when `cluster
     add` runs init as tether the seam is provisioned as root by the operator/sim — broker.yaml is operator config.
   - **`--build build` vs `--build up`.** The sim rebuilds the docker image only on `build` (not `up`); a stale
     baked binary masked the new command for two runs (the same G1 gotcha).
   All fixes are additive (ProtoVersion stays 2) and kept every hermetic gate green.
9. **Q6 legacy quarantine DROPPED (implementation-time correction).** The workflow drafts + critics asserted
   `AddNode`/`OpClusterAdd` were "unrouted dead code" safe to delete. Grep during implementation proved otherwise:
   `admin.AddNode(...)` is the **synchronous-add primitive ~10 test suites use to bootstrap cluster members**
   (g2/g3/g5/g7/force-single/rebind/operation-controller), kept deliberately for the d9_integration harness
   (clusterwrite.go:103); `OpClusterAdd` is the **CLI-unreachable proto-version-skew reject** (a safety path,
   `cluster_c8_hints_test.go:13`). The CLI `add` verb was already deleted in C8, so there is no live footgun
   and no name collision with the new `cluster add`. Deleting would break a large test surface for zero
   product gain → kept as-is. (Discipline: "实现中发现设计问题先改文档再改代码" — plan revised before code.)
10. **#7 catch-up: SIZE-SCALED DEADLINE ONLY in v1 (Stage-C review M2 adjudication).** The plan's #7 (§4 P6,
    §5, §11, §9-D, §12.8) described TWO parts: a size-scaled `catchup_deadline` (SHIPPED) AND a leader-local
    AppliedIndex progress-map + ~90s stall-window (NOT shipped). v1 ships **only** the size-scaled deadline
    (base 2m + dbBytes/512KiB·s, clamped to 30m). Consequence: a genuinely-slow-beyond-clamp WAN joiner can be
    false-BLOCKED — but a BLOCKED join op is **operator-recoverable** (`cluster ops confirm <op>` re-arms it),
    so it is a recoverable annoyance, never data loss or a stuck grow. The AppliedIndex stall-window (so the op
    "never BLOCKs while advancing") is a documented **follow-up**; §11's "the stall-window is the real gate"
    claim and §9-D's promotes-when-progressing test are superseded by this note. The 30-min clamp is generous
    for the current fleet (small command-domain DBs). (Security-pragmatic: the recoverable BLOCK is acceptable
    for v1; the progress-map adds a per-tick cursor probe + leader-local map not yet warranted.)

## 13. Stage-C internal review — main-process adjudications

The adversarial internal review (`docs/reviews/g4-review.md`) surfaced 1 BLOCKER + 6 MAJOR + 7 MINOR (1 refuted).
Main-process dispositions (all fixed in this same pass unless noted):
- **B1** (findJoinOp attaches to terminal op → resume dead-end) — **FIXED**: findJoinOp attaches only to a
  non-terminal op; driveAdd short-circuits an already-VOTER joiner to release+done (crash-post-SERVING resume).
- **M1** (--preserve-js-data signed-but-dropped + dead-end) — **FIXED**: PreserveData unblocks the reset gate
  (both flags move the store aside, never delete); flag help + refusal message corrected; auto backup→restore
  deferred (this note). **M2** (#7 stall-window) — **DEFERRED + documented** (§12.10). **M3** (sentinel-after-
  rename crash wedge) — **FIXED**: the backup dir's existence is the durable idempotency signal. **M4/M5/M6**
  (cutover/resume/fence test gaps) — **tests added** (see below).
- **m1** (StartJoinOperation grow fence) **FIXED**; **m2** (force monitor 8223) **FIXED**; **m3** (dead
  OpClusterGrowCutover) **DELETED**; **m4** (ReadDir fail-open) **FIXED**; **m5/m6/m7** test gaps **addressed**.
- Added hermetic tests: cutover revival-failure BLOCK + R3 gate + render, move-aside crash-window, growActive
  symmetric fence + carve-out, StartJoin different-node fence, ReadDir fail-closed, CanonicalGrowReqBytes
  JoinBundle/OpID tamper, mesh-peers handler. drill 11 grows a data-bearing former-N1 asserting grow-bak (m7).
