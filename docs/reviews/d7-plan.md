# D7 Plan (FINAL) — cluster lifecycle + safe membership + force-single escape

> **Status**: Stage A finalized by the main process (sole adjudicator). Drafted by a 4×Opus-4.8 adversarial workflow (4 drafters / 4 critics / 1 synth — full synth artifact in `d7-plan-synth.md`). This file is the **implementation ruler** for Stage B. Load-bearing API claims were re-verified against source by the main process (the D5 fake-reader lesson). Build-and-prove; production cutover = D9.

---

## 0. Adjudication summary (decisions that bind Stage B)

**Scope decision**: D7 stays **one phase** (the largest in the epic). Membership entry / health / exit / recovery all hang on the two-phase `ClusterNodeUpsert` op + the `phase` state machine + one harness, so it is one coherent unit. Riskiest-coupled items are sequenced first (§K) so the irreducible core (FSM plumbing + force-single) lands before the CLI veneer.

**Open-question rulings** (the synth escalated 6 genuine forks; I rule):

| OQ | Ruling |
|---|---|
| **OQ-1 catch-up barrier domain** | **command-domain**. Barrier = the leader's `AppliedIndex()` (the existing `cluster_meta.applied_index` command cursor) captured under `VerifyLeaderRead` at AddVoter time. The new voter is caught up when its **local** `AppliedIndex() >= barrier`, sustained non-lagging for a fixed wallclock, max-wait → `catch_up_stalled`. Rejects raft-domain `CommitIndex` (a command cursor can never reach it — config/noop entries don't pass through `FSM.Apply`, so every join would hang). Consistent with the D6 `epoch-as-local-row-epoch` precedent. **This is a state-machine-contract clarification of §8.2 — flagged for external-review sign-off.** |
| **OQ-2 forged-sig verdict** | **poison-skip via a new `errAppliedRejected` sentinel** (verified: `fsm.go:111` turns an applier error into retry→`panic`; a committed forged entry verified-false on every replica would brick the cluster on every boot's log replay — a one-shot remote brick a compromised leader (in-scope §18.2.4) must NOT get). Rejects Draft 1's panic. |
| **OQ-3 offline raft boundary** | **refined stricter than synth**. The RecoverCluster + FSM wiring (needs the unexported `fsm`) lives in **`internal/cluster`** as an exported `RecoverSingleNode(dir, selfID)` (+ offline-probe helpers). **`internal/clusteroffline`** holds only orchestration — `flock` (x/sys/unix), the (b)(c)(d) preconditions, peer **TCP-liveness** probe (`net.DialTimeout`, no raft), dump-divergent (SQL) — and calls `internal/cluster`. **raft stays confined to `internal/cluster`.** cobra `cluster_offline.go` calls `clusteroffline` functions (not banned tokens). |
| **OQ-4 join-signature fetch** | **paste-token**. Add a `cluster sign-join <nonce>` verb on the joiner (doc-amend the §8.1 table). Flow: `cluster add <host> <node-pub>` on the leader issues a nonce + prints the instruction → operator runs `cluster sign-join <nonce>` on the joiner (it has the node-ident seed + shell, §10.6) → re-runs `cluster add <host> <node-pub> --join-token <token>`. Admin strictly local; **no new NATS subject/ACL**. |
| **OQ-5 nonce single-use + binding** | **leader-local pre-propose single-use + honest doc note** (single-use is a consistency property, not a security boundary — §18.2.4 accepts a compromised leader proposes anything; per the security-pragmatic feedback, no replicated nonce ledger). Canonical signed message binds `(domain-sep, node_id, node_ident_pub, join_nonce)` — **no cluster-id** (the fresh leader-issued nonce already defeats cross-cluster replay). Rejects Draft 3's `OpJoinNonceIssue` ledger. |
| **OQ-6 plain-drain voter semantics** | **yes**. Plain `drain` (no `--retire`) keeps the node a raft voter but sheds serving (expose-home) load; the quorum-projection guard is on **projected serving-set fault tolerance**, so plain-drain-at-N=2 still fires the **F==0 TTY+typed-confirm** (matches §8.3 L272 "含已处于 N=2 时再 drain"). |

**Verified-against-source before finalizing** (so Stage B doesn't build on a wrong assumption):
- `auth.VerifySignature(pub string, msg, sig []byte) error` at `internal/auth/nkey.go:55`; `go list -deps ./internal/auth` ⇒ only `nats-io/nkeys` + `jwt/v2`, **no `nats.go`/`internal/cluster`/`hashicorp/raft`**. So `internal/cluster` imports `internal/auth` cycle-free, and the D6 no-NATS guard (`test/d6/regression_test.go:166`, bans `nats-io/nats.go` specifically, **not** `nkeys`) still passes.
- `fsm.go:102-111` retry→panic on `ApplyTx` error; `fsm.go:221` `appliedPoison` commits an `applied_index` advance and runs no op — the template the `errAppliedRejected` branch reuses.
- `hashicorp/raft@v1.7.3` `RecoverCluster` replays `[snapshotIndex+1 .. LastIndex()]` through `fsm.Apply` and writes a single-server config+snapshot; `HasExistingState` gates the (b) precondition; `commitIndex` is **never** persisted to the BoltStore (only `currentTerm`/`votedFor`) — so the §8.4(c) "apply up to commitIndex" prose is unimplementable offline and is rewritten (§B-2).
- `serve.go:128` sets `AdminSocketPath`, never touches `cluster.*` — already clean for byte-unchanged build-and-prove.

---

## A. Scope & build-and-prove boundary

Three tiers with opposite provability:

| Tier | What | Real in production at D7? | Proven by |
|---|---|---|---|
| **(A) ONLINE** — `cluster add/remove/drain/retire/transfer-leader/rotate-tunnel-cert`, ctl/NATS `status`/`doctor`, online adminsock ops, the new `ClusterNode*` ops, `Node.AddVoter/RemoveServer/LeadershipTransferToServer`, the orchestrator + leader-startup reconciliation | **NO**. `serve.go` byte-unchanged; production broker builds no `cluster.Node`. Online cluster subcommands exist but **fail-fast "cluster mode not enabled"** when the adminsock cluster backend is nil. | gated `test/d7` (`//go:build d7_integration`, `TestD7Matrix -race`), extending the `test/d6` multi-broker routed-NATS + mTLS-raft + real-tunnel shape with a **dynamic join** path |
| **(B) OFFLINE** — `force-single`/`recover` + offline `status`, operating disk `raft/`+`tether.db` with a STOPPED daemon | **YES, genuinely real** (the escape hatch has no cutover; D9 only adds `--from-existing`) | gated `test/d7` runs them on **harness-created real disk** (spin a real cluster, kill it, run the offline tool on the dead node's files) |
| **(C) inert-until-D9** — the `serve.go`/`broker.New` lines that would construct `cluster.Node`, register the adminsock cluster backend, take the daemon-side disk lock | deferred to D9 | — |

**Explicitly DEFERRED (with reason):**
- **Replicated alert store** (`alert ls/ack` store-backed) → **D8b** (already owns it). D7 ships only the **client-synthesized banners** it needs for status exit codes (`force_single_active`, `quorum_lost`) + the one legitimate D7 alert write: `force_single_active` written by the *offline tool* into the now-single-node disk DB. drain's "persistent severe" at F==0 is a **predicate + TTY confirm + loud log** at D7, not a replicated row.
- **Daemon-side disk-lock acquisition** (`broker.New`/`serve.go` taking `tether.lock`) → **D9** (touching production startup violates build-and-prove). D7's offline-vs-live-daemon interlock = a **BoltDB-lock probe** (§H).
- **`cluster init --from-existing`** → **D9**. D7 ships the `init` skeleton erroring "use the D9 path"; the harness bootstraps directly.
- **account.nk / cluster-CA rotation tooling** → **D9**. D7 retire ships the **runbook doc + the status honesty warning** "retired node credentials remain cryptographically valid until rotation" (§8.3 L273).

**Guard extension** — `test/d7/regression_test.go` mirrors `test/d6/regression_test.go`:
- New banned-token SSOT `d7BannedTokens` over `cmd/tether` + `internal/broker` + `internal/agent` (minus the mechanism files): `AttachClusterAdminSeam(`, `.clusterAdmin =` / `clusterAdmin:`, `AddVoter(` / `RemoveServer(` / `LeadershipTransferToServer(` / `ProposeClusterNodeUpsert(`, and `cluster_nodes`-**write** SQL (`INSERT INTO cluster_nodes` / `UPDATE cluster_nodes` / `DELETE FROM cluster_nodes`) appearing in any scanned production file. The orchestrator/responder mechanism file `internal/broker/clusteradmin.go` is **excluded** from the scan (the D7 analogue of `home.go`).
- **Bidirectional self-check** (analogue of `TestD6GuardSelfCheck`): prove each token both (i) passes on clean source AND (ii) is **caught when planted** in a scanned file (the D5 fake-reader vacuity class — a guard that only proves "clean passes" is vacuous).
- **Import-cycle re-assertions**: `internal/clusternodes` stays nats-free + raft-free even after D7 co-locates the `ClusterNodeUpsert` **Plan** helper there; **`internal/cluster` stays nats.go-free AFTER the new `internal/auth` edge** (none of the drafts guarded this — bans `nats-io/nats.go`, not `nkeys`).

---

## B. Doc-first amendments (apply to `docs/distributed-broker-architecture.md` BEFORE code)

**B-1. §19-D7 — add a `### D7 范围定稿（先改正文）` block** mirroring the D5/D6 blocks: build-and-prove (cutover=D9); `serve.go` byte-unchanged; production builds no `cluster.Node`; online cluster commands fail-fast "cluster mode not enabled"; offline force-single/recover genuinely real; guard `TestD7ProductionWiresNoCluster` token list + `internal/clusteroffline`/`internal/cluster` package boundaries; migration 0013; gated `test/d7` harness.

**B-2. §8.4(c) (L291-292) — REWRITE the two-store reconcile** (the highest-stakes correctness item; all 4 critics blocking). Replace "把 BoltDB 已提交到 commitIndex 的 entry 应用进 SQLite" with:
> force-single 的两存储调和 = **`raft.RecoverCluster({self})` 本身驱动**：它把本地 BoltDB log `[snapshotIndex+1 .. LastIndex()]` 经 `fsm.Apply`（幂等、同 txn 推进 `applied_index`）前向重放进 SQLite，再写单成员快照 + config entry。**恢复点 = 本地 `LastIndex()`**（`commitIndex` 离线不可得、从不落 BoltStore；运维已声明 peers 死亡，本节点整条 log 即权威时间线，其未提交尾部被 force-single 按既成事实提交——loud log 记之）。**绝不**再手搓 `ExecCommand` 前置重放（`ExecCommand` 不推 `applied_index`，叠加 RecoverCluster 的重放 = 非幂等 op 双应用）。`recover --dump-divergent`（回归 peer 上跑）取证该回归节点 **pre-wipe SQLite 的行**，诚实声明"此节点曾有、新时间线可能没有，不可自动合并"。

**B-3. §8.1 (L260-263) — pin the join-PoP binding + verify-fail verdict + persistence.**
- Signed message = domain-separated canonical tuple `"tether-cluster-join-v2\0" || node_id || "\0" || node_ident_pub || "\0" || join_nonce` (no cluster-id — OQ-5). Verify inputs travel in `cmd.Aux` (apply-inert JSON) **and** persist as `cluster_nodes` columns (survive snapshot truncation for retry + audit). A cheap **Aux-vs-Body cross-check** (Aux values == baked column literals) guards a leader splicing mismatched values.
- Verify-fail verdict prose:
> follower `Apply(ClusterNodeUpsert)` 复算 join PoP 失败 = **POISON 跳过**（推进 `applied_index`、不执行 roster UPSERT、loud log），**绝不返回 applier 错误**（那会触发 §3.7 重试→panic 的 wedge）。verify 是确定性纯函数（固定 nkey 库 + canonical 消息），全副本同判，不存在"部分接受、部分拒绝"的分叉。
- Amend §13.1: whitelist the ONE custom applier (`clusterNodeUpsertApplier`) as the documented exception to "all ops ride `genericExecApplier`", with a negative-control (a deliberately-wrong verify must poison-skip identically on every replica).

**B-4. §8.1 — add the `leader-startup membership reconciliation pass`** (the real no-silent-fork fix; status render is a display, not a safety property):
> 新 leader 上任后、服务任何成员命令前，对每个 `cluster_nodes` 行**幂等调和** roster `phase` 与 `raft.GetConfiguration()`：`{PENDING ∧ raft-voter}`（AddVoter 提交后旧 leader 崩在 phase 推进前）→ 前向补 `CATCHING_UP`，**绝不** RemoveServer；`{VOTER_ADD_FAILED ∧ raft-voter}`（AddVoter 超时≠失败，config entry 实已提交——D4 committed-but-ack-lost 同类）→ 重查 config，实为 voter 则推 `CATCHING_UP`；`{roster=∅ ∧ raft-voter}` → loud `INCONSISTENT`、拒自动动作、指 `cluster doctor`。

**B-5. §8.2 (L269) — pin the catch-up barrier domain (OQ-1).** Add: the predicate is **command-domain** — barrier = the leader's `AppliedIndex()` (the `cluster_meta.applied_index` command cursor) captured under `VerifyLeaderRead` at AddVoter time; the voter is caught up when its **local** `AppliedIndex() >= barrier`, sustained as a non-lagging voter for a fixed wallclock, max-wait → `catch_up_stalled`. Explicitly **NOT** raft `CommitIndex` (config/noop entries never pass through `FSM.Apply`, so the command cursor can never reach an all-entry barrier → every join would hang). Keeps both comparison sides in the command domain (the HA "0 committed business loss" claim is about business state). *External-review sign-off requested on this domain choice.*

**B-6. §8.2/§8.3 — `catch_up_stalled` is a DERIVED display state, not a 7th phase** (the 0008 `phase` CHECK has exactly 6 values; writing a 7th → CHECK fail → applier error → panic). It stays `phase=CATCHING_UP`; the stall is derived from `phase_changed_at` + max-wait, detail in `voter_add_error`.

**B-7. §8.4(a)/§18.2.5(a) (L276, L460) — the disk interlock is TWO mechanisms** ("share one advisory lock with the daemon" is false at D7: the production daemon constructs no `cluster.Node`, holds no lock):
> offline 工具 (a) 取 `flock(2)` `${DataDir}/tether.lock`（防两 offline 实例并发）**且** (b) **探测 `raft/raft.db` 的 BoltDB 排他锁**（活 daemon 开 BoltStore 即持有它）——不可排他打开 → HARD-REFUSE "daemon 仍在运行；先 `systemctl mask` 并停它"。daemon 端取 `tether.lock` 属 **D9**（改 `serve.go`/`broker.New` 是 cutover）。`SetMaxOpenConns(1)` 是进程内串行化，零跨进程保护。

**B-8. §8.4(d) (L293) — peer-reachable HARD-REFUSE is TCP-liveness, not just mTLS:**
> 对每个 `--confirm-peers-dead` peer 的 `raft_addr` **完成 TCP 连接即 HARD-REFUSE**（peer 接受 TCP = 活着，即便随后 TLS 因证书轮换/吊销失败）。mTLS 握手仅用于 mode-B status **展示**；安全闸取更保守的 TCP-completes 判定。`--confirm-peers-dead` 须列出 disk roster 里**每个**非 self 节点，漏一个（那个仅被分区的节点会脑裂）即拒。

**B-9. §17 (L439-441) — add the `--json` schema (versioned) + `reach_source` discriminator + 5 render fixtures.** ctl/NATS-view `reachable:false` MUST carry `reach_source:"self-report"` (didn't hear over NATS) vs `"raft-ping"` (offline mode B, actually pinged + dead); **NATS-unreachable = UNKNOWN, never DEAD**; exit code 2 (`quorum_lost`) emittable in NATS view **only from a positive self-report of read-only/no-quorum**, never from absence of reports (the false-quorum-lost → wrong-force-single chain §17 exists to prevent). Fixtures: HEALTHY-HA(0)/DEGRADED(1)/quorum-lost(2)/force-single(3)/joining(1).

**B-10. §15/§8.3 — `cluster rotate-tunnel-cert <node_id>` is a D7 online command** (§19-D6 L604 already assigns it here). It rides `ClusterNodeUpsert` writing `cert_fp`/`cert_fp_prev`/`cert_fp_valid_until` (no join-PoP path — cert rotation is not a new admission), reusing the D6 `cert_pins` `VerifyConnection` machinery (先父后子).

---

## C. Migrations / schema

**`0013_cluster_nodes_join_pop.sql`** (one migration, ADD COLUMN only — preserves PK + UNIQUE(name) + idx_cluster_nodes_phase; leader-baked literals only, NO `CURRENT_TIMESTAMP` §3.4):
```sql
ALTER TABLE cluster_nodes ADD COLUMN join_nonce       TEXT;      -- leader-issued, baked; followers re-verify from row+Aux
ALTER TABLE cluster_nodes ADD COLUMN join_sig         TEXT;      -- hex(ed25519 sig) over the canonical join tuple
ALTER TABLE cluster_nodes ADD COLUMN voter_add_error  TEXT;      -- VOTER_ADD_FAILED detail / 'catch_up_stalled'; NULL=none
ALTER TABLE cluster_nodes ADD COLUMN phase_changed_at TIMESTAMP; -- leader-baked literal; stall derivation
```
**Decisions**: columns **nullable, NOT `NOT NULL DEFAULT ''`** (an empty sig must be impossible-to-pass, not conventionally-skipped; `nkeys.Verify('','')` fails closed; nullable keeps the D9 self-row/grandfathered rows legal). **No** `cluster_join_nonces` table / `OpJoinNonceIssue` op (OQ-5). **No** `raft_config_voter` mirror column (the canonical config is `raft.GetConfiguration()`; persisting a mirror invites the exact DB-vs-raft fork §8.1 forbids — status joins live config against the roster, the fork is *rendered* never *stored*). **No** drain-flag schema — drain deadline rides a `cluster_meta` KV (§D.4).

---

## D. Ops + Plan/Apply

Three node ops + one drain-meta op in `internal/cluster/command.go` (`const` + `knownOps` + `defaultAppliers()`). Plan helpers co-locate in **`internal/clusternodes/plan.go`** (pure-SQL leaf; its package doc already declares the intent). `commandVersion` stays **2** (Aux exists since D4). All four carry **no reqID** (leader-driven, like `OpPortReassignHome`; the CAS/phase guards are the idempotency anchors).

**D.1 `OpClusterNodeUpsert` — Phase-1 admission (the ONE custom applier).**
- **Plan (`clusternodes.PlanUpsert`)**: bakes `INSERT INTO cluster_nodes(...) VALUES(<literals>, phase='JOIN_VERIFIED_PENDING_VOTER', join_nonce=<lit>, join_sig=<lit>, phase_changed_at=<LitTime>) ON CONFLICT(node_id) DO UPDATE SET cert_fp=…,… WHERE <monotonic phase-rank guard>` (encode the 6 phases as an integer ladder; refuse to regress a more-advanced phase to PENDING via a stale re-add — the membership analogue of `PlanReassignHome`'s `epoch < new` CAS at `port/plan.go:189`). Carries `{node_id, node_ident_pub, join_nonce, join_sig}` in `cmd.Aux`.
- **Custom applier `clusterNodeUpsertApplier`** (registered in `defaultAppliers()` instead of `genericExecApplier`):
  1. decode `cmd.Aux` → `{node_id, identPub, nonce, sig}`
  2. `msg := canonicalJoinMsg(node_id, identPub, nonce)`
  3. cross-check Aux values == baked Body column literals (defense-in-depth)
  4. `if auth.VerifySignature(identPub, msg, sigBytes) != nil { loud log; return errAppliedRejected }` — poison-skip the op SQL, advance applied_index, never error/panic
  5. else `genericExecApplier{}.ApplyTx(tx, cmd)`
- **FSM plumbing (the small fenced fsm.go change)**: add a package sentinel `errAppliedRejected`; in `applyCommand`, check `errors.Is(err, errAppliedRejected)` from `ApplyTx` **before** the retry loop and treat it exactly like the `appliedPoison` path (commit the applied_index advance, run no op, return `appliedRejected{index}`, loud log) — NOT a transient error. Doc-amended (§B-3) + negative-control test (§J).
- **L-2**: verify = `auth.VerifySignature` (`internal/auth/nkey.go:55`, deps cycle-free + nats.go-free). `internal/cluster` imports `internal/auth` directly — **no new `joinpop`/`clusterauth` package** (verified unnecessary). `clusternodes` keeps only the Plan baking (pure-SQL); the verify lives in `cluster`, reachable from `FSM.Apply`.

**D.2 `OpClusterNodePhase` — transitions (`genericExecApplier`).** `UPDATE cluster_nodes SET phase=<LitText>, voter_add_error=<lit|NULL>, phase_changed_at=<LitTime> WHERE node_id=<lit> AND phase IN (<allowed predecessors>)`. The `phase IN (...)` predecessor guard = deterministic CAS (a stale ex-leader's disallowed transition is RowsAffected==0 no-op). Edges: `PENDING→CATCHING_UP`, `CATCHING_UP→VOTER`, `*→VOTER_ADD_FAILED`, `VOTER→DRAINING`, `DRAINING→RETIRING`, `DRAINING→VOTER` (abort).

**D.3 `OpClusterNodeRemove` — roster delete (`genericExecApplier`).** `DELETE FROM cluster_nodes WHERE node_id=<lit> AND phase IN ('RETIRING','VOTER_ADD_FAILED')` (a live VOTER is structurally undeletable). Idempotent (RowsAffected==0 re-delete).

**D.4 `OpClusterDrainSet` — drain flag (`genericExecApplier`, no migration).** `cluster_meta` UPSERT `key='draining:'+node_id, value=<LitTime(deadline)>`, monotonic guard (later deadline wins; clear = delete), reusing the audit-checkpoint monotonic-UPSERT pattern.

---

## E. Node membership wrappers + orchestrator (raft confined, L-2)

`n.raft` unexported (`node.go:71`). New `internal/cluster/membership.go`:
```go
func (n *Node) AddVoter(id raft.ServerID, addr raft.ServerAddress) error             // raft.AddVoter(id, addr, 0, applyTimeout); leader-gated
func (n *Node) RemoveServer(id raft.ServerID) error
func (n *Node) LeadershipTransferToServer(id raft.ServerID, a raft.ServerAddress) error
func (n *Node) RaftConfiguration() ([]raft.Server, error)                            // generalizes NumVoters' GetConfiguration
func (n *Node) LeaderWithID() (raft.ServerAddress, raft.ServerID)                     // non-leader fail-fast naming
```
All leader-gate first (`State()==Leader` else `raft.ErrNotLeader` → `cluster.IsNotLeader`). AddVoter/RemoveServer are **raft config-change actions, NOT `Propose`**.

**Orchestrator (broker-side `internal/broker/clusteradmin.go`, build-and-prove, guard-excluded):**
1. `Propose(PlanUpsert)` → Phase-1 committed (roster=`JOIN_VERIFIED_PENDING_VOTER`, PoP re-verified by all followers).
2. **only after step-1 nil** → `AddVoter(id, addr)`. err → `Propose(PlanPhase(*→VOTER_ADD_FAILED, err))`; ok → `Propose(PlanPhase(PENDING→CATCHING_UP))`.
3. catch-up gate (§B-5 command-domain barrier) → `Propose(PlanPhase(CATCHING_UP→VOTER))`.

**No-silent-fork = ordering + leader-startup reconciliation + render**: Phase-1 commit strictly precedes AddVoter ⇒ `{phase=VOTER ∧ not-voter}` unreachable; the post-crash `{PENDING ∧ raft-voter}` / `{∅ ∧ raft-voter}` states are closed by the **leader-startup reconciliation pass** (§B-4, driven by the committed phase column + live `RaftConfiguration()`, never in-memory leader state); idempotent retry (incl. across leadership transfer) re-adds when phase≥PENDING (no re-sign — nonce consumed via the monotonic guard); status cross-renders `phase` vs `role` → bold `INCONSISTENT` (defense-in-depth backstop, not the primary guarantee).

---

## F. CLI + admin socket

**Cobra group `cmd/tether/cluster.go` + `cmd/tether/cluster_offline.go`.** Offline disk logic = `internal/clusteroffline` (orchestration only, calls `internal/cluster.RecoverSingleNode`; OQ-3). cobra `force-single` calls `clusteroffline.ForceSingle(dir, ...)` — a function call, not a banned token.

| Subcommand | Transport | Prod behavior (no cluster.Node) |
|---|---|---|
| `add <host> <node-pub> [--join-token]` / `remove <node_id>` | online adminsock → leader | "cluster mode not enabled" |
| `sign-join <nonce>` | local (joiner signs with node-ident seed) | real |
| `drain <node_id> [--retire\|--now\|--abort]` / `transfer-leader <node_id>` / `rotate-tunnel-cert <node_id>` | online adminsock | same |
| `status` / `doctor` `[--json] [--offline]` | NATS (default) OR disk (`--offline`) | NATS view works; `--offline` real |
| `node-pub` / `keygen` | local file | real |
| `init [--from-existing]` | local | `--from-existing` errors "D9 path" |
| `force-single --confirm-peers-dead <ids...>` / `recover --dump-divergent <file>` | **offline disk** | **real** (daemon stopped) |
| `alert ls/ack` | NATS | D8b store; D7 client-synthesized banners only |

**New adminsock ops** (extend the envelope, `protocol.go`): `OpClusterAdd/Remove/Drain/Transfer/Status/RotateCert`. `Request` gains `NodeID,NodePub,Host,JoinToken,Retire/Now/Abort bool` (`omitempty`, byte-compatible). `Response` gains `Cluster *ClusterStatusReport, NotLeader bool, LeaderHost string, QuorumProj *QuorumProjection`. Broker wires these via an **optional `ClusterAdminBackend` interface** in `adminsock.Backend` (nil in production → "cluster mode not enabled"; harness sets it via a D7 test seam). adminsock takes an interface — never imports `internal/cluster` (cycle-clean).

**Non-leader fail-fast naming the leader (NO forwarding)**: backend checks `IsLeader()`; else `{Error:"not_leader", LeaderHost:<roster public_host of LeaderWithID()>}`; CLI prints `re-run on <host>` + exits non-zero. Mid-election (empty leader) → `"no leader (election in progress); retry"`.

**`--json` + exit codes**: reuse `json.MarshalIndent` (`proxy.go:146`); `0=HEALTHY-HA,1=DEGRADED-writable,2=read-only/quorum-lost,3=force-single` via a single `healthExitCode()` SSOT (table-tested). TTY typed-confirm reuses `confirmProxyOn` (`proxy.go:287`). **force-single/recover NEVER honor `--yes`**; **drain at F==0 NEVER honors `--yes`** (typed node_id mandatory).

---

## G. drain / retire (online; reuses D6 rehome + D5 AllAtTarget)

1. **Quorum-projection guard FIRST** — projected **serving-set** fault tolerance (fires whenever the op drops serving F to 0, incl. plain-drain-at-N=2 per OQ-6). Render `"after op: N voters, quorum=K, tolerate F failures"`. **F==0 → TTY + typed node_id confirm + persistent-severe predicate (loud log at D7); `--yes` rejected.**
2. **AllAtTarget stream guard (retire only)** — `jsstream.AllAtTarget` (D5-staged) over the node's home-owned `history-<sid>`/`OBJ_xfer-<sid>`; **hard-refuse retire** if any below target.
3. **`OpClusterDrainSet`** raises `broker_draining` + `drain_deadline = now + notice` (`--now` = now).
4. **Migrate exposes BEFORE stopping (D6 rehome)** — for each ALLOCATED expose homed here: rebuild-ON → `Propose(port.PlanReassignHome(port, newEligibleVoter))` → agent self-rehomes on next reconnect (D6 §7.4). **rebuild-OFF → enumerate by name/port in the confirm prompt + require typed node_id** (destroying multiple rebuild-OFF behind one y/N is a footgun — show the list, not a count). Progress: `"draining: 3/5 migrated, 1 rebuild-OFF pending"`.
5. **transfer-leader FIRST if this node is leader** (`LeadershipTransferToServer(caught-up follower)`) before any self-removal.
6. **`--retire` → §8.1 order**: `PlanPhase(DRAINING→RETIRING)` → `RemoveServer(id)` → `PlanRemove(id)`. Each step's failure leaves a status-visible stuck phase + next-step command.
7. `--abort`: clear `broker_draining`, `PlanPhase(DRAINING→VOTER)`. `--now`: skip notice.
8. **retire honesty**: print + status-render "retired node credentials remain cryptographically valid until rotation" (account.nk/CA rotation runbook = D7 doc; tooling = D9).

---

## H. force-single / recover offline

`internal/clusteroffline` (orchestration) → `internal/cluster.RecoverSingleNode(dir, selfID)` (raft + FSM wiring). Opens `${DataDir}/raft/raft.db` (raftboltdb, in `internal/cluster`) + `${DataDir}/tether.db` (storage.OpenWAL).

**Disk interlock (two mechanisms, §B-7):** (a) `flock(LOCK_EX|LOCK_NB)` on `${DataDir}/tether.lock` via `golang.org/x/sys/unix.Flock` (bars two offline runs); (b) **probe `raft/raft.db` BoltDB exclusive openability** — not openable (live daemon holds it) → HARD-REFUSE "daemon running; `systemctl mask` + stop first". flock auto-releases on process exit (correct for a kill-9'd daemon).

**force-single preconditions (in order, all before disk mutation):**
- **(lock)** (a)+(b).
- **(b) empty-state refuse** — `raft.HasExistingState(...)==false` OR `tether.db` missing/zero OR no `cluster_meta.applied_index` → REFUSE "no existing raft state, would build empty cluster losing all data".
- **(d) peer-reachable HARD-REFUSE (BEFORE (c) mutates)** — `--confirm-peers-dead` must enumerate **every** non-self roster node (refuse if one omitted); for each, `net.DialTimeout` its `raft_addr` — **any completed TCP connection → HARD-REFUSE** (§B-8).
- **(c) reconcile + config rewrite = `raft.RecoverCluster({self})` ITSELF (§B-2)** — do NOT hand-roll an `ExecCommand` pre-replay. Recovery point = local `LastIndex()`; uncommitted tail committed-by-fiat with a loud log.
- Raise `force_single_active` persistent severe into the (now single-node) `tether.db`.
- **TTY + typed self node_id, NO `--yes`.**

**recover --dump-divergent (rejoining peer, before `cluster add`):**
- (lock) (a)+(b); **TTY + typed wiped node_id, NO `--yes`**.
- Dump every Apply-owned table's rows to `<file>` `O_EXCL` 0600 (never overwrite a prior dump), then **`fsync` the file AND the containing dir, re-`stat` size>0** (durability before wipe). Print "N rows (sessions/ports/audit) existed only on this node, permanently discarded, saved `<file>` forensic-only, not auto-mergeable".
- **dump-write-or-fsync-fail → REFUSE wipe**; else wipe `raft/`+`tether.db` so `cluster add` re-provisions clean.

---

## I. status / doctor double-view (§17)

**(A) ctl/NATS view (default, laptop)** — asks each NATS-reachable broker for its self-report over a broker-side `tether.v2.cluster.status` responder (in `internal/broker`, build-and-prove-gated + guard-banned activation token; keeps `internal/cluster` nats-free). Each broker self-reports its own phase/role/applied-lag/`now-LastContact`/account.nk-fp/stream actual-target. **NEVER dials :7400.** `reach_source:"self-report"`; NATS-unreachable = UNKNOWN, never DEAD; exit 2 only from a positive read-only self-report (§B-9). Banner: "NATS view, not a direct probe — not a basis for force-single."

**(B) broker-local offline view (`--offline`)** — disk raft config + admin socket, zero NATS, **directly mTLS-pings each peer over :7400** (`clusterTLSConfigs`; the only mode that probes :7400). `reach_source:"raft-ping"`.

**Columns**: `node_id|name|phase|role(leader/voter/learner)|applied-lag|last-contact|account.nk-fp(Y/N)|stream-replicas(actual/target)` + one bold health line + per-degraded-state next-step pointer. `role` from `RaftConfiguration()`, phase from roster, cross-rendered → `INCONSISTENT` on mismatch. `learner` is renderable-but-never-produced in D7 (AddVoter sets Voter suffrage immediately — do NOT assert a learner fixture).

**`--json` (versioned, with `reach_source`):**
```json
{"schema_version":1,"view":"ctl-nats","health":"HEALTHY_HA","exit_code":0,
 "leader_node_id":"br-1","banner":"...","next_step":"",
 "nodes":[{"node_id":"br-1","name":"alpha","phase":"VOTER","role":"leader",
   "applied_lag":0,"last_contact_secs":0,"account_nk_match":true,
   "stream_replicas":{"actual":3,"target":3},"reachable":true,"reach_source":"self-report"}]}
```
**5 render fixtures** (`test/d7/fixtures/`, byte-stable golden): HEALTHY-HA(0), DEGRADED(1 CATCHING_UP), quorum-lost(2 banner names the go-to-broker-host), force-single(3), joining(1 VOTER_ADD_FAILED cleanup pointer + phase/role fork rendered).

---

## J. Test plan (NON-vacuous; each states the assertion that FAILS if the mechanism is broken)

**§13.11 membership (gated `d7_integration`, real ≥3-node routed-NATS+mTLS-raft, `-race`):**
- **AddVoter-fail-after-Phase-1**: inject a real AddVoter failure (bad/closed `raft_addr`). FAILS-if-broken: phase≠`VOTER_ADD_FAILED`, OR raft config has a voter, OR status doesn't render the stall. Retry → idempotent (query a **follower's** DB: no duplicate row).
- **Forged-sig poison-skip**: mint a sig with the **wrong nkey**, drive through real 3-node raft. FAILS-if-broken: any **follower's** `cluster_nodes` has the row, OR any replica panicked, OR `applied_index` didn't advance, OR the follower fails to apply the **next legitimate** entry. (Read the follower DB — the leader pre-verified; the cross-node proof is on followers.) Positive control: valid sig commits identically on all 3.
- **Catch-up stalled (command-domain non-vacuous)**: AddVoter ok, throttle the joiner's AppendEntries so it never reaches the barrier within max-wait, **with a config-change entry between the barrier and the joiner's command stream**. FAILS-if-broken: the command-domain predicate spuriously passes/never-passes, OR `catch_up_stalled` doesn't derive, OR the node isn't a raft voter.
- **No-silent-fork kill-leader-mid-promotion (NOT a model-check)**: kill the leader between the AddVoter future returning and the phase-bump committing on followers; run status against the new leader. FAILS-if-broken: the leader-startup reconciliation leaves `{PENDING∧voter}` un-completed, OR a cleanup RemoveServers the mid-promote node, OR status mis-renders.
- **Repeat-add same node_id + across-leadership-transfer**: phase machine idempotent, no double-AddVoter, no re-sign.

**§13.12 force-single→recover (gated, `-race`):**
- Real 3-node → kill 2 → real offline path on the survivor's disk: (b) empty-disk refuse on a wiped dir; (d) HARD-REFUSE when a "dead" peer's :7400 still accepts TCP (bring one back); peers truly dead → `RecoverCluster` succeeds, restart as N=1 writable.
- **Restart-no-double-apply (the load-bearing one)**: survivor's BoltDB log has committed entries beyond its SQLite `applied_index`. FAILS-if-broken: after force-single + restart, `applied_index` didn't reach `LastIndex()`, OR a non-idempotent op double-applied (assert exact row counts), OR a subsequent FSM restart re-applies (idempotent-skip must fire).
- **Uncommitted-tail divergence**: survivor has a tail the cluster never committed (kill leader mid-AppendEntries). FAILS-if-broken: the apply-by-fiat policy isn't honored / not loudly logged.
- **Offline disk-lock = TWO real `os/exec` subprocesses** (same-process two-goroutine flock is vacuous — flock is per open file description). FAILS-if-broken: the second doesn't get refused. **+ fd-baseline gate** (a leaked flock fd is invisible to NumGoroutine).
- **BoltDB-lock probe vs live daemon**: a fake daemon holds `raft.db` → force-single refuses.
- **recover dump durability**: read-only dump dir → wipe refused; success → fsync'd 0600 file, content asserted.

**§13.13 status --json + exit codes (cheap, `make test`, no cluster):**
- 5 golden fixtures × `healthExitCode` mapping × `--json` schema stability.
- **No-:7400-from-laptop behavioral (NOT fixtures-only)**: a peer whose :7400 is blackholed but NATS self-report reachable. FAILS-if-broken: ctl view reports it `UNREACHABLE`/emits `quorum_lost` instead of `reachable:true reach_source:self-report`.
- **drain `--yes` at F==0 rejected**: `drain --retire --yes` at projected F==0 → exit non-zero, roster + raft config unchanged; same at F≥1 → `--yes` honored.
- Non-leader fail-fast NAMES a resolvable host; mid-election → "no leader, retry".

**Placement / gates:**
- **`make test` (cheap)**: sqlbake of the new ops; PlanUpsert/Phase/Remove literals + monotonic/CAS guards; `auth.VerifySignature` forged/wrong-key/malformed table; the `errAppliedRejected` fsm unit (verify-fail = applied_index advances + no row + no panic, on a plain DB); phase-CAS RowsAffected==0; `clusteroffline` (b)-precondition + flock unit; status render + 5 fixtures + exit codes + no-:7400 behavioral; drain-F==0-`--yes`-reject; guard `TestD7ProductionWiresNoCluster` + bidirectional self-check; import-cycle guards (`clusternodes` stays nats+raft-free; **`cluster` stays nats.go-free after the auth edge**).
- **gated `d7_integration` (`TestD7Matrix -race`, OUT of `make test`)**: the multi-node membership + force-single + status drills (routed-NATS+raft starves `make test` parallelism — the D5/D6 lesson).
- **e2e matrix**: a `TestD7` subtest with the **cheap** membership-happy-path + status exit-code in the always-on net; the heavy drill stays gated.
- **Concurrency (no goleak)**: orchestrator goroutines (catch-up poll, drain migration loop, AddVoter future waits), the offline flock, the :7400 probe conns — all under `-race` + in-repo `runtime.NumGoroutine` poll-with-tolerance + **fd-baseline** gate. The fd gate is load-bearing for leaked BoltStore handles + :7400 dial conns.

---

## K. Risk-ranked task ordering (riskiest/most-coupled first)

1. **Doc-first amendments B-1..B-10** — block everything. State-machine-changing: B-2 (offline recovery point), B-3 (poison-skip not panic), B-4 (leader-startup reconciliation), B-5 (catch-up domain).
2. **`errAppliedRejected` FSM plumbing + `clusterNodeUpsertApplier` (verify-then-exec) + `canonicalJoinMsg` + `internal/auth` import** — the only new code reachable from `FSM.Apply` that could wedge replicas; must be poison-skip, deterministic, L-2-clean. Add the `cluster`-stays-nats.go-free import-cycle guard here.
3. **Migration 0013 + the 3+1 ops' Plan helpers (`clusternodes/plan.go`) + phase-rank/predecessor CAS guards.**
4. **Node membership wrappers + orchestrator phase machine + leader-startup reconciliation + idempotent retry across leadership change** — the no-silent-fork core.
5. **`internal/clusteroffline` + `internal/cluster.RecoverSingleNode` + flock + BoltDB-lock probe + `RecoverCluster`-driven reconcile + :7400 TCP-liveness gate + dump-fsync-before-wipe** — genuinely-real, irreversible, highest blast radius. Build the 3-node drill here.
6. **Guard `test/d7/regression_test.go`** (new banned tokens incl. field-write + struct-literal forms; `internal/clusteroffline` excluded; `RecoverCluster(`/`raftboltdb.New(` banned in cmd/broker/agent scope; bidirectional self-check) + import-cycle re-assertions — land alongside 2-5 as the build-and-prove tripwire.
7. **drain/retire orchestration** (D6 rehome + D5 AllAtTarget + serving-set quorum guard + rebuild-OFF enumerated confirm).
8. **adminsock cluster ops + `ClusterAdminBackend` (nil in prod) + non-leader fail-fast naming.**
9. **status/doctor double-view + `reach_source` + exit-code SSOT + 5 fixtures + no-:7400 rule.**
10. **cobra `cluster.go`/`cluster_offline.go` + `--json`/TTY confirm + `rotate-tunnel-cert` + `sign-join`.**
11. **e2e matrix entry + runbook docs (force-single, account.nk/CA rotation-for-retire).**

---

## L. Key files

`internal/cluster/{command.go,clustermeta.go,fsm.go,read.go,node.go,membership.go(new),offline.go(new RecoverSingleNode)}`; `internal/clusternodes/{read.go,plan.go(new)}`; `internal/auth/nkey.go:55` (FSM-reachable verify, cycle-free); `internal/port/plan.go:163` (CAS reference); `internal/adminsock/{protocol.go,server.go}`; `internal/storage/migrations/{0008_cluster_nodes.sql,0013_cluster_nodes_join_pop.sql(new)}`; `internal/clusteroffline/(new)`; `internal/broker/clusteradmin.go(new, build-and-prove orchestrator + status responder, guard-excluded)`; `cmd/tether/{cluster.go,cluster_offline.go}(new)`; `test/d7/(new harness + regression_test.go + fixtures/)`; `test/d6/regression_test.go` (guard pattern to extend); `cmd/tether/serve.go:128` (verified byte-unchanged-clean).
