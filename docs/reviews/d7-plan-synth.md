Confirmed: `serve.go` sets `AdminSocketPath` (line 128) but never references `cluster.New`/`AttachClusterSeam` — it is already clean for D7's build-and-prove. I now have everything. Here is the synthesized, implementation-ready plan.

---

# D7 Plan (synthesized candidate) — cluster lifecycle + safe membership + force-single escape

> Synthesizer artifact for the main-process adjudicator. Resolves the 4 drafts + 4 critiques against verified source. Where a draft was factually wrong about the codebase, it is called out. Where a fork is genuine, it is escalated in §L, not silently picked.

---

## A. Executive summary + scope boundary

**D7 ships three tiers with opposite provability** (Draft 4's framing, verified correct):

| Tier | What | Real in production at D7? | Proven by |
|---|---|---|---|
| **(A) ONLINE membership/drain/status** — `cluster add/remove/drain/retire/transfer-leader/rotate-tunnel-cert`, ctl/NATS `status`/`doctor`, online adminsock ops, the new `ClusterNode*` ops, `Node.AddVoter`/`RemoveServer`/`LeadershipTransferToServer` wrappers, the two-phase orchestrator | **NO.** `serve.go` stays BYTE-UNCHANGED (verified: it sets `AdminSocketPath` at serve.go:128, never touches `cluster.*`); production broker constructs no `cluster.Node`. Online cluster subcommands exist in the binary but **fail-fast "cluster mode not enabled"** when the adminsock cluster backend is nil. | gated `test/d7` harness (`//go:build d7_integration`, run under `TestD7Matrix -race`), extending the `test/d6` multi-broker routed-NATS + mTLS-raft + real-tunnel shape with a **dynamic join** path |
| **(B) OFFLINE force-single/recover + offline `status`** — operate on disk `raft/` + `tether.db` with a STOPPED daemon, no cluster.Node, no NATS | **YES, genuinely real.** This is the escape hatch; it has no "cutover." D9 only adds `--from-existing`. | gated `test/d7` runs them on **harness-created real disk** (spin a real cluster, kill it, run the offline tool against the dead node's on-disk files) |
| **(C) inert-until-D9** — the `serve.go`/`broker.New` lines that would construct `cluster.Node`, register the adminsock cluster backend, and take the daemon-side disk lock | deferred to D9 | — |

**Explicitly DEFERRED to D8/D9 (with reason):**
- **Replicated alert store** (`alert ls/ack` store-backed rows) → **D8b** (§16-D5 L155/L413 already owns the store). D7 ships only **client-synthesized banners** (`force_single_active`, `quorum_lost`) it needs for status exit codes, plus the one legitimate D7 alert write: `force_single_active` written by the *offline tool* into the *single-node disk DB* (Draft 3 §2.8, correct). drain's "persistent severe" at F==0 is a **predicate + TTY confirm + log** at D7, not a replicated row.
- **Daemon-side disk lock acquisition** (`broker.New`/`serve.go` taking `tether.lock`) → **D9** (touching production startup violates build-and-prove). D7's offline-vs-live-daemon interlock is a **BoltDB-lock probe** (see §H, Critiques 1/2/3 consensus).
- **`cluster init --from-existing`** (live broker → cluster migration) → **D9**. D7 ships the `init` skeleton that errors "use the D9 path"; harness bootstraps directly.
- **account.nk / cluster-CA rotation tooling** (full-fleet re-provision after retire) → **D9**. D7 retire ships the **runbook doc + the status warning** "retired node credentials remain cryptographically valid until rotation" (§8.3 L273 honesty requirement).

---

## B. Doc-first amendments (ready to apply to `docs/distributed-broker-architecture.md`)

These MUST land before code (§3 hard constraint). Each is an implementation ruler the current prose leaves wrong or ambiguous.

**B-1. §19-D7 (L607-611): add a `### D7 范围定稿（先改正文）` block** mirroring the D5/D6 blocks (L596/L604). State: build-and-prove (cutover=D9); `serve.go` byte-unchanged; production broker constructs no `cluster.Node`; online cluster commands fail-fast "cluster mode not enabled"; **offline force-single/recover are genuinely real**; the guard `TestD7ProductionWiresNoCluster` token list + `internal/clusteroffline` package boundary; migration 0013 decision; the gated `test/d7` harness.

**B-2. §8.4(c) (L291-292) — REWRITE the two-store reconcile (the most important amendment; Critiques 1/2/3/4 all blocking here).** Current prose: "把 BoltDB 已提交到 **commitIndex** 的 entry 应用进 SQLite". **This is unimplementable offline** (verified: `commitIndex` is volatile raft state, never persisted to BoltDB; only `currentTerm`/`votedFor` are in the StableStore). And it would **double-apply**: `raft.RecoverCluster` (verified api.go:399-407) **itself replays every log entry `snapshotIndex+1 .. lastLogIndex` through `fsm.Apply`** (our idempotent, applied_index-advancing FSM), so a hand-rolled `ExecCommand` pre-replay is redundant-or-corrupting (verified: `ExecCommand` command.go:143 runs `genericExecApplier` and does **NOT** advance `applied_index` — that's `applyCommand` in fsm.go). **New prose (adopt Draft 2's `LastIndex` recovery point + the §18.2.5(c) hedge the doc already offers at L460):**
> force-single 的两存储调和 = **`raft.RecoverCluster({self})` 本身驱动**：它把本地 BoltDB log `[snapshotIndex+1 .. LastIndex()]` 经 `fsm.Apply`（幂等、同 txn 推进 `applied_index`）前向重放进 SQLite，再写一份单成员快照 + config entry。**恢复点 = 本地 `LastIndex()`**（commitIndex 离线不可得；运维已声明 peers 死亡，本节点的整条 log 即权威时间线，其未提交尾部被 force-single **按既成事实提交**——loud log 记此）。**绝不**再手搓 `ExecCommand` 前置重放（会双应用非幂等 op）。`--dump-divergent`（recover 工具、回归 peer 上跑）取证的是**该回归节点 pre-wipe SQLite 的行**，诚实声明仅"此节点曾有、新时间线可能没有"，不可自动合并。

**B-3. §8.1 (L260-263) — pin the join-PoP message binding + the verify-fail verdict + persistence (Critiques 1/2/3/4).**
- **Signed message = domain-separated canonical tuple** `"tether-cluster-join-v2\0" || node_id || "\0" || node_ident_pub || "\0" || join_nonce` (Draft 1's binding; Critique 1 m4 correct that Draft 2's bare-nonce is replay-weak across nodes). Per the security-pragmatic feedback, binding `node_id` is the load-bearing part; cluster-id binding is optional (see §L OQ-5).
- **Verify inputs travel in `cmd.Aux`** (apply-inert JSON `{node_id, node_ident_pub, join_nonce, join_sig}`) AND are persisted as `cluster_nodes` columns. The Aux copy is **mechanically required**: an applier cannot introspect baked SQL literals to recover the bytes to verify (Critiques 2/3/4). The columns survive snapshot truncation for forensic audit + retry reconstruction (Critique 1/3 M4 — Draft 3's wire-only is wrong). A cheap **Aux-vs-Body cross-check** (Aux values == baked column literals) guards a leader splicing mismatched values.
- **Verify-fail verdict = POISON-SKIP, NOT panic (BLOCKER — reject Draft 1's recommendation).** Verified fsm.go:111: an applier returning an error → retried 3× → **`panic`**. A forged committed entry verified deterministically-false on every replica would **crash-loop the whole cluster on every boot's log replay** — a one-shot remote brick from any (compromised/buggy) leader, which §18.2.4 explicitly accepts as in-scope. The existing never-wedge invariant (§3.7/§2.8) forbids this. New prose:
> follower `Apply(ClusterNodeUpsert)` 复算 join PoP 失败 = **POISON 跳过**（推进 `applied_index`、不执行 roster UPSERT、loud log），**绝不返回 applier 错误**（那会触发 §3.7 重试→panic 的 wedge）。

**B-4. §8.1 — add a NEW `leader-startup membership reconciliation pass` requirement (Critique 1 B3 — the real no-silent-fork fix).** The status renderer is a *display*, not a safety property. New prose:
> 新 leader 上任后、服务任何成员命令前，对每个 `cluster_nodes` 行**幂等调和** roster `phase` 与 `raft.GetConfiguration()`：`{PENDING ∧ raft-voter}`（AddVoter 提交后旧 leader 崩在 phase 推进前）→ 前向补 `CATCHING_UP`，**绝不** RemoveServer；`{VOTER_ADD_FAILED ∧ raft-voter}`（AddVoter 超时≠失败，config entry 实已提交——D4 "committed but ack lost" 同类歧义）→ 重查 config，若实为 voter 则推 `CATCHING_UP` 而非信旧记录；`{roster=∅ ∧ raft-voter}` → loud `INCONSISTENT`、拒自动动作、指 `cluster doctor`。

**B-5. §8.2 (L269) — clarify the catch-up barrier domain (Critique 1/4 M5/M6 fork). The doc's `applied_index >= barrier` is COMMAND-domain; §7.2 L218 already established it is incompatible with raft `CommitIndex` (all-entry domain).** Add a D7 note that pins the predicate as **command-domain `applied_index` ≥ a command-domain barrier**: the barrier is the leader's **`AppliedIndex()` (SQLite cursor, command-domain) captured under `VerifyLeaderRead` at AddVoter time**, NOT `CommitIndex`. The new voter is caught up when its local command-domain `applied_index ≥ that barrier`, sustained for a fixed wallclock, max-wait → `catch_up_stalled`. This keeps both sides of the comparison in the command domain (the HA "0 committed business loss" claim is about business state). See §L OQ-1 — this is a genuine doc change touching a state-machine contract and the adjudicator should sign off.

**B-6. §8.2 / §8.3 — `catch_up_stalled` is a DERIVED display state, not a 7th phase (Critique 2/3 M3/M7).** Verified: the 0008 `phase` CHECK has exactly 6 values; writing `phase='catch_up_stalled'` would fail the CHECK → applier error → panic. New note: it stays `phase=CATCHING_UP` with a stall derived from `phase_changed_at` + max-wait (or carried in a `voter_add_error` column).

**B-7. §8.4(a) / §18.2.5(a) (L276, L460) — the disk lock is TWO mechanisms (Critiques 1/2/3 M1/B3).** Current prose "与 daemon 共享同一把磁盘 advisory lock" is **false at D7**: production daemon never constructs `cluster.Node`, so it holds no `tether.lock`. New prose:
> offline 工具 (a) 取 `flock(2)` `${DataDir}/tether.lock`（防两 offline 实例并发）**且** (b) **探测 `raft/raft.db` 的 BoltDB 排他锁**（活 daemon 开 BoltStore 即持有它）——`raft.db` 不可排他打开 → HARD-REFUSE "daemon 仍在运行；先 `systemctl mask` 并停它"。daemon 端取 `tether.lock` 是 **D9**（改 `serve.go`/`broker.New` 属 cutover）。`SetMaxOpenConns(1)` 是进程内串行化，提供**零**跨进程保护，非替代。

**B-8. §8.4(d) (L293) — the peer-reachable HARD-REFUSE gate is TCP-liveness, not just mTLS handshake success (Critique 1/3 M3/B4).** New prose: 对每个 `--confirm-peers-dead` peer 的 `raft_addr` **完成 TCP 连接即 HARD-REFUSE**（peer 接受 TCP = 活着，即便随后 TLS 因证书轮换/吊销失败也是活的）；用 `clusterTLSConfigs` 客户端配置做握手仅为 mode-B status **展示** liveness，**安全闸取更保守的 TCP-completes 判定**。`--confirm-peers-dead` 须列出 disk roster 里**每个**非 self 节点（漏一个→那个仅被分区的节点会脑裂），漏列即拒。

**B-9. §17 (L439-441) — add the `--json` stable schema (versioned) + the `reach_source` discriminator + the 5 render fixtures.** Pin: ctl/NATS view's `reachable:false` MUST carry `reach_source:"self-report"` (didn't hear over NATS) vs `"raft-ping"` (offline mode B, actually pinged + dead); **NATS-unreachable contributes UNKNOWN, never DEAD**; exit code 2 (`quorum_lost`) is emittable in NATS view **only from a positive self-report of read-only/no-quorum**, never from absence of reports (Critique 3 M7 — the false-quorum-lost → wrong-force-single chain §17 exists to prevent). Fixtures: HEALTHY-HA(0) / DEGRADED(1) / quorum-lost(2) / force-single(3) / joining(1).

**B-10. §15 / §8.3 — `cluster rotate-tunnel-cert <node_id>` is a D7 online command (Critique 1/2 m2, §19-D6 L604 already assigns it to D7).** It rides the existing `ClusterNodeUpsert` op writing `cert_fp`/`cert_fp_prev`/`cert_fp_valid_until` (no join-PoP path; cert rotation is not a new admission). Keep minimal; reuse D6 `cert_pins` `VerifyConnection` machinery (先父后子).

---

## C. Migrations / schema

**Migration 0013 — `0013_cluster_nodes_join_pop.sql` (one migration, ADD COLUMN only, no rebuild — preserves PK + UNIQUE(name) + idx_cluster_nodes_phase):**

```sql
-- 0013 — D7 §8.1 join-PoP + half-success bookkeeping. Leader-baked literals only
-- (NO CURRENT_TIMESTAMP, §3.4). cluster_nodes exists since 0008; phase CHECK already
-- carries all 6 values (no enum change). Columns nullable: the D9 init --from-existing
-- self-row + pre-D7 grandfathered rows are admitted by bootstrap, not a challenge.
ALTER TABLE cluster_nodes ADD COLUMN join_nonce       TEXT;   -- leader-issued, baked; followers re-verify from row+Aux
ALTER TABLE cluster_nodes ADD COLUMN join_sig         TEXT;   -- hex(ed25519 sig) over the canonical join tuple
ALTER TABLE cluster_nodes ADD COLUMN voter_add_error  TEXT;   -- VOTER_ADD_FAILED detail / 'catch_up_stalled'; NULL = none
ALTER TABLE cluster_nodes ADD COLUMN phase_changed_at TIMESTAMP; -- leader-baked literal; "stuck in phase X for T" + stall derivation
```

**Decisions resolved against the critiques:**
- **Nullable, NOT `NOT NULL DEFAULT ''`** (Critique 2 M2): an empty-string sig must be impossible-to-pass, not conventionally skipped. `nkeys.Verify('', '')` fails closed; nullable keeps the D9 self-row legal.
- **No separate `cluster_join_nonces` table / `OpJoinNonceIssue` op** (REJECT Draft 3 — Critiques 1/2/3/4 unanimous M1/M2/M4). The nonce is single-use by binding to exactly one `cluster_nodes` PK row; a second admission of the same node_id is the phase-machine idempotent path. Draft 3's issue-op adds an untested orphan-nonce state + a second committed entry per join. Nonce single-use is enforced **leader-local pre-propose** with the honest doc note that it is a consistency property, not a security boundary (§18.2.4 accepts a compromised leader proposes anything anyway) — see §L OQ-5.
- **No `raft_config_voter` mirror column** (Draft 2 §3, correct): the canonical raft config is `raft.GetConfiguration()`; persisting a mirror invites the exact DB-vs-raft fork §8.1 forbids. status joins live config against the roster row; the fork is *rendered*, never *stored*.
- **No drain-flag schema** — drain deadline/state rides a `cluster_meta` KV via a dedicated op (§D), since `cluster_meta` exists since 0009 and the existing audit-cursor monotonic-UPSERT pattern fits.

---

## D. Ops + Plan/Apply

**Three new ops** in `internal/cluster/command.go` (`const` block + `knownOps` map + `defaultAppliers()`), plus a drain-meta op. Plan helpers co-locate in **`internal/clusternodes/plan.go`** (its package doc L8 already declares the intent; verified raft-free leaf). `commandVersion` stays **2** (no envelope shape change; Aux already exists since D4).

### D.1 `OpClusterNodeUpsert` — Phase-1 roster admission (the ONE custom applier)
- **Plan (`clusternodes.PlanUpsert`)**: bakes `INSERT INTO cluster_nodes(...) VALUES(<all literals>, phase='JOIN_VERIFIED_PENDING_VOTER', join_nonce=<lit>, join_sig=<lit>, phase_changed_at=<LitTime>) ON CONFLICT(node_id) DO UPDATE SET cert_fp=…, …` with a **monotonic phase-rank guard** in the `DO UPDATE ... WHERE` (encode the 6 phases as an integer ladder; refuse to regress a more-advanced phase back to PENDING via a stale re-add — the membership analogue of `PlanReassignHome`'s `epoch < new` CAS, verified port/plan.go:189-192). Also carries `{node_id, node_ident_pub, join_nonce, join_sig}` in `cmd.Aux`.
- **Custom applier `clusterNodeUpsertApplier` (the architectural exception, registered in `defaultAppliers()` instead of `genericExecApplier`):**
  1. decode `cmd.Aux` → `{node_id, identPub, nonce, sig}`
  2. `msg := canonicalJoinMsg(node_id, identPub, nonce)` (domain-separated)
  3. cross-check: Aux values equal the baked Body column literals (defense-in-depth)
  4. `if auth.VerifySignature(identPub, msg, sig) != nil { log loud; return appliedRejected }` — **the new plumbing (see below)**: poison-skip the op SQL, advance applied_index, never error/panic
  5. else `genericExecApplier{}.ApplyTx(tx, cmd)` execs the baked UPSERT
- **L-2 reachability, verified cycle-free:** the verify is `auth.VerifySignature` (verified nkey.go:55). `go list -deps ./internal/auth` confirmed **zero** nats.go/cluster/raft. So `internal/cluster` imports `internal/auth` directly — **no new package** (REJECT Drafts 3/4's `internal/joinpop`/`internal/clusterauth` as YAGNI; Critique 2 B3). `clusternodes` stays pure-SQL (only the Plan baking lives there; the verify is in `cluster`, reachable from `FSM.Apply`).
- **The required FSM plumbing (Critique 1/4 B2/B3 — verified mandatory):** today `applyCommand` reaches `appliedPoison` ONLY via `cmd==nil` at decode (fsm.go:91), upstream of the applier — a custom applier has **no way** to signal "skip op SQL but advance + commit applied_index" without erroring (→panic). **D7 adds a typed sentinel** `Applier.ApplyTx` may return `errAppliedRejected` (a package sentinel); `applyCommand` checks `errors.Is(err, errAppliedRejected)` BEFORE the retry loop and treats it exactly like the poison path (commit the applied_index advance, run no op, return `appliedRejected{index}`, loud log) — NOT as a transient error. This is a small, well-fenced fsm.go change, doc-amended into §8.1 + §2.8, with a negative-control test (§J).

### D.2 `OpClusterNodePhase` — transitions (genericExecApplier)
`UPDATE cluster_nodes SET phase=<LitText>, voter_add_error=<lit|NULL>, phase_changed_at=<LitTime> WHERE node_id=<lit> AND phase IN (<allowed predecessors>)`. The `phase IN (...)` predecessor guard = deterministic CAS (a stale ex-leader's disallowed transition is RowsAffected==0 no-op). Transitions: `PENDING→CATCHING_UP` (AddVoter ok), `CATCHING_UP→VOTER` (catch-up gate), `*→VOTER_ADD_FAILED`, `VOTER→DRAINING`, `DRAINING→RETIRING`, `DRAINING→VOTER` (abort).

### D.3 `OpClusterNodeRemove` — roster delete (genericExecApplier)
`DELETE FROM cluster_nodes WHERE node_id=<lit> AND phase IN ('RETIRING','VOTER_ADD_FAILED')` — only a node walked through removal phases (or a failed add) is deletable; a bare delete of a live VOTER is structurally refused. Idempotent (RowsAffected==0 re-delete).

### D.4 `OpClusterDrainSet` — drain flag (genericExecApplier, no migration)
`cluster_meta` UPSERT `key='draining:'+node_id, value=<LitTime(deadline)>` with a monotonic guard (later deadline wins; clear = delete), reusing the audit-checkpoint monotonic-UPSERT pattern. Rides `genericExecApplier`.

All four carry **no reqID** (leader-driven, no originating-broker key — like `OpPortReassignHome`; the CAS/phase guards are the idempotency anchors).

---

## E. Node membership wrappers (raft confined, L-2)

`n.raft` is unexported (verified node.go:71). Add to a new `internal/cluster/membership.go` (raft stays inside the package):

```go
func (n *Node) AddVoter(id raft.ServerID, addr raft.ServerAddress) error            // raft.AddVoter(id, addr, 0, applyTimeout); leader-gated
func (n *Node) RemoveServer(id raft.ServerID) error                                 // raft.RemoveServer(id, 0, applyTimeout)
func (n *Node) LeadershipTransferToServer(id raft.ServerID, a raft.ServerAddress) error
func (n *Node) RaftConfiguration() ([]raft.Server, error)                           // generalizes NumVoters' GetConfiguration; for status role + idempotency
func (n *Node) LeaderWithID() (raft.ServerAddress, raft.ServerID)                    // non-leader fail-fast naming
```

All leader-gate first (`n.raft.State()==Leader`, else `raft.ErrNotLeader` → `cluster.IsNotLeader`, verified node.go:256). `AddVoter`/`RemoveServer` are **raft config-change actions, not `Propose`** (§8.1 L259).

**Sequencing (orchestrator, broker-side `internal/broker/clusteradmin.go`):**
1. `Propose(PlanUpsert)` → Phase-1 committed (roster=`JOIN_VERIFIED_PENDING_VOTER`, PoP re-verified by all followers).
2. **only after step 1 returns nil** → `AddVoter(id, addr)`.
   - err → `Propose(PlanPhase(*→VOTER_ADD_FAILED, err))`. Consistent: roster=VOTER_ADD_FAILED, no raft voter.
   - ok → `Propose(PlanPhase(PENDING→CATCHING_UP))`.
3. catch-up gate (§B-5 command-domain barrier) → `Propose(PlanPhase(CATCHING_UP→VOTER))`.

**No-silent-fork = ordering + leader-startup reconciliation + render (Critique 1 B3):**
- Phase-1 commit strictly precedes AddVoter → `{phase=VOTER ∧ not-voter}` unreachable.
- The dangerous post-crash states `{PENDING ∧ raft-voter}` and `{∅ ∧ raft-voter}` are closed by the **leader-startup reconciliation pass** (§B-4), driven purely by the committed phase column + live `RaftConfiguration()`, never in-memory leader state — so a leadership change loses no progress and a naive cleanup never RemoveServers a mid-promote node.
- **Idempotent retry** (incl. across leadership transfer): re-add when phase≥PENDING skips Phase-1 (no re-sign, nonce consumed via the monotonic guard); `AddVoter` is idempotent by node_id.
- status cross-renders `phase` (roster) and `role` (RaftConfiguration), emitting bold `INCONSISTENT` for any mismatch — defense-in-depth backstop, not the primary guarantee.

---

## F. CLI + admin socket

**Cobra group `cmd/tether/cluster.go`** (+ `cmd/tether/cluster_offline.go`). Offline disk-surgery logic lives in **`internal/clusteroffline`** (may import raft — L-2 holds, raft stays in the cluster package family; Draft 4 §1 + Critiques 2/3 m1/M6). The cobra `force-single` subcommand calls `clusteroffline.ForceSingle(dir)` — a function call, not a banned token — so the guard scans cmd/broker/agent without false-positiving the real offline path.

| Subcommand | Transport | Production behavior (no cluster.Node) |
|---|---|---|
| `add <host> <node-pub>` / `remove <node_id>` | online adminsock → leader | "cluster mode not enabled" |
| `drain <node_id> [--retire\|--now\|--abort]` / `transfer-leader <node_id>` / `rotate-tunnel-cert <node_id>` | online adminsock | same |
| `status` / `doctor` `[--json] [--offline]` | NATS (default) OR disk (`--offline`) | NATS view works against reachable brokers; `--offline` is real |
| `node-pub` / `keygen` | local file (prints node-ident pubkey, nkey-prefix fat-finger guard) | real |
| `init [--from-existing]` | local | `--from-existing` errors "D9 path" |
| `force-single --confirm-peers-dead <ids...>` / `recover --dump-divergent <file>` | **offline disk** | **real** (daemon stopped) |
| `alert ls/ack` | NATS | D8b store; D7 client-synthesized banners only |

**Join-signature fetch:** operator pastes a token produced by `tether cluster node-pub`/a `sign-join` helper on the joiner (the joiner has the node-ident seed + shell, §10.6). This keeps "admin strictly local, no network bypass" cleanest and adds no new NATS subject/ACL — see §L OQ-4 (Critique 3 m2 leans paste-token; Critique 2 m4 cautions against an unrequested `sign-join` verb).

**New adminsock ops** (extend the verified-extensible envelope, protocol.go): `OpClusterAdd/Remove/Drain/Transfer/Status/RotateCert`. `Request` gains `NodeID, NodePub, Host, JoinToken, Retire/Now/Abort bool` (`omitempty`, byte-compatible with the existing 4 ops). `Response` gains `Cluster *ClusterStatusReport`, `NotLeader bool`, `LeaderHost string`, `QuorumProj *QuorumProjection`. The broker wires these via a new **optional `ClusterAdminBackend` interface** in `adminsock.Backend` (nil in production → ops return "cluster mode not enabled"; harness sets it via a D7 test seam). adminsock takes function values/an interface — never imports `internal/cluster` (cycle-clean).

**Non-leader fail-fast naming the leader (§8.1 L246, no forward — REJECT Draft 3 OQ-3 forwarding, Critique 2 M3):** the backend checks `IsLeader()`; if not, returns `{Error:"not_leader", LeaderHost:<host>}` where host = `LeaderWithID()` resolved via the roster `public_host`. CLI prints `re-run on <host>` + exits non-zero. Mid-election (empty leader) → `"no leader (election in progress); retry"` (Critique 4 m3 — test this).

**`--json` + exit codes:** reuse `json.MarshalIndent` (proxy.go:146). `0=HEALTHY-HA, 1=DEGRADED-writable, 2=read-only/quorum-lost, 3=force-single` via a single `healthExitCode()` SSOT (table-tested). TTY typed-confirm reuses `confirmProxyOn` (proxy.go:287). **force-single/recover NEVER honor `--yes`** (§8.1 L255). **drain at F==0 NEVER honors `--yes`** (typed node_id mandatory; Critique 4 M2 — test the bypass-rejection).

---

## G. drain / retire

Orchestrated (online), reusing D6 rehome + D5 AllAtTarget (先父后子):
1. **Quorum-projection guard FIRST.** Compute the projected **serving-set** fault tolerance. **Critique 2/3 M4/M6 fix:** the guard fires whenever the op drops serving fault-tolerance to 0, which the doc (L272) explicitly includes for **plain drain at N=2** ("含已处于 N=2 时再 drain"), not only `--retire`. Render `"after op: N voters, quorum=K, tolerate F failures"`. **F==0 → TTY + typed node_id confirm + persistent-severe predicate (logged at D7, D8b row later); `--yes` rejected.**
2. **AllAtTarget stream guard (retire only):** `jsstream.AllAtTarget` (D5-staged) over the node's home-owned `history-<sid>`/`OBJ_xfer-<sid>` streams; **hard-refuse retire** if any below target (data not yet redundant).
3. **`OpClusterDrainSet`** raises `broker_draining` + `drain_deadline = now + notice` (`--now` = now).
4. **Migrate exposes BEFORE stopping (D6 rehome):** for each ALLOCATED expose homed here, rebuild-ON → `Propose(port.PlanReassignHome(port, newEligibleVoter))` → agent self-rehomes on next reconnect (D6 §7.4). **rebuild-OFF → enumerate by name/port in the confirm prompt and require typed node_id (Critique 3 M6 — destroying multiple rebuild-OFF behind one y/N is a footgun; show the destruction list, not a count).** Progress: `"draining: 3/5 migrated, 1 rebuild-OFF pending"`.
5. **transfer-leader FIRST if this node is leader** (`LeadershipTransferToServer(caught-up follower)`) before any self-removal.
6. **`--retire` → §8.1 order:** `Propose(PlanPhase(DRAINING→RETIRING))` → `RemoveServer(id)` → `Propose(PlanRemove(id))`. Each step's failure leaves a status-visible stuck phase + next-step command.
7. `--abort`: clear `broker_draining`, `PlanPhase(DRAINING→VOTER)`. `--now`: skip notice.
8. **retire honesty:** print + status-render "retired node credentials remain cryptographically valid until rotation" (account.nk/CA rotation runbook = D7 doc deliverable; rotation tooling = D9).

---

## H. force-single / recover offline

Lives in **`internal/clusteroffline`** (imports raft directly — L-2 OK). Opens disk: `${DataDir}/raft/raft.db` (raftboltdb) + `${DataDir}/tether.db` (storage.OpenWAL).

**Disk lock (two mechanisms, §B-7):**
- (a) `flock(LOCK_EX|LOCK_NB)` on `${DataDir}/tether.lock` via `golang.org/x/sys/unix.Flock` (x/sys in go.mod) — bars two concurrent offline runs.
- (b) **probe `raft/raft.db` BoltDB exclusive openability** — a live production daemon holds it; not openable → HARD-REFUSE "daemon running; `systemctl mask` + stop first". This is the real offline-vs-live-daemon interlock at D7 (daemon-side `tether.lock` is D9). flock auto-releases on process exit (correct for a kill-9'd daemon).

**force-single preconditions (in order, all before disk mutation):**
- **(lock)** (a) + (b) above.
- **(b) empty-state refuse:** `raft.HasExistingState(logs, stable, snaps)==false` (verified api.go:462) OR `tether.db` missing/zero OR no `cluster_meta.applied_index` → REFUSE "no existing raft state, would build empty cluster losing all data".
- **(d) peer-reachable HARD-REFUSE (BEFORE (c) mutates):** `--confirm-peers-dead` must enumerate **every** non-self roster node (refuse if one omitted). For each, dial its `raft_addr` (:7400); **any completed TCP connection → HARD-REFUSE** "peer alive → split-brain" (§B-8; a TLS-rejected-but-TCP-accepting peer is still alive).
- **(c) reconcile + config rewrite = `raft.RecoverCluster({self})` ITSELF (§B-2, the BLOCKER fix):** do NOT hand-roll an `ExecCommand` pre-replay. `RecoverCluster` restores the newest snapshot, replays `[snapshotIndex+1..LastIndex()]` through the idempotent applied_index-advancing `fsm.Apply`, then writes a single-server config + snapshot. Recovery point = local `LastIndex()`; the uncommitted tail is committed-by-fiat with a loud log.
- Raise `force_single_active` persistent severe into the (now single-node) `tether.db`.
- **TTY + typed self node_id, NO `--yes`.**

**recover --dump-divergent (rejoining peer, before `cluster add`):**
- (lock) (a)+(b).
- **TTY + typed wiped node_id, NO `--yes`.**
- Dump every Apply-owned table's rows to `<file>`, then **`fsync` the file AND the containing dir, re-`stat` size>0**, `O_EXCL` 0600 (Critique 3 M5 — durability before wipe; never overwrite a prior dump). Print "N rows (sessions/ports/audit) existed only on this node, permanently discarded, saved `<file>` forensic-only, not auto-mergeable".
- **dump-write-or-fsync-fail → REFUSE wipe.** Then wipe `raft/` + `tether.db` so `cluster add` re-provisions clean.

---

## I. status / doctor double-view

**Two modes, never mixed (§17):**

**(A) ctl/NATS view (default, laptop):** asks each NATS-reachable broker for its self-report over a broker-side `tether.v2.cluster.status` responder (broker-side handler, build-and-prove-gated + guard-banned activation token; needs both `cluster.Node` reads + nats.go, so it lives in `internal/broker`, keeping `internal/cluster` nats-free — re-asserted by a D7 import-cycle guard, Critique 2 M2). Each broker self-reports its own phase/role/applied-lag/`now-LastContact`/account.nk-fp/stream actual-target. **NEVER dials :7400.** `reach_source:"self-report"`; NATS-unreachable peers = UNKNOWN, never DEAD; exit 2 only from a positive read-only self-report (§B-9, Critique 3 M7). Banner: "NATS view, not a direct probe — not a basis for force-single."

**(B) broker-local offline view (`--offline`):** disk raft config + admin socket, zero NATS, **directly mTLS-pings each peer over :7400** using `clusterTLSConfigs` (the only mode that probes :7400). `reach_source:"raft-ping"`.

**Columns (§17 L440):** `node_id|name|phase|role(leader/voter/learner)|applied-lag|last-contact|account.nk-fp(Y/N)|stream-replicas(actual/target)` + one bold health line + a next-step pointer per degraded state. `role` from `RaftConfiguration()`; phase from roster; status cross-renders both → `INCONSISTENT` on mismatch. `learner` is renderable-but-never-produced in D7 (AddVoter sets Voter suffrage immediately; do NOT assert a learner fixture — Critique 1/2 m3/M3).

**`--json` schema (versioned, with `reach_source`):**
```json
{"schema_version":1,"view":"ctl-nats","health":"HEALTHY_HA","exit_code":0,
 "leader_node_id":"br-1","banner":"...","next_step":"",
 "nodes":[{"node_id":"br-1","name":"alpha","phase":"VOTER","role":"leader",
   "applied_lag":0,"last_contact_secs":0,"account_nk_match":true,
   "stream_replicas":{"actual":3,"target":3},"reachable":true,"reach_source":"self-report"}]}
```

**5 render fixtures** (`test/d7/fixtures/`, byte-stable golden): HEALTHY-HA(0), DEGRADED(1, a node CATCHING_UP), quorum-lost(2, banner names go-to-broker-host), force-single(3), joining(1, VOTER_ADD_FAILED cleanup pointer + phase/role fork rendered).

---

## J. Test plan (NON-vacuous; each states the assertion that FAILS if the mechanism is broken)

**§13.11 membership (gated `d7_integration`, real ≥3-node routed-NATS+mTLS-raft, `-race`):**
- **AddVoter-fail-after-Phase-1:** inject a real AddVoter failure (bad `raft_addr`/closed port). FAILS-if-broken: phase≠`VOTER_ADD_FAILED`, OR raft config has a voter, OR status doesn't render the stall. Then retry → idempotent (no re-sign; query a **follower's** DB to confirm no duplicate row).
- **Forged-sig poison-skip (Critique 1/4 B2/B3 — the non-vacuous form):** mint a sig with the **wrong nkey** (real adversary holds a key), drive through real 3-node raft. FAILS-if-broken: any follower's `cluster_nodes` has the row, OR any replica panicked, OR `applied_index` didn't advance, OR the follower fails to apply the **next legitimate** entry. **Read the follower DB, not the leader's** (the leader pre-verified; the cross-node proof is on followers). Positive control: valid sig commits identically on all 3.
- **Catch-up stalled (Critique 4 M6 — non-vacuous domain):** AddVoter ok, then throttle the joiner's AppendEntries so it never reaches the barrier within max-wait, **with non-LogCommand entries (a config change) between the barrier and the joiner's command stream**. FAILS-if-broken: the command-domain predicate spuriously passes/never-passes, OR `catch_up_stalled` doesn't derive, OR the node isn't a raft voter.
- **No-silent-fork kill-leader-mid-promotion (Critique 4 B4 — NOT a model-check):** kill the leader between the AddVoter future returning and the phase-bump committing on followers; run status against the new leader. FAILS-if-broken: the leader-startup reconciliation pass leaves `{PENDING∧voter}` un-completed, OR a cleanup RemoveServers the mid-promote node, OR status doesn't render the transient correctly. (Reject Draft 1's `model-checks the reachable state set` — it proves the model, not the code.)
- **Repeat-add same node_id + across-leadership-transfer:** phase machine idempotent, no double-AddVoter, no re-sign.

**§13.12 force-single→recover (gated, `-race`):**
- Real 3-node → kill 2 → run the **real offline path** on the survivor's disk: assert (b) empty-disk refuse on a wiped dir; (d) HARD-REFUSE when a "dead" peer's :7400 still accepts TCP (bring one back); then peers truly dead → `RecoverCluster` succeeds, restart as N=1 writable.
- **Restart-no-double-apply (Critique 4 B2 — the load-bearing one):** survivor's BoltDB log has committed entries beyond its SQLite `applied_index`. FAILS-if-broken: after force-single + restart, `applied_index` didn't reach `LastIndex()`, OR a non-idempotent op double-applied (assert exact row counts), OR a subsequent FSM restart re-applies (idempotent-skip path must fire).
- **Uncommitted-tail divergence (Critique 4 B1):** survivor has a tail the cluster never committed (kill the leader mid-AppendEntries). FAILS-if-broken: the documented apply-by-fiat policy isn't honored / not loudly logged.
- **Offline disk-lock = TWO real `os/exec` subprocesses** (Critique 4 M4 — same-process two-goroutine flock is vacuous: flock is per open file description). FAILS-if-broken: the second doesn't get EWOULDBLOCK refuse. **+ fd-baseline gate** (a leaked flock fd is invisible to NumGoroutine — load-bearing).
- **BoltDB-lock probe vs live daemon:** a fake daemon holds `raft.db` → force-single refuses.
- **recover dump durability:** read-only dump dir → wipe refused; success → fsync'd 0600 file, content asserted.

**§13.13 status --json + exit codes (cheap, `make test` — no cluster):**
- 5 golden fixtures × `healthExitCode` mapping × `--json` schema stability.
- **No-:7400-from-laptop behavioral (Critique 3/4 M5/M7 — NOT fixtures-only):** a peer whose :7400 is blackholed but NATS self-report is reachable. FAILS-if-broken: ctl view reports it `UNREACHABLE`/emits `quorum_lost`, instead of `reachable:true reach_source:self-report`.
- **drain `--yes` at F==0 rejected (Critique 4 M2):** `drain --retire --yes` at projected F==0 → exit non-zero, roster + raft config unchanged; same at F≥1 → `--yes` honored.
- Non-leader fail-fast NAMES a resolvable host; mid-election → "no leader, retry".

**Placement / gates:**
- **`make test` (cheap):** sqlbake of new ops, PlanUpsert/Phase/Remove literals + monotonic guards, `auth.VerifySignature` forged/wrong-key/malformed table, the `errAppliedRejected` fsm unit (verify-fail = applied_index advances + no row + no panic, on a plain DB), phase-CAS RowsAffected==0, `clusteroffline` (b)-precondition + flock unit, status render + 5 fixtures + exit codes + no-:7400 behavioral, drain-F==0-`--yes`-reject, guard `TestD7ProductionWiresNoCluster` + bidirectional self-check, import-cycle guards (`clusternodes` stays nats+raft-free; **`cluster` stays nats-free AFTER the auth edge** — Critique 2 M2, none of the drafts added this).
- **gated `d7_integration` (`TestD7Matrix -race`, OUT of `make test`):** the multi-node membership + force-single + status double-view drills (routed-NATS+raft starves `make test` parallelism — the D5/D6 lesson).
- **e2e matrix:** a `TestD7` subtest with the **cheap** membership-happy-path + status exit-code in the always-on net; the heavy drill stays gated.
- **Concurrency (no goleak):** orchestrator goroutines (catch-up poll, drain migration loop, AddVoter future waits), the offline flock, and the :7400 probe conns all under `-race` + in-repo `runtime.NumGoroutine` poll-with-tolerance + **fd-baseline** gate (`test/concurrency/helpers_test.go`). The fd gate is load-bearing for leaked BoltStore handles + :7400 dial conns (the D1/D5 leak lesson).

---

## K. Risk-ranked task breakdown & ordering (riskiest/most-coupled first)

1. **Doc-first amendments B-1..B-10** — block everything. B-2 (offline recovery point), B-3 (poison-skip not panic), B-4 (leader-startup reconciliation), B-5 (catch-up domain) are the state-machine-changing ones.
2. **`errAppliedRejected` FSM plumbing + `clusterNodeUpsertApplier` (verify-then-exec) + canonical join message + `auth` import** — riskiest: the only new code reachable from `FSM.Apply` that could wedge replicas; must be poison-skip (not panic), deterministic, L-2-clean. Unit-prove with the 3-node forged-sig negative control. **Add the `cluster`-stays-nats-free import-cycle guard here.**
3. **Migration 0013 + the 3+1 ops' Plan helpers (`clusternodes/plan.go`) + phase-rank CAS guards.**
4. **Node membership wrappers + orchestrator phase machine + leader-startup reconciliation pass + idempotent retry across leadership change** — the no-silent-fork core.
5. **`internal/clusteroffline` force-single/recover + flock + BoltDB-lock probe + `RecoverCluster`-driven reconcile + :7400 TCP-liveness gate + dump-fsync-before-wipe** — genuinely-real, irreversible, highest blast radius. Build the 3-node drill here.
6. **Guard test `test/d7/regression_test.go` (extend D6 pattern: new banned tokens incl. the field-write + struct-literal forms; `internal/clusteroffline` excluded; `RecoverCluster(`/`raftboltdb.New(` banned in the cmd/broker/agent scope; bidirectional self-check) + import-cycle re-assertions** — land alongside 2-5 as the build-and-prove tripwire (early task per the prompt).
7. **drain/retire orchestration** (D6 rehome + D5 AllAtTarget + serving-set quorum guard + rebuild-OFF enumerated confirm).
8. **adminsock cluster ops + `ClusterAdminBackend` (nil in prod) + non-leader fail-fast naming.**
9. **status/doctor double-view + `reach_source` + exit-code SSOT + 5 fixtures + no-:7400 rule.**
10. **cobra `cluster.go`/`cluster_offline.go` wiring + `--json`/TTY confirm + `rotate-tunnel-cert`.**
11. **e2e matrix entry + runbook docs (force-single, account.nk/CA rotation-for-retire).**

---

## L. OPEN QUESTIONS for the human adjudicator

**OQ-1 (catch-up barrier domain — genuine doc-contract change).** §8.2 L269 literally says `applied_index >= barrier`; §7.2 L218 already proved command-domain `applied_index` ≠ raft `CommitIndex`. My recommendation (§B-5): keep the predicate **command-domain** (barrier = leader's SQLite `applied_index` under `VerifyLeaderRead` at AddVoter time), since the HA "0 committed business loss" claim is about business state. Draft 3 proposed switching to **raft-domain `r.AppliedIndex()` ≥ CommitIndex** (requires a new primitive). Critique 1 says Draft 3 is right; Critique 4 says it's a contract change needing sign-off. **This amends a state-machine contract — adjudicator/external-review should confirm the domain explicitly.** My lean: command-domain, consistent with the D6 epoch-as-local-row-epoch precedent (§7.2 already chose local-row-epoch over raft-index for the same domain reason).

**OQ-2 (forged-sig verdict — I recommend resolved, flagging the fsm.go change).** Adopt **poison-skip via `errAppliedRejected`** (reject Draft 1's panic — verified it bricks the cluster). The only open part: this adds a small new branch to `applyCommand` (a sentinel check before the retry loop). Confirm the main process is comfortable touching `fsm.go` (the D1-hardened fail-stop path) for this — it is fenced and doc-amended (§B-3, §2.8).

**OQ-3 (offline-tool raft import boundary).** I recommend `internal/clusteroffline` imports raft directly (L-2 holds — raft confined to the cluster package family; cobra calls a function not a banned token). Alternative: route through an `internal/cluster.OpenOfflineStore` helper (Draft 1 OQ-7). Lean: direct import in `clusteroffline`, guard scans only cmd/broker/agent.

**OQ-4 (join-signature fetch UX).** Recommend the **paste-token** flow (joiner has shell + node-ident seed; no new NATS subject/ACL; keeps admin strictly local). Alternative: interactive over a new broker-only subject (adds §6.2 ACL surface). Note Critique 2 m4 cautions against adding an unrequested `sign-join` verb not in the §8.1 table — so the token could be emitted by the existing `node-pub` path rather than a new verb. Adjudicator to pick the exact CLI shape.

**OQ-5 (nonce single-use strength).** Recommend **leader-local pre-propose enforcement** + an honest doc note that single-use is a consistency property, not a security boundary (§18.2.4 accepts a compromised leader proposes anything; the security-pragmatic feedback favors not adding the replicated nonce ledger for a theoretical chain). Reject Draft 3's `OpJoinNonceIssue` ledger (orphan-nonce state, extra op, untested — Critiques 1/2/3/4 unanimous). Also decide whether the canonical join message binds **cluster-id** (Draft 1) in addition to `node_id` — recommend `node_id` binding (load-bearing) + cluster-id optional, per pragmatism.

**OQ-6 (drain-without-retire voter semantics).** §8.3 L272's "含已处于 N=2 时再 drain" implies a plain drain at N=2 must trigger the F==0 guard even though the node stays a raft voter (it sheds serving capacity). I model the guard on **projected serving-set fault tolerance**, not just raft config membership (§G step 1). Confirm: does a plain `drain` (no `--retire`) keep the node a voter (sheds expose-home load only) while still hitting F==0 confirm at N=2? My lean: yes (matches the doc parenthetical). Critique 2/3 M4/M6 flag the drafts conflated this.

**Files most relevant (all absolute):** `/home/weiland/projects/dist_experiment_control/internal/cluster/{command.go,clustermeta.go,fsm.go,read.go,node.go}` (ops, appliers, the `errAppliedRejected` plumbing point fsm.go:111/180/220, membership wrappers), `/home/weiland/projects/dist_experiment_control/internal/clusternodes/{read.go,plan.go(new)}` (pure-SQL Plan helpers), `/home/weiland/projects/dist_experiment_control/internal/auth/nkey.go:55` (FSM-reachable verify, verified cycle-free), `/home/weiland/projects/dist_experiment_control/internal/port/plan.go:163` (CAS reference), `/home/weiland/projects/dist_experiment_control/internal/adminsock/{protocol.go,server.go}` (new ops + ClusterAdminBackend), `/home/weiland/projects/dist_experiment_control/internal/storage/migrations/0008_cluster_nodes.sql` + `0013_cluster_nodes_join_pop.sql`(new), `/home/weiland/projects/dist_experiment_control/internal/clusteroffline/`(new, raft-importing offline surgery), `/home/weiland/projects/dist_experiment_control/internal/broker/clusteradmin.go`(new, build-and-prove orchestrator + status responder), `/home/weiland/projects/dist_experiment_control/cmd/tether/{cluster.go,cluster_offline.go}`(new), `/home/weiland/projects/dist_experiment_control/test/d6/regression_test.go` (guard pattern to extend), `/home/weiland/projects/dist_experiment_control/cmd/tether/serve.go:128` (verified byte-unchanged-clean), and `~/go/pkg/mod/github.com/hashicorp/raft@v1.7.3/api.go:313` (`RecoverCluster` replays through `fsm.Apply` to `LastIndex`) / `:462` (`HasExistingState` for the (b) precondition).

**Factually-wrong-about-the-codebase claims found in the drafts (verified):** (1) Drafts 1/3/4's force-single "apply BoltDB entries up to `commitIndex`" — `commitIndex` is never persisted to disk; only Draft 2 caught this. (2) Drafts 1/2/3/4's "reconcile via `ExecCommand` then `RecoverCluster`" — `ExecCommand` does not advance `applied_index` and `RecoverCluster` already replays through `fsm.Apply` → double-apply; no draft was fully correct. (3) Draft 1's forged-sig **panic** recommendation — verified fsm.go:111 turns an applier error into a cluster-wide brick; the poison-skip the others want is NOT reachable from an applier without new plumbing (the `errAppliedRejected` branch this plan adds). (4) Drafts 3/4's new `internal/joinpop`/`internal/clusterauth` leaf package — verified unnecessary: `internal/auth` has zero nats.go/cluster/raft deps, so `internal/cluster` imports it directly with no cycle. (5) Draft 3's "join_nonce/join_sig wire-only, not persisted" — breaks deterministic follower re-verify input carriage + cross-snapshot retry.