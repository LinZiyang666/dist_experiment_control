# g2-g6-plan.md — G2 (force-single 完整化) + G6 (容量感知 MaxBytes) 定稿 plan

> 定稿人：主进程（唯一定稿人）。基于 Stage-A 多专家对抗性 workflow（12 个 Opus 4.8 专家：6 drafter →
> 5 critic → 1 synth，run `wf_30e30054-3a0`，1.87M tok）综合的候选骨架 + 主进程侦察核验裁决。
> 本 epic = HA grow/force-single/deploy 整治 roadmap 的 G2+G6（一起做：membership 状态机 vs 数据面 JS
> 容量、代码零冲突，且同为现网单 broker racknerd 止血）。走 3 阶段 7 步，本轮到外审门。

---

## 0. 主进程定稿裁决（OQ1–OQ10）

Stage-A synth 骨架在现场代码核验中**推翻了背景文档 `docs/reviews/v0.4.5-ha-grow-ops-gotchas.md` 的两个前提**，
定稿全部采纳这些核验：

- **[已核实] `RemoveServer` 对已不在 raft config 的节点是幂等 no-op、不报错**（`internal/cluster/
  membership.go:61-75` docstring + hashicorp raft 语义）。→ 背景文档 §#12 "RemoveServer 因幽灵报错" **错误**。
  真正的在线删除死锁只有两段：① `RemoveNode` phase-gate（`clusterdrain.go:203`）拒 VOTER；② `PlanClusterNodeRemove`
  （`membership_ops.go:294-316`）只 DELETE phase ∈ {VOTER_ADD_FAILED, RETIRING, CATCHING_UP}，VOTER 行是
  `RowsAffected==0` 静默 no-op。
- **[已核实] 迁移双破真实存在**：`ListPeersForTopology`（`clusternodes/read.go:59-66`）**不按 raft config 过滤**，
  只按 `topoMeshPhases={CATCHING_UP,VOTER,DRAINING,RETIRING}`。现网 pc732 仍 phase=VOTER → 仍在 mesh → 升级后
  reconciler 会 `len(Peers)==2` 且 pc732 的 `nats_route` 若可解析 → 渲 clustered（**非** zero-routes，不触发
  `config.go:144` fail-closed）→ **把 hand-standalone conf 覆盖回 clustered**（现网二次破坏）。这是最高危迁移项。
- **[已核实] `preflight.go:58-70` `jetstreamSafeSubkeys={store_dir}`，`max_file_store` 被 fail-closed 拒** →
  渲 `max_file_store` 会 brick reconciler + de-cluster render + rolling-upgrade。背景 doc "另附：该显式渲
  max_file_store" **判错、删除**。
- **[已核实]** `js.AccountInfo()` boot 结果被丢弃（`broker.go:856-862`）；`diskUsage()`（`disk.go:127-137`）返回
  `(used, total)`。stale "4 GiB" 注释在 `transfer.go:56-57,186-187`（`:201` 内联注释已正确写 8 GiB）。

**逐 OQ 裁决**：

| OQ | 裁决 | 理由 |
|---|---|---|
| **OQ4** #12 model：DELETE vs EJECTED | ★ **DELETE outright**（推翻 synth 的 EJECTED 骨架） | force-single 后 abandoned 已被 RecoverToSelf(Online) 移出 raft config → 从 roster DELETE 是 quorum-safe（无 B1 fork）；"没有这行"天然满足 roster 不发 / DialURLs 不 dial / clusterstatus 不 Inconsistent / to-standalone tally 只见 self —— **免去 EJECTED 在 5 处的特判 + 免 wire 新 phase 值**；reabsorb 已 defer 到 G4 + rejoin 走 wipe identity → 保留 row 无人消费，EJECTED 的"保 identity"好处落空。可见性由 force-single 响应的 `Abandoned` 列表 + `force_single_active` marker + #20 banner 提供。 |
| **OQ1/OQ9** #20 render 分层 | offline **inline render standalone**（cmd/tether 层）；online **靠 reconciler 收敛**（不 inline render） | offline 后 broker 要重启、reconciler 没跑，重启前必须渲对否则 broker `voters<=1 + conf clustered` → exit 70；online broker+reconciler 在跑，DELETE abandoned bump topology_gen → reconciler 看 `peers=[self]` → 渲 standalone（`ActionSwappedReloadPending`，写盘不 auto-restart）。两路径最终 conf 一致，reconciler 保持唯一 conf writer（无第二 writer 竞争）。 |
| **OQ1** #20 不 auto-restart | **不 auto-restart nats、不 auto-wipe store** | broker 以 `User=tether` 跑不能 `systemctl`；重启 blip 共享的 core NATS 控制面 + 把 standalone JS 载到未 reset 的 clustered store（wedge）。收敛成 operator 一条 `systemctl restart nats-server` + `mv` store 备份（LOUD 打印）。online 无缝卖点保留（控制面通道不断）。 |
| **OQ3** 迁移守卫 | **采纳**：reconciler 的 peer 集排除 "VOTER-phase 但 not-in-committed-raft-config" ghost | 专门保护**现网 racknerd 升级**（遗留 pc732 VOTER 幽灵，升级前 force-single 留下、无 DELETE 逻辑）：让幽灵在 operator 动手前就离开 mesh → `peers=[self]` + own already-standalone → reconciler noop 保住手改 conf。未来的 force-single 由 DELETE 天然处理。 |
| **OQ5** #12 removal 落点 | 新 **leader-gated force-remove planner**（DELETE by node_id + leader 证明 not-in-committed-config）；force-single abandoned prune 与 operator ghost passthrough 共用 | 确认"~3 行 passthrough"预算不足（`PlanClusterNodeRemove` 只删 3 个 phase）；确认 RemoveServer 幂等（不需它成功）。 |
| **OQ6** #21 ceiling + 公式 | ceiling = `AccountInfo().Limits.MaxStore`（finite 时）else `statfs`-based fallback；**OBJ_xfer-only 盘感知**（events/history 固定 1G）；`MaxBytes = clamp(fraction·(ceiling − 2GiB), floor, min(8GiB, avail_for_obj))`；`avail_for_obj < floor` → **refuse**；**绝不 emit MaxBytes ≤ 0** | statfs robust 回落免验证；OBJ-only 保 events/history 控制面流；硬不变量恒 `(floor, 8GiB]`；`ObjectStoreConfig.MaxBytes<=0` 被 nats 视 UNLIMITED（更糟静默 re-brick）。具体 fraction/floor 数字 Stage-B 精调到 drill 21(4g) 翻绿。 |
| **OQ8** eject 可靠性 | **best-effort inline prune（log-not-fail）+ 确定性 operator RemoveNode ③**；**去掉** reconcile-floor | force-single swap 后节点有 leader 接触，re-run 被 dwell gate（`CodeQuorumNotLost`）拒 → "loud-fail 让 operator re-run" unreachable；reconcile-floor 长命 marker 误踢 regrow joiner + boot 双破。 |
| **OQ2** 现网 pre-flight | 写进 §5 上线序列（上线前必做，**不阻塞 Stage-B**） | 6 项实况（conf 是否 latently re-clustered / pc732 route 是否可解析 / marker 是否 SET / …）决定 rollout 分支。sim 里建模 upgrade-with-ghost drill 代偿一部分。 |
| **OQ7** agent consumer 是否 LIVE | DELETE 方案下**正确性无关**（consumer LIVE→现役收敛，未 LIVE→为 G3 铺路，都对）；Stage-B 确认 `agent/roster.go:359` 状态以定 drill 断言强度 | DELETE 让 roster 自然收敛，不依赖 consumer 状态。 |
| **OQ10** drill 20 建模 | **采纳**：operator `systemctl restart nats-server` + JS store reset 建模为**显式 sim-deploy step** | 符合 simcluster mandate（供给/重启是 sim 的活，tether 不 auto-restart 是设计，不替它弥补）。 |

---

## 1. 逐 gotcha 最终 approach

### #12 幽灵 VOTER（keystone；DELETE 模型）

**A. force-single 从 roster DELETE abandoned 行（both paths）**
- 新 planner `cluster.PlanForceSinglePrune(abandonedIDs, now)`：`DELETE FROM cluster_nodes WHERE node_id IN (...)`
  + `rosterGenBumpStmt` + `topologyGenBumpStmt`（mesh-leave，让 reconciler 醒来收敛 #20 online 侧）。change-gated。
- **online**：`force_single_online.go:284-289` 拿到 `abandoned` 后（epoch Propose 之后），`Propose(PlanForceSinglePrune(abandoned))`
  —— **best-effort**（失败仅 log + 在 response 标注，不翻掉 OK）。abandoned 已被 `RecoverToSelfOnline` 移出 config
  → DELETE quorum-safe。
- **offline**：`offline.go:104-116`，在 `RecoverSingleNode` **之后**、贴着 `raiseForceSingleMarker`，用同一 direct-SQL
  路径 `DELETE FROM cluster_nodes WHERE node_id IN (abandoned)`（`NoSnapshotRestoreOnStart=true` 使 post-recover raw
  write 持久，同 marker）。re-run 幂等。
- 效果：roster 立即无 abandoned → `readRosterBrokers`（`cluster_roster.go:98`）**天然不发**（无需加 WHERE 过滤）→
  agent/ctl 名册收敛到 `{survivor}`；`clusterstatus` 无 Inconsistent 行；`to-standalone` voter tally 只见 self=1 →
  **直接解锁 #20**。

**B. membership-aware RemoveNode 幽灵 passthrough（清现网存量 pc732 = VOTER-not-in-committed-config）**
- `clusterdrain.go:203` phase-gate 放行 "VOTER-phase 但 **not-in-committed-raft-config**" 的 ghost；对**在 config 的
  live VOTER 仍硬拒**（保 B1 anti-fork）。
- not-in-config 证明必须读 **leader-committed config**（`node.RaftConfiguration()`，RemoveNode 已 leader-gated）。
- 删除走同一新 leader-gated force-remove planner（`PlanForceSinglePrune` 复用，或平行 `PlanClusterGhostRemove(nodeID)`
  —— Stage-B 定，语义：DELETE by node_id，调用点已证 not-in-config）。RemoveServer 对 not-in-config 是幂等 no-op（先调、no-op、再 DELETE）。
- ownership guard（`clusterdrain.go:223`，现仅 `phaseAddFailed` 触发）扩展到 ghost：ghost 仍 home exposes
  （`port_allocations.home_broker=ghost`）默认拒、`--force` 绕过；文档记 §7.4 agent-reconnect 会 re-home 到 survivor。

**C. 迁移守卫（保护现网升级防双破，OQ3）**
- reconciler 构造 peer 集时排除 "VOTER-phase 但 not-in-committed-raft-config" ghost：在 reconcile 调用
  `ListPeersForTopology` 后用 `RaftConfiguration()` 交集过滤（或在 `read.go` 增 committed-config 参数）。Stage-B 择更小改动。
- 使现网 racknerd 升级后 reconciler 看 `peers=[self]` → 不把 pc732 当 route peer → 不渲 clustered 覆盖手改 conf。

**驳回**：EJECTED 终态 phase（OQ4）；`force_single_active`-gated reconcile-floor auto-eject（OQ8：长命 marker 误踢 +
boot 双破）。

**落点**：`membership_ops.go`(新 planner)· `force_single_online.go:284-289` · `clusteroffline/offline.go:104-116` ·
`clusterdrain.go:203,223,232-237` · `clusternodes/read.go:59-66`(迁移守卫)· `cmd/tether/cluster.go`(RemoveNode CLI 措辞)

---

### #20 survivor nats.conf 滞留 clustered → JS 静默 503

- **offline force-single**：cmd/tether 层（`cmd/tether/cluster_offline.go`，package main 已 import
  natscluster/natsconf）**inline render standalone conf** —— 抽出 `runReconcileToStandalone` 的 socket-less render
  core（render + F3 store_dir-缺失拒 + post-apply re-parse 证明 + `warnClusteredJSShrink` + `.bak`），**跳过 socket
  voter tally**（force-single 的 peer-dead HARD-REFUSE + `{self}` raft 是比 tally 更强的 N=1 证明，满足 R3 explicit-
  intent + provable-N=1）。**already-standalone = benign no-op**（对现网 racknerd 幂等，绝不 re-clobber 手改 conf）。
  identity source = `cluster_nodes` 自身行的 `nats_server_id`+`bus_nkey_pub`（不塞进 `clusteroffline.ForceSingle`，
  它是纯 orchestration/storage 叶子）。
- **online force-single**：**不 inline render**。§1#12-A 的 DELETE abandoned 已 bump topology_gen → reconciler 醒 →
  `peers=[self]` → 渲 standalone conf（`ActionSwappedReloadPending`，写盘）。operator 一条 `systemctl restart
  nats-server`（+ 需要时 `mv jetstream` reset）激活。控制面通道不断。
- **持久 DATA-PLANE-DEGRADED banner**：`force_single_active` AND live-conf-still-clustered（`natsconf.Preflight().
  IsClusteredJetStream()` / reload-pending）→ `cluster status` banner + `doctor`。**绝不走 JS event stream**（那正是
  503 的东西，现网烂 5 天的根因）。
- **不 auto-restart / 不 auto-wipe store**（OQ1）。
- **可选**：`natsreconcile/reconcile.go` 对 on-disk-standalone/live-clustered delta 报 `swapped_reload_pending` 而非
  每 tick 重发不可 reload 的 SIGHUP（防 SIGHUP-storm）。Stage-B 视工作量取舍（列为 nice-to-have）。

**落点**：`cmd/tether/cluster_natsconf.go:118-239`（抽 render core 到共享 helper，供 offline force-single 复用）·
`cmd/tether/cluster_offline.go`(offline inline render)· `internal/broker/clusterstatus.go`(banner)· `natsreconcile/
reconcile.go`(可选 storm 抑制)

---

### #10 被弃节点冷启动 STUCK（exit-70 crash-loop）

- `broker.go:895-906` 加 `voters>=2` 差分诊断分支（在现有 `voters<=1` 与 generic 之间）：用**仅 quorum-free 本地信号**
  （`NumVoters()` + `RaftConfiguration()` + 有界并发 TCP peer-probe，复用 `clusteroffline.ProbePeers`），emit **ranked
  differential**：peer 可达但仍无 JS quorum ⇒ 很可能被 force-single 踢出（重启不自愈）；所有 peer 不可达 ⇒ 瞬时故障
  **或**被踢，去 survivor 确认。**绝不硬断言"你被踢了"**（本地无法证明）。给 crash-loop 止血指引（`systemctl stop`）+
  引用 runbook rejoin chain。保持 **fail-stop**（不 auto-wipe）。
- **驳回** boot JS-probe retry-grace（触碰 boot hot path，出 G2 范围）。

**落点**：`broker.go:895-906`（复用 `clusteroffline.ProbePeers` + `RaftConfiguration()`）· `docs/cluster-runbook.md`

---

### #15 被弃节点无法简单 rejoin（reabsorb）

- `cluster reabsorb <node>` 编排命令 **OUT → G4 grow family**（耦合 wipe+rejoin、依赖 join prepare/approve +
  grow-onto-migrated-broker 快照 hazard）。
- G2 只出：#10 的 actionable message + **runbook 手动 rejoin 链**（wipe 过时 raft → `recovery rejoin prepare` →
  survivor 上 `cluster join approve`）。

**落点**：`docs/cluster-runbook.md`

---

### #21 OBJ_xfer 8 GiB 硬编码 → 小盘 tier-B 全废（G6）

- **不渲 max_file_store**（定案）。修 `transfer.go:56-57,186-187` stale "4 GiB" 注释。
- **OBJ_xfer MaxBytes 盘感知**（`transfer.go:201` ensureXferBucket + `:248` raiseXferReplicas 单一来源函数）：
  - `ceiling = AccountInfo().Limits.MaxStore`（finite 时；Stage-B 验证 unset max_file_store 时报有限值）else
    `statfs(StoreDir)` 派生（`disk.go` diskUsage 的 `total`/`free`，取 `~0.75·free` 或与 nats 默认对齐的估值）。
  - `avail_for_obj = ceiling − eventsHistoryReservation`（events 1G + history 1G = 2GiB 固定）。
  - `if avail_for_obj < floor` → **refuse** `"js store too small for tier-B (have X, need >=Y)"`。
  - `MaxBytes = clamp(fraction·avail_for_obj, floor, min(8GiB, avail_for_obj))`；**硬不变量：恒 `(floor, 8GiB]`、
    绝不 ≤ 0**（`<=0` 被 nats 当 UNLIMITED）。
  - fraction/floor 具体数字 Stage-B 精调到 **drill 21(4g tmpfs) 翻绿**（4g → nats ceiling ≈3G，−2G → ≈1G 供 OBJ →
    clamp 出一个 <4g 且装得下小 tier-B 的值）；**大盘不回归**（ceiling 大 → clamp 到 8GiB，等同现状）。
- **raiseXferReplicas raise-only 保留现有 bucket MaxBytes**（不 recompute-shrink，否则 grow 后 `UpdateObjectStore`
  撞 shrink-below-used reject → replication_degraded latch）。
- **per-transfer cap bucket-aware**（`transfer.go:432` 现固定 2GiB `transferMaxBytes`）：too_large gate 用
  `min(transferMaxBytes, bucketMaxBytes)`，否则小盘 bucket 上 accept-then-10047。
- **驳回**：formula-mirror（`Bavail/4*3` 耦合 nats 版本）+ 三流 split + broker.yaml knob（v1 不做，列 OQ）。

**落点**：`transfer.go:56-57,186-187,194-218(:201),246-259(:248),432-433` · `disk.go:123-137`(复用)· `broker.go:856-862`
(AccountInfo 捕获，若用该来源)· `preflight.go:58-70`(确认 max_file_store 守卫不变)

---

## 2. Epic scopeBoundary

**IN G2**：force-single DELETE abandoned（both paths：online best-effort Propose / offline post-recover direct-SQL）
+ 新 force-remove planner；membership-aware RemoveNode 幽灵 passthrough（放行 VOTER-not-in-committed-config +
leader-committed-config 证明 + ownership guard）；迁移守卫（reconciler peer 集排除 not-in-config VOTER）；offline
force-single inline de-cluster render（already-standalone no-op）+ online reconciler 收敛路径；持久 DATA-PLANE-DEGRADED
banner；#10 差分诊断 cold-start error（message-only, fail-stop）；runbook rejoin/ghost-remove 章节。

**IN G6**：disk-aware OBJ_xfer MaxBytes（AccountInfo/statfs ceiling，clamp≤8GiB，floor-or-refuse，never≤0）；
raiseXferReplicas raise-only 保留 MaxBytes；per-transfer cap bucket-aware；**不渲** max_file_store；修 stale 注释。

**DEFER**：G3（agent 侧 roster consumer 收敛 / cache-invalidation）；G4（`cluster reabsorb`、grow-onto-survivor）；
G5（升级，无新增）；G7（"survivor JS 503 / de-cluster pending" / "ghost pending removal" 正式 alert kind，G2 先 status
text 呈现）。

**OUT entirely**：EJECTED 终态 phase（OQ4 选 DELETE）；reconcile-floor auto-eject；auto nats restart / auto JS-store
wipe；boot JS-probe retry-grace；formula-mirror + 三流 split + broker.yaml xfer knob；"park read-only when JS down"。

**wire-compat 判定**：`ProtoVersion` 保持 2、`ClusterRosterSchemaVersion` 不变。DELETE 模型**无新 phase 值**（连
EJECTED 加性字符串都不需要）→ wire 零影响，混版车队 + 半修的 racknerd 安全，无需 v1→v2、无需重装。

---

## 3. TestItems

**hermetic Go（各自 package，嵌入式 `nats-server/v2/test`，`-race` + 触碰并发面带内建 NumGoroutine/fd leak 门）**：
1. `cluster`: `PlanForceSinglePrune` 表驱动——多 abandoned 一次 DELETE；re-run `RowsAffected==0` 幂等；bump
   roster_generation + topology_generation（change-gated）。
2. `broker`: `handleForceSingleCommit` 后 abandoned 行从 `cluster_nodes` 消失 且 `response.Abandoned` 匹配；**注入
   prune Propose 失败 → 响应仍 OK（best-effort），不 false-fail**；raft config 已是 `{self}`（prune 不 fork）。
3. `broker`(对抗): `RemoveNode` 删 "VOTER-not-in-committed-config" ghost（RemoveServer 幂等 no-op、delete 命中）;
   **在 committed-config 的 live VOTER 仍硬拒**（B1 guard，读 leader config 非 stale follower）；ghost 仍 home
   exposes → 无 `--force` 拒、`--force` 过。
4. `clusteroffline`(parity): offline ForceSingle DELETE abandoned 后 `cluster_nodes` 行与 online 路径逐行一致；
   direct-SQL 在 `RecoverSingleNode` 之后、re-run 幂等。
5. `broker`: `readRosterBrokers` DELETE 后不含 abandoned；全删到只剩 self → `buildSignedRoster` 仍 Verify 通过。
6. `broker`: clusterstatus——prune 后无 Inconsistent 行；**存量 VOTER-not-in-config ghost（未 prune）仍 Inconsistent
   `true`**（回归钉 `:259-260`，证明只 DELETE 才消，不是静默漏标）。
7. **迁移回归（最高危）**: reconciler peer 集排除 VOTER-not-in-committed-config → `peers=[self]` + own already-standalone
   → **PRESERVED noop**（不覆盖手改 conf）；**去掉迁移守卫时** `[self, ghost]` + ghost route 可解析 → 渲 clustered
   （钉住双破 hazard，守卫后转 GREEN）。
8. `cmd/tether`(#20): offline force-single inline render——clustered conf → standalone（无 cluster{} 块、store_dir
   保留、`nats-server -t` 过）；**already-standalone conf → 字节级 no-op**（覆盖 racknerd 实际 conf 形状）；F3
   store_dir 缺失拒。
9. `cmd/tether`: `to-standalone` tally 在 abandoned 已 prune 后见 voters==1 解锁；genuinely-unknown 非预期 role 仍拒。
10. `broker`(#10): cold-start 表驱动——`voters==1`→to-standalone hint（不变）；`voters>=2` + JS unavailable + peer
    reachable→差分 rejoin fail-stop（断言 token，非 generic exit-70）；peer unreachable→"transient-or-ejected"措辞；
    从不硬断言"你被踢了"。
11. **`broker`(#21 硬不变量)**: 表驱动 injected ceiling {tiny4G, racknerd~10.5G, huge}——恒 `(floor, 8GiB]`；4G →
    OBJ 小到 `CreateObjectStore` 被 admit；huge → cap 8GiB（不回归）；ceiling 连 floor 都不够 → 明确 refuse（**断言
    非 0、非 unlimited、非 8G**）；AccountInfo/statfs 错 → 保守回落。
12. `broker`: `raiseXferReplicas` raise-only 保留现有 bucket MaxBytes（不 shrink below used）。
13. `broker`: per-transfer cap——bucket 900MiB 时 1GiB tier-B push 在 admission 就 reject（非 accept-then-fail）；
    大盘 2GiB 仍过。
14. **golden**: `natscluster.Render` 任意 Config 输出**永不含 `max_file_store`**（单元级钉住 + drill grep guard）。

**drill flip（deploy-tier，weilandserver，signature-guarded；sim server 已开）**：
- **drill 20 → GREEN**：force-single 后 nats.conf 无 `cluster{}` + mtime 变；operator `systemctl restart nats-server`
  + `mv jetstream` store reset 建模为**显式 sim-deploy step**（OQ10）；之后 tier-B push（`--ack-alerts`）成功且文件真
  落地；signature-guarded 断言 "restart-pending / DATA-PLANE-DEGRADED banner 在重启前出现"；保留 JS-independent 控制面
  存活正控。
- **drill 21 → GREEN**：4g tmpfs 上 tier-B push 成功；**保留 `! grep max_file_store nats.conf` 守卫（必须仍绿）**；
  机制锚定断言 `OBJ_xfer-<sid>` MaxBytes < 4g cap（证盘感知非侥幸）。
- **新增 drill "upgrade-with-ghost"（推荐）**：force-single + 手 de-cluster + 残 VOTER ghost 的 broker，升级二进制后
  断言 reconciler **不** re-cluster on-disk conf（钉迁移守卫），operator `recovery node remove <ghost>` 成功、client
  roster 丢 ghost。

**e2e 矩阵**：hermetic Go 进各 package；**不**把 clustered raft-swap + nats-restart 加进 `make e2e`（routed-JS flake
类，e2e 刻意串行避开）；真 nats-restart 证明归 drill 20/21。

---

## 4. 现网 racknerd 安全上线序列（上线时执行，不阻塞 Stage-B）

**Step 0 — pre-flight 抓取（gate rollout）**：① on-disk nats.conf 仍手 standalone 还是已 latently re-clustered；
② pc732 `cluster_nodes` 行 phase(应 VOTER) + `bus_nkey_pub`/`nats_route` 是否可解析；③ `cluster_meta` `force_single_active`
是否 SET；④ topology reconcile loop 是否活跃；⑤ nats varz `config_load_time` vs conf mtime；⑥ pc732 是否 home expose。

**Step 1 — 上线二进制（含迁移守卫）**：迁移守卫使幽灵离开 mesh → reconciler noop 保住手改 conf。

**Step 2 — operator 清 pc732**：`cluster recovery node remove pc732`（membership-aware ghost passthrough）→ 删
not-in-config VOTER 行。**零 conf 写、零 nats 重启、零数据触碰**（pc732 若 home expose 先 §7.4 re-home 或 `--force`）。

**Step 3 — 验证**：client re-register 不再列 pc732；`cluster status` N=1、无 Inconsistent、无 DEGRADED。

**Step 4 — 分支**：conf 仍手 standalone → 不重启收工；conf 已 latently re-clustered → 清 ghost 后 `reconcile nats
--to-standalone --confirm-single`（现已解锁）→ 受控 `systemctl restart nats-server`。

**铁律**：**不在 racknerd re-run force-single**（已 N=1+standalone，canonical render 可能丢手调 key）；任何 render
路径对 already-standalone conf 必须是证明过的字节级 no-op（Go 测试覆盖 racknerd 实际 conf 形状，见 TestItem 8）。

**Step 5 — G6 同二进制**：disk-aware OBJ_xfer 是 conf-neutral / restart-neutral，只作用于新 bucket（racknerd tier-B
从未建过 bucket）→ 立即解锁 tier-B。

---

## 5. Stage-B 实现顺序（先父后子）

1. **#12-A 新 force-remove planner + force-single DELETE abandoned**（both paths）——keystone，解锁 #20 tally。
2. **#12-B membership-aware RemoveNode + #12-C 迁移守卫**——清存量 + 保护现网升级。
3. **#20 offline inline render（抽 render core）+ online reconciler 路径 + DATA-PLANE-DEGRADED banner**。
4. **#10 cold-start 差分诊断**（message-only）。
5. **#21 disk-aware OBJ_xfer MaxBytes + raise-only 保留 + per-transfer bucket-aware**（G6，独立可并行）。
6. **runbook**（#15 手动 rejoin 链 + ghost-remove）。
7. hermetic Go 测试同 PR 落盘；drill 20/21 flip + upgrade-with-ghost 新 drill。

每处对 already-standalone / 混版 / 现网半修态的**幂等 + no-op** 是贯穿不变量。
