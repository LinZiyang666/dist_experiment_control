# g2-g6-review.md — Stage-C 内审报告 + 主进程裁决

内审方式：6 个 Opus 4.8 专家对抗性 workflow（run `wf_0eb9278f-748`，834k tok / 215 tool-calls）——维度
forcesingle-membership / natsconf-lifecycle / coldstart-capacity / invariant-wire-compat / correctness-edgecase
/ test-adequacy。专家只读实现、可新增测试，绝不改实现。`make lint`（golangci-lint v2）= **0 issues**。

主进程逐条裁决（Stage-C 阶段二，串行修复）。

## BLOCKER（2 专家独立发现 → 采纳并修复）

**deClusterStandaloneConf identity harvest 用 AuthIdentity 而非 cluster_nodes**（`cmd/tether/cluster_offline.go`）
- Finding：`deClusterStandaloneConf` 用 `own.AuthIdentity()` 取 broker nkey，但**真 clustered conf 的
  authorization.users 块每个 peer 一个 nkey → AuthIdentity 对多 user 返回 ""** → offline 自动 de-cluster 对
  任何 N≥2 集群失效（报 "could not resolve identity"），operator 一次误重启即撞 exit-70 crash-loop（正是 #20
  要防的）。**这违背了 plan §1#20 明确的 identity source = cluster_nodes 的 nats_server_id+bus_nkey_pub。**
- **裁决：采纳**（我实现时用错了 API）。修复：① 新增 `readSelfIdentity(dbPath, selfID)` 从 cluster_nodes 读本
  broker 的 nats_server_id + bus_nkey_pub；`deClusterStandaloneConf` 用它，account issuer 仍从单值
  auth_callout.issuer（AuthIdentity 能解析）。② 按 test-adequacy 专家建议 **split 出纯 render core
  `buildStandaloneConf`**（无 DryRun 外部 binary）→ hermetic 可测。③ 新增 `cmd/tether/g2_decluster_test.go`：
  2-auth_user clustered conf → 渲出的 standalone conf auth_user == 本 broker 的 bus_nkey（非 peer 的、非
  AuthIdentity）、无 cluster{} 块；already-standalone byte no-op；missing store_dir refuse。全绿。

## MAJOR（采纳并修复）

1. **jsStoreCeiling statfs 回落用 total 而非 free**（`internal/broker/transfer.go`）— total·0.75 高估容量（盘
   已用很多时 bucket 建了但 admission 失败）。**采纳**：改 `free = total-used`，`free·0.75`（diskUsage 已返回
   used，原被 `_` 丢弃）。对齐 plan OQ6。
2. **online 指引 + runbook 的 to-standalone 命令缺 --broker-nkey**（`cluster_offline.go` + `docs/cluster-
   runbook.md §2.2/§3.2`）— runReconcileToStandalone 对多 user conf 需显式 --broker-nkey（同 BLOCKER 根因），
   operator 照抄跑不通。**采纳（文档修复）**：online 指引 + runbook 两处加 `--server-name <self> --broker-nkey
   <self-bus-nkey>` + 来源说明（server_name 在 conf 的 `server_name:` 行；broker-nkey 在 broker.yaml）。
   > 后续（记录，非本 epic）：让 `reconcile nats --to-standalone` 从 admin socket auto-source self identity，
   > 使 online 恢复零填空——UX 改进、非正确性，留后续或外审建议。

## minor（采纳并修复）

3. **offline pruneRosterPeers 的 gen bump 无 change-gate**（`internal/clusteroffline/offline.go`）— 与 online
   PlanClusterNodePrune 的 `WHERE changes()>0` parity gap，re-prune 已删行会二次 bump。**采纳**：累积
   DELETE 的 RowsAffected，仅 `total>0` 时 bump。测试断言从"monotone"收紧为"re-prune 不 bump"。
4. **raiseXferReplicas 读现有 MaxBytes 失败回落 8G**（`transfer.go`）— 小盘 bucket 被静默 raise 回 8G（#21
   footgun 复发）。**采纳**：读失败改**返回 retriable `ErrMetaGroupNotReady`**（两个调用点已处理重试），不用错误
   的 8G 猜测（XferReplicaState 是包级函数、无 b，无法算 disk-aware fallback，故走重试而非猜测）。
5. **filterGhostPeers 在 RaftConfiguration 错误时 fail-open 可能双破**（`internal/broker/topology_reconcile.go`）
   — 罕见（本地读几乎不 error），但正是守卫最该顶住的场景。**采纳**：改**fail-SAFE**——config-read 错误时返回
   `filterSelfOnly`（只 self），reconciler 渲 standalone 或 fail-closed，绝不渲 clustered 覆盖手改 conf。
   （nil cl/node 的 unwired 路径仍 fail-open——无 raft 无 ghost 风险，专家的 fail-open 测试覆盖此路径。）
6. **nodeInCommittedConfig 命名 overstate（latest vs committed）**（`clusterdrain.go`）— **采纳**：改注释说明
   RaftConfiguration 返回 latest（leader 上含 in-flight），leader-gate 是 load-bearing 前提。

## minor（部分采纳 / 驳回，记录理由）

7. **online prune 失败无 in-band signal**（force_single_online.go）— 建议 ForceSingleReport 加 PruneIncomplete
   字段。**驳回加 wire 字段**：best-effort 失败后，幽灵在 `cluster status` 显示 `Inconsistent=true` +
   DATA-PLANE-DEGRADED banner（#20）已提供发现路径，operator `recovery node remove` 收尾（#12-B）。加字段增
   wire 面 + 复杂度，收益低。**记录为 suggestedTest**（注入 prune 失败断言 status Inconsistent + removeGhost 收尾）。
8. **offline prune snapshot-fragile**（RecoverCluster snapshot 含 abandoned，unlogged prune 不在 snapshot）—
   **驳回实现改动**：专家自证近乎不可达（leader post-election no-op 推进日志 + SnapshotForJoin 强制当前
   snapshot 覆盖），正常 grow 路径不触发。**记录为 suggestedTest**（offline-force-single → grow joiner → 断言
   joiner roster 无 abandoned）+ 后续可选 self-heal（leader startup reconcile-prune not-in-config roster，也覆盖
   online best-effort 残留）留 G7。
9. **removeGhost racing re-add / 窗口**（clusterdrain.go）— narrow window（assertNoActiveOp + hashicorp raft
   single-in-flight-config-change 覆盖常见情形）。**采纳注释澄清**（"catches a racing re-add" 措辞已随 #6 注释
   一并校正），不加锁（收益 < 复杂度）。

## 专家新增测试（整合，保留）

- `internal/broker/g2_ghostfilter_test.go`（#12-C 迁移守卫，plan 最高风险项、原无测试）：drop not-in-config
  ghost、keep in-config nonvoter(CATCHING_UP)、always keep self、nil-fail-open。-race 绿。
- `internal/broker/g2_banner_test.go`（#20 静默-503 banner，原无测试）：force-single 未激活+clustered→无 banner；
  激活+clustered→banner；standalone→banner 清除；missing/空 conf→best-effort 无 banner 不 fail。-race 绿。

## 主进程补充测试

- `cmd/tether/g2_decluster_test.go`（BLOCKER 回归）：见上。
- `internal/clusteroffline/g2_prune_test.go`：re-prune change-gate no-bump 断言。
- 已有（Stage-B）：cluster PlanClusterNodePrune、broker RemoveNode ghost、broker xferMaxBytesForCeiling 硬不
  变量、natscluster golden max_file_store。

## 未闭合测试缺口（记录，闭合核验 / 外审 / 后续补）

several 高价值项因逻辑内联（需先 extract 才可 hermetic 测）暂记为 suggestedTests：#10 cold-start 差分诊断
（extract `bootJSUnavailableDiag` 后 table-test tokens）、raiseXfer MaxBytes 保留（embedded-JS）、per-transfer
bucket-aware gate（embedded-JS）、removeGhost ownership+leader guard（2-node harness）、online prune 注入失败
best-effort、jsStoreCeiling 来源阶梯。**deploy-tier drill 20/21 + upgrade-with-ghost 覆盖端到端**。

## Stage-C 阶段三：独立闭合核验（3 专家，run `wf_70476de5-474`）

独立核验结论：**BLOCKER + MAJOR#1（jsStoreCeiling free）+ 全部 4 个 minor 修复 = CLOSED**（用 hermetic 测试
+ 代码/raft 语义推理确认：g2_decluster 测试对 AuthIdentity 路径会 FAIL 故真钉住根因；filterSelfOnly 经 Render
zero-routes fail-closed 证实无法 de-cluster 健康多节点集群；raiseXfer retriable 在两个调用点不卡正常 raise）。
**3 个驳回项理由成立**（online prune 靠 status Inconsistent+banner 发现；offline prune 与 marker 同 durability；
removeGhost 窗口 narrow）。

核验抓到 **2 个我修 MAJOR#2 时引入的 NEW 缺陷，已修**：
- **NEW MAJOR（doc）**：三处 `--broker-nkey ... from broker.yaml` 事实错误——broker.yaml 只存路径
  （serveconf.go:48），bus nkey 在 secrets_dir 的 broker.nk seed（派生 pub）或 cluster_nodes.bus_nkey_pub。
  **已修**：online 指引 + runbook §2.2/§3.2 三处改指正确来源。
- **NEW minor**：offline force-single 对 already-standalone conf 仍打印 destructive `rm -rf` JS-store 警告 +
  假称 "de-clustered"，违反 plan §4 "already-standalone = proven no-op" 铁律。**已修**：deClusterStandaloneConf
  返回 `changed bool`，already 分支不打印 destructive 警告、改说 "already standalone (no change)"。

剩余（记录，不阻塞）：filterGhostPeers 的 fail-safe 分支无直接测试——但 hashicorp raft `GetConfiguration()`
synchronous respond(nil)、wired node 上 `RaftConfiguration()` 实际从不 error，该分支 practically unreachable，
经推理 + 下游 Render fail-closed 核验。

## 硬闸状态

**全过**：lint 0；build + vet 0；touched 包（cluster/clusteroffline/natscluster/natsconf/cmd-tether/broker）
`-race` 全绿；`make test`（全包）exit 0；`make e2e`（全矩阵）**实质绿**——两次撞 pre-existing flake（`DrainLeader`
时序 + `TestManifestNoSecrets` 子串误匹配随机 pubkey 里的 "SUA"，均单独重跑绿、非 G2 回归），`d7
ForceSingleRecover` 断言我更新为 prune 新行为（`rosterBefore - abandoned`，同时钉住"restart 不 resurrect
pruned peers"）。**deploy-tier**（sim server weilandserver，`--build build` re-bake 新 tether）：**drill 21 GREEN
8/8**（#21：4g tmpfs 上 tier-B push works + 文件真落地）；**drill 20 GREEN 14/14**（改 OFFLINE force-single 验
#20 auto-de-cluster→标准 standalone conf→JS-store reset→tier-B WORKS at N=1 + #12 prune；online force-single 的
dwell 被 #23 `Restart=always` survivor bounce 反复重置、时序不可靠，非 G2 bug，故 drill 走 offline 路径）。
