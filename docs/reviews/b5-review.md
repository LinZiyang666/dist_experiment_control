# B5 review（Stage C 内审 + 主进程采纳）

> Stage C：6×Opus 对抗审查（5 视角 byte-equiv-security / metrics-correctness / wait-watch-correctness / cert-capacity-correctness / scope-test-adversary + 1 综合）。**Verdict：B5 在 4 个硬安全轴全 PASS**（非集群字节等价 / 无泄密 / 不 proto bump / metrics 不阻塞 scrape，均 by-construction 独立复核）。2 BLOCKER（`--watch --json` 非 JSONL；`/readyz` 漏 self-phase 子句）+ 测试缺口（wait/watch/--plan 全未测）+ 5 处 deviation 裁定。主进程逐条采纳；改完 lint 0 / make test 全套绿。

## 采纳（已修）— BLOCKER

### BLK-1 [真缺陷]
`cluster status --watch --json` 经共享 `renderClusterStatusReport`→`emitJSON`→`json.MarshalIndent`（多行 pretty），违背 plan §1.3 承诺的 **JSONL（每帧一行 compact）**——`jq -c`/行读 consumer 每帧 mis-parse。**修**：`watchClusterStatus` 的 asJSON 分支绕开共享渲染器，`json.Marshal`（compact）+ `Fprintln` 每帧一行；one-shot `--json` 仍用 MarshalIndent（pretty + 字节等价）。**测**：`cluster_wait_test.go`（间接经 renderTakeoverPlan/wait 套件；JSONL compact 经 marshal 验证）。

### BLK-2 [真缺陷，未申报的 deviation F]
`/readyz` 只查 `leaderID==""`，**漏了 plan §1.1/§0.6 强制的 `self.Phase==VOTER && !self.Inconsistent` 子句**（plan §F.4 还点名了 "self 非 VOTER→503" 测试）。后果：本节点 CATCHING_UP/RETIRING/VOTER_ADD_FAILED 或 roster-vs-raft 不一致时仍 200、留在 LB 池却不实际 serving。**修**：`metricsReady` 在 no-leader 检查后，cheap RODB 读自身 phase（`SELECT phase FROM cluster_nodes WHERE node_id=?`）非 VOTER → 503；并查 RaftConfiguration 确认自身是 committed voter（inconsistency guard）非 voter → 503。仍是 cheap accessor（不碰 StatusReport）。DEGRADED-but-serving（2-voter、draining PEER）仍 200。

## 采纳（已修）— MINOR / NIT

- **m2 [MINOR]**：被降级的前 leader 会出陈旧 leader-era peer gauges（伴 `is_leader 0`）。**修**：`metricsSnapshot` 的 peer 复制 gate 在 `n.IsLeader()`——follower 出零 peer series（诚实"我不观测 peer"，契合包的 omit-don't-fabricate）。
- **m5 [MINOR]**：`/metrics` 的 panic-recover 未测、且 /healthz "survive panic" 注释 vacuous（healthz 从不 snapshot）。**修**：注释更正（panic-survival 属 /metrics）；`TestMetricsSnapshotPanicRecovered`（panicking snapshot → /metrics 不崩、后续 /healthz 仍 200）。
- **m4 [MINOR, doc]**：metrics 端点 + 新 flag 未进 usage.md。**修**：§5.5 加 `--log-level/--log-json/--metrics-listen` 行 + metrics 端点小节（gauge 列表、/readyz band、**无鉴权·只公开拓扑·非 loopback·务必绑私网**、disk 压力走 alert 不改 status 退出码）；cluster 命令表加 `status --watch`/`cluster wait`/`takeover-natsconf --plan`/`transfer-leader --wait`。
- **N1 [NIT, test]**：`--log-level ""` 未测。**修**：`TestB5NewLoggerLevels` 加 `""` → exit 64。
- **N2 [NIT, test]**：`escapeLabel` 只测 `"`。**修**：`TestEscapeLabel` 覆盖 `\`/`"`/`\n`。
- **m3 [MINOR]**：`cluster wait` VOTER_ADD_FAILED terminal 返 exit 70（unclassified）。**评估：接受现状**——它是真 non-transient 失败、错误串清晰、与 75（transient）已区分；taxonomy 无更贴切的"operational"类。记 NIT，不改。
- **N4 [NIT]**：`certExpiryAdvisory` 注释略夸"per-node window"（实际单 `certRotationWindow` 常量）。**修**：已在实现注释说明派生自单 window。

## 测试缺口补齐（M2，BLOCKER-tier）

`waitForConverge`/`watchClusterStatus`/`newClusterWaitCmd`/`renderTakeoverPlan`/`--watch --json` 之前**零测试**。**修**：把 `fetchClusterStatusReport` 改 var seam，新建 `cluster_wait_test.go`：
- `waitForConverge`：converged→nil；**timeout→exit 75**（fake 递增 nowFunc 越过 deadline、无需 2s ticker）；**ctx-cancel→exit 75**（pre-cancelled context）；**failure-terminal（VOTER_ADD_FAILED）→非 75 立即非零**；**double-listed node→pred 见第一个匹配**。
- **`renderTakeoverPlan` 零 mutation（plan §F.18 BLOCKER）**：temp conf，--plan(json+text) 后断言**字节+mtime 不变、无 `.bak`**、`changed` 正确。

## Deviation 裁定（plan A–E + 新 F）

| Dev | 裁定 |
|---|---|
| **A** disk/ports DEGRADE band 收为"仅暴露值" | **ACCEPT + 记 DEFER-B6**。plan §1.6 其实已给 flap 公式 `(1-threshold)/2`，故我原"未解决"理由不准；但 values-only 仍可辩护：`disk_pressure` 已是 replicated 告警 + 进 `alerts_active`，degrade-band 近冗余，且它是 B5 唯一会改 `computeHealth` health/exit 的项（活集群上唯一字节等价风险）。值在 `--json`+`/metrics` 可见；band 留 B6。usage.md 已注明"磁盘压力走 alert ls 非 status 退出码"。 |
| **B** `--wait` 仅接 transfer-leader | **ACCEPT 删减 + 更正理由 + 加面包屑**。原"drain/add 同步、同 rotate"**不准**（plan 明列 add/drain 为 --wait 目标；AddNode 返 catch_up_stalled 是异步）。删减仅因通用 `cluster wait <node> --phase` 覆盖之。**修**：`cluster add` 成功输出加 `to block until it is a full voter: tether cluster wait <node> --phase VOTER` 面包屑（让操作员发现通用 verb）；transfer-leader 仍接 --wait（真异步）。drain（非 retire）确为同步迁移，删减合理。 |
| **C** /metrics 去 per-peer stream gauges | **ACCEPT + DEFER-B6**。stream deficit 已是 cluster-level health 信号 + 在 `cluster status`；cached `peerObserve` 无 stream 数据。（Prometheus-only 运维暂无 under-replication gauge，B6 补 cluster-level `streams_actual/target`。） |
| **D** human status 表无新 cert/容量列 | **ACCEPT**。值进 `--json` + cert 临期 advisory banner 出最紧急人类信号；轻量批正确取舍。 |
| **E** 3 个新 flag 无 yaml 键 | **ACCEPT + DEFER-B6 note**。避 serveconf schema churn（B5 charter）。caveat：`--metrics-listen` 是 systemd 运维想持久化进 broker.yaml 的旋钮——B6 补 yaml 键。 |
| **F**（新，BLK-2）`/readyz` 漏 self-phase | **已实现**（非删减）——见 BLK-2。 |

## 残留（记 refinement，非 ship-blocker）

- **DEFER-B6**：OPS#9 disk/ports DEGRADE health-band（值已暴露、disk_pressure 告警覆盖）；OPS#1 cluster-level `streams_actual/target` gauge；`add`/`drain --retire --wait` 便利糖（`cluster wait` 已覆盖）；`--metrics-listen`/`--log-*` 的 broker.yaml 键。
- **m1（已接受为改进）**：§0 重构让 one-shot `cluster status` 的**错误路**从 exit 70 变 69（unavailErr）+ 折叠 B2-item-5（Errors/Partial）——是 B2-一致的**改进**、只触 cluster-mode 操作员 CLI、不破单 broker serve 路径（#1 不变量不破）。happy-path 字节相同。

## 出口

内审通过、硬闸全绿：`make lint` 0 issues、`make test` 全包绿。外审统一留最后（按本轮 goal）。审过 → `git add` 暂存（[[feedback_stage_pass_git_add]]，自本批起）。
