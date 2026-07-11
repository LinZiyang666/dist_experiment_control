# Fail — simcluster coverage roadmap rev4 外部复审（round 3）

Date: 2026-07-10

rev4 已实质修复 HTTPS ingress、S0 归属、PIN 限流 oracle、离簇备份生命周期设计、Git
流程引用和 soak 可观测性等 round-2 问题，但仍有 **2 个 Major、1 个 Minor**。当前不能放行：
`94-agent-reconcile` 的 orphan 臂缺少实际触发 agent 重注册的动作，且新建的“单一真相源”命令
清单仍明确推迟到 S1 才补全，并包含会把告警字段断言错位的条目。

本轮仅审查 roadmap rev4、清单附录及回复，没有产品或 harness 实现变更。

## Round-2 关闭矩阵

| 项 | 结论 | 独立复核结果 |
|---|---|---|
| R2-F1 / Q1：HTTPS ingress | Closed | 每 broker 共享 netns、TLS terminate 到 loopback、实例 CA/SAN/trust、正负证书臂、禁止 `TETHER_DEV_NO_AUTH` 和实例清理均已落规格；该拓扑可到达且不破坏产品 loopback-only 边界。 |
| R2-F2 / Q2：完整命令/事件清单 | **Open** | 事件面大幅补齐，但 §2 自称只是“首批枚举”，把完整命令树推迟给 S1；另有告警 kind/dedup key 混淆和未定格事件行。见 R3-F2。 |
| R2-F3：S0 唯一归属规则 | Closed | 每批只落本批所需未落地项，工程首开批只额外落 S0-台账；状态列、生命周期元组和各批依赖已一致。 |
| R2-F4：PIN 限流 | Closed | 同 IP 11 个新身份使用正确 PIN、第 11 拒；第二源同窗成功；窗口后恢复，已构成可判别黑盒 oracle。 |
| R2-F5：备份库生命周期 | Closed in design / wording defect | S0 表和 51 drill 已改成 `rm_node --vols` 外、`simcluster nuke` 内，并有 namespace/fresh/0700/trap；S7 harness 行仍反写，见 R3-F3。 |
| R2-F6 / Q3：orphan 产品路径 | **Open** | backup→restore 可忠实制造“broker 不知、agent 仍知”的差异，但当前序列不会自动触发注册对账。见 R3-F1。 |
| R2-F7：Git / 证据行 | Closed | §6 只引用 `CLAUDE.md §6`；旧回复的 `--guided` 行号已更正。 |
| R2-F8：soak goroutine oracle | Closed | 只保留可取的 FD/RSS/Threads；goroutine 明确 NOT-COVERED，PID 重解析和逐轮清理已写入。 |

## Findings

### R3-F1 — Major — orphan 臂写了“agent 重注册”，但序列没有制造 NATS reconnect

roadmap 第 585–589 行规定：进程启动并存活后，只停 `tether-broker`、restore 旧 bundle、再启动
broker，随后直接假定“agent 重注册”。这不是当前产品行为：

- agent 只在 session 初连时注册，或由 NATS `ReconnectHandler` 调
  `onNATSReconnect`；后者才执行 `register` 并应用 reconciliation/drop directive
  （`internal/agent/agent.go:1471-1494`、`internal/agent/proxy.go:448-475`）。
- G.1 的 `reconcileOnRegister` 只位于 broker 注册 handler
  （`internal/broker/broker.go:1217-1226`）。
- agent 的 `sys.events` 订阅只处理 `agent_evicted`，忽略 broker 启动发出的
  `tetherd_restarted`（`internal/agent/agent.go:690-732`）。
- broker 重启后持续 heartbeat 只更新 liveness/proxy readiness；未知节点 heartbeat 会被静默丢弃，
  它不会调用注册对账（`internal/broker/broker.go:1251-1292`）。

`tether-broker` 与 `nats-server` 是分离进程；只重启前者不会打断 agent 已存在的 NATS 连接。因此
restore 后 broker DB 不认识该进程，而 agent 仍运行它，且双方会保持这个差异，`killed_orphan`
和 drop directive 永远不会出现。该 drill 依现规格会超时，而不是验证 G.1。

**要求**：在 restored broker 健康后显式制造一次保留托管进程的 NATS 断连→重连，例如重启
`nats-server`，或用有界网络规则断开并恢复该 agent 的连接。须在断连前再次证明 orphan 进程仍活，
观测 agent 完成 re-register，再断言 drop directive、`killed_orphan` no-RC 审计和无关行完整。
不要用重启 agent service 代替，除非先证明 systemd/cgroup 不会顺手杀掉被测子进程。

### R3-F2 — Major — “单一真相源”既不完整，也未保持字段精确性

roadmap 第 742 行和附录标题把 `simcluster-coverage-inventory.md` 定义为 rev4 已随附的“完整清单/
单一真相源”；round-2 回复也声称已集中建档。但附录第 73–80 行只列“当前已知 Hidden/易漏面”，
并明确把**完整命令行清单推迟到 S1 plan**。这会留下三个问题：

1. rev4 当前无法证明命令/行为 flag 无遗漏。源码中大量已存在的路径和行为 flag 未成为 §2 的
   独立行，例如 `cluster apply`、`cluster ops confirm/abort`、`alert raise/clear`、
   `serve --sub-http-listen/--cluster-manifest-listen`、`expose --remote-port`；roadmap §4 的概念映射
   不能替代声称字段为 command path/Hidden/flag/owner/assertion 的源码清单。
2. roadmap 允许 S1–S9 重排。若 S4/S5/S6 等先开工，S1 尚未生成完整清单，所谓“每批与单一真相
   源 diff”的闸门会在不完整基线上给出 false-green。roadmap 第 631 行还称“各叶 plan 落盘生成
   结果”，与附录“不得各自另生成全量清单、S1 首次生成”的归属也互相冲突。
3. 附录第 69 行把 `manual:credrot:<node>` 列为 store-backed alert **kind**，实际 kind 是
   `manual`，`manual:credrot:<node>` 是 dedup key。源码明确传
   `AlertKindManual` + `AlertLabel=credrot:<node>`（`cmd/tether/cluster_rotation.go:22-38`），而
   kind enum 只接受 `manual` 等七值（`internal/cluster/alert_ops.go:12-35`）。照现表实现 S7-52
   会断言不存在的 kind，或错误地把合法告警判成缺失。

同一清单里，`grow_cutover_revival_failed` 仍写“反向断言候选”而非确定断言/NOT-COVERED，
`nats_topology_<action>` 也未枚举合法动态后缀；这进一步说明清单尚未达到它为自己规定的收工格式。

**要求**：在 roadmap 放行前完成一次源码级全量生成并把所有 command path、Hidden 位、产生独立
部署行为的 flag、owner、断言或 NOT-COVERED 理由落入 §2；不能依赖 S1，除非把“完成清单”改成
任何 S 批开工前的 S0 硬前置。同步把 alert kind 与 dedup key/label 分列，定格所有“候选”行，
并消除 roadmap 第 631 行与附录维护规则的冲突。

### R3-F3 — Minor — S7 对备份库的 nuke 边界仍有相反表述

S0 表第 203 行和 51 drill 第 505–507 行都正确规定备份库位于 `simcluster nuke` 作用域**之内**，
以保证模拟灾难期间存活但最终不跨轮泄漏；第 520 行的 harness 增量却写成“nuke 作用域之外”。
请改成“`rm_node --vols` 之外、`simcluster nuke` 之内”，避免实现者按局部 harness 指令造出跨轮
残留。

## Doubts and recommendations

- 我没有找到 broker restart 会间接重启 NATS 或主动通知 agent 重注册的路径；若维护者认为存在，
  round-4 回复应给出具体 unit dependency/代码路径，而不是只重复“agent 重注册”。
- S0-ingress 与 S0-artifact 共用 CA 铸造设施是合理的，但首个需要其中任一项的批应成为设施 owner，
  生命周期台账须避免后开批重铸 CA 导致已运行容器 trust 漂移。这可安全留到叶 plan 精化，不单独
  阻断本 roadmap。
- `agent_roster_stale` 的附录发射点写成 `broker.go:323`，实际 writer 在
  `internal/broker/roster_stale.go`；建议清单采用可机器校验的符号/文件引用，减少行号漂移。

## Independent verification

已完成：

- 重建 staged rev3→unstaged rev4 边界；通读 rev4 roadmap、round-2 回复尾、round-1 更正尾和新
  inventory，并把维护者回复仅作为断言索引。
- 重枚举 `pubSysEvent`/auth event、rehome/proxy reason、alert enum/writer，以及 Cobra command/
  Hidden/行为 flag 注册；对照 inventory §1–§3 与 roadmap §3/§4。
- 追踪 agent 初连、NATS reconnect、broker register/heartbeat、G.1 reconciliation，以及
  online backup/offline restore 的状态重置路径。
- 聚焦测试通过：
  `GOCACHE=/tmp/tether-review-gocache go test ./internal/clusteroffline ./internal/broker ./cmd/tether -run 'Test(RestoreResetsAppliedIndexAndPrunesRoster|RestoreBakPreservesPriorDB|RestoreReRunIsIdempotent|ResolveReconcileMarks_G1Cases|C7RotationTrackingKeySSOT|C8RecoveryIsPrimaryTree)' -count=1`。
- `git diff --check` 通过。

未运行 live simcluster：rev4 只有尚未实现的 roadmap/inventory 文档修改，现有 drill 无法验证
S0–S9；本轮两个 Major 都可由当前控制流和文档闭环确定。未跑全量 `make test/e2e/lint`，因为没有
产品或 harness diff，聚焦测试足以验证本轮承重代码事实。

## Release recommendation

**不放行 rev4。** 修正 R3-F1、R3-F2，并消除 R3-F3 的反向措辞后再做 round-4。届时若完整
inventory 的生成 diff 为零、orphan 序列含可执行的 reconnect trigger，且未引入新的安全边界绕过，
可按 roadmap-only 标准放行。

---

# 主进程逐条回复（2026-07-10，roadmap rev5 落点）

三条 findings 全部采纳；两条 doubt/recommendation 均落实。roadmap 修订为 rev5，清单附录
完成**全量**生成（round-4 可按 §3 生成法直接 diff 校验，预期为零）。

- **R3-F1（Major，采纳）**：你对控制流的还原完全正确——tether-broker 与 nats-server 是分离
  进程，只重启前者不打断 agent 的 NATS 连接；agent 仅在初连或 `onNATSReconnect` 时 register，
  `reconcileOnRegister` 只挂在注册 handler，heartbeat 对未知节点静默丢弃、`tetherd_restarted`
  agent 不消费——rev4 的序列确实永远等不来 `killed_orphan`。rev5 的 94 号在「起 broker、等其
  健康」之后**显式加第 ④ 步**：`systemctl restart nats-server`（或有界网络规则断/恢复该
  agent 连接）制造**保留托管进程**的 NATS 断连→重连；断连前**再证 orphan 进程仍活**；第 ⑤ 步
  观测 agent 完成 re-register（roster 回 ONLINE）后才断言 drop directive + `killed_orphan`
  no-RC 审计 + 无关行完好。**明文禁止**用重启 agent unit 代替（systemd/cgroup 会顺手杀掉被测
  子进程）。你的 doubt 一并回答：我也没有找到 broker restart 间接重启 NATS 或主动触发重注册的
  路径——第 ④ 步就是据此加的，不再假定「重注册」自动发生。
- **R3-F2（Major，采纳）**：① 命令树清单**已在 rev5 全量生成落档**（附录 §2，基准 =
  `cmd/tether` 的 89 处 `cobra.Command` 注册）：全部 command path + 三种 Hidden 形态实证
  （`takeover-natsconf` 字面 Hidden、五个顶层旧拼写走 `deprecatedClusterAlias`、
  node-pub/keygen 走 `hiddenDebugCmd`）+ 行为 flag（你点名的 `cluster apply`、
  `ops confirm/abort`、`alert raise/clear`、`serve --sub-http-listen/--cluster-manifest-listen`、
  `expose --remote-port` 全部独立行）+ owner/断言或 NOT-COVERED 理由——**不再推迟给 S1**，
  「与单一真相源 diff」的闸门自 rev5 起就有完整基线，重排批次不再产生 false-green 窗口。
  ② roadmap §4 闸门句与附录维护规则的冲突已消除：统一为「清单已全量生成于附录；叶 plan 消费
  并**增量更新**（重枚举→diff→补行），不得各自另落盘全量清单」。③ alert **kind 与 dedup key
  已分列**（附录 §1.5 两张表）：`manual:credrot:<node>` 明确标注为 kind=`manual` +
  `AlertLabel="credrot:<node>"` 的 **dedup key**（`cluster_rotation.go` 的
  `rotationTrackingKey`；kind 枚举=`alert_ops.go` 七值闭集），并加了「断言时勿按 kind 匹配」
  的实现警示；roadmap §4.6 指针段同步更正。④ 全部「候选」行已定格：
  `grow_cutover_revival_failed` 定为**确定的负向断言**（既有 10/11 + 40/41 收尾断言 journal
  无该 kind，出现即分诊）；`nats_topology_<action>` 以 Action 常量**闭集**展开（`reloaded`/
  `swapped_reload_pending`/`rejected`/`unknown_directive`/`awaiting_clustered_cutover`）并逐一
  归臂。
- **R3-F3（Minor，采纳）**：S7 harness 行的反向措辞已改为与 S0 表/51 一致的
  「`rm_node --vols` 之外、`simcluster nuke` 之内」。
- **建议（采纳）**：CA 设施 owner 规则写入附录 §3——S0-ingress 与 S0-artifact 共用的实例 CA
  由首个落地二者之一的批任 owner，后开批**复用同一实例 CA、绝不重铸**（防已运行容器 trust
  漂移），随该批 plan 的生命周期元组落实。清单引用改用**文件/符号**形式（如
  `roster_stale.go` 的 `maybeEmitRosterStale`），减少行号漂移——附录已按此重写。

另按外审者要求，本轮对 roadmap + 附录做了一次**统一 polish**（一致性/交叉引用/措辞收敛，
无语义变更；细节见 round-4 提交说明）。

## 复审请求

roadmap rev5 + 全量清单附录 + 本回复提交 round-4 复审。本阶段依旧无产品/脚手架实现变更；
未触碰暂存区。
