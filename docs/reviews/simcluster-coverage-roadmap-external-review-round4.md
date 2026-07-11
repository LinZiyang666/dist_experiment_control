# Fail — simcluster coverage roadmap rev5 外部复审（round 4）

Date: 2026-07-10

rev5 已关闭 R3-F1 的 orphan reconnect 缺口和 R3-F3 的备份库措辞冲突，也正确拆分了 alert
kind/dedup key；但 R3-F2 **仍未关闭**。新附录覆盖了可达 command path，却没有覆盖它自己承诺的
全部 Hidden 位和“产生独立部署行为的 flag”。本轮确认 **1 个 Major、2 个 Minor**，暂不放行。

## Round-3 关闭矩阵

| 项 | 结论 | 独立复核结果 |
|---|---|---|
| R3-F1：orphan 缺 re-register trigger | Closed | restore 后显式重启 NATS（或有界断连/恢复）会触发 agent `ReconnectHandler`；register 自带 no-responder/leader-unavailable retry，最终进入 `reconcileOnRegister`。断连前复证进程仍活、禁止重启 agent unit、最终 drop + audit + 无关行 oracle 均已写入。 |
| R3-F2：完整且精确的单一清单 | **Open** | command path 基本齐全，alert 字段也已修正；但 Hidden flag 和多项安全/破坏性行为 flag 仍不在清单，生成法也无法证明它们无遗漏。见 R4-F1。 |
| R3-F3：备份库 nuke 边界 | Closed | S0、51 drill、S7 harness 均统一为 `rm_node --vols` 外、`simcluster nuke` 内。 |
| CA owner / 引用建议 | Mostly closed | 首个使用批持有 CA、后批复用且不重铸已入 §3；仍有一处 agent 文件引用不精确，见 R4-F3。 |

## Findings

### R4-F1 — Major — “源码级全量清单”仍遗漏安全门和破坏性行为 flag

附录第 88–92、162–166 行声称以 89 处 `Use:` 和 flag 定义完成了全量生成，并规定行为 flag
覆盖所有“产生独立部署行为”的参数。独立对照实际注册代码后，command path 数量大体对得上，
但 flag 列仍有明确非输出类遗漏：

- `cluster recovery node remove` 只列 `--manual`，漏掉 `--force` 和 `--confirm-node-id`
  （`cmd/tether/cluster.go` 的 `newClusterRemoveCmd`）。前者允许带有 expose 的失败 voter 被强删，
  后者是 unattended machine-confirm 的双因子安全门。
- `cluster add` 漏掉 `--preserve-js-data`、`--auto-confirm-catchup`、`--dry-run`、
  `--notify-webhook`，以及改变落盘位置/配置的 `--data-dir/--db/--config`。其中 preserve 直接决定
  former-N1 JetStream 灾难操作的保留结果，auto-confirm 改变 BLOCKED op 的人工确认边界。
- `cluster upgrade` 漏 `--notify-webhook`；`cluster init` 漏 `--dry-run` alias、
  `--confirm-node-id` 和 `--config`；`cluster transfer-leader` 漏 `--timeout`；`cluster retire`
  漏 `--timeout/--secrets-dir`。
- `serve` 漏 `--nats-conf-path/--nats-server-bin/--admin-socket/--config/--db` 等实际部署
  seam；agent daemon 漏 `--log-level/--log-json`。
- Hidden 位并不只有 Hidden command。源码还有 `cluster drain --retire`，以及 raw remove/init/add/
  force-single/resnapshot/rejoin/restore 等 Tier-2 命令通过 `registerYesRejector` 注册的 Hidden
  `--yes`。这些 flag 的价值正是让危险命令 fail-closed 并输出明确拒绝，而附录完全未登记。

这些不是格式细节：当前 roadmap 的“全功能面无遗漏闸”会把未列、未归属、未断言的破坏性路径
判成完整，正是 R3-F2 要消除的 false-green。仅 `grep -rn 'Use:'` 也只能数构造点，不能证明注册
可达性、继承 flag、Hidden flag 或行为 flag 完整。

**要求**：用实际构造后的 Cobra tree（递归 `Commands()` + `LocalNonPersistentFlags()` /
`InheritedFlags()`，记录 command/flag Hidden）或等价 AST 工具生成机器可 diff 的清单；至少补齐
上述 flag，并为每个行为 flag 指定 drill/断言或明确 NOT-COVERED。生成器输出与附录归一化后必须
零 diff，不能再用“89 处 Use”替代全树证明。

### R4-F2 — Minor — `/sub` 三项被标成 reason，实际是 `pubSysEvent` kind

附录 §1.4 和生成法第 168 行把 `sub_render_empty`、`proxy_no_ready_nodes`、`proxy_partial`
称为 proxy reason；roadmap 第 757–758 行也沿用该措辞。实际
`internal/broker/proxy_cluster.go::decideProxyEvents` 返回的是 event kind，
`emitProxyCountEvents` 将它们直接传给 `pubSysEvent(kind, fields)`；payload 只有
`sid/ready/capable`，没有对应 reason 字段。请把表头/边界/生成法统一为 event kind，避免叶 plan
错误地在 `reason` 字段中查找而得到假失败。

### R4-F3 — Minor — orphan 段把 `onNATSReconnect` 归到了错误文件，且 ONLINE 不是重注册证据

roadmap 第 590–591 行写成 `internal/agent/agent.go` 的 `onNATSReconnect`；回调注册在
`agent.go`，函数本体位于 `internal/agent/proxy.go`。更重要的是，restore 的旧 bundle 已含该 node，
broker 启动后 heartbeat 本身即可使 roster 回 ONLINE；所以第 595 行的“roster 回 ONLINE/注册事件”
不应写成二选一。请以**断连后的新 `agent_registered` event（或 agent 明确 re-register 日志）**为
重注册证据；最终 drop directive + `killed_orphan` audit 继续作为 reconciliation 的承重 oracle。

## Doubts and recommendations

- “全量清单”最好由可重复执行、提交在仓库中的生成器产出；人工表即使本轮补齐，后续 flag 增删仍
  很容易再次漂移。生成器可以只校验结构，覆盖归属/断言仍由人审。
- 重启 NATS 会同时让 broker 和 agent 重连；agent `register` 已对 no responders 与 transient
  leader-unavailable 重试，因此顺序可执行。叶 plan 仍应给 re-register 和最终 kill 独立超时，便于
  区分“broker 未恢复”“agent 未注册”“directive 未执行”。

## Independent verification

已完成：

- 重建 staged rev4→unstaged rev5 边界，通读三份修改及 round-3 回复，回复仅作断言索引。
- 枚举生产 `cmd/tether` 的 `Use:`、`AddCommand`、三种 Hidden command 构造、全部 `MarkHidden`/
  `registerYesRejector` 和 flag 注册，并逐项对照 inventory §2。
- 重枚举 `pubSysEvent`、auth emit、rehome/proxy event 常量、NATS topology Action 闭集与 alert enum/
  dedup key；确认 alert 修订正确，定位 `/sub` channel 字段误称。
- 复查 agent register retry、NATS reconnect、broker register/heartbeat reconciliation 和 restore
  状态路径，确认新增 NATS reconnect trigger 可执行。
- 聚焦测试：restore/reconcile/C8 machine-confirm/proxy event 测试通过。C7 admin-socket 测试在受限
  沙箱内因 Unix socket 未出现失败，按相同命令在沙箱外重跑通过；这是环境限制，不是产品失败。
- `git diff --check` 在报告落盘前通过；最终 cached check 记录在 tasklist。

未运行 live simcluster：rev5 仍只有未实现的 roadmap/inventory 文档，现有 deploy-tier 无法验证
未来 94 或新的 inventory gate。当前 Major 由源码注册树与清单直接 diff 即可确定。

## Release recommendation

**不放行 rev5。** R4-F1 补齐并以可重复的树级枚举得到零 diff 后，再修正两个 Minor；若 round-5
没有新的安全/可执行性问题，即可按 roadmap-only 标准放行。

---

# 主进程逐条回复（2026-07-10，roadmap rev6 落点）

三条 findings + 两条 recommendation 全部采纳；每处 flag 注册点逐一核对属实（七处
`registerYesRejector`：`cluster_add.go` add、`cluster.go` node remove/init、
`cluster_backup.go` restore、`cluster_offline.go` force-single/resnapshot/rejoin prepare；
`drain --retire` 的 MarkHidden redirect；add/init/upgrade/retire/transfer-leader/serve/agent
各漏列 flag；`decideProxyEvents` 返回 event kind、payload 仅 `sid/ready/capable`；
`onNATSReconnect` 本体在 `internal/agent/proxy.go`）。清单附录修订为 rev6，roadmap 同步。

- **R4-F1（Major，采纳）**：① 全部点名 flag 已入附录 §2——`node remove` 补
  `--force`（孤儿化语义）+ `--confirm-node-id`（双因子）；`cluster add` 补
  `--preserve-js-data/--auto-confirm-catchup/--dry-run/--notify-webhook/--data-dir/--db/
  --config`；`upgrade` 补 `--notify-webhook`；`init` 补 `--check 的 --dry-run 别名/
  --confirm-node-id/--config`；`transfer-leader` 补 `--timeout`；`retire` 补
  `--timeout/--secrets-dir`；`serve` 补 `--config/--db/--admin-socket/--nats-conf-path/
  --nats-server-bin`；agent 补 `--log-level/--log-json`。② **Hidden flag 面独立成行**：
  `drain --retire`（REMOVED-redirect）与 **Tier-2 Hidden `--yes` 拒绝面**（×7）+
  **machine-confirm 双因子**各一行，全部给了 drill 落点（40 的安全门负例组、22 拒绝门第
  (e) 臂、42/43/50 的 typed-confirm 顺带 `assert_refuses`），§3 对应 drill 规格已加落点句。
  ③ 生成法升级为**构造后 Cobra 树递归**（`Commands()` + `LocalNonPersistentFlags()/
  InheritedFlags()`，command/flag 两级 Hidden；「89 处 Use」降级为不作证明），并采纳
  recommendation：**结构校验生成器随 S0-台账（S1）提交入仓**（本 roadmap-only 阶段不添加
  实现，round-5 请按升级后的生成法对附录 §2 做树级 diff 校验）。
- **R4-F2（Minor，采纳）**：`sub_render_empty`/`proxy_no_ready_nodes`/`proxy_partial` 三项
  在附录 §0/§1.4/§3 与 roadmap §4.6 全部更正为 **pubSysEvent event kind**（标题注明经
  `decideProxyEvents`→`emitProxyCountEvents`→`pubSysEvent`、payload 无 `reason` 字段、
  断言按 kind 匹配——防叶 plan 在 reason 字段查找得假失败）。
- **R4-F3（Minor，采纳）**：94 号修正——`onNATSReconnect` 归属改为「本体在
  `internal/agent/proxy.go`、注册于 agent.go」；重注册证据改为**断连后新的
  `agent_registered` 事件（或 agent re-register 日志）**，并明文「roster 回 ONLINE 不算」
  （restore 旧 bundle 已含该 node，heartbeat 即可翻回 ONLINE）；按 recommendation 给
  re-register 与最终 kill **独立超时**（区分 broker 未恢复 / agent 未注册 / directive
  未执行）。

## 复审请求

roadmap rev6 + 清单附录 rev6 + 本回复提交 round-5 复审。本阶段依旧无产品/脚手架实现变更；
未触碰暂存区。
