# 文档三拆一 + 挪动 · 内审报告

> 范围：把单体 `docs/usage.md`（HEAD 版 2588 行）按读者拆成 `usage.md`（使用者篇）/
> `broker-ops.md`（broker 运维，新）/ `cluster.md`（集群 HA，新）三册（**刻意保留原始分节编号**），
> 外加 `docs/v2-*.md` 移入 `docs/reviews/`、根 `log.md` 删除、`.gitignore` 忽略
> `docs/devices-ops.local.md`。作者意图视为正确（"假设做对了"），本轮只审**执行是否到位**。

## 1. 概述

- **正文内容保全 = 通过（穷尽核验）**：旧 usage 的 **70** 个 numbered `###` 子节 **1:1** 落到新三册中恰好一份（§7.7 搬回 usage 后落点为 usage 51 / broker-ops 16 / cluster 3，编号集合与旧文档相同、无重复无缺失；另旧 `### broker 主机` 提升为 `## 附录 A`，故新三册 `###` 总数为 71。原报告"87"数字不可复现，已按外审 MINOR-2 更正为可复现的 70），
  落点与声明的映射逐条吻合、无 misfile；对每个匹配小节做 unified-diff，正文**去空行后字节一致**；关键 token
  （端口 `14000-14999`/`8090`/`4222`/`7400`、flag `--sha256`/`--url`、`allow_roots`、`auth_callout`、
  退出码、阈值 `2 GiB`/`8 MiB`、`takeover-natsconf` 等）与告警标记（⚠/不可逆/破坏性/覆盖/危险）在新三册并集中
  **计数不减**。四类核心风险（整段丢失 / 警告被删弱化 / 技术事实静默改写 / 同段重复搬进两份）**均未命中**。
- **问题集中在结构锚点、跨册指针、全仓同步**，非内容丢失。共 **17 条**（0 BLOCKER / 5 MAJOR / 8 MINOR / 4 NIT）。
- **处置：17 条全部采纳并修复**；唯 `requirements.md` 的 `logs/log.md`（NIT-2，本就虚构路径 + 冻结文档 +
  非本次引入）作为 **accepted-residual** 记录不改（详见 §6）。

## 2. 方法

Stage-C 多专家对抗性内审，现场草拟 Workflow 脚本，**全程 Opus 4.8、agent 数为静态常量、专家只读不改实现**：

| 阶段 | agent 数（固定） | 角色 |
|---|---|---|
| Review | **5** | 5 维度 finder：内容保全 / 链接交叉引用 / 读者边界 / 结构导航 / 全仓集成 |
| Verify | **3** | 3 席独立对抗核验：逐条证伪 finder findings + 独立完整性批判查遗漏 |
| Synthesize | **1** | 综合定稿：去重合并、分级 |

> **诚实记录**：第 5 个 finder（repo-wide-integration）只返回占位 `probe`、实际失职。但 **3 席对抗核验者的
> 完整性扫描恰好补齐了它该找的全部 repo-wide 项**（`internal/natsconf` 源码注释、`install.sh`、`devices.md`、
> 错误码索引），且内容保全 finder 独立覆盖了 `log.md`/`CLAUDE.md`/`README`。主进程另**亲自补一次全仓
> `grep` 兜底**（见 §5），净覆盖未受损。这正是"对抗核验 + 完整性批判"设计对单点失职的容错价值。

## 3. Finding 汇总（17 条，含处置）

### MAJOR（5，全修）

| # | 位置 | 问题 | 处置 |
|---|---|---|---|
| M1 | `usage.md` 目录 → §3/§5/§6/§8 | 拆分丢弃 `## 3./5./6./8.` 章标题，目录 4 个自锚 `#3-配置文件` 等跳空 | 在 §3.1/§5.1/§6.1/§8.1 前补回四个 `## N.` 章标题（沿用原文本，slug 与目录锚点对齐） |
| M2 | `broker-ops.md` 目录 → 全部 | 无任何 `## N.`/`## 附录 A` 父标题，目录 8 锚点 7 死 | 补 `## 2./3./5./7./8./9.（broker 侧）` + `## 附录 A：broker 主机目录结构` |
| M3 | 删除 `log.md` → `CLAUDE.md` 悬空 | 452 行实机日志整删未迁移；`CLAUDE.md` §1 doc-map + §7 实机验证指针悬空 | 按"删除有意"处理：移除 §1 doc-map 那行；§7 改指现存 memory + 私有 `docs/devices-ops.local.md`（log.md 的实际继任者） |
| M4 | `CLAUDE.md:11`；`README.md:16` | doc-map/前门仍称 usage 为"全量手册"、未登记 broker-ops/cluster | `CLAUDE.md` §1 改 usage 描述为"使用者手册" + 新增 broker-ops/cluster 两条；`README` 拆成 usage/broker-ops/cluster 三链 |
| M5 | `cluster.md:133` | §5.6 搬入后逐字保留旧自指"`usage.md` 这里只放入口和常用边界"，"这里"实为 cluster.md | 改"`usage.md` 这里"→"本册（cluster.md）这里" |

### MINOR（8，全修）

| # | 位置 | 问题 | 处置 |
|---|---|---|---|
| m1 | `usage.md` ×11 | 跨册裸 §-引用未标册名（§5.6→cluster、§7.7/§7.3/§8.5→broker-ops、§9.7.1→cluster、§2.1–2.3 跨册） | 逐条补册名；§2.1–2.3 拆成"§2.1–2.2 本册（§2.3–2.4 见 broker-ops.md）" |
| m2 | `broker-ops.md` ×3、`cluster.md` ×2 | 反向裸引用（→cluster §5.6 / →usage §2/§8.3/§5.1）未标册 | 逐条补 `cluster.md §5.6` / `usage.md §2`/`§8.3`/`§5.1` |
| m3 | `usage.md`/`broker-ops.md` 附录 | 附录 A 父标题两册均被删（usage 悬空 `### agent 主机` + A→C 断号；broker-ops 裸 `### broker 主机`） | usage 补 `## 附录 A：目录结构总结`（+broker 主机 breadcrumb）；broker-ops 提升为 `## 附录 A`（并计入 M2 锚点） |
| m4 | `usage.md` §4 速查表 | 表内枚举 serve/cluster/admin/alert 但无跨册详解指针 | §4 顶部加一条 note：serve/admin→broker-ops §5.5/§5.20、cluster/alert→cluster.md §5.6/§5.7；cluster 行内补 `cluster.md §5.6` |
| m5 | `cluster.md` | 无 `## 目录`（另两册有），少册内跳转入口 | 加极简 `## 目录`（脚本生成锚点，已验证 0 死锚）+ 兄弟册 breadcrumb |
| m6 | `distributed-broker-architecture.md` ×3；`install.sh:465`；`cutover.go:25`；`takeover.go:244` | live 文档 + 生产源码注释仍写 `usage.md §2.3/§3.4`（已移入 broker-ops） | 全部改为 `broker-ops.md §2.3/§3.4`（Go 注释改动 vet/build 已过） |
| m7 | `usage.md` §9 前言 + 跳号 | 前言称"列出**全部**已知 code"，但 §9.7/§9.7.1/§9.10 已移出；9.6→9.8、9.9→9.11 跳号无面包屑 | 前言改"使用者侧 code；broker/集群侧见 broker-ops §9.7/9.10、cluster §9.7.1"；两处跳号加 breadcrumb |
| m8 | `docs/reviews/v2-usability-program.md:3,7` | v2-*.md 移入 reviews/ 后，`../v2-usability-proposals.md` 相对路径断链 | `../v2-` → 同级 `v2-`（该文件未随移动，故 `../` 失效） |

### NIT（4，3 修 + 1 accepted-residual）

| # | 位置 | 问题 | 处置 |
|---|---|---|---|
| n1 | `cluster.md` 头部 | "单机用户无需本册"措辞低估 member 面（`alert ls/ack`、`status --remote` 是 ctl 只读命令） | 头部补一句：集群部署下 member/ctl 也会用到 §5.7/§5.6 只读命令 |
| n2 | `internal/natsconf` ×3（`takeover.go:103`/`preflight.go:32`/`preflight_test.go:110`） | 按行号引用 `usage.md:970`（max_payload 调优），拆分令行号漂移 | 改用节级锚 `usage.md §5.16`（max_payload/tier-A 实际在 §5.16 push/pull，非 finder 猜的 §5.14） |
| n3 | `docs/devices.md:5` | 仍单指 `usage.md` 为 tether 操作细节 | 扩链为 usage（使用者）/ broker-ops（运维）/ cluster（HA） |
| n4 | `requirements.md:14/523/525/541` | 引 `logs/log.md`（虚构路径）+ `log.md §系统边界` | **accepted-residual**：`logs/log.md` 本就虚构、非本次引入，且该文档自述"已冻结为历史"，不改冻结文档；仅登记 |

## 4. 修复明细（按文件）

- `docs/usage.md`：补 `## 3./5./6./8.` + `## 附录 A`；目录补附录条目；11 处跨册 §-引用补册名；§4 加跨册指针 note；
  §9 前言改写 + 两处跳号 breadcrumb。
- `docs/broker-ops.md`：补 `## 2./3./5./7./8./9.（broker 侧）` + `## 附录 A：broker 主机目录结构`；3 处反向引用补册名。
- `docs/cluster.md`：修 §5.6 自指（M5）；加 `## 目录` + 兄弟册 breadcrumb；2 处 `§5.1` 补 `usage.md`；头部补 member 提示。
- `CLAUDE.md`：§1 doc-map 改 usage 描述 + 新增 broker-ops/cluster、删 log.md 那行；§7 实机验证指针改指 memory + devices-ops.local.md。
- `README.md`：usage 单链拆成 usage/broker-ops/cluster 三链。
- `docs/devices.md`：操作细节指针扩三册。
- `docs/distributed-broker-architecture.md`：3 处 `usage.md §2.3/§3.4` → `broker-ops.md`。
- `docs/reviews/v2-usability-program.md`：2 处 `../v2-` → `v2-`。
- `scripts/install.sh` / `internal/broker/cutover.go` / `internal/natsconf/{takeover,preflight,preflight_test}.go`：注释里的 `usage.md §2.3` → `broker-ops.md §2.3`、`usage.md:970` → `usage.md §5.16`（纯注释，`go vet ./internal/natsconf/` + `go build ./internal/broker/` 通过）。

## 5. 闭合核验

- **页内锚点脚本核验**（GitHub slug 算法逐份比对 TOC `](#...)` vs 实际标题；§7.7 搬回 usage 后复跑）：`usage.md` 14/14、`broker-ops.md` 8/8、
  `cluster.md` 4/4 —— **三份 0 死锚**。
- **Go 编译**：`go vet ./internal/natsconf/`（含 test 文件）+ `go build ./internal/broker/` —— **通过**。
- **全仓残留 grep**：
  - live 文档/源码指向 `usage.md §{2.3,2.4,3.3,3.4,5.5,5.6,5.7,5.20,7.x,8.4,8.5,9.7,9.10}` —— **0 残留**（剩余命中全在 `docs/reviews/` 冻结历史文档，按约定不改）。
  - 源码 `usage.md:<行号>` 数字锚点 —— **0 残留**。
  - 对已删 `log.md` 的悬空引用 —— **本次新增悬空 0**（`requirements.md` 有已登记历史残余 `logs/log.md`，accepted-residual；不表述为"全仓绝对 0 悬空"）。
  - 指向已移动 `docs/v2-*.md` 的全仓旧路径 —— **0 残留**（外审 MAJOR-1 修 13 处精确文件名 + RE-FAIL 复审再修 1 处 `c-overseer-3.md` brace-expansion 写法，含原 `../v2-*` 断链；改用宽松前缀 `grep -rn 'docs/v2-'`（排除两份拆分报告）= 0）。
- **`## 章标题序列**无重复：usage（§7 仅含 §7.7、附录B 跳空 → broker-ops，TOC 已签路）、broker-ops（§1/§4/§6 跳空非 broker 相关，§7 只剩 7.1–7.6）、cluster（§5.6/5.7/9.7.1 为 `###`，TOC 直链）。

## 6. 假阳性驳回 / accepted-residual

- **probe-1**（finder #5 占位输出）：3 核验者一致 FALSE_POSITIVE —— 纯 scaffolding、无可核验陈述；其对应真实问题（M4）已独立覆盖。
- **严重度下调**（核验者一致，已并入）：结构死锚 finder 报 BLOCKER → 下调 **MAJOR**（正文 100% 保全、可滚动到达，属导航断裂非内容丢失）；多条 bare-ref 由 MAJOR → **MINOR**（有 TOC 册级 breadcrumb 兜底）。
- **audience-boundary §7.7 分区**：内审时采纳 2/3 核验者的 MINOR（视为忠实执行作者 §7.1–7.7→broker-ops 映射）、留异议供外审。**外审裁定拉回 usage.md**（外审 MAJOR-2）——已整改：§7.7 整节移回 usage.md 保留原编号，两册目录 / §7 跳号说明 / `remote_fs.mode` 指针同步，broker-ops 原处留不占 §7.7 标题的跨册提示，三册锚点复跑 0 死锚。
- **NIT-2 requirements.md `logs/log.md`**：accepted-residual（虚构路径 + 冻结文档 + 非本次引入），仅登记不改。

## 7. 开放问题 —— 外审已裁定

1. **`log.md`**：外审裁定**删除、不归档、不在审查范围**。保持现状（已移除 `CLAUDE.md` 悬空指针、改指 memory + `devices-ops.local.md`）。
2. **`requirements.md` 的 `logs/log.md`**：外审**接受 accepted-residual**（改动前已不存在 + 冻结文档）。结论表述收紧为"本次新增悬空 0，另有已登记历史残余"，不表述为"全仓绝对 0 悬空"。
3. **§7.7 分区**：外审裁定**拉回 usage.md**（外审 MAJOR-2）——已整改（见 §6）。

> 本报告为 Stage-C 内审记录；外审通过后方算 done。**外审阶段不 `git add`**（暂存为外审者工作）。
