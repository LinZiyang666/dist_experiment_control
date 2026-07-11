# Pass — simcluster coverage roadmap rev7 外部复审（round 6）

Date: 2026-07-10

rev7 已系统性关闭 round-5 的两个 Major：命令面不再按外审点名增量手补，而是按构造后的 Cobra
树重建；restore 也恢复为正确的 never-escapable typed-confirm 模型。本轮未发现 Blocker/Major，
确认 **1 个 Minor**，不阻断 roadmap-only 放行。

## Round-5 关闭矩阵

| 项 | 结论 | 独立复核结果 |
|---|---|---|
| R5-F1：构造树与全量 inventory 非零 diff | Closed | 94-path 构造树口径成立；local/persistent/inherited、command/flag Hidden、此前遗漏的 agent/join/doctor/recovery/serve flags 均已显式列出或落入受约束的四名排除规则。高风险 flags 有 drill/断言。 |
| R5-F2：restore 被误归 machine-confirm | Closed | restore 已从 machine-confirm 行删除；三断言分别覆盖 provenance mismatch、Hidden `--yes` 拒绝、flag+env 正确仍不得非交互绕过。与 `machineEscapable=false` 和既有测试一致。 |
| Persistent flag 采集 | Closed | §3 已加入 `PersistentFlags()` 并与 local/inherited 去重，group/root 定义位置不再依赖子命令偶现。 |
| 首开批 generator 归属 | Closed | 生成器随 S0-台账由工程首开批落地，不再绑定 S1，批次重排无空窗。 |

## Finding

### R6-F1 — Minor — `agent --nats-url` 不应被描述为 ctl-side transport

附录 §2 的统一排除规则把所有 `--nats-url` 描述为“ctl 侧连接串”，只保留
`serve --nats-url` 例外；但 `tether agent --nats-url` 也是 daemon 的部署 seam，它决定 agent
连接的 broker pool，与 serve 一样不是一次性 ctl transport。当前 agent 行因此没有列该 flag。

这不构成覆盖缺口：roadmap S1-60 的 agent 主旅程和 S9-96 的路径钉定都明确使用 agent NATS
连接，后者还要求注入前确认 connected server，所以不会造成未测试功能面。建议在进入实现前把
agent 加为统一排除规则的第三个例外，并在 agent 行恢复 `--nats-url`，避免生成器把错误分类永久化。

## Doubts and recommendations

- 独立计数得到 `newRootCmd()` 构造树 94 paths；显式调用 `InitDefaultCompletionCmd()` 后为 99
  （completion root + 四 shell）。rev7 把 94 作为源码构造树、completion 作为运行期另注记，口径
  可接受。未来生成器应把这个 convention 写进测试，防 Cobra 版本升级改变默认注入时机。
- §2 的四名统一排除规则是合理的降噪机制，但生成器只能验证名字，不能判断语义。`--nats-url`
  已证明同名参数可同时属于 ctl transport 和 daemon deployment seam；例外表仍需人工审查。

## Independent verification

已完成：

- 重建 staged rev6→unstaged rev7 边界，通读 roadmap、inventory 和 round-5 回复；回复只作索引。
- 临时构造 `newRootCmd()` 递归计数：completion 初始化前 94、显式初始化后 99；诊断测试随后删除，
  工作树无 `cmd/` 残留。
- 逐项反查 agent、join、doctor、reconcile、force-single、resnapshot、restore、incident export、
  node remove、serve 的真实 flag/Hidden 和控制流，并核对统一排除规则。
- 验证新增高风险归属：`accept-audit-loss` 双臂、doctor 错输入对照、fresh-host raft override、
  incident O_EXCL、join identity/provenance、restore 三断言均具备可执行 setup 与判别 oracle。
- 聚焦测试通过：C8 recovery tree、machine-confirm 双因子、restore never-escapable、proxy event、
  G.1 reconcile、restore reset/idempotence。
- `git diff --check` 通过。

未运行 live simcluster：rev7 仍是 roadmap/inventory，没有 S0-S9 实现；当前放行判断针对规格完整性
和可执行性。真实 deploy-tier 验证由各 S 批的既定 workflow、真 server 单跑与批收尾全量承担。

## Release recommendation

**放行 rev7 roadmap。** R6-F1 可在首个 S 批落结构生成器或对应叶 plan 时修正，不要求再开一轮
roadmap 外审。后续任何生成器非零 diff、新行为 flag 无归属、或 restore 确认模型回退，都应重新
触发该批的外审闸门。

---

# 主进程回复（2026-07-10，随 commit 落地）

- **R6-F1（Minor，采纳——一行分类勘误随本次 commit 顺手修正，不等首个 S 批）**：附录 §2
  排除规则的例外从两个扩为三个（`agent --nats-url` 与 `serve --nats-url` 同归 **daemon 部署
  seam**——它决定 agent 拨入的 broker/seed list），agent 行恢复 `--nats-url` 并补 S9-95 的
  seed-list 重连语义归属。生成器落地时按此分类，不把错误分类永久化。
- 两条 recommendation 记入待办：completion 的「构造树 94 / 运行期注入后 99」口径写进未来
  生成器测试（防 Cobra 版本升级改变注入时机）；排除规则例外表保持人工审查（生成器只验名字）。
- 感谢六轮外审。roadmap（rev7）+ 清单附录（rev7 + R6-F1 勘误）随本 commit 定稿；后续按 §6
  开工约定逐批推进。
