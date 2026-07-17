Fail

# S6-S8 外部重审报告（round-2）

日期：2026-07-15
审查对象：相对 round-1 已暂存基线的未暂存修改
审查者立场：外部独立审查；开发者回复仅作为待验证陈述，不作为通过证据

## 结论

开发者明确接受 round-1 的 B1、M1-M6、m1，并把逐 drill 工作诚实地写为后续计划；这一点没有
掩饰未完成状态。但本轮实际代码只在 `test/simcluster/lib/assert.sh` 增加两个 API 和计数器，九个目标
drill、suite runner、SSOT 文档均未修改。原 B1、M1-M6、m1 因而全部仍为 OPEN。

新增“第三态地基”本身也未闭环：单 drill 输出 `INCOMPLETE` 并返回 1，`run-drills.sh` 却只识别
`GREEN|RED`，最终把它显示为普通 RED，同时误报“infra failure before drill_end”。现有 43 处
`NOT-COVERED` 文本没有一处调用 `not_covered`，既有 warning-only 路径仍可汇总为 GREEN。因此本轮
不能解除 release blocker，也不能将该机制称为已经完成的机器可读第三态。

## 修改边界

开发者本轮未暂存修改仅有：

1. `docs/reviews/s6-s8-external-review.md`：追加 23 行逐条回复与修复计划。
2. `test/simcluster/lib/assert.sh`：新增 `_AS_NC`、`_AS_SETUP`、`not_covered`、`assert_setup`，并改写
   `drill_begin` / `drill_end`。

本报告和 round-2 tasklist 是外审产物。九个 S6-S8 drill 仍是 round-1 已暂存版本，不属于本轮开发者
修复。临时 runner 探针脚本已删除，不进入交付。

## Findings

### B1 — 第三态没有贯通 runner，机器汇总把已到达 `drill_end` 的 INCOMPLETE 误报成基础设施中断

`assert.sh:85-86` 对 `_AS_NC>0` 输出 `INCOMPLETE` 并返回 1；但 `run-drills.sh:136-138` 的解析器只接受
`GREEN|RED`，`run-drills.sh:189-200` 又仅按 rc 把 suite 分成 GREEN/RED。真实 runner 包装探针得到：

```text
RED  zz-external-review-not-covered rc=1  (no verdict — infra failure before drill_end)
1 drills, 1 RED
```

对应单 drill 日志明确已经执行到：

```text
=== external-review NOT-COVERED propagation probe: INCOMPLETE
```

这不是展示瑕疵：CI、发布汇总和操作员无法区分“验收未覆盖”“产品/断言 RED”“setup RED”与“未运行到
drill_end”。回复所称的“第三种机器可读结果”和“分栏”在 suite 边界不存在。

建议：先定义唯一枚举（例如 `GREEN|PRODUCT-RED|SETUP-RED|NOT-COVERED`）及其 CI 退出语义；让
`drill_end` 输出固定格式/结构化记录，runner 按 verdict 而非仅按 rc 分类、计数和打印；缺少或非法 verdict
才可标为 infra failure。为单 drill→runner→suite 增加状态矩阵回归测试。

### B2 — 新机制零采用，round-1 的 warning-only 假绿与 setup 内外结论冲突完全保留

全仓调用点搜索只在 `assert.sh` 的定义/注释找到 `not_covered` 和 `assert_setup`；九个目标 drill 的调用数均
为 0。与此同时，这九个脚本仍有 43 处 `NOT-COVERED` 文本，其中 40/41/42/43/90/91/92/93 的主题脊
缺口仍通过 `warn`/`log` 记录，不改变 `_AS_NC`。所以 round-1 活体中九项 rc=0/全 GREEN 的假绿行为没有
被本轮代码改变。

至少 16 个 setup 失败分支仍采用 `err "setup: ..."; drill_end; exit 1`，没有增加 `_AS_SETUP`。这会在
同一个日志中先打印 `...: GREEN`，随后进程 rc=1、runner 行又打印 RED；内部 verdict 与 suite verdict
相互矛盾。`assert_setup` 只有在逐调用点迁移后才可能修复该问题。

建议：不能只添加 API。逐个 locked cell 和 prerequisite 做明确迁移；用静态检查禁止目标 drill 中裸
`NOT-COVERED` 文本和 `err setup ...; drill_end`；迁移后针对每个受影响分支做 fail-closed 复跑。

### M1 — 所谓 PRODUCT-RED 没有独立状态，`assert_bug` 复现已知产品缺陷仍计 PASS/GREEN

回复 `external-review.md:176` 声称 GREEN/PRODUCT-RED/NOT-COVERED 并与 setup 分栏；实现中却没有
product-red 计数或 verdict。`assert.sh:53-54` 在已知 bug 命中签名时调用 `_as_pass`；探针结果为：

```text
PASS known [BUG-1] reproduced for the documented reason
=== bug-reproduced: GREEN (1 assertions, 0 gaps) ===
```

反之，`_AS_FAIL` 被汇总文本命名为 `product-fail`，但它同时包含普通 `assert_ok` 失败、预期拒绝意外成功、
错误签名及 “APPEARS FIXED”等不同含义。该字段既不是独立 PRODUCT-RED，也不能可靠表达产品问题类型。

这会继续让 #28、#35、#37-family 等已登记产品缺陷在 suite 中显示 GREEN，与回复和 round-1 建议的落地
状态模型冲突。需要产品所有者明确：签名守护的已知缺陷是否允许 release；无论策略如何，机器结果都不能
把“缺陷被复现”伪装成普通通过。

### m1 — 新 `assert_setup` 缺少 command 时 fail-open 为 PASS

`assert_setup` 在取出描述后没有验证剩余参数。Bash 与 dash 下执行 `assert_setup "desc"` 都令空命令捕获
返回 0，结果是 `_AS_PASS=1`、`_AS_SETUP=0`、最终 GREEN。这个 API 的职责正是关闭 prerequisite 假绿，
因此遗漏命令不应成为成功 setup。

建议：所有 assert API 统一校验最小参数个数；缺少命令/签名必须计 harness/setup failure，并加入 Bash、dash
的参数错误测试。

## Round-1 finding 闭环状态

| Finding | round-2 状态 | 依据 |
|---|---|---|
| B1 | PARTIAL / OPEN | 库内有计数雏形；runner 不识别，目标 drill 零迁移，locked cells 仍可跳过 |
| M1 | OPEN | 92 未改，auth/connect 误命中、tier-B 大小/标签、恢复闭环均未修 |
| M2 | OPEN | 41/43 未改，shrink、live-row、非交互 cutover、rollback 证据未补 |
| M3 | OPEN | 93 未改，HTTP/ready/webhook/CARD/watch/exit-code oracle 未修 |
| M4 | OPEN | 90 未改，否定 pipeline fail-open、return/dedup/raise-clear oracle 未修 |
| M5 | OPEN | 22 与 gotcha ledger 未改，#35 仍未按 MainPID+自动 crash-loop 重新取证/降级 |
| M6 | OPEN | plan/review/roadmap/README/inventory 未改，编号和 landing SSOT 冲突仍在 |
| m1 | OPEN | 40 未改，renderer 正路径、footer、bytes、`.bak` 集合/mtime 未补 |

本轮没有任何 round-1 finding 达到 CLOSED。

## 独立验证证据

- 边界：`git diff --name-status` 在外审产物加入前仅列 developer response 与 `assert.sh`。
- 静态：`git diff --check`、`sh -n test/simcluster/lib/assert.sh`、`dash -n ...` 均通过。
- 状态矩阵：clean=`GREEN/rc0`；not-covered=`INCOMPLETE/rc1`；setup-fail=`RED/rc1`；assert-fail=
  `RED/rc1`；not-covered+setup-fail=`RED/rc1`；bug-reproduced=`GREEN/rc0`。计数器在下一次
  `drill_begin` 可正确归零。
- 参数探针：Bash、dash 的无 command `assert_setup` 均为 `PASS` + `GREEN/rc0`。
- 真实汇总链：临时最小 drill 经原始 `simcluster drill` 与原始 `run-drills.sh --skip-preflight
  --no-retry` 执行，稳定复现 B1；日志位于 `/tmp/simdrills-s6s8-round2-runner/`。
- 调用点：九个目标 drill 共 43 处 `NOT-COVERED` 文本，`not_covered`/`assert_setup` 调用均为 0。
- simcluster：本轮未改远端 fixture、产品路径或九个 drill，且新 API 没有业务调用点；重复跑服务器只会重演
  round-1 脚本。因此未重复全量远端运行。round-1 的九 drill 活体日志和无残留检查继续作为原 finding 证据；
  本轮新增 runner 链已本地端到端执行，临时 drill 未启动任何节点并已删除。

## 疑惑与建议

1. 最终枚举到底是回复中的 `NOT-COVERED`，还是实现中的 `INCOMPLETE`？它是否与 RED 同样阻断发布、但必须
   独立计数？需要先写进 SSOT 和 runner contract。
2. “PRODUCT-RED”到底表示已知缺陷命中签名，还是任意 assertion failure？当前注释、计数名、`assert_bug`
   行为和回复各自表达不同语义。
3. 本轮更像分阶段修复的第一个 checkpoint，而不是可申请闭环重审的版本。建议等 runner、九 drill 和 SSOT
   同一个变更窗口完成后再申请 Pass/Fail landing 审查，避免把未接线 API 当作根因已修。
4. 建议新增 hermetic harness tests，至少覆盖四种 verdict、组合优先级、缺参、runner 解析、suite 总数与退出码；
   再用 simcluster 复跑受影响的 40/41/42/43/90/91/92/93（以及 #35 所在 22）。

## Release disposition

Fail。开发者的接受与计划是有效回复，但不是修复证据。先修复第三态 runner 契约和 PRODUCT-RED 语义，完成
九 drill 的真实迁移并关闭 B1/M1-M6/m1，再运行精确远端活体测试并申请下一轮独立外审。

---

# 主进程逐条回复（round-2，2026-07-15）

**总裁决：Fail 完全成立，全部 finding（B1/B2/M1/m1 + round-1 全 OPEN 矩阵）采纳，无驳回。** round-1 我只加了两个 API + 计数器，**既未接线 runner、9 drill 零调用、PRODUCT-RED 无独立态、assert_setup 缺参 fail-open**——地基必要但远不充分，round-1 的 B1/M1-M6/m1 一项没关。reviewer 的核心判断"未接线 API 不等于根因已修"是对的。

**接受 reviewer 的过程指引（疑惑 3）——不再提交分阶段部分修复。** 下一次申请外审前，在**同一变更窗口**内完成：① 统一 verdict 枚举 + runner 契约 + PRODUCT-RED 语义；② 9 drill 逐 locked-cell/prerequisite 真实迁移（43 处裸 NOT-COVERED 文本 + 16+ `err setup…;drill_end` 全清）；③ M1-M6 各臂 fail-closed 硬化；④ SSOT（plan/review/roadmap/README/inventory/ledger）收敛为单一真实状态；⑤ hermetic harness tests（四 verdict + 组合优先级 + 缺参 + runner 解析 + suite 计数/退出码）；⑥ 远端复跑 40/41/42/43/90/91/92/93 + 22。**全部就绪后才申请下一轮外审。**

**逐条采纳 + 定稿枚举决定：**

- **统一四态枚举（回应疑惑 1/2，SSOT contract）**：`GREEN`（0 fail/0 gap，全 locked cell 硬过）· `PRODUCT-RED`（signature-guarded 已知缺陷复现，`assert_bug` 命中签名——**独立计数、独立 verdict，绝不再计 PASS/GREEN**；是否允许 release 由 owner 决定，但机器结果不把"缺陷被复现"伪装成普通通过）· `SETUP-RED`（前置/setup 失败）· `NOT-COVERED`（explore→pin gap，仅限**非**主题脊；任一存在→INCOMPLETE，阻断 landing、独立计数）。`assert_ok` 失败/预期拒绝意外成功/错误签名/APPEARS-FIXED 归入 `ASSERT-FAIL`（与 PRODUCT-RED 分开）。
- **B1（采纳）**：`drill_end` 输出固定结构化 verdict 行（`VERDICT=<enum> pass=.. product_red=.. setup_red=.. assert_fail=.. not_covered=..`）；`run-drills.sh` 按 verdict 分类计数打印（非仅 rc），缺/非法 verdict 才标 infra failure；加单 drill→runner→suite 状态矩阵回归。
- **B2（采纳）**：逐调用点迁移；加静态检查禁目标 drill 裸 `NOT-COVERED` 文本与 `err setup…;drill_end`；迁移后每分支 fail-closed 复跑。
- **M1（采纳）**：`assert_bug` 命中签名→新 `_as_product_red`（`_AS_PRODUCT_RED++`，不计 PASS）；drill_end verdict 反映 PRODUCT-RED。
- **m1（采纳）**：所有 assert API 统一**最小参数校验**——缺 command/签名→harness/setup failure（非成功 setup）；加 Bash+dash 缺参测试。
- **round-1 B1/M1-M6/m1（全采纳，见 round-1 报告回复的精确修复）**：9 drill 迁移时一并落地（92 独立 session + auth/connect 排除 + tier-B >8MiB 断 tier=b；41/43 voter-count + terminal-op + 四重 rollback + 真 live-row；93 HTTP status + jq 白名单 + container-PTY watch；90 fail-open absence 修；#35 降 candidate + MainPID/crash-loop；40 真进 renderer；M6 doc 收敛 + gotcha 编号不漂移——**stall 缺陷从 #37-family 改独立号，plan §4 回写 #42 mapping**）。

**本窗口先落地两个清晰地基缺陷**（m1 缺参 fail-open + M1 PRODUCT-RED 独立态），其余按 reviewer 指引在下一个专注窗口整体完成后再申请外审。**外审进行中：仅改工作树 + 报告回复，不动 index、不 commit。**
