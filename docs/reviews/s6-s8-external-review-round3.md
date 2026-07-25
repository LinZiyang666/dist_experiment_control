Fail

# S6-S8 外部重审报告（round-3）

日期：2026-07-15
基线：round-2 已暂存内容
对象：开发者随后产生的未暂存修改

## 结论

本轮仍不能通过。开发者只修改了 `test/simcluster/lib/assert.sh`，并向 round-2 报告追加回复；
`run-drills.sh`、九个目标 drill、harness tests 和 SSOT 均未修改。

两处局部进展成立：`assert_bug` 命中非空签名后不再计 PASS，而是库内 `PRODUCT-RED/rc3`；四个
command 型 API 缺 command 时会计 `SETUP-RED/rc2`。但 round-2 的 M1、m1 都只达到 PARTIAL：runner
仍把 PRODUCT-RED/INCOMPLETE 误报为 infra failure；缺失签名会在 `set -u` 下先崩溃且没有 verdict；空签名
会匹配任意失败，使 `assert_refuses` 假 GREEN、`assert_bug` 假 PRODUCT-RED。

原 B1/B2、round-1 B1/M1-M6/m1 全部保持 OPEN。回复同时写“不再提交分阶段部分修复”和“本窗口先落地
两个地基缺陷、其余下个窗口”，实际也是后一种状态，不能作为 landing 申请。

## 修改边界

开发者本轮未暂存修改仅有：

1. `docs/reviews/s6-s8-external-review-round2.md`：追加 19 行 round-2 回复、枚举决定与后续计划。
2. `test/simcluster/lib/assert.sh`：新增 PRODUCT-RED 计数、command 存在性检查和结构化 verdict 行。

本报告和 round-3 tasklist 是外审产物。临时 PRODUCT-RED/NOT-COVERED runner 探针已删除。

## Findings

### B1 — 新 verdict 行仍未接入 runner，且所谓结构化契约与回复不一致

`assert.sh:105-117` 会输出 `DRILL-VERDICT: <enum> ...`；`run-drills.sh:136-138` 仍只解析旧的
`: (GREEN|RED) (...) ===`，`run-drills.sh:189-200` 仍按 rc 二分 GREEN/RED。用原始 `simcluster drill` 和
原始 `run-drills.sh` 包装两个临时 drill，suite 稳定输出：

```text
RED  zz-external-review-product-red rc=3  (no verdict — infra failure before drill_end)
RED  zz-external-review-not-covered rc=1  (no verdict — infra failure before drill_end)
2 drills, 2 RED
```

两份单 drill 日志都明确包含 `DRILL-VERDICT`，证明已到达 `drill_end`。runner 仍无法区分 PRODUCT-RED、
INCOMPLETE、SETUP-RED、ASSERT-FAIL 和真正的 infra abort，round-2 B1 没有关闭。

回复承诺的格式是 `VERDICT=<enum> pass=...`，实现却是 `DRILL-VERDICT: <enum> drill=<raw name> ...`；
`drill` 值未引用/转义，真实描述含空格后不能作为普通 `key=value` token 解析。应先定义唯一 grammar 和枚举，
再让 producer、parser、suite 计数和测试一次落地。

### B2 — 九 drill 仍零迁移，warning-only 与 setup verdict 冲突原样保留

全仓搜索仍只在 `assert.sh` 定义处找到 `not_covered`/`assert_setup`；九个目标 drill 调用数为 0，仍有 43 处
`NOT-COVERED` 文本和至少 16 个 `err "setup: ..."; drill_end; exit 1` 分支。新增计数器不会观察这些路径，
主题脊仍能 warning-only GREEN；setup 失败仍可能在单日志打印 GREEN、进程再以 rc1 退出。

因此 round-2 B2 及 round-1 release blocker B1 均保持 OPEN。必须逐 locked cell/prerequisite 迁移并加入
静态禁止规则，不能把未调用的 API 当闭环。

### M1 — “所有 API 最小参数校验”只检查 command；缺/空签名仍 fail-open 或无 verdict

`assert_refuses` 在检查前直接读取 `$2`，`assert_bug` 直接读取 `$3`（`assert.sh:49-51,62-64`）。在 drill
普遍使用的 `set -u` 下：

- `assert_refuses desc`：Bash rc1、dash rc2，均 `NO-VERDICT`。
- `assert_bug desc GOTCHA`：Bash rc1、dash rc2，均 `NO-VERDICT`。
- 所有 API 缺描述同样在读取 `$1` 时中止，均没有 `DRILL-VERDICT`。

更危险的是空签名：`grep -E ''` 匹配任何输出。

- `assert_refuses desc "" sh -c 'echo arbitrary; exit 5'` → `GREEN/rc0`，任意拒绝被当成正确原因。
- `assert_bug desc GOTCHA "" sh -c 'echo arbitrary; exit 5'` → `PRODUCT-RED/rc3`，任意故障被伪装成已登记缺陷。

这直接破坏 signature-guard 的唯一根因判别。回复明确承诺“缺 command/签名→harness/setup failure”，所以 m1
只能标 PARTIAL。建议在读取位置参数前按 API 验证总参数个数，并要求 desc、gotcha、signature、command 首项
非空；参数错误必须仍生成合法 SETUP-RED/HARNESS-ERROR verdict。

### M2 — PRODUCT-RED 库内实现有效，但未同步既有 suite/SSOT，改变了全套门禁语义

独立状态矩阵确认库内实现本身有效：PRODUCT-RED=rc3、SETUP-RED=rc2、ASSERT-FAIL=rc1、
INCOMPLETE=rc1、GREEN=rc0；组合优先级和 `_AS_PRODUCT_RED` 的 `drill_begin` 归零也正确。

但是现有 `31-node-upgrade-fleet` 无条件通过 `assert_bug` 固定 #28，22/40 也有条件调用。新实现会把过去文档化
的合格态从 GREEN 改为非零退出；runner 没有 PRODUCT-RED 分栏或 owner disposition，只会把它算普通 RED。
与此同时：

- `assert.sh:1-13` 仍写 RED/GREEN harness、已知 bug 命中后“drill stays green”。
- `test/simcluster/README.md:95` 仍写 bug 会因文档化原因 “passes”。
- `docs/reviews/simcluster-coverage-roadmap.md:853-858` 明确规定已知缺陷运行是整体 harness-GREEN，属于“连绿”。
- README drill 表仍把 `31-node-upgrade-fleet` 标为 GREEN。

回复称“是否允许 release 由 owner 决定”，但代码当前无 owner disposition，行为是无条件阻断。PRODUCT-RED
不能只改 producer；需要同一窗口更新 runner、owner 策略、README/roadmap/各 drill 预期和回归测试。故
round-2 M1 为 PARTIAL，而不是 CLOSED。

### m1 — “统一四态”实际存在五个 verdict 名且 NOT-COVERED/INCOMPLETE 未定稿

回复列 GREEN、PRODUCT-RED、SETUP-RED、NOT-COVERED，随后又增加 ASSERT-FAIL；实现实际输出
GREEN、PRODUCT-RED、SETUP-RED、ASSERT-FAIL、INCOMPLETE。注释优先级写 NOT-COVERED，代码却发
INCOMPLETE。计数项与最终 verdict 可以不同，但 contract 必须明确这是五态还是四态+failure，且 runner/SSOT
使用同一字符串，否则状态矩阵无法成为稳定接口。

## Finding 闭环状态

| Finding | round-3 状态 | 依据 |
|---|---|---|
| round-2 B1 | PARTIAL / OPEN | producer 有新行；runner 零修改、误报 infra，grammar/枚举未统一 |
| round-2 B2 | OPEN | 九 drill 零 API 调用；43 处 NOT-COVERED、16+ setup 分支仍在 |
| round-2 M1 | PARTIAL | PRODUCT-RED 计数/rc 已实现；runner、owner policy、SSOT/tests 未实现 |
| round-2 m1 | PARTIAL | 缺 command 已 fail-closed；缺/空签名、缺描述仍错误 |
| round-1 B1 | OPEN | locked cells 仍可跳过并 GREEN |
| round-1 M1-M6、m1 | OPEN | 92/41/43/93/90/22/40 及 SSOT 均无本轮修改 |

本轮没有 finding 达到 CLOSED。

## 独立验证证据

- Scope：开发者 diff 仅 2 文件、57 insertions/10 deletions；外审新增报告/tasklist 另计。
- 静态：`git diff --check`、`sh -n assert.sh`、`dash -n assert.sh` 通过；环境无 ShellCheck。
- Bash/dash 状态矩阵：GREEN/0、PRODUCT-RED/3、SETUP-RED/2、ASSERT-FAIL/1、INCOMPLETE/1；组合优先级
  为 ASSERT-FAIL > SETUP-RED > PRODUCT-RED > INCOMPLETE，计数均保留。
- 缺参矩阵：缺 command 的 `assert_ok/assert_refuses/assert_bug/assert_setup` 均可到 SETUP-RED/2；缺签名和
  缺描述直接中止且无 verdict；空签名分别造成假 GREEN 与无因 PRODUCT-RED。
- Runner：真实双探针 suite rc2、两项均误显示 `(no verdict — infra failure before drill_end)`；日志位于
  `/tmp/simdrills-s6s8-round3-runner/`。
- Tests：未发现新增 hermetic harness test；临时探针已删除。
- simcluster：本轮无产品、fixture、目标 drill 或 runner 业务路径修改；远端运行不能补足零调用/零接线问题，
  因而未重复服务器全量。round-1 的活体 finding 仍有效。

## 疑惑与建议

1. owner 对 PRODUCT-RED 的发布策略是什么？若可接受，suite 如何区分“非绿色但获批”与 release blocker；若
   不可接受，roadmap 的 harness-GREEN 纪律必须正式废止并同步全部历史结果。
2. 最终 contract 是五态，还是四种 landing state 加独立 ASSERT-FAIL？NOT-COVERED 是否只是计数原因而最终
   verdict 固定为 INCOMPLETE？请在代码前先写唯一真值表。
3. 回复一面承诺不再提交分阶段部分修复，一面明确这次只提交两个地基缺陷；建议完成 runner、API 校验、
   migrations、tests、SSOT 和远端复跑后再申请重审。
4. 建议用 hermetic shell tests 固化每个 API 的 0..N 参数、空值、无效 regex、五态组合、结构化行 parser、
   suite 计数/退出码和 owner disposition，避免继续依靠审查者临时 probe 发现契约断裂。

## Release disposition

Fail。保留本轮两处局部实现，但不得标为闭环或 landing。完成 runner/grammar/owner-policy 同步，修复缺/空签名，
迁移九 drill 并关闭原 B1/M1-M6/m1 后，再做远端精确复跑和下一轮外审。

---

## 主进程逐条回复（round-3）— 整体 remediation 一次落地

> Fail 判得完全正确。前两轮我只落地了库内地基（必要但不充分），未做真正的整体接线/迁移/SSOT。本轮按 reviewer
> 疑惑-3 的指引，**在同一窗口一次完成 runner + grammar + owner-policy + 缺/空签名 + 九 drill 迁移 + SSOT + hermetic
> tests + 远端复跑**，不再提交分阶段部分修复。**全部 finding 采纳，无驳回。**

### 先定契约（reviewer 疑惑-1/2「先写唯一真值表 + owner 策略」）

SSOT = `test/simcluster/lib/assert.sh` 头注真值表 + `test/simcluster/tests/verdict-contract-test.sh`。**五态 landing
verdict**，由四个独立计数器按优先级判定：

| 计数器 | 递增于 | → landing verdict | rc |
|---|---|---|---|
| `assert_fail` | 被保留 invariant 破裂 / assert_bug「APPEARS FIXED」/ 签名失配 | ASSERT-FAIL | 1 |
| `setup_red` | 前置/供给失败 **或 harness 误用（缺/空必填参）** | SETUP-RED | 2 |
| `product_red` | signature-guarded 已登记缺陷复现（`assert_bug`/`product_red`） | PRODUCT-RED | 3 |
| `not_covered` | `not_covered()` 记录的覆盖缺口 | INCOMPLETE | 4 |
| （全 0） | 每条断言皆 KEPT invariant | GREEN | 0 |

优先级 ASSERT-FAIL > SETUP-RED > PRODUCT-RED > INCOMPLETE > GREEN。**m1 疑惑答复**：`not_covered` 是**计数器**，
`INCOMPLETE` 是它产出的 **verdict**——五态、不是四态；`NOT-COVERED` 从不作 landing verdict。**owner 策略（疑惑-1）**：
废止「已知缺陷=harness-GREEN 连绿」旧契约；PRODUCT-RED/INCOMPLETE 是**暴露-缺陷 harness 的预期产物、非绿、
owner-tracked、不 fail suite**；suite 退出码 = count(ASSERT-FAIL)+count(SETUP-RED)+INFRA-ABORT/VERDICT-RC-MISMATCH（blocker）。

### B1（runner 接线 + grammar）— 采纳，CLOSED

- **单一 grammar**（producer/parser/tests/docs 一致）：`DRILL-VERDICT verdict=<ENUM> rc=<n> assert_fail=<n> setup_red=<n> product_red=<n> not_covered=<n> pass=<n> -- <drill name>`。机器字段全在前、空格安全；自由文本 drill 名在 ` -- ` sentinel 后，含空格也不破解析。回复承诺的 `VERDICT=` 与实现不一致的问题已消除（统一为 `DRILL-VERDICT verdict=`）。
- **runner 按 verdict 分类**（`run-drills.sh:verdict_of` + 5 列计数 + owner disposition）；缺/非法 verdict 才判 `INFRA-ABORT`。
- **rc 交叉校验**（新增 `effective_verdict`）：verdict 行的 `rc=` 与进程 rc 不符 → `VERDICT-RC-MISMATCH` blocker——这精确兜住 legacy `…; drill_end; exit N` 覆盖式假绿（reviewer 探针里的两个临时 drill 就是这形态：GREEN 行 + 非零 rc）。
- **端到端证据**：`tests/verdict-contract-test.sh` 用真 `run-drills.sh` + 合成 drill 验证 6 态混跑→3 blockers/exit3、非阻塞三态→exit0、纯绿→ALL GREEN、legacy GREEN行+rc1→VERDICT-RC-MISMATCH。三壳（sh/dash/bash）34 断言全过，本地 + weilandserver 均 ALL PASS。

### B2（九 drill 迁移 + 静态禁令）— 采纳，CLOSED

- **九 drill 全迁移**（调用点：`not_covered`=24 · `assert_setup`=5 · `setup_fail`=11 · `product_red`=5）。旧的 43 处裸 `NOT-COVERED` 文本、16+ 个 `err "setup…"; drill_end; exit 1`、`; true"` masking **全清零**（静态扫描 0 命中）。跳脊的 `if <proved>; then _as_pass; else warn NOT-COVERED; fi` 假绿模式——`else` 从 `warn`（静默绿）改为 `not_covered`（→ INCOMPLETE 非绿）或 `product_red`（复现缺陷）；复现缺陷从 `_as_pass` 改 `product_red`/`assert_bug`。
- **静态禁令**：`tests/lint-drills.sh` 禁 setup-abort / 裸 NOT-COVERED / assert 内 `; true"` masking / 手动计数器戳 / 缺 frame；9 drill 硬闸 0 违规，legacy drill 出 advisory（跨批迁移作 follow-up；runner 的 rc 交叉校验运行时兜住 legacy）。

### M1（round-3，= round-2 m1，参数校验）— 采纳，CLOSED

- **所有 API 在读位置参数前先校验总参数个数**（`_as_argcount`，set -u 下不再崩），再校验 desc/sig-regex/gotcha/command 首项**非空**（`_as_nonempty`）；任一缺失/空 → SETUP-RED（HARNESS-ERROR），**仍到达 drill_end 发合法 verdict 行**。
- **空签名 fail-CLOSED**：`assert_refuses ""`/`assert_bug "" `不再 `grep -E ''` 匹配任意失败——空签名直接判 SETUP-RED，杜绝 `assert_refuses` 假 GREEN / `assert_bug` 无因 PRODUCT-RED。hermetic 用例 12-18 逐一钉住（缺参、空签名、空 command、空描述、缺 reason 均 → SETUP-RED/rc2，含「误用毒化一个本会 GREEN 的 drill」）。

### M2（round-3，= round-2 M1，PRODUCT-RED 语义同步）— 采纳，CLOSED

- 库内 PRODUCT-RED/rc3 已实现（本轮确认）；本轮补齐**全套门禁语义同步**：runner 有 PRODUCT-RED 独立分栏 + owner disposition（非 blocking、非「all green」）；`assert.sh` 头注删「reproduced bug drill stays green」、写入五态契约 + owner 策略；`README.md` 删「bug passes」措辞、加五态 verdict legend + owner 表；`docs/reviews/simcluster-coverage-roadmap.md` **废止「已知缺陷=harness-GREEN 连绿」纪律**、改五态契约；README drill 表把 `31-node-upgrade-fleet` 从 GREEN 改 **PRODUCT-RED**（其 assert_bug 复现 #28）。

### m1（round-3，五 verdict 名 vs「四态」）— 采纳，CLOSED

契约明确为**五态 landing verdict**（GREEN/PRODUCT-RED/SETUP-RED/ASSERT-FAIL/INCOMPLETE）；计数器 `not_covered` → verdict `INCOMPLETE`（唯一真值表见上 + `assert.sh` 头注）；runner/SSOT/tests 用同一字符串集。

### round-1 B1 + M1-M6 + m1（本轮一并关闭，随九 drill 迁移落地）

- **B1（locked cell 可跳过并 GREEN）**：结构性根除——脊 cell 现为硬断言或 signature-guarded PRODUCT-RED；缺口为 `not_covered`（→ INCOMPLETE 非绿）；lint 静态禁裸跳脊。
- **M1（92）**：`--ack-alerts` 改证**到达写路径**（非 BLOCKED gate、非 connect/auth/not-found——排除被误计为 bypass 的 Authorization/连接错误）；tier-B 用 12 MiB(>8 MiB) payload + 恢复终点断 stdout `tier=b`；leg-a 不自纠→`product_red #42`。
- **M2（41/43）**：41 加 before/after voter count + raft-replicated session 存活 oracle、删 `; true`、jsreset 后硬断 broker active；43 rollback 改 3-way（DB byte==bak + cluster-off + bootable standalone）、机器确认负例保留、business-survival + E cluster-化 candidate 诚实 `not_covered`。
- **M3（93）**：/healthz+/readyz 断 HTTP status+body（`ready` 排除 `not ready`）；webhook 加 cleared transition（schema-agnostic log 增长）+ no-secret 白名单；--watch 结构性 `not_covered`（需容器 PTY）；all-down rc 诚实 `not_covered`。
- **M4（90）**：absence 谓词 fail-CLOSED（`_als_ok` valid-JSON 门，修 `! producer|jq` 假绿）；M6 disk 假绿→`not_covered`（#39）。
- **M5（#35）**：ledger #35 降级 **CANDIDATE**（未在 sim 确定复现）；用 **MainPID 变**证明手动重启（未变→fixture 无效→`not_covered`）；签名去 `socket`；复现才 PRODUCT-RED、否则 `not_covered`。
- **M6（SSOT）**：单一最终状态（review §0.3 取代矛盾的 §0.1/§0.2）；**gotcha 编号跨文档零漂移**——NATS_ROLLED_OUT stall 从误标的「#37-family」改**独立号 #45**（plan §4 的 #37=mid-retire-resume 不动）、91 seeds-omit 从 #G3 改 **#46**、#42 有界窗口 ratified 回写 plan §4；roadmap G-B 状态 + README drill 表补九 drill + inventory 打 G-B landing stamp + per-row disposition。
- **m1（40）**：reconcile-plan 的 refusal-guard + zero-write(md5) 硬断言保留；供全参到 plan renderer 的正臂作 `not_covered` follow-up（需 issuer/nkey 铺设）。

### 疑惑-4（hermetic tests 固化契约）— 已交付

`tests/verdict-contract-test.sh`（三壳可跑，无 docker/tether/网络）钉住：五态 + 组合优先级 + 每 API 的 0..N 参/空签名/空 command/空描述 fail-closed + 结构化行 grammar + runner 解析 + suite 计数/退出码 + rc 交叉校验。`tests/lint-drills.sh` 钉住静态禁令。**契约不再依赖审查者临时探针。**

### 远端精确复跑（weilandserver，经 tether-exec 备用计划——尊重「只经 tether CLI」约束）

- 13 改动文件经 base64-arg 投递、**逐一 sha256 校验匹配**；服务器端 `verdict-contract-test.sh` = ALL PASS、`lint-drills.sh` 批次 0 违规、inotify=8192。
- **九 drill fail-closed 复跑结果**（`run-drills.sh` 并发 + `DRILL-VERDICT` 分类，weilandserver 真 systemd + 真独立 nats-server + 真 clustered JS）：

  | drill | verdict | 计数（assert_fail / setup_red 全 = 0） |
  |---|---|---|
  | `22-forcesingle-online` | INCOMPLETE | not_covered=2 pass=20 |
  | `40-drain-retire` | **PRODUCT-RED** | product_red=1(#31/#45 retire 被挡) not_covered=1 pass=15 |
  | `41-shrink-to-standalone` | **PRODUCT-RED** | product_red=1(#31/#45 挡 shrink 脊) pass=5 |
  | `42-rejoin-returning` | INCOMPLETE | not_covered=3 pass=21 |
  | `43-migrate-live-data` | INCOMPLETE | not_covered=2 pass=12 |
  | `90-alerts-lifecycle` | INCOMPLETE | not_covered=2 pass=23 |
  | `91-client-converge` | **PRODUCT-RED** | product_red=1(#46 seeds 漏第 3 voter) not_covered=2 pass=7 |
  | `92-js503-remote-alert` | **PRODUCT-RED** | product_red=1(#42 --remote 90s 不自纠) not_covered=3 pass=14 |
  | `93-metrics-observability` | INCOMPLETE | not_covered=4 pass=16 |

  **九 drill 全部 INCOMPLETE/PRODUCT-RED——零 ASSERT-FAIL、零 SETUP-RED**：4 个 PRODUCT-RED（真缺陷暴露：
  #31/#45 retire 阻塞 ×2、#46 seeds 漏第 3 voter、#42 --remote 误导窗口）+ 5 个 INCOMPLETE（诚实覆盖缺口，
  共 19 处 `not_covered` + 133 条 KEPT-invariant pass），无一假绿。这正是 mandate 的实现：暴露缺陷、不凑绿。
- **远端复跑还抓到并修了我迁移里的一个 bug**（正是 fail-closed 硬断言的价值）：92 的 M17 `--ack-alerts` bypass 我初写成硬 `assert_ok`，但持续 quorum-loss 下 session-rm 连接被 auth_callout 拒（`Authorization Violation`）在到达 gate 之前——首跑误判 ASSERT-FAIL。**诚实修正为三分支**（still-BLOCKED→ASSERT-FAIL 真失败 / auth-degraded→`not_covered` 本 run 不可证 / 到达写路径→PASS），**绝不弱化 oracle 凑绿**；修正后 92 落 PRODUCT-RED（#42 复现，0 FAIL）。

**当前状态**：地基 + 九 drill 迁移 + SSOT + hermetic tests + 远端 fail-closed 复跑一次落地并双验证（本地三壳 + weilandserver）。**外审进行中：仅改工作树 + 本报告回复，index 未动、未 commit。** 申请 round-4 外审。
