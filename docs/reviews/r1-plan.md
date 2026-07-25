# R1 plan — 仪器地基（定稿）

Date: 2026-07-18 · 总纲：`docs/reviews/allgreen-remediation-roadmap.md` §4-R1 + §3 仪器契约
对抗草拟来源：`docs/reviews/allgreen-roadmap-synthesis.md`（4 视角 + 4 对抗评审 + 综合，主进程定稿）

> **本批铁律：零产品逻辑改动。** 出口硬断言 `git diff --stat internal/ cmd/ scripts/` 为空。

## 1. 工作项

| # | 项 | 文件 | 要点 |
|---|---|---|---|
| A | `poll_until` 栈式可重入（H5） | `lib/log.sh` | 消除模式 A（内层失败→外层冒用内层 desc 立即超时）与模式 B（内层成功→外层 deadline 无限延后=无界挂起）。**帧栈**实现：predicate 仍在当前 shell 执行（保持 `d`/`dexec` 等函数可见、保持既有副作用语义），嵌套调用对称 push/pop 后栈顶仍是本帧 |
| B | per-drill `timeout`（**当前完全缺失**） | `run-drills.sh` | H5 模式 B 的兜底。默认 45min/drill（96/97 实测 ~22/16min），可 `--drill-timeout` 覆盖；超时记为 INFRA-ABORT 且**不可重试** |
| C | rollup 落盘（H15） | `run-drills.sh` | summary 同时写 `$LOGDIR/rollup.tsv`（37 行，机器可 parse）+ `rollup.txt`；`WAIVER-USED <flag>` 行 |
| D | ssh 韧性（H15） | `remote.sh` | `ConnectTimeout` / `ServerAliveInterval` / `ServerAliveCountMax`；本轮实测服务器端 runner 已退出而本地 ssh 不返回 |
| E | `not_covered` 加 `class`（契约扩展） | `lib/assert.sh` | 签名 `not_covered <desc> <reason> <class>`，`class ∈ {gap,runtime-guard}`；缺参数 = SETUP-RED（harness-misuse 一致处理） |
| F | `kept_sites` 计数器 | `lib/assert.sh` + 新 `tests/kept-sites.sh` | = 六类断言调用点总数（`assert_ok`/`assert_refuses`/`assert_setup`/`assert_bug`/`product_red`/`not_covered`）。**对"倒置 assert_ok → product_red"中性，对删臂致命**。R1 冻结初值到 `test/simcluster/expected-kept-sites.tsv` |
| G | INVERTED 配对 lint（防 80/81/82 型假绿复发） | `tests/lint-drills.sh` | 含 `INVERTED` 标记的断言块须同块配对 `product_red|assert_bug` |
| H | lint BATCH 16→37 + jq 字段路径校验（H12） | `tests/lint-drills.sh` | 暴露的违规**就地修正**，不得缩回 BATCH 消红。jq 校验：drill 内嵌 jq 表达式须能 `jq -n` 编译 |
| I | 失败卡片不截断（H13 通用部分） | `lib/assert.sh` | 失败诊断不再 `head -N` 掉尾部；复合 `&&` 断言拆解规范写进 lint |
| J | `--verify-ledger` + N1/N2/N3 | `run-drills.sh` + 新 `test/simcluster/expected-verdicts.tsv` | ledger 是**评审工件**；runner 的 exit code **绝不读它**，始终对任何非 GREEN fail-closed |

## 2. 出口断言

1. 构造用例证明 `poll_until` 模式 A 与模式 B **均已消除**（`tests/verdict-contract-test.sh` 内，dash + sh 双跑）。
2. 契约测试新增覆盖：`class` 参数缺失必红 · `kept_sites` 下降必红 · **倒置 assert_ok 翻成 product_red 后 `kept_sites` 不得误报** · N1/N2/N3 正反两向。
3. `sh tests/lint-drills.sh`（BATCH=37）exit 0；此前未受闸的 21 个 drill 暴露的违规就地修正。
4. rollup 落盘产物存在且可 parse 出 37 行。
5. **`git diff --stat internal/ cmd/ scripts/` 为空。**
6. ledger 初值由**批末两轮全套取交集**建立；两轮不一致的 drill 标 `unstable` 并记录抖动幅度。

## 2.5 实现期的三处订正（实测推翻计划）

1. **`run-drills.sh` 是 bash，不是 POSIX sh**（`DRILLS=()` 数组、`[[ =~ ]]`、`BASH_REMATCH`；pristine HEAD 上
   `dash -n` 即报错）。原计划写的 `dash -n run-drills.sh` 验收条件本就不成立，改为 `bash -n`。
2. **BATCH=37 的硬闸不能要求 `bare-not-covered` 当场清零**——那 7 个 drill 的 24 处 warn 转换**就是 R2 的工作面**，
   在 R1 修掉等于把 R2 并进来、并跳过 R2「每个新增红格必须事先枚举 owner」的要求。
   改为 **PENDING 机制**：带 owner 批号的显式待办行，打印醒目、不计硬失败；且配 **STALE-PENDING 检查**——
   某行对应的违规若已不再触发，该行本身即硬失败（防止 allowlist 腐化成永久 waiver）。R2 出口负责清空这些行。
3. **两条静态规则实现后被撤回**（记录在案，避免后人重蹈）：
   - **jq 可编译性检查**：从 shell 双引号串里提取出的是转义形态（`\"`），导致 40+ 假阳性；
     且它**本来也抓不到 H1**——H1 的 jq 语法完全合法，只是路径匹配不到。一上来就误报的规则只会被下一个人关掉。
   - **gotcha-unpinned**（执行代码引用了 `#NN` 却无 product_red/assert_bug）：静态上**分不清
     「钉住一个活缺陷」与「回归测试一个已修缺陷」**（94 的 `#49-hardening`、32 的 #28/#31、73 的 #29/#30/#32/#33
     都是后者），10 条命中里只有 3 条为真。
   - **这一类的正确执法点是 R2 的 ledger 交叉核对**：`docs/deploy-tier-gotchas.md` 里每一条**未闭合**的 gotcha
     必须在 `expected-verdicts.tsv` 中映射到某个 drill 的**非绿**格。ledger 知道哪条缺陷还活着，静态 grep 不知道。
   - 保留的可靠规则：`empty-needle`（空 grep 针 = 永真 oracle）。H1/H13 那一类**由非恒真证明（roadmap §3 gate C）
     兜底，而非静态规则**——这是设计取舍，不是遗漏。

## 3. 允许的 verdict 变化

本批**允许** verdict 变化（H5/timeout 修复必然把静默挂起转成确定性失败），但**每一处变化必须绑定到
A/B/H 的具体修复点**。「本批不改变任何 verdict」是自相矛盾的写法，已驳回。

## 4. 验证

hermetic：`sh tests/verdict-contract-test.sh` + `sh tests/lint-drills.sh` + `sh -n`/`dash -n` 全 lib（秒级）。
deploy-tier：**全套 ×2**（全工程唯一的双轮建基线）。

---

## 5. 完成记录（2026-07-19）

### 出口断言逐条

| # | 断言 | 结果 |
|---|---|---|
| 1 | poll_until 模式 A/B 均消除，构造用例 + 非恒真证明 | ✅ `tests/poll-reentrancy-test.sh` 7/7（sh + dash）。模式 B 旧实现挂到 20s 兜底被杀、新实现 3s 退出 |
| 2 | 契约测试覆盖 class / kept_sites / N 规则 | ✅ `verdict-contract-test.sh` **41 项 ALL PASS** |
| 3 | lint BATCH=37 exit 0 | ✅ 0 硬违规；7 条 `bare-not-covered` 进 PENDING（owner R2）+ STALE-PENDING 反腐检查 |
| 4 | rollup 落盘可 parse 37 行 | ✅ 两轮 `rollup.tsv`/`rollup.txt` 均落盘 |
| 5 | `git diff internal/ cmd/ scripts/` 为空 | ✅ 零产品逻辑改动 |
| 6 | ledger 初值由两轮取交集，不一致标 unstable | ✅ `test/simcluster/expected-verdicts.tsv`；`kept_sites` 冻结 **1247 站点 / 37 drill** |

### 基线（r1a `-j4` 42min / r1b `-j6` 30min 取交集）
**24 稳定 GREEN · 9 稳定非绿 · 4 不稳定**（41 / 73 / 74 / 96）。N1 无主非绿检查：**0**。

### 计划外但必须做的一项：verdict 契约本不是全覆盖的
`lib/log.sh` 的 `die()` 是 `err + exit 1`，**不经 `drill_end`、不发 verdict 行**。全仓 **38 处**分布在 6 个 drill
（73×20 / 74×8 / 71×5 / 32×2 / 70×2 / 31×1）。实测后果：r1a 中 drill 73 上一行的 `assert_ok` 已记录真实
ASSERT-FAIL，随后 `die` 把整个 drill 变成 **INFRA-ABORT**——runner 眼里唯一表示「测试框架自己坏了」的分类，
**真实发现被贴上了我们的 bug 标签**。而 `assert.sh` 文件头一直声称「a well-formed drill ALWAYS emits a
verdict line」，这句话是失实的，已一并订正。
**修法：让 frame 拥有退出权**（`drill_begin` 后 `die` 记 SETUP-RED 并走 `drill_end`，precedence 不变；
frame 外保持原语义），而不是改 38 个调用点——那是 38 次改变判定的机会。配非恒真证明（frame 内必产 verdict /
frame 外必不产）并入永久回归。

### 归因（R1 规则：每处 verdict 变化必须绑定修复点）
- **74 ASSERT-FAIL → GREEN（r1a）**：pass 38→50，B-dp 与 C-SKEW-precond **真跑并通过**。归因 **H5 模式 A**——
  74 的 `poll_until 240 5` 此前被嵌套 `ss_up` 塌缩成 10s 窗口。**同时坐实 H10：74 的红是 harness 造成的，非 #33/#34。**
  （r1b 又转 ASSERT-FAIL ⇒ 标 unstable，符合 #34 的 per-run 分支依赖特性）
- **73 GREEN → INFRA-ABORT（r1a）→ ASSERT-FAIL（r1b）**：见上「die 契约漏洞」+ #34 不稳定。
- **50 PRODUCT-RED → ASSERT-FAIL（两轮稳定）**：三条 `product_red` 照常复现（product_red=3 不变），新增
  `L1+L2 IDENTITY` 失败。归因 **H5 模式 B** 此前在**偷偷延长超时**——修好后前置等待不再被意外延长，drill 更早往下走，
  撞上尚未收敛的 broker，而 L1+L2 是一次**无重试的即时读**（紧跟已知 crash-loop ~73s 的恢复臂）。
  **不在 R1 修**（drill 50 归 R10），已挂 owner=R10 记入 ledger。

### 实现期撤回的两条静态规则
见 §2.5。要点：jq 可编译性检查 40+ 假阳性且**本就抓不到 H1**；gotcha-unpinned **静态上分不清「钉活缺陷」与
「回归测已修缺陷」**（10 条命中仅 3 真）。该类改由 **R2 的 ledger 交叉核对**执法。
