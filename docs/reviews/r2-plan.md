# R2 plan — 记账诚实化（定稿）

Date: 2026-07-19 · 总纲：`docs/reviews/allgreen-remediation-roadmap.md` §4-R2 · 前置：R1 已收口
基线：`test/simcluster/expected-verdicts.tsv`（24 稳定 GREEN / 9 稳定非绿 / 4 不稳定）

> **本批是全工程唯一被允许扩大红区的批。** 代价是：**扩大的每一格必须在实现前逐格枚举**，
> 带 owner + 归属批号；**未被预先枚举的新非绿 = 本批失败**。
> 这是可以事前枚举的——H4 是**纯记账转换**，不解锁任何被短路的执行路径。

## 1. 工作项

| # | 项 | 面 | 说明 |
|---|---|---|---|
| A | 24 处 `warn "…NOT-COVERED"` → `not_covered(<desc>,<reason>,<class>)` | 30×3 · 31×1 · 62×1 · 71×4 · 73×3 · 74×10 · 82×2 | 实测分布已核。**desc/reason 一字不改**，只把记录方式从旁路 `warn` 换成计数的 `not_covered` |
| B | 80/81/82 三处裸倒置 `assert_ok` 配 `product_red` | #25 / #26 / #27 | **翻法是重写谓词，不是换关键字**——见 §3 |
| C | `62:118` 描述串正名 + 另加 gap 类 `not_covered` 登记 OQ-2 | 62 | 现状：谓词是活体测量 `_statfs_healthy`（**不是**裸 `assert_ok` 假绿），但描述串写成 "NOT-COVERED" 却计 pass ⇒ 名实不符 |
| D | **ledger 交叉核对**（从 R1 移交，见 r1-plan §2.5） | 新 `tests/ledger-crosscheck.sh` | `docs/deploy-tier-gotchas.md` 里每条**未闭合**的 gotcha 必须在 `expected-verdicts.tsv` 映射到某个 drill 的**非绿**格。这是「钉活缺陷 vs 回归测已修缺陷」的正确执法点——**ledger 知道哪条缺陷还活着，静态 grep 不知道** |
| E | 清空 `tests/lint-drills.sh` 的 7 条 PENDING 行 | lint | A 做完后 `bare-not-covered` 不再触发；**STALE-PENDING 检查会强制要求删除这些行**（R1 已埋） |

## 2. 扩红预先枚举（**这是本批的验收基准**）

依 `lib/assert.sh` 的 precedence：`ASSERT-FAIL > SETUP-RED > PRODUCT-RED > INCOMPLETE > GREEN`。

| drill | 基线 verdict | 本批动作 | **预测 verdict** | 变化? | owner | 归属批 |
|---|---|---|---|---|---|---|
| 30-rolling-upgrade | ASSERT-FAIL | 3 warn → not_covered | ASSERT-FAIL | 否（precedence 吸收） | P3+H1 | R4/R7 |
| 31-node-upgrade-fleet | PRODUCT-RED | 1 warn → not_covered | PRODUCT-RED | 否（precedence 吸收） | #28 | R9 |
| 71-expose-rehome-failover | ASSERT-FAIL | 4 warn → not_covered | ASSERT-FAIL | 否 | P1/#29 | R8 |
| 73-proxy-cluster-ha | UNSTABLE | 3 warn → not_covered | UNSTABLE（不变） | 否 | #34 | R6/R8 |
| **62-remote-fs-safe** | GREEN | 1 warn → not_covered + C 项 gap | **INCOMPLETE** | **是** | OQ-2 | **R15** |
| **74-rebalance-on-return** | UNSTABLE(GREEN/AF) | 10 warn → not_covered | **INCOMPLETE**（其 GREEN 分支变 INCOMPLETE） | **是** | #34 | **R6/R8** |
| **80-session-isolation** | GREEN | B: #25 谓词重写 + product_red | **PRODUCT-RED** | **是** | #25 | **R12** |
| **81-admin-evict-session-rm** | GREEN | B: #26 谓词重写 + product_red | **PRODUCT-RED** | **是** | #26 | **R12** |
| **82-agent-onboarding-invite** | GREEN | 2 warn → not_covered + B: #27 | **PRODUCT-RED**（product_red 压过 INCOMPLETE） | **是** | #27 | **R12** |

**预测净效应：稳定 GREEN 24 → 19**（62/80/81/82 确定性转非绿；74 的 GREEN 分支转 INCOMPLETE）。
**任何不在此表中的 verdict 变化 = 本批失败**，必须查明原因而非事后补录。

## 3. B 项的翻法（关键，别做成换关键字）

`assert_bug` 修好后契约会自动判 APPEARS-FIXED，但**倒置 `assert_ok` 不会**——它在产品修好后
可能因不相关原因保持为真而**静默继续绿**。所以三处必须**重写谓词**，让它断言「产品应有的正确行为」：

| drill | gotcha | 现状（倒置：缺陷存在 = 断言成功） | 目标谓词（产品修好后才为真） |
|---|---|---|---|
| 80 | #25 | 第 11 次同源 correct-PIN 首连**仍成功** = 无限速 | `assert_refuses` + per-IP rate-limit 签名 |
| 81 | #26 | evict 后 managed 子进程**仍活着** = 未清理 | 子进程必须已从**宿主进程表**消失 |
| 82 | #27 | well-known manifest listener **未 bound** | listener 必须已 bound |
| — | — | 本批**只加 `product_red` 记账让缺陷进入判定**；谓词重写**待 R12 产品修复时同批完成** | 见 R12 出口④ |

> **本批范围锁**：B 项只做「让三条已登记缺陷从 GREEN 变成 PRODUCT-RED」这一记账动作，
> **不重写谓词、不改产品**。谓词重写与产品修复是 R12 的事，两者必须同批（否则留一个自制的红）。
> 这与 §2 的预测一致：80/81/82 → PRODUCT-RED。

## 4. 出口断言

1. `grep -cE '^[[:space:]]*(warn|log)[[:space:]].*NOT-COVERED'`（剥注释后）== **0**。
2. `sh tests/lint-drills.sh` exit 0，且 **PENDING 行已全部删除**（STALE-PENDING 检查通过）。
3. 80/81/82 **必须落 PRODUCT-RED**——落不了就是 B 项没做对。
4. **实际 verdict 变化集合 ⊆ §2 预测表**；差集非空 = 本批失败。
5. `kept_sites` 逐 drill 不下降（`warn`→`not_covered` 是**新增**站点，只升不降；倒置翻转对它中性）。
6. `tests/ledger-crosscheck.sh` exit 0。
7. `git diff --stat internal/ cmd/ scripts/` 为空（本批仍是零产品改动）。

## 5. 验证

hermetic：`verdict-contract-test.sh` + `lint-drills.sh` + `kept-sites.sh --check` + `ledger-crosscheck.sh`。
deploy-tier：**全套 ×1**（记账语义是套件级，必须整体重算 ledger）。

---

## 6. 完成记录（2026-07-19）

### 出口断言逐条

| # | 断言 | 结果 |
|---|---|---|
| 1 | 剥注释后 `warn.*NOT-COVERED` == 0 | ✅ 24 处转成 **27 个 `not_covered` 调用**（3 处拆分；13 gap / 14 runtime-guard） |
| 2 | lint exit 0 且 PENDING 行清空 | ✅ STALE-PENDING 检查按设计逼我删掉 7 行 |
| 3 | 80/81/82 必须落 PRODUCT-RED | ✅ 三者实跑均为 PRODUCT-RED |
| 4 | 实际变化 ⊆ 预测表 | ⚠ **部分**——见下「预测偏差」 |
| 5 | `kept_sites` 不下降 | ✅ **1247 → 1274（+27）**，只增不减 |
| 6 | ledger 交叉核对 | ✅ 新增 `tests/ledger-crosscheck.sh`；无主数 **9 → 6** |
| 7 | 产品代码零改动 | ✅ |

### 实跑结果（r2，`-j5`，2305s）
`GREEN=20 · PRODUCT-RED=7 · INCOMPLETE=4 · SETUP-RED=1 · ASSERT-FAIL=5 · INFRA-ABORT=0`

### 预测偏差（本批最重要的产出）
**5 处预测全部命中**：62→INCOMPLETE · 74→INCOMPLETE · 80/81/82→PRODUCT-RED。
但差集非空，逐条裁定：

| drill | 变化 | 裁定 |
|---|---|---|
| 41 / 96 | UNSTABLE → ASSERT-FAIL | **落在已记录的抖动带内**（r1a/r1b 已观测过该值），非新状态 |
| 73 | UNSTABLE → INCOMPLETE | **新状态，但可归因 R2-A**：3 处 not_covered 落地后，断言全过时自然落 INCOMPLETE。**我的预测表在这里写错了**——对已有非绿的 drill 只写「precedence 吸收、无变化」，忽略了它们也可能在某轮**断言全过**从而露出 INCOMPLETE |
| **50** | ASSERT-FAIL → PRODUCT-RED | **不是修好了，是 flake**。`pass=67` 与失败轮**完全相同**（同样多断言跑了）、`assert_fail` 1→0。即 R1 诊断的那条「紧跟 crash-loop 恢复臂的无重试即时读」在时序上偶尔赶上。**推翻 R1 基线把 50 记为「两轮稳定 ASSERT-FAIL」**——两轮不够，第三个数据点才暴露。已改记 UNSTABLE，owner 保持 R10 |

**方法论教训**：`UNSTABLE` 的预测不能只写「保持不变」，必须写成
「解析到 {已观测值} ∪ {本批新增记账可能引入的态}」。已据此修订总纲 §5 的中间态验收写法。

### 遗留的 6 条无主缺陷（全部有归属批，非遗漏）
`#36`（online force-single `--yes` 不走 Tier-2）· `#39`（disk_pressure 间隔无 knob）· `#42`（quorum-loss 后误报窗口）
→ **R6 定案**；`#47`（CATCHING_UP 永久滞留）· `#48`（agent 黏退役 broker 孤岛）· `#49`（resnapshot preflight 不一致）
→ **R7/R9**。它们当前没有非绿格钉住，`ledger-crosscheck` 会持续报红直到对应批次补上 owner cell。

### 顺带补的一件事
`tests/lint-install.sh`（R3 产出）**没有被任何 runner 调用**——一个没人跑的闸等于不存在，与它要防的失败模式同型。
新增 `tests/run-all.sh` 统一 6 道 hermetic 闸（poll-reentrancy / verdict-contract / lint-drills / lint-install /
ledger-crosscheck / kept-sites --check），作为每批收口与每次 deploy-tier 跑之前的固定入口。

---

## 7. R3 完成记录（install.sh heredoc / P9）

**核心发现**：11 处 heredoc **只有 1 处**（`:592` 纯字面的 broker banner）能合法改成 `<<'EOF'`；
其余 10 处**真的需要参数展开**（unit 的 `ExecStart=$bin/…`、broker.yaml 的 `$DOMAIN/$LIB_DIR`、
nats.conf 的 `store_dir=$lib` 等）。**所以 P9 只能靠转义修，不能靠加引号**——这一点与直觉相反，
是本批最重要的判定。全文审计后，整个文件里唯一**非有意**的 shell-active 构造就是 P9 那对反引号。

**逐字节渲染对照**（本批最重要的验收）：改动前后所有 heredoc 的渲染输出 diff **只有一行**——
```
-# G4 §B / #23: Restart=always (not on-failure). The  grow cutover restarts nats-server with a
+# G4 §B / #23: Restart=always (not on-failure). The 'cluster add' grow cutover restarts nats-server with a
```
`-` 行就是修复前的现实：`cluster add` 两个词一直在被静默删除。

**真实部署路径验证**：R3 全套（含 `--build` 重建镜像，镜像内 install.sh 指纹 `e6f036eec2ee` == 仓库指纹）
`cluster: not found` 出现次数 **0**；对照修复前的 R2 全套是 **6 个 drill**。

**新增永久门** `tests/lint-install.sh`：扫描未加引号 heredoc 正文中的反引号 / `$(` （root 运行的安装脚本里
命令替换即注入面）。配双向非恒真证明：对修复前的原文件必红、对故意塞回反引号的变体必红、恢复后必绿。

**R3 全套结果**：`GREEN=19 · PRODUCT-RED=6 · INCOMPLETE=3 · SETUP-RED=2 · ASSERT-FAIL=7`（2358s）。
与 R2 的 3 处差异（40 / 50 / 74）**全部落在已记录的抖动带内**，无一可归因于 install.sh 改动
——符合 R3 计划的 `deferred-attribution` 纪律。

## 8. R4 完成记录（升级面 oracle / H1+H3+P8）

**非恒真证明（决定性）**：用从两个真实文件里**逐字提取**的谓词 + 真实产品 JSON 做的对照表——

| 场景 | 修复后 `_all_on_next` / `_dryrun_no_touch` | **修复前（HEAD）** |
|---|---|---|
| 三台全在 NEXTVER（滚动完成） | PASS / FAIL | **FAIL / PASS** |
| 三台全在 v-cur（什么都没滚） | FAIL / PASS | **FAIL / PASS** |
| 半滚（brk2 落后） | FAIL / FAIL | **FAIL / PASS** |
| 旧 PascalCase schema 漂移 | FAIL / FAIL | **FAIL / PASS** |
| 空输出（ctl 挂了） | FAIL / FAIL | **FAIL / PASS** |

修复前那一列是铁证：`_all_on_next` **在全部 5 个场景都 FAIL**（含滚动完全成功那个）、
`_dryrun_no_touch` **在全部 5 个场景都 PASS**（含三台全在新版那个）——正是 H1 描述的
「永不可过 / 永久空绿」这一对。修复后两者随状态双向翻转。

**P8 破坏面已核**：`node ls --brokers --json` 的机器消费方**全仓只有 drill 30** 一个；
drill 32 的 `.BrokerVer` 只出现在散文注释里，它实际读的是 `cluster status --json`（不同且未改的 schema）。
`schema_version` 1 → 2（键名变更是破坏性的）。

**顺带强化**：`_dryrun_no_touch` 加了非空守卫——读不出版本现在**失败**而不是静默满足「不在 NEXTVER」。
没有这个守卫，任何未来的 schema 漂移都会直接退回永久空绿。这是加强，不是放宽。

---

## 9. R5 完成记录（混沌/HA/观测面 oracle）

### 实跑（6 drill 定向）
`40=GREEN · 42=SETUP-RED · 71/74/93/96=ASSERT-FAIL`

### H8 已确证修好，且**打开了一段从未跑过的覆盖面**
42 的失败点从 `I create real post-force-single transfer-audit Raft entries`（fixture 第 27 条）
推进到 `F cluster add resumes the pre-approved returning-node op`（第 37 条），**pass 27 → 37**。
即：补 `--ack-alerts` 后 rejoin/resnapshot 整段**首次真正执行**，新暴露的是更深处的 `cluster add`
恢复不了 pre-approved 返回节点 op 的问题。
**新红挂 owner**：`#47`（`cluster add` 把 joiner 永久留在 CATCHING_UP）族 → **R9**。
这同时给 `ledger-crosscheck` 的 `#47` 补上了 owner cell。

### H8 的处置模板（全工程「设计如此」项的范式）
`gateDestructive` 拦 push 是**设计**，drill 漏传 flag 才是 bug。修法是
**补 flag + 另加一条 `assert_refuses` 带签名正面钉住「不带 flag 时该门必须拦」**
——把设计意图变成 KEPT invariant，而不是绕过它。`kept_sites` 因此不降反升。

### 本批最重要的方法论发现：三道闸查不出「死函数」
前一个 agent 在 API 超时前**完整定义了 `_b_migrate_forensics` 却从未接线**。
`sh -n` / `dash -n` / `lint-drills` / `kept_sites` **四道闸全过**——因为它们都检查不出
「函数定义了没人调」；`kept_sites` 甚至因新增站点而**上升**，给出改进的假象。
若非补完 agent 主动发现，H9 会以「已完成」的面目留在树里。
**已据此把死函数扫描加入每批收口检查**：
`for f in $(grep -oE "^_[a-z0-9_]+\(\)" <drill> | tr -d '()'); do [ $(grep -c "\b$f\b" <drill>) -le 1 ] && echo dead; done`
同扫发现 drill 93 的 `_leader_metric` 亦为死函数，但它**在 HEAD 里就存在**（本批 diff 未触及）
⇒ 既有 harness 债，记 **R14**，不算本批回归。

### 非恒真证明（三项双向对照，最直白的一条）
H11(b) 同样输入下：**旧选法选中 0-home 的 brk3**（于是 `_ktgt_empty` 的 "rehome away" 判据
从一开始就被真空满足）、**新选法拒绝出选**。
另记：补完 agent 的 H11(a) 验证 stub **第一次跑出假 PASS**（`$(_dist)` 在子 shell 里跑、
漂移计数器不持久），它自行发现并声明「这是我的 harness bug、不是 drill 的」后改用时间戳重跑
——这正是非恒真证明该有的态度。

---

## 10. R7a 完成记录（周期对账注册表）· R7b 拆出

### 拆批理由（技术性，非规模）
- **R7a**：注册表接口 + 重写现有 4 处（行为等价，假时钟证明）+ **#58/P10** + **#31**（release-failure 形态）+ CLI rc 语义。
- **R7b（未开工）**：P3 升级锁 lease+TTL · #45（`topoConvergedForOp` 无界门）· #47（CATCHING_UP）· #49（仅 drill 42 复验）。
- **切点**：#58/#31 是**识别式收敛**（读产品已写的状态、调已存在的命令路径 `reconcileXferObjects`/`PlanClearGrowActive`）；
  P3/#45/#47 是**截止式收敛**——需要 lease + TTL + 续约持有者这一**全新机制**。
  在接口尚未证明时于同一批发明新机制，正会撞上「禁止新建策略」的一票否决不变量。**R7b 现在起步于一个已冻结、已证明的注册表。**

### 接口
`(name, interval, leaderOnly, lastTick, fn)`，`internal/broker/reconcile_registry.go`：
- **锚定 deadline**（`nextDue += interval`）而非按 wall-clock 重采样 —— 这是精确等价可证明的前提。
- **`leaderOnly` 由注册表统一门控**、每轮 sweep 求值一次；被门掉的 pass **推进日程但不推进 `lastTick`**
  ⇒ 晋升后的 follower 不会补跑积压，R13 的状态面也不会谎报某 pass 跑过。
- 错误 → 指数退避（上限 5 min）；四个被重写的 pass **恒返回 nil**（内部自行 log，同改前）——
  否则退避会改变节奏、破坏等价。
- **零新增 goroutine、零自有 timer**：两个 ticker 收敛为一个，长期存活对象数**净减少**。

### 等价证明（出口断言 2 的核心）
- `TestReconcileRegistryMatchesLegacyTickers`：30 分钟 1800 次模拟 tick，计数与**独立推导**的闭式
  `floor(T/P)` 相符（1800/1800/1800/6）——oracle 来自 ticker 契约，**不是**从注册表读回。
- `TestReconcileRegistryFiresOnTheSameInstants`：把计数强化到**时刻**——第 i 次必须精确落在 `t0 + i·P`。
  **按 `now` 重采样的实现能过计数检查，但过不了这条。**
- `TestReconcilePassEffectsMatchLegacy`：把 R7 前的循环体**逐字复制**当 oracle，并行 fixture 跑 20 tick、
  每 tick 比对 node/port/process 三张表，含反空跑守卫。
- 门：`make test` ✅ · `make e2e` ✅(541s) · `make lint` ✅ · `-race` ✅ · **泄漏门** ✅。

### 主进程补的一处加固（R-a 风险的机械化）
agent 正确指出：#58 从一次性删除变成**循环**后，其安全性依赖 `transfer.go` 里一条
「tracker 登记 **先于** prepare 投递给 agent」的**顺序约定，而这条约定没有任何机制强制**。
我实测确认顺序成立（`put()` 在 :586、forward 在 :612），但**约定不等于保障**——
一旦有人把 forward 挪到 put 之前、或新增「先写对象后登记」的路径，周期 reaper 就变成**数据丢失循环**。
⇒ 已把约定改成**强制前置条件**：forward 之前断言 tracker 已有该 entry，否则拒绝投递并报
`internal_error`（带 R7a ordering guard 说明）。这正是总纲风险 **R-a** 的机械化兜底。

### agent 主动披露的三条残留（均已接受，不掩盖）
1. **#31 只修好了一半**：marker 已置但 operation 行尚不存在的 **acquire-lock 窗口**（`cluster add` HALT 在
   P2/P3）仍会永久泄漏。agent 判断「在此处发明超时」正是不变量禁止的新策略 ⇒ 留给 R7b 的 lease+TTL。
   **「#31 已修」只对 release-failure 形态为真**，roadmap 命名的正是这一形态。
2. **rc 语义是行为破坏性变更**：`cluster add` 在 4 次尝试后仍未确认释放锁时**退 69**。
   把 `cluster add` 当 fire-and-forget 的自动化会开始看到「grow 成功但命令失败」。
   判断为正确（此刻成员控制面确实被 fence 住），但需在 R15 文档批显式说明。
   **连带**：drill `30:158`/`30:165` 两条倒置 `assert_ok`（谓词 `_roll_halted_on_growlock`）**现在会变假**，
   须同批翻正——由主进程执行。
3. **granularity 耦合**：驱动 ticker 取 `min(interval)`；未来若注册一个亚秒 pass，全体 pass 的**求值频率**
   都会随之上升（开销小，但唤醒率成了共享全局量）。且精确时刻等价只在「pass 间隔整除 granularity」时成立
   ——当前默认值全部满足，任意配置值不保证。

## 11. R7b 完成记录（lease + TTL 截止式收敛）

### 半成品盘点：一处「编译过、测试过、功能是死的」，且**比原缺陷更危险**
前一个 agent 因 API 中断死在「Now the CLI lease keeper:」。它留下的状态**自洽但不完整**——
broker/store 侧四项都已完成（lease 存储层、reaper、`topoAdvance` 有界推进、CATCHING_UP 拆分），
`renew-lock` 的 broker handler 与 `PlanRenew{Grow,Upgrade}Lease` 也都在，
**但没有任何客户端会发 renew**。

后果不是「少做了一块」，而是**把阻塞型缺陷变成了正确性缺陷**：
acquire 盖一个 15 分钟 lease、无人续约 ⇒ reaper 会把锁从**正在运行的操作**手里收走。
而 `cluster add` 合法运行可达 `opCatchupMaxWindow` = **30 分钟 = 两倍 TTL**
⇒ **每一次慢 grow 都会在中途丢掉自己的互斥锁**，两个成员变更可能并发跑。
**四道闸（build / test / lint / 无死函数）全部会放行它。**
同类第二处：`tether cluster unlock` 在三处注释里被称作「运维的出路」却根本不存在，
`ClusterHealthResp.{Upgrade,Grow}LeaseExpiry`（schema v5）是**有生产者、无消费者**的字段。

> **这是本工程第二次遇到「半接线」**（第一次是 R5 的 `_b_migrate_forensics` 定义了没人调）。
> 两次都过了全部静态闸。**结论：静态闸查不出「接线」，只有端到端断言能**——
> 故 R7b 补的 `TestDriveUpgradeActuallyStartsAKeeper` 是**变异验证**过的（把 keeper 间隔改成 100h 必红）。

### TTL 推导（写进 `lock_lease.go`，并由 `TestLeaseTTLDerivationHolds` 钉死算式）
TTL 界定的是「**健康持有者最长可以多久没能落地一次续约**」，不是操作耗时：
`(a) 无可写 leader 窗口 3min（= upgradeConvergeTimeout）+ (b) broker 间时钟偏移 5min
（= upgradeTriggerSkew，过期时间由一个 leader 盖、另一个 leader 读）+ (c) 容一次漏续约 TTL/3`
⇒ `TTL ≥ 8min / (2/3) = 12min` → 取 **15 min**（~3 min 余量）。

### 出口断言 2/3 的实证
- **lease 双向**：`TestLeaseHolderIsNeverRobbed` 以生产节奏续约、reaper 每 30s 触发，跑满 **4×TTL**
  模拟时间（每把锁 >200 次 reap 判定）**零过期**；`TestLeaseExpiresOnceTheHolderStops` 证明
  TTL−1s 不动、TTL+1ns 必收。另加：过期从**最后一次续约**起算而非 acquire；续约**结构上无法创建**锁；
  grow 续约绑定 joiner；无 lease 的锁永不过期。
- **#45 没修坏正确设计**：`TestTopoAdvanceN1BoundaryStaysNonterminal` 把 N→1 边界带着**已过期的 deadline**
  跑到 **30 天**，每 tick 断言 `NATS_ROLLED_OUT && !terminal && !BLOCKED` 且错误串点名 `--to-standalone`。
  **变异验证**：把 carve-out 关掉（`if false && …`）该测试立刻失败并打印
  「the two-phase boundary is BY DESIGN, not a stall」。

### 主进程补的一件事
`cluster unlock` 缺 `simcluster-coverage-inventory.md §2` 的清单行（`docs/` 在该 agent 权限外，
而 `make test` 的命令树闸以该清单为准）。已补，drill 覆盖 owner 记 **R9**。

### 三条残留（已接受，不掩盖）
1. **keeper 是全批单点**：其它项失败都安全降级，而 keeper 停止续约会把阻塞型 bug 变成正确性 bug。
   `TestDriveUpgradeActuallyStartsAKeeper` 是守卫且经变异验证，**但只覆盖 `driveUpgrade`；
   `driveAdd` 的 keeper 没有等价的端到端接线测试**（它为 P2/P3 自我 shell-out，难以 hermetic 驱动）。**本批最弱处。**
2. `clusterLockNotHeldCode` 是**跨包重复的 wire 字面量**（`cmd/tether` 与 `internal/broker` 各一份、无编译期链接）；
   任一侧改名会**静默**把终态降级成「瞬态」。`TestLockNotHeldCodeIsWireStable` 钉住，但那是字面匹配、不是类型链接。
3. `cluster unlock --grow` 发空 `JoinerNode`（无条件清除）——health 回包不带 marker 的 joiner id。
   有意为之且已文档化（按猜测的 joiner 谓词化会产出「静默 no-op 却报 OK」），靠「拒绝活 lease + 清除后再探确认」兜底。

### 门
`make test`（63 包全过 0 失败）· `make lint` 0 issues · `-race`（broker/cluster/cmd）· **泄漏门** ·
`make e2e` 523s 全矩阵 PASS。途中修掉一个真实竞态（keeper 先 latch `lost` 后写日志 ⇒ 改成先写后 latch）。

## 12. R8 完成记录（不依赖对端事件的主动投递）

### 主进程的一个验证错误（必须记下来）
接手 R8 中间态时我跑了自制的「死函数扫描」并报告**无死函数**——**这是错的**。
续做 agent 找出 `internal/agent/home_push.go:101` 的 `forgetHomeAck` **零调用者**
（后果：`rehomeAckTo` 会在 agent 生命周期内每个端口泄漏一条，不是无害的）。

**我的扫描为什么漏**：它用 `grep -rho "\bfn\b" | wc -l ≤ 1` 判定，
**把注释里的提及也算进了引用数** ⇒ 一个「定义 + 被注释提到一次」的死函数计数为 2，逃过判定。

**更要紧的是：`make lint` 原生就能抓这一类**，而我在中间态自查时只跑了 `go build` 和 `go test`、**没跑 lint**：
```
internal/agent/home_push.go:101:17: func (*Agent).forgetHomeAck is unused (unused)
```
⇒ **纠正流程**：此后每次接手中间态，自查必须是 `go build` + `go vet` + **`make lint`** + `go test`，
不要用自制 grep 替代已有的、更准的工具。自制扫描只作为对**跨包接线**（lint 看不到的那类，如 R7b 的
「broker handler 存在但无客户端调用」）的补充。

### 不变量的结构性保证（本批核心）
`TestHomeDeliveryFollowsADrainWithASilentAgent`：fake agent **构造上就是只听不说**（不暴露任何
register/heartbeat 方法），再叠加 `peerSilenceMonitor` 订阅三个 agent-pub 生命周期 subject，
**断言其计数在 drain 前后不变**。
⇒ 把主张从「数据面跟上了」强化为「**数据面在对端一声不吭的前提下跟上了**」。
没有这一层，一次偶发重连会让测试在「修没修」两种情况下都通过——而 P1 的本质恰恰就是「投递依赖重连」。

### 变异验证（6 次，全部实测）
| 变异 | 结果 |
|---|---|
| **A2 精确复现 R8 前的缺陷**（`homeAssignmentApplied` 忽略 epoch ⇒ 已收敛端口永不重投，投递重新被对端事件门控） | `INVARIANT VIOLATED: agent's newest home = (brk-a:7000, epoch 1), want (brk-b:7000, epoch 2)` |
| A 投递通道禁用 | 4 测试红，含 `re-delivered only 0 time(s)` |
| **B pass 未注册**（R5/R7b 的失败模式） | `the home-delivery pass is NOT registered — the active delivery channel would be dead code` |
| C 撤回 #33 修复 | `#33: a SUCCESSFUL rehome proved a live tunnel but left ProxyBound false` |
| E rc 门失效 | `drain returned success while the data plane was still on the OLD home — this is the release blocker` |
| F wire 字面量改名 | 新增的 cmd/tether pin 触发 |

**变异 B 是专门针对本工程两次「半接线」教训写的**——它断言 pass **确实注册在注册表里**，
而不只是「函数存在」。

### 第二个被找出的半接线
`TestDataplaneNotConvergedCodeIsWireStable` **根本不存在**，但
`home_convergence.go:53` 与 `error_hints.go:105` **两处注释都点名它**作为
`dataplane_not_converged` 跨包字面量的守卫。改名会**静默**把未收敛的 drain 从 exit 75 降级成 70。已补写并变异验证。

### 两条风险（已接受）
1. **`TestRetireConsultsTheConvergenceGate` 是源码字符串 pin，不是行为测试**。retire 的门在
   `driveRetire` 的 `OpStateRehomeExposes` 臂里，走真实 FSM 需要 raft 复制的 `port_allocations` 行，
   而没有 plan 命令能种下它。⇒ retire 的**判定逻辑**有行为测试，但**调用点**只被 `strings.Contains` 钉住，
   一次把检查挪到状态转移之后的重构能逃过它。drain 与 upgrade 无此缺口。
   **诚实的验证者是 deploy-tier 的 drill 40**；若要变成真 FSM 测试，需要一个 test-only 的 `PlanPortAllocationSet`。
2. `homeDeliveryState` 有意是 **leader-local 且不复制**的 ⇒ drain 中途换主会清空 applied 集、新 leader 全量重投一次。
   这是设计且自愈，但意味着**与选举竞争的 drain 可能对一个其实已收敛的数据面报 `dataplane_not_converged`**
   ——**方向安全**（永不出现假 rc=0），但运维会看到。

### 门
`make test`（63 包 0 失败）· `-race`（broker/agent/cluster/cmd）· **泄漏门** · `make lint` **0 issues** ·
`make e2e` 512s 全绿。

## 13. R9 产品侧完成记录（P3 / #28 / #47）

### P3：把「一个字段干两件事」拆成显式三态
根因是 `AgentVer string` 同时表达「版本」与「有没有 agent」，于是 `""` 既是「不知道」也是「没有」。
新增 `AgentPresence`（`AgentUnknown`/`AgentPresent`/`AgentAbsent`）+ `Step.BrokerOnly`，
让执行器消费**plan 时**的决定，而不是在 roll 中途重新推导。
- (a) present+到版 → done；(b) present+未到版（**含「已声明但不在 node list」**）→ not done；
  (c) absent → **broker 守护进程本身就是整台主机**。
- **零值是 `AgentUnknown` 且 fail-closed**：忘记分类的调用方拿到的是 P3 前的行为，**永不会**误走 broker-only 捷径。
  11 条既有 planner 测试原样通过。
- `resolveColocatedAgent` **有意不对称**：**已声明**的 agent（`--colocated-agent-nid`）即使停机也算 present
  （⇒ roll 大声 HALT 而非静默跳过）；**未声明**的只有在 node list 里**被观测到**才算 present。
  `handleNodeListReq` 返回一切注册过的节点（ONLINE|STALE|OFFLINE），故仅停机的 agent 仍落 (b)。
  只有「从未注册过」才到 (c)。

### #28：选了 (a) 真加 flag，理由是选 (b) 会文档化一条死路
`internal/agent/Config.UpgradeURLAllowlist` **本就存在且本就被消费**（`upgrade.go:76`），
**缺的只是 CLI 表面**。若选 (b)（改文档指向 broker 侧），自托管制品的运维打开 broker 白名单后
**仍会被每个 agent 以 `url_not_allowed_local` 拒绝，且无处可改**——那是真实运维缺口，不是文档 bug。
除 flag 外还加了 `agent.yaml` 的 `upgrade.url_allow`，走 serve 同款 **flag > yaml > 内建默认**优先级：
`install.sh` 管理的 agent 由 systemd unit 启动，运维不会去改 `ExecStart`，**只给 flag 等于几乎不可用**。
两者皆空 ⇒ 保持 `defaultAgentURLAllowlist`，**既有安装行为不变**。启动时校验条目
（非 URL 前缀永不可能匹配 ⇒ 静默禁用升级）。

### #47：R7b 已完成实现，本批只补测试——**但证明了 R7b 的测试有盲区**
R7b 的三条 #47 测试**直接调用** `driveCatchingUp`/`boundCatchingUp`。把 bound 从 `driveJoin`
**断开**后：
```
R7b 自己的 #47 测试：ok      ← 依然全绿
新增的控制器回路测试：FAIL: TestControllerBoundsCatchingUpForAReachableJoiner
```
⇒ **直接调用被测函数的测试，证明不了它被接进了真实回路**——这与本工程两次「半接线」是同一个失效模式。
新增三条经 `driveInFlightOperations`（真实入口）的测试。

### 变异验证（8 次，7 次命中）
`M1` AtTarget 重新混淆 (b)/(c) → `a second run still plans 3 upgrade(s) … the roll never converges` ·
`M2` 执行器忽略 `Step.BrokerOnly` → **`HALTED at brk1: agent re-exec refused: agent_no_responders`（P3 原始故障链逐字复现）** ·
`28-A/A'/B` 三种半接线形态均被抓 · `47-A` 断开 bound → `after 40 ticks a REACHABLE joiner is STILL in CATCHING_UP` ·
`47-C` 不升级 blockAfterAttempts → 命中。

### 三条风险（已接受）
1. **P3 的 presence oracle 是 session 作用域的 node list**：另一 session 里的未声明同机 agent、
   或注册被清除过的，会被判成 agentless ⇒ broker-only ⇒ agent 留在旧版。
   靠「逐主机大声打印 note」+「声明覆盖观测」+「重跑会捡起来」缓解，但**这是 P3 唯一可能少升而非 HALT 的地方**。
2. **#47 残留（如实披露）**：变异 `47-B`（AddVoter 成功但 `sub.isVoter` 不反映）**未命中**——
   该分支在 hermetic 单节点 raft 里不可达（假 peer 得参与它正被加入的那个 quorum）。
   那行 `return false` 正确且有注释，但**今天没有变异测试**，诚实的验证者是 deploy-tier drill。
3. `TestAgentDaemonReachesTheAllowlistResolver` 有 **20s 上界**而非瞬时——它靠「守护进程*未能*拒绝而后阻塞」
   来检出半接线（第一版在变异下把整包挂住了，故显式加界）。

### 主进程的一处流程错误（记录）
用户建议「每批收口后 `git add` 暂存，便于下批对比」——我**在 R9 进行中就执行了 `git add -A`**，
把 R9 的半程工作一并暂存，导致本批暂存边界糊掉（agent 发现 P3 的文件已在索引里并如实报告了这个异常）。
**订正：只在批次收口时暂存，不得在批次进行中暂存。** R10 起严格执行。

## 14. R9 drill 侧完成记录

### P1 在 deploy-tier 被确证修好——但先要修掉一个「drill 在测自己」的错误
drill 71 的 arm B 此前带 `--now`，而 `--now` 会把收敛 deadline 压成 `now`
⇒ **rc=75 是唯一可达的结局，drill measuring its own flag**。去掉后实测：
**drain 返回 rc=0、expose 迁到幸存 voter 并 SERVE、agent 零次 re-register**。
新的三个分离 oracle：`B-cmd`（rc=0 **或** rc=75 且带 `ErrDataPlaneNotConverged` 精确句）·
`B-migrate`（终态：expose 落在幸存 voter **且**其公网口经真隧道吐出 sentinel）·
`B-silent`（agent MainPID 不变 **且** journal 有 ≥1 条 home-delivery、**零**条 re-register/rebuild）。
另加 `B-rerun`：rc=75 的错误串所建议的补救**真的能收敛**。

### 三个 harness bug（都是 drill 自己的问题，非产品）
1. **jq 的 `//` 吞掉 `false`**：`.assumed // empty` 对 `false` 什么都不输出
   ⇒ `assumed=false` **永久不可断言**。是非恒真证明脚本在任何实跑之前发现的。
2. **write probe 劫持了 ctl 的 session**：`session create` 会 `WriteCurrentSession`，而 probe 共享
   `HOME=/home/sim` ⇒ 从第一次迭代起，所有 session 作用域的读都答的是一个空的临时 session
   （`node ls` 打印 `(no nodes)`；`node ls --brokers` 的 **agent 半边**在真有 skew 的主机上读出 `?`/`skew=false`）。
   broker 半边照常工作（走 cluster-health、非 session 作用域）——**这正是它能潜伏到今天的原因**：
   直到有了 agent 侧 oracle 才暴露。
3. **drill 的散文能搞死自己的数据面**：sentinel token 把自由文本 `$_AS_DRILL` 嵌进单引号 `sh -c` 载荷，
   我给 drill 71 改标题时加的一个撇号 ⇒ `sh: 1: Syntax error` ⇒ **在任何产品断言之前就 SETUP-RED**，连挂 3 轮。

### OQ-6 供给 + G5 首次真覆盖
新增 `drills/lib/colocated.sh`，在一个集群里同时构造 `AgentPresence` 三态：
**DECLARED**（nid≠node_id ⇒ `assumed:false`）· **OBSERVED**（nid==node_id ⇒ `assumed:true`）· **ABSENT**（leader ⇒ plan 读作 `[broker-only]`）。
(b) 态的 HALT **逐字复现** R9 产品侧的 M2 变异：`HALTED at brk2: agent re-exec refused: agent_no_responders`；
**在该处成功反而判 ASSERT-FAIL**。
G5：逐跳推进（只第 1 跳动、2–3 跳未动、失败路径上 leader-last 仍成立）· `skew=true` 后清除 ·
两台 agent 主机的 whole-host 双到版 · agentless 主机用 `(broker_ver|agent_ver)` 元组 · 两轮 roll 的 MainPID 不变 ·
写连续性覆盖**失败的那轮和成功的那轮**。
`cluster unlock`：HALT 后锁仍持有 → 默认清除**拒绝**活 lease → `--force` 清除并确认 → 独立复探 →
**终局证明 = 下一轮 roll 能过 acquire-lock**。
另加**机制 vs 终态**双 oracle：roll 自己的日志必须显示它 re-exec 了 observed 主机的 agent，
且必须显示已修复主机 `SKIP … already at target`
——**只断言终态会把「运维手工重启修好的」记成 `cluster upgrade` 的功劳**。

### 非恒真证明脚本（67/67，已接进 `run-all.sh`）
`tests/r9d-nonvacuity.sh` **逐字**从 drill 文件里抽取每个 oracle 再喂坏输入。最有力的两条：
- `_all_on_next` 在半升级主机上**仍为 TRUE**，而新的 `_whole_host_at` 为 FALSE
  ⇒ **whole-host 判据严格强于旧 oracle**。
- `_roll_halted_on_growlock` 现在能匹配 G4 §B 的 `grow … is in progress` 措辞——**R9-D 之前的模式漏掉了它**。

### 产品未修 → 挂 owner（无一放宽）
`#47` drill 42 终点 3/3 复现 ⇒ **PRODUCT-RED，owner R9 产品侧**（`ledger-crosscheck` 无主 6→5）·
`#34` drill 73，owner R8 产品侧（4 轮未自然触发，用**强制漂移变异**在真实栈上证明分支）·
`#29` drill 71 的 G/F 臂仍被挡，保留为已登记缺口 · `#31` **4 轮均未复现**（R7a 的 release-confirm 不变量成立），
故 drill 30 改为**正向断言**它。

### 主进程补的一件事
`#49` 已在 R6 定案 ALREADY-FIXED、本批 drill 42 复验 GREEN，但台账仍列为未闭合
（`docs/` 在该 agent 权限外）⇒ 已闭合归档。**`ledger-crosscheck` 无主缺陷 9 → 4**（余 #36/#39/#42/#48）。

### 门
`sh -n`+`dash -n` 全过 · `lint-drills` 0 违规 · **`kept-sites --check` PASS 且大幅上升**
（30: 16→63 · 42: 36→42 · 71: 27→38 · 73: 40→47，**只增不减**）· `run-all.sh` 除 `ledger-crosscheck` 外全绿。

## 15. R10 完成记录（DR/备份恢复）——最高价值出口达成

### DR 尾段第一次端到端跑通
drill 51 的 terminus：**灾前 sentinel 从原公网端口 14000 吐回**，经重建反向隧道、
在真实 N=3→全灭→N=1 恢复之后，**全程只用产品动词驱动**。此路径此前从未被证明过一次。
§5.2 逐字执行：`G-nats` 跑的是 **restore 自己印出的 `reconcile nats --manual` 命令原文**，非 drill 代劳。

### 产品侧两处高价值发现
1. **变异验证推翻了修复自己写的理由**：P5 注释称 `Ping()` 是「THE fix」，实测把 `Ping`+`quick_check` 都拿掉
   五态仍全过（任一探针都强制建连）。而 **`quick_check` 从没被测过**：拿掉它一个真损坏的 DB 拿到健康证明
   ——**#50 更深一层**。补第 6 态（corrupt page），每层现在都有只有它能抓的状态。
2. **#53 拆分**（主进程批准）：`#53-silence` CLOSED（两端告警 + 变异验证）；`#53-scope` WONTFIX-BY-DESIGN
   ——把 JS 拉进 bundle 会**强迫 `backup --offline` 去跟活的 nats-server 说话**，让 offline/online bundle
   范围悄悄不同，那正是 #53 本身所属的谎言类。

### H2 翻正（drill 弃手写 seam 改调产品动词）
51 此前手写 3/4 字段 seam（含无效键 `nats_route`）。现改调 `recovery restore --config`，
F-b8 实测断言 seam **五字段齐全 + 无效键消除**。

### 台账闭合 4 条 + 一条新缺口
`#50`/`#51`/`#52`(silence 半)/`#64` 已标 CLOSED；`ledger-crosscheck` 无主缺陷回到 **4**（余 #36/#39/#42/#48）。
**新经验发现（无台账号，须 owner）**：P2 把 seam 写进 restore 制造了 fresh-box 的**属主张力**——
seam 必须写进 root-owned `/etc/tether/broker.yaml`，故 `restore --config` **只有 root 能 apply**；
以 `sudo -u tether`（broker-ops #6 规定的用户）跑会 fail-closed；以 root 跑又留下 `User=tether` 读不了的
root-owned 数据 ⇒ DR 需要一条 `chown -R tether:tether /var/lib/tether`，而 **runbook §5.2 漏了它**。
drill 把它作为唯一残留 `[GAP #6-chown]` 暴露（登记后用 #6 remedy 清除），而非掩盖。

### 门
`make test` · `-race` · 泄漏门 · `make lint` 0 issues · `make e2e` 527s ·
drill hermetic：`kept-sites --check`（50: 71→87 · 51: 67→77，只增）· `r9d-nonvacuity` 89/89 · `run-all.sh` 全过。

## 16. R11 完成记录（身份与凭据）

### deploy-tier 抓到一个 hermetic 漏掉的真缺口——而且不是任何一个初始假说
产品侧四项落地后，drill 52 在真实栈上 B3/55c 仍红。**我给 agent 的三个候选（conf 格式/include 文件/派生方式）全错。**
真因是**输出污染**：`renderDoctor` 在 FATAL 时返 `usageErr`，`main.go:71` 把 `error: <msg>` 打到 **stderr**；
drill 用 `--json 2>&1 | jq` 捕获 ⇒ jq 收到 `{合法JSON}` 后跟 `error: … N FATAL` ⇒ **jq 解析失败**。
`55b`（收敛态）之所以过，是它无 FATAL ⇒ 无 stderr ⇒ 干净 JSON。
**这是只有真实栈的 `2>&1` 合并才暴露的类**——hermetic 测试从没合并过 `main.go` 的 stderr。
**修**：`--json` 模式 FATAL 返回 **quiet 非零**（JSON 的 `summary.fatal` + 退出码已表达失败，
stderr 散文只会在机器消费下污染流）；人类表格模式不变。新增 `TestClusterDoctorJSONStreamStaysParseableOnFatal`
（经 `newRootCmd()` 走 cobra 的 SilenceUsage/Errors + 模拟 `2>&1` 合并 + `json.Unmarshal`），双向变异验证。
**教训（记台账）**：机器输出命令的 `2>&1` 洁净度**必须在 sink 之后测**。

### 四项产品侧
- **#54 facet 1/2**：doctor 接 `readClusterPublicIdentities`、skew 计 FATAL；reconcile 检出 skew 非零退出。
  runbook §2.1 订正（reconcile 是 restart 之后的 fail-closed VERIFY 步）。**r11e 实测 B2/B3/55a/55b/55c 全绿**。
- **#55**：未加新动词（R6 定案：重启即换 issuer、动词多余）；skew 可见即解决。
- **#56/P11**：self-only 动词旁路通用 leader-redirect；「死循环」措辞收窄为 UX 误导。
- **P12/DOC-23**：pin-mismatch 文案改 FILE-level 恢复。
- **#63**：re-pin 机理从源码立住（完整 register 携带新 pin，不 bump epoch），残留（active push 按 epoch 判、
  裸 cert 轮换不推送静默 agent）用 `TestActivePushIsBlindToBareCertRotation` 钉在代码里，归 R14。

### 台账连闭 5 条
#54/#55/#56/#63/DOC-23 全标 CLOSED（#63 带 R14 残留）。`ledger-crosscheck` 无主回到 **4**（#36/#39/#42/#48）。

### drill 52 两条剩余失败 = drill 传输错配，非产品（归 R11 drill 收尾）
- **D8a/D8c**（`alert clear` rc=69）：真错是 `dial admin.sock: no such file`——`alert raise/clear` 是
  **operator-only admin-socket 命令**（设计，`alert.go:29-31`），而 drill 用 `ctl -- alert clear`（ctl 无 admin socket）。
  **产品正确**（operator/member 信任分层，不可弱化）；drill 应改 `_bt brk1 -- tether alert clear`。
  **我初判为 `--ack-alerts` 门、错了**，agent 核实纠正。
- **D2c**：泄漏的 in-flight op = #31 grow-lock 族（R7 残留/R14），drill 自己的 D-spine 已 `product_red` 它。非 R11。

### 门
`make lint` 0 · `make test` 无 FAIL · `-race` · 泄漏门 · `make e2e` 517s。

## 17. R12 完成记录（接入/会话/安全承诺）

### 又一次「证据真、归因错」——#46 台账机理假说被推翻
台账写「`DeriveSeedEndpoints` 不 derive 第 3 endpoint」。实测 `DeriveSeedEndpoints` **本就正确处理 3 voter**。
真缺陷在**触发架构**：per-grow converge 只在 **leadership-acquired 边沿**重触发，稳定单 leader 集群**永不再触发**
⇒ brk2 被下一次 grow 悄悄救回，而**最后一个 voter brk3 无后继 grow**。修：leader 每 observe tick 用
`seedSetEqual` change-gate **幂等** re-converge。这是本工程反复出现的模式的又一例。

### 五项产品侧
- **#25**（P7 安全）：per-IP PIN 限速。`authcallout/ratelimit.go` token bucket（burst 10 / refill 10-min），
  **本地 in-memory、无分布式状态、无新依赖**（`x/time/rate` 已在依赖里）。错误 PIN 计数、超阈连正确 PIN 也拒。
  `client_info.host` 可信边界已在代码论证：nats-server 从 TCP 对端盖、callout 层不可伪造；
  共享 NAT 是**有意接受的 v1 权衡**（memory 安全实用主义）。已入群成员/agent 不再走 PIN 路径 ⇒ 共享 NAT 攻击者
  打不掉现有 session。empty host fail-open（不可被攻击者强制）。**本地 auth_callout e2e 真跑通**（memory 纪律）。
- **#26**：evict agent 侧收割 managed 子进程（exec children `Setpgid`+登记，`agent_evicted` 到达后 SIGKILL 进程组）。
  仅 evict 触发，正常 restart/upgrade 保留子进程供 G.1 reconcile。
- **#27**：`manifest_listen` 在 cluster 模式默认 `127.0.0.1:7480`，init 后 discovery serve-ready。
- **webhook wire 契约**：`webhookPayload` 显式结构体 + schema/version 常量。测试含**反射守卫**
  （未来加 secret 字段会 fail build-gate）+ 逐字段 on-wire 捕获 + **否定断言**
  （对手构造 JSON-injection 的告警仍只序列化出白名单键、无夹带）。**no-secret 不变量结构性成立**。
- **DOC-12**：判断=**订正 architecture §H.1、不补 writer**（把 `agent_evicted` 改名 `kicked` 会破坏 agent 订阅+drill 81+P9）。

### 变异验证（3 条全复现确切缺陷）
#25 guard 禁用 → `11th correct-PIN connect from a blocked IP must be rate-limited`（含真 callout e2e）·
#26 去收割 → `managed exec child STILL present in the host process table after evict (leaked)` ·
#46 去周期 converge → `must converge ALL voters including the 3rd, got [only self]`。

### 门
`make lint` 0 · `make test` 无 FAIL · `-race`（authcallout/agent/broker/p9）· 泄漏门 · `make e2e` 536s。
**零新依赖**（安全实用主义守住）。

### drill 侧待做（R12 drill）
80/81/82 从 R2 制造的 PRODUCT-RED **翻正**（#25/#26/#27 真修好 ⇒ 谓词重写为正向：
80→`assert_refuses <限速签名>`、81→子进程已回收、82→listener 已 bound）；91→#46 翻正（第 3 voter 进 endpoints）。

### R12 drill 侧完成（四笔 R2 红债平掉）
80(#25)→**GREEN**(44断言，限速翻正)· 81(#26)→**GREEN**(收割翻正)· 91(#46)→**GREEN**(第3voter翻正)·
82(#27)→**INCOMPLETE**（#27 翻正本身 GREEN，但撤红后浮出预登记的 U1-U4 user-service in-sim gap，正如 §2 预测 "product_red 压过 INCOMPLETE"）。
**80 的翻正判断**：客户端看不到限速原因（nats-server 只回通用 `Authorization Violation`，callout reason 是 server-only）
⇒ 断言 **broker 日志** `PIN attempt rate-limited` 这条**真实可观测证据**，而非假设一个客户端能看到的串。
非恒真：`tests/r9d-nonvacuity.sh` 加 R12 段（16 条双向，suite 105/105）。80/81/82/91 的 `kept_sites` 只增不减。

## 18. R13 产品侧完成记录（可观测性与可测性能力）· drill 侧另做

**范围**：三项产品 + 文档；**95-D / D6 / 93 收尾 / #42 判断** 属 drill 侧或"物理界限不修"，见下。

### 1) runtime 自省能力（97 的结构性缺口，走 §2 (a)）
新增 broker-local admin 动词 **`tether admin runtime [--json]`**（`OpRuntime`）：返回
`goroutines`（`runtime.NumGoroutine()` **进程内真值**）· `threads`（pprof threadcreate，OS 线程/M 数）·
`open_fds`（`/proc/self/fd`，非 Linux 返 -1）· `rss`（`/proc/self/statm`）· `uptime`（`Now-bootAt`）· 每个 R7 reconciler 的 `last_tick`。
- **走正常 CLI + 正常权限（root-only 0600 admin socket）+ `docs/broker-ops.md §5.20/§7.6` 文档化**——无 build-tag/env/隐藏路径（T2）。
- **只读本进程 + R7a 注册表 `status()`**（R13 只消费 `lastTick`、不改机制）；单机 + 集群、leader + follower 都能答（不碰 DB/raft/leadership）。
- **明令禁止 Threads-as-goroutine 代理**：`goroutines` 恒来自 `NumGoroutine`。变异验证钉死（见下）。
- **pprof 判断 = 不引入**：计数（连采看爬升）足以定位泄漏且已被 socket 网住；常驻 pprof 徒增攻击面（heap/stack dump + CPU-profile DoS）+ 体积，深度栈级取证是罕见离线动作。写进代码注释 + `broker-ops.md`。
- **零新增长期存活对象/goroutine/timer**：快照按需算、`/proc` 句柄 use-then-close（`TestOpenFDCountDoesNotLeak` 钉住）——这个抓泄漏的动词自己不泄漏。
- **`2>&1` 洁净度（R11 教训，测在 sink 之后）**：`--json` 成功只落 JSON、失败只落 stderr——`TestAdminRuntimeJSONStreamStaysClean` 合并 stdout+stderr 后 `json.Unmarshal` 通过；`TestAdminRuntimeJSONFailureWritesNothingToStdout` 断言失败时 stdout 为空。
- **反半接线（本工程 6 次教训）**：`test/p9` 的 `TestAdminRuntimeEndToEnd` 起**真 broker Run**、经**真 socket** 调 `OpRuntime`、断言核心 reconciler 齐 + `node-states` 的 `last_tick` **跨两次调用前进**（证真被接线、值真活）。
  - **半接线变异验证**：删掉 `broker.go` 的 `RuntimeSnapshot: b.runtimeSnapshot` 一行 ⇒ e2e 立刻 `runtime introspection unavailable` FAIL。
- **接线顺序**：注册表创建 + `registerCoreReconcilePasses` **上移到 admin socket 之前**（goroutine-start happens-before ⇒ admin 协程读 `b.reconcilers`/`b.bootAt` 无 race）；`-race` 全绿。

### 2) #39：disk_pressure 间隔 operator knob
`broker.observability.disk_check_interval`（yaml，Go duration）+ `--disk-check-interval`（flag），优先级
**flag > yaml > 内建默认（5m）**（`serveconf.DiskCheckIntervalDuration` + serve 精度分支，与 `proc_gc_interval` 同款）。
子秒/负值 Load 时拒（防 `time.NewTicker` panic + statfs 空转）。**默认保持 5m**。
`TestDiskMonitorHonorsConfiguredInterval` 判定式钉住：20ms 间隔 220ms 内探针 ≥4 次；默认（0⇒5m）**恰 1 次**（仅启动 tick）。

### 3) D3：voter 重启后 `cluster status` exit code 抖动数秒 — **判断=瞬态真实，文档化稳定语义 + opt-in 去抖**
**判断**：瞬态是**真实的**（voter 重启期间冗余度真的下降 ⇒ `0→1(DEGRADED)→0` 是诚实上报，非缺陷）。
故**不改默认**（改默认 = 掩盖真瞬态）：
- **文档化稳定语义**（`docs/cluster-runbook.md §2.4`）：0/1/2/3 各态定义 + 瞬态窗口由 observe/收敛时间界定（leader 每 ~5s 复探，voter 应答且 applied-lag 回落即复位）。
- **opt-in `--settle <dur>` 去抖**（默认关）：只对 `DEGRADED(1)` 等窗口内澄清；清了退 0，窗口后仍 DEGRADED 则**诚实退 1**（sustained NOT-HA 不被掩盖）；`QUORUM_LOST(2)`/`FORCE_SINGLE(3)` **立即返回不去抖**（永非良性重启瞬态）。
`TestSettle*` 五例钉住（healthy 立返 · 瞬态清了退 0 · sustained 退 1 · hard 态立返 ×2）。

### 不做的（判断如实说明）
- **#42（quorum-loss 后 ~10s 误报窗口）**：判断 = **TFence=10s 的物理界限、非缺陷**——`fenced` 需 leader-contact 持续 stale 达 `TFence` 才拒，这是 raft lease 安全性的固有下界（早拒会在良性抖动中误 fence）。**不强行"修"一个物理界限**；留 drill 侧记为**有界观测缺口**（有界、可披露）。
- **95-D**：R6 定案 **REFUTED（假缺口，谓词过严 `leader_id=="brk1"`）**——**drill 侧**收紧谓词 + 加负向臂，**产品侧不改**。
- **D6 / 93 收尾**：drill 侧（94 的 ps LOST overclaim 清偿 / 93 收尾），产品侧不涉及。

### 门
`go build`+`go vet`+`make lint`（0 issues）全过；`internal/broker`+`internal/adminsock`+`internal/serveconf`+`cmd/tether`+`test/p9`+`test/concurrency`（泄漏门）+`test/p7` `-count=1` 全绿；触碰面 `-race` 全绿；命令树 golden 双份已 reconcile（+`admin runtime`、+`cluster status --settle`、+`serve --disk-check-interval`）。
**drill 侧待做（另做）**：`simcluster-coverage-inventory.md §2` 需登记这 3 个新 CLI 面（golden 已更新，inventory 由主进程 reconcile）；95-D 谓词收紧 + 负向臂；#42 记有界观测缺口；D6/93 收尾。

## 18b. R13 drill 侧完成（97/95/94 全绿 + 93 收尾）
- **里程碑：第一个「结构性不可测」缺口变可测**——97 的 goroutine 泄漏门此前永久 not_covered，
  现用 `admin runtime --json|jq .goroutines`（进程内真值）建真门：floor→6-cycle soak→quiesce→重采，
  `post≤pre+GOR_TOL`。契约值头注定死（`GOR_TOL=2*NPROC+16`/`SAMPLES=3`/`QUIESCE=25s`）。实测 76→77⇒无泄漏。
- **95-D 收紧挖出两个更深 bug**：`_d_raft_ok` 从钉 brk1 收紧到「leader 存在且稳定」+两条负向臂证明能红；
  unmask (a) 不存在的 `session rm --yes` flag、(b) 一次性 boot resumer 的 1s JS-probe 竞态。95→GREEN。
  残留（非阻塞）：boot resumer 无周期重试，产品 owner 可选修。
- **94 ps LOST overclaim 清偿**：补 A1d/e/f 真断言 LOST 派生态 + 判别子（agt2 仍 RUNNING）。GREEN。
- **93 webhook 判定 = 既有时序 flake 非回归**（warmup/cleared 总 POST、raise 间歇落⇒同码；真因 re-seed 吞 delta）；
  drill 侧修时序；card/json 用 `--settle 30s` 去抖；#42 记 gap（物理界限）。93→INCOMPLETE(仅#42)。
- 非恒真 `r9d-nonvacuity.sh` +25 → **130/130**。
- 台账：#42 有主(93)、#39 CLOSED(R13 knob) ⇒ ledger 无主 **4→2**（余 #36/#48，R14）。

## 19. R14 产品侧完成记录（Q4/#48/#36）

### `make e2e` 抓到了静态闸 + `-race` + 泄漏门全放行的错误——全工程 e2e 价值最鲜明的一次
Q4 第一版用 **owner-fp 幂等**（同 owner `already_exists` ⇒ 返回已提交 session），**过了所有静态闸 + `-race` + 泄漏门**，
却被 `make e2e` 逮住：`test/d9` 的 `SessionCreateRoutesThroughRaft`/`TwoBrokerJoinReplicates` 做两次同 actor create、
**断言第二次被拒**以证明第一次提交了。任何 owner-fp 幂等都会破坏这个跨包契约。
**这是 e2e 作为最后硬闸的价值：抓单测与静态分析结构上够不到的跨包语义契约。**

### 三项修复
- **Q4**（机理①）：`proposeOrForward` 返回 nil **已意味着写提交到 raft** ⇒ read-back 超时**非致命**，
  首次即返回 **best-effort success**（`{SID,OwnerFP,ACTIVE,now}`，权威 `created_at` 下次 `session ls` 收敛）。
  **杀掉重试死循环**（无重试→无 `already_exists` 死锁）**且不碰重复拒绝契约**（d9 保持绿）。绝不假成功——
  只有 commit 确认后才到那条路径。read-back 窗口 50→**150**（3s，覆盖 R6 实测 1.37s apply-lag ×2 余量、<5s ctl deadline）。
  补 `already_exists` hint + exit 64。单机模式不变（原子 create，无 read-back）。
- **#48**（R6 机理订正：被饿着非投毒）：agent 侧 `rosterRefreshLoop` 数连续静默刷新，
  持续静默 + 缓存 roster 仍有可拨 voter ⇒ `rebuildOnBrokerSilence` 一次性排除静默 broker、重建到幸存者。
  **复用 R8 的 agent 侧 rebuild-onto-voter 机制**（不是 R8 的 broker→agent push——孤岛 agent 从集群够不到），
  #48 只加一个**新触发器（静默）**。
- **#36**：online force-single 的 `if online` 分支在 admin socket 之前补 `rejectedUnattendedYes`，
  `--online --yes` 现在与 offline 一致被 Tier-2 拒（exit 64）。TTY 确认保留。

### 变异验证（4 条，各复现确切失败）
Q4 去 best-effort → `committed-but-not-visible create must report success, got: not visible after commit`（R6 原症状）·
Q4 重加 owner-fp 幂等 → `same-owner duplicate must be ErrAlreadyExists (D9 contract)`（本地钉住 d9 契约）·
#48 禁静默触发 → `agent did not escape the silent broker within 6s (starved on the island)` ·
#36 去 rejector → `want usage error (64), got admin socket dial`（证明它够到了 socket）。

### e2e 收尾三跑记录（诚实留痕）
第 1 次 FAIL = R14 agent 在 e2e 启动后又修的一个 `raft.ServerID/ServerAddress` 类型错误（e2e 用了修前快照）；
第 2 次 FAIL = `TestProxyTunnelReconnectMatrix` 负载 flake（R13 已文档化）；
**第 3 次干净全套 = `ok test/e2e 544s`、rc=0、该 flake 这次 PASS、D9Matrix PASS**。
孤立无缓存复跑该 flake（含 e2e_matrix tag）均 PASS ⇒ 确认 flake 非 R14 回归。

### 门
`go build`/`vet`/`make lint` 0 · `-race`(broker/agent/cmd) · 泄漏门 · `make test` · **`make e2e` 544s rc=0 D9 PASS**。

### drill 侧待做（R14 drill）
Q3（drill fixture 自相矛盾，改种子不自我后台化）· Q4/#48/#36 翻正（22 force-single --yes、40/41 retire 孤岛、96 D3）·
27 处 runtime-guard 判定轮不 fire（确定性化或按 §2 三判据重裁）· #48 台账翻正 · 96 D6b 补提交者归属条件（#65 遗留）。

## 20. R14 drill 侧完成记录（非确定性消除 + 四翻正 + 无主归零）

### 本工程闭环最漂亮的一次：Q4 的 D3 从「60 次全红」到「确定性绿」
drill 96 的 D3（分区中多数派写）在原始缺陷报告里就是那条 **60 次全红、结构上死循环**的断言
（`session create` 非幂等 + read-back 超时 ⇒ `poll_until` 永远转不了绿）。
R6 分区实验精确归因（机理① apply-lag 1.37s > 1s 上限）→ R14 产品侧 best-effort success 修复
→ drill 翻正 → **deploy-tier 确定性 PASS**（日志原文 "now a deterministic positive after the R14 fix"）。
**缺陷报告 → 定案 → 修复 → 验证的完整闭环。**

### 27 处 runtime-guard 逐处处置（终态门核心）
- **9 处 runtime-guard → gap**（§2 判据：非 sim intrinsic timing，而是绑定 open-defect 的覆盖缺口，
  产品修好即退休）：96 的 #57 transfer 两处（determinization 实测不足——1GiB 仍非确定性抢跑/被在飞捕获/audit 在死 home）·
  96 D6b minority-write（R6-REFUTED，需长连接客户端 Y）· 71×4（#29 族）· 73×1（#33/#34）· 51 H1a（字节相同 cert DR 重连可靠抢过 shell）。
- **18 处保留 runtime-guard，判定轮全部 0 触发**（真 sim-timing 诚实阀）。
- **R15 关键数字：R14 范围内判定轮 runtime-guard 触发 = 0。** 终态门本批范围满足。

### 四项翻正最终 verdict
#36（drill 22 **GREEN**）· Q4（96 D3 确定性 PASS）· #48（drill 41 **GREEN**，窗口 210→300s 适配 stock 3-min SLA）·
Q3（96 F 臂种子改 `tether exec` 持有、非 `nohup &` 自退，源码正确但 F 臂被 arm-D 恢复超时 gate 为 gap，非 Q3 所致）。
**A1e 判定**：既非 flake 亦非 #57 缺陷，是 drill bug——#57 的 anti-vacuity control 用了会被杀的 home(agt1/brk2)，
改走全程 UP 的 agt2/brk1 后 audit face 真被测 ⇒ #57 保持 PRODUCT-RED、A1e 不再假失败。v4 确认 96 = PRODUCT-RED af=0 nc_guard=0。

### 提交者归属（R6 #65 遗留）落地
drill 96 D6b 补 `_c3_committed_by`（读 brk1 自己 broker.log 的 `session created` 行确认提交者）+ 修一个 grep-BRE `[^\n]` 在 canary3 的 n 处截断的 bug。
新逻辑：canary3 三 broker 都持久但 brk1 未提交 ⇒ 记**合法多数派提交，非假 #65**——正是 R6 揭示的台账幽灵「5/6 durable minority write」的真相。

### 主进程补的三件收尾（agent 权限外）
- **#36/#48 台账 CLOSE**（产品已修 + drill 翻正向）⇒ `ledger-crosscheck` **首次 OK（无主缺陷归零）**。
- **74 的 9 处级联守卫 runtime-guard → gap**（与 73 同构、landing-neutral，绑定已确认 #33/#34，非 sim-timing）+ 头注说明。
  ⇒ 74 可执行 `runtime-guard` 归零，消除 R15 前唯一剩余的 guard-fire 路径。

### 门
`sh tests/run-all.sh`: **六道全 PASS**（含 ledger-crosscheck 首次 OK）· `kept-sites` 无回归。

---

## §21 R15 收官（据 r15a 全套地面真相重新定范围，进行中）

r15a（全套 -j4 --build）：GREEN=23 / PRODUCT-RED=4(31/42/51/96) / INCOMPLETE=6(30/62/71/73/74/82) / ASSERT-FAIL=4(41/50/52/93)，全 37 `nc_guard=0`（R14 runtime-guard 终态门守住）。#34 经 r15a 证不在范围（73/74 的 C-auto 通过，唯一 gap 是 #30 reader；报告 §311-314 已排除 #34 出根因族）。

### 产品修复（全带 mutation 证明）
- **#58**（96 orphan tier-B 不回收）：`homeOwnsXferBucket`(home 分区)+`reaperMayDelete`(leader) 冲突 → session homed 到非-leader 时无人回收。修：新增 `reaperCaughtUp`（raft-domain caught-up、去 leader）替换 reap gate；pass `leaderOnly:false`（home-authoritative per-broker）；catch-up gate 关闭 reassignment split-view 竞态。测试：`TestReaperCaughtUpGate`/重写 subtest(c)/`TestXferOrphanReapHomePartition`；变异 A(leaderOnly 回退)+B(home-gate 恒真=DATA LOSS) 全 RED。规格表 `reconcile_registry_test.go:473` 同步 false。
- **51 #31/#45**：血脉=restore-侧。`normalizeRestoreStaging` 漏清 grow/upgrade marker+lease+非终态 op → 阻塞 re-grow（assertNoActiveOp）。修：同 txn 加 `DELETE cluster_meta WHERE key IN(grow/upgrade active+lease)` + `DELETE cluster_operations WHERE terminal=0`（保留终态历史）。`TestRestoreClearsStaleGrowUpgradeAndOpResidue` + 变异 RED。（取证 a6d5f11 判定 restore-侧：reconcile 侧对无 lease marker fail-closed、对 stalled retire 无 reaper[topoAdvance numVoters≤1 carve-out 禁 deadline]、新 pass 是禁止的 policy+竞态。）
- **93 webhook raised**：内存态 baseline + 亚秒 raft 租约 blip → :176 reset → :120 re-seed 吞掉 blip 内 commit 的 transition。修：只在**真 handoff 到别节点**(LeaderID≠SelfID)才 reset baseline，同节点 blip 保留（返回补发）；加 `SelfID` 到 cfg。`TestWebhookSurvivesSameNodeLeaseBlip`+更新 handoff 测试；变异(无条件 reset)→ blip 丢失 RED。（取证 a39f659：内存基线不耐 lease jitter；用户"能修的都补修"→ 修而非披露。）
- **#28**（31）：产品**已完成**（agent.go `resolveAgentUpgradeAllow`+`--upgrade-url-allow`+agent.yaml `upgrade.url_allow`；upgrade.go:76 消费 `UpgradeURLAllowlist`；`agent_upgrade_allow_test.go` 证半接线闭合）。仅缺 drill 侧 flip。
- **P3**（30 agentless upgrade）：产品**已完成无需改**（`AgentPresence` 三态+`AtTarget` broker-only+drive 跳 reexec-agent+`reconcileUpgradeLock` lease-expiry 释放锁）；30 drill P3 断言已正向 GREEN。（取证 a558b23b）。

### 诊断结论（决定 drill 侧动作）
- **41**（ASSERT-FAIL，串行仍红=非 flake）：drill over-specified——:157/:159 用 connz 物理断连 oracle，但 non-final retire(N=3→2)时 mesh 未断、silence 结构上不 fire；:161/:214 的功能可达 oracle 已过。附一个未证的产品 fast-path 小缺口(`rosterRequiresReconnect` host/IP 失配)。→ 需 SIM_KEEP live 确认 fixable-vs-GAP。（取证 aca27ecf）
- **50**（ASSERT-FAIL）：drill 自相矛盾，**与 R15 改动无关**。L1+L2 一次性 read 断言 zed absent，但 drill 故意保 brk2 存活+不 de-cluster → self-heal re-cluster 把 zed 复制回；违反 drill 自己 L3 注释（backup-moment 回滚属 total-loss drill 51）。→ 修 drill（身份 oracle 移 51 或删 zed 负半句）。（取证 ae7ccb1）
- **52**：确认 load flake（-j1 隔离串行 GREEN pass=62），r15b 自然复绿。
- **42 #47**：controller 有 extensive R7b 实现；returning 节点 deploy-tier 特定，取证 aca1599 进行中。

### 待办
drill flips（31/51/96）+ #30 reader wiring（71/73/74/96）+ 41/50 drill 修 + 42 定夺 + 披露（62/82/30-bc）→ 硬闸 → re-run → 两轮全套 → coverage-boundary + G-1..G-10 → 停外审门。

### §21.1 stale-binary 事故（重要，2026-07-20）
r15v1/r15v2 用了**02:05 的旧 vendor/tether**——`remote.sh` 需 `--build` 才重建+stage binary（:24 `do_build=0`），我误用无参 `remote.sh`（只 rsync tree、不重建 binary）→ sim docker 镜像嵌了旧 binary（缺 admin events + 缺我的产品修复 #58/93/config）。症状：71/73/74 #30 ASSERT-FAIL（admin events 命令不在命令树）、96 #58 未回收（旧码无 reaperCaughtUp）。drill 编辑是 rsync 的（当前有效），但 binary 旧。**修正（两层）**：(1) `remote.sh --build` 重建 staging vendor/tether；(2) 但 sim 用**缓存 docker 镜像 tether-sim:dev**，还需 **`remote.sh --build build`**（build 子命令→服务器 docker build 重建镜像）。sim 有 stale-image 守卫（simcluster:551 image-sha≠vendor-sha 即 die），r15v3 全 11 drills 死于该守卫（=守卫正确挡了无效结果）。正确命令 `remote.sh --build build` 后 → **r15v4** 用正确 binary+镜像重跑 11 drills。r15v1/v2 结果作废；50✓ 是 drill 修复（有效）、93✓ 是无 blip 侥幸（待 r15v3 确认）。教训：deploy-tier re-run 必须 `remote.sh --build`（产品改动后），否则测旧 binary。
