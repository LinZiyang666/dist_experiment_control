# 整治工程候选 Roadmap（综合稿 · 交主进程定稿）

> 合成自 D1–D4 四份方案 + 四份对抗评审。**所有争议数字已在仓库实测复核**，与四份方案的自述均有出入，见 §0。
> 目标：`test/simcluster/` 37 个 deploy-tier drill 真实全绿；流程止于外审门（外审全部批次完成后统一一次）。

---

## 0. 实测校准（先钉死地面真相——四份方案的清点全部不准）

剥掉整行注释后的**可执行调用点**实测（`test/simcluster/drills/*.sh`，37 个 drill）：

| 类别 | 实测总数 | 分布 |
|---|---|---|
| `assert_bug` | **3** | 22:170(#35) · 31:88(#28) · 51:321(#51) |
| `product_red` | **27** | 52:7 · 50:3 · 51:3 · 96:3 · 40:2 · 41:2 · 91:2 · 92:2 · 22:1 · 90:1 · 93:1 |
| `not_covered` | **26** | 96:10 · 97:5 · 52:4 · 95:2 · 51:2 · 22/50/91:各1 |
| `warn "…NOT-COVERED"`（旁路记账） | **24** | 74:10 · 71:4 · 30:3 · 73:3 · 82:2 · 31:1 · 62:1 |
| `INVERTED` 标记（含注释） | 15 | 绝大多数**已配对** `product_red`（50/52/96），**只有 80/81/82 三处裸 `assert_ok`** ⇒ 正是 H4 点名的三个假绿 |

**结论订正**（写进 plan，防止各批按错数排期）：
1. 简报"20 处翻正"是**严重低估**：终态需关闭的断言站点 = 3 `assert_bug` + 27 `product_red` + 24 转换而来的 `not_covered` + 26 既有 `not_covered` ≈ **80 处**。
2. D3 评审"12 处 assert_bug / 21 个 drill"、D1"74 有 15 处 assert_bug"、D2"assert_bug=8"、D4"36 处 pin"**全部错误**——它们把 `warn NOT-COVERED` 计数当成了 `assert_bug` 计数。
3. D3 评审"15+ 处未登记的倒置 assert_ok"**不成立**：INVERTED 块的仓库范式是 `assert_ok 谓词 + 配对 product_red`（`50:31` 明写 "Guard: R-INVERTED (assert_ok predicate + bare product_red)"），只有 80/81/82 缺配对。但**该范式必须被 lint 钉住**（见 R1 出口），否则未来新增的 INVERTED 块可以再次落成假绿。
4. `lib/assert.sh:72` `_as_pass()` 只被 `assert_ok`(102) / `assert_refuses`(116) / `assert_setup`(192) 调用 ⇒ **`pass` 计数不是"断言条数"**。D1 的 `min_pass 单调不减` 闸在 R2 把倒置 `assert_ok` 翻成 `product_red` 时**必然自伤**（pass 下降）。反注水闸必须重定义（见 §1.3）。
5. `not_covered` 的 26 处**分属两类**，混为一谈会逼出删断言：
   - **gap 类**（缺陷/缺口 pin，产品修好即消失）：`52:440`(#55 窗口) · `96:240`(arm B/C 整臂缺失) · `51:375`(DR terminus) · `52:464/493` · `50:433` 等；
   - **runtime-guard 类**（本轮构造未落地的诚实守卫，**不因产品修好而消失**）：`97:243`（被测进程中途重启）· `97:252`（/proc 读失败）· `97:273`（SOAK_CYCLES < LEAK_MIN_N）· `96:280`（loopback 上 80 MiB 传输常在 kill 前跑完，**non-deterministic in-sim**）· `96:301/304/314/320` · `95:232/251` · `91:63`/`51:431`（grow flake）· `22:130`。
6. `broker.go:1095-1128` 主循环实测有**三种异构形态**：`ReconcileStates`（per-broker-local）+ `reconcilePorts`（leader-only）+ `reconcileTunnelSessions`（per-broker）+ **独立 `gcTicker`（`ProcGCInterval` ≠ `ReconcileInterval`）**。任何"注册表"接口必须原生表达 `interval` + `leaderOnly` + `name`，否则连现状都表达不了。
7. `drills/30-rolling-upgrade.sh:184` 存在一条**四份方案全都漏掉的结构性缺口 OQ-6**：`colocated-agent whole-host leg — sim brokers run no colocated agent`。它是**供给机器缺口（铁律③里 sim 的活）**，不是产品缺陷、也不是宿主物理限制。见 §2.4。
8. `62:118` **不是**裸 `assert_ok` 假绿：它带活体测量 `_statfs_healthy`，断言的是"wedge 是 SIGCONT-reversible 的近似态"。真正的问题是**描述串把它写成"NOT-COVERED"却计 pass**。正解是改描述使其名实相符 + 另立 gap 类 `not_covered` 登记 OQ-2（不是删断言）。
9. `weilandserver` 在 CLAUDE.md §5 中是**专用服务器**（不是 D2/D3 假设的"共享宿主"）。OQ-2 的可行性必须**实测复核**，不得建立在错误前提上（见 §2.3）。

---

## 1. 仪器契约（R1/R2 一次性定形，此后冻结）

### 1.1 verdict ledger：`expected-verdicts.tsv`
37 行 × `drill / 期望 verdict / 四计数器 / 每个非绿格的 owner(gotcha|Q|H 编号) / 归属批号`。

**验收规则（关键：不是"逐格精确相等"）**——四份方案的"逐格相等"在 24 处 warn 是**级联守卫**（`74:240` "the locked SKEW baseline did NOT establish"、`71:190` "the FIXTURE assertion above is RED"、`30:181` "#31 blocked the upgrade"）的事实下不可满足：产品修好一半时 drill 会从 `{fixture RED, 4×not_covered}` 跃迁到 `{fixture 通过, 臂真跑, N×assert_fail}`，这是**事前不可枚举的双向跃迁**。压力之下唯一出路就是放松差分——正好落进禁令。

替代规则（**三条**，机器可判）：
- **N1 无主非绿**：任一非绿格必须能定位到一个 owner 编号 + 归属批号。**无主非绿 = 该批失败**。
- **N2 意外变绿必须解释**：某格意外变绿时，必须给出二选一书面判定：(甲) 上游 fixture 终于建立、下游臂第一次真跑并通过（附该臂 pass 增量证据）；(乙) 空绿（触发一次全套 + 该断言的非恒真复证）。**默认假设是 (乙)**。
- **N3 归属批只许前移不许后推**：非绿格的归属批号一旦写定，只能提前关闭，不得改推到更晚的批（防止债务滚雪球到收官）。

ledger 是**评审工件**，`run-drills.sh` 的 exit code **绝不读它**——runner 始终对任何非 GREEN fail-closed。这一条在 R1 内审逐字确认，否则 ledger 退化成 waiver。

### 1.2 `not_covered` 加 class 参数（本方案对 D4-F1 的正面解）
签名改为 `not_covered <desc> <reason> <class>`，`class ∈ {gap, runtime-guard}`（`lib/assert.sh:177`，加 `_as_argcount 3`）。

- **终态要求**：`gap` 类计数 == 0；`runtime-guard` 类**在两轮判定运行中一次都不得触发**。
- 若判定运行中 runtime-guard 触发 ⇒ **该轮不计入判定的两轮**，且触发点自动成为 R14 的工作项（"把该构造做成确定性的"）。
- 这样既堵死"拿注释/守卫换绿"，又不逼人删掉 `96:280`/`97:243` 这类诚实守卫。**"不许有 not_covered"的扁平规则被明确驳回**（见 §6）。

### 1.3 反注水闸（三层，取代 D1/D3 的 `pass ≥ 基线`）
- **A 断言站点计数单调不减**：`kept_sites = assert_ok + assert_refuses + assert_setup + assert_bug + product_red + not_covered` 的**调用点数**，逐 drill 不得下降。它对"倒置 assert_ok → product_red"是**中性**的（站点数不变），对"删臂/删断言"是**致命**的。R1 冻结初值。
- **B 谓词方向审查（人工，写进每批内审模板）**：本批 touched drill 的 `assert_*` **谓词 diff** 必须逐条标注 `strengthen | equal | weaken`；**任何 weaken 一律驳回**，唯一例外是"修正错误判红"（如 95-D 谓词过严），且必须走 C。
- **C 非恒真证明（全工程通用强制程序）**：**每一条被改动或新增的断言**，必须用一次刻意的坏输入证明它**能红**，证据落盘。这是唯一能挡住 H1（永久空绿）、H6（D6b 空绿）复发的机制，比任何 grep 都硬。从 D3 的 B8 局部程序**提升为全工程通则**。
- **D INVERTED 配对 lint**：任何含 `INVERTED` 标记的断言块必须在同块内配对 `product_red|assert_bug`，否则 lint 红（防 80/81/82 型假绿复发）。

### 1.4 waiver 处置
`ALLOW_PRODUCT_RED` / `ALLOW_INCOMPLETE`（`run-drills.sh:51-52`）**保留但禁用**——D4"物理删除"会让 R3–R14 全程 runner 必然非零退出、与中期 ledger 门语义打架，实操中反而会被临时加回。改为：
- runner 在使用任一 waiver 标志时打印机器可读的 `WAIVER-USED <flag>` 行；
- 终态门断言 argv 无 `--allow-*` **且** rollup 中无 `WAIVER-USED`；
- 全工程**任何批次的验收都不得使用这两个标志**（内审必查项）。

### 1.5 其它 R1 硬件
- `poll_until` 栈式局部化（H5，8 处嵌套含 `lib/tether.sh:42 wait_phase`）；
- **`run-drills.sh` 加 per-drill `timeout`**（实测当前**完全没有**——这是 H5 模式 B 无界挂起能存在的前提）；
- rollup 落盘产物 + `ssh` 补 `ConnectTimeout`/`ServerAliveInterval`（H15）；
- `lint-drills.sh` BATCH 16 → **37**（实测 `tests/lint-drills.sh:27` 现列 16 个，21/37 未受闸）+ 内嵌 jq 字段路径校验（H12）+ 描述串一致性。

---

## 2. 硬问题 2：结构性不可覆盖的原则性答案

### 2.1 三道判据（缺一不可，按序过）

| 判据 | 问法 | 作用 |
|---|---|---|
| **T1 归属测试** | 假如 tether 是完美的，这件事在这里**还测不了吗**？ | 答"还测不了" ⇒ 障碍在**宿主基质** → (b)/(c)；答"就能测了" ⇒ 障碍在**产品** → (a) |
| **T2 防后门测试** | 若这个 drill 根本不存在，**真实运维者还会不会要这个能力**？ | 要 ⇒ 是可运维性能力，(a) 放行；不要、只有 drill 会调用 ⇒ 是测试后门，**禁止** |
| **T3 执行基质测试**（新增，专防 (b) 变 waiver） | 移出去的门有没有**具体宿主 / 具体构造 / 具体断言 / 具体责任人 + 时限**？ | 四项缺一 ⇒ **不许移出**，退回诚实的非绿并在边界声明中披露 |

**(a) 的实现红线**：走正常 CLI 动词 + 正常权限 + `docs/broker-ops.md` 文档化。**严禁** build-tag gate、env-var gate、只有 drill 知道的隐藏路径——违者即"为迎合测试改产品"，内审一票驳回。

### 2.2 #55（account.nk 轮换的 auth-rejection 窗口）→ **(a) 给产品加原子 switch-over 动词**
- T1：答"就能测了"。`docs/deploy-tier-gotchas.md` 自陈 FLIP 条件；#55 不可构造的**根因是 #54（运行中集群 issuer 永不变 NEW）**——把产品做不到的事记成测试做不到的事，正是铁律④的反面教材。
- T2：通过。密钥泄露响应中轮换 account.nk 是常规运维动作，`broker-ops.md` 已按可用书写，现实是没有任何动词能完成、也没有任何动词能看见 skew（P6）。
- **明确驳回 D2 的"分支 2"**（Q1 若定案为 B/C 则把 #55 移出登记）：那是**按省事程度分流**，违反 D2 自己的规则，且移出后无执行基质、永不执行、违反其自设的"不是 waiver 三条"第②条。**Q1 结论只决定动词的语义（砖化解除 vs reconciler 幂等 vs 纯计时），不决定是否建造。** 落地 R11。
- 范围锁死：只做 `stage/commit` 两段式 switch-over + issuer skew 可见性，**不做 CA 管理、不做自动轮换调度**。

### 2.3 OQ-2（真不可中断-D 需 kernel nfsd + hard mount）→ **(b)+(c)，但门槛是 T3，且前提必须实测**
- T1：答"还测不了"（tether 在这里没做错任何事，障碍是内核挂载语义 + 宿主隔离）。
- **前提复核（R6 强制项）**：D2/D3 假设 weilandserver 是"共享宿主"，而 CLAUDE.md §5 称其为**专用服务器**。R6 必须实测回答：能否在专用机的独占时段（或一次性 VM / 快照回滚）上安全构造 kernel nfsd + hard mount。
  - **能** ⇒ 建立独立硬件门 `HW-1`，**真跑真绿**，drill 62 其余臂在 37 内诚实 GREEN。
  - **不能** ⇒ 走 (c)：drill 62 保留一条 **gap 类 `not_covered`**，62 诚实停在 INCOMPLETE，**"37 全绿"不成立**，收官声明写成"36/37 GREEN + 1 条已披露的结构性缺口（OQ-2）"。**诚实的非绿优于改名的绿。**
- **驳回 D1 的 cross-suite 断言**（62 内断言"62X 落盘记录存在/新鲜/GREEN"）：它测的是一份**纸面记录**，任何人手写一个 GREEN 文件即可让 62 变绿，是全套里伪造成本最低的绿；且 D1 同时允许"62X 未执行也可披露交付"，两条自相矛盾。
- `62:118` 的处置：**保留**其活体 `_statfs_healthy` 测量，把描述串改成名实相符的 "Arm2 wedge is SIGCONT-reversible (approximation, not true-D)"，并**另加**一条 gap 类 `not_covered` 登记 OQ-2。不删断言、不改松。

### 2.4 97 的 goroutine 计数 → **(a) 给产品加 runtime 自省能力**
- T1：答"就能测了"（产品零自省面：生产二进制无 pprof/expvar/NumGoroutine 出口）。
- T2：通过。现网车队已有泄漏/崩溃事故，运维在活的 broker 上**根本无法诊断 goroutine 泄漏**；hermetic 层的 `NumGoroutine` 门一出进程就瞎。
- 实现：既有 admin/控制面动词返回 `goroutines / threads / open_fds / rss / uptime / 各 reconciler last-tick`（last-tick 字段**并入 R7 的注册表接口一次到位**，R13 只消费不改机制）；**倾向不引入完整 `net/http/pprof`**（攻击面 + 体积），若引入必须 loopback/unix socket + 配置开关 + 默认关。
- **明确禁止** `/proc/<pid>/status` 的 `Threads` 当 goroutine 代理——Threads 是 M 不是 G，10k 泄漏 G 可零线程增长，那是 false-green oracle。
- **谓词形态必须在 plan 里写死**（防"第一次 flake 就调松阈值"）：`注入负载 → quiesce → 计数回落到 pre-load 基线 ± tolerance`，复用仓库 `test/concurrency/helpers_test.go` 的 poll-with-tolerance 手法 + `leak.sh` 已在用的 bounded high-water 纪律，**刻意不用 goleak**。drill 的 fd/RSS 判据**一个字不放松**，goroutine 只是第四条同样严格的曲线。

### 2.5 同类缺口的统一裁决（四份方案全部漏掉，本稿补入）

| 缺口 | 出处 | T1 | 裁决 | 归属批 |
|---|---|---|---|---|
| **OQ-6** colocated-agent whole-host leg（sim brokers 不跑同机 agent） | `30:184` | "就能测了"——但障碍在**供给机器**，属铁律③里 **sim 的活** | **(d) sim 能力补齐**：给 broker 主机供给 colocated agent（R9 的 whole-host 双到版判据本就需要它） | R9 |
| **`sys.events` 无 operator reader（#30）** | `73:271` / `74:490` | "就能测了"（产品缺读取面） | **(a)**，与 D4 的 DOC-12 三事件 writer 同面 | R12 |
| **96 arm B（run-PTY kill-broker → DOC-28）** | `96:240` | 由 R6 定案（drill 自述 GREEN-by-design source-closed） | 若确为 source-closed ⇒ (c) 出界并书面登记；否则写臂 | R6 定案 → R14 |
| **96 arm C（expose-crash RETURN + home_reassign_failed）** | `96:240` | "就能测了" | 移交 **drill 71** 的覆盖领地，由 R8 写 | R8 |
| **96/95/97 的 runtime-guard 非确定性**（loopback 传输抢跑、orphan 造不出、DELETING 泊不住、soak 进程重启） | §0-5 | "就能测了"（sim 构造能力 + 少量产品可观测性） | **确定性化**，不许留成守卫 | R14 |

**闭合声明纪律**：终态 out-of-suite ledger 的条目集合必须与 §2 的逐条裁决**精确对应**。任何后续想走 (b) 的项，必须先过 T1+T3 并经主进程明确放行——**默认驳回**。

---

## 3. 批次序列（R1–R15，含一条条件批 R8x）

> 规模标注：S=small（<1 天当量）· M=medium · L=large · XL（需拆批阀门）
> 所有批次：plan（对抗草拟→主进程定稿）→ 实现 → 内审（对抗 workflow）。外审只在 R15 后一次。

### 阶段一 · 仪器与基质（R1–R3）——**零产品逻辑改动**

#### R1 — 仪器地基：可重入等待 / 超时 / 落盘 / lint 去盲区 / 契约扩展
- **覆盖**：H5（`lib/log.sh:26-38` 全局 `_pu_*` → 栈式，8 处嵌套点含 `lib/tether.sh:42 wait_phase`）· **per-drill `timeout`（当前完全缺失）** · H15（rollup 落盘 + ssh 超时参数）· H12（lint 看穿内嵌 jq 字段路径与描述串）· H13 通用部分（失败卡片不再 `head -8` 截掉 `(exit N)` 行；四段 `&&` 复合断言拆解规范）· H16 · `not_covered` 加 `class` 参数（§1.2）· `kept_sites` 计数器 + `--verify-ledger` 差分（§1.1/1.3A）· INVERTED 配对 lint（§1.3D）· lint BATCH 16→37 · `WAIVER-USED` 标记（§1.4）
- **出口断言**：
  1. 构造用例证明 `poll_until` 模式 A（冒用内层 desc）与模式 B（无界挂起）**均已消除**；
  2. `tests/verdict-contract-test.sh` 覆盖：`class` 参数缺失必红 · `kept_sites` 下降必红 · **"倒置 assert_ok 翻成 product_red 后 kept_sites 不得误报"**（这条专测 §0-4 的坑）· ledger 的 N1/N2/N3 三规则正反两向；
  3. `sh tests/lint-drills.sh` BATCH=37 可运行（此前未受闸的 21 个 drill 暴露的违规**就地修正**，不得靠缩回 BATCH 消红）；
  4. rollup 落盘产物存在且可 parse 出 37 行（本轮实测 summary 一次都没打印过，落盘是后续所有对账的前提）；
  5. **`git diff` 不得触及 `internal/ cmd/ scripts/`**；
  6. **ledger 初值由本批批末的两轮全套取交集建立**（不是一轮）；两轮不一致的 drill 标 `unstable` 并记录抖动幅度。
- **中间态说明**：本批**允许 verdict 变化**（H5/H15/timeout 修复必然把静默挂起转成确定性失败），但**每一处变化必须绑定到 H5/H12/H15/timeout 的具体修复点**。D1"本批不改变任何 verdict"的写法被驳回（自相矛盾）。
- **验证**：hermetic（`verdict-contract-test.sh` + `lint-drills.sh` + `sh -n`/`dash -n` 全 lib，秒级）+ **全套 ×2**（建基线，全工程唯一的双轮建基线）
- **依赖**：— ｜ **规模**：L

#### R2 — 记账诚实化（H4）：全工程唯一被允许扩大红区的批
- **覆盖**：24 处 `warn "…NOT-COVERED"` → `not_covered(<desc>,<reason>,<class>)`（分布实测：74:10 · 71:4 · 30:3 · 73:3 · 82:2 · 31:1 · 62:1；**多数是级联守卫 ⇒ class=runtime-guard**，仅少数是真缺口 ⇒ class=gap）· 80/81/82 三处裸倒置 `assert_ok` 配上 `product_red`（#25/#26/#27）· `62:118` 描述串正名 + 另加 gap 类 `not_covered` 登记 OQ-2 · 26 处既有 `not_covered` 逐处标注 class · ledger 首次写入 owner + 归属批号
- **出口断言**：
  1. `grep -cE '^[[:space:]]*warn[[:space:]].*NOT-COVERED' drills/*.sh` == **0**；
  2. `lint-drills.sh`（BATCH=37）exit 0；INVERTED 配对 lint 通过；
  3. 三个假绿 drill（80/81/82）**必须落 PRODUCT-RED**——落不了就是 H4 没修对；
  4. **ledger 红区扩大的每一格必须在本批 plan 里事先枚举**，带 owner + 归属批号（#25→R12 · #26→R12 · #27→R12 · 各 runtime-guard→R14）。**未被预先枚举的新非绿 = 本批失败**；
  5. `kept_sites` 逐 drill 不下降（倒置翻转对它中性）。
- **中间态**：**这是全工程红峰值 #1**。验收看的是"扩大是否精确等于声明"，不是绿数。
- **验证**：hermetic + **全套 ×1**（记账语义是套件级，必须整体重算 ledger）
- **依赖**：R1 ｜ **规模**：L

#### R3 — 部署基质：install.sh 未引号 heredoc（P9）
- **覆盖**：P9（11 处 heredoc 仅 1 处加引号；正文反引号被以 **root** 命令替换）· 新增 `tests/lint-install.sh` 永久回归门
- **出口断言**：
  1. 11 处 heredoc 逐处判定"需展开/不需展开"并书面给理由，不需展开者一律 `<<'EOF'`；需展开者显式注释；
  2. **文件级证据**：在 weilandserver 上断言 install.sh 写出的文件内容与源文本**逐字节一致**（含反引号原样落盘）；
  3. `remote.sh` 的 re-vendor 路径生效性：断言容器内 install.sh 内容指纹 == 仓库指纹（防 B5 型"改了没生效"误判）；
  4. **归因纪律**：受污染的 6 个 drill（30/40/41/71/73/74）此时 H1/H3/H7/H9/H10/H11 **尚未修**，因此本批**不做 verdict 级归因**——ledger 中这 6 个 drill 的偏移一律标 `deferred-attribution`，到 R4/R5/R9 各自修好 oracle 时再回溯。（吸收 D2 评审 O1）
- **为何排这么早**：P9 是**地基污染源**，效果上等同于 oracle 缺陷。R6 的定案实验（Q1 要同采 `nats.conf` issuer + `broker.err`）对证据纯度依赖最强，**绝不能在一条仍会以 root 做命令替换的 install.sh 上做证伪实验**。（吸收 D1 评审 O1）
- **验证**：`sh tests/lint-install.sh` + `make lint` + `make test` + **全套 ×1**（install.sh 是全 37 drill 的共同供给路径）
- **依赖**：R2 ｜ **规模**：S

### 阶段二 · Oracle 修复（R4–R5）——**零产品逻辑改动，唯一例外 P8**

#### R4 — Oracle I：升级面（30）
- **覆盖**：H1（`_ver_of` 查 `.nodes[].nid/.release`，实际是 `.brokers[].NodeID/.BrokerVer` ⇒ 版本断言恒失败 + dry-run 断言**永久空绿**）· H3（`_do_roll` rc 丢弃 / `roll.log` 不落盘 / 终末 warn 硬编码反向叙事 MECH=0）· **P8**（`brokerVersionRow` 补 json tag + schema_version bump）
- **H1↔P8 强制耦合**：H1 修读取端、P8 改产出端 schema，分批做会导致 `_ver_of` 修两次并留一个虚假绿窗口。这是全方案**唯一**允许产品改动混入 oracle 批的破例。
- **出口断言**：
  1. **dry-run 路径**的版本断言用一次注入的错误版本证明**能红**（非恒真证明，§1.3C）；
  2. `_do_roll` 的 rc 被检查、`roll.log` 落盘并进失败卡片、终末叙事从 roll.log 推导；
  3. `node ls --brokers --json` 由 drill 30 以 **jq 逐字段**断言（不是 grep 存在性）；
  4. **真 roll 路径的非恒真证明延后到 R7 之后补做**——实测 `30:151` 注释写死"real roll 在 ~每次 live run 都 HALT 在 acquire-upgrade-lock（#31 泄漏的 grow lock）"，#31/P3 未修前真 roll 跑不起来。R4 出口**不得**要求它。（吸收 D3 评审 O1）
- **验证**：`make test` + `make lint`（P8 是 Go 改动，先 hermetic）+ 单跑 30 ｜ 不跑全套
- **依赖**：R3 ｜ **规模**：M

#### R5 — Oracle II：混沌 / HA / 观测面
- **覆盖**：H6（96 D6b 空绿：canary3 从未写入却落"已回滚"分支；rc=69 记成 rc=0 ⇒ **#65 地面真相污染**）· H14（96 F0c 门控注释承诺但代码不存在；F0c 与 F4 同谓词 ⇒ 一根因计两条 ASSERT-FAIL）· H7（40 两个不一致 leader 判据；`sim_leader` fallback 硬编码到刚被杀的 brk1）· H8（42 fixture 的 `tether push` 补 `--ack-alerts`）· H9（71 B-migrate 失败落终态证据）· H10（74 harness 前置失败被叙述成 `#33 族 release-blocking` ⇒ 订正叙事）· H11（74 Arm C 滚动重启后 settle；`_count_on` 的 -1 fail-closed 让 0-home broker 中选）· H13（93 三条 RED 的可定位诊断）
- **H2 明确不在本批**：drill 51 手写 seam 的唯一合规修法是"改调 `recovery restore --config`"，而该动词是 P2（R10）。此时修 H2 只能去**手写一份更漂亮的 4 字段 seam**，正是铁律②④禁止的代劳。**H2 归 R10。**（吸收 D2 评审 F2）
- **出口断言**：
  1. **H8 的正解是修 fixture 不修门**——`gateDestructive` 拦 push 是设计如此，本批把该门翻成 `assert_refuses` 带签名的 KEPT invariant（`lib/assert.sh:106` 已支持 sig-regex 强制）；
  2. 96 的 F0c 真实实现且与 F4 不再同谓词；
  3. 40/42 从 SETUP-RED 转为**有内容的**红或绿（SETUP-RED 残留视为未完成）；
  4. 每一条被改动的断言过非恒真证明；
  5. 新暴露的红逐条挂 owner（rejoin/resnapshot 段→R9 · 74 Arm C→R8/R9 · 96 的 Q 面→R6）；无主新红 = 本批失败。
- **验证**：单跑 40 / 42 / 71 / 74 / 93 / 96（`-j` 并发 ≈30–40 min）+ lint ｜ 不跑全套
- **依赖**：R4 ｜ **规模**：L

### 阶段三 · 定案（R6）——**只取证、不改实现**

#### R6 — 定案实验批：Q1–Q4 前半 + 候选 gotcha + 设计如此项 + 结构性缺口三判据
- **覆盖**：
  - **Q1**（恢复 gen-1 account.nk + 重启后 120s 认证未恢复；A 轮换单向砖化(blocker) / B 纯计时 / C reconciler 重渲反向 skew）——**同采 `nats.conf` issuer + `broker.err`**，必须唯一区分 A/B/C；
  - **Q2**（被分区少数派对客户端是连接黑洞 vs `handler.go:102-104` fenced()→deny 的设计意图）——可证伪预测：brk1 journal 应有 `authcallout: handle failed`，验证其**有或无**；
  - 候选定案：**#65**（分区少数派 stale-leader 写有时持久，raft safety，**≥10 轮统计定性**，须在 H6 修好后的干净地面真相上做）· #35 · #36 · #46 · #59 · #63 · #33（未归因）· #34（复核"fire-gate 正确 DEFER"）· #42 · #49 · **95-D**（很可能是谓词过严的假缺口）· `96:240` arm B 是否 source-closed；
  - **设计如此项判定表**：gateDestructive 拦 push · force-single 必须人工确认 · fenced()→deny · #34 的 DEFER；
  - **结构性缺口三判据裁决**（§2）：#55 / OQ-2 **可行性实测**（专用机独占时段 or 一次性 VM 能否安全承载 kernel nfsd hard mount）/ 97 / **OQ-6** / #30 operator reader；产出 `coverage-boundary.md` 初稿 + out-of-suite ledger 初稿。
- **出口断言**：
  1. 每条产出三元组【假说 → 可证伪预测 → 实测证据（journal 行号 / 命令输出）→ 裁定（CONFIRMED-DEFECT / REFUTED / AS-DESIGNED）→ 归属批】。**禁止"可能/疑似"结论**；无法定案者必须写成"实验设计 X 已执行、证据不足、需 Y 条件"并排入 R14，**不得默认为缺陷，也不得默认为非缺陷**；
  2. AS-DESIGNED 项的 drill 改用 `assert_refuses` **正面钉住那道拒绝**（不是绕过、不是删除；`kept_sites` 因此不降）；
  3. **#65 若判 CONFIRMED ⇒ 触发条件批 R8x**（见下），并预先声明：raft safety 缺陷未闭合前**工程不得宣布全绿**；
  4. **OQ-2 的 T3 四项（宿主/构造/断言/责任人+时限）当批填满或明确填不满**——填不满即锁定 §2.3 的 (c) 分支，收官声明形态在此刻确定，不许拖到 R15；
  5. **零产品逻辑改动**；唯一允许的产品改动是纯观测（日志补 issuer 指纹），且必须论证不改变任何控制流，并**走独立提交、ledger 比对在观测改动之前的构建上做**。
- **验证**：针对性单跑 40 / 52 / 62 / 95 / 96 + scratchpad 一次性取证脚本（不入库）｜ 不跑全套、不跑 e2e
- **依赖**：R5 ｜ **规模**：M

### 阶段四 · 架构根因（R7–R8 + 条件 R8x）

#### R7 — 架构 A：周期对账注册表（"该周期性重跑却只跑了一次"）
- **覆盖**：#58/P10（reaper boot-only + `reaperMayDelete()` 非-leader 恒 false）· #31（grow lock best-effort 释放失败不重试）· P3 的**锁面**（`releaseUpgradeLock` 只在干净完成时调 ⇒ stale-lock 清除动词，改 lease + TTL）· #45（retire op 停滞 `NATS_ROLLED_OUT` 永不 terminal ⇒ watchdog）· #47 · #49
- **接口设计（实测约束，§0-6）**：注册元组必须是 `(name, interval, leaderOnly, lastTick, fn)`——现状已有三种异构形态 + 独立 `gcTicker`，扁平单 interval 表达不了。`lastTick` 字段**在此批一次到位**（R13 的可观测性只消费不改机制，避免 D2 评审 O3 的规则自打脸）。
- **出口断言（内审的唯一判据）**：
  1. **不变量**：每个 pass 只允许"读期望态 → 比对实际态 → 调用**既有幂等**命令路径"，**禁止新建策略**（一票否决项）；
  2. 每个 pass 有三条 hermetic 单测：(a) 收敛后连续 3 tick 零副作用；(b) 中途外部改动后重新收敛；(c) 两 broker 并发跑不重复写；
  3. **冻结判据不是"审一遍"，而是"用注册表重写现有 3 个 call site + gcTicker，行为等价（假时钟证明）"**，外加 #31（leader-only + 重试退避）作为**第三形态样本**证明接口跨形态泛化；
  4. CLI 在收敛未达成时**不得返回 rc=0**（族的共同失败形态根治点）；
  5. **翻正同批**：40 的 `product_red`×2（#45）· 96 的 #58 相关 `product_red` · **`30:158`/`30:165` 两条倒置 assert_ok**（谓词 `_roll_halted_on_growlock`，#31 一修好立刻为假 ⇒ 若不同批翻转，本批自伤变红）——D3/D4 都漏了这两处；
  6. **R4 遗留的"真 roll 路径非恒真证明"在本批补做**。
- **验证**：`make test` + `go test ./internal/broker/ ./internal/cluster/` + `-race` + **仓库内建 NumGoroutine/fd 泄漏门（非 goleak）** + `make e2e` → 全绿后 **全套 ×1**（主循环是全 37 drill 的共因面）
- **依赖**：R6 ｜ **规模**：XL（若接口重写超出可审范围，按预设阀门拆 R7a 接口/R7b 族成员）

#### R8 — 架构 B：不依赖对端事件的主动投递
- **覆盖**：P1（`home.go:121` 自陈 "on the next reconnect"，而 `clusterdrain.go:151-152` 的 drain 根本不断连接 ⇒ 新 home 永不投递）· #48（agent 黏在退役 broker 孤岛、旧 broker 继续供 stale VOTER roster）· P3 的**语义面**（`AtTarget` 对无同机 agent 短路为 broker-only）· #57（在飞 tier-B 的 home broker crash 后终态 audit 永不写）· #33（按 R6 归因结论处置）· **`96:240` arm C（expose-crash RETURN + home_reassign_failed event）移交本批写入 drill 71 领地**
- **出口断言**：
  1. **不变量**：home/roster 变更的投递**不以对端产生任何事件为前提**。判据 = agent 侧**完全静默**（不重连、不重启、不发命令）下，drain 之后数据面必须在有界时间内跟随；
  2. 投递通道本身注册为 R7 注册表的一个 pass（append-only，不改机制），白拿 R7 的幂等/收敛测试框架；
  3. drain / retire / upgrade 三个动词各有一条"rc 语义"断言（控制面写成功但数据面未收敛 ⇒ 非零）；
  4. **翻正同批**：71 的 4 处 runtime-guard/gap（H9 已落终态证据）· 73 的 3 处 · 96 的 arm C 部分。
- **验证**：`make test` + `-race` + 泄漏门 + `make e2e` → 单跑 30 / 40 / 43 / 71 / 73 / 74 ｜ 不跑全套（R7 刚跑过；若 ledger 出现"无主变绿"则追加）
- **依赖**：R7 ｜ **规模**：L

#### R8x — 【条件批】raft safety：#65
- **触发**：当且仅当 R6 判 #65 为 CONFIRMED。
- **覆盖**：分区少数派 stale-leader 写持久（共识层）。
- **出口断言**：确定性复现构造 + 修复 + `-race` + `make e2e` + 96 的 D6b 从 gap 类 not_covered 转真断言。**若修复规模超出单批**，则按预先声明升级为独立 roadmap 项，且**工程不得宣布全绿**，收官声明必须并列披露。
- **验证**：`make test` + `make e2e` + `-race` + 单跑 96 + 全套 ×1
- **依赖**：R6（触发）、R7 ｜ **规模**：L（不可预估，故独立成批）

### 阶段五 · 单点产品面（R9–R13）

#### R9 — 升级与集群生命周期
- **覆盖**：P3 的 `AtTarget` 收尾 · #28（agent 升级 URL 白名单硬编码不可配）· #47 · #49 · #37（retire 换主收敛）· #35/#36/#46 按 R6 判定表处置 · **§6 未覆盖面**：G5 滚动升级机制（净覆盖为零）· whole-host 双到版判据 · **OQ-6：sim 给 broker 主机供给 colocated agent**（§2.5）· rejoin / resnapshot 整段（H8 已解锁）
- **出口断言**：
  1. G5 每一跳的版本推进 + 服务连续性 + 失败回滚路径均被断言（依赖 R4 修好的 `_ver_of` / `_do_roll`）；
  2. whole-host 双到版判据显式定义并断言；无 colocated agent 时的 broker-only 语义单独断言；
  3. **OQ-6 落地后**，colocated-agent whole-host leg 首次真实覆盖，`30:184` 的对应 warn 消解；
  4. #47：joiner 在有界时间内离开 CATCHING_UP 或以明确错误终止（不得以"最终会好"放宽）；
  5. 42 的 rejoin/resnapshot 整段**首次端到端跑通并绿**；
  6. R7 的 stale-lock 清除动词被 drill 30 实际调用并断言锁确已释放；
  7. **AS-DESIGNED 的 force-single 人工确认不得取消**，用 `assert_refuses` 钉住；
  8. 翻正：31 的 `assert_bug`(#28) + 1 处 warn → `assert_ok` · 22 的 `assert_bug`(#35，按 R6 判定) · 41 的 `product_red`×2 · 30 剩余 warn。
- **验证**：`make test` + `make e2e` + `-race` → 单跑 10 / 11 / 12 / 22 / 30 / 31 / 40 / 41 / 42 / 43（≈20 min 并发）｜ 不跑全套
- **依赖**：R8 ｜ **规模**：L

#### R10 — DR / 备份恢复：让 DR 尾段第一次被证明
- **覆盖**：P2（`recovery restore` 加 `--config`，调现成 `applyClusterSeam`@`cluster.go:880`）· **H2（在 P2 之后翻正：drill 51 弃手写 seam，改调产品动词）** · P4（restore 剪到单 voter 却不去集群化 nats.conf ⇒ 复用 `clusterstatus.go:354` remedy）· P5（`storage.OpenReadOnly` 后加 `db.Ping()`）· #52 · #53 · D2（runbook §5.2 缺 seam 步骤 / §5 备份路径不可写 DOC-19 / JS 不随 bundle DOC-27）
- **出口断言**：
  1. **【最高价值出口】51 的 DR 尾段（#52 → #53 → terminus）一次不落端到端跑通并全绿**——这是全工程唯一"至今从未被端到端证明过一次"的路径；
  2. 断言必须**从订正后的 runbook 逐字执行**，不得由 drill 脚本代劳 runbook 缺失的步骤（铁律②③④：一次 DR 若靠 drill 的复杂脚本才成功，那是 tether 的失败被掩盖）；
  3. **P5 是"守门人说谎"类缺陷**（`doctor --offline --db <不存在>` 报 0 fatal exit 0），优先级高于其表面严重度：出口覆盖"库不存在 / 空文件 / 截断 / 目录 / 权限拒绝"五态；**并回扫所有把 doctor 当 `assert_setup` 用的 drill，逐个用坏输入证明前置门现在真能失败**；
  4. #53：bundle 含 JS 或不含但**明确告警**，二选一并论证；**静默丢 history/audit 不可接受**；
  5. 翻正：50 的 `product_red`×3 + `not_covered`×1 · 51 的 `assert_bug`×1 + `product_red`×3 + `not_covered`×2。
- **验证**：`make test` + `make lint` + `make e2e` → 单跑 50 / 51（DR 重，≈15 min）｜ 不跑全套
- **依赖**：R8 ｜ **规模**：L

#### R11 — 身份与凭据：issuer skew 可见 + 原子 switch-over（#55 的 (a) 解）
- **覆盖**：P6/#54（doctor 接 `readClusterPublicIdentities`；`reconcile nats` 检出 skew **必须非零退出**——当前 false all-clear，是第二个"守门人说谎"）· **#55：account.nk 两段式原子 switch-over 动词（stage/commit）** · **Q1 结论落地**（语义由 R6 的 A/B/C 定案锁定）· P11（self-only 动词旁路通用 leader-redirect；`clusterdrain.go:386-388` vs `clusterstatus.go:649-657`+`cluster.go:625` 统一）· P12/DOC-23（pin-mismatch 砖化态文案改为"还原旧 cert 文件"）· #63（按 R6 定案）· D1 台账订正（#52/#54/#63 + #56 措辞收窄）
- **出口断言**：
  1. switch-over 是**真实运维能力**：CLI 动词 + `docs/broker-ops.md` 文档 + 权限与幂等语义。**build-tag / env-var / 隐藏路径一律驳回**；
  2. 动词落地后 **#55 的 auth-rejection 窗口在 drill 52 被实际构造并断言**。若仍构造不出，当批查明是动词语义不足（继续改产品）还是 Q1 结论有误（回退定案），**不得退回 not_covered**；
  3. P11/P12 的验收标准是"**按产品给出的指引逐字执行能真的恢复**"，不是"文案读起来合理"；
  4. 翻正：52 的 `product_red`×7 + `not_covered`×4 全部消解（B4–B7/B5d 从 gap 类 not_covered 转真断言）；
  5. **范围锁**：只做 switch-over + skew 可见性；不做 CA 管理、不做自动轮换调度。若 Q1 定案为假说 A 使范围显著扩大，按预设阀门拆 R11a（可见性 P6/P11/P12）/ R11b（staged rotation）。
- **验证**：`make test` + `go test ./internal/broker/` + `-race` + **本地 auth_callout 模式真跑通**（MEMORY 纪律：改 NATS perm/auth_callout 不在远端循环调试）+ `make e2e` → 单跑 13 / 52 ｜ 不跑全套
- **依赖**：R6, R10 ｜ **规模**：L（含拆批阀门）

#### R12 — 接入 / 会话 / 安全承诺
- **覆盖**：P7/#25（PIN CONNECT per-IP 限速；`architecture.md §E.6` 承诺未实现，`client_info.host` 已可得）· #26（evict 不清 managed 子进程）· #27（well-known discovery 不 serve-ready）· D4（DOC-12 三事件补 writer；80/81 零实现）· **`sys.events` operator reader（#30，§2.5）** · **webhook on-the-wire JSON 契约（三层全无覆盖，触及安全承诺）**
- **出口断言**：
  1. 限速断言**两向都要**：超阈被拒（`assert_refuses` 带签名）+ 未超阈不被误伤；对抗性覆盖单 IP 高频 / 多 IP 分散 / 代理后（`client_info.host` 的可信边界须在 plan 论证，不得假设可信）。遵"安全实用主义"：不引入分布式状态与新依赖；
  2. #26 断言 evict 后 managed 子进程在**宿主进程表**中确已消失（不是仅看 CLI rc）；
  3. webhook wire 契约三层（触发→序列化→投递）逐字段断言 + **一条否定断言**（畸形/伪造载荷被拒），不得只断言 HTTP 200；
  4. **80/81/82 必须全部落 GREEN**——它们是 R2 故意制造的红的**唯一消除者**（倒置 assert_ok 的翻法是**重写谓词**：80 的 #25 翻成 `assert_refuses <rate-limit-sig>`、81 的 #26 翻成"子进程必须已回收"、82 的 #27 翻成"listener 必须已 bound"，**不是换关键字**）；每条过非恒真证明；
  5. 82 的 warn×2 消解。
- **验证**：`make test` + `-race`（限速涉并发计数）+ `make e2e` → 单跑 80 / 81 / 82 ｜ 不跑全套
- **依赖**：R8 ｜ **规模**：M

#### R13 — 可观测性与可测性能力（97 的 (a) 解）
- **覆盖**：**runtime 自省能力**（goroutines / threads / open_fds / rss / uptime / reconciler last-tick——last-tick 由 R7 接口提供）· #39（disk_pressure 间隔固定无 knob）· #42（quorum-loss 后 ~10s 误报窗口，按 R6 定案）· D6（94 声称覆盖 `ps` LOST 实为零断言 ⇒ **overclaim 清偿**）· D3（`cluster status` exit code 抖动 ⇒ 定义并文档化稳定语义）· 93 收尾 · **95-D 按 R6 定案处置**
- **出口断言**：
  1. 能力走**正常 CLI 动词 + 正常权限 + `docs/broker-ops.md` 文档**；若含 pprof 必须 loopback/unix socket + 配置开关 + 默认关 + 一条"关闭时零暴露"断言；
  2. 97 的 goroutine 门谓词写死为 `注入负载 → quiesce → 回落到 pre-load 基线 ± tolerance`（§2.4），**容差、采样点数、判定窗口在 plan 阶段即为契约**，不得留给实现；`/proc` Threads 代理明令禁止；
  3. 94 的 `ps` LOST：补真断言或**收回声明并同步 README**；不允许继续 overclaim（`kept_sites` 显著上升是"还债"的正面证据）；
  4. **95-D 若判为假缺口，处置方向必须是收紧谓词到正确语义**，且必须在同 drill 内新增一条**负向臂**证明新谓词能红——谓词只有在能红的时候，它的绿才有意义；
  5. 97 的 5 处 not_covered：gap 类清零，runtime-guard 类移交 R14。
- **验证**：`make test` + `-race` + **泄漏门**（新长期对象正是它要抓的形态）+ `make e2e` + `make lint` → 单跑 90 / 92 / 93 / 94 / 97（97 是 soak，最长）｜ 不跑全套
- **依赖**：R8, R12 ｜ **规模**：L

### 阶段六 · 收口（R14–R15）

#### R14 — 【命名溢出批】非确定性消除 + Q3/Q4 落地 + 无主残余
> 匿名 buffer 在执行期一定会变成垃圾桶——本批**必须命名、进 DAG、是 R15 的前置**。（吸收 D3 评审 F4）
- **覆盖**：
  - **runtime-guard 非确定性消除**（终态门要求它们在判定运行中一次都不触发）：`96:280`（loopback 上 80 MiB 传输抢跑 ⇒ 构造确定性中断点）· `96:301/304/314/320`（orphan 造不出 / 计数不可读 ⇒ 可读性可由 R13 的自省能力补）· `95:232/251`（N=2 raft/JS 解耦、DELETING 泊车）· `97:232/243/252/273`（被测进程稳定性 + 样本数下界）· `91:63`/`51:431`（grow flake）· `22:130`；
  - **Q3**（exec 返回 rc=0 但进程表 30s 内从未记为 RUNNING）· **Q4**（session create 写已提交却报失败、非幂等、`error_hints.go` 无 `already_exists`）——先定案后修，Q4 若确认非幂等须补幂等 + 提示条目；
  - R6 中"证据不足"项的复议；R9/R10 新覆盖面暴露的**无主新缺陷**（G5 / DR 尾段 / webhook 三层几乎必然产出）；
  - `96:240` arm B 的最终处置（source-closed ⇒ (c) 出界书面登记；否则写臂）。
- **出口断言**：
  1. `class=runtime-guard` 的每一处：要么构造确定性化、要么**证明该构造在 sim 中不可确定性化并按 §2 三判据重新裁决**（不得默认保留）；
  2. Q3/Q4 定案三元组齐全，CONFIRMED 项当批修复 + 翻正；
  3. 全仓 `class=gap` 的 `not_covered` 计数 == 0（OQ-2 的 (c) 分支除外，见 §2.3）；
  4. `assert_bug` == 0、`product_red` == 0、`warn NOT-COVERED` == 0；
  5. ledger 中无任何无主非绿格。
- **验证**：`make test` + `make e2e` + `-race` + 泄漏门 → 单跑 22 / 51 / 91 / 95 / 96 / 97 ｜ 不跑全套
- **依赖**：R9, R10, R11, R12, R13（+ R8x 若触发）｜ **规模**：L

#### R15 — 收官：文档台账 / 边界定稿 / 终态闸
- **覆盖**：D1（台账三处订正 + #56 措辞收窄，若 R11 未做完）· D3 · D5（README drill 表 verdict 漂移 + scenario 列腐化）· DOC-28 · **`coverage-boundary.md` + out-of-suite ledger 定稿**（条目集合必须与 §2 逐条裁决精确对应）· OQ-2 的最终形态落地（HW-1 真跑真绿 **或** (c) 的诚实披露）· 终态 lint 规则全部生效 · `expected-verdicts.tsv` 收敛
- **出口断言**：§4 的 G-1…G-10 全部成立并逐条留证。**本批禁止修任何新发现的缺陷**（新发现回退 R14；收官批不能同时是冻结批和兜底批）。
- **验证**：**裸跑全套 ×2**（不同 `-j` 档；两轮均满足终态门且结果一致）+ HW-1 在其宿主执行（若 (b) 分支）+ `make test` + `make e2e` + `make lint` + `-race` + 泄漏门 → **停在外审门**
- **依赖**：R14 ｜ **规模**：M

---

## 4. 硬问题 1：「真实全绿」的操作化定义（G-1…G-10，全部机器可判）

| # | 判据 | 判定方式 |
|---|---|---|
| **G-1** | 37 行 `DRILL-VERDICT` 一条不缺，每行 `verdict=GREEN rc=0 assert_fail=0 setup_red=0 product_red=0 not_covered=0` | 解析落盘 rollup（**不读 stdout**——H15 证明 stdout 不可靠） |
| **G-2** | runner exit 0、rollup 打印 `ALL GREEN`；**argv 无 `--allow-*` 且 rollup 无 `WAIVER-USED`** | wrapper 断言 |
| **G-3** | INFRA-ABORT = 0、无 VERDICT-RC-MISMATCH、无 CONTRACT-ERROR | 37 行 verdict ∩ 37 drill 名，集合相等 |
| **G-4** | **连跑两轮**（不同 `-j` 档）均满足 G-1..G-3 且结果一致；两轮中 `class=runtime-guard` 的 `not_covered` **一次都未触发**（触发即该轮不计入判定轮） | rollup 逐轮比对 |
| **G-5** | infra-flake 重跑记录落盘、每条附 infra 签名、**重跑前后 verdict 必须一致** | `run-drills.sh:171 is_flake` 产物审计。**不要求"重跑次数为 0"**——那是宿主属性不是 tether 属性（见 §6 驳回项） |
| **G-6** | 可执行代码中 `assert_bug` == 0 · `product_red` == 0 · `class=gap` 的 `not_covered` == 0 · `warn .*NOT-COVERED` == 0 · 无未配对的 INVERTED 块 | lint（剥整行注释后扫描） |
| **G-7** | `sh tests/lint-drills.sh`（BATCH=**37**）exit 0 · `sh tests/verdict-contract-test.sh` exit 0 · `sh tests/lint-install.sh` exit 0 | 三条 exit 0 |
| **G-8** | **`kept_sites` 逐 drill ≥ R1 冻结初值**；R15 交付逐 drill 对照表（初值 vs 终值）作为"没靠改松测试换绿"的正面证据 | 差分脚本 |
| **G-9** | `make test` + `make e2e` + `make lint` 全绿；触碰并发/reconcile/传输/Raft 面另过 `-race` + 仓库内建 NumGoroutine/fd 泄漏门（**刻意不用 goleak**） | 三条 exit 0 |
| **G-10** | 声明格式：**绝不允许裸 "ALL GREEN"**。必须写成"37/37 GREEN，覆盖边界见 `coverage-boundary.md`，边界外 N 项（逐条列出宿主/构造/断言/责任人/时限）" | 文本模板校验 + ledger 集合比对 |

**翻正回归的显式排期**（硬问题 1 后半）：80 处站点已逐批分配——R2 转换 24 处 warn（记账）；R7：30×2(倒置) + 40×2 + 96 部分；R8：71×4 + 73×3 + 96 arm C；R9：31×2 + 22×1 + 41×2 + 30 剩余；R10：50×4 + 51×6；R11：52×11；R12：80/81/82×3(重写谓词) ；R13：93×1 + 94 + 97 gap；R14：96 剩余 + 95 + 91 + 22。**契约铁律：修复与其翻正同批同 PR**——`assert_bug` 修好后 exit 0 会判 ASSERT-FAIL "APPEARS FIXED"，不同批必然留一个自制的红。

---

## 5. 硬问题 3：「先变红」的中间态在哪几批、验收怎么写

| 阶段 | 批次 | ledger 允许方向 | 验收标准 |
|---|---|---|---|
| **红峰值 #1** | **R2 后** | **唯一允许扩大**（80/81/82 GREEN→PRODUCT-RED；24 处 warn 转 not_covered 使若干 GREEN→INCOMPLETE） | 扩大的**每一格必须在 R2 plan 里事先枚举**，带 owner + 归属批号。未被枚举的新非绿 = 本批失败。**这是可以事前枚举的**，因为 H4 是纯记账转换、不解锁任何被短路的执行路径 |
| **红峰值 #2** | **R4/R5 后** | 允许扩大，但**不要求事前逐格枚举**（H1 的空绿散去、H8 解锁 rejoin 整段、H11 解锁 74 Arm C 后的红是路径依赖的，事前不可知） | 用 **N1 无主非绿**：每一格新非绿必须能挂到已登记 owner 或**当批新开编号**；无主 = 失败。**明确不采用"多红一行少红一行都算失败"**（见 §6 驳回项） |
| **中立** | R3, R6 | 不许有语义变化 | R3 的 6 个受污染 drill 标 `deferred-attribution`；R6 的观测改动走独立提交、ledger 在其之前的构建上比对 |
| **单调收缩** | **R7 起全部** | **只许缩小** | 任何 GREEN→非 GREEN 一律判回归、硬失败。新写的断言首次为红**不算变红**（不在基线内，入账即记真实 verdict）。**N2**：意外变绿必须书面二选一（真修好 / 空绿），默认假设空绿 |

**R2 的红债归属表**（写死在 R2 plan，构成红债的可追踪链）：#25→R12 · #26→R12 · #27→R12 · 74 的 10 处级联守卫→R8/R9 · 71 的 4 处→R8 · 30 的 3 处→R7/R9 · 73 的 3 处→R8 · 82 的 2 处→R12 · 31 的 1 处→R9 · 62 的 1 处→R15（OQ-2）。**R12 不过，R2 的账就留在红区**——这个归属关系在 R2 plan 里就写死。

---

## 6. 硬问题 6：不该直接修的 → 定案实验批（R6，Q3/Q4 落 R14）

| 类别 | 条目 | 处置 |
|---|---|---|
| **开放问题**（报告明写不得当已定结论） | Q1（A/B/C 三假说）· Q2（黑洞 vs fenced deny） | **R6 只取证不改实现**。Q1 结论**书面锁定 R11 的动词语义**——假说 A 要砖化解除、假说 C 要 reconciler 幂等，两者是不同的产品改动，先定案才不返工 |
| | Q3（exec rc=0 但从未 RUNNING）· Q4（session create 写已提交却报失败 + 非幂等） | **R14**（不阻塞任何批，故排后） |
| **候选态 gotcha** | #33 #34 #35 #36 #42 #46 #49 #59 #63 **#65** · 95-D 假缺口 · `96:240` arm B | R6 定案 → CONFIRMED 进 R8/R9/R13/R14；REFUTED 则删除/降级台账并同步 README；**#65 CONFIRMED ⇒ 触发 R8x 并预声明"未闭合则不得宣布全绿"** |
| **设计如此（严禁"修"）** | `gateDestructive` 拦 push（H8 已证）· force-single 必须人工确认 · `handler.go:102-104` fenced()→deny（Q2 待证）· #34 的 fire-gate 正确 DEFER | R6 出判定表 → 对应 drill 改用 **`assert_refuses` 带签名正面钉住那道拒绝**（不是绕过、不是删除、不是放宽）。H8 的正解是**修 fixture 补 `--ack-alerts`，不是拆门** |
| **逃生舱封堵** | — | **R15 不得新增任何"设计如此"判定**。任何新增判定必须在对应功能批内完成论证并经内审，不得在收官现场追加 |

**结论纪律**：R6 禁止输出"可能 / 疑似"。无法定案者写成"实验设计 X 已执行、证据不足、需 Y 条件"并排入 R14，**不得默认为缺陷、也不得默认为非缺陷**。#65 需 ≥10 轮重复采样（且必须在 H6 修好后的干净地面真相上做）。

---

## 7. 硬问题 4：批次粒度原则（15 批 + 1 条件批）

1. **共享文件单批独占，且结构性改动一次性买断**：`lib/assert.sh` / `lib/log.sh` / `run-drills.sh` / `tests/lint-*.sh` 的**全部逻辑改动集中在 R1/R2**，此后各批对它们只做 append-only 的登记行。若某产品批发现必须再改共享 lib 逻辑，视为 R1 遗漏，须在该批 plan 显式立项并按 R1 同等标准（hermetic 契约测试 + 全套回归）处理。
2. **机制一次性定形并冻结**：周期 reconciler 注册表只在 R7 定形（含 `lastTick` 字段一次到位），族成员散落 R8/R9/R13 只做 append-only 注册。这是"六个族成员挤进一批不可审"与"六批反复改同一主循环"之间的第三条路。
3. **一批 = 一个可独立论证的不变量**（内审专家能就单个不变量给判据）：R7 是"pass 幂等且收敛即 no-op"；R8 是"投递不以对端事件为前提"；R10 是"守门人不说谎"；R11 是"轮换可协调 + oracle 非零退出"；R12 是"承诺即实现"；R13 是"观测能力 ≠ 被测行为"。
4. **`drills/lib/*.sh` 按 family 单一 owner 批**：`cluster.sh`→R7/R9 · `dataplane.sh`+`proxy.sh`→R8 · `artifact.sh`→R10 · `ident.sh`→R11 · `events.sh`→R12 · `leak.sh`→R13。**drill 文件本身也划所有权**（D3 评审 O6）：80/81/82→R12（R13 不得再碰）· 96→R7/R8/R14 分段声明 · 71→R8。
5. **单批规模上界**：touched drill ≤ 10 且 hermetic 腿能在一次 `go test` 子集跑完；超过即拆。R7/R11 预设拆批阀门。
6. **批数 15**：更少会突破共享文件单一 owner 原则；更多会让 ledger 差分的评审开销超过批本身。

---

## 8. 硬问题 5：验证成本 —— 全套 36 min 只在 6 个时点跑（共 8 轮 ≈ 4.8 h）

**默认每批只跑**：hermetic 腿（`make test`/`make e2e`/`make lint`，秒到 8 min）+ 本批 touched drill 单跑（`run-drills.sh -j`，宿主 `fs.inotify.max_user_instances` 已调至 8192，7 并发绰绰有余）。

**跑全套的触发条件（仅四条）**：
- (i) 改了 `lib/*.sh` / `run-drills.sh` / `lint-drills.sh` 语义 → **R1（×2 建基线）、R2**
- (ii) 产品改动落在全局地基 → **R3（install.sh 是 37 drill 共同供给路径）、R7（broker 主循环是共因面）**、R8x（若触发）
- (iii) 终局 → **R15（×2，不同 `-j` 档）**
- (iv) 任何批出现 **无主的变绿**（N2 的 (乙) 分支）→ 临时加跑一次定位

**通用铁律**：任何触及 Go 代码的批，**hermetic 两层（`make test` + `make e2e` ≈8 min）必须在跑任何 drill 之前跑完并全绿**。drill 比 hermetic 贵 20 倍——用 36 分钟的部署层测试去抓一个 Go 层空指针，是本工程最容易犯也最贵的错误。

---

## 9. 吸收清单（四份方案的 salvageable，已并入上文）

| 来源 | 保留内容 | 落点 |
|---|---|---|
| D1 | measurement-first 的三条不可交换依赖论证（H1→升级批、H2→DR 批、H5→全套公共污染面）· expectations ledger 的"双向严格"思想（改造后保留）· 根因族整批做 + 明确排除 #34 · B3 定案批夹在仪器与产品之间 · #55/97 的 (a) 裁决 | R1–R2 排序、§1.1、R7/R8、R6、§2.2/2.4 |
| D2 | **三腿证明**（hermetic 尺 / 裸命令 transcript / drill 回归锁），尤其"腿 3 只有在该 drill 自身尺子缺陷同批修好后才许翻正向" · 共享文件所有权切分 · 翻转工作量实测订正 · 铁律④每批固定检查项（"本批新增 drill 行数是否超过被删的 GAP 绕过行数"） · R8 内审模板 | 各批 verification、§7、§0、内审模板 |
| D3 | 按"机制"而非"条目"切根因族（两条独立不变量）· R7 的一票否决项"pass 只读期望态→比对→调既有幂等路径，禁止新建策略" + 三条幂等 hermetic 单测 · leadership-gate 逐 pass 显式声明（leader-only vs per-broker-local 不得混用一个门）· H1↔P8 强制耦合 · "绝不允许裸 ALL GREEN" · **非恒真证明程序升为全工程通则** | R7、R4、G-10、§1.3C |
| D4 | 从终态倒推的分层（oracle→账本→基质→定案→架构→单点→终态）· "修 pin 与翻 pin 同批"由契约推出 · **`not_covered` 分类（gap / runtime-guard）** · 97 的"drill 判据一个字不放松，补的是观测能力不是被测行为" · H8 的"修 fixture 不修门"升为通则 · **OQ-6 / #30 operator reader / 96 arm B/C 的补充枚举** | 全局、§1.2、§2.4、R5、§2.5 |
| 评审共识 | min_pass 重定义 · P9 前置 · 命名溢出批 · ledger 基线双轮取交集 · #55 分支 2 删除 · H2 归 DR 批 · 62X 纸面断言删除 | §1.3A、R3、R14、R1、§2.2、R5/R10、§2.3 |

---

## 10. 驳回清单（逐条理由）

| 被驳回 | 来源 | 理由 |
|---|---|---|
| **`min_pass`（pass 计数）单调不减** | D1 | 实测 `lib/assert.sh:72` `_as_pass` 只被 `assert_ok`/`assert_refuses`/`assert_setup` 调用。R2 把倒置 `assert_ok` 翻成 `product_red` 会使 80/81/82 的 pass **必然下降** ⇒ 旗舰防线在第一次真用时自伤。改为 `kept_sites` 断言站点计数（§1.3A） |
| **62 内的 cross-suite 断言（校验 62X 落盘记录存在/新鲜/GREEN）** | D1 | 断言对象是一份**纸面记录**，伪造成本为零；且与"62X 未执行也可披露交付"直接自相矛盾（62X 未执行 ⇒ 记录缺失 ⇒ 62 RED ⇒ 37 全绿不成立）。改为 §2.3 的二分：真跑 HW-1，或诚实停在 36/37 + 披露 |
| **"预先写死 B2 预期红行清单，多红一行少红一行都算失败"** | D1 | 对 R2（纯记账转换）**可行且保留**；对 R4/R5（解锁被短路的执行路径）**逻辑上不可满足**——要求预先知道只有修好仪器才能知道的答案，落地只会退化成事后回填或被放宽。改为 N1 无主非绿规则（§5） |
| **"无法归因的新红一律当作 harness 自身 bug 在批内修掉"** | D1 | 对归因方向预设偏见（默认怪 harness），而 H10 恰是反例（把 harness 前置失败误报成产品缺陷）。最省力的合规路径变成"判定它是 harness bug 并改掉断言"——结构性的 toy 化压力。改为：归因不明者带 signature 冻结、排入 R14，**禁止在同批修改产生该红的断言语义** |
| **#55 的"分支 2"（Q1 定案为 B/C 则移出登记）** | D2 | 按省事程度分流，违反 D2 自己的路由规则；且移出后无执行基质、永不执行、违反其自设"不是 waiver 三条"第②条。#55 的根因明确是产品缺 switch-over 动词，一律走 (a)（§2.2） |
| **"H2 先行"排在 P2 之前** | D2 | 物理不可能。H2 的唯一合规修法是改调 `recovery restore --config`，而该动词就是同批的 P2。强行先行只能去手写一份更漂亮的 seam = 铁律②④禁止的代劳。H2 归 R10 |
| **物理删除 `ALLOW_PRODUCT_RED`/`ALLOW_INCOMPLETE`** | D4 | 删除后 R3–R14 全程 runner 必然非零退出，与中期 ledger 门语义相反且无优先级规定，实操中最省事的路径就是临时加回 waiver。改为保留但禁用 + `WAIVER-USED` 标记 + 终态 argv 断言（§1.4） |
| **终态 lint "`not_covered` 计数 == 0"（扁平规则）** | D4/D3 | 实测 26 处中过半是**运行时诚实守卫**（`96:280` loopback 抢跑、`97:243` 被测进程重启、`95:232` N=2 解耦失败），它们不因产品修好而消失。归零的唯一途径是删掉它们 ⇒ 把"我没构造成功"静默变成 GREEN，比 waiver 更危险（不留痕）。改为 class 分流 + runtime-guard 在判定轮不触发（§1.2），并把非确定性消除排成 R14 |
| **"逐格精确相等 / 意外的绿也失败"** | D4/D3 | 24 处 warn 是**级联守卫**，产品修好一半时会出现事前不可枚举的双向跃迁；"上游 fixture 终于建立、下游臂第一次真跑并通过"这个**真实的好消息**会被判失败，反复经历后必然退让成单向差分。改为 N2：意外变绿**必须解释**（默认假设空绿），而不是必然失败 |
| **"infra-flake 重跑次数必须为 0"** | D1 | `run-drills.sh:171 is_flake` 的类目全是宿主环境 flake（systemd PID1 死亡 / container-not-running / inotify 上限），与 tether 无关。要求两轮零抖动是把终态门押在宿主上，大概率现场被放宽——而被放宽的恰是一条防注水条款。改为 G-5：重跑落盘 + 附签名 + **前后 verdict 必须一致** |
| **"37 套件内所有 INVERTED 都是未登记假绿"（15+ 处）** | D3 | 实测证伪：仓库范式是 `assert_ok 谓词 + 配对 product_red`（`50:31` 明写），50/52/96 均已配对。真正裸露的只有 80/81/82 三处。但该范式必须由 lint 钉住（§1.3D） |
| **"weilandserver 是共享宿主"（作为 OQ-2 移出的唯一理由）** | D2/D3 | CLAUDE.md §5 称其为**专用服务器**。这个前提未经证实却独力支撑整个 (b) 分支。改为 R6 强制实测复核，能跑就真跑 HW-1（§2.3） |
| **匿名 buffer 批位 / "残余 gotcha 由收官批兜底"** | D1/D3 | 收官批不能同时是冻结批和兜底批；匿名 buffer 在执行期必成垃圾桶。改为命名的 R14，进 DAG 且是 R15 的前置 |
| **"翻正 = 换 assert 关键字"** | D1/D2/D3 | 对 `assert_bug` 成立（契约自动判 APPEARS FIXED），对**倒置 `assert_ok` 不成立**——80 的 #25 正确翻法是 `assert_refuses <rate-limit-sig>`（谓词整个反过来），81/82 同理；且倒置 pin 在产品修好后可能因不相关原因保持为真而**静默继续绿**。改为"重写谓词 + 非恒真证明"（R12 出口④） |

---

## 11. 主要风险与缓解（供定稿时压缩进各批 plan）

- **R-a｜R7 把一次性破坏动作周期化 ⇒ 每 30 s 重复破坏一次**（比原缺陷严重一个数量级）。缓解：一票否决项"pass 只读期望态→比对→调既有幂等路径，禁止新建策略" + 三条幂等 hermetic 单测 + leadership-gate 逐 pass 显式声明 + `-race` + 泄漏门 + 批末全套。
- **R-b｜R7 接口冻结不当 ⇒ R8/R9/R13 四批返工 × 36 min**（全工程最大单点）。缓解：冻结判据是"用注册表**重写**现有 3 个 call site + `gcTicker`，行为等价（假时钟证明）" + #31 作第三形态样本 + `lastTick` 一次到位，而不是"审一遍"。
- **R-c｜R1 的 ledger 基线写错 ⇒ 后续每批建在错地基上**。缓解：双轮取交集 + `unstable` 标记 + 内审专家**从 drill 源码独立重算 37 行**并逐行比对，不一致必须在 R1 内闭合。
- **R-d｜R9/R10 的新覆盖面（G5 净覆盖为零 / DR 尾段从未证明 / webhook 三层）几乎必然产出新缺陷 ⇒ 范围失控**。缓解：新缺陷登记后一律进 **R14**，禁止就地扩大当前批；R15 禁止修任何新缺陷。
- **R-e｜(a) 类加能力滑坡成测试后门**。缓解：T1+T2+"红线三禁"（无 build-tag / 无 env gate / 无隐藏路径）写进 R11/R13 的 plan 并逐条书面论证；内审设显式审查项"这个能力真实运维者会不会用"。目前通过的只有 #55 switch-over 与 97 runtime gauge 两项。
- **R-f｜install.sh 经 `remote.sh` re-vendor 路径滞后 ⇒ R3 改了不生效、误判"没修好"**。缓解：R3 第一条验证步骤即断言容器内 install.sh 内容指纹 == 仓库指纹。
- **R-g｜铁律④持续滑坡**（drill 越写越复杂才能让操作"成功"）。缓解：每批内审固定检查项——本批新增的 drill 代码是否在替 tether 干集群操作的活；具体化为一问：**本批新增 drill 行数是否超过被删除的 GAP 绕过行数？** 超过必须解释。
- **R-h｜外审只在末尾一次，累积判断偏差到最后才被检验**。缓解：R6 判定表、ledger 的逐批留档（构成完整红绿涨落审计轨迹）、`coverage-boundary.md` + out-of-suite ledger、G-8 的 `kept_sites` 对照表——四份可独立复核的书面产物，外审可据此逆向审查全链。
