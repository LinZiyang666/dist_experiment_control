# 整治工程总纲 — 把 simcluster 推到真实全绿（R1–R15）

Date: 2026-07-18 · 起点 commit `fec3bfa` · 定稿人：主进程
输入：`docs/reviews/simcluster-full-suite-run-2026-07-18.md`（全量实跑缺陷报告）
候选稿存档：`docs/reviews/allgreen-roadmap-synthesis.md`（4 视角对抗草拟 + 4 对抗评审 + 综合，9 agent 全存活）

> **目标（用户下达）**：按缺陷报告逐条修复，最终让 `test/simcluster/` 的 37 个 deploy-tier drill
> **真实全绿**——tether 真的经得起测试，**不是把测试改松、不是 toy/快乐测试**。
>
> **流程特例（用户下达）**：批次可分多批，但**外审只在所有批次全部完成后统一进行一次**；
> 未推进到外审门之前不停。故各批走 §3 的 plan → 实现 → **内审**，外审统一在 R15 之后。

---

## 0. 实测校准（地面真相，已由主进程逐条复核）

剥掉整行注释后的**可执行调用点**（`test/simcluster/drills/*.sh`，37 个 drill）：

| 类别 | 实测 | 说明 |
|---|---|---|
| `assert_bug` | **3** | 22(#35) · 31(#28) · 51(#51) |
| `product_red` | **27** | 52:7 · 50:3 · 51:3 · 96:3 · 40:2 · 41:2 · 91:2 · 92:2 · 22/90/93:各1 |
| `not_covered` | **26** | 96:10 · 97:5 · 52:4 · 95:2 · 51:2 · 22/50/91:各1 |
| `warn "…NOT-COVERED"`（旁路记账） | **24** | 74:10 · 71:4 · 30:3 · 73:3 · 82:2 · 31:1 · 62:1 |
| 裸倒置 `assert_ok`（无配对 product_red） | **3** | 仅 80/81/82 —— 即 H4c 点名的三处假绿 |

**终态需关闭的断言站点 ≈ 80 处**（缺陷报告与本工程简报早期写的"20 处"是唯一 gotcha 编号数，**不是**站点数，已订正）。

另两条承重复核：
- `lib/assert.sh:72` 的 `_as_pass` **只**被 `assert_ok`/`assert_refuses`/`assert_setup` 调用 ⇒ **`pass` 不是断言条数**，
  不能用作反注水闸（否则 R2 把倒置 `assert_ok` 翻成 `product_red` 时会自伤）。
- `run-drills.sh` **完全没有 per-drill `timeout`** ⇒ 这正是 H5 模式 B 无界挂起能拖垮全套的前提。

---

## 1. 「真实全绿」的操作化定义（G-1…G-10，全部机器可判）

| # | 判据 |
|---|---|
| G-1 | 37 行 `DRILL-VERDICT` 一条不缺，每行 `verdict=GREEN rc=0` 且四计数器全 0。解析**落盘 rollup**（不读 stdout——H15 证明 stdout 不可靠） |
| G-2 | runner exit 0、rollup 打印 `ALL GREEN`；**argv 无 `--allow-*` 且 rollup 无 `WAIVER-USED`** |
| G-3 | INFRA-ABORT=0、无 VERDICT-RC-MISMATCH、无 CONTRACT-ERROR；37 行 verdict ∩ 37 drill 名集合相等 |
| G-4 | **连跑两轮**（不同 `-j` 档）均满足 G-1..G-3 且结果一致；两轮中 `class=runtime-guard` 的 `not_covered` **一次都未触发** |
| G-5 | infra-flake 重跑落盘 + 附 infra 签名 + **重跑前后 verdict 一致**（**不**要求重跑次数为 0——那是宿主属性不是 tether 属性） |
| G-6 | 可执行代码中 `assert_bug`==0 · `product_red`==0 · `class=gap` 的 `not_covered`==0 · `warn .*NOT-COVERED`==0 · 无未配对 INVERTED 块 |
| G-7 | `lint-drills.sh`(BATCH=**37**) · `verdict-contract-test.sh` · `lint-install.sh` 三条 exit 0 |
| G-8 | **`kept_sites` 逐 drill ≥ R1 冻结初值**（`kept_sites` = 六类断言的调用点总数）；R15 交付初值 vs 终值对照表，作为"没靠改松测试换绿"的正面证据 |
| G-9 | `make test` + `make e2e` + `make lint` 全绿；触碰并发/reconcile/传输/Raft 面另过 `-race` + 仓库内建 NumGoroutine/fd 泄漏门（**刻意不用 goleak**） |
| G-10 | 声明格式：**绝不允许裸 "ALL GREEN"**。必须写成「37/37 GREEN，覆盖边界见 `coverage-boundary.md`，边界外 N 项（逐条列出宿主/构造/断言/责任人/时限）」 |

---

## 2. 结构性不可覆盖缺口的原则性答案

### 三道判据（按序过，缺一不可）

| 判据 | 问法 | 作用 |
|---|---|---|
| **T1 归属** | 假如 tether 是完美的，这件事在这里**还测不了吗**？ | 「还测不了」⇒ 障碍在宿主基质 → (b)/(c)；「就能测了」⇒ 障碍在产品 → **(a)** |
| **T2 防后门** | 若这个 drill 根本不存在，**真实运维者还会不会要这个能力**？ | 要 ⇒ 可运维性能力，(a) 放行；只有 drill 会调用 ⇒ **测试后门，禁止** |
| **T3 执行基质** | 移出去的门有没有**具体宿主 / 构造 / 断言 / 责任人+时限**？ | 四项缺一 ⇒ **不许移出**，退回诚实的非绿并在边界声明中披露 |

**(a) 的实现红线**：走正常 CLI 动词 + 正常权限 + `docs/broker-ops.md` 文档化。
**严禁** build-tag gate / env-var gate / 只有 drill 知道的隐藏路径——违者即「为迎合测试改产品」，内审一票驳回。

### 逐条裁决

| 缺口 | 裁决 | 依据 |
|---|---|---|
| **#55**（account.nk 轮换 auth-rejection 窗口） | ~~**(a)** 给产品加原子 switch-over 动词~~ **→ R6 撤销：不加动词**。真实问题并入 **P6/#54**（skew 可见性 + reconcile fail-closed），R11 已修 | **R6 实测推翻前提**「运行中集群 issuer 永不变 NEW」：`topology_reconcile.go:233` 从**进程启动 seed** 实时导出、`natsreconcile/reconcile.go:157` 纯内容比对换、非 generation-gated ⇒ **带新 seed 重启一台 broker 20s 内就变 NEW issuer**，只重启一台即构造出 #55 的确切 skew（且 auth_callout 跨 broker 队列组 ⇒ ~1/N 授权违规掷硬币，比 per-broker skew 更糟）。故 #55 非「不可构造」而是「skew 无人可见 + reconcile 报 false all-clear」= #54。加原子动词对「重启即换 issuer」多余（铁律④）。落 R11 = 只修 #54 可见性 |
| **OQ-2**（真不可中断-D 需 kernel nfsd + hard mount） | **(b) 或 (c)，前提须实测**：CLAUDE.md §5 称 weilandserver 为**专用服务器**（非共享宿主）⇒ R6 必须实测能否在独占时段/一次性 VM 上安全构造。**能** ⇒ 建独立硬件门 HW-1 真跑真绿；**不能** ⇒ 62 保留一条 gap 类 `not_covered`，收官写成「36/37 GREEN + 1 条已披露的结构性缺口」 | **诚实的非绿优于改名的绿**。明确驳回「62 内断言一份落盘 GREEN 记录」——那是全套里伪造成本最低的绿 |
| **97 goroutine 计数** | **(a)** 给产品加 **runtime 自省能力**（goroutines/threads/open_fds/rss/uptime/各 reconciler last-tick） | T1「就能测了」（产品零自省面）。T2 通过：现网已有泄漏/崩溃事故，运维在活 broker 上根本无法诊断。**明令禁止**用 `/proc/<pid>/status` 的 Threads 当 goroutine 代理（Threads 是 M 不是 G）。若引入 pprof 必须 loopback/unix socket + 开关 + 默认关。落 R13 |
| **OQ-6**（sim broker 主机不跑 colocated agent） | **供给机器缺口**（铁律③里 **sim 的活**），不是产品缺陷也不是宿主限制 ⇒ R9 补供给 | 与 P3 并存不矛盾：产品**必须**支持无同机 agent（P3），sim **也应**能测 whole-host 双到版路径（OQ-6） |

---

## 3. 仪器契约（R1/R2 一次性定形，此后冻结）

- **`expected-verdicts.tsv`**：37 行 × 期望 verdict × 四计数器 × 每个非绿格的 owner × 归属批号。
  验收用三条规则（**不是**逐格精确相等——24 处 warn 是级联守卫，产品修好一半时会双向跃迁，逐格相等不可满足、压力下必被放宽）：
  - **N1 无主非绿**：任一非绿格必须能定位 owner + 归属批号。无主非绿 = 该批失败。
  - **N2 意外变绿必须解释**：书面二选一 —— (甲) 上游 fixture 终于建立、下游臂第一次真跑并通过（附 pass 增量证据）；(乙) 空绿（触发全套 + 非恒真复证）。**默认假设 (乙)**。
  - **N3 归属批只许前移不许后推**（防债务滚雪球到收官）。
  - ledger 是**评审工件**；`run-drills.sh` 的 exit code **绝不读它**，runner 始终对任何非 GREEN fail-closed。
- **`not_covered` 加 class 参数**：`class ∈ {gap, runtime-guard}`。终态要求 `gap`==0；`runtime-guard` 在两轮判定运行中**一次都不得触发**（触发即该轮不计入判定轮，并成为 R14 工作项）。
  —— 明确驳回「`not_covered` 计数扁平归零」：26 处中过半是**运行时诚实守卫**，归零的唯一途径是删掉它们，那比 waiver 更危险（不留痕）。
- **反注水闸四层**：A `kept_sites` 逐 drill 不下降（对倒置翻转中性、对删臂致命）· B 谓词方向 diff 逐条标 `strengthen|equal|weaken`，**任何 weaken 一律驳回** · **C 非恒真证明（全工程通则）**：每一条被改动/新增的断言必须用一次刻意的坏输入证明它**能红**，证据落盘 · D INVERTED 配对 lint。
- **waiver**：`ALLOW_PRODUCT_RED`/`ALLOW_INCOMPLETE` **保留但禁用**（物理删除会让中期各批 runner 必然非零退出、反而诱发临时加回）；使用时打印 `WAIVER-USED`；**任何批次的验收都不得使用**。

---

## 4. 批次序列

> 每批：plan（对抗草拟 → 主进程定稿）→ 实现（可用 subagent 提速）→ 内审（对抗 workflow）→ 主进程采纳修改。
> **外审只在 R15 之后统一一次。**

### 阶段一 · 仪器与基质（零产品逻辑改动）

| 批 | 覆盖 | 出口要点 | 验证 | 规模 |
|---|---|---|---|---|
| **R1** 仪器地基 | H5（`poll_until` 栈式局部化，8 处嵌套含 `wait_phase`）· **per-drill `timeout`（当前完全缺失）** · H15 · H12 · H13 通用部分 · H16 · `not_covered` 加 class · `kept_sites` + `--verify-ledger` · INVERTED 配对 lint · lint BATCH 16→**37** · `WAIVER-USED` | 构造用例证明 poll_until 模式 A/B 均消除；契约测试覆盖 class 缺失必红 / `kept_sites` 下降必红 / N1-N3 正反两向；BATCH=37 下暴露的违规**就地修正**（不得缩回 BATCH 消红）；rollup 落盘可 parse 37 行；**`git diff` 不得触及 `internal/ cmd/ scripts/`**；**ledger 初值由批末两轮全套取交集**建立 | hermetic + **全套 ×2**（唯一建基线） | L |
| **R2** 记账诚实化（H4） | 24 处 warn → `not_covered(class)` · 80/81/82 三处裸倒置补 `product_red`(#25/#26/#27) · `62:118` 描述串正名 + 另加 gap 类登记 OQ-2 · 26 处既有 not_covered 标 class · ledger 写入 owner+归属批 | `grep -c 'warn.*NOT-COVERED'`==0；80/81/82 **必须落 PRODUCT-RED**；**红区扩大的每一格必须在 plan 里事先枚举**（本批是纯记账转换，可事前枚举），未枚举的新非绿=失败；`kept_sites` 不降 | hermetic + 全套 ×1 | L |
| **R3** install.sh heredoc（P9） | 11 处 heredoc 逐处判定 + `tests/lint-install.sh` 永久门 | 文件级证据：install.sh 写出的内容与源文本**逐字节一致**；断言容器内 install.sh 指纹 == 仓库指纹（防"改了没生效"）；6 个受污染 drill 此时 oracle 未修 ⇒ 一律标 `deferred-attribution`，不做 verdict 级归因 | lint-install + `make test` + 全套 ×1 | S |

**为何 R3 这么早**：P9 是**地基污染源**（安装时以 root 做命令替换），效果等同 oracle 缺陷；R6 的定案实验对证据纯度依赖最强，**不能在一条仍会以 root 做命令替换的 install.sh 上做证伪实验**。

### 阶段二 · Oracle 修复（零产品逻辑改动，唯一例外 P8）

| 批 | 覆盖 | 出口要点 | 验证 | 规模 |
|---|---|---|---|---|
| **R4** Oracle-升级面 | H1 · H3 · **P8**（json tag + schema_version bump） | H1↔P8 **强制同批**（H1 修读取端、P8 改产出端，分批会修两次并留虚假绿窗口——全方案唯一允许产品改动混入 oracle 批的破例）；dry-run 版本断言过非恒真证明；`_do_roll` 判 rc + roll.log 落盘 + 终末叙事由 roll.log 推导；**真 roll 路径的非恒真证明延后到 R7**（#31 未修前真 roll 跑不起来） | `make test`+`make lint`+单跑 30 | M |
| **R5** Oracle-混沌/HA/观测 | H6 · H14 · H7 · H8 · H9 · H10 · H11 · H13 | H8 正解是**修 fixture 补 `--ack-alerts`，不是拆门**（`gateDestructive` 是设计如此）⇒ 翻成 `assert_refuses` 带签名的 KEPT invariant；96 的 F0c 真实实现且与 F4 不同谓词；40/42 从 SETUP-RED 转为**有内容的**红或绿；新暴露的红逐条挂 owner，无主=失败。**H2 不在本批**（其唯一合规修法是改调 `restore --config`，那是 R10 的 P2；此时修只能手写更漂亮的 seam = 铁律②④禁止的代劳） | 单跑 40/42/71/74/93/96 | L |

### 阶段三 · 定案（只取证、不改实现）

| 批 | 覆盖 | 出口要点 | 验证 | 规模 |
|---|---|---|---|---|
| **R6** 定案实验 | Q1（同采 `nats.conf` issuer + `broker.err` 区分 A/B/C）· Q2（证伪预测：brk1 journal 有无 `authcallout: handle failed`）· 候选定案 #33/#34/#35/#36/#42/#46/#49/#59/#63/**#65**（≥10 轮，须在 H6 修好后的干净地面真相上做）· 95-D 假缺口 · `96:240` arm B · **设计如此判定表** · **结构性缺口三判据裁决**（含 OQ-2 可行性实测）· `coverage-boundary.md` 初稿 | 每条产出三元组【假说→可证伪预测→实测证据→裁定(CONFIRMED/REFUTED/AS-DESIGNED)→归属批】；**禁止"可能/疑似"**；AS-DESIGNED 项改用 `assert_refuses` **正面钉住那道拒绝**（不是绕过/删除/放宽）；**#65 若 CONFIRMED ⇒ 触发 R8x 并预声明"未闭合则不得宣布全绿"**；**OQ-2 的 T3 四项当批填满或明确填不满**（不许拖到 R15）；**零产品逻辑改动** | 针对性单跑 40/52/62/95/96 | M |

### 阶段四 · 架构根因

| 批 | 覆盖 | 出口要点 | 验证 | 规模 |
|---|---|---|---|---|
| **R7** 周期对账注册表 | #58/P10 · #31 · P3 锁面（lease+TTL） · #45 · #47 · #49 | 接口元组必须是 `(name, interval, leaderOnly, lastTick, fn)`——实测主循环已有三种异构形态 + 独立 `gcTicker`，扁平单 interval 表达不了现状；**`lastTick` 一次到位**（R13 只消费不改机制）。**一票否决不变量**：每个 pass 只允许"读期望态→比对→调**既有幂等**命令路径"，**禁止新建策略**；三条幂等 hermetic 单测；**冻结判据是"用注册表重写现有 3 个 call site + gcTicker 且行为等价（假时钟证明）"**，不是"审一遍"；CLI 在收敛未达成时不得返回 rc=0；**翻正同批**含 `30:158/30:165` 两条倒置 assert_ok（#31 一修好即为假，不同批必自伤） | `make test`+`-race`+泄漏门+`make e2e` → **全套 ×1** | XL（预设拆 R7a/R7b 阀门） |
| **R8** 主动投递 | P1 · #48 · P3 语义面 · #57 · #33（按 R6） · `96:240` arm C | **不变量**：home/roster 变更的投递**不以对端产生任何事件为前提**——判据 = agent 侧完全静默（不重连/不重启/不发命令）下，drain 之后数据面必须在有界时间内跟随；投递通道注册为 R7 的一个 pass（append-only）；drain/retire/upgrade 三动词各有"rc 语义"断言（控制面写成功但数据面未收敛 ⇒ 非零） | `make test`+`-race`+`make e2e` → 单跑 30/40/43/71/73/74 | L |
| ~~**R8x**~~ 【条件批·**未触发**】 | **R6 已判 #65 = REFUTED，本批取消** —— 台账「6 轮中 5 次持久」的成因是**归属错误**（`--nats-url` 只决定 ctl 连哪台入口服务器，不决定哪台 broker 处理请求；分区期直读三台 SQLite 为 `brk1=no, brk2=yes, brk3=yes`，与「少数派 stale-leader 写」预测正好相反），不在 raft 层。**工程不再有 raft-safety 级阻断项。** 详见 `docs/reviews/r6-findings.md` | — |

### 阶段五 · 单点产品面

| 批 | 覆盖 | 出口要点 | 规模 |
|---|---|---|---|
| **R9** 升级与集群生命周期 | P3 收尾 · #28 · #47 · #49 · #37 · #35/#36/#46（按 R6） · **G5 净覆盖为零** · whole-host 双到版判据 · **OQ-6 供给** · rejoin/resnapshot 整段 | G5 每一跳版本推进 + 服务连续性 + 失败回滚均断言；无 colocated agent 时的 broker-only 语义单独断言；#47 须"有界时间内离开 CATCHING_UP 或以明确错误终止"（不得以"最终会好"放宽）；42 的 rejoin/resnapshot **首次端到端跑通并绿**；**force-single 人工确认不得取消**，用 `assert_refuses` 钉住 | L |
| **R10** DR/备份恢复 | P2 · **H2（在 P2 之后翻正：51 弃手写 seam 改调产品动词）** · P4 · P5 · #52 · #53 · D2 | **【最高价值出口】51 的 DR 尾段（#52→#53→terminus）一次不落端到端跑通并全绿**——全工程唯一"至今从未被端到端证明过一次"的路径；断言必须**从订正后的 runbook 逐字执行**，不得由 drill 代劳 runbook 缺失的步骤（铁律②③④）；P5 是"守门人说谎"类，出口覆盖"库不存在/空文件/截断/目录/权限拒绝"五态，**并回扫所有把 doctor 当 `assert_setup` 用的 drill**；#53 二选一（bundle 含 JS 或不含但**明确告警**），静默丢 history/audit 不可接受 | L |
| **R11** 身份与凭据 | P6/#54（facet 1+2 可见性/fail-closed）· ~~#55 switch-over 动词~~（R6 撤销，并入 #54）· Q1 结论落地（A/B/C 全 REFUTED，产品无需修）· P11/#56 · P12/DOC-23 · #63（源码立机理 + R14 残留）· D1 台账订正 | **#55 不加动词**（R6：重启即换 issuer ⇒ 动词多余；真实问题是 #54 可见性，已修 doctor+reconcile skew 检查）；P11/P12 验收标准是"按产品指引逐字执行能真的恢复"，不是"文案读起来合理"——#56 self-only 动词旁路通用 leader-redirect、DOC-23 文案指向 FILE-level 恢复；#63 从源码立住 re-pin 机理（register 回包携带轮换后 pin）+ 加测试，active-push 残留归 R14；**改 auth_callout 本地 auth_callout 模式真跑通**（memory 纪律） | **产品侧已交付（2026-07-19）** |
| **R12** 接入/会话/安全 | P7/#25 · #26 · #27 · D4 · #30 operator reader · **webhook wire 契约** | 限速断言两向都要（超阈被拒 + 未超阈不误伤），覆盖单 IP 高频/多 IP 分散/代理后（`client_info.host` 可信边界须论证）；#26 断言子进程在**宿主进程表**中确已消失；webhook 三层逐字段 + **一条否定断言**（畸形/伪造载荷被拒），不得只断 HTTP 200；**80/81/82 必须全部落 GREEN**——翻法是**重写谓词**（80→`assert_refuses <rate-limit-sig>`、81→"子进程必须已回收"、82→"listener 必须已 bound"），**不是换关键字** | M |
| **R13** 可观测性与可测性能力 | **runtime 自省能力** · #39 · #42 · D6（`ps` LOST overclaim 清偿） · D3 · 93 收尾 · 95-D | 97 的 goroutine 门谓词写死为「注入负载→quiesce→回落到 pre-load 基线 ± tolerance」，**容差/采样点数/判定窗口在 plan 阶段即为契约**；94 的 `ps` LOST 补真断言或**收回声明并同步 README**；95-D 若判假缺口，处置方向是**收紧谓词到正确语义**并新增负向臂证明新谓词能红 | L |

### 阶段六 · 收口

| 批 | 覆盖 | 出口要点 | 规模 |
|---|---|---|---|
| **R14** 非确定性消除 + Q3/Q4 + 无主残余 | runtime-guard 逐处确定性化（`96:280` loopback 抢跑 · `97:243/252/273` · `95:232/251` · `91:63`/`51:431` grow flake · `22:130` 等）· Q3 · Q4 · R6 "证据不足"项复议 · R9/R10 新覆盖面暴露的无主新缺陷 | 每处 runtime-guard 要么确定性化、要么**证明不可确定性化并按三判据重新裁决**（不得默认保留）；`class=gap` 计数==0；`assert_bug`==0、`product_red`==0、`warn NOT-COVERED`==0；ledger 无无主非绿 | L |
| **R15** 收官 | D1 · D3 · D5 · DOC-28 · `coverage-boundary.md` + out-of-suite ledger 定稿 · OQ-2 最终形态 · 终态 lint 全生效 | G-1…G-10 逐条留证；**本批禁止修任何新发现的缺陷**（新发现回退 R14——收官批不能同时是冻结批和兜底批） | M |

---

### R6 定案对后续批次的输入订正（2026-07-19，**必读，否则后续批会照过时输入干活**）

详见 `docs/reviews/r6-findings.md`。对 R7–R14 工作内容有实质改变的六条：

| 条目 | 原计划假设 | R6 定案后的真实情况 | 受影响批 |
|---|---|---|---|
| **#65** | 可能 CONFIRMED ⇒ 触发 R8x、未闭合不得宣布全绿 | **REFUTED**（归属错误：`--nats-url` 只选入口服务器不选处理者）⇒ **R8x 取消，无 raft-safety 阻断** | R8x 删除 |
| **#45** | 停滞在 rehome/migrate | **停滞在拓扑收敛门**（无计数器/deadline/watchdog）；且 **N=2→1 是 BY DESIGN**（drill 41 断言其 PASS）⇒ **只修无界门那半，不得动两阶段边界** | **R7** |
| **#49** | 需实现 preflight 与 FSM 一致 | **ALREADY-FIXED**（`previewRecoveredRoster` 已在副本 DB + 克隆 raft 树上真跑 recovery 后拒绝，已有 RED→GREEN 单测）⇒ **只需 drill 42 复验** | **R7/R9** |
| **#33** | 根因未归因、仅观测 | **根因已归因**：`agent/proxy.go:152-156` rehome 成功分支不恢复 `proxyTunnelUp`，两个写者对 `proxy_ready` 竞态 ⇒ **抖动的隧道自愈、稳定的永不自愈** | **R8** |
| **#48** | agent 被退役 broker 的 stale VOTER roster「投毒」 | 后果确认，但**机理是「被饿着」不是「被投毒」**（退役 broker 什么都不答）；且「DB 显示 ONLINE」**本身是归属错误**（`nodes.status` 从不复制，ONLINE 恰证明应答来自那台退役 broker 自己） | **R8** |
| **#55** | sim 结构上不可构造 ⇒ 走 (a) 加原子 switch-over 动词 | **前提为假**：带新 seed 重启即在 20s 内换 issuer，**只重启一台就构造出 #55 的确切 skew** ⇒ **可构造，R11 重开**；且因 auth_callout 是跨 broker 队列组，后果比 per-broker skew 更糟（~1/N 授权违规掷硬币） | **R11**（§2 的 (a) 裁决据此重开） |

另外三条**从台账撤销/降级**：`#35`（REFUTED as stated，只剩条件句）· `#59`（REFUTED，只读存活是设计态 `proxyStateFrozenReadonly`）· `#63`（REFUTED，3/3 实跑 A7d PASS）。
Q1 的 A/B/C **全部 REFUTED**——台账「account.nk 轮换后无法就地恢复」是 drill 健康门
（把**容错度**当**存活性**编码，任何 N=2 都必 exit 1）造成的**对产品的错误指控**。

**流程订正**：各批出口断言里的「`git diff --stat internal/ cmd/ scripts/` 为空」应读作
「**本批新增**的 diff 为空」——工作树在多批次累积下永远不可能为空。

---

## 5. 「先变红」的中间态

| 阶段 | ledger 允许方向 | 验收标准 |
|---|---|---|
| **红峰值 #1（R2 后）** | **唯一允许扩大** | 每一格扩大**必须事先枚举**（H4 是纯记账转换、不解锁任何被短路的执行路径，故可事前枚举）。红债归属写死：#25/#26/#27→R12 · 74×10→R8/R9 · 71×4→R8 · 30×3→R7/R9 · 73×3→R8 · 82×2→R12 · 31×1→R9 · 62×1→R15 |
| **红峰值 #2（R4/R5 后）** | 允许扩大，**不要求逐格枚举**（H1 空绿散去、H8 解锁 rejoin、H11 解锁 74 Arm C 后的红是路径依赖的，事前不可知） | 用 **N1 无主非绿**：新非绿必须挂到已登记 owner 或当批新开编号 |
| **中立（R3, R6）** | 不许有语义变化 | R3 标 `deferred-attribution`；R6 的观测改动走独立提交、ledger 在其之前的构建上比对 |
| **单调收缩（R7 起）** | **只许缩小** | 任何 GREEN→非 GREEN 一律判回归、硬失败。新写的断言首次为红**不算变红**。**N2**：意外变绿必须书面二选一，默认假设空绿 |

---

## 6. 验证成本：全套 36 min 只在 6 个时点跑（共 8 轮 ≈ 4.8 h）

**默认每批只跑**：hermetic 腿（`make test`/`make e2e`/`make lint`）+ 本批 touched drill 单跑。

**跑全套的触发条件（仅四条）**：(i) 改了 `lib/*.sh`/`run-drills.sh`/`lint-drills.sh` 语义 → R1(×2)、R2 ·
(ii) 产品改动落在全局地基 → R3、R7、R8x · (iii) 终局 → R15(×2，不同 `-j` 档) ·
(iv) 任何批出现**无主的变绿**（N2 的乙分支）→ 临时加跑一次定位。

**通用铁律**：任何触及 Go 代码的批，**hermetic 两层必须在跑任何 drill 之前跑完并全绿**。
drill 比 hermetic 贵 20 倍——用 36 分钟的部署层测试去抓一个 Go 层空指针，是本工程最容易犯也最贵的错误。

---

## 7. 主要风险

| 风险 | 缓解 |
|---|---|
| **R-a** R7 把一次性破坏动作周期化 ⇒ 每 30s 重复破坏一次（比原缺陷严重一个数量级） | 一票否决不变量 + 三条幂等 hermetic 单测 + leadership-gate 逐 pass 显式声明 + `-race` + 泄漏门 + 批末全套 |
| **R-b** R7 接口冻结不当 ⇒ R8/R9/R13 四批返工（最大单点） | 冻结判据是"**重写**现有 3 个 call site + `gcTicker` 且行为等价（假时钟证明）" + #31 作第三形态样本 + `lastTick` 一次到位 |
| **R-c** R1 的 ledger 基线写错 ⇒ 后续每批建在错地基上 | 双轮取交集 + `unstable` 标记 + 内审专家**从 drill 源码独立重算 37 行**并逐行比对 |
| **R-d** R9/R10 新覆盖面几乎必然产出新缺陷 ⇒ 范围失控 | 新缺陷登记后一律进 R14，禁止就地扩大当前批；R15 禁止修任何新缺陷 |
| **R-e** (a) 类加能力滑坡成测试后门 | T1+T2+红线三禁写进 R11/R13 plan 并逐条书面论证；内审设显式审查项"这个能力真实运维者会不会用"。目前通过的只有 #55 switch-over 与 97 runtime gauge 两项 |
| **R-f** install.sh 经 `remote.sh` re-vendor 滞后 ⇒ R3 改了不生效 | R3 第一条验证步骤即断言容器内 install.sh 指纹 == 仓库指纹 |
| **R-g** 铁律④滑坡（drill 越写越复杂才能让操作"成功"） | 每批内审固定检查项：**本批新增 drill 行数是否超过被删除的 GAP 绕过行数？** 超过必须解释 |
| **R-h** 外审只在末尾一次，累积偏差最后才被检验 | 四份可独立复核的书面产物：R6 判定表 · ledger 逐批留档（完整红绿涨落审计轨迹）· `coverage-boundary.md` + out-of-suite ledger · G-8 的 `kept_sites` 对照表 |

---

## 8. 被驳回的方案（防止执行期重新滑回）

`min_pass` 单调不减（pass 不是断言数，R2 必自伤）· 62 内断言一份落盘 GREEN 记录（伪造成本为零）·
"多红一行少红一行都算失败"用于 R4/R5（逻辑上不可满足）· "无法归因的新红一律当 harness bug 修掉"（H10 是反例，制造 toy 化压力）·
#55 按 Q1 结论分流是否建造（按省事程度分流）· H2 先行于 P2（物理不可能）· 物理删除 waiver 标志（诱发临时加回）·
`not_covered` 扁平归零（逼人删诚实守卫）· "infra-flake 重跑次数为 0"（把终态门押在宿主属性上）·
"37 套件内所有 INVERTED 都是假绿"（实测证伪，只有 80/81/82 裸露）· weilandserver 是共享宿主（实为专用机，须实测）·
匿名 buffer 批位（必成垃圾桶）· "翻正=换 assert 关键字"（对倒置 assert_ok 不成立，须重写谓词）

逐条理由见 `docs/reviews/allgreen-roadmap-synthesis.md` §10。
