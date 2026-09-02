# 测试编写规范（含安全编写规范）

> 本文的每一条都来自**真实踩过的坑**，并注明出处。它不是通用最佳实践清单——
> 通用清单谁都会写、谁也不会读。这里只收录**在本仓库造成过假绿灯、假保证或安全回归**的形态。
>
> 适用范围：所有 `_test.go`、所有门禁（gate）脚本、`test/simcluster/` 的 drill 断言。
> 与 `CLAUDE.md §5` 的关系：§5 讲**跑哪些测试**，本文讲**怎么写测试**。

---

## 零、层次、目录与执行形式

### 0.1 层次是测试的属性，目录只是它的地址

| 层 | 谓词（判断归属时问的问题） | 位置 | 跑它的命令 |
|---|---|---|---|
| L0 gate | 只读源码树/配置，不实例化产品运行时 | `test/architecture`、`test/determinism`、CLAUDE.md §5 闸门表点名的包 | `make gates` |
| L1 unit | 同包；不起嵌入式 nats-server、不建 raft 节点、不 exec 子进程 | `internal/<pkg>`、`cmd/tether` | `go test ./internal/<pkg>/` |
| L2 component | 同包；起嵌入式 NATS 或 raft 节点 | 同上 | 同上 |
| L3 integration | `test/<dir>` 外部包，进程内真 broker+agent，无 build tag | `test/p*`、`test/d3`、`test/d4`、`test/security`、`test/chaos`、`test/cli_e2e`、`test/cluster`、`test/storage`、`test/proxydial`、`test/concurrency` | `make test` |
| L4 tagged integration | `<x>_integration` build tag，只在矩阵跑 | `test/d5..d9` | `make e2e-one T=TestD<N>Matrix` |
| L5 matrix | 唯一允许 exec `go test` 的地方 | `test/e2e` + `test/e2e/parallel` | `make e2e-parallel` |
| L6 deploy-tier | bash，真 Docker + systemd，不进 go test | `test/simcluster/drills` | `./local.sh drill <name>`（按需）；hermetic 自检集 `test/simcluster/tests/run-all.sh` |

同一个 `internal/broker` 里 `adaptive_catchup_test.go` 是 L1、`reply_egress_test.go` 是 L2——**层次不由目录决定**。
教科书体系对 "component" 的定义互相矛盾（ISTQB 把它等同 unit；Fowler 把它放在 integration 之上），
所以本仓**不背标签**：每个测试包的包头写三行——**起了什么真的 / 打桩了什么 / 明确不覆盖什么**
（`test/p6/expose_e2e_test.go` 包头是范本）。目录 ↔ 层次 ↔ tag ↔ 矩阵的地图在 `test/README.md`。

### 0.2 目录与文件名

- 测试文件、测试函数按**被测单元**命名（CLAUDE.md §3 step 5b）；溯源写 `// origin: …` 一行。
- **目录**同样不按开发过程事件命名。存量的 `test/p*`、`test/d*` 是矩阵契约的一部分（`all_phases_test.go` 字面量、
  `shard.go`、账本路径键），**冻结不迁**：不新增、不删除（精确集合），新目录一律主题命名。
- 文件首注释若写了文件名（`// x_test.go — …`），它必须等于 basename。改名时顺手改头，否则下一个人读到的是一个不存在的文件。

### 0.3 每条规范的执行形式（闸门 / 靠人读）

| 条 | 执行形式 | 为什么是这个形式 |
|---|---|---|
| T1 | 靠人读；就绪 helper 里的裸 sleep 屏障由 `test/determinism/sleep_barrier_test.go` 冻结（账本 19，`// sleep-fixture:` 豁免） | "超时是否按实际环境写"无法机械判定；它最常见的机械子集可以 |
| T2 | 闸门 `test/determinism/raft_timing_guard_test.go` | 复合字面量与赋值两种语法形式同一计数器 |
| T3 | 闸门 `test/determinism/leader_premise_test.go`（裸 `.IsLeader()` / `.State() == raft.Leader` 读——不在**身份已证明**的轮询 helper 谓词闭包里（外部原语按 import 路径、本包 helper 由 `verifiedPollingHelpers` 从实现证明）、不是 body 只等待的循环条件——进 `path: func` 递减账本）+ helper `test/clusterharness.WithLeader`（观测→行动→再观测，移动即重来；它证明的只是前后两次读数，不排除 A→B→A，不撤销副作用） | 站点形状可机械判定；"读了之后有没有假设它不变"不能，所以账本冻结存量、门只拦新增 |
| T4 / T5 | 靠人读 | 是诊断方法与行为侧修法，不是断言 |
| T6 | 闸门 `test/determinism/leak_assert_shape_test.go`：断言形状（≥5 轮 exercise）+ 覆盖面（`TestLeakGateCoversRiskyPackages`：非测试文件 import raft/yamux/tunnel/pty 的包，必须有 `Test*` 函数体内直接调用共享泄漏 helper）+ `-race` 归属（`test/e2e/parallel/inventory_test.go`：这些包必须被某个 -race 矩阵**整包**跑到，`-run` 子集不算） | 三个问题三道门：断言够不够狠、哪些包必须有、有了是否真在 -race 下跑 |
| T7 | 闸门 `test/determinism/raft_timing_guard_test.go`（`TestProductTimingSleepsReferenceTheConstant`，同包形态）；跨包形态靠人读 + `broker.LeaseGrantWindow()` 访问器 | 跨包测试引用不到未导出常量，所以它们的字面量只能靠导出访问器消除 |
| G1 | 变异验证本身靠人读；**指向控制测试的锚点**由 `test/architecture/gate_standards_test.go` 强制（`// gate-control: TestXxx` 必须指向同文件的正负控制；账本 13） | 变异验证是一次性动作，但"哪个测试是这道门的控制"必须是树上一行可核对的指针 |
| G2 / G2b / G4 / G5 / G6 | 靠人读 | 写门时的设计判断 |
| G3 | 键形状登记由 `gate_standards_test.go` 强制（每本 `legacy*` 账本登记 file / site / promise）；豁免理由靠人读 | 文件级键会盖住该文件未来的全部站点 |
| G7 | 闸门 `test/architecture/fuzz_corpus_test.go`（语料预算与文件头）；语料机制本身靠人读 | 提交过多语料是"深思熟虑的拷贝"唯一的失败模式 |
| A1 / A2 / A3 / A5 | 靠人读 | 断言语义 |
| A4 | 闸门 `test/determinism/promised_guard_test.go` | 注释里点名的 Test 必须存在（README 表不在扫描面——`test/determinism/README.md` 曾因此留了一行死引用） |
| S1–S5 | 靠人读；S3 的一半由 `cmd/tether` 的 wire 错误码 ↔ exit class 门守 | 安全语义 |
| §六 R1 | 闸门 `test/determinism/test_inventory_test.go` | 删/改名测试必须手改 golden |

这张表的价值在**执行形式那一列的真实性**：带闸门的行是少数（数它一次就会腐化一次——这句话第一版写着「只有 4 条」，
落地当天就已经不对）。每次给某条装上门，来这里改一行——一条"已有闸门"的规范如果表里写着靠人读，读者会以为它没人守；
反过来，2026-09-01 内审发现 T3 与 T6 的门都已落地而这张表还写着「靠人读」（L5-F3 / L6-F6），是同一个错误的另一面。

---

## 一、时间、并发与分布式前提

### T1 · 超时必须按测试**实际运行的环境**写，不是按理想环境

`-race` 让每次内存访问慢 **5–10 倍**；`make e2e-parallel` 下还有 20 个 worker 同时抢内存带宽、
页缓存与调度器。一个在空闲机器上"够用"的超时，在这两者叠加时是**必然的 flake**。

> **案例**：`test/d7` 用 60ms 的 raft 心跳超时，而该套件按 §5 必须带 `-race`。
> 一次 GC 暂停就足以在 `AddNode` 提交途中掀翻 leader，约 1/7 失败。

### T2 · raft / 选举 / 租约超时**必须引用生产常量**，不得自编数字

```go
// 对
HeartbeatTimeout:   cluster.MultinodeHeartbeatTimeout,
ElectionTimeout:    cluster.MultinodeElectionTimeout,
LeaderLeaseTimeout: cluster.MultinodeLeaderLeaseTimeout,

// 错
HeartbeatTimeout:   50 * time.Millisecond,   // 比生产短 20 倍
LeaderLeaseTimeout: 25 * time.Millisecond,   // leader 25ms 拿不到多数派 ack 就下台
```

自编的数字有两个问题：它**测的不是生产会发生的事**，而且它会漂移——生产常量改了，
测试不会跟着改，于是测试继续验证一个已经不存在的配置。

> **案例**：全仓 17 处测试硬编码 50–150ms，只有 1 处对齐生产的 1000ms。
> 三个满载 flake（d3 / broker / d7）全部命中这个模式。
> **注意**：把 60ms 调到 300ms 也不算修好——那只是"调到 `-race` 刚好能过"，
> 仍然不是生产会发生的事。外审 M4 正是这么指出的。

**例外有两类**，都必须**显式覆盖 + 写明理由 + 进门禁豁免表**：

1. **断言依赖快速故障检测**（如"leader 掉线后应在 X 内换主"）。所选值仍须容纳 `-race` + 满载的最坏情况。
2. **配置的非法性本身就是 fixture** —— 测试要的就是"这个组合会被拒绝"。

> **案例（第 2 类，批量替换时踩到的）**：`TestD3FailingNewReapsTransportNoLeak` 用
> `LeaderLeaseTimeout(2s) > HeartbeatTimeout(1s)` 这个**非法排序**让 `raft.ValidateConfig`
> 在 `BootstrapCluster` 内部拒绝，从而使 `New` 在传输已建立**之后**失败——那条失败路径正是
> 该测试要验证会被回收的东西。换成生产常量（合法排序）后 `New` 成功，
> **测试就什么都不断言了**。
>
> 这条教训有两面：批量对齐**会**破坏这类反例，而它**被这个测试自己抓住了**——
> 一个断言"必须失败"的测试，在断言失效时会立刻变红。这正是好 fixture 的样子。

### T3 · 不得假设"刚观测到的分布式状态在下一步仍然成立"

```go
// 错：观测与使用之间，领导权可能已经转移
if n.IsLeader() { leaderIdx = i }
followerIdx := 1 - leaderIdx
nodes[followerIdx].Propose(...)      // 它现在可能是 leader 了
```

前提失效时，正确做法是**重新建立前提**（有限次重试整个观测—操作序列），
**不是**放宽断言让测试通过。二者的区别是：前者让测试只在前提成立时断言，
后者让测试在任何情况下都不失败——后者等于删掉了测试。

> **案例**：`TestD3FollowerPINWriteStaleReplicaReturnsNotLeader_Review` 期望 follower 返回
> `not_leader`，满载下实得 `session does not exist`——因为领导权已转移，
> 被测节点成了真 leader，于是真的执行了业务逻辑。

### T4 · 资源饥饿会**伪装成产品缺陷**——先确认被测进程拿到了多少机器

一个断言超时失败时，失败信息往往是产品语义的（"某标志没有清除"），
于是第一反应是"产品坏了"或"超时写小了"。但第三种可能常常才是真的：
**这个进程分到的机器不够**。

判断方法不是猜，是量：同一测试**隔离**跑若干次拿到耗时分布，
再与失败时的实际耗时对比。差一个数量级就不是调度抖动。

> **案例**：`TestProxyFalseOnlineRecoversAfterTunnelDrop` 在满载并行下失败，
> 报的是 `proxy_ready did not clear on tunnel drop (Defect B: false-online persists)`
> ——读起来完全像产品回归。隔离跑 8 次：**0.81–1.38s**，方差极小；
> 失败那次等满 **15s** 仍未清除。
>
> 真因在失败行里：`FAIL TestAllPhases (worker 6, cpus 12,13,56,57)`——
> `TestAllPhases` 只分到 **2 个物理核**，而它内部要串行跑 11 个 phase 子进程，
> 每个都是带 `-race` 的二进制加嵌入式 NATS。跑全量串行时同一个 phase 独享整机。
> **工作没有变慢，是机器变少了。**
>
> 修复是给不可拆分的重单元一个**宽 worker**（`reserveHeavyWorker`），
> 不是把 15s 改成 30s——注释里"15s（原 5s）"那行本身就是上一次这么改的痕迹。

**怀疑 flake 时的 ad hoc 猎捕**（不进任何闸门，按需跑）：

```sh
make e2e-parallel E2EPAR_FLAGS="-repeat 3 -shuffle=on"   # 全矩阵三轮 + 打乱包内测试顺序；seed 在每个单元的输出里
go test -count=20 -race -run 'TestXxx' ./test/pN/         # 单个可疑测试隔离复跑，拿耗时分布再与失败那次比
```

`-shuffle` 由 runner 透传给每次 `go test`（`test/e2e/parallel/main.go`）：可解析单元直接作为 `go test` 旗；
whole 矩阵单元（`TestAllPhases` 等自己再 fork `go test` 的五个）经 `GOFLAGS=-shuffle=…` 传给它们的子进程——
第一版只加旗，对这五个单元（含 11 个 phase 套件）是空操作（内审 L4-F5）。seed 出现在**内层**输出里，只在失败时可见。
它暴露的是**测试之间的顺序依赖**——一类 `-race` 与泄漏门都看不见的假绿。

### T5 · "探测一个空闲端口再使用"是 TOCTOU，必须容忍它被抢走

`findFreePort()` 这类 helper 会绑定、读端口号、关闭、返回数字。**返回到使用之间，
机器上任何进程都可能占用它。** 单测试串行时这是理论竞态，20 个 worker 并行时是常态。

正确做法是**换一个端口重试**，不是加长超时——端口被别人拿走，等多久都不会还回来。

> **案例**：`TestTunnelReconnectTransientDenyRetries` 在满载下 **0.17s** 就失败，报的是
> `broker denied REGISTER: public_port_bind_failed`，而失败行是 `newReconnectHarness`
> ——**harness 还没搭好**。读起来像"隧道重连坏了"，实际与重连毫无关系。
> 失败得快是关键线索：断言预算是 3s/5s，0.17s 说明是硬错误而非超时。

### T6 · 并发测试必须带 `-race` + 仓库内建泄漏门

见 `CLAUDE.md §5`。另注意：**串行调用的测试即使加了 `-race` 也证明不了并发安全**。

> **案例**：`loopSet.done` 在锁外初始化，4 个测试全部串行调 `Go`，
> `-race` 全绿是**假阴性**。补了并发回归测试才暴露。

### T7 · 围绕**产品时间常量**的等待必须引用常量或走注入时钟，不得写字面量

`leaseGrantWindow`、`probeTTL`、`DefaultStaleAfter` 这类常量改一次，所有按旧值标定的
`time.Sleep(1100 * time.Millisecond)` 就同时变成在测另一条分支——而且**不会红**。

> **案例**：`leaseGrantWindow` 从 1s 改为 5s（cloned-credential round 2）之后，
> `test/p2/cloned_instance_e2e_test.go` 睡 1200ms 的测试仍自称「deliberately LONGER than leaseGrantWindow」，
> `internal/broker/lease_probe_staleness_test.go` 的注释仍写「1.1s later — past leaseGrantWindow」。
> 两处六周没人发现，因为测试照样绿——它们只是悄悄换成了在测"窗口内"那条分支。
> 当时钟是注入的（`broker.Config.Now`）就推进注入时钟；跨包拿不到常量就导出只读访问器；
> sleep 本身就是夹具时写一行 `// sleep-fixture: <理由>`。

---

## 二、门禁 / 扫描器类测试

这类测试的失效方式与普通测试不同：普通测试坏了会**变红**，门禁坏了会**变绿**。

### G1 · 必须做变异验证：注入缺陷，门禁必须变红

写完门禁后**立刻**手工注入一个它声称能抓的缺陷，确认它精确点名后再恢复。
没做过这一步的门禁，等同于没有门禁。

> **案例（差点重蹈覆辙）**：反向 ACL 对账的第一版**通过了变异测试**——即它是空转的。
> 原因是 `ctrl.by.*.>` 这类模板级通配授权吞掉了一切（正向对账排除了 `.>`，反向忘了）。
> 不做变异实验，就会写出第二个"宣称守护实则不守"的门禁。

### G2 · 必须有自检，防止退化为"匹配不到任何东西"

一个扫描器若因重构而匹配不到任何目标，会报出完美的 "0 unclassified codes"。
所以门禁要断言**自己看到的东西非空**，并对每种支持的形态合成一个样本。

> **案例**：`TestErrorCodeCoverageSelfCheck` 为 form 1–3、8 各合成一个样本；
> `TestACLExtractorsAreNotVacuous` 断言提取结果非空。
> **反例**：`make e2e-parallel` 的解析层没有覆盖自检时，静默丢掉 `TestAllPhases`
> 连带 11 个 phase 套件，跑得又快又绿又少了三分之一。

### G2b · 非空性地板**不得对"目标就是清空"的那棵树计数**

G2 要求门禁断言自己看到的东西非空。但有一类门禁的**成功状态就是"树上一个都不剩"**——
命名冻结、废弃 API 清零、豁免清单排空。对这类门禁按 G2 的字面做法加一条
`if len(found) == 0 { t.Fatal("扫描器瞎了") }`，会得到一条**在代码库正确时必然失败**的断言，
于是下一个人只能把它删掉或调低——门禁从此再无自检。

**判据（一句话）**：非空性地板可以对活树计数，**当且仅当**该门禁的成功状态里仍然含有它所计数的东西。

- 成功状态**保留**被计数对象（如"五个装配点都在，只是字段词汇必须一致"）⇒ 对活树计数**合法**。
- 成功状态**清空**被计数对象（如"过程命名文件为零"）⇒ 必须改为**对合成样本**做自检：
  手工造几个正样本 + 几个负样本喂给同一个判定函数，证明它认得出来；
  真树上的计数换成**方向性**断言（"不得新增"、"清单里的条目必须仍然存在"）。

> **案例（正）**：`internal/natsconf/assembly_parity_test.go` 的 `len(baseline) < 6 → Fatal`——
> 唯一装配点在成功状态下必然存在，所以数它是对的；数不到就是扫描器瞎了。
> **案例（反→已改正）**：`test/determinism/test_naming_test.go` 的命名冻结，成功状态是**零个**过程命名文件，
> 所以它的自检 `TestProcessNamePatternRecognisesTheRealShapes` 喂的是**合成文件名**（正例必须匹配、
> 反例必须不匹配），真树上只断言"不得新增"和"清单条目不得失效"。
> 同理 `test/determinism/leak_assert_shape_test.go` 的 `scanned < 8`：它数的是**泄漏断言**，
> 那些断言在成功状态下依然存在，所以合法——数的不是它要清空的那批东西。

**为什么值得单列一条**：这个坑只在"门禁做的是清理工作"时出现，而清理型门禁恰恰是最容易被
"照抄上一个门禁的地板"写坏的一类。B 批两个门禁各自在注释里独立想明白了一遍，
说明它不是自明的。

### G3 · 豁免精确到 **site**，且**必须带理由**

- 豁免的 key 是 `file:line`，不是 `file`——文件级豁免会盖住该文件里未来新增的所有问题。
- 每条豁免必须写清"为什么这里不是问题 / 在哪里被别的机制覆盖"。
- 没有理由的豁免清单只是"慢一点的没有门禁"。

> **案例**：`unresolved` 曾以文件名为 map key，**同文件的第二个动态 site 直接消失**。
> 收窄后立刻暴露 7 个从未被报告过的站点。

### G4 · 报告端不得用"数量"证明"集合"

`len(got) == len(want)` 看不见"一个被重复、另一个被丢弃"，也看不见"一个被替换成另一个"。
要比较 **identity multiset**。

> **案例**：并行 runner 丢失原始 `-run` 后，unit 数量 107 完全正确，
> 但 `TestTransferDefaultsMatrix` 声明 3 个测试却跑了整包——**数量对，跑的不是同一个东西**。

### G5 · 防御措施本身也要被测试

新加的守卫会走到**它自己没被覆盖的那条路径**上。

> **案例**：为防"少跑报绿"加的结果对账，读的是 `len(units)`，
> 而默认非 split 模式的 work 在 `tests` 里、`units` 恒空——
> **每一次成功的默认运行都被判为失败**，而且没人发现，因为 Makefile 总传 `-split`，
> 我只测了自己会走的那条路径。

### G6 · 门禁自身必须**可以拥有单元测试**

如果某个机制让"给这个包加测试"变得不可能，那不是限制，是缺陷。

> **案例**：覆盖自检用 `./test/e2e/...` 递归列测试，而 `go test -list` 不输出包归属，
> 于是 parallel 包自己的 `_test.go` 被当成串行 gate 的顶层测试而拒绝启动。
> 这被写进 plan §10 当作"限制"，真相是**限制是自己造的**。

### G7 · fuzz 语料：crasher 进 `testdata/fuzz`，interesting 语料留在 `$GOCACHE`

`go test -fuzz` 只把**导致失败**的输入写进 `testdata/fuzz/<Fuzz>/`；它自己造出来的 interesting 语料
（走到新分支的输入）存在 `$(go env GOCACHE)/fuzz/…`，**不在工作树里**。

- crasher：随修复一起提交，它就是永久回归用例，以后每次 `go test` 都回放。
- 精选种子：写进 `f.Add`，随代码走。
- 饱和语料（如 SOCKS5 那 23 条穷举了 REP 码 0x01–0x08 与 ATYP 1/3/4）：**要提交就得手动拷贝**
  `cp $(go env GOCACHE)/fuzz/github.com/LinZiyang666/tether/<pkg>/<Fuzz>/* <pkg>/testdata/fuzz/<Fuzz>/`；
  批量语料（几百条 JSON 变体）不提交。
- 该不该 fuzz 一段代码只问一句：**这段解析逻辑是自己写的，还是标准库写的？**
  `encoding/json` 已被全世界 fuzz 过；手写的逐字节解析器（对端可控输入）才是猎场。
- fuzz 证明的是"不崩"，不是"算对了"——错误码报错、分支写反，只要不 panic 它都看不见；那些仍靠手写断言。

> **案例**：三份独立草案都写了「提交 `testdata` 下生成的语料」，按字面执行得到零文件——机制写错了。

---

## 三、断言与证据

### A1 · 不用"没报错"证明"做对了"

要断言**可观测的终态**（行没了 / voter 数降了 / 文件被创建了），不是"调用返回 nil"。

### A2 · 不得伪造数据去迎合断言

> **案例**：`tokenhash_test.go` 里我先写了一个编造的摘要 `f5a1d1b1...`，
> 然后特判它去跟 `sha256.Sum256` 比较——**正是该文件头部注释明令禁止的做法**。
> 固定向量必须是真实计算出来的值。

### A3 · 到不了终态就**如实降级措辞**，不要把边界写在脚注、把结论写得更大

> **案例**：D7 retire 测试停在 `NATS_ROLLED_OUT`（`Terminal=false`），
> 却在标题和结论句里写成"真实三节点 retire **成功路径**"。
> 准确说法是"retire 的**不可逆步骤**已恢复真实三节点集成覆盖"。
> **伪造终态断言比停在中间更糟**——那会断言一个没发生的收敛。

### A4 · 注释里写了 `TestXxx`，该测试就必须存在

由元闸门 `test/determinism/promised_guard_test.go` 强制。

> **案例**：这条规则是在同一批次里**第 4 次**写出"承诺了但不存在的测试"之后加的。
> 它一装上就发现仓库里还有 **34 条**既存的失效引用——不是某次疏忽，是系统性模式。

### A5 · 覆盖率是"被执行"的证据，不是"被验证"的证据

一行被跑到 ≠ 那行的行为被断言。本仓**不测量、不发布覆盖率**，也不设阈值：数字一存在就变目标，
而它的反向激励是删注释、删防御分支（`.golangci.yml` 头部设计约束 1 记着 `maintidx` 的同类实测）。
审查时若想知道"改了的生产行有没有被任何测试执行到"，用一次性命令，不进 Makefile：

```sh
go test -coverprofile=/tmp/c.out ./internal/<pkg>/ && go tool cover -func=/tmp/c.out | grep -v '100.0%'
```

---

## 四、安全编写规范

### S1 · fail-closed：空值、缺失、无法解析，一律按"不安全"处理

```go
// 错：空 host 被判为 loopback
if host == "localhost" || host == "" { return true }

// 对：空 host 意味着"所有网卡"
if host == "" { return false }
```

> **案例**：合并三处 HTTP 监听时，我把 `return host == "localhost"` 读成冗余表达式、
> 简化成 `return true`。后果是 `sub_http_listen: ":8080"`（最自然的写法）
> 从"启动硬失败并解释原因"变成**静默绑 0.0.0.0**，
> 而 `/sub` 吐 subscriber token + PSK、manifest 吐 account-signed roster，都是明文 HTTP。
>
> 更值得记的是**为什么没抓到**：我的 `policy_test.go` 只做 AST 检查 bool 字面量，
> 而回归在**行为侧**；既有的行为测试只喂了 `0.0.0.0:0` / `8.8.8.8:80`，
> **没有一条覆盖空 host**。结构检查与行为检查互相替代不了。

### S2 · 安全默认值不能用 warning 代替

默认监听 loopback、默认要求非空凭据、默认有限 TTL。
"启动时打印一条警告"不是安全默认值——没人读日志。

> **案例**：`tools/rescue.py` 默认 `0.0.0.0`、token 默认空（空 token = 无认证）、
> TTL 默认 `None`（与文件头 "TTL suicide" 的安全主张相反）、明文 TCP、
> agent 与 CLI 共用凭据。外审判定为完整远程代码执行面，已排除出批次。

### S3 · 错误分类必须区分**永久**与**瞬时**，含混时保守取"未知"

把永久错误分类成"可重试"，等于让监控**永远重试一个不可能成功的操作**。
一个 code 若同时覆盖永久与瞬时成因，**保守留未分类**（70），
不要按"最常见的成因"猜一个。

> **案例**：`pty_alloc_failed` 同时覆盖临时 PTY 压力、**永久缺失的 `/dev/ptmx`**、
> 以及 `SubscribeSync` 失败，却被分类成 75（自愈瞬时）——
> 这是 A1 本身引入的、A1 立项要消除的那个缺陷。
> 更严重的一次：**全部五个 force-single 拒绝码**（`peer_alive` / `quorum_not_lost` /
> `force_single_refused` / `arm_expired` / `is_leader`）都落在 70，
> 而 `docs/usage.md §9.13` 告诉自动化 70 是可重试的。

### S4 · 明细错误进日志，不上 wire

存储层错误文本（SQLite 报错）可能泄露路径与结构。回复里给 code，明细写 broker 日志。

### S5 · 去重 / 聚合的 key 必须包含**身份**，且必须排除**数值噪声**

两个方向都会坏：

- key 里少了身份 → 第二个故障节点被整窗吞掉，事故读起来像"只有一个节点挂了"；
- key 里混进数值（term / index / duration）→ 每次都不同，**去重完全失效**。

> **案例**：`dedupKey` 先是只用 `a.(string)`，而 raft 传的是 `raft.ServerAddress` /
> `ServerID` 这类**具名 string 类型**，断言为 false，两个失联 peer 合并成一个。
> 改用 reflect string-kind 修好后，又加了个 `fmt.Stringer` 兜底当"便宜的保险"——
> 而 `time.Duration` 正是 Stringer，于是 `heartbeat timeout` 变成
> `heartbeat timeout\x1f1s`，**从另一侧把去重关掉了**。

---

## 五、写给未来的自己：三个反复出现的形态

1. **"我刚写下的断言是否为真"是结构性盲区。**
   一个把"删除假 godoc"当作核心工作项的批次，自己**新增了 7 条假 godoc**。
   自我验收查不出这类问题——需要元闸门（A4）或外部审查。

2. **快 + 绿 + 少跑，长得和快 + 绿 + 跑全了一模一样。**
   凡是"这次怎么这么快"的时刻，先怀疑覆盖，再庆祝性能。
   四次静默少跑全部出自同一个解析层（见 `docs/reviews/e2e-parallel-plan.md §4`）。

3. **把调度问题误诊为供给问题。**
   整轮全量串行期间机器 97.5% 空闲，却被记成"并行会资源饿死"；
   simcluster 的"drills 必须串行"真因是 `fs.inotify.max_user_instances` 耗尽。
   **优化或归因之前，先量瓶颈在哪一层。**

4. **听起来对的因果，一查就塌。**
   "测试慢是因为层次太重"、"flake 是环境问题"、"两次跑同一包是不同构建配置"——
   三条都曾被写进文档或注释，三条都被一次实测推翻（rootcause 四类根因无一是层次；
   `go list` 闭包 md5 全等）。**任何"未修缺陷"或"已知冗余"进入优先级表前，
   先 `git log -S<关键字>` / `git tag --contains` / 跑一次命令核一遍（1 分钟）。**

---

## 六、改测试代码的收据要求

测试代码重构**没有测试保护**——删错一条断言，套件照样全绿。所以每一类改动都要有一份**机械收据**：

- **R1 · 删除 / 改名任何 Test、Fuzz、Benchmark 函数**：`test/determinism/testdata/test_function_inventory.txt`
  必须**手改**（删旧行），commit message 写明理由。`-update-test-inventory` 只追加、拒删。
- **R2 · 合并 / 吸收 helper**（如把 N 份 `seedSession` 收成一份）：留一张"被吸收"表 + 反向断言
  （被吸收文件不得再出现非转发定义），与 `layering_test.go` 的 `originalUnion` 同形。
- **R3 · 改跑测试的方式**（矩阵、分片、去重）：收据必须证明**同一选择在同一二进制上跑过**，两半缺一不可——
  (a) **选择相同**：改前后每个被折/被分的 (pkg, tags, race, run) 下 `go test -list` 的**名字集合**逐字相等
  （数量相等不算，G4）；(b) **二进制相同**：`go list -deps -test -race [-tags] -f '{{.ImportPath}} {{.GoFiles}} {{.Imports}}'`
  的闭包 hash 相等；再加 (c) 一轮 `make e2e-parallel` ALL PASS。第一版写的是「双跑取 `--- PASS/FAIL` 名字集合」，
  而 B6 实际交付的是 (a)+(b)+(c)——runner 按闭包 hash 折叠时**每次运行**都在做 (b)，(a) 只需在改动时做一次。
  两份写法曾并存（内审 L5-F5），按「安全实用主义」取实际可执行的这一份。

> **案例**：`layering_test.go:279-297` 记载四份规则合并时丢了一整行 + 一条子句，全绿；
> `promised_guard` 一装上抓到 34 条死承诺；e2e runner 曾静默少跑 11 个套件并因此更快。
> 三次都是同一个形状：**套件看不见自己的收缩。**

## 七、受保护资产清单（改之前先读它旁边的注释）

| 资产 | 位置 | 它守的是什么 |
|---|---|---|
| 两本只减不增的账本 | `test/determinism/legacy_process_named_{list,funcs}.go` | 条目消失即红，永不腐化成永久豁免 |
| 合并收据 | `test/architecture/layering_test.go` 的 `originalUnion` / `deletedRegressionTests` | 四份被删文件的每条子句可反查 |
| 精确计数门 | `test/architecture/tls_verify_pairing_test.go` | 站点数钉死，新增合规站点也红——逼人读 |
| golden 只收紧 | `test/architecture/structural_budget_test.go` 的 `-update-structural-budget` | 拒绝写入更宽的值 |
| 身份清单 | `test/determinism/testdata/test_function_inventory.txt` | 测试函数只增不减 |
| harness 环规则 | `internal/testharness/harness.go` 头注释 + `test/architecture/layering_test.go` 的 `internal/testharness` 行（机械半） | testharness 不得 import broker/agent/session（14+7 个包内测试 import 它）；产品依赖原语去 `test/stackharness`（其头注释有完整论证） |
| 泄漏门否决 goleak | `internal/testharness/leakgate.go` | 理由在文件头 |
| KNOWN REDUNDANCY 段 | `test/e2e/all_phases_test.go` 头 | 原文保留 + 2026-09-01 实测段；矩阵源不为去重而改 |
| 日志流单映射 | `test/simcluster/drills/lib/logs.sh` + `test/architecture/simcluster_log_oracle_test.go` | 十几份内联映射曾把健康产品报失败 |
| 唯一全矩阵闸 | `make e2e-parallel`（串行 target 已删除） | 存在的 target 就会有人跑 |
| 全部注释 | `cmd/` + `internal/` 29–33% 的行 | `.golangci.yml` 故意不开行数 linter |
