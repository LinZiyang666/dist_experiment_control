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

### 0.4 真二进制层（`test/cli_e2e/realstack_harness_test.go`，hot-update S1/S3/S4）

**Release-mode timing budgets (S10 lead decision D-A, 2026-10-22).** The 500 ms agent freeze,
≤5 s sole-voter reload/first durable write, 30 s Tier-A transfer and 10 s T46 durable-progress
budgets are production SLOs. Build both measured images with the shared cached
`testharness.BuildReleaseCandidate`: `CGO_ENABLED=0`, `-trimpath`, release `-s -w -X ...ReleaseVersion=...`
flags and no `-race`; retain inherited mutation/observer overlays. The harness may run under
`-race`. Run the same functional assertions against race candidates separately, without an SLO
verdict. Budget receipts identify the measured executable SHA, toolchain, race/CGO settings,
duration and unchanged limit. `CheckReleaseBudget` must reject an actual race-built candidate,
even with an in-budget duration. Go omits linker flags from build info with `-trimpath`; the
shared builder fixes those flags and the receipt records the executable's available metadata.
A release-mode miss requires a product fix and regression/mutation evidence. T46 push
prepare-accepted remains `not_covered` only when its release receipt proves the 10 s miss is
upload-bound; eventual success cannot replace the cut-anchored durable Sync deadline.


L3 里有一个子形态，谓词是「**被测的 ctl 与 agent 是编译出的 `tether` 二进制的真进程**」，而不是同进程里的
`agent.New`/CLI helper。它守的是进程内集成结构上看不见的东西：真 ctl 的退出码与 stderr 文案、真 agent 子进程的
5 s 心跳与 broker 默认 `StaleAfter`/`OfflineAfter` 的交互、NATS 连接被掀掉时两个真客户端各自的重连与续接。
写它有五条不能违反的纪律，每条都来自 S1 落地时踩到的一次假绿/假红：

1. **匿名模式必须 `opts.AlwaysEnableNonce = true`**（嵌入 nats-server）。否则带 nkey 的真 ctl 得到 `nkeys not
   supported`；而**用 `TETHER_DEV_NO_AUTH` 绕过**会让 `tether exec` 的可靠路径静默退化成 legacy——整个套件仍然全绿，
   守的却是另一条路径。这是本 harness 最重要的一行。callout 模式照抄 `test/p3` 的 Nkeys+AuthCallout 形状。
2. **broker 保留产品默认 `StaleAfter`/`OfflineAfter`（5 s / 60 s）**。进程内套件常用 300 ms / 900 ms 让测试快，
   但真 agent 子进程 5 s 一次心跳，任何 > 0.9 s 的 bounce 都会被判 OFFLINE——那是 harness 制造的 `node_offline`，
   不是产品的。
3. **故障注入只切 TCP，不伪造字节**。匹配请求的 PUB/HPUB 帧转发前，relay（`natsrelay_test.go`）先封住
   对应 server→client 回包方向并给出已封闭收据，再完整转发请求、断链。必须另观察首次副作用已经发生，
   同时证明该 reply 未交付；仅「forward 后立刻 close」有竞态，不能推出回复必丢。重试用原 handle/操作序号，
   create/input/resize/signal/KA/hangup/terminal-ack 各自有外部收据。要模拟 server/broker 重启就真重启，不能伪造回复。
4. **每条用例自带非空性断言**：agent 日志 `agent: exec … reliable=true` 恰 1 行（走了可靠路径且只执行一次），
   或观察订阅计数 `process-resume` create ≥ 1 且 `exec.req.forwarded` == 0。一条在 legacy 上跑绿的「续接」测试
   什么都没证明。
5. **Linux-only（`//go:build linux`）**，且进 `-race` 矩阵（`TestReliableExecMatrix`），不进 `make test` 之外的任何
   串行路径。Darwin 上它不编译、不计数——`test/README.md` 的表里写明。

前一发布的真二进制（N-1 象限里旧 ctl / 旧 agent 那两格）**不在** `go test` 里构建：hermetic 用旧方的**订阅面**代替旧方
本身——旧 agent 是 `legacyAgentSim`（只答 `exec`、对 `process-resume` 静默），旧 broker 是 server 端 deny 掉今天 broker 的
`process-resume.req` 订阅（`newRealStackLegacyBroker`：v0.6.0 broker 本就没有这条订阅，ctl 得到的是 server 自己的 503；
注入自证读 server 的连接表，不读假设）；真 v(N-1) 二进制由 `test/simcluster/lib/stage.sh` 的 `vendor/tether-prev`
供给（从前一 `v*` tag 的源码在 detached worktree 里构建，`tether-prev-source.json` 为收据，构建失败 fail-closed；
`tests/prev-supply-test.sh` 守它），只被 deploy-tier drill 使用。

S3 在 `test/cli_e2e/node_upgrade_image_test.go` 沿用 realStack（Linux-only，真 ctl/agent；当前进程统一经共享
subreaper/CLOEXEC lease/pidfd supervisor，Setpgid 只作补充），验证 `node upgrade` 真实替换运行映像。以下四条是
[S3 plan §7 T16](reviews/hot-update-agent-bootstrap-plan.md) 明列、由 **S3 交付**的测试承诺，
不是本段声称当前候选已跑绿的收据；S3 历史 deploy-tier 结果见 evidence §14：

| 测试 | 必须观察的结果 |
|---|---|
| `TestNodeUpgradeCarriesLiveExecAcrossExec` | agent 同 PID、`/proc/<pid>/exe` SHA 改变；原 child 身份不变、producer 字节连续、原 ctl rc 23；marker committed/generation/resource_phase=active/imported_resources=1/freeze_window_ms>0；`image activated` 恰一行；无残留 handoff memfd、稳态 fd≥3 全 CLOEXEC |
| `TestNodeUpgradeGenerationChainKeepsLiveExec` | gen1→gen2→gen1 的真实运行 SHA 序列、generation 1→2；同一 child 跨两跳，流与终态仍精确；每代一个冻结窗样本 |
| `TestNodeUpgradeDefersOnBusyResource` | 开 legacy PTY 时 rc 75，文本含 `upgrade_deferred` 与 `legacy_session_busy`；无 marker、磁盘不变、业务继续；可靠 PTY 则由 S4 的 image/升级用例证明可交接 |
| `TestNodeUpgradeRefusesLegacyCandidateWithLiveTasks` | fake candidate 的 `version` 没有 ABI 行，活任务在场时 rc 64 `image_abi_unsupported`；无 marker、磁盘不变、child 继续；这个 fixture 只证明 ABI 拒绝，不冒充真 N-1 binary |

候选由 `httptest` 明文 loopback 提供，agent 显式传 `--upgrade-url-allow http://127.0.0.1:` 前缀；
gen2 用同源码 `-ldflags` 变体或尾部附加字节的副本，**SHA 必须不同**，`version` 末行仍为 `image-abi v1`。
不能用 fake execFn 或直接调用 `agent.New` 代替这层。真 binary 由 harness 嵌套 `go build` 产出：
生产变异用 `GOFLAGS=-overlay=<json>` 传到整棵构建图，单独给外层 `go test -overlay` 不会自动改变内层构建。
至少注入 New 忽略携带映像而初始化空 runtime、Run 在 Thaw 前 register、删除 ABI 门三种缺陷，确认各自红；
冻结窗保留 `freeze_window_ms/shim_ms/import_ms` 原始样本；D-A 规定 release candidate 超过 500 ms 必须失败，race candidate 只记录功能样本，不提高 `upgradebudget.LocalFreezeWindow`。

**激活日志的跨语言契约**（origin: hot-update S3 plan §3.2 / §8.2；DR-1）：
`cmd/tether/agent_log_format_test.go` 的 `TestAgentLogFormatMatchesActivationRecords` 使用真实 `newLoggerTo`，
以固定时钟分别渲染 text/JSON **完整 slog record**，与
`test/simcluster/tests/testdata/agent-activation-records.txt` 的两行 golden 逐字比较，并钉住生产 activation emitter。
Python 的 `test/simcluster/tests/hot-update-process-probe-test.py` 消费同一 golden，经真实 CLI judge 验证。
改 emitter、logger 格式或解析器时须核对两侧；不能拿去掉 record 包装的裸 message 证明解析成功，
也不能修改 golden 掩盖回归。缺字段、重复字段/record、非整数/非正数及裸 message 的负控制都须拒绝。

**混合探针按观测窗判定**（origin: hot-update S3 plan §8.1 / §8.2；DR-4/DR-5/DRILLS-V1）：
升级窗口使用新日志 cursor 后**实际观察到的** `agent: image cut` 与 `agent: image activated`，
由探针自己的单调时钟记 `cut` / `activated` marks；不是用启动 upgrade 命令或预计下载耗时安排 marks。
先拿 `started.json` 握手再升级，持续发请求直到观察到 activated 加 tail；到 max-duration 仍缺 mark 必须失败。
日志轮询到达时刻是观测端点，不冒称它与进程内部 Freeze/Thaw 时间戳完全重合。
P0 校准不能替 P1 证明负载：P1 本身须达到配置速率的 90%，read/write/short 各有成功样本，
并至少有一个 **rc 0 且 ledger 确有副作用的 write，其请求区间与 `[cut, activated]` 相交**。
`agent_upgrading` 只允许请求开始于该窗内且无副作用；重复副作用、rc 0 无副作用、非零 rc 有副作用、
ledger 中无对应已发 write token 的副作用都拒绝。样本必须完整、已 reap，skipped 不能算请求；
保留 reliable/legacy 路径计数。缺少实际窗口内成功写、全部 exec 被拒、升级发生在流量结束后等反例必须变红。
这些是 hermetic oracle 与 S3 drill 的编写规则；S3 历史实跑见 evidence §14，当前候选仍须独立回归。

**S4 双 PTY 与真实 syscall 收据**（origin: hot-update S4 plan §8 / §9，lead L5/L6）：
`test/cli_e2e/run_reliable_test.go` 使用两个独立 PTY：本地 ctl 自己 setsid 并成功取得真实 controlling tty，
远端业务使用原 kernel PTY。TIOCSCTTY 失败是 setup failure，不能忽略；本地允许测试 slave keeper，
远端禁止额外 slave keeper。ctl 的退出必须由**原进程 waitpid**证明，不能等本地 master EOF 或开一个新 run 顶替。
原 foreground/job 的 boot/PID/ticks/nonce、master index/mount、agent 同 PID 与 running SHA 是各自独立的身份收据。
raw 双向 bytes 比较保留原顺序、次数及首尾，不过滤 CR/LF、不用源 ledger 拼回收到的输出。
T35 的 upgrade/rollback、T36 丢回复和 T37 settlement 均须证明 reliable 路径及唯一 spawn；
resize 去重测试在首次 resize 后由对端主动改变尺寸，signal 先获处理 handshake，不能用天然幂等掩盖重复副作用。

T04 的 blocked raw slave 必须实际不读；记录一次真实 master.Write 的 attemptLen、n、error、调用序，
断言 `0<n<attemptLen`，冻结 Applied 恰为 n，跨**同 PID exec**后 suffix 无重无丢。
只有 `0<A<Q`、构造短写返回值或检查 SetDeadline 返回 nil 都不是短 syscall 非空收据。
T03 要在 resize 前后实际堵塞读/写并证 deadline 唤醒；T02 要证明 leader 为 zombie、ReapHeld，
后代超过旧 200 ms 再写尾流；不能用 leader 活着的用例代替尾流窗口。
真 binary 内层 build 沿用 GOFLAGS overlay 传播与 race 插桩收据，外层 `go test -overlay` 不能代表生产变异已加载。

可靠 stdio 的测试按 lead L5 覆盖 TTY/FIFO 独立 OFD、regular file 与 `/dev/null` 成功，socket 在 create 前提示回落，
不以 64 拒绝文件重定向。EOF/实际 0x04/raw 0x03/外部 SIGINT/SIGTERM/显式 signal 分开，取消后 join、恢复 termios/原 FD flags。
Darwin `TestLegacyRunDarwinTerminalLifecycle` 为 darwin-tag 真 legacy 测试；cross-build/vet-tags/unsupported twin 可编译
与实际 Darwin run/resize/cancel 分列，无 runner 写 NOT_RUN「未在 Darwin 实跑」，不阻塞 S4 出口。

**五轮资源锚点**：ptysession、terminalio 和直接 import pty 的新宿主，在 Test body **直接**调用
`testharness.AssertNoGoroutineLeak/AssertNoFDLeak`，每个 assertion site 登记 `leakExerciseAnchors`，
实际 exercise≥5 轮且每轮有 spawn/I/O/cut/import/失败回收等所声明活动。空循环、死 helper 或循环外唯一 spawn 不算五轮。
T18/T34/T43 的泄漏变异须分别留下 master dup、pump 或 replay/pidfd 责任并击红；不能靠 trap kill 做出产品清场。
在 owner 仍活时按 FD 类别与 identity 验保留/释放阶段，settle 后本 task 归零；总 FD 相等不能掩盖提前关闭业务。
ptysession 整包 `-race -count=3`，其它风险包按整包 race 矩阵覆盖；命名筛选不冒称整包 race。

### 0.5 同 PID exec harness（`internal/execsession/runtime_image_exec_test.go`、`internal/agentimage/bundle_exec_test.go`，hot-update S2）

谓词是「**同一个 PID 把自己 exec 成新映像，原 child / pipe / memfd 由内核原样持有**」。一个测试函数扮三个角色
（父进程 / helper 的 before 半 / helper 的 after 半），形状固定，每条都是一次假绿或死锁换来的：

1. **一个测试函数 + mode env + 精确 `-test.run=^Name$ -test.count=1`**；helper 内**只** `handoff.Prepared.Exec`，不二次
   fork（`spawnexec` 的 init 分派与本 harness 的 mode 键不冲突——它们各自的键集专属，且 helper 是 `os.Args[0]` 同一二进制）。
2. **环境双向剥离**：helper 环境 = `handoff.CleanEnvironment(os.Environ())` 剥掉本 harness 的全部键再加本 mode 的键；
   业务 child 的 `cmd.Env` 同样剥（`runtimeChildEnvironment`），并由 child 打印 `${MODE}${_TETHER_HANDOFF}` 为空自证。
   after 半二次 `handoff.Restore()` 必须是 `(nil, nil)`（dispatch 只消费一次）。
3. **oracle 在 exec 之前捕获，after 半只读 receipt**：能证明连续性的量（PID / start ticks / pipe dev:ino / ACK 偏移 /
   InputBase / 预算 / 在途请求数）在 before 半写进 `handoff.State.Data` 的 fixture，after 半与内核和导入结果比对后写
   receipt JSON，父进程逐项断言。fixture **可以**带 Image（因而带 registry secret）——它只在 sealed memfd 里走；绝不能带
   它的是 stdout/stderr/日志：两处 harness 的父进程都断言 helper 输出不含 `"secret"`（execsession 的还断言不含 `[redacted]`）。
   **after 半的资源收据必须按类别计数**（`/proc/self/fd` 链接名：`pipe:[…]`、`/memfd:tether-replay`、`anon_inode:[pidfd]`、
   `/memfd:tether-handoff`），Close 后各类归零、总数低于导入前——「总数没涨」的 oracle 在 closeTask 变成 no-op 时照样绿
   （内审 R1/M28）；预算收据取导入后的 `ReplayBudgetUsed()`，不是导出方的数字。
4. **后代与 child 的推进用控制管道（fd 3）或 marker 文件门控，不用 sleep 计时**；所有进程走共享 subreaper/CLOEXEC lease/pidfd supervisor 并 join；`Setpgid + cmd.Cancel` 杀组只作补充。
   跨 exec 的 ctl oracle 是**真 `execclient.Run`** 对嵌入 NATS——它的 500 ms attempt 重发就是「旧映像的在途请求丢失、
   新映像回答重发」的机制本身，所以「冻结期至少一个请求在途」是必须断言的非空性（否则测试只证明了一次普通 exec）。
5. **负控制用 mutation env 改导出对象**（`drop-stderr-replay`：after 半必须 `IMAGE_ORACLE_REJECT` 并非零退出、ctl 拿不到结果），
   **生产变异用 `go test -overlay`** 且每个变异副本在入口打 `IMAGE_MUTATION=<name>` 自证——overlay 是整个构建图的输入，
   被 exec 的 helper 就是同一二进制，所以 before/after 两半都带变异。
6. **memfd/pipe 计数法**：`/proc/self/fd` 的链接名（`/memfd:tether-handoff (deleted)` 必须为 0、每条业务 pipe 恰 1 引用），
   跳过 fd 0–2（父进程把 helper 的 stdout/stderr 捕获在同一条 pipe 上）。带 NATS 的 exec 测试**只跑一次**做字面收据
   （嵌入 server 的 accepted socket 异步回收让父侧 FD 容差 0 flake）；五轮资源门另用无 NATS 的 bundle exec 测试与
   进程内往返（共享冻结 Manager，`imageDupSource` 用 `F_DUPFD_CLOEXEC` 扮演 handoff 的复制）。
7. **Linux-only、进 `TestLeakRiskMatrix`**；三处 exec 测试在 `-race` 下各 +2–3 s。
8. **S3 进程内注入用 `agentboot.FromCut` + `handofftest.DupSource`**，经 `Config.BootImage` 进入生产 New/Run，
   不另造一套导入路径。A 完成 Freeze/导出后，用 `internal/handoff/handofftest.DupResources` 对资源逐文件
   `F_DUPFD_CLOEXEC`，再把 bundle、rows、DupSource 与其 Borrow 资源传给 FromCut。`TakeFile` 转移副本所有权，
   `Borrow` 只借未取项、`Remaining` 计数、`Swap/Drop` 制造同形错位/缺文件，`Close` 只关未取项；
   **不把 A 的原 `*os.File` 直接交给 B**。拒绝后验证 salvage 可与 Borrow 余项按 ID 合并再派发，
   不用 Close 销毁导入冻结态（Manager 仍冻结时返回 `workload.ErrFrozen`，此时应用 Detach）。
   dup 仍共享 open-file-description，A 已导出的旧 runtime 在 B 接管成功后只 Discard、不 Thaw；
   这是调用纪律，不能假设 API 会拒绝旧 runtime 的 Thaw（exec 失败回滚恰恰需要允许它）。
   共享 child 的 cleanup 次序必须是 **kill child → 等 B 记终态 → `A.workloads.Thaw()`**，再完成其余释放，
   避免 A 的 reaper 抢走 B 的终态。带并发/泄漏风险的用例用共享 leak helper，≥5 轮 exercise；
   这些是 plan §7 T3–T5/T8/T9/T14 由 S3 交付的进程内测试形状，不能替代同 PID 真 exec 的收据。
9. **未取文件的释放要在活进程中观察**（origin: hot-update S3 plan §1.3 / §1.6；RS-1）：
   held/refused/imported 可保留 handoff 余项供 Repack；Activate 成功 Thaw 后，真实 Restored 的未 TakeFile 文件应立即关闭，
   已取业务文件继续由 runtime 持有。进程内 FromCut 的借用资源由 fixture owner 回收，不能把它当成 Restored 的释放证明；
   不能等 receiver 退出后才验 EPIPE/fd 消失，否则内核退出清场会让漏 Close 也通过。

### 0.6 三角色真二进制 harness（hot-update S3）

谓词是「**dispatcher test binary → real tether → receiver test binary（作为 `.prev`）**，三者用同一 PID
连续 exec」。父进程仍是 go test，负责构建、起 dispatcher、收 receipt；被测中间一跳必须是真
`go build ./cmd/tether` 产物，才能观察 main/boot shim/Cobra/FinishBoot 的真实顺序。
位置与测试承诺来自 [S3 plan §7](reviews/hot-update-agent-bootstrap-plan.md)：
`internal/agentboot/boot_exec_test.go`，由 **S3 交付**；本规范不代签部署；S3 历史收据见 evidence §14，当前候选需重新验证。

1. **三角色边界固定**：父进程在 sandbox 准备真 tether、marker 和 `.prev`（test binary 的副本）；
   dispatcher 起真实 child，Freeze → Prepare → `Prepared.Exec(<sandbox>/tether, ["tether","agent",…], env)`；
   tether 经生产回滚路径 exec `.prev`；receiver 才 `handoff.Restore` → `agentimage.Restore(Target{fresh Manager})`，
   按需要 Thaw 并验 oracle，写 receipt、释放资源后退出。不能用测试 shim 代替中间的 main，也不能用 fork 伪装再派发。
2. **receiver 必须在 `init()` 按专用 mode env 分派，早于 testing 解析 flag**：它继承的是 tether argv，
   不是 `-test.*` argv；等测试函数运行才分派会先被 flag parser 拒绝。环境清理沿用 §0.5 的双向剥离，
   父→dispatcher 用精确 test selector；mode 只给对应角色，业务 child 不继承 handoff/mode 密钥。
3. **marker fixture 镜像生产字段，不手造另一份协议**：父按 `upgradeMarker` 写 JSON，fixture 源旁带
   `// drill-pin: mirrors internal/agent/upgrade_state.go upgradeMarker`，测试/守卫旁带
   `// origin: hot-update S3 plan §7`。receipt 与 marker/dst SHA 必须由父断言，不能只以 helper rc 0 判成功。
4. **跨中间映像失败仍须证明同一资源**：dispatch 前记 State 字节 SHA、文件 dev/ino、child PID/start ticks、lineage；
   receiver 核对同一 bundle 与文件，原 child 仍活且能读到 exec 后的新字节；再派发 env 含原 `TETHER_INSTANCE_ID`。
   manifest nonce 每次重建，不要求相等；receiver 二次 `handoff.Restore()` 为 nil、handoff memfd 为 0，
   输出不含 `"secret"`，不把 bundle 或其 raw Data 写进 stdout/stderr/receipt。
5. **用真实早失败触发路径**：`TestAgentBootstrapRunsBootCheckBeforeRestore` 以未知 Cobra flag 证明
   boot_count 先增加、尚无 `image restored`、FinishBoot 把资源送到 prev；
   `TestBundlePresentAndBudgetExhaustedRedispatchesToPrev` 令预算已耗尽，证明 boot rollback 携资源、marker 与 dst SHA 收敛。
   `TestBeginCrashWindowClosesForeignDescriptorsAndContinues` 用坏 nonce 证明 W1 关闭非 CLOEXEC 外来 fd 后继续，
   另以真实 `version` argv 证明 Begin 无条件运行、W1b 放弃资源而 version 照常输出。
   W1b 的 EPIPE/fd 断言须发生在接收进程仍活着时，不能由命令退出代替 Abandon 清场；
   W1 还须覆盖 `/proc` 普查失败后的 fcntl 回退与关闭前 dev/ino 复核（origin: hot-update S3 plan §1.5 / §7 T13；RS-2、RESOURCES-V1、RB-5）。
   `TestFinishBootRedispatchTable` 按 plan 的 proof/TargetInstance/拒绝与 salvage 半取表覆盖所有权分支；
   不把这些 hermetic 结果写成 deploy-tier 覆盖。
6. **变异要命中被测角色**：test binary 的 helper 变异可用 `go test -overlay`；中间真 tether 的生产变异必须
   经 `GOFLAGS=-overlay=<json>` 传入嵌套 build，并由构建/运行收据证明加载了变异。
   必测删除 FinishBoot、预算记账移到 Restore 后、去掉 Begin lineage 快照、boot rollback 改裸 exec、
   W1 不关外来 fd、Begin 移到 daemon 条件内；每种记录红灯及恢复后结果，不能用「看起来会红」作收据。
7. **隔离与资源纪律**：Linux-only，父用 `Setpgid` 与有界取消/kill-group 清理，所有路径用临时 sandbox/Home；
   不运行 PATH 上已安装的 tether，不触碰生产 Home。fd 按 pipe/replay/pidfd/handoff 分类别计数，业务推进用门控而非 sleep。
   并发/泄漏敏感覆盖用共享 helper ≥5 轮；进程内重复资源门与三角色字面收据各守其范围，不能以总 fd 数相等掩盖类别错漏。

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

## S6 broker-image validation

The [frozen S6 plan](reviews/hot-update-broker-image-plan.md) §11 fixes T01–T58 and mandatory mutations;
§12 fixes each owner. [Architecture §22.9](distributed-broker-architecture.md#229-s6-broker-image-binding-contract-final-receipts-pending)
is the living contract; landed tests are not final exit receipts. Follow §0.4–§0.6: sandbox-built real
artifacts, same-PID init dispatch, original socket/child identity, exact helper selectors, nested-build
GOFLAGS overlay propagation, and independent byte/effect oracles. Never exercise installed tether or
production state. No test process starts another go test. Preflight/Bolt second-open negatives require
bounded helper kill+wait; an old DB restore must be detected before replay can conceal missing ACKs.

Every concurrency/leak test directly uses both shared goroutine/FD assertions and directly calls its
exercise across ≥5 rounds. Partial constructor/close/Take, timer callback, accepted RPC, current-stop and
ACTIVE rollback paths must be exercised while original owners live; fixture exit cannot stand in for cleanup.
Inject each mandatory defect independently, preserve patch digest/native rc/failing assertion, restore,
then rerun positive control. Source scanners require on-disk mutants. P31/P32/P33 own exact caps, all-OS
lifetime/layering/build-tag/ctx, closed enum/error/wire/private inventory, matrix and leak anchors; workers
report rows instead of editing mechanical ledgers. The unfiltered gate/matrix controls must kill omissions.

P35 runs one final candidate's three hard gates (`make test`, `make e2e-parallel`, `make lint`) and separate
`make gates`, whole race matrix, Linux/Darwin builds, native Darwin resource regression,37/39.BR and preserved
S3/S4/S5 regressions. These are lead commands, not package-worker commands. Parallel matrix GREEN needs
no serial rerun. Source/artifact/running SHA, exact command/native rc, all scheduled slots/attempts,
nonempty originals/ACKs/true-started effects and measured marginal wall costs belong in receipts.
Normal max5s is hard; freeze500ms is complete required-domain calibration. Linux/cross-build does not sign
Darwin native; absent native runner is NOT_RUN and cannot close S6 exit6. P34 signs only linked verified
P35 receipts. [The documentation audit](../test/hotupdate/README.md#s6-documentation-audit) is read-only and
has independent missing-source/command/dead-test/premature-tick controls; it never signs product behavior.


## S7 transfer-continuation validation (PENDING receipts)

The [frozen S7 plan](reviews/hot-update-transfer-continuation-plan.md) §10–§12 and lead LQ1–LQ3/L15–L18
bind validation. These are required checks, not completed receipts. P36 runs only bounded documentation
checks; P34 owns T54 and its central controls, P35 the inventories/leak/race/matrix rows, P37 final evidence.
The [README audit](../test/hotupdate/README.md#s7-documentation-audit) is separately runnable and does not
certify product behavior. Missing dependent real implementations remain NOT_RUN, never fake successes.

- Use shared `test/transferharness` and the FIX-ORPHAN process supervisor for every process, including
  fixture/same-PID dispatcher/native child. Verify zero surviving descendants after panic/timeout/SIGKILL
  by PID+start ticks. Setpgid cleanup or Pdeathsig alone is insufficient. Sandbox HOME/state and workspace
  binaries only; installed tether and production state are prohibited.
- Cover both tiers, push/pull and prepare/write/Sync/preprimitive/postprimitive/courier windows using real
  A→B→A artifacts at the original PID, original source/tmp/parent OFDs and original ID/NUID. dev/ino/offset
  equality cannot substitute for kernel OFD comparison. Independent target observation at each boundary
  rejects transient in-place partial writes even if final SHA is correct. ACTIVE failures export current
  progress at generation+1; pristine imports cannot Seek/Truncate/repair shared flags.
- Default 120s chunk/history dedupe crossings use actual wall time. Both competitors are already sent:
  absent scan→original accepted with ACK lost→retry held until 120s+5s→late retry rejected by CAS.
  Simultaneous first acceptance after a wait is masked by a fresh dedupe window and does not kill M02/M12.
  Full raw physical records and canonical content/count are required; Apply=1 is not history=1.
- T30 exercises both actual TTL List edges with bounded ctx/cancel/join and ≤5s pass, followed by a real
  next pass without fixture metadata writes. T31 covers DELETING/cold restart/orphan sweep protection,
  ordinary CONNECT denial, receiver-only settlement permissions and terminal-before-deletion. LQ1 controls
  prove no flag→75/no deletion, explicit abandonment→exact terminal=1/failed/outcome_unknown=true.
- Every explicit risk package in plan §11.3 has direct Test-body shared goroutine and FD asserts after
  ≥5 direct exercise calls, with P35 exact anchor and whole-package race tuple. Include filetransfer,
  transferpath, drillobserve and test/transferharness. Pure codec/contracts remain whole-race with a no-IO
  declaration; add lifecycle exercises if they begin IO. Do not broaden worker checks to whole broker/
  agent/tunnel races. Lead/P37 runs the full matrix; per-test partitions keep the 600s unit ceiling, with
  >70% units split or optimized. Tagged tests and every scanner follow moved definitions immediately.
- Each M01–M14 subcase mutates the real production predicate independently, builds with inherited
  GOFLAGS overlay through nested builders and records source patch/artifact/running SHA. Compile/setup
  failure is not a behavioral kill. Source scanners require on-disk mutations. Restore then rerun baseline;
  preserve native failure text/rc and do not relax any assertion or inventory to make it pass.

The frozen positive parent 63 requires A/A1/B/C1/C2/C3/R1/R2/X/W1/W2, each independently native GREEN0.
Four independent buckets carry 66,549,440 B and 1 GiB each push/pull. Every leg/cut has actual active data and
post-cut progress; prepared sender substitutes and completed short files are INCOMPLETE4. C1 carrier,
C2 data connection and C3 same-binary/same-store JS restart have separate injection/identity receipts.
R1 pristine and R2 ACTIVE are separate; W1/W2 are separate real 120s watchdog units. Ctrl-C and session-rm
victims use separate buckets from each other and from the four survivors. Preserve 61's all 41 assertions,
37.T capable single/N3 live cuts plus legacy defer,39.BR genuine old Run-unwind PRODUCT-RED3 and the
S3/S4/S5/S6 regression set. S9 binary upgrades cannot be signed from same-store JS restart.

Controls 64 has 23 independent launch arms (M08/M10/M11/M12/M13 subcases separate), outside parent 63.
Native verdict grammar/order remains ASSERT-FAIL1→SETUP-RED2→PRODUCT-RED3→INCOMPLETE4→GREEN0;
expected native red stays red. Business temporary refusal 75 is never drill INCOMPLETE4. Missing fixture
custody/overflow/drain ACK is SETUP-RED2; expired live window is 4; runner timeout without drill_end remains
INFRA-ABORT. Ending requires canonical history, joined courier/finalizer/watchdog, consumer delete/writer
retirement, fresh per-process drain+JS scan, quiet window≥2s after join, second fresh drain+scan, then
keeper/fixture teardown. Teardown cannot kill late effects to manufacture a unique terminal.

Every product receipt carries source_sha, dirty_digest, artifact_sha, running_sha, test_or_arm, command,
native_rc, mutation_id, raw_path, oracle_input_sha, reader_digest and wall_s; each cut additionally carries
pid, process_start, boot_id, image_generation and cut_id. P37 reviews actual raw content/coverage and
candidate freshness before P36 signs B-3/B-6 transfer portions or E-1/E-2/E-3/E-5/E-6. E-4 remains S6;
F/G/H totals remain open. Product/config/oracle changes invalidate affected receipts even if a later
docs-only audit passes. Record launch-unit actual wall/direct poll, baseline/marginal wall and FD/RSS/JS
peaks separately; plan cost estimates remain ESTIMATE/NOT_MEASURED. Darwin cross-build and test-binary
compile differ from native lifecycle tests; required missing native receipts leave their exit unsigned.

## S8/S9/S10 transaction, infra and final-candidate validation

The frozen [S8](reviews/hot-update-upgrade-orchestration-plan.md),
[S9](reviews/hot-update-infra-nats-caddy-plan.md) and
[S10](reviews/hot-update-fullstack-acceptance-plan.md) lead verdicts define the current validation scope.
Target describe→prepare→root stage→same-TxID Start→fresh wait is the product route; fixtures cannot
write selectors, `.prev`, markers or journals to complete it. Test status-query rc0 separately from
wait/rollback completion: historical completed, register and disk-only SHA must fail current proof.
Exercise three completion receipts with full attempt/revision/direction/scope/nonce/sequence, current
rollback after ACTIVE, incarnation-guarded G.1, and actual activation/retirement events for two live
generations. Capability, release N-1 and same-source mechanism binaries retain separate receipts.

S9 integration has real stock version/artifact, privilege, business/auth and process/socket/storage
oracles. Auth-first tests need signed ordinary CONNECT plus normal-queue probe after SUB/PONG; a static
credential or S6 local handler probe cannot sign it. Read ACK prefixes before product recovery gates
release writer/replay/reconcile/GC. N1 original-store restart, N3 pre-LDM cohort movement, N2/quorum/
replica refusals and healthy N2 Caddy are independent. Caddy canonical fresh TLS, WSS migrate/return,
current ACME final sync, permissions and exact old/temporary retirement cannot be inferred from a URL
or HTTP200. A serving shadow may survive an action timeout but the transaction stays pending/halted.

Use installed per-test race partitions with a 600s unit ceiling; >420s requires decomposition or lower
cost. Whole-package race is an exact discovery union, including all suite tags and newly moved leaf
owners. Every source scanner/tag-gated test follows moved symbols immediately. Every subprocess, even
a fixture, nested build, native child or init dispatcher, uses shared internal/testharness supervision
(S6 FIX-ORPHAN; S9 L22/S10 L15), not a second Pdeathsig-only supervisor. Verify PID+start ticks, join/reap
on panic/timeout/SIGKILL and preserve cleanup-found leak failures. Direct shared goroutine/FD assertions
and ≥5 direct exercise calls are required for each risk package, with retirement observed while the
receiver remains alive. Parent runtime.NumGoroutine does not measure agent resources across exec.

S10 99 calibration/full rollout/prefix/soak and 100's 50 independent controls need all original cohorts,
all scheduled 10/s slots/attempts and raw bytes/effects/current identity; no completed short transfer
substitutes for a live window. Baseline→mutant→restore records exact source/dirty/artifact/running/oracle
identities, native rc and assertion. Compilation/setup/timeout/refusal is not a continuity mutant kill.
Use on-disk mutations for scanners and GOFLAGS overlay propagation through nested real builds. Record
launch wall/direct poll, baseline and marginal cost, FD/RSS/JS peaks; estimates remain NOT_MEASURED.

38/39/85/86/99/100 live runs, final same-candidate three hard gates plus separate gates/whole-race and
Darwin native are NOT_RUN/PENDING. S8/S10 normal full-profile freeze500ms is a hard gate; S3–S7
calibration samples do not sign it. S9 hot support is linux/amd64 + systemd/pidfd/cgroup/nft; cross-build
never signs a native platform. `lint-install` validates only its actual extracted heredoc templates;
live units/permissions/ACME/kernel behavior require S9 receipts. Workers run authorized targeted checks;
lead runs global gates/matrices and deploy drills serially on the sealed candidate after reporting cost.
P34/P35 (S10) and P39 (S9) retain unsigned product/coverage claims until reviewed raw receipts exist.
