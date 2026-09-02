# 测试体系革新计划（post-1.0 叶子增量）

> Date: 2026-09-01。CLAUDE.md §3 阶段 A 产物，**主进程定稿**。
> 来源：step 1 的对抗性 workflow（5 视角起草 architecture / infra / quality / distributed / conservative → 3 个立场固定的 critic destroyer / economist / executor，每个读全部 5 份 → 1 综合；run `wf_17da804f-171`，9 agent，55 min）。
> 本文只写计划，不含实现代码。file:line 对 `0be2365`（remote-fs stale-health 增量已提交后的干净树）核实。
> 基线：`make gates` 实测 **1m31s**（wall；user 2m7s），每道新门的边际成本按此记账（§5.3）。

## §-1 主进程定稿裁决（step 2）

综合稿的骨架**整体采纳**：它把三个 critic 高度收敛的结论（杀目录迁移、杀改矩阵源去重、杀 import 环的 testharness 收敛、杀覆盖率、杀自建变异器、杀周扫 timer；留测试身份 golden、租约时钟接缝、raft 计时门盲区、手写解析器 fuzz、发布等闸门、tag 局部性、runner 层去重）摆成了可执行的批次，且每条都能追溯到草案 id 与 critic 的 verdict。**这一次革新的真实内容不是"重排测试"，而是三件事：让测试自己出具身份收据（此后重构测试才有安全网）、把写好了但没在跑的东西接上线（fuzz / hermetic 闸集 / 发布链）、把两条已核实的闸门盲区与一个产品级时钟绕过补上。** "彻底"体现在它们都变成机械属性，不是体现在动了多少文件。

以下是我在综合稿之上做的改动，逐条列明（未列出的部分即原样采纳）：

| # | 项 | 定稿裁决 |
|---|---|---|
| **F1** | 在途冲突面 | 综合稿写于 remote-fs 增量提交前，整章"在途碰撞表"与"各批 Makefile/CLAUDE.md 改动作为最后一个 commit"的排序规则**作废**：`0be2365` 已落地，工作树干净。所有账本/golden 的首快照**现在**取；Makefile/CLAUDE.md 改动随各批一起做。spawnsafe/spawnexec 的 fuzz 目标（`sanitizePATH`/`Prepare`）不再是"推迟项"，但仍不进本增量——它们的输入不是对端可控的（是本机 mountinfo/PATH），不属于 I7 的判据 |
| **F2** | 事实清单里"`tether ps` 超 1MiB 未修" | **撤回**。`internal/broker/sessions.go:243-282` `reply_too_large` 兜底、`internal/proto/messages.go` `PortsTruncated`，首落 71e4943，`git tag --contains` 含 v0.5.0/v0.5.1。根因是我的 memory 停在 08-05，已改正。与本 plan 竞争的产品缺陷只剩 **Clash**（`internal/tunnel/tunnel.go:1301` 裸 `tls.Dialer{NetDialer:&net.Dialer{Timeout:5*time.Second}}`，`internal/tunnel` 零 proxydial 引用）。**排序裁决**：Clash 是独立产品增量、不并入本 plan；本 plan 不因它停等——用户已明令本增量推进到外审 |
| **F3** | seedSession 的真实计数 | 事实清单写"16 份、10 份差几个字节"，**过度渲染**。`^func seedSession` 是前缀匹配，混入了 `seedSessionAndNode/WithPin/WithPIN/Node` 5 个不同函数。严格同名 `seedSession` 11 份：**8 份**走 `session.Create(db, sid, sid, fp, "test-pin-hash", now)`、差异只在 `t.Fatalf` 消息格式（p4/p7/p8/p9/p10/chaos/cli_e2e/security）；d3/d4 两份是 `storage.OpenWAL(dbPath)` + 裸 `INSERT OR IGNORE`（raft 路径，语义不同）；agentprov 一份裸 INSERT 无 state 列。**B5 吸收 8 份，不是 6 份**（综合稿按 md5 只数到 6，漏了 cli_e2e 与 security 那两份语义相同、字面不同的） |
| **F4** | 940 冗余的判据 | 采纳 runner 层运行时 `go list -deps -test` 闭包 hash（§0 A8）。我独立复核：`internal/cluster`/`internal/broker` 在无 tag 与 d5–d9 每个 tag 下 **GoFiles + Imports + TestGoFiles + XTestGoFiles 四者全同**，生产源零个 `_integration` 门控文件；`phasefluidity_integration` 确实改变 broker 文件集（多 `phasefluidity_lifecycle_test.go`）——所以判据必须是运行时 hash 而非静态表，executor 的击穿成立。**收益如实写**：44 核上 ceiling=max(unit)，wall-clock 收益≈0；只在 2 核 CI 省约 11 min CPU（broker 255s + cluster ~100s×4），而 B1 让 CI e2e 时长变成 tag→release 延迟，所以它从"审美"变成"发布延迟"的收益 |
| **F5** | B5（租约 [5s,10s) 重放窗）现状 | 5 分钟核实：`docs/reviews/cloned-credential-instances-review-round2.md:349`「把间隔改成 6s 后同一测试仍然 FAIL……窗口只是从 [1s,10s) 收窄到 [5s,10s)」；后两轮外审与 gotcha **零处**再提 B5 ⇒ 按记录**仍开**。预裁决 A14 **生效**：B3 的模型测试若在 `advanceClock(6s)` 后命中 I1（两个 live-subscribed 同裸名），则 (a) 记录 seed 与最短序列进测试注释，(b) 该场景以 `// origin: docs/reviews/cloned-credential-instances-review-round2.md B5` 的 `t.Skip` 落地，(c) **另开产品叶子增量修 B5**，本增量不改租约裁决逻辑、不放宽断言。若模型跑不到红（守卫已在别处关掉了窗口），则如实记录"B5 已关，round2 记录过时"并把 Skip 去掉。**实施结果（B3）：复现，且形状比 round2 记录的更精确**——round2 的 6s 变体在今天的树上**过**（后台探针的 recheck 在 incumbent 订阅后才跑，缓存里是"held by A"）；真正开着的是 **register→subscribe 间隔 ≥ `backgroundProbeBudget`(3s)** 的形状：recheck 在 incumbent 订阅前完成、缓存"nobody"、克隆在 `leaseGrantWindow+1s` 读到"free"拿裸名而 incumbent 活着应答。`TestLeaseModelReplayWindowWhenTheIncumbentSubscribesLate` 以 Skip + 完整 trace 落地；`TestLeaseModelReplayWindowAfterABackgroundProbe`（订阅早于 recheck）**通过**。修法留给产品增量（候选：grant 时作废该名的 probeCache，或让 background verdict 的 `at` 取 `startedAt`——round2 §5.2 已提这一半） |
| **F6** | p2 `cloned_instance_e2e_test.go:216-223` | 综合稿给两个方向二选一。定稿：**两个都做**。(a) 现有 1200ms 测试**保留**，注释改为真值——它测的是"restart 落在 leaseGrantWindow **内**时 durable 名仍被保留"，这是 drill 83 之后新出现的合法路径，不是 bug；(b) 新增 `TestRestartedInstanceReclaimsItsNameAfterTheGrantWindow`，等待 `broker.LeaseGrantWindow() + 1s` 走 probe 路径。导出只读访问器 `broker.LeaseGrantWindow()`（先例：`cli.ClearCompletionCacheForTest`、`tunnel.SetRegLogClockForTest`）。代价 p2 +6s wall，换来的是 hermetic 层**唯一**覆盖"过窗口后靠 probe 保名"的测试——正是 drill 83 抓到、hermetic 够不到的那一类 |
| **F7** | I5（e2e 恢复 push-to-main） | **不改触发集**。经济学前提确实失效（仓库 PUBLIC、近 30 天 8 次提交），但 ci.yml 注释里的另一半论证仍成立：本机硬闸已让每个 commit 跑过全矩阵，CI 那份的价值是**另一套环境**，而环境按周变。只重写 ci.yml:5-20 的过时数字为今日事实 + 可复现命令。用户若在外审时要改触发集，一行 diff |
| **F8** | I4 发布延迟 | **接受** tag→release 等 e2e（2 核 ≈42 min，B6 后 ≈30 min）。发版是低频事件，artifact 出仓是本地硬闸够不到的唯一路径；逃生口是 `workflow_dispatch` 复跑，不是绕过 |
| **F9** | run-all.sh 漏网的 3 个 `*-external-review*.sh` | **进循环**，不改名。它们是 hermetic 检查，红了就是信息；`.sh` 不纳入命名冻结扫描（只扫 .go 维持，登记为不做） |
| **F10** | p11 并入 `ci_workflow_test.go` 并删目录 | **做**，放 B4（在 test_layout_map 精确集合之前落地，集合初值因此是 **18** 不是 19；矩阵 golden 在 B6 生成时已无 p11）。理由：两份解析测同一个 ci.yml 是 layering 四份那次的形态；本增量恰好把它需要的四处契约（allPhases、README 表、精确集合、身份 golden）都建了，收据齐全 |
| **F11** | sleep_barrier 扩面（OQ-5）、helper 名冻结（OQ-6）、439 账本 p8 条目（OQ-8）、thelper（OQ-9）、mutation spike（OQ-13） | **全部不做**。理由与综合稿 §6 一致：无缺陷类被拦、或校准成本未知、或与"改到时 drain"的账本哲学冲突 |
| **F12** | chaos 无 -race、不在矩阵（destroyer missing #5） | 综合稿只"登记缺口"。定稿**修**：B6 给 all_phases 加一个 `-race` 的 chaos 矩阵单元（22 个测试，NATS-only，不起 raft，T4 饥饿风险低）。这是 T6 的明文违反，不该只登记 |
| **F13** | 端口性质测试 I4 不变量 | 采纳 I4 订正为 cutoff 驱动（`port.go` 的 GC 按 cutoff 不按行数） |
| **F14** | B6 翻默认门槛（OQ-12） | **一轮双跑集合收据后同批翻默认**，收据贴进内审报告；保留 `-no-dedupe` 回退 |
| **F15** | 边际成本记账 | 每道新门在 §5.3 写"`make gates` 增量（落地时实测替换估值）"与"账本条目 / 排空路径"；**新门总增量上限 10s**（基线 1m31s 的 ~11%），超了就砍 AST 扫描面 |

**外审门**：本增量按 CLAUDE.md §3 走到 step 5b（测试归位）后**停在 step 6 外审**，不 commit、不 push、不 `git add`。批次顺序 B0→B6，每批独立可合入、单独过三硬闸。

---

## §0 裁决摘要（草案间实质分歧，逐条定案）

| # | 分歧 | 裁决 | 依据（含被击穿的一方、被谁用什么证据击穿） |
|---|---|---|---|
| **A1** | 竞争的产品缺陷是两条还是一条；谁先 | **只剩 Clash 一条；独立增量，不并入、不停等** | 见 §-1 F2 |
| **A2** | 目录按 phase 命名要不要改（arch A7） | **不改。冻结新增（精确集合）+ README 地图承担可读性** | destroyer/economist kill、executor 延后：CLAUDE.md §3 5b「存量的轮次标签不追溯改」明文；`legacy_process_named_list.go:30-33` 与 `batch-b2-plan.md:362-365` 两份书面裁决；承重面 73 条账本路径键 + 49 处活文档/注释引用 + 5 个门的键 + `shard.go:76`；零缺陷类被拦。arch「承重链被夸大」成立但不改变收益为零 |
| **A3** | testharness 收敛到哪 / 收多少 | **`test/stackharness`（新包，与 clusterharness 平级）；本增量只收 8 份同语义 `seedSession`；`startBroker/startAgent` functional-options 登记为条件项（触发 = 下一次 `broker.Config`/`agent.Config` 新增测试必设字段）** | quality Q3 三 critic kill：`internal/broker` 14 个 `package broker` 测试、`internal/agent` 7 个、`internal/session/session_test.go` 都 import `internal/testharness`，一 import broker/session 即环。economist：16 份不动零成本，收益只在字段变更那一刻兑现 |
| **A4** | 参数化 helper vs 重复 | **拆开：`clusterharness.go:19-25` 作为「禁止布尔模式函数」成立，作为「禁止收敛」不成立；集群 builders 永不合并，单机 stack 按需收** | 三份草案各读一半；critic 一致 |
| **A5** | fuzz 的形态 | **5 个手写/对端可控解析器新 target + `make fuzz` 逐 target 循环、包列表动态发现 + socks5 饱和语料从 `$GOCACHE/fuzz` 拷入 testdata + 周跑 CI job；不合并 proto 22→1；不做 push/PR 触发** | 三份草案都把「提交 testdata 下生成的语料」写错——`-fuzz` 只把 crasher 写进 testdata，interesting 语料在 `$GOCACHE/fuzz`（我实测：`$(go env GOCACHE)/fuzz/.../FuzzSOCKS5Reply/` 23 个文件）。FUZZ_PKGS 账本被 economist 击穿（为可动态发现的事实造账本）。22→1 合并被击穿（测的仍是 encoding/json；删 22 名要手改身份 golden） |
| **A6** | 覆盖率测不测 | **不测。不加 target/工具/CI job**；`cover-diff` 的合法用途写成 testing-standards 里的一条命令 | I9 被 kill：`.golangci.yml` 设计约束 1（行数指标反向激励删注释）+ Makefile「存在的 target 就会有人跑」——数字一存在就变目标 |
| **A7** | mutation testing 的形态 | **不建工具、不加 job、不加 target、不做 spike** | Q2 被 economist kill：≈2.3 万 mutant × broker 单包 ~100s；产出「不设阈值的幸存者清单」无人有义务处理；审查 workflow 已有「专家新增对抗测试」这一人类版 mutation |
| **A8** | 940 次冗余去不去、在哪一层 | **去，只在 runner 层**：运行时闭包 hash 全等才折叠（不等即不折）+ 首合默认关 + 双跑集合收据 → 同批翻默认。改矩阵源（arch A6）kill。前置 tag 局部性门 + 矩阵 golden | 见 §-1 F4。arch A6 被 destroyer/executor kill（改唯一全矩阵闸的源、`make e2e-one T=TestD5Matrix` 语义变、B5 note 自证删包静默） |
| **A9** | simcluster 进不进 CI | **hermetic 集 `tests/run-all.sh` 进 `make gates` 与 CI build-test（先补反向对账）；docker drill 不进 CI；周扫 timer kill** | I10 违反 CLAUDE.md:189-190「非必要绝不运行」；GPU 机每周 16–25 个 privileged 容器；周报无人认领 triage。I3 三 critic keep，executor 抓到 run-all.sh 自己漏 3 个脚本（我复核属实） |
| **A10** | 渐进 vs 专门清扫轮次 | **冻结 + 递减账本，不开清扫轮次**。唯一例外：修陈旧文件头（一次性，修完门的账本为 0） | Q5 kill（§3 5b 明文；439 次逐站点 Edit ≈3–4 人日换零行为）；`productPrefixes` 捷径被 executor 击穿（`HasPrefix` 加 "G1" 放行 G10–G19）；`^Test(G1\|G2)` 也不安全（4 条 broker `TestG2*` 是审查批次名） |
| **A11** | G1 机制化到什么程度 | **薄形态**：闸门文件 `// gate-control: TestXxx` 锚点 + 同文件存在性 + 递减账本 + 四个小门补正负样本表；不做 AST 同谓词核对 | Q1 的 AST 半被击穿（要把 26 个门重构成「不走树的谓词」才能核）；三 critic 一致 P1 |
| **A12** | sleep 立法粒度 | **一个门、一本 `path: func` 账本；扫描面 = `start*/seed*` helper 函数体（21 条）**；D9 的窄门放 `raft_timing_guard_test.go` 同文件。**替换目标订正**：就绪必须是测试真正依赖的可观测量——agent 用 `WaitNodeOnline`；broker 用 subject 的短超时 `nc.Request` 或 DB 谓词；**禁止**用 `WaitConnect` 替代 settle（它只证明 NATS 接受连接，看不见 `broker.go` register→subscribe 那条窗） | 三门三账本 = layering 四份不一致的形态；rootcause 四类根因无一是 sleep 屏障，故不批量重写；但 21 处 start* 是 T1 的精确起点且门便宜 |
| **A13** | 活源码注释里的假前提能否改 | **改**：`split.go:47-55` 事实错误直接改正；`all_phases_test.go:14-25` 原段保留、其后追加「2026-09-01 实测」段，写可复现命令不写会腐化的数字 | 「冻结不追溯」只管 `docs/reviews/`；三 critic 一致 |
| **A14** | D1 优先级与 B5 命中时处置 | **P1；命中 B5 ⇒ Skip + origin + 记 seed + 另开产品增量** | 见 §-1 F5。「200 序列×30 步 ≤10s」不可达（probe 走真实 `time.After`：inlineProbeGrace / backgroundProbeBudget 3s） |
| **A15** | test/chaos 的位置 | **注入自证补丁 + 进 -race 矩阵单元（F12）；新故障格子与 netfault 中继延后** | destroyer 发现草案假设的闸门不存在（`grep chaos all_phases_test.go` = 0；22 个 Test 无 tag）；三节点 mTLS raft 格子放 `make test` 是 T4 饥饿假红源 |
| **A16** | 全局排序 | **在途已提交，作废** | §-1 F1 |
| **A17** | e2e 恢复 push-to-main | **不改触发集，只修注释** | §-1 F7 |

---

## §1 问题与证据（只列有证据的）

**规模**：`0be2365` 上 665 个 `*_test.go`、2880 个 `Test` 函数；测试 142k 行 vs 生产 85.6k 行；`test/` 31 个顶层目录，19 个为 `p1..p13`/`d3..d9`；`test/README.md` 10 行只提 `e2e/`；`docs/testing-standards.md` 五节无任何层次定义、无「怎么改/删/合并测试」条款。

**唯一跨提交的测试身份收据不存在**：runner 的 reconcile 只在单轮内对账（`main.go:508-540`）；`layering_test.go:279-297` 记载删整行 + 删子句两种变异全绿；`promised_guard_test.go:17-20` 一装上抓到 34 条死承诺。测试代码重构没有测试保护——删错一条断言套件照样全绿。

**S3 拒绝去重的前提为假**：`internal/cluster`/`internal/broker` 在无 tag 与 d5–d9 每个 tag 下 GoFiles/Imports/TestGoFiles/XTestGoFiles md5 全等（`ff4e4637`/`62a28e76`），生产源零个 `_integration` 门控文件；D1–D4 根本不传 -tags（`all_phases_test.go:208/225/260/280`）；`split.go:50` 点名的 `d3_integration` 不存在于 `Makefile:32`。今日冗余按 Test 数估算 110×4 + 745 + 24×2 + 20 ≈ 1,253 次/轮（写命令不写数进注释）。

**租约裁决绕过已注入时钟、测试标定脱节**：`broker.go:66` 有 `Now func() time.Time`；`:1637` `probeCache.Store(... time.Now().UTC())` 与 `:1994` `adjudicateLease(..., time.Now().UTC())` 直接取墙钟（全文件仅此两处）。`:1707` `leaseGrantWindow = leaseSubscribeSettle`（5s）；而 `lease_probe_staleness_test.go:41`「only covers the first 1s」、`:60`「1.1s later — past leaseGrantWindow」，`test/p2/cloned_instance_e2e_test.go:216-223` 睡 1200ms 自称「deliberately LONGER than leaseGrantWindow」——**现在比窗口短**，该测试在验证它没描述的分支。B5 [5s,10s) 重放窗按 round2 记录仍开。

**既有闸门盲区**：`raft_timing_guard_test.go:118-135` 只匹配 `*ast.KeyValueExpr`；`internal/cluster/prevote_test.go:40-42` 用赋值语句设 `HeartbeatTimeout/ElectionTimeout/LeaderLeaseTimeout = 50ms`，既不被报也不在豁免表。G2「匹配不到目标却报完美」的形状。

**接线缺口**：`tests/run-all.sh` 头注释自述「a gate nobody runs is a gate that does not exist」，Makefile/ci.yml/test/architecture 零调用者，且自身循环漏 `r16-g67-g69-external-review.sh`、`r16-g67-g69-external-rereview.sh`、`s7-s9-external-review.sh`；`release.yml:6-8` 在 `push: tags:` 上直接 goreleaser、无 `needs`，ci.yml e2e 的 `if:` 含 `refs/tags/v` 但两 workflow 间无依赖；ci.yml:16-19 的「38 pushes/30d、2000 min」已失效（近 30 天 8 次提交、仓库 PUBLIC）。

**fuzz 预算指错地方**：`internal/proto` 非测试代码 0 处自定义 `(Un)MarshalJSON`，22 个 Fuzz 测的是 encoding/json；手写对端可控解析器零 fuzz：`tunnel.go:1556 parseRegisterLine`、`ssproxy/crypto.go:96 readChunk`、`authcallout/handler.go:270 parseRole`、`proto/subjects.go` 7 个 `Parse*`、`clusterroster/invite.go:68/:142`。Makefile/CI 无 fuzz，`testdata/fuzz` 为空；`make test` 只跑 f.Add 的 13 个种子（我实测 -fuzz 15s = 404 万次 / socks5 6s 饱和于 23 条）。

**泄漏门覆盖面**：import tunnel/pty/raft 的测试包里，`internal/clusteroffline`（21 个测试文件 0 处 NumGoroutine，restore/resnapshot 起 raft）、`internal/httplisten`、`test/d3`、`test/d6`、`test/d7` 无泄漏门；`leak_assert_shape_test.go` 只认 4 个 helper 名。

**T3 无门**：`parallel-flake-rootcause.md` 结论表根因 2「门禁 —」；`.IsLeader()` 测试站点 33 处；d7 `adminForLeader` 与 d3 手写重试是两种写法。

**就绪协议靠 sleep**：`start(Broker|Agent)|seedSession` 函数体含 `time.Sleep` 者 21 个；`test/p*+d*` 123 处 `time.Sleep(`。

**chaos 违反 T6**：22 个测试无 build tag、不在任何矩阵、只在 `go test ./...` 下无 -race 跑。

**资产腐化**：`context.Background()` 生产 44 处、`ctx-root/ctx-none` 标注 8 处（CLAUDE.md 仍写「存量 39 处」）；陈旧文件头（首注释 `// x_test.go` ≠ basename）56–61 个；`test/determinism/README.md:22/28` 仍说 `TestApplyReachabilityDeterminismLint` 是 `t.Skip`，该函数在 `.go` 里已不存在。

---

## §2 设计：革新后的测试体系

### 2.1 层次定义（写进 `docs/testing-standards.md` §零；**靠人读**，闸门只管登记表的真实性）

| 层 | 谓词（判断归属时问的问题） | 位置 | 跑它的命令 |
|---|---|---|---|
| L0 gate | 只读源码树/配置，不实例化产品运行时 | `test/architecture`、`test/determinism`、CLAUDE.md 闸门表点名的包 | `make gates` |
| L1 unit | 同包；不起嵌入式 nats-server、不建 raft 节点、不 exec 子进程 | `internal/<pkg>`、`cmd/tether` | `go test ./internal/<pkg>/` |
| L2 component | 同包；起嵌入式 NATS 或 raft 节点（不搬家、不标记） | 同上 | 同上 |
| L3 integration | `test/<topic>` 外部包，进程内真 broker+agent，无 build tag | `test/p*`、`test/d3`、`test/d4`、`test/security`、`test/chaos`、`test/cli_e2e`… | `make test` |
| L4 tagged integration | `<x>_integration` tag，只在矩阵跑；**tag 门控文件只许落在自己的 `test/<dir>/`** | `test/d5..d9` | `make e2e-one T=TestD<N>Matrix` |
| L5 matrix | 唯一允许 exec `go test` 的地方 | `test/e2e` + `test/e2e/parallel` | `make e2e-parallel` |
| L6 deploy-tier | bash，真 Docker+systemd，不进 go test | `test/simcluster/drills` | `./local.sh drill <name>`（按需）；hermetic 自检集 `tests/run-all.sh` 进 `make gates` |

**层次是测试的属性，目录只是它的地址。** 同一个 `internal/broker` 里 `adaptive_catchup_test.go` 是 L1、`reply_egress_test.go` 是 L2。三套教科书体系（ISTQB / Fowler / 四层）对 component 的定义互相矛盾，所以标准文档**不背标签**，只写"起了什么真的、打桩了什么、明确不覆盖什么"三行（`test/p6/expose_e2e_test.go` 包头是范本）。

### 2.2 目录与命名规则

- **目录名是 e2e-matrix 契约的一部分**：现有 `p*/d*` 目录**冻结为精确集合**（B4 落地时 18 个：p11 已并入）；目录消失即红、新增匹配 `^[a-z][0-9]+$` 的目录即红。不是递减账本（无排空义务），是与 TLS 配对门「今天 4」同款的精确钉死。新目录一律主题命名。
- **测试文件/函数按被测单元命名**（§3 5b 不变）；**文件首注释里的文件名必须等于 basename**（新门，账本修完即 0）。
- **溯源用 `// origin:`**；新增站点注释家族：`// sleep-fixture: <reason>`（sleep 本身是夹具）、`// gate-control: TestXxx`（闸门的正负控制表）、既有 `ctx-root:/ctx-none:`。
- helper 函数名不冻结。

### 2.3 harness 边界（写成 `internal/testharness/harness.go` 头注释与 layering 规则）

| 包 | 收什么 | import 约束（**环的根因**） |
|---|---|---|
| `internal/testharness` | 无产品依赖（auth/storage 除外）的原语：NATS/DB/WaitFor/泄漏门 | **不得** import broker/agent/session——14+7 个包内测试与 `session_test.go` 都 import 它 |
| `test/stackharness`（新） | 单机 stack 的产品依赖原语：`SeedSession`（今）；`StartBroker/StartAgent` functional options（条件项） | 可 import 产品包；只许被 `_test.go` 与 `test/` 树 import（layering 反向扫描） |
| `test/clusterharness` | 集群：RouteCA/WaitForCond/FreePort + `WithLeader`（新） | 同上；集群 builders 永不合并 |

### 2.4 三档跑什么

| 档 | 内容 |
|---|---|
| 日常（改到哪跑哪） | `go test ./internal/<pkg>/`；改手写解析器 ⇒ `make fuzz FUZZ='^FuzzX$' FUZZTIME=30s`；改 raft/tunnel/PTY/reconcile/传输 ⇒ 该包 `-race` + 泄漏门 |
| 提交前硬闸（不变） | `make test` + `make e2e-parallel` + `make lint`；改了闸门 ⇒ `make gates`（含 `run-all.sh`；golangci 全局锁，跑前确认无其他 lint） |
| CI | 每次 push：build-test（+`run-all.sh`）、lint；周一：e2e、fuzz（60s/target）；tag：e2e → **release 等三闸** |

---

## §3 逐批次改动清单

每批独立可合入、单独过三硬闸。触碰 p*/d* 套件或矩阵的批次（B3/B5/B6）额外跑 `make e2e-parallel`。

### B0 — 收据与真值修正（零行为改动）

| 项 | 来源 | 文件 | 改动 |
|---|---|---|---|
| 测试函数 identity golden | cons A1（三 critic keep） | `test/determinism/test_inventory_test.go`（新）、`testdata/test_function_inventory.txt`（新） | 每行 `<path>: <Func>`，扫 `Test/Fuzz/Benchmark`，复用 `test_naming_test.go:329` 的 `testFuncDeclRe`（不写第二份正则）；`-update-test-inventory` 只追加、拒删行（删/改名必须手改 golden + commit message 写理由——structural budget refuse-to-widen 同形，方向相反）；地板 ≥2800；按 identity 多重集比较（G4） |
| raft 计时门赋值形式 | D8（三 keep） | `test/determinism/raft_timing_guard_test.go`、`internal/cluster/prevote_test.go` | walker 识别 `*ast.AssignStmt` LHS 为受管字段选择器；prevote 三处入豁免表（T2 例外 1：原生 raft prevote 语义测试，对齐生产常量需 waitForLeader 3s→≥10s，不值）；commit message 写「扩展前 offenders 0、扩展后 3（全豁免）」 |
| 假前提注释 | I1 + A13 | `test/e2e/parallel/split.go:47-55`、`test/e2e/all_phases_test.go:14-25` | split.go 改正（删 `d3_integration`、删「不同 tag」论证）并指向 B6 机制；all_phases 原段保留、追加「2026-09-01 实测」段：`go list -deps -test [-tags …] -f '{{.ImportPath}} {{.GoFiles}} {{.TestGoFiles}} {{.XTestGoFiles}}' \| sort \| md5sum` 四包相等 + 冗余次数用 `grep -c '^func Test'` 重导出 |
| 租约注释真值 | D1(c) 前半 | `internal/broker/lease_probe_staleness_test.go:41,60` | 「1s」→ 引用 `leaseGrantWindow` 常量名；该测试实际走的分支以实跑 `reason` code 核对并写进注释 |
| CI 经济学注释 | F7 | `.github/workflows/ci.yml:5-20` | 重写为今日事实 + 可复现命令（`git log --since='30 days ago' --format=%h main \| wc -l`、`gh repo view --json visibility`）；触发集不动 |
| README 过时行 | executor missing #6 | `test/determinism/README.md:22,28` | 改为现状（函数已不存在、D2 lint 的落点是 `apply_reachability_test.go`） |
| 标准文档 | cons A3 + Q7 + arch A1 + D9 + G7 | `docs/testing-standards.md` | §零层次表（2.1）；每条 T/G/A/S 加「执行形式：闸门 `<file>` / 靠人读（原因）」；T7「围绕产品时间常量的等待必须引用常量或走注入时钟」；G7 语料规则（crasher 随修复提交；精选种子写 `f.Add`；testdata 只放机器发现输入；GOCACHE 批量语料永不提交，含拷贝命令）；A5「覆盖率是被执行的证据不是被验证的证据；审查辅助命令 `go test -coverprofile … && git diff -U0 …`」；§六 R1「删/改名 Test 走 golden 手改 + 理由」；§七 受保护资产清单（两本账本规则、originalUnion/deletedRegressionTests、TLS 精确计数、golden 只收紧、`harness.go` 环规则、`leakgate.go` goleak 否决、KNOWN REDUNDANCY 段、`logs.sh` 单映射、e2e-parallel 唯一闸、全部注释）各带 file:line |
| test/README 地图（文字） | cons A7 | `test/README.md` | 表：目录 / 层 / build tag / 跑它的命令 / 守的不变量 / 承重引用；闸门在 B4 装 |
| 基线记录 | economist missing #2/#3 | `docs/reviews/test-system-overhaul-baseline.md`（新，过程产物） | `make gates` 1m31s；`go test -json ./internal/broker/` 按测试耗时 top-20（回答「4m37s 大头在哪」，供下一增量）；30 个起嵌入 NATS 的同包文件占包耗时比例（决定 A8 永久不做） |

收据：golden 首快照与 `go test -list` 全集对账；`make test` + `make gates` + `make lint`。触碰闸门：新 `test_inventory`（CLAUDE.md 表行）、扩 `raft_timing_guard`。

### B1 — 发布与 CI 接线

| 项 | 来源 | 文件 | 改动 |
|---|---|---|---|
| release 等三闸 | I4（三 keep） | `.github/workflows/ci.yml`（新 job `release`：`needs: [build-test, lint, e2e]`、`if: startsWith(github.ref, 'refs/tags/v')`、job 级 `permissions: contents: write`，步骤原样搬 goreleaser）、**删** `release.yml`、`test/architecture/ci_workflow_test.go`（`TestReleaseJobNeedsEveryGate`、`TestNoWorkflowPublishesOutsideCI`）、`docs/usage.md` 对 release.yml 的引用 | e2e job 的 `if:` 已含 `refs/tags/v`，needs 链在 tag 上可达 |
| hermetic 闸集接线 | I3（三 keep + executor 反向对账） | `Makefile` gates（`$(MAKE) lint` 前加 `sh test/simcluster/tests/run-all.sh`，不经管道）、`ci.yml` build-test 加 step、`test/architecture/gate_registry_test.go`（`^\tsh (test/\S+)$` 行解析计入 covered）、新门 `test/architecture/simcluster_gate_set_test.go`：`tests/*.sh` 每个（除 run-all.sh 自身与被它以其他方式调用的 kept-sites.sh）要么在 run-all.sh 循环里、要么在带理由的显式排除表里；3 个漏网脚本**进循环**（F9） | 先本地跑一次 run-all.sh 记录耗时写进 recipe 注释 |

收据：`make gates` 输出含 run-all.sh 的 ALL PASS；`go test ./test/architecture/`。触碰：Makefile、CLAUDE.md 表（+2 行：hermetic 闸集、CI 发布链）、gate_registry。

### B2 — fuzz 预算重定向

| 项 | 文件 | 改动 |
|---|---|---|
| 5 个新 target（各放被测单元既有测试文件，`// origin: docs/reviews/test-system-overhaul-plan.md B2`） | `internal/tunnel/*_test.go`（parseRegisterLine 所在测试文件）、`internal/agent/ssproxy/*_test.go`、`internal/authcallout/handler_test.go`、`internal/proto/subjects_*_test.go`、`internal/clusterroster/invite_test.go` | `FuzzParseRegisterLine`（ok ⇒ 六字段格式化回去再解析得同元组；`!ok` 不 panic）；`FuzzAEADReadChunk`（不 panic、分配 ≤ maxChunk+tagSize、writer→reader 差分）；`FuzzParseRole`（永不返回 sid/nid 为空的 agent 角色）；`FuzzSubjectParseBuildFixpoint`（7 个 `Parse*`：ok ⇒ `Subj*(结果) == 输入`）；`FuzzParseInvite`（Parse→Build→Parse 不动点 + 未知参数拒绝）。`f.Add` 吸收各文件既有表驱动对抗输入 |
| socks5 语料 | `internal/proxydial/testdata/fuzz/FuzzSOCKS5Reply/*` | 从 `$(go env GOCACHE)/fuzz/github.com/LinZiyang666/tether/internal/proxydial/FuzzSOCKS5Reply/` 拷贝（今天 23 文件，以实数为准） |
| `make fuzz` | `Makefile` | `FUZZTIME ?= 60s`、`FUZZ ?= .`；`go list ./... \| xargs -n1 go test -list '^Fuzz'` 动态发现，逐 target `go test -run '^$' -fuzz "^$name\$" -fuzztime $(FUZZTIME) $pkg`，任一非零 rc 立即失败、不经管道 |
| 语料预算门 | `test/architecture/fuzz_corpus_test.go`（新） | 每 target ≤64 文件、总量 ≤256KiB、首行 `go test fuzz v1` |
| 周跑 job | `.github/workflows/ci.yml` job `fuzz`（schedule + dispatch，不在 push/PR）、`ci_workflow_test.go` 可达性 | `make fuzz FUZZTIME=60s`；`if: failure()` 上传 `**/testdata/fuzz/**`；不加 actions/cache |

收据：5 条 G1 变异在 worktree **真跑到红**（§4）；`go test -race ./internal/tunnel/ ./internal/agent/ssproxy/`；身份 golden 追加 5 个 Fuzz 名。触碰：Makefile（fuzz target）、CLAUDE.md（测试纪律加「fuzz 三态：种子回归在 make test / 周跑 / 按需 make fuzz」+ 表行 fuzz_corpus）。

### B3 — 租约时钟与产品计时（D1 + D9 + F6）

| 项 | 文件 | 改动 |
|---|---|---|
| 接注入时钟 | `internal/broker/broker.go:1637`、`:1994` | `time.Now().UTC()` → `b.cfg.Now().UTC()`（默认行为不变；**唯一生产改动**） |
| 只读访问器 | `internal/broker/broker.go` | `func LeaseGrantWindow() time.Duration { return leaseGrantWindow }`，doc 写明只供跨包测试标定 |
| 模型化随机序列测试 | `internal/broker/lease_model_test.go`（新，按被测单元命名） | 手写种子生成器（不引第三方）；事件字母表 register/subscribed/farewell/crash/beat/probeAnswer/advanceClock；四条不变量 I1 无两个 live-subscribed 实例同裸名、I2 无竞争者重启保裸名、I3 后缀名不落 DB、I4 有界时钟推进后不永远 probe_pending；`knownScenarios` 表把既有 lease 场景各写成固定 seed 序列并断言触发同一 `reason` 分支。**预算**：`probeAnswer(none)` 每序列 ≤1 步、序列 60×20，实测每步成本写进测试头；若包内新增 >30s，同批加 `Config` 探针预算 seam。**B5 预裁决见 §-1 F5** |
| p2 真值 + 过窗口测试 | `test/p2/cloned_instance_e2e_test.go` | 1200ms 测试注释改真值（restart **在**窗口内）；新增 `TestRestartedInstanceReclaimsItsNameAfterTheGrantWindow`（`broker.LeaseGrantWindow()+1s`，`// sleep-fixture:`） |
| 产品计时 sleep 门 | `test/determinism/raft_timing_guard_test.go` 同文件 `TestProductTimingSleepsReferenceTheConstant` | 同一测试函数内同时出现受管常量标识符引用与 `time.Sleep(<纯字面量>)` 即报；受管名单显式表（`claimProbeBudget/probeTTL/backgroundProbeBudget/leaseSubscribeSettle/leaseGrantWindow/DefaultLeaseGrace/DefaultStaleAfter`）；第一版只覆盖同包测试；`file:func` 递减账本 |

收据：`go test -race ./internal/broker/`；`go test ./test/p2/`；`make e2e-parallel`（p2 被碰）。触碰：`raft_timing_guard`（CLAUDE.md 表该行描述扩一句）。

### B4 — 登记与冻结闸

| 项 | 来源 | 文件 | 改动 |
|---|---|---|---|
| p11 并入 | F10 | `test/architecture/ci_workflow_test.go` 吸收 `test/p11/ci_test.go` 的断言；**删** `test/p11/`；`all_phases_test.go` allPhases 列表、身份 golden 手改 + 理由 | 先做，让后面的精确集合初值是 18 |
| test tree 地图门 + 精确集合 | arch A1 + cons A7 | `test/architecture/test_layout_map_test.go`（新） | 双向：含 .go/.sh 的顶层目录 ↔ README 表；表中矩阵名必须 `func Test<Name>(` 存在于 all_phases；tag ∈ Makefile `ALL_TEST_TAGS`；`^[a-z][0-9]+$` 目录 = 精确集合 18 |
| tag 局部性 | arch A2 | `test/architecture/build_tags_test.go` | `TestIntegrationTagsAreLocalToTheirSuiteDir`：每个 `_integration$` tag 的门控文件全在同一 `test/<dir>`；例外账本 1 条（`internal/broker/phasefluidity_lifecycle_test.go`）；门控文件总数钉死 |
| ctx 站点冻结 | cons A8 | `test/architecture/ctx_background_ledger_test.go` + `legacy_ctx_background_sites.go`（新） | 非测试 `.go` 每处 `context.Background()` 要么同行/上一行有 `ctx-root:/ctx-none:`，要么在 `path: func` 账本（初值 ≈36）；CLAUDE.md「存量 39 处」改指账本 |
| 文件头一致 | Q6 | 56–61 个 `*_test.go` 首行（**逐个 Edit**，只换文件名 token，旧名带批次信息的追加「(formerly …)」）；`test/determinism/test_naming_test.go` 加 `TestTestFileHeaderNamesItself` | 谓词：文件第一个注释块里第一条形如 `// <name>_test.go` 的行 ⇒ name==basename；**账本初值 0**（修完再装门） |
| 闸门标准元门 | Q1 薄 + Q7 键形状 | `test/architecture/gate_standards_test.go` + `legacy_gates_without_controls.go`（新） | (a) CLAUDE.md 表点名的每个门文件：`// gate-control: TestXxx` 锚点 + 该 Test 同文件存在，否则须在递减账本；本批给 `docs_layout/ci_workflow/nolint_directive/simcluster_log_oracle` 四个小门补正负样本表并从账本删行。(b) 键形状：`legacy.*` 的 map/slice 字面量必须在显式登记表 `{账本, 键单位 ∈ {file, promise, site}}` 中 |
| 就绪 sleep 冻结 | arch A4 + cons A4（窄面） | `test/determinism/sleep_barrier_test.go` + `legacy_sleep_barriers.go`（新） | AST：`^func (start(Broker\|Agent)\|seedSession)\w*` 函数体内作为 ExprStmt 的 `time.Sleep(...)`，不在 for/select、不在 WaitFor 闭包 ⇒ 屏障；`// sleep-fixture:` 豁免；账本 21 条；排空只在改到那份 helper 时 |
| -shuffle 透传 | D5 缩 | `test/e2e/parallel/main.go`、`docs/testing-standards.md` T4 | `-shuffle` 透传 go test；T4 写 ad hoc hunt 命令 |

收据：`go test -list` 全集不变（身份 golden 除 p11 手改外零 diff）；每本账本首跑数写进 commit message；`make gates`。触碰：CLAUDE.md 表（+4 行：layout map、ctx 账本、gate_standards、sleep_barrier；文件头并入命名冻结行）。

### B5 — 性质测试与泄漏面

| 项 | 来源 | 文件 | 改动 |
|---|---|---|---|
| 端口性质测试 | D6 | `internal/port/port_test.go`、`plan_test.go` | 种子随机序列 200×40、8 端口小带；I1 端口唯一、I2 (sid,name) 唯一、I3 auto=最低空闲、I4 `GC(cutoff)` 后不存在 end-of-life < cutoff 的 FREED/REVOKED 行且 ≥cutoff 一条不少；固定 seed 表复现既有路径 |
| FSM 重放等价 | D7 | `internal/cluster/crash_invariant_test.go` | 随机切点 Snapshot→Restore→重放 + 故意重放 [1..k]，投影相等；不加 t.Parallel |
| 泄漏面 | D3（缩） | `internal/clusteroffline/{restore,resnapshot}_test.go`（加共享 helper）、内联 NumGoroutine 站点改共享 helper、`test/determinism/leak_assert_shape_test.go` 加 `TestLeakGateCoversRiskyPackages`（非测试文件 import raft/tunnel/pty/yamux 的包须有共享 helper 调用；账本 file 级带理由） | -race 归属收据留 B6 |
| T3 helper + 门 | D4 | `test/clusterharness/leader.go` + `leader_test.go`（新）、`test/d7/integration_test.go`、`test/d3/*_test.go`、`test/determinism/leader_premise_test.go`（新） | `WithLeader(t, nodes, budget, fn)` 观测→执行→再观测、有界重试；门允许三形状（轮询闭包内 / WithLeader 内 / 账本）；账本初值 = 首跑数（33 左右） |
| chaos 注入自证 | D10 缩 | `test/chaos/*_test.go` | 每个注入先 `proveInjected`，失败即 `t.Fatal("injection not proven")`；断言不改 |
| seedSession 收敛 | cons A5 → F3 | `test/stackharness/seed.go`、`seed_test.go`（新）；8 个套件改 3 行转发器；`test/architecture/layering_test.go` 加反向 import 扫描（`test/stackharness` 只许被 `_test.go`/`test/` import）；`internal/testharness/harness.go` 头注释加 2.3 的环规则 | `absorbedSeedSession` 表 + `TestAbsorbedPrimitivesAreNotRedefined`（被吸收文件仍存在且不得再出现非转发定义）；d3/d4/agentprov 裸 INSERT 不动 |

收据：每包 `go test -list` identity；`go test -race ./internal/port/ ./internal/cluster/ ./test/cluster/ ./internal/clusteroffline/`；`make e2e-parallel`（8 个套件 + d3/d7 被碰）。触碰：`leak_assert_shape`（CLAUDE.md 泄漏门行补覆盖面闸）、layering 表、新 `leader_premise`（表行）。

### B6 — 矩阵去重与 -race 归属

| 项 | 来源 | 文件 | 改动 |
|---|---|---|---|
| 矩阵单元 golden | arch A3 | `test/e2e/parallel/inventory_test.go` + `testdata/matrix_units_golden.txt`（新） | 调 `splitMatrices`（同一解析器；该包 `package main` **不可被外包 import**，故必须在此）；键 `(matrix, pkg, tags, race, hasRun, runFilter)`；`-update-matrix-inventory` 只增行；「唯一 `go test` 入口」子句（test/e2e 之外 `exec.Command("go","test",…)` 即红） |
| chaos 进 -race 矩阵 | F12 | `test/e2e/all_phases_test.go` | 新增 `TestChaosMatrix`：`-race -count=1 ./test/chaos/...` |
| runner 去重 | I2 + cons A9 | `test/e2e/parallel/dedupe.go`、`dedupe_test.go`（新）、`main.go`（`-dedupe` 旗、计划打印折叠与 hash、折叠单元单一 name 以过 reconcile）、`split.go` 注释 | 分组 (pkg, race, runFilter, hasRun)；组内 tags 不同者各跑 `go list -deps -test -tags <tags> -f '{{.ImportPath}} {{.GoFiles}} {{.TestGoFiles}} {{.XTestGoFiles}}'` 取 sha256，**全等才折叠**（不等/出错即不折）；`-timeout` 取组内最大 |
| 翻默认 | F14 | 同上 | 双跑收据（无旗 vs `-dedupe`：每 (pkg,tags,race) 下 `--- PASS/FAIL` 名字集合相等、次数下降）落进内审报告 → 同批翻默认，留 `-no-dedupe` |
| -race 归属收据 | D3(d) | `test/e2e/parallel/inventory_test.go` | 风险包集合（B5 的 risky 集合）每个都被某矩阵以 -race 跑到，否则红 |

收据：dry-run 打印 `units: N -> M` 及折叠 hash；三轮 `make e2e-parallel` ALL PASS。触碰：Makefile gates 加 `./test/e2e/parallel/`、CLAUDE.md 表行（矩阵单元清单）。

---

## §4 测试矩阵（每条新闸门/helper/target：验收 + G1 变异 + G2 自检）

| 对象 | 验收断言 | G1 注入 → 哪个门红 | G2 自检 |
|---|---|---|---|
| `test_inventory`（B0） | golden 行数 = `go test -list` 全集 | 删任一 Test → 红点名 `path: Func`；改名 → 旧名缺失红；`-update` 在删了函数的树上 → 拒写非零退出 | 合成源码 3 Test+1 Fuzz+1 非 Test ⇒ 恰 4 条；地板 ≥2800 |
| raft 赋值形式（B0） | prevote 3 处在豁免 | 合成 `cfg.HeartbeatTimeout = 60*time.Millisecond` 必报；删 prevote 豁免 → 红；注释掉 AssignStmt 分支 → 合成自检红 | 合成正例（赋值）+ 负例（`c.CommitTimeout` 不在受管表） |
| release job（B1） | ci.yml 有 release job 且 needs 三闸 | 从 `needs` 删 `e2e` → 红；加回带 `tags:` 的 release.yml → 红；`if:` 写成 `refs/tags/x` → 红 | `ciJobBlocks` 既有非空自检 |
| run-all.sh 接线 + 反向对账（B1） | `make gates` 含 ALL PASS；`tests/*.sh` 全登记 | 从 recipe 删 `sh` 行 → gate_registry 红；新建 `tests/zz.sh` 不进循环不进排除表 → 反向对账红 | 合成 run-all 文本 + 合成目录列表，缺一即报 |
| 5 个 fuzz target（B2，worktree 真跑到红） | 种子在 `make test` 下照跑 | ①`parseRegisterLine` 删字段数检查 → 种子 panic 红；②`readChunk` 删 `n > maxChunk` → 分配上界红；③`parseRole` 放行空 sid → 红；④`ParseCmdBy` 接受错误段数 → 不动点红；⑤`ParseInvite` 放行未知参数 → 红 | 每 target f.Add ≥ 既有表驱动用例数 |
| socks5 语料 + 预算门（B2） | `go test ./internal/proxydial/` 回放语料 | ATYP=4 分支少读 1 字节 → 不带 -fuzz 也红、点名语料文件（若种子也抓到，**如实记录**）；放第 65 个文件 → 预算门红 | 合成 testdata 目录 |
| `make fuzz`（B2） | 退出码 0；`FUZZ=` 收窄有效 | 向 `parseRegisterLine` 注入 `panic` → `make fuzz FUZZ=FuzzParseRegisterLine FUZZTIME=10s` 红 | 动态发现结果非空（≥28） |
| 租约模型（B3） | `knownScenarios` 各触发预期 `reason` | ① `leaseGrantWindow` 改回 1s → I1 反例（drill 83 形状）；② 删 `broker.go:1629-1636`「younger 观测」守卫 → I1/I2 红；③ `probeTTL` 改 0 → I4 红；三条各自单独跑 | 生成器覆盖 ≥3 个事件类型 |
| 产品计时 sleep 门（B3） | 账本 0–2 条 | 合成引用 `probeTTL` + `time.Sleep(1100*time.Millisecond)` → 红；`time.Sleep(probeTTL/10)` → 绿 | 合成正负样本 |
| layout map + 精确集合（B4） | 全部顶层目录登记 | `mkdir test/p14` + `_test.go` → 红点名；README 删 `chaos` 行 → 红；表里写 `TestP13Matrix` → 红；删 `test/d9` 不改集合 → 红 | 合成目录名正负各 2 |
| tag 局部性（B4） | 计数钉死 | `internal/proc/x_test.go` 带 `//go:build d5_integration` → 红；删 phasefluidity 账本条目 → 红 | 复用 `treeBuildTags` 非空 |
| ctx 账本（B4） | 44 站点 = 标注 + 账本 | 加 `_ = context.Background()` 无标注 → 红；同行 `// ctx-none: probe` → 绿；账本函数改名 → 红 | 合成：上一行/同行算标注，隔一行不算 |
| 文件头门（B4） | 账本 0 | 把某头改成别名 → 红 | 合成 `// x_test.go` vs basename y |
| gate_standards（B4） | 门锚点或入账；四小门有正负表 | 删 docs_layout 锚点 → 红；锚点指 `TestNope` → 红；四小门谓词改恒 true → 各自控制表红；账本加已有锚点的文件 → stale 红 | 地板 `len(gatesSeen) ≥ 20` |
| sleep_barrier（B4） | 账本 21 | 已清理的 startBroker 加回 `Sleep(50ms)` → 红；`// sleep-fixture:` → 绿；`for { Sleep; if ok {break} }` 不报 | 合成正 3 形 / 反 3 形 |
| 端口性质（B5） | 200×40 通过，<5s | 分配跳过 taken 最后一项 → I1 红；唯一性检查 `>0`→`>1` → I2 红；GC 忽略 cutoff → I4 红 | 固定 seed 表命中既有路径 |
| FSM 重放（B5） | 投影相等 | 去掉 `index <= applied_index` 短路 → 红；Snapshot 漏 applied_index → 红 | 生成器 ≥3 op 族 |
| 泄漏覆盖闸（B5） | 风险包全覆盖 | 删 internal/agent 唯一共享 helper 调用 → 覆盖闸红；账本加不存在的包 → 红 | 风险包集合非空（≥4） |
| WithLeader + leader_premise（B5） | 站点分入三形状 | helper 单测：fake 翻转一次而重试计数 0 → 红；裸 `.IsLeader()` 不入账 → 红 | 合成三形状正例 + 裸调用负例 |
| proveInjected（B5） | 每个 chaos 测试有非空 proveInjected | 任一改恒 true → 自检红 | — |
| stackharness.SeedSession（B5） | 8 包 `go test -list` 不变 | p7 重新本地定义 seedSession → 收据红 | `absorbedSeedSession` ≥8 |
| 矩阵 golden（B6） | 首次 golden 与 `-dry-run -split` 对一次 | TestD4Matrix 删 `./internal/storage/...` → 红缺行；D1 加包不更新 → 红要求 -update；D5 删 `-race` → 红；test/p4 写 `exec.Command("go","test",…)` → 入口子句红 | 矩阵数 ≥15 |
| dedupe（B6） | dry-run 打印折叠含 hash | 给 internal/cluster 加 `//go:build d5_integration` 文件 → 不折叠（且 tag 局部性先红）；hash 改常量 → `TestClosureHashDistinguishesTagSets` 红；去掉「不等则保留」→ 红；折叠后 scheduledNames 仍按 5 计 → reconcile 报 missing | 两个不同闭包得不同 hash |

---

## §5 闸门与文档改动

### 5.1 CLAUDE.md §5 闸门表

| 动作 | 行 | 位置 | 管什么 |
|---|---|---|---|
| 加 | 测试身份清单 | `test/determinism/test_inventory_test.go` + `testdata/` | Test/Fuzz/Benchmark 名 `path: Func` 只增不减；updater 拒收缩；删/改名手改 + commit 理由 |
| 加 | test tree 地图 | `test/architecture/test_layout_map_test.go` | 目录 ↔ README 表双向；矩阵名/tag 真实性；`p*/d*` 目录精确集合 18 |
| 加 | hermetic 闸集 | `test/simcluster/tests/run-all.sh` + `test/architecture/simcluster_gate_set_test.go` | simcluster 自检脚本集；`tests/*.sh` 反向对账 |
| 加 | CI 发布链 | `test/architecture/ci_workflow_test.go` | release job `needs` 三闸；无 workflow 在 ci.yml 之外 `tags:` 发布 |
| 加 | ctx 站点冻结 | `test/architecture/ctx_background_ledger_test.go` | 新站点必须 `ctx-root:/ctx-none:`；存量递减账本 |
| 加 | 闸门标准 | `test/architecture/gate_standards_test.go` | G1 锚点 `// gate-control:` 同文件存在 + 递减账本；账本键形状登记表 |
| 加 | 就绪 sleep 冻结 | `test/determinism/sleep_barrier_test.go` | `start*/seed*` helper 体内裸 sleep；`// sleep-fixture:` 豁免；21 条递减账本 |
| 加 | T3 前提 | `test/determinism/leader_premise_test.go` | `.IsLeader()` 三形状；递减账本 |
| 加 | fuzz 语料预算 | `test/architecture/fuzz_corpus_test.go` | 每 target ≤64 文件/≤256KiB；首行格式 |
| 加 | 矩阵单元清单 | `test/e2e/parallel/inventory_test.go` + `testdata/` | (matrix,pkg,tags,race,run) 拒收缩；唯一 `go test` 入口；-race 归属 |
| 改 | 命名冻结 | 加「文件首注释文件名 = basename（账本 0）」 |
| 改 | 确定性 lint | 加「raft 计时门识别赋值形式；产品计时 sleep 引用常量」 |
| 改 | 泄漏门 | 加「风险包覆盖闸」 |
| 改 | build-tag 编译闸 | 加「`_integration` tag 门控文件只许在自己的 test/<dir>」 |
| 改 | §5 文字 | 「存量 39 处」→ 指账本；测试纪律加 fuzz 三态；「按需测试」加 `make fuzz FUZZ=… FUZZTIME=30s` |

`gate_registry_test.go` 跟着改：新增 `^\tsh (test/\S+)$` 行解析计入 covered；gates recipe 加 `./test/e2e/parallel/`。

### 5.2 `docs/testing-standards.md`

见 B0 表"标准文档"行。

### 5.3 每道新闸门的边际成本（基线 `make gates` 1m31s；落地时以实测替换）

| 闸门 | `make gates` 增量（估） | 账本条目 / 排空路径 |
|---|---|---|
| test_inventory | <1s（regex 扫 665 文件） | 无账本；新增测试跑 `-update`（同 structural budget 摩擦） |
| test_layout_map | <0.5s | 精确集合 18，无排空义务 |
| simcluster_gate_set + run-all.sh | run-all.sh 落地实测填 | 排除表 0 条（3 个漏网进循环） |
| ci_workflow 扩 | ~0 | 无 |
| ctx_background | <1s | ≈36 条；改到那行时补注释 |
| gate_standards | <1s | ≈22 条；每补一张控制表减一 |
| sleep_barrier | <1s（AST 限 21 函数） | 21 条；改到该 helper 时换可观测量 |
| leader_premise | <1s | ≈33 条；改到该测试时改 WithLeader 或入轮询 |
| fuzz_corpus | ~0 | 无 |
| 文件头 | ~0 | 0（修完装门） |
| tag 局部性 / raft 赋值 / 产品计时 / 泄漏覆盖 | 各 <1s（扩既有文件） | 1 / 3（豁免）/ 0–2 / file 级 |
| 矩阵 golden（make test 与 gates） | <1s | 无；矩阵改动手改 |

**总增量上限 10s**（F15）。

**实测（2026-09-01，内审后，44 核开发机）**：`make gates` 全程 **3m32s**（接线前基线 1m31s）。增量的构成：`sh test/simcluster/tests/run-all.sh` ≈2 min（hermetic 闸集本身，5 个 poll 类脚本等真计时器——它是被接线的既有门，不是新门）；`./test/e2e/parallel/` 整包 ≈5s（其中 `TestClosureHashDistinguishesTagSets` 跑真 `go list` 4 次 ≈1s、既有 non-split 路径测试 ≈3.5s）；`./test/stackharness/` <0.1s；新 Go 门（身份清单、layout 地图、ctx 账本、gate_standards、sleep 屏障、leader_premise、语料预算、tag 局部性、raft 赋值、产品计时、泄漏覆盖、发布链、orphan 语料、日志 oracle 谓词）在 `test/determinism`（整包 18s，落门前 ≈17s）与 `test/architecture`（整包 4.3s，落门前 ≈4s）各 <1s，合计 <10s——F15 成立。

---

## §6 明确不做（含被 critic 杀掉的提案）

| 项 | 谁杀 / 谁降 | 理由 |
|---|---|---|
| arch A7 目录迁移 | destroyer、economist kill | §0 A2；**不列为下一增量** |
| arch A6 改矩阵源去重 | destroyer、executor kill | §0 A8 |
| arch A8 component 标记 `-short` | economist kill | 只做 B0 基线测量；若 30 个嵌入 NATS 文件占包耗时 <30% 永久登记不做 |
| I9 覆盖率 | destroyer、economist kill | §0 A6 |
| I10 周扫 timer | destroyer、economist kill | §0 A9 |
| I5 恢复 push 触发 | 主进程 | §-1 F7 |
| I6 的 FUZZ_PKGS 账本 | economist | 动态发现无物可烂 |
| I7 的 proto 22→1 合并 | economist | 仍测 encoding/json；删 22 名要手改 golden |
| Q2 mutation 工具/CI/target/spike | economist kill | §0 A7；§-1 F11 |
| Q3 `internal/testharness/broker.go` | 三 critic kill | import cycle |
| Q4 样板计数棘轮 + RunAsync 原语 | economist | 原语在 startBroker 收敛延后下是死代码 |
| Q5 排空 439 账本、Q6 helper 名冻结、Q7 thelper、Q8 「不可能失败」门 | §-1 F11 | 无缺陷类被拦 / 校准成本未知 / 与账本哲学冲突 |
| D2 netfault 中继、D10 新故障格子、D5 flake 账本 | economist 条件化 | D1 落地后仍有真 TCP 时延决定的分支时再做；只做 -shuffle 透传 |
| cons A4 全树 123 处屏障扫描 | §0 A12 | 21 处零误报后再议（本增量不议） |
| spawnsafe/spawnexec 的 fuzz 目标 | 主进程 | 输入非对端可控，不属 I7 判据 |
| `.sh` 纳入命名冻结 | §-1 F9 | 只扫 .go |
| 合并集群 builders、收敛 69 处 `done <- x.Run(ctx)`、41 处 `INSERT INTO sessions`、追溯改 docs/reviews、goleak、t.Parallel、`-short` 进任何 target、全量串行 target | 各草案一致 | 已有书面裁决 |
| Clash 修复 | 独立产品增量 | §-1 F2 |

---

## §7 风险与回滚面

| 风险 | 缓解 / 回滚 |
|---|---|
| **B3 模型在现树命中 B5**：`make test` 变红 | 预裁决 F5：Skip + origin + 记 seed + 产品增量；不放宽断言 |
| **B3 模型耗时**：probe 真实预算 | 序列规模封顶 + `none` 步 ≤1；fallback = Config seam（同批） |
| **B6 是唯一全矩阵闸**：runner 四次静默少跑史 | opt-in 默认关 → 双跑集合收据 → 翻默认；`-no-dedupe` 一键回退；矩阵源只加不改（chaos 单元是加）；tag 局部性门在去重前提被破坏时先红 |
| **B1 发布延迟**：tag→release 等 e2e | §-1 F8 接受；逃生口是 dispatch 复跑 |
| **B1 run-all.sh 里 3 个漏网脚本可能本来就红** | 那是要暴露的；先本地跑一次，红的按 Mandate 呈现 |
| **文件头修改误删注释** | 只换 token、diff 逐条人读、`(formerly …)` 保留批次信息；用 Edit 不用 sed |
| **门校准误报训练人绕过**（sleep_barrier、leader_premise） | 第一版扫描面窄 + 存量全入账 + 站点注释逃生口带理由 |
| **structural budget 四维**（包文件数/代码行）| 新文件落在 test/architecture、test/determinism、test/e2e/parallel；落地前查 `testdata/structural_budget_golden.txt` 是否含这三个包，含则先看余量 |
| **golangci 全局锁** | 每批跑 `make gates` 前确认无其他 lint |
| **B2 `-fuzz` 写 crasher** | 变异验证在 worktree 跑 |
| **p2 +6s** | 一个测试、一个 `sleep-fixture`；换来 hermetic 层唯一的过窗口路径覆盖 |
| 回滚面 | 每批独立 commit；门可单文件 revert；生产改动只有 broker.go 两行 + 一个只读访问器（默认行为等价） |

---

## §8 OQ 裁决（综合稿 13 条全部定案）

| # | 问题 | 裁决 |
|---|---|---|
| 1 | arch A7 目录迁移永不 / 下一增量 | **永不主动做**；目录冻结新增。理由 §0 A2 |
| 2 | I5 触发集；2b I4 延迟 | 不改触发集（F7）；接受延迟（F8） |
| 3 | B5 现状 | 仍开（F5）；模型命中 ⇒ Skip + 产品增量 |
| 4 | p2 改写方向 | 两个都做（F6）；导出 `broker.LeaseGrantWindow()` |
| 5 | sleep_barrier 扩面；5b prevote | 不扩；prevote 豁免 |
| 6 | helper 名冻结 | 不做 |
| 7 | p11 合并 | 做（F10） |
| 8 | 439 账本 p8 条目 | 不动 |
| 9 | thelper | 不做 |
| 10 | run-all.sh 3 个漏网脚本 | 进循环（F9） |
| 11 | Clash 回归测试归属 | Clash 增量自己的 |
| 12 | B6 翻默认门槛 | 一轮双跑收据后同批翻（F14） |
| 13 | mutation spike | 不做 |

## §9 实施记录：与本 plan 的偏离（内审后补登，2026-09-01）

> 定稿后不改 §-1～§8 的文字；落地与 plan 不一致之处**全部**登记在这里（内审 §6.2 对账出 19 条、简报只登了 6 条，
> "偏离清单不完整"是内审 Fail 的理由之一）。每条写清树上实际是什么、为什么、在哪里可核。

| # | plan 条目 | 树上实际 | 为什么 / 在哪核 |
|---|---|---|---|
| D-1 | §3 B5 `WithLeader` 首批采用 `test/d7/integration_test.go`、`test/d3/*_test.go` | **d3 的 follower PIN 写已迁**（`test/d3/follower_pin_test.go`）；d7 的 `adminForLeader` 等三处**未迁**，进 leader_premise 账本 | d7 那三处是"取当前 leader 的 admin、调用方按 not-leader 重扫"的形状，迁移要改 8 个调用点，留给下一增量；`TestWithLeaderHasARealSuiteCaller` 钉住至少一个真实调用方 |
| D-2 | §3 B3 字母表含 farewell / `probeAnswer(none)`、序列 60×20、>30s 加 seam | farewell 仍不建模（在 replyLeaseVerdict，不在 adjudicateLease）；**silent 事件已加**（`silentThenRegister`，每序列 ≤1）；**seam 已加**（`backgroundProbeBudgetOverride`，仅随机走用）；**60×20 已恢复**（~6s） | 内审 L2-F1：无 silent 事件时 480 步后台探针 0 次；`lease_model_test.go` 头注释 |
| D-3 | §3 B3 / §4 knownScenarios「断言触发同一 reason 分支」 | **不可表达**：`adjudicateLease` 成功路径无 reason（code 只有三个错误码），场景断言拿到的名字 | 加 reason 是产品 seam，超出本增量；`lease_model_test.go` 头注释 INVARIANTS 段 |
| D-4 | §3 B3 不变量 I3「后缀名不落 DB」 | **删去并说明**：后缀实例会以新名再注册、行照写；plan 想说的是"contested register 写零行"，那是 broker.go 的顺序注释、不是 DB 谓词 | 同上 |
| D-5 | §3 B1 `docs/usage.md` 对 release.yml 的引用 | 内审前未改，**内审后已改**（附录 C）；并加 `TestLiveDocsReferenceOnlyExistingWorkflowFiles` | L3-F2 / L5-F4 / L6-F4 |
| D-6 | §3 B5 `harness.go` 头注释加 §2.3 环规则 | 内审前未改，**内审后已加**，并在 `layering_test.go` 加 `internal/testharness` 规则行（`originalUnion` 登记为空继承） | L5-F5 |
| D-7 | §5.1 CLAUDE.md 三行（T3 前提 / 泄漏门覆盖闸 / build-tag 局部性） | 内审前未改，**内审后已加**；另加「seedSession 收据」行；`确定性 lint` 行点名 `raft_timing_guard_test.go` | L5-F2 / L6-F5 |
| D-8 | §3 B4 四个小门补正负样本表 | docs_layout / nolint_directive 只加锚点指向既有自检（薄形式，与 §0 A11 一致）；**simcluster_log_oracle 内审后已补纯谓词 + 正负样本 + 锚点**，账本 13→12 | L5-F9 / L6-F8 |
| D-9 | §4 B3 G1 ②「删 younger 观测守卫 → I1/I2 红」 | **G1 未通过**：该守卫在单飞 probeInFlight 下不可达，禁掉后所有测试仍绿。B3 的 G1 计数是 **2/3**，简报写"3 条"是记假 | `broker.go` 守卫处与 `lease_probe_staleness_test.go` 头注释已如实写明 |
| D-10 | §7「矩阵源只加不改」 | `TestD1Matrix` 240s→300s（+clusteroffline，-race 实测 ~81s）；新增 `TestChaosMatrix`、`TestLeakRiskMatrix`（agent+tunnel 整包 -race，~30s） | 都是 -race 归属的必然结果；CLAUDE.md 该行改为「只为归属加包，不为去重改」 |
| D-11 | §3 B5 端口性质 200×40、`plan_test.go`、固定 seed 表 | 120×40（6s）；只在 `port_test.go`；无固定 seed 表（走的 G2 地板在同一测试末尾）；**模型改 token-hash 键、精确双向比较**；`PlanGCTerminated`（复制路径）不在模型内 | L2-F2 / L2-F7 / L2-F10；复制路径 GC 谓词的一致性留给 cluster 增量 |
| D-12 | §4 FSM 重放「生成器 ≥3 op 族」 | 内审前 1 族，**内审后三族**（set / dedup / poison）+ 投影含 `cluster_reqid_ledger` + 恢复后 dedup 断言 + 族地板 | L2-F3 |
| D-13 | §3 B6 闭包模板 `{{.TestGoFiles}} {{.XTestGoFiles}}`；`-no-dedupe` | `{{.Imports}}`（`-deps -test` 变体已含 _test 文件，等价且多 import 边）；`-dedupe=false` | L4-F9；`dedupe.go` 头注释 |
| D-14 | §3 B2「5 个新 target」 | 6 个（+`FuzzAEADRoundTrip`） | 正向 |
| D-15 | §3 B5「内联 NumGoroutine 站点改共享 helper」、`resnapshot_test.go` 加 helper | 只改 broker 两处；5 处内联未改；resnapshot 未改 | 留给下一增量；泄漏覆盖闸只要求每个风险包至少一处 |
| D-16 | §4 gate_standards 地板 ≥20 | 15 | 表里 `_test.go` 路径实际就这么多 |
| D-17 | §5.3 边际成本「落地时以实测替换」；§-1 F15 ≤10s | `make gates` 由 1m31s 变为**约 3.5 min**：其中 `run-all.sh` ≈2 min（hermetic 闸集本身，不是新门）、`test/e2e/parallel` ≈5s、新 Go 门各 <1s | 见 §5.3 下方实测行；F15 的 10s 指新 Go 门，成立 |
| D-18 | 简报数字 golden 2953 / `proveInjected` 9 处 / `chaos` | 2975（内审前）/ 10 处；内审后 golden 因幻影删 7、改名/删测试再手改 | 数字不再进文档正文 |
| D-19 | §六 R3「双跑 `--- PASS/FAIL` 名字集合」 | 收据实际是 (a) `go test -list` 名字集合逐字相等 + (b) `go list -race` 闭包 hash 相等 + (c) 一轮 ALL PASS；R3 文本已按此改写 | L5-F5；`docs/testing-standards.md` §六 R3 |
| D-20 | §-1 F14 保留 `-no-dedupe` | 旗名 `-dedupe=false` | 同 D-13 |
| D-21 | §4 泄漏覆盖闸「死 helper 里的调用也算」 | 内审后只认 `Test*` 函数体内（含闭包）的调用 | L1-F10 |
| D-22 | §3 B4 leader_premise 账本 5 | 扫描器收紧（循环体、IIFE、select、`.State()==raft.Leader` 都报）后 **13**（d3 迁移后不含 d3） | L1-F7 / L6-F9 |
| D-23 | §3 B6 -race 归属「import tunnel/raft 的包」 | `riskyPackages` 扫描面是 cmd/+internal/（产品包）；`test/concurrency` 直接 import tunnel，只在 RemoteFS 矩阵的 `-run` 子集下跑 -race（29 个 Test 中 14 个排除，含 4 个隧道测试）。整包 -race 实测 48s，**未加进矩阵**：该包含 T5 端口 TOCTOU 的已知瞬时 flake，先修它再进 | 内审 runner verifier；下一增量 |
