# batch B2 — 债务清理增量的内审综合报告（5 lane 审查 + 5 lane 对抗性核验）

> 综合者：主进程之外的 synthesizer agent。输入 = 10 份 lane 报告（全部完整，无缺失/截断）：
> `review-{A1-budget,A2-keys,B1-drill,B2-agenttest,X-honesty}.md` + 对应 5 份 `verify-*.md`。
> 审查对象：Part A = commit `54f125a`（A1 观测预算 / A2 三张豁免表 re-key）；
> Part B = 工作树未提交改动（B1 drill-41 gap 恢复 / B2 新 agent hermetic 测试 / B3 plan §10.1–10.3）。
> 所有 lane 的变异实验都在 `/tmp/*` 私有副本里做，`git status --porcelain` 全程只有那三条 Part B 路径 —— 我复核过，共享树未被污染。

---

## 1. 结论（verdict）

**NOT READY for external review.** 产品代码本身没有发现缺陷 —— 10 个 lane 里没有一条 finding 指向
`internal/**` 的行为错误，A1 的谓词组成、锁、A2 的 51↔51 数据迁移都被两组独立方法证明是正确的。
但这是一次**"给门加牙"的债务清理**，而它的两个核心机制各有一个被实测复现的洞：

1. **A2 的新 key 在 `cmd/tether` 里不是单射的**（两个独立成因），后果是**静默吸收**一个全新的
   unresolved 站点 —— 三个门全绿。旧的 `file:line` key 失败时是**响的**（红 → 重编号）。
   这一项不修，A2 就不是"把 key 变稳"，而是**把一个吵闹但诚实的 key 换成了一个安静的 key**。
   正确实现在同一个 commit 里、30 行之外的 `internal/auth` 就有。
2. **Part B 的三份持久记录没有跟着改**：`expected-verdicts.tsv:28` 仍是 `INCOMPLETE / 2`，
   `expected-verdicts-log.md:82` 与 `simcluster-coverage-boundary.md:61`（"**不 launder**：不假装物理断连稳定发生"）
   仍在描述改动前的世界。下一次 `run-drills.sh` 会把 drill 41 判成 **DEVIATION**（确定性，非概率）。

外审前的**硬前置**只有这两条。其余 22 条 finding 都可以带着走或在同一轮里顺手清掉。
第 3 节列出的 6 条被驳回/夸大的 finding 同样重要 —— 它们下一轮不该被重新提出。

---

## 2. 存活的 finding（verifier-CONFIRMED + NEW），按严重度排序

严重度是**核验后的和解值**：lane 与其 verifier 分歧时取证据支撑的那一档，并在行内注明。
"BLOCKER" 一栏为空 —— 唯一一条 BLOCKER（B1-F1）的技术论证被其 verifier 逐条驳回，见第 3 节。

### MAJOR

| # | 位置 | 一句话失败场景 | 证据强度 |
|---|---|---|---|
| M1 | `cmd/tether/error_code_coverage_test.go:405` + `:420` | `curFunc = fd.Name.Name` 丢掉 receiver，且 FuncDecl 结束时 `funcSiteOrd` 归零 —— `clusterstatus.go` 新增第二个 admin backend 类型、其 `HandleCluster` 回 `Response{Code: e.Code()}`，这个**全新的动态 code** 被 `HandleCluster#1` 的豁免静默吸收；若该 code 无 exit class 就是 exit 70，而 `docs/usage.md §9.13` 让自动化对 70 无限重试 —— 正是这张表存在的理由 | **最强**。review-A2 与 verify-A2 各自独立在**真实树**上复现：加一个同名方法 → 三门全绿；**只改方法名**的对照 → `TestErrorCodeCoverage` 红并点名 `verifierProbeHandle#1`。另有 4 例合成用例 + 2 个正对照 |
| M2 | 同上（`<file-scope>` 桶） | **无需同名碰撞**：任意两个 file-scope 站点被任意一个 `func` 隔开，就都 key 成 `<file-scope>#1`。加一条豁免永久盖住两个站点 | verify-A2 在**真实 `clusterstatus.go`** 上复现：两个不同的包级 unresolved 站点 → 一个 key |
| M3 | `internal/broker/clusterwrite.go:535-543`；`internal/broker/cluster_observe_budget_test.go:118` | `observeStreamCountForBudget` **零个测试引用**（我复核了 grep：只有 clusterwrite.go 的 4 处）。新测试手工拼 `clusterReplicaObserveBudget(b.observedStreamCount())`，从不调用真正做组合的那个函数。删掉消费分支 = 修复的**逐字节回退**，`internal/broker` 整包绿 | **最强**。review-A1（283s）与 verify-A1（292s）各自独立跑通；verify-A1 另加**正对照**：删*存储*半边（plan §10.2 声称验过的那个变异）确实红。即"验过的是无关的那一半" |
| M4 | `test/simcluster/expected-verdicts.tsv:28`；`expected-verdicts-log.md:82`；`docs/reviews/simcluster-coverage-boundary.md:61` | `classify_match`（`run-drills.sh:513-549`）：`GREEN != INCOMPLETE` 且 bands=`-` → 落到 `printf 'DEVIATION'`。下一次全套 sweep 丢掉历次报告都在依赖的 "NO DEVIATIONS" 性质；更糟的是 coverage-boundary.md 留着 "不 launder" 的**站着的指令**与 drill 的硬断言正面冲突 | **4 个 lane 一致 CONFIRMED，我亲手复核了 tsv 行与 coverage-boundary 第 61 行**。维护者看不到它：`d41fix.log` 是单 drill 运行，从不查表 |
| M5 | `internal/agent/roster_proactive_rehome_test.go:63-67, 98-101, 117` | wiring 测试唯一的正向断言是**过定的**：hermetic 连的是 loopback，`rosterContainsHost` 不过滤 undialable 而当前 roster 循环过滤 —— 喂一个**逐字节相同**的 roster 也照样 `rebuildRequested=true`。有人接受兄弟测试的邀请去修这条不对称，两个测试变红，其中一个宣布"**wiring 回归**"并指向 drill 41 —— 正是这个文件为终结的那个假叙事 | **4 个 agent 独立复现**（review-B2 / verify-B2 / review-X / verify-X）。verify-B2 试图找一个"能救 case 1 的另一种修法"，结论是**结构性耦合、不存在**。修法是 2 行（`PublicHost:"self.example"` + loopback 放 `NatsRoute`），仓库里 `established_agent_retries_test.go:49-58` 已有先例 |
| M6 | 同上：`:49-62` 的理由段 | "hermetic 测试到不了 LEAVING 臂"**是事实错误** —— `rosterRequiresReconnect` 过滤 `PublicHost` 但匹配 `PublicHost` **或** `NatsRoute` 主机名。后果不是措辞问题：LEAVING 臂（每次真实 `cluster shrink` 都走）在 CI 跑的所有层里**没有 wiring 覆盖**。将来有人在 leaving 分支前加短路，removal 臂照常、新测试照绿、`make test` / `make e2e-parallel` 照绿 | verify-B2 用**该文件自己的 helper** 端到端复现 LEAVING 臂可达；`grep -rl "refreshRosterOnce\|RosterRefreshOnly" test/` → **无命中**（e2e 层也不碰） |
| M7 | `internal/broker/alert_reconcile.go:191` → `clusterwrite.go:392`（唯一写者）；消费者 `clusterstatus.go:273` **无 leader gate** | 预算修复**只在 leader 上生效**。follower 上 `observedStreamCount()` 终生为 0，`observeStreamCountForBudget` 永远返回 `sessions+1` —— 正是被谴责的那个项。3 broker / 1 ACTIVE session / 60 个孤儿 `OBJ_xfer-*`：leader 上 `cluster status` 得到真实测量，两个 follower 得到 `stream_actual: -1`。而 commit 与 §10.2 都把回退描述成"首轮" | 两 lane 独立 CONFIRMED；两个 verifier 都把 MAJOR 降为 **MODERATE**（fail-closed、严格优于改前、非回归）。**残留的诚实缺陷是那句"首轮"** —— 在 follower 上是"每轮" |

### MINOR

| # | 位置 | 一句话失败场景 |
|---|---|---|
| m1 | `internal/agent/roster.go:398`（`accepted &&`）| 该 conjunct 未被任何测试钉住 —— `_ = accepted` 变异下 `internal/agent` **整包**绿。落后的 follower 给刚 home 到新 broker 的 agent 发一个 pre-add 世代 roster → `adoptRoster` 按单调世代规则拒绝 → 没有这个 gate，被拒的 roster 仍喂给策略层 → removal 臂 → `nc.Close()` + session cancel，**每个 refresh tick 一次，永远**。verifier 复现于包级并把 MAJOR 降为 MODERATE（"缺一条断言"而非"守卫错了"）|
| m2 | `test/determinism/raft_timing_guard_test.go:53/:59` | **第四张 line-keyed 豁免表**，且是最弱的一张：无 stale-entry 检查（`exempt` 只在 `:107` 被读），reason 还按行交叉引用（"same site as :509"），一行上有**两个**被守护字段。在 `transport_test.go` 上方插 3 行 → 门红 → 机械重编号；日后有人把一个**真的** `HeartbeatTimeout: 50ms` 放到 512 行，静默继承豁免。但 verify-X 用 plan **§4 row 6** 直接驳掉"没人看过它"的框架（我复核了 `batch-b2-plan.md:607`：已登记 + 已给缓解"不得在 transport_test.go 增删任何一行"）→ 事实成立、severity 从 MAJOR 降到 MINOR |
| m3 | `cmd/tether/error_code_coverage_test.go:85, 86, 92, 103` | `unclassifiedCodeAllowlist` 里 **4 条散文行引用今天就是错的**（`error_hints.go:158-166`→实际 `:263`；`error_hints_test.go:80`→`:81`；`xfer_inflight.go:504`→`:655`；`expose.go:262-264`→`:233`）。读者去审 `home_broker_restart` 为何免分类，落到 `ledgerRowInputs` 的结构体字段上，于是要么当豁免过期删掉、要么重推一遍整个论证。verify-X 补一条更重的：`xfer_inflight.go:504` 是 plan §4 row 2 **书面承诺要同步**的那一条，而站点正是在**本批次的** `0e9c5d5` 里移动的 —— 登记了、派活了、还是漏了 |
| m4 | `internal/broker/cluster_observe_budget_test.go:29,44,49,59,65,73`；`clusterwrite.go:428`；`clusterstatus.go:266` | 改名只到常量为止：钉关系的测试仍叫 `...ScalesWithSessions`，失败信息仍说 "per-session term is not wired"（点名一个已不存在的常量）、"session count"。commit 自己的诊断是"**旧名字正是错配藏身之处**" —— 那个名字还留在钉关系的门的输出里 |
| m5 | `internal/broker/broker.go:588`；测试 `:132-139` | "UNOBSERVED 报告不得覆盖缓存"这条断言**咬不住它点名的守卫**：探针 `ReplicaReport{Observed:false}` 同时满足 `len(Streams)==0`，删掉 `!rep.Observed` 半边后整包绿（两 lane 各自跑通）。且它宣称防的路径（meta-not-ready tick）根本到不了 `cacheReplicaSnapshot` —— 挡住它的是 `if err == nil`。将来有人相信这条断言而删掉 `if err == nil` |
| m6 | `internal/broker/broker.go:602` | **NEW（verify-A1）**：新测试只把计数往上驱动（0→10），所以分不清"记录当前值"与"记录历史最高值"。改成 high-water mark 后 `-run 'Budget\|Observe\|Replica\|Metric'` 全绿 —— 而 lane 报告自己声称的"计数会往下跟"性质，代码与测试里**哪儿都没断言** |
| m7 | `internal/broker/clusterwrite.go:536` | **NEW（verify-A1）**：增长窗口是新项**小于**被它替换的旧项的唯一情形。leader 缓存=1，突发 100 session + 100 `OBJ_xfer-*`，在下一次观测**完成**前每次预算都是 `budget(1)=3.25s`，而改前会从活 DB 读出 `budget(100)=28s` —— 约 **8 倍更少**，方向正是 commit 要修的那个。文档 `:558-560` 已声明该滞后（故非缺陷），但"下一 tick 自愈"两头都低估了：trainer tick 是一次**无界**全量 walk，且在 follower 上永不自愈 |
| m8 | `cmd/tether/error_code_coverage_test.go:227-235`（并 `:612`、`:616`）| **NEW（verify-A2）**：被 re-key 的文件里仍有三句话说自己是 line-keyed，其中一句就坐在 `unresolvedCodeSites` **内部**，明确说这个改动"**不在本次做**、留给下一个读者"。改动就在它上方 80 行做完了。文件在跟自己吵架，而physically 贴着数据的那一方是输的一方 —— 下一个加豁免的人在 use point 读到 `// exempted at this exact line`，写下 `foo.go:412`，被 shape 断言用一条根本不提行号的信息拒掉 |
| m9 | `internal/agent/run.go:170`/`:213`、`transfer.go:89` 三条 `#N in this function` | **verifier 把 PLAUSIBLE 升为 CONFIRMED**（review-A2 F6）：ordinal key 声称"函数内增删站点会变红并强迫重读"，但对**一增一减相抵**的编辑为假 —— verify-A2 在真实 `handleRunForwarded` 上做到了：一个站点变可解析 + 同函数内新增一个 unresolved，N 不变、每个 key 仍活，**三门全绿**，而四条 reason 现在各自描述另一个物理站点。这正是"把动态 reason 换成字面量、别处新增一个动态的"这种普通重构的形状 |
| m10 | `internal/agent/roster_proactive_rehome_test.go`（`TestProactiveRehomeIsBlindToAnIPConnectedURL`）| **NEW（verify-B2 N1）**：记录下来的局限其实是**字符串同一性盲**，不是 IP 盲 —— 任何不在 roster 里的连接主机名都盲（vanity/CNAME `broker_url`、与 `public_host` 用了不同别名的 `cfg.NATSURL`）。而结合 F5：shipped 形态 `host:"127.0.0.1"` + 远端 agent 走 `wss://<domain>:443`，**IP 那个触发条件在现网拓扑里基本不可能发生**。于是测试名成了 grep 锚点，却告诉未来的人"只有 IP 才需要修" |
| m11 | `test/simcluster/drills/41-shrink-to-standalone.sh:174-176` | **NEW（verify-B1）**：drill 注释把 `nats_topology_*` nudge 当**实测事实**写下（"The wake-up is the … nudge"）。两份 journal 只证明了**效果**（`rosterRequiresReconnect` 触发，且两次运行同偏移）；没有任何 journal 或 broker 日志记录该 publish 或其接收。§10.3 自己的教训是"未经验证的降级会被后续三轮当作既成事实转述"—— 这是同一个错误换个符号，而且写在下一个读者会引用的地方 |

### NIT

| # | 位置 | 要点 |
|---|---|---|
| n1 | `cluster_observe_budget_test.go:121-123` | 死断言：117 行已 `t.Fatalf` 要求恰好为 10，121 行再测 `<=1` —— 任何变异都到不了，是一条穿着 `if` 的注释 |
| n2 | `alert_reconcile.go:192-211` | 降级的 leader 不重置 `lastReplicaStreams`：曾在 500 流时当 leader、之后集群缩到 2 流，`cluster status` 仍按 500 流定预算（旧代码是活 DB 读、立刻收缩）。verifier 收窄了影响：预算是上限不是 sleep，只在 JS meta 无响应时才真花时间 |
| n3 | `clusterwrite.go` 常量 | 上限在 `(30s-3s)/250ms = 108` 流处生效，即带 transfer 的 session 约 54 个就饱和 —— 目标工况（transfer-heavy）上两个项给同一答案。附带：review-A1 F8 的 "never less" 分句被 m7 推翻 |
| n4 | `cmd/tether/error_class_test.go:52-69` | 那条**存在目的就是证明 site-scoping** 的回归测试对 M1/M2 结构性盲：注释说"同一**文件**内的站点不得互相遮蔽"，fixture 却把两个站点放在同一**函数**里 —— 那是 ordinal 唯一处理正确的排布。改两行 fixture 当初就能在写下缺陷的同时抓到它 |
| n5 | `internal/auth/acl_reconcile_test.go:663` vs `:700-702` | 豁免扫描只覆盖 `internal/broker`，而它保护的 `subscribedSubjects` 覆盖 `internal/broker` + `internal/proto`。今天潜伏（`internal/proto` 非测试文件里没有 `Subscribe(`）|
| n6 | `internal/auth/acl_reconcile_test.go:582-588, 686-692` | auth 的 ordinal 是单射的（安全方向），但两处注释描述的语义不是它实际计算的（同名碰撞时它跨函数计数）。若按 M1 建议把 auth 形状抄到 cmd/tether，注释要一起改，或把 receiver 放进 key |
| n7 | `roster_proactive_rehome_test.go:193`、`:98-101` | fixture 注释描述了该路径根本不碰的状态（`rosterRefreshNow` 是死赋值，删掉仍绿）；而 `cfg.RegisterTimeout` 才是承重的。表把 fixture 形状耦合到期望字段（`if !tc.wantRebuild { seedBrokers = removed }`），第三行会静默拿错 fixture |
| n8 | `broker.go:584-586`、`clusterwrite.go:387-388` | 两处注释仍说缓存"是给 /metrics gauge 用的"，而新字段注释说"**不是** metric，这是保留它的唯一理由"。这两句正是将来动 `observeAndCache` 的人会先读的 |
| n9 | `internal/broker/audit.go:26-29` | `pubSysEvent` 的 "falls back to core publish otherwise" 只对"从来没有 JS"成立；`publishAudit` 走 JS 分支时 2s 超时后**直接返回错误、无 core 回退**，而 `topology_reconcile.go:154-160` 对 `eventKey` 无条件存盘，失败的 publish 对该 `(gen|action|reason)` 永不重试。`audit.go:25` 已声明 "Always best-effort"，故为已文档化的取舍 |

---

## 3. 被驳回 / 被夸大的 finding（下一轮不要再提）

| 原 finding | 判决 | 为什么 |
|---|---|---|
| **B1-F1 BLOCKER** —"这两条断言历史上跑红过、仓库已把它归类为 timing flake、而且窗口被从 210s 收窄到 60s" | **技术论证被驳回；降为 MAJOR，只剩文档义务** | ①"同一份代码"**是假的**：lane 用 `git log -S rosterRequiresReconnect` 只数了一个标识符；`55b1451`(07-21，在 07-20 那些红**之后**)重写了这条路径为 `rebuildOntoVoter` 并加了整个 `#48` silence-rebuild 路径 —— **而且 `55b1451` 本身就是把这两条臂换成 gap 的那个 commit**，所以每一次红都早于当前代码。我核实了 `rebuildOntoVoter` 确由 `55b1451` 引入。②证据不是一次捕获而是**两次独立运行**，延迟 57.595s / 57.683s，**相差 88 毫秒** —— 在 lane 自己的 full-jitter (0,180s] 模型下概率 ≈0.1%，它用自己的统计推翻了自己的推断。③两条臂在 `d41fix.log` 里都**没有** `poll_until: condition met after Ns` 行（`lib/log.sh:110` 仅在 elapsed≥5s 时打印）→ 60s 预算实际消耗 **<5s**，"只剩 3s 余量"的算术是错的。④"一个 timer 两套标准"（60s vs 第 245 行的 300s）是范畴错误：300s 覆盖 `#48` **silence-rebuild** SLA，这条臂覆盖**事件驱动 proactive** 路径。⑤lane 倚重的 coverage-boundary 条目里那个 GREEN 数据点是 `r15v1`，而 `r2-plan.md §21.1` 明写 `r15v1/r15v2 结果作废`。⑥"窗口被收窄"只对一半成立：**臂 #2 是 30→60，是放宽**（我核实了 `02913d9` 的两个原始预算）。**存活的只有程序性那半**：`coverage-boundary.md:61` 必须同一次编辑改掉，且该文档要求的"两轮全套稳定性"用两次单 drill 运行不算 —— 即 M4，不是回退恢复的理由 |
| **B1-F5** —"臂 #1 在 /connz 不可达时 fail-open，能出假绿" | **REFUTED（只剩卫生建议）** | 机械事实成立（jq 在空/坏 JSON/空数组下都非 0），但**这一对断言产生不了假绿**：臂 #2 是 fail-closed 且**正向要求** agt1 出现在某个**当前 VOTER** 的 /connz 上，而此时 brk2 已 `voters 3→2` + `op_state=RETIRED`；NATS 客户端只有一条 session 连接，故"在 voter 上"蕴含"不在 brk2 上"。lane 自己的残留场景（monitor 不可达 **且** agent 没走）恰好让臂 #2 红 |
| **B1-F3** —"oracle 不区分它点名的机制"（CONFIRMED/MAJOR）| **降为 PLAUSIBLE / MINOR** | 抽象论点公允，但以 CONFIRMED 声称"至少四种机制都能在 60s 内产生同样观测"却零证据表明任何替代机制在此触发：机制 3（~20s 断连看门狗）需要一次**断连**，两份 journal 都没有；机制 2（`hardRestartNatsServer`）门在四个合取条件上，一个都没观测到；而 `rebuildOntoVoter` 的 WARN 字符串全树唯一，在两份 journal 里都出现在同一偏移 ±100ms 内。其建设性建议（顺手断言那条 WARN 行）值得采纳 —— 但那是改进，不是已落地内容的缺陷 |
| **B1-F4(a)** —"57s 什么也证明不了，32% 概率下很普通" | **REFUTED** | 见上 ②。F4(c)（nudge 有损）作为 **NIT** 成立（n9）|
| **B2-F4** —"M9 `RosterRefreshOnly: true→false` 存活 → 全车队写风暴，而新测试不会眨眼" | **头号变异被 REFUTED** | verify-B2 在**包级**跑同一变异：`roster_runtime_test.go` 三个测试红（`TestRosterRefreshConvergesNoRaft` 存在的目的就是断言 `refreshOnlySeen != 0`）。lane 报的 `ok ... 0.050s` 是 `-run` 限定的单测运行 —— 它测的是"新测试抓不到 M9"，报成了"M9 存活"。那个后果**已经被 `make test` 拦住了**。M10（`RosterGen→0`）确实存活，但影响远低于框架：`maybeEmitRosterStale` 在 `agentGen==0` 时早返回，损失仅一条可观测性事件。残留有用的部分：`serveRosterRefresh` 从不检查 `msg.Data` —— 作为建议成立 |
| **X-F3** —"第四张表**没人看过**" | **框架被驳回（事实成立）** | plan **§4 row 6** 已按 file:line 登记该表并给出明确处置（"不得在 `transport_test.go` 增删任何一行"）—— 我核实了 `batch-b2-plan.md:607`。这是**已文档化的取舍**，不是盲区。存活的是更窄的一句：§10.2 写下"要去找它的兄弟"这条教训，却没回头看自己 §4 早已索引的那个兄弟 → m2，MINOR |
| **X-F6 / A2-F4 的归因与框架** | **部分更正** | `alloc_failed` 的行引用是**继承的腐烂**：`52d3b80` 时 `:264` 是对的，在 `808552d`（本次 cleanup 前两个 commit）移到 `:233` —— 不是 `54f125a` 造成的。且 commit 那句"prose cannot rot either"在它自己的句子里就限定为 `unresolvedCodeSites` **内部**那三条互指 map 站点的 reason，与 `unclassifiedCodeAllowlist` 引用**产品源码**是两个范畴。事实（4 条今天就错）不受影响，只是修辞被拉伸了 |
| 其他 severity 校准 | — | A1-F2 / A1-F3 / A1-F7 / A1-F8 与 A2-F3 / B2-F2("dishonest" 一词) / B2-F3 均被各自 verifier 判为**事实成立、严重度或某个支撑分句偏热**。特别地：A1-F3（"日后把 trainer 那次调用也加上预算会造成自锁"）的失败场景以"if someone later …"开头 —— **树里不存在的未来编辑**，属 NIT/文档级；A1-F8 的 "never less" 分句**是错的**（见 m7）。反向一例：verify-A1 认真考虑过把 M3 判为 OVERSTATED（"是门的牙，不是产品缺陷"）并**明确否决** —— 因为本仓库的成规就是"每条新守卫都要对它声称能抓的缺陷做变异验证"，而 §10.2 验的是无关的那半边 |

---

## 4. Lane X 的完备性判决：哪些债真的关了，哪些只是换了标签

| 债 | 声称 | 实际 | 依据 |
|---|---|---|---|
| **D1** 观测预算忽略 `OBJ_xfer-*`（外审 doubt 3）| 关闭，只剩 250ms 常量未刻画 | **部分修复** | 谓词组成完备、锁完整（两 lane 各自穷举：唯一写者、12 个 `lastReplica*` 站点全在锁内、无第四个贡献者、无截断报告路径）。但：leader 上一次 reconciler pass 之后才对（M7），follower 与新选出的 leader 上仍是旧的少计；且 doubt 3 真正点的是**上限区间**（n≥108 饱和于 30s、模型自洽阈值 n>120），而上限未动。§10.2 的"债 1 清掉"应作**部分修复**。verify-X 补一条公允：维护者在外审原答复里已承认最坏情形是"可见的 routinely unobserved 而非静默误判为已收敛" —— 所以不是偷换新说法，只是 frame sentence 过宽 |
| **D2** line-keyed 豁免表（外审 doubt 4）| 2 张表 + 1 条 shape 断言 re-key，"找到第三处"，双向变异验证 | **数据上 FIXED，机制上有缺陷；且扫兄弟差一张** | 51↔51 严格双射被**两种不同方法**独立重建（一方重实现扫描器双发 key，一方把赋值改成存旧 line key 再与 `54f125a^` 的旧表 join）：0 丢、0 得、48 条 reason 逐字节相同、3 条 retarget **全部正确**。"插在站点上方仍绿"（17 个文件各 +37 行）与"函数内新增站点变红"（`HandleCluster#11`）都复现。shape 断言从 1 臂变 4 臂，严格更强，且无法被空表满足。**但**：`cmd/tether` 的新 key 不是单射的（M1/M2），而正确实现就在同一 commit 的 `internal/auth` 里 —— 一个 commit 造出了同一套 key 方案的两个实现，一个安全一个不安全；第四张表（m2）与 6 条散文引用（m3）未扫 |
| **D3** drill 41 两处 `not_covered(gap)`（外审 doubt 5）| 以行为证明关闭，GREEN pass=33 | **drill 里 FIXED，ledger 里没有** | 方向是 `not_covered → assert_ok`（真强化）；`kept_sites` 31→31、baseline 28、`--check` OK；四个 anti-inflation 门（lint-drills / validate-verdicts / kept-sites / ledger-crosscheck）全绿；单 hunk 无越界。§10.3 的每个头条数字（62ms、~57s、hostname 非 IP、pass=33、not_covered=0、product_red=0）都从捕获物里复现，**无一注水** —— 这是三节里证据最扎实的一节。但三份持久记录仍描述改动前的世界（M4），且**仓库里没有这次 GREEN 的证据留痕**：日志在 repo 外的 scratch 目录，plan 未引 run id |
| **D6** `rosterRequiresReconnect` 的 IP 盲 + previous/current 栅栏不对称 | **明确"只记录、不修"** | **诚实的重新标注，而且做对了** | 两条都是**会在被修好时自毁**的可执行陈述（`t.Fatal("…apparently been fixed — good. Replace it…")`），两条都有各自定向变红的变异（M8 只红那一个测试），两条局限都被独立追到 shipped `nats.conf`/`getConnectURLs`/`preflight.go` 层面证实为真 —— **不是冻结 bug 的恒等式测试**。唯一走偏是 M5（fixture 问题，非 pattern 问题）与 m10（记录的是最不可达的那个实例）|
| **无测试被删/被弱化** | — | **成立，且被验到 body 级** | 测试函数清单 `54f125a^`→`54f125a`：2387→2388，**0 删除**，1 新增。verify-X 把这条 name-only 的扫描扩展到两个被重写的 **body**：`error_class_test.go` 的 shape 断言 1 臂→4 臂，`acl_reconcile_test.go` 的 walker 保留双向且仍 4/4 解析。没有任何测试为了变绿而被掏空 |

**一句话**：三笔债里 **1 笔真关（D2 的数据迁移）、2 笔部分关（D1 只在 leader、D3 只在 drill）**，
外加 1 笔**模范级的诚实重标注（D6）**。没有一笔是"改个标签就算关了"。

---

## 5. 本增量里风险最高的改动

**A2 对 `cmd/tether/unresolvedCodeSites` 的 re-key（51 条）。**

理由，按权重排：

1. **它是唯一一处"用新机制替换一个能工作的旧检测机制"的改动。** A1 改的是一个项的算法（产品正确，问题在门没牙）；
   B1 是把 gap 换成断言（方向是强化，四个 ledger 门都能量它）；B2 是净新增覆盖的新文件（最坏情况是新测试不够好）。
   只有 A2 把一张**在役的门**的 key 整体换掉 —— 51 条豁免同时改锚点。
2. **失败模式从"响"变成"静"。** 旧 `file:line` key 腐烂时**必然变红**（站点上方插一行 → 门红 → 机械重编号），
   这确实烦人，但它诚实。新 ordinal key 在同名碰撞或 file-scope 桶里**静默吸收**一个全新的 unresolved 站点：
   三个门全绿，diff 里什么都看不出来。**用一个吵闹的错换来一个安静的错，是本增量里唯一一处严重度净上升的交易。**
3. **它守的东西离运维最近。** 一个未分类的 wire code → exit 70 → `docs/usage.md §9.13` 告诉自动化对 70 无限重试。
   相比之下 m2 那张 raft timing 表守的是"测试夹具要引用生产常量"，假豁免的后果是一个跑着生产不会跑的节奏的 harness。
4. **触发条件不冷门。** `HandleCluster` 是**接口方法**，"在 `clusterstatus.go` 里加第二个 admin backend 类型"
   是对该文件最可能的编辑；而 M2 连同名碰撞都不需要 —— 任意一个 `func` 把两个包级站点隔开就够了。
   `internal/broker/clusterdrain.go` 今天已有四个同名 `Error()` FuncDecl，形状离命中只差一个 `Code:`。
5. **正确实现在同一个 commit 里、30 行之外。** `internal/auth/acl_reconcile_test.go:593-621` 用 per-file
   `map[string]int` 且不重置，同一个探针下**变红**。一次 commit 写出同一套 key 方案的两个实现、一个单射一个不单射，
   并且把不单射的那个用在了守护更关键契约的那张表上 —— 这不是取舍，是遗漏。
6. **本该抓到它的回归测试结构性盲**（n4）：`TestExternalReviewErrorCodeGateReportsEveryDynamicSite`
   的注释说"同一**文件**内的站点不得互相遮蔽"，fixture 却把两个站点放同一**函数** —— 恰是 ordinal
   唯一处理正确的排布。缺陷与"证明缺陷不存在"的测试是同一次编辑写下的。

**修法明确且便宜**：把 `internal/auth` 的形状抄过来（per-file `map[string]int`，不在 FuncDecl 结束时重置），
并把 receiver 类型放进 key（这样 key 读起来也不含糊）；同时把 n4 的 fixture 改成跨函数（review-A2 F5 已给出
可直接落的 `TestUnresolvedSiteKeysAreInjectiveAcrossFunctions`，含 3 个用例）。加上 M4 的三份记录同步，
这个增量就可以交外审。

---

## 附：审查卫生

- 10 份 lane 报告全部完整，无缺失、无截断；无任何 lane 报告"无法完成"。
- 5 个 verifier 合计 **REFUTED 2 条**（B1-F5、B2-F4 的头号变异）、**部分驳回 2 条框架**（B1-F1 的技术论证、X-F3 的"没人看过"）、
  **CONFIRMED 其余 30 条事实**、**调低 8 条严重度**、**调高 1 条**（A2-F6 从 PLAUSIBLE 到 CONFIRMED）、**新增 8 条**。
- 每个 lane 都交了"我试过但没打破"的清单；其中若干条本身是有价值的**否定结果**（谓词组成完备、锁完整、
  0 与真零流可区分、无截断报告路径、无第五张 line-keyed 表、`IsUndialableHost` 无额外不对称、
  两条 recorded-limitation 都不是恒等式测试）—— 这些不该在下一轮被重新推导。
- 我本人复核过的载荷事实：`git status`（仅三条 Part B 路径）、`expected-verdicts.tsv:28`、
  `simcluster-coverage-boundary.md:61`、`batch-b2-plan.md:607`、`raft_timing_guard_test.go:52-61`、
  `observeStreamCountForBudget` 的零测试引用、drill 两条臂的新旧预算（210→60 / 30→60）、
  `rebuildOntoVoter` 由 `55b1451` 引入。
