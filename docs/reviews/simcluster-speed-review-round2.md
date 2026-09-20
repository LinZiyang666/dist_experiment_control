# simcluster-speed · 内审 round 2（Block 2a / G1 / 2b + round-1 处置）

> 由主进程从 workflow `wf_0b646f15-626` 的**结构化 finding + verifier verdict** 生成（原始输出
> `tasks/wwz00bwb8.output`，key `result.lanes`；脚本 `scratchpad/review-round2.js`）。每条 finding 只保留一行
> 主张与一行处置；verifier 的完整理由在 workflow 输出里。**处置期间主树没有被审查者写过**（六个 lane 各在
> `/tmp/claude-1000/review2-R*` 的 rsync 副本里施加变异并 cmp 还原；round-1 的 moving-target 教训）。

## 0. 覆盖与方法

6 条只读审查 lane → 6 个对抗性 verifier（默认立场 REFUTED，必须自己读码 / 施加变异 / 跑测试才能 CONFIRM），
**每阶段 agent 数静态固定（6 → 6）**，所有 `agent()` 省略 `model`。审查对象 = round-1 处置之后、round-2 开始前的
工作树（HEAD a3431a1 + 未提交增量），包括 G1 g 曲线、2b（74 拆臂、73 翻臂、S2 -j6、V7 -j12、#84/#85/#86/#87）与
round-1 的 51 条处置本身。审查用时 56 min、12 agent、2.9 M token、991 次工具调用。

| lane | 视角 | findings | verifier 改判 |
|---|---|---|---|
| R1 | claim conservation / laundering | 9 | — |
| R2 | 产品正确性（#84 / #85 / #86 / #87 / #83 / H） | 7 | — |
| R3 | harness 与闸门 | 15 | R3-F2 MAJOR → MINOR |
| R4 | 变异回放 | 13 | — |
| R5 | 证据严谨性 | 10 | — |
| R6 | P 协议与台账 | 14 | — |

**结果**：68 条 finding — BLOCKER 1 / MAJOR 21 / MINOR 24 / NOTE 22；裁决 **CONFIRMED 68 / REFUTED 0**（R4-2-F13
被 verifier 部分驳回：A3b/C3b 半不成立，MA 半成立）。**全部处置**——没有 NOTED-无行动的 MAJOR，没有分期到下版的项。

处置词汇：**ACCEPTED**（按 finding 修）、**ACCEPTED-AS-DOCUMENTED**（不改行为，把事实写进代码/文档）、
**NOTED**（无行动，理由随行）、**REFUTED**（verifier 驳回）。

**本轮新增的产品变更**（与 round-1 一样，不留到下版）：
- **#89（新登记，FIXED）**：broker 自己的 NATS 客户端在本机 nats-server 被 bounce 后经 nats.go 的 INFO 发现池**漫游到 peer 的
  NATS 并永久留在那里**，且完全静默。`brokerConnectOptions` 加 `IgnoreDiscoveredServers` + `DontRandomize` + reconnect /
  disconnect 日志。这是 R1-F1 BLOCKER 的归因产物（见 §1）。
- **R2-F1 / R2-F3**：`putWithJSWatchdog.cut` 读 Put 自己的结果（nil = 上传已完成、只有 nats.go 被 ACL 拒的 trailing purge 被切；
  非 artifact 错误交分类器）。修前同名重试每次重传三遍后以 `jetstream_not_ready` 拒绝一个已完整落地的对象。
- **R2-F2**：#86 的 home 半 ② 改为 **command 域**的滞后读数（`cluster.CommandApplyLagging`，RO 池，对比最新已提交 LogCommand），
  raft 域的 `CaughtUp` 在 FSM 还在 SQLite apply 里时就读 true——正是 #86 的窗口。
- **R2-F4**：`pollClusterHealthUntil` 早退——集群模式下每个跨 home 的 expose 不再固定多付 400 ms。
- **R2-F5**：size_mismatch 路径的 `store.Delete` 上自己的 3 s deadline（此前坐满 phase 预算才 abandon）。
- **R2-F6 / R2-F7**：preflight 对 0 / 负值的措辞按 nats-server 源码改正；两种 start-joiner PAUSE 第二行带机器可读的 `PAUSE-KIND`。

**审查覆盖不到的**：deploy-tier 收据（verifier 不跑 docker）。本轮主进程在处置后跑了 5 个改过断言的单元（镜像 #14，§5）。

## 1. BLOCKER（1）

### R1-F1 ≡ R5-F2 · plan §8.3 / §8.5b′、`contention-sensors.tsv`、96 manifest · CONFIRMED
- **主张**：拆臂后 96.D 在 G1 cap 0 / cap 8 / S2 -j6 三次并发样本里打出 `PRODUCT-RED #65`（pre-heal committer snapshot =
  yes：brk1 自己的 broker.log 在 heal 之前就有 `session created … canary3`），plan 把它们记成 "#71 传感器 / LOAD-SENSITIVE"、
  registry 写 `none-after-split`——把 drill 自己定义为**决定性 #65 证据**的读数改标成一个已死的传感器。
- **处置**：ACCEPTED，**归因完成**。读 fault.sh（规则在容器自己的 netns、宿主链互拆的假设排除）、96 的 D 臂源码
  （`session created` 只由请求 handler 在 `createSession` 返回 nil 后打印）、`proposeOrForward` / `Forwarder.forward`
  （follower 的 forward 是本机 NATS 上的**广播**、5 s 超时）与 raft 时序（LeaderLeaseTimeout 500 ms）之后，brk1 在路由与
  raft 都被切的情况下 **1 s 内 rc=0** 只有一种可能：brk1 的 broker 不在 brk1 的 NATS 上。`brokerConnectOptions` 只有
  `Name` + `MaxReconnects(-1)`——nats.go 把 INFO 广播的每台 server 加进重连池、断线时先试其它条目，而 topology reconciler
  对不可 reload 的 route delta 做 staggered hard restart（负载下 reload 探针读到 stale 时触发）→ brk1 的 broker 重连到 brk2
  的 NATS、永不回来、无任何日志。它经 brk2 的 NATS 把 ctl 的 create 转给活 leader、秒回、在自己 log 里写 `session created`。
  这同时解释了 rc=0 恒成立（含 solo）、"pre-heal visible on brk1" 多数为 yes、只有并发样本有 brk1 的 commit 行、以及 R6 当年
  的"少数派认证 rc=69 50/50"。**收据**：`TestBrokerConnectionRejoinsItsOwnServerInsteadOfRoamingToAPeer`——同一脚本跑两遍，
  broker 的真实选项在 A 重启后回到 A，修前选项集（对照）漫游到 B 且 A 回来后也不回来；去掉 `IgnoreDiscoveredServers` 变异红。
  台账：**#89 新条 FIXED**；#65 保持 REFUTED；#71 加 2026-09-20 注；plan §8.3 / §8.5b′ 三格改写；registry
  `71-minority-commit default 96-mid-flight-chaos.D`；96 manifest `# forgoes: D=-`；drill 96.D 加 **D0f 前提自证**（每个
  broker 的 /connz 恰有一个 loopback 的 `tetherd`，红 = ASSERT-FAIL，且 D6b 的 #65 判定被它门控为 runtime-guard）与三 broker
  的 committer census。镜像 #14 的 96.D：D0f PASS、census brk1=no brk2=yes brk3=no、INCOMPLETE 2 = MATCH。**没有再在 deploy tier
  上复现漫游本身**（需要负载触发 hard restart）；机制由 hermetic 测试钉住，D0f 从此每次都读。

## 2. MAJOR（21）

### R1-F2 ≡ R3-F3 · kept-sites / assert-identity 臂行（X7 / §5.5 / §6.1 #2 / D-6）· CONFIRMED
- **主张**：plan 承诺的 `<drill>.<arm>` + `<drill>._shared` 行没有交付、§8 也没记录；一条 claim 在两臂之间挪动对三个门都不可见。
- **处置**：ACCEPTED。`lib/manifest.sh` 新增 `manifest_line_arms`（与 arm-lint 同一块级 parser、按行给 owner）；kept-sites
  对 manifest drill 多出每臂 + `_shared` 行（0 也出行）；assert-identity 对 manifest drill 按 `<drill>.<arm>` 键。D-6 在副本
  里复放（74 的 `B-negctrl-rc` SRAB → C）：kept-sites 红（SRAB 30→29）、identity 红（`.SRAB` 缺 / `.C` 多）；
  kept-sites-selftest 性质 3 钉住。两基线重生成并带表头注。

### R1-F3 · drill 67 注入脸只看措辞 · CONFIRMED
- **主张**：`refused 1 attempt(s) over 7m0s` 这种坐满默认预算的修后措辞能过全部措辞判据 + 非真空齿分支 3，落成校准的 MATCH。
- **处置**：ACCEPTED。注入 push 计时；**≥150 s 先判 product_red**（不看措辞）；齿分支 3 要求 `refused ≥2 attempt(s)`，1 次的
  refusal 单独记 not_covered（齿的旧注释"单次 refusal 不带计数"是错的，改正）。

### R2-F1 · `putWithJSWatchdog.cut` 丢弃 Put 自己的结果 · CONFIRMED
- **主张**：同名对象的 Put 完成后卡在 ACL 拒绝的 trailing PURGE，被切成 no-progress（transient）→ 重试再传一遍 → 三次后以
  `jetstream_not_ready` 拒绝一个完整正确的对象。
- **处置**：ACCEPTED。`cut` 读 `<-done`：nil → 成功；`context.Canceled/DeadlineExceeded/nats.ErrTimeout` → 看门狗裁决；
  其它 → Put 自己的 verdict 交分类器。`TestPutWatchdogHonoursAPutThatFinishedInsideTheDeniedPurge`（ctl-ACL 夹具，同名两次、
  1 次尝试、字节一致、看到 PURGE violation 证明走了那条路）。同名路径上仍多付一个 30 s 探测窗（延迟，不是失败；usage.md 写明）。

### R2-F2 · #86 home 半 ② 在它为之而写的窗口里是瞎的 · CONFIRMED
- **处置**：ACCEPTED。`cluster.CommandApplyLagging`（RO 池 + `newestCommittedCommandIndex` 跳过 noop/config）、broker
  `applyLagOf`（`!CaughtUp || cmdLagging`）、`homeApplyLagging` 包级函数（Broker 方法预算 285 已满）。
  `TestCommandApplyLaggingSeesAFollowerWhoseFSMHasNotAppliedTheEntry`（applyCommitGate 卡住 FSM：`CaughtUp==true` 且
  `lagging==true`）、`TestApplyLagReadsBothDomains`、`TestUnknownTokenIsTransientOnlyOnALaggingClusteredReplica`（R4-2-F7）。
  三条变异红。台账 #86 ② 与 `expose.go` 注释改写。

### R3-F1 ≡ R4-2-F5 · teardown-recovery-nonvacuity 的 DEADLINE 检查真空 · CONFIRMED
- **处置**：ACCEPTED。sed 范围只看 `_budget_left` 函数体，另一行钉 `DEADLINE=` 存在；M9 复放红；plan §8.6 的"全红"改成
  "M9 在 round-1 是假收据"。

### R4-2-F1 · 探针的 leaderless 半未钉住 · CONFIRMED
- **处置**：ACCEPTED。`probeJetStreamForBucket` 收 `streamLookup` 窄接口；测试用假 stream handle 直接驱动探针（leaderless →
  失败、有 leader / standalone → last_seq）；`if false && …` 变异红。

### R4-2-F2 · pushTierB 的看门狗 + 重试接线未钉住 · CONFIRMED
- **处置**：ACCEPTED。`TestPushTierBWiresTheWatchdogAndTheRetryLadder`（AST：恰一个 `putWithJSRetry(putCtx, attemptPut,
  jetStreamUnavailableFace, jsPutRetryAttempts, jsPutRetryBackoff, sleepCtx)`、恰一个看门狗调用且钟为三常量、probe 闭包恰调一次
  `probeJetStreamForBucket`）；(a)(b) 两变异红。

### R4-2-F3 · witness 3 无独占行 · CONFIRMED
- **处置**：ACCEPTED。加 "BLOCKED on promote, CATCHING_UP still in the timeline" 行；`range e.Timeline[:0]` 红。

### R4-2-F4 · ping 门的 KEPT-hint 检查被任一处满足 · CONFIRMED
- **处置**：ACCEPTED。`installHintLiteralRe` 否定模式（任何 `log "…"` 行带字面 ping 值即盲）+ 多处 hint 合成控制；真
  install.sh 247 行手抄变异红。

### R4-2-F6 · R2-F4 守卫概率性 · CONFIRMED
- **处置**：ACCEPTED。新行在 confirming probe 里取消（`sends == cutoverAttempts+1`），删 post-probe 检查 30/30 红。

### R4-2-F7 / R4-2-F8 / R4-2-F9 · #86 home 半、agent handler、commit-REFUSED abandon 未钉 · CONFIRMED
- **处置**：ACCEPTED。home_test 两行（调用点 `if false` 红）；`TestHandleExposeForwardedRetriesATransientHomeAndReportsTheTransientCode`
  （真 nats.Msg + scripted adapter：预算 0 红、恒 frpc_failed 红）；`TestPushTierBCommitRefusedSendsFailedFinalize`（共享
  `commitPathFixture`；`_ = abandon` 红）。

### R5-F1 · S2 sweep 相 36 min 是错的 · CONFIRMED
- **处置**：ACCEPTED。按 progress.tsv 重算：sweep 1719 s ≈ 28.7 min、attribution ≈33 min；§8.5b′ / §8.5b″ / drill-costs 表头
  改；-j6 → -j12 的真实杠杆 −14%，不是 −32%。

### R5-F3 · "hermetic 2 节点 R2 复现" 无 artifact · CONFIRMED
- **处置**：ACCEPTED-AS-DOCUMENTED。plan §8.5c、台账 #84 与 `transfer.go` 的探针注释改为"2026-09-19 一次未保存的临时实验，
  是设计理由不是收据"；有记录的证据（67v/67w /jsz 采样、谓词 + 探针测试）单列。没有重做实验：它会证明的（leaderless 应答 =
  失败）已由 R4-2-F1 的探针级测试钉住。

### R6-1 · #33 "by #80" 未收据 · CONFIRMED（≡ R1-F5 / R5-F6 MINOR）
- **处置**：ACCEPTED。台账 #33 标题 → "已修复：症状…；机制归因 #80 为 CANDIDATE"，正文写观测到的机制（nats.go 池重连 +
  重注册；session 重建行在健康样本上红过）；plan §8.5b 读法改写；drill 73 拆成硬断言 + 门控机制观测（R6-4）。

### R6-2 · #70 标题的 CANDIDATE 是给门读的、理由为假 · CONFIRMED（R3-F2 同族）
- **处置**：ACCEPTED。#70 标题 OPEN（30 行本来就拥有它）；`lib/ledger.sh` 成为**唯一**的标题状态读者（trailing group、
  逐子句、交叉引用截止子句、code span 空白化、英文词整词），ledger-crosscheck 与 validate-verdicts 都读它（R3-F7）；
  S-7..S-10 自测；README / registry / 台账的 "weekly" 改成事实（没有机制按周跑）。

### R6-3 · #34 台账没碰 · CONFIRMED
- **处置**：ACCEPTED。#34 重标题（只剩面 1）+ 2026-09-19/20 块：三面读数、cap 8 复现、rc=64 面 = #86 的重归因、harness 假红、
  候选机制两条（含 R6-13 的 M3 rotate）、owner；drill 74 gap 文本只认面 1；两条从未发火的 band 退役（R1-F8 / R6-14）；
  log.md ## 74 补拆臂后收据。

### R3-F2 · ledger-crosscheck 子串匹配 · CONFIRMED，MAJOR → MINOR（verifier）
- **处置**：ACCEPTED（见 R6-2）。

## 3. MINOR（24）

- **R1-F4**（67 第一次成功尝试不计时）→ ACCEPTED：attempt 1 无论结果都计时；≥100 s 的成功也是 product_red（12 MB 是秒级）。
- **R1-F5**（#33 ② 从未观测到）→ ACCEPTED（并入 R6-1）。
- **R1-F6**（96 parent/.A 的 `-`）→ ACCEPTED：A2 的 post-restart 记录改 runtime-guard（与 pre-restart 同类）→ .A 钉 4、parent 7；
  镜像 #14 的 96.A nc_gap=4 = MATCH。
- **R1-F7**（98 水位重试方向论证错）→ ACCEPTED：注释改正；水位读到后**重断**自证 C（C′）。**副产物**：新日志暴露了 S2 两次
  SETUP-RED 的真因——`node ls` 不带 `-a` 在 agt1 已 STALE 时返回 `nodes: []`；`_hb_of` / 水位读改 `-a`；98 solo INCOMPLETE 1
  pass=14、水位一次读到。
- **R2-F3**（cut 洗掉 Put 自己的错误）→ ACCEPTED（同 R2-F1 修法）：`TestPutWatchdogReturnsThePutsOwnErrorHeldByTheDeniedPurge`
  （本地 I/O 错 → 原样、不重试）。
- **R2-F4**（每个跨 home expose 多 400 ms）→ ACCEPTED：`pollClusterHealthUntil` 早退，只在 waited > step 时打日志；
  `TestPollClusterHealthUntilEndsTheGatherWhenTheAnswerIsIn`（early exit 变异 401 ms 红）。
- **R2-F5**（size_mismatch 的 Delete 坐满预算）→ ACCEPTED：`tombstoneUploadedObject` 在 `cmd.Context()` 下 3 s deadline（不新增
  `context.Background()` 站点）；`TestTombstoneUploadedObjectReturnsOnItsOwnDeadline`（phase ctx 变异 20 s 红）。
- **R3-F4**（`grep -c … || echo 0` 的 `0\n0`）→ ACCEPTED；arm-aggregation H-1 钉"无 integer expression expected"。
- **R3-F5**（`worst: 0` → `timeout 0`）→ ACCEPTED：unit 展开时校验 worst/grows；H-2。
- **R3-F6**（attempt2 跳过 DRILL-ARM 校验）→ ACCEPTED：`effective_verdict` 按 unit 键（去 `.attemptN`）；H-3（变异 → LOAD-SENSITIVE 红）。
- **R3-F7**（validate-verdicts 仍位置式读闭合）→ ACCEPTED：读 `lib/ledger.sh`；selftest 的 #99 夹具闭合词挪到标题。
- **R3-F8**（arm-lint R6/R4/R2/重复键）→ ACCEPTED：`exit([[:space:];&|)}]|$)`、setup_fail 命令位、grows 下界 =
  `manifest_shared_grow_floor`（共享 fixture 的第一个 grow 站点）、重复键红；selftest 四行。lint 的块级 parser 与
  `manifest_line_arms` 一并修正为"整行注释不算代码"（本轮 96 的一条列 0 注释里的 "the case" 曾把它绊倒）。
- **R3-F9**（drill 内 wrapper 函数把 N 条 claim 折成一行）→ ACCEPTED：描述位是变量（裸或引号内整变量）即 BLIND；selftest shape 5。
- **R3-F10**（sidecar 不清理 / grow-done 晚截断）→ ACCEPTED：清理列表加 `.tmo/.grow-done/.timeline.tsv`；父进程入 lane 前截断。
- **R3-F11**（--replay 拆臂前归档无 LEGACY-UNIT；缺 log = CONTRACT-ERROR）→ ACCEPTED：LEGACY-UNIT 路径 + 缺 log = INFRA-ABORT；H-6/H-7。
- **R3-F12**（D-11 r9d 臂 oracle 未交付）→ ACCEPTED：74 SRAB/C、96 A/D/F 五节（175 proved）+ `# arms:` 对账。
- **R4-2-F10**（size-aware floor 未测）→ ACCEPTED：recording floor 通道断言 4/2 chunk 的字节数；`floorFor(0)` 红。
- **R5-F4**（transfer.go 把 120 s 归到 "image #7 receipt"）→ ACCEPTED：注释改为有记录的证据（67v/67w）+ 未保存实验的定位。
- **R5-F5**（67x 的 194 ms 不在记录里；67v 是另一 profile）→ ACCEPTED：§8.5c 订正块；67 成功行 600 字符。
- **R5-F6**（plan 第三合取项名字错）→ ACCEPTED（并入 R6-1）。
- **R5-F7**（51@cap0 / 91@cap8 的归因是推断）→ ACCEPTED：§8.3 / 台账 #70 写成推断；51 非 0 rc 的 grow 输出整段落盘。
- **R6-4**（73 把 fixture 相关性当硬合取）→ ACCEPTED：拆成硬断言 + 门控观测 + gap（见 R6-1）；镜像 #14 -j5 与 solo 两次都 PASS。
- **R6-5**（#86 ↔ [#29 续] 未互指；计数 3/7 错）→ ACCEPTED：4/8；两条互指；50/51/52 的 `--on-broker` 绕法登记为待办（未跑，不假称已判）。
- **R6-6**（usage.md / transfer.go 两张脸）→ ACCEPTED：usage.md 三张脸 + R2-F1/F3 注；`attemptPut` 与 `putWithJSRetry` 注释重写。
- **R6-7**（log.md 无 52/60/81/94 段、无结案）→ ACCEPTED：九段 2026-09-20 段 + 2026-09-04 节结案行。

## 4. NOTE（22）

- **R1-F8 / R6-14**（74 两条 band 从未发火）→ ACCEPTED：退役（bands → `-`），下次以 DEVIATION 归因。
- **R1-F9**（drill-costs 非完整 run 的行）→ ACCEPTED：8 行从完整样本重种、列名 `secs`（n=1）、表头写明。
- **R2-F6**（preflight 0/负值措辞）→ ACCEPTED：0 = 服务端默认 2m/2、负 = 每个客户端首个 ping 即被关（verifier 订正了 reviewer 的
  "负值按原文行为"）；preflight_test 四行。
- **R2-F7**（两种 PAUSE 同签名）→ ACCEPTED：第二行 `PAUSE-KIND: start-daemons | booting`；runbook §1 写明；
  `TestStartJoinerHintSaysRestartNotStart` 断言两行互斥。
- **R3-F13**（73 conn==home 前置当断言）→ ACCEPTED（并入 R6-4）。
- **R3-F14**（67 sampler 上界算错、无 trap）→ ACCEPTED：跑到循环结束、EXIT trap 回收。
- **R3-F15**（runner 边角）→ ACCEPTED：argv 重复单元折叠（H-4）；单臂 manifest 也出 ARMS 行（H-5）；README 的 DRILL-ARM 措辞
  改为"证明 runner 请求的臂到达了 drill_end；分支正确性由 arm-lint R4 保证"。
- **R4-2-F11**（`ListObjectsShowDeleted` 注释夸大）→ ACCEPTED-AS-DOCUMENTED：注释改为"对 referenced 集合无影响、留着让循环看见
  tombstone"。
- **R4-2-F12**（`!initRan` 冗余）→ ACCEPTED：加 init=true+全真行使其承重（变异红）。
- **R4-2-F13**（§8.6 MA/A3b/C3b 是编译红）→ 部分 REFUTED（verifier：只有 MA 是字面编译红）→ MA 行注明"编译红；行为等价变异亦红"。
- **R5-F8**（74C1 "7 s 内发火" 非正向证据）→ ACCEPTED：plan 改写。
- **R5-F9**（地址池实验无 artifact、算术不一致）→ ACCEPTED-AS-DOCUMENTED：31 − 3 = 28 ≠ 观测 29，两者都写、不假装一致；
  verifier 按 moby ipamutils 复核 15+16=31（reviewer 的 32 是错的）。
- **R5-F10**（八处小数字）→ ACCEPTED：lg2 51 INCOMPLETE、W_grow 加数是 cap 8 并发时长、67u loadavg 18.9、00-skeleton 是 4ea2b89、
  C 5/5、45 vs 47.4 各指其物、§8.5c 标题"四张镜像三张脸"、7 行表。
- **R6-8**（#84 留 owner 列"给门看"）→ ACCEPTED-AS-DOCUMENTED：理由改正（门只遍历 OPEN）。
- **R6-9**（#85 "应看到孤儿组消失"）→ ACCEPTED-AS-DOCUMENTED：hermetic 钉住、deploy-tier 未观测（floor 比任何 drill 窗都长）。
- **R6-10**（DOC-25 无地址）→ ACCEPTED：DOC-n 表加 DOC-25（CLOSED）。
- **R6-11**（#83 只记一个样本）→ ACCEPTED：7 个 GREEN。
- **R6-12**（#88 de jure 无 owner）→ ACCEPTED：96.F owner 列点名 #88（parent/.D 的 owner 文本同步）；状态写成"5/9 未归因"。
- **R6-13**（#34 面 1 的 M3 rotate 候选机制）→ ACCEPTED：写进 #34 待查方向。

## 5. 处置之后的 deploy-tier 收据（镜像 #14 = 本树 + 全部 round-2 产品修复；`./local.sh --build build`）

改过断言的每个单元各跑一次（`run-drills.sh --no-retry --no-attribute`；第一批 -j5，73 因 Q-xcheck 与我误留的 attribution
pass 撞车、又因一次 provision 残留 flake 重跑，最终 solo）：

| 单元 | verdict | 与期望 | 备注 |
|---|---|---|---|
| 67 | INCOMPLETE 1 pass=19 | MATCH | 注入脸 70 s rc=75 `refused ≥2 attempt(s)`（R2-F3 修后 chunk-ack 的瞬时错误被真的重试到）；CONTROL(after) 第一次 1 s |
| 98 | INCOMPLETE 1 pass=14 | MATCH | 水位一次读到（`-a`）、C 与 C′ PASS、same-PID 重建路径 |
| 96.A | INCOMPLETE nc_gap=4 nc_guard=1 | MATCH（新 pin 4；parent 7） | |
| 96.D | INCOMPLETE 2 pass=26 | MATCH | **D0f PASS**；census brk1=no brk2=yes brk3=no |
| 73（-j5） | ASSERT-FAIL Q-xcheck | — | `[#33 FIXED]` + `[#33 mechanism]` 都 PASS（23 s，pre=brk2 post=brk1）；Q-xcheck 一次读 `brk1≠brk3` = 第 3 个样本 → 改成 60 s poll |
| 73（solo，poll 后） | GREEN pass=47 | MATCH | AUTO-RECOVERED 27 s，pre=brk3 post=brk1；Q-xcheck PASS |

## 6. 本轮硬闸与收据

见 plan §8.8（round-2 之后的复采）。
