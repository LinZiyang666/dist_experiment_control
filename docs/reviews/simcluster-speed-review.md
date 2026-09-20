# simcluster-speed · 内审 round 1

> 由主进程从 workflow `wf_33c77646-fd1` 的**结构化 finding + verifier verdict** 生成（原始输出
> `tasks/wvctsifk0.output`，key `result.lanes`）。本文每条 finding 只保留一行主张与一行处置；
> 完整的 verifier 理由在 workflow 输出里，不在此复述。

## 0. 覆盖与方法

6 条只读审查 lane → 6 个对抗性 verifier（默认立场 REFUTED，必须自己读码 / 施加变异 / 跑测试才能 CONFIRM），
**每阶段 agent 数静态固定（6 → 6）**，所有 `agent()` 省略 `model`。审查对象是 Block 0 / 1a / 1b / 2a 与 #83
的未提交工作树（快照 ≈15:30–15:52，审查期间主进程仍在编辑——多条 finding 的 verifier 注明「主树已含修复」）。

| lane | 视角 | findings |
|---|---|---|
| R1 | claim conservation / laundering（期望值改动是否把缺陷洗成 gap） | 6 |
| R2 | 产品正确性（`cluster add` 恢复、preflight、tier-B 传输） | 6 |
| R3 | harness 与闸门（能否被静默拆掉） | 9 |
| R4 | 变异回放 + lint | 9 |
| R5 | 证据严谨性（plan §8 的数字与收据） | 9 |
| R6 | P 协议与台账 | 12 |

**结果**：51 条 finding — BLOCKER 2 / MAJOR 5 / MINOR 25 / NOTE 19；裁决 CONFIRMED 50 / REFUTED 1
（R4-F9）。两条 BLOCKER（R1-F1 = R6-F1 同一件事；R4-F1）与全部 MAJOR 均已处置。

**审查覆盖不到的**：deploy-tier 收据本身（verifier 不跑 docker）。因此 §2 里每条「收据」都是主进程在
审查之后单独跑出来的，drill 名 + 镜像号 + 秒数可在 `drill-costs.tsv` / plan §8 反查。

处置词汇：**ACCEPTED**（按 finding 修）、**ACCEPTED-AS-DOCUMENTED**（不改行为，把事实写进代码/文档）、
**NOTED**（无行动，理由随行）、**REFUTED**（verifier 驳回）。

## 1. BLOCKER（2）

### R1-F1 ≡ R6-F1 · `test/simcluster/expected-verdicts.tsv` 67 行 · CONFIRMED
- **主张**：把 67 的 Put-leg stall 记成 coverage gap（`INCOMPLETE 2`）正是 plan X27 / §5.2 禁止的洗白——
  一个「每次降级 JS 推送都坐满预算」的 broker 会零偏差地匹配期望。
- **处置**：ACCEPTED。① drill 67 改为对 Put-leg 的 `nats: timeout | context deadline exceeded | no responders`
  判 `product_red #84`（两张脸 + post-recovery 脸），expected 先改 `PRODUCT-RED 1 #67 #84`；② 登记
  `deploy-tier-gotchas.md #84`；③ **产品修复**（不留到下版）：`cmd/tether/transfer.go` 的 Put 跑在
  `putWithJSWatchdog`（STREAM.INFO 探针、3×10 s）之下，探针必须**在 ctl 的 ACL 之内**且**拒绝无 leader 的
  stream**；瞬时脸（`no responders` / `no response from stream` / 503）经 `putWithJSRetry` 有界重试 3 次
  （3 s / 6 s）后以 `jetstream_not_ready` + retry 提示拒绝；两条路径都经 abandon 释放槽位。
- **钉住**：`TestPutWithJSWatchdogCancelsAStalledPutOnlyAfterStrikes`、`TestProbeJetStreamForBucketStaysInsideTheCtlACL`
  （负控制证明 fixture 真拒 `$JS.API.INFO`）、`TestProbeJetStreamForBucketRejectsALeaderlessStream`、
  `TestPutWithJSRetryRetriesOnlyTheInstantJetStreamFaces`、`TestJetStreamUnavailableFaceClassifiesPutErrors`。
- **收据**：镜像 #6 67 `INCOMPLETE 1 pass=18`（stall 脸被 watchdog 在 36 s 内截住）但 CONTROL(after) 第 1 次
  因探针打 `$JS.API.INFO` 违反 ACL 而被误切；镜像 #7 修 ACL 后注入脸变成瞬时 `no responders`（0.9 s）→
  PRODUCT-RED，post-recovery 脸坐满 121 s；镜像 #8 两张注入脸都处理（`INCOMPLETE 1 pass=18` 270 s）但
  post-recovery 仍 121 s——drill 的 /jsz + /connz 采样把第三张脸钉死（chunk 在选举窗里被静默丢弃、此后 stream
  健康而 `last_seq` 不动）；镜像 #9 探针加进度半边（`stallNoProgress`，可重试）→ **67 `INCOMPLETE 1 pass=18`
  187 s，CONTROL(after) 一次 push 内恢复**。四张镜像的四次 RED 各是一条**新事实**，不是同一条没修好
  （plan §8.5c）。同一条线上顺带发现并修掉的 #85（失败 Put 的无 meta chunk 组无人回收）见 plan §8.5d。

### R4-F1 · `cmd/tether/cluster_add_drive.go:1009` 等 · CONFIRMED
- **主张**：`make lint` 在增量上是红的：2× errcheck（`defer conn.Close()`）+ 1× nilerr（`resumeBlockedJoinWith`）。
- **处置**：ACCEPTED。errcheck 两处改 `defer func() { _ = conn.Close() }()`；nilerr 处保留「best-effort 预检、
  `waitJoinServing` 负责重试」的语义并以带理由的 `//nolint:nilerr` 落基线（`expectedDirectives` 35→36，
  `test/architecture/nolint_directive_test.go`）；顺手清掉 ineffassign / wastedassign。`make lint` 0 issues。

## 2. MAJOR（5）

### R2-F2 · `internal/natsconf/preflight.go:88` · CONFIRMED →MAJOR
- **主张**：`passthroughValueCheck` 接受负的 `ping_max` / 负 duration；nats-server `-t` 放行，上线后每个客户端
  在第一次 ping 就被踢。
- **处置**：ACCEPTED。`ping_interval` 必须是 >0 的 duration，`ping_max` ≥1；`preflight_test.go` 增 4 条负例。

### R3-1 · `test/simcluster/tests/assert-identity.sh:97` · CONFIRMED
- **主张**：assert-identity 与 kept-sites 看不见 `drills/lib/setup-forcesingle.sh` 里的 8 个 claim 站点——一条
  共享守卫可以被整段删掉而两门全绿。
- **处置**：ACCEPTED。两份扫描都加 `$DRILLS/lib/*.sh`，键 `lib/<name>`；基线重生成并在文件头注明来源
  （`lib/setup-forcesingle 8`）。

### R4-F2 · `internal/broker/transfer.go:1502` · CONFIRMED
- **主张**：`handlePushCommitReq` 里的 `markCommitted` 接线无测试：删掉它全绿。
- **处置**：ACCEPTED。`TestPushCommitHandoffMarksTheEntryCommittedBeforeForwarding`（`transfer_finalize_test.go`），
  删 `markCommitted` 块即红（已变异验证）。

### R5-F1 · plan §8.2 · CONFIRMED
- **主张**：§8.2 把 hermetic 并发窗口归到 30a 一个单元；硬时间戳落在 42a 尾部 + 30a + 30b。
- **处置**：ACCEPTED。§8.2 按时间戳改写，三个受污染样本全部标注、不再当作 A/B 证据。

### R6-F2 · `docs/deploy-tier-gotchas.md` · CONFIRMED →MINOR
- **主张**：P-a / P-b 机理未进台账，plan §5.2 0a 的验收「机理写进台账」未满足。
- **处置**：ACCEPTED。#84 条目是那条「新号」，正文点名 P-a（abandon 路径 / `claimAbandonedPush`）与
  P-b（`phaseTimeoutFor` 的 size 派生预算）各自在这条缺陷里扮演的角色。

## 3. MINOR（25）

| id | 位置 | 主张（一行） | 处置 |
|---|---|---|---|
| R1-F2 / R3-2 / R4-F3 | `test/architecture/nats_ping_defaults_test.go` | ping 门只钉 `PING_INTERVAL_S`、不钉 `PING_MAX`，drill 腿缺席即静默放行；四份拷贝只对账三份 | ACCEPTED：drill 腿**必需**且两键都读、都比；控制测试扩到「半对盲 / 字面拷贝盲 / 两种漂移都看见」 |
| R1-F3 | `drills/98:226` | RECOVERY 合取仍有「发现→注入之间连接已迁走」的真空窗 | ACCEPTED（→NOTE）：注入后、取水位前加自证 C：`_cut_still_holds_agt1`（CUT_BROKER 的 /connz 此刻仍持有 agt1）；nonvacuity 门钉住其位置与形态 |
| R1-F4 / R5-F6 | `drill-costs.tsv` | plan 说改了、树里没改；98 行理由过时 | ACCEPTED：按单元写入 solo-2026-09-19 的临时再播种行（96.A/D/F、67、74.SRAB/.C、42、10、98）+ 文件头注明「S2 sweep 后整体重播」 |
| R2-F1 / R4-F7 | `cluster_add_drive.go:572` | `resumeBlockedJoin` 对**每个** CATCHING_UP 之后的 BLOCKED 都 confirm，与自己的注释矛盾 | ACCEPTED：只对 `LastError` 以 `broker.OpBlockedCatchupDeadlineMsg` 开头的 BLOCKED confirm（常量由 broker 导出，两侧不再各写一份文案）；测试增 AddVoter-exhausted 不 confirm 行 |
| R2-F3 / R6-F4 | `transfer.go:358` | commit 请求传输失败 / 回复畸形 / `!cr.OK` 都不 abandon，槽位被占到预算尽头 | ACCEPTED：三条路径分别 `abandon("commit_lost" / "commit_reply_malformed" / cr.Code)`；`TestPushTierBCommitLostStillSendsFailedFinalize` |
| R3-3 | `lib/log.sh:26` | 保护 sidecar 不可写的 `\|\| true` 无任何门保护 | ACCEPTED：timeline-test 增 `set -euo pipefail` 下不可写 sidecar 的用例，删 `\|\| true` 即红 |
| R3-4 / R4-F5 | `tests/teardown-recovery-nonvacuity-test.sh` | 「一个预算」检查匹配 0 次而空转；不钉 SERVER-SIDE 证据行；不钉「排除 CUT_BROKER」半边 | ACCEPTED：精确钉 2 处 `poll_until "$(_budget_left)"`、0 处 `poll_until "$RECOVERY_BUDGET"`、SERVER-SIDE 断言存在、`_budget_left` 由 DEADLINE 派生、CUT_BROKER 排除行存在（变异 M6–M9 红） |
| R3-5 / R4-F4 | `scripts/install.sh:229` | KEPT 提示手抄 `20s`/`2` 在门之外；任一 `^ping_interval` 存在即整段压制 | ACCEPTED：`NATS_PING_INTERVAL`/`NATS_PING_MAX` **定义一次**，模板与提示都引用它；KEPT 报告三种形态（缺键 / 值不同 / 一致）；门读定义 + 模板 + 提示三处 |
| R3-6 | plan §6.3 H-7′ | 验收项「重装后 `.new` 含两键且原文件字节不变」未交付 | ACCEPTED：drill 32 增 H-7′ 臂（8 条 claim，含 45 s 值差 fixture）；收据镜像 #6 `GREEN pass=62` |
| R5-F2 | plan §8.2 | 10a/10b「−97 s vs p50 305 s」把 7 月 −j6 sweep 与 solo 混算 | ACCEPTED：改用 plan 自己的 solo 基线（spine3：grow brk2/brk3 分段时刻） |
| R5-F3 / R6-F7 | `broker-ops.md:927`、`install.sh` | 服务端陈旧连接窗口写成 60–80 s / 6–8 min，与本增量收据（58 s / ≈4:00）矛盾 | ACCEPTED：`broker-ops.md §8.10` 与 install.sh 注释改为实测数（≈4 min 实测 4:00；新配置 ≤64 s）；plan §8.1c 同步 |
| R5-F4 | `internal/agent/conn_liveness_test.go:113` | 阳性对照声称钉 (ping_max+1)×interval，实际放行 (ping_max+3)×interval | ACCEPTED：改为**精确计数**——nats-server `ping.out+1 > MaxPingsOut` 才关，哑客户端恰好看到 `MaxPingsOut` 个 PING，`==` 断言；读 deadline 收到 (max+2)×interval（首 tick ≤20 % 抖动）。变异「服务端 max+1」→ 红（saw 3 PINGs） |
| R5-F5 | plan §8.1c | 「IMPACT 均 4:00 整」高估来源：4:03 / 4:00 是两个心跳时间戳之差 | ACCEPTED：措辞改为心跳时间戳差，且说明 sidecar 之前没有 IMPACT 瞬时 |
| R6-F3 | `internal/broker/transfer.go:1862` | 数据竞争：`handleFinalizeReq` 锁外读 `preview.committed`，`markCommitted` 锁内写 | ACCEPTED：`claimAbandonedPush` 在锁内一并返回 `committed`，调用方只用返回值 |
| R6-F5 | `gotchas.md #70` | 代码引用 `gotcha #70 (simcluster-speed)` 但条目从未提 cutover 候选 ① | ACCEPTED：#70 增候选 ① 段 |
| R6-F6 | `expected-verdicts-log.md ## 98` | H/X14 重构后陈旧（pass=13、330 s 分项、IMPACT-first） | ACCEPTED：增 2026-09-19 段（RECOVERY_BUDGET=260 的推导、pass 变化、自证 C） |

## 4. NOTE（19）

| id | 位置 | 主张（一行） | 处置 |
|---|---|---|---|
| R1-F5 | drill 74 | 审查时主树正处于 74 拆臂中途，两门为红 | NOTED：审查快照的瞬态；74 现已一致（`.SRAB`/`.C` 子行、基线含来源头注） |
| R1-F6 / R6-F12 | plan §8.1c | 承诺三个 RECOVERY 合取项、交付两个且未说明 | ACCEPTED：§8.1c 改为两合取形式（X14 允许），并写明第三项为何不要（agent slog 的 disconnect→rebuild 只用于 RECOVERY-PATH 归类） |
| R2-F4 | `cluster_add_drive.go:684` | 最终确认探测期间 ctx 取消被报成「cutover NOT confirmed」 | ACCEPTED：`cutoverBrokerWithPoll` 在最终探测前后检查 `ctx.Err()`；`TestCutoverBrokerCancelledCtxIsNotNotConfirmed` |
| R2-F5 | `cluster_grow_trigger.go:176` | `NonvoterCommitted` 是被 cap（32 条）的 timeline 事实，被挤出后 barrier 静默退回 2 min 旧等待 | ACCEPTED：`joinNonvoterCommitted` 三重见证——当前状态 ≥ CATCHING_UP、BLOCKED 且 last_error 以 `OpBlockedCatchupDeadlineMsg` 开头（该文案只由 `boundCatchingUp` 从 CATCHING_UP 写出）、timeline；前两者读的是 trim 碰不到的列。测试增「CATCHING_UP 已被挤出」三行，变异 MG/MG2 红 |
| R2-F6 | `preflight.go:72` | 注释把未加引号 `2m` 的误读写成 2 097 152（实为 2 000 000） | ACCEPTED：注释与错误文案改正 |
| R3-7 | `nats_ping_defaults_test.go:117` | gate-control 只练 reader；删掉 ping_max 比较全绿 | ACCEPTED：控制测试改为驱动**比较**（两种漂移都必须被看见） |
| R3-8 | `assert-identity.sh:41` | tokenizer 的已知盲形（今日树中无）与死代码 `masked` | ACCEPTED：删死代码；盲形（`if ! assert_ok` / `FOO=x assert_ok` / `eval` / `command`）改为 fail-closed 报红，见 §5 |
| R3-9 | `timeline-test.sh:39`、T-1 | 一条永不失败的检查（`tl.tsv.unset` 无人写）；T-1 把整行 DRILL-POLL-WAIT 遮掉 | ACCEPTED：删空检查、改为 scratch 目录文件计数；T-1 只遮 `wall=`/`t0=` 两个数并带控制 grep |
| R4-F6 | `transfer.go:534/634` | `phaseTimeoutFor` 只测函数、不钉调用点；pull 侧换回裸 `timeout` 全绿 | ACCEPTED：`TestTierBEntryPointsDeriveTheirPhaseTimeoutFromTheSize`（AST 钉：两个入口各恰一次调用、末参必须是 `phaseTimeoutFor(cmd, <本次的 size>, timeout)`）；三种磁盘变异均红 |
| R4-F8 | `conn_liveness_test.go:20` | `MaxPingsOut` 断言今天是恒等式（常量 = nats.go 默认值） | ACCEPTED：options 施加在哨兵值（−1）之上；删 `MaxPingsOutstanding` 即红（变异 ME） |
| R4-F9 | `cluster_add_drive.go:955` | 包装过的 ExecStart 让 `isTetherServeArgv` 读成「无进程」 | **REFUTED**（verifier 实测：`env`/`nice` exec 到目标，cmdline 即目标；不 exec 的 shell 包装留下子进程被采样看见）。无行动 |
| R5-F7 | plan §8.1 | 硬闸收据早于 H / #83，且无 lint 日志 | NOTED→将在 phase 收尾重新采集全部四道硬闸收据（plan §8.8），本文 §6 记录本轮的 lint/包级结果 |
| R5-F8 | plan §8.0/§8.1 | 若干计数差一（25/30 含汇总行；+21 inventory 明细） | ACCEPTED：计数改正并注明口径 |
| R5-F9 | plan §8.0 | 「floor 5 min + slack 60 s」读作 6 min，12 MB 实际武装恰 5 min | ACCEPTED：改写为 `XferBudget` 的真实分支（floor 平铺） |
| R6-F8 | plan 前言 / §6.5 | 「零 wire 变更」在 #83 的 `nonvoter_committed` 之后不成立 | ACCEPTED：前言与 §6.5 改写；矩阵补 #83 / P-a / P-b 行 |
| R6-F9 | `cluster_add_drive.go:691` | HALT 文案缺 plan 承诺的 `cluster status --json` 摘要 | ACCEPTED：`cutoverBrokerWithPoll(..., summary)` + `formerN1HealthSummary`；测试断言摘要出现在 HALT 中 |
| R6-F10 | plan §8.5 | #83 关闭只靠一个 GREEN 样本 | NOTED：G1 g-curve 含 42 三次（cap 0/5/8），样本随 G1 增加；plan §8.5 注明 |
| R6-F11 | `ledger-crosscheck.sh:78` | CANDIDATE/闭合检测仍是位置式（标题后 3 行），#33 修法是挪文字不是加固门 | ACCEPTED：见 §5 |

## 5. 审查之后、本报告之内的补充处置

（本节记录 §3/§4 里标「见 §5」的两条，因为它们改的是闸门本身，按 CLAUDE.md §5 要写明为什么。）

- **R3-8 盲形 fail-closed**：assert-identity 的 tokenizer 只认行首 `assert_ok|assert_fail|…`。`if ! assert_ok …`、
  `FOO=x assert_ok …`、`eval 'assert_ok …'`、`command assert_ok …` 都是合法 shell 却对门不可见——今天树里没有，
  明天就会有。处置不是教 tokenizer 认更多形状（每教一种就有下一种），而是**看见即红**：扫描到这些前缀形状直接
  报「claim 站点对身份门不可见，请改写成行首调用」。
- **R6-F11 台账状态改读标题**：`ledger-crosscheck.sh` 的闭合 / CANDIDATE 判定改为**只读 `### #N —` 标题行**
  （本仓台账约定：状态词写在标题括号里，`（已修复，…）` / `（OPEN）` / `（CANDIDATE，未归因）`），正文里任何
  交叉引用都不再能把一条活缺陷洗成 CANDIDATE 或已闭合。切换时露出的事实：**DOC-28 此前被正文里一句
  「源码 SB-96-3 已闭合行为面」读成已闭合数月**——它其实是开着的文档缺口，本轮补 `usage.md §5.13` 一段后真正
  关闭；#79/#81/DOC-23/DOC-27 标题补状态词；#69/#70 按台账自己的用词（候选）在标题标 CANDIDATE。新
  `tests/ledger-crosscheck-selftest.sh`（S-1…S-6 + 控制）接进 run-all.sh。这是把隐含约定写成可机械判读的
  形式，不是放宽。

## 6. 本轮硬闸与收据

- 包级：`go test ./cmd/tether ./internal/broker ./internal/agent ./internal/natsconf ./internal/auth ./internal/proto
  ./test/determinism ./test/architecture` 全绿（2026-09-19 18:0x，处置全部落盘之后）；新增守卫的变异台账在 plan §8.6。
- `make lint`：0 issues（R4-F1 之后再跑一次：新增两处 unparam 由测试传非常量参数消除）。
- hermetic 闸集 `sh test/simcluster/tests/run-all.sh`：ALL PASS 186 s（含新增的 arm-manifest-lint / contention-registry /
  arm-aggregation 三对与 ledger-crosscheck-selftest）。
- deploy-tier 收据（solo，无并发 go test；主机另有租户的 python 负载 ≈10–13 核）：32 `GREEN pass=62`（镜像 #6）；
  98 `INCOMPLETE 1 pass=13 244 s`（镜像 #7，自证 C PASS）；67 `INCOMPLETE 1 pass=18 187 s`（镜像 #9）；
  67u（镜像 #8）一次 CONTROL(before) 两连拒的 ASSERT-FAIL 记为 #67 sub-face 4 残余的高负载形态，不 band。
- 全矩阵 `make e2e-parallel` 与 `make gates` 在 phase 收尾（Task 5）统一重采，届时更新 plan §8.8。
