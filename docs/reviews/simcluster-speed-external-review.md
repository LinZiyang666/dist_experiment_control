# simcluster-speed 外审

日期：2026-09-20。角色：独立外审；本轮只新增本报告，未修改实现。审查完成后按用户指示将全部代码、测试、文档及本报告加入暂存区，未提交；暂存不改变下述不通过结论。

## 结论

**不通过，暂不放行。** 确认 5 条 MAJOR（P2）问题：2 条产品正确性问题，3 条演练调度或证据完整性问题。均有本轮独立边界复现；现有相关测试通过不能覆盖这些触发条件。请实现者按 `CLAUDE.md` 的流程在本报告逐条回复采纳或驳回理由，附修复与回归证据后复审。

## 范围与依据

- 基线：`HEAD a3431a12be3cf947cc797c28fea93382fa68472d`；审查开始时暂存区为空，范围为暂存区外的 77 个已跟踪文件修改与 29 个未跟踪文件。
- 依据：`CLAUDE.md`、`docs/requirements.md`、`docs/distributed-broker-architecture.md`、`docs/deploy-tier-gotchas.md`，以及本增量的 plan、research、两轮内审处置、使用说明和 simcluster 文档。
- 架构理解：ctl/agent 经 NATS 与 broker 交互；broker 的 Raft/SQLite 承担控制状态，JetStream 承担 tier-B 文件传输，tunnel 承担数据面。审查重点是跨组件超时、状态终结权，以及演练拆臂后证据和调度约束是否仍成立。
- 代码范围包括 cluster add 的启动/追赶/恢复、tier-B Put 看门狗与 abandon、传输清理、expose 的 home apply 等待与重试、连接存活与重连、simcluster 拆臂/聚合/限流/回放及对应测试。
- 以下行号对应本轮所审工作树。严重度 MAJOR/P2 表示需要修复的条件性正确性缺陷；没有把未复现猜测列为 finding。

## F1 · MAJOR / P2：expose 重试预算没有约束一次 AddProxy，可能在 broker 回滚后迟到成功

**位置：** `internal/agent/expose.go:53–63`，尤其 56–60；关联 `internal/broker/expose.go:385–395`、`internal/agent/tunnel_adapter.go:82–95`、`internal/tunnel/tunnel.go:1512–1527`。

新增的 3 秒预算只在一次 `AddProxy` 返回 transient deny 后检查。允许进入下一次调用时，既没有把剩余时间传下去，也没有约束其完成时间；下一次即使超预算才成功，仍直接返回 nil。真实 adapter 还会等待 `opMu`，然后调用使用 client 生命周期 context 的 `OpenHome`，并非使用本次 expose 的剩余预算。底层 dial/REGISTER 各自允许的等待也不能保证总调用落在这里的预算内。

**触发与影响：** 首次 REGISTER 经 2.6 秒返回 `home_catching_up`，等待 250 毫秒后允许重试；第二次再用 2.6 秒成功。两次各自都小于 broker 默认的 5 秒 forward timeout，但总计约 5.45 秒。broker 已返回 `agent_no_responders` 并回滚分配，agent 随后却将 expose 当成成功，保留端口记录，造成两端状态不一致。最终是否由 reconcile 清理不改变本次请求已经错误终结的事实。

**本轮复现：** 使用真实 NATS、真实 `handleExposeForwarded` 与 state store，仅 adapter 注入上述延迟/结果，NATS request 使用与 broker 默认相同的 5 秒等待：

```text
TestExposeRetriesFinishBeforeForwardExpires
request_err=nats: timeout elapsed=5.475762601s calls=2 persisted_ports=1
FAIL: the broker's 5s forward expired but the agent retried successfully after it and retained the port
```

这是 handler 层复现，未启动完整 broker/tunnel；broker 超时后的回滚由上述生产代码确认。现有立即返回的 scripted adapter 测试没有覆盖慢拒绝后再慢成功。

**修复与验收：** 将总截止时间/剩余预算贯穿 adapter、锁等待、dial 和 REGISTER；超时必须阻止迟到安装或完成必要清理。只用 goroutine 加 select 提前返回、放任后台成功安装仍不满足要求。补充慢首次拒绝、慢重试成功及 adapter 竞争场景，证明 broker 到达 forward deadline 前已有确定结果，并且不会留下迟到成功的端口/会话。

**实现者回复（2026-09-20）：ACCEPTED，已修。** 复现成立：预算只在两次调用**之间**检查，每次调用本身不受约束。修法是**一个 deadline 贯穿整条梯子**，不是 goroutine+select：
- `internal/agent/expose.go`：`addProxyWithTransientRetry(ctx, …)` 收一个带 deadline 的 ctx（`exposeOpenContext(runCtx)` = run ctx 的 3 s 子 ctx，Run 之前退化为独立 deadline），每次 `AddProxy(ctx, tok)` 都在它之下；剩余预算 ≤ 一步就不再重试。被 deadline 切断的调用若之前已收到过 transient deny，返回**同时**包着 DenyError 与 `context.DeadlineExceeded` 的错误（`%w … %w`），handler 据此仍答 `home_catching_up`（exit 75 重试），不是 `frpc_failed`。
- `ExposeAdapter.AddProxy(ctx, p)`（接口签名变更，12 个测试 fake + 4 个生产调用点同步）：ctx 只约束**打开**（锁等待、dial、REGISTER、安装），从不约束打开后的 session 生命周期——这条边界写在接口注释与 `tunnel.Client.OpenHome` 注释上，就是为了不把 gotcha #80 往下挪一层。四个生产调用点各自声明自己的界：forwarded expose = 3 s 预算；boot replay / rehome open = run ctx（session 重建时放弃 dial 而不是把它做完到下一个 session 的世界里）；proxy 路径 = 无 deadline（`proxyStartLocked` 按 dataplane_lifetime 门不得收 ctx，行内 `ctx-none:` 标注，行为与改前一致）。
- `internal/agent/tunnel_adapter.go`：`opMu sync.Mutex` → 容量 1 的 channel 信号量；`AddProxy` 用 `acquireOp(ctx)`（select 于 ctx.Done），`ApplyHome`/`RemoveProxy` 保持无界获取。锁等待超时不 dial、不留 `localFor` 映射。
- `internal/tunnel/tunnel.go`：`OpenHome(ctx, …)`——握手 ctx = `c.ctx` 的子 ctx + `context.AfterFunc(ctx, cancelHS)`，两边任一结束都关掉 conn；**安装前在 `c.mu` 下再查 `ctx.Err()`**，REGISTER OK 之后 deadline 才到的 transport 直接丢弃并返回 deadline（迟到安装的那扇窗）。返回错误命名 caller 的 deadline 而不是 "use of closed network connection"。`Open`/`ApplyHome` 传 `c.ctx`（语义不变）。
- **收据**：审查者的 `TestExposeRetriesFinishBeforeForwardExpires`（签名按新接口适配、fake 遵守 ctx）在修后 `request_err=<nil> elapsed=3.05s calls=2 persisted_ports=0` PASS。正式测试：`internal/agent/expose_test.go` `TestHandleExposeForwardedAnswersInsideTheBrokersForwardWindow`（审查的 2.6 s + 2.6 s 形状，请求等恰好 5 s：修前 `elapsed 5.0s calls 2` 无回复→红；修后 3.0 s 回 `home_catching_up`、state.json 0 端口）+ 梯子表新增两行（慢重试被切、无先例 deny 的切断就是 deadline 本身、无 deadline 的 ctx 不重试）；`internal/agent/tunnel_adapter_test.go` `TestAddProxyGivesUpTheLockWaitAtTheCallersDeadline`（600 ms 锁持有 vs 150 ms 预算：≈150 ms 返回、0 次 REGISTER、无映射）+ 锁在预算内释放则正常打开；`internal/tunnel/open_deadline_test.go` 四条——deadline 内没答的 REGISTER 被切且 600 ms 后**不**安装（迟到成功）、成功打开的 session **不随 caller ctx 死**（cancel 后 DropTransport 仍自愈重连——只看 `HasSession` 抓不到这条，第一版就没抓到）、已结束的 ctx 不 dial、REGISTER OK 与安装之间 ctx 结束 ×20 次 迭代无一安装。
- **变异（全部红）**：M1 删安装前的 `ctx.Err()` fence → 迭代 1 即 "OpenHome succeeded although the caller's ctx ended"；M2 `sessCtx` 派生自握手 ctx → 掉线后不再重连；M3 `dialAndRegister` 用 `c.ctx` 忽略 caller → 614 ms 才返回；M4 `AddProxy` 改回无界 `acquireOpBlocking` → 602 ms；M5 每次调用 `context.WithoutCancel(ctx)`（修前形状）→ 梯子表两行 + handler 测试 `elapsed 5.0s calls 2` 全红。
- deploy-tier：镜像 #15（本树）跑 `74-rebalance-on-return.SRAB` solo（rehome/expose 路径）INCOMPLETE 1 pass=36 MATCH（见 F4 回复）。

## F2 · MAJOR / P2：新增 push abandon 接受 tier-A，夺走接收端的终结权

**位置：** `internal/broker/transfer.go:1866–1868`；原子 claim 的对应缺口在 `427–438`，尤其 434。

新增分支允许所有 `verb=push && Kind=failed` 的创建者 finalize，`claimAbandonedPush` 只排除 finalized、非 push 和 committed，没有限定 tier-B。`committed` 只在 `handlePushCommitReq` 中设置；tier-A push 没有该阶段，创建后立即转交接收 agent，tracker 中的 committed 却始终为 false。

**触发与影响：** 合法创建者对自己正在接收的 tier-A push 发布 `finalize{failed}`，broker 接受并写 failed 终态、取消看门狗、移除 tracker，但没有取消 agent 的接收/落盘。agent 随后发出的真实完成事件失去对应 entry，形成“审计已失败、实际仍可落盘”的终态失配。这是同一创建者可触发的协议错误；当前标准 CLI 的 abandon 调用位于 tier-B 路径，复现不依赖其会自动向 tier-A 发送该请求，也不声称存在跨用户越权。

**本轮复现：** 使用真实 NATS 和 `handleFinalizeReq`，按 tier-A push 实际形状创建 tracker entry（`verb=push,tier=a,committed=false`），填入合法成员/创建者，再向其 finalize subject 请求 `Kind=failed,Tier=a`：

```text
TestCreatorCannotFinalizeAReceivingTierAPush
finalize response={OK:true Code: Error:} tracker_present=false
FAIL: creator took terminal ownership of a tier-A push already owned by the receiving agent
```

该测试直接构造接收阶段的 entry，没有启动完整文件接收；它已证明新增 handler 会接受本应拒绝的终结请求并删除跟踪记录。

**修复与验收：** 仅允许可信 tracker entry 为 tier-B 且尚未 commit 的 push abandon，在锁内 claim 时也检查 tier；不能信任请求体的 `Tier` 字段代替该检查。补充 tier-A 创建者 finalize 被拒绝、tracker/watchdog 保留、随后 agent 能正常产生唯一终态的测试，并保留 tier-B commit/abandon 竞争测试。

**实现者回复（2026-09-20）：ACCEPTED，已修。** 复现成立：`claimAbandonedPush` 只看 finalized / verb / committed，而 tier-A 从没有 commit 阶段，`committed` 终生为 false。
- `internal/broker/transfer.go`：`claimAbandonedPush` 在同一把 `tracker.mu` 下多查 **tracked** `e.tier == "b"`，返回一个封闭枚举 `abandonRefusal`（claimed / noEntry / finalized / notPush / committed / **tierA**）而不是 `(ok, committed)` 二元组，handler 按枚举穷举选回复（无 `default:`）：tierA → `verb_mismatch` + "tier-a push: the receiving agent owns the terminal; the creator cannot abandon it"，entry、watchdog、`entry.cancel` 全都不动；committed → 原来的 refusal；noEntry / finalized → 幂等 OK（watchdog 或重复 finalize 已写终态，行为不变）；notPush → verb_mismatch。**`fin.Tier` 对 push 不再有投票权**：audit 记录的 tier 只对 pull 接受 body 覆盖（那是 pull.req 时乐观盖 "b" 的既有语义），push 一律用 tracked tier。
- 枚举登记：`test/determinism/enum_switch_default_test.go` `enumFamilies` 加 `broker.abandonRefusal`（前缀 `abandon`，全仓无其它同前缀常量）并进 `familiesWithSwitches`——这是覆盖**扩大**，不是放宽。
- **收据**：审查者的 `TestCreatorCannotFinalizeAReceivingTierAPush`（未改一字）修后 `{OK:false Code:verb_mismatch Error:tier-a push: …} tracker_present=true` PASS。正式测试 `internal/broker/transfer_finalize_test.go`：`TestClaimAbandonedPushOnlyBeforeCommit` 表加 tier-A 行（按 handlePushReq 的真实形状：tier "a"、无 bucket、never committed）并断言拒绝不置 finalized、随后 `claimFinalize`（agent 的终态）仍能且只能 claim 一次；`TestPushCreatorFinalizeFreesTheBucketBeforeCommit` 新增 Arm 4——真 NATS 上的 handler，body `Tier` ∈ {a, b, ""} 三种都拒、entry 留存未 claim、watchdog ctx 未被 cancel、然后 agent 终态 claim 恰一次。tier-B 的 commit/abandon 竞争测试（`TestPushCommitHandoffMarksTheEntryCommittedBeforeForwarding` + Arm 1/2/3）保留并按新返回形状更新。
- **变异**：删掉 `case e.tier != "b"` → 表测 `claimAbandonedPush(push-tier-a) = 0, want 5` 与 handler Arm 4 `got ok=true` 同时红。body-信任型实现被 Arm 4 的 `Tier="b"` 行抓住。

## F3 · MAJOR / P2：replay 的 legacy 回退逐臂判断，可用旧父日志掩盖缺失臂

**位置：** `test/simcluster/run-drills.sh:288–303`，尤其 292–299。

注释要求“所有 arm 日志都不存在”才把整项作为一次 legacy unit；实现却对每个 unit 独立判断。只要某一臂日志缺失且父日志存在，就用父日志替换该臂，即使兄弟臂的新日志已存在。

**本轮复现：** 临时归档同时放入合法 GREEN 的 `74-rebalance-on-return.SRAB.log` 和旧的 `74-rebalance-on-return.log`，不放 `.C.log`；对应 rc 均为 0。对完整 drill 74 执行 `--replay` 后：

```text
units: 74-rebalance-on-return.SRAB 74-rebalance-on-return
ARMS 74-rebalance-on-return GREEN 0 0 0 0 0 0 0 2 ...
ALL GREEN
replay_exit=0
```

ARMS 中的 `arms_missing=0`，C 完全没有证据却被旧父日志抵消。该合成例对当前 expected 表另有 `DEVIATION` 提示，但 missing 计数和退出状态仍为 0；偏离提示不能恢复被漏掉的 C，也不构成缺臂阻断。

**影响：** 混有拆臂前后收据的归档可被当成完整执行通过，破坏 plan §5.5 对缺臂、聚合和 legacy 兼容的约定。

**修复与验收：** 按整个 drill 判断 legacy 资格；只要存在任一新 arm 日志，就保留所选 arm 身份，对缺失日志计 INFRA-ABORT/arms_missing。只有整组 arm 日志均不存在时才能回退到一次父单元。增加“父日志 + 一臂日志 + 缺失另一臂”的回放回归，并覆盖全部旧格式归档和显式单臂选择。

**实现者回复（2026-09-20）：ACCEPTED，已修。** 复现成立（注释写的是"所有 arm 日志都不存在"，实现是逐 unit 判）。
- `test/simcluster/run-drills.sh`：legacy 资格改为**每个 drill 一次**、扫的是 **manifest 声明的全部 arm**（`manifest_arms`），不是本次选中的那几个——混合归档不因为你只点了一个臂就变成纯旧归档。任一 `<drill>.<arm>.log` 存在 ⇒ 该 drill post-split，缺的臂按既有路径走：unit INFRA-ABORT、ARMS 行 arms_missing +1、drill 阻断、非零退出。
- **收据**（`tests/arm-aggregation-test.sh`，在 `make gates` 的 hermetic 闸集里真跑）：H-8 = 审查的形状（`f-split.A.log` GREEN + 旧 `f-split.log`、D/F 缺）→ 无 LEGACY-UNIT、arms_missing=2、D/F INFRA-ABORT、rc=1 且无 "ALL GREEN"；H-8b 在同一归档上显式点缺失臂 `f-split.D` → INFRA-ABORT 非 legacy；H-8c 纯旧归档 + 显式单臂 `f-split.A` → 仍是 LEGACY-UNIT（H-6 的行为保留）。
- **变异**：把判据改回 `[ -e "$LOGDIR/$u.log" ]`（修前形状）→ H-8 四条全红（`ARMS f-split GREEN 0 … f-split.A f-split`、rc=0、1 行 ALL GREEN）。

## F4 · MAJOR / P2：grow-done 累计跨过 nuke 重试，尚在扩容时提前释放限流名额

**位置：** `test/simcluster/run-drills.sh:915–917`；写入方 `test/simcluster/simcluster:365`，重试方 `test/simcluster/drills/lib/cluster.sh:25–53`。

`grow_active` 将整个 unit 生命周期内的成功 marker 数与 `# grows:` 比较。74/96 的臂声明 grows=2，实际调用默认允许重试的 `grow_to_3`；nuke 后重建不会清除前一代集群的成功 marker。失败的 grow 本身虽不写 marker，前一代成功的另一台节点仍会留下计数。

**触发步骤：**

1. 第一次 brk2 grow 成功，写入第 1 行；brk3 失败，未达到 3 VOTER。
2. helper nuke 全集群并重新初始化。
3. 第二次 brk2 grow 成功，累计第 2 行；第二次 brk3 仍在 grow。
4. `grow_active` 看到 `2 >= declared(2)`，返回 0，将该 unit 从正在扩容数量中排除，runner 因而可再启动一个 grow unit。

**本轮复现：** 直接 source 真实 `grow_to_3`，使用从 runner 原样提取的 `grow_active`；SIM stub 按生产成功写 marker 的协议模拟上述过程，并让第二次 brk3 暂停 3 秒：

```text
attempt 1/2: grow brk2 rc=0, grow brk3 rc=1
NUKING + retrying the whole grow
second_attempt_brk3_running=yes marker_count=2 grow_active=0 expected=1
attempt 2/2: grow brk2 rc=0, grow brk3 rc=0
GROW-ATTEMPTS: 2 (retry=1)
grow_retry_exit=1
```

这是无需 Docker 的真实 helper/调度谓词复现。它证明释放判据错误；没有据此声称既有部署收据均发生过实际超 cap。

**影响：** 最容易在并发压力下走到的重建重试路径反而绕过 grow-cap，使默认并发上限及基于该上限解释的观测不可靠。

**修复与验收：** 完成信号必须绑定当前集群/尝试代次，或由当前完整 grow fixture 成功后显式发出；保守方案可在重试后持有名额直到 unit 退出。简单按节点名去重仍不能处理前一代两台 grow 成功、后置检查失败后重建的情形。补充上述重建场景，证明新一代最后一次 grow 完成前始终占用名额。

**实现者回复（2026-09-20）：ACCEPTED，已修，取的是"由完整 fixture 成功后显式发出"这条。** 复现成立。"nuke 时截断"我也考虑过并否决：它管住了计数，但管不住"第一代两台都成功、post-check 失败"那一窗——那时名额已经放了，截断只能事后再占回来，不满足"始终占用"。
- `test/simcluster/drills/lib/cluster.sh`：`grow_to_3` / `grow_to_2` 对自己的 `$SIM grow` 调用把 `SIM_GROW_DONE_FILE` 置空（只对那一条命令），cmd_grow 因此不写每-grow 行；fixture 在自己的 post-check（`_three_voters` / 两道 false-green guard）通过之后一次写入声明的行数（`_grow_lane_done`：brk2+brk3 / brk2）。于是：重试代次不产生任何行；fixture 失败写零行、名额持有到 unit 退出（保守方向）；直接 `$SIM grow` 的 drill（没有 in-unit 重试）保留 cmd_grow 的每-grow 行。`manifest.sh` 的 "a retry declares nothing new" 那句从"声明上成立"变成"事实上成立"，注释同步（run-drills.sh 头、manifest.sh、simcluster cmd_grow）。
- **收据**（hermetic，`tests/arm-aggregation-test.sh` E-3）：source **真** `drills/lib/cluster.sh`，stub simcluster 按 cmd_grow 的协议写 marker（`SIM_GROW_DONE_FILE` 非空即追加）——审查的场景原样：attempt 1 brk2 成功 / brk3 失败 → nuke → attempt 2 brk2 成功、brk3 暂停；暂停中 sidecar **0 行**（修前是 2 ≥ declared 2、名额已放），放行后恰 2 行 (brk2, brk3)、`GROW-ATTEMPTS: 2` 仍是一等证据；E-3b 两次都失败 → 0 行；E-3c `grow_to_2` JS-meta guard 失败 → 0 行、两 guard 通过 → 1 行。
- **变异**：去掉两处 `SIM_GROW_DONE_FILE=` 前缀（修前形状）→ E-3 "sidecar holds 2 line(s) mid-retry" + E-3b 全红。
- **deploy-tier 收据（真栈，镜像 #15 = 本树重烤，`run-drills.sh --no-retry --no-attribute 74-rebalance-on-return.SRAB`，394 s）**：INCOMPLETE 1 pass=36 **MATCH**（既有 #34 面）；`74-rebalance-on-return.SRAB.grow-done` 恰两行且**同一秒**（`1789926236 brk2` / `1789926236 brk3`——由 fixture 在 post-check 后一次写出；cmd_grow 的每-grow 行会相隔一两分钟）；`GROW-ATTEMPTS: 1 (retry=1)`；`regime.tsv` = `REGIME default 5 0`（F5 的新侧车）。这一跑同时覆盖 F1 的 agent 路径（74 = expose + rehome，36 条断言通过）。成本：烤镜像 17 s + 394 s，单元一个。

## F5 · MAJOR / P2：replay 用本次命令默认值覆盖原始 REGIME，丢失真实运行模式

**位置：** `test/simcluster/run-drills.sh:1122–1123`；关联 replay 删除原 rollup 的 `465–467`。

replay 不执行 drill，却仍从当前 `LIVE_GROW/GROW_CAP/GROW_STAGGER` 生成描述“本次 sweep 实际运行模式”的 REGIME。原 rollup 先被删除，原运行参数没有读取或保存。普通回放因而会把 live-grow 收据改成 default；反向携带 `--live-grow` 也不能作为原执行确实无上限的证据。

**本轮复现：** 临时归档包含合法 GREEN 的 `00-skeleton` 日志和原始 REGIME，对同一目录普通 `--replay`：

```text
before: REGIME live-grow 0 30
after:  REGIME default   5 0
```

回放退出 0，期间没有执行任何 drill。

**影响：** 回放会改变归档对争用模式的陈述，无法可靠区分限流样本和 live-grow 样本；这直接影响 contention registry、LOAD-SENSITIVE 归因和发版所需 live-grow 收据的解释。

**修复与验收：** 把实际运行参数保存为回放不会重写的原始 metadata，回放读取并保留它；历史归档确实无 metadata 时明确写 UNKNOWN，不能从本次回放 flags 推断历史事实。增加 live-grow/default 双向回放、非默认 stagger/cap 保留以及无 metadata 旧归档测试。

**实现者回复（2026-09-20）：ACCEPTED，已修。** 复现成立。
- `test/simcluster/run-drills.sh`：live 模式在清理之后、任何 launch 之前写 `$LOGDIR/regime.tsv`（一行，与 REGIME 行同列：`REGIME\t<default|live-grow>\t<cap>\t<stagger>`），它进 live 模式的清理列表、**replay 从不写它**（replay 只重写 rollup）。summary 的 REGIME 行改为**从 regime.tsv 读**：live 读回刚写的、replay 读归档的；文件缺失或形状不对 ⇒ `REGIME\tunknown\t-\t-` 并在 summary 明说"归档早于 regime 记录，回放 flags 说明不了原始运行"。另外 **`--replay` 拒绝 `--live-grow` / `--grow-cap` / `--grow-stagger`**（rc=2）：它们描述一次运行，而 replay 不运行——同 `--replay --retry` 那条 MA6 的裁法。头注释登记 `regime.tsv` 为新 artifact、REGIME 行多一个 `unknown` 取值（`tests/deviation-report-test.sh` 把 REGIME 当 keyed short row 处理，不受影响）。
- **收据**（`tests/arm-aggregation-test.sh` H-9）：live-grow 归档 plain replay 后 REGIME 仍 `live-grow 0 0` 且 `regime.tsv` 字节不变；`--grow-cap 1` 归档 replay 保留 cap 1；`--grow-stagger 7` 归档 replay 保留 7；`--replay --live-grow` 被拒且归档 REGIME 行不动；手工构造的无 regime.tsv 归档 → `unknown - -`。
- **变异**：summary 改回从 `LIVE_GROW/GROW_CAP/GROW_STAGGER` 写 REGIME（修前形状）→ H-9 三条 "REGIME after replay: default 5 0" 红。

## 本轮验证记录与边界

| 验证 | 结果 |
| --- | --- |
| `cmd/tether`、`internal/broker`、`internal/agent`、`internal/cluster`、`internal/natsconf` 中本增量相关测试，`go test -race` 定向运行 | 通过 |
| `sh test/simcluster/tests/run-all.sh` | ALL PASS |
| `go test ./test/architecture ./test/determinism -count=1` | 通过 |
| concurrency 中 broker/agent cancel、active tunnel close 的 goroutine 泄漏，以及 tunnel/broker FD 稳定测试，`-race` | 通过 |
| F1/F2 的独立 Go overlay 回归断言，`-race` | 两项均按预期失败，确认边界缺陷 |
| F3/F5 的临时归档回放；F4 的 helper/调度谓词复现 | 分别确认缺臂被掩盖、REGIME 改写、过早释放 |
| `git diff --check`（暂存前，仅已跟踪文件的差分） | 通过 |
| 全部暂存后的 `git diff --cached --check`（含原未跟踪文件） | 退出 2：`test/simcluster/tests/assert-identity.baseline.tsv` 第 65、560、692、750、880、889、892、965 行含行尾空白；保持被审查内容原样，未修改 |

第一次在普通沙箱运行需监听本地端口的 Go 测试被环境拒绝；授权本地监听后重跑通过，未将沙箱错误计为产品失败。本轮没有重跑全量 `make test`、Docker 部署演练或完整 E2E，也没有把已有内审/plan 的部署记录当作本轮独立复验。

临时复现工件位于 `/tmp/tether-speed-external-review-ng9buu9b/`：`agent_probe_test.go`、`broker_probe_test.go`、`overlay.json`、`grow-retry.sh`、`sim-stub`、`grow_active.sh`、两个回放目录及输出。这些临时文件不属于提交内容；上文保留了不依赖临时目录存续的触发条件和关键输出。Go 复现命令：

```sh
go test -race -overlay /tmp/tether-speed-external-review-ng9buu9b/overlay.json \
  ./internal/agent ./internal/broker \
  -run 'Test(CreatorCannotFinalizeAReceivingTierAPush|ExposeRetriesFinishBeforeForwardExpires)$' \
  -v -count=1
```

修复后应将对应行为回归纳入正式职责测试；本轮以 overlay 保持外审不修改实现的边界。

## 主进程处置汇总（2026-09-20，等待复审）

**结论**：5 条 MAJOR 全部 **ACCEPTED 并修复**，无驳回、无分期；逐条回复在各 finding 的"实现者回复"下。两个 overlay probe 已分别
纳入正式职责测试（`internal/agent/expose_test.go` / `internal/broker/transfer_finalize_test.go`），审查者原 probe 在修后树上均 PASS。

**改动面（相对本报告暂存时的 index；本轮全部留在工作树、未 `git add`，复审以 index 为基线即可见）**：
- 产品：`internal/agent/expose.go`（deadline 贯穿梯子、`ExposeAdapter.AddProxy(ctx, p)`、`exposeOpenContext` 包级函数）、
  `internal/agent/tunnel_adapter.go`（channel 信号量 op 锁、`acquireOp(ctx)`）、`internal/agent/agent.go` / `instance.go` /
  `proxy.go`（三个调用点各自的 ctx；proxy 路径 `ctx-none:` 标注）、`internal/tunnel/tunnel.go`（`OpenHome(ctx, …)`、
  握手 ctx 合并、安装前 fence）、`internal/broker/transfer.go`（`abandonRefusal` 枚举、tracked tier、body `Tier` 对 push 无投票权）。
- 测试：`internal/agent/expose_test.go`（+3 表行、+1 handler 测试）、新 `internal/agent/tunnel_adapter_test.go`（2）、新
  `internal/tunnel/open_deadline_test.go`（4）、`internal/broker/transfer_finalize_test.go`（表 + Arm 4）、12 个 fake 与 `test/d6`
  三处签名适配；`test/determinism/enum_switch_default_test.go`（+家族 `broker.abandonRefusal`，覆盖扩大）、
  `test/determinism/testdata/test_function_inventory.txt`（+7，只增）。
- harness：`test/simcluster/run-drills.sh`（F3 per-drill legacy、F5 `regime.tsv` + REGIME 读回 + 拒 regime flags、头注释）、
  `test/simcluster/drills/lib/cluster.sh`（F4 `_grow_lane_done`、fixture 写标记）、`test/simcluster/simcluster`（cmd_grow 注释）、
  `test/simcluster/lib/manifest.sh`（grows 语义注释）、`test/simcluster/tests/arm-aggregation-test.sh`（E-3/E-3b/E-3c、
  H-8/H-8b/H-8c、H-9）、`test/simcluster/README.md`。
- 文档：`docs/deploy-tier-gotchas.md`（#86 ③ 的 deadline 语义、#84 的 abandon 边界）、plan §8.9。

**变异**：每条新守卫都注入了它声称能抓的缺陷并确认红——F1 五条（M1–M5）、F2 一条、F3/F4/F5 各一条（修前形状）。

**硬闸（最终树）**：`make test` rc=0（384 s）、`make lint` rc=0、`make gates` rc=0（300 s，hermetic 闸集 ALL PASS）、
`make e2e-parallel` 第一遍 rc=2 —— 单一红 `TestBrokerSilenceEscapesToVoter`（"agent did not exit after cancel"，本轮未触及的
路径，`make e2e-one T=TestLeakRiskMatrix` PASS、单测 `-race -count=10` 10/10）—— 重跑 **ALL PASS 3m52s**。如实登记为既有负载
敏感面（plan §8.9）。

**deploy-tier（本轮独立复验，镜像 #15 = 修后树重烤）**：`run-drills.sh --no-retry --no-attribute 74-rebalance-on-return.SRAB`
394 s → INCOMPLETE 1 pass=36 MATCH；`.grow-done` 恰两行同一秒（fixture 写）、`GROW-ATTEMPTS: 1`、`regime.tsv` 就位。
这是 F4 的场景单元，也走 F1 的 agent expose/rehome 路径。其余 42 个 drill 未重跑（本轮改动不触及它们的断言；F4 改的是
16 个 clustered drill 共用的 fixture，hermetic E-3 用真 cluster.sh 覆盖，74.SRAB 是真栈样本）。
