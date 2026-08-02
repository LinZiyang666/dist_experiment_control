Pass

# upgrade follow-ups + gotcha #72 大改后独立外部复审

日期：2026-08-02

审查者角色：外部测试/审查者；不信任内部 review 和开发者自报绿测，所有结论重新从代码、
第三方库行为、独立反例、门禁和真实 simcluster 取证。

## 最终结论

**Pass — 当前完整工作树可放行，但放行单元必须包含本报告列出的 unstaged 外审修复。**

开发者大改后已修复上一轮 F1–F8，原始 reviewer 反例、完整测试、`-race`、gates、E2E
和 simcluster hermetic 均重新通过。但复审仍发现 1 个 Blocker、3 个 Major、1 个 Medium 和 3 处文档
漂移：包括 finalizer 发布前的 `nc.mu` 永久挂死窗口、初连后立即断连的 watchdog 误清、
systemd stop 与 rebuild escalation 竞争时的进程复活、同 host sibling 误回滚他人 marker，以及 drill 98
空水位 false-green。这些问题已在用户授权后直接修复，并分别加了能在修复前精确变红的独立反例。

修复后未再发现可达的 Blocker/Major 代码问题，全量门禁和真实 nats:// 故障注入通过。
仅暂存的开发者基线**不可单独发布**；它仍包含本轮已修复的高严重度问题。

## 审查边界与分层

- 初始边界：`HEAD` 之外全部 34 个候选文件，审查开始时 index 为空。
- 候选复审完成后，按用户要求执行 `git add -A`；开发者基线现为 staged 层：
  34 files，2961 insertions，83 deletions。
- 外审修复、独立回归、tasklist 更新、文档订正和本最终报告全部保持 unstaged，
  可直接用 `git diff` 与 staged 基线比较。
- 权威链：`CLAUDE.md` / requirements → `docs/distributed-broker-architecture.md` 与
  `docs/deploy-tier-gotchas.md` → usage/testing/simcluster Mandate → 历史 review。
- 完整 tasklist：`docs/reviews/upgrade-followups-gotcha72-external-review-tasklist.md`。

## 本轮 findings 与修复

### R1 — Blocker：finalizer 发布前的 `ConnectedUrl` 仍可永久阻塞

**问题。** 候选代码在 `connectNATS` 返回后，先调用 `nc.ConnectedUrl()`，之后才构造和发布
`sessionFinalizer`。连接可在 `nats.Connect` 返回后立即进入 reconnect；`doReconnect` 持有 `nc.mu`
跨越无 deadline 层，而 `ConnectedUrl` 需要同一把锁的读锁。若 observer 卡住，session defer 无法到达；
若父 ctx 此时取消，disconnect callback 又会因关停 guard 不再布置 watchdog，则 operator stop 仍可被
永久劫持。这说明候选 F3 仅把 observer 从 teardown callback 移走，未封住 initial-session 窗口。

**修复。** `internal/agent/agent.go:785-809` 现在先构造/发布 finalizer，并用
`context.AfterFunc(parent)` 使父关停能从另一 goroutine 进入同一 `sync.Once` ladder，然后才读
`ConnectedUrl`。因此 observer 即使挂死，也已有可达的 cancel/poison/escalate 路径。

**反例。** `internal/agent/session_teardown_order_test.go:85-124`
`TestSessionPublishesFinalizerBeforeNATSObserver`。

### R2 — Major：成功初连后清 watchdog 会误清新断连的唯一 timer

**问题。** 候选在 `connectNATS` 返回后无条件调用 `stopRedialWatchdog()`。若 NATS 在 connect
返回前后立即断开，`DisconnectErrHandler` 先布置了当前 session 的 timer，session goroutine 随后却把它当成
旧 timer 清掉。Disconnect callback 通常只发生一次，节点可从此无 stuck-redial 恢复保障。

**修复。** `internal/agent/agent.go:768-773` 在新连接能产生 callback 之前清理上个 session 的 timer。

**反例。** `internal/agent/session_teardown_order_test.go:126-157`
`TestSessionClearsStaleRedialBeforeConnecting`。

### R3 — Major：systemd stop 与 rebuild S5 竞争可 self-exec 复活正在关停的 daemon

**问题。** `sessionFinalizer.once` 保留第一个 caller 的 intent。如果 redial callback 以 `rebuild` 先抢到
single-flight，随后 systemd 取消父 ctx，20s ladder 到 S5 时候选仍会 self-exec，与 stop 的生命周期意图相反。

**修复。** `internal/agent/conn_teardown.go:233-244` 在真正进入 S5 前重新读取父 ctx；父关停压过
早先的 rebuild intent，转入 exit 91。`agent.go:789-801` 的 session defer 也显式以父 ctx 判定 shutdown。

**反例。** `internal/agent/conn_teardown_test.go:227-248`
`TestParentCancellationOverridesRebuildEscalation`。

### R4 — Major：teardown escalation 可回滚同 host 其他实例的 pending upgrade

**问题。** `restorePrevForEscalation` 只检查 marker 为 pending，没有执行项目已定义的
`markerTargetsThisAgent` 身份不变量。共享二进制 sibling 若恰好发生 teardown wedge + exec failure，可把目标实例
的 pending marker 和磁盘映像擅自转为 rollback。这与 `upgrade_state.go` 明文规定的“sibling 不得报告或转移他人
upgrade”矛盾。

**修复。** `internal/agent/conn_teardown.go:306-316` 在持 host upgrade lock 后同时校验 marker state 与
SID/NID target；非本实例 marker 保持原状，仅执行 escalation 的第二次 exec 尝试。

**反例。** `internal/agent/session_teardown_order_test.go:236-268`
`TestWedgedTeardownDoesNotRestoreSiblingUpgrade`。

### R5 — Medium：drill 98 的 post-impact heartbeat 水位可为空

**问题。** 候选已把 `HB0` 移到 impact 之后，但没有验证读取成功。短暂 ctl/jq 错误会产生空水位；
之后任何非空 heartbeat 都会被视为“推进”，弱化产品重注册证据。

**修复。** `test/simcluster/drills/98-stuck-redial-recovery.sh:176-178` 在进入 recovery poll 前对空水位
fail closed。`test/simcluster/tests/teardown-recovery-nonvacuity-test.sh:15-22` 增加结构反例防回归。

### R6 — Minor：三处文档与当前实现/证据漂移

- `docs/deploy-tier-gotchas.md:696-704` 把“closer 永远 join”改为真实契约：S3/S4 返回前 join，
  S5 由 process replacement/exit 终结当前进程。
- `docs/reviews/line2-plan.md` 的 `nilerr ×5` 说明仍写“这四处”，已更正为五处。
- `docs/reviews/simcluster-coverage-inventory.md` 仍称 drill 33 “首跑待 DNS 修复”，与同文档和 verdict log
  已记录的真跑矛盾，已改为“已完成真跑”。

## 开发者 F1–F8 重新验证

上一轮报告的 8 个 findings 均不仅根据回应文字关闭，而是重跑代码路径和原 reviewer 反例：

- F1：`subEvict.Unsubscribe` 已进入 bounded cleanup，无 direct defer；通过。
- F2：register 使用 finalizer 可取消的 `runCtx`；通过。
- F3：`rebuildOntoVoter` 不再在 finalizer 前读 `ConnectedUrl`，改为无锁快照；通过。
- F4：S5 首次 exec 失败后恢复 prev 并再 exec，二次失败不返回污染的 `Run`；通过。
- F5：drill 98 已具备 post-impact watermark、单绝对 deadline、connz present/absent/error 三态和其他 voter
  landing；本轮又补了非空水位。
- F6：drill 31 untouched oracle 拒绝 agt2 的 skipped/failed/success 任何 staged 行；通过。
- F7：主要文档过度宣称已订正；本轮补齐三处遗留漂移。
- F8：新 `context.Background()` 站点已标注 context class 与生产可取消性；通过。

## 独立测试与门禁证据

### 开发者基线入 index 前

- 原 reviewer 4 个 Go 反例：全部 PASS。
- `teardown-recovery-nonvacuity-test.sh`：PASS。
- `go test -race ./internal/agent ./internal/broker -count=1`：PASS。
- `sh test/simcluster/tests/run-all.sh`：ALL PASS。
- `make gates`：PASS，lint 0 issues。
- `make test`：PASS。
- `make e2e-parallel`：15/15 顶层测试，ALL PASS，3m34.103s。
- `git diff --check`：PASS（当时仅检查 tracked diff；Git 不会把尚未跟踪的新文件纳入该命令）。

### 外审修复后

- 新增定向反例：finalizer-before-observer、watchdog-before-connect、parent-shutdown-wins、
  sibling-marker-isolation，以及 shell 空水位非空性；全部 PASS。
- `go test -race ./internal/agent -count=1`：PASS（16.740s）。
- `make gates`：首轮只因 revive 要求 `context.Context` 置于参数首位而红；调整签名后第二轮
  PASS，lint 0 issues。无行为失败被掩盖。
- `make test`：PASS，所有 package 绿。
- `sh test/simcluster/tests/run-all.sh`：ALL PASS，包括新的 teardown/drill non-vacuity oracle。
- 真实 simcluster：宿为 `weilandserver`，重建当前代码镜像后运行隔离实例
  `drill-98-stuck-redial-recovery`。3 broker + 1 agent + 1 ctl 启动，live edge 为 brk3；仅黑洞
  agt1→brk3:4222，brk1 仍可达。impact PASS，post-impact heartbeat 非空，heartbeat 在共享 330s
  deadline 内推进，连接落到其他 voter，same-PID 正常 rebuild，未触发 S5，heal 后仍 ONLINE。
  13 个产品/setup 断言全 PASS，`assert_fail=0 setup_red=0 product_red=0`。脚本最终为已知
  `INCOMPLETE rc=4`，唯一 `not_covered=1` 是 WSS arm。隔离容器/网络/卷已清理。
- `make e2e-parallel`：15/15 顶层测试全部被调度，ALL PASS，3m34.653s。
- `git diff --check` 与 `git diff HEAD --check`：PASS；staged 新 shell 文件的 EOF 空行也已在
  unstaged 修复层清理。

## 疑惑、保留面与建议

1. **WSS 故障形状仍未覆盖。** 现场事故是 half-dead `wss://` handshake/write 路径，当前 simcluster
   只能黑洞 nats:// TCP edge。因此本 Pass 表示当前状态机、反例和可构造的 deploy-tier 恢复路径
   可放行，**不表示 gotcha #72 可以关闭**。建议上线前/金丝雀期在真 WSS 前置上多轮施加
   半开握手/写路径黑洞，并把该 arm 加入 simcluster 后再关台账。
2. **S5 真终态未在真进程中命中。** Go 测试用 seam 验证 intent、exec 重试和 marker 身份；本次
   drill 使用设计的 same-PID normal rebuild，未观察真 `os.Exit(91)` 或真 self-exec。建议补 subprocess/systemd
   故障注入，分别量到 shutdown exit 91 和 rebuild PID/image 转移。
3. **仍保留一次 `ConnectedUrl` observer。** 现在它已被 finalizer 与 parent AfterFunc 包围，不再能无界
   劫持关停；但从简化锁面看，长期可考虑完全不调 NATS observer，或由已知 dial target/健康 callback
   发布诊断快照。这是可维护性建议，不是本轮放行阻断。
4. **本次真 drill 只运行一轮。** 它证明了非空和当前镜像的一次真恢复，不等于概率性
   竞态的统计证明。建议金丝雀期记录多轮 OFFLINE 时长、rebuild/escalate 分类和资源回基线。

## 放行意见

我对**当前完整工作树（staged 开发者基线 + unstaged 外审修复）**作 Pass 放行。
发布/合并前必须将 unstaged 修复与基线一起纳入发布单元，但按用户要求，本次审查结束时它们仍留在
暂存区外供对比。仅发布 staged 基线会重新引入 R1–R5，不在本 Pass 范围内。

---

## 主进程对外审修复的审查（2026-08-02）

本轮外审者不仅出结论，还**直接改了实现**（R1–R5）与三处文档（R6）。按 CLAUDE.md §4，实现只能由
主进程定稿，所以这些改动我逐条复核而不是采信；下面是复核结论与我补做的证据。

### 逐条结论：R1–R6 全部采纳，无一处需要回退或改写

**R1（finalizer 发布前的 `ConnectedUrl` 窗口）— 采纳。** 上一轮我修 F3 时把 observer 从 `rebuildOntoVoter`
里删干净了，却漏了 `session()` 自己在**初连之后、finalizer 发布之前**还留着一次 `nc.ConnectedUrl()`。
那一次调用同样取 `nc.mu.RLock`，同样能被一个立刻开始的 `doReconnect` 无限期挡住，而此刻 `sessFin` 尚未
publish——`fireRedial` / 父 ctx 都拿不到任何可用的 ladder。**F3 的修复只覆盖了两个站点中的一个**，这是我
自己没有把不变量扫到底，不是外审者过度收紧。

我重点验的是新引入的 `context.AfterFunc` 会不会制造新问题，结论是不会：
- **cleanup 丢失无害。** AfterFunc 抢先进 `once` 时 `takeCleanups` 只取到当时已注册的项，setup 期后注册的
  `Unsubscribe` 不会执行——但紧随其后的 `nc.Close()` 本就清掉全部 subscription，没有资源逃逸。
- **proxy drain 屏障顺序不变。** 我一度担心它被跳过，实际上屏障本身就是**第一条** bounded cleanup，
  cleanups 按注册序在 closer goroutine 内执行，drain → Unsubscribe → Close 的次序与原来一致。
- **不泄漏 goroutine。** 父 ctx 是标准 cancelCtx 链，`AfterFunc` 走 propagateCancel 快路径不驻留 goroutine；
  `defer stopParentTeardown()` 注册在 `defer fin.Do` 之后、LIFO 中先执行，stop 返回 false（f 已启动）时由
  `once` 阻塞兜住，session 不会带着未完成的 ladder 返回。

**R2（watchdog 误清）— 采纳，是真缺陷。** `ReconnectHandler` 里已有 `stopRedialWatchdog`，所以把清理挪到
`connectNATS` 之前不会让 connect 期间布置的 timer 失去回收者；而挪之前那个形态确实会把**新连接**
disconnect callback 刚布置的唯一 timer 当成旧 timer 清掉。DisconnectErr 通常只来一次，清掉即永久失去恢复保障。

**R3（父关停 vs rebuild intent）— 采纳。** `once` 保留首个 caller 的 intent，redial 回调抢先时 S5 会 self-exec，
把正在被 `systemctl stop` 的 daemon 原地复活。在终态前重读父 ctx 是最小且正确的修法。

**R4（sibling 误回滚）— 采纳，且后果比报告写得更重。** 见下面的变异 M4：缺这半个判据时，sibling 的 escalation
把**目标实例的 NEW 映像换成了 OLD**——不是"marker 状态漂移"，是共享二进制被第三方降级。

**R5 / R6 — 采纳。** 空水位 fail-closed 与三处文档订正都是收紧方向，其中 `line2-plan.md` 的"这四处→这五处"
是我上一轮加 `nilerr` 第 5 处时漏改的自相矛盾。

### 我补做的：5 条新守卫的变异验证

外审者给每个逻辑问题都配了反例，但**没有做非空性证明**。本仓规矩是每条新守卫都要注入它声称能抓的那个
缺陷并确认变红（否则就是恒等式测试——批次 B 真实翻过车）。我逐条施加变异、跑对应测试、还原：

| 变异 | 注入的缺陷 | 守卫 | 结果 |
|---|---|---|---|
| M1 | 在 finalizer 发布前插一次 `nc.ConnectedUrl()` | `TestSessionPublishesFinalizerBeforeNATSObserver` | **RED**（observer=agent.go:778 早于 finalizer=790） |
| M2 | `stopRedialWatchdog` 退回 `connectNATS` 之后 | `TestSessionClearsStaleRedialBeforeConnecting` | **RED**（stop=777 晚于 connect=772） |
| M3 | S5 的父 ctx 判据反转（`!= nil` → `== nil`） | `TestParentCancellationOverridesRebuildEscalation` | **RED**（intent="rebuild"，want shutdown） |
| M4 | 去掉 `!a.markerTargetsThisAgent(m)` | `TestWedgedTeardownDoesNotRestoreSiblingUpgrade` | **RED**（共享二进制被改成 "OLD"） |
| M5 | 去掉 98 的 `[ -n "$HB0" ] \|\| die` | `teardown-recovery-nonvacuity-test.sh` | **RED**（空水位使 RECOVERY 变空） |

还原后 `git diff --stat` 精确回到外审修复层的 `35 / 16 / 1` 行，四个 Go 反例与 shell 守卫全部回绿。

### 复核用的门禁证据（主进程独立重跑，退出码单独回显、不经管道）

- `go test -race -count=1 ./internal/agent/`：**0**（17.595s）
- `make gates`：**0** · `make test`：**0** · `make e2e-parallel`：**0**
- `sh test/simcluster/tests/run-all.sh`：**0**，ALL PASS（含新的 teardown/drill 非空性 oracle）
- `git diff --check` / `git diff HEAD --check`：**0**；无未跟踪残留

真机 drill 不重跑：本轮 unstaged 层只改了 98 的一行 fail-closed 断言（收紧，不改被测行为）与文档，
产品侧改动已由上述 Go 反例 + hermetic 覆盖；上一轮 `31 pass=30 / 98 pass=13、assert_fail=0` 的真跑结论仍然成立。

### 我接受外审保留的四条，且不在本增量内假装解决

WSS 故障形状仍不可构造（gotcha #72 **保持 OPEN**，flip 条件是 simcluster 补 wss 前端后多轮 GREEN）；
S5 真终态未在真进程命中（Go 侧用 seam 验的是 intent/exec 重试/marker 身份）；`ConnectedUrl` 仍保留一次
observer（现已被 finalizer + parent AfterFunc 包围，属可维护性建议）；真 drill 只跑一轮，不构成概率性
竞态的统计证明。这四条随本增量一起入档，不因 Pass 而消失。
