# 热更新 S4 · pty-terminal 实现 plan（定稿）

> 定稿：2026-10-02，lead（主进程）。起草：4 路 GPT-6-Astra lens 草案 → 4 路 Astra 对抗批评 → 1 路 Astra 综合（原始稿与批评在仓库外
> `~/tether-hot-update-handoff-20260920T2356Z/s4-plan/`）。下面「lead 定稿裁决」**覆盖**正文中与之冲突的任何文字；正文其余部分即为冻结合同。

## lead 定稿裁决（覆盖正文）

| # | 裁决 | 理由 |
|---|---|---|
| L1 = Q1 | **S4 不扩 broker/DB authority incarnation。** 「finalPrepare 后丢 session_destroyed 事件 + 同 SID 已硬删重建」这一窗口归 **S8**（升级事务的权威连续性专项）——写进 stages-plan §S8 scope_in，不是「以后再说」。S4 交付其余四窗口 + register-gone，并以 `TestPTYSessionRecreationBoundary` 如实记录该窗口当前不可判别；C-10 PTY 变体在 S4 **不勾**，S8 闭合时勾。 | 补这一窗口要给 session 引入不可复用 incarnation，跨 broker DB / wire / lease 三处，正是 S8 upgradetx 的事务面；放进 S4 会把 S4 的 PTY 工作与 broker 迁移捆在一起。它仍在本次全栈热更新增量之内（feedback：不得分期到下个版本）。 |
| L2 = Q2 | 采纳推荐：S4 清场范围 = 原 session（同 SID 多 PGID、stopped foreground 必须清完）；主动 `setsid` 脱离的后代**明列不覆盖**，usage/architecture 文字不写任意子树保证。 | containment（cgroup）是独立产品范围；用同 SID census 冒充任意子树清场是假收据。 |
| L3 = Q3 | 采纳推荐：非 TTY 外部 SIGINT/SIGTERM = 本地取消（沿 `main.go` 根 NotifyContext）；远端语义由 raw `0x03` 与显式 `signal` op 负责；不并存第二个 handler。 | 两个 handler 抢同一信号是已知竞态（批评 D-12）。 |
| L4 = Q4 | 采纳推荐：500 ms 全冻结窗在 S4 仍是**校准**（完整报告、超标显式写出、不删最大值）；5 s 新请求/业务停顿是硬判。S8 统一定性能 profile。 | 不在首跑前偷偷升格又在首跑后降回。 |
| L5 = Q5 | **改推荐**：可靠 run 支持 TTY/FIFO（`/proc/self/fd` 重开独立 OFD）、**regular file 与 `/dev/null`**（I/O 不会无限阻塞，Cancel 只需在有界读写之间检查）；socket 与其它不能安全重开/唤醒的类型在 **create 之前**回落 legacy run，并向 stderr 打一行 `tether: <stdin|stdout> is a <kind>; using legacy run (no hot-update continuity)`。不以 64 拒绝。 | `tether run cmd < file`、`> file` 今天能用；可靠路径首版把它变成用户错误是回归。create 前回落符合「create 后绝不 fallback」不变量。 |
| L6 | **Darwin**：交付 = `GOOS=darwin GOARCH=arm64 go build ./...` + `make vet-tags` + `master_other`/`io_other` 的单元测试在 Linux 上可编译；T44 `TestLegacyRunDarwinTerminalLifecycle` 写成 `//go:build darwin` 的真实测试，本机无 Darwin runner → 记 **NOT_RUN**，**不阻塞 S4 出口**（与 S0–S3 的 Darwin 口径一致），证据如实写「未在 Darwin 实跑」。 | 本机只有 Linux；把交叉编译写成实跑是假收据，因缺 runner 卡死 stage 又与既有口径不一致。 |
| L7 | **并行策略**：P0 除契约外，**为 §2.3 每个冻结签名在其未来 owner 的文件里落一个可编译桩**（返回 `errors.New("<pkg>: not implemented (S4 P<n>)")` 之类的哨兵错误，绝不返回假成功），P0 合入后 **P1–P15 同时开工**，各自把桩替换为实现；跨包测试在依赖包合入前允许 NOT_RUN，合入后由 lead 按 §12.1 DAG 顺序集成复跑。P16/P17 在实现稳定后开工。模型：HARD 包 `gpt-6-astra`，NORMAL 包 `gpt-6.1-sol`。 | 用户 2026-10-02：「worker 分配可以很激进」「最难的给 astra，其他的给 6.1 sol」。按依赖链串行要 8 跳；桩让 15 包同时写，集成在 lead 侧收敛。桩文件在 P0 合入后归下游 owner，所有权仍按时间互斥。 |
| L8 | §12 的文件 owner 表是硬约束：实现者需要改表外文件时只写「Requests for other packages」，由 lead 转派或调整 owner 表。 | 合并用 `git apply --3way`，两包同改一文件就是冲突。 |
> 基线：只读快照 `/home/weiland/s4wt/synth`，HEAD `0ec45bfeab0e614c1f900e8b29cb87e0091de981`，S0–S3 已实现。行号均指此快照；历史 plan 的行号不作当前代码证据。
> 输入：`plan-term.md`（T）、`plan-proto.md`（P）、`plan-fence.md`（F）、`plan-ctl.md`（C）及 `crit-eof.md`（E）、`crit-fd.md`（D）、`crit-auth.md`（A）、`crit-drill.md`（R）。格式沿用 `docs/reviews/hot-update-agent-bootstrap-plan.md`。
> 上位契约：stages-plan S4、S3/S5 边界；architecture §21、§22.4–22.6；handoff-failures §4；tasklist C-4/C-8/C-10 PTY/C-11/B-2 PTY；CLAUDE、testing-standards、simcluster README。本次只读、未运行构建、测试、gate 或 drill；本文所有新收据均为 **NOT_RUN**。

## 定稿裁决（建议冻结值；开放问题见 §13）

| # | 裁决 | 理由与依据 |
|---|---|---|
| D1 | `proto.ImageABIVersion` 保持 1，新增 `image-domains exec-v1 pty-v1`，放在最后一行 `image-abi v1` 之前；纯 exec image 不输出任何 S4 新字段 | S3 两层 reader 都 `DisallowUnknownFields`（`agentimage/bundle.go:201`、`execsession/image.go:216`）。拒绝“同 ABI 就能读 PTY”，也拒绝为 PTY 一刀切阻断纯 exec 的 S3 回滚；域检查必须覆盖全部保留状态。 |
| D2 | 保持 `prev ∈ {running image, candidate}`，按实际 SHA 对应的 reader 能力检查 target 和 prev；不恢复 `running==disk` 门 | `agent/upgrade.go:714–727` 已修复共享 binary sibling；bootstrap plan 的最初文字已被其自身实跑订正取代。 |
| D3 | 同一2s总预算内先 `probe Kind=pty`、再 `pty-probe`；`PTYVersion=1` 表示完整协议；唯一新 op 集合为 `pty-probe/input/resize/signal/keepalive/hangup` | S3对未知pty-probe先走无Handle身份拒绝（runtime.go:248），不能将process_identity当unsupported。第一步识别agent，第二步验broker白名单；pending/容量/权限都不降级，create发出后不重建。 |
| D4 | `InputSeq/InputACK` 按字节；`ControlSeq/ControlACK` 单独递增；输入整包按容量接纳 | 采用 T/F/C 的双序列，拒绝 P 的统一 `OpSeq` 选项，理由是隔离堵塞输入与紧急控制的实现复杂度。P 的 credit 前提确实可避免其所称死锁，不能将其写成事实错误。 |
| D5 | master 一个 owner、一个活动 wrapper、两泵借用；真 source end 才释放 `ReapHeld`；去掉 200ms close | `pty.started` 在真正 Start 前即置位（`pty.go:121`）；可靠资格必须用 `ready` 加身份。`agent/run.go:335–358` 先 Wait 再限时 Close 的顺序必须替换。 |
| D6 | 撤权先关输入/控制，Manager 激活后执行清场，再启允许的泵；任一资源激活后不得 Detach 初始 image | 当前 `execsession/fence.go:221–226` 先泵后 kill；`agentimage/restore.go:289–296`、`agentboot/image.go:318–323` 存在部分激活后误 Detach 路径。 |
| D7 | Hangup 保留既有 SIGHUP 语义，不增加 SIGCONT/SIGKILL；Signal→Close→receipt 在 Manager guard 内不可分割 | 既有 `pty.Hangup`（`:377–395`）跨两把锁，Signal 返回后 Manager 可以 Freeze；“Close 必使任意前台收到 HUP”也不是可靠清场证明。 |
| D8 | 已 create 的 reliable PTY 使用固定 180s 恢复窗；deadline 从首次失联开始，重试不续期；KA 仍使用原绝对截止 | 当前 `execclient/client.go:291–331` 内部无限重试，500ms 只是 attempt；不能拿它证明 watchdog 的原 ctl 生存。180s 是客户端等待上限，不是新增权限。 |
| D9 | `36.A/W/C/L` 全 GREEN；真实旧 exporter 的同故障负控为 `39.P`、原生 PRODUCT-RED | validator 强制父判词为子臂 join（`:236–300`）；35.C 的 GREEN 是缩窄后的 legacy 判据，不是把同一连续性 oracle 的 FAIL 改绿。 |
| D10 | Darwin 只承诺 legacy run；Linux reliable actor 不进入 Darwin legacy 路径 | `workload/replay_unsupported.go` 不支持 replay；`pty/master_other.go:13` 不提供 Linux poller/deadline 保证。交叉编译不冒充实际终端收据。 |
| D11 | Agent 恰 125 methods；在现有 agent/cmd 文件接线，状态机进子包；所有文件唯一 owner | golden 已钉 125（`structural_budget_golden.txt:116`）。公共接口 P0 先落地，后续包不得抢写公共文件或批量更新 golden。 |
| D12 | 正常 A 臂端到端请求 ≤5s、业务停顿 ≤5s 硬判；500ms 全冻结窗仍按 S3 采样并显式报告超标 | 采用 P §9 的校准口径；T/F/C 建议升格硬门，会改变验收承诺。是否本阶段升格留 Q4，默认不放宽任何数值。 |

## §0 目标与出口（5 条 + 机械判定）

| # | stages-plan S4 出口 | 同一最终候选的机械判定及证据 |
|---|---|---|
| 1 | 真 PTY：尾流、partial write 同 PID exec、FIN/Ctrl-D、控制/freeze、非 started EIO | §8 T01–T18 的真实 PTY/kernel 收据；四个固定测试名逐字保留；每条规定变异均实际击红。不能以 `0<A<Q` 代替一次真实短 Write，不能以 leader 活着代替尾流臂。 |
| 2 | `test/cli_e2e/run_reliable_test.go` 真 binary + 伪终端；legacy 可用 | T35–T44；原 ctl `waitpid`、termios、双向原始 bytes、丢回复未交付证明、四象限；内层构建跟随 overlay 与 race。 |
| 3 | drill 36 GREEN：原终端、resize、长前台跨升级和一次回滚；新 run ≤5s；34 不劣化 | `36.A/W/C/L`、父 36 全 GREEN；34 七臂全回归；39.P 预期 PRODUCT-RED 单独存放；33 A/C/C2 与 35 回归。每臂身份、运行 SHA、marker、输入/输出、原 ctl rc、资源退出、日志四流缺一即失败。 |
| 4 | 变异：200ms 截断、`File.Fd` resize、EIO 统一 EOF 都红 | T02/T03/T01 的独立 mutation 收据；另外短 Write 记账、旧 bundle 回滚、撤权后先写、重复控制、假 negative control 也必须红。 |
| 5 | 三硬闸 + gates；Darwin；整包 race + 五轮 leak；C-11 收口 | §10 全门登记；实际 Darwin run/resize/cancel 与 cross-build 分列；§11 tasklist 逐项按 artifact 勾选。Q1/Q2 涉及范围须先裁决；未解决边界不得宣称 C-10/C-11 全覆盖。 |

**范围**：原 PTY、pipe exec/PTY 共存的 runtime image、可靠 run、legacy 安全拒绝、S3 的 active rollback。S5 的 TCP/SSH/SS、broker Endpoint、S8 的通用事务/首次 legacy exporter 迁入不在本阶段。不得删除 `flow_busy`/`ss_busy`，不得改 broker OfflineAfter、认证模型或 transport ping 来换绿。

**执行纪律**：本文件是未来实施合同，不是本轮执行授权；本轮只有此文件可写。未来每包交付实现、命名测试、实际 mutation、资源收据；最终三硬闸与 drill 严格串行，不能引用另一 SHA 的绿灯。

## §1 代码裁判与总结构

### 1.1 草案有争议的事实，按当前代码裁定

| 争议 | 裁定（文件:行） | 设计后果 |
|---|---|---|
| S3 reader 会忽略新增 image 字段 | E/A/D 正确：`agentimage/bundle.go:197–204` 与 `execsession/image.go:211–217` 均严格拒绝 | domain 能力与 `omitempty` 必须同时存在；不能只靠 wire 的 additive 习惯。 |
| capability 只需扫描 Tasks/Tombstones | A 正确：`procrequest/registry.go:270–306` 的 Settle 只改 Phase，Collect 等原 lease 到期才删 | `Registry.Records.Kind=pty` 即使 settled 仍要求 `pty-v1`。 |
| Start 的 `started` 足以把 EIO 解释成 FIN | E 正确：`pty/pty.go:121` 早于 `start()`，`ready` 到 `:191` 才成立 | 成功 Start、adoption、master 身份三者全部满足。 |
| frozen import 可统一要求活 controlling SID/foreground | E 正确：`workload/process_state_linux.go:15–18,79–88` 明确允许 unreaped zombie；`ReapHeld` 是合法尾流身份 | tty 当前 SID/pgrp 是观测，不能当永久身份；不能 TCSETS/tcsetpgrp 修复来通过校验。 |
| Hangup 已经受完整进程 freeze 保护 | D/A 正确：`pty/pty.go:385` Signal 与 `:394` Close 分离；`workload/process.go:239–246` 可在中间冻结 | 添加 Manager control transaction，而不是再套 Session.mu。 |
| 原输出 Abort 已经固化 final End | E 正确：`workload/output.go:369–383` 标错、设 deadline 后不等待在飞 read 记账 | PTY finalizer 单独 join；不能直接把现有 Output.Abort 当安全终止 API。pipe 的既有语义不随意扩改。 |
| Runtime Thaw 失败仍是 pristine import | D 正确：`agentimage/restore.go:289` 已 Thaw Manager，`agentboot/image.go:319` 仍 Detach | 把 active 边界前移，失败表按“有无已激活资源”划分。 |
| Revoke 遇 BootNow 故障只损失时间精度 | A 正确：`execsession/revoke.go:44–47` 在登记前返回；`runtime.go:252` 还忽略读钟错误 | 先不可逆登记责任；钟故障不得准入、续 KA、回收责任。 |
| 500ms `transport.call` 可做公平调度单位 | E/D/R 正确：`execclient/client.go:291–331` 循环不返回 | 新增单 attempt API；PTY loop 调度 KA/input/control/output，旧 exec call 继续以其包装。 |
| broker 要解析并 stamp Run 才能安全 | T 的扩大改动不采用；`broker/exec.go:570–574,617` 是 raw overlay，`agent/exec.go:704` 用顶层 ActorFP 建 Scope | broker 只改两个 op switch；agent 深拷贝 Run 并覆盖 Run.ActorFP。 |
| 单pty-probe足以支持S3 agent降级 | 四批评的这条建议不成立：`execsession/runtime.go:238–249` 仅识别旧probe，未知pty-probe无Handle先返ErrIdentity，尚未到`:285` unsupported分支；`agent/exec.go:683`映射process_identity | 采用C的两阶段探测但修其pending错误；先probe Kind=pty，合法旧cap缺PTYVersion安全降级，再pty-probe验证新broker；绝不把identity拒绝当旧版本。 |
| 35.C 能证明把 raw continuity FAIL 包成 GREEN 合法 | P 的类比不成立；`35-hot-update-exec-carrier.sh:449` 断言 legacy 路径及诚实记录；`39…sh:324–326` 才是 assert_bug | 39.P 按原生 PRODUCT-RED；不修改 validator/runner 契约。 |
| 现 lost-reply relay 证明 reply 未交付 | R 正确：`natsrelay_test.go:107–113` 先 forward request 再 close，server→client 可抢先成功 | 在请求转发前封住对应回包方向，外部收据确认生效，再断链。 |
| 所有 session 删除都能由 register 补到 | A 正确指出边界：`agent.go:1779–1783` 只捕获 gone；`session/session.go:42–49`、`session/plan.go:23–48` 无不可复用 incarnation | finalPrepare 后丢事件且同 SID 重建无本地充分证据；Q1 必须明确裁决。 |

Linux 的 last-slave/read EIO 与 tty 状态变化同时核对了上游 [n_tty.c](https://github.com/torvalds/linux/blob/master/drivers/tty/n_tty.c)、[pty.c](https://github.com/torvalds/linux/blob/master/drivers/tty/pty.c)。这些机制说明 EIO 必须按 source domain 解释，不能推导任意 HUP/清场保证；实际支持内核仍由 T01–T08 和 drill 记录 `uname` 证明。

### 1.2 职责与永久不变量

`agent → execsession → ptysession → pty/workload`；`procrequest` 只拥有身份、intent、lease、附件、terminal；`agentimage/agentboot` 组合 image。`ptysession` 不 import agent/broker/NATS/execsession；`pty` 不 import ptysession；`execclient` 不拥有服务器 FD；本地终端适配进 `internal/terminalio`。Manager 唯一 wait/signal owner，terminal actor 唯一 master close/deadline owner。

一个 reliable PTY 只有一个 public task PID、一个 ProcessHandle、一个 runtime record、一个 `pty-task.<pid>` resource、一个合并输出流。resource ID 禁用斜线，遵守 `handoff/handoff.go:202–213` 的名称白名单；Kind 固定 `pty-task-v1`。不登记 `a.procs`，不走 gateRun；legacy 仍登记 procs、持 gateRun 到尾流结束。两个域共享同一 `workload.Manager`、registry、节点 replay 预算与 courier。不得为 reliable run 再维护第二套 ps/kill 表。

## §2 先冻结的协议、类型与接口（P0）

### 2.1 wire 唯一形状

`ProcessResumeVerb/ProcessResumeVersion/ProtoVersion` 不变；下面 **8 request + 10 reply = 18 个新增字段**，全部 `omitempty`。复用 `PtyResizeEvent` 的 `Cols/Rows`。不加 `OpSeq`、`InputClosed`、`InputFINAt` 或第二个 verb；两步能力探测见下表。

| 类型 | Go 字段 / JSON tag / 类型 |
|---|---|
| ProcessResumeReq | `Kind / kind / string`；`Run / run / *RunReq`；`Input / input / []byte`；`InputSeq / input_seq / uint64`；`InputFIN / input_fin / bool`；`Resize / resize / *PtyResizeEvent`；`Signal / signal / int`；`ControlSeq / control_seq / uint64` |
| ProcessResumeReply | `TaskKind / task_kind / string`；`PTYVersion / pty_version / int`；`InputACK / input_ack / uint64`；`InputAccepted / input_accepted / uint64`；`InputWindow / input_window / uint64`；`InputFIN / input_fin / bool`；`InputEnded / input_ended / bool`；`InputError / input_error / string`；`ControlACK / control_ack / uint64`；`ControlError / control_error / string` |

`Kind=""` 是原 exec，`Kind="pty"` 是 PTY；不接受额外 `"exec"` 别名。reply 的 `Kind` 仍是 `capability/status/error`，不能挪作 task kind。PTY 所有 reply（包括 tombstone/settled）带 `TaskKind=pty`；旧 exec 默认字段零值，不改其 hash/JSON。

| op | 合法 payload；接纳规则 |
|---|---|
| `probe` | Kind空时原 exec 合同原样；Kind=pty时同旧probe无Handle/Run/Exec，先返回完整PTYVersion及lease；旧agent忽略Kind而回合法旧cap，缺PTYVersion可在create前降级。 |
| `pty-probe` | `Kind=pty, ClientLineage`，无 Handle/Run/Exec/输入/控制；成功 `Kind=capability, PTYVersion=1, Lease`；只证明路径协议支持，不预留 child。 |
| `create` | `Kind=pty, Run!=nil, Exec=nil, Handle, AttachEpoch`；所有输入、输出 ACK、control、TerminalID 为零。复用 lease 及同 RequestID。 |
| `resume/ack/terminal-ack` | `Kind=pty, Handle, AttachEpoch`；Run/Exec 与变更 payload 必须空；输出 ACK 按原协议。terminal-ack 仍需匹配 TerminalID。 |
| `input` | `Kind=pty, Handle, AttachEpoch, InputSeq, Input, InputFIN`；允许携输出 ACK；最多 32KiB 原始 Input。空数据只在 FIN 或查询重试时有意义。 |
| `resize/signal/keepalive/hangup` | 同 Scope/Handle/AttachEpoch，`ControlSeq≥1`；分别只允许 Resize、Signal、空、空 payload；允许输出 ACK。 |

对每个 op 做字段互斥验证、溢出检查、当前 ACL、完整 Scope/MAC/boot/runtime instance/RequestID/AttachEpoch 校验；失败前无 kernel/queue/clock-renew 副作用。signal v1 只允许 `SIGINT/SIGTERM/SIGHUP`，目标是 Manager 持有的原进程组；raw `0x03` 由真实 line discipline 送当时前台组，不将两者混为一谈。resize 的 ioctl 自然作用于原 terminal 并触发 SIGWINCH。

broker `exec.go` 两处 switch：attach ACL 将 `input/resize/signal/keepalive/hangup` 加到 create/resume/ack/terminal-ack 一侧；总白名单另加 `pty-probe`。probe 仍严格 ONLINE；所有变更 op 每次查当前 session/member/node ACL。`procforward` 不改；agent 在任何异步使用前复制 `Run.Argv` 和 `Run.Env` map，`Run.ActorFP=Scope.ActorFP`。错 SID/NID/ActorFP/lineage/epoch/MAC/私有 inbox/未知 op 的矩阵逐项 T27/T28。

### 2.2 能力与错误

在同一个绝对2s总预算内先发 `probe Kind=pty`，验证Kind=capability、lease/scope/instance/CallID。合法旧cap不含PTYVersion时，在 **create 未发出** 时返回 `execclient.ErrUnsupported`；新cap声明PTYVersion=1才继续发唯一新op `pty-probe` 验证broker全路径。第二步重新验证并只使用它签发的lease，第一份不用于create；不能每步重给2s。传输可重试各自同事务。已验证旧verb/旧op/unsupported或旧端无响应也可在create前降级；`process_identity`、权限拒绝、busy、容量不足、畸形cap直接报错。旧S1/S3 broker可转发第一步，却会在第二步拒新op，于是在create前安全退回；旧S3 agent则在第一步就被识别，避免未知pty-probe提前触发identity拒绝。

S3→S4 pending 且 prev 不读 PTY 时，两步 probe 均返回真实完整能力；新 PTY create 在 Reserve 前 `process_busy`。新 Agent 打开任何订阅前就建立这一门，legacy gateRun 同样关闭；commit **落盘成功**且确认 marker 归属本 instance 后才打开。不能把 capability 隐藏成 unsupported，使新 ctl 借 legacy 开出一个不能回滚的终端。

可靠协议复用 `process_busy/conflict/identity/stale/capacity/revoked/expired/output_discarded/unsupported_process_operation` 等已注册码。legacy 队列溢出另新增一个稳定 `RunChunk.Reason="pty_input_overflow"`，归74并明确任务可能已运行，进入同一 wire-error/exit-class 双向覆盖门；不能拿原 `pty_alloc_failed/exec_failed` 错报启动失败。`InputError/ControlError` 是状态诊断，不是 `Code`；定义固定前缀 `pty input aborted:`、`pty control failed:`，后接闭合原因词 `write_error/replay_error/hangup/revoked/authority_expired/source_ended/ioctl_error/signal_error/close_error`，不携未界定 payload，长度 ≤4096。`SettledError` 包含 InputAbort 与最终 A/Q；rc0 加输入/输出 abort 返回 74，权限 77，配置 64，接纳暂忙 75；远端正常 rc23 原样，终端先恢复再退出。未知新拒绝不能误归 70 或 unsupported。

### 2.3 P0 公共边界（精确签名，实现文件见 §12）

P0 先落类型、常量、wire 和行为无变化的兼容包装；下面新函数/方法由各唯一 owner 实现，签名不得私自改。新数据面导出 API 均 ctx-free。

```go
// internal/pty/contracts.go
type TerminalIdentity struct {
    Version int `json:"version"`
    BootID string `json:"boot_id"`
    LeaderPID int `json:"leader_pid"`
    StartTimeTicks int64 `json:"start_time_ticks"`
    Dev uint64 `json:"dev"`
    Ino uint64 `json:"ino"`
    Rdev uint64 `json:"rdev"`
    MountID uint64 `json:"mount_id"`
    PTYIndex uint32 `json:"pty_index"`
    Started bool `json:"started"` // 成功 Start + Adopt 后的证明，不是 Session.started
}
type TerminalObservation struct {
    Cols uint16 `json:"cols"`; Rows uint16 `json:"rows"`
    SessionID int `json:"session_id,omitempty"`; ForegroundPGID int `json:"foreground_pgid,omitempty"`
    ControlTTYKnown bool `json:"control_tty_known"`
    Termios []byte `json:"termios,omitempty"` // 同 OS/arch 的观察字节；绝不用于 restore ioctl
}
type Master interface {
    Read([]byte) (int, error)
    Write([]byte) (int, error)
    SetReadDeadline(time.Time) error
    SetWriteDeadline(time.Time) error
    Resize(cols, rows uint16) error
    Observe() (TerminalObservation, error)
    Borrow() *os.File // 仅 frozen，调用者不得 I/O/Close/Fd
    Close() error
}
// P2：(*Session).AdoptDraining(*workload.Manager, workload.Identity) error
// P2：(*Session).TakeMaster() (Master, TerminalIdentity, error)
// P2：RestoreMaster(*os.File, TerminalIdentity, workload.Record) (Master, error)
// P2：CheckMaster(*os.File, TerminalIdentity, workload.Record) error

// internal/workload/control_contract.go；Control 只在回调期间有效
type Control struct {
    Identity Identity
    Exit *Exit
    Signal func(syscall.Signal) error
}
type SessionMember struct {
    Identity Identity `json:"identity"`
    SessionID int `json:"session_id"`
}
// P1：(*Manager).WithControl(string, func(Control) error) error
// P1：(*Manager).FreezeUntil(time.Time) ([]Record, error)
// P1：ScanSession(Identity) ([]SessionMember, error)
// P1：SignalIdentity(SessionMember, syscall.Signal) error // pidfd + 再验 boot/ticks/SID

// internal/ptysession/contracts.go
type InputStatus struct { Applied, Accepted, Window uint64; FIN, Ended bool; Error string }
type ControlRequest struct { Seq uint64; Op string; Cols, Rows uint16; Signal int }
type ControlReceipt struct { Seq uint64; Digest, Error string }
type HealthProof struct { Generation uint64; AtBootNS int64; Healthy bool }
type Batch struct { Offset uint64; Data []byte; FIN bool; Error string }
type Config struct {
    Manager *workload.Manager
    PID string
    Master pty.Master
    Identity pty.TerminalIdentity
    Input, Output *workload.Replay
    Clock func() (int64, error)
    KAIntervalMS int
}
// P3：New(Config) (*Actor,error)；Import(Config, Image) (*Actor,error)
// P3：(*Actor).AcceptInput(seq uint64, data []byte, fin bool) (InputStatus,error)
// P3：(*Actor).ApplyControl(ControlRequest) (ControlReceipt,error)
// P3：(*Actor).InputStatus() InputStatus
// P3：(*Actor).OutputBatch(ack uint64, fin bool, limit int) (Batch,error)
// P3：(*Actor).Freeze(time.Time) (Image,map[string]*os.File,error)
// P3：(*Actor).PreActivate() error；(*Actor).Activate() error
// P3：(*Actor).Revoke(reason string) error；(*Actor).Tick(HealthProof) error
// P3：(*Actor).Detach() (map[string]*os.File,error)；(*Actor).Close() error
```

`Batch` 是 P0 自有边界；现有 `Replay.Read(offset,dst)(int,error)`（`workload/replay.go:165`）不改签名，P3 组合其状态，P6 转换成 `proto.ProcessOutput`。

补充冻结签名：P4 `(*Registry).RetainedKinds() []string` 读锁下遍历所有records；P6 `(*Runtime).RequiredDomains() []string` 只读汇总用于live preflight，`RequiredDomains(Image) []string`用于最终编码，不用冻结业务才能知道domain。P6 `(*Runtime).PTYHealth(ptysession.HealthProof)` 的参数为该唯一类型。P7 新增 `(*Frozen).PrepareChecked(validate func(Bundle) error, extra ...handoff.Resource) (*handoff.Prepared,error)`：重Export并Validate之后、encode/Prepare之前调用validate；现Prepare包装它传nil，P9所有install/ReExecOnly/watchdog调用改用Checked并传纯domain检查，避免“检查Freeze旧bundle、Prepare又偷偷换了内容”的洞（现freeze.go:195–211）。

其它固定边界：P4 新增 `(*Registry).ReservePTY(scope Scope, handle proto.ProcessHandle, spec proto.RunReq, pid, inbox string, epoch uint64) (Record,bool,error)`；原 Reserve 不改签名、digest 不变。P6 给 Config 追加 `StartPTY func(string,*proto.RunReq)(*pty.Session,string,error)`、`PTYStarted func(string,proto.RunReq,workload.Identity)`、`PTYAdmission func() error`；StartPTY 成功已 AdoptDraining，Runtime 负责 TakeMaster。P6 提供 `RequiredDomains(Image) []string`（纯函数）、`(*Runtime).PreActivate() error`、`(*Runtime).Active() bool`、`(*Runtime).PTYHealth(HealthProof)`；P7 只依赖这三个 activation 边界及现有 Freeze/Export。

P10 客户端冻结为两段 API：`ProbePTY(ctx context.Context,nc *nats.Conn,subject string)(*PTYClient,error)`；`(*PTYClient).Run(ctx context.Context,spec proto.RunReq,in Source,out Sink,events <-chan ControlEvent)(Result,error)`；`(*PTYClient).Close() error`。`Source` 为 `Read([]byte)(int,error); Cancel() error`，`Sink` 为 `Write([]byte)(int,error); Cancel() error`，Cancel 必须唤醒其正在阻塞的 I/O，调用者随后 join；它们归 `internal/execclient/pty_contract.go`。P11 `terminalio.Open(in,out *os.File)(*IO,error)`、`(*IO).EnterRaw() error`、`(*IO).Restore() error`、`(*IO).Close() error`，IO 的 `Source()/Sink()` 满足上述接口但 terminalio 不 import execclient（结构接口）。先 Probe→Open/EnterRaw→Run(create)；Close/Restore 幂等。

客户端其余准确类型：`ControlEvent struct { Op string; Cols,Rows uint16; Signal int }`，ControlSeq由唯一protocol loop铸造；P10给既有Result追加 `InputAbort string; InputACK,InputAccepted uint64`。P11在其io.go定义同形的 `Source`/`Sink` 两接口，准确方法为 `(*IO).Source() Source`、`(*IO).Sink() Sink`；结构赋值给execclient接口，无反向依赖。所有实现者按这些签名编译，不以额外goroutine把任意不可取消Reader伪装成Source。

P0 同时完整冻结 §5.1 的 JSON shape、所有 enum 常量及如下预算：`PTYInputBytes=256<<10`、`PTYBatchBytes=32<<10`、`PTYRecovery=180*time.Second`、`PTYRecoveryOpportunity=5*time.Second`。它们进 upgradebudget；已有 8MiB/256MiB、500ms/5s/120s 不改。每 actor 输入/输出 replay 共预留 8.25MiB；固定一个读 Pending 和一个写 buffer 各 ≤32KiB，由同时存活 actor 数上限再作总内存核算，不能声称这 64KiB 已被 ReplayBudget 计入。

## §3 PTY owner、字节状态与控制事务

### 3.1 master 交接与内核身份

Allocate 独占新 PTY 时继续 `prepareMaster`；成功消耗 original，失败 original 仍由调用者关。`TakeMaster` 仅在 ready、AdoptDraining 成功、没有旧 I/O borrower/Wait 后执行一次；把 `Session.Master` 清空并置 transferred，原 Session 的 Close/Hangup/WriteInput/SetSize 不再有 terminal 权限。旧字段暂保留给 legacy 与既有测试，可靠路径禁止直接读取。Start watchdog 的未采用 child 清理责任仍由 Session 保留，不能把失败 Start 纳入 started domain。

import 先只读验证 CLOEXEC、O_NONBLOCK、O_RDWR、字符设备、TIOCGPTN/index、devpts mount ID、fstat tuple 和导出时 identity；两个 master 即使 `/dev/ptmx` inode 一样也不能互换。通过 RawConn 取 FD/flags/ioctl，禁止 `.Fd()`。不在 import 上调用可修改共享 OFD flags 的 `prepareMaster`。需要 poller wrapper 时只对冻结来源做 `F_DUPFD_CLOEXEC` 后包装；失败只关新 dup，原借入 FD 留给 salvage；成功才转移并关旧 wrapper。不得对同一个 descriptor 整数造两个 os.File。验证两向 SetDeadline 均支持，并在真实阻塞读/写测试证明 deadline 确实唤醒；“构造返回 nil”不是证据。

termios、winsize、当前 SID/pgrp 在 Freeze 只作观测。原 kernel PTY 自己携带 termios/队列/foreground，不重开 slave、不保存一个额外 slave keeper、不 TCSAFLUSH/TCSETS/tcsetpgrp。leader zombie 且 descendant 持 slave 的 draining image，允许 tty SID/pgrp 不再等于原 leader，甚至 ioctl 已不可用；永久证明依赖原 master identity 与 Manager 的 boot/parent/PID/ticks/ReapHeld。终态无 live master 时不再读取可能已复用的 OS PID。

### 3.2 输入、FIN 与有限内存

定义 `A=Applied=InputACK`、`Q=Accepted`；`0≤A≤Q`，输入 replay `[A,Q)`，`Q-A≤256KiB`。ACK 的单位是 **master.Write 实际返回的 n 字节**，不是 slave 程序 read/处理，不承诺 canonical line discipline 永不丢弃其规则允许丢弃的内容。ctl 保留 `[A,sentEnd)`，只凭 A 释放责任；Q 是接纳/credit 信息，不能使 ctl 丢掉尚未写入 master 的字节。

每个 input 最大 32KiB；同一 ctl 只留一个未获接纳回复的 input 包，但可有至多 256KiB 已接纳未 ACK 的数据。seq>A 的重叠部分逐字比对 retained replay；seq<A 的已应用前缀直接裁掉，不读已释放 ring；`seq>Q` gap、重叠不同、uint64 溢出均 conflict。Window=`capacity-(Q-A)`，A/Q/Window 同一锁下快照；容量不足整包不变更，不接纳 FIN，返回 status+Window，保持控制和输出 poll 可调度。正常路径不做部分容量接纳。

存储异常是另一个分支：Replay.Append 实际写了前缀后失败，Q 只增加真实保存量，发布稳定 InputError/Ended，不倒退、不把未存 suffix 算成功；伴随 FIN 不提交。write(n,err) 无论 err 是 deadline/EIO/其它，先 `A+=n` 并 Ack 输入 log，再分类；自然短写继续 suffix，freeze deadline 是暂停而非 InputError。真实 write EIO 永远是 input abort，绝非 FIN。

InputFIN 仅在整个请求 data 接纳后锁定 `F=Q`；reply `InputFIN=true` 时 Q 即固定 FIN 终点，无须新字段。到 `A==F` 置 InputEnded=true，停输入源，**master 不关、不发 HUP、不注入 0x04**。FIN 后仅允许对已接纳区间的合法重试；不同 F/新数据拒绝。FIN 已接纳未 applied 随 image 携带。Write 故障、Hangup、撤权、source 已终止则 `InputEnded=true, InputError!=empty`，保留最终 A/Q（可以 A<Q），结算不再等待永远不会到来的 A==Q。ctl 收到这种状态仍交付/ACK 尾输出与真实退出，再以74报告未应用输入，不能让 rc0 掩盖。

Ctrl-D 是输入字节：canonical 的 VEOF、raw 的普通 0x04、自定义 VEOF 分开测试。escape/UTF-8 允许从任意字节切开，产品不做字符修复或 flush。

### 3.3 合并输出、Pending 与尾流

PTY stdout/stderr 本来共用 slave，wire 只用 Stdout 承载合并有序流；Stderr 是无 FD、无预算的空流，固定 `Offset=0, FIN=true`，拒绝非零 stderr ACK。每 PTY 输出 replay 8MiB、满则停读形成背压；ctl stdout 写入实际 n 后才能 ACK n，writer 短写不能 ACK 整批。FIN/Error 只在抵达 final End 的最后一批出现，和已有 ProcessOutput 合同一致。

分开四件事：leader exit、SourceEnded、输出对客户端已逻辑结束、reader 是否仍在 discard-drain。started+身份已验证的 master **Read** 在 last-slave EIO 时先保存 n>0，再置真 FIN；普通 pipe、Allocate 未 Start、Start 失败、write EIO、人工 Close、deadline、校验失败都不借用此规则。人工 Hangup 关闭造成 reader wakeup 记 aborted（除非它此前已见真 FIN）。

读到 n 后 replay 写失败：先停止/唤醒并 join 在飞 read，完成所有已读字节的保留决定，再固定 `End=最后可持久保留前缀 E` 和 OutputError；无法写进 replay 的 suffix 保存在 `Pending≤32KiB` 作责任/诊断，**不属于可交付 `[Base,E)`，以后绝不追加到 E 后**。错误发布后 End 不增长。actor 接着进入 discard-drain，持续读真实 source 到最后 slave end；discard 数据不再进入 replay，不能把逻辑 abort 当成内核 EOF。既有 Output.Abort API 不直接复用为此 finalizer。

正常顺序：AdoptDraining→读真实 source end→AllowReap→Manager.Wait→记录真实 terminal/courier exit→等 terminal-ack→资源释放。leader 先退出也不启动任何 tail timeout；T02 的 descendant 在 >200ms 后再写，跨 exec 后必须完整。输出因废弃/撤权被截断时仍保持 reap hold，直到 source 真结束或明确管理清场/显式 master close 完成。HUP-immune child 可继续 RUNNING，KA Hangup 不升级 SIGKILL；状态不可伪造 terminal。

### 3.4 controls、Hangup 和锁

ControlSeq 从1开始，只接纳 `last+1`；重复 last 且 canonical digest 相同返回原 receipt（包括失败）；相同 seq 不同内容 conflict，更旧 seq stale 不执行，gap conflict。一个 ctl 至多一个未确认 control，保留 last receipt 足够有界。CallID 只关联回复，先验 CallID 再读任何 ACK/状态，不能当副作用去重键。新 resize 在已有未确认 resize 完成后才合并到最新值；不能改写待重试操作的 payload。

锁纪律：Runtime 只短持状态锁取得 task 引用/登记 revocation；Master Read/Write、pause/join、/proc census、网络 probe 均在 Runtime/Manager/state mutex 外。每 actor 有可截止等待的 control token，pump 不取此 token；actor state mutex 只记账，Replay 内锁排最后。Hangup 先占 control token、暂停两泵并 join，再进 `Manager.WithControl`；回调内做有界 Signal、Close，随后仍持 Manager guard，短持 actor state mutex 提交实际关闭状态与 receipt，不 join。唯一嵌套顺序为 control token→Manager guard→actor state→Replay；任何路径不得持 actor state 再取前两者。Manager Freeze 与整个回调同一 guard，故没有可导出 Signal 后 Close 前的状态，也没有无锁发布 receipt 的数据竞争。

| 情况 | master/输入/control receipt | 恢复与重试 |
|---|---|---|
| frozen/unknown/身份错误，guard 未执行 | 不 Close、不消费 seq、不置 reaped | frozen 按 fence 等待/重试；恢复原两泵；unknown/identity 拒绝并保 owner 待检查。 |
| 同一保留行已 exit 或 Signal 返回 ESRCH | 无需再 signal，允许关闭仍属本 actor 的 master | 原子记录 receipt；input aborted；不能误操作复用 PID。 |
| Signal 成功 | 同 guard 内 Close，写 receipt，InputEnded+hangup | 即使回包丢失也不再 Signal；KA 的 reap 标志只在该提交点设置。 |
| Signal 其它错误 | 不 Close，失败 receipt 消费该 seq，恢复原两泵 | 客户端看到 ControlError；若要重试副作用必须新 seq。KA 内部失败保留责任，下一有界 tick 新事务重试并限频告警。 |
| Close 返回错误 | Signal 已发生，保存 close_error receipt，不再 Signal；保留实际 closed/uncertain 状态与 cleanup 责任 | 不伪称回滚未发生；若 FD 已被关闭不得重建 PTY；下一代只续做可验证 cleanup。 |

不补 SIGCONT。stopped foreground/HUP-immune 的 Hangup 只证明明确 HUP 与 master close；管理撤权是另一个 SIGKILL 清场路径，见 §6。

## §4 Freeze、import 与失败状态表

### 4.1 完整截止与双向屏障

Freeze 使用调用者给的一个绝对 deadline，覆盖 Runtime 锁获取、control token、两泵 join、replay freeze、Manager guard；不能先无限 `mu.Lock()` 再开始计时。对短 mutex 用 deadline-aware TryLock 等待，对长活动用 generation/ack channel。协议为 active→freezing→frozen；先关闭 intake，确认未有 starting（仍以 registry.Freeze 为唯一判据），同时给两向独立 deadline，等待每个旧 pump generation 在完成 `n>0` 记账后确认停止，才冻结两份 replay。

owner 是唯一 SetReadDeadline/SetWriteDeadline 调用者，不用 `SetDeadline`；清读 deadline 不得清写，反之亦然。每方向恢复只有旧 generation done 后才可启动下一泵。timeout 返回 deferred，不 Close master；撤销 freeze 后旧泵在记账屏障退出时唯一地恢复该方向，不能“双泵”也不能“留下永停方向”。如果不能取得安全屏障则不允许 Export/Prepare；不能用 deadline 早退伪造 frozen。

Runtime refactor 不把 blocking operation 放进 `Handle` 所持 r.mu。Freeze 等待期间允许 Revoke 登记不可逆责任，最终 Prepare 再取一致状态；Handle 的 frozen 等待保留 S2 的透明重试行为。Manager 新 FreezeUntil 与现有 Freeze 兼容；旧 API 不删除。

### 4.2 导入三段式与激活边界

1. **纯校验**：整个 registry/tasks/tombstones/revocations、kind、A/Q/FIN、control digest/receipt、budget、文件名集合、全局数量、boot/identity/deadlines 先验证，不取 FD，不 signal/reap，不改变 tty。
2. **取得所有权但冻结**：各资源先只读 Check，逐个 TakeFile/RestoreMaster/RestoreReplay；第二 task 失败时前面已取的资源以原 kind 完整加入 Salvage，未取的 Borrow 合并；失败方不能丢 master 或改 O_NONBLOCK。Manager 仍冻结，任何泵都未启动。
3. **预激活后提交**：所有 actor 的 PreActivate 校验、可执行的 deadline 清除准备及撤权输入封闭全部成功后，才越过 active 边界。`Imported` 与 `agentboot.Image` 在 Manager.Thaw **之前**标注资源激活责任（即使随后失败），禁止走 pristine Detach。Manager.Thaw 后先执行已撤权/绝对授权到期的终止策略，再启 output cleanup/drain，最后仅启合法 input/control；registry/Handle admission/courier/register 依序开放。

激活不能以每泵无界启动回调为失败源；所有可提前验证的步骤放 PreActivate。仍注入“第二 actor 启动失败”验证 active 失败分支：保住已接管资源，封闭未成功方向，有限重试激活；需要回滚时重新 Freeze **当前 owner 集合**、重 Export、Prepare，绝不重派最初 bundle。若安全点暂不可得，保持可运行方向与 cleanup 责任、`rollback_blocked` 有界重试，不能 Close 来凑 rollback 截止。

当前态cut允许同一runtime内“已active actor”和“尚冻结actor”并存：对已冻结actor的Freeze返回其当前冻结快照，不重建、不先Thaw；对active actor执行屏障。Image的资源责任标记可先为active，但成功的`image activated`日志、ready与marker统计只在全部应启方向就绪后发布，不能用partial activation冒充冻结窗结束。

| 窗口/失败 | owner 与动作 | 允许再派发的数据 |
|---|---|---|
| 纯校验失败、尚未 Take | held/refused，原 master 不动 | 原 bundle + Borrow |
| 第二资源导入失败、全部未 active | 冻结 Salvage+Borrow；新建的 wrapper 只关闭自身无权副本 | 原字节与全部原资源，MergeResources 按 kind 校验 |
| PreActivate 失败、Manager 未动 | 保持 imported/frozen；可 Detach | 原 bundle；确认 OFD/原 FD flags 未改变 |
| Manager 或任一 pump 已 active 后失败 | active failure；保留 runtime，不能 Detach/Discard restored table | 只准当前态新 cut，generation+1 |
| 本地 exec syscall 失败 | 关 Prepared 副本→Manager/Runtime 按撤权顺序 Thaw→开业务 gate→磁盘/marker recovery | 原在用 owner 不丢；下一次再 cut 当前态 |
| watchdog 120s 到期 | 维持 install interlock，cut 当前态；deferred 5s 重试 | 新 A/Q、output Base/End、controls、KA、revoke/terminal，全量重导 |

## §5 image schema、候选与回滚兼容

### 5.1 仅 additive，kind-aware 文件集合

原 exec `TaskImage` 和 resourceKind 原样。追加 `procrequest.Record.Kind string json:"kind,omitempty"`、`execsession.TaskImage.Kind string json:"kind,omitempty"`、`TaskImage.PTY *ptysession.Image json:"pty,omitempty"`、`TombstoneImage.Kind string json:"kind,omitempty"`、`TombstoneImage.PTY *ptysession.Tombstone json:"pty,omitempty"`。新 fail-closed revocation 扩展 `RevocationImage.ClockUnknown bool json:"clock_unknown,omitempty"` 归 `pty-v1` 扩展语义；即使没有 PTY task，只要该扩展非零，旧 reader 也不得接收。其余旧字段类型/tag/名字不动。

P0 `ptysession.Image` 固定 JSON（字段名见下表；snake_case tag 与名称逐一对应，如 `KADeadlineBootNS`→`ka_deadline_boot_ns`；Version=1）。Image 与 Tombstone 自有字段均不加 omitempty；仅外层 PTY 指针和上文 Kind/ClockUnknown 省略零值，保证纯 exec 不出现任何新域。TerminalObservation 的可选 tag 按 §2.3：

| 字段 / 类型 | 合同 |
|---|---|
| `Version int; Identity pty.TerminalIdentity; Observation pty.TerminalObservation` | 成功 Start 证明与当前观察分离；只有持 master 的状态要检 master。 |
| `Input workload.ReplayState; Output workload.ReplayState` | Input.Base=A、End=Q；Output.Base/End/FIN 为合并流。 |
| `InputFIN bool; InputEnded bool; InputError string` | FIN 固定于 Q；Ended 可 A<Q，仅在非空错误时合法。 |
| `OutputError string; Pending []byte; Discarding bool; SourceEnded bool; MasterClosed bool` | 最终 End 一旦 Error 发布不可变；Pending≤32KiB；SourceEnded 与 logical ended 分离。 |
| `ControlSeq uint64; ControlDigest string; ControlError string` | last committed 控制收据，初始 seq0 时其它为空；不能以普通 ack 推进。 |
| `KAIntervalMS int; KADeadlineBootNS int64; ExpiredSinceBootNS int64; RecoveryNotBeforeBootNS int64; NextJudgeBootNS int64; LastKASeq uint64` | §6.1，绝对 CLOCK_BOOTTIME。连接 proof 不序列化为有效健康证明。 |
| `AuthorityDeadlineBootNS int64; Revoked string; CleanupPending bool; CleanupMembers []workload.SessionMember` | authority0=没有独立到期源；清场责任可跨 cut，成员数有界、完整超界则 deferred。 |

`ptysession.Tombstone` 只保留 `InputACK/InputAccepted uint64`、`InputFIN/InputEnded bool`、`InputError string`、`ControlSeq uint64`、`ControlDigest/ControlError string`；外层保留已有 terminal、finalEnd/FIN、AtBootNS。正常 settled registry 保留 Kind；不必在已 settled Record 重复输入诊断（持终端收据的 ctl 负责报告）。新字段 tag 全入 image inventory，Image/Tombstone 不含函数、mutex、连接或绝对 FD 号。

| PTY 状态 | resource kind / Files | image 必要条件 |
|---|---|---|
| running / draining / frozen | `pty-task-v1`；`master,input-replay,output-replay`，每名恰一份 | identity 匹配，A≤Q，两个 replay 校验；无 stderr FD |
| logical input FIN，但 source 活 | 同上 | FIN 不删除 master；输入 log 到 terminal 前保留，可能为空 |
| Hangup 已关闭 master，child HUP-immune | `input-replay,output-replay` | MasterClosed、InputEnded、output clean FIN 或 aborted；Manager 行仍可 RUNNING |
| terminal 未 ACK | `input-replay,output-replay` | master 已关闭；真实 terminal，最终 A/Q 与输出终点可结算 |
| discarded tombstone / settled record | 无 resource、无 FD、无 replay budget | kind 和最终责任仍在 metadata，旧 reader 能力检查仍看见它 |
| spawn 失败 | 已 Reserve 的同 handle terminal；无 master，可有已分配 replay | 无假 started identity；退出/错误可查询；不能再 spawn 相同 request |

预算在 child Start 前预留：Runtime 持 admission 查看 duplicate→新请求分配两 replay→ReservePTY→启动；Reserve 失败释放预留，duplicate 路径不受新容量检查误拒。spawn 后任何设置失败杀/清理已采用 child，但保留同 handle 的真实 terminal 与可交付前缀；不能退回“未运行”的容量拒绝。metadata≤16MiB、handoff FD 总数≤16384；从实际 kind/文件表计数，不用“所有任务六 FD”近似堵死容量。Pending 的总量单独上限检查并记 evidence。

### 5.2 domain 检查精确时序

`proto.ImageDomainsLine()` 单源发 `image-domains exec-v1 pty-v1`；最后一行仍 ImageABILine。无 domain 行而 ABI1 的 S3 等价仅 `exec-v1`；无 ABI 仍仅允许完全空 image 的 bare exec；ABI>1 仍一律拒绝。重复/畸形声明 smoke_failed；未知 domain token 可忽略为“此 reader 另有能力”，不能声称自己有未知能力。所有能力与 probe 所执行文件的 SHA 绑定。

将 `checkCandidateABI(a,abi)` 升级为包级 `checkCandidateABI(a *Agent, candidate imageCapabilities) error`；`imageCapabilities{ABI int; Domains []string; SHA256 string}` 由一次现有隔离 version probe 读取。旧 smokeVersion/ImageABI 包装继续存在，避免新增 Agent method。错误仍 `image_abi_unsupported`，保留 ABI 错误原字符串；domain 缺失用 `upgrade: candidate image domain is unsupported: requires pty-v1` 明确诊断，CLI hint 不误说没有 ABI 行。

检查三次：①安装临时候选 smoke 后、任何 prev/marker/dst mutation 前，以一致 live retained-state summary 检查；②gate closed、Freeze 成功后从实际 bundle 检查，捕获下载时新建；③`Frozen.Prepare` 重 Export 后、Prepare/Exec 前再检查，捕获冻结期 revocation 扩展。Preflight 在下载前可判本地 legacy/domain 约束并保持只读；候选内容只在下载验证后才可断定，不能谎称未知候选也能提前判 ABI。

`RequiredDomains` 遍历最终将编码的 `Registry.Records`（含 settled）、Tasks、Tombstones、Revocations、控制/cleanup 状态和 resource kind；不能仅 `len(TaskPIDs)>0`。纯 exec 所有新增字段零值省略，逐字用真实 S3 reader 验证能 Decode+Restore；不能以当前 reader 反序列化旧 fixture 冒充向后兼容。

prev 选择保持代码 `upgrade.go:726` 的 SHA 集合规则，再叠加能力：dst==running 取当前 reader 能力；dst==candidate 取已验证 candidate 能力；任何第三 SHA 或 running hash 失败拒绝。marker 追加 SHA-bound `candidate_image_domains/prev_image_domains`（以及匹配 SHA）用于 pending 准入，不输出秘密；旧 marker 无声明保守按 actual prev 的受控 probe 判断。每次回滚在切换前重新验证 pinned prev SHA；共享 marker 只由 `markerTargetsThisAgent` 目标方支配。

rollback 目标若缺 pty-v1，pending 期间禁止产生 PTY 扩展状态；对 S4→S3 且 retained PTY 非空在 mutation 前拒绝。S3→S4 第一次迁入已有 exec 可以运行，但新 reliable/legacy PTY create 等 commit。S4 sibling 在 dst==candidate 时 prev 就是 candidate，不必再要求它等于 exporter；如两者都声明 pty-v1，活 PTY 可以交接。不可用“清空 tombstone/settled metadata”伪造可读性。

## §6 KA、撤权、session_destroyed 与清场

### 6.1 跨代绝对时间与健康证明

沿用 KAIntervalMS=0 不武装；正值规范到 [1s,60s]，grace=`max(6×interval,180s)`。创建成功时唯一初始 grant；只有 **新 ControlSeq 的 keepalive 首次提交** 才置 `KADeadline=BootNow+grace`，清 expired 判定。重复 keepalive、poll/ACK、Import、NATS reconnect、恢复本地 pumps 都不得 restamp。现 `run.go:853–856,1066–1069,1089–1095` 的 legacy 重盖时间不得进入 reliable actor。

KA expiry 不单凭连接状态杀任务：每次判定需要当前 NATS connection generation 的新 round-trip proof，两次独立成功且期间 deadline/LastKASeq 没变才 Hangup。probe 在锁外，回到 token 内复核 generation、scope、deadline 与 revoke。导入和连接更换废弃旧 proof，但不清 `ExpiredSince/RecoveryNotBefore/NextJudge`；第一次发现过期时固定一次 `RecoveryNotBefore=max(KADeadline,首次过期时刻)+5s`，同一 expiry 跨代不再重置。导入时 NextJudge 已到即安排判定，不另等一个启动周期。持续健康且没有新 KA 时，在这一次机会后两个 reaper tick 内采取 Hangup；重复导入不能无限授予五秒机会。网络持续不健康只能保留过期责任，不能据此声称 ctl 已死；健康恢复后无第二次新 grace。

`AuthorityDeadlineBootNS` 有真实外部授权期限才设置，零代表未提供；create lease 的默认30s只管首次 Reserve，**不是会话授权 TTL**。authority 到期必须在输入继续写前按撤权路径处理，不能等待 KA 双 proof。当前产品没有不可复用 session incarnation/独立 authority TTL 的事实如实记录，不用造一个30s deadline 掩盖。

BootNow 失败：不接受新 create/KA grant、不推进 GC 或 terminal TTL；Revoke 先在 Runtime 登记该 SID 与 task 的不可逆标志，再读钟，失败保存 `ClockUnknown=true`。该标志阻止任何该 SID lease 新建/复活直到重新建立可证明的授权边界；恢复时钟后不能把标记挪到更早，也不能把未知时刻写0当已过期。frozen/start/active 三窗口都测到，旧 exec 也不能旁路此拒绝。

### 6.2 撤权优先、跨 PGID 责任与 tombstone

事件路径保留 `agent.go:1051–1059` 的 daemon 不主动退出；register-gone 两码保留 `:1779–1783` 的撤权后权威退出，测试不得对两者都断言“agent 仍在线”。Revoke 先禁止新 input/control/KA，唤醒正在阻塞的 write 并把其已返回 n 记账；已经进入 kernel 的字节无法撤回，严格“零新输入”oracle 从 Revoke 完成关闭写入口、或 frozen 导入开始算，不能否认已在飞 write。

冻结期只记责任；Prepare 最后重导；import 在任何 queued input 恢复前清场。启动期在 allocation/start/adopt 每个分界重查 revoke；Start 成功后收到撤权则不启输入，唯一 cleanup owner 收养并清理，避免 orphan。输出满时固定 End 后 discard-drain，不能等已撤权 ctl 的 ACK。

管理清场不等同 KA Hangup。先 SIGKILL 持有原 PGID，再扫描与原 leader 同 session 的其它 PGID/成员（包含 stopped foreground）；对每个 PID 获取 pidfd、再验 boot/ticks/SID 后发 SIGKILL，循环直到两次稳定 census 无匹配成员。扫描/kill 不持 Runtime/Manager mutex；每批有界，cut 只在批间暂停，`CleanupPending/CleanupMembers` 随 image 携带。遇 /proc 不可读/身份不符/持续 fork，不报“清完”，保留责任和诊断；有限元数据超界 defer cut 而非漏掉成员。leader 的 reap hold 保到 source/cleanup 完成。`setsid` 脱离会话的后代不靠猜 UID/PID 范围杀，范围裁决见 Q2；不能把同 SID census 写成任意子树 containment。

reliable PTY 的 revoke、output_discarded、terminal tombstone、settled record 都保留 Kind、最终输入 A/Q/错误、合并输出 End/FIN、控制 receipt 与真实 exit。晚到 input/signal/KA/resize 不恢复 master，不 renew，错 scope 不能取 tombstone。持完整 terminal/两输出终点/输入终点的 ctl 在 terminal-ack reply 丢失后，才能把原 lease 已过期且 settled 被 Collect 的拒绝理解为已完成；没有这些证据的 `process_expired` 一律失败。

### 6.3 可证明的删除边界

覆盖事件在 Freeze 前、Freeze 后/Prepare 前、Take 后/Activate 前、active 四窗口；另测 finalPrepare 后事件丢失且 session 持续 gone，由首次 register-gone 清场。这是本阶段实际可做的链路。

**finalPrepare 后丢事件 + 同 SID 已被硬删重建**：当前 wire、数据库 session projection、lease 都没有 authority incarnation，新 agent 的首个 register 可能成功；本地 Export 次数无法补出该信息。Q1 推荐把此反例单列未覆盖并冻结范围文字；未获 lead 接受范围缩窄前，C-10 完整覆盖及 C-11 全项均不能打勾。若 lead 要求本阶段闭合，必须另立 broker/DB/wire incarnation 的范围修订和独占包，不能让实施者偷偷扩 `procforward` 或假写测试通过。

## §7 agent、ctl、legacy 与 Darwin 接线

### 7.1 agent 接线与 `pty_busy` 移除

`run.go` 保留 handleRunForwarded legacy；抽包级 `startRunPTY(a,pid,req)` 共用 spawnPolicy、safe argv/env/cwd、Allocate/Start、身份、adoption 与 courier started，可靠 StartPTY 走它。可靠 audit 一次 `agent: run ... reliable=true`，相同 request 的重发不多生 child；broker create audit 可多条（`broker/exec.go:627–632` 明文如此）。warning 只写 agent 日志或状态，不插入原 PTY 输出。

legacy `.in/.resize/.ka` 先按 `PtyNodeHeader` 丢双发布副本，再查只含 legacy 的 procs。wildcard callback 不直接 `WriteInput`：每 PTY 有一个有界内存队列和 writer，enqueue 非阻塞，慢 PTY 不堵第二 PTY。容量满发一次 RunChunk failed（Reason `pty_input_overflow`，日志 Detail `pty input buffer full`），挂起该 session 输入并触发明确 Hangup/cleanup 责任；绝不静默丢字节或无限开 goroutine。resize/KA 也进同 owner。legacy 的尾流改为真实 source end→AllowReap→Wait，无200ms替代 timer；wire subjects/ready/attach/双发布兼容不改。

删除 `ReasonPTYBusy` 及 DeferredReasons 的活跃成员，11→10；历史文档/收据可保留该字面量。`legacy_session_busy` 合并保护：先读取 gateRun+gateExec 的 inflight，再查 procs 和 Manager 中非 Runtime task 的行，各 busy 谓词取并集，任一非空即拒绝，避免漏掉 admission→owner 交接；未 attach、分配中、spawn 中、draining legacy 均拒绝。旧代码 `upgrade_image.go:180–198` 把 procs 计入“owned”排除在 legacy 之外，这一分支必须重写，不能只删第一项。可靠 PTY 全在 TaskPIDs，不能误计 legacy。保留其余10码的逐项制造/晚到/exec失败测试。

保持 `cutMu` 与 install/flock 独占责任；本阶段不改 S5 expose/proxy/SS gates。ps/kill 继续统一 Manager；legacy Session 只作为旧调用入口委托，不新增 Agent receiver method。Agent production 文件数仍19内，剥注释代码<8000；需要更大状态机就提到子包，不能删事故注释换空间。

### 7.2 reliable ctl 的可调度循环与恢复窗

保留 run 的参数、Safe/Cwd/TERM/locale、alerts gate；无新 flag/无新 cmd 生产文件。Probe 成功后本地 IO/raw 初始化完成再 create；首输出 subscriber 预建，replay 保第一字节，无需 legacy ready/attach。ready timeout 到期且 CreateSent=true 返回74，不能 legacy 重开。

execclient 一条 transport owner loop、一次最多一个在飞 RPC，使用新增单 attempt（≤500ms 且不超过外层 deadline）调度；旧 `call` 包装它供 exec 回归。每次 retry 可换 CallID，但 Handle/InputSeq/ControlSeq 不变；迟到 reply 先过滤 CallID。input 接纳等待 credit 时仍可 poll output、KA、control；KA/control 优先级有界，不饿死输出。stdout writer 独立有界单批 worker，只回实际写入进度给 loop；blocked stdout 不冻结 KA。ctl 自己的待 ACK input≤256KiB、单输出批≤32KiB，断链不继续无界读 stdin。

create 已送出的 PTY 才启用180s absolute recovery：从首次未获有效答复/可重试 node_offline 开始，timeout/no responders/reconnecting/node_offline/agent_upgrading/process_busy 都重试原 handle；每个失败不重置窗口，只有同实例合法 status 回来才结束本次失联。到期74，保留“task may have executed”信息；权限/instance lost/坏 MAC/revoked/unsupported-after-create立即终止，无 fallback。120s watchdog + cut/boot/register 有恢复余量，但不承诺必在原 KA grace 内回来；W 在故障前取新 KA 收据，并硬验回连未授予新 grace。健康正常新 run 仍≤5s started，长业务本身不要求5s退出。

### 7.3 本地 FD、信号和 Darwin

terminalio 对 Linux TTY/FIFO 用 `/proc/self/fd/<n>` 重开独立 OFD，`O_CLOEXEC|O_NONBLOCK|O_NOCTTY`，再验 dev/ino/type；不能以 dup+SetNonblock 改原 stdin/stdout OFD。自有 wake FD/poller 唤醒 read/write，Cancel 后 join；只关自己 FD，原 flags/termios必须恢复。不支持无法安全重开的 socket/任意 io.Reader；regular file 非 pollable，可靠模式在 create 前明确64拒绝，建议用户用本地 pipe 驱动；不承诺远程文件系统阻塞可取消。这是固定实现边界，不放任 worker 靠进程退出回收。

raw 0x03 作为数据送远端。外部 SIGINT/SIGTERM 采用根 NotifyContext 的本地取消语义（Q3 推荐）：停止输入/KA，有限 best-effort控制不作为远端已收到 Signal 的承诺；清理/join、Restore、再返回；可靠 run 不再装一个与 `main.go:98` 争抢非 TTY SIGINT 的 handler。显式 signal op 在协议/kernel测试覆盖。正常 stdin EOF 只发 InputFIN、继续收输出；本地取消不等同 EOF/自动远端 SIGKILL。

Darwin 不广告 PTYVersion1，probe pre-create unsupported，进入原 legacy backend；portable legacy owner 用其平台支持的 Close 唤醒/队列，不引用 workload.NewReplay、Linux `/proc`、Linux image/deadline校验。保留 master_other resize。Linux actor/身份文件有正确 build tag，unsupported 文件有编译 stub，platform refuse不偷偷 fallback在create后。实际 Darwin run、resize、取消/termios收据与 `GOOS=darwin GOARCH=arm64` 编译/vet-tags 两栏记录；缺 runner 标 NOT_RUN，不能声称 stage done。

## §8 测试、变异与非空前提

以下每行是实际测试责任，不是只写字段 round-trip。Txx 对应的 mutation 在独立临时 checkout/overlay 中一次只改一处，保存 baseline green、mutation diff/SHA、精确命令、rc、失败断言，恢复后再绿。测试禁止 fork `go test`；同 PID exec harness 用当前 test binary 或共享真实 product build，按既有 spawnexec init 分派。新测试名不得替换/删掉原清单行。

### 8.1 kernel、actor、registry（P1–P4）

| ID / 测试名 | owner / 文件 | 必须断言的非空事实 | 必跑变异 |
|---|---|---|---|
| T01 `TestPtyEIOOnlyInStartedDomain` | P2 `internal/pty/terminal_test.go` | 真 started PTY 写尾字节后 rc23、read EIO时master仍开且FIN；同一分类器的未Start/失败Start/通用pipe/write EIO/deadline/人工Close各不FIN；n>0先保留 | EIO全吞EOF；资格改看Session.started；先处理err再n，分别红 |
| T02 `TestPtyTailOutlivesLeaderExit` | P3 `internal/ptysession/output_test.go` | leader已为Z、ReapHeld=true；后代持原slave，确认超过旧200ms才写tail，跨Freeze/exec后原ctl收到完整tail+23；无额外slavekeeper | 恢复200ms Close；leader exit即AllowReap；import强制TIOCGSID==leader，分别红 |
| T03 `TestPtyResizePreservesPollability` | P2 `internal/pty/master_poll_test.go` | resize前后阻塞读与堵塞raw slave的写deadline实测有效，原O_NONBLOCK不变、真实远端winsize变化 | resize改File.Fd；清O_NONBLOCK；只检查SetDeadline返回，测试必须仍能抓真实等待超界 |
| T04 `TestPtyActorSurvivesSamePIDExecWithBlockedSlave` | P3 `internal/ptysession/image_exec_test.go` | raw slave明确不读；一次真实master.Write记录attemptLen、`0<n<attemptLen`、error、调用序；冻结Applied恰n；同PID exec后原master index/boot/ticks、原slave完整suffix无重无丢；输出同时非空 | Applied按len而非n；丢suffix；冻结早于记账；重新Allocate，分别红 |
| T05 `TestPtyLogicalFINIsNotTerminalEOF` | P3 `internal/ptysession/input_test.go` | canonical/raw/customVEOF三臂；FIN已接纳未Applied跨exec；只逻辑FIN时slave继续可输出且无HUP；真实0x04按当前termios | FIN→Close、FIN→SIGHUP、FIN→注入0x04三个独立变异；提前把FIN终点置于部分接纳前缀 |
| T06 `TestPtyDuplexDeadlineIsolation` | P3 `internal/ptysession/fence_test.go` | 只停读时写能推进、只停写时读能推进；清一侧另一侧仍有deadline；freeze timeout后两向均恢复且每方向只有一个pump | 改SetDeadline；交叉清deadline；超时直接起第二泵；漏恢复一泵 |
| T07 `TestPtyFreezeDeadlineIncludesControlAdmission` | P3 `internal/ptysession/fence_test.go` | control token被真实操作占有，Freeze从调用时起有界；Runtime/Manager锁等待同一deadline；不关master、不等ctl ACK | 把deadline移至拿锁后；持Runtime.mu阻塞Write；Close唤醒当freeze成功 |
| T08 `TestHangupSequenceNeverSplitsAcrossSnapshot` | P3 `internal/ptysession/control_test.go` | Signal后阻住Close、并发Manager.FreezeUntil不能拿到中间态；反序freeze先到则HUP=0/master开/seq不变；失败解冻双泵可用 | Signal与Close拆guard；提前seq/reaped；冻表仍Close |
| T09 `TestPtyHangupFailureReceipts` | P3 `internal/ptysession/control_test.go` | frozen/unknown/exited/ESRCH/EPERM/close_error完整表；同seq丢reply再发HUP一次；stopped/HUP-immune不伪造exit | 拒绝也Close；失败receipt不保存；自动SIGCONT或SIGKILL |
| T10 `TestPtyMasterTransferAndWrongDescriptorRefusal` | P2 `internal/pty/terminal_test.go` | Take一次、旧Session.Close不关新owner；两个真PTY互换master、非TTY、错误mount/index、blocking/缺CLOEXEC拒绝且原FD flags没变 | 再造同FD wrapper；import prepareMaster修flags；仅fstat鉴别两个ptmx |
| T11 `TestPtyInputCreditAndOverlap` | P3 `internal/ptysession/input_test.go` | Q-A达到256KiB时整包拒接且control可执行；重复/部分重叠/已Ack前缀/差异/gap/溢出；FIN整包原子；ctl不凭Q丢数据 | Accepted当ACK；部分容量接纳仍置FIN；不比retained重叠；用输入Applied阻塞control |
| T12 `TestPtyInputAbortStillSettles` | P6 `internal/execsession/pty_test.go` | 注入真实write故障和replay写前缀失败，A<Q+InputError/Ended；输出完整、远端rc0仍settled error74；跨image结果不变 | write EIO转FIN；清InputError；terminal-ack要求A==Q导致永等 |
| T13 `TestPtyOutputAbortFreezesFinalEnd` | P3 `internal/ptysession/output_test.go` | Read的n已发生、Append前缀失败，有真实非空Pending；最后一批才Error；发布后End永不增长；discard-drain仍等最后slave | Abort不joinread；把Pending追加到Error后；logical abort直接AllowReap |
| T14 `TestPtyControlReplayDoesNotRepeatSideEffects` | P3 `internal/ptysession/control_test.go` | 首次SIGINT由子程序handshake确认后才重发；resize第一次后子程序主动改尺寸，重复旧seq不得覆盖；gap/conflict/stale可判 | 去掉digest/seq fence；只按CallID去重；重复resize执行 |
| T15 `TestProcessControlGuardAndSessionCleanup` | P1 `internal/workload/control_test.go` | WithControl与Freeze/Wait互斥；同SID不同PGID及stopped子进程清场；错boot/ticks/SID不signal；扫描失败保Pending | guard仅围Signal；只kill原PGID；去identity复核 |
| T16 `TestSessionCleanupSurvivesSamePIDExec` | P1 `internal/workload/control_exec_test.go` | 清场一批后freeze，剩余同SID成员及责任跨exec完成，终态前无残留；明确setsid逃逸反例不声称覆盖 | 导出省CleanupPending；单次census就报告完成 |
| T17 `TestPTYIntentAndSettledKindSurviveImage` | P4 `internal/procrequest/pty_exec_test.go` | normalize初始size、KA、nil/empty map/slice；Run.Env按key规范、ActorFP不参与intent但Scope验证；argv/cwd/Safe/env/size/KA变化冲突；settled Kind保留到lease到期 | 丢Kind；把PTY转ExecReq hash；map迭代拼hash；过早Collect |
| T18 `TestPtyRepeatedLifecycleReleasesResources` | P3 `internal/ptysession/lifecycle_test.go` | ≥5轮真实spawn/双泵/背压/两次cut/import/failed import/partial activate/terminal-ack；owner活着时master/replay/pidfd归基线，直接调用共享leak helper | 留一个master dup、停一泵不join、释放错误budget，分别红 |

T17 的 intent 前缀固定 `tether-pty-intent-v1\n`；Run.Env 是 **map**（`proto/messages.go:813`），不采用草案“保留 env slice 顺序”的说法。canonical JSON 对 map key 排序；初始size任一≤0采用80x24，>65535拒绝；resize必须1..65535不做溢出截断；KA负值拒绝，0关闭，正值规范后hash。原 exec digest 不改。

### 8.2 runtime、image、agent、升级（P6–P9）

| ID / 测试名 | owner / 文件 | 断言 | 必跑变异 |
|---|---|---|---|
| T19 `TestPTYImageClosedMasterAndTombstones` | P6 `internal/execsession/pty_image_test.go` | §5.1每种文件集合、两个tasks第二个失败、空stderr、terminal/settled/tombstone、master closed但child live；salvage可再派发全部原资源 | 固定六FD；kind错当exec；第一个master丢失；stderr造真实replay |
| T20 `TestPTYRevokeBeforeQueuedInputActivation` | P6 `internal/execsession/pty_revoke_test.go` | 冻结时queued suffix非空；撤权后原slave/外部文件计数零新增输入；先处理revoke再启泵；start/active/frozen三臂 | 恢复旧pumps-first顺序；导入漏Revoked |
| T21 `TestPTYRevokeClockFailureRetainsResponsibility` | P6 `internal/execsession/pty_revoke_test.go` | BootNow失败仍登记；无new create/KA/GC；跨代拒旧lease并完成kill，未知时刻不写0 | clock error直接return；忽略错误用now0；unknown标志不导出 |
| T22 `TestPTYKAAbsoluteDeadlinesAcrossGenerations` | P3 `internal/ptysession/control_test.go` | fake boot clock，无真实等180s；KA0、重复seq、不健康、generation变、两freshproof、authority到期、三次import；健康时有界Hangup，恢复机会只一次 | Import/reconnect restamp；携旧健康proof；每代重发5s机会；ACK续KA |
| T23 `TestPTYPartialActivationNeverDetachesInitialImage` | P7 `internal/agentboot/pty_activation_test.go` | 第二actor激活失败时第一个已应用新输入；phase active，初始Detach拒绝；当前cut A/Q/controls/output确实推进并可恢复 | error后Detach初始bundle；Active标记放全部成功后 |
| T24 `TestPTYBundleRollbackCarriesCurrentState` | P7 `internal/agentimage/pty_exec_test.go` | 真同PID两跳A→B→A；B新增输入applied、output ACK/Base、resize/KA receipt后回滚；真实identity和rc23 | 缓存原bundle重用；少generation++；丢第二代control receipt |
| T25 `TestPTYSpawnBudgetAndFailureOwnership` | P6 `internal/execsession/pty_test.go` | 容量不足Start调用0；duplicate不因预算满误拒；spawn后各setup失败同handle只一child，真error terminal、cleanup无泄漏 | 先Start后NewReplay；失败删record允许重建；释放两次budget |
| T26 `TestSessionDestroyedRevokesPTYAcrossCut` | P8 `internal/agent/session_destroyed_pty_test.go` | §6.3四窗口+register-gone两码，含sameSID前台/后代；event臂daemon不主动退，gone臂撤权后退出；tombstone导入不复活 | 漏Prepare重Export；只kill原PGID；gone默认return前漏Revoke |
| T27 `TestProcessResumePTYAuthorizationMatrix` | P5 `internal/broker/process_resume_pty_test.go` | 每新op的current member/session/node/private inbox/unknownop拒绝，当前授权撤销后无副作用；原raw overlay保未知字段 | 任一新op绕ACL；扩大attach到probe；未知op默认放行 |
| T28 `TestPTYScopeAndActorStamp` | P8 `internal/agent/run_reliable_test.go` | forged nestedRun.ActorFP被顶层覆盖、deepcopy；错Handle各字段/AttachEpoch、payload互斥全部零spawn/zeroinput/control/KA | 信任nestedActorFP；浅拷贝map；Kind从req信任不对record |
| T29 `TestPreflightDefersOnEachReasonCode`（扩展原名） | P9 `internal/agent/upgrade_image_test.go` | 十活跃码；legacy run四阶段+下载后新legacy均deferred，可靠PTY可cut；flow/ss等原拒绝保持 | 只删pty_busy不补legacy；从procs排除legacy；reliable误入gateRun |
| T30 `TestPTYCandidateDomainsAndSiblingPrev` | P9 `internal/agent/upgrade_domain_test.go` | live/terminal/tombstone/settled/clock_unknown各缺domain拒绝、磁盘不变；exec-only真实S3reader成功；ABI高于1空节点仍拒；shared dst==candidate成功，第三SHA/不能hash失败 | 只扫Tasks；只看ABI1；恢复running==disk；不验actualprevdomain |
| T31 `TestPTYPendingAdmissionCannotFallBack` | P9 `internal/agent/upgrade_domain_test.go` | 首次S3→S4 pending，真实PTYcap，reliable create busy、legacy run75、均零Start；commit落盘失败仍关门；sibling marker不误关 | capability置0；gate在订阅后关；MarkReady当commit |
| T32 `TestPTYFinalPrepareRechecksDomains` | P9 `internal/agent/upgrade_domain_test.go` | download后/Freeze后/Prepare前产生新retained扩展；最后检查拒、thaw、post-flip marker/dst符合S3回收表 | 只preflight检查；清空settled记录凑旧ABI；Prepare不复核 |
| T33 `TestPTYUpgradeExecFailureRestoresBothDirections` | P9 `internal/agent/upgrade_image_test.go` | syscall.Exec失败原master活，两向/control恢复，Queued输入恰一次，原runtime不Close，marker真实thawed | 漏abandon；打开gate先于恢复；Prepared失败关原master |
| T34 `TestPTYRuntimeFiveRoundImportFailures` | P6 `internal/execsession/pty_image_test.go` | ≥5轮各含两个真PTY、failed Take/验证/partial activation，正反方向控制都活动；五轮leak helper直接位于test body | 丢salvage master/replay、observer重复/遗留，分别红 |

Q1 的同 SID 删除重建反例另以 `TestPTYSessionRecreationBoundary`（P8，session_destroyed_pty_test.go）记录当前机制无法判别的现象；它不是接受产品缺陷的 GREEN 功能测试，不进入“C-10 已全覆盖”的正收据。lead 选择扩范围时，必须将它改成要求拒绝旧 incarnation 的回归测试再冻计划。

### 8.3 客户端、真 binary 与 gate（P0/P10–P16）

| ID / 测试名 | owner / 文件 | 断言 | 必跑变异 |
|---|---|---|---|
| T35 `TestRunReliableDuplexAcrossUpgradeAndRollback` | P13 `test/cli_e2e/run_reliable_test.go` | 两PTY fixture、本地ctl真controlling tty、远端原前台；raw bytes逐字有序对比，原ctl进程到最终rc23；B活跃推进后回滚 | 新run替代原handle；源ledger补输出；初始bundle回滚 |
| T36 `TestRunReliableLostRepliesDoNotDuplicateEffects` | P13 同文件 | create/input/resize/signal/KA/hangup/terminal-ack每个reply真正被relay阻断；首次效果有外部收据，同handle/seq重发；signal有处理handshake，resize中间被主动改变 | relay改回forward后close；重复效果去重删除；用同尺寸幂等伪oracle |
| T37 `TestRunReliableTerminalAndAbortSettlement` | P13 同文件 | merged stdout+empty stderr、最终batch、stdin EOF、A<Q abort、termios恢复；已收到terminal后丢ackreply直到lease过期仍只settle一次 | abort先return不settle；空stderr不FIN；InputError被rc0吞掉 |
| T38 `TestRunReliableNMinusOneQuadrants` | P13 同文件 | §9.5四象限和三类broker，旧agent同时取真实S3快照产物与vendor/tether-prev；capability/child/audit路径可观测；缺PTYVersion才legacy，create后任何拒绝无重建 | 跳过首probe直接向S3发pty-probe；普通probe误判全路径支持；timeout后create fallback；permission77转legacy |
| T39 `TestRunReliableLocalCancellationRestoresTerminal` | P13 同文件 | blocked stdin+blocked stdout、raw/nonTTY、外部SIGINT/SIGTERM、remote signal、local EOF各区分；Cancel后join，原FD flags/termios不变 | 永久Read goroutine；dup改O_NONBLOCK；远端退出前os.Exit；同时注册SIGINThandler |
| T40 `TestPTYClientSchedulesKAWhileOutputBlocked` | P10 `internal/execclient/pty_test.go` | source/sink可取消；输出不消费时KA仍按新seq被接纳，队列封顶；500ms attempt结束后其它op可进 | loop调用无限call；stdout在protocol loop同步阻塞；input无credit仍读 |
| T41 `TestPTYClientRecoveryDeadlineIsAbsolute` | P10 同文件 | node_offline跨60s、120s watchdog、180s固定到期，fake clock推进；恢复status必须同instance；重复错误不延期 | 每次错误重置deadline；收到旧CallID推进ACK；恢复新handle |
| T42 `TestLegacyPTYInputDoesNotBlockOtherSessions` | P8 `internal/agent/run_legacy_test.go` | 一个raw slave堵塞，另一个PTY输入/resize/KA继续；node+legacy双发布只注入一次；overflow显式失败 | callback直接master.Write；去PtyNodeHeader过滤；无界queue |
| T43 `TestTerminalIOCancellationAndDescriptorOwnership` | P11 `internal/terminalio/io_test.go` | 真FIFO/TTY独立OFD、originalflags、读写取消/join；socket/regularfile预create明确拒绝；五轮资源检查 | dup代reopen；只cancelctx不wakeFD；关闭原stdin |
| T44 `TestLegacyRunDarwinTerminalLifecycle` | P13 `test/cli_e2e/run_darwin_test.go` | Darwin实际binary run/resize/cancel+tail、本地restore，probe不支持可靠；build和实际run分别计数 | Darwin走NewReplay；Close/resize stub假成功 |
| T45 `TestPTYWireShapeAndZeroValueCompatibility` | P0 `internal/proto/process_resume_pty_test.go` | 18字段准确、原exec零值golden不变、完整capability、互斥表 | 改旧tag/type；漏omitempty；TaskKind覆盖Kind |
| T46 `TestPTYDomainVersionProbeAndPureExecReadability` | P9 `internal/agent/upgrade_domain_test.go` | 首行/最后ABI行pin、domains中间行、重复/畸形拒绝；原version probe组隔离、bounded output/timeout无业务wait干扰 | 把ABI挪到非末行；重复domain取最后掩错；未SHA绑定 |
| T47 `TestPTYGateControls` | P16 各既有gate控制测试文件内各自正负子臂，不建立跨文件假锚 | wire/image/test/matrix删行、漏race/anchor、enum裸default、Agent126、ctx-export、layering逆向各红；新增gate-control必须同文件真实存在 | 与左栏一一对应，更新正例不放宽原预算 |
| T48 `test_complete_duplex_trace_passes` 及负例组 | P14 `test/simcluster/tests/hot-update-pty-probe-test.py` | 真完整fixture通过；drop/dup/reorder、emptywindow、假PID/foreground、earlyHUP/EOF、旧A/Q、FD/zombie、不可读日志、缺waitpid各失败 | 删对应judge分支，反例必须反而变绿以证明控制非死代码 |
| T49 `TestRunReliableErrorExitClasses` | P12 `cmd/tether/run_reliable_test.go` | 64/74/75/77、remote23、InputAbort、CreateSent状态及恢复terminal顺序 | unknown→70；permission→fallback；恢复前os.Exit |
| T50 `TestPTYRuntimeAdmissionLockDeadline` | P6 `internal/execsession/activation_test.go` | Runtime入口锁/control活动消耗预算也有界；revocation可在freeze期间登记；不持r.mu join | 入口裸mu.Lock；Runtime锁中等待actor/control |

T47 是控制责任集合的索引，实际 Go 测试必须用各 gate 文件已存在/新增的精确名字入 inventory；不生成一个与实现无关的空测试。P16 在交付收据中列清精确函数名。其它 T01–T46/T49/T50 均是固定新增或扩展的实际名称。

### 8.4 leak、race、测量纪律

`ptysession` 整包 `-race -count=3`；pty/workload/procrequest/execsession/execclient/agentimage/agentboot/agent 的改动按完整矩阵覆盖，不能只跑 `-run Pty` 宣称整包 race。ptysession、terminalio、直接 import pty 的新测试宿主有真实 ≥5轮生命周期并在 Test body 直接调用 `testharness.AssertNoGoroutineLeak/AssertNoFDLeak`；在 `leakExerciseAnchors` 钉 `exercisePtyLifecycle/exercisePTYImageFailure/exerciseTerminalIO` 等实际调用。锚点是每个 assertion site，不能用空五轮+循环外spawn。

稳定FD基线只作辅助；在 owner 仍活且 cleanup 之前按类别/identity查master、replay、handoff、pidfd、child/zombie；预期在保留阶段非零、settle后本task归零。失败import后原master依然可读写，再清理；否则“最终fd不涨”可能只是提前关业务。

无 fake ready sleep；用对端receipt/pipe handshake/原identity作为屏障。180s/KA 用注入 boot clock做单元测试，不把产品计时缩小偷换生产预算；真实W使用120s。资源/freeze测量保存原始样本，记录机器负载、kernel、Go版本、source SHA、build race状态，不能删最大值。

## §9 drill 36、39.P、N-1 与成本

### 9.1 fixture、证据与公共探针

新增 `hot_update_pty_probe.py`：local master驱动原 ctl，ctl在自己的session拿 **真实 controlling tty**；TIOCSCTTY失败即SETUP-RED，不忽略。远端第二个独立PTY才是被测业务终端。local可以保留测试用slave，不能等local master EOF判断ctl结束，必须原ctl waitpid。远端不得多开slave keeper。

远端fixture是带nonce的raw字节协议：关闭ECHO/OPOST，输入接收与输出发送各自递增id、长度、hash、原始byte文件；数据中含分段escape/UTF-8/0x04/0x03的专门子臂，普通raw主流不让这些被termios解释。逐字比 `ctl.sent`→`remote.received` 与 `remote.sent`→`ctl.received`，同时验顺序/次数/首尾；不只用集合/hash总值，不删CR/LF、不把producer ledger拼回ctl输出。

每次故障前两侧各有≥20条真实进展；完整观察窗≥20个采样点，cut短窗只要求真实非空且写操作与cut有交集，不强求500ms里20条。长前台job不同于shell PID，记录leader、foreground、后代boot/PID/ticks/nonce，明确预期SIGINT只杀短子作业；主job在升级/回滚窗HUP=0、无早EOF，正常终点原ctl自主rc23。agent同PID、running SHA真正变化、marker generation/phase、一次真实spawn和courier终态是独立oracle。

公共 strict mixed probe 在已有 `hot_update_mixed_probe.py` 加显式strict选项；36.A使用，34保原校准判据。strict要求connect_failures=request_failures=refusals=over_5s=0、read/write/short三类非空、10/s调度达到已声明下限9/s、至少一次有副作用write跨cut，started响应≤5s；任何agent_upgrading75也不能从strict失败计数偷删。统计p50/p95/p99/max及每类路径；500ms全窗另表，不能用局部shim时长替代。

四日志流只经 `drills/lib/logs.sh` 读取；新鲜完整cursor、inode/长度单调、读失败/旋转需显式错误。正向审计先证明本nonce的started/activation/exit等必要行非空，才判HUP/错误为0；不能`|| echo 0`，不能固定tail N截掉反例。PTY census新增独立分类/子命令，不改变34原先把ptmx视为unsafe的语义；比原master index/mount/dev身份、每代handoff=0、最终master/replay/pidfd=0和无Z/原fg消失。清理只在结论及证据捕获后执行，不能将trap kill计为产品清场。

### 9.2 固定 arms

| arm / 目标判词 | 故障与非空前提 | 核心oracle | estimate / declared worst（秒） |
|---|---|---|---|
| `36.A` GREEN | fresh N1；连续raw双工+长foreground；稳定负载校准后真实node upgrade到不同SHA；resize在cut两侧；strict mixed新run/读/短命令 | 原PTY/fg/ctl不换；exact bytes/0HUP/0earlyEOF/rc23；新run≤5s；正常业务maxstall≤5s；freeze全窗样本；资源退出 | 120 / 300 |
| `36.W` GREEN | 新KA已提交；预先留真实accepted pending input及未ACK output；SYN黑洞使新代已激活但不能register，真实120s watchdog回prev后heal | 原ctl保持原handle跨node_offline；第二代通过kernel排队输入实际推进A，output End推进；回迁用当前A/Q/ACK/Base/receipt，非原bundle；同PID原master、gen+1、真实rolled_back；KA绝对值没重授 | 240 / 420 |
| `36.C` GREEN | C1 nats-server restart+25s outage（>redialAfter）并确认session rebuild；C2 leader先exit后>200ms尾流与FIN/canonical/raw专门fixture，各有非空输入输出 | carrier替换无新PTY；KA未restamp；C2真tail/FIN/rc；同一父C只有全部子场景成功才GREEN，不用一场景掩盖另一场景 | 150 / 360 |
| `36.L` GREEN | 原old ctl→new agent legacy run；attach前/运行中upgrade拒绝；原child继续后结束；同时普通legacy resize与双发布 | legacy_session_busy、磁盘/marker不变、原terminal可用；gate closed时新legacy75且零spawn；新ctl对old agent/old broker precreate fallback可用，不宣称它们可热更新 | 120 / 300 |
| `39.P` PRODUCT-RED | **真正old exporter + old ctl**，vendor/tether-prev来源/版本/SHA die-gate；与36.A相同HUP敏感原PTYfixture、相同upgrade故障 | 运行同一continuity judge；旧bare exec丢原master，旧路径HUP/EOF/管理丢失有精确产品签名；child故障后允许已死，采集不是存活setup门 | 150 / 300 |

36.W 的黑洞中不能虚构网络已送入新控制。需要同时证明 output ACK/Base/control receipt 在首次激活后更新的部分，由 T24/T35 的可通信 A→B→A 回滚臂承担；W 独立证明黑洞+120s真实watchdog以及 B 能通过**已接纳**输入/真实输出推进。若W采集不到相应推进事实就失败，不能写“预计推进”。两份收据共同构成“当前态回滚”，一份不能替代另一份。

36每臂为独立新供给、可由runner并行；不在A运行失败后改成skip。35/34的业务语义不改，36L与39P分别负责legacy安全拒绝和真实旧路径负控制。

### 9.3 verdict 的精确形状

`expected-verdicts.tsv` 追加36父及A/W/C/L五行：`GREEN, nc_gap=0, bands=-, owner=-`，note-ref每臂真实存在；父按join推导。39保留X GREEN、Y PRODUCT-RED，追加P PRODUCT-RED、nc_gap0，父仍PRODUCT-RED。P owner固定 `hot-update-plan H1 legacy PTY exporter; S8 legacy-to-capable first migration`；父owner保留完整现有子串 `hot-update-plan H0 legacy exporter; S8 legacy-to-capable first migration`，再用 `; ` 拼完整P owner。不能改写成大意相近文本，因为 validator 的无#id路径要求 literal substring（`:285–291`）。

39.P predicate先验证有效供给/故障确实发生/日志可读，返回真实continuity failure；`assert_bug`要求匹配首次实跑重钉的 **单一精确签名**，建议结构 `HOT_UPDATE_LEGACY_PTY_EXPORTER output=... hup=... management=... ctl=... pid=... nonce=...`，值以旧binary实际收据固定，禁止先猜rc70作为事实。unexpected fixed→ASSERT-FAIL要求更新验收；setup/观测坏→SETUP-RED或ASSERT-FAIL，绝不PRODUCT-RED。

三层rc分开存：`continuity_oracle_rc=1` 是同正oracle失败；39.P native `DRILL-VERDICT verdict=PRODUCT-RED rc=3 assert_fail=0 setup_red=0 product_red>0 not_covered=0 nc_gap=0`；sweep没有waiver时按阻塞drill计数（39整体一次，不按红臂数）；`--allow-product-red`只改变sweep放行，不把native改GREEN。35.C（`:449`）用的是legacy-outcome窄判据，不能作raw FAIL包装先例；39.Y（`:324`）的assert_bug和精确签名才是结构先例，但其fault后要求producer活着的采集不得照搬给HUP敏感PTY。

负控制必须有“36同oracle正输入通过／39P旧输入失败／删除关键检查后旧输入错误通过”的三向selftest。脚本缺失字段、空窗口、控制TTY建立失败、wrong SHA、没有原ctl rc均不能恰好匹配产品缺陷签名。原34/35/39Y历史receipt保留原文，追加S4结果不覆盖。

### 9.4 登记、运行次序与成本

五份账本共同更新：assert-identity、kept-sites、drill-costs、expected-verdicts+log、coverage inventory；新probe selftest sh进run-all，新增shell谓词进r9d正负非空性；所有log helper使用受既有gate约束。默认成本表先写estimate，首跑后写actual source SHA/日期/并发度，不能把estimate当测量。

`34` 当前七臂 declared worst=2940s（49min），不是“回归2min”；runner `:528–535` 硬超时为 `min(2×worst, global timeout)`。36本计划Σworst=1380s（23min），串行ceiling46min；39X/Y/PΣworst=1020s（17min），ceiling34min；34串行ceiling98min。仅三组无并发理论ceiling178min，再加restage/33/35/收据分析；不能对lead报“一轮4min”。这些是上界算术，不是预计墙钟。

未来实施预告：hermetic run-all边际+5–10s；restage一次预计3–5min；36 `-j4`估计4–7min；34 `-j4`约7–12min（未校准）；39三臂约5–8min；33约5–9min；35 `-j3`约2–3min；初次判词重钉/失败重跑另计。完整deploy批预计30–50min，机器负载和实际costs修正后再定，不作为保证。drill与go test/lint/gates不并发；runner内部允许其既有并行。

建议授权实施后顺序：静态/定向测试及mutation→hermetic gates→单次最终三硬闸→restage并记artifact→36全臂→34七臂→39含P（显式允许预期PRODUCT-RED但保存native）→33/35→账本actual更新→受影响hermetic gate重跑。任何生产修订使前述最终SHA变动，按影响重新跑收据，不拿旧SHA签新版本。命令rc直接保存，不用tee管道吞退出码；本轮不执行这些命令。

### 9.5 N-1 四象限及 image 窗口

| ctl / agent | current broker | old S1/S3 process-resume broker | old no-verb broker |
|---|---|---|---|
| new / new | probe Kind=pty→pty-probe→可靠；carrier/upgrade/rollback连续性收据 | 第二步新op被拒→legacy；不承诺热更新连续 | precreate无verb兼容探测→legacy |
| old / new | 原legacy wire，双发布去重；活会话使upgrade deferred | 同左 | 同左（可用性以实际旧供给确认） |
| new / old | 第一步合法cap缺PTYVersion→legacy，不发送pty-probe，零可靠create；无runtime者按unsupported | 同左 | 同左 |
| old / old | 原功能基线；39P升级反例 | 原功能基线 | 原功能基线 |

任一象限都以实际actor/broker版本、running SHA和child/audit路径确认，不能仅看client“fallback”日志。new/new一旦create后broker回到不认识新op，必须原handle恢复或明确74，绝不legacy再执行；authorization77直接终止。image另测 S4→S3纯exec成功、有PTY retained拒绝、S3→S4 pending新建busy、S4→S4两代+回滚、共享binary sibling prev==candidate；legacy exporter首次迁入只给39.P反例，不冒充本阶段修复。

## §10 全门、账本与边际秒数

耗时是新增工作的估计，最终收据填写实际边际秒；不为快而删除原矩阵/控制。P16拥有机械gate/ledger文件，产品包提交给P16新增字段/测试/enum/ctx站点清单，不自行写同一golden。

| CLAUDE 门 | S4动作/owner | 新增边际预算 |
|---|---|---|
| lint三层、nolint有效性 | 保持 `.golangci.yml` 不变、无新豁免；触及注释整段保留；P16核验，产品owner修lint | 全lint同候选一次；边际5–15s估计 |
| 结构四维 | Agent恰125、文件≤19、剥注释代码<8000；main-noncli不跨1300（当前golden1200量子），新增逻辑留子包；自动updater只收紧，禁止批量放宽 | <1s |
| layering | 新ptysession→pty/workload、execsession→ptysession单向；禁ptysession→agent/broker/NATS/execsession，terminalio禁网络/agent；保originalUnion；P16 | <1s |
| TLS配对、inbox配对、ACL↔订阅 | 无新TLS/inbox命名路径/subject；仍natsinbox；P5白名单只扩新op；P16重跑正负控制，不增例外 | <1s；真实ACL测试另计 |
| build tags/vet-tags | linux与unsupported成对；无新integration tag、无借tag逃漏；Darwin构建+实际run分别收据 | 5–15s编译估计；Darwin实跑另30–60s |
| T3 leader premise、sleep冻结 | 使用WaitNodeOnline/对端业务receipt；新drill故障前证明identity；不增legacy premise/sleep账本 | <1s |
| NATS ping一致、server pin、安全floor | 不改任一ping/OfflineAfter/nats版本/install/stage；所有原门继续跑 | 0新增产品时间 |
| naming freeze、test tree地图、docs布局 | 新文件稳定语义名，头注释等basename；不新建p*/d*目录；run_reliable_test.go固定；test/README登记新matrix；计划文档入reviews | <1s |
| ctx站点 | 新Background仅真实ctx-root/ctx-none就地注释；P9重构smokeVersion时给根probe计时ctx标ctx-root，P16删除其旧ledger站点；P8未触及waitForAttachOnSub那一行则保留其旧条目，不虚报消账 | <1s |
| gate标准/registry自对账 | 修改的gate同文件gate-control正负控制；继续由现有make gates包扫描接线，不另造“文档门” | <1s |
| test inventory | 只追加新Test/Fuzz/Benchmark；原四固定名不可改；新类型测试全部登记；P16逐行查删改 | <1s |
| 确定性lint/origin/promised-guard | origin指向最终冻结plan；本文T47索引不生成死promise；无产品计时裸sleep、无假测试名 | <1s |
| enum families/default | P0新 `ptysession.actorPhase`（phaseActive/phaseFreezing/phaseFrozen/phaseClosed）、`ptysession.sourcePhase`（sourceReading/sourceDiscarding/sourceEnded）和 `execclient.ptyPhase`（ptyProbing/ptyCreating/ptyAttached/ptySettling/ptyDone）列入家族表；其它新增iota同样登记；default不代替穷举 | <1s |
| wire error-code↔exit class | 18新字段、1新legacy Reason `pty_input_overflow`→74；复用码铸造点的static resolver覆盖新分支；P12更新四处表/测试，未知不落70；Input/ControlError不伪造Code | 1–2s |
| wire inventory | `ProcessResumeReq`8、reply10精确追加；旧字段零删除、零tag/type改动；P16唯一写者 | <1s |
| CLI tree golden | 没有新命令/flag/Hidden位，预期零diff；帮助文本可改；P12核验 | <1s |
| leak shape/risky packages | ptysession/terminalio及新增import pty宿主直接共享helper；真实5轮、每站anchor，例外表不增 | 10–25s生命周期；扫描<1s |
| seedSession收据 | 新cli测试复用realstack，8个既有转发器不改；不复制另一个seed | 0新增 |
| dataplane lifetime | 把ptysession、pty owner导出freeze/activate/API加入ctx-free守卫；原Runtime.Close(ctx)仅既有白名单；不为新actor增ctx豁免 | <1s |
| upgrade budget pin | Go新增PTY常量；shell追加PTY_RECOVERY/PTY_RECOVERY_OPPORTUNITY/PTY_INPUT_BYTES/PTY_BATCH_BYTES/LIVE_FLOW_STALL等用到的精确项，新增未对账变量仍红 | <1s |
| image inventory | 从Bundle可达的Kind/PTY/ClockUnknown/SessionMember等逐项追加；不同OS叶子类型规则原样；JSON无函数/FD号 | <1s |
| simcluster log oracle | 全部四流用logs.sh；P14自检cursor失败不变0，P15脚本source完整；原例外精确计数不放宽 | 1–2s |
| spawn stall evidence/runtime isolation | StartPTY复用现spawnsafe/spawnexec，不增加“裸cmd.Start+goroutine timeout”；原四错误铸造点、helper单fork/64租约不变 | 原门回归；边际0–2s |
| hermetic闸集 | 新pty-probe-test.sh进run-all；tests/*.sh双向对账；两个新增shell gate/变化谓词有非空正负控制 | +5–10s（原整集约2min） |
| CI发布链 | 不改workflow/release依赖、fuzz触发规则 | 0新增 |
| fuzz语料预算 | 本阶段无需新fuzz目标；若测试最小反例入corpus仍≤64/目录、≤256KiB/仓库，只收crasher | 0新增 |
| matrix inventory/-race归属 | `test/e2e/all_phases_test.go` TestLeakRiskMatrix追加ptysession和terminalio整包；pty确保整包race；新TestReliableRunMatrix单元 `./test/cli_e2e/...,-race,-run ^TestRunReliable`，保留旧ReliableExec tuple，inventory append-only | 新run矩阵60–150s；新增内层binary构建20–45s，不能假称跨go-test共享sync.Once |

P16记录全表每行 PASS/FAIL/NOT_RUN；不改的门也在最终make gates收据里，不把“本阶段没动”当豁免。结构门不因新增包未达扫描阈值而漏评：P0≤300行契约，ptysession production目标≤1800行、≤10文件；超量需拆职责并更新owner表，经lead审查，不靠删除注释。

最终正式命令集合保持 `make test`、`make e2e-parallel`、`make lint`、`make gates`；需要的定向race×3和mutation先于最终批。全量串行e2e target不复活；不在并行矩阵过后再手工串一遍“确认”。实际 `go test/build/vet/make/drill` 均为未来实施动作，本轮未运行。

## §11 文档与 tasklist 只凭收据勾选

| 文档 / 条目 | 必须更新的内容 | 放行收据 |
|---|---|---|
| architecture §21、§22.4–22.6 | capability/downgrade、18字段、PTY domain/image文件表、EIO/FIN、controls、KA、revocation-first active失败表、domain sibling规则、10个deferred、已知删除/逃逸边界 | 实现与wire/image inventory一致；T01–T34 |
| usage.md | reliable run、逻辑EOF/Ctrl-D、stdout合并、stdin/stdout类型限制、外部signal、错误exit、旧版本fallback与pending忙、恢复窗 | T35–T44/T49 |
| testing-standards/test README | 双PTY真实ctty、短syscall非空、lostreply因果、inner overlay/race、矩阵新行、五轮锚点 | T04/T18/T34/T36/T43，P16门 |
| simcluster README/expected-verdicts/log/coverage inventory | 36四臂GREEN、39P原生红及parentjoin、34完整回归、成本actual、四日志流与negative签名 | 同SHA完整rollup+native rc+raw evidence |
| deploy-tier-gotchas、handoff-failures §4、handoff §8.5、主handoff | 更新H1六个接线缺口各有证据，不把设计候选称完成；保留历史负控与未覆盖边界 | §0全出口及Q1/Q2裁决 |
| C-4（可靠ctl/重连语义） | exec已实现部分保留；补PTY capability、输入实际ACK、合并output、terminal-ack与四象限 | T11/T12/T35–T41，36A/C/L，39P |
| C-8（完整接线） | pipe/PTY、stdin/resize/KA、ps/kill/courier、管理清场/真exit | T08/T15/T16/T26/T28/T42，36A/W/L |
| C-10 PTY变体 | 事件/冻结/导入/注册gone/tombstone/钟故障/同SID清场；Q1无法观测窗口单列 | T20/T21/T26、真实CLI session-rm子臂与管理清场收据；未接受范围前整体不勾 |
| C-11（全部test/race/leak/drill/docs） | 同一候选完整收据；不拿S3 exec收据替代PTY，不用cleanup修补 | §0所有出口、P16门、实际Darwin、§13裁决；任一NOT_RUN不勾 |
| B-2 PTY子项 | 原terminal双向字节与无重建oracle完成；SSH/TCP/SS子项保留未勾 | T35 + 36A/W/C + 39P；B-2总owner仍S5 |
| handoff-failures §4.1–6 | 1 EIO→T01；2 FIN→T05；3控制fence→T08/T14；4尾流/队列→T02/T11/T42；5内核/期限→T10/T22；6真实出口→T04/T35/36 | 每条一绿一实际mutation红；不以一句“H1 done”替代 |

新证据追加 `hot-update-evidence.md` 的 S4 节：环境/候选SHA、18字段diff、image字段diff、每测试/mutation命令rc、每drillartifact/identity/日志cursor/native与sweep判词、cost、freeze分布、N-1二进制来源、Darwin，以及所有NOT_RUN。`hot-update-tasklist.md` 历史已勾项不取消，陈旧“当前停止点”改成实际S4状态；只在相应收据闭合后勾具体项。当前候选计划阶段一项也不新增勾选。

## §12 并行实施工作包（严格独占文件）

### 12.1 落地顺序与协作规则

lead 先冻结本文和 Q1–Q5，再落 **P0**。P0 新文件只放 §2/§5 的完整类型、常量、无行为兼容包装和测试，不放占位“成功”实现；下游函数签名已由本文精确冻结。后续18个包中的17个可同时开始写各自文件；编译/集成按下列 DAG 落地。这里的并行指实施者可同时工作，不允许共享文件“各改不同区段”。

第一波可独立落地：P1/P4/P5/P10/P11/P14；随后 P1→P2→P3→P6→P7→P9→P8；P10+P11→P12；P5+P8+P9+P12→P13；P14→P15；P16在字段/测试/脚本齐后生成账本并落地；P17在实现稳定后写最终文档/receipt。P9可先落兼容门，其涉及真实PTY的测试等待P8，不是P8/P9互相等待编译。测试依赖与落地依赖分开列，尚缺依赖时允许 NOT_RUN，不准stub成PASS。

共享文件只有本节列出的owner可写。其他包提交接口请求/补丁建议给owner，不自行修改它；跨包接口变化必须lead更新P0合同后整体协调，禁止临时第二套type。测试helpers也按文件独占；新的共享test API由P13固定，不让每包复制真实binary构建。每包交付一个文件清单及diff receipt，意外修改清单外文件不接受。

P14/P15的跨语言合同也先冻结：`hot_update_pty_probe.py` 子命令 `identity/trace/census/continuity/legacy-exporter/selftest`，共同参数 `--evidence DIR --nonce HEX`；`continuity` 正常0、业务不连续1、证据无效2，`legacy-exporter`不能自行把1改0。JSON顶层固定 `version:1, nonce, images[], processes[], windows[], input:{sent,received,applied,accepted}, output:{sent,received,base,end}, controls[], ctl:{pid,start_ticks,wait_status}, resources:{before,after,settled}, logs:{cursors,read_ok}, oracle:{valid,continuous,reason}`，计数/offset为非负整数、缺字段拒绝；raw bytes在旁文件有length+sha且judge实际读取。`hot_update_pty.sh` 提供 `pty_fixture_start <dir> <nonce> <ctl_binary>`、`pty_checkpoint <name>`、`pty_fixture_finish`、`pty_collect_logs`，复用现有SIM/SID/NID/host变量，所有输出写evidence目录，不回填业务输出。P14先交selftest与接口后，P15可独立接四臂。

| 包 / 名称 | 难度 | 文件数 | 落地依赖 | 测试通过前还须具备 |
|---|---|---:|---|---|
| P0 公共契约与预算 | HARD | 10 | lead裁决 | 无 |
| P1 Manager事务与会话清场 | HARD | 6 | P0 | P2/P3供同PID联合臂；本包guard测试可先跑 |
| P2 master与portable legacy owner | HARD | 10 | P0、P1 | 无额外 |
| P3 PTY双工actor | HARD | 13 | P0、P1、P2 | 无额外 |
| P4 registry PTY intent | HARD | 4 | P0 | 无额外 |
| P5 broker白名单与ACL | NORMAL | 2 | P0 | P8供端到端零副作用联测 |
| P6 runtime多kind与撤权 | HARD | 9 | P0、P3、P4 | P7供整体activation/import联测 |
| P7 image与部分激活恢复 | HARD | 9 | P0、P6 | 无额外 |
| P8 agent可靠/legacy接线 | HARD | 9 | P0、P2、P6、P7、P9 | P5供授权全链路 |
| P9 ABI/domain与安装回滚门 | HARD | 12 | P0、P6、P7 | P8供真实PTY/legacy；P13供真binary升级 |
| P10 reliable PTY client | HARD | 6 | P0 | P5/P8供真实wire联测；fake transport可先跑 |
| P11 本地可取消终端IO | HARD | 5 | P0 | 无额外 |
| P12 CLI薄分支与错误表 | NORMAL | 8 | P0、P10、P11 | P5/P8供真binaryrun |
| P13 真binary/PTY/丢回复测试 | HARD | 8 | P5、P8、P9、P12 | P16矩阵登记后完整gate；Darwin实际runner |
| P14 drill探针与strict传感器 | NORMAL | 7 | P0 | P15供脚本集成；selftest可先跑 |
| P15 36/39P脚本 | NORMAL | 3 | P14 | P5/P8/P9/P12/P16及restage后实跑 |
| P16 门与机械账本 | NORMAL | 18 | P0–P15接口/文件齐 | P17文档声明齐；最终三硬闸/全部mutation |
| P17 文档与证据封口 | NORMAL | 15 | 已冻结plan | P0–P16真实收据；无receipt不勾 |

**P0 — 公共契约与预算。** 目标：先给所有实施者相同的wire/type/API，保障新字段默认不污染旧exec。拥有T45、预算关系/版本行测试；变异：旧tag/type改变、漏omitempty、domain/ABI行错序、预算shell/Go漂移（后者与P16联测）。只读其他包，不修改实现骨架来假编译。

- `internal/proto/process_resume.go`
- `internal/proto/version.go`
- `internal/proto/process_resume_pty_test.go`
- `internal/pty/contracts.go`
- `internal/workload/control_contract.go`
- `internal/ptysession/contracts.go`
- `internal/ptysession/image_types.go`
- `internal/execclient/pty_contract.go`
- `internal/upgradebudget/budget.go`
- `internal/upgradebudget/budget_test.go`

**P1 — Manager事务与会话清场。** 目标：WithControl/FreezeUntil guard、pidfd身份验证、同SID跨PGID可续做责任。拥有T15/T16及原ProcessState回归；变异：guard拆开、只kill一个PGID、pid复用误杀、漏cleanup责任。不能在Manager锁内scan/join，不能扩大到任意UID进程。

- `internal/workload/process.go`
- `internal/workload/process_state_linux.go`
- `internal/workload/control_linux.go`
- `internal/workload/control_other.go`
- `internal/workload/control_test.go`
- `internal/workload/control_exec_test.go`

**P2 — master与portable legacy owner。** 目标：成功Start/AdoptDraining→唯一Take，frozen FD只读检查/失败归属，portable有界legacy队列。拥有T01/T03/T10、Session现有owner/abandonedStart回归、legacy owner五轮；变异：File.Fd、双wrapper、旧Session.Close、修flags、错PTMX匹配、早EIO FIN。

- `internal/pty/pty.go`
- `internal/pty/master_linux.go`
- `internal/pty/master_other.go`
- `internal/pty/terminal_linux.go`
- `internal/pty/terminal_other.go`
- `internal/pty/legacy_io.go`
- `internal/pty/legacy_io_test.go`
- `internal/pty/terminal_test.go`
- `internal/pty/master_poll_test.go`
- `internal/pty/process_owner_test.go`

**P3 — PTY双工actor。** 目标：A/Q有界输入、合并replay、source/abort分离、control receipt、KA、freeze/recover唯一泵。拥有T02/T04–T09/T11/T13/T14/T18/T22；必须跑各自全部变异，尤其真实短Write、尾流Z leader和单向deadline臂，不能靠fake IO代替。

- `internal/ptysession/actor.go`
- `internal/ptysession/input.go`
- `internal/ptysession/output.go`
- `internal/ptysession/control.go`
- `internal/ptysession/fence.go`
- `internal/ptysession/image.go`
- `internal/ptysession/actor_test.go`
- `internal/ptysession/input_test.go`
- `internal/ptysession/output_test.go`
- `internal/ptysession/control_test.go`
- `internal/ptysession/fence_test.go`
- `internal/ptysession/image_exec_test.go`
- `internal/ptysession/lifecycle_test.go`

**P4 — registry PTY intent。** 目标：新增ReservePTY、canonical Run hash、Kind贯穿settled/restore；不改变exec digest。拥有T17与 `pty_test.go` 的normalization表；变异：省Kind、任一intent字段不hash、ActorFP信任、未到期Collect、错epoch接纳。

- `internal/procrequest/registry.go`
- `internal/procrequest/state.go`
- `internal/procrequest/pty_test.go`
- `internal/procrequest/pty_exec_test.go`

**P5 — broker白名单与ACL。** 目标：只改两个switch及对应测试，保raw overlay/reply guard/current ACL。拥有T27；变异每新op绕ACL、probe放到STALE、未知op默认允许。`internal/procforward` 明确不在任何包写集合，发现需要改变就退回lead审范围。

- `internal/broker/exec.go`
- `internal/broker/process_resume_pty_test.go`

**P6 — runtime多kind与撤权。** 目标：task/image/tombstone分发、pre-spawn预算、输入错误结算、revocation-first、absolute clock、可截止入口。拥有T12/T19–T21/T25/T34/T50和原execsession全部回归；变异：先泵后kill、BootNow丢标志、kind资源错派、budget晚预留、finalEnd后追加、丢salvage。

- `internal/execsession/runtime.go`
- `internal/execsession/fence.go`
- `internal/execsession/image.go`
- `internal/execsession/revoke.go`
- `internal/execsession/pty.go`
- `internal/execsession/pty_test.go`
- `internal/execsession/pty_image_test.go`
- `internal/execsession/pty_revoke_test.go`
- `internal/execsession/activation_test.go`

**P7 — image与部分激活恢复。** 目标：kind-aware验证/salvage、10个deferred集合、PreActivate/active边界、当前态再派发。拥有T23/T24、原Freeze/Restore/boot phase回归；变异：Manager已thaw仍Detach、初始bundle回滚、第二资源失败关原master、只算6FD。

- `internal/agentimage/bundle.go`
- `internal/agentimage/freeze.go`
- `internal/agentimage/restore.go`
- `internal/agentimage/freeze_test.go`
- `internal/agentimage/pty_test.go`
- `internal/agentimage/pty_exec_test.go`
- `internal/agentboot/image.go`
- `internal/agentboot/image_test.go`
- `internal/agentboot/pty_activation_test.go`

**P8 — agent可靠/legacy接线。** 目标：共用StartPTY、125 methods不变、courier/ps/kill、wildcard队列、不可靠路径拒绝cut、reliable health proof。拥有T26/T28/T42、Q1边界反例、原ctl_liveness/SessionDestroyed/Bootstrap回归；变异：trusted nestedActor、callback阻塞、双发布重复、先订阅后pending门、event/gone退出混淆。通过包级函数/字段接线，不新增Agent方法。

- `internal/agent/agent.go`
- `internal/agent/exec.go`
- `internal/agent/run.go`
- `internal/agent/bootstrap.go`
- `internal/agent/bootstrap_test.go`
- `internal/agent/ctl_liveness_test.go`
- `internal/agent/run_reliable_test.go`
- `internal/agent/run_legacy_test.go`
- `internal/agent/session_destroyed_pty_test.go`

**P9 — ABI/domain与安装回滚门。** 目标：SHA绑定域探测、三次能力复核、pending创建门、shared sibling、`pty_busy`移除后完整legacy guard、watchdog当前态。拥有T29–T33/T46及所有既有安装/prev/probe隔离回归；变异：漏settled、只查ABI、强制running==disk、晚准备不查domain、commit未落盘就放行、exec失败关原FD。只有P9改main.go的version行，P12不得碰该文件。

- `internal/agent/upgrade.go`
- `internal/agent/upgrade_state.go`
- `internal/agent/upgrade_image.go`
- `internal/agent/upgrade_domain_test.go`
- `internal/agent/upgrade_smoke_test.go`
- `internal/agent/upgrade_image_test.go`
- `internal/agent/upgrade_state_test.go`
- `internal/agent/upgrade_admission_test.go`
- `internal/agent/upgrade_probe_lifetime_test.go`
- `internal/agent/upgrade_marker_target_test.go`
- `internal/agent/upgrade_descriptor_owners_test.go`
- `cmd/tether/main.go`

**P10 — reliable PTY client。** 目标：单attempt可调度、Probe/Run两段、bounded input/output、CallID、control重试、180s绝对恢复、输入abort结算。拥有T40/T41、client五轮cancel/retry、原exec可靠与CallID回归；变异：无限call饿死KA、Q释放buffer、旧CallID推进、create后fallback、每错误续恢复窗。

- `internal/execclient/client.go`
- `internal/execclient/pty.go`
- `internal/execclient/pty_test.go`
- `internal/execclient/pty_lifecycle_test.go`
- `internal/execclient/client_reject_callid_test.go`
- `internal/execclient/client_test.go`

**P11 — 本地可取消终端IO。** 目标：独立OFD、wake、bounded worker、join/restore、unsupported类型。拥有T43与五轮真实TTY/FIFO读写取消；变异：dup改原flags、关原stdin、Read goroutine永久留存、只ctxcancel不唤醒。Darwin不声称支持可靠adapter。

- `internal/terminalio/io.go`
- `internal/terminalio/io_linux.go`
- `internal/terminalio/io_other.go`
- `internal/terminalio/io_test.go`
- `internal/terminalio/io_linux_test.go`

**P12 — CLI薄分支与错误表。** 目标：现有run文件接Probe/raw/create、legacy fallback、SignalContext单一取消语义、restore先于退出、Reason覆盖。拥有T49、error class/hints/emitter四处双向门与原exec回归；变异：原始terminal未恢复就exit、77fallback、inputabort落rc0、pty_input_overflow无74映射。业务循环不进package main。

- `cmd/tether/run.go`
- `cmd/tether/ctl_connect.go`
- `cmd/tether/exec.go`
- `cmd/tether/error_hints.go`
- `cmd/tether/error_class_test.go`
- `cmd/tether/error_hints_test.go`
- `cmd/tether/error_code_coverage_test.go`
- `cmd/tether/run_reliable_test.go`

**P13 — 真binary/PTY/丢回复测试。** 目标：真实进程链、双PTY、oracle、relay因果与N-1版本供给。拥有T35–T39/T44、既有exec lostreply回归和node升级联测；变异：reply漏过、信号合并假绿、幂等resize假绿、sourceledger回填、重建ctl/task、非TTY根signal竞争。使用现realstack inner race/overlay，不fork go test；`sync.Once`仅当前测试进程共享，不能压低矩阵成本估计。

- `test/cli_e2e/run_reliable_test.go`
- `test/cli_e2e/run_darwin_test.go`
- `test/cli_e2e/pty_harness_test.go`
- `test/cli_e2e/natsrelay_test.go`
- `test/cli_e2e/realstack_harness_test.go`
- `test/cli_e2e/exec_reliable_test.go`
- `test/cli_e2e/run_modes_test.go`
- `test/cli_e2e/node_upgrade_image_test.go`

**P14 — drill探针与strict传感器。** 目标：可信有序字节/FD/原ctl/日志判据及36严格混合请求模式。拥有T48及mixed selftests；变异：去任一negative分支、空log当0、丢wrongfd、over_5s仅记录、空fault window通过。34原judge不降级，不改logs映射源。

- `test/simcluster/drills/lib/hot_update_pty_probe.py`
- `test/simcluster/drills/lib/hot_update_pty.sh`
- `test/simcluster/drills/lib/hot_update_mixed_probe.py`
- `test/simcluster/tests/hot-update-pty-probe-test.py`
- `test/simcluster/tests/hot-update-pty-probe-test.sh`
- `test/simcluster/tests/hot-update-mixed-probe-test.py`
- `test/simcluster/tests/hot-update-mixed-probe-test.sh`

**P15 — 36/39P脚本。** 目标：四正臂与旧exporter负臂、真实artifact、故障非空与原始rc。拥有36全臂、39P、r9d新增谓词双侧；变异：旧SHA/假ctty/故障未发生、expectalive遮HUP、直接包装rawfail绿、漏当前态证据。34/35/33不改脚本，按回归执行。

- `test/simcluster/drills/36-hot-update-pty.sh`
- `test/simcluster/drills/39-hot-update-legacy-n1.sh`
- `test/simcluster/tests/r9d-nonvacuity.sh`

**P16 — 门与机械账本。** 目标：§10全部门逐条收据、只增inventory、whole-package race/inner run matrix、五份sim账本。拥有T47各门既有控制及新增子臂；变异按§10逐条，尤其遗漏PTy包race、死leak锚、Agent126、未知enum默认、image删除、parentowner substring。它是唯一ledger写者；不得改validator/runner或结构golden去顺应错误实现。以下列表不含结构golden，因为本阶段预期零放宽、无需修改。

- `internal/proto/testdata/wire_field_inventory.txt`
- `test/architecture/testdata/image_field_inventory.txt`
- `test/determinism/testdata/test_function_inventory.txt`
- `test/e2e/parallel/testdata/matrix_units_golden.txt`
- `test/e2e/all_phases_test.go`
- `test/determinism/enum_switch_default_test.go`
- `test/determinism/leak_assert_shape_test.go`
- `test/architecture/layering_test.go`
- `test/architecture/dataplane_lifetime_test.go`
- `test/architecture/legacy_ctx_background_sites.go`
- `test/architecture/upgrade_budget_pin_test.go`
- `test/simcluster/drills/lib/hot_update_budget.sh`
- `test/simcluster/tests/run-all.sh`
- `test/simcluster/tests/assert-identity.baseline.tsv`
- `test/simcluster/tests/kept-sites.baseline.tsv`
- `test/simcluster/drill-costs.tsv`
- `test/simcluster/expected-verdicts.tsv`
- `test/simcluster/expected-verdicts-log.md`

**P17 — 文档与证据封口。** 目标：§11活文档与当前状态一致，将本候选按lead裁决复制成仓库binding plan；owner只凭真实receipt勾tasklist。无新增产品测试；拥有docs layout/test map/docs version门的文档侧结果；变异：删matrix地图行、把未跑Darwin记PASS、把39P改GREEN/掩native3，审核和门必须拒绝。更新耗时估计2–5s门边际，实际分析时间不冒充测试秒数。

- `CLAUDE.md`
- `docs/distributed-broker-architecture.md`
- `docs/usage.md`
- `docs/testing-standards.md`
- `docs/deploy-tier-gotchas.md`
- `docs/reviews/hot-update-stages-plan.md`
- `docs/reviews/hot-update-handoff-failures.md`
- `docs/reviews/hot-update-handoff.md`
- `docs/reviews/hot-update-tasklist.md`
- `docs/reviews/hot-update-evidence.md`
- `docs/reviews/simcluster-coverage-inventory.md`
- `docs/reviews/INDEX.md`
- `docs/reviews/hot-update-pty-terminal-plan.md`
- `test/README.md`
- `test/simcluster/README.md`

以上 **18包、154个唯一文件**；本节列的是将来实施写集合，本次没有创建这些文件。未列路径只读；必须新增/修改别的文件时，lead先调整唯一owner表，不让多个包临时争抢。

## §13 开放问题（仅 lead 的产品/范围判断，5 项）

| # | 需要冻结的判断 | 推荐 | 对出口的影响 |
|---|---|---|---|
| Q1 | S4 是否扩到 broker/数据库 authority incarnation，以覆盖finalPrepare后丢删除事件且同SID重建？ | **本阶段不扩**；明文限定可观测事件/持续gone的撤权保证，保留重建反例给S8权威连续性专项 | 必须同步修订范围并保留C-10整体未勾；若要求无此限制，先追加独占包与迁移合同再冻结，当前候选不能声称全覆盖。 |
| Q2 | “子树清场”是否要求杀掉主动setsid脱离原session的后代？ | **S4限定原session及已验证捕获成员**；同SID多PGID/stopped必须清完，setsid逃逸明列不覆盖 | containment/cgroup是单独产品范围；C-8/C-10措辞与receipt不得写任意恶意后代保证。 |
| Q3 | 非TTY外部SIGINT/SIGTERM要本地取消还是可靠远端转发？ | **本地取消**，沿现根NotifyContext；raw0x03和显式signal op负责远端语义 | usage与T39按此判；不同时保留两套竞争handler。 |
| Q4 | S4是否把500ms冻结目标由校准直接升成release硬门？ | **继续校准**，完整报告超标；5s正常请求/业务停顿仍硬判，S8统一冻结性能profile | 不偷偷提高500ms；若lead升格，36A超标必须红，不能首跑后再改回记录。 |
| Q5 | 可靠run首版是否接受不可poll/cancel的regular-file/socket本地stdio？ | **create前64拒绝**，支持TTY/FIFO；文件输入/输出通过本地pipe适配；legacy保持原兼容 | 确保取消/join承诺真实；若必须直接重定向文件，需另定可取消IO机制与范围，不能用永久goroutine补洞。 |

180s客户端恢复窗、domain能力而非ABI2、双序列、39P原生红、Darwin不广告可靠PTY均已在正文作出确定推荐，不再作为可由实现者任选的开放分支。lead未签§13前，P0不以“默认用户批准”落地。

## §附录 A：critique item → adopted/rejected → 落点

索引以每份 critique 的最后 must-fix 清单编号为准，保留其 BLOCKER/MAJOR 严重度；括号给前文详细项别名。每行都给具体设计和可判别测试；“采纳边界”指接受 critique 指出的不可证明性并列 lead 范围裁决，不代表该缺口已经修好。四份各15项，共60项，未将相同问题去重成一个漏账项。

### A.1 `crit-eof.md`

| item | 处置 | 具体落点 |
|---|---|---|
| E§6.1 BLOCKER（§2.1）draining leader身份 | 采纳 | §3.1/3.3/5.1允许ReapHeld zombie及失效tty SID；T02跨exec尾流；不重设ctty。 |
| E§6.2 BLOCKER（§2.2）真partial write | 采纳 | §3.2、T04必须一次真实`0<n<attemptLen`，记录n后冻结；`0<A<Q`单独不算。 |
| E§6.3 BLOCKER（§2.3）输入write失败wire | 采纳 | §2.1/3.2 InputError/InputEnded+最终A/Q；T12/T37，rc0不能吞abort。 |
| E§6.4 BLOCKER（§2.5）Pending/End不闭合 | 采纳 | §3.3固定可保留前缀E，非交付Pending不再长End；T13真实非空Pending、terminal-ack完成。 |
| E§6.5 BLOCKER（§2.7）撤权先泵后kill | 采纳 | §4.2/6.2先封输入、清场、允许泵；T20原slave外部副作用零新增。 |
| E§6.6 MAJOR（§2.6）abort/source/reap混淆 | 采纳 | §3.3 SourceEnded与Discarding分离、joinread、保reaphold；T02/T13。 |
| E§6.7 MAJOR（§2.4）FIN整包终点 | 采纳 | §3.2全包容量接纳、异常前缀不FIN、F=Q；T05/T11三种FIN坏变异独立。 |
| E§6.8 MAJOR（§2.8）watchdog原ctl/KA | 采纳 | §7.2单attempt+180s原handle、§6.1原期限；T40/T41、36W。 |
| E§6.9 MAJOR 36/39P判词 | 采纳 | §9.2/9.3正36GREEN、旧39Pnative3及parentjoin；T48负控三向。 |
| E§6.10 MAJOR（§2.9）pending capability | 采纳 | §2.2真实cap+busy、§5.2 target/prev门；T31同时堵legacy。 |
| E§6.11 MAJOR Hangup/foreground/清场 | 采纳 | §3.4错误事务表、不新增CONT、§6.2同SID跨PGID；T08/T09/T15/T16，Q2范围。 |
| E§6.12 MAJOR（§2.10/2.11）legacy阻塞/nonTTY信号 | 采纳 | §7.1有限queue、不堵wildcard；§7.3单一根取消；T39/T42。 |
| E§6.13 MAJOR（§2.12）spawn前容量 | 采纳 | §5.1 duplicate先查、预算在Start前、postspawn同handle终态；T25。 |
| E§6.14 MAJOR merged/tombstone/settle | 采纳 | §3.3/5.1/6.2空stderr、closed master/tombstone、终ack丢失；T19/T24/T37。 |
| E§6.15 MAJOR出口/任务收据 | 采纳 | §0/8/11逐项test+mutation+drill，NOT_RUN不勾；P17封口。 |

### A.2 `crit-fd.md`

| item | 处置 | 具体落点 |
|---|---|---|
| D§6.1 BLOCKER（D1）wrapper/borrow/poller | 采纳 | §3.1唯一Take、frozen只读Check、dup失败归属、实际两向deadline；T03/T10。 |
| D§6.2 BLOCKER（D2）单向deadline隔离 | 采纳 | §4.1每方向owner deadline，T06四隔离臂及超时两向恢复。 |
| D§6.3 BLOCKER（D3）外层锁等待 | 采纳 | §3.4/4.1从入口deadline；T07/T50，Runtime/Manager锁外join/scan/network。 |
| D§6.4 BLOCKER（D4）n记账/双泵 | 采纳 | §4.1每generation done在n记账后；T04/T06丢n/双泵/停泵mutation。 |
| D§6.5 BLOCKER（D5）撤权前输入 | 采纳 | §4.2/6.2，T20/T21；修现`fence.go:221–226`。 |
| D§6.6 BLOCKER（D6）部分Thaw | 采纳 | §4.2 active边界在Manager.Thaw前，失败只当前cut；T23/T24。 |
| D§6.7 BLOCKER（D8）Darwin | 采纳 | §7.3 portable legacy不用Replay/冻结假设；T44实际平台与build分列。 |
| D§6.8 MAJOR（D9/D10）ACK/错误/seq/credit | 采纳 | §2.1/3.2双序列与InputError；T11/T12。D10指出P的credit前提有效，故不以“P必死锁”否决P；择双序列是设计取舍。 |
| D§6.9 MAJOR（D12）pending/恢复 | 采纳 | §2.2/5.2/7.2真实cap、180s绝对窗；T31/T41。 |
| D§6.10 MAJOR race/leak/matrix/39P | 采纳 | §8.4/9.3/10；保旧tuple、加新run矩阵，成本承认额外build；T18/T34/T48。 |
| D§6.11 MAJOR（D7）Hangup完整表 | 采纳 | §3.4 frozen/error/ESRCH/close_error均有receipt与恢复语义；T08/T09。 |
| D§6.12 MAJOR（D11）local worker/nonTTY | 采纳 | §7.3独立OFD、Cancel+join、不可poll类型预create拒绝；T39/T43，Q3/Q5。 |
| D§6.13 MAJOR wire/op/stamp | 部分采纳；拒绝single probe建议 | §2.1唯一六op、18字段、agent深拷贝覆盖；探测采用两步，S3 runtime.go:238–249先拒无Handle而非unknownop，详§1.1；T27/T28/T38/T45。 |
| D§6.14 MAJOR tombstone/salvage | 采纳 | §5.1完整文件集合与kind-aware MergeResources；第二task失败原master仍可回派，T19/T34。 |
| D§6.15 MAJOR tasklist/setsid边界 | 采纳边界 | §6.2/11/13 Q2；T15/T16只证同SID及捕获身份，不声称恶意逃逸containment。 |

### A.3 `crit-auth.md`

| item | 处置 | 具体落点 |
|---|---|---|
| A§6.1 BLOCKER（A1）activation撤权优先 | 采纳 | §4.2/6.2，T20真实queued输入外部副作用。 |
| A§6.2 BLOCKER（A2）BootNow失败 | 采纳 | §6.1先登记ClockUnknown、禁续权/GC；T21三窗口。 |
| A§6.3 BLOCKER（A3）绝对期限/代proof | 采纳 | §6.1持久expiry/opportunity/nextjudge，proof绑定generation、两freshproof、只一次机会；T22三代反例。 |
| A§6.4 BLOCKER（A4）首迁pending旁路 | 采纳 | §2.2/5.2订阅前关legacy、cap真实、reserve前busy；T31。 |
| A§6.5 BLOCKER（A5）全部retained状态 | 采纳 | §5.2含settled Registry/clock_unknown/resource kind，三次检查；T30/T32；代码`registry.go:282–305`支持批评。 |
| A§6.6 BLOCKER（A6）最后Export≠权威日志 | 采纳边界 | §6.3/Q1公开不可观测反例，不把register-gone当全部；TestPTYSessionRecreationBoundary与C-10/C-11不勾规则。 |
| A§6.7 MAJOR（A7）清场可续做/范围 | 采纳边界 | §6.2多PGID、pidfd/identity、循环census、CleanupPending跨cut；T15/T16；Q2不外推setsid。 |
| A§6.8 MAJOR（A8）Hangup guard/失败seq | 采纳 | §3.4，frozen未执行不消费seq/reaped，Signal失败receipt可判；T08/T09。 |
| A§6.9 MAJOR（A9）实际可调度恢复 | 采纳 | §7.2单attempt、180s绝对窗和原KA，T40/T41、36W。 |
| A§6.10 MAJOR（A10）全op权限负例 | 采纳 | §2.1/8 T27/T28全scope+inbox+epoch+payload+ActorFP，broker仅switch。 |
| A§6.11 MAJOR input责任/错误 | 采纳 | §3.2只A释放，InputError/Ended、FIN单终点；T11/T12/T37。 |
| A§6.12 MAJOR daemon事件/gone分开 | 采纳 | §6.2引`agent.go:1051,1779`；T26两个不同daemon断言。 |
| A§6.13 MAJOR pty_busy移除防洞 | 采纳 | §7.1完整legacy并集及gate次序，T29四阶段+latecut，保全部其它拒绝。 |
| A§6.14 MAJOR负控原生红 | 采纳 | §9.3 native3/parentjoin/owner，T48、P15；不改变S4“36 GREEN”。 |
| A§6.15 MAJOR出口/同候选收据 | 采纳 | §0/8/11同SHA映射，真实mutation与drill，cleanup不充当oracle。 |

### A.4 `crit-drill.md`

| item | 处置 | 具体落点 |
|---|---|---|
| R§6.1 BLOCKER 36/39P/native/sweep | 采纳 | §9.3精确parentjoin与owner literal；35C语义纠正；39P原生PRODUCT-RED，三个rc不混用。 |
| R§6.2 BLOCKER watchdog原handle | 采纳 | §7.2/9.2，T41、36W真120s，不能重启ctl/改OfflineAfter/授新KA。 |
| R§6.3 BLOCKER current rollback非空 | 采纳 | §4.2/9.2 W真实prequeued推进+T24/T35通信臂ACK/Base/control推进；旧bundlemutation必红。 |
| R§6.4 BLOCKER双PTY/bytes/原foreground | 采纳 | §9.1真实TIOCSCTTY/waitpid、有序rawbytes，不靠echo或normalize；T35。 |
| R§6.5 BLOCKER relay竞态/伪去重测试 | 采纳 | §1.1、T36先阻回包后转发，外部effect receipt；signal handshake/resize中间改值。 |
| R§6.6 BLOCKER撤权/partial activate/inputabort | 采纳 | §3.2/4.2/6.2、T12/T20/T23/T37。 |
| R§6.7 MAJOR HUP敏感负控采集 | 采纳 | §9.2/9.3故障前alive是setup，故障后死是产品观察；不照抄39Y存活前提。 |
| R§6.8 MAJOR FD/日志真实性 | 采纳 | §8.4/9.1 PTY专属census、owner活时查、完整日志cursor；T48漏资源/日志失败负例。 |
| R§6.9 MAJOR strict探针/500ms/成本 | 采纳 | §9.1 strict新增模式，§9.4重算34七臂与2×worst，§10边际秒，Q4冻结预算口径。 |
| R§6.10 MAJOR五出口/tasklist | 采纳 | §0/11每项receipt映射，NOT_RUN不勾。 |
| R§6.11 MAJOR唯一wire/image/旧broker | 部分采纳；拒绝single probe建议 | §2/5/9.5精确18字段、双seq、pending不fallback、broker仅两switch；S3提前identity检查决定采用两步，§1.1代码证据与T38覆盖。 |
| R§6.12 MAJOR固定名/race/leak/Darwin | 采纳 | §8四固定名、P16整包matrix/五轮锚、P13内层race/overlay与额外build成本、T44实际Darwin。 |
| R§6.13 MAJOR真syscall/EIO/FIN/fg | 采纳 | T01–T06/T14各非空前提及独立变异；raw0x03与显式signal目标区分。 |
| R§6.14 MAJOR nonTTY/stopped Hangup | 采纳 | §3.4不CONT、不自动KILL；§7.3根取消、Q3；T09/T39。 |
| R§6.15 MAJOR所有机械门owner | 采纳 | §10逐门、§12 154文件唯一owner；Agent恰125、inventories只增、enum/error/layering/ctx/naming不豁免。 |

### A.5 没有采纳的草案选项及理由（不遗留正文二选一）

| 选项 | 处置 | 理由/证据 |
|---|---|---|
| 原样保持ABI1且无domain门、只看活Tasks | 拒绝 | strict decode与settled保留事实见§1.1，必须§5全部retained检查。 |
| 整体升级ImageABIVersion=2 | 拒绝此选项 | 本阶段新增域可通过omitempty及能力门保留纯exec S3 reader；并非声称ABI2本身错误。T30真实旧reader是这项选择的责任。 |
| P统一OpSeq | 拒绝此选项 | credit前提可以成立，FD批评对此是正确的；采用双序列减少控制与byte写入耦合，§3.2/3.4明确各自排序保证。 |
| E§3 N-1、D§6.13、A§3协商次数、R§3探测所建议的single pty-probe | 拒绝该建议 | 单步可识别旧broker，却不能正确分类新broker后的S3 agent：runtime.go:248提前process_identity。采用C两步、共2s、完整scope校验；T38必须含真实S3版本。 |
| pending时cap=0 | 拒绝 | 会使新ctl进入legacy；两步cap均须真实，busy留到Reserve前；§2.2/T31关闭。 |
| 一律要求恢复时TIOCGSID/foreground==原leader | 拒绝 | workload已允许zombie/ReapHeld（process_state_linux.go:15–18）；kernel状态不是永久identity，T02。 |
| 复用Output.Abort就宣布finalEnd稳定 | 拒绝 | output.go:369–383不joinread；必须§3.3 finalizer。 |
| 500ms attempt意味着PTY dispatcher有界 | 拒绝 | client.go:291–331内层retry无总体截止，§7.2重构。 |
| 35.C先例允许把39P raw FAIL改GREEN | 拒绝 | 35C脚本`:449`窄判据与validator parentjoin反证；§9.3。 |
| 为Hangup补SIGCONT或把KA升级SIGKILL | 拒绝 | 与既有pty.go:358–364 HUP-immune合同冲突；管理撤权独立清场。 |
| 所有legacy平台统一Linux replay actor | 拒绝 | replay_unsupported.go和master_other.go的平台事实；§7.3/T44。 |

冻结前lead核对：§13五项已写裁决、P0类型/tag签名与§5一致、§12文件唯一、附录60项都有落点、所有“已完成”表述有同候选receipt。本文目前仍是候选，全部新增验收保持 NOT_RUN。
