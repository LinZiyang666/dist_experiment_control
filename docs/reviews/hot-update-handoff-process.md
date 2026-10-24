# 热更新进程请求与执行 Runtime 交接

## 1. 停止点与结论

用户已明确停止本轮实现，转交 Claude。本交接仅整理已有代码、测试和收据；没有修改生产、测试、inventory、暂存区，也没有在停止指令后启动新验证。

**完整热更新未完成，当前 `internal/execsession` 仍为 Fail。** tunnel 审查线确认的 F1/F2 尚未修复。`internal/procrequest` 的有界组件审查通过，不代表可靠 exec 的整条路径或完整热更新通过。

本线归档：

- [procrequest 独立测试报告](hot-update-procrequest-test-report.md)：原样复制 `/tmp/tether-control-recovery/procrequest-registry-system-report.md`，11 项测试。
- [execsession 独立测试报告](hot-update-execsession-test-report.md)：原样复制 `/tmp/tether-control-recovery/execsession-runtime-system-report.md`，4 项成功主线测试，报告首行为 Fail。
- 两份报告的源码哈希标识当时的测试快照；后续源码或 proto 的变化不能自动继承这些收据。

后续总体契约继续以 [正式计划](hot-update-plan.md) 和 [任务清单](hot-update-tasklist.md) 为准。agent、broker、NATS、Caddy、ctl 的连续性要求、freeze ≤500ms、新控制请求 ≤5s、原业务不中止及全部旧映像退出，均未被本组件测试证明。

## 2. 代码位置与职责

| 位置 | 当前职责 | 明确边界 |
| --- | --- | --- |
| `internal/proto/process_resume.go` | 独立 `process-resume` 动词、租约、请求句柄、控制请求、双输出批次 | 新 wire 不是给 legacy `exec` 加几个字段；旧 broker 解码再编码会丢新字段 |
| `internal/procrequest/registry.go` | 创建身份、唯一接纳、附件代数、终态与结算、有限容量及 GC | 不创建进程，不持有 pipe，不代替当前授权 |
| `internal/procrequest/state.go` | 私有 registry 导出与恢复 | 状态含 HMAC secret；不转移进程/流所有权，不核验 owner PID |
| `internal/procrequest/clock_linux.go` | Linux boot ID 与 `CLOCK_BOOTTIME` | 非 Linux 实现返回 unsupported；不用墙钟 TTL |
| `internal/execsession/runtime.go` | 将 Registry、Manager、Input、Output、Replay 接成可靠 exec 会话 | **没有 Runtime Freeze/Export/Import**，不能据此触发热更新 |
| `internal/agent/exec.go:595` 起 | 当前新增 `initProcessRuntime`、forwarded handler、停止清理适配 | 本线没有执行 agent 适配层集成测试 |
| `internal/agent/agent.go:387` | 持有 `processRuntime` | 当前接线完整性以主进程交接为准 |
| `internal/broker/exec.go:565` 起 | 新动词 broker 路由与授权入口 | 属另一审查线；本报告不替它认证 |
| `cmd/tether/exec.go` | 已出现新动词调用 | 本线未验证真实 ctl 的短写、续接、终态确认行为 |

只读收尾时看到的 agent 适配：使用 `process-` 加新 PID 生成独立 Runtime incarnation；以 `a.workloads` 为 Manager；Start 委派 `startExecPipes`，Started/Exited 回调进入 courier；forwarded handler 从当前 SID/NID、broker-stamped ActorFP 和句柄 lineage 构造 Scope。停止清理调用 Runtime.Close，预算 5s。这是源码观察，**不是适配层测试通过声明**。

现存 `execsession-adapter-compile.log` 仅记录过 `execsession [no test files]` 及 agent 的编译型定向运行，不能替代新增 Runtime 测试或真实产品链路。

## 3. 创建身份协议：已经裁决的规则

1. **完整 `ProcessHandle{Lease, RequestID}` 是请求身份。** LeaseID 是去重键的一部分。不能只以 ClientLineage/RequestID 永久去重，也不能结果 TTL 删除后把旧请求当新任务。
2. Lease 绑定 Version、LeaseID、TargetInstance、BootID、SID、NID、ActorFP、ClientLineage、IssuedBootNS、ExpiresBootNS 和 MAC。Registry 实例隐含绑定 TargetInstance/secret。
3. 实际 map key 是 Scope + LeaseID + RequestID。runtime generation 不入 key，也不是授权凭据。
4. ctl 首次发送前必须保留完整句柄。超时重试保持原 lease 与 RequestID；**不得自动换租约/ID，也不得回退 legacy exec**。新 LeaseID 配旧 RequestID 是显式新任务。
5. lease 限制未知请求的首次接纳期限，不是已知活任务的运行授权期限。到期边界为 `now >= ExpiresBootNS`；已知 starting/running/terminal 仍可查询与续接，但每次都须外层重查当前授权。
6. 只有 `settled` 且 lease 到期的记录才 GC。starting、live、未 ACK 的 terminal 不因到期丢弃。容量满时拒绝新创建；不能驱逐还负有责任的记录来制造可用空间。
7. 同 key、同 effectful intent 返回原记录；同 key、不同 argv/env/cwd/stdin/safe 拒绝。服务端计算版本化 hash，环境 map 的顺序不影响结果；ActorFP 已在 Scope/lease 绑定，不作为 Exec intent 字段。
8. 只有 Reserve 返回的 `fresh=true` 授予内部调用者一次创建权。先留 starting 记录，再 Start。Start 失败也必须成为保留终态，不能删除 reservation 后允许重复执行。
9. 热恢复必须保留原 secret、完整记录与原 incarnation。冷启动丢失旧状态时必须形成新的 incarnation/key。现有 agent 路由 lineage 可继承，但不能因此给空 registry 冒用旧创建身份。
10. 私有、完整的合法空 snapshot 可以 Restore；“从未接纳任务的真实空状态”和“丢了旧状态后伪造空表”不能混为一谈。

`/tmp/hot-update-exec-wire-decision.md` 的早期 §4 仍提可选 lineage 高水位；本轮已经选定稳定 LeaseID 方案，不要从那段旧建议重新引入第二套协议。

## 4. `internal/procrequest` 全部公开 API

### 类型与限制

- `Scope{SID, NID, ActorFP, ClientLineage}`：ActorFP 必须通过 `auth.ValidFingerprint`；其他 token 有字符与长度校验。接收的是**已经经过外层当前授权**的身份。
- `Terminal{ID, ExitCode, Signal, Error}`：保留准确终态；ID 有 token 校验，exit/signal 有范围与组合校验，Error 有 UTF-8/4096 字节上限。
- `Record{Handle, IntentHash, PID, Inbox, AttachEpoch, Phase, Terminal}`：Phase 为 starting/running/terminal/settled。返回的 Terminal 不与内部记录共享可写对象。
- 默认容量 `DefaultLimit=4096`，上限 `MaxLimit=16384`；默认 lease TTL 为 30s，上限 5min。`New` 的 limit/ttl 为零时采用默认值。
- 公开错误：`ErrIdentity`、`ErrExpired`、`ErrConflict`、`ErrCapacity`、`ErrFrozen`、`ErrBusy`、`ErrStale`、`ErrUnknown`。

| API | 当前契约 |
| --- | --- |
| `New(instance string, limit int, ttl time.Duration) (*Registry, error)` | 创建新的 secret，读取真实 boot ID，默认时钟为 boot clock |
| `Issue(scope Scope) (proto.ProcessLease, error)` | 签发有限时长 lease；不为 lineage 维护无界表；冻结或无有效时钟时拒绝 |
| `Reserve(scope, handle, spec, pid, inbox, epoch) (Record, bool, error)` | 原子接纳并选唯一创建者；bool 为 true 才能 Start；重复返回原 PID；不同 intent 拒绝 |
| `Lookup(scope, handle) (Record, error)` | 验证绑定与签名后返回已知记录；过期的未知身份不能重新创建 |
| `Attach(scope, handle, inbox, epoch) (Record, error)` | 只接受更新的附件代数，或同代同 inbox 的幂等重试；旧代/同代换 inbox 拒绝 |
| `Running(handle, pid) error` | 必须匹配已接纳 PID；不能把 terminal/settled 改回 running |
| `Complete(handle, pid, terminal) error` | 必须匹配原 PID；相同终态幂等，不同终态冲突；setup error 也有记录 |
| `Settle(scope, handle, terminalID) error` | 检查当前身份与准确 terminal ID；**调用者先验证双输出 ACK/FIN**，本层不确认字节 |
| `Collect() int` | 只回收 settled+expired；冻结或时钟错误时不回收 |
| `Freeze() (State, error)` | 有 starting 时 ErrBusy；成功后冻结所有状态变化与 GC，深拷贝 secret/records |
| `Thaw()` | 显式重新开放状态变化 |
| `Restore(state State) (*Registry, error)` | 全部校验后返回冻结态 Registry，失败不暴露部分 owner |

`State{Version, Instance, BootID, Secret, Limit, TTL, Records}` 是私有 handoff 数据，不是可公开日志或未经认证的恢复文件。Restore 校验真实当前 boot、签名、配额、唯一 key/PID、phase/terminal/附件结构；拒绝 starting。owner PID/startticks、资源交叉引用与联合切换属于外层 handoff/runtime 的职责。

## 5. `internal/execsession` 全部公开 API与内部生命周期

```go
type Config struct {
    Instance string
    Manager *workload.Manager
    Start func(string, *proto.ExecReq) (*workload.Stdio, string, error)
    Started func(string, proto.ExecReq)
    Exited func(string, int)
    ReplayCapacity int64
}
func New(Config) (*Runtime, error)
func (*Runtime) Handle(procrequest.Scope, proto.ProcessResumeReq, string) (proto.ProcessResumeReply, error)
func (*Runtime) Close(context.Context) error
```

- New 要求全部 hook 和 Manager 非空，ReplayCapacity 默认为 workload 的默认值且有上限。Registry 当前用默认限额/TTL。
- Start 成功须转移明确 Stdio pipe 与**已经 AdoptDraining 的 child**。失败须不遗留 pipe；未返回的安全 spawn helper 属 adapter 责任。Started/Exited 只应入队责任，不能等待网络或 ACK。
- **Started 回调当前先于真实 Start**。不能把 courier 入队等同于已成功获得真实 OS PID；“新请求真实 started ≤5s”还需要端到端真实 PID 证据。
- Handle 要求协议 Version、非零 CallID、req.ActorFP 与 Scope 一致。支持 probe/create/resume/ack/terminal-ack；未知 op 拒绝。
- probe 无副作用签发 lease。create 原子 Reserve，仅 fresh 时创建 task 并异步 start；对 Exec 的 slice/map 做副本，避免 handler 生命周期结束后借用可写字段。
- task 持有 stdin/stdout/stderr 三个 Replay、Input、两个 Output、完成信号。stdin 是 ExecReq 内的一次性 payload，暂不是任意交互输入协议。
- stdout/stderr 独立累计偏移；每次响应每流最多 32KiB。先检查两流 ACK 的范围与 FIN，再释放任一前缀。当前实现对低于 Base 的 ACK 也返回偏移错误；ctl 的旧请求/旧回复处理须与这个实际契约一致。
- 输出结束后释放 ReapHeld，由 Manager sole Wait4 获得真实退出收据。Runtime 先 Complete 保住结果，再 Exited 入队、Forget process record。终态 ACK 与 broker courier ACK 是不同责任。
- terminal-ack 要求准确 terminalID、双流 FIN、已确认所有输出；成功 Settle 后关闭流资源、删除 task，registry tombstone 留到 lease 到期。
- Close 是真实资源销毁：关闭准入、SIGKILL 自己拥有的进程组、等待 task 完成并清理。**断连、broker 切换、内部重连、计划 freeze 不能调用它。**
- Runtime 当前没有联合 admission fence、Freeze、Export、Import 或 same-PID Runtime 接管能力。Registry/Process/Input/Output 的单独原语通过，不等于它们已经形成一致资源图。

## 6. 尚未修复的阻断项：tunnel F1/F2

以下两项已经由另一审查线写真实反例，本线交叉阅读认可；停止时仍未修改生产。原始确认日志：`/tmp/tether-control-recovery/execsession-failure-confirmed.log`，该组原始结果 **FAIL，0.095s**。

### F1：Close 已取消后，晚到的 Start 失败无人最终回收

- 位置：`runtime.go:172 start`、`:234 failStart`、`:372 Close`。
- 时序：Start hook 阻塞 → Close 设置 closed，因 ctx 取消返回 → hook 随后返回 error → failStart 只关闭 stdin replay，Finish 两个输出 replay → done 关闭，但已返回的 Close 不再清理 task。
- 实际证据：`TestRuntimeCloseCanceledStartupReclaimsLateFailure` 中仍有 1 个 task、两个输出 replay 仍可 Read 返回 EOF、预算仍占用，探针再次分配失败。
- 后续修复须让已关闭 Runtime 的迟到启动结束路径承担最终回收责任；不能依赖调用者恰好第二次 Close。仍要保留未返回 Start 的 owner，不能 timeout 时冒充它已经结束。

### F2：真实 Start 后的 pump 初始化失败关闭了终态所需 replay

- 位置：`runtime.go:172 start` 错误分支、`:334 outputBatch`、`:354 closeTask`。
- 时序：child 已真实启动并被 Manager 接管 → warning 超过合法小 replay 的容量使 stderr pump 构造失败 → kill/reap 成功，但 closeTask 把输出日志一起关闭 → Registry 有 terminal，resume 读取 replay 得到 `file already closed`。
- 实际证据：`TestRuntimePumpSetupFailureRetainsObservableTerminal` 得到真实 signal9 的 terminal 元数据，却不能通过 resume 观察/结算该失败尝试。
- 后续修复须区分停止原 pump/pipe 和释放仍需交付的结果，不能伪造成功 FIN 或丢掉已读字节。准确失败终态应可重试查询并最终 ACK 清理。

同组正例已绿：真正满 stdout、客户端无 ACK 时 Close 能结束原 child、回收 Manager 与预算；第三次 Replay 分配失败也能保留并结算 setup terminal。这些正例不关闭 F1/F2。

## 7. 已运行的 11 + 4 项独立测试及原始收据

原始日志均在 `/tmp/tether-control-recovery/`。归档报告保留了全部函数名、行号和哈希。

| 组 | 实际命令/结果 | 收据 |
| --- | --- | --- |
| Registry 全部 11 Test | `go test -race -count=3 ./internal/procrequest`，PASS 4.558s | `procrequest-registry-final.log` |
| Registry 最初 9 Test | 单轮 race PASS 1.109s | `procrequest-registry-first.log` |
| Registry 原 same-PID exec | 单轮 race PASS 2.105s | `procrequest-exec-first.log` |
| Registry 过期检查负控制 | `/tmp` Go overlay，预期 FAIL rc1，0.038s；GC 后旧请求得到 fresh=true | `procrequest-no-expiry-negative.log` |
| Registry 终态深拷贝负控制 | `/tmp` Go overlay，预期 FAIL rc1，0.038s；调用者把保留 signal15 改为1 | `procrequest-terminal-alias-negative.log` |
| Runtime 成功主线 4 Test | 下方原命令，三轮 race，PASS 10.743s | `execsession-runtime-final.log` |
| Runtime 初次主线 | 单轮 race PASS 4.249s | `execsession-runtime-first.log` |
| Runtime 双流 ACK 原子性负控制 | `/tmp` overlay 先释放 stdout 再校验 stderr，预期 FAIL rc1，0.062s | `execsession-ack-partial-negative.log` |

Runtime 实际原命令（shell 单引号内是普通正则 alternation，不带反斜线）：

```sh
go test -race -count=3 -run '^TestRuntime(DuplicateCreate|DisconnectedClient|RejectsCrossActor|RepeatedExecution)' ./internal/execsession
```

这些负控制均已执行并结束，不是待跑项；没有直接改生产文件。Runtime 最终测试文件在三轮通过之后仅改了一条失败诊断，避免反射打印活跃 `os.File`；未修改 oracle。停止后没有为该诊断改动重跑测试。

核心证据：

- Registry 32 并发接纳只产生一个实际子进程；丢首响应返回原 PID；lease 精确边界、GC 与并发旧重试不能重新授予创建；18 类非法 Restore 全部拒绝且不暴露部分 owner。
- Registry 真同 PID exec 保留原子进程的 PID/startticks/PPID、原输入 pipe、rc23，以及另一真实子进程在 exec 前退出的 rc17。冷启动检查是在同一测试进程中 New 新 instance/key，**不是真重启 drill**。
- Runtime 16 并发重试只触发一次真实 cmd.Start；计数在 Start 成功后、Adopt 前增加，避免重复进程被 Adopt 拒绝后从统计中消失。Started/Exited 也精确计次。
- Runtime 48KiB 真 stdin 与 stdout/stderr/FIN/rc23 完整；256KiB 真输入在 1KiB replay 下形成实际背压，epoch2 续接保原 child 与原 stdout/stderr inode。丢失一批响应不会自动 ACK。
- Registry 五轮资源锚：`registry_test.go:689`，循环691、builtin g694/FD695、helper698。
- Runtime 五轮资源锚：`runtime_test.go:430`，循环432、builtin g435/FD436、helper439。FD 容差均为0。inventory/anchor 的最终整合由主进程负责，本线未更改。

## 8. 下一步陷阱和未验证范围

1. 首先修复 F1/F2，以原红测试转绿，再对成功路径回归；不要靠跳过失败测试、扩大资源容差或把关闭错误包装成成功来收尾。
2. Runtime 的完整 freeze/export/import、pre-Cobra 恢复、回滚、进程/流/courier/registry 联合 snapshot 仍须实现。不能把独立 Registry Restore 直接接升级按钮。
3. 任一 starting 接纳或未稳定的 spawn helper 都须挡住联合 snapshot。没有导入全部资源前不能激活新控制 owner，也不能注册空进程快照导致活任务被收成 EXITED(-1)。
4. 普通 exec 的 `AdoptDraining`/ReapHeld 必须保留：后代持有原 pipe 时，先退出的 shell leader 应保持 zombie 占住 PID，才能安全按原进程组 kill/evict；禁止 reap 后仍对裸 PGID 发信号。
5. 当前 Scope 只是参数。本线没有证明真实 broker JWT、当前 membership/ACTIVE、节点归属、撤权、私有 inbox、leader/follower 以及 N-1 路由。能力 probe 后切到旧节点仍必须拒绝；不能退回旧 exec。
6. ctl 必须持久保留完整 handle；按实际 stdout/stderr Write 的 n 记 ACK，保留短写后缀，处理旧 CallID/AttachEpoch、丢回复和 ACK 重试。没有真实 ctl 双端字节收据时不能声称“用户无感”。
7. Settle 成功但回复丢失，随后 lease 到期且 tombstone GC，重发 terminal-ack 会得到 ErrExpired。已经持有完整字节和终态的 ctl 需要明确收敛规则；不能重 create，也不能永远等确认。
8. Registry 状态含 secret，禁止在失败日志中 dump 整个 State。恢复只信任私有 authenticated handoff；冷状态丢失必须换 incarnation，不能因为 agent 路由 lineage 继承而重用空去重表。
9. 当前 Runtime 主线的“断开客户端”是停止 Handle/ACK 调用，未创建真实断开的 NATS socket。原数据由实际 pipe 产生，但不能外推到 Caddy/NATS 滚动、broker 切换或真实网络。
10. remote-FS 安全 spawn lease、Run/PTY、SSH 隧道、文件传输、broker tracker/SQLite、NATS/Caddy 退出、Linux runtime 全代码迁移均需要各自既定能力测试与真实 simcluster drill。计划升级不等于任意 kill-9/断电无损。
11. ≤500ms 从原资源停泵到新资源可 I/O 计时，包含 exec/Go init；≤5s 包含排队/重试。单 actor Freeze 小测与本报告的整体耗时都不是预算证据。
12. 没有在本线运行最新全量 gates、lint、matrix 或完整 deploy-tier。必须核对主进程交接的最终门状态、测试 inventory、leak anchor 和文档更新，不能套用更早版本的绿色门。

## 9. 运行会话清单与停止确认

本线没有仍在运行的测试、服务或负控制。先前启动的工具会话均已结束：

- Registry：34702、86324、44483、93310、96057。
- Runtime：99232、86339、15594。
- 93310/96057/15594 是预期 rc1 的负控制，其余已完成测试为 rc0。
- 停止指令后只补取了已经完成的 86324、99232 的退出回执，均 rc0；未启动新测试。
- 主进程和其他审查线的会话不由本线管理，不将它们推断成已结束。

完成本文件与两份报告归档后，本线停止工作，等待新的明确授权。

## S5 current contract addendum (2026-10-03)

The dated stop-point observations above remain historical. S0–S4 follow their own plans/receipts;
this addendum does not overwrite earlier hashes or turn historical NOT_RUN into a current result.
The [binding S5 plan](hot-update-flow-carrier-plan.md) and
[live architecture §22.8](../distributed-broker-architecture.md#228-s5-flow-and-ss-domains-binding-contract)
define flow/SS ownership. [Final S5 receipts](hot-update-evidence.md#16-s5-flow-carrier-ss-receipt-index)
are NOT_RUN pending P29.

Endpoint owns public listener/socket/journal/fence, rawsocket owns original kernel sockets,
flow owns replay/offset/FIN, SS actor owns parser/nonce/cipher suffix/material/authority;
carrier TLS/yamux is replaceable and not image state. Each image resource has one owner;
Prepared duplicates are separate cut-owned descriptors, and all private state uses sealed memfd.
PREPARE occurs before global freeze. Final Prepare re-exports every domain, including late revoke/fired/cleanup.
Restore takes all resources frozen with no workers, then Manager/revoke → raw pumps (SS AuthorityPending)
→ TLS/yamux REGISTER/exact OK → fresh bounded local-applied RESUME → SS.Authorize/data bind → NATS/register.
Any partial activation creates current-state responsibility; rollback cuts current offsets/nonces/retirement
at generation+1, never Detaches the pristine bundle. Exec failure Thaws originals and reacquires SS authority.

Flow ACK is original raw Write's n, with dedupe before Write; received≠applied, short suffix retained.
Source EOF fixes FIN End and CloseWrite once at Applied=End, preserving reverse tail. Abort preserves actual
prefix/final receipt without fake EOF. Retired OPEN never redials after receipt GC; SO_ERROR has one consumer.
Revoke irreversibly shuts originals/dup and bound public flow, including silent victims; legitimate survivors
are checked separately. SS uses actual-material salt history and an independent subkey+nonce ledger.

S4 failures §4's tail/partial/control/deadline/identity/EOF responsibilities require same-S5-candidate
36.A/W/C/L and39.P regression plus their units/mutations. PTY finalPrepare lost-event + same-SID recreation
remains S8. Broker same-PID exec/Endpoint export remains S6; NATS/Caddy switching S9; final mixed rollout S10.
500 ms full required-domain freeze is calibration with max/excesses, normal business max5 s is hard.
Darwin native lifecycle is NOT_RUN without a runner, distinct from cross-build and nonblocking under S4 L6.
Completion still requires original test/drill/docs/review steps and actual receipts; this addendum signs none.
