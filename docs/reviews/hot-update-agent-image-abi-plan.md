# S2 · agent-image-abi 实现 plan（定稿）

> 定稿：2026-09-21，主进程。来源：plan workflow `wf_3d6f574a-223`（4 视角草案 fence/kernel/schema/harness → 4 交叉批评 → 1 综合）。
> 主进程对综合稿的裁定见 §11（Q1–Q5 全部裁定）与本节「定稿修订」；未修订处综合稿原文即定稿。
>
> **定稿修订（对综合稿的改动）**
> 1. **exec harness 的放置与拼装**：`internal/execsession/runtime_image_exec_test.go` 保持 `package execsession`（内部测试，需要白盒收据），
>    因此**不 import `internal/agentimage`**（内部测试包 import 一个 import 本包的包是 import cycle）。冻结序在测试里手拼：
>    `Runtime.Freeze` → `Manager.Freeze` → `Manager.ExportProcesses` → `Runtime.Export` → `handoff.Prepare`；after 半
>    `handoff.Restore` → `execsession.Import` → `Manager.RestoreProcesses` → `Runtime.Thaw`（Manager 先 Thaw）。
>    Bundle 级的真 exec 由 `internal/agentimage/bundle_exec_test.go::TestBundleExecRestoresEveryDomain` 另证。§5.2 第 5 点据此改读。
> 2. **Q1 采 D1（Handle 冻结期阻塞）**；execclient 与 cmd/tether 零改动。`process_busy` 在计划路径不可见，因此不存在「延后」的缺陷。
> 3. **Q2 采 D11（running-abandoned 的 Discard 形状）**，常量 `AbandonedRunningGrace = 15 * time.Minute`，与 `AbandonedTerminalGrace` 同一文件、同一注释块；usage §9.13.2 与 §22.4 item 8 同步改。
> 4. **Q3 fail-closed**（`probesInFlight > 0` 即 deferred，含超时后保留的 sticky-dead 探测）；「D-state 线程阻塞 execve」在 §22.5 标为**假设**，S3 hangfs 臂验证。
> 5. **Q4 三条小门全落**（layering 行、ctx-free 小门、image 字段 append-only 账本），账本门镜像 `internal/proto/wire_inventory_test.go` 的形状（go/ast 收集、updater 只增拒删、`# gate-control` 正负控制）。
> 6. **Q5 接受**两处放置（courier 往返测试在 `internal/agent`；exec harness 在 execsession）。
> 7. Export 返回的 `Image.Registry.Secret` 只存在于内存与 sealed memfd：测试断言错误串/日志不含它（§5.1 `TestRuntimeImportErrorsNeverCarryRegistrySecret`）。
> 8. `TestAgentImageEntryPointsAcceptNoContext` 的白名单只列 `Runtime.Close(ctx)`（S0 既有签名）与 `workload.Output/Input.Freeze(ctx)`（S0 既有原语）；新导出符号一律 ctx-free。

基线：HEAD `cab90d1`，index tree `b342e0c1d0c6a693b9f5283210d042a09de29944`（本稿 `git write-tree` 复核相同）。全部行号取自该树，关键原语行为（`t.done` 只在 `conclude` 关闭、`Output.Freeze` 拒绝 `paused`、`Manager.Signal` 在 frozen 下 `ErrFrozen`、`handoff.TakeFile` 单向、INV-1 `result` 保留、`execclient.receive` 把任何 `Kind=="error"` 当终态、`boundedHomeRead` 不经 Policy）已逐条对照源码。

**综合原则**：四份批评一致判定的两条 BLOCKER（harness 稿"等 `t.done`"、fence 稿"导入失败 Close 已取 FD"）与一致的 MAJOR（courier 快照早于 fence、`done` 派生 phase 的 finish→conclude 窗口、Thaw 顺序丢补 kill、Manager 外来行、未知 JSON 键忽略、`launched && stUnprobed` 漏超时探测）全部按批评修正；四份草案各自正确的部分（kernel 的六类内核对象清单与三段式导入、schema 的 phase 派生/`Secret` 脱敏/DisallowUnknownFields/D-state 区分、fence 的 Manager 归属 agentimage 与 Thaw 响亮失败、harness 的两层 oracle 与 overlay 变异自证）保留。三个互斥方案（Handle 冻结期策略、running-abandoned、探测在飞判据）本稿各给出一个推荐并列入 §11 供主进程裁定。

---

## §0 目标与出口

**goal**（照抄 stages-plan S2）：把 execsession Runtime 变成带版本化状态 ABI 的可交接运行层：统一准入 fence、联合 Freeze/Export/Import/Thaw、`internal/agentimage.Bundle` 适配 `internal/handoff`、courier outbox 与 spawnsafe 状态导出、真实同 PID exec 证明原 child/pipe/字节/退出码连续。**不接产品路径**（`initProcessRuntime` / bootstrap / upgrade.go / marker / G.1 / session_destroyed 导入半 = S3）。

| # | 出口（stages-plan 原文） | 机械判定 |
|---|---|---|
| 1 | 五包 `-race -count=3` 全绿 | `go test -race -count=3 ./internal/execsession ./internal/agentimage ./internal/workload ./internal/procrequest ./internal/spawnsafe; echo rc=$?` → 0 |
| 2 | 真实同 PID exec：运行中 child（stdout 部分 ACK、stdin 未写完、stderr warning prefix、后代持 pipe）跨 exec 后 **execclient** 得到精确字节/顺序/rc；原 PID/start ticks/pipe inode 不变；TargetInstance/lease key 原值恢复 | `go test -race -count=1 -run '^TestRuntimeImagePreservesLiveChildAcrossExec$' -v ./internal/execsession`：父进程跑嵌入 NATS + 真 `execclient.Run` 作 oracle；receipt 字段逐项断言（§5.2），含非空性 `StdoutAckAtFreeze==4`、`InputBaseAtFreeze<len(stdin)`、`BlockedRequests>=1`、`DescendantAliveAcrossExec` |
| 3 | Freeze 在 starting/in-flight spawn 存在时 ErrBusy 或有界 defer；导入拒绝表（schema/boot ID/PID 复用/FD 指纹/预算超限/重复 handle）无部分安装、不 reap/不 signal | `go test -race -count=1 -run '^TestRuntimeImageFreezeRefusesWhileStarting$|^TestRuntimeImageImportRejectsInvalidBundle$' ./internal/execsession` + `go test -race -count=1 -run '^TestSpawnInFlightDefersFreeze$|^TestFreezeDefersOnForeignProcessRows$|^TestBundleRestoreRejectsPartialBundleWithoutInstalling$' ./internal/agentimage`；拒绝表每行断言：已取文件 `fstat` 仍成功且 CLOEXEC、fd 回基线、child 仍活、`Manager.Snapshot()` 逐行不变、goroutine 不增；表末旧 runtime `Thaw` 并 drain 到真实终态（"什么都没被消费"的终极证明）。**定稿修订**：PID 复用（start ticks）由 `Manager.RestoreProcesses` 校验（`internal/workload` 既有测试 + 真 exec 路径），execsession 层只校验行/任务互相覆盖与外来行 |
| 4 | 四条变异各自红（留 rc） | §5.4 的 M1–M4 用 `go test -overlay` 施加，每条 rc≠0 且失败文本含 `IMAGE_MUTATION=<name>` 自证标记；rc 与文本进 evidence §13.1 |
| 5 | 五轮 freeze/export/import 资源门；三硬闸；Darwin；inventory/anchor/矩阵登记 | `go test -race -count=1 -run '^TestRuntimeImageRoundTripReleasesResources$' ./internal/execsession`（进程内，FD/memfd 容差 0、goroutine 在 `AssertNoGoroutineLeak` 的 ±2 内——`GoroutineLeakTolerance=2` 是 helper 常量，容差 0 只对 fd 轴成立）+ `go test -race -count=1 -run '^TestBundleFreezeRollbackReleasesResources$|^TestBundleRepeatedExecReleasesResources$' ./internal/agentimage`（父进程基线）；`make test; echo rc=$?`、`make e2e-parallel; echo rc=$?`、`make lint; echo rc=$?`（各取 rc，不管道）；`GOOS=darwin GOARCH=arm64 go build ./... && make vet-tags`；三本清单只增（§6） |
| 6 | 同 PID helper 复用 handoff/procrequest 的 dispatch 形状 | 单测试函数 + mode switch、`-test.run=^Name$ -test.count=1`、`handoff.CleanEnvironment` + 自家 mode 键双向剥离（业务 child 的 `cmd.Env` 也剥，`runtimeChildEnvironment`）、helper 内只 `Prepared.Exec` 不二次 fork（after 半 `spawned_after==0`）；**定稿修订**：环境剥离的自证折进主测试的 receipt（业务 child 打印 `${MODE}${_TETHER_HANDOFF}` 为 `env::`、after 半二次 `Restore()` 为 nil），不另设 `TestRuntimeImageHelperEnvironmentIsStripped`（一次 exec 一个测试，成本 ≈2 s/-race） |

**本 stage 的 12 条定稿裁决（D）**——各节展开：

| D | 裁决 |
|---|---|
| D1 | **冻结窗内 `Handle` 阻塞而不拒绝**（quiescing 期只 create 阻塞；frozen 期全部 op 阻塞在 `sync.Cond`；Thaw/Close/Discard 无条件 Broadcast）。`ErrFrozen`/`process_busy` 只在非计划路径可见。理由：`execclient.receive` 把任何 `Kind=="error"` 当终态（client.go:351-353），create 后即 74；client 的 500 ms attempt 重发（:315-330）就是跨 exec 续接。不改 execclient/cmd（§11 Q1） |
| D2 | **"无在飞 spawn"的唯一判据是 `registry.Freeze()` 的 `ErrBusy`**（state.go:27-31，无副作用）；绝不等 `t.done`（它只在 `conclude` :385-392 关闭 = 任务终态已发布） |
| D3 | **Freeze 顺序 = registry → 泵 → （agentimage 层）Manager**，全部在 `r.mu` 下对 observe/Handle/Revoke/Sweep 原子；Manager 的 Freeze/Export/Restore/Thaw 归 `agentimage`（它是 legacy exec / PTY 共享表，exec.go:251 / run.go PTY `Adopt` 同一张 `a.workloads`） |
| D4 | **Thaw 顺序 = Manager 先于 Runtime**；`Runtime.Thaw` 的补 kill 遇 `workload.ErrFrozen` 在**改任何状态之前**判定并返回错误（响亮、all-or-nothing） |
| D5 | **phase 由 registry record 派生**（`running`/`terminal`），不导出 `done`；Import 对 terminal 任务自造已关闭的 `done`。finish→conclude 之间的不持锁窗口（:494-497 → :385-392）由 `TestRuntimeImageExportsTaskBetweenFinishAndConclude` 钉住 |
| D6 | **导入三段式**：①纯数据校验 → ②TakeFile + 只读 fd 谓词（fstat/F_GETFL/F_GET_SEALS/可轮询证明）→ ③构造。①②失败把已取文件原样交回 `Salvage{[]handoff.Resource}`，**绝不 Close 业务 FD**；③失败经 `Detach` 回收（释放预算、不关 fd）同样入 Salvage。Manager `RestoreProcesses` 是最后一个可失败步骤 |
| D7 | **所有域 `DisallowUnknownFields`**（plan §3.6 "未知状态字段在激活前拒绝"是绑定裁决）；新字段 `omitempty` 且零值合法；Version 不等即拒 |
| D8 | **courier 导出在 `Runtime.Freeze` 成功之后、`Prepare` 之前**（registry 无 starting ⇒ 无在飞 `enqueueStarted`；observe gated ⇒ 无 `enqueueExit`）；courier 不冻结投递（重发幂等） |
| D9 | **spawnsafe 拒绝 Freeze 的谓词 = `wedged>0 ∨ probesInFlight>0`**，后者是新的、与 map 无关的包级计数（launch 时 ++、`probe(mp)` 返回时 --）；sticky-dead 挂载在其探测线程返回前**永远 deferred**（fail-closed；D-state 阻塞 execve 记为**假设**而非 gotcha，§11 Q3）。abandoned helper 不进 schema：`wedged>0` 已 defer，且 `reapOnReturn` 会 kill+Wait |
| D10 | **`Owners.Busy []func() (reason, detail string, busy bool)` 钩子**让 S3 把 `homeReadInFlight`（agent.go:1773-1815，不经 Policy）、`pty_busy` 等塞进来而不改 `Freeze` 签名 |
| D11 | **running-abandoned 采 `Output.Discard` 形状**（15 min 无 touch 且流已满 → 该流在当前 End 记 aborted、泵改为读而丢弃、child 继续、字节保留到终态 sweep），不 kill、不伪造 Ack（§11 Q2） |
| D12 | **测试放置**：exec harness 与 execclient oracle 都在 `internal/execsession/runtime_image_exec_test.go`（stages-plan 点名的文件；execclient 不 import execsession，无环）；courier 往返测试在 `internal/agent/proc_delivery_test.go`（`procCourier` 未导出、agentimage 不得 import agent，§11 Q5） |

---

## §1 fence 状态表与 execsession 新 API

### 1.1 `Runtime` / `task` 新字段（runtime.go:117-130 / :76-99）

```go
// Runtime
quiescing bool           // Freeze 正在等 starting 归零：create 阻塞，attach 类 op 照常
frozen    bool           // Freeze 成功 → Thaw 之间；Import 出来的 runtime 生而为 true
exported  bool           // Export 成功过（旧映像）；Discard 的前提
imported  bool           // Import 构造（Thaw 时为每个 live 任务起 observer；Detach 的前提）
frozenAt  int64          // boot ns，Freeze 成功那一刻 r.now()
cond      *sync.Cond     // New/Import 时 sync.NewCond(&r.mu)
frozenRegistry procrequest.State  // registry.Freeze 的返回值（含 Secret，只在内存；Thaw 时清零）
// task
observing bool           // 有 observe goroutine（start() 进 observe 前置 true；Thaw 为 imported live 任务置 true）
frozen    *taskFreeze    // Freeze 借出的快照与文件；Thaw/Discard/Detach 后置 nil
```
```go
type taskFreeze struct {
    input  *workload.InputState      // nil ⇔ t.input == nil
    output [2]*workload.OutputState  // nil ⇔ t.output[i] == nil
    bare   [3]*workload.ReplayState  // 无 actor 的裸 log（failStart / setup-fail 的未建流）
    files  map[string]*os.File       // 六名之子集（§2.2），借用原件
}
```

### 1.2 API 签名（全部 ctx-free；新文件 `internal/execsession/image.go` + `fence.go`）

```go
var (
    ErrNotFrozen = errors.New("exec session: runtime is not frozen")
    ErrImage     = errors.New("exec session: invalid image")   // 每条校验拒绝 %w 它
)
func (r *Runtime) Freeze(deadline time.Time) error                       // procrequest.ErrBusy | ErrClosed | procrequest.ErrFrozen(已冻结)
func (r *Runtime) Export() (Image, []handoff.Resource, error)            // 只在 frozen；资源里的 *os.File 是借用原件（handoff.Prepare 复制）
func (r *Runtime) Thaw() error                                           // Manager 必须已 Thaw；否则不改状态返回 workload.ErrFrozen
func (r *Runtime) Discard() error                                        // 只在 frozen && exported：关闭自己的原件、不做 I/O、closed=true
func (r *Runtime) Detach() ([]handoff.Resource, error)                   // 只在 frozen && imported && 未 Thaw：拆除不关文件、释放预算、closed=true
func (r *Runtime) Frozen() bool
func (r *Runtime) TaskPIDs() []string                                    // agentimage 外来行预检
func (r *Runtime) ReplayBudgetUsed() int64
type FileSource interface{ TakeFile(id, name string) (*os.File, error) } // *handoff.Restored 满足
func Import(cfg Config, img Image, rows []workload.Record, files FileSource) (*Runtime, []handoff.Resource, error)
                                                                          // 失败返回 (nil, salvage, err)；salvage 是已取文件的 Prepare-ready 形状
```
`cfg.Instance` 为空则取 `img.Instance`，非空必须相等（S3 传原值；`initProcessRuntime` :615 的 `"process-"+NewPID()` 只用于冷启动）。`cfg.Clock` 经 `procrequest.RestoreWithClock` 注入 registry。

唯一新增 `context.Background()` 站点（`fence.go`）：
```go
// ctx-none: the image fence is deadline-bound by design (dataplane_lifetime rule:
// no caller-owned ctx may reach a data-plane object); workload.Output/Input.Freeze(ctx) is its only consumer.
ctx, cancel := context.WithDeadline(context.Background(), deadline)
```

### 1.3 Freeze 顺序

```
Freeze(deadline):
  lock r.mu; closed→ErrClosed; frozen→procrequest.ErrFrozen
  r.quiescing = true                        // create 从此阻塞在 cond；attach/probe 照常（字节流到最后一刻）
  loop:
    err := r.freezeLocked(ctx)
    nil        → r.quiescing=false; r.frozen=true; r.frozenAt=now; unlock; return nil   （不 Broadcast：frozen 仍阻塞）
    ErrBusy && now<deadline → unlock; sleep 5ms; lock; continue
    其它/到期  → r.quiescing=false; cond.Broadcast(); unlock; return err
```
`freezeLocked`（持 `r.mu` 全程）：
1. `state, err := r.registry.Freeze()`（state.go:24-40）——任一 record `starting` → `ErrBusy`，无副作用。**为什么这是唯一判据**：record 从 `Reserve`（registry.go:198）到 `Running`（runtime.go:355）或 failStart→`Complete` 恒为 `starting`，且这两个切换都在 `r.mu` 下；所以成功 ⇒ 不存在在飞的启动 owner，后面每一步都不会撞上"泵建到一半"。Freeze 报 `ErrBusy` 时 Warn 点名 starting 的 pid（`lostTerminal && starting` 是我们的 bug 态，会让节点永久 deferred 直到重启，§10 残余风险）。
2. 逐任务（按 pid 排序）：`t.input != nil → t.input.Freeze(ctx)`；`t.output[i] != nil → t.output[i].Freeze(ctx)`（§4-1 修补后接受 Abort 过的流）；actor 为 nil 而 `t.logs[k] != nil` → `t.logs[k].Freeze()`（replay.go:248-259）。任一失败 → 逆序 `Thaw()` 已冻结项 → `r.registry.Thaw()` → 返回。
3. `r.frozenRegistry = state`。

**持锁等泵是安全的**：泵 goroutine（output.go:114 / input.go:100）只取 `Output.mu → Replay.mu`，从不取 `r.mu`；锁序与 observe（`r.mu → Snapshot → output.mu → replay.mu`）一致。正常耗时 µs–ms；deadline 由调用方（S3 的 ≤5 s 安全点预算）传入。

**Manager 不在这里冻**（D3）：`agentimage.Freeze` 在 `Runtime.Freeze` 之后 `Manager.Freeze()`。两者之间 child 退出会被 reaper 记成 `Exit != nil` 行（非 ReapHeld）或保持僵尸（ReapHeld，reaper :289 跳过）——两种形状都随 ProcessState 导出并由新映像记账（§1.6）。

### 1.4 Thaw 顺序（`Runtime.Thaw`，旧映像原地与新映像激活共用）

0. `lock; closed→ErrClosed; !frozen→ErrNotFrozen`。**先判**：存在 `t.revoked != "" && t.terminalAt == 0` 的任务且 `cfg.Manager.Frozen()`（新增访问器）→ 不改任何状态，返回 `workload.ErrFrozen`（调用方顺序错了）。
1. 逐任务泵 Thaw：`t.input.Thaw()`（内含 `log.Thaw()` + 清临时 deadline，input.go:208-225）、`t.output[i].Thaw()`（output.go:220-235；restored 的已结束流 `run()` :119 立即退出不留泵）、裸 log `Thaw()`；`t.frozen = nil`。
2. `r.registry.Thaw()`；`r.frozenRegistry = State{}`。
3. 补发冻结期登记的撤权（§22.4 item 7(b)）：对 `t.revoked != "" && t.terminalAt == 0` 的任务 `cfg.Manager.Signal(t.pid, SIGKILL)`；`ErrUnknown`/`ErrExited` 忽略，`ErrFrozen` 不可能（步骤 0 已判）。幂等：非冻结 Revoke 已 kill 的任务再 kill 得 `ErrExited`。
4. `imported` 且 `!t.observing && terminalAt==0 && !lostTerminal` 的任务：`t.observing = true; go func(){ defer r.conclude(t); r.observe(t, nil) }()`（与 start() :297/:377 同形；`conclude` 是唯一发布点，closed 下自退——F1 语义保留）。
5. `r.frozen = false; cond.Broadcast()`。

**顺序论证**：registry 在泵之后 thaw，让 Handle 的第一个 resume 一定读到已复位的泵（临时 deadline 已清），而不是"registry 活着但 `SetReadDeadline` 还没清"的半状态——stages-plan risk "临时 pause deadline 当业务 deadline"就是这一步。网络（courier 投递、register）在 agentimage/S3 层，永远在 `Runtime.Thaw` 之后。

### 1.5 fence 状态表（来源 × 状态 → 行为）

| 来源 | open | quiescing | frozen（旧映像） | frozen+imported（新映像，Thaw 前） | closed / discarded |
|---|---|---|---|---|---|
| `Handle` probe | Issue | Issue | **cond.Wait**（Thaw→继续；Close→`ErrClosed`） | 同左 | `ErrClosed` |
| `Handle` create | Collect+sweep+Reserve+`go start` | **cond.Wait** | cond.Wait | cond.Wait | `ErrClosed` |
| `Handle` resume/ack/terminal-ack | Attach+acknowledge+batch(+Settle) | 正常（字节继续流） | cond.Wait（随旧映像消失，ctl 500 ms 重发到新映像） | cond.Wait | `ErrClosed` |
| `start()` owner | :296-378 | 正常（Freeze 等它到 Running/failStart） | **结构上不存在** | 不存在 | conclude 自退 |
| `observe` tick | :457-506 | 正常 | **整段 gate**：`:461` 取锁后 `if r.frozen { if r.closed {unlock; return}; unlock; <-timer.C; continue }`——不 `abortStalledRevokedLocked`、不 AllowReap、不 Snapshot、不 `input.Close`、不 finish | goroutine 不存在（Thaw 才起） | closed 分支 :467-470 |
| `finish` | :513-536 | 正常 | **不可达**；`:522-524` ErrFrozen→false 保留为 belt 并注明"只在 registry 被单独 Freeze（公开 API）时可达" | 不可达 | — |
| `Revoke` | mark+Signal+记 revocations | 同 open | **只 mark + 记 revocations，不 Signal**；返回 killed=被标记数 | 同左（S3 的 `session_not_found_or_deleting` 走这里） | `(0,nil)` |
| `Sweep`/`sweepLocked` | 正常 | 正常 | `:104` 加 `\|\| r.frozen` → `(0,0,nil)`（否则 `retireLocked` 关掉已导出的 log） | 同左 | `(0,0,nil)` |
| `Freeze` | → quiescing/frozen | `ErrFrozen`（重入） | `ErrFrozen` | `ErrFrozen` | `ErrClosed` |
| `Export` | `ErrNotFrozen` | `ErrNotFrozen` | Image + 借用资源；`exported=true` | 允许（回滚重导当前态） | `ErrClosed` |
| `Thaw` | `ErrNotFrozen` | `ErrNotFrozen` | 逆序 thaw + 补 kill | 逆序 + 补 kill + 起 observer | `ErrClosed` |
| `Discard` | 错误 | 错误 | 只在 `exported`：closeTask 每任务（关原件、不 I/O）、清表、closed=true、Broadcast | `exported`（重导后）同左 | `ErrClosed`（内审 F7 订正：不是 nil） |
| `Detach` | 错误 | 错误 | 错误 | 拆除不关文件 → `[]handoff.Resource`；closed=true；**thaw 过或 export 过即拒**（`thawed` 位，内审 F3/R2） | 错误 |
| `Close(ctx)` | :667-689 | thawLocked(泵+registry) 后 teardown | `exported` → 等价 Discard；否则 **进程表仍冻结 → 拒绝 `workload.ErrFrozen` 不改状态**，表已 thaw → thawLocal 后 teardown（内审 F4/V1/R3/P6 修订） | 同左；**无 observer 的 live 任务直接 `retireLocked`+close(done)**（没有 owner 会发布） | 幂等 nil |
| Manager reaper（外部） | reap | reap | agentimage 已 `Manager.Freeze` → :289 不 reap，退出的 child 成僵尸 | 新 Manager 无 poller 直到 `Thaw` :245-260 | — |
| 泵 | 读/写 | 读/写 | paused，临时 deadline 已清零，log frozen | Restore 出来即 paused 无 goroutine | Close |
| courier `Exited` | finish :533 | 同 | 不可达 | 不可达 | — |

### 1.6 冻结期 child 退出的记账（S1 review E5 冻结半）

三条路径全部归到"新映像 observe 记账，Exited 恰一次"：
- (a) child 在 **runtime 冻结、Manager 未冻**（agentimage 两次 Freeze 之间的微秒窗）退出：非 ReapHeld 行被 reaper 记成 `Exit != nil`（ReapHeld 行保持僵尸）；observe gated → finish 不跑 → ProcessState 以终态行/live 行导出（process_state_linux.go:31-33 僵尸合法、:33 终态行跳过身份检查）→ Import 接受 "running 任务 + `Exit != nil` 行" → Thaw 后首个 tick `outputsEnded`（泵 thaw 后读到 EOF）→ AllowReap → Snapshot 见 Exit → finish。
- (b) child 在 **Manager 也冻**之后退出：kernel 僵尸 → live 行（可能 ReapHeld）→ 新 Manager Thaw 起 reaper → 同上。
- (c) exec 之后退出：常规路径。

`finish` 的 ErrFrozen→false 分支保留。钉住：`TestRuntimeImageAccountsExitDuringFreeze` 两臂（§5.1）+ exec harness 任务 C。

### 1.7 starting / in-flight 处置

- `Freeze` 对 starting 的处置 = §1.3 的有界重试；到期返回 `procrequest.ErrBusy`，agentimage 映射 `DeferredError{Reason: "spawn_in_flight", Detail: "registry: N starting record(s)"}`。
- quiescing 期 create 阻塞，所以重试不会被持续到达的 create 饿死（fence 批评 MINOR：轮询间隙 Reserve 出新 starting）。
- 一个 `lostTerminal && Phase=="starting"` 的任务让 Freeze 永远 ErrBusy：Warn 点名、§22.5 已知限制、残余风险表。

### 1.8 runtime.go / revoke.go 逐点改动

| 位置 | 改动 |
|---|---|
| runtime.go:149 `New` | `r.cond = sync.NewCond(&r.mu)` |
| :161-163 之后 | `for (r.quiescing && req.Op == "create") \|\| r.frozen { if r.closed { return reply, ErrClosed }; r.cond.Wait() }`，再 `if r.closed { return reply, ErrClosed }` |
| :297 `start()` | 进 `observe` 前 `r.mu.Lock(); t.observing = true; r.mu.Unlock()`（合并进 :332-375 的临界区） |
| :442-444 `failStart` | `if t.logs[0] != nil { _ = t.logs[0].Close(); t.logs[0] = nil }` |
| :477-479 / :491-493 `observe` | 抽 `closeInputLocked(t)`：`if t.input != nil { _ = t.input.Close(); t.input, t.logs[0] = nil, nil }`——**"nil 即释放"纪律**：Export 不必询问已关闭的 actor（`Replay.Freeze` 对 closed 返回 `os.ErrClosed` :251-253，`Input.Freeze` 返回 `fmt.Errorf` :161-163，都不是"不存在"的信号） |
| :461 之后 | observe 的 frozen gate（§1.5） |
| :522-524 | 注释补"不可达论证"（Freeze 与 observe 同在 r.mu 下，Manager 冻结在 runtime 之后） |
| :667 `Close` | 先按 §1.5 的 `exported`/imported 分支处理；`closed=true` 后 `cond.Broadcast()` |
| revoke.go:59-65 | `if t.terminalAt != 0 { continue }; if !r.frozen { _ = r.cfg.Manager.Signal(...) }; killed++`；头注释整段搬运并补 (b) 臂 |
| revoke.go:104 | `if r.closed \|\| r.frozen { return 0, 0, nil }` |
| revoke.go:111 `sweepLocked` | （D11）running-abandoned 臂加在 `:114-118` 的 done 检查**之前**（§4-6） |

---

## §2 image schema v1

### 2.1 `execsession.Image`（进 `Bundle.Runtime`；字节留在匿名 FD，JSON 只含偏移/容量/身份）

| 字段 | JSON | 类型 | 来源 | Import 校验 |
|---|---|---|---|---|
| Version | `version` | int | `ImageVersion=1` | ≠1 → reject（纪元断裂） |
| Instance | `instance` | string | `r.cfg.Instance` | `validToken`；== `Registry.Instance`；`cfg.Instance!=""` 时相等 |
| FrozenAtBootNS | `frozen_at_boot_ns` | int64 | `r.frozenAt` | ≥ 每个 task 的 `last_touch`/`terminal_at`、每个 tombstone/revocation 的 `at_boot_ns`；≤ now |
| ReplayCapacity | `replay_capacity` | int64 | `r.cfg.ReplayCapacity` | **信息字段**：新任务用 `cfg.ReplayCapacity`；每份 log 自带 Capacity |
| BudgetLimit | `budget_limit,omitempty` | int64 | `r.budget.Limit`（= `Config.ReplayBudget`，今天恒 0） | 信息字段；Import 用**新二进制**的 `Config.ReplayBudget`（0=Default）校验 `used ≤ limit`，不采用它（内审 I2/R6 修正了第一版的采用） |
| BudgetUsed | `budget_used` | int64 | `r.budget.Used()` | == Σ 导出 log Capacity；≤ 生效 limit |
| Registry | `registry` | `procrequest.State` | `r.frozenRegistry`（**含 Secret**，只进 sealed memfd） | `procrequest.RestoreWithClock(state, cfg.Clock)` 全表规则（Version/Instance/BootID==当前/Secret 长度/Limit/TTL/无 starting/唯一 key/唯一 PID） |
| Tasks | `tasks` | `[]TaskImage` | `r.tasks` 按 pid 排序 | 下表 |
| Tombstones | `tombstones,omitempty` | `[]TombstoneImage` | `r.discarded` | pid 唯一且 ∉ Tasks；registry 有同 pid record 且 `Phase=="terminal"`、`Handle`/`Terminal` deep-equal；`scope` 不导出（Import 用 `scopeOf(handle)` 重建） |
| Revocations | `revocations,omitempty` | `[]RevocationImage{sid, at_boot_ns, reason}` | `r.revocations` | sid `validToken` 唯一；reason ≤4096 UTF-8 |

`TombstoneImage{PID, Handle proto.ProcessHandle, Terminal procrequest.Terminal, FinalEnd [2]uint64, FinalFIN [2]bool, AtBootNS int64}`（json tag 全 snake_case）。

`TaskImage`：

| 字段 | JSON | 说明 / 校验 |
|---|---|---|
| PID | `pid` | validToken；registry 有 `Record.PID==pid`；tasks 内唯一 |
| Handle | `handle` | deep-equal 该 record.Handle（按 handle 反查） |
| Abort | `abort,omitempty` | `[2]string`；非空 ⇒ record terminal、`Output[i]==nil`、`bare[i+1]` 存在且 `!FIN` |
| Revoked | `revoked,omitempty` | ≤4096 UTF-8；可与任何 phase 共存 |
| LostTerminal | `lost_terminal,omitempty` | ⇒ record.Phase=="running" 且无 stdin |
| TerminalAt / LastTouch | `terminal_at,omitempty` / `last_touch` | ≤ `frozen_at_boot_ns`；record terminal ⇒ `terminal_at != 0`（时钟失败例外：允许 0，Import 用 `frozen_at` 填） |
| Stdin | `stdin,omitempty` | `*StreamImage`；record running ⇒ 必须存在且 `Input!=nil`；terminal ⇒ 必须缺席 |
| Stdout / Stderr | `stdout,omitempty` / `stderr,omitempty` | `*StreamImage`；running ⇒ 存在且 `Output!=nil`；terminal ⇒ 三态之一：缺席（allocate 部分失败的空 FIN）、裸 log、带 Output |
| Files | `files` | `map[name]FileIdentity{dev,ino}`；键集合 == 由上面推导的期望集合（多/少都拒），Import 对每个已取文件 fstat 比对 |

`StreamImage{Replay workload.ReplayState; Output *workload.OutputState; Input *workload.InputState}`：有 actor 时 `Replay` 必须 == actor 快照的 `.Replay`；`Input.EOF` ⇔ `stdin-target` 缺席（input.go:69-70）；`Output.ReadError!=""` ⇒ `!FIN`；`Pending` 非空 ⇒ `ReadError!=""`（output.go:83-86 的规则在阶段①预演）；（D11）`Output.Discarding ⇒ ReadError!="" && !FIN`。

**settled record 有 task → reject**（settle 时 :273 已 retire）；running record 无 task → reject；terminal record 既无 task 也无 tombstone → reject（M4 的捕获点之一）。

### 2.2 Resource / Files 命名与指纹

每任务**一个** `handoff.Resource`：`ID = "exec-task." + pid`（ULID 合 `validName` :136-148）、`Kind = "exec-task"`、`Data = nil`（Image 是唯一真相）、`Files` ⊆ 六名：

| 名 | 对象 | 存在条件 | Import 用途 |
|---|---|---|---|
| `stdin-replay` | memfd | Stdin 存在 | `RestoreReplay` |
| `stdin-target` | pipe 写端 | `Input!=nil && !Input.EOF` | `RestoreInput` |
| `stdout-replay` / `stderr-replay` | memfd | 流存在 | `RestoreReplay` |
| `stdout-source` / `stderr-source` | pipe 读端 | `Output!=nil` | `RestoreOutput`（已结束的 FIN 流也带 source：读端到 retire 才关，不给后代提前 EPIPE） |

内核对象清单（六类，全部已 CLOEXEC、经 `handoff.Prepare` 的 `F_DUPFD_CLOEXEC` 副本过去）：replay memfd（`MFD_CLOEXEC`+`GROW|SHRINK|SEAL`，size==capacity）、stdout/stderr 读端与 stdin 写端（`os.Pipe` ⇒ `O_CLOEXEC|O_NONBLOCK`，共享 open-file-description）、child（活/僵尸，靠同 PID 父子关系）、pidfd（`RestoreProcesses` 自己重开，process_state_linux.go:99；不带）、manifest memfd（`Restore` 后 `defer Close` :320）。**指纹三层**：handoff 层 `describeFile` 逐字比对（image_linux.go:371-377）；execsession 层 `files[name]{dev,ino}`（抓"两个同容量 memfd 互换"这种 handoff 层看不见的形状）；workload 层导出的只读谓词 `CheckReplayFile`/`CheckStreamFile`（§4-3）+ `SetReadDeadline(time.Time{})==nil` 可轮询证明（这不是"清 deadline"——deadline 是 Go poller 状态，exec 后根本没有旧 deadline）。

文件数上限：`6 × len(tasks) > handoff` 的 `maxFiles`(16384) → `agentimage` 在 Prepare 前判 `image_too_large`（registry 上限 4096 任务 × 6 = 24576 可超限；否则 Prepare 在 dup 上万 FD 后才失败）。

### 2.3 拒绝表（每行 = `TestRuntimeImageImportRejectsInvalidBundle` 一个子用例）

| # | 变异 | 校验点（阶段） |
|---|---|---|
| R1 | `version=2` | ① |
| R2 | `instance` ≠ `registry.instance` / ≠ `cfg.Instance` | ① |
| R3 | `registry.boot_id` ≠ 当前 | ① `RestoreWithClock`（state.go:55） |
| R4 | tasks 内 pid 重复 / task pid 无 record / handle ≠ record.Handle | ① |
| R5 | 两个 task 同 handle（registry 重复身份） | ① `Restore` :69-71 |
| R6 | `budget_used` ≠ Σcapacity / Σ > limit / 某 capacity > 8 MiB | ① （M3 的捕获点） |
| R7 | terminal record 有 stdin / running record 缺 stdin 或 Output / `abort` 与 Output 并存 / `lost_terminal` 配 terminal record | ① |
| R8 | running/terminal record 无 task 亦无 tombstone；settled record 有 task | ① （M4 捕获点） |
| R9 | tombstone 无 record / 与 task 同 pid / Terminal 不等 | ① |
| R10 | `last_touch` > `frozen_at` / `frozen_at` > now | ① |
| R11 | `Pending` 非空但 `ReadError==""` / `Input.EOF && stdin-target 在场` / `ReadError && FIN` | ① |
| R12 | `files` 多一个名 / 少一个名 | ①（少的那个在 ② `TakeFile` 也会报） |
| R13 | 声明 `stderr-source` 但 bundle 无该文件 | ② TakeFile 失败 → Salvage 含已取文件 |
| R14 | replay 换成普通 tmp 文件 / size≠capacity / 未 seal | ② `CheckReplayFile` |
| R15 | stdout-source 与 stdout-replay 互换 / stdin-target 放进 stdout-source | ② `CheckStreamFile`（方向/FIFO）+ dev/ino |
| R16 | 两个同容量 memfd 互换 | ② dev/ino 指纹 |
| R17 | rows 里 live task 的 `StartTimeTicks+1`（PID 复用）/ 无行 / terminal task 却有行 | ① 数据交叉（rows 参数）；真 exec 下 `RestoreProcesses` :85-87 再抓一次 |
| R18 | rows 有外来行（ID ∉ tasks） | ① `ErrForeignProcess`（agentimage 映射 `foreign_process_rows`） |

每行断言：`errors.Is(err, ErrImage)`；salvage 中每个文件 `fstat` 成功且 `FD_CLOEXEC`；child `kill(pid,0)==nil` 且 **持续写 stdout 的 child 不收 SIGPIPE**（用 `yes` 形状的 child 证明读端未被关）；`Manager.Snapshot()` 逐行不变；`budget.Used()==0`；`runtime.NumGoroutine()` 不增；随后旧 runtime `Thaw` 并 drain/settle 到真实 rc（"什么都没被消费"的终极证明）。错误文本只由静态串 + pid/ID 构成，永不 `%v` 任何 State。

### 2.4 预算守恒

`usedBefore == Σ(导出 log Capacity) == img.BudgetUsed == usedAfter`；Import 只经 `RestoreReplay`（replay.go:106 `reserve`）预留一次；`Detach`/`Discard`/`Close` 经 `Replay.Close`/`Replay.Detach` 释放。探针形状：导入后 `NewReplay(&budget, limit-used)` 成功、`+1` 得 `ErrBackpressure`。

### 2.5 导入原子性（D6）

```
Import(cfg, img, rows, files):
  ① validateImage(img, rows, cfg)           // 纯数据；dry-run RestoreWithClock；R1–R12/R17/R18
  ② 按 task/stream 顺序 TakeFile 六名；每个文件只做只读谓词（CheckReplayFile / CheckStreamFile(write) / fstat==files[name]）
     任一失败 → 把已取文件按 []handoff.Resource{ID,Kind,Files} 原样返回（不 Close）
  ③ 构造：registry := RestoreWithClock；逐 task RestoreReplay→RestoreOutput/RestoreInput（**可轮询证明在这里**：os.NewFile 进
     poller 失败 / SetDeadline 报错的 regular file 在构造期暴露，内审 I7 订正：第一版把它写在 ②）；terminal 任务 close(done)；
     tombstones/revocations 重建
     失败（os 级意外 / 不可轮询文件）→ 对已构造 actor Detach（§4-2）回收进 salvage，Detach 自身被拒的错误 join 进返回值
     （不静默丢文件，内审 resources-V3）
  返回 frozen+imported runtime（t.frozen 已填、Export 可用于回滚重导），不起任何 goroutine
```
Import 全程只读 `cfg.Manager`（不调 Signal/AllowReap/Forget）——"不 reap/不 signal"是结构性的。agentimage 层的顺序（内审 I5 修订）：
`Courier.Check` + `Spawn.ImportReady` 两个**纯预检**先于任何安装；`RestoreProcesses` 由 agentimage 在 execsession.Import 成功
**之后**调用（它自身"失败无副作用" :53-56），是**最后一个内核校验步骤**；其后的 courier 安装与 spawn 判定安装只剩「空表 / 未起」
这类本进程状态前提，失败经 `Runtime.Detach` + `Manager.Discard` 回滚。

### 2.6 image schema 的 N-1 规则（写进 §22.5）

同 Version 内只增字段、`omitempty`、零值合法；`DisallowUnknownFields`；Version 不等即拒；回滚（gen N+1 导出 → gen N 导入）只在新字段为零时可读，非零即拒绝 → 保 FD 再派发 / "保留新代"（S8 裁定）。Bundle 可达结构的 `{字段, tag, 类型}` 进只增账本（§6，Q4）。

---

## §3 `internal/agentimage`

文件：`bundle.go`（Bundle/Lineage/Validate/DeferredError/常量/`String()` 摘要）、`freeze.go`（Owners/Freeze/Frozen）、`restore.go`（Target/Decode/Restore/Import/Imported/Salvage）、`courier.go`（CourierState 类型 + 纯校验）。**不 import `internal/agent`、nats.go**（layering 行，§6）。无 `_unsupported.go`：所有 OS 调用经 workload/procrequest/handoff 既有变体（`ExportProcesses`/`Prepare` 在 darwin 返回 `handoff.ErrUnsupported` → `DeferredError{platform_unsupported}`）。

### 3.1 Bundle

| 字段 | JSON | 类型 | 校验（Decode/Validate；任一失败整体拒绝） | 来源 |
|---|---|---|---|---|
| Version | `version` | int | ==1 | `BundleVersion` |
| Instance | `instance` | string | validName ≤256；== `Runtime.Instance` | Owners.Runtime |
| Lineage.Generation | `lineage.generation` | uint64 | ≥1；== `handoff.State.Generation` | Owners.Lineage |
| Lineage.BootID | `lineage.boot_id` | string | == 当前 == `Runtime.Registry.BootID` == `Processes.BootID` | `procrequest.CurrentBootID()` |
| Lineage.ProtoVersion | `lineage.proto_version` | int | == `proto.ProtoVersion`（跨纪元拒绝） | `proto.ProtoVersion` |
| Lineage.InstanceID / RoutingNID | `lineage.instance_id,omitempty` / `routing_nid,omitempty` | string | 空或 token；S3 与 env 携带值交叉核对（`Lineage.Matches`） | S3 填 |
| Lineage.ExporterRelease | `lineage.exporter_release,omitempty` | string | ≤64；只记录 | `proto.ReleaseVersion` |
| Processes | `processes` | `workload.ProcessState` | `OwnerPID==os.Getpid()`；每行 ID ∈ Runtime.Tasks（否则 `foreign_process_rows`）；live 行 ⇔ task record running | `Manager.ExportProcesses()` |
| Runtime | `runtime` | `execsession.Image` | §2 全表；JSON ≤ `MaxRuntimeBytes`(8 MiB) | `Runtime.Export()` |
| Courier | `courier` | `CourierState` | §3.5 | `Owners.Courier.ExportOutbox()` |
| SpawnSafe | `spawn_safe,omitempty` | `*spawnsafe.State` | §3.6 | `Policy.Export()` |

体积：`MaxBundleBytes = 16 MiB`（handoff 的 64 MiB 是外层）；超限 → `DeferredError{image_too_large}`（不裁剪）。`Bundle.String()` 只输出摘要（任务数/代/instance），降低误打印面；`Decode` 成功后立即 `src.State.Data = nil`（原始 JSON 含 Secret）。

### 3.2 API

```go
type BusyCheck func() (reason, detail string, busy bool)
type Owners struct {
    Runtime *execsession.Runtime
    Manager *workload.Manager
    Spawn   *spawnsafe.Policy        // nil 允许
    Courier CourierExporter          // interface{ ExportOutbox() (CourierState, error) }；nil ⇒ 空
    Busy    []BusyCheck              // D10：S3 塞 homeReadInFlight / pty_busy / …
    Lineage Lineage
}
type DeferredError struct{ Reason, Detail string; Cause error }   // Error() 含三者；errors.As 取
const (
    ReasonSpawnInFlight        = "spawn_in_flight"           // registry starting 或 spawnsafe wedged>0（Detail 区分）
    ReasonRemoteFSProbeInFlight = "remote_fs_probe_in_flight" // spawnsafe probesInFlight>0
    ReasonForeignProcessRows   = "foreign_process_rows"      // Manager 行 ∉ runtime 任务（S3 preflight 的 pty_busy/legacy_session_busy 先于它，本码是 belt）
    ReasonImageTooLarge        = "image_too_large"
    ReasonRuntimeClosed        = "runtime_closed"
    ReasonPlatformUnsupported  = "platform_unsupported"
)
func Freeze(o Owners, deadline time.Time) (*Frozen, error)
func (f *Frozen) Bundle() Bundle
func (f *Frozen) Prepare(role string, extra ...handoff.Resource) (*handoff.Prepared, error) // State{Role, Instance, Generation, Data: json(bundle)} + 每 task 一个 Resource + extra（S3/harness 钩子）
func (f *Frozen) Thaw() error      // Manager.Thaw → Runtime.Thaw
func (f *Frozen) Discard() error   // 进程内交接专用：Runtime.Discard（Manager 不动，新 owner 持有）
type Target struct {
    Runtime execsession.Config      // Instance 留空由 Image 供给；Manager 必填
    Spawn   *spawnsafe.Policy       // nil ⇒ 跳过
    Courier CourierImporter         // interface{ ImportOutbox(CourierState) error }；nil ⇒ 跳过
}
type Salvage struct{ State handoff.State; Resources []handoff.Resource }  // Close() 关全部
func Decode(state handoff.State) (Bundle, error)                          // 纯：DisallowUnknownFields + Validate
func Restore(src *handoff.Restored, tgt Target) (*Imported, *Salvage, error)
func Import(b Bundle, rows []workload.Record, files execsession.FileSource, tgt Target) (*Imported, *Salvage, error) // 进程内半
type Imported struct{ Bundle Bundle; Runtime *execsession.Runtime; Manager *workload.Manager; FrozenAt int64 }
func (i *Imported) Thaw() error                       // Manager.Thaw → Runtime.Thaw；记录 thawedAt
func (i *Imported) FreezeWindow() time.Duration       // thawedAt − Bundle.Runtime.FrozenAtBootNS（同 CLOCK_BOOTTIME 跨 exec 可比；S3 ≤500 ms 校准原语）
func (i *Imported) Detach() (*Salvage, error)         // Thaw 前：Runtime.Detach + Manager.Discard
```

### 3.3 Freeze 顺序（每步失败逆序回滚）

1. `Busy` 钩子逐个 → 任一 busy → `DeferredError`（不冻任何域）。
2. `Spawn != nil`：`Spawn.Export()` 的前置判定 `Census()`：`Wedged>0` → `spawn_in_flight`（Detail `spawnsafe: wedged=N`）；`ProbesInFlight>0` → `remote_fs_probe_in_flight`（Detail 含 pending/retained 计数）。廉价、无等待。
3. **外来行预检**（只读）：`Manager.Snapshot()` 每行 ID ∈ `Runtime.TaskPIDs()`，否则 `foreign_process_rows`（避免为一个必然回滚的 freeze 先冻 runtime、让 legacy `AdoptDraining` :251-254 撞 `ErrFrozen` 杀掉刚 spawn 的 child）。
4. `Runtime.Freeze(deadline)`：`ErrBusy → spawn_in_flight`（Detail `registry: starting`）；`ErrClosed → runtime_closed`。
5. `Manager.Freeze()`：`ErrFrozen` **视为错误**（不做"谁先冻谁负责"的隐式约定）→ 回滚 `Runtime.Thaw()`。
6. 复核外来行（`Manager.Freeze` 返回的 rows）→ 有则回滚（`Manager.Thaw()` 先，再 `Runtime.Thaw()`）。
7. 导出：`Manager.ExportProcesses()` → `Runtime.Export()` → `Spawn.Export()` → `Courier.ExportOutbox()`（D8：在 fence 之后）。
8. 组 Bundle；`6×len(tasks) > 16384` 或 JSON 超限 → `image_too_large` 回滚。

`Frozen.Thaw()`：`Manager.Thaw()` **先**（Runtime.Thaw 步骤 3 的补 kill 需要它），再 `Runtime.Thaw()`。exec 失败路径 = `prepared.Close(); frozen.Thaw()`；回滚后 `Frozen` 作废，再升级必须重新 Freeze（回滚必须重新导出当前态）。

### 3.4 Restore 顺序

1. `Decode(src.State)`（纯；含 `handoff.State.Role/Instance/Generation` 与 bundle 交叉）→ `src.State.Data=nil`。
2. courier/spawnsafe 状态纯校验（`CourierState.Validate`、`spawnsafe.State.Validate`）——把所有纯数据失败前移到取 FD 之前。
3. `rows`：`tgt.Runtime.Manager` 未用 → `rows = b.Processes.Records`；已用（进程内共享冻结表）→ 必须 `Frozen()` 且 `Snapshot()` 与 `b.Processes.Records` 逐行相等（ID/OSPID/Exit 有无/ReapHeld），`rows = Snapshot()`。
4. `execsession.Import(cfg, b.Runtime, rows, src)`（cfg.Instance 留空）→ 失败：`(nil, &Salvage{State: src.State, Resources: salvage}, err)`；**不 Close `src`**（未取文件由调用方 Close 或再派发）。
5. Manager 未用 → `Manager.RestoreProcesses(b.Processes)` → 失败：`rt.Detach()` 并入 Salvage。
6. `tgt.Spawn != nil && b.SpawnSafe != nil` → `Spawn.Import(*b.SpawnSafe)`（已纯校验，只剩"Policy 非空"这类调用方错误 → 视为致命：`rt.Detach()` + `Manager.Discard()`）。
7. `tgt.Courier != nil` → `Courier.ImportOutbox(b.Courier)`（同上）。
8. 返回 `Imported`（全部仍 frozen）。courier 投递启动与首个 register 由 S3 在 `Thaw` 之后。

### 3.5 courier

`agentimage.CourierState{Version int; Events []CourierEvent}`；`CourierEvent{PID, Kind string; Payload json.RawMessage; EnqueuedAtUnixNano, NextUnixNano int64; Fails, Silent int; Parked bool; RC int; EndedAtUnixNano, StartedAtUnixNano, StartTimeTicks int64}`（对应 `pendingProcEvent` proc_delivery.go:71-86 逐字段）。`Validate`：Version==1；len ≤ `MaxCourierEvents`(4096，测试钉 == `courierMaxPending`)；Σpayload ≤ `MaxCourierBytes`(8 MiB)；(pid,kind) 唯一；kind ∈ {started,exit}；pid token；`json.Valid(payload)`；`Parked ⇒ kind==exit`（attempt :322-338 只 park exit）；`Silent ≤ courierParkAfter`。

`internal/agent/proc_delivery.go` 只加包级函数 + 一个小适配类型（Agent 方法 125 不动；agent 代码行 6858 → ≈6960，量化 6000 不变）：
```go
type courierImage struct{ c *procCourier }                        // 实现 agentimage.CourierExporter/CourierImporter
func (courierImage) ExportOutbox() (agentimage.CourierState, error) // c.mu 下按 key 排序复制全字段（payload 原字节）
func (courierImage) ImportOutbox(st agentimage.CourierState) error  // 仅 len(c.pending)==0 且 run 未起；Validate；payload 重解码为 ProcStartedEvent/ProcExitEvent 与 PID/RC/EndedAt/StartedAt/StartTimeTicks 逐字段对账，不一致拒绝（D8）；next 重置为 now；fails/silent/parked/enqueuedAt 原值；不经 add（不 evict、不 nudge）
```
不变量（测试钉）：导出前后 `pendingStartedSnapshot()`/`pendingExitSnapshot()`/`registerReceipt()` 集合相等；同 pid started+exit 的顺序信息（`deliverRound` :245-266 靠 `parked` 判定）随 `Parked` 原样保留（P9 转 S3 的前提）。

### 3.6 spawnsafe（新文件 `internal/spawnsafe/image.go`）

```go
type State struct { Version int `json:"version"`; Gen string `json:"gen"`; Verdicts []MountVerdict `json:"verdicts"` }
type MountVerdict struct { Mountpoint, Signature string; Healthy bool; DecidedAtUnixNano int64 }
type Census struct { Wedged, ProbesPending, ProbesRetained int }   // pending = launched && stUnprobed；retained = result!=nil && state==stUnhealthy（sticky-dead 的自愈通道）
var ErrSpawnInFlight, ErrProbeInFlight error
func (p *Policy) Census() Census
func (p *Policy) Export() (State, error)   // Wedged>0 → ErrSpawnInFlight；probesInFlight>0 → ErrProbeInFlight；只导出 healthy 与 "probe 返回 false" 的 unhealthy（超时臂的 unhealthy 因 result!=nil 已被拒）
func (p *Policy) Import(st State) error    // 要求 len(p.health)==0 && wedged==0（刚 New）；Gen != 当前 → 全部跳过（返回 nil，计数进 Detail 日志）；mountpoint 在 p.mounts 且 signature 相等且 kind==kindRemoteProbe 才安装（镜像 applyMounts :478-509 的 carry-over 规则）；decidedAt 原值（TTL :690 从原点续判）、result=nil、launched=false
```
计数改动：`probesInFlight int` 字段；launcher `:711-716` 改为 `p.probesInFlight++; go func(){ ok := probe(mp); p.mu.Lock(); p.probesInFlight--; p.mu.Unlock(); ch <- ok }()`——与 map 无关，`applyMounts` 丢掉 signature 变化的条目也不会让在飞 goroutine 隐身（kernel 批评）。`wedged` 已覆盖 `RunStartWithCleanup`/`boundedTouch`/`boundedResolveInDirs` 的在飞与 abandoned（都持 slot 到返回，:1145/:381/:1035）；`wedged` **不导入**（旧 goroutine 不跨 exec、永不递减）。新写的 `switch h.state` 枚举三个成员、不写 `default`（enum_switch_default 家族 `spawnsafe.probeState`）。Export/Import 都在 `p.mu` 下且不调 `invalidateHealthy`（spawn_stall 门的锁支配规则）。

### 3.7 与 S3 的接口契约（S3 只加调用）

- bootstrap：`handoff.Restore()` → `agent.BootUpgradeCheck` → `agentimage.Decode` → `Agent.New` → `bundle.Lineage.Matches(instanceID, routingNID)` → `agentimage.Restore(src, Target{Runtime: 同 initProcessRuntime 的 Config 但 Instance 留空, Spawn: a.spawnPolicy, Courier: courierImage{a.courier}})` → `a.processRuntime = imported.Runtime` → 首个 register 前 `imported.Thaw()`。
- 失败：`Salvage` → S3 的 handoff 再派发 API 用 `Salvage.Resources ∪ src 未取资源` 重新 `Prepare` 同一 `State`、exec prev。`Import` 成功后若 bootstrap 仍失败 → `imported.Detach()` 得到同形 Salvage。
- 升级：`agentimage.Freeze(Owners{…, Busy: []BusyCheck{homeReadBusy(a)}}, deadline)` → `*DeferredError.Reason` 直接进 deferred 集合 → `Frozen.Prepare("agent")` → `Prepared.Exec(path, args, execEnv()+dispatch)`；失败 `Frozen.Thaw()`。
- schema 冻结：S3 不加字段；S4+ 加域走账本只增。

---

## §4 workload / procrequest 缺口修补清单

| # | 函数 | 改动 |
|---|---|---|
| 1 | `Output.Freeze`（output.go:172-212） | `Output` 加 `frozen bool`。拒绝条件 `p.closed \|\| p.frozen`（不再是 `p.paused`）；`if !p.paused { p.paused = true; close(p.pause) }`（Abort :263-266 已置 paused、已关 pause，二次 close 会 panic）；成功后 `p.frozen = true`。`thaw` :220-235 加 `p.frozen = false`。`RestoreOutput` :100 置 `frozen: true`。注释 :252-253 "Freeze after Abort reports the pump unavailable" **整段改写**为"an aborted stream still hands over its retained bytes and End; Freeze after Abort succeeds and restores as an ended, pumpless stream"。树外副本实跑确认今天返回 `workload: output unavailable for freeze` |
| 2 | `Replay.Detach() (*os.File, ReplayState, error)`、`Output.Detach() (source *os.File, log *Replay, ok bool)`、`Input.Detach() (target *os.File, log *Replay, ok bool)` | 要求 `frozen && !closed`（Replay）/ `paused && !running && !closed`（actor，即 Restore 出来未 Thaw 或 Freeze 后）；释放预算、置 closed、**不关文件**、返回原 `*os.File` |
| 3 | `CheckReplayFile(f, capacity) error`、`CheckStreamFile(f, write bool) error` | 导出包装 `validateReplayFile`（replay_linux.go:308-337）/ `validateStreamFile`（stdio.go:324-351）；darwin 变体返回 `handoff.ErrUnsupported` |
| 4 | `ReplayBudget.Used() int64` | 守恒断言观测点 |
| 5 | `Manager.Frozen() bool`；`Manager.Discard() error` | Discard 仅 `restored && frozen && !polling`（`RestoreProcesses` :111 置 `restored=true`，`Thaw` 清）：逐行 `proc.Release()`（`os.FindProcess` :99 在本 toolchain 开 pidfd，实测 +1 fd）、清表、`frozen=false`；不 wait、不 signal |
| 6 | （D11）`Output.Discard(reason)` + `OutputState.Discarding bool json:"discarding,omitempty"` | `if closed \|\| readErr!="" \|\| FIN → return`；`readErr=reason; discarding=true`；未 paused 时 `SetReadDeadline(now)` 唤醒。`run()` :119 的退出条件改为 `paused \|\| closed \|\| FIN \|\| (readErr!="" && !discarding)`；`discarding` 分支：`Read(buf)` 丢弃，EOF/非 deadline 错误 → `discarding=false` 退出（流已在 End 处 aborted）。`RestoreOutput`：`Discarding ⇒ ReadError!="" && !FIN`。`outputsEnded`（runtime.go:542）无需改（ReadError 已非空） |
| 7 | `procrequest.RestoreWithClock(state State, clock func()(int64,error)) (*Registry, error)` | `Restore` 委托 `nil`（state.go:61 写死 `bootNow`，否则导入后 registry 与 runtime 两套钟——`NewWithClock` :97-100 注释说要避免的） |
| 8 | `procrequest.CurrentBootID() (string, error)` | 导出包装 `currentBootID` |
| 9 | `type Secret []byte` + `String()`/`Format()` 打印 `[redacted]`；`State.Secret Secret` | JSON 仍 base64 往返（json 不看 Stringer）；`%v/%+v/%s/%x` 全部脱敏；`len(state.Secret)` :56 与 `append([]byte(nil), state.Secret...)` :60 编译不变 |
| 10 | `process_state_linux.go` | **不改**：僵尸/ReapHeld 已合法（:15-18、:31-33、:135 只对已 reap 行拒 ReapHeld；process_state_exec_test.go:110-118/246-262 已证跨 exec 的 held zombie） |

---

## §5 测试清单

### 5.1 表（名称/文件/断言/变异；全部 `//go:build linux`，与包内既有测试一致）

| 名称 | 文件 | 断言 | 变异 |
|---|---|---|---|
| `TestRuntimeImagePreservesLiveChildAcrossExec` | `internal/execsession/runtime_image_exec_test.go` | §5.2 exec harness；父进程真 `execclient.Run` 得 stdout/stderr SHA 精确、rc 23、`Executed`；receipt：OwnerPID==helper PID、child PID/start ticks/三条 pipe inode 前后相等、`StdoutAckAtFreeze==4`、`InputBaseAtFreeze<len(stdin)`、`BlockedRequests>=1`、`DescendantAliveAcrossExec`、`SpawnedAfter==0`、`PipeRefs=={1,1,1}`、`HandoffMemfd==0`、`TargetInstance=="image-original"`、原 handle 直接 resume 通过、B（leader 已退、后代持 stdout 的 ReapHeld 僵尸）rc 37 + `heldheld-tail`、C（冻结期 raw SIGKILL 成僵尸）新映像 `Exited=={C:1}`、before 侧 Exited 0 | M1（drop-stderr）、M5（导入铸新 instance → `ErrInstance`）、M7（terminal 任务也起 observe → Exited 计数 2） |
| `TestRuntimeImageOracleRejectsMissingStream` | 同上 | env `MUTATION=drop-stderr-replay`：after 半打 `IMAGE_ORACLE_REJECT`、非零退出、无 receipt、父进程 execclient 拿不到 rc（提交进树的跨 exec 负控制，replay_exec_test.go:340-352 形状） | 删 R12/R13 校验 → helper 意外成功 → 红 |
| ~~`TestRuntimeImageHelperEnvironmentIsStripped`~~（定稿修订 #6 折进上一行的 receipt；内审 P9 指出本行残留） | 同上 | 业务 child 打印 `$TETHER_TEST_RUNTIME_IMAGE_MODE`/`$_TETHER_HANDOFF` 均为空；after 半二次 `handoff.Restore()` 为 (nil,nil) | — |
| `TestRuntimeImageRestoresPartiallyAckedOutputs` | `internal/execsession/runtime_image_test.go` | 进程内（共享冻结 Manager）：A 运行中（stdout 10 B 只 ACK 4、stderr 未 ACK）+ B setup 失败（`failureStartPTYStdoutChild` 形状，`abort[1]!=""`）终态待 ack + revoked&满日志的 D（Abort 过的流）；Freeze→Export→dup 进 fake FileSource→Import→旧 Discard→新 Thaw；A 首批 `Offset==4` 字节逐一相等；B 的 stderr batch `Error==原 abort 文本`、`FIN=false`、terminal-ack 逐流 `(FIN&&ack==End)\|\|(aborted&&ack==End)` 结清；D 的 aborted 流字节可读；Exited 各恰 1 次 | M7、M9（`Output.Freeze` 恢复 paused 一刀切） |
| `TestRuntimeImageFreezeRefusesWhileStarting` | 同上 | entered/release Start（revoke_inflight_start_test.go:32-45 形状）；`Freeze(now+200ms)` 在 [200,400) ms 返回 `errors.Is(procrequest.ErrBusy)`、`Frozen()==false`、probe/resume 仍成功（无部分冻结：registry 未冻、泵未冻）、期间并发 create 阻塞（release 后返回）；`close(release)` 后 Freeze 成功且 child 未被 kill、Thaw 后 drain 得完整字节与 rc | M6（删 registry.Freeze 的 starting 检查 → start() 的 `registry.Running` :355 得 ErrFrozen → :369 SIGKILL child → 红） |
| `TestRuntimeImageExportsTaskBetweenFinishAndConclude` | 同上 | 白盒：在 `Exited` 钩子里阻塞（finish 已返回 true、conclude 未跑），此时 Freeze→Export→Import 必须成功，导入侧该任务 terminal 且可 settle | phase 改回由 done 派生 → Import 拒绝 → 红 |
| `TestRuntimeImageAccountsExitDuringFreeze` | 同上 | 两臂：`zombie`（AdoptDraining + Manager 也冻；raw `SIGTERM`；`/proc/<pid>/stat` 为 Z；冻结期 Exited 0；Export 含 live 行；Import 新 Manager `RestoreProcesses`；Thaw 后 Exited 恰 1、终态 Signal 15）；`reaped`（`Adopt` 非 draining、Manager 不冻；等行 `Exit!=nil`；导出 running task + 终态行；Import 接受；Thaw 后 Exited 恰 1）。直接 oracle：冻结期用包装 Manager 计数 `AllowReap/Snapshot/Signal` 调用 == 0、`t.input` 仍非 nil | 去 observe gate → 包装 Manager 计数 >0 / `t.input` 被 :491 关掉 → 红（不再用"Exited 变 1"这种不会发生的差分） |
| `TestRuntimeImageImportRejectsInvalidBundle` | 同上 | §2.3 R1–R18 表驱动；每行断言见 §2.3；裸 `testharness.FDCount()`/`runtime.NumGoroutine()` 比较（不调 leak helper，避开 anchor 门） | 逐行删对应校验 → 该行绿变 |
| `TestRuntimeImportErrorsNeverCarryRegistrySecret` | 同上 | 遍历 R1–R18 的 `err.Error()`、Warn 文本、`fmt.Sprintf("%+v", state)`、`fmt.Sprintf("%x", state.Secret)` 不含 hex/base64 secret；3 个 8 MiB 日志任务的 Image JSON < 64 KiB | 错误里 `%v` State / 删 `Secret.Format` → 红 |
| `TestRuntimeImagePreservesTombstonesAndRevocations` | 同上 | 注入时钟（`RestoreWithClock` 保住）；Sweep 出 tombstone E + `Revoke(sidX)`；导入后 E 的 terminal-ack 带 finalEnd/finalFIN → settled、其它 op → `ErrOutputDiscarded`；撤权前 lease create → `ErrRevoked`、撤权后 lease → 成功 | M4（Export 不写 tombstones → `missing resource owner`/R8）；漏 revocations → 旧 lease create 成功 → 红 |
| `TestRuntimeFrozenFenceBlocksUntilThaw` | 同上 | 冻结含活任务：五种 op 的 Handle goroutine 100 ms 内不返回；Thaw 后 ≤1 s 返回正常批次且 `lastTouch` 在 Thaw 后才更新；第二臂 Close → 全部 `ErrClosed`；第三臂 quiescing 期 resume 返回、create 阻塞；`Sweep()==(0,0,nil)` 且推过 grace 不造 tombstone；`Revoke` 返回 killed=1 但 child 仍活（kill -0）；Thaw（Manager 已 thaw）后 ≤2 s 死、终态 `revoked:`；Thaw 在 Manager 仍冻时返回 `workload.ErrFrozen` 且 `Frozen()` 仍 true、泵仍 paused | 去 Handle 等待 → 立即 ErrFrozen 红；frozen 下仍 Signal → "child 仍活"红；Thaw 吞 ErrFrozen → 红 |
| `TestRuntimeImageRoundTripReleasesResources` | 同上 | anchor `exerciseRuntimeImageRoundTrip`（create→freeze→export→dup→import(共享 Manager)→旧 Discard→新 Thaw→drain→settle→Close，含 1 活 1 终态 1 tombstone）；`for i := 0; i < 5; i++`；`AssertNoFDLeak(…, 0)`（memfd/pidfd 都是 fd）+ `AssertNoGoroutineLeak` 在循环外 | Discard 漏关原件 → FD 红；Import 双 reserve 由 M3 抓 |
| `TestRuntimeFreezeLatencySample` | 同上 | 8 个满背压任务：Freeze/Export/Import/Thaw 各自耗时 `t.Logf("calibration sample, NOT a budget claim")`；只断 <5 s | — |
| `TestSweepDiscardsAbandonedRunningOutput` | `internal/execsession/revoke_test.go` | （D11）64 B 容量、child 写 80 B 后 `exec sleep 30`；推时钟到 lease 到期 + 15 min → Sweep → 两流 `Discarding`、child 继续（写不再阻塞：child 随后再写 1 KiB 成功）、resume 得 64 B 前缀 + `Error` 以 `abandoned:` 开头、`FIN=false`；child 退出后 terminal、2 min 后 tombstone 释放预算；负臂：15 min 内有 touch 不动；流未满（只写 10 B）不动；导出/导入保留 `Discarding` 且 Thaw 后泵继续丢弃 | 去 stalled 条件 → 负臂 2 红；去 grace → 负臂 1 红；Discard 改成 Abort → "child 再写成功"红 |
| `TestOutputFreezeAfterAbortExportsEndedStream` | `internal/workload/output_test.go` | `Abort(reason)` 后 `Freeze` 成功无 panic，`ReadError==reason`、`FIN=false`、files 含 source+replay；`RestoreOutput`+`Thaw` 不起常驻泵（goroutine 数不变）；Freeze 对已 Freeze 未 Abort 的流仍不可用；Abort-after-Freeze 后二次 Freeze 也不可用（`frozen` 标志） | M9；漏跳过 `close(pause)` → panic 红 |
| `TestReplayOutputInputDetachKeepFilesOpen` | `internal/workload/replay_test.go` | Restore→Detach 返回原 `*os.File` 且 fstat 成功、`Used()` 回 0、二次 Close 无 panic；Detach 只在 paused&&!running 允许 | Detach 关文件 → EBADF 红 |
| `TestManagerDiscardReleasesRestoredHandlesWithoutReaping` | `internal/workload/process_state_test.go` | RestoreProcesses（同进程真 child）→ Discard：表空、`frozen=false`、pidfd 关闭（FD 回基线）、child 仍活且测试随后 `wait4` 得真实 rc；对未 restored/普通 frozen 表 → 错误且表不变 | 漏 Release → FD 红；放行普通表 → 红 |
| `TestReplayBudgetUsedTracksReserveAndRelease` | `internal/workload/replay_test.go` | New/Restore 后 `Used()==Σ`；Close 回落；reserve 失败不计 | — |
| `TestRegistryRestoreWithClockAndRedactedSecret` | `internal/procrequest/registry_test.go` | `RestoreWithClock(state, fake)` 后 Issue 的 `IssuedBootNS==fake`、Collect 按注入钟；`%v/%+v/%s/%x` 均 `[redacted]` 而 `json.Marshal` 含且往返 | 删 Format → `%x` 红 |
| `TestBundleRoundTrip` | `internal/agentimage/bundle_test.go` | 四域进程内往返（Owners{2 task runtime, 共享 Manager, Policy(假 mountinfo 一条 nfs + healthy verdict), courier 桩, Busy 空}）→ Freeze → `Bundle()` JSON 往返 → `Import(共享 Manager)` → 旧 `Frozen.Discard` → `Imported.Thaw` → drain rc 23；spawn 新 Policy 对同 mount 不再 probe（桩计数 0）；courier 桩收到同一事件集；Freeze 顺序桩记录 busy→spawn→foreign 预检→runtime→manager→export，Thaw 逆序 | 漏 SpawnSafe → 桩计数 1；漏 Courier → 集合空 |
| `TestBundleRestoreRejectsPartialBundleWithoutInstalling` | `internal/agentimage/restore_test.go` | bundle 级行：`bundle_version`/`instance_mismatch`/`lineage.boot_id`/`proto_version+1`/`generation 0`/`processes.owner_pid`/`courier.kind`/`courier payload≠字段`/`spawn_safe 未知 state`/`foreign row`/`image_too_large`/未知 JSON 键（每域各一）：Restore 返回 `(nil, Salvage, err)`；Salvage 每文件仍打开且 CLOEXEC；fresh Manager `Snapshot()` 空（未 RestoreProcesses）、Policy health 空；`handoff.Prepare(salvage.State, salvage.Resources)` 可再执行；`src` 未被 Close；child 仍活 | Manager 先安装再校验 runtime → "Snapshot 空"红；去 `DisallowUnknownFields` → 未知键行绿变 |
| `TestBudgetConservedAcrossImport` | `internal/agentimage/restore_test.go` | 3 task 不同容量含 terminal：`usedBefore == Σ == Bundle.Runtime.BudgetUsed == 导入后 ReplayBudgetUsed()`；`NewReplay(limit-used)` 成功、`+1` 得 `ErrBackpressure`；全部结清 Close 后 0 | M3 |
| `TestSpawnInFlightDefersFreeze` | `internal/agentimage/freeze_test.go` | `TryAcquireSpawnSlot` → Freeze 返回 `*DeferredError{spawn_in_flight}` 且 runtime/Manager/courier 均未冻（Handle 正常、`Manager.Frozen()==false`、courier 桩未被 Export）；Release 后成功；Probe seam 阻塞臂 → `remote_fs_probe_in_flight`；**超时后**（state 已 unhealthy、result 保留）仍 deferred；probe 返回后可导出；Busy 钩子返回 busy → 该 reason、零域冻结；runtime in-flight Start → `spawn_in_flight` Detail 含 `registry` | M11（Export 忽略 probesInFlight）；漏 Busy 遍历 → 红 |
| `TestFreezeDefersOnForeignProcessRows` | 同上 | Manager 另 Adopt 一个非 runtime child（模拟 legacy exec）→ `foreign_process_rows`；预检发生在冻结任何域之前（包装 runtime 计 Freeze 调用 == 0） | 漏预检 → 红 |
| `TestBundleFreezeRollbackReleasesResources` | 同上 | anchor `exerciseBundleFreezeRollback`：5 轮 Freeze→Prepare→`Prepared.Close`→`Frozen.Thaw`（exec 失败路径）后 drain 得完整字节与 rc；goroutine/FD/memfd 容差 0；Freeze 后 Thaw 前泵不读（管道写端阻塞可观测） | `Frozen.Thaw` 漏 `Manager.Thaw` → 第二轮 `Manager.Freeze` ErrFrozen → 红；Thaw 漏 `registry.Thaw` → drain 得 ErrFrozen → 红 |
| `TestBundleExecRestoresEveryDomain` | `internal/agentimage/bundle_exec_test.go` | 真 exec（无 NATS，courier 桩）：一个活 child + healthy verdict + 2 条 courier 事件（含 parked）；before Freeze→Prepare→Exec；after `handoff.Restore`→`Decode`→`Restore`→`FreezeWindow()>0`→`Thaw`→drain rc 23；receipt 含 pid/instance/servedAfter；helper 输出与 receipt 不含 `"secret"` 字面 | Export 漏 courier exit（M2 在 agent 包另证）；Lineage.Generation 不 +1 → 红 |
| `TestBundleRepeatedExecReleasesResources` | 同上 | 父进程基线，anchor `exerciseBundleExec`，5 轮，goroutine/FD 容差 0（`TestHandoffRepeatedExecReleasesResources` image_exec_test.go:63-74 形状；无 NATS 所以父侧基线稳） | — |
| `TestCourierStateRejectsMalformed` | `internal/agentimage/courier_test.go` | 非法 kind / 非 token pid / 非 JSON payload / 超 4096 / 重复 key / parked started / Silent>3 各拒绝；`MaxCourierEvents == courierMaxPending` 由 agent 包测试钉 | — |
| `TestCourierOutboxSurvivesImage` | `internal/agent/proc_delivery_test.go` | `courierAgent` 夹具（:20-32）无 responder：started(A)、exit(A)、exit(B, parked)、exit(C, fails=2, next=+1h)→`ExportOutbox`→JSON 往返→`ImportOutbox` 到新 courier：事件值切片（排序后 DeepEqual，next 除外）、`pendingStartedSnapshot()`/`pendingExitSnapshot()`/`registerReceipt()` 集合相等、`next≈now`；接 `startAckResponder` 后 A started 先于 A exit 送达、C 在 backoff 内不送、B 永不 | M2（Export 对 `kind==exit` `continue` → `pendingExitSnapshot` 不等 → 红）；不重置 next → 红 |
| `TestCourierImportRejectsInconsistentEntries` | 同上 | payload rc≠RC / parked started / 重复 key / 超 4096 / pending 非空时导入 → 拒绝且 pending 不变 | 去 payload 对账 → 行绿变 |
| `TestPolicyExportRefusesInFlightSpawnAndProbe` | `internal/spawnsafe/image_test.go` | probe fn 阻塞 → `Census().ProbesPending==1`、Export `ErrProbeInFlight`；超时后 `ProbesRetained==1`、仍 `ErrProbeInFlight`；`applyMounts` 换 signature 丢掉条目后计数仍 1（map 无关）；probe 返回后 0 可导出；`TryAcquireSpawnSlot` 持有 → `ErrSpawnInFlight` | M11 |
| `TestPolicyImportPreservesVerdictsAndSkipsForeignGeneration` | 同上 | healthy（旧 decidedAt）/unhealthy 两项 → Export → 新 Policy Import：healthy 过 TTL 后重探（Now 注入）、unhealthy sticky 不起探测 goroutine（NumGoroutine 不变、probe 计数 0）；signature 不等跳过；Gen 不等全部跳过且不报错；非空 Policy → 错误 | Import 把 unhealthy 装成 unprobed → 重探计数红；忽略 signature → 红 |
| `TestAgentImageEntryPointsAcceptNoContext` | `test/architecture/dataplane_lifetime_test.go` | （Q4）`internal/agentimage` 全部导出符号 + execsession 的 `Freeze/Export/Import/Thaw/Discard/Detach` 签名不含 `context.Context`；`Runtime.Close(ctx)` 登记为带理由的已知例外 | 给 Freeze 加 ctx 参数 → 红 |
| `TestImageFieldInventoryAppendOnly` | `internal/agentimage/image_inventory_test.go` | （Q4）go/types 从 `Bundle` 递归可达具名 struct（含 execsession/procrequest/workload/proto 嵌套）每行 `Pkg.Type.Field<TAB>tag<TAB>type` 与 `testdata/image_field_inventory.txt` 逐行相等；`-update-image-inventory` 只增拒删；`// gate-control: TestImageInventoryCollectorSeesNestedFields` | 真改磁盘给 `TaskImage` 改一个 tag 再还原 → 红；删账本一行 → updater 拒绝 |

### 5.2 同 PID exec harness（`internal/execsession/runtime_image_exec_test.go`，`package execsession`）

**dispatch 形状**（exit #6）：单测试 + mode switch（process_state_exec_test.go:44-77）：
```go
const runtimeImageMode = "TETHER_TEST_RUNTIME_IMAGE_MODE"   // + _REPORT / _FIXTURE / _MUTATION / _NATS_URL / _CONTROL
func TestRuntimeImagePreservesLiveChildAcrossExec(t *testing.T) {
    switch os.Getenv(runtimeImageMode) { case "before": runtimeImageBefore(t); return; case "after": runtimeImageAfter(t); return; case "": default: t.Fatalf(...) }
    // 父进程：嵌入 NATS（natstest.RunServer）+ natsinbox.InboxConnectOptions(actor) 的 ctl 连接 + execclient.Run（goroutine）
    cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestRuntimeImagePreservesLiveChildAcrossExec$", "-test.count=1")
    cmd.Env = append(runtimeImageEnvironment("before"), REPORT=…, FIXTURE=…, NATS_URL=…, MUTATION=…)
    cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}; cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
}
func runtimeImageEnvironment(mode string) []string   // handoff.CleanEnvironment(os.Environ()) 再剥六个 TETHER_TEST_RUNTIME_IMAGE_* 键、追加 mode
```
`runtimeFixtureConfig(t, capacity) (*runtimeFixture, Config)` 从 `newRuntimeFixture`（runtime_test.go:62-129）抽出，让 `New` 与 `Import` 共用；其 `Start` 适配器 `cmd.Env = runtimeImageEnvironment("")` 再剥 mode 键（业务 child 不得继承 harness 身份——双向剥离的 child 半）；`ExtraFiles=[controlR]`（fd 3 控制管道，后代靠它门控，**不用 sleep 计时**）。

**before 半**：
1. `cfg.Instance="image-original"`、fake clock、`Start` 返回 warning `"degraded-path"`（stderr 前缀经 `NewOutputWithPrefix` :347）。
2. 责任者 goroutine 订阅 `SubjCmdForwarded(sid, nid, process-resume)`：盖章 `ActorFP`、`atomic` 计数 `received`、交 `r.Handle`、回包（client_test.go:560-570 形状）。
3. 任务 A（ctl 驱动，argv 由父进程的 execclient 提交）：`sh -c 'IFS= read -r first; printf "out:%s\n" "$first"; printf "err:%s\n" "$first" >&2; ( read -r go2 <&3; printf descendant-tail ) & cat; printf "err-tail\n" >&2; exit 23'`，`Stdin = "first\n" + 192 KiB payload`（stdin log 容量 `max(64KiB,len)` :418，pipe 64 KiB ⇒ 冻结时必然 `Base<End`）。父进程的 execclient 只是普通 client；责任者观察到 A 的 `StdoutACK ≥ 4` 后（部分 ACK）才开始冻结。
4. 任务 B（责任者直接 Handle）：`sh -c 'printf held; ( read -r go3 <&3; printf held-tail ) & exit 37'`（ReapHeld 僵尸）；任务 C：`exec sleep 30`。
5. 冻结序（定稿修订 1）：**手拼**——`Runtime.Freeze(deadline)` → `Manager.Freeze()` → `Manager.ExportProcesses()` → `Runtime.Export()` → `handoff.Prepare(State{Role:"runtime-test", Instance, Generation:1, Data: json(fixture)}, resources+control)`。本文件是 `package execsession` 内部测试，**不得 import `internal/agentimage`**（import cycle）。C 在 Freeze 后 raw `syscall.Kill(SIGKILL)`，轮询 `/proc/<pid>/stat` 到 Z 再 Export。
6. 等 `received` 在 Freeze 之后再增 ≥1（ctl 的 idle poll ≤250 ms）→ `BlockedRequests` 写进 fixture；fixture 还含 handle/epoch/inbox/ACK 偏移/PID/ticks/inode/InputBase/BudgetUsed（**不含 secret**）。
7. 负控制 `MUTATION=drop-stderr-replay`：在 oracle 捕获之后删掉 A 的 `stderr-replay` 文件（replay_exec_test.go:170-183 形状）。
8. `Frozen.Prepare("runtime-test", handoff.Resource{ID:"control", Kind:"pipe", Files:{"r": controlR}})` → `Exec(os.Args[0], …, runtimeImageEnvironment("after"))` → `t.Fatal("exec returned")`。

**after 半**：
1. `handoff.Restore()`（断言二次为 (nil,nil)）；`os.Getpid()==fixture.OwnerPID`。
2. 新 `workload.Manager`、`runtimeFixtureConfig` 的 Start 改为返回错误 `"no spawn after import"`；`execsession.Import(cfg, fixture.Image, fixture.Processes.Records, src)` → `Manager.RestoreProcesses(fixture.Processes)`（定稿修订 1：不经 agentimage）；负控制模式期望 Import 返回 err → 打印 `IMAGE_ORACLE_REJECT: <err>` 后 `os.Exit(1)`。
3. **Thaw 前**：`Handle(resume)` goroutine 阻塞（100 ms 不返回）；`task.input.Snapshot().Replay.Base == fixture.InputBase`；`ReplayBudgetUsed()==fixture.BudgetUsed`；`/proc/self/fd` 每条 pipe inode 恰 1 引用（image_exec_test.go:423-444 `handoffIdentityCount` 形状）、`/memfd:tether-handoff` 0；`src.Close()` 后引用不变；`kill(desc,0)==nil`。
4. `Imported.Thaw()`（阻塞的 Handle 返回批次）→ 重连 NATS、重订阅、服务；写 `go2\n`/`go3\n` 到控制管道（**先于**等待 settle——后代持 stdout 写端，不放行 A 永不 FIN）；A settled 后直接 Handle 校验 B rc 37 + `heldheld-tail`、C 终态 Signal 9 且 `Exited=={C:1}`、tombstone terminal-ack settled、sidX 旧 lease create → `ErrRevoked`、`reply.TargetInstance=="image-original"`、原 handle 直接 resume 通过 → 全部 Close → memfd 引用 0、FD 回导入后基线减去关闭数 → 写 receipt。
5. 父进程：execclient 结果 `stdout == "out:first\n" + payload + "descendant-tail"`（顺序精确：`descendant-tail` 在 exec 之后才放行）、`stderr == "degraded-path\nerr:first\nerr-tail\n"`、rc 23、`Executed`；receipt 逐项。

### 5.3 五轮资源门

- 进程内：`exerciseRuntimeImageRoundTrip`（共享冻结 Manager，Thaw 因 `polling=true` 不起第二 reaper、无新 pidfd）；`exerciseBundleFreezeRollback`。
- exec 型：`exerciseBundleExec`（父进程基线，无 NATS）。
- 带 NATS 的 exec 测试**单次运行**只做字面收据（嵌入 server 的 accepted socket 异步回收会让父侧 FD 容差 0 flake）。

### 5.4 四条必做变异（+补充）的施加与验证法

overlay 是整个构建图的输入，被 exec 的 helper 就是 `os.Args[0]` 同一二进制，所以 before/after 两半都带变异。**自证**：每个变异副本在 `Import` 入口加 `fmt.Fprintln(os.Stderr, "IMAGE_MUTATION=<name>")`，父进程失败输出必须含它。

| # | 变异（overlay 文件） | 守卫 | 预期红 |
|---|---|---|---|
| M1 | `image.go` Export 跳过 `stderr-replay`/`stderr-source` 文件与 `Stderr` 状态 | `TestRuntimeImagePreservesLiveChildAcrossExec`、`TestBudgetConservedAcrossImport` | R6/R7/R12 拒绝 → after 半 `IMAGE_ORACLE_REJECT` → 父断言无 receipt 失败；预算守恒红 |
| M2 | `proc_delivery.go` `ExportOutbox` 对 `kind==exit` `continue` | `TestCourierOutboxSurvivesImage` | `pendingExitSnapshot` 不等 |
| M3 | `image.go` 阶段③对每个 log 再 `budget.reserve(capacity)` 一次 | `TestBudgetConservedAcrossImport`、`TestRuntimeImageRoundTripInProcess` 的 Used 断言 | `Used()` 翻倍；`NewReplay(limit-used)` 得 ErrBackpressure |
| M4 | Export 不写 `tombstones` | `TestRuntimeImagePreservesTombstonesAndRevocations` | R8 拒绝或 terminal-ack → `missing resource owner` |
| M5 | Import 用 `proc.NewPID()` 铸新 instance | exec harness | `ErrInstance` |
| M6 | 删 `registry.Freeze` 的 starting 检查 | `…FreezeRefusesWhileStarting` | child 被 SIGKILL |
| M7 | Thaw 对 terminal 任务也起 observer | `…RestoresPartiallyAckedOutputs` | Exited 计数 2 |
| M8 | 去 observe 的 frozen gate | `…AccountsExitDuringFreeze`（包装 Manager 计数） | 冻结期 Manager 调用 >0 |
| M9 | `Output.Freeze` 恢复 `p.paused` 拒绝 | `TestOutputFreezeAfterAbortExportsEndedStream` | unavailable |
| M10 | Handle 去 cond 等待 | `TestRuntimeFrozenFenceBlocksUntilThaw` | 立即 ErrFrozen |
| M11 | `spawnsafe.Export` 忽略 `probesInFlight` | `TestPolicyExportRefusesInFlightSpawnAndProbe` | 导出成功 |

---

## §6 闸门与登记

| 项 | 动作 | 边际 |
|---|---|---|
| test inventory | `go test ./test/determinism/ -run '^TestTestFunctionInventoryOnlyGrows$' -update-test-inventory`（**精确名**：`-run TestTestInventory` 只匹配 `…ExtractorRecognisesTheShapes`，不会更新 golden）；核对新增行数 == 新测试数（≈32） | — |
| 矩阵 golden | `test/e2e/all_phases_test.go:288-289` 包列表 + `"./internal/agentimage/..."`；`go test ./test/e2e/parallel/ -run '^TestMatrixUnitInventoryOnlyGrows$' -update-matrix-inventory`（+1 行）；`TestLeakRiskMatrix` 头注释加一句 agentimage 入列理由；300 s timeout 下核对总时长（-race ×1 三处 exec 测试各 +2–3 s） | ≈+8 s |
| leak anchors | `test/determinism/leak_assert_shape_test.go:140` `leakExerciseAnchors` +3：`internal/execsession/runtime_image_test.go:TestRuntimeImageRoundTripReleasesResources → exerciseRuntimeImageRoundTrip`、`internal/agentimage/freeze_test.go:TestBundleFreezeRollbackReleasesResources → exerciseBundleFreezeRollback`、`internal/agentimage/bundle_exec_test.go:TestBundleRepeatedExecReleasesResources → exerciseBundleExec`；拒绝表测试用裸计数不调 helper | — |
| layering | `layering_test.go` `layerRules` +1：`internal/agentimage` bannedTransitive `internal/agent`、`internal/broker`、`nats.go`、`nats-server`；requiredTransitive `internal/handoff`、`internal/execsession`、`internal/spawnsafe` | <1 s |
| ctx 账本 | 新站点同行 `// ctx-none:` 标注（`ctx_background_ledger_test.go` 只认同行/上一行）；账本不增 | — |
| ctx-free 小门 | `dataplane_lifetime_test.go` 加 `TestAgentImageEntryPointsAcceptNoContext`（Q4） | <1 s |
| image 字段账本 | `internal/agentimage/image_inventory_test.go` + `testdata/image_field_inventory.txt`；Makefile `gates:` 行加 `./internal/agentimage/`；CLAUDE.md §5 闸门表 +1 行（`gate_registry_test` 会核对它真在 `make gates` 里）（Q4） | ≈1 s |
| 结构预算 | execsession 639→≈1150（<2000 无条目）；agentimage ≈600；spawnsafe 742→≈880；workload 1265→≈1400；agent 6858→≈6960（量化 6000 不变）；Agent 方法 125 不动；golden 不动 | — |
| build tags | 新测试文件只用 `//go:build linux`（预声明 GOOS tag 不计入 `_integration` 门的 23） | — |
| naming | 文件按被测单元（`runtime_image_test.go`、`image_test.go`、`bundle_test.go`…），无 round/review/批次前缀；文件首注释文件名 == basename | — |
| promised-guard | 本 plan 与 §22.5 点名的每个 `TestXxx` 必须存在（docs 不扫，但 Go 注释里的 `// origin:` 与承诺要对得上） | — |
| enum switch | spawnsafe 新 `switch h.state` 枚举三成员无 default | — |
| gate-control | 新门文件带 `// gate-control: TestXxx` 正负控制 | — |
| Darwin | `GOOS=darwin GOARCH=arm64 go build ./... && GOOS=darwin GOARCH=arm64 go vet ./internal/execsession ./internal/agentimage ./internal/spawnsafe ./internal/workload ./internal/procrequest && make vet-tags` | — |
| `make gates` | 因矩阵 golden / inventory / anchor / layering / CLAUDE.md 表变更必跑（含 run-all.sh ≈2 min，总 ≈3.5 min）；不与另一个 lint 并行 | — |

---

## §7 文档清单

| 文档 | 内容 |
|---|---|
| `docs/distributed-broker-architecture.md` 新增 **§22.5 运行层 image ABI v1** | Image/TaskImage/StreamImage/Bundle 字段表（§2.1/§3.1 缩略）；Resource/Files 命名与三层指纹；fence 状态表（§1.5 原样）；Freeze/Thaw 顺序与理由（registry 先、Manager 归 agentimage、Manager 先 thaw、补 kill 响亮失败）；Handle 冻结期阻塞（D1，含 execclient 把任何 error 当终态的事实、`process_busy` 只在非计划路径可见）；导入三段式与 Salvage/Detach 再派发形状；deferred 原因码首版（`spawn_in_flight`/`remote_fs_probe_in_flight`/`foreign_process_rows`/`image_too_large`/`runtime_closed`/`platform_unsupported`）与 `Busy` 钩子；spawnsafe 拒绝规则与"D-state 线程阻塞 execve"**标为假设、S3 remote-fs 臂（hangfs）验证**；sticky-dead 挂载节点在探测线程返回前不能热更新（`--interrupting` 出口归 S8 OQ6）；秘密只在 sealed memfd、`Secret` 脱敏、错误文本纪律；image schema N-1 规则（§2.6）；已知限制（`lostTerminal+starting` 永远 ErrBusy；handoff `restoreDescriptor` 在 `validateResources` 失败时既不关也不加 CLOEXEC 导入 fd :327-329 → S3 崩溃窗定义）；`Restored` 不能枚举文件名，多余文件由 `Restored.Close` 关闭 |
| §22.4 item 7 | 补 (b) 臂已实现（Revoke 冻结期只登记、kill 推迟到 Thaw、哪个映像 thaw 就由它执行）；(c) 臂与"导入不复活"测试留 S3 |
| §22.4 item 8 | 按 Q2 裁决改写 running-abandoned 一句（Discard 形状：15 min 无 touch 且流已满 → 该流 aborted 于 End、后续输出丢弃、child 继续） |
| `docs/usage.md` §9.13.2 | 若 Q2 采纳：把"卡在写上直到上面三条之一发生"改为"无人 ACK 15 分钟后该流被截断（回来的 ctl 拿到已保留的 8 MiB 与 `abandoned:` 截断标记）、进程继续运行到自己退出" |
| `docs/testing-standards.md` 新增 §0.5「同 PID exec harness 形状」 | 精确 `-test.run=^Name$ -test.count=1`；mode env 键集专属 + `handoff.CleanEnvironment` 双向剥离（业务 child 的 `cmd.Env` 也剥并由 child 打印证明）；exec 前捕获 oracle、after 半只读 receipt；`Setpgid + Cancel` 杀组；helper 内只 `Prepared.Exec` 不二次 fork；负控制用 mutation env 改导出对象、生产变异用 overlay 且以 `IMAGE_MUTATION=` 标记自证；memfd 计数法（`/proc/self/fd` 链接名 `/memfd:tether-replay (deleted)`）；后代用控制管道门控不用 sleep；父进程嵌入 NATS + 真 execclient 作跨 exec oracle 且单次运行、五轮门另用无 NATS 的 exec 测试 |
| `docs/reviews/hot-update-evidence.md` §13 | 命令/rc/时长/HEAD/`git write-tree`；M1–M11 变异表与 `IMAGE_MUTATION` 标记行；三本清单增量数；Freeze/Thaw 延迟样本标「校准数据（本机 44 核）」；归档件 SHA 与「ABI 代际机制证明（dev gen-1）」；覆盖限制（P9/D8 转 S3；D-state 假设未验证） |
| `docs/reviews/hot-update-tasklist.md` | C-2（:29）、C-6（:33）勾选并写收据；C-3（:30）/C-7（:34）写「ABI 半 S2 已做，接线 S3」；C-10（:38）写「(b) 臂 S2 已做，(c) 臂与导入不复活测试 S3」 |
| `docs/reviews/INDEX.md` | 追加 S2 一行 |
| `CLAUDE.md` §5 闸门表 | 若 Q4 采纳 +1 行「image 字段账本」；「数据面生命周期」行补第 ④ 条（agentimage/execsession 导出接口 ctx-free） |
| `docs/deploy-tier-gotchas.md` | **不新增条目**（S2 无 drill；D-state 假设未观测，不写 gotcha） |
| `docs/reviews/hot-update-agent-image-abi-plan.md` | 主进程定稿本文 |

---

## §8 实施顺序（每步可独立 `go test`）

1. procrequest：`RestoreWithClock`、`CurrentBootID`、`Secret` 类型 + `TestRegistryRestoreWithClockAndRedactedSecret` → `go test -race ./internal/procrequest`。
2. workload：`Output.frozen` + Freeze 接纳 aborted、`Detach`×3、`CheckReplayFile/CheckStreamFile`、`Used()`、`Manager.Frozen/Discard` + 四条测试 → `go test -race ./internal/workload`。
3. execsession fence（§1.8 的 runtime.go/revoke.go 改动、`closeInputLocked`、`observing`、cond）→ 既有 4 文件全绿 `go test -race -count=2 ./internal/execsession`。
4. `image.go`：类型 + Freeze/Export/Thaw/Discard/Detach + `TestRuntimeImageFreezeRefusesWhileStarting` / `TestRuntimeFrozenFenceBlocksUntilThaw` / `TestRuntimeFreezeLatencySample`。
5. `Import` 三段式 + 拒绝表 + `…RestoresPartiallyAckedOutputs` / `…ExportsTaskBetweenFinishAndConclude` / `…AccountsExitDuringFreeze` / `…PreservesTombstonesAndRevocations` / `…RoundTripReleasesResources` / `TestRuntimeImportErrorsNeverCarryRegistrySecret` → `go test -race -count=3 ./internal/execsession`。
6. spawnsafe `image.go` + `probesInFlight` 计数 + 两条测试 → `go test -race ./internal/spawnsafe`。
7. agentimage 包（bundle/freeze/restore/courier）+ `TestBundleRoundTrip` / `TestBundleRestoreRejectsPartialBundleWithoutInstalling` / `TestBudgetConservedAcrossImport` / `TestSpawnInFlightDefersFreeze` / `TestFreezeDefersOnForeignProcessRows` / `TestBundleFreezeRollbackReleasesResources` / `TestCourierStateRejectsMalformed` → `go test -race -count=3 ./internal/agentimage`。
8. agent courier 适配 + 两条测试 → `go test -race ./internal/agent -run '^TestCourier'`。
9. exec harness：`runtime_image_exec_test.go`（三条）+ `bundle_exec_test.go`（两条）→ `go test -race -count=3 ./internal/execsession ./internal/agentimage`；`GOOS=darwin … go vet`。
10. （Q2 采纳）`Output.Discard` + Sweep running 臂 + `TestSweepDiscardsAbandonedRunningOutput`。
11. （Q4 采纳）三小门 + Makefile/CLAUDE.md 登记。
12. 登记（§6）→ `make gates; echo rc=$?`；三硬闸 `make test; echo rc=$?`、`make e2e-parallel; echo rc=$?`、`make lint; echo rc=$?`。
13. 变异 §5.4 逐条 `go test -overlay=/tmp/s2-mut/<m>.json -race -count=1 -run <守卫> <包> > /tmp/s2-mut/<m>.out 2>&1; echo "<m> rc=$?"`，核对标记行；evidence §13；docs §7。
14. 归档：`mkdir -p ~/tether-hot-update-handoff-20260920T2356Z/s2-evidence && CGO_ENABLED=0 go build -trimpath -o ~/tether-hot-update-handoff-20260920T2356Z/s2-evidence/tether-gen1 ./cmd/tether && (cd ~/tether-hot-update-handoff-20260920T2356Z/s2-evidence && sha256sum tether-gen1 > tether-gen1.sha256 && git -C /home/weiland/dist_experiment_control write-tree > tree.txt && printf 'ABI generation-mechanism proof (dev gen-1), NOT a release-grade N-1 artifact\n' > README)`。
15. `git add -A && git status --short | grep -v '^[AMDR] ' | wc -l`（期望 0）；不 commit、不 push。

---

## §9 批评处置表

| 批评（lens · 目标） | 严重度 | 处置 |
|---|---|---|
| 四份 · harness 稿 Freeze 步骤①等 `t.done` | BLOCKER | **采纳**：删除；D2 只用 `registry.Freeze` ErrBusy；M6 改为删 starting 检查 |
| 四份 · fence 稿导入失败 Close 已取 FD | BLOCKER | **采纳**：D6 三段式 + Salvage；拒绝表断言改为"文件仍打开、child 不收 SIGPIPE" |
| kernel/harness · fence 稿 `launched && stUnprobed` 漏超时探测 | BLOCKER/MAJOR | **采纳**：D9 `probesInFlight` 计数（map 无关）；sticky-dead 永远 deferred |
| fence/kernel/schema · `done` 派生 phase 的 finish→conclude 窗口 | MAJOR | **采纳**：D5；新测试 `…ExportsTaskBetweenFinishAndConclude` |
| fence/kernel/harness · kernel 稿 Thaw 顺序丢补 kill | MAJOR | **采纳**：D4；`Manager.Frozen()` 前判、all-or-nothing |
| fence/schema · harness 稿 courier 快照早于 fence | MAJOR | **采纳**：D8 |
| fence/schema · 外来行无规则 / ProcessState 内嵌 execsession.Image | MAJOR | **采纳**：ProcessState 只在 Bundle 顶层；R18 + `foreign_process_rows` + 冻结前只读预检 |
| fence/schema · kernel 稿未知 JSON 键忽略 | MAJOR | **采纳**：D7 |
| schema · Freeze ErrBusy 重试不关准入被 create 饿死 | MAJOR | **采纳**：`quiescing` + create cond.Wait |
| fence/schema/kernel · 四种 Handle 策略 | MAJOR | **裁决 D1（阻塞）**，§11 Q1 交主进程 |
| fence · harness 稿两层 oracle 没有一条同时满足四前提 + execclient | MAJOR | **采纳**：exec harness 父进程跑真 execclient，child 四前提齐全 |
| fence/kernel · Discard 不关原件 → 五轮门红 | MAJOR | **采纳**：Discard 关闭自己的原件（dup 保住内核对象） |
| kernel · Thaw ④ 起 observer 无 `defer conclude` | MAJOR | **采纳**：与 start() 同形 |
| kernel · Manager 先冻杀 in-flight spawn | MAJOR | **采纳**：D3 registry 先 |
| kernel/harness · `boundedHomeRead` 四稿看不见 | MAJOR | **采纳**：D10 `Busy` 钩子 |
| harness · 进程内往返 m1/m2 双 Manager 的 reaper/pidfd 编排 | MAJOR | **采纳**：进程内共享冻结 Manager；`RestoreProcesses` 只在真 exec |
| harness · schema 稿 Handle 阻塞的 Broadcast 漏洞 | MAJOR | **部分采纳**：保留阻塞（D1），但 Thaw/Close/Discard 无条件 Broadcast、Close 后必返回 ErrClosed 有正测试；不采纳"改回拒绝" |
| fence · 三种 running-abandoned 互斥 | MINOR | **裁决 D11**（Discard 形状），§11 Q2；kernel 的伪造 Ack 方案不进 plan |
| kernel · schema 稿 `Output.Discard` 置 readErr 让泵退出 | MINOR | **采纳**：`discarding` 分支读而丢弃 |
| fence · `TestRuntimeImageAccountsExitDuringFreeze` 第一臂不可达、变异不红 | MINOR | **采纳**：两臂（Adopt/AdoptDraining）+ 包装 Manager 计数作直接 oracle |
| kernel/harness · M8 不红 | MINOR | **采纳**：同上直接 oracle |
| fence/harness · kernel 稿 `Manager.Freeze` 容忍 ErrFrozen | MINOR | **采纳**：视为错误 |
| fence/kernel/harness · exec harness 任务 A "settled 后写 go2" 死锁 | MINOR | **采纳**：go2/go3 先于等待 settle |
| harness · kernel 稿后代 `sleep 1` 计时假设 | MINOR | **采纳**：控制管道门控 |
| harness · `-run TestTestInventory`/`TestMatrixUnitsInventory` 命令错名 | MINOR | **采纳**：§6 精确名 |
| fence/schema · ctx-free 小门会被 `Close(ctx)` 打红 | MINOR | **采纳**：白名单 + Close 例外 |
| harness · fence 稿 `stdin nil ⇔ done` 撞裸 log | MINOR | **采纳**：三态流（缺席/裸 log/actor） |
| harness · `setup_error` 死字段、`os.ErrClosed` 误述 | MINOR | **采纳**：删除；注释引用正确错误串 |
| harness · Image 无 `budget_limit` | MINOR | **采纳**：信息字段 omitempty |
| schema · `budget_limit/replay_capacity` 覆盖新配置 | MINOR | **采纳**：只作信息字段 |
| schema · courier 每条 64 KiB 上限拒绝合法大 argv | MINOR | **采纳**：总字节预算 + `image_too_large` |
| schema · TombstoneImage 内嵌无 tag 的 Scope | MINOR | **采纳**：不导出 scope |
| schema · Abort-after-Freeze 与 Freeze-after-Abort 同形 | MINOR | **采纳**：`Output.frozen` 显式标志 |
| schema · 秘密在 `Restored.State.Data`/Bundle 原始 JSON | MINOR | **采纳**：Decode 后清零、`Bundle.String()` 摘要 |
| schema · deferred 词表漂移 | MINOR | **采纳**：只用 stages-plan 词 + `foreign_process_rows`/`image_too_large`/`runtime_closed`/`platform_unsupported` 四个 S2 定义常量 |
| schema · 新测试文件须 `//go:build linux` | MINOR | **采纳** |
| schema · D-state 阻塞 execve 是未观测假设 | MAJOR(部分) | **采纳其事实认定**（不写 gotcha、标假设）；**不采纳"只对 pending defer"**——fail-closed 优先，§11 Q3 |
| kernel · `Restored` 不能枚举文件、多余文件不可拒 | MINOR | **采纳**：如实写进 §22.5 |
| kernel · `goroutine 容差 0` 与 helper 常量 2 矛盾 | MINOR | **采纳**：出口措辞改为 fd 轴 0、goroutine ±2 |
| kernel · 拒绝表测试调 leak helper 会撞 anchor 门 | MINOR | **采纳**：裸计数 |
| kernel · schema 稿 abandoned ledger 的 `-race` 数据竞争 | MINOR | **不适用**：D9 不做 ledger（wedged>0 已 defer，reapOnReturn 会 kill+Wait） |
| kernel · handoff `restoreDescriptor` 失败态不加 CLOEXEC | 记录 | 写进 §22.5 已知限制，S3 崩溃窗定义 |
| harness · stages-plan 测试名漂移 | MINOR | **采纳**：沿用 stages-plan 名；courier 测试位置调整在 evidence 记明（Q5） |
| fence · `Thaw` 非原子（补 kill 失败时泵已 thaw） | MINOR | **采纳**：D4 前判 |
| fence · agentimage 外来行检查在 Manager.Freeze 之后 | MINOR | **采纳**：冻结前只读预检 + 冻后复核 |

---

## §10 带入项裁决与残余风险

| 带入项 | 裁决 | 理由 |
|---|---|---|
| running-abandoned 8 MiB 背压封顶（S1 §9-1） | **do-in-S2（D11，Discard 形状；§11 Q2 待确认）** | 用户硬指令禁止分期；三方案里只有 Discard 与既有文案"任务继续"和 S0 aborted 合同（Error 与 End 同位）一致；kill 是用户可见倒退，drop-oldest 伪造 Ack 让回来的 ctl 永远 `process_conflict`；`last_touch`/`Discarding` 随 image 导出所以 grace 与丢弃状态跨代连续；实现在最后一步、可独立 go test |
| `finish` 遇 ErrFrozen 的冻结期 exit 记账（S1 review E5 冻结半） | **do-in-S2** | observe 整段 gate，Freeze 与 observe 同在 `r.mu` 下、registry 先于 Manager 冻结 ⇒ finish 结构上看不到 ErrFrozen；exit 以终态行或僵尸随 ProcessState 导出、新映像 observe 记账恰一次；`:522-524` 保留为 belt；`TestRuntimeImageAccountsExitDuringFreeze` 两臂 + exec harness 任务 C |
| courier「exit 先于 started 落地」P9 | **defer-S3** | 缺口在 `onRegisterSuccess`（proc_delivery.go:415-457）与 `localProcessSnapshot` 的 register 重放规则，是 h1 行为；S3 的"首个 register 前恢复 courier"与 `TestRegisterReceiptSettlesOnlyCarriedEvents` 正好重写这条规则。S2 只保证 outbox 全字段与 per-pid 顺序（`Parked`）无损导出导入 |
| tombstone/revocations 随 image 导出（S1 §9-11） | **do-in-S2** | Image `tombstones[]/revocations[]`；R8/R9 校验；`handleDiscarded` 与 create 的撤权前 lease 拒绝导入后原样成立；(b) 臂已做，(c) 臂与"导入不复活"留 S3 |
| S1 review D8（probe 撕裂行 `torn` 标记，deploy 侧） | **defer-S3** | 改的是 `hot_update_process_probe.py` 与 drill 34/35 oracle；S2 无 drill、不动五份 simcluster 基线；S3 重钉 34 正向 oracle 时一并加 |
| （新）D-state 探测线程与 execve | **do-in-S2 机制 / 假设转 S3 验证** | S2 交付 `probesInFlight` 计数与 fail-closed 拒绝；"de_thread 等 D-state 线程"是假设（现代 nfs/cifs/fuse 多为 TASK_KILLABLE），S3 remote-fs 臂（hangfs）实测后决定是否放宽为只对 pending defer |

**残余风险**：
1. Handle 阻塞依赖调用方在 Freeze 后**必然** Thaw/Exec/Close/Discard；一个 Freeze 后什么都不做的调用方会让 Handle goroutine 挂到进程结束——S3 的 upgrade 路径结构上不会，测试由泄漏门守。
2. `lostTerminal && Phase=="starting"` 让节点永久 deferred（Warn 点名 pid）；是"我们的 bug"态，S8 的 `--interrupting` 是唯一出口。
3. sticky-dead 挂载的节点在探测线程返回前不能热更新（fail-closed 的代价，现网 timan107 类机器需 `--interrupting`）。
4. Manager 是共享表：S3 若不先停收 legacy exec/PTY 再 Freeze，冻结窗内到达的 legacy `AdoptDraining` 撞 ErrFrozen 被杀（exec.go:251-254）；S2 只能预检 defer。
5. Import 阶段③失败经 Detach 回收——只剩 os 级意外；handoff 层自身失败（sealed memfd 非法、`validateResources` 失败不加 CLOEXEC）是 S3 崩溃窗定义。
6. exec harness 与 -race ×3：三处 exec 测试各 +2–3 s；满 worker 下 NATS 重连时序用 `WaitFor` 不用 sleep；定位用 `make e2e-one T=TestLeakRiskMatrix`。
7. Bundle JSON 含 registry Secret：只进 sealed memfd；S3 任何 `Logger.*(bundle)` 都是泄密，靠 `Bundle.String()` 摘要 + `Secret.Format` + `TestRuntimeImportErrorsNeverCarryRegistrySecret` 降低面，无完整机械门。
8. courier 在 Freeze→Exec 间仍投递：已送达被删的条目新映像重发一次（broker 幂等，audit 可能双记），与 §22.4-2⑨ 同口径，写进 §22.5。
9. 进程内往返把同一 open-file-description 交给两组 actor（旧 frozen + 新），只靠 Discard 的"不做 I/O"保证；已导出的旧 runtime **允许 Thaw**（那是 exec 失败的回滚 `Frozen.Thaw`），所以「导入成功后只 Discard」是调用纪律不是 API 拒绝（内审 F6/P2/V2 订正）。
10. `Output.Discard` 改变用户可见语义（15 min 后截断），需 usage 改文案；若 Q2 不采纳，`usage §9.13.2` 现文案中"直到它自己结束"对一个阻塞在写上的 child 是不可能的，应至少改成如实描述。

---

## §11 主进程裁定（定稿：Q1 D1 / Q2 D11 / Q3 fail-closed / Q4 三门全落 / Q5 接受——推荐列全部采纳，理由如表）

| # | 问题 | 推荐 |
|---|---|---|
| Q1 | 冻结窗内 `Handle` 采 **D1 阻塞**（quiescing 只阻 create、frozen 阻全部；Thaw/Close/Discard Broadcast；`process_busy` 只在非计划路径可见；不改 execclient/cmd）；还是改 execclient 对 `process_busy` 做同 CallID 有界重试并把它加进 cmd/tether 退出类表（pre-create 70→75，触碰 `error_code_coverage` 门与 CLI 表面）？ | **D1**。零 wire、零 client、零 CLI 改动即满足出口 #2 的真 execclient oracle；NATS 每条 process-resume 已在独立 goroutine（exec.go:65）阻塞无 head-of-line；client 的 500 ms 重发天然跨 exec。client 侧重试作为 belt 记入 S3 候选（若 S3 适配层还有别的 ErrBusy 来源再做）。 |
| Q2 | running-abandoned 是否在 S2 做成 **Discard 形状**（15 min 无 touch 且流已满 → 该流 aborted 于 End、泵读而丢弃、child 继续、`OutputState.Discarding` 进 image）并改 usage §9.13.2？ | **做**。用户硬指令不许分期；三方案里唯一与"放弃 = 任务继续"文案和 S0 aborted 合同一致的；改动自包含（workload ≈40 行 + Sweep 臂 ≈25 行 + 1 字段），放在实施顺序最后一步。 |
| Q3 | spawnsafe 拒绝 Freeze 的谓词是否 **fail-closed 到任何在飞探测 goroutine**（`probesInFlight>0`，含超时后 sticky-dead 保留的那一个 → 该节点永久 deferred 直到探测返回或 daemon 重启），并把"D-state 线程阻塞 execve"记为假设、由 S3 hangfs 臂验证后再考虑放宽为只对 pending defer？ | **fail-closed**。INV-1 是唯一的"已退出"证明；放宽的收益是让一台已知有死挂载的机器热更新，代价是一次可能整机挂死的 execve；S8 的 `--interrupting` 是显式出口。 |
| Q4 | 是否一并落三条小门：`internal/agentimage` layering 行；`TestAgentImageEntryPointsAcceptNoContext`（白名单 + `Close(ctx)` 例外）；Bundle 可达字段的 append-only 账本（`image_inventory_test.go` + testdata + Makefile gates + CLAUDE.md 表 +1 行）？合计 <3 s，但账本是新闸门（需 gate-control 与变异收据）。 | **三条都落**。layering 与 ctx-free 各 <1 s、把两句话变成机械形式；字段账本是 plan §3.6"manager 状态 ABI N-1 矩阵"那一行的机械半，S4/S5/S7 每个都要往 schema 加域，没有它回滚可读性只靠人读。 |
| Q5 | `TestCourierOutboxSurvivesImage` 放 `internal/agent`（stages-plan 写在 agentimage；但 `procCourier` 未导出且 agentimage 不得 import agent），agentimage 只保 `TestCourierStateRejectsMalformed`；exec harness 与真 execclient oracle 放 `internal/execsession/runtime_image_exec_test.go`（execsession 测试 import execclient + nats-server test；execclient 不依赖 execsession，无环）——接受这两处放置并在 evidence 记明？ | **接受**。layering 是硬约束；execsession 位置是 stages-plan 点名的文件，且只有 Handle 层能读到 PID/ticks/inode/InputBase/ACK 偏移这些细粒度收据，父进程的真 execclient 提供字面出口。 |