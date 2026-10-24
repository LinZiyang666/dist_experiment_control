# 热更新 S3 · agent-bootstrap（agent bootstrap 与 node upgrade 接线）实现 plan

> 定稿：2026-09-21，主进程。来源：workflow `hot-update-s3-agent-bootstrap-plan`（4 路草拟 boot / session / deploy / drill → 4 路交叉批评 → 1 路综合；原始产物在 `~/tether-hot-update-handoff-20260920T2356Z/s3-plan/`）。
> 上位文档：`docs/reviews/hot-update-stages-plan.md` §1 S3 一节、§3 贯穿纪律；S2 契约 `docs/distributed-broker-architecture.md` §22.5 + [hot-update-agent-image-abi-plan.md](hot-update-agent-image-abi-plan.md) §3.7。
> 基线：S2 候选树 `e6815c90229c115c116b2c51788cb012e3a1534a`（index，未 commit）。

## 定稿裁决（§14 五问 + 主进程修订）

| # | 裁决 | 理由 |
|---|---|---|
| Q1 | **拒绝**：`node upgrade` 到无 `image-abi` 行的候选（v0.6.0）且本节点带活任务/tombstone/撤权/courier 事件 → `image_abi_unsupported`(64)，磁盘零变更；空映像才走裸 exec | 不拒绝 = S3 亲手把 registry HMAC secret 的 memfd 与非 CLOEXEC 的业务 pipe 交给一个不认 handoff 的旧映像及其每个 child，比 H0 更差；违反「本版彻底修复」 |
| Q2 | **继续**：`handoff.Restore()` 自身失败（W1）关掉外来 fd 后作为普通启动继续（空表 register、marker `resource_phase=lost`、日志签名） | 与 S12「NAT 后唯一活进程绝不 exit」同向；崩溃窗如实定义并进 S8 注入矩阵 |
| Q3 | **保活**：ReExecOnly 腿 exec 失败 → Thaw 后保活并 Error 一行；`upgrade_state_test.go` 的 `os.Exit(1)` 契约测试改写；`cluster-runbook.md` 一句 | systemd control-group 停止会杀光刚 Thaw 的活业务；cluster upgrade 驱动器本就靠运行 SHA 复核判该腿 |
| Q4 | **body-等价系统单元**：34.U 用 `agentUserUnitBody` 逐字渲染 + `User=sim` 作系统单元；README 如实标注不证 `systemctl --user` | 真 user manager 需改 sim 镜像（dbus/logind/linger），归 S8 |
| Q5 | **加 gate**：冻结窗内 legacy exec/run/push/pull/expose 得 `agent_upgrading`(75)；proxy 指令静默跳过由 broker 重推 | gate 的 inflight 计数同时是 `legacy_session_busy`/`transfers_in_flight` 的数据源，关掉「检查过了才新建」的 TOCTOU |
| 修订 1 | `tether version` 的 `image-abi v<N>` 行**必须是最后一行**（append-only）；`N = proto.ImageABIVersion`，`agentimage.BundleVersion = proto.ImageABIVersion`；旧 `smokeVersion` 只读首行，兼容 | 确定性 lint 的版本字面量 SSOT；N-1 的 smoke gate 不变 |
| 修订 2 | 心跳前置复用 `heartbeatLoop` 的 payload 构造（抽成包级 `heartbeatPayload(a, nc)`），不另造第二份心跳格式 | 一份 wire 格式一个构造点 |
| 修订 3 | T16 真二进制 e2e 用 `--upgrade-url-allow http://127.0.0.1:` 前缀 + httptest 明文（`test/p10/upgrade_e2e_test.go` 同形）；gen2 = 同源码 `-ldflags` 变体或尾部附加字节的副本（SHA 必须不同、`version` 输出含 `image-abi` 行） | 不引入 TLS 供给；Mandate 只约束 deploy tier |
| 修订 4 | drill 实跑序列（§8.3，≈30–35 min）按用户 2026-09-21 裁定「不准停、不准延后（含 drill 实跑）」直接执行，成本已在本节预告；与 go test/lint/gates 严格串行 | 记忆条目 feedback-no-stopping-drive-all-stages |
| 修订 5 | `workload.DefaultReplayCapacity/Budget` 改为引用 `upgradebudget` 的常量（值不变）；`upgradebudget` 只 import `time`，layering 加「禁 import 任何 internal」一行 | 单源；不引入环 |
| 修订 6 | `Frozen.Prepare` 重 Export（P5）只刷新 Runtime 域与 resources，courier 域**不**重导（它在 Freeze 里最后导出、之后仍在投递、exec 后由新映像重投一次，§22.4 ⑨ 幂等）；重 Export 后必须重新 `Validate` + 大小检查 | 与 S2 §22.5-7 顺序论证一致 |
| 修订 7 | 本 stage **不改** `.golangci.yml`、不放宽任何 golden；`main-noncli` 若跨 1300 手改并写理由；`internal/agent` 文件数 ≤19 | §3 贯穿纪律 1 |

其余按综合稿执行；综合稿中「（Qn）」处按上表读。

---

# S3 · agent-bootstrap 实现 plan（综合定稿候选）

> 综合四份草案（boot / session / deploy / drill）与四份批评。行号均在当前候选树（S2 已暂存）上复核；缩写：**held** = `handoff.Restore()` 成功、文件一个都没取；**imported** = `agentimage.Restore` 成功、仍冻结；**active** = 已 Thaw；**ready** = 首个 register 成功；**lost** = 崩溃窗。

## §0 目标与出口（6 条 + 机械判定）

| # | 出口（stages-plan S3 原文） | 机械判定 |
|---|---|---|
| 1 | drill 34 GREEN：原 child PID/boot/startticks/nonce 不变、stdout/stderr 序号完整、ps RUNNING、原 ctl rc 23、agent 同 PID 换新 SHA + committed marker；`expected-verdicts` PRODUCT-RED→GREEN，红收据保留成对 | `cd test/simcluster && ./run-drills.sh -j4 --no-attribute 34-hot-update-process` 七臂全 GREEN；`expected-verdicts.tsv` 34 父行 DERIVED GREEN（`validate-verdicts.sh` 规则 ④ PARENT-JOIN :236/:300）；`expected-verdicts-log.md §34` 保留 S1 段落 verbatim + S3 GREEN 段 |
| 2 | 34 新臂全绿；tether-prev 负控制臂 PRODUCT-RED；导入失败臂 | **措辞按 §8.0 裁决改写**：臂 = A/G/U/N/P/W/F 全 GREEN；负控制独立成 `39-hot-update-legacy-n1`（Y 臂 exporter=v0.6.0 → PRODUCT-RED；X 臂 target=v0.6.0 → `image_abi_unsupported` 拒绝 GREEN）；导入失败回滚 = hermetic 真二进制两条 + W 臂（再派发 API 真实路径）；34 里**不登记** not_covered（GREEN-NCGAP :76 会红） |
| 3 | drill 33 判词不劣化；B 臂 120s watchdog 有 bundle 时行为明确 | `./local.sh drill 33-node-upgrade-success` = INCOMPLETE nc_gap=1（#73）；B4 合取（`register deadline exceeded` 短语 upgrade_state.go:668、`.prev` consumed、exe==OLD、MainPID 不变）仍成立；agent slog 多出 `image cut`/`image re-dispatched`/`image activated` 各一行 |
| 4 | hermetic 9 条命名测试 | `go test -race -count=3 ./internal/agent ./internal/agentboot ./internal/agentimage ./internal/handoff ./internal/upgradebudget`（§7 表；每条带变异收据） |
| 5 | freeze→新泵可用 ≤500ms 有测量样本 | `agent: image activated … freeze_window_ms=N shim_ms= import_ms=` 日志行（hermetic ×2 + drill 34.A/G/U/N ≥4 样本）写进 evidence §14，标「校准数据」；**不设断言** |
| 6 | 三硬闸 + `make gates`；Darwin；agent 代码行 <8000、方法 ≤126 | `make test; echo rc=$?` / `make e2e-parallel; echo rc=$?` / `make lint; echo rc=$?` / `make gates; echo rc=$?`（不用管道）；`GOOS=darwin GOARCH=arm64 go build ./... && make vet-tags`；`go test ./test/architecture/ -run StructuralBudget`（internal/agent 今天 6959 行/17 文件 → 预算 ≈7600 行/19 文件，golden `pkg-code-lines internal/agent 6000` 与 `type-methods internal/agent.Agent 125` 不动） |

## §1 启动序与回退状态表

### 1.1 新包与落点（结构预算）

| 文件 | 内容 | 估计代码行 |
|---|---|---|
| `internal/upgradebudget/budget.go` + `budget_test.go` | §6 常量表；只 import `time` | 45 |
| `internal/agentboot/image.go` | 进程级映像持有者 `Image`（phase 机、`Begin/Claim/Restore/Activate/Repack/Abandon/MarkReady`、lineage 快照、时间戳） | 240 |
| `internal/agentboot/crashwindow_linux.go` / `_other.go` | `closeForeignDescriptors()`（/proc/self/fd 扫描，关掉 fd≥3 且无 FD_CLOEXEC 的）；darwin 返回 0 | 50 |
| `internal/agentboot/image_test.go`、`boot_exec_test.go` | §7 T1/T2/T13 三角色真二进制 harness | ~600（测试不计） |
| `internal/handoff/handoff.go` + `image_linux.go` | additive：`(*Restored).Borrow()`、`(*Restored).Repack()`、`MergeResources()` | 60 |
| `internal/agentimage/restore.go` + `freeze.go` + `bundle.go` | additive：`Salvage.Prepare` 按 ID 合并；`Frozen.Prepare` 前**重新 Export**（撤权/tombstone 刷新）；5 个 Reason 常量 + `DeferredReasons` 集合（11 个） | 40 |
| `internal/agent/bootstrap.go`（新，包级函数） | `BootUpgradeCheck` 扩展、`FinishBoot`、`restoreImage`、`activateImage`、`runFailedBeforeControlReady`、`stampMarkerPhase`、`redispatchEnv` | 200 |
| `internal/agent/upgrade_image.go`（新，包级函数） | `upgradeGate`、`preflightDeferrals`、`busyChecks`、`imageOwners`、`imageCut{freeze,prepare,abandon}`、`execImageOrRecover`、`publishHeartbeatOnce`、`candidateABI` | 320 |
| 修改：`agent.go`(+40) `upgrade.go`(+60/−15) `upgrade_state.go`(+70) `exec.go`(+20) `run.go`(+6) `transfer.go`(+9) `expose.go`(+4) `proxy.go`(+4) `proc_delivery.go`(+10) `instance.go`(+12) | 各节 | ≈+230 |
| `cmd/tether/main.go` | +3 行调用（`agentboot.Begin` 顶部无条件；`agent.FinishBoot(err)`；version 第 4 行） | 3 |
| `cmd/tether/error_hints.go` / `node.go` | 3 个新码（表项、hint×2、transient/config 集合各一行）；**先量 main-noncli**（raw 1273，量子 1200，余 27 行；超 1300 手改 golden 写理由） | ≈12 |
| `internal/proto/version.go` | `ImageABIVersion = 1`、`ImageABILine()`；`agentimage.BundleVersion = proto.ImageABIVersion` | 6 |

`internal/agent`：6959 + ≈750 ≈ 7700 < 8000（余 300 行给内审注释）；文件 17→19（阈值 >20；**S4/S5 必须在既有文件或子包里长**，plan 明写）。Agent 方法 125 不动（新增全是字段与包级函数）。`internal/agentboot` **不 import** `internal/agent`/nats.go/broker（layering 加一行）；`internal/agent` import `agentboot`。

### 1.2 顺序（一屏）

```
main()
 ├─ agentboot.Begin(stderrLogger)             ← 无条件、第一条语句：handoff.Restore()（无 env=nil，一次 LookupEnv）；
 │      快照 TETHER_INSTANCE_ID/TETHER_ROUTING_NID（消费前）；env 在场但 !isAgentDaemonInvocation → §1.5 W1b
 ├─ if daemon: agent.BootUpgradeCheck(logger)  ← 改：bootRollback 分支经 img.Repack() 再派发（同一 bundle）
 ├─ Cobra → RunE → agent.New(cfg)
 │     ├─ mintInstanceID 之后立刻 setExecLineage(instanceID, restoredRoutingNID)   ← BLOCKER 修复（§3.1）
 │     └─ 末步：img := cfg.BootImage ?? agentboot.Claim(); img!=nil → restoreImage(a, img)（失败即 New 失败，不回落）
 │           else initProcessRuntime(a)
 │     └─ a.Run(ctx) 第一条语句 activateImage(a)（Thaw → marker resource_phase=active）在任何 defer / courier.run / 网络之前
 │           首个 register 成功 → commitUpgradeAfterRegister → img.MarkReady()
 └─ agent.FinishBoot(err)                       ← ExecuteContext 返回后唯一收口：held/imported 且所有权成立 → 再派发 prev；否则 Abandon
```

### 1.3 `agentboot.Image`（API 签名）

```go
package agentboot // imports: handoff, agentimage, execsession, spawnsafe, workload, procrequest, log/slog
type Phase string // "" | held | refused | imported | active | ready | lost（字符串常量，不入 iota 家族）
type Lineage struct{ InstanceID, RoutingNID string } // Begin 在任何消费前读到的 env 快照
type Image struct { mu sync.Mutex; phase Phase; restored *handoff.Restored; bundleCut *agentimage.Bundle // FromCut 路径
    files execsession.FileSource; rows []workload.Record; resources []handoff.Resource
    imported *agentimage.Imported; salvage *agentimage.Salvage; generation uint64
    env Lineage; beganAt, shimDoneAt, restoredAt, activatedAt int64 /* BootNow */ ; summary string }
func Begin(logger *slog.Logger) *Image           // 见 §1.5；成功 → 包级 current，phase=held，Summary()="role/instance/gen/resources=N"（永不含 Data）
func Claim() *Image                              // 一次性交出 current（第二次返回 nil）
func FromCut(b agentimage.Bundle, rows []workload.Record, files execsession.FileSource, res []handoff.Resource) *Image // 进程内/测试
func (i *Image) Env() Lineage                    // 再派发 env 的权威来源
func (i *Image) Restore(tgt agentimage.Target) (*agentimage.Imported, error) // held→imported；失败 → phase=refused，salvage 保留
func (i *Image) Activate() (window, shim, imp time.Duration, err error)     // imported→active（Manager 先、Runtime 后 = Imported.Thaw）；失败 → Detach → refused
func (i *Image) Repack() (*handoff.Prepared, error) // held: restored.Repack()；refused/imported(Detach 后): salvage.Prepare(restored.Borrow()...)；FromCut: handoff.Prepare(state, resources)
func (i *Image) Abandon(logger, reason string)   // phase=lost：salvage 文件 Close、restored.Close() → child EPIPE；日志只带 summary
func (i *Image) MarkReady(); func (i *Image) Phase() Phase; func (i *Image) Generation() uint64
```

`Restore` 先 `agentimage.Decode(state)` 纯校验（不取文件），再 `agentimage.Restore(restored, tgt)`（`restore.go:141-146` 会把 `src.State.Data` 置 nil——因此 **refused 态的 Repack 必须走 salvage**，绝不走 `restored.Repack()`；`salvage.Prepare` 遇 Decode 失败用 raw 字节）。

### 1.4 再派发 API（S2 契约的 additive 补丁）

| 补丁 | 签名 / 语义 | 测试 |
|---|---|---|
| handoff P1 | `func (r *Restored) Borrow() []Resource`：把**仍未 TakeFile** 的文件按 ID 分组返回，`Files` 指向 Restored 仍持有的 `*os.File`，**不转移所有权**（`Prepare` 只 dup，image_linux.go:137）；Close 后返回 nil | `TestRestoredBorrowRepacksUntakenFiles`（三跳真 exec + exec 失败后同一 Restored 仍可 TakeFile） |
| handoff P2 | `func (r *Restored) Repack() (*Prepared, error) = Prepare(r.State, r.Borrow())`；`image_unsupported.go` 返回 `ErrUnsupported` | 同上 |
| handoff P3 | `func MergeResources(groups ...[]Resource) ([]Resource, error)`：按 ID 并集，Kind 必须一致，同 ID 同名文件出现两次 = 双 owner → error | `TestMergeResourcesRefusesDoubleOwner` |
| agentimage P4 | `Salvage.Prepare(extra...)` 内部 `MergeResources(s.Resources, extra)` 后再 `handoff.Prepare`（今天 append 会撞 image_linux.go:85 重复 ID 拒绝；`execsession.Import` 中途失败留下同 ID 半取） | `TestSalvagePrepareMergesRestoredRemainder` |
| agentimage P5 | `Frozen.Prepare` 在 encode 前**重新** `o.Runtime.Export()` 刷新 `f.bundle.Runtime` 与 `f.resources`（Export 对已冻结 runtime 可重复调用，image.go:133-136 明文）——Freeze 返回后、exec 前落地的 `Revoke`（fence 下只改内存 revoke.go:59-84）才能进 bundle | `TestFrozenPrepareCarriesRevocationsMarkedAfterFreeze`（Freeze→Revoke→Prepare→解码断言 Revocations 含之；变异：去掉重 Export → 红） |
| agentimage P6 | `bundle.go` 常量块追加 `ReasonPTYBusy/ReasonFlowBusy/ReasonSSBusy/ReasonTransfersInFlight/ReasonLegacySessionBusy` + `var DeferredReasons = [11]string` | `TestDeferredReasonSetIsTheDocumentedOne`（长度 11、无重复、与 §22.6 表同序） |
| agentimage P7 | `Owners.Lineage.InstanceID/RoutingNID` 由 S3 填（无 schema 变更；字段已在账本） | §7 T9 |

零 schema/字段账本变更（`image_field_inventory` 应无 diff）。

### 1.5 崩溃窗定义（unknown/error；S8 注入点）

| 窗 | 触发 | S3 行为 | 日志签名 | S8 注入点 |
|---|---|---|---|---|
| **W1** | `handoff.Restore()` 出错（sealed 非法/nonce/PID/UID/版本/`validateResources` 失败——此刻 `image_linux.go:327` 之后的导入 fd **既未关也未加 CLOEXEC**） | `closeForeignDescriptors()`（关 fd≥3 且非 CLOEXEC 者：此刻进程无 goroutine/无 child，Go 自有 fd 全 CLOEXEC，非 CLOEXEC 的只可能是 `Prepared.Exec` 清位遗留；原 child 立刻 EPIPE、secret memfd 不再被任何 child 继承）→ 记 `crashWindow="handoff_restore"` → **继续作为普通启动**（Q2）→ New 后 `stampMarkerPhase(lost)` | `agent: dispatched image unreadable — tasks of the previous image are lost crash_window=handoff_restore closed_fds=N` | env 指向非 sealed memfd / nonce 错 / manifest 尾部多字节 / fd 指纹篡改 |
| **W1b** | env 在场但 `!isAgentDaemonInvocation`（argv 分类器跨版本漂移） | `Abandon("dispatched image reached a non-daemon invocation")` + 签名 → 命令照常执行 | `agent: image abandoned crash_window=non_daemon_invocation` | `Prepared.Exec(tether, ["tether","version"], env)` |
| **W2** | 无再派发目标：ReExecOnly 腿无 marker、prev 槽已消耗、所有权证明失败、`Repack` 失败、prev 自身导入失败（第二跳） | `Abandon`（关 salvage/restored → child EPIPE）→ main 以非零退出（supervisor 拉起干净 prev；nohup 下节点停机 = §21.3 已知 GAP 同险）。**代数上界 2 跳**：prev 槽单一被 rename 消耗（executeRollback :345）、marker 离开 pending、proof 只在 continue_pending 铸造 | `agent: image abandoned crash_window=bootstrap_no_rollback_target` | prev 用 prevstub 拒绝导入 |
| **W3** | Thaw 之后、control-ready 之前：register **权威拒绝**（default 臂 :1712）| 先尝试再派发当前态（§3.5）；exec 失败 → Thaw → 今天的退出（`defer stopProcessRuntime` Close 杀掉携带任务——**如实写进 §22.6**）；`session_not_found*` → 撤权不回滚；`ctx.Err()` → 不回滚 | `agent: run failed before control-ready; re-dispatching current state` | 假 broker 回 proto_mismatch |
| **W4** | Restore 与 Thaw 之间 panic | 内核关 fd → EPIPE（H0） | Go panic 进 boot.err 流 | — |

### 1.6 状态表（boot shim × bundle × marker × watchdog）

| # | bundle | marker / 决议 | watchdog | 动作 | 去向 | 关键日志 | 不变量 |
|---|---|---|---|---|---|---|---|
| 1 | none | 任意 | 今天 | 今天全部行为 | New 新 mint | 无新行 | `resource_phase` 不写 |
| 2 | held | none/终态（proceed：ReExecOnly 腿、再派发到达的 prev、install 失败残留） | 不武装 | New→Restore→Run→Thaw | ready | `bootstrap holds a dispatched image`、`image restored`、`image activated` | 首个 register 前必已 Thaw |
| 3 | held | pending ∧ self==new ∧ 预算内 | Run 里武装 | boot_count++（两次 fsync，在冻结窗内，计入 `shim_ms`）→ proof；同 #2 | ready | +`booting a staged upgrade` | 只有本进程持 proof |
| 4 | held | pending ∧ 预算/期限耗尽 | 未武装 | `img.Repack()` → `executeRollback(reexec=true, redispatchExec(prepared))`；exec 失败 → `prepared.Close()`、映像仍 held（Borrow 只借用）→ 继续以新映像启动并导入（S12 同一路径，upgrade_state.go:365-377） | prev（同一 bundle） | `boot: budget/deadline exhausted`（一字不改）+ `image re-dispatched` | prev 收到与新映像相同 State/nonce 新 manifest、同 dev/ino |
| 5 | held | pending ∧ self==prev（converge） | — | 写 rolled_back，继续 #2 | ready | `converged an interrupted rollback` | 无 proof → 后续失败按 TargetInstance 判 |
| 6 | held | orphan | — | rollback_failed，继续 #2 | ready | `marker orphaned` | 同上 |
| 7 | held | Cobra/YAML/logger/identity/New 失败 | 未武装 | FinishBoot：所有权成立 → Repack → rollback(redispatch)；否则 Abandon+exit | prev / W2 | `bootstrap failed before control-ready; re-dispatching` | 从未导入、从未 register；env 快照带 lineage |
| 8 | refused | Restore 拒绝 / Thaw 失败（Detach） | 未武装 | 同 #7，Repack = `salvage.Prepare(restored.Borrow()...)` | prev / W2 | 同 #7 | 只 Detach、绝不 Close（Close 在表冻结时拒绝 runtime.go:800-803） |
| 9 | active | pending，targets | 武装 | `resource_phase=active, imported_resources=N, freeze_window_ms=`；courier.run；session→register | ready | `image activated … freeze_window_ms=` | 网络在 Thaw 之后；G.1 快照含导入行 |
| 10 | active | Run 早退（权威拒绝） | 武装 | §3.5：Freeze 当前态→Prepare→rollback(redispatch)；失败→Thaw→return err | prev / 今天的退出 | `run failed before control-ready` | 导出**当前态**（gen+1） |
| 11 | active | pending 到期未 commit | 触发 | Freeze 当前态：deferred→`rollback_blocked` + 5s 重试 + 每分钟 Error；成功→Prepare→rollback(redispatch)；exec 失败→Close+Thaw 继续跑 | prev / 原地 | `register deadline exceeded`（一字不改）+ `redispatched`/`rollback blocked` | 不 kill、不关业务 fd 凑期限；期间第二次 install 被拒 `upgrade_in_progress` |
| 12 | ready | committed | 已停 | 之后 Run 失败 = 普通失败 | — | `upgrade COMMITTED`（detail 追加 `; image gen G: N tasks carried, freeze window Xms`） | committed marker 保留 phase |
| 13 | W1 | 任意 | pending∧targets 则武装 | 关外来 fd → 普通启动 → `resource_phase=lost` | 空表 register（H0 结局，如实） | `dispatched image unreadable` | 不伪造成功 |

### 1.7 所有权证明与再派发 env（BLOCKER 修复）

- **FinishBoot 所有权判据**：`m.State==pending ∧ (m.UpgradeID==currentUpgradeBootProof() ∨ (m.TargetInstance!="" ∧ m.TargetInstance==img.Env().InstanceID))`。后者覆盖 shim 被跳过（`os.Executable`/flock 失败返回 ""）的进程；共享 binary 上 sibling 没有我们的 `_TETHER_HANDOFF`，不存在拿别人映像回滚别人升级。
- **再派发 env**：`redispatchEnv(img) = execEnvFor(img.Env().InstanceID, img.Env().RoutingNID)`（instance.go 新增 `execEnvFor(inst, route string)`，`execEnv()` 改为 `execEnvFor(loadedLineage(...), loadedLineage(...))`，行为不变）。快照在 `Begin` 里、任何消费（`mintInstanceIDLocked` :127 Unsetenv、agent.go:882 Unsetenv）之前读取——**New 内任何位置的失败都不再丢 lineage**。
- **belt**：`agent.New` 在 `mintInstanceID` 返回后立刻 `setExecLineage(instanceID, strings.TrimSpace(os.Getenv(routingNIDEnv)))`（routingNIDEnv 的读取+Unsetenv 整段从 :879-883 上提到 :795 之前；`resumeRoutingNID` 仍在原位；末尾 `setExecLineage(a.instanceID, nidOf(a))` 保留）。
- **prev 侧 lineage 裁决**（`adoptImageLineage`，在 restoreImage 内）：bundle `Lineage.InstanceID/RoutingNID` 形状合法（26 位 lowerBase32 / `acceptableLeaseName`）→ env 缺失时填入；env 在场且相等 → 无事；env 在场且不等 → **bundle 为准**（sealed memfd 经 PID/UID/nonce 校验，env 可被任何父进程改写）+ Warn 一行；bundle 值畸形 → 保留 env + Warn。`Lineage.Matches` 只用于决定是否 Warn，**绝不作为拒绝依据**。覆盖 `a.instanceID` 后重跑 `setExecLineage`。

## §2 `node upgrade` 接线（upgrade.go / upgrade_state.go / upgrade_image.go）

### 2.1 `handleUpgradeInstallLocked`（:127）新序

1. 入口门 :135-144 改为 `pending ∧ (now.Before(m.Deadline) ∨ a.rollbackInFlight.Load())` → `upgrade_in_progress`（关 rollback_blocked 期间第二次 install 换掉 `.prev` 的洞）。
2. allowlist :146-156 不变。
3. **运行映像 == 磁盘二进制**门：`runningImageSHA() != sha256OfFile(exePath)` → `install_failed` Error `running image differs from the on-disk binary; restart the agent before upgrading`（磁盘零变更）。让「prev == exporter」成为不变量，再派发永远不会把非 CLOEXEC 副本交给不认 handoff 的 prev（Q3 boot 的 exporter_sha 因此不需要）。
   > **实跑订正（2026-10-02，drill 33 C2 ASSERT-FAIL）**：此门与本 plan §8「33 A/C/C2 不变」自相矛盾——C2 的共享二进制 sibling
   > 在 A 提交后 dst == 候选 ≠ 运行映像，被此门永久拒绝。不变量改为 **prev ∈ {运行映像, 候选}**（两者都能接住任何回滚映像），
   > 判定挪到 extract 后、smoke 前（`installNewBinary` 的 `installPolicy.runningSHA`）；下载前只保留「运行映像可哈希」。
   > 守卫 `TestInstallPrevSlotAcceptsOnlyRollbackCapableImages`（变异 M1 恢复本条原文 → sibling 行红；M2 去门 → unrelated 行红；M3 忽略哈希错 → unhashable 行红）。
4. **preflight #1**（下载前，只读）：`preflightDeferrals(a)` 非空 → reply `{Code:"upgrade_deferred", Error:"deferred: pty_busy (2 PTY sessions); flow_busy (…)"}`，零磁盘变更、无 marker，日志 `agent: upgrade deferred reasons=[…]`。
5. 下载/sha/extract :158-200 不变；`smokeVersion` 改返回 `(release string, abi int)`：首行解析不变（v0.6.0 的 smokeVersion 也只读首行 :796-812，4 行输出兼容），后续行匹配 `image-abi v<N>`。
6. **候选 ABI 门**：`abi==0 ∧ !imageIsEmpty(a)`（任务>0 ∨ tombstones>0 ∨ revocations>0 ∨ courier pending>0）→ reply `image_abi_unsupported`（磁盘零变更）；`abi==0 ∧ empty` → Warn + 今天的裸 exec 路径（无 bundle 可泄）。`abi > proto.ImageABIVersion` → 同码（跨代由 S8）。
7. `installNewBinary` :530：marker 多写 `Generation: a.imageGeneration+1, ResourcePhase:"staged"`；其余（prev link/marker fsync/flip/syncDir）不变。
8. **gate 关闭** `closeUpgradeGate(a)`（§4.0）→ **心跳前置** `publishHeartbeatOnce(a, nc)` + `nc.FlushTimeout(ReplyDrain)`（把 5s STALE 空窗从 0 起算，否则 ~20% 实跑里新 exec 探针撞 `node_offline: status=STALE` admit.go:322-330）。
9. **cut**：`cut, err := freezeForUpgrade(a, now+SafePointWait)`（= `agentimage.Freeze(imageOwners(a), deadline)`）：
   - `*DeferredError` → `openUpgradeGate` → `executeRollback(exePath, m, "deferred("+reason+")", logger, false, nil)`（un-flip：prev 回 dst、marker rolled_back、`ResourcePhase:"deferred"`）→ reply `upgrade_deferred`（第二个字面量站点，coverage 门要字面量）。
   - 其它错误（ErrFrozen/export/encode）→ 同样 un-flip，Detail `freeze failed: <class>` → reply `install_failed`。
10. reply OK `{BinaryReplaced, NewVersion}`（**Freeze 在 reply 之前**：deferral 永远同步；否则 `--wait` 150s 后打 `likely ROLLED BACK` 退 64 node.go:474-486）→ 日志 `agent: image cut generation= tasks= courier_events=`。
11. `a.reExecInPlace(nc, exePath, cut, tracked=true)`。

### 2.2 `reExecInPlace(nc, exePath, cut *imageCut, tracked bool)`（:301 新形状）

```go
if cut == nil { /* 空映像裸 exec 路径（步骤 6）*/ …今天的 goroutine… }
if a.cfg.UpgradeNoExit && a.cfg.UpgradeExecFn == nil { cut.abandon(a); return }   // 进程内 harness：Thaw + 开门，p10/durability/security/chaos 四套件语义不变
go func() {
    a.cutMu.Lock()                                   // §2.5 latch：Run 的 defer stopProcessRuntime 与本 goroutine 串行
    defer a.cutMu.Unlock()
    _ = nc.FlushTimeout(upgradebudget.ReplyDrain)   // 有界 flush 取代裸 100ms sleep（:306）
    if a.cfg.UpgradeCutObserver != nil { a.cfg.UpgradeCutObserver(cut.frozen.Bundle()) }
    prepared, err := cut.frozen.Prepare()           // P5：此处重 Export，撤权最新；Prepare→Exec 只隔一次函数调用
    if err != nil { cut.abandon(a); if tracked { a.recoverFromFailedExec(exePath, err) }; return }
    argv := append([]string(nil), os.Args...); argv[0] = exePath
    err = a.upgradeImageExec(exePath, argv, prepared) // 生产 prepared.Exec(exePath, argv, execEnv())；测试 cfg.UpgradeExecFn(exePath)
    _ = prepared.Close()                             // 只有失败才回来
    cut.abandon(a)                                   // 先 Thaw（Manager→Runtime）+ 开门，再动盘面
    if tracked { if a.recoverFromFailedExec(exePath, err) { return } }   // F3：Detail 含 syscall.Exec；m.ResourcePhase="thawed"
    log Error "re-exec failed with no pending marker; staying alive on the thawed image"   // ReExecOnly 腿：Q3（推荐保活，改 :488-495 契约测试）
}()
```

- `imageCut{frozen *agentimage.Frozen; tasks int; generation uint64}`；`abandon(a)` = `frozen.Thaw()`（Manager 先、Runtime 后，freeze.go:201-211）→ `openUpgradeGate(a)`；Thaw 失败只 Error（S2 结构上不可达）。
- 冻结窗 = Freeze → 新映像 Thaw，含 ReplyDrain（≤100ms）+ 安装后的 0 次 fsync（安装在 cut 前）+ exec + Go init + Restore + shim（sha 20MiB + 2×fsync）+ Cobra/YAML/identity + New + Import + Thaw。

### 2.3 `handleReExecOnly`（:254）

sha 守卫后：`runningImageSHA()==staged sha` 已由守卫保证 → `candidateABI(exePath)`（同 smoke）→ ABI 门 → preflight（deferred → `upgrade_deferred`，无磁盘变更）→ gate 关 → 心跳 → cut（deferred → `upgrade_deferred`；本腿无 marker 可 un-flip，Freeze 必须先于 reply）→ reply OK → `reExecInPlace(nc, exePath, cut, tracked=false)`。并发两次 ReExecOnly：本腿也走 `upgradeInstallMu.TryLock`（:74 之前返回的路径改为先 TryLock，失败回 `upgrade_in_progress`）。

### 2.4 marker additive 字段（upgrade_state.go:81）

```go
Generation        uint64 `json:"generation,omitempty"`         // 安装时 = a.imageGeneration+1
ResourcePhase     string `json:"resource_phase,omitempty"`     // staged|active|redispatched|thawed|deferred|lost|rollback_blocked
ImportedResources int    `json:"imported_resources,omitempty"` // 新映像 Thaw 后
FreezeWindowMS    int64  `json:"freeze_window_ms,omitempty"`   // 新映像 Thaw 后（Imported.FreezeWindow）
```
写点：`staged`+`Generation` 随 pending 一起（installNewBinary，flock 已持）；`active`+`ImportedResources`+`FreezeWindowMS` 在 Run 的 Thaw **之后**（`stampMarkerPhase`：flock 短临界区，pending ∧ `markerTargetsThisAgent` 才写，fsync 不进冻结窗）；`redispatched/thawed/deferred/lost/rollback_blocked` 在各自 `executeRollback`/watchdog 写 marker 的同一次写里。committed 时不清（面包屑）；`upgradeRegisterReport` :681 的 detail 追加 `; image gen %d: %d tasks carried, freeze window %dms`（UpgradeDetail 是文本，零 wire 变更）。`readUpgradeMarker` 的 `json.Unmarshal` 无 DisallowUnknownFields，v0.6.0 读新 marker 兼容；**绝不放 bundle 内容**。

### 2.5 cut latch 与 Run 退出

`Runtime.Close` 对 **frozen && exported** 的 runtime 静默 `discardLocked`（runtime.go:794-798）——不是拒绝而是吞掉。Run 的 `defer stopProcessRuntime` :941 与 cut goroutine 无互斥时，SIGTERM 落在 Freeze→exec 之间会 discard 旧 runtime、随后 exec 失败的 Thaw 得 ErrClosed。修复：`a.cutMu sync.Mutex`；cut goroutine 从 Freeze 前持到 Exec（成功不返回，锁随进程消失）；`stopProcessRuntime` 先 `a.cutMu.Lock()`（exec 成功时永不返回——进程已被替换，无人观测；失败时等 abandon 完成再 Close）。§22.6 写成调用纪律：「exported 的 runtime 被 Close = discard」。

### 2.6 watchdog（:609/:651）

goroutine 改循环：`for { select { case <-wctx.Done(): return; case <-timer.C: if a.watchdogRollback(exePath) == rollbackRetry { timer.Reset(RollbackCutRetry); continue }; return } }`。`watchdogRollback` 在 flock 闭包内：pending ∧ targets → `a.image!=nil && phase==active` → `closeUpgradeGate` → `cut,err := freezeForUpgrade(a, now+SafePointWait)`：DeferredError → 开门、`m.ResourcePhase="rollback_blocked"`（Detail=reason，每分钟一行 `agent: upgrade rollback blocked reason=`）、`a.rollbackInFlight.Store(true)`、返回 rollbackRetry（**不**裸回滚、不 kill）；成功 → `prepared := cut.frozen.Prepare()`；`m.ResourcePhase="redispatched"`；`executeRollback(exePath, m, "register deadline exceeded (%s) without a successful register", logger, true, redispatchExec(prepared, a.upgradeExec))`；exec 返回（失败/假 exec）→ `prepared.Close(); cut.abandon(a)`（S12 继续跑）。无映像（legacy pending）→ 今天的裸回滚。`UpgradeNoExit` 分支 execFn=nil 保持。

### 2.7 `redispatchExec`

```go
func redispatchExec(prepared *handoff.Prepared, env []string, fallback func(string) error) func(string) error {
    return func(p string) error { if prepared == nil { return fallback(p) }; argv := append([]string(nil), os.Args...); argv[0] = p; return prepared.Exec(p, argv, env) } }
```
boot 分支 env = `redispatchEnv(img)`；watchdog/Run 早退 env = `execEnv()`（New 已 setExecLineage）。

## §3 agent 会话接线

### 3.1 `agent.New`（:753）

- :795 前：`restoredRouting := strings.TrimSpace(os.Getenv(routingNIDEnv)); _ = os.Unsetenv(routingNIDEnv)`；`mintInstanceID` 后立刻 `setExecLineage(instanceID, restoredRouting)`（§1.7 belt）。
- :812 `initProcessRuntime(a)` **删除**，挪到 New 最后一步（:890 `setExecLineage` 之后）：
```go
img := cfg.BootImage; if img == nil { img = agentboot.Claim() }
if img != nil { if err := restoreImage(a, img); err != nil { return nil, err } } else { initProcessRuntime(a) }
```
  理由：`Target.Spawn` 需要 :852 的 `a.spawnPolicy`（New 时 health map 空、`IsHangablePath` 不建条目 → `ImportReady` 成立）；lineage 裁决需要 `nidOf(a)`；restore 之后 New 内无可失败步骤。`Claim()` 覆盖 `agent join --start`（agent_join.go:174）而无需 cmd/tether 接线；`Config.BootImage *agentboot.Image` 供测试注入 `agentboot.FromCut`。
- `restoreImage(a, img)`（bootstrap.go）：`exec.go` 抽出 `processRuntimeConfig(a) execsession.Config`（Manager/Start/Started/Exited/Warn 整段搬，`Instance` 留空）；`tgt := agentimage.Target{Runtime: processRuntimeConfig(a), Spawn: a.spawnPolicy, Courier: NewCourierImage(a)}`；`imp, err := img.Restore(tgt)`；失败 → **不**回落 `initProcessRuntime`（禁止空表 register 的结构保证），返回 `&ImageRestoreError{Summary: img.Summary(), Err: err}`（`Error()` 永不打 bundle）；成功 → `adoptImageLineage(a, imp.Bundle.Lineage)`（§1.7）、`a.processRuntime=imp.Runtime; a.image=img; a.imageGeneration=imp.Bundle.Lineage.Generation`、日志 `agent: image restored generation= tasks= courier_events= spawn_verdicts=`。courier 在 :811 已 New 且空 → `importCourierState` 的空表前提（proc_delivery_image.go:91）成立。
- Agent 新字段：`image *agentboot.Image; imageGeneration uint64; cutMu sync.Mutex; gate upgradeGate; rollbackInFlight atomic.Bool`。

### 3.2 `Run`（:914）

第一条语句、在 `loadRosterCacheAtBoot`（hangable Home）与任何 defer 之前：
```go
if err := activateImage(a); err != nil { return err }   // 失败：img 已 Detach → refused；无 defer 跑 → 不会对冻结 runtime 调 Close
```
`activateImage`：`win, shim, imp, err := a.image.Activate()`（= `Imported.Thaw()`：Manager 先、Runtime 后 restore.go:275-287）；成功 → `stampMarkerPhase(a, "active", tasks, win)` + `agent: image activated generation=%d tasks=%d freeze_window_ms=%d shim_ms=%d import_ms=%d`（**恰一行/代**，drill 与校准的唯一读点）。失败（结构上只能 ErrClosed，不可达）→ `Imported.Detach()` 取 Salvage（此刻 `thawed` 未置位，restore.go:305 合法；`discardTable` 因 Manager 已 thaw 会报错——只记录，reapLoop 到 exec 为止的毫秒窗如实写进已知限制）→ return err → FinishBoot 再派发。随后 `go a.courier.run(ctx)` :919、`armUpgradeWatchdog` :924 照旧。

### 3.3 session / register

- :943 `if err != nil { return err }` → `return runFailedBeforeControlReady(a, err)`（§3.5）。
- register :1712 default 臂前新增：`case resp.Code == proto.CodeSessionNotFound || resp.Code == proto.CodeSessionNotFoundOrDeleting:` → `revokeSessionWorkloads(a, resp.Code)`（§22.4-7(c) 的 S3 半，两个码 broker.go:2553/2558 都发）→ 包 `errSessionGone` 返回。
- 成功臂 :1693 `commitUpgradeAfterRegister` 之后 `if a.image != nil { a.image.MarkReady() }`。

### 3.4 courier / G.1

- `localProcessSnapshot`（run.go:633）零改动：导入行来自 `a.workloads.Snapshot()`（`RestoreProcesses` 装入，StartedAt/StartTimeTicks 原值）∪ 导入 outbox；import 在 New、register 在 session → 快照结构上不可能空表。
- **P9 结算**（proc_delivery.go:452-456）exit 臂新增前提：`c.pending` 里同 pid 仍有 `procEventStarted` → `continue`（broker 的 `resolveReconcile` 只遍历已有 RUNNING/LOST 行 reconcile.go:80-83，未知 pid 的 exited 行被忽略，「不在 Accepted」此时意为「未知」而非「已结清」；旧规则会删掉唯一一份 rc）。N-1 旧 broker：started 永不 ACK → exit 最多滞留至 `courierMaxLifetime`，每次 register 携带 exited 行、broker 幂等——写进 §22.6。
- Thaw 后 observer 起来才把终态送进 courier；courier.run 在 Thaw 之后 → 顺序天然成立。

### 3.5 `runFailedBeforeControlReady(a, err)`

1. `ctx.Err()!=nil ∨ errors.Is(err, context.Canceled)` → return err（SIGTERM/systemctl stop 落在 pending 窗内**不**再派发，否则 systemd 等 MainPID 退出、TimeoutStopSec 后 SIGKILL 刚派发的 prev）。
2. `errors.Is(err, errSessionGone)` → 已撤权 → return err。
3. `a.image!=nil ∧ phase==active ∧ marker pending ∧ targets` → 与 watchdog 同一段（`upgradeMu`+flock 串行，谁先谁赢）：cut 当前态 → Prepare → `executeRollback(…, true, redispatchExec)`；deferred → 不回滚、return err；exec 失败 → Close+Thaw → return err（随后 `defer stopProcessRuntime` Close 杀任务——如实）。
4. 其余 → return err。

### 3.6 session_destroyed 三臂（零新代码，只加测试）

(a) 撤权在 Freeze **之前**：`revokeSessionWorkloads` :718 → tombstone/kill → 随 image 导出（S2）。(b) 撤权在 Freeze **之后、exec 之前**：`Revoke` 只登记 → **P5 的 Prepare 重 Export** 把它带进 bundle → 新映像 Thaw 补杀（fence.go:222）。(c) 撤权落在 exec 窗内（事件丢失）：新映像 register 被 broker 以 `session_not_found*` 拒绝 → §3.3 撤权。(d) 导入后再来一次 → `Revoke` 幂等、同 handle 的 create 得 `process_revoked`、tombstone resume 得 `process_output_discarded`。

### 3.7 `FinishBoot(runErr error)`（bootstrap.go；main.go :91 后一行）

```go
img := agentboot.Current(); if img == nil { return }
switch img.Phase() { case PhaseNone, PhaseReady, PhaseLost, PhaseActive: return }   // active：Run 内已尝试 §3.5
_ = withUpgradeFileLock(exePath, true, func() error {
    m, _ := readUpgradeMarker(...)
    if !ownsPendingMarker(m, img) { img.Abandon(logger, "bootstrap failed with no rollback target: "+errClass(runErr)); return nil }   // W2
    prepared, err := img.Repack(); if err != nil { m.ResourcePhase="lost"; img.Abandon(...); executeRollback(exePath, m, reason, logger, true, realExec); return nil }  // 裸回滚：fd 已关
    m.ResourcePhase = "redispatched"
    executeRollback(exePath, m, "bootstrap failed before control-ready: "+errClass(runErr), logger, true, redispatchExec(prepared, redispatchEnv(img), realExec))
    prepared.Close(); img.Abandon(logger, "exec of previous binary failed after rollback"); return nil })   // 只有 exec 失败才到这里；main 接着非零退出
```
`errClass` 只取分类词（`cobra|config|identity|bundle_rejected|lineage|thaw_failed|new`），**不**把 err 原文写进 marker。覆盖 RunE 的 7 个早返回（agent.go:303/307/315/328/353/363/384）与 Cobra 层错误，无需逐处插桩。

## §4 预检 deferred 集合

### 4.0 准入 gate（`upgradeGate`）

`type upgradeGate struct{ mu sync.Mutex; closed bool; inflight [4]int }`（kind: exec/run/transfer/expose）。`admit(a, kind) (release func(), ok bool)`：关门 → ok=false；开门 → inflight++，release 递减。站点：`handleExecForwarded` exec.go:118（legacy）→ `ExecChunk{Kind:error, Error:"agent_upgrading: image switch in progress; retry"}`；`handleRunForwarded` run.go:77 → `RunChunk{Kind:failed, Reason:"agent_upgrading"}`；`handlePushForwarded/handlePushCommitForwarded/handlePullForwarded` transfer.go:104/223/374 与 `handleExposeForwarded` expose.go:121 → `Code:"agent_upgrading"`；`proxyStartLocked` proxy.go:537 关门时直接 return（broker ≈5s 重推自愈）。handler 持 inflight 到返回（legacy exec/run 含 child 整个生命周期）→ **inflight 计数就是 legacy_session_busy / transfers_in_flight 的数据源**，关门后「检查过了才新建」不可能发生（legacy 子进程撞冻结表被 SIGKILL 的 TOCTOU 关掉）。关门点：cut 之前；开门点：deferral / exec 失败 / abandon。

### 4.1 原因码表（首版 11 个 = 裁决 7 + S2 6，`spawn_in_flight`/`remote_fs_probe_in_flight` 两侧共有）

| Reason | 判据（Detail） | 数据源 | 竞争处置 | 移除 stage |
|---|---|---|---|---|
| `pty_busy` | `len(a.procs)>0`（`"%d PTY session(s)"`）∨ inflight[run]>0 | agent.go:374 `procs`；gate | gate 关门后 run 拒绝 | S4 |
| `legacy_session_busy` | inflight[exec]>0 ∨ `workloads.Snapshot()` 行 ∉ `processRuntime.TaskPIDs()` ∧ ∉ `a.procs`（`"%d legacy exec child(ren)"`） | gate；Manager | 同上；Freeze 的 `foreignRows` 是 belt | S10 保留（文档化） |
| `flow_busy` | `cfg.ExposeAdapter.(interface{ SessionCount() int })` >0 ∨ `len(a.rehomeRunning)>0`；**严格：任一 expose 会话即 busy**（exec 关掉 TLS/yamux 控制连接，公网端口在重拨前有黑窗——§3-4 禁止沿用旧行为） | 新 `(*tunnel.Client).SessionCount()`（`len(c.sessions)` 于 `c.mu`，tunnel.go:1314 客户端侧）+ `TunnelExposeAdapter` 实现；测试 adapter 不实现=无流 | 无竞争窗（新会话由 broker 推送，exec 后重放） | S5 |
| `ss_busy` | `a.proxy.servingLocked()`（proxy.go:187，在其锁下读；`"proxy serving on local port N"`）**严格** | proxy | 同上 | S5 |
| `transfers_in_flight` | inflight[transfer]+inflight[expose]>0 ∨ 未过期 `pushCommitCache` 条目>0（agent.go:448，`pushCommitEntryExpired`） | gate；cache | gate 关门 | S7 |
| `remote_fs_probe_in_flight` | `a.homeReadInFlight`（agent.go:490，`homeReadMu` 下，detail `home state.json read in flight`）∨ `Census().ProbesInFlight>0`；`a.homeHangable` 时先做一次有界（≤RequestLatency）Home 读探测，超时 → 本码（detail=home） | agent；spawnsafe | Freeze 自带 belt | S8/S10 |
| `spawn_in_flight` | `Census().Wedged>0`；Freeze 时 registry `starting`（ErrBusy） | spawnsafe；Runtime.Freeze | SafePointWait 5s 有界等 | S8 |
| `foreign_process_rows` / `image_too_large` / `runtime_closed` / `platform_unsupported` | S2 原码（`a.processRuntime==nil` → `runtime_closed`） | agentimage | belt | S8/S10 |

`preflightDeferrals(a) []agentimage.DeferredError` 纯读、返回**全部**命中；`busyChecks(a) []agentimage.BusyCheck` 是同一批谓词（Freeze 只报第一条）。`imageOwners(a) = Owners{Runtime, Manager:&a.workloads, Spawn:a.spawnPolicy, Courier:NewCourierImage(a), Busy:busyChecks(a), Lineage:{Generation:a.imageGeneration+1, InstanceID:a.instanceID, RoutingNID:nidOf(a)}}`。

### 4.2 wire 形状 / ctl 呈现 / exit class（零新字段，ProtoVersion 不动）

| Code | 发射点（字面量） | Error 文本 | exit class | hint | 集合 |
|---|---|---|---|---|---|
| `upgrade_deferred` | install preflight#1、post-flip Freeze、ReExecOnly 两处 | `deferred: <reason> (<detail>); …` | `exitTransient` 75 | **不登记**（`brokerErrorMessage` :379-381 会用 hint 替换 Error，原因码消失；表旁注释说明） | `transientUpgradeCodes`（`--all` 跳过、canary 仍中止） |
| `image_abi_unsupported` | ABI 门 | `candidate has no image-abi line and this node carries N live task(s); pick a hot-update-capable release or drain first` | `exitUsage` 64 | 登记 | `configUpgradeCodes` |
| `agent_upgrading` | gate 五站点 | `image switch in progress; retry` | 75 | 登记 | — |

broker `upgrade.go:113` 前缀 `agent_rejected:` 透传；wire inventory 无 diff；`error_code_coverage` 门自动纳入（复合字面量形式）。`Deferred []string` 字段留给 S8（写进 §22.6「后续 additive」）。

## §5 部署单位与 FD 纪律

### 5.1 三形态零 unit 改动
exec 同 PID → systemd `MainPID`/`ExecMainStartTimestamp`/`NRestarts` 逐字相等；`KillMode`/`Restart` 从不触发（S3 热路径没有 `os.Exit`——只有 W2 退出，那里 `Restart=on-failure` 正是收敛器）；user unit（`agentUserUnitBody`）与 setsid-nohup（install.sh :576 起法）同理；`scripts/install.sh` 与 `cmd/tether/agent.go:500-520` 一行不改。

### 5.2 FD 白名单审计表（exec 前）

| fd | 创建站点 | CLOEXEC | 过 exec | 断言 |
|---|---|---|---|---|
| 0/1 | supervisor / nohup 重定向 | 否（设计） | 是 | 不检查 |
| 2 | `dup3(fd,2,0)`（agent_logsink_dup3.go:10）→ agent.boot.err | 否（设计：panic sink 跨 exec） | 是 | `readlink /proc/<pid>/fd/2` 前后同 inode |
| NATS socket / tunnel TLS | `net.Dial`（SOCK_CLOEXEC） | 是 | 否 | fdinfo flags |
| agent.log writer | logrotate.go:93 `os.OpenFile` | 是 | 否 | — |
| `.tether-upgrade.lock` | upgrade_state.go:134，闭包返回即 Close | 是 | 否 | `ls /proc/<pid>/fd` 无该路径 |
| exec child 三组 pipe | workload/stdio.go:29-31 `os.Pipe` | 是 | 仅 handoff dup 副本 | `pipe:[ino]` 每条恰 1 引用；导入后 CLOEXEC 已恢复（image_linux.go:330-337） |
| replay 环 | replay_linux.go:15 `MFD_CLOEXEC` | 是 | 经副本 | 按类别前后相等，≥2/任务（stdin 无输入时 2 环 fence.go:137-178；精确数首跑后钉） |
| pidfd | 内核恒 CLOEXEC | 是 | 否（`RestoreProcesses` 重开） | 计数 == tasks |
| handoff manifest memfd | image_linux.go:182 `MFD_CLOEXEC`；Exec 窗内清位 | 窗内否 | 是；`restoreDescriptor:320` 关 | == 0 |
| spawnexec 租约 AF_UNIX | **helper 子进程**（helper.go:212-218），agent 不持有 | n/a | n/a | `ss -xlp | grep -c tether-spawnexec-<uid>` 前后相等 |
| ssproxy/PTY/transfer fd | `net.Listen`/`/dev/ptmx`/`unix.Open(...O_CLOEXEC)`（transfer.go:977-1149） | 是 | 否（对应 deferred 码兜底） | — |

审计命令（drill/hermetic 同一条）：`for f in /proc/$PID/fdinfo/[3-9]* /proc/$PID/fdinfo/[1-9][0-9]*; do fd=${f##*/}; flags=$(awk '/^flags/{print $2}' $f); [ $((8#$flags & 8#2000000)) -ne 0 ] || echo "NONCLOEXEC $fd $(readlink /proc/$PID/fd/$fd)"; done` → 稳态与导入后均为空。变异：hermetic 用 `unix.FcntlInt(fd, F_SETFD, 0)` 清一个 → 红。

### 5.3 旧映像退出 oracle
`sha256(/proc/<pid>/exe)==新`、`/proc/<pid>/maps` 无旧 inode、`memfd:tether-handoff==0`、`anon_inode:[pidfd]==tasks`、无 `Z` child、`pgrep -f 'tether agent'` 计数不变、租约计数不变。memfd 名字 `tether-replay`/`tether-handoff` 两处加 `// drill-pin: test/simcluster/drills/lib/hot_update_process_probe.py fdcensus` 并由探针 selftest 对源码字面量 pin。

### 5.4 代际链
`Lineage.Generation = a.imageGeneration+1`；marker `generation` 同值；三层证明：hermetic 三跳同 PID（T9）、真二进制 gen1→gen2→gen1（T16）、drill 34.G（A→B→A：tether→tether-next→tether，第三跳的导入方是最老代码 = 同 Version 内向后可读的直接证明；零供给构建改动，只加 `artifact_add` 第二 tarball）。

## §6 `internal/upgradebudget` 常量表

| 名 | 值 | 用处 | 引用者（改为引用，值不变） |
|---|---|---|---|
| `LocalFreezeWindow` | 500ms | 目标；S3 只记录 | evidence、`hot_update_budget.sh:HOT_UPDATE_FREEZE_MS` |
| `RequestLatency` | 5s | 每新控制请求；Home 有界探测上界 | 探针 `HOT_UPDATE_REQUEST_S` |
| `ProbeRatePerSecond` | 10 | 混合探针 | `HOT_UPDATE_PROBE_RATE` |
| `LiveFlowStall` / `TransferProgress` | 5s / 10s | S5 / S7 | 预留 |
| `ComponentHandover` | 120s | `upgradeRegisterDeadline`（upgrade_state.go:67）；`node.go:405 upgradeWaitBudget = ComponentHandover+30s`（仍 var）；`node.go:461` 文案 `fmt.Sprintf("deadline %ds", …)` 渲染字节逐字相同（33 A1 grep） | agent、ctl、`HOT_UPDATE_REGISTER_DEADLINE_S` |
| `FullStackRollout` | 15min | S10 | 预留 |
| `StreamReplayBytes` / `NodeReplayBytes` | 8MiB / 256MiB | `workload.DefaultReplayCapacity/Budget`（replay.go:13-14）改为引用 | workload |
| `MaxGenerations` | 2 | S8 | 预留 |
| `SafePointWait` | 5s | `agentimage.Freeze` deadline | cut/watchdog |
| `ReplyDrain` | 100ms | OK 回复 flush 上界（替代裸 sleep） | reExecInPlace、心跳 flush |
| `RollbackCutRetry` | 5s | rollback_blocked 重试 | watchdog |
| `BootBudget` | 3 | `upgradeBootBudget`（:60） | agent |

`budget_test.go` 钉相等关系（`SafePointWait < ComponentHandover`、`ReplyDrain < LocalFreezeWindow`）；shell 单源 `test/simcluster/drills/lib/hot_update_budget.sh`（`HOT_UPDATE_*=` 行）由新门 `test/architecture/upgrade_budget_pin_test.go` 与 Go 常量三方对账（`nats_ping_defaults_test.go` 同形，`gate-control:` 锚 + 正负控制，边际 <1s；CLAUDE.md 闸门表 +1 行）。

## §7 测试清单

真二进制层形状（testing-standards §0.6 新增）：**三角色 harness** `internal/agentboot/boot_exec_test.go`——父（go test）→ dispatcher（同一 test 二进制 mode=dispatch：起 child、`agentimage.Freeze`、`Prepare`、`Exec(<sandbox>/tether, ["tether","agent",…], env)`）→ tether（真 `go build ./cmd/tether` 产物；marker JSON 由父按 `upgradeMarker` 字段手写，注释 `// drill-pin: mirrors internal/agent/upgrade_state.go upgradeMarker`）→ receiver（`.prev` = test 二进制副本；`init()` 按 mode env 分派、早于 `testing` 解析 flag，spawnexec.init 同形 → `handoff.Restore()` → `agentimage.Restore(Target{fresh Manager})` → 写 receipt → `os.Exit`）。父断言 receipt + marker + dst sha。进程内层：`agentboot.FromCut` 注入 `Config.BootImage`（映像来源 F_DUPFD_CLOEXEC 复制文件，A 表 cleanup 顺序：kill child → 等 B 记终态 → `A.workloads.Thaw()`）。

| # | 名称 | 文件 | 断言 | 变异（每条必须真注入并变红） |
|---|---|---|---|---|
| T1 | `TestAgentBootstrapRunsBootCheckBeforeRestore` | `internal/agentboot/boot_exec_test.go` | marker pending 预算内，argv 带 `--flag-that-does-not-exist`：boot_count 0→1（budget 记账早于 Cobra）；Cobra 拒绝 → FinishBoot → receiver 收到同一 bundle（State 字节 sha 相等、文件 dev/ino 相等、child 活、读到 exec 后写的字节）；marker rolled_back Detail 前缀 `bootstrap failed before control-ready: cobra` + `resource_phase=redispatched`；tether stderr **无** `image restored`；receiver env 含原 `TETHER_INSTANCE_ID` | 删 main.go `agent.FinishBoot` 调用 → receipt 缺失；boot_count++ 挪到 Restore 之后 → 仍 0；再派发 env 用裸 `execEnv()`（快照删除）→ receiver 报 lineage 缺失 |
| T2 | `TestBundlePresentAndBudgetExhaustedRedispatchesToPrev` | 同上 | boot_count==budget：tether 走 bootRollback → receiver Restore 成功（同 T1 断言）、marker Detail 以 `boot: budget/deadline exhausted` 开头、dst sha==prev sha、`memfd:tether-handoff==0`、receiver 二次 `handoff.Restore()` 为 nil、输出不含 `"secret"` | bootRollback 改回裸 realExec → receiver `handoff.Restore()` 失败/为 nil |
| T2' | `TestBootRollbackExecFailureKeepsImageHeld` | `internal/agent/upgrade_state_test.go` | 进程内：`bootUpgradeCheckLocked(…, img, failingExecFn)` → prepared 非 nil 且已 Close、`img.Phase()==held`、随后 `img.Restore` 仍成功（Borrow 借用语义）、fd 普查回基线 | Borrow 改为转移所有权 → Restore 拒绝 |
| T3 | `TestImportFailureNeverRegistersEmptyTable` | `internal/agent/bootstrap_test.go` | 嵌入 broker；`cfg.BootImage=FromCut(篡改 bundle)` → New 返回 `*ImageRestoreError`、a==nil；img.Phase()==refused、salvage 非 nil；broker 节点表 1s 内无该 nid；原 child 活；`finishBoot(…, fakeExec)` 收到 Prepared 且其 State 可 `agentimage.Decode`、tasks==1（**不是空 Data**）；marker redispatched | New 失败回落 `initProcessRuntime` → broker 出现 nid；Repack 对 refused 走 `restored.Repack()` → Decode 失败 |
| T4 | `TestReconcileSnapshotIncludesImportedTasks` | 同上 | A 起真实任务 → `freezeForUpgrade` → FromCut → B `New`+`Run`：`localProcessSnapshot(B)` 含 pid、StartedAt/StartTimeTicks 等于 A 行、StartedUnacked=false；首个 register 时（beforeRegisterHook）`processRuntime.Frozen()==false` 且 courier loop 已起；broker AcceptedProcesses 含 pid、DropProcesses 空；ps RUNNING；child 未 kill | restoreImage 跳过 `RestoreProcesses` → G.1 -1；Thaw 挪到 register 之后 → hook 红 |
| T5 | `TestSessionDestroyedTombstoneSurvivesImport` | 同上 | 四臂 §3.6 (a)(b)(c)(d)：(b) Freeze 后 `handleSysEvent(session_destroyed)` → 只登记 → Prepare（重 Export）→ B 导入+Activate → child 1s 内 SIGKILL、终态 `revoked:`；(c) 假 broker 回 `session_not_found` 与 `session_not_found_or_deleting` 各一子臂 → `revokeSessionWorkloads` 被调、child 死、Run 返回 | P5 重 Export 删除 → (b) child 活；default 臂 revoke 删除 → (c) child 活 |
| T6 | `TestPreflightDefersOnEachReasonCode` | `internal/agent/upgrade_image_test.go` | 表驱动 11 行（§4.1 每码一个制造法：`a.procs` 塞 procRec / 真 sleep child 经 workloads 收养 / 假 adapter `SessionCount()=1` / proxy serving / gate inflight & pushCommitCache / `homeReadInFlight=true` & 阻塞 Probe / wedged / AdoptDraining 外来行 / `stopProcessRuntime` / …）：`preflightDeferrals` 恰含该码与 Detail；同状态 `agentimage.Freeze(imageOwners(a),…)` 返回同码 `*DeferredError` 且 Runtime/Manager 未冻；经真 NATS `handleUpgradeForwarded`：Code==`upgrade_deferred`、Error 含 `<reason> (`、httptest 命中 0、无 marker/prev、dst 不变；`DeferredReasons` 长度 11 | 逐个删钩子 → 对应行红；preflight 只返回第一条 → 多命中行红；preflight 挪到下载后 → 命中 1 |
| T7 | `TestUpgradeCutFailureAfterFlipRollsDiskBack` | 同上 | preflight 通过后、下载期间（httptest 阻塞）起 PTY → cut deferred → marker rolled_back Detail `deferred(pty_busy)` + `resource_phase=deferred`、dst sha==旧、.prev 消失、reply `upgrade_deferred`、runtime 未冻；第二臂注入 Manager 已被外部冻结 → `install_failed` + 同样 un-flip | cut 失败分支不调 executeRollback → dst 仍新 |
| T8 | `TestUpgradeExecFailureThawsOriginalBusiness` | 同上 | 活任务（部分 ACK）+ marker pending + `UpgradeExecFn` 返回 error：`Frozen()==false` 两表、gate 开、Handle resume 1s 内返回新字节、rc 23 精确送达、marker rolled_back + `resource_phase=thawed`、dst=prev、Prepared 已 Close（`memfd:tether-handoff==0`）；`syncObserver` 时序：Thaw 早于 marker 写；五轮无 fd/goroutine 增长（共享泄漏 helper 锚） | 删 `cut.abandon` → Handle 阻塞超时；先 recover 再 Thaw → 时序红 |
| T8' | `TestRunExitDuringCutWaitsForAbandon` | 同上 | 活任务 + 注入 exec 必失败 + Freeze 后立刻 cancel Run ctx → child 仍有 owner（Exited 最终送达、fd 回基线）、`Runtime.Close` 未对 exported runtime 静默 discard | 去掉 `cutMu` latch → Thaw 得 ErrClosed |
| T9 | `TestGenerationChainExportImportExport` | `internal/agentimage/bundle_exec_test.go`（三跳同 PID）+ `internal/agent/bootstrap_test.go`（进程内 A→B→C） | hop2 Generation==1、Thaw 后 child 继续写并再 Freeze（Generation 2）→ hop3 Generation==2；PID/ticks/pipe ino 三跳不变、stdout 精确 = before+middle+after、rc 23、两次 FreezeWindow 样本、handoff memfd 0；agent 级：每代 `Lineage.InstanceID/RoutingNID` 非空且等于 `a.instanceID/nidOf`；marker `generation` = 代 | `hop2-no-generation-bump` → hop3 Generation==1；Lineage 不填 InstanceID → 非空断言红 |
| T10 | `TestRegisterReceiptSettlesOnlyCarriedEvents` | `internal/agent/proc_delivery_test.go` | ImportOutbox{X:started(fails=2), X:exit rc=7}；register 回复 Accepted=[] → X.exit 与 X.started 均保留；ACK X.started 后再 register（Reconciled 含 X）→ X.exit 结清；receipt 后 enqueueExit(Y) 与替换 X → 指针身份对账，替换后的 X 不被误删 | 删 startedPending 守卫 → 第一臂 X.exit 被删 |
| T11 | `TestWatchdogRollbackReexportsCurrentStateOrDefers` | `internal/agent/upgrade_state_test.go` | 导入并 thaw 的活任务 + marker 过期：`UpgradeCutObserver` 收到 bundle（tasks=1、Generation=imported+1）、execFn 收到非 nil Prepared、marker rolled_back Detail 含 `register deadline exceeded` + `resource_phase=redispatched`；假 exec 返回后 runtime 已 Thaw、child 活；deferred 子臂（注入 pty_busy）→ 不 exec、marker pending + `rollback_blocked`、返回 rollbackRetry、fake clock 推进后第二次成功；**阻塞期间第二次 install → `upgrade_in_progress` 且 `.prev` sha 不变** | watchdog 改回裸 realExec → 观察者未调；deferred 改裸回滚 → 红；入口门去掉 rollbackInFlight → .prev 被换 |
| T12 | `TestRunFailedBeforeControlReadyTable` | `internal/agent/bootstrap_test.go` | 表驱动：ctx 取消 → execFn 未调、marker pending；session_gone → 撤权不回滚；`proto_mismatch` 权威拒绝 → execFn 收 Prepared（tasks==1）、child 活；exec 失败 → Thaw 后返回 err | 删 ctx 分支 → 取消臂误再派发 |
| T13 | `TestBeginCrashWindowClosesForeignDescriptorsAndContinues` | `internal/agentboot/boot_exec_test.go` | Prepare pipe 读端 + memfd，Exec 到 helper 但 nonce 改错 → Begin 返回 nil、`closeForeignDescriptors()==2`、`/proc/self/fd` 无 pipe/`memfd:tether-handoff`、父 pipe 写端 EPIPE、helper 退出码 0；**W1b 子臂**：`Exec(tether, ["tether","version"], env)` → stderr 崩溃窗签名、`version` 仍打印、memfd 已关 | 不关 fd → 写端不 EPIPE；Begin 挪回 `if daemon` 块内 → version 臂 memfd 仍在 |
| T14 | `TestFinishBootRedispatchTable` | 同上 | 四臂：held+pending+proof==UpgradeID → execFn 收 prepared、marker redispatched；held+pending+proof≠ 且 TargetInstance==快照 → 同样再派发；proof≠ 且 TargetInstance≠ → Abandon（child 写端 EPIPE）、marker 不动；refused（半取 Salvage）→ `Salvage.Prepare(Borrow…)` 合并后 Prepare 成功（无重复 ID） | Salvage.Prepare 回退为 append → duplicate；删所有权检查 → 第三臂误再派发 |
| T15 | `TestRestoredBorrowRepacksUntakenFiles` + `TestMergeResourcesRefusesDoubleOwner` + `TestSalvagePrepareMergesRestoredRemainder` + `TestFrozenPrepareCarriesRevocationsMarkedAfterFreeze` | `internal/handoff/image_exec_test.go`、`internal/agentimage/restore_test.go`、`freeze_test.go` | §1.4 P1–P5 | 各自 |
| T16 | `TestNodeUpgradeCarriesLiveExecAcrossExec` / `TestNodeUpgradeGenerationChainKeepsLiveExec` / `TestNodeUpgradeDefersOnBusyResource` / `TestNodeUpgradeRefusesLegacyCandidateWithLiveTasks` | `test/cli_e2e/node_upgrade_image_test.go`（真二进制 realStack，无 supervisor，`Setpgid`） | 同 PID、`/proc/exe` sha 换、producer 字节连续、rc 23、marker committed/generation/resource_phase=active/imported_resources=1/freeze_window_ms>0、`image activated` 恰 1 行（校准样本）、§5.2 FD 审计为空、handoff memfd 0；gen1→gen2→gen1 sha 序列与 generation 1→2；PTY 打开时 rc 75 含 `upgrade_deferred` 与 `pty_busy`、无 marker、关 PTY 后成功；候选 fake `version` 无 ABI 行 + 活任务 → rc 64 `image_abi_unsupported`、无 marker、child 继续 | `-overlay` 让 New 持映像时仍 initProcessRuntime → ctl 74（=H0 签名）；Run 先 register 后 Thaw → EXITED(-1)；删 ABI 门 → 走 Prepared.Exec |
| T17 | `TestUpgradeDeferredCodesAreClassified` | `cmd/tether/exitcode_test.go` + coverage 门 | `upgrade_deferred`→75 transient、`agent_upgrading`→75、`image_abi_unsupported`→64 config；`upgrade_deferred` **无** hint 且渲染含 reason 串；`--all` 跳过 | 删表项 → 覆盖门红；登记 hint → 渲染断言红 |
| T18 | `TestBudgetConstantsAgreeWithTheirOwners` / `TestUpgradeBudgetPinControls` / `TestDeferredReasonSetIsTheDocumentedOne` / `TestAgentImageEntryPointsAcceptNoContext`（④ 扩展 agentboot） | 各包 | §6 / §9 | 各自 |

`go test -race -count=3` 覆盖 agent/agentboot/agentimage/handoff/upgradebudget；触碰 runtime/reconcile 的测试带泄漏锚（≥5 轮）。

## §8 drill 34 / 33 / 39

### 8.0 结构裁决
- **34 转 arm-split**（`# arms: A G U N P W F`，fixture N1、grows 0、worst A=300 G=420 U=420 N=360 P=420 W=600 F=420），父行 DERIVED GREEN；`_continuity` 在 A 升 `assert_ok`（描述文本一字不改），`_legacy_continuity` 与 `assert_bug` 调用删除；S1 签名保留在探针 selftest 作 hermetic 负例 + `expected-verdicts-log §34` S1 段落 verbatim（红半收据）。
- **负控制不进 34**（PARENT-JOIN 会把父行钉成 PRODUCT-RED）：新建 `39-hot-update-legacy-n1`（`# arms: X Y`）。Y = exporter=v0.6.0（真旧 binary 作运行映像，dev ctl 回落 legacy exec，`node upgrade` 到 tether-next → 裸 exec）→ PRODUCT-RED，owner prose「hot-update-plan H0 legacy exporter; S8 legacy→capable first migration」，签名预测 `output=epipe management=exited_minus_one ctl=idle_timeout`，**首跑后重钉**（S1 先例）。X = target=v0.6.0（`node upgrade agt1 --url tether-prev`，有活任务）→ `assert_refuses agent_rejected:image_abi_unsupported` + 盘面零变更 + producer 连续 → GREEN（Q1）。39 父行 = PRODUCT-RED（join）。
- **导入失败回滚臂**：同源 artifact 无 switch-free 触发（drill 草案 §6.3 穷举表引用），Mandate 禁开关 → hermetic T3/T14/T1 承担 + W 臂证再派发 API；34 **不**登记 not_covered；exit #2 措辞据此改写；`expected-verdicts-log §34` 写明。
- 34.U = `agentUserUnitBody` 逐字渲染作 `User=sim` **系统**单元（先 `sctl agt1 disable --now tether-agent`，避免 lease 后缀 → `clone_family_upgrade_unsupported`），证 unit-body/Restart=on-failure/RestartSec=5 下同 PID；README 如实标注不证 `systemctl --user`（in-sim user manager 缺口已由 82 U3 拥有）（Q4）。

### 8.1 臂清单

| 臂 | 制造法 | oracle（judge/谓词） | 负控制 | 非空性 | 秒/worst |
|---|---|---|---|---|---|
| **34.A** | 今天流程：producer 活 → `node upgrade` gen1→gen2 | `_continuity` + `_hotupdate`：final ps EXITED(23)、`audit.exec_lines==1`（`agent: exec.*$NONCE`）、`restored_lines==1`（`image activated`）、`deferred_lines==0`、FD 普查按类相等（handoff 0、replay ≥2 且前后相等、pipe/pidfd 相等、children 不变）、`marker.generation==1 ∧ resource_phase==active ∧ imported_resources==1 ∧ freeze_window_ms>0 ∧ state==committed`、`systemctl show -p MainPID,ExecMainStartTimestamp,NRestarts` 逐字相等、租约计数相等、§5.2 审计为空、upgrade.stdout 含 staged/waiting…120s/COMMITTED、broker slog committed outcome | 39.Y；hermetic S1 签名证据被 judge 拒 | baseline ≥20、post 推进 ≥20、freeze_window_ms>0 | 70/300 |
| **34.G** | 同一 producer 跨 tether→tether-next→tether（`artifact_add` 第二 tarball） | `_hotupdate --hops 2`：hop1.new_sha==hop2.old_sha、pid/starttime 全程同、两 marker 各 committed 带 generation 1/2 与 freeze_window_ms、restored_lines==2、exec_lines==1、FD 三点相等；每 hop 前 `_stream_advancing` | hermetic 链断裂 | 同 A | 100/420 |
| **34.U** | body-等价系统单元 | A 判据子集（MainPID/ExecMainStartTimestamp/NRestarts、continuity、marker、FD） | 39.Y | 同 A | 90/420 |
| **34.N** | `sctl agt1 disable --now` → `dexec -u sim agt1 -- sh -c 'setsid nohup /home/sim/.local/bin/tether agent … 1>/dev/null 2>>…/agent.boot.err & echo $! > /tmp/hup-agent-<nonce>.pid'` | PID 取 pidfile（恰 1 个且 cmdline 匹配、exe sha==vendor、`systemctl show -p MainPID --value == 0`）；A 判据子集按 `/proc` starttime；`readlink /proc/$PID/fd/2` 前后同 inode；cleanup 用探针 `stop-agent`（pidfd 钉住） | 39.Y；pidfile 0/2 匹配被拒（r9d） | 同 A | 80/360 |
| **34.P** | ctl1 内 `hot_update_mixed_probe.py run --rate 10`：P0 校准 30s（无升级，`assert_setup`）→ P1 40s、t=10s 处 `node upgrade` | P0：`request_failures==0 ∧ connect_failures==0 ∧ rate≥9`（传感器不饿死）；P1 `assert_ok`：`dup_side_effects==0 ∧ rc0_without_effect==0 ∧ request_failures==0 ∧ connect_failures==0 ∧ stale_offline_failures==0 ∧ in_window≥1`；**只记录**：分类 p50/p95/p99/max、`over_5s`、reliable/legacy 路径计数（`agent: exec … reliable=`）、`agent_upgrading` 计数（单列一类，不计入 request_failures——它是可分类的 75） | hermetic shim 注入重复副作用/失败/STALE stderr | P0 通过 | 170/420 |
| **34.W** | 照 33 B：producer 活（established）→ `fault_synblock_on agt1 4222` + `fault_assert_blackholed` → `node upgrade` → gen2 导入 thaw 但注册不了 → 120s watchdog 重导出**当前态**再派发 prev → `_w_rolled_back` 成立后 `fault_synblock_off` | `_w_rolled_back`：marker rolled_back ∧ Detail 含 `register deadline exceeded` ∧ `resource_phase=redispatched` ∧ `generation==2` ∧ dst==OLD ∧ .prev consumed ∧ exe==OLD ∧ 同 pid/starttime；heal 后 ps 行 LOST→RUNNING、源推进、无 pipe_error、终态 EXITED(23) 由 prev courier 送达、exec_lines==1、restored_lines==2；**ctl 结局只记录**（节点 OFFLINE ~60s 时 `node_offline` 74，#90 边界） | 33 B（无业务）作参考；hermetic restored_lines==1 → 红 | 注入前 `_stream_advancing`、blackholed 自证 | 240/600 |
| **34.F** | 62 的 `_mount_hangfs/_wedge/_heal`：healthy → producer → wedge → `exec --cwd /mnt/hung -- true`（词法 fast-fail 不会 wedged；探测 goroutine 卡 statfs → `ProbesInFlight/Retained=1`）→ `node upgrade` | `assert_refuses "…" "agent_rejected:upgrade_deferred"` + `_deferred_reason`（`remote_fs_probe_in_flight`）+ `_deferred_left_disk_untouched`（dst==OLD ∧ 无 pending marker ∧ exe==OLD ∧ 同 pid ∧ 源/ctl 推进）+ 线程状态普查记录；`_heal` → 再 `node upgrade` → `_continuity`+`_hotupdate` GREEN；诊断记录：hangfs 上阻塞线程时主线程 `execv` 是否 5s 内完成（不判定） | hermetic T6 | wedge 后 `_statfs_blocks` | 110/420 |
| **39.X** | `artifact_up … vendor/tether-prev` + 35 的 `_prev_die_gate`；producer 活 → `node upgrade` 到 v0.6.0 | `assert_refuses … image_abi_unsupported`；dst sha 不变、无 marker、producer 连续、ctl 后续 rc 23 | 34.A 正控制 | fd census 非空 | 90/300 |
| **39.Y** | agt1 运行映像装为 tether-prev；producer 经 dev ctl（回落 legacy，`--timeout 60s`）；`node upgrade` 到 tether-next | `assert_bug "…" "H0 legacy exporter (S8)" '^HOT_UPDATE_LEGACY_EXPORTER output=epipe management=exited_minus_one ctl=… pid=… nonce=…$' _legacy_exporter`（judge 拒绝正 oracle 形状；selftest 双向） | 34.A | fd census | 180/420 |
| **33** | 不改脚本 | INCOMPLETE nc_gap=1（#73）；B4 合取不变（空 bundle 再派发）；A/C/C2 不变 | 既有 | — | 214 |
| **35** | `-j3` 回归（S3 动了 courier 起点/Run 顺序） | GREEN×3 | — | — | 120 |

### 8.2 hermetic 控制与五份基线
- `hot_update_process_probe.py` +`identity`/`fdcensus`/`stop-agent`/`hotupdate`/`legacy-exporter`/`freeze-window` 子命令；selftest：`hotupdate` 正例 + 负例（S1 签名、handoff=1、replay 类变化、pidfd ±1、children 变化、exec_lines 2、restored_lines 0/2、final RUNNING、freeze_window 缺失、`--hops 2` 链断裂）；`fdcensus` 分类 + memfd 名字 pin（读 replay_linux.go:15 / image_linux.go:182 字面量）；日志短语 pin（`image activated`/`image cut`/`upgrade deferred`）对源码 grep。
- 新 `drills/lib/hot_update_mixed_probe.py` + `tests/hot-update-mixed-probe-test.sh/.py`（假 `tether` shim 驱动真实调度器：分类、副作用去重、百分位精确、rate<9 红、窗口两侧、STALE 分类）→ 进 `run-all.sh` 循环（`simcluster_gate_set_test.go` 动态对账）。
- `tests/r9d-nonvacuity.sh` +34/39 谓词两侧：`_deferred_reason`、`_deferred_left_disk_untouched`、`_w_rolled_back`、`_agent_pid_nohup`、`_hotupdate` 包装、`_prev_die_gate`；`lint-drills`：全部谓词写成函数（R-NOSHC）、needle 非空。
- 五份基线：`assert-identity.baseline.tsv`（34 按臂重键 + `._shared`，`assert_bug` 行消失、同描述 `assert_ok` 出现在 `.A`；39 新行；头注释记 primitive swap）、`kept-sites.baseline.tsv`（只增）、`drill-costs.tsv`（删 :72 父行，首跑后加 34.A–F 与 39.X/Y 行）、`expected-verdicts.tsv`（34 父 + 7 子 GREEN；39 父 PRODUCT-RED、.X GREEN、.Y PRODUCT-RED owner prose）+ `-log.md`（§34 S1 段 verbatim + S3 段每臂小节；§39 新）、`docs/reviews/simcluster-coverage-inventory.md` :147 段改写。
- 供给：`drills/lib/artifact.sh` +`artifact_add <binary> <tarball>`（复用 `artifact_up` 打包/sha/复核，只 `d cp` 进已运行 backend，设 `ART2_URL/ART2_SHA`）；`stage.sh` 不改；`logs.sh` 不改。

### 8.3 命令序列与成本预告（实跑前主进程在 plan/会话预告并取得同意；与 go test/lint/gates 严格串行；本机 weilandserver）
1. `cd test/simcluster && sh tests/run-all.sh; echo rc=$?`（≈2–3 min）
2. `./local.sh build`（≈3–5 min）；记 `vendor/tether-source.json`、`docker image inspect tether-sim:dev --format '{{.Id}}'`、HEAD+dirty。
3. `./run-drills.sh -j4 --no-attribute 34-hot-update-process`（7 臂 Σ≈15 min，wall ≈6–7 min）。
4. `./run-drills.sh --no-attribute --allow-product-red 39-hot-update-legacy-n1`（≈4 min；Y 首跑大概率 ASSERT-FAIL 直到重钉，+4 min）。
5. `./local.sh drill 33-node-upgrade-success`（≈4 min）。
6. `./run-drills.sh -j3 35-hot-update-exec-carrier`（≈2 min）。
7. 从 rollup 更新 `drill-costs.tsv` → `sh tests/run-all.sh` 再跑。
**合计 ≈ 30–35 min 墙钟（含一次重钉）**。evidence §14 记：每臂 verdict/秒数/pass、freeze_window_ms 样本表（标「校准数据」）、P 的分布与失败分类、W 的 ctl 结局与 OFFLINE 时刻、F 线程普查、39.Y 签名重钉、33/35 回归。

## §9 闸门与登记

| 闸门 | 动作 |
|---|---|
| test inventory | `go test ./test/determinism/ -run '^TestTestFunctionInventoryOnlyGrows$' -update-test-inventory`（只增；核对新增行数==本 stage 新测试数） |
| 矩阵 golden | `TestLeakGateCoversRiskyPackages` 先跑：agentboot 若被判 risky 则加入 `TestLeakRiskMatrix` 并 `-update-matrix-inventory`（commit message 写理由）；`TestReliableExecMatrix` 的 `-run` 加 `TestNodeUpgrade` |
| 命名冻结 | 文件按单元命名（`bootstrap_test.go`、`upgrade_image_test.go`、`boot_exec_test.go`）；`// origin: S3 plan §x / critique Bn` 锚 |
| promised-guard | 注释点名的测试必须存在（本 plan 的 T 表名即承诺） |
| ctx 账本 | 新 `context.Background()` 站点：`smokeVersion` 已标；`candidateABI` 复用它；agentboot 无 ctx |
| dataplane_lifetime ④ | 扫描列表加 `internal/agentboot`（导出函数 ctx-free）；变异：给 `Begin` 加 ctx → 红（`make gates` 收据） |
| layering | +`internal/agentboot` 禁 import `internal/agent`/`internal/broker`/nats.go；+`internal/upgradebudget` 禁一切 internal；`test/stackharness` 反向扫描不受影响 |
| 结构预算 | commit 前 `go test ./test/architecture/ -run StructuralBudget`；main-noncli 先量（1273→≤1299 否则手改 golden 写理由）；internal/agent 文件 19/20 |
| wire inventory | 应无 diff（零新字段）；`go test ./internal/proto -run Inventory` |
| 错误码 ↔ exit class | `TestErrorCodeCoverage` 双向：3 个新码字面量发射点 + 表项 |
| ACL | 无新 subject/verb；`acl_reconcile_test` 应无 diff |
| CLI golden | `TestCommandTree` 不应变（version 输出不在 golden） |
| 确定性 lint | `image-abi v1` 由 `"image-abi v" + strconv.Itoa(proto.ImageABIVersion)` 拼接，跑 `test/determinism` 版本字面量门核对 |
| image 字段账本 | 应无 diff |
| 新门 | `upgrade_budget_pin_test.go`（`gate-control:` 锚 + 正负控制 + CLAUDE.md 表行 + `gate_registry_test` 对账） |
| Darwin | `GOOS=darwin GOARCH=arm64 go build ./... && make vet-tags`（`crashwindow_other.go`、handoff `image_unsupported.go` 的 `Repack`） |
| hermetic 闸集 | `run-all.sh` 加 mixed-probe-test 一项；`simcluster_log_oracle_test` 无改动 |

## §10 文档清单（压缩）
- `docs/distributed-broker-architecture.md` **§22.6（新，S3 活契约）**：§1.2 顺序、§1.6 状态表、§1.5 崩溃窗、再派发 API 与 2 跳代数、lineage 裁决（env 快照 + bundle 为准）、deferred 11 码表（判据/移除 stage）、gate 语义、marker 字段/值域/写点/日志短语表（唯一命名源）、P9 结算与 N-1 滞留、cut latch 纪律（exported runtime 被 Close = discard）、Freeze-before-reply、心跳前置、ABI 门与运行映像==磁盘门、ReExecOnly 腿契约、已知限制（Thaw 失败毫秒窗、join --start 形态冻结窗含 manifest 预取、W3 杀任务）。§21.3 修订（安装序、exec 失败=Thaw+就地恢复、watchdog 重导出、boot 检查 × bundle）。§22.5 追注（P4/P5、Lineage 由 S3 填）。
- `docs/usage.md` §5.19 `node upgrade`：`upgrade_deferred`(75) 原因码表与处置（含「带 expose/proxy 的节点 S5 前恒 deferred」）、`image_abi_unsupported`(64)、`agent_upgrading`(75)、`resource_phase` 读法、rollback_blocked 运维出口；§9.13 +3 行。
- `docs/deploy-tier-gotchas.md`：新条目「handoff 校验失败 = 崩溃窗 W1」「rollback_blocked 出口」「升级到无 bootstrap 的 binary 被 image_abi_unsupported 拒绝（39.X）」；#73 不动。
- `docs/testing-standards.md` §0.4 追加 cli_e2e 真二进制 + §0.6 三角色 harness；§0.5 追加 `agentboot.FromCut` 注入形状。
- `test/simcluster/README.md`：34 行改写（七臂表）、39 行新增、文件表 +2、`hot_update_budget.sh` 单源说明；`test/README.md` 矩阵表若变。
- `docs/reviews/hot-update-evidence.md §14`、`hot-update-tasklist.md` 勾 C-3/C-7/C-9/C-10（只勾有收据项；C-4 半）、`INDEX.md` 一行；`CLAUDE.md` 闸门表 +1 行。
- `docs/cluster-runbook.md` 一句：ReExecOnly 腿 exec 失败改为保活（若 Q3 采纳）。

## §11 实施顺序（每步可独立 `go test`；drill 最后且串行；编辑中途不跑全量闸）
1. `internal/upgradebudget` + 引用改写（workload/agent/node.go）→ `go test ./internal/upgradebudget ./internal/workload ./cmd/tether -run 'Budget|Upgrade'`。
2. handoff P1–P3 → `go test -race -count=3 ./internal/handoff`（三跳测试）。
3. agentimage P4–P7 → `go test -race -count=3 ./internal/agentimage`。
4. `internal/agentboot`（Image、crashwindow）+ image_test（进程内）→ `go test -race -count=3 ./internal/agentboot`。
5. `instance.go`（`execEnvFor`、New 早期 setExecLineage）+ `exec.go`（`processRuntimeConfig` 抽出）→ `go test ./internal/agent -run 'Instance|Lineage|Runtime'`。
6. `bootstrap.go`（restoreImage/activateImage/FinishBoot/BootUpgradeCheck 扩展/stampMarkerPhase）+ New/Run/register 接线 + T3/T4/T5/T12/T14 → `go test -race ./internal/agent -run 'Bootstrap|ImportFailure|ReconcileSnapshot|SessionDestroyed|FinishBoot|RunFailed'`。
7. `upgrade_image.go`（gate/preflight/cut/心跳/ABI 门）+ upgrade.go 新序 + reExecInPlace + marker 字段 + T6/T7/T8/T8' → `go test -race ./internal/agent -run 'Preflight|UpgradeCut|UpgradeExecFailure|RunExitDuringCut'`；**必跑** `go test ./test/p10/... ./test/security/... ./test/chaos/... -run Upgrade`（UpgradeNoExit 语义）。
8. watchdog/Run 早退/P9 + T10/T11 → `go test -race ./internal/agent -run 'Watchdog|RegisterReceipt'`。
9. proto/cmd：version 第 4 行、3 个码、hint/表/集合 + T17 → `go test ./cmd/tether ./internal/proto`。
10. main.go 三处调用 + agentboot 三角色真二进制 harness T1/T2/T13 → `go test -race ./internal/agentboot -run 'Bootstrap|BudgetExhausted|CrashWindow'`。
11. `test/cli_e2e` T16 → `go test -race -count=1 ./test/cli_e2e -run TestNodeUpgrade`。
12. 新门/登记（§9）→ `go test ./test/architecture/ ./test/determinism/ ./test/e2e/parallel/ -count=1`；改了门则 `make gates`。
13. 变异收据：每条 T 的变异用 `-overlay`（编译输入类）或真改磁盘再还原（扫描源码的门），逐条记 rc。
14. Darwin build + vet-tags → `make test; echo rc=$?` → `make e2e-parallel; echo rc=$?` → `make lint; echo rc=$?` → `make gates; echo rc=$?`。
15. drill 脚本/探针/基线 → §8.3 序列（预告成本、取得同意、串行）。
16. 文档 §10 → evidence → tasklist → INDEX。

## §12 批评处置表（压缩；B=BLOCKER/M=MAJOR/m=MINOR）

| 批评 | 处置 | 落点 |
|---|---|---|
| B：New 内 lineage 被消费后再派发丢 instance（三位批评者） | 采纳：env 快照在 `Begin`（消费前）+ `execEnvFor` + New 早期 `setExecLineage` + bundle lineage 为准的 prev 裁决 | §1.7、T1/T3 |
| B：`Remaining()` 转移所有权与 S12 继续路径矛盾 | 采纳借用语义 `Borrow()` | §1.4 P1、T2'/T15 |
| B/M：tether-prev 负控制放 34 违反 PARENT-JOIN；not_covered 违反 GREEN-NCGAP | 采纳：39-hot-update-legacy-n1（X/Y）；34 只 GREEN 臂 | §8.0 |
| B：session 草案 drills 覆盖不足 | 采纳 drill/deploy 的七臂 | §8.1 |
| M：`handoff.Restore` 只在 daemon argv 下调用 | 采纳：`Begin` 无条件在 main 顶部；W1b | §1.2/§1.5、T13 |
| M：`agent join --start` 站点未接线 | 采纳 boot 形状：New 自取 `agentboot.Claim()`；不加 AST 门 | §3.1 |
| M：reply-first 下 Freeze 非 deferred 错误只 log 不 un-flip | 采纳 freeze-before-reply + 所有 post-flip 失败统一 un-flip | §2.1 步骤 9、T7 |
| M：`runFailedBeforeControlReady` 对 ctx.Err 也回滚 | 采纳排除 | §3.5、T12 |
| M：deploy W3 直接退出杀任务 | 采纳 session 形状：先尝试再派发当前态 | §3.5 |
| M：Busy 钩子 TOCTOU（PTY/legacy 撞冻结表被杀） | 采纳 session 的 `upgradeGate`（含 inflight 作数据源） | §4.0、T6 |
| M：`upgrade_deferred` 登记 hint 会吞原因 | 采纳：不登记，表旁注释；drill grep Error 文本 | §4.2、T17 |
| M：rollback_blocked 无界重试让第二次 install 换 .prev | 采纳：入口门加 `rollbackInFlight` | §2.1 步骤 1、T11 |
| M：S3 exporter 对不认 handoff 的目标带 bundle exec（secret/fd 泄漏） | 采纳 ABI 门（`image-abi v1` 第 4 行 + `image_abi_unsupported`）；空映像才裸 exec | §2.1 步骤 6、T16 |
| M：Freeze 后落地的撤权不在 bundle（Prepare 不重 Export） | 采纳 P5：Prepare 重 Export，且 Prepare 紧贴 Exec；7(c) 两个码 | §1.4 P5、§3.3、T5 |
| M：混合探针零失败判据撞 STALE 窗（真实产品缺口） | 采纳心跳前置 + 探针单列 `stale_offline_failures==0` | §2.1 步骤 8、34.P |
| M：Restore 失败后 `restored.Repack()` 的 State.Data 为空 | 采纳：refused 态 Repack 走 salvage | §1.3、T3 |
| M：`Runtime.Close` 对 frozen&&exported 静默 discard，与 Run defer 竞争 | 采纳 `cutMu` latch | §2.5、T8' |
| M：flow_busy/ss_busy 宽松判据违反 §3-4 | 采纳严格；代价写 usage | §4.1 |
| M：deploy cut-before-reply 但 UpgradeNoExit 早返回不 Thaw | 采纳：`cut.abandon` | §2.2 |
| M：Thaw 失败 Detach 误读 I11 | 部分采纳：Thaw 失败结构上不可达，best-effort Detach + 再派发，毫秒窗如实登记 | §3.2 |
| M：B 臂「追加字节」同 tag → UNCONFIRMED | 采纳 A→B→A | §5.4、34.G |
| M：boot H 臂阻断时序 | 采纳 33 B 形状（先阻断） | 34.W |
| M：34.U 假设 user manager | 采纳 body-等价系统单元 | §8.0、Q4 |
| M：四份命名不一致 | 采纳 §22.6 唯一命名表 + 探针 pin | §2.4、§8.2 |
| m：所有权证明只看 pending（deploy）/ shim 跳过时 proof 空（boot） | 采纳合取判据 | §1.7、T14 |
| m：T1 第二个变异不成立 | 采纳：改为 W1b/fork 可观测差分 | T13 |
| m：`-ldflags buildFault` drill 开关 | 驳回该臂（Mandate） | §8.0 |
| m：PrevImageABI 门恒真 | 驳回该字段；改为运行映像==磁盘门 | §2.1 步骤 3 |
| m：pkg-files 19/20 | 采纳：两个新文件、S4/S5 在既有文件/子包长 | §1.1 |
| m：hangable Home 在 Thaw 前的无界读 | 采纳有界探测进 `remote_fs_probe_in_flight` | §4.1 |
| m：ReExecOnly 无回滚、2 跳上界为 0 | 采纳如实文档；preflight 对该腿同样严格 | §2.3、§10 |
| m：冻结窗组成漏 shim fsync | 采纳 `shim_ms/import_ms` 字段 | §3.2 |
| m：prev 导入失败「保 FD 不 Close」比 H0 差 | 采纳 boot 的 Abandon+非零退出 | §1.5 W2 |
| m：`/proc/pid/syscall` 需 ptrace 权限 | 采纳 stat/wchan + ledger 停滞 | 39.Y judge |
| m：replay memfd 钉 3 可能假红 | 采纳 ≥2 且前后相等，首跑后钉 | §5.2 |
| m：34.I（崩溃窗第二跳）无 verdict 值 | 驳回入 S3 drill（S8 注入矩阵） | — |
| m：join --start 的 manifest 预取在冻结窗内 | 采纳登记已知限制 | §10 |
| m：并发 ReExecOnly ErrFrozen → install_failed | 采纳 TryLock | §2.3 |
| m：成本估计偏低 | 采纳 drill 草案 30–35 min | §8.3 |

## §13 残余风险
1. 首批 `freeze_window_ms` 样本可能 >500ms（shim 的 20MiB sha + 2×fsync + Cobra/YAML；S2 -race 下 163ms 同源）：只记录、按段归因（`shim_ms/import_ms`），候选优化（marker 记 NewSHA+size+mtime 先比对）登记为 S8 项，不抬门。
2. 现网所有带 expose/proxy 的节点在 S5 前 `node upgrade` 恒 deferred（严格判据）；usage 明写，否则运维当缺陷。
3. W3：Thaw 后 register 权威拒绝且再派发 exec 失败 → Close 杀任务后退出（今天的形状）；如实登记。
4. `agent join --start` 形态：re-exec 会重跑 join（invite 可能单次）→ 大概率再派发 → prev 同样失败 → W2；S3 不在 drill 覆盖，文档登记（今天已如此）。
5. 39.Y 签名是预测（ctl 终态取决于 dev ctl 的 legacy 回落行为），首跑后重钉；预算一次额外 4 min。
6. 34.P 的 10/s 探针每秒 ~10 条 auth_callout 连接；P0 已红 = SETUP-RED（传感器饿死），不得降速买绿。
7. `main-noncli` 距量子边界 27 行；三个新码的 hint/表项可能跨 1300 → 手改 golden 写理由。
8. gate 关闭窗（≤500ms）内 legacy ctl 得 `agent_upgrading`(75)：N-1 窗口本不承诺连续，可分类比静默挂起诚实；drill 探针单列。
9. cut latch 让 SIGTERM 落在 Freeze→exec 之间时 exec 仍进行（新映像不知道 SIGTERM）→ systemd TimeoutStopSec 后 SIGKILL；≤1s 窗，登记。
10. 进程内 agent 级测试的共享 child（A 冻结表 + B 恢复表）：cleanup 顺序错 = 泄漏门/-race 假红；`FromCut` 必须 dup 文件。
11. drill 与 go test/lint 并发会把健康产品报红（记忆条目）；七臂 -j4 期间宿主负载显著，P 臂分布会被污染——串行、不留残留容器。

## §14 需主进程裁定（≤5，附推荐）

| # | 问题 | 推荐 |
|---|---|---|
| Q1 | `node upgrade` 到不认 handoff 的候选（v0.6.0）：S3 就加 ABI 门拒绝（`image_abi_unsupported`，39.X 变 GREEN-refusal，S8 exit #4 的「capable→legacy 明确拒绝」提前吸收），还是不拒绝、让 39.X 以 PRODUCT-RED + 新 gotcha 钉住泄漏（含 registry HMAC secret memfd 进入旧映像及其每个 child、producer 阻塞而非 EPIPE）？ | **拒绝**。不拒绝等于 S3 自己引入一个秘密泄漏与比 H0 更差的 stall，违反「本版彻底修复」；exit #2 的 PRODUCT-RED 由 39.Y（exporter=legacy）承担。 |
| Q2 | W1（`handoff.Restore()` 自身失败）：关外来 fd 后**继续作为普通启动**（节点可达、空表 register、H0 结局、marker lost），还是 `os.Exit` 非零让 supervisor 收敛（nohup 节点死）？ | **继续**（与 S12「NAT 后唯一活进程绝不 exit」同向；两者都是可判定的 unknown 窗并进 S8 注入矩阵）。 |
| Q3 | ReExecOnly（cluster upgrade 共址腿）exec 失败：保持 `os.Exit(1)`（`upgrade_state_test.go:488-495` 钉住；systemd control-group 停止会杀光刚 Thaw 的活业务），还是 Thaw 后保活并记录（cluster upgrade 驱动器靠运行 SHA 复核判该腿失败）？ | **保活**；改契约测试 + cluster-runbook 一句。 |
| Q4 | 34.U：body-等价系统单元（`agentUserUnitBody` 逐字 + `User=sim`，GREEN，README 标注不证 `systemctl --user`），还是在 U 臂做 dbus+logind+linger 供给 spike 并接受 34.U/父行 INCOMPLETE？ | **body-等价**；真 user manager 需改 sim 镜像，归 S8 no-systemd/user-unit 复核。 |
| Q5 | 冻结窗内的 legacy exec/run/push/pull/expose：`upgradeGate` 拒绝为 `agent_upgrading`(75，新 wire code)，还是不加 gate、让 ≤500ms 内的 legacy 请求撞冻结表（子进程被 SIGKILL、ctl 得 exec_failed、exit(-1) 落在已导出 outbox 之外）？ | **加 gate**（它同时是 legacy_session_busy/transfers_in_flight 的数据源，关掉「检查过了才新建」的 TOCTOU）。 |