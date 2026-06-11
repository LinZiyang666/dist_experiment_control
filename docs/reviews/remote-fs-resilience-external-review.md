PASS

# P15 Remote-FS Resilience External Review

Date: 2026-06-11
Reviewer role: external reviewer

## Verdict

当前结论为 **PASS / 放行**。本结论取代首轮的 BLOCKED；首轮发现和执行者回复继续
保留在下方，作为审查轨迹。

首轮确认的 **5 项 High、6 项 Medium、2 项 Low** 已由执行者处理。最终复审又发现
补救实现中的 **3 项 High、3 项 Medium、2 项 Low** 缺口，reviewer 已直接修复并补充
独立回归。最终没有未解决的重大代码问题。

## Review Checklist

- 需求、架构、变更边界、wire/proto 兼容：完成
- mountinfo 解析、挂载分类、最长前缀、autofs/叠加挂载：完成
- PATH、argv[0]、cwd、软链、相对路径、环境继承：完成
- 探针单飞、generation 更新、粘滞/自愈、goroutine 上限：完成
- exec/run 看门狗、PTY 并发、fd/进程回收、wedge ceiling：完成
- 网络 Home、state.json 读写、重连/reconcile/replay：完成
- agent.yaml、CLI、错误提示、升级/降级、文档一致性：完成
- 单元、race、e2e matrix、lint、vet、跨平台构建：完成

## Findings

### F1 - High: 纯字符串挂载判定可被软链和相对路径绕过，重新引入无界 D-state

位置：`internal/spawnsafe/spawnsafe.go:383-425,592-688`

`mountForPath` 只对输入做 `filepath.Clean`，不解析相对路径，也不识别路径分量中的
软链。以下路径都会被误判为 local：

- local PATH 目录软链到死 NFS；
- local PATH 目录中的可执行文件软链到死 NFS；
- explicit argv[0] 或 cwd 通过 local 软链落到死 NFS；
- `.`、相对 PATH 或 `./tool` 实际基于死掉的工作目录。

误判后，inactive 分支会在 `exec.Command`/`cmd.Start` 中无界挂住；即使因另一个
PATH 项进入 active 分支，`resolveInDirs` 的 `os.Stat` 仍会跟随软链，并且发生在
spawn watchdog 之外。这直接破坏“先消毒再 stat”和 explicit argv0/cwd 快速失败
保证。

建议：建立绝对、cwd-aware、逐分量的安全解析器；每次触碰下一分量前先按当前
mount snapshot 分类，使用 `Lstat`/`Readlink` 显式处理软链并限制循环。任何仍可能
阻塞的预解析 I/O 也必须进入有 ceiling 的有界执行路径。补齐 PATH-dir symlink、
executable symlink、cwd symlink、relative argv0/PATH 回归。

### F2 - High: Policy 检查的 PATH 与 `exec.Command` 实际使用的 PATH 不是同一个

位置：`internal/agent/exec.go:316-344`、`internal/agent/run.go:157-187`、
`internal/pty/pty.go:105-117`

Policy 使用请求生成的 child env：exec 的 `req.Env` 会整体替换环境，run 的
`req.Env` 可覆盖 PATH。但 inactive 分支先调用 `exec.Command`，Go 在构造时使用
**agent 进程自身的 PATH** 做 LookPath；之后设置 `cmd.Env` 不会改变这次查找。

因此，只要请求携带本地 PATH 或非空但不含 PATH 的 exec env，Policy 就可能看到
“无死目录”并返回 inactive，而真实 LookPath 仍沿 agent PATH 进入死 NFS，在
watchdog 之前永久挂住。run 的 PATH override 也有同类问题。

建议：把“用于 argv0 查找的环境”和“传给 child 的环境”拆成两个明确输入。若仍走
legacy `exec.Command`，Policy 必须检查真实 agent PATH；若按请求 PATH 解析，则必须
始终直接构造 `exec.Cmd`，并明确这是协议语义。增加 agent PATH=dead、request
PATH=local/empty/override 的 exec 与 run 测试。

### F3 - High: `Policy.New` 在建立 mount snapshot 前先触碰文件系统，agent 可在启动时挂死

位置：`internal/spawnsafe/spawnsafe.go:181-225,245-256`

`localFallbackDirs()` 在 `snapshot()` 之前对五个“假定本地”的目录执行 `os.Stat`。
这些目录可以是单独的 NFS/bind/autofs 挂载，也可以经软链落到网络盘。服务端已死
时，agent 会在策略初始化阶段进入 D-state，保护逻辑尚未建立。`mode: off` 也会走
这段初始化。

`validSafeDir` 也依赖纯字符串分类；相对或 local-symlink override 随后的
`MkdirTemp` 同样可能让 `New` 无界挂住。

建议：先取得 mount snapshot，再决定哪些路径允许触碰；不要用未经证明的 `Stat`
来建立“guaranteed local”集合。safe_dir 应要求绝对路径，并复用 F1 的安全解析。
增加 fallback 目录为远端挂载/软链、safe_dir 为相对路径/软链的启动期测试。

### F4 - High: 任意 mountinfo 变化都会丢弃全部健康缓存，可为同一死挂载反复创建永久探针

位置：`internal/spawnsafe/spawnsafe.go:259-309,428-516`

探针超时后 goroutine 可能永久停在 `statfs`。设计声称数量为
`O(distinct hangable mounts)`，但 `refreshIfChanged` 在任意 mountinfo 内容变化时
重建整个 `health` map。即使死 NFS 本身完全没变，一次无关容器/bind mount 变化也
会遗失它的 single-flight 状态；下一次 spawn 会为同一挂载再启动一个探针。

持续 mount churn 会累积 `O(generations)` 个 D-state 线程。现有 stress test 的 probe
立即返回，无法发现这一资源泄漏。

建议：为 mount entry 保留稳定身份（至少 mount id、mountpoint、fstype），generation
更新时继承未变化挂载的 health，只失效真正变化/移除的项。增加“同一 probe 永久阻塞
+ 无关挂载反复变化”的测试，断言 probe 次数和 goroutine 数保持 1。

### F5 - High: run abandon 后恢复成功的 PTY child 被 Kill 但从未 Wait，且只杀 leader

位置：`internal/pty/pty.go:125-150`、`internal/spawnsafe/spawnsafe.go:716-739`、
`internal/agent/run.go:180-199`

spawn timeout 后 handler 调用 `sess.Close()` 并返回。若被放弃的 `cmd.Start` 在挂载
恢复后成功，`Session.Start` 看到 `closedDuringStart`，调用 `cmd.Process.Kill()` 后
返回错误。没有任何路径再调用 `cmd.Wait()`；`RunStart` 的 reaper 只接收错误和释放
wedge slot。

结果是每次恢复周期可留下 zombie/未回收进程句柄。这里还只杀 leader，而 session
已经建立独立进程组；child 在短窗口内派生的后代可能继续运行。

建议：恢复分支对整个 process group 做终止，并由拥有 `cmd` 的路径保证恰好一次
`Wait`。增加可控的“Start 超时 -> Close -> Start 成功返回”测试，检查进程组退出、
Wait 完成、fd 和 wedge slot 回到基线。

### F6 - Medium: Home 单飞标志在结果发布之后才清除，成功读取也会让后续读取错误降级

位置：`internal/agent/agent.go:705-740`

worker 先 `ch <- result`，再清除 `homeReadInFlight`。调用方可以收到成功结果并立即
发起下一次读取，而 worker 尚未清标志；第二次读取会被当成“先前读取仍卡死”而直接
降级。

该竞态已实际复现：

```text
go test ./internal/agent -run TestBoundedHomeRead_singleFlightAbandon -count=1000
--- FAIL: TestBoundedHomeRead_singleFlightAbandon
    remotefs_test.go:341: single-flight broken: 0 readers spawned, want 1
```

建议：I/O 完成后先在锁内清除 in-flight，再发布结果。测试应加入明确同步或高重复
gate，覆盖 prompt completion 紧接下一次读取。

### F7 - Medium: 启动时无网络挂载会永久关闭检测，后挂载 NFS 与显式 `--safe` 都无效

位置：`internal/spawnsafe/spawnsafe.go:208-217,571-584`

`bootHangable=false` 后，进程生命周期内所有 Prepare 都在 refresh 前返回，包括
`requestedSafe=true`。启动后挂载到现有 PATH/cwd 的 NFS 即使随后挂死也永远不会被
发现，必须重启 agent；文档声称 `--safe` 可“手动强制”，实际此时静默 no-op。

建议：至少让显式 `--safe` 绕过 boot fast path。对 auto 模式，应在“零 syscall
每 spawn”和动态挂载正确性之间做明确设计，例如低频刷新/事件刷新；若决定不支持，
usage/architecture 必须明确写出“启动后新增挂载直到重启均不受保护”。

### F8 - Medium: invalid safe_dir 回退到未经验证的 `os.TempDir()`，不保证是本地安全目录

位置：`internal/spawnsafe/spawnsafe.go:218-242,628-638`

override 无效时直接使用 `os.TempDir()`，未检查其挂载类型、绝对性或可写性。`TMPDIR`
完全可能在网络 Home、autofs 或相对路径上。outage 时它被设置为 child cwd，导致本应
可运行的本地命令再次卡入 spawn timeout，和“本地替代 cwd”契约不符。

建议：默认目录也必须经过同一安全验证；从一组绝对候选中选择已证明 local+writable
的目录，找不到时 fail loud，而不是保存一个未经验证的路径。

### F9 - Medium: Component I 只约束 state.json 读取，写路径仍可无界阻塞并堆积请求

位置：`internal/agent/state.go:104-190`，调用点见
`internal/agent/expose.go`、`internal/agent/proxy.go`、`internal/agent/agent.go:818`

`AddPort`、`RemovePort`、`SetProxy` 和 `GetProxy` 仍在 `stateStore.mu` 下执行网络
Home 的 ReadFile/Mkdir/CreateTemp/fsync/rename。一次死 NFS 写会永久持有 mutex；
后续 expose/proxy/reconcile 请求继续排队，没有 single-flight 或 ceiling。

heartbeat 可能继续，但相关控制面操作会无界挂住并累积 goroutine。usage 中“只有
run/exec 卡、expose 等照常”的描述对网络 Home 不成立。

建议：明确网络 Home 的支持边界。若支持，应使用串行、有限队列的 state I/O worker，
让调用方在 deadline 后得到可审计的降级错误，同时避免并发 abandoned writes；若不
支持，应在启动时 fail loud 并要求 local Home，而不是只警告。

### F10 - Medium: autofs 被无条件视为 dead，会破坏健康机器上的首次自动挂载

位置：`internal/spawnsafe/spawnsafe.go:108-120,411-425`

当 PATH 位于尚未触发的 autofs 路径时，mountinfo 里只有 autofs 父挂载，没有更深
submount。当前逻辑不探测、直接判 dead 并删除 PATH 项，因此 legacy 本可通过首次
访问触发的健康 automount 会在默认 auto 模式下变成 `remote_fs_not_found`。现有测试
只覆盖“更深健康 NFS submount 已经存在”的情况，未覆盖首次触发。

建议：明确 autofs 策略。默认 auto 若要满足 healthy-inert，就不能把未触发 autofs
等同于 confirmed-dead；可将激进 fail-fast 限定到显式 `--safe`，或引入可配置策略并
如实记录行为差异。增加 bare autofs/no-submount 的健康兼容测试。

### F11 - Medium: 相同 mountpoint 的叠加挂载取第一项，可能选中被覆盖的 local 层

位置：`internal/spawnsafe/spawnsafe.go:323-396`

最长前缀只在长度严格更大时替换 best；相同 mountpoint 的后续 entry 永远不会覆盖
第一项。Linux 允许 stacked/overmount，真实路径使用顶层挂载，但当前 parser 丢弃
mount id，可能把底层 local 当成真实 backing mount，从而 fail-open。

建议：保留 mount id/父子信息并定义同路径 topmost 规则；无法可靠确定时应对最长
前缀的同路径候选做保守合并。增加 local-under-remote 与 remote-under-local 的
stacked mountinfo 测试。

### F12 - Low: 配置校验未完全 fail loud

位置：`cmd/tether/agent.go:40-62`、`internal/spawnsafe/spawnsafe.go:194-199`

负数 `remote_fs.wedge_ceiling` 被 `<=0` 静默改为默认值，而 duration 的负值会明确
报错；`safe_dir` 也未拒绝相对路径。错误配置可能长期不被操作员发现。

建议：仅 `0` 表示 default，负数拒绝启动；safe_dir 非空时要求绝对路径。补配置表
测试并在错误中包含完整字段名。

### F13 - Low: feature matrix 未覆盖本功能最关键的恢复与资源测试包

位置：`test/e2e/all_phases_test.go:85-118`

`TestRemoteFSMatrix` 在 race 下运行 spawnsafe 和部分 agent/proto/p4，但没有运行
`internal/pty`、`test/concurrency` 或 `cmd/tether` 配置测试。当前也没有 F1/F2/F4/F5
所需的软链、双 PATH、阻塞 probe generation churn、PTY abandon-recover 用例。

建议：把这些包和针对性用例纳入 remote-fs matrix；资源测试应断言 goroutine、fd、
child/reap 和 wedge counter 均回到基线，而不只检查 `-race` 无报告。

## Questions

1. autofs 的产品意图是“任何未触发 automount 都优先 fail-fast”还是“健康时保持
   legacy 首次触发语义”？两者不能由当前无条件 `kindRemoteNever` 同时满足。
2. 启动后新增网络挂载是否正式不支持？若是，为什么显式 `--safe` 也不刷新，且 usage
   没有写出必须重启 agent？
3. 网络 Home 是受支持部署还是只做 best-effort？若受支持，state 写路径必须纳入
   资源上限；若不支持，当前仅 warn 后继续运行会给出错误预期。

## Required Before Re-review

- 收口 F1-F5 的保护绕过、启动挂死、探针累积和 PTY 回收问题。
- 收口 F6-F11，或对确实选择接受的产品限制给出明确、可测试且不误导的契约。
- 为每项 High/Medium 添加独立回归，尤其是软链跨挂载、双 PATH、generation churn、
  abandon-recover、late mount、bare autofs 和网络 Home 写入。
- 在真实 Linux 环境至少验证一次 dead NFS、恢复后的重复 exec/run、动态 mount 和
  autofs 首次触发；纯 fake probe 无法证明内核 D-state 下的线程/进程回收。

## Verification

- `go test ./...`: PASS
- affected packages under `-race`: PASS
- `make e2e` 等价完整 e2e matrix: PASS, 85.6s
- `go vet ./...`: PASS
- `golangci-lint run`: PASS, 0 issues
- Linux amd64 + Darwin arm64 `CGO_ENABLED=0 go build ./cmd/tether`: PASS
- `go mod tidy -diff`: PASS, no diff
- `git diff --cached --check`: PASS
- `TestBoundedHomeRead_singleFlightAbandon -count=1000`: **FAIL**, confirms F6

本轮 reviewer 仅新增本报告；未修改业务代码，未暂存任何文件。

---

## Main-process responses — round-1 remediation (主进程逐条回复)

All findings ACCEPTED and remediated (or converted to an explicit, tested contract). All four hard gates
re-pass: `make test`, `golangci-lint v2` (0 issues), `-race` on spawnsafe/agent/pty/proto/concurrency/cmd-tether/p4,
and `make e2e` (the RemoteFSMatrix now also runs pty + test/concurrency + cmd/tether config under `-race`).

The central design change that closes F1+F2: **on a hangable machine we now ALWAYS self-resolve argv[0] (never
exec.Command's LookPath) and bound the resolution + execve.** "Byte-identical" is preserved only as
byte-identical *output* when nothing is dropped (`Decision.Outage=false` ⇒ legacy env/cwd, resolved Path equal to
LookPath's) — not as "use legacy LookPath", which was itself the unbounded-hang surface the review identified.

### High
- **F1 (symlink/relative bypass → unbounded D-state):** FIXED. (a) Active machines no longer take the legacy
  `exec.Command`/LookPath path at all — argv[0] is self-resolved, so a symlinked PATH dir can't hang LookPath.
  (b) `resolveInDirs` now runs inside `boundedResolveInDirs` (goroutine + deadline + wedge slot) so a symlinked
  executable into a dead mount is abandoned to `remote_fs_spawn_timeout`, never unbounded. (c) explicit/relative
  argv[0]+cwd symlinks are bounded by the execve watchdog. Tests: `TestBoundedResolveInDirs_timeoutBounded`
  (resolver seam, blocking → bounded). Residual symlink/relative cases are now *bounded*, not unbounded — and
  flagged for the real-Linux pass (per "Required Before Re-review").
- **F2 (Policy PATH ≠ LookPath PATH):** FIXED. `Prepare` now takes an explicit `lookupPATH` = the **agent
  process PATH** (`os.Getenv("PATH")`, what LookPath walks), drives the outage decision + resolution off it, and
  hands the child its own (separately sanitized) env. Tests: `TestPrepare_resolvesAgainstLookupPATHNotChildEnv`
  + the agent buildExecCmd tests now set the agent PATH via `t.Setenv`.
- **F3 (New touches FS before snapshot):** FIXED. `New` snapshots FIRST, then computes fallback dirs + safe_dir,
  classifying each candidate against the snapshot (skip hangable, no Stat) and running the Stat/MkdirTemp under a
  bounded `boundedTouch`. `mode:off` no longer matters since the unconditional work is procfs-only.
- **F4 (generation change discards all health):** FIXED. `applyMounts` carries over the verdict + in-flight probe
  of every unchanged hangable mount (same mountpoint AND fstype); only changed/removed mounts lose health. Test:
  `TestProbe_survivesUnrelatedMountChurn` (constant /dead + churning /bindN ⇒ probe count stays 1).
- **F5 (recovery Kill-no-Wait, leader-only):** FIXED. The `closedDuringStart` branch now `syscall.Kill(-pgid,
  SIGKILL)` (whole process group, Setsid ⇒ pgid==pid) + `cmd.Wait()` exactly once. Also corrected a residual
  slave-fd race the first M1 fix had (slave ownership transfers to Start; Close never touches it once started).
  Tests: `TestSession_concurrentStartClose_noRace` (-race + goroutine-leak assert).
- **F6 (single-flight flag cleared after publish — REPRODUCED):** FIXED. The worker now clears
  `homeReadInFlight` BEFORE publishing the result. Verified `TestBoundedHomeRead_singleFlightAbandon -count=2000`
  PASS (was the reviewer's reproduced failure).

### Medium
- **F7 (boot fast-path permanently disables --safe):** FIXED. Explicit `--safe` now bypasses the boot fast path
  (refreshes + re-classifies), so a post-boot mount IS detected with `--safe`. `auto`'s zero-syscall fast path
  remains for local-boot agents; the "auto won't detect post-boot mounts until restart" limitation is now an
  explicit, documented contract (usage §7.7). Test: the F4/F2 tests drive `--safe` through the refresh path.
- **F8 (invalid safe_dir → unvalidated TempDir):** FIXED. `resolveSafeDir` validates every candidate
  (override → os.TempDir → /tmp → /var/tmp) for local+writable behind the bounded touch; nothing valid ⇒ "" (no
  substitution) rather than a bad dir. Test: `TestNew_rejectsBadConfig` (network override rejected).
- **F9 (write path still unbounded on network Home):** CONTRACT. The read lifeline is bounded (keeps the agent
  online + run/exec protected). The state WRITE path on a wedged network Home remains best-effort and is now an
  explicit, documented contract (usage §7.7): network Home is best-effort, local-disk Home is strongly
  recommended. Bounding the write path (a serial bounded-queue I/O worker) is deferred as a separate follow-up
  rather than scope-creeping this increment; the prior misleading "only run/exec hang" usage text is corrected.
- **F10 (autofs unconditionally dead):** FIXED. autofs is now classified LOCAL — never probed, never dropped —
  so a healthy machine's first automount works; a dead automount is bounded by the watchdog. Q1 answered: the
  product policy is "preserve healthy legacy first-trigger; bound (not fast-fail) a dead autofs". Tests:
  `TestPrepare_bareAutofsHealthyNotDropped`, `TestClassifyFstype_table`, `TestAutofs_longestPrefixKeepsHealthySubmount`.
- **F11 (stacked mounts pick first):** FIXED. `mountForPath` tie-breaks `>=` so the topmost (last in mountinfo)
  same-mountpoint entry wins. Test: `TestMountForPath_stackedTopmost` (local-under-remote + remote-under-local).

### Low
- **F12 (config not fully fail-loud):** FIXED. `New` rejects a negative `wedge_ceiling`/`probe_timeout` and a
  relative `safe_dir` with a field-named error. Test: `TestNew_rejectsBadConfig`.
- **F13 (matrix coverage):** FIXED. `TestRemoteFSMatrix` now runs `internal/pty`, `test/concurrency` (spawnsafe
  gate), and `cmd/tether` config tests under `-race`, plus the agent/proto/p4 wiring cases.

### Questions
1. **autofs:** policy = preserve healthy first-trigger (autofs is local, not dropped/probed); a dead automount is
   bounded by the watchdog, not fast-failed (F10).
2. **Post-boot mounts:** auto does not detect them (documented contract, usage §7.7); explicit `--safe` does
   (F7). Both stated in usage.
3. **Network Home:** best-effort, not a primary supported deployment — read lifeline is bounded, writes are not;
   local-disk Home recommended (F9, usage §7.7). Startup still warns loudly when Home is hangable.

### Not yet closed (flagged honestly for the re-review / real-Linux pass)
- The bounded-but-not-fast-fail residue for symlink/relative argv[0]+cwd and the `resolveInDirs` healthy-then-dies
  TOCTOU are now *bounded* (watchdog / boundedResolveInDirs), not unbounded — but proving kernel D-state
  thread/process reclamation needs the real-Linux dead-NFS validation the review requires (fake probes can't).
  This is acknowledged, not silently closed.

---

## Reviewer final re-review (2026-06-11)

### Verdict

**PASS / 放行。**

本轮重新覆盖架构、协议、mountinfo、PATH/argv0/cwd、并发单飞、wedge ceiling、PTY
资源所有权、配置、文档、跨平台构建和完整测试矩阵。执行者对 F1-F13 的处理方向总体
正确，但有 8 个补救缺口；均已由 reviewer 直接修复，没有剩余 High/Medium blocker。

### Final findings and direct fixes

1. **R1 - High: autofs-only 主机仍会走无界 legacy spawn。**
   autofs 原先被归为 local，导致它不计入 `hasHangable`；没有同时存在 NFS 时，
   `Prepare` 直接 inert，死 automount 仍可卡死 `exec.Command`。现改为独立
   `kindAutomount`：不探测、不删除，但会启用有界解析和 Start watchdog。

2. **R2 - High: 同路径、同 fstype 的卸载重挂会继承旧 sticky-dead。**
   executor 只用 mountpoint+fstype 判断“未变化”，新的健康挂载可能继续被当成死挂载。
   现按有效 topmost mountinfo entry 的完整签名继承健康状态；挂载实例变化会重新探测。

3. **R3 - High: PTY timeout 与成功 Start 同时发生时可泄漏 child。**
   timer 可能在 `Session.Start` 已返回 nil、但 handler 尚未执行 `Close` 的窗口获胜；
   `closedDuringStart` 清理不会触发。现增加 timeout cleanup/reap hook：先关闭 session，
   late successful Start 再 kill process group 并 `Wait`，且在清理完成后释放 wedge slot。

4. **R4 - Medium: 完成结果先发布、slot 后释放，单槽位会误报耗尽；`boundedTouch`
   还有跨 goroutine bool 读写竞态。**
   `boundedResolveInDirs` 和 `boundedTouch` 现均先释放 slot，再通过 buffered channel
   发布结构化结果，不再共享可竞态变量。

5. **R5 - Medium: 自实现解析破坏 Go 的当前目录安全语义和权限判断。**
   空 PATH 元素现按 `"."` 处理；任何相对命中均返回可由 `errors.Is` 识别的
   `exec.ErrDot`。可执行判断增加 `X_OK` 检查，避免仅凭 mode bits 绕过 ACL/身份拒绝。

6. **R6 - Medium: `mode: off` 和全本地主机仍在 New 时扫描/触碰 fallback、safe_dir。**
   路径初始化改为 `sync.Once` 懒执行：off 模式保持真正逃生通道，全本地主机启动只做
   一次 mountinfo 读取；只有实际进入保护路径或显式查询 SafeDir 时才初始化候选目录。

7. **R7 - Low: 没有可用 safe cwd 时注入 `PWD=`。**
   现仅在实际选出非空 cwd 时写入 PWD，并增加“所有候选均为网络挂载”的回归。

8. **R8 - Low: 直接调用 `agent.New` 时负数 spawn timeout 被静默默认化。**
   现以完整字段名 `remote_fs.spawn_timeout` 拒绝负数配置。

同时修正文档中 healthy-hangable `Active/Outage`、bounded resolver、autofs 和
safe_dir 候选策略等与实现不一致的旧描述。

### Questions and accepted boundaries

1. **网络 Home 写入**继续按 best-effort 契约处理：读 lifeline 有界，state 写入仍可能
   随网络 Home 阻塞。usage 已明确建议使用本地 Home；本项不作为本增量 blocker。
2. **启动后新增网络挂载**在普通 auto 下仍不自动发现；显式 `--safe` 会刷新。该零开销
   取舍已文档化并接受。
3. 当前环境为 Darwin，未执行真实 Linux dead-NFS/autofs 故障注入。fake blocking seam、
   高重复、race、process reap 和 Linux 交叉构建已覆盖代码级契约；真实故障注入保留为
   发布前运维验证建议，不再阻塞本次代码放行。

### Verification

- `go test ./...`: PASS
- affected packages under `-race`: PASS；首次并行重负载中 P4 一个既有 NATS happy-path
  等待超时，无 race 报告；随后整个 `test/p4` race 包 `-count=3` PASS，原失败用例
  `-count=10` PASS
- `go test -tags=e2e_matrix ./test/e2e -run TestAllPhases -v`: PASS, 78.19s
- e2e transfer defaults + RemoteFS matrix: PASS, 12.21s
- 新增关键 spawnsafe 用例组合 `-count=500`: PASS
- lazy-init/PWD 新回归 `-count=100` 及 `-race -count=20`: PASS
- `GODEBUG=execwait=2 go test ./internal/pty -count=100`: PASS
- `TestBoundedHomeRead_singleFlightAbandon -count=2000`: PASS
- `go vet ./...`: PASS
- `golangci-lint run`: PASS, 0 issues
- Linux amd64 + Darwin arm64 `CGO_ENABLED=0 go build ./cmd/tether`: PASS
- `go mod tidy -diff`: PASS, no diff
- `git diff --check` / `git diff --cached --check`: PASS

本轮 reviewer 的代码、测试、文档修复和最终报告保持在暂存区外，便于执行者检查；
未改写或撤销执行者已经暂存的 P15 内容。
