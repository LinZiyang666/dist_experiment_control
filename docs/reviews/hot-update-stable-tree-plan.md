# S0 · stable-tree（稳定交接树）实现 plan（主进程定稿）

日期：2026-09-20。上位文档：[stage 分治计划](hot-update-stages-plan.md) S0 一节与 §-1 OQ5 裁决。
由 3 路草拟（statemachine / protocol / gates）→ 3 路交叉批评 → 1 路综合的 workflow 产物定稿；原始产物归档于仓库外 `~/tether-hot-update-handoff-20260920T2356Z/s0-plan/`。

**主进程定稿裁决**（综合稿 §0 六条裁决点全部采纳；此外）：
- 综合稿把 OQ5「在 `Offset` 处 aborted」精确化为「在 `Offset+len(Data)` 处、只出现在到达 End 的那一批、与 FIN 同位互斥」——采纳，作为 OQ5 的读法；`stream_error` 在 S0 判定不需要（流属性由 `task.abort` / `OutputState.ReadError` 承载），S2 映像必须导出 `task.abort`。
- S0 不引入 `workload.Output.Abort`：已建泵的流在 SIGKILL 后 drain 到真 EOF（不丢内核 pipe 缓冲字节），只有未建泵的流被 aborted；「aborted 带保留字节」用 PTY master 作 stdout 源经真实内核 EIO 覆盖。
- `cmd/tether/exec.go` S0 不碰（aborted-but-settled 与 terminal error 都退 70，与交接树一致）；退出码 taxonomy 与 usage §9.13 的张力在 S1 先改文档再改码。
- 追加稳定树清理项：交接树自带的 2 条 staticcheck QF1001（`internal/procrequest/registry.go:346`、`internal/broker/exec.go:631`）。

---

> 综合自 statemachine / protocol / gates 三份草案与三份交叉批评；所有行号指**当前 index** 中的文件（`internal/execsession/runtime.go` 406 行、`internal/workload/output.go` 263 行、`internal/execclient/client.go` 270 行、`internal/proto/process_resume.go` 81 行）。本人对 index 树补做的只读核验：① `golangci-lint run ./internal/procrequest/ ./internal/broker/` → rc=1，**恰 2 条** `QF1001`（`registry.go:346`、`broker/exec.go:631`），gates 批评的 BLOCKER 属实；② 三本 golden 相对 HEAD 已暂存 wire 1/0、test 114/0、matrix 2/0、structural 1/1（`Agent 127→126`）；③ `TestLeakRiskMatrix` 今天只含 agent/tunnel/handoff/workload 四包；④ `test/simcluster/tests/run-all.sh` 已含 `caddy-artifact-test` 与 `hot-update-process-probe-test` 两个交接树新增脚本。

## §0 目标与出口

**goal**（照抄 stages-plan S0）：把 Codex 交接的 169 文件暂存树变成三硬闸全绿、登记齐全、两增量边界清楚、可复现的基线；**不新增任何产品能力**。

**exit（机械判定，照抄 S0 exit 六条）**
1. `go test -race ./internal/execsession -run '^TestRuntime(CloseCanceledStartupReclaimsLateFailure|PumpSetupFailureRetainsObservableTerminal|PartialAllocationFailureSettlesAndReleasesBudget|CloseReapsChildWithUnacknowledgedFullOutput)$' -count=3` 四条 PASS，断言未削弱；F1/F2 各有一处 overlay 变异证明能再红（留 rc）。
2. `make test`、`make e2e-parallel`（并行全绿即通过）、单份 `make lint`、`make gates`（含 `sh test/simcluster/tests/run-all.sh`）全绿，退出码直接取。
3. `GOOS=darwin GOARCH=arm64 go build ./...` 绿。
4. test/wire/matrix inventory 新增行数 = 预期（**vs index：test +45+9=+54 / wire +49 / matrix +3**；vs HEAD：+168 / +50 / +5），`TestEveryLeakAssertionExercisesItsSubjectRepeatedly` 绿（+3 anchor）；structural golden 未放宽（vs HEAD 只许 `Agent 127→126`）。
5. `git ls-files | grep -c __pycache__` = 0；`git add -A` 后 `git status --short | grep -v '^[AMDR] '` 为空（无未暂存、无未跟踪；内审 DOC-1 指出「`git status --short` 为空」在暂存树上结构上不可能成立）。
6. plan 落盘（本文定稿为 `docs/reviews/hot-update-stable-tree-plan.md`）；evidence 追加 S0 段（命令/rc/时长/HEAD/`git write-tree`）。

**本 plan 对 OQ5 合同的精确化（不重开裁决，只钉死读法；三份草案与三位批评者一致）**

| 条款 | 精确读法 |
|---|---|
| "`Error != ""` 表示该流在 `Offset` 处 aborted" | Error 与 FIN **同位**：都作用于 `Offset+len(Data)`，且**只出现在到达 End 的那一批**（非终批两个标记皆无，终批可同时带 Data）。否则 >32KiB 的 aborted 流会在第一批就被客户端判为终结、第二批触发 `bytes after abort`、Run 提前退出、task 永不 settle——这正是任务书点名必须修掉的形状（三位批评者的 BLOCKER）。 |
| "FIN 保持 false、不调用 `Replay.Finish()`" | 异常路径零 `Finish()` 调用：`Finish(` 只在 `allocate` L231（stdin）、`failStart` L241（未创建 child 的两条空流真 FIN）与 `Output.run` 的真 EOF 臂。 |
| "terminal-ack 逐流接受 `(FIN && ack==End) \|\| (aborted && ack==End)`" | **request 侧 `StdoutFIN/StderrFIN` 只回显真 EOF**（aborted 流永远 FIN=false）；服务端凭自己的 abort 记录判定；`acknowledge` L320 **一字不改**——它就是「把 aborted 改写成正常 FIN → ErrReplayOffset」的服务端拒绝面。**驳回** protocol 草案「request FIN = 已在 ACK 处结束（EOF 或 abort）」：同名字段两端语义不同，且把 S0 tests 点名要变红的伪造 FIN 合法化。 |
| "execclient 把 aborted 呈现为错误但仍完成 settle" | `accept` 不再在 `Error` 上返回；`Run` 在 terminal-ack 结清**之后**返回 `*SettledError`，`Result` 携带真实 exit/signal + `TerminalError` + 逐流 abort。 |
| "registry 终态加 additive `stream_error`（若需要）" | **S0 判定不需要**：abort 原因是流属性，已由 `task.abort`/`OutputState.ReadError` 承载并在每次 resume 的终批送达直到 settle；settled 后 task 删除、registry 只剩 tombstone、无消费者；加字段要同步 `validTerminal`（L361）/`transition` 幂等相等（L235）/`Restore` 校验三处且复制真相。`Terminal` 不在 `internal/proto`，本来就不影响 wire +49。**S2 plan 必须把 `task.abort [2]string` 纳入 execsession 映像状态**（未建泵的流只有 task 记得）。 |
| "同一 aborted 语义 S4/S5 复用" | S0 的 aborted 来源恰是两类：① 泵从未建立（setup 失败）→ `t.abort[i]`；② 泵自己记录的 `ReadError`（EIO / 存储失败）→ `OutputState.ReadError`。S4 的非 started-PTY 域 EIO 与 S5 的流错误都是第 ② 类，**不需要新 API**。 |

**六条主进程裁决点（草案 questions_for_root 的答复，本 plan 已按此写死）**
1. `cmd/tether/exec.go` **S0 不碰**。现状：`reliableErr != nil → execFailureMessage(err.Error())` → class 70。S0 后 aborted-but-settled 与 terminal error 都退 70（与交接树今天一致，只是 task 现在真的 settle 了）。stages-plan 把「exit/signal/aborted 呈现」放在 S1；把「已执行但输出截断」映射成 70 与 usage §9.13「真跑起来即透传」有 WHAT 层张力，**文档先于代码**，留 S1。`SettledError.Error()` 把 terminal error **放首位、原文**，`execFailureMessage` 按冒号前缀取码的分类（`remote_fs_spawn_timeout` 等）不丢。
2. `stream_error`：不加（见上表）。
3. **不新增 `workload.Output.Abort`**：已建泵的流在 SIGKILL 后 drain 到真 EOF（不丢内核 pipe 缓冲里的字节、stdout 终态确定为 FIN），只有未建泵的流记 `t.abort[i]`。「aborted 带保留字节」臂用 **PTY master 作 stdout 源**（child 退出 → 内核 EIO，`internal/pty` 的 `TestMasterStartedChildTailPrecedesKernelEIO` 已证 tail 先于 EIO）在 runtime 级确定性覆盖，走的是产品既有 `Output.run` default 臂，零新 API、零测试钩子。
4. leak 覆盖：把 F2 形状的 `exerciseRuntimeSetupFailure(t)` 并入 `TestRuntimeRepeatedExecutionReleasesResources` 同一循环（anchor 仍是 `exerciseRuntimeExecution`，anchor 数保持 +3，exit #4 的 3 不变）。
5. 新增 Test 函数 **9 个**（execsession 5、execclient 3、proto 1）→ inventory vs index +54；每条都对应一个可被 overlay 变异打红的不变量（§3 表）。
6. `Runtime.Close` 采用两段式（锁内非阻塞探测已发布 task 立即 retire、只对 pending 等 ctx），**不留 `retired` 标志**（`closeTask`/`delete` 本就幂等、`close(done)` 只在 `conclude` 一处，三位批评者一致指出该标志是打不红的死防线）。

**S0 追加的稳定树清理项（gates 批评 BLOCKER）**：`internal/procrequest/registry.go:346` 与 `internal/broker/exec.go:631` 两处 `if !(a || b || …)` 机械改写为 De Morgan 形式（`if !(c >= 'a' && c <= 'z') && !(c >= 'A' && c <= 'Z') && !(c >= '0' && c <= '9') && c != '-' && c != '_'` 一类，**保留周围注释**）；实现前先在未动的 index 树上跑一次单份 `make lint` 拿基线收据，evidence 里把「交接树自带的红」与「S0 引入的红」分栏。

## §1 Runtime 状态机修法（`internal/execsession/runtime.go`）

### 1.1 状态（派生态，不新增 iota 枚举——不触发 15 家族枚举门）

全部字段只在 `r.mu` 下读写：

```go
type task struct {
	handle proto.ProcessHandle
	pid    string
	logs   [3]*workload.Replay // stdin, stdout, stderr
	input  *workload.Input
	output [2]*workload.Output
	// abort records, per output stream, why the runtime ended it WITHOUT a
	// true EOF: its actor was never built after the child already existed.
	// Bytes up to End stay deliverable, nothing follows, Replay.FIN is never
	// set. A stream whose pump exists records its own failure in
	// OutputState.ReadError instead; streamAbort joins the two.
	// origin: docs/reviews/hot-update-stages-plan.md §-1 OQ5 (S0 F2).
	abort [2]string
	done  chan struct{}
}
```

| 状态 | 证据（r.mu 下） | 资源 owner |
|---|---|---|
| created | 在 `r.tasks`，`logs` 全 nil，`done` 未关 | start goroutine |
| allocated / starting | `logs` 非 nil；registry `starting`；Start hook 在飞 | start goroutine（**唯一**；Close 不冒充它结束） |
| running | `output[i]!=nil` 或 `abort[i]!=""`；registry `running`（或 setup 失败时仍 `starting`） | Output/Input actor 持 pipe 端与 log；Manager 持 child |
| **terminal-draining**（S0 新命名，非新状态机分支） | registry `terminal`；`done` 已关；某条已建泵的流尚未 FIN（setup 失败后 child 已 SIGKILL，泵在 drain 内核缓冲） | 表持 replay；泵持 pipe 读端 |
| terminal | registry `terminal`；`done` 已关；每条流 FIN 或 aborted | 表（`r.tasks`）持 replay 直到 settle |
| retired | 已从 `r.tasks` 删除、三 log/两 output/input 已 Close | 无 |

**唯一最终清场 owner 规则**：资源释放 + 表删除**只经 `retireLocked(t)`**（= `closeTask(t)` + `delete`，幂等）。三条到达路径：(a) `Handle("terminal-ack")` Settle 成功后；(b) `Close()` 对**已发布**（`done` 已关）的 task 立即、不等 ctx；(c) start goroutine 的**唯一发布点 `conclude(t)`** 在 `r.closed` 下自行 retire 再 `close(t.done)`。(c) 是 F1 的修法；(b) 堵住 Go select 随机取臂造成的「已发布 task 约 50% 漏 retire」（批评者用探针实测 40 轮 23 次）。

happens-before：`conclude` 在 `r.mu` 下「先 retire（若 closed）后 close(done)」；`Close` 在同一把锁下置 `closed=true` 并探测 `done`。要么 Close 先置位 → conclude 看见 closed 自行 retire；要么 done 先关 → Close 非阻塞探测命中 → Close retire。两边都到 retire 也无害（幂等）。`t.done` 只在 `conclude` 关一次（start 只跑一次），无 double-close。

### 1.2 改后各函数（伪代码 = 可直接抄的 Go）

```go
// Handle: 只改 terminal-ack 段（L157-168）
if req.Op == "terminal-ack" {
	// acknowledge already proved every FIN flag against a true EOF at End and
	// refused a FIN claim on an aborted stream. Here each stream must be
	// collectable at its final offset: (FIN && ack==End) || (aborted && ack==End).
	if record.Terminal == nil || req.TerminalID != record.Terminal.ID ||
		!streamSettled(t, 0, req.StdoutFIN) || !streamSettled(t, 1, req.StderrFIN) ||
		len(reply.Stdout.Data) != 0 || len(reply.Stderr.Data) != 0 {
		return reply, procrequest.ErrConflict
	}
	if err := r.registry.Settle(scope, *req.Handle, req.TerminalID); err != nil {
		return reply, err
	}
	r.retireLocked(t)
	reply.Phase = "settled"
}
```

```go
func (r *Runtime) start(t *task, spec proto.ExecReq) {
	defer r.conclude(t) // the only publication point; retires under a closed runtime (F1)
	r.mu.Lock()
	err := r.allocate(t, spec.Stdin)
	closed := r.closed
	r.mu.Unlock()
	if err != nil || closed {                 // L178-184 原样：未创建 child 的失败仍是两条空流真 FIN
		if err == nil {
			err = fmt.Errorf("exec session: runtime closed before start")
		}
		r.failStart(t, err)
		return
	}
	r.cfg.Started(t.pid, spec)
	stdio, warning, err := r.cfg.Start(t.pid, &spec)
	if err != nil {
		r.failStart(t, err)
		return
	}
	r.mu.Lock()
	// L192-207 原样：NewOutput / NewOutputWithPrefix / NewInput / registry.Running，逐项 stdio.X = nil
	...
	if err != nil {
		// This child already exists. Terminate the owned subtree; never pretend
		// creation did not run. Every retained byte and this failure stay for
		// the client: creation ran, so its result must remain settleable
		// exactly once. A stream whose actor was never built is ABORTED here at
		// its current End (never a fabricated FIN); a stream whose pump exists
		// keeps draining the killed child's pipe to its true EOF. closeTask
		// here was F2: it closed logs the protocol still had to deliver.
		// A runtime closed meanwhile is handled by observe's first tick.
		_ = r.cfg.Manager.Signal(t.pid, syscall.SIGKILL)
		for i := range t.output {
			if t.output[i] == nil {
				t.abort[i] = boundedError(err.Error())
			}
		}
	}
	stdio.Close() // only the ends that were never transferred (nil-safe)
	r.mu.Unlock()
	r.observe(t, err)
}
```
注意：原 L208 的 `|| r.closed` **删除**——closed 路径的 SIGKILL+closeTask **只在 `observe` 一处**（它的首次迭代不等 ticker、立即执行），这样 §3 的 LateSuccess 变异才是单站点变异。原注释「Terminate the owned subtree before releasing its original pipes; never pretend creation did not run」整段搬入新注释。

```go
// failStart（L234-248）不改。conclude 负责 closed 下的收尾。

func (r *Runtime) observe(t *task, setupErr error) {
	// 逻辑不改（L250-280）：ready := setupErr != nil || r.closed || outputsEnded(t)；
	// closed 分支仍 Signal(SIGKILL)+closeTask（这是 AllowReap 契约"explicitly closed"的那一半，
	// 且 closed 下没有客户端、无物可交付）；AllowReap → 唯一 reaper 的 Exit → finish。
	// 唯一差别来自 outputsEnded 变为 abort-aware（见下）。
}
```
setup 失败下 `ready` 立即为真 → AllowReap → reap → `finish` 记 terminal；已建的 stdout 泵继续 drain 到真 EOF（**terminal-draining**）。为什么不等 outputsEnded 再 finish：setup 失败时 `registry.Running` **未被调用**，record 仍是 `starting`，`Handle` 对 `starting` 提前返回不带批次（L141-146）；若 stdout 已被 8 字节容量背压，客户端拿不到字节就不能 ACK、泵不能前进、outputsEnded 永假、finish 永不到——**死锁**。先 finish 让 phase 进 `terminal`，客户端才能 drain。AllowReap 前已 SIGKILL 整个 owned pgid，满足 `AllowReap` 注释（process.go:116-117）"explicitly terminated"那一半；能逃出 pgid 的后代本来就不可达（与正常 exec 同一既有边界，见 §7）。

```go
// conclude is the startup owner's single publication point. A runtime that
// closed while this owner was still working has no client left to settle the
// result, so the owner retires its own resources before signalling done. An
// open runtime retains them for terminal-ack. Close therefore never has to be
// called twice, and a Close that gave up on its context loses nothing (F1).
func (r *Runtime) conclude(t *task) {
	r.mu.Lock()
	if r.closed {
		r.retireLocked(t)
	}
	close(t.done)
	r.mu.Unlock()
}

// retireLocked is the only place a task's resources are released and its
// table entry removed. Idempotent: the settle path, Close and a late startup
// owner under a closed runtime may each reach it; the first one does the work.
func (r *Runtime) retireLocked(t *task) {
	closeTask(t)
	delete(r.tasks, t.pid)
}

// boundedError keeps a failure text within the 4096-byte limit shared by
// procrequest.validTerminal and workload.RestoreOutput's ReadError.
func boundedError(s string) string {
	if len(s) > 4096 {
		return "exec session: process setup or wait failed"
	}
	return s
}
// finish（L282-296）：L287-289 的截断改为 terminal.Error = boundedError(terminal.Error)，其余不变。

func outputsEnded(t *task) bool {
	for i, output := range t.output {
		if t.abort[i] != "" {
			continue
		}
		if output == nil {
			return false
		}
		state := output.Snapshot()
		if !state.Replay.FIN && state.ReadError == "" {
			return false
		}
	}
	return true
}

// streamAbort reports why output stream i ended without a true EOF, or ""
// while it is live or ended by EOF. A pump-recorded read failure and a runtime
// abort are the same protocol fact. Callers read it BEFORE the log snapshot:
// once non-empty the pump has exited (a read failure is recorded in the same
// critical section as its last Append), so End cannot move afterwards.
func streamAbort(t *task, i int) string {
	if t.abort[i] != "" {
		return t.abort[i]
	}
	if t.output[i] == nil {
		return ""
	}
	state := t.output[i].Snapshot()
	if state.Replay.FIN {
		return "" // a true EOF is never reported as an abort
	}
	return state.ReadError
}

// streamSettled: after this request's ACK was applied, stream i is collectable
// only when the client holds every byte up to its final offset — a true EOF
// the client echoed as FIN, or a retained abort acknowledged where the source
// stopped (fin is then false: acknowledge refused a FIN claim on a non-FIN log).
func streamSettled(t *task, i int, fin bool) bool {
	log := t.logs[i+1]
	if log == nil {
		return fin // allocate never built this log: an empty true EOF (partial-allocation path)
	}
	abort := streamAbort(t, i) // before the snapshot; see streamAbort
	state := log.Snapshot()
	if state.Base != state.End {
		return false
	}
	if fin {
		return state.FIN
	}
	return abort != ""
}

// acknowledge（L311-332）不改。

func outputBatch(t *task, i int) (*proto.ProcessOutput, error) {
	log := t.logs[i+1]
	if log == nil {
		return &proto.ProcessOutput{FIN: true}, nil // partial-allocation path, unchanged
	}
	abort := streamAbort(t, i) // before the snapshot; see streamAbort
	state := log.Snapshot()
	buf := make([]byte, min(uint64(32<<10), state.End-state.Base))
	n, err := log.Read(state.Base, buf)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	batch := &proto.ProcessOutput{Offset: state.Base, Data: buf[:n]}
	// Both end markers apply exactly at Offset+len(Data) and only in the batch
	// that reaches End: a client that sees either already holds every retained
	// byte. They are mutually exclusive by construction — an abort never sets
	// Replay.FIN and a true EOF is never reported as an abort.
	if state.Base+uint64(n) == state.End {
		batch.FIN = state.FIN
		if !state.FIN {
			batch.Error = abort
		}
	}
	return batch, nil
}
// L348-350（无条件塞 ReadError）删除。closeTask（L354-368）不改。

// Close is actual runtime teardown, never connection replacement or freeze.
// A stalled startup keeps its owner until it returns even if ctx expires; that
// owner then retires its own resources (conclude), so Close is never required
// a second time. Tasks whose owner has already published are retired here at
// once, without waiting on ctx: a blocking select against an expired ctx
// picked the ctx arm about half the time and left them behind.
func (r *Runtime) Close(ctx context.Context) error {
	r.mu.Lock()
	r.closed = true
	pending := make([]*task, 0, len(r.tasks))
	for _, t := range r.tasks {
		_ = r.cfg.Manager.Signal(t.pid, syscall.SIGKILL) // ErrUnknown before adoption is fine
		select {
		case <-t.done:
			r.retireLocked(t) // terminal awaiting an ACK that can no longer arrive
		default:
			pending = append(pending, t)
		}
	}
	r.mu.Unlock()
	for _, t := range pending {
		select {
		case <-t.done: // conclude has already retired it under closed
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}
```

**并发情形核对**：双 Close——都置 closed、都 SIGKILL（幂等）、都只等同一 `done`（广播）、无第二个进程 waiter（唯一 Wait4 仍是 Manager reapLoop）、retire 幂等。Close vs terminal-ack——同持 `r.mu`，Handle 先赢则 settle+retire、Close 看不到；Close 先赢则 Handle L76 拒绝。Close vs create——create 锁内插表并 `go start`；start 在 allocate 后读到 closed → failStart → conclude retire。Close 期间 Start hook 迟到成功——attach → observe 首次迭代见 closed → SIGKILL+closeTask → AllowReap → reap → finish → conclude retire。ctx 过期——返回 `ctx.Err()`，goroutine 存活到 hook 返回，不 leak（conclude）、不冒充已结束。`retireLocked` 在 `r.mu` 下等泵退出（`Output.Close`/`Input.Close` 先关 source/target 再 `<-done`）；泵不取 `r.mu`，无死锁。

**workload 无改动**（无 `Output.Abort`）。`internal/agent/exec.go` 末尾三函数与 `internal/broker/exec.go:568-613` **不改**（只转发 `Handle` 结果 / 只重编码 request）。

## §2 协议三端修改清单

| 文件:符号 | 改什么 |
|---|---|
| `internal/proto/process_resume.go:31-33` `ProcessResumeReq` 注释 | 追加：`StdoutFIN/StderrFIN echo a TRUE source EOF only, at StdoutACK/StderrACK == End. An aborted stream is acknowledged at its final offset with the flag false; the agent refuses a FIN claim on a non-FIN log.` |
| 同文件 :68 `ProcessResumeReply.Error` | 追加：`Kind "error": the refusal. Kind "status": the task's own setup or wait failure (procrequest.Terminal.Error), never a stream abort.` |
| 同文件 :74-75 `ProcessOutput` 注释 | 替换为：`ProcessOutput is one bounded replay batch starting at an absolute offset. FIN and Error both apply at Offset+len(Data) and only in the batch that reaches the stream's end. FIN means a true source EOF. Error means the stream ABORTED there: every byte before it was retained and delivered (in this or earlier batches), nothing follows, and FIN stays false — an abort never fabricates EOF. A batch short of the end carries neither. origin: docs/reviews/hot-update-stages-plan.md §-1 OQ5.` |
| **wire 字段** | **零新字段**；wire inventory 恰 +49（全在已暂存的 `Process*` 类型）。 |
| `internal/execclient/client.go:25-30` `Result` | 加 `TerminalError, StdoutAbort, StderrAbort string`（注释：terminal error = agent 记录的 setup/wait 失败；abort = 该流在交付并确认 N 字节后未经真 EOF 结束的原因；全空 = 干净运行）+ `StdoutBytes, StderrBytes uint64`。 |
| 同文件（新类型） | `type SettledError struct{ Result Result }`；doc：`returned by Run only AFTER the task settled on the agent: every delivered byte was acknowledged and the terminal collected, but the outcome is not a clean run. Result carries the child's real exit code and signal. A transport or identity failure is never wrapped in it.` `Error()`：`<TerminalError>; stdout aborted after N byte(s)[: reason]; stderr aborted after M byte(s)[: reason]`，空段省略，与 `TerminalError` 同文的 reason 省略（F2 形状不三连重复）；**terminal error 首位、原文**（`execFailureMessage` 按首个 `:` 前取码）。 |
| 同文件 :32-36 `receiver` | 加 `abort string`；`func (r *receiver) ended() bool { return r.fin \|\| r.abort != "" }` |
| 同文件 :227-262 `accept` | ① 入口后加 `if batch.FIN && batch.Error != "" → "exec client: conflicting stream end"`；② `end > r.offset` 段：`r.fin → "exec client: bytes after FIN"`（保留既有子串）、`r.abort != "" → "exec client: bytes after abort"`；写入段不变（短写先返回）；③ `if batch.FIN \|\| batch.Error != ""`：`end != r.offset → "exec client: conflicting final offset"`；`(batch.FIN && r.abort != "") \|\| (batch.Error != "" && r.fin) → "exec client: conflicting stream end"`；`batch.FIN → r.fin = true`；`batch.Error != "" && r.abort == "" → r.abort = batch.Error`；④ **删除 L258-260** `if batch.Error != "" { return errors.New(batch.Error) }`——正是它让 Run 提前退出、task 永不 settle。 |
| 同文件 :112-133 `Run` | L113 不变（`req.StdoutFIN = out.fin` 只发真 EOF）；L114 `out.fin && errOut.fin` → `out.ended() && errOut.ended()`；L115 后填 `result.TerminalError = reply.Error; result.StdoutAbort, result.StderrAbort = out.abort, errOut.abort; result.StdoutBytes, result.StderrBytes = out.offset, errOut.offset`；L119-120 注释「holding both FINs」→「holding every stream's end」；L129-131 替换为 `if result.TerminalError != "" \|\| result.StdoutAbort != "" \|\| result.StderrAbort != "" { return result, &SettledError{Result: result} }; return result, nil`。L46-48 头注释加一句 `A settled but unclean result is returned as *SettledError together with the acknowledged Result.` |
| 同文件 :264-270 `acknowledgePartial` | 不改（已只发 `fin`）。 |
| `cmd/tether/exec.go:77-97` | **不改**（§0 裁决 1）。CLI golden 零变化。 |
| `internal/execsession/runtime_test.go:207` `runtimeClient.accept` | 不改；加一行注释：成功主线不许出现 `Error`，故 fatal。 |
| `internal/procrequest/registry.go:346`、`internal/broker/exec.go:631` | QF1001 机械改写（§0）。 |

## §3 测试清单

命名按被测单元，函数上方 `// origin: docs/reviews/hot-update-stable-tree-plan.md §3`；F1/F2 两条原测试**字节不变**（同一 oracle 的红→绿收据）。Overlay 变异全部对 `/tmp/s0/mut/<m>/` 下的产品文件副本用 Edit 工具改、overlay 引用，仓库不动；**先绿收据、再红收据**，每条 rc 进 evidence。

| # | 名称 / 文件 | 断言 | 变异（期望红的形态） |
|---|---|---|---|
| T1 | `TestRuntimeCloseCanceledStartupReclaimsLateFailure`（既有 F1，不改）`internal/execsession/runtime_failure_test.go` | Close(canceled)=Canceled；late Start 失败后 tasks==0、`logs[1:]` Read→`os.ErrClosed`、全额预算 probe 成功；cleanup 二次 Close 返回 nil | **f1**：`conclude` 删掉 `if r.closed { r.retireLocked(t) }` → rc=1，日志 `closed runtime retained 1 completed startup owner(s)` / `Read = EOF` / `retained replay budget` |
| T2 | `TestRuntimePumpSetupFailureRetainsObservableTerminal`（既有 F2，不改） | starts==1、Manager 空；resume 无 error；Phase terminal、TerminalID/Error 非空、ExitCode -1 | **f2**：start 的 setup 失败分支在 `Signal` 后插回 `closeTask(t)` → rc=1，`not observable through resume: … file already closed` |
| T3 | `TestRuntimePartialAllocationFailureSettlesAndReleasesBudget`（既有，加强） | 追加 `reply.Stdout.Error == "" && reply.Stderr.Error == ""`——未创建 child 的失败是 EOF 不是 abort | `outputBatch` 对 `log == nil` 也回 Error → 红 |
| T4 | `TestRuntimeCloseReapsChildWithUnacknowledgedFullOutput`（既有，不改） | 不变；现在走 pending→observe(closed)→conclude→retire 路径 | 回归钉（无变异） |
| T5 | `TestRuntimeSetupFailureAbortsUnbuiltStreamAndSettles`（新） | fixture 同 T2 但 child `printf 12345678901234567890; exec sleep 30`、capacity 8、warning 16B、`Limit=(64<<10)+16`。await done；循环 resume/ack 直到 stdout FIN：每批 `Error==""`、`Offset==已收长度`、`len(Data)<=8`，收齐 **20 字节且 FIN**；每个回复 stderr `Offset==0 && len(Data)==0 && !FIN && Error!=""`（含 `output requires unused replay log`）；settle 前 `owner.logs[2].Snapshot().FIN==false`；terminal `Signal==9 && ExitCode==-1 && Error!=""`。负控制：terminal-ack{StderrFIN:true,StderrACK:0} → `errors.Is(err, workload.ErrReplayOffset)`；terminal-ack{StdoutFIN:false,StdoutACK:20} → `errors.Is(err, procrequest.ErrConflict)`；两次后 `registry.Lookup` 仍 terminal。然后 terminal-ack{StdoutACK:20,StdoutFIN:true,StderrACK:0,StderrFIN:false} → settled；重发 → settled。之后 tasks==0、Manager 空、全额 probe 成功、`owner.logs[1].Read`→ErrClosed | **fakefin**：`t.abort[i] = boundedError(...)` 换成 `_ = t.logs[i+1].Finish()` → 红 `stderr must be aborted, not FIN`（Error 空 / FIN true） |
| T6 | `TestRuntimeReadErrorAbortsStreamAtFinalOffsetAndSettles`（新；aborted 带保留字节、跨批） | Start hook `failureStartPTYStdoutChild`：`cpty.Open()`（`github.com/creack/pty`）→ master 经 `SyscallConn().Control` 内 `unix.FcntlInt(fd, F_DUPFD_CLOEXEC, 3)` + `unix.SetNonblock` + `os.NewFile` 变成可轮询（镜像 `internal/pty/master_linux.go:18-56`；creack 的 `Open` 调过 `File.Fd()` 把原 wrapper 置回阻塞，故必须 dup）→ `workload.NewStdio()`；`stdio.Bind(cmd)` 后 `cmd.Stdout = slave`；`Setpgid`；Start 后 `stdio.CloseChild()`、`slave.Close()`（child 的副本成为最后一个 slave）、`stdio.Stdout.Close()`、`stdio.Stdout = master`；`AdoptDraining`。child `/bin/sh -c "head -c 40960 /dev/zero \| tr '\\0' a; exit 23"`，capacity 64KiB，`Limit=3*(64<<10)`。await（r.mu 下）`owner.output[0].Snapshot().ReadError!="" && owner.logs[1].Snapshot().End==40960`，await done。resume#1：Stdout `{Offset 0, len 32768, FIN false, Error ""}`、Stderr `{Offset 0, FIN true}`、terminal ExitCode 23；ack 32768 → resume#2：`{Offset 32768, len 8192, FIN false, Error 含 "input/output error"}`。负控制：terminal-ack{StdoutACK 32768,StdoutFIN false} → ErrConflict；terminal-ack{StdoutACK 40960,StdoutFIN **true**} → ErrReplayOffset。terminal-ack{StdoutACK 40960,StdoutFIN false,StderrACK 0,StderrFIN true} → settled；tasks==0、probe 成功、Manager 空 | **lastbatch**：`outputBatch` 去掉 `state.Base+uint64(n) == state.End` 守卫（Error 无条件附加，即 index 现状）→ 红 `non-final batch carries Error` |
| T7 | `TestRuntimeCloseCanceledStartupReclaimsLateSuccess`（新） | hook `close(entered); <-release; return failureStartChild(...)`（真 child `exec sleep 30`，hook 内用 `manager.Snapshot()[0].OSPID` 记 ospid）；Close(canceled)=Canceled；release；await done：tasks==0、`syscall.Kill(ospid,0)`→ESRCH、Manager 空、`logs[1:]` Read→ErrClosed、全额 probe 成功 | `observe` 的 `ready` 去掉 `r.closed` → child 存活、done 永不关 → 5s 超时红（或去掉 closed 分支的 `Signal` → ESRCH 断言红） |
| T8 | `TestRuntimeCloseRetiresUnsettledTerminalWithoutWaiting`（新） | child `printf out; exit 23`；await done（terminal、不 settle）；Close(**已取消** ctx) 返回 **nil**；tasks==0、logs 关闭、probe 成功；定向命令带 `-count=20` | Close 改回「所有 task 一律 pending + 单个阻塞 select」→ 约半数轮返回 Canceled / task 残留 → 红 |
| T9 | `TestRuntimeConcurrentCloseRetiresOnce`（新，并发） | 同 T4 的满 stdout child；两个 goroutine 同时 Close(5s)；都 nil、无 panic；tasks==0、Manager 空、ESRCH、probe 成功 | Close 把 `for range r.tasks` 挪到 `r.mu.Unlock()` 之后（与 retireLocked 的 delete 竞争）→ `-race` 报告 → 红。诚实标注：`retired` 标志类变异打不红，故不设该字段 |
| T10 | `TestRuntimeRepeatedExecutionReleasesResources`（既有 anchor，循环体加 `exerciseRuntimeSetupFailure(t)`）`runtime_test.go`（helper 在 `runtime_failure_test.go`） | `for i := 0; i < 5; i++ { exerciseRuntimeExecution(t); exerciseRuntimeSetupFailure(t) }`；helper 跑 T5 形状到 settle 后**显式** `r.Close(5s)`；断言在循环外不变 | `retireLocked` 去掉 `closeTask(t)` → 泵 goroutine/memfd FD 增长 → 红。诚实标注：没有只经 setup-failure 轮才红的单站点变异 |
| T11 | `TestRunSettlesAbortedStreamAndReportsIt`（新）`internal/execclient/client_test.go` | stub：create→`clientStatus`+Stdout{Data abc}；ack req `StdoutACK 3, StdoutFIN false`；回 terminal `{TerminalID original-terminal, ExitCode 17, Stdout{Offset 3, Error "source failed"}, Stderr{FIN true}}`；`f.next(t,"terminal-ack")` 请求 `StdoutACK 3 && !StdoutFIN && StderrACK 0 && StderrFIN`（**不用** `settleClient`，它把 `!StdoutFIN` 判 premature）；回 settled；`run.finish`：`errors.As(err, *SettledError)`、`Result.ExitCode 17`、`StdoutAbort=="source failed"`、`StdoutBytes 3`、`TerminalError==""`、out=="abc"、`err.Error()` 含 `stdout aborted after 3 byte(s): source failed`、`!errors.Is(err, ErrUnsupported)` | 在 `accept` 末尾恢复 `if batch.Error != "" { return errors.New(batch.Error) }` → `next(t,"terminal-ack")` 超时 `waiting for terminal-ack` → 红 |
| T12 | `TestRunRejectsConflictingStreamEndMarkers`（新，表） | 行：`fin_then_error` first{abc,FIN} bad{Offset 3,Error x} want `conflicting stream end`，partial ack FIN=true；`error_then_fin` first{abc,Error x} bad{Offset 3,FIN} want `conflicting stream end`，partial FIN=false；`error_then_bytes` first{abc,Error x} bad{Offset 3,Data d} want `after abort`，partial FIN=false；`both_markers` first{abc} bad{Offset 3,FIN,Error x} want `conflicting stream end`；`stale_abort_offset` first{abc} bad{Data ab,Error x} want `conflicting final`。每行 out=="abc"、partial ack `StdoutACK==3` 且 FIN 如列 | 去掉跨批互斥检查 `(batch.FIN && r.abort != "") \|\| (batch.Error != "" && r.fin)` → 前两行红 |
| T13 | `TestRunRealChildSetupFailureSettlesAbortedStream`（新，端到端） | 复用 L513 的真 `execsession.Runtime` + relay 形状：`ReplayCapacity 8`、Start hook 真 child（`/bin/sh -c 'exec sleep 30'`，Setpgid，AdoptDraining）返回 `strings.Repeat("w",16)` warning；relay 观察到 terminal-ack 请求 `StderrFIN==false && StderrACK==0 && StdoutFIN==true && StdoutACK==0`、回复 Phase settled；Run 返回 `*SettledError`：`TerminalError!=""`、`StderrAbort!=""`、`StdoutAbort==""`、`Signal 9`、`ExitCode -1`；cleanup `rt.Close` 返回 nil、`manager.Snapshot()` 空 | `Run` 恢复 `out.fin && errOut.fin` → 永不 terminal-ack → 客户端 ctx 超时 → 红（这条同时钉住 runtime↔execclient 两端约定一致） |
| T14 | `TestRunRealChildSurvivesLostCreateAndSettlementReplies`（既有） | 不变：干净路径返回 nil 而非 SettledError | 回归钉 |
| T15 | `TestProcessOutputAbortedNeverEncodesFIN`（新）`internal/proto/process_resume_test.go` | `Marshal(ProcessOutput{Offset:3,Error:"x"})` 无 `"fin"`/`"data"` 键；`Marshal({Offset:3,FIN:true})` 无 `"error"`；`Unmarshal({"offset":3,"error":"x"})` FIN false；`Unmarshal({"offset":0})` 全零值合法（N-1 旧端不发 error = 无 abort） | `FIN` 去掉 `omitempty` → `"fin":false` 出现 → 红 |

新增 Test 函数 = T5,T6,T7,T8,T9（execsession 5）+ T11,T12,T13（execclient 3）+ T15（proto 1）= **9**。T6/T7 需 `runtime_failure_test.go` 新增 import `cpty "github.com/creack/pty"`、`golang.org/x/sys/unix`（该文件 `//go:build linux`；creack/pty 不在两个 risky-import 扫描名单且扫描跳过 `_test.go`）。

## §4 登记与闸门步骤（串行；退出码直接取，绝不 `| tail`）

```sh
cd /home/weiland/dist_experiment_control && mkdir -p /tmp/s0/mut
# 0. 基线收据（未动的 index 树）
git rev-parse HEAD; git status --short | wc -l                       # cab90d1；0 未跟踪（169 已 staged）
make lint > /tmp/s0/lint-baseline.log 2>&1; echo rc=$?               # 预期 rc=1、恰 2 条 QF1001；多出的也记入 evidence 并在 S0 修
# 1. plan 落盘 docs/reviews/hot-update-stable-tree-plan.md（origin 门要求它先存在）
# 2. 实现 §1/§2 + QF1001 ×2 + 新测试 §3；gofmt
gofmt -l ./cmd ./internal ./test; echo rc=$?                          # 空输出
go build ./... && GOOS=darwin GOARCH=arm64 go build ./...; echo rc=$?
GOOS=darwin GOARCH=arm64 go vet ./internal/execsession/ ./internal/execclient/ ./internal/procrequest/ ./internal/proto/; echo rc=$?
# 3. 绿收据
go test -race -count=3 ./internal/execsession -run '^TestRuntime(CloseCanceledStartupReclaimsLateFailure|PumpSetupFailureRetainsObservableTerminal|PartialAllocationFailureSettlesAndReleasesBudget|CloseReapsChildWithUnacknowledgedFullOutput)$' > /tmp/s0/targeted.log 2>&1; echo rc=$?
go test -race -count=20 ./internal/execsession -run '^TestRuntimeCloseRetiresUnsettledTerminalWithoutWaiting$' > /tmp/s0/close-nowait.log 2>&1; echo rc=$?
go test -race -count=3 ./internal/execsession ./internal/execclient ./internal/procrequest ./internal/proto ./internal/workload > /tmp/s0/pkgs-race.log 2>&1; echo rc=$?
# 4. 红收据（每条：cp 产品文件到 /tmp/s0/mut/<m>/、用 Edit 改副本、写 overlay.json、跑对应测试）
for m in f1 f2 fakefin lastbatch closewait; do mkdir -p /tmp/s0/mut/$m; cp internal/execsession/runtime.go /tmp/s0/mut/$m/runtime.go; printf '{"Replace":{"%s":"%s"}}\n' "$PWD/internal/execsession/runtime.go" "/tmp/s0/mut/$m/runtime.go" > /tmp/s0/mut/$m/overlay.json; done
mkdir -p /tmp/s0/mut/client; cp internal/execclient/client.go /tmp/s0/mut/client/client.go; printf '{"Replace":{"%s":"%s"}}\n' "$PWD/internal/execclient/client.go" "/tmp/s0/mut/client/client.go" > /tmp/s0/mut/client/overlay.json
mkdir -p /tmp/s0/mut/proto; cp internal/proto/process_resume.go /tmp/s0/mut/proto/process_resume.go; printf '{"Replace":{"%s":"%s"}}\n' "$PWD/internal/proto/process_resume.go" "/tmp/s0/mut/proto/process_resume.go" > /tmp/s0/mut/proto/overlay.json
go test -race -count=1 -overlay=/tmp/s0/mut/f1/overlay.json ./internal/execsession -run '^TestRuntimeCloseCanceledStartupReclaimsLateFailure$' > /tmp/s0/mut/f1.log 2>&1; echo rc=$?            # 期望 1（exit #1 必需）
go test -race -count=1 -overlay=/tmp/s0/mut/f2/overlay.json ./internal/execsession -run '^TestRuntimePumpSetupFailureRetainsObservableTerminal$' > /tmp/s0/mut/f2.log 2>&1; echo rc=$?          # 期望 1（exit #1 必需）
go test -race -count=1 -overlay=/tmp/s0/mut/fakefin/overlay.json ./internal/execsession -run '^TestRuntimeSetupFailureAbortsUnbuiltStreamAndSettles$' > /tmp/s0/mut/fakefin.log 2>&1; echo rc=$?   # 期望 1
go test -race -count=1 -overlay=/tmp/s0/mut/lastbatch/overlay.json ./internal/execsession -run '^TestRuntimeReadErrorAbortsStreamAtFinalOffsetAndSettles$' > /tmp/s0/mut/lastbatch.log 2>&1; echo rc=$?  # 期望 1
go test -race -count=20 -overlay=/tmp/s0/mut/closewait/overlay.json ./internal/execsession -run '^TestRuntimeCloseRetiresUnsettledTerminalWithoutWaiting$' > /tmp/s0/mut/closewait.log 2>&1; echo rc=$?  # 期望 1
go test -race -count=1 -overlay=/tmp/s0/mut/client/overlay.json ./internal/execclient -run '^TestRunSettlesAbortedStreamAndReportsIt$' > /tmp/s0/mut/client.log 2>&1; echo rc=$?                # 期望 1
go test -count=1 -overlay=/tmp/s0/mut/proto/overlay.json ./internal/proto -run '^TestProcessOutputAbortedNeverEncodesFIN$' > /tmp/s0/mut/proto.log 2>&1; echo rc=$?                        # 期望 1
# （T7/T9/T10/T12/T13 的变异按 §3 同法各留一份 rc；每条 log 里 grep 期望红串）
# 5. 登记（全部新测试写完之后；三个 updater 都在首次 git add 之前跑完）
#   5a leak anchor：leak_assert_shape_test.go L140-174 map 末尾新开一组（带 origin 注释）加三条精确条目
#      "internal/pty/master_poll_test.go:TestMasterPollableLifecycleReleasesResources":       "exerciseMasterReadDeadline",
#      "internal/procrequest/registry_test.go:TestRegistryRepeatedLifecycleReleasesResources": "exerciseRegistryLifecycle",
#      "internal/execsession/runtime_test.go:TestRuntimeRepeatedExecutionReleasesResources":  "exerciseRuntimeExecution",
go test ./test/determinism/ -run '^TestEveryLeakAssertionExercisesItsSubjectRepeatedly$' -count=1 -v > /tmp/s0/anchor.log 2>&1; echo rc=$?
#   5b 矩阵源：test/e2e/all_phases_test.go L283-284 包列表追加 "./internal/procrequest/..." "./internal/execsession/..." "./internal/execclient/..." + 头注释一句 + origin
go test ./test/e2e/parallel/ -run '^TestMatrixUnitInventoryOnlyGrows$' -count=1 -v -update-matrix-inventory > /tmp/s0/inv-matrix.log 2>&1; echo rc=$?
grep -c 'no tests to run' /tmp/s0/inv-matrix.log; grep -o 'appended [0-9]* unit' /tmp/s0/inv-matrix.log; git diff --numstat -- test/e2e/parallel/testdata/matrix_units_golden.txt     # 0 / appended 3 unit / 3 0
go test ./internal/proto -run '^TestWireFieldInventoryAppendOnly$' -count=1 -v -update-wire-inventory > /tmp/s0/inv-wire.log 2>&1; echo rc=$?
grep -c 'no tests to run' /tmp/s0/inv-wire.log; grep -o 'wrote [^ ]* ([0-9]* entries)' /tmp/s0/inv-wire.log; git diff --numstat -- internal/proto/testdata/wire_field_inventory.txt   # 0 / 496 entries / 49 0
git diff -- internal/proto/testdata/wire_field_inventory.txt | grep '^+' | grep -v '^+++' | grep -vc '^+Process'      # 0：新增行全是 Process* 家族
go test ./test/determinism/ -run '^TestTestFunctionInventoryOnlyGrows$' -count=1 -v -update-test-inventory > /tmp/s0/inv-test.log 2>&1; echo rc=$?
grep -c 'no tests to run' /tmp/s0/inv-test.log; grep -o 'appended [0-9]* function' /tmp/s0/inv-test.log; git diff --numstat -- test/determinism/testdata/test_function_inventory.txt   # 0 / appended 54 function / 54 0
git diff -- test/determinism/testdata/test_function_inventory.txt | grep '^+' | grep -v '^+++' | sort        # 逐字 = 45 交接名 + §3 的 9 个新名
git diff HEAD --numstat -- internal/proto/testdata/wire_field_inventory.txt test/determinism/testdata/test_function_inventory.txt test/e2e/parallel/testdata/matrix_units_golden.txt   # 50 / 168 / 5
git diff HEAD -- test/architecture/testdata/structural_budget_golden.txt | grep '^[-+][^-+]'   # 只许 Agent 127→126
go run ./test/e2e/parallel -split -shards 8 -dry-run > /tmp/s0/dryrun.log 2>&1; grep -E '^(dedupe|units|coverage|deadline)' /tmp/s0/dryrun.log   # 记实际数（今天基线 dedupe 42->34 / units 34->69），不写预测
# 6. 门
go test -count=1 ./test/determinism/ ./test/architecture/ ./test/e2e/parallel/ > /tmp/s0/gates-go.log 2>&1; echo rc=$?
time make gates > /tmp/s0/gates.log 2>&1; echo rc=$?          # 基线 3m32s + run-all.sh 两个新脚本 ≈17s；期间不得另起任何 golangci-lint（全局锁 rc=3）
time make test  > /tmp/s0/test.log  2>&1; echo rc=$?          # 基线 6m32s；红项应为 0
time make e2e-parallel > /tmp/s0/e2e.log 2>&1; echo rc=$?; grep -E '^(ALL PASS|FAILURES:|DEADLINE EXCEEDED)' /tmp/s0/e2e.log
time make lint > /tmp/s0/lint.log 2>&1; echo rc=$?            # 单份，与 gates 内那份串行
# 7. 两增量边界（gates §7）
F=~/tether-hot-update-handoff-20260920T2356Z/evidence/final-candidate-sha256.txt
while read sha path; do cur=$(git show ":$path" | sha256sum | awk '{print $1}'); [ "$sha" = "$cur" ] || echo "SHARED: $path"; done < $F   # 今天 6 行；S0 后 +wire_field_inventory.txt
git diff --cached HEAD -- test/determinism/testdata/test_function_inventory.txt | grep '^+' | grep -v '^+++' | sed 's/^+//' | grep -F -f <(awk '{print $2}' $F | grep '_test.go$' | sed 's/$/:/') > /tmp/s0/ccr-tests.txt; wc -l /tmp/s0/ccr-tests.txt   # 43
go test -count=1 -run "^($(awk -F': ' '{print $2}' /tmp/s0/ccr-tests.txt | paste -sd'|'))$" ./cmd/tether/ ./internal/agent/ ./internal/broker/ ./internal/jsstream/ ./internal/tunnel/ ./test/cli_e2e/ ./test/determinism/ ./test/p13/ ./test/security/ > /tmp/s0/ccr-regress.log 2>&1; echo rc=$?
# 8. 收尾
git ls-files | grep -c __pycache__                            # 0
git status --short | grep '^??'                               # 人工逐行过目（/tmp、*.log、__pycache__、/bin、根目录 tether 已 ignore）
git add -A && git status --short | wc -l && git diff --cached --numstat | awk '$1=="-"' | wc -l && git write-tree   # 0 / 0（无二进制混入）/ tree hash 进 evidence
```

登记假成功的**真实**来源只有两个：`-run` 拼错（`ok … [no tests to run]` rc=0）与在新测试写完之前跑 updater。`-update-*` 是自定义 flag，**永不命中 go test 缓存**（gates 草案那句诊断有误，删掉）；`-count=1` 留作无害习惯。每次登记三件套缺一不算：`--- PASS: <updater 测试>` 行、`appended N`/`wrote (N entries)` 行、golden numstat。

e2e-parallel 首轮若见 `FAIL TestLeakRiskMatrix:internal/{agent,execclient}` 或 `FAIL D4/D5:internal/broker[k/8]`（两增量交叉面 = 都被改的 `internal/agent` 与 `internal/broker`；execclient 的 500ms 请求窗口在 18 worker + 本机外部负载下是新 flake 面），只许 `make e2e-one T=<Matrix>` 或 `E2EPAR_FLAGS='-run <Matrix> -repeat 3 -shuffle=on -v' make e2e-parallel` 定位，**不得串行复核全量**。

## §5 文档 / 证据 / 边界记录

- **`docs/reviews/hot-update-stable-tree-plan.md`**：本文定稿（先于代码落盘）。
- **`docs/reviews/hot-update-evidence.md`** 追加 `## 11. S0 稳定交接树`：HEAD `cab90d1`、lint 基线（2 条 QF1001）、每条命令 / rc / 时长、三本 golden 的 numstat（vs index 54/49/3、vs HEAD 168/50/5）与 `appended`/`wrote` 行、dry-run 实际输出、变异表（每条 rc + 红串）、`ALL PASS` 行、gates/lint/darwin rc、run-all.sh 两个新脚本的 rc/时长、边界表（46 文件：40 未动 / 6 共享 / 123 hot-update 独有；S0 后共享集 +`wire_field_inventory.txt`）与再生命令、ccr 43 测试回归 rc、`git write-tree`。**分栏 ccr / hot-update；只追加不改写。**
- **`docs/reviews/hot-update-tasklist.md` 执行记录**追加一行：「2026-09-20 S0：F1/F2 修复（唯一清场 owner + aborted 合同）；H-6 首次登记（test +54 / wire +49 / matrix +3 / anchor +3）；两处 QF1001 清理；不勾功能项。」
- **`docs/reviews/INDEX.md`** 追加一行（S0 · 链接 `hot-update-stable-tree-plan.md`）；顺手不要在第 28 行那个把表切成两张的空行处再插空行（是否删那个空行由主进程定）。
- **`docs/distributed-broker-architecture.md` §22.3 末尾**追加一段（HOW，无版本字面量）：「可靠 exec 输出流的异常终结：`ProcessOutput.Error` 与 `FIN` 同位（`Offset+len(Data)`）、只出现在到达 End 的批次、互斥；aborted 流已读字节保留、FIN 保持 false、不调用 `Replay.Finish`；request 侧 FIN 只回显真 EOF，agent 拒绝对非 FIN 日志的 FIN 声明；terminal-ack 逐流 `(FIN && ack==End) || (aborted && ack==End)`；ctl 在 settle 后把 terminal error / 流 abort 作为错误结果呈现并保留真实 exit/signal。aborted 的两个来源：泵未建立（setup 失败后 child 已存在，`task.abort`）与泵自记 `ReadError`（EIO/存储失败），S4 非 started-PTY 域 EIO 与 S5 流错误复用后者。setup 失败后 child 被 SIGKILL、终态先记录，已建泵 drain 到真 EOF（terminal-draining）。已关闭 Runtime 下迟到 Start 的唯一最终清场 owner 是 startup goroutine 自身（conclude），Close 对已发布 task 不等 ctx 立即回收。」
- **`docs/reviews/hot-update-stages-plan.md`**：建议在 §-1 OQ5 行末追加一句精确化脚注（「在 `Offset` 处」= `Offset+len(Data)`、只在终批；`stream_error` 在 S0 判定不需要，S2 映像纳入 `task.abort`）——由主进程裁定是否动定稿文本。
- **S2 交接项**（写入本 plan §7 与 evidence）：execsession 映像必须导出 `task.abort [2]string`；aborted-未 settle 的 task 在 Freeze/Export 下的建模。
- **S1 交接项**：`tether exec` 对 `*SettledError` 的呈现（退出码 taxonomy 若变必须先改 usage §9.13）；`finish()` 在 `registry.Complete` 失败（S2 `ErrFrozen`）时不 Exited/不 Forget 的既有缺口。

## §6 批评处置表

| # | 批评者 · target | 严重度 | 处置 | 落点 |
|---|---|---|---|---|
| A1/B1/C3 | statemachine `outputBatch` 无条件 `Error` × 自家 `accept` | BLOCKER/MAJOR | **采纳** | §1 `outputBatch` 只在 `Base+n==End` 附加 Error；§3 T6（PTY 40KiB 跨两批）+ lastbatch 变异 |
| C1 | 三份都没跑 lint 基线；index 树 2 条 QF1001 | BLOCKER | **采纳**（本人复跑证实） | §0 稳定树清理项；§4 步骤 0 基线收据 |
| A2/B3/C4 | protocol/gates 保留阻塞 select，已发布 task 约 50% 漏 retire | MAJOR | **采纳** | §1 两段式 Close；§3 T8（`-count=20`）+ closewait 变异 |
| A3/B2/C2 | request FIN 语义三份互斥 | MAJOR | **采纳**（取 statemachine/gates 形态，驳回 protocol 的重定义） | §0 精确化表；`acknowledge` 不改；§2 proto 注释；§3 T5/T6 的 ErrReplayOffset 负控制 + T13 两端端到端 |
| A4 | gates 的 drain 引入「terminal && 流 live」、违反 AdoptDraining 契约 | MAJOR | **部分驳回**：保留 drain（不加 Abort），把该状态命名为 terminal-draining 并写进 §22.3。理由：setup 失败时 record 仍 `starting`、`Handle` 不发批次，等 outputsEnded 再 finish 会在背压 stdout 上**死锁**；Abort 丢内核缓冲字节且让 stdout 终态非确定；AllowReap 前已 SIGKILL 整个 owned pgid，满足契约「explicitly terminated」半边；逃出 pgid 的后代与正常 exec 同一既有边界 | §1 observe 说明；§7 残余风险 1 |
| B4 | Abort 丢字节 + 非确定性；S0 无 Abort 用武之地 | MAJOR | **采纳** | §0 裁决 3 |
| C7 | gates 设计里「aborted 带保留字节」臂在 runtime 级无确定性测试 | MAJOR | **采纳**（用真实 EIO 而非新 API） | §3 T6 PTY master 源 |
| B5 | protocol 缺 real-runtime + real-execclient 的 aborted 结清测试 | MAJOR | **采纳** | §3 T13 |
| A6/B6/C5 | statemachine 的 cmd/tether 建议让 rc0+abort 退 0；protocol 的 70 映射违反 usage §9.13 | MAJOR | **采纳**（两者都不做）：S0 不碰 `exec.go`，现状 70；S1 先改文档再改码 | §0 裁决 1；§5 S1 交接项 |
| A5/C6(a) | 「Abort 跳过 join → goroutine +5」是恒等式变异 | MAJOR | **采纳**（无 Abort 后 moot）；leak 变异改为 `retireLocked` 去掉 `closeTask` | §3 T10 |
| C6(b)/A11/B13 | `retired` 标志是打不红的死防线；ConcurrentClose 变异是双重人为改动 | MAJOR/MINOR | **采纳**：不设 `retired`；变异改为「Close 在锁外遍历 r.tasks」→ `-race` 红 | §1 retireLocked；§3 T9 |
| C6(c)/B12/C11 | proto omitempty 测试变异写错 / 两份草案缺该测试 | MAJOR/MINOR | **采纳**：变异改为 FIN 去 omitempty | §3 T15 |
| A7 | gates `outputBatch` 先 Snapshot 后 abortReason 的窄竞态 | MINOR | **采纳** | §1 `streamAbort` 先于 `log.Snapshot()`，`streamSettled` 同 |
| A8/A9/B11/C14 | Abort 在 paused/frozen 下关业务 FD | MINOR | moot（无 Abort） | — |
| A10 | 泵自记 ReadError + child 阻塞在满 pipe 上永不退出 | MINOR | **采纳为覆盖限制**（既有行为；T6 的 child 自行退出，不受影响） | §7 风险 5；S5 |
| A12 | gates `StreamAborted` 只带一条流、terminal error 优先丢流信息 | MINOR | **采纳** | §2 `SettledError` + `Result` 三字段 |
| A13/B9 | 「在 Offset 处」措辞 vs `Offset+len(Data)` | MINOR | **采纳** | §0 精确化表；§2 proto 注释；§5 §22.3 与 stages-plan 脚注建议 |
| B7/C9 | 「-update 再跑命中缓存」诊断错误 | MINOR | **采纳**（删说法，留三件套） | §4 |
| B8/C10 | numstat 基线漂移（index vs HEAD） | MINOR | **采纳**：两种基线都记；updater 在首次 `git add` 前完成 | §0 exit #4；§4 步骤 5 |
| B10 | `stream_error` 偏离 OQ5 原文须显式记录 | MINOR | **采纳** | §0 精确化表；§5 stages-plan 脚注建议 |
| C8 | F2 形状 stdout FIN-xor-Error 非确定 | MINOR | moot：drain 下 stdout 确定 FIN（T5 断言 20 字节 + FIN） | §3 T5 |
| C12 | abort cause 无界 | MINOR | **采纳** | §1 `boundedError` |
| C13 | gates `accept` 缺跨批互斥 | MINOR | **采纳** | §2 accept ③；§3 T12 |
| C15 | dry-run 数字是预测 | MINOR | **采纳**：只记实跑输出 | §4 步骤 5 |
| C16 | run-all.sh 两个新脚本未核 | MINOR | **采纳**（批评者已实跑 rc=0，≈17s） | §4 gates 预算；§5 evidence |

## §7 残余风险与覆盖限制

1. **terminal-draining 的出口依赖客户端**：setup 失败后若有逃出 pgid 的后代持 stdout 写端，stdout 永不 FIN，客户端等到自己的 ctx、task 保留到 Close——与正常 exec 后代持 pipe 的既有边界同类，S0 不引入也不修；写进 evidence 覆盖限制，S5 流错误语义再收口。
2. **`finish()` 在 `registry.Complete` 失败时提前 return**（不 Exited、不 Forget，Manager 留下 exited 行）：S0 只在 closed 路径由 conclude 兜底清 task；S2 引入 `ErrFrozen` 后此路径真实可达，S1/S2 必须处理。
3. **`AllowReap` 返回 `ErrUnknown`**（adapter 违反「Start 成功 = 已 AdoptDraining」）时 observe 10ms 自旋、Close 卡到 ctx 到期：既有行为，S0 不改；S1 的 Started 后置带 PID 时一并处理。
4. **CLI 呈现**：S0 后 aborted-but-settled 与 terminal error 在 `tether exec` 上都退 70（`execFailureMessage` 无 hint 默认类），真实 exit/signal 只在 `Result` 里；usage §9.13 把 70 读作可重试——已执行且有副作用的命令被健壮重试规则再跑一遍的风险留 S1 裁定，且必须文档先行。
5. **泵自记 ReadError 但 child 未退出**（pipe 源 EIO 类）：客户端把流记为 aborted 后继续等 terminal，child 若阻塞在无人读的 pipe 上永不退出，只靠 ctl `--timeout` 兜底。T6 的 PTY 源 child 自行退出，不受影响。
6. **T6 依赖内核 N_TTY 的「tail 先于 EIO」顺序**：由 `internal/pty/master_poll_test.go:TestMasterStartedChildTailPrecedesKernelEIO` 在同一内核上证明；若未来内核改变该顺序，两条测试会同时红，可归因。
7. **`Close` ctx 过期后 startup goroutine 存活到 hook 返回**：agent 进程若先退出，迟到 spawn 的 child 由 `startBounded` 的 abandon 路径处理——S0 未增测。
8. **execclient 对 `FIN && Error` 同批判协议错误并提前返回（不 settle）**：runtime 永不产生该形状；任何未来放宽都会让此路径变成泄漏源。
9. **e2e 首轮交叉 flake 面**：两增量都改了 `internal/agent`/`internal/broker`；`TestLeakRiskMatrix` 新增 execclient 单元的 500ms 请求窗口在满 worker 下是新 flake 面；定位只许 `e2e-one` / `-repeat 3 -shuffle=on`。
10. **inventory 期望数绑定最终测试集**：主进程若裁剪 §3 的 9 个新测试，exit #4 的 +54 必须同步重算并与 numstat 对账，不能沿用 45 或 54。
11. **S2 前置**：`task.abort` 进映像；aborted-未 settle task 的 Freeze/Export 建模；`Output.Snapshot().ReadError` 已随 `OutputState` 走。