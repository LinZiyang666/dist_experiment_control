package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/LinZiyang666/tether/internal/execsession"
	"github.com/LinZiyang666/tether/internal/proc"
	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/pty"
	"github.com/LinZiyang666/tether/internal/spawnexec"
	"github.com/LinZiyang666/tether/internal/spawnsafe"
	"github.com/LinZiyang666/tether/internal/workload"
	"github.com/nats-io/nats.go"
)

// streamChunkSize bounds how much output is shipped in a single ExecChunk
// publish. NATS default max message size is 1MiB; 4KiB keeps the stream
// fine-grained enough that progress bars / partial lines surface
// promptly without spamming.
const streamChunkSize = 4 * 1024

// dispatchForwarded routes a `cmd.node.<N>.<verb>.req.forwarded` message
// to the right verb handler. Subject layout:
//
//	tether.v2.s.<sid>.cmd.node.<nid>.<verb>.req.forwarded
//
// (10 tokens; same as the cmd.by.* tree minus the actor segment).
func (a *Agent) dispatchForwarded(nc *nats.Conn, msg *nats.Msg) {
	// Audit shard 01 F5: a forwarded msg that arrives mid-shutdown
	// would otherwise spawn a goroutine that publishes onto a
	// draining nats.Conn. Drop early if runCtx is gone — the
	// caller already told subFwd.Unsubscribe to stop new
	// dispatches; this catches the in-flight race.
	if rc := a.loadRunCtx(); rc != nil {
		if err := rc.Err(); err != nil {
			return
		}
	}
	parts := strings.Split(msg.Subject, ".")
	if len(parts) != 10 || parts[8] != "req" || parts[9] != "forwarded" {
		a.cfg.Logger.Warn("agent: forwarded subject malformed", "subject", msg.Subject)
		return
	}
	verb := parts[7]
	// Each verb handler is dispatched in its own goroutine so a
	// long-running run (which blocks in pty.Wait) doesn't head-of-line
	// block subsequent kill.req.forwarded / exec.req.forwarded
	// messages on the same single NATS subscription. Without this,
	// `tether run sleep 60` + Ctrl-C would never deliver the kill
	// signal because the dispatch goroutine is still inside Wait.
	switch verb {
	case "exec":
		go a.handleExecForwarded(nc, msg)
	case proto.ProcessResumeVerb:
		go handleProcessResumeForwarded(a, nc, msg)
	case "run":
		go a.handleRunForwarded(nc, msg)
	case "kill":
		go a.handleKillForwarded(nc, msg)
	case "expose":
		go a.handleExposeForwarded(nc, msg)
	case "expose-rm":
		go a.handleExposeRmForwarded(nc, msg)
	case "proxy-keys":
		// round-2 F4: assign the arrival sequence and register with the drain
		// barrier UNDER proxyDrainMu, so once Run sets proxyDraining no further
		// Add can happen and proxyHandlerWG.Wait is a sound shutdown barrier.
		a.proxyDrainMu.Lock()
		// origin: hot-update S5 plan §7.2. Never wrap the signed sequence or
		// admit a handler whose arrival cannot be represented by the sealed cut.
		if a.proxyDraining || a.proxyDispatchN < 0 || a.proxyDispatchN == math.MaxInt64 {
			a.proxyDrainMu.Unlock()
			return
		}
		a.proxyDispatchN++
		seq := a.proxyDispatchN
		a.proxyHandlerWG.Add(1)
		a.proxyDrainMu.Unlock()
		go func() {
			defer a.proxyHandlerWG.Done()
			a.handleProxyKeysForwarded(nc, msg, seq)
		}()
	case "home":
		// R8a P1: the broker's ACTIVE home-directive push. Before it existed, home
		// changes reached this agent ONLY on the register reply — i.e. only when THIS
		// agent happened to reconnect — so a `cluster drain` on a healthy, silent agent
		// never reached the data plane at all.
		go a.handleHomeForwarded(nc, msg)
	case "upgrade":
		go a.handleUpgradeForwarded(nc, msg)
	case "push":
		go a.handlePushForwarded(nc, msg)
	case "pull":
		go a.handlePullForwarded(nc, msg)
	case "push-commit":
		go a.handlePushCommitForwarded(nc, msg)
	case proto.ClaimProbeVerb:
		// Answered INLINE rather than in a goroutine: the reply is two fields
		// and the broker is holding a register slot open waiting for it, so the
		// dispatch cost of a goroutine would be pure added latency on the one
		// path where latency turns into a wrong lease verdict.
		replyClaimProbe(a, nc, msg)
	default:
		a.cfg.Logger.Warn("agent: unknown forwarded verb", "verb", verb)
	}
}

// handleExecForwarded runs an ExecReq and streams ExecChunks back on the
// request's reply inbox. See proto.ExecChunk for the wire shape.
func (a *Agent) handleExecForwarded(nc *nats.Conn, msg *nats.Msg) {
	if msg.Reply == "" {
		a.cfg.Logger.Warn("agent: exec.req.forwarded without Reply inbox")
		return
	}
	// origin: hot-update S3 plan §4.0. Hold admission for the whole handler.
	release, admitted := a.gate.admit(gateExec)
	if !admitted {
		// origin: hot-update S4 plan §9.2 / N1-REFUSAL. v0.6.0 knows try_again as 75; keep the upgrade reason in detail.
		a.replyChunk(nc, msg.Reply, proto.ExecChunk{Kind: "error", Error: "try_again: agent_upgrading: image switch in progress; retry"})
		return
	}
	defer release()

	var req proto.ExecReq
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		a.replyChunk(nc, msg.Reply, proto.ExecChunk{
			Kind: "error", Error: "json_parse: " + err.Error(),
		})
		return
	}
	if len(req.Argv) == 0 {
		a.replyChunk(nc, msg.Reply, proto.ExecChunk{
			Kind: "error", Error: "argv required",
		})
		return
	}

	pid := proc.NewPID()
	a.cfg.Logger.Info("agent: exec", "pid", pid, "argv", req.Argv)
	a.replyChunk(nc, msg.Reply, proto.ExecChunk{Kind: "started", PID: pid})
	// h1 C1: lifecycle events go through the courier (current-conn, ACKed,
	// register-replayed) — never a fire-and-forget publish on the captured
	// spawn-time conn, which silently lost every exit after a conn rebuild
	// (zombie class a, the 2026-08-04 incident). Since S1 the proc.started
	// is enqueued by startExecPipes right after the real spawn and BEFORE
	// the child enters the process table (the single started site shared
	// with the reliable runtime), so a spawn that never happened leaves no
	// EXITED(-1) row behind — the same convention `run` has always had.
	// origin: docs/reviews/hot-update-reliable-exec-plan.md §3.1.

	exitCode, err := a.runChild(nc, msg.Reply, pid, &req)

	if err != nil {
		a.replyChunk(nc, msg.Reply, proto.ExecChunk{
			Kind: "error", PID: pid, Error: err.Error(),
		})
		return
	}
	a.replyChunk(nc, msg.Reply, proto.ExecChunk{
		Kind: "exit", PID: pid, ExitCode: exitCode,
	})
}

// startExecPipes starts one validated exec request using the shared safe-spawn
// policy and transfers successful child wait/signal ownership to workloads.
// Success returns explicit parent pipe ends with AdoptDraining already active;
// the caller owns stdin, output delivery, AllowReap/Wait and final retirement.
// No successful child is waited here, and no courier or transport is touched.
//
// Failure returns no pipes and closes every owned stdio end. A timed-out spawn
// keeps the existing abandoned-helper waiter and wedge-slot lifecycle. Warning
// is retained after a successful safe Start, including a later adoption failure,
// so each caller can deliver that existing side effect on its chosen stream.
func startExecPipes(a *Agent, pid string, req *proto.ExecReq) (_ *workload.Stdio, warning string, err error) {
	cmd, decision, err := a.buildExecCmd(req)
	if err != nil {
		// Hung-fs fail-fast (remote_fs_unhealthy / _unsafe_cwd / _not_found).
		return nil, warning, err
	}
	stdio, err := workload.NewStdio()
	if err != nil {
		return nil, warning, err
	}
	transferred := false
	defer func() {
		if !transferred {
			stdio.Close()
		}
	}()
	stdio.Bind(cmd)
	var handshake *spawnexec.Handshake
	if decision.Active {
		// Do not abandon a direct cmd.Start goroutine: a target execve stalled in
		// the kernel can leave that goroutine unable to reach a GC safepoint and
		// freeze the entire agent at the next stop-the-world. The local helper
		// execs quickly; only its independent runtime touches the risky target.
		cmd, handshake, err = spawnexec.Prepare(cmd)
		if err != nil {
			return nil, warning, err
		}
		setExecProcGroup(cmd) // helper is the target subtree's group leader
	}

	// Start. In safe mode the execve is bounded by a spawn-window deadline so a
	// D-state hang (e.g. a TOCTOU mount death after resolution) is abandoned and
	// surfaced as remote_fs_spawn_timeout instead of wedging the handler forever.
	// On abandon we close the stdout/stderr pipe read ends and reap the cmd once
	// its execve eventually returns, so the pipe fds are reclaimed rather than
	// leaked unbounded over a long outage (review M4).
	if decision.Active {
		err := a.startBounded(
			func() error { return handshake.Start(cmd) },
			func() {
				handshake.Cancel()
				_ = stdio.Stdout.Close()
				_ = stdio.Stderr.Close()
			},
			func(startErr error) {
				reapAbandonedExecChild(a, cmd, startErr)
			},
		)
		if err != nil {
			// The run side has logged this since v0.3.3 (run.go); exec had nothing,
			// so timan107's 30s spawn timeouts left no trace at all in agent.log and
			// gotcha #81 had to be attributed entirely from the ctl side.
			a.cfg.Logger.Warn("agent: exec spawn bounded-start failed",
				"pid", pid, "argv0", req.Argv[0], "cwd", req.Cwd, "err", err)
			return nil, warning, err
		}
		warning = decision.Warn
	} else if err := cmd.Start(); err != nil {
		return nil, warning, err
	}
	stdio.CloseChild()

	// C-5 (hot-update S1): the courier owns the proc.started BEFORE the child
	// becomes visible in the process table. A register snapshot taken between
	// the two reads (owners first, then the courier) then always sees either
	// a pending started or a table row already covered by one; the reverse
	// order re-opened the G.1 false-orphan window at microsecond scale. The
	// identity is captured once here and shared by the started event, the
	// table row and the reliable runtime's Started hook.
	// origin: docs/reviews/hot-update-reliable-exec-plan.md §3.1.
	id := execChildIdentity(pid, cmd.Process)
	a.courier.enqueueStarted(pid, req.Argv, req.ActorFP, id.BootID, id.StartTimeTicks, id.StartedAt)
	fireAfterEnqueueStarted(a, pid) // test seam: the order above is the C-5 invariant

	// #26: track the live child so an admin EVICT can reap it (setsid-nohup deploys
	// have no cgroup to do it when the daemon self-exits). Pruned when this
	// synchronous handler transfers its exit to the courier, whether it exits on its
	// own or is SIGKILLed by reapManagedChildren.
	if err := a.workloads.AdoptDraining(id, cmd.Process); err != nil {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		_ = cmd.Wait() // adoption failed: the caller still owns this one waiter
		return nil, warning, err
	}
	transferred = true
	return stdio, warning, nil
}

// runChild spawns the child process, streams stdout/stderr to replyTo,
// and returns the exit code. A non-nil error is returned ONLY for setup
// failures (pipe / Start), not for child exit codes — those are the
// returned exitCode value and the caller pubs an "exit" chunk.
func (a *Agent) runChild(nc *nats.Conn, replyTo, pid string, req *proto.ExecReq) (exitCode int, runErr error) {
	// The process table retains the real exit until the courier owns it. A
	// register snapshot can therefore never see a gap between the two owners.
	defer func() {
		a.courier.enqueueExit(pid, exitCode)
		a.unregisterExecChild(pid)
	}()
	stdio, warning, err := startExecPipes(a, pid, req)
	if stdio != nil {
		defer stdio.Close()
	}
	if warning != "" {
		a.replyChunk(nc, replyTo, proto.ExecChunk{Kind: "stderr", Data: []byte(warning + "\n")})
	}
	if err != nil {
		return -1, err
	}
	inputDone := make(chan struct{})
	go func() {
		defer close(inputDone)
		defer func() { _ = stdio.Stdin.Close() }()
		// Explicit ownership replaces os/exec's hidden stdin copier. Empty input
		// closes immediately, preserving the legacy EOF seen by non-PTY children.
		_, _ = stdio.Stdin.Write(req.Stdin)
	}()

	var wg sync.WaitGroup
	wg.Add(2)
	go a.streamPipe(&wg, nc, stdio.Stdout, replyTo, "stdout")
	go a.streamPipe(&wg, nc, stdio.Stderr, replyTo, "stderr")
	wg.Wait()
	if err := a.workloads.AllowReap(pid); err != nil {
		return -1, err
	}
	result, err := a.workloads.Wait(pid)
	// As with Cmd.Wait, a child that exits without reading its input must not
	// retain the parent's writer goroutine. Its descendants may still have the
	// read end, so close the owned writer explicitly after the output drained.
	_ = stdio.Stdin.Close()
	<-inputDone
	if err != nil {
		return -1, err
	}
	if result.Signal != 0 {
		// ExitStatus returns -1 when the child was terminated by a
		// signal rather than a clean exit. Stream a stderr note so
		// the operator can tell e.g. "my own `pkill -f <pattern>`
		// caught the exec child's shell" apart from "agent crashed",
		// but keep the legacy wire shape (exit chunk with ExitCode=-1)
		// so existing ctl tooling that special-cases negative exit
		// codes keeps working.
		note := []byte(fmt.Sprintf(
			"\n[tether agent] child terminated by signal (%s) — usually external pkill / SIGTERM matched the shell's argv\n",
			syscall.Signal(result.Signal).String()))
		a.replyChunk(nc, replyTo, proto.ExecChunk{Kind: "stderr", Data: note})
	}
	return result.Code, nil
}

func (a *Agent) streamPipe(wg *sync.WaitGroup, nc *nats.Conn, r io.ReadCloser, replyTo, kind string) {
	defer wg.Done()
	buf := make([]byte, streamChunkSize)
	for {
		n, err := r.Read(buf)
		if n > 0 {
			data := make([]byte, n)
			copy(data, buf[:n])
			a.replyChunk(nc, replyTo, proto.ExecChunk{Kind: kind, Data: data})
		}
		if err != nil {
			return // io.EOF or pipe closed = child finished
		}
	}
}

// liveConn resolves the connection a reply should actually go out on.
//
// h1 C1: a lifecycle chunk rides the CURRENT conn — after a mid-run conn rebuild the
// captured spawn conn is closed and the chunk (like the old exit publish) would
// silently vanish. Fall back to the captured conn only when no current conn exists
// (tests that never store ncBox).
//
// origin: prerelease audit agent-exec/L3-F1. run.go had this reasoning and applied it
// to exactly one publish; exec's chunks — started, exit, error, and every stdout and
// stderr chunk — all rode the conn captured when the child was spawned. A rebuild
// therefore silenced a running `tether exec` completely, and the caller saw a command
// that produced no output and never exited.
//
// WHAT THIS DOES NOT PROMISE, so nobody reads more into it than it says: chunks are
// not buffered across a rebuild. Whatever the old conn had accepted but not yet
// flushed is gone, so output can have a HOLE at the switchover even though the stream
// resumes. Resolving per chunk means the stream resumes at all; it does not mean it is
// complete. Callers that need completeness have the exit code and the process rows.
//
// A package-level function taking *Agent, not a method: the structural-budget ratchet
// pins this type's method count exactly.
func liveConn(a *Agent, captured *nats.Conn) *nats.Conn {
	if cur := a.ncBox.Load(); cur != nil {
		return cur
	}
	return captured
}

func (a *Agent) replyChunk(nc *nats.Conn, replyTo string, c proto.ExecChunk) {
	if replyTo == "" {
		return
	}
	// Resolved HERE, so every caller — including streamPipe's per-chunk loop — is
	// covered by construction rather than by each one remembering.
	nc = liveConn(a, nc)
	payload, err := json.Marshal(c)
	if err != nil {
		// Programming error — our own types should always marshal.
		a.cfg.Logger.Warn("agent: marshal ExecChunk", "err", err, "kind", c.Kind)
		return
	}
	if err := nc.Publish(replyTo, payload); err != nil {
		a.cfg.Logger.Warn("agent: reply chunk pub", "err", err, "kind", c.Kind)
	}
}

// pubProcStarted / pubProcStartedWithTriple / pubProcExit were DELETED in h1
// C1: they fire-and-forget published on a CAPTURED conn, which is exactly the
// zombie-class-a defect (a mid-run conn rebuild silently lost every exit).
// Their replacement is the procCourier (proc_delivery.go) — current-conn,
// ACKed, register-replayed. The (boot_id, start_time_ticks) full-info triple
// the PTY variant carried now rides enqueueStarted's parameters; exec children
// pass ""/0 as before (sync lifecycle, no G.1 verification need).

func envSliceFromMap(m map[string]string) []string { return spawnsafe.EnvSliceFromMap(m) }
func (a *Agent) buildExecCmd(req *proto.ExecReq) (*exec.Cmd, spawnsafe.Decision, error) {
	return spawnsafe.BuildExecCmd(a.spawnPolicy, req, a.execBaseEnv(req))
}
func setExecProcGroup(cmd *exec.Cmd) { spawnsafe.SetExecProcGroup(cmd) }

// execBaseEnv is the env an exec child would otherwise run with: the override
// map when set (which REPLACES the agent env, matching today's semantics), else
// the inherited agent environment. The spawn policy sanitizes this base's PATH
// when active.
func (a *Agent) execBaseEnv(req *proto.ExecReq) []string {
	if len(req.Env) > 0 {
		return envSliceFromMap(req.Env)
	}
	// Defence in depth: the agent already unsets the lineage variables from its
	// own environment at startup (mintInstanceID), so os.Environ() cannot carry
	// them. Stripping again costs one pass and makes this call site correct even
	// if something re-set them.
	return stripInstanceEnv(os.Environ())
}

// startBounded runs start (a cmd.Start) under the safe-mode spawn-window
// deadline. Contract (abandon → onAbandon → late reapOnReturn → slot release, and
// why each step exists) lives on spawnsafe.RunStartWithCleanup; this is a thin
// binding of the agent's configured deadline to it.
//
// It used to be a second, line-for-line copy of that watchdog. That duplication is
// how gotcha #81's fix nearly shipped incomplete: the healthy-verdict invalidation
// has to hang off EVERY execve watchdog, and a second copy is a second place to
// forget it — the same shape as the internal/tunnel fence that took three review
// rounds to find in three different kill paths. Collapsing leaves exactly two
// mint sites for ErrSpawnTimeout, both inside spawnsafe, which is what lets
// test/architecture/spawn_stall_evidence_test.go pin them by exact count.
//
// The wrapper stays (rather than inlining the policy call at both use sites)
// because it binds a.spawnTimeout() in one place and keeps the injectable
// callbacks that make abandon/reap accounting hermetically testable without a
// real D-state execve.

func reapAbandonedExecChild(a *Agent, cmd *exec.Cmd, startErr error) {
	spawnsafe.ReapAbandonedExecChild(a.cfg.Logger, cmd, startErr)
}

func (a *Agent) startBounded(start func() error, onAbandon func(), reapOnReturn func(error)) error {
	return a.spawnPolicy.RunStartWithCleanup(start, a.spawnTimeout(), onAbandon, reapOnReturn)
}

// remoteFSFailReason maps a spawnsafe fail-fast / watchdog error to its wire
// reason string (RunChunk.Reason / the run-side error_hints key). Falls back to
// "exec_failed" for any non-remote-fs error.
func remoteFSFailReason(err error) string {
	var fe *spawnsafe.FSError
	if errors.As(err, &fe) {
		return fe.Code
	}
	switch {
	case errors.Is(err, spawnsafe.ErrSpawnTimeout):
		return spawnsafe.ReasonSpawnTimeout
	case errors.Is(err, spawnsafe.ErrTooManyWedged):
		return spawnsafe.ReasonTooManyWedged
	default:
		return "exec_failed"
	}
}

func mergeChildEnv(override map[string]string) []string {
	return spawnsafe.MergeChildEnv(stripInstanceEnv(os.Environ()), override)
}

func initProcessRuntime(a *Agent) {
	// The routing lineage can survive a legacy exec without a request table.
	// Use a separate incarnation here; a process dispatched with an image
	// restores that exact runtime instead (restoreImage, hot-update S3) and
	// never reaches this mint.
	cfg := processRuntimeConfig(a)
	cfg.Instance = "process-" + proc.NewPID()
	runtime, err := execsession.New(cfg)
	if err != nil {
		a.cfg.Logger.Debug("agent: reliable process runtime unavailable", "err", err)
		return
	}
	a.processRuntime = runtime
}

// processBootClock is captured into each runtime configuration. Tests can
// advance terminal retention without changing the process table's real clock.
// origin: hot-update S4 plan §6.1 / T26
var processBootClock = procrequest.BootNow

// processRuntimeConfig is the runtime's adapter into this agent: the shared
// process table and exec/PTY hooks. Instance is left empty — the mint fills
// it for a fresh runtime, an imported image supplies its own (the lease keys
// of every carried task are bound to it).
func processRuntimeConfig(a *Agent) execsession.Config {
	// origin: hot-update S5 plan §3.4 / T30. Every domain keeps this pointer;
	// initializing it before either owner starts prevents split reservations.
	if a.replayBudget == nil {
		a.replayBudget = &workload.ReplayBudget{Limit: workload.DefaultReplayBudget}
	}
	return execsession.Config{
		Logger:             a.cfg.Logger,
		Manager:            &a.workloads,
		Clock:              processBootClock,
		SharedReplayBudget: a.replayBudget,
		Start: func(pid string, req *proto.ExecReq) (*workload.Stdio, string, error) {
			return startExecPipes(a, pid, req)
		},
		StartPTY: func(pid string, req *proto.RunReq) (*pty.Session, string, error) {
			return startRunPTY(a, pid, req)
		},
		// The runtime owns the payload-free audit because it also owns RequestID.
		// origin: hot-update S4 plan §9.1; P18.
		PTYStarted:   func(string, proto.RunReq, workload.Identity) {},
		PTYAdmission: func() error { return checkPTYAdmission(a) },
		// The proc.started itself is enqueued by startExecPipes (the single
		// started site, before adoption — C-5). This hook only leaves the
		// audit line drills count: exactly one `agent: exec … reliable=true`
		// per real spawn, with the OS identity the table row carries.
		Started: func(pid string, req proto.ExecReq, id workload.Identity) {
			a.cfg.Logger.Info("agent: exec", "pid", pid, "argv", req.Argv, "reliable", true,
				"os_pid", id.OSPID, "start_time_ticks", id.StartTimeTicks)
		},
		Exited: func(pid string, code int) { a.courier.enqueueExit(pid, code) },
		Warn:   func(msg string, err error) { a.cfg.Logger.Warn(msg, "err", err) },
	}
}

// processRefusalCode maps a runtime refusal to the stable wire code the client
// classifies by (proto.CodeProcess*). Unknown errors carry no code and the
// client falls back to the message prefix. origin: hot-update reliable-exec plan §3.4.
func processRefusalCode(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, procrequest.ErrInstance):
		return proto.CodeProcessInstanceLost
	case errors.Is(err, procrequest.ErrCapacity):
		return proto.CodeProcessCapacity
	case errors.Is(err, procrequest.ErrExpired):
		return proto.CodeProcessExpired
	case errors.Is(err, procrequest.ErrConflict), errors.Is(err, workload.ErrReplayOffset):
		return proto.CodeProcessConflict
	case errors.Is(err, procrequest.ErrStale):
		return proto.CodeProcessStale
	// origin: hot-update S4 CLOSE admission contract / plan §4.1
	// The composite also matches ErrBusy: classify retained recovery first.
	case errors.Is(err, execsession.ErrRollbackPending):
		return proto.CodeProcessRecovering
	case errors.Is(err, procrequest.ErrBusy), errors.Is(err, procrequest.ErrFrozen):
		return proto.CodeProcessBusy
	case errors.Is(err, execsession.ErrRevoked):
		return proto.CodeProcessRevoked
	case errors.Is(err, execsession.ErrOutputDiscarded):
		return proto.CodeProcessOutputDiscarded
	case errors.Is(err, execsession.ErrClosed):
		return proto.CodeProcessRuntimeClosed
	case errors.Is(err, procrequest.ErrIdentity), errors.Is(err, procrequest.ErrUnknown), errors.Is(err, execsession.ErrTerminalUnrecorded):
		return proto.CodeProcessIdentity
	}
	return ""
}

func handleProcessResumeForwarded(a *Agent, nc *nats.Conn, msg *nats.Msg) {
	if msg.Reply == "" || msg.Subject != proto.SubjCmdForwarded(a.cfg.SID, nidOf(a), proto.ProcessResumeVerb) {
		return
	}
	var req proto.ProcessResumeReq
	reply := proto.ProcessResumeReply{Version: proto.ProcessResumeVersion, Kind: "error"}
	err := json.Unmarshal(msg.Data, &req)
	if err == nil {
		reply.CallID = req.CallID
		if a.processRuntime == nil {
			err = errProcessUnsupported
		} else {
			lineage := req.ClientLineage
			if req.Handle != nil {
				lineage = req.Handle.Lease.ClientLineage
			}
			scope := procrequest.Scope{SID: a.cfg.SID, NID: nidOf(a), ActorFP: req.ActorFP, ClientLineage: lineage}
			req = ownProcessRun(scope, req)
			reply, err = a.processRuntime.Handle(scope, req, msg.Reply)
		}
	}
	if err != nil {
		code := processRefusalCode(err)
		if code == "" {
			// A newer kind-specific runtime may already classify its refusal.
			// Preserve that typed code while replacing only the wire envelope.
			code = reply.Code
		}
		if errors.Is(err, errProcessUnsupported) {
			code = proto.CodeProcessUnsupported
		}
		// A composite literal so the wire-code coverage gate sees this Code
		// site; its outcomes are the proto.CodeProcess* constants.
		reply = proto.ProcessResumeReply{Version: proto.ProcessResumeVersion, CallID: reply.CallID,
			Kind: "error", Code: code, Error: err.Error()}
		// origin: hot-update S4 plan §2.1/§3.3; P10 integration — every PTY reply,
		// refusals included, names its task kind and carries the fixed empty stderr,
		// or the reliable client rejects the envelope instead of reading the code.
		if req.Kind == proto.ProcessKindPTY {
			reply.TaskKind = proto.ProcessKindPTY
			reply.Stderr = &proto.ProcessOutput{FIN: true}
		}
	}
	body, marshalErr := json.Marshal(reply)
	if marshalErr != nil {
		return
	}
	if conn := liveConn(a, nc); conn != nil {
		_ = conn.Publish(msg.Reply, body)
	}
}

// ownProcessRun severs caller-owned storage before the runtime's asynchronous
// startup can observe it. Only the broker-stamped scope supplies audit identity.
// origin: hot-update S4 plan §2.1 / T28
func ownProcessRun(scope procrequest.Scope, req proto.ProcessResumeReq) proto.ProcessResumeReq {
	if req.Run != nil {
		run := *req.Run
		run.Argv = append([]string(nil), run.Argv...)
		run.Env = maps.Clone(run.Env)
		run.ActorFP = scope.ActorFP
		req.Run = &run
	}
	return req
}

// errProcessUnsupported is the agent's own "no runtime here" refusal; its
// text is the legacy-fallback signal older clients match on.
var errProcessUnsupported = errors.New("unsupported process protocol")

// revokeSessionWorkloads answers the broker's session_destroyed for this
// agent's session: every reliable task is killed and tombstoned (Revoke),
// every legacy exec child and PTY session is SIGKILLed. The daemon itself
// stays up — exiting would let a systemd control-group stop take every
// unrelated workload in the unit with it; the register refusal path retires
// the daemon on its own schedule. origin: hot-update reliable-exec plan §3.5 / §-1 Q3.
func revokeSessionWorkloads(a *Agent, reason string) {
	killed := 0
	if a.processRuntime != nil {
		// The runtime stamps the revocation with the clock that issued the
		// leases it will refuse; a clock failure is logged, never a silent
		// "nothing to revoke" (S1 review P6). The legacy reap below runs
		// regardless.
		n, err := a.processRuntime.Revoke(a.cfg.SID, reason)
		if err != nil {
			a.cfg.Logger.Error("agent: reliable tasks NOT revoked", "reason", reason, "err", err)
		}
		killed = n
	}
	// origin: hot-update S7 plan §6.4 / P20. Authority loss also closes transfer byte gates.
	reapRevokedAgentTransfers(a)
	a.cfg.Logger.Warn("agent: session revoked; managed workloads killed", "reason", reason, "reliable_killed", killed)
}

// sweepProcessRuntime bounds abandoned terminals on an idle node; the agent's
// heartbeat tick drives it so no extra goroutine or ctx site is needed.
func sweepProcessRuntime(a *Agent) {
	if a.processRuntime == nil {
		return
	}
	res, err := a.processRuntime.Sweep()
	if err != nil {
		a.cfg.Logger.Warn("agent: abandoned exec sweep skipped", "err", err)
		return
	}
	if res != (execsession.SweepResult{}) {
		a.cfg.Logger.Info("agent: abandoned exec results reclaimed", "discarded", res.Discarded,
			"expired", res.Expired, "truncated", res.Truncated)
	}
}

func stopProcessRuntime(ctx context.Context, a *Agent) {
	// origin: hot-update S3 plan §2.5. Close of an exported runtime discards
	// it, so Run must wait for a failed exec to thaw before cleaning up.
	a.cutMu.Lock()
	defer a.cutMu.Unlock()
	if a.processRuntime == nil {
		return
	}
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := a.processRuntime.Close(cleanup); err != nil {
		a.cfg.Logger.Warn("agent: process runtime cleanup incomplete", "err", err)
	}
}
