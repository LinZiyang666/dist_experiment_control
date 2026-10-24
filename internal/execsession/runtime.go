// Package execsession owns reliable exec requests and their original byte
// streams. Connections carry bounded replay batches but own no child or pipe.
package execsession

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/LinZiyang666/tether/internal/drillobserve"
	"github.com/LinZiyang666/tether/internal/proc"
	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/pty"
	"github.com/LinZiyang666/tether/internal/ptysession"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/workload"
)

// Start transfers explicit pipes and a child already adopted by Manager on
// success. On failure it retains no pipes; abandoned spawn helpers remain the
// adapter's responsibility. Lifecycle callbacks must only enqueue ownership,
// never wait for a control connection or an acknowledgement.
//
// Started fires AFTER the real Start returned and the child is provably in
// Manager (its OS identity is passed along), never before: a "started" that
// precedes the spawn is exactly the G.1 false-orphan window S1 closes.
// origin: docs/reviews/hot-update-reliable-exec-plan.md §3.1.
type Config struct {
	// Logger receives passive, payload-free PTY receipts through the agent handler.
	Logger *slog.Logger
	// StartPTY success transfers an adopted draining child, never raw pipes.
	// origin: hot-update S4 plan §2.3
	StartPTY     func(string, *proto.RunReq) (*pty.Session, string, error)
	PTYStarted   func(string, proto.RunReq, workload.Identity)
	PTYAdmission func() error
	// PTYActivate optionally wraps a real actor activation at the responsibility
	// boundary. Fault injection may fail a later owner after an earlier one ran.
	// origin: hot-update S4 plan §4.2 / T23
	PTYActivate    func(string, *ptysession.Actor) error
	Instance       string
	Manager        *workload.Manager
	Start          func(string, *proto.ExecReq) (*workload.Stdio, string, error)
	Started        func(string, proto.ExecReq, workload.Identity)
	Exited         func(string, int)
	ReplayCapacity int64
	// ReplayBudget bounds the replay capacity reserved across every task of
	// this runtime (0 = workload.DefaultReplayBudget). It is THIS binary's
	// bound: an imported image's own figure is recorded, never adopted.
	ReplayBudget int64
	// SharedReplayBudget is the node reservation owner shared with flow/PTY.
	// When present, its Limit takes precedence over ReplayBudget and is never
	// overwritten by construction or import. Nil preserves standalone runtimes.
	// origin: hot-update S5 plan §3.4 / T30
	SharedReplayBudget *workload.ReplayBudget
	// Clock returns CLOCK_BOOTTIME nanoseconds; nil selects the real clock.
	// The registry shares it, so lease expiry, abandoned-terminal grace and
	// tombstone TTL are measured on one clock.
	Clock func() (int64, error)
	// Warn receives faults the runtime survives but an operator should see
	// (a terminal the registry refused to record). nil drops them.
	Warn func(msg string, err error)
}

var (
	// ErrClosed is returned by every operation on a runtime after Close.
	ErrClosed = errors.New("exec session: runtime closed")
	// ErrRevoked is returned once the task's session authority revoked it:
	// nothing can be created, resumed or revived under a lease that predates
	// the revocation. origin: docs/reviews/hot-update-reliable-exec-plan.md §3.5.
	ErrRevoked = errors.New("exec session: task revoked by session authority")
	// ErrOutputDiscarded reports that the client abandoned a terminal task
	// for longer than its lease plus AbandonedTerminalGrace: the terminal is
	// still queryable from its tombstone, the bytes are gone.
	ErrOutputDiscarded = errors.New("exec session: output discarded after the client abandoned the terminal")
	// ErrTerminalUnrecorded means the child ended but the registry refused
	// the terminal (our bug or a frozen table): the process table and the
	// broker exit event are still correct, only settlement is impossible.
	ErrTerminalUnrecorded = errors.New("exec session: terminal result could not be recorded")
	errUnadopted          = errors.New("exec session: adapter returned a child that is not adopted")
)

const (
	// AbandonedTerminalGrace bounds how long a terminal task keeps its replay
	// logs after the last client touch (and after its lease expired). Beyond
	// it the bytes are discarded and a tombstone keeps the terminal; without
	// a bound, abandoned ctl processes exhausted the node's replay budget.
	// origin: S0 internal review (protocol verifier, missed).
	AbandonedTerminalGrace = 2 * time.Minute
	// DiscardTombstoneTTL is how long a discarded terminal stays queryable
	// before the registry record is settled and collected.
	DiscardTombstoneTTL = 15 * time.Minute
	// AbandonedRunningGrace bounds a RUNNING task nobody acknowledges: once
	// its lease has expired and no client touched it for this long, a
	// stalled (full) output stream is truncated so the child is not held on
	// a blocked write forever (workload.Output.Discard). The child keeps
	// running, the retained bytes stay readable, and a client that returns
	// sees the stream end as aborted at the truncation point. It is
	// deliberately long: an operator's ctl that lost its network for ten
	// minutes must find its bytes intact. origin: hot-update S2 plan D11
	// (the running-abandoned half of tasklist C-6).
	AbandonedRunningGrace = 15 * time.Minute
)

type task struct {
	kind string
	pty  *ptysession.Actor
	// ptyState is used only when spawn/setup failed before an actor existed.
	// The two reserved logs still belong to this handle until settlement.
	ptyState *ptysession.Image
	control  ptysession.ControlReceipt
	opMu     sync.Mutex // serializes protocol calls outside Runtime.mu
	handle   proto.ProcessHandle
	pid      string
	logs     [3]*workload.Replay // stdin, stdout, stderr
	input    *workload.Input
	output   [2]*workload.Output
	// abort records, per output stream, why the runtime ended it WITHOUT a
	// true EOF: its actor was never built after the child already existed.
	// Bytes up to End stay deliverable, nothing follows, Replay.FIN is never
	// set. A stream whose pump exists records its own failure in
	// OutputState.ReadError instead; streamAbort joins the two.
	// origin: docs/reviews/hot-update-stages-plan.md §-1 OQ5 (S0 F2).
	abort [2]string
	// revoked names the session authority reason once Revoke killed this
	// task; a non-terminal revoked task refuses every operation.
	revoked string
	// lostTerminal is set when the child ended but the registry refused the
	// terminal: the task is still retired by Close, never settled.
	lostTerminal bool
	// terminalAt and lastTouch (boot ns) drive the abandoned-terminal sweep.
	terminalAt int64
	lastTouch  int64
	done       chan struct{}
	// owned is true while a goroutine will publish this task (the startup
	// owner from create, or the observer Thaw starts for an imported live
	// task). An imported task is unowned until Thaw; Close retires such a
	// task itself, since nobody else will ever close done.
	owned bool
	// frozen holds the stream states and borrowed files Freeze collected;
	// Export reads them, Thaw/Discard/Detach drop them.
	frozen *taskFreeze
}

// taskFreeze is one task's half of a runtime cut: the state each stream was
// frozen at and the ORIGINAL files (borrowed, never closed here) that
// handoff.Prepare duplicates. A stream is one of three shapes: an actor
// (Output/Input set), a bare log (allocated but its pump was never built —
// failStart and the post-Start setup-failure abort), or absent (allocate
// failed part-way). have[i] is false for an absent stream.
type taskFreeze struct {
	pty    *ptysession.Image
	have   [3]bool
	replay [3]workload.ReplayState
	input  *workload.InputState
	output [2]*workload.OutputState
	files  map[string]*os.File
}

// tombstone keeps a discarded task's terminal and final stream offsets so a
// client that already holds every byte can still settle exactly once.
type tombstone struct {
	kind     string
	pty      *ptysession.Tombstone
	scope    procrequest.Scope
	handle   proto.ProcessHandle
	terminal procrequest.Terminal
	finalEnd [2]uint64
	finalFIN [2]bool
	atBootNS int64
}

type revocation struct {
	generation   uint64 // prevents an older clock completion from clearing a newer mark
	clockUnknown bool
	atBootNS     int64
	reason       string
}

type Runtime struct {
	// Every replay allocation/restoration charges this exact pointer. The value
	// budget below backs standalone runtimes; it is never reserved independently
	// of this pointer. A supplied node budget is never copied.
	// origin: hot-update S5 plan §2 / §3.4 / L13
	sharedReplayBudget *workload.ReplayBudget

	mu       sync.Mutex
	cfg      Config
	registry *procrequest.Registry
	budget   workload.ReplayBudget // standalone backing storage; not a second reservation owner
	tasks    map[string]*task
	// discarded holds tombstones by pid; revocations holds session
	// revocations by SID until the longest lease they could invalidate
	// has expired.
	discarded      map[string]*tombstone
	revocations    map[string]revocation
	now            func() (int64, error)
	ptyCarrierLost bool // one loss/restoration pair across failed session setup attempts
	lastBootNS     int64
	closed         bool
	// The image fence (fence.go). quiescing: Freeze is waiting for every
	// "starting" record to settle — create blocks, attach ops still flow.
	// frozen: Freeze succeeded and Thaw has not run — every op blocks on
	// admission (a planned image switch within the request budget is never
	// visible to a client as an error, S2 plan D1). exported: Export ran on this
	// frozen runtime, so
	// its resources may already have a second owner and it may only be
	// Discarded or Thawed. imported: Import built this runtime; its live
	// tasks get their observer at Thaw. frozenAt: boot ns of the cut.
	quiescing     bool
	freezing      bool // intake closed; pump barriers run outside mu
	activating    bool
	activation    activationProgress // per-owner completion and owned freeze recovery
	partialActive bool
	// activatePTY permits failure injection at the otherwise infallible commit.
	// Successful calls always transfer to the real actor; production uses nil.
	activatePTY func(*ptysession.Actor) error
	// thawExec permits an activation failure after real pipe thaw for tests.
	// Production uses nil; partial progress still belongs to this transaction.
	thawExec       func(*task) error
	ioActive       int // calls holding task references outside mu
	frozen         bool
	exported       bool
	imported       bool
	thawed         bool // an imported runtime was activated once: Detach is no longer an exit
	frozenAt       int64
	frozenRegistry procrequest.State
	cond           *sync.Cond
}

func New(cfg Config) (*Runtime, error) {
	if cfg.Manager == nil || cfg.Start == nil || cfg.Started == nil || cfg.Exited == nil {
		return nil, fmt.Errorf("exec session: incomplete resource adapter")
	}
	if cfg.ReplayCapacity == 0 {
		cfg.ReplayCapacity = workload.DefaultReplayCapacity
	}
	// A malformed node bound must fail before constructing any runtime owner.
	// origin: hot-update S5 plan §3.4 / T30
	if cfg.ReplayCapacity < 1 || cfg.ReplayCapacity > workload.DefaultReplayCapacity || cfg.ReplayBudget < 0 || cfg.SharedReplayBudget != nil && cfg.SharedReplayBudget.Limit < 0 {
		return nil, fmt.Errorf("exec session: invalid replay capacity or budget")
	}
	if cfg.Clock == nil {
		cfg.Clock = procrequest.BootNow
	}
	registry, err := procrequest.NewWithClock(cfg.Instance, 0, 0, cfg.Clock)
	if err != nil {
		return nil, err
	}
	r := &Runtime{cfg: cfg, registry: registry, tasks: make(map[string]*task),
		discarded: make(map[string]*tombstone), revocations: make(map[string]revocation), now: cfg.Clock}
	initializeReplayBudget(r)
	r.cond = sync.NewCond(&r.mu)
	return r, nil
}

// Handle is called only after current broker authorization. One response
// carries up to 32 KiB per output stream. Repeating a call is safe; CallID lets
// the client ignore an old reply without confusing it with a newer ACK.
func (r *Runtime) Handle(scope procrequest.Scope, req proto.ProcessResumeReq, inbox string) (proto.ProcessResumeReply, error) {
	reply := proto.ProcessResumeReply{Version: proto.ProcessResumeVersion, CallID: req.CallID,
		Kind: "status", TargetInstance: r.cfg.Instance, AttachEpoch: req.AttachEpoch}
	// The image fence (S2 plan D1) queues a planned switch transparently
	// within RequestLatency. Quiescing delays only create; a frozen runtime
	// delays every op. Old waiters vanish with exec and the same identity's
	// 500 ms resend reaches the new image; Thaw/rollback/Close wakes local
	// waiters. On expiry, process_recovering keeps both clients retrying within
	// their original recovery budgets without releasing resources or cancelling
	// the recovery owner. Genuine process_busy remains a final refusal.
	// origin: hot-update S4 CLOSE admission contract / plan §4.1
	if err := admitHandleUntil(r, req.Op, time.Now().Add(upgradebudget.RequestLatency)); err != nil {
		return reply, err
	}
	defer r.mu.Unlock()
	if req.Version != proto.ProcessResumeVersion || req.CallID == 0 || req.ActorFP != scope.ActorFP {
		return reply, procrequest.ErrIdentity
	}
	if err := validatePayload(req); err != nil {
		return reply, err
	}
	if req.Kind == proto.ProcessKindPTY {
		return r.handlePTYLocked(scope, req, inbox, reply)
	}
	if req.Op == "probe" {
		return handleExecProbeLocked(r, scope, req, reply)
	}
	if req.Handle == nil || req.AttachEpoch == 0 {
		return reply, procrequest.ErrIdentity
	}
	reply.RequestID = req.Handle.RequestID
	now, clockErr := r.now()
	if clockErr != nil || now < 0 {
		return reply, fmt.Errorf("exec session: boot clock unavailable: %w", clockErr)
	}
	r.lastBootNS = max(r.lastBootNS, now)
	record, err := resolveExecRecordLocked(r, scope, req, inbox, now)
	if err != nil {
		return reply, err
	}
	if record.Kind != "" {
		return reply, procrequest.ErrIdentity
	}
	reply.PID, reply.Phase = record.PID, record.Phase
	if record.Terminal != nil {
		reply.TerminalID, reply.ExitCode, reply.Signal = record.Terminal.ID, record.Terminal.ExitCode, record.Terminal.Signal
		reply.Error = record.Terminal.Error
	}
	if record.Phase == "settled" {
		if req.Op == "terminal-ack" && req.TerminalID != reply.TerminalID {
			return reply, procrequest.ErrConflict
		}
		return reply, nil
	}
	return handleExecTaskLocked(r, scope, req, reply, record, now)
}

func handleExecProbeLocked(r *Runtime, scope procrequest.Scope, req proto.ProcessResumeReq, reply proto.ProcessResumeReply) (proto.ProcessResumeReply, error) {
	if req.Handle != nil || req.Exec != nil || scope.ClientLineage != req.ClientLineage {
		return reply, procrequest.ErrIdentity
	}
	lease, err := r.registry.Issue(scope)
	if err == nil {
		reply.Kind, reply.Lease = "capability", &lease
		r.lastBootNS = max(r.lastBootNS, lease.IssuedBootNS)
	}
	return reply, err
}

// resolveExecRecordLocked preserves admission order: validate create, reject
// revoked authority, collect, reserve by identity, then publish one startup owner.
func resolveExecRecordLocked(r *Runtime, scope procrequest.Scope, req proto.ProcessResumeReq, inbox string, now int64) (procrequest.Record, error) {
	var record procrequest.Record
	var err error
	switch req.Op {
	case "create":
		if req.Exec == nil || req.StdoutACK != 0 || req.StderrACK != 0 || req.StdoutFIN || req.StderrFIN || req.TerminalID != "" {
			return record, procrequest.ErrIdentity
		}
		// A lease issued before the session authority revoked this session
		// can never create; one issued after it belongs to a later session
		// of the same name.
		if rev, ok := r.revocations[scope.SID]; ok && (rev.clockUnknown || req.Handle.Lease.IssuedBootNS < rev.atBootNS) {
			return record, ErrRevoked
		}
		var fresh bool
		r.registry.Collect()
		r.sweepLocked(now)
		record, fresh, err = r.registry.Reserve(scope, *req.Handle, *req.Exec, proc.NewPID(), inbox, req.AttachEpoch)
		if err == nil && fresh {
			t := &task{handle: *req.Handle, pid: record.PID, done: make(chan struct{}), lastTouch: now, owned: true}
			r.tasks[record.PID] = t
			// The immutable request came from a decoder; copy its reference
			// fields before letting the startup owner outlive this handler.
			spec := copySpec(*req.Exec)
			spec.ActorFP = scope.ActorFP
			go r.start(t, spec)
		}
	case "resume", "ack", "terminal-ack":
		if req.Exec != nil {
			return record, procrequest.ErrIdentity
		}
		record, err = r.registry.Lookup(scope, *req.Handle)
		if err == nil && record.Kind != "" {
			return record, procrequest.ErrIdentity
		}
		if err == nil {
			record, err = r.registry.Attach(scope, *req.Handle, inbox, req.AttachEpoch)
		}
	default:
		return record, fmt.Errorf("exec session: unsupported operation")
	}
	return record, err
}

func handleExecTaskLocked(r *Runtime, scope procrequest.Scope, req proto.ProcessResumeReq, reply proto.ProcessResumeReply, record procrequest.Record, now int64) (proto.ProcessResumeReply, error) {
	t := r.tasks[record.PID]
	if t == nil {
		if ts := r.discarded[record.PID]; ts != nil {
			return r.handleDiscarded(scope, req, reply, ts)
		}
		return reply, fmt.Errorf("exec session: missing resource owner")
	}
	t.lastTouch = now
	if t.revoked != "" && record.Terminal == nil {
		return reply, ErrRevoked
	}
	if t.lostTerminal {
		return reply, ErrTerminalUnrecorded
	}
	if record.Phase == "starting" {
		if req.StdoutACK != 0 || req.StderrACK != 0 || req.StdoutFIN || req.StderrFIN || req.Op == "terminal-ack" {
			return reply, procrequest.ErrConflict
		}
		return reply, nil
	}
	if err := acknowledge(t, req); err != nil {
		return reply, err
	}
	var err error
	reply.Stdout, err = outputBatch(t, 0)
	if err == nil {
		reply.Stderr, err = outputBatch(t, 1)
	}
	if err != nil {
		return reply, err
	}
	if req.Op == "terminal-ack" {
		return settleExecLocked(r, scope, req, reply, record, t)
	}
	return reply, nil
}

func settleExecLocked(r *Runtime, scope procrequest.Scope, req proto.ProcessResumeReq, reply proto.ProcessResumeReply, record procrequest.Record, t *task) (proto.ProcessResumeReply, error) {
	// acknowledge already proved every FIN flag against a true EOF at End
	// and refused a FIN claim on an aborted stream. Here each stream must
	// be collectable at its final offset: (FIN && ack==End) || (aborted &&
	// ack==End). origin: docs/reviews/hot-update-stages-plan.md §-1 OQ5.
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
	return reply, nil
}

// handleDiscarded serves a task whose bytes were discarded by the sweep. A
// client that already acknowledged every byte up to the final offsets may
// still settle exactly once; anyone else learns the terminal and that the
// output is gone.
func (r *Runtime) handleDiscarded(scope procrequest.Scope, req proto.ProcessResumeReq, reply proto.ProcessResumeReply, ts *tombstone) (proto.ProcessResumeReply, error) {
	if ts.kind == proto.ProcessKindPTY {
		fillPTYTombstone(&reply, ts.pty)
	}
	if req.Op == "terminal-ack" && req.TerminalID == ts.terminal.ID &&
		req.StdoutACK == ts.finalEnd[0] && req.StderrACK == ts.finalEnd[1] &&
		req.StdoutFIN == ts.finalFIN[0] && req.StderrFIN == ts.finalFIN[1] {
		if err := r.registry.Settle(scope, *req.Handle, req.TerminalID); err != nil {
			return reply, err
		}
		logPTYDiscardedSettlement(r, reply.PID, req, ts)
		delete(r.discarded, reply.PID)
		reply.Phase = "settled"
		return reply, nil
	}
	return reply, ErrOutputDiscarded
}

func (r *Runtime) start(t *task, spec proto.ExecReq) {
	defer r.conclude(t) // the only publication point; retires under a closed runtime (F1)
	r.mu.Lock()
	err := r.allocate(t, spec.Stdin)
	closed := r.closed
	revoked := t.revoked
	r.mu.Unlock()
	if err != nil || closed || revoked != "" {
		switch {
		case err != nil:
		case closed:
			err = fmt.Errorf("exec session: runtime closed before start")
		default:
			// Revoked between admission and spawn: never start a child for
			// a session that no longer exists (S1 internal review P1).
			err = fmt.Errorf("revoked: %s", revoked)
		}
		r.failStart(t, err)
		return
	}
	stdio, warning, err := r.cfg.Start(t.pid, &spec)
	if err != nil {
		r.failStart(t, err)
		return
	}
	// Start succeeded == the child is already adopted (sole wait/signal
	// owner). An adapter that violates this hands us a child nobody can
	// reap; refuse it as a setup failure rather than spin on AllowReap.
	rec, adopted := r.cfg.Manager.Lookup(t.pid)
	if !adopted {
		stdio.Close()
		r.failStart(t, errUnadopted)
		return
	}
	r.cfg.Started(t.pid, spec, rec.Identity)
	r.mu.Lock()
	if t.revoked != "" {
		// Revoke ran while the startup owner was inside Start: its Signal
		// found no adopted child and killed nothing. The child exists now,
		// so the revocation is carried out here; the pumps below drain its
		// pipes to EOF and observe records the "revoked:" terminal for real.
		// origin: hot-update S1 internal review P1.
		_ = r.cfg.Manager.Signal(t.pid, syscall.SIGKILL)
	}
	if warning != "" {
		warning += "\n"
	}
	// origin: hot-update S5 plan §9.1 / DRILL-OBSERVER. Attribute original
	// pipe FDs before ownership transfers into stream actors.
	if drillobserve.Hook != nil {
		observeRuntimeStdio(stdio)
	}
	t.output[0], err = workload.NewOutput(stdio.Stdout, t.logs[1])
	if err == nil {
		stdio.Stdout = nil
		t.output[1], err = workload.NewOutputWithPrefix(stdio.Stderr, t.logs[2], []byte(warning))
	}
	if err == nil {
		stdio.Stderr = nil
		t.input, err = workload.NewInput(stdio.Stdin, t.logs[0])
	}
	if err == nil {
		stdio.Stdin = nil
		err = r.registry.Running(t.handle, t.pid)
	}
	if err != nil {
		// This child already exists. Terminate the owned subtree; never
		// pretend creation did not run. Every retained byte and this failure
		// stay for the client: creation ran, so its result must remain
		// settleable exactly once. A stream whose actor was never built is
		// ABORTED here at its current End (never a fabricated FIN); a stream
		// whose pump exists keeps draining the killed child's pipe to its true
		// EOF. Closing the task here was F2: it closed logs the protocol still
		// had to deliver. A runtime closed meanwhile is handled by observe's
		// first tick. origin: docs/reviews/hot-update-handoff-failures.md §1 F2.
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

// conclude is the startup owner's single publication point. A runtime that
// closed while this owner was still working has no client left to settle the
// result, so the owner retires its own resources before signalling done. An
// open runtime retains them for terminal-ack. Close therefore never has to be
// called twice, and a Close that gave up on its context loses nothing.
// origin: docs/reviews/hot-update-handoff-failures.md §1 F1.
func (r *Runtime) conclude(t *task) {
	r.mu.Lock()
	if r.closed {
		r.retireLocked(t)
	}
	close(t.done)
	r.mu.Unlock()
}

// retireLocked is the only place a task's table entry is removed and the last
// place its resources are released (observe's closed branch and failStart may
// already have closed some of them). Idempotent: the settle path, Close, the
// abandoned-terminal sweep and a late startup owner under a closed runtime may
// each reach it; the first one does the work.
func (r *Runtime) retireLocked(t *task) {
	delete(r.tasks, t.pid)
	if t.kind == proto.ProcessKindPTY {
		// Closing an actor joins pumps. Pin retirement across the unlocked join
		// so a concurrent cut cannot account a half-released replay reservation.
		r.ioActive++
		r.mu.Unlock()
		closeTask(t)
		r.mu.Lock()
		r.ioActive--
		return
	}
	closeTask(t)
}

// boundedError keeps a failure text acceptable to procrequest.validTerminal
// (at most 4096 bytes of valid UTF-8) and to workload.RestoreOutput's
// ReadError. A text the registry would refuse must never reach Complete: the
// terminal would then be dropped and the task would sit in "starting" with
// its replays reserved until Close. origin: S0 internal review C-1/G2.
func boundedError(s string) string {
	s = strings.ToValidUTF8(s, "�")
	if len(s) > 4096 {
		return "exec session: process setup or wait failed"
	}
	return s
}

func (r *Runtime) allocate(t *task, input []byte) error {
	capacities := [3]int64{max(64<<10, int64(len(input))), r.cfg.ReplayCapacity, r.cfg.ReplayCapacity}
	for i, capacity := range capacities {
		log, err := workload.NewReplay(r.sharedReplayBudget, capacity)
		if err != nil {
			return err
		}
		t.logs[i] = log
	}
	if _, err := t.logs[0].Append(input); err != nil {
		return err
	}
	return t.logs[0].Finish()
}

func (r *Runtime) failStart(t *task, cause error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	// No process was transferred. Empty outputs still have real final
	// offsets so a client can settle this failed attempt exactly once.
	for _, log := range t.logs[1:] {
		if log != nil {
			_ = log.Finish()
		}
	}
	closeInputLocked(t)
	r.finish(t, workload.Exit{Code: -1}, cause)
}

// closeInputLocked releases the stdin side and, by the "nil means released"
// discipline, drops both references: the image exporter must be able to tell
// "this stream is gone" from "this stream exists" without asking a closed
// actor (Replay.Freeze on a closed log answers os.ErrClosed, Input.Freeze
// answers an error — neither is "absent"). origin: S2 plan §1.8.
func closeInputLocked(t *task) {
	if t.input != nil {
		_ = t.input.Close()
	} else if t.logs[0] != nil {
		_ = t.logs[0].Close()
	}
	t.input, t.logs[0] = nil, nil
}

// observe waits until the child's result may be collected, then records it.
// A setup failure is ready at once: the record is still "starting", so Handle
// sends no batches; waiting for the already-built stdout pump to reach EOF
// would deadlock behind a backpressured log nobody can ACK. The child was
// SIGKILLed with its whole owned process group before AllowReap, which is the
// "explicitly terminated" half of Manager's draining contract; the pump keeps
// draining the pipe to its true EOF while the terminal is already collectable
// (terminal-draining). A closed runtime has no client to deliver to, so it
// ends the pumps and logs here and lets the sole reaper finish the record.
func (r *Runtime) observe(t *task, setupErr error) {
	timer := time.NewTicker(10 * time.Millisecond)
	defer timer.Stop()
	for {
		r.mu.Lock()
		if r.frozen || r.freezing || r.activating {
			// The image fence: nothing here may move while a cut is exported
			// — no abort (it would tear the exported snapshot), no reap, no
			// finish. A frozen runtime that was closed was Discarded: the
			// image moved on and this child belongs to the new one, so leave
			// without the closed branch's SIGKILL. origin: S2 plan §1.5.
			if r.closed {
				r.mu.Unlock()
				return
			}
			r.mu.Unlock()
			<-timer.C
			continue
		}
		if t.revoked != "" {
			r.abortStalledRevokedLocked(t) // S1 review P2: a full log of a revoked task never gets ACKed
		}
		ready := setupErr != nil || r.closed || outputsEnded(t)
		if ready {
			if r.closed {
				_ = r.cfg.Manager.Signal(t.pid, syscall.SIGKILL)
				closeTask(t)
			}
			if err := r.cfg.Manager.AllowReap(t.pid); err != nil {
				if errors.Is(err, workload.ErrUnknown) {
					// The adapter's contract (Start succeeded == adopted) was
					// broken after the fact, or the table was cleared under us:
					// there is no child to wait for. Record that as the result
					// instead of polling forever. origin: S0 review §5 item 3.
					closeInputLocked(t)
					if r.finish(t, workload.Exit{Code: -1, WaitError: err.Error()}, err) {
						r.mu.Unlock()
						return
					}
				}
				r.mu.Unlock()
				<-timer.C
				continue
			}
			for _, row := range r.cfg.Manager.Snapshot() {
				if row.ID == t.pid && row.Exit != nil {
					closeInputLocked(t)
					if r.finish(t, *row.Exit, setupErr) {
						r.mu.Unlock()
						return
					}
					break
				}
			}
		}
		r.mu.Unlock()
		<-timer.C
	}
}

// finish records the child's result. It returns false only when the registry
// is frozen so the caller retries; every other registry refusal is our own
// bug, and the process table plus the broker's exit event stay authoritative
// regardless — the task is marked lostTerminal so a client learns it cannot
// be settled instead of waiting. Since S2 the ErrFrozen arm is a belt: the
// image fence gates observe (the only caller) under the same r.mu that
// Freeze holds, and the registry is frozen before the process table, so a
// child that exits during a cut is exported as a zombie or a retained exit
// row and accounted by the next image's observer — this arm is reachable
// only if someone freezes the registry on its own. origin: S2 plan §1.6.
func (r *Runtime) finish(t *task, exit workload.Exit, cause error) bool {
	terminal := procrequest.Terminal{ID: proc.NewPID(), ExitCode: exit.Code, Signal: exit.Signal, Error: exit.WaitError}
	if cause != nil {
		terminal.Error = cause.Error()
	} else if t.revoked != "" {
		terminal.Error = "revoked: " + t.revoked
	}
	terminal.Error = boundedError(terminal.Error)
	// The runtime owns the result before the process table retires it.
	if err := r.registry.Complete(t.handle, t.pid, terminal); err != nil {
		if errors.Is(err, procrequest.ErrFrozen) {
			return false
		}
		t.lostTerminal = true
		if r.cfg.Warn != nil {
			r.cfg.Warn("exec session: terminal result refused by the request registry", err)
		}
	}
	if now, err := r.now(); err == nil {
		t.terminalAt = now
	}
	logPTYExit(r, t, exit)
	r.cfg.Exited(t.pid, exit.Code)
	_ = r.cfg.Manager.Forget(t.pid)
	return true
}

// Close is actual runtime teardown, never connection replacement or freeze.
// A stalled startup keeps its owner until it returns even if ctx expires; that
// owner then retires its own resources (conclude), so Close is never required
// a second time. Tasks whose owner has already published are retired here at
// once, without waiting on ctx: a blocking select against an expired ctx
// picked the ctx arm about half the time and left them behind.
//
// Under the image fence (S2 plan §1.5): a frozen runtime that was Exported
// is only Discarded — its resources may already belong to the next image,
// so nothing is killed or drained. A frozen runtime that was never exported,
// or an imported one nobody thawed, is torn down as usual AFTER its local
// state is thawed here (pumps and registry), so that the observers take the
// S0 closed-branch teardown (SIGKILL, closeTask, AllowReap, finish → Exited
// + Forget) instead of the frozen+closed gate written for a Discard; its
// unowned imported tasks are retired here because no goroutine will ever
// publish them. While the process table is still frozen that teardown
// cannot kill or reap anything, so Close refuses with workload.ErrFrozen
// and changes nothing: thaw the table first (rollback) or Detach (the
// pre-Thaw exit of an imported runtime). origin: S2 internal review
// F4/V1/R3/P6 — the first version tore down without thawing and swallowed
// Signal's ErrFrozen, leaving a killed-but-unreaped ReapHeld zombie and no
// Exited.
func (r *Runtime) Close(ctx context.Context) error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	if r.frozen && r.exported {
		err := r.discardLocked()
		r.mu.Unlock()
		return err
	}
	if r.frozen {
		if r.cfg.Manager.Frozen() {
			r.mu.Unlock()
			return fmt.Errorf("exec session: close under the image fence: %w (thaw the process table first, or Detach)", workload.ErrFrozen)
		}
		r.mu.Unlock()
		if err := r.Thaw(); err != nil {
			return err
		}
		r.mu.Lock()
	}
	// origin: hot-update S4 plan §4.2 / T34
	// A partial activation deliberately keeps admission frozen for a recut.
	// Close commits teardown instead: its observers must be able to publish
	// real terminal results even for actors that never started their pumps.
	if r.partialActive {
		r.registry.Thaw()
		r.partialActive = false
	}
	r.closed = true
	r.quiescing = false
	broadcastAdmissionLocked(r)
	pending := make([]*task, 0, len(r.tasks))
	for _, t := range r.tasks {
		if err := r.cfg.Manager.Signal(t.pid, syscall.SIGKILL); err != nil && errors.Is(err, workload.ErrFrozen) && r.cfg.Warn != nil {
			r.cfg.Warn("exec session: close could not kill a task under a frozen process table", err)
		}
		if !t.owned && t.kind == proto.ProcessKindPTY && t.terminalAt == 0 {
			t.owned = true
			go func(t *task) { defer r.conclude(t); r.observePTY(t, nil) }(t)
		}
		if !t.owned {
			r.retireLocked(t) // imported and never thawed: nobody will publish it
			close(t.done)
			continue
		}
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

func copySpec(spec proto.ExecReq) proto.ExecReq {
	spec.Argv = append([]string(nil), spec.Argv...)
	spec.Stdin = append([]byte(nil), spec.Stdin...)
	if spec.Env != nil {
		env := make(map[string]string, len(spec.Env))
		for k, v := range spec.Env {
			env[k] = v
		}
		spec.Env = env
	}
	return spec
}

// initializeReplayBudget chooses the owner once, before any actor can reserve.
// Retaining a pointer (rather than copying Limit/Used) prevents empty logs in
// different domains from each claiming the same remaining node capacity.
// origin: hot-update S5 plan §3.4 / T30
func initializeReplayBudget(r *Runtime) {
	r.sharedReplayBudget = r.cfg.SharedReplayBudget
	if r.sharedReplayBudget == nil {
		r.budget.Limit = r.cfg.ReplayBudget
		r.sharedReplayBudget = &r.budget
	}
}

// SharedReplayBudget exposes the immutable reservation owner selected at New
// or Import, so composition can connect flow without copying accounting state.
// The caller must configure Limit before any domain starts reserving capacity.
// origin: hot-update S5 plan §3.4 / T30
func SharedReplayBudget(r *Runtime) *workload.ReplayBudget {
	if r == nil {
		return nil
	}
	return r.sharedReplayBudget
}

// ownedReplayBytes requires Runtime.mu or a not-yet-published imported runtime.
// Closed logs can still be referenced during asynchronous teardown, but they
// no longer own reservations. Counting retained bytes would undercharge ACKed
// or empty logs; reading shared Used would charge other domains a second time.
// origin: hot-update S5 plan §3.4 / T30
func ownedReplayBytes(r *Runtime) int64 {
	var used int64
	for _, t := range r.tasks {
		for _, log := range t.logs {
			used += workload.ReplayReservedBytes(log)
		}
	}
	return used
}

// replayLimit normalizes the receiving node bound for pure validation.
// Reservation itself remains atomic in ReplayBudget, including races between
// domain imports after their read-only validation has succeeded.
// origin: hot-update S5 plan §3.4 / T30
func replayLimit(cfg Config) int64 {
	limit := cfg.ReplayBudget
	if cfg.SharedReplayBudget != nil {
		limit = cfg.SharedReplayBudget.Limit
	}
	if limit == 0 {
		limit = workload.DefaultReplayBudget
	}
	return limit
}

// ObserveRuntime reports owned live logs, even between cuts or after ACK.
// It is a package function to preserve Runtime's fixed method ceiling.
// origin: hot-update S5 plan §3.4 / DRILL-OBSERVER
func ObserveRuntime(r *Runtime) {
	if drillobserve.Hook == nil || r == nil {
		return
	}
	r.mu.Lock()
	counts := map[string]*drillobserve.Event{"exec": {Kind: "domain-live", Domain: "exec"}, "pty": {Kind: "domain-live", Domain: "pty"}}
	var files []drillobserve.Event
	for _, t := range r.tasks {
		domain := "exec"
		if t.kind == proto.ProcessKindPTY {
			domain = "pty"
		}
		for _, log := range t.logs {
			state, dev, ino, live := workload.ReplayObservation(log)
			if live {
				e := counts[domain]
				e.Values[1]++
				e.Values[2] += uint64(state.Capacity)
				e.Values[3] += state.End - state.Base
				files = append(files, drillobserve.Event{Kind: "domain-file", Domain: domain, Values: [16]uint64{dev, ino}})
			}
		}
	}
	r.mu.Unlock()
	for _, e := range files {
		drillobserve.Hook(e)
	}
	for _, name := range []string{"exec", "pty"} {
		drillobserve.Hook(*counts[name])
	}
}

func observeRuntimeStdio(stdio *workload.Stdio) {
	for _, file := range []*os.File{stdio.Stdin, stdio.Stdout, stdio.Stderr} {
		if file == nil {
			continue
		}
		info, err := file.Stat()
		if err != nil {
			continue
		}
		if fp, ok := info.Sys().(*syscall.Stat_t); ok {
			drillobserve.Hook(drillobserve.Event{Kind: "domain-file", Domain: "exec", Values: [16]uint64{uint64(fp.Dev), uint64(fp.Ino)}})
		}
	}
}
