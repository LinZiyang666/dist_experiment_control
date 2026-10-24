// pty.go owns kind dispatch and terminal admission. The terminal actor owns
// every master operation; the runtime owns the handle, budget and lifecycle.
// origin: hot-update S4 plan §4.1 / §5.1 / §6.2
package execsession

import (
	"errors"
	"fmt"
	"math"
	"runtime"
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

// RequiredDomains includes settled records: collecting the last descriptor
// does not make metadata readable by a reader that lacks its domain.
func (r *Runtime) RequiredDomains() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	ptyRequired := false
	for _, kind := range r.registry.RetainedKinds() {
		ptyRequired = ptyRequired || kind == proto.ProcessKindPTY
	}
	for _, t := range r.tasks {
		ptyRequired = ptyRequired || t.kind == proto.ProcessKindPTY
	}
	for _, t := range r.discarded {
		ptyRequired = ptyRequired || t.kind == proto.ProcessKindPTY
	}
	for _, rev := range r.revocations {
		ptyRequired = ptyRequired || rev.clockUnknown
	}
	return imageDomains(ptyRequired)
}

// RequiredDomains reports the domains needed to decode the actual cut.
func RequiredDomains(img Image) []string {
	needed := false
	for _, rec := range img.Registry.Records {
		needed = needed || rec.Kind == proto.ProcessKindPTY
	}
	for _, t := range img.Tasks {
		needed = needed || t.Kind == proto.ProcessKindPTY || t.PTY != nil
	}
	for _, t := range img.Tombstones {
		needed = needed || t.Kind == proto.ProcessKindPTY || t.PTY != nil
	}
	for _, rev := range img.Revocations {
		needed = needed || rev.ClockUnknown
	}
	return imageDomains(needed)
}

func imageDomains(ptyRequired bool) []string {
	if ptyRequired {
		return []string{"exec-v1", "pty-v1"}
	}
	return []string{"exec-v1"}
}

// Active is irreversible for an imported runtime, including partial startup.
func (r *Runtime) Active() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.thawed
}

// PTYHealth carries fresh connection proof to actors without holding the
// admission lock through control transactions or session census.
func (r *Runtime) PTYHealth(proof ptysession.HealthProof) {
	r.mu.Lock()
	if r.closed || r.frozen || r.freezing || r.activating {
		r.mu.Unlock()
		return
	}
	tasks := r.sortedTasks()
	r.ioActive++
	r.mu.Unlock()
	defer r.endIO()
	for _, t := range tasks {
		if t.pty != nil {
			if err := t.pty.Tick(proof); err != nil && r.cfg.Warn != nil {
				r.cfg.Warn("exec session: PTY health tick", err)
			}
		}
	}
}

func (r *Runtime) endIO() {
	r.mu.Lock()
	r.ioActive--
	r.cond.Broadcast()
	r.mu.Unlock()
}

// Reject mixed payloads before any attach, ACK, queue or clock side effect.
// Empty kind deliberately keeps the original exec spelling and JSON shape.
func validatePayload(q proto.ProcessResumeReq) error {
	mutation := len(q.Input) != 0 || q.InputSeq != 0 || q.InputFIN || q.Resize != nil || q.Signal != 0 || q.ControlSeq != 0
	if q.Kind == "" {
		if q.Run != nil || mutation || q.Op == proto.ProcessOpPTYProbe {
			return procrequest.ErrIdentity
		}
		return nil
	}
	if q.Kind != proto.ProcessKindPTY || q.Exec != nil {
		return procrequest.ErrIdentity
	}
	if q.Op != "terminal-ack" && q.TerminalID != "" {
		return procrequest.ErrIdentity
	}
	if q.Op != "create" && q.Run != nil {
		return procrequest.ErrIdentity
	}
	switch q.Op {
	case "probe", proto.ProcessOpPTYProbe:
		if q.Handle != nil || q.Run != nil || q.AttachEpoch != 0 || mutation || q.StdoutACK != 0 || q.StderrACK != 0 || q.StdoutFIN || q.StderrFIN {
			return procrequest.ErrIdentity
		}
	case "create":
		if q.Run == nil || mutation || q.StdoutACK != 0 || q.StderrACK != 0 || q.StdoutFIN || q.StderrFIN {
			return procrequest.ErrIdentity
		}
	case "resume", "ack", "terminal-ack":
		if mutation {
			return procrequest.ErrIdentity
		}
	case proto.ProcessOpInput:
		if q.Resize != nil || q.Signal != 0 || q.ControlSeq != 0 || len(q.Input) > upgradebudget.PTYBatchBytes || uint64(len(q.Input)) > math.MaxUint64-q.InputSeq {
			return procrequest.ErrIdentity
		}
	case proto.ProcessOpResize, proto.ProcessOpSignal, proto.ProcessOpKeepalive, proto.ProcessOpHangup:
		if len(q.Input) != 0 || q.InputSeq != 0 || q.InputFIN || q.ControlSeq == 0 {
			return procrequest.ErrIdentity
		}
		if q.Op == proto.ProcessOpResize {
			if q.Resize == nil || q.Resize.Cols < 1 || q.Resize.Cols > 65535 || q.Resize.Rows < 1 || q.Resize.Rows > 65535 || q.Signal != 0 {
				return procrequest.ErrIdentity
			}
		} else if q.Resize != nil {
			return procrequest.ErrIdentity
		}
		if q.Op == proto.ProcessOpSignal {
			if q.Signal != int(syscall.SIGINT) && q.Signal != int(syscall.SIGTERM) && q.Signal != int(syscall.SIGHUP) {
				return procrequest.ErrIdentity
			}
		} else if q.Signal != 0 {
			return procrequest.ErrIdentity
		}
	default:
		return fmt.Errorf("exec session: unsupported operation")
	}
	if q.StderrACK != 0 {
		return workload.ErrReplayOffset
	}
	return nil
}

// handlePTYLocked temporarily drops mu for every actor call. ioActive pins the
// cut until those calls have returned, including waits for a control token.
func (r *Runtime) handlePTYLocked(scope procrequest.Scope, q proto.ProcessResumeReq, inbox string, reply proto.ProcessResumeReply) (proto.ProcessResumeReply, error) {
	reply.TaskKind = proto.ProcessKindPTY
	if q.Op == "probe" || q.Op == proto.ProcessOpPTYProbe {
		return handlePTYProbeLocked(r, scope, q, reply)
	}
	// origin: hot-update S4 plan §3.3 / T37
	// The virtual stream is already ended before Start and remains ended
	// after resource retirement; neither path reaches the actor conversion.
	reply.Stderr = &proto.ProcessOutput{FIN: true}
	if q.Handle == nil || q.AttachEpoch == 0 {
		return reply, procrequest.ErrIdentity
	}
	reply.RequestID = q.Handle.RequestID
	now, err := r.now()
	if err != nil || now < 0 {
		return reply, fmt.Errorf("exec session: boot clock unavailable: %w", err)
	}
	r.lastBootNS = max(r.lastBootNS, now)
	rec, err := resolvePTYRecordLocked(r, scope, q, inbox, now)
	if err != nil {
		return reply, err
	}
	if rec.Kind != proto.ProcessKindPTY {
		return reply, procrequest.ErrIdentity
	}
	reply.PID, reply.Phase = rec.PID, rec.Phase
	if rec.Terminal != nil {
		reply.TerminalID, reply.ExitCode, reply.Signal, reply.Error = rec.Terminal.ID, rec.Terminal.ExitCode, rec.Terminal.Signal, rec.Terminal.Error
	}
	if rec.Phase == "settled" {
		if q.Op == "terminal-ack" && q.TerminalID != reply.TerminalID {
			return reply, procrequest.ErrConflict
		}
		return reply, nil
	}
	return handlePTYTaskLocked(r, scope, q, reply, rec, now)
}

func handlePTYProbeLocked(r *Runtime, scope procrequest.Scope, q proto.ProcessResumeReq, reply proto.ProcessResumeReply) (proto.ProcessResumeReply, error) {
	if scope.ClientLineage != q.ClientLineage {
		return reply, procrequest.ErrIdentity
	}
	lease, err := r.registry.Issue(scope)
	if err == nil {
		reply.Kind, reply.Lease = "capability", &lease
		r.lastBootNS = max(r.lastBootNS, lease.IssuedBootNS)
		if runtime.GOOS == "linux" {
			reply.PTYVersion = proto.PTYVersion
		}
	}
	return reply, err
}

func resolvePTYRecordLocked(r *Runtime, scope procrequest.Scope, q proto.ProcessResumeReq, inbox string, now int64) (procrequest.Record, error) {
	if q.Op == "create" {
		return createPTYLocked(r, scope, q, inbox, now)
	}
	rec, err := r.registry.Lookup(scope, *q.Handle)
	if err == nil && rec.Kind != proto.ProcessKindPTY {
		err = procrequest.ErrIdentity
	}
	if err == nil {
		rec, err = r.registry.Attach(scope, *q.Handle, inbox, q.AttachEpoch)
	}
	return rec, err
}

func createPTYLocked(r *Runtime, scope procrequest.Scope, q proto.ProcessResumeReq, inbox string, now int64) (procrequest.Record, error) {
	if rev, ok := r.revocations[scope.SID]; ok && (rev.clockUnknown || q.Handle.Lease.IssuedBootNS < rev.atBootNS) {
		return procrequest.Record{}, ErrRevoked
	}
	// Lookup validates scope/MAC before allocating. Duplicate intent is still
	// checked by ReservePTY, but never competes for a second budget reservation.
	existing, lookupErr := r.registry.Lookup(scope, *q.Handle)
	if lookupErr != nil && !errors.Is(lookupErr, procrequest.ErrUnknown) {
		return procrequest.Record{}, lookupErr
	}
	if lookupErr == nil && existing.Kind != proto.ProcessKindPTY {
		return procrequest.Record{}, procrequest.ErrIdentity
	}
	var pending *task
	if errors.Is(lookupErr, procrequest.ErrUnknown) {
		if runtime.GOOS != "linux" || r.cfg.StartPTY == nil || r.cfg.PTYStarted == nil {
			return procrequest.Record{}, fmt.Errorf("exec session: unsupported operation")
		}
		if r.cfg.PTYAdmission != nil {
			if err := r.cfg.PTYAdmission(); err != nil {
				return procrequest.Record{}, err
			}
		}
		pending = &task{kind: proto.ProcessKindPTY, handle: *q.Handle, pid: proc.NewPID(), done: make(chan struct{}), lastTouch: now, owned: true}
		// origin: hot-update S4 plan §5.1 / T25
		// Both replay reservations precede ReservePTY and the first possible Start.
		if err := allocatePTY(r, pending); err != nil {
			closeTask(pending)
			return procrequest.Record{}, errors.Join(procrequest.ErrCapacity, err)
		}
	}
	pid := existing.PID
	if pending != nil {
		pid = pending.pid
	}
	rec, fresh, err := r.registry.ReservePTY(scope, *q.Handle, *q.Run, pid, inbox, q.AttachEpoch)
	if err != nil || !fresh {
		if pending != nil {
			closeTask(pending)
		}
	} else {
		r.tasks[rec.PID] = pending
		spec := copyRun(*q.Run)
		spec.ActorFP = scope.ActorFP
		go r.startPTY(pending, spec)
	}
	return rec, err
}

func handlePTYTaskLocked(r *Runtime, scope procrequest.Scope, q proto.ProcessResumeReq, reply proto.ProcessResumeReply, rec procrequest.Record, now int64) (proto.ProcessResumeReply, error) {
	t := r.tasks[rec.PID]
	if t == nil {
		if ts := r.discarded[rec.PID]; ts != nil {
			return r.handleDiscarded(scope, q, reply, ts)
		}
		return reply, fmt.Errorf("exec session: missing resource owner")
	}
	if t.revoked != "" && (rec.Terminal == nil || isPTYMutation(q.Op)) {
		return reply, ErrRevoked
	}
	if t.lostTerminal {
		return reply, ErrTerminalUnrecorded
	}
	t.lastTouch = now
	if rec.Phase == "starting" {
		if isPTYMutation(q.Op) || q.Op == "terminal-ack" || q.StdoutACK != 0 || q.StdoutFIN {
			return reply, procrequest.ErrBusy
		}
		return reply, nil
	}
	return handlePTYActorLocked(r, scope, q, reply, rec, t)
}

// handlePTYActorLocked returns with mu held, just like its caller. opMu is
// acquired outside mu while ioActive pins the referenced actor against a cut.
func handlePTYActorLocked(r *Runtime, scope procrequest.Scope, q proto.ProcessResumeReq, reply proto.ProcessResumeReply, rec procrequest.Record, t *task) (proto.ProcessResumeReply, error) {
	r.ioActive++
	r.mu.Unlock()
	t.opMu.Lock()
	defer t.opMu.Unlock()
	r.mu.Lock()
	current, lookupErr := r.registry.Lookup(scope, *q.Handle)
	if lookupErr != nil || current.AttachEpoch != q.AttachEpoch {
		r.ioActive--
		if lookupErr != nil {
			return reply, lookupErr
		}
		return reply, procrequest.ErrStale
	}
	// Completion can win while this request waits for opMu. The retained
	// registry result, rather than the retired Manager row, is authoritative.
	// The observer shares opMu through Complete/Forget, so no live actor call
	// can lose its process row between this check and its control transaction.
	// origin: hot-update S4 plan §6.2 / T34; PRODUCT-R4 arm A
	rec = current
	reply.Phase = rec.Phase
	if rec.Terminal != nil {
		reply.TerminalID, reply.ExitCode, reply.Signal, reply.Error = rec.Terminal.ID, rec.Terminal.ExitCode, rec.Terminal.Signal, rec.Terminal.Error
	}
	if current.Phase == "settled" {
		r.ioActive--
		reply.Phase = "settled"
		if q.Op == "terminal-ack" && q.TerminalID != reply.TerminalID {
			return reply, procrequest.ErrConflict
		}
		return reply, nil
	}
	r.mu.Unlock()
	reply, err := r.ptyReply(t, q, reply)
	r.mu.Lock()
	r.ioActive--
	r.cond.Broadcast()
	if err != nil {
		return reply, err
	}
	if q.Op == "terminal-ack" {
		return settlePTYLocked(r, scope, q, reply, rec, t)
	}
	return reply, nil
}

func settlePTYLocked(r *Runtime, scope procrequest.Scope, q proto.ProcessResumeReq, reply proto.ProcessResumeReply, rec procrequest.Record, t *task) (proto.ProcessResumeReply, error) {
	// origin: hot-update S4 plan §3.2 / T12
	// Ended with InputError settles even when Applied < Accepted. Those bytes
	// remain a diagnostic obligation, not work a dead writer can finish.
	if rec.Terminal == nil || q.TerminalID != rec.Terminal.ID || !reply.InputEnded ||
		len(reply.Stdout.Data) != 0 || (!reply.Stdout.FIN && reply.Stdout.Error == "") ||
		(reply.Stdout.FIN && !q.StdoutFIN) || !q.StderrFIN {
		return reply, procrequest.ErrConflict
	}
	if err := r.registry.Settle(scope, *q.Handle, q.TerminalID); err != nil {
		return reply, err
	}
	logPTYSettlement(r, t, *rec.Terminal)
	r.retireLocked(t)
	reply.Phase = "settled"
	return reply, nil
}

func isPTYMutation(op string) bool {
	switch op {
	case "input", "resize", "signal", "keepalive", "hangup":
		return true
	}
	return false
}

func (r *Runtime) ptyReply(t *task, q proto.ProcessResumeReq, reply proto.ProcessResumeReply) (proto.ProcessResumeReply, error) {
	// Recheck after waiting behind another request: revocation may have arrived
	// while this handler held only a reference, not the admission mutex.
	r.mu.Lock()
	revoked, actor := t.revoked, t.pty
	var bare *ptysession.Image
	if t.ptyState != nil {
		copy := *t.ptyState
		bare = &copy
	}
	r.mu.Unlock()
	if revoked != "" && isPTYMutation(q.Op) {
		return reply, ErrRevoked
	}
	if actor == nil {
		if bare == nil {
			return reply, fmt.Errorf("exec session: missing PTY state")
		}
		if isPTYMutation(q.Op) {
			return reply, procrequest.ErrConflict
		}
		if q.StderrACK != 0 {
			return reply, workload.ErrReplayOffset
		}
		state := t.logs[1].Snapshot()
		if q.StdoutACK < state.Base || q.StdoutACK > state.End || q.StdoutFIN && (!state.FIN || q.StdoutACK != state.End) {
			return reply, workload.ErrReplayOffset
		}
		if err := t.logs[1].Ack(q.StdoutACK); err != nil {
			return reply, err
		}
		var err error
		reply.Stdout, err = outputBatch(t, 0)
		if err != nil {
			return reply, err
		}
		if !reply.Stdout.FIN && reply.Stdout.Offset+uint64(len(reply.Stdout.Data)) == state.End {
			reply.Stdout.Error = bare.OutputError
		}
		fillPTYTombstone(&reply, ptyTombstone(*bare))
	} else {
		// Check output ACK before executing any input/control side effect.
		batch, err := actor.OutputBatch(q.StdoutACK, q.StdoutFIN, upgradebudget.PTYBatchBytes)
		if err != nil {
			return reply, err
		}
		reply.Stdout = &proto.ProcessOutput{Offset: batch.Offset, Data: batch.Data, FIN: batch.FIN, Error: batch.Error}
		// Late input/control requests still receive output and the exact terminal,
		// but cannot revive its master, renew authority or create a new receipt.
		// origin: hot-update S4 plan §6.2 / T35; PRODUCT-R4 arm A
		if reply.TerminalID == "" {
			switch q.Op {
			case proto.ProcessOpInput:
				_, err = actor.AcceptInput(q.InputSeq, q.Input, q.InputFIN)
				// A committed abort owns a final input receipt and a live output tail.
				// Refused identity/sequence/payload requests must still fail normally.
				// origin: hot-update S4 internal review rv-eof rv-eof-2
				var aborted *ptysession.InputAbortError
				if errors.As(err, &aborted) {
					err = nil
				}
			case proto.ProcessOpResize, proto.ProcessOpSignal, proto.ProcessOpKeepalive, proto.ProcessOpHangup:
				control := ptysession.ControlRequest{Seq: q.ControlSeq, Op: q.Op, Signal: q.Signal}
				if q.Resize != nil {
					control.Cols, control.Rows = uint16(q.Resize.Cols), uint16(q.Resize.Rows)
				}
				var receipt ptysession.ControlReceipt
				receipt, err = actor.ApplyControl(control)
				// origin: hot-update S4 plan §3.4 / §9.1; P18.
				// opMu serializes retries: only an advanced committed receipt is audited.
				if receipt.Seq > t.control.Seq && q.Op != proto.ProcessOpKeepalive {
					ptysession.ControlRecord(t.pid, q.Op, receipt).Log(r.cfg.Logger)
				}
				if receipt.Seq != 0 {
					t.control = receipt
				}
			}
		}
		fillPTYInput(&reply, actor.InputStatus())
		reply.ControlACK, reply.ControlError = t.control.Seq, t.control.Error
		if err != nil {
			return reply, err
		}
	}
	reply.Stderr = &proto.ProcessOutput{FIN: true}
	return reply, nil
}

func fillPTYInput(reply *proto.ProcessResumeReply, st ptysession.InputStatus) {
	reply.InputACK, reply.InputAccepted, reply.InputWindow = st.Applied, st.Accepted, st.Window
	reply.InputFIN, reply.InputEnded, reply.InputError = st.FIN, st.Ended, st.Error
}

func fillPTYTombstone(reply *proto.ProcessResumeReply, st *ptysession.Tombstone) {
	reply.TaskKind = proto.ProcessKindPTY
	if st == nil {
		return
	}
	reply.InputACK, reply.InputAccepted, reply.InputFIN = st.InputACK, st.InputAccepted, st.InputFIN
	reply.InputEnded, reply.InputError = st.InputEnded, st.InputError
	reply.ControlACK, reply.ControlError = st.ControlSeq, st.ControlError
}

func ptyTombstone(st ptysession.Image) *ptysession.Tombstone {
	return &ptysession.Tombstone{InputACK: st.Input.Base, InputAccepted: st.Input.End, InputFIN: st.InputFIN,
		InputEnded: st.InputEnded, InputError: st.InputError, ControlSeq: st.ControlSeq, ControlDigest: st.ControlDigest, ControlError: st.ControlError}
}

func copyRun(spec proto.RunReq) proto.RunReq {
	spec.Argv = append([]string(nil), spec.Argv...)
	if spec.Env != nil {
		env := make(map[string]string, len(spec.Env))
		for k, v := range spec.Env {
			env[k] = v
		}
		spec.Env = env
	}
	return spec
}

func allocatePTY(r *Runtime, t *task) error {
	for i, capacity := range []int64{upgradebudget.PTYInputBytes, workload.DefaultReplayCapacity} {
		log, err := workload.NewReplay(r.sharedReplayBudget, capacity)
		if err != nil {
			return err
		}
		t.logs[i] = log
	}
	return nil
}

func (r *Runtime) startPTY(t *task, spec proto.RunReq) {
	defer r.conclude(t)
	r.mu.Lock()
	closed, revoked := r.closed, t.revoked
	r.mu.Unlock()
	if closed || revoked != "" {
		failPTYStart(r, t, fmt.Errorf("exec session: closed or revoked before PTY start"))
		return
	}
	session, _, err := r.cfg.StartPTY(t.pid, &spec)
	if err != nil {
		failPTYStart(r, t, err)
		return
	}
	row, adopted := r.cfg.Manager.Lookup(t.pid)
	if !adopted || session == nil {
		if session != nil {
			_ = session.Close()
		}
		failPTYStart(r, t, errUnadopted)
		return
	}
	ptysession.StartedRecord(t.pid, t.handle.RequestID, row.OSPID, row.StartTimeTicks).Log(r.cfg.Logger)
	r.cfg.PTYStarted(t.pid, spec, row.Identity)
	master, identity, err := session.TakeMaster()
	if err != nil {
		_ = r.cfg.Manager.Signal(t.pid, syscall.SIGKILL)
		_ = session.Close()
		finishPTYSetupFailure(r, t, err)
		return
	}
	// Attribute the original master after successful adoption, including a
	// subsequent setup failure; the kernel census decides whether it is live.
	// origin: hot-update S5 plan §9.1 / DRILL-OBSERVER
	if drillobserve.Hook != nil {
		drillobserve.Hook(drillobserve.Event{Kind: "domain-file", Domain: "pty", Values: [16]uint64{identity.Dev, identity.Ino}})
	}
	now, err := r.now()
	if err != nil {
		_ = r.cfg.Manager.Signal(t.pid, syscall.SIGKILL)
		_ = master.Close()
		finishPTYSetupFailure(r, t, err)
		return
	}
	// Import is also the construction primitive for a new, paused actor. New
	// would start queued input before the post-adoption revocation recheck.
	st := ptysession.Image{Version: 1, Identity: identity}
	st.Observation, err = master.Observe()
	if err == nil {
		st.Input, _, err = t.logs[0].Freeze()
	}
	if err == nil {
		st.Output, _, err = t.logs[1].Freeze()
	}
	if spec.KAIntervalMS > 0 {
		st.KAIntervalMS = max(1000, min(60000, spec.KAIntervalMS))
		grace := max(6*time.Duration(st.KAIntervalMS)*time.Millisecond, upgradebudget.PTYRecovery)
		if now > math.MaxInt64-int64(grace) {
			err = fmt.Errorf("exec session: PTY clock overflow")
		} else {
			st.KADeadlineBootNS = now + int64(grace)
		}
	}
	var actor *ptysession.Actor
	if err == nil {
		actor, err = ptysession.Import(ptysession.Config{Manager: r.cfg.Manager, PID: t.pid, Master: master, Identity: identity, Input: t.logs[0], Output: t.logs[1], Clock: r.now, KAIntervalMS: st.KAIntervalMS}, st)
	}
	if err != nil {
		_ = r.cfg.Manager.Signal(t.pid, syscall.SIGKILL)
		_ = master.Close()
		finishPTYSetupFailure(r, t, err)
		return
	}
	r.mu.Lock()
	t.pty = actor
	revoked, closed = t.revoked, r.closed
	r.mu.Unlock()
	if revoked != "" || closed {
		err = actor.Revoke("revoked")
	}
	if err == nil {
		err = actor.PreActivate()
	}
	if err == nil {
		err = actor.Activate()
	}
	if err != nil {
		// A child exists: retain the handle and logs, and retry cleanup through the
		// sole observer. No capacity refusal may invite a second execution now.
		_ = actor.Revoke("revoked")
	}
	r.mu.Lock()
	if runningErr := r.registry.Running(t.handle, t.pid); err == nil {
		err = runningErr
	}
	r.mu.Unlock()
	r.observePTY(t, err)
}

func failPTYStart(r *Runtime, t *task, cause error) {
	for _, log := range t.logs[:2] {
		log.Thaw()
	}
	_ = t.logs[1].Finish()
	st := &ptysession.Image{Version: 1, Input: t.logs[0].Snapshot(), Output: t.logs[1].Snapshot(), MasterClosed: true, SourceEnded: true, InputEnded: true, InputError: "pty input aborted: source_ended"}
	r.mu.Lock()
	t.ptyState = st
	r.finish(t, workload.Exit{Code: -1}, cause)
	r.mu.Unlock()
}

func finishPTYSetupFailure(r *Runtime, t *task, cause error) {
	// A real child existed, and closing its master is an abort, never a clean
	// source EOF. Keep the same handle and reserved logs while cleanup retries.
	for _, log := range t.logs[:2] {
		log.Thaw()
	}
	row, _ := r.cfg.Manager.Lookup(t.pid)
	st := &ptysession.Image{Version: 1, Input: t.logs[0].Snapshot(), Output: t.logs[1].Snapshot(),
		MasterClosed: true, SourceEnded: true, InputEnded: true, InputError: "pty input aborted: source_ended",
		OutputError: boundedError(cause.Error()), CleanupPending: true,
		Identity: pty.TerminalIdentity{Version: 1, BootID: row.BootID, LeaderPID: row.OSPID, StartTimeTicks: row.StartTimeTicks}}
	r.mu.Lock()
	t.ptyState = st
	_ = r.registry.Running(t.handle, t.pid)
	r.mu.Unlock()
	observePTYSetup(r, t, cause)
}

// Before an actor exists the runtime retains the adopted child's cleanup
// responsibility. Census/signal run outside both runtime and Manager locks;
// ReapHeld pins the leader identity until two empty descendant scans agree.
// origin: hot-update S4 plan §5.1 / §6.2 / T25
func observePTYSetup(r *Runtime, t *task, cause error) {
	timer := time.NewTicker(10 * time.Millisecond)
	defer timer.Stop()
	stable := 0
	for {
		r.mu.Lock()
		if r.frozen || r.freezing || r.activating {
			closed := r.closed
			r.mu.Unlock()
			if closed {
				return
			}
			<-timer.C
			continue
		}
		r.ioActive++
		r.mu.Unlock()
		row, ok := r.cfg.Manager.Lookup(t.pid)
		if !ok {
			r.endIO()
			r.mu.Lock()
			r.finish(t, workload.Exit{Code: -1}, errUnadopted)
			r.mu.Unlock()
			return
		}
		var members []workload.SessionMember
		var cleanupErr error
		if row.Exit == nil && stable < 2 {
			_ = r.cfg.Manager.Signal(t.pid, syscall.SIGKILL)
			members, cleanupErr = workload.ScanSession(row.Identity)
			if cleanupErr == nil {
				remaining := members[:0]
				for _, member := range members {
					if member.Identity.OSPID == row.OSPID {
						continue
					}
					remaining = append(remaining, member)
					signalErr := workload.SignalIdentity(member, syscall.SIGKILL)
					if signalErr != nil && !errors.Is(signalErr, syscall.ESRCH) {
						cleanupErr = errors.Join(cleanupErr, signalErr)
					}
				}
				members = remaining
				if len(members) == 0 {
					stable++
				} else {
					stable = 0
				}
			}
		}
		if stable >= 2 {
			_ = r.cfg.Manager.AllowReap(t.pid)
		}
		r.mu.Lock()
		t.ptyState.CleanupPending = stable < 2
		t.ptyState.CleanupMembers = append([]workload.SessionMember(nil), members...)
		r.ioActive--
		if row.Exit != nil {
			t.ptyState.CleanupPending = false
			t.ptyState.CleanupMembers = nil
			if r.finish(t, *row.Exit, cause) {
				r.mu.Unlock()
				return
			}
		}
		r.mu.Unlock()
		if cleanupErr != nil && r.cfg.Warn != nil {
			r.cfg.Warn("exec session: failed PTY setup cleanup pending", cleanupErr)
		}
		<-timer.C
	}
}

func (r *Runtime) observePTY(t *task, setupErr error) {
	if t.pty == nil {
		if setupErr == nil && t.ptyState != nil {
			setupErr = errors.New(t.ptyState.OutputError)
		}
		observePTYSetup(r, t, setupErr)
		return
	}
	timer := time.NewTicker(10 * time.Millisecond)
	defer timer.Stop()
	for {
		r.mu.Lock()
		frozen, closed, revoked := r.frozen || r.freezing || r.activating, r.closed, t.revoked
		if !frozen {
			r.ioActive++
		}
		actor := t.pty
		r.mu.Unlock()
		if frozen {
			if closed {
				return
			}
			<-timer.C
			continue
		}
		// Match protocol and revocation lock order: opMu before Runtime.mu.
		// Forget must not retire the Manager row under a late control or census.
		// origin: hot-update S4 plan §6.2 / T34; PRODUCT-R4 arm A / FX-A
		t.opMu.Lock()
		if revoked != "" || closed || setupErr != nil {
			if err := actor.Revoke("revoked"); err != nil && r.cfg.Warn != nil {
				r.cfg.Warn("exec session: PTY cleanup pending", err)
			}
			if setupErr != nil {
				_ = actor.Activate()
			}
			if closed {
				_ = actor.Close()
				_ = r.cfg.Manager.Signal(t.pid, syscall.SIGKILL)
				_ = r.cfg.Manager.AllowReap(t.pid)
			}
		}
		// The actor releases ReapHeld only after source end or explicit cleanup.
		// A zombie leader with a slave-owning descendant remains nonterminal here.
		row, ok := r.cfg.Manager.Lookup(t.pid)
		r.mu.Lock()
		r.ioActive--
		if ok && row.Exit != nil && !r.frozen && !r.freezing && !r.activating && r.finish(t, *row.Exit, setupErr) {
			r.mu.Unlock()
			t.opMu.Unlock()
			return
		}
		r.mu.Unlock()
		t.opMu.Unlock()
		<-timer.C
	}
}

// activateActor keeps failure injection at the commit boundary. It cannot
// substitute an actor, synthesize input progress or bypass PreActivate.
func (r *Runtime) activateActor(t *task) error {
	actor := t.pty
	if r.cfg.PTYActivate != nil {
		return r.cfg.PTYActivate(t.pid, actor)
	}
	if r.activatePTY != nil {
		return r.activatePTY(actor)
	}
	return actor.Activate()
}
