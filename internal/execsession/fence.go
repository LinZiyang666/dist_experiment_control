// fence.go — the image fence: the runtime half of a same-PID image switch.
//
// Freeze cuts every admission source at once (Handle, the startup owners,
// observe, Revoke, Sweep), Export hands the cut to internal/agentimage as a
// versioned Image plus borrowed descriptors, Thaw reverses the cut in the
// order the client contract needs, Discard retires an exported old image and
// Detach gives an imported one's descriptors back for a re-dispatch. The
// process table (workload.Manager) is deliberately NOT frozen here: it is
// shared with the legacy exec and PTY paths, so its cut belongs to the layer
// that owns all of them (agentimage), which freezes it AFTER this runtime
// and thaws it BEFORE — see Thaw for why that order is load-bearing.
// origin: docs/reviews/hot-update-agent-image-abi-plan.md §1.
package execsession

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/workload"
)

var (
	// ErrNotFrozen is returned by Export/Thaw/Discard/Detach on a live runtime.
	ErrNotFrozen = errors.New("exec session: runtime is not frozen")
	// ErrImage wraps every image validation refusal (Import, Export self-checks).
	ErrImage = errors.New("exec session: invalid image")
)

// freezePollInterval is how often Freeze re-checks for a "starting" record
// while it waits for an in-flight creation to reach running or fail.
const freezePollInterval = 5 * time.Millisecond

// Freeze cuts the runtime. On success no task is being created, every pump
// is paused with its bytes retained, the registry refuses every mutation and
// every Handle call queues until Thaw, Close, or its hard request budget;
// the state can then be read by Export. The only "no spawn in flight" evidence is the registry itself
// (procrequest.Registry.Freeze returns ErrBusy while any record is
// "starting"; a record is "starting" from Reserve to Running/failStart, and
// both transitions happen under r.mu) — never a task's done channel, which
// closes only when the whole result has been published. While a creation is
// in flight, further creates block (quiescing) and Freeze re-checks every
// freezePollInterval until deadline, then returns procrequest.ErrBusy with
// nothing frozen. Attach ops keep flowing during that wait so bytes move to
// the last moment. The pump barrier is bounded by the same deadline; any
// failure restores what was frozen. If restoration outlasts the deadline or
// fails, ErrRollbackPending exposes its retained responsibility and admission
// stays closed until the recovery acknowledgement or a successful Thaw retry.
//
// Freeze is deadline-based, not ctx-based, on purpose: data-plane objects
// must never be bound to a caller's context (gotcha #80 /
// dataplane_lifetime gate), and the workload actors are the only consumers
// of the derived context.
// origin: hot-update S4 plan §4.1 / T50
func (r *Runtime) Freeze(deadline time.Time) error {
	if !lockBefore(&r.mu, deadline) {
		return procrequest.ErrBusy
	}
	if r.closed {
		r.mu.Unlock()
		return ErrClosed
	}
	if r.frozen || (r.quiescing && !r.partialActive) || r.freezing || r.activating {
		r.mu.Unlock()
		return procrequest.ErrFrozen
	}
	r.quiescing = true
	for {
		state, err := r.registry.Freeze()
		if err == nil {
			now, clockErr := r.now()
			if clockErr != nil {
				r.registry.Thaw()
				r.quiescing = false
				broadcastAdmissionLocked(r)
				r.mu.Unlock()
				return fmt.Errorf("exec session: freeze: clock: %w", clockErr)
			}
			r.lastBootNS = max(r.lastBootNS, now)
			r.activation.generation.Add(1)
			r.freezing = true
			r.activation.recovery = make(chan struct{})
			tasks := r.sortedTasks()
			r.mu.Unlock()
			return r.freezeTasks(deadline, state, now, tasks)
		}
		if !errors.Is(err, procrequest.ErrBusy) || !time.Now().Before(deadline) {
			r.quiescing = false
			broadcastAdmissionLocked(r)
			r.mu.Unlock()
			if errors.Is(err, procrequest.ErrBusy) && r.cfg.Warn != nil {
				r.cfg.Warn("exec session: image freeze deferred by an in-flight creation", err)
			}
			return err
		}
		r.mu.Unlock()
		waitFreezePoll(deadline)
		if !lockBefore(&r.mu, deadline) {
			// Complete cancellation when the short state lock becomes available;
			// never extend the caller's deadline while another entrant holds it.
			if r.mu.TryLock() {
				r.quiescing = false
				broadcastAdmissionLocked(r)
				r.mu.Unlock()
			} else {
				go r.cancelFreezeAdmission()
			}
			if r.cfg.Warn != nil {
				r.cfg.Warn("exec session: image freeze deferred by an in-flight creation", err)
			}
			return err
		}
		if r.closed {
			r.quiescing = false
			broadcastAdmissionLocked(r)
			r.mu.Unlock()
			return ErrClosed
		}
	}
}

func lockBefore(mu *sync.Mutex, deadline time.Time) bool {
	for time.Now().Before(deadline) {
		if mu.TryLock() {
			return true
		}
		waitFreezePoll(deadline)
	}
	return false
}

func waitFreezePoll(deadline time.Time) {
	if left := time.Until(deadline); left > 0 {
		time.Sleep(min(left, freezePollInterval))
	}
}

func (r *Runtime) cancelFreezeAdmission() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.quiescing = false
	broadcastAdmissionLocked(r)
}

// The registry freezes first, then outstanding references drain, then actors
// pause. Runtime.mu never covers a pump join or a control-token wait. Revoke
// can therefore append irreversible responsibility throughout this barrier.
func (r *Runtime) freezeTasks(deadline time.Time, state procrequest.State, now int64, tasks []*task) error {
	// ctx-none: the data-plane fence receives one absolute deadline
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	var err error
	for {
		if !lockBefore(&r.mu, deadline) {
			err = procrequest.ErrBusy
			break
		}
		active, closed := r.ioActive, r.closed
		r.mu.Unlock()
		if closed {
			err = ErrClosed
			break
		}
		if active == 0 {
			break
		}
		waitFreezePoll(deadline)
	}
	snapshots := make(map[*task]*taskFreeze, len(tasks))
	var attempted []*task
	if err == nil {
		for _, t := range tasks {
			if ctx.Err() != nil {
				err = procrequest.ErrBusy
				break
			}
			attempted = append(attempted, t)
			var tf *taskFreeze
			tf, err = freezeTask(ctx, t)
			if err != nil {
				break
			}
			snapshots[t] = tf
		}
	}
	if err == nil && !time.Now().Before(deadline) {
		err = procrequest.ErrBusy
	}
	if err != nil {
		return recoverFreezeTasks(r, deadline, tasks, attempted, snapshots, err, false)
	}
	if !lockBefore(&r.mu, deadline) {
		return recoverFreezeTasks(r, deadline, tasks, attempted, snapshots, procrequest.ErrBusy, false)
	}
	for t, tf := range snapshots {
		t.frozen = tf
	}
	r.activation.completed = nil
	close(r.activation.recovery)
	r.frozenRegistry, r.frozenAt = state, now
	r.freezing, r.quiescing, r.frozen = false, false, true
	r.partialActive = false
	r.mu.Unlock()
	return nil
}

// freezeTask pauses one task's actors and borrows their files. On error the
// caller thaws the task; thawTask is idempotent over half-frozen tasks.
func freezeTask(ctx context.Context, t *task) (*taskFreeze, error) {
	if t.kind == proto.ProcessKindPTY {
		return freezePTYTask(ctx, t)
	}
	tf := &taskFreeze{files: make(map[string]*os.File, 6)}
	switch {
	case t.input != nil:
		st, files, err := t.input.Freeze(ctx)
		if err != nil {
			return nil, fmt.Errorf("exec session: freeze stdin of %s: %w", t.pid, err)
		}
		tf.have[0], tf.replay[0], tf.input = true, st.Replay, &st
		tf.files[fileStdinReplay] = files["replay"]
		if f := files["target"]; f != nil {
			tf.files[fileStdinTarget] = f
		}
	case t.logs[0] != nil:
		st, file, err := t.logs[0].Freeze()
		if err != nil {
			return nil, fmt.Errorf("exec session: freeze stdin log of %s: %w", t.pid, err)
		}
		tf.have[0], tf.replay[0] = true, st
		tf.files[fileStdinReplay] = file
	}
	for i := range 2 {
		replayName, sourceName := streamFileNames(i)
		switch {
		case t.output[i] != nil:
			st, files, err := t.output[i].Freeze(ctx)
			if err != nil {
				return nil, fmt.Errorf("exec session: freeze output %d of %s: %w", i, t.pid, err)
			}
			tf.have[i+1], tf.replay[i+1], tf.output[i] = true, st.Replay, &st
			tf.files[replayName], tf.files[sourceName] = files["replay"], files["source"]
		case t.logs[i+1] != nil:
			st, file, err := t.logs[i+1].Freeze()
			if err != nil {
				return nil, fmt.Errorf("exec session: freeze output log %d of %s: %w", i, t.pid, err)
			}
			tf.have[i+1], tf.replay[i+1] = true, st
			tf.files[replayName] = file
		}
	}
	return tf, nil
}

// thawTask resumes whatever freezeTask paused; every underlying Thaw is a
// no-op on a stream that was not frozen, so a half-frozen task is fine.
func thawTask(t *task) error {
	if t.pty != nil {
		if err := t.pty.PreActivate(); err != nil {
			return err
		}
		return t.pty.Activate()
	}
	if t.input != nil {
		t.input.Thaw()
	} else if t.logs[0] != nil {
		t.logs[0].Thaw()
	}
	for i := range 2 {
		if t.output[i] != nil {
			t.output[i].Thaw()
		} else if t.logs[i+1] != nil {
			t.logs[i+1].Thaw()
		}
	}
	t.frozen = nil
	return nil
}

func thawExecTask(r *Runtime, t *task) error {
	if r.thawExec != nil {
		return r.thawExec(t)
	}
	return thawTask(t)
}

// Thaw first closes revoked input and executes cleanup, then resumes pumps,
// then opens registry admission. Partial activation retains current owners.
// Close uses this same order for a frozen, unexported runtime: its observers
// must enter the ordinary closed teardown, never the frozen+closed discard
// branch that deliberately leaves the next image's children untouched.
// origin: hot-update S4 plan §4.2 / §6.2 / T20
func (r *Runtime) Thaw() error {
	if err := r.PreActivate(); err != nil {
		return err
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return ErrClosed
	}
	if (!r.frozen && !r.partialActive) || r.activating || r.freezing {
		r.mu.Unlock()
		return ErrNotFrozen
	}
	tasks := unfinishedTasks(r)
	for _, t := range tasks {
		if t.revoked != "" && t.terminalAt == 0 && r.cfg.Manager.Frozen() {
			r.mu.Unlock()
			return workload.ErrFrozen
		}
	}
	r.activating = true
	// Mark before the first possible side effect, not after the last success.
	r.thawed = r.imported
	r.mu.Unlock()
	var activationErr error
	for _, t := range tasks {
		r.mu.Lock()
		reason := t.revoked
		r.mu.Unlock()
		if reason != "" && t.terminalAt == 0 {
			if t.pty != nil {
				if err := t.pty.Revoke(reason); err != nil {
					activationErr = err
					break
				}
			} else {
				_ = r.cfg.Manager.Signal(t.pid, syscall.SIGKILL)
			}
		}
		if t.pty != nil {
			if err := r.activateActor(t); err != nil {
				activationErr = err
				break
			}
		} else {
			// origin: hot-update S4 CLOSE thaw failure / plan §4.2
			// A failed pipe thaw may have resumed some streams. Keep the owner
			// unfinished and admission closed so retry or a current-state cut
			// carries that progress instead of publishing activation success.
			if err := thawExecTask(r, t); err != nil {
				activationErr = err
				break
			}
		}
		r.mu.Lock()
		if r.activation.completed == nil {
			r.activation.completed = make(map[*task]bool)
		}
		r.activation.completed[t] = true
		r.mu.Unlock()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.activating = false
	if activationErr != nil {
		// Some pumps may already have advanced. Close admission while permitting
		// a new Freeze over this CURRENT mixture of active and frozen owners.
		r.frozen, r.partialActive, r.quiescing = false, true, true
		broadcastAdmissionLocked(r)
		return activationErr
	}
	finishRuntimeActivationLocked(r)
	return nil
}

// Discard retires an OLD image's runtime after its export was handed over:
// this process's own descriptors are closed (the next image holds
// independent duplicates of the same open-file descriptions), no pump ever
// runs again, no child is signalled or reaped, and every waiter gets
// ErrClosed. Only a frozen runtime whose Export ran may be discarded; a
// runtime that was never exported still owns its tasks and must Thaw or
// Close instead. It is the in-process half of the exec path (the exec itself
// discards the old image for free) and what the round-trip tests use.
func (r *Runtime) Discard() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return ErrClosed
	}
	if !r.frozen || !r.exported {
		return ErrNotFrozen
	}
	return r.discardLocked()
}

func (r *Runtime) discardLocked() error {
	for pid, t := range r.tasks {
		if t.kind == proto.ProcessKindPTY {
			files, err := detachPTYTask(t)
			if err != nil {
				return fmt.Errorf("exec session: frozen PTY detach failed: %w", err)
			}
			for _, file := range files {
				_ = file.Close()
			}
		} else {
			closeTask(t)
		}
		t.frozen = nil
		delete(r.tasks, pid)
	}
	r.discarded = make(map[string]*tombstone)
	r.closed = true
	broadcastAdmissionLocked(r)
	return nil
}

// Detach dismantles an IMPORTED runtime that was never thawed and gives
// every descriptor back, open and untouched, as Prepare-ready resources:
// the shape a bootstrap needs to re-dispatch the same bundle to the previous
// image after a later step failed. The replay budget is released, no child
// is signalled or reaped, and the runtime is closed. A runtime that was
// thawed (its observers ran, its offsets moved, its table reaped) or
// exported (a Prepared holds duplicates of these very descriptors) is
// refused: handing the originals out then would create a second owner.
// origin: S2 internal review F3/R2.
func (r *Runtime) Detach() ([]handoff.Resource, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, ErrClosed
	}
	if !r.frozen || !r.imported || r.thawed || r.exported {
		return nil, ErrNotFrozen
	}
	resources := make([]handoff.Resource, 0, len(r.tasks))
	for _, t := range r.sortedTasks() {
		files, err := detachTask(t)
		if err != nil {
			// Unreachable on this state (every actor was built paused and
			// frozen), but a short salvage must never read as a full one.
			return nil, fmt.Errorf("%w: task %s: %v", ErrImage, t.pid, err)
		}
		resources = append(resources, taskResource(t.kind, t.pid, files))
	}
	r.tasks = make(map[string]*task)
	r.discarded = make(map[string]*tombstone)
	r.closed = true
	broadcastAdmissionLocked(r)
	return resources, nil
}

// detachTask returns a never-thawed task's files under their bundle names.
// Every actor here is paused with no goroutine (Import built it so), which
// is exactly what the workload Detach primitives require; a refused Detach
// is reported rather than swallowed (S2 internal review resources-V3), and
// the actors detached so far are still referenced by the returned map.
func detachTask(t *task) (map[string]*os.File, error) {
	if t.kind == proto.ProcessKindPTY {
		return detachPTYTask(t)
	}
	files := make(map[string]*os.File, 6)
	detachLog := func(name string, log *workload.Replay) error {
		file, _, err := log.Detach()
		if err != nil {
			return fmt.Errorf("detach %s: %w", name, err)
		}
		files[name] = file
		return nil
	}
	switch {
	case t.input != nil:
		target, log, ok := t.input.Detach()
		if !ok {
			return files, errors.New("stdin actor refused detach")
		}
		if target != nil {
			files[fileStdinTarget] = target
		}
		if err := detachLog(fileStdinReplay, log); err != nil {
			return files, err
		}
	case t.logs[0] != nil:
		if err := detachLog(fileStdinReplay, t.logs[0]); err != nil {
			return files, err
		}
	}
	for i := range 2 {
		replayName, sourceName := streamFileNames(i)
		switch {
		case t.output[i] != nil:
			source, log, ok := t.output[i].Detach()
			if !ok {
				return files, fmt.Errorf("output %d actor refused detach", i)
			}
			files[sourceName] = source
			if err := detachLog(replayName, log); err != nil {
				return files, err
			}
		case t.logs[i+1] != nil:
			if err := detachLog(replayName, t.logs[i+1]); err != nil {
				return files, err
			}
		}
	}
	t.input, t.logs, t.output = nil, [3]*workload.Replay{}, [2]*workload.Output{}
	return files, nil
}

// Frozen reports whether the fence is up (Freeze succeeded, Thaw has not run).
func (r *Runtime) Frozen() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.frozen
}

// Instance reports the identity every lease this runtime issues is bound to.
func (r *Runtime) Instance() string { return r.cfg.Instance }

// TaskPIDs lists the task IDs this runtime owns, sorted. The image layer
// uses it to prove every process-table row belongs to this runtime before
// it cuts anything (a foreign row — a legacy exec or a PTY — defers the
// upgrade instead of being exported under the wrong owner).
func (r *Runtime) TaskPIDs() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	pids := make([]string, 0, len(r.tasks))
	for pid := range r.tasks {
		pids = append(pids, pid)
	}
	sort.Strings(pids)
	return pids
}

// ReplayBudgetUsed reports this runtime's owned reservations, excluding flow
// and other runtimes that share its node budget. Empty and ACKed logs count.
// origin: hot-update S5 plan §3.4 / T30
func (r *Runtime) ReplayBudgetUsed() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return ownedReplayBytes(r)
}

func (r *Runtime) sortedTasks() []*task {
	tasks := make([]*task, 0, len(r.tasks))
	for _, t := range r.tasks {
		tasks = append(tasks, t)
	}
	sort.Slice(tasks, func(i, j int) bool { return tasks[i].pid < tasks[j].pid })
	return tasks
}
