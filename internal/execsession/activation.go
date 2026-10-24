package execsession

import (
	"errors"
	"fmt"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
)

// Completion belongs to this runtime until every owner resumes or a new cut
// resets the attempt. A failed retry must not prepare, restart or re-grant a
// completed owner.
// recovery is the acknowledgement of the freeze transaction's rollback owner.
// origin: hot-update S4 internal review rv-fd rv-fd-2 / rv-fd-3 / rv-fd-V1
type activationProgress struct {
	completed map[*task]bool
	recovery  chan struct{}
	// A queued rollback owns responsibility before Runtime.mu is available.
	// generation prevents that delayed owner from thawing a later cut.
	generation atomic.Uint64
	handoff    atomic.Pointer[thawHandoff]
	// changed is the admission broadcast; it is protected by Runtime.mu.
	changed chan struct{}
}

type thawHandoff struct {
	generation uint64
	done       chan struct{}
	err        error // published by closing done
}

// ErrRollbackPending means admission remains closed while the freeze owner
// restores earlier actors. Thaw or a new cut may proceed after acknowledgement.
var ErrRollbackPending = errors.New("exec session: freeze rollback pending")

// PreActivate does all actor checks before the caller activates its Manager.
// Revoke on a frozen actor closes intake, but leaves cleanup to activation.
func (r *Runtime) PreActivate() error {
	if err := awaitFreezeRecovery(r, time.Now().Add(upgradebudget.LocalFreezeWindow)); err != nil {
		return err
	}
	if !lockBefore(&r.mu, time.Now().Add(upgradebudget.LocalFreezeWindow)) {
		return ErrRollbackPending
	}
	if r.closed {
		r.mu.Unlock()
		return ErrClosed
	}
	if (!r.frozen && !r.partialActive) || r.freezing || r.activating {
		r.mu.Unlock()
		return ErrNotFrozen
	}
	tasks := unfinishedTasks(r)
	r.ioActive++
	r.mu.Unlock()
	defer r.endIO()
	now, err := r.now()
	if err != nil {
		return fmt.Errorf("exec session: preactivate clock: %w", err)
	}
	for _, t := range tasks {
		if t.pty == nil {
			continue
		}
		r.mu.Lock()
		reason := t.revoked
		if t.frozen != nil && t.frozen.pty != nil {
			st := t.frozen.pty
			if reason == "" {
				reason = st.Revoked
			}
			if reason == "" && st.AuthorityDeadlineBootNS != 0 && now >= st.AuthorityDeadlineBootNS {
				reason = "authority_expired"
			}
		}
		if reason != "" {
			t.revoked = reason
		}
		r.mu.Unlock()
		if reason != "" {
			if err := t.pty.Revoke(reason); err != nil {
				return err
			}
		}
		if err := t.pty.PreActivate(); err != nil {
			return err
		}
	}
	return nil
}

// unfinishedTasks is called under Runtime.mu; task pointers stay pinned by the
// frozen admission or the activation reference throughout the attempt.
func unfinishedTasks(r *Runtime) []*task {
	var tasks []*task
	for _, t := range r.sortedTasks() {
		if !r.activation.completed[t] {
			tasks = append(tasks, t)
		}
	}
	return tasks
}

// TaskPIDsUntil is the bounded read-only half of the ownership precheck.
func TaskPIDsUntil(r *Runtime, deadline time.Time) ([]string, error) {
	if !lockBefore(&r.mu, deadline) {
		return nil, procrequest.ErrBusy
	}
	defer r.mu.Unlock()
	tasks := r.sortedTasks()
	pids := make([]string, 0, len(tasks))
	for _, t := range tasks {
		pids = append(pids, t.pid)
	}
	return pids, nil
}

// CheckLiveUntil keeps the agent's read-only cut predicates within the same
// ownership deadline; Export is a mutating fence API and is not a liveness read.
// The earlier caller used the following contract for its unfrozen query:
// Export rejects an unfrozen live runtime without changing it;
// ErrClosed is checked first, so this is a read-only liveness query.
// Reading closed directly also bounds admission without marking a frozen
// runtime exported or requiring a second ordinary-lock acquisition.
// origin: hot-update S4 internal review rv-fd rv-fd-3
func CheckLiveUntil(r *Runtime, deadline time.Time) error {
	if !lockBefore(&r.mu, deadline) {
		return procrequest.ErrBusy
	}
	defer r.mu.Unlock()
	if r.closed {
		return ErrClosed
	}
	return nil
}

// recoverFreezeTasks transfers the already closed admission to a rollback
// owner. It publishes no frozen cut and never closes a live master. The caller
// waits only within its original deadline; the stored acknowledgement prevents
// another activation/cut from overtaking restoration of this generation.
// origin: hot-update S4 internal review rv-fd rv-fd-3 / rv-fd-V1
func recoverFreezeTasks(r *Runtime, deadline time.Time, tasks, attempted []*task, snapshots map[*task]*taskFreeze, cause error, thawManager bool) error {
	// No actor was touched while references drained. The uncontended state
	// lock can cancel admission immediately even at the deadline; contention
	// transfers that same responsibility to the acknowledged recovery owner.
	if !thawManager && len(attempted) == 0 && r.mu.TryLock() {
		r.freezing = false
		// A composed rollback of an empty cut has completed every owner too.
		// Treating it as a no-actor freeze cancellation retained partialActive
		// forever even though there was nothing left that could fail activation.
		if r.partialActive && len(tasks) == 0 {
			finishRuntimeActivationLocked(r)
		} else {
			r.quiescing = r.partialActive
			if !r.partialActive {
				r.registry.Thaw()
			}
		}
		close(r.activation.recovery)
		broadcastAdmissionLocked(r)
		r.mu.Unlock()
		return cause
	}
	result := make(chan error, 1)
	go func() {
		r.mu.Lock()
		done := r.activation.recovery
		for t, tf := range snapshots {
			t.frozen = tf
		}
		completed := make(map[*task]bool, len(tasks))
		closed := r.closed
		r.mu.Unlock()
		var restoreErr error
		if !closed {
			if thawManager {
				r.cfg.Manager.Thaw()
			}
			// Undo only our barrier. On partial activation a previously frozen actor
			// may coexist with active ones; its actor Freeze/Activate is idempotent.
			for _, t := range tasks {
				r.mu.Lock()
				reason := t.revoked
				_, paused := snapshots[t]
				wasFrozen := t.frozen != nil
				r.mu.Unlock()
				if reason != "" {
					if t.pty != nil {
						restoreErr = errors.Join(restoreErr, t.pty.Revoke(reason))
					} else {
						_ = r.cfg.Manager.Signal(t.pid, syscall.SIGKILL)
					}
				}
				var err error
				if t.pty != nil {
					until := time.Now().Add(upgradebudget.LocalFreezeWindow)
					if paused || wasFrozen {
						err = t.pty.PreActivateUntil(until)
						if err == nil {
							err = t.pty.ActivateUntil(until)
						}
					} else {
						// Actor.Freeze owns its timeout rollback. Re-entering Activate on the
						// failing actor could wait on the same control token past our deadline.
						err = t.pty.ConfirmRunning(until)
					}
				} else {
					err = thawTask(t)
				}
				completed[t] = err == nil
				restoreErr = errors.Join(restoreErr, err)
			}
		}
		r.mu.Lock()
		r.activation.completed = completed
		r.freezing = false
		// A rollback fault remains a recoverable partial activation. Registry and
		// Handle admission stay closed until Thaw completes every unfinished owner.
		r.partialActive, r.quiescing = restoreErr != nil, restoreErr != nil
		if restoreErr == nil && !r.closed {
			finishRuntimeActivationLocked(r)
		}
		broadcastAdmissionLocked(r)
		close(done)
		r.mu.Unlock()
		result <- errors.Join(cause, restoreErr)
	}()
	if left := time.Until(deadline); left > 0 {
		timer := time.NewTimer(left)
		defer timer.Stop()
		select {
		case err := <-result:
			return err
		case <-timer.C:
		}
	}
	return errors.Join(cause, ErrRollbackPending)
}

func awaitFreezeRecovery(r *Runtime, deadline time.Time) error {
	if queued := r.activation.handoff.Load(); queued != nil {
		if err := awaitThawHandoff(queued, deadline); err != nil && !errors.Is(err, ErrRollbackPending) {
			return err
		}
		select {
		case <-queued.done:
		default:
			return ErrRollbackPending
		}
	}
	if !lockBefore(&r.mu, deadline) {
		return ErrRollbackPending
	}
	recovering, done := r.freezing, r.activation.recovery
	r.mu.Unlock()
	if !recovering || done == nil {
		return nil
	}
	timer := time.NewTimer(max(time.Until(deadline), 0))
	defer timer.Stop()
	select {
	case <-done:
		return nil
	case <-timer.C:
		return ErrRollbackPending
	}
}

// ThawUntil transfers restoration of a composed cut to the runtime's recovery
// transaction. A deadline expiry reports that retained responsibility instead
// of synchronously waiting for activation locks after the caller's cutoff.
// origin: hot-update S4 internal review rv-fd rv-fd-3 / rv-fd-V1
func ThawUntil(r *Runtime, deadline time.Time, thawManager bool) error {
	// origin: hot-update S4 internal review rv-fd rv-fd-3 activation recovery stall
	// The caller may arrive after its deadline, with mu briefly contended.
	// Publish the owner independently of mu: a pending receipt without this
	// handoff stranded a frozen runtime and all Handle callers indefinitely.
	for {
		generation := r.activation.generation.Load()
		queued := r.activation.handoff.Load()
		if queued != nil && queued.generation == generation {
			return awaitThawHandoff(queued, deadline)
		}
		// A delayed old-generation job cannot acknowledge this cut. Replace
		// its slot; its own generation guard and compare-and-swap epilogue
		// keep it from touching this cut or clearing the replacement owner.
		next := &thawHandoff{generation: generation, done: make(chan struct{})}
		if r.activation.handoff.CompareAndSwap(queued, next) {
			go completeThawHandoff(r, next, deadline, thawManager)
			return awaitThawHandoff(next, deadline)
		}
	}
}

func awaitThawHandoff(queued *thawHandoff, deadline time.Time) error {
	timer := time.NewTimer(max(time.Until(deadline), 0))
	defer timer.Stop()
	select {
	case <-queued.done:
		return queued.err
	case <-timer.C:
		return ErrRollbackPending
	}
}

func completeThawHandoff(r *Runtime, queued *thawHandoff, deadline time.Time, thawManager bool) {
	defer func() {
		r.activation.handoff.CompareAndSwap(queued, nil)
		close(queued.done)
	}()
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		queued.err = ErrClosed
		return
	}
	if r.activation.generation.Load() != queued.generation || r.freezing || r.activating {
		r.mu.Unlock()
		queued.err = ErrRollbackPending
		return
	}
	if !r.frozen && !r.partialActive {
		r.mu.Unlock()
		return // another owner already restored this generation
	}
	tasks := r.sortedTasks()
	snapshots := make(map[*task]*taskFreeze, len(tasks))
	for _, t := range tasks {
		if t.frozen != nil {
			snapshots[t] = t.frozen
		}
	}
	r.frozen, r.partialActive, r.freezing, r.quiescing = false, true, true, true
	r.thawed = r.imported
	r.activation.recovery = make(chan struct{})
	r.mu.Unlock()
	queued.err = recoverFreezeTasks(r, deadline, tasks, tasks, snapshots, nil, thawManager)
}

// finishRuntimeActivationLocked opens admission only after every owner is
// running, and installs observers for imported tasks even on cut rollback.
func finishRuntimeActivationLocked(r *Runtime) {
	r.partialActive = false
	r.activation.completed = nil
	r.quiescing = false
	r.registry.Thaw()
	r.frozenRegistry = procrequest.State{}
	r.frozen = false
	for _, t := range r.sortedTasks() {
		t.frozen = nil
		if r.imported && !t.owned && t.terminalAt == 0 {
			t.owned = true
			go func(t *task) {
				defer r.conclude(t)
				if t.kind == proto.ProcessKindPTY {
					r.observePTY(t, nil)
				} else {
					r.observe(t, nil)
				}
			}(t)
		}
	}
	r.exported = false
	broadcastAdmissionLocked(r)
}
