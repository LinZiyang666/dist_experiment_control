package ptysession

import (
	"errors"
	"os"
	"time"

	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/workload"
)

func (a *Actor) takeControl(deadline time.Time) error {
	if !time.Now().Before(deadline) {
		return os.ErrDeadlineExceeded
	}
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	select {
	case <-a.control:
		return nil
	case <-timer.C:
		return os.ErrDeadlineExceeded
	}
}

// Even admission to the short accounting lock shares the caller's deadline.
// No new goroutine is stranded trying to acquire a mutex after timeout.
func (a *Actor) lockUntil(deadline time.Time) error {
	for time.Now().Before(deadline) {
		if a.mu.TryLock() {
			return nil
		}
		timer := time.NewTimer(min(time.Millisecond, time.Until(deadline)))
		<-timer.C
	}
	return os.ErrDeadlineExceeded
}

func (a *Actor) pauseLocked(p *pump) {
	if p.paused {
		return
	}
	p.paused = true
	if p.running && !stopped(p.stop) {
		close(p.stop)
	}
}

func waitPump(done <-chan struct{}, deadline time.Time) error {
	if !time.Now().Before(deadline) {
		return os.ErrDeadlineExceeded
	}
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	select {
	case <-done:
		return nil
	case <-timer.C:
		return os.ErrDeadlineExceeded
	}
}

// Each direction owns exactly one deadline, including clears on recovery.
func (a *Actor) directionDeadline(read bool, deadline time.Time) error {
	if read {
		return a.cfg.Master.SetReadDeadline(deadline)
	}
	return a.cfg.Master.SetWriteDeadline(deadline)
}

// pause joins each direction independently. Master deadlines never share a
// SetDeadline operation, so one direction cannot silently release the other.
// origin: hot-update S4 plan §4.1 / T06 / T07
func (a *Actor) pause(deadline time.Time) error {
	err := pauseGeneration(a, deadline)
	if err != nil {
		a.resume()
	}
	return err
}

// pauseGeneration leaves timeout recovery with the lifecycle token owner.
func pauseGeneration(a *Actor, deadline time.Time) error {
	if err := a.lockUntil(deadline); err != nil {
		return err
	}
	if a.phase != phaseActive {
		a.mu.Unlock()
		return workload.ErrFrozen
	}
	a.phase = phaseFreezing
	a.pauseLocked(&a.read)
	a.pauseLocked(&a.write)
	rd, wd := a.read.done, a.write.done
	closed := a.state.MasterClosed
	a.mu.Unlock()
	var err error
	if !closed {
		err = errors.Join(a.directionDeadline(true, time.Now()), a.directionDeadline(false, time.Now()))
	}
	if err == nil {
		err = waitPump(rd, deadline)
	}
	if err == nil {
		err = waitPump(wd, deadline)
	}
	return err
}

// resume is called with the control token. A generation still leaving its
// syscall keeps its stop request and restarts itself only after recording n.
func (a *Actor) resume() {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.phase == phaseClosed {
		return
	}
	a.cfg.Input.Thaw()
	a.cfg.Output.Thaw()
	if !a.state.MasterClosed {
		if err := a.directionDeadline(true, time.Time{}); err != nil {
			a.state.OutputError = err.Error()
			a.source = sourceEnded
		}
		if err := a.directionDeadline(false, time.Time{}); err != nil {
			a.abortInputLocked("write_error")
		}
	}
	a.phase = phaseActive
	a.files = nil
	a.prepared = false
	a.read.paused = false
	a.write.paused = false
	a.startPumpsLocked()
}

// Freeze borrows files only after both byte-accounting barriers. Repeated
// export of a frozen actor refreshes revocation metadata without thawing I/O.
func (a *Actor) Freeze(deadline time.Time) (Image, map[string]*os.File, error) {
	if err := a.takeControl(deadline); err != nil {
		return Image{}, nil, err
	}
	release := true
	defer func() {
		if release {
			a.control <- struct{}{}
		}
	}()
	if err := a.lockUntil(deadline); err != nil {
		return Image{}, nil, err
	}
	if a.phase == phaseFrozen && a.files != nil {
		state := a.snapshotLocked()
		files := copyFiles(a.files)
		a.mu.Unlock()
		return state, files, nil
	}
	a.mu.Unlock()
	if err := pauseGeneration(a, deadline); err != nil {
		release = false
		go recoverFreezeGeneration(a)
		return Image{}, nil, err
	}
	state, files, err := a.freezeReplays(deadline)
	if err != nil {
		release = false
		go recoverFreezeGeneration(a)
		return Image{}, nil, err
	}
	return state, files, nil
}

func (a *Actor) freezeReplays(deadline time.Time) (Image, map[string]*os.File, error) {
	if err := a.lockUntil(deadline); err != nil {
		return Image{}, nil, err
	}
	closed := a.state.MasterClosed
	observation := a.state.Observation
	a.mu.Unlock()
	if !closed {
		var err error
		observation, err = a.cfg.Master.Observe()
		if err != nil {
			return Image{}, nil, err
		}
	}
	if err := a.lockUntil(deadline); err != nil {
		return Image{}, nil, err
	}
	defer a.mu.Unlock()
	_, in, err := a.cfg.Input.Freeze()
	if err != nil {
		return Image{}, nil, err
	}
	_, out, err := a.cfg.Output.Freeze()
	if err != nil {
		a.cfg.Input.Thaw()
		return Image{}, nil, err
	}
	if !time.Now().Before(deadline) {
		return Image{}, nil, os.ErrDeadlineExceeded
	}
	files := map[string]*os.File{"input-replay": in, "output-replay": out}
	if !closed {
		files["master"] = a.cfg.Master.Borrow()
		if files["master"] == nil {
			return Image{}, nil, errors.New("ptysession: missing frozen master")
		}
	}
	a.state.Observation = observation
	a.phase = phaseFrozen
	a.files = files
	return a.snapshotLocked(), copyFiles(files), nil
}

// PreActivate records expired authority while every imported borrower is still
// stopped. It performs no signals while the shared Manager remains frozen.
func (a *Actor) PreActivate() error {
	return a.PreActivateUntil(time.Now().Add(upgradebudget.LocalFreezeWindow))
}

// PreActivateUntil shares the transaction deadline with control and accounting.
func (a *Actor) PreActivateUntil(deadline time.Time) error {
	if err := a.takeControl(deadline); err != nil {
		return err
	}
	defer func() { a.control <- struct{}{} }()
	if err := a.lockUntil(deadline); err != nil {
		return err
	}
	defer a.mu.Unlock()
	// origin: hot-update S4 internal review rv-fd rv-fd-2
	// A completed owner is already running; retry must not touch its deadlines.
	if a.phase == phaseActive {
		return nil
	}
	now, err := a.cfg.Clock()
	if err != nil {
		return err
	}
	if a.phase != phaseFrozen {
		return workload.ErrFrozen
	}
	if a.state.AuthorityDeadlineBootNS != 0 && now >= a.state.AuthorityDeadlineBootNS && a.state.Revoked == "" {
		a.state.Revoked = "authority expired"
		a.state.CleanupPending = true
	}
	if a.state.Revoked != "" {
		a.abortInputLocked(revokedInputReason(a.state.Revoked))
	}
	a.prepared = true
	return nil
}

// Activate is legal only after Manager activation. Revoked input never starts:
// cleanup must run first, and only an allowed output drainer may follow it.
func (a *Actor) Activate() error {
	return a.ActivateUntil(time.Now().Add(upgradebudget.LocalFreezeWindow))
}

// ActivateUntil bounds admission to an unfinished owner.
func (a *Actor) ActivateUntil(deadline time.Time) error {
	if err := a.takeControl(deadline); err != nil {
		return err
	}
	defer func() { a.control <- struct{}{} }()
	frozen, err := workload.FrozenUntil(a.cfg.Manager, deadline)
	if err != nil {
		return err
	}
	if frozen {
		return workload.ErrFrozen
	}
	if err := a.lockUntil(deadline); err != nil {
		return err
	}
	if a.phase == phaseActive {
		a.mu.Unlock()
		return nil
	}
	a.mu.Unlock()
	now, err := a.cfg.Clock()
	if err != nil {
		return err
	}
	if err := a.lockUntil(deadline); err != nil {
		return err
	}
	if a.state.AuthorityDeadlineBootNS != 0 && now >= a.state.AuthorityDeadlineBootNS && a.state.Revoked == "" {
		a.state.Revoked = "authority expired"
		a.state.CleanupPending = true
		a.abortInputLocked(revokedInputReason(a.state.Revoked))
	}
	if a.phase != phaseFrozen {
		a.mu.Unlock()
		return workload.ErrFrozen
	}
	prepared := a.prepared
	a.mu.Unlock()
	if !prepared {
		return errors.New("ptysession: activation was not prepared")
	}
	// A frozen source may have ended while Manager was frozen. Retry the reap
	// release here; it is safe only for true source end or explicit master close.
	if err := a.cleanup(); err != nil {
		return err
	}
	a.mu.Lock()
	// A revoked, full replay must not backpressure a killed child forever.
	// Both imported borrowers are stopped here, so the retained End is final.
	if a.state.Revoked != "" && a.source == sourceReading && !a.state.MasterClosed {
		a.state.OutputError = "pty output aborted: " + a.state.Revoked
		a.source = sourceDiscarding
	}
	a.mu.Unlock()
	if err := a.releaseReap(); err != nil && !errors.Is(err, workload.ErrUnknown) {
		return err
	}
	a.resume()
	return nil
}

// recoverFreezeGeneration owns the transferred lifecycle token until the old
// generations acknowledge their final accounting and recovery commits. Every
// subsequent Freeze/control/Close confirms that commit by acquiring this token;
// no late recovery can overlap another lifecycle transaction or resurrect Close.
// origin: hot-update S4 internal review rv-fd rv-fd-3
func recoverFreezeGeneration(a *Actor) {
	defer func() { a.control <- struct{}{} }()
	a.mu.Lock()
	if a.phase != phaseFreezing {
		a.mu.Unlock()
		return
	}
	rd, wd := a.read.done, a.write.done
	a.mu.Unlock()
	<-rd
	<-wd
	a.resume()
}

// ConfirmRunning waits for a timeout recovery's token acknowledgement before
// Runtime may reopen admission. It never accepts a half-frozen generation.
func (a *Actor) ConfirmRunning(deadline time.Time) error {
	if err := a.takeControl(deadline); err != nil {
		return err
	}
	defer func() { a.control <- struct{}{} }()
	if err := a.lockUntil(deadline); err != nil {
		return err
	}
	defer a.mu.Unlock()
	if a.phase != phaseActive {
		return workload.ErrFrozen
	}
	return nil
}
