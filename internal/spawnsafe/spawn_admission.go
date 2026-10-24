// spawn_admission.go owns the image cut over all bounded spawn/lookup/touch
// operations and mount-probe launches, including callers outside Runtime.
package spawnsafe

import (
	"errors"
	"fmt"
	"time"
)

var errSpawnCutActive = errors.New("spawnsafe: image cut already active")

// spawnSlot is one operation, not just one count. A touch/resolver publishes
// its result after releasing; a timer selected at the same instant must not
// convert another operation's slot into an abandoned one.
type spawnSlot struct {
	policy    *Policy
	abandoned bool // protected by policy.mu
	released  bool // protected by policy.mu
}

// lockSpawnAdmission returns with mu held and admission open. Waiting does
// not spend the spawn window: no syscall/helper exists until acquisition.
// Like Runtime's queued creates, these pre-start waiters disappear safely on
// exec and resume on rollback. No policy mutex is held across that wait.
func (p *Policy) lockSpawnAdmission() {
	p.mu.Lock()
	for p.spawnCut != nil {
		resume := p.spawnCut
		p.mu.Unlock()
		<-resume
		p.mu.Lock()
	}
}

func (p *Policy) acquireSpawnSlot() *spawnSlot {
	p.lockSpawnAdmission()
	defer p.mu.Unlock()
	if p.inFlight+p.wedged >= p.wedgeCeiling {
		return nil
	}
	p.inFlight++
	return &spawnSlot{policy: p}
}

func (s *spawnSlot) abandon() {
	p := s.policy
	p.mu.Lock()
	defer p.mu.Unlock()
	if s.released || s.abandoned {
		return
	}
	s.abandoned = true
	p.inFlight--
	p.wedged++
	p.notifySpawnChangedLocked()
}

func (s *spawnSlot) release() {
	p := s.policy
	p.mu.Lock()
	defer p.mu.Unlock()
	if s.released {
		return
	}
	s.released = true
	if s.abandoned {
		p.wedged--
	} else {
		p.inFlight--
	}
	p.notifySpawnChangedLocked()
}

func (p *Policy) notifySpawnChangedLocked() {
	if p.spawnChanged != nil {
		close(p.spawnChanged)
		p.spawnChanged = nil
	}
}

func lockSpawnBefore(p *Policy, deadline time.Time) bool {
	for time.Now().Before(deadline) {
		if p.mu.TryLock() {
			return true
		}
		time.Sleep(min(time.Millisecond, time.Until(deadline)))
	}
	return false
}

// Freeze closes slot/probe admission and waits for in-time slots to drain.
// Call only AFTER Runtime.Freeze has quiesced creations and joined startup
// owners, and BEFORE Manager.Freeze. Otherwise an already-reserved creation
// could wait here while Runtime waits for that same creation to adopt.
// Abandoned operations and probes keep their existing immediate deferral;
// one absolute cut deadline covers lock acquisition and every drain wakeup.
// On failure this call reopens its own admission; on success Thaw is required
// if exec does not happen. Export still independently rejects live operations.
// origin: hot-update S4 plan §4.1 / SPAWN-INFLIGHT tests 1–4.
func (p *Policy) Freeze(deadline time.Time) error {
	if !lockSpawnBefore(p, deadline) {
		return fmt.Errorf("%w: policy guard deadline", ErrSpawnInFlight)
	}
	if p.spawnCut != nil {
		p.mu.Unlock()
		return errSpawnCutActive
	}
	p.spawnCut = make(chan struct{})
	cut := p.spawnCut
	timer := time.NewTimer(max(time.Until(deadline), 0))
	defer timer.Stop()
	for p.spawnCut == cut {
		var err error
		switch {
		case p.wedged > 0:
			err = fmt.Errorf("%w: abandoned=%d", ErrSpawnInFlight, p.wedged)
		case p.probesInFlight > 0:
			err = fmt.Errorf("%w: probes=%d", ErrProbeInFlight, p.probesInFlight)
		case !time.Now().Before(deadline):
			err = fmt.Errorf("%w: drain deadline, in flight=%d", ErrSpawnInFlight, p.inFlight)
		case p.inFlight == 0:
			p.mu.Unlock()
			return nil
		}
		if err != nil {
			p.thawSpawnLocked()
			p.mu.Unlock()
			return err
		}
		if p.spawnChanged == nil {
			p.spawnChanged = make(chan struct{})
		}
		changed := p.spawnChanged
		p.mu.Unlock()
		select {
		case <-changed:
		case <-timer.C:
		}
		if !lockSpawnBefore(p, deadline) {
			// The short mutex may be contended at the absolute cutoff. Keep
			// cancellation owned without extending the caller's budget.
			go p.cancelSpawnCut(cut)
			return fmt.Errorf("%w: drain deadline", ErrSpawnInFlight)
		}
	}
	p.mu.Unlock()
	return fmt.Errorf("%w: cut canceled while draining", ErrSpawnInFlight)
}

func (p *Policy) cancelSpawnCut(cut chan struct{}) {
	p.mu.Lock()
	if p.spawnCut == cut {
		p.thawSpawnLocked()
	}
	p.mu.Unlock()
}

// Thaw reopens spawn/probe admission after a failed or abandoned image switch.
// It is idempotent, so a failed cut's cancellation can safely finish later.
func (p *Policy) Thaw() {
	p.mu.Lock()
	p.thawSpawnLocked()
	p.mu.Unlock()
}

func (p *Policy) thawSpawnLocked() {
	if p.spawnCut != nil {
		close(p.spawnCut)
		p.spawnCut = nil
		p.notifySpawnChangedLocked()
	}
}
