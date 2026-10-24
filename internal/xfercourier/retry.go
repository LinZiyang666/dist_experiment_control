package xfercourier

import (
	"bytes"
	"errors"
	"math"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/xfercontract"
)

const (
	settlementAttempt = 5 * time.Second
	retryMinimum      = 500 * time.Millisecond
	retryMaximum      = 2 * time.Second
)

// Step performs at most one settlement request. The registry drives retries;
// an expired byte budget is absent here by design. Each request has a fresh
// bounded settlement attempt, clipped to the enclosing operation deadline.
// origin: hot-update S7 plan §1.2 / §4 / T16
func (c *courier) Step(deadline xfercontract.Deadline) error {
	c.mu.Lock()
	// This guard and Acknowledge's admission close share the same lock. Checking
	// a receipt after sending would allow an extra terminal after exact ACK.
	// origin: hot-update S7 plan §4 / T16
	if c.state.Stopped {
		c.mu.Unlock()
		return nil
	}
	if c.freezing || c.stopping || c.stepping {
		c.mu.Unlock()
		return ErrRecovering
	}
	if c.conflict != nil {
		c.mu.Unlock()
		return ErrConflict
	}
	now, remaining, err := c.remaining(deadline)
	if err != nil {
		c.mu.Unlock()
		return err
	}
	if c.state.NextAttempt.Deadline != 0 && now < int64(c.state.NextAttempt.Deadline) {
		c.mu.Unlock()
		return nil
	}
	if c.state.Attempt.Ordinal == math.MaxUint32 {
		c.mu.Unlock()
		return errors.New("xfercourier: attempt ordinal exhausted")
	}
	if remaining > settlementAttempt {
		remaining = settlementAttempt
	}
	bound := xfercontract.Deadline(now + int64(remaining))
	// Every attempt sends a fresh copy of the SAME receiver evidence. A transport
	// may serialize/modify its request without modifying retained responsibility.
	outcome := c.state.Outcome
	request := xfercontract.Request{Op: xfercontract.OpTerminal, CallID: terminalID(c.state.Identity),
		Identity: c.state.Identity, Origin: c.state.Capsule.Origin, Outcome: &outcome}
	attempt := xfercontract.Attempt{LogicalID: request.CallID, Ordinal: c.state.Attempt.Ordinal + 1,
		Phase: controlbudget.PhaseRequest, StartedMonoNS: now}
	c.state.Attempt = attempt
	c.stepping = true
	c.enterLocked()
	c.mu.Unlock()
	defer c.leaveStep()
	c.observe(xfercontract.ObserveCallbackEnter, attempt)
	response, wireAttempt, requestErr := c.config.Transport.Request(request, bound)
	// The transport owns sent-unknown classification. No response or error is
	// permission to clear the original outcome, capsule, or canonical bytes.
	attempt.Sent = wireAttempt.Sent
	attempt.Class = wireAttempt.Class
	boot, end, clockErr := c.config.Clock()
	if clockErr != nil || boot != c.bootID || end < now {
		end = now
		requestErr = errors.Join(requestErr, errors.New("xfercourier: settlement clock unavailable"))
	}
	attempt.FinishedMonoNS = end
	if requestErr == nil {
		if response.Code == xfercontract.CodeTerminalConflict {
			requestErr = ErrConflict
		} else if response.Code != "" || response.Error != "" {
			requestErr = ErrReconcile
		} else if response.Receipt == nil {
			requestErr = ErrReconcile
		} else {
			requestErr = c.Acknowledge(*response.Receipt)
		}
	}
	c.mu.Lock()
	if !c.state.Stopped {
		attempt.BackoffNS = int64(retryDelay(attempt.Ordinal))
		if end > math.MaxInt64-attempt.BackoffNS {
			requestErr = errors.Join(requestErr, controlbudget.ErrInvalidDeadline)
		} else {
			c.state.NextAttempt = xfercontract.BootDeadline{BootID: c.bootID,
				Deadline: xfercontract.Deadline(end + attempt.BackoffNS)}
		}
	}
	c.state.Attempt = attempt
	c.mu.Unlock()
	c.observe(xfercontract.ObserveAttempt, attempt)
	c.observe(xfercontract.ObserveCallbackJoin, attempt)
	return requestErr
}

// Freeze is a snapshot/join barrier; the enclosing registry owns admission
// after it returns. A timed-out barrier reopens this original actor immediately,
// so the registry can Thaw without a late snapshot writer or renewed retry.
// origin: hot-update S7 plan §8.2 / T16
func (c *courier) Freeze(deadline xfercontract.Deadline) (xfercontract.CourierState, error) {
	c.mu.Lock()
	if c.freezing || c.stopping {
		c.mu.Unlock()
		return xfercontract.CourierState{}, ErrRecovering
	}
	c.freezing = true
	idle := c.idle
	c.mu.Unlock()
	defer func() { c.mu.Lock(); c.freezing = false; c.mu.Unlock() }()
	if err := c.join(idle, deadline); err != nil {
		return xfercontract.CourierState{}, err
	}
	c.mu.Lock()
	if c.stopping {
		c.mu.Unlock()
		return xfercontract.CourierState{}, ErrRecovering
	}
	if c.conflict != nil {
		c.mu.Unlock()
		return xfercontract.CourierState{}, ErrConflict
	}
	state := cloneState(c.state)
	c.enterLocked()
	c.mu.Unlock()
	defer c.leave()
	c.observe(xfercontract.ObserveFreeze, state.Attempt)
	return state, nil
}

// Stop closes future admission before waiting for the in-flight request and
// observation callbacks. A timeout retains reservations and the entire state;
// a subsequent Stop can join and release them exactly once.
// origin: hot-update S7 plan §8.2 / §8.3 / T16
func (c *courier) Stop(deadline xfercontract.Deadline) error {
	c.mu.Lock()
	c.stopping = true
	idle := c.idle
	c.mu.Unlock()
	if err := c.join(idle, deadline); err != nil {
		return err
	}
	c.mu.Lock()
	c.state.Stopped = true
	err := c.release()
	c.stopping = false
	c.mu.Unlock()
	return err
}

func (c *courier) release() error {
	var result error
	for _, slot := range []*xfercontract.ReservationID{&c.canonicalReservation, &c.reservation} {
		if *slot == "" {
			continue
		}
		if err := c.config.Capacity.Release(*slot); err != nil {
			result = errors.Join(result, errors.New("xfercourier: reservation release failed"))
		} else {
			*slot = ""
		}
	}
	return result
}

func (c *courier) remaining(deadline xfercontract.Deadline) (int64, time.Duration, error) {
	if deadline <= 0 {
		return 0, 0, controlbudget.ErrInvalidDeadline
	}
	boot, now, err := c.config.Clock()
	if err != nil || boot != c.bootID || now <= 0 {
		return 0, 0, errors.New("xfercourier: settlement clock unavailable")
	}
	if now >= int64(deadline) {
		return now, 0, controlbudget.ErrExpired
	}
	return now, time.Duration(int64(deadline) - now), nil
}

func (c *courier) join(idle <-chan struct{}, deadline xfercontract.Deadline) error {
	_, remaining, err := c.remaining(deadline)
	if err != nil {
		return err
	}
	timer := time.NewTimer(remaining)
	defer timer.Stop()
	select {
	case <-idle:
		_, _, err := c.remaining(deadline)
		return err
	case <-timer.C:
		return controlbudget.ErrExpired
	}
}

func (c *courier) enterLocked() {
	if c.active == 0 {
		c.idle = make(chan struct{})
	}
	c.active++
}

func (c *courier) leave() {
	c.mu.Lock()
	c.active--
	if c.active == 0 {
		close(c.idle)
	}
	c.mu.Unlock()
}

func (c *courier) leaveStep() {
	c.mu.Lock()
	c.stepping = false
	c.active--
	if c.active == 0 {
		close(c.idle)
	}
	c.mu.Unlock()
}

func (c *courier) observe(kind xfercontract.ObservationKind, attempt xfercontract.Attempt) {
	if c.config.Observe == nil {
		return
	}
	// Receipt callbacks can overlap an already admitted request. Allocate and
	// emit sequence numbers under one producer lock so the collector cannot
	// observe event 2 before event 1 and falsely poison a healthy cut.
	// origin: hot-update S7 plan §10.4 / T16
	c.observationMu.Lock()
	defer c.observationMu.Unlock()
	boot, now, err := c.config.Clock()
	if err != nil {
		now = 0
	}
	c.mu.Lock()
	c.eventSeq++
	s := c.state
	o := xfercontract.Observation{Kind: kind, BootID: boot, BoottimeNS: now, EventSeq: c.eventSeq,
		TransferID: s.Identity.TransferID, EntryNonce: s.Identity.EntryNonce, ObjectVersion: s.Identity.ObjectVersion,
		Phase: xfercontract.PhaseTerminalPending, Outcome: s.Outcome.Outcome, Primitive: s.Outcome.Primitive,
		ParentSynced: s.Outcome.ParentSynced, GrantNonce: s.Outcome.Grant.GrantNonce,
		CanonicalDigest: s.Canonical.Digest, AdmissionClosed: s.Stopped || c.stopping || c.freezing, Attempt: attempt}
	if s.Receipt != nil {
		o.Phase = xfercontract.PhaseHistoryConfirmed
		o.HistorySeq = s.Receipt.HistorySeq
		o.StopNonce = s.Receipt.TerminalID
	}
	c.mu.Unlock()
	c.config.Observe(o) // Capsule, error strings and canonical payload never enter observations.
}

func retryDelay(ordinal uint32) time.Duration {
	if ordinal <= 1 {
		return retryMinimum
	}
	if ordinal == 2 {
		return 2 * retryMinimum
	}
	return retryMaximum
}

func cloneState(state xfercontract.CourierState) xfercontract.CourierState {
	state.Canonical.Bytes = bytes.Clone(state.Canonical.Bytes)
	if state.Receipt != nil {
		receipt := *state.Receipt
		state.Receipt = &receipt
	}
	return state
}
