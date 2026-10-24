package ptysession

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"os"
	"syscall"
	"time"

	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/workload"
)

func controlDigest(r ControlRequest) (string, error) {
	switch r.Op {
	case "resize":
		if r.Cols == 0 || r.Rows == 0 || r.Signal != 0 {
			return "", ErrConflict
		}
	case "signal":
		if r.Cols != 0 || r.Rows != 0 || (r.Signal != int(syscall.SIGINT) && r.Signal != int(syscall.SIGTERM) && r.Signal != int(syscall.SIGHUP)) {
			return "", ErrConflict
		}
	case "hangup", "keepalive":
		if r.Cols != 0 || r.Rows != 0 || r.Signal != 0 {
			return "", ErrConflict
		}
	default:
		return "", ErrConflict
	}
	if r.Seq == 0 {
		return "", ErrConflict
	}
	return fmt.Sprintf("%x", sha256.Sum256([]byte(fmt.Sprintf("%s:%d:%d:%d", r.Op, r.Cols, r.Rows, r.Signal)))), nil
}

func (a *Actor) receiptLocked() ControlReceipt {
	return ControlReceipt{Seq: a.state.ControlSeq, Digest: a.state.ControlDigest, Error: a.state.ControlError}
}

func (a *Actor) commitControlLocked(r ControlRequest, digest string, err error) ControlReceipt {
	a.state.ControlSeq = r.Seq
	a.state.ControlDigest = digest
	a.state.ControlError = ""
	if err != nil {
		reason := "ioctl_error"
		switch r.Op {
		case "signal":
			reason = "signal_error"
		case "hangup":
			reason = "signal_error"
			if a.state.MasterClosed {
				reason = "close_error"
			}
		case "keepalive":
			reason = "authority_expired"
		}
		a.state.ControlError = "pty control failed: " + reason
	}
	return a.receiptLocked()
}

// ApplyControl deduplicates the side effect, not a carrier CallID. Failed
// operations also commit a receipt; only a rejected Manager guard leaves seq.
// origin: hot-update S4 plan §3.4 / T09 / T14
func (a *Actor) ApplyControl(r ControlRequest) (ControlReceipt, error) {
	digest, err := controlDigest(r)
	if err != nil {
		return ControlReceipt{}, err
	}
	deadline := time.Now().Add(upgradebudget.LocalFreezeWindow)
	if err := a.takeControl(deadline); err != nil {
		return ControlReceipt{}, err
	}
	defer func() { a.control <- struct{}{} }()
	a.mu.Lock()
	last := a.receiptLocked()
	if a.phase != phaseActive {
		a.mu.Unlock()
		return last, workload.ErrFrozen
	}
	if a.state.Revoked != "" {
		a.mu.Unlock()
		return last, ErrRevoked
	}
	if r.Seq == last.Seq {
		a.mu.Unlock()
		if digest != last.Digest {
			return last, ErrConflict
		}
		return last, nil
	}
	if r.Seq < last.Seq {
		a.mu.Unlock()
		return last, ErrStale
	}
	if last.Seq == math.MaxUint64 || r.Seq != last.Seq+1 {
		a.mu.Unlock()
		return last, ErrConflict
	}
	a.mu.Unlock()
	// Duplicate receipts above remain idempotent, including KA. New effects
	// require current independent authority inside the control transaction,
	// rather than depending on the next liveness Tick to notice expiration.
	// origin: hot-update S4 internal review rv-auth RV-AUTH-2
	a.mu.Lock()
	authority := a.state.AuthorityDeadlineBootNS
	a.mu.Unlock()
	if authority != 0 {
		now, clockErr := a.cfg.Clock()
		if clockErr != nil {
			return last, clockErr
		}
		if now >= authority {
			_, _, revokeErr := markRevoked(a, "authority expired", false)
			if revokeErr == nil {
				revokeErr = finishRevocation(a, "authority expired", deadline)
			}
			return last, errors.Join(ErrRevoked, revokeErr)
		}
	}
	if r.Op == "hangup" {
		return a.hangup(deadline, &r, digest)
	}
	var receipt ControlReceipt
	err = a.cfg.Manager.WithControl(a.cfg.PID, func(c workload.Control) error {
		a.mu.Lock()
		revoked := a.state.Revoked != ""
		a.mu.Unlock()
		if revoked {
			return ErrRevoked
		}
		var effectErr error
		switch r.Op {
		case "resize":
			a.mu.Lock()
			closed := a.state.MasterClosed
			a.mu.Unlock()
			if closed {
				effectErr = os.ErrClosed
			} else {
				effectErr = a.cfg.Master.Resize(r.Cols, r.Rows)
			}
		case "signal":
			if c.Exit != nil {
				effectErr = workload.ErrExited
			} else {
				effectErr = c.Signal(syscall.Signal(r.Signal))
			}
		case "keepalive":
			effectErr = a.renewKA(r.Seq)
		default:
			return ErrConflict
		}
		a.mu.Lock()
		receipt = a.commitControlLocked(r, digest, effectErr)
		a.mu.Unlock()
		return nil
	})
	if err != nil {
		return last, err
	}
	return receipt, nil
}

// hangup holds one Manager transaction through Signal, Close and the receipt.
// Pump joins happen before entering that guard, never inside a Manager lock.
// origin: hot-update S4 plan §3.4 / T08 / T09
func (a *Actor) hangup(deadline time.Time, r *ControlRequest, digest string) (ControlReceipt, error) {
	a.mu.Lock()
	last := a.receiptLocked()
	a.mu.Unlock()
	if err := a.pause(deadline); err != nil {
		return last, err
	}
	defer a.resume()
	var receipt = last
	var operationErr error
	err := a.cfg.Manager.WithControl(a.cfg.PID, func(c workload.Control) error {
		var guardErr error
		receipt, operationErr, guardErr = a.commitHangup(c, r, digest)
		return guardErr
	})
	if err != nil {
		return last, err
	}
	a.mu.Lock()
	closed := a.state.MasterClosed
	a.mu.Unlock()
	if closed {
		_ = a.cfg.Manager.AllowReap(a.cfg.PID)
	}
	// Wire controls carry failures in their replayable receipt. Internal KA
	// transactions instead return the error so a later bounded tick can retry.
	if r == nil {
		return receipt, operationErr
	}
	return receipt, nil
}

// commitHangup is called only with the control token and Manager guard held.
// Keeping the transaction in one function makes every failure's ownership and
// sequence decision explicit without giving tests a substitute process owner.
func (a *Actor) commitHangup(c workload.Control, r *ControlRequest, digest string) (ControlReceipt, error, error) {
	a.mu.Lock()
	closed, revoked := a.state.MasterClosed, a.state.Revoked != ""
	receipt := a.receiptLocked()
	a.mu.Unlock()
	if revoked {
		return receipt, nil, ErrRevoked
	}
	var effectErr error
	if !closed && c.Exit == nil {
		effectErr = c.Signal(syscall.SIGHUP)
	}
	if errors.Is(effectErr, syscall.ESRCH) {
		effectErr = nil
	}
	if effectErr == nil && !closed {
		effectErr = a.cfg.Master.Close()
		a.mu.Lock()
		a.state.MasterClosed = true
		a.source = sourceEnded // Explicit close stops discard-drain without claiming true EOF.
		a.abortInputLocked("hangup")
		if a.state.OutputError == "" && !a.cfg.Output.Snapshot().FIN {
			a.state.OutputError = "pty output aborted: hangup"
		}
		if effectErr != nil {
			effectErr = fmt.Errorf("close_error: %w", effectErr)
			a.state.CleanupPending = true
		}
		a.mu.Unlock()
	}
	a.mu.Lock()
	if r != nil {
		receipt = a.commitControlLocked(*r, digest, effectErr)
	}
	a.mu.Unlock()
	return receipt, effectErr, nil
}

func normalizeInterval(ms int) int {
	if ms <= 0 {
		return 0
	}
	return min(60000, max(1000, ms))
}

func grantDeadline(now int64, ms int) (int64, error) {
	grace := max(6*time.Duration(ms)*time.Millisecond, upgradebudget.PTYRecovery)
	if now < 0 || now > math.MaxInt64-int64(grace) {
		return 0, fmt.Errorf("ptysession: keepalive clock overflow")
	}
	return now + int64(grace), nil
}

func (a *Actor) renewKA(seq uint64) error {
	now, err := a.cfg.Clock()
	if err != nil {
		return err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.state.Revoked != "" {
		return ErrRevoked
	}
	if a.state.KAIntervalMS != 0 {
		deadline, err := grantDeadline(now, a.state.KAIntervalMS)
		if err != nil {
			return err
		}
		a.state.KADeadlineBootNS = deadline
	}
	a.state.LastKASeq = seq
	a.state.ExpiredSinceBootNS = 0
	a.state.RecoveryNotBeforeBootNS = 0
	a.state.NextJudgeBootNS = 0
	a.proof = HealthProof{}
	return nil
}

// Tick consumes externally obtained round-trip evidence. Neither reconnect nor
// import is evidence of a live control path; proof is never part of Image.
// origin: hot-update S4 plan §6.1 / T22
func (a *Actor) Tick(proof HealthProof) error {
	now, err := a.cfg.Clock()
	if err != nil {
		return err
	}
	a.mu.Lock()
	authority := a.state.AuthorityDeadlineBootNS
	a.mu.Unlock()
	if authority != 0 && now >= authority {
		return a.Revoke("authority expired")
	}
	deadline := time.Now().Add(upgradebudget.LocalFreezeWindow)
	if err := a.takeControl(deadline); err != nil {
		return err
	}
	defer func() { a.control <- struct{}{} }()
	a.mu.Lock()
	if a.phase != phaseActive {
		a.mu.Unlock()
		return workload.ErrFrozen
	}
	revoked := a.state.Revoked != ""
	a.mu.Unlock()
	if revoked {
		if err := a.cleanup(); err != nil {
			return err
		}
		return a.releaseReap()
	}
	a.mu.Lock()
	judge := a.judgeLocked(now, proof)
	a.mu.Unlock()
	if !judge {
		return nil
	}
	_, err = a.hangup(deadline, nil, "")
	return err
}

func (a *Actor) judgeLocked(now int64, p HealthProof) bool {
	s := &a.state
	if s.KAIntervalMS == 0 || s.MasterClosed || now < s.KADeadlineBootNS {
		a.proof = HealthProof{}
		return false
	}
	if s.ExpiredSinceBootNS == 0 {
		s.ExpiredSinceBootNS = max(now, 1)
		s.RecoveryNotBeforeBootNS = max(s.KADeadlineBootNS, now) + int64(upgradebudget.PTYRecoveryOpportunity)
	}
	// This runs with the control token held, after any token wait. Invalidating
	// the proof also clears a prior strike so disconnection never counts as
	// evidence that the controller died.
	// origin: hot-update S4 internal review rv-auth RV-AUTH-3
	if p.Verifier != nil && !p.Verifier.Current(p.Generation) {
		p.Healthy = false
	}
	if !p.Healthy || p.Generation == 0 || p.AtBootNS > now || p.AtBootNS <= 0 {
		a.proof = HealthProof{}
		return false
	}
	if a.proof.Generation != p.Generation {
		a.proof = HealthProof{}
	}
	if now < s.RecoveryNotBeforeBootNS || now < s.NextJudgeBootNS {
		return false
	}
	if p.AtBootNS < s.RecoveryNotBeforeBootNS || p.AtBootNS <= a.proof.AtBootNS {
		return false
	}
	second := a.proof.Healthy && a.proofDeadline == s.KADeadlineBootNS && a.proofSeq == s.LastKASeq
	a.proof = p
	a.proofDeadline = s.KADeadlineBootNS
	a.proofSeq = s.LastKASeq
	s.NextJudgeBootNS = now + int64(time.Second)
	return second
}

// Revoke records irreversible responsibility before taking the control token
// or reading the clock. Freeze can still export that record while cleanup waits.
func (a *Actor) Revoke(reason string) error {
	return revokeActor(a, reason, false)
}

// RevokeTerminal is reserved for the runtime's retained terminal result, after
// the Manager has completed waiting (before or after the runtime retires the
// exit row). Closed master alone
// is insufficient: a live HUP-immune child may still owe session cleanup.
// Existing cleanup obligations and captured identities are never cleared.
// origin: hot-update S4 internal review rv-auth RV-AUTH-1
func RevokeTerminal(a *Actor, reason string) error {
	return revokeActor(a, reason, true)
}

func revokeActor(a *Actor, reason string, terminal bool) error {
	if reason == "" {
		reason = "revoked"
	}
	active, completed, err := markRevoked(a, reason, terminal)
	if err != nil || completed || !active {
		return err
	}
	deadline := time.Now().Add(upgradebudget.LocalFreezeWindow)
	if err := a.takeControl(deadline); err != nil {
		return err
	}
	defer func() { a.control <- struct{}{} }()
	a.mu.Lock()
	active = a.phase == phaseActive
	a.mu.Unlock()
	if !active {
		return nil
	}
	return finishRevocation(a, reason, deadline)
}

// markRevoked publishes irreversible responsibility before waiting for any
// borrower. Only an already completed terminal may avoid rearming cleanup.
func markRevoked(a *Actor, reason string, terminal bool) (active, completed bool, err error) {
	a.mu.Lock()
	if a.phase == phaseClosed {
		a.mu.Unlock()
		return false, false, os.ErrClosed
	}
	// The runtime's terminal result and the actor's discharged census must
	// agree. Neither a closed master nor a missing Manager row proves this.
	// origin: hot-update S4 internal review rv-auth RV-AUTH-1
	completed = terminal && a.state.MasterClosed && !a.state.CleanupPending
	if a.state.Revoked == "" {
		a.state.Revoked = reason
		a.state.CleanupPending = !completed
	}
	a.abortInputLocked(revokedInputReason(reason))
	a.pauseLocked(&a.write)
	closed := a.state.MasterClosed
	active = a.phase == phaseActive
	a.mu.Unlock()
	if !closed && active {
		_ = a.cfg.Master.SetWriteDeadline(time.Now())
	}
	return active, completed, nil
}

// finishRevocation runs only with the control token held. Control expiry uses
// the same cleanup path without recursively acquiring its own token.
func finishRevocation(a *Actor, reason string, deadline time.Time) error {
	if err := a.pause(deadline); err != nil {
		return err
	}
	err := a.cleanup()
	a.mu.Lock()
	if a.source == sourceReading && !a.state.MasterClosed {
		a.state.OutputError = "pty output aborted: " + reason
		a.source = sourceDiscarding
	}
	a.mu.Unlock()
	a.resume()
	if err != nil {
		return err
	}
	// Expired authority enters Revoke on every Tick, so this path must also
	// discharge the reap hold once its final cleanup census settles.
	// origin: hot-update S4 plan §6.2 / T16
	return a.releaseReap()
}

// cleanup performs one bounded session census per call and preserves captured
// identities across cuts. A later tick finishes members that were still alive
// after SIGKILL. Actively escaped sessions are outside the S4 contract.
func (a *Actor) cleanup() error {
	a.mu.Lock()
	revoked := a.state.Revoked
	pending := a.state.CleanupPending
	prior := append([]workload.SessionMember(nil), a.state.CleanupMembers...)
	a.mu.Unlock()
	if revoked == "" || !pending {
		return nil
	}
	if a.cfg.Manager.Frozen() {
		return workload.ErrFrozen
	}
	row, ok := a.cfg.Manager.Lookup(a.cfg.PID)
	if !ok {
		return workload.ErrUnknown
	}
	members, err := workload.ScanSession(row.Identity)
	if err != nil {
		return err
	}
	seen := make(map[int]bool)
	for _, m := range members {
		seen[m.Identity.OSPID] = true
	}
	for _, m := range prior {
		if !seen[m.Identity.OSPID] {
			members = append(members, m)
			seen[m.Identity.OSPID] = true
		}
	}
	if len(members) > 4096 {
		return fmt.Errorf("ptysession: cleanup census exceeds image bound")
	}
	a.mu.Lock()
	a.state.CleanupMembers = members
	// One empty census is insufficient while a preceding batch may still fork.
	a.state.CleanupPending = true
	a.mu.Unlock()
	var result error
	for _, m := range members {
		if err := workload.SignalIdentity(m, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) && !errors.Is(err, workload.ErrExited) {
			result = errors.Join(result, err)
		}
	}
	// Retain only currently observable members. Captured identities whose signal
	// failed remain responsibility until a later verified census can settle them.
	if result == nil {
		remaining, err := workload.ScanSession(row.Identity)
		if err != nil {
			return err
		}
		if len(remaining) > 4096 {
			return fmt.Errorf("ptysession: cleanup census exceeds image bound")
		}
		a.mu.Lock()
		a.state.CleanupMembers = remaining
		// Completion requires two consecutive empty censuses. Until then the
		// leader's zombie pins the SID and makes the next census verifiable.
		// origin: hot-update S4 plan §6.2 / T16
		a.state.CleanupPending = len(members) != 0 || len(remaining) != 0
		a.mu.Unlock()
	}
	return result
}

// Runtime reasons describe the authority event; input diagnostics use the
// frozen protocol vocabulary instead of copying arbitrary administrative text.
func revokedInputReason(reason string) string {
	if reason == "authority expired" || reason == "authority_expired" {
		return "authority_expired"
	}
	return "revoked"
}
