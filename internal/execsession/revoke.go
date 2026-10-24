// revoke.go — session-authority revocation and the abandoned-terminal sweep.
//
// Both are ownership decisions the runtime makes without a client: Revoke
// answers the broker's session_destroyed (the session that authorized every
// task here no longer exists), Sweep bounds how long a terminal nobody
// collects may keep its replay budget. Neither touches a live pump's bytes:
// revocation kills the owned process group and lets the ordinary
// terminal-draining path record the result; the sweep only ever retires a
// task whose child has already ended.
// origin: docs/reviews/hot-update-reliable-exec-plan.md §3.5 / §3.6.
package execsession

import (
	"errors"
	"fmt"
	"syscall"

	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/ptysession"
	"github.com/LinZiyang666/tether/internal/workload"
)

// Revoke SIGKILLs every task's owned process group (exec children are
// Setpgid leaders), marks each task revoked, and records the revocation so a
// CREATE whose lease predates it is refused (a lease issued after a later
// re-creation of the session is not). Terminal tasks stay settleable — their
// result already exists — but nothing can be revived. It returns how many
// live tasks were revoked: each is killed here, or by start() the moment its
// child is adopted (see below). Close drops the table; the S2 image exports
// it. The revocation instant comes from the runtime's own clock — the one
// that issued every lease it will be compared against (S1 review P6) — and a
// clock failure is returned, never silently turned into "nothing to revoke".
//
// Two shapes the marking alone does not cover (S1 internal review P1/P2):
//   - a task whose startup owner is still inside Start has no adopted child
//     yet, so the Signal here finds nothing; start() re-reads t.revoked
//     after adoption and kills the child there (and refuses to spawn at all
//     when the mark landed before Start);
//   - a task whose output log is full has a pump waiting for an ACK that no
//     client will ever send; the dead child's EOF would never be read and
//     the task never reach a terminal. observe therefore aborts a revoked
//     task's stalled streams (ended at their current End, bytes retained)
//     the moment they stall — a pipe that still drains to EOF keeps its true
//     FIN — so the child is reaped and finish records the "revoked:" result.
//
// origin: hot-update S4 plan §6.1 / T20 / T21
func (r *Runtime) Revoke(sid, reason string) (killed int, err error) {
	if reason == "" {
		reason = "revoked"
	}
	reason = boundedError(reason)
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return 0, nil
	}
	// Publish responsibility BEFORE consulting the fallible clock. Unknown
	// stays sticky: a later successful clock cannot prove when authority ended.
	previous, existed := r.revocations[sid]
	generation := previous.generation + 1
	r.revocations[sid] = revocation{generation: generation, atBootNS: max(previous.atBootNS, r.lastBootNS), reason: reason, clockUnknown: true}
	var tasks []*task
	for _, t := range r.tasks {
		if t.handle.Lease.SID != sid {
			continue
		}
		if t.revoked == "" && t.terminalAt == 0 {
			killed++
		}
		t.revoked = reason
		// Finished actors have no borrowers or cleanup transaction to resume.
		// Their metadata must also reach a local thaw after a frozen revoke.
		// origin: hot-update S4 internal review rv-auth RV-AUTH-1
		if !r.frozen || r.activating || t.terminalAt != 0 {
			tasks = append(tasks, t)
		}
	}
	r.ioActive++
	r.mu.Unlock()
	now, clockErr := r.now()
	if clockErr == nil && now <= 0 {
		clockErr = errors.New("boot clock returned a nonpositive instant")
	}
	r.mu.Lock()
	rev := r.revocations[sid]
	// A concurrent Revoke may have recorded a newer failed clock while this
	// read was in flight. That unknown boundary must never be cleared by us.
	if rev.generation == generation && clockErr == nil && now > 0 && (!existed || !previous.clockUnknown) {
		rev.atBootNS = max(previous.atBootNS, now)
		rev.clockUnknown = false
		r.revocations[sid] = rev
	}
	r.mu.Unlock()
	for _, t := range tasks {
		// The observer transfers the terminal and retires its Manager row under
		// this same token. Choose terminal revocation only after that transfer,
		// or a live cleanup census can race Forget and report unknown process.
		// origin: hot-update S4 plan §6.2 / T34; PRODUCT-R4 FX-A
		t.opMu.Lock()
		r.mu.Lock()
		actor, terminal := t.pty, t.terminalAt != 0
		r.mu.Unlock()
		if actor != nil {
			// Wait can finish before the observer acquires opMu to publish the
			// result. Its retained exit row is also completion evidence: a census
			// must not inspect the already reaped (and possibly reused) leader.
			// Closed master alone is never enough to take this path.
			// origin: hot-update S4 plan §6.2 / T34; PRODUCT-R4 FX-A
			if !terminal {
				row, ok := r.cfg.Manager.Lookup(t.pid)
				terminal = ok && row.Exit != nil
			}
			// A retained terminal has already released its reap hold and retired
			// its Manager row. Record the authority event without inventing a new
			// cleanup obligation; live actors still require a verified census.
			// origin: hot-update S4 internal review rv-auth RV-AUTH-1
			if terminal {
				err = errors.Join(err, ptysession.RevokeTerminal(actor, reason))
			} else {
				err = errors.Join(err, actor.Revoke(reason))
			}
		} else if !terminal {
			// A starting task is marked even if the Manager has no child yet. Its
			// startup owner checks that mark again after adoption.
			signalErr := r.cfg.Manager.Signal(t.pid, syscall.SIGKILL)
			if signalErr != nil && !errors.Is(signalErr, workload.ErrUnknown) && !errors.Is(signalErr, workload.ErrExited) {
				err = errors.Join(err, signalErr)
			}
		}
		t.opMu.Unlock()
	}
	r.endIO()
	if clockErr != nil {
		err = errors.Join(err, fmt.Errorf("exec session: revoke %s: clock: %w", sid, clockErr))
	}
	return killed, err
}

// abortStalledRevokedLocked ends every stream of a revoked task whose pump
// is waiting for replay capacity: nobody will ACK a revoked task, so that
// wait is forever. A stream still draining (capacity left, no EOF yet) is
// left to reach its true EOF. Called under r.mu from observe's tick.
func (r *Runtime) abortStalledRevokedLocked(t *task) {
	for _, output := range t.output {
		if output == nil {
			continue
		}
		state := output.Snapshot()
		if state.Replay.FIN || state.ReadError != "" {
			continue
		}
		if state.Replay.End-state.Replay.Base >= uint64(state.Replay.Capacity) {
			output.Abort("revoked: " + t.revoked)
		}
	}
}

// SweepResult counts what one Sweep reclaimed: Discarded terminal outputs
// (now tombstones), Expired tombstones (settled and collected) and Truncated
// running streams (the running-abandoned bound, D11).
type SweepResult struct {
	Discarded, Expired, Truncated int
}

// Sweep releases the replay logs of terminal tasks nobody collected once
// both their creation lease and AbandonedTerminalGrace since the last client
// touch have passed, keeping a tombstone so the terminal stays queryable and
// a client that already holds every byte can still settle. Tombstones older
// than DiscardTombstoneTTL are settled and collected; revocations older than
// the longest lease they could invalidate are forgotten. It also applies the
// running-abandoned bound: a live task nobody touched for
// AbandonedRunningGrace after its lease expired, whose output log is full,
// has its stalled stream truncated (workload.Output.Discard) so the child
// can go on — the retained 8 MiB stay readable, the truncation is reported
// as an aborted stream to any client that comes back, the process is never
// killed and no ACK is fabricated (S2 plan D11 / §22.4 item 8). It is
// driven from create and from the agent's heartbeat tick, so an idle node
// is bounded too. Like Revoke it reads the runtime's own clock and reports
// its failure; under the image fence it does nothing (a sweep would retire
// tasks whose logs are already exported).
func (r *Runtime) Sweep() (SweepResult, error) {
	now, err := r.now()
	if err != nil {
		return SweepResult{}, fmt.Errorf("exec session: sweep: clock: %w", err)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || r.frozen || r.freezing || r.activating || r.ioActive != 0 {
		return SweepResult{}, nil
	}
	return r.sweepLocked(now), nil
}

func (r *Runtime) sweepLocked(now int64) (res SweepResult) {
	if r.ioActive != 0 {
		return res
	}
	grace := AbandonedTerminalGrace.Nanoseconds()
	runningGrace := AbandonedRunningGrace.Nanoseconds()
	for pid, t := range r.tasks {
		if t.terminalAt == 0 && t.revoked == "" && t.owned &&
			now >= t.handle.Lease.ExpiresBootNS && now >= t.lastTouch+runningGrace {
			for _, output := range t.output {
				if output == nil {
					continue
				}
				state := output.Snapshot()
				if state.Replay.FIN || state.ReadError != "" ||
					state.Replay.End-state.Replay.Base < uint64(state.Replay.Capacity) {
					continue // draining normally, ended, or not actually stalled
				}
				output.Discard("abandoned: no client acknowledged this stream for " +
					AbandonedRunningGrace.String() + " and its log is full; later output is dropped")
				res.Truncated++
			}
		}
		select {
		case <-t.done:
		default:
			continue // the startup owner has not published; nothing is terminal yet
		}
		if t.terminalAt == 0 || t.lostTerminal {
			continue
		}
		if now < t.handle.Lease.ExpiresBootNS || now < t.lastTouch+grace || now < t.terminalAt+grace {
			continue
		}
		record, err := r.registry.Lookup(scopeOf(t.handle), t.handle)
		if err != nil || record.Terminal == nil || record.Phase == "settled" {
			continue
		}
		ts := &tombstone{kind: t.kind, scope: scopeOf(t.handle), handle: t.handle, terminal: *record.Terminal, atBootNS: now}
		for i := range 2 {
			if t.logs[i+1] == nil {
				ts.finalFIN[i] = true // an unallocated log is an empty true EOF
				continue
			}
			state := t.logs[i+1].Snapshot()
			ts.finalEnd[i] = state.End
			ts.finalFIN[i] = state.FIN
		}
		if t.kind == proto.ProcessKindPTY {
			ts.finalFIN[1] = true
			if t.pty != nil {
				input := t.pty.InputStatus()
				ts.pty = &ptysession.Tombstone{InputACK: input.Applied, InputAccepted: input.Accepted, InputFIN: input.FIN,
					InputEnded: input.Ended, InputError: input.Error, ControlSeq: t.control.Seq, ControlDigest: t.control.Digest, ControlError: t.control.Error}
			} else {
				ts.pty = ptyTombstone(*t.ptyState)
			}
		}
		r.retireLocked(t)
		r.discarded[pid] = ts
		res.Discarded++
	}
	ttl := DiscardTombstoneTTL.Nanoseconds()
	for pid, ts := range r.discarded {
		if now < ts.atBootNS+ttl {
			continue
		}
		// Settling under the tombstone's own scope is the runtime's decision:
		// the client abandoned the result long ago; a late retry gets ErrExpired
		// once Collect has reclaimed the lease.
		_ = r.registry.Settle(ts.scope, ts.handle, ts.terminal.ID)
		delete(r.discarded, pid)
		res.Expired++
	}
	r.registry.Collect()
	horizon := procrequest.MaxTTL.Nanoseconds()
	for sid, rev := range r.revocations {
		if !rev.clockUnknown && now >= rev.atBootNS && now-rev.atBootNS >= horizon {
			delete(r.revocations, sid)
		}
	}
	return res
}

func scopeOf(h proto.ProcessHandle) procrequest.Scope {
	l := h.Lease
	return procrequest.Scope{SID: l.SID, NID: l.NID, ActorFP: l.ActorFP, ClientLineage: l.ClientLineage}
}
