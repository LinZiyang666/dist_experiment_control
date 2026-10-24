package pty

import (
	"errors"
	"io"
	"syscall"
	"time"
)

// Legacy terminal batching and allocation classification live with the terminal owner.
// origin: hot-update S5 plan §10 / AGENT-SLIM-S5.
const LegacyChunkSize = 4 * 1024
const LegacyFlushInterval = 50 * time.Millisecond

// pumpMasterToBus reads the PTY master and publishes byte chunks to
// outSubj. Returns when EOF is reached (child closed its end). 4KB or
// 50ms whichever first — see architecture C.5.2.
func PumpLegacyOutput(master io.Reader, publish func([]byte)) {
	buf := make([]byte, LegacyChunkSize)
	pending := make([]byte, 0, LegacyChunkSize)
	flushTimer := time.NewTimer(LegacyFlushInterval)
	defer flushTimer.Stop()

	// resetTimer drains the channel if a tick is already queued and
	// arms a fresh LegacyFlushInterval. MUST be called every time the
	// timer fires (even when pending is empty) so the periodic flush
	// keeps ticking. Otherwise time.NewTimer fires once and stops,
	// and any future PTY output that doesn't fill 4KB sits in
	// `pending` until the child closes (operator pain: tmux echo
	// only appears in batches at session end).
	resetTimer := func() {
		if !flushTimer.Stop() {
			select {
			case <-flushTimer.C:
			default:
			}
		}
		flushTimer.Reset(LegacyFlushInterval)
	}

	flush := func() {
		if len(pending) == 0 {
			resetTimer()
			return
		}
		// Make a copy because nats.Publish may queue and reuse our buf.
		out := make([]byte, len(pending))
		copy(out, pending)
		// PER FLUSH, not the conn captured at spawn.
		//
		// origin: prerelease audit round 2, I-F2. L3-F2 moved the INPUT direction to a
		// session-scoped subscription and left the OUTPUT direction publishing on the
		// spawn conn, while a comment at the call site claimed this pump "resolves its
		// conn per flush". After a session rebuild that conn is closed, so the terminal
		// went silent in the direction the user actually watches — and the comment said
		// otherwise.
		publish(out)
		pending = pending[:0]
		resetTimer()
	}

	readCh := make(chan readResult, 1)
	go func() {
		for {
			n, err := master.Read(buf)
			data := append([]byte(nil), buf[:n]...)
			readCh <- readResult{data: data, err: err}
			if err != nil {
				return
			}
		}
	}()

	for {
		select {
		case r := <-readCh:
			if len(r.data) > 0 {
				pending = append(pending, r.data...)
				if len(pending) >= LegacyChunkSize {
					flush()
				}
			}
			if r.err != nil {
				flush()
				return
			}
		case <-flushTimer.C:
			flush()
		}
	}
}

type readResult struct {
	data []byte
	err  error
}

// NormalizedSize returns sane defaults for cols/rows when ctl forgot.
// 80x24 is the conventional dumb-terminal default.
func NormalizedSize(cols, rows int) (int, int) {
	if cols <= 0 {
		cols = 80
	}
	if rows <= 0 {
		rows = 24
	}
	return cols, rows
}

// FailureIsTransient decides which of the two PTY-failure wire codes a pty.Allocate error earns.
//
// origin: line-2 external review M18 / PC-3. This predicate used to be written inline in handleRunReq,
// which made the DECISION untestable while leaving the two code literals testable — and the review
// measured the consequence: replacing the condition with `if false` (the transient branch permanently
// unreachable) left ./internal/agent/ and ./cmd/tether/ both green. The wire-code coverage gate only
// asserts that each code literal EXISTS somewhere, so it could not see that one of them had become
// dead. Extracting the predicate is what gives the criterion itself a test
// (TestPTYFailureTransientClassification).
//
// The two code literals stay written out at the two call sites, unchanged: the coverage gate resolves
// codes statically and a `reason` variable there would need an unresolvedCodeSites exemption, which is an
// exemption avoidable by writing the code out.
//
// EMFILE / ENFILE  the process or the host is out of file descriptors. Self-healing — the next attempt
//
//	after some sessions close will succeed, so pty_alloc_failed is retryable.
//
// anything else    /dev/ptmx is absent, not permitted, or the container forbids it. A property of the
//
//	HOST, not of this instant; pty_unavailable tells automation to stop retrying.
//
// TransientErrnos are the allocation failures that clear on their own as sessions close.
//
// ENOSPC is the one that matters most and was missing. origin: line-2 external review M17 / PC-2 / F2 / D5.
// Opening /dev/ptmx goes ptmx_open -> devpts_new_index, and when the devpts instance is at its limit
// (/proc/sys/kernel/pty/max, minus pty_reserve for non-root, or a `max=` mount option) that returns
// -ENOSPC, not EMFILE. So PTY EXHAUSTION -- the single most likely transient PTY failure on a busy host,
// and the exact thing pty_alloc_failed exists to name -- was landing in the terminal branch and telling
// automation to stop retrying something that clears in seconds.
//
// Measured, not inferred: in a privileged container with devpts mounted `newinstance,max=1`, the second
// open returns `no space left on device` with EMFILE=false ENFILE=false ENOSPC=true. This host's
// /proc/sys/kernel/pty/max is 4096, so an ordinary busy broker can reach it.
//
// ENOMEM and EAGAIN are included on the same principle: both are resource-pressure errnos that a later
// attempt can win. The list is a variable while the two Reason literals at the call site stay written out
// -- the wire-code coverage gate resolves codes statically, and an errno list is not a code.
var TransientErrnos = []syscall.Errno{
	syscall.EMFILE, // this process is out of file descriptors
	syscall.ENFILE, // the host's file table is full
	syscall.ENOSPC, // devpts index exhaustion -- the pty limit, see above
	syscall.ENOMEM, // kernel allocation pressure
	syscall.EAGAIN, // resource temporarily unavailable
}

func FailureIsTransient(err error) bool {
	for _, e := range TransientErrnos {
		if errors.Is(err, e) {
			return true
		}
	}
	return false
}

// kaGraceFloor: the reap window never drops below 3 minutes (h1 plan Q3).
// A laptop lid-close under 3min resumes its keepalives unharmed; longer
// suspends hang the session up — sshd-with-ClientAliveInterval semantics,
// documented in usage.md. For tmux the cost is a reattach.
const LegacyKeepaliveGraceFloor = 3 * time.Minute

// LegacyChunkSize / LegacyFlushInterval bound the master→.out pump. 4KB or
// 50ms whichever comes first — see architecture C.5.2.

// KeepaliveGraceFor derives the reap window from a ctl-advertised keepalive
// interval: 0 = not advertised = never reap; otherwise the interval clamps to
// [1s, 60s] and the grace is max(6×interval, LegacyKeepaliveGraceFloor) — six missed beats
// or three minutes, whichever is longer.
func KeepaliveGraceFor(intervalMS int) time.Duration {
	if intervalMS <= 0 {
		return 0
	}
	iv := time.Duration(intervalMS) * time.Millisecond
	if iv < time.Second {
		iv = time.Second
	}
	if iv > time.Minute {
		iv = time.Minute
	}
	if g := 6 * iv; g > LegacyKeepaliveGraceFloor {
		return g
	}
	return LegacyKeepaliveGraceFloor
}

// shouldReapRun is the PURE reap decision (table-tested): a run is reaped
// only when (armed) ∧ (not already reaped) ∧ (child not already exited —
// recycled-pgid guard) ∧ (keepalive silence exceeds grace) ∧ (the agent's
// conn was verifiably healthy on THIS tick) ∧ (this is the second
// consecutive such tick). Anything less reaps healthy sessions: an
// unhealthy-conn tick proves only that the AGENT is deaf, not that the ctl
// is dead.
func ShouldReapLegacy(now time.Time, grace time.Duration, lastKA time.Time, reaped, waitDone, connProven, probeConfirmed bool) int {
	if grace <= 0 || reaped || waitDone {
		return 0
	}
	if now.Sub(lastKA) <= grace {
		return 0
	}
	if !connProven {
		return 0
	}
	if !probeConfirmed {
		return 1
	}
	return 2
}
