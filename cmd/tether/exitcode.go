package main

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/LinZiyang666/tether/internal/infracli"
	"github.com/nats-io/nats.go"
)

// exitcode.go (B2 item 3) — the process exit-code taxonomy SSOT.
//
// Today main() exits 1 on ANY error, so "broker unreachable" / "bad arg" / "not_owner" / a
// malformed reply all collapse to exit 1 — a monitoring/converge script cannot tell them apart.
// This defines a small, sysexits-style taxonomy and a single central classifier
// (classifyExit, called only from the main() sink).
//
// Reserved ranges that DO NOT come from this taxonomy (documented so the values never collide):
//
//   - 0..3 are the `cluster status` HEALTH codes, emitted ONLY by that command (it os.Exit()s
//     directly with rep.ExitCode and never reaches the main sink).
//
//   - exec/run pass the REMOTE process's exit code through (os.Exit(chunk.ExitCode), any value
//     0..255) when the remote process ACTUALLY RAN. The disambiguator there is WHICH command you
//     ran, not the value.
//
//     BUT NOT WHEN THE BROKER REFUSED THEM — origin: prerelease audit round 2, C8/J6. This
//     paragraph used to end "so 77 == permission is scoped to broker-RPC commands", and that
//     stopped being true when execFailureMessage started routing exec's refusals through
//     classifyExit so exec and run would agree. An exec that never reached a process now exits
//     64/69/75/77 from THIS taxonomy. The two cases are still distinguishable, just not by the
//     command: a passed-through code is a code the remote process chose, and a classified one
//     arrives with a `exec: ...` message on stderr naming the broker's refusal.
//
// Compatibility: 0 still means success, so existing `$? != 0` scripts are unaffected; only the
// SPECIFIC nonzero value becomes more informative.
const (
	exitOK          = 0  // success
	exitUsage       = 64 // EX_USAGE: bad arg / missing flag / mutually-exclusive
	exitUnavailable = 69 // EX_UNAVAILABLE: broker/NATS unreachable, no-responder, admin socket down
	exitInternal    = 70 // EX_SOFTWARE: malformed reply / decode error / UNCLASSIFIED (default)
	// exitUnsettled (hot-update S1) is EX_IOERR reused for one meaning: the
	// remote command was ACCEPTED (its create reached the transport) but the
	// result is not in hand — a stream aborted, the ctl gave up waiting, the
	// owning agent instance changed, the agent went OFFLINE, the session was
	// deleted. The command may have run with side effects, so usage §9.13
	// lists 74 as TERMINAL: a rerun mints a new RequestID and executes again.
	// Reliable exec uses it as a presentation-layer class after acceptance.
	// Transfer continuation also maps explicit unresolved receipt/commit/history
	// wire codes to 74: those refusals retain already accepted responsibility.
	// origin: hot-update S7 plan §7 / T34. origin: docs/reviews/hot-update-reliable-exec-plan.md §1.1.
	exitUnsettled = 74 // EX_IOERR: accepted, result unknown — never retry blindly
	exitTransient = 75 // EX_TEMPFAIL: positively-identified transient (retry-later)
	exitNoPerm    = 77 // EX_NOPERM: not_owner / not_a_member / not_leader-on-mutate
)

// ExitError keeps the central CLI class while presentation lives in the leaf.
type ExitError = infracli.ExitError

// wireCodeOf returns the structurally-carried wire code of err, or "" when err carries none.
func wireCodeOf(err error) string {
	var ee *ExitError
	if errors.As(err, &ee) {
		return ee.Code
	}
	return ""
}

// errorIsQuiet reports whether the terminal error opted out of the main sink's "error: ..." print
// (an *ExitError with Quiet set). Used by the main sink so a machine-output command's non-zero exit
// does not append prose to its own stdout stream under a `2>&1` merge.
func errorIsQuiet(err error) bool {
	var ee *ExitError
	return errors.As(err, &ee) && ee.Quiet
}

// usageErr/unavailErr/permErr wrap an error with an explicit class at the SOURCE (positive
// evidence). The classifier never string-sniffs prose for a class — that would make a reworded
// message silently change a script's exit code.
func usageErr(format string, a ...any) error {
	return &ExitError{Class: exitUsage, Err: fmt.Errorf(format, a...)}
}
func unavailErr(format string, a ...any) error {
	return &ExitError{Class: exitUnavailable, Err: fmt.Errorf(format, a...)}
}
func permErr(format string, a ...any) error {
	return &ExitError{Class: exitNoPerm, Err: fmt.Errorf(format, a...)}
}

// classifyExit maps a terminal error to a process exit code (the ONLY caller is the main sink).
// Precedence: an explicit *ExitError class (positive evidence) wins; then a small set of
// well-known sentinels; otherwise the UNCLASSIFIED default is 70 (a tether-side gap, never the
// old exit-1 that collided with everything).
func classifyExit(err error) int {
	if err == nil {
		return exitOK
	}
	var ee *ExitError
	if errors.As(err, &ee) {
		return ee.Class
	}
	// Audit UX-MAJOR-2: "no active session" is a TERMINAL login-required precondition (checked
	// before any NATS dial), not a tether bug — it must be exit 64 (usage) so a robust-retry
	// wrapper doesn't retry it forever. The 15 sites raise it as a plain error; classify by the
	// stable prefix here rather than wrapping each call site.
	if strings.HasPrefix(err.Error(), "no active session") {
		return exitUsage
	}
	if errors.Is(err, nats.ErrNoResponders) {
		return exitUnavailable
	}
	// Both the context-deadline (RequestWithContext) and the nats client timeout (nc.Request)
	// mean "broker reachable but slow" → retryable.
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) || errors.Is(err, nats.ErrTimeout) {
		return exitTransient
	}
	return exitInternal
}
