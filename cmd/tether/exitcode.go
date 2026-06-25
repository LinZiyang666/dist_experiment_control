package main

import (
	"context"
	"errors"
	"fmt"
	"strings"

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
//   - 0..3 are the `cluster status` HEALTH codes, emitted ONLY by that command (it os.Exit()s
//     directly with rep.ExitCode and never reaches the main sink).
//   - exec/run pass the REMOTE process's exit code through (os.Exit(chunk.ExitCode), any value
//     0..255) — they also never reach the sink. The disambiguator there is WHICH command you ran,
//     not the value, so "77 == permission" is scoped to broker-RPC commands.
//
// Compatibility: 0 still means success, so existing `$? != 0` scripts are unaffected; only the
// SPECIFIC nonzero value becomes more informative.
const (
	exitOK          = 0  // success
	exitUsage       = 64 // EX_USAGE: bad arg / missing flag / mutually-exclusive
	exitUnavailable = 69 // EX_UNAVAILABLE: broker/NATS unreachable, no-responder, admin socket down
	exitInternal    = 70 // EX_SOFTWARE: malformed reply / decode error / UNCLASSIFIED (default)
	exitTransient   = 75 // EX_TEMPFAIL: positively-identified transient (retry-later)
	exitNoPerm      = 77 // EX_NOPERM: not_owner / not_a_member / not_leader-on-mutate
)

// ExitError carries an explicit exit class through the error chain. The central classifier
// honors it before any heuristic, so a class set at the SOURCE (positive evidence) always wins.
type ExitError struct {
	Class int
	Err   error
}

func (e *ExitError) Error() string { return e.Err.Error() }
func (e *ExitError) Unwrap() error { return e.Err }

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
