package cluster

import (
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// sqlbake is the SINGLE audited "leader value -> SQL literal" surface for the D2
// op set (architecture §3.3/§3.4 + docs/reviews/d2-plan.md §2). The leader bakes
// every value that feeds a PK / unique index / fence / timestamp into the
// Statement.SQL string itself, so the raft-log entry is byte-identical across
// replicas and a replica's Apply needs no out-of-band parameters.
//
// These helpers are EXPORTED because the per-op Plan* functions live in the
// mutator packages (internal/{port,proc,node,session,agentprov}) and render
// their own Command (internal/cluster must NOT import those packages — see
// d2-plan §3). They remain the ONLY sanctioned value->SQL path; an AST lint
// forbids fmt.Sprintf-of-SQL outside this surface.
//
// Statement.Args []any is FORBIDDEN for real D2 ops: encoding/json of []any is
// not round-trip type-stable — an int64 past 2^53 loses precision
// (1750512345123456789 -> 1750512345123456768) and []byte becomes base64. Since
// processes.start_time_ticks is an int64 compared for EXACT equality by the
// PID-reuse defense (proc.pidReused), a corrupted value silently breaks security
// and diverges replicas. Baking via LitInt keeps the value exact.

// LitText renders a Go string as a single-quoted SQL string literal. It is the
// ONLY text->literal path: it doubles embedded single quotes (SQL escaping) and
// REJECTS an embedded NUL — SQLite truncates a C-string literal at a NUL, so an
// un-rejected NUL is both a data-corruption and an injection/truncation vector.
// Returning an error lets the leader-side Plan fail BEFORE proposing a malformed
// entry.
func LitText(s string) (string, error) {
	if strings.IndexByte(s, 0) >= 0 {
		return "", fmt.Errorf("cluster: LitText: embedded NUL byte in %q", s)
	}
	// Reject invalid UTF-8 FAIL-CLOSED: the Command travels the raft log as JSON
	// (cmd.encode = json.Marshal), and json.Marshal silently replaces an invalid
	// UTF-8 byte with U+FFFD — a directly-baked text column would be SILENTLY
	// corrupted on the wire (encode -> raft -> decode), diverging replicas. The
	// live mutators bind text as a parameter (raw bytes preserved); rather than
	// corrupt, we refuse a non-UTF-8 value before proposing. (Stage C blocker.)
	if !utf8.ValidString(s) {
		return "", fmt.Errorf("cluster: LitText: invalid UTF-8 in %q (raft-log JSON would corrupt it)", s)
	}
	return "'" + strings.ReplaceAll(s, "'", "''") + "'", nil
}

// MustLitText is LitText for inputs already known NUL-free (constants/enums). It
// panics on a NUL — a programming error, never reachable from external data.
func MustLitText(s string) string {
	out, err := LitText(s)
	if err != nil {
		panic(err)
	}
	return out
}

// LitInt renders an int64 as a bare SQL integer literal — exact, never via a
// float. Use for ports, exit codes, start_time_ticks, boolean 0/1 columns, etc.
func LitInt(n int64) string {
	return strconv.FormatInt(n, 10)
}

// LitTime renders a timestamp as a single-quoted SQL literal BYTE-IDENTICAL to
// how modernc.org/sqlite v1.50.0 stores a bound time.Time — which is EXACTLY
// Go's time.Time.String() of the value as given, faithful to its zone and even
// its monotonic reading. Empirically (TestLitTimeMatchesBoundParam, a BLOCKING
// gate):
//
//	UTC, no monotonic  -> '2026-06-21 13:14:15.5 +0000 UTC'
//	+0800              -> '2026-06-21 21:14:15.5 +0800 CST'   (NOT converted to UTC)
//	time.Now()         -> '... -0500 CDT m=+0.001080637'      (monotonic suffix kept)
//
// This is NOT RFC3339. CRITICAL: LitTime does NOT force .UTC(); the CALLER MUST
// pass the exact same time.Time the live mutator binds — i.e. apply .UTC() iff
// the live mutator does (port/proc/node/agentprov bind now.UTC(); session.* bind
// the raw now, monotonic and all). Passing a differently-zoned time silently
// breaks §13.2/DIFF-1 equivalence and the live lexical OFFLINE->REVOKE compare in
// port.ListAllocatedForOfflineNodes. Driver-coupled: a modernc bump must
// re-verify (architecture §3.4 D2 amendment).
func LitTime(t time.Time) string {
	return "'" + t.String() + "'"
}

// LitNull renders SQL NULL (for nullable columns whose value is absent).
func LitNull() string { return "NULL" }

// LitTextAll renders each string via LitText, returning the first error (e.g. an
// embedded NUL). Convenience for the many text columns a Plan* func bakes.
func LitTextAll(ss ...string) ([]string, error) {
	out := make([]string, len(ss))
	for i, s := range ss {
		v, err := LitText(s)
		if err != nil {
			return nil, err
		}
		out[i] = v
	}
	return out, nil
}
