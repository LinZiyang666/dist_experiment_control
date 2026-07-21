package broker

// runtime_introspect.go (R13) — the broker's process self-introspection for the OpRuntime admin
// verb. It answers the one question the fleet's leak/crash incidents could not: "is this LIVE
// broker leaking goroutines / fds, or has a reconciler stalled?" — without a debugger, a restart,
// or a pprof HTTP surface.
//
// WHY NO pprof (the judgment the roadmap §2 asked for)
// ----------------------------------------------------
// net/http/pprof is deliberately NOT introduced. The operational need is "spot a leak on a live
// broker", which the COUNTS below serve completely: a leak shows as a monotonically climbing
// goroutine or fd count across successive `admin runtime` polls, already gated behind the root-only
// 0600 admin socket. A standing pprof endpoint would instead add (a) attack surface — full heap /
// goroutine-stack dumps and a CPU-profile DoS vector, exposing far more than counts; and (b) binary
// size + a listener to secure. Deep stack-level forensics is a rare, deliberate, offline action, not
// a permanent HTTP surface. So: counts over the existing socket, no pprof.
//
// WHY NumGoroutine AND NOT /proc Threads
// --------------------------------------
// Goroutines is runtime.NumGoroutine() — the Go scheduler's live count. The OS thread count
// (Threads) is a genuinely different quantity: 10k parked leaked goroutines add ZERO OS threads, so
// using Threads as a goroutine proxy would report a flat count while the process bleeds goroutines.
// Both are reported, from their own sources, so they can be seen to diverge.

import (
	"bufio"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"

	"github.com/LinZiyang666/tether/internal/adminsock"
)

// runtimeSnapshot builds the OpRuntime report from THIS process's live runtime plus the R7
// reconcile registry. It allocates only the returned slices and closes every fd it opens — the leak
// detector must never itself leak. Safe to call from the admin goroutine concurrently with the Run
// loop: NumGoroutine/pprof are process-global, the /proc reads are stateless, bootAt is written once
// before the admin socket starts (happens-before), and reconcilers.status() is internally locked.
func (b *Broker) runtimeSnapshot() *adminsock.RuntimeReport {
	rep := &adminsock.RuntimeReport{
		Schema:        "admin_runtime",
		SchemaVersion: 1,
		Goroutines:    runtime.NumGoroutine(),               // in-process TRUTH, never the Threads proxy
		Threads:       pprof.Lookup("threadcreate").Count(), // OS thread (M) count — a DIFFERENT quantity
		OpenFDs:       openFDCount(),                        // -1 if not measurable (non-Linux)
		RSSBytes:      residentSetBytes(),                   // -1 if not measurable (non-Linux)
	}
	if !b.bootAt.IsZero() {
		rep.UptimeSeconds = b.cfg.Now().Sub(b.bootAt).Seconds()
	}
	// Per-reconciler last-tick from the R7a registry (R13 only CONSUMES lastTick; it does not touch
	// the scheduler). Nil registry (an admin call landing in the sliver of Run before the registry is
	// wired) yields an empty list rather than a panic.
	if b.reconcilers != nil {
		for _, st := range b.reconcilers.status() {
			rep.Reconcilers = append(rep.Reconcilers, adminsock.ReconcilerTick{
				Name:       st.Name,
				IntervalMS: st.Interval.Milliseconds(),
				LeaderOnly: st.LeaderOnly,
				LastTick:   st.LastTick,
				Runs:       st.Runs,
				Skips:      st.Skips,
				LastErr:    st.LastErr,
			})
		}
	}
	return rep
}

// openFDCount returns the number of open file descriptors for this process by counting entries in
// /proc/self/fd. Returns -1 when that directory cannot be read (non-Linux, or /proc not mounted) so
// the caller reports "unavailable" rather than a fabricated 0. The directory handle is CLOSED before
// returning — a leak probe that leaked an fd on every call would be its own worst bug.
func openFDCount() int {
	d, err := os.Open("/proc/self/fd")
	if err != nil {
		return -1
	}
	defer func() { _ = d.Close() }()
	names, err := d.Readdirnames(-1)
	if err != nil {
		return -1
	}
	// Readdirnames includes the fd for the open directory handle itself; subtract it so the count is
	// the process's steady-state open fds, not "+1 while this probe runs".
	if n := len(names) - 1; n >= 0 {
		return n
	}
	return len(names)
}

// residentSetBytes returns the process resident-set size in bytes, parsed from /proc/self/statm
// (field 2 = resident pages) times the page size. Returns -1 when not measurable. This is the actual
// RSS, not a proxy for anything else.
func residentSetBytes() int64 {
	f, err := os.Open("/proc/self/statm")
	if err != nil {
		return -1
	}
	defer func() { _ = f.Close() }()
	sc := bufio.NewScanner(f)
	if !sc.Scan() {
		return -1
	}
	// Format: "size resident shared text lib data dt" (all in pages). We want field index 1.
	fields := splitFields(sc.Text())
	if len(fields) < 2 {
		return -1
	}
	residentPages, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil {
		return -1
	}
	return residentPages * int64(os.Getpagesize())
}

// splitFields splits on ASCII spaces without allocating a regexp; statm is a single space-separated
// line so this is sufficient (strings.Fields would also work — kept explicit to avoid pulling a
// larger import for one call).
func splitFields(s string) []string {
	var out []string
	start := -1
	for i := 0; i < len(s); i++ {
		if s[i] == ' ' {
			if start >= 0 {
				out = append(out, s[start:i])
				start = -1
			}
		} else if start < 0 {
			start = i
		}
	}
	if start >= 0 {
		out = append(out, s[start:])
	}
	return out
}
