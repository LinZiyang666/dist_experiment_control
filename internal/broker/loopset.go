package broker

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// loopSet starts long-running cluster loops and joins them at shutdown, keeping
// its own count.
//
// Batch-A A5. The count used to be a hand-written constant sitting next to the
// `go` statements:
//
//	loopCount := 4
//	if poster != nil {
//	    loopCount = 5
//	}
//
// with the join reading `cap(b.cl.loopDone)`. Nothing tied the number to the
// number of goroutines actually started, and no test referenced loopCount at
// all — adding a sixth loop and forgetting to bump it was a silent edit.
//
// The two failure directions are not symmetric:
//
//   - too HIGH: shutdown waits 10s per phantom slot. Noisy, but visible.
//   - too LOW: the join returns while a loop is still running. clusterShutdownOrdered
//     then proceeds to step 3 and on to nc.Drain while that loop may still be
//     doing JetStream/raft I/O — reproducing precisely the "publish-after-Drain
//     silently loses an audit record" hazard that clusterwrite.go:132-137
//     documents, and which that same comment notes the goroutine leak gate
//     cannot catch (the goroutine does exit; it just exits too late).
//
// Counting where the goroutines are actually created removes the class.
type loopSet struct {
	mu      sync.Mutex
	names   []string
	done    chan struct{}
	started bool

	// stats holds per-loop liveness for RuntimeReport. These loops were
	// previously invisible: nothing reported whether the topology reconciler had
	// ticked in the last minute or died quietly on its first iteration.
	stats map[string]*loopStat
}

// loopStat holds what the set can actually observe about a loop, which is
// exactly one thing: when it was started.
//
// Batch-A review F-03: this originally also carried Runs and LastErr. Runs was
// incremented once at startup and was therefore permanently 1; LastErr had a
// reader in RuntimeReport and NO writer anywhere in the repo, so it reported the
// empty string forever. Publishing a field an operator reads as "this loop's
// last error" while nothing can ever set it is worse than publishing nothing —
// an empty LastErr looks like "no errors".
//
// The loops own their iteration cadence internally, so per-iteration liveness is
// not observable from here. It comes from reconcileRegistry.status() for the
// passes that registry manages; moving these four onto it is B7's job.
type loopStat struct {
	StartedAt time.Time
}

func newLoopSet() *loopSet {
	// done is sized once here rather than lazily inside Go. Batch-A review F-01:
	// the lazy `if l.done == nil { l.done = make(...) }` sat OUTSIDE the mutex
	// while every other field was guarded, so two concurrent Go calls could race
	// on it. The existing tests all called Go serially, which is why -race stayed
	// green — a false negative, not a proof.
	return &loopSet{
		stats: map[string]*loopStat{},
		done:  make(chan struct{}, loopSetCapacity),
	}
}

// loopSetCapacity bounds how many loops one set can hold. Go panics past it
// rather than blocking a loop's exit forever on a full channel.
const loopSetCapacity = 64

// Go registers and starts one loop. It must be called before Join.
func (l *loopSet) Go(ctx context.Context, name string, fn func(context.Context)) {
	l.mu.Lock()
	if l.started {
		l.mu.Unlock()
		panic("loopSet.Go after Join: a loop started during shutdown would never be joined")
	}
	if len(l.names) >= loopSetCapacity {
		l.mu.Unlock()
		panic("loopSet: more loops than capacity; raise loopSetCapacity")
	}
	l.names = append(l.names, name)
	if l.stats[name] == nil {
		l.stats[name] = &loopStat{}
	}
	l.mu.Unlock()

	go func() {
		defer func() { l.done <- struct{}{} }()
		l.markStarted(name)
		fn(ctx)
	}()
}

func (l *loopSet) markStarted(name string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if st := l.stats[name]; st != nil {
		st.StartedAt = time.Now()
	}
}

// Join waits for every started loop to exit, allowing `timeout` PER LOOP. It
// names the set that did not come back — the old code could only say "a cluster loop
// did not exit", which does not tell an operator which subsystem is wedged.
func (l *loopSet) Join(timeout time.Duration, logger *slog.Logger) {
	l.mu.Lock()
	n := len(l.names)
	names := append([]string{}, l.names...)
	l.started = true
	l.mu.Unlock()

	// One budget PER LOOP, matching the behaviour this replaced. Returning on the
	// first timeout would abandon every remaining loop — precisely the
	// "join returns while a loop is still publishing" path this type exists to
	// close, just reached from the other direction (batch-A review M1).
	//
	// Worst case is n*timeout, which is the same bound the hand-written loop had.
	// A wedged loop is loud (one warn per loop) rather than silently truncating
	// the join.
	timedOut := 0
	for i := 0; i < n; i++ {
		select {
		case <-l.done:
		case <-time.After(timeout):
			timedOut++
			if logger != nil {
				logger.Warn("broker: cluster loop did not exit within shutdown budget",
					"timeout", timeout, "joined", i, "of", n, "timed_out", timedOut, "loops", names)
			}
		}
	}
}

// Count reports how many loops were started. Used by the tests that pin the
// self-counting property — the whole point of this type — rather than by
// production code, which never needs to ask.
//
//nolint:unused // asserted by loopset_test.go; see the type doc.
func (l *loopSet) Count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.names)
}

// Snapshot returns per-loop liveness for RuntimeReport.
func (l *loopSet) Snapshot() map[string]loopStat {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make(map[string]loopStat, len(l.stats))
	for k, v := range l.stats {
		out[k] = *v
	}
	return out
}
