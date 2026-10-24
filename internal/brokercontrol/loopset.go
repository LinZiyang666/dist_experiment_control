package brokercontrol

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
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
type LoopSet struct {
	Mu        sync.Mutex
	Names     []string
	Done      chan struct{}
	Started   bool
	Completed int
	Changed   chan struct{}

	// stats holds per-loop liveness for RuntimeReport. These loops were previously invisible: nothing
	// reported whether the topology reconciler had ticked in the last minute or died quietly on its
	// first iteration. Since B7 the second half of that sentence is answerable — see loopStat.
	Stats map[string]*LoopStat
}

// loopStat holds what the set can actually observe about a loop.
//
// Batch-A review F-03: this originally carried Runs and LastErr. Runs was incremented once at startup
// and was therefore permanently 1; LastErr had a reader in RuntimeReport and NO writer anywhere in the
// repo, so it reported the empty string forever. Publishing a field an operator reads as "this loop's
// last error" while nothing can ever set it is worse than publishing nothing — an empty LastErr looks
// like "no errors".
//
// B7 restores per-iteration liveness WITHOUT restoring that defect. The distinction matters: F-03's
// Runs was written once, at startup, so it could not tell a live loop from one that returned on its
// first line. Iters/LastIter are written BY THE LOOP, once per completed iteration, via Beat — so a
// loop that died after one iteration reports Iters=1 and a LastIter that stops advancing, which is
// exactly the signal that was missing.
//
// The loops own their cadence internally (each creates its own ticker), so the set cannot infer
// iterations; it has to be told. Cadence is the loop's DECLARED period and is what makes Iters
// interpretable:
//
//	Cadence != 0  periodic. Iters must grow at roughly 1/Cadence; a stalled loop is one whose
//	              LastIter is older than a few Cadences. Iters == 0 after startup means DEAD.
//	Cadence == 0  event-driven (the alert webhook poster). Iters grows only when work arrives, so
//	              Iters == 0 is NOT evidence of death and must not be read as such.
//
// A periodic loop that forgets to Beat reports Iters=0, i.e. it looks dead rather than looks healthy.
// That is the correct direction for a liveness signal to fail in.
type LoopStat struct {
	StartedAt time.Time
	Cadence   time.Duration
	LastIter  time.Time
	Iters     uint64
}

func NewLoopSet() *LoopSet {
	// done is sized once here rather than lazily inside Go. Batch-A review F-01:
	// the lazy `if l.done == nil { l.done = make(...) }` sat OUTSIDE the mutex
	// while every other field was guarded, so two concurrent Go calls could race
	// on it. The existing tests all called Go serially, which is why -race stayed
	// green — a false negative, not a proof.
	return &LoopSet{
		Stats: map[string]*LoopStat{},
		Done:  make(chan struct{}, LoopSetCapacity),
	}
}

// loopSetCapacity bounds how many loops one set can hold. Go panics past it
// rather than blocking a loop's exit forever on a full channel.
const LoopSetCapacity = 64

// Go registers and starts one loop. It must be called before Join.
func (l *LoopSet) Go(ctx context.Context, name string, fn func(context.Context)) {
	l.Mu.Lock()
	if l.Started {
		l.Mu.Unlock()
		panic("loopSet.Go after Join: a loop started during shutdown would never be joined")
	}
	if len(l.Names) >= LoopSetCapacity {
		l.Mu.Unlock()
		panic("loopSet: more loops than capacity; raise loopSetCapacity")
	}
	l.Names = append(l.Names, name)
	if l.Stats[name] == nil {
		l.Stats[name] = &LoopStat{}
	}
	l.Mu.Unlock()

	go func() {
		defer func() {
			l.Mu.Lock()
			l.Completed++
			if l.Changed != nil {
				close(l.Changed)
				l.Changed = nil
			}
			l.Mu.Unlock()
			l.Done <- struct{}{}
		}()
		l.MarkStarted(name)
		fn(ctx)
	}()
}

func (l *LoopSet) MarkStarted(name string) {
	l.Mu.Lock()
	defer l.Mu.Unlock()
	if st := l.Stats[name]; st != nil {
		st.StartedAt = time.Now()
	}
}

// GoEvery starts a loop that DECLARES a period. It is otherwise identical to Go: the loop still owns
// its own ticker, because these loops each have a shutdown/poll shape the set has no business
// rewriting. What the declaration buys is interpretability — Snapshot's Iters is meaningless without a
// cadence to compare it against, and a periodic loop reporting Iters=0 is only diagnosable as DEAD if
// something recorded that it was supposed to be ticking.
//
// The loop must call Beat(name) once per completed iteration. Not calling it makes the loop look dead,
// never healthy.
func (l *LoopSet) GoEvery(ctx context.Context, name string, cadence time.Duration, fn func(context.Context)) {
	l.Mu.Lock()
	if l.Stats[name] == nil {
		l.Stats[name] = &LoopStat{}
	}
	l.Stats[name].Cadence = cadence
	l.Mu.Unlock()
	l.Go(ctx, name, fn)
}

// Beat records ONE completed iteration of a named loop.
//
// This is the field F-03 deleted, restored with the defect removed. F-03's Runs was written once at
// startup, so "1" meant both "running fine" and "returned on its first line". Beat is written by the
// loop body, so the count and the timestamp both stop advancing exactly when the loop stops — which is
// the question an operator is actually asking when a convergence duty appears wedged.
//
// Unknown names are ignored rather than panicking: a Beat racing a Join during shutdown must not take
// the process down.
func (l *LoopSet) Beat(name string) {
	l.Mu.Lock()
	defer l.Mu.Unlock()
	if st := l.Stats[name]; st != nil {
		st.Iters++
		st.LastIter = time.Now()
	}
}

// Join waits for every started loop to exit, allowing `timeout` PER LOOP. It
// names the set that did not come back — the old code could only say "a cluster loop
// did not exit", which does not tell an operator which subsystem is wedged.
func (l *LoopSet) Join(timeout time.Duration, logger *slog.Logger) {
	l.Mu.Lock()
	n := len(l.Names)
	names := append([]string{}, l.Names...)
	l.Started = true
	l.Mu.Unlock()

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
		case <-l.Done:
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
// (There used to be a //nolint:unused here. nolintlint's allow-unused:false found it dangling: the
// method IS used, from loopset_test.go, so `unused` never reported it and the directive suppressed
// nothing. A directive that suppresses nothing still reads to the next person as "this is knowingly
// unused", which is the opposite of true.)
func (l *LoopSet) Count() int {
	l.Mu.Lock()
	defer l.Mu.Unlock()
	return len(l.Names)
}

// Snapshot returns per-loop liveness for RuntimeReport.
func (l *LoopSet) Snapshot() map[string]LoopStat {
	l.Mu.Lock()
	defer l.Mu.Unlock()
	out := make(map[string]LoopStat, len(l.Stats))
	for k, v := range l.Stats {
		out[k] = *v
	}
	return out
}

// JoinNATSControl waits for the asynchronous drain worker's actual completion.
// It preserves the previously installed callback and bounds terminal cleanup.
// Drain starts an asynchronous nats.go worker. Waiting for its closed callback
// prevents terminal Run return from racing the server/listener cleanup while
// that worker is still flushing admitted replies. Keep the existing callback.
// origin: hot-update S6 plan §3.2 / §12.1 P11
func JoinNATSControl(nc *nats.Conn) error {
	if nc.IsClosed() {
		return nil
	}
	done := make(chan struct{})
	previous := nc.ClosedHandler()
	nc.SetClosedHandler(func(conn *nats.Conn) {
		if previous != nil {
			previous(conn)
		}
		close(done)
	})
	if err := nc.Drain(); err != nil {
		nc.Close()
		return err
	}
	budget := nc.Opts.DrainTimeout + 5*time.Second
	timer := time.NewTimer(budget)
	defer timer.Stop()
	select {
	case <-done:
		return nil
	case <-timer.C:
		nc.Close()
		return fmt.Errorf("broker: NATS drain join deadline expired")
	}
}

// JoinCleanupError preserves the original Run sentinel when cleanup succeeds,
// while retaining both failures when terminal cleanup itself fails.
// errors.Join(err, nil) still wraps err and changes the embedded Broker.Run
// cancellation contract even though no shutdown error occurred.
// origin: hot-update S6 plan §4.2 / T52
func JoinCleanupError(runErr, cleanupErr error) error {
	if cleanupErr == nil {
		return runErr
	}
	return errors.Join(runErr, cleanupErr)
}
