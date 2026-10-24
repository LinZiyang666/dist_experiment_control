// legacy_input.go owns the bounded legacy input/control queue independently
// of agent subscription wiring and keepalive bookkeeping.
// origin: hot-update S4 plan §7.1 / T42.
package pty

import (
	"sync"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
)

// LegacyInputEventLimit bounds controls independently of their byte size.
// legacyPTYInput owns the legacy input/control queue. Bytes remain charged
// while a write is in flight, and zero-byte controls have a separate count
// bound, so neither a blocked slave nor a resize flood grows memory forever.
// origin: hot-update S4 plan §7.1 / T42
const LegacyInputEventLimit = 256

// LegacyInputEvent keeps bytes and zero-byte controls ordered by one writer.
type LegacyInputEvent struct {
	Data      []byte
	Resize    *proto.PtyResizeEvent
	Keepalive bool
}

// LegacyInput serializes each session independently of shared subscriptions.
type LegacyInput struct {
	mu       sync.Mutex
	queue    []LegacyInputEvent
	bytes    int
	stopped  bool
	ready    *sync.Cond
	done     chan struct{}
	fail     func()
	failDone chan struct{}
	sess     *Session
}

// NewLegacyInput starts the session writer; callbacks retain agent bookkeeping.
func NewLegacyInput(sess *Session, keepalive, fail func()) *LegacyInput {
	q := &LegacyInput{sess: sess, done: make(chan struct{}), failDone: make(chan struct{}), fail: fail}
	close(q.failDone) // no failure owner exists until overflow is latched
	q.ready = sync.NewCond(&q.mu)
	go func() {
		defer close(q.done)
		for {
			q.mu.Lock()
			for len(q.queue) == 0 && !q.stopped {
				q.ready.Wait()
			}
			if q.stopped {
				q.mu.Unlock()
				return
			}
			ev := q.queue[0]
			q.queue[0] = LegacyInputEvent{}
			q.queue = q.queue[1:]
			q.mu.Unlock()
			if ev.Resize != nil {
				_ = sess.SetSize(ev.Resize.Cols, ev.Resize.Rows)
			} else if ev.Keepalive {
				keepalive()
			} else {
				data := ev.Data
				for len(data) > 0 {
					n, err := sess.WriteInput(data)
					data = data[n:]
					if err != nil || n == 0 {
						break // source closure is accounted by the legacy lifecycle owner
					}
				}
			}
			q.mu.Lock()
			q.bytes -= len(ev.Data)
			q.mu.Unlock()
		}
	}()
	return q
}

// Enqueue admits one event without waiting for the session writer.
// enqueue never waits for kernel I/O. Failure is latched under the same lock
// as acceptance, so exactly one failure is published and no later input enters.
func (q *LegacyInput) Enqueue(ev LegacyInputEvent) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.stopped {
		return
	}
	if len(ev.Data) > upgradebudget.PTYInputBytes-q.bytes || len(q.queue) >= LegacyInputEventLimit {
		q.stopped = true
		q.queue = nil
		q.failDone = make(chan struct{})
		done := q.failDone
		go func() { defer close(done); q.fail() }()
	} else {
		ev.Data = append([]byte(nil), ev.Data...)
		q.bytes += len(ev.Data)
		q.queue = append(q.queue, ev)
	}
	q.ready.Signal()
}

func (q *LegacyInput) Stop() {
	q.mu.Lock()
	q.stopped = true
	q.queue = nil
	q.ready.Broadcast()
	failDone := q.failDone
	q.mu.Unlock()
	// The output owner has already drained. Closing now wakes a blocked
	// input write without racing the sole output reader or truncating a tail.
	_ = q.sess.Close()
	<-q.done
	<-failDone
}

// Snapshot reports queue pressure without exposing the ownership lock.
// In-flight bytes remain charged even after their event leaves the queue.
func (q *LegacyInput) Snapshot() (bytes, events int, stopped bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.bytes, len(q.queue), q.stopped
}

// Done closes after the sole writer has retired.
func (q *LegacyInput) Done() <-chan struct{} { return q.done }
