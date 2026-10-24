package workload

import (
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sync"
	"syscall"

	"github.com/LinZiyang666/tether/internal/upgradebudget"
)

// The replay defaults are the hot-update acceptance budgets (plan §7):
// one source, read from here so a "tuning" of the per-stream cap cannot
// drift away from the number the drills and runbooks quote.
const (
	DefaultReplayCapacity int64 = upgradebudget.StreamReplayBytes
	DefaultReplayBudget   int64 = upgradebudget.NodeReplayBytes
	replayVersion               = 1
)

var (
	ErrBackpressure = errors.New("workload: replay capacity exhausted")
	ErrReplayOffset = errors.New("workload: replay offset outside retained range")
)

// ReplayBudget reserves the full capacity of each log, including empty logs.
// It bounds worst-case retained data across streams; a quiet stream cannot
// overcommit memory that will only become visible during a carrier outage.
// The zero value uses DefaultReplayBudget. Set Limit before first use. Share
// the same pointer across domains; neither the mutex nor reservations may be
// copied into another budget. Imports must preserve the receiving node Limit.
// origin: hot-update S5 plan §3.4 / T30
type ReplayBudget struct {
	Limit int64
	mu    sync.Mutex
	used  int64
}

func (b *ReplayBudget) reserve(capacity int64) error {
	if b == nil || capacity <= 0 || capacity > DefaultReplayCapacity {
		return fmt.Errorf("workload: invalid replay capacity or missing budget")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	limit := b.Limit
	if limit == 0 {
		limit = DefaultReplayBudget
	}
	if capacity > limit-b.used {
		return ErrBackpressure
	}
	b.used += capacity
	return nil
}

func (b *ReplayBudget) release(capacity int64) {
	b.mu.Lock()
	b.used -= capacity
	b.mu.Unlock()
}

// ReplayState describes a bounded byte log. Base is the receiver-confirmed
// prefix; End includes every byte the source has entrusted to this owner. FIN
// applies exactly at End and remains until the domain records its delivery.
// Neither successful Publish nor successful socket buffering is an ACK here:
// the adapter must only call Ack when its peer has accepted responsibility.
type ReplayState struct {
	Version  int    `json:"version"`
	Capacity int64  `json:"capacity"`
	Base     uint64 `json:"base"`
	End      uint64 `json:"end"`
	FIN      bool   `json:"fin"`
}

// Replay owns an anonymous fixed-size ring file. Prepare borrows its frozen
// descriptor through the same explicit resource bundle as business sockets.
// Bytes stay outside the JSON metadata limit, and offsets survive any number
// of physical ring wraps. Its mutex is also the append/ACK/freeze barrier.
type Replay struct {
	mu     sync.Mutex
	file   *os.File
	budget *ReplayBudget
	state  ReplayState
	frozen bool
	closed bool
	change chan struct{}
}

func NewReplay(budget *ReplayBudget, capacity int64) (*Replay, error) {
	if err := budget.reserve(capacity); err != nil {
		return nil, err
	}
	f, err := newReplayFile(capacity)
	if err != nil {
		budget.release(capacity)
		return nil, err
	}
	return &Replay{file: f, budget: budget, change: make(chan struct{}), state: ReplayState{Version: replayVersion, Capacity: capacity}}, nil
}

// RestoreReplay takes ownership of file only on success and starts frozen.
// The caller has already verified its resource identity through handoff; the
// domain's global rollback keeps this log frozen until all resources validate.
func RestoreReplay(budget *ReplayBudget, state ReplayState, file *os.File) (*Replay, error) {
	if state.Version != replayVersion || state.Base > state.End || state.Capacity <= 0 ||
		state.Capacity > DefaultReplayCapacity || state.End-state.Base > uint64(state.Capacity) {
		return nil, fmt.Errorf("workload: invalid replay state")
	}
	if err := validateReplayFile(file, state.Capacity); err != nil {
		return nil, err
	}
	if err := budget.reserve(state.Capacity); err != nil {
		return nil, err
	}
	return &Replay{file: file, budget: budget, state: state, frozen: true, change: make(chan struct{})}, nil
}

// Append writes the largest prefix that fits. Every reported byte is retained
// before End advances. Callers must preserve an unwritten suffix when err is
// ErrBackpressure or an I/O error; reading more source bytes would lose it.
func (r *Replay) Append(data []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return 0, os.ErrClosed
	}
	if r.frozen {
		return 0, ErrFrozen
	}
	if r.state.FIN {
		return 0, fmt.Errorf("workload: append after stream FIN")
	}
	if uint64(len(data)) > math.MaxUint64-r.state.End {
		return 0, fmt.Errorf("workload: stream offset exhausted")
	}
	free := int(r.state.Capacity - int64(r.state.End-r.state.Base))
	want := min(len(data), free)
	n := 0
	defer func() {
		if n > 0 {
			r.changedLocked()
		}
	}()
	for n < want {
		physical := int64(r.state.End % uint64(r.state.Capacity))
		part := min(want-n, int(r.state.Capacity-physical))
		written, err := r.file.WriteAt(data[n:n+part], physical)
		r.state.End += uint64(written)
		n += written
		if err != nil {
			return n, err
		}
		if written != part {
			return n, io.ErrShortWrite
		}
	}
	if n != len(data) {
		return n, ErrBackpressure
	}
	return n, nil
}

// Read copies a retained contiguous logical range into dst without consuming
// it. n>0 may accompany EOF when the range reaches FIN; adapters must deliver
// those bytes before forwarding FIN. Reads are permitted while frozen.
func (r *Replay) Read(offset uint64, dst []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return 0, os.ErrClosed
	}
	if offset < r.state.Base || offset > r.state.End {
		return 0, ErrReplayOffset
	}
	want := int(min(uint64(len(dst)), r.state.End-offset))
	n := 0
	for n < want {
		physical := int64((offset + uint64(n)) % uint64(r.state.Capacity))
		part := min(want-n, int(r.state.Capacity-physical))
		read, err := r.file.ReadAt(dst[n:n+part], physical)
		n += read
		if err != nil {
			return n, err
		}
		if read != part {
			return n, io.ErrUnexpectedEOF
		}
	}
	if r.state.FIN && offset+uint64(n) == r.state.End {
		return n, io.EOF
	}
	return n, nil
}

// Ack monotonically releases a confirmed prefix. Duplicate/older ACKs are
// harmless; future ACKs cannot discard bytes that have never been appended.
func (r *Replay) Ack(offset uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return os.ErrClosed
	}
	if r.frozen {
		return ErrFrozen
	}
	if offset > r.state.End {
		return ErrReplayOffset
	}
	if offset > r.state.Base {
		// ACK frees ring space, not capacity: this owner can refill the entire
		// ring during the next outage. Only Close/Detach release reservation.
		// origin: hot-update S5 plan §3.4 / TestReplayACKKeepsCapacityReservation
		r.state.Base = offset
		r.changedLocked()
	}
	return nil
}

func (r *Replay) Finish() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return os.ErrClosed
	}
	if r.frozen {
		return ErrFrozen
	}
	r.state.FIN = true
	r.changedLocked()
	return nil
}

func (r *Replay) Snapshot() ReplayState {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.state
}

// Observe returns a snapshot and the notification associated with that exact
// snapshot, avoiding a missed wake between checking capacity and waiting. Close
// also wakes observers. This does not consume bytes or acknowledge delivery.
func (r *Replay) Observe() (ReplayState, <-chan struct{}) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.state, r.change
}

func (r *Replay) changedLocked() {
	close(r.change)
	r.change = make(chan struct{})
}

// Freeze borrows the original descriptor for the handoff package to duplicate.
// Callers must not close it or change its flags, nor resume the old owner while
// a replacement image can act on the same file. Close on an abandoned owner is
// valid only after deciding which image retains the authoritative state.
func (r *Replay) Freeze() (ReplayState, *os.File, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return ReplayState{}, nil, os.ErrClosed
	}
	if r.frozen {
		return ReplayState{}, nil, ErrFrozen
	}
	r.frozen = true
	return r.state, r.file, nil
}

func (r *Replay) Thaw() {
	r.mu.Lock()
	r.frozen = false
	r.mu.Unlock()
}

func (r *Replay) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	r.closed = true
	close(r.change)
	err := r.file.Close()
	r.budget.release(r.state.Capacity)
	return err
}

// Detach retires a frozen log WITHOUT closing its file: the caller receives
// the ring file and the state it was frozen at, and the budget reservation is
// released. It exists for an import that validated and built actors and then
// has to give every descriptor back for a re-dispatch (S2 plan D6); a live
// or closed log is refused. origin: hot-update S2 plan §4 item 2.
func (r *Replay) Detach() (*os.File, ReplayState, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, ReplayState{}, os.ErrClosed
	}
	if !r.frozen {
		return nil, ReplayState{}, fmt.Errorf("workload: detach requires a frozen replay log")
	}
	r.closed = true
	close(r.change)
	file := r.file
	r.file = nil
	r.budget.release(r.state.Capacity)
	return file, r.state, nil
}

// Used reports the capacity currently reserved across every live log. It is
// the observation point for the image budget-conservation invariant: the sum
// after an import must equal the sum the exporter reported.
func (b *ReplayBudget) Used() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.used
}

// CheckReplayFile is the read-only replay-descriptor predicate an importer
// runs on a file it has taken from a handoff bundle BEFORE building any
// actor on it: a sealed anonymous ring of exactly capacity bytes, read-write,
// non-append, CLOEXEC. It never changes the descriptor. The same predicate
// guards RestoreReplay; exposing it lets the import stage reject a bad
// bundle while every descriptor is still returnable.
func CheckReplayFile(file *os.File, capacity int64) error {
	return validateReplayFile(file, capacity)
}

// ReplayReservedBytes reports the capacity this log still owns. Closed and
// detached wrappers may remain referenced during cleanup, but their reservation
// has already been released. Nil denotes an absent stream. This is a domain's
// accounting primitive; global Used includes every domain sharing the budget.
// origin: hot-update S5 plan §3.4 / T30
func ReplayReservedBytes(r *Replay) int64 {
	if r == nil {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return 0
	}
	return r.state.Capacity
}

// ReplayObservation returns a live reservation and its real descriptor identity
// without borrowing/exporting the ring or changing its frozen state.
// The caller emits after this lock has been released.
// origin: hot-update S5 plan §3.4 / DRILL-OBSERVER
func ReplayObservation(r *Replay) (ReplayState, uint64, uint64, bool) {
	if r == nil {
		return ReplayState{}, 0, 0, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || r.file == nil {
		return ReplayState{}, 0, 0, false
	}
	info, err := r.file.Stat()
	if err != nil {
		return r.state, 0, 0, true
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return r.state, 0, 0, true
	}
	return r.state, uint64(stat.Dev), uint64(stat.Ino), true
}
