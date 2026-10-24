package cli

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
)

const (
	AttemptReceiptFDEnv = "TETHER_ATTEMPT_RECEIPT_FD"
	AttemptLogicalIDEnv = "TETHER_ATTEMPT_LOGICAL_ID"
	AttemptLimit        = 128
	AttemptQueueLimit   = 4096
	AttemptByteLimit    = 128 << 20
)

// AttemptLogicalID adopts the collector's pre-registered slot identity. A bad
// value fails before adopting the receipt FD or starting any network operation.
// origin: hot-update S6 internal review rv-ctl-2
func AttemptLogicalID(fallback string) (string, error) {
	id, supplied := os.LookupEnv(AttemptLogicalIDEnv)
	if !supplied {
		id = fallback
	}
	if !validLogicalID(id) {
		return "", ErrAttemptInvalid
	}
	return id, nil
}

// AttemptSinkFromEnv opts in to a separate JSONL channel. The FD is dedicated
// to this CLI invocation and transferred to the sink; it is never stdout or
// stderr, and an invalid configuration is an error rather than absent evidence.
func AttemptSinkFromEnv(deadline controlbudget.Deadline) (*AttemptSink, error) {
	value, configured := os.LookupEnv(AttemptReceiptFDEnv)
	if !configured {
		return nil, nil
	}
	fd, err := strconv.ParseUint(value, 10, 32)
	if err != nil || fd <= 2 {
		return nil, ErrAttemptInvalid
	}
	if _, err = controlbudget.Remaining(deadline); err != nil {
		return nil, err
	}
	file, err := controlbudget.ReceiptFile(uintptr(fd))
	if err != nil {
		return nil, err
	}
	sink, err := NewAttemptSinkWithin(file, deadline)
	if err != nil {
		_ = file.Close()
	}
	return sink, err
}

var (
	ErrAttemptOverflow = errors.New("cli: attempt receipt capacity exceeded")
	ErrAttemptInvalid  = errors.New("cli: invalid attempt receipt")
	ErrAttemptClosed   = errors.New("cli: attempt receipt sink closed")
)

type attemptKey struct {
	id      string
	ordinal uint32
}

// AttemptSink owns a dedicated file and its writer. Completion order may differ
// from Begin order; the per-logical-ID ordinal is assigned at Begin, never by
// the writer. Close drains and joins, and returns every sticky capacity/I/O
// failure even when the control operation itself succeeded.
// origin: hot-update S6 plan §8.3 / T35
type AttemptSink struct {
	mu            sync.Mutex
	file          *os.File
	queue         chan []byte
	done          chan struct{}
	counts        map[string]uint32
	pending       map[attemptKey]controlbudget.Attempt
	bytes         int
	identityBytes int
	closed        bool
	err           error
	admissionEnd  time.Time
}

var _ controlbudget.Observer = (*AttemptSink)(nil)

// NewAttemptSink transfers file ownership on success. RunE should use the
// Within variant to bound pre-start writes by admission; terminal writes
// retain a separate short drain after the task has truly started.
func NewAttemptSink(file *os.File) (*AttemptSink, error) {
	deadline, err := controlbudget.After(upgradebudget.RequestLatency)
	if err != nil {
		return nil, err
	}
	return NewAttemptSinkWithin(file, deadline)
}

func NewAttemptSinkWithin(file *os.File, deadline controlbudget.Deadline) (*AttemptSink, error) {
	if file == nil {
		return nil, ErrAttemptInvalid
	}
	raw, err := file.SyscallConn()
	if err != nil {
		return nil, err
	}
	var standard bool
	if err = raw.Control(func(fd uintptr) { standard = fd <= 2 }); err != nil {
		return nil, err
	}
	if standard {
		return nil, ErrAttemptInvalid
	}
	remaining, err := controlbudget.Remaining(deadline)
	if err != nil {
		return nil, err
	}
	if err = controlbudget.ReceiptCloseOnExec(file); err != nil {
		return nil, err
	}
	// Pipes/sockets need a pollable write deadline: an abandoned collector must
	// not leave a writer behind at CLI exit. Regular files do not support one.
	if err = file.SetWriteDeadline(time.Now().Add(remaining)); err != nil {
		info, statErr := file.Stat()
		if statErr != nil || !info.Mode().IsRegular() || !errors.Is(err, os.ErrNoDeadline) {
			return nil, errors.Join(err, statErr)
		}
	}
	sink := &AttemptSink{
		file: file, queue: make(chan []byte, AttemptQueueLimit), done: make(chan struct{}),
		counts: make(map[string]uint32), pending: make(map[attemptKey]controlbudget.Attempt), admissionEnd: time.Now().Add(remaining),
	}
	go sink.writeReceipts()
	return sink, nil
}

func validLogicalID(id string) bool {
	if len(id) == 0 || len(id) > 128 {
		return false
	}
	for _, c := range id {
		if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9') && c != '_' && c != '-' && c != '.' && c != ':' {
			return false
		}
	}
	return true
}

// Fixed deep-owner classes join the retry helper vocabulary; free-form errors
// still fail closed and can never carry transport credentials into JSONL.
// origin: hot-update S6 plan §8.3 / T35
func validAttemptClass(class string) bool {
	switch class {
	case "success", "error", "retryable", "refused", "ok", "timeout", "no_responders", "connection", "auth", "denied", "unsupported", "canceled", "skipped", "terminal", "result_unknown":
		return true
	default:
		return false
	}
}

func (s *AttemptSink) failLocked(err error) error {
	s.err = errors.Join(s.err, err)
	return s.err
}

func (s *AttemptSink) Begin(id string, phase controlbudget.AttemptPhase) (controlbudget.Attempt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return controlbudget.Attempt{}, ErrAttemptClosed
	}
	if s.err != nil {
		return controlbudget.Attempt{}, s.err
	}
	if !validLogicalID(id) || !controlbudget.ValidPhase(phase) {
		return controlbudget.Attempt{}, s.failLocked(ErrAttemptInvalid)
	}
	// Pending reservations count too: a producer cannot begin unbounded work
	// without Record, nor allocate an unbounded map of never-finished slot IDs.
	// origin: hot-update S6 plan §8.3 / T35
	if s.counts[id] >= AttemptLimit || len(s.pending) >= AttemptQueueLimit || s.identityBytes+len(id)+64 > AttemptByteLimit {
		return controlbudget.Attempt{}, s.failLocked(ErrAttemptOverflow)
	}
	now, err := controlbudget.Now()
	if err != nil {
		return controlbudget.Attempt{}, s.failLocked(err)
	}
	if s.counts[id] == 0 {
		s.identityBytes += len(id) + 64
	}
	s.counts[id]++
	a := controlbudget.Attempt{LogicalID: id, Ordinal: s.counts[id], Phase: phase, StartedMonoNS: now}
	s.pending[attemptKey{id, a.Ordinal}] = a
	return a, nil
}

func (s *AttemptSink) Record(a controlbudget.Attempt) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrAttemptClosed
	}
	if s.err != nil {
		return s.err
	}
	key := attemptKey{a.LogicalID, a.Ordinal}
	start, ok := s.pending[key]
	now, err := controlbudget.Now()
	if err != nil {
		return s.failLocked(err)
	}
	if !ok || a.Phase != start.Phase || a.StartedMonoNS != start.StartedMonoNS ||
		a.FinishedMonoNS < a.StartedMonoNS || a.FinishedMonoNS > now || !validAttemptClass(a.Class) ||
		a.BackoffNS < 0 || a.BackoffNS != 0 && a.Phase != controlbudget.PhaseBackoff {
		return s.failLocked(ErrAttemptInvalid)
	}
	// origin: hot-update S6 internal review rv-ctl-8
	// Older helper aliases remain accepted at the API boundary, while the
	// production channel publishes one closed class vocabulary for every owner.
	switch a.Class {
	case "success":
		a.Class = "ok"
	case "error":
		a.Class = "terminal"
	}
	body, err := json.Marshal(a)
	if err != nil {
		return s.failLocked(err)
	}
	body = append(body, '\n')
	if s.bytes+len(body) > AttemptByteLimit {
		return s.failLocked(ErrAttemptOverflow)
	}
	// Capacity is a failed receipt, never a silently dropped success/failure.
	// origin: hot-update S6 plan §8.3 / T35
	select {
	case s.queue <- body:
		s.bytes += len(body)
		delete(s.pending, key)
		return nil
	default:
		return s.failLocked(ErrAttemptOverflow)
	}
}

func (s *AttemptSink) writeReceipts() {
	defer close(s.done)
	var writeErr error
	write := func(body []byte) {
		if writeErr != nil {
			return // Close reports failure; no subsequent success can erase it.
		}
		// origin: hot-update S6 internal review rv-ctl-5
		// True start ends admission, while terminal receipts may arrive hours
		// later. Each write has a bounded drain; an expired admission deadline
		// cannot invalidate a settled task's terminal evidence.
		now := time.Now()
		end := now.Add(250 * time.Millisecond)
		if now.Before(s.admissionEnd) && s.admissionEnd.Before(end) {
			end = s.admissionEnd
		}
		if err := s.file.SetWriteDeadline(end); err != nil && !errors.Is(err, os.ErrNoDeadline) {
			writeErr = err
		}
		var n int
		if writeErr == nil {
			n, writeErr = s.file.Write(body)
		}
		if writeErr == nil && n != len(body) {
			writeErr = io.ErrShortWrite
		}
		if writeErr != nil {
			s.mu.Lock()
			_ = s.failLocked(writeErr)
			s.mu.Unlock()
		}
	}
	for body := range s.queue {
		write(body)
	}
	// origin: hot-update S6 internal review rv-ctl-2
	// Close is the producer's census, never a collector-side reconstruction.
	// The queue is closed and drained, so every reserved ordinal is final.
	s.mu.Lock()
	ids := make([]string, 0, len(s.counts))
	for id := range s.counts {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	var closing [][]byte
	for _, id := range ids {
		body, err := json.Marshal(struct {
			Kind         string `json:"kind"`
			LogicalID    string `json:"logical_id"`
			AttemptCount uint32 `json:"attempt_count"`
			Overflow     bool   `json:"overflow"`
		}{"observer_close", id, s.counts[id], errors.Is(s.err, ErrAttemptOverflow)})
		if err != nil {
			_ = s.failLocked(err)
		} else {
			closing = append(closing, append(body, '\n'))
		}
	}
	s.mu.Unlock()
	for _, body := range closing {
		write(body)
	}
	if err := s.file.Close(); err != nil {
		s.mu.Lock()
		_ = s.failLocked(err)
		s.mu.Unlock()
	}
}

func (s *AttemptSink) Close() error {
	s.mu.Lock()
	if !s.closed {
		s.closed = true
		if len(s.pending) != 0 {
			_ = s.failLocked(errors.New("cli: incomplete attempt receipts"))
		}
		close(s.queue)
	}
	s.mu.Unlock()
	<-s.done // explicit join, including blocked-write cancellation by deadline
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

func beginAttempt(observer controlbudget.Observer, id string, phase controlbudget.AttemptPhase) (controlbudget.Attempt, error) {
	if observer == nil {
		return controlbudget.Attempt{}, nil
	}
	return observer.Begin(id, phase)
}

func completeAttempt(observer controlbudget.Observer, a controlbudget.Attempt, sent bool, class string, backoff time.Duration) error {
	if observer == nil {
		return nil
	}
	now, err := controlbudget.Now()
	if err != nil {
		return err
	}
	a.FinishedMonoNS, a.Sent, a.Class, a.BackoffNS = now, sent, class, int64(backoff)
	return observer.Record(a)
}
