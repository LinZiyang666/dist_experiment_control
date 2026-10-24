// Package workload owns live business processes independently of control
// connections. Process reaping and signalling share one ownership barrier;
// reconnecting a transport must not destroy or forget its children.
package workload

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"sync"
	"syscall"
	"time"
)

var (
	ErrUnknown = errors.New("workload: unknown process")
	ErrExited  = errors.New("workload: process already exited")
	ErrFrozen  = errors.New("workload: process ownership frozen")
)

// Identity binds the public task ID to the original OS child. Metadata is
// immutable after adoption; a new process cannot reuse an existing task ID.
type Identity struct {
	ID             string    `json:"id"`
	OSPID          int       `json:"os_pid"`
	BootID         string    `json:"boot_id,omitempty"`
	StartTimeTicks int64     `json:"start_time_ticks,omitempty"`
	StartedAt      time.Time `json:"started_at"`
}

// Exit retains the kernel result independently of stream EOF or delivery ACK.
// Code is -1 for a signal death or an unavailable status. WaitError distinguishes
// the latter: an ECHILD must never become a fabricated successful exit.
type Exit struct {
	Code      int            `json:"code"`
	Signal    int            `json:"signal,omitempty"`
	Status    uint32         `json:"status"`
	Usage     syscall.Rusage `json:"usage"`
	EndedAt   time.Time      `json:"ended_at"`
	WaitError string         `json:"wait_error,omitempty"`
}

// Record is a point-in-time copy. Exit remains present until the control adapter
// has transferred its delivery responsibility and explicitly forgets the task.
type Record struct {
	Identity
	Exit     *Exit `json:"exit,omitempty"`
	ReapHeld bool  `json:"reap_held,omitempty"`
}

type child struct {
	record Record
	proc   *os.Process
	done   chan struct{}
}

// Manager's zero value is ready for use. It creates one polling goroutine only
// while it owns unreaped children; retaining terminal records does not retain a
// goroutine. No os/exec.Cmd.Wait or other waiter may race an adopted process.
//
// This barrier covers the process table, wait and signals. An application must
// also freeze its I/O and every admission path before attempting an exec: this
// table alone is deliberately not a complete runtime snapshot.
type Manager struct {
	mu      sync.Mutex
	tasks   map[string]*child
	frozen  bool
	polling bool
	// restored is set by RestoreProcesses and cleared by Thaw: only such a
	// table may be Discarded (handles released without reaping), because
	// only such a table has never owned a wait status of its own.
	restored bool
}

// Adopt transfers sole wait/signal ownership after a successful Start. The
// caller must use explicit stdio files, with no hidden os/exec copier goroutines
// or parent pipe closers that depend on Cmd.Wait. On error ownership stays with
// the caller, which must reap the child itself.
func (m *Manager) Adopt(id Identity, p *os.Process) error {
	return m.adopt(id, p, false)
}

// AdoptDraining reserves the original process group until its output owners
// report EOF via AllowReap. A shell can exit while descendants still hold its
// pipes: retaining the unreaped leader keeps that PGID from being recycled and
// keeps administrative kill/evict able to terminate the original subtree.
func (m *Manager) AdoptDraining(id Identity, p *os.Process) error {
	return m.adopt(id, p, true)
}

func (m *Manager) adopt(id Identity, p *os.Process, holdReap bool) error {
	if p == nil || p.Pid <= 0 || id.ID == "" || id.OSPID != p.Pid || id.StartedAt.IsZero() {
		return fmt.Errorf("workload: incomplete process identity")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.frozen {
		return ErrFrozen
	}
	if _, exists := m.tasks[id.ID]; exists {
		return fmt.Errorf("workload: duplicate task %q", id.ID)
	}
	for _, c := range m.tasks {
		if c.record.OSPID == id.OSPID && c.record.Exit == nil {
			return fmt.Errorf("workload: OS child already owned")
		}
	}
	if m.tasks == nil {
		m.tasks = make(map[string]*child)
	}
	m.tasks[id.ID] = &child{record: Record{Identity: id, ReapHeld: holdReap}, proc: p, done: make(chan struct{})}
	if !m.polling {
		m.polling = true
		go m.reapLoop()
	}
	return nil
}

// AllowReap releases the output-lifetime hold. The adapter must have observed
// both output EOFs or explicitly terminated/closed the owned stream resources.
func (m *Manager) AllowReap(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.frozen {
		return ErrFrozen
	}
	c := m.tasks[id]
	if c == nil {
		return ErrUnknown
	}
	c.record.ReapHeld = false
	return nil
}

// Wait observes the sole reaper's retained result. It never closes stdio or
// kills a process, and any number of observers may wait for the same result.
func (m *Manager) Wait(id string) (Exit, error) {
	m.mu.Lock()
	c := m.tasks[id]
	m.mu.Unlock()
	if c == nil {
		return Exit{}, ErrUnknown
	}
	<-c.done
	m.mu.Lock()
	result := *c.record.Exit
	m.mu.Unlock()
	if result.WaitError != "" {
		return result, errors.New(result.WaitError)
	}
	return result, nil
}

// Signal targets the original process group. The lock excludes reap, so the
// leader cannot be reaped and its pid recycled between validation and kill.
// A zombie leader keeps its pid reserved until the sole waiter records exit.
func (m *Manager) Signal(id string, sig syscall.Signal) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.frozen {
		return ErrFrozen
	}
	c := m.tasks[id]
	if c == nil {
		return ErrUnknown
	}
	if c.record.Exit != nil {
		return ErrExited
	}
	return syscall.Kill(-c.record.OSPID, sig)
}

// WithControl holds the wait/signal/freeze barrier for the entire callback.
// Callers pause and join their pumps before entering; the callback may only
// signal, close the already-quiescent master, and publish its control receipt.
// It must not call Manager methods, scan a session, or wait for other owners.
// origin: hot-update S4 plan §3.4 / T15
func (m *Manager) WithControl(id string, apply func(Control) error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.frozen {
		return ErrFrozen
	}
	c := m.tasks[id]
	if c == nil {
		return ErrUnknown
	}
	if apply == nil {
		return fmt.Errorf("workload: missing control callback")
	}
	control := Control{Identity: c.record.Identity}
	if c.record.Exit != nil {
		// Terminal rows retain authority over the actor's master, but never over
		// a PID that may already have been reused. Do not inspect that PID.
		result := *c.record.Exit
		control.Exit = &result
	} else {
		identity, err := checkControlIdentity(c.record.Identity)
		if err != nil {
			return err
		}
		control.Identity = identity
	}
	control.Signal = func(sig syscall.Signal) error {
		if c.record.Exit != nil {
			return ErrExited
		}
		return syscall.Kill(-c.record.OSPID, sig)
	}
	return apply(control)
}

// SignalAll is the explicit administrative stop/evict path. Like Signal it
// never takes a detached list of PIDs and acts after releasing the reap lock.
func (m *Manager) SignalAll(sig syscall.Signal) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.frozen {
		return ErrFrozen
	}
	var errs []error
	for _, c := range m.tasks {
		if c.record.Exit == nil {
			if err := syscall.Kill(-c.record.OSPID, sig); err != nil && !errors.Is(err, syscall.ESRCH) {
				errs = append(errs, err)
			}
		}
	}
	return errors.Join(errs...)
}

// Forget releases a terminal record after its result has been transferred to
// the delivery owner. Live tasks cannot disappear from the ownership census.
func (m *Manager) Forget(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.frozen {
		return ErrFrozen
	}
	c := m.tasks[id]
	if c == nil {
		return ErrUnknown
	}
	if c.record.Exit == nil {
		return fmt.Errorf("workload: cannot forget a live process")
	}
	delete(m.tasks, id)
	return nil
}

func (m *Manager) Snapshot() []Record {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.snapshotLocked()
}

// Lookup returns one task's point-in-time record. A runtime uses it right
// after a successful Start to prove the adapter really transferred the child
// (Start succeeded == already adopted) and to read its OS identity.
func (m *Manager) Lookup(id string) (Record, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	c, ok := m.tasks[id]
	if !ok {
		return Record{}, false
	}
	r := c.record
	if r.Exit != nil {
		exit := *r.Exit
		r.Exit = &exit
	}
	return r, true
}

// Freeze is the process-table part of a runtime cut. On return no wait/signal
// operation is in flight and all reaped results are in the snapshot. The kernel
// may still exit children: they remain waitable zombies until Thaw or restore.
func (m *Manager) Freeze() ([]Record, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.freezeLocked()
}

// FreezeUntil includes acquisition of the control barrier in the caller's
// absolute deadline. A timeout leaves ownership active; no late waiter can
// freeze the table after the caller has already rolled its other owners back.
// origin: hot-update S4 plan §4.1 / T15
func (m *Manager) FreezeUntil(deadline time.Time) ([]Record, error) {
	for {
		if !time.Now().Before(deadline) {
			return nil, os.ErrDeadlineExceeded
		}
		if m.mu.TryLock() {
			defer m.mu.Unlock()
			// Scheduling between TryLock and publication counts against the same
			// budget, including on an otherwise uncontended table.
			if !time.Now().Before(deadline) {
				return nil, os.ErrDeadlineExceeded
			}
			return m.freezeLocked()
		}
		time.Sleep(min(time.Millisecond, time.Until(deadline)))
	}
}

func (m *Manager) freezeLocked() ([]Record, error) {
	if m.frozen {
		return nil, ErrFrozen
	}
	m.frozen = true
	return m.snapshotLocked(), nil
}

func (m *Manager) Thaw() {
	m.mu.Lock()
	m.frozen, m.restored = false, false
	// Restored managers have no running poller until the whole runtime has
	// installed its owners. Existing frozen managers keep their own poller.
	if !m.polling {
		for _, c := range m.tasks {
			if c.record.Exit == nil {
				m.polling = true
				go m.reapLoop()
				break
			}
		}
	}
	m.mu.Unlock()
}

// Frozen reports whether the table currently refuses wait/signal/adopt.
// The image layer uses it to refuse a Runtime.Thaw ordered before the
// Manager's (the deferred revocation kill would be lost silently otherwise).
func (m *Manager) Frozen() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.frozen
}

// Discard releases a restored, never-thawed table without reaping or
// signalling anything: the process handles (pidfds) are released, the rows
// forgotten, the manager returned to its unused zero state. It is the
// Manager half of an import that succeeded and then had to be abandoned
// (a re-dispatch to the previous image, S2 plan D6): the children stay
// exactly as the kernel holds them for whoever restores next. A table that
// ever polled, was never restored, or is not frozen is refused unchanged.
func (m *Manager) Discard() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.restored || !m.frozen || m.polling {
		return fmt.Errorf("workload: discard requires a restored, unthawed process table")
	}
	var errs []error
	for _, c := range m.tasks {
		if c.proc != nil {
			errs = append(errs, c.proc.Release())
		}
	}
	m.tasks, m.frozen, m.restored = nil, false, false
	return errors.Join(errs...)
}

func (m *Manager) snapshotLocked() []Record {
	rows := make([]Record, 0, len(m.tasks))
	for _, c := range m.tasks {
		r := c.record
		if r.Exit != nil {
			exit := *r.Exit
			r.Exit = &exit
		}
		rows = append(rows, r)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].ID < rows[j].ID })
	return rows
}

func (m *Manager) reapLoop() {
	// WNOHANG avoids an uninterruptible userspace Wait stack during a planned
	// image switch. Poll only our exact children, never P_ALL: spawn helpers and
	// candidate probes have separate owners and must retain their own statuses.
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		m.mu.Lock()
		live := false
		for _, c := range m.tasks {
			if c.record.Exit != nil {
				continue
			}
			if !m.frozen && !c.record.ReapHeld {
				m.reapLocked(c)
			}
			live = live || c.record.Exit == nil
		}
		if !live {
			m.polling = false
			m.mu.Unlock()
			return
		}
		m.mu.Unlock()
		<-tick.C
	}
}

func (m *Manager) reapLocked(c *child) {
	var status syscall.WaitStatus
	var usage syscall.Rusage
	pid, err := syscall.Wait4(c.record.OSPID, &status, syscall.WNOHANG, &usage)
	if errors.Is(err, syscall.EINTR) || (err == nil && pid == 0) {
		return
	}
	result := Exit{Code: -1, Status: uint32(status), Usage: usage, EndedAt: time.Now().UTC()}
	if err != nil {
		result.WaitError = fmt.Sprintf("workload: wait child: %v", err)
	} else {
		result.Code = status.ExitStatus()
		if status.Signaled() {
			result.Signal = int(status.Signal())
		}
	}
	// Release closes os.Process's optional pidfd; Wait4 consumed only the kernel
	// exit record. Publish the result before releasing the same lock as Signal.
	_ = c.proc.Release()
	c.proc = nil
	c.record.Exit = &result
	close(c.done)
}
