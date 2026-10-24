package ptysession

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/workload"
)

var (
	ErrConflict = errors.New("ptysession: conflicting sequence or payload")
	ErrStale    = errors.New("ptysession: stale control sequence")
	ErrRevoked  = errors.New("ptysession: authority revoked")
)

// Actor owns both borrowers of one master. The token serializes lifecycle and
// control transactions; mu only protects accounting, never a blocking syscall.
// The Manager guard precedes mu whenever both are needed. Replay locks come last.
// origin: hot-update S4 plan §3.4 / §4.1
type Actor struct {
	control       chan struct{}
	mu            sync.Mutex
	cfg           Config
	phase         actorPhase
	source        sourcePhase
	state         Image
	read, write   pump
	inputClosing  string
	proof         HealthProof
	proofDeadline int64
	proofSeq      uint64
	prepared      bool
	files         map[string]*os.File
}

// A pump generation owns its done channel until its final byte accounting.
// Resuming a timed-out freeze does not replace a still-running generation.
type pump struct {
	running bool
	paused  bool
	stop    chan struct{}
	done    chan struct{}
}

func newActor(cfg Config, state Image, phase actorPhase) *Actor {
	a := &Actor{cfg: cfg, state: state, phase: phase, control: make(chan struct{}, 1)}
	a.control <- struct{}{}
	a.source = sourceReading
	if state.Discarding {
		a.source = sourceDiscarding
	}
	if state.SourceEnded {
		a.source = sourceEnded
	}
	for _, p := range []*pump{&a.read, &a.write} {
		p.done = make(chan struct{})
		close(p.done)
		p.paused = phase != phaseActive
	}
	return a
}

func validateConfig(cfg Config, closed bool) error {
	if cfg.Manager == nil || cfg.PID == "" || cfg.Input == nil || cfg.Output == nil || cfg.Clock == nil || (cfg.Master == nil) != closed {
		return fmt.Errorf("ptysession: incomplete actor resources")
	}
	if !closed && (cfg.Identity.Version != 1 || !cfg.Identity.Started || cfg.Identity.LeaderPID <= 0 || cfg.Identity.BootID == "" || cfg.Identity.StartTimeTicks <= 0) {
		return fmt.Errorf("ptysession: terminal has no successful adoption identity")
	}
	if cfg.Input.Snapshot().Capacity != upgradebudget.PTYInputBytes || cfg.Output.Snapshot().Capacity != upgradebudget.StreamReplayBytes {
		return fmt.Errorf("ptysession: incorrect replay reservation")
	}
	return nil
}

// New takes resources only on success. The initial KA grant is minted here,
// exactly once; activation and imports never consult the clock for a new grant.
func New(cfg Config) (*Actor, error) {
	if err := validateConfig(cfg, false); err != nil {
		return nil, err
	}
	in, out := cfg.Input.Snapshot(), cfg.Output.Snapshot()
	if in.Base != 0 || in.End != 0 || in.FIN || out.Base != 0 || out.End != 0 || out.FIN {
		return nil, fmt.Errorf("ptysession: new actor requires empty replays")
	}
	now, err := cfg.Clock()
	if err != nil {
		return nil, err
	}
	if now < 0 {
		return nil, fmt.Errorf("ptysession: invalid boot clock")
	}
	interval := normalizeInterval(cfg.KAIntervalMS)
	state := Image{Version: 1, Identity: cfg.Identity, KAIntervalMS: interval}
	if interval != 0 {
		state.KADeadlineBootNS, err = grantDeadline(now, interval)
		if err != nil {
			return nil, err
		}
	}
	if err := cfg.Master.SetReadDeadline(time.Time{}); err != nil {
		return nil, err
	}
	if err := cfg.Master.SetWriteDeadline(time.Time{}); err != nil {
		return nil, err
	}
	a := newActor(cfg, state, phaseActive)
	a.mu.Lock()
	a.startPumpsLocked()
	a.mu.Unlock()
	return a, nil
}

func (a *Actor) startPumpsLocked() {
	a.startPumpLocked(&a.read, true)
	a.startPumpLocked(&a.write, false)
}

func (a *Actor) startPumpLocked(p *pump, read bool) {
	if a.phase != phaseActive || p.running || p.paused || a.state.MasterClosed {
		return
	}
	if (read && a.source == sourceEnded) || (!read && (a.state.InputEnded || a.inputClosing != "" || a.state.Revoked != "")) {
		return
	}
	p.running = true
	p.stop = make(chan struct{})
	p.done = make(chan struct{})
	if read {
		go a.readLoop(p.stop)
	} else {
		go a.writeLoop(p.stop)
	}
}

func (a *Actor) pumpDone(p *pump, read bool) {
	a.mu.Lock()
	p.running = false
	if !read {
		finalizeInputLocked(a)
	}
	close(p.done)
	// The old borrower is done before a replacement can touch the master.
	// This also completes recovery when a freeze timed out before its barrier.
	a.startPumpLocked(p, read)
	a.mu.Unlock()
}

func stopped(stop <-chan struct{}) bool {
	select {
	case <-stop:
		return true
	default:
		return false
	}
}

// origin: hot-update S4 plan §2.1 / T12 — images validate this closed wire
// vocabulary; platform error strings must never become retained diagnostics.
// origin: hot-update S4 internal review rv-eof rv-eof-1 / rv-fd rv-fd-1
// Closing intake cannot publish final counters while a kernel receipt is still
// owned by the writer. Its generation's done channel is the accounting barrier.
func (a *Actor) abortInputLocked(reason string) {
	if a.state.InputEnded {
		return
	}
	// Source closure is provisional until n is accounted. A real write/storage
	// failure or revocation in that window must not be mistaken for clean FIN.
	if a.inputClosing == "" || a.inputClosing == "source_ended" {
		a.inputClosing = reason
	}
	a.pauseLocked(&a.write)
	if !a.write.running {
		finalizeInputLocked(a)
	}
}

func finalizeInputLocked(a *Actor) {
	if a.inputClosing == "" || a.state.InputEnded {
		return
	}
	s := a.cfg.Input.Snapshot()
	a.state.InputEnded = true
	// Source end is clean only after every logical FIN byte was applied. Real
	// storage/write/revocation errors remain errors even when A happens to equal Q.
	if a.inputClosing != "source_ended" || !a.state.InputFIN || s.Base != s.End {
		a.state.InputError = "pty input aborted: " + a.inputClosing
	}
}

// Close is resource retirement, never a way to obtain a successful freeze.
// Closing the master wakes both borrowers; the replay files outlive both joins.
func (a *Actor) Close() error {
	<-a.control
	defer func() { a.control <- struct{}{} }()
	a.mu.Lock()
	if a.phase == phaseClosed {
		a.mu.Unlock()
		return nil
	}
	a.phase = phaseClosed
	a.pauseLocked(&a.read)
	a.pauseLocked(&a.write)
	rd, wd := a.read.done, a.write.done
	a.abortInputLocked("close_error")
	master := a.cfg.Master
	closed := a.state.MasterClosed
	a.state.MasterClosed = true
	a.mu.Unlock()
	var err error
	if master != nil && !closed {
		err = master.Close()
	}
	<-rd
	<-wd
	// An explicit master close ends this owner's tail responsibility, but does
	// not manufacture the child's terminal status. Manager remains the waiter.
	reapErr := a.cfg.Manager.AllowReap(a.cfg.PID)
	if errors.Is(reapErr, workload.ErrUnknown) {
		reapErr = nil
	}
	return errors.Join(err, reapErr, a.cfg.Input.Close(), a.cfg.Output.Close())
}
