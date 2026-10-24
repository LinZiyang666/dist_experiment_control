package tunnel

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/flow"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/listenerimage"
	"github.com/LinZiyang666/tether/internal/rawsocket"
	"github.com/LinZiyang666/tether/internal/sealedstate"
)

// A fired AfterFunc remains a real owner until its callback returns. timer=nil
// means no future scheduling, never that cleanup has joined. The callback can
// call closeEndpoint without waiting on its own completion record.
// origin: hot-update S6 plan §6.2 / T22
var errServerLegacyBusy = errors.New("tunnel: legacy_session_busy")

type endpointTimer struct {
	done chan struct{}
	once sync.Once
}

func finishEndpointTimer(s *Server, work *endpointTimer) {
	work.once.Do(func() {
		s.mu.Lock()
		delete(s.callbacks, work)
		close(work.done)
		s.mu.Unlock()
	})
}
func stopEndpointTimerLocked(s *Server, e *flowEndpoint) {
	if e.timer != nil {
		if e.timer.Stop() && e.timerWork != nil {
			work := e.timerWork
			work.once.Do(func() { delete(s.callbacks, work); close(work.done) })
		}
		e.timer = nil
	}
}

// Connection accounting starts at Accept, before a handler is launched. Fencing
// holds the same table lock, so a conn accepted across the boundary cannot sneak
// a new REGISTER into the joined generation. It is safe to close TLS's underlying
// socket: only control connections are here, never original public raw sockets.
// origin: hot-update S6 plan §6.1 / §6.2 / T23
func trackControlConnection(s *Server, conn net.Conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.imagePaused {
		return false
	}
	if s.controlConns == nil {
		s.controlConns = make(map[net.Conn]chan struct{})
	}
	s.controlConns[conn] = make(chan struct{})
	return true
}
func untrackControlConnection(s *Server, conn net.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if done := s.controlConns[conn]; done != nil {
		delete(s.controlConns, conn)
		close(done)
	}
}
func closeServerTransport(conn net.Conn) {
	if tc, ok := conn.(*tls.Conn); ok {
		_ = tc.NetConn().Close()
	}
	_ = conn.Close()
}
func startServerCarrierMonitor(s *Server, e *flowEndpoint, a *flowCarrier) {
	done := make(chan struct{})
	s.mu.Lock()
	if s.monitors == nil {
		s.monitors = make(map[*flowCarrier]chan struct{})
	}
	s.monitors[a] = done
	s.mu.Unlock()
	go func() {
		<-a.Stop
		a.Join()
		detachEndpoint(s, e, a)
		s.mu.Lock()
		delete(s.monitors, a)
		close(done)
		s.mu.Unlock()
	}()
}
func waitServerBackground(s *Server) {
	s.mu.Lock()
	var done []chan struct{}
	for _, ch := range s.monitors {
		done = append(done, ch)
	}
	for work := range s.callbacks {
		done = append(done, work.done)
	}
	s.mu.Unlock()
	for _, ch := range done {
		<-ch
	}
}

// The absolute BOOTTIME deadline is converted only for a bounded netpoll wait.
// Every phase recomputes the remainder, including time queued on a table lock.
// No wall-clock timestamp or fresh per-phase timeout can extend the cut.
// origin: hot-update S6 plan §2.1 / T21–T23
func serverPauseDeadline(d controlbudget.Deadline) (time.Time, error) {
	now, err := rawsocket.BootNow()
	if err != nil {
		return time.Time{}, err
	}
	if int64(d) <= now {
		return time.Time{}, os.ErrDeadlineExceeded
	}
	return time.Now().Add(time.Duration(int64(d) - now)), nil
}
func serverPauseWait(done <-chan struct{}, d controlbudget.Deadline) error {
	until, err := serverPauseDeadline(d)
	if err != nil {
		return err
	}
	timer := time.NewTimer(time.Until(until))
	defer timer.Stop()
	select {
	case <-done:
		return nil
	case <-timer.C:
		return os.ErrDeadlineExceeded
	}
}

// Each endpoint keeps a named native Accept view in e.listener. Native TCP
// deadlines yield idle reservations fairly; the keeper still owns the original
// LISTEN OFD. This view is an explicit duplicate, closed/joined before Export,
// rather than an unregistered net.FileListener left beside an active keeper view.
// origin: hot-update S6 plan §6.1 / §6.2 / T21
func endpointAcceptView(k listenerimage.Keeper) (net.Listener, error) {
	_, res, err := k.Export()
	if err != nil {
		return nil, err
	}
	return net.FileListener(res.Files["listener"])
}
func pauseEndpointListener(s *Server, e *flowEndpoint, d controlbudget.Deadline) error {
	s.mu.Lock()
	if e.phase == EndpointPhaseClosing || e.phase == EndpointPhaseClosed {
		s.mu.Unlock()
		return nil
	}
	ln, k := e.listener, e.keeper
	s.mu.Unlock()
	if k == nil && ln != nil {
		var err error
		k, err = listenerimage.Capture(ln, endpointListenerRef(e.id))
		// Capture can return a real owner with a close error. Register it first;
		// recovery must never lose its original OFD merely because err is nonnil.
		if k != nil {
			s.mu.Lock()
			e.keeper = k
			s.mu.Unlock()
		}
		if err != nil {
			return err
		}
	} else if ln != nil {
		_ = ln.Close()
	}
	if k != nil {
		return k.Pause(d)
	}
	return nil
}

// Partial failure retains the fenced owner and all completion channels. Recovery
// may wait with a new caller-supplied deadline, but cannot manufacture a joined
// receipt or reactivate a worker that is still mutating the old generation.
// origin: hot-update S6 plan §4.2 / §6.2 / T21–T23
type serverPausedCut struct {
	mu        sync.Mutex
	server    *Server
	flows     *flow.Frozen
	dormant   *serverImportedImage
	endpoints []*flowEndpoint
	joins     []<-chan struct{}
	released  bool
	complete  bool
}

func freezeServerRuntime(s *Server, d controlbudget.Deadline) (serverRuntimeCut, error) {
	until, err := serverPauseDeadline(d)
	if err != nil {
		return nil, err
	}
	if s == nil {
		return nil, errFlowState
	}
	if !flowLockBefore(&s.mu, until) {
		return nil, os.ErrDeadlineExceeded
	}
	// Active legacy listeners/pumps are intentionally ineligible. Refuse before
	// changing admission; closing them to make the census empty is data loss.
	if s.closed || s.imagePaused {
		s.mu.Unlock()
		return nil, errFlowState
	}
	// Closing bridges remain in the census after their table row disappears.
	// They cannot be made image-eligible by destroying their original sockets.
	// origin: hot-update S6 plan §6.2 / T23
	if len(s.sessions) != 0 || s.legacyWorkers != 0 {
		s.mu.Unlock()
		return nil, errServerLegacyBusy
	}
	cut := &serverPausedCut{server: s}
	s.imagePaused = true
	for _, e := range serverEndpointsLocked(s) {
		e.paused = true
		if e.pause == nil {
			e.pause = make(chan struct{})
		}
		close(e.pause)
		stopEndpointTimerLocked(s, e)
		cut.endpoints = append(cut.endpoints, e)
		cut.joins = append(cut.joins, e.done)
	}
	for work := range s.callbacks {
		cut.joins = append(cut.joins, work.done)
	}
	var conns []net.Conn
	for conn, ch := range s.controlConns {
		conns = append(conns, conn)
		cut.joins = append(cut.joins, ch)
	}
	// Track both attached carriers and detached monitors. An attachment pointer
	// can disappear before the monitor finishes its last detach/cleanup decision.
	carriers := make(map[*flowCarrier]bool)
	for _, e := range cut.endpoints {
		if e.attachment != nil {
			carriers[e.attachment] = true
		}
	}
	for a, ch := range s.monitors {
		carriers[a] = true
		cut.joins = append(cut.joins, ch)
	}
	ln, k, acceptDone := s.ln, s.controlKeeper, s.acceptDone

	s.mu.Unlock()
	// Pause public acceptance without terminal stop. Capture closes the old native
	// view only after acquiring the keeper duplicate, preserving kernel backlog.
	var pauseErr error
	for _, e := range cut.endpoints {
		pauseErr = errors.Join(pauseErr, pauseEndpointListener(s, e, d))
	}
	if ln != nil {
		if k == nil {
			// tls.Listener intentionally hides its underlying listener. Start records
			// the raw listener independently so Capture never pretends TLS is native.
			s.mu.Lock()
			raw := s.controlRaw
			s.mu.Unlock()
			k, err = listenerimage.Capture(raw, "tunnel-control")
			if k != nil {
				s.mu.Lock()
				s.controlKeeper = k
				s.mu.Unlock()
			}
			pauseErr = errors.Join(pauseErr, err)
		} else {
			_ = ln.Close()
		}
		if k != nil {
			if acceptDone != nil {
				cut.joins = append(cut.joins, acceptDone)
			}
			pauseErr = errors.Join(pauseErr, k.Pause(d))
		}
	}
	// Underlying TLS close precedes worker joins and the raw manager safe point.
	// Stream deadlines alone cannot cancel yamux's shared window-update writer.
	// origin: hot-update S6 plan §6.2 / T23
	for _, conn := range conns {
		closeServerTransport(conn)
	}
	for a := range carriers {
		a.Close()
	}
	for _, ch := range cut.joins {
		if err = serverPauseWait(ch, d); err != nil {
			return cut, err
		}
	}
	// A pre-fence REGISTER may have installed its monitor after our first table
	// snapshot. Handler completion closes that launch window; take the final
	// monitor/callback census afterwards, then join before freezing raw workers.
	s.mu.Lock()
	for a, ch := range s.monitors {
		carriers[a] = true
		cut.joins = append(cut.joins, ch)
	}
	for work := range s.callbacks {
		cut.joins = append(cut.joins, work.done)
	}
	s.mu.Unlock()
	for a := range carriers {
		a.Close()
	}
	for _, ch := range cut.joins {
		if err = serverPauseWait(ch, d); err != nil {
			return cut, err
		}
	}
	if pauseErr != nil {
		return cut, pauseErr
	}
	// launch is fenced by a.close before Wait, so the wait cannot race a new Add.
	for a := range carriers {
		joined := make(chan struct{})
		cut.joins = append(cut.joins, joined)
		go func() { a.Join(); close(joined) }()
		if err = serverPauseWait(joined, d); err != nil {
			return cut, err
		}
	}
	until, err = serverPauseDeadline(d)
	if err != nil {
		return cut, err
	}
	if s.flowState != nil {
		cut.flows, err = s.flowState.manager.Freeze(until)
		// An ACTIVE store-constructor failure can precede tunnel activation.
		// Its imported manager is already frozen, with no pumps to join. Keep
		// that exact pristine flow closure available to the new current cut.
		// origin: hot-update S6 plan §4.2; internal review rv-fd-3
		if errors.Is(err, flow.ErrFrozen) && s.importedImage != nil {
			cut.dormant = s.importedImage
			err = nil
		}
	}
	if err != nil {
		return cut, err
	}
	cut.complete = true
	return cut, nil
}
func serverEndpointsLocked(s *Server) []*flowEndpoint {
	var endpoints []*flowEndpoint
	if s.flowState != nil {
		for _, e := range s.flowState.endpoints {
			endpoints = append(endpoints, e)
		}
	}
	sort.Slice(endpoints, func(i, j int) bool { return endpoints[i].allocation.Port < endpoints[j].allocation.Port })
	return endpoints
}
func (cut *serverPausedCut) flowExport() (flow.Image, *sealedstate.Snapshot, error) {
	cut.mu.Lock()
	defer cut.mu.Unlock()
	if cut.released || !cut.complete || cut.flows == nil && cut.dormant == nil {
		return flow.Image{}, nil, errFlowState
	}
	if cut.dormant != nil {
		var resources []handoff.Resource
		for _, res := range cut.dormant.resources {
			if res.ID == cut.dormant.image.Flows.StateRef || strings.HasPrefix(res.ID, "flow.") {
				resources = append(resources, res)
			}
		}
		return cut.dormant.image.Flows, &sealedstate.Snapshot{Resources: resources}, nil
	}
	return cut.flows.Export()
}
func (cut *serverPausedCut) thaw(d controlbudget.Deadline) error {
	cut.mu.Lock()
	defer cut.mu.Unlock()
	if cut.released {
		return errFlowState
	}
	for _, ch := range cut.joins {
		if err := serverPauseWait(ch, d); err != nil {
			return err
		}
	}
	if _, err := serverPauseDeadline(d); err != nil {
		return err
	}
	s := cut.server
	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	if closed {
		return errFlowState
	}
	// Construct all replacement views before restarting anything. A failure leaves
	// the complete original graph fenced and retryable, with no partial activation.
	views := make(map[*flowEndpoint]net.Listener)
	var control net.Listener
	var err error
	defer func() {
		if err != nil {
			for _, ln := range views {
				_ = ln.Close()
			}
			if control != nil {
				_ = control.Close()
			}
		}
	}()
	s.mu.Lock()
	endpoints := serverEndpointsLocked(s)
	k := s.controlKeeper
	s.mu.Unlock()
	for _, e := range endpoints {
		s.mu.Lock()
		live := endpointLiveLocked(s, e)
		s.mu.Unlock()
		if !live {
			continue
		}
		if e.keeper != nil {
			views[e], err = endpointAcceptView(e.keeper)
			if err != nil {
				s.mu.Lock()
				live = endpointLiveLocked(s, e)
				s.mu.Unlock()
				if live {
					return err
				}
				err = nil
			}
		}
	}
	if k != nil {
		control, err = endpointAcceptView(k)
		if err != nil {
			return err
		}
		var cfg *tls.Config
		cfg, err = s.serverTLSConfig()
		if err != nil {
			return err
		}
		control = tls.NewListener(control, cfg)
	}
	if cut.dormant != nil {
		cut.dormant.mu.Lock()
		cut.dormant.active = true
		cut.dormant.mu.Unlock()
		s.mu.Lock()
		state := takeServerImportStateLocked(s)
		s.mu.Unlock()
		if state != nil {
			if err = state.Close(); err != nil {
				return err
			}
		}
		if err = s.flowState.manager.ActivateRaw(); err != nil {
			return err
		}
	}
	if cut.flows != nil {
		err = cut.flows.Thaw()
		if err != nil {
			return err
		}
	}
	s.mu.Lock()
	// Terminal Close can win while replacement views are constructed. A new
	// Accept owner would outlive its stop snapshot and keep the LISTEN alias.
	// origin: hot-update S6 plan §4.2 / T52; internal review rv-fd-3
	if s.closed {
		s.mu.Unlock()
		err = errFlowState
		return err
	}
	if s.runCtx == nil {
		s.runCtx = context.Background() // ctx-root: imported control handlers live until Server.Close joins them.
	}
	for _, e := range endpoints {
		// Revocation is irreversible even in a failed cut. Never restart a removed
		// allocation, or clear its Fired/CleanupPending decision during rollback.
		if !endpointLiveLocked(s, e) {
			if ln := views[e]; ln != nil {
				_ = ln.Close()
			}
			continue
		}
		if e.pendingRaw != nil {
			err = rawsocket.Release(e.pendingRaw)
			if err != nil {
				s.mu.Unlock()
				return err
			}
		}
		e.paused = false
		e.pause = make(chan struct{})
		e.done = make(chan struct{})
		if ln := views[e]; ln != nil {
			e.listener = ln
		}
		if e.attachment == nil {
			armDetachLocked(s, e, time.Now())
		}
		if e.listener != nil {
			go endpointAccept(s, e)
		} else {
			close(e.done)
		}
	}
	s.imagePaused = false
	if control != nil {
		s.ln = control
		s.acceptDone = make(chan struct{})
		done := s.acceptDone
		s.wg.Add(1)
		go func() { defer s.wg.Done(); defer close(done); s.acceptLoop(s.runCtx, control) }()
	}
	s.mu.Unlock()
	cut.released = true
	return nil
}
func (cut *serverPausedCut) discard() error {
	cut.mu.Lock()
	defer cut.mu.Unlock()
	if cut.released {
		return errFlowState
	}
	// Discard retires old wrappers after their duplicates have been handed on.
	// Close the frozen manager first: terminal Close then cannot submit a revoke
	// through its old journal and SHUT_RDWR the new owner's same business OFD.
	// It also releases the cut-owned sealed FD, which Server.Close cannot see.
	// origin: hot-update S6 plan §4.2 / §6.2 / T23
	if cut.flows != nil {
		if err := cut.flows.Discard(); err != nil {
			return err
		}
	}
	if cut.dormant != nil {
		resources, err := cut.server.flowState.manager.Detach()
		if err != nil {
			return err
		}
		for _, res := range resources {
			for _, file := range res.Files {
				_ = file.Close()
			}
		}
	}
	cut.server.mu.Lock()
	cut.server.retiringImage = cut.complete
	cut.server.mu.Unlock()
	cut.server.Close()
	cut.released = true
	return nil
}

// snapshot is a runtime projection, not another codec. P07 alone validates the
// broker-only descriptor vocabulary and encodes these records into its sealed FD.
// Original resource files are borrowed while cut.mu and admission remain fenced.
// origin: hot-update S6 plan §2.2 / §6.2 / T21–T23
func (cut *serverPausedCut) snapshot() (serverStateSnapshot, error) {
	cut.mu.Lock()
	defer cut.mu.Unlock()
	out := serverStateSnapshot{}
	if cut.released || !cut.complete {
		return out, errFlowState
	}
	s := cut.server
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || !s.imagePaused {
		return out, errFlowState
	}
	for _, ch := range cut.joins {
		select {
		case <-ch:
		default:
			return out, errFlowState
		}
	}
	if s.flowState != nil && cut.flows == nil && cut.dormant == nil {
		return out, errFlowState
	}
	// The safe point supplies files; the single codec projection supplies every
	// record and field. A parallel runtime encoder made real cuts unreadable.
	// origin: hot-update S6 plan §2.2 / §6.2; internal review rv-fd-1
	for _, e := range serverEndpointsLocked(s) {
		var res handoff.Resource
		if e.keeper != nil {
			_, borrowed, err := e.keeper.Export()
			if err != nil {
				return out, err
			}
			res = borrowed
		}
		if e.pendingRaw != nil {
			if err := rawsocket.Hold(e.pendingRaw); err != nil {
				return out, err
			}
			file, err := e.pendingRaw.ExportFile()
			if err != nil {
				return out, err
			}
			if res.ID == "" {
				res = handoff.Resource{ID: endpointListenerRef(e.id), Kind: "raw-socket-v1", Files: map[string]*os.File{}}
			}
			res.Files["raw"] = file
		}
		if res.ID != "" {
			out.Resources = append(out.Resources, res)
		}
	}
	if s.controlKeeper != nil {
		_, res, err := s.controlKeeper.Export()
		if err != nil {
			return out, err
		}
		out.Resources = append(out.Resources, res)
	}
	return snapshotServerLocked(s, out.Resources)
}

func serverWaiterIDs(waiters []*flowEndpoint) []byte {
	var ids []byte
	for _, e := range waiters {
		ids = append(ids, e.id[:]...)
	}
	return ids
}

var _ serverRuntimeCut = (*serverPausedCut)(nil)

// Legacy bridges are deliberately refused by planned image admission, including
// the closing interval after their session table row has been removed. Their
// parent loop/handler stays in wg until all child launches have been accounted.
// origin: hot-update S6 plan §6.2 / T23
func launchLegacyServerWorker(s *Server, fn func()) {
	s.mu.Lock()
	s.legacyWorkers++
	s.wg.Add(1)
	s.mu.Unlock()
	go func() {
		defer s.wg.Done()
		defer func() { s.mu.Lock(); s.legacyWorkers--; s.mu.Unlock() }()
		fn()
	}()
}
