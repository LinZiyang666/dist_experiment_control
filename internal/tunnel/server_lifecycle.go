package tunnel

import (
	"context"
	"encoding/hex"
	"errors"
	"net"

	"github.com/LinZiyang666/tether/internal/flow"
)

// publicAcceptLoop pumps every TCP connection on the public port into
// a brand-new yamux stream to the agent. The agent will write/read
// bytes on that stream from/to its local port.
func (s *Server) publicAcceptLoop(ctx context.Context, sess *serverSession) {
	defer func() {
		s.mu.Lock()
		if cur, ok := s.sessions[sess.publicPort]; ok && cur == sess {
			delete(s.sessions, sess.publicPort)
		}
		s.mu.Unlock()
		_ = sess.listener.Close()
		_ = sess.yamuxSess.Close()
		_ = sess.rawConn.Close()
	}()
	for {
		pubConn, err := sess.listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			s.logger.Warn("tunnel: public accept", "err", err, "port", sess.publicPort)
			return
		}
		launchLegacyServerWorker(s, func() { s.bridgePublicToYamux(pubConn, sess) })
	}
}

func (s *Server) bridgePublicToYamux(pubConn net.Conn, sess *serverSession) {
	stream, err := sess.yamuxSess.Open()
	if err != nil {
		s.logger.Warn("tunnel: yamux Open", "err", err)
		_ = pubConn.Close()
		return
	}
	bridge(pubConn, stream)
}

// CloseProxy tears down the single public-port session for `port` (listener +
// yamux + control conn) immediately, independent of the agent or NATS. Used by
// the P13 `proxy off` authoritative kill switch so the public exit dies the
// moment the owner disables, not whenever the agent next processes the OFF.
// No-op (returns false) if no session owns that port.
func (s *Server) CloseProxy(port int) bool {
	return revokeServerImage(s, func(e *flowEndpoint) bool { return e.allocation.Port == port }, func() bool {
		return closeServerProxy(s, port)
	})
}

func closeServerProxy(s *Server, port int) bool {
	s.mu.Lock()
	// Advance the kill generation FIRST, so any in-flight handleAgent that
	// already passed the token lookup for this port aborts its install (F1).
	s.killGen[port]++
	sess, ok := s.sessions[port]
	if ok {
		delete(s.sessions, port)
	}
	// A retained v1 Endpoint owns the allocation even after its carrier left.
	// Preserve that exact hard-revoke fence in the private image; otherwise a
	// reconnect can create a fresh Endpoint for the just-closed allocation.
	// origin: hot-update S6 plan §6.2 / T25
	if s.flowState != nil {
		if e := s.flowState.endpoints[port]; e != nil {
			s.ensureAllocationFenceMapsLocked()
			key := allocationKey(e.allocation)
			s.killGenAllocation[key]++
			s.closedAllocation[key] = true
		}
	}
	s.mu.Unlock()

	flowPorts := closeMatchingEndpoints(s, func(e *flowEndpoint) bool { return e.allocation.Port == port })
	if !ok {
		return len(flowPorts) > 0
	}
	closeServerSession(sess)
	return true
}

// CloseProxyIf tears down the public listener only if the currently installed
// session still matches the supplied allocation identity. Even when no listener
// is installed, it bumps an allocation-scoped generation so an old in-flight
// REGISTER for that exact token cannot install after the caller freed/revoked
// the allocation. The fence is not bare-port scoped: a stale close for token A
// must not deny or close a new allocation on the same public port with token B.
func (s *Server) CloseProxyIf(info SessionInfo) bool {
	return revokeServerImage(s, func(e *flowEndpoint) bool {
		return e.allocation.Port == info.PublicPort && e.allocation.SID == info.SID && e.allocation.NID == info.NID && e.allocation.TokenHash == info.TokenHash
	}, func() bool { return closeServerProxyIf(s, info) })
}

func closeServerProxyIf(s *Server, info SessionInfo) bool {
	if info.PublicPort <= 0 {
		return false
	}
	s.mu.Lock()
	s.ensureAllocationFenceMapsLocked()
	key := sessionFenceKeyFromInfo(info)
	// A same-token rehome advances epoch. Check the installed owner's full
	// identity before poisoning its shared allocation fence with an old close.
	// origin: hot-update S6 plan §6.2 / T26
	if s.flowState != nil {
		if e := s.flowState.endpoints[info.PublicPort]; e != nil && allocationKey(e.allocation) == key && e.allocation.Epoch != info.Epoch {
			s.mu.Unlock()
			return false
		}
	}
	s.killGenAllocation[key]++
	s.closedAllocation[key] = true
	sess, ok := s.sessions[info.PublicPort]
	if !ok {
		s.maybePruneAllocationLocked(key)
		s.mu.Unlock()
		return len(closeMatchingEndpoints(s, func(e *flowEndpoint) bool {
			return allocationKey(e.allocation) == key && e.allocation.Epoch == info.Epoch
		})) > 0
	}
	if sess.sid != info.SID || sess.nid != info.NID || sess.tokenHash != info.TokenHash || sess.epoch != info.Epoch {
		s.maybePruneAllocationLocked(key)
		s.mu.Unlock()
		return len(closeMatchingEndpoints(s, func(e *flowEndpoint) bool {
			return allocationKey(e.allocation) == key && e.allocation.Epoch == info.Epoch
		})) > 0
	}
	delete(s.sessions, info.PublicPort)
	s.maybePruneAllocationLocked(key)
	s.mu.Unlock()

	closeMatchingEndpoints(s, func(e *flowEndpoint) bool {
		return allocationKey(e.allocation) == key && e.allocation.Epoch == info.Epoch
	})
	closeServerSession(sess)
	return true
}

// SessionInfos snapshots every installed public listener identity.
func (s *Server) SessionInfos() []SessionInfo {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]SessionInfo, 0, len(s.sessions))
	for _, sess := range s.sessions {
		out = append(out, SessionInfo{
			PublicPort: sess.publicPort,
			SID:        sess.sid,
			NID:        sess.nid,
			TokenHash:  sess.tokenHash,
			Epoch:      sess.epoch,
		})
	}
	if s.flowState != nil {
		for _, e := range s.flowState.endpoints {
			if e.phase != EndpointPhaseClosed {
				out = append(out, endpointInfo(e))
			}
		}
	}
	return out
}

// ServerAllocationReleased proves an imported, identity-bound hard revoke has
// no remaining owner or in-flight install. Absence from SessionInfos alone is
// not retirement: disconnects and live closing aliases also remove listeners.
// origin: hot-update S8 plan §4.2; S8 internal review P13-COHORT F2
func ServerAllocationReleased(s *Server, info SessionInfo) bool {
	if s == nil || info.PublicPort <= 0 || info.SID == "" || info.NID == "" || info.TokenHash == "" {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := sessionFenceKeyFor(info.PublicPort, info.SID, info.NID, info.TokenHash)
	// Retention treats an unreadable fence conservatively as still pinned. A
	// positive retirement proof needs a decoded, explicit sealed close instead.
	sealed := false
	if s.importedImage != nil {
		for _, record := range s.importedImage.state.Records {
			if record.Kind == "fences" {
				for _, field := range record.Fields {
					if field.Name == "closed_allocation" {
						values, err := serverMapRead(field.Bytes)
						sealed = err == nil && values[serverAllocationKey(key)] == 1
					}
				}
			}
		}
	}
	return s.closedAllocation[key] && sealed &&
		s.inflightByAllocation[key] == 0 && !endpointRefLocked(s, info.SID, &key) && s.sessions[info.PublicPort] == nil
}

func closeServerSession(sess *serverSession) {
	if sess == nil {
		return
	}
	sess.cancel()
	if sess.listener != nil {
		_ = sess.listener.Close()
	}
	if sess.yamuxSess != nil {
		_ = sess.yamuxSess.Close()
	}
	if sess.rawConn != nil {
		_ = sess.rawConn.Close()
	}
}

// ForgetSession closes any remaining listeners for sid and PRUNES its kill-gen
// bookkeeping (self-review: killGenSession would otherwise grow unbounded across
// session lifecycles). Safe to call ONLY when the session is permanently gone
// (finalizeSessionRm, after dropSessionRows) — once its allocation rows are
// deleted, tunnelTokenLookup denies every REGISTER for sid regardless of the
// pruned generation, so a late in-flight REGISTER can never install.
func (s *Server) ForgetSession(sid string) {
	s.mu.Lock()
	// round-6 F4: BUMP the session fence (not merely delete it) so an authorized
	// REGISTER paused between snapshot and install observes a changed generation
	// and aborts — even after its allocation rows are gone. Mark the session
	// forgotten; the entry is only pruned once no handler is in-flight, so the
	// fence can't be lost from under a paused handler.
	s.killGenSession[sid]++
	s.forgotten[sid] = true
	var victims []*serverSession
	for port, sess := range s.sessions {
		if sess.sid == sid {
			s.killGen[port]++
			victims = append(victims, sess)
			delete(s.sessions, port)
		}
	}
	s.maybePruneSessionLocked(sid)
	s.mu.Unlock()

	closeMatchingEndpoints(s, func(e *flowEndpoint) bool { return e.allocation.SID == sid })
	for _, sess := range victims {
		closeServerSession(sess)
	}
}

// fenceSnap is the set of kill generations a REGISTER handler captured before
// it started installing. Comparing the whole struct is what makes the fence
// total: adding a fourth dimension means adding a field here, and every
// comparison site picks it up automatically.
//
// Batch-A A9. This replaced a snapshot of three locals plus TWO hand-written
// `||` chains that repeated the same three comparisons verbatim. The three
// dimensions did not arrive together — killGen[port] came from round-2 F1,
// killGenSession[sid] from round-5 F1, killGenAllocation[fenceKey] from
// round-6 F4 — i.e. three consecutive external reviews each found the fence
// incomplete in a new direction.
//
// A fourth is entirely plausible (per-nid, for multi-home). Under the old shape
// that meant editing three places, and OMITTING one of the two chains COMPILES
// CLEANLY. The consequence of that silent miss is not a crash: an exit that was
// already killed gets resurrected by a REGISTER racing the kill, and re-binds
// the public port. A data-plane hole, in precisely the spot three reviews in a
// row already had to patch.
type fenceSnap struct {
	port  int64
	sess  int64
	alloc int64
}

// fenceSnapLocked captures the current fence. Caller holds s.mu.
func (s *Server) fenceSnapLocked(port int, sid string, fenceKey sessionFenceKey) fenceSnap {
	return fenceSnap{
		port:  s.killGen[port],
		sess:  s.killGenSession[sid],
		alloc: s.killGenAllocation[fenceKey],
	}
}

// fenceChangedLocked reports whether any fence dimension moved since snap.
// Caller holds s.mu.
//
// s.closed is deliberately NOT folded in: it is server lifecycle, not a fence
// dimension, and call sites read better spelling out `s.closed || changed`.
func (s *Server) fenceChangedLocked(snap fenceSnap, port int, sid string, fenceKey sessionFenceKey) bool {
	return s.fenceSnapLocked(port, sid, fenceKey) != snap
}

// releaseInflightLocked drops one in-flight REGISTER's bookkeeping, DELETING the
// key when the count reaches zero. Caller holds s.mu.
//
// origin: prerelease audit broker-dataplane/BDP-F2 (reproduced by the main process:
// 200 unauthenticated REGISTERs left 200 permanent entries in each map). `m[k]--`
// writes 0 back, it does not remove the key, and the two pruners below can only fire
// for a sid ForgetSession has marked forgotten or an allocation CloseProxyIf has
// closed — neither is ever true for a session that never existed. So every denied
// probe from a stranger used to leave two entries behind for the life of the
// process.
//
// Deleting at zero is EXACTLY equivalent to the old bookkeeping for the fence: both
// pruners read a missing key as 0, which still satisfies their `<= 0` test, so the
// round-2 / round-5 / round-6 fence invariants are untouched. Order matters — decrement
// (or delete) first, then let the pruners run.
func (s *Server) releaseInflightLocked(sid string, key sessionFenceKey) {
	if n := s.inflightBySID[sid] - 1; n > 0 {
		s.inflightBySID[sid] = n
	} else {
		delete(s.inflightBySID, sid)
	}
	if n := s.inflightByAllocation[key] - 1; n > 0 {
		s.inflightByAllocation[key] = n
	} else {
		delete(s.inflightByAllocation, key)
	}
	s.maybePruneSessionLocked(sid)
	s.maybePruneAllocationLocked(key)
}

// maybePruneSessionLocked deletes a forgotten session's kill-gen bookkeeping
// once no REGISTER handler is in-flight for it (so the fence is never removed
// from under a paused handler). Caller holds s.mu.
func (s *Server) maybePruneSessionLocked(sid string) {
	if s.forgotten[sid] && s.inflightBySID[sid] <= 0 && !endpointRefLocked(s, sid, nil) {
		delete(s.killGenSession, sid)
		delete(s.inflightBySID, sid)
		delete(s.forgotten, sid)
	}
}

func (s *Server) ensureAllocationFenceMapsLocked() {
	if s.killGenAllocation == nil {
		s.killGenAllocation = map[sessionFenceKey]int64{}
	}
	if s.inflightByAllocation == nil {
		s.inflightByAllocation = map[sessionFenceKey]int{}
	}
	if s.closedAllocation == nil {
		s.closedAllocation = map[sessionFenceKey]bool{}
	}
}

func (s *Server) maybePruneAllocationLocked(key sessionFenceKey) {
	// A paused image still owns the removed allocation's exported aliases and
	// its late revoke decision. Keep the tombstone until the final export;
	// absence from the live Endpoint table alone cannot prove retirement.
	// origin: hot-update S6 plan §6.2 / T25
	if !s.imagePaused && !serverImportedRevokeRetained(s, key) && s.closedAllocation[key] && s.inflightByAllocation[key] <= 0 && !endpointRefLocked(s, key.sid, &key) {
		delete(s.killGenAllocation, key)
		delete(s.inflightByAllocation, key)
		delete(s.closedAllocation, key)
	}
}

// Imported revokes are sealed decisions, even after their last raw alias was
// removed. A rejected INITIAL/RESUME must not prune the decision and authorize
// the same allocation on the next retry. Ordinary live-close pruning is intact.
// origin: hot-update S6 plan §6.2 / T25
func serverImportedRevokeRetained(s *Server, key sessionFenceKey) bool {
	if s.importedImage == nil {
		return false
	}
	for _, record := range s.importedImage.state.Records {
		if record.Kind != "fences" {
			continue
		}
		for _, field := range record.Fields {
			if field.Name == "closed_allocation" {
				values, err := serverMapRead(field.Bytes)
				return err != nil || values[serverAllocationKey(key)] == 1
			}
		}
	}
	return false
}

// CloseSession closes EVERY installed public listener owned by sid and bumps
// each port's kill generation, WITHOUT consulting any database (round-4 F3).
// `proxy off` calls it so the data plane dies even if the broker's subsequent
// allocation-store query fails — the broker-side immediate kill must not depend
// on a post-switch DB read. Returns the ports it closed.
func (s *Server) CloseSession(sid string) []int {
	s.mu.Lock()
	// Bump the SESSION kill generation FIRST and unconditionally, so an in-flight
	// REGISTER that already passed auth but hasn't inserted its session yet
	// aborts at the install check (round-5 F1) — not just the listeners already
	// in s.sessions.
	s.killGenSession[sid]++
	var victims []*serverSession
	var ports []int
	for port, sess := range s.sessions {
		if sess.sid == sid {
			s.killGen[port]++
			victims = append(victims, sess)
			ports = append(ports, port)
			delete(s.sessions, port)
		}
	}
	s.mu.Unlock()

	for _, sess := range victims {
		closeServerSession(sess)
	}
	ports = append(ports, closeMatchingEndpoints(s, func(e *flowEndpoint) bool { return e.allocation.SID == sid })...)
	return ports
}

// Close releases the control listener and every active agent session.
// Idempotent. After Close returns, in-flight handleAgent goroutines
// have all observed s.closed=true and either exited or rolled back
// their would-be insert; the bound public ports are released.
func (s *Server) Close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	ln := s.ln
	keeper := s.controlKeeper
	state := takeServerImportStateLocked(s)
	var conns []net.Conn
	for conn := range s.controlConns {
		conns = append(conns, conn)
	}
	if s.lifetimeDone != nil {
		s.lifetimeOnce.Do(func() { close(s.lifetimeDone) })
	}
	// Snapshot sessions under lock; close them OUTSIDE the lock so
	// a slow TLS close_notify or kernel listener teardown can't
	// stall every other handleAgent waiting for s.mu (audit F9).
	snap := make([]*serverSession, 0, len(s.sessions))
	for _, sess := range s.sessions {
		snap = append(snap, sess)
	}
	s.sessions = map[int]*serverSession{}
	s.mu.Unlock()

	if ln != nil {
		_ = ln.Close()
	}
	for _, conn := range conns {
		closeServerTransport(conn)
	}
	if keeper != nil {
		_ = keeper.Close()
	}
	if state != nil {
		_ = state.Close()
	}
	for _, sess := range snap {
		closeServerSession(sess)
	}
	closeMatchingEndpoints(s, func(*flowEndpoint) bool { return true })
	if s.flowState != nil {
		_ = s.flowState.manager.Close()
	}
	// Wait for in-flight handleAgent goroutines to drain so the
	// caller of Close knows the bound public ports are released
	// (vs. just "scheduled to close"). Bound by the per-session
	// cancellation above; can't run forever.
	s.wg.Wait()
	waitServerBackground(s)
}

// SetServerImageRevoke binds the joined safe point to the broker cut latch.
// It is process wiring and must never enter the private-state codec.
// origin: hot-update S6 plan §6.2 / T25
func SetServerImageRevoke(s *Server, revoke func([]string, func() error) error) error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if revoke != nil && (!s.imagePaused || len(s.monitors) != 0 || len(s.callbacks) != 0) {
		return errors.New("tunnel: image revoke requires joined safe point")
	}
	s.imageRevoke = revoke
	return nil
}

func revokeServerImage(s *Server, match func(*flowEndpoint) bool, closeOwner func() bool) bool {
	s.mu.Lock()
	revoke := s.imageRevoke
	if !s.imagePaused || revoke == nil {
		s.mu.Unlock()
		return closeOwner()
	}
	ids := []string{"broker.tunnel.fences"}
	if s.flowState != nil {
		for _, e := range s.flowState.endpoints {
			if match(e) {
				ids = append(ids, endpointListenerRef(e.id))
				for _, r := range flow.States(s.flowState.manager, e.id) {
					ids = append(ids, "flow."+hex.EncodeToString(r.ID[:]))
				}
			}
		}
	}
	s.mu.Unlock()
	effective := false
	// Freeze already joined these endpoints' pumps, monitors, callbacks and
	// accepts. Their terminal closes cannot wait for a callback taking this
	// latch, which would otherwise invert cut -> server during hard revoke.
	// origin: hot-update S6 plan §6.2 / T25
	if err := revoke(ids, func() error { effective = closeOwner(); return nil }); err != nil {
		s.logger.Error("tunnel: image hard revoke refused")
		return false
	}
	return effective
}
