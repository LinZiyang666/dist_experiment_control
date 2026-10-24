package tunnel

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"os"
	"time"

	"github.com/LinZiyang666/tether/internal/drillobserve"
	"github.com/LinZiyang666/tether/internal/flow"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/tunnel/transport"
	"github.com/hashicorp/yamux"
)

// A fresh v1 allocation can reach its home before the replica applies it.
// Retry only typed non-authoritative refusals inside one existing RESUME
// attempt. Each attempt runs a fresh REGISTER; terminal denial and network
// failure still return immediately. The successful session keeps Start's
// lifetime, because dialAndRegister stops its handshake cancellation hook.
// origin: hot-update S5 plan §4.1 / §4.3 / T13 / DRILL-RUN 75.C
func dialInitialFlow(ctx context.Context, c *Client, port int, token, addr string, epoch int64, pins proto.CertPins) (net.Conn, *yamux.Session, error) {
	if c.flowState == nil {
		return c.dialAndRegister(ctx, port, token, addr, epoch, pins)
	}
	budget, cancel := context.WithTimeout(ctx, ResumeAttempt)
	defer cancel()
	for {
		conn, mux, err := c.dialAndRegister(budget, port, token, addr, epoch, pins)
		var deny *DenyError
		if err == nil || !errors.As(err, &deny) || (deny.Reason != proto.ReasonHomeCatchingUp && deny.Reason != "try_again") {
			return conn, mux, err
		}
		timer := time.NewTimer(20 * time.Millisecond)
		select {
		case <-budget.Done():
			timer.Stop()
			return nil, nil, err
		case <-timer.C:
		}
	}
}

// dialAndRegister performs ONE TLS dial + REGISTER handshake + yamux client
// setup. ctx cancels both the dial (DialContext) AND an in-flight handshake
// read/write (a watcher closes the conn on ctx.Done) so Close/Start-cancel
// reap a blocked redial promptly instead of waiting out the 5s read deadline.
// A broker DENY returns *DenyError (wraps ErrRegisterDenied); all transport
// failures return plain (transient) errors.
//
// TLS wraps the underlying TCP so REGISTER + token cannot be observed by a
// passive listener (architecture F.5); v1 InsecureSkipVerify is sufficient
// for the passive-eavesdrop threat model (see clientTLSConfig).
func (c *Client) dialAndRegister(ctx context.Context, publicPort int, token, brokerAddr string, epoch int64, certPins proto.CertPins) (net.Conn, *yamux.Session, error) {
	// Restored recovery can dial without OpenHome; it pins the same mode.
	// origin: hot-update S5 plan §4.1 / §7.1 / P13-REGRESS
	c.mu.Lock()
	c.flowConfigUsed = true
	c.mu.Unlock()
	if st := c.flowState; st != nil {
		release, err := lockClientCarrier(ctx, c, publicPort)
		if err != nil {
			return nil, nil, err
		}
		defer release()
		st.mu.Lock()
		e := st.endpoints[publicPort]
		if e != nil && e.token == token && e.allocation.Epoch == epoch &&
			effectiveTunnelAddress(c, e.brokerAddr) == effectiveTunnelAddress(c, brokerAddr) && e.pins == certPins {
			a := e.attachment
			if a != nil && !a.Mux.IsClosed() {
				select {
				case <-a.Stop:
				default:
					st.mu.Unlock()
					return a.Conn, a.Mux, nil
				}
			}
		}
		st.mu.Unlock()
	}
	// D6 §7.5: dial THIS expose's home addr; "" falls back to the single
	// configured Client.brokerAddr (the N=1 / --tunnel-addr path). The TLS config
	// pins the home cert when pins are present, else InsecureSkipVerify (N=1).
	addr := brokerAddr
	if addr == "" {
		addr = c.brokerAddr
	}
	cfg := clientTLSConfigPinned(certPins)
	timeout := 5 * time.Second
	var flowGeneration uint64
	retained := false
	if c.flowState != nil {
		c.flowState.mu.Lock()
		frozen := c.flowState.frozen
		flowGeneration = c.flowState.generation
		retained = c.flowState.endpoints[publicPort] != nil
		c.flowState.admission++
		c.flowState.mu.Unlock()
		defer func() { c.flowState.mu.Lock(); c.flowState.admission--; c.flowState.mu.Unlock() }()
		if frozen {
			return nil, nil, errFlowState
		}
		timeout = ResumeAttempt
	}
	// The fallback shares the original pin checks and deadline, and is allowed
	// only before REGISTER and before any retained v1 endpoint responsibility.
	// origin: hot-update S5 plan §4.1 / T15
	cfg = flowClientTLSConfig(cfg, c.flowState != nil)
	dialer := &net.Dialer{Timeout: timeout}
	conn, err := dialFlowTLS(ctx, "tcp", addr, dialer, cfg, retained)
	if err != nil {
		return nil, nil, fmt.Errorf("tunnel client: dial broker (TLS): %w", err)
	}
	// Close the conn on ctx.Done until the handshake completes so a blocked
	// Write/ReadString is interrupted by Close/Start-cancel/the caller's open
	// deadline. AfterFunc + stop is deterministic where a goroutine selecting on
	// {ctx.Done, hsDone} was not: since OpenHome cancels its handshake ctx on
	// EVERY return (external review F1), a watcher first scheduled after both
	// channels were closed would pick a branch at random and could close the
	// conn of a session that was just installed (re-review R2). stop() returning
	// true means the close never started, so a completed handshake's conn is
	// never touched; if ctx is ALREADY done the close runs at once, which is
	// correct — the caller discards the result on ctx.Err().
	stopClose := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stopClose()

	// D6 §7.2(b): 6-field REGISTER carrying this expose's home epoch (0 in N=1).
	line := fmt.Sprintf("REGISTER %s %s %d %s %d\n", c.sid, c.nidValue(), publicPort, token, epoch)
	if _, err := conn.Write([]byte(line)); err != nil {
		_ = conn.Close()
		return nil, nil, fmt.Errorf("tunnel client: write REGISTER: %w", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	br := bufio.NewReader(conn)
	err = readRegisterReply(br)
	_ = conn.SetReadDeadline(time.Time{})
	if err != nil {
		_ = conn.Close()
		var deny *DenyError
		if errors.As(err, &deny) {
			return nil, nil, deny
		}
		return nil, nil, fmt.Errorf("tunnel client: read REGISTER reply: %w", err)
	}

	muxCfg := muxConfig(c.logger)
	if selectedFlow(conn) {
		muxCfg = flowMuxConfig(io.Discard)
	}
	yamuxSess, err := yamux.Client(retainRegisterReader(conn, br), muxCfg)
	if err != nil {
		_ = conn.Close()
		return nil, nil, fmt.Errorf("tunnel client: yamux client: %w", err)
	}
	if selectedFlow(conn) {
		if err = attachClientFlow(c, conn, yamuxSess, publicPort, token, brokerAddr, epoch, certPins, flowGeneration); err != nil {
			_ = conn.Close()
			_ = yamuxSess.Close()
			return nil, nil, err
		}
	}
	return conn, yamuxSess, nil
}

// supervise owns one (publicPort, generation) for life: it runs the accept
// loop on the current yamux session, and on a session drop re-dials with
// backoff, swapping the live transport in place. It exits ONLY on ctx
// cancel (Close / Start-cancel / a newer Open replacing this port) or a
// terminal DENY (proxy off / revoke). The loop structure makes the state
// hook fire exactly once per edge: one notifyState(false) per drop, one
// notifyState(true) per successful reconnect — they strictly alternate.
// R-13: epoch and ownership are fixed for this generation; normal rehome creates
// a fresh supervisor. Address and pins may change only for initial naming or
// same-home certificate rotation. Redial snapshots both under c.mu and a gen
// check, never from a successor's slot or from the hot accept loop.
func (c *Client) supervise(ctx context.Context, publicPort, localPort int, token, brokerAddr string, epoch int64, certPins proto.CertPins, gen uint64, initial *yamux.Session) {
	sess := initial
	for {
		c.runAcceptLoop(ctx, publicPort, localPort, sess)
		if ctx.Err() != nil {
			return // intentional teardown — owner already knows; no callback
		}
		c.notifyState(publicPort, false) // down edge (one per drop)
		for {
			conn, ys, pinned, err := c.redialWithBackoff(ctx, publicPort, token, brokerAddr, epoch, certPins, gen)
			if err != nil {
				// The down edge above is final on a terminal DENY or cancellation.
				c.dropSession(publicPort, gen)
				return
			}
			installed, retry := c.swapTransport(publicPort, gen, conn, ys, pinned)
			if installed {
				sess = ys
				break
			}
			_ = conn.Close()
			_ = ys.Close()
			if !retry {
				return // this generation was superseded
			}
			// A pin upgrade overtook an unverified handshake. Keep the same
			// supervisor and retry with current pins; do not duplicate the down edge.
		}

		c.logger.Info("tunnel: reconnected", "public_port", publicPort, "local_port", localPort, "epoch", epoch)
		c.notifyState(publicPort, true) // up edge (one per reconnect)
	}
}

// redialWithBackoff retries dialAndRegister with full-jitter exponential
// backoff until it succeeds, hits a terminal error (terminal DENY or ctx
// cancel), or ctx is done. Returns the new transport on success, or a
// non-nil error on terminal stop.
func (c *Client) redialWithBackoff(ctx context.Context, publicPort int, token, brokerAddr string, epoch int64, certPins proto.CertPins, gen uint64) (net.Conn, *yamux.Session, bool, error) {
	sleep, ceiling := c.backoffBase, c.backoffMax
	if st := c.flowState; st != nil {
		st.mu.Lock()
		e := st.endpoints[publicPort]
		st.mu.Unlock()
		if e != nil {
			sleep, ceiling = 250*time.Millisecond, time.Second
		}
	}
	for {
		// An imported/original owner shares the same first-detach bound. The
		// frozen interval is resumable, but never starts a second raw connector.
		var attemptDeadline time.Time
		if st := c.flowState; st != nil {
			st.mu.Lock()
			e, frozen := st.endpoints[publicPort], st.frozen
			if e != nil {
				attemptDeadline = e.detachDeadline
			}
			st.mu.Unlock()
			if frozen {
				timer := time.NewTimer(10 * time.Millisecond)
				select {
				case <-ctx.Done():
					timer.Stop()
					return nil, nil, false, ctx.Err()
				case <-timer.C:
				}
				continue
			}
			if !attemptDeadline.IsZero() && !time.Now().Before(attemptDeadline) {
				// Keep deadline refusal visible to image recovery until teardown;
				// deleting the last endpoint would make ResumeCarriers report an
				// empty success for an irreversibly retired original owner.
				// origin: hot-update S5 internal review HOTUPDATE-T40
				rejectClientEndpoint(c, publicPort)
				return nil, nil, false, os.ErrDeadlineExceeded
			}
		}
		timer := time.NewTimer(jitter(sleep))
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, nil, false, ctx.Err()
		case <-timer.C:
		}
		// Pick up rotation or initial naming atomically from THIS generation,
		// never a successor's address/pins. Off the hot accept path and locked
		// against ApplyHome's writes.
		pins, addr := certPins, brokerAddr
		c.mu.Lock()
		if cur, ok := c.sessions[publicPort]; ok && cur.gen == gen {
			pins, addr = cur.certPins, cur.brokerAddr
		}
		c.mu.Unlock()
		dialCtx, cancel := context.WithCancel(ctx)
		if !attemptDeadline.IsZero() {
			cancel()
			dialCtx, cancel = context.WithDeadline(ctx, minTime(attemptDeadline, time.Now().Add(ResumeAttempt)))
		}
		conn, ys, err := c.dialAndRegister(dialCtx, publicPort, token, addr, epoch, pins)
		cancel()
		if err == nil {
			return conn, ys, pins.Current != "", nil
		}
		// A lost broker owner cannot preserve its old business sockets. Keep
		// this allocation's supervisor, retire those sockets, and use a fresh
		// authenticated initial RESUME for subsequent requests on the same port.
		// origin: hot-update S5 plan §0 / §4.2 / P13-REGRESS
		var refusal *flow.Failure
		if errors.As(err, &refusal) && refusal.Reason == flow.ReasonUnknownFlow && retireLostClientEndpoint(c, publicPort, gen) {
			continue
		}
		var de *DenyError
		if errors.As(err, &de) && !denyIsTransient(de.Reason) {
			// Authoritative refusal (proxy off / revoke / unknown reason):
			// stop. A revoked exit can never be resurrected by reconnect.
			c.logger.Warn("tunnel: reconnect denied, stopping",
				"public_port", publicPort, "reason", de.Reason)
			// The legacy session slot does not own retained v1 business FDs.
			// Terminal authority refusal must retire those original sockets too.
			// origin: hot-update S5 internal review HOTUPDATE-T40
			rejectClientEndpoint(c, publicPort)
			return nil, nil, false, err
		}
		// Transient (network error OR a transient DENY): back off and retry.
		if de != nil {
			c.logger.Info("tunnel: reconnect transient DENY, retrying",
				"public_port", publicPort, "reason", de.Reason)
		}
		sleep *= 2
		if sleep > ceiling {
			sleep = ceiling
		}
	}
}

// swapTransport replaces a supervised port's live conn+yamux in place IFF
// the slot still belongs to this generation (not superseded by a concurrent
// Open/Close). Returns false to tell the caller to discard its freshly-dialed
// transport. KEEPS the same cancel — the one-cancel-for-life invariant.
func (c *Client) swapTransport(publicPort int, gen uint64, conn net.Conn, ys *yamux.Session, pinned bool) (installed, retry bool) {
	c.mu.Lock()
	cur, ok := c.sessions[publicPort]
	if !ok || cur.gen != gen {
		c.mu.Unlock()
		return false, false
	}
	if !pinned && cur.certPins.Current != "" {
		tlsConn, ok := conn.(*tls.Conn)
		if !ok || clientTLSConfigPinned(cur.certPins).VerifyConnection(tlsConn.ConnectionState()) != nil {
			c.mu.Unlock()
			return false, true
		}
		pinned = true
	}
	oldConn, oldYS := cur.conn, cur.yamuxSess
	cur.conn = conn
	cur.yamuxSess = ys
	cur.pinned = pinned
	c.mu.Unlock()
	// Close the predecessor transport AFTER releasing c.mu. On a HALF-OPEN drop
	// (the residential-link case) the old conn's fd + yamux goroutines are not
	// yet reaped — the local side only saw a keepalive timeout, not a peer FIN —
	// so without this every reconnect would leak one fd + yamux internals,
	// unbounded over a long flaky-link outage. Closing an already-closed
	// conn/session (the clean-drop path) is a harmless no-op.
	if oldYS != ys {
		_ = oldYS.Close()
	}
	if oldConn != conn {
		_ = oldConn.Close()
	}
	return true, false
}

// dropSession removes a supervised port's slot IFF it still belongs to this
// generation, scrubbing the cached token. A no-op if a newer Open already
// replaced the slot.
func (c *Client) dropSession(publicPort int, gen uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if cur, ok := c.sessions[publicPort]; ok && cur.gen == gen {
		// Release the per-session ctx (and its children) — matching Close and the
		// Open-replace path. On the terminal-DENY exit the sessCtx was NEVER
		// canceled, so without this its cancelCtx lingers in c.ctx's children list
		// until the whole agent run-ctx ends; repeated revoke/off cycles would
		// accumulate them. Idempotent on the ctx-cancel path (already canceled).
		cur.cancel()
		cur.token = ""
		delete(c.sessions, publicPort)
	}
}

// jitter returns a full-jitter sleep in (0, d]. Per-attempt randomness
// de-synchronizes a fleet reconnecting after a shared broker blip.
func jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	return time.Duration(rand.Int64N(int64(d)) + 1)
}

// Close drops the session for one public port (e.g. after expose-rm).
// No-op if not open.
func (c *Client) Close(publicPort int) {
	closeClientEndpoint(c, publicPort)
	c.mu.Lock()
	sess, ok := c.sessions[publicPort]
	delete(c.sessions, publicPort)
	c.mu.Unlock()
	if !ok {
		return
	}
	sess.cancel()
	_ = sess.conn.Close()
	_ = sess.yamuxSess.Close()
}

// runAcceptLoop pumps streams off ONE yamux session value (passed in, not
// re-read from c.sessions — keeps the hot path lock-free) until that session
// errors. It returns on any Accept error; the caller (supervise) checks
// ctx.Err() to tell an intentional close from a drop that warrants a redial.
func (c *Client) runAcceptLoop(ctx context.Context, publicPort, localPort int, sess *yamux.Session) {
	if clientFlowWait(c, publicPort, sess) {
		return
	}
	observation := observeLegacySession(c, publicPort, sess)
	for {
		stream, err := sess.Accept()
		if err != nil {
			if ctx.Err() == nil {
				c.logger.Info("tunnel: session closed", "public_port", publicPort, "err", err)
			}
			return
		}
		go c.bridgeStreamToLocal(stream, localPort, observation)
	}
}

func (c *Client) bridgeStreamToLocal(stream net.Conn, localPort int, observation *drillobserve.Event) {
	local, err := dialLegacyLocal(c, localPort)
	if err != nil {
		c.logger.Warn("tunnel: dial local", "err", err, "local_port", localPort)
		_ = stream.Close()
		return
	}
	observeLegacyBridge(observation, stream, local)
	bridge(local, stream)
}

func retryRegisterDenied(reason string) bool {
	// home_catching_up (D6 §7.2): the home replica has not yet applied the
	// latest OpPortReassignHome (presented epoch > its local row epoch). It is
	// TRANSIENT — the agent must keep retrying until the home catches up, never
	// treat it as terminal (which would brick the rehome). The reason string is
	// the proto SSOT shared with the broker emit-side (no duplicated literal).
	// Policy stays in the tunnel owner so its transport leaf cannot import the
	// public protocol's authentication and transaction dependencies.
	// origin: S7 internal review GATE-CONVERGE-layering F3
	return reason == proto.ReasonHomeCatchingUp || transport.DenyIsTransient(reason)
}
