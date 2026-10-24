package tunnel

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/hashicorp/yamux"
)

// ----- agent side -----------------------------------------------------------

// Client is the agent side. Owns one yamux session per public port the
// broker has authorized us to expose, plus one supervisor goroutine per
// port that re-dials with capped backoff on session loss (see supervise).
type Client struct {
	// Separate from per-control-session state; P1 installs the retained owner.
	// origin: hot-update S5 plan §2 / L13
	flowState *clientFlowState

	brokerAddr string
	sid        string
	// nid is the name presented on the REGISTER line. It is settable because a
	// cloned-credential instance learns its routing name from the broker's
	// register reply, which happens strictly AFTER this client is constructed
	// (cmd/tether builds the adapter before agent.New, before any connect).
	// Without a seam here, an instance leased `foo-02` would still REGISTER on
	// the tunnel as `foo`, tunnelTokenLookup would match the basename's
	// allocation row, and the install — keyed on the public port alone — would
	// evict the incumbent's live session.
	//
	// atomic because supervise/redial goroutines read it off the caller's
	// goroutine.
	nid atomic.Pointer[string]
	// nidGen fences slow Opens against a rename. See SetNID and Open.
	nidGen          atomic.Uint64
	localPortLookup LocalPortLookup
	logger          *slog.Logger

	backoffBase time.Duration // first redial wait (default 500ms)
	backoffMax  time.Duration // redial wait ceiling (default 30s)

	mu             sync.Mutex
	flowConfigUsed bool                          // first open/dial makes the transport mode immutable (mu-guarded)
	sessions       map[int]*clientSession        // public port -> session
	nextGen        uint64                        // monotonic generation stamp (mu-guarded)
	stateHook      func(publicPort int, up bool) // set once before Start; nil = no-op
	ctx            context.Context
	cancel         context.CancelFunc
	stopped        chan struct{} // Root cleanup receipt; cancellation alone does not join retained flow FDs.
}

type clientSession struct {
	pinned     bool   // this transport has passed a certificate-pin check
	nid        string // identity authorized by this slot's REGISTER
	publicPort int
	localPort  int                // redial target; reused across redials
	token      string             // in-memory only; NEVER logged, NEVER persisted here
	conn       net.Conn           // swapped in place on a successful redial (under c.mu)
	yamuxSess  *yamux.Session     // swapped in place on a successful redial (under c.mu)
	cancel     context.CancelFunc // ONE per generation, for life (the sole stop signal)
	gen        uint64             // fences a stale supervisor against a concurrent Open/Close

	// D6 §7.5 per-expose home: the broker addr this expose dials, its home epoch,
	// and the cert pins to verify the home with. One Client can fan out to N
	// homes. Epoch is immutable per generation; redial snapshots address/pins
	// under c.mu with a generation check for rotation and initial naming.
	// brokerAddr "" ⇒ fall back to Client.brokerAddr (N=1).
	brokerAddr string
	epoch      int64
	certPins   proto.CertPins
}

// SessionCount includes disconnected sessions awaiting redial: their public
// listeners still belong to this image and cannot cross exec yet.
// origin: hot-update S3 plan §4.1.
func (c *Client) SessionCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.sessions)
}

// NewClient returns an agent-side tunnel client.
func NewClient(brokerAddr, sid, nid string, lookup LocalPortLookup, logger *slog.Logger) *Client {
	// Restored owners are built before the agent logger is available. They
	// still execute the normal expose replay and reconnect logging paths.
	// origin: hot-update S5 internal review HOTUPDATE-T39/T41/T44/T45
	if logger == nil {
		logger = slog.Default()
	}
	c := &Client{
		brokerAddr:      brokerAddr,
		sid:             sid,
		localPortLookup: lookup,
		logger:          logger,
		backoffBase:     500 * time.Millisecond,
		backoffMax:      30 * time.Second,
		sessions:        map[int]*clientSession{},
	}
	c.nid.Store(&nid)
	return c
}

// SetNID updates the name this client presents on the REGISTER line.
//
// Called when a cloned-credential instance adopts a broker-assigned lease name.
// It is safe to call between sessions; an in-flight REGISTER already holds the
// value it read, and the agent only adopts a name during a session rebuild, so
// no dial observes a half-changed identity.
func (c *Client) SetNID(nid string) {
	prev := c.nidValue()
	c.nid.Store(&nid)
	// BUMP THE GENERATION BEFORE ANYTHING ELSE.
	//
	// Open reads the nid, then dials and completes a REGISTER handshake — all
	// outside c.mu, because it is slow. A rename landing inside that window
	// leaves an Open that already told the server it is the OLD name, and which
	// then installs its session into the map the rename just emptied. The
	// process goes on bridging a port it authorized under a name it no longer
	// holds, which is the very fan-out this rename exists to end.
	//
	// The generation is the fence: Open samples it up front and re-checks it
	// under the lock before installing. (external review F6)
	c.nidGen.Add(1)
	if prev == "" || prev == nid {
		return
	}
	// RETIRE EVERY SESSION HELD UNDER THE PREVIOUS NAME.
	//
	// Changing the name means this process is no longer the holder of the name
	// those sessions were authorized under — their allocations belong to
	// whoever holds it now. Leaving them running would keep this process
	// bridging another node's public ports, and the tunnel server's session map
	// is keyed on the public port ALONE, so the incumbent could not take them
	// back except opportunistically, on the next transport drop.
	//
	// Retiring here is not a data-plane regression for the retiring side: an
	// instance that has just been told it is not the basename holder has no
	// claim to those ports, and it will be issued its own if it exposes any.
	c.mu.Lock()
	retired := c.sessions
	c.sessions = map[int]*clientSession{}
	c.mu.Unlock()
	// Flow endpoints retain business FDs independently of the legacy slot.
	// A routing identity change revokes those owners as well.
	// origin: hot-update S5 plan §4.2 / T13
	if st := c.flowState; st != nil {
		st.mu.Lock()
		var ports []int
		for port, e := range st.endpoints {
			if e.allocation.NID != nid {
				ports = append(ports, port)
			}
		}
		st.mu.Unlock()
		for _, port := range ports {
			closeClientEndpoint(c, port)
		}
	}
	closers := make([]func(), 0, len(retired))
	for _, sess := range retired {
		// CANCEL INLINE, CLOSE OFF-GOROUTINE.
		//
		// The sessions are already fenced out of c.sessions by the map swap
		// above, so cancelling is enough to stop them being used. The Close
		// calls are NOT bounded: tls.Conn.Close takes the connection's write
		// lock before it can set a deadline, so a parked write on a blackholed
		// socket blocks for the kernel's retransmit time.
		//
		// This runs synchronously from adoptRoutingNID — in one caller BEFORE
		// the session finalizer is invoked, and in the other AFTER `rebuilding`
		// has been latched. Blocking here would therefore stall the very
		// teardown ladder gotcha #72 exists to bound, and in the second case
		// would leave `rebuilding` latched forever: every later reconnect
		// dropped, the finalizer never invoked, the node permanently dead with
		// no escalation. That is the defect #72 documented, reintroduced
		// outside the ladder built for it.
		sess.cancel()
		// EMIT THE DOWN EDGE. external review F6: retiring silently left every
		// mirror of this port's health latched at the last value it saw —
		// proxyTunnelUp / ProxyBound stayed true for a bridge that is being torn
		// down, so the agent reported a working exit on a dead tunnel. The hook
		// contract is one edge per transition, and this IS a transition.
		c.notifyState(sess.publicPort, false)
		conn, ysess := sess.conn, sess.yamuxSess
		// BOUNDED, AND ONE GOROUTINE FOR THE WHOLE RETIREMENT rather than one
		// per session. Close on a blackholed socket parks for the kernel's
		// retransmit time (that is why it is off the caller's path at all), so
		// an unbounded fan-out here is a goroutine and fd leak proportional to
		// how many ports the instance happened to be serving.
		closers = append(closers, func() {
			if conn != nil {
				_ = conn.Close()
			}
			if ysess != nil {
				_ = ysess.Close()
			}
		})
	}
	if len(closers) > 0 {
		go func() {
			for _, fn := range closers {
				fn()
			}
		}()
	}
}

// nidValue reads the current REGISTER identity.
func (c *Client) nidValue() string {
	if p := c.nid.Load(); p != nil {
		return *p
	}
	return ""
}

// SetSessionStateHook installs a callback fired (off any lock) whenever a
// supervised port's data plane transitions up/down: on the first drop
// (up→down), on every successful redial (down→up), and a final down
// before a terminal-DENY exit. Set it once before Start. The agent wires
// this to publish proxy ready/unready so /sub tracks real liveness.
func (c *Client) SetSessionStateHook(fn func(publicPort int, up bool)) {
	c.mu.Lock()
	c.stateHook = fn
	c.mu.Unlock()
}

// notifyState invokes the session-state hook (nil-guarded) OUTSIDE c.mu.
// The supervisor that calls this owns transition de-duplication, so this
// fires only on a genuine up/down edge.
func (c *Client) notifyState(publicPort int, up bool) {
	c.mu.Lock()
	hook := c.stateHook
	c.mu.Unlock()
	if hook != nil {
		hook(publicPort, up)
	}
}

// Start anchors the lifecycle to ctx. Sessions registered via Open
// are unwound on ctx.Done.
func (c *Client) Start(ctx context.Context) {
	c.mu.Lock()
	c.ctx, c.cancel = context.WithCancel(ctx)
	lifetime := c.ctx
	done := make(chan struct{})
	c.stopped = done
	c.mu.Unlock()
	go func() {
		defer close(done)
		<-lifetime.Done()
		c.mu.Lock()
		retired := c.sessions
		c.sessions = map[int]*clientSession{}
		st := c.flowState
		c.mu.Unlock()
		// Fence publication first, then wake TLS's shared writer without any
		// owner lock. A failed carrier must not hold cancellation behind Write.
		// origin: hot-update S5 plan §3.5 / §7.2 / P13-REGRESS
		for _, sess := range retired {
			sess.cancel()
			if secure, ok := sess.conn.(*tls.Conn); ok {
				_ = secure.NetConn().Close()
			}
			_ = sess.conn.Close()
			_ = sess.yamuxSess.Close()
		}
		if st != nil {
			st.mu.Lock()
			var ports []int
			for p := range st.endpoints {
				ports = append(ports, p)
			}
			st.mu.Unlock()
			for _, p := range ports {
				closeClientEndpoint(c, p)
			}
			_ = st.manager.Close()
		}
	}()
}

// Open establishes (or replaces) a yamux session for one (publicPort,
// token) pair and starts its supervisor. localPort is the agent-side port
// to dial when the broker pushes a new stream. Blocks on the FIRST dial +
// REGISTER (so AddProxy's rollback contract sees a first-attempt failure)
// and returns that error verbatim. AFTER a successful Open, transient
// session drops are recovered by the supervisor's reconnect loop; the
// caller is not involved again unless it calls Close.
// Open is the N=1 / single-home convenience entry: it dials the Client's
// configured brokerAddr with epoch 0 and no cert pins (InsecureSkipVerify
// fallback). Equivalent to OpenHome(publicPort, localPort, token, "", 0,
// proto.CertPins{}). Kept so non-clustered callers (and existing tests) stay
// unchanged.
func (c *Client) Open(publicPort, localPort int, token string) error {
	if c.ctx == nil {
		return errors.New("tunnel client: Start not called")
	}
	return c.OpenHome(c.ctx, publicPort, localPort, token, "", 0, proto.CertPins{})
}

// OpenHome establishes (or epoch-monotone replaces) a yamux session for one
// (publicPort, token) pair against a SPECIFIC home broker (D6 §7.5), pinned by
// certPins, at home epoch. brokerAddr "" falls back to the Client's single
// configured addr (N=1). See the package doc / Open for the supervisor contract.
//
// ctx bounds THE OPEN ONLY — the wait for a dial, the REGISTER handshake and the
// install under c.mu — never the session it installs: the session and its
// supervisor live on the Client's Start ctx exactly as before. When ctx ends
// mid-handshake the conn is closed and the ctx error returned; when it ends
// between a successful REGISTER and the install, the freshly-registered transport
// is discarded and the ctx error returned, so a caller whose deadline has passed
// never gets a late success it already reported as a failure. The agent's
// forwarded-expose handler passes the remaining share of its reply budget here
// (gotcha #86 / external review F1); reconcile and boot replay pass the run ctx.
func (c *Client) OpenHome(ctx context.Context, publicPort, localPort int, token, brokerAddr string, epoch int64, certPins proto.CertPins) error {
	// Freeze configuration before even admission/lookup: an in-flight legacy
	// open must not acquire v1 ownership halfway through its handshake.
	// origin: hot-update S5 plan §4.1 / §7.1 / P13-REGRESS
	c.mu.Lock()
	c.flowConfigUsed = true
	c.mu.Unlock()
	// Cover the install/replace interval as well as dial: an old legacy owner
	// can still exist between a verified v1 attach and the session-map commit.
	// origin: hot-update S5 plan §7 / T12
	if st := c.flowState; st != nil {
		st.mu.Lock()
		if st.frozen {
			st.mu.Unlock()
			return errFlowState
		}
		st.admission++
		st.mu.Unlock()
		defer func() { st.mu.Lock(); st.admission--; st.mu.Unlock() }()
	}
	if c.ctx == nil {
		return errors.New("tunnel client: Start not called")
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("tunnel client: Open: %w", err)
	}
	if c.flowState != nil {
		release, err := lockClientAllocation(ctx, c, publicPort)
		if err != nil {
			return err
		}
		defer release()
	}
	// Sample the rename generation BEFORE the REGISTER handshake reads the nid,
	// so the check under the lock below covers the whole dial window.
	openGen := c.nidGen.Load()
	openNID := c.nidValue()
	if reused, err := reuseClientSession(ctx, c, publicPort, localPort, token, brokerAddr, epoch, certPins, openNID, openGen); reused || err != nil {
		return err
	}
	// D6 §7.7/R-22: never dial a NAMED home (non-empty brokerAddr) insecurely.
	// A clustered expose with no pins yet (e.g. a state.json replay before the
	// register reply re-delivers them) defers — the caller retries when pins
	// arrive. The N=1 path (empty brokerAddr → Client.brokerAddr fallback) is
	// unaffected and keeps its InsecureSkipVerify.
	if brokerAddr != "" && certPins.Current == "" {
		return ErrHomePinsRequired
	}
	if c.flowState != nil && retireClientAllocation(c, publicPort, localPort, token, epoch, openNID) {
		return nil
	}
	// The handshake ends when EITHER the caller's ctx or the Client's Start ctx ends: hsCtx is a
	// child of c.ctx (Start-cancel still reaps a blocked dial) that the caller's ctx cancels too.
	// Only the handshake sees it — the session installed below is anchored to c.ctx, not to
	// the caller (a control-plane deadline must not become a data-plane lifetime, gotcha #80).
	hsCtx, cancelHS := context.WithCancel(c.ctx)
	defer cancelHS()
	stopRelay := context.AfterFunc(ctx, cancelHS)
	defer stopRelay()
	conn, yamuxSess, err := dialInitialFlow(hsCtx, c, publicPort, token, brokerAddr, epoch, certPins)
	if err != nil {
		if cerr := ctx.Err(); cerr != nil {
			// Name the caller's deadline, not the closed-conn symptom it produced ("use of closed
			// network connection" reads as a broker fault; "context deadline exceeded" reads as
			// what it is — the open did not finish inside the caller's budget).
			return fmt.Errorf("tunnel client: Open: %w (%v)", cerr, err)
		}
		return fmt.Errorf("tunnel client: Open: %w", err)
	}
	sessCtx, cancel := context.WithCancel(c.ctx)

	c.mu.Lock()
	// External review F1: the caller's budget ran out between REGISTER OK and the install. Its
	// caller has already (or is about to) report this open as failed; installing now would leave
	// a live session for a port the broker rolled back. Discard the transport (the broker sees a
	// session drop on a row it is deleting anyway) and return the deadline.
	if err := ctx.Err(); err != nil {
		c.mu.Unlock()
		cancel()
		_ = conn.Close()
		_ = yamuxSess.Close()
		return fmt.Errorf("tunnel client: Open finished after the caller's deadline: %w", err)
	}
	// THE RENAME FENCE (external review F6). This Open told the server which
	// name it was during a REGISTER that happened outside the lock; if the name
	// has changed since, that handshake is void and installing the session would
	// keep this process bridging a port authorized under a name it no longer
	// holds. Discard it exactly like a stale-epoch Open below.
	if c.nidGen.Load() != openGen {
		c.mu.Unlock()
		cancel()
		_ = conn.Close()
		_ = yamuxSess.Close()
		return nil
	}
	// Audit shard 01 F2: between Start's cleanup goroutine running
	// (c.ctx already canceled) and an in-flight Open inserting into
	// c.sessions, we'd leak the new session forever. Re-check ctx
	// under mu — if Start is shutting down, roll back this Open.
	if err := c.ctx.Err(); err != nil {
		c.mu.Unlock()
		cancel()
		_ = conn.Close()
		_ = yamuxSess.Close()
		return fmt.Errorf("tunnel client: Open after Start ctx cancel: %w", err)
	}
	if old, ok := c.sessions[publicPort]; ok {
		// D6 §7.4/R-13: epoch-monotone rehome replace. If a NEWER home epoch is
		// already installed for this port (two directives raced; the higher won),
		// THIS Open is stale — discard the freshly-dialed transport and report
		// success (the port IS served, by the newer epoch). Equal/greater epoch
		// (incl. the N=1 epoch-0 re-expose path) proceeds as a normal replace.
		if old.epoch > epoch {
			c.mu.Unlock()
			cancel()
			_ = conn.Close()
			_ = yamuxSess.Close()
			return nil
		}
		// Replace a prior session (re-expose / proxy rebuild / rehome). Canceling
		// its ctx retires the old supervisor; bumping nextGen fences it out of
		// swapTransport/dropSession on this port.
		old.cancel()
		_ = old.conn.Close()
		_ = old.yamuxSess.Close()
	}
	c.nextGen++
	gen := c.nextGen
	c.sessions[publicPort] = &clientSession{
		pinned:     certPins.Current != "",
		nid:        openNID,
		publicPort: publicPort,
		localPort:  localPort,
		token:      token,
		conn:       conn,
		yamuxSess:  yamuxSess,
		cancel:     cancel,
		gen:        gen,
		brokerAddr: brokerAddr,
		epoch:      epoch,
		certPins:   certPins,
	}
	c.mu.Unlock()

	// R-13: spawn parameters belong to this generation. Redial may pick up an
	// initial named address or rotated pins only under c.mu with a gen check;
	// it must never read an Open-replacement's configuration.
	go c.supervise(sessCtx, publicPort, localPort, token, brokerAddr, epoch, certPins, gen, yamuxSess)
	c.logger.Info("tunnel: opened",
		"public_port", publicPort, "local_port", localPort, "epoch", epoch)
	return nil
}

// reuseClientSession keeps a verified retained slot without another REGISTER.
// origin: hot-update S5 plan §4.1 / §7.1 / P13-REGRESS
func reuseClientSession(ctx context.Context, c *Client, publicPort, localPort int, token, brokerAddr string, epoch int64, certPins proto.CertPins, openNID string, openGen uint64) (bool, error) {
	// A control-session rebuild replays desired exposes while their data-plane
	// sessions may still own the listeners. Dialing another REGISTER first would
	// collide with our own port. Retain the existing slot, including a supervisor
	// currently redialing, only for exactly the same authorized configuration.
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.ctx.Err(); err != nil {
		return false, fmt.Errorf("tunnel client: Open after Start ctx cancel: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return false, fmt.Errorf("tunnel client: Open: %w", err)
	}
	if old := c.sessions[publicPort]; old != nil && old.nid == openNID && c.nidGen.Load() == openGen &&
		old.localPort == localPort && old.token == token && old.epoch == epoch &&
		effectiveTunnelAddress(c, old.brokerAddr) == effectiveTunnelAddress(c, brokerAddr) {
		// Mutable desired pins are not proof that this existing transport was
		// verified: a fallback session may originally have used no pins.
		pins := certPins
		if pins.Current == "" {
			pins = old.certPins
		}
		if brokerAddr != "" && pins.Current == "" {
			return false, ErrHomePinsRequired
		}
		if brokerAddr != "" || certPins.Current != "" {
			if err := verifySessionPeer(old, pins); err != nil {
				return false, err
			}
		}
		if certPins.Current != "" {
			old.certPins = certPins
		}
		if brokerAddr != "" {
			old.brokerAddr = brokerAddr
		}
		syncClientRoute(c, publicPort, old.brokerAddr, old.certPins)
		return true, nil
	}
	return false, nil
}

// effectiveTunnelAddress compares the configured fallback and explicit homes.
func effectiveTunnelAddress(c *Client, addr string) string {
	if addr == "" {
		return c.brokerAddr
	}
	return addr
}

// verifySessionPeer upgrades an unverified transport using its actual TLS peer.
// Desired pins alone do not prove a prior handshake; an already verified live
// transport may survive a normal pin rotation until its next redial.
// Caller holds c.mu; no network operation occurs here.
func verifySessionPeer(sess *clientSession, pins proto.CertPins) error {
	if sess.pinned {
		return nil
	}
	conn, ok := sess.conn.(*tls.Conn)
	if !ok || pins.Current == "" {
		return ErrHomePinsRequired
	}
	if err := clientTLSConfigPinned(pins).VerifyConnection(conn.ConnectionState()); err != nil {
		return fmt.Errorf("tunnel client: replay certificate: %w", err)
	}
	sess.pinned = true
	return nil
}

// ApplyHome applies a home directive to an OPEN expose (D6 §7.4):
//   - epoch > session epoch → an actual REHOME: Open-replace against the new home
//     (reusing the existing localPort + token), the atomic cutover.
//   - epoch == session epoch → a PURE-PIN (cert-rotation) update (R-24 / external
//     review F4): refresh the session's cert pins IN PLACE, WITHOUT tearing the
//     live transport. An already named home keeps its address; the first naming
//     of a fallback may adopt a new advertised address after peer verification.
//     The supervisor reads address/pins together under c.mu, gen-fenced, on redial.
//   - epoch < session epoch → a stale duplicate: no-op.
//
// A directive for a port with no open session is a no-op (the agent's
// open-from-state path handles a deferred/closed expose). The rehome dial BLOCKS;
// the caller retries a returned transient home_catching_up (R-15).
func (c *Client) ApplyHome(publicPort int, brokerAddr string, epoch int64, certPins proto.CertPins) error {
	c.mu.Lock()
	sess, ok := c.sessions[publicPort]
	if !ok {
		c.mu.Unlock()
		return nil // not open — nothing to rehome (directive for a closed/unknown expose)
	}
	if epoch < sess.epoch {
		c.mu.Unlock()
		return nil // stale lower-epoch directive
	}
	if epoch == sess.epoch {
		// An already named home requires a new epoch to move. The first naming
		// after single-to-cluster migration may advertise a new address for
		// this same broker; verify its live peer below before adopting it.
		if brokerAddr != "" && sess.brokerAddr != "" && sess.brokerAddr != brokerAddr {
			c.mu.Unlock()
			return errors.New("tunnel client: home address changed without a newer epoch")
		}
		// Same home epoch, possibly rotated pins: update in place, no transport tear.
		if (brokerAddr != "" || sess.brokerAddr != "") && certPins.Current == "" {
			c.mu.Unlock()
			return ErrHomePinsRequired
		}
		if certPins.Current != "" {
			if err := verifySessionPeer(sess, certPins); err != nil {
				c.mu.Unlock()
				return err
			}
		}
		if brokerAddr != "" {
			sess.brokerAddr = brokerAddr
		}
		sess.certPins = certPins
		syncClientRoute(c, publicPort, sess.brokerAddr, sess.certPins)
		c.mu.Unlock()
		return nil
	}
	localPort, token := sess.localPort, sess.token
	c.mu.Unlock()
	// OpenHome is the atomic replace (re-checks the epoch under mu, cancels the
	// old supervisor, installs the new session + supervisor). A transient/terminal
	// DENY surfaces to the caller for retry/handling. The rehome dial is bounded by
	// the Client's own lifetime only: its caller (the agent's reconcile ladder)
	// owns the retry, and nobody upstream is holding a request open for it.
	return c.OpenHome(c.ctx, publicPort, localPort, token, brokerAddr, epoch, certPins)
}

func (c *Client) HasSession(publicPort int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.sessions[publicPort]
	return ok
}

// LocalPort returns the installed desired configuration, including while its
// supervisor reconnects. An ignored stale Open must not change adapter caches.
func (c *Client) LocalPort(publicPort int) (int, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if sess := c.sessions[publicPort]; sess != nil {
		return sess.localPort, true
	}
	return 0, false
}

// StopClient joins the Start lifetime, including retained raw/replay ownership.
// A per-port Close only removes that endpoint; it cannot certify root cleanup.
// origin: hot-update S6 plan §9.3 / T56
func StopClient(c *Client, timeout time.Duration) error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	cancel, done, state := c.cancel, c.stopped, c.flowState
	c.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if done == nil {
		if state != nil {
			return state.manager.Close()
		}
		return nil
	}
	if timeout <= 0 {
		return errors.New("tunnel: client stop deadline required")
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
		return nil
	case <-timer.C:
		return errors.New("tunnel: client cleanup did not join")
	}
}
