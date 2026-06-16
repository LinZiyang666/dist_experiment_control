// Package tunnel is the P6 data plane: a minimal reverse TCP tunnel
// from a public broker port to a local agent port.
//
// The architecture (F.1 / F.2) calls for embedded frp on the broker
// side and a frpc subprocess on the agent. We deviate: tunnel is a
// from-scratch in-Go implementation backed by hashicorp/yamux for
// stream multiplexing, ~200 LOC vs frp's ~50 transitive deps. The
// behavior contract (TCP traffic from public-port lands on local-port,
// with auth-by-token) is identical from a user POV. If we ever need
// frp's wider feature set (HTTP vhost, kcp, plugins, web admin) we can
// swap the implementation behind the same internal API; until then
// keeping it native saves a major dep upgrade burden.
//
// Wire shape (line-based control + binary stream multiplex):
//
//  1. agent dials broker:7000.
//  2. agent writes:  REGISTER <sid> <nid> <port> <token>\n
//     one line, ASCII; broker reads + parses.
//  3. broker computes SHA256(token), looks up port_allocations:
//     - state must be ALLOCATED;
//     - port must equal the row's port;
//     - sid/nid must equal the row's sid/nid.
//     On match, broker writes:  OK\n  and starts a yamux SERVER
//     session over the same TCP connection. On mismatch, broker
//     writes  DENY <reason>\n  and closes.
//  4. broker starts listening on the public port (e.g. :14022). For
//     each accepted public connection, broker opens a new yamux
//     stream to the agent.
//  5. agent receives the yamux stream, dials its local_port, and
//     pipes bytes both ways with io.Copy. The public TCP client
//     sees the bytes the local server sent and vice-versa.
//
// One yamux session per (agent, public_port). Session breakage (network
// blip, etc.) is detected by both sides as a closed yamux session. The
// agent side runs a per-port supervisor goroutine that re-dials +
// re-REGISTERs with capped backoff (the broker's still-ALLOCATED row +
// token_hash re-authorize it), surfaces up/down to the owner via the
// session-state hook, and stops permanently on an authoritative DENY
// (proxy off / revoke). On a full agent restart the tunnel is instead
// rebuilt from state.json (see agent.replayPortsFromState).
package tunnel

import (
	"bufio"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/yamux"
)

// ErrRegisterDenied wraps every broker DENY reply. errors.Is(err,
// ErrRegisterDenied) is true for a *DenyError; the reconnect loop then
// classifies the Reason as terminal (stop) or transient (retry).
var ErrRegisterDenied = errors.New("tunnel: broker denied REGISTER")

// DenyError carries the broker's DENY reason string for terminal-vs-
// transient classification (see denyIsTransient).
type DenyError struct{ Reason string }

func (e *DenyError) Error() string { return "tunnel: broker denied REGISTER: " + e.Reason }

func (e *DenyError) Is(target error) bool { return target == ErrRegisterDenied }

// denyIsTransient reports whether a DENY reason is worth retrying. Only
// the broker's two non-authoritative reasons are transient: a public-port
// bind race (the old listener is mid-teardown) and a broker-side store
// fault (try_again, see broker.tunnelTokenLookup). Every other reason —
// including any UNKNOWN one from a newer broker — is treated terminal:
// the safe default is to stop rather than hammer a broker that
// authoritatively refused us.
func denyIsTransient(reason string) bool {
	switch reason {
	case "public_port_bind_failed", "try_again":
		return true
	default:
		return false
	}
}

// TokenLookup is what the broker calls on each REGISTER to decide
// allow/deny. Returns nil if (sid, nid, port, token) maps to an
// ALLOCATED row whose token_hash matches; non-nil error otherwise.
// Backed in production by internal/port.LookupByTokenHash.
type TokenLookup func(sid, nid string, port int, tokenHash string) error

// LocalPortLookup is the agent-side counterpart: given the public port
// the broker registered us for, return which local port to dial. The
// agent backs this by its in-memory copy of state.json port_tokens.
type LocalPortLookup func(publicPort int) (int, error)

// Server is the broker side of the tunnel. Owns the control listener
// (`:7000`) plus one public listener per registered (agent, port).
type Server struct {
	addr        string
	publicHost  string // bind addr for public ports (default 0.0.0.0)
	tokenLookup TokenLookup
	logger      *slog.Logger

	// tlsCert, when non-nil, pins a specific server cert. nil means
	// "generate an ephemeral self-signed cert on Start" (architecture
	// F.5 fallback). Callers pass this through NewServerWithCert.
	tlsCert *tls.Certificate

	mu       sync.Mutex
	sessions map[int]*serverSession // public port -> session
	// killGen[port] is bumped by CloseProxy. A handleAgent that snapshotted an
	// older value before binding refuses to install (F1 in-flight-REGISTER race).
	killGen map[int]int64
	// killGenSession[sid] is bumped by CloseSession — even when NO listener is
	// installed yet — so a session-level OFF also fences an in-flight REGISTER
	// that passed token auth but hasn't inserted its serverSession (round-5 F1).
	killGenSession map[string]int64
	// inflightBySID[sid] counts REGISTER handlers currently between snapshot and
	// install for sid; forgotten[sid] marks a deleted session awaiting prune.
	// ForgetSession BUMPS the fence and only prunes once in-flight drains, so a
	// paused authorized REGISTER can't install after deletion (round-6 F4).
	inflightBySID map[string]int
	forgotten     map[string]bool
	// closed flips true after Close()/ctx-cancel begin. Guards against
	// the audit-found shutdown race: in-flight handleAgent that's mid
	// REGISTER could otherwise insert into s.sessions AFTER Close
	// drained it, leaking the bound public port across broker restarts.
	closed bool
	ln     net.Listener   // control listener; Close() must close it
	wg     sync.WaitGroup // tracks in-flight handleAgent goroutines
}

type serverSession struct {
	sid        string // owning session — lets CloseSession kill by sid without a DB query
	publicPort int
	listener   net.Listener
	yamuxSess  *yamux.Session
	rawConn    net.Conn // the raw TCP control connection from agent
	cancel     context.CancelFunc
}

// NewServer returns a broker-side tunnel server. Call Start to bind.
func NewServer(addr, publicHost string, lookup TokenLookup, logger *slog.Logger) *Server {
	return &Server{
		addr:           addr,
		publicHost:     publicHost,
		tokenLookup:    lookup,
		logger:         logger,
		sessions:       map[int]*serverSession{},
		killGen:        map[int]int64{},
		killGenSession: map[string]int64{},
		inflightBySID:  map[string]int{},
		forgotten:      map[string]bool{},
	}
}

// Start begins accepting agent control connections. Returns when the
// TLS listener is bound (so callers know it's ready); the actual
// accept loop runs in a goroutine until ctx is canceled.
//
// Architecture F.5: control signaling is wrapped in TLS so the bearer
// token in the REGISTER line cannot be observed by passive
// eavesdroppers between agent and broker. With s.tlsCert nil we
// generate a fresh ephemeral self-signed cert on each Start;
// production deployments can pin one via NewServerWithCert.
func (s *Server) Start(ctx context.Context) error {
	tlsCfg, err := serverTLSConfig(s.tlsCert)
	if err != nil {
		return err
	}
	rawLn, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("tunnel server: listen %s: %w", s.addr, err)
	}
	ln := tls.NewListener(rawLn, tlsCfg)
	s.mu.Lock()
	s.ln = ln
	s.mu.Unlock()

	go s.acceptLoop(ctx, ln)
	go func() {
		<-ctx.Done()
		s.Close()
	}()
	s.logger.Info("tunnel: server listening", "addr", s.addr)
	return nil
}

func (s *Server) acceptLoop(ctx context.Context, ln net.Listener) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			// Listener closed by Close() (Close closes ln + sets
			// s.closed); return cleanly so the goroutine exits
			// instead of spinning on accept-after-close errors.
			s.mu.Lock()
			done := s.closed
			s.mu.Unlock()
			if done {
				return
			}
			s.logger.Warn("tunnel server: accept", "err", err)
			continue
		}
		s.wg.Add(1)
		go func(c net.Conn) {
			defer s.wg.Done()
			s.handleAgent(ctx, c)
		}(conn)
	}
}

// handleAgent reads the REGISTER line, validates, then runs the
// public-port acceptor. One goroutine per agent control conn (= per
// public port).
func (s *Server) handleAgent(ctx context.Context, conn net.Conn) {
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	br := bufio.NewReader(conn)
	line, err := br.ReadString('\n')
	if err != nil {
		s.logger.Warn("tunnel server: read REGISTER", "err", err)
		_ = conn.Close()
		return
	}
	_ = conn.SetReadDeadline(time.Time{})

	sid, nid, port, token, ok := parseRegisterLine(line)
	if !ok {
		_, _ = conn.Write([]byte("DENY malformed_register\n"))
		_ = conn.Close()
		return
	}

	// Snapshot BOTH the port and session kill generations BEFORE authorizing.
	// If a CloseProxy(port) [round-2 F1] or a CloseSession(sid) [round-5 F1]
	// lands during our handshake (the proxy-off kill switch firing between the
	// token lookup and session install), the matching generation advances and we
	// abort the install — so an already-authorized REGISTER can't resurrect the
	// public listener after OFF returned, whether OFF closed by port or by sid.
	s.mu.Lock()
	gen := s.killGen[port]
	sessGen := s.killGenSession[sid]
	// round-6 F4: mark this REGISTER in-flight for sid so ForgetSession can't
	// prune the session tombstone (and lose the fence) while we're paused
	// between snapshot and install. Decremented when the handler returns.
	s.inflightBySID[sid]++
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.inflightBySID[sid]--
		s.maybePruneSessionLocked(sid)
		s.mu.Unlock()
	}()

	if err := s.tokenLookup(sid, nid, port, hashToken(token)); err != nil {
		s.logger.Info("tunnel server: REGISTER denied",
			"sid", sid, "nid", nid, "port", port, "err", err)
		_, _ = conn.Write([]byte("DENY " + err.Error() + "\n"))
		_ = conn.Close()
		return
	}

	// Bind the public port BEFORE writing OK so the agent's Open()
	// returning unblocks only after our publicAcceptLoop is ready to
	// receive the first connection. Without this ordering there's a
	// race where ctl pipes traffic at the public port immediately
	// after Open returns and we're not listening yet.
	publicLn, err := net.Listen("tcp", net.JoinHostPort(s.publicHost, strconv.Itoa(port)))
	if err != nil {
		s.logger.Warn("tunnel server: bind public port", "err", err, "port", port)
		_, _ = conn.Write([]byte("DENY public_port_bind_failed\n"))
		_ = conn.Close()
		return
	}

	// Pre-fence BEFORE writing OK: a CloseProxy(port) / CloseSession(sid) /
	// ForgetSession(sid) that landed while we were paused in tokenLookup must
	// abort HERE — before the agent's Open() sees OK — so a forgotten session
	// never receives an authorized reply and the just-bound listener is torn
	// down before it can be dialed. Without this gate, OK below returns the
	// agent's Open() while the listener is still bound, and the post-yamux
	// re-check only closes it later — a window the round-6 ForgetSession test
	// loses under load. The post-yamux re-check stays as the authoritative
	// install-time fence for the residual snapshot→install window.
	s.mu.Lock()
	fenced := s.closed || s.killGen[port] != gen || s.killGenSession[sid] != sessGen
	s.mu.Unlock()
	if fenced {
		_ = publicLn.Close()
		_, _ = conn.Write([]byte("DENY session_closed\n"))
		_ = conn.Close()
		return
	}

	if _, err := conn.Write([]byte("OK\n")); err != nil {
		_ = publicLn.Close()
		_ = conn.Close()
		return
	}

	yamuxSess, err := yamux.Server(conn, nil)
	if err != nil {
		s.logger.Warn("tunnel server: yamux server", "err", err)
		_ = publicLn.Close()
		_ = conn.Close()
		return
	}

	sessCtx, cancel := context.WithCancel(ctx)
	sess := &serverSession{
		sid:        sid,
		publicPort: port,
		listener:   publicLn,
		yamuxSess:  yamuxSess,
		rawConn:    conn,
		cancel:     cancel,
	}
	s.mu.Lock()
	if s.closed || s.killGen[port] != gen || s.killGenSession[sid] != sessGen {
		// Server shutting down, OR a CloseProxy(port)/CloseSession(sid) fired
		// during our handshake (round-2/round-5 F1). Either way don't install —
		// roll back the per-conn state so we don't leak the public port or
		// resurrect a killed exit.
		s.mu.Unlock()
		cancel()
		_ = publicLn.Close()
		_ = yamuxSess.Close()
		_ = conn.Close()
		return
	}
	// Replace any prior session on this port (agent restart / re-expose).
	if old, ok := s.sessions[port]; ok {
		old.cancel()
		_ = old.listener.Close()
		_ = old.rawConn.Close()
	}
	s.sessions[port] = sess
	s.mu.Unlock()

	s.logger.Info("tunnel: registered",
		"sid", sid, "nid", nid, "public_port", port)

	// Watch the yamux session's close channel so a client-side Close
	// (or network drop) tears down the public listener instead of
	// leaving an orphan accepting connections that go nowhere.
	go func() {
		<-yamuxSess.CloseChan()
		cancel()
		_ = sess.listener.Close()
	}()

	go s.publicAcceptLoop(sessCtx, sess)
}

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
		go s.bridgePublicToYamux(pubConn, sess)
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
	s.mu.Lock()
	// Advance the kill generation FIRST, so any in-flight handleAgent that
	// already passed the token lookup for this port aborts its install (F1).
	s.killGen[port]++
	sess, ok := s.sessions[port]
	if ok {
		delete(s.sessions, port)
	}
	s.mu.Unlock()

	if !ok {
		return false
	}
	sess.cancel()
	_ = sess.listener.Close()
	_ = sess.rawConn.Close()
	return true
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

	for _, sess := range victims {
		sess.cancel()
		_ = sess.listener.Close()
		_ = sess.rawConn.Close()
	}
}

// maybePruneSessionLocked deletes a forgotten session's kill-gen bookkeeping
// once no REGISTER handler is in-flight for it (so the fence is never removed
// from under a paused handler). Caller holds s.mu.
func (s *Server) maybePruneSessionLocked(sid string) {
	if s.forgotten[sid] && s.inflightBySID[sid] <= 0 {
		delete(s.killGenSession, sid)
		delete(s.inflightBySID, sid)
		delete(s.forgotten, sid)
	}
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
		sess.cancel()
		_ = sess.listener.Close()
		_ = sess.rawConn.Close()
	}
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
	for _, sess := range snap {
		sess.cancel()
		_ = sess.listener.Close()
		_ = sess.rawConn.Close()
	}
	// Wait for in-flight handleAgent goroutines to drain so the
	// caller of Close knows the bound public ports are released
	// (vs. just "scheduled to close"). Bound by the per-session
	// cancellation above; can't run forever.
	s.wg.Wait()
}

// ----- agent side -----------------------------------------------------------

// Client is the agent side. Owns one yamux session per public port the
// broker has authorized us to expose, plus one supervisor goroutine per
// port that re-dials with capped backoff on session loss (see supervise).
type Client struct {
	brokerAddr      string
	sid             string
	nid             string
	localPortLookup LocalPortLookup
	logger          *slog.Logger

	backoffBase time.Duration // first redial wait (default 500ms)
	backoffMax  time.Duration // redial wait ceiling (default 30s)

	mu        sync.Mutex
	sessions  map[int]*clientSession        // public port -> session
	nextGen   uint64                        // monotonic generation stamp (mu-guarded)
	stateHook func(publicPort int, up bool) // set once before Start; nil = no-op
	ctx       context.Context
	cancel    context.CancelFunc
}

type clientSession struct {
	publicPort int
	localPort  int                // redial target; reused across redials
	token      string             // in-memory only; NEVER logged, NEVER persisted here
	conn       net.Conn           // swapped in place on a successful redial (under c.mu)
	yamuxSess  *yamux.Session     // swapped in place on a successful redial (under c.mu)
	cancel     context.CancelFunc // ONE per generation, for life (the sole stop signal)
	gen        uint64             // fences a stale supervisor against a concurrent Open/Close
}

// NewClient returns an agent-side tunnel client.
func NewClient(brokerAddr, sid, nid string, lookup LocalPortLookup, logger *slog.Logger) *Client {
	return &Client{
		brokerAddr:      brokerAddr,
		sid:             sid,
		nid:             nid,
		localPortLookup: lookup,
		logger:          logger,
		backoffBase:     500 * time.Millisecond,
		backoffMax:      30 * time.Second,
		sessions:        map[int]*clientSession{},
	}
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
	c.ctx, c.cancel = context.WithCancel(ctx)
	go func() {
		<-c.ctx.Done()
		c.mu.Lock()
		for _, sess := range c.sessions {
			sess.cancel()
			_ = sess.conn.Close()
			_ = sess.yamuxSess.Close()
		}
		c.sessions = map[int]*clientSession{}
		c.mu.Unlock()
	}()
}

// Open establishes (or replaces) a yamux session for one (publicPort,
// token) pair and starts its supervisor. localPort is the agent-side port
// to dial when the broker pushes a new stream. Blocks on the FIRST dial +
// REGISTER (so AddProxy's rollback contract sees a first-attempt failure)
// and returns that error verbatim. AFTER a successful Open, transient
// session drops are recovered by the supervisor's reconnect loop; the
// caller is not involved again unless it calls Close.
func (c *Client) Open(publicPort, localPort int, token string) error {
	if c.ctx == nil {
		return errors.New("tunnel client: Start not called")
	}
	conn, yamuxSess, err := c.dialAndRegister(c.ctx, publicPort, token)
	if err != nil {
		return fmt.Errorf("tunnel client: Open: %w", err)
	}
	sessCtx, cancel := context.WithCancel(c.ctx)

	c.mu.Lock()
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
		// Replace a prior session (re-expose / full proxy rebuild). Canceling
		// its ctx retires the old supervisor; bumping nextGen fences it out of
		// swapTransport/dropSession on this port.
		old.cancel()
		_ = old.conn.Close()
		_ = old.yamuxSess.Close()
	}
	c.nextGen++
	gen := c.nextGen
	c.sessions[publicPort] = &clientSession{
		publicPort: publicPort,
		localPort:  localPort,
		token:      token,
		conn:       conn,
		yamuxSess:  yamuxSess,
		cancel:     cancel,
		gen:        gen,
	}
	c.mu.Unlock()

	go c.supervise(sessCtx, publicPort, localPort, token, gen, yamuxSess)
	c.logger.Info("tunnel: opened",
		"public_port", publicPort, "local_port", localPort)
	return nil
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
func (c *Client) dialAndRegister(ctx context.Context, publicPort int, token string) (net.Conn, *yamux.Session, error) {
	dialer := &tls.Dialer{NetDialer: &net.Dialer{Timeout: 5 * time.Second}, Config: clientTLSConfig()}
	conn, err := dialer.DialContext(ctx, "tcp", c.brokerAddr)
	if err != nil {
		return nil, nil, fmt.Errorf("tunnel client: dial broker (TLS): %w", err)
	}
	// Close the conn on ctx.Done until the handshake completes so a blocked
	// Write/ReadString is interrupted by Close/Start-cancel. On the success
	// path stopHS fires before any later ctx.Done can reach the conn; if ctx
	// is ALREADY canceled we're shutting down, so closing the fresh conn is
	// correct (the supervisor discards it on ctx.Err()).
	hsDone := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-hsDone:
		}
	}()
	defer close(hsDone)

	line := fmt.Sprintf("REGISTER %s %s %d %s\n", c.sid, c.nid, publicPort, token)
	if _, err := conn.Write([]byte(line)); err != nil {
		_ = conn.Close()
		return nil, nil, fmt.Errorf("tunnel client: write REGISTER: %w", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	br := bufio.NewReader(conn)
	resp, err := br.ReadString('\n')
	if err != nil {
		_ = conn.Close()
		return nil, nil, fmt.Errorf("tunnel client: read REGISTER reply: %w", err)
	}
	_ = conn.SetReadDeadline(time.Time{})
	resp = strings.TrimRight(resp, "\r\n")
	if resp != "OK" {
		_ = conn.Close()
		reason := strings.TrimSpace(strings.TrimPrefix(resp, "DENY"))
		return nil, nil, &DenyError{Reason: reason}
	}

	yamuxSess, err := yamux.Client(conn, nil)
	if err != nil {
		_ = conn.Close()
		return nil, nil, fmt.Errorf("tunnel client: yamux client: %w", err)
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
func (c *Client) supervise(ctx context.Context, publicPort, localPort int, token string, gen uint64, initial *yamux.Session) {
	sess := initial
	for {
		c.runAcceptLoop(ctx, publicPort, localPort, sess)
		if ctx.Err() != nil {
			return // intentional teardown — owner already knows; no callback
		}
		c.notifyState(publicPort, false) // down edge (one per drop)
		conn, ys, err := c.redialWithBackoff(ctx, publicPort, token)
		if err != nil {
			// ctx-cancel or terminal DENY: the down edge above is the final
			// state; drop the slot so status reflects a dead exit, then stop.
			c.dropSession(publicPort, gen)
			return
		}
		if !c.swapTransport(publicPort, gen, conn, ys) {
			// A concurrent Open/Close superseded us. Close the freshly-dialed
			// transport (FD-leak guard) and let the new owner take over.
			_ = conn.Close()
			_ = ys.Close()
			return
		}
		sess = ys
		c.notifyState(publicPort, true) // up edge (one per reconnect)
	}
}

// redialWithBackoff retries dialAndRegister with full-jitter exponential
// backoff until it succeeds, hits a terminal error (terminal DENY or ctx
// cancel), or ctx is done. Returns the new transport on success, or a
// non-nil error on terminal stop.
func (c *Client) redialWithBackoff(ctx context.Context, publicPort int, token string) (net.Conn, *yamux.Session, error) {
	sleep := c.backoffBase
	for {
		timer := time.NewTimer(jitter(sleep))
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, nil, ctx.Err()
		case <-timer.C:
		}
		conn, ys, err := c.dialAndRegister(ctx, publicPort, token)
		if err == nil {
			return conn, ys, nil
		}
		var de *DenyError
		if errors.As(err, &de) && !denyIsTransient(de.Reason) {
			// Authoritative refusal (proxy off / revoke / unknown reason):
			// stop. A revoked exit can never be resurrected by reconnect.
			c.logger.Warn("tunnel: reconnect denied, stopping",
				"public_port", publicPort, "reason", de.Reason)
			return nil, nil, err
		}
		// Transient (network error OR a transient DENY): back off and retry.
		if de != nil {
			c.logger.Info("tunnel: reconnect transient DENY, retrying",
				"public_port", publicPort, "reason", de.Reason)
		}
		sleep *= 2
		if sleep > c.backoffMax {
			sleep = c.backoffMax
		}
	}
}

// swapTransport replaces a supervised port's live conn+yamux in place IFF
// the slot still belongs to this generation (not superseded by a concurrent
// Open/Close). Returns false to tell the caller to discard its freshly-dialed
// transport. KEEPS the same cancel — the one-cancel-for-life invariant.
func (c *Client) swapTransport(publicPort int, gen uint64, conn net.Conn, ys *yamux.Session) bool {
	c.mu.Lock()
	cur, ok := c.sessions[publicPort]
	if !ok || cur.gen != gen {
		c.mu.Unlock()
		return false
	}
	oldConn, oldYS := cur.conn, cur.yamuxSess
	cur.conn = conn
	cur.yamuxSess = ys
	c.mu.Unlock()
	// Close the predecessor transport AFTER releasing c.mu. On a HALF-OPEN drop
	// (the residential-link case) the old conn's fd + yamux goroutines are not
	// yet reaped — the local side only saw a keepalive timeout, not a peer FIN —
	// so without this every reconnect would leak one fd + yamux internals,
	// unbounded over a long flaky-link outage. Closing an already-closed
	// conn/session (the clean-drop path) is a harmless no-op.
	_ = oldYS.Close()
	_ = oldConn.Close()
	return true
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
	for {
		stream, err := sess.Accept()
		if err != nil {
			if ctx.Err() == nil {
				c.logger.Info("tunnel: session closed", "public_port", publicPort, "err", err)
			}
			return
		}
		go c.bridgeStreamToLocal(stream, localPort)
	}
}

func (c *Client) bridgeStreamToLocal(stream net.Conn, localPort int) {
	local, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(localPort)), 5*time.Second)
	if err != nil {
		c.logger.Warn("tunnel: dial local", "err", err, "local_port", localPort)
		_ = stream.Close()
		return
	}
	bridge(local, stream)
}

// ----- common helpers --------------------------------------------------------

// bridge io.Copy's bytes both directions and closes both sides when
// either direction terminates. Pure, no logging — this is on the hot
// path of every public TCP connection.
func bridge(a, b io.ReadWriteCloser) {
	done := make(chan struct{}, 2)
	go func() {
		_, _ = io.Copy(a, b)
		_ = a.Close()
		_ = b.Close()
		done <- struct{}{}
	}()
	go func() {
		_, _ = io.Copy(b, a)
		_ = a.Close()
		_ = b.Close()
		done <- struct{}{}
	}()
	<-done
}

func parseRegisterLine(line string) (sid, nid string, port int, token string, ok bool) {
	line = strings.TrimRight(line, "\r\n")
	parts := strings.Split(line, " ")
	if len(parts) != 5 || parts[0] != "REGISTER" {
		return "", "", 0, "", false
	}
	port, err := strconv.Atoi(parts[3])
	if err != nil {
		return "", "", 0, "", false
	}
	return parts[1], parts[2], port, parts[4], true
}

// hashToken is the SHA256 hex digest of raw. Kept locally
// (duplicated with internal/port.HashToken) to keep tunnel a
// dep-graph leaf: importing port would pull SQLite into the wire
// layer with no real benefit. Audit shard 06 F10 — flagged as low,
// resolved by comment, not extraction.
func hashToken(raw string) string {
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}
