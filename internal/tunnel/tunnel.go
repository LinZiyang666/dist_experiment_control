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
//   1. agent dials broker:7000.
//   2. agent writes:  REGISTER <sid> <nid> <port> <token>\n
//      one line, ASCII; broker reads + parses.
//   3. broker computes SHA256(token), looks up port_allocations:
//      - state must be ALLOCATED;
//      - port must equal the row's port;
//      - sid/nid must equal the row's sid/nid.
//      On match, broker writes:  OK\n  and starts a yamux SERVER
//      session over the same TCP connection. On mismatch, broker
//      writes  DENY <reason>\n  and closes.
//   4. broker starts listening on the public port (e.g. :14022). For
//      each accepted public connection, broker opens a new yamux
//      stream to the agent.
//   5. agent receives the yamux stream, dials its local_port, and
//      pipes bytes both ways with io.Copy. The public TCP client
//      sees the bytes the local server sent and vice-versa.
//
// One yamux session per (agent, public_port). Session breakage
// (network blip, agent restart, etc.) is detected by both sides as a
// closed yamux session; agent re-registers from state.json on its
// next ConnectAll() call.
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
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/yamux"
)

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
	// closed flips true after Close()/ctx-cancel begin. Guards against
	// the audit-found shutdown race: in-flight handleAgent that's mid
	// REGISTER could otherwise insert into s.sessions AFTER Close
	// drained it, leaking the bound public port across broker restarts.
	closed bool
	ln     net.Listener   // control listener; Close() must close it
	wg     sync.WaitGroup // tracks in-flight handleAgent goroutines
}

type serverSession struct {
	publicPort  int
	listener    net.Listener
	yamuxSess   *yamux.Session
	rawConn     net.Conn // the raw TCP control connection from agent
	cancel      context.CancelFunc
}

// NewServer returns a broker-side tunnel server. Call Start to bind.
func NewServer(addr, publicHost string, lookup TokenLookup, logger *slog.Logger) *Server {
	return &Server{
		addr:        addr,
		publicHost:  publicHost,
		tokenLookup: lookup,
		logger:      logger,
		sessions:    map[int]*serverSession{},
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
		publicPort: port,
		listener:   publicLn,
		yamuxSess:  yamuxSess,
		rawConn:    conn,
		cancel:     cancel,
	}
	s.mu.Lock()
	if s.closed {
		// Server is shutting down; don't insert into a map that's
		// been drained or is about to be. Roll the per-conn state
		// back so we don't leak the public port across broker
		// restart (the audit-found EADDRINUSE failure mode).
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
// broker has authorized us to expose. Reconnects on session loss.
type Client struct {
	brokerAddr      string
	sid             string
	nid             string
	localPortLookup LocalPortLookup
	logger          *slog.Logger

	mu       sync.Mutex
	sessions map[int]*clientSession // public port -> session
	ctx      context.Context
	cancel   context.CancelFunc
}

type clientSession struct {
	publicPort int
	conn       net.Conn
	yamuxSess  *yamux.Session
	cancel     context.CancelFunc
}

// NewClient returns an agent-side tunnel client.
func NewClient(brokerAddr, sid, nid string, lookup LocalPortLookup, logger *slog.Logger) *Client {
	return &Client{
		brokerAddr:      brokerAddr,
		sid:             sid,
		nid:             nid,
		localPortLookup: lookup,
		logger:          logger,
		sessions:        map[int]*clientSession{},
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
// token) pair. localPort is the agent-side port to dial when the
// broker pushes a new stream. Blocks until REGISTER is acknowledged
// (or fails fast on DENY).
func (c *Client) Open(publicPort, localPort int, token string) error {
	if c.ctx == nil {
		return errors.New("tunnel client: Start not called")
	}
	// TLS dial wraps the underlying TCP so REGISTER + token cannot be
	// observed by a passive listener between agent and broker
	// (architecture F.5). v1 falls back to InsecureSkipVerify because
	// the broker's self-signed cert has no PKI to validate against;
	// the threat model F.5 actually targets is passive eavesdrop and
	// that's still satisfied. See clientTLSConfig().
	dialer := &net.Dialer{Timeout: 5 * time.Second}
	conn, err := tls.DialWithDialer(dialer, "tcp", c.brokerAddr, clientTLSConfig())
	if err != nil {
		return fmt.Errorf("tunnel client: dial broker (TLS): %w", err)
	}
	line := fmt.Sprintf("REGISTER %s %s %d %s\n", c.sid, c.nid, publicPort, token)
	if _, err := conn.Write([]byte(line)); err != nil {
		_ = conn.Close()
		return fmt.Errorf("tunnel client: write REGISTER: %w", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	br := bufio.NewReader(conn)
	resp, err := br.ReadString('\n')
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("tunnel client: read REGISTER reply: %w", err)
	}
	_ = conn.SetReadDeadline(time.Time{})
	resp = strings.TrimRight(resp, "\r\n")
	if resp != "OK" {
		_ = conn.Close()
		return fmt.Errorf("tunnel client: broker denied REGISTER: %s", resp)
	}

	yamuxSess, err := yamux.Client(conn, nil)
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("tunnel client: yamux client: %w", err)
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
		old.cancel()
		_ = old.conn.Close()
		_ = old.yamuxSess.Close()
	}
	c.sessions[publicPort] = &clientSession{
		publicPort: publicPort,
		conn:       conn,
		yamuxSess:  yamuxSess,
		cancel:     cancel,
	}
	c.mu.Unlock()

	go c.streamAcceptLoop(sessCtx, publicPort, localPort, yamuxSess)
	c.logger.Info("tunnel: opened",
		"public_port", publicPort, "local_port", localPort)
	return nil
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

func (c *Client) streamAcceptLoop(ctx context.Context, publicPort, localPort int, sess *yamux.Session) {
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
