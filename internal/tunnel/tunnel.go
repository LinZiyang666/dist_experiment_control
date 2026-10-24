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
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/LinZiyang666/tether/internal/backoff"
	"github.com/LinZiyang666/tether/internal/listenerimage"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/tokenhash"
	"github.com/LinZiyang666/tether/internal/tunnel/transport"
	"github.com/hashicorp/yamux"
)

// ErrHomePinsRequired is returned when targeting a CLUSTERED home (non-empty
// brokerAddr) is targeted without cert pins (D6 §7.7/R-22). The agent must NOT
// dial a named home insecurely; it defers the dial until a register/expose reply
// re-delivers the pins. The N=1 path (empty brokerAddr) never hits this.
var ErrHomePinsRequired = errors.New("tunnel client: clustered home addr without cert pins")

// TokenLookup is what the broker calls on each REGISTER to decide
// allow/deny. Returns nil if (sid, nid, port, token) maps to an
// ALLOCATED row whose token_hash matches; non-nil error otherwise.
// Backed in production by internal/port.LookupByTokenHash. The epoch is the
// agent-presented per-port expose epoch (D6 §7.2, REGISTER 6th field); the
// production lookup ignores it when the row carries no cluster home.
type TokenLookup func(sid, nid string, port int, tokenHash string, epoch int64) error

// LocalPortLookup is the agent-side counterpart: given the public port
// the broker registered us for, return which local port to dial. The
// agent backs this by its in-memory copy of state.json port_tokens.
type LocalPortLookup func(publicPort int) (int, error)

// Limits on the UNAUTHENTICATED half of the control listener. Everything here
// runs BEFORE tokenLookup, on a socket the deployment guide tells operators to
// open to the public internet (docs/broker-ops.md: "公网放行 443 + 7000 +
// 14000-14999"), so each of these is a bound on what a stranger can make this
// process do.
//
// origin: prerelease audit broker-dataplane/BDP-F1..F3 + the main process's own
// MP-1. The file already knew this listener was hostile — the regLog damper below
// calls it "the ONE unauthenticated internet-triggerable log site" and names the
// 2026-08-11 disk-fill — but the bound it grew was on LOG VOLUME only. Bytes,
// goroutines and map entries had none.
const (
	// maxRegisterLine caps the REGISTER line. The worst LEGAL line is 146 bytes:
	// "REGISTER " (9) + sid (<=32) + nid (<=32) + port (<=5) + token (43, base64url
	// of 32 bytes from port.genToken) + epoch (<=20) + 4 separators + "\n". 512 is
	// >3x that and still refuses the unbounded read.
	//
	// bufio.Reader.ReadString has NO ceiling: collectFragments accumulates 4 KiB
	// fragments with bytes.Clone and joins them, so the peak is ~2x the line and the
	// line is whatever the attacker sends inside the 5s read deadline — a deadline
	// bounds TIME, not BYTES. Measured: feeding 200 MiB without a newline yields a
	// 200 MiB string and 406 MiB of allocation, per connection.
	maxRegisterLine = 512

	// maxRegisterHandshakes caps concurrent pre-authorization handshakes PROCESS-WIDE.
	// Every accepted conn costs a goroutine, an fd and a tls.Conn's buffers until it is
	// either denied or installed; without a cap a stranger converts connection rate
	// directly into broker memory and fd pressure.
	//
	// origin: prerelease audit round 2, CC-6's sibling CC-3. This was 256 and it was the
	// ONLY ceiling, which made it a cheaper attack than the one it replaced: the slot is
	// taken in acceptLoop BEFORE the goroutine starts, the listener is TLS so the
	// handshake does not happen until the first read inside handleAgent, and that read
	// carries a 5s deadline — so a peer that completes a bare TCP connect and then sends
	// nothing holds a slot for five seconds at a cost of one socket. 256 idle sockets
	// against a listener the deployment guide tells operators to expose to the public
	// internet kept it permanently saturated, and every agent reconnect and every
	// `tether expose` in the fleet failed with a bare EOF while the broker looked
	// healthy. Trading a memory-pressure DoS for a cheaper availability DoS is not a fix.
	//
	// The process-wide number is now an ABSOLUTE bound on goroutines and fds, sized
	// against the fd budget rather than against a fleet. What stops one source from
	// monopolising it is maxRegisterHandshakesPerIP below.
	maxRegisterHandshakes = 1024

	// maxRegisterHandshakesPerIP caps concurrent pre-authorization handshakes from ONE
	// source address, which is the half that actually preserves availability: a single
	// attacker — or a single misbehaving agent in a reconnect storm — can occupy at most
	// this many slots, so the rest of the budget stays available to everyone else.
	//
	// SIZING, and what it does NOT buy.
	//
	// The tunnel opens one control connection PER EXPOSED PORT, and the port band is 1000
	// wide (port.DefaultPortBandLow), so one host legitimately re-dials as many
	// connections as it holds exposes when a broker restarts. A first attempt at 8 was
	// derived from a wrong model of that and broke the repo's own 50-client stress test
	// immediately. 128 covers any realistic single host — and a host past it degrades to
	// a retry, not a refusal, because the agent's dial loop already backs off.
	//
	// What this buys: one source can hold at most 1/8 of the process-wide budget, so the
	// single-attacker case CC-3 describes — 256 idle sockets locking the whole fleet out
	// — costs 8x more and still leaves 7/8 of the budget for everyone else.
	//
	// What it does NOT buy, stated so nobody reads more into it: a flood from many
	// addresses still saturates the global ceiling. That is a firewall and connection-rate
	// problem, not something a per-connection accounting map can solve, and pretending
	// otherwise here would be the same mistake as the original 256.
	maxRegisterHandshakesPerIP = 128

	// registerWriteBudget bounds the writes on the unauthenticated half (the TLS
	// handshake's own writes, every DENY, and the OK). Without it a peer that never
	// reads — a zero-window socket, or the half-open link #78 documents — parks the
	// handler in the kernel's retransmit path with nothing able to interrupt it.
	// MUST be cleared before yamux takes over: a deadline is an absolute instant, so
	// leaving it set would fail every tunnel write 5 seconds later.
	registerWriteBudget = 5 * time.Second
)

// yamuxLogger routes yamux's own logging into slog instead of a bare fd 2.
//
// origin: prerelease audit broker-dataplane (verifier-found). yamux.DefaultConfig
// sets LogOutput: os.Stderr, and both mux constructors passed nil. Those lines are
// emitted PER FRAME and do not terminate the session ("Discarding data for stream",
// "frame for missing stream" — normal noise whenever a stream is reset, which the
// __proxy__ workload does constantly). On the broker they land in journald, outside
// the broker.log an operator reads and outside h1's in-process size cap; on the
// AGENT they land in the panic sink, whose ceiling is applied only at open time —
// so a long-running process can crowd out the stack trace the sink exists to keep.
// cmd/tether/agent_logsink.go states the invariant this violated: "no tether code
// does" scribble unboundedly on fd 2.
//
// Debug level is deliberate: this is torn-stream noise, so at the default level it
// writes nothing at all, and `--log-level debug` gets it back — strictly better
// than today's "cannot be turned off and lands where nobody looks".
// muxConfig returns yamux's defaults with logging redirected to l. yamux.VerifyConfig
// requires exactly one of Logger/LogOutput, so LogOutput must be nil'd.
func muxConfig(l *slog.Logger) *yamux.Config {
	c := yamux.DefaultConfig()
	c.LogOutput = nil
	c.Logger = log.New(yamuxWriter{l}, "", 0)
	return c
}

// yamuxWriter adapts slog to the *log.Logger yamux.Config.Logger expects.
type yamuxWriter struct{ l *slog.Logger }

func (w yamuxWriter) Write(p []byte) (int, error) {
	w.l.Debug(strings.TrimSuffix(string(p), "\n"))
	return len(p), nil
}

// Server is the broker side of the tunnel. Owns the control listener
// (`:7000`) plus one public listener per registered (agent, port).
type Server struct {
	// Installed by P1 only; legacy paths never inspect or invoke this owner.
	// origin: hot-update S5 plan §2 / L13
	flowState *serverFlowState
	// Pristine metadata follows the Server terminal owner until activation/detach.
	// origin: hot-update S6 plan §4.2; internal review rv-fd-8
	importedImage *serverImportedImage

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
	// killGenAllocation is bumped by CloseProxyIf for one allocation identity.
	// Unlike killGen, it does not fence a newly reallocated port with a different
	// token_hash; this keeps stale cluster-close broadcasts from denying the new
	// listener while still killing an old in-flight REGISTER for the freed row.
	killGenAllocation map[sessionFenceKey]int64
	// killGenSession[sid] is bumped by CloseSession — even when NO listener is
	// installed yet — so a session-level OFF also fences an in-flight REGISTER
	// that passed token auth but hasn't inserted its serverSession (round-5 F1).
	killGenSession map[string]int64
	// inflightBySID[sid] counts REGISTER handlers currently between snapshot and
	// install for sid; forgotten[sid] marks a deleted session awaiting prune.
	// ForgetSession BUMPS the fence and only prunes once in-flight drains, so a
	// paused authorized REGISTER can't install after deletion (round-6 F4).
	inflightBySID        map[string]int
	forgotten            map[string]bool
	inflightByAllocation map[sessionFenceKey]int
	closedAllocation     map[sessionFenceKey]bool
	// closed flips true after Close()/ctx-cancel begin. Guards against
	// the audit-found shutdown race: in-flight handleAgent that's mid
	// REGISTER could otherwise insert into s.sessions AFTER Close
	// drained it, leaking the bound public port across broker restarts.
	closed bool
	ln     net.Listener   // control listener; Close() must close it
	wg     sync.WaitGroup // tracks in-flight handleAgent goroutines

	// Admission, accepted handshakes and carrier monitors have separate joins.
	// origin: hot-update S6 plan §6.2 / T23
	imagePaused   bool
	retiringImage bool
	// Installed only after the image safe point has joined workers. A hard
	// revoke then shares Prepare/ExecCommit's latch and closes every Prepared
	// alias before its authoritative caller can acknowledge the mutation.
	// origin: hot-update S6 plan §6.2 / T25
	imageRevoke   func([]string, func() error) error
	legacyWorkers uint32
	controlKeeper listenerimage.Keeper
	controlRaw    net.Listener
	runCtx        context.Context
	acceptDone    chan struct{}
	lifetimeDone  chan struct{}
	lifetimeOnce  sync.Once
	controlConns  map[net.Conn]chan struct{}
	monitors      map[*flowCarrier]chan struct{}
	callbacks     map[*endpointTimer]struct{}

	// regLog (#78) dampens the `read REGISTER` WARN — the ONE unauthenticated
	// internet-triggerable log site on this listener. A single agent behind a
	// half-open :7000 (TCP connects, bytes die) produced one WARN every 5s
	// forever, the fuel of the 2026-08-11 disk-fill.
	//
	// PER-CLASS Trackers (review M1): backoff.Tracker's log discipline fires
	// on the first failure of a run AND on a class change — that class-change
	// rule is correct for a SERIAL single-owner loop ("a new problem must not
	// hide behind an old one's suppression"), but :7000 is a public port with
	// MANY unrelated sources. Sharing one Tracker across classes meant two
	// coexisting sources (an EOF prober + a timeout half-open agent) flip the
	// class every event and log every time — the flood, undamped. So each
	// coarse class {eof, timeout, other} gets its OWN Tracker: a class change
	// is now a different Tracker, not a logged transition, and each class is
	// independently paced. Fixed 3-wide, bounded memory — still the ONE damper
	// (backoff.Tracker), not a second throttle (h1 E1).
	//
	// Each Tracker also uses Due() for time-based reaffirmation (review Mi5):
	// a suppressed class re-logs once when its backoff instant passes, so a
	// quiet broker's cross-week event is not silenced for the whole process
	// lifetime and the Cap is a live "at most one line per Cap" pacing.
	// regLogMu guards the map: handleAgent is one goroutine per conn and
	// Tracker is not goroutine-safe.
	regLogMu sync.Mutex
	regLog   map[string]*backoff.Tracker
	// regLogNow is the damper's clock seam (nil ⇒ time.Now). Test-only
	// injection via SetRegLogClockForTest — Recover's anti-flap floor needs
	// a controllable clock to be assertable.
	regLogNow func() time.Time

	// hsPerIP counts IN-FLIGHT pre-authorization handshakes per source address, so one
	// address cannot consume the process-wide ceiling (round 2, CC-3). Entries are
	// deleted on the way down, so the map holds at most one key per address with a live
	// handshake — an unbounded scan of source addresses cannot grow it, because a
	// refused connection never gets a key and a completed one removes its own.
	hsMu    sync.Mutex
	hsPerIP map[string]int
}

// SessionInfo is the stable identity of an installed public proxy listener.
// TokenHash fences port reuse: a stale close for an old allocation must not close
// a new allocation that reused the same public port.
type SessionInfo struct {
	PublicPort int    `json:"port"`
	SID        string `json:"sid"`
	NID        string `json:"nid"`
	TokenHash  string `json:"token_hash"`
	Epoch      int64  `json:"epoch"`
}

type sessionFenceKey struct {
	publicPort int
	sid        string
	nid        string
	tokenHash  string
}

func sessionFenceKeyFor(publicPort int, sid, nid, tokenHash string) sessionFenceKey {
	return sessionFenceKey{
		publicPort: publicPort,
		sid:        sid,
		nid:        nid,
		tokenHash:  tokenHash,
	}
}

func sessionFenceKeyFromInfo(info SessionInfo) sessionFenceKey {
	return sessionFenceKeyFor(info.PublicPort, info.SID, info.NID, info.TokenHash)
}

type serverSession struct {
	sid        string // owning session — lets CloseSession kill by sid without a DB query
	nid        string
	publicPort int
	tokenHash  string
	epoch      int64
	listener   net.Listener
	yamuxSess  *yamux.Session
	rawConn    net.Conn // the raw TCP control connection from agent
	cancel     context.CancelFunc
}

// NewServer returns a broker-side tunnel server. Call Start to bind.
func NewServer(addr, publicHost string, lookup TokenLookup, logger *slog.Logger) *Server {
	return newServer(addr, publicHost, lookup, nil, logger)
}

// NewServerWithCert is like NewServer but pins a STABLE persistent tunnel cert
// (D6 §15 RF3) instead of generating an ephemeral self-signed one on Start, so
// the advertised fingerprint survives restarts and the agent can pin it. It is
// HARNESS/TEST ONLY in D6 build-and-prove — production keeps calling NewServer
// (cert nil → ephemeral, the N=1 fallback); the stable-cert cutover is D9. The
// build-and-prove guard bans NewServerWithCert( from production files.
func NewServerWithCert(addr, publicHost string, lookup TokenLookup, cert *tls.Certificate, logger *slog.Logger) *Server {
	return newServer(addr, publicHost, lookup, cert, logger)
}

func newServer(addr, publicHost string, lookup TokenLookup, cert *tls.Certificate, logger *slog.Logger) *Server {
	return &Server{
		addr:                 addr,
		publicHost:           publicHost,
		tokenLookup:          lookup,
		tlsCert:              cert,
		logger:               logger,
		sessions:             map[int]*serverSession{},
		killGen:              map[int]int64{},
		killGenAllocation:    map[sessionFenceKey]int64{},
		killGenSession:       map[string]int64{},
		inflightBySID:        map[string]int{},
		forgotten:            map[string]bool{},
		inflightByAllocation: map[sessionFenceKey]int{},
		closedAllocation:     map[sessionFenceKey]bool{},
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
	tlsCfg, err := s.serverTLSConfig()
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
	s.controlRaw = rawLn
	s.runCtx = ctx
	s.acceptDone = make(chan struct{})
	s.lifetimeDone = make(chan struct{})
	acceptDone, lifetimeDone := s.acceptDone, s.lifetimeDone
	s.mu.Unlock()

	// The accept loop owns every subsequent wg.Add for handleAgent. Register it before launch so Close's
	// Wait cannot observe a zero counter while an accepted connection is still about to Add a handler.
	// Its Done is the proof that no future handler Add is possible.
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer close(acceptDone)
		s.acceptLoop(ctx, ln)
	}()
	go func() {
		select {
		case <-ctx.Done():
			s.Close()
		case <-lifetimeDone:
		}
	}()
	s.logger.Info("tunnel: server listening", "addr", s.addr)
	return nil
}

// StartOwned keeps the data-plane lifetime independent of Run's stop signal.
// RuntimeSlot performs terminal Close after a canceled cut has reclaimed its
// Prepared aliases; a context watcher must never SHUT_RDWR exported raw OFDs.
// origin: hot-update S6 plan §1.2 / rv-fd-6
func StartOwned(s *Server) error {
	return s.Start(context.Background()) // ctx-root: explicit broker data-plane owner.
}

func (s *Server) SetCertificate(cert *tls.Certificate) {
	s.mu.Lock()
	s.tlsCert = cert
	s.mu.Unlock()
}

func (s *Server) serverTLSConfig() (*tls.Config, error) {
	s.mu.Lock()
	if s.tlsCert == nil {
		c, err := generateSelfSignedCert()
		if err != nil {
			s.mu.Unlock()
			return nil, err
		}
		s.tlsCert = &c
	}
	s.mu.Unlock()
	cfg := &tls.Config{
		GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
			s.mu.Lock()
			defer s.mu.Unlock()
			if s.tlsCert == nil {
				return nil, fmt.Errorf("tunnel server: no TLS certificate loaded")
			}
			return s.tlsCert, nil
		},
		MinVersion: tls.VersionTLS12,
	}
	s.mu.Lock()
	enabled := s.flowState != nil
	s.mu.Unlock()
	// origin: hot-update S5 plan §4.1 / T15
	cfg = flowServerTLSConfig(cfg, enabled)
	return cfg, nil
}

func (s *Server) acceptLoop(ctx context.Context, ln net.Listener) {
	// handshakes bounds concurrent PRE-AUTHORIZATION handlers (BDP-F1's second half):
	// without it an unauthenticated peer turns connection rate straight into
	// goroutines and fds. A conn that cannot get a slot is closed immediately rather
	// than queued, so the cap is a real ceiling and not a delay.
	handshakes := make(chan struct{}, maxRegisterHandshakes)
	// delay is the net/http-shaped accept backoff. origin: BDP-F3 — Go's poll.FD.Accept
	// retries EINTR/EAGAIN/ECONNABORTED but NOT EMFILE/ENFILE, so on fd exhaustion the
	// old `Warn; continue` became a full-speed spin: one core pinned (starving raft and
	// NATS goroutines on a 1-2 core VPS) and the 150 MiB of rotated log history
	// overwritten within seconds, destroying the evidence of the incident in progress.
	var delay time.Duration
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			// A closed listener is terminal, and it must be checked INDEPENDENTLY of
			// s.closed: today Close() sets that flag first, but with a backoff in place
			// any future path that closes this listener without going through
			// Server.Close would otherwise become a permanent once-per-second spin.
			if errors.Is(err, net.ErrClosed) {
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
			if delay == 0 {
				delay = 5 * time.Millisecond
			} else {
				delay *= 2
			}
			if delay > time.Second {
				delay = time.Second
			}
			s.logger.Warn("tunnel server: accept", "err", err, "retry_in", delay)
			select {
			case <-ctx.Done():
				return
			case <-time.After(delay):
			}
			continue
		}
		delay = 0
		if !trackControlConnection(s, conn) {
			_ = conn.Close()
			continue
		}
		// PER-SOURCE FIRST, so one address cannot consume the process-wide budget and
		// lock the fleet out — see maxRegisterHandshakesPerIP. Refused either way
		// WITHOUT completing the TLS handshake: answering would cost exactly the work
		// the ceiling exists to refuse.
		host := remoteHostOf(conn)
		if !s.acquireHandshakeSlotForIP(host) {
			s.registerHandshakeRefused(conn, "per_ip")
			untrackControlConnection(s, conn)
			_ = conn.Close()
			continue
		}
		select {
		case handshakes <- struct{}{}:
		default:
			// At the process-wide ceiling: refuse without spending a goroutine on it.
			s.releaseHandshakeSlotForIP(host)
			s.registerHandshakeRefused(conn, "global")
			untrackControlConnection(s, conn)
			_ = conn.Close()
			continue
		}
		s.wg.Add(1)
		go func(c net.Conn, h string) {
			defer s.wg.Done()
			defer untrackControlConnection(s, c)
			defer func() { <-handshakes }()
			defer s.releaseHandshakeSlotForIP(h)
			s.handleAgent(ctx, c)
		}(conn, host)
	}
}

// acquireHandshakeSlotForIP takes one of host's maxRegisterHandshakesPerIP slots,
// reporting whether it got one. Paired with releaseHandshakeSlotForIP on EVERY exit
// path — including the global-ceiling refusal, which acquires this one first.
func (s *Server) acquireHandshakeSlotForIP(host string) bool {
	s.hsMu.Lock()
	defer s.hsMu.Unlock()
	if s.hsPerIP == nil {
		s.hsPerIP = map[string]int{}
	}
	if s.hsPerIP[host] >= maxRegisterHandshakesPerIP {
		return false
	}
	s.hsPerIP[host]++
	return true
}

// releaseHandshakeSlotForIP returns a slot and DELETES the key at zero — the same
// delete-on-zero discipline releaseInflightLocked uses, and for the same reason: a
// counter left at 0 is a permanent map entry keyed by something the peer chose.
func (s *Server) releaseHandshakeSlotForIP(host string) {
	s.hsMu.Lock()
	defer s.hsMu.Unlock()
	if s.hsPerIP[host] <= 1 {
		delete(s.hsPerIP, host)
		return
	}
	s.hsPerIP[host]--
}

// registerHandshakeRefused logs a refused-at-the-ceiling connection through the
// SAME damper as the read failures below — this site is reachable by an
// unauthenticated flood, so an undamped line here would be the log flood the
// damper exists to prevent, arriving through a different door.
//
// `which` distinguishes the per-IP ceiling from the process-wide one, because they
// mean different things to an operator: the first is one source misbehaving, the
// second is the whole listener saturated.
func (s *Server) registerHandshakeRefused(conn net.Conn, which string) {
	remote := remoteHostOf(conn)
	// A DEDICATED damper class per ceiling, not registerReadFailed's.
	//
	// origin: prerelease audit round 2, C6. Routing this through registerReadFailed was
	// wrong twice: regLogClass has a fixed {eof, timeout, other} taxonomy so a fresh
	// sentinel collapses into "other" and shares a bucket with genuine read failures,
	// and the line it produced said "read REGISTER" when no REGISTER was ever read —
	// the connection was refused before the TLS handshake.
	class := "handshake_ceiling_" + which
	s.regLogMu.Lock()
	now := s.regLogNowTime()
	t := s.regLogTracker(class)
	logNow, suppressed := t.FailReaffirm(now, class)
	fails := t.Fails()
	s.regLogMu.Unlock()
	if logNow {
		s.logger.Warn("tunnel server: refused a control connection at the handshake ceiling",
			"ceiling", which, "remote", remote, "suppressed_since_last", suppressed)
		return
	}
	s.logger.Debug("tunnel server: handshake ceiling refusal (suppressed repeat)",
		"ceiling", which, "remote", remote, "fails", fails)
}

// regLogTracker returns the per-class Tracker, creating it on first use.
// Caller holds regLogMu.
func (s *Server) regLogTracker(class string) *backoff.Tracker {
	if s.regLog == nil {
		s.regLog = map[string]*backoff.Tracker{}
	}
	t := s.regLog[class]
	if t == nil {
		t = backoff.New(backoff.Policy{Base: 30 * time.Second, Cap: 5 * time.Minute})
		s.regLog[class] = t
	}
	return t
}

func (s *Server) regLogNowTime() time.Time {
	if s.regLogNow != nil {
		return s.regLogNow()
	}
	return time.Now()
}

// registerReadFailed (#78) is the damped replacement for the per-connection
// `read REGISTER` WARN. The WARN carries the remote HOST (port stripped —
// ephemeral) so the operator can finally see WHO is dialing, plus how many
// identical failures were swallowed since the last logged line. Suppressed
// repeats drop to Debug. Each coarse class is paced by its own Tracker, and
// Due() gives a per-Cap reaffirmation so a quiet broker's cross-week event of
// the same class is not silenced for the whole process lifetime.
func (s *Server) registerReadFailed(conn net.Conn, err error) {
	class := regLogClass(err)
	remote := ""
	if addr := conn.RemoteAddr(); addr != nil {
		remote = addr.String()
		if host, _, splitErr := net.SplitHostPort(remote); splitErr == nil {
			remote = host
		}
	}
	s.regLogMu.Lock()
	now := s.regLogNowTime()
	t := s.regLogTracker(class)
	// FailReaffirm is the ONE primitive (external review F4): it logs on the
	// first failure of a run and once per Cap window thereafter (reaffirmation),
	// and RESETS the suppressed counter each time it says to log — so the next
	// line never re-reports a count already disclosed. The old hand-cobbled
	// Due+Fail double-counted: Fail incremented suppressed AFTER a reaffirmation
	// read it, so every subsequent line reported an accumulating value.
	logNow, suppressed := t.FailReaffirm(now, class)
	fails := t.Fails()
	s.regLogMu.Unlock()
	if logNow {
		s.logger.Warn("tunnel server: read REGISTER",
			"class", class, "err", err, "remote", remote, "suppressed_since_last", suppressed)
	} else {
		s.logger.Debug("tunnel server: read REGISTER (suppressed repeat)",
			"class", class, "err", err, "remote", remote, "fails", fails)
	}
}

// registerDenied logs an authentication refusal through the SAME damper the read
// failures use, so a flood of denials costs one line per Cap window instead of one per
// connection. The reason a refusal deserves the damper and not silence: a legitimate
// agent whose token was rotated hits this branch, and an operator needs to see it.
//
// The damper is keyed on a fixed class rather than on anything from the wire — keying it
// on sid would give an attacker a fresh bucket per made-up session, which is the damper
// paying for the attack instead of bounding it.
func (s *Server) registerDenied(conn net.Conn, sid, nid string, port int, epoch int64, err error) {
	const class = "denied"
	remote := ""
	if addr := conn.RemoteAddr(); addr != nil {
		remote = addr.String()
		if host, _, splitErr := net.SplitHostPort(remote); splitErr == nil {
			remote = host
		}
	}
	s.regLogMu.Lock()
	now := s.regLogNowTime()
	t := s.regLogTracker(class)
	logNow, suppressed := t.FailReaffirm(now, class)
	fails := t.Fails()
	s.regLogMu.Unlock()
	if logNow {
		s.logger.Info("tunnel server: REGISTER denied",
			"sid", sid, "nid", nid, "port", port, "epoch", epoch, "err", err,
			"remote", remote, "suppressed_since_last", suppressed)
		return
	}
	s.logger.Debug("tunnel server: REGISTER denied (suppressed repeat)",
		"sid", sid, "nid", nid, "err", err, "remote", remote, "fails", fails)
}

// registerReadOK (#78) ends every class's read-failure run on the first
// AUTHORIZED REGISTER (called after tokenLookup succeeds — review M2: an
// unauthenticated garbage line that merely parses must NOT fake a recovery
// nor re-arm the WARN). Recover's anti-flap floor folds a run shorter than
// the Base so a tight success/failure interleave never amplifies. A genuine
// authorized register means the listener is healthy again, so ALL classes
// recover together.
func (s *Server) registerReadOK() {
	s.regLogMu.Lock()
	now := s.regLogNowTime()
	type rec struct {
		class      string
		suppressed int
	}
	var recovered []rec
	for class, t := range s.regLog {
		if t.Failing() {
			if sup, ok := t.Recover(now); ok {
				recovered = append(recovered, rec{class, sup})
			}
		}
	}
	s.regLogMu.Unlock()
	for _, r := range recovered {
		s.logger.Info("tunnel server: REGISTER reads recovered", "class", r.class, "suppressed", r.suppressed)
	}
}

// handleAgent reads the REGISTER line, validates, then runs the
// public-port acceptor. One goroutine per agent control conn (= per
// public port).
func (s *Server) handleAgent(ctx context.Context, conn net.Conn) {
	// ctx must be able to interrupt a stuck Write/Read before yamux takes over, or
	// Close's wg.Wait has no bound at all. The client side of this same file already
	// does exactly this and says why (dialAndRegister's hsDone watcher); the server
	// side never did, so Close's "Bound by the per-session cancellation above; can't
	// run forever" was false for any handler that had not yet installed a session —
	// per-session cancellation cannot reach a handler that has no serverSession yet.
	hsDone := make(chan struct{})
	hsJoined := make(chan struct{})
	go func() {
		defer close(hsJoined)
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-hsDone:
		}
	}()
	defer func() { close(hsDone); <-hsJoined }()

	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	_ = conn.SetWriteDeadline(time.Now().Add(registerWriteBudget))
	// NewReaderSize + ReadSlice, NOT ReadString: the buffer IS the ceiling. A line
	// that does not fit returns bufio.ErrBufferFull, which we hand to the existing
	// damped failure path rather than minting a new DENY reason — that keeps this
	// call site byte-identical for the source-text gate in
	// register_log_damping_test.go (TestHandleAgentWiresTheDamper) and costs the
	// prober no new information.
	br := bufio.NewReaderSize(conn, maxRegisterLine)
	lineB, err := br.ReadSlice('\n')
	if err != nil {
		s.registerReadFailed(conn, err)
		_ = conn.Close()
		return
	}
	// Copy off the reader's buffer. parseRegisterLine returns strings.Split
	// sub-slices, which alias whatever the reader holds; the copy is <=512 bytes and
	// makes the retained keys independent of it.
	line := string(lineB)
	_ = conn.SetReadDeadline(time.Time{})

	sid, nid, port, token, epoch, ok := parseRegisterLine(line)
	if !ok {
		_, _ = conn.Write([]byte("DENY malformed_register\n"))
		_ = conn.Close()
		return
	}
	// VALIDATE BEFORE TOUCHING ANY MAP OR ANY LOG LINE.
	//
	// origin: prerelease audit — main process MP-1(b)/(c). parseRegisterLine only
	// splits into 6 fields and range-checks port/epoch; sid and nid arrive from an
	// unauthenticated peer completely unconstrained. Everything below this point
	// either keys a long-lived map on them (fenceKey / inflightBySID) or prints them
	// (the DENY log), so an unvalidated pair is both a permanent allocation and an
	// unbounded log field. The fuzz seed for this parser has said "empty sid: syntax
	// accepts, caller validates" since it was written — this is the caller finally
	// doing it. Every legitimate REGISTER already satisfies these: an agent refuses
	// to start on an invalid sid/nid (internal/agent.New) and auth_callout refuses to
	// admit one (internal/authcallout), and a lease name `<base>-NN` is inside
	// ValidateNID's budget by construction (proto.MaxLeaseBasenameLen).
	if proto.ValidateSID(sid) != nil || proto.ValidateNID(nid) != nil {
		_, _ = conn.Write([]byte("DENY malformed_register\n"))
		_ = conn.Close()
		return
	}
	tokenHash := hashToken(token)
	fenceKey := sessionFenceKeyFor(port, sid, nid, tokenHash)

	// Snapshot BOTH the port and session kill generations BEFORE authorizing.
	// If a CloseProxy(port) [round-2 F1] or a CloseSession(sid) [round-5 F1]
	// lands during our handshake (the proxy-off kill switch firing between the
	// token lookup and session install), the matching generation advances and we
	// abort the install — so an already-authorized REGISTER can't resurrect the
	// public listener after OFF returned, whether OFF closed by port or by sid.
	s.mu.Lock()
	snap := s.fenceSnapLocked(port, sid, fenceKey)
	// round-6 F4: mark this REGISTER in-flight for sid so ForgetSession can't
	// prune the session tombstone (and lose the fence) while we're paused
	// between snapshot and install. Decremented when the handler returns.
	s.inflightBySID[sid]++
	s.inflightByAllocation[fenceKey]++
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.releaseInflightLocked(sid, fenceKey)
		s.mu.Unlock()
	}()

	// A selected v1 carrier enters the independent Endpoint owner. Legacy
	// listeners retain their established lifecycle and exact REGISTER grammar.
	// origin: hot-update S5 plan §4.1 / §4.2
	if selectedFlow(conn) {
		if s.flowState == nil {
			_ = conn.Close()
			return
		}
		handleFlowRegister(s, conn, br, Allocation{SID: sid, NID: nid, Port: port, TokenHash: tokenHash, Epoch: epoch}, snap, ctx.Done())
		return
	}
	if err := s.tokenLookup(sid, nid, port, tokenHash, epoch); err != nil {
		// THROUGH THE DAMPER, like every other line on this listener.
		//
		// origin: prerelease audit, main-process MP-1 leg (c), completed. The identifier
		// validation added earlier bounds what these fields can CONTAIN; it does nothing
		// about how OFTEN the line is written. :7000 must be reachable from the public
		// internet (a NAT-ed agent dials it), the TLS is self-signed so anyone completes
		// the handshake, and this branch is one Info line per rejected connection. That
		// is the shape of gotcha #78 — a disk filled by an unthrottled log line on this
		// very listener — reproduced one function further down.
		s.registerDenied(conn, sid, nid, port, epoch, err)
		_, _ = conn.Write([]byte("DENY " + err.Error() + "\n"))
		_ = conn.Close()
		return
	}
	// M2: recovery is signalled only by an AUTHORIZED register — an
	// unauthenticated garbage line (which passed parse but fails tokenLookup)
	// must not fake a recovery Info nor re-arm the damper.
	s.registerReadOK()

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
	fenced := s.closed || s.imagePaused || s.fenceChangedLocked(snap, port, sid, fenceKey)
	s.mu.Unlock()
	if fenced {
		_ = publicLn.Close()
		_, _ = conn.Write([]byte("DENY session_closed\n"))
		_ = conn.Close()
		return
	}

	if err := writeRegisterOK(conn); err != nil {
		_ = publicLn.Close()
		_ = conn.Close()
		return
	}

	// The write budget covered the unauthenticated half; yamux has its own
	// ConnectionWriteTimeout and a live tunnel must not inherit an absolute
	// deadline from the handshake.
	_ = conn.SetWriteDeadline(time.Time{})
	yamuxSess, err := yamux.Server(retainRegisterReader(conn, br), muxConfig(s.logger))
	if err != nil {
		s.logger.Warn("tunnel server: yamux server", "err", err)
		_ = publicLn.Close()
		_ = conn.Close()
		return
	}

	sessCtx, cancel := context.WithCancel(ctx)
	sess := &serverSession{
		sid:        sid,
		nid:        nid,
		publicPort: port,
		tokenHash:  tokenHash,
		epoch:      epoch,
		listener:   publicLn,
		yamuxSess:  yamuxSess,
		rawConn:    conn,
		cancel:     cancel,
	}
	s.mu.Lock()
	if s.closed || s.imagePaused || s.fenceChangedLocked(snap, port, sid, fenceKey) {
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
	var old *serverSession
	if prior, ok := s.sessions[port]; ok {
		old = prior
	}
	s.sessions[port] = sess
	s.mu.Unlock()
	if old != nil {
		closeServerSession(old)
	}

	s.logger.Info("tunnel: registered",
		"sid", sid, "nid", nid, "public_port", port)

	// Watch the yamux session's close channel so a client-side Close
	// (or network drop) tears down the public listener instead of
	// leaving an orphan accepting connections that go nowhere.
	launchLegacyServerWorker(s, func() {
		<-yamuxSess.CloseChan()
		cancel()
		_ = sess.listener.Close()
	})

	launchLegacyServerWorker(s, func() { s.publicAcceptLoop(sessCtx, sess) })
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

// hashToken is the SHA256 hex digest of raw. Kept locally
// (duplicated with internal/port.HashToken) to keep tunnel a
// dep-graph leaf: importing port would pull SQLite into the wire
// layer with no real benefit. Audit shard 06 F10 — flagged as low,
// resolved by comment, not extraction.
// hashToken delegates to the shared scheme (batch-A A11). tokenhash imports
// stdlib only, so tunnel stays a dependency-graph leaf.
func hashToken(raw string) string { return tokenhash.Sum(raw) }

// The runtime retains its API while the leaf owns this implementation.
// origin: hot-update S6 plan §9.3 / TUNNEL-SLIM
var ErrRegisterDenied = transport.ErrRegisterDenied

type DenyError = transport.DenyError

func denyIsTransient(reason string) bool { return retryRegisterDenied(reason) }

// Primitive REGISTER parsing and damping classification stay below the runtime.
// origin: hot-update S6 plan §9.3 / TUNNEL-SLIM
func remoteHostOf(conn net.Conn) string { return transport.RemoteHostOf(conn) }
func regLogClass(err error) string      { return transport.RegLogClass(err) }
func parseRegisterLine(line string) (sid, nid string, port int, token string, epoch int64, ok bool) {
	return transport.ParseRegisterLine(line)
}
