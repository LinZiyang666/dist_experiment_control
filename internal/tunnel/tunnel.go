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
	"math/rand/v2"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/LinZiyang666/tether/internal/backoff"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/tokenhash"
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
	case "public_port_bind_failed", "try_again", proto.ReasonHomeCatchingUp:
		// home_catching_up (D6 §7.2): the home replica has not yet applied the
		// latest OpPortReassignHome (presented epoch > its local row epoch). It is
		// TRANSIENT — the agent must keep retrying until the home catches up, never
		// treat it as terminal (which would brick the rehome). The reason string is
		// the proto SSOT shared with the broker emit-side (no duplicated literal).
		return true
	default:
		return false
	}
}

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
	s.mu.Unlock()

	// The accept loop owns every subsequent wg.Add for handleAgent. Register it before launch so Close's
	// Wait cannot observe a zero counter while an accepted connection is still about to Add a handler.
	// Its Done is the proof that no future handler Add is possible.
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.acceptLoop(ctx, ln)
	}()
	go func() {
		<-ctx.Done()
		s.Close()
	}()
	s.logger.Info("tunnel: server listening", "addr", s.addr)
	return nil
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
	return &tls.Config{
		GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
			s.mu.Lock()
			defer s.mu.Unlock()
			if s.tlsCert == nil {
				return nil, fmt.Errorf("tunnel server: no TLS certificate loaded")
			}
			return s.tlsCert, nil
		},
		MinVersion: tls.VersionTLS12,
	}, nil
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
		// PER-SOURCE FIRST, so one address cannot consume the process-wide budget and
		// lock the fleet out — see maxRegisterHandshakesPerIP. Refused either way
		// WITHOUT completing the TLS handshake: answering would cost exactly the work
		// the ceiling exists to refuse.
		host := remoteHostOf(conn)
		if !s.acquireHandshakeSlotForIP(host) {
			s.registerHandshakeRefused(conn, "per_ip")
			_ = conn.Close()
			continue
		}
		select {
		case handshakes <- struct{}{}:
		default:
			// At the process-wide ceiling: refuse without spending a goroutine on it.
			s.releaseHandshakeSlotForIP(host)
			s.registerHandshakeRefused(conn, "global")
			_ = conn.Close()
			continue
		}
		s.wg.Add(1)
		go func(c net.Conn, h string) {
			defer s.wg.Done()
			defer func() { <-handshakes }()
			defer s.releaseHandshakeSlotForIP(h)
			s.handleAgent(ctx, c)
		}(conn, host)
	}
}

// remoteHostOf is the source address a handshake is accounted to. A conn whose
// RemoteAddr cannot be parsed accounts to "" — one shared bucket, which is the
// conservative direction: an unattributable peer competes with other unattributable
// peers rather than getting an unmetered slot.
func remoteHostOf(conn net.Conn) string {
	addr := conn.RemoteAddr()
	if addr == nil {
		return ""
	}
	host, _, err := net.SplitHostPort(addr.String())
	if err != nil {
		return addr.String()
	}
	return host
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

// regLogClasses is the fixed, coarse REGISTER-read failure taxonomy (review
// M1). NEVER err.Error(): variable detail (a port, a byte count) would defeat
// suppression. Each class carries its OWN Tracker so a class change is a
// different Tracker, not a logged transition — independent per-class pacing.
func regLogClass(err error) string {
	var ne net.Error
	switch {
	case errors.Is(err, io.EOF):
		return "eof"
	case errors.As(err, &ne) && ne.Timeout():
		return "timeout"
	default:
		return "other"
	}
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
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-hsDone:
		}
	}()
	defer close(hsDone)

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
	fenced := s.closed || s.fenceChangedLocked(snap, port, sid, fenceKey)
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

	// The write budget covered the unauthenticated half; yamux has its own
	// ConnectionWriteTimeout and a live tunnel must not inherit an absolute
	// deadline from the handshake.
	_ = conn.SetWriteDeadline(time.Time{})
	yamuxSess, err := yamux.Server(conn, muxConfig(s.logger))
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
	if s.closed || s.fenceChangedLocked(snap, port, sid, fenceKey) {
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
	if info.PublicPort <= 0 {
		return false
	}
	s.mu.Lock()
	s.ensureAllocationFenceMapsLocked()
	key := sessionFenceKeyFromInfo(info)
	s.killGenAllocation[key]++
	s.closedAllocation[key] = true
	sess, ok := s.sessions[info.PublicPort]
	if !ok {
		s.maybePruneAllocationLocked(key)
		s.mu.Unlock()
		return false
	}
	if sess.sid != info.SID || sess.nid != info.NID || sess.tokenHash != info.TokenHash {
		s.maybePruneAllocationLocked(key)
		s.mu.Unlock()
		return false
	}
	delete(s.sessions, info.PublicPort)
	s.maybePruneAllocationLocked(key)
	s.mu.Unlock()

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
	return out
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
	if s.forgotten[sid] && s.inflightBySID[sid] <= 0 {
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
	if s.closedAllocation[key] && s.inflightByAllocation[key] <= 0 {
		delete(s.killGenAllocation, key)
		delete(s.inflightByAllocation, key)
		delete(s.closedAllocation, key)
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
		closeServerSession(sess)
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
		closeServerSession(sess)
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

	// D6 §7.5 per-expose home: the broker addr this expose dials, its home epoch,
	// and the cert pins to verify the home with. These are stored on the session
	// (NOT a single shared Client field) so one Client fans out to N homes; the
	// SUPERVISOR reads them ONLY as spawn-time value params (R-13), never back from
	// this struct in its loop. brokerAddr "" ⇒ fall back to Client.brokerAddr (N=1).
	brokerAddr string
	epoch      int64
	certPins   proto.CertPins
}

// NewClient returns an agent-side tunnel client.
func NewClient(brokerAddr, sid, nid string, lookup LocalPortLookup, logger *slog.Logger) *Client {
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
// Open is the N=1 / single-home convenience entry: it dials the Client's
// configured brokerAddr with epoch 0 and no cert pins (InsecureSkipVerify
// fallback). Equivalent to OpenHome(publicPort, localPort, token, "", 0,
// proto.CertPins{}). Kept so non-clustered callers (and existing tests) stay
// unchanged.
func (c *Client) Open(publicPort, localPort int, token string) error {
	return c.OpenHome(publicPort, localPort, token, "", 0, proto.CertPins{})
}

// OpenHome establishes (or epoch-monotone replaces) a yamux session for one
// (publicPort, token) pair against a SPECIFIC home broker (D6 §7.5), pinned by
// certPins, at home epoch. brokerAddr "" falls back to the Client's single
// configured addr (N=1). See the package doc / Open for the supervisor contract.
func (c *Client) OpenHome(publicPort, localPort int, token, brokerAddr string, epoch int64, certPins proto.CertPins) error {
	if c.ctx == nil {
		return errors.New("tunnel client: Start not called")
	}
	// Sample the rename generation BEFORE the REGISTER handshake reads the nid,
	// so the check under the lock below covers the whole dial window.
	openGen := c.nidGen.Load()
	// D6 §7.7/R-22: never dial a NAMED home (non-empty brokerAddr) insecurely.
	// A clustered expose with no pins yet (e.g. a state.json replay before the
	// register reply re-delivers them) defers — the caller retries when pins
	// arrive. The N=1 path (empty brokerAddr → Client.brokerAddr fallback) is
	// unaffected and keeps its InsecureSkipVerify.
	if brokerAddr != "" && certPins.Current == "" {
		return ErrHomePinsRequired
	}
	conn, yamuxSess, err := c.dialAndRegister(c.ctx, publicPort, token, brokerAddr, epoch, certPins)
	if err != nil {
		return fmt.Errorf("tunnel client: Open: %w", err)
	}
	sessCtx, cancel := context.WithCancel(c.ctx)

	c.mu.Lock()
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

	// R-13: the supervisor receives brokerAddr/epoch/certPins as VALUE PARAMS
	// snapshotted here — it MUST NOT read them back from c.sessions[port] in its
	// loop (that would be an unsynchronized read racing Open-replace's map write).
	go c.supervise(sessCtx, publicPort, localPort, token, brokerAddr, epoch, certPins, gen, yamuxSess)
	c.logger.Info("tunnel: opened",
		"public_port", publicPort, "local_port", localPort, "epoch", epoch)
	return nil
}

// ApplyHome applies a home directive to an OPEN expose (D6 §7.4):
//   - epoch > session epoch → an actual REHOME: Open-replace against the new home
//     (reusing the existing localPort + token), the atomic cutover.
//   - epoch == session epoch → a PURE-PIN (cert-rotation) update (R-24 / external
//     review F4): refresh the session's cert pins IN PLACE, WITHOUT tearing the
//     live transport (brokerAddr is unchanged at the same epoch; the running
//     supervisor picks the new pins up on its next redial, which reads them under
//     c.mu, gen-fenced).
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
		// Same home epoch, possibly rotated pins: update in place, no transport tear.
		if (brokerAddr != "" || sess.brokerAddr != "") && certPins.Current == "" {
			c.mu.Unlock()
			return ErrHomePinsRequired
		}
		sess.certPins = certPins
		c.mu.Unlock()
		return nil
	}
	localPort, token := sess.localPort, sess.token
	c.mu.Unlock()
	// OpenHome is the atomic replace (re-checks the epoch under mu, cancels the
	// old supervisor, installs the new session + supervisor). A transient/terminal
	// DENY surfaces to the caller for retry/handling.
	return c.OpenHome(publicPort, localPort, token, brokerAddr, epoch, certPins)
}

func (c *Client) HasSession(publicPort int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.sessions[publicPort]
	return ok
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
	// D6 §7.5: dial THIS expose's home addr; "" falls back to the single
	// configured Client.brokerAddr (the N=1 / --tunnel-addr path). The TLS config
	// pins the home cert when pins are present, else InsecureSkipVerify (N=1).
	addr := brokerAddr
	if addr == "" {
		addr = c.brokerAddr
	}
	dialer := &tls.Dialer{NetDialer: &net.Dialer{Timeout: 5 * time.Second}, Config: clientTLSConfigPinned(certPins)}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
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

	// D6 §7.2(b): 6-field REGISTER carrying this expose's home epoch (0 in N=1).
	line := fmt.Sprintf("REGISTER %s %s %d %s %d\n", c.sid, c.nidValue(), publicPort, token, epoch)
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

	yamuxSess, err := yamux.Client(conn, muxConfig(c.logger))
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
// R-13: brokerAddr/epoch are VALUE PARAMS snapshotted at the `go c.supervise(...)`
// spawn in Open — this goroutine NEVER reads THEM back from c.sessions[port]
// (which Open-replace mutates under c.mu); a rehome is a fresh Open (new
// supervisor with new params), not a mutation of this one's. certPins is the
// ONE exception: a same-epoch cert rotation (ApplyHome / R-24) updates
// sess.certPins in place, so redialWithBackoff reads the CURRENT pins under c.mu
// (gen-fenced) instead of the spawn-time value — off the hot accept path, so
// race-free. certPins here is only the initial/fallback value.
func (c *Client) supervise(ctx context.Context, publicPort, localPort int, token, brokerAddr string, epoch int64, certPins proto.CertPins, gen uint64, initial *yamux.Session) {
	sess := initial
	for {
		c.runAcceptLoop(ctx, publicPort, localPort, sess)
		if ctx.Err() != nil {
			return // intentional teardown — owner already knows; no callback
		}
		c.notifyState(publicPort, false) // down edge (one per drop)
		conn, ys, err := c.redialWithBackoff(ctx, publicPort, token, brokerAddr, epoch, certPins, gen)
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
func (c *Client) redialWithBackoff(ctx context.Context, publicPort int, token, brokerAddr string, epoch int64, certPins proto.CertPins, gen uint64) (net.Conn, *yamux.Session, error) {
	sleep := c.backoffBase
	for {
		timer := time.NewTimer(jitter(sleep))
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, nil, ctx.Err()
		case <-timer.C:
		}
		// Pick up a same-epoch cert rotation (ApplyHome / R-24): read THIS session's
		// current pins under c.mu (gen-fenced so we never read a successor's pins).
		// Off the hot accept path → race-free with ApplyHome's write.
		pins := certPins
		c.mu.Lock()
		if cur, ok := c.sessions[publicPort]; ok && cur.gen == gen {
			pins = cur.certPins
		}
		c.mu.Unlock()
		conn, ys, err := c.dialAndRegister(ctx, publicPort, token, brokerAddr, epoch, pins)
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

func parseRegisterLine(line string) (sid, nid string, port int, token string, epoch int64, ok bool) {
	line = strings.TrimRight(line, "\r\n")
	parts := strings.Split(line, " ")
	// D6 §7.2(b): the v2 REGISTER grammar is EXACTLY 6 fields
	// (REGISTER <sid> <nid> <port> <token> <epoch>). A 5-field (pre-D6) or
	// 7-field line is rejected — never mis-bound.
	if len(parts) != 6 || parts[0] != "REGISTER" {
		return "", "", 0, "", 0, false
	}
	port, err := strconv.Atoi(parts[3])
	if err != nil {
		return "", "", 0, "", 0, false
	}
	// ParseInt(_, 10, 64) matches port_allocations.epoch (int64) and rejects
	// overflow that strconv.Atoi would silently truncate; negatives are rejected
	// (epoch is a non-negative monotone counter).
	epoch, err = strconv.ParseInt(parts[5], 10, 64)
	if err != nil || epoch < 0 {
		return "", "", 0, "", 0, false
	}
	return parts[1], parts[2], port, parts[4], epoch, true
}

// hashToken is the SHA256 hex digest of raw. Kept locally
// (duplicated with internal/port.HashToken) to keep tunnel a
// dep-graph leaf: importing port would pull SQLite into the wire
// layer with no real benefit. Audit shard 06 F10 — flagged as low,
// resolved by comment, not extraction.
// hashToken delegates to the shared scheme (batch-A A11). tokenhash imports
// stdlib only, so tunnel stays a dependency-graph leaf.
func hashToken(raw string) string { return tokenhash.Sum(raw) }
