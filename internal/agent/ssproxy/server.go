package ssproxy

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"runtime"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/LinZiyang666/tether/internal/rawsocket"
)

const (
	handshakeTimeout = 10 * time.Second
	dialTimeout      = 10 * time.Second
)

// Key is one active subscriber's Shadowsocks credential. KeyID is the
// subscriber's stable internal id (sub_id) so revocation can target a
// single entry; Secret is the SS password string (the Clash `password`
// field). The master key is derived on SetKeys, never stored elsewhere.
type Key struct {
	KeyID  string
	Secret string
}

type keyEntry struct {
	keyID    string
	master   []byte
	material *materialState
}

// ErrStopped is returned by SetKeys (and by Start) once the server has been
// stopped. A Server is SINGLE-USE: shutdown latches `closed` one way and there is
// no restart -- callers rebuild by constructing a fresh New().
//
// origin: 2026-08-21 weilandserver incident. The agent treated the stopped error as
// an ignorable transient, logged a WARN and returned, so a keyset push arriving at a
// dead server did nothing 5416 times over 7.5 hours. It is a TERMINAL signal meaning
// "this object is spent; build another one", and it is a typed sentinel so callers can
// act on it with errors.Is instead of matching a string. The message text is preserved
// verbatim from the pre-fix fmt.Errorf so operator greps and the simcluster log oracle
// keep matching historical logs.
var ErrStopped = errors.New("ssproxy: server stopped")

// Server is the embedded Shadowsocks AEAD server. It binds 127.0.0.1 only
// and is reachable from the public internet exclusively via the broker's
// yamux tunnel. Safe for concurrent use; all methods are idempotent where
// noted.
type Server struct {
	// The actor owner outlives control-plane sessions and transient image cuts.
	// origin: hot-update S5 plan §2 / L13
	imageState *serverImageState

	logger *slog.Logger

	mu        sync.Mutex
	ln        net.Listener
	localPort int
	keys      []*keyEntry                      // copy-on-write snapshot
	allConns  map[net.Conn]struct{}            // every conn this server owns -- accepted AND upstream (closed by Stop; see runActor)
	keyConns  map[string]map[net.Conn]struct{} // keyID -> live conns (force-close on revoke)
	closed    bool
	// acceptExited latches when the accept loop returns. It exists so Serving can
	// answer "am I able to serve?" rather than "was I once started": an accept loop
	// that died on a non-temporary listener error leaves closed==false and ln!=nil,
	// and without this the server would still claim to be serving.
	acceptExited bool

	// stopCtx/stopCancel are the server's OWN operation-scoped cancellation, created in
	// Start and cancelled in shutdown. They are what let Stop interrupt a handler that is
	// blocked in DNS resolution or an upstream dial.
	//
	// origin: proxy-lifecycle external review F1/F5. The first cut of this increment banned
	// context from the package outright, having concluded from the incident that "ctx is the
	// problem". That conflated two different things. What killed the data plane was a ctx
	// whose lifetime was owned by the CALLER (the agent's per-session runCtx): cancelling it
	// meant "your session ended", and the server wrongly took that as "you should die".
	// Cancellation the server creates for ITSELF has the opposite property — it can only fire
	// when this server is being stopped, which is exactly when the in-flight lookup should be
	// abandoned. Banning it left `Stop` waiting on the system resolver (no deadline at all;
	// resolv.conf-bound, typically 30s) while the agent held proxyRuntime.mu, which can
	// starve heartbeats and cross the broker's rehome dwell into a public-port rotation.
	//
	// The invariant is therefore "no CALLER-OWNED context reaches this type", not "no
	// context exists here" — and it is enforced by Start's signature, not by an import ban.
	stopCtx    context.Context
	stopCancel context.CancelFunc

	// allowPrivate governs the destination policy (round-6 F12). The library
	// DEFAULT (New) is permissive so in-process tests can target loopback; the
	// AGENT flips it off (DenyPrivateDestinations) so a subscription can't reach
	// agent-loopback / LAN / link-local / metadata endpoints.
	allowPrivate bool

	wg sync.WaitGroup
}

// New returns an unstarted Server. The destination policy defaults to permissive
// (DenyPrivateDestinations tightens it; the agent does so by default).
func New(logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Server{logger: logger, allowPrivate: true}
}

// DenyPrivateDestinations switches the server to a deny-by-default destination
// policy: after decrypting the SOCKS target it refuses loopback, private,
// link-local, multicast, unspecified, and metadata addresses (round-6 F12).
func (s *Server) DenyPrivateDestinations() {
	s.mu.Lock()
	s.allowPrivate = false
	s.mu.Unlock()
}

func (s *Server) allowPrivateNow() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.allowPrivate
}

// blockedCIDRs are IANA special-purpose / non-publicly-routable prefixes that
// the category checks (IsPrivate/IsLoopback/…) miss — notably RFC 6598 shared
// address space (incl. the 100.100.100.200 cloud-metadata endpoint) and RFC 2544
// benchmarking space, which DO satisfy IsGlobalUnicast (round-7 F2).
var blockedCIDRs = mustParseCIDRs(
	"0.0.0.0/8",       // "this host on this network"
	"100.64.0.0/10",   // RFC 6598 shared (CGNAT) — includes 100.100.100.200 metadata
	"169.254.0.0/16",  // link-local (also IsLinkLocalUnicast) — 169.254.169.254 metadata
	"192.0.0.0/24",    // IETF protocol assignments
	"192.0.2.0/24",    // TEST-NET-1
	"192.88.99.0/24",  // 6to4 relay anycast (deprecated)
	"198.18.0.0/15",   // RFC 2544 benchmarking
	"198.51.100.0/24", // TEST-NET-2
	"203.0.113.0/24",  // TEST-NET-3
	"240.0.0.0/4",     // reserved (includes 255.255.255.255 broadcast)
	"::/128",          // unspecified v6
	"::1/128",         // loopback v6
	"64:ff9b::/96",    // NAT64 well-known prefix (can encode blocked IPv4 targets)
	"64:ff9b:1::/48",  // NAT64 local-use
	"100::/64",        // discard-only
	"100:0:0:1::/64",  // dummy IPv6 prefix
	"2001::/32",       // Teredo transition prefix
	"2001:2::/48",     // benchmarking
	"2001:10::/28",    // deprecated ORCHID
	"2001:db8::/32",   // documentation
	"2002::/16",       // 6to4 transition prefix
	"3fff::/20",       // documentation
	"5f00::/16",       // SRv6 SIDs
	"fc00::/7",        // unique-local
	"fe80::/10",       // link-local v6
	"ff00::/8",        // multicast v6
)

func mustParseCIDRs(cidrs ...string) []*net.IPNet {
	out := make([]*net.IPNet, 0, len(cidrs))
	for _, c := range cidrs {
		_, n, err := net.ParseCIDR(c)
		if err != nil {
			panic("ssproxy: bad blocked CIDR " + c)
		}
		out = append(out, n)
	}
	return out
}

// blockedIP reports whether ip is NOT a publicly-routable destination — by
// category AND by the explicit special-purpose prefix table. Normalizes
// IPv4-mapped IPv6 so a ::ffff:127.0.0.1 can't slip past the v4 checks.
func blockedIP(ip net.IP) bool {
	if ip == nil {
		return true
	}
	if v4 := ip.To4(); v4 != nil {
		ip = v4
	}
	if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast() || ip.IsMulticast() || ip.IsUnspecified() ||
		ip.IsInterfaceLocalMulticast() {
		return true
	}
	for _, n := range blockedCIDRs {
		if n.Contains(ip) {
			return true
		}
	}
	return false
}

// destAllowed is the policy PRECHECK on the decrypted target. With allowPrivate
// it permits everything; otherwise it rejects a literal non-public IP, or a
// hostname ANY of whose resolved IPs is non-public. NOTE: this is only a fast
// fail — the authoritative defense against DNS rebinding is the dialer Control
// callback (dialTarget), which validates the ACTUAL IP being connected, closing
// the resolve-then-redial TOCTOU (round-7 F1).
// It takes a ctx so a Stop() can abandon the lookup: net.LookupIP has no deadline of any
// kind and is bounded only by resolv.conf (typically ~30s), which used to make Stop() wait
// that long while the agent held proxyRuntime.mu (external review F1). The ctx is always the
// server's OWN (see opContext), never a caller's.
func (s *Server) destAllowed(ctx context.Context, host string) bool {
	if s.allowPrivateNow() {
		return true
	}
	if ip := net.ParseIP(host); ip != nil {
		return !blockedIP(ip)
	}
	addrs, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil || len(addrs) == 0 {
		return false
	}
	for _, a := range addrs {
		if blockedIP(a.IP) {
			return false
		}
	}
	return true
}

// dialTarget connects to target. Under the deny-private policy it installs a
// Dialer.Control that re-validates EVERY candidate IP the resolver returns at
// connect time, so the IP actually dialed is the IP validated — a rebinding
// resolver that returns a public answer to the precheck and a loopback answer to
// the dial is rejected here (round-7 F1).
// It takes a ctx for the same reason destAllowed does: Dialer.Timeout bounds a dial at 10s,
// but a Stop() must not have to wait out even that while proxyRuntime.mu is held.
func (s *Server) dialTarget(ctx context.Context, target string) (net.Conn, error) {
	d := net.Dialer{Timeout: dialTimeout}
	if !s.allowPrivateNow() {
		d.Control = func(_, address string, _ syscall.RawConn) error {
			h, _, err := net.SplitHostPort(address)
			if err != nil {
				return err
			}
			if blockedIP(net.ParseIP(h)) {
				return errors.New("ssproxy: destination policy denied")
			}
			return nil
		}
	}
	return d.DialContext(ctx, "tcp", target)
}

// Start binds 127.0.0.1:wantLocalPort (wantLocalPort == 0 => OS-chosen free
// port), installs the initial key set, and runs the accept loop. Returns the
// bound local port. Calling Start twice is a no-op that returns the
// already-bound port.
//
// Start takes NO context, and that is load-bearing rather than incidental.
// origin: 2026-08-21 weilandserver incident (docs/reviews/proxy-lifecycle-plan.md).
// This server used to be anchored to whatever ctx its caller held, which in the
// agent was the PER-SESSION runCtx (internal/agent/agent.go:920, rewritten every
// session). A control-plane session rebuild therefore killed the data plane, and
// the corpse was never rebuilt: the node advertised a READY exit with no listener
// behind it for 7h40m. Handing this object no ctx at all is what makes the set of
// things that can stop it a CLOSED, greppable list -- Stop() and nothing else --
// instead of "whoever happens to hold that ctx". The four legitimate callers of
// Stop are enumerated on stopProxyOnRunExit in internal/agent/proxy.go.
func (s *Server) Start(wantLocalPort int, keys []Key) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// ORDER MATTERS: the stopped check precedes the already-bound early return.
	//
	// origin: proxy-lifecycle plan A1. shutdown() closes s.ln but deliberately does NOT nil it
	// (Serving reads it, and a nil'd listener would make a stopped server indistinguishable from a
	// never-started one). With the early return first, calling Start on a STOPPED server returned
	// (s.localPort, nil) -- a SUCCESS handing back a dead listener. That was unreachable only
	// because proxyStartLocked always constructs a fresh ssproxy.New(); this increment adds rebuild
	// paths that reach it, so the order is now part of the contract, not an accident.
	if s.closed {
		return 0, fmt.Errorf("%w: cannot restart (construct a new Server)", ErrStopped)
	}
	if s.ln != nil {
		return s.localPort, nil
	}
	ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", wantLocalPort))
	if err != nil {
		return 0, fmt.Errorf("ssproxy: listen: %w", err)
	}
	// ORDER MATTERS: assert before publishing ln to the struct.
	//
	// origin: line-2 external review, severity lane's blind-spot #2. The checked type assertion added
	// earlier in this increment sat AFTER `s.ln = ln`, so on the error path the Server kept a pointer to
	// a listener it had just Closed while localPort stayed 0 -- and the `if s.ln != nil` early-return at
	// the top of this function then made the SECOND call return (0, nil). A zero port with a nil error is
	// worse than the panic the assertion was added to prevent: a panic has a stack.
	tcpAddr, ok := ln.Addr().(*net.TCPAddr)
	if !ok {
		_ = ln.Close()
		return 0, fmt.Errorf("ssproxy: listener address is %T, not *net.TCPAddr", ln.Addr())
	}
	s.ln = ln
	s.localPort = tcpAddr.Port
	s.allConns = map[net.Conn]struct{}{}
	s.keyConns = map[string]map[net.Conn]struct{}{}
	entries, keyErr := prepareKeys(keys)
	if keyErr != nil {
		_ = ln.Close()
		s.ln = nil
		return 0, keyErr
	}
	if _, keyErr = applyKeysLocked(s, entries); keyErr != nil {
		_ = ln.Close()
		s.ln = nil
		return 0, keyErr
	}
	s.imageState.authority = AuthorityPhaseAuthorized
	s.imageState.acceptRunning = true
	// ctx-root: this server's own lifetime root. Deliberately NOT derived from any caller
	// context — see the stopCtx field comment. shutdown() cancels it, which is what makes an
	// in-flight resolve/dial abandonable and Stop() genuinely bounded.
	s.stopCtx, s.stopCancel = context.WithCancel(context.Background())
	s.wg.Add(1)
	go s.acceptLoop(ln)
	return s.localPort, nil
}

// Serving reports whether this server is actually able to serve traffic right
// now: bound, not stopped, and its accept loop still running.
//
// origin: 2026-08-21 weilandserver incident. The agent used `p.srv != nil` as its
// "am I serving?" predicate, which a STOPPED server satisfies -- that is precisely
// how a dead exit was advertised as READY for 7h40m. A pointer answers "do I have
// one", never "does it work"; this method answers the second question and is the
// only thing callers may use to decide readiness.
//
// SetKeys reports ErrStopped for the `closed` case ONLY. It does NOT report the
// acceptExited case — an accept loop that died on a non-temporary listener error
// leaves closed==false, so SetKeys succeeds while nothing is accepting. That is
// precisely the corpse cause SetKeys cannot see, so a caller deciding readiness
// must consult THIS method and not infer liveness from SetKeys returning nil.
// (Internal review caught the earlier wording here claiming the two conditions
// coincide; they do not, and the difference is the one corpse cause left.)
func (s *Server) Serving() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ln != nil && !s.closed && !s.acceptExited
}

// closeListenerForTest closes ONLY the listener, leaving `closed` false — reproducing an accept
// loop that died on a non-temporary error while the server still believes it is open. That state
// is invisible to SetKeys (which only consults `closed`) and is exactly what the acceptExited
// latch exists to catch, so it needs to be constructible from a test.
//
// UNEXPORTED on purpose. origin: proxy-lifecycle external review F6 — the first cut exported it
// as CloseListenerForTest, which put a `ForTest` method on the production API surface and let any
// caller manufacture the anomalous closed==false && acceptExited==true state. The test that needs
// it lives in this package, so package scope is sufficient and the surface stays honest.
func (s *Server) closeListenerForTest() {
	s.mu.Lock()
	ln := s.ln
	s.mu.Unlock()
	if ln != nil {
		_ = ln.Close()
	}
}

// LocalPort returns the bound port, or 0 if not started.
func (s *Server) LocalPort() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.localPort
}

// Stop closes the listener and every live connection, then waits for all
// goroutines to exit. Idempotent; safe to call when never started.
func (s *Server) Stop() {
	s.shutdown()
	s.wg.Wait()
}

// shutdown closes the listener + every live conn (accepted AND upstream), WITHOUT
// waiting on the WaitGroup. Stop is shutdown + wg.Wait; keeping the two separate
// means a caller that must not block can still tear the listeners down.
//
// The caller-owned ctx watcher was removed by proxy-lifecycle. The remaining
// operation context belongs to this server and only interrupts its own DNS/dial;
// transient control-plane cancellation still cannot tear down the listener.
func (s *Server) shutdown() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	st := ensureImageState(s)
	st.authority = AuthorityPhaseRevoked
	releaseImportedStateLocked(s)
	var actors []*ssActor
	for _, a := range st.actors {
		markRevokedLocked(a)
		actors = append(actors, a)
	}
	ln, cancel := s.ln, s.stopCancel
	listenerFile := st.listenerFile
	st.listenerFile = nil
	signalStateLocked(st)
	s.mu.Unlock()
	// No socket close, callback or join runs under the table guard. In particular
	// a flow-revoke callback may itself stop the server.
	if cancel != nil {
		cancel()
	}
	if ln != nil {
		_ = ln.Close()
	}
	cleanupActors(s, actors)
	if listenerFile != nil {
		_ = listenerFile.Close()
	}
}

func (s *Server) acceptLoop(ln net.Listener) {
	defer s.wg.Done()
	for {
		s.mu.Lock()
		st := s.imageState
		if s.closed || st.frozen {
			finishAcceptLocked(s)
			s.mu.Unlock()
			return
		}
		s.mu.Unlock()
		if tcp, ok := ln.(*net.TCPListener); ok {
			_ = tcp.SetDeadline(time.Now().Add(actorPollInterval))
		}
		c, err := ln.Accept()
		if err != nil {
			if timeoutError(err) {
				continue
			}
			s.mu.Lock()
			finishAcceptLocked(s)
			s.mu.Unlock()
			return
		}
		s.mu.Lock()
		a, err := acceptActorLocked(s, c)
		if err == nil && !st.frozen {
			startActorLocked(s, a)
		}
		s.mu.Unlock()
		if err != nil {
			_ = c.Close()
		}
	}
}

// Retire the accept worker before releasing the guard that observed Freeze.
// Otherwise a cut timeout can resume between that observation and deferred
// cleanup, miss the still-running worker, and leave no accept owner afterward.
// origin: hot-update S5 plan §5.3 / T22
func finishAcceptLocked(s *Server) {
	s.acceptExited = true
	s.imageState.acceptRunning = false
	signalStateLocked(s.imageState)
}

func (s *Server) snapshotKeys() []*keyEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.keys
}

// readTargetAddr parses the SOCKS-style target address (ATYP + addr + port)
// that prefixes the first decrypted payload of a Shadowsocks stream.
func readTargetAddr(r io.Reader) (string, error) {
	atyp := make([]byte, 1)
	if _, err := io.ReadFull(r, atyp); err != nil {
		return "", err
	}
	var host string
	switch atyp[0] {
	case 0x01: // IPv4
		b := make([]byte, net.IPv4len)
		if _, err := io.ReadFull(r, b); err != nil {
			return "", err
		}
		host = net.IP(b).String()
	case 0x04: // IPv6
		b := make([]byte, net.IPv6len)
		if _, err := io.ReadFull(r, b); err != nil {
			return "", err
		}
		host = net.IP(b).String()
	case 0x03: // domain
		l := make([]byte, 1)
		if _, err := io.ReadFull(r, l); err != nil {
			return "", err
		}
		b := make([]byte, int(l[0]))
		if _, err := io.ReadFull(r, b); err != nil {
			return "", err
		}
		host = string(b)
	default:
		return "", errBadAddr
	}
	pb := make([]byte, 2)
	if _, err := io.ReadFull(r, pb); err != nil {
		return "", err
	}
	return net.JoinHostPort(host, strconv.Itoa(int(binary.BigEndian.Uint16(pb)))), nil
}

// Legacy retains the existing precheck plus dial-time policy check. Its resolver
// and dial are cancellable but not exportable; Freeze rejects this operation
// until the connected socket and the chosen address have been committed.
func connectLegacyActorLocked(s *Server, a *ssActor, target string) error {
	p := a.state
	now, err := actorNow()
	if err != nil {
		return err
	}
	if p.number("connect_deadline_boot_ns") == 0 {
		p.put("connect_deadline_boot_ns", uint64(now+int64(dialTimeout)))
	}
	left := time.Duration(int64(p.number("connect_deadline_boot_ns")) - now)
	if left <= 0 {
		return errors.New("ssproxy: connect deadline")
	}
	ctx, cancel := context.WithTimeout(s.stopCtx, left)
	a.dnsInFlight = true
	s.mu.Unlock()
	host, _, err := net.SplitHostPort(target)
	var remote net.Conn
	if err == nil && !s.destAllowed(ctx, host) {
		// The decrypted target and KeyID are private state, including on denial.
		// origin: hot-update S5 plan §6.6 / T26
		s.logger.Warn("ssproxy: blocked destination", "reason", "destination_policy_denied")
		err = errors.New("ssproxy: destination policy denied")
	}
	if err == nil {
		remote, err = s.dialTarget(ctx, target)
	}
	cancel()
	s.mu.Lock()
	a.dnsInFlight = false
	if err != nil {
		return errors.New("ssproxy: destination unavailable")
	}
	// origin: hot-update S5 plan §6.1 / T17
	upstream, ok := remote.(*net.TCPConn)
	if !ok {
		_ = remote.Close()
		return ErrStateUnrepresentable
	}
	addr, ok := remote.RemoteAddr().(*net.TCPAddr)
	if !ok {
		_ = remote.Close()
		return ErrStateUnrepresentable
	}
	a.upstream = upstream
	a.upstreamConn = remote
	if p.flag("revoked") || s.closed {
		shutdownSocket(a.upstream)
		return ErrStopped
	}
	ip := addr.AddrPort().Addr().As16()
	p.putBytes("resolved_ip_candidates", ip[:])
	p.put("dns_decided", 1)
	p.put("policy_approved", 1)
	p.put("connect_state", uint64(rawsocket.ConnectConnected))
	p.put("connect_so_error_consumed", 1)
	s.allConns[remote] = struct{}{}
	id := string(p.bytes("authorized_key_id"))
	if s.keyConns[id] == nil {
		s.keyConns[id] = make(map[net.Conn]struct{})
	}
	s.keyConns[id][remote] = struct{}{}
	p.put("phase", uint64(SSPhaseRelay))
	return nil
}

func (s *Server) ExpectFlow(id [16]byte, tuple rawsocket.Tuple) error {
	if id == ([16]byte{}) {
		return ErrStateUnrepresentable
	}
	return expectSocket(s, id, tuple)
}

// ExpectLegacyFlow reserves the actual pre-SYN tuple without inventing a v1
// FlowID. Legacy carrier ownership still blocks every image cut; key removal
// closes this actor's original socket and lets the legacy bridge terminate.
// origin: hot-update S5 plan §4.1 / §6.5 / §9.1 / DRILL-RUN
func ExpectLegacyFlow(s *Server, tuple rawsocket.Tuple) error {
	return expectSocket(s, [16]byte{}, tuple)
}

func expectSocket(s *Server, id [16]byte, tuple rawsocket.Tuple) error {
	if runtime.GOOS != "linux" {
		return rawsocket.ErrUnsupported
	}
	if !tuple.Local.IsValid() || !tuple.Remote.IsValid() || !tuple.Local.Addr().IsLoopback() || !tuple.Remote.Addr().IsLoopback() {
		return ErrStateUnrepresentable
	}
	now, err := actorNow()
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	st := ensureImageState(s)
	if s.closed {
		return ErrStopped
	}
	if st.frozen {
		return ErrAuthorityPending
	}
	if st.revoke == nil {
		return ErrStateUnrepresentable
	}
	if tuple.Remote.Port() != uint16(s.localPort) {
		return ErrStateUnrepresentable
	}
	if _, ok := st.bindings[tuple]; ok {
		return ErrStateUnrepresentable
	}
	for _, b := range st.bindings {
		if id != ([16]byte{}) && b.id == id {
			return ErrStateUnrepresentable
		}
	}
	// Expired reservations cannot accumulate unbounded state after a failed dial.
	for k, b := range st.bindings {
		if !b.consumed && b.deadline <= now {
			delete(st.bindings, k)
		}
	}
	if len(st.bindings) >= MaxActors {
		return ErrCapacity
	}
	st.bindings[tuple] = &flowBinding{id: id, tuple: tuple, deadline: now + int64(handshakeTimeout)}
	return nil
}
func (s *Server) SetFlowRevoke(fn func([16]byte, string)) error {
	if fn == nil {
		return ErrStateUnrepresentable
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	st := ensureImageState(s)
	if s.closed {
		return ErrStopped
	}
	st.revoke = fn
	return nil
}

// The absolute authority deadline belongs to the actor, including while fresh
// authorization is unavailable. It is never renewed by Freeze/Thaw/import.
// origin: hot-update S5 plan §6.5
func actorAuthorityDeadline(a *ssActor) (time.Duration, bool) {
	deadline := a.state.number("authority_deadline_boot_ns")
	if deadline == 0 {
		return 0, false
	}
	now, err := actorNow()
	if err != nil || uint64(now) >= deadline {
		return 0, true
	}
	return time.Duration(deadline - uint64(now)), false
}
