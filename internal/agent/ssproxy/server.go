package ssproxy

import (
	"context"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"syscall"
	"time"

	"golang.org/x/crypto/chacha20poly1305"
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
	keyID  string
	master []byte
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
	logger *slog.Logger

	mu        sync.Mutex
	ln        net.Listener
	localPort int
	keys      []*keyEntry                      // copy-on-write snapshot
	allConns  map[net.Conn]struct{}            // every conn this server owns -- accepted AND upstream (closed by Stop; see handleConn)
	keyConns  map[string]map[net.Conn]struct{} // keyID -> live conns (force-close on revoke)
	saltSeen  map[string]*saltRing             // keyID -> bounded seen-salt filter (F11 replay)
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

// maxSaltsPerKey bounds the per-key replay filter (round-6 F11). Sized for a
// generous recent-connection window; a replay older than this for the same key
// slips through (a bounded mitigation of the SS-AEAD salt-uniqueness rule).
const maxSaltsPerKey = 8192

// saltRing is a bounded FIFO set of recently-seen client salts for one key.
type saltRing struct {
	set   map[string]struct{}
	order []string
}

// observe records salt and returns true if it is NEW (not a replay).
func (r *saltRing) observe(salt []byte) bool {
	k := string(salt)
	if _, dup := r.set[k]; dup {
		return false
	}
	r.set[k] = struct{}{}
	r.order = append(r.order, k)
	if len(r.order) > maxSaltsPerKey {
		old := r.order[0]
		r.order = r.order[1:]
		delete(r.set, old)
	}
	return true
}

// recordSalt returns false if (keyID, salt) was already seen — i.e. a replay.
func (s *Server) recordSalt(keyID string, salt []byte) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.saltSeen == nil {
		s.saltSeen = map[string]*saltRing{}
	}
	r := s.saltSeen[keyID]
	if r == nil {
		r = &saltRing{set: map[string]struct{}{}}
		s.saltSeen[keyID] = r
	}
	return r.observe(salt)
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
				return fmt.Errorf("ssproxy: destination %s not publicly routable", address)
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
	s.setKeysLocked(keys)
	// ctx-root: this server's own lifetime root. Deliberately NOT derived from any caller
	// context — see the stopCtx field comment. shutdown() cancels it, which is what makes an
	// in-flight resolve/dial abandonable and Stop() genuinely bounded.
	s.stopCtx, s.stopCancel = context.WithCancel(context.Background())
	s.wg.Add(1)
	go s.acceptLoop(ln)
	return s.localPort, nil
}

// opContext returns the server's own cancellation context for one in-flight operation.
// Before Start (or in a Server built by a test that never started) it degrades to a
// background context, so callers never have to nil-check.
func (s *Server) opContext() context.Context {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopCtx == nil {
		// ctx-root: unstarted server; there is nothing to cancel yet.
		return context.Background()
	}
	return s.stopCtx
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

// SetKeys atomically replaces the active key set. Connections whose KeyID is
// no longer present are force-closed (hard revoke). Idempotent.
func (s *Server) SetKeys(keys []Key) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrStopped
	}
	s.setKeysLocked(keys)
	return nil
}

func (s *Server) setKeysLocked(keys []Key) {
	newKeys := make([]*keyEntry, 0, len(keys))
	present := make(map[string]bool, len(keys))
	for _, k := range keys {
		newKeys = append(newKeys, &keyEntry{keyID: k.KeyID, master: evpBytesToKey(k.Secret, keySize)})
		present[k.KeyID] = true
	}
	s.keys = newKeys
	if s.keyConns != nil {
		for keyID, set := range s.keyConns {
			if present[keyID] {
				continue
			}
			for c := range set {
				_ = c.Close()
			}
			delete(s.keyConns, keyID)
		}
	}
	// round-6 F11: drop the per-key replay state for any revoked key so it can't
	// grow unbounded and so a re-created key starts fresh.
	for keyID := range s.saltSeen {
		if !present[keyID] {
			delete(s.saltSeen, keyID)
		}
	}
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
// The previous wording described cancelling a ctx and being callable "from the
// ctx-watch goroutine". Both are gone: proxy-lifecycle removed this type's context
// entirely, and with it that goroutine. A comment describing deleted machinery is
// worse than no comment — it sends the next reader looking for a cancellation path
// that does not exist.
func (s *Server) shutdown() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	// Cancel BEFORE closing sockets: a handler blocked in LookupIPAddr or DialContext is
	// unreachable by closing the accepted conn, and this is the only thing that frees it.
	// Without it Stop() waits out the system resolver while the agent holds proxyRuntime.mu
	// (external review F1).
	if s.stopCancel != nil {
		s.stopCancel()
	}
	if s.ln != nil {
		_ = s.ln.Close()
	}
	for c := range s.allConns {
		_ = c.Close()
	}
	s.mu.Unlock()
}

func (s *Server) acceptLoop(ln net.Listener) {
	defer s.wg.Done()
	// Latch that the loop is gone so Serving stops claiming this server can take
	// traffic. Accept can return a non-temporary error (fd exhaustion, the listener
	// closed under us) while `closed` is still false, and without this latch the
	// server would keep reporting itself healthy with nobody listening -- the exact
	// shape of the incident this increment exists to close.
	defer func() {
		s.mu.Lock()
		s.acceptExited = true
		s.mu.Unlock()
	}()
	for {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		if !s.trackConn(c) {
			_ = c.Close()
			continue
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer s.untrackConn(c)
			defer func() { _ = c.Close() }()
			s.handleConn(c)
		}()
	}
}

func (s *Server) trackConn(c net.Conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	s.allConns[c] = struct{}{}
	return true
}

func (s *Server) untrackConn(c net.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.allConns, c)
	for id, set := range s.keyConns {
		if _, ok := set[c]; ok {
			delete(set, c)
			if len(set) == 0 {
				delete(s.keyConns, id)
			}
		}
	}
}

// bindKeyConn registers c under keyID iff that key is still active. Returns
// false if the key was revoked between trial decryption and registration.
func (s *Server) bindKeyConn(keyID string, c net.Conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	active := false
	for _, k := range s.keys {
		if k.keyID == keyID {
			active = true
			break
		}
	}
	if !active {
		return false
	}
	set := s.keyConns[keyID]
	if set == nil {
		set = map[net.Conn]struct{}{}
		s.keyConns[keyID] = set
	}
	set[c] = struct{}{}
	return true
}

// unbindKeyConn removes one conn from its key's live set. Without it a long-lived server
// would accumulate an entry per completed relay in keyConns (the same class of leak the
// allConns untrack closes), and a revoke would walk conns that are already gone.
func (s *Server) unbindKeyConn(keyID string, c net.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	set := s.keyConns[keyID]
	if set == nil {
		return
	}
	delete(set, c)
	if len(set) == 0 {
		delete(s.keyConns, keyID)
	}
}

func (s *Server) snapshotKeys() []*keyEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.keys
}

func (s *Server) handleConn(c net.Conn) {
	_ = c.SetReadDeadline(time.Now().Add(handshakeTimeout))

	salt := make([]byte, saltSize)
	if _, err := io.ReadFull(c, salt); err != nil {
		return
	}
	firstLen := make([]byte, 2+tagSize)
	if _, err := io.ReadFull(c, firstLen); err != nil {
		return
	}

	// Trial-decrypt the first length chunk against each active key. The PSK
	// whose derived subkey authenticates the chunk identifies the subscriber.
	var (
		chosenID     string
		chosenMaster []byte
		rdAEAD       cipher.AEAD
		rdNonce      []byte
		firstN       int
	)
	for _, k := range s.snapshotKeys() {
		sk, err := sessionSubkey(k.master, salt)
		if err != nil {
			continue
		}
		aead, err := chacha20poly1305.New(sk)
		if err != nil {
			continue
		}
		nonce := make([]byte, aead.NonceSize())
		trial := append([]byte(nil), firstLen...)
		plain, err := aead.Open(trial[:0], nonce, trial, nil)
		if err != nil {
			continue
		}
		n := int(binary.BigEndian.Uint16(plain))
		if n == 0 || n > maxChunk {
			continue
		}
		chosenID = k.keyID
		chosenMaster = k.master
		rdAEAD = aead
		incNonce(nonce)
		rdNonce = nonce
		firstN = n
		break
	}
	if rdAEAD == nil {
		return // no key matched: fail-closed (empty keyset rejects all)
	}
	// round-6 F11: reject a replayed client salt for this key BEFORE doing any
	// further work — the SS-AEAD salt must be unique per key, and a replayed
	// handshake would otherwise re-open the target connection.
	if !s.recordSalt(chosenID, salt) {
		s.logger.Debug("ssproxy: rejecting replayed client salt", "key", chosenID)
		return
	}
	if !s.bindKeyConn(chosenID, c) {
		return // revoked mid-handshake
	}

	// Read the first payload chunk (already have its length).
	payBuf := make([]byte, firstN+tagSize)
	if _, err := io.ReadFull(c, payBuf); err != nil {
		return
	}
	firstPlain, err := rdAEAD.Open(payBuf[:0], rdNonce, payBuf, nil)
	if err != nil {
		return
	}
	incNonce(rdNonce)

	reader := &aeadReader{r: c, aead: rdAEAD, nonce: rdNonce, buf: firstPlain, off: 0}
	target, err := readTargetAddr(reader)
	if err != nil {
		return
	}
	_ = c.SetReadDeadline(time.Time{}) // clear handshake deadline for relay

	// round-6 F12 / round-7 F1,F2: destination policy. Fast-fail precheck, then
	// dialTarget validates the ACTUAL connected IP (closes the rebinding TOCTOU).
	// One server-owned context covers BOTH pre-track blocking spans (resolve + dial), so a
	// concurrent Stop() abandons them instead of waiting them out (external review F1).
	opCtx := s.opContext()
	host, _, splitErr := net.SplitHostPort(target)
	if splitErr != nil || !s.destAllowed(opCtx, host) {
		s.logger.Warn("ssproxy: blocked non-public destination", "target", target)
		return
	}

	remote, err := s.dialTarget(opCtx, target)
	if err != nil {
		s.logger.Debug("ssproxy: dial target failed", "target", target, "err", err)
		return
	}
	defer func() { _ = remote.Close() }()
	// Track the UPSTREAM conn too, so shutdown() can close it.
	//
	// origin: proxy-lifecycle plan A2 (bounded teardown; same family as gotcha #72).
	// allConns used to hold only ACCEPTED conns, so shutdown closed the client side and
	// left this one open -- and relay's wg.Wait() needs BOTH directions to finish, while
	// io.Copy(w, remote) sits blocked in remote.Read(). An upstream that does not answer
	// the half-close therefore hung Stop() indefinitely, and Stop() is called while
	// holding the agent's p.mu (internal/agent/proxy.go proxyTeardownLocked) -- one idle
	// upstream could wedge the whole proxy runtime. This increment makes rebuild paths
	// far more reachable, so an unbounded Stop is no longer a latent risk.
	//
	// If the server is ALREADY shutting down, trackConn returns false: close immediately
	// rather than starting a relay nobody will tear down.
	if !s.trackConn(remote) {
		return
	}
	defer s.untrackConn(remote)

	// Bind the UPSTREAM to the SAME key as the accepted conn, so a hard revoke closes BOTH
	// halves atomically.
	//
	// origin: proxy-lifecycle external review F2. keyConns held only the subscriber side, so
	// SetKeys' revoke closed the accepted socket and left the upstream blocked in
	// relay's remote->client copy. The DATA-plane guarantee still held (the subscriber's
	// socket was gone), but the RESOURCE contract did not: handler goroutine, both map
	// entries and both fds survived until the whole server stopped. An authorised key could
	// pre-open many silent upstreams and have them outlive its own revocation.
	//
	// bindKeyConn re-checks that the key is still active, so a key revoked DURING the dial
	// never reaches a relay: the bind fails and the deferred Close reclaims the upstream
	// immediately.
	//
	// SCOPE, stated precisely because the earlier wording over-claimed (external rereview R2):
	// this does NOT prevent the outbound connect itself. The dial's context is the SERVER's
	// (cancelled by Stop), not the key's, so a revoke arriving while the resolver or the TCP
	// handshake is in flight can still let that one connect COMPLETE before the bind rejects
	// it. What is guaranteed is narrower and is the part that matters for the revoke contract:
	// no relay is started for a dead credential, and the socket is reclaimed at once rather
	// than surviving until the whole server stops. Making revocation cancel in-flight dials
	// would need a per-key/per-session cancellable context; that is a deliberate non-goal here
	// (it would put a caller-shaped lifetime back into this type's hot path), and if the hard
	// revoke contract is ever tightened to "no outbound side effects after revoke" it should be
	// its own increment.
	if !s.bindKeyConn(chosenID, remote) {
		return
	}
	defer s.unbindKeyConn(chosenID, remote)

	// Response direction: fresh salt + subkey, written before any payload.
	wsalt := make([]byte, saltSize)
	if _, err := rand.Read(wsalt); err != nil {
		return
	}
	if _, err := c.Write(wsalt); err != nil {
		return
	}
	wsk, err := sessionSubkey(chosenMaster, wsalt)
	if err != nil {
		return
	}
	wAEAD, err := chacha20poly1305.New(wsk)
	if err != nil {
		return
	}
	writer := newAEADWriter(c, wAEAD)

	s.relay(c, reader, writer, remote)
}

func (s *Server) relay(client net.Conn, r io.Reader, w io.Writer, remote net.Conn) {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, _ = io.Copy(remote, r) // client -> remote
		halfCloseWrite(remote)    // propagate the client's half-close, don't truncate the response
	}()
	go func() {
		defer wg.Done()
		_, _ = io.Copy(w, remote) // remote -> client
		halfCloseWrite(client)
	}()
	wg.Wait()
}

// halfCloseWrite shuts down only the write half of a TCP conn (so a pending
// response in the other direction is not truncated). Falls back to a full
// close for non-TCP conns. The final full close is the caller's deferred Close.
func halfCloseWrite(c net.Conn) {
	if tc, ok := c.(*net.TCPConn); ok {
		_ = tc.CloseWrite()
		return
	}
	_ = c.Close()
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
