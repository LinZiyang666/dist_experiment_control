package ssproxy

import (
	"context"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
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
	allConns  map[net.Conn]struct{}            // every accepted conn (closed by Stop)
	keyConns  map[string]map[net.Conn]struct{} // keyID -> live conns (force-close on revoke)
	saltSeen  map[string]*saltRing             // keyID -> bounded seen-salt filter (F11 replay)
	ctx       context.Context
	cancel    context.CancelFunc
	closed    bool

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
func (s *Server) destAllowed(host string) bool {
	if s.allowPrivateNow() {
		return true
	}
	if ip := net.ParseIP(host); ip != nil {
		return !blockedIP(ip)
	}
	ips, err := net.LookupIP(host)
	if err != nil || len(ips) == 0 {
		return false
	}
	for _, ip := range ips {
		if blockedIP(ip) {
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
func (s *Server) dialTarget(target string) (net.Conn, error) {
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
	return d.Dial("tcp", target)
}

// Start binds 127.0.0.1:wantLocalPort (wantLocalPort == 0 => OS-chosen free
// port), installs the initial key set, and runs the accept loop anchored to
// ctx. Returns the bound local port. Calling Start twice is a no-op that
// returns the already-bound port.
func (s *Server) Start(ctx context.Context, wantLocalPort int, keys []Key) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ln != nil {
		return s.localPort, nil
	}
	if s.closed {
		return 0, fmt.Errorf("ssproxy: server already stopped")
	}
	ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", wantLocalPort))
	if err != nil {
		return 0, fmt.Errorf("ssproxy: listen: %w", err)
	}
	s.ln = ln
	s.localPort = ln.Addr().(*net.TCPAddr).Port
	s.allConns = map[net.Conn]struct{}{}
	s.keyConns = map[string]map[net.Conn]struct{}{}
	s.setKeysLocked(keys)
	s.ctx, s.cancel = context.WithCancel(ctx)
	s.wg.Add(1)
	go s.acceptLoop(ln)
	// ctx-watch goroutine: tracked by wg (calls shutdown, NOT Stop, so it
	// never waits on the WaitGroup it belongs to).
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		<-s.ctx.Done()
		s.shutdown()
	}()
	return s.localPort, nil
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
		return fmt.Errorf("ssproxy: server stopped")
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

// shutdown closes the listener + every live conn and cancels ctx, WITHOUT
// waiting on the WaitGroup — so it is safe to call from a wg-tracked goroutine
// (the ctx-watch goroutine) as well as from Stop.
func (s *Server) shutdown() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	if s.cancel != nil {
		s.cancel()
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
	host, _, splitErr := net.SplitHostPort(target)
	if splitErr != nil || !s.destAllowed(host) {
		s.logger.Warn("ssproxy: blocked non-public destination", "target", target)
		return
	}

	remote, err := s.dialTarget(target)
	if err != nil {
		s.logger.Debug("ssproxy: dial target failed", "target", target, "err", err)
		return
	}
	defer func() { _ = remote.Close() }()

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
