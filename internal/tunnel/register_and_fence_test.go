package tunnel

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
)

// origin: d6_test.go (renamed in B6) — docs/reviews/d6-external-review.md, docs/reviews/d6-review.md
//
// TestD6RegisterLineParse (R-11, §7.2(b)): the v2 REGISTER grammar is EXACTLY 6
// fields with an int64 epoch; 5/7-field, non-int, negative, and overflow epochs
// are rejected (mapped to malformed_register by the caller).
func TestD6RegisterLineParse(t *testing.T) {
	tests := []struct {
		name      string
		line      string
		wantOK    bool
		wantPort  int
		wantEpoch int64
	}{
		{"valid epoch 0", "REGISTER lab lab-1 14000 tok 0\n", true, 14000, 0},
		{"valid epoch 5", "REGISTER lab lab-1 14000 tok 5\n", true, 14000, 5},
		{"valid CRLF", "REGISTER lab lab-1 14000 tok 7\r\n", true, 14000, 7},
		{"five fields (pre-D6)", "REGISTER lab lab-1 14000 tok\n", false, 0, 0},
		{"seven fields", "REGISTER lab lab-1 14000 tok 5 extra\n", false, 0, 0},
		{"non-int epoch", "REGISTER lab lab-1 14000 tok abc\n", false, 0, 0},
		{"negative epoch", "REGISTER lab lab-1 14000 tok -1\n", false, 0, 0},
		{"overflow epoch", "REGISTER lab lab-1 14000 tok 99999999999999999999\n", false, 0, 0},
		{"bad port", "REGISTER lab lab-1 notaport tok 0\n", false, 0, 0},
		{"wrong verb", "HELLO lab lab-1 14000 tok 0\n", false, 0, 0},
		{"empty", "\n", false, 0, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, port, _, epoch, ok := parseRegisterLine(tt.line)
			if ok != tt.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tt.wantOK)
			}
			if ok && (port != tt.wantPort || epoch != tt.wantEpoch) {
				t.Fatalf("port/epoch = %d/%d, want %d/%d", port, epoch, tt.wantPort, tt.wantEpoch)
			}
		})
	}
}

// TestD6DenyTransientClassification (R-16): home_catching_up is TRANSIENT (the
// shared proto const), the existing transient reasons are unchanged, and every
// other / unknown reason stays TERMINAL (the safe default that prevents a brick).
func TestD6DenyTransientClassification(t *testing.T) {
	transient := []string{proto.ReasonHomeCatchingUp, "try_again", "public_port_bind_failed"}
	terminal := []string{"token_unknown_or_revoked", "session_closed", "malformed_register", "", "some_new_reason"}
	for _, r := range transient {
		if !denyIsTransient(r) {
			t.Errorf("denyIsTransient(%q) = false, want true", r)
		}
	}
	for _, r := range terminal {
		if denyIsTransient(r) {
			t.Errorf("denyIsTransient(%q) = true, want false (terminal default)", r)
		}
	}
	// Pin the SSOT: the emit-side const equals the literal the classifier matches.
	if proto.ReasonHomeCatchingUp != "home_catching_up" {
		t.Fatalf("proto.ReasonHomeCatchingUp drifted: %q", proto.ReasonHomeCatchingUp)
	}
}

func TestD9CloseProxyIfFencesPortReuse(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	port := ln.Addr().(*net.TCPAddr).Port
	srv := &Server{
		sessions:             map[int]*serverSession{},
		killGen:              map[int]int64{},
		killGenAllocation:    map[sessionFenceKey]int64{},
		inflightByAllocation: map[sessionFenceKey]int{},
		closedAllocation:     map[sessionFenceKey]bool{},
	}
	oldKey := sessionFenceKeyFor(port, "lab", "lab-1", "old-token-hash")
	newKey := sessionFenceKeyFor(port, "lab", "lab-1", "new-token-hash")
	srv.inflightByAllocation[oldKey] = 1
	srv.sessions[port] = &serverSession{
		sid:        "lab",
		nid:        "lab-1",
		publicPort: port,
		tokenHash:  "new-token-hash",
		epoch:      7,
		listener:   ln,
		cancel:     func() {},
	}

	infos := srv.SessionInfos()
	if len(infos) != 1 || infos[0].TokenHash != "new-token-hash" || infos[0].Epoch != 7 {
		t.Fatalf("SessionInfos = %#v, want installed identity", infos)
	}
	if srv.CloseProxyIf(SessionInfo{PublicPort: port, SID: "lab", NID: "lab-1", TokenHash: "old-token-hash", Epoch: 6}) {
		t.Fatal("stale close for a reused port/token must not close the installed new session")
	}
	if _, ok := srv.sessions[port]; !ok {
		t.Fatal("mismatched CloseProxyIf removed the current session")
	}
	if srv.killGen[port] != 0 {
		t.Fatalf("mismatched installed session bumped killGen = %d, want 0", srv.killGen[port])
	}
	if srv.killGenAllocation[oldKey] != 1 {
		t.Fatalf("stale close did not fence old in-flight allocation; got %d", srv.killGenAllocation[oldKey])
	}
	if srv.killGenAllocation[newKey] != 0 {
		t.Fatalf("stale close fenced new allocation token; got %d", srv.killGenAllocation[newKey])
	}
	if !srv.CloseProxyIf(SessionInfo{PublicPort: port, SID: "lab", NID: "lab-1", TokenHash: "new-token-hash", Epoch: 7}) {
		t.Fatal("matching CloseProxyIf did not close the installed session")
	}
	if _, ok := srv.sessions[port]; ok {
		t.Fatal("matching CloseProxyIf left the session installed")
	}
	if srv.killGen[port] != 0 {
		t.Fatalf("matching close bumped bare-port killGen = %d, want 0", srv.killGen[port])
	}
	if srv.killGenAllocation[newKey] != 0 {
		t.Fatalf("matching close leaked allocation fence without in-flight register; got %d", srv.killGenAllocation[newKey])
	}
	if srv.CloseProxyIf(SessionInfo{PublicPort: port, SID: "lab", NID: "lab-1", TokenHash: "old-token-hash", Epoch: 6}) {
		t.Fatal("no-session stale CloseProxyIf must return false")
	}
	if srv.killGen[port] != 0 {
		t.Fatalf("no-session close bumped bare-port killGen = %d, want 0", srv.killGen[port])
	}
	if srv.killGenAllocation[oldKey] != 2 {
		t.Fatalf("no-session stale close did not fence old in-flight allocation; got %d", srv.killGenAllocation[oldKey])
	}
	if srv.CloseProxyIf(SessionInfo{PublicPort: port, SID: "lab", NID: "lab-1", TokenHash: "new-token-hash", Epoch: 7}) {
		t.Fatal("second CloseProxyIf with no installed session must return false")
	}
	if srv.killGenAllocation[newKey] != 0 {
		t.Fatalf("no-session close leaked fence for non-in-flight new allocation; got %d", srv.killGenAllocation[newKey])
	}
}

// TestD6CertFingerprintSSOT (R-20): the fingerprint is "sha256:"+hex over the
// LEAF DER, stable across calls, distinct for distinct certs, and the
// raw-DER helper agrees with the *x509.Certificate one.
func TestD6CertFingerprintSSOT(t *testing.T) {
	a := mustLeaf(t)
	b := mustLeaf(t)
	fpA := CertFingerprint(a)
	if !strings.HasPrefix(fpA, "sha256:") || len(fpA) != len("sha256:")+64 {
		t.Fatalf("bad fp shape: %q", fpA)
	}
	if fpA != CertFingerprint(a) {
		t.Fatal("fingerprint not stable across calls")
	}
	if fpA == CertFingerprint(b) {
		t.Fatal("two distinct certs produced the same fingerprint")
	}
	if fpA != certFingerprintDER(a.Raw) {
		t.Fatal("certFingerprintDER disagrees with CertFingerprint")
	}
}

// TestD6CertPinVerify (R-21, adversarial): empty pins → InsecureSkipVerify with
// NO callback (the N=1 fallback); non-empty pins → a VerifyConnection that
// accepts Current, accepts an unexpired Previous, and fail-closed-rejects an
// expired/nil-window Previous, a non-pinned cert, and an empty peer chain.
func TestD6CertPinVerify(t *testing.T) {
	home := mustLeaf(t)
	rogue := mustLeaf(t)
	fpHome := CertFingerprint(home)
	fpRogue := CertFingerprint(rogue)

	// Empty pins = legacy InsecureSkipVerify, no callback.
	cfg := clientTLSConfigPinned(proto.CertPins{})
	if !cfg.InsecureSkipVerify || cfg.VerifyConnection != nil {
		t.Fatal("empty pins must be InsecureSkipVerify with no callback (N=1 fallback)")
	}

	state := func(leaf *x509.Certificate) tls.ConnectionState {
		return tls.ConnectionState{PeerCertificates: []*x509.Certificate{leaf}}
	}
	future := time.Now().Add(time.Hour)
	past := time.Now().Add(-time.Hour)

	cases := []struct {
		name    string
		pins    proto.CertPins
		present *x509.Certificate
		wantErr bool
	}{
		{"current matches", proto.CertPins{Current: fpHome}, home, false},
		{"current mismatch (rogue)", proto.CertPins{Current: fpHome}, rogue, true},
		{"previous within window", proto.CertPins{Current: fpRogue, Previous: fpHome, ValidUntil: &future}, home, false},
		{"previous after window", proto.CertPins{Current: fpRogue, Previous: fpHome, ValidUntil: &past}, home, true},
		{"previous nil window (fail-closed)", proto.CertPins{Current: fpRogue, Previous: fpHome}, home, true},
		{"prefix of current rejected", proto.CertPins{Current: fpHome[:len(fpHome)-2]}, home, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			vc := clientTLSConfigPinned(c.pins).VerifyConnection
			if vc == nil {
				t.Fatal("expected a VerifyConnection callback for non-empty pins")
			}
			err := vc(state(c.present))
			if (err != nil) != c.wantErr {
				t.Fatalf("VerifyConnection err = %v, wantErr = %v", err, c.wantErr)
			}
		})
	}

	// Empty peer chain → reject (fail-closed).
	vc := clientTLSConfigPinned(proto.CertPins{Current: fpHome}).VerifyConnection
	if vc(tls.ConnectionState{}) == nil {
		t.Fatal("empty PeerCertificates must be rejected")
	}
}

// TestD6PinnedConfigChoice (review A5 M1): the cert-pin TLS config uses
// VerifyConnection (resumption-safe), NOT VerifyPeerCertificate (which Go skips
// on a resumed handshake), and sets NO ClientSessionCache (so resumption never
// happens today — but if someone adds a cache, TestD6VerifyConnectionRunsOnResume
// must also be kept, this static pin forces that pairing).
func TestD6PinnedConfigChoice(t *testing.T) {
	cfg := clientTLSConfigPinned(proto.CertPins{Current: "sha256:x"})
	if cfg.VerifyConnection == nil {
		t.Error("pinned config must use VerifyConnection (resumption-safe)")
	}
	if cfg.VerifyPeerCertificate != nil {
		t.Error("pinned config must NOT use VerifyPeerCertificate (skipped on TLS-1.3 resumption)")
	}
	if cfg.ClientSessionCache != nil {
		t.Error("pinned config must NOT set a ClientSessionCache without a resumption pin-verify test")
	}
}

// TestD6VerifyConnectionRunsOnResume (review A5 M1): proves the load-bearing
// property — VerifyConnection runs on a RESUMED TLS 1.3 handshake (the reason the
// pin check is there and not in VerifyPeerCertificate). A counting wrapper over
// the production callback observes it fire on BOTH the full AND the resumed dial.
func TestD6VerifyConnectionRunsOnResume(t *testing.T) {
	srvCert, err := generateSelfSignedCert()
	if err != nil {
		t.Fatal(err)
	}
	leaf, _ := x509.ParseCertificate(srvCert.Certificate[0])
	pins := proto.CertPins{Current: CertFingerprint(leaf)}

	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{srvCert}, MinVersion: tls.VersionTLS13,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				_ = c.(*tls.Conn).Handshake()
				// Read a byte so the session ticket is flushed before close.
				_ = c.SetReadDeadline(time.Now().Add(time.Second))
				buf := make([]byte, 1)
				_, _ = c.Read(buf)
				_ = c.Close()
			}(c)
		}
	}()

	var (
		mu        sync.Mutex
		vcCalls   int
		sawResume bool
	)
	base := clientTLSConfigPinned(pins)
	clientCfg := &tls.Config{
		MinVersion:         tls.VersionTLS13,
		InsecureSkipVerify: true, //nolint:gosec // pin check is in VerifyConnection
		ClientSessionCache: tls.NewLRUClientSessionCache(4),
		VerifyConnection: func(cs tls.ConnectionState) error {
			mu.Lock()
			vcCalls++
			if cs.DidResume {
				sawResume = true
			}
			mu.Unlock()
			return base.VerifyConnection(cs) // the PRODUCTION pin check
		},
	}

	dial := func() {
		c, err := tls.Dial("tcp", ln.Addr().String(), clientCfg)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		_, _ = c.Write([]byte("x"))
		_ = c.Close()
	}
	dial() // full handshake (issues a session ticket)
	time.Sleep(50 * time.Millisecond)
	dial() // should RESUME

	mu.Lock()
	defer mu.Unlock()
	if vcCalls < 2 {
		t.Fatalf("VerifyConnection ran %d times, want it on both dials", vcCalls)
	}
	if !sawResume {
		t.Skip("TLS session did not resume in this run (no ticket); the static choice is still pinned by TestD6PinnedConfigChoice")
	}
}

// TestD6ReviewPurePinUpdateSameEpoch ensures a cert-rotation directive with the
// same home addr and same epoch refreshes the session's pin set in place. D6
// R-24 requires this pure-pin update to avoid tearing the transport; otherwise a
// later home restart with the rotated cert is rejected by the supervisor's stale
// spawn-time pins.
func TestD6ReviewPurePinUpdateSameEpoch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, sessCancel := context.WithCancel(ctx)
	defer sessCancel()

	c := NewClient("fallback:7000", "lab", "lab-1", nil, nil)
	c.ctx = ctx
	c.sessions[14000] = &clientSession{
		publicPort: 14000,
		localPort:  9000,
		token:      "tok",
		cancel:     sessCancel,
		brokerAddr: "10.0.0.2:7000",
		epoch:      3,
		certPins:   proto.CertPins{Current: "sha256:old"},
	}

	err := c.ApplyHome(14000, "10.0.0.2:7000", 3, proto.CertPins{Current: "sha256:new"})
	if err != nil {
		t.Fatalf("same-epoch pure-pin update returned error: %v", err)
	}
	c.mu.Lock()
	got := c.sessions[14000].certPins.Current
	c.mu.Unlock()
	if got != "sha256:new" {
		t.Fatalf("same-epoch pure-pin directive did not update session pins: got %q", got)
	}
}

func TestD6SameEpochDirectiveCannotClearClusterPins(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, sessCancel := context.WithCancel(ctx)
	defer sessCancel()

	c := NewClient("fallback:7000", "lab", "lab-1", nil, nil)
	c.ctx = ctx
	c.sessions[14000] = &clientSession{
		publicPort: 14000,
		localPort:  9000,
		token:      "tok",
		cancel:     sessCancel,
		brokerAddr: "10.0.0.2:7000",
		epoch:      3,
		certPins:   proto.CertPins{Current: "sha256:old"},
	}

	err := c.ApplyHome(14000, "10.0.0.2:7000", 3, proto.CertPins{})
	if !errors.Is(err, ErrHomePinsRequired) {
		t.Fatalf("same-epoch clustered directive without pins err=%v, want ErrHomePinsRequired", err)
	}
	c.mu.Lock()
	got := c.sessions[14000].certPins.Current
	c.mu.Unlock()
	if got != "sha256:old" {
		t.Fatalf("empty same-epoch directive cleared existing pin: got %q", got)
	}
}

func TestD6SameEpochEmptyFallbackDirectiveCannotClearClusterPins(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, sessCancel := context.WithCancel(ctx)
	defer sessCancel()

	c := NewClient("fallback:7000", "lab", "lab-1", nil, nil)
	c.ctx = ctx
	c.sessions[14000] = &clientSession{
		publicPort: 14000,
		localPort:  9000,
		token:      "tok",
		cancel:     sessCancel,
		brokerAddr: "10.0.0.2:7000",
		epoch:      3,
		certPins:   proto.CertPins{Current: "sha256:old"},
	}

	err := c.ApplyHome(14000, "", 3, proto.CertPins{})
	if !errors.Is(err, ErrHomePinsRequired) {
		t.Fatalf("same-epoch fallback directive without pins err=%v, want ErrHomePinsRequired", err)
	}
	c.mu.Lock()
	got := c.sessions[14000].certPins.Current
	c.mu.Unlock()
	if got != "sha256:old" {
		t.Fatalf("empty fallback directive cleared existing cluster pin: got %q", got)
	}
}

func mustLeaf(t *testing.T) *x509.Certificate {
	t.Helper()
	cert, err := generateSelfSignedCert()
	if err != nil {
		t.Fatalf("gen cert: %v", err)
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		t.Fatalf("parse leaf: %v", err)
	}
	return leaf
}
