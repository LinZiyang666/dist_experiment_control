package tunnel

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"
)

// register_admission_test.go — the UNAUTHENTICATED half of the control listener.
//
// Everything here exercises what a stranger can do BEFORE tokenLookup runs, on a
// socket the deployment guide tells operators to expose to the internet. The
// assertions are all about BOUNDS (bytes, map entries, goroutines), because the
// bound is what was missing — the behaviour (deny) was always correct.
//
// origin: prerelease audit broker-dataplane/BDP-F1..F3 + main-process MP-1.

// admissionServer is the harness listener plus everything a dialler needs to
// verify it. The fingerprint is carried explicitly because these tests dial a
// self-signed leaf: hostname verification cannot apply, but PEER verification
// still can, and the repo's TLS pairing gate is there to insist on exactly that
// distinction (test/architecture/tls_verify_pairing_test.go). Generating the
// cert here instead of letting Start mint an ephemeral one is what makes the
// pin knowable in the first place.
type admissionServer struct {
	srv  *Server
	addr string
	fp   string
}

// startAdmissionServer starts a control listener whose tokenLookup always denies.
func startAdmissionServer(t *testing.T) admissionServer {
	t.Helper()
	cert, err := generateSelfSignedCert()
	if err != nil {
		t.Fatalf("gen cert: %v", err)
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		t.Fatalf("parse leaf: %v", err)
	}
	srv := NewServerWithCert("127.0.0.1:0", "127.0.0.1",
		func(sid, nid string, port int, tokenHash string, epoch int64) error {
			return fmt.Errorf("token_unknown_or_revoked")
		},
		&cert,
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(srv.Close)
	srv.mu.Lock()
	addr := srv.ln.Addr().String()
	srv.mu.Unlock()
	return admissionServer{srv: srv, addr: addr, fp: CertFingerprint(leaf)}
}

// admissionTLS is the dial config for the harness listener: hostname
// verification off (the leaf is self-signed and its SANs are not the dial
// address), peer verification ON against the fingerprint the harness just
// minted. Same shape as the pinned production path in tls.go, and the same
// shape the pairing gate requires of every InsecureSkipVerify site that is not
// in its ledger.
func admissionTLS(fp string) *tls.Config {
	return &tls.Config{
		InsecureSkipVerify: true,
		MinVersion:         tls.VersionTLS12,
		VerifyConnection: func(cs tls.ConnectionState) error {
			if len(cs.PeerCertificates) == 0 {
				return errors.New("admission harness: peer presented no certificate")
			}
			if got := certFingerprintDER(cs.PeerCertificates[0].Raw); got != fp {
				return fmt.Errorf("admission harness: peer fingerprint %s, want %s", got, fp)
			}
			return nil
		},
	}
}

// sendRegister dials, writes one line verbatim, drains the reply and closes.
func sendRegister(t *testing.T, a admissionServer, line string) string {
	t.Helper()
	c, err := tls.Dial("tcp", a.addr, admissionTLS(a.fp))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = c.Close() }()
	_ = c.SetDeadline(time.Now().Add(10 * time.Second))
	if _, err := io.WriteString(c, line); err != nil {
		return ""
	}
	reply, _ := bufio.NewReader(c).ReadString('\n')
	return strings.TrimRight(reply, "\r\n")
}

// inflightSizes reports the two maps a pre-authorization handler touches.
func inflightSizes(s *Server) (bySID, byAlloc, killSess int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.inflightBySID), len(s.inflightByAllocation), len(s.killGenSession)
}

// waitInflightDrained waits for the handlers to return, then reports the sizes.
func waitInflightDrained(t *testing.T, s *Server) (int, int, int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		a, b, c := inflightSizes(s)
		if a == 0 && b == 0 && c == 0 {
			return a, b, c
		}
		time.Sleep(10 * time.Millisecond)
	}
	return inflightSizes(s)
}

// TestUnauthenticatedRegisterRetainsNoState is the BDP-F2 guard: a denied REGISTER
// must leave NOTHING behind. `m[k]--` writes 0 back without removing the key, and
// the pruners can only fire for a session ForgetSession has forgotten — never true
// for a session that never existed — so before the fix every denied probe leaked
// two permanent map entries.
func TestUnauthenticatedRegisterRetainsNoState(t *testing.T) {
	a := startAdmissionServer(t)
	srv := a.srv
	const n = 60
	for i := 0; i < n; i++ {
		// Well-formed and ValidateSID/ValidateNID-legal, so it reaches tokenLookup
		// and is denied there — the exact shape that used to retain state.
		reply := sendRegister(t, a, fmt.Sprintf("REGISTER lab-%02d gpu%02d 14000 tok%d 0\n", i%50, i%50, i))
		if !strings.HasPrefix(reply, "DENY") {
			t.Fatalf("attempt %d: expected DENY, got %q", i, reply)
		}
	}
	bySID, byAlloc, killSess := waitInflightDrained(t, srv)
	if bySID != 0 || byAlloc != 0 {
		t.Errorf("after %d denied REGISTERs: inflightBySID=%d inflightByAllocation=%d; want 0/0 — "+
			"an unauthenticated peer must not be able to pin map entries for the life of the process",
			n, bySID, byAlloc)
	}
	// killGenSession is only ever READ on this path (a map index read does not
	// insert), so it must stay empty too; asserting it pins "only those two tables
	// could ever grow" rather than leaving that as a claim in a comment.
	if killSess != 0 {
		t.Errorf("killGenSession=%d; want 0 (this path only reads it)", killSess)
	}
}

// TestRegisterLineIsBounded is the BDP-F1 guard: the reader's buffer IS the ceiling.
//
// THE DISCRIMINATOR IS TIME, NOT "does the server eventually give up". The first
// version of this test asserted only that the connection ended up closed — which an
// UNBOUNDED reader also does, just five seconds later when the read deadline fires.
// It passed against the reverted implementation, i.e. it was a vacuous guard, and
// mutation verification is the only reason that was noticed.
//
// Bounded, the server refuses the moment the buffer fills (microseconds after the
// bytes land). Unbounded, it accumulates until the 5s read deadline expires — and
// those five seconds of accumulation, at the attacker's line rate, ARE the defect.
// So: send far more than the ceiling with no newline, and require the refusal to
// arrive in a fraction of the read deadline.
func TestRegisterLineIsBounded(t *testing.T) {
	a := startAdmissionServer(t)
	srv := a.srv
	c, err := tls.Dial("tcp", a.addr, admissionTLS(a.fp))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = c.Close() }()
	_ = c.SetDeadline(time.Now().Add(30 * time.Second))

	start := time.Now()
	// One "line" far larger than the ceiling, with no newline anywhere.
	if _, werr := io.WriteString(c, "REGISTER "+strings.Repeat("A", 64*1024)); werr != nil {
		// The server refused mid-write, which is the bounded behaviour.
		if el := time.Since(start); el > boundedRefusalBudget {
			t.Fatalf("write failed only after %v; the ceiling should refuse promptly", el)
		}
		return
	}
	buf := make([]byte, 64)
	_, rerr := c.Read(buf)
	elapsed := time.Since(start)
	if rerr == nil {
		t.Fatal("server answered an over-long REGISTER line instead of refusing it")
	}
	// 2.5s is half the 5s read deadline: comfortably above any scheduling noise a
	// bounded refusal could suffer under -race and a loaded parallel run, and
	// comfortably below the deadline an unbounded reader must wait out.
	if elapsed > boundedRefusalBudget {
		t.Fatalf("over-long REGISTER line was refused only after %v (>%v): the reader accumulated "+
			"until the read deadline instead of stopping at the buffer ceiling — that window, at "+
			"the peer's line rate, is the remote memory-exhaustion defect", elapsed, boundedRefusalBudget)
	}
	if bySID, byAlloc, _ := waitInflightDrained(t, srv); bySID != 0 || byAlloc != 0 {
		t.Errorf("an over-long line left state behind: inflightBySID=%d inflightByAllocation=%d", bySID, byAlloc)
	}
}

// boundedRefusalBudget is half the handler's 5s read deadline — see the comment on
// TestRegisterLineIsBounded for why the discriminator is time.
const boundedRefusalBudget = 2500 * time.Millisecond

// TestRegisterRejectsInvalidIdentifiersBeforeTouchingState is the MP-1(b)/(c) guard.
// parseRegisterLine deliberately does not constrain sid/nid (its own fuzz seed says
// "empty sid: syntax accepts, caller validates"); this pins that the caller does.
// The assertion is not merely "denied" — it is "denied WITHOUT keying a map on it",
// because the map key and the log field are the actual hazards.
func TestRegisterRejectsInvalidIdentifiersBeforeTouchingState(t *testing.T) {
	a := startAdmissionServer(t)
	srv := a.srv
	long := strings.Repeat("x", 200) // ValidateNID caps at 32
	cases := []struct{ name, line string }{
		{"uppercase sid", "REGISTER LAB gpu1 14000 tok 0\n"},
		{"over-long nid", "REGISTER lab " + long + " 14000 tok 0\n"},
		{"reserved sid", "REGISTER admin gpu1 14000 tok 0\n"},
		{"sid with metacharacter", "REGISTER la*b gpu1 14000 tok 0\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reply := sendRegister(t, a, tc.line)
			if reply != "DENY malformed_register" {
				t.Errorf("reply = %q; want DENY malformed_register — an unvalidated sid/nid "+
					"reaches a long-lived map key and an unbounded log field", reply)
			}
		})
	}
	bySID, byAlloc, _ := waitInflightDrained(t, srv)
	if bySID != 0 || byAlloc != 0 {
		t.Errorf("invalid identifiers left state behind: inflightBySID=%d inflightByAllocation=%d",
			bySID, byAlloc)
	}
}

// TestAcceptLoopStopsOnClosedListener is the BDP-F3 guard's terminal condition. With
// a backoff in place, an accept error that is NOT routed to a return becomes a
// permanent once-per-second spin instead of the full-speed one it replaced — so the
// net.ErrClosed exit has to be checked independently of the s.closed flag.
func TestAcceptLoopStopsOnClosedListener(t *testing.T) {
	srv := NewServer("127.0.0.1:0", "127.0.0.1", nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	_ = ln.Close() // every Accept from here on returns net.ErrClosed
	// s.closed stays FALSE on purpose: that is what makes this a test of the
	// ErrClosed arm and not of the pre-existing shutdown arm.
	done := make(chan struct{})
	go func() {
		srv.acceptLoop(context.Background(), ln)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("acceptLoop did not return on a closed listener: it is spinning (before the " +
			"backoff this burned a core; after it, it is a permanent 1/s wakeup)")
	}
	if _, err := ln.Accept(); !errors.Is(err, net.ErrClosed) {
		t.Skipf("platform does not report net.ErrClosed for a closed listener: %v", err)
	}
}

// origin: prerelease audit round 2, CC-3.
//
// ONE SOURCE MUST NOT BE ABLE TO LOCK THE FLEET OUT.
//
// The first version of this ceiling was a single process-wide channel of 256. The slot
// is taken in acceptLoop BEFORE the goroutine starts, the listener is TLS so the
// handshake does not happen until the first read inside handleAgent, and that read
// carries a 5s deadline — so a peer that completes a bare TCP connect and then sends
// nothing holds a slot for five seconds at a cost of one socket. 256 idle sockets kept
// the public :7000 permanently saturated: every agent reconnect and every `tether
// expose` in the fleet failed with a bare EOF, while the broker looked healthy. That is
// a cheaper attack than the memory-pressure one the ceiling replaced.
//
// The discriminator is that a SECOND source still gets through while the first is at
// its limit. A test that only counted refusals would pass against the global-only
// ceiling too.
func TestOneSourceCannotExhaustTheHandshakeCeiling(t *testing.T) {
	srv := &Server{}

	const host = "10.0.0.7"
	for i := 0; i < maxRegisterHandshakesPerIP; i++ {
		if !srv.acquireHandshakeSlotForIP(host) {
			t.Fatalf("a legitimate source was refused at slot %d of its own per-IP budget %d",
				i, maxRegisterHandshakesPerIP)
		}
	}
	if srv.acquireHandshakeSlotForIP(host) {
		t.Fatalf("one source took more than %d concurrent handshakes.\n\n"+
			"Without a per-source cap it takes the whole process-wide budget, and every other "+
			"agent in the fleet is refused before it can even complete a TLS handshake.",
			maxRegisterHandshakesPerIP)
	}

	// THE POINT: somebody else is still served.
	if !srv.acquireHandshakeSlotForIP("10.0.0.8") {
		t.Fatal("a DIFFERENT source was refused while the first was at its limit.\n\n" +
			"That is the fleet-wide outage this cap exists to prevent — one attacker, or one " +
			"agent in a reconnect storm, denying every other host.")
	}
	srv.releaseHandshakeSlotForIP("10.0.0.8")

	// And the budget is genuinely returned, not leaked.
	srv.releaseHandshakeSlotForIP(host)
	if !srv.acquireHandshakeSlotForIP(host) {
		t.Fatal("a released slot was not reusable — the cap would ratchet down to zero over the " +
			"life of the process")
	}
	srv.releaseHandshakeSlotForIP(host)

	// Delete-on-zero: a counter left at 0 is a permanent map entry keyed by something the
	// peer chose, which is the leak releaseInflightLocked exists to avoid.
	for i := 0; i < maxRegisterHandshakesPerIP-1; i++ {
		srv.releaseHandshakeSlotForIP(host)
	}
	srv.hsMu.Lock()
	n := len(srv.hsPerIP)
	srv.hsMu.Unlock()
	if n != 0 {
		t.Errorf("hsPerIP retained %d entr(y/ies) after every slot was released; an unauthenticated "+
			"peer can pin one map entry per source address it dials from", n)
	}
}

// origin: prerelease audit round 2, CC-3.
//
// The process-wide ceiling stays, and stays ABOVE the per-source one by enough that the
// per-source cap is the binding constraint for a single attacker. If they were equal,
// one source would again be able to saturate everything.
func TestTheProcessWideCeilingIsNotTheBindingConstraintForOneSource(t *testing.T) {
	if maxRegisterHandshakesPerIP >= maxRegisterHandshakes {
		t.Fatalf("per-IP cap %d >= process-wide cap %d: one source can still take the whole budget",
			maxRegisterHandshakesPerIP, maxRegisterHandshakes)
	}
	// The property is "one source cannot take a large SHARE of the budget", not any
	// particular ratio. 8 means a single attacker holds at most 12.5% and seven eighths
	// stay available — which is the fleet-availability claim CC-3 was about. A higher
	// ratio would be better against one attacker and worse for a legitimate host with
	// many exposes, and that trade is what the sizing comment on the constants argues.
	const minSourcesAtFullOccupancy = 8
	if got := maxRegisterHandshakes / maxRegisterHandshakesPerIP; got < minSourcesAtFullOccupancy {
		t.Errorf("the process-wide ceiling admits only %d distinct sources at full per-source "+
			"occupancy (want >= %d): one source can take too large a share, which is the "+
			"fleet-wide outage the per-source cap exists to prevent",
			got, minSourcesAtFullOccupancy)
	}
}
