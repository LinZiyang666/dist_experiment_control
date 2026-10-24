// Real embedded NATS enforces private and legacy subscription permissions.
// Connection wrappers delay or cut real TCP bytes; no auth response is forged.
// Deep proxy/TLS/WSS cancellation is owned by P19 and tested separately below.
package natsinbox_test

import (
	"bufio"
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"io"
	"math/big"
	"net"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/natsinbox"
	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/proxydial"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

const inboxTestIdentity = "test-inbox-identity"

// origin: hot-update S6 plan §8.2 / T34
func TestBrokerInboxAbsoluteDeadline(t *testing.T) {
	requireInboxClock(t)
	t.Run("private_pair_and_detached_success", testInboxPrivateSuccess)
	t.Run("refusal_only_and_observer", testInboxRefusalFallback)
	t.Run("fallback_remaining", testInboxFallbackRemaining)
	t.Run("flush_error_is_not_refusal", testInboxFlushError)
	t.Run("legacy_probe_failure_preserves_connection", testLegacyInboxProbeFailure)
	t.Run("expired_and_invalid", testInboxInvalidDeadline)
	t.Run("observer_failures_close_owner", testInboxObserverErrors)
	// This is a real dependency test, not a substitute dial implementation.
	// P00 still owns the callable adapter until P19 lands; lead replaces it
	// with proxydial.OptionsWithin and removes the isolated stub/probe.
	t.Run("deep_dial_pending_P19", testInboxDeepDialBudget)
	t.Run("proxy_wss_pending_P19", testInboxProxyWSSBudget)
}

// origin: hot-update S6 plan §8.2 / T57
func TestBrokerInboxLeakCycles(t *testing.T) {
	requireInboxClock(t)
	// Warm the netpoller before the strict FD baseline; server and client
	// sockets below are closed and their goroutines joined on every round.
	exerciseInboxFallbackCancel(t)
	before, fds := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseInboxFallbackCancel(t)
	}
	testharness.AssertNoGoroutineLeak(t, "inbox fallback cancellation", before)
	testharness.AssertNoFDLeak(t, "inbox fallback cancellation", fds, 0)
}

func exerciseInboxFallbackCancel(t *testing.T) {
	t.Helper()
	testInboxRefusalFallback(t)
	testInboxFallbackRemaining(t)
	testInboxFlushError(t)
	testInboxObserverErrors(t)
}

// origin: hot-update S6 plan §8.2 / T34
// Legacy callers own request timeouts: a failed probe is not downgrade proof.
func testLegacyInboxProbeFailure(t *testing.T) {
	t.Helper()
	s := startInboxServer(t, true)
	defer stopInboxServer(s)
	d := &inboxTestDialer{cutProbe: true}
	nc, fallback, err := natsinbox.Connect(s.ClientURL(), inboxTestIdentity, append(inboxTestOptions(), nats.SetCustomDialer(d)))
	if nc != nil {
		defer nc.Close()
	}
	if err != nil || nc == nil || fallback || d.calls.Load() != 1 || !d.probeSeen.Load() {
		t.Fatalf("legacy probe failure changed admission: conn=%v fallback=%v calls=%d probe=%v err=%v", nc != nil, fallback, d.calls.Load(), d.probeSeen.Load(), err)
	}
	if nc.Opts.InboxPrefix != auth.InboxPrefixFor(inboxTestIdentity) || nc.Opts.User != auth.InboxCapableMarker {
		t.Fatal("failed legacy probe lost private inbox pairing")
	}
	if err := nc.FlushTimeout(time.Second); err == nil {
		t.Fatal("probe failure was not exercised on the disconnected transport")
	}
}

func testInboxPrivateSuccess(t *testing.T) {
	t.Helper()
	s := startInboxServer(t, true)
	defer stopInboxServer(s)
	observer := &inboxObserver{}
	deadline := inboxTestDeadline(t, 120*time.Millisecond)
	nc, fallback, err := natsinbox.ConnectWithin(s.ClientURL(), inboxTestIdentity, inboxTestOptions(), deadline, observer)
	if err != nil || fallback || nc == nil {
		t.Fatalf("private admission: conn=%v fallback=%v err=%v", nc != nil, fallback, err)
	}
	defer nc.Close()
	if nc.Opts.InboxPrefix != auth.InboxPrefixFor(inboxTestIdentity) || nc.Opts.User != auth.InboxCapableMarker {
		t.Fatal("private prefix and CONNECT marker lost their pairing")
	}
	observer.assert(t, []controlbudget.AttemptPhase{controlbudget.PhaseConnect, controlbudget.PhaseInboxProbe}, []string{"success", "success"})
	// This is an expiry boundary, not a readiness sleep: a sticky deadline
	// or stale cancellation hook must fail the subsequent real PING/PONG.
	now, err := procrequest.BootNow()
	remaining := time.Duration(int64(deadline) - now)
	if err != nil {
		t.Fatal(err)
	}
	timer := time.NewTimer(remaining + 30*time.Millisecond)
	defer timer.Stop()
	<-timer.C
	if err := nc.FlushTimeout(time.Second); err != nil {
		t.Fatalf("admission deadline killed the long-lived connection: %v", err)
	}
}

func testInboxRefusalFallback(t *testing.T) {
	t.Helper()
	s := startInboxServer(t, false)
	defer stopInboxServer(s)
	d := &inboxTestDialer{}
	observer := &inboxObserver{}
	callback := make(chan error, 8)
	opts := append(inboxTestOptions(), nats.SetCustomDialer(d), nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) { callback <- err }))
	inspect, err := inspectInboxTestOptions(opts)
	if err != nil {
		t.Fatal(err)
	}
	nc, fallback, err := natsinbox.ConnectWithin(s.ClientURL(), inboxTestIdentity, opts, inboxTestDeadline(t, time.Second), observer)
	if err != nil || !fallback || nc == nil {
		t.Fatalf("legacy admission: conn=%v fallback=%v err=%v", nc != nil, fallback, err)
	}
	defer nc.Close()
	if d.calls.Load() != 2 || nc.Opts.InboxPrefix != "" || nc.Opts.User != "legacy" {
		t.Fatalf("fallback did not use the original options: calls=%d prefix=%q user=%q", d.calls.Load(), nc.Opts.InboxPrefix, nc.Opts.User)
	}
	after, err := inspectInboxTestOptions(opts)
	if err != nil || after.User != inspect.User || after.Timeout != inspect.Timeout || after.InboxPrefix != inspect.InboxPrefix {
		t.Fatal("ConnectWithin mutated the caller's options")
	}
	observer.assert(t, []controlbudget.AttemptPhase{controlbudget.PhaseConnect, controlbudget.PhaseInboxProbe, controlbudget.PhaseInboxFallback}, []string{"success", "refused", "success"})
	if _, err := nc.SubscribeSync("forbidden.business"); err != nil {
		t.Fatal(err)
	}
	if err := nc.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-callback:
		if !strings.Contains(err.Error(), "forbidden.business") {
			t.Fatalf("probe refusal leaked to caller callback: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("unrelated permission error was swallowed")
	}
	// Repeat the callback check on a successful private connection: its
	// wrapper survives admission and must preserve the caller's handler.
	modern := startInboxServer(t, true)
	defer stopInboxServer(modern)
	private, fallback, err := natsinbox.ConnectWithin(modern.ClientURL(), inboxTestIdentity, opts, inboxTestDeadline(t, time.Second), nil)
	if err != nil || fallback {
		t.Fatalf("private callback admission: fallback=%v err=%v", fallback, err)
	}
	defer private.Close()
	if _, err := private.SubscribeSync("forbidden.business"); err != nil {
		t.Fatal(err)
	}
	if err := private.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-callback:
		if !strings.Contains(err.Error(), "forbidden.business") {
			t.Fatalf("wrong private callback error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("private wrapper swallowed unrelated permission error")
	}
}

func testInboxFallbackRemaining(t *testing.T) {
	t.Helper()
	s := startInboxServer(t, false)
	defer stopInboxServer(s)
	blackhole, closeHole := inboxBlackhole(t)
	defer closeHole()
	const budget = 450 * time.Millisecond
	d := &inboxTestDialer{probeDelay: 250 * time.Millisecond, fallbackAddress: blackhole}
	observer := &inboxObserver{}
	start := time.Now()
	nc, fallback, err := natsinbox.ConnectWithin(s.ClientURL(), inboxTestIdentity, append(inboxTestOptions(), nats.Timeout(700*time.Millisecond), nats.SetCustomDialer(d)), inboxTestDeadline(t, budget), observer)
	if nc != nil {
		nc.Close()
	}
	elapsed := time.Since(start)
	if err == nil || nc != nil || fallback || d.calls.Load() != 2 || !d.probeSeen.Load() {
		t.Fatalf("stalled fallback was not exercised: conn=%v fallback=%v calls=%d probe=%v err=%v", nc != nil, fallback, d.calls.Load(), d.probeSeen.Load(), err)
	}
	if elapsed > budget+120*time.Millisecond {
		t.Fatalf("fallback reset the absolute budget: elapsed=%s budget=%s", elapsed, budget)
	}
	observer.assert(t, []controlbudget.AttemptPhase{controlbudget.PhaseConnect, controlbudget.PhaseInboxProbe, controlbudget.PhaseInboxFallback}, []string{"success", "refused", "timeout"})
}

func testInboxFlushError(t *testing.T) {
	t.Helper()
	s := startInboxServer(t, true)
	defer stopInboxServer(s)
	d := &inboxTestDialer{cutProbe: true}
	observer := &inboxObserver{}
	nc, fallback, err := natsinbox.ConnectWithin(s.ClientURL(), inboxTestIdentity, append(inboxTestOptions(), nats.SetCustomDialer(d)), inboxTestDeadline(t, time.Second), observer)
	if nc != nil {
		nc.Close()
	}
	if err == nil || nc != nil || fallback || d.calls.Load() != 1 || !d.probeSeen.Load() {
		t.Fatalf("flush failure triggered a downgrade: conn=%v fallback=%v calls=%d probe=%v err=%v", nc != nil, fallback, d.calls.Load(), d.probeSeen.Load(), err)
	}
	observer.assert(t, []controlbudget.AttemptPhase{controlbudget.PhaseConnect, controlbudget.PhaseInboxProbe}, []string{"success", "error"})
}

func testInboxInvalidDeadline(t *testing.T) {
	t.Helper()
	d := &inboxTestDialer{}
	for _, deadline := range []controlbudget.Deadline{0, -1, 1} {
		nc, fallback, err := natsinbox.ConnectWithin("nats://127.0.0.1:1", inboxTestIdentity, []nats.Option{nats.SetCustomDialer(d)}, deadline, nil)
		if err == nil || nc != nil || fallback || d.calls.Load() != 0 {
			t.Fatalf("invalid deadline admitted a dial: deadline=%d calls=%d err=%v", deadline, d.calls.Load(), err)
		}
	}
	optionErr := errors.New("invalid option")
	nc, fallback, err := natsinbox.ConnectWithin("nats://127.0.0.1:1", inboxTestIdentity, []nats.Option{func(*nats.Options) error { return optionErr }}, inboxTestDeadline(t, time.Second), nil)
	if !errors.Is(err, optionErr) || nc != nil || fallback {
		t.Fatalf("option error was lost: %v", err)
	}
}

func testInboxObserverErrors(t *testing.T) {
	t.Helper()
	s := startInboxServer(t, false)
	defer stopInboxServer(s)
	for _, failure := range []struct{ begin, record int }{{1, 0}, {2, 0}, {3, 0}, {0, 1}, {0, 2}, {0, 3}} {
		observer := &inboxObserver{failBegin: failure.begin, failRecord: failure.record}
		nc, fallback, err := natsinbox.ConnectWithin(s.ClientURL(), inboxTestIdentity, inboxTestOptions(), inboxTestDeadline(t, time.Second), observer)
		if nc != nil {
			nc.Close()
		}
		if !errors.Is(err, errInboxObserver) || nc != nil || fallback {
			t.Fatalf("observer failure was hidden: failure=%+v conn=%v fallback=%v err=%v", failure, nc != nil, fallback, err)
		}
		// Observe release while the server is alive. Fixture shutdown would
		// close the peer and hide a client owner abandoned on an error path.
		if !testharness.WaitFor(t, time.Second, time.Millisecond, func() bool { return s.NumClients() == 0 }) {
			t.Fatalf("observer failure retained a connection: failure=%+v clients=%d", failure, s.NumClients())
		}
	}
}

// The full multi-URL deep-dial seam consumes P19's real adapter. This remains
// red with P00's explicit NotImplementedError; there is no skip or fake success.
func testInboxDeepDialBudget(t *testing.T) {
	t.Helper()
	addresses := make([]string, 3)
	for i := range addresses {
		address, closeHole := inboxBlackhole(t)
		defer closeHole()
		addresses[i] = "nats://" + address
	}
	const budget = 350 * time.Millisecond
	deadline := inboxTestDeadline(t, budget)
	observer := &inboxObserver{}
	opts, err := proxydial.OptionsWithin(proxydial.Env(func(string) (string, bool) { return "", false }), deadline, observer)
	if err != nil {
		t.Fatalf("P19 bounded dial dependency: %v", err)
	}
	opts = append(opts, inboxTestOptions()...)
	start := time.Now()
	nc, fallback, err := natsinbox.ConnectWithin(strings.Join(addresses, ","), inboxTestIdentity, opts, deadline, observer)
	if nc != nil {
		nc.Close()
	}
	if err == nil || nc != nil || fallback || time.Since(start) > budget+120*time.Millisecond {
		t.Fatalf("multi-URL dial exceeded the shared deadline: conn=%v fallback=%v elapsed=%s err=%v", nc != nil, fallback, time.Since(start), err)
	}
	if len(observer.snapshot()) == 0 {
		t.Fatal("deep dial attempt disappeared")
	}
}

func requireInboxClock(t *testing.T) {
	t.Helper()
	if runtime.GOOS != "linux" {
		t.Skip("BOOTTIME admission requires Linux; Darwin compilation is a separate receipt")
	}
	if _, err := procrequest.BootNow(); err != nil {
		t.Fatal(err)
	}
}

func inboxTestDeadline(t *testing.T, duration time.Duration) controlbudget.Deadline {
	t.Helper()
	now, err := procrequest.BootNow()
	if err != nil {
		t.Fatal(err)
	}
	return controlbudget.Deadline(now + int64(duration))
}

func inboxTestOptions() []nats.Option {
	return []nats.Option{nats.UserInfo("legacy", ""), nats.NoReconnect(), nats.Timeout(time.Second), nats.DontRandomize()}
}

func startInboxServer(t *testing.T, private bool, wsConfig ...*tls.Config) *server.Server {
	t.Helper()
	grant := "_INBOX.>"
	if private {
		grant = auth.InboxPrefixFor(inboxTestIdentity) + ".>"
	}
	options := &server.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true,
		Users: []*server.User{
			{Username: auth.InboxCapableMarker, Permissions: &server.Permissions{Subscribe: &server.SubjectPermission{Allow: []string{grant}}}},
			{Username: "legacy", Permissions: &server.Permissions{Subscribe: &server.SubjectPermission{Allow: []string{"_INBOX.>"}}}},
		},
	}
	if len(wsConfig) != 0 {
		options.Websocket = server.WebsocketOpts{Host: "127.0.0.1", Port: -1, TLSConfig: wsConfig[0]}
	}
	s, err := server.NewServer(options)
	if err != nil {
		t.Fatal(err)
	}
	go s.Start()
	if !s.ReadyForConnections(5 * time.Second) {
		stopInboxServer(s)
		t.Fatal("NATS fixture did not become ready")
	}
	return s
}

func stopInboxServer(s *server.Server) {
	s.Shutdown()
	s.WaitForShutdown()
}

// inboxBlackhole accepts real TCP and reads it without sending an INFO reply.
// Closing the listener and both directions joins the accept/read owner.
func inboxBlackhole(t *testing.T) (string, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var connections []net.Conn
	var workers sync.WaitGroup
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			connections = append(connections, c)
			mu.Unlock()
			workers.Add(1)
			go func() { defer workers.Done(); _, _ = io.Copy(io.Discard, c); _ = c.Close() }()
		}
	}()
	return ln.Addr().String(), func() {
		_ = ln.Close()
		<-done
		mu.Lock()
		for _, c := range connections {
			_ = c.Close()
		}
		mu.Unlock()
		workers.Wait()
	}
}

type inboxTestDialer struct {
	calls           atomic.Int32
	probeSeen       atomic.Bool
	probeDelay      time.Duration
	cutProbe        bool
	fallbackAddress string
}

func (d *inboxTestDialer) Dial(network, address string) (net.Conn, error) {
	ordinal := d.calls.Add(1)
	if ordinal > 1 && d.fallbackAddress != "" {
		address = d.fallbackAddress
	}
	c, err := (&net.Dialer{Timeout: time.Second}).Dial(network, address)
	if err != nil {
		return nil, err
	}
	if ordinal == 1 {
		return &inboxProbeConn{Conn: c, dialer: d}, nil
	}
	return c, nil
}

type inboxProbeConn struct {
	net.Conn
	dialer *inboxTestDialer
}

func (c *inboxProbeConn) Write(body []byte) (int, error) {
	if bytes.Contains(body, []byte("SUB "+auth.InboxPrefixFor(inboxTestIdentity))) && c.dialer.probeSeen.CompareAndSwap(false, true) {
		if c.dialer.cutProbe {
			_ = c.Close()
			return 0, net.ErrClosed
		}
		if c.dialer.probeDelay > 0 {
			timer := time.NewTimer(c.dialer.probeDelay)
			defer timer.Stop()
			<-timer.C
		}
	}
	return c.Conn.Write(body)
}

var errInboxObserver = errors.New("inbox observer capacity exceeded")

type inboxObserver struct {
	mu         sync.Mutex
	begun      int
	attempts   []controlbudget.Attempt
	failBegin  int
	failRecord int
}

func (o *inboxObserver) Begin(logicalID string, phase controlbudget.AttemptPhase) (controlbudget.Attempt, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.begun++
	if o.begun == o.failBegin {
		return controlbudget.Attempt{}, errInboxObserver
	}
	now, err := procrequest.BootNow()
	if logicalID == "" {
		logicalID = "inbox-logical-operation"
	}
	return controlbudget.Attempt{LogicalID: logicalID, Ordinal: uint32(o.begun), Phase: phase, StartedMonoNS: now}, err
}

func (o *inboxObserver) Record(attempt controlbudget.Attempt) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.attempts)+1 == o.failRecord {
		return errInboxObserver
	}
	o.attempts = append(o.attempts, attempt)
	return nil
}

func (*inboxObserver) Close() error { return nil }

func (o *inboxObserver) snapshot() []controlbudget.Attempt {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]controlbudget.Attempt(nil), o.attempts...)
}

func (o *inboxObserver) assert(t *testing.T, phases []controlbudget.AttemptPhase, classes []string) {
	t.Helper()
	attempts := o.snapshot()
	if len(attempts) != len(phases) {
		t.Fatalf("attempts lost: got=%+v want=%v", attempts, phases)
	}
	for i, attempt := range attempts {
		if attempt.Phase != phases[i] || attempt.Ordinal != uint32(i+1) || attempt.Class != classes[i] || attempt.LogicalID != "inbox-logical-operation" || attempt.StartedMonoNS <= 0 || attempt.FinishedMonoNS < attempt.StartedMonoNS {
			t.Fatalf("invalid attempt receipt %d: %+v", i, attempt)
		}
		if attempt.Sent != (attempt.Phase == controlbudget.PhaseInboxProbe) {
			t.Fatalf("probe transmission receipt lost: %+v", attempt)
		}
	}
}

func inspectInboxTestOptions(opts []nats.Option) (nats.Options, error) {
	inspect := nats.GetDefaultOptions()
	for _, option := range opts {
		if err := option(&inspect); err != nil {
			return nats.Options{}, err
		}
	}
	return inspect, nil
}

// The proxy forwards real WSS bytes and rejects local DNS assumptions by using
// an unresolvable target name. Both private and legacy probes reach real NATS.
func testInboxProxyWSSBudget(t *testing.T) {
	t.Helper()
	certificate, roots := inboxTestTLS(t)
	for _, private := range []bool{true, false} {
		s := startInboxServer(t, private, certificate)
		info, err := s.Varz(nil)
		if err != nil {
			stopInboxServer(s)
			t.Fatal(err)
		}
		backend := net.JoinHostPort("127.0.0.1", strconv.Itoa(info.Websocket.Port))
		proxyURL, connects, closeProxy := inboxConnectProxy(t, backend)
		func() {
			defer stopInboxServer(s)
			defer closeProxy()
			const budget = 400 * time.Millisecond
			deadline := inboxTestDeadline(t, budget)
			opts, err := proxydial.OptionsWithin(func(key string) (string, bool) {
				return proxyURL, key == "HTTPS_PROXY"
			}, deadline, nil)
			if err != nil {
				t.Fatalf("P19 proxy/WSS dependency: %v", err)
			}
			opts = append(opts, inboxTestOptions()...)
			opts = append(opts, nats.Secure(&tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}))
			nc, fallback, err := natsinbox.ConnectWithin("wss://inbox-budget.invalid:"+strconv.Itoa(info.Websocket.Port), inboxTestIdentity, opts, deadline, nil)
			if err != nil || nc == nil || fallback == private {
				t.Fatalf("proxy/WSS admission: private=%v fallback=%v err=%v", private, fallback, err)
			}
			defer nc.Close()
			wantConnects := int32(1)
			if !private {
				wantConnects = 2
			}
			if connects.Load() != wantConnects {
				t.Fatalf("remote DNS/proxy bypassed: connects=%d want=%d", connects.Load(), wantConnects)
			}
			now, err := procrequest.BootNow()
			if err != nil {
				t.Fatal(err)
			}
			timer := time.NewTimer(time.Duration(int64(deadline)-now) + 30*time.Millisecond)
			defer timer.Stop()
			<-timer.C
			if err := nc.FlushTimeout(time.Second); err != nil {
				t.Fatalf("proxy/WSS retained its admission cancellation hook: %v", err)
			}
		}()
	}
}

func inboxTestTLS(t *testing.T) (*tls.Config, *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	cert := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "inbox-budget.invalid"},
		DNSNames: []string{"inbox-budget.invalid"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, cert, cert, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(leaf)
	return &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}, MinVersion: tls.VersionTLS12}, roots
}

// Each CONNECT owns both sockets until both copy loops finish. Closing the
// fixture wakes the loops before joining; it cannot orphan a stalled handshake.
func inboxConnectProxy(t *testing.T, backend string) (string, *atomic.Int32, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var connects atomic.Int32
	var mu sync.Mutex
	var owned []net.Conn
	stopped := false
	var workers sync.WaitGroup
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			client, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			owned = append(owned, client)
			mu.Unlock()
			workers.Add(1)
			go func() {
				defer workers.Done()
				defer closeInboxTestConnection(t, client)
				reader := bufio.NewReader(client)
				line, err := reader.ReadString('\n')
				if err != nil || !strings.HasPrefix(line, "CONNECT inbox-budget.invalid:") {
					return
				}
				for {
					line, err = reader.ReadString('\n')
					if err != nil {
						return
					}
					if line == "\r\n" {
						break
					}
				}
				upstream, err := (&net.Dialer{Timeout: time.Second}).Dial("tcp", backend)
				if err != nil {
					return
				}
				defer closeInboxTestConnection(t, upstream)
				mu.Lock()
				if stopped {
					mu.Unlock()
					return
				}
				owned = append(owned, upstream)
				mu.Unlock()
				connects.Add(1)
				if _, err := io.WriteString(client, "HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
					return
				}
				copied := make(chan struct{})
				go func() { _, _ = io.Copy(upstream, reader); _ = upstream.Close(); close(copied) }()
				_, _ = io.Copy(client, upstream)
				_ = client.Close()
				_ = upstream.Close()
				<-copied
			}()
		}
	}()
	return "http://" + ln.Addr().String(), &connects, func() {
		_ = ln.Close()
		<-done
		mu.Lock()
		stopped = true
		for _, c := range owned {
			_ = c.Close()
		}
		mu.Unlock()
		workers.Wait()
	}
}

// origin: hot-update S6 plan §8.2 / T34
// The copy loops and failure backstop can close the same owned socket first.
func closeInboxTestConnection(t *testing.T, conn net.Conn) {
	t.Helper()
	if err := conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		t.Errorf("close inbox fixture connection: %v", err)
	}
}
