package proxydial

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/testharness"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// The observer is a unit-test receiver for the transport emitter, not a retry
// implementation. Bound identities model the operation-level adapter P18 owns.
type dialObserver struct {
	mu         sync.Mutex
	attempts   []controlbudget.Attempt
	next       uint32
	failBegin  bool
	failRecord bool
}

func (o *dialObserver) Begin(id string, phase controlbudget.AttemptPhase) (controlbudget.Attempt, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.failBegin {
		return controlbudget.Attempt{}, errors.New("observer overflow")
	}
	if id != "" {
		return controlbudget.Attempt{}, errors.New("transport invented logical identity")
	}
	o.next++
	now, err := procrequest.BootNow()
	return controlbudget.Attempt{LogicalID: "operation", Ordinal: o.next, Phase: phase, StartedMonoNS: now}, err
}
func (o *dialObserver) Record(a controlbudget.Attempt) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.failRecord {
		return errors.New("observer overflow")
	}
	o.attempts = append(o.attempts, a)
	return nil
}
func (o *dialObserver) Close() error { return nil }
func (o *dialObserver) snapshot() []controlbudget.Attempt {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]controlbudget.Attempt(nil), o.attempts...)
}

func dialDeadline(t *testing.T, remaining time.Duration) controlbudget.Deadline {
	t.Helper()
	now, err := procrequest.BootNow()
	if err != nil {
		t.Skipf("boot clock unavailable: %v", err)
	}
	return controlbudget.Deadline(now + int64(remaining))
}

// Each peer has one joined accepted handler. A stalled handler is released by
// the PRODUCT closing its socket; test cleanup is only the failure-path backstop.
func budgetPeer(t *testing.T, handle func(net.Conn)) (string, <-chan struct{}) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	var mu sync.Mutex
	var accepted net.Conn
	go func() {
		defer close(done)
		c, err := ln.Accept()
		if err != nil {
			return
		}
		mu.Lock()
		accepted = c
		mu.Unlock()
		defer func() {
			// origin: hot-update S6 plan §8.2 / T34Proxy
			// The failure backstop may already have closed this accepted socket.
			if err := c.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				t.Errorf("close proxy fixture connection: %v", err)
			}
		}()
		handle(c)
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		mu.Lock()
		if accepted != nil {
			_ = accepted.Close()
		}
		mu.Unlock()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("peer handler failed to join")
		}
	})
	return ln.Addr().String(), done
}

func awaitPeer(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("product abandoned handshake socket/worker")
	}
}

// origin: hot-update S6 plan §8.2 / T34Proxy
func TestBrokerProxyAbsoluteDeadline(t *testing.T) {
	t.Run("always_bounded_direct_and_bypass", func(t *testing.T) {
		for _, env := range []Env{fakeEnv(nil), fakeEnv(map[string]string{"HTTPS_PROXY": "http://unused.invalid:1", "NO_PROXY": "*"})} {
			opts, err := OptionsWithin(env, dialDeadline(t, 80*time.Millisecond), &dialObserver{})
			if err != nil {
				t.Fatal(err)
			}
			o := applyToProbe(t, opts)
			if !o.SkipHostLookup || o.CustomDialer == nil {
				t.Fatal("direct path must bypass nats.go LookupHost")
			}
			d := o.CustomDialer.(*dialer)
			d.lookup = func(ctx context.Context, _ string) ([]string, error) {
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(500 * time.Millisecond):
					return nil, errors.New("unbounded resolver")
				}
			}
			started := time.Now()
			_, err = d.Dial("tcp", "budget.invalid:4222")
			if !errors.Is(err, context.DeadlineExceeded) || time.Since(started) > 350*time.Millisecond {
				t.Fatalf("resolver did not consume remaining budget: %v (%v)", err, time.Since(started))
			}
		}
	})
	for _, mode := range []string{"http", "https", "socks5", "tls", "ws", "wss", "http_ws", "http_wss", "connect"} {
		t.Run(mode, func(t *testing.T) { exerciseStalledAdmission(t, mode) })
	}
	t.Run("one_budget_across_urls", func(t *testing.T) {
		first, firstDone := budgetPeer(t, func(c net.Conn) {
			_, _ = c.Write([]byte("INFO {}\r\n"))
			br := bufio.NewReader(c)
			_, _ = br.ReadString('\n')
			_, _ = br.ReadString('\n')
			// Hold the first endpoint until it consumes the entire admission budget.
			_, _ = io.Copy(io.Discard, br)
		})
		second, _ := budgetPeer(t, func(c net.Conn) { _, _ = io.Copy(io.Discard, c) })
		observer := &dialObserver{}
		remaining := 200 * time.Millisecond
		opts, err := OptionsWithin(fakeEnv(nil), dialDeadline(t, remaining), observer)
		if err != nil {
			t.Fatal(err)
		}
		started := time.Now()
		nc, err := nats.Connect("nats://"+first+",nats://"+second, append([]nats.Option{nats.DontRandomize()}, opts...)...)
		if nc != nil {
			nc.Close()
		}
		err = FinishWithin(opts, err)
		if err == nil || time.Since(started) > remaining+100*time.Millisecond {
			t.Fatalf("URL pool reset budget: %v (%v)", err, time.Since(started))
		}
		awaitPeer(t, firstDone)
		attempts := observer.snapshot()
		if len(attempts) != 2 || attempts[1].Sent || attempts[1].Class != "timeout" || attempts[0].Class != "timeout" || !attempts[0].Sent {
			t.Fatalf("missing first failed/sent CONNECT receipt: %+v", attempts)
		}
	})
	t.Run("early_cancellation_joins_dns_and_proxy", exerciseExplicitCancellation)
	t.Run("success_detaches_and_keeps_buffered_info", exerciseDetachedAdmission)
	t.Run("secure_success_detaches", exerciseSecureAdmission)
	t.Run("observer_failure_is_terminal", func(t *testing.T) {
		observer := &dialObserver{failBegin: true}
		opts, err := OptionsWithin(fakeEnv(nil), dialDeadline(t, time.Second), observer)
		if err != nil {
			t.Fatal(err)
		}
		_, err = applyToProbe(t, opts).CustomDialer.Dial("tcp", "127.0.0.1:1")
		if err == nil || !strings.Contains(err.Error(), "overflow") {
			t.Fatalf("Begin failure hidden: %v", err)
		}
		if err = FinishWithin(opts, errors.New("nats replaced dial error")); !strings.Contains(err.Error(), "overflow") {
			t.Fatalf("Begin failure lost during NATS cleanup: %v", err)
		}
		address, done := budgetPeer(t, func(c net.Conn) { _, _ = io.Copy(io.Discard, c) })
		observer = &dialObserver{failRecord: true}
		opts, err = OptionsWithin(fakeEnv(nil), dialDeadline(t, 60*time.Millisecond), observer)
		if err != nil {
			t.Fatal(err)
		}
		_, err = nats.Connect("nats://"+address, opts...)
		err = FinishWithin(opts, err)
		if err == nil || !strings.Contains(err.Error(), "overflow") {
			t.Fatalf("Record failure hidden: %v", err)
		}
		awaitPeer(t, done)
	})
	t.Run("invalid_deadline_and_proxy", func(t *testing.T) {
		if _, err := OptionsWithin(fakeEnv(nil), 0, nil); err == nil {
			t.Fatal("zero deadline accepted")
		}
		if _, err := OptionsWithin(fakeEnv(map[string]string{"HTTPS_PROXY": "ftp://secret:password@invalid:1"}), dialDeadline(t, time.Second), nil); err == nil || strings.Contains(err.Error(), "password") {
			t.Fatalf("proxy did not fail closed/secret-free: %v", err)
		}
	})
}

func exerciseStalledAdmission(t *testing.T, mode string) {
	t.Helper()
	observer := &dialObserver{}
	address, done := budgetPeer(t, func(c net.Conn) {
		switch mode {
		case "http", "http_ws", "http_wss":
			br := bufio.NewReader(c)
			req, err := http.ReadRequest(br)
			if err != nil {
				return
			}
			if req.Host != "broker.invalid:4222" {
				t.Errorf("proxy lost remote DNS hostname: %q", req.Host)
			}
			response := "HTTP/1.1 200"
			if mode != "http" {
				response = "HTTP/1.1 200 Connection established\r\n\r\n"
			}
			_, _ = c.Write([]byte(response))
			_, _ = io.Copy(io.Discard, br)
		case "connect":
			_, _ = c.Write([]byte("INFO {}\r\n"))
			_, _ = io.Copy(io.Discard, c)
		default:
			_, _ = io.Copy(io.Discard, c)
		}
	})
	env := fakeEnv(nil)
	target := mode + "://" + address
	if mode == "http" || mode == "https" || mode == "socks5" {
		env = fakeEnv(map[string]string{"HTTPS_PROXY": mode + "://" + address})
		target = "nats://broker.invalid:4222"
	}
	if mode == "connect" {
		target = "nats://" + address
	}
	if mode == "http_ws" || mode == "http_wss" {
		env = fakeEnv(map[string]string{"HTTPS_PROXY": "http://" + address})
		scheme := "ws"
		if mode == "http_wss" {
			scheme = "wss"
		}
		target = scheme + "://broker.invalid:4222"
	}
	opts, err := OptionsWithin(env, dialDeadline(t, 70*time.Millisecond), observer)
	if err != nil {
		t.Fatal(err)
	}
	if mode == "tls" || mode == "wss" || mode == "http_wss" {
		opts = append([]nats.Option{nats.Secure(&tls.Config{MinVersion: tls.VersionTLS12}), nats.TLSHandshakeFirst()}, opts...)
	}
	started := time.Now()
	// Outer watchdog restores a mutated fixed-10s path without leaving workers.
	result := make(chan error, 1)
	go func() {
		nc, e := nats.Connect(target, opts...)
		if nc != nil {
			nc.Close()
		}
		result <- FinishWithin(opts, e)
	}()
	select {
	case err = <-result:
	case <-time.After(500 * time.Millisecond):
		_ = FinishWithin(opts, context.DeadlineExceeded)
		select {
		case <-result:
		case <-time.After(time.Second):
		}
		t.Fatal("fixed/reset deadline or abandoned handshake exceeded admission budget")
	}
	if err == nil || time.Since(started) > 400*time.Millisecond {
		t.Fatalf("%s handshake escaped deadline: %v (%v)", mode, err, time.Since(started))
	}
	awaitPeer(t, done)
	attempts := observer.snapshot()
	found := false
	for _, a := range attempts {
		if a.LogicalID != "operation" || a.FinishedMonoNS < a.StartedMonoNS {
			t.Fatalf("invalid transport receipt: %+v", a)
		}
		if a.Phase == controlbudget.PhaseConnect && a.Class == "timeout" {
			found = true
		}
	}
	if !found {
		t.Fatalf("%s missing failed CONNECT attempt: %+v", mode, attempts)
	}
	if mode == "ws" || mode == "wss" || mode == "tls" || mode == "http_ws" || mode == "http_wss" {
		phase := controlbudget.PhaseTLS
		if mode == "ws" || mode == "http_ws" {
			phase = controlbudget.PhaseWS
		}
		found = false
		for _, a := range attempts {
			if a.Phase == phase {
				found = true
			}
		}
		if !found {
			t.Fatalf("%s missing handshake phase: %+v", mode, attempts)
		}
	}
}

func exerciseDetachedAdmission(t *testing.T) {
	t.Helper()
	observer := &dialObserver{}
	address, done := budgetPeer(t, func(c net.Conn) {
		br := bufio.NewReader(c)
		req, err := http.ReadRequest(br)
		if err != nil {
			return
		}
		if req.Host != "broker.invalid:4222" {
			t.Errorf("remote DNS target: %q", req.Host)
		}
		_, _ = c.Write([]byte("HTTP/1.1 200 Connection established\r\n\r\nINFO {}\r\n"))
		for {
			line, e := br.ReadString('\n')
			if e != nil {
				return
			}
			if strings.TrimSpace(line) == "PING" {
				_, _ = c.Write([]byte("PONG\r\n"))
			}
		}
	})
	opts, err := OptionsWithin(fakeEnv(map[string]string{"HTTPS_PROXY": "http://" + address}), dialDeadline(t, 150*time.Millisecond), observer)
	if err != nil {
		t.Fatal(err)
	}
	nc, err := nats.Connect("nats://broker.invalid:4222", opts...)
	err = FinishWithin(opts, err)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	// Waiting past the actual admission deadline proves the former sticky timer
	// cannot kill a long-running command. This is the timing oracle itself.
	time.Sleep(200 * time.Millisecond)
	if err := nc.FlushTimeout(time.Second); err != nil {
		t.Fatalf("successful CONNECT retained admission deadline: %v", err)
	}
	nc.Close()
	awaitPeer(t, done)
	attempts := observer.snapshot()
	found := false
	for _, a := range attempts {
		if a.Phase == controlbudget.PhaseConnect && a.Class == "ok" && a.Sent {
			found = true
		}
	}
	if !found {
		t.Fatalf("success lost CONNECT receipt: %+v", attempts)
	}
}

// origin: hot-update S6 plan §8.2 / T57Proxy
func TestBrokerProxyLeakCycles(t *testing.T) {
	goroutines, fds := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseProxyDeadlineCancel(t)
	}
	testharness.AssertNoGoroutineLeak(t, "proxy admission cancellation", goroutines)
	testharness.AssertNoFDLeak(t, "proxy admission cancellation", fds, 0)
}
func exerciseProxyDeadlineCancel(t *testing.T) {
	t.Helper()
	// Each round creates real stalled HTTP/SOCKS/TLS/WSS sockets, reaches the
	// shared absolute deadline and observes peer EOF before fixture cleanup.
	for _, mode := range []string{"http", "socks5", "tls", "wss", "http_ws", "http_wss", "connect"} {
		t.Run(fmt.Sprintf("cancel_%s", mode), func(t *testing.T) { exerciseStalledAdmission(t, mode) })
	}
	t.Run("early_cancel", exerciseExplicitCancellation)
	t.Run("detached", exerciseDetachedAdmission)
	t.Run("secure_detached", exerciseSecureAdmission)
}

func exerciseExplicitCancellation(t *testing.T) {
	t.Helper()
	t.Run("resolver", func(t *testing.T) {
		observer := &dialObserver{}
		opts, err := OptionsWithin(fakeEnv(nil), dialDeadline(t, time.Second), observer)
		if err != nil {
			t.Fatal(err)
		}
		d := applyToProbe(t, opts).CustomDialer.(*dialer)
		entered := make(chan struct{})
		canceled, release := make(chan struct{}), make(chan struct{})
		d.lookup = func(ctx context.Context, _ string) ([]string, error) {
			close(entered)
			<-ctx.Done()
			close(canceled)
			<-release
			return nil, ctx.Err()
		}
		result := make(chan error, 1)
		go func() { _, e := d.Dial("tcp", "cancel.invalid:4222"); result <- e }()
		<-entered

		finished := make(chan error, 1)
		go func() { finished <- FinishWithin(opts, context.Canceled) }()
		<-canceled
		// A cancellation receipt is not a join receipt. The resolver deliberately
		// holds its exit until this barrier verifies that FinishWithin is waiting.
		select {
		case e := <-finished:
			t.Errorf("cancellation returned before resolver exit: %v", e)
		case <-time.After(30 * time.Millisecond):
		}
		close(release)
		select {
		case err = <-finished:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("cancel result: %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("resolver did not join")
		}
		select {
		case err = <-result:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("resolver result: %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("resolver worker did not return")
		}

		found := false
		for _, a := range observer.snapshot() {
			if a.Phase == controlbudget.PhaseCancel && a.Class == "canceled" {
				found = true
			}
		}
		if !found {
			t.Fatal("missing cancellation receipt")
		}
		if _, err = d.Dial("tcp", "127.0.0.1:1"); !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled admission allowed a new dial: %v", err)
		}
	})
	t.Run("proxy", func(t *testing.T) {
		entered := make(chan struct{})
		address, done := budgetPeer(t, func(c net.Conn) {
			br := bufio.NewReader(c)
			if _, e := http.ReadRequest(br); e != nil {
				return
			}
			close(entered)
			_, _ = io.Copy(io.Discard, br)
		})
		opts, err := OptionsWithin(fakeEnv(map[string]string{"HTTPS_PROXY": "http://" + address}), dialDeadline(t, time.Second), &dialObserver{})
		if err != nil {
			t.Fatal(err)
		}
		d := applyToProbe(t, opts).CustomDialer
		result := make(chan error, 1)
		go func() {
			c, e := d.Dial("tcp", "cancel.invalid:4222")
			if c != nil {
				_ = c.Close()
			}
			result <- e
		}()
		<-entered
		started := time.Now()
		err = FinishWithin(opts, context.Canceled)
		if !errors.Is(err, context.Canceled) || time.Since(started) > 350*time.Millisecond {
			t.Fatalf("proxy cancellation failed to join: %v", err)
		}
		select {
		case err = <-result:
			if err == nil {
				t.Fatal("canceled proxy succeeded")
			}
		case <-time.After(time.Second):
			t.Fatal("cancellation returned before proxy worker joined")
		}
		awaitPeer(t, done)
	})
}

func exerciseSecureAdmission(t *testing.T) {
	t.Helper()
	certServer := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	cert := certServer.TLS.Certificates[0]
	// origin: hot-update S6 FIX-CTL / existing TLS pairing gate
	// Successful TLS admission verifies the actual fixture certificate, while
	// stalled peers below never complete a handshake and need no bypass.
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(leaf)
	certServer.Close()
	for _, scheme := range []string{"tls", "ws", "wss"} {
		t.Run(scheme, func(t *testing.T) {
			cfg := &natsserver.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true}
			if scheme == "tls" {
				cfg.TLSConfig = &tls.Config{Certificates: []tls.Certificate{cert}}
				cfg.TLSHandshakeFirst = true
			}
			if scheme == "ws" || scheme == "wss" {
				cfg.Websocket = natsserver.WebsocketOpts{Host: "127.0.0.1", Port: -1, NoTLS: scheme == "ws"}
				if scheme == "wss" {
					cfg.Websocket.TLSConfig = &tls.Config{Certificates: []tls.Certificate{cert}}
				}
			}
			server, err := natsserver.NewServer(cfg)
			if err != nil {
				t.Fatal(err)
			}
			go server.Start()
			defer func() { server.Shutdown(); server.WaitForShutdown() }()
			if !server.ReadyForConnections(time.Second) {
				t.Fatal("NATS peer did not start")
			}
			target := scheme + "://" + server.Addr().String()
			if scheme == "ws" || scheme == "wss" {
				target = server.WebsocketURL()
			}
			observer := &dialObserver{}
			remaining := 200 * time.Millisecond
			opts, err := OptionsWithin(fakeEnv(nil), dialDeadline(t, remaining), observer)
			if err != nil {
				t.Fatal(err)
			}
			if scheme == "tls" || scheme == "wss" {
				opts = append([]nats.Option{nats.Secure(&tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12})}, opts...)
			}
			if scheme == "tls" {
				opts = append([]nats.Option{nats.TLSHandshakeFirst()}, opts...)
			}
			nc, err := nats.Connect(target, opts...)
			err = FinishWithin(opts, err)
			if err != nil {
				t.Fatal(err)
			}
			defer nc.Close()
			time.Sleep(remaining + 30*time.Millisecond)
			if err := nc.FlushTimeout(time.Second); err != nil {
				t.Fatalf("%s admission deadline killed live connection: %v", scheme, err)
			}
			phases := map[controlbudget.AttemptPhase]bool{}
			for _, a := range observer.snapshot() {
				if a.Class != "ok" || !a.Sent {
					t.Fatalf("invalid successful handshake scope: %+v", a)
				}
				phases[a.Phase] = true
			}
			if !phases[controlbudget.PhaseConnect] || ((scheme == "tls" || scheme == "wss") && !phases[controlbudget.PhaseTLS]) || ((scheme == "ws" || scheme == "wss") && !phases[controlbudget.PhaseWS]) {
				t.Fatalf("missing secure handshake scopes: %+v", phases)
			}
		})
	}
}
