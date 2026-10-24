//go:build linux

package cli_e2e_test

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
)

// The relay supplies the real server's INFO but withholds its reconnect PONG.
// nats.go's real CONNECT read deadline then produces the arm C1 async timeout.
// A dropped established socket alone does not exercise that error callback.
// origin: hot-update S4 plan §7.2 / §7.3 / T35; PRODUCT-R4 arm C1
type terminalCarrierRelay struct {
	ln                                net.Listener
	upstream                          string
	mu                                sync.Mutex
	firstHandshake                    time.Time
	conns                             map[net.Conn]bool
	wg                                sync.WaitGroup
	acceptDone, stop, resume, timeout chan struct{}
	outage                            atomic.Bool
	timeoutOnce                       sync.Once
}

func startTerminalCarrierRelay(t *testing.T, upstream string) (*terminalCarrierRelay, string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	r := &terminalCarrierRelay{ln: ln, upstream: strings.TrimPrefix(upstream, "nats://"), conns: make(map[net.Conn]bool), acceptDone: make(chan struct{}), stop: make(chan struct{}), resume: make(chan struct{}), timeout: make(chan struct{})}
	go func() {
		defer close(r.acceptDone)
		for {
			client, err := ln.Accept()
			if err != nil {
				return
			}
			r.mu.Lock()
			r.conns[client] = true
			r.mu.Unlock()
			r.wg.Add(1)
			go r.serve(client)
		}
	}()
	t.Cleanup(func() {
		close(r.stop)
		_ = ln.Close()
		<-r.acceptDone
		r.mu.Lock()
		for conn := range r.conns {
			_ = conn.Close()
		}
		r.mu.Unlock()
		r.wg.Wait()
	})
	return r, "nats://" + ln.Addr().String()
}

func (r *terminalCarrierRelay) serve(client net.Conn) {
	defer r.wg.Done()
	defer func() { _ = client.Close(); r.mu.Lock(); delete(r.conns, client); r.mu.Unlock() }()
	server, err := net.DialTimeout("tcp", r.upstream, 3*time.Second)
	if err != nil {
		return
	}
	defer func() { _ = server.Close() }()
	if r.outage.Load() {
		rd := bufio.NewReader(server)
		info, err := rd.ReadBytes('\n')
		if err != nil {
			return
		}
		if _, err := client.Write(info); err != nil {
			return
		}
		cr := bufio.NewReader(client)
		for {
			_, _, fields, err := readNATSFrame(cr)
			if err != nil {
				return
			}
			if len(fields) != 0 && fields[0] == "PING" {
				break
			}
		}
		// A second CONNECT proves that the first PONG read really timed out.
		// nats.go retains the failed socket until a later reconnect/Close, so
		// waiting for EOF would observe teardown instead of the timeout itself.
		r.mu.Lock()
		if r.firstHandshake.IsZero() {
			r.firstHandshake = time.Now()
		} else if time.Since(r.firstHandshake) >= 1500*time.Millisecond {
			r.timeoutOnce.Do(func() { close(r.timeout) })
		}
		r.mu.Unlock()
		select {
		case <-r.resume:
		case <-r.stop:
		}
		return
	}
	done := make(chan struct{})
	// Hide TCP's ReaderFrom/WriterTo fast paths. Go's splice pool retains
	// kernel pipes across Copy calls; those fixture-owned cache descriptors
	// would obscure the strict FD oracle for the session's real resources.
	go func() {
		_, _ = io.Copy(struct{ io.Writer }{server}, struct{ io.Reader }{client})
		_ = server.Close()
		close(done)
	}()
	_, _ = io.Copy(struct{ io.Writer }{client}, struct{ io.Reader }{server})
	_ = client.Close()
	<-done
}

func (r *terminalCarrierRelay) cut() {
	r.outage.Store(true)
	r.mu.Lock()
	defer r.mu.Unlock()
	for conn := range r.conns {
		_ = conn.Close()
	}
}

// origin: hot-update S4 plan §7.2 / §7.3 / T35; PRODUCT-R4 arm C1
// stderr is the SAME slave as stdout. Exact remote bytes must survive a real
// carrier timeout; a final cancellation must still print AFTER termios restore.
func TestRunReliableCarrierDiagnosticsStayOutsideTerminal(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	if runIsolatedReliableTest(t) {
		return
	}
	buildTether(t)
	g0, fd0 := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		for _, recover := range []bool{true, false} {
			t.Run(fmt.Sprintf("round_%d/recover_%v", round, recover), func(t *testing.T) {
				// origin: hot-update S5 PROC-LEAK / final process census.
				testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

				s := newRealStack(t, false)
				trace := tracePTYWire(t, s)
				r, url := startTerminalCarrierRelay(t, s.url)
				f := newPTYFixture(t)
				ctl := startTerminalCtlStderr(t, s, s.bin, url, true, f.argv("duplex")...)
				f.identity(t)
				ctl.expect(t, []byte("READY\n"))
				requirePTYReliable(t, s, 1)
				r.cut()
				select {
				case <-r.timeout:
				case <-time.After(15 * time.Second):
					t.Fatal("real reconnect CONNECT did not time out")
				}
				if recover {
					r.outage.Store(false)
					close(r.resume)
					payload := ptyPayload(round)
					ctl.frame(t, payload)
					want := append([]byte("READY\n"), ptyEcho(payload)...)
					ctl.expect(t, want)
					ctl.frame(t, []byte("EXIT"))
					ctl.wait(t, 23)
					ctl.expect(t, append(want, []byte("TAIL-OUT\nTAIL-ERR\n")...))
				} else {
					if err := ctl.cmd.Process.Signal(syscall.SIGTERM); err != nil {
						t.Fatal(err)
					}
					ctl.wait(t, 74)
					if !testharnessWait(5*time.Second, func() bool { return bytes.Contains(ctl.out.bytes(), []byte("error: run:")) }) {
						t.Fatalf("final failure was hidden: %q", ctl.out.bytes())
					}
					got := ctl.out.bytes()
					if !bytes.HasPrefix(got, []byte("READY\nerror: run:")) || !bytes.HasSuffix(got, []byte("\r\n")) || bytes.Contains(got, []byte("nats async error")) || bytes.Contains(got, []byte("read tcp")) || !bytes.Contains(got, []byte("interrupted (Ctrl-C/SIGTERM)")) || !bytes.Contains(got, []byte("remote task keeps running")) {
						t.Fatalf("failure did not print clearly in restored cooked mode: %q", got)
					}
				}
				trace.requireOriginalHandle(t)
				f.once(t)
			})
		}
	}
	testharness.AssertNoGoroutineLeak(t, "PTY carrier diagnostics", g0)
	testharness.AssertNoFDLeak(t, "PTY carrier diagnostics", fd0, 0)
}
