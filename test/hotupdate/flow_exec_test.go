//go:build linux && hotupdate_integration

package hotupdate_test

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/broker"
	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"golang.org/x/sys/unix"
)

const (
	flowRecordBytes      = 16 << 10
	flowRecordCount      = 512
	flowBusinessDeadline = upgradebudget.LiveFlowStall
)

// The expected stream is computed from the application handshake and declared
// final sequence, independently of the transport codec and the sender's ledger.
// Every byte includes direction/sequence information: a duplicate prefix cannot
// hide behind a matching final byte count or a hash of the source buffer.
func flowOracleRecord(nonce [16]byte, direction byte, seq uint64) []byte {
	b := make([]byte, flowRecordBytes)
	copy(b, nonce[:])
	b[16] = direction
	binary.BigEndian.PutUint64(b[17:25], seq)
	for i := 25; i < len(b); i++ {
		b[i] = byte(uint64(i)*31+seq*17+uint64(direction)) ^ nonce[i%len(nonce)]
	}
	return b
}

type flowWireReceipt struct {
	Records uint64
	Bytes   uint64
	Digest  [32]byte
	EOF     bool
	MaxGap  time.Duration
}

func flowReadLedger(conn *net.TCPConn, nonce [16]byte, direction byte, count uint64, progress *atomic.Uint64) (flowWireReceipt, error) {
	return flowReadLedgerFrom(conn, nonce, direction, 0, count, progress)
}

func flowReadLedgerFrom(conn *net.TCPConn, nonce [16]byte, direction byte, first, count uint64, progress *atomic.Uint64) (flowWireReceipt, error) {
	var receipt flowWireReceipt
	h := sha256.New()
	b := make([]byte, flowRecordBytes)
	last := time.Now()
	for seq := first; seq-first < count; seq++ {
		if err := conn.SetReadDeadline(time.Now().Add(flowBusinessDeadline)); err != nil {
			return receipt, err
		}
		n, err := io.ReadFull(conn, b)
		if err != nil {
			return receipt, fmt.Errorf("final sequence missing: direction=%d sequence=%d bytes=%d: %w", direction, seq, n, err)
		}
		if !bytes.Equal(b, flowOracleRecord(nonce, direction, seq)) {
			return receipt, fmt.Errorf("byte ledger mismatch: direction=%d sequence=%d", direction, seq)
		}
		gap := time.Since(last)
		if gap > receipt.MaxGap {
			receipt.MaxGap = gap
		}
		last = time.Now()
		_, _ = h.Write(b)
		receipt.Records++
		receipt.Bytes += uint64(len(b))
		if progress != nil {
			progress.Store(receipt.Records)
		}
	}
	if err := conn.SetReadDeadline(time.Now().Add(flowBusinessDeadline)); err != nil {
		return receipt, err
	}
	n, err := conn.Read(b[:1])
	// A timeout after the last required byte is trailing silence, not EOF.
	if n != 0 || err != io.EOF {
		return receipt, fmt.Errorf("exact final EOF missing: bytes=%d error=%v", n, err)
	}
	receipt.EOF = true
	copy(receipt.Digest[:], h.Sum(nil))
	return receipt, nil
}

func flowWriteBytes(conn *net.TCPConn, b []byte) error {
	for len(b) != 0 {
		if err := conn.SetWriteDeadline(time.Now().Add(flowBusinessDeadline)); err != nil {
			return err
		}
		n, err := conn.Write(b)
		b = b[n:]
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrNoProgress
		}
	}
	return nil
}

type flowTargetListener struct {
	listener    net.Listener
	accepted    chan *net.TCPConn
	count       atomic.Int64
	mu          sync.Mutex
	connections []*net.TCPConn
	done        chan struct{}
}

func flowListenTarget(t *testing.T) *flowTargetListener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f := &flowTargetListener{listener: ln, accepted: make(chan *net.TCPConn, 32), done: make(chan struct{})}
	go func() {
		defer close(f.done)
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			tcp := conn.(*net.TCPConn)
			f.count.Add(1)
			f.mu.Lock()
			f.connections = append(f.connections, tcp)
			f.mu.Unlock()
			select {
			case f.accepted <- tcp:
			default:
				_ = tcp.Close()
			}
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		<-f.done
		f.mu.Lock()
		defer f.mu.Unlock()
		for _, conn := range f.connections {
			_ = conn.Close()
		}
	})
	return f
}

func flowTCPPort(addr net.Addr) int { return addr.(*net.TCPAddr).Port }

type flowBusinessPair struct {
	public, target *net.TCPConn
	nonce          [16]byte
}

func flowConnectPair(t *testing.T, target *flowTargetListener, publicPort int) flowBusinessPair {
	t.Helper()
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", publicPort), flowBusinessDeadline)
	if err != nil {
		t.Fatal(err)
	}
	p := flowBusinessPair{public: conn.(*net.TCPConn)}
	t.Cleanup(func() { _ = p.public.Close() })
	select {
	case p.target = <-target.accepted:
	case <-time.After(flowBusinessDeadline):
		t.Fatal("original target connection was not accepted")
	}
	if _, err := rand.Read(p.nonce[:]); err != nil {
		t.Fatal(err)
	}
	if err := flowWriteBytes(p.target, p.nonce[:]); err != nil {
		t.Fatal(err)
	}
	var got [16]byte
	_ = p.public.SetReadDeadline(time.Now().Add(flowBusinessDeadline))
	if _, err := io.ReadFull(p.public, got[:]); err != nil || got != p.nonce {
		t.Fatalf("application nonce handshake: %v", err)
	}
	return p
}

type flowStreamResult struct {
	receipt flowWireReceipt
	err     error
}
type flowDuplexStream struct {
	progress [2]atomic.Uint64
	results  [4]chan flowStreamResult
}

// Independent readers check both directions while writers continue during the
// upgrade. A final gate leaves a declared tail after activation, so completing
// the stream before the cut cannot accidentally count as continuity.
func flowStartDuplex(t *testing.T, p flowBusinessPair, finish <-chan struct{}, first uint64, paced bool, pacing ...time.Duration) *flowDuplexStream {
	t.Helper()
	interval := 10 * time.Millisecond
	if len(pacing) != 0 {
		interval = pacing[0]
	}
	s := &flowDuplexStream{}
	for i := range s.results {
		s.results[i] = make(chan flowStreamResult, 1)
	}
	for direction := 0; direction < 2; direction++ {
		source, sink := p.public, p.target
		if direction == 1 {
			source, sink = p.target, p.public
		}
		go func() {
			var err error
			for seq := uint64(0); seq < flowRecordCount; seq++ {
				if seq == flowRecordCount-32 {
					select {
					case <-finish:
					case <-t.Context().Done():
						err = t.Context().Err()
					}
					if err != nil {
						break
					}
				}
				err = flowWriteBytes(source, flowOracleRecord(p.nonce, byte(direction), first+seq))
				if err != nil {
					break
				}
				if paced {
					select {
					case <-time.After(interval):
					case <-t.Context().Done():
						err = t.Context().Err()
					}
					if err != nil {
						break
					}
				}
			}
			if err == nil {
				err = source.CloseWrite()
			}
			s.results[direction*2] <- flowStreamResult{err: err}
		}()
		go func() {
			receipt, err := flowReadLedgerFrom(sink, p.nonce, byte(direction), first, flowRecordCount, &s.progress[direction])
			s.results[direction*2+1] <- flowStreamResult{receipt: receipt, err: err}
		}()
	}
	// Join before leak assertions, including failure paths. Closing a fixture here
	// is cleanup; successful product retirement is asserted separately while alive.
	t.Cleanup(func() {
		_ = p.public.Close()
		_ = p.target.Close()
		for _, result := range s.results {
			select {
			case <-result:
			case <-time.After(flowBusinessDeadline):
				t.Error("business oracle worker did not join")
			}
		}
	})
	return s
}

func flowAwaitProgress(t *testing.T, s *flowDuplexStream, count uint64) {
	t.Helper()
	if !testharness.WaitFor(t, flowBusinessDeadline, 10*time.Millisecond, func() bool { return s.progress[0].Load() >= count && s.progress[1].Load() >= count }) {
		t.Fatalf("duplex nonempty checkpoint missing: received=%d/%d want>=%d", s.progress[0].Load(), s.progress[1].Load(), count)
	}
}

func flowFinishDuplex(t *testing.T, s *flowDuplexStream) {
	t.Helper()
	for i, result := range s.results {
		select {
		case r := <-result:
			// Put the result back: cleanup observes the same completed worker.
			result <- r
			if r.err != nil {
				t.Fatalf("original flow worker %d: %v", i, r.err)
			}
			if i%2 == 1 && (r.receipt.Records != flowRecordCount || r.receipt.Bytes != flowRecordCount*flowRecordBytes || !r.receipt.EOF || r.receipt.MaxGap > flowBusinessDeadline) {
				t.Fatalf("incomplete duplex final receipt: %+v", r.receipt)
			}
			if i%2 == 1 {
				t.Logf("direction=%d records=%d bytes=%d sha256=%x max_gap=%s exact_eof=%t", i/2, r.receipt.Records, r.receipt.Bytes, r.receipt.Digest, r.receipt.MaxGap, r.receipt.EOF)
			}
		case <-time.After(20 * time.Second):
			t.Fatal("original duplex flow did not reach its declared final sequence")
		}
	}
}

type flowArtifacts struct {
	original, candidate  string
	tar                  []byte
	tarSHA, candidateSHA string
}

func flowBuildArtifacts(t *testing.T) flowArtifacts {
	t.Helper()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	a := flowArtifacts{original: filepath.Join(t.TempDir(), "tether"), candidate: filepath.Join(t.TempDir(), "tether")}
	for index, out := range []string{a.original, a.candidate} {
		// Both real roles are instrumented. GOFLAGS is inherited so production
		// overlays reach the nested build, rather than only changing this harness.
		cmd := exec.CommandContext(t.Context(), "go", "build", "-race", "-trimpath", "-ldflags", fmt.Sprintf("-X github.com/LinZiyang666/tether/internal/proto.ReleaseVersion=v0.0.0-flow-%d", index), "-o", out, "./cmd/tether")
		cleanup := hotUpdateOwnProcessGroup(cmd)
		defer cleanup()
		cmd.Dir = root
		if output, err := testharness.CombinedOutput(cmd); err != nil {
			t.Fatalf("build real flow artifact: %v\n%s", err, output)
		}
	}
	body, err := os.ReadFile(a.candidate)
	if err != nil {
		t.Fatal(err)
	}
	a.candidateSHA = fmt.Sprintf("%x", sha256.Sum256(body))
	if flowFileSHA(t, a.original) == a.candidateSHA {
		t.Fatal("two real artifacts have the same SHA")
	}
	var b bytes.Buffer
	gz, err := gzip.NewWriterLevel(&b, gzip.NoCompression)
	if err != nil {
		t.Fatal(err)
	}
	tw := tar.NewWriter(gz)
	if err := tw.WriteHeader(&tar.Header{Name: "tether", Mode: 0o755, Size: int64(len(body))}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write(body); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	a.tar = b.Bytes()
	a.tarSHA = fmt.Sprintf("%x", sha256.Sum256(a.tar))
	return a
}

func flowFileSHA(t *testing.T, path string) string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

func flowSandboxEnvironment(home string) []string {
	env := handoff.CleanEnvironment(os.Environ())
	out := make([]string, 0, len(env)+2)
	for _, entry := range env {
		if strings.HasPrefix(entry, "TETHER_") || strings.HasPrefix(entry, "_TETHER_") || strings.HasPrefix(entry, "GORACE=") {
			continue
		}
		out = append(out, entry)
	}
	return append(out, "TETHER_HOME="+home, "GORACE=atexit_sleep_ms=0")
}

func flowFreeAddress(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}

type flowProductStack struct {
	db                                  *sql.DB
	ns                                  *natsserver.Server
	binary, home, agentBinary, url, log string
	agent                               *exec.Cmd
	agentExited                         chan struct{}
	artifactURL                         string
}

func flowStartProduct(t *testing.T, a flowArtifacts) *flowProductStack {
	t.Helper()
	s := &flowProductStack{binary: a.original, home: t.TempDir(), agentBinary: filepath.Join(t.TempDir(), "tether"), log: filepath.Join(t.TempDir(), "agent.log")}
	body, err := os.ReadFile(a.original)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(s.agentBinary, body, 0o755); err != nil {
		t.Fatal(err)
	}
	artifact := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(a.tar) }))
	t.Cleanup(artifact.Close)
	s.artifactURL = artifact.URL
	s.db, err = storage.Open(filepath.Join(t.TempDir(), "stack.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.db.Close() })
	s.ns, err = natsserver.NewServer(&natsserver.Options{Host: "127.0.0.1", Port: -1, AlwaysEnableNonce: true, NoSigs: true})
	if err != nil {
		t.Fatal(err)
	}
	go s.ns.Start()
	t.Cleanup(func() { s.ns.Shutdown(); s.ns.WaitForShutdown() })
	if !s.ns.ReadyForConnections(5 * time.Second) {
		t.Fatal("private NATS not ready")
	}
	s.url = s.ns.ClientURL()
	identity, err := cli.EnsureIdentity(s.home)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(s.home, "current_session"), []byte("flow-lab\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	pinHash, err := auth.HashPIN("flow-lab-pin")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.Create(s.db, "flow-lab", "flow-lab", identity.Fingerprint, pinHash, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	tunnelAddr := flowFreeAddress(t)
	// Every expose explicitly requests an OS-selected free port. The broad
	// band permits bounded bind retry before business traffic starts.
	b, err := broker.New(broker.Config{NATSURL: s.url, DB: s.db, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), PublicHost: "127.0.0.1", TunnelControlAddr: tunnelAddr, TunnelPublicHost: "127.0.0.1", PortBandLow: 1024, PortBandHigh: 65535, UpgradeURLAllowlist: []string{artifact.URL + "/"}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	brokerDone := make(chan error, 1)
	go func() { brokerDone <- b.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-brokerDone:
			if err != nil && err != context.Canceled {
				t.Errorf("private broker exit: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Error("private broker did not join")
		}
		// Drain is asynchronous; close NATS only after its actual broker
		// connection has left, avoiding teardown-created flush goroutines.
		if !testharness.WaitFor(t, 5*time.Second, 20*time.Millisecond, func() bool { return !flowBrokerSubscription(s.ns, "") }) {
			t.Error("private broker did not drain")
		}
	})
	if !testharness.WaitFor(t, 10*time.Second, 20*time.Millisecond, func() bool {
		return flowBrokerSubscription(s.ns, ".node-upgrade.req") || flowBrokerSubscription(s.ns, ".expose.req")
	}) {
		t.Fatal("private broker did not subscribe")
	}
	out, err := os.Create(filepath.Join(t.TempDir(), "agent-output"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = out.Close() })
	s.agent = exec.Command(s.agentBinary, "agent", "--session", "flow-lab", "--nid", "flow-agent", "--nats-url", s.url, "--tunnel-addr", tunnelAddr, "--upgrade-url-allow", artifact.URL+"/", "--log-file", s.log, "--log-level", "info")
	s.agent.Dir = filepath.Dir(s.agentBinary)
	s.agent.Env = flowRegisteredEnvironment(t, s)
	s.agent.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
	s.agent.Stdout, s.agent.Stderr = out, out
	waitProcess, err := testharness.StartProcess(s.agent)
	if err != nil {
		t.Fatal(err)
	}
	s.agentExited = make(chan struct{})
	go func() { _ = waitProcess(); close(s.agentExited) }()
	t.Cleanup(func() {
		_ = s.agent.Process.Signal(syscall.SIGTERM)
		select {
		case <-s.agentExited:
		case <-time.After(10 * time.Second):
			_ = syscall.Kill(-s.agent.Process.Pid, syscall.SIGKILL)
			<-s.agentExited
		}
		_ = syscall.Kill(-s.agent.Process.Pid, syscall.SIGKILL)
		if t.Failed() {
			log, _ := os.ReadFile(s.log)
			t.Logf("private agent log:\n%s", log)
			output, _ := os.ReadFile(out.Name())
			t.Logf("private agent output:\n%s", output)
			boot, _ := os.ReadFile(filepath.Join(filepath.Dir(s.log), "agent.boot.err"))
			t.Logf("private agent boot error:\n%s", boot)
		}
	})
	testharness.WaitNodeOnline(t, s.db, "flow-lab", "flow-agent", 20*time.Second)
	if !testharness.WaitFor(t, 10*time.Second, 20*time.Millisecond, func() bool {
		cs, err := s.ns.Connz(&natsserver.ConnzOptions{State: natsserver.ConnOpen, Subscriptions: true})
		if err != nil {
			return false
		}
		for _, conn := range cs.Conns {
			for _, sub := range conn.Subs {
				if sub == proto.SubjCmdForwarded("flow-lab", "flow-agent", "*") {
					return true
				}
			}
		}
		return false
	}) {
		t.Fatal("real agent did not install its command intake")
	}
	return s
}

func flowBrokerSubscription(ns *natsserver.Server, needle string) bool {
	cs, err := ns.Connz(&natsserver.ConnzOptions{State: natsserver.ConnOpen, Subscriptions: true})
	if err != nil {
		return false
	}
	for _, conn := range cs.Conns {
		if conn.Name == "tetherd" {
			if needle == "" {
				return true
			}
			for _, sub := range conn.Subs {
				if strings.Contains(sub, needle) {
					return true
				}
			}
		}
	}
	return false
}

func flowProductCommand(t *testing.T, s *flowProductStack, args ...string) ([]byte, error) {
	t.Helper()
	return flowProductCommandWithin(t, s, upgradebudget.ComponentHandover+30*time.Second, args...)
}

func flowProductCommandWithin(t *testing.T, s *flowProductStack, budget time.Duration, args ...string) ([]byte, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), budget)
	defer cancel()
	// A dead agent cannot complete this fixture's upgrade. Stop the observer
	// immediately so a real bootstrap refusal is not hidden behind a 150-second
	// CLI wait in every exercise round. The actual upgrade deadline is unchanged.
	// origin: hot-update S5 plan §7.1 / §8 T39 / T41
	go func() {
		select {
		case <-s.agentExited:
			cancel()
		case <-ctx.Done():
		}
	}()
	full := append([]string{args[0], "--home", s.home, "--nats-url", s.url}, args[1:]...)
	cmd := exec.CommandContext(ctx, s.binary, full...)
	cleanup := hotUpdateOwnProcessGroup(cmd)
	defer cleanup()
	cmd.Env = flowSandboxEnvironment(s.home)
	return testharness.CombinedOutput(cmd)
}

// origin: hot-update S5 internal review HOTUPDATE-T39/T41
func TestNodeUpgradeWaitObservesRealRegister(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	a := flowBuildArtifacts(t)
	for round := 0; round < 5; round++ {
		t.Run(strconv.Itoa(round), func(t *testing.T) { exerciseUpgradeRegisterObserver(t, a) })
	}
	testharness.AssertNoGoroutineLeak(t, "real register observer x5", beforeG)
	testharness.AssertNoFDLeak(t, "real register observer x5", beforeFD, 0)
}

func exerciseUpgradeRegisterObserver(t *testing.T, a flowArtifacts) {
	t.Helper()
	s := flowStartProduct(t, a)
	target := flowListenTarget(t)
	p := flowConnectPair(t, target, flowProductExpose(t, s, target))
	flowExchangeRecord(t, p, 0, 0)
	view, err := flowTransactionUpgrade(t, s, a)
	if err != nil {
		var release, status string
		rowErr := s.db.QueryRow(`SELECT release_version, status FROM nodes WHERE sid=? AND nid=?`, "flow-lab", "flow-agent").Scan(&release, &status)
		t.Fatalf("real upgrade observer: %v; registered release=%s status=%s row=%v", err, release, status, rowErr)
	}
	if view.Observation.Current.Phase != "completed" || flowFileSHA(t, fmt.Sprintf("/proc/%d/exe", s.agent.Process.Pid)) != a.candidateSHA {
		t.Fatal("observer lacks a real post-exec registration receipt")
	}
	flowRequireUpgradedRegister(t, s)
	flowExchangeRecord(t, p, 1, 0)
}

// The observer must fail promptly when the original product process exits;
// otherwise one bootstrap refusal consumes the full handover budget per round.
// This isolates the observer's cancellation, without claiming a product upgrade.
// origin: hot-update S5 plan §7.1 / §8 T39 / T41
func TestFlowProductCommandStopsWhenAgentExits(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(strconv.Itoa(round), func(t *testing.T) { exerciseFlowCommandAgentExit(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "flow command observer cancellation", beforeG)
	testharness.AssertNoFDLeak(t, "flow command observer cancellation", beforeFD, 0)
}

func exerciseFlowCommandAgentExit(t *testing.T) {
	t.Helper()
	home := t.TempDir()
	if err := syscall.Mkfifo(filepath.Join(home, "hold"), 0o600); err != nil {
		t.Fatal(err)
	}
	observer := filepath.Join(home, "observer")
	// Builtins keep the blocking FIFO read in the direct child that CommandContext
	// owns. A sleep subprocess would obscure whether the observer actually joined.
	if err := os.WriteFile(observer, []byte("#!/bin/sh\n: > \"$3/started\"\nread -r reply < \"$3/hold\"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	s := &flowProductStack{binary: observer, home: home, agentExited: make(chan struct{})}
	done := make(chan error, 1)
	go func() { _, err := flowProductCommand(t, s, "node"); done <- err }()
	joined := false
	t.Cleanup(func() {
		if !joined {
			select {
			case <-done:
			case <-time.After(flowBusinessDeadline):
				t.Error("command observer did not join after cancellation")
			}
		}
	})
	if !testharness.WaitFor(t, flowBusinessDeadline, time.Millisecond, func() bool {
		_, err := os.Stat(filepath.Join(home, "started"))
		return err == nil
	}) {
		t.Fatal("command observer never entered its bounded fixture")
	}
	close(s.agentExited)
	select {
	case err := <-done:
		joined = true
		if err == nil {
			t.Fatal("agent exit produced a successful command result")
		}
	case <-time.After(time.Second):
		t.Fatal("command observer kept waiting after the agent exited")
	}
}

func flowProductExpose(t *testing.T, s *flowProductStack, target *flowTargetListener) int {
	t.Helper()
	for attempt := 0; attempt < 8; attempt++ {
		// The target is a real occupied port on this loopback host. Force one
		// collision before creating the original cohort, proving that setup's
		// retry handles the actual v1 bind refusal without retrying revocation.
		// origin: hot-update S6 plan §6.3 / T28 / TESTFIX-2
		portText := strconv.Itoa(flowTCPPort(target.listener.Addr()))
		if attempt != 0 {
			_, value, err := net.SplitHostPort(flowFreeAddress(t))
			if err != nil {
				t.Fatal(err)
			}
			portText = value
		}
		beforeLog, err := os.ReadFile(s.log)
		if err != nil {
			t.Fatal(err)
		}
		out, err := flowProductCommand(t, s, "expose", "flow-agent", "--name", "flow-target", "--local", strconv.Itoa(flowTCPPort(target.listener.Addr())), "--remote-port", portText)
		if err == nil {
			if attempt == 0 {
				t.Fatal("expose claimed an already occupied public port")
			}
			break
		}
		log, readErr := os.ReadFile(s.log)
		if readErr != nil || len(log) < len(beforeLog) {
			t.Fatalf("read current expose failure: %v", readErr)
		}
		// CLI hints deliberately omit the agent's Error detail. Only the new
		// log suffix for this exact port can certify a bind collision; a stale
		// refusal or generic EX_USAGE must never authorize another expose.
		if !flowExposeBindFailure(string(out), string(log[len(beforeLog):]), portText) || attempt == 7 {
			t.Fatalf("real expose: %v\n%s\n%s", err, out, log[len(beforeLog):])
		}
	}
	var port int
	if err := s.db.QueryRow(`SELECT port FROM port_allocations WHERE sid=? AND nid=? AND name=? AND state='ALLOCATED'`, "flow-lab", "flow-agent", "flow-target").Scan(&port); err != nil {
		t.Fatal(err)
	}
	return port
}

func flowExposeBindFailure(output, attemptLog, port string) bool {
	if !strings.Contains(output, "(agent_rejected:frpc_failed)") {
		return false
	}
	for _, line := range strings.Split(attemptLog, "\n") {
		if strings.Contains(line, `msg="agent: ExposeAdapter.AddProxy"`) &&
			strings.Contains(line, `err="tunnel adapter AddProxy: tunnel client: Open: tunnel: broker denied REGISTER: public_port_bind_failed"`) &&
			strings.HasSuffix(line, " name=flow-target port="+port) {
			return true
		}
	}
	return false
}

// origin: hot-update S6 plan §6.3 / T28 / TESTFIX-2
func TestExposeBindFailureClassificationRejectsOtherFailures(t *testing.T) {
	output := "error: expose failed: local proxy failed (agent_rejected:frpc_failed)"
	line := `msg="agent: ExposeAdapter.AddProxy" err="tunnel adapter AddProxy: tunnel client: Open: tunnel: broker denied REGISTER: public_port_bind_failed" name=flow-target port=12345`
	if !flowExposeBindFailure(output, line, "12345") {
		t.Fatal("current bind refusal was not recognized")
	}
	for _, tc := range []struct{ output, log, port string }{
		{output, line, "1234"},
		{output, strings.ReplaceAll(line, "flow-target", "another-target"), "12345"},
		{output, strings.ReplaceAll(line, "public_port_bind_failed", "session_closed"), "12345"},
		{output, "", "12345"},
		{"error: unknown flag", line, "12345"},
	} {
		if flowExposeBindFailure(tc.output, tc.log, tc.port) {
			t.Fatal("unrelated expose failure was retried as a bind collision")
		}
	}
}

func flowProcessTicks(t *testing.T, pid int) string {
	t.Helper()
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		t.Fatal(err)
	}
	end := strings.LastIndexByte(string(b), ')')
	if end < 0 {
		t.Fatal("malformed process identity")
	}
	fields := strings.Fields(string(b[end+1:]))
	if len(fields) < 20 {
		t.Fatal("missing process start ticks")
	}
	return fields[19]
}

type flowSocketIdentity struct {
	Dev, Ino uint64
	Type     uint32
}

func flowTCPTableAddress(addr net.Addr) string {
	a := addr.(*net.TCPAddr)
	ip := a.IP.To4()
	return fmt.Sprintf("%02X%02X%02X%02X:%04X", ip[3], ip[2], ip[1], ip[0], a.Port)
}

// Find the product descriptor using the independently accepted target tuple.
// Reading proc and fstat never duplicates the OFD or changes O_NONBLOCK.
func flowProductSocketIdentity(t *testing.T, pid int, local, remote net.Addr) flowSocketIdentity {
	t.Helper()
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/net/tcp", pid))
	if err != nil {
		t.Fatal(err)
	}
	var inode string
	for _, line := range strings.Split(string(b), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 10 && fields[1] == flowTCPTableAddress(local) && fields[2] == flowTCPTableAddress(remote) {
			inode = fields[9]
		}
	}
	if inode == "" || inode == "0" {
		t.Fatal("original product TCP tuple has no kernel inode")
	}
	dir := fmt.Sprintf("/proc/%d/fd", pid)
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		path := filepath.Join(dir, entry.Name())
		link, err := os.Readlink(path)
		if err != nil || link != "socket:["+inode+"]" {
			continue
		}
		var stat unix.Stat_t
		if err := unix.Stat(path, &stat); err != nil {
			t.Fatal(err)
		}
		return flowSocketIdentity{Dev: uint64(stat.Dev), Ino: stat.Ino, Type: stat.Mode & unix.S_IFMT}
	}
	t.Fatal("original socket inode is absent from the product owner")
	return flowSocketIdentity{}
}

func flowCheckOriginalSocket(t *testing.T, pid int, local, remote net.Addr, before flowSocketIdentity) {
	t.Helper()
	after := flowProductSocketIdentity(t, pid, local, remote)
	if err := flowValidateSocketIdentity(before, after); err != nil {
		t.Fatal(err)
	}
}

func flowValidateSocketIdentity(before, after flowSocketIdentity) error {
	if before != after || after.Type != unix.S_IFSOCK {
		return fmt.Errorf("original business OFD changed: before=%+v after=%+v", before, after)
	}
	return nil
}

func flowValidateRunningSHA(before, after, candidate string) error {
	if after != candidate || after == before {
		return fmt.Errorf("running SHA did not change to candidate: before=%s after=%s candidate=%s", before, after, candidate)
	}
	return nil
}

func flowProductFDs(t *testing.T, pid int) []string {
	t.Helper()
	dir := fmt.Sprintf("/proc/%d/fd", pid)
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var links []string
	for _, entry := range entries {
		if link, err := os.Readlink(filepath.Join(dir, entry.Name())); err == nil {
			links = append(links, link)
		}
	}
	return links
}

func flowCountLinks(links []string, needle string) int {
	n := 0
	for _, link := range links {
		if strings.Contains(link, needle) {
			n++
		}
	}
	return n
}

// S8's current View replaces the legacy upgradeMarker projection here.
// origin: S10 internal review TAGGED-SUITE-SWEEP-A F2; hot-update S8 plan §4.2 / §5.2
func flowUpgradeProduct(t *testing.T, s *flowProductStack, a flowArtifacts) {
	t.Helper()
	pid := s.agent.Process.Pid
	ticks := flowProcessTicks(t, pid)
	beforeSHA := flowFileSHA(t, fmt.Sprintf("/proc/%d/exe", pid))
	// Upgrade uses strict ONLINE admission. The real heartbeat is the phase
	// anchor; lowering product StaleAfter would create a harness-only failure.
	if !testharness.WaitFor(t, 10*time.Second, 20*time.Millisecond, func() bool {
		var last string
		if err := s.db.QueryRow(`SELECT COALESCE(last_heartbeat_at,'') FROM nodes WHERE sid=? AND nid=?`, "flow-lab", "flow-agent").Scan(&last); err != nil {
			return false
		}
		hb, err := time.Parse("2006-01-02 15:04:05.999999999 -0700 MST", last)
		return err == nil && time.Since(hb) < time.Second
	}) {
		t.Fatal("real heartbeat phase anchor missing")
	}
	view, err := flowTransactionUpgrade(t, s, a)
	if err != nil {
		t.Fatalf("real node upgrade: %v", err)
	}
	if got := flowProcessTicks(t, pid); got != ticks {
		t.Fatalf("agent process identity changed: ticks=%s/%s", ticks, got)
	}
	afterSHA := flowFileSHA(t, fmt.Sprintf("/proc/%d/exe", pid))
	// origin: hot-update S5 plan §8 / T39. A disk replacement without exec is
	// insufficient even if the current View claims completed and the sockets work.
	if err := flowValidateRunningSHA(beforeSHA, afterSHA, a.candidateSHA); err != nil {
		t.Fatal(err)
	}
	// Root stage keeps the canonical dispatcher immutable. The selector and
	// kernel executing SHA must independently identify the staged candidate.
	if got := flowFileSHA(t, s.agentBinary); got != beforeSHA || view.Observation.EffectiveSHA != a.candidateSHA {
		t.Fatalf("canonical SHA=%s source=%s effective=%s candidate=%s", got, beforeSHA, view.Observation.EffectiveSHA, a.candidateSHA)
	}
	// Imported task counts are not flow counts. The
	// independent socket/byte receipts below prove retained flow ownership.
	// A cold owner is generation one; its first handoff must advance to two.
	// origin: hot-update S5 plan §4.4 / §8 T39 / T41
	if view.Observation.Current.Handle.Generation != 2 || view.Observation.Current.CutStage != "released" {
		t.Fatalf("incomplete actual activation receipt: %+v", view.Observation.Current)
	}
	log, err := os.ReadFile(s.log)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(log), "agent: image final cut") != 1 || strings.Count(string(log), "agent: image activated") != 1 {
		t.Fatal("expected exactly one product cut and activation")
	}
	freezeWindowMS := flowActivationWindow(t, string(log))
	links := flowProductFDs(t, pid)
	if flowCountLinks(links, "memfd:tether-handoff") != 0 {
		t.Fatal("handoff manifest still owned after activation")
	}
	// The 500ms window is calibration under S4 L4, while per-flow I/O keeps
	// the five-second hard deadline. Always report the full observed maximum.
	t.Logf("real product pid=%d ticks=%s running_sha=%s -> %s freeze_window_ms=%d exceeds_500ms=%t", pid, ticks, beforeSHA, afterSHA, freezeWindowMS, freezeWindowMS > upgradebudget.LocalFreezeWindow.Milliseconds())
}

func flowRequireProductRetired(t *testing.T, s *flowProductStack, identities, brokerIdentities []flowSocketIdentity, beforeReplay int) {
	t.Helper()
	out, err := flowProductCommand(t, s, "expose", "rm", "flow-agent", "--name", "flow-target")
	if err != nil {
		t.Fatalf("remove completed expose: %v\n%s", err, out)
	}
	if !testharness.WaitFor(t, flowBusinessDeadline, 20*time.Millisecond, func() bool {
		links := flowProductFDs(t, s.agent.Process.Pid)
		// All non-replay cut memfds must be gone too, regardless of the leaf
		// owner's chosen memfd label. A guessed state label would miss leaks.
		if flowCountLinks(links, "memfd:tether-replay") != beforeReplay || flowCountLinks(links, "memfd:") != beforeReplay {
			return false
		}
		for _, identity := range identities {
			if flowCountLinks(links, fmt.Sprintf("socket:[%d]", identity.Ino)) != 0 {
				return false
			}
		}
		brokerLinks := flowProductFDs(t, os.Getpid())
		for _, identity := range brokerIdentities {
			if flowCountLinks(brokerLinks, fmt.Sprintf("socket:[%d]", identity.Ino)) != 0 {
				return false
			}
		}
		return true
	}) {
		t.Fatal("product did not retire original flow sockets/replay while agent remained alive")
	}
	_ = flowProcessTicks(t, s.agent.Process.Pid)
}

// origin: hot-update S5 plan §8 / T39
// origin: hot-update S5 internal review HOTUPDATE-T39
func TestAgentSamePIDExecKeepsFourTCPFlows(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	a := flowBuildArtifacts(t)
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			exerciseAgentFourTCPFlows(t, a)
		})
	}
	testharness.AssertNoGoroutineLeak(t, "four original TCP flows across product exec", beforeG)
	testharness.AssertNoFDLeak(t, "four original TCP flows across product exec", beforeFD, 0)
}

func exerciseAgentFourTCPFlows(t *testing.T, a flowArtifacts) {
	s := flowStartProduct(t, a)
	target := flowListenTarget(t)
	port := flowProductExpose(t, s, target)
	beforeReplay := flowCountLinks(flowProductFDs(t, s.agent.Process.Pid), "memfd:tether-replay")
	finish := make(chan struct{})
	var finishOnce sync.Once
	release := func() { finishOnce.Do(func() { close(finish) }) }
	t.Cleanup(release)
	var pairs []flowBusinessPair
	var streams []*flowDuplexStream
	var agentIDs, brokerIDs []flowSocketIdentity
	for i := 0; i < 4; i++ {
		p := flowConnectPair(t, target, port)
		pairs = append(pairs, p)
		agentIDs = append(agentIDs, flowProductSocketIdentity(t, s.agent.Process.Pid, p.target.RemoteAddr(), p.target.LocalAddr()))
		brokerIDs = append(brokerIDs, flowProductSocketIdentity(t, os.Getpid(), p.public.RemoteAddr(), p.public.LocalAddr()))
		// Keep the declared tail beyond heartbeat-phase alignment, staging,
		// exec and the CLI's poll. Hitting this intentional writer gate early
		// would manufacture a five-second read stall in the fixture itself.
		// origin: hot-update S5 internal review HOTUPDATE-T39
		streams = append(streams, flowStartDuplex(t, p, finish, 0, true, 50*time.Millisecond))
	}
	for _, stream := range streams {
		flowAwaitProgress(t, stream, 8)
	}
	if target.count.Load() != 4 {
		t.Fatalf("initial original target accepts=%d want=4", target.count.Load())
	}
	var before [4][2]uint64
	for i, stream := range streams {
		before[i] = [2]uint64{stream.progress[0].Load(), stream.progress[1].Load()}
	}
	flowUpgradeProduct(t, s, a)
	for i, p := range pairs {
		flowCheckOriginalSocket(t, s.agent.Process.Pid, p.target.RemoteAddr(), p.target.LocalAddr(), agentIDs[i])
		flowCheckOriginalSocket(t, os.Getpid(), p.public.RemoteAddr(), p.public.LocalAddr(), brokerIDs[i])
		for d := 0; d < 2; d++ {
			if streams[i].progress[d].Load() <= before[i][d] {
				t.Fatalf("flow=%d direction=%d made no progress during upgrade", i, d)
			}
		}
	}
	release()
	for _, stream := range streams {
		flowFinishDuplex(t, stream)
	}
	if target.count.Load() != 4 {
		t.Fatalf("application redial: target accepts=%d want=4", target.count.Load())
	}
	flowRequireProductRetired(t, s, agentIDs, brokerIDs, beforeReplay)
}
