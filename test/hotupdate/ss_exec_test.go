//go:build linux && hotupdate_integration

package hotupdate

import (
	"bufio"
	"bytes"
	"context"
	"crypto/cipher"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agent/ssproxy"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/rawsocket"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"golang.org/x/crypto/chacha20poly1305"
	"golang.org/x/crypto/hkdf"
)

const ssProbeBudget = upgradebudget.LiveFlowStall
const ssProbePassword = "s5-independent-reference-bob-credential"
const ssVictimPassword = "s5-independent-reference-alice-credential"
const ssProbeMode = "TETHER_TEST_SS_PROBE_MODE"
const ssProbeSealFD = "TETHER_TEST_SS_SEAL_FD"

type ssProbeOutput struct {
	mu   sync.Mutex
	data bytes.Buffer
}

func (output *ssProbeOutput) Write(data []byte) (int, error) {
	output.mu.Lock()
	defer output.mu.Unlock()
	return output.data.Write(data)
}
func (output *ssProbeOutput) snapshot() []byte {
	output.mu.Lock()
	defer output.mu.Unlock()
	return append([]byte(nil), output.data.Bytes()...)
}

func ssReferenceFingerprint(t *testing.T, conn *net.TCPConn) rawsocket.Fingerprint {
	t.Helper()
	raw, err := conn.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var stat syscall.Stat_t
	var statErr error
	if err := raw.Control(func(fd uintptr) { statErr = syscall.Fstat(int(fd), &stat) }); err != nil || statErr != nil {
		t.Fatal("reference original socket fingerprint unavailable")
	}
	return rawsocket.Fingerprint{Dev: stat.Dev, Ino: stat.Ino, Type: stat.Mode & syscall.S_IFMT}
}

// The client owns its framing and nonce decisions. It imports no SS codec;
// matching product plaintext alone would miss a reset or duplicated frame.
// origin: hot-update S5 plan §6.2 / T42
type ssIndependentClient struct {
	socket           *net.TCPConn
	master           []byte
	tx, rx           cipher.AEAD
	txNonce, rxNonce [12]byte
	pending          []byte
	onIOError        func() error
}

func (cl *ssIndependentClient) fail(t *testing.T, message string) {
	t.Helper()
	if cl.onIOError != nil {
		if err := cl.onIOError(); err != nil {
			t.Fatal(err)
		}
	}
	t.Fatal(message)
}

func ssReferenceMaster(password string) []byte {
	var master, prior []byte
	for len(master) < 32 {
		h := md5.New()
		_, _ = h.Write(prior)
		_, _ = h.Write([]byte(password))
		prior = h.Sum(nil)
		master = append(master, prior...)
	}
	return master[:32]
}

func ssReferenceAEAD(t *testing.T, master, salt []byte) cipher.AEAD {
	t.Helper()
	key := make([]byte, 32)
	if _, err := io.ReadFull(hkdf.New(sha1.New, master, salt, []byte("ss-subkey")), key); err != nil {
		t.Fatal("reference subkey derivation failed")
	}
	aead, err := chacha20poly1305.New(key)
	if err != nil {
		t.Fatal("reference AEAD creation failed")
	}
	return aead
}

func ssReferenceAdvance(nonce *[12]byte) {
	for i := range nonce {
		nonce[i]++
		if nonce[i] != 0 {
			return
		}
	}
	panic("reference nonce exhausted")
}

func ssWriteAll(t *testing.T, w io.Writer, data []byte) {
	t.Helper()
	for len(data) != 0 {
		n, err := w.Write(data)
		if err != nil || n <= 0 {
			t.Fatal("reference write failed")
		}
		data = data[n:]
	}
}

func ssReferenceDial(t *testing.T, port int, password, target string, reserve func(rawsocket.Tuple) error) *ssIndependentClient {
	t.Helper()
	address := netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), uint16(port))
	dialer := net.Dialer{Timeout: ssProbeBudget, Control: func(_, _ string, raw syscall.RawConn) error {
		var operationErr error
		if err := raw.Control(func(fd uintptr) {
			// A loopback MSS can exceed an entire SS frame, making each write
			// all-or-zero despite tiny SO_SNDBUF. Negotiate smaller real TCP
			// segments before connect so backpressure can leave a cipher suffix.
			// origin: hot-update S5 plan §6.1 / §8 T42
			if err := syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_MAXSEG, 1024); err != nil {
				operationErr = err
				return
			}
			// The external reference reserves the real ephemeral tuple before
			// connect, using the standard library rather than the product dialer.
			if err := syscall.Bind(int(fd), &syscall.SockaddrInet4{Addr: [4]byte{127, 0, 0, 1}}); err != nil {
				operationErr = err
				return
			}
			local, err := syscall.Getsockname(int(fd))
			if err != nil {
				operationErr = err
				return
			}
			ipv4, ok := local.(*syscall.SockaddrInet4)
			if !ok {
				operationErr = fmt.Errorf("reference socket is not IPv4")
				return
			}
			operationErr = reserve(rawsocket.Tuple{Local: netip.AddrPortFrom(netip.AddrFrom4(ipv4.Addr), uint16(ipv4.Port)), Remote: address})
		}); err != nil {
			return err
		}
		return operationErr
	}}
	conn, err := dialer.DialContext(t.Context(), "tcp4", address.String())
	if err != nil {
		t.Fatalf("reference original socket connect: %v", err)
	}
	sock := conn.(*net.TCPConn)
	t.Cleanup(func() { _ = sock.Close() })
	cl := &ssIndependentClient{socket: sock, master: ssReferenceMaster(password)}
	salt := make([]byte, 32)
	if _, err := rand.Read(salt); err != nil {
		t.Fatal("reference salt generation failed")
	}
	cl.tx = ssReferenceAEAD(t, cl.master, salt)
	ssWriteAll(t, sock, salt)
	host, portText, err := net.SplitHostPort(target)
	if err != nil {
		t.Fatal(err)
	}
	pn, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatal(err)
	}
	ip := net.ParseIP(host).To4()
	if ip == nil {
		t.Fatal("reference fixture requires an IPv4 target")
	}
	header := append([]byte{1}, ip...)
	header = append(header, byte(pn>>8), byte(pn))
	cl.send(t, header)
	return cl
}

func (cl *ssIndependentClient) encode(t *testing.T, plaintext []byte) []byte {
	t.Helper()
	if len(plaintext) == 0 || len(plaintext) > 0x3fff {
		t.Fatal("reference chunk size out of bounds")
	}
	var length [2]byte
	binary.BigEndian.PutUint16(length[:], uint16(len(plaintext)))
	wire := cl.tx.Seal(nil, cl.txNonce[:], length[:], nil)
	ssReferenceAdvance(&cl.txNonce)
	wire = cl.tx.Seal(wire, cl.txNonce[:], plaintext, nil)
	ssReferenceAdvance(&cl.txNonce)
	return wire
}

func (cl *ssIndependentClient) send(t *testing.T, plaintext []byte) {
	t.Helper()
	_ = cl.socket.SetWriteDeadline(time.Now().Add(ssProbeBudget))
	ssWriteAll(t, cl.socket, cl.encode(t, plaintext))
}

func (cl *ssIndependentClient) receive(t *testing.T, want []byte) {
	t.Helper()
	_ = cl.socket.SetReadDeadline(time.Now().Add(ssProbeBudget))
	if cl.rx == nil {
		salt := make([]byte, 32)
		if _, err := io.ReadFull(cl.socket, salt); err != nil {
			cl.fail(t, "reference response salt unavailable")
		}
		cl.rx = ssReferenceAEAD(t, cl.master, salt)
	}
	for len(cl.pending) < len(want) {
		lengthCipher := make([]byte, 18)
		if _, err := io.ReadFull(cl.socket, lengthCipher); err != nil {
			cl.fail(t, "reference encrypted length unavailable")
		}
		length, err := cl.rx.Open(nil, cl.rxNonce[:], lengthCipher, nil)
		if err != nil {
			t.Fatal("independent cipher trace: length authentication failed")
		}
		ssReferenceAdvance(&cl.rxNonce)
		n := int(binary.BigEndian.Uint16(length))
		if n == 0 || n > 0x3fff {
			t.Fatal("independent cipher trace: invalid length")
		}
		payload := make([]byte, n+16)
		if _, err := io.ReadFull(cl.socket, payload); err != nil {
			cl.fail(t, "reference encrypted payload unavailable")
		}
		plain, err := cl.rx.Open(nil, cl.rxNonce[:], payload, nil)
		if err != nil {
			t.Fatal("independent cipher trace: payload authentication failed")
		}
		ssReferenceAdvance(&cl.rxNonce)
		cl.pending = append(cl.pending, plain...)
	}
	if !bytes.Equal(cl.pending[:len(want)], want) {
		t.Fatal("independent duplex sequence/hash mismatch")
	}
	cl.pending = cl.pending[len(want):]
}

// Each target keeps accepting until fixture teardown. A redial cannot hide
// behind a listener that stopped after its first successful Accept.
type ssTarget struct {
	ln      net.Listener
	mu      sync.Mutex
	conns   []net.Conn
	accepts int
	wg      sync.WaitGroup
}

func ssNewTarget(t *testing.T) *ssTarget {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	target := &ssTarget{ln: ln}
	target.wg.Add(1)
	go func() {
		defer target.wg.Done()
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			target.mu.Lock()
			target.accepts++
			target.conns = append(target.conns, conn)
			target.mu.Unlock()
		}
	}()
	t.Cleanup(target.close)
	return target
}

func (target *ssTarget) close() {
	_ = target.ln.Close()
	target.wg.Wait()
	target.mu.Lock()
	defer target.mu.Unlock()
	for _, conn := range target.conns {
		_ = conn.Close()
	}
}

func (target *ssTarget) original(t *testing.T) net.Conn {
	t.Helper()
	deadline := time.Now().Add(ssProbeBudget)
	for time.Now().Before(deadline) {
		target.mu.Lock()
		if len(target.conns) > 0 {
			conn := target.conns[0]
			target.mu.Unlock()
			return conn
		}
		target.mu.Unlock()
		time.Sleep(time.Millisecond)
	}
	t.Fatal("target Accept never occurred")
	return nil
}

func (target *ssTarget) assertOneAccept(t *testing.T) {
	t.Helper()
	target.mu.Lock()
	defer target.mu.Unlock()
	if target.accepts != 1 {
		t.Fatalf("target Accept=%d, want exactly 1 original connection", target.accepts)
	}
}

func ssDuplexSample(t *testing.T, client *ssIndependentClient, target net.Conn, sequence int) {
	t.Helper()
	forward := []byte(fmt.Sprintf("forward:%06d:%x", sequence, sha256.Sum256([]byte(strconv.Itoa(sequence)))))
	reverse := []byte(fmt.Sprintf("reverse:%06d:%x", sequence, sha256.Sum256(forward)))
	client.send(t, forward)
	_ = target.SetReadDeadline(time.Now().Add(ssProbeBudget))
	got := make([]byte, len(forward))
	if _, err := io.ReadFull(target, got); err != nil {
		t.Fatal("original target forward bytes unavailable")
	}
	if !bytes.Equal(got, forward) {
		t.Fatal("original target forward sequence/hash mismatch")
	}
	_ = target.SetWriteDeadline(time.Now().Add(ssProbeBudget))
	ssWriteAll(t, target, reverse)
	client.receive(t, reverse)
}

// The legacy server is a runnable compatibility control while S5 actors are
// still P0 stubs. It verifies the independent client before any migration
// claim, and does not stand in for the same-PID or fresh-grant receipts.
// origin: hot-update S5 plan §6.2 / T42
func TestSSIndependentReferenceDuplexTrace(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(strconv.Itoa(round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			exerciseSSReferenceDuplex(t)
		})
	}
	testharness.AssertNoGoroutineLeak(t, "ss-reference-duplex", beforeG)
	testharness.AssertNoFDLeak(t, "ss-reference-duplex", beforeFD, 0)
}

func exerciseSSReferenceDuplex(t *testing.T) {
	t.Helper()
	target := ssNewTarget(t)
	server := ssproxy.New(nil)
	t.Cleanup(server.Stop)
	port, err := server.Start(0, []ssproxy.Key{{KeyID: "bob", Secret: ssProbePassword}})
	if err != nil {
		t.Fatal(err)
	}
	client := ssReferenceDial(t, port, ssProbePassword, target.ln.Addr().String(), func(rawsocket.Tuple) error { return nil })
	conn := target.original(t)
	for sequence := 0; sequence < 40; sequence++ {
		ssDuplexSample(t, client, conn, sequence)
	}
	target.assertOneAccept(t)
	server.Stop()
	ssAssertSilentTargetClosed(t, conn)
}

// origin: hot-update S5 plan §8 T42 / §6.2
// This is the same-PID kernel receipt. Two product artifacts and full agent
// bootstrap are a separate P19/P14/P16 integration receipt.
func TestSSRealExecKeepsIndependentCipherTrace(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	helper := ssBuildProbe(t, ssProbeHelperTest.Name)
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseSSRealExec(t, helper)
	}
	testharness.AssertNoGoroutineLeak(t, "ss-real-exec", beforeG)
	testharness.AssertNoFDLeak(t, "ss-real-exec", beforeFD, 0)
}

// This runnable control verifies that the temporary primitive probe notices
// duplicate Seal calls even while all ciphertext and plaintext stay correct.
// origin: hot-update S5 plan §6.2 / T42
func TestSSActualSealLedgerCoversIndependentDuplexTrace(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	helper := ssBuildProbe(t, ssProbeHelperTest.Name)
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseSSSealControl(t, helper)
	}
	testharness.AssertNoGoroutineLeak(t, "ss-seal-control", beforeG)
	testharness.AssertNoFDLeak(t, "ss-seal-control", beforeFD, 0)
}

func exerciseSSRealExec(t *testing.T, helper string) { t.Helper(); exerciseSSProbe(t, helper, true) }
func exerciseSSSealControl(t *testing.T, helper string) {
	t.Helper()
	exerciseSSProbe(t, helper, false)
}

func exerciseSSProbe(t *testing.T, helper string, performCut bool) {

	t.Helper()
	target := ssNewTarget(t)
	defer target.close()
	commandsR, commandsW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer func(closeFunc func() error) { _ = closeFunc() }(commandsR.Close)
	defer func(closeFunc func() error) { _ = closeFunc() }(commandsW.Close)
	eventsR, eventsW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer func(closeFunc func() error) { _ = closeFunc() }(eventsR.Close)
	defer func(closeFunc func() error) { _ = closeFunc() }(eventsW.Close)
	ctx, cancel := context.WithTimeout(t.Context(), 6*ssProbeBudget)
	defer cancel()
	cmd := exec.CommandContext(ctx, helper, "-test.run=^TestSSProbeHelper$", "-test.count=1")
	mode := "legacy-control"
	if performCut {
		mode = "before"
	}
	cmd.Env = append(ssProbeEnvironment(mode, "5"), "HOME="+t.TempDir())
	cmd.ExtraFiles = []*os.File{commandsR, eventsW, eventsW}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	var output ssProbeOutput
	cmd.Stdout, cmd.Stderr = &output, &output
	waitProcess, err := testharness.StartProcess(cmd)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL); _ = waitProcess() }()
	_ = commandsR.Close()
	_ = eventsW.Close()
	ledger := make(map[string]struct{})
	var ledgerMu sync.Mutex
	shortWrites := 0
	progress := make(chan string, 16)
	traceErrors := make(chan error, 1)
	pumpDone := make(chan struct{})
	go func() {
		defer close(pumpDone)
		defer close(progress)
		reader := bufio.NewReader(eventsR)
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				traceErrors <- err
				return
			}
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, "seal ") {
				key := strings.TrimPrefix(line, "seal ")
				ledgerMu.Lock()
				_, duplicate := ledger[key]
				ledger[key] = struct{}{}
				ledgerMu.Unlock()
				if duplicate || len(key) != 64 {
					traceErrors <- fmt.Errorf("actual subkey/nonce reused by Seal or malformed primitive ledger")
					return
				}
				continue
			}
			if strings.HasPrefix(line, "short ") {
				var n, attempted int
				if _, err := fmt.Sscanf(line, "short %d %d", &n, &attempted); err != nil || n <= 0 || n >= attempted {
					traceErrors <- fmt.Errorf("invalid actual partial Write receipt")
					return
				}
				ledgerMu.Lock()
				shortWrites++
				ledgerMu.Unlock()
				continue
			}
			select {
			case progress <- line:
			case <-ctx.Done():
				return
			}
		}
	}()
	defer func() { _ = eventsR.Close(); <-pumpDone }()
	readEvent := func(want string) string {
		t.Helper()
		// The helper owns the five-second cut budget. Leave time for its failure
		// record to reach this observer instead of racing the same deadline.
		// origin: hot-update S5 plan §8 T42
		timer := time.NewTimer(ssProbeBudget + time.Second)
		defer timer.Stop()
		select {
		case line, open := <-progress:
			if !open {
				t.Fatalf("SS helper missing %s: %s", want, output.snapshot())
			}
			if !strings.HasPrefix(line, want+" ") {
				t.Fatalf("SS helper event out of order, want %s", want)
			}
			return strings.TrimPrefix(line, want+" ")
		case err := <-traceErrors:
			if err == io.EOF {
				// The event pipe can close before testing flushes the helper's
				// failure to stdout. End and reap the failed child before reading
				// its diagnostic; pipe EOF alone does not prove process exit.
				_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
				_ = waitProcess()
			}
			t.Fatalf("SS helper trace failed: %v\n%s", err, output.snapshot())
		case <-timer.C:
			t.Fatalf("SS helper missing %s before deadline: %s", want, output.snapshot())
		}
		return ""
	}
	port, err := strconv.Atoi(readEvent("listening"))
	if err != nil {
		t.Fatal(err)
	}
	client := ssReferenceDial(t, port, ssProbePassword, target.ln.Addr().String(), func(tuple rawsocket.Tuple) error {
		// Tuple is transferred through an operation pipe, never a public
		// image, log, ordinary file, argv or environment variable.
		ssWriteAll(t, commandsW, []byte("expect "+tuple.Local.String()+" "+tuple.Remote.String()+"\n"))
		_ = readEvent("expected")
		return nil
	})
	client.onIOError = func() error {
		select {
		case err := <-traceErrors:
			return err
		default:
			return nil
		}
	}
	defer func(closeFunc func() error) { _ = closeFunc() }(client.socket.Close)
	original := ssReferenceFingerprint(t, client.socket)
	conn := target.original(t)
	for sequence := 0; sequence < 20; sequence++ {
		ssDuplexSample(t, client, conn, sequence)
	}
	if performCut {
		ssWriteAll(t, commandsW, []byte("small-buffers\n"))
		_ = readEvent("buffers")
		if err := client.socket.SetReadBuffer(4096); err != nil {
			t.Fatal(err)
		}
		// A paused reference reader and bounded kernel buffers force a real
		// short ciphertext syscall, not merely a nonempty application buffer.
		flood := bytes.Repeat([]byte("independent-reverse-tail/"), 32768)
		floodDone := make(chan error, 1)
		go func() {
			_ = conn.SetWriteDeadline(time.Now().Add(4 * ssProbeBudget))
			remaining := flood
			for len(remaining) > 0 {
				n, err := conn.Write(remaining)
				if err != nil || n <= 0 {
					floodDone <- fmt.Errorf("target reverse tail write failed")
					return
				}
				remaining = remaining[n:]
			}
			floodDone <- nil
		}()
		floodJoined := false
		defer func() {
			_ = conn.Close()
			if !floodJoined {
				<-floodDone
			}
		}()
		// Retain a real partially received encrypted payload over exec. Sending
		// only a prefix after the pre-cut samples rules out an empty parser cut.
		partialPlain := []byte("partial-payload-spanning-the-original-image-cut")
		partial := client.encode(t, partialPlain)
		ssWriteAll(t, client.socket, partial[:23])
		ssWriteAll(t, commandsW, []byte("cut\n"))
		before := readEvent("cut")
		after := readEvent("restored")
		if before != after || !strings.HasPrefix(after, strconv.Itoa(cmd.Process.Pid)+"/") {
			t.Fatal("SS exec changed PID/start ticks or original socket fingerprints")
		}
		ssWriteAll(t, client.socket, partial[23:])
		_ = conn.SetReadDeadline(time.Now().Add(ssProbeBudget))
		got := make([]byte, len(partialPlain))
		if _, err := io.ReadFull(conn, got); err != nil || !bytes.Equal(got, partialPlain) {
			t.Fatal("partial encrypted payload lost or reparsed across exec")
		}
		if err := client.socket.SetReadBuffer(256 << 10); err != nil {
			t.Fatal(err)
		}
		client.receive(t, flood)
		if err := <-floodDone; err != nil {
			floodJoined = true
			t.Fatal(err)
		}
		floodJoined = true
		ledgerMu.Lock()
		observedShort := shortWrites
		ledgerMu.Unlock()
		if observedShort == 0 {
			t.Fatal("SS exec cut lacks a real 0<n<attemptLen ciphertext Write receipt")
		}
	}
	for sequence := 20; sequence < 40; sequence++ {
		ssDuplexSample(t, client, conn, sequence)
	}
	if ssReferenceFingerprint(t, client.socket) != original {
		t.Fatal("reference application socket changed across exec")
	}
	ssWriteAll(t, commandsW, []byte("stop\n"))
	_ = readEvent("closed")
	// The receiver stays alive for this receipt: process exit cannot conceal
	// a leaked sealed state or borrowed business descriptor.
	target.assertOneAccept(t)
	ledgerMu.Lock()
	primitives := len(ledger)
	ledgerMu.Unlock()
	if primitives < 80 {
		t.Fatalf("actual Seal ledger is vacuous: got %d primitives, want at least 80", primitives)
	}
	ssWriteAll(t, commandsW, []byte("exit\n"))
	if err := waitProcess(); err != nil {
		t.Fatalf("SS helper failed: %v\n%s", err, output.snapshot())
	}
}

func ssProbeEnvironment(mode, sealFD string) []string {
	env := handoff.CleanEnvironment(os.Environ())
	filtered := env[:0]
	for _, entry := range env {
		if strings.HasPrefix(entry, ssProbeMode+"=") || strings.HasPrefix(entry, ssProbeSealFD+"=") || strings.HasPrefix(entry, "TETHER_INSTANCE_ID=") || strings.HasPrefix(entry, "TETHER_ROUTING_NID=") {
			continue
		}
		filtered = append(filtered, entry)
	}
	return append(filtered, ssProbeMode+"="+mode, ssProbeSealFD+"="+sealFD)
}

func ssFileNumber(t *testing.T, file *os.File) int {
	t.Helper()
	raw, err := file.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	fd := -1
	if err := raw.Control(func(value uintptr) { fd = int(value) }); err != nil {
		t.Fatal(err)
	}
	return fd
}

func ssProbeIdentity(t *testing.T, resources []handoff.Resource) string {
	t.Helper()
	stat, err := os.ReadFile("/proc/self/stat")
	if err != nil {
		t.Fatal(err)
	}
	end := bytes.LastIndexByte(stat, ')')
	fields := strings.Fields(string(stat[end+1:]))
	if len(fields) < 20 {
		t.Fatal("process start ticks missing")
	}
	var sockets []string
	for _, resource := range resources {
		// Restore consumes and closes the sealed directory; only the original
		// listener and business sockets belong in this kernel identity receipt.
		// origin: hot-update S5 plan §5.3 / §8 T42
		if resource.Kind != "ss-listener-v1" && resource.Kind != "ss-conn-v1" {
			continue
		}
		for _, file := range resource.Files {
			var state syscall.Stat_t
			if err := syscall.Fstat(ssFileNumber(t, file), &state); err != nil {
				t.Fatal(err)
			}
			if state.Mode&syscall.S_IFMT == syscall.S_IFSOCK {
				sockets = append(sockets, fmt.Sprintf("%d.%d", state.Dev, state.Ino))
			}
		}
	}
	if len(sockets) < 3 {
		t.Fatal("SS cut missing original listener/loopback/upstream sockets")
	}
	// Sorting makes the receipt independent of resource-map iteration order.
	sort.Strings(sockets)
	return fmt.Sprintf("%d/%s/%s", os.Getpid(), fields[19], strings.Join(sockets, ","))
}

var ssProbeHelperTest = testing.InternalTest{Name: "TestSSProbeHelper", F: ssExecHelper}

func ssExecHelper(t *testing.T) {
	var server *ssproxy.Server
	var commands, events, trace *os.File
	if os.Getenv(ssProbeMode) == "before" || os.Getenv(ssProbeMode) == "legacy-control" {
		commands, events, trace = os.NewFile(3, "ss-commands"), os.NewFile(4, "ss-events"), os.NewFile(5, "ss-trace")
		// ExtraFiles are inherited without CLOEXEC. Consume that first dispatch
		// before asking Prepared to duplicate these controls for the next exec.
		// origin: hot-update S5 plan §5.3 / §8 T42
		for _, file := range []*os.File{commands, events, trace} {
			syscall.CloseOnExec(ssFileNumber(t, file))
		}
		server = ssproxy.New(nil)
		port, err := server.Start(0, []ssproxy.Key{{KeyID: "bob", Secret: ssProbePassword}})
		if err != nil {
			t.Fatal(err)
		}
		defer server.Stop()
		if os.Getenv(ssProbeMode) != "legacy-control" {
			if err := server.SetFlowRevoke(func([16]byte, string) {}); err != nil {
				t.Fatalf("SS flow actor dependency: %v", err)
			}
		}
		if _, err := fmt.Fprintf(events, "listening %d\n", port); err != nil {
			t.Fatal(err)
		}
	} else {
		source, err := handoff.Restore()
		if err != nil || source == nil {
			t.Fatal("SS real exec handoff missing")
		}
		defer func(closeFunc func() error) { _ = closeFunc() }(source.Close)
		commands, err = source.TakeFile("ss-probe-control", "commands")
		if err != nil {
			t.Fatal(err)
		}
		events, err = source.TakeFile("ss-probe-control", "events")
		if err != nil {
			t.Fatal(err)
		}
		trace, err = source.TakeFile("ss-probe-control", "trace")
		if err != nil {
			t.Fatal(err)
		}
		_ = os.Setenv(ssProbeSealFD, strconv.Itoa(ssFileNumber(t, trace)))
		var image ssproxy.Image
		if err := json.Unmarshal(source.State.Data, &image); err != nil {
			t.Fatal(err)
		}
		// Capture the actual received descriptors before TakeFile transfers
		// them. Restore's resource return is failure salvage, empty on success.
		// origin: hot-update S5 plan §5.3 / §8 T42
		resources := source.Borrow()
		var salvage []handoff.Resource
		server, salvage, err = ssproxy.Restore(image, source)
		if err != nil {
			t.Fatalf("SS image restore dependency: %v", err)
		}
		if len(salvage) != 0 {
			t.Fatal("successful SS restore retained a second resource owner")
		}
		defer server.Stop()
		identity := ssProbeIdentity(t, resources)
		// Function pointers are not image state. Reinstall the bound-flow
		// terminal sink before activation, as the original owner did, so Stop
		// can discharge CleanupPending while this receiver is still alive.
		// origin: hot-update S5 plan §6.5 / §8 T42
		if err := server.SetFlowRevoke(func([16]byte, string) {}); err != nil {
			t.Fatal(err)
		}
		if again, err := handoff.Restore(); again != nil || err != nil {
			t.Fatal("SS handoff consumed twice")
		}
		if err := server.ActivateRaw(); err != nil {
			t.Fatal(err)
		}
		if err := server.Authorize(ssproxy.Grant{Enabled: true, Generation: 2, Epoch: 1, AppliedRevision: 3, Keys: []ssproxy.Key{{KeyID: "bob", Secret: ssProbePassword}}}); err != nil {
			t.Fatal(err)
		}
		if err := source.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := fmt.Fprintf(events, "restored %s\n", identity); err != nil {
			t.Fatal(err)
		}
	}
	defer func(closeFunc func() error) { _ = closeFunc() }(commands.Close)
	defer func(closeFunc func() error) { _ = closeFunc() }(events.Close)
	defer func(closeFunc func() error) { _ = closeFunc() }(trace.Close)
	scanner := bufio.NewScanner(commands)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) == 0 {
			t.Fatal("empty SS helper command")
		}
		switch fields[0] {
		case "expect":
			if len(fields) != 3 {
				t.Fatal("invalid SS flow reservation command")
			}
			local, err := netip.ParseAddrPort(fields[1])
			if err != nil {
				t.Fatal("invalid SS local tuple")
			}
			remote, err := netip.ParseAddrPort(fields[2])
			if err != nil {
				t.Fatal("invalid SS remote tuple")
			}
			if os.Getenv(ssProbeMode) != "legacy-control" {
				if err := server.ExpectFlow([16]byte{1}, rawsocket.Tuple{Local: local, Remote: remote}); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := fmt.Fprintln(events, "expected ok"); err != nil {
				t.Fatal(err)
			}
		case "small-buffers":
			frozen, err := server.Freeze(time.Now().Add(ssProbeBudget))
			if err != nil {
				t.Fatal(err)
			}
			_, snapshot, err := frozen.Export()
			if err != nil {
				t.Fatal(err)
			}
			for _, resource := range snapshot.Resources {
				for _, file := range resource.Files {
					fd := ssFileNumber(t, file)
					var stat syscall.Stat_t
					if err := syscall.Fstat(fd, &stat); err != nil {
						t.Fatal(err)
					}
					if stat.Mode&syscall.S_IFMT == syscall.S_IFSOCK {
						if err := syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_SNDBUF, 4096); err != nil {
							t.Fatal(err)
						}
					}
				}
			}
			if err := snapshot.Close(); err != nil {
				t.Fatal(err)
			}
			if err := frozen.Thaw(); err != nil {
				t.Fatal(err)
			}
			if err := server.Authorize(ssproxy.Grant{Enabled: true, Generation: 1, Epoch: 1, AppliedRevision: 2, Keys: []ssproxy.Key{{KeyID: "bob", Secret: ssProbePassword}}}); err != nil {
				t.Fatal(err)
			}
			if _, err := fmt.Fprintln(events, "buffers ok"); err != nil {
				t.Fatal(err)
			}
		case "cut":
			// Poll the actual cursor before freezing; receipt-driven setup
			// prevents a scheduler race from turning this into an empty cut.
			deadline := time.Now().Add(ssProbeBudget)
			var lastPartial, lastPartialCipher bool
			for {
				frozen, err := server.Freeze(deadline)
				if err != nil {
					t.Fatalf("SS freeze dependency: %v; prior received_partial=%t sealed_partial=%t", err, lastPartial, lastPartialCipher)
				}
				state, err := frozen.SnapshotState()
				if err != nil {
					t.Fatal(err)
				}
				partial, partialCipher := false, false
				for _, record := range state.Records {
					var cursor uint64
					var cipherBytes int
					for _, field := range record.Fields {
						if field.Name == "tx_cipher_write_cursor" {
							cursor = field.U64
						}
						if field.Name == "tx_cipher_pending" {
							cipherBytes = len(field.Bytes)
						}
					}
					if record.Kind == "ss-actor" && cursor > 0 && cursor < uint64(cipherBytes) {
						partialCipher = true
					}
					for _, field := range record.Fields {
						if record.Kind == "ss-actor" && field.Name == "rx_payload_cursor" && field.U64 > 0 {
							partial = true
						}
					}
				}
				if !partial || !partialCipher {
					lastPartial, lastPartialCipher = partial, partialCipher
					if err := frozen.Thaw(); err != nil {
						t.Fatal(err)
					}
					if !time.Now().Before(deadline) {
						t.Fatalf("SS cut has no simultaneous partial encrypted payload and already-Sealed ciphertext suffix: received_partial=%t sealed_partial=%t", partial, partialCipher)
					}
					if err := server.Authorize(ssproxy.Grant{Enabled: true, Generation: 1, Epoch: 1, AppliedRevision: 2, Keys: []ssproxy.Key{{KeyID: "bob", Secret: ssProbePassword}}}); err != nil {
						t.Fatal(err)
					}
					time.Sleep(time.Millisecond)
					continue
				}
				image, snapshot, err := frozen.Export()
				if err != nil {
					t.Fatalf("SS export dependency: %v", err)
				}
				t.Cleanup(func() { _ = snapshot.Close() })
				if _, err := fmt.Fprintf(events, "cut %s\n", ssProbeIdentity(t, snapshot.Resources)); err != nil {
					t.Fatal(err)
				}
				data, err := json.Marshal(image)
				if err != nil {
					t.Fatal(err)
				}
				resources := append(snapshot.Resources, handoff.Resource{ID: "ss-probe-control", Kind: "ss-probe-control", Files: map[string]*os.File{"commands": commands, "events": events, "trace": trace}})
				prepared, err := handoff.Prepare(handoff.State{Role: "agent", Instance: "ss-probe", Generation: 2, Data: data}, resources)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = prepared.Close() })
				if err := prepared.Exec(os.Args[0], os.Args, ssProbeEnvironment("after", "")); err != nil {
					t.Fatalf("SS actual syscall.Exec failed: %v", err)
				}
				t.Fatal("SS exec returned success")
			}
		case "stop":
			server.Stop()
			entries, err := os.ReadDir("/proc/self/fd")
			if err != nil {
				t.Fatal(err)
			}
			for _, entry := range entries {
				fd, _ := strconv.Atoi(entry.Name())
				if fd <= 2 {
					continue
				}
				name, err := os.Readlink("/proc/self/fd/" + entry.Name())
				if err != nil {
					continue
				}
				if strings.HasPrefix(name, "socket:") || strings.Contains(name, "memfd:tether") {
					t.Fatal("live SS receiver retained business socket or sealed/handoff/replay memfd after Stop")
				}
			}
			if _, err := fmt.Fprintln(events, "closed ok"); err != nil {
				t.Fatal(err)
			}
		case "exit":
			return
		default:
			t.Fatal("unknown SS helper command")
		}
	}
	t.Fatal("SS helper command pipe closed before exit")
}
