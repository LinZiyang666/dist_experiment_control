//go:build linux && hotupdate_integration

package hotupdate_test

import (
	"crypto/cipher"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"golang.org/x/crypto/chacha20poly1305"
	"golang.org/x/crypto/hkdf"
)

// origin: hot-update S5 plan §5.4 / §9.2 / §8 T45
// origin: hot-update S5 internal review HOTUPDATE-T45
// origin: S7 internal review GATE-CONVERGE-contracts F7
// origin: S8 internal review TAGGED-SUITE-SWEEP-A F4
func TestReadyPendingWatchdogTakesSecondCurrentCut(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	if os.Geteuid() == 0 {
		// Root bypasses directory permissions. Silently skipping or treating a
		// successful commit as a fault would turn the watchdog test into T44.
		t.Fatal("SETUP-RED: the real marker permission fault requires a non-root runner")
	}
	// Build the shared immutable artifacts before parallel fixtures start. Every
	// fixture retains its own original process, sockets and real 120s deadline.
	// The enclosing Run joins all five rounds before either leak assertion;
	// overlapping independent windows keeps this unit inside the 600s budget.
	// origin: S7 internal review GATE-CONVERGE-contracts F7 / plan lead L17
	artifacts := buildRollbackArtifacts(t)
	beforeFD, beforeG := testharness.FDCount(), runtime.NumGoroutine()
	t.Run("watchdog rounds", func(t *testing.T) {
		for round := 0; round < 5; round++ {
			t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) {
				t.Parallel()
				exerciseReadyPendingFlowSSWatchdog(t, &artifacts)
			})
		}
	})
	testharness.AssertNoFDLeak(t, "ready/pending flow/SS watchdog x5", beforeFD, 0)
	testharness.AssertNoGoroutineLeak(t, "ready/pending flow/SS watchdog x5", beforeG)
}

func exerciseReadyPendingFlowSSWatchdog(t *testing.T, artifacts **rollbackArtifacts) {
	t.Helper()
	// Parallel subtests have independent temp roots; each needs its own final
	// process census after fixture cleanup, in addition to artifact custody.
	// origin: hot-update S7 plan lead L16 / GATE-CONVERGE-contracts F7
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))
	f := newRollbackFixture(t, artifacts, true)
	defer f.close()
	f.awaitSamples(20)
	start := time.Now()
	f.upgrade(1)
	// The cold v1 owner starts at one. Both the ready receipt and the watchdog
	// rollback must name the next exact generation, as the ownership CAS does.
	// origin: hot-update S5 plan §4.4 / §5.4 / §8 T45
	// origin: S8 internal review TAGGED-SUITE-SWEEP-A F4. The failure is
	// now the v2 durable completion CAS, after control, migration and release;
	// the imported image remains serving while that native write is denied.
	f.wait("real completion failure after control-ready", 20*time.Second, func() bool {
		body, _ := os.ReadFile(f.log)
		output, _ := os.ReadFile(f.output)
		return strings.Contains(string(output), "ROLLBACK_COMMIT_FAILED") && rollbackReadyCount(body, 2) == 1
	})
	f.mu.Lock()
	installed := f.faultInstalled
	f.mu.Unlock()
	if !installed {
		t.Fatal("SETUP-RED: completion fault was not armed at the boot/register barrier")
	}
	if _, err := os.Stat(filepath.Join(f.home, "rollback-fault.entered")); err != nil {
		t.Fatal("SETUP-RED: actual completion CAS did not consume its permission fault")
	}
	view, ok := f.view()
	a := view.Observation.Current
	if !ok || a.Phase == upgradetx.PhaseCompleted || a.Handle.Generation != 2 ||
		a.Control.Kind != upgradetx.ReceiptControlReady || a.Resources.Kind != upgradetx.ReceiptResourcesMigrated ||
		a.Released.Kind != upgradetx.ReceiptOldGenerationReleased || f.commitCount() != 0 {
		t.Fatalf("ready image must retain its pending transaction after the actual failed completion: phase=%s", a.Phase)
	}
	deadline := f.watchdogDeadline
	if deadline != time.Unix(0, view.Historical.Grant.ExpiresAt).Add(upgradebudget.ComponentHandover) ||
		deadline.Before(start.Add(upgradebudget.ComponentHandover-5*time.Second)) || deadline.After(time.Now().Add(2*upgradebudget.ComponentHandover)) {
		t.Fatal("watchdog must use the original grant's unaccelerated absolute deadline")
	}
	if rollbackSHA(fmt.Sprintf("/proc/%d/exe", f.pid)) != f.artifacts.b.sha {
		t.Fatal("completion fault did not reach the real B image")
	}
	// Only the first completion fails. Restore the journal directory before
	// the watchdog's independent PREPARE/fsync writes, while streams keep running.
	if err := os.Chmod(f.journalDir, 0700); err != nil {
		t.Fatal(err)
	}
	first := f.cut(0)
	f.requireCut(first, 2)
	progress := f.progress()
	f.awaitSamples(progress.raw + 20)
	f.requireAdvanced(progress)
	if progress.grants < 2 {
		t.Fatal("SS must already have fresh RESUME authority before NATS commit failure")
	}

	var terminal rollbackRegister
	f.wait("automatic watchdog current-state rollback", time.Until(deadline)+30*time.Second, func() bool {
		f.mu.Lock()
		registers := append([]rollbackRegister(nil), f.registers...)
		f.mu.Unlock()
		for index, r := range registers {
			if index >= 2 && r.running == f.artifacts.a.sha && proto.SameRelease(r.req.ReleaseVersion, "v0.0.0-rollback-a") {
				terminal = r
				return true
			}
		}
		// Any full register during the window would retry commit and destroy
		// the W precondition. Roster refresh and heartbeats remain unmodified.
		if len(registers) != 2 {
			t.Fatal("SETUP-RED: an extra full register invalidated the pending-ready window")
		}
		currentView, known := f.view()
		current := currentView.Observation.Current
		if !known && time.Now().Before(deadline) || f.commitCount() != 0 || current.Phase == upgradetx.PhaseCompleted ||
			known && (current.Handle.Generation != 2 && (current.Handle.Generation != 3 || time.Now().Before(deadline))) {
			t.Fatal("pending-ready transaction committed or vanished before watchdog")
		}

		return false
	})
	if terminal.when.Before(deadline) || terminal.running != f.artifacts.a.sha || terminal.req.Incarnation != rollbackIncarnation {
		t.Fatal("automatic rollback must register a second current cut at the original deadline")
	}
	// Register remains only a control witness. Require the reverse's independent
	// three durable receipts and actual running/effective SHA after its reply.
	f.awaitImage(f.artifacts.a.sha, 3)
	second := f.cut(1)
	f.requireCurrentCut(first, second)
	f.awaitSamples(f.progress().raw + 20)
	f.requireAdvanced(progress)
	f.requireOriginalBusiness()
	body, _ := os.ReadFile(f.log)
	if rollbackReadyCount(body, 3) != 1 {
		t.Fatal("previous image did not activate the second current cut")
	}
	output, _ := os.ReadFile(f.output)
	if f.commitCount() != 1 || strings.Contains(string(output), "ROLLBACK_COMPLETED forward ") {
		t.Fatal("watchdog coverage was replaced by a completed forward or lacks completed reverse")
	}
	t.Logf("real pending-ready watchdog: elapsed=%s, deadline=%s, second generation=%d", terminal.when.Sub(start), deadline.Format(time.RFC3339Nano), second.Generation)
}

func rollbackReadyCount(body []byte, generation uint64) int {
	count := 0
	for _, line := range strings.Split(string(body), "\n") {
		var record struct {
			Message    string `json:"msg"`
			Generation uint64 `json:"generation"`
		}
		if json.Unmarshal([]byte(line), &record) == nil && record.Message == "rollback fixture ready" && record.Generation == generation {
			count++
		}
	}
	return count
}

// Independent SS framing: no ssproxy encryptor, decoder or nonce helper is
// imported. Successful Open under the next reference nonce is evidence of
// server continuity; any reset, repeat or skipped primitive is a sticky failure.
// Keys/salts/nonces stay in memory, never a receipt, argv, environment or log.
type rollbackReference struct {
	conn                    net.Conn
	master                  []byte
	readAEAD, writeAEAD     cipher.AEAD
	rx, tx                  uint64
	readPending             []byte
	ledger                  map[[44]byte]bool
	readDomain, writeDomain [32]byte
}

func newRollbackReference(conn net.Conn, password []byte, targetPort int) (*rollbackReference, error) {
	r := &rollbackReference{conn: conn, ledger: make(map[[44]byte]bool)}
	var previous []byte
	for len(r.master) < chacha20poly1305.KeySize {
		h := md5.New()
		_, _ = h.Write(previous)
		_, _ = h.Write(password)
		previous = h.Sum(nil)
		r.master = append(r.master, previous...)
	}
	r.master = r.master[:chacha20poly1305.KeySize]
	var salt [32]byte
	if _, err := rand.Read(salt[:]); err != nil {
		return nil, errors.New("reference entropy unavailable")
	}
	var err error
	r.writeAEAD, r.writeDomain, err = r.derive(salt[:])
	if err != nil {
		return nil, err
	}
	if err := rollbackWriteAll(conn, salt[:]); err != nil {
		return nil, errors.New("reference salt write failed")
	}
	// SOCKS address, IPv4 loopback, network-order target port. It is emitted
	// only inside an authenticated SS chunk, never a public image field.
	address := []byte{1, 127, 0, 0, 1, byte(targetPort >> 8), byte(targetPort)}
	if _, err := r.Write(address); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *rollbackReference) derive(salt []byte) (cipher.AEAD, [32]byte, error) {
	var key [32]byte
	if _, err := io.ReadFull(hkdf.New(sha1.New, r.master, salt, []byte("ss-subkey")), key[:]); err != nil {
		return nil, [32]byte{}, errors.New("reference key derivation failed")
	}
	aead, err := chacha20poly1305.New(key[:])
	if err != nil {
		return nil, [32]byte{}, errors.New("reference cipher initialization failed")
	}
	return aead, sha256.Sum256(key[:]), nil
}

func (r *rollbackReference) nonce(domain [32]byte, sequence uint64) ([]byte, error) {
	var nonce [12]byte
	binary.LittleEndian.PutUint64(nonce[:8], sequence)
	var identity [44]byte
	copy(identity[:32], domain[:])
	copy(identity[32:], nonce[:])
	if r.ledger[identity] {
		return nil, errors.New("reference detected an actual-subkey/nonce reuse")
	}
	r.ledger[identity] = true
	return nonce[:], nil
}

func (r *rollbackReference) Write(plain []byte) (int, error) {
	if len(plain) == 0 || len(plain) > 0x3fff {
		return 0, errors.New("reference invalid plaintext length")
	}
	var length [2]byte
	binary.BigEndian.PutUint16(length[:], uint16(len(plain)))
	for _, body := range [][]byte{length[:], plain} {
		nonce, err := r.nonce(r.writeDomain, r.tx)
		if err != nil {
			return 0, err
		}
		wire := r.writeAEAD.Seal(nil, nonce, body, nil)
		r.tx++
		if err := rollbackWriteAll(r.conn, wire); err != nil {
			return 0, errors.New("reference cipher write failed")
		}
	}
	return len(plain), nil
}

func (r *rollbackReference) Read(plain []byte) (int, error) {
	if r.readAEAD == nil {
		var salt [32]byte
		if _, err := io.ReadFull(r.conn, salt[:]); err != nil {
			return 0, errors.New("reference response salt missing")
		}
		var err error
		r.readAEAD, r.readDomain, err = r.derive(salt[:])
		if err != nil {
			return 0, err
		}
		if r.readDomain == r.writeDomain {
			return 0, errors.New("response salt reused the inbound nonce domain")
		}
	}
	if len(r.readPending) == 0 {
		length, err := r.open(2)
		if err != nil {
			return 0, err
		}
		size := int(binary.BigEndian.Uint16(length))
		if size == 0 || size > 0x3fff {
			return 0, errors.New("reference rejected malformed SS length")
		}
		r.readPending, err = r.open(size)
		if err != nil {
			return 0, err
		}
	}
	n := copy(plain, r.readPending)
	r.readPending = r.readPending[n:]
	return n, nil
}

func (r *rollbackReference) open(size int) ([]byte, error) {
	wire := make([]byte, size+chacha20poly1305.Overhead)
	if _, err := io.ReadFull(r.conn, wire); err != nil {
		return nil, errors.New("reference encrypted response truncated or stalled")
	}
	nonce, err := r.nonce(r.readDomain, r.rx)
	if err != nil {
		return nil, err
	}
	plain, err := r.readAEAD.Open(nil, nonce, wire, nil)
	if err != nil {
		return nil, errors.New("independent SS trace rejected reset, repeated, or skipped server nonce")
	}
	r.rx++
	return plain, nil
}

type rollbackProbe struct {
	conn            net.Conn
	ss              *rollbackReference
	mu              sync.Mutex
	samples, rx, tx uint64
	maxGap          time.Duration
	last            time.Time
	err             error
	stop            chan struct{}
	done            chan struct{}
}

func newRollbackProbe(t *testing.T, port int, password []byte, targetPort int) *rollbackProbe {
	t.Helper()
	var conn net.Conn
	// A dynamic public port can be stolen after reservation; a bind failure
	// is a setup failure, never a successful continuity receipt.
	deadline := time.Now().Add(10 * time.Second)
	for {
		var err error
		conn, err = net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), time.Second)
		if err == nil {
			break
		}
		if !time.Now().Before(deadline) {
			t.Fatal("public endpoint did not bind")
		}
		// sleep-fixture: wait for initial SS listener/REGISTER, before the
		// original business connection exists; never a reconnect after setup.
		time.Sleep(10 * time.Millisecond)
	}
	p := &rollbackProbe{conn: conn, stop: make(chan struct{}), done: make(chan struct{}), last: time.Now()}
	if err := conn.SetDeadline(time.Now().Add(upgradebudget.LiveFlowStall)); err != nil {
		_ = conn.Close()
		t.Fatal(err)
	}
	if password != nil {
		var err error
		p.ss, err = newRollbackReference(conn, password, targetPort)
		if err != nil {
			_ = conn.Close()
			t.Fatal(err)
		}
	}
	go p.run()
	return p
}

func (p *rollbackProbe) run() {
	defer close(p.done)
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	var reader io.Reader = p.conn
	var writer io.Writer = p.conn
	if p.ss != nil {
		reader, writer = p.ss, p.ss
	}
	for sequence := uint64(1); ; sequence++ {
		select {
		case <-p.stop:
			return
		default:
		}
		if err := p.conn.SetDeadline(time.Now().Add(upgradebudget.LiveFlowStall)); err != nil {
			p.fail(errors.New("original business socket deadline failed"))
			return
		}
		body := rollbackChallenge(sequence)
		if err := rollbackWriteAll(writer, body[:]); err != nil {
			p.fail(errors.New("original business request failed or stalled"))
			return
		}
		var response [256]byte
		if _, err := io.ReadFull(reader, response[:]); err != nil {
			p.fail(err)
			return
		}
		for i := 8; i < len(body); i++ {
			body[i] ^= 0xa5
		}
		if response != body {
			p.fail(errors.New("independent byte ledger detected dropped, duplicated, or reordered business bytes"))
			return
		}
		now := time.Now()
		p.mu.Lock()
		gap := now.Sub(p.last)
		if gap > p.maxGap {
			p.maxGap = gap
		}
		p.last = now
		p.samples = sequence
		if p.ss != nil {
			p.rx, p.tx = p.ss.rx, p.ss.tx
		}
		p.mu.Unlock()
		select {
		case <-p.stop:
			return
		case <-tick.C:
		}
	}
}

func (p *rollbackProbe) fail(err error) {
	p.mu.Lock()
	if p.err == nil {
		p.err = err
	}
	p.mu.Unlock()
}
func (p *rollbackProbe) failure() error { p.mu.Lock(); defer p.mu.Unlock(); return p.err }

func (p *rollbackProbe) snapshot() (samples, rx, tx uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.samples, p.rx, p.tx
}

func (p *rollbackProbe) requireWindow(t *testing.T) {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err != nil {
		t.Fatal(p.err)
	}
	gap := max(p.maxGap, time.Since(p.last))
	if p.samples < 20 || gap > upgradebudget.LiveFlowStall {
		t.Fatalf("normal flow continuity: samples=%d max/trailing gap=%s exceeds %s", p.samples, gap, upgradebudget.LiveFlowStall)
	}
	t.Logf("normal original flow: samples=%d max/trailing gap=%s", p.samples, gap)
}

func (p *rollbackProbe) close() {
	close(p.stop)
	_ = p.conn.Close()
	<-p.done
}
