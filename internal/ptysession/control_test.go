//go:build linux

package ptysession

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/workload"
)

// Real: sequence receipts and retained replay; controlled: the pure transaction
// table's signal outcome. Frozen/unknown guards are tested against real Manager.
// origin: hot-update S4 plan §3.4 / T09
func TestPtyHangupFailureReceipts(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	g0, fd0 := runtime.NumGoroutine(), testharness.FDCount()
	for i := 0; i < 5; i++ {
		t.Run("receipts", func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			exerciseHangupReceipts(t)
		})
		for _, stopped := range []bool{false, true} {
			t.Run("retained-process", func(t *testing.T) {
				// origin: hot-update S5 PROC-LEAK / final process census.
				testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

				exerciseHangupRetainsProcess(t, stopped)
			})
		}
	}
	testharness.AssertNoGoroutineLeak(t, "hangup receipts", g0)
	testharness.AssertNoFDLeak(t, "hangup receipts", fd0, 0)
	t.Run("manager-rejection", func(t *testing.T) {
		// origin: hot-update S5 PROC-LEAK / final process census.
		testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

		f := startActorFixture(t, "tty.setraw(0)\nmark('ready')\nwait('exit')\n")
		waitMark(t, f.dir, "ready")
		_, err := f.manager.FreezeUntil(time.Now().Add(actorTestBudget))
		requireDependency(t, err)
		r, err := f.a.ApplyControl(ControlRequest{Seq: 1, Op: "hangup"})
		if !errors.Is(err, workload.ErrFrozen) || r.Seq != 0 {
			t.Fatalf("frozen guard consumed receipt: %+v %v", r, err)
		}
		f.a.mu.Lock()
		closed := f.a.state.MasterClosed
		f.a.mu.Unlock()
		if closed {
			t.Fatal("frozen guard closed master")
		}
		f.manager.Thaw()
	})
}

func exerciseHangupReceipts(t *testing.T) {
	for _, tc := range []struct {
		name                string
		signalErr, closeErr error
		exited              bool
		signals, closes     int
		diagnostic          string
	}{
		{"success", nil, nil, false, 1, 1, ""}, {"exited", nil, nil, true, 0, 1, ""},
		{"esrch", syscall.ESRCH, nil, false, 1, 1, ""}, {"eperm", syscall.EPERM, nil, false, 1, 0, "pty control failed: signal_error"},
		{"close-error", nil, syscall.EBADF, false, 1, 1, "pty control failed: close_error"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := int64(1)
			a := tableActor(t, &now)
			m := a.cfg.Master.(*tableMaster)
			m.closeErr = tc.closeErr
			signals := 0
			c := workload.Control{Signal: func(sig syscall.Signal) error {
				if sig != syscall.SIGHUP {
					t.Fatalf("Hangup sent %v", sig)
				}
				signals++
				return tc.signalErr
			}}
			if tc.exited {
				c.Exit = &workload.Exit{}
			}
			r := ControlRequest{Seq: 1, Op: "hangup"}
			digest, _ := controlDigest(r)
			receipt, operationErr, err := a.commitHangup(c, &r, digest)
			if err != nil || receipt.Seq != 1 || receipt.Error != tc.diagnostic || signals != tc.signals || m.closes != tc.closes {
				t.Fatalf("transaction: %+v signals=%d closes=%d err=%v", receipt, signals, m.closes, err)
			}
			// The wire uses closed reason tokens; the internal result still carries
			// the exact syscall cause for cleanup/retry decisions.
			if (tc.signalErr != nil && tc.signalErr != syscall.ESRCH && !errors.Is(operationErr, tc.signalErr)) ||
				(tc.closeErr != nil && !errors.Is(operationErr, tc.closeErr)) {
				t.Fatal("transaction lost the underlying syscall error", operationErr)
			}
			if tc.diagnostic == "" && receipt.Error != "" {
				t.Fatalf("unexpected failure %+v", receipt)
			}
			if tc.closes == 0 && (a.state.InputEnded || a.state.MasterClosed) {
				t.Fatal("failed HUP consumed owner")
			}
			got, err := a.ApplyControl(r)
			if err != nil || got != receipt || signals != tc.signals || m.closes != tc.closes {
				t.Fatal("lost reply repeated side effect")
			}
		})
	}
}

// Real: original process-group signal with a child handshake, and resize whose
// first effect the child deliberately overwrites before retrying the same seq.
// origin: hot-update S4 plan §3.4 / T14
func TestPtyControlReplayDoesNotRepeatSideEffects(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	requirePTYImplementation(t)
	g0, fd0 := runtime.NumGoroutine(), testharness.FDCount()
	for i := 0; i < 5; i++ {
		t.Run("effects", func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			exerciseControlReplay(t)
		})
	}
	testharness.AssertNoGoroutineLeak(t, "control replay", g0)
	testharness.AssertNoFDLeak(t, "control replay", fd0, 0)
}
func exerciseControlReplay(t *testing.T) {
	f := startActorFixture(t, `tty.setraw(0)
count=0
def interrupt(*args):
 global count
 count+=1
 mark('interrupt',str(count).encode())
signal.signal(signal.SIGINT,interrupt)
mark('ready')
wait('resize-seen')
termios.tcsetwinsize(0,(33,99))
mark('overwritten')
wait('report')
mark('size',str(termios.tcgetwinsize(0)).encode())
mark('final-count',str(count).encode())
wait('exit')
`)
	waitMark(t, f.dir, "ready")
	first := ControlRequest{Seq: 1, Op: "signal", Signal: int(syscall.SIGINT)}
	receipt, err := f.a.ApplyControl(first)
	requireDependency(t, err)
	if receipt.Error != "" {
		t.Fatal(receipt)
	}
	waitMark(t, f.dir, "interrupt")
	if got, err := f.a.ApplyControl(first); err != nil || got != receipt {
		t.Fatalf("signal retry %+v %v", got, err)
	}
	resize := ControlRequest{Seq: 2, Op: "resize", Cols: 120, Rows: 40}
	if r, err := f.a.ApplyControl(resize); err != nil || r.Error != "" {
		t.Fatalf("resize: %+v %v", r, err)
	}
	markActor(t, f.dir, "resize-seen")
	waitMark(t, f.dir, "overwritten")
	if _, err := f.a.ApplyControl(resize); err != nil {
		t.Fatal(err)
	}
	for _, r := range []ControlRequest{{Seq: 4, Op: "hangup"}, {Seq: 2, Op: "resize", Cols: 100, Rows: 40}} {
		if _, err := f.a.ApplyControl(r); !errors.Is(err, ErrConflict) {
			t.Fatal("missing conflict fence", err)
		}
	}
	if _, err := f.a.ApplyControl(first); !errors.Is(err, ErrStale) {
		t.Fatal("missing stale fence", err)
	}
	markActor(t, f.dir, "report")
	waitMark(t, f.dir, "size")
	waitMark(t, f.dir, "final-count")
	requireFileBytes(t, f.dir+"/size", []byte("(33, 99)"))
	requireFileBytes(t, f.dir+"/final-count", []byte("1"))
}

// Real: Manager admission and kernel Close. Controlled: a barrier after HUP,
// before Close, to force the exact snapshot race rather than relying on timing.
// origin: hot-update S4 plan §3.4 / T08
func TestHangupSequenceNeverSplitsAcrossSnapshot(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	requirePTYImplementation(t)
	g0, fd0 := runtime.NumGoroutine(), testharness.FDCount()
	for i := 0; i < 5; i++ {
		t.Run("guard", func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			exerciseHangupSnapshot(t)
		})
		t.Run("frozen-first", func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			exerciseHangupFrozenFirst(t)
		})
		t.Run("reap-after-close", func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			exerciseHangupReapBarrier(t)
		})
	}
	testharness.AssertNoGoroutineLeak(t, "hangup snapshot", g0)
	testharness.AssertNoFDLeak(t, "hangup snapshot", fd0, 0)
}
func exerciseHangupSnapshot(t *testing.T) {
	f := startActorFixture(t, "signal.signal(signal.SIGHUP,lambda *_: mark('hup'))\ntty.setraw(0)\nmark('ready')\nwait('exit')\n")
	waitMark(t, f.dir, "ready")
	m := instrumentActor(t, f.a)
	release := make(chan struct{})
	m.closeEntered = make(chan struct{}, 1)
	m.releaseClose = release
	var once bool
	defer func() {
		if !once {
			close(release)
		}
	}()
	done := make(chan error, 1)
	go func() { _, err := f.a.ApplyControl(ControlRequest{Seq: 1, Op: "hangup"}); done <- err }()
	select {
	case <-m.closeEntered:
	case err := <-done:
		requireDependency(t, err)
		t.Fatal("Close was not entered")
	case <-time.After(actorTestBudget):
		t.Fatal("hangup did not enter close")
	}
	waitMark(t, f.dir, "hup")
	if _, err := f.manager.FreezeUntil(time.Now().Add(30 * time.Millisecond)); err == nil {
		t.Fatal("snapshot split Signal and Close")
	}
	f.a.mu.Lock()
	seq := f.a.state.ControlSeq
	f.a.mu.Unlock()
	if seq != 0 {
		t.Fatal("receipt preceded Close")
	}
	close(release)
	once = true
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	rows, err := f.manager.FreezeUntil(time.Now().Add(actorTestBudget))
	requireDependency(t, err)
	f.a.mu.Lock()
	closed := f.a.state.MasterClosed
	seq = f.a.state.ControlSeq
	f.a.mu.Unlock()
	if !closed || seq != 1 || len(rows) != 1 || rows[0].Exit != nil {
		t.Fatalf("split or forged exit: closed=%v seq=%d rows=%+v", closed, seq, rows)
	}
	f.manager.Thaw()
}

// Real: imported replay ownership and absolute metadata; controlled: boot
// clock and round-trip proofs, avoiding a real 180-second wait.
// origin: hot-update S4 plan §6.1 / T22
func TestPTYKAAbsoluteDeadlinesAcrossGenerations(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	g0, fd0 := runtime.NumGoroutine(), testharness.FDCount()
	for i := 0; i < 5; i++ {
		t.Run("clock", func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			exerciseKADeadlines(t)
		})
		t.Run("expired-authority-cleanup", func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			exerciseExpiredCleanupReap(t)
		})
	}
	testharness.AssertNoGoroutineLeak(t, "KA generations", g0)
	testharness.AssertNoFDLeak(t, "KA generations", fd0, 0)
	t.Run("healthy-hangup", func(t *testing.T) {
		// origin: hot-update S5 PROC-LEAK / final process census.
		testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

		f := startActorFixture(t, "signal.signal(signal.SIGHUP,lambda *_: mark('hup'))\ntty.setraw(0)\nmark('ready')\nwait('exit')\n")
		waitMark(t, f.dir, "ready")
		now := int64(time.Hour)
		f.a.cfg.Clock = func() (int64, error) { return now, nil }
		f.a.mu.Lock()
		f.a.state.KAIntervalMS = 1000
		f.a.state.KADeadlineBootNS = now
		f.a.mu.Unlock()
		requireDependency(t, f.a.Tick(HealthProof{Generation: 1, AtBootNS: now, Healthy: true}))
		now += int64(5 * time.Second)
		requireDependency(t, f.a.Tick(HealthProof{Generation: 1, AtBootNS: now, Healthy: true}))
		now += int64(time.Second)
		requireDependency(t, f.a.Tick(HealthProof{Generation: 1, AtBootNS: now, Healthy: true}))
		waitMark(t, f.dir, "hup")
		f.a.mu.Lock()
		closed := f.a.state.MasterClosed
		f.a.mu.Unlock()
		row, _ := f.manager.Lookup("terminal")
		if !closed || row.Exit != nil {
			t.Fatalf("KA failed to close or forged exit: closed=%v row=%+v", closed, row)
		}
	})
}
func exerciseKADeadlines(t *testing.T) {
	now := int64(time.Second)
	a := tableActor(t, &now)
	if a.judgeLocked(int64(time.Hour), HealthProof{Generation: 1, AtBootNS: int64(time.Hour), Healthy: true}) {
		t.Fatal("KA0 armed")
	}
	a.state.KAIntervalMS = 1000
	if err := a.renewKA(1); err != nil {
		t.Fatal(err)
	}
	r := ControlRequest{Seq: 1, Op: "keepalive"}
	digest, _ := controlDigest(r)
	a.commitControlLocked(r, digest, nil)
	original := a.state.KADeadlineBootNS
	now += int64(100 * time.Second)
	if _, err := a.ApplyControl(r); err != nil {
		t.Fatal(err)
	}
	if a.state.KADeadlineBootNS != original {
		t.Fatal("duplicate KA renewed grant")
	}
	if _, err := a.OutputBatch(0, false, 10); err != nil {
		t.Fatal(err)
	}
	if a.state.KADeadlineBootNS != original {
		t.Fatal("ACK renewed KA")
	}
	now = original + 1
	if a.judgeLocked(now, HealthProof{}) {
		t.Fatal("unhealthy expiry killed")
	}
	opportunity := a.state.RecoveryNotBeforeBootNS
	now = opportunity
	p := HealthProof{Generation: 1, AtBootNS: now, Healthy: true}
	if a.judgeLocked(now, p) {
		t.Fatal("one proof killed")
	}
	for generation := uint64(2); generation <= 4; generation++ {
		a.phase = phaseFrozen
		_, _, err := a.cfg.Input.Freeze()
		if err != nil && !errors.Is(err, workload.ErrFrozen) {
			t.Fatal(err)
		}
		_, _, err = a.cfg.Output.Freeze()
		if err != nil && !errors.Is(err, workload.ErrFrozen) {
			t.Fatal(err)
		}
		image := a.snapshotLocked()
		imported, err := Import(a.cfg, image)
		if err != nil {
			t.Fatal(err)
		}
		a = imported
		now += int64(time.Second)
		if a.state.KADeadlineBootNS != original || a.state.RecoveryNotBeforeBootNS != opportunity || a.proof.Healthy {
			t.Fatal("import restamped deadline or inherited proof")
		}
		p = HealthProof{Generation: generation, AtBootNS: now, Healthy: true}
		if a.judgeLocked(now, p) {
			t.Fatal("import reused previous proof")
		}
	}
	now += int64(time.Second)
	p.AtBootNS = now
	if !a.judgeLocked(now, p) {
		t.Fatal("three imports deferred expired KA indefinitely")
	}
	// A connection change requires two proofs, even without another import.
	now += int64(time.Second)
	p.Generation++
	p.AtBootNS = now
	if a.judgeLocked(now, p) {
		t.Fatal("connection generation reused old proof")
	}
	a.state.AuthorityDeadlineBootNS = now
	a.phase = phaseFrozen
	if err := a.PreActivate(); err != nil {
		t.Fatal(err)
	}
	if a.state.Revoked == "" || !a.state.InputEnded || a.state.InputError == "" {
		t.Fatal("expired authority left input eligible")
	}
	a.cfg.Clock = func() (int64, error) { return 0, os.ErrInvalid }
	before := a.state.KADeadlineBootNS
	if err := a.renewKA(2); err == nil || a.state.KADeadlineBootNS != before {
		t.Fatal("broken clock granted authority")
	}
}

func exerciseHangupFrozenFirst(t *testing.T) {
	f := startActorFixture(t, "signal.signal(signal.SIGHUP,lambda *_: mark('hup'))\ntty.setraw(0)\nmark('ready')\ndata=os.read(0,100)\nos.write(1,data)\nwait('exit')\n")
	waitMark(t, f.dir, "ready")
	_, err := f.manager.FreezeUntil(time.Now().Add(actorTestBudget))
	requireDependency(t, err)
	receipt, err := f.a.ApplyControl(ControlRequest{Seq: 1, Op: "hangup"})
	if !errors.Is(err, workload.ErrFrozen) || receipt.Seq != 0 {
		t.Fatalf("frozen guard changed receipt: %+v %v", receipt, err)
	}
	f.a.mu.Lock()
	closed := f.a.state.MasterClosed
	f.a.mu.Unlock()
	if closed {
		t.Fatal("frozen guard closed master")
	}
	if _, err := os.Stat(f.dir + "/hup"); !os.IsNotExist(err) {
		t.Fatal("frozen guard sent HUP")
	}
	f.manager.Thaw()
	f.a.cfg.PID = "unknown"
	receipt, err = f.a.ApplyControl(ControlRequest{Seq: 1, Op: "hangup"})
	f.a.cfg.PID = "terminal"
	if !errors.Is(err, workload.ErrUnknown) || receipt.Seq != 0 {
		t.Fatalf("unknown guard changed receipt: %+v %v", receipt, err)
	}
	if _, err := f.a.AcceptInput(0, []byte("both-pumps-live"), false); err != nil {
		t.Fatal(err)
	}
	waitOutput(t, f.a, "both-pumps-live")
}

func exerciseHangupRetainsProcess(t *testing.T, stopped bool) {
	requirePTYImplementation(t)
	body := "signal.signal(signal.SIGHUP,signal.SIG_IGN)\ntty.setraw(0)\nmark('ready')\n"
	if stopped {
		body += "os.kill(os.getpid(),signal.SIGSTOP)\n"
	}
	body += "wait('exit')\n"
	f := startActorFixture(t, body)
	waitMark(t, f.dir, "ready")
	if stopped {
		awaitActor(t, "original child stopped", func() bool { return processStopped(f.pid) })
	}
	// The driver's last-master close may itself send HUP followed by CONT.
	// Observe the stopped job after our explicit HUP but before that close;
	// this keeps the no-explicit-CONT assertion separate from kernel policy.
	// origin: hot-update S4 plan §3.4 / T09
	m := instrumentActor(t, f.a)
	release := make(chan struct{})
	m.closeEntered = make(chan struct{}, 1)
	m.releaseClose = release
	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()
	type result struct {
		receipt ControlReceipt
		err     error
	}
	done := make(chan result, 1)
	go func() {
		receipt, err := f.a.ApplyControl(ControlRequest{Seq: 1, Op: "hangup"})
		done <- result{receipt, err}
	}()
	select {
	case <-m.closeEntered:
	case r := <-done:
		t.Fatalf("hangup skipped close: %+v %v", r.receipt, r.err)
	case <-time.After(actorTestBudget):
		t.Fatal("hangup did not enter close")
	}
	if stopped && !processStopped(f.pid) {
		t.Fatal("explicit hangup signal continued stopped foreground")
	}
	close(release)
	released = true
	r := <-done
	receipt, err := r.receipt, r.err
	requireDependency(t, err)
	if receipt.Error != "" {
		t.Fatal(receipt.Error)
	}
	row, ok := f.manager.Lookup("terminal")
	if !ok || row.Exit != nil {
		t.Fatal("hangup fabricated terminal status")
	}
	if err := syscall.Kill(f.pid, 0); err != nil {
		t.Fatal("hangup killed HUP-immune child", err)
	}
	duplicate, err := f.a.ApplyControl(ControlRequest{Seq: 1, Op: "hangup"})
	if err != nil || duplicate != receipt {
		t.Fatalf("hangup retry changed receipt %+v %v", duplicate, err)
	}
}
func processStopped(pid int) bool {
	data, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return false
	}
	text := string(data)
	end := strings.LastIndexByte(text, ')')
	return end >= 0 && strings.HasPrefix(text[end+1:], " T ")
}

// A Hangup still joining a reader has not closed the master and cannot release
// the zombie identity. This is observable before Manager's callback guard starts.
// origin: hot-update S4 plan §3.4 / T08
func exerciseHangupReapBarrier(t *testing.T) {
	f := startActorFixture(t, "signal.signal(signal.SIGHUP,signal.SIG_IGN)\ntty.setraw(0)\nmark('ready')\nwait('write')\nos.write(1,b'BEFORE_CLOSE')\nwait('exit')\nmark('exiting')\nsys.exit(23)\n")
	waitMark(t, f.dir, "ready")
	m := instrumentActor(t, f.a)
	freezeActor(t, f.a)
	release := make(chan struct{})
	defer func() {
		if !stopped(release) {
			close(release)
		}
	}()
	m.readReturned = make(chan struct{}, 1)
	m.releaseRead = release
	activateActor(t, f.a)
	markActor(t, f.dir, "write")
	select {
	case <-m.readReturned:
	case <-time.After(actorTestBudget):
		t.Fatal("missing in-flight read")
	}
	waited := make(chan error, 1)
	go func() { _, err := f.manager.Wait("terminal"); waited <- err }()
	done := make(chan error, 1)
	go func() { _, err := f.a.ApplyControl(ControlRequest{Seq: 1, Op: "hangup"}); done <- err }()
	markActor(t, f.dir, "exit")
	waitMark(t, f.dir, "exiting")
	awaitActor(t, "leader exit while Hangup joins read", func() bool {
		if processZombie(f.pid) {
			return true
		}
		select {
		case err := <-waited:
			t.Fatalf("Hangup released reap before Close: %v", err)
		default:
		}
		return false
	})
	select {
	case err := <-waited:
		t.Fatalf("Hangup released reap before Close: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-waited:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(actorTestBudget):
		t.Fatal("Hangup did not release reap after Close")
	}
}

// Source EOF can arrive between the first revocation batch and its follow-up
// census. Persistently expired authority must still settle through Revoke.
// origin: hot-update S4 plan §6.1 / §6.2 / T22
func exerciseExpiredCleanupReap(t *testing.T) {
	f := startActorFixture(t, "tty.setraw(0)\nmark('ready')\nwait('exit')\n")
	waitMark(t, f.dir, "ready")
	if err := f.a.Revoke("authority expired"); err != nil {
		t.Fatal(err)
	}
	awaitActor(t, "revoked source EOF", func() bool { f.a.mu.Lock(); defer f.a.mu.Unlock(); return f.a.state.SourceEnded })
	row, _ := f.manager.Lookup("terminal")
	if !row.ReapHeld {
		t.Fatal("source EOF released pending cleanup")
	}
	f.a.mu.Lock()
	f.a.state.AuthorityDeadlineBootNS = 1
	f.a.mu.Unlock()
	awaitActor(t, "expired authority cleanup released reap", func() bool {
		if err := f.a.Tick(HealthProof{}); err != nil {
			t.Fatal(err)
		}
		row, _ := f.manager.Lookup("terminal")
		return !row.ReapHeld
	})
	if exit, err := f.manager.Wait("terminal"); err != nil || exit.Signal != int(syscall.SIGKILL) {
		t.Fatalf("revocation lost real exit: %+v %v", exit, err)
	}
}
