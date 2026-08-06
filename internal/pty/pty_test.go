package pty

import (
	"bytes"
	"errors"
	"io"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestAllocateSetSizeClose(t *testing.T) {
	s, err := Allocate(120, 40)
	if err != nil {
		t.Fatal(err)
	}
	if s.Master == nil {
		t.Fatal("master fd missing")
	}
	if err := s.SetSize(80, 24); err != nil {
		t.Errorf("SetSize: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

func TestStartCapturesEcho(t *testing.T) {
	s, err := Allocate(80, 24)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	if err := s.Start([]string{"sh", "-c", "echo hello-pty"}, nil, "", ""); err != nil {
		t.Fatal(err)
	}
	// Drain master in a goroutine to avoid the child blocking on output.
	out := make(chan string, 1)
	go func() {
		var buf bytes.Buffer
		_, _ = io.Copy(&buf, s.Master)
		out <- buf.String()
	}()

	exit, err := s.Wait()
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if exit != 0 {
		t.Errorf("exit code: got %d want 0", exit)
	}
	select {
	case got := <-out:
		if !strings.Contains(got, "hello-pty") {
			t.Errorf("PTY output missing 'hello-pty': %q", got)
		}
	case <-time.After(2 * time.Second):
		t.Error("master read did not finish after Wait")
	}
}

func TestSignalDeliversSIGINTToProcessGroup(t *testing.T) {
	s, err := Allocate(80, 24)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	// `sleep 30` traps and exits 130 on SIGINT; if our signal fails to
	// deliver, the test will hang for 30s and time out.
	if err := s.Start([]string{"sh", "-c", "trap 'exit 130' INT; sleep 30"}, nil, "", ""); err != nil {
		t.Fatal(err)
	}
	go func() { _, _ = io.Copy(io.Discard, s.Master) }()

	time.Sleep(200 * time.Millisecond) // let trap install
	if err := s.Signal(syscall.SIGINT); err != nil {
		t.Fatalf("Signal SIGINT: %v", err)
	}

	done := make(chan int, 1)
	go func() {
		exit, _ := s.Wait()
		done <- exit
	}()
	select {
	case exit := <-done:
		if exit != 130 {
			t.Errorf("exit code on SIGINT: got %d want 130", exit)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("child did not exit after SIGINT")
	}
}

func TestStartTwiceRejected(t *testing.T) {
	s, err := Allocate(80, 24)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	if err := s.Start([]string{"true"}, nil, "", ""); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Wait(); err != nil {
		t.Fatal(err)
	}
	// Drain residual master output on a CAPTURED handle (not the struct field)
	// and join the goroutine before returning, so it can't race the deferred
	// Close that nils s.Master (-race gate).
	master := s.Master
	done := make(chan struct{})
	go func() { _, _ = io.Copy(io.Discard, master); close(done) }()

	if err := s.Start([]string{"true"}, nil, "", ""); err == nil {
		t.Error("Start twice must fail; slave fd should be consumed")
	}
	_ = s.Close() // EOF the drain goroutine
	<-done
}

// TestSession_concurrentStartClose_noRace stresses the abandon-vs-Close surface
// (review M1): the run watchdog calls Close() while an in-flight Start may still
// be in cmd.Start. With the Session mutex + closed flag, Start's post-exec writes
// and Close's teardown never race and never double-close the slave fd. Run under
// -race; the pre-fix unsynchronized code trips the detector here.
func TestSession_concurrentStartClose_noRace(t *testing.T) {
	before := runtime.NumGoroutine()
	for i := 0; i < 30; i++ {
		s, err := Allocate(80, 24)
		if err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); _ = s.Start([]string{"true"}, nil, "", "") }()
		go func() { defer wg.Done(); _ = s.Close() }()
		wg.Wait()
		_ = s.Close() // idempotent, must not double-close
	}
	// Review F5: the closed-during-start recovery path must Wait its child (kill
	// the group + reap), so no zombie-reaping goroutines / fds leak.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= before+2 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Errorf("goroutine leak after concurrent Start/Close: before=%d now=%d", before, runtime.NumGoroutine())
}

// TestSession_closeBeforeStart pins the deterministic ordering: Close (sets
// closed) before Start ⇒ Start fails cleanly without touching/closing fds Close
// already reclaimed (no double-close, review M1).
func TestSession_closeBeforeStart(t *testing.T) {
	s, err := Allocate(80, 24)
	if err != nil {
		t.Fatal(err)
	}
	_ = s.Close()
	if err := s.Start([]string{"true"}, nil, "", ""); err == nil {
		t.Error("Start after Close must fail")
	}
	_ = s.Close() // idempotent
}

func TestKillAndWaitAfterAbandonedStart_reapsProcessGroup(t *testing.T) {
	s, err := Allocate(80, 24)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	if err := s.Start([]string{"sh", "-c", "sleep 30"}, nil, "", ""); err != nil {
		t.Fatal(err)
	}
	pid := s.OSPID()
	s.KillAndWaitAfterAbandonedStart()
	if err := syscall.Kill(pid, 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("abandoned child still exists after kill+Wait: pid=%d err=%v", pid, err)
	}
}

// ---- h1 D: Hangup ----------------------------------------------------------
// origin: docs/reviews/h1-plan.md workstream D (zombie class b).

// TestHangupDeliversSIGHUPToProcessGroup: Hangup must end a default-handler
// child like a real terminal close would, and Wait maps the signal death to
// rc=-1 (pty.Wait's documented mapping — NOT 128+HUP).
func TestHangupDeliversSIGHUPToProcessGroup(t *testing.T) {
	s, err := Allocate(80, 24)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	if err := s.Start([]string{"sleep", "30"}, nil, "", ""); err != nil {
		t.Fatal(err)
	}
	go func() { _, _ = io.Copy(io.Discard, s.Master) }()
	time.Sleep(100 * time.Millisecond)

	s.Hangup()

	done := make(chan int, 1)
	go func() {
		exit, _ := s.Wait()
		done <- exit
	}()
	select {
	case exit := <-done:
		if exit != -1 {
			t.Errorf("exit code on SIGHUP death: got %d want -1 (pty.Wait's signal mapping)", exit)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("child did not exit after Hangup")
	}
}

// TestHangupSparesHUPImmuneChild: a child that traps/ignores HUP keeps
// running — Hangup is "the terminal went away", NOT a kill escalation, and a
// survivor honestly keeps its RUNNING row.
func TestHangupSparesHUPImmuneChild(t *testing.T) {
	s, err := Allocate(80, 24)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	if err := s.Start([]string{"sh", "-c", "trap '' HUP; sleep 30"}, nil, "", ""); err != nil {
		t.Fatal(err)
	}
	go func() { _, _ = io.Copy(io.Discard, s.Master) }()
	time.Sleep(300 * time.Millisecond) // let the trap install

	s.Hangup()
	time.Sleep(300 * time.Millisecond)

	if err := syscall.Kill(s.OSPID(), 0); err != nil {
		t.Fatalf("HUP-immune child died after Hangup (escalation crept in): %v", err)
	}
	// Cleanup: really end it.
	_ = s.Signal(syscall.SIGKILL)
	_, _ = s.Wait()
}

// TestHangupConcurrentMasterIONoRace is the -race regression for the
// closed-in-place contract: Hangup must not nil s.Master while another
// goroutine (run.go's `.in` callback shape) reads/writes it lock-free.
// Mutation check: making Hangup call Close() (which nils the pointer) makes
// this test fail under -race.
func TestHangupConcurrentMasterIONoRace(t *testing.T) {
	s, err := Allocate(80, 24)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	if err := s.Start([]string{"sleep", "5"}, nil, "", ""); err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_, _ = s.Master.Write([]byte("x")) // the lock-free .in shape
			}
		}
	}()
	time.Sleep(50 * time.Millisecond)
	s.Hangup()
	time.Sleep(50 * time.Millisecond)
	close(stop)
	// JOIN before returning. Not politeness: the deferred Close() nils
	// s.Master, and that write races this goroutine's lock-free read — the
	// very hazard Hangup's closed-in-place contract exists to avoid. Leaving
	// the goroutine running would make the test fail on the TEARDOWN race and
	// say nothing about Hangup. (The same narrow window exists in production
	// between run.go's subIn.Unsubscribe and sess.Close; it is pre-existing,
	// unrelated to h1, and out of this increment's scope.)
	wg.Wait()
	if s.Master == nil {
		t.Fatal("Hangup nil'd s.Master — the closed-in-place contract is broken (data race with the .in callback)")
	}
	_, _ = s.Wait()
}
