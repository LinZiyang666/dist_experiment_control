package pty

import (
	"bytes"
	"io"
	"strings"
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

	if err := s.Start([]string{"sh", "-c", "echo hello-pty"}, nil, ""); err != nil {
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
	if err := s.Start([]string{"sh", "-c", "trap 'exit 130' INT; sleep 30"}, nil, ""); err != nil {
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

	if err := s.Start([]string{"true"}, nil, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Wait(); err != nil {
		t.Fatal(err)
	}
	go func() { _, _ = io.Copy(io.Discard, s.Master) }()

	if err := s.Start([]string{"true"}, nil, ""); err == nil {
		t.Error("Start twice must fail; slave fd should be consumed")
	}
}
