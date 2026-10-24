package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/execclient"
	"github.com/LinZiyang666/tether/internal/proto"
)

// exec_interrupt_message_test.go — the 74 line printed when the ctl gives up
// on an accepted task must name the cause that actually fired.
//
// origin: hot-update S1 internal review, ctl lane (verifier). exec.go's
// post-create ctx branch renders context.Canceled (Ctrl-C / SIGTERM via
// signal.NotifyContext in main.go) with the same "gave up waiting after
// --timeout 10m0s" prose as context.DeadlineExceeded. An operator who pressed
// Ctrl-C three seconds into a ten-minute exec reads that the timeout fired,
// and a log reader cannot tell an interrupted exec from a timed-out one.
func TestReliableExecInterruptMessageNamesTheCause(t *testing.T) {
	handle := proto.ProcessHandle{RequestID: "req-1"}
	after := execclient.Result{Handle: handle, CreateSent: true}
	canceled := classifyReliableFailure("agt1", 10*time.Minute, after, context.Canceled)
	if canceled == nil {
		t.Fatal("nil error")
	}
	if classifyExit(canceled) != exitUnsettled {
		t.Fatalf("Ctrl-C after create must stay 74: %v", canceled)
	}
	if strings.Contains(canceled.Error(), "--timeout") {
		t.Errorf("an interrupted exec is reported as a --timeout expiry: %q", canceled.Error())
	}
	if !strings.Contains(canceled.Error(), "interrupt") && !strings.Contains(canceled.Error(), "cancel") {
		t.Errorf("an interrupted exec does not say it was interrupted: %q", canceled.Error())
	}
	timedOut := classifyReliableFailure("agt1", 10*time.Minute, after, context.DeadlineExceeded)
	if timedOut == nil || !strings.Contains(timedOut.Error(), "--timeout 10m0s") {
		t.Errorf("a real timeout must still name the --timeout that fired: %v", timedOut)
	}
}
