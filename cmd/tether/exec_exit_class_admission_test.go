package main

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/execclient"
	"github.com/LinZiyang666/tether/internal/proto"
)

// exec_exit_class_admission_test.go — the exit class of a reliable exec whose
// CREATE the agent's admission table positively refused.
//
// procrequest.Registry.Reserve returns ErrCapacity only when no record exists
// for the handle (an existing record is returned before the capacity check),
// so `process_capacity` in reply to a create — original or resent — means
// "no task was admitted for this request": nothing ran, nothing will run, and
// the same command is safe to retry once the table drains. error_hints.go
// classifies it 75 and its comment says it "clears on its own"; the
// unresolvedCodeSites reason for handleProcessResumeForwarded says the same.
// But the code can only ever arrive AFTER the create was handed to the
// transport (it is the create's own reply), and classifyReliableFailure's
// phase rule turns every post-create refusal but 77 into 74 with the prose
// "the remote task was accepted and its result is unknown" — the one class
// usage §9.13 tells automation never to retry. The 75 entry is dead and the
// message is false.
//
// origin: hot-update S1 internal review, ctl lane (plan §1.1 table vs §1.6 /
// §2.1 `process_capacity → 75`; usage §9.13.2 "重试一条从未启动的命令是安全的").
func TestReliableExecAdmissionRefusalKeepsRetryableClass(t *testing.T) {
	handle := proto.ProcessHandle{RequestID: "req-1"}
	result := execclient.Result{Handle: handle, CreateSent: true}
	refusal := &execclient.ProtocolError{Code: proto.CodeProcessCapacity,
		Message: "process request: admission capacity exhausted", AfterCreate: true}
	err := classifyReliableFailure("agt1", 2*time.Second, result, refusal)
	if err == nil {
		t.Fatal("nil error")
	}
	want, tabled := brokerCodeExitClasses[proto.CodeProcessCapacity]
	if !tabled {
		t.Fatalf("%s has no entry in brokerCodeExitClasses", proto.CodeProcessCapacity)
	}
	if got := classifyExit(err); got != want {
		t.Errorf("process_capacity refusal of the create exits %d; the table promises %d (retry-safe: no task was admitted): %v", got, want, err)
	}
	var xe *ExitError
	if !errors.As(err, &xe) || xe.Code != proto.CodeProcessCapacity {
		t.Errorf("wire code not carried structurally: %+v", err)
	}
	if strings.Contains(err.Error(), "accepted") {
		t.Errorf("a refused admission is presented as an accepted task: %q", err.Error())
	}
}
