package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/execclient"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
)

// origin: hot-update S6 internal review rv-ctl-9 / plan §8.2
func TestExecAdmissionExpiryNamesTheAdmissionBudget(t *testing.T) {
	result := execclient.Result{CreateSent: true, Handle: proto.ProcessHandle{RequestID: "original"}}
	err := classifyReliableFailure("node", 10*time.Minute, result, errors.Join(controlbudget.ErrExpired, context.DeadlineExceeded))
	if classifyExit(err) != exitUnsettled || !strings.Contains(err.Error(), "control_result_unknown") || !strings.Contains(err.Error(), "admission expired after 5s") || strings.Contains(err.Error(), "--timeout") {
		t.Fatalf("admission expiry misclassified as a user timeout: %v", err)
	}
	legacy := legacyExecReceiveFailure(true, 10*time.Minute, context.DeadlineExceeded)
	if classifyExit(legacy) != exitUnsettled || !strings.Contains(legacy.Error(), "control_result_unknown") || !strings.Contains(legacy.Error(), "admission expired after 5s") || strings.Contains(legacy.Error(), "--timeout") {
		t.Fatalf("sent legacy task did not keep its admission/unknown boundary: %v", legacy)
	}
	for _, failure := range []error{context.Canceled, nats.ErrConnectionClosed} {
		if err := legacyExecReceiveFailure(true, 10*time.Minute, failure); classifyExit(err) != exitUnsettled || !strings.Contains(err.Error(), "control_result_unknown") {
			t.Fatalf("sent legacy failure lost its unknown-result class: %v", err)
		}
	}
}

// exec_exit_class_test.go — the exit-code taxonomy of the reliable exec path
// (docs/usage.md §9.13.2). The ctl's bounded connect retry is tested beside
// its unit in ctl_connect_test.go.

// origin: docs/reviews/hot-update-reliable-exec-plan.md §1.1 — by PHASE, not
// by prose: once create was sent everything but a permission refusal is 74,
// a settled-but-unclean run is 74 unless the child provably never ran, and
// a pre-create failure keeps its retryable class.
func TestReliableExecFailureClassTable(t *testing.T) {
	handle := proto.ProcessHandle{RequestID: "req-1"}
	for _, tc := range []struct {
		name      string
		result    execclient.Result
		err       error
		wantClass int
		wantCode  string
		wantText  string
	}{
		{"settled_stream_abort_exit0", execclient.Result{Handle: handle, CreateSent: true, Executed: true},
			&execclient.SettledError{Result: execclient.Result{Handle: handle, Executed: true, StdoutAbort: "input/output error", StdoutBytes: 40960}},
			exitUnsettled, "", "stdout aborted after 40960 byte(s)"},
		{"settled_terminal_fault_after_run", execclient.Result{Handle: handle, CreateSent: true},
			&execclient.SettledError{Result: execclient.Result{Handle: handle, Executed: true, TerminalError: "workload: output requires unused replay log", Signal: 9, ExitCode: -1}},
			exitUnsettled, "", "remote exit=-1 signal=9"},
		{"settled_never_started_spawn_code", execclient.Result{Handle: handle, CreateSent: true},
			&execclient.SettledError{Result: execclient.Result{Handle: handle, TerminalError: "remote_fs_spawn_timeout: /mnt hung", ExitCode: -1}},
			exitTransient, "", "remote_fs_spawn_timeout"},
		{"settled_never_started_unknown_code", execclient.Result{Handle: handle, CreateSent: true},
			&execclient.SettledError{Result: execclient.Result{Handle: handle, TerminalError: "exec session: adapter returned a child that is not adopted", ExitCode: -1}},
			exitInternal, "", "not adopted"},
		{"refusal_before_create_permission", execclient.Result{},
			&execclient.ProtocolError{Code: "not_a_member", Message: "not_a_member"}, exitNoPerm, "not_a_member", "not_a_member"},
		{"refusal_before_create_session", execclient.Result{},
			&execclient.ProtocolError{Code: "session_not_found_or_deleting", Message: "session_not_found_or_deleting"}, exitUsage, "session_not_found_or_deleting", ""},
		{"refusal_after_create_permission_stays_77", execclient.Result{Handle: handle, CreateSent: true},
			&execclient.ProtocolError{Code: "not_a_member", Message: "not_a_member", AfterCreate: true}, exitNoPerm, "not_a_member", ""},
		{"refusal_after_create_instance_lost", execclient.Result{Handle: handle, CreateSent: true},
			&execclient.ProtocolError{Code: proto.CodeProcessInstanceLost, Message: "process request: lease was issued by another runtime instance", AfterCreate: true}, exitUnsettled, proto.CodeProcessInstanceLost, "result is unknown"},
		{"refusal_after_create_session_deleted", execclient.Result{Handle: handle, CreateSent: true},
			&execclient.ProtocolError{Code: "session_not_found_or_deleting", Message: "session_not_found_or_deleting", AfterCreate: true}, exitUnsettled, "session_not_found_or_deleting", ""},
		{"refusal_after_create_node_offline", execclient.Result{Handle: handle, CreateSent: true},
			&execclient.ProtocolError{Code: "node_offline", Message: "node_offline: status=OFFLINE", AfterCreate: true}, exitUnsettled, "node_offline", ""},
		// A peer without ProcessResumeReply.Code: the prose prefix keys the
		// class lookup, but no wire code is carried (S1 review C7).
		{"refusal_after_create_legacy_prose_only", execclient.Result{Handle: handle, CreateSent: true},
			&execclient.ProtocolError{Message: "process request: result expired; execution is not retried", AfterCreate: true}, exitUnsettled, "", ""},
		// Refusals that prove nothing was admitted keep their table class even
		// after create was sent (S1 review C2 / ctl-V1); the prose of a typed
		// refusal never decides the class.
		{"refusal_after_create_capacity_is_retryable", execclient.Result{Handle: handle, CreateSent: true},
			&execclient.ProtocolError{Code: proto.CodeProcessCapacity, Message: "process request: admission capacity exhausted", AfterCreate: true}, exitTransient, proto.CodeProcessCapacity, ""},
		{"refusal_after_create_too_large_is_usage", execclient.Result{Handle: handle, CreateSent: true},
			&execclient.ProtocolError{Code: proto.CodeProcessTooLarge, Message: "process request too large", AfterCreate: true}, exitUsage, proto.CodeProcessTooLarge, ""},
		{"refusal_before_create_recovering_is_retryable", execclient.Result{},
			&execclient.ProtocolError{Code: proto.CodeProcessRecovering, Message: "retained recovery"}, exitTransient, proto.CodeProcessRecovering, ""},
		{"refusal_after_create_recovering_is_unsettled", execclient.Result{Handle: handle, CreateSent: true},
			&execclient.ProtocolError{Code: proto.CodeProcessRecovering, Message: "retained recovery", AfterCreate: true}, exitUnsettled, proto.CodeProcessRecovering, "result is unknown"},
		{"refusal_before_create_capacity_is_retryable", execclient.Result{},
			&execclient.ProtocolError{Code: proto.CodeProcessCapacity, Message: "process request: admission capacity exhausted"}, exitTransient, proto.CodeProcessCapacity, ""},
		{"refusal_after_create_runtime_closed_is_unsettled", execclient.Result{Handle: handle, CreateSent: true},
			&execclient.ProtocolError{Code: proto.CodeProcessRuntimeClosed, Message: "exec session: runtime closed", AfterCreate: true}, exitUnsettled, proto.CodeProcessRuntimeClosed, "result is unknown"},
		{"refusal_after_create_unsupported_op_is_unsettled", execclient.Result{Handle: handle, CreateSent: true},
			&execclient.ProtocolError{Code: proto.CodeProcessUnsupportedOp, Message: "unsupported process operation", AfterCreate: true}, exitUnsettled, proto.CodeProcessUnsupportedOp, "result is unknown"},
		{"publish_too_large_before_create_is_usage", execclient.Result{}, nats.ErrMaxPayload, exitUsage, "", "does not fit"},
		{"timeout_before_create", execclient.Result{}, context.DeadlineExceeded, exitTransient, "", "before the command was accepted"},
		{"canceled_before_create", execclient.Result{}, context.Canceled, exitTransient, "", ""},
		{"timeout_after_create_lost_reply_no_pid", execclient.Result{Handle: handle, CreateSent: true}, context.DeadlineExceeded, exitUnsettled, "", "NOT killed"},
		{"canceled_after_create", execclient.Result{Handle: handle, CreateSent: true}, context.Canceled, exitUnsettled, "", "NOT killed"},
		{"connection_closed_after_create", execclient.Result{Handle: handle, CreateSent: true}, nats.ErrConnectionClosed, exitUnsettled, "", "result is unknown"},
		{"connection_closed_before_create", execclient.Result{}, nats.ErrConnectionClosed, exitInternal, "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := classifyReliableFailure("agt1", 2*time.Second, tc.result, tc.err)
			if err == nil {
				t.Fatal("nil error")
			}
			if got := classifyExit(err); got != tc.wantClass {
				t.Fatalf("class = %d, want %d (%v)", got, tc.wantClass, err)
			}
			var xe *ExitError
			if tc.wantCode != "" {
				if !errors.As(err, &xe) || xe.Code != tc.wantCode {
					t.Fatalf("wire code not carried: %+v", err)
				}
			}
			if tc.wantText != "" && !strings.Contains(err.Error(), tc.wantText) {
				t.Fatalf("message %q lacks %q", err.Error(), tc.wantText)
			}
			if tc.wantClass == exitUnsettled && (tc.wantClass == exitUnavailable || tc.wantClass == exitInternal || tc.wantClass == exitTransient) {
				t.Fatal("74 collided with a retryable class")
			}
		})
	}
}

// origin: docs/reviews/hot-update-reliable-exec-plan.md §1.1 — 74 is a
// presentation-layer class: never a value in the pre-accept refusal table,
// never one of the retryable classes, honoured verbatim by the classifier.
func TestExitUnsettledIsPresentationOnly(t *testing.T) {
	if exitUnsettled != 74 {
		t.Fatalf("exitUnsettled = %d, want 74 (EX_IOERR)", exitUnsettled)
	}
	for code, class := range brokerCodeExitClasses {
		if class == exitUnsettled {
			t.Fatalf("brokerCodeExitClasses[%q] = 74: a pre-accept refusal must never claim the accepted-result class", code)
		}
	}
	for _, retryable := range []int{exitUnavailable, exitInternal, exitTransient} {
		if retryable == exitUnsettled {
			t.Fatal("74 must not be one of the retryable classes")
		}
	}
	if got := classifyExit(&ExitError{Class: exitUnsettled, Err: errors.New("x")}); got != exitUnsettled {
		t.Fatalf("classifyExit did not honour the explicit class: %d", got)
	}
}
