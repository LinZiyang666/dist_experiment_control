package xferclient

import (
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
)

// A broker can reply successfully on the ctl connection while its own sent
// prepare has lost the agent's reply. That explicit uncertainty carries the
// same reconciliation duty as a timeout on the ctl connection itself. An
// unready owner's refusal of the replay keeps that duty; a definitive refusal,
// or an unready owner refusing a first attempt that was never forwarded, does not.
// origin: S7 internal review FIX-TIERA2 F1 / hot-update S7 plan §7 / T46
func TestTransferClientReplaysPrepareAfterForwardedReplyLoss(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exercisePrepareReconciliation(t)
	}
	testharness.AssertNoGoroutineLeak(t, "forwarded prepare reconciliation", g)
	testharness.AssertNoFDLeak(t, "forwarded prepare reconciliation", fd, 0)
}

func exercisePrepareReconciliation(t *testing.T) {
	t.Helper()
	for _, code := range []string{x.CodeReconcileRequired, x.CodeResultUnknown} {
		exerciseForwardedPrepareCode(t, []string{code})
	}
	// The broker re-forwards the same prepare while the original agent is still
	// reconnecting after its restart: "not subscribed" refuses only that replay.
	for _, code := range []string{x.CodeOwnerRecovering, x.CodeSessionDeleting, x.CodeUnknown} {
		exerciseForwardedPrepareCode(t, []string{x.CodeReconcileRequired, code})
	}
	for code, exit := range map[string]int{x.CodeNotOwner: 77, x.CodeIntentConflict: 64, x.CodeOwnerRecovering: 75} {
		exerciseForwardedPrepareRefusal(t, nil, code, exit)
	}
	for code, exit := range map[string]int{x.CodeNotOwner: 77, x.CodeIntentConflict: 64} {
		exerciseForwardedPrepareRefusal(t, []string{x.CodeReconcileRequired}, code, exit)
	}
}

func inlinePullFixture(t *testing.T) *clientFixture {
	t.Helper()
	f := newClientFixture(t, "pull", func(c *Config) { c.Intent.Tier = "a"; c.Intent.ChunkSize = 0 })
	f.transport.identity.ObjectVersion = ""
	return f
}

// openPrepareStatus is the original owner's live, still-open entry: the
// prepare was admitted but its agent preparation was never acknowledged.
func openPrepareStatus(f *clientFixture) (x.Response, x.Attempt, error, bool) {
	status := f.transport.status()
	status.Phase = x.PhaseOpen
	return x.Response{Status: &status}, x.Attempt{Sent: true}, nil, true
}

// exerciseForwardedPrepareRefusal: the first len(uncertain) prepares answer
// with uncertainty, the next with a refusal that must end the invocation.
func exerciseForwardedPrepareRefusal(t *testing.T, uncertain []string, code string, exit int) {
	t.Helper()
	f := inlinePullFixture(t)
	defer f.close()
	f.transport.onRequest = func(r x.Request) (x.Response, x.Attempt, error, bool) {
		switch r.Op {
		case x.OpPrepare:
			if n := f.transport.counts[x.OpPrepare]; n <= len(uncertain) {
				return x.Response{Code: uncertain[n-1]}, x.Attempt{Sent: true}, nil, true
			}
			return x.Response{Code: code}, x.Attempt{Sent: true}, nil, true
		case x.OpStatus:
			return openPrepareStatus(f)
		}
		return x.Response{}, x.Attempt{}, nil, false
	}
	_, err := f.client.Run(testDeadline(t, 3*time.Second))
	prepares, statuses := f.transport.counts[x.OpPrepare], f.transport.counts[x.OpStatus]
	if ErrorCode(err) != code || ExitCode(err) != exit || prepares != len(uncertain)+1 || statuses != len(uncertain) || f.atomic.commits != 0 {
		t.Fatalf("after %v, definitive %s refusal was retried or reclassified: err=%v exit=%d prepares=%d statuses=%d primitives=%d",
			uncertain, code, err, ExitCode(err), prepares, statuses, f.atomic.commits)
	}
}

// exerciseForwardedPrepareCode: every prepare in codes answers with that
// typed uncertainty; the next replay of the same request is accepted.
func exerciseForwardedPrepareCode(t *testing.T, codes []string) {
	t.Helper()
	f := inlinePullFixture(t)
	defer f.close()
	var original x.Request
	f.transport.onRequest = func(r x.Request) (x.Response, x.Attempt, error, bool) {
		switch r.Op {
		case x.OpPrepare:
			n := f.transport.counts[x.OpPrepare]
			if n == 1 {
				original = r
			} else {
				if !reflect.DeepEqual(original, r) {
					t.Fatal("prepare replay changed the original request")
				}
				if trace := f.transport.trace; trace[len(trace)-2] != x.OpStatus {
					t.Fatal("prepare replay preceded original-owner status reconciliation")
				}
			}
			if n <= len(codes) {
				return x.Response{Code: codes[n-1], Error: "agent prepare reply lost"}, x.Attempt{Sent: true, Class: "ok"}, nil, true
			}
		case x.OpStatus:
			return openPrepareStatus(f)
		}
		return x.Response{}, x.Attempt{}, nil, false
	}
	receipt, err := f.client.Run(testDeadline(t, 3*time.Second))
	if err != nil || receipt.Kind != "complete" {
		t.Fatalf("forwarded prepare uncertainty %v stopped the original pull: receipt=%+v err=%v", codes, receipt, err)
	}
	if f.transport.counts[x.OpPrepare] != len(codes)+1 || f.transport.counts[x.OpPermit] != 1 || f.transport.dataOpened != 0 || f.atomic.commits != 1 {
		t.Fatalf("replay after %v changed invocation ownership: calls=%v data=%d primitives=%d", codes, f.transport.counts, f.transport.dataOpened, f.atomic.commits)
	}
}
