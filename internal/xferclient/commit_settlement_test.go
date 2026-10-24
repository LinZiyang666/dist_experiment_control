package xferclient

import (
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
)

// origin: S7 internal review FIX-CUT F1 / hot-update S7 plan §4 / §7
func TestTransferLostCommitClosedGateResolvesCanonicalTerminal(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseLostCommitClosedGate(t)
	}
	testharness.AssertNoGoroutineLeak(t, "lost commit closed gate", g)
	testharness.AssertNoFDLeak(t, "lost commit closed gate", fd, 0)
}

func exerciseLostCommitClosedGate(t *testing.T) {
	t.Helper()
	for _, kind := range []string{"complete", "failed", "unresolved"} {
		t.Run(kind, func(t *testing.T) {
			f := newClientFixture(t, "push")
			defer f.close()
			f.transport.onRequest = func(r x.Request) (x.Response, x.Attempt, error, bool) {
				attempt := x.Attempt{Sent: true, LogicalID: r.CallID}
				switch r.Op {
				case x.OpCommit:
					if f.transport.counts[x.OpCommit] == 1 {
						return x.Response{}, attempt, errors.New("accepted commit reply dropped at receive edge"), true
					}
					// The receiver's closed gate rejects a repeated grant after its
					// primitive. Its canonical outcome can still be awaiting history.
					return x.Response{Code: x.CodeCanceled, Error: "receiver gate closed"}, attempt, nil, true
				case x.OpStatus:
					s := f.transport.status()
					s.Phase = x.PhaseCommitReserved
					if f.transport.counts[x.OpCommit] > 1 && kind != "unresolved" {
						receipt := f.transport.terminal()
						if kind == "failed" {
							receipt.Kind, receipt.Code = "failed", x.CodeCanceled
						}
						s.State, s.Phase, s.Terminal = x.StatusTerminal, x.PhaseSettled, &receipt
					}
					return x.Response{Status: &s}, attempt, nil, true
				}
				return x.Response{}, x.Attempt{}, nil, false
			}
			receipt, err := f.client.Run(testDeadline(t, 2*time.Second))
			switch kind {
			case "complete":
				if err != nil || receipt != f.transport.terminal() {
					t.Fatalf("lost commit fabricated cancellation instead of retained complete: receipt=%+v code=%s native_rc=%d err=%v",
						receipt, ErrorCode(err), ExitCode(err), err)
				}
			case "failed":
				if receipt.Kind != "failed" || receipt.Code != x.CodeCanceled || ErrorCode(err) != x.CodeCanceled || ExitCode(err) != 64 {
					t.Fatalf("closed gate bypassed canonical failed receipt: receipt=%+v native_rc=%d err=%v", receipt, ExitCode(err), err)
				}
			case "unresolved":
				if receipt.Kind != "" || ErrorCode(err) != x.CodeResultUnknown || ExitCode(err) != 74 || f.client.pending == nil || f.client.pending.Op != x.OpCommit {
					t.Fatalf("closed gate guessed an unresolved outcome or lost commit custody: receipt=%+v err=%v", receipt, err)
				}
			}
			if f.writers != 1 || f.transport.counts[x.OpPermit] != 1 || f.transport.counts[x.OpPrepare] != 1 || f.transport.counts[x.OpCommit] < 2 {
				t.Fatal("fixture missed commit replay or client restarted byte work")
			}
			commits := 0
			for i, op := range f.transport.trace {
				if op == x.OpCommit {
					commits++
				}
				if op == x.OpCommit && commits > 1 && f.transport.trace[i-1] != x.OpStatus {
					t.Fatalf("commit replay skipped original-owner status: %v", f.transport.trace)
				}
			}
		})
	}
}
