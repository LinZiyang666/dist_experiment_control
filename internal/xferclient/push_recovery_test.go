package xferclient

import (
	"io"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xferworker"
)

type pushRecoveryAuthority struct{}

func (pushRecoveryAuthority) Check(x.Identity, x.AccessKind, x.Deadline) error { return nil }

// The first commit reaches the real worker's grant gate after its prepared
// entry has been frozen and exported. A typed refusal there must retain the
// original invocation just as a lost commit reply does. The first status is
// also unavailable; only the following census releases the cut with Thaw.
// origin: S7 internal review OWNER-RECOVERING-RETRY F1 / hot-update S7 plan §7 / T33
func TestTransferPushRetriesOwnerRecoveringAcrossWorkerFreeze(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exercisePushFrozenOwnerRecovery(t)
	}
	testharness.AssertNoGoroutineLeak(t, "push frozen owner recovery", g)
	testharness.AssertNoFDLeak(t, "push frozen owner recovery", fd, 0)
}

func exercisePushFrozenOwnerRecovery(t *testing.T) {
	t.Helper()
	for _, tier := range []string{"a", "b"} {
		t.Run(tier, func(t *testing.T) {
			f := newClientFixture(t, "push", func(c *Config) {
				c.Intent.Tier = tier
				if tier == "a" {
					c.Intent.ChunkSize = 0
				}
			})
			defer f.close()
			if tier == "a" {
				f.transport.identity.ObjectVersion = ""
			}
			source := f.client.cfg.Source
			if _, err := source.Seek(17, io.SeekStart); err != nil {
				t.Fatal(err)
			}
			physical := clientPhysicalChunks(t, f)
			authority := pushRecoveryAuthority{}
			worker, err := xferworker.NewWorker(x.WorkerConfig{Base: f.client.cfg.Base,
				Identity: x.Identity{AgentInstance: f.transport.identity.AgentInstance}, Authority: authority})
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := worker.Stop(testDeadline(t, 3*time.Second)); err != nil {
					t.Error(err)
				}
			}()
			var original x.Request
			var originalDeadline x.Deadline
			var frozen, thawed bool
			statuses := 0
			f.transport.onRequest = func(r x.Request) (x.Response, x.Attempt, error, bool) {
				attempt := x.Attempt{Sent: true, LogicalID: r.CallID}
				switch r.Op {
				case x.OpPrepare:
					_, err := worker.Prepare(x.Prepare{CallID: r.CallID, Identity: r.Identity, Origin: r.Origin,
						Permit: *r.Permit, Intent: *r.Intent, Inline: r.Inline, Budget: f.transport.status().Budget}, testDeadline(t, time.Second))
					if err != nil {
						t.Fatal(err)
					}
				case x.OpCommit:
					if f.transport.counts[x.OpCommit] == 1 {
						original, originalDeadline = r, f.client.deadline
						cut, err := worker.Freeze(testDeadline(t, time.Second))
						if err != nil {
							t.Fatal(err)
						}
						state, resources, err := cut.Export()
						if err != nil || len(resources) != 0 || len(state.Entries) != 1 || state.Entries[0].Prepare.Identity != r.Identity || !state.Admission.Closed {
							t.Fatalf("cut missed the original prepared responsibility: entries=%d resources=%d err=%v", len(state.Entries), len(resources), err)
						}
						grant := x.CommitGrant{Identity: r.Identity, GrantNonce: "original-grant", ReceiverInstance: r.Identity.AgentInstance, TerminalID: terminalSlot(r.Identity)}
						_, err = worker.Apply(x.Control{Kind: x.ControlGrant, Identity: r.Identity, Grant: &grant}, testDeadline(t, time.Second))
						if err == nil || !strings.Contains(err.Error(), x.CodeOwnerRecovering) {
							t.Fatalf("grant did not hit the frozen worker gate: %v", err)
						}
						frozen = true
						return x.Response{Code: x.CodeOwnerRecovering, Error: err.Error()}, attempt, nil, true
					}
					if !thawed || statuses < 2 || !reflect.DeepEqual(r, original) || f.client.deadline > originalDeadline || !f.transport.started.Equal(f.client.brokerStarted) {
						t.Fatal("commit replay bypassed the cut census or changed the original request/start/deadline")
					}
				case x.OpStatus:
					if frozen && !thawed {
						statuses++
						if statuses == 1 {
							return x.Response{Code: x.CodeOwnerRecovering, Error: "original owner census is frozen"}, attempt, nil, true
						}
						if err := worker.Thaw(x.Bindings{Authority: authority, OwnerGeneration: 1}, testDeadline(t, time.Second)); err != nil {
							t.Fatal(err)
						}
						status, err := worker.Status(original.Identity)
						if err != nil || status.Identity != original.Identity || status.State != x.StatusLive {
							t.Fatalf("thawed owner lost the prepared entry: %+v %v", status, err)
						}
						thawed = true
					}
				}
				return x.Response{}, x.Attempt{}, nil, false
			}
			receipt, err := f.client.Run(testDeadline(t, 5*time.Second))
			if err != nil || receipt != f.transport.terminal() {
				t.Fatalf("%s push stopped at the agent image freeze: receipt=%+v native_rc=%d err=%v", tier, receipt, ExitCode(err), err)
			}
			if !frozen || !thawed || f.transport.counts[x.OpCommit] != 2 || f.transport.counts[x.OpPermit] != 1 || f.transport.counts[x.OpPrepare] != 1 || f.transport.counts[x.OpSource] != 1 {
				t.Fatalf("recovery restarted the original transfer: %v", f.transport.counts)
			}
			if tier == "b" && (f.writers != 1 || f.writer.appends != 3 || f.writer.finalizes != 1 || f.writer.spec.Identity != original.Identity) {
				t.Fatal("commit deferral repeated byte work or changed the fixed object")
			}
			if offset, err := source.Seek(0, io.SeekCurrent); err != nil || offset != 17 || source != f.client.cfg.Source {
				t.Fatalf("recovery rewound or replaced the pinned source: offset=%d err=%v", offset, err)
			}
			assertClientPhysicalChunks(t, f, physical)
		})
	}
}

// Admission refusals and permission/identity failures remain definitive; only
// an already prepared invocation may wait out a recovering owner's cut.
// origin: S7 internal review OWNER-RECOVERING-RETRY F1 / hot-update S7 plan §6.1 / §7
func TestTransferPushOwnerRecoveryPreservesDefinitiveRefusals(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exercisePushRecoveryRefusals(t)
	}
	testharness.AssertNoGoroutineLeak(t, "push recovery refusals", g)
	testharness.AssertNoFDLeak(t, "push recovery refusals", fd, 0)
}

func exercisePushRecoveryRefusals(t *testing.T) {
	t.Helper()
	for _, tc := range []struct {
		op   x.Operation
		code string
		exit int
	}{
		{x.OpPermit, x.CodeOwnerRecovering, 75},
		{x.OpPrepare, x.CodeOwnerRecovering, 75},
		{x.OpCommit, x.CodeNotMember, 77},
		{x.OpCommit, x.CodeIntentConflict, 64},
		{x.OpCommit, x.CodeSessionDeleting, 75},
	} {
		f := newClientFixture(t, "push")
		f.transport.onRequest = func(r x.Request) (x.Response, x.Attempt, error, bool) {
			if r.Op == tc.op {
				return x.Response{Code: tc.code}, x.Attempt{Sent: true}, nil, true
			}
			return x.Response{}, x.Attempt{}, nil, false
		}
		_, err := f.client.Run(testDeadline(t, 3*time.Second))
		if ErrorCode(err) != tc.code || ExitCode(err) != tc.exit || f.transport.counts[tc.op] != 1 || f.transport.counts[x.OpStatus] != 0 {
			t.Fatalf("definitive %s refusal of %s was retried/reclassified: err=%v native_rc=%d calls=%v", tc.code, tc.op, err, ExitCode(err), f.transport.counts)
		}
		f.close()
	}
}

// A recovering owner cannot keep an invocation alive by resetting its five-
// second attempts or original byte deadline. It retains one unresolved commit.
// origin: S7 internal review OWNER-RECOVERING-RETRY F1 / hot-update S7 plan §1.2 / §7
func TestTransferPushOwnerRecoveryStopsAtOriginalDeadline(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exercisePushRecoveryDeadline(t)
	}
	testharness.AssertNoGoroutineLeak(t, "push recovery original deadline", g)
	testharness.AssertNoFDLeak(t, "push recovery original deadline", fd, 0)
}

func exercisePushRecoveryDeadline(t *testing.T) {
	t.Helper()
	f := newClientFixture(t, "push")
	defer f.close()
	deadline := testDeadline(t, 1400*time.Millisecond)
	f.transport.onRequest = func(r x.Request) (x.Response, x.Attempt, error, bool) {
		if r.Op == x.OpCommit {
			return x.Response{Code: x.CodeOwnerRecovering}, x.Attempt{Sent: true}, nil, true
		}
		return x.Response{}, x.Attempt{}, nil, false
	}
	receipt, err := f.client.Run(deadline)
	if receipt.Kind != "" || ErrorCode(err) != x.CodeResultUnknown || ExitCode(err) != 74 || f.client.pending == nil || f.client.pending.Op != x.OpCommit {
		t.Fatalf("unresolved recovering commit lost custody/classification: receipt=%+v native_rc=%d err=%v", receipt, ExitCode(err), err)
	}
	if _, err := f.client.remaining(deadline); err == nil || f.client.deadline > deadline || f.transport.counts[x.OpCommit] < 2 || f.transport.counts[x.OpStatus] == 0 {
		t.Fatalf("recovery escaped/reset its original deadline: client=%d original=%d calls=%v", f.client.deadline, deadline, f.transport.counts)
	}
	if f.writers != 1 || f.writer.appends != 3 || f.writer.finalizes != 1 || f.transport.counts[x.OpPermit] != 1 || f.transport.counts[x.OpPrepare] != 1 {
		t.Fatal("deadline recovery restarted the original byte work")
	}
}
