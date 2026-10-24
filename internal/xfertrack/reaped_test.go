package xfertrack

import (
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
)

// A watchdog-ended row is remembered after its own retention: Status keeps
// answering result_unknown (not "never created") and the image exports the
// memory; a receiver-reported terminal leaves no such memory, and the memory
// itself expires after xferRecentlyReapedTTL.
// origin: hot-update S7 plan §5 / §8.3 / T20 — lead follow-up 2 item 3 (Reaped had no producer)
func TestTransferWatchdogReapedIdentityOutlivesItsRow(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("round-"+strconv.Itoa(round), func(t *testing.T) { exerciseWatchdogReapedIdentity(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "transfer activation", g)
	testharness.AssertNoFDLeak(t, "transfer activation", fd, 0)
}

func exerciseWatchdogReapedIdentity(t *testing.T) {
	t.Helper()
	for _, tc := range []struct {
		name     string
		watchdog bool
	}{{"watchdog ends the entry", true}, {"receiver reports before the budget", false}} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, "b", "push")
			f.admit(t)
			grant := f.reserve(t)
			id := f.permit.Identity
			if tc.watchdog {
				f.setResolver(t, func(x.Identity, x.Deadline) (x.ReceiverOutcome, error) {
					return x.ReceiverOutcome{Grant: grant, Outcome: x.OutcomeNotEntered}, nil
				})
				row := f.ledger.row(id)
				f.clock.advance(time.Duration(row.BudgetSeal.EffectiveBudgetNS) + time.Second)
				f.owner.watchdogPass(f.clock.deadline())
			} else if _, err := f.owner.Finish(id, f.outcome(grant), f.clock.deadline()); err != nil {
				t.Fatal(err)
			}
			row := f.ledger.row(id)
			if row.CanonicalTerminal == nil {
				t.Fatal("no terminal was decided")
			}
			view, err := Decisions(f.owner)
			if err != nil {
				t.Fatal(err)
			}
			cleanup := x.CleanupReceipt{Identity: row.Identity, OwnerGeneration: 1, WriterRetired: true, ConsumersRetired: true, FenceSequence: 1,
				ObjectPurged: true, TmpUnlinked: true, DirectorySynced: true, Digest: "cleanup-proof"}
			if err = view.AcceptCleanup(key(id), row.EntryNonce, row.Revision, cleanup, f.clock.deadline()); err != nil {
				t.Fatal(err)
			}
			row = f.ledger.row(id)
			if err = f.ledger.Retire(key(id), row.EntryNonce, x.RetirementProof{Revision: row.Revision}, f.clock.deadline()); err != nil {
				t.Fatal(err)
			}
			f.clock.advance(2 * xferRecentlyReapedTTL)
			f.owner.watchdogPass(f.clock.deadline())
			if f.hasEntry(t, id) {
				t.Fatal("settled row outlived its retention")
			}
			status, err := f.owner.Status(x.StatusRequest{Identity: id, Origin: row.Origin}, f.clock.deadline())
			if err != nil || status.State != x.StatusUnknown {
				t.Fatalf("aged-out status = %+v %v", status, err)
			}
			if remembered := status.Code == x.CodeResultUnknown; remembered != tc.watchdog {
				t.Fatalf("reaped memory = %v (code %q), want %v", remembered, status.Code, tc.watchdog)
			}
			frozen, err := f.owner.Freeze(f.clock.deadline())
			if err != nil {
				t.Fatal(err)
			}
			state, _, err := frozen.Export()
			if err != nil {
				t.Fatal(err)
			}
			if want := map[bool]int{true: 1, false: 0}[tc.watchdog]; len(state.Reaped) != want {
				t.Fatalf("exported reaped = %+v, want %d", state.Reaped, want)
			}
			if tc.watchdog {
				reaped := state.Reaped[0]
				if reaped.Identity != id || reaped.Retention.Identity != id || reaped.Retention.Deadline.BootID != "boot" ||
					reaped.Retention.ResultUntil.Sub(reaped.At) != xferRecentlyReapedTTL {
					t.Fatalf("exported reaped entry = %+v", reaped)
				}
				// The image round trip keeps the memory until its own deadline.
				restored, err := Import(f.owner.cfg, state)
				if err != nil {
					t.Fatal(err)
				}
				imported := restored.(*owner)
				if len(imported.state.Reaped) != 1 {
					t.Fatal("import dropped the reaped memory")
				}
				f.clock.advance(xferRecentlyReapedTTL)
				imported.pruneReaped()
				if len(imported.state.Reaped) != 0 {
					t.Fatal("reaped memory outlived xferRecentlyReapedTTL")
				}
				if err := restored.Stop(f.clock.deadline()); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
