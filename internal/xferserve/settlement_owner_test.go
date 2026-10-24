package xferserve

import (
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/testharness"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
)

// These ports exercise resolution only. Unsupported mutation methods cannot
// return fake success; the real tracker/finalizer cut runs in the N3 regression.
type settlementTracker struct{ x.TrackerPort }

func (settlementTracker) Abandon(x.Identity, x.Deadline) (x.Status, error) {
	return x.Status{}, errors.New("mutation is outside the resolution fixture")
}

type settlementFinalizer struct{ x.FinalizerPort }

// Resolution cannot convert a deadline, prior incarnation, incomplete graph
// or read-only remote authority into local Cancel/Abandon permission.
// origin: S7 internal review s7-p9term-request F2 / hot-update S7 plan §6.4 / §8.2
func TestTransferSettlementOwnerRefusesIncompleteAuthority(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("round", func(t *testing.T) { exerciseSettlementOwnerRefusal(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "settlement owner authority", g)
	testharness.AssertNoFDLeak(t, "settlement owner authority", fd, 0)
}

func exerciseSettlementOwnerRefusal(t *testing.T) {
	origin := x.Origin{BrokerID: "original", InstanceID: "instance", BootID: "boot", Generation: 1}
	view := SettlementView{Origin: origin, Tracker: settlementTracker{}, Finalizer: settlementFinalizer{}}
	for _, reason := range []string{"zero origin", "unbound origin", "foreign broker", "foreign instance", "foreign boot", "foreign generation", "prior roots", "no getter", "wrong graph", "no tracker", "no finalizer", "no abandoner", "expired", "expired lookup"} {
		t.Run(reason, func(t *testing.T) {
			defer func() {
				if err := recover(); err != nil {
					t.Fatalf("%s panicked instead of refusing mutation authority: %v", reason, err)
				}
			}()
			current := view
			requested := origin
			calls := 0
			d := assemblyDeadline(t)
			owners := Owners{Origin: origin, Settlement: func() SettlementView { calls++; return current }}
			switch reason {
			case "zero origin":
				requested = x.Origin{}
			case "unbound origin":
				requested, owners.Origin, current.Origin = x.Origin{}, x.Origin{}, x.Origin{}
			case "foreign broker":
				requested.BrokerID = "foreign"
			case "foreign instance":
				requested.InstanceID = "foreign"
			case "foreign boot":
				requested.BootID = "foreign"
			case "foreign generation":
				requested.Generation++
			case "prior roots":
				owners.PreviousRoots = []string{"unreconciled"}
			case "no getter":
				owners.Settlement = nil
			case "wrong graph":
				current.Origin.InstanceID = "retired"
			case "no tracker":
				current.Tracker = nil
			case "no finalizer":
				current.Finalizer = nil
			case "no abandoner":
				current.Tracker = struct{ x.TrackerPort }{view.Tracker}
			case "expired":
				d = 1
			case "expired lookup":
				var err error
				d, err = controlbudget.After(5 * time.Millisecond)
				if err != nil {
					t.Fatal(err)
				}
				owners.Settlement = func() SettlementView {
					calls++
					for {
						if _, err := controlbudget.Remaining(d); err != nil {
							return current
						}
						time.Sleep(time.Millisecond)
					}
				}
			}
			tracker, finalizer, err := owners.SettlementOwner(requested, d)
			if err == nil || tracker != nil || finalizer != nil {
				t.Fatalf("%s returned mutation authority: tracker=%T finalizer=%T err=%v", reason, tracker, finalizer, err)
			}
			if (requested != origin || reason == "prior roots" || reason == "expired") && calls != 0 {
				t.Fatalf("%s borrowed a graph before establishing original authority", reason)
			}
		})
	}
}
