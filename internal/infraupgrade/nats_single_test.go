package infraupgrade

import (
	"errors"
	"slices"
	"testing"

	c "github.com/LinZiyang666/tether/internal/infracontract"
)

// Both cases have standalone JS; adding a sole tether voter must not select
// LDM or make a temporary broker/NATS peer a prerequisite.
// origin: hot-update S9 plan §4.3 / T30
func TestNATSSingleUsesStopWithoutLDM(t *testing.T) {
	for _, topology := range []string{"default_single", "sole_voter_standalone_JS"} {
		t.Run(topology, func(t *testing.T) {
			f := newNATSFixture(t, true)
			status, err := f.driver.Advance(f.plan, f.deadline, nil)
			if err != nil || status.Phase != c.Completed {
				t.Fatalf("single advance: %+v %v", status, err)
			}
			if f.effects[c.SignalLDM] != 0 || f.effects[c.SignalStop] != 1 || f.effects[c.ReleaseStart] != 1 {
				t.Fatal("standalone used LDM, hidden restart, or sibling")
			}
			f.before(t, "install.outbound.2", "host.stop")
			f.before(t, "host.release_start", "recovery.verify")
			f.before(t, "recovery.verify", "transfer.rebind")
			f.before(t, "recovery.release", "ready.after")
			f.before(t, "ready.after", "cohort.move")
			for _, request := range f.requests {
				if request.Store != (c.Store{}) && request.Store != f.plan.Store {
					t.Fatal("standalone changed its store")
				}
			}
		})
	}
	t.Run("N2_quorum_refusal", func(t *testing.T) {
		f := newNATSFixture(t, false)
		f.fail["ready.before.1"] = 1
		status, err := f.driver.Advance(f.plan, f.deadline, nil)
		if err == nil || status.Phase != c.Deferred {
			t.Fatalf("N2 refusal bypassed: %+v %v", status, err)
		}
		f.noSignals(t)
		natsAssertSanitized(t, status, err)
	})
	t.Run("ACK_loss_keeps_repair_closed", func(t *testing.T) {
		f := newNATSFixture(t, true)
		f.fail["recovery.verify"] = 1
		status, err := f.driver.Advance(f.plan, f.deadline, nil)
		if err == nil || status.Phase != c.Halted || !f.recoveryHeld {
			t.Fatalf("ACK loss did not halt behind gate: %+v %v", status, err)
		}
		if slices.Contains(f.events, "transfer.rebind") || slices.Contains(f.events, "ready.after") {
			t.Fatal("replay or business readiness ran before ACK oracle")
		}
		natsAssertSanitized(t, status, err)
	})
	t.Run("current_state_rollback_accepts_original_SHA", func(t *testing.T) {
		f := newNATSFixture(t, true)
		f.plan.Action = c.Rollback
		f.plan.Candidate, f.plan.Previous = f.plan.Previous, f.plan.Candidate
		f.plan.Before.ExeSHA256 = f.plan.Previous.BinarySHA256
		f.current.ExeSHA256 = f.plan.Candidate.BinarySHA256
		status, err := f.driver.Advance(f.plan, f.deadline, nil)
		if err != nil || status.Phase != c.Completed || status.Current.ExeSHA256 != f.plan.Candidate.BinarySHA256 {
			t.Fatalf("current-store rollback: %+v %v", status, err)
		}
		if countEvent(f.events, "artifact.compatible") != 2 || countEvent(f.events, "artifact.verify") != 2 {
			t.Fatal("rollback skipped verified current-state compatibility in either direction")
		}
	})
	for _, defect := range []string{"cancelled", "expired", "wrong_component", "reload", "unknown_before", "previous_mismatch", "unknown_store"} {
		t.Run(defect, func(t *testing.T) {
			f := newNATSFixture(t, true)
			var stop chan struct{}
			switch defect {
			case "cancelled":
				stop = make(chan struct{})
				close(stop)
			case "expired":
				f.deadline = 1
			case "wrong_component":
				f.plan.Component = c.Caddy
			case "reload":
				f.plan.Action = c.Reload
			case "unknown_before":
				f.plan.Before.StartTicks = 0
			case "previous_mismatch":
				f.plan.Previous.BinarySHA256[0]++
			case "unknown_store":
				f.plan.Store.LockInode = 0
			}
			status, err := f.driver.Advance(f.plan, f.deadline, stop)
			if err == nil || status.Phase != c.Deferred || len(f.events) != 0 {
				t.Fatalf("invalid plan had effects: %+v %v %v", status, err, f.events)
			}
			if defect == "reload" && !errors.Is(err, c.ErrUnsupported) {
				t.Fatal("NATS reload must be unsupported")
			}
		})
	}
}
