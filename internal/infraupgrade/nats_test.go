package infraupgrade

import (
	"errors"
	"reflect"
	"slices"
	"testing"

	"github.com/LinZiyang666/tether/internal/brokercontrol"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/infracontract/contractstub"
	"github.com/LinZiyang666/tether/internal/proto"
)

// These are coordinator state-table tests. The injected owners expose observable
// receipts and failure windows; they do not certify stock binaries, systemd,
// pidfds, quorum, auth, or transport behavior. P28 owns those independent exits.
// origin: hot-update S9 plan §4.2 / §9.2 / T29
func TestNATSCutWaitsForInstalledSuccessors(t *testing.T) {
	t.Run("all_control_and_data_successors_before_LDM", func(t *testing.T) {
		f := newNATSFixture(t, false)
		status, err := f.driver.Advance(f.plan, f.deadline, nil)
		if err != nil || status.Phase != c.Completed {
			t.Fatalf("advance: %v, %+v", err, status)
		}
		f.before(t, "install.outbound.1", "host.ldm")
		f.before(t, "install.outbound.2", "host.ldm")
		f.before(t, "cohort.bind.outbound", "host.ldm")
		f.before(t, "recovery.seal", "host.ldm")
		f.before(t, "ready.before.2", "host.ldm")
		f.before(t, "host.promote", "host.ldm")
		f.before(t, "host.ldm", "host.stop")
		f.before(t, "recovery.verify", "recovery.release")
		f.before(t, "cohort.bind.return", "tx.completed")
		if status.Census.LiveBusiness == 0 || status.Census.OldOwnerPending != 0 {
			t.Fatal("completion waited for business exit or retained old obligations")
		}
	})
	for _, event := range []string{"transfer.fence", "recovery.seal", "cohort.seal", "routes.prepare.outbound",
		"install.outbound.1", "install.outbound.2", "cohort.move", "routes.bind.outbound", "cohort.bind.outbound", "ready.before.2"} {
		t.Run("refuse_"+event, func(t *testing.T) {
			f := newNATSFixture(t, false)
			f.fail[event] = 1
			status, err := f.driver.Advance(f.plan, f.deadline, nil)
			if err == nil || status.Phase != c.Deferred {
				t.Fatalf("missing preparation was not deferred: %+v %v", status, err)
			}
			f.noSignals(t)
			if f.transfersHeld || f.recoveryHeld || f.cohortHeld {
				t.Fatal("short seal remained after defer")
			}
		})
	}
	for _, defect := range []string{"late_data_NC", "rebuilt_consumer", "missing_auth", "missing_WSS", "missing_topology", "missing_groups", "empty_cohort",
		"duplicate_participant", "future_CID", "forwarded_ACK", "unchanged_raw", "wrong_nonce", "wrong_target", "renewed_lease",
		"incomplete_transfer", "changed_transfer", "changed_checkpoint", "changed_initial_runtime"} {
		t.Run(defect, func(t *testing.T) {
			f := newNATSFixture(t, false)
			f.defect = defect
			status, err := f.driver.Advance(f.plan, f.deadline, nil)
			if err == nil || status.Phase != c.Deferred {
				t.Fatalf("invalid preparation admitted: %+v %v", status, err)
			}
			f.noSignals(t)
			if defect == "incomplete_transfer" && slices.Contains(f.events, "recovery.seal") {
				t.Fatal("incomplete transfer admitted the recovery checkpoint")
			}
			if defect == "changed_transfer" && slices.Contains(f.events, "cohort.seal") {
				t.Fatal("mismatched recovery receipts admitted the cohort")
			}
			if defect == "changed_checkpoint" && slices.Contains(f.events, "routes.prepare.outbound") {
				t.Fatal("mismatched checkpoint installed successors")
			}
		})
	}
	t.Run("broker_local_attachment_waits_for_restart", func(t *testing.T) {
		f := newNATSFixture(t, false)
		f.defect = "broker_local_pending"
		status, err := f.driver.Advance(f.plan, f.deadline, nil)
		if err != nil || status.Phase != c.Completed {
			t.Fatalf("local-only broker could not complete: %+v %v", status, err)
		}
		f.before(t, "install.outbound.3", "host.ldm")
		f.before(t, "ready.after", "install.return.3")
	})
	for _, defect := range []string{"receipt_tx", "receipt_sequence", "receipt_op", "receipt_journal", "receipt_digest", "receipt_applied", "CAS_proof", "CAS_revision", "CAS_phase"} {
		t.Run(defect, func(t *testing.T) {
			f := newNATSFixture(t, false)
			f.defect = defect
			status, err := f.driver.Advance(f.plan, f.deadline, nil)
			if err == nil || status.Phase == c.Completed {
				t.Fatalf("unbound receipt accepted: %+v %v", status, err)
			}
			f.noSignals(t)
		})
	}
	t.Run("nil_and_stub_dependencies", func(t *testing.T) {
		if _, err := NewNATS(c.Dependencies{}); !errors.Is(err, c.ErrContractStub) {
			t.Fatalf("nil factory: %v", err)
		}
		f := newNATSFixture(t, false)
		deps := f.dependencies()
		deps.Host = (*natsFixtureHost)(nil)
		if _, err := NewNATS(deps); !errors.Is(err, c.ErrContractStub) {
			t.Fatalf("typed nil factory: %v", err)
		}
		driver, err := NewNATS(contractstub.NewDependencies())
		if err != nil {
			t.Fatal(err)
		}
		status, err := driver.Advance(f.plan, f.deadline, nil)
		if !errors.Is(err, c.ErrContractStub) || status.Phase == c.Completed {
			t.Fatalf("stub advertised success: %+v %v", status, err)
		}
	})
	for _, owner := range []string{"cohort", "recovery", "transfer"} {
		t.Run("missing_partial_handle_"+owner, func(t *testing.T) {
			f := newNATSFixture(t, false)
			f.defect = "partial_" + owner
			status, err := f.driver.Advance(f.plan, f.deadline, nil)
			if err == nil || status.Phase != c.Halted {
				t.Fatalf("unknown partial owner was declared released: %+v %v", status, err)
			}
			if slices.Contains(f.events, owner+".release") {
				t.Fatal("release used a missing ownership identity")
			}
			f.noSignals(t)
		})
	}
}

// origin: hot-update S9 plan §4.2 / §7.2 / T31
func TestNATSHaltedRollAndJoinGate(t *testing.T) {
	for _, event := range []string{"host.fence", "host.promote", "host.ldm", "host.stop", "host.release_start",
		"recovery.verify", "transfer.rebind", "ready.after", "routes.prepare.return", "install.return.2", "cohort.return",
		"routes.bind.return", "cohort.bind.return", "cohort.release", "transfer.release", "tx.completed", "ready.before.3",
		"observe.signal_applied", "observe.old_exit", "observe.startup_released", "observe.return_verified", "observe.completed"} {
		t.Run("halt_"+event, func(t *testing.T) {
			f := newNATSFixture(t, false)
			f.fail[event] = 1
			status, err := f.driver.Advance(f.plan, f.deadline, nil)
			if err == nil || status.Phase != c.Halted {
				t.Fatalf("post-cut failure was not halted: %+v %v", status, err)
			}
			count := len(f.events)
			next := f.plan
			next.Step++
			next.Tx[0]++
			status, err = f.driver.Advance(next, f.deadline, nil)
			if err == nil || status.Phase == c.Completed || len(f.events) != count {
				t.Fatal("halted component rolled the next node")
			}
			if event == "recovery.verify" && slices.Contains(f.events, "recovery.release") {
				t.Fatal("failed ACK oracle opened repair")
			}
			status, err = f.driver.Recover(f.plan, f.deadline, nil)
			if err != nil || status.Phase != c.Completed || status.Code != "" || status.Detail != "" {
				t.Fatalf("same-attempt recovery: %+v %v; %v", status, err, f.events)
			}
			if f.effects[c.SignalLDM] != 1 || f.effects[c.SignalStop] != 1 {
				t.Fatal("recovery issued a second signal")
			}
			if event == "cohort.bind.return" && countEvent(f.events, "cohort.return") != 1 {
				t.Fatal("lost Bind reply caused a second return move")
			}
		})
	}
	for _, defect := range []string{"old_alive", "old_socket", "pending_after_return", "pending_owner", "empty_final_census", "incomplete_census", "wrong_runtime_SHA", "reused_old_PID", "return_missing_attachment", "final_health"} {
		t.Run(defect, func(t *testing.T) {
			f := newNATSFixture(t, false)
			f.defect = defect
			status, err := f.driver.Advance(f.plan, f.deadline, nil)
			if err == nil || status.Phase == c.Completed {
				t.Fatalf("invalid retirement completed: %+v %v", status, err)
			}
			if (defect == "wrong_runtime_SHA" || defect == "reused_old_PID") && f.returned {
				t.Fatal("unverified runtime received returned attachments")
			}
		})
	}
	t.Run("lost_stop_reply_reuses_exact_request", func(t *testing.T) {
		f := newNATSFixture(t, false)
		f.loseReply = c.SignalStop
		status, err := f.driver.Advance(f.plan, f.deadline, nil)
		if err == nil || status.Phase != c.Halted || f.oldAlive {
			t.Fatalf("lost-reply window not exercised: %+v %v", status, err)
		}
		status, err = f.driver.Recover(f.plan, f.deadline, nil)
		if err != nil || status.Phase != c.Completed {
			t.Fatalf("recover lost stop: %+v %v; %v", status, err, f.events)
		}
		var stops []c.HostRequest
		for _, request := range f.requests {
			if request.Op == c.SignalStop {
				stops = append(stops, request)
			}
		}
		if len(stops) != 2 || !reflect.DeepEqual(stops[0], stops[1]) || f.effects[c.SignalStop] != 1 {
			t.Fatal("lost stop reply was not queried by its original identity")
		}
		if f.effects[c.SignalLDM] != 1 {
			t.Fatal("recover repeated LDM")
		}
	})
	t.Run("concurrent_advance_is_bounded_busy", func(t *testing.T) {
		f := newNATSFixture(t, false)
		entered, release := make(chan struct{}), make(chan struct{})
		f.hook = func(name string) {
			if name == "ready.before.1" {
				close(entered)
				<-release
			}
		}
		done := make(chan error, 1)
		go func() { defer close(done); _, err := f.driver.Advance(f.plan, f.deadline, nil); done <- err }()
		<-entered
		status, err := f.driver.Advance(f.plan, f.deadline, nil)
		close(release)
		firstErr := <-done
		if err == nil || status.Code != "infra_busy" || firstErr != nil {
			t.Fatalf("concurrent calls: %v %v %+v", firstErr, err, status)
		}
	})
	t.Run("LDM_self_exit_skips_stop", func(t *testing.T) {
		f := newNATSFixture(t, false)
		f.ldmExits = true
		status, err := f.driver.Advance(f.plan, f.deadline, nil)
		if err != nil || status.Phase != c.Completed || f.effects[c.SignalStop] != 0 {
			t.Fatalf("self exit signaled another owner: %+v %v", status, err)
		}
	})
	t.Run("restart_cannot_invent_missing_checkpoint", func(t *testing.T) {
		f := newNATSFixture(t, false)
		f.fail["recovery.verify"] = 1
		_, _ = f.driver.Advance(f.plan, f.deadline, nil)
		// Recovery must still refuse when the trusted owner loses the original
		// checkpoint; controller replacement alone no longer loses that record.
		// origin: S9 internal review rv-infra F08
		f.transitionBytes = nil
		fresh, err := NewNATS(f.dependencies())
		if err != nil {
			t.Fatal(err)
		}
		count := len(f.requests)
		status, err := fresh.Recover(f.plan, f.deadline, nil)
		if err == nil || status.Phase != c.Halted || len(f.requests) != count {
			t.Fatal("fresh controller fabricated recovery evidence")
		}
	})
	t.Run("recovery_cannot_renew_component_or_route_deadline", func(t *testing.T) {
		for _, expired := range []string{"component", "route"} {
			f := newNATSFixture(t, false)
			f.fail["install.return.2"] = 1
			_, _ = f.driver.Advance(f.plan, f.deadline, nil)
			r := f.driver.(*natsDriver).run
			if expired == "component" {
				r.deadline = 1
			} else {
				r.inbound.deadline = 1
			}
			prepared := countEvent(f.events, "routes.prepare.return")
			status, err := f.driver.Recover(f.plan, f.deadline, nil)
			if expired == "component" {
				// Recovery can retire responsibility without renewing or erasing
				// the failed normal handover. The original deadline remains red.
				// origin: S9 internal review rv-infra F08
				if err != nil || status.Phase != c.Completed || status.Code != "infra_budget_exceeded" || r.deadline != 1 || !r.timedOut || countEvent(f.events, "routes.prepare.return") != prepared {
					t.Fatalf("normal timeout was renewed or recovery could not retire: %v %v", status, err)
				}
				continue
			}
			if err == nil || status.Phase != c.Halted || countEvent(f.events, "routes.prepare.return") != prepared {
				t.Fatalf("expired %s renewed: %+v %v", expired, status, err)
			}
		}
	})
	t.Run("exact_release_join_gate", func(t *testing.T) {
		// This calls the existing production gate, not a replica of its predicate.
		if err := brokercontrol.VersionSkewRefusal(proto.ProtoVersion, proto.ReleaseVersion, "temporary", nil); err != nil {
			t.Fatalf("exact release refused: %v", err)
		}
		for _, release := range []string{"", "9999.0.0"} {
			var skew *brokercontrol.ErrJoinVersionSkew
			if err := brokercontrol.VersionSkewRefusal(proto.ProtoVersion, release, "temporary", nil); !errors.As(err, &skew) {
				t.Fatalf("different release joined: %q %v", release, err)
			}
		}
	})
}
