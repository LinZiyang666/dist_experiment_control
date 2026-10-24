package infraupgrade

import (
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"slices"
	"strings"
	"testing"

	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/infracontract/contractstub"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// An injected BOOTTIME clock crosses the actual 120s constant without a two
// minute sleep. This proves driver behavior, not stock Caddy's serving lifetime.
// origin: hot-update S9 plan §5.4 / §7.2 / T23 / M12
func TestCaddyDeadlineKeepsServingShadow(t *testing.T) {
	t.Run("artifact_time_is_outside_handover", func(t *testing.T) {
		h := newCaddyFixture(t, c.Upgrade)
		h.verifyDelay = int64(caddyHandoverBudget)
		status, err := h.driver.Advance(h.plan, h.deadline(), nil)
		if err != nil || status.Phase != c.Completed || h.saved.TimedOut {
			t.Fatalf("artifact validation incorrectly consumed component handover: %v", err)
		}
	})
	for _, op := range []c.HostOp{c.StartWarm, c.SwitchIngress, c.SignalGraceful, c.FinalizeStorage, c.ReleaseStart} {
		t.Run(fmt.Sprintf("deadline_after_%d", op), func(t *testing.T) {
			h := newCaddyFixture(t, c.Upgrade)
			h.after = func(applied c.HostOp) {
				if applied == op {
					h.now += int64(caddyHandoverBudget) + 1
					h.after = nil
				}
			}
			status, err := h.driver.Advance(h.plan, h.deadline(), nil)
			if err == nil || status.Phase != c.Halted || status.Code != "infra_budget_exceeded" || !h.saved.TimedOut {
				t.Fatalf("overrun did not stay red: %+v / %v", status, err)
			}
			if !h.warmLive || h.applied[c.RetireWarm] != 0 || h.returned {
				t.Fatal("component deadline killed or retired the serving shadow")
			}
			if op == c.FinalizeStorage && (h.applied[c.ReleaseStart] != 0 || !h.fenced || h.oldLive) {
				t.Fatal("final storage timeout released startup or invented an old server")
			}
			before := h.applied[op]
			h.newDriver() // Controller restart: all continuation state comes from the journal.
			status, err = h.driver.Recover(h.plan, h.deadline(), nil)
			if err != nil || status.Phase != c.Completed || h.warmLive || !h.saved.TimedOut || h.applied[op] != before && op != c.SwitchIngress {
				t.Fatalf("recovery lost current state or original timeout: phase=%v err=%v", status.Phase, err)
			}
		})
	}
	t.Run("cancel_after_redirect", func(t *testing.T) {
		h := newCaddyFixture(t, c.Upgrade)
		stop := make(chan struct{})
		h.after = func(op c.HostOp) {
			if op == c.SwitchIngress {
				close(stop)
				h.after = nil
			}
		}
		status, err := h.driver.Advance(h.plan, h.deadline(), stop)
		if err == nil || status.Phase != c.Halted || !h.warmLive || !h.oldLive || !h.redirect || h.applied[c.RetireWarm] != 0 {
			t.Fatal("cancellation dismantled the last healthy ingress")
		}
	})
}

// origin: hot-update S9 plan §5.4 / §7.2 / T22 / T23
func TestCaddyRecoveryReusesExactHostRequest(t *testing.T) {
	for _, op := range []c.HostOp{c.Stage, c.FenceStart, c.StartWarm, c.SwitchIngress, c.Promote, c.SignalGraceful, c.FinalizeStorage, c.ReleaseStart, c.RetireWarm} {
		t.Run(fmt.Sprintf("lost_reply_%d", op), func(t *testing.T) {
			h := newCaddyFixture(t, c.Upgrade)
			h.lost = op
			status, err := h.driver.Advance(h.plan, h.deadline(), nil)
			if err == nil || status.Phase != c.Halted || h.applied[op] != 1 || h.saved.Pending == nil {
				t.Fatalf("did not hit actual applied/lost-reply window: %v / %v", status.Phase, err)
			}
			h.newDriver()
			status, err = h.driver.Recover(h.plan, h.deadline(), nil)
			want := 1
			if op == c.SwitchIngress {
				want = 2 // Outbound and return are different, legitimate operations.
			}
			if err != nil || status.Phase != c.Completed || h.applied[op] != want || !slices.Contains(h.trace, "cached") {
				t.Fatalf("lost reply caused a new effect or failed recovery: %v / %v", status.Phase, err)
			}
		})
	}
}

// origin: hot-update S9 plan §2.2 / §7.2 / T22
func TestCaddyRecoveryRejectsCorruptTransition(t *testing.T) {
	for _, field := range []string{"plan", "version", "cursor", "warm", "journal", "request", "receipt"} {
		t.Run(field, func(t *testing.T) {
			h := newCaddyFixture(t, c.Upgrade)
			h.after = func(op c.HostOp) {
				if op == c.FinalizeStorage {
					h.now += int64(caddyHandoverBudget) + 1
					h.after = nil
				}
			}
			if _, err := h.driver.Advance(h.plan, h.deadline(), nil); err == nil {
				t.Fatal("did not reach the recovery window")
			}
			switch field {
			case "plan":
				h.saved.Plan.Step++
			case "version":
				h.saved.Version++
			case "cursor":
				h.saved.Cursor = caddyFinished + 1
			case "warm":
				h.saved.Warm.InvocationID = "wrong-invocation"
			case "journal":
				h.saved.Journal = caddyDigest(99)
			case "request":
				h.saved.Pending.Tx = caddyID(99)
			case "receipt":
				h.saved.Receipt.RequestDigest = caddyDigest(99)
			}
			before := len(h.requests)
			h.newDriver()
			status, err := h.driver.Recover(h.plan, h.deadline(), nil)
			if err == nil || status.Phase == c.Completed || len(h.requests) != before {
				t.Fatal("corrupt recovery state authorized another host effect")
			}
		})
	}
}

// origin: hot-update S9 plan §2.1 / §2.3 / §5.2 / T22 / M12
func TestCaddyDriverRejectsUnboundEvidence(t *testing.T) {
	for _, change := range []string{"incomplete", "empty", "unknown_owner", "late_accept", "wrong_invocation", "extra_retired_owner", "stale_generation"} {
		t.Run(change, func(t *testing.T) {
			h := newCaddyFixture(t, c.Upgrade)
			h.censusHook = func(census *c.Census) {
				if !h.moved {
					return
				}
				switch change {
				case "incomplete":
					census.Complete = false
				case "empty":
					census.Processes = nil
				case "stale_generation":
					census.Generation = 1
				case "unknown_owner":
					census.Sockets = []c.Socket{{Owner: caddyTestProcess(99, caddyDigest(99)), NetNS: 1, Device: 1, Inode: 1, Cookie: 1}}
				case "late_accept":
					if h.applied[c.Promote] != 0 && h.oldLive {
						census.OldOwnerPending = 1
					}
				case "wrong_invocation":
					for i := range census.Processes {
						if census.Processes[i] == h.warm {
							census.Processes[i].InvocationID = "another-invocation"
						}
					}
				case "extra_retired_owner":
					if h.applied[c.RetireWarm] != 0 {
						census.Processes = append(census.Processes, caddyTestProcess(90, h.plan.Previous.BinarySHA256))
					}
				}
			}
			status, err := h.driver.Advance(h.plan, h.deadline(), nil)
			if err == nil || status.Phase == c.Completed {
				t.Fatal("unbound or partial census authorized completion")
			}
			if change == "late_accept" && h.applied[c.SignalGraceful] != 0 {
				t.Fatal("late accepted responsibility was stopped")
			}
		})
	}
	for _, field := range []string{"nonce", "raw", "participant", "target", "health", "remaining", "duplicate"} {
		t.Run("bind_"+field, func(t *testing.T) {
			h := newCaddyFixture(t, c.Upgrade)
			h.bindHook = func(proofs []c.RouteProof) {
				switch field {
				case "nonce":
					proofs[0].Nonce = caddyID(99)
				case "raw":
					proofs[0].New.Raw = proofs[0].Old.Raw
				case "participant":
					proofs[0].Participant = caddyID(99)
				case "target":
					proofs[0].Target.Ingress = h.main
				case "health":
					proofs[0].New.HealthGeneration++
				case "remaining":
					proofs[0].RemainingNS++
				case "duplicate":
					proofs[1] = proofs[0]
				}
			}
			status, err := h.driver.Advance(h.plan, h.deadline(), nil)
			if err == nil || status.Phase == c.Completed || h.applied[c.SignalGraceful] != 0 {
				t.Fatal("unbound route proof authorized cutting the old invocation")
			}
		})
	}
	t.Run("signer_changed_operand", func(t *testing.T) {
		h := newCaddyFixture(t, c.Upgrade)
		h.signHook = func(r *c.HostRequest) { r.PolicyRevision++ }
		_, err := h.driver.Advance(h.plan, h.deadline(), nil)
		if err == nil || len(h.requests) != 0 {
			t.Fatal("signer changed an admitted invocation")
		}
	})
	t.Run("root_reload", func(t *testing.T) {
		h := newCaddyFixture(t, c.Reload)
		h.fault = "root_reload"
		status, err := h.driver.Advance(h.plan, h.deadline(), nil)
		if err == nil || status.Phase != c.Deferred || len(h.requests) != 0 {
			t.Fatal("legacy root admin load was admitted")
		}
	})
	t.Run("missing_adapters", func(t *testing.T) {
		if driver, err := NewCaddy(contractstub.NewDependencies()); driver != nil || !errors.Is(err, c.ErrContractStub) {
			t.Fatal("stub dependencies advertised a Caddy capability")
		}
		h := newCaddyFixture(t, c.Upgrade)
		deps := h.deps()
		var absent *caddyTestHost
		deps.Host = absent
		if driver, err := NewCaddy(deps); driver != nil || !errors.Is(err, c.ErrContractStub) {
			t.Fatal("typed nil dependency advertised a Caddy capability")
		}
	})
	t.Run("private_error", func(t *testing.T) {
		h := newCaddyFixture(t, c.Upgrade)
		h.fault = "secret_error"
		status, err := h.driver.Advance(h.plan, h.deadline(), nil)
		if err == nil || strings.Contains(err.Error(), "canary") || strings.Contains(status.Detail, "canary") {
			t.Fatal("private backend diagnostic escaped the driver boundary")
		}
	})
	t.Run("partial_move", func(t *testing.T) {
		h := newCaddyFixture(t, c.Upgrade)
		h.fault = "partial_move"
		status, err := h.driver.Advance(h.plan, h.deadline(), nil)
		if err == nil || status.Phase == c.Completed || h.saved.Move.Census.Digest != (c.Digest{}) || h.applied[c.SignalGraceful] != 0 {
			t.Fatal("partial errored move was persisted as successful recovery evidence")
		}
	})
}

// Each round keeps a real original pipe and its writer alive across the driver's
// deadline, controller replacement and recovery; cleanup joins that writer. It
// is a business-lifetime oracle only, not a substitute for P29's actual Caddy.
// origin: hot-update S9 plan §12 P10 / T22 / T23
func TestCaddyDriverLifecycle(t *testing.T) {
	goroutines, fds := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseCaddyDriver(t)
	}
	testharness.AssertNoGoroutineLeak(t, "Caddy driver", goroutines)
	testharness.AssertNoFDLeak(t, "Caddy driver", fds, 0)
}

func exerciseCaddyDriver(t *testing.T) {
	t.Helper()
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	write := make(chan byte)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for value := range write {
			if _, err := writer.Write([]byte{value}); err != nil {
				return
			}
		}
	}()
	defer func() {
		close(write)
		_ = reader.Close()
		_ = writer.Close()
		<-done
	}()
	check := func(value byte) {
		t.Helper()
		write <- value
		var data [1]byte
		if _, err := io.ReadFull(reader, data[:]); err != nil || data[0] != value {
			t.Fatalf("original business pipe stopped progressing: %v", err)
		}
	}
	check(1)
	h := newCaddyFixture(t, c.Upgrade)
	h.asyncStart, h.startDelay = true, 2
	h.realWait = true
	h.newDriver()
	h.after = func(op c.HostOp) {
		if op == c.FinalizeStorage {
			h.now += int64(caddyHandoverBudget) + 1
			h.after = nil
		}
	}
	if _, err := h.driver.Advance(h.plan, h.deadline(), nil); err == nil {
		t.Fatal("deadline fault was not hit")
	}
	check(2)
	h.newDriver()
	status, err := h.driver.Recover(h.plan, h.deadline(), nil)
	if err != nil || status.Phase != c.Completed || h.warmLive {
		t.Fatalf("recovery did not retire old responsibilities: %v", err)
	}
	check(3)
}
