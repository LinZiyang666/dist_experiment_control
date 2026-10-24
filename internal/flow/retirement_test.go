//go:build linux

package flow

import (
	"errors"
	"runtime"
	"testing"

	"github.com/LinZiyang666/tether/internal/testharness"
)

// origin: hot-update S5 plan §3.2 / T05
func exerciseRetiredOpenCannotDialAfterReceiptGC(t *testing.T) {
	exerciseTerminalReceiptBinding(t)
	m, req, _, cleanup := flowFixture(t)
	defer cleanup()
	if err := m.RevokeFlow(req.ID, ReasonRevoked); err != nil {
		t.Fatal(err)
	}
	if err := CollectReceipt(m, req.ID); err != nil {
		t.Fatal(err)
	}
	if len(m.state.records) != 0 {
		t.Fatal("receipt GC precondition missing")
	}
	if _, err := Open(m, req, nil); !errors.Is(err, &Failure{Reason: ReasonRetired}) {
		t.Fatalf("GC forgot retired OPEN: %v", err)
	}
	r := m.state.retirement[req.EndpointID]
	for seq := uint64(3); seq < 3+MaxRetirementHoles; seq++ {
		r.next = seq + 1
		if err := retire(r, seq); err != nil {
			t.Fatal(err)
		}
	}
	count := len(r.holes)
	if count != MaxRetirementHoles {
		t.Fatal("holes saturation precondition missing")
	}
	if err := retire(r, 3+MaxRetirementHoles); !errors.Is(err, &Failure{Reason: ReasonCapacity}) {
		t.Fatalf("full retirement holes accepted eviction: %v", err)
	}
	if !retired(r, 3) || len(r.holes) != count {
		t.Fatal("capacity failure evicted retirement proof")
	}
	if err := retire(r, 2); err != nil {
		t.Fatal(err)
	}
	if len(r.holes) != 0 || r.through != 2+MaxRetirementHoles {
		t.Fatal("contiguous retirement did not compact")
	}
}

// origin: hot-update S5 internal review rv-flow-1 / rv-flow-V1
func exerciseTerminalReceiptBinding(t *testing.T) {
	t.Helper()
	m, req, _, cleanup := flowFixture(t)
	defer cleanup()
	if err := RebindReceipt(m, req.ID, req.EndpointID, req.Intent, 2, 2); !errors.Is(err, &Failure{Reason: ReasonOwnerConflict}) {
		t.Fatalf("live flow accepted terminal-only binding: %v", err)
	}
	if err := m.RevokeFlow(req.ID, ReasonRevoked); err != nil {
		t.Fatal(err)
	}
	if len(States(m, req.EndpointID)) != 0 || len(m.state.records) != 1 || len(m.state.retirement[req.EndpointID].live) != 0 {
		t.Fatal("settlement did not separate live ownership from retained receipt history")
	}
	badIntent := req.Intent
	badIntent[0]++
	for _, mismatch := range []struct {
		endpoint EndpointID
		intent   [32]byte
	}{{EndpointID{3}, req.Intent}, {req.EndpointID, badIntent}} {
		if err := RebindReceipt(m, req.ID, mismatch.endpoint, mismatch.intent, 2, 2); !errors.Is(err, &Failure{Reason: ReasonIntentConflict}) {
			t.Fatalf("terminal receipt accepted another identity: %v", err)
		}
	}
	if err := RebindReceipt(m, req.ID, req.EndpointID, req.Intent, 2, 2); err != nil {
		t.Fatal(err)
	}
	for _, generations := range [][2]uint64{{1, 2}, {2, 1}, {0, 2}, {2, 0}} {
		if err := RebindReceipt(m, req.ID, req.EndpointID, req.Intent, generations[0], generations[1]); !errors.Is(err, &Failure{Reason: ReasonOwnerConflict}) {
			t.Fatalf("terminal receipt accepted stale/zero generation: %v", err)
		}
	}
	frames, notification, err := Controls(m, req.ID, 2)
	if err != nil || notification != nil || len(frames) != 2 || frames[1].Kind != KindClose || m.state.cfg.Budget.Used() != 0 {
		t.Fatalf("rebound receipt changed byte/resource responsibility: %v", err)
	}
	if _, _, err := Controls(m, req.ID, 1); !errors.Is(err, &Failure{Reason: ReasonOwnerConflict}) {
		t.Fatal("retained receipt allowed old carrier")
	}
	if err := ForgetEndpoint(m, req.EndpointID); err != nil || len(m.state.records) != 0 || len(m.state.retirement) != 0 {
		t.Fatalf("fenced allocation retained settled history: %v", err)
	}
}

// origin: hot-update S5 plan §8 / TestRetiredOpenCannotDialAfterReceiptGC
func TestRetiredOpenCannotDialAfterReceiptGC(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseRetiredOpenCannotDialAfterReceiptGC(t)
	}
	testharness.AssertNoGoroutineLeak(t, "TestRetiredOpenCannotDialAfterReceiptGC", beforeG)
	testharness.AssertNoFDLeak(t, "TestRetiredOpenCannotDialAfterReceiptGC", beforeFD, 0)
}
