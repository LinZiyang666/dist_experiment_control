//go:build linux

package flow

import (
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
)

// origin: hot-update S5 internal review SS-REVOKE-1
func TestHardRevokeHoldsFinalControlUntilRawWorkersJoin(t *testing.T) {
	g0, fd0 := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("round", func(t *testing.T) { exerciseHardRevokeControlBoundary(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "hard revoke control boundary", g0)
	testharness.AssertNoFDLeak(t, "hard revoke control boundary", fd0, 0)
}

// origin: hot-update S5 internal review SS-REVOKE-1
func TestTerminalReceiptPublishesOnlyReleasedReplay(t *testing.T) {
	g0, fd0 := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseTerminalReplayPublication(t)
	}
	testharness.AssertNoGoroutineLeak(t, "terminal replay publication", g0)
	testharness.AssertNoFDLeak(t, "terminal replay publication", fd0, 0)
}

func exerciseTerminalReplayPublication(t *testing.T) {
	t.Helper()
	for sample := 0; sample < 100; sample++ {
		t.Run("terminal", func(t *testing.T) {
			m, req, _, cleanup := flowFixture(t)
			defer cleanup()
			observed, ready, stop := make(chan bool, 1), make(chan struct{}), make(chan struct{})
			defer close(stop)
			go func() {
				close(ready)
				for {
					state, _, err := Observe(m, req.ID)
					if err != nil {
						observed <- false
						return
					}
					if state.Phase == FlowPhaseTerminal {
						observed <- m.state.cfg.Budget.Used() == 0
						return
					}
					select {
					case <-stop:
						return
					default:
					}
				}
			}()
			<-ready
			closeRecord(m.state.records[req.ID])
			select {
			case released := <-observed:
				if !released {
					t.Fatal("terminal receipt still owned replay capacity")
				}
			case <-time.After(time.Second):
				t.Fatal("terminal observer did not join")
			}
		})
	}
}

func exerciseHardRevokeControlBoundary(t *testing.T) {
	t.Helper()
	m, req, peer, cleanup := flowFixture(t)
	defer cleanup()
	if _, err := peer.Write([]byte("unreceipted-source")); err != nil {
		t.Fatal(err)
	}
	state := waitFlow(t, m, req.ID, func(s State) bool { return s.TxEnd == uint64(len("unreceipted-source")) })
	r := m.state.records[req.ID]
	r.mu.Lock()
	// Hold the exact published authorization boundary while raw workers have
	// not yet joined. The connector/source can still commit its last syscall n.
	r.revoked, r.cleanupPending = true, true
	r.reason, r.closeMode = ReasonRevoked, CloseModeRevoke
	r.mu.Unlock()
	frames, notification, err := Controls(m, req.ID, req.CarrierGen)
	if err != nil {
		t.Fatal(err)
	}
	// The merged CLOSE guard also withholds final ends. Prove the broader
	// safe point independently: even an ACK snapshot must wait for raw join.
	// origin: hot-update S5 plan §6.5 / T21 — integration mutation replay.
	if len(frames) != 0 || notification == nil {
		t.Error("hard revoke exposed controls before joining raw workers")
	}
	for _, f := range frames {
		if f.Kind == KindClose {
			t.Error("hard revoke published final ends before joining raw workers")
		}
	}
	if err := Receive(m, incoming(req, KindACK, state.TxEnd, nil)); err != nil {
		t.Error("in-flight valid ACK fenced a revoking carrier", err)
	}
	if err := Receive(m, incoming(req, KindACK, state.TxEnd+1, nil)); err == nil {
		t.Error("revoking flow accepted an impossible applied offset")
	}
	if err := Receive(m, incoming(req, KindFIN, 0, nil)); err != nil {
		t.Error("in-flight valid FIN fenced a revoking carrier", err)
	}
	if err := Receive(m, incoming(req, KindFIN, 0, []byte("invalid"))); err == nil {
		t.Error("revoking flow accepted a malformed FIN")
	}
	if held, _, err := Observe(m, req.ID); err != nil || held.TxBase != state.TxBase || held.FINFlags&2 != 0 {
		t.Error("in-flight control changed revoked byte or FIN accounting", err)
	}
	if err := Receive(m, incoming(req, KindData, 0, []byte("forbidden"))); !errors.Is(err, &Failure{Reason: ReasonRetired}) {
		t.Error("revoking flow accepted target plaintext", err)
	}
	if err := m.RevokeFlow(req.ID, ReasonRevoked); err != nil {
		t.Fatal(err)
	}
	final, _, err := Observe(m, req.ID)
	if err != nil || final.FinalEnd[0] != state.TxEnd || final.FinalApplied[0] != state.TxBase {
		t.Fatal("hard revoke lost actual final offsets", err)
	}
	r.mu.Lock()
	pending := r.cleanupPending
	r.mu.Unlock()
	if pending {
		t.Fatal("completed revoke retained an unfinished cleanup responsibility")
	}
}
