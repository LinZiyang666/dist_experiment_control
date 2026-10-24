//go:build linux

package tunnel

import (
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/flow"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// origin: hot-update S5 internal review rv-flow-3
func TestEndpointPermitSupportsSuccessiveOwnerGenerations(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("round", func(t *testing.T) { exerciseSuccessiveOwnerPermits(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "successive owner permits", beforeG)
	testharness.AssertNoFDLeak(t, "successive owner permits", beforeFD, 0)
}

func exerciseSuccessiveOwnerPermits(t *testing.T) {
	t.Helper()
	f := settlementSetup(t)
	for owner := uint64(1); owner <= 3; owner++ {
		cut := [16]byte{byte(owner)}
		if err := f.client.PrepareHandoff(owner+1, cut, time.Now().Add(ResumeAttempt)); err != nil {
			t.Fatalf("PREPARE %d -> %d: %v", owner, owner+1, err)
		}
		f.client.flowState.mu.Lock()
		permit := f.peer.permit
		f.client.flowState.mu.Unlock()
		if permit == nil || permit.owner != owner || permit.next != owner+1 || permit.cutID != cut || permit.consumed {
			t.Fatalf("client retained predecessor permit for owner %d", owner)
		}
		// Repeating this PREPARE must retain its original deadline and identity.
		if err := f.client.PrepareHandoff(owner+1, cut, time.Now().Add(ResumeAttempt)); err != nil {
			t.Fatal(err)
		}
		f.client.flowState.mu.Lock()
		if f.peer.permit != permit {
			f.client.flowState.mu.Unlock()
			t.Fatal("PREPARE retry replaced its live permit")
		}
		activate := resumeRequest{Scope: flow.ResumeScopeEndpoint, Mode: flow.ResumeModeActivate, RequestID: permit.requestID, Endpoint: f.peer.id, Owner: owner, Next: owner + 1, Cut: cut, Digest: [32]byte{byte(owner + 10)}, Target: f.peer.target, Ticket: append([]byte(nil), f.peer.ticket...)}
		f.peer.request = activate
		old := f.peer.attachment
		f.client.flowState.mu.Unlock()
		old.Close()
		old.Join()
		if err := resumeClientEndpoint(f.client, f.peer, time.Now().Add(ResumeAttempt)); err != nil {
			t.Fatal(err)
		}
		f.client.flowState.mu.Lock()
		got, consumed := f.peer.owner, f.peer.permit.consumed
		f.client.flowState.mu.Unlock()
		if got != owner+1 || !consumed {
			t.Fatal("ACTIVATE did not install the successor owner")
		}
		// A distinct request from the predecessor cannot consume another permit.
		activate.RequestID[15]++
		if _, _, err := resumeEndpoint(f.server, f.endpoint, activate, nil, time.Now().Add(ResumeAttempt), nil); err == nil {
			t.Fatal("old owner activated again")
		}
	}
}
