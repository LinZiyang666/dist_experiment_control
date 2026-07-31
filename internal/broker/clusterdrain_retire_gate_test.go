package broker

import (
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/adminsock"
)

// origin: batch-A A13; renamed from a13_drain_retire_test.go during line-2 closure verification, which
// widened the FILE-name pattern's batch-letter class from a hand-picked `(r|p|d|b|c|g|s)` to `[a-z]`
// and immediately found this file — the same `A`-letter gap that had already been found one level down
// in the function-name pattern. Named for the unit now: the retire gate on cluster drain.
//
// `cluster drain --retire` performed RemoveServer plus a roster row
// delete — an irreversible raft membership change — through the SYNCHRONOUS
// path, which (unlike the OpClusterRetire operation) has no opStillLive TOCTOU
// recheck, no replicable deadline, no BLOCKED escape hatch and no resume.
//
// cmd/tether hardcodes Retire:false, so it was unreachable from any released
// CLI. But "unreachable from our CLI" is not "unreachable": the socket accepted
// the field, so one hand-written adminsock request re-armed the whole thing.
//
// This pins the refusal, and pins that it names the supported alternative —
// a refusal that does not say what to do instead just gets worked around.

func TestDrainRetireIsRefusedOnTheSocket(t *testing.T) {
	b := &clusterAdminBackend{}
	resp := b.handleDrain(adminsock.Request{
		Op:        adminsock.OpClusterDrain,
		NodeID:    "brk-2",
		Retire:    true,
		Confirmed: true,
	})

	if resp.OK {
		t.Fatal("the socket accepted drain --retire; the unprotected irreversible path is reachable again")
	}
	if resp.Code != adminsock.CodeBadRequest {
		t.Errorf("refusal code = %q, want %q (so automation branches on the kind, not on prose)",
			resp.Code, adminsock.CodeBadRequest)
	}
	if !strings.Contains(resp.Error, "cluster retire") {
		t.Errorf("refusal %q does not name `cluster retire`; an operator told only 'no' will reach for "+
			"the socket directly", resp.Error)
	}
}

// TestDrainWithoutRetireIsNotRefusedByTheRetireGate is the non-vacuity half. A
// refusal test that would also pass if drain were broken outright proves
// nothing, so this asserts the gate is NARROW: with Retire:false the same bare
// backend must get PAST the refusal.
//
// It proves that by the only signal a dependency-free backend can give — it
// panics on the nil ClusterAdmin further down. Reaching that panic IS the
// evidence that control flow crossed the gate; the retire case above returns
// cleanly from the same zero-value receiver precisely because it stops first.
func TestDrainWithoutRetireIsNotRefusedByTheRetireGate(t *testing.T) {
	b := &clusterAdminBackend{}
	req := adminsock.Request{
		Op:        adminsock.OpClusterDrain,
		NodeID:    "brk-2",
		Retire:    false,
		Confirmed: true,
	}

	crossedTheGate := func() (crossed bool) {
		defer func() {
			if recover() != nil {
				crossed = true // reached the nil ClusterAdmin, i.e. past the gate
			}
		}()
		resp := b.handleDrain(req)
		if strings.Contains(resp.Error, "no longer served on this socket") {
			t.Errorf("plain drain hit the retire refusal: %q — the gate is too wide", resp.Error)
			return false
		}
		return true
	}()

	if !crossedTheGate {
		t.Error("a non-retire drain did not get past the retire gate")
	}
}
