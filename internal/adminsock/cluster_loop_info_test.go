package adminsock

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// cluster_loop_info_test.go — the ClusterLoopInfo JSON contract.
//
// ⚠ THIS TEST IS RED ON THE TREE IT WAS WRITTEN AGAINST. It is a review artefact demonstrating a
// defect, not a passing gate. See /tmp/tether-b2-review/review-B7.md finding B7-11.
//
// THE DEFECT
// ----------
// Iterations uint64, tagged json:"iterations,omitempty", drops the value 0 — and 0 is the ONLY value
// that carries the diagnosis. The type's own doc says:
//
//	Cadence != 0  periodic. Iters == 0 after startup means the loop returned without ever iterating
//	Cadence == 0  event-driven. Iters == 0 means "nothing happened", NOT dead.
//
// Both of those readings depend on seeing a 0. In the emitted JSON neither field is present for either
// loop, so in `admin runtime --json` and in every export-incident bundle — the consumer this type's
// doc explicitly names — a DEAD periodic loop and a healthy idle event-driven loop are byte-identical.
// That is the same failure the batch-A M8 review fixed for a different field: a machine-readable
// payload that cannot express the distinction its own documentation is built on.
//
// The companion LastIter time.Time, tagged json:"last_iter,omitempty", is the mirror-image bug:
// omitempty has NO effect on a struct, so last_iter is emitted unconditionally as
// "0001-01-01T00:00:00Z". The two neighbouring fields therefore disagree about whether "never" is
// representable.
//
// MUTATION THAT PROVES THIS TEST: remove `,omitempty` from `iterations` and `cadence_ms` in
// protocol.go -> green. Both are additive-shape changes, so docs/usage.md §9.14 needs no bump.
func TestClusterLoopInfoJSONDistinguishesDeadFromIdle(t *testing.T) {
	started := time.Unix(1_700_000_000, 0).UTC()

	// A periodic loop that returned on its first line. runTopologyReconcileLoop does exactly this when
	// NatsConfPath is empty; a wedged loop looks the same. This is the row an operator must be able to
	// find in an incident bundle.
	dead := ClusterLoopInfo{Name: "topology-reconcile", StartedAt: started, CadenceMS: 5000}
	// An event-driven loop that simply had no work. Nothing is wrong with it.
	idle := ClusterLoopInfo{Name: "alert-webhook", StartedAt: started}

	deadJSON, err := json.Marshal(dead)
	if err != nil {
		t.Fatalf("marshal dead: %v", err)
	}
	idleJSON, err := json.Marshal(idle)
	if err != nil {
		t.Fatalf("marshal idle: %v", err)
	}

	// 1. The dead loop's ITERATION COUNT must survive the wire. It is the diagnosis.
	if !strings.Contains(string(deadJSON), `"iterations"`) {
		t.Errorf("a dead periodic loop emits no `iterations` key at all:\n  %s\n"+
			"`omitempty` drops the value 0, which is the ONLY value that means DEAD. An export-incident "+
			"bundle therefore cannot distinguish \"this loop never iterated\" from \"this producer does "+
			"not report iterations\".", deadJSON)
	}

	// 2. And its declared cadence must survive, because that is what makes the 0 readable.
	if !strings.Contains(string(deadJSON), `"cadence_ms"`) {
		t.Errorf("a periodic loop emits no `cadence_ms`:\n  %s", deadJSON)
	}

	// 3. The two rows must not be textually identical modulo the name — that identity IS the defect.
	deadShape := strings.ReplaceAll(string(deadJSON), `"topology-reconcile"`, `"X"`)
	idleShape := strings.ReplaceAll(string(idleJSON), `"alert-webhook"`, `"X"`)
	if deadShape == idleShape {
		t.Errorf("a DEAD periodic loop and an IDLE event-driven loop serialize to the same JSON:\n"+
			"  dead: %s\n  idle: %s\n"+
			"The type's doc builds its whole reading on telling these apart (cadence!=0 && iters==0 => "+
			"dead; cadence==0 && iters==0 => benign), and the wire shape cannot express it.",
			deadJSON, idleJSON)
	}

	// 4. The inert omitempty, stated as its own assertion so the fix does not accidentally leave a
	//    zero-time in the bundle while claiming the field is optional.
	if strings.Contains(string(idleJSON), `"last_iter"`) && strings.Contains(string(idleJSON), "0001-01-01") {
		t.Errorf("`last_iter,omitempty` is inert (encoding/json never omits a struct), so a loop that "+
			"never beat reports last_iter=0001-01-01T00:00:00Z:\n  %s\n"+
			"Either drop the misleading omitempty or make the field a *time.Time / an int64 epoch.", idleJSON)
	}
}
