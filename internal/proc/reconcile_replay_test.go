package proc

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
)

// TestD4ReconcileBatchByteIdenticalReplay (§13.7 #3) proves the OpReconcileBatch entry
// is SELF-SUFFICIENT: any node decoding the SAME committed Command bytes (what raft
// replicates) replays byte-identical audit via the pure ReplayReconcileAudit — and the
// baked Aux is total-ordered, so the replay does NOT depend on the resolver's
// (Go-map) input order. Vacuity controls: shuffling the input yields the SAME Aux
// bytes; stripping Aux yields an empty replay (so a dropped audit blob is detectable).
func TestD4ReconcileBatchByteIdenticalReplay(t *testing.T) {
	now := time.Date(2026, 6, 21, 1, 2, 3, 0, time.UTC)
	rc := 7
	in := ReconcileBatchInput{
		SID: "lab",
		Marks: []ExitMark{
			{PID: "01b", ExitCode: -1, When: now},
			{PID: "01a", ExitCode: 5, When: now},
		},
		ProcAudit: []ReconProcAudit{
			{NID: "n1", PID: "01z", Kind: "killed_orphan", RC: nil, Ts: now},
			{NID: "n1", PID: "01a", Kind: "reconciled_closed", RC: &rc, Ts: now},
			{NID: "n1", PID: "01a", Kind: "killed_orphan", RC: nil, Ts: now}, // PID-reuse: same pid, both kinds
		},
		PortAudit: []ReconPortAudit{
			{NID: "n1", Port: 15000, Name: "b", LocalPort: 9, Kind: "reconciled", Ts: now},
			{NID: "n1", Port: 14000, Name: "a", LocalPort: 8, Kind: "reconciled", Ts: now},
		},
	}
	cmd, err := PlanReconcileBatch(in)
	if err != nil {
		t.Fatal(err)
	}
	if len(cmd.Aux) == 0 {
		t.Fatal("ReconcileBatch must carry the audit tuples in Aux")
	}

	// Round-trip the Command JSON exactly as raft replicates + a new leader decodes.
	data, err := json.Marshal(cmd)
	if err != nil {
		t.Fatal(err)
	}
	var cmd2 cluster.Command
	if err := json.Unmarshal(data, &cmd2); err != nil {
		t.Fatal(err)
	}

	p1, po1, err := ReplayReconcileAudit(cmd)
	if err != nil {
		t.Fatal(err)
	}
	p2, po2, err := ReplayReconcileAudit(&cmd2)
	if err != nil {
		t.Fatal(err)
	}
	if mustJSON(t, p1) != mustJSON(t, p2) {
		t.Fatalf("proc-audit replay diverged across the Command round-trip:\n a=%s\n b=%s", mustJSON(t, p1), mustJSON(t, p2))
	}
	if mustJSON(t, po1) != mustJSON(t, po2) {
		t.Fatalf("port-audit replay diverged across the Command round-trip")
	}
	// killed_orphan carries NO rc; reconciled_closed does — verify in the replayed records.
	if p1[0].Kind != "killed_orphan" || p1[0].RC != nil {
		t.Fatalf("first proc record must be 01a/killed_orphan with NO rc (total order (PID,Kind)); got %+v", p1[0])
	}
	if p1[1].Kind != "reconciled_closed" || p1[1].RC == nil || *p1[1].RC != 7 {
		t.Fatalf("second proc record must be 01a/reconciled_closed rc=7; got %+v", p1[1])
	}

	// Determinism vs resolver input order: a shuffled input bakes the SAME Aux bytes.
	shuffled := ReconcileBatchInput{
		SID:       in.SID,
		Marks:     []ExitMark{in.Marks[1], in.Marks[0]},
		ProcAudit: []ReconProcAudit{in.ProcAudit[2], in.ProcAudit[0], in.ProcAudit[1]},
		PortAudit: []ReconPortAudit{in.PortAudit[1], in.PortAudit[0]},
	}
	cmd3, err := PlanReconcileBatch(shuffled)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(cmd.Aux, cmd3.Aux) {
		t.Fatalf("Aux must be byte-stable regardless of input order (total-ordered):\n a=%s\n b=%s", cmd.Aux, cmd3.Aux)
	}
	if !bytes.Equal([]byte(mustJSON(t, cmd.Body)), []byte(mustJSON(t, cmd3.Body))) {
		t.Fatal("Body (MarkExited UPDATEs) must also be byte-stable regardless of input order")
	}

	// Self-sufficiency control: a ReconcileBatch with NO Aux replays empty (so a
	// dropped audit blob is detectable, not silently treated as "no audit").
	stripped := *cmd
	stripped.Aux = nil
	sp, spo, err := ReplayReconcileAudit(&stripped)
	if err != nil {
		t.Fatal(err)
	}
	if len(sp) != 0 || len(spo) != 0 {
		t.Fatal("a ReconcileBatch with no Aux must replay an EMPTY audit set")
	}
}

// TestD4ReconcileReplayMonotonicTimeByteIdentical (review m6) feeds a MONOTONIC-bearing
// time.Now() (not a time.Date literal) through bake → JSON round-trip → replay and
// asserts byte-identity, making the Ts-handling claim non-vacuous (every other test
// seeds monotonic-free time.Date).
func TestD4ReconcileReplayMonotonicTimeByteIdentical(t *testing.T) {
	now := time.Now() // carries a monotonic reading
	rc := 3
	in := ReconcileBatchInput{
		SID:       "lab",
		Marks:     []ExitMark{{PID: "01a", ExitCode: 3, When: now}},
		ProcAudit: []ReconProcAudit{{NID: "n1", PID: "01a", Kind: "reconciled_closed", RC: &rc, Ts: now}},
	}
	cmd, err := PlanReconcileBatch(in)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(cmd)
	var cmd2 cluster.Command
	if err := json.Unmarshal(data, &cmd2); err != nil {
		t.Fatal(err)
	}
	p1, _, _ := ReplayReconcileAudit(cmd)
	p2, _, _ := ReplayReconcileAudit(&cmd2)
	if mustJSON(t, p1) != mustJSON(t, p2) {
		t.Fatalf("monotonic-bearing Ts must replay byte-identically across the round-trip:\n a=%s\n b=%s", mustJSON(t, p1), mustJSON(t, p2))
	}
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}
