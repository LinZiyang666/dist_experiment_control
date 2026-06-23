package xferaudit

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/schema"
)

func sampleRec(kind string) schema.AuditTransfer {
	return schema.AuditTransfer{
		V: schema.AuditSchemaVersion, Kind: kind, Verb: "push",
		Ts:         time.Date(2026, 6, 23, 10, 0, 0, 0, time.UTC),
		Session:    "lab", Node: "lab-1",
		ActorNkey:  "UACTOR", ActorFp: "fp",
		TransferID: "tid-deadbeef", Path: "/x", Size: 4096, SHA256: "abc",
		Tier: "b", Bucket: "xfer-lab", Bytes: 4096, DurationMs: 12, Code: "", Error: "",
	}
}

// TestPlanReplayByteIdentical: Plan(rec) -> Replay -> the SAME record, and the replayed
// JSON is byte-identical to a directly-marshaled live record (the property the D5 publisher
// relies on so two leaders replaying the committed bytes publish identical payloads).
func TestPlanReplayByteIdentical(t *testing.T) {
	rec := sampleRec("complete")
	cmd, err := PlanTransferAudit(rec)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	if len(cmd.Body) != 0 {
		t.Fatalf("OpTransferAudit must have EMPTY body (apply no-op), got %d statements", len(cmd.Body))
	}
	if cmd.Op != cluster.OpTransferAudit {
		t.Fatalf("op = %q, want TransferAudit", cmd.Op)
	}
	got, ok, err := ReplayTransferAudit(cmd)
	if err != nil || !ok {
		t.Fatalf("replay: ok=%v err=%v", ok, err)
	}
	// Live payload = the record as pubAuditTransfer would marshal it, with the monotonic
	// reading stripped (Plan does rec.Ts.Round(0); a wall-clock-only Ts is unaffected).
	live := rec
	live.Ts = live.Ts.Round(0)
	wantJSON, _ := json.Marshal(live)
	gotJSON, _ := json.Marshal(got)
	if string(wantJSON) != string(gotJSON) {
		t.Fatalf("replayed JSON not byte-identical:\n live=%s\n got =%s", wantJSON, gotJSON)
	}
}

// TestReqIDStableAndHex: the derived reqID is 64 lowercase hex (a valid Command.ReqID),
// stable for the same (transfer_id, kind), and DISTINCT across kinds (start/complete/failed
// are distinct rows that must each commit) and across transfer ids.
func TestReqIDStableAndHex(t *testing.T) {
	a := TransferReqID("tid-1", "complete")
	if len(a) != 64 {
		t.Fatalf("reqID len = %d, want 64", len(a))
	}
	for i := 0; i < len(a); i++ {
		c := a[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			t.Fatalf("reqID has non-lowercase-hex byte %q", c)
		}
	}
	if TransferReqID("tid-1", "complete") != a {
		t.Fatalf("reqID not stable for same input")
	}
	if TransferReqID("tid-1", "failed") == a {
		t.Fatalf("reqID must differ across kinds (complete vs failed) — distinct audit rows")
	}
	if TransferReqID("tid-2", "complete") == a {
		t.Fatalf("reqID must differ across transfer ids")
	}
}

// TestPlanRejectsMalformed: an empty transfer_id or an unknown kind is a leader-side
// programming error, rejected at Plan so it never becomes a poison raft entry.
func TestPlanRejectsMalformed(t *testing.T) {
	if _, err := PlanTransferAudit(schema.AuditTransfer{Kind: "complete"}); err == nil {
		t.Fatal("empty transfer_id must be rejected")
	}
	bad := sampleRec("bogus")
	if _, err := PlanTransferAudit(bad); err == nil {
		t.Fatal("unknown kind must be rejected")
	}
}

// TestReplayEmptyAux: an OpTransferAudit with no Aux yields ok=false (nothing to publish),
// and a non-TransferAudit command is a caller error.
func TestReplayEmptyAux(t *testing.T) {
	empty := cluster.NewCommand(cluster.OpTransferAudit)
	_, ok, err := ReplayTransferAudit(empty)
	if err != nil || ok {
		t.Fatalf("empty-Aux replay: ok=%v err=%v, want ok=false err=nil", ok, err)
	}
	if _, _, err := ReplayTransferAudit(cluster.NewCommand(cluster.OpReconcileBatch)); err == nil {
		t.Fatal("replay of a non-TransferAudit command must error")
	}
}
