package xferaudit

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/schema"
	"github.com/LinZiyang666/tether/internal/xfercontract"
)

func sampleRec(kind string) schema.AuditTransfer {
	return schema.AuditTransfer{
		V: schema.AuditSchemaVersion, Kind: kind, Verb: "push",
		Ts:      time.Date(2026, 6, 23, 10, 0, 0, 0, time.UTC),
		Session: "lab", Node: "lab-1",
		ActorNkey: "UACTOR", ActorFp: "fp",
		TransferID: "tid-deadbeef", Path: "/x", Size: 4096, SHA256: "abc",
		Tier: "b", Bucket: "xfer-lab", Bytes: 4096, DurationMs: 12, Code: "", Error: "",
	}
}

func continuationRec(kind string) schema.AuditTransfer {
	rec := sampleRec(kind)
	rec.AgentInstance, rec.EntryNonce, rec.IntentDigest = "agent-original", "entry-original", "intent-original"
	rec.ObjectVersion = "object-original"
	if kind != "start" {
		rec.TerminalID = schema.TransferTerminalID(rec.Session, rec.TransferID, rec.EntryNonce)
	}
	return rec
}

// origin: hot-update S7 plan §5.1–§5.2 / T23
func TestTransferCanonicalAuditPreservesIdentityAndBytes(t *testing.T) {
	for _, kind := range []string{"start", "complete", "failed"} {
		t.Run(kind, func(t *testing.T) {
			rec := continuationRec(kind)
			canonical, err := CanonicalTransferAudit(rec)
			if err != nil {
				t.Fatal(err)
			}
			want, err := json.Marshal(rec)
			if err != nil {
				t.Fatal(err)
			}
			sum := sha256.Sum256(want)
			if !bytes.Equal(canonical.Bytes, want) || canonical.Digest != hex.EncodeToString(sum[:]) {
				t.Fatal("canonical digest must cover the exact normalized history bytes")
			}
			for _, field := range []string{"agent_instance", "entry_nonce", "intent_digest", "object_version"} {
				if !bytes.Contains(canonical.Bytes, []byte(`"`+field+`":`)) {
					t.Fatalf("canonical history is missing frozen identity field %s", field)
				}
			}
			if kind != "start" && !bytes.Contains(canonical.Bytes, []byte(`"terminal_id":"6590c9ef08eabeda63ee1e08f497ef00c1e6315dfd01cd5bf746e985c1c4d691"`)) {
				t.Fatal("terminal history lost its exact terminal identity")
			}
			wantIdentity := xfercontract.Identity{SID: rec.Session, NID: rec.Node, ActorFP: rec.ActorFp,
				AgentInstance: rec.AgentInstance, TransferID: rec.TransferID, EntryNonce: rec.EntryNonce,
				IntentDigest: rec.IntentDigest, ObjectVersion: rec.ObjectVersion}
			if canonical.Identity != wantIdentity || canonical.Kind != kind {
				t.Fatalf("canonical identity = %+v, want %+v", canonical.Identity, wantIdentity)
			}
			cmd, err := PlanTransferAudit(rec)
			if err != nil {
				t.Fatal(err)
			}
			got, ok, err := ReplayTransferAudit(cmd)
			if err != nil || !ok {
				t.Fatalf("replay: ok=%v err=%v", ok, err)
			}
			again, err := CanonicalTransferAudit(got)
			if err != nil || !reflect.DeepEqual(again, canonical) || cmd.ReqID != canonical.ReqID {
				t.Fatalf("replay changed canonical record: err=%v", err)
			}
			// Every retained identity and outcome participates in the content key.
			for _, change := range []func(*schema.AuditTransfer){
				func(r *schema.AuditTransfer) { r.EntryNonce += "-changed" },
				func(r *schema.AuditTransfer) { r.AgentInstance += "-changed" },
				func(r *schema.AuditTransfer) { r.IntentDigest += "-changed" },
				func(r *schema.AuditTransfer) { r.ObjectVersion += "-changed" },
				func(r *schema.AuditTransfer) { r.ActorFp += "-changed" },
				func(r *schema.AuditTransfer) { r.Ts = r.Ts.Add(time.Nanosecond) },
			} {
				changed := rec
				change(&changed)
				key, err := TransferRecordReqID(changed)
				if err != nil || key == canonical.ReqID {
					t.Fatalf("changed canonical content reused ReqID: err=%v", err)
				}
			}
		})
	}
}

// origin: hot-update S7 plan §5.2 / T23 / lead LQ1
func TestTransferAuditRejectsPartialIdentityAndFalseAbandonment(t *testing.T) {
	for name, change := range map[string]func(*schema.AuditTransfer){
		"version":        func(r *schema.AuditTransfer) { r.V++ },
		"session":        func(r *schema.AuditTransfer) { r.Session = "bad.sid" },
		"node":           func(r *schema.AuditTransfer) { r.Node = "" },
		"actor":          func(r *schema.AuditTransfer) { r.ActorFp = "" },
		"agent":          func(r *schema.AuditTransfer) { r.AgentInstance = "" },
		"nonce":          func(r *schema.AuditTransfer) { r.EntryNonce = "" },
		"intent":         func(r *schema.AuditTransfer) { r.IntentDigest = "" },
		"object":         func(r *schema.AuditTransfer) { r.ObjectVersion = "" },
		"timestamp":      func(r *schema.AuditTransfer) { r.Ts = time.Time{} },
		"terminal":       func(r *schema.AuditTransfer) { r.TerminalID = "" },
		"wrong terminal": func(r *schema.AuditTransfer) { r.TerminalID = "foreign-terminal" },
		"verb":           func(r *schema.AuditTransfer) { r.Verb = "unknown" },
		"tier":           func(r *schema.AuditTransfer) { r.Tier = "unknown" },
		"kind":           func(r *schema.AuditTransfer) { r.Kind = "unknown" },
		"false complete": func(r *schema.AuditTransfer) {
			r.OutcomeUnknown = true
			r.Code = xfercontract.ReasonSettlementAbandoned
		},
		"false known":         func(r *schema.AuditTransfer) { r.Kind = "failed"; r.Code = xfercontract.ReasonSettlementAbandoned },
		"unexplained unknown": func(r *schema.AuditTransfer) { r.Kind = "failed"; r.OutcomeUnknown = true },
		"start terminal":      func(r *schema.AuditTransfer) { r.Kind = "start" },
	} {
		t.Run(name, func(t *testing.T) {
			rec := continuationRec("complete")
			change(&rec)
			if _, err := PlanTransferAudit(rec); err == nil {
				t.Fatal("malformed continuation must be rejected before Raft proposal")
			}
		})
	}
	// Any lone extension is v1, including a terminal flag after identity loss.
	for name, change := range map[string]func(*schema.AuditTransfer){
		"agent":    func(r *schema.AuditTransfer) { r.AgentInstance = "agent" },
		"nonce":    func(r *schema.AuditTransfer) { r.EntryNonce = "nonce" },
		"intent":   func(r *schema.AuditTransfer) { r.IntentDigest = "intent" },
		"object":   func(r *schema.AuditTransfer) { r.ObjectVersion = "object" },
		"terminal": func(r *schema.AuditTransfer) { r.TerminalID = "terminal" },
		"unknown":  func(r *schema.AuditTransfer) { r.OutcomeUnknown = true },
	} {
		t.Run("lone "+name, func(t *testing.T) {
			rec := sampleRec("failed")
			change(&rec)
			if _, err := PlanTransferAudit(rec); err == nil {
				t.Fatal("partial extension must not downgrade to legacy")
			}
		})
	}
	unknown := continuationRec("failed")
	unknown.Code, unknown.OutcomeUnknown = xfercontract.ReasonSettlementAbandoned, true
	cmd, err := PlanTransferAudit(unknown)
	if err != nil {
		t.Fatal(err)
	}
	got, ok, err := ReplayTransferAudit(cmd)
	if err != nil || !ok || !got.OutcomeUnknown || got.Kind != "failed" || got.Code != xfercontract.ReasonSettlementAbandoned {
		t.Fatalf("abandonment must preserve the unknown failed outcome: %+v, %v", got, err)
	}
	canonical, err := CanonicalTransferAudit(got)
	if err != nil || !bytes.Contains(canonical.Bytes, []byte(`"outcome_unknown":true`)) {
		t.Fatalf("history must expose the unknown outcome under its frozen field name: %v", err)
	}
}

// origin: hot-update S7 plan §5.2 / T23
func TestTransferAuditReplayRejectsContentSplice(t *testing.T) {
	for _, kind := range []string{"start", "complete", "failed"} {
		t.Run(kind, func(t *testing.T) {
			original, err := PlanTransferAudit(continuationRec(kind))
			if err != nil {
				t.Fatal(err)
			}
			changed := continuationRec(kind)
			changed.Ts = changed.Ts.Add(time.Second)
			cmd, err := PlanTransferAudit(changed)
			if err != nil {
				t.Fatal(err)
			}
			cmd.ReqID = original.ReqID
			if _, ok, err := ReplayTransferAudit(cmd); err == nil || ok {
				t.Fatal("spliced bytes must not replay under the original ReqID")
			}
		})
	}
}

// origin: hot-update S7 plan §9 / T23
func TestTransferAuditLegacySchemaBytesRemainUnchanged(t *testing.T) {
	rec := sampleRec("complete")
	got, err := json.Marshal(rec)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"v":1,"kind":"complete","verb":"push","ts":"2026-06-23T10:00:00Z","session":"lab","node":"lab-1","actor_nkey":"UACTOR","actor_fp":"fp","transfer_id":"tid-deadbeef","path":"/x","size":4096,"sha256":"abc","tier":"b","bucket":"xfer-lab","bytes":4096,"duration_ms":12}`
	if string(got) != want || schema.TransferContinuation(rec) {
		t.Fatalf("zero extensions changed legacy history bytes: %s", got)
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

// TestReqIDStableAndHex: the record-level reqID is 64 lowercase hex (a valid
// Command.ReqID), stable for the same normalized record, and DISTINCT across
// legitimate later records even if transfer_id and kind are reused.
func TestReqIDStableAndHex(t *testing.T) {
	rec := sampleRec("complete")
	a, err := TransferRecordReqID(rec)
	if err != nil {
		t.Fatalf("record reqID: %v", err)
	}
	if len(a) != 64 {
		t.Fatalf("reqID len = %d, want 64", len(a))
	}
	for i := 0; i < len(a); i++ {
		c := a[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			t.Fatalf("reqID has non-lowercase-hex byte %q", c)
		}
	}
	a2, err := TransferRecordReqID(rec)
	if err != nil {
		t.Fatalf("record reqID again: %v", err)
	}
	if a2 != a {
		t.Fatalf("reqID not stable for same record")
	}
	rec2 := rec
	rec2.Bytes++
	b, err := TransferRecordReqID(rec2)
	if err != nil {
		t.Fatalf("record reqID changed: %v", err)
	}
	if b == a {
		t.Fatalf("reqID must differ for distinct records with the same transfer_id/kind")
	}
	cmdA, err := PlanTransferAudit(rec)
	if err != nil {
		t.Fatalf("plan rec: %v", err)
	}
	cmdB, err := PlanTransferAudit(rec2)
	if err != nil {
		t.Fatalf("plan rec2: %v", err)
	}
	if cmdA.ReqID == cmdB.ReqID {
		t.Fatalf("PlanTransferAudit ReqID must differ for distinct records with the same transfer_id/kind")
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
