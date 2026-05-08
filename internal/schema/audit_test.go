package schema

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"
)

func TestAuditSchemaVersionPositive(t *testing.T) {
	if AuditSchemaVersion < 1 {
		t.Fatalf("AuditSchemaVersion must be ≥ 1, got %d", AuditSchemaVersion)
	}
}

// Round-trip: Marshal → Unmarshal → Marshal yields identical bytes for every
// audit envelope. This protects the H.5 wire shape from accidental field
// reordering / pointer-vs-value drift.
func TestAuditEnvelopesRoundtrip(t *testing.T) {
	t0 := time.Date(2026, 5, 8, 9, 32, 0, 0, time.UTC)
	rcZero := 0
	dur := int64(130432)

	cases := []any{
		&AuditCall{
			V: AuditSchemaVersion, Kind: "call", Ts: t0,
			ActorNkey: "UABCD", ActorFp: "SHA256:xxx=", ActorName: "alice",
			Session: "lab", Node: "lab-1", ReqID: "01hzxk",
			Verb: "run",
			Target: map[string]any{
				"argv": []any{"python", "train.py"},
				"cwd":  "/home/alice",
			},
			OK: true,
		},
		&AuditCall{
			V: AuditSchemaVersion, Kind: "admin_denied", Ts: t0,
			ActorNkey: "UABCD", ActorFp: "SHA256:xxx=",
			Session: "lab", ReqID: "01hzxm",
			Verb: "session.rm", OK: false, Error: "not owner",
		},
		&AuditProc{
			V: AuditSchemaVersion, Kind: "start", Ts: t0,
			Session: "lab", Node: "lab-1", PID: "01hzxk",
			Cmd: "python train.py",
		},
		&AuditProc{
			V: AuditSchemaVersion, Kind: "exit", Ts: t0,
			Session: "lab", Node: "lab-1", PID: "01hzxk",
			RC: &rcZero, DurationMs: &dur,
		},
		&AuditPort{
			V: AuditSchemaVersion, Kind: "allocated", Ts: t0,
			Session: "lab", Node: "lab-1",
			Port: 14022, Name: "jupyter", LocalPort: 8888,
			ActorNkey: "UABCD", ActorFp: "SHA256:xxx=",
		},
		&AuditPort{
			V: AuditSchemaVersion, Kind: "revoked", Ts: t0,
			Session: "lab", Node: "lab-1", Port: 14022,
		},
	}

	for i, v := range cases {
		first, err := json.Marshal(v)
		if err != nil {
			t.Fatalf("case %d: marshal: %v", i, err)
		}
		dec := newOf(v)
		if err := json.Unmarshal(first, dec); err != nil {
			t.Fatalf("case %d: unmarshal: %v\nbytes=%s", i, err, first)
		}
		second, err := json.Marshal(dec)
		if err != nil {
			t.Fatalf("case %d: re-marshal: %v", i, err)
		}
		if !bytes.Equal(first, second) {
			t.Errorf("case %d: round-trip mismatch:\n  first:  %s\n  second: %s", i, first, second)
		}
	}
}

// Optional fields with zero value must not appear in the JSON, otherwise
// every audit row pays for fields it doesn't use.
func TestAuditOmitemptyHonored(t *testing.T) {
	a := &AuditCall{V: AuditSchemaVersion, Kind: "call", Session: "lab", ReqID: "x", Verb: "ps", OK: true}
	b, err := json.Marshal(a)
	if err != nil {
		t.Fatal(err)
	}
	for _, banned := range []string{`"node":""`, `"actor_name":""`, `"target":`, `"error":""`} {
		if bytes.Contains(b, []byte(banned)) {
			t.Errorf("expected omitempty to drop %s; got %s", banned, b)
		}
	}
}

func newOf(v any) any {
	switch v.(type) {
	case *AuditCall:
		return &AuditCall{}
	case *AuditProc:
		return &AuditProc{}
	case *AuditPort:
		return &AuditPort{}
	}
	panic("newOf: unknown type")
}
