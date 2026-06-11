package proto

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestSafeFieldOmitemptyByteIdentical: the remote-fs-resilience Safe field MUST
// be `omitempty` on both ExecReq and RunReq so a false value serializes away
// entirely — keeping a new ctl that omits --safe byte-identical on the wire to
// today (and to an old ctl). A regression dropping omitempty would emit
// `"safe":false` and break that guarantee (ProtoVersion would effectively bump).
func TestSafeFieldOmitemptyByteIdentical(t *testing.T) {
	t.Run("ExecReq", func(t *testing.T) {
		no, _ := json.Marshal(ExecReq{Argv: []string{"echo"}})
		zero, _ := json.Marshal(ExecReq{Argv: []string{"echo"}, Safe: false})
		if string(no) != string(zero) {
			t.Errorf("Safe:false must marshal identically to omitted:\n  %s\n  %s", no, zero)
		}
		if strings.Contains(string(zero), "safe") {
			t.Errorf("omitempty failed: zero Safe leaked a safe key: %s", zero)
		}
	})
	t.Run("RunReq", func(t *testing.T) {
		no, _ := json.Marshal(RunReq{Argv: []string{"bash"}})
		zero, _ := json.Marshal(RunReq{Argv: []string{"bash"}, Safe: false})
		if string(no) != string(zero) {
			t.Errorf("Safe:false must marshal identically to omitted:\n  %s\n  %s", no, zero)
		}
		if strings.Contains(string(zero), "safe") {
			t.Errorf("omitempty failed: zero Safe leaked a safe key: %s", zero)
		}
	})
}

// TestSafeFieldRoundtrip: a true Safe serializes under the agreed json tag and
// survives marshal/unmarshal on both verbs.
func TestSafeFieldRoundtrip(t *testing.T) {
	eb, _ := json.Marshal(ExecReq{Argv: []string{"echo"}, Safe: true})
	if !strings.Contains(string(eb), `"safe":true`) {
		t.Errorf("ExecReq Safe:true must serialize under safe: %s", eb)
	}
	var eg ExecReq
	if err := json.Unmarshal(eb, &eg); err != nil || !eg.Safe {
		t.Errorf("ExecReq roundtrip Safe: got %v err %v", eg.Safe, err)
	}

	rb, _ := json.Marshal(RunReq{Argv: []string{"bash"}, Safe: true})
	if !strings.Contains(string(rb), `"safe":true`) {
		t.Errorf("RunReq Safe:true must serialize under safe: %s", rb)
	}
	var rg RunReq
	if err := json.Unmarshal(rb, &rg); err != nil || !rg.Safe {
		t.Errorf("RunReq roundtrip Safe: got %v err %v", rg.Safe, err)
	}
}

// TestSafeFieldLegacyBodyDecodesToFalse: a pre-feature ctl body (no safe key)
// MUST decode to Safe==false on a new agent — the old-ctl→new-agent compat path.
func TestSafeFieldLegacyBodyDecodesToFalse(t *testing.T) {
	var e ExecReq
	if err := json.Unmarshal([]byte(`{"argv":["echo"]}`), &e); err != nil || e.Safe {
		t.Errorf("legacy ExecReq must decode Safe=false, got %v err %v", e.Safe, err)
	}
	var r RunReq
	if err := json.Unmarshal([]byte(`{"argv":["bash"]}`), &r); err != nil || r.Safe {
		t.Errorf("legacy RunReq must decode Safe=false, got %v err %v", r.Safe, err)
	}
}

// TestSafeFieldNewBodyDecodesOnOldStruct: a NEW ctl that SETS --safe talks to an
// OLD agent whose struct has no Safe field. The decode path uses plain
// json.Unmarshal (no DisallowUnknownFields), so the unknown safe key MUST be
// silently ignored — the request degrades to the legacy (no safe-mode) spawn
// rather than erroring. We simulate the old struct locally.
func TestSafeFieldNewBodyDecodesOnOldStruct(t *testing.T) {
	type oldExecReq struct {
		Argv []string `json:"argv"`
	}
	var got oldExecReq
	if err := json.Unmarshal([]byte(`{"argv":["echo"],"safe":true}`), &got); err != nil {
		t.Fatalf("old struct must ignore unknown safe key, got error: %v", err)
	}
	if len(got.Argv) != 1 || got.Argv[0] != "echo" {
		t.Errorf("known fields must still decode: %+v", got)
	}
}
