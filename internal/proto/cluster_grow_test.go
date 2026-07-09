package proto

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestCanonicalGrowReqBytes_domainSeparation pins that a grow signature can NEVER verify as an upgrade
// signature (and vice-versa) even when the common fields (op/target/issued_at) coincide: the grow canonical
// bytes carry a domain-separation prefix the upgrade bytes do not, so the two byte strings are disjoint.
func TestCanonicalGrowReqBytes_domainSeparation(t *testing.T) {
	grow := CanonicalGrowReqBytes(&ClusterGrowReq{Op: "reload", TargetNode: "brk-a", IssuedAt: "2026-07-08T00:00:00Z"})
	up := CanonicalUpgradeReqBytes(&ClusterUpgradeReq{Op: "reload", TargetNode: "brk-a", IssuedAt: "2026-07-08T00:00:00Z"})
	if string(grow) == string(up) {
		t.Fatalf("grow and upgrade canonical bytes must differ (domain separation), both = %q", grow)
	}
	if !strings.HasPrefix(string(grow), "tether-cluster-grow-v2\n") {
		t.Fatalf("grow canonical bytes must carry the domain-sep prefix, got %q", grow)
	}
	if strings.HasPrefix(string(up), "tether-cluster-grow-v2") {
		t.Fatalf("upgrade canonical bytes must NOT carry the grow prefix, got %q", up)
	}
}

// TestCanonicalGrowReqBytes_fieldSensitivity pins that any field change invalidates the signed bytes (no
// two distinct requests share a canonical form), including the boolean flags encoded as 0/1.
func TestCanonicalGrowReqBytes_fieldSensitivity(t *testing.T) {
	// review m5: the base MUST populate JoinBundle + OpID (the highest-value signed fields — a swapped join
	// bundle admits a different node as voter) so a future edit dropping them from the canonical bytes is
	// caught. Every signed field is mutated below.
	base := &ClusterGrowReq{Op: "mesh-cutover", TargetNode: "brk-a", JoinerNode: "brk-b", JoinBundle: "tether-join:v1:aaa", OpID: "op-1", GrowEpoch: "e1", IssuedAt: "t"}
	b0 := string(CanonicalGrowReqBytes(base))
	mutations := []func(*ClusterGrowReq){
		func(r *ClusterGrowReq) { r.Op = "approve-join" },
		func(r *ClusterGrowReq) { r.TargetNode = "brk-c" },
		func(r *ClusterGrowReq) { r.JoinerNode = "brk-z" },
		func(r *ClusterGrowReq) { r.JoinBundle = "tether-join:v1:EVIL" }, // m5: a swapped join bundle must invalidate the sig
		func(r *ClusterGrowReq) { r.OpID = "op-2" },                      // m5: op_id is signed
		func(r *ClusterGrowReq) { r.GrowEpoch = "e2" },
		func(r *ClusterGrowReq) { r.PreserveData = true },
		func(r *ClusterGrowReq) { r.ResetAck = true },
		func(r *ClusterGrowReq) { r.IssuedAt = "t2" },
	}
	for i, m := range mutations {
		cp := *base
		m(&cp)
		if got := string(CanonicalGrowReqBytes(&cp)); got == b0 {
			t.Fatalf("mutation %d did not change the canonical bytes (signature would not detect the tamper)", i)
		}
	}
}

// TestClusterGrowReq_additiveOmitemptyRoundtrip pins the wire discipline: a required-only request marshals
// WITHOUT any optional field (so a pre-G4 decoder that ignores unknowns is unaffected), and an unknown
// field on the wire decodes to the zero value (forward-compatible). ProtoVersion stays 2.
func TestClusterGrowReq_additiveOmitemptyRoundtrip(t *testing.T) {
	req := &ClusterGrowReq{Op: "acquire-lock", TargetNode: "brk-a", IssuedAt: "t", Sig: "deadbeef"}
	raw, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	for _, omitted := range []string{"joiner_node", "join_bundle", "op_id", "grow_epoch", "preserve_data", "reset_ack"} {
		if strings.Contains(string(raw), omitted) {
			t.Fatalf("required-only marshal must omit optional %q, got %s", omitted, raw)
		}
	}
	// A future field the current decoder does not know must decode to zero-value, not error.
	var back ClusterGrowReq
	if err := json.Unmarshal([]byte(`{"op":"acquire-lock","target_node":"brk-a","issued_at":"t","sig":"x","future_field":123}`), &back); err != nil {
		t.Fatalf("unknown field must decode, got %v", err)
	}
	if back.Op != "acquire-lock" || back.TargetNode != "brk-a" || back.PreserveData || back.ResetAck {
		t.Fatalf("decode mismatch: %+v", back)
	}
	if ProtoVersion != 2 {
		t.Fatalf("G4 must not bump the wire version; ProtoVersion=%d", ProtoVersion)
	}
}

// TestClusterGrowResp_additiveOmitempty pins the reply is all-optional (a bare OK reply is minimal).
func TestClusterGrowResp_additiveOmitempty(t *testing.T) {
	raw, err := json.Marshal(&ClusterGrowResp{OK: true})
	if err != nil {
		t.Fatal(err)
	}
	if got := string(raw); got != `{"ok":true}` {
		t.Fatalf("a bare OK ClusterGrowResp must marshal to {\"ok\":true}, got %s", got)
	}
}
