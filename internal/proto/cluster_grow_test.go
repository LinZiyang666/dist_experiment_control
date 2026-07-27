package proto

import (
	"encoding/json"
	"reflect"
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

// TestCanonicalGrowReqBytesCoversEveryField is the precondition B4 named for any future in-band
// version field, and it closes a gap the field-sensitivity test cannot close on its own.
//
// TestCanonicalGrowReqBytes_fieldSensitivity mutates a HAND-WRITTEN list of fields. That proves
// every listed field is signed; it says nothing about a field nobody added to the list. So adding
// a tenth field to ClusterGrowReq — exactly what "stamp a schema version" would have done — leaves
// both tests green while the new field rides the wire UNSIGNED, freely settable by any caller whose
// signature covers only the other nine.
//
// This asserts the two sets agree: every exported field of ClusterGrowReq is either signed (its
// value changes the canonical bytes) or listed below with a reason.
func TestCanonicalGrowReqBytesCoversEveryField(t *testing.T) {
	// Sig is excluded BY DESIGN: it is the signature over the other fields, so it cannot be inside
	// its own input. Any other exclusion needs a reason written here.
	unsignedByDesign := map[string]string{
		"Sig": "the signature itself — CanonicalGrowReqBytes is its input, so including it is impossible",
	}

	rt := reflect.TypeOf(ClusterGrowReq{})
	if rt.NumField() < 10 {
		t.Fatalf("ClusterGrowReq has %d fields; this guard was written against 11 and a shrink means "+
			"a signed field was REMOVED — check whether the canonical bytes still cover the rest",
			rt.NumField())
	}

	base := &ClusterGrowReq{
		Op: "mesh-cutover", TargetNode: "brk-a", JoinerNode: "brk-b",
		JoinBundle: "tether-join:v1:aaa", OpID: "op-1", GrowEpoch: "e1", IssuedAt: "t",
	}
	b0 := string(CanonicalGrowReqBytes(base))

	for i := 0; i < rt.NumField(); i++ {
		f := rt.Field(i)
		if f.PkgPath != "" {
			continue // unexported: not on the wire
		}
		if reason, ok := unsignedByDesign[f.Name]; ok {
			if reason == "" {
				t.Errorf("%s is excluded from the signed bytes with an empty reason", f.Name)
			}
			continue
		}
		// Perturb this field and require the canonical bytes to change.
		mutated := *base
		v := reflect.ValueOf(&mutated).Elem().Field(i)
		switch v.Kind() {
		case reflect.String:
			v.SetString("PERTURBED-" + f.Name)
		case reflect.Bool:
			v.SetBool(!v.Bool())
		case reflect.Int, reflect.Int64:
			v.SetInt(v.Int() + 1)
		default:
			t.Errorf("%s has kind %s, which this guard does not know how to perturb — extend it "+
				"rather than letting the field go unchecked", f.Name, v.Kind())
			continue
		}
		if got := string(CanonicalGrowReqBytes(&mutated)); got == b0 {
			t.Errorf("changing ClusterGrowReq.%s does NOT change the canonical signed bytes.\n"+
				"That field rides the grow wire UNSIGNED: any caller can set it freely and a "+
				"signature computed over the other fields still verifies. Either include it in "+
				"CanonicalGrowReqBytes (a signed-bytes change — every in-flight request stops "+
				"verifying, so it needs a cross-version story) or add it to unsignedByDesign with "+
				"the reason it is safe unsigned.", f.Name)
		}
	}
}

// TestFieldCoverageGuardIsNotVacuous proves the guard above would actually catch a new unsigned
// field. Without it, a perturbation loop that silently skipped every field would report full
// coverage.
func TestFieldCoverageGuardIsNotVacuous(t *testing.T) {
	// A struct shaped like ClusterGrowReq but with a field the canonical function ignores.
	type withUnsigned struct {
		Op       string
		Unsigned string
	}
	canonical := func(r *withUnsigned) string { return "prefix\n" + r.Op } // deliberately ignores Unsigned

	base := &withUnsigned{Op: "x", Unsigned: "y"}
	b0 := canonical(base)
	rt := reflect.TypeOf(withUnsigned{})
	var missed []string
	for i := 0; i < rt.NumField(); i++ {
		m := *base
		reflect.ValueOf(&m).Elem().Field(i).SetString("PERTURBED")
		if canonical(&m) == b0 {
			missed = append(missed, rt.Field(i).Name)
		}
	}
	if len(missed) != 1 || missed[0] != "Unsigned" {
		t.Fatalf("the perturbation technique reported %v as unsigned, want exactly [Unsigned] — the "+
			"loop in TestCanonicalGrowReqBytesCoversEveryField cannot detect an unsigned field", missed)
	}
}
