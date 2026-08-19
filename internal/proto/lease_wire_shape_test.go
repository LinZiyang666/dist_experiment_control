package proto

import (
	"encoding/json"
	"testing"
)

// origin: docs/reviews/cloned-credential-instances-plan.md §4.1 — the N-1
// promise for this increment is "every new key is omitempty with a legal zero
// value", stated three times: instance_id / leased_nid absent = a pre-feature
// agent, `lease` absent = uncontested, `leased` absent = an ordinary device.
//
// The golden fixtures under testdata/golden cover NodeRegisterReq and
// NodeRegisterResp, so those two are already pinned byte-for-byte. NodeListEntry
// is NOT in the fixture set, which is exactly the message whose zero-value shape
// the plan calls "multiplicity-1 的 --json 逐字节不变" — the operator-visible
// half of the promise. This pins all three from one place, independent of which
// structs anyone remembers to add to the golden list.
//
// MUTATIONS that must turn this red: drop `,omitempty` from any of
// NodeRegisterReq.InstanceID, NodeRegisterReq.LeasedNID,
// NodeRegisterResp.Lease, or NodeListEntry.Leased.
func TestLeaseKeysAreAbsentFromEveryZeroValueWireBody(t *testing.T) {
	cases := []struct {
		name    string
		v       any
		absent  []string
		present []string
	}{
		{
			name:    "pre-feature register request",
			v:       &NodeRegisterReq{ProtoVersion: ProtoVersion, NID: "gpu1"},
			absent:  []string{"instance_id", "leased_nid"},
			present: []string{"nid", "proto_version"},
		},
		{
			name:    "uncontested register reply",
			v:       &NodeRegisterResp{OK: true},
			absent:  []string{"lease"},
			present: []string{"ok"},
		},
		{
			name:    "ordinary node list row",
			v:       &NodeListEntry{NID: "gpu1", Status: "ONLINE"},
			absent:  []string{"leased"},
			present: []string{"nid", "status"},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			raw, err := json.Marshal(c.v)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			var keys map[string]json.RawMessage
			if err := json.Unmarshal(raw, &keys); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			for _, k := range c.absent {
				if _, ok := keys[k]; ok {
					t.Errorf("key %q must be absent from a zero-valued body (an N-1 peer's byte "+
						"stream would change): %s", k, raw)
				}
			}
			for _, k := range c.present {
				if _, ok := keys[k]; !ok {
					t.Fatalf("vacuity check: key %q missing, so this fixture is not the message "+
						"it claims to be: %s", k, raw)
				}
			}
		})
	}
}

// origin: docs/reviews/cloned-credential-instances-plan.md §6 — an OLD peer
// must be able to decode a NEW body, and a NEW peer must decode an OLD body,
// with the missing keys landing on the documented zero-value semantics.
//
// MUTATION that must turn this red: give NodeRegisterReq.InstanceID or
// NodeRegisterResp.Lease a non-zero default anywhere on the decode path, or
// make a decoder reject unknown keys (json.Decoder.DisallowUnknownFields).
func TestOldAndNewBodiesDecodeAcrossTheVersionBoundary(t *testing.T) {
	// NEW body → OLD decoder: the two new keys are simply dropped.
	newReq, err := json.Marshal(&NodeRegisterReq{
		ProtoVersion: ProtoVersion, NID: "gpu1",
		InstanceID: "abcdefghijklmnopqrstuvwxyz", LeasedNID: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	var oldSide struct {
		ProtoVersion int    `json:"proto_version"`
		NID          string `json:"nid"`
	}
	if err := json.Unmarshal(newReq, &oldSide); err != nil {
		t.Fatalf("an old broker must decode a new register body: %v", err)
	}
	if oldSide.NID != "gpu1" || oldSide.ProtoVersion != ProtoVersion {
		t.Fatalf("old-side decode lost a pre-existing field: %+v", oldSide)
	}

	// OLD body → NEW decoder: the missing keys must land on the zero values the
	// broker reads as "a pre-feature agent" and "holds its basename".
	var newSide NodeRegisterReq
	if err := json.Unmarshal([]byte(`{"proto_version":2,"nid":"gpu1"}`), &newSide); err != nil {
		t.Fatal(err)
	}
	if newSide.InstanceID != "" {
		t.Errorf("a missing instance_id must decode to \"\" (the pre-feature marker); got %q", newSide.InstanceID)
	}
	if newSide.LeasedNID {
		t.Errorf("a missing leased_nid must decode to false (holds its basename)")
	}
	if err := ValidateInstanceID(newSide.InstanceID); err == nil {
		t.Errorf("the empty instance id must NOT validate — callers rely on absence being handled " +
			"before validation, not by it")
	}

	// OLD reply → NEW agent: a nil Lease is legacy mode.
	var resp NodeRegisterResp
	if err := json.Unmarshal([]byte(`{"ok":true}`), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Lease != nil {
		t.Errorf("a reply with no lease key must decode to a nil Lease; got %+v", resp.Lease)
	}
}
