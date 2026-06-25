package adminsock

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestResponseCodeRoundTrips (B2 item 4): Response.Code round-trips on the wire; a legacy reply
// (no "code" key) decodes to "" (so an old broker / old CLI stays compatible).
func TestResponseCodeRoundTrips(t *testing.T) {
	b, err := json.Marshal(Response{Op: "cluster_remove", Error: "x", Code: CodeNodeUnknown})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), `"code":"node_unknown"`) {
		t.Errorf("code not serialized: %s", b)
	}
	var legacy Response
	if err := json.Unmarshal([]byte(`{"op":"cluster_remove","error":"x"}`), &legacy); err != nil {
		t.Fatal(err)
	}
	if legacy.Code != "" {
		t.Errorf("legacy reply (no code key) must decode Code=\"\", got %q", legacy.Code)
	}
}

// TestRequestForceOmitemptyRoundTrip (B3 review m5): Request.Force is additive + omitempty — it is
// absent on the wire when unset (non-cluster code never sets it) and round-trips when set.
func TestRequestForceOmitemptyRoundTrip(t *testing.T) {
	b, err := json.Marshal(Request{Op: OpClusterRemove, NodeID: "brk-b"})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "force") {
		t.Errorf("unset Force must be omitted: %s", b)
	}
	var got Request
	if err := json.Unmarshal([]byte(`{"op":"cluster_remove","node_id":"brk-b","force":true}`), &got); err != nil {
		t.Fatal(err)
	}
	if !got.Force {
		t.Error("Force=true must round-trip")
	}
}

// TestClusterStatusReportBranchFieldsAlwaysPresent (B2 item 5): a report marshals with
// errors + partial + is_leader_view ALWAYS present (branch-load-bearing, no omitempty), so a
// monitor can rely on them.
func TestClusterStatusReportBranchFieldsAlwaysPresent(t *testing.T) {
	b, err := json.Marshal(ClusterStatusReport{SchemaVersion: 1, Errors: []string{}})
	if err != nil {
		t.Fatal(err)
	}
	s := string(b)
	for _, key := range []string{`"errors":[]`, `"partial":false`, `"is_leader_view":false`, `"schema_version":1`} {
		if !strings.Contains(s, key) {
			t.Errorf("branch field missing: %s not in %s", key, s)
		}
	}
}

// TestB6WireAdditionsOmitemptyRoundTrip — Stage-C M-j: every B6 wire addition is additive-omitempty
// so an empty Request/Response serializes without the new keys (an OLD CLI/broker stays compatible).
func TestB6WireAdditionsOmitemptyRoundTrip(t *testing.T) {
	emptyReq, err := json.Marshal(Request{Op: OpClusterStatus})
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"joiner_proto", "joiner_release", "backup_path", "since", "sids"} {
		if strings.Contains(string(emptyReq), k) {
			t.Fatalf("empty Request leaked B6 key %q: %s", k, emptyReq)
		}
	}
	emptyResp, err := json.Marshal(Response{Op: OpClusterStatus})
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"backup", "incident"} {
		if strings.Contains(string(emptyResp), k) {
			t.Fatalf("empty Response leaked B6 key %q: %s", k, emptyResp)
		}
	}
	// A populated B6 request/response round-trips.
	req := Request{Op: OpClusterAdd, JoinerProto: 2, JoinerRelease: "v1.2.3", BackupPath: "/b", Since: "24h", SIDs: []string{"s1"}}
	var gotReq Request
	if b, _ := json.Marshal(req); json.Unmarshal(b, &gotReq) != nil || gotReq.JoinerProto != 2 || gotReq.Since != "24h" || len(gotReq.SIDs) != 1 {
		t.Fatalf("B6 request round-trip wrong: %+v", gotReq)
	}
}
