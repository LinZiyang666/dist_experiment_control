package main

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/proto"
)

// TestEmitJSONSchemaAndNormSlice (B2 items 1+2): a --json payload carries schema + schema_version,
// and a nil slice marshals as [] (not null) so `jq '.nodes | length'` is always defined.
func TestEmitJSONSchemaAndNormSlice(t *testing.T) {
	var sb strings.Builder
	if err := emitJSON(&sb, nodeLsJSON{Schema: "node_ls", SchemaVersion: 1, Nodes: normSlice[proto.NodeListEntry](nil)}); err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(sb.String()), &m); err != nil {
		t.Fatalf("not parseable: %v", err)
	}
	if m["schema"] != "node_ls" {
		t.Errorf("schema = %v, want node_ls", m["schema"])
	}
	if _, ok := m["schema_version"]; !ok {
		t.Errorf("schema_version missing from %v", m)
	}
	nodes, ok := m["nodes"].([]any)
	if !ok {
		t.Fatalf("nodes must be a JSON array, not null: %v", m["nodes"])
	}
	if len(nodes) != 0 {
		t.Errorf("nil nodes should marshal to [], got %d", len(nodes))
	}
}

// TestAlertLsJSONDedupKeyRoundTrip (B2 item 1): alert ls --json carries dedup_key (no omitempty),
// so it round-trips into alert ack; an empty key still serializes.
func TestAlertLsJSONDedupKeyRoundTrip(t *testing.T) {
	alerts := []proto.AlertView{{Kind: "quorum_lost", Severity: proto.SeveritySevere, DedupKey: "qk-1", Message: "m"}}
	var sb strings.Builder
	if err := emitJSON(&sb, alertLsJSON{Schema: "alert_ls", SchemaVersion: 1, Alerts: normSlice(alerts)}); err != nil {
		t.Fatal(err)
	}
	var decoded alertLsJSON
	if err := json.Unmarshal([]byte(sb.String()), &decoded); err != nil {
		t.Fatalf("not parseable: %v", err)
	}
	if len(decoded.Alerts) != 1 || decoded.Alerts[0].DedupKey != "qk-1" {
		t.Fatalf("dedup_key did not round-trip: %+v", decoded.Alerts)
	}
	ack := proto.AlertAckReq{DedupKey: decoded.Alerts[0].DedupKey} // feeds `alert ack`
	if ack.DedupKey != "qk-1" {
		t.Errorf("ack key = %q, want qk-1", ack.DedupKey)
	}

	var sb2 strings.Builder
	_ = emitJSON(&sb2, alertLsJSON{Schema: "alert_ls", SchemaVersion: 1, Alerts: []proto.AlertView{{Kind: "k"}}})
	if !strings.Contains(sb2.String(), `"dedup_key": ""`) {
		t.Errorf("empty dedup_key must still serialize (no omitempty): %q", sb2.String())
	}
}

// TestExposeJSONShape (B2 item 1): expose --json emits {public_host,port,name,node}; expose rm
// --json emits {name,node,port} (no public_host — ExposeRmResp has none).
func TestExposeJSONShape(t *testing.T) {
	var sb strings.Builder
	if err := emitJSON(&sb, exposeJSON{Schema: "expose", SchemaVersion: 1, PublicHost: "b.example", Port: 14001, Name: "jup", Node: "gpu-01"}); err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(sb.String()), &m); err != nil {
		t.Fatalf("not parseable: %v", err)
	}
	for _, k := range []string{"schema", "schema_version", "public_host", "port", "name", "node"} {
		if _, ok := m[k]; !ok {
			t.Errorf("expose --json missing %q: %v", k, m)
		}
	}

	var sr strings.Builder
	if err := emitJSON(&sr, exposeRmJSON{Schema: "expose_rm", SchemaVersion: 1, Name: "jup", Node: "gpu-01", Port: 14001}); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(sr.String(), "public_host") {
		t.Errorf("expose rm --json must NOT carry public_host: %q", sr.String())
	}
}
