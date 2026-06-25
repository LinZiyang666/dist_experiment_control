package adminsock

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

// TestB4RequestAlertFieldsOmitempty (plan §F.19): the new operator-alert Request fields are
// omitempty — an empty request is byte-compatible with a pre-B4 client — and round-trip cleanly.
func TestB4RequestAlertFieldsOmitempty(t *testing.T) {
	empty, err := json.Marshal(Request{Op: OpClusterStatus})
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"alert_kind", "alert_severity", "alert_message", "alert_label", "alert_dedup_key"} {
		if strings.Contains(string(empty), k) {
			t.Fatalf("empty request leaked %q: %s", k, empty)
		}
	}
	set := Request{Op: OpClusterAlertRaise, AlertKind: "manual", AlertSeverity: "severe", AlertMessage: "m", AlertLabel: "l", AlertDedupKey: "manual:l"}
	b, err := json.Marshal(set)
	if err != nil {
		t.Fatal(err)
	}
	var got Request
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, set) {
		t.Fatalf("round-trip mismatch: %+v != %+v", got, set)
	}
}

// TestB4AlertOpsAreClusterOps (plan §F.19): the two operator-alert verbs route to Backend.Cluster.
func TestB4AlertOpsAreClusterOps(t *testing.T) {
	if !clusterOps[OpClusterAlertRaise] || !clusterOps[OpClusterAlertClear] {
		t.Fatal("OpClusterAlertRaise/Clear must be in clusterOps")
	}
}

// TestB4AlertNilBackendClusterNotEnabled (plan §F.19): on a single broker (nil Backend.Cluster)
// the operator-alert verbs reply cluster_not_enabled — no panic, no Propose.
func TestB4AlertNilBackendClusterNotEnabled(t *testing.T) {
	s := New("/tmp/unused-b4.sock", Backend{}) // Cluster == nil
	for _, op := range []string{OpClusterAlertRaise, OpClusterAlertClear} {
		resp := s.dispatch(Request{Op: op})
		if resp.Code != CodeClusterNotEnabled {
			t.Fatalf("%s on nil backend: code = %q, want %q", op, resp.Code, CodeClusterNotEnabled)
		}
	}
}
