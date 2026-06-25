package broker

import (
	"database/sql"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/storage"
)

// b6_incident_test.go — B6 OPS#12: the export-incident assembler's secret-scrub + allowlist
// projection (make test). Tests the pure helpers (no live node needed): a seeded DB with PoP
// material must never appear in the projected roster, and the audit Body denylist must redact
// secret-shaped keys.

func incidentTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := storage.OpenWAL("file:" + filepath.Join(t.TempDir(), "tether.db"))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(
		`INSERT INTO cluster_nodes
		 (node_id,name,node_ident_pub,nats_server_id,raft_addr,nats_route,tunnel_addr,public_host,cert_fp,phase,added_at,join_nonce,join_sig,voter_add_error,phase_changed_at)
		 VALUES('node-A','broker-A','Ipub','node-A','10.0.0.1:7400','nats://x','10.0.0.1:7443','a.ex','sha256:cc','VOTER','2026-06-24 00:00:00 +0000 UTC','SECRET_NONCE','SECRET_SIG','catch_up_stalled','2026-06-24 01:00:00 +0000 UTC')`,
	); err != nil {
		t.Fatalf("seed node: %v", err)
	}
	if _, err := db.Exec(
		`INSERT INTO alerts(id,kind,severity,dedup_key,state,message,raised_at)
		 VALUES('01','below_quorum','severe','below_quorum','ACTIVE','quorum lost','2026-06-24 00:00:00 +0000 UTC')`,
	); err != nil {
		t.Fatalf("seed alert: %v", err)
	}
	if _, err := db.Exec(
		`INSERT INTO alert_acks(dedup_key,acked_by,acked_at) VALUES('below_quorum','op','2026-06-24 00:05:00 +0000 UTC')`,
	); err != nil {
		t.Fatalf("seed ack: %v", err)
	}
	return db
}

func TestExportRosterAllowlistNoPoPLeak(t *testing.T) {
	db := incidentTestDB(t)
	roster, err := exportRoster(db)
	if err != nil {
		t.Fatalf("exportRoster: %v", err)
	}
	if len(roster) != 1 {
		t.Fatalf("want 1 node, got %d", len(roster))
	}
	n := roster[0]
	if n.NodeID != "node-A" || n.Phase != "VOTER" || n.VoterAddError != "catch_up_stalled" || n.PhaseChangedAt == "" {
		t.Fatalf("projection wrong: %+v", n)
	}
	// The PoP material must NEVER appear in the serialized projection.
	blob, _ := json.Marshal(roster)
	for _, secret := range []string{"SECRET_NONCE", "SECRET_SIG", "join_nonce", "join_sig"} {
		if strings.Contains(string(blob), secret) {
			t.Fatalf("incident roster leaked %q: %s", secret, blob)
		}
	}
}

func TestExportAlertsWithAck(t *testing.T) {
	db := incidentTestDB(t)
	alerts, err := exportAlerts(db, time.Time{})
	if err != nil {
		t.Fatalf("exportAlerts: %v", err)
	}
	if len(alerts) != 1 {
		t.Fatalf("want 1 alert, got %d", len(alerts))
	}
	a := alerts[0]
	if a.Kind != "below_quorum" || a.AckedBy != "op" || a.AckedAt == "" {
		t.Fatalf("alert export wrong: %+v", a)
	}
}

func TestScrubAuditBody(t *testing.T) {
	in := map[string]any{
		"cmd":        "mysql -psecret", // a command line is the MOST likely secret-in-cleartext (MAJOR-1)
		"pin_hash":   "deadbeef",
		"join_nonce": "abc",
		"api_token":  "xyz",
		"seed":       "s",
		"signature":  "g",
		"password":   "p",
		"exit_code":  float64(0),
		"pid":        float64(1234),
	}
	out := scrubAuditBody(in)
	// non-secret metadata survives.
	if out["exit_code"] != float64(0) || out["pid"] != float64(1234) {
		t.Fatalf("non-secret keys must survive: %+v", out)
	}
	// every secret-shaped key (incl. cmd + password) is redacted.
	for _, redacted := range []string{"cmd", "pin_hash", "join_nonce", "api_token", "seed", "signature", "password"} {
		if out[redacted] != "[redacted]" {
			t.Fatalf("key %q must be redacted, got %v", redacted, out[redacted])
		}
	}
	blob, _ := json.Marshal(out)
	for _, secret := range []string{"deadbeef", "\"abc\"", "xyz", "mysql -psecret"} {
		if strings.Contains(string(blob), secret) {
			t.Fatalf("scrubbed body leaked %q: %s", secret, blob)
		}
	}
}

// TestScrubAuditBodyNestedTarget — MAJOR-2: the denylist must recurse into the free-form nested
// AuditCall.Target map; a secret one level down must not bypass the scrub.
func TestScrubAuditBodyNestedTarget(t *testing.T) {
	in := map[string]any{
		"verb": "expose",
		"target": map[string]any{
			"token": "SHOULD_REDACT",
			"port":  float64(14022),
			"nested": map[string]any{
				"api_key": "ALSO_REDACT",
				"host":    "keep.example",
			},
		},
		"list": []any{
			map[string]any{"secret": "REDACT_IN_SLICE", "ok": "keep"},
		},
	}
	out := scrubAuditBody(in)
	target := out["target"].(map[string]any)
	if target["token"] != "[redacted]" {
		t.Fatalf("nested target.token must be redacted, got %v", target["token"])
	}
	if target["port"] != float64(14022) {
		t.Fatalf("nested target.port must survive, got %v", target["port"])
	}
	nested := target["nested"].(map[string]any)
	if nested["api_key"] != "[redacted]" || nested["host"] != "keep.example" {
		t.Fatalf("deeper nesting wrong: %+v", nested)
	}
	sliceElem := out["list"].([]any)[0].(map[string]any)
	if sliceElem["secret"] != "[redacted]" || sliceElem["ok"] != "keep" {
		t.Fatalf("slice-of-map scrub wrong: %+v", sliceElem)
	}
	blob, _ := json.Marshal(out)
	for _, leaked := range []string{"SHOULD_REDACT", "ALSO_REDACT", "REDACT_IN_SLICE"} {
		if strings.Contains(string(blob), leaked) {
			t.Fatalf("nested scrub leaked %q: %s", leaked, blob)
		}
	}
}
