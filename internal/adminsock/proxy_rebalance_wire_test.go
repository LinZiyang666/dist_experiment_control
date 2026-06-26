package adminsock

import (
	"encoding/json"
	"strings"
	"testing"
)

// proxy_rebalance_wire_test.go (C-rebalance) — wire-compat + secret-free guards for the rebalance
// request/response additions (R3 review). The report travels over the local admin socket back to the
// CLI; it must stay additive (old CLI tolerates the new field) and must NEVER carry a token/psk/key.

// TestProxyRebalanceWireAdditive: the new Response.ProxyRebalance / Request.DryRun are additive +
// omitempty — an OLD CLI (a struct without the field) decodes a new broker's reply without error, and a
// request without --dry-run omits the field entirely.
func TestProxyRebalanceWireAdditive(t *testing.T) {
	resp := Response{Op: OpClusterRebalanceProxy, OK: true, ProxyRebalance: &ProxyRebalanceReport{
		Voters: 3, Proxies: 4, Planned: 1, Moves: []ProxyRebalanceMove{{SID: "s", NID: "n", Port: 1, From: "a", To: "b", Done: true}},
	}}
	raw, err := json.Marshal(resp)
	if err != nil {
		t.Fatal(err)
	}
	// Old CLI: a Response shape WITHOUT proxy_rebalance must still decode cleanly (ignore unknown key).
	var old struct {
		Op string `json:"op"`
		OK bool   `json:"ok"`
	}
	if err := json.Unmarshal(raw, &old); err != nil {
		t.Fatalf("old CLI must tolerate the new reply: %v", err)
	}
	if !old.OK || old.Op != OpClusterRebalanceProxy {
		t.Errorf("old decode lost base fields: %+v", old)
	}

	// A request without --dry-run omits dry_run entirely (omitempty).
	if b, _ := json.Marshal(Request{Op: OpClusterRebalanceProxy}); strings.Contains(string(b), "dry_run") {
		t.Errorf("dry_run must be omitted when false: %s", b)
	}
	if b, _ := json.Marshal(Request{Op: OpClusterRebalanceProxy, DryRun: true}); !strings.Contains(string(b), `"dry_run":true`) {
		t.Errorf("dry_run must serialize when set: %s", b)
	}
	// OpClusterRebalanceProxy must be routed to the cluster backend.
	if !clusterOps[OpClusterRebalanceProxy] {
		t.Error("OpClusterRebalanceProxy missing from the clusterOps routing map")
	}
}

// TestProxyRebalanceReportSecretFree: the report's JSON key set is exactly the home-metadata fields —
// no token/psk/key/secret/cipher ever appears (the report goes to the operator's terminal).
func TestProxyRebalanceReportSecretFree(t *testing.T) {
	raw, _ := json.Marshal(ProxyRebalanceReport{
		Voters: 2, Proxies: 2, Planned: 1,
		Moves: []ProxyRebalanceMove{{SID: "s", NID: "n", Port: 1, From: "a", To: "b", Done: true, Error: "x"}},
	})
	low := strings.ToLower(string(raw))
	for _, forbidden := range []string{"token", "psk", "secret", "cipher", "\"key\"", "keys"} {
		if strings.Contains(low, forbidden) {
			t.Errorf("rebalance report leaks a secret-shaped field %q: %s", forbidden, raw)
		}
	}
}
