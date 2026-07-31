package brokermetrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestRenderSingleModeHonest (B5 plan §F.2): a single broker emits cluster_mode 0 + alerts_active,
// but NO raft/peer gauges — it never fabricates a flat applied_index 0 that reads as a stuck raft.
func TestRenderSingleModeHonest(t *testing.T) {
	var sb strings.Builder
	Render(&sb, Snapshot{ClusterMode: false, AlertsActive: 2})
	out := sb.String()
	if !strings.Contains(out, "tether_broker_cluster_mode 0") {
		t.Errorf("single mode must emit cluster_mode 0:\n%s", out)
	}
	if !strings.Contains(out, "tether_broker_alerts_active 2") {
		t.Errorf("alerts_active must be present in single mode:\n%s", out)
	}
	for _, forbidden := range []string{"is_leader", "voters", "applied_index", "peer_applied_lag", "xfer_unreapable"} {
		if strings.Contains(out, forbidden) {
			t.Errorf("single mode must NOT emit cluster gauge %q:\n%s", forbidden, out)
		}
	}
}

// TestRenderClusterEveryGaugePresentAndEscaped (B5 plan §F.3): cluster mode emits every gauge
// (zero values included, never omitted), and peer labels are escaped.
func TestRenderClusterEveryGaugePresentAndEscaped(t *testing.T) {
	var sb strings.Builder
	Render(&sb, Snapshot{
		ClusterMode: true, IsLeader: true, Voters: 3, QuorumMargin: 1,
		AppliedIndex: 100, CommitIndex: 100, ForceSingle: false, AlertsActive: 0,
		XferUnreapableBuckets: 2,
		Peers: []PeerSnap{
			{NodeID: `brk"b`, AppliedLag: 5, Reachable: true},
			{NodeID: "brk-c", AppliedLag: 0, Reachable: false},
		},
	})
	out := sb.String()
	for _, want := range []string{
		"tether_broker_is_leader 1", "tether_broker_voters 3", "tether_broker_quorum_margin 1",
		"tether_broker_applied_index 100", "tether_broker_commit_index 100",
		"tether_broker_force_single 0", "tether_broker_alerts_active 0",
		"tether_broker_xfer_unreapable_buckets 2",         // N-6: always-present cluster gauge, value flows from the snapshot
		`tether_broker_peer_applied_lag{node="brk\"b"} 5`, // escaped quote
		`tether_broker_peer_reachable{node="brk-c"} 0`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing gauge line %q in:\n%s", want, out)
		}
	}
}

func newTestServer(snap Snapshot, ready func() (bool, string)) *httptest.Server {
	return httptest.NewServer(Handler(func() Snapshot { return snap }, ready))
}

// TestReadyzBands (B5 plan §F.4): single → 200; cluster with a leader → 200; cluster without a
// leader → 503. /healthz always 200; bad method → 405; unknown path → 404.
func TestEndpointBands(t *testing.T) {
	srv := newTestServer(Snapshot{ClusterMode: true}, func() (bool, string) { return true, "leader=brk-a" })
	defer srv.Close()
	get := func(path string) (int, string) {
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		buf := make([]byte, 256)
		n, _ := resp.Body.Read(buf)
		return resp.StatusCode, string(buf[:n])
	}
	if code, _ := get("/healthz"); code != 200 {
		t.Errorf("/healthz = %d, want 200", code)
	}
	if code, _ := get("/readyz"); code != 200 {
		t.Errorf("/readyz with leader = %d, want 200", code)
	}
	if code, _ := get("/metrics"); code != 200 {
		t.Errorf("/metrics = %d, want 200", code)
	}
	if code, _ := get("/nope"); code != 404 {
		t.Errorf("unknown path = %d, want 404", code)
	}
	// non-GET → 405
	resp, err := http.Post(srv.URL+"/metrics", "text/plain", nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("POST /metrics = %d, want 405", resp.StatusCode)
	}

	// not-ready (no leader) → 503
	srv2 := newTestServer(Snapshot{ClusterMode: true}, func() (bool, string) { return false, "no leader" })
	defer srv2.Close()
	resp2, err := http.Get(srv2.URL + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp2.Body.Close()
	if resp2.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("/readyz without leader = %d, want 503", resp2.StatusCode)
	}
}

// TestMetricsSnapshotPanicRecovered (Stage-C m5): a panicking snapshot closure must NOT crash the
// broker process — /metrics recovers, and a subsequent /healthz still answers 200.
func TestMetricsSnapshotPanicRecovered(t *testing.T) {
	srv := httptest.NewServer(Handler(func() Snapshot { panic("boom") }, func() (bool, string) { return true, "ok" }))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/metrics") // panics inside, recovered
	if err != nil {
		t.Fatalf("/metrics request failed (server crashed?): %v", err)
	}
	_ = resp.Body.Close()
	// the server must still be up
	h, err := http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatalf("/healthz after a panicking /metrics: %v", err)
	}
	_ = h.Body.Close()
	if h.StatusCode != http.StatusOK {
		t.Errorf("/healthz after panic = %d, want 200", h.StatusCode)
	}
}

// TestEscapeLabel (Stage-C N2): the exposition label escaper handles backslash, quote, and newline.
func TestEscapeLabel(t *testing.T) {
	if got := escapeLabel(`a\b"c` + "\n" + "d"); got != `a\\b\"c\nd` {
		t.Errorf("escapeLabel = %q, want %q", got, `a\\b\"c\nd`)
	}
}
