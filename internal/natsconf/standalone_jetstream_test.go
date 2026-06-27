package natsconf

import "testing"

// TestIsStandaloneJetStream pins the standalone-JS detector that gates the grow's JS-store
// reset warning (cmd/tether/cluster_natsconf.go) — a node running JetStream WITHOUT a
// cluster{} block cannot be restarted clustered in place (the meta wedges; test/d9
// GrowStandaloneRestartWedgesJS), so this must fire on exactly that shape.
func TestIsStandaloneJetStream(t *testing.T) {
	cases := []struct {
		name   string
		parsed map[string]any
		want   bool
	}{
		{"jetstream, no cluster ⇒ standalone", map[string]any{"jetstream": map[string]any{"store_dir": "/var/lib/tether/jetstream"}}, true},
		{"jetstream:true bool form, no cluster ⇒ standalone", map[string]any{"jetstream": true}, true},
		{"jetstream + cluster ⇒ clustered", map[string]any{"jetstream": map[string]any{}, "cluster": map[string]any{"name": "c"}}, false},
		{"jetstream + EMPTY cluster{} ⇒ treated clustered (cluster key present)", map[string]any{"jetstream": map[string]any{}, "cluster": map[string]any{}}, false},
		{"cluster, no jetstream ⇒ not JS", map[string]any{"cluster": map[string]any{}}, false},
		{"neither", map[string]any{}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			o := &Ownership{Parsed: tc.parsed}
			if got := o.IsStandaloneJetStream(); got != tc.want {
				t.Errorf("IsStandaloneJetStream()=%v, want %v", got, tc.want)
			}
		})
	}
}

// TestIsClusteredJetStream pins the REVERSE predicate (v0.4.2 shrink): it fires on exactly
// jetstream{} + cluster{} (a voter that must be reset to standalone when it shrinks to N=1), and is
// false for standalone (jetstream, no cluster) and for non-JS confs.
func TestIsClusteredJetStream(t *testing.T) {
	cases := []struct {
		name   string
		parsed map[string]any
		want   bool
	}{
		{"jetstream + cluster ⇒ clustered", map[string]any{"jetstream": map[string]any{}, "cluster": map[string]any{"name": "c"}}, true},
		{"jetstream + empty cluster{} ⇒ clustered (cluster key present)", map[string]any{"jetstream": map[string]any{}, "cluster": map[string]any{}}, true},
		{"jetstream, no cluster ⇒ standalone, NOT clustered", map[string]any{"jetstream": map[string]any{"store_dir": "/x"}}, false},
		{"cluster, no jetstream ⇒ not JS", map[string]any{"cluster": map[string]any{}}, false},
		{"neither", map[string]any{}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			o := &Ownership{Parsed: tc.parsed}
			if got := o.IsClusteredJetStream(); got != tc.want {
				t.Errorf("IsClusteredJetStream()=%v, want %v", got, tc.want)
			}
		})
	}
}

// TestIsStandaloneJetStreamRealInstallConf locks the detector to the ACTUAL shape `cluster
// init --from-existing` leaves an N=1 broker in: the install.sh conf (jetstream{} + no
// cluster{}) must be flagged standalone, so the takeover warns before the operator grows.
func TestIsStandaloneJetStreamRealInstallConf(t *testing.T) {
	own, err := Preflight(writeConf(t, installSHConf))
	if err != nil {
		t.Fatalf("Preflight install.sh conf: %v", err)
	}
	if !own.IsStandaloneJetStream() {
		t.Fatal("the install.sh (N=1 from-existing) conf must be detected as standalone JetStream")
	}
}
