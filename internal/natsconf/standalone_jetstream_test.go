package natsconf

import "testing"

// standalone_jetstream_test.go — the two INDEPENDENT facts a nats.conf carries, pinned separately.
//
// origin: batch B2 independent external RE-review RB2-1
//
// This file used to pin the compound predicates `IsStandaloneJetStream`/`IsClusteredJetStream`
// (`keyPresence && (!)hasCluster`). Those are gone, and the table below is why they had to go rather
// than be renamed: the row "cluster, no jetstream" and the row "jetstream: false + cluster" have
// DIFFERENT correct answers for the two questions, and a single predicate answering both got at least
// one of them wrong at every call site that used it.
//
//	                              IsClusteredTopology   HasJetStream
//	jetstream{store_dir} only              false            true      <- the N=1 from-existing shape
//	jetstream{} + cluster{}                 true            true      <- an ordinary voter
//	jetstream: false + cluster{}            true           FALSE      <- de-clusterable; used to refuse
//	jetstream: false, no cluster{}         false           FALSE      <- used to get an `rm -rf` advisory
//	cluster{} only, no jetstream            true           false
//	neither                                false           false
//
// Rows three and four are the two live defects RB2-1 found, and neither is expressible with one
// predicate: row three needs "clustered" to be TRUE (so force-single de-clusters it) while "JetStream
// on" is FALSE (so no store_dir is demanded); row four needs the mirror.

func TestTopologyAndJetStreamEnablementAreIndependentFacts(t *testing.T) {
	cases := []struct {
		name          string
		parsed        map[string]any
		wantClustered bool
		wantJSOn      bool
		why           string
	}{
		{
			name:   "jetstream{store_dir}, no cluster — the N=1 from-existing shape",
			parsed: map[string]any{"jetstream": map[string]any{"store_dir": "/var/lib/tether/jetstream"}},
			// standalone topology, JS on: THE migration hazard. A clustered restart in place orphans the
			// streams (test/d9 GrowStandaloneRestartWedgesJS), so this is the one shape that must get the
			// JS-store reset warning.
			wantClustered: false, wantJSOn: true,
			why: "the only shape warnStandaloneJSGrow should fire on",
		},
		{
			name:          "jetstream:true bool form, no cluster",
			parsed:        map[string]any{"jetstream": true},
			wantClustered: false, wantJSOn: true,
			why: "the scalar enable form is enablement just as much as the block form",
		},
		{
			name:          "jetstream{} + cluster{name} — an ordinary voter",
			parsed:        map[string]any{"jetstream": map[string]any{}, "cluster": map[string]any{"name": "c"}},
			wantClustered: true, wantJSOn: true,
		},
		{
			name:          "jetstream{} + EMPTY cluster{} — key presence is what makes it clustered",
			parsed:        map[string]any{"jetstream": map[string]any{}, "cluster": map[string]any{}},
			wantClustered: true, wantJSOn: true,
		},
		{
			name:          "jetstream:false + cluster{} — DEFECT ROW 1 (RB2-1)",
			parsed:        map[string]any{"jetstream": false, "cluster": map[string]any{"name": "c"}},
			wantClustered: true, wantJSOn: false,
			why: "clustered so force-single MUST de-cluster it; JS off so it must NOT be asked for a " +
				"store_dir. The old compound said 'clustered JetStream' on key presence, entered the " +
				"de-cluster arm, then died on the missing store_dir — the shape could not be de-clustered " +
				"at all, which is precisely the state an operator reaches during a JetStream incident",
		},
		{
			name:          "jetstream:disabled, no cluster — DEFECT ROW 2 (RB2-1)",
			parsed:        map[string]any{"jetstream": "disabled"},
			wantClustered: false, wantJSOn: false,
			why: "standalone topology but JS OFF, so there is no standalone JS meta to migrate. The old " +
				"compound made the takeover print `sudo rm -rf <jetstream store_dir from nats.conf>` — a " +
				"destructive instruction, with a placeholder path, on a false premise",
		},
		{
			name:          "cluster{} only, no jetstream key",
			parsed:        map[string]any{"cluster": map[string]any{}},
			wantClustered: true, wantJSOn: false,
		},
		{
			name:          "neither",
			parsed:        map[string]any{},
			wantClustered: false, wantJSOn: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			o := &Ownership{Parsed: tc.parsed}
			if got := o.IsClusteredTopology(); got != tc.wantClustered {
				t.Errorf("IsClusteredTopology()=%v, want %v. %s", got, tc.wantClustered, tc.why)
			}
			if got := o.IsStandaloneTopology(); got != !tc.wantClustered {
				t.Errorf("IsStandaloneTopology()=%v, want %v — the two must be exact complements",
					got, !tc.wantClustered)
			}
			if got := o.HasJetStream(); got != tc.wantJSOn {
				t.Errorf("HasJetStream()=%v, want %v. %s", got, tc.wantJSOn, tc.why)
			}
		})
	}
}

// TestTheTwoFactsAreNotRedundant is the non-vacuity companion: if topology and enablement always agreed
// there would be no reason to keep two predicates, and folding them back into one compound would be an
// improvement rather than the regression RB2-1 documents. The table above must therefore contain at
// least one row of each disagreeing combination.
func TestTheTwoFactsAreNotRedundant(t *testing.T) {
	clusteredJSOff := &Ownership{Parsed: map[string]any{"jetstream": false, "cluster": map[string]any{}}}
	standaloneJSOn := &Ownership{Parsed: map[string]any{"jetstream": map[string]any{"store_dir": "/x"}}}

	if !clusteredJSOff.IsClusteredTopology() || clusteredJSOff.HasJetStream() {
		t.Error("a clustered conf with JetStream explicitly OFF must read clustered=true, js=false — " +
			"this is the combination the old compound predicate could not express")
	}
	if standaloneJSOn.IsClusteredTopology() || !standaloneJSOn.HasJetStream() {
		t.Error("a standalone conf with JetStream ON must read clustered=false, js=true")
	}
}

// TestRealInstallConfIsStandaloneWithJetStreamOn locks both facts to the ACTUAL shape
// `cluster init --from-existing` leaves an N=1 broker in: the install.sh conf is standalone topology
// AND has JetStream enabled, so it is the shape the takeover must warn about before a grow.
func TestRealInstallConfIsStandaloneWithJetStreamOn(t *testing.T) {
	own, err := Preflight(writeConf(t, installSHConf))
	if err != nil {
		t.Fatalf("Preflight install.sh conf: %v", err)
	}
	if !own.IsStandaloneTopology() {
		t.Error("the install.sh (N=1 from-existing) conf must read as standalone topology")
	}
	if !own.HasJetStream() {
		t.Error("the install.sh conf enables JetStream; if this reads false the takeover would skip the " +
			"JS-store reset warning on the one node that genuinely needs it")
	}
}
