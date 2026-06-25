package proto

import "testing"

// TestEvalDestructiveGate is the EXIT-B corroboration table (§10.4): severe gates fire on
// genuine quorum loss / force-single but NOT on a confirmed leader, a brief election (a node
// that recently heard a leader), pure client jitter (zero replies), or a single lagging
// follower while another confirms a writable leader.
func TestEvalDestructiveGate(t *testing.T) {
	stale := func(wl, st, fs bool) ClusterHealthResp {
		return ClusterHealthResp{WritableLeaderConfirmed: wl, LeaderContactStale: st, ForceSingleActive: fs}
	}
	cases := []struct {
		name        string
		replies     []ClusterHealthResp
		wantQuorum  bool
		wantForce   bool
		wantBlocked bool
	}{
		{"zero replies = ctl jitter / non-cluster => no gate", nil, false, false, false},
		{"confirmed writable leader => no gate",
			[]ClusterHealthResp{stale(true, false, false), stale(false, false, false)}, false, false, false},
		{"all followers stale, no leader => quorum_lost gate",
			[]ClusterHealthResp{stale(false, true, false), stale(false, true, false)}, true, false, true},
		{"brief election: a follower recently heard a leader => no gate",
			[]ClusterHealthResp{stale(false, false, false), stale(false, true, false)}, false, false, false},
		{"force_single reported => force gate (even with a writable leader present)",
			[]ClusterHealthResp{stale(true, false, true)}, false, true, true},
		{"single lagging follower but leader confirmed elsewhere => no gate",
			[]ClusterHealthResp{stale(false, true, false), stale(true, false, false)}, false, false, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			g := EvalDestructiveGate(c.replies)
			if g.QuorumLost != c.wantQuorum || g.ForceSingleActive != c.wantForce || g.Blocked() != c.wantBlocked {
				t.Fatalf("gate=%+v blocked=%v, want quorum=%v force=%v blocked=%v",
					g, g.Blocked(), c.wantQuorum, c.wantForce, c.wantBlocked)
			}
		})
	}
}
