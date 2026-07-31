package broker

import (
	"slices"
	"testing"

	"github.com/LinZiyang666/tether/internal/proto"
)

// origin: g1g7_audit_test.go (renamed in B6) — the G1–G7 cross-cutting audit's broker-side pins;
// docs/reviews/g1-external-review.md and the g4/g5/g7 review documents alongside it.

// origin: batch-A A9; renamed during line-2 review PF-7, which found the function-name freeze's regex
// had no `A` in its batch-letter list and so had let this one through.
//
// TestRebalanceTargetsExcludeDrainingBroker pins that BOTH proxy-home target selectors exclude a broker that
// is mid-drain: DrainNode raises the broker_draining marker + migrates exposes BEFORE flipping
// VOTER->DRAINING, so a draining node is still phase=VOTER (otherwise eligible) in that window and must never
// be chosen as a rebalance/rehome target (mirrors the allocatePort DrainingNodes guard). brk-b here is
// reachable + healthy, so the ONLY reason to exclude it is the draining marker — a sense-lock against
// dropping the filter.
func TestRebalanceTargetsExcludeDrainingBroker(t *testing.T) {
	db := openDB(t)
	b := &Broker{cfg: Config{DB: db, Logger: silentLogger(), PublicHost: "a.example"}, selfID: "brk-a"}
	b.clusterMode = true
	seedClusterNodeHost(t, b, "brk-a", "a.example", "sha256:a", "VOTER")
	seedClusterNodeHost(t, b, "brk-b", "b.example", "sha256:b", "VOTER") // reachable + deliverable, but draining
	if _, err := db.Exec(`INSERT INTO cluster_meta(key,value) VALUES(?,?)`, "draining:brk-b", "9999999999999999999"); err != nil {
		t.Fatalf("seed draining marker: %v", err)
	}

	homes := b.eligibleProxyHomes()
	if slices.Contains(homes, "brk-b") {
		t.Fatalf("A9: eligibleProxyHomes must exclude the draining broker brk-b: %v", homes)
	}
	if !slices.Contains(homes, "brk-a") {
		t.Fatalf("A9: the non-draining self voter brk-a must remain eligible: %v", homes)
	}
	if got := b.pickProxyRehomeTarget("brk-c"); got == "brk-b" {
		t.Fatalf("A9: pickProxyRehomeTarget must not choose the draining broker brk-b, got %q", got)
	}
}

// TestCutoverRestartDecision pins the A1 three-way liveness→action mapping that stops a transient monitor
// probe error from SIGKILLing a healthy clustered broker mid-grow-resume. `clustered` dominates (never
// bounce a clustered data plane); an up-standalone nats is SIGKILL'd to revive it; a down/unreachable nats
// is only waited on (systemd Restart=always revives it — SIGKILLing an absent process would just error).
func TestCutoverRestartDecision(t *testing.T) {
	cases := []struct {
		name                 string
		clustered, reachable bool
		want                 cutoverAction
	}{
		{"healthy clustered → no bounce", true, true, cutoverAlreadyClustered},
		{"clustered dominates even without the reachable flag", true, false, cutoverAlreadyClustered},
		{"up but standalone → SIGKILL to revive clustered", false, true, cutoverSIGKILLToRevive},
		{"down/unreachable → await systemd revival, no SIGKILL", false, false, cutoverAwaitRevival},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := cutoverRestartDecision(c.clustered, c.reachable); got != c.want {
				t.Fatalf("cutoverRestartDecision(%v,%v) = %v, want %v", c.clustered, c.reachable, got, c.want)
			}
		})
	}
}

// TestSeedHostMatchDisjointAllDeadIsKnownTradeoff pins the A14 audit's KNOWN, DELIBERATE tradeoff. The
// provenance-free clobber guard (OQ-B) cannot tell a stored seed set that points ONLY at departed brokers
// from an operator-curated VIP/LB floor — BOTH have zero host-match against the current roster. So
// deriveAndConvergeSeedsFromRoster hands off (keeps the stale set) rather than risk clobbering a VIP floor.
// This is the documented flip side of TestG3SeedHostMatchProtectsVIP; a "fix" that rebuilt on a disjoint
// set would REGRESS VIP protection. Reversing it needs seed provenance (a maintainer call, deferred).
func TestSeedHostMatchDisjointAllDeadIsKnownTradeoff(t *testing.T) {
	stored := []string{
		"wss://dead-a.example.com:443/nats",
		"wss://dead-b.example.com:443/nats",
	}
	roster := []proto.RosterBroker{
		{NodeID: "survivor", PublicHost: "survivor.example.com", Phase: proto.RosterPhaseVoter},
	}
	if seedHostsMatchAnyBroker(stored, roster) {
		t.Fatal("A14: a disjoint all-dead stored set must match NO broker (provenance-free guard = indistinguishable " +
			"from a VIP floor); the caller hands off. Rebuilding here would clobber real VIP floors.")
	}
	// Sanity: a set that DOES contain the survivor's host matches (the guard fires → rebuild), so the
	// tradeoff is specific to the disjoint case, not a blanket hands-off.
	withSurvivor := append(stored, "wss://survivor.example.com:443/nats")
	if !seedHostsMatchAnyBroker(withSurvivor, roster) {
		t.Fatal("a stored set containing a current broker's host must match (the guard rebuilds it)")
	}
}
