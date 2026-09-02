package broker

import (
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/proto"
)

// seed_helper_test.go (formerly g3_seed_helper_test.go) — G3 #1 DB-backed tests for deriveAndConvergeSeedsFromRoster on a single-node
// raft (make test). Pins: first-publish-stays-manual, the deterministic change-gate (no needless
// seed_generation bump), bootstrap read-back preservation, shrink convergence (a stale endpoint drops +
// gen bumps), the host-match clobber guard (an operator VIP set is left alone), and the empty-set floor
// (an all-undialable roster keeps the stored set, never wipes). Multi-broker GROW convergence is proven
// by the pure-function TestDeriveSeedEndpoints "grow converge" case + the deploy-tier sim drill.

func g3AdminWithSelf(t *testing.T, host string) *ClusterAdmin {
	t.Helper()
	n, addr := d7SingleNode(t, "self")
	admin := NewClusterAdmin(n, nil)
	in := d7JoinInput(t, "self", addr)
	in.PublicHost = host
	caughtUp := func(barrier uint64) (bool, error) { cur, err := n.AppliedIndex(); return cur >= barrier, err }
	if err := admin.AddNode(in, addr, caughtUp, 5*time.Second); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	return admin
}

func TestG3SeedFirstPublishStaysManual(t *testing.T) {
	admin := g3AdminWithSelf(t, "host.example")
	// AddNode already ran the helper once with no seeds → it must NOT bootstrap a first publish.
	if eps, _, _, err := admin.ReadSeeds(); err != nil || len(eps) != 0 {
		t.Fatalf("first publish must stay manual (no auto-bootstrap), got %v err=%v", eps, err)
	}
}

func TestG3SeedChangeGateAndBootstrapPreserve(t *testing.T) {
	admin := g3AdminWithSelf(t, "host.example")
	if _, err := admin.PublishSeeds([]string{"wss://host.example:443"}, "https://boot.example/manifest"); err != nil {
		t.Fatalf("PublishSeeds: %v", err)
	}
	_, _, gen0, _ := admin.ReadSeeds()
	// derived set == stored set → change-gate must skip (no seed_generation bump, no fleet re-adopt).
	if err := admin.deriveAndConvergeSeedsFromRoster(); err != nil {
		t.Fatalf("helper: %v", err)
	}
	_, boot, gen1, _ := admin.ReadSeeds()
	if gen1 != gen0 {
		t.Errorf("change-gate: unchanged set must not bump seed_generation: %d -> %d", gen0, gen1)
	}
	if boot != "https://boot.example/manifest" {
		t.Errorf("bootstrap must be preserved (read-back), got %q", boot)
	}
}

func TestG3SeedShrinkConvergesDropsStale(t *testing.T) {
	admin := g3AdminWithSelf(t, "host.example")
	// stored carries self + a stale endpoint whose host is no longer any broker; host.example matches self
	// → the helper OWNS this set and rebuilds it to the current roster (just self), dropping the stale one.
	if _, err := admin.PublishSeeds([]string{"wss://host.example:443", "wss://stale.example:443"}, "https://boot.example/m"); err != nil {
		t.Fatalf("PublishSeeds: %v", err)
	}
	_, _, gen0, _ := admin.ReadSeeds()
	if err := admin.deriveAndConvergeSeedsFromRoster(); err != nil {
		t.Fatalf("helper: %v", err)
	}
	eps, boot, gen1, _ := admin.ReadSeeds()
	if strings.Contains(strings.Join(eps, ","), "stale.example") {
		t.Errorf("shrink: stale endpoint must be dropped, got %v", eps)
	}
	if len(eps) != 1 || eps[0] != "wss://host.example:443" {
		t.Errorf("shrink: converged set must be just the live broker, got %v", eps)
	}
	if gen1 <= gen0 {
		t.Errorf("shrink: a real change must bump seed_generation: %d -> %d", gen0, gen1)
	}
	if boot != "https://boot.example/m" {
		t.Errorf("bootstrap must survive the converge republish, got %q", boot)
	}
}

// TestG3AsyncRetireTerminalIncludesSeedConvergence pins the deploy-tier A3
// failure: the operation controller (the production retire path) used to mark
// RETIRED without ever invoking the helper exercised above. With no future
// leadership edge, the signed seed bundle advertised the retired endpoint
// forever even though the roster deletion and operation were terminal.
func TestG3AsyncRetireTerminalIncludesSeedConvergence(t *testing.T) {
	admin := g3AdminWithSelf(t, "self.example")
	if _, err := admin.PublishSeeds([]string{"wss://self.example:443", "wss://retired.example:443"}, ""); err != nil {
		t.Fatalf("PublishSeeds: %v", err)
	}
	_, _, gen0, _ := admin.ReadSeeds()
	now := time.Date(2026, 7, 16, 0, 0, 0, 0, time.UTC)
	start := cluster.OpStartInput{
		OpID: "op-retire-seeds", Kind: cluster.OpKindRetire, TargetNode: "retired",
		InitState: cluster.OpStateNatsRolledOut, Confirmed: true, Timeline: `[{"s":"NATS_ROLLED_OUT"}]`,
	}
	if err := admin.node.Propose(func(*sql.DB) (*cluster.Command, error) {
		return cluster.PlanClusterOpStart(start, now)
	}); err != nil {
		t.Fatalf("seed retire operation: %v", err)
	}
	op, err := cluster.OperationByID(admin.node.RODB(), start.OpID)
	if err != nil || op == nil {
		t.Fatalf("read seeded operation: op=%+v err=%v", op, err)
	}
	admin.driveRetire(op, substrate{})

	got, err := cluster.OperationByID(admin.node.RODB(), start.OpID)
	if err != nil || got == nil || !got.Terminal || got.OpState != cluster.OpStateRetired {
		t.Fatalf("retire did not reach terminal after seed convergence: op=%+v err=%v", got, err)
	}
	eps, _, gen1, err := admin.ReadSeeds()
	if err != nil {
		t.Fatal(err)
	}
	if len(eps) != 1 || eps[0] != "wss://self.example:443" {
		t.Fatalf("terminal RETIRED still advertises retired endpoint: %v", eps)
	}
	if gen1 <= gen0 {
		t.Fatalf("terminal RETIRED did not advance seed_generation: %d -> %d", gen0, gen1)
	}
}

func TestG3SeedHostMatchProtectsVIP(t *testing.T) {
	admin := g3AdminWithSelf(t, "host.example")
	// stored is an operator-curated VIP set whose host matches NO broker → the helper must be hands-off.
	if _, err := admin.PublishSeeds([]string{"wss://vip.lb.example:443"}, ""); err != nil {
		t.Fatalf("PublishSeeds: %v", err)
	}
	_, _, gen0, _ := admin.ReadSeeds()
	if err := admin.deriveAndConvergeSeedsFromRoster(); err != nil {
		t.Fatalf("helper: %v", err)
	}
	eps, _, gen1, _ := admin.ReadSeeds()
	if len(eps) != 1 || eps[0] != "wss://vip.lb.example:443" {
		t.Errorf("VIP set must be left untouched, got %v", eps)
	}
	if gen1 != gen0 {
		t.Errorf("VIP protection: no republish, seed_generation must not bump: %d -> %d", gen0, gen1)
	}
}

func TestG3SeedEmptyFloorKeepsStored(t *testing.T) {
	admin := g3AdminWithSelf(t, "localhost") // self public_host is undialable
	// stored matches self (localhost) → helper takes over, but derive yields empty (all undialable) → the
	// empty-set floor must keep the stored set (never wipe / strand cold-start clients) and not bump.
	if _, err := admin.PublishSeeds([]string{"wss://localhost:443"}, ""); err != nil {
		t.Fatalf("PublishSeeds: %v", err)
	}
	_, _, gen0, _ := admin.ReadSeeds()
	if err := admin.deriveAndConvergeSeedsFromRoster(); err != nil {
		t.Fatalf("helper: %v", err)
	}
	eps, _, gen1, _ := admin.ReadSeeds()
	if len(eps) != 1 || eps[0] != "wss://localhost:443" {
		t.Errorf("empty-set floor: stored set must be kept, got %v", eps)
	}
	if gen1 != gen0 {
		t.Errorf("empty-set floor: no republish, seed_generation must not bump: %d -> %d", gen0, gen1)
	}
}

// m14: multi-broker GROW convergence at the helper/DB layer (the single-self tests above cannot surface a
// glue bug that only appears with ≥2 roster rows — phase-tier mishandling, host-match guard mis-firing).
func TestG3SeedGrowConvergesMultiBroker(t *testing.T) {
	n, addr := d7SingleNode(t, "self")
	admin := NewClusterAdmin(n, nil)
	in := d7JoinInput(t, "self", addr)
	in.PublicHost = "self.example"
	caughtUp := func(barrier uint64) (bool, error) { cur, err := n.AppliedIndex(); return cur >= barrier, err }
	if err := admin.AddNode(in, addr, caughtUp, 5*time.Second); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	if _, err := admin.PublishSeeds([]string{"wss://self.example:443"}, ""); err != nil { // host-match self → helper owns it
		t.Fatalf("PublishSeeds: %v", err)
	}
	_, _, gen0, _ := admin.ReadSeeds()
	// Add a SECOND roster row (a joined broker) via the phase-1 upsert — no raft AddVoter needed; derive
	// reads cluster_nodes, not the raft config.
	b2 := d7JoinInput(t, "b2", "10.0.0.2:7000")
	b2.PublicHost = "b2.example"
	if err := n.Propose(func(*sql.DB) (*cluster.Command, error) { return cluster.PlanClusterNodeUpsert(b2) }); err != nil {
		t.Fatalf("upsert b2 roster row: %v", err)
	}
	if err := admin.deriveAndConvergeSeedsFromRoster(); err != nil {
		t.Fatalf("helper: %v", err)
	}
	eps, _, gen1, _ := admin.ReadSeeds()
	joined := strings.Join(eps, ",")
	if !strings.Contains(joined, "self.example") || !strings.Contains(joined, "b2.example") {
		t.Fatalf("grow: seeds must contain BOTH brokers, got %v", eps)
	}
	if gen1 <= gen0 {
		t.Errorf("grow: seed_generation must bump once, %d -> %d", gen0, gen1)
	}
}

// TestG3SeedThirdVoterConvergesViaLeaderMaintenance is the #46 / SB-91 regression at the helper layer.
// It reproduces the TRIGGER gap (NOT a pure-derivation bug — DeriveSeedEndpoints handles 3 voters fine):
// two more brokers reach VOTER in the roster WITHOUT any op re-running seed convergence (the per-grow
// converge fires once, gated behind topoAdvance, and the only re-trigger was the leadership-acquired edge
// that never re-fires on a stable single leader). The seeds therefore stay stale — the LAST voter has no
// successor grow to rescue it. Then driveLeaderMaintenance (the fix, wired into the observe loop's leader
// tick) re-converges and the 3rd voter finally enters `seeds show`; a 2nd tick is idempotent (no churn).
func TestG3SeedThirdVoterConvergesViaLeaderMaintenance(t *testing.T) {
	n, addr := d7SingleNode(t, "self")
	admin := NewClusterAdmin(n, nil)
	self := d7JoinInput(t, "self", addr)
	self.PublicHost = "self.example"
	caughtUp := func(barrier uint64) (bool, error) { cur, err := n.AppliedIndex(); return cur >= barrier, err }
	if err := admin.AddNode(self, addr, caughtUp, 5*time.Second); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	if _, err := admin.PublishSeeds([]string{"wss://self.example:443"}, ""); err != nil { // host-match self → helper owns it
		t.Fatalf("PublishSeeds: %v", err)
	}

	// brk2 + brk3 join the roster and reach VOTER — but NO converge runs (the #46 gap: the grow-op
	// converge was skipped/no-op'd and no leadership edge fires on this stable single leader).
	for _, b := range []struct{ id, host string }{{"b2", "b2.example"}, {"b3", "b3.example"}} {
		in := d7JoinInput(t, b.id, "10.0.0.9:7000")
		in.PublicHost = b.host
		if err := n.Propose(func(*sql.DB) (*cluster.Command, error) { return cluster.PlanClusterNodeUpsert(in) }); err != nil {
			t.Fatalf("upsert %s: %v", b.id, err)
		}
		if err := n.Propose(func(*sql.DB) (*cluster.Command, error) {
			return cluster.PlanClusterNodePhase(b.id, proto.RosterPhaseVoter,
				[]string{proto.RosterPhasePending, proto.RosterPhaseVoter}, "", time.Now().UTC())
		}); err != nil {
			t.Fatalf("promote %s to VOTER: %v", b.id, err)
		}
	}

	// Bug state: with no converge trigger the seeds still advertise only self (b2 AND b3 absent).
	eps, _, gen0, _ := admin.ReadSeeds()
	if len(eps) != 1 || eps[0] != "wss://self.example:443" {
		t.Fatalf("precondition: without a converge trigger seeds must still be stale, got %v", eps)
	}

	// The fix: the observe loop's per-tick leader maintenance re-converges from the roster.
	admin.driveLeaderMaintenance()

	eps, _, gen1, _ := admin.ReadSeeds()
	joined := strings.Join(eps, ",")
	if !strings.Contains(joined, "self.example") || !strings.Contains(joined, "b2.example") || !strings.Contains(joined, "b3.example") {
		t.Fatalf("#46: leader maintenance must converge ALL voters including the 3rd, got %v", eps)
	}
	if gen1 <= gen0 {
		t.Fatalf("#46: a real convergence must bump seed_generation once: %d -> %d", gen0, gen1)
	}

	// Idempotent: a 2nd maintenance tick on the converged set must NOT churn seed_generation (routine
	// ticks must never force the fleet to re-adopt).
	admin.driveLeaderMaintenance()
	if _, _, gen2, _ := admin.ReadSeeds(); gen2 != gen1 {
		t.Fatalf("#46: maintenance on a converged set must be a no-op, seed_generation churned: %d -> %d", gen1, gen2)
	}
}

// m15: the leadership backstop (ReconcileMembershipOnLeadership) must converge stale seeds AND be a no-op
// on an already-converged set (routine elections must not churn the fleet's seed_generation).
func TestG3SeedBackstopConvergesAndIsIdempotent(t *testing.T) {
	admin := g3AdminWithSelf(t, "self.example")
	if _, err := admin.PublishSeeds([]string{"wss://self.example:443", "wss://stale.example:443"}, ""); err != nil {
		t.Fatalf("PublishSeeds: %v", err)
	}
	_, _, gen0, _ := admin.ReadSeeds()
	if err := admin.ReconcileMembershipOnLeadership(); err != nil {
		t.Fatalf("backstop: %v", err)
	}
	eps, _, gen1, _ := admin.ReadSeeds()
	if strings.Contains(strings.Join(eps, ","), "stale.example") {
		t.Errorf("backstop must converge (drop the stale endpoint): %v", eps)
	}
	if gen1 <= gen0 {
		t.Errorf("backstop must bump seed_generation on a real convergence: %d -> %d", gen0, gen1)
	}
	// Second call on the now-converged set → no bump (idempotent; the change-gate keeps elections from churning).
	if err := admin.ReconcileMembershipOnLeadership(); err != nil {
		t.Fatalf("backstop 2: %v", err)
	}
	if _, _, gen2, _ := admin.ReadSeeds(); gen2 != gen1 {
		t.Errorf("backstop on a converged set must be a no-op (no churn): %d -> %d", gen1, gen2)
	}
}
