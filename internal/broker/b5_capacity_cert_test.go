package broker

import (
	"database/sql"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
)

// b5_capacity_cert_test.go — reviewer-added (B5 cert-capacity-correctness lens). The
// pre-existing TestB5CertExpiryAdvisory only exercises the PURE advisory helper. These
// drive the full StatusReport round-trip on a real single-node raft: the cert literal
// (LitTime, time.String() format) survives the sql.NullTime scan, capacity is self-row
// ONLY + ports_used is home_broker=selfID-scoped (plan test #16), and the advisory
// NEVER changes health/ExitCode (plan test #14).

// b5SeedPortParent inserts the sessions+nodes parent rows a port_allocations FK requires
// (foreign_keys=1 on the cluster node DB), then one ALLOCATED port row homed at the given
// broker. Driven directly (not via the FSM) — the test only needs the row present for the
// COUNT, not a replicated write.
func b5SeedPortParent(t *testing.T, n *cluster.Node, port int, homeBroker string) {
	t.Helper()
	if err := n.BoundedStaleRead(func(db *sql.DB) error {
		if _, err := db.Exec(`INSERT OR IGNORE INTO sessions(sid,name,owner_pubkey_fp,pin_hash,state,created_at) VALUES('s1','n','fp','h','ACTIVE',?)`, time.Now().UTC()); err != nil {
			return err
		}
		if _, err := db.Exec(`INSERT OR IGNORE INTO nodes(nid,sid,status,registered_at) VALUES('nd','s1','ONLINE',?)`, time.Now().UTC()); err != nil {
			return err
		}
		_, err := db.Exec(`INSERT INTO port_allocations(port,sid,nid,name,local_port,token_hash,state,created_by_fp,created_at,home_broker)
		   VALUES(?,'s1','nd','exp',22,'th','ALLOCATED','fp',?,?)`, port, time.Now().UTC(), homeBroker)
		return err
	}); err != nil {
		t.Fatalf("seed port row: %v", err)
	}
}

// TestB5CapacityIsSelfRowAndHomeScoped (plan §F.16/§F.9): ports_used counts ONLY the
// rows THIS broker homes (home_broker=selfID); a row homed at a DIFFERENT broker must
// not inflate the band. disk_free_pct is filled (StoreDir set, real tmp dir). Both ride
// ONLY the self row.
func TestB5CapacityIsSelfRowAndHomeScoped(t *testing.T) {
	n, addr := d7SingleNode(t, "single-1")
	admin := NewClusterAdmin(n, nil)
	in := d7JoinInput(t, "single-1", addr)
	caughtUp := func(b uint64) (bool, error) { c, e := n.AppliedIndex(); return c >= b, e }
	if err := admin.AddNode(in, addr, caughtUp, 5*time.Second); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	// One ALLOCATED port homed HERE, one homed at a PEER broker (must NOT be counted).
	b5SeedPortParent(t, n, 14001, "single-1")
	b5SeedPortParent(t, n, 14002, "other-broker")

	admin.SetCapacityProbes(t.TempDir(), 14000, 14999) // band of 1000; real dir → disk known

	rep, err := admin.StatusReport("ctl-nats")
	if err != nil {
		t.Fatalf("StatusReport: %v", err)
	}
	var found bool
	for _, nd := range rep.Nodes {
		if nd.NodeID != "single-1" {
			// A peer row (none here, but assert the invariant) must NEVER carry capacity.
			if nd.PortsUsed != 0 || nd.DiskFreePct != 0 || nd.PortsTotal != 0 {
				t.Errorf("peer %q must have no capacity (self-row only): %+v", nd.NodeID, nd)
			}
			continue
		}
		found = true
		if nd.PortsUsed != 1 {
			t.Errorf("ports_used = %d, want 1 (only home_broker=single-1; the peer's ALLOCATED 14002 must NOT count)", nd.PortsUsed)
		}
		if nd.PortsTotal != 1000 {
			t.Errorf("ports_total = %d, want 1000 (band high-low+1)", nd.PortsTotal)
		}
		if nd.DiskFreePct <= 0 || nd.DiskFreePct > 100 {
			t.Errorf("disk_free_pct = %d, want a real 1..100 (StoreDir is set)", nd.DiskFreePct)
		}
	}
	if !found {
		t.Fatalf("self row single-1 missing from %+v", rep.Nodes)
	}
}

// TestB5CapacityAbsentWithoutProbes (plan §F.17): with NO SetCapacityProbes call (the
// default a test/single broker leaves), the self-row capacity fields stay ABSENT (0) and
// no disk syscall is forced — honest "not configured".
func TestB5CapacityAbsentWithoutProbes(t *testing.T) {
	n, addr := d7SingleNode(t, "single-1")
	admin := NewClusterAdmin(n, nil)
	in := d7JoinInput(t, "single-1", addr)
	caughtUp := func(b uint64) (bool, error) { c, e := n.AppliedIndex(); return c >= b, e }
	if err := admin.AddNode(in, addr, caughtUp, 5*time.Second); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	// No SetCapacityProbes → storeDir "", portBandHigh 0.
	rep, err := admin.StatusReport("ctl-nats")
	if err != nil {
		t.Fatalf("StatusReport: %v", err)
	}
	for _, nd := range rep.Nodes {
		if nd.DiskFreePct != 0 || nd.PortsUsed != 0 || nd.PortsTotal != 0 {
			t.Errorf("node %q must have absent capacity without probes: disk=%d pu=%d pt=%d",
				nd.NodeID, nd.DiskFreePct, nd.PortsUsed, nd.PortsTotal)
		}
	}
}

// TestB5CertRoundTripAndAdvisoryNoHealthChange (plan §F.14): a rotated cert's literal
// (LitTime → time.String() format) survives the StatusReport sql.NullTime scan, the
// derived CertValidSecs is positive, and an IN-WINDOW (near-expiry) cert appends the
// advisory to the banner WITHOUT changing Health/ExitCode vs a fresh cert.
func TestB5CertRoundTripAndAdvisoryNoHealthChange(t *testing.T) {
	n, addr := d7SingleNode(t, "single-1")
	admin := NewClusterAdmin(n, nil)
	in := d7JoinInput(t, "single-1", addr)
	caughtUp := func(b uint64) (bool, error) { c, e := n.AppliedIndex(); return c >= b, e }
	if err := admin.AddNode(in, addr, caughtUp, 5*time.Second); err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	// Baseline (no cert window pinned): record health/exit.
	base, err := admin.StatusReport("ctl-nats")
	if err != nil {
		t.Fatalf("baseline StatusReport: %v", err)
	}

	// Rotate with a SHORT window so CertValidSecs lands inside window/8 → advisory.
	// window/8 of 24h = 3h; pick valid_until = now + 1h (well within), window arg 24h.
	if err := n.Propose(func(*sql.DB) (*cluster.Command, error) {
		return cluster.PlanClusterCertRotate("single-1", "sha256:NEW", admin.now().Add(time.Hour))
	}); err != nil {
		t.Fatalf("cert rotate propose: %v", err)
	}

	rep, err := admin.StatusReport("ctl-nats")
	if err != nil {
		t.Fatalf("StatusReport after rotate: %v", err)
	}
	var sawSecs int64
	for _, nd := range rep.Nodes {
		if nd.NodeID == "single-1" {
			sawSecs = nd.CertValidSecs
			if nd.CertFP != "sha256:NEW" {
				t.Errorf("cert_fp = %q, want sha256:NEW (round-trip)", nd.CertFP)
			}
		}
	}
	// The LitTime literal MUST have scanned into a usable time — a failed scan would make
	// StatusReport error out entirely; a silent Valid=false would leave CertValidSecs 0.
	if sawSecs <= 0 || sawSecs > int64((90*time.Minute).Seconds()) {
		t.Fatalf("CertValidSecs = %d, want ~3600 (LitTime→sql.NullTime round-trip)", sawSecs)
	}
	// Advisory appended, health/exit UNCHANGED from the no-cert baseline.
	if rep.Health != base.Health {
		t.Errorf("near-expiry cert changed Health %q -> %q (must be advisory-only)", base.Health, rep.Health)
	}
	if rep.ExitCode != base.ExitCode {
		t.Errorf("near-expiry cert changed ExitCode %d -> %d (must be advisory-only)", base.ExitCode, rep.ExitCode)
	}
	if rep.Banner == base.Banner {
		t.Errorf("near-expiry cert should append an ADVISORY to the banner; banner unchanged %q", rep.Banner)
	}
}
