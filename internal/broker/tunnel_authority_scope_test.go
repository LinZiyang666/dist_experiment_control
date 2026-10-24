package broker

import (
	"errors"
	"fmt"
	"runtime"
	"testing"

	"github.com/LinZiyang666/tether/internal/port"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/tunnel"
)

// origin: hot-update S5 internal review rv-authority-1
func TestTunnelAuthorityRequiresProxyParticipation(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) { exerciseTunnelProxyParticipation(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "tunnel proxy participation", g)
	testharness.AssertNoFDLeak(t, "tunnel proxy participation", fd, 0)
}

func exerciseTunnelProxyParticipation(t *testing.T) {
	t.Helper()
	db := openDB(t)
	b, req := authorityFixture(t, db)
	seedClusterNode(t, b, "home", "server", "127.0.0.1:7000", "sha256:pin", "VOTER")
	for _, tc := range []struct {
		name, state string
		capable     int
		want        bool
	}{
		{"participating", "ONLINE", 1, true},
		{"opted-out", "ONLINE", 0, false},
		{"invalid-participation", "ONLINE", 2, false},
		{"stale", "STALE", 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Model a corrupt on-disk value without changing the production schema.
			if _, err := db.Exec(`PRAGMA ignore_check_constraints=ON`); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`UPDATE nodes SET proxy_capable=?,status=? WHERE sid='lab' AND nid='lab-1'`, tc.capable, tc.state); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`PRAGMA ignore_check_constraints=OFF`); err != nil {
				t.Fatal(err)
			}
			push, err := proxyAuthoritySnapshot(b, "lab", "lab-1")
			if err != nil || push.Enabled != tc.want {
				t.Fatalf("push participation = %v, error=%v", push.Enabled, err)
			}
			grant, err := tunnelAuthorityLookup(b, req)
			if tc.want {
				if err != nil || !grant.Enabled || len(grant.Keys) != 2 {
					t.Fatalf("participating node lost fresh grant: %v", err)
				}
			} else if !errors.Is(err, errTunnelAuthorityRevoked) || grant.Enabled || len(grant.Keys) != 0 {
				t.Errorf("nonparticipating node received fresh authority: enabled=%v keys=%d error=%v", grant.Enabled, len(grant.Keys), err)
			}
			if err := b.tunnelTokenLookup(req.Allocation.SID, req.Allocation.NID, req.Allocation.Port, req.Allocation.TokenHash, req.Allocation.Epoch); (err == nil) != tc.want {
				t.Errorf("legacy participation differs: %v", err)
			}
			// Ordinary expose permission survives proxy opt-out and STALE status.
			if _, err := db.Exec(`UPDATE port_allocations SET name='web' WHERE name=?`, port.ProxyPortName); err != nil {
				t.Fatal(err)
			}
			ordinary, err := tunnelAuthorityLookup(b, req)
			if err != nil || !ordinary.Enabled || len(ordinary.Keys) != 0 {
				t.Errorf("ordinary expose depends on proxy participation: %v", err)
			}
			if _, err := db.Exec(`UPDATE port_allocations SET name=? WHERE name='web'`, port.ProxyPortName); err != nil {
				t.Fatal(err)
			}
		})
	}
	t.Run("participation-in-snapshot", func(t *testing.T) {
		f := newProxyAuthorityFixture(t)
		req := tunnel.AuthRequest{Allocation: tunnel.Allocation{SID: "lab", NID: "node", Port: f.alloc.Port, TokenHash: f.alloc.TokenHash, Epoch: f.alloc.Epoch}}
		// Commit opt-out after the revision read pinned the WAL snapshot but
		// before allocation/node validation. A separate node read mixes cuts.
		f.hook.arm("FROM port_allocations", func() error {
			_, err := f.writer.Exec(`UPDATE nodes SET proxy_capable=0 WHERE sid='lab' AND nid='node'`)
			return err
		})
		old, err := tunnelAuthorityLookup(f.b, req)
		if !f.hook.fired() || err != nil || !old.Enabled || len(old.Keys) != 2 {
			t.Fatalf("authority mixed the concurrent participation cut: %v", err)
		}
		fresh, err := tunnelAuthorityLookup(f.b, req)
		if !errors.Is(err, errTunnelAuthorityRevoked) || fresh.Enabled || len(fresh.Keys) != 0 {
			t.Fatalf("fresh authority missed committed opt-out: %v", err)
		}
	})
}
