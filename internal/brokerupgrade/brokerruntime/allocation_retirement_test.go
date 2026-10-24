package brokerruntime

import (
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/node"
	"github.com/LinZiyang666/tether/internal/port"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// origin: S8 internal review P13-COHORT F2; hot-update S8 plan §4.2 / §6.2
func TestAllocationRetirementMatchesTheCurrentTokenAndOriginalEpoch(t *testing.T) {
	db := testharness.OpenDB(t)
	if _, err := session.Create(db, "lab", "lab", "owner", "unused-pin", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := node.Register(db, node.RegisterInput{SID: "lab", NID: "agent"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	a, err := port.Allocate(db, "lab", "agent", "original", 1234, 4100, "owner", false, &port.Config{BandLow: 4100, BandHigh: 4100})
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	proof := func(p int, sid, nid, hash string) bool {
		calls++
		return p == 4100 && sid == "lab" && nid == "agent" && hash == a.TokenHash
	}
	if !ReleasedAllocation(db, "broker_runtime", "tunnel:4100", "allocation:lab/agent@0", proof) || calls != 1 {
		t.Fatal("the exact imported allocation could not present its sealed retirement")
	}
	for _, origin := range []string{"allocation:lab/agent@1", "allocation:other/agent@0", "allocation:lab/replacement@0"} {
		if ReleasedAllocation(db, "broker_runtime", "tunnel:4100", origin, proof) {
			t.Fatal("a reused allocation borrowed the original retirement")
		}
	}
	if calls != 1 {
		t.Fatal("a mismatched creation reached the terminal observer")
	}
	if ReleasedAllocation(db, "broker_runtime", "tunnel:4100", "allocation:lab/agent@0", func(int, string, string, string) bool { return false }) {
		t.Fatal("SQL identity alone proved retirement")
	}
	if _, err := db.Exec("UPDATE port_allocations SET token_hash='replacement' WHERE port=4100"); err != nil {
		t.Fatal(err)
	}
	if ReleasedAllocation(db, "broker_runtime", "tunnel:4100", "allocation:lab/agent@0", proof) {
		t.Fatal("the old terminal discharged a replacement token")
	}
}
