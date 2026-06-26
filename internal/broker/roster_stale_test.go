package broker

import (
	"fmt"
	"testing"
	"time"
)

// roster_stale_test.go (C1 §8) — the agent_roster_stale predicate + dedup. The predicate is anchored
// on a WALL-CLOCK last-change time (not the generation value), so it is correct whether the
// generation is a small monotone counter or a legacy unix-nano timestamp.

func TestRosterStaleShouldEmit(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	old := now.Add(-2 * rosterStaleGrace).UnixNano()    // well past the grace window
	recent := now.Add(-rosterStaleGrace / 2).UnixNano() // inside the grace window
	cases := []struct {
		name                 string
		agentGen, currentGen uint64
		lastChangeNano       int64
		want                 bool
	}{
		{"cold agent gen0", 0, 5, old, false},
		{"converged equal", 5, 5, old, false},
		{"ahead", 6, 5, old, false},
		{"behind but within grace", 4, 5, recent, false},
		{"behind and past grace", 4, 5, old, true},
		// small-ordinal counter (not a timestamp): anchored on wall-clock, not time.Unix(0,42).
		{"small counter recent change → no storm", 41, 42, recent, false},
		{"small counter old change → emit", 41, 42, old, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := rosterStaleShouldEmit(c.agentGen, c.currentGen, c.lastChangeNano, now); got != c.want {
				t.Fatalf("shouldEmit(%d,%d,...)=%v want %v", c.agentGen, c.currentGen, got, c.want)
			}
		})
	}
}

func TestRosterStaleClaimDedup(t *testing.T) {
	b := &Broker{}
	if !b.rosterStaleClaim("s", "n", 5) {
		t.Fatal("first claim must succeed")
	}
	if b.rosterStaleClaim("s", "n", 5) {
		t.Fatal("re-claim at the same generation must be deduped")
	}
	if b.rosterStaleClaim("s", "n", 4) {
		t.Fatal("a lower generation must be deduped (already warned at >= )")
	}
	if !b.rosterStaleClaim("s", "n", 6) {
		t.Fatal("a higher generation must re-warn")
	}
	if !b.rosterStaleClaim("s", "other", 5) {
		t.Fatal("a different (sid,nid) is independent")
	}
}

func TestRosterStaleClaimBounded(t *testing.T) {
	b := &Broker{}
	for i := 0; i < rosterStaleDedupCap*2; i++ {
		b.rosterStaleClaim(fmt.Sprintf("s%d", i), "n", 1)
	}
	b.rosterStaleMu.Lock()
	n := len(b.rosterStaleWarned)
	b.rosterStaleMu.Unlock()
	if n > rosterStaleDedupCap {
		t.Fatalf("dedup map must stay bounded at %d, got %d", rosterStaleDedupCap, n)
	}
}

// TestRosterStaleFieldsScrubbed (Stage-C TEST-8) pins the agent_roster_stale event body to exactly
// {sid,nid,agent_gen,current_gen} so a future edit cannot silently leak a token/PSK/cert_fp/route.
func TestRosterStaleFieldsScrubbed(t *testing.T) {
	f := rosterStaleFields("s", "n", 3, 9)
	want := map[string]bool{"sid": true, "nid": true, "agent_gen": true, "current_gen": true}
	if len(f) != len(want) {
		t.Fatalf("unexpected field count %d: %v", len(f), f)
	}
	for k := range f {
		if !want[k] {
			t.Fatalf("unexpected key %q in scrubbed event (possible secret/topology leak)", k)
		}
	}
	for _, forbidden := range []string{"cert_fp", "nats_route", "public_host", "token", "psk", "secret"} {
		if _, ok := f[forbidden]; ok {
			t.Fatalf("forbidden key %q must never appear in agent_roster_stale", forbidden)
		}
	}
}

// TestMaybeEmitRosterStaleNilNoOp — a nil roster (single / non-cluster broker) must be a no-op: no
// claim recorded, no panic (byte-equivalent register reply).
func TestMaybeEmitRosterStaleNilNoOp(t *testing.T) {
	b := &Broker{}
	b.maybeEmitRosterStale("s", "n", 5, nil)
	b.rosterStaleMu.Lock()
	n := len(b.rosterStaleWarned)
	b.rosterStaleMu.Unlock()
	if n != 0 {
		t.Fatalf("nil roster must not record a claim, got %d", n)
	}
}
