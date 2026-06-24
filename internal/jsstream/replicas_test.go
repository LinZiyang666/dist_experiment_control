package jsstream

import "testing"

// TestD5ReplicasForProperties (E-B1): the exact values + the load-bearing invariants
// (monotone non-decreasing, replicasFor(n) <= n, capped at 3). The cap catches a min(n,5)
// regression; the <= n invariant catches "never request more replicas than servers".
func TestD5ReplicasForProperties(t *testing.T) {
	cases := []struct {
		n, want int
	}{
		{-5, 1}, {0, 1}, {1, 1}, {2, 2}, {3, 3}, {4, 3}, {5, 3}, {100, 3},
	}
	prev := 0
	for _, c := range cases {
		got := ReplicasFor(c.n)
		if got != c.want {
			t.Errorf("ReplicasFor(%d) = %d, want %d", c.n, got, c.want)
		}
		if got > 3 {
			t.Errorf("ReplicasFor(%d) = %d exceeds the R<=3 cap", c.n, got)
		}
		if c.n >= 1 && got > c.n {
			t.Errorf("ReplicasFor(%d) = %d requests more replicas than servers", c.n, got)
		}
		if c.n >= 0 && got < prev {
			t.Errorf("ReplicasFor not monotone: ReplicasFor(%d)=%d < prev %d", c.n, got, prev)
		}
		if c.n >= 0 {
			prev = got
		}
	}
}

// TestD5IsMetaGroupNotReady (E-B3 classifier / internal review M1): a TRANSIENT
// placement-capacity rejection is retriable, while a PERMANENT misconfiguration (JS-10074
// non-clustered mode) and unrelated errors are NOT — else the reconcile loop spins forever
// masked by Degraded().
func TestD5IsMetaGroupNotReady(t *testing.T) {
	yes := []string{
		"no suitable peer for placement",
		"insufficient resources available", // transient placement capacity (NOT storage)
		"not enough replicas",
		"no peers available",
	}
	for _, m := range yes {
		if !IsMetaGroupNotReady(errString(m)) {
			t.Errorf("IsMetaGroupNotReady(%q) = false, want true (transient)", m)
		}
	}
	no := []string{
		"replicas > 1 not supported in non-clustered mode", // JS-10074: PERMANENT, not retriable
		"insufficient storage resources available",         // audit jsstream F3: disk-full peer is PERMANENT (retrying spins forever), not transient
		"lost connection to peers",                         // a real connectivity failure
		"stream not found",                                 // unrelated
	}
	for _, m := range no {
		if IsMetaGroupNotReady(errString(m)) {
			t.Errorf("IsMetaGroupNotReady(%q) = true, want false (permanent/unrelated)", m)
		}
	}
	if IsMetaGroupNotReady(nil) {
		t.Error("nil must not be meta-not-ready")
	}
}

type errString string

func (e errString) Error() string { return string(e) }
