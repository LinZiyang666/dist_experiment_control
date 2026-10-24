package broker

import (
	"os"
	"strings"
	"testing"
)

// proxy_cluster_guard_test.go (C5) — the build-and-prove guard: every proxy CONTROL write in cluster
// mode must go through Raft (Propose / proposeOrForward), NEVER a direct cfg.DB mutator. The single-
// mode direct mutators (session.SetProxyEnabled/BumpProxyEpoch, proxysub.Create/Revoke,
// port.AllocateProxy, the createSubAndBump/revokeSubAndBump tx helpers) would write the RODB handle in
// cluster mode (illegal — the cluster.Node owns the WAL) and fork replicated state. The ONLY DB write
// the cluster proxy path performs directly is proxy_ready, and that is a LIVENESS column via
// livenessDB() (the heartbeat path), never raft. This test enumerates the forbidden tokens in the two
// cluster-only proxy files.
func TestC5GuardNoDirectProxyWrite(t *testing.T) {
	forbidden := []string{
		"session.SetProxyEnabled",
		"session.SetProxyEnabledAndBumpEpoch",
		"session.BumpProxyEpoch",
		"proxysub.Create(",
		"proxysub.Revoke(",
		"port.AllocateProxy(",
		"createSubAndBump",
		"revokeSubAndBump",
		"bumpEpochTx",
		// B2 (Stage-C): the proxy allocation MUST be planned on the closure's live n.db under applyMu,
		// never pre-baked on the RODB cfg.DB handle (findFreePort on a stale RODB snapshot outside
		// applyMu races a concurrent expose to the same port → UNIQUE trip → FSM panic). Forbid the
		// cfg.DB form specifically (PlanAllocateProxy(db, ...) inside the Propose closure is correct).
		"PlanAllocateProxy(b.cfg.DB",
		// NOTE: NOT the bare "AllocateProxy(b.cfg.DB" — that would also match the legitimate closure form
		// if someone wrote it; the cfg.DB form above is the precise forbidden pattern. The bare direct
		// mutator "port.AllocateProxy(" is already forbidden above.
	}
	for _, file := range []string{"proxy_cluster_wire.go", "proxy_reconcile.go", "home.go"} {
		src, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("read %s: %v", file, err)
		}
		text := string(src)
		for _, tok := range forbidden {
			if strings.Contains(text, tok) {
				t.Fatalf("%s: cluster proxy path uses the single-mode direct mutator %q — control writes "+
					"MUST go through Raft (Propose/proposeOrForward), not the RODB cfg.DB handle", file, tok)
			}
		}
	}
}

// TestC5GuardReaperWritesViaPropose follows allocation and rehome through the
// maintenance helpers to their concrete guarded planners. Both decisions must
// still be made inside the leader's Propose closure after extraction.
// origin: hot-update S6 plan §7.2 / T41
func TestC5GuardReaperWritesViaPropose(t *testing.T) {
	for _, check := range []struct {
		file, function string
		calls          []string
	}{
		{"proxy_reconcile.go", "allocateProxyAndPush", []string{"b.cl.node.Propose(func(db *sql.DB)", "planHomeAllocate(b, db,"}},
		{"proxy_reconcile.go", "rehomeProxy", []string{"committedHomeMove(b, existing, target)"}},
		{"home.go", "committedHomeMove", []string{"b.cl.node.Propose(func(db *sql.DB)", "planHomeReassign(b, db,"}},
		{"home.go", "planHomeAllocate", []string{"guardHomeMutation(b, db,", "plans.Rotate(db, guard,", "plans.Allocate(db, guard,"}},
		{"home.go", "planHomeReassign", []string{"guardHomeMutation(b, db,", "plans.Reassign(db, guard,"}},
		{"home.go", "installHomeMaintenance", []string{"Reassign: port.PlanReassignHomeGuarded", "Allocate: port.PlanAllocateGuarded", "Rotate: port.PlanRotateAllocationGuarded"}},
	} {
		src, err := os.ReadFile(check.file)
		if err != nil {
			t.Fatal(err)
		}
		body := funcBodyText(t, check.file, string(src), check.function)
		for _, want := range check.calls {
			if !strings.Contains(body, want) {
				t.Errorf("%s:%s lost raft-routed guarded planner %q", check.file, check.function, want)
			}
		}
	}
}
