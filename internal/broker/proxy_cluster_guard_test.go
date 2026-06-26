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
	for _, file := range []string{"proxy_cluster_wire.go", "proxy_reconcile.go"} {
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

// TestC5GuardReaperWritesViaPropose asserts the reaper's allocation + rehome both go through
// b.cl.node.Propose (the leader-local raft write), the token-correct path.
func TestC5GuardReaperWritesViaPropose(t *testing.T) {
	src, err := os.ReadFile("proxy_reconcile.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(src)
	for _, want := range []string{"b.cl.node.Propose", "PlanAllocateProxy", "PlanReassignHome"} {
		if !strings.Contains(text, want) {
			t.Fatalf("proxy_reconcile.go: expected the reaper to use %q (raft-routed data plane)", want)
		}
	}
}
