// budget_test.go — the relations the budgets must keep between themselves.
// The cross-package equalities (workload's replay defaults, the agent's
// register deadline) are pinned by the owning packages' tests so this leaf
// stays import-free.
// origin: docs/reviews/hot-update-agent-bootstrap-plan.md §6.
package upgradebudget

import (
	"testing"
	"time"
)

func TestBudgetRelations(t *testing.T) {
	if SafePointWait >= ComponentHandover {
		t.Fatalf("SafePointWait %v must be a small part of ComponentHandover %v", SafePointWait, ComponentHandover)
	}
	if ReplyDrain >= LocalFreezeWindow {
		t.Fatalf("ReplyDrain %v must leave most of LocalFreezeWindow %v to the switch itself", ReplyDrain, LocalFreezeWindow)
	}
	if RollbackCutRetry >= ComponentHandover {
		t.Fatalf("RollbackCutRetry %v must fit many times into ComponentHandover %v", RollbackCutRetry, ComponentHandover)
	}
	if StreamReplayBytes <= 0 || NodeReplayBytes < StreamReplayBytes {
		t.Fatalf("replay budgets: stream %d node %d", StreamReplayBytes, NodeReplayBytes)
	}
	if MaxGenerations < 2 || BootBudget < 1 || ProbeRatePerSecond < 1 {
		t.Fatalf("counts: generations %d boots %d probes/s %d", MaxGenerations, BootBudget, ProbeRatePerSecond)
	}
}

// origin: hot-update S4 plan §2.3 / §7.2
func TestPTYBudgetRelations(t *testing.T) {
	if PTYInputBytes != 256<<10 || PTYBatchBytes != 32<<10 || PTYRecovery != 180*time.Second || PTYRecoveryOpportunity != 5*time.Second {
		t.Fatalf("PTY budgets changed: input=%d batch=%d recovery=%v opportunity=%v", PTYInputBytes, PTYBatchBytes, PTYRecovery, PTYRecoveryOpportunity)
	}
	if PTYBatchBytes <= 0 || PTYBatchBytes > PTYInputBytes || PTYInputBytes > StreamReplayBytes {
		t.Fatal("PTY batch must fit input replay, which must fit a stream reservation")
	}
	// One watchdog, one deferred-cut retry and safe-point wait, reply drain,
	// local exec/boot window, and a bounded register request must fit before
	// the original ctl gives up. None of these terms renews its deadline.
	margin := RollbackCutRetry + SafePointWait + ReplyDrain + LocalFreezeWindow + RequestLatency
	if PTYRecovery <= ComponentHandover+margin {
		t.Fatalf("PTY recovery %v must exceed watchdog %v plus cut/boot/register margin %v", PTYRecovery, ComponentHandover, margin)
	}
	if PTYRecoveryOpportunity <= 0 || PTYRecoveryOpportunity >= PTYRecovery || PTYRecoveryOpportunity > RequestLatency {
		t.Fatal("one keepalive recovery opportunity must be bounded by a normal request")
	}
	if StreamReplayBytes+PTYInputBytes > NodeReplayBytes {
		t.Fatal("one terminal's input and output reservations must fit the node budget")
	}
}
