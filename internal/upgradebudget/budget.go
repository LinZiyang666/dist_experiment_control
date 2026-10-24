// Package upgradebudget is the single source of the hot-update acceptance
// budgets (docs/reviews/hot-update-plan.md §7) and the derived bounds the
// switch itself uses. Every number that a test, a drill or a runbook quotes
// about the switch comes from here: the deploy-tier shell single source
// (test/simcluster/drills/lib/hot_update_budget.sh) is reconciled against
// these constants by a gate, so "adjusting a keepalive by hand" cannot
// silently move an acceptance window the way the NATS ping interval once
// did (simcluster-speed H).
//
// The package imports only the standard library: it is a leaf every layer
// (workload, agent, cmd) may read, and it must never grow a dependency on
// any of them.
//
// Nothing here is a claim that the product meets the number; the acceptance
// profile is frozen before the first full acceptance run (plan §7), and a
// failed sample never raises a budget — the plan text changes first, with
// the user-visible impact spelled out.
// origin: docs/reviews/hot-update-agent-bootstrap-plan.md §6.
package upgradebudget

import "time"

const (
	// LocalFreezeWindow is the budget from pausing the old image's pumps to
	// the new image's pumps being usable again (exec, Go init, import and
	// thaw included). S3 only records samples against it.
	LocalFreezeWindow = 500 * time.Millisecond
	// RequestLatency bounds every new control request end to end during an
	// upgrade (queueing, freeze and retries included); it is also the upper
	// bound of the bounded home-state probe the preflight runs on a hangable
	// home.
	RequestLatency = 5 * time.Second
	// ProbeRatePerSecond is the mixed-probe rate the acceptance drill drives
	// (reads, writes with side effects and short commands).
	ProbeRatePerSecond = 10
	// A fixed maintenance lease covers admission plus clock/proof margin. It
	// never renews from a late response or a newly elected leader.
	// origin: hot-update S6 plan §7.1 / §13
	BrokerMaintenanceTTL = RequestLatency + 3*time.Second
	// LiveFlowStall is the longest a live flow's probe may see no progress
	// while its carrier is being switched (S5).
	LiveFlowStall = 5 * time.Second
	// TransferProgress is the interval within which a transfer must show
	// durable progress on a healthy, unthrottled arm (S7).
	TransferProgress = 10 * time.Second
	// ComponentHandover bounds one component's handover once its artifact
	// is staged; for the agent it is the register deadline of a pending
	// upgrade marker.
	ComponentHandover = 120 * time.Second
	// FullStackRollout bounds a baseline full-stack rolling upgrade (S10).
	FullStackRollout = 15 * time.Minute
	// StreamReplayBytes is the default unacknowledged replay per logical
	// stream; NodeReplayBytes the per-node total. Both back-pressure or
	// defer the upgrade instead of killing live work.
	StreamReplayBytes int64 = 8 << 20
	NodeReplayBytes   int64 = 256 << 20
	// MaxGenerations is the most images that may coexist across a chain of
	// switches before the oldest must be gone (S8).
	MaxGenerations = 2

	// SafePointWait bounds the wait for a safe point inside a cut: the
	// registry's in-flight creation is the only thing the fence waits for,
	// and it must not consume the handover budget.
	SafePointWait = 5 * time.Second
	// ReplyDrain bounds the flush of the OK reply (and the heartbeat sent
	// ahead of the cut) before the image is exec'd; it replaces the bare
	// 100 ms sleep the legacy re-exec used.
	ReplyDrain = 100 * time.Millisecond
	// RollbackCutRetry is the interval at which a rollback that could not
	// reach a safe point (a deferral) is retried without killing anything.
	RollbackCutRetry = 5 * time.Second
	// BootBudget is how many boots of a staged image the marker allows while
	// pending before the boot check rolls back.
	BootBudget = 3

	// PTYInputBytes caps accepted but unapplied input; each protocol batch
	// is smaller so output, control, and keepalive remain schedulable.
	// origin: hot-update S4 plan §2.3 / §3.2
	PTYInputBytes = 256 << 10
	PTYBatchBytes = 32 << 10
	// PTYRecovery is one absolute client recovery window, not a new grant
	// of authority. It covers the register watchdog and a rollback cut,
	// boot, and register margin without renewing on each failed attempt.
	PTYRecovery = 180 * time.Second
	// PTYRecoveryOpportunity is granted once per keepalive expiry, even
	// across image generations; import must not restart this opportunity.
	PTYRecoveryOpportunity = 5 * time.Second
)
