package broker

import (
	"context"
	"time"

	"github.com/LinZiyang666/tether/internal/node"
	"github.com/LinZiyang666/tether/internal/proc"
)

// reconcile_passes.go (R7a) — the broker's registered convergence passes.
//
// Every pass below obeys the one-vote-veto invariant documented at the top of
// reconcile_registry.go: read the expected state, compare it against the actual
// state, then call an ALREADY-EXISTING idempotent command path. No pass here
// invents a policy, and each one names the pre-existing path it calls.
//
// The first four are the pre-R7 inline loop bodies, moved VERBATIM. They are the
// rewrite that freezes the interface: if the registry cannot express them
// exactly — same cadence, same leadership gate, same effects — then the
// interface is wrong and the batches that build on it (R8's delivery channel,
// R9's lifecycle watchdogs, R13's status surface) would have had to reopen it.
// TestReconcileRegistryMatchesLegacyTickers proves the cadence half under a fake
// clock; TestReconcilePassEffectsMatchLegacy proves the effect half.

// reconcileLeaderGate is the registry's leadership predicate.
//
// It reproduces the pre-R7 inline condition EXACTLY (`!b.clusterMode ||
// b.cl.node.IsLeader()`), with nil guards added because the registry can be
// driven by tests that never wired a raft node. In single mode there is only one
// broker, so it is always "the leader" and leaderOnly passes always run.
func (b *Broker) reconcileLeaderGate() bool {
	if !b.clusterMode {
		return true
	}
	return b.cl != nil && b.cl.node != nil && b.cl.node.IsLeader()
}

// registerCoreReconcilePasses wires the standing passes. Called from Run exactly
// once, before the loop starts.
//
// Ordering matters only in that it is DETERMINISTIC (registration order is sweep
// order); no pass depends on another's effects within the same tick.
func (b *Broker) registerCoreReconcilePasses() {
	r := b.reconcilers

	// --- pass 1/4 of the pre-R7 rewrite: node liveness state machine ---
	//
	// Per-broker-LOCAL, deliberately not leader-only: it reads livenessDB(),
	// which in cluster mode is this broker's own liveness view of the agents
	// homed to it. Gating it on leadership would freeze every follower's node
	// states at whatever they were when leadership moved.
	//
	// Idempotent path: node.ReconcileStates (a level-triggered UPDATE keyed on
	// last_seen vs. StaleAfter/OfflineAfter — running it twice in a row is a
	// no-op).
	r.register("node-states", b.cfg.ReconcileInterval, false, func(_ context.Context, now time.Time) error {
		n, err := node.ReconcileStates(b.livenessDB(), now, b.cfg.StaleAfter, b.cfg.OfflineAfter)
		if err != nil {
			// Behavior-equivalence: the pre-R7 body LOGGED and continued. It
			// must keep doing so — returning the error here would earn the pass
			// exponential backoff and change its cadence, breaking the very
			// equivalence this rewrite has to prove.
			b.cfg.Logger.Warn("broker: reconcile failed", "err", err)
			return nil
		}
		if n > 0 {
			b.cfg.Logger.Info("broker: state transitions", "count", n)
		}
		return nil
	})

	// --- pass 2/4: OFFLINE-node port revocation (the LEADER-ONLY shape) ---
	//
	// D9 round-1 MAJOR: this scan is a leader-local DECISION that forwards a
	// PlanRevoke through raft. On a follower it would be idempotent but
	// wasteful — N brokers each re-deciding and re-forwarding the same revoke
	// every tick — so it stays leader-gated exactly as before.
	//
	// Idempotent path: b.reconcilePorts (PlanRevoke is keyed on the allocation
	// row; re-revoking an already-revoked port is a no-op).
	r.register("ports", b.cfg.ReconcileInterval, true, func(_ context.Context, now time.Time) error {
		if revoked := b.reconcilePorts(now); revoked > 0 {
			b.cfg.Logger.Info("broker: port revocations", "count", revoked)
		}
		return nil
	})

	// --- pass 3/4: stale tunnel proxy reap ---
	//
	// Per-broker: it closes THIS process's own listener fds for sessions that
	// no longer exist. Purely local, so never leader-gated.
	//
	// Idempotent path: b.reconcileTunnelSessions (closing an already-closed
	// proxy is a no-op).
	r.register("tunnel-sessions", b.cfg.ReconcileInterval, false, func(context.Context, time.Time) error {
		if closed := b.reconcileTunnelSessions(); closed > 0 {
			b.cfg.Logger.Info("broker: stale tunnel proxies closed", "count", closed)
		}
		return nil
	})

	// --- pass 4/4: process-row retention GC (the DIFFERENT-CADENCE shape) ---
	//
	// This is the pass that makes a flat single-interval table impossible: it
	// runs on ProcGCInterval (5 min), not ReconcileInterval (1s). Sweeping
	// retention 300× more often than needed would be pure write amplification.
	//
	// The cluster-mode skip is a MODE gate, not a leadership gate, and is kept
	// verbatim: `processes` is replicated state, so deleting rows outside raft
	// would fork leader/follower SQLite contents. It stays single-node-only
	// until it has a replicated command.
	//
	// Idempotent path: proc.GCExited (a DELETE bounded by a cutoff — the second
	// run over the same cutoff deletes nothing).
	r.register("proc-gc", b.cfg.ProcGCInterval, false, func(_ context.Context, now time.Time) error {
		// batch B / B3: this used b.livenessDB(), which was misleading in a way worth naming.
		// The pass is NOT a liveness write — it DELETEs from `processes`, replicated state — and
		// the mode gate above is the only thing that made the old handle safe. Asking
		// singleWriter() instead makes the restriction STRUCTURAL: it returns (nil, false) in
		// cluster mode, so deleting the guard below can no longer turn this into an
		// outside-raft write to a replicated table. The guard stays as the early, cheap exit.
		//
		// (An earlier note in this batch claimed the old code already bypassed raft. It did not —
		// the mode gate prevented it. The defect was the name, not the behaviour.)
		if b.clusterMode {
			return nil
		}
		db, ok := b.singleWriter()
		if !ok {
			// Unreachable given the gate above — which is exactly why it returns the named error
			// rather than nil. If the two ever disagree, a silent nil means `processes` rows
			// accumulate forever and nobody finds out; the reconciler logs a returned error, so
			// the contradiction becomes visible instead of becoming a slow leak.
			return singleWriteRefusal()
		}
		cutoff := now.Add(-b.cfg.ProcRetention)
		n, err := proc.GCExited(db, cutoff)
		if err != nil {
			b.cfg.Logger.Warn("broker: proc gc", "err", err)
			return nil
		}
		if n > 0 {
			b.cfg.Logger.Info("broker: proc gc", "deleted", n, "cutoff", cutoff)
		}
		return nil
	})

	// --- #58/P10: orphan xfer-object reaper, now actually periodic ---
	//
	// The reaper function is UNCHANGED. The bug was never in what it deleted;
	// it was that Run called it once, at a moment when its own first gate
	// (reaperMayDelete) is false on every cluster-mode broker, and then never
	// called it again. A boot-only call behind a boot-false gate reaps nothing,
	// ever, and tier-B object garbage accumulates until the JetStream store
	// fills.
	//
	// SAFETY OF PERIODIZING A DELETE (the R-a risk, argued explicitly):
	// the reaper skips any bucket present in transfers.activeOBJStreams(), and
	// the tracker entry — with its bucket already set — is put() BEFORE the
	// prepare is forwarded to the agent, i.e. before a single byte of the
	// object can exist. So for the entire lifetime of a live transfer its
	// bucket is excluded. The window between ensureXferBucket and put() holds
	// no objects at all, because nobody has been told the bucket exists yet.
	// The reaper is therefore STRICTLY SAFER when run periodically than at
	// boot: at boot the tracker is empty and every object looks orphan, whereas
	// at steady state the tracker is populated and live work is protected.
	//
	// NOT leader-only (#58/P10 fix): the reaper's authority is HOME, not leadership.
	// reconcileXferObjects gates internally on reaperCaughtUp (raft-domain catch-up,
	// leader OR caught-up follower) AND homeOwnsXferBucket (this broker is the session's
	// home). Home is a partition — each session has exactly one home — so every caught-up
	// broker reaps ONLY its own home's orphans and no two brokers ever touch the same
	// bucket. A leader-only pass could never reap a session homed to a follower, which is
	// the exact bug that made tier-B garbage immortal on a cluster.
	//
	// Idempotent path: b.reconcileXferObjects (deleting an already-deleted
	// object returns ErrObjectNotFound, which it swallows).
	r.register("xfer-orphan-reap", b.cfg.XferReapInterval, false, func(ctx context.Context, _ time.Time) error {
		n, err := b.reconcileXferObjects(ctx)
		if err != nil {
			return err
		}
		if n > 0 {
			b.cfg.Logger.Info("broker: orphan xfer objects reaped", "count", n)
		}
		return nil
	})

	// #57 Lane B: finalize-on-recovery. On the same cadence, close any DANGLING in-flight-transfer start
	// row whose HOME broker crashed mid-flight (the durable ledger file outlived the process but no terminal
	// was written). Runs after Run's late-wiring attaches the cluster audit sink, so the synthetic terminal
	// routes through leader Apply and its content-reqID dedups. Shares the reap cadence (no new goroutine).
	r.register("xfer-inflight-finalize", b.cfg.XferReapInterval, false, func(ctx context.Context, _ time.Time) error {
		n, err := b.finalizeStrandedXfers(ctx)
		if err != nil {
			return err
		}
		if n > 0 {
			b.cfg.Logger.Info("broker: finalized stranded in-flight transfers after a home restart", "count", n)
		}
		return nil
	})

	// --- #31: stale grow-lock reaper (the THIRD SHAPE: leader-only + backoff) ---
	//
	// Registered here; the pass body and its convergence predicate live in
	// reconcile_grow_lock.go, which explains why this is the sample that proves
	// the interface generalizes past "move the existing loop bodies".
	r.register("grow-lock", b.cfg.GrowLockReapInterval, true, b.reconcileGrowLock)

	// --- P3 (R7b): expired upgrade roll-lock reaper (the LEASE shape) ---
	//
	// Same shape as grow-lock — leader-only, own cadence, can genuinely fail
	// through raft — but its convergence predicate is a LEASE rather than
	// evidence the product wrote. See reconcile_upgrade_lock.go for why that
	// distinction is the whole reason R7 was split into a and b.
	r.register("upgrade-lock", b.cfg.UpgradeLockReapInterval, true, b.reconcileUpgradeLock)

	// --- P1 (R8a): ACTIVE home-directive delivery (the DELIVERY shape) ---
	//
	// The headline release blocker: before R8 a home change reached an agent ONLY
	// on the register reply, i.e. only if the AGENT produced a reconnect. A drain
	// notifies nobody, so a silent agent never learned its expose moved and the
	// data plane stayed pinned to the drained broker forever.
	//
	// Registering the delivery as a PASS (not as an edge fired inside DrainNode) is
	// the whole point: a one-shot publish from the drain would be lost the moment
	// the agent was momentarily unreachable, which is the same "delivery depends on
	// a lucky event" bug in a new costume. Level-triggered re-delivery is what makes
	// "the agent is completely silent" a survivable state.
	//
	// Leader-only: it re-delivers what the leader's own homeForRegister computes
	// from replicated rows; N brokers pushing the same assignment every tick would
	// be pure amplification (the same argument as the `ports` pass).
	//
	// Idempotent path: agent.applyHomeDirectives — the SAME function the register
	// reply drives, epoch-monotone and documented non-tearing at equal epoch. This
	// pass invents no directive: homeForRegister is the single builder for both
	// delivery paths, so they cannot drift.
	r.register("home-delivery", b.cfg.HomeDeliverInterval, true, b.reconcileHomeDelivery)
}
