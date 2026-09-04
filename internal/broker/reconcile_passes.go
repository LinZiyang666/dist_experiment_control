package broker

import (
	"context"
	"database/sql"
	"log/slog"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/node"
	"github.com/LinZiyang666/tether/internal/port"
	"github.com/LinZiyang666/tether/internal/proc"
	"github.com/LinZiyang666/tether/internal/session"
)

// h1 B1 GC bounds. gcChunkRows caps the keyset baked into ONE raft entry
// (bounds both the entry size and the Apply txn); maxGCChunksPerTick bounds
// how many sequential synchronous Proposes one pass tick may issue — worst
// case the tick blocks for one raft-apply timeout before the registry's own
// error handling takes over. 10×500 rows / 5min drains the incident's 24k-row
// backlog in ~25 minutes without ever building a mega-entry. portRetention is
// deliberately a const, not a knob (h1 plan Q4): FREED/REVOKED rows are pure
// history; 24h of it is plenty for `ps -a` forensics.
const (
	gcChunkRows        = 500
	maxGCChunksPerTick = 10
	portRetention      = 24 * time.Hour
)

// gcProposeChunks drives one cluster-mode GC pass tick: plan a ≤gcChunkRows
// keyset chunk on the leader DB, Propose it, repeat until a chunk comes back
// empty or the per-tick chunk budget is spent. A FREE function, not a Broker
// method, on purpose (h1 plan X-2: the type-methods ledger hand-raise for
// this increment is spent on respondBytes).
//
// Gates: leader-only is enforced by the pass's authorityLeader registration;
// reaperCaughtUp() keeps a just-elected leader from planning deletes off a
// still-catching-up FSM view (same gate the xfer reaper uses). Propose errors
// return to the registry, whose per-pass backoff is the damper.
func gcProposeChunks(b *Broker, pass string, plan func(db *sql.DB) (*cluster.Command, int, error)) error {
	if b.cl == nil || b.cl.node == nil {
		return nil // cluster wiring not up yet (boot window)
	}
	if !b.reaperCaughtUp() {
		return nil
	}
	return gcDrain(b.cfg.Logger, pass, b.cl.node.Propose, plan)
}

// gcDrain is gcProposeChunks' loop with the raft node lifted out as a
// `propose` parameter, so the two bounds that matter — at most
// maxGCChunksPerTick Proposes per tick, and STOP at the first empty chunk
// (idle-zero-writes: an idle GC pass must issue no raft writes at all) — are
// directly testable without standing up a cluster.
func gcDrain(
	logger *slog.Logger,
	pass string,
	propose func(func(*sql.DB) (*cluster.Command, error)) error,
	plan func(db *sql.DB) (*cluster.Command, int, error),
) error {
	total := 0
	for i := 0; i < maxGCChunksPerTick; i++ {
		n := 0
		err := propose(func(db *sql.DB) (*cluster.Command, error) {
			cmd, planned, perr := plan(db)
			n = planned
			return cmd, perr
		})
		if err != nil {
			return err
		}
		if n == 0 {
			break
		}
		total += n
	}
	if total > 0 {
		logger.Info("broker: storage gc proposed", "pass", pass, "rows", total)
	}
	return nil
}

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
	r.register("node-states", b.cfg.ReconcileInterval, authorityAny, func(_ context.Context, now time.Time) error {
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
	r.register("ports", b.cfg.ReconcileInterval, authorityLeader, func(_ context.Context, now time.Time) error {
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
	r.register("tunnel-sessions", b.cfg.ReconcileInterval, authorityAny, func(context.Context, time.Time) error {
		if closed := b.reconcileTunnelSessions(); closed > 0 {
			b.cfg.Logger.Info("broker: stale tunnel proxies closed", "count", closed)
		}
		return nil
	})

	// --- pass 4: process-row retention GC (the DIFFERENT-CADENCE shape) ---
	//
	// This is the pass that makes a flat single-interval table impossible: it
	// runs on ProcGCInterval (5 min), not ReconcileInterval (1s). Sweeping
	// retention 300× more often than needed would be pure write amplification.
	//
	// h1 B1: the old `if b.clusterMode { return nil }` MODE gate is replaced by
	// a real cluster branch. That skip was correct for its stated reason —
	// `processes` is replicated state, deleting outside raft forks the FSM —
	// but its consequence was that a cluster-mode fleet had NO retention at
	// all: the 2026-08-04 incident broker (force-single N=1) had accumulated
	// 8,479 EXITED rows with nothing ever able to delete them. The cluster
	// branch routes the same retention decision through raft (OpProcGC,
	// leader-planned explicit pid keysets — see proc.PlanGCExited), so the
	// pass is now authorityLeader: in single mode that predicate is vacuously
	// true (byte-identical behavior); in cluster mode only the leader plans
	// and Proposes.
	//
	// Idempotent path: single mode proc.GCExited (cutoff-bounded DELETE);
	// cluster mode keyed DELETE + re-asserted status guard (replay no-ops).
	r.register("proc-gc", b.cfg.ProcGCInterval, authorityLeader, func(_ context.Context, now time.Time) error {
		if b.clusterMode {
			return gcProposeChunks(b, "proc-gc", func(db *sql.DB) (*cluster.Command, int, error) {
				return proc.PlanGCExited(db, now.Add(-b.cfg.ProcRetention), gcChunkRows)
			})
		}
		db, ok := b.singleWriter()
		if !ok {
			// Unreachable given the mode branch above — which is exactly why it returns the
			// named error rather than nil. If the two ever disagree, a silent nil means
			// `processes` rows accumulate forever and nobody finds out; the reconciler logs a
			// returned error, so the contradiction becomes visible instead of a slow leak.
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

	// --- pass 5: port-allocation history retention GC (h1 B1) ---
	//
	// port_allocations FREED/REVOKED rows had NO retention in ANY mode — the
	// gap behind the 2026-08-04 `tether ps` incident (a hot reaper-rotation
	// loop minted one FREED `__proxy__` row per 20s for five weeks; 24k rows
	// pushed the unbounded ps reply past max_payload). Same cadence and the
	// same single/cluster split as proc-gc; retention is the hardcoded
	// portRetention (24h — no knob until someone asks; h1 plan Q4).
	r.register("port-gc", b.cfg.ProcGCInterval, authorityLeader, func(_ context.Context, now time.Time) error {
		if b.clusterMode {
			return gcProposeChunks(b, "port-gc", func(db *sql.DB) (*cluster.Command, int, error) {
				return port.PlanGCTerminated(db, now.Add(-portRetention), gcChunkRows)
			})
		}
		db, ok := b.singleWriter()
		if !ok {
			return singleWriteRefusal()
		}
		cutoff := now.Add(-portRetention)
		n, err := port.GCTerminated(db, cutoff)
		if err != nil {
			b.cfg.Logger.Warn("broker: port gc", "err", err)
			return nil
		}
		if n > 0 {
			b.cfg.Logger.Info("broker: port gc", "deleted", n, "cutoff", cutoff)
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
	r.register("xfer-orphan-reap", b.cfg.XferReapInterval, authorityAny, func(ctx context.Context, _ time.Time) error {
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
	r.register("xfer-inflight-finalize", b.cfg.XferReapInterval, authorityAny, func(ctx context.Context, _ time.Time) error {
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
	r.register("grow-lock", b.cfg.GrowLockReapInterval, authorityLeader, b.reconcileGrowLock)

	// --- P3 (R7b): expired upgrade roll-lock reaper (the LEASE shape) ---
	//
	// Same shape as grow-lock — leader-only, own cadence, can genuinely fail
	// through raft — but its convergence predicate is a LEASE rather than
	// evidence the product wrote. See reconcile_upgrade_lock.go for why that
	// distinction is the whole reason R7 was split into a and b.
	r.register("upgrade-lock", b.cfg.UpgradeLockReapInterval, authorityLeader, b.reconcileUpgradeLock)

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
	r.register("home-delivery", b.cfg.HomeDeliverInterval, authorityLeader, b.reconcileHomeDelivery)

	// batch C: clear broker_draining markers whose node has no roster row left. See
	// reconcile_drain_marker.go for why this is the ONLY case it clears, and why the two wider
	// predicates the refactor roadmap proposed would each break something. Idempotent path:
	// cluster.PlanClusterDrainSet(node, nil), the same command AbortDrain and the retire ladder use.
	r.register("drain-marker", b.cfg.DrainMarkerReapInterval, authorityLeader, b.reconcileDrainMarkers)

	// --- the one-shot session-create admission backfill, RETRIED UNTIL IT COMMITS ---
	//
	// origin: prerelease audit external review M-2. The backfill also runs once in Run,
	// and for a while that was the ONLY place it ran, on the reasoning that "a broker that
	// boots while an election is in progress tries again on its next boot". A rolling
	// upgrade has no next boot: it is followers-first/leader-last, so every new broker
	// starts while the OLD leader is still serving and returns early, and the final step
	// transfers leadership to an already-running new follower before restarting the old
	// leader — which comes back as a follower and skips it too. Nothing was left to retry,
	// so the marker and the rows never appeared on any node.
	//
	// As a PASS it is level-triggered instead: whoever holds leadership keeps answering
	// "is this done yet?" until the marker is committed, which also covers a transient
	// Propose failure without needing a restart. Once seeded it is a single indexed read
	// per tick, and the latch below skips even that.
	//
	// Leader-only for the same reason `ports` is: the decision is leader-local and the
	// result reaches every replica through the log, so N brokers re-deciding it would be
	// pure amplification.
	//
	// Idempotent path: seedSessionCreators re-reads the marker inside the Propose closure
	// under the leader's applyMu, and a nil command is Propose's documented no-op.
	r.register("session-creator-seed", b.cfg.ReconcileInterval, authorityLeader,
		func(_ context.Context, now time.Time) error { return seedCreatorsPass(b, now) })
}

// seedCreatorsPass is the body of the session-creator-seed reconcile pass.
//
// Extracted rather than inlined for the reason the maintidx gate exists: this block took
// registerCoreReconcilePasses past the god-function threshold, and the gate's whole value
// is that an eleventh god function needs a deliberate edit — so the answer is a name, not
// a new exemption. A package-level function taking *Broker, not a method, because the
// structural-budget ratchet pins this type's method count exactly.
//
// origin: prerelease audit external review M-2.
func seedCreatorsPass(b *Broker, now time.Time) error {
	if b.creatorsSeeded.Load() {
		return nil
	}
	if seeded, err := session.CreatorsSeeded(b.read().SQL()); err == nil && seeded {
		b.creatorsSeeded.Store(true)
		return nil
	}
	n, err := seedSessionCreators(b, now)
	if err != nil {
		// LOG AND CONTINUE, like the node-states pass: returning the error would earn this
		// pass exponential backoff, and the one situation it exists for — a cluster that is
		// mid-upgrade and briefly cannot satisfy the mixed-version capability gate — is
		// exactly when backing off is wrong.
		b.cfg.Logger.Warn("broker: session-creator upgrade backfill", "err", err)
		return nil
	}
	if n > 0 {
		b.cfg.Logger.Info("broker: grandfathered existing session owners into the "+
			"session-create allow-list", "count", n)
	}
	return nil
}
