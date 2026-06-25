// alert_reconcile.go (D8b §10) is the leader-gated alert reconciler. It is a SEPARATE long-lived
// goroutine — deliberately NOT folded into the audit publisher tick (§16 D8(d)): folding it
// there would (1) invert liveness (PublishOnce can early-return on an un-ACKed publish exactly
// when the cluster is degraded — the moment the degraded alert must fire), (2) starve the audit
// drain (an alert Propose blocking under applyMu stalls the §6.3 publish), and (3) break D5
// idle-zero-writes (a level-triggered re-propose every tick writes a fresh raft entry forever).
// So it has its own IsLeader && !LeaderContactStale gate, its own ctx-child, and proposes a
// raise/clear ONLY on a genuine desired-vs-current TRANSITION.
//
// Post-D9 cutover it is wired in CLUSTER mode (wireClusterLate starts rec.Run); SINGLE mode does
// not construct it. (The per-phase TestD8ProductionWiresNoCluster guard was removed at cutover.)
package broker

import (
	"context"
	"database/sql"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
)

// alertClusterReader is the narrow raft-free view the reconciler consumes (*cluster.Node
// satisfies it; tests substitute a fake). Keeping it an interface holds the L-2 line.
type alertClusterReader interface {
	IsLeader() bool
	LeaderContactStale(time.Time) bool
	NumVoters() (int, error)
}

// AlertReconcilerConfig wires the reconciler. Node + DB + Propose are required.
type AlertReconcilerConfig struct {
	Node    alertClusterReader
	DB      *sql.DB
	Propose func(plan func(*sql.DB) (*cluster.Command, error)) error
	Now     func() time.Time
	Logger  *slog.Logger
	Poll    time.Duration
	// Observe is the READ-ONLY replica observation (broker AuditPublisher.ObserveReplicas).
	// nil disables the replication_degraded kind (events/history/xfer not observed here).
	Observe func(context.Context) (ReplicaReport, error)
	// Webhook (B6 OPS#2), when non-nil, is fed every COMMITTED alert transition (the delta of
	// the committed ACTIVE set between passes) — NOT the raise/clear decision list, which can be
	// proposed-but-not-committed (leadership lost mid-pass). nil disables the webhook entirely.
	Webhook func(WebhookEvent)
	// LeaderID returns the current leader's node id (stamped into the webhook body).
	LeaderID func() string
}

// AlertReconciler raises/clears the store-backed alerts the leader can cheaply determine:
// replication_degraded (from Observe), below_quorum (from NumVoters), broker_draining (from
// the cluster_meta draining markers). raft_lag + broker_down are writerless until D9 (the
// leader cannot read per-peer cursor/liveness pre-D9). disk_pressure is forwarded by each
// broker's disk monitor (alert_forward.go), and manual is operator-driven — neither is owned
// here, so the clear pass NEVER touches them.
type AlertReconciler struct {
	cfg AlertReconcilerConfig
	// webhook committed-delta state (leader-local, single-goroutine — no lock needed): the
	// committed ACTIVE set observed on the PREVIOUS leader pass. webhookSeeded is reset whenever
	// this node is not leader, so on (re)gaining leadership the first pass establishes a baseline
	// WITHOUT firing (else a new leader would re-POST every already-active alert).
	prevActive    map[string]bool
	webhookSeeded bool
}

func NewAlertReconciler(cfg AlertReconcilerConfig) *AlertReconciler {
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if cfg.Poll <= 0 {
		cfg.Poll = 500 * time.Millisecond
	}
	return &AlertReconciler{cfg: cfg}
}

// Run ticks the reconciler until ctx is cancelled. Each tick is leader-gated; a non-leader or
// fence-stale node does nothing (no reads, no writes).
func (r *AlertReconciler) Run(ctx context.Context) {
	t := time.NewTicker(r.cfg.Poll)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := r.ReconcileAlertsOnce(ctx); err != nil {
				r.cfg.Logger.Warn("d8b: alert reconcile pass", "err", err)
			}
		}
	}
}

type alertSpec struct {
	key, kind, severity, message string
}

// fireWebhookDelta posts every committed raised/cleared transition between the previous leader
// pass and `current`. The first pass after (re)gaining leadership only seeds the baseline.
func (r *AlertReconciler) fireWebhookDelta(current map[string]bool, now time.Time) {
	if r.cfg.Webhook == nil {
		return
	}
	if !r.webhookSeeded {
		r.prevActive = cloneKeySet(current)
		r.webhookSeeded = true
		return
	}
	leader := ""
	if r.cfg.LeaderID != nil {
		leader = r.cfg.LeaderID()
	}
	ts := now.UTC().Format(time.RFC3339Nano)
	for key := range current {
		if !r.prevActive[key] {
			r.cfg.Webhook(r.webhookEvent("raised", key, leader, ts))
		}
	}
	for key := range r.prevActive {
		if !current[key] {
			r.cfg.Webhook(r.webhookEvent("cleared", key, leader, ts))
		}
	}
	r.prevActive = cloneKeySet(current)
}

// webhookEvent builds a transition event, enriching it with the alert's kind/severity/message
// (most-recent row for the dedup_key) + the node parsed from a node-scoped key.
func (r *AlertReconciler) webhookEvent(transition, key, leader, ts string) WebhookEvent {
	ev := WebhookEvent{Transition: transition, DedupKey: key, ClusterLeader: leader, Ts: ts}
	if i := strings.IndexByte(key, ':'); i >= 0 {
		ev.Node = key[i+1:]
	}
	var kind, severity, message string
	if err := r.cfg.DB.QueryRow(
		`SELECT kind, severity, message FROM alerts WHERE dedup_key=? ORDER BY raised_at DESC LIMIT 1`, key,
	).Scan(&kind, &severity, &message); err == nil {
		ev.Kind, ev.Severity, ev.Message = kind, severity, message
	}
	return ev
}

func cloneKeySet(m map[string]bool) map[string]bool {
	out := make(map[string]bool, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// NOTE: the loop OWNS only replication_degraded / below_quorum / broker_draining:* — each
// clear path below is scoped to its own key, so disk_pressure (forwarded) and manual
// (operator) are never retired here.

// ReconcileAlertsOnce runs one leader-gated pass: compute desired raise/clear decisions for
// the owned kinds against the current ACTIVE set, and Propose ONLY the transitions. Returns
// nil (not an error) on a non-leader / fence-stale node so Run keeps ticking quietly.
func (r *AlertReconciler) ReconcileAlertsOnce(ctx context.Context) error {
	now := r.cfg.Now()
	if !r.cfg.Node.IsLeader() || r.cfg.Node.LeaderContactStale(now) {
		r.webhookSeeded = false // lost leadership → re-baseline on the next leader pass (no re-fire)
		return nil
	}
	current, err := cluster.ActiveAlertKeys(r.cfg.DB)
	if err != nil {
		return err
	}
	// B6 OPS#2: fire the webhook off the COMMITTED delta (current vs the previous leader pass),
	// BEFORE computing raise/clear decisions — committed truth, idle-zero-POST, no double-fire on
	// a leadership change. A propose-but-uncommitted raise never reaches here (current is the
	// committed ACTIVE set), closing the swallowed-not-leader hole.
	r.fireWebhookDelta(current, now)
	var raises []alertSpec
	var clears []string

	// replication_degraded — clear ONLY on a POSITIVE observation (Observed && AllAtTarget);
	// a transient unobserved pass (JS meta-not-ready) must NOT flip it (review: clearing on
	// !Degraded() would false-clear when Observed==false).
	if r.cfg.Observe != nil {
		rep, oerr := r.cfg.Observe(ctx)
		if oerr != nil {
			r.cfg.Logger.Warn("d8b: replica observation failed (state unchanged)", "err", oerr)
		} else if rep.Observed {
			key := cluster.DedupKeyGlobal(cluster.AlertKindReplicationDegraded)
			switch {
			case rep.Degraded() && !current[key]:
				raises = append(raises, alertSpec{
					key: key, kind: cluster.AlertKindReplicationDegraded, severity: cluster.AlertSeveritySevere,
					message: "audit/object stream replicas are below target — HA not yet met; run `tether cluster status`",
				})
			case !rep.Degraded() && current[key]:
				// Clear on ANY positive observation that is not degraded — AllAtTarget OR an
				// empty stream set (m5: Observed&&len==0 yields !Degraded&&!AllAtTarget, which a
				// pure AllAtTarget() clear would wedge ACTIVE forever). A transient !Observed pass
				// never reaches here (the outer `else if rep.Observed` gate), so this does NOT
				// reintroduce the false-clear the clear-condition fix guards against.
				clears = append(clears, key)
			}
		}
	}

	// below_quorum — F==0 at exactly 2 voters (one failure → read-only). N=1 is force-single
	// territory (client-synthesized force_single_active, not this), N>=3 has F>=1.
	if nv, nerr := r.cfg.Node.NumVoters(); nerr == nil {
		key := cluster.DedupKeyGlobal(cluster.AlertKindBelowQuorum)
		below := nv == 2
		if below && !current[key] {
			raises = append(raises, alertSpec{
				key: key, kind: cluster.AlertKindBelowQuorum, severity: cluster.AlertSeverityInfo,
				message: "cluster tolerates 0 broker failures — one more makes it read-only; run `tether cluster add`",
			})
		} else if !below && current[key] {
			clears = append(clears, key)
		}
	}

	// broker_draining — one alert per node with an active draining marker.
	if nodes, derr := cluster.DrainingNodes(r.cfg.DB); derr == nil {
		want := make(map[string]bool, len(nodes))
		for _, node := range nodes {
			key := cluster.DedupKeyNode(cluster.AlertKindBrokerDraining, node)
			want[key] = true
			if !current[key] {
				raises = append(raises, alertSpec{
					key: key, kind: cluster.AlertKindBrokerDraining, severity: cluster.AlertSeverityInfo,
					message: "broker " + node + " is draining — its exposes are migrating; run `tether cluster status`",
				})
			}
		}
		for key := range current {
			if strings.HasPrefix(key, cluster.AlertKindBrokerDraining+":") && !want[key] {
				clears = append(clears, key)
			}
		}
	}

	for _, s := range raises {
		id := newAlertID(s.key, now)
		spec := s
		if perr := r.cfg.Propose(func(db *sql.DB) (*cluster.Command, error) {
			return cluster.PlanAlertRaise(id, spec.kind, spec.severity, spec.key, spec.message, now)
		}); perr != nil && !cluster.IsNotLeader(perr) {
			r.cfg.Logger.Warn("d8b: alert raise propose", "err", perr, "key", spec.key)
		}
	}
	for _, key := range clears {
		k := key
		if perr := r.cfg.Propose(func(db *sql.DB) (*cluster.Command, error) {
			return cluster.PlanAlertClear(k, now)
		}); perr != nil && !cluster.IsNotLeader(perr) {
			r.cfg.Logger.Warn("d8b: alert clear propose", "err", perr, "key", k)
		}
	}
	return nil
}
