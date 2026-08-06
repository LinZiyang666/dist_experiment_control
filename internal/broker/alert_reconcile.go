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

	"github.com/LinZiyang666/tether/internal/backoff"
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
	// SelfID is this broker's node id. Used ONLY to tell a genuine leadership HANDOFF (LeaderID
	// is some OTHER node) apart from a same-node lease blip (LeaderID is us or empty) when deciding
	// whether to discard the webhook baseline — see ReconcileAlertsOnce (#93/H13). Empty ⇒ the
	// conservative old behavior (always re-baseline on any leadership loss).
	SelfID string
	// SetJSUnavailable (G7b #20③), when non-nil, is called with the leader's SUSTAINED JetStream-503
	// verdict: true once Observe has failed with the 10008 signature for >= jsDownThreshold, false on
	// the first positive observation OR when this node is no longer leader. nil disables JS-503 signalling.
	SetJSUnavailable func(bool)
	// Beat (B7), when non-nil, is called once per completed loop iteration — see
	// AuditPublisherConfig.Beat for why the set cannot infer this itself.
	Beat func()
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
	// committed ACTIVE set observed on the PREVIOUS leader pass. webhookSeeded is reset on a
	// GENUINE handoff to another node (so the returning leader re-baselines WITHOUT re-POSTing
	// every already-active alert) but PRESERVED across a same-node lease blip (#93/H13 — else a
	// transition committed during the blip would be absorbed into the re-seed and dropped forever).
	prevActive    map[string]bool
	webhookSeeded bool
	// jsDownSince (G7b #20③) is the leader-local wall-clock of the FIRST JS-503 in the current run of
	// failures (zero = not currently down). Reset on a positive observation or on losing leadership.
	jsDownSince time.Time
	// h1 E3 log-discipline dampers (single-goroutine: only Run's tick touches
	// them). LOG-only — the 500ms cadence is untouched, because
	// jsDownThreshold's sustained-window detection needs a per-tick
	// evaluation. Without these, the same JS outage that drove the d5 loop's
	// 5.3GB WARN spam also drove these two sites at 2 lines/s.
	passBO    *backoff.Tracker
	observeBO *backoff.Tracker
}

// jsDownThreshold is how long the leader's replica observation must fail with the JS-503 (10008)
// signature before the sustained-outage signal is raised — long enough that an election / meta-reform
// blip does not false-alarm, short enough that a multi-day silent rot is impossible.
const jsDownThreshold = 60 * time.Second

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
	return &AlertReconciler{
		cfg:       cfg,
		passBO:    backoff.New(backoff.Policy{Base: time.Second, Cap: 5 * time.Minute}),
		observeBO: backoff.New(backoff.Policy{Base: time.Second, Cap: 5 * time.Minute}),
	}
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
				// h1 E3: log-discipline only (first failure + class change +
				// recovery-with-count); the pass itself keeps its cadence.
				if r.passBO.Fail(r.cfg.Now(), classifyJSErrClass(err)) {
					r.cfg.Logger.Warn("d8b: alert reconcile pass (repeats suppressed)", "err", err)
				}
			} else if suppressed, ok := r.passBO.Recover(r.cfg.Now()); ok {
				r.cfg.Logger.Info("d8b: alert reconcile recovered", "suppressed_failures", suppressed)
			}
			// Beat on EVERY iteration, including the failing ones: the question this answers is "is the
			// loop alive", and a loop that fails every pass is alive and needs its lastErr read, not to
			// be mistaken for a dead one.
			if r.cfg.Beat != nil {
				r.cfg.Beat()
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
		// #93/H13: only DISCARD the webhook baseline on a GENUINE handoff to ANOTHER node — that
		// node's reconciler now owns (and fires) the committed deltas, so re-firing them on our
		// return would duplicate. A SAME-NODE lease blip (a sub-second raft step-down/re-elect with
		// no other leader, common when a saturated host starves the raft lease past
		// LeaderLeaseTimeout) must NOT discard: a transition committed during the blip was fired by
		// nobody, and re-seeding would absorb it into the baseline and DROP it forever. Preserving the
		// baseline across a blip re-fires the missed delta on our return; the only cost is a possible
		// DUPLICATE webhook on a genuine handoff-and-back, which the dedup_key makes idempotent —
		// strictly better than a silent drop. Empty SelfID / nil LeaderID ⇒ conservative old reset.
		if r.cfg.SelfID == "" || r.cfg.LeaderID == nil {
			r.webhookSeeded = false
		} else if lid := r.cfg.LeaderID(); lid != "" && lid != r.cfg.SelfID {
			r.webhookSeeded = false // a different node is leader now — it owns the deltas
		}
		// G7b #20③: a demoted leader drops its JS-503 state so it never answers a stale-true self-report.
		r.jsDownSince = time.Time{}
		if r.cfg.SetJSUnavailable != nil {
			r.cfg.SetJSUnavailable(false)
		}
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
			// h1 E3: same log discipline as the pass error above; the
			// jsDownSince clock below still advances on EVERY failing tick —
			// only the log line is damped.
			if r.observeBO.Fail(r.cfg.Now(), classifyJSErrClass(oerr)) {
				r.cfg.Logger.Warn("d8b: replica observation failed (state unchanged; repeats suppressed)", "err", oerr)
			}
			// G7b #20③: sustained JS-503 detection. Advance the down-since clock ONLY on the 10008
			// "system temporarily unavailable" signature (a stream-not-found / timeout / no-responder is
			// NOT a wedged JS and must not raise). Raise the synthetic signal once it has persisted for
			// >= jsDownThreshold, so an election / meta-reform blip never false-alarms.
			if r.cfg.SetJSUnavailable != nil {
				if classifyJSUnavailable(oerr) {
					if r.jsDownSince.IsZero() {
						r.jsDownSince = now
					}
					if now.Sub(r.jsDownSince) >= jsDownThreshold {
						r.cfg.SetJSUnavailable(true)
					}
				} else {
					// Stage-C M1: a NON-10008 error (stream-not-found / timeout / no-responder) is NOT a
					// wedged-at-503 JS — a code that would never RAISE the signal must never PERPETUATE it
					// (inv-6 no-stuck-ACTIVE; a force-single N=1 leader never demotes, so this is its only
					// clear path). Symmetric with the positive-observe clear below.
					r.jsDownSince = time.Time{}
					r.cfg.SetJSUnavailable(false)
				}
			}
		} else if rep.Observed {
			// h1 E3: end the observe-failure log run with its suppression count.
			if suppressed, ok := r.observeBO.Recover(r.cfg.Now()); ok {
				r.cfg.Logger.Info("d8b: replica observation recovered", "suppressed_failures", suppressed)
			}
			// G7b #20③: a POSITIVE observation means JS is answering again → clear the JS-503 signal.
			r.jsDownSince = time.Time{}
			if r.cfg.SetJSUnavailable != nil {
				r.cfg.SetJSUnavailable(false)
			}
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
				message: "cluster tolerates 0 broker failures — one more makes it read-only; add a voter with `tether cluster join prepare` then `join approve`",
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
