// cluster_health.go (D8b §10) is the broker-side responder for the member-reachable
// cluster-health + alert RPCs. Post-D9 cutover the subscriptions are wired in CLUSTER mode
// (wireClusterLate / SubscribeClusterHealth); in SINGLE mode they are not wired, so a ctl probe
// gets ErrNoResponders and the gate/banner stay silent (byte-identical). (The per-phase
// TestD8ProductionWiresNoCluster guard was a build-and-prove scaffold removed at the D9 cutover.)
package broker

import (
	"database/sql"
	"encoding/json"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
)

// SubscribeClusterHealth wires the BROADCAST cluster-health responder (§10.4): every broker
// answers (no queue group) so ctl can corroborate all reachable views into a destructive gate
// WITHOUT a Raft write. writable_leader_confirmed is set ONLY after a VerifyLeaderRead barrier
// — a partitioned ex-leader still reporting State()==Leader within its lease FAILS VerifyLeader
// and answers false, so the gate fires precisely in the data-loss window.
func SubscribeClusterHealth(nc *nats.Conn, node *cluster.Node, db *sql.DB, now func() time.Time, topoSelf func() *topoSelfReport) (*nats.Subscription, error) {
	return nc.Subscribe(proto.SubjCtrlClusterHealthWildcard, clusterHealthResponder(node, db, now, topoSelf))
}

// SubscribeClusterRosterPull (G3 #17) wires the member-reachable roster-pull responder: a ctl connected
// to ANY broker requests that broker's signed manifest on the live conn, so discovery converges from the
// actually-connected survivor instead of the pinned floor/bootstrap host. It serves the broker's
// PRE-SIGNED, ≥30s-rate-limited manifestBytes() cache — NEVER buildSignedRoster/buildSeedBundle per
// request (that would be an unauthenticated-adjacent ed25519 signing DoS amplifier). Broadcast (no queue
// group). In single mode / with no account seed manifestBytes() returns ok=false → the responder stays
// silent → a ctl probe gets ErrNoResponders and falls back (byte-identical to today). The reply is the
// discovery-only, account-signed ClusterManifest (roster + seeds; zero secrets).
func SubscribeClusterRosterPull(nc *nats.Conn, manifestFn func() ([]byte, bool)) (*nats.Subscription, error) {
	return nc.Subscribe(proto.SubjCtrlClusterRosterWildcard, func(msg *nats.Msg) {
		if msg.Reply == "" {
			return
		}
		body, ok := manifestFn()
		if !ok || body == nil {
			// Stage-C m12: SINGLE mode → this responder is never wired (cluster-only mount) → no interest →
			// the server fast-fails the ctl request with ErrNoResponders. CLUSTER mode with no account seed
			// yet (or the sub-second window before the first sign) → the responder IS subscribed, so this
			// silent return makes the ctl wait out its request timeout before falling back. Latency-only, a
			// narrow window; correctness holds (the ctl still falls back to HTTP/cache).
			return
		}
		_ = msg.Respond(body)
	})
}

// SubscribeClusterCursor (D9 §17 round-1 BLOCKER fix) wires the same broadcast health
// responder on the BROKER-ONLY tether.v2.cluster.cursor.req subject — the one the leader's
// observability poll scatters to (its broker nkey can pub+sub cluster.> but not ctrl.by.*).
// Every broker answers (no queue group) so the leader collects each voter's AppliedIndex.
func SubscribeClusterCursor(nc *nats.Conn, node *cluster.Node, db *sql.DB, now func() time.Time, topoSelf func() *topoSelfReport) (*nats.Subscription, error) {
	return nc.Subscribe(proto.SubjClusterCursor, clusterHealthResponder(node, db, now, topoSelf))
}

// clusterHealthResponder builds the shared health-reply handler used by both the member-
// facing cluster-health RPC and the broker-only §17 cursor probe. topoSelf returns this broker's
// latest C3 topology reconcile self-report (nil until the first pass / non-cluster).
func clusterHealthResponder(node *cluster.Node, db *sql.DB, now func() time.Time, topoSelf func() *topoSelfReport) func(*nats.Msg) {
	return func(msg *nats.Msg) {
		if msg.Reply == "" {
			return
		}
		resp := proto.ClusterHealthResp{
			SchemaVersion:      proto.ClusterHealthSchemaVersion,
			LeaderContactStale: node.LeaderContactStale(now()),
			ForceSingleActive:  forceSingleActive(db),
			NodeID:             node.SelfID(),
			ReleaseVersion:     proto.ReleaseVersion, // B6 OPS#4: live self-reported version
			ProtoVer:           proto.ProtoVersion,
			PhaseFluidityOps:   cluster.HasPhaseFluidityOps(), // F5: advertise capability for the v0.4.2 readdr/route ops
		}
		// C3 §2.7: a C3 broker ALWAYS reports topology (TopoReported=true), even at gen 0, so the
		// HEALTHY-HA gate can distinguish "reporting + behind" from "not reporting (old broker)".
		if topoSelf != nil {
			resp.TopoReported = true
			if ts := topoSelf(); ts != nil {
				resp.TopoApplied = ts.Applied
				resp.TopoObserved = ts.Observed
				resp.TopoReconcileReason = ts.Reason
			}
		}
		// D9 §17 (step 10b): self-report the command-domain AppliedIndex so the leader's
		// observability poll can compute this broker's raft_lag.
		if ai, err := node.AppliedIndex(); err == nil {
			resp.AppliedIndex = ai
		}
		if verr := node.VerifyLeaderRead(func(*sql.DB) error { return nil }); verr == nil {
			resp.WritableLeaderConfirmed = true
		}
		if _, leaderID := node.LeaderWithID(); leaderID != "" {
			resp.LeaderID = leaderID // best-effort, banner text only
		}
		b, err := json.Marshal(resp)
		if err != nil {
			return
		}
		_ = msg.Respond(b)
	}
}

// SubscribeAlertLs wires the QUEUE-GROUP alert-ls responder (§10.1/§10.3): any ONE broker
// serves the bounded-stale replicated read (alerts are Raft-replicated, so any broker is
// authoritative-enough for the banner). One round-trip, best-effort.
func SubscribeAlertLs(nc *nats.Conn, db *sql.DB) (*nats.Subscription, error) {
	return nc.QueueSubscribe(proto.SubjCtrlAlertLsWildcard, "alert-ls", func(msg *nats.Msg) {
		if msg.Reply == "" {
			return
		}
		alerts, err := cluster.ActiveAlerts(db)
		if err != nil {
			return
		}
		views := make([]proto.AlertView, 0, len(alerts))
		for _, a := range alerts {
			views = append(views, proto.AlertView{
				ID: a.ID, Kind: a.Kind, Severity: a.Severity, DedupKey: a.DedupKey,
				Message: a.Message, RaisedAt: a.RaisedAt, AckedBy: a.AckedBy, AckedAt: a.AckedAt,
			})
		}
		b, err := json.Marshal(proto.AlertLsResp{Alerts: views})
		if err != nil {
			return
		}
		_ = msg.Respond(b)
	})
}

// SubscribeAlertAck wires the QUEUE-GROUP ack responder (§10.1): ONE broker forwards the ack
// to the leader (VerbAlertAck). acked_by is the NATS-authenticated actor from the subject
// (display-only) — the ctl never asserts it. Replies "ok" or a short error.
func SubscribeAlertAck(nc *nats.Conn, fwd *Forwarder) (*nats.Subscription, error) {
	return nc.QueueSubscribe(proto.SubjCtrlAlertAckWildcard, "alert-ack", func(msg *nats.Msg) {
		if msg.Reply == "" {
			return
		}
		actor, _, ok := proto.ParseCtrlBy(msg.Subject)
		if !ok {
			_ = msg.Respond([]byte("error: bad subject"))
			return
		}
		var req proto.AlertAckReq
		if err := json.Unmarshal(msg.Data, &req); err != nil || req.DedupKey == "" {
			_ = msg.Respond([]byte("error: bad request"))
			return
		}
		payload, err := json.Marshal(AlertAckPayload{DedupKey: req.DedupKey, AckedBy: actor})
		if err != nil {
			_ = msg.Respond([]byte("error: marshal"))
			return
		}
		if ferr := fwd.Forward(VerbAlertAck, "", payload); ferr != nil {
			_ = msg.Respond([]byte("error: " + ferr.Error()))
			return
		}
		_ = msg.Respond([]byte("ok"))
	})
}

// forceSingleActive reports whether the persisted D7 force_single_active marker is set in the
// replicated cluster_meta (read-only; any broker's local committed copy).
func forceSingleActive(db *sql.DB) bool {
	var one int
	err := db.QueryRow(`SELECT 1 FROM cluster_meta WHERE key=? LIMIT 1`, cluster.MetaKeyForceSingle).Scan(&one)
	return err == nil
}
