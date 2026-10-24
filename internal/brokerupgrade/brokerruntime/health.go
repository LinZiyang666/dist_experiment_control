// Package brokerruntime holds the broker-side runtime adapters that the S8
// broker upgrade drives and observes: S6 image-run cut helpers, maintenance
// protocol helpers, roll-lock decisions and public health facts. It sits below
// brokerupgrade because these import brokerimage/brokercontrol, which already
// import brokerupgrade; brokerupgrade itself must never import this package.
// The Broker owner keeps only thin adapters (P15 per-file net-zero moves).
// origin: hot-update S8 plan §11.2 P15 / §12.1
package brokerruntime

import (
	"database/sql"
	"encoding/json"
	"time"

	"github.com/LinZiyang666/tether/internal/brokercontrol"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/infraattach"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
)

// ImageStatusMode preserves the status projection's conservative quorum label
// when the voter count cannot be read. It is observation, never cut authority.
// Moved from broker.setBrokerImagePhase to keep its retirement adapter net-zero.
// origin: hot-update S8 plan §12.1; S8 internal review P13-COHORT F2
func ImageStatusMode(clustered bool, voters func() (int, error)) string {
	if !clustered {
		return proto.BrokerImageModeSingle
	}
	if count, err := voters(); err == nil && count == 1 {
		return proto.BrokerImageModeSoleVoter
	}
	return proto.BrokerImageModeQuorum
}

// HealthFacts assembles the stateless half of the broadcast cluster-health reply
// (§10.4): the roll/grow lock facts, the three-axis version gate, topology and
// self-reported counts. Writable-leader proof, the account key and image/upgrade
// projections stay with the responder, which owns those live handles.
func HealthFacts(node *cluster.Node, db *sql.DB, contactStale bool, colocatedAgentNID string, topoSelf func() *brokercontrol.TopoSelfReport) proto.ClusterHealthResp {
	resp := proto.ClusterHealthResp{
		SchemaVersion:      proto.ClusterHealthSchemaVersion,
		LeaderContactStale: contactStale,
		ForceSingleActive:  brokercontrol.ForceSingleActive(db),
		UpgradeLockActive:  brokercontrol.UpgradeActive(db),          // External-review round2 B1: expose the roll lock for stale-lock self-heal
		GrowLockActive:     brokercontrol.GrowActiveJoiner(db) != "", // G4 §B: expose the grow lock for stale-lock self-heal (cluster add)
		// R7b: expose each lock's LEASE expiry so `cluster unlock` can refuse to rip a LIVE lock out
		// from under a renewing holder, and can name the never-self-healing "no lease at all" case.
		UpgradeLeaseExpiry: brokercontrol.LeaseExpiryString(db, cluster.MetaKeyUpgradeLease),
		GrowLeaseExpiry:    brokercontrol.LeaseExpiryString(db, cluster.MetaKeyGrowLease),
		NodeID:             node.SelfID(),
		ReleaseVersion:     proto.ReleaseVersion, // B6 OPS#4: live self-reported version
		ProtoVer:           proto.ProtoVersion,
		CommandVer:         cluster.CommandVersion(),      // G5 #13: three-axis rolling-upgrade gate (proto+command+ops)
		ColocatedAgentNID:  colocatedAgentNID,             // G5 #19: dual-version correlation ("" ⇒ CLI (assumed) fallback)
		PhaseFluidityOps:   cluster.HasPhaseFluidityOps(), // F5: advertise capability for the v0.4.2 readdr/route ops
		// increment 2 internal review: same for the admission-table write, which the
		// leader gates on before proposing a `session-allow` / `--remove`.
		SessionCreatorOps: cluster.HasSessionCreatorOps(),
	}
	// C3 §2.7: a C3 broker ALWAYS reports topology (TopoReported=true), even at gen 0, so the
	// HEALTHY-HA gate can distinguish "reporting + behind" from "not reporting (old broker)".
	if topoSelf != nil {
		resp.TopoReported = true
		if ts := topoSelf(); ts != nil {
			resp.TopoApplied = ts.Applied
			resp.TopoObserved = ts.Observed
			resp.TopoReconcileReason = ts.Reason
			resp.TopoAction = ts.Action
		}
	}
	// D9 §17 (step 10b): self-report the command-domain AppliedIndex so the leader's
	// observability poll can compute this broker's raft_lag.
	if ai, err := node.AppliedIndex(); err == nil {
		resp.AppliedIndex = ai
	}
	// G7b #16: self-report this broker's ALLOCATED __proxy__ home count for the `cluster status
	// --homes --remote` aggregate (a scalar count, no per-SID identity → no cross-session leak).
	if self := node.SelfID(); self != "" {
		var homeCount int
		if db.QueryRow(`SELECT COUNT(*) FROM port_allocations WHERE name='__proxy__' AND state='ALLOCATED' AND home_broker=?`, self).Scan(&homeCount) == nil {
			resp.ProxyHomeCount = homeCount
			resp.ProxyHomeReported = true // External-review m1: distinguish real 0 from unreported
		}
		// G5 Stage-C M2: self-report VOTER status so `cluster upgrade` plans over the real voter roster.
		var phase string
		if db.QueryRow(`SELECT phase FROM cluster_nodes WHERE node_id=?`, self).Scan(&phase) == nil {
			resp.IsVoter = phase == "VOTER"
		}
	}
	return resp
}

// SubscribeClusterRosterPull (G3 #17) wires the member-reachable roster-pull responder: a ctl connected
// to ANY broker requests that broker's signed manifest on the live conn, so discovery converges from the
// actually-connected survivor instead of the pinned floor/bootstrap host. It serves the broker's
// PRE-SIGNED, ≥30s-rate-limited manifestBytes() cache — NEVER buildSignedRoster/buildSeedBundle per
// request (that would be an unauthenticated-adjacent ed25519 signing DoS amplifier). Broadcast (no queue
// group). In single mode / with no account seed manifestBytes() returns ok=false → the responder stays
// silent → a ctl probe gets ErrNoResponders and falls back (byte-identical to today). The reply is the
// discovery-only, account-signed ClusterManifest (roster + seeds; zero secrets).
func SubscribeClusterRosterPull(nc *nats.Conn, manifestFn func() ([]byte, bool), respond func(*nats.Msg, []byte)) (*nats.Subscription, error) {
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
		respond(msg, body)
	})
}

// clusterHealthResponder builds the shared health-reply handler used by both the member-
// facing cluster-health RPC and the broker-only §17 cursor probe. topoSelf returns this broker's
// latest C3 topology reconcile self-report (nil until the first pass / non-cluster). accountPub
// returns this broker's auth_callout account public key (batch B / B4 — see
// proto.ClusterHealthResp.AccountNkPub); nil means the caller could not supply one, which is
// reported as "did not answer" rather than as a match.
func ClusterHealthResponder(nc *nats.Conn, node *cluster.Node, db *sql.DB, now func() time.Time, topoSelf func() *brokercontrol.TopoSelfReport, jsUnavail func() bool, colocatedAgentNID string, accountPub func() string, project func(*proto.ClusterHealthResp), respond func(*nats.Msg, []byte)) func(*nats.Msg) {
	return func(msg *nats.Msg) {
		if msg.Reply == "" {
			return
		}
		// The S8 control probe must traverse exactly this broker. Ordinary
		// health scatters omit a target and retain their broadcast semantics.
		// origin: S8 internal review cluster-roll F9
		var probe struct {
			Target string `json:"target_node"`
		}
		if len(msg.Data) > 0 && json.Unmarshal(msg.Data, &probe) == nil && probe.Target != "" && probe.Target != node.SelfID() {
			return
		}
		// VerifyLeaderRead is the authoritative "writable right now" probe. Hashicorp Raft can remain
		// in State()==Leader while VerifyLeader already fails after quorum loss; treating every State
		// leader as contact-fresh leaves the off-box summary in "electing (transient)" forever. Fold the
		// failed barrier into the contact verdict for that ex-leader. Followers still use LastContact+TFence.
		t := now()
		writable := node.VerifyLeaderRead(func(*sql.DB) error { return nil }) == nil
		stateLeader := node.IsLeader()
		_, leaderID := node.LeaderWithID()
		contactStale := infraattach.HealthContactStale(node.LeaderContactStale(t), stateLeader, writable, leaderID, node.SelfID())
		// The lock/version/topology/self-count facts are assembled by the broker runtime leaf.
		// origin: hot-update S8 plan §12.1 (P15 cluster_health.go net-zero move)
		resp := HealthFacts(node, db, contactStale, colocatedAgentNID, topoSelf)
		// G7b #20③: self-report the sustained JetStream-503 signal (leader-observed; false on a follower
		// or a healthy leader). Additive omitempty — an old broker omits it and the ctl sees false.
		if jsUnavail != nil {
			resp.JetStreamUnavailable = jsUnavail()
		}
		// batch B / B4: self-report the auth_callout account key so `cluster status` can render an
		// ACCT.NK column that is a real per-node answer instead of a hardcoded Y.
		//
		// Reported=true even when the key is EMPTY. That is the point of the flag: a clustered
		// broker with no account seed is a genuine finding (it cannot answer auth_callout at all),
		// and it must be distinguishable from a pre-v6 broker that simply omits the field. Only a
		// nil accountPub — a caller that could not supply the getter — stays unreported.
		if accountPub != nil {
			resp.AccountNkPub = accountPub()
			resp.AccountNkReported = true
		}
		project(&resp)
		if writable {
			resp.WritableLeaderConfirmed = true
		}
		if leaderID != "" {
			resp.LeaderID = leaderID // best-effort, banner text only
		}
		b, err := json.Marshal(resp)
		if err != nil {
			return
		}
		respond(msg, b)
	}
}

// SubscribeAlertLs wires the QUEUE-GROUP alert-ls responder (§10.1/§10.3): any ONE broker
// serves the bounded-stale replicated read (alerts are Raft-replicated, so any broker is
// authoritative-enough for the banner). One round-trip, best-effort.
func SubscribeAlertLs(nc *nats.Conn, db *sql.DB, respond func(*nats.Msg, []byte)) (*nats.Subscription, error) {
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
		respond(msg, b)
	})
}

// SubscribeAlertAck wires the QUEUE-GROUP ack responder (§10.1): ONE broker forwards the ack
// to the leader (VerbAlertAck). acked_by is the NATS-authenticated actor from the subject
// (display-only) — the ctl never asserts it. Replies "ok" or a short error.
func SubscribeAlertAck(nc *nats.Conn, forward func([]byte) error, payloadFor func(string, string) ([]byte, error), validate func(string) error, respond func(*nats.Msg, []byte)) (*nats.Subscription, error) {
	return nc.QueueSubscribe(proto.SubjCtrlAlertAckWildcard, "alert-ack", func(msg *nats.Msg) {
		if msg.Reply == "" {
			return
		}
		actor, _, ok := proto.ParseCtrlBy(msg.Subject)
		if !ok {
			respond(msg, []byte("error: bad subject"))
			return
		}
		var req proto.AlertAckReq
		if err := json.Unmarshal(msg.Data, &req); err != nil || req.DedupKey == "" {
			respond(msg, []byte("error: bad request"))
			return
		}
		// VALIDATE BEFORE FORWARDING. origin: prerelease audit broker-cluster-ops/L3-F2.
		// Non-empty was the only check, and what sits downstream is a raft Propose:
		// whatever arrives here is replicated to every broker and replayed on every
		// restore. The manual-alert admin path has validated its own text since it was
		// written; this path, which is reachable over NATS rather than over the admin
		// socket, had not.
		if verr := validate(req.DedupKey); verr != nil {
			respond(msg, []byte("error: "+verr.Error()))
			return
		}
		payload, err := payloadFor(req.DedupKey, actor)
		if err != nil {
			respond(msg, []byte("error: marshal"))
			return
		}
		if ferr := forward(payload); ferr != nil {
			respond(msg, []byte("error: "+ferr.Error()))
			return
		}
		respond(msg, []byte("ok"))
	})
}
