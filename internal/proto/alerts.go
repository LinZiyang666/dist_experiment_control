package proto

// D8b (§10) wire types for the member-reachable cluster-health + alert RPCs, and the pure
// client-synthesized destructive-gate decision.

// ClusterHealthSchemaVersion versions the ClusterHealthResp wire shape. v2 (C3) adds the topology
// reconcile self-report (TopoApplied/TopoObserved/TopoReconcileReason/TopoReported).
const ClusterHealthSchemaVersion = 2

// ClusterHealthResp is one broker's answer to a broadcast cluster-health probe (§10.4). The
// ctl corroborates ALL replies to decide a destructive gate WITHOUT a Raft write.
type ClusterHealthResp struct {
	// WritableLeaderConfirmed is true ONLY on a node that passed a VerifyLeaderRead barrier
	// (a genuinely-writable leader). A partitioned ex-leader within its lease window reports
	// State()==Leader but FAILS VerifyLeader, so it answers false — the gate fires precisely
	// in the data-loss window (the bare-State() bug the review flagged).
	WritableLeaderConfirmed bool   `json:"writable_leader_confirmed"`
	LeaderContactStale      bool   `json:"leader_contact_stale"` // no leader contact within T_fence
	LeaderID                string `json:"leader_id,omitempty"`  // best-effort, banner text only
	ForceSingleActive       bool   `json:"force_single_active"`  // this node's persisted D7 marker
	SchemaVersion           int    `json:"schema_version"`
	// NodeID + AppliedIndex (D9 §17, step 10b) let the leader's observability poll detect
	// raft_lag (a voter's command-domain AppliedIndex trailing the leader's CommitIndex) and
	// broker_down (a known voter that does not answer the broadcast within the window). raft
	// does not cleanly expose per-peer cursors/liveness, so each broker self-reports here.
	NodeID       string `json:"node_id,omitempty"`
	AppliedIndex uint64 `json:"applied_index"`
	// B6 OPS#4: each broker self-reports its running version so `cluster status` renders a VER
	// column (a live self-report — no persisted column that goes stale on upgrade). Additive
	// omitempty: an older broker omits them; the renderer shows "?".
	ReleaseVersion string `json:"release_version,omitempty"`
	ProtoVer       int    `json:"proto_ver,omitempty"`
	// TopoApplied/TopoObserved/TopoReconcileReason (C3 §2.7) are this broker's NATS topology reconcile
	// self-report: Applied = the generation rendered into the on-disk conf, Observed = the generation
	// the LIVE nats-server confirmed loading (via the /varz probe). TopoReported distinguishes a C3
	// broker that genuinely reports (always true) from an older/non-reporting one — so the HEALTHY-HA
	// gate degrades only a reporting voter that is behind, never false-greens via a >0 magnitude guard.
	TopoApplied         uint64 `json:"topo_applied,omitempty"`
	TopoObserved        uint64 `json:"topo_observed,omitempty"`
	TopoReconcileReason string `json:"topo_reconcile_reason,omitempty"`
	TopoReported        bool   `json:"topo_reported,omitempty"`
}

// AlertView is one ACTIVE alert as rendered for the banner / `alert ls` (§10.1/§10.3).
type AlertView struct {
	ID       string `json:"id"`
	Kind     string `json:"kind"`
	Severity string `json:"severity"`
	DedupKey string `json:"dedup_key"`
	Message  string `json:"message"`
	RaisedAt string `json:"raised_at"`
	AckedBy  string `json:"acked_by,omitempty"`
	AckedAt  string `json:"acked_at,omitempty"`
}

// AlertLsResp is the queue-group answer to an alert-ls request (one broker serves the
// replicated, bounded-stale read).
type AlertLsResp struct {
	Alerts []AlertView `json:"alerts"`
}

// AlertAckReq asks the cluster to ack an alert. acked_by is NOT carried here — the broker
// derives it from the NATS-authenticated actor segment of the ctrl.by.<actor>.alert.ack.req
// subject (display-only, §10.1).
type AlertAckReq struct {
	DedupKey string `json:"dedup_key"`
}

// SeveritySevere / SeverityInfo mirror the alert store severities (kept here so ctl-side
// rendering need not import internal/cluster).
const (
	SeveritySevere = "severe"
	SeverityInfo   = "info"
)

// DestructiveGate is the client-synthesized severe gate decision (§10.4): the two severe
// kinds that HARD-GATE a destructive op. replication_degraded is severe but does NOT gate
// (store-backed, §10.4) — it is never represented here.
type DestructiveGate struct {
	QuorumLost        bool
	ForceSingleActive bool
}

// Blocked reports whether either severe gate fired.
func (g DestructiveGate) Blocked() bool { return g.QuorumLost || g.ForceSingleActive }

// EvalDestructiveGate corroborates cluster-health replies into the gate decision (§10.4).
// quorum_lost fires iff >=1 reply arrived AND no reply confirms a writable leader AND EVERY
// reply reports stale leader contact (>T_fence) — so a confirmed leader anywhere, or a node
// that recently heard a leader (a brief election), does NOT gate. ZERO replies => no gate
// (ctl network jitter / non-cluster: server corroboration is required, §10.4 "纯抖动不阻断";
// in production N=1 there is no responder, so a destructive op is never gated — byte-identical
// to today). force_single_active fires iff any reply reports the persisted D7 marker. The gate
// is an ADVISORY pre-check — the authoritative protection is the broker rejecting a write it
// cannot quorum-serve.
func EvalDestructiveGate(replies []ClusterHealthResp) DestructiveGate {
	if len(replies) == 0 {
		return DestructiveGate{}
	}
	anyWritableLeader, allStale, anyForceSingle := false, true, false
	for _, r := range replies {
		if r.WritableLeaderConfirmed {
			anyWritableLeader = true
		}
		if !r.LeaderContactStale {
			allStale = false
		}
		if r.ForceSingleActive {
			anyForceSingle = true
		}
	}
	return DestructiveGate{
		QuorumLost:        !anyWritableLeader && allStale,
		ForceSingleActive: anyForceSingle,
	}
}
