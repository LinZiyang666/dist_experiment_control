package proto

// D8b (§10) wire types for the member-reachable cluster-health + alert RPCs, and the pure
// client-synthesized destructive-gate decision.

// ClusterHealthSchemaVersion versions the ClusterHealthResp wire shape. v2 (C3) adds the topology
// reconcile self-report (TopoApplied/TopoObserved/TopoReconcileReason/TopoReported); v3 (G5/G7b) adds the
// version-skew + JS-503 + voter self-report batch (ProxyHomeCount/ProxyHomeReported/JetStreamUnavailable/
// CommandVer/ColocatedAgentNID/IsVoter/UpgradeLockActive); v4 (G4) adds GrowLockActive; v5 (R7b) adds the
// two lock-lease expiries (UpgradeLeaseExpiry/GrowLeaseExpiry); v6 (batch B / B4) adds the account-key
// self-report (AccountNkPub/AccountNkReported) that makes the `cluster status` ACCT.NK column honest;
// v7 (batch C / C3) adds TopoAction, the closed-enum topology reconcile action that replaces
// substring-matching TopoReconcileReason on both status renderers.
// No consumer gates on this value — decoding is omitempty-additive — so it is a documentation ledger,
// not a compat switch.
const ClusterHealthSchemaVersion = 7

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
	// ProxyHomeCount (G7b #16, schema v3) is this broker's ALLOCATED __proxy__ home count — an
	// AGGREGATE the ctl folds per-broker for `cluster status --homes --remote`. It carries NO per-SID
	// identity, so it cannot leak another session's topology (ACL-clean; rides the member-granted
	// cluster-health.req, no new subject). Additive omitempty: an older broker omits it → 0.
	ProxyHomeCount int `json:"proxy_home_count,omitempty"`
	// ProxyHomeReported (External-review m1) distinguishes "this broker has 0 proxy homes" from "this
	// broker predates the field and did not report it" (like TopoReported). A G7+ broker always sets it
	// true; an older broker omits it → false → the ctl renders "?" instead of silently summing it as 0.
	ProxyHomeReported bool `json:"proxy_home_reported,omitempty"`
	// JetStreamUnavailable (G7b #20③, schema v3) is this leader's SUSTAINED JetStream-503 self-report:
	// the leader's replica observation has failed with the 10008 "system temporarily unavailable"
	// signature for >= the sustained window (the racknerd force-single-clustered-conf rot). The ctl folds
	// it into the `cluster status --remote` verdict/banner (Option C: a synthetic signal, no store-backed
	// alert kind => zero FSM-poison migration). Additive omitempty: an older broker omits it => false.
	JetStreamUnavailable bool `json:"jetstream_unavailable,omitempty"`
	// CommandVer (G5 #13, schema v3) is this broker's raft command-encoding version (cluster.CommandVersion,
	// DECOUPLED from ProtoVer). `cluster upgrade` refuses a rolling roll whose canary reports a different
	// CommandVer than the un-upgraded voters — a commandVersion bump POISONs the log per-replica, so it must
	// be a flag-day reinstall, never a roll. Additive omitempty: an older broker omits it → 0 (fail-closed).
	CommandVer int `json:"command_ver,omitempty"`
	// ColocatedAgentNID (G5 #19) is the co-located agent's nid this broker self-declares, so `node ls
	// --brokers` correlates the broker daemon's version with its agent's RELEASE (a pre-G5 broker omits it
	// → the CLI falls back to the node_id==nid convention, labelled "(assumed)"). Additive omitempty.
	ColocatedAgentNID string `json:"colocated_agent_nid,omitempty"`
	// IsVoter (G5 Stage-C M2) is true when this broker's cluster_nodes.phase == VOTER. `cluster upgrade`
	// uses it so the roll plans over the REAL voter roster (an answering learner/draining broker is not a
	// voter, must not be counted toward the N=2 fence, and is never a transfer-leader target). Additive
	// omitempty: a pre-G5 broker omits it → false → the orchestrator falls back to "answered ⇒ voter".
	IsVoter bool `json:"is_voter,omitempty"`
	// UpgradeLockActive (External-review round2 B1) is true when this broker's committed cluster_meta holds
	// the `cluster_upgrade_active` roll-lock marker. `cluster upgrade` reads it so a re-run whose plan is a
	// no-op (all hosts already at target) can still DETECT + clear a stale lock left by a prior roll whose
	// release-lock did not confirm — the self-heal path that would otherwise never run. Additive omitempty.
	UpgradeLockActive bool `json:"upgrade_lock_active,omitempty"`
	// GrowLockActive (G4 §B) is true when this broker's committed cluster_meta holds the
	// `cluster_grow_active` marker. `cluster add` reads it so a re-run whose grow already completed can still
	// DETECT + clear a stale lock left by a prior grow whose release-lock did not confirm (the self-heal path,
	// mirroring UpgradeLockActive). Additive omitempty → pre-G4 brokers decode false.
	GrowLockActive bool `json:"grow_lock_active,omitempty"`
	// UpgradeLeaseExpiry / GrowLeaseExpiry (R7b, schema v5) are the RFC3339Nano expiry instants of the two
	// membership locks' LEASES, as committed on this broker. They exist so `tether cluster unlock` can tell
	// an operator the difference between the two states an operator most needs distinguished:
	//
	//	expiry in the FUTURE  → somebody is still renewing; the lock is LIVE and ripping it out would drop
	//	                        the mutex under a running roll/grow (--force required).
	//	expiry in the PAST or
	//	absent while the lock
	//	IS held               → nobody is renewing. Absent means the lock predates leases entirely, which is
	//	                        precisely the case that will NEVER self-heal and must be cleared by hand.
	//
	// Additive omitempty: a pre-R7b broker omits them → "" → the CLI reports "no lease" rather than
	// inventing an expiry.
	UpgradeLeaseExpiry string `json:"upgrade_lease_expiry,omitempty"`
	GrowLeaseExpiry    string `json:"grow_lease_expiry,omitempty"`
	// TopoApplied/TopoObserved/TopoReconcileReason (C3 §2.7) are this broker's NATS topology reconcile
	// self-report: Applied = the generation rendered into the on-disk conf, Observed = the generation
	// the LIVE nats-server confirmed loading (via the /varz probe). TopoReported distinguishes a C3
	// broker that genuinely reports (always true) from an older/non-reporting one — so the HEALTHY-HA
	// gate degrades only a reporting voter that is behind, never false-greens via a >0 magnitude guard.
	//
	// TopoAction (batch C) is the natsconf.Action* the last reconcile pass returned. It exists because
	// TopoReconcileReason is FREE TEXT and both status renderers were substring-matching it with
	// DIFFERENT substring sets — a convention maintained across a package boundary, which duly
	// diverged. Action is a closed enum the producer already emits, so classifying on it removes the
	// divergence mechanism instead of this instance of it. Additive omitempty, exactly like
	// TopoReported above: a pre-batch-C broker omits it and both ends fall back to the (now single,
	// shared) reason matcher in natsconf.ClassifyTopo.
	TopoApplied         uint64 `json:"topo_applied,omitempty"`
	TopoObserved        uint64 `json:"topo_observed,omitempty"`
	TopoReconcileReason string `json:"topo_reconcile_reason,omitempty"`
	TopoAction          string `json:"topo_action,omitempty"`
	TopoReported        bool   `json:"topo_reported,omitempty"`
	// PhaseFluidityOps (review F5): this broker's binary advertises support for the v0.4.2
	// phase-fluidity membership ops (OpClusterNodeReaddr/Route). The leader gates SetRaftAddr/
	// SetNatsRoute on ALL voters advertising it — an older binary's knownOps lacks those ops and would
	// decodeCommand-poison the replicated entry (advance applied_index, skip the SQL), forking its
	// replica. Additive omitempty: an older broker omits it (false ⇒ treated as NOT capable, fail-closed).
	PhaseFluidityOps bool `json:"phase_fluidity_ops,omitempty"`
	// AccountNkPub / AccountNkReported (batch B, B4, schema v6) are this broker's auth_callout ACCOUNT
	// public key — the key it signs every auth_callout response JWT with — and a flag saying it
	// answered at all.
	//
	// WHY THIS IS ON THE WIRE AT ALL. `cluster status` has always rendered an ACCT.NK column, and
	// until this field existed the producer hardcoded `AccountNkMatch: true` — the column said Y for
	// every node, always, including nodes it had never heard from. The legend admitted it
	// ("currently always Y — per-node verification not yet wired"), which made it a documented lie
	// rather than a fixed one. This closes it: the column now reflects a real per-node answer.
	//
	// WHAT DIVERGENCE MEANS. Every broker signs auth_callout JWTs with this key, and every
	// nats-server validates the reply against the issuer pinned in ITS OWN rendered conf. The
	// auth_callout subscription is queue-grouped ("tether-authcallout"), so a CONNECT raised at
	// server A can be answered by broker B. If B holds a different account key than A pinned, that
	// CONNECT fails — and the next one, answered by A, succeeds. The operator sees agents failing to
	// join at random with no pattern and no error naming a key. This column is the only place that
	// state is visible.
	//
	// NOT A NEW DISCLOSURE. The account public key is already member-visible: it is a field of the
	// account-signed SeedBundle served by the member-reachable roster-pull responder
	// (SubscribeClusterRosterPull → clusterroster.BuildSeeds → SeedBundle.AccountPub), and it is
	// printed by `cluster seeds show`. It is the operator's ROOT PUBLIC authority, never a seed.
	//
	// AccountNkReported distinguishes "this broker has no account seed" (reported, empty) from "this
	// broker predates the field" (not reported) — the same three-state discipline as
	// TopoReported/ProxyHomeReported. The renderer prints "?" for the latter and never Y.
	AccountNkPub      string `json:"account_nk_pub,omitempty"`
	AccountNkReported bool   `json:"account_nk_reported,omitempty"`
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
