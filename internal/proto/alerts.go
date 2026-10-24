package proto

// D8b (§10) wire types for the member-reachable cluster-health + alert RPCs, and the pure
// client-synthesized destructive-gate decision.

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
const SeveritySevere = "severe"
const SeverityInfo = "info"

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

	anyWritableLeader, allStale, anyForceSingle := false, true, false
	for _, r := range replies {
		anyWritableLeader = anyWritableLeader || r.WritableLeaderConfirmed
		allStale = allStale && r.LeaderContactStale
		anyForceSingle = anyForceSingle || r.ForceSingleActive
	}
	return DestructiveGate{
		QuorumLost:        len(replies) > 0 && !anyWritableLeader && allStale,
		ForceSingleActive: anyForceSingle,
	}
}

// UpgradeSummary is the public allowlist. Full execution history and resource
// identities remain private to the actor or local administrator.
// origin: hot-update S8 plan §2.2 / §7.1 / P13
type UpgradeSummary struct {
	TxID       string `json:"tx_id,omitempty"`
	CutID      string `json:"cut_id,omitempty"`
	Phase      string `json:"phase,omitempty"`
	Outcome    string `json:"outcome,omitempty"`
	RunningSHA string `json:"running_sha,omitempty"`
	Reason     string `json:"reason,omitempty"`
	Generation uint64 `json:"generation,omitempty"`
	Revision   uint64 `json:"revision,omitempty"`
}
