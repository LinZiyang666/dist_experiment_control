// Package adminsock implements the broker-local admin Unix socket
// (architecture I.2b / P9). The wire protocol is intentionally
// trivial: one line-delimited JSON Request per connection, then one
// line-delimited JSON Response, then connection closes. No streaming,
// no multiplexing — admin commands are infrequent and read-mostly,
// so per-call connect overhead is fine and the simplicity makes the
// permission model easy to reason about.
//
// All wire types live here so the server (broker side) and client
// (cmd/tether/admin side) decode the same shapes.
package adminsock

import "time"

// Op enumerates the admin verbs. Keep this list small: every
// addition needs a corresponding handler + a CLI subcommand. v1
// covers the four actions architecture P9 calls out.
const (
	OpSessions = "sessions"
	OpNodes    = "nodes"
	OpAudit    = "audit"
	OpEvict    = "evict"

	// D7 §8.1 cluster admin verbs. They are routed to Backend.Cluster (a
	// ClusterAdminBackend); when that is nil (production until the D9 cutover) the
	// server replies "cluster mode not enabled". A non-leader broker replies with
	// NotLeader+LeaderHost so the CLI tells the operator where to re-run (§8.1: no
	// network forwarding of admin changes).
	OpClusterAdd       = "cluster_add"
	OpClusterRemove    = "cluster_remove"
	OpClusterDrain     = "cluster_drain"
	OpClusterTransfer  = "cluster_transfer"
	OpClusterStatus    = "cluster_status"
	OpClusterRotateCrt = "cluster_rotate_cert"
)

// clusterOps is the set the server routes to Backend.Cluster.
var clusterOps = map[string]bool{
	OpClusterAdd: true, OpClusterRemove: true, OpClusterDrain: true,
	OpClusterTransfer: true, OpClusterStatus: true, OpClusterRotateCrt: true,
}

// Request is the on-wire admin call. Op selects the verb; the
// remaining fields are op-specific (and unused fields stay empty).
type Request struct {
	Op string `json:"op"`

	// Audit args
	SID string `json:"sid,omitempty"` // also used by Evict
	N   int    `json:"n,omitempty"`   // audit tail count

	// Evict args
	NID string `json:"nid,omitempty"`

	// D7 cluster args (all omitempty; byte-compatible with the original 4 ops).
	NodeID    string `json:"node_id,omitempty"`
	NodePub   string `json:"node_pub,omitempty"`   // node-identity pubkey the operator typed
	Host      string `json:"host,omitempty"`       // new node's host/raft addr
	JoinToken string `json:"join_token,omitempty"` // nonce|sig from `cluster sign-join`
	CertFP    string `json:"cert_fp,omitempty"`    // rotate-tunnel-cert
	Retire    bool   `json:"retire,omitempty"`
	Now       bool   `json:"now,omitempty"`
	Abort     bool   `json:"abort,omitempty"`
	Confirmed bool   `json:"confirmed,omitempty"` // the F==0 typed-confirm result
}

// Response is the on-wire reply. Exactly one of Sessions / Nodes /
// Audit / Evict is populated based on the request op. Error is
// non-empty for failures; OK=true means the requested action
// succeeded (used by Evict where there's no payload to inspect).
type Response struct {
	Op string `json:"op"`

	OK    bool   `json:"ok,omitempty"`
	Error string `json:"error,omitempty"`

	Sessions []SessionEntry `json:"sessions,omitempty"`
	Nodes    []NodeEntry    `json:"nodes,omitempty"`
	Audit    []AuditEntry   `json:"audit,omitempty"`

	Evict *EvictResult `json:"evict,omitempty"`

	// D7 cluster reply fields.
	Cluster    *ClusterStatusReport `json:"cluster,omitempty"`
	QuorumProj *QuorumProjection    `json:"quorum_proj,omitempty"` // set when an F==0 confirm is required
	NotLeader  bool                 `json:"not_leader,omitempty"`  // admin change attempted on a follower
	LeaderHost string               `json:"leader_host,omitempty"` // where to re-run (empty mid-election)
	Nonce      string               `json:"nonce,omitempty"`       // cluster add step-1 challenge: sign this on the joiner
}

// ClusterStatusReport is the broker's self-report for `cluster status`/`doctor`
// (one report per NATS-reachable broker; the ctl view aggregates them). reach_source
// distinguishes a self-report from a direct :7400 raft-ping (offline mode B).
type ClusterStatusReport struct {
	SchemaVersion int                 `json:"schema_version"` // §17 / review F4: monitors negotiate on this
	View          string              `json:"view"`           // "ctl-nats" | "offline"
	Health        string              `json:"health"`         // HEALTHY_HA | DEGRADED | QUORUM_LOST | FORCE_SINGLE | "" (offline snapshot)
	ExitCode      int                 `json:"exit_code"`      // 0 | 1 | 2 | 3
	LeaderID      string              `json:"leader_id"`      // empty mid-election
	Banner        string              `json:"banner"`
	NextStep      string              `json:"next_step"`
	Nodes         []ClusterNodeStatus `json:"nodes"`
}

// ClusterNodeStatus is one roster row joined against the live raft configuration.
type ClusterNodeStatus struct {
	NodeID          string `json:"node_id"`
	Name            string `json:"name"`
	Phase           string `json:"phase"` // roster phase
	Role            string `json:"role"`  // leader | voter | learner | "" (not in raft config)
	AppliedLag      uint64 `json:"applied_lag"`
	LastContactSecs int64  `json:"last_contact_secs"`
	AccountNkMatch  bool   `json:"account_nk_match"`
	StreamActual    int    `json:"stream_actual"`
	StreamTarget    int    `json:"stream_target"`
	Reachable       bool   `json:"reachable"`
	ReachSource     string `json:"reach_source"` // "self-report" | "raft-ping"
	Inconsistent    bool   `json:"inconsistent"` // phase says voter but raft config disagrees (or vice-versa)
}

// QuorumProjection mirrors broker.QuorumProjection on the wire (the F==0 confirm gate).
type QuorumProjection struct {
	Voters         int `json:"voters"`
	Quorum         int `json:"quorum"`
	FaultTolerance int `json:"fault_tolerance"`
}

// ClusterAdminBackend handles the D7 cluster verbs. The broker provides an adapter
// that wraps its ClusterAdmin; adminsock stays a leaf (it imports neither
// internal/cluster nor internal/broker — the adapter translates to these wire
// types). nil Backend.Cluster (production until D9) => "cluster mode not enabled".
type ClusterAdminBackend interface {
	HandleCluster(req Request) Response
}

// SessionEntry mirrors the SQLite sessions row (no pin_hash; that
// column never leaves storage even via admin).
type SessionEntry struct {
	SID       string    `json:"sid"`
	Name      string    `json:"name"`
	OwnerFP   string    `json:"owner_fp"`
	State     string    `json:"state"`
	CreatedAt time.Time `json:"created_at"`
}

// NodeEntry mirrors the SQLite nodes row. The client derives the
// heartbeat age for display from LastHeartbeatAt.
type NodeEntry struct {
	SID             string    `json:"sid"`
	NID             string    `json:"nid"`
	Status          string    `json:"status"`
	LastHeartbeatAt time.Time `json:"last_heartbeat_at"`
	BootID          string    `json:"boot_id,omitempty"`
	ReleaseVersion  string    `json:"release_version,omitempty"`
	ProtoVersion    int       `json:"proto_version,omitempty"`
}

// AuditEntry is one already-decoded audit message from
// `history-<sid>`. Subject is preserved so the operator sees which
// of {audit.call, audit.proc, audit.port} this row is, and Body is
// the raw JSON so the operator can inspect verb-specific fields
// without the admin protocol needing to mirror every audit shape.
type AuditEntry struct {
	Subject string         `json:"subject"`
	Seq     uint64         `json:"seq"`
	Ts      time.Time      `json:"ts"`
	Body    map[string]any `json:"body"`
}

// EvictResult reports what the broker actually changed when an
// `evict` succeeded: which rows it deleted, and whether a
// sys.events agent_evicted broadcast was emitted (false when the
// target had no provisioning row, i.e. the evict was a no-op).
type EvictResult struct {
	SID                string `json:"sid"`
	NID                string `json:"nid"`
	NodeRowDeleted     bool   `json:"node_row_deleted"`
	AgentProvDeleted   bool   `json:"agent_prov_deleted"`
	BroadcastedEvicted bool   `json:"broadcasted_evicted"`
}
