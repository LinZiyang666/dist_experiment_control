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

	// B4 operator alert verbs: raise / clear a Raft-replicated alert from the broker
	// host (operator trust tier, same as the cluster_* verbs above). Leader-local: a
	// follower replies NotLeader+LeaderHost. nil Backend.Cluster (single mode) =>
	// cluster_not_enabled (alerts are Raft-replicated; a single broker has no raft).
	OpClusterAlertRaise = "cluster_alert_raise"
	OpClusterAlertClear = "cluster_alert_clear"

	// B6 OPS#3 online backup: write a consistent { state.db, manifest.json } bundle off the
	// node's RO handle (any node, leader or follower — no raft write). nil Backend.Cluster
	// (single mode) => cluster_not_enabled (use `cluster backup --offline` for a single broker).
	OpClusterBackup = "cluster_backup"

	// B6 OPS#12 incident export: a leader-local READ-ONLY assembler over the three replicated
	// sources (alerts + roster + per-sid audit). Secret-scrubbed (allowlist projection). nil
	// Backend.Cluster (single mode) => cluster_not_enabled.
	OpExportIncident = "export_incident"

	// B7 DOC#2 ops controller: a READ-ONLY view of membership operations, DERIVED from the
	// replicated cluster_nodes columns (phase / phase_changed_at / added_at / voter_add_error) —
	// no separate state, so it can never diverge from the phase SSOT. Leader-agnostic (any node
	// reads its replica). nil Backend.Cluster (single mode) => cluster_not_enabled.
	OpClusterOps = "cluster_ops"
)

// Cluster-admin machine error codes (B2 item 4) — stable identifiers set on Response.Code so a
// converge/monitoring script can branch on the failure KIND instead of substring-matching prose.
// "" means success or a legacy/un-coded reply (the CLI then classifies it as exitInternal=70).
const (
	CodeNotLeader             = "not_leader"
	CodeAlreadyVoter          = "already_voter"
	CodeNotAVoter             = "not_a_voter"
	CodeCatchUpStalled        = "catch_up_stalled"
	CodeQuorumConfirmRequired = "quorum_confirm_required"
	CodeNonceUsed             = "nonce_used"
	CodeClusterNotEnabled     = "cluster_not_enabled"
	CodeNodeUnknown           = "node_unknown"
	CodeStoreError            = "store_error"
	CodeBadRequest            = "bad_request"
	CodeRemoveOwnsResources   = "remove_owns_resources" // B3 item 7: a VOTER_ADD_FAILED node still homes exposes
	CodeVersionSkew           = "version_skew"          // B6 A3: joiner proto != cluster proto (hard reject; CLI exit 64)
)

// clusterOps is the set the server routes to Backend.Cluster.
var clusterOps = map[string]bool{
	OpClusterAdd: true, OpClusterRemove: true, OpClusterDrain: true,
	OpClusterTransfer: true, OpClusterStatus: true, OpClusterRotateCrt: true,
	OpClusterAlertRaise: true, OpClusterAlertClear: true,
	OpClusterBackup: true, OpExportIncident: true, OpClusterOps: true,
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
	CertFP    string `json:"cert_fp,omitempty"`    // rotate-tunnel-cert / cluster add (the joiner's tunnel cert fp)
	// D9 round-1 BLOCKER: the joiner's expose-home identity (else an added voter can never
	// serve as an expose home — D6 rehome is structurally impossible). nats_server_id is
	// derived from node_id (the §6.5 SSOT), not carried here.
	TunnelAddr string `json:"tunnel_addr,omitempty"` // cluster add: the joiner's public tunnel addr
	PublicHost string `json:"public_host,omitempty"` // cluster add: the joiner's public host
	NatsRoute  string `json:"nats_route,omitempty"`  // cluster add: the joiner's NATS route URL
	// B6 A3 version-skew gate (additive, omitempty): the joiner declares what it will run, read
	// from `cluster sign-join` (defaulted from the JOINER binary's proto.{ProtoVersion,ReleaseVersion}).
	// Proto mismatch is a HARD reject; release skew is advisory (a rolling upgrade legitimately
	// mixes releases). 0 / "" = not declared (an older `cluster add`) → allow + warn.
	JoinerProto   int    `json:"joiner_proto,omitempty"`
	JoinerRelease string `json:"joiner_release,omitempty"`
	Retire        bool   `json:"retire,omitempty"`
	Now           bool   `json:"now,omitempty"`
	Abort         bool   `json:"abort,omitempty"`
	Confirmed     bool   `json:"confirmed,omitempty"` // the F==0 typed-confirm result
	// B3 item 7: cluster remove --force bypasses ONLY the new expose-ownership probe on a
	// VOTER_ADD_FAILED node (never the raft phase-gate). Additive, omitempty, LOCAL socket only.
	Force bool `json:"force,omitempty"`

	// B4 operator-alert args (all omitempty; byte-compatible with the pre-B4 ops). raise
	// uses Kind/Severity/Message (+optional Label → dedup key manual:<label>); clear uses
	// DedupKey. The broker re-validates Kind==manual and Severity ∈ {info,severe}.
	AlertKind     string `json:"alert_kind,omitempty"`
	AlertSeverity string `json:"alert_severity,omitempty"`
	AlertMessage  string `json:"alert_message,omitempty"`
	AlertLabel    string `json:"alert_label,omitempty"`
	AlertDedupKey string `json:"alert_dedup_key,omitempty"`

	// B6 OPS#3: the SERVER-LOCAL bundle directory the broker writes the online backup into.
	BackupPath string `json:"backup_path,omitempty"`
	// External-review F3: opt-in to backing up a NON-leader follower (a possibly-stale local view).
	// Default false ⇒ online backup must run on the leader (the freshest committed state).
	AllowFollower bool `json:"allow_follower,omitempty"`

	// B6 OPS#12 export-incident: optional time window (a Go duration string, e.g. "24h") + sid
	// filter (default = ACTIVE sessions).
	Since string   `json:"since,omitempty"`
	SIDs  []string `json:"sids,omitempty"`

	// B7 DOC#2 `cluster ops show <node>`: when set, scope the ops view to one node (else list all).
	OpsNode string `json:"ops_node,omitempty"`
}

// Response is the on-wire reply. Exactly one of Sessions / Nodes /
// Audit / Evict is populated based on the request op. Error is
// non-empty for failures; OK=true means the requested action
// succeeded (used by Evict where there's no payload to inspect).
type Response struct {
	Op string `json:"op"`

	OK    bool   `json:"ok,omitempty"`
	Error string `json:"error,omitempty"`
	// B2 item 4: stable machine error code (one of the Code* consts), "" on success / legacy.
	// Additive + omitempty ⇒ an old CLI ignores it and an old broker never sets it.
	Code string `json:"code,omitempty"`

	Sessions []SessionEntry `json:"sessions,omitempty"`
	Nodes    []NodeEntry    `json:"nodes,omitempty"`
	Audit    []AuditEntry   `json:"audit,omitempty"`

	Evict *EvictResult `json:"evict,omitempty"`

	// Alert (B4) reports what an operator alert raise/clear did: the resolved dedup_key
	// (so the CLI can echo the exact `alert clear <key>` command) and whether the raise
	// was a no-op because a manual alert was already ACTIVE for that key.
	Alert *AlertResult `json:"alert,omitempty"`

	// D7 cluster reply fields.
	Cluster    *ClusterStatusReport `json:"cluster,omitempty"`
	QuorumProj *QuorumProjection    `json:"quorum_proj,omitempty"` // set when an F==0 confirm is required
	NotLeader  bool                 `json:"not_leader,omitempty"`  // admin change attempted on a follower
	LeaderHost string               `json:"leader_host,omitempty"` // where to re-run (empty mid-election)
	Nonce      string               `json:"nonce,omitempty"`       // cluster add step-1 challenge: sign this on the joiner

	// B6 OPS#3 online backup result.
	Backup *BackupResult `json:"backup,omitempty"`

	// B6 OPS#12 export-incident result.
	Incident *IncidentBundle `json:"incident,omitempty"`

	// B7 DOC#2 ops controller result.
	Ops []ClusterOpEntry `json:"ops,omitempty"`
}

// ClusterOpEntry is one membership operation, DERIVED from a cluster_nodes row (B7 DOC#2). Kind is
// inferred from phase; State is the live phase; LastError carries voter_add_error / a stall hint.
type ClusterOpEntry struct {
	NodeID    string `json:"node_id"`
	Kind      string `json:"kind"`  // add | drain | retire
	State     string `json:"state"` // in_progress | done | failed | stalled | draining | retiring
	Phase     string `json:"phase"`
	StartedAt string `json:"started_at,omitempty"`
	UpdatedAt string `json:"updated_at,omitempty"`
	LastError string `json:"last_error,omitempty"`
	Resume    string `json:"resume,omitempty"` // operator hint for an interrupted/failed op
}

// BackupResult reports a completed online backup (the server-local bundle dir + its size + the
// committed cursor + the node identity it captured).
type BackupResult struct {
	Path         string `json:"path"`
	Bytes        int64  `json:"bytes"`
	AppliedIndex uint64 `json:"applied_index"`
	SelfID       string `json:"self_id"`
	// External-review F3 (+ re-review): backup-source freshness provenance for the CLI/DR tooling.
	// SourceRole is "leader" | "follower" (a string so the follower warning serializes).
	SourceRole string `json:"source_role"`
	LeaderID   string `json:"leader_id,omitempty"`
}

// IncidentBundle is the B6 OPS#12 read-only forensic export: the alert timeline, the membership
// timeline (allowlist-projected — NEVER the join PoP material), and per-session audit history.
// Every field is secret-scrubbed at assembly (the broker never SELECT *'s cluster_nodes and
// denylist-scrubs the open audit Body).
type IncidentBundle struct {
	Schema        string                  `json:"schema"`         // Audit UX: machine-dispatch discriminator
	SchemaVersion int                     `json:"schema_version"` // B2 (schema, schema_version) contract
	GeneratedAt   string                  `json:"generated_at"`
	LeaderID      string                  `json:"leader_id"`
	Since         string                  `json:"since,omitempty"`
	Alerts        []IncidentAlert         `json:"alerts"`
	Roster        []IncidentNode          `json:"roster"`
	Audit         map[string][]AuditEntry `json:"audit"` // keyed by sid
	// Audit EH-MAJOR-1: a forensic bundle must NEVER read as complete-but-empty when a read failed.
	// Partial=true + Errors lists any source that could not be fully assembled (mirrors B2 item 5).
	Errors  []string `json:"errors"`
	Partial bool     `json:"partial"`
}

// IncidentAlert is one alert history row (+ ack, if any).
type IncidentAlert struct {
	ID        string `json:"id"`
	Kind      string `json:"kind"`
	Severity  string `json:"severity"`
	DedupKey  string `json:"dedup_key"`
	State     string `json:"state"`
	Message   string `json:"message"`
	RaisedAt  string `json:"raised_at"`
	ClearedAt string `json:"cleared_at,omitempty"`
	AckedBy   string `json:"acked_by,omitempty"`
	AckedAt   string `json:"acked_at,omitempty"`
}

// IncidentNode is the allowlist-projected membership-timeline row. join_nonce/join_sig (the 0013
// PoP material) and cert columns are NEVER included.
type IncidentNode struct {
	NodeID         string `json:"node_id"`
	Name           string `json:"name"`
	Phase          string `json:"phase"`
	RaftAddr       string `json:"raft_addr"`
	AddedAt        string `json:"added_at,omitempty"`
	PhaseChangedAt string `json:"phase_changed_at,omitempty"`
	VoterAddError  string `json:"voter_add_error,omitempty"`
}

// ClusterStatusReport is the broker's self-report for `cluster status`/`doctor`
// (one report per NATS-reachable broker; the ctl view aggregates them). reach_source
// distinguishes a self-report from a direct :7400 raft-ping (offline mode B).
type ClusterStatusReport struct {
	SchemaVersion int    `json:"schema_version"` // §17 / review F4: monitors negotiate on this
	View          string `json:"view"`           // "ctl-nats" | "offline"
	Health        string `json:"health"`         // HEALTHY_HA | DEGRADED | QUORUM_LOST | FORCE_SINGLE | "" (offline snapshot)
	ExitCode      int    `json:"exit_code"`      // 0 | 1 | 2 | 3
	LeaderID      string `json:"leader_id"`      // empty mid-election
	Banner        string `json:"banner"`
	NextStep      string `json:"next_step"`
	// B1: plain-language, user-facing additions. All ADDITIVE — schema_version stays 1
	// (additive omitempty / always-present fields don't break a monitor that ignores
	// unknown keys; schema_version discriminates a BREAKING shape change only).
	Verdict      string `json:"verdict,omitempty"`   // voter-count plain-language verdict (authoritative socket view)
	ViewHost     string `json:"view_host,omitempty"` // SelfID of the broker that produced this self-report
	IsLeaderView bool   `json:"is_leader_view"`      // NO omitempty: false (a non-leader view) MUST serialize
	// B2 item 5: broker-side problems folded into the report instead of being dropped when a
	// report is ALSO present. Branch-load-bearing ⇒ NOT omitempty (the CLI normalizes Errors to
	// [] so `errors`/`partial` are always present for a monitor).
	Errors  []string            `json:"errors"`
	Partial bool                `json:"partial"`
	Nodes   []ClusterNodeStatus `json:"nodes"`
}

// ClusterNodeStatus is one roster row joined against the live raft configuration.
type ClusterNodeStatus struct {
	NodeID     string `json:"node_id"`
	Name       string `json:"name"`
	Phase      string `json:"phase"` // roster phase
	Role       string `json:"role"`  // leader | voter | learner | "" (not in raft config)
	AppliedLag uint64 `json:"applied_lag"`
	// Audit OBS-MAJOR-1: last_contact_secs was a fabricated freshness signal (declared, NEVER
	// written, always 0 = "just contacted" for a dead peer) — removed. Real reachability is
	// Reachable / AppliedLag / ReachSource.
	AccountNkMatch bool   `json:"account_nk_match"`
	StreamActual   int    `json:"stream_actual"`
	StreamTarget   int    `json:"stream_target"`
	Reachable      bool   `json:"reachable"`
	ReachSource    string `json:"reach_source"` // "self" | "unverified" | "nats-health" | "raft-ping" | "disk-snapshot"
	Inconsistent   bool   `json:"inconsistent"` // phase says voter but raft config disagrees (or vice-versa)

	// B5 OPS#7 cert-rotation visibility (additive omitempty; schema_version stays 1). Public
	// fingerprints only — never private cert material. CertValidSecs is now→valid_until in
	// seconds (clock-honest, not an RFC3339 timestamp); 0/absent = no rotation window pinned.
	CertFP        string `json:"cert_fp,omitempty"`
	CertFPPrev    string `json:"cert_fp_prev,omitempty"`
	CertValidSecs int64  `json:"cert_valid_secs,omitempty"`

	// B5 OPS#9 capacity signals (additive omitempty). Populated ONLY on the self row (a leader
	// cannot statfs a peer's disk); absent on peers and on a node with no StoreDir configured.
	DiskFreePct int `json:"disk_free_pct,omitempty"`
	PortsUsed   int `json:"ports_used,omitempty"`
	PortsTotal  int `json:"ports_total,omitempty"`

	// B6 OPS#4 running version (additive omitempty; live self-report from each broker's
	// ClusterHealthResp — no persisted column to go stale on an upgrade). Empty = a node that
	// did not report (older broker / unreachable) → rendered as "?".
	ReleaseVersion string `json:"release_version,omitempty"`
	ProtoVer       int    `json:"proto_ver,omitempty"`
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

// AlertResult reports the outcome of an operator alert raise/clear (B4). DedupKey is the
// resolved key (manual or manual:<label>) for the CLI to echo the clear command; AlreadyActive
// is true when a raise was a no-op because a manual alert was already ACTIVE for that key.
type AlertResult struct {
	DedupKey      string `json:"dedup_key"`
	AlreadyActive bool   `json:"already_active,omitempty"`
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
