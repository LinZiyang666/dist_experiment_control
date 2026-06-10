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
)

// Request is the on-wire admin call. Op selects the verb; the
// remaining fields are op-specific (and unused fields stay empty).
type Request struct {
	Op string `json:"op"`

	// Audit args
	SID string `json:"sid,omitempty"` // also used by Evict
	N   int    `json:"n,omitempty"`   // audit tail count

	// Evict args
	NID string `json:"nid,omitempty"`
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
