// Package schema defines the long-lived JSON envelopes that land in JetStream
// `history-<sid>` (architecture H.5). These types are an append-only contract:
// once written they must remain decodable, so additions must be backward-
// compatible (new optional fields only) and breaking changes require a new
// AuditSchemaVersion.
package schema

import "time"

// AuditSchemaVersion is stamped into every audit envelope's `v` field.
// Decoders may use it to dispatch shape per version (v1 only for now).
const AuditSchemaVersion = 1

// AuditCall is one row per command call attempt (architecture H.5).
//
// kind ∈ {"call", "admin", "admin_denied", "exec", "reconciled"}.
// "exec" entries record only meta (no stdin/stdout bytes).
type AuditCall struct {
	V         int            `json:"v"`
	Kind      string         `json:"kind"`
	Ts        time.Time      `json:"ts"`
	ActorNkey string         `json:"actor_nkey"`           // unforgeable, from subject (B.1)
	ActorFp   string         `json:"actor_fp"`             // SQLite mapping
	ActorName string         `json:"actor_name,omitempty"` // display only
	Session   string         `json:"session"`
	Node      string         `json:"node,omitempty"` // omitted for non-node verbs
	ReqID     string         `json:"req_id"`
	Verb      string         `json:"verb"`
	Target    map[string]any `json:"target,omitempty"` // verb-specific, free-form
	OK        bool           `json:"ok"`
	Error     string         `json:"error,omitempty"`
}

// AuditProc is a process lifecycle envelope.
//
// kind ∈ {"start", "exit", "reconciled_closed", "killed_orphan"}.
type AuditProc struct {
	V          int       `json:"v"`
	Kind       string    `json:"kind"`
	Ts         time.Time `json:"ts"`
	Session    string    `json:"session"`
	Node       string    `json:"node"`
	PID        string    `json:"pid"`
	RC         *int      `json:"rc,omitempty"`          // nil unless exit/reconciled_closed
	DurationMs *int64    `json:"duration_ms,omitempty"` // nil unless exit
	Cmd        string    `json:"cmd,omitempty"`         // populated only on kind=start
}

// AuditPort is a port allocation lifecycle envelope.
//
// kind ∈ {"allocated", "revoked", "freed", "reconciled"}.
type AuditPort struct {
	V         int       `json:"v"`
	Kind      string    `json:"kind"`
	Ts        time.Time `json:"ts"`
	Session   string    `json:"session"`
	Node      string    `json:"node"`
	Port      int       `json:"port"`
	Name      string    `json:"name,omitempty"`
	LocalPort int       `json:"local_port,omitempty"`
	ActorNkey string    `json:"actor_nkey,omitempty"` // populated when user-triggered
	ActorFp   string    `json:"actor_fp,omitempty"`
}

// AuditTransfer is a file-transfer lifecycle envelope.
// file-transfer-plan §Audit.
//
//   kind ∈ {"start","complete","failed"} — receiver-finalization invariant:
//     start    written when broker accepts the prepare;
//     complete written on agent ev.transfer (push) or ctl finalize.req (pull);
//     failed   same parties, on the failure path or broker-side timeout.
//   verb ∈ {"push","pull"}.
//   tier ∈ {"a","b"} — "a" inline ≤ 8 MiB, "b" JetStream ObjectStore ≤ 200 MiB.
//
// Bytes / DurationMs are populated only on complete/failed; Bucket only
// on tier B; Code only on failed (machine-readable). The schema name
// matches the file-transfer-plan §Audit table verbatim — append-only.
type AuditTransfer struct {
	V          int       `json:"v"`
	Kind       string    `json:"kind"`
	Verb       string    `json:"verb"`
	Ts         time.Time `json:"ts"`
	Session    string    `json:"session"`
	Node       string    `json:"node"`
	ActorNkey  string    `json:"actor_nkey,omitempty"`
	ActorFp    string    `json:"actor_fp,omitempty"`
	TransferID string    `json:"transfer_id"`
	Path       string    `json:"path,omitempty"`
	Size       int64     `json:"size,omitempty"`
	SHA256     string    `json:"sha256,omitempty"`
	Tier       string    `json:"tier,omitempty"`
	Bucket     string    `json:"bucket,omitempty"`
	Bytes      int64     `json:"bytes,omitempty"`
	DurationMs int64     `json:"duration_ms,omitempty"`
	Code       string    `json:"code,omitempty"`
	Error      string    `json:"error,omitempty"`
}
