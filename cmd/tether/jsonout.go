package main

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/proto"
)

// jsonout.go (B2 items 1+2) — the shared --json machinery.
//
// The 8 list/result CLI-local DTOs below each carry BOTH a "schema" string discriminator and a
// "schema_version" int, so a monitor keys on (schema, schema_version). The `cluster status` family
// is the exception: socket/offline reports (adminsock.ClusterStatusReport) carry schema_version +
// discriminate on `view` ("ctl-nats"/"offline") and the lightweight `--remote` summary discriminates
// on view ("ctl-remote") with no schema_version (B1 review F6). These structs are CLI-LOCAL — the
// proto wire (internal/proto) gains no fields, so wire byte-compatibility is preserved. --json is
// opt-in; the default human text path is unchanged.
//
// schema_version bump policy: bump ONLY on a breaking change (a removed/renamed key, a changed
// type, or changed semantics of an existing field). Adding an omitempty field is NOT a bump.
// Branch-load-bearing fields (schema, schema_version, and any field a script switches on) are
// never omitempty — their stable presence IS the schema.

// emitJSON marshals payload as indented JSON to w. An encode failure is an internal (70) error.
func emitJSON(w io.Writer, payload any) error {
	b, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return &ExitError{Class: exitInternal, Err: fmt.Errorf("encode --json: %w", err)}
	}
	_, err = fmt.Fprintln(w, string(b))
	return err
}

// normSlice returns a non-nil empty slice so a nil []T marshals as [] (not null) — a script doing
// `jq '.nodes | length'` must get 0, never a null-deref.
func normSlice[T any](s []T) []T {
	if s == nil {
		return []T{}
	}
	return s
}

type nodeLsJSON struct {
	Schema        string                `json:"schema"`         // "node_ls"
	SchemaVersion int                   `json:"schema_version"` // 1
	Nodes         []proto.NodeListEntry `json:"nodes"`
}

type alertLsJSON struct {
	Schema        string            `json:"schema"`         // "alert_ls"
	SchemaVersion int               `json:"schema_version"` // 1
	Alerts        []proto.AlertView `json:"alerts"`         // each carries dedup_key for the ack round-trip
}

type psJSON struct {
	Schema        string              `json:"schema"`         // "ps"
	SchemaVersion int                 `json:"schema_version"` // 1
	Processes     []proto.PsEntry     `json:"processes"`
	Ports         []proto.PsPortEntry `json:"ports"`
}

type sessionLsJSON struct {
	Schema        string               `json:"schema"`         // "session_ls"
	SchemaVersion int                  `json:"schema_version"` // 1
	Sessions      []proto.SessionEntry `json:"sessions"`
	ActiveSID     string               `json:"active_sid,omitempty"` // informational ⇒ omitempty
}

type adminSessionsJSON struct {
	Schema        string                   `json:"schema"`         // "admin_sessions"
	SchemaVersion int                      `json:"schema_version"` // 1
	Sessions      []adminsock.SessionEntry `json:"sessions"`
}

type adminNodesJSON struct {
	Schema        string                `json:"schema"`         // "admin_nodes"
	SchemaVersion int                   `json:"schema_version"` // 1
	Nodes         []adminsock.NodeEntry `json:"nodes"`
}

type adminAuditJSON struct {
	Schema        string                 `json:"schema"`         // "admin_audit"
	SchemaVersion int                    `json:"schema_version"` // 1
	Audit         []adminsock.AuditEntry `json:"audit"`
}

type exposeJSON struct {
	Schema        string `json:"schema"`         // "expose"
	SchemaVersion int    `json:"schema_version"` // 1
	PublicHost    string `json:"public_host"`
	Port          int    `json:"port"`
	Name          string `json:"name"`
	Node          string `json:"node"`
	// B4: where the broker homed the expose + its rebuild policy. All omitempty (a single
	// broker / un-homed / rebuild-ON default leaves them zero) → no schema_version bump.
	HomeBroker string `json:"home_broker,omitempty"`
	Epoch      int64  `json:"epoch,omitempty"`
	RebuildOff bool   `json:"rebuild_off,omitempty"`
}

// exposeExplainJSON is the B4 `expose explain <name>` machine schema. It carries ONLY the fields
// that are actually recorded today (name/node/local_port/state/public_port/home/epoch/rebuild +
// created); the deferred observability fields (last_error / reconnects / ready_reason /
// last_rehome_at) are ABSENT, not null placeholders — a clean schema_version bump adds them when
// the agent→broker telemetry channel lands (B5 / DOC#5).
type exposeExplainJSON struct {
	Schema        string `json:"schema"`         // "expose_explain"
	SchemaVersion int    `json:"schema_version"` // 1
	Name          string `json:"name"`
	Node          string `json:"node"`
	LocalPort     int    `json:"local_port"`
	PublicPort    int    `json:"public_port"`
	State         string `json:"state"`
	Rebuild       bool   `json:"rebuild"`               // true = auto-rehome on home failure (default); false = --no-rebuild
	HomeBroker    string `json:"home_broker,omitempty"` // '' = single broker / un-homed
	Epoch         int64  `json:"epoch,omitempty"`       // >0 ⇒ the expose has moved homes
	Moved         bool   `json:"moved"`                 // epoch>0; script-switched ⇒ never omitempty (a bool's absence is ambiguous)
	CreatedByFP   string `json:"created_by_fp,omitempty"`
}

// takeoverPlanJSON is the B5 OPS#10 `takeover-natsconf --plan --json` schema. `changed` is
// branch-load-bearing (a converge script keys on it) ⇒ never omitempty.
type takeoverPlanJSON struct {
	Schema               string   `json:"schema"`         // "takeover_natsconf_plan"
	SchemaVersion        int      `json:"schema_version"` // 1
	Changed              bool     `json:"changed"`
	ServerName           string   `json:"server_name"`
	ClientListen         string   `json:"client_listen"`
	JSStoreDir           string   `json:"js_store_dir,omitempty"`
	Routes               []string `json:"routes"`
	AuthUsers            []string `json:"auth_users"`
	OwnershipRegenerated []string `json:"ownership_regenerated"`
	DryRun               string   `json:"dry_run"`
	RestartOrderHint     string   `json:"restart_order_hint"`
}

type exposeRmJSON struct {
	Schema        string `json:"schema"`         // "expose_rm"
	SchemaVersion int    `json:"schema_version"` // 1
	Name          string `json:"name"`
	Node          string `json:"node"`
	Port          int    `json:"port"` // freed public port (ExposeRmResp has no public_host)
}
