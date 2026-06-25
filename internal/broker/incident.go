package broker

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
)

// incident.go — the B6 OPS#12 `cluster export-incident` assembler. A leader-local READ-ONLY
// export over the three already-replicated sources: the alert timeline (alerts + alert_acks),
// the membership timeline (cluster_nodes, ALLOWLIST-projected — never the join PoP material),
// and per-session audit history (history-<sid> via the JS audit tail, with a key-denylist scrub
// of the open Body map). It NEVER `SELECT *`s.
//
// External-review F10: the audit-body scrub is BEST-EFFORT, not a guarantee. It redacts VALUES under
// secret-SHAPED keys (the denylist below), but a secret that appears as the VALUE of an innocent key
// (e.g. message="token=abc") or inside the free-form alert/timeline text is NOT caught. The bundle is
// "low-secret", not "secret-free" — a human must review it before sharing externally.

// incidentAuditTailN bounds how many audit messages per sid the export pulls.
const incidentAuditTailN = 200

// auditScrubSubstrings are the case-insensitive key substrings denylisted from the open audit
// Body map (the audit Body is map[string]any, so a future tool could log a secret-shaped key).
// "cmd" is included because an exec/run command line is the MOST likely place a secret appears
// in cleartext (e.g. `mysql -psecret`, a bearer token in a curl arg) and the incident bundle is a
// shareable artifact — forensic intent does not outweigh cleartext credential egress.
var auditScrubSubstrings = []string{
	"pin", "token", "seed", "sig", "nonce", "secret", "key",
	"cmd", "pass", "auth", "cred", "bearer", "cookie", "private",
}

func (b *clusterAdminBackend) handleExportIncident(req adminsock.Request) adminsock.Response {
	ro := b.admin.node.RODB()

	var since time.Time
	if req.Since != "" {
		d, err := time.ParseDuration(req.Since)
		if err != nil {
			return adminsock.Response{Op: req.Op, Error: fmt.Sprintf("bad --since %q: %v", req.Since, err), Code: adminsock.CodeBadRequest}
		}
		since = b.admin.now().Add(-d)
	}

	alerts, err := exportAlerts(ro, since)
	if err != nil {
		return adminsock.Response{Op: req.Op, Error: err.Error(), Code: adminsock.CodeStoreError}
	}
	roster, err := exportRoster(ro)
	if err != nil {
		return adminsock.Response{Op: req.Op, Error: err.Error(), Code: adminsock.CodeStoreError}
	}

	// sids: explicit --sid filter, else the ACTIVE sessions. Audit EH-MAJOR-1: a session-enumeration
	// error must mark the bundle Partial (never silently yield an empty Audit map).
	var partial bool
	var errs []string
	sids := req.SIDs
	if len(sids) == 0 && b.activeSIDs != nil {
		s, e := b.activeSIDs()
		if e != nil {
			partial = true
			errs = append(errs, "active-session enumeration failed: "+e.Error())
		} else {
			sids = s
		}
	}
	audit := map[string][]adminsock.AuditEntry{}
	if b.auditTail != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		for _, sid := range sids {
			entries, e := b.auditTail(ctx, sid, incidentAuditTailN)
			if e != nil {
				// A "history_unavailable" (no stream for this sid) is benign; any other error marks
				// the bundle Partial so a forensic reader knows audit for that sid is missing.
				if !strings.Contains(e.Error(), "history_unavailable") {
					partial = true
					errs = append(errs, "audit for "+sid+": "+e.Error())
				}
				continue
			}
			for i := range entries {
				entries[i].Body = scrubAuditBody(entries[i].Body)
			}
			audit[sid] = entries
		}
	}

	_, leaderID := b.admin.node.LeaderWithID()
	bundle := &adminsock.IncidentBundle{
		Schema:        "incident",
		SchemaVersion: 1,
		Errors:        errs,
		Partial:       partial,
		GeneratedAt:   b.admin.now().UTC().Format(time.RFC3339Nano),
		LeaderID:      leaderID,
		Since:         req.Since,
		Alerts:        alerts,
		Roster:        roster,
		Audit:         audit,
	}
	return adminsock.Response{Op: req.Op, OK: true, Incident: bundle}
}

// exportAlerts reads the alert history (+ ack) since a cutoff (zero = all). Explicit column list.
func exportAlerts(ro *sql.DB, since time.Time) ([]adminsock.IncidentAlert, error) {
	q := `SELECT a.id, a.kind, a.severity, a.dedup_key, a.state, a.message, a.raised_at,
	             COALESCE(a.cleared_at,''), COALESCE(k.acked_by,''), COALESCE(k.acked_at,'')
	      FROM alerts a LEFT JOIN alert_acks k ON k.dedup_key = a.dedup_key`
	args := []any{}
	if !since.IsZero() {
		q += ` WHERE a.raised_at >= ?`
		args = append(args, since.UTC().Format(time.RFC3339Nano))
	}
	q += ` ORDER BY a.raised_at`
	rows, err := ro.Query(q, args...)
	if err != nil {
		return nil, fmt.Errorf("export alerts: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []adminsock.IncidentAlert
	for rows.Next() {
		var a adminsock.IncidentAlert
		if err := rows.Scan(&a.ID, &a.Kind, &a.Severity, &a.DedupKey, &a.State, &a.Message, &a.RaisedAt,
			&a.ClearedAt, &a.AckedBy, &a.AckedAt); err != nil {
			return nil, fmt.Errorf("scan alert: %w", err)
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// exportRoster is the membership-timeline allowlist projection. It enumerates ONLY the public +
// timeline columns; join_nonce/join_sig (PoP material) and cert columns are NEVER selected (the
// same no-SELECT-* discipline as the manifest's clusteroffline.ProjectRoster).
func exportRoster(ro *sql.DB) ([]adminsock.IncidentNode, error) {
	rows, err := ro.Query(`SELECT node_id, name, phase, raft_addr,
	    COALESCE(added_at,''), COALESCE(phase_changed_at,''), COALESCE(voter_add_error,'')
	    FROM cluster_nodes ORDER BY node_id`)
	if err != nil {
		return nil, fmt.Errorf("export roster: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []adminsock.IncidentNode
	for rows.Next() {
		var n adminsock.IncidentNode
		if err := rows.Scan(&n.NodeID, &n.Name, &n.Phase, &n.RaftAddr, &n.AddedAt, &n.PhaseChangedAt, &n.VoterAddError); err != nil {
			return nil, fmt.Errorf("scan node: %w", err)
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

// scrubAuditBody redacts any key whose name contains a denylisted secret-shaped substring. The
// audit Body is an open map[string]any, so this is a defense-in-depth pass over a surface a future
// tool could pollute. It RECURSES into nested map[string]any / []any values (the audit Body's
// free-form AuditCall.Target is a nested map, so a depth-1-only scrub would let target.token leak).
func scrubAuditBody(body map[string]any) map[string]any {
	if body == nil {
		return nil
	}
	return scrubAny(body).(map[string]any)
}

// scrubAny applies the denylist recursively: a denylisted KEY redacts its whole subtree; an
// allowed key descends into nested maps/slices.
func scrubAny(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, val := range t {
			if keyIsDenylisted(k) {
				out[k] = "[redacted]"
			} else {
				out[k] = scrubAny(val)
			}
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, e := range t {
			out[i] = scrubAny(e)
		}
		return out
	default:
		return v
	}
}

func keyIsDenylisted(k string) bool {
	lk := strings.ToLower(k)
	for _, sub := range auditScrubSubstrings {
		if strings.Contains(lk, sub) {
			return true
		}
	}
	return false
}
