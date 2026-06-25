package broker

// alertadmin.go — B4 operator alert verbs (raise / clear) over the broker-local admin socket.
// They are the operator-trust-tier counterpart to `alert ls` / `alert ack` (which are member
// NATS RPCs): raising/clearing a cluster-wide alert is an operator action and rides the SAME
// leader-local admin path as the cluster_* verbs (callAdmin → HandleCluster → node.Propose),
// so NO NATS subject and NO member ACL is touched.
//
// Manual-kind ONLY: a system kind would fight the leader-gated reconciler (which owns the
// derived kinds replication_degraded / below_quorum / broker_draining:* and re-raises/clears
// them). The reconciler never touches `manual`, so an operator-raised manual alert persists
// until an operator clears it.

import (
	"database/sql"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/cluster"
)

// maxAlertTextLen caps an operator alert message / label / clear-key. A manual alert is a short
// human note; a megabyte string is malformed input, not a store failure → bad_request.
const maxAlertTextLen = 4096

// validAlertText mirrors the LitText bake constraints (reject NUL + non-UTF-8) plus a length
// cap, so a malformed message/label/key is rejected as bad_request BEFORE Propose — the Propose
// closure then only ever sees clean input and PlanAlert* cannot fail on LitText (which would
// otherwise surface as a mislabeled store_error). field names the offending arg for the message.
func validAlertText(field, s string, maxLen int) error {
	if !utf8.ValidString(s) {
		return fmt.Errorf("alert: %s must be valid UTF-8", field)
	}
	if strings.ContainsRune(s, 0) {
		return fmt.Errorf("alert: %s must not contain a NUL byte", field)
	}
	if len(s) > maxLen {
		return fmt.Errorf("alert: %s is too long (%d bytes; max %d)", field, len(s), maxLen)
	}
	return nil
}

// handleAlertRaise raises an operator manual alert through raft (OpAlertRaise). Validation is
// all up-front so a bad request never reaches Propose: kind must be manual, severity must be in
// the enum, message must be present + clean, label (if any) clean. The in-closure read reports
// whether a manual alert was already ACTIVE for the resolved key (PlanAlertRaise is a
// WHERE-NOT-EXISTS no-op in that case), so the CLI can warn instead of silently overwriting.
func (b *clusterAdminBackend) handleAlertRaise(req adminsock.Request) adminsock.Response {
	op := adminsock.OpClusterAlertRaise
	bad := func(msg string) adminsock.Response {
		return adminsock.Response{Op: op, Error: msg, Code: adminsock.CodeBadRequest}
	}
	if req.AlertKind != cluster.AlertKindManual {
		return bad(`alert raise: --kind must be "manual" (system kinds are raised automatically by the cluster)`)
	}
	if !cluster.ValidAlertSeverity(req.AlertSeverity) {
		return bad(fmt.Sprintf("alert raise: --severity must be %q or %q", cluster.AlertSeveritySevere, cluster.AlertSeverityInfo))
	}
	if req.AlertMessage == "" {
		return bad("alert raise: --message is required")
	}
	if err := validAlertText("--message", req.AlertMessage, maxAlertTextLen); err != nil {
		return bad(err.Error())
	}
	if err := validAlertText("--label", req.AlertLabel, maxAlertTextLen); err != nil {
		return bad(err.Error())
	}
	key := cluster.DedupKeyGlobal(cluster.AlertKindManual)
	if req.AlertLabel != "" {
		key = cluster.DedupKeyNode(cluster.AlertKindManual, req.AlertLabel)
	}
	now := b.admin.now()
	id := newAlertID(key, now)
	var alreadyActive bool
	if err := b.admin.node.Propose(func(db *sql.DB) (*cluster.Command, error) {
		var n int
		if e := db.QueryRow(`SELECT COUNT(*) FROM alerts WHERE dedup_key=? AND state='ACTIVE'`, key).Scan(&n); e != nil {
			return nil, e
		}
		alreadyActive = n > 0
		return cluster.PlanAlertRaise(id, cluster.AlertKindManual, req.AlertSeverity, key, req.AlertMessage, now)
	}); err != nil {
		return b.alertProposeError(op, err)
	}
	return adminsock.Response{Op: op, OK: true, Alert: &adminsock.AlertResult{DedupKey: key, AlreadyActive: alreadyActive}}
}

// handleAlertClear clears the ACTIVE alert for a dedup_key (OpAlertClear). Idempotent: clearing
// an absent/already-cleared key is a committed no-op, reported OK. No pre-read (avoids the
// MaxOpenConns(1) nested-query hazard) — PlanAlertClear's UPDATE … WHERE state='ACTIVE' is
// self-idempotent.
func (b *clusterAdminBackend) handleAlertClear(req adminsock.Request) adminsock.Response {
	op := adminsock.OpClusterAlertClear
	if req.AlertDedupKey == "" {
		return adminsock.Response{Op: op, Error: `alert clear: a dedup_key is required (e.g. "manual" or "manual:<label>")`, Code: adminsock.CodeBadRequest}
	}
	if err := validAlertText("dedup_key", req.AlertDedupKey, maxAlertTextLen); err != nil {
		return adminsock.Response{Op: op, Error: err.Error(), Code: adminsock.CodeBadRequest}
	}
	now := b.admin.now()
	if err := b.admin.node.Propose(func(*sql.DB) (*cluster.Command, error) {
		return cluster.PlanAlertClear(req.AlertDedupKey, now)
	}); err != nil {
		return b.alertProposeError(op, err)
	}
	return adminsock.Response{Op: op, OK: true, Alert: &adminsock.AlertResult{DedupKey: req.AlertDedupKey}}
}

// alertProposeError maps a Propose failure for an alert op. A leadership loss BETWEEN
// HandleCluster's leader gate and this Propose (TOCTOU) → not_leader naming the new leader; any
// other failure is a genuine store/raft error. Because all input was validated up-front, a Plan
// (LitText) rejection cannot occur here, so there is no bad_request branch.
func (b *clusterAdminBackend) alertProposeError(op string, err error) adminsock.Response {
	if cluster.IsNotLeader(err) {
		_, leaderID := b.admin.node.LeaderWithID()
		host := b.admin.leaderHost(leaderID)
		msg := "not leader; re-run on the leader host"
		if leaderID == "" {
			msg = "no leader (election in progress); retry"
		}
		return adminsock.Response{Op: op, NotLeader: true, LeaderHost: host, Error: msg, Code: adminsock.CodeNotLeader}
	}
	return adminsock.Response{Op: op, Error: err.Error(), Code: adminsock.CodeStoreError}
}
