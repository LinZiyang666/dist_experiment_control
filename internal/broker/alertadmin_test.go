package broker

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/cluster"
)

// origin: b4_alertadmin_test.go (renamed in B6) — docs/reviews/b4-plan.md, docs/reviews/b4-review.md
//
// TestB4ValidAlertText (plan §F.14 helper): the LitText-mirroring guard rejects NUL, non-UTF-8,
// and over-length text — the up-front gate that keeps malformed input out of the Propose closure.
func TestB4ValidAlertText(t *testing.T) {
	if err := validAlertText("m", "fine", maxAlertTextLen); err != nil {
		t.Errorf("clean text rejected: %v", err)
	}
	if err := validAlertText("m", "a\x00b", maxAlertTextLen); err == nil {
		t.Error("NUL byte must be rejected")
	}
	if err := validAlertText("m", "\xff\xfe", maxAlertTextLen); err == nil {
		t.Error("invalid UTF-8 must be rejected")
	}
	if err := validAlertText("m", strings.Repeat("x", maxAlertTextLen+1), maxAlertTextLen); err == nil {
		t.Error("over-length text must be rejected")
	}
}

// TestB4AlertRaisePoisonSafe (plan §F.14): every malformed raise is rejected as bad_request BEFORE
// any raft Propose. The backend's admin is nil here, so if validation did NOT short-circuit, the
// code would nil-panic at b.admin.now()/Propose — passing therefore proves zero raft entries.
func TestB4AlertRaisePoisonSafe(t *testing.T) {
	be := &clusterAdminBackend{} // admin == nil ⇒ any Propose access panics
	cases := []struct {
		name string
		req  adminsock.Request
	}{
		{"bad-kind", adminsock.Request{Op: adminsock.OpClusterAlertRaise, AlertKind: "below_quorum", AlertSeverity: "severe", AlertMessage: "x"}},
		{"bad-severity", adminsock.Request{Op: adminsock.OpClusterAlertRaise, AlertKind: "manual", AlertSeverity: "critical", AlertMessage: "x"}},
		{"empty-message", adminsock.Request{Op: adminsock.OpClusterAlertRaise, AlertKind: "manual", AlertSeverity: "severe", AlertMessage: ""}},
		{"nul-message", adminsock.Request{Op: adminsock.OpClusterAlertRaise, AlertKind: "manual", AlertSeverity: "severe", AlertMessage: "bad\x00msg"}},
		{"non-utf8-message", adminsock.Request{Op: adminsock.OpClusterAlertRaise, AlertKind: "manual", AlertSeverity: "severe", AlertMessage: "\xff\xfe"}},
		{"huge-message", adminsock.Request{Op: adminsock.OpClusterAlertRaise, AlertKind: "manual", AlertSeverity: "severe", AlertMessage: strings.Repeat("x", maxAlertTextLen+1)}},
		{"nul-label", adminsock.Request{Op: adminsock.OpClusterAlertRaise, AlertKind: "manual", AlertSeverity: "severe", AlertMessage: "ok", AlertLabel: "l\x00"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			resp := be.handleAlertRaise(c.req)
			if resp.Code != adminsock.CodeBadRequest {
				t.Fatalf("%s: code = %q, want %q", c.name, resp.Code, adminsock.CodeBadRequest)
			}
			if resp.OK {
				t.Fatalf("%s: must not be OK", c.name)
			}
		})
	}
}

// TestB4ValidAlertTextAllowsControlChars (test-adversary, adversarial encoding): validAlertText
// rejects ONLY NUL + non-UTF-8 + over-length — it deliberately does NOT reject newlines or tabs.
// This is the documented behavior (it mirrors the LitText bake, which only bars NUL/non-UTF-8), but
// it means an operator CAN inject a multi-line / tab-containing manual alert message that will then
// flow into the always-on ps/node banner. Pin the behavior so a future change to the banner
// renderer (column alignment) is made with this input shape in mind.
func TestB4ValidAlertTextAllowsControlChars(t *testing.T) {
	for _, s := range []string{"line1\nline2", "col1\tcol2", "trailing newline\n", "\r\n"} {
		if err := validAlertText("--message", s, maxAlertTextLen); err != nil {
			t.Errorf("validAlertText rejected %q (newlines/tabs are currently allowed): %v", s, err)
		}
	}
	// And the boundary: exactly maxLen bytes is allowed, +1 is not.
	if err := validAlertText("--message", strings.Repeat("x", maxAlertTextLen), maxAlertTextLen); err != nil {
		t.Errorf("exactly maxLen must be allowed: %v", err)
	}
	if err := validAlertText("--message", strings.Repeat("x", maxAlertTextLen+1), maxAlertTextLen); err == nil {
		t.Error("maxLen+1 must be rejected")
	}
	// A multibyte rune straddling the byte cap must still be rejected by BYTE length (len, not rune
	// count) — assert the cap is bytes so a 4096-rune unicode string over 4096 bytes is rejected.
	multi := strings.Repeat("世", maxAlertTextLen) // 3 bytes each → far over the byte cap
	if err := validAlertText("--message", multi, maxAlertTextLen); err == nil {
		t.Error("a multibyte string over the BYTE cap must be rejected (cap is bytes, not runes)")
	}
}

// TestB4AlertClearBadRequest (plan §F.18 sibling): clear validates before Propose too — an empty
// or malformed key is bad_request without touching the (nil) admin/Propose.
func TestB4AlertClearBadRequest(t *testing.T) {
	be := &clusterAdminBackend{}
	if resp := be.handleAlertClear(adminsock.Request{Op: adminsock.OpClusterAlertClear, AlertDedupKey: ""}); resp.Code != adminsock.CodeBadRequest {
		t.Fatalf("empty key: code = %q, want bad_request", resp.Code)
	}
	if resp := be.handleAlertClear(adminsock.Request{Op: adminsock.OpClusterAlertClear, AlertDedupKey: "k\x00"}); resp.Code != adminsock.CodeBadRequest {
		t.Fatalf("NUL key: code = %q, want bad_request", resp.Code)
	}
}

// TestB4AlertProposeErrorStoreBranch (plan §F.17 partial): a non-leadership Propose failure maps to
// store_error (the not_leader branch is exercised by the d8 follower drill). admin is nil and the
// store_error branch never touches it.
func TestB4AlertProposeErrorStoreBranch(t *testing.T) {
	be := &clusterAdminBackend{}
	resp := be.alertProposeError(adminsock.OpClusterAlertRaise, errors.New("disk full"))
	if resp.Code != adminsock.CodeStoreError {
		t.Fatalf("non-leader error: code = %q, want store_error", resp.Code)
	}
	if resp.NotLeader {
		t.Error("a store error must not set NotLeader")
	}
}

// TestB4ReconcilerNeverClearsManual (plan §F.15): the leader-gated reconcile loop owns ONLY its
// derived kinds. An operator-raised `manual` alert survives a healthy (would-clear-my-keys)
// reconcile pass — it persists until an operator clears it.
func TestB4ReconcilerNeverClearsManual(t *testing.T) {
	db := reconTestDB(t)
	now := time.Date(2026, 6, 24, 12, 0, 0, 0, time.UTC)
	// Operator raises BOTH a global `manual` key and a labeled `manual:brk-b` key (m5: the
	// broker_draining clear is a PREFIX match, so the invariant must hold for labeled keys too).
	for _, k := range []struct{ id, key string }{{"manual@1", cluster.DedupKeyGlobal(cluster.AlertKindManual)}, {"manual-b@1", cluster.DedupKeyNode(cluster.AlertKindManual, "brk-b")}} {
		cmd, err := cluster.PlanAlertRaise(k.id, cluster.AlertKindManual, cluster.AlertSeveritySevere, k.key, "maintenance", now)
		if err != nil {
			t.Fatal(err)
		}
		if err := cluster.ExecCommand(db, cmd); err != nil {
			t.Fatal(err)
		}
	}
	active := activeKeys(t, db)
	if !active["manual"] || !active["manual:brk-b"] {
		t.Fatalf("setup: both manual keys should be active, got %v", active)
	}

	count := 0
	r := newReconciler(db, &alertFake{leader: true, voters: 3}, func(context.Context) (ReplicaReport, error) { return reportAtTarget(), nil }, &count)
	if err := r.ReconcileAlertsOnce(context.Background()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	active = activeKeys(t, db)
	if !active["manual"] || !active["manual:brk-b"] {
		t.Fatalf("the reconciler cleared a manual alert — it must only touch its own derived kinds; got %v", active)
	}
}
