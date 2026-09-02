package main

import (
	"errors"
	"testing"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/proto"
)

// audit_test.go (formerly g1g7_audit_test.go) — the G1–G7 cross-cutting audit's ctl-side pins.

// TestResolveJoinOp pins the A4 audit fix: the join-op resolver must distinguish a TRANSPORT error
// (→ surface it so driveAdd HALTs with a retry hint) from a genuine absence (node_unknown → "", a fresh
// prepare/approve) and a stale TERMINAL op (→ "", a fresh prepare), while a LIVE op is resumed by its
// op_id. The pre-fix code returned "" on a transport error too, which forked a resume-after-cutover (the
// leader's nats just SIGKILL-restarted, mid-reconnect) into a fresh nonce → a different op → the
// "another operation is in flight — abort first" refusal.
func TestResolveJoinOp(t *testing.T) {
	transient := errors.New("nats: request timeout")
	cases := []struct {
		name    string
		resp    *proto.ClusterGrowResp
		err     error
		wantOp  string
		wantErr bool
	}{
		{"transport error → surface (NOT absence)", nil, transient, "", true},
		{"nil reply without an error → error", nil, nil, "", true},
		{"node_unknown → genuine absence, fresh prepare", &proto.ClusterGrowResp{Code: adminsock.CodeNodeUnknown}, nil, "", false},
		{"other non-OK → fail closed (retry)", &proto.ClusterGrowResp{Code: "cluster_busy", Error: "x"}, nil, "", true},
		{"stale terminal op → fresh prepare, do NOT resume", &proto.ClusterGrowResp{OK: true, Terminal: true, OpID: "op-terminal"}, nil, "", false},
		{"live op → resume by op_id", &proto.ClusterGrowResp{OK: true, Terminal: false, OpID: "op-live"}, nil, "op-live", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			op, err := resolveJoinOp(c.resp, c.err, "leader-a")
			if (err != nil) != c.wantErr {
				t.Fatalf("resolveJoinOp err = %v, wantErr %v", err, c.wantErr)
			}
			if op != c.wantOp {
				t.Fatalf("resolveJoinOp op = %q, want %q", op, c.wantOp)
			}
		})
	}
}

// TestBlockedConfirmDecision pins C1's edge-count semantics for --auto-confirm-catchup: a confirm is spent
// ONLY on the ENTER-BLOCKED edge (!prevBlocked); budget exhaustion errors out immediately (surfacing the
// actionable BLOCKED hint); a same-stall poll with budget remaining neither confirms nor errors (keep
// polling — the deadline path surfaces a never-cleared stall). The pre-C1 code spent budget every poll.
func TestBlockedConfirmDecision(t *testing.T) {
	cases := []struct {
		name             string
		confirms, budget int
		prevBlocked      bool
		wantErr          bool
		wantConfirm      bool
	}{
		{"budget 0: first BLOCKED errors immediately, no confirm", 0, 0, false, true, false},
		{"budget 1: enter edge spends the confirm", 0, 1, false, false, true},
		{"budget 1: same stall next poll → exhausted → error", 1, 1, true, true, false},
		{"budget 2: enter edge spends confirm #1", 0, 2, false, false, true},
		{"budget 2: same stall, budget remains → keep polling (no confirm, no error)", 1, 2, true, false, false},
		{"budget 2: a distinct later stall (edge) spends confirm #2", 1, 2, false, false, true},
		{"budget 2: exhausted on the 3rd distinct edge", 2, 2, false, true, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			gotErr, gotConfirm := blockedConfirmDecision(c.confirms, c.budget, c.prevBlocked)
			if gotErr != c.wantErr || gotConfirm != c.wantConfirm {
				t.Fatalf("blockedConfirmDecision(%d,%d,%v) = (err=%v,confirm=%v), want (err=%v,confirm=%v)",
					c.confirms, c.budget, c.prevBlocked, gotErr, gotConfirm, c.wantErr, c.wantConfirm)
			}
		})
	}
}

// TestConfirmLanded pins F1's gate: only an OK confirm-op reply counts as landed (spends budget + arms the
// BLOCKED edge). A transport error or a non-OK reply is NOT landed → the next poll must retry it.
func TestConfirmLanded(t *testing.T) {
	cases := []struct {
		name string
		resp *proto.ClusterGrowResp
		err  error
		want bool
	}{
		{"transport error → not landed", nil, errors.New("nats: timeout"), false},
		{"nil reply, no error → not landed", nil, nil, false},
		{"non-OK reply → not landed", &proto.ClusterGrowResp{Code: "store_error", Error: "x"}, nil, false},
		{"OK reply → landed", &proto.ClusterGrowResp{OK: true}, nil, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := confirmLanded(c.resp, c.err); got != c.want {
				t.Fatalf("confirmLanded(%+v, %v) = %v, want %v", c.resp, c.err, got, c.want)
			}
		})
	}
}
