package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
	"github.com/spf13/cobra"
)

// audit_nats_test.go (formerly g1g7_audit_nats_test.go) — the audit fixes whose regression guards need a live NATS harness
// (buildUpgradeNodes / driveAdd). Reuses the g5 external-review harness (startCLIExternalReviewNATS,
// cliExternalAccount, mustSub).

// TestBuildUpgradeNodesFailsClosedOnNodeListError (A7 Stage-C): a node-list reply that decodes cleanly but
// carries an application-error Code + empty Nodes must FAIL CLOSED — otherwise agentRelease stays empty,
// every host looks stale, and an already-at-target cluster is planned into a full disruptive roll.
func TestBuildUpgradeNodesFailsClosedOnNodeListError(t *testing.T) {
	url := startCLIExternalReviewNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer nc.Close()
	_, pub := cliExternalAccount(t)
	actor, sid := "Uactor-a7", "lab"
	mustSub(t, nc, proto.SubjCtrlNodeList(actor, sid), func(m *nats.Msg) {
		if m.Reply != "" {
			body, _ := json.Marshal(proto.NodeListResp{Code: "store_error", Error: "sqlite busy"})
			_ = m.Respond(body)
		}
	})
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	_, berr := buildUpgradeNodes(context.Background(), nc, actor, sid, pub, io.Discard)
	if berr == nil {
		t.Fatal("A7: a node-list reply carrying an application-error Code must fail closed, not plan on incomplete data")
	}
	if !strings.Contains(berr.Error(), "store_error") {
		t.Fatalf("A7: the fail-closed error should surface the node-list Code, got: %v", berr)
	}
}

// TestBuildUpgradeNodesWarnsOnResponderFallback (A2 Stage-C): when the signed roster is unavailable but >1
// broker answers health, the planner FALLS BACK to responders (must NOT fail closed — a pre-G3 cluster
// needs it) and WARNs loudly so an operator whose roster fetch merely blipped is not silently planning a
// quorum-touching roll unverified.
func TestBuildUpgradeNodesWarnsOnResponderFallback(t *testing.T) {
	url := startCLIExternalReviewNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer nc.Close()
	_, pub := cliExternalAccount(t)
	actor, sid := "Uactor-a2", "lab"
	mustSub(t, nc, proto.SubjCtrlNodeList(actor, sid), func(m *nats.Msg) {
		if m.Reply != "" {
			body, _ := json.Marshal(proto.NodeListResp{}) // Code=="" (success), empty Nodes is legitimate
			_ = m.Respond(body)
		}
	})
	mustSub(t, nc, proto.SubjCtrlClusterHealth(actor), func(m *nats.Msg) {
		a, _ := json.Marshal(proto.ClusterHealthResp{NodeID: "brk-a", ReleaseVersion: "v1", IsVoter: true})
		b, _ := json.Marshal(proto.ClusterHealthResp{NodeID: "brk-b", ReleaseVersion: "v1", IsVoter: true})
		_ = m.Respond(a)
		_ = m.Respond(b)
	})
	// deliberately NO SubjCtrlClusterRoster responder → fetchUpgradeRosterWithRetry returns nil → fallback.
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	nodes, berr := buildUpgradeNodes(context.Background(), nc, actor, sid, pub, &buf)
	if berr != nil {
		t.Fatalf("A2: the responder fallback must SUCCEED (not fail closed) for a rosterless multi-broker cluster: %v", berr)
	}
	if !strings.Contains(buf.String(), "signed cluster roster was unavailable") {
		t.Fatalf("A2: the fallback over >1 responder must WARN loudly; got out=%q", buf.String())
	}
	if len(nodes) != 2 {
		t.Fatalf("A2: fallback should plan over both responders, got %d", len(nodes))
	}
}

// TestDriveAddDryRunSuppressesWebhook (A5 Stage-C): a --dry-run that hits a preflight HALT must fire NO
// webhook POST (the halt path used to POST despite the "touching nothing" promise).
func TestDriveAddDryRunSuppressesWebhook(t *testing.T) {
	url := startCLIExternalReviewNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer nc.Close()
	// NO health responder → currentLeader fails → the preflight HALT (the path that used to POST).
	var posts int32
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		atomic.AddInt32(&posts, 1)
	}))
	defer srv.Close()

	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	cmd.SetOut(io.Discard)
	jp := joinerParams{Joiner: "brk-x"}
	derr := driveAdd(cmd, nc, "Uactor-a5", "lab", nil, "", jp, "", true, time.Second, srv.URL)
	if derr == nil {
		t.Fatal("A5: expected a preflight HALT (no leader) so the dry-run webhook-suppression path is exercised")
	}
	if n := atomic.LoadInt32(&posts); n != 0 {
		t.Fatalf("A5: --dry-run must POST no webhook even on a preflight halt, got %d posts", n)
	}
}

// TestWaitJoinServingRetriesFailedConfirm (F1 external review): a confirm-op that fails transiently must NOT
// burn the --auto-confirm-catchup budget or arm the BLOCKED edge — the NEXT BLOCKED poll must re-send it, so
// the join still converges to SERVING instead of stalling to the full timeout. Reproduces the F1 scenario
// with budget=2: the first confirm-op returns a non-OK error, the op stays BLOCKED, and the fix must retry.
func TestWaitJoinServingRetriesFailedConfirm(t *testing.T) {
	url := startCLIExternalReviewNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer nc.Close()
	seed, _ := cliExternalAccount(t)
	actor := "Uactor-f1"
	var confirmCalls int32
	mustSub(t, nc, proto.SubjCtrlClusterGrow(actor), func(m *nats.Msg) {
		if m.Reply == "" {
			return
		}
		var req proto.ClusterGrowReq
		_ = json.Unmarshal(m.Data, &req)
		var resp proto.ClusterGrowResp
		switch req.Op {
		case "join-status":
			// The op only progresses to SERVING once a confirm-op has LANDED (confirmCalls>=2 here: the first
			// confirm fails, the retry succeeds). Pre-fix, a failed confirm burned the budget + armed the edge
			// so the retry never fired, confirmCalls stayed 1, and this stayed BLOCKED until the join timeout.
			if atomic.LoadInt32(&confirmCalls) >= 2 {
				resp = proto.ClusterGrowResp{OK: true, OpState: "SERVING", Terminal: true}
			} else {
				resp = proto.ClusterGrowResp{OK: true, OpState: "BLOCKED", LastError: "catch_up_stalled"}
			}
		case "confirm-op":
			// The FIRST confirm-op fails (non-OK, transient); the retry succeeds.
			if atomic.AddInt32(&confirmCalls, 1) == 1 {
				resp = proto.ClusterGrowResp{Code: "store_error", Error: "transient"}
			} else {
				resp = proto.ClusterGrowResp{OK: true, OpID: req.OpID}
			}
		default:
			resp = proto.ClusterGrowResp{OK: true}
		}
		body, _ := json.Marshal(resp)
		_ = m.Respond(body)
	})
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	// budget=2 reproduces the F1 stall: pre-fix, a failed confirm burned a slot + armed prevBlocked, so the
	// same BLOCKED state never re-confirmed and stalled to the timeout. No health responder → joinerIsVoter
	// stays false (503 fast-path). A 30s cap fails a regressed build within a bounded time.
	jp := joinerParams{Joiner: "brk-x", AutoConfirmCatchup: 2}
	werr := waitJoinServing(context.Background(), nc, actor, seed, "leader-a", "op-1", jp, 30*time.Second, io.Discard)
	if werr != nil {
		t.Fatalf("F1: waitJoinServing must reach SERVING via a RETRIED confirm, not stall to timeout: %v", werr)
	}
	if n := atomic.LoadInt32(&confirmCalls); n < 2 {
		t.Fatalf("F1: a failed first confirm-op must be RETRIED on the next BLOCKED poll (budget must not be burned), got %d confirm calls", n)
	}
}
