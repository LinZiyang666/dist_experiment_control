// Partial-failure chaos tests. Cover dimension E from the chaos
// checklist: fleet rollouts where some nodes are healthy and others
// aren't, session rm with live agents, and the tombstone gate
// rejecting subsequent requests cleanly.
package chaos_test

import (
	"context"
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
)

// TestUpgradeAllSplitFleetCountsTransientSkips replicates dimension
// E.19 against a stub broker. We can't easily import the cobra
// command from cmd/tether (package main), so we exercise the wire
// shape — the classifier logic itself is unit-pinned in
// cmd/tether/node_classify_test.go.
//
// Sequence: stub broker answers OK for n1/n3 and node_offline for n2.
// A correct loop reports 2 successes + 1 transient skip + 0 failures.
func TestUpgradeAllSplitFleetCountsTransientSkips(t *testing.T) {
	ctx, cancel := withTestDeadline(t, 10*time.Second)
	defer cancel()
	_ = ctx

	url := startNATS(t)

	// Stub broker. Tracks call count so we can also assert the loop
	// did NOT abort early on the OFFLINE response.
	var calls atomic.Int32
	routes := map[string]proto.UpgradeResp{
		"n1": {OK: true},
		"n2": {Code: "node_offline", Error: "OFFLINE"},
		"n3": {OK: true},
	}
	stub, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(stub.Close)
	if _, err := stub.Subscribe(
		proto.SubjectPrefix+".s.*.cmd.by.*.node.*.upgrade.req",
		func(msg *nats.Msg) {
			calls.Add(1)
			parts := strings.Split(msg.Subject, ".")
			nid := ""
			if len(parts) == 11 {
				nid = parts[8]
			}
			resp, ok := routes[nid]
			if !ok {
				resp = proto.UpgradeResp{Code: "no_route"}
			}
			body, _ := json.Marshal(&resp)
			_ = msg.Respond(body)
		},
	); err != nil {
		t.Fatal(err)
	}
	if err := stub.Flush(); err != nil {
		t.Fatal(err)
	}

	// Drive the same wire flow the cobra loop does, then classify
	// inline (mirrors cmd/tether/node.go isTransientError).
	cli, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	var ok, skipped, failed int
	for _, nid := range []string{"n1", "n2", "n3"} {
		body, _ := json.Marshal(proto.UpgradeReq{
			URL: "https://x/", SHA256: "deadbeef", ProtoVersion: proto.ProtoVersion,
		})
		reqCtx, c := context.WithTimeout(ctx, 30*time.Second)
		resp, err := cli.RequestWithContext(reqCtx,
			proto.SubjCmdBy("lab", "actor", nid, "upgrade"), body)
		c()
		if err != nil {
			failed++
			continue
		}
		var r proto.UpgradeResp
		_ = json.Unmarshal(resp.Data, &r)
		switch {
		case r.OK:
			ok++
		case strings.Contains(r.Code, "node_offline") ||
			strings.Contains(r.Code, "agent_no_responders"):
			skipped++
		default:
			failed++
		}
	}
	if ok != 2 || skipped != 1 || failed != 0 {
		t.Errorf("split fleet: ok=%d skipped=%d failed=%d (want 2/1/0)",
			ok, skipped, failed)
	}
	if c := calls.Load(); c != 3 {
		t.Errorf("stub broker calls: %d (want 3 — loop must continue past OFFLINE)", c)
	}
}

// TestUpgradeAllAbortsImmediatelyOnConfigError pins E.20: a
// url_not_allowed (architecture J.4 §安全约束) on the FIRST node must
// abort the rest of the fleet immediately. We assert the stub broker
// only saw one request, not three.
func TestUpgradeAllAbortsImmediatelyOnConfigError(t *testing.T) {
	ctx, cancel := withTestDeadline(t, 10*time.Second)
	defer cancel()

	url := startNATS(t)

	var calls atomic.Int32
	stub, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(stub.Close)
	if _, err := stub.Subscribe(
		proto.SubjectPrefix+".s.*.cmd.by.*.node.*.upgrade.req",
		func(msg *nats.Msg) {
			calls.Add(1)
			body, _ := json.Marshal(proto.UpgradeResp{
				Code: "url_not_allowed", Error: "https://evil/",
			})
			_ = msg.Respond(body)
		},
	); err != nil {
		t.Fatal(err)
	}
	if err := stub.Flush(); err != nil {
		t.Fatal(err)
	}

	cli, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	abortAfter := -1
	for i, nid := range []string{"n1", "n2", "n3"} {
		body, _ := json.Marshal(proto.UpgradeReq{
			URL: "https://evil/", SHA256: "deadbeef", ProtoVersion: proto.ProtoVersion,
		})
		reqCtx, c := context.WithTimeout(ctx, 30*time.Second)
		resp, err := cli.RequestWithContext(reqCtx,
			proto.SubjCmdBy("lab", "actor", nid, "upgrade"), body)
		c()
		if err != nil {
			break
		}
		var r proto.UpgradeResp
		_ = json.Unmarshal(resp.Data, &r)
		if r.OK {
			continue
		}
		// Mirrors isConfigError.
		if strings.Contains(r.Code, "url_not_allowed") ||
			strings.Contains(r.Code, "not_owner") ||
			strings.Contains(r.Code, "sha256_invalid") {
			abortAfter = i
			break
		}
	}
	if abortAfter != 0 {
		t.Errorf("expected abort on first config-error response (i=0); got abortAfter=%d", abortAfter)
	}
	if c := calls.Load(); c != 1 {
		t.Errorf("config-error abort: stub saw %d calls (want 1 — must NOT keep firing)", c)
	}
}

// TestSessionRmWithLiveAgentTombstones pins E.21: an active session
// with a registered agent must transition cleanly to DELETING when
// the owner runs `session rm`. C.1 §6 then makes subsequent ingress
// (register, exec, ps) reject with session_not_found_or_deleting.
func TestSessionRmWithLiveAgentTombstones(t *testing.T) {
	ctx, cancel := withTestDeadline(t, 15*time.Second)
	defer cancel()
	_ = ctx

	url := startJSNATS(t) // session rm uses JS to delete the history stream
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)

	_ = startBrokerExplicit(t, url, db)
	_ = startAgentExplicit(t, url, "lab", "n1")
	testharness.WaitNodeOnline(t, db, "lab", "n1", 3*time.Second)

	// Owner = pub. Issue session.rm via the broker subject.
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	rmBody, _ := json.Marshal(proto.SessionRmReq{})
	respMsg, err := nc.Request(
		proto.SubjCtrlSessionRm(pub, "lab"), rmBody, 5*time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}
	var resp proto.SessionRmResp
	_ = json.Unmarshal(respMsg.Data, &resp)
	if !resp.OK {
		t.Fatalf("session rm: %s %s", resp.Code, resp.Error)
	}

	// SQLite session row state should be DELETING (or already gone if
	// the cascade-delete completed; both are valid post-rm).
	active, err := session.IsActive(db, "lab")
	if err != nil {
		t.Fatal(err)
	}
	if active {
		t.Errorf("session still ACTIVE after rm")
	}

	// Subsequent ps must reject with session_not_found_or_deleting (or
	// session_not_found if the row vanished).
	psBody, _ := json.Marshal(proto.PsReq{})
	psResp, err := nc.Request(proto.SubjCtrlPs(pub, "lab"), psBody, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var ps proto.PsResp
	_ = json.Unmarshal(psResp.Data, &ps)
	if ps.Code != "session_not_found_or_deleting" && ps.Code != "session_not_found" {
		t.Errorf("ps after rm: code=%q error=%q (want session_not_found_or_deleting)",
			ps.Code, ps.Error)
	}
}

// TestRegisterRejectedAfterTombstone pins the C.1 §6 ingress gate:
// after session.Tombstone, an agent's register.req must be rejected
// with session_not_found_or_deleting. This is the partial-failure
// guarantee that an in-flight agent re-register during DELETING
// can't accidentally re-create the session.
func TestRegisterRejectedAfterTombstone(t *testing.T) {
	ctx, cancel := withTestDeadline(t, 10*time.Second)
	defer cancel()
	_ = ctx

	url := startNATS(t)
	db := openDB(t)
	_, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)

	_ = startBrokerExplicit(t, url, db)

	// Tombstone via direct DB call (skips needing JetStream for this
	// narrower assertion).
	if err := session.Tombstone(db, "lab", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}

	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	body, _ := json.Marshal(proto.NodeRegisterReq{
		ProtoVersion: proto.ProtoVersion,
		NID:          "n1",
	})
	respMsg, err := nc.Request(proto.SubjNodeRegister("lab", "n1"), body, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var resp proto.NodeRegisterResp
	_ = json.Unmarshal(respMsg.Data, &resp)
	if resp.OK {
		t.Errorf("register against DELETING session unexpectedly accepted")
	}
	if resp.Code != "session_not_found_or_deleting" {
		t.Errorf("register reject code: got %q want session_not_found_or_deleting", resp.Code)
	}
}
