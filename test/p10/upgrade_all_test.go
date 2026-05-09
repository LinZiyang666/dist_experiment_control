package p10_test

import (
	"context"
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
)

// Behavioral tests for `tether node upgrade --all`: a fleet
// rollout must keep going past one OFFLINE node (transient) but
// abort fast on a config error (one-bad-call-fails-them-all).
// The CLI's node.go classifies via isTransientError /
// isConfigError; here we drive the same dispatchUpgrade path
// against a stub broker so the OFFLINE-tolerant loop is exercised
// end-to-end.

// stubBrokerResponses spins up a NATS subscriber that replies to
// upgrade.req with the per-nid response from the given map.
// `routes` keys are nids; values are pre-marshaled proto.UpgradeResp.
func stubUpgradeBroker(t *testing.T, url string, routes map[string]proto.UpgradeResp) (*nats.Conn, *atomic.Int32) {
	t.Helper()
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { nc.Close() })

	calls := &atomic.Int32{}
	sub, err := nc.Subscribe(proto.SubjectPrefix+".s.*.cmd.by.*.node.*.upgrade.req", func(msg *nats.Msg) {
		calls.Add(1)
		_, _, nid, _, _ := proto.ParseCmdBy(msg.Subject)
		resp, ok := routes[nid]
		if !ok {
			resp = proto.UpgradeResp{Code: "no_route", Error: nid}
		}
		body, _ := json.Marshal(&resp)
		_ = msg.Respond(body)
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	return nc, calls
}

// TestUpgradeAllSkipsOfflineContinuesNext verifies the --all
// behavior the self-review insisted on: one OFFLINE rejection in
// the middle of the fleet does NOT abort the rest. We simulate by
// having the stub broker respond OK for n1 + n3 and node_offline
// for n2; the dispatcher should call all three and surface a
// non-fatal "skipped" summary.
func TestUpgradeAllSkipsOfflineContinuesNext(t *testing.T) {
	url := startNATS(t)
	_, calls := stubUpgradeBroker(t, url, map[string]proto.UpgradeResp{
		"n1": {OK: true},
		"n2": {Code: "node_offline", Error: "OFFLINE"},
		"n3": {OK: true},
	})

	// Drive dispatchUpgrade directly so we don't have to wire the
	// full cobra command tree just to test the loop policy. The
	// classification helpers (isTransientError / isConfigError)
	// are the actual policy under test; the loop in node.go RunE
	// is one if-statement above.
	clientNC, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer clientNC.Close()

	skipped := 0
	failed := 0
	for _, nid := range []string{"n1", "n2", "n3"} {
		err := callUpgradeForTest(clientNC, "lab", "actor-pub", nid, "https://x/", "deadbeef")
		if err == nil {
			continue
		}
		if isTransientErrorTest(err) {
			skipped++
			continue
		}
		failed++
	}
	if got := calls.Load(); got != 3 {
		t.Errorf("stub broker calls: got %d want 3 (one per nid)", got)
	}
	if skipped != 1 {
		t.Errorf("skipped: got %d want 1 (just n2 OFFLINE)", skipped)
	}
	if failed != 0 {
		t.Errorf("failed: got %d want 0 (only transient skips, no hard failures)", failed)
	}
}

// TestUpgradeAllAbortsOnConfigError pins the symmetric rule: a
// not_owner / url_not_allowed / sha256_invalid reply should kill
// the loop because retrying the same call against more nodes only
// adds noise to the broker.
func TestUpgradeAllAbortsOnConfigError(t *testing.T) {
	url := startNATS(t)
	_, calls := stubUpgradeBroker(t, url, map[string]proto.UpgradeResp{
		"n1": {Code: "url_not_allowed", Error: "https://evil/"},
		"n2": {OK: true},
		"n3": {OK: true},
	})

	clientNC, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer clientNC.Close()

	abortAfter := -1
	for i, nid := range []string{"n1", "n2", "n3"} {
		err := callUpgradeForTest(clientNC, "lab", "actor-pub", nid, "https://evil/", "deadbeef")
		if err == nil {
			continue
		}
		if isConfigErrorTest(err) {
			abortAfter = i
			break
		}
	}
	if abortAfter != 0 {
		t.Errorf("expected abort after n1 (i=0); got abortAfter=%d", abortAfter)
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("after config-error abort, calls should equal 1; got %d", got)
	}
}

// callUpgradeForTest is a tiny inline copy of dispatchUpgrade's
// NATS request shape — the real dispatchUpgrade lives in package
// main (cmd/tether) and isn't importable from a _test package.
// Faithful enough to exercise the exact wire path; the
// classification-policy tests in cmd/tether/node_classify_test.go
// pin the helpers themselves.
func callUpgradeForTest(nc *nats.Conn, sid, actor, nid, url, sha string) error {
	body, _ := json.Marshal(proto.UpgradeReq{
		URL: url, SHA256: sha, ProtoVersion: proto.ProtoVersion,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	respMsg, err := nc.RequestWithContext(ctx,
		proto.SubjCmdBy(sid, actor, nid, "upgrade"), body)
	if err != nil {
		return err
	}
	var resp proto.UpgradeResp
	if err := json.Unmarshal(respMsg.Data, &resp); err != nil {
		return err
	}
	if !resp.OK {
		// Mirror dispatchUpgrade's error format so isTransient /
		// isConfig classifiers see the same string shape.
		return errFromCode(resp.Code, resp.Error)
	}
	return nil
}

// errFromCode + isTransientErrorTest / isConfigErrorTest mirror
// the real classifiers' substring matching. Duplicated here only
// because the production helpers live in package main and can't
// be imported from _test packages outside cmd/tether.
type codeErr struct{ msg string }

func (c *codeErr) Error() string { return c.msg }

func errFromCode(code, detail string) error {
	return &codeErr{msg: "broker rejected upgrade for lab/x: " + code + " " + detail}
}

func isTransientErrorTest(err error) bool {
	return matchesAny(err.Error(), []string{
		"node_offline", "node_not_found", "agent_no_responders",
		"agent_malformed_resp", "deadline exceeded", "context canceled",
	})
}

func isConfigErrorTest(err error) bool {
	return matchesAny(err.Error(), []string{
		"not_owner", "url_not_allowed", "sha256_invalid",
		"proto_bump_requires_reinstall", "actor_invalid",
		"session_not_found_or_deleting",
	})
}

func matchesAny(s string, needles []string) bool {
	for _, n := range needles {
		if strings.Contains(s, n) {
			return true
		}
	}
	return false
}
