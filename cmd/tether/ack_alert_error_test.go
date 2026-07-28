package main

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
)

func startD8ReviewNATS(t *testing.T) string {
	t.Helper()
	opts := natstest.DefaultTestOptions
	opts.Port = -1
	ns := natstest.RunServer(&opts)
	t.Cleanup(func() {
		ns.Shutdown()
		ns.WaitForShutdown()
	})
	if !ns.ReadyForConnections(2 * time.Second) {
		t.Fatal("embedded nats-server not ready")
	}
	return ns.ClientURL()
}

// origin: d8_external_review_test.go (renamed in B6) — docs/reviews/d8-external-review.md
//
// TestD8ReviewAckAlertErrorReplyIsCommandError pins the explicit `tether alert ack`
// command contract: an error response from the broker must make the command fail, not exit
// zero after printing "error: ...". The always-on banner can be best-effort; an explicit ack
// command cannot silently succeed when no leader accepted the ack.
func TestD8ReviewAckAlertErrorReplyIsCommandError(t *testing.T) {
	url := startD8ReviewNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	const actor = "UACTOR"
	subj := proto.SubjCtrlAlertAck(actor)
	sub, err := nc.Subscribe(subj, func(msg *nats.Msg) {
		_ = msg.Respond([]byte("error: no leader"))
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	reply, err := ackAlert(nc, actor, "below_quorum")
	if err == nil {
		t.Fatalf("ackAlert returned nil error for broker error reply %q", reply)
	}
}

// TestD8ReviewAlertLsStrictFetchErrorsOnQueryFailure pins the split between the
// best-effort banner fetch and the explicit `tether alert ls` command: the explicit command
// must fail when the alert store cannot be queried instead of rendering "no active alerts".
func TestD8ReviewAlertLsStrictFetchErrorsOnQueryFailure(t *testing.T) {
	url := startD8ReviewNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	const actor = "UACTOR"
	if alerts, err := fetchAlertsStrict(nc, actor); err == nil {
		t.Fatalf("fetchAlertsStrict returned nil error with no responder; alerts=%v", alerts)
	}

	subj := proto.SubjCtrlAlertLs(actor)
	sub, err := nc.Subscribe(subj, func(msg *nats.Msg) {
		_ = msg.Respond([]byte("not-json"))
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	if alerts, err := fetchAlertsStrict(nc, actor); err == nil {
		t.Fatalf("fetchAlertsStrict returned nil error for malformed reply; alerts=%v", alerts)
	}
}

// TestD8ReviewDestructiveGateCoverage pins the D8b destructive-gate contract from
// docs/distributed-broker-architecture.md §10.4 and docs/reviews/d8-plan.md: the
// severe alert gate is not limited to session rm; push/pull and tunnel/process
// writes must also be guarded before they publish mutating requests.
func TestD8ReviewDestructiveGateCoverage(t *testing.T) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	dir := filepath.Dir(file)
	cases := []struct {
		name string
		file string
	}{
		{name: "session rm", file: "session.go"},
		{name: "push/pull", file: "transfer.go"},
		{name: "expose/expose rm", file: "expose.go"},
		{name: "run/kill", file: "run.go"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body, err := os.ReadFile(filepath.Join(dir, tc.file))
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(body), "gateDestructive(") {
				t.Fatalf("%s has no gateDestructive call", tc.file)
			}
		})
	}
}
