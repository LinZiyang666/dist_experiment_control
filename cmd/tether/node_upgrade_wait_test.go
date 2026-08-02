package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
	"github.com/spf13/cobra"
)

// origin: upgrade-safety plan §5 — waitForUpgradeCommit and the --all canary
// gate, driven against stub NATS responders (the policy under test is ctl's,
// not the broker's).

// stubNodeList answers node.list.req with a single-entry roster whose release
// is read from the pointer on every call — tests mutate it to simulate the
// agent re-registering as the new version mid-poll.
func stubNodeList(t *testing.T, nc *nats.Conn, nid string, release *atomic.Value) {
	t.Helper()
	_, err := nc.Subscribe(proto.SubjectPrefix+".ctrl.by.*.s.*.node.list.req", func(msg *nats.Msg) {
		body, _ := json.Marshal(proto.NodeListResp{Nodes: []proto.NodeListEntry{{
			NID: nid, Status: "ONLINE", ReleaseVersion: release.Load().(string),
		}}})
		_ = msg.Respond(body)
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
}

// stubUpgradeOK answers upgrade.req with {OK, NewVersion} and counts calls.
func stubUpgradeOK(t *testing.T, nc *nats.Conn, newVersion string) *atomic.Int32 {
	t.Helper()
	calls := &atomic.Int32{}
	_, err := nc.Subscribe(proto.SubjectPrefix+".s.*.cmd.by.*.node.*.upgrade.req", func(msg *nats.Msg) {
		calls.Add(1)
		body, _ := json.Marshal(proto.UpgradeResp{OK: true, NewVersion: newVersion})
		_ = msg.Respond(body)
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	return calls
}

func waitTestCmd(t *testing.T) (*cobra.Command, *bytes.Buffer) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	var out bytes.Buffer
	cmd := &cobra.Command{}
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetContext(ctx)
	return cmd, &out
}

func TestWaitForUpgradeCommitSeesNewRelease(t *testing.T) {
	url := testharness.StartNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	release := &atomic.Value{}
	release.Store("v0.0.9-new")
	stubNodeList(t, nc, "n1", release)

	cmd, out := waitTestCmd(t)
	if err := waitForUpgradeCommit(cmd, nc, "lab", "actor-pub", "n1", "v0.0.9-new", "v0.0.1-old"); err != nil {
		t.Fatalf("wait should commit: %v (output: %s)", err, out.String())
	}
	if !strings.Contains(out.String(), "COMMITTED") || !strings.Contains(out.String(), "v0.0.9-new") {
		t.Errorf("output missing the COMMITTED verdict: %s", out.String())
	}
}

// origin: internal review S9/S19 — the legacy fallback (newVersion=="",
// pre-upgrade-safety agent) and the true old→new mid-poll transition,
// including the Status!=ONLINE hold: an entry that already shows the new
// release but is not ONLINE yet must NOT be read as committed.
func TestWaitForUpgradeCommitLegacyFallbackSeesReleaseChange(t *testing.T) {
	url := testharness.StartNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	type row struct {
		release string
		status  string
	}
	cur := &atomic.Value{}
	cur.Store(row{"v0.0.1-old", "ONLINE"})
	_, err = nc.Subscribe(proto.SubjectPrefix+".ctrl.by.*.s.*.node.list.req", func(msg *nats.Msg) {
		r := cur.Load().(row)
		body, _ := json.Marshal(proto.NodeListResp{Nodes: []proto.NodeListEntry{{
			NID: "n1", Status: r.status, ReleaseVersion: r.release,
		}}})
		_ = msg.Respond(body)
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	oldPoll := upgradeWaitPoll
	upgradeWaitPoll = 50 * time.Millisecond
	t.Cleanup(func() { upgradeWaitPoll = oldPoll })

	// Walk the node through: old/ONLINE → new/STALE (must hold) → new/ONLINE.
	go func() {
		time.Sleep(120 * time.Millisecond)
		cur.Store(row{"v0.0.2-new", "STALE"})
		time.Sleep(200 * time.Millisecond)
		cur.Store(row{"v0.0.2-new", "ONLINE"})
	}()

	cmd, out := waitTestCmd(t)
	if err := waitForUpgradeCommit(cmd, nc, "lab", "actor-pub", "n1", "", "v0.0.1-old"); err != nil {
		t.Fatalf("legacy fallback should commit on release change: %v (output: %s)", err, out.String())
	}
	if !strings.Contains(out.String(), "release changed") {
		t.Errorf("output missing the fallback verdict: %s", out.String())
	}
}

// origin: internal review S4, hardened by external review F3 — a same-tag
// re-push cannot be told apart from the stale pre-upgrade row, so
// confirmation must FAIL CLOSED with a loud UNCONFIRMED error (never a
// hollow COMMITTED, and never a silent success that would release a fleet).
func TestWaitForUpgradeCommitSameTagFailsClosedAsUnconfirmed(t *testing.T) {
	url := testharness.StartNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	// No node.list stub on purpose: the verdict must decide BEFORE any poll.

	cmd, out := waitTestCmd(t)
	err = waitForUpgradeCommit(cmd, nc, "lab", "actor-pub", "n1", "v0.4.7", "v0.4.7")
	if err == nil || !strings.Contains(err.Error(), "UNCONFIRMED") {
		t.Fatalf("same-tag re-push must fail closed as UNCONFIRMED; got err=%v", err)
	}
	s := out.String()
	if !strings.Contains(s, "⚠") || !strings.Contains(s, "same-tag") {
		t.Errorf("output missing the loud warning: %s", s)
	}
	if strings.Contains(s, "— upgrade COMMITTED") {
		t.Errorf("same-tag re-push must NOT claim COMMITTED: %s", s)
	}
}

func TestWaitForUpgradeCommitReportsLikelyRollback(t *testing.T) {
	url := testharness.StartNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	release := &atomic.Value{}
	release.Store("v0.0.1-old") // never becomes the new release
	stubNodeList(t, nc, "n1", release)

	old := upgradeWaitBudget
	upgradeWaitBudget = 0 // the deadline check runs before the first sleep
	t.Cleanup(func() { upgradeWaitBudget = old })

	cmd, out := waitTestCmd(t)
	err = waitForUpgradeCommit(cmd, nc, "lab", "actor-pub", "n1", "v0.0.9-new", "v0.0.1-old")
	if err == nil {
		t.Fatal("wait past the budget must be an error (non-zero exit)")
	}
	if !strings.Contains(out.String(), "ROLLED BACK") {
		t.Errorf("output missing the likely-ROLLED-BACK verdict: %s", out.String())
	}
	// origin: internal review S6 — the verdict must carry a terminal exit
	// class (64), not fall through to the unclassified 70 that usage.md
	// §9.13 tells automation to retry: retrying re-pushes the same broken
	// artifact through another triple crash-boot and rollback.
	var ee *ExitError
	if !errors.As(err, &ee) || ee.Class != exitUsage {
		t.Errorf("wait-timeout error class = %+v, want ExitError{Class: 64}", err)
	}
}

// origin: internal review S3 — a PRE-upgrade-safety agent fills NewVersion
// with the FULL `tether version` first line, not a bare tag; dispatchUpgrade
// must normalize it away (→ "") so --wait uses the release-change fallback
// instead of an unsatisfiable equality that fake-ROLLED-BACKs every
// first-hop upgrade and aborts the canary fleet.
func TestDispatchUpgradeNormalizesLegacyNewVersion(t *testing.T) {
	url := testharness.StartNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	_, err = nc.Subscribe(proto.SubjectPrefix+".s.*.cmd.by.*.node.*.upgrade.req", func(msg *nats.Msg) {
		body, _ := json.Marshal(proto.UpgradeResp{OK: true, NewVersion: "tether v0.4.7 (proto v2)"})
		_ = msg.Respond(body)
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	cmd, out := waitTestCmd(t)
	got, err := dispatchUpgrade(cmd, nc, "lab", "actor-pub", "n1", "https://x/", "deadbeef", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if got != "" {
		t.Errorf("legacy full-line NewVersion must normalize to \"\"; got %q", got)
	}
	if strings.Contains(out.String(), "staged (→") {
		t.Errorf("legacy reply must not print the tagged staged line: %s", out.String())
	}
}

// The canary gate: a canary that never re-registers as the new release must
// abort the fleet with every other node UNTOUCHED (zero dispatches beyond the
// canary's own).
func TestUpgradeAllCanaryFailureLeavesFleetUntouched(t *testing.T) {
	url := testharness.StartNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	calls := stubUpgradeOK(t, nc, "v0.0.9-new")
	release := &atomic.Value{}
	release.Store("v0.0.1-old") // canary stays on the old release → rollback verdict
	stubNodeList(t, nc, "n1", release)

	old := upgradeWaitBudget
	upgradeWaitBudget = 0
	t.Cleanup(func() { upgradeWaitBudget = old })

	cmd, _ := waitTestCmd(t)
	err = runUpgradeAll(cmd, nc, "lab", "actor-pub", []string{"n1", "n2", "n3"}, "https://x/", "deadbeef", 5*time.Second)
	if err == nil || !strings.Contains(err.Error(), "canary n1 failed, 2 node(s) untouched") {
		t.Fatalf("expected the canary-failed abort; got err=%v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("upgrade dispatches: got %d, want 1 (canary only — the fleet must be untouched)", got)
	}
}

// The happy path: canary commits, the rest are dispatched WITHOUT per-node
// waiting, and the operator is pointed at `node ls` for verification.
func TestUpgradeAllCanarySuccessFansOutRest(t *testing.T) {
	url := testharness.StartNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	calls := stubUpgradeOK(t, nc, "v0.0.9-new")
	release := &atomic.Value{}
	release.Store("v0.0.1-old") // true old→new transition (S19): baseline reads old…
	stubNodeList(t, nc, "n1", release)

	oldPoll := upgradeWaitPoll
	upgradeWaitPoll = 50 * time.Millisecond
	t.Cleanup(func() { upgradeWaitPoll = oldPoll })
	go func() { // …and the canary re-registers as the new release mid-poll
		time.Sleep(150 * time.Millisecond)
		release.Store("v0.0.9-new")
	}()

	cmd, out := waitTestCmd(t)
	if err := runUpgradeAll(cmd, nc, "lab", "actor-pub", []string{"n1", "n2", "n3"}, "https://x/", "deadbeef", 5*time.Second); err != nil {
		t.Fatalf("fleet rollout should succeed: %v (output: %s)", err, out.String())
	}
	if got := calls.Load(); got != 3 {
		t.Errorf("upgrade dispatches: got %d, want 3 (canary + 2 fleet)", got)
	}
	s := out.String()
	if !strings.Contains(s, "canary: n1") || !strings.Contains(s, "COMMITTED") || !strings.Contains(s, "node ls") {
		t.Errorf("output missing canary/COMMITTED/node-ls hints: %s", s)
	}
	// origin: internal review S16 — the staged line's real format is pinned
	// here AND quoted verbatim in usage.md §5.19; drift in either direction
	// breaks operators' greps.
	if !strings.Contains(s, "✔ lab/n1: staged (→ v0.0.9-new, smoke ok); agent re-exec in progress") {
		t.Errorf("staged line drifted from the documented format: %s", s)
	}
}

// origin: upgrade-safety external review F2 — a canary is a health gate, not
// merely an artifact-staging gate. A same-tag re-push is especially important
// for recovery, but the stale node.list row cannot prove that the staged image
// re-execed and registered. The fleet must remain held when that proof is
// unavailable; otherwise a binary that passes `version` and then crashes in
// Agent.Run is dispatched to every node at once.
func TestUpgradeAllSameTagCannotReleaseFleetWithoutCommitProof(t *testing.T) {
	url := testharness.StartNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	calls := stubUpgradeOK(t, nc, "v0.4.7")
	release := &atomic.Value{}
	release.Store("v0.4.7")
	stubNodeList(t, nc, "n1", release)

	cmd, out := waitTestCmd(t)
	err = runUpgradeAll(cmd, nc, "lab", "actor-pub", []string{"n1", "n2", "n3"},
		"https://x/", "deadbeef", 5*time.Second)
	if err == nil {
		t.Fatalf("same-tag canary supplied no re-register proof but released the fleet: %s", out.String())
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("upgrade dispatches = %d, want canary only; same-tag stale state cannot unlock the fleet", got)
	}
}

// origin: upgrade-safety external review F3 — the legacy N-1 fallback is
// meaningful only with a known pre-dispatch release. With an empty baseline,
// any ordinary stale ONLINE row is unequal to "" and therefore cannot be
// treated as evidence that the agent re-execed after this request.
func TestWaitForUpgradeCommitMissingLegacyBaselineCannotCommitStaleRow(t *testing.T) {
	url := testharness.StartNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	release := &atomic.Value{}
	release.Store("v0.4.7")
	stubNodeList(t, nc, "n1", release)

	oldPoll, oldBudget := upgradeWaitPoll, upgradeWaitBudget
	upgradeWaitPoll = 10 * time.Millisecond
	upgradeWaitBudget = 80 * time.Millisecond
	t.Cleanup(func() {
		upgradeWaitPoll = oldPoll
		upgradeWaitBudget = oldBudget
	})

	cmd, out := waitTestCmd(t)
	err = waitForUpgradeCommit(cmd, nc, "lab", "actor-pub", "n1", "", "")
	if err == nil {
		t.Fatalf("missing baseline converted a stale row into COMMITTED: %s", out.String())
	}
	if strings.Contains(out.String(), "COMMITTED") {
		t.Fatalf("missing baseline must never produce a COMMITTED verdict: %s", out.String())
	}
}
