//go:build linux

package cli_e2e_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/test/transferharness"
)

// Decode the real CLI's public projection without supplying missing fields.
// Unknown is affirmative census evidence; a refusal is never an empty success.
// origin: hot-update S7 plan §6.1 / §7 / P33
func validateTransferStatus(body, sid, nid, tid string) (proto.TransferStatusResp, error) {
	var status proto.TransferStatusResp
	decoder := json.NewDecoder(strings.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&status); err != nil {
		return status, err
	}
	if err := decoder.Decode(new(json.RawMessage)); err != io.EOF {
		return status, errors.New("status contains more than one result")
	}
	if status.Identity.SID != sid || status.Identity.NID != nid || status.Identity.TransferID != tid ||
		status.Origin.BrokerID == "" || status.Origin.InstanceID == "" || status.Origin.BootID == "" || status.Code != "" {
		return status, errors.New("status lacks original authoritative scope")
	}
	switch status.State {
	case proto.StatusUnknown:
		if status.Terminal != nil || status.Phase != "" || status.Identity.EntryNonce != "" {
			return status, errors.New("unknown status carries a retained outcome")
		}
	case proto.StatusLive:
		if status.Phase == "" || status.Terminal != nil || status.Identity.EntryNonce == "" || status.Budget.StartedAt.IsZero() {
			return status, errors.New("live status lacks retained responsibility")
		}
	case proto.StatusTerminal:
		r := status.Terminal
		if r == nil || r.HistorySeq == 0 || r.HistoryStream == "" || r.TerminalID == "" || len(r.CanonicalDigest) != 64 ||
			(r.Kind != "complete" && r.Kind != "failed") || (r.OutcomeUnknown && r.Kind != "failed") {
			return status, errors.New("terminal status lacks an exact truthful receipt")
		}
	default:
		return status, errors.New("status state is not in the frozen vocabulary")
	}
	return status, nil
}

func (f *transferStack) status(p *transferPeer, tid string) proto.TransferStatusResp {
	f.t.Helper()
	r, stdout, stderr := transferNative(f.ctx, f.t, f.command(p, "transfer", "status", tid, "--node", p.nid, "--json"))
	if r.NativeRC != 0 {
		f.t.Fatalf("transfer status native_rc=%d: %s", r.NativeRC, stderr)
	}
	status, err := validateTransferStatus(stdout, p.sid, p.nid, tid)
	if err != nil {
		f.t.Fatalf("invalid status: %v: %s", err, stdout)
	}
	return status
}

func (f *transferStack) requireCapable(p *transferPeer) {
	f.t.Helper()
	r, stdout, stderr := transferNative(f.ctx, f.t, f.command(p, "transfer", "status", "never-created", "--node", p.nid, "--json"))
	status, err := validateTransferStatus(stdout, p.sid, p.nid, "never-created")
	if r.NativeRC != 0 || err != nil || status.State != proto.StatusUnknown {
		f.t.Fatalf("NOT_RUN pending P22/P24 production activation and P25/P26 binding: original-origin census unavailable: rc=%d stdout=%q stderr=%q decode=%v", r.NativeRC, stdout, stderr, err)
	}
}

// A fresh CLI query must not acquire creation authority or mutate the bucket.
// Test both retained live and exact terminal states, then remove the responder:
// silence cannot become authoritative unknown or trigger a legacy attempt.
// origin: hot-update S7 plan §6.1 / §7 / P33
func TestTransferBinaryStatusIsReadOnly(t *testing.T) {
	// origin: hot-update S10 plan §11.1 / TEST-BUDGET
	if runIsolatedBinaryTest(t, binaryFixtureTestBudget) {
		return
	}
	buildTether(t)
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseTransferBinaryStatus(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "transfer status", g)
	testharness.AssertNoFDLeak(t, "transfer status", fd, 0)
}

func exerciseTransferBinaryStatus(t *testing.T) {
	f := newTransferStack(t, buildTether(t), buildTether(t), false, "status")
	p := f.peers[0]
	f.requireCapable(p)
	before := f.history(p)
	baseWire := len(f.rows())
	unknown := f.status(p, "absent")
	if unknown.State != proto.StatusUnknown || !reflect.DeepEqual(before, f.history(p)) {
		t.Fatal("read-only unknown query changed history or fabricated retained state")
	}
	rejectTransferCreation(t, f.rows()[baseWire:])
	src, digest := transferPayload(t, f.h.Root, "source", 9<<20)
	dst := filepath.Join(f.h.Root, "destination")
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	relay, url := startNATSReplyFence(t, f.url, func(subject string, _ []byte) bool {
		return subject == proto.SubjCmdBy(p.sid, p.id.PublicKey, p.nid, "push")
	}, release, false)
	ctl := f.transfer(p, f.bin, url, "push", src, dst)
	if !testharnessWait(5*time.Second, func() bool { return len(relay.receipts()) > 0 }) {
		t.Fatal("no retained prepare to query")
	}
	var req proto.PushPrepareReq
	if err := json.Unmarshal(relay.requireLoss(t).Request, &req); err != nil || req.Continuation == nil {
		t.Fatal("prepare is not capable", err)
	}
	baseWire = len(f.rows())
	live := f.status(p, req.TransferID)
	if live.State != proto.StatusLive || live.Identity != req.Continuation.Identity {
		t.Fatalf("status replaced original entry: %+v", live)
	}
	rejectTransferCreation(t, f.rows()[baseWire:])
	unblock()
	r, stdout, stderr := transferNative(f.ctx, t, ctl)
	if r.NativeRC != 0 || !strings.Contains(stdout, "kind=complete") {
		t.Fatalf("transfer failed: rc=%d %s %s", r.NativeRC, stdout, stderr)
	}
	terminal := f.status(p, req.TransferID)
	if terminal.State != proto.StatusTerminal || terminal.Terminal == nil || terminal.Terminal.SHA256 != digest {
		t.Fatalf("terminal query lost exact outcome: %+v", terminal)
	}
	if err := checkTransferHistory(f.history(p), terminal); err != nil {
		t.Fatal(err)
	}
	r, human, stderr := transferNative(f.ctx, t, f.command(p, "transfer", "status", req.TransferID, "--node", p.nid))
	for _, field := range []string{"transfer_id=" + req.TransferID, "state=terminal", "kind=complete", "outcome_unknown=false"} {
		if r.NativeRC != 0 || !strings.Contains(human, field) {
			t.Fatalf("human/JSON disagreement: missing %s: rc=%d %s %s", field, r.NativeRC, human, stderr)
		}
	}
	if err := f.broker.Stop(); err != nil {
		t.Fatal(err)
	}
	baseWire = len(f.rows())
	r, stdout, stderr = transferNative(f.ctx, t, f.command(p, "transfer", "status", req.TransferID, "--node", p.nid, "--json"))
	if (r.NativeRC != 74 && r.NativeRC != 75) || strings.Contains(stdout, `"state":"unknown"`) || strings.Contains(stdout, "complete") {
		t.Fatalf("unavailable owner became a successful status: rc=%d %s %s", r.NativeRC, stdout, stderr)
	}
	rejectTransferCreation(t, f.rows()[baseWire:])
}

func rejectTransferCreation(t *testing.T, rows []transferWireRow) {
	t.Helper()
	for _, row := range rows {
		if strings.HasSuffix(row.Subject, ".permit.req") || strings.HasSuffix(row.Subject, ".push.req") || strings.HasSuffix(row.Subject, ".pull.req") || strings.HasSuffix(row.Subject, ".reserve.req") {
			t.Fatalf("read-only query emitted mutation %s", row.Subject)
		}
	}
}

// These checks need no activated transfer graph. An unreachable endpoint makes
// validation ordering observable: malformed scope/rate must still return rc64.
// origin: hot-update S7 plan §7 / lead LQ2 / P33
func TestTransferBinaryStatusRejectsInvalidScopeAndRate(t *testing.T) {
	// origin: hot-update S10 plan §11.1 / TEST-BUDGET
	if runIsolatedBinaryTest(t, binaryFixtureTestBudget) {
		return
	}
	buildTether(t)
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseTransferBinaryValidation(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "transfer argument validation", g)
	testharness.AssertNoFDLeak(t, "transfer argument validation", fd, 0)
}

func exerciseTransferBinaryValidation(t *testing.T) {
	h := transferharness.New(t, transferSupervisor{})
	f := &transferStack{t: t, h: h, ctx: t.Context(), bin: copyTransferArtifact(t, h.Root, "ctl", buildTether(t)), url: "nats://127.0.0.1:1"}
	p := &transferPeer{home: filepath.Join(h.Root, "home")}
	if err := os.MkdirAll(p.home, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(p.home, "current_session"), []byte("validation\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{
		{"transfer", "status", "tid"},
		{"transfer", "status", "tid", "--node", "*"},
		{"transfer", "status", "bad.id", "--node", "agt1"},
		{"transfer", "status", "tid", "extra", "--node", "agt1"},
		{"push", "source", "agt1:/destination", "--rate-limit", "-1"},
		{"pull", "agt1:/source", "destination", "--rate-limit", "-1"},
	} {
		r, stdout, stderr := transferNative(f.ctx, t, f.command(p, args...))
		if r.NativeRC != 64 || strings.TrimSpace(stdout) != "" || stderr == "" {
			t.Fatalf("invalid arguments did not fail before connection: %v rc=%d stdout=%q stderr=%q", args, r.NativeRC, stdout, stderr)
		}
		if _, err := os.Stat(filepath.Join(p.home, "keys", "default.nk")); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("invalid arguments reached identity creation: %v", err)
		}
	}
}

// The real binary must reject an impossible production rate before START or
// object bytes. A parser-only negative test cannot prove the persisted budget.
// origin: hot-update S7 plan §1.2 / lead LQ2 / P33
func TestTransferBinaryRateBudgetRejectsBeforeCreate(t *testing.T) {
	// origin: hot-update S10 plan §11.1 / TEST-BUDGET
	if runIsolatedBinaryTest(t, binaryFixtureTestBudget) {
		return
	}
	buildTether(t)
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseTransferBinaryRateBudget(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "transfer rate budget", g)
	testharness.AssertNoFDLeak(t, "transfer rate budget", fd, 0)
}

func exerciseTransferBinaryRateBudget(t *testing.T) {
	f := newTransferStack(t, buildTether(t), buildTether(t), false, "rate")
	p := f.peers[0]
	f.requireCapable(p)
	before := f.history(p)
	baseWire := len(f.rows())
	src, _ := transferPayload(t, f.h.Root, "source", 9<<20)
	dst := filepath.Join(f.h.Root, "destination")
	r, stdout, stderr := transferNative(f.ctx, t, f.transfer(p, f.bin, f.url, "push", src, dst, "--rate-limit", "1"))
	if r.NativeRC != 64 || !strings.Contains(stderr, "transfer_budget_exceeded") || strings.Contains(stdout, "complete") {
		t.Fatalf("impossible rate was not refused: rc=%d %s %s", r.NativeRC, stdout, stderr)
	}
	if _, err := os.Stat(dst); !errors.Is(err, os.ErrNotExist) || !reflect.DeepEqual(before, f.history(p)) {
		t.Fatal("budget refusal created target or history")
	}
	rejectTransferCreation(t, f.rows()[baseWire:])
}

// Controls exercise the same reader used for live JSON; synthetic bytes here
// certify the oracle only, never substitute for a binary continuation receipt.
// origin: hot-update S7 plan §6.1 / §7 / P33
func TestTransferBinaryStatusEvidenceControls(t *testing.T) {
	good := proto.TransferStatusResp{State: proto.StatusUnknown,
		Identity: proto.Identity{SID: "session", NID: "node", TransferID: "tid"},
		Origin:   proto.Origin{BrokerID: "broker", InstanceID: "instance", BootID: "boot"}}
	marshal := func(s proto.TransferStatusResp) string {
		b, err := json.Marshal(s)
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	if _, err := validateTransferStatus(marshal(good), "session", "node", "tid"); err != nil {
		t.Fatal("positive status control", err)
	}
	for _, name := range []string{"wrong-id", "missing-origin", "refusal-as-unknown", "missing-state", "live-without-entry", "complete-without-proof", "abandon-as-complete", "trailing-result"} {
		t.Run(name, func(t *testing.T) {
			bad := good
			switch name {
			case "wrong-id":
				bad.Identity.TransferID = "replacement"
			case "missing-origin":
				bad.Origin = proto.Origin{}
			case "refusal-as-unknown":
				bad.Code = "transfer_owner_recovering"
			case "missing-state":
				bad.State = ""
			case "live-without-entry":
				bad.State, bad.Phase = proto.StatusLive, proto.PhasePrepared
			case "complete-without-proof":
				bad.State, bad.Terminal = proto.StatusTerminal, &proto.TerminalReceipt{Kind: "complete"}
			case "abandon-as-complete":
				bad.State, bad.Terminal = proto.StatusTerminal, &proto.TerminalReceipt{Kind: "complete", OutcomeUnknown: true, TerminalID: "terminal", HistoryStream: "history-session", HistorySeq: 2, CanonicalDigest: strings.Repeat("a", 64)}
			}
			body := marshal(bad)
			if name == "trailing-result" {
				body += "\n{}"
			}
			if _, err := validateTransferStatus(body, "session", "node", "tid"); err == nil {
				t.Fatal("status oracle accepted", name)
			}
		})
	}
	// Abandonment is a real failed outcome-unknown projection, never an invalid
	// enum that a reader may silently replace with an ordinary complete result.
	good.State = proto.StatusTerminal
	good.Terminal = &proto.TerminalReceipt{Kind: "failed", Code: "settlement_abandoned", OutcomeUnknown: true,
		TerminalID: "terminal", HistoryStream: "history-session", HistorySeq: 2, CanonicalDigest: strings.Repeat("b", 64)}
	if _, err := validateTransferStatus(marshal(good), "session", "node", "tid"); err != nil {
		t.Fatal("valid abandonment projection refused", err)
	}
	if bytes.Count([]byte(marshal(good)), []byte(`"outcome_unknown":true`)) != 1 {
		t.Fatal("abandonment projection lost its outcome-unknown flag")
	}
}
