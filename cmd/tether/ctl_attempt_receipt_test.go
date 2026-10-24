//go:build linux

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
	"github.com/spf13/cobra"
	"golang.org/x/sys/unix"
)

// Real: a workspace-built CLI, real NATS, inherited receipt pipe, and the
// production Python collector. The absent cut windows deliberately remain
// INCOMPLETE; this test signs only the actual producer's receipt format.
// origin: hot-update S6 internal review rv-ctl-2 / rv-ctl-8 / plan §8.3
func TestCtlBinaryAttemptReceiptMatchesCollector(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	binary := filepath.Join(t.TempDir(), "tether-test")
	build := exec.CommandContext(t.Context(), "go", "build", "-race", "-o", binary, ".")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build sandbox CLI: %v: %s", err, output)
	}
	for round := 0; round < 5; round++ {
		t.Run(strconv.Itoa(round), func(t *testing.T) { exerciseCtlBinaryReceipt(t, binary) })
	}
	testharness.AssertNoGoroutineLeak(t, "CLI receipt collector", beforeG)
	testharness.AssertNoFDLeak(t, "CLI receipt collector", beforeFD, 0)
}

func exerciseCtlBinaryReceipt(t *testing.T, binary string) {
	t.Helper()
	url := testharness.StartNATS(t)
	home := t.TempDir()
	writeReviewPsIdentity(t, home, "lab")
	id, err := cli.EnsureIdentity(home)
	if err != nil {
		t.Fatal(err)
	}
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	body, err := json.Marshal(proto.NodeListResp{Nodes: []proto.NodeListEntry{{NID: "node", Status: "ONLINE"}}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = nc.Subscribe(proto.SubjCtrlNodeList(id.PublicKey, "lab"), func(msg *nats.Msg) { _ = msg.Respond(body) })
	if err != nil {
		t.Fatal(err)
	}
	if err = nc.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close(); _ = w.Close() }()
	const nonce = "abcdef0123456789abcdef0123456789"
	logicalID := nonce + "-0"
	command := exec.CommandContext(t.Context(), binary, "node", "ls", "--json", "--home", home, "--nats-url", url)
	command.ExtraFiles = []*os.File{w}
	command.Env = append(os.Environ(), "TETHER_HOME="+home, cli.DevNoAuthEnv+"=1", cli.AttemptReceiptFDEnv+"=3", cli.AttemptLogicalIDEnv+"="+logicalID)
	started, err := controlbudget.Now()
	if err != nil {
		t.Fatal(err)
	}
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("sandbox node ls: %v: %s", err, output)
	}
	finished, err := controlbudget.Now()
	if err != nil {
		t.Fatal(err)
	}
	_ = w.Close()
	receipts, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	evidence := t.TempDir()
	if err = os.WriteFile(filepath.Join(evidence, "attempts.jsonl"), receipts, 0o600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(evidence, "stdout.json"), output, 0o600); err != nil {
		t.Fatal(err)
	}
	collector, err := filepath.Abs("../../test/simcluster/drills/lib/hot_update_broker_probe.py")
	if err != nil {
		t.Fatal(err)
	}
	// This is the actual collector's judge, including its exact field set,
	// slot identity, sent-request, ordinal, final-result and close census guards.
	script := `import importlib.util,json,pathlib,sys
spec=importlib.util.spec_from_file_location("collector",sys.argv[1]); c=importlib.util.module_from_spec(spec); spec.loader.exec_module(c)
root=pathlib.Path(sys.argv[2]); start,end=map(int,sys.argv[3:5]); nonce=sys.argv[5]; logical=nonce+"-0"
def rows(name,values): (root/(name+".jsonl")).write_text("".join(json.dumps(row)+"\n" for row in values))
c.publish(root/"run.json",dict(schema=c.SCHEMA,nonce=nonce,rate=10,request_ns=c.REQUEST_NS,started_ns=start,scheduling_ended_ns=start+100000000,ended_ns=max(end,start+100000000+c.REQUEST_NS),expected_slots=1,late_tail_ns=c.REQUEST_NS,complete=True,late_tail_complete=True,hops=["absent-cut"]))
rows("slots",[dict(logical_id=logical,slot=0,scheduled_ns=start,**{"class":"read"})]); rows("results",[dict(logical_id=logical,started_ns=start,finished_ns=end,rc=0,validated_read=c.validated_read(json.loads((root/"stdout.json").read_text()),"nodes"))])
rows("effects",[]); rows("windows",[]); result=c.control_judge(root)
assert result["rc"]==4 and not result["failures"],result
print(json.dumps(result))`
	judge := exec.CommandContext(t.Context(), "python3", "-I", "-B", "-c", script, collector, evidence, strconv.FormatInt(started, 10), strconv.FormatInt(finished, 10), nonce)
	if output, err := judge.CombinedOutput(); err != nil {
		t.Fatalf("real CLI rejected by collector: %v: %s", err, output)
	}
}

// receiptPipe hands the CLI a dedicated receipt FD exactly as the drill
// collector does (a pipe write end passed by number) and returns the read end.
func receiptPipe(t *testing.T) *os.File {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	// The sink adopts the FD number; hand it a private duplicate so the test's
	// *os.File never closes a descriptor number the sink already owns.
	fd, err := unix.Dup(int(w.Fd()))
	if err != nil {
		t.Fatal(err)
	}
	_ = w.Close()
	t.Setenv(cli.AttemptReceiptFDEnv, strconv.Itoa(fd))
	return r
}

func readReceiptRows(t *testing.T, r *os.File) []map[string]any {
	t.Helper()
	if err := r.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	raw, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("receipt stream not closed by the CLI: %v", err)
	}
	var rows []map[string]any
	for _, line := range strings.Split(strings.TrimSuffix(string(raw), "\n"), "\n") {
		if line == "" {
			continue
		}
		var row map[string]any
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			t.Fatalf("malformed receipt line %q: %v", line, err)
		}
		rows = append(rows, row)
	}
	return rows
}

// The production receipt channel must satisfy the ONE broker collector that
// judges it (test/simcluster/drills/lib/hot_update_broker_probe.py
// control_judge): exactly one terminal "result" attempt per logical ID, and a
// terminal observer_close record carrying attempt_count/overflow. The P29
// selftest satisfies both only with a fake Python CLI (CHILD), never with
// this binary.
// origin: hot-update S6 internal review rv-ctl
// origin: S7 internal review MAKETEST-TRANSFER F1
func TestNodeLsAttemptReceiptMatchesBrokerCollector(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(strconv.Itoa(round), func(t *testing.T) { exerciseNodeLsReceipt(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "node ls receipt collector", beforeG)
	testharness.AssertNoFDLeak(t, "node ls receipt collector", beforeFD, 0)
}

func exerciseNodeLsReceipt(t *testing.T) {
	t.Helper()
	url := testharness.StartNATS(t)
	home := t.TempDir()
	writeReviewPsIdentity(t, home, "lab")
	id, err := cli.EnsureIdentity(home)
	if err != nil {
		t.Fatal(err)
	}
	responder, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer responder.Close()
	body, err := json.Marshal(proto.NodeListResp{Nodes: []proto.NodeListEntry{{NID: "n1", Status: "ONLINE"}}})
	if err != nil {
		t.Fatal(err)
	}
	sub, err := responder.Subscribe(proto.SubjCtrlNodeList(id.PublicKey, "lab"), func(m *nats.Msg) { _ = m.Respond(body) })
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := responder.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	t.Setenv(cli.DevNoAuthEnv, "1")
	t.Setenv("TETHER_HOME", home)
	t.Setenv(cli.AttemptLogicalIDEnv, "collector-slot-0")
	r := receiptPipe(t)
	stdout, stderr, err := runRoot(t, "node", "ls", "--json", "--home", home, "--nats-url", url)
	if err != nil {
		t.Fatalf("node ls failed: err=%v stderr=%q", err, stderr)
	}
	if !strings.Contains(stdout, `"n1"`) {
		t.Fatalf("node ls did not print the validated read: %q", stdout)
	}
	rows := readReceiptRows(t, r)
	ids := map[string]bool{}
	phases := map[string]int{}
	closes := 0
	for _, row := range rows {
		if row["logical_id"] != "collector-slot-0" {
			t.Errorf("collector slot identity changed: %v", row["logical_id"])
		}
		if row["kind"] == "observer_close" {
			closes++
			overflow, ok := row["overflow"].(bool)
			if row["attempt_count"] != float64(len(rows)-1) || !ok || overflow {
				t.Errorf("observer census does not match attempts: %v", row)
			}
			continue
		}
		ids[row["logical_id"].(string)] = true
		phases[row["phase"].(string)]++
	}
	t.Logf("receipt rows=%d phases=%v logical_ids=%d", len(rows), phases, len(ids))
	if len(ids) != 1 {
		t.Errorf("one RunE emitted %d logical identities", len(ids))
	}
	if phases["result"] != 1 {
		t.Errorf("collector requires exactly one terminal result attempt per logical ID; got %d (DoPhaseWithin finishRetry + finishCtlAdmission)", phases["result"])
	}
	if phases["connect"] != 1 || phases["request"] != 1 {
		t.Errorf("one transport attempt has multiple receipt owners: %v", phases)
	}
	if closes != 1 {
		t.Errorf("collector requires a terminal observer_close record (attempt_count/overflow); got %d", closes)
	}
}

// exec/run call finishCtlAdmission (which writes the terminal result attempt
// and Closes the sink) only after the remote task settles, and a sink error
// replaces the command's result. A task that legitimately outlives the 5s
// admission window must keep its success; admission ends at true start.
// The ctx deadline below only shortens the admission window under test; the
// same code path runs for every reliable exec longer than five seconds.
// origin: hot-update S6 internal review rv-ctl
func TestFinishAdmissionAfterLongTaskPreservesSuccess(t *testing.T) {
	r := receiptPipe(t)
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	cmd := &cobra.Command{}
	cmd.SetContext(ctx)
	a, err := beginCtlAdmission(cmd)
	if err != nil {
		t.Fatal(err)
	}
	if a.observer == nil {
		t.Fatal("receipt FD did not install an observer")
	}
	// The task truly started inside admission and keeps running past it.
	<-ctx.Done()
	time.Sleep(100 * time.Millisecond)
	var result error
	finishCtlAdmission(a, &result)
	rows := readReceiptRows(t, r)
	if result != nil {
		t.Fatalf("successful long task reported failure after admission: %v (rows=%d)", result, len(rows))
	}
}

// Plan §8.2: an eligible CONNECT whose auth refusal cannot be told apart from
// a blackout may retry inside the same absolute deadline, but exhaustion must
// keep the original auth error and must not be rewritten as a plain timeout.
// The final attempt here is truncated by the shared deadline itself.
// origin: hot-update S6 internal review rv-ctl
func TestConnectRetryExhaustionPreservesAuthRefusal(t *testing.T) {
	deadline := ctlTestDeadline(t, 1200*time.Millisecond)
	calls := 0
	_, err := connectWithAuthRetryWithin(t.Context(), deadline, "auth-exhaustion", nil, true,
		func(d controlbudget.Deadline) (*nats.Conn, error) {
			calls++
			remaining, rerr := controlbudget.Remaining(d)
			if rerr != nil {
				return nil, rerr
			}
			if remaining < 600*time.Millisecond {
				// The callout reply needs ~600ms (slow RTT/proxy); the shared
				// deadline cuts the socket first, exactly like nats.Timeout.
				time.Sleep(remaining)
				return nil, &net.OpError{Op: "read", Net: "tcp", Err: os.ErrDeadlineExceeded}
			}
			time.Sleep(600 * time.Millisecond)
			return nil, nats.ErrAuthorization
		}, nil)
	if calls < 2 {
		t.Fatalf("eligible refusal was not retried: calls=%d err=%v", calls, err)
	}
	if !errors.Is(err, nats.ErrAuthorization) {
		t.Fatalf("exhaustion rewrote the auth refusal as %v after %d attempts", err, calls)
	}
}

// origin: hot-update S6 internal review rv-ctl-5
func TestCtlSettledStatusSurvivesReceiptFailure(t *testing.T) {
	var diagnostics bytes.Buffer
	sinkErr := errors.New("collector closed")
	observer := &ctlTestObserver{beginErr: sinkErr}
	a := &ctlAdmission{logicalID: "settled", observer: observer, settled: true, diagnostics: &diagnostics}
	var result error
	finishCtlAdmission(a, &result)
	if result != nil || observer.closes != 1 || !strings.Contains(diagnostics.String(), sinkErr.Error()) {
		t.Fatalf("receipt failure replaced or hid a settled status: result=%v closes=%d diagnostics=%q", result, observer.closes, diagnostics.String())
	}
}

// Network admission must finish while the operation can still report its final
// result; a request timeout at the outer deadline leaves that receipt late.
// origin: hot-update S6 plan §8.2 / T30 / EXITS-2-3
func TestCtlAdmissionLeavesTimeForFinalReceipt(t *testing.T) {
	t.Setenv(cli.AttemptReceiptFDEnv, "")
	t.Setenv(cli.AttemptLogicalIDEnv, "")
	if err := os.Unsetenv(cli.AttemptReceiptFDEnv); err != nil {
		t.Fatal(err)
	}
	if err := os.Unsetenv(cli.AttemptLogicalIDEnv); err != nil {
		t.Fatal(err)
	}
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseCtlResultReserve(t)
	}
	testharness.AssertNoGoroutineLeak(t, "control result reserve x5", beforeG)
	testharness.AssertNoFDLeak(t, "control result reserve x5", beforeFD, 0)
}
func exerciseCtlResultReserve(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	cmd := &cobra.Command{}
	cmd.SetContext(ctx)
	admission, err := beginCtlAdmission(cmd)
	if err != nil {
		t.Fatal(err)
	}
	phase, cancelPhase, err := ctlAdmissionContext(ctx, admission.deadline)
	if err != nil {
		t.Fatal(err)
	}
	defer cancelPhase()
	<-phase.Done()
	outer, _ := ctx.Deadline()
	if time.Until(outer) < cli.ControlResultReserve/2 {
		t.Fatal("network expiry consumed the final-result reporting window")
	}
	var result error
	finishCtlAdmission(admission, &result)
	if result != nil || ctx.Err() != nil {
		t.Fatalf("final receipt outlived operation: result=%v context=%v", result, ctx.Err())
	}
}
