package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/spf13/cobra"
)

// Presentation and argument forwarding are tested independently of byte work.
// The dependency arm requires the real composition; no test double can turn
// missing P25/P26 implementations into a claimed successful transfer.
// origin: hot-update S7 plan §11 T34 / §12.2 P27
// origin: S7 internal review TRANSFER-MAKETEST F2
func TestTransferCommandStatusRateAndExitCodes(t *testing.T) {
	t.Run("surface", func(t *testing.T) {
		root := newRootCmd()
		status, args, err := root.Find([]string{"transfer", "status"})
		if err != nil || len(args) != 0 || status.CommandPath() != "tether transfer status" {
			t.Fatalf("missing status command: args=%v err=%v", args, err)
		}
		for _, name := range []string{"node", "json", "home", "nats-url"} {
			if flag := status.Flags().Lookup(name); flag == nil || flag.Hidden {
				t.Fatalf("status lacks visible --%s", name)
			}
		}
		for _, verb := range []string{"push", "pull"} {
			cmd, _, err := root.Find([]string{verb})
			if err != nil || cmd.Flags().Lookup("rate-limit") == nil {
				t.Fatalf("%s lacks --rate-limit: %v", verb, err)
			}
			if flag := cmd.Flags().Lookup("rate-limit"); flag.DefValue != "0" || flag.Value.Type() != "int64" || flag.Hidden {
				t.Fatalf("%s rate default/type/visibility: %+v", verb, flag)
			}
		}
		rm, _, err := root.Find([]string{"session", "rm"})
		if err != nil || rm.Flags().Lookup("abandon-unsettled-transfers") == nil {
			t.Fatalf("session rm lacks explicit abandonment: %v", err)
		}
		checkCommandTreeGolden(t, commandTreeGolden, dumpCommandTree(root))
		root.InitDefaultCompletionCmd()
		checkCommandTreeGolden(t, commandTreeGoldenRuntime, dumpCommandTree(root))
	})
	t.Run("arguments", func(t *testing.T) {
		for _, args := range [][]string{{"status", "id"}, {"status", "id", "--node", "*"}, {"status", "id.bad", "--node", "node"}, {"status", "id", "extra", "--node", "node"}} {
			cmd := newTransferCmd(transferCommandPorts{})
			cmd.SetArgs(append(args, "--home", t.TempDir()))
			cmd.SetOut(&bytes.Buffer{})
			cmd.SetErr(&bytes.Buffer{})
			if err := cmd.Execute(); classifyExit(err) != exitUsage {
				t.Fatalf("args=%v: err=%v class=%d", args, err, classifyExit(err))
			}
		}
		for _, constructor := range []func() *cobra.Command{newPushCmd, newPullCmd} {
			cmd := constructor()
			args := []string{"local", "node:/remote"}
			if cmd.Name() == "pull" {
				args = []string{"node:/remote", "local"}
			}
			cmd.SetArgs(append(args, "--rate-limit=-1", "--home", t.TempDir()))
			cmd.SetOut(&bytes.Buffer{})
			cmd.SetErr(&bytes.Buffer{})
			if err := cmd.Execute(); classifyExit(err) != exitUsage || !strings.Contains(err.Error(), "rate-limit") {
				t.Fatalf("%s negative rate: %v", cmd.Name(), err)
			}
		}
	})
	t.Run("exit_codes", func(t *testing.T) {
		// Literal expected classes keep the table independent of the production
		// map; adding or removing a code cannot silently change this obligation.
		classes := map[string]int{
			xfercontract.CodeIntentConflict: 64, xfercontract.CodePermitExpired: 64,
			xfercontract.CodeOwnerRecovering: 75, xfercontract.CodeResultUnknown: 74,
			xfercontract.CodeObjectConflict: 64, xfercontract.CodeObjectVersionMismatch: 64,
			xfercontract.CodeSourceChanged: 64, xfercontract.CodeStorageHorizon: 75,
			xfercontract.CodePayloadUnsupported: 64, xfercontract.CodeCapacityExceeded: 75,
			xfercontract.CodeTerminalConflict: 74, xfercontract.CodeCommitInflight: 74,
			xfercontract.CodeDurabilityUnknown: 74, xfercontract.CodeReconcileRequired: 74,
			xfercontract.CodeBudgetExceeded: 75, xfercontract.CodeCanceled: 64,
			xfercontract.CodeNotMember: 77, xfercontract.CodeNotOwner: 77,
			xfercontract.CodeUnknown: 64, xfercontract.CodeSessionDeleting: 75,
		}
		for code, want := range classes {
			err := transferRefusalErr(code, "code=%s detail remains", code)
			if classifyExit(err) != want || wireCodeOf(err) != code || !strings.Contains(err.Error(), "detail remains") {
				t.Errorf("code=%s: class=%d wire=%q error=%v want=%d", code, classifyExit(err), wireCodeOf(err), err, want)
			}
		}
		if got := classifyExit(transferRefusalErr("future_code", "future")); got != 70 {
			t.Fatalf("unknown refusal class=%d", got)
		}
		for _, class := range []int{0, 1, 64, 69, 70, 74, 75, 77, 999} {
			ports := transferCommandPorts{Classify: func(error) (string, int) { return "code", class }}
			err := transferLeafError(ports, errors.New("result unresolved"))
			want := class
			if class == 0 || class == 1 || class == 999 {
				want = 74
			}
			if classifyExit(err) != want || wireCodeOf(err) != "code" {
				t.Fatalf("leaf class %d: %v class=%d", class, err, classifyExit(err))
			}
		}
	})
	t.Run("session_removal", func(t *testing.T) {
		for _, abandon := range []bool{false, true} {
			body, err := sessionRemovalBody(abandon)
			if err != nil {
				t.Fatal(err)
			}
			want := "{}"
			if abandon {
				want = `{"abandon_unsettled_transfers":true}`
			}
			if string(body) != want {
				t.Fatalf("abandon=%t: body=%s want=%s", abandon, body, want)
			}
		}
		for _, code := range []string{xfercontract.CodeSessionDeleting, xfercontract.CodeCommitInflight} {
			err := sessionRemovalRefusal(code, "pending transfer IDs: a (commit_reserved), b (terminal_pending)")
			if classifyExit(err) != 75 || !strings.Contains(err.Error(), "a (commit_reserved), b (terminal_pending)") || wireCodeOf(err) != code {
				t.Fatalf("settlement refusal lost details/class: %v class=%d", err, classifyExit(err))
			}
		}
	})
	t.Run("presentation", func(t *testing.T) {
		beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
		for round := 0; round < 5; round++ {
			exerciseTransferCommand(t)
		}
		testharness.AssertNoGoroutineLeak(t, "transfer CLI presentation", beforeG)
		testharness.AssertNoFDLeak(t, "transfer CLI presentation", beforeFD, 0)
	})
	t.Run("production_composition", func(t *testing.T) {
		if transferCLI.Open == nil || transferCLI.Client == nil || transferCLI.Classify == nil || transferCLI.Receiver == nil {
			t.Fatal("NOT_RUN pending P25/P26/P05: real production factory composition is absent")
		}
	})
}

func exerciseTransferCommand(t *testing.T) {
	t.Helper()
	receipt := xfercontract.TerminalReceipt{TerminalID: "terminal", Kind: "complete", CanonicalDigest: "canonical", ReqID: "request", HistoryStream: "history", HistorySeq: 11}
	for _, state := range []xfercontract.StatusState{xfercontract.StatusUnknown, xfercontract.StatusLive, xfercontract.StatusTerminal, "", "future"} {
		for _, machine := range []bool{false, true} {
			var out bytes.Buffer
			cmd := &cobra.Command{}
			cmd.SetOut(&out)
			status := xfercontract.Status{State: state, Identity: xfercontract.Identity{SID: "session", NID: "node", TransferID: "id"}, Phase: xfercontract.PhaseReceiving,
				DurableOffset: 32768, ChunkIndex: 1, LastConsumedStreamSeq: 17, CleanupPending: true}
			if state == xfercontract.StatusTerminal {
				status.Terminal = &receipt
			}
			err := renderTransferStatus(cmd, status, machine)
			if state == "" || state == "future" {
				if classifyExit(err) != 74 || out.Len() != 0 {
					t.Fatalf("missing/future state claimed success: %v output=%s", err, out.String())
				}
				continue
			}
			if err != nil {
				t.Fatal(err)
			}
			if machine {
				var view map[string]json.RawMessage
				if err := json.Unmarshal(out.Bytes(), &view); err != nil {
					t.Fatal(err)
				}
				for _, key := range []string{"state", "phase", "identity", "origin", "budget", "durable_offset", "chunk_index", "last_consumed_stream_seq", "cleanup_pending"} {
					if _, ok := view[key]; !ok {
						t.Fatalf("JSON lacks %s: %s", key, out.String())
					}
				}
				if _, ok := view["State"]; ok {
					t.Fatal("Go field name leaked into machine schema")
				}
				if strings.Contains(out.String(), "capsule") || strings.Contains(out.String(), "signature") {
					t.Fatal("settlement authentication leaked into status")
				}
			} else if !strings.Contains(out.String(), "state="+string(state)) || strings.Count(out.String(), "\n") != 1 {
				t.Fatalf("human result is not exactly one state line: %s", out.String())
			}
		}
	}
	for _, kind := range []string{"complete", "failed", "future"} {
		for _, unknown := range []bool{false, true} {
			mutated := receipt
			mutated.Kind, mutated.OutcomeUnknown = kind, unknown
			status := xfercontract.Status{State: xfercontract.StatusTerminal, Terminal: &mutated}
			var out bytes.Buffer
			cmd := &cobra.Command{}
			cmd.SetOut(&out)
			err := renderTransferStatus(cmd, status, true)
			if kind == "future" || (kind == "complete" && unknown) {
				if classifyExit(err) != 74 || out.Len() != 0 {
					t.Fatalf("inconsistent receipt claimed success: %v %s", err, out.String())
				}
			} else if err != nil || (unknown && !strings.Contains(out.String(), `"outcome_unknown":true`)) {
				t.Fatalf("terminal query discarded outcome: %v %s", err, out.String())
			}
		}
	}
	for _, mutate := range []func(*xfercontract.TerminalReceipt){
		func(r *xfercontract.TerminalReceipt) { r.TerminalID = "" },
		func(r *xfercontract.TerminalReceipt) { r.CanonicalDigest = "" },
		func(r *xfercontract.TerminalReceipt) { r.ReqID = "" },
		func(r *xfercontract.TerminalReceipt) { r.HistoryStream = "" },
		func(r *xfercontract.TerminalReceipt) { r.HistorySeq = 0 },
	} {
		mutated := receipt
		mutate(&mutated)
		if validTransferReceipt(mutated) {
			t.Fatalf("incomplete proof accepted: %+v", mutated)
		}
	}
	// Exercise the real Cobra boundary with an intentionally failing invocation
	// spy: this asserts forwarding, not real engine or byte-work behavior.
	home := t.TempDir()
	if err := os.WriteFile(filepath.Join(home, "current_session"), []byte("session"), 0o600); err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(home, "source")
	if err := os.WriteFile(source, []byte("source"), 0o600); err != nil {
		t.Fatal(err)
	}
	// Validation must precede invocation construction even with a bound
	// transport. Sparse oversized input avoids doing any byte transfer work.
	oversized := filepath.Join(home, "oversized")
	large, err := os.Create(oversized)
	if err != nil {
		t.Fatal(err)
	}
	if err := large.Truncate(proto.XferMaxBytes + 1); err != nil {
		t.Fatal(err)
	}
	if err := large.Close(); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{filepath.Join(home, "absent"), home, oversized} {
		invoked := false
		ports := transferCommandPorts{
			Open: func(*cobra.Command, string, string, string) (xfercontract.ClientConfig, func() error, error) {
				return xfercontract.ClientConfig{}, func() error { return nil }, nil
			},
			Client: func(xfercontract.ClientConfig) (xfercontract.Invocation, error) {
				invoked = true
				return nil, errors.New("unexpected client construction")
			},
			Classify: func(error) (string, int) { return xfercontract.CodeResultUnknown, 74 },
		}
		cmd := newPushCmdWithPorts(ports)
		cmd.SilenceUsage, cmd.SilenceErrors = true, true
		cmd.SetOut(&bytes.Buffer{})
		cmd.SetErr(&bytes.Buffer{})
		cmd.SetArgs([]string{path, "node:/remote", "--home", home})
		if err := cmd.Execute(); classifyExit(err) != 64 || invoked {
			t.Fatalf("unsafe local source %s entered the invocation: %v invoked=%t", path, err, invoked)
		}
	}
	exerciseTransferInvocationPresentation(t, home, source)
	for _, rate := range []int64{0, 256 * 1024, 4 * 1024 * 1024} {
		var captured xfercontract.ClientConfig
		var pinned *os.File
		closed := false
		ports := transferCommandPorts{
			Open: func(*cobra.Command, string, string, string) (xfercontract.ClientConfig, func() error, error) {
				return xfercontract.ClientConfig{}, func() error { closed = true; return nil }, nil
			},
			Client: func(cfg xfercontract.ClientConfig) (xfercontract.Invocation, error) {
				captured, pinned = cfg, cfg.Source
				return nil, errors.New("test: deliberate construction refusal")
			},
			Classify: func(error) (string, int) { return xfercontract.CodeOwnerRecovering, 75 },
		}
		cmd := newPushCmdWithPorts(ports)
		cmd.SilenceUsage, cmd.SilenceErrors = true, true
		var out bytes.Buffer
		cmd.SetOut(&out)
		cmd.SetErr(&bytes.Buffer{})
		cmd.SetArgs([]string{source, "node:/remote", "--home", home, fmt.Sprintf("--rate-limit=%d", rate)})
		err := cmd.Execute()
		if classifyExit(err) != 75 || !closed || out.Len() != 0 || captured.Intent.RateLimitBPS != rate || captured.Intent.Size != 6 || captured.Identity.TransferID == "" || captured.Source == nil {
			t.Fatalf("rate/identity/pinned-source forwarding failed: err=%v config=%+v closed=%t output=%s", err, captured.Identity, closed, out.String())
		}
		if _, err := pinned.Stat(); !errors.Is(err, os.ErrClosed) {
			t.Fatalf("source survived failed invocation construction: %v", err)
		}
	}
}

// commandInvocationProbe measures CLI emission and cancellation/Stop ordering.
// Its fixed returns never claim to exercise transfer data work or history IO.
type commandInvocationProbe struct {
	source   *os.File
	receipt  xfercontract.TerminalReceipt
	err      error
	onRun    func()
	onCancel func()
	onStop   func()
}

func (p commandInvocationProbe) Run(xfercontract.Deadline) (xfercontract.TerminalReceipt, error) {
	if p.onRun != nil {
		p.onRun()
	}
	return p.receipt, p.err
}
func (p commandInvocationProbe) Cancel(xfercontract.CancelReason, xfercontract.Deadline) (xfercontract.Status, error) {
	if p.onCancel != nil {
		p.onCancel()
	}
	return xfercontract.Status{}, nil
}
func (p commandInvocationProbe) Stop(xfercontract.Deadline) error {
	if p.onStop != nil {
		p.onStop()
	}
	// Successful construction takes the original source pin; the invocation
	// must retire it just as the real client does (S7 plan §7 / §8.1).
	if p.source != nil {
		return p.source.Close()
	}
	return nil
}

type commandStatusProbe struct {
	request  xfercontract.Request
	response xfercontract.Response
	err      error
}

func (p *commandStatusProbe) Request(req xfercontract.Request, _ xfercontract.Deadline) (xfercontract.Response, xfercontract.Attempt, error) {
	p.request = req
	return p.response, xfercontract.Attempt{}, p.err
}
func (*commandStatusProbe) OpenData(xfercontract.Identity, xfercontract.Deadline) (jetstream.JetStream, error) {
	panic("read-only status opened a data connection")
}
func (*commandStatusProbe) CloseData(xfercontract.Identity, xfercontract.Deadline) error {
	panic("read-only status retired data responsibility")
}

func exerciseTransferInvocationPresentation(t *testing.T, home, source string) {
	t.Helper()
	complete := xfercontract.TerminalReceipt{TerminalID: "terminal", Kind: "complete", CanonicalDigest: "canonical", ReqID: "req", HistoryStream: "history", HistorySeq: 1}
	for _, tc := range []struct {
		name    string
		receipt xfercontract.TerminalReceipt
		class   int
		lines   int
	}{
		{"complete", complete, 0, 1},
		{"missing", xfercontract.TerminalReceipt{}, 74, 0},
		{"failed", xfercontract.TerminalReceipt{TerminalID: "terminal", Kind: "failed", CanonicalDigest: "canonical", ReqID: "req", HistoryStream: "history", HistorySeq: 1, Code: xfercontract.CodeNotMember}, 77, 1},
		{"abandoned", xfercontract.TerminalReceipt{TerminalID: "terminal", Kind: "failed", CanonicalDigest: "canonical", ReqID: "req", HistoryStream: "history", HistorySeq: 1, OutcomeUnknown: true, Code: xfercontract.ReasonSettlementAbandoned}, 74, 1},
	} {
		stopped := false
		ports := transferCommandPorts{
			Open: func(*cobra.Command, string, string, string) (xfercontract.ClientConfig, func() error, error) {
				return xfercontract.ClientConfig{}, func() error { return nil }, nil
			},
			Client: func(cfg xfercontract.ClientConfig) (xfercontract.Invocation, error) {
				return commandInvocationProbe{source: cfg.Source, receipt: tc.receipt, onStop: func() { stopped = true }}, nil
			},
			Classify: func(error) (string, int) { return xfercontract.CodeResultUnknown, 74 },
		}
		cmd := newPushCmdWithPorts(ports)
		cmd.SilenceUsage, cmd.SilenceErrors = true, true
		var out bytes.Buffer
		cmd.SetOut(&out)
		cmd.SetErr(&bytes.Buffer{})
		cmd.SetArgs([]string{source, "node:/remote", "--home", home})
		err := cmd.Execute()
		if classifyExit(err) != tc.class || !stopped || strings.Count(out.String(), "\n") != tc.lines {
			t.Fatalf("%s: err=%v class=%d stopped=%t output=%s", tc.name, err, classifyExit(err), stopped, out.String())
		}
		if tc.name == "missing" && !strings.Contains(err.Error(), "tether transfer status") {
			t.Fatal("unresolved result omitted the original ID/status query")
		}
		if tc.name == "abandoned" && !strings.Contains(out.String(), "outcome_unknown=true") {
			t.Fatal("abandonment presented a determinate failure")
		}
	}
	// Cancellation uses a real context callback. Stop must run only after the
	// callback has returned, and the same invocation receives Cancel and Stop.
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	started, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	stopped := false
	ports := transferCommandPorts{
		Open: func(*cobra.Command, string, string, string) (xfercontract.ClientConfig, func() error, error) {
			return xfercontract.ClientConfig{}, func() error { return nil }, nil
		},
		Classify: func(error) (string, int) { return xfercontract.CodeResultUnknown, 74 },
		Client: func(cfg xfercontract.ClientConfig) (xfercontract.Invocation, error) {
			return commandInvocationProbe{
				source:   cfg.Source,
				err:      errors.New("probe: unresolved after cancellation"),
				onRun:    func() { cancel(); <-started; close(release) },
				onCancel: func() { close(started); <-release; time.Sleep(10 * time.Millisecond); close(done) },
				onStop: func() {
					select {
					case <-done:
						stopped = true
					default:
						t.Error("Stop raced an unjoined cancellation callback")
					}
				},
			}, nil
		},
	}
	cmd := newPushCmdWithPorts(ports)
	cmd.SilenceUsage, cmd.SilenceErrors = true, true
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetArgs([]string{source, "node:/remote", "--home", home})
	if err := cmd.ExecuteContext(ctx); classifyExit(err) != 74 || !stopped {
		t.Fatalf("cancellation owner: err=%v stopped=%t", err, stopped)
	}

	for _, tc := range []struct {
		name   string
		status *xfercontract.Status
		code   string
		class  int
	}{
		{"unknown", &xfercontract.Status{State: xfercontract.StatusUnknown, Identity: xfercontract.Identity{SID: "session", NID: "node", TransferID: "id"}}, "", 0},
		{"missing", nil, "", 74},
		{"wrong_identity", &xfercontract.Status{State: xfercontract.StatusUnknown, Identity: xfercontract.Identity{SID: "other", NID: "node", TransferID: "id"}}, "", 74},
		{"unavailable", nil, xfercontract.CodeOwnerRecovering, 75},
		{"deleting", nil, xfercontract.CodeSessionDeleting, 75},
	} {
		probe := &commandStatusProbe{response: xfercontract.Response{Status: tc.status, Code: tc.code}}
		ports := transferCommandPorts{
			Open: func(*cobra.Command, string, string, string) (xfercontract.ClientConfig, func() error, error) {
				return xfercontract.ClientConfig{Transport: probe}, func() error { return nil }, nil
			},
			Classify: func(error) (string, int) { return xfercontract.CodeResultUnknown, 74 },
		}
		cmd := newTransferCmd(ports)
		cmd.SilenceUsage, cmd.SilenceErrors = true, true
		var out bytes.Buffer
		cmd.SetOut(&out)
		cmd.SetErr(&bytes.Buffer{})
		cmd.SetArgs([]string{"status", "id", "--node", "node", "--json", "--home", home})
		err := cmd.Execute()
		if classifyExit(err) != tc.class {
			t.Fatalf("status %s: %v class=%d", tc.name, err, classifyExit(err))
		}
		if probe.request.Op != xfercontract.OpStatus || probe.request.Identity.TransferID != "id" || probe.request.CallID == "" || probe.request.Intent != nil || probe.request.Permit != nil {
			t.Fatalf("status mutated ownership: %+v", probe.request)
		}
		if tc.class != 0 && out.Len() != 0 {
			t.Fatalf("refused status emitted a successful result: %s", out.String())
		}
	}
}
