package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/brokerimage"
	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/execclient"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/nats-io/nats.go"
	"github.com/spf13/cobra"
)

// origin: hot-update S6 internal review rv-ctl-10 / plan §8.2
func TestCtlDiscoveryReservesThePrimaryBudget(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	home := t.TempDir()
	if err := cli.WriteClusterEndpoints(home, &cli.ClusterEndpoints{SchemaVersion: cli.ClusterEndpointsSchemaVersion, PinAccountPub: "existing-pin", BootstrapURL: server.URL}); err != nil {
		t.Fatal(err)
	}
	observer := &ctlTestObserver{}
	a := &ctlAdmission{deadline: ctlTestDeadline(t, 1400*time.Millisecond), logicalID: "discovery-reserve", observer: cli.BindControlObserver(observer, "discovery-reserve")}
	if err := refreshCtlEndpointsWithin(t.Context(), a, nil, home, "nats://127.0.0.1:1", "actor"); err != nil {
		t.Fatal(err)
	}
	cache, err := cli.ReadClusterEndpoints(home)
	if err != nil || cache == nil || cache.FetchedAt != "" || requests.Load() != 0 || len(observer.attempts) != 1 || observer.attempts[0].Class != "skipped" {
		t.Fatalf("best-effort refresh stole the primary budget: requests=%d attempts=%+v cache=%+v err=%v", requests.Load(), observer.attempts, cache, err)
	}
}

// origin: hot-update S6 plan §8.2 / T33
func TestRetryAuthReceiptBoundary(t *testing.T) {
	home := t.TempDir()
	const sid, fingerprint = "lab", "SHA256:accepted-key"
	if err := cli.WriteAuthReceipt(home, sid, fingerprint); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, verb, sid, fp string
		active, eligible    bool
	}{
		{"historical", "exec", sid, fingerprint, true, true},
		{"fresh", "exec", sid, "SHA256:fresh", true, false},
		{"rotated", "exec", sid, "SHA256:rotated", true, false},
		{"foreign-session", "ps", "other", fingerprint, true, false},
		{"unactivated", "exec", sid, fingerprint, false, false},
		{"login", "login", sid, fingerprint, true, false},
		{"pin", "login: activation for session lab", sid, fingerprint, true, false},
		{"pin-space", "login --pin", sid, fingerprint, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			eligible := connectRetryEligible(tc.verb, home, tc.sid, tc.active, tc.fp)
			if eligible != tc.eligible {
				t.Fatalf("receipt eligibility=%v want=%v", eligible, tc.eligible)
			}
			deadline := ctlTestDeadline(t, time.Second)
			attempts := 0
			observer := &ctlTestObserver{}
			_, err := connectWithAuthRetryWithin(t.Context(), deadline, "auth-boundary", observer, eligible,
				func(got controlbudget.Deadline) (*nats.Conn, error) {
					if got != deadline {
						t.Fatalf("CONNECT reset deadline: got=%d want=%d", got, deadline)
					}
					attempts++
					if tc.eligible && attempts == 2 {
						return nil, nil
					}
					return nil, nats.ErrAuthorization
				}, nil)
			if tc.eligible {
				if err != nil || attempts != 2 {
					t.Fatalf("historical retry: attempts=%d err=%v", attempts, err)
				}
				if len(observer.attempts) != 3 || observer.attempts[0].Class != "error" || observer.attempts[1].BackoffNS <= 0 || observer.attempts[2].Class != "success" {
					t.Fatalf("attempt loss: %+v", observer.attempts)
				}
			} else if !errors.Is(err, nats.ErrAuthorization) || attempts != 1 {
				t.Fatalf("fresh/PIN refusal retried: attempts=%d err=%v", attempts, err)
			}
		})
	}
	t.Run("eligible-final-auth-error", func(t *testing.T) {
		started := time.Now()
		attempts := 0
		_, err := connectWithAuthRetryWithin(t.Context(), ctlTestDeadline(t, 600*time.Millisecond), "auth-final", nil, true,
			func(controlbudget.Deadline) (*nats.Conn, error) { attempts++; return nil, nats.ErrAuthorization }, nil)
		if !errors.Is(err, nats.ErrAuthorization) || attempts != 2 || time.Since(started) >= upgradebudget.RequestLatency {
			t.Fatalf("final auth refusal lost: attempts=%d elapsed=%s err=%v", attempts, time.Since(started), err)
		}
	})
	t.Run("explicit-terminal-refusal", func(t *testing.T) {
		attempts := 0
		denied := errors.New("Permissions Violation for Publish")
		_, err := connectWithAuthRetryWithin(t.Context(), ctlTestDeadline(t, time.Second), "acl", nil, true,
			func(controlbudget.Deadline) (*nats.Conn, error) { attempts++; return nil, denied }, nil)
		if attempts != 1 || !errors.Is(err, denied) {
			t.Fatalf("ACL refusal retried: attempts=%d err=%v", attempts, err)
		}
	})
}

// origin: hot-update S6 plan §8.2 / §9.1 / T36
func TestControlResultUnknownNoReplay(t *testing.T) {
	unknown := brokerimage.DetailControlResultUnknown
	t.Run("run-sent-never-legacy", func(t *testing.T) {
		var trace []string
		client := &runTestClient{trace: &trace, result: execclient.Result{CreateSent: true}, err: execclient.ErrUnsupported, check: func(context.Context, proto.RunReq, execclient.Source, execclient.Sink, <-chan execclient.ControlEvent) {
		}}
		terminal := &runTestTerminal{trace: &trace}
		exits := 0
		legacy, err := runReliableBranch(t.Context(), "node", proto.RunReq{Argv: []string{"sh"}}, &bytes.Buffer{}, reliableRunDeps{
			probe:  func(context.Context) (runPTYClient, error) { return client, nil },
			open:   func() (runTerminal, error) { return terminal, nil },
			events: func(context.Context) (<-chan execclient.ControlEvent, func()) { return nil, func() {} },
			exit:   func(execclient.Result) { exits++ },
		})
		if legacy || exits != 0 || classifyExit(err) != exitUnsettled {
			t.Fatalf("sent run requested replay: legacy=%v exits=%d err=%v", legacy, exits, err)
		}
	})
	for _, err := range []error{
		brokerErrorMessage("reload", proto.CodeAgentNoResponders, unknown),
		execFailureMessage(proto.CodeAgentNoResponders + ": " + unknown),
		runFailureMessage(proto.CodeAgentNoResponders + ": " + unknown),
		classifyReliableFailure("node", time.Second, execclient.Result{CreateSent: true}, &execclient.ProtocolError{Code: proto.CodeAgentNoResponders, Message: unknown, AfterCreate: true}),
	} {
		if classifyExit(err) != exitUnsettled || wireCodeOf(err) != proto.CodeAgentNoResponders || isTransientError(err) {
			t.Fatalf("sent result was classified as replayable: exit=%d code=%q err=%v", classifyExit(err), wireCodeOf(err), err)
		}
	}
	for _, tc := range []struct {
		code, detail string
		class        int
	}{
		{proto.CodeBadRequest, brokerimage.DetailMaintenanceObserverUnsupported, exitUsage},
		{proto.CodeUpgradeDeferred, brokerimage.DetailMaintenanceExpired, exitTransient},
		{proto.CodeStoreError, brokerimage.DetailStoreCloseUnknown, exitInternal},
		{proto.CodeAgentNoResponders, unknown, exitUnsettled},
	} {
		err := brokerErrorMessage("reload", tc.code, tc.detail)
		if classifyExit(err) != tc.class || wireCodeOf(err) != tc.code || !strings.Contains(err.Error(), tc.detail) {
			t.Fatalf("broker image refusal lost code/detail/exit mapping: %v", err)
		}
	}

	if classifyExit(brokerErrorMessage("ps", proto.CodeAgentNoResponders, "ordinary unavailable")) != exitTransient {
		t.Fatal("ordinary no responders lost its retryable class")
	}
	if classifyExit(brokerErrorMessage("ps", proto.CodeAgentNoResponders, "text mentions "+unknown)) != exitTransient {
		t.Fatal("unknown reason matched unrelated prose")
	}

	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(strconv.Itoa(round), func(t *testing.T) { exerciseControlResultUnknown(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "unknown control result", beforeG)
	testharness.AssertNoFDLeak(t, "unknown control result", beforeFD, 0)
}

// origin: hot-update S6 plan §8.2 / T30 / T57Cmd
func TestBrokerCommandAdmissionWiring(t *testing.T) {
	for _, path := range []string{"exec.go", "run.go", "ps.go", "node.go"} {
		f, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		if path == "exec.go" || path == "run.go" {
			var budget, publish []token.Pos
			ast.Inspect(f, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				if name, ok := call.Fun.(*ast.Ident); ok && name.Name == "ctlAdmissionContext" {
					budget = append(budget, call.Pos())
				}
				if selector, ok := call.Fun.(*ast.SelectorExpr); ok && selector.Sel.Name == "PublishRequest" {
					publish = append(publish, call.Pos())
				}
				return true
			})
			if len(budget) != 1 || len(publish) != 1 || budget[0] >= publish[0] {
				t.Errorf("%s publishes legacy mutation before checking admission: budget=%v publish=%v", path, budget, publish)
			}
		}
		found := false
		ast.Inspect(f, func(n ast.Node) bool {
			kv, ok := n.(*ast.KeyValueExpr)
			if !ok {
				return true
			}
			key, ok := kv.Key.(*ast.Ident)
			if !ok || key.Name != "RunE" {
				return true
			}
			fn, ok := kv.Value.(*ast.FuncLit)
			if !ok || len(fn.Body.List) == 0 {
				return true
			}
			assign, ok := fn.Body.List[0].(*ast.AssignStmt)
			if !ok || len(assign.Rhs) != 1 {
				return true
			}
			call, ok := assign.Rhs[0].(*ast.CallExpr)
			if !ok {
				return true
			}
			name, ok := call.Fun.(*ast.Ident)
			if ok && name.Name == "beginCtlAdmission" {
				found = true
			}
			return true
		})
		if !found {
			t.Errorf("%s lacks admission at RunE entry", path)
		}
	}
	t.Setenv("TETHER_PS_TIMEOUT", "1h")
	if psRequestTimeout() != upgradebudget.RequestLatency {
		t.Fatal("ps expands the five-second admission budget")
	}
	source, err := os.ReadFile("ps.go")
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(source, []byte("raise TETHER_PS_TIMEOUT")) {
		t.Fatal("ps advises extending admission")
	}
	for _, tc := range []struct{ path, required string }{
		{"exec.go", "execclient.RunWithin"}, {"run.go", "execclient.ProbePTYWithin"},
		{"ps.go", "requestCtlWithin"}, {"node.go", "requestCtlWithin"},
		{"ctl_connect.go", "cli.ConnectWithin"},
	} {
		source, err := os.ReadFile(tc.path)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Contains(source, []byte(tc.required)) {
			t.Errorf("%s does not consume %s", tc.path, tc.required)
		}
	}
	f, err := parser.ParseFile(token.NewFileSet(), "ctl_connect.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	ast.Inspect(f, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		if selector, ok := call.Fun.(*ast.SelectorExpr); ok && selector.Sel.Name == "RunE" {
			t.Error("an entire RunE is invoked from a retry path")
		}
		for _, arg := range call.Args {
			selector, ok := arg.(*ast.SelectorExpr)
			if ok && selector.Sel.Name == "RunE" {
				t.Error("an entire RunE is passed to a retry helper")
			}
		}
		return true
	})
}

// origin: hot-update S6 plan §8.2 / T57Cmd
func TestBrokerCommandLeakCycles(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(strconv.Itoa(round), func(t *testing.T) { exerciseCtlAdmissionCancel(t) })
		t.Run("pty-"+strconv.Itoa(round), func(t *testing.T) { exercisePTYAdmission(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "ctl admission cancellation", beforeG)
	testharness.AssertNoFDLeak(t, "ctl admission cancellation", beforeFD, 0)
}

func exerciseCtlAdmissionCancel(t *testing.T) {
	t.Helper()
	url := testharness.StartNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	// Join both the request callback and cancellation path before returning.
	entered := make(chan struct{})
	release := make(chan struct{})
	joined := make(chan struct{})
	sub, err := nc.Subscribe(proto.SubjCtrlAlertLs("actor"), func(*nats.Msg) { close(entered); <-release; close(joined) })
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := nc.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	observer := &ctlTestObserver{}
	done := make(chan error, 1)
	go func() {
		_, err := cli.FetchAlertsWithin(cli.WithControlObservation(ctx, "cancel-read", observer), nc, "actor", upgradebudget.RequestLatency)
		done <- err
	}()
	select {
	case <-entered:
	case <-t.Context().Done():
		t.Fatal("request never reached broker")
	}
	cancel()
	close(release)
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("request ignored cancellation: %v", err)
		}
	case <-t.Context().Done():
		t.Fatal("canceled request did not join")
	}
	<-joined
	if len(observer.attempts) != 1 || !observer.attempts[0].Sent || observer.attempts[0].Class != "error" {
		t.Fatalf("canceled sent request receipt lost: %+v", observer.attempts)
	}
	if err := nc.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	// The eligible retry wait is canceled by a causal report, not by a sleep.
	retryCtx, cancelRetry := context.WithCancel(t.Context())
	attempts := 0
	_, err = connectWithAuthRetryWithin(retryCtx, ctlTestDeadline(t, upgradebudget.RequestLatency), "cancel", nil, true,
		func(controlbudget.Deadline) (*nats.Conn, error) { attempts++; return nil, nats.ErrAuthorization },
		func(int, error, time.Duration) { cancelRetry() })
	if attempts != 1 || !errors.Is(err, nats.ErrAuthorization) {
		t.Fatalf("retry survived cancellation: attempts=%d err=%v", attempts, err)
	}
}

func ctlTestDeadline(t *testing.T, duration time.Duration) controlbudget.Deadline {
	t.Helper()
	now, err := controlbudget.Now()
	if err != nil {
		t.Fatal(err)
	}
	return controlbudget.Deadline(now + int64(duration))
}

type ctlTestObserver struct {
	mu        sync.Mutex
	ordinal   uint32
	attempts  []controlbudget.Attempt
	closes    int
	beginErr  error
	recordErr error
}

func (o *ctlTestObserver) Begin(id string, phase controlbudget.AttemptPhase) (controlbudget.Attempt, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.beginErr != nil {
		return controlbudget.Attempt{}, o.beginErr
	}
	o.ordinal++
	now, err := controlbudget.Now()
	return controlbudget.Attempt{LogicalID: id, Ordinal: o.ordinal, Phase: phase, StartedMonoNS: now}, err
}
func (o *ctlTestObserver) Record(a controlbudget.Attempt) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.attempts = append(o.attempts, a)
	return o.recordErr
}
func (o *ctlTestObserver) Close() error { o.mu.Lock(); defer o.mu.Unlock(); o.closes++; return nil }

// origin: hot-update S6 plan §8.2 / T30; live dependencies P18-P21
func TestBrokerCommandReadsWithinAdmission(t *testing.T) {
	url := testharness.StartNATS(t)
	home := t.TempDir()
	writeReviewPsIdentity(t, home, "lab")
	t.Setenv(cli.DevNoAuthEnv, "1")
	t.Setenv("TETHER_HOME", home)
	t.Setenv("TETHER_PS_TIMEOUT", "1h")
	start := time.Now()
	_, _, err := runRoot(t, "ps", "--home", home, "--nats-url", url)
	if err == nil || strings.Contains(err.Error(), "not implemented") || time.Since(start) > upgradebudget.RequestLatency+500*time.Millisecond || strings.Contains(err.Error(), "raise TETHER_PS_TIMEOUT") {
		t.Fatalf("ps reset admission: elapsed=%s err=%v", time.Since(start), err)
	}
}

// origin: hot-update S6 plan §8.3 / T35
func TestBrokerCommandObserverOwnership(t *testing.T) {
	t.Run("banner-insufficient-budget", func(t *testing.T) {
		defer func() {
			if p := recover(); p != nil {
				t.Fatalf("banner consumed insufficient admission: panic=%v", p)
			}
		}()
		observer := &ctlTestObserver{}
		a := &ctlAdmission{deadline: ctlTestDeadline(t, cli.ControlBannerWindow/2), logicalID: "skip-banner", observer: observer}
		cmd := &cobra.Command{}
		cmd.SetContext(context.WithValue(t.Context(), ctlAdmissionKey{}, a))
		if err := withBannerWithin(cmd, nil, "actor", false); err != nil {
			t.Fatal(err)
		}
		if len(observer.attempts) != 1 || observer.attempts[0].Class != "skipped" {
			t.Fatalf("banner consumed insufficient admission: %+v", observer.attempts)
		}
	})

	t.Run("unset-receipt-fd", func(t *testing.T) {
		t.Setenv("TETHER_ATTEMPT_RECEIPT_FD", "")
		if err := os.Unsetenv("TETHER_ATTEMPT_RECEIPT_FD"); err != nil {
			t.Fatal(err)
		}
		cmd := &cobra.Command{}
		cmd.SetContext(t.Context())
		a, err := beginCtlAdmission(cmd)
		if err != nil {
			t.Fatal(err)
		}
		if a.observer != nil {
			t.Fatal("unset receipt FD became a typed-nil observer")
		}
		finishCtlAdmission(a, &err)
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("bounded-nil-identity", func(t *testing.T) {
		defer func() {
			if p := recover(); p != nil {
				t.Fatalf("bounded connector lost nil identity guard: panic=%v", p)
			}
		}()
		if _, err := cli.ConnectWithin("nats://127.0.0.1:1", nil, 1, nil); err == nil || err.Error() != "cli: nil identity" {
			t.Fatalf("bounded connector lost nil identity guard: %v", err)
		}
	})
	t.Run("ctl-nil-identity", func(t *testing.T) {
		defer func() {
			if p := recover(); p != nil {
				t.Fatalf("ctl connector lost nil identity guard: panic=%v", p)
			}
		}()
		cmd := &cobra.Command{}
		cmd.SetContext(t.Context())
		if _, err := connectCtlOpts(cmd, "exec", t.TempDir(), "nats://127.0.0.1:1", nil); err == nil || err.Error() != "ctl: nil identity" {
			t.Fatalf("ctl connector lost nil identity guard: %v", err)
		}
	})
	for _, beforeDial := range []bool{true, false} {
		sinkErr := errors.New("receipt unavailable")
		failing := &ctlTestObserver{}
		if beforeDial {
			failing.beginErr = sinkErr
		} else {
			failing.recordErr = sinkErr
		}
		calls := 0
		_, err := connectWithAuthRetryWithin(t.Context(), ctlTestDeadline(t, time.Second), "sink-error", failing, true,
			func(controlbudget.Deadline) (*nats.Conn, error) { calls++; return nil, nats.ErrAuthorization }, nil)
		want := 1
		if beforeDial {
			want = 0
		}
		if calls != want || !errors.Is(err, sinkErr) {
			t.Fatalf("receipt failure allowed dial/retry: calls=%d want=%d err=%v", calls, want, err)
		}
	}

	observer := &ctlTestObserver{}
	a := &ctlAdmission{deadline: ctlTestDeadline(t, time.Second), logicalID: "command", observer: cli.BindControlObserver(observer, "command")}
	err := observeCtlPhase(a, controlbudget.PhaseDiscovery, func() error { return nats.ErrNoResponders })
	if !errors.Is(err, nats.ErrNoResponders) {
		t.Fatal(err)
	}
	if err := observeCtlPhase(a, controlbudget.PhaseBanner, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	if len(observer.attempts) != 2 || observer.attempts[0].Class != "error" || observer.attempts[1].Class != "success" || observer.attempts[0].LogicalID != "command" {
		t.Fatalf("observer lost failed phase: %+v", observer.attempts)
	}
	cmd := &cobra.Command{}
	cmd.SetContext(context.WithValue(t.Context(), ctlAdmissionKey{}, a))
	cfg := ctlAdmissionConfig(cmd)
	if cfg.Deadline != a.deadline || cfg.LogicalID != a.logicalID || cfg.Observer != a.observer {
		t.Fatal("process admission lost command identity")
	}
	var result error
	finishCtlAdmission(a, &result)
	finishCtlAdmission(a, &result)
	if result != nil || observer.closes != 1 || len(observer.attempts) != 3 || observer.attempts[2].Phase != controlbudget.PhaseResult {
		t.Fatalf("observer ownership did not close/join exactly once: closes=%d attempts=%+v err=%v", observer.closes, observer.attempts, result)
	}
}

func exerciseControlResultUnknown(t *testing.T) {
	t.Helper()
	unknown := brokerimage.DetailControlResultUnknown
	// A real broker-side effect occurs while its reply is withheld. No second
	// publication may occur, even when the transport budget expires.
	url := testharness.StartNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	var effects atomic.Int32
	done := make(chan struct{})
	sub, err := nc.Subscribe(proto.SubjCtrlAlertAck("actor"), func(*nats.Msg) {
		if effects.Add(1) == 1 {
			close(done)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := nc.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	_, err = cli.AckAlertWithin(t.Context(), nc, "actor", "cut-record", 80*time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), unknown) {
		t.Fatalf("lost commit reply did not report unknown: %v", err)
	}
	if !strings.HasPrefix(err.Error(), proto.CodeAgentNoResponders+": ") {
		t.Fatalf("unknown result lost its wire code prefix: %v", err)
	}
	translated := brokerErrorMessage("alert ack", proto.CodeAgentNoResponders, strings.TrimPrefix(err.Error(), proto.CodeAgentNoResponders+": "))
	if classifyExit(translated) != exitUnsettled {
		t.Fatalf("sent mutation became replayable after CLI translation: %v", translated)
	}
	select {
	case <-done:
	case <-t.Context().Done():
		t.Fatal("mutation never reached broker")
	}
	if err := nc.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	if effects.Load() != 1 {
		t.Fatalf("unsafe mutation replayed: effects=%d", effects.Load())
	}
}

// A real NATS exchange keeps PTY probes and accepted-but-starting replies on
// admission, then proves an actually running task outlives that deadline.
// origin: hot-update S6 plan §8.2 / T31 / T57Cmd
func exercisePTYAdmission(t *testing.T) {
	t.Helper()
	url := testharness.StartNATS(t)
	id, err := cli.EnsureIdentity(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	nc, err := nats.Connect(url, nats.CustomInboxPrefix(auth.InboxPrefixFor(id.PublicKey)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	subject := proto.SubjCmdBy("lab", id.PublicKey, "node", proto.ProcessResumeVerb)
	if c, err := execclient.ProbePTYWithin(t.Context(), nc, subject, execclient.AdmissionConfig{LogicalID: "pty-invalid"}); c != nil || err == nil {
		t.Fatalf("PTY accepted invalid admission: client=%v err=%v", c, err)
	}
	plain, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	if c, err := execclient.ProbePTYWithin(t.Context(), plain, subject, execclient.AdmissionConfig{Deadline: ctlTestDeadline(t, time.Second), LogicalID: "pty-private"}); c != nil || err == nil || errors.Is(err, execclient.ErrUnsupported) {
		plain.Close()
		t.Fatalf("unknown PTY inbox authorized legacy: client=%v err=%v", c, err)
	}
	plain.Close()
	if c, err := execclient.ProbePTYWithin(t.Context(), nc, subject, execclient.AdmissionConfig{Deadline: ctlTestDeadline(t, 100*time.Millisecond), LogicalID: "pty-silent"}); c != nil || err == nil || errors.Is(err, execclient.ErrUnsupported) {
		t.Fatalf("silent PTY probe authorized legacy: client=%v err=%v", c, err)
	}
	for _, phase := range []string{"starting", "running"} {
		t.Run(phase, func(t *testing.T) {
			started := make(chan struct{})
			continued := make(chan struct{})
			var first, afterAdmission sync.Once
			deadline := ctlTestDeadline(t, 400*time.Millisecond)
			inputFIN := false
			sub, err := nc.Subscribe(subject, func(m *nats.Msg) {
				var req proto.ProcessResumeReq
				if json.Unmarshal(m.Data, &req) != nil {
					return
				}
				reply := proto.ProcessResumeReply{Version: proto.ProcessResumeVersion, CallID: req.CallID}
				if req.Handle == nil {
					reply.Kind, reply.TargetInstance, reply.TaskKind, reply.PTYVersion = "capability", "original-instance", proto.ProcessKindPTY, proto.PTYVersion
					reply.Lease = &proto.ProcessLease{Version: proto.ProcessResumeVersion, LeaseID: "lease-1", TargetInstance: "original-instance", BootID: "original-boot", SID: "lab", NID: "node", ActorFP: id.Fingerprint, ClientLineage: req.ClientLineage, IssuedBootNS: 1, ExpiresBootNS: 999, MAC: "opaque-unit-test-MAC"}
				} else {
					if req.InputFIN {
						inputFIN = true
					}
					reply.Kind, reply.PID, reply.RequestID, reply.TargetInstance, reply.AttachEpoch, reply.Phase = "status", "original-process", req.Handle.RequestID, "original-instance", req.AttachEpoch, phase
					reply.TaskKind, reply.InputWindow = proto.ProcessKindPTY, upgradebudget.PTYInputBytes
					reply.Stderr = &proto.ProcessOutput{FIN: true}
					reply.InputFIN, reply.InputEnded = inputFIN, inputFIN
					first.Do(func() { close(started) })
					if now, err := controlbudget.Now(); phase == "running" && err == nil && now > int64(deadline) {
						afterAdmission.Do(func() { close(continued) })
					}
				}
				body, _ := json.Marshal(reply)
				_ = m.Respond(body)
			})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = sub.Unsubscribe() }()
			if err := nc.FlushTimeout(time.Second); err != nil {
				t.Fatal(err)
			}
			client, err := execclient.ProbePTYWithin(t.Context(), nc, subject, execclient.AdmissionConfig{Deadline: deadline, LogicalID: "pty-" + phase})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = client.Close() }()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan error, 1)
			terminal := &runTestTerminal{}
			go func() {
				_, err := client.Run(ctx, proto.RunReq{Argv: []string{"unit-fixture"}}, terminal, terminal, nil)
				done <- err
			}()
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("PTY create did not reach the wire")
			}
			if phase == "starting" {
				select {
				case err := <-done:
					if !errors.Is(err, context.DeadlineExceeded) {
						t.Fatalf("starting PTY escaped admission: %v", err)
					}
				case <-time.After(time.Second):
					cancel()
					<-done
					t.Fatal("starting PTY renewed admission")
				}
			} else {
				select {
				case err := <-done:
					t.Fatalf("running PTY kept sticky admission: %v", err)
				case <-continued: // require a real post-admission transport request
				case <-time.After(2 * time.Second):
					cancel()
					<-done
					t.Fatal("running PTY never resumed beyond admission")
				}
				cancel()
				select {
				case err := <-done:
					if !errors.Is(err, context.Canceled) {
						t.Fatalf("running PTY cancellation lost: %v", err)
					}
				case <-time.After(time.Second):
					t.Fatal("running PTY did not join cancellation")
				}
			}
		})
	}
}
