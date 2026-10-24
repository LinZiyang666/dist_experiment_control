//go:build linux

// Real NATS, agent request/refusal mapping, Runtime and adopted exec/PTY children.
// The forwarding callback supplies the broker's actor stamp; ACL and reexec are
// outside this test. Recovery waits for a real admission refusal before thawing.
package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/execclient"
	"github.com/LinZiyang666/tether/internal/execsession"
	"github.com/LinZiyang666/tether/internal/natsinbox"
	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/workload"
	"github.com/nats-io/nats.go"
)

// origin: hot-update S4 CLOSE admission contract / plan §4.1 / §7.2
func TestProcessClientsContinueAfterBoundedAdmissionRecovery(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	t.Run("rounds", func(t *testing.T) {
		for round := range 5 {
			t.Run(fmt.Sprintf("busy/%d", round), func(t *testing.T) {
				t.Parallel()
				exercisePTYGenuineBusyAdmission(t)
			})
			for _, kind := range []string{"exec", proto.ProcessKindPTY} {
				t.Run(fmt.Sprintf("%s/%d", kind, round), func(t *testing.T) {
					t.Parallel()
					exerciseProcessAdmissionRecovery(t, kind)
				})
			}
		}
	})
	testharness.AssertNoGoroutineLeak(t, "agent/client admission recovery x5", beforeG)
	testharness.AssertNoFDLeak(t, "agent/client admission recovery x5", beforeFD, 0)
}

func exerciseProcessAdmissionRecovery(t *testing.T, kind string) {
	t.Helper()
	url := startNATS(t)
	a := newBootAgent(t, url, "admission-node")
	agentNC, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer agentNC.Close()
	a.ncBox.Store(agentNC)
	seed, err := auth.GenerateUserSeed()
	if err != nil {
		t.Fatal(err)
	}
	actor, err := auth.PublicKeyFromSeed(seed)
	if err != nil {
		t.Fatal(err)
	}
	fp, err := auth.FingerprintFromActor(actor)
	if err != nil {
		t.Fatal(err)
	}
	clientNC, err := nats.Connect(url, natsinbox.InboxConnectOptions(actor)...)
	if err != nil {
		t.Fatal(err)
	}
	defer clientNC.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 4*upgradebudget.RequestLatency)
	defer cancel()
	subject := proto.SubjCmdBy(a.cfg.SID, actor, nidOf(a), proto.ProcessResumeVerb)
	cut, resumed := make(chan time.Time, 1), make(chan proto.ProcessResumeReq, 64)
	out := &admissionOutput{ready: make(chan struct{})}
	var freezeOnce sync.Once
	var handlers sync.WaitGroup
	var frozen atomic.Bool
	var starts atomic.Int32
	cfg := processRuntimeConfig(a)
	cfg.Instance = "bounded-admission"
	started, ptyStarted := cfg.Started, cfg.PTYStarted
	cfg.Started = func(pid string, spec proto.ExecReq, id workload.Identity) {
		starts.Add(1)
		started(pid, spec, id)
	}
	cfg.PTYStarted = func(pid string, spec proto.RunReq, id workload.Identity) {
		starts.Add(1)
		ptyStarted(pid, spec, id)
	}
	if err := a.processRuntime.Close(ctx); err != nil {
		t.Fatal(err)
	}
	a.processRuntime, err = execsession.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	bridge, err := agentNC.Subscribe(subject, func(msg *nats.Msg) {
		var req proto.ProcessResumeReq
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			t.Error(err)
			return
		}
		if req.Op != "probe" && req.Op != proto.ProcessOpPTYProbe && req.Op != "create" && out.text() == "ready" {
			freezeOnce.Do(func() {
				if err := a.processRuntime.Freeze(time.Now().Add(bootTestBudget)); err != nil {
					t.Error(err)
					return
				}
				frozen.Store(true)
				cut <- time.Now()
			})
		}
		if frozen.Load() {
			select {
			case resumed <- req:
			default:
			}
		}
		req.ActorFP = fp
		body, err := json.Marshal(req)
		if err != nil {
			t.Error(err)
			return
		}
		forwarded := &nats.Msg{Subject: proto.SubjCmdForwarded(a.cfg.SID, nidOf(a), proto.ProcessResumeVerb), Reply: msg.Reply, Data: body}
		handlers.Add(1)
		go func() {
			defer handlers.Done()
			handleProcessResumeForwarded(a, agentNC, forwarded)
		}()
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = bridge.Drain()
		// Drain is asynchronous. Its barrier joins all preceding callbacks,
		// so no callback can add another Handle owner while Wait joins them.
		joined := make(chan struct{})
		if err := agentNC.Barrier(func() { close(joined) }); err != nil {
			t.Error(err)
		} else {
			<-joined
		}
		_ = a.processRuntime.Thaw()
		handlers.Wait()
	}()
	// Observe the actual agent reply independently of CallID filtering. PTY
	// may have moved to a newer attempt when a five-second waiter replies.
	receipts, err := agentNC.SubscribeSync(auth.InboxPrefixFor(actor) + ".>")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = receipts.Unsubscribe() }()
	if err := agentNC.FlushTimeout(bootTestBudget); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(t.TempDir(), "release")
	command := "printf ready; while ! test -f \"$RELEASE\"; do sleep 0.01; done; printf done; exit 23"
	var stderr bytes.Buffer
	done := make(chan struct{})
	var result execclient.Result
	var runErr error
	if kind == proto.ProcessKindPTY {
		client, err := execclient.ProbePTY(ctx, clientNC, subject)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = client.Close() }()
		in := &admissionInput{closed: make(chan struct{})}
		go func() {
			defer close(done)
			result, runErr = client.Run(ctx, proto.RunReq{Argv: []string{"/bin/sh", "-c", command}, Env: map[string]string{"RELEASE": marker}}, in, out, nil)
		}()
	} else {
		go func() {
			defer close(done)
			result, runErr = execclient.Run(ctx, clientNC, subject,
				proto.ExecReq{Argv: []string{"/bin/sh", "-c", command}, Env: map[string]string{"RELEASE": marker}}, out, &stderr)
		}()
	}
	defer func() { cancel(); <-done }()
	var cutAt time.Time
	select {
	case cutAt = <-cut:
	case <-ctx.Done():
		t.Fatal("client never attached before the admission cut")
	}
	var first proto.ProcessResumeReq
	select {
	case first = <-resumed:
	case <-ctx.Done():
		t.Fatal("cut had no attached request")
	}
	for {
		msg, err := receipts.NextMsgWithContext(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var reply proto.ProcessResumeReply
		if err := json.Unmarshal(msg.Data, &reply); err != nil {
			t.Fatal(err)
		}
		if reply.Kind != "error" {
			continue
		}
		if reply.Code != proto.CodeProcessRecovering || time.Since(cutAt) < upgradebudget.RequestLatency {
			t.Fatalf("admission expiry lost its recovery contract: code=%q elapsed=%s", reply.Code, time.Since(cutAt))
		}
		break
	}
	select {
	case <-done:
		t.Fatalf("client abandoned a retained live process: %v", runErr)
	default:
	}
	if !a.processRuntime.Frozen() || starts.Load() != 1 || len(a.processRuntime.TaskPIDs()) != 1 {
		t.Fatal("admission expiry released or duplicated the original owner")
	}
	frozen.Store(false)
	if err := a.processRuntime.Thaw(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(marker, []byte("release"), 0600); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("original client did not settle after recovery")
	}
	if runErr != nil || result.ExitCode != 23 || !result.Executed || !result.CreateSent || out.text() != "readydone" || stderr.Len() != 0 || starts.Load() != 1 {
		t.Fatalf("original task failed continuity: result=%+v err=%v output=%q starts=%d", result, runErr, out.text(), starts.Load())
	}
	if first.Handle == nil || result.Handle != *first.Handle || result.PID == "" || len(a.processRuntime.TaskPIDs()) != 0 {
		t.Fatal("recovery changed identity or failed terminal settlement")
	}
	for len(resumed) != 0 {
		retry := <-resumed
		if retry.Op != first.Op || retry.Handle == nil || *retry.Handle != *first.Handle ||
			(kind != proto.ProcessKindPTY && retry.CallID != first.CallID) {
			t.Fatal("admission retry changed its original transaction")
		}
	}
}

type admissionOutput struct {
	mu    sync.Mutex
	data  bytes.Buffer
	ready chan struct{}
	once  sync.Once
}

func (o *admissionOutput) Write(p []byte) (int, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	n, err := o.data.Write(p)
	if bytes.Contains(o.data.Bytes(), []byte("ready")) {
		o.once.Do(func() { close(o.ready) })
	}
	return n, err
}

func (o *admissionOutput) text() string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.data.String()
}

func (*admissionOutput) Cancel() error { return nil } // buffer writes never block

type admissionInput struct {
	closed chan struct{}
	once   sync.Once
}

func (i *admissionInput) Read([]byte) (int, error) {
	<-i.closed
	return 0, context.Canceled
}

func (i *admissionInput) Cancel() error {
	i.once.Do(func() { close(i.closed) })
	return nil
}

// origin: hot-update S4 CLOSE admission contract / plan §2.2
func TestProcessRecoveryRefusalPrecedesBusyMapping(t *testing.T) {
	if got := processRefusalCode(errors.Join(procrequest.ErrBusy, execsession.ErrRollbackPending)); got != proto.CodeProcessRecovering {
		t.Fatalf("retained recovery became ordinary busy: %s", got)
	}
	for _, err := range []error{procrequest.ErrBusy, procrequest.ErrFrozen} {
		if got := processRefusalCode(err); got != proto.CodeProcessBusy {
			t.Fatalf("genuine busy became recoverable: %s", got)
		}
	}
}

// The actual SHA-bound pending-upgrade policy refuses before ReservePTY.
// The candidate fixtures are version readers; no upgrade or deployment runs.
func exercisePTYGenuineBusyAdmission(t *testing.T) {
	t.Helper()
	url := startNATS(t)
	a := domainNewPendingAgent(t)
	agentNC, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer agentNC.Close()
	a.ncBox.Store(agentNC)
	seed, err := auth.GenerateUserSeed()
	if err != nil {
		t.Fatal(err)
	}
	actor, err := auth.PublicKeyFromSeed(seed)
	if err != nil {
		t.Fatal(err)
	}
	fp, err := auth.FingerprintFromActor(actor)
	if err != nil {
		t.Fatal(err)
	}
	clientNC, err := nats.Connect(url, natsinbox.InboxConnectOptions(actor)...)
	if err != nil {
		t.Fatal(err)
	}
	defer clientNC.Close()
	subject := proto.SubjCmdBy(a.cfg.SID, actor, nidOf(a), proto.ProcessResumeVerb)
	var creates atomic.Int32
	bridge, err := agentNC.Subscribe(subject, func(msg *nats.Msg) {
		var req proto.ProcessResumeReq
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			t.Error(err)
			return
		}
		if req.Op == "create" {
			creates.Add(1)
		}
		req.ActorFP = fp
		body, err := json.Marshal(req)
		if err != nil {
			t.Error(err)
			return
		}
		handleProcessResumeForwarded(a, agentNC, &nats.Msg{Subject: proto.SubjCmdForwarded(a.cfg.SID, nidOf(a), proto.ProcessResumeVerb), Reply: msg.Reply, Data: body})
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = bridge.Unsubscribe() }()
	if err := agentNC.FlushTimeout(bootTestBudget); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), bootTestBudget)
	defer cancel()
	client, err := execclient.ProbePTY(ctx, clientNC, subject)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	result, err := client.Run(ctx, proto.RunReq{Argv: []string{"/bin/true"}},
		&admissionInput{closed: make(chan struct{})}, &admissionOutput{ready: make(chan struct{})}, nil)
	var refusal *execclient.ProtocolError
	if !errors.As(err, &refusal) || refusal.Code != proto.CodeProcessBusy || refusal.AfterCreate ||
		!result.CreateSent || result.PID != "" || creates.Load() != 1 || len(a.workloads.Snapshot()) != 0 || len(a.processRuntime.TaskPIDs()) != 0 {
		t.Fatalf("pending admission became recovery or started a child: result=%+v err=%v creates=%d", result, err, creates.Load())
	}
}
