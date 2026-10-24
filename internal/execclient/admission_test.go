package execclient

// Real NATS and client protocol owners; responses model the admission boundary.
// The agent/Runtime integration is in agent/process_recovery_test.go.
// No broker ACL, OS process or image handoff is replaced by these fixtures.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/execsession"
	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/workload"
	"github.com/nats-io/nats.go"
)

// origin: hot-update S4 CLOSE admission contract / plan §4.1
func TestExecClientRecoveryRefusalKeepsOriginalTransaction(t *testing.T) {
	t.Run("recovered", func(t *testing.T) {
		f := newClientFixture(t)
		var out bytes.Buffer
		run := f.start(t, 3*clientTestBudget, &out, io.Discard)
		first := f.negotiate(t)
		retry := first
		for range 3 {
			sendClientReply(t, retry, proto.ProcessResumeReply{Version: 1, CallID: retry.req.CallID,
				Kind: "error", Code: proto.CodeProcessRecovering, Error: "retained recovery"})
			retry = f.next(t, "create")
			if !reflect.DeepEqual(retry.req, first.req) {
				t.Fatal("recovery changed CallID, handle or immutable creation intent")
			}
		}
		reply := terminalClientStatus(retry)
		reply.Stdout.Data = []byte("exact-output")
		sendClientReply(t, retry, reply)
		result := settleClient(t, f, run)
		if result.ExitCode != 17 || out.String() != "exact-output" || result.Handle != *first.req.Handle {
			t.Fatal("recovery lost output, terminal or original identity")
		}
	})
	for _, end := range []string{"cancel", "deadline"} {
		t.Run(end, func(t *testing.T) {
			f := newClientFixture(t)
			budget := 3 * clientTestBudget
			if end == "deadline" {
				budget = time.Second
			}
			run := f.start(t, budget, io.Discard, io.Discard)
			first := f.negotiate(t)
			sendClientReply(t, first, proto.ProcessResumeReply{Version: 1, CallID: first.req.CallID,
				Kind: "error", Code: proto.CodeProcessRecovering, Error: "retained recovery"})
			retry := f.next(t, "create")
			if !reflect.DeepEqual(first.req, retry.req) {
				t.Fatal("recovery changed transaction before context expiry")
			}
			if end == "cancel" {
				run.cancel()
			}
			result, err := run.finish(t)
			want := context.Canceled
			if end == "deadline" {
				want = context.DeadlineExceeded
			}
			if !errors.Is(err, want) || !result.CreateSent || result.Handle != *first.req.Handle {
				t.Fatalf("recovery outlived its caller or lost responsibility: %+v err=%v", result, err)
			}
		})
	}
}

// origin: hot-update S4 CLOSE admission contract / plan §2.2 / §7.2
func TestProcessClientsKeepGenuineBusyFinal(t *testing.T) {
	for _, kind := range []string{"exec", "pty"} {
		for _, phase := range []string{"create", "attached", "recovering"} {
			t.Run(kind+"/"+phase, func(t *testing.T) {
				f := newClientFixture(t)
				var run *clientRun
				var ex clientExchange
				if kind == "pty" {
					c := probePTYFixture(t, f)
					run = startPTYFixture(t, c, proto.RunReq{Argv: []string{"fixture"}}, newPTYTestSource(nil, false, false), newPTYTestSink(nil), nil)
					ex = f.next(t, "create")
					if phase == "attached" {
						sendClientReply(t, ex, ptyStatus(ex, 0, 0, 0))
						ex = f.next(t, "ack")
					}
				} else {
					run = f.start(t, 3*clientTestBudget, io.Discard, io.Discard)
					ex = f.negotiate(t)
					if phase == "attached" {
						sendClientReply(t, ex, clientStatus(ex))
						ex = f.next(t, "ack")
					}
				}
				if phase == "recovering" {
					// Losing a create reply already made this handle ambiguous.
					// Ordinary busy must still be final in the new contract.
					ex = f.next(t, "create")
				}
				sendClientReply(t, ex, proto.ProcessResumeReply{Version: 1, CallID: ex.req.CallID,
					Kind: "error", Code: proto.CodeProcessBusy, Error: "genuine busy"})
				result, err := run.finish(t)
				var pe *ProtocolError
				if !errors.As(err, &pe) || pe.Code != proto.CodeProcessBusy || errors.Is(err, ErrUnsupported) || !result.CreateSent {
					t.Fatalf("genuine busy became retry/fallback: %+v err=%v", result, err)
				}
				if _, err := f.sub.NextMsg(100 * time.Millisecond); !errors.Is(err, nats.ErrTimeout) {
					t.Fatalf("client retried a genuine busy refusal: %v", err)
				}
			})
		}
	}
}

// origin: hot-update S4 CLOSE admission contract / architecture §21.2
func TestRecoveryRefusalPreservesNMinusOneSafety(t *testing.T) {
	f := newClientFixture(t)
	inbox := f.nc.NewInbox()
	sub, err := f.nc.SubscribeSync(inbox)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	// The refusal is published on another connection: without this round trip
	// the server may route it before it has processed this SUB, dropping it and
	// turning the assertion into a 5 s receive timeout under load.
	if err := f.nc.Flush(); err != nil {
		t.Fatal(err)
	}
	tx := &transport{nc: f.nc, sub: sub, subject: f.subject, inbox: inbox, created: true}
	// receive and isUnsupported are unchanged from the pre-CLOSE client.
	// Its call wrapper retried only transport errors, and its PTY wrapper had
	// the closed code set below. This pins that compatibility seam without
	// claiming to run a previous release binary.
	reply := proto.ProcessResumeReply{Version: 1, CallID: 9, Kind: "error",
		Code: proto.CodeProcessRecovering, Error: "exec session: freeze rollback pending"}
	body, err := json.Marshal(reply)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.responder.Publish(inbox, body); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), clientTestBudget)
	defer cancel()
	_, err = tx.receive(ctx, reply.CallID)
	var pe *ProtocolError
	if !errors.As(err, &pe) || pe.Code != proto.CodeProcessRecovering || !pe.AfterCreate || isUnsupported(err) || ptyUnsupported(err) {
		t.Fatalf("old receive contract allowed fallback or lost refusal: %v", err)
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, nats.ErrTimeout) ||
		errors.Is(err, nats.ErrNoResponders) || errors.Is(err, nats.ErrConnectionReconnecting) {
		t.Fatal("old exec call would retry the unknown code")
	}
	code := pe.Code
	if code == "" {
		code, _, _ = strings.Cut(pe.Message, ":")
	}
	if code == "node_offline" || code == "agent_upgrading" || code == proto.CodeProcessBusy {
		t.Fatal("old PTY recovery set would accept the new code")
	}
}

var (
	_ ProbeWithinFunc  = ProbeWithin
	_ CreateWithinFunc = CreateWithin
	_ RunWithinFunc    = RunWithin
)

// origin: hot-update S6 internal review rv-ctl-1 / lead D1
func TestAdmissionLegacyProofRequiresExplicitOldRelease(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseLegacyVersionProof(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "legacy version proof", beforeG)
	testharness.AssertNoFDLeak(t, "legacy version proof", beforeFD, 0)
}

func exerciseLegacyVersionProof(t *testing.T) {
	t.Helper()
	for _, version := range []string{"", "v0.7.0", "v0.6.0-junk", "v0.6.0", "0.6.0"} {
		t.Run(version, func(t *testing.T) {
			f := newClientFixture(t)
			body, err := json.Marshal(proto.NodeListResp{Nodes: []proto.NodeListEntry{{NID: "node", Status: "ONLINE", ReleaseVersion: version}}})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := f.responder.Subscribe(proto.SubjCtrlNodeList(f.actor, "lab"), func(msg *nats.Msg) { _ = msg.Respond(body) }); err != nil {
				t.Fatal(err)
			}
			if err := f.responder.FlushTimeout(time.Second); err != nil {
				t.Fatal(err)
			}
			old, err := legacyNodeProof(t.Context(), &transport{nc: f.nc, subject: f.subject}, admissionConfig(t, time.Second))
			want := version == "v0.6.0" || version == "0.6.0"
			if err != nil || old != want {
				t.Fatalf("version %q supplied legacy proof=%t want=%t err=%v", version, old, want, err)
			}
		})
	}
}

// Real: private NATS inboxes, reliable Runtime deduplication, pipes and OS child
// effects. Only the broker's actor stamp is supplied by the fixture. No claim
// about callout authentication, broker exec or deploy-tier blackout is made.
type admissionObserver struct {
	mu                    sync.Mutex
	attempts              []controlbudget.Attempt
	ordinal               uint32
	failBegin, failRecord bool
	beginDelay            time.Duration
	delayPhase            controlbudget.AttemptPhase
}

func (o *admissionObserver) Begin(id string, phase controlbudget.AttemptPhase) (controlbudget.Attempt, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.failBegin {
		return controlbudget.Attempt{}, errors.New("observer begin refused")
	}
	if o.beginDelay > 0 && (o.delayPhase == "" || phase == o.delayPhase) {
		time.Sleep(o.beginDelay)
	}
	o.ordinal++
	now, err := procrequest.BootNow()
	return controlbudget.Attempt{LogicalID: id, Ordinal: o.ordinal, Phase: phase, StartedMonoNS: now}, err
}
func (o *admissionObserver) Record(a controlbudget.Attempt) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.failRecord {
		return errors.New("observer record refused")
	}
	o.attempts = append(o.attempts, a)
	return nil
}
func (*admissionObserver) Close() error { return nil }
func (o *admissionObserver) snapshot() []controlbudget.Attempt {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]controlbudget.Attempt(nil), o.attempts...)
}

func admissionConfig(t *testing.T, duration time.Duration) AdmissionConfig {
	t.Helper()
	now, err := procrequest.BootNow()
	if err != nil {
		t.Fatal(err)
	}
	return AdmissionConfig{Deadline: controlbudget.Deadline(now + int64(duration)), LogicalID: "original-logical-operation", Observer: &admissionObserver{}}
}

func startAdmission(t *testing.T, f *clientFixture, cfg AdmissionConfig, spec proto.ExecReq, out io.Writer) *clientRun {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	r := &clientRun{done: make(chan struct{}), cancel: cancel}
	go func() {
		defer close(r.done)
		r.result, r.err = RunWithin(ctx, f.nc, f.subject, spec, out, io.Discard, cfg)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-r.done:
		case <-time.After(clientTestBudget):
			t.Error("admission client did not join")
		}
	})
	return r
}

func admissionRuntime(t *testing.T) (*execsession.Runtime, *atomic.Int64) {
	t.Helper()
	var manager workload.Manager
	starts := new(atomic.Int64)
	rt, err := execsession.New(execsession.Config{Instance: "admission-original-instance", Manager: &manager,
		Started: func(string, proto.ExecReq, workload.Identity) {}, Exited: func(string, int) {},
		Start: func(id string, spec *proto.ExecReq) (*workload.Stdio, string, error) {
			stdio, err := workload.NewStdio()
			if err != nil {
				return nil, "", err
			}
			cmd := exec.Command(spec.Argv[0], spec.Argv[1:]...)
			cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
			stdio.Bind(cmd)
			if err := cmd.Start(); err != nil {
				stdio.Close()
				return nil, "", err
			}
			starts.Add(1)
			stdio.CloseChild()
			if err := manager.AdoptDraining(workload.Identity{ID: id, OSPID: cmd.Process.Pid, StartedAt: time.Now().UTC()}, cmd.Process); err != nil {
				_ = cmd.Process.Kill()
				_ = cmd.Wait()
				stdio.Close()
				return nil, "", err
			}
			return stdio, "", nil
		}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), clientTestBudget)
		defer cancel()
		if err := rt.Close(ctx); err != nil {
			t.Errorf("runtime cleanup: %v", err)
		}
	})
	return rt, starts
}

func admissionRuntimeReply(t *testing.T, rt *execsession.Runtime, f *clientFixture, ex clientExchange) proto.ProcessResumeReply {
	t.Helper()
	req := ex.req
	req.ActorFP = f.fp
	lineage := req.ClientLineage
	if req.Handle != nil {
		lineage = req.Handle.Lease.ClientLineage
	}
	reply, err := rt.Handle(procrequest.Scope{SID: "lab", NID: "node", ActorFP: f.fp, ClientLineage: lineage}, req, ex.msg.Reply)
	if err != nil {
		t.Fatal(err)
	}
	return reply
}

func finishAdmissionRuntime(t *testing.T, rt *execsession.Runtime, f *clientFixture, r *clientRun) Result {
	t.Helper()
	for {
		ex := f.next(t, "")
		reply := admissionRuntimeReply(t, rt, f, ex)
		sendClientReply(t, ex, reply)
		if ex.req.Op == "terminal-ack" {
			break
		}
	}
	result, err := r.finish(t)
	if err != nil || !result.CreateSent || !result.Executed || result.ExitCode != 0 || result.PID == "" {
		t.Fatalf("original child did not settle: %+v err=%v", result, err)
	}
	return result
}

// origin: hot-update S6 plan §8.2 / T31
func TestAdmissionProbeTimeoutDoesNotDowngrade(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("real reliable Runtime uses Linux replay")
	}
	gBefore, fdBefore := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseProbeBlackout(t) })
	}
	for _, code := range []string{proto.CodeProcessUnsupported, proto.CodeProcessUnsupportedOp} {
		t.Run("explicit_unsupported/"+code, func(t *testing.T) {
			f := newClientFixture(t)
			cfg := admissionConfig(t, time.Second)
			r := startAdmission(t, f, cfg, proto.ExecReq{Argv: []string{"/bin/true"}}, io.Discard)
			probe := f.next(t, "probe")
			sendClientReply(t, probe, proto.ProcessResumeReply{Version: 1, CallID: probe.req.CallID, Kind: "error", Code: code, Error: "unsupported process protocol"})
			result, err := r.finish(t)
			if !errors.Is(err, ErrUnsupported) || result.CreateSent {
				t.Fatalf("explicit unsupported boundary: %+v %v", result, err)
			}
			// Model the caller's permitted legacy branch with one real PUB and
			// one real child effect; RunWithin itself owns no legacy publisher.
			legacy := proto.SubjCmdBy("lab", f.actor, "node", "exec")
			if err := f.nc.Publish(legacy, []byte("legacy-once")); err != nil {
				t.Fatal(err)
			}
			msg, err := f.sub.NextMsg(clientTestBudget)
			if err != nil || msg.Subject != legacy {
				t.Fatalf("missing legacy PUB: %v", err)
			}
			marker := t.TempDir() + "/legacy-effect"
			if err := exec.Command("/bin/sh", "-c", `printf x >> "$1"`, "fixture", marker).Run(); err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(marker)
			if err != nil || string(data) != "x" {
				t.Fatalf("legacy effect: %q %v", data, err)
			}
			if _, err := f.sub.NextMsg(20 * time.Millisecond); !errors.Is(err, nats.ErrTimeout) {
				t.Fatalf("extra legacy/create PUB: %v", err)
			}
		})
	}
	testharness.AssertNoGoroutineLeak(t, "exec probe blackout", gBefore)
	testharness.AssertNoFDLeak(t, "exec probe blackout", fdBefore, 0)
}

func exerciseProbeBlackout(t *testing.T) {
	t.Helper()
	f := newClientFixture(t)
	// origin: hot-update S6 internal review rv-ctl-1 / lead D1
	// A completed modern metadata read during silence is not legacy evidence.
	// This keeps the positive old-version door distinct from the blackout path.
	nodes, err := json.Marshal(proto.NodeListResp{Nodes: []proto.NodeListEntry{{NID: "node", Status: "ONLINE", ReleaseVersion: "v0.7.0", ProtoVersion: proto.ProtoVersion}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.responder.Subscribe(proto.SubjCtrlNodeList(f.actor, "lab"), func(msg *nats.Msg) { _ = msg.Respond(nodes) }); err != nil {
		t.Fatal(err)
	}
	if err := f.responder.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	rt, starts := admissionRuntime(t)
	cfg := admissionConfig(t, 5*time.Second)
	marker := t.TempDir() + "/effect"
	r := startAdmission(t, f, cfg, proto.ExecReq{Argv: []string{"/bin/sh", "-c", `printf x >> "$1"; printf original`, "fixture", marker}}, io.Discard)
	began := time.Now()
	first := f.next(t, "probe")
	current := first
	for time.Since(began) < 2100*time.Millisecond {
		current = f.next(t, "probe")
		if !reflect.DeepEqual(current.req, first.req) {
			t.Fatal("probe retry changed CallID or lineage")
		}
	}
	if time.Since(began) < 2100*time.Millisecond {
		t.Fatal("blackout premise missing")
	}
	sendClientReply(t, current, admissionRuntimeReply(t, rt, f, current))
	create := f.next(t, "create")
	sendClientReply(t, create, admissionRuntimeReply(t, rt, f, create))
	finishAdmissionRuntime(t, rt, f, r)
	data, err := os.ReadFile(marker)
	if err != nil || string(data) != "x" || starts.Load() != 1 || time.Since(began) > 5*time.Second {
		t.Fatalf("blackout violated budget/exact effect: %q starts=%d elapsed=%v err=%v", data, starts.Load(), time.Since(began), err)
	}
	o := cfg.Observer.(*admissionObserver).snapshot()
	var failed, createSends int
	for i, a := range o {
		if a.LogicalID != cfg.LogicalID || a.Ordinal != uint32(i+1) || a.FinishedMonoNS < a.StartedMonoNS {
			t.Fatalf("incomplete attempt: %+v", a)
		}
		if a.Phase == controlbudget.PhaseProbe && a.Class == "timeout" && a.Sent {
			failed++
		}
		if a.Phase == controlbudget.PhaseCreate && a.Sent {
			createSends++
		}
	}
	if failed < 4 || createSends != 1 {
		t.Fatalf("missing attempts: failed=%d create=%d", failed, createSends)
	}
	if _, err := f.sub.NextMsg(20 * time.Millisecond); !errors.Is(err, nats.ErrTimeout) {
		t.Fatalf("extra/legacy request: %v", err)
	}
}

// origin: hot-update S6 plan §8.2 / T32
func TestRetryStableLogicalIdentity(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("real reliable Runtime uses Linux replay")
	}
	gBefore, fdBefore := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseStableAdmission(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "exec stable admission", gBefore)
	testharness.AssertNoFDLeak(t, "exec stable admission", fdBefore, 0)
}

func exerciseStableAdmission(t *testing.T) {
	t.Helper()
	f := newClientFixture(t)
	rt, starts := admissionRuntime(t)
	cfg := admissionConfig(t, 1800*time.Millisecond)
	marker, release := t.TempDir()+"/effect", t.TempDir()+"/release"
	var out bytes.Buffer
	r := startAdmission(t, f, cfg, proto.ExecReq{Argv: []string{"/bin/sh", "-c", `printf x >> "$1"; printf started; while [ ! -f "$2" ]; do sleep 0.01; done; printf tail`, "fixture", marker, release}}, &out)
	probe := f.next(t, "probe")
	sendClientReply(t, probe, admissionRuntimeReply(t, rt, f, probe))
	create := f.next(t, "create")
	_ = admissionRuntimeReply(t, rt, f, create) // actual child starts, receipt is lost
	retry := f.next(t, "create")
	if !reflect.DeepEqual(create.req, retry.req) || !bytes.Equal(create.msg.Data, retry.msg.Data) {
		t.Fatal("lost create reply changed canonical payload, CallID, lease or RequestID")
	}
	reply := admissionRuntimeReply(t, rt, f, retry)
	sendClientReply(t, retry, reply)
	if reply.Phase != "running" {
		t.Fatalf("true-start premise missing: %+v", reply)
	}
	// Keep the actual child alive beyond admission. Replies during this period
	// must use the user context without retaining a sticky admission timeout.
	for {
		ex := f.next(t, "ack")
		now, err := procrequest.BootNow()
		if err != nil {
			t.Fatal(err)
		}
		sendClientReply(t, ex, admissionRuntimeReply(t, rt, f, ex))
		if now > int64(cfg.Deadline) {
			break
		}
	}
	if err := os.WriteFile(release, []byte("release"), 0600); err != nil {
		t.Fatal(err)
	}
	result := finishAdmissionRuntime(t, rt, f, r)
	data, err := os.ReadFile(marker)
	if err != nil || string(data) != "x" || starts.Load() != 1 || result.Handle != *create.req.Handle || out.String() != "startedtail" {
		t.Fatalf("retry started another process or lost result: starts=%d data=%q output=%q err=%v", starts.Load(), data, out.String(), err)
	}
}

// origin: hot-update S6 plan §8.2 / T57Exec
func TestBrokerExecClientLeakCycles(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("admission uses Linux BOOTTIME")
	}
	gBefore, fdBefore := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseAdmissionBlackout(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "exec admission cancellation", gBefore)
	testharness.AssertNoFDLeak(t, "exec admission cancellation", fdBefore, 0)
}

func exerciseAdmissionBlackout(t *testing.T) {
	t.Helper()
	for _, phase := range []string{"probe", "create", "starting", "observer_begin", "observer_record", "no_responders", "legacy_confirmed", "terminal_refusal", "invalid_budget", "probe_api", "create_api", "post_create_unsupported", "expired_budget", "private_inbox", "recovering", "slow_observer", "invalid_probe", "invalid_create", "early_context", "slow_backoff", "wrong_capability", "post_create_identity"} {
		t.Run(phase, func(t *testing.T) {
			f := newClientFixture(t)
			// The caller owns this reusable connection. Count subscriptions before
			// its Close can hide an admission inbox that failed to release on return.
			before := f.nc.NumSubscriptions()
			defer func() {
				if got := f.nc.NumSubscriptions(); got != before {
					t.Errorf("admission inbox leaked before connection close: got %d want %d", got, before)
				}
			}()
			duration := time.Second
			if phase == "probe" || phase == "create" || phase == "starting" || phase == "no_responders" {
				duration = 120 * time.Millisecond
			}
			cfg := admissionConfig(t, duration)
			began := time.Now()
			r := startAdmission(t, f, cfg, proto.ExecReq{Argv: []string{"/bin/true"}}, io.Discard)
			probe := f.next(t, "probe")
			if phase == "probe" {
				r.cancel()
				_, err := r.finish(t)
				if !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
				return
			}
			if phase == "no_responders" {
				if err := f.sub.Unsubscribe(); err != nil {
					t.Fatal(err)
				}
				if err := f.responder.FlushTimeout(clientTestBudget); err != nil {
					t.Fatal(err)
				}
				r.cancel()
				_, _ = r.finish(t)
				// With every responder gone, the next request gets a real NATS 503.
				// The prior silent attempt alone would only prove timeout handling.
				cfg = admissionConfig(t, 600*time.Millisecond)
				r = startAdmission(t, f, cfg, proto.ExecReq{Argv: []string{"/bin/true"}}, io.Discard)
				_, err := r.finish(t)
				attempts := cfg.Observer.(*admissionObserver).snapshot()
				saw503 := false
				for _, a := range attempts {
					if a.Class == "no_responders" && a.Sent {
						saw503 = true
					}
				}
				if !saw503 {
					t.Fatalf("real no-responder premise missing: %+v", attempts)
				}
				if time.Since(began) > time.Second {
					t.Fatalf("absent responder reset budget: %v", time.Since(began))
				}
				if errors.Is(err, ErrUnsupported) || !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("absent peer downgraded: %v", err)
				}
				return
			}
			if phase == "terminal_refusal" {
				sendClientReply(t, probe, proto.ProcessResumeReply{Version: 1, CallID: probe.req.CallID, Kind: "error", Code: "not_a_member", Error: "private-refusal-text"})
				_, err := r.finish(t)
				var refusal *ProtocolError
				if !errors.As(err, &refusal) || errors.Is(err, ErrUnsupported) {
					t.Fatalf("ACL was retried/downgraded: %v", err)
				}
				// A peer's prose can contain private data. The receipt must carry only
				// the class, while the original typed refusal remains the user result.
				attempts := cfg.Observer.(*admissionObserver).snapshot()
				if len(attempts) != 1 || attempts[0].Class != "terminal" || !attempts[0].Sent {
					t.Fatalf("terminal receipt lost: %+v", attempts)
				}
				body, err := json.Marshal(attempts)
				if err != nil || bytes.Contains(body, []byte("private-refusal-text")) {
					t.Fatal("peer error text leaked into attempt receipt")
				}
				return
			}
			sendClientReply(t, probe, f.capability(probe))
			create := f.next(t, "create")
			if phase == "starting" {
				reply := clientStatus(create)
				reply.Phase = "starting"
				sendClientReply(t, create, reply)
				_ = f.next(t, "ack")
			}
			if phase == "probe" || phase == "create" || phase == "starting" {
				if phase == "create" {
					r.cancel()
				}
				result, err := r.finish(t)
				if time.Since(began) > time.Second {
					t.Fatalf("admission reset absolute budget: %v", time.Since(began))
				}
				if !result.CreateSent || result.Executed || errors.Is(err, ErrUnsupported) || (!errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)) {
					t.Fatalf("admission cancellation: %+v %v", result, err)
				}
				return
			}
			// Finish this transaction before separately exercising exported
			// APIs: they must preserve supplied CallIDs and the sent boundary.
			sendClientReply(t, create, terminalClientStatus(create))
			_ = settleClient(t, f, r)
			if phase == "legacy_confirmed" {
				cfg.LegacyConfirmed = true
				_, err := ProbeWithin(t.Context(), f.nc, f.subject, proto.ProcessResumeReq{Version: 1, Op: "probe"}, cfg)
				if !errors.Is(err, ErrUnsupported) {
					t.Fatal(err)
				}
				return
			}
			if phase == "invalid_budget" {
				cfg.Deadline = 0
			}
			if phase == "expired_budget" {
				cfg.Deadline = 1
			}
			if phase == "private_inbox" {
				nc, err := nats.Connect(f.server.ClientURL())
				if err != nil {
					t.Fatal(err)
				}
				defer nc.Close()
				_, err = ProbeWithin(t.Context(), nc, f.subject, probe.req, cfg)
				if err == nil || errors.Is(err, ErrUnsupported) {
					t.Fatalf("unknown inbox allowed legacy: %v", err)
				}
				return
			}
			if phase == "post_create_unsupported" {
				cfg.LegacyConfirmed = true
			}
			if phase == "probe_api" || phase == "create_api" || phase == "post_create_unsupported" || phase == "recovering" || phase == "wrong_capability" || phase == "post_create_identity" {
				done := make(chan error, 1)
				go func() {
					var err error
					if phase == "probe_api" || phase == "wrong_capability" {
						_, err = ProbeWithin(t.Context(), f.nc, f.subject, probe.req, cfg)
					} else {
						_, err = CreateWithin(t.Context(), f.nc, f.subject, create.req, cfg)
					}
					done <- err
				}()
				op := "create"
				if phase == "probe_api" || phase == "wrong_capability" {
					op = "probe"
				}
				ex := f.next(t, op)
				if ex.req.CallID != map[string]uint64{"probe": probe.req.CallID, "create": create.req.CallID}[op] {
					t.Fatal("exported API changed supplied CallID")
				}
				if phase == "recovering" {
					sendClientReply(t, ex, proto.ProcessResumeReply{Version: 1, CallID: ex.req.CallID, Kind: "error", Code: proto.CodeProcessRecovering, Error: "retained recovery"})
					retry := f.next(t, "create")
					if !reflect.DeepEqual(ex.req, retry.req) {
						t.Fatal("recovery renewed handle or CallID")
					}
					ex = retry
				}
				switch phase {
				case "probe_api", "wrong_capability":
					reply := f.capability(ex)
					if phase == "wrong_capability" {
						reply.Lease.ClientLineage = "foreign-lineage"
					}
					sendClientReply(t, ex, reply)
				case "post_create_identity":
					reply := clientStatus(ex)
					reply.RequestID = "foreign-request"
					sendClientReply(t, ex, reply)
				case "post_create_unsupported":
					sendClientReply(t, ex, proto.ProcessResumeReply{Version: 1, CallID: ex.req.CallID, Kind: "error", Code: proto.CodeProcessUnsupported, Error: "unsupported process protocol"})
				default:
					reply := clientStatus(ex)
					reply.Phase = "starting"
					sendClientReply(t, ex, reply)
					next := f.next(t, "resume")
					if !reflect.DeepEqual(next.req.Handle, ex.req.Handle) || next.req.Exec != nil {
						t.Fatal("starting receipt rebuilt create")
					}
					sendClientReply(t, next, clientStatus(next))
				}
				err := <-done
				if phase == "post_create_unsupported" {
					var pe *ProtocolError
					if errors.Is(err, ErrUnsupported) || !errors.As(err, &pe) || !pe.AfterCreate {
						t.Fatalf("sent create allowed fallback: %v", err)
					}
				} else if phase == "wrong_capability" || phase == "post_create_identity" {
					if err == nil || errors.Is(err, ErrUnsupported) {
						t.Fatalf("foreign admission accepted: %v", err)
					}
				} else if err != nil {
					t.Fatal(err)
				}
				return
			}
			if phase == "slow_backoff" {
				cfg = admissionConfig(t, 50*time.Millisecond)
				o := cfg.Observer.(*admissionObserver)
				o.beginDelay = 100 * time.Millisecond
				o.delayPhase = controlbudget.PhaseBackoff
				ctx, cancel := context.WithTimeout(t.Context(), time.Second)
				defer cancel()
				done := make(chan error, 1)
				go func() { _, err := CreateWithin(ctx, f.nc, f.subject, create.req, cfg); done <- err }()
				ex := f.next(t, "create")
				sendClientReply(t, ex, proto.ProcessResumeReply{Version: 1, CallID: ex.req.CallID, Kind: "error", Code: proto.CodeProcessRecovering, Error: "retained recovery"})
				if err := <-done; !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("slow backoff extended admission: %v", err)
				}
				sawTimeout := false
				for _, a := range o.snapshot() {
					if a.Phase == controlbudget.PhaseBackoff && a.Class == "timeout" && a.BackoffNS == 0 {
						sawTimeout = true
					}
				}
				if !sawTimeout {
					t.Fatal("expired observer started backoff timer")
				}
				return
			}
			if phase == "invalid_probe" || phase == "invalid_create" || phase == "early_context" || phase == "slow_observer" {
				ctx := t.Context()
				if phase == "early_context" {
					var cancel context.CancelFunc
					ctx, cancel = context.WithTimeout(ctx, 20*time.Millisecond)
					defer cancel()
				}
				if phase == "slow_observer" {
					cfg = admissionConfig(t, 50*time.Millisecond)
					cfg.Observer.(*admissionObserver).beginDelay = 100 * time.Millisecond
				}
				var err error
				switch phase {
				case "invalid_probe":
					bad := probe.req
					bad.Exec = create.req.Exec
					_, err = ProbeWithin(ctx, f.nc, f.subject, bad, cfg)
				case "invalid_create":
					bad := create.req
					bad.Handle = nil
					_, err = CreateWithin(ctx, f.nc, f.subject, bad, cfg)
				default:
					_, err = CreateWithin(ctx, f.nc, f.subject, create.req, cfg)
				}
				if err == nil || errors.Is(err, ErrUnsupported) {
					t.Fatalf("invalid/expired admission accepted: %v", err)
				}
				if phase == "early_context" || phase == "slow_observer" {
					if !errors.Is(err, context.DeadlineExceeded) {
						t.Fatal(err)
					}
				}
				if phase == "invalid_probe" || phase == "invalid_create" || phase == "slow_observer" {
					if _, err := f.sub.NextMsg(20 * time.Millisecond); !errors.Is(err, nats.ErrTimeout) {
						t.Fatalf("refused admission published: %v", err)
					}
				}
				return
			}
			o := cfg.Observer.(*admissionObserver)
			o.failBegin, o.failRecord = phase == "observer_begin", phase == "observer_record"
			if phase == "observer_record" {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				done := make(chan error, 1)
				go func() { _, err := CreateWithin(ctx, f.nc, f.subject, create.req, cfg); done <- err }()
				ex := f.next(t, "create")
				sendClientReply(t, ex, terminalClientStatus(ex))
				if err := <-done; err == nil || !strings.Contains(err.Error(), "observer record") {
					t.Fatalf("silent observer loss: %v", err)
				}
			} else {
				// Bound the adversarial call independently so a broken absolute-clock
				// guard fails through assertions rather than stranding a test worker.
				ctx, cancel := context.WithTimeout(t.Context(), time.Second)
				defer cancel()
				_, err := CreateWithin(ctx, f.nc, f.subject, create.req, cfg)
				if err == nil || errors.Is(err, ErrUnsupported) {
					t.Fatalf("invalid budget/observer accepted: %v", err)
				}
				if phase == "observer_begin" && !strings.Contains(err.Error(), "observer begin") {
					t.Fatalf("observer failure became a transport timeout: %v", err)
				}
				if phase == "invalid_budget" && !strings.Contains(err.Error(), "invalid admission budget") {
					t.Fatalf("zero budget reached transport: %v", err)
				}
				if phase == "expired_budget" && !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("wrong expired budget error: %v", err)
				}
				if _, err := f.sub.NextMsg(20 * time.Millisecond); !errors.Is(err, nats.ErrTimeout) {
					t.Fatalf("refused admission published: %v", err)
				}
			}
		})
	}
}
