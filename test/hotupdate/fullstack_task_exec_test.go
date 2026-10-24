//go:build linux && hotupdate_integration

package hotupdate_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/natsinbox"
	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradecli"
	"github.com/nats-io/nats.go"
)

// The peer retains one authenticated connection, inbox, handle and attachment.
// Reply retry never probes or creates a replacement task. The actual ctl below
// independently covers the user-facing command's original waitpid.
type fullstackTaskPeer struct {
	nc            *nats.Conn
	sub           *nats.Subscription
	subject, kind string
	handle        proto.ProcessHandle
	call          uint64
	out, stderr   []byte
	ack, errACK   uint64
	fin, errFIN   bool
	last          proto.ProcessResumeReply
	create        proto.ProcessResumeReq
}

const fullstackTaskRelease = "s10-task-mechanism"

// Challenge the unmodified workspace product before any upgrade CLI dependency.
// S7's overlay-only enablers cannot activate the production custody reader.
// origin: S10 internal review TAGGED-SUITE-SWEEP-B F1
func TestFullstackRuntimeSampleUsesProductCustody(t *testing.T) {
	artifacts := buildFullstackObservationArtifact(t)
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) { exerciseFullstackRuntimeSample(t, artifacts) })
	}
	testharness.AssertNoGoroutineLeak(t, "product runtime custody reader x5", g)
	testharness.AssertNoFDLeak(t, "product runtime custody reader x5", fd, 0)
}

// These lifecycle guards never cut an image. Building and packing an unused
// successor duplicates large race artifacts without exercising another path.
// origin: hot-update S10 plan §11.1 / TAGGED-SUITE-SWEEP-B F1/F5
func buildFullstackObservationArtifact(t *testing.T) transferArtifacts {
	t.Helper()
	a := buildHotUpdateArtifact(t, fullstackTaskRelease+"-original")
	return transferArtifacts{a: a.Path, shaA: a.SHA256, source: a.SourceCommit, dirty: a.SourceDigest}
}

func exerciseFullstackRuntimeSample(t *testing.T, artifacts transferArtifacts) {
	t.Helper()
	s := newFullstackTaskStack(t, artifacts)
	defer s.close()
	// Sampling must run on the registered capable fixture, rather than leaving
	// the later cut on the intentionally refused legacy migration route.
	// origin: S10 internal review TAGGED-SUITE-SWEEP-B F3
	for _, role := range []upgradecli.BootRole{
		upgradecli.AgentBootRole(s.agentHome, s.sid, s.nid, uint32(os.Geteuid())),
		upgradecli.BrokerBootRole(s.h.Root, filepath.Join(s.h.Root, "broker.yaml"), "", uint32(os.Geteuid())),
	} {
		plan, err := upgradecli.RegisteredJournal(role)
		transferMust(t, err)
		if !plan.Registered {
			t.Fatal("fullstack task fixture booted without its upgrade domain registration")
		}
	}
	// Verify the live broker/agent route can describe the registered runtime.
	// This also rejects a session whose placeholder PIN has no incarnation.
	peer := newFullstackTaskPeer(t, s, "")
	defer peer.close()
	id, err := cli.EnsureIdentity(s.home)
	transferMust(t, err)
	actor, err := auth.FingerprintFromActor(id.PublicKey)
	transferMust(t, err)
	control := upgradecli.NewNodeSession(s.sid, id.PublicKey, actor, func(ctx context.Context, subject string, body []byte) ([]byte, error) {
		reply, err := peer.nc.RequestWithContext(ctx, subject, body)
		if err != nil {
			return nil, err
		}
		return reply.Data, nil
	}, peer.close)
	_, err = upgradecli.DescribeNode(t.Context(), control, s.nid)
	transferMust(t, err)
	before, after := fullstackRuntimeSample(t, s), fullstackRuntimeSample(t, s)
	if before.Nonce == after.Nonce || before.Sequence >= after.Sequence || before.AtBootNS >= after.AtBootNS ||
		before.Binding != after.Binding || before.PID != after.PID || before.StartTicks != after.StartTicks || before.ExeSHA != after.ExeSHA {
		t.Fatal("fresh runtime challenges reused a sample or crossed a product image")
	}
	wantScope := sha256.Sum256([]byte(t.Name()))
	if before.ScopeDigest != fmt.Sprintf("%x", wantScope) || after.ScopeDigest != before.ScopeDigest {
		t.Fatal("runtime challenge lost its sealed scope")
	}
}

func newFullstackTaskPeer(t *testing.T, s *fullstackTaskStack, kind string) *fullstackTaskPeer {
	t.Helper()
	id, err := cli.EnsureIdentity(s.home)
	transferMust(t, err)
	nc, fallback, err := natsinbox.Connect(s.url, id.PublicKey, []nats.Option{
		nats.Nkey(id.PublicKey, func(nonce []byte) ([]byte, error) { return auth.SignWithSeed(id.Seed, nonce) }),
		nats.Name(cli.CtlNameForSession(s.sid)), nats.MaxReconnects(-1)})
	transferMust(t, err)
	if fallback {
		nc.Close()
		t.Fatal("SETUP-RED: task peer did not negotiate private inbox")
	}
	w := &fullstackTaskPeer{nc: nc, kind: kind, subject: proto.SubjCmdBy(s.sid, id.PublicKey, s.nid, proto.ProcessResumeVerb)}
	t.Cleanup(w.close)
	w.sub, err = nc.SubscribeSync(nc.NewInbox())
	transferMust(t, err)
	transferMust(t, nc.FlushTimeout(time.Second))
	lineage := fmt.Sprintf("fullstack-%d", time.Now().UnixNano())
	op := "probe"
	if kind == proto.ProcessKindPTY {
		op = proto.ProcessOpPTYProbe
	}
	r := fullstackRPC(t, w, proto.ProcessResumeReq{Op: op, ClientLineage: lineage})
	if r.Kind != "capability" || r.Lease == nil || r.Lease.SID != s.sid || r.Lease.NID != s.nid ||
		r.Lease.ClientLineage != lineage || kind == proto.ProcessKindPTY && r.PTYVersion != proto.PTYVersion {
		t.Fatal("SETUP-RED: reliable task capability/lease missing")
	}
	w.handle = proto.ProcessHandle{Lease: *r.Lease, RequestID: lineage}
	return w
}

func (w *fullstackTaskPeer) close() {
	if w.sub != nil {
		_ = w.sub.Unsubscribe()
	}
	if w.nc != nil {
		w.nc.Close()
	}
}

func fullstackRPC(t *testing.T, w *fullstackTaskPeer, req proto.ProcessResumeReq) proto.ProcessResumeReply {
	t.Helper()
	w.call++
	req.Version, req.CallID, req.Kind = proto.ProcessResumeVersion, w.call, w.kind
	if req.Op != "probe" && req.Op != proto.ProcessOpPTYProbe {
		req.Handle, req.AttachEpoch = &w.handle, 1
		if req.Op != "create" {
			req.StdoutACK, req.StderrACK = w.ack, w.errACK
			req.StdoutFIN, req.StderrFIN = w.fin, w.errFIN
		}
	}
	body, err := json.Marshal(req)
	transferMust(t, err)
	// Retransmit the exact request bytes inside one absolute budget. A late
	// reply for another call cannot advance ACK or consume a terminal.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		transferMust(t, w.nc.PublishRequest(w.subject, w.sub.Subject, body))
		ctx, cancel := context.WithDeadline(t.Context(), fullstackAttemptDeadline(deadline, time.Now().Add(500*time.Millisecond)))
		message, err := w.sub.NextMsgWithContext(ctx)
		cancel()
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, nats.ErrTimeout) {
			continue
		}
		transferMust(t, err)
		var reply proto.ProcessResumeReply
		transferMust(t, json.Unmarshal(message.Data, &reply))
		if reply.CallID != req.CallID {
			continue
		}
		if reply.Kind == "error" {
			t.Fatalf("original task request refused: op=%s code=%s", req.Op, reply.Code)
		}
		if reply.Kind == "status" {
			// A natural last-slave EOF aborts an already empty input direction.
			// It is loss only when accepted bytes remain unapplied; control errors
			// still fail even when the eventual task terminal would be successful.
			if reply.InputError != "" && reply.InputACK != reply.InputAccepted || reply.ControlError != "" || req.ControlSeq != 0 && reply.ControlACK != req.ControlSeq {
				t.Fatalf("original task lost input or its exact control receipt: op=%s control=%d/%d input_error=%q control_error=%q", req.Op, req.ControlSeq, reply.ControlACK, reply.InputError, reply.ControlError)
			}
			if reply.RequestID != w.handle.RequestID || reply.TargetInstance != w.handle.Lease.TargetInstance ||
				reply.TaskKind != w.kind || reply.AttachEpoch != 1 {
				t.Fatal("reply belongs to a replacement handle or task kind")
			}
			if w.last.PID != "" && (reply.PID != w.last.PID || w.last.TerminalID != "" && reply.TerminalID != w.last.TerminalID) {
				t.Fatal("original task PID or unique terminal changed")
			}
			fullstackAppend(t, &w.out, reply.Stdout)
			fullstackAppend(t, &w.stderr, reply.Stderr)
			w.last = reply
		}
		return reply
	}
	t.Fatalf("original task request exceeded its absolute budget: op=%s", req.Op)
	return proto.ProcessResumeReply{}
}

func fullstackAttemptDeadline(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

// Compare every overlapping replay byte; neither a count nor the sender's own
// digest detects duplicated/corrupted prefixes. ACK remains caller controlled.
func fullstackAppend(t *testing.T, accumulated *[]byte, output *proto.ProcessOutput) {
	t.Helper()
	if output == nil {
		return
	}
	if output.Error != "" || output.Offset > uint64(len(*accumulated)) {
		t.Fatal("task output was aborted or has a sequence gap")
	}
	end := output.Offset + uint64(len(output.Data))
	overlap := min(uint64(len(*accumulated)), end) - output.Offset
	if !bytes.Equal((*accumulated)[output.Offset:output.Offset+overlap], output.Data[:overlap]) {
		t.Fatal("task replay changed an already observed byte")
	}
	*accumulated = append(*accumulated, output.Data[overlap:]...)
}

func fullstackCreate(t *testing.T, w *fullstackTaskPeer, argv []string, input ...[]byte) {
	t.Helper()
	w.create = proto.ProcessResumeReq{Op: "create"}
	if w.kind == proto.ProcessKindPTY {
		w.create.Run = &proto.RunReq{Argv: argv, Cols: 80, Rows: 24, KAIntervalMS: 5000}
	} else {
		w.create.Exec = &proto.ExecReq{Argv: argv}
		if len(input) != 0 {
			w.create.Exec.Stdin = bytes.Clone(input[0])
		}
	}
	fullstackRPC(t, w, w.create)
}

func fullstackPoll(t *testing.T, w *fullstackTaskPeer, predicate func(proto.ProcessResumeReply) bool) {
	t.Helper()
	if !testharness.WaitFor(t, 10*time.Second, 10*time.Millisecond, func() bool {
		return predicate(fullstackRPC(t, w, proto.ProcessResumeReq{Op: "resume"}))
	}) {
		t.Fatal("original task failed to reach its required nonempty window")
	}
}

func fullstackTerminal(t *testing.T, w *fullstackTaskPeer, output, stderr []byte, code int) {
	t.Helper()
	fullstackPoll(t, w, func(r proto.ProcessResumeReply) bool {
		return r.TerminalID != "" && r.Stdout != nil && r.Stdout.FIN && r.Stderr != nil && r.Stderr.FIN
	})
	if !bytes.Equal(w.out, output) || !bytes.Equal(w.stderr, stderr) || w.last.ExitCode != code || w.last.Signal != 0 {
		t.Fatalf("original task bytes/terminal differ: stdout=%d stderr=%d exit=%d signal=%d", len(w.out), len(w.stderr), w.last.ExitCode, w.last.Signal)
	}
}

func fullstackSettle(t *testing.T, w *fullstackTaskPeer) {
	t.Helper()
	w.ack, w.errACK = uint64(len(w.out)), uint64(len(w.stderr))
	w.fin, w.errFIN = true, true
	terminal := w.last.TerminalID
	if terminal == "" {
		t.Fatal("cannot settle without the original terminal")
	}
	r := fullstackRPC(t, w, proto.ProcessResumeReq{Op: "terminal-ack", TerminalID: terminal})
	if r.Phase != "settled" || r.TerminalID != terminal {
		t.Fatal("terminal ACK did not retain the exact settled receipt")
	}
}

func fullstackFinish(t *testing.T, w *fullstackTaskPeer, output, stderr []byte, code int) {
	t.Helper()
	fullstackTerminal(t, w, output, stderr, code)
	fullstackSettle(t, w)
}

// origin: hot-update S10 plan §6 / §10.2 P21 / M01A/M02E/M03E/M05E/P/R/M11W/I/O
func TestFullstackTasksRetainCurrentRollbackState(t *testing.T) {
	t.Run("oracle", fullstackRetainedControls)
	parent := t
	var artifacts transferArtifacts
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) {
			if artifacts.a == "" {
				artifacts = buildFullstackTaskArtifacts(parent)
			}
			exerciseTaskRollback(t, artifacts)
		})
	}
	testharness.AssertNoGoroutineLeak(t, "fullstack current task rollback x5", g)
	testharness.AssertNoFDLeak(t, "fullstack current task rollback x5", fd, 0)
}

func buildFullstackTaskArtifacts(t *testing.T) transferArtifacts {
	t.Helper()
	artifact := buildHotUpdateArtifact(t, fullstackTaskRelease+"-original")
	candidate := buildHotUpdateArtifact(t, fullstackTaskRelease)
	a := transferArtifacts{a: artifact.Path, shaA: artifact.SHA256, source: artifact.SourceCommit, dirty: artifact.SourceDigest}
	a.b, a.shaB = candidate.Path, candidate.SHA256
	a.archiveA, a.archiveB = transferArchive(t, a.a), transferArchive(t, a.b)
	t.Logf("task-artifacts source=%s dirty=%s original=%s candidate=%s toolchain=%s flags=%q/%q GOFLAGS=%q", a.source, a.dirty, a.shaA, a.shaB, runtime.Version(), artifact.BuildArgs, candidate.BuildArgs, os.Getenv("GOFLAGS"))
	return a
}

func exerciseTaskRollback(t *testing.T, a transferArtifacts) {
	started := time.Now()
	s := newFullstackTaskStack(t, a)
	defer s.close()
	ctlRoot, ctlArgv := fullstackBusiness(t, "exec")
	ctlArgs := append([]string{"exec", "--home", s.home, "--nats-url", s.url, s.nid, "--"}, ctlArgv...)
	ctl := s.launch("original-exec-ctl", filepath.Join(s.h.Root, "ctl"), s.home, ctlArgs, false)
	fullstackChild(t, ctlRoot)
	root, argv := fullstackBusiness(t, "exec")
	original := newFullstackTaskPeer(t, s, "")
	// Exceed a normal 64KiB kernel pipe so the original input owner still has
	// an unapplied suffix when the business child is held at its read gate.
	input := bytes.Repeat([]byte("ordered-stdin\x00\xff!"), 16384)
	argv = append(argv, fmt.Sprint(len(input)))
	fullstackCreate(t, original, argv, input)
	id := fullstackChild(t, root)
	pipes := fullstackTaskPipes(t, s, id)
	fullstackPoll(t, original, func(_ proto.ProcessResumeReply) bool { return len(original.out) != 0 && len(original.stderr) != 0 })
	// A second create with the original identity must not repeat the child's
	// append-only start effect, even when the first reply was lost.
	fullstackRPC(t, original, original.create)
	fullstackTaskCycles(t, s, "")
	t.Logf("startup/io/five-cycle=%s", time.Since(started))
	fullstackTaskCut(t, s, "agent", false)
	fullstackCheckPipes(t, s, pipes)
	// These four responsibilities are admitted only after ACTIVE. A bootstrap
	// snapshot rollback would preserve the original but lose every one of them.
	live := newFullstackTaskPeer(t, s, "")
	liveRoot, liveArgv := fullstackBusiness(t, "exec")
	fullstackCreate(t, live, liveArgv)
	fullstackPoll(t, live, func(_ proto.ProcessResumeReply) bool {
		return bytes.Equal(live.out, []byte("PREFIX")) && bytes.Equal(live.stderr, []byte("STDERR_PREFIX"))
	})
	livePTY := newFullstackTaskPeer(t, s, proto.ProcessKindPTY)
	ptyRoot, ptyArgv := fullstackBusiness(t, "pty")
	fullstackCreate(t, livePTY, ptyArgv)
	fullstackPoll(t, livePTY, func(_ proto.ProcessResumeReply) bool { return bytes.Equal(livePTY.out, []byte("PREFIX")) })
	terminal := newFullstackTaskPeer(t, s, "")
	fullstackCreate(t, terminal, []string{"/usr/bin/python3", "-I", "-c", "import os; os.write(1,b'unacked'); raise SystemExit(23)"})
	fullstackTerminal(t, terminal, []byte("unacked"), nil, 23)
	settled := newFullstackTaskPeer(t, s, proto.ProcessKindPTY)
	fullstackCreate(t, settled, []string{"/usr/bin/python3", "-I", "-c", "import os; os.write(1,b'settled'); raise SystemExit(23)"})
	fullstackFinish(t, settled, []byte("settled"), nil, 23)
	peers := []*fullstackTaskPeer{original, live, livePTY, terminal, settled}
	before := fullstackRetained(peers)
	fullstackTaskCycles(t, s, "")
	fullstackTaskCycles(t, s, proto.ProcessKindPTY)
	now, err := procrequest.BootNow()
	transferMust(t, err)
	if now >= settled.handle.Lease.ExpiresBootNS {
		t.Fatal("settled PTY lease expired before the required rollback window")
	}
	fullstackTaskCut(t, s, "agent", true)
	fullstackCheckPipes(t, s, pipes)
	for _, peer := range peers {
		fullstackRPC(t, peer, proto.ProcessResumeReq{Op: "resume"})
	}
	transferMust(t, fullstackCheckRetained(before, fullstackRetained(peers)))
	fullstackGate(t, root, "drain")
	if !bytes.Equal(fullstackWaitFile(t, filepath.Join(root, "input")), input) {
		t.Fatal("original exec input lost or duplicated a pending suffix")
	}
	for _, pair := range []struct {
		peer *fullstackTaskPeer
		root string
	}{{original, root}, {live, liveRoot}, {livePTY, ptyRoot}} {
		fullstackGate(t, pair.root, "finish")
		errout := []byte(nil)
		if pair.peer.kind == "" {
			errout = []byte("STDERR_PREFIXSTDERR_TAIL")
		}
		fullstackFinish(t, pair.peer, []byte("PREFIXTAIL"), errout, 23)
		fullstackOnce(t, pair.root)
	}
	fullstackSettle(t, terminal)
	// A retained settled create returns the same receipt and never spawns.
	fullstackRPC(t, settled, settled.create)
	if settled.last.Phase != "settled" {
		t.Fatal("settled unexpired PTY was recreated after reverse")
	}
	if _, alive := fullstackChildState(id); alive {
		t.Fatal("settled original child remains live or zombie")
	}
	fullstackGate(t, ctlRoot, "finish")
	fullstackNativeWait(t, ctl, 23)
	stdout, err := os.ReadFile(ctl.Receipt().StdoutPath)
	transferMust(t, err)
	stderr, err := os.ReadFile(ctl.Receipt().StderrPath)
	transferMust(t, err)
	if !bytes.Equal(stdout, []byte("PREFIXTAIL")) || !bytes.Equal(stderr, []byte("STDERR_PREFIXSTDERR_TAIL")) {
		t.Fatal("original exec ctl did not deliver the exact two output streams")
	}
	fullstackOnce(t, ctlRoot)
	t.Logf("task lifecycle including cut/settle=%s", time.Since(started))
}

type fullstackRetainedTask struct {
	Handle                     proto.ProcessHandle
	Kind, PID, Terminal, Phase string
	Code, Signal               int
	Out, Err                   []byte
}

func fullstackRetained(peers []*fullstackTaskPeer) []fullstackRetainedTask {
	var rows []fullstackRetainedTask
	for _, w := range peers {
		rows = append(rows, fullstackRetainedTask{w.handle, w.kind, w.last.PID, w.last.TerminalID, w.last.Phase,
			w.last.ExitCode, w.last.Signal, bytes.Clone(w.out), bytes.Clone(w.stderr)})
	}
	return rows
}

func fullstackCheckRetained(before, after []fullstackRetainedTask) error {
	if len(before) != 5 || !reflect.DeepEqual(before, after) {
		return errors.New("current rollback changed retained task identity, kind, bytes or terminal")
	}
	if before[1].Kind != "" || before[2].Kind != "pty" || before[3].Terminal == "" || before[4].Phase != "settled" {
		return errors.New("current rollback omitted a required ACTIVE admission window")
	}
	return nil
}

// These are oracle controls, explicitly separate from the real product rounds.
// A green control cannot discharge an unavailable accepted-cut mutation.
func fullstackRetainedControls(t *testing.T) {
	// origin: S10 internal review TAGGED-SUITE-SWEEP-B F4
	t.Run("start-outcome", func(t *testing.T) {
		for _, args := range [][]string{
			{"node", "upgrade", "--tx-id", "original", "--artifact-receipt", "receipt"},
			{"node", "upgrade-rollback", "--tx-id", "original"},
			{"broker", "upgrade", "reload", "--tx-id", "original", "--artifact-receipt", "receipt"},
			{"broker", "upgrade", "rollback", "--tx-id", "original"},
		} {
			if !fullstackTaskStartUnsettled(args, 74) || fullstackTaskStartUnsettled(args, 75) || fullstackTaskStartUnsettled(args, 64) {
				t.Fatal("an uncertain Start must wait on its original handle; refusals must fail")
			}
		}
		for _, args := range [][]string{
			{"node", "upgrade", "--prepare-only"}, {"node", "upgrade-status", "--tx-id", "original"},
			{"node", "upgrade-wait", "--tx-id", "original"}, {"broker", "upgrade", "status", "--tx-id", "original"},
			{"broker", "upgrade", "wait", "--tx-id", "original"},
		} {
			if fullstackTaskStartUnsettled(args, 74) {
				t.Fatal("prepare, query or wait failure was accepted as an uncertain Start")
			}
		}
	})
	rows := make([]fullstackRetainedTask, 5)
	for i := range rows {
		rows[i] = fullstackRetainedTask{Handle: proto.ProcessHandle{RequestID: fmt.Sprint(i)}, PID: fmt.Sprint(i), Out: []byte("prefix")}
	}
	rows[2].Kind, rows[3].Terminal, rows[4].Kind, rows[4].Phase = "pty", "terminal", "pty", "settled"
	transferMust(t, fullstackCheckRetained(rows, rows))
	for _, field := range []string{"handle", "kind", "pid", "terminal", "exit", "signal", "stdout", "stderr", "phase", "missing"} {
		t.Run(field, func(t *testing.T) {
			changed := append([]fullstackRetainedTask(nil), rows...)
			switch field {
			case "handle":
				changed[1].Handle.RequestID = "replacement"
			case "kind":
				changed[2].Kind = ""
			case "pid":
				changed[1].PID = "replacement"
			case "terminal":
				changed[3].Terminal = "replacement"
			case "exit":
				changed[3].Code++
			case "signal":
				changed[3].Signal++
			case "stdout":
				changed[0].Out = []byte("prefiy")
			case "stderr":
				changed[0].Err = []byte("duplicate")
			case "phase":
				changed[4].Phase = "running"
			case "missing":
				changed = changed[:4]
			}
			if fullstackCheckRetained(rows, changed) == nil {
				t.Fatal("retained-state oracle accepted " + field)
			}
		})
	}
}
