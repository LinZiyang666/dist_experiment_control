//go:build linux

package cli_e2e_test

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
	"golang.org/x/sys/unix"
)

// The shared lease supervisor owns parent-death cleanup across worker exec.
// Keep the target group for the fixture's existing explicit signal oracles.
// origin: hot-update S6 plan §9.3 / FIX-ORPHAN CLI fixtures.
func agentProcessAttributes() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{Setpgid: true}
}
func identityStillAlive(id ptyIdentity) bool {
	ticks, err := upgradeProcessIdentity(id.PID)
	return err == nil && ticks == id.Ticks
}

// Each issued op and legacy run is independently observed before broker
// forwarding. An old broker may reject pty-probe before it reaches an agent;
// an audit string alone cannot prove whether the client sent that probe.
type ptyWireTrace struct {
	mu       sync.Mutex
	requests []proto.ProcessResumeReq
	legacy   int
}

func tracePTYWire(t *testing.T, s *realStack) *ptyWireTrace {
	t.Helper()
	nc := connectPTYObserver(t, s)
	trace := &ptyWireTrace{}
	_, err := nc.Subscribe(proto.SubjCmdBy(s.sid, s.ctlID.PublicKey, s.nid, "*"), func(m *nats.Msg) {
		trace.mu.Lock()
		defer trace.mu.Unlock()
		if strings.Contains(m.Subject, "."+proto.ProcessResumeVerb+".") {
			var req proto.ProcessResumeReq
			if json.Unmarshal(m.Data, &req) == nil {
				trace.requests = append(trace.requests, req)
			}
		}
		if strings.Contains(m.Subject, ".run.") {
			trace.legacy++
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := nc.FlushTimeout(2 * time.Second); err != nil {
		t.Fatal(err)
	}
	return trace
}
func (tr *ptyWireTrace) snapshot() ([]proto.ProcessResumeReq, int) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return append([]proto.ProcessResumeReq(nil), tr.requests...), tr.legacy
}
func (tr *ptyWireTrace) requireOriginalHandle(t *testing.T) {
	t.Helper()
	requests, legacy := tr.snapshot()
	if legacy != 0 {
		t.Fatalf("reliable task rebuilt using %d legacy run(s)", legacy)
	}
	var handle *proto.ProcessHandle
	creates := 0
	for _, req := range requests {
		if req.Op == "create" {
			creates++
			if handle == nil {
				handle = req.Handle
			}
		}
		if req.Handle != nil && handle != nil && !samePTYHandle(req.Handle, handle) {
			t.Fatal("request changed original process handle")
		}
	}
	if creates == 0 || handle == nil {
		t.Fatal("no reliable create was observed")
	}
}

// origin: hot-update S4 plan §8.3 T35 / §4.2. B must advance input/output
// before its watchdog rolls back. A restored initial bundle fails this byte
// oracle because those B-only ACKs and bytes did not exist in that bundle.
// origin: hot-update S4 internal review rv-drill DRILL-8
// origin: S8 internal review upgrade-maketest-2 F2
func TestRunReliableDuplexAcrossUpgradeAndRollback(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	if runIsolatedReliableTest(t) {
		return
	}
	candidate := packUpgradeArtifact(t, mustReadFile(t, historicalPTYImage(t, true)))
	srv := serveUpgradeArtifacts(t, candidate)
	var commitFault sync.Once
	var fault atomic.Bool
	type markerReceipt struct {
		body []byte
		err  error
	}
	rollbackReceipt := make(chan markerReceipt, 1)
	s := newHistoricalPTYUpgradeStack(t, srv, func(s *realStack) {
		s.beforeAgent = func(s *realStack) {
			_, s.agentURL = startNATSRelay(t, s.url, func(subject string, body []byte) bool {
				// origin: hot-update S4 plan §8.3 / T35; S3 §21.3
				// The successful register clears a delivered rollback marker.
				// Capture the real durable receipt before forwarding that
				// register, rather than racing its intentional deletion.
				if strings.Contains(subject, ".register.req") && bytes.Contains(body, []byte(`"upgrade_state":"rolled_back"`)) {
					b, err := os.ReadFile(upgradeMarkerFile(s))
					select {
					case rollbackReceipt <- markerReceipt{body: b, err: err}:
					default:
					}
				}
				if strings.Contains(subject, ".register.req") && bytes.Contains(body, []byte(`"upgrade_state":"committed"`)) {
					commitFault.Do(func() {
						if err := os.Chmod(filepath.Dir(s.agentBin), 0o555); err == nil {
							fault.Store(true)
						}
					})
				}
				return false // transparent transport; only the marker write is faulted
			})
		}
	})
	t.Cleanup(func() { _ = os.Chmod(filepath.Dir(s.agentBin), 0o755) })
	agentPID := s.agentPID()
	agentTicks, err := upgradeProcessIdentity(agentPID)
	if err != nil {
		t.Fatal(err)
	}
	initialSHA := upgradeFileSHA(t, fmt.Sprintf("/proc/%d/exe", agentPID))
	f := newPTYFixture(t)
	trace := tracePTYWire(t, s)
	started := time.Now()
	ctl := startTerminalCtl(t, s, s.bin, s.url, f.argv("duplex")...)
	id := f.identity(t)
	if time.Since(started) > 5*time.Second {
		t.Fatalf("new run started after the 5 s budget: %s", time.Since(started))
	}
	ctl.expect(t, []byte("READY\n"))
	requirePTYReliable(t, s, 1)
	want := []byte("READY\n")
	advance := func(round int) {
		t.Helper()
		ctl.alive(t)
		payload := ptyPayload(round)
		ctl.frame(t, payload)
		want = append(want, ptyEcho(payload)...)
		ctl.expect(t, want)
		if !identityStillAlive(id) {
			t.Fatal("remote foreground PID/start ticks changed")
		}
		st, err := os.Stat(id.TTY)
		if err != nil {
			t.Fatal(err)
		}
		native := st.Sys().(*syscall.Stat_t)
		if uint64(native.Dev) != id.Dev || native.Ino != id.Ino || uint64(native.Rdev) != id.Rdev {
			t.Fatal("original remote slave identity changed")
		}
		trace.requireOriginalHandle(t)
		f.once(t)
		f.requireNoSignals(t)
	}
	advance(0)
	res := nodeUpgrade(t, s, srv, 0, candidate, "--wait=false")
	if res.rc != 0 {
		t.Fatalf("upgrade with live reliable PTY: %+v", res)
	}
	if !testharnessWait(20*time.Second, func() bool {
		return s.agentLogCount(`agent: image activated generation=1`) == 1 && s.agentLogCount(`agent: upgrade commit: marker write failed`) >= 1
	}) {
		t.Fatal("candidate did not activate with a failed durable commit")
	}
	if !fault.Load() {
		t.Fatal("commit write failure was not injected")
	}
	if got := upgradeFileSHA(t, fmt.Sprintf("/proc/%d/exe", agentPID)); got != candidate.binSHA {
		t.Fatal("B was not the running image")
	}
	var activeMarker upgradeImageMarker
	if err := json.Unmarshal(mustReadFile(t, upgradeMarkerFile(s)), &activeMarker); err != nil {
		t.Fatal(err)
	}
	if activeMarker.State != "pending" || activeMarker.Generation != 1 || activeMarker.ResourcePhase != "active" || activeMarker.ImportedResources != 1 || activeMarker.FreezeWindowMS <= 0 {
		t.Fatalf("B did not retain an active pending PTY image: %+v", activeMarker)
	}

	if err := os.Chmod(filepath.Dir(s.agentBin), 0o755); err != nil {
		t.Fatal(err)
	}
	advance(1)
	// Keep exercising the original terminal throughout the real 120 s product
	// watchdog budget. Each round-trip is bounded by the five-second contract.
	deadline := time.Now().Add(upgradebudget.ComponentHandover + 30*time.Second)
	round := 2
	for upgradeFileSHA(t, fmt.Sprintf("/proc/%d/exe", agentPID)) != initialSHA {
		if time.Now().After(deadline) {
			t.Fatal("active B did not watchdog-rollback to A")
		}
		start := time.Now()
		advance(round)
		if time.Since(start) > 5*time.Second {
			t.Fatalf("terminal interruption exceeded 5 s: %s", time.Since(start))
		}
		round++
		// sleep-fixture: workload cadence; readiness uses bytes, SHA and marker.
		time.Sleep(time.Second)
	}
	if ticks, err := upgradeProcessIdentity(agentPID); err != nil || ticks != agentTicks || s.agentPID() != agentPID {
		t.Fatal("agent was restarted rather than execed")
	}
	if !testharnessWait(15*time.Second, func() bool { return s.agentLogCount(`agent: image activated generation=2`) == 1 }) {
		t.Fatal("rollback did not activate a current-state generation")
	}
	var receipt markerReceipt
	select {
	case receipt = <-rollbackReceipt:
	case <-time.After(15 * time.Second):
		t.Fatal("rollback did not report its durable terminal marker")
	}
	if receipt.err != nil {
		t.Fatal(receipt.err)
	}
	var marker upgradeImageMarker
	if err := json.Unmarshal(receipt.body, &marker); err != nil {
		t.Fatal(err)
	}
	if marker.State != "rolled_back" || marker.Generation != 2 || marker.ImportedResources != 1 {
		t.Fatalf("watchdog receipt: %+v", marker)
	}
	maxFreeze := int64(0)
	for _, line := range strings.Split(string(mustReadFile(t, s.agentOut)), "\n") {
		if strings.Contains(line, "agent: image activated") {
			t.Logf("freeze calibration: %s", line)
			_, tail, ok := strings.Cut(line, "freeze_window_ms=")
			if !ok {
				t.Fatal("activation omitted the freeze-window sample")
			}
			fields := strings.Fields(tail)
			if len(fields) == 0 {
				t.Fatal("activation freeze-window sample is empty")
			}
			ms, err := strconv.ParseInt(fields[0], 10, 64)
			if err != nil || ms <= 0 {
				t.Fatal("activation freeze-window sample is invalid")
			}
			if ms > maxFreeze {
				maxFreeze = ms
			}
			if ms > 500 {
				t.Logf("freeze calibration EXCEEDED 500 ms: %d ms", ms)
			}
		}
	}
	t.Logf("freeze calibration maximum=%d ms (all raw samples retained)", maxFreeze)
	requireUpgradeFDs(t, agentPID)
	advance(round)
	ctl.frame(t, []byte("EXIT"))
	want = append(want, []byte("TAIL-OUT\nTAIL-ERR\n")...)
	ctl.wait(t, 23)
	ctl.expect(t, want)
	f.requireNoSignals(t)
	trace.requireOriginalHandle(t)
	f.once(t)
	requirePTYReliable(t, s, 1)
	requirePTYResourcesReleased(t, s, id)
}

func mustReadFile(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func requirePTYResourcesReleased(t *testing.T, s *realStack, id ptyIdentity) {
	t.Helper()
	if !testharnessWait(15*time.Second, func() bool {
		if _, err := os.Stat(fmt.Sprintf("/proc/%d", id.PID)); !os.IsNotExist(err) {
			return false
		}
		entries, err := os.ReadDir(fmt.Sprintf("/proc/%d/fd", s.agentPID()))
		if err != nil {
			return false
		}
		for _, entry := range entries {
			target, err := os.Readlink(fmt.Sprintf("/proc/%d/fd/%s", s.agentPID(), entry.Name()))
			if err == nil && (strings.Contains(target, "memfd:tether-replay") || strings.Contains(target, "memfd:tether-handoff")) {
				return false
			}
			if err == nil && strings.Contains(target, "anon_inode:[pidfd]") {
				return false
			}
			info, infoErr := os.ReadFile(fmt.Sprintf("/proc/%d/fdinfo/%s", s.agentPID(), entry.Name()))
			if infoErr == nil {
				for _, line := range strings.Split(string(info), "\n") {
					fields := strings.Fields(line)
					if len(fields) == 2 && fields[0] == "tty-index:" && fields[1] == filepath.Base(id.TTY) {
						return false
					}
				}
			}
		}
		return true
	}) {
		t.Fatal("settled PTY retained child/zombie/replay/handoff resources")
	}
}

// wirePTY is a strict peer used for explicit controls that the run command
// has no flag for. The agent remains the built product process; requests go
// through the real broker, authenticated private inbox and runtime.
type wirePTY struct {
	nc          *nats.Conn
	ctx         context.Context
	inbox       string
	sub         *nats.Subscription
	subject     string
	lineage     string
	handle      proto.ProcessHandle
	call        uint64
	ack         uint64
	stdoutFIN   bool
	stdoutError string
	output      []byte
	last        proto.ProcessResumeReply
}

func newWirePTY(t *testing.T, s *realStack, url string) *wirePTY {
	t.Helper()
	w := &wirePTY{nc: connectPTYPeer(t, s, url), subject: proto.SubjCmdBy(s.sid, s.ctlID.PublicKey, s.nid, proto.ProcessResumeVerb), lineage: fmt.Sprintf("s4-%d", time.Now().UnixNano())}
	// origin: hot-update S4 plan §2.1 / T36
	// AttachEpoch=1 names one inbox for the entire attachment, including
	// reconnects. Conn.Request allocates a new inbox and makes a retry stale.
	w.ctx, w.inbox = t.Context(), w.nc.NewInbox()
	var err error
	w.sub, err = w.nc.SubscribeSync(w.inbox)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.sub.Unsubscribe() })
	if err := w.nc.Flush(); err != nil {
		t.Fatal(err)
	}
	for _, op := range []string{"probe", proto.ProcessOpPTYProbe} {
		req := proto.ProcessResumeReq{Op: op, ClientLineage: w.lineage}
		r := w.rpc(t, req)
		if r.Kind != "capability" || r.PTYVersion != proto.PTYVersion || r.Lease == nil || r.Lease.SID != s.sid || r.Lease.NID != s.nid || r.Lease.ClientLineage != w.lineage || r.Lease.TargetInstance == "" {
			t.Fatalf("invalid %s PTY capability: kind=%q version=%d code=%q error=%q", op, r.Kind, r.PTYVersion, r.Code, r.Error)
		}
		w.handle = proto.ProcessHandle{Lease: *r.Lease, RequestID: "request-" + w.lineage}
	}
	return w
}
func (w *wirePTY) request(req proto.ProcessResumeReq) proto.ProcessResumeReq {
	w.call++
	req.Version = proto.ProcessResumeVersion
	req.CallID = w.call
	req.Kind = proto.ProcessKindPTY
	if req.Op != "probe" && req.Op != proto.ProcessOpPTYProbe {
		req.Handle = &w.handle
		req.AttachEpoch = 1
		req.StdoutACK = w.ack
		req.StdoutFIN = w.stdoutFIN
		req.StderrFIN = true
	}
	if req.Op == "create" {
		req.StdoutACK = 0
		req.StdoutFIN = false
		req.StderrFIN = false
	}
	return req
}
func (w *wirePTY) send(req proto.ProcessResumeReq) (proto.ProcessResumeReply, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return proto.ProcessResumeReply{}, err
	}
	if err := w.nc.PublishRequest(w.subject, w.inbox, body); err != nil {
		return proto.ProcessResumeReply{}, err
	}
	ctx, cancel := context.WithTimeout(w.ctx, 2*time.Second)
	defer cancel()
	for {
		msg, err := w.sub.NextMsgWithContext(ctx)
		if err != nil {
			return proto.ProcessResumeReply{}, err
		}
		var r proto.ProcessResumeReply
		if err := json.Unmarshal(msg.Data, &r); err != nil {
			return r, err
		}
		if r.CallID == req.CallID {
			return r, nil
		}
	}
}
func (w *wirePTY) rpc(t *testing.T, req proto.ProcessResumeReq) proto.ProcessResumeReply {
	t.Helper()
	r, err := w.send(w.request(req))
	if err != nil {
		t.Fatal(err)
	}
	if r.Kind == "status" {
		w.consume(t, r)
	}
	return r
}
func (w *wirePTY) consume(t *testing.T, r proto.ProcessResumeReply) {
	t.Helper()
	requirePTYStatus(t, r)
	if r.Stdout != nil {
		if r.Stdout.Offset != w.ack {
			t.Fatalf("merged stdout out of order: offset=%d ACK=%d", r.Stdout.Offset, w.ack)
		}
		w.output = append(w.output, r.Stdout.Data...)
		w.ack += uint64(len(r.Stdout.Data))
		w.stdoutFIN = r.Stdout.FIN
		w.stdoutError = r.Stdout.Error
	}
	w.last = r
}
func (w *wirePTY) create(t *testing.T, f *ptyFixture, mode string) {
	t.Helper()
	r := w.rpc(t, proto.ProcessResumeReq{Op: "create", Run: &proto.RunReq{Argv: f.argv(mode), Cols: 80, Rows: 24, KAIntervalMS: 5000}})
	requirePTYStatus(t, r)
}
func (w *wirePTY) pollUntil(t *testing.T, pred func(proto.ProcessResumeReply) bool) {
	t.Helper()
	if !testharnessWait(15*time.Second, func() bool { return pred(w.rpc(t, proto.ProcessResumeReq{Op: "resume"})) }) {
		t.Fatal("PTY status did not reach expected state")
	}
}
func (w *wirePTY) settle(t *testing.T) proto.ProcessResumeReply {
	t.Helper()
	w.pollUntil(t, func(r proto.ProcessResumeReply) bool {
		return r.TerminalID != "" && (w.stdoutFIN || w.stdoutError != "") && r.InputEnded
	})
	r := w.rpc(t, proto.ProcessResumeReq{Op: "terminal-ack", TerminalID: w.last.TerminalID})
	if r.Kind == "error" {
		t.Fatalf("terminal did not settle: %s %s", r.Code, r.Error)
	}
	return r
}

// origin: hot-update S4 plan §8.3 T36. Signal processing is observed before
// retry, and resize is changed by the child between attempts. Thus neither
// Unix signal coalescing nor setting the same size twice is a fake oracle.
func TestRunReliableLostRepliesDoNotDuplicateEffects(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	if runIsolatedReliableTest(t) {
		return
	}
	for _, op := range []string{"create", "input", "resize", "signal", "keepalive", "hangup", "terminal-ack"} {
		t.Run(op, func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			s := newRealStack(t, false)
			f := newPTYFixture(t)
			var armed atomic.Bool
			release := make(chan struct{})
			var releaseOnce sync.Once
			t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
			relay, url := startNATSReplyFence(t, s.url, func(subject string, body []byte) bool {
				var req proto.ProcessResumeReq
				return armed.Load() && strings.Contains(subject, "process-resume.req") && json.Unmarshal(body, &req) == nil && req.Op == op
			}, release, false)
			w := newWirePTY(t, s, url)
			trace := tracePTYWire(t, s)
			var req proto.ProcessResumeReq
			if op == "create" {
				req = proto.ProcessResumeReq{Op: op, Run: &proto.RunReq{Argv: f.argv("duplex"), Cols: 80, Rows: 24, KAIntervalMS: 5000}}
			} else {
				w.create(t, f, "duplex")
				f.identity(t)
				w.pollUntil(t, func(r proto.ProcessResumeReply) bool { return bytes.Equal(w.output, []byte("READY\n")) })
			}
			switch op {
			case "input":
				req = proto.ProcessResumeReq{Op: op, Input: ptyFrame(ptyPayload(0)), InputSeq: 0}
			case "resize":
				req = proto.ProcessResumeReq{Op: op, ControlSeq: 1, Resize: &proto.PtyResizeEvent{Cols: 137, Rows: 53}}
			case "signal":
				req = proto.ProcessResumeReq{Op: op, ControlSeq: 1, Signal: int(syscall.SIGINT)}
			case "keepalive", "hangup":
				req = proto.ProcessResumeReq{Op: op, ControlSeq: 1}
			case "terminal-ack":
				w.rpc(t, proto.ProcessResumeReq{Op: "input", Input: ptyFrame([]byte("EXIT")), InputFIN: true})
				w.pollUntil(t, func(r proto.ProcessResumeReply) bool { return r.TerminalID != "" && w.stdoutFIN && r.InputEnded })
				req = proto.ProcessResumeReq{Op: op, TerminalID: w.last.TerminalID}
			}
			armed.Store(true)
			sent := w.request(req)
			result := make(chan error, 1)
			go func() { _, err := w.send(sent); result <- err }()
			if !testharnessWait(10*time.Second, func() bool { return len(relay.receipts()) == 1 }) {
				t.Fatal("matched operation produced no swallowed reply")
			}
			loss := relay.requireLoss(t)
			var first proto.ProcessResumeReply
			if err := json.Unmarshal(loss.Reply, &first); err != nil {
				t.Fatal(err)
			}
			if first.CallID != sent.CallID {
				t.Fatal("relay swallowed a reply for another call")
			}
			if first.Kind != "status" {
				t.Fatalf("first operation failed: %s %s", first.Code, first.Error)
			}
			switch op {
			case "create":
				f.identity(t)
				f.once(t)
			case "input":
				if got := f.receipt(t, "inputs"); !bytes.Equal(got, ptyFrame(ptyPayload(0))) {
					t.Fatal("first input was not applied exactly once")
				}
			case "signal":
				if string(f.receipt(t, "signal-2")) != "1" {
					t.Fatal("first SIGINT was not handled")
				}
			case "resize":
				if string(f.receipt(t, "size")) != "53 137" {
					t.Fatal("first resize did not reach the kernel")
				}
				f.gate(t, "alter")
				if string(f.receipt(t, "altered")) != "41 103" {
					t.Fatal("resize adversary did not change size between retries")
				}
			case "keepalive":
				if first.ControlACK != 1 {
					t.Fatal("first keepalive was not committed")
				}
			case "hangup":
				if !first.InputEnded || first.InputError == "" {
					t.Fatal("first hangup did not close the input domain")
				}
			case "terminal-ack":
				if first.TerminalID != sent.TerminalID {
					t.Fatal("first settlement did not retain the terminal receipt")
				}
			}
			releaseOnce.Do(func() { close(release) })
			if err := <-result; err == nil {
				t.Fatal("the supposedly lost reply reached the requester")
			}
			if !testharnessWait(5*time.Second, w.nc.IsConnected) {
				t.Fatal("peer did not reconnect")
			}
			// Retry exactly the same operation/handle/byte or control sequence. Only
			// CallID and output acknowledgments may advance between transport calls.
			retry := sent
			w.call++
			retry.CallID = w.call
			r, err := w.send(retry)
			if err != nil {
				t.Fatal(err)
			}
			requirePTYStatus(t, r)
			if r.PID != first.PID || r.ControlACK != first.ControlACK || r.ControlError != first.ControlError {
				t.Fatal("duplicate changed the original receipt")
			}
			switch op {
			case "input":
				f.gate(t, "inspect")
				_ = f.receipt(t, "inspected")
				if got := mustReadFile(t, f.path("inputs")); !bytes.Equal(got, ptyFrame(ptyPayload(0))) {
					t.Fatal("duplicate input reached slave twice")
				}
			case "signal":
				// A second distinct signal handshake proves the first handler has
				// returned. Retry-induced SIGINT cannot hide behind signal coalescing.
				_ = syscall.Kill(firstPID(t, f), syscall.SIGTERM)
				_ = f.receipt(t, "signal-15")
				if string(mustReadFile(t, f.path("signal-2"))) != "1" {
					t.Fatal("duplicate SIGINT had a second effect")
				}
			case "resize":
				f.gate(t, "inspect")
				if !resizeSurvivesRetry("53 137", "41 103", string(f.receipt(t, "inspected"))) {
					t.Fatal("duplicate resize reapplied the original dimensions")
				}
			case "keepalive":
				if r.ControlACK != 1 {
					t.Fatal("duplicate keepalive consumed another sequence")
				}
			}
			f.once(t)
			trace.requireOriginalHandle(t)
			requirePTYReliable(t, s, 1)
			if op != "terminal-ack" {
				if op == "hangup" {
					f.gate(t, "finish")
				} else {
					w.consume(t, r)
					w.rpc(t, proto.ProcessResumeReq{Op: "input", InputSeq: w.last.InputAccepted, Input: ptyFrame([]byte("EXIT")), InputFIN: true})
				}
				w.settle(t)
			}
		})
	}
}
func firstPID(t *testing.T, f *ptyFixture) int {
	t.Helper()
	var id ptyIdentity
	if err := json.Unmarshal(mustReadFile(t, f.path("identity")), &id); err != nil {
		t.Fatal(err)
	}
	return id.PID
}

// origin: hot-update S4 plan §8.3 T37 / §3.2–3.3 / §6.2.
func TestRunReliableTerminalAndAbortSettlement(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	if runIsolatedReliableTest(t) {
		return
	}
	// origin: hot-update S4 plan §3.2 / T37. Force the credit/source-end
	// race explicitly: a late input packet must expose final A/Q as status,
	// never a top-level refusal that prevents the original tail settling.
	t.Run("input_after_source_end_returns_final_status", func(t *testing.T) {
		// origin: hot-update S5 PROC-LEAK / final process census.
		testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

		s := newRealStack(t, false)
		f := newPTYFixture(t)
		w := newWirePTY(t, s, s.url)
		w.create(t, f, "blocked")
		id := f.identity(t)
		w.pollUntil(t, func(r proto.ProcessResumeReply) bool { return r.Phase == "running" })
		for seq := uint64(0); seq < 2*upgradebudget.PTYBatchBytes; seq += upgradebudget.PTYBatchBytes {
			r := w.rpc(t, proto.ProcessResumeReq{Op: "input", InputSeq: seq, Input: bytes.Repeat([]byte("I"), upgradebudget.PTYBatchBytes)})
			requirePTYStatus(t, r)
		}
		w.pollUntil(t, func(r proto.ProcessResumeReply) bool {
			return r.InputAccepted-r.InputACK >= upgradebudget.PTYBatchBytes
		})
		f.gate(t, "finish")
		w.pollUntil(t, func(r proto.ProcessResumeReply) bool { return r.TerminalID != "" && r.InputEnded && w.stdoutFIN })
		final := w.last
		r := w.rpc(t, proto.ProcessResumeReq{Op: "input", InputSeq: final.InputAccepted, Input: []byte("late")})
		requirePTYStatus(t, r)
		if !r.InputEnded || r.InputError == "" || r.InputACK != final.InputACK || r.InputAccepted != final.InputAccepted || r.InputACK >= r.InputAccepted {
			t.Fatalf("late input changed final responsibility: before=%+v after=%+v", final, r)
		}
		if settled := w.settle(t); settled.Phase != "settled" || settled.InputError != final.InputError {
			t.Fatalf("late input prevented diagnosed settlement: %+v", settled)
		}
		if !bytes.Equal(w.output, []byte("READY\nTAIL-OUT\nTAIL-ERR\n")) {
			t.Fatalf("late input lost merged tail: %q", w.output)
		}
		f.once(t)
		requirePTYResourcesReleased(t, s, id)
	})
	t.Run("merged_tail_and_local_eof", func(t *testing.T) {
		// origin: hot-update S5 PROC-LEAK / final process census.
		testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

		s := newRealStack(t, false)
		f := newPTYFixture(t)
		trace := tracePTYWire(t, s)
		r, w, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = r.Close() }()
		defer func() { _ = w.Close() }()
		ctl := startFileRun(t, s, s.bin, s.url, r, nil, f.argv("eof")...)
		id := f.identity(t)
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		if !testharnessWait(10*time.Second, func() bool {
			reqs, _ := trace.snapshot()
			for _, req := range reqs {
				if req.Op == "input" && req.InputFIN {
					return true
				}
			}
			return false
		}) {
			t.Fatal("local EOF was not sent as logical InputFIN")
		}
		ctl.alive(t)
		if !identityStillAlive(id) {
			t.Fatal("logical EOF hung up the remote terminal")
		}
		f.gate(t, "finish")
		res := ctl.result(t)
		if res.rc != 0 || res.stdout != "READY\nTAIL-OUT\nTAIL-ERR\n" {
			t.Fatalf("merged final output after local EOF: %+v", res)
		}
		trace.requireOriginalHandle(t)
		requirePTYReliable(t, s, 1)
		requirePTYResourcesReleased(t, s, id)
	})
	t.Run("accepted_input_aborts_but_still_settles", func(t *testing.T) {
		// origin: hot-update S5 PROC-LEAK / final process census.
		testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

		s := newRealStack(t, false)
		f := newPTYFixture(t)
		trace := tracePTYWire(t, s)
		// The local raw terminal also has to restore after an A<Q abort.
		// The remote slave never reads, so input accepted by the actor exceeds
		// kernel-applied bytes while the original ctl is still alive.
		ctl := startTerminalCtl(t, s, s.bin, s.url, f.argv("blocked")...)
		id := f.identity(t)
		ctl.expect(t, []byte("READY\n"))
		status := observePTYReplies(t, s)
		writeDone := make(chan struct{})
		go func() {
			_, _ = ctl.master.Write(bytes.Repeat([]byte("I"), upgradebudget.PTYInputBytes*2))
			close(writeDone)
		}()
		t.Cleanup(func() { _ = ctl.master.Close(); <-writeDone })
		if !testharnessWait(15*time.Second, func() bool {
			for _, r := range status.snapshot() {
				// A single in-flight packet can become fully applied before
				// exit. Require a full batch behind the unread slave's kernel
				// queue so the final abort necessarily retains A<Q.
				if r.InputACK < r.InputAccepted && r.InputAccepted-r.InputACK >= upgradebudget.PTYBatchBytes {
					return true
				}
			}
			return false
		}) {
			t.Fatal("fixture never established A<Q")
		}
		f.gate(t, "finish")
		ctl.wait(t, 74)
		ctl.expect(t, []byte("READY\nTAIL-OUT\nTAIL-ERR\n"))
		replies := status.snapshot()
		abort, settled := false, false
		for _, r := range replies {
			if r.Kind == "status" && r.TaskKind == proto.ProcessKindPTY && r.Phase != "starting" {
				requirePTYStatus(t, r)
			}
			if r.InputEnded && r.InputError != "" && r.InputACK < r.InputAccepted {
				abort = true
			}
			if r.Phase == "settled" {
				settled = true
			}
		}
		if !abort || !settled {
			t.Fatalf("input abort returned before settlement: abort=%v settled=%v stderr=%q final replies=%+v", abort, settled, ctl.stderr.bytes(), replies[max(0, len(replies)-5):])
		}
		trace.requireOriginalHandle(t)
		requirePTYResourcesReleased(t, s, id)
	})
	t.Run("terminal_ack_reply_lost_until_lease_collected", func(t *testing.T) {
		// origin: hot-update S5 PROC-LEAK / final process census.
		testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

		s := newRealStack(t, false)
		f := newPTYFixture(t)
		trace := tracePTYWire(t, s)
		var drop atomic.Bool
		drop.Store(true)
		relay, url := startNATSReplyFence(t, s.url, func(subject string, body []byte) bool {
			var req proto.ProcessResumeReq
			return drop.Load() && strings.Contains(subject, "process-resume.req") && json.Unmarshal(body, &req) == nil && req.Op == "terminal-ack"
		}, nil, true)
		ctl := startTerminalCtl(t, s, s.bin, url, f.argv("duplex")...)
		id := f.identity(t)
		ctl.expect(t, []byte("READY\n"))
		ctl.frame(t, []byte("EXIT"))
		if !testharnessWait(15*time.Second, func() bool { return len(relay.receipts()) > 0 }) {
			t.Fatal("terminal-ack reply loss not injected")
		}
		loss := relay.requireLoss(t)
		var req proto.ProcessResumeReq
		var r proto.ProcessResumeReply
		if err := json.Unmarshal(loss.Request, &req); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(loss.Reply, &r); err != nil {
			t.Fatal(err)
		}
		if req.TerminalID == "" || r.Phase != "settled" || req.TerminalID != r.TerminalID || req.Handle == nil {
			t.Fatal("client did not possess a complete terminal before ACK loss")
		}
		// Do not shorten the product lease. Observe the real boot clock and then
		// prove collection with the real runtime's process_expired refusal.
		peer := connectPTYPeer(t, s, s.url)
		call := uint64(100000)
		expired := false
		deadline := time.Now().Add(time.Duration(req.Handle.Lease.ExpiresBootNS-req.Handle.Lease.IssuedBootNS) + 15*time.Second)
		for time.Now().Before(deadline) {
			var now unix.Timespec
			if err := unix.ClockGettime(unix.CLOCK_BOOTTIME, &now); err != nil {
				t.Fatal(err)
			}
			if now.Nano() > req.Handle.Lease.ExpiresBootNS {
				call++
				query := proto.ProcessResumeReq{Version: proto.ProcessResumeVersion, CallID: call, Kind: proto.ProcessKindPTY, Op: "resume", Handle: req.Handle, AttachEpoch: 1}
				body, _ := json.Marshal(query)
				msg, err := peer.Request(proto.SubjCmdBy(s.sid, s.ctlID.PublicKey, s.nid, proto.ProcessResumeVerb), body, time.Second)
				if err == nil {
					var reply proto.ProcessResumeReply
					if json.Unmarshal(msg.Data, &reply) == nil && reply.Code == proto.CodeProcessExpired {
						expired = true
						break
					}
				}
			}
			// sleep-fixture: poll an actual absolute lease deadline, never readiness.
			time.Sleep(100 * time.Millisecond)
		}
		if !expired {
			t.Fatal("settled record was not collected after its real lease expired")
		}
		drop.Store(false)
		ctl.wait(t, 23)
		ctl.expect(t, []byte("READY\nTAIL-OUT\nTAIL-ERR\n"))
		reqs, _ := trace.snapshot()
		acks := 0
		for _, retry := range reqs {
			if retry.Op == "terminal-ack" {
				acks++
				if retry.TerminalID != req.TerminalID || !reflect.DeepEqual(retry.Handle, req.Handle) {
					t.Fatal("lost ACK recreated or resettled another terminal")
				}
			}
		}
		if acks < 2 || len(relay.receipts()) < 2 {
			t.Fatal("terminal-ack was not retried through lease expiry")
		}
		requirePTYReliable(t, s, 1)
		f.once(t)
		requirePTYResourcesReleased(t, s, id)
	})
}

type ptyReplyTrace struct {
	mu      sync.Mutex
	replies []proto.ProcessResumeReply
}

func (tr *ptyReplyTrace) snapshot() []proto.ProcessResumeReply {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return append([]proto.ProcessResumeReply(nil), tr.replies...)
}
func observePTYReplies(t *testing.T, s *realStack) *ptyReplyTrace {
	t.Helper()
	tr := &ptyReplyTrace{}
	nc := connectPTYObserver(t, s)
	_, err := nc.Subscribe(auth.InboxPrefixFor(s.ctlID.PublicKey)+".>", func(m *nats.Msg) {
		var r proto.ProcessResumeReply
		if json.Unmarshal(m.Data, &r) == nil && r.Version == proto.ProcessResumeVersion && r.Kind != "" {
			tr.mu.Lock()
			tr.replies = append(tr.replies, r)
			tr.mu.Unlock()
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := nc.FlushTimeout(2 * time.Second); err != nil {
		t.Fatal(err)
	}
	return tr
}

type fileRun struct {
	cmd         *exec.Cmd
	out, stderr ptyCapture
	done        chan struct{}
	err         error
	cancel      context.CancelFunc
}

// Join the lease owner before publishing completion; Cmd.Wait alone leaves
// the supervisor's pidfd and descendant ownership outside the fixture.
// origin: hot-update S6 plan §9.3 / FIX-ORPHAN CLI fixtures.
func startFileRun(t *testing.T, s *realStack, bin, url string, in, out *os.File, argv ...string) *fileRun {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Minute)
	args := append([]string{"run", "--home", s.ctlHome, "--nats-url", url, s.nid, "--"}, argv...)
	p := &fileRun{cmd: exec.CommandContext(ctx, bin, args...), done: make(chan struct{}), cancel: cancel}
	p.cmd.Env = append(ctlEnv(), "TETHER_HOME="+s.ctlHome)
	p.cmd.Stdin = in
	p.cmd.Stdout = &p.out
	p.cmd.Stderr = &p.stderr
	if out != nil {
		p.cmd.Stdout = out
	}
	p.cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	wait, err := testharness.StartProcess(p.cmd)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	go func() { p.err = wait(); close(p.done) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-p.done:
		case <-time.After(5 * time.Second):
			_ = syscall.Kill(-p.cmd.Process.Pid, syscall.SIGKILL)
			<-p.done
		}
	})
	return p
}
func (p *fileRun) alive(t *testing.T) {
	t.Helper()
	select {
	case <-p.done:
		t.Fatalf("ctl exited early: %v stderr=%q", p.err, p.stderr.bytes())
	default:
	}
}
func (p *fileRun) result(t *testing.T) ctlResult {
	t.Helper()
	select {
	case <-p.done:
	case <-time.After(20 * time.Second):
		t.Fatal("file ctl failed to cancel/join its local I/O")
	}
	return ctlResult{rc: p.cmd.ProcessState.ExitCode(), stdout: string(p.out.bytes()), stderr: string(p.stderr.bytes())}
}

// origin: hot-update S4 plan §8.3 T39 / L3. Each exercise holds stdin or
// stdout blocked during external cancellation. Five real lifecycles make
// parent-side leaked readers/waiters visible to the shared leak gate.
func TestRunReliableLocalCancellationRestoresTerminal(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	if runIsolatedReliableTest(t) {
		return
	}
	// origin: hot-update S4 plan §7.3 / T39 / L3
	// signal.Notify broadcasts to all registered channels. A second handler
	// may race root cancellation yet pass an individual behavioral run; keep
	// the single-owner rule explicit as well as exercising real cancellation.
	source, err := parser.ParseFile(token.NewFileSet(), "../../cmd/tether/run.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	ast.Inspect(source, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || (selector.Sel.Name != "Notify" && selector.Sel.Name != "NotifyContext") {
			return true
		}
		owner, ok := selector.X.(*ast.Ident)
		if !ok || owner.Name != "signal" {
			return true
		}
		for _, arg := range call.Args {
			if sig, ok := arg.(*ast.SelectorExpr); ok && (sig.Sel.Name == "SIGINT" || sig.Sel.Name == "SIGTERM") {
				t.Fatal("run registers a second external cancellation signal handler")
			}
		}
		return true
	})
	buildTether(t)
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	const rounds = 5
	for round := 0; round < rounds; round++ {
		t.Run(fmt.Sprintf("round_%d", round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			for _, sig := range []syscall.Signal{syscall.SIGINT, syscall.SIGTERM} {
				t.Run(sig.String()+"_raw_blocked_stdin", func(t *testing.T) {
					// origin: hot-update S5 PROC-LEAK / final process census.
					testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

					s := newRealStack(t, false)
					f := newPTYFixture(t)
					ctl := startTerminalCtl(t, s, s.bin, s.url, f.argv("duplex")...)
					id := f.identity(t)
					ctl.expect(t, []byte("READY\n"))
					ctl.alive(t)
					if err := ctl.cmd.Process.Signal(sig); err != nil {
						t.Fatal(err)
					}
					ctl.wait(t, 74)
					requirePTYReliable(t, s, 1)
					// Local cancellation must finish before a HUP-immune remote workload
					// chooses to exit; os.Exit or a Wait-on-remote implementation fails.
					if !identityStillAlive(id) {
						t.Fatal("external local cancellation was mistaken for remote task exit")
					}
					f.gate(t, "finish")
				})
				t.Run(sig.String()+"_non_tty_blocked_both", func(t *testing.T) {
					// origin: hot-update S5 PROC-LEAK / final process census.
					testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

					s := newRealStack(t, false)
					f := newPTYFixture(t)
					inR, inW, err := os.Pipe()
					if err != nil {
						t.Fatal(err)
					}
					defer func() { _ = inR.Close() }()
					defer func() { _ = inW.Close() }()
					outR, outW, err := os.Pipe()
					if err != nil {
						t.Fatal(err)
					}
					defer func() { _ = outR.Close() }()
					defer func() { _ = outW.Close() }()
					// Prepare the blocking descriptors os/exec will inherit before
					// taking the baseline. Later observations only inspect flags.
					_ = inR.Fd()
					_ = outW.Fd()
					inFlags, err := localDescriptorFlags(inR)
					if err != nil {
						t.Fatal(err)
					}
					outFlags, err := localDescriptorFlags(outW)
					if err != nil {
						t.Fatal(err)
					}
					ctl := startFileRun(t, s, s.bin, s.url, inR, outW, f.argv("flood")...)
					f.identity(t)
					// The unread output pipe must actually fill, not just be capable of
					// blocking. FIONREAD supplies an independent kernel receipt.
					if !testharnessWait(10*time.Second, func() bool { n, err := unix.IoctlGetInt(int(outR.Fd()), unix.TIOCINQ); return err == nil && n >= 4096 }) {
						t.Fatal("local stdout was never blocked")
					}
					ctl.alive(t)
					if err := ctl.cmd.Process.Signal(sig); err != nil {
						t.Fatal(err)
					}
					res := ctl.result(t)
					if res.rc != 74 {
						t.Fatalf("local non-TTY signal competed with root cancellation: %+v", res)
					}
					gotIn, err := localDescriptorFlags(inR)
					if err != nil {
						t.Fatal(err)
					}
					gotOut, err := localDescriptorFlags(outW)
					if err != nil {
						t.Fatal(err)
					}
					if gotIn != inFlags || gotOut != outFlags {
						t.Fatalf("ctl mutated original FIFO OFD: in=%#x/%#x out=%#x/%#x", gotIn, inFlags, gotOut, outFlags)
					}
					requirePTYReliable(t, s, 1)
				})
			}
			t.Run("raw_ctrl_c_is_remote_signal", func(t *testing.T) {
				// origin: hot-update S5 PROC-LEAK / final process census.
				testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

				s := newRealStack(t, false)
				f := newPTYFixture(t)
				ctl := startTerminalCtl(t, s, s.bin, s.url, f.argv("signals")...)
				f.identity(t)
				ctl.expect(t, []byte("READY\n"))
				ctl.write(t, []byte{3})
				if string(f.receipt(t, "signal-2")) != "1" {
					t.Fatal("raw Ctrl-C was not delivered to the remote foreground")
				}
				ctl.alive(t)
				ctl.frame(t, []byte("EXIT"))
				ctl.wait(t, 23)
				ctl.expect(t, []byte("READY\nTAIL-OUT\nTAIL-ERR\n"))
			})
		})
	}
	testharness.AssertNoGoroutineLeak(t, "reliable run local cancellation", beforeG)
	testharness.AssertNoFDLeak(t, "reliable run local cancellation", beforeFD, 0)
}

// A source snapshot is extracted from the current repository's object store;
// no other worktree or installed binary is used. Old binaries are deliberately
// built from their own source, while the current binary inherits GOFLAGS
// overlays and the harness's race mode through tetherBuildCmd.
const ptyS3Snapshot = "0ec45bfeab0e614c1f900e8b29cb87e0091de981"

var ptyS3Binary string

func buildS3PTYBinary(t *testing.T) string {
	t.Helper()
	if ptyS3Binary != "" {
		return ptyS3Binary
	}
	out := buildHistoricalPTYBinary(t, ptyS3Snapshot, "s3", "v0.0.0-s3-snapshot")
	version, err := testharness.CombinedOutput(exec.Command(out, "version"))
	if err != nil {
		t.Fatalf("S3 version: %v %s", err, version)
	}
	if !bytes.Contains(version, []byte(proto.ImageABILine())) || bytes.Contains(version, []byte("pty-v1")) {
		t.Fatalf("S3 artifact is not the frozen exec-only ABI reader: %s", version)
	}
	t.Logf("S3 supply: commit=%s binary_sha256=%s race=%v", ptyS3Snapshot, upgradeFileSHA(t, out), raceEnabled)
	ptyS3Binary = out
	return out
}

// Extract historical source without touching another worktree or relying on
// installed/staged binaries. Each isolated process owns this immutable cache.
// origin: hot-update S4 plan §8.3 / T38; CLI-LATENCY complete N-1 supply.
func buildHistoricalPTYBinary(t *testing.T, revision, label, version string) string {
	t.Helper()
	buildTether(t)
	source := filepath.Join(tetherBinDir, label+"-source")
	if err := os.Mkdir(source, 0o700); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("git", "archive", revision)
	cmd.Dir = "../.."
	archive, err := testharness.Output(cmd)
	if err != nil {
		t.Fatalf("historical source %s unavailable: %v", revision, err)
	}
	rd := tar.NewReader(bytes.NewReader(archive))
	for {
		header, err := rd.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(source, header.Name)
		if !strings.HasPrefix(path, source+string(os.PathSeparator)) {
			t.Fatal("snapshot archive escaped its sandbox")
		}
		if header.Typeflag == tar.TypeDir {
			if err := os.MkdirAll(path, 0o700); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if header.Typeflag != tar.TypeReg {
			continue
		}
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, os.FileMode(header.Mode))
		if err != nil {
			t.Fatal(err)
		}
		_, copyErr := io.Copy(f, rd)
		closeErr := f.Close()
		if copyErr != nil || closeErr != nil {
			t.Fatalf("extract historical source: %v %v", copyErr, closeErr)
		}
	}
	out := filepath.Join(tetherBinDir, "tether-"+label)
	build := tetherBuildCmd(out)
	// Extracted source has no .git directory; bind identity to the explicit
	// snapshot SHA instead of Go walking into an unrelated parent VCS root.
	// A distinct release label lets the real node-upgrade CLI confirm register
	// rather than triggering its same-tag UNCONFIRMED branch.
	build.Args = append(build.Args[:len(build.Args)-1], "-buildvcs=false", "-ldflags", "-X github.com/LinZiyang666/tether/internal/proto.ReleaseVersion="+version, "./cmd/tether")
	build.Dir = source
	// A current-source overlay cannot transform the historical reader. Strip
	// only overlay flags; retain ordinary build flags and the race setting.
	build.Env = append(os.Environ(), "GOFLAGS="+withoutGoOverlay(os.Getenv("GOFLAGS")))
	if output, err := testharness.CombinedOutput(build); err != nil {
		t.Fatalf("build historical binary: %v\n%s", err, output)
	}
	return out
}

func withoutGoOverlay(flags string) string {
	fields := strings.Fields(flags)
	out := fields[:0]
	skip := false
	for _, f := range fields {
		if skip {
			skip = false
			continue
		}
		if f == "-overlay" {
			skip = true
			continue
		}
		if strings.HasPrefix(f, "-overlay=") {
			continue
		}
		out = append(out, f)
	}
	return strings.Join(out, " ")
}

// Match the staged previous-release rule: a post-release tree uses the
// highest reachable release; a clean exact release uses its predecessor.
// origin: hot-update S4 plan §9.5 / T38; test/simcluster/lib/stage.sh supply rule.
func buildPreviousPTYBinary(t *testing.T) string {
	t.Helper()
	listing, err := testharness.Output(exec.Command("git", "tag", "--list", "v*", "--merged", "HEAD", "--sort=-v:refname"))
	if err != nil {
		t.Fatal(err)
	}
	tags := strings.Fields(string(listing))
	if len(tags) == 0 {
		t.Fatal("no previous release tag is reachable from HEAD")
	}
	exact, exactErr := testharness.Output(exec.Command("git", "describe", "--tags", "--exact-match", "--match", "v*", "HEAD"))
	dirty, err := testharness.Output(exec.Command("git", "status", "--porcelain"))
	if err != nil {
		t.Fatal(err)
	}
	if exactErr == nil && len(bytes.TrimSpace(dirty)) == 0 && tags[0] == strings.TrimSpace(string(exact)) {
		tags = tags[1:]
	}
	if len(tags) == 0 {
		t.Fatal("the exact release has no previous release tag")
	}
	tag := tags[0]
	commit, err := testharness.Output(exec.Command("git", "rev-list", "-n1", tag))
	if err != nil {
		t.Fatal(err)
	}
	out := buildHistoricalPTYBinary(t, strings.TrimSpace(string(commit)), "previous", tag)
	supply := map[string]string{"tag": tag, "commit": strings.TrimSpace(string(commit)),
		"binary_sha256": upgradeFileSHA(t, out), "goos": runtime.GOOS, "goarch": runtime.GOARCH}
	body, err := json.Marshal(supply)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(out+"-source.json", body, 0o600); err != nil {
		t.Fatal(err)
	}
	return out
}

func previousPTYBinary(t *testing.T) string {
	t.Helper()
	path := filepath.Join("..", "simcluster", "vendor", "tether-prev")
	body, err := os.ReadFile(path + "-source.json")
	if os.IsNotExist(err) {
		// A clean worktree need not have deploy-tier vendor artifacts. Build
		// the same real previous release from its tag, in our private cache,
		// so these 24 compatibility arms never become optional coverage.
		// origin: hot-update S4 plan §8.3 / T38; CLI-LATENCY full matrix.
		path = buildPreviousPTYBinary(t)
		body, err = os.ReadFile(path + "-source.json")
	}
	if err != nil {
		t.Fatal(err)
	}
	var supply struct {
		Tag    string `json:"tag"`
		Commit string `json:"commit"`
		SHA    string `json:"binary_sha256"`
		GOOS   string `json:"goos"`
		GOARCH string `json:"goarch"`
	}
	if err := json.Unmarshal(body, &supply); err != nil {
		t.Fatal(err)
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		t.Fatal(err)
	}
	if supply.Tag == "" || len(supply.Commit) != 40 || supply.GOOS != runtime.GOOS || supply.GOARCH != runtime.GOARCH || upgradeFileSHA(t, absolute) != supply.SHA {
		t.Fatal("previous release artifact/source receipt mismatch")
	}
	commit, err := testharness.Output(exec.Command("git", "rev-list", "-n1", supply.Tag))
	if err != nil || strings.TrimSpace(string(commit)) != supply.Commit {
		t.Fatal("previous release supply is not bound to its release tag")
	}
	version, err := testharness.CombinedOutput(exec.Command(absolute, "version"))
	if err != nil || !bytes.Contains(version, []byte(supply.Tag)) {
		t.Fatalf("previous release version disagrees with its supply: %v %s", err, version)
	}
	t.Logf("previous release: tag=%s commit=%s SHA=%s", supply.Tag, supply.Commit, supply.SHA)
	return absolute
}

// origin: hot-update S4 plan §8.3 T38 / §9.5 / §2.2. All four quadrants
// are crossed with current, old-op and no-verb broker surfaces. The old-op
// broker is the real S3 binary with its own session/register authority.
// The no-verb surface denies the command subscription at the NATS server.
// origin: S8 internal review upgrade-maketest-2 F2
func TestRunReliableNMinusOneQuadrants(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	if runIsolatedReliableTest(t) {
		return
	}
	for _, oldKind := range []string{"s3", "previous_release"} {
		t.Run(oldKind, func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			var old string
			if oldKind == "s3" {
				old = buildS3PTYBinary(t)
			} else {
				old = previousPTYBinary(t)
			}
			for _, surface := range []string{"current", "old_ops", "no_verb"} {
				for _, ctlOld := range []bool{false, true} {
					for _, agentOld := range []bool{false, true} {
						t.Run(fmt.Sprintf("%s/ctl_old_%v/agent_old_%v", surface, ctlOld, agentOld), func(t *testing.T) {
							// origin: hot-update S5 PROC-LEAK / final process census.
							testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

							options := &realStack{callout: surface != "current", legacyBrokerSurface: surface == "no_verb"}
							if surface == "old_ops" {
								options.brokerBin = buildS3PTYBinary(t)
								options.diskDB = true
							}
							if agentOld {
								options.agentBin = old
							}
							s := bootRealStack(t, options)
							if surface == "no_verb" && s.brokerSubscribes("process-resume.req") {
								t.Fatal("old broker surface retained the new process handler")
							}
							trace := tracePTYWire(t, s)
							f := newPTYFixture(t)
							bin := s.bin
							if ctlOld {
								bin = old
							}
							ctl := startTerminalCtl(t, s, bin, s.url, f.argv("duplex")...)
							id := f.identity(t)
							ctl.expect(t, []byte("READY\n"))
							payload := ptyPayload(0)
							ctl.frame(t, payload)
							want := append([]byte("READY\n"), ptyEcho(payload)...)
							ctl.expect(t, want)
							ctl.frame(t, []byte("EXIT"))
							want = append(want, []byte("TAIL-OUT\nTAIL-ERR\n")...)
							ctl.wait(t, 23)
							ctl.expect(t, want)
							f.once(t)
							reqs, legacy := trace.snapshot()
							reliable := !ctlOld && !agentOld && surface == "current"
							creates, firstProbe, secondProbe := 0, 0, 0
							for _, req := range reqs {
								switch req.Op {
								case "create":
									creates++
								case "probe":
									firstProbe++
								case proto.ProcessOpPTYProbe:
									secondProbe++
								}
							}
							if reliable {
								if creates < 1 || firstProbe < 1 || secondProbe < 1 || legacy != 0 {
									t.Fatalf("new/new path: creates=%d probes=%d/%d legacy=%d", creates, firstProbe, secondProbe, legacy)
								}
								requirePTYReliable(t, s, 1)
								trace.requireOriginalHandle(t)
							} else {
								if creates != 0 || legacy != 1 {
									t.Fatalf("compatibility rebuilt or chose wrong path: creates=%d legacy=%d", creates, legacy)
								}
								if agentOld && secondProbe != 0 {
									t.Fatal("new ctl sent pty-probe to an agent with no PTYVersion")
								}
								if !ctlOld && surface != "no_verb" && firstProbe == 0 {
									t.Fatal("new ctl skipped the first capability probe")
								}
								if !ctlOld && !agentOld && surface == "old_ops" && secondProbe == 0 {
									t.Fatal("ordinary probe was mistaken for full broker path support")
								}
								if ctlOld && (firstProbe != 0 || secondProbe != 0) {
									t.Fatal("old ctl did not use its legacy wire surface")
								}
							}
							running := old
							if !agentOld {
								running = s.bin
							}
							if upgradeFileSHA(t, fmt.Sprintf("/proc/%d/exe", s.agentPID())) != upgradeFileSHA(t, running) {
								t.Fatal("quadrant used a different agent artifact")
							}
							if identityStillAlive(id) {
								t.Fatal("terminal child remained live after exit")
							}
						})
					}
				}
			}
		})
	}
	// Both reader checks use the same immutable historical artifact; packing
	// it again tests no image or admission behavior.
	// origin: hot-update S4 plan §8.3 / T38; CLI-LATENCY immutable setup.
	s3Candidate := packUpgradeArtifact(t, mustReadFile(t, buildS3PTYBinary(t)))
	t.Run("pure_exec_image_read_by_true_s3", func(t *testing.T) {
		// origin: hot-update S5 PROC-LEAK / final process census.
		testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

		candidate := s3Candidate
		srv := serveUpgradeArtifacts(t, candidate)
		s := newHistoricalPTYUpgradeStack(t, srv)
		pid := s.agentPID()
		ticks, err := upgradeProcessIdentity(pid)
		if err != nil {
			t.Fatal(err)
		}
		p := startUpgradeProducer(t, s)
		res := nodeUpgrade(t, s, srv, 0, candidate)
		if res.rc != 0 || !strings.Contains(res.stdout, "COMMITTED") {
			t.Fatalf("S3 rejected a pure exec image: %+v", res)
		}
		requireUpgradeReceipt(t, s, pid, ticks, candidate.binSHA, 1)
		p.release(t, 20)
		p.checkpoint(t, 40)
		p.finish(t, s)
	})
	t.Run("retained_pty_refuses_true_s3_reader", func(t *testing.T) {
		// origin: hot-update S5 PROC-LEAK / final process census.
		testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

		candidate := s3Candidate
		srv := serveUpgradeArtifacts(t, candidate)
		s := newHistoricalPTYUpgradeStack(t, srv)
		f := newPTYFixture(t)
		ctl := startTerminalCtl(t, s, s.bin, s.url, f.argv("duplex")...)
		f.identity(t)
		ctl.expect(t, []byte("READY\n"))
		requirePTYReliable(t, s, 1)
		before := upgradeFileSHA(t, s.agentBin)
		res := nodeUpgrade(t, s, srv, 0, candidate, "--wait=false")
		if res.rc != 64 || !strings.Contains(res.stderr, "image_abi_unsupported") || !strings.Contains(res.stderr, "pty-v1") {
			t.Fatalf("S3 reader accepted retained PTY state: %+v", res)
		}
		requireUpgradeUnchanged(t, s, before)
		input := ptyPayload(0)
		ctl.frame(t, input)
		want := append([]byte("READY\n"), ptyEcho(input)...)
		ctl.expect(t, want)
		ctl.frame(t, []byte("EXIT"))
		want = append(want, []byte("TAIL-OUT\nTAIL-ERR\n")...)
		ctl.wait(t, 23)
		ctl.expect(t, want)
	})

	t.Run("s3_to_s4_pending_advertises_but_refuses_pty_create", func(t *testing.T) {
		// origin: hot-update S5 PROC-LEAK / final process census.
		testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

		old := buildS3PTYBinary(t)
		current := packUpgradeArtifact(t, mustReadFile(t, historicalPTYImage(t, true)))
		srv := serveUpgradeArtifacts(t, current)
		var inject sync.Once
		var fault atomic.Bool
		s := newHistoricalPTYUpgradeStack(t, srv, func(s *realStack) {
			if err := os.WriteFile(s.agentBin, mustReadFile(t, old), 0o755); err != nil {
				t.Fatal(err)
			}
			s.beforeAgent = func(s *realStack) {
				_, s.agentURL = startNATSRelay(t, s.url, func(subject string, body []byte) bool {
					if strings.Contains(subject, ".register.req") && bytes.Contains(body, []byte(`"upgrade_state":"committed"`)) {
						inject.Do(func() {
							if err := os.Chmod(filepath.Dir(s.agentBin), 0o555); err == nil {
								fault.Store(true)
							}
						})
					}
					return false
				})
			}
		})
		t.Cleanup(func() { _ = os.Chmod(filepath.Dir(s.agentBin), 0o755) })
		producer := startUpgradeProducer(t, s)
		res := nodeUpgrade(t, s, srv, 0, current, "--wait=false")
		if res.rc != 0 {
			t.Fatalf("S3 to S4 upgrade: %+v", res)
		}
		if !testharnessWait(20*time.Second, func() bool {
			return s.agentLogCount(`agent: image activated generation=1`) == 1 && s.agentLogCount(`agent: upgrade commit: marker write failed`) >= 1
		}) || !fault.Load() {
			t.Fatal("S3 to S4 pending window was not established")
		}
		// origin: hot-update S4 plan §2.2 / §7.1 / T38
		// The failed commit is logged before the SHA-bound admission refresh
		// finishes and Run installs subscriptions. Starting the ctl at that log
		// tests an absent agent instead: its bounded probe can legitimately time
		// out and choose legacy. Observe the new agent's actual intake before
		// testing the advertised capability and pre-Reserve busy refusal.
		if !testharnessWait(20*time.Second, func() bool {
			connz, err := s.server().Connz(&natsserver.ConnzOptions{State: natsserver.ConnOpen, Subscriptions: true})
			if err != nil {
				return false
			}
			for _, conn := range connz.Conns {
				for _, subject := range conn.Subs {
					if subject == proto.SubjCmdForwarded(s.sid, s.nid, "*") {
						return true
					}
				}
			}
			return false
		}) {
			t.Fatal("pending S4 agent did not install its forwarded-command subscription")
		}
		if err := os.Chmod(filepath.Dir(s.agentBin), 0o755); err != nil {
			t.Fatal(err)
		}
		trace := tracePTYWire(t, s)
		replies := observePTYReplies(t, s)
		f := newPTYFixture(t)
		started := time.Now()
		ctl := startTerminalCtl(t, s, s.bin, s.url, f.argv("duplex")...)
		ctl.wait(t, 75)
		if elapsed := time.Since(started); elapsed > 5*time.Second {
			t.Fatalf("pending admission refusal was not prompt: %s", elapsed)
		}
		reqs, legacy := trace.snapshot()
		probes, creates := 0, 0
		var createCall uint64
		for _, req := range reqs {
			if req.Op == "probe" || req.Op == proto.ProcessOpPTYProbe {
				probes++
			}
			if req.Op == "create" {
				creates++
				createCall = req.CallID
			}
		}
		if probes != 2 || creates != 1 || legacy != 0 {
			t.Fatalf("pending hid capability or fell back: probes=%d creates=%d legacy=%d", probes, creates, legacy)
		}
		capabilities, refusals := 0, 0
		for _, reply := range replies.snapshot() {
			if reply.TaskKind != proto.ProcessKindPTY {
				continue
			}
			if reply.Kind == "capability" && reply.PTYVersion == proto.PTYVersion && reply.Lease != nil {
				capabilities++
			}
			if reply.CallID == createCall && reply.Kind == "error" && reply.Code == proto.CodeProcessBusy {
				refusals++
			}
		}
		if capabilities != 2 || refusals != 1 {
			t.Fatalf("pending did not advertise real PTY capability and refuse admission: capabilities=%d refusals=%d replies=%+v", capabilities, refusals, replies.snapshot())
		}
		if _, err := os.Stat(f.path("starts")); !os.IsNotExist(err) {
			t.Fatal("pending created a PTY unreadable by its S3 rollback target")
		}
		if !strings.Contains(string(ctl.stderr.bytes()), "process_busy") {
			t.Fatalf("pending create was not an admission refusal: %s", ctl.stderr.bytes())
		}
		requirePTYReliable(t, s, 0)
		producer.finish(t, s)
	})

	t.Run("old_broker_after_create_never_rebuilds", func(t *testing.T) {
		// origin: hot-update S5 PROC-LEAK / final process census.
		testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

		s := bootRealStack(t, &realStack{diskDB: true})
		f := newPTYFixture(t)
		trace := tracePTYWire(t, s)
		ctl := startTerminalCtl(t, s, s.bin, s.url, f.argv("duplex")...)
		f.identity(t)
		ctl.expect(t, []byte("READY\n"))
		requirePTYReliable(t, s, 1)
		s.stopBroker()
		s.brokerBin = buildS3PTYBinary(t)
		s.startBroker()
		ctl.frame(t, ptyPayload(0))
		ctl.wait(t, 74)
		trace.requireOriginalHandle(t)
		f.once(t)
		requirePTYReliable(t, s, 1)
	})
	t.Run("permission_after_create_never_rebuilds", func(t *testing.T) {
		// origin: hot-update S5 PROC-LEAK / final process census.
		testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

		s := newRealStack(t, false)
		f := newPTYFixture(t)
		trace := tracePTYWire(t, s)
		ctl := startTerminalCtl(t, s, s.bin, s.url, f.argv("duplex")...)
		f.identity(t)
		ctl.expect(t, []byte("READY\n"))
		requirePTYReliable(t, s, 1)
		if _, err := s.db.Exec(`DELETE FROM members WHERE sid=? AND pubkey_fp=?`, s.sid, s.ctlID.Fingerprint); err != nil {
			t.Fatal(err)
		}
		ctl.frame(t, ptyPayload(0))
		ctl.wait(t, 77)
		trace.requireOriginalHandle(t)
		f.once(t)
	})

	t.Run("permission_never_falls_back", func(t *testing.T) {
		// origin: hot-update S5 PROC-LEAK / final process census.
		testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

		s := newRealStack(t, false)
		trace := tracePTYWire(t, s)
		if _, err := s.db.Exec(`DELETE FROM members WHERE sid=? AND pubkey_fp=?`, s.sid, s.ctlID.Fingerprint); err != nil {
			t.Fatal(err)
		}
		f := newPTYFixture(t)
		ctl := startTerminalCtl(t, s, s.bin, s.url, f.argv("duplex")...)
		ctl.wait(t, 77)
		reqs, legacy := trace.snapshot()
		if legacy != 0 {
			t.Fatal("permission 77 fell back to legacy run")
		}
		for _, req := range reqs {
			if req.Op == "create" {
				t.Fatal("permission failure created a PTY")
			}
		}
		if _, err := os.Stat(f.path("starts")); !os.IsNotExist(err) {
			t.Fatal("denied request had a child side effect")
		}
	})
	t.Run("timeout_after_create_never_rebuilds", func(t *testing.T) {
		// origin: hot-update S5 PROC-LEAK / final process census.
		testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

		s := newRealStack(t, false)
		f := newPTYFixture(t)
		trace := tracePTYWire(t, s)
		hold := make(chan struct{})
		var once sync.Once
		t.Cleanup(func() { once.Do(func() { close(hold) }) })
		relay, url := startNATSReplyFence(t, s.url, func(subject string, body []byte) bool {
			var r proto.ProcessResumeReq
			return strings.Contains(subject, "process-resume.req") && json.Unmarshal(body, &r) == nil && r.Op == "create"
		}, hold, true)
		ctl := startTerminalCtl(t, s, s.bin, url, f.argv("duplex")...)
		f.identity(t)
		if !testharnessWait(10*time.Second, func() bool { return len(relay.receipts()) > 0 }) {
			t.Fatal("create reply was not lost")
		}
		relay.requireLoss(t)
		// Observe an actual retry of the original create while its reply stays
		// fenced. This proves the 500 ms attempt expired even on a busy worker;
		// elapsed sleep alone could cancel before the protocol loop retried.
		// origin: hot-update S4 plan §8.3 / T38; CLI-LATENCY observable barriers.
		if !testharnessWait(10*time.Second, func() bool {
			reqs, _ := trace.snapshot()
			creates := 0
			for _, req := range reqs {
				if req.Op == "create" {
					creates++
				}
			}
			return creates >= 2
		}) {
			t.Fatal("original create was not retried after its ready attempt expired")
		}
		if err := ctl.cmd.Process.Signal(syscall.SIGTERM); err != nil {
			t.Fatal(err)
		}
		ctl.wait(t, 74)
		once.Do(func() { close(hold) })
		trace.requireOriginalHandle(t)
		f.once(t)
	})
}

// origin: hot-update S4 plan §8.3 T35–T39. Verify the local fixture itself
// against today's real legacy binary, so dependency-blocked S4 tests cannot
// conceal a broken controlling-TTY or terminal-capture harness.
func TestPTYHarnessDuplexBytesAndDescriptorRestoration(t *testing.T) {
	// origin: hot-update S10 plan §11.1 / TEST-BUDGET
	if runIsolatedBinaryTest(t, binaryFixtureTestBudget) {
		return
	}
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	buildTether(t)
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	const rounds = 5
	for round := 0; round < rounds; round++ {
		t.Run(fmt.Sprintf("round_%d", round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			s := newRealStack(t, false)
			f := newPTYFixture(t)
			ctl := startTerminalCtl(t, s, s.bin, s.url, f.argv("duplex")...)
			f.identity(t)
			ctl.expect(t, []byte("READY\n"))
			input := ptyPayload(round)
			ctl.frame(t, input)
			want := append([]byte("READY\n"), ptyEcho(input)...)
			ctl.expect(t, want)
			ctl.resize(t, 137, 53)
			if string(f.receipt(t, "size")) != "53 137" {
				t.Fatal("fixture did not propagate a real terminal resize")
			}
			ctl.frame(t, []byte("EXIT"))
			want = append(want, []byte("TAIL-OUT\nTAIL-ERR\n")...)
			ctl.wait(t, 23)
			ctl.expect(t, want)
			f.once(t)
		})
	}
	testharness.AssertNoGoroutineLeak(t, "local real-binary PTY fixture", beforeG)
	testharness.AssertNoFDLeak(t, "local real-binary PTY fixture", beforeFD, 0)
}

// origin: hot-update S4 plan §8.3 T35. A new lease or RequestID is a new
// task, even when argv and output happen to match the original command.
func TestPTYTraceRejectsHandleRecreation(t *testing.T) {
	first := proto.ProcessHandle{Lease: proto.ProcessLease{LeaseID: "first", TargetInstance: "runtime-A"}, RequestID: "original"}
	for _, tc := range []struct {
		name   string
		handle proto.ProcessHandle
		valid  bool
	}{
		{"same", first, true},
		{"new_request", proto.ProcessHandle{Lease: first.Lease, RequestID: "replacement"}, false},
		{"new_lease", proto.ProcessHandle{Lease: proto.ProcessLease{LeaseID: "second", TargetInstance: "runtime-A"}, RequestID: first.RequestID}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := samePTYHandle(&first, &tc.handle); got != tc.valid {
				t.Fatalf("handle oracle accepted %s=%v", tc.name, got)
			}
		})
	}
	// origin: hot-update S4 plan §9.1 / T35; P18 lifecycle records
	// Completion must preserve the one-spawn receipt; repeated start records
	// must still expose duplication rather than being collapsed by PID.
	const started = `level=INFO msg="agent: run reliable=true started" pid=original`
	const exited = `level=INFO msg="agent: run reliable=true exited" pid=original code=23`
	for _, tc := range []struct {
		name, records string
		starts        int
	}{
		{"empty_records", "", 0},
		{"exit_without_start", exited, 0},
		{"one_start", started, 1},
		{"one_completed_child", started + "\n" + exited, 1},
		{"duplicate_completed_child", started + "\n" + exited + "\n" + started + "\n" + exited, 2},
		{"json_completed_child", `{"msg":"agent: run reliable=true started","pid":"original"}` + "\n" + `{"msg":"agent: run reliable=true exited","pid":"original"}`, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := len(regexp.MustCompile(reliablePTYStartPattern).FindAllString(tc.records, -1)); got != tc.starts {
				t.Fatalf("reliable spawn records=%d want=%d: %s", got, tc.starts, tc.records)
			}
		})
	}
}
func samePTYHandle(a, b *proto.ProcessHandle) bool { return reflect.DeepEqual(a, b) }

// origin: hot-update S4 plan §8.3 T36. A resize test which never changes
// the size between attempts can pass even when both ioctl effects run.
func TestPTYResizeOracleRequiresInterveningChange(t *testing.T) {
	for _, tc := range []struct {
		name, first, intermediate, after string
		valid                            bool
	}{
		{"deduplicated", "53 137", "41 103", "41 103", true},
		{"duplicate_effect", "53 137", "41 103", "53 137", false},
		{"same_size_fake_oracle", "53 137", "53 137", "53 137", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := resizeSurvivesRetry(tc.first, tc.intermediate, tc.after); got != tc.valid {
				t.Fatalf("resize oracle accepted %s=%v", tc.name, got)
			}
		})
	}
}
func resizeSurvivesRetry(first, intermediate, after string) bool {
	return first != intermediate && intermediate == after
}

// Callout mode needs the stack's own broker principal for observing command
// subjects. Ctl ACLs intentionally do not grant subscriptions to those trees.
func connectPTYObserver(t *testing.T, s *realStack) *nats.Conn {
	t.Helper()
	var nc *nats.Conn
	if s.callout {
		kp, err := nkeys.FromSeed(s.brokerSeed)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(kp.Wipe)
		pub, err := kp.PublicKey()
		if err != nil {
			t.Fatal(err)
		}
		nc, err = nats.Connect(s.url, nats.Nkey(pub, kp.Sign), nats.Name("p13-wire-observer"), nats.Timeout(2*time.Second))
		if err != nil {
			t.Fatal(err)
		}
	} else {
		nc = connect(t, s.url)
	}
	t.Cleanup(nc.Close)
	return nc
}
