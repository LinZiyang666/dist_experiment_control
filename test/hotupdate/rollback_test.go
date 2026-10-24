//go:build linux && hotupdate_integration

package hotupdate_test

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/flow"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/tunnel"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/internal/workload"
	natsserver "github.com/nats-io/nats-server/v2/server"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
)

// The rollback oracle requires a live independent SS stream before its first
// cut. Exercise that prerequisite separately so policy setup errors cannot be
// mistaken for an image import or watchdog failure.
// origin: hot-update S5 plan §6.1 / §8 T44 / T45
func TestRollbackFixtureKeepsIndependentPrivateTargetTraffic(t *testing.T) {
	beforeFD, beforeG := testharness.FDCount(), runtime.NumGoroutine()
	var artifacts *rollbackArtifacts
	for round := 0; round < 5; round++ {
		exerciseRollbackPrivateTargetTraffic(t, &artifacts)
	}
	testharness.AssertNoFDLeak(t, "rollback private target prerequisite x5", beforeFD, 0)
	testharness.AssertNoGoroutineLeak(t, "rollback private target prerequisite x5", beforeG)
}

func exerciseRollbackPrivateTargetTraffic(t *testing.T, artifacts **rollbackArtifacts) {
	t.Helper()
	f := newRollbackFixture(t, artifacts, false)
	defer f.close()
	f.awaitSamples(40)
	if f.rawTarget.accepts.Load() != 1 || f.ssTarget.accepts.Load() != 1 {
		t.Fatal("rollback prerequisite replaced an original raw or SS target")
	}
	_, rx, tx := f.ssProbe.snapshot()
	if rx == 0 || tx == 0 {
		t.Fatal("rollback prerequisite lacks independent duplex SS nonce progress")
	}
}

// origin: hot-update S5 plan §5.4 / §8 T44
// origin: hot-update S5 internal review HOTUPDATE-T44
// origin: S8 internal review TAGGED-SUITE-SWEEP-A F4
func TestActiveRollbackExportsCurrentFlowSSState(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeFD, beforeG := testharness.FDCount(), runtime.NumGoroutine()
	// Each round includes both real execs. Resource checks run after the fixture
	// joins its owners; continuity and kernel identity are checked before teardown.
	var artifacts *rollbackArtifacts
	for round := 0; round < 5; round++ {
		exerciseActiveFlowSSRollback(t, &artifacts)
	}
	testharness.AssertNoFDLeak(t, "current flow/SS rollback x5", beforeFD, 0)
	testharness.AssertNoGoroutineLeak(t, "current flow/SS rollback x5", beforeG)
}

func exerciseActiveFlowSSRollback(t *testing.T, artifacts **rollbackArtifacts) {
	t.Helper()
	f := newRollbackFixture(t, artifacts, false)
	defer f.close()
	f.awaitSamples(20)
	f.upgrade(1)
	// The initial v1 owner is generation one, so the two real cuts hand off
	// to generations two and three. Keep exact receipts for both transitions.
	// origin: hot-update S5 plan §4.4 / §5.4 / §8 T44
	f.awaitImage(f.artifacts.b.sha, 2)
	first := f.cut(0)
	f.requireCut(first, 2)
	before := f.progress()
	f.awaitSamples(before.raw + 20)
	f.requireAdvanced(before)
	f.upgrade(0)
	f.awaitImage(f.artifacts.a.sha, 3)
	second := f.cut(1)
	f.requireCurrentCut(first, second)
	f.awaitSamples(f.progress().raw + 20)
	f.requireOriginalBusiness()
	if f.commitCount() != 2 {
		t.Fatalf("A -> B -> A must commit twice, got %d", f.commitCount())
	}
}

// origin: hot-update S5 plan §8 T44/T45; independent oracle controls.
// These controls exercise the same comparator as the real tests even while
// another owner's P0 implementation blocks the real product fixture.
func TestRollbackCutRejectsStaleCountersAndOwner(t *testing.T) {
	first := rollbackCut{Generation: 1, SSCount: 1, Flow: &tunnel.ClientImage{Version: 1, Flows: flow.Image{Version: 1, ReservedBytes: 2 * upgradebudget.StreamReplayBytes}}}
	second := rollbackCut{Generation: 2, SSCount: 1, Flow: &tunnel.ClientImage{Version: 1, Flows: flow.Image{Version: 1, ReservedBytes: 2 * upgradebudget.StreamReplayBytes}}}
	for i := byte(1); i <= 2; i++ {
		id, endpoint := flow.ID{i}, flow.EndpointID{i}
		r := flow.RecordImage{ID: id, OpenSeq: uint64(i), Phase: flow.FlowPhaseActive,
			TxBase: 100, TxEnd: 200, RxApplied: 100, RxReceived: 100,
			RawRef: fmt.Sprintf("flow.%d", i), ReplayRef: fmt.Sprintf("replay.%d", i)}
		first.Flow.Flows.Records = append(first.Flow.Flows.Records, r)
		r.TxBase, r.TxEnd, r.RxApplied, r.RxReceived = 200, 300, 200, 200
		second.Flow.Flows.Records = append(second.Flow.Flows.Records, r)
		e := tunnel.EndpointImage{ID: endpoint, OwnerGen: 1, CarrierGen: 1, FlowIDs: []flow.ID{id}}
		first.Flow.Endpoints = append(first.Flow.Endpoints, e)
		e.OwnerGen, e.CarrierGen = 2, 2
		second.Flow.Endpoints = append(second.Flow.Endpoints, e)
	}
	if err := rollbackCurrentCutError(first, second); err != nil {
		t.Fatalf("progressing original cut rejected: %v", err)
	}
	for _, tc := range []struct {
		name   string
		mutate func(*rollbackCut)
	}{
		{"initial-bundle", func(c *rollbackCut) {
			c.Flow.Flows.Records = append([]flow.RecordImage(nil), first.Flow.Flows.Records...)
		}},
		{"generation-not-incremented", func(c *rollbackCut) { c.Generation = first.Generation }},
		{"applied-not-advanced", func(c *rollbackCut) { c.Flow.Flows.Records[0].RxApplied = first.Flow.Flows.Records[0].RxApplied }},
		{"base-not-advanced", func(c *rollbackCut) { c.Flow.Flows.Records[0].TxBase = first.Flow.Flows.Records[0].TxBase }},
		{"end-not-advanced", func(c *rollbackCut) { c.Flow.Flows.Records[0].TxEnd = first.Flow.Flows.Records[0].TxEnd }},
		{"owner-not-incremented", func(c *rollbackCut) { c.Flow.Endpoints[0].OwnerGen = first.Flow.Endpoints[0].OwnerGen }},
		{"carrier-not-advanced", func(c *rollbackCut) { c.Flow.Endpoints[0].CarrierGen = first.Flow.Endpoints[0].CarrierGen }},
		{"replacement-flow", func(c *rollbackCut) { c.Flow.Flows.Records[0].ID = flow.ID{99} }},
		{"duplicate-flow", func(c *rollbackCut) { c.Flow.Flows.Records[1] = c.Flow.Flows.Records[0] }},
		{"missing-replay-reservation", func(c *rollbackCut) { c.Flow.Flows.ReservedBytes = 0 }},
		{"missing-ss", func(c *rollbackCut) { c.SSCount = 0 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed := second
			public := *second.Flow
			public.Flows.Records = append([]flow.RecordImage(nil), second.Flow.Flows.Records...)
			public.Endpoints = append([]tunnel.EndpointImage(nil), second.Flow.Endpoints...)
			changed.Flow = &public
			tc.mutate(&changed)
			if rollbackCurrentCutError(first, changed) == nil {
				t.Fatal("current-cut oracle accepted stale or replacement ownership")
			}
		})
	}
}

type rollbackArtifact struct {
	path, sha, tarSHA string
	tarball           []byte
}

type rollbackArtifacts struct{ a, b rollbackArtifact }

// Only public image counters leave the process through this observer. In
// particular SS state, nonces, keys and grants never enter the receipt. The
// independent client below checks the nonce sequence from authenticated wire.
type rollbackCut struct {
	Generation uint64              `json:"generation"`
	Flow       *tunnel.ClientImage `json:"flow"`
	SSCount    int                 `json:"ss_count"`
}

// Registration is a control witness, never a transaction completion proof.
// origin: S8 internal review TAGGED-SUITE-SWEEP-A F4
type rollbackRegister struct {
	req     proto.NodeRegisterReq
	when    time.Time
	running string
}

type rollbackFixture struct {
	t                           *testing.T
	artifacts                   *rollbackArtifacts
	dir, bin, home, log, output string
	server                      *natsserver.Server
	nc                          *nats.Conn
	tunnel                      *tunnel.Server
	cancel                      context.CancelFunc
	http                        *httptest.Server
	cmd                         *exec.Cmd
	processDone                 chan struct{}
	processErr                  error
	pid                         int
	startTicks                  string
	rawTarget, ssTarget         *rollbackTarget
	rawProbe, ssProbe           *rollbackProbe
	publicRaw, publicSS         int
	tokenRaw, tokenSS, password string
	mu                          sync.Mutex
	registers                   []rollbackRegister
	callbackErr                 error
	faultArmed, faultInstalled  bool
	authorizations              atomic.Uint64
	originalSockets             map[string]bool
	closed                      bool
	key                         upgradetx.Key
	current                     upgradetx.Handle
	journalDir                  string
	watchdogDeadline            time.Time
}

// The NATS responder is an explicit control fixture, not an alternate image
// implementation. Real agent New/Run, installer, bootstrap, flow/SS owners and
// watchdog execute in the product process. TLS RESUME has a fresh bounded grant
// on every call; the fixture never suspends authority to manufacture rollback.
func newRollbackFixture(t *testing.T, artifacts **rollbackArtifacts, watchdog bool) *rollbackFixture {
	t.Helper()
	f := &rollbackFixture{t: t, faultArmed: watchdog, dir: t.TempDir()}
	f.bin, f.home = filepath.Join(f.dir, "bin", "tether"), filepath.Join(f.dir, "home")
	f.log, f.output = filepath.Join(f.dir, "agent.log"), filepath.Join(f.dir, "agent.stdout")
	for _, dir := range []string{filepath.Dir(f.bin), f.home} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(f.close)
	f.publicRaw, f.publicSS = rollbackFreePort(t), rollbackFreePort(t)
	for f.publicSS == f.publicRaw {
		f.publicSS = rollbackFreePort(t)
	}
	f.tokenRaw, f.tokenSS, f.password = rollbackRandom(t), rollbackRandom(t), rollbackRandom(t)
	controlAddr := net.JoinHostPort("127.0.0.1", strconv.Itoa(rollbackFreePort(t)))
	logger := slog.New(slog.DiscardHandler)
	f.tunnel = tunnel.NewServer(controlAddr, "127.0.0.1", f.lookup, logger)
	// origin: hot-update S5 plan §2 / T44 / T45. A P0 stub is a hard,
	// explicit failure here, never a skip or legacy continuity substitute.
	err := f.tunnel.SetFlowConfig(tunnel.FlowConfig{
		Budget:     &workload.ReplayBudget{Limit: upgradebudget.NodeReplayBytes},
		InstanceID: "aaaaaaaaaaaaaaaaaaaaaaaaaa", OwnerGen: 1, BootID: "rollback-broker",
		MaxEndpoints: tunnel.MaxEndpoints, MaxPendingFDs: 32,
		TokenLookup: func(r tunnel.AuthRequest) error {
			return f.checkAllocation(r.Allocation)
		},
		Authorize: func(r tunnel.AuthRequest) (tunnel.Authority, error) {
			if err := f.checkAllocation(r.Allocation); err != nil {
				return tunnel.Authority{}, err
			}
			select {
			case <-r.Stop:
				return tunnel.Authority{}, errors.New("fixture authority stopped")
			default:
			}
			if !r.Deadline.After(time.Now()) {
				return tunnel.Authority{}, errors.New("fixture authority deadline expired")
			}
			f.authorizations.Add(1)
			return tunnel.Authority{Enabled: true, Allocation: r.Allocation,
				Generation: 1, ProxyEpoch: 1, AppliedRevision: 1,
				Keys: []tunnel.AuthorizedKey{{ID: "survivor", Secret: []byte(f.password)}}}, nil
		},
	})
	if err != nil {
		t.Fatalf("real rollback requires the S5 flow owner: %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	f.cancel = cancel
	if err := f.tunnel.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if *artifacts == nil {
		*artifacts = buildRollbackArtifacts(t)
	}
	f.artifacts = *artifacts
	rollbackCopy(t, f.artifacts.a.path, f.bin)
	rollbackRegisterDomain(t, f)
	opts := natstest.DefaultTestOptions
	opts.Host, opts.Port, opts.AlwaysEnableNonce = "127.0.0.1", -1, true
	f.server = natstest.RunServer(&opts)
	f.nc, err = nats.Connect(f.server.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.nc.Subscribe(proto.SubjNodeRegister("rollback", "node"), f.register); err != nil {
		t.Fatal(err)
	}
	rollbackInstallControlProbe(t, f)
	if err := f.nc.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	f.http = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/0.tar.gz":
			_, _ = w.Write(f.artifacts.a.tarball)
		case "/1.tar.gz":
			_, _ = w.Write(f.artifacts.b.tarball)
		default:
			http.NotFound(w, r)
		}
	}))
	// The loopback target is an intentional test policy opt-in, identical in
	// both artifacts. All user paths and inherited fleet routing are isolated.
	// The command reads per-session policy, rather than a file at the home root.
	// A misplaced opt-in makes the real server reject this loopback target before
	// any upgrade, so a missing response salt cannot diagnose rollback continuity.
	// origin: hot-update S5 plan §6.1 / §8 T44 / T45
	policyDir := filepath.Join(f.home, "agent", "rollback")
	if err := os.MkdirAll(policyDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(policyDir, "agent.yaml"), []byte("proxy:\n  allow_private_destinations: true\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	out, err := os.Create(f.output)
	if err != nil {
		t.Fatal(err)
	}
	f.cmd = exec.Command(f.bin, "agent", "--session", "rollback", "--nid", "node",
		"--nats-url", f.server.ClientURL(), "--tunnel-addr", controlAddr,
		"--log-file", f.log, "--log-json", "--upgrade-url-allow", f.http.URL+"/")
	f.cmd.Env = rollbackEnvironment(f.home)
	f.cmd.Stdout, f.cmd.Stderr = out, out
	f.cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
	waitProcess, err := testharness.StartProcess(f.cmd)
	if err != nil {
		_ = out.Close()
		t.Fatal(err)
	}
	_ = out.Close()
	f.pid = f.cmd.Process.Pid
	f.startTicks = rollbackStartTicks(t, f.pid)
	f.processDone = make(chan struct{})
	go func() { f.processErr = waitProcess(); close(f.processDone) }()
	f.wait("initial agent registration", 20*time.Second, func() bool {
		f.mu.Lock()
		defer f.mu.Unlock()
		return len(f.registers) == 1
	})
	f.wait("agent command subscription", 10*time.Second, func() bool {
		z, err := f.server.Connz(&natsserver.ConnzOptions{State: natsserver.ConnOpen, Subscriptions: true})
		if err != nil {
			return false
		}
		for _, c := range z.Conns {
			for _, s := range c.Subs {
				if s == proto.SubjCmdForwarded("rollback", "node", "*") {
					return true
				}
			}
		}
		return false
	})
	f.rawTarget, f.ssTarget = newRollbackTarget(t), newRollbackTarget(t)
	var resp proto.ExposeForwardedResp
	f.request("expose", proto.ExposeForwardedReq{Name: "raw", Port: f.publicRaw,
		LocalPort: f.rawTarget.port(), Token: f.tokenRaw, ActorFP: "fixture"}, &resp)
	if !resp.OK {
		t.Fatalf("raw expose failed: code=%s", resp.Code)
	}
	f.rawProbe = newRollbackProbe(t, f.publicRaw, nil, 0)
	// The SS target is selected in the independently encoded handshake, not
	// by redirecting the flow owner or reconstructing the target after exec.
	f.ssProbe = newRollbackProbe(t, f.publicSS, []byte(f.password), f.ssTarget.port())
	f.awaitSamples(20)
	f.originalSockets = rollbackSocketInodes(t, f.pid)
	if len(f.originalSockets) < 6 {
		t.Fatalf("business socket census is empty: %d sockets", len(f.originalSockets))
	}
	return f
}

func (f *rollbackFixture) checkAllocation(a tunnel.Allocation) error {
	if a.SID != "rollback" || a.NID != "node" || a.Epoch != 0 {
		return errors.New("fixture allocation mismatch")
	}
	var token string
	switch a.Port {
	case f.publicRaw:
		token = f.tokenRaw
	case f.publicSS:
		token = f.tokenSS
	default:
		return errors.New("fixture unknown allocation")
	}
	if a.TokenHash != fmt.Sprintf("%x", sha256.Sum256([]byte(token))) {
		return errors.New("fixture token mismatch")
	}
	return nil
}

func (f *rollbackFixture) lookup(sid, nid string, port int, hash string, epoch int64) error {
	return f.checkAllocation(tunnel.Allocation{SID: sid, NID: nid, Port: port, TokenHash: hash, Epoch: epoch})
}

func (f *rollbackFixture) register(msg *nats.Msg) {
	var req proto.NodeRegisterReq
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		f.recordError(errors.New("fixture register decode failed"))
		return
	}
	f.mu.Lock()
	if f.faultArmed && proto.SameRelease(req.ReleaseVersion, "v0.0.0-rollback-b") {
		// The passive completion observer installs the permission fault only at
		// the actual v2 completion CAS, after registration and all three receipts.
		// origin: S8 internal review TAGGED-SUITE-SWEEP-A F4
		if rollbackSHA(fmt.Sprintf("/proc/%d/exe", f.pid)) != f.artifacts.b.sha || req.Incarnation != rollbackIncarnation {
			f.callbackErr = errors.New("commit fault missed the accepted boot/register barrier")
		} else if err := os.WriteFile(filepath.Join(f.home, "rollback-fault"), []byte("armed"), 0600); err != nil {
			f.callbackErr = err
		} else {
			f.faultInstalled = true
		}
		f.faultArmed = false
	}
	f.registers = append(f.registers, rollbackRegister{req: req, when: time.Now(), running: rollbackSHA(fmt.Sprintf("/proc/%d/exe", f.pid))})
	f.mu.Unlock()
	resp := proto.NodeRegisterResp{OK: true, Incarnation: rollbackIncarnation, Proxy: &proto.ProxyDirective{Enabled: true,
		PublicPort: f.publicSS, Token: f.tokenSS, Cipher: "chacha20-ietf-poly1305",
		Generation: 1, Epoch: 1, Keys: []proto.ProxyKey{{SubID: "survivor", Secret: f.password}}}}
	for _, p := range req.LocalPorts {
		resp.KeepPorts = append(resp.KeepPorts, p.Port)
	}
	for _, p := range req.LocalProcesses {
		resp.AcceptedProcesses = append(resp.AcceptedProcesses, p.PID)
	}
	body, err := json.Marshal(resp)
	if err == nil {
		err = msg.Respond(body)
	}
	if err != nil {
		f.recordError(errors.New("fixture register reply failed"))
	}
}

func (f *rollbackFixture) recordError(err error) {
	f.mu.Lock()
	if f.callbackErr == nil {
		f.callbackErr = err
	}
	f.mu.Unlock()
}

func (f *rollbackFixture) request(verb string, request, response any) {
	f.t.Helper()
	body, err := json.Marshal(request)
	if err != nil {
		f.t.Fatal("fixture request encode failed")
	}
	ctx, cancel := context.WithTimeout(f.t.Context(), upgradebudget.ComponentHandover)
	defer cancel()
	msg, err := f.nc.RequestWithContext(ctx, proto.SubjCmdForwarded("rollback", "node", verb), body)
	if err != nil {
		f.t.Fatalf("forwarded %s: %v", verb, err)
	}
	if err := json.Unmarshal(msg.Data, response); err != nil {
		f.t.Fatalf("forwarded %s response decode failed", verb)
	}
}

func (f *rollbackFixture) wait(label string, budget time.Duration, ready func() bool) {
	f.t.Helper()
	timer, tick := time.NewTimer(budget), time.NewTicker(10*time.Millisecond)
	defer timer.Stop()
	defer tick.Stop()
	for {
		f.requireHealthy()
		if ready() {
			return
		}
		select {
		case <-timer.C:
			f.t.Fatalf("timed out waiting for %s", label)
		case <-f.t.Context().Done():
			f.t.Fatal("rollback fixture canceled")
		case <-tick.C:
		}
	}
}

func (f *rollbackFixture) requireHealthy() {
	f.t.Helper()
	if f.processDone != nil {
		select {
		case <-f.processDone:
			f.t.Fatalf("original agent exited: %v", f.processErr)
		default:
		}
	}
	f.mu.Lock()
	err := f.callbackErr
	f.mu.Unlock()
	if err != nil {
		f.t.Fatal(err)
	}
	for _, p := range []*rollbackProbe{f.rawProbe, f.ssProbe} {
		if p != nil {
			if err := p.failure(); err != nil {
				f.t.Fatal(err)
			}
		}
	}
}

func (f *rollbackFixture) cut(index int) rollbackCut {
	f.t.Helper()
	var cuts []rollbackCut
	f.wait("public current-cut receipt", 10*time.Second, func() bool {
		body, err := os.ReadFile(f.output)
		if err != nil {
			return false
		}
		cuts = nil
		for _, line := range strings.Split(string(body), "\n") {
			if !strings.HasPrefix(line, "ROLLBACK_CUT ") {
				continue
			}
			var c rollbackCut
			if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "ROLLBACK_CUT ")), &c); err != nil {
				f.t.Fatal("invalid public cut receipt")
			}
			cuts = append(cuts, c)
		}
		return len(cuts) > index
	})
	return cuts[index]
}

func (f *rollbackFixture) requireCut(c rollbackCut, generation uint64) {
	f.t.Helper()
	if err := rollbackCutError(c, generation); err != nil {
		f.t.Fatal(err)
	}
}

func (f *rollbackFixture) requireCurrentCut(first, second rollbackCut) {
	f.t.Helper()
	if err := rollbackCurrentCutError(first, second); err != nil {
		f.t.Fatal(err)
	}
}

func rollbackCutError(c rollbackCut, generation uint64) error {
	if c.Generation != generation || c.Flow == nil || c.SSCount != 1 || len(c.Flow.Flows.Records) < 2 || len(c.Flow.Endpoints) < 2 {
		return errors.New("cut must retain both original flow and SS domains with the exact generation")
	}
	if c.Flow.Flows.ReservedBytes != int64(len(c.Flow.Flows.Records))*upgradebudget.StreamReplayBytes {
		return errors.New("cut lost or duplicated an owned send replay capacity reservation")
	}
	for _, r := range c.Flow.Flows.Records {
		if r.RxApplied == 0 || r.TxBase == 0 || r.TxEnd < r.TxBase || r.RxReceived < r.RxApplied || r.RawRef == "" || r.ReplayRef == "" {
			return errors.New("cut has no applied/ACKed bytes or original descriptor ownership")
		}
	}
	for _, e := range c.Flow.Endpoints {
		if e.OwnerGen == 0 || e.CarrierGen == 0 || len(e.FlowIDs) != 1 {
			return errors.New("cut lacks the original owner/attachment receipt")
		}
	}
	return nil
}

// origin: hot-update S5 plan §5.4 / T44 / T45. Comparing identities as a
// set prevents a fresh Dial or replacement flow from satisfying byte counters.
func rollbackCurrentCutError(first, second rollbackCut) error {
	if err := rollbackCutError(first, first.Generation); err != nil {
		return err
	}
	if err := rollbackCutError(second, first.Generation+1); err != nil {
		return err
	}
	if len(second.Flow.Flows.Records) != len(first.Flow.Flows.Records) || len(second.Flow.Endpoints) != len(first.Flow.Endpoints) {
		return errors.New("second cut replaced the original cohort")
	}
	byID := make(map[flow.ID]flow.RecordImage)
	for _, r := range first.Flow.Flows.Records {
		byID[r.ID] = r
	}
	for _, r := range second.Flow.Flows.Records {
		old, ok := byID[r.ID]
		if !ok || r.OpenSeq != old.OpenSeq || r.Intent != old.Intent || r.RawRef != old.RawRef || r.ReplayRef != old.ReplayRef || r.RxApplied <= old.RxApplied || r.TxBase <= old.TxBase || r.TxEnd <= old.TxEnd {
			return errors.New("second cut reused initial offsets/Base or changed original flow ownership")
		}
		delete(byID, r.ID)
	}
	endpoints := make(map[flow.EndpointID]tunnel.EndpointImage)
	for _, e := range first.Flow.Endpoints {
		endpoints[e.ID] = e
	}
	for _, e := range second.Flow.Endpoints {
		old, ok := endpoints[e.ID]
		if !ok || e.OwnerGen != old.OwnerGen+1 || e.CarrierGen <= old.CarrierGen {
			return errors.New("second cut did not advance the consumed owner/attachment receipt")
		}
		delete(endpoints, e.ID)
	}
	if len(byID) != 0 || len(endpoints) != 0 {
		return errors.New("second cut omitted an original owner")
	}
	return nil
}

type rollbackProgress struct{ raw, ss, rxNonce, txNonce, grants uint64 }

func (f *rollbackFixture) progress() rollbackProgress {
	raw, _, _ := f.rawProbe.snapshot()
	ss, rx, tx := f.ssProbe.snapshot()
	return rollbackProgress{raw: raw, ss: ss, rxNonce: rx, txNonce: tx, grants: f.authorizations.Load()}
}

func (f *rollbackFixture) awaitSamples(n uint64) {
	f.t.Helper()
	f.wait("independent raw and SS samples", 15*time.Second, func() bool { p := f.progress(); return p.raw >= n && p.ss >= n })
}

func (f *rollbackFixture) requireAdvanced(before rollbackProgress) {
	f.t.Helper()
	after := f.progress()
	if after.raw <= before.raw || after.ss <= before.ss || after.rxNonce <= before.rxNonce || after.txNonce <= before.txNonce {
		f.t.Fatal("B must advance plaintext, independent SS nonces, and both directions before the second cut")
	}
}

func (f *rollbackFixture) requireOriginalBusiness() {
	f.t.Helper()
	f.requireHealthy()
	if rollbackStartTicks(f.t, f.pid) != f.startTicks {
		f.t.Fatal("agent PID/start ticks changed")
	}
	for _, target := range []*rollbackTarget{f.rawTarget, f.ssTarget} {
		if target.accepts.Load() != 1 {
			f.t.Fatalf("original target must Accept once, got %d", target.accepts.Load())
		}
	}
	if f.authorizations.Load() < 3 {
		f.t.Fatal("both execs must obtain fresh RESUME authority")
	}
	// Carrier sockets may change; every original business socket is selected
	// by the target's peer port, never by a total-FD count that can hide swaps.
	current := rollbackSocketInodes(f.t, f.pid)
	for _, target := range []*rollbackTarget{f.rawTarget, f.ssTarget} {
		ino := target.agentInode(f.t, f.pid)
		if !f.originalSockets[ino] || !current[ino] {
			f.t.Fatal("original target socket inode changed across exec")
		}
	}
	for _, p := range []*rollbackProbe{f.rawProbe, f.ssProbe} {
		_, _, _ = p.snapshot()
		p.requireWindow(f.t)
	}
	entries, err := os.ReadDir(fmt.Sprintf("/proc/%d/fd", f.pid))
	if err != nil {
		f.t.Fatal(err)
	}
	for _, entry := range entries {
		name, _ := os.Readlink(fmt.Sprintf("/proc/%d/fd/%s", f.pid, entry.Name()))
		if strings.Contains(name, "memfd:tether-handoff") {
			f.t.Fatal("live receiver retained a dispatch manifest")
		}
	}
}

func (f *rollbackFixture) commitCount() int {
	body, _ := os.ReadFile(f.output)
	return strings.Count(string(body), "ROLLBACK_COMPLETED ")
}

func (f *rollbackFixture) close() {
	if f.closed {
		return
	}
	f.closed = true
	_ = os.Chmod(filepath.Dir(f.bin), 0o700)
	if f.journalDir != "" {
		_ = os.Chmod(f.journalDir, 0700)
	}
	for _, p := range []*rollbackProbe{f.rawProbe, f.ssProbe} {
		if p != nil {
			p.close()
		}
	}
	if f.processDone != nil {
		// origin: hot-update S5 PROC-LEAK. A joined leader can leave group
		// members alive; teardown retains responsibility for the whole group.
		defer func() { _ = syscall.Kill(-f.pid, syscall.SIGKILL) }()
		_ = f.cmd.Process.Signal(syscall.SIGTERM)
		select {
		case <-f.processDone:
		case <-time.After(10 * time.Second):
			_ = syscall.Kill(-f.pid, syscall.SIGKILL)
			<-f.processDone
		}
		_ = syscall.Kill(-f.pid, syscall.SIGKILL)
	}
	for _, target := range []*rollbackTarget{f.rawTarget, f.ssTarget} {
		if target != nil {
			target.close()
		}
	}
	if f.cancel != nil {
		f.cancel()
	}
	if f.tunnel != nil {
		f.tunnel.Close()
	}
	if f.nc != nil {
		f.nc.Close()
	}
	if f.server != nil {
		f.server.Shutdown()
		f.server.WaitForShutdown()
	}
	if f.http != nil {
		f.http.Close()
		f.http.CloseClientConnections()
	}
	// Preserve the private product's failure stage after owners have joined.
	// The passive observer emits only public image counters, never SS state.
	// origin: hot-update S5 plan §8 T44 / T45
	if f.t.Failed() {
		for _, path := range []string{f.log, f.output, filepath.Join(f.dir, "agent.boot.err")} {
			body, _ := os.ReadFile(path)
			f.t.Logf("private rollback product %s:\n%s", filepath.Base(path), body)
		}
	}
}

func rollbackEnvironment(home string) []string {
	var env []string
	for _, e := range handoff.CleanEnvironment(os.Environ()) {
		key, _, _ := strings.Cut(e, "=")
		if key == "TETHER_HOME" || key == "HOME" || strings.HasPrefix(key, "TETHER_") || strings.HasPrefix(key, "_TETHER_") {
			continue
		}
		env = append(env, e)
	}
	return append(env, "HOME="+home, "TETHER_HOME="+home, "TETHER_DEV_NO_AUTH=", "TETHER_SESSION=", "TETHER_NATS_URL=", "GORACE=atexit_sleep_ms=0")
}

func rollbackFreePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	return port
}

func rollbackRandom(t *testing.T) string {
	t.Helper()
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatal("fixture entropy unavailable")
	}
	return fmt.Sprintf("%x", b)
}

func rollbackStartTicks(t *testing.T, pid int) string {
	t.Helper()
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		t.Fatal(err)
	}
	end := strings.LastIndexByte(string(b), ')')
	if end < 0 {
		t.Fatal("invalid process identity")
	}
	fields := strings.Fields(string(b[end+1:]))
	if len(fields) < 20 {
		t.Fatal("truncated process identity")
	}
	return fields[19]
}

func rollbackSocketInodes(t *testing.T, pid int) map[string]bool {
	t.Helper()
	entries, err := os.ReadDir(fmt.Sprintf("/proc/%d/fd", pid))
	if err != nil {
		t.Fatal(err)
	}
	result := make(map[string]bool)
	for _, e := range entries {
		link, _ := os.Readlink(fmt.Sprintf("/proc/%d/fd/%s", pid, e.Name()))
		if strings.HasPrefix(link, "socket:[") {
			result[strings.TrimSuffix(strings.TrimPrefix(link, "socket:["), "]")] = true
		}
	}
	return result
}

func rollbackSHA(path string) string {
	f, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer func() { _ = f.Close() }()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return ""
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

func rollbackCopy(t *testing.T, source, target string) {
	t.Helper()
	in, err := os.Open(source)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = in.Close() }()
	out, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o755)
	if err != nil {
		t.Fatal(err)
	}
	_, err = io.Copy(out, in)
	closeErr := out.Close()
	if err != nil || closeErr != nil {
		t.Fatal("artifact copy failed")
	}
}

// The product is built, not replaced by a test executable. Passive observers
// use overlays in the private artifact directory; no other owner's source is
// edited. An upstream GOFLAGS overlay is merged first so mutations reach the
// nested build as well as the parent test binary.
func buildRollbackArtifacts(t *testing.T) *rollbackArtifacts {
	t.Helper()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	var overlay struct{ Replace map[string]string }
	overlay.Replace = make(map[string]string)
	var flags []string
	for _, flag := range strings.Fields(os.Getenv("GOFLAGS")) {
		if strings.HasPrefix(flag, "-overlay=") {
			body, err := os.ReadFile(strings.TrimPrefix(flag, "-overlay="))
			if err != nil || json.Unmarshal(body, &overlay) != nil {
				t.Fatal("upstream mutation overlay is unreadable")
			}
		} else {
			flags = append(flags, flag)
		}
	}
	read := func(rel string) (string, string) {
		path := filepath.Join(root, rel)
		actual := path
		if replacement, ok := overlay.Replace[path]; ok {
			actual = replacement
		}
		body, err := os.ReadFile(actual)
		if err != nil {
			t.Fatal(err)
		}
		return path, string(body)
	}
	path, source := read("cmd/tether/agent.go")
	needle := "a, err := agent.New(cfg)"
	if strings.Count(source, needle) != 1 {
		t.Fatal("product config observer attachment moved")
	}
	source = strings.Replace(source, "\"bytes\"", "\"bytes\"\n\"encoding/json\"\n\"reflect\"\n\"github.com/LinZiyang666/tether/internal/agentimage\"", 1)
	source = strings.Replace(source, needle, "cfg.UpgradeCutObserver = rollbackPublicCutObserver\n"+needle, 1)
	source += `
// origin: hot-update S5 plan §8 T44/T45; passive fixture observer.
func rollbackPublicCutObserver(b agentimage.Bundle) {
    public := struct {
        Generation uint64 ` + "`json:\"generation\"`" + `
        Flow any ` + "`json:\"flow\"`" + `
        SSCount int ` + "`json:\"ss_count\"`" + `
    }{Generation: b.Lineage.Generation}
    v := reflect.ValueOf(b)
    if f := v.FieldByName("Flow"); f.IsValid() && f.Kind() == reflect.Pointer && !f.IsNil() { public.Flow = f.Interface() }
    if f := v.FieldByName("SS"); f.IsValid() && f.Kind() == reflect.Pointer && !f.IsNil() {
        public.SSCount = int(f.Elem().FieldByName("ConnectionCount").Int())
    }
    body, err := json.Marshal(public)
    if err != nil { panic("public rollback receipt encoding failed") }
    fmt.Fprintln(os.Stdout, "ROLLBACK_CUT " + string(body))
}
`
	patched := filepath.Join(dir, "agent-command.go")
	if err := os.WriteFile(patched, []byte(source), 0o600); err != nil {
		t.Fatal(err)
	}
	overlay.Replace[path] = patched
	// The control-ready barrier moved to the agent upgrade leaf in S8.
	// origin: S7 internal review GATE-CONVERGE-contracts F7
	path, source = read("internal/agentupgrade/control.go")
	source, err = attachRollbackReadyObserver(source)
	if err != nil {
		t.Fatal(err)
	}
	patched = filepath.Join(dir, "agent-runtime.go")
	if err := os.WriteFile(patched, []byte(source), 0o600); err != nil {
		t.Fatal(err)
	}
	overlay.Replace[path] = patched
	rollbackCompletionOverlay(t, root, dir, overlay.Replace)
	body, err := json.Marshal(overlay)
	if err != nil {
		t.Fatal(err)
	}
	overlayPath := filepath.Join(dir, "overlay.json")
	if err := os.WriteFile(overlayPath, body, 0o600); err != nil {
		t.Fatal(err)
	}
	flags = append(flags, "-overlay="+overlayPath)
	build := func(version string) rollbackArtifact {
		path := filepath.Join(dir, version)
		cmd := exec.CommandContext(t.Context(), "go", "build", "-race", "-trimpath", "-o", path,
			"-ldflags", "-X github.com/LinZiyang666/tether/internal/proto.ReleaseVersion="+version, "./cmd/tether")
		cleanup := hotUpdateOwnProcessGroup(cmd)
		defer cleanup()
		cmd.Dir = root
		cmd.Env = append(os.Environ(), "GOFLAGS="+strings.Join(flags, " "))
		if out, err := testharness.CombinedOutput(cmd); err != nil {
			t.Fatalf("build real rollback artifact: %v\n%s", err, out)
		}
		binary, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var archive bytes.Buffer
		gz, err := gzip.NewWriterLevel(&archive, gzip.NoCompression)
		if err != nil {
			t.Fatal(err)
		}
		tarWriter := tar.NewWriter(gz)
		if err := tarWriter.WriteHeader(&tar.Header{Name: "tether", Mode: 0o755, Size: int64(len(binary))}); err != nil {
			t.Fatal(err)
		}
		if _, err := tarWriter.Write(binary); err != nil {
			t.Fatal(err)
		}
		if err := tarWriter.Close(); err != nil {
			t.Fatal(err)
		}
		if err := gz.Close(); err != nil {
			t.Fatal(err)
		}
		a := rollbackArtifact{path: path, sha: fmt.Sprintf("%x", sha256.Sum256(binary)), tarSHA: fmt.Sprintf("%x", sha256.Sum256(archive.Bytes())), tarball: archive.Bytes()}
		t.Logf("real artifact %s SHA=%s; nested build -race, inherited overlay", version, a.sha)
		return a
	}
	a := &rollbackArtifacts{a: build("v0.0.0-rollback-a"), b: build("v0.0.0-rollback-b")}
	if a.a.sha == a.b.sha {
		t.Fatal("A and B must be different real artifacts")
	}
	return a
}

// A challenge and its response have separate ledgers: the target verifies the
// exact client sequence and transforms its bytes, so a local echo or an
// unobserved dropped request cannot satisfy the client-side oracle.
type rollbackTarget struct {
	extra    sync.WaitGroup
	ln       net.Listener
	accepts  atomic.Uint64
	mu       sync.Mutex
	conn     net.Conn
	peerPort int
	done     chan struct{}
}

func newRollbackTarget(t *testing.T) *rollbackTarget {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &rollbackTarget{ln: ln, done: make(chan struct{})}
	go func() {
		defer close(s.done)
		c, err := ln.Accept()
		if err != nil {
			return
		}
		s.accepts.Add(1)
		s.mu.Lock()
		s.conn = c
		s.peerPort = c.RemoteAddr().(*net.TCPAddr).Port
		s.mu.Unlock()
		defer func() { _ = c.Close() }()
		// A second Accept is observable while the original connection lives.
		s.extra.Add(1)
		go func() {
			defer s.extra.Done()
			for {
				extra, err := ln.Accept()
				if err != nil {
					return
				}
				s.accepts.Add(1)
				_ = extra.Close()
			}
		}()
		for seq := uint64(1); ; seq++ {
			var got [256]byte
			if _, err := io.ReadFull(c, got[:]); err != nil {
				return
			}
			want := rollbackChallenge(seq)
			if got != want {
				return
			}
			for i := 8; i < len(got); i++ {
				got[i] ^= 0xa5
			}
			if err := rollbackWriteAll(c, got[:]); err != nil {
				return
			}
		}
	}()
	return s
}

func (s *rollbackTarget) port() int { return s.ln.Addr().(*net.TCPAddr).Port }

func (s *rollbackTarget) close() {
	_ = s.ln.Close()
	s.mu.Lock()
	c := s.conn
	s.mu.Unlock()
	if c != nil {
		_ = c.Close()
	}
	<-s.done
	s.extra.Wait()
}

func (s *rollbackTarget) agentInode(t *testing.T, pid int) string {
	t.Helper()
	s.mu.Lock()
	port := s.peerPort
	s.mu.Unlock()
	body, err := os.ReadFile(fmt.Sprintf("/proc/%d/net/tcp", pid))
	if err != nil {
		t.Fatal(err)
	}
	want := fmt.Sprintf("0100007F:%04X", port)
	for _, line := range strings.Split(string(body), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 10 && fields[1] == want && fields[2] == fmt.Sprintf("0100007F:%04X", s.port()) {
			return fields[9]
		}
	}
	t.Fatal("original accepted target socket is absent from the live agent")
	return ""
}

func rollbackChallenge(seq uint64) [256]byte {
	var body [256]byte
	binary.BigEndian.PutUint64(body[:8], seq)
	for i := 8; i < len(body); i++ {
		body[i] = byte(seq*17 + uint64(i)*31)
	}
	return body
}

func rollbackWriteAll(w io.Writer, body []byte) error {
	for len(body) > 0 {
		n, err := w.Write(body)
		body = body[n:]
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrNoProgress
		}
	}
	return nil
}

// The passive hook must attach to exactly one current control-ready barrier.
// A stale owner, missing call or duplicate call is a setup failure, never a
// watchdog continuity receipt.
// origin: S7 internal review GATE-CONVERGE-contracts F7
func attachRollbackReadyObserver(source string) (string, error) {
	const needle = "view, err := Boot(ctx, p.Endpoint, ports.Cut.Attempt)"
	if strings.Count(source, needle) != 1 {
		return "", errors.New("product MarkReady observer attachment moved")
	}
	return strings.Replace(source, needle, needle+"\n p.Legacy.Logger.Info(\"rollback fixture ready\", \"generation\", p.Image.Generation())", 1), nil
}

// The same predicate used by the nested artifact builder must reject a moved
// or removed production barrier before any real upgrade is attempted.
// origin: S7 internal review GATE-CONVERGE-contracts F7
func TestRollbackReadyObserverRequiresCurrentControlBarrier(t *testing.T) {
	body, err := os.ReadFile("../../internal/agentupgrade/control.go")
	if err != nil {
		t.Fatal(err)
	}
	good := string(body)
	observed, err := attachRollbackReadyObserver(good)
	if err != nil || strings.Count(observed, "rollback fixture ready") != 1 {
		t.Fatalf("current control-ready observer: err=%v", err)
	}
	for _, bad := range []string{
		strings.Replace(good, "view, err := Boot(ctx, p.Endpoint, ports.Cut.Attempt)", "", 1),
		strings.Replace(good, "view, err := Boot(ctx, p.Endpoint, ports.Cut.Attempt)", "view, err := Boot(ctx, p.Endpoint, stale)", 1),
		strings.Replace(good, "view, err := Boot(ctx, p.Endpoint, ports.Cut.Attempt)", "view, err := Boot(ctx, p.Endpoint, ports.Cut.Attempt); view, err := Boot(ctx, p.Endpoint, ports.Cut.Attempt)", 1),
	} {
		if _, err := attachRollbackReadyObserver(bad); err == nil {
			t.Fatal("missing, stale or duplicate ready barrier supplied an observer")
		}
	}
}
