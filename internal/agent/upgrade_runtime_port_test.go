//go:build linux

// upgrade_runtime_port_test.go — the agent's production S8 RuntimePort driven
// through the real journal, domain lease, root publication, strict forwarded
// route and engine. Only the final exec is a hook: a same-PID exec belongs to
// the P23 integration, and every returning exec here proves the source stayed.
// origin: hot-update S8 plan §2.2 / §5.1 / §6.1 / §7.1 / §11.2 P17; S8 FIX-AGENTWIRE
package agent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agentupgrade"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradeartifact"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/upgradecut"
	"github.com/LinZiyang666/tether/internal/upgradelock"
	"github.com/LinZiyang666/tether/internal/upgradeport"
	"github.com/LinZiyang666/tether/internal/upgradestore"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/internal/upgradewire"
	"github.com/nats-io/nats.go"
	"golang.org/x/sys/unix"
)

// s8Domains is this binary's own reader set plus the required adjunct domain.
const s8Domains = "exec-v1 pty-v1 flow-v1 ss-v1 broker-image-v1 transfer-v1"

// s8Script declares a complete S8 reader. The capability advertisement is
// test-only: the production emitter does not advertise it until the broker
// exporter and authority writers exist (S8 plan §6.1, P13/P19 coordination).
func s8Script(tag, domains string) []byte {
	return []byte("#!/bin/sh\n# " + tag + "\necho '" + proto.VersionLine() + "'\necho 'image-domains " + domains +
		" upgrade-cut-v1'\necho 'image-features upgrade-tx-v1 authority-incarnation-v1 upgrade-cut-v1'\necho 'image-read-epochs 1'\necho '" +
		proto.ImageABILine() + "'\n")
}

func shaOf(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// grantedRoute is one agent's real journal (OpenJournal: legacy projection
// reader enabled), registered domain, root publication and the production
// wiring installed by initAgentUpgrade: Complete supplies the agent's own
// RuntimePort and registry exactly as a configured agent gets them.
type grantedRoute struct {
	key      upgradetx.Key
	store    *upgradestore.Store
	control  string
	location upgradeartifact.Location
	source   []byte
	actor    string
}

// openGrantedRoute keeps the journal in the domain control directory, the
// readonly-bin layout, so legacy marker assertions at the executable stay
// independent; marker=true shares the writable install's .tether-upgrade.json.
func openGrantedRoute(t *testing.T, a *Agent, marker bool) *grantedRoute {
	t.Helper()
	return openGrantedRouteWith(t, a, s8Script("running source "+a.instanceID, s8Domains), marker)
}

func openGrantedRouteWith(t *testing.T, a *Agent, source []byte, marker bool) *grantedRoute {
	t.Helper()
	canon := func(path string) string {
		resolved, err := filepath.EvalSymlinks(path)
		if err != nil {
			t.Fatal(err)
		}
		return resolved
	}
	control, slots := canon(t.TempDir()), canon(t.TempDir())
	// Installation-controlled directories admit no group or other writer.
	for _, dir := range []string{control, slots} {
		if err := os.Chmod(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	dst := canon(a.cfg.UpgradeExecutablePath)
	a.cfg.UpgradeExecutablePath = dst
	a.cfg.UpgradeRunningImagePath = filepath.Join(canon(t.TempDir()), "running-image")
	if err := os.WriteFile(a.cfg.UpgradeRunningImagePath, source, 0o755); err != nil {
		t.Fatal(err)
	}
	key := upgradetx.Key{Component: upgradetx.ComponentAgent, SID: a.cfg.SID, ConfiguredNID: a.cfg.NID, UID: uint32(os.Geteuid())}
	id, err := upgradelock.RegisterDomain(upgradelock.Registration{CanonicalPath: dst, ControlDir: control,
		DeploymentID: "agent-transaction-" + a.instanceID, Participants: []upgradelock.Participant{{Key: key, ServiceID: "agent-" + a.instanceID}}})
	if err != nil {
		t.Fatal(err)
	}
	key.DomainID = id
	owner, err := agentupgrade.ProcessOwner(a.instanceID)
	if err != nil {
		t.Fatal(err)
	}
	journal := filepath.Join(control, "upgrade-state.json")
	if marker {
		journal = upgradeMarkerPath(dst)
	}
	store, err := agentupgrade.OpenJournal(journal, filepath.Join(control, id+".guard"), key, owner, a.imageGeneration)
	if err != nil {
		t.Fatal(err)
	}
	// A registered fixture keeps the incarnation its broker actually issued;
	// only an agent that never registered gets the placeholder authority.
	// origin: S8 FIX-DESCRIBE (P24 recreation fixture over the real adjunct)
	if agentIncarnation(a) == "" {
		incarnation := strings.Repeat("7", 64)
		a.incarnation.Store(&incarnation)
	}
	a.cfg.Upgrade = &UpgradeDependencies{Key: key, Store: store}
	if err := initAgentUpgrade(a, nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { a.cfg.Upgrade, a.upgradeEndpoint = nil, nil })
	return &grantedRoute{key: key, store: store, control: control, source: source, actor: "SHA256:transaction-fixture-actor",
		location: upgradeartifact.Location{CanonicalPath: dst, Directory: slots}}
}

// publication stands in for the root stage helper's trusted registration: it
// authorizes the fixed location and reserves through the real domain lease.
type publication struct {
	location upgradeartifact.Location
}

func (p publication) Authorize(context.Context, upgradetx.Grant) (upgradeartifact.Location, error) {
	return p.location, nil
}
func (p publication) Reserve(ctx context.Context, g upgradetx.Grant) (upgradelock.Lease, error) {
	return upgradelock.Acquire(ctx, g.Handle.Key, "")
}
func (p publication) CheckImage(_ context.Context, path, sha, release, digest string) (string, error) {
	return shaOf([]byte(path + "\x00" + sha + "\x00" + release + "\x00" + digest)), nil
}

func (r *grantedRoute) prepareRequest(candidate []byte) upgradewire.Request {
	domains := []upgradetx.Domain{upgradetx.DomainAuthority, upgradetx.DomainBrokerRuntime, upgradetx.DomainBrokerStore, upgradetx.DomainControl, upgradetx.DomainExec,
		upgradetx.DomainFlow, upgradetx.DomainPTY, upgradetx.DomainSS, upgradetx.DomainTransferTracker, upgradetx.DomainTransferWorker, upgradetx.DomainUpgrade}
	return upgradewire.Request{Op: upgradewire.OpPrepare, Key: r.key, Prepare: upgradetx.PrepareRequest{ClientNonce: upgradeNonce(), ActorFP: r.actor,
		Mode: upgradetx.ModeHot, SourceRelease: proto.ReleaseVersion, TargetRelease: proto.ReleaseVersion, SourceSHA: shaOf(r.source),
		TargetSHA: shaOf(candidate), CapabilityDigest: strings.Repeat("c", 64), ExpiresAt: time.Now().Add(upgradebudget.ComponentHandover).UnixNano(),
		Scope: upgradetx.Scope{ID: upgradeNonce(), Participants: []upgradetx.Key{r.key}, Domains: domains}}}
}

// publish issues the inert grant through the strict route and stages both
// immutable SHA slots for it, as `tether upgrade stage` would.
func (r *grantedRoute) publish(t *testing.T, a *Agent, nc *nats.Conn, candidate []byte) (upgradetx.Grant, upgradeartifact.Receipt) {
	t.Helper()
	resp := r.send(t, a, nc, r.prepareRequest(candidate), upgradebudget.RequestLatency)
	grant := resp.View.Historical.Grant
	if resp.Code != "" || grant.Handle.Key != r.key || grant.TargetSHA != shaOf(candidate) {
		t.Fatalf("prepare refused: code=%s error=%s", resp.Code, resp.Error)
	}
	ctx := upgradeartifact.WithPublicationPort(t.Context(), publication{r.location})
	receipt, err := upgradeartifact.Publish(ctx, grant, bytes.NewReader(r.source), bytes.NewReader(candidate))
	if err != nil {
		t.Fatal(err)
	}
	return grant, receipt
}

func (r *grantedRoute) start(t *testing.T, a *Agent, nc *nats.Conn, grant upgradetx.Grant, receipt upgradeartifact.Receipt) upgradewire.Response {
	t.Helper()
	return r.send(t, a, nc, upgradewire.Request{Op: upgradewire.OpStart, Key: r.key, Grant: grant, Artifact: receipt}, cutTestWait)
}

// send drives one request through the agent's real forwarded dispatcher with
// the production endpoint initAgentUpgrade bound.
func (r *grantedRoute) send(t *testing.T, a *Agent, nc *nats.Conn, req upgradewire.Request, wait time.Duration) upgradewire.Response {
	t.Helper()
	a.setRunCtx(t.Context())
	a.proxyDrainMu.Lock()
	draining := a.proxyDraining
	a.proxyDraining = false
	a.proxyDrainMu.Unlock()
	defer func() {
		a.proxyDrainMu.Lock()
		a.proxyDraining = draining
		a.proxyDrainMu.Unlock()
	}()
	subject := proto.SubjCmdForwarded(a.cfg.SID, nidOf(a), "upgrade-tx")
	sub, err := nc.Subscribe(subject, func(msg *nats.Msg) {
		if !dispatchAgentUpgrade(a, nc, msg) {
			t.Error("transaction was not dispatched")
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := nc.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	body, err := upgradewire.EncodeForwarded(req, r.actor)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), wait)
	defer cancel()
	msg, err := nc.RequestWithContext(ctx, subject, body)
	if err != nil {
		t.Fatal(err)
	}
	a.upgradeRequestWG.Wait()
	resp, err := upgradewire.DecodeResponse(msg.Data)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

// current reads the journal's Current; absence is the grant-unknown refusal.
func (r *grantedRoute) current() (upgradetx.View, error) { return r.store.Load(r.key, "") }

// detach removes the transaction wiring so a legacy fixture in the same agent
// keeps its retired-handler semantics (a wired agent refuses legacy re-exec).
func (r *grantedRoute) detach(a *Agent) { a.cfg.Upgrade, a.upgradeEndpoint = nil, nil }

// requireUnaccepted proves a refusal happened before acceptance: no record,
// publication action, selector or Current exists for the grant.
func (r *grantedRoute) requireUnaccepted(t *testing.T, grant upgradetx.Grant) {
	t.Helper()
	for _, tx := range []string{grant.Handle.TxID, ""} {
		_, err := r.store.Load(r.key, tx)
		var refusal *upgradetx.Error
		if !errors.As(err, &refusal) || refusal.Code != upgradetx.CodeGrantUnknown {
			t.Fatalf("refused Start left an accepted record (tx=%q): %v", tx, err)
		}
	}
}

// lateRuntime times a late admission right after the first read-only
// Preflight. Every operation still runs the production adapter unchanged.
type lateRuntime struct {
	upgradeport.RuntimePort
	preflights  int
	after       func()
	afterFreeze func()
}

func (r *lateRuntime) Preflight(ctx context.Context, g upgradetx.Grant, receipt upgradeartifact.Receipt) (upgradeport.PreflightProof, error) {
	proof, err := r.RuntimePort.Preflight(ctx, g, receipt)
	r.preflights++
	if r.preflights == 1 && err == nil && r.after != nil {
		r.after()
	}
	return proof, err
}

func (r *lateRuntime) Freeze(h upgradetx.Handle) (upgradeport.Cut, error) {
	cut, err := r.RuntimePort.Freeze(h)
	if err == nil && r.afterFreeze != nil {
		r.afterFreeze()
	}
	return cut, err
}

// interpose rebinds the production endpoint over lateRuntime.
func (r *grantedRoute) interpose(a *Agent, after func()) *lateRuntime {
	late := &lateRuntime{RuntimePort: a.cfg.Upgrade.Runtime, after: after}
	a.cfg.Upgrade.Runtime = late
	a.upgradeEndpoint = agentupgrade.New(r.store, late, a.cfg.Upgrade.Registry)
	return late
}

// requireRecovery proves a post-activation-intent failure kept Current: the
// record stays the accepted transaction in recovery, the effective selector
// and both pins are the source's, and the reservation was released by thaw.
func (r *grantedRoute) requireRecovery(t *testing.T, grant upgradetx.Grant, receipt upgradeartifact.Receipt) {
	t.Helper()
	view, err := r.current()
	if err != nil {
		t.Fatal(err)
	}
	h := view.Historical
	if h.Grant.Handle.TxID != grant.Handle.TxID || h.Forward.Phase != upgradetx.PhaseRecoveryRequired || h.Forward.CutStage != upgradetx.CutPartialActive ||
		h.EffectiveSHA != grant.SourceSHA || h.RecordedPreviousSHA != grant.SourceSHA {
		t.Fatalf("failure after the activation intent released or misreported Current: phase=%s stage=%s", h.Forward.Phase, h.Forward.CutStage)
	}
	pinned := strings.Join(view.Metadata.Pins, "\n")
	if !strings.Contains(pinned, receipt.SourcePath) || !strings.Contains(pinned, receipt.TargetPath) {
		t.Fatal("recovery lost the source or target pin")
	}
	r.requireDomainHeld(t, false)
}

// requireCancelled proves an R0 failure before activation: the source owner
// was thawed and re-observed, and the record is a proven cancellation.
func (r *grantedRoute) requireCancelled(t *testing.T, grant upgradetx.Grant) {
	t.Helper()
	view, err := r.current()
	if err != nil {
		t.Fatal(err)
	}
	h := view.Historical
	if h.Grant.Handle.TxID != grant.Handle.TxID || h.Forward.Phase != upgradetx.PhaseCancelled || h.Forward.CutStage != upgradetx.CutNone ||
		h.Cancellation == nil || !upgradetx.CancellationSettled(h) || h.EffectiveSHA != grant.SourceSHA {
		t.Fatalf("pre-activation failure is not a proven cancellation: phase=%s stage=%s", h.Forward.Phase, h.Forward.CutStage)
	}
	r.requireDomainHeld(t, false)
}

// requireDomainHeld probes with an independent OFD, so it detects both an
// early unlock and a leaked reservation without borrowing either.
func (r *grantedRoute) requireDomainHeld(t *testing.T, held bool) {
	t.Helper()
	contender, err := os.OpenFile(filepath.Join(r.control, r.key.DomainID+".lease"), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = contender.Close() }()
	err = unix.Flock(int(contender.Fd()), unix.LOCK_EX|unix.LOCK_NB)
	if blocked := errors.Is(err, unix.EWOULDBLOCK); blocked != held || (!blocked && err != nil) {
		t.Fatalf("domain reservation held=%t, want %t: %v", blocked, held, err)
	} else if !blocked {
		_ = unix.Flock(int(contender.Fd()), unix.LOCK_UN)
	}
}

type preparedFile struct {
	Name string `json:"name"`
	Dev  uint64 `json:"dev"`
	Ino  uint64 `json:"ino"`
}

type preparedResource struct {
	ID    string          `json:"id"`
	Kind  string          `json:"kind"`
	Data  json.RawMessage `json:"data"`
	Files []preparedFile  `json:"files"`
}

// preparedAdjunct reads the live sealed manifest the exec would carry and
// returns its single upgrade-cut resource; nothing private is printed.
func preparedAdjunct(t *testing.T) (upgradecut.State, preparedResource) {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		path := "/proc/self/fd/" + entry.Name()
		if name, _ := os.Readlink(path); !strings.HasPrefix(name, "/memfd:tether-handoff") {
			continue
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var manifest struct {
			Resources []preparedResource `json:"resources"`
		}
		if json.Unmarshal(data, &manifest) != nil {
			t.Fatal("invalid prepared manifest")
		}
		var found []preparedResource
		for _, resource := range manifest.Resources {
			if resource.ID == upgradecut.ResourceID || resource.Kind == upgradecut.Version {
				found = append(found, resource)
			}
		}
		if len(found) != 1 {
			t.Fatalf("prepared handoff carries %d upgrade adjuncts, want 1", len(found))
		}
		var raw []byte
		if err := json.Unmarshal(found[0].Data, &raw); err != nil {
			raw = found[0].Data
		}
		state, err := upgradecut.Decode(raw)
		if err != nil {
			t.Fatal("prepared adjunct does not decode")
		}
		return state, found[0]
	}
	t.Fatal("no prepared handoff manifest")
	return upgradecut.State{}, preparedResource{}
}

// origin: hot-update S8 plan §5.1 / §6.1 / M08 / §11.2 P17; S8 FIX-AGENTWIRE.
// A granted Start reaches the final re-export with the required adjunct naming
// exactly this cut, this process and its incarnation, and the held domain
// lease OFD as its only auxiliary file. An exec that returns leaves the source
// running: the cut is thawed, the reservation released, the business task
// alive, and the accepted transaction keeps Current for recovery.
func TestAgentTransactionCutCarriesAdjunctAndLease(t *testing.T) {
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))
	beforeFD, beforeG := testharness.FDCount(), runtime.NumGoroutine()
	for round := 0; round < 5; round++ {
		t.Run(strconv.Itoa(round), func(t *testing.T) { exerciseTransactionCut(t) })
	}
	testharness.AssertNoFDLeak(t, "agent transaction cut x5", beforeFD, 0)
	testharness.AssertNoGoroutineLeak(t, "agent transaction cut x5", beforeG)
}

func exerciseTransactionCut(t *testing.T) {
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))
	a := cutTestAgent(t)
	task := cutStartTask(t, a)
	nc := cutNATS(t)
	r := openGrantedRoute(t, a, true)
	dst := a.cfg.UpgradeExecutablePath
	before := mustSHA(t, dst)
	candidate := s8Script("candidate "+a.instanceID, s8Domains)
	grant, receipt := r.publish(t, a, nc, candidate)
	owner, err := agentupgrade.ProcessOwner(a.instanceID)
	if err != nil {
		t.Fatal(err)
	}
	execs := 0
	a.cfg.UpgradeExecFn = func(path string) error {
		execs++
		if path != receipt.TargetPath {
			t.Errorf("exec target %q is not the published candidate slot", path)
		}
		if !a.processRuntime.Frozen() || !a.workloads.Frozen() {
			t.Error("exec reached before the owners were frozen")
		}
		state, resource := preparedAdjunct(t)
		if !upgradetx.SameCut(state.Attempt, grant.Handle) || state.SourceSHA != grant.SourceSHA || state.TargetSHA != grant.TargetSHA ||
			state.Owner != owner || len(state.Authorities) != 1 || state.Authorities[0] != (upgradecut.Authority{SID: a.cfg.SID, Incarnation: agentIncarnation(a)}) {
			t.Error("adjunct does not name this cut, process and incarnation")
		}
		found := false
		for _, row := range state.Scope.Expected {
			found = found || row.Domain == upgradetx.DomainExec && row.ID == "task:"+task.pid && row.Owner == owner && row.Custody == "source"
		}
		if !found {
			t.Error("adjunct scope lost the live task identity")
		}
		var lease syscall.Stat_t
		if err := syscall.Stat(filepath.Join(r.control, r.key.DomainID+".lease"), &lease); err != nil {
			t.Fatal(err)
		}
		if len(resource.Files) != 1 || resource.Files[0].Name != upgradecut.AuxiliaryName(state.LockFD) ||
			resource.Files[0].Dev != lease.Dev || resource.Files[0].Ino != lease.Ino {
			t.Error("adjunct auxiliary file is not exactly the domain lease")
		}
		r.requireDomainHeld(t, true)
		return syscall.ENOEXEC
	}
	resp := r.start(t, a, nc, grant, receipt)
	if execs != 1 || resp.Code != upgradetx.CodeUnsettled {
		t.Fatalf("returned exec: execs=%d code=%s error=%s", execs, resp.Code, resp.Error)
	}
	view, err := r.current()
	if err != nil {
		t.Fatal(err)
	}
	h := view.Historical
	if h.Grant.Handle.TxID != grant.Handle.TxID || h.Forward.Phase != upgradetx.PhaseRecoveryRequired || h.Forward.CutStage != upgradetx.CutPartialActive ||
		h.EffectiveSHA != grant.SourceSHA || h.RecordedPreviousSHA != grant.SourceSHA {
		t.Fatalf("returned exec released or misreported Current: phase=%s stage=%s", h.Forward.Phase, h.Forward.CutStage)
	}
	pinned := strings.Join(view.Metadata.Pins, "\n")
	if !strings.Contains(pinned, receipt.SourcePath) || !strings.Contains(pinned, receipt.TargetPath) {
		t.Error("recovery lost the source or target pin")
	}
	r.requireDomainHeld(t, false)
	cutRequireOpen(t, a)
	if mustSHA(t, dst) != before {
		t.Error("an S8 cut changed the canonical executable")
	}
	cutFinishTask(t, a, task)
	// The unresolved Current refuses the next execution; it is not GC'd.
	next, nextReceipt := r.publish(t, a, nc, s8Script("second candidate "+a.instanceID, s8Domains))
	if again := r.start(t, a, nc, next, nextReceipt); again.Code == "" || execs != 1 {
		t.Fatalf("unresolved Current admitted another Start: %+v", again)
	}
}

type startResult struct {
	view upgradetx.View
	err  error
}

// exerciseGrantedInstallLeg holds the granted exec in flight and attacks the
// gap: a competing granted Start, a grantless envelope and a direct cut. The
// competing Start runs only after the first returns and is then refused by
// the unresolved journal; nothing but the first ever reaches exec.
// origin: hot-update S8 plan §3.1 / §3.2 / §4.1 / §8.1; S8 FIX-AGENTWIRE
func exerciseGrantedInstallLeg(t *testing.T) {
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))
	a := cutTestAgent(t)
	task := cutStartTask(t, a)
	nc := cutNATS(t)
	r := openGrantedRoute(t, a, false)
	grant, receipt := r.publish(t, a, nc, s8Script("granted install "+a.instanceID, s8Domains))
	competing, competingReceipt := r.publish(t, a, nc, s8Script("competing install "+a.instanceID, s8Domains))
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	var execs atomic.Int32
	a.cfg.UpgradeExecFn = func(string) error {
		if execs.Add(1) == 1 {
			close(entered)
			<-release
		}
		return syscall.ENOEXEC
	}
	endpoint := a.upgradeEndpoint
	first, second := make(chan startResult, 1), make(chan startResult, 1)
	go func() {
		view, err := endpoint.Start(t.Context(), grant, receipt)
		first <- startResult{view, err}
	}()
	select {
	case <-entered:
	case <-time.After(cutTestWait):
		t.Fatal("granted Start did not reach exec")
	}
	if legacy := cutForward(t, a, nc, proto.UpgradeForwardedReq{ReExecOnly: true, SHA256: mustSHA(t, a.cfg.UpgradeExecutablePath)}); legacy.OK || legacy.Code != "upgrade_deferred" {
		t.Errorf("grantless re-exec overlapped the granted exec: %+v", legacy)
	}
	if cut, err := freezeForUpgrade(a, time.Now().Add(20*time.Millisecond)); err == nil {
		_ = cut.abandon(a)
		t.Error("a direct cut overlapped the granted exec")
	}
	go func() {
		view, err := endpoint.Start(t.Context(), competing, competingReceipt)
		second <- startResult{view, err}
	}()
	select {
	case <-second:
		t.Error("a competing Start ran while the granted exec was in flight")
	case <-time.After(100 * time.Millisecond):
	}
	if !a.processRuntime.Frozen() || !a.workloads.Frozen() {
		t.Error("the granted exec ran with thawed owners")
	}
	r.requireDomainHeld(t, true)
	releaseOnce.Do(func() { close(release) })
	// The returned exec is accepted-unknown (74); the competing grant, issued
	// for the same source generation, can no longer start behind it.
	for i, ch := range []chan startResult{first, second} {
		select {
		case got := <-ch:
			var refusal *upgradetx.Error
			if !errors.As(got.err, &refusal) || i == 0 && refusal.Code != upgradetx.CodeUnsettled || i == 1 && refusal.Code == "" {
				t.Errorf("Start %d reported %v", i, got.err)
			}
		case <-time.After(cutTestWait):
			t.Fatal("granted Start did not return")
		}
	}
	if execs.Load() != 1 {
		t.Errorf("exec reached %d times, want only the accepted cut", execs.Load())
	}
	r.requireRecovery(t, grant, receipt)
	if _, err := r.store.Load(r.key, competing.Handle.TxID); err == nil {
		t.Error("the competing grant was accepted behind an unresolved Current")
	}
	cutRequireOpen(t, a)
	cutFinishTask(t, a, task)
}
