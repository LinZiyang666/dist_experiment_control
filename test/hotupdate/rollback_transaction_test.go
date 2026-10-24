//go:build linux && hotupdate_integration

package hotupdate_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agentimage"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradeartifact"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/upgradecli"
	"github.com/LinZiyang666/tether/internal/upgradelock"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/internal/upgradewire"
	"github.com/nats-io/nats.go"
)

const rollbackIncarnation = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

// Installer retrofit precedes daemon start. It publishes no artifacts, selector
// or completion; only the target's subsequent Describe/Prepare grants a cut.
// origin: S8 internal review TAGGED-SUITE-SWEEP-A F4 / hot-update S8 plan §5.2
func rollbackRegisterDomain(t *testing.T, f *rollbackFixture) {
	t.Helper()
	role := upgradecli.AgentBootRole(f.home, "rollback", "node", uint32(os.Geteuid()))
	if err := os.MkdirAll(filepath.Join(role.ControlDir, "artifacts"), 0700); err != nil {
		t.Fatal(err)
	}
	reg, err := upgradelock.RegisterDomain(upgradelock.Registration{CanonicalPath: f.bin, ControlDir: role.ControlDir,
		DeploymentID: "rollback-fixture", Participants: []upgradelock.Participant{{Key: role.Want, ServiceID: "tether-agent@rollback"}}})
	if err != nil {
		t.Fatal(err)
	}
	f.key = role.Want
	f.key.DomainID = reg
	f.journalDir = filepath.Dir(role.Journal(f.key, upgradelock.Registration{}))
}

// The external control fixture relays the actual nonce-bound heartbeat through
// the agent's real forwarded subscriber. A canned proof would bypass SUB/Flush.
// origin: S8 internal review TAGGED-SUITE-SWEEP-A F4 / hot-update S8 plan §4.2
func rollbackInstallControlProbe(t *testing.T, f *rollbackFixture) {
	t.Helper()
	_, err := f.nc.Subscribe(proto.SubjNodeHeartbeat("rollback", "node"), func(m *nats.Msg) {
		if m.Reply == "" {
			return
		}
		var heartbeat proto.HeartbeatPayload
		if json.Unmarshal(m.Data, &heartbeat) != nil || heartbeat.ControlProbeNonce == "" {
			f.recordError(fmt.Errorf("control probe lacks its heartbeat nonce"))
			return
		}
		body, _ := json.Marshal(map[string]string{"query_nonce": heartbeat.ControlProbeNonce})
		msg, err := f.nc.Request(proto.SubjCmdForwarded("rollback", "node", proto.ClaimProbeVerb), body, upgradebudget.RequestLatency)
		if err != nil {
			f.recordError(fmt.Errorf("control fixture could not traverse current subscriber: %w", err))
			return
		}
		if err := m.Respond(msg.Data); err != nil {
			f.recordError(err)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
}

func (f *rollbackFixture) transaction(req upgradewire.Request) upgradewire.Response {
	f.t.Helper()
	body, err := upgradewire.EncodeForwarded(req, "fixture")
	if err != nil {
		f.t.Fatal(err)
	}
	var resp upgradewire.Response
	if req.Op == upgradewire.OpQuery {
		// Read replies can be lost when the original process is replaced.
		// Like the product wait command, retry only this read with a new nonce
		// inside the enclosing wait's original budget; never reissue Start.
		ctx, cancel := context.WithTimeout(f.t.Context(), upgradebudget.RequestLatency)
		defer cancel()
		msg, err := f.nc.RequestWithContext(ctx, proto.SubjCmdForwarded("rollback", "node", "upgrade-tx"), body)
		if err != nil {
			return upgradewire.Response{Code: upgradetx.CodeUnsettled}
		}
		if err := json.Unmarshal(msg.Data, &resp); err != nil {
			f.t.Fatal("transaction query response decode failed")
		}
	} else {
		f.request("upgrade-tx", json.RawMessage(body), &resp)
	}
	if resp.Code != "" && resp.Code != upgradetx.CodeUnsettled && resp.Code != upgradetx.CodeDeferred {
		f.t.Fatalf("transaction %s refused: %s: %s", req.Op, resp.Code, resp.Error)
	}
	return resp
}

func (f *rollbackFixture) view() (upgradetx.View, bool) {
	f.t.Helper()
	r := f.transaction(upgradewire.Request{Op: upgradewire.OpQuery, Key: f.key,
		TxID: f.current.TxID, QueryNonce: rollbackRandom(f.t)[:32]})
	if r.Code != "" {
		return r.View, false
	}
	if h := r.View.Observation.Current.Handle; h.TxID != "" && h.TxID != f.current.TxID {
		f.current = h // observe an automatic reverse; never reissue an intent
		return f.view()
	}
	return r.View, true
}

// Both cuts use authorized transactions. The first is target-issued
// Describe/Prepare -> real stage CLI -> same-grant Start. The second is the
// current target's independent reverse, bound to its recorded previous SHA.
// origin: S8 internal review TAGGED-SUITE-SWEEP-A F4 / hot-update S8 plan §8.1
func (f *rollbackFixture) upgrade(index int) {
	f.t.Helper()
	if index == 0 {
		view, ok := f.view()
		if !ok || upgradetx.Completed(view.Observation.Current, view.Observation) != nil {
			f.t.Fatal("reverse requires the actual completed forward")
		}
		a := view.Observation.Current
		scope := a.Scope
		scope.Expected = append([]upgradetx.Resource(nil), view.Observation.Census.Resources...)
		f.reverseCurrent(view, scope)
		return
	}
	d := f.transaction(upgradewire.Request{Op: upgradewire.OpDescribe,
		Key: upgradewire.DescribeSelector(f.key), QueryNonce: rollbackRandom(f.t)[:32]})
	if d.Code != "" || d.Description == nil || d.Description.Key != f.key {
		f.t.Fatalf("target did not describe its live original scope: %+v", d)
	}
	a := f.artifacts.b
	release, caps, err := agentimage.ProbeCapabilities(a.path, testharness.RunProcess)
	if err != nil {
		f.t.Fatal(err)
	}
	request, err := upgradecli.BuildPrepare(upgradecli.Target{Key: d.Description.Key, SourceSHA: d.Description.RunningSHA,
		SourceRelease: d.Description.SourceRelease, Scope: d.Description.Scope}, upgradecli.Candidate{Path: a.path, SHA: a.sha,
		Release: release, CapabilityDigest: upgradecli.CapabilityDigest(upgradetx.ComponentAgent, caps)}, "fixture", time.Now())
	if err != nil {
		f.t.Fatal(err)
	}
	r := f.transaction(upgradewire.Request{Op: upgradewire.OpPrepare, Key: f.key, Prepare: request})
	if r.Code != "" {
		f.t.Fatalf("target refused prepare: %+v", r)
	}
	grant := r.View.Historical.Grant
	f.current = grant.Handle
	f.watchdogDeadline = time.Unix(0, grant.ExpiresAt).Add(upgradebudget.ComponentHandover)
	dir := f.t.TempDir()
	grantPath, receiptPath := filepath.Join(dir, "grant.json"), filepath.Join(dir, "receipt.json")
	body, err := json.Marshal(grant)
	if err != nil {
		f.t.Fatal(err)
	}
	if err := os.WriteFile(grantPath, body, 0600); err != nil {
		f.t.Fatal(err)
	}
	cmd := exec.CommandContext(f.t.Context(), f.artifacts.a.path, "upgrade", "stage", "--grant", grantPath,
		"--source", fmt.Sprintf("/proc/%d/exe", f.pid), "--candidate", a.path, "--receipt-out", receiptPath)
	cmd.Env = rollbackEnvironment(f.home)
	if out, err := testharness.CombinedOutput(cmd); err != nil {
		f.t.Fatalf("real authorized stage failed: %v: %s", err, out)
	}
	body, err = os.ReadFile(receiptPath)
	if err != nil {
		f.t.Fatal(err)
	}
	var receipt upgradeartifact.Receipt
	if err := json.Unmarshal(body, &receipt); err != nil {
		f.t.Fatal(err)
	}
	r = f.transaction(upgradewire.Request{Op: upgradewire.OpStart, Key: f.key, Grant: grant, Artifact: receipt})
	if r.Code != "" && r.Code != upgradetx.CodeUnsettled {
		f.t.Fatalf("same-grant Start was not accepted: %+v", r)
	}
}

func (f *rollbackFixture) reverseCurrent(view upgradetx.View, scope upgradetx.Scope) {
	f.t.Helper()
	a := view.Observation.Current
	expected := upgradetx.Fence{Current: a.Handle, Owner: view.Observation.Census.Stamp.Producer,
		RunningSHA: view.Observation.RunningSHA, PreviousSHA: view.Historical.RecordedPreviousSHA, Incarnation: rollbackIncarnation}
	r := f.transaction(upgradewire.Request{Op: upgradewire.OpRollback, Key: f.key, Expected: expected,
		Reverse: upgradetx.ReverseRequest{ClientNonce: rollbackRandom(f.t), ActorFP: "fixture", HistoricalTxID: view.Historical.Grant.Handle.TxID,
			TargetSHA: f.artifacts.a.sha, Scope: scope, ExpiresAt: time.Now().Add(upgradebudget.ComponentHandover).UnixNano()}})
	if r.Code != "" && r.Code != upgradetx.CodeUnsettled {
		f.t.Fatalf("reverse was not accepted: %+v", r)
	}
	if h := r.View.Historical.Reverse.Handle; h.TxID != "" {
		f.current = h
	}
}

// An explicit reverse of a serving but uncommitted cut shares the watchdog's
// current-export path. It keeps the watchdog's original scope and therefore
// exercises the pending-cut boundary independently of the later timer expiry.
// origin: S8 internal review TAGGED-SUITE-SWEEP-A F4 / hot-update S8 plan §8.1
func TestReadyPendingReverseKeepsCurrentFlowSSState(t *testing.T) {
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))
	fd, g := testharness.FDCount(), runtime.NumGoroutine()
	var artifacts *rollbackArtifacts
	for round := 0; round < 5; round++ {
		exercisePendingFlowSSReverse(t, &artifacts)
	}
	testharness.AssertNoFDLeak(t, "pending current flow/SS reverse x5", fd, 0)
	testharness.AssertNoGoroutineLeak(t, "pending current flow/SS reverse x5", g)
}

func exercisePendingFlowSSReverse(t *testing.T, artifacts **rollbackArtifacts) {
	t.Helper()
	f := newRollbackFixture(t, artifacts, true)
	defer f.close()
	f.upgrade(1)
	f.wait("native pending completion failure", 20*time.Second, func() bool {
		body, _ := os.ReadFile(f.output)
		return strings.Contains(string(body), "ROLLBACK_COMMIT_FAILED")
	})
	view, ok := f.view()
	if !ok || view.Observation.Current.Phase == upgradetx.PhaseCompleted || f.commitCount() != 0 {
		t.Fatal("pending reverse lost the real completion failure")
	}
	if err := os.Chmod(f.journalDir, 0700); err != nil {
		t.Fatal(err)
	}
	first := f.cut(0)
	f.requireCut(first, 2)
	before := f.progress()
	f.awaitSamples(before.raw + 20)
	f.requireAdvanced(before)
	f.reverseCurrent(view, view.Observation.Current.Scope)
	f.awaitImage(f.artifacts.a.sha, 3)
	f.requireCurrentCut(first, f.cut(1))
	f.requireOriginalBusiness()
}

func (f *rollbackFixture) awaitImage(sha string, generation uint64) {
	f.t.Helper()
	var completed upgradetx.View
	f.wait("running image and three durable transaction receipts", 30*time.Second, func() bool {
		view, ok := f.view()
		o := view.Observation
		ready := ok && o.Current.Handle.Generation == generation && o.RunningSHA == sha &&
			upgradetx.Completed(o.Current, o) == nil && rollbackSHA(fmt.Sprintf("/proc/%d/exe", f.pid)) == sha
		if ready {
			completed = view
		}
		return ready
	})
	if rollbackStartTicks(f.t, f.pid) != f.startTicks {
		f.t.Fatal("authorized cut changed original PID/start ticks")
	}
	// The immutable canonical dispatcher stays at A. Effective and /proc SHA
	// agree through the transaction, rather than a v1 rename of that dispatcher.
	if rollbackSHA(f.bin) != f.artifacts.a.sha {
		f.t.Fatal("transaction replaced the registered immutable dispatcher")
	}
	body, _ := os.ReadFile(f.log)
	// imported_resources counts exec/PTY tasks, not descriptors or flows.
	// This cohort owns two flow domains and one SS actor, with zero tasks.
	for _, row := range completed.Observation.Census.Resources {
		if row.Domain == upgradetx.DomainExec || row.Domain == upgradetx.DomainPTY {
			f.t.Fatal("current resource receipt unexpectedly replaced the original task-free cohort")
		}
	}
	for _, line := range strings.Split(string(body), "\n") {
		var entry struct {
			Message string `json:"msg"`
		}
		if json.Unmarshal([]byte(line), &entry) != nil {
			continue
		}
		var gotGeneration uint64
		var tasks int
		var freezeMS int64
		if n, _ := fmt.Sscanf(entry.Message, "agent: image activated generation=%d tasks=%d freeze_window_ms=%d", &gotGeneration, &tasks, &freezeMS); n == 3 && gotGeneration == generation && tasks == 0 && freezeMS > 0 {
			f.t.Logf("generation %d freeze_window_ms=%d calibration_exceeded=%v", generation, freezeMS, time.Duration(freezeMS)*time.Millisecond > upgradebudget.LocalFreezeWindow)
			return
		}
	}
	f.t.Fatal("actual activation lacks current task ownership or a positive freeze window")
}

// Fault the actual completion CAS on disk, after all completion receipts have
// been collected. The permission failure leaves the business ACTIVE and the
// journal pending; the product watchdog must export CURRENT state at its own
// unchanged absolute deadline. No marker or completion is synthesized here.
// origin: S8 internal review TAGGED-SUITE-SWEEP-A F4 / hot-update S8 plan §8.1
func rollbackCompletionOverlay(t *testing.T, root, dir string, replacements map[string]string) {
	t.Helper()
	path := filepath.Join(root, "internal/agentupgrade/boot.go")
	read := path
	if replacement := replacements[path]; replacement != "" {
		read = replacement
	}
	body, err := os.ReadFile(read)
	if err != nil {
		t.Fatal(err)
	}
	source := string(body)
	const needle = "return e.apply(&record, upgradetx.Event{Phase: upgradetx.PhaseCompleted, CutStage: upgradetx.CutReleased, Outcome: upgradetx.OutcomeSuccess})"
	if strings.Count(source, needle) != 1 {
		t.Fatal("actual v2 completion CAS observer attachment moved")
	}
	source = strings.Replace(source, "\"reflect\"", "\"reflect\"\n\"fmt\"\n\"os\"\n\"path/filepath\"", 1)
	source = strings.Replace(source, needle, "return rollbackCompletionObservation(record, func() error { "+needle+" })", 1)
	source += `
func rollbackCompletionObservation(record upgradetx.Record, complete func() error) error {
 home := os.Getenv("TETHER_HOME")
 armed := filepath.Join(home, "rollback-fault")
 if err := os.Rename(armed, armed + ".entered"); err == nil {
  directory := filepath.Join(home, "agents", record.Key.SID, "upgrades", record.Key.DomainID, record.Key.ConfiguredNID)
  if err := os.Chmod(directory, 0555); err != nil { panic("completion fault permission setup failed") }
 }
 err := complete()
 if err == nil { fmt.Fprintln(os.Stdout, "ROLLBACK_COMPLETED", current(record).Direction, current(record).Handle.Generation) }
 if err != nil { fmt.Fprintln(os.Stdout, "ROLLBACK_COMMIT_FAILED") }
 return err
}
`
	patched := filepath.Join(dir, "agent-transaction-completion.go")
	if err := os.WriteFile(patched, []byte(source), 0600); err != nil {
		t.Fatal(err)
	}
	replacements[path] = patched
	// Keep the native refusal visible in this private fixture; no error is
	// converted into a watchdog success or an alternate recovery operation.
	path = filepath.Join(root, "internal/agentupgrade/wiring.go")
	read = path
	if replacement := replacements[path]; replacement != "" {
		read = replacement
	}
	body, err = os.ReadFile(read)
	if err != nil {
		t.Fatal(err)
	}
	source = string(body)
	const watchdog = `logger.Error("agent: transaction watchdog retained unresolved evidence")`
	if strings.Count(source, watchdog) != 1 {
		t.Fatal("actual watchdog refusal observer attachment moved")
	}
	source = strings.Replace(source, watchdog, `logger.Error("agent: transaction watchdog retained unresolved evidence", "error", err)`, 1)
	patched = filepath.Join(dir, "agent-transaction-watchdog.go")
	if err := os.WriteFile(patched, []byte(source), 0600); err != nil {
		t.Fatal(err)
	}
	replacements[path] = patched
	path = filepath.Join(root, "internal/agentupgrade/rollback.go")
	read = path
	if replacement := replacements[path]; replacement != "" {
		read = replacement
	}
	body, err = os.ReadFile(read)
	if err != nil {
		t.Fatal(err)
	}
	source = string(body)
	const unresolved = `return e.markUnknown(reverse, "current reverse cut remains unresolved")`
	if strings.Count(source, unresolved) != 1 {
		t.Fatal("current reverse refusal observer attachment moved")
	}
	source = strings.Replace(source, `"errors"`, "\"errors\"\n\"fmt\"\n\"os\"", 1)
	source = strings.Replace(source, unresolved, `fmt.Fprintln(os.Stdout, "ROLLBACK_REVERSE_ERROR", err); `+unresolved, 1)
	patched = filepath.Join(dir, "agent-transaction-reverse.go")
	if err := os.WriteFile(patched, []byte(source), 0600); err != nil {
		t.Fatal(err)
	}
	replacements[path] = patched
}
