//go:build linux

// transfer_upgrade_test.go exercises real upgrade gates, marker fsync and the
// composed runtime cut. The transfer port records wiring calls; P06/P10 actor
// semantics and the real 120-second watchdog are exercised by P32's T38.
package agent

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agentimage"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/xferagent"
	"github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xferworker"
	"github.com/nats-io/nats.go/jetstream"
)

// origin: hot-update S7 plan §8.4 / P21 / T38 wiring
func TestTransferUpgradeRequiresPreparedAndCourierDomains(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseTransferUpgradeDomains(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "transfer upgrade domains", beforeG)
	testharness.AssertNoFDLeak(t, "transfer upgrade domains", beforeFD, 0)
}

func exerciseTransferUpgradeDomains(t *testing.T) {
	for _, phase := range []string{"prepared", "courier", "retained_empty"} {
		t.Run(phase, func(t *testing.T) {
			a := cutTestAgent(t)
			if transferImageWorker(a) != nil {
				t.Fatal("an unbuilt transfer handler supplied an image owner")
			}
			installUpgradeTransferOwner(t, a, &upgradeTransferPort{})
			required := requiredUpgradeDomains(a)
			if !strings.Contains(strings.Join(required, " "), xfercontract.Version) || imageIsEmpty(a) {
				t.Fatal("retained transfer owner was omitted from the upgrade domain")
			}
			weak := agentimage.Capabilities{ABI: 1, Domains: []string{"exec-v1", "pty-v1", "flow-v1", "ss-v1"}}
			if err := checkCandidateABI(a, weak); !errors.Is(err, errImageABIUnsupported) || !strings.Contains(err.Error(), xfercontract.Version) {
				t.Fatalf("old reader admitted retained transfer state: %v", err)
			}
			weak.Domains = append(weak.Domains, xfercontract.Version)
			if err := checkCandidateABI(a, weak); err != nil {
				t.Fatal(err)
			}
			if got := imageOwners(a); got.Transfers == nil {
				t.Fatal("upgrade cut omitted the handler's bound worker")
			}
			// Capable admission is represented by the worker. Expose and legacy
			// push caches still veto the cut independently of that owner.
			release, ok := admitTransferCreation(a)
			if !ok {
				t.Fatal("capable creation was refused without a pending reader")
			}
			if reasons := preflightDeferrals(a); len(reasons) != 0 {
				t.Fatalf("capable admission was treated as legacy: %v", reasons)
			}
			release()
			legacy, ok := a.gate.admit(gateTransfer)
			if !ok {
				t.Fatal("legacy admission setup failed")
			}
			if reasons := preflightDeferrals(a); !containsTransferDeferral(reasons) {
				t.Fatal("legacy transfer admission lost its deferral")
			}
			legacy()
			a.pushCommitCache["legacy-prepared"] = pushCommitEntry{added: a.upgradeNow(), size: 1}
			if reasons := preflightDeferrals(a); !containsTransferDeferral(reasons) {
				t.Fatal("legacy prepared entry lost its deferral")
			}
			delete(a.pushCommitCache, "legacy-prepared")
			expose, ok := a.gate.admit(gateExpose)
			if !ok {
				t.Fatal("expose admission setup failed")
			}
			if reasons := preflightDeferrals(a); !containsTransferDeferral(reasons) {
				t.Fatal("control-only expose lost its deferral")
			}
			expose()
		})
	}
}

func containsTransferDeferral(reasons []agentimage.DeferredError) bool {
	for _, reason := range reasons {
		if reason.Reason == agentimage.ReasonTransfersInFlight {
			return true
		}
	}
	return false
}

// origin: hot-update S7 plan §8.4 / P21 / T38 wiring
func TestTransferPendingPrevAdmissionRequiresDurableCommit(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseTransferPendingAdmission(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "pending transfer admission", beforeG)
	testharness.AssertNoFDLeak(t, "pending transfer admission", beforeFD, 0)
}

func exerciseTransferPendingAdmission(t *testing.T) {
	for _, arm := range []string{"old_prev", "capable_prev", "file_sync_failure", "directory_sync_failure", "bad_prev_sha", "sibling"} {
		t.Run(arm, func(t *testing.T) {
			a := cutTestAgent(t)
			cutImportActive(t, a)
			installUpgradeTransferOwner(t, a, &upgradeTransferPort{})
			m := cutPending(t, a)
			domains := []string{"exec-v1", "pty-v1", "flow-v1", "ss-v1"}
			if arm == "capable_prev" {
				domains = append(domains, xfercontract.Version)
			}
			dst := a.cfg.UpgradeExecutablePath
			if err := os.WriteFile(upgradePrevPath(dst), []byte(domainScript(strings.Join(domains, " "), 1)), 0755); err != nil {
				t.Fatal(err)
			}
			m.PrevSHA = mustSHA(t, upgradePrevPath(dst))
			m.PrevImageSHA256, m.PrevImageABI, m.PrevImageDomains = m.PrevSHA, 1, domains
			if arm == "bad_prev_sha" {
				m.PrevImageSHA256 = "wrong-bytes"
			}
			if err := writeUpgradeMarker(upgradeMarkerPath(dst), m); err != nil {
				t.Fatal(err)
			}
			a.upgradeBootProofID = m.UpgradeID
			if arm == "sibling" {
				a.instanceID = "other-instance"
				a.upgradeBootProofID = ""
			}
			if err := initPTYUpgradeAdmission(a); (err != nil) != (arm == "bad_prev_sha") {
				t.Fatalf("pending initialization: %v", err)
			}
			blocked := arm != "capable_prev" && arm != "sibling"
			requireTransferCreationAdmission(t, a, blocked)
			if arm != "bad_prev_sha" && (checkFlowUpgradeAdmission(a) != nil || checkSSUpgradeAdmission(a) != nil) {
				t.Fatal("a transfer-only restriction blocked unrelated capable domains")
			}
			a.image.MarkReady()
			a.gate.close()
			a.gate.open()
			releaseFlowSSUpgradeAdmission(a, "foreign-upgrade")
			requireTransferCreationAdmission(t, a, blocked)
			if blocked && !flowSSAdmissionPending(a, m.UpgradeID) {
				t.Fatal("transfer-only gate was omitted from durable commit retry")
			}
			original := upgradeSyncObserver
			defer func() { upgradeSyncObserver = original }()
			if strings.HasSuffix(arm, "sync_failure") {
				upgradeSyncObserver = func(kind, path string) error {
					if arm == "file_sync_failure" && kind == "file" && strings.HasPrefix(filepath.Base(path), ".tether-upgrade-marker-") ||
						arm == "directory_sync_failure" && kind == "dir" && path == filepath.Dir(dst) {
						return syscall.EIO
					}
					return nil
				}
			}
			a.commitUpgradeAfterRegister(upgradeStateCommitted)
			if strings.HasSuffix(arm, "sync_failure") {
				requireTransferCreationAdmission(t, a, true)
				upgradeSyncObserver = original
				a.commitUpgradeAfterRegister(upgradeStateCommitted)
			}
			requireTransferCreationAdmission(t, a, false)
		})
	}
}

func requireTransferCreationAdmission(t *testing.T, a *Agent, blocked bool) {
	t.Helper()
	if errors.Is(checkTransferUpgradeAdmission(a), procrequest.ErrBusy) != blocked {
		t.Fatalf("transfer creation guard differs, blocked=%v", blocked)
	}
	release, admitted := admitTransferCreation(a)
	if admitted {
		release()
	}
	if admitted == blocked {
		t.Fatalf("transfer creation admission=%v want blocked=%v", admitted, blocked)
	}
}

// origin: hot-update S7 plan §8.2 / P21 / T38 wiring
func TestTransferReturningExecRebindsBeforeThaw(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseTransferReturningExec(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "returning transfer exec", beforeG)
	testharness.AssertNoFDLeak(t, "returning transfer exec", beforeFD, 0)
}

func exerciseTransferReturningExec(t *testing.T) {
	a := cutTestAgent(t)
	boot, err := procrequest.CurrentBootID()
	if err != nil {
		t.Fatal(err)
	}
	owner := &upgradeTransferPort{state: xfercontract.WorkerState{Schema: xfercontract.StateSchema,
		Lineage: xfercontract.Lineage{InstanceID: a.instanceID, BootID: boot, Generation: 1}}}
	installUpgradeTransferOwner(t, a, owner)
	a.gate.close()
	cut, err := freezeForUpgrade(a, time.Now().Add(cutTestWait))
	if err != nil {
		t.Fatal(err)
	}
	if cut.frozen.Bundle().Transfers == nil {
		t.Fatal("agent cut lost the retained transfer domain")
	}
	// The exec attempt replaced the control session: thaw must bind the
	// CURRENT JetStream, resolved at the call, not the one seen at the cut.
	before := owner.thawed
	current := &upgradeJetStream{}
	a.js = current
	if err := cut.abandon(a); err != nil {
		t.Fatal(err)
	}
	if owner.thawed.JS != current || owner.thawed.JS == before.JS || owner.thawed.Transport == nil || owner.thawed.Authority == nil ||
		owner.thawed.OwnerGeneration != 1 || owner.freezeUntil <= 0 || owner.thawUntil != owner.freezeUntil {
		t.Fatalf("returning exec thaw refreshed the deadline or used the old backend: %+v", owner.thawed)
	}
	if !reflect.DeepEqual(requiredUpgradeDomains(a), cut.frozen.Bundle().RequiredDomains()) {
		t.Fatal("live and frozen transfer domain declarations disagree")
	}
	requireTransferCreationAdmission(t, a, false)
}

// This port observes composition only. Unexpected actor operations fail loudly
// so these tests cannot sign P06 actor behavior or substitute a contract stub.
type upgradeTransferPort struct {
	state                  xfercontract.WorkerState
	freezeUntil, thawUntil controlbudget.Deadline
	thawed                 xferworker.Bindings
}

// upgradeJetStream is only an identity for the bound backend.
type upgradeJetStream struct{ jetstream.JetStream }

// installUpgradeTransferOwner builds the production handler runtime around the
// recording port, exactly as an imported worker is adopted: the image owner is
// then the handler's bound worker, not a second registration.
func installUpgradeTransferOwner(t *testing.T, a *Agent, owner *upgradeTransferPort) {
	t.Helper()
	a.cfg.TransferWorker = transferWorkerDependencies{Imported: func(*xferagent.Runtime) (xferworker.Manager, error) { return owner, nil }}
	h := transferHandler(a)
	if h.Runtime == nil || h.Worker != owner || transferImageWorker(a) == nil {
		t.Fatalf("handler did not bind the transfer owner: %v", h.WorkerError)
	}
}

func (*upgradeTransferPort) Prepare(xfercontract.Prepare, controlbudget.Deadline) (xfercontract.Status, error) {
	panic("unexpected transfer prepare")
}
func (*upgradeTransferPort) Start(xfercontract.Identity) error { panic("unexpected transfer start") }
func (*upgradeTransferPort) Apply(xfercontract.Control, controlbudget.Deadline) (xfercontract.Status, error) {
	panic("unexpected transfer control")
}
func (*upgradeTransferPort) Status(xfercontract.Identity) (xfercontract.Status, error) {
	panic("unexpected transfer status")
}
func (p *upgradeTransferPort) Freeze(deadline controlbudget.Deadline) (xferworker.Frozen, error) {
	p.freezeUntil, p.state.Admission.Closed, p.state.Admission.Generation = deadline, true, p.state.Lineage.Generation
	return p, nil
}
func (p *upgradeTransferPort) Export() (xfercontract.WorkerState, []handoff.Resource, error) {
	return p.state, nil, nil
}
func (*upgradeTransferPort) DetachPristine() (xfercontract.Salvage, error) {
	panic("unexpected transfer detach")
}
func (p *upgradeTransferPort) Thaw(bindings xferworker.Bindings, deadline controlbudget.Deadline) error {
	p.thawed, p.thawUntil = bindings, deadline
	return nil
}
func (*upgradeTransferPort) Stop(controlbudget.Deadline) error { return nil }
