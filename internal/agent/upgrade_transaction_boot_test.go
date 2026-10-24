//go:build linux

// upgrade_transaction_boot_test.go — the target half of an S8 cut before
// control-ready: the carried adjunct is verified against the configured key,
// the running bytes and the accepted journal before Restore takes a business
// descriptor; the inherited lease reaches the commit owner only after
// activation; and a constructor that fails before activation returns the
// pristine image to the recorded source bytes instead of the v1 recovery.
// The image is built by the exact in-process path the exec path runs
// (agentboot.FromCut over a duplicating source); the exec itself is P23's.
// origin: hot-update S8 plan §3.3 / §5.1 / §6.1 / §8.1 / §11.2 P17; S8 FIX-AGENTWIRE
package agent

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"

	"github.com/LinZiyang666/tether/internal/agentboot"
	"github.com/LinZiyang666/tether/internal/agentupgrade"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/handoff/handofftest"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradeartifact"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/upgradecut"
	"github.com/LinZiyang666/tether/internal/upgradelock"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"golang.org/x/sys/unix"
)

// acceptedCut drives a real granted Start to its final re-export and captures
// the adjunct it would carry. The returned exec leaves the journal holding
// that accepted cut (partial_active) and releases the exporter's reservation.
func acceptedCut(t *testing.T) (*Agent, *grantedRoute, upgradetx.Grant, upgradeartifact.Receipt, upgradecut.State) {
	t.Helper()
	src := cutTestAgent(t)
	nc := cutNATS(t)
	r := openGrantedRoute(t, src, false)
	grant, receipt := r.publish(t, src, nc, s8Script("boot target "+src.instanceID, s8Domains))
	var state upgradecut.State
	src.cfg.UpgradeExecFn = func(string) error {
		state, _ = preparedAdjunct(t)
		return syscall.ENOEXEC
	}
	if resp := r.start(t, src, nc, grant, receipt); resp.Code != upgradetx.CodeUnsettled || state.Attempt.TxID != grant.Handle.TxID {
		t.Fatalf("source did not reach its final re-export: %+v", resp)
	}
	r.detach(src)
	return src, r, grant, receipt, state
}

// adjunctImage is an exporter's frozen empty runtime plus the adjunct and a
// duplicate of the re-acquired domain lease, as the handoff would deliver it.
// The exporter's own lease wrapper is closed: only the image holds the OFD,
// so a held image keeps the reservation until release ends that custody.
func adjunctImage(t *testing.T, state upgradecut.State) (*agentboot.Image, func()) {
	t.Helper()
	exporter := newBootAgent(t, "nats://127.0.0.1:1", "exporter")
	frozen := freezeBootAgent(t, exporter)
	_, resources, err := exporter.processRuntime.Export()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), upgradebudget.SafePointWait)
	defer cancel()
	lease, err := upgradelock.Acquire(ctx, state.Attempt.Key, "")
	if err != nil {
		t.Fatal(err)
	}
	duplicate, err := unix.FcntlInt(lease.FD(), unix.F_DUPFD_CLOEXEC, 3)
	if err != nil {
		t.Fatal(err)
	}
	borrowed := os.NewFile(uintptr(duplicate), "exported-lease")
	adjunct, err := upgradecut.Pack(state, map[uint32]*os.File{state.LockFD: borrowed})
	if err != nil {
		t.Fatal(err)
	}
	all := append(append([]handoff.Resource(nil), resources...), adjunct)
	source := handofftest.DupResources(t, all)
	_ = borrowed.Close()
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	b := frozen.Bundle()
	img := agentboot.FromCut(b, b.Processes.Records, source, source.Borrow())
	var once sync.Once
	release := func() {
		once.Do(func() {
			img.Abandon(nil, agentboot.CrashWindowNoRollbackTarget, "test")
			source.Close()
			_ = frozen.Discard()
		})
	}
	t.Cleanup(release)
	return img, release
}

func bootTarget(t *testing.T, img *agentboot.Image, ports *UpgradeDependencies, running string) (*Agent, error) {
	t.Helper()
	a, err := New(Config{SID: "lab", NID: "cut-agent", Home: t.TempDir(), NATSURL: "nats://127.0.0.1:1", Logger: testharness.SilentLog(),
		RemoteFSMode: "off", UpgradeNoExit: true, BootImage: img, Upgrade: ports, UpgradeRunningImagePath: running,
		UpgradeExecutablePath: filepath.Join(t.TempDir(), "tether")})
	if err == nil {
		cleanupBootAgent(t, a)
	}
	return a, err
}

// origin: hot-update S8 plan §5.1 / §6.1 / §11.2 P17 / M08; S8 FIX-AGENTWIRE.
// Import accepts only the configured key, the running target (or the pristine
// return to its source) and the journal's accepted, unsettled activation;
// every other shape is refused while the image stays held. The forward
// arrival captures the watchdog fence from the journal, and the lease moves
// to the commit owner only once activation committed.
func TestTransactionBootImportsOnlyTheAcceptedCut(t *testing.T) {
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))
	beforeFD, beforeG := testharness.FDCount(), runtime.NumGoroutine()
	for round := 0; round < 5; round++ {
		t.Run(strconv.Itoa(round), func(t *testing.T) { exerciseTransactionBootImport(t) })
	}
	testharness.AssertNoFDLeak(t, "transaction boot import x5", beforeFD, 0)
	testharness.AssertNoGoroutineLeak(t, "transaction boot import x5", beforeG)
}

func exerciseTransactionBootImport(t *testing.T) {
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))
	src, r, grant, receipt, state := acceptedCut(t)
	unrelated := writeScript(t, "#!/bin/sh\n# unrelated running bytes\n")
	otherKey := r.key
	otherKey.ConfiguredNID = "other-agent"
	// The same key's journal elsewhere never accepted this cut.
	owner, err := agentupgrade.ProcessOwner(src.instanceID)
	if err != nil {
		t.Fatal(err)
	}
	foreign, err := agentupgrade.OpenJournal(filepath.Join(r.control, "foreign-state.json"), filepath.Join(r.control, r.key.DomainID+".guard"), r.key, owner, 1)
	if err != nil {
		t.Fatal(err)
	}
	// Every refusal leaves the SAME image held and untaken: it is offered
	// again to the next constructor and finally imported.
	img, release := adjunctImage(t, state)
	defer release()
	for _, refused := range []struct {
		name    string
		ports   *UpgradeDependencies
		running string
	}{
		{"unconfigured", nil, receipt.TargetPath},
		{"other_bytes", &UpgradeDependencies{Key: r.key, Store: r.store}, unrelated},
		{"other_key", &UpgradeDependencies{Key: otherKey, Store: r.store}, receipt.TargetPath},
		{"unaccepted_journal", &UpgradeDependencies{Key: r.key, Store: foreign}, receipt.TargetPath},
	} {
		if _, err := bootTarget(t, img, refused.ports, refused.running); err == nil || img.Phase() != agentboot.PhaseHeld || img.UpgradeCut() != nil {
			t.Fatalf("%s: unverified adjunct imported: err=%v phase=%s", refused.name, err, img.Phase())
		}
		if _, carried := img.CarriedUpgradeCut(); !carried {
			t.Fatalf("%s: refused import lost the carried adjunct", refused.name)
		}
		r.requireDomainHeld(t, true)
	}
	a, err := bootTarget(t, img, &UpgradeDependencies{Key: r.key, Store: r.store}, receipt.TargetPath)
	if err != nil {
		t.Fatal(err)
	}
	ports := a.cfg.Upgrade
	fence := ports.WatchdogFence
	// The watchdog keeps the accepted grant's absolute deadline; only the
	// automatic reverse request it may issue carries the handover window.
	// origin: S8 internal review activation-and-route-migration (single handover window)
	if ports.Cut == nil || !upgradetx.SameCut(ports.Cut.Attempt, grant.Handle) || ports.Cut.TargetSHA != grant.TargetSHA ||
		!upgradetx.SameCut(fence.Current, grant.Handle) || fence.Owner != state.Owner || fence.RunningSHA != grant.TargetSHA ||
		fence.PreviousSHA != grant.SourceSHA || fence.Incarnation != state.Authorities[0].Incarnation ||
		ports.WatchdogDeadline != grant.ExpiresAt || ports.WatchdogReverse.ExpiresAt != grant.ExpiresAt+int64(upgradebudget.ComponentHandover) ||
		agentIncarnation(a) != state.Authorities[0].Incarnation {
		t.Fatal("the forward arrival did not bind its fence, deadline and incarnation to the accepted journal")
	}
	if img.Phase() != agentboot.PhaseImported || img.CutStage() != upgradetx.CutPristine || ports.Lease != nil {
		t.Fatalf("pristine import moved the lease or activation: phase=%s", img.Phase())
	}
	// The completion registry is bound to this cut, not the source's refusal.
	row := upgradetx.Resource{Domain: upgradetx.DomainExec, ID: "late:" + a.instanceID, Origin: "late", Owner: state.Owner,
		Generation: grant.Handle.Generation, State: "running", Custody: "target"}
	sequence, err := ports.Registry.Begin(row)
	if err != nil || sequence == 0 || ports.Registry.Finish(sequence, row) != nil {
		t.Fatalf("imported cut has no completion registry: %v", err)
	}
	if err := activateImage(a); err != nil {
		t.Fatal(err)
	}
	if ports.Lease == nil || ports.Lease.DomainID() != r.key.DomainID || img.TakeUpgradeLease() != nil {
		t.Fatal("activation did not hand exactly one adopted lease to the commit owner")
	}
	r.requireDomainHeld(t, true)
	if err := ports.Lease.Close(); err != nil {
		t.Fatal(err)
	}
	ports.Lease = nil
	r.requireDomainHeld(t, false)
}

// origin: hot-update S8 plan §3.3 / §8.1; S8 FIX-AGENTWIRE. A constructor that
// fails before activation must not enter the v1 marker recovery: the target
// returns the SAME pristine image, adjunct and lease included, to the source
// pin the accepted journal recorded. Without a verifiable source, or when this
// process already is the source, the only safe end is the crash window.
func TestFinishBootReturnsTransactionImageToSource(t *testing.T) {
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))
	beforeFD, beforeG := testharness.FDCount(), runtime.NumGoroutine()
	for round := 0; round < 5; round++ {
		t.Run(strconv.Itoa(round), func(t *testing.T) { exerciseTransactionFinishBoot(t) })
	}
	testharness.AssertNoFDLeak(t, "transaction finish boot x5", beforeFD, 0)
	testharness.AssertNoGoroutineLeak(t, "transaction finish boot x5", beforeG)
}

func exerciseTransactionFinishBoot(t *testing.T) {
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))
	src, r, grant, receipt, state := acceptedCut(t)
	ports := &UpgradeDependencies{Key: r.key, Store: r.store}
	old := bootRedispatchExec
	previous := bootTransactionPorts.Load()
	t.Cleanup(func() { bootRedispatchExec = old; bootTransactionPorts.Store(previous) })
	marker := upgradeMarkerPath(src.cfg.UpgradeExecutablePath)
	for _, tc := range []struct {
		name    string
		ports   *UpgradeDependencies
		running string
		want    string // "" abandons without a redispatch
	}{
		{"returns_to_source", ports, grant.TargetSHA, receipt.SourcePath},
		{"already_source", ports, grant.SourceSHA, ""},
		{"no_journal", nil, grant.TargetSHA, ""},
		{"wrong_bytes", ports, strings.Repeat("e", 64), ""},
	} {
		img, release := adjunctImage(t, state)
		bootTransactionPorts.Store(tc.ports)
		dispatched := []string{}
		bootRedispatchExec = func(p *handoff.Prepared, env []string, fallback func(string) error) func(string) error {
			return func(path string) error {
				if p == nil {
					t.Error("transaction return fell back to a bare exec")
				} else if carried, _ := preparedAdjunct(t); !upgradetx.SameCut(carried.Attempt, state.Attempt) {
					t.Error("the pristine return lost its adjunct")
				}
				dispatched = append(dispatched, path)
				return syscall.ENOEXEC
			}
		}
		carriedState, carried := img.CarriedUpgradeCut()
		if !carried {
			t.Fatal("FinishBoot would route an S8 dispatch to the v1 recovery")
		}
		finishTransactionBoot(img, carriedState, slog.New(slog.DiscardHandler), errors.New("unknown flag: fixture"), tc.running)
		if tc.want != "" && (len(dispatched) != 1 || dispatched[0] != tc.want) || tc.want == "" && len(dispatched) != 0 {
			t.Fatalf("%s: redispatch=%v want %q", tc.name, dispatched, tc.want)
		}
		if img.Phase() != agentboot.PhaseLost || img.CrashWindow() != agentboot.CrashWindowNoRollbackTarget {
			t.Fatalf("%s: a returned redispatch kept ownership: phase=%s", tc.name, img.Phase())
		}
		if _, err := os.Stat(marker); !os.IsNotExist(err) {
			t.Fatalf("%s: the transaction return wrote a v1 marker", tc.name)
		}
		release()
	}
	// The journal is untouched by either outcome: recovery stays with Current.
	r.requireRecovery(t, grant, receipt)
}
