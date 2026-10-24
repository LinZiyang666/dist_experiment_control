//go:build linux

// upgrade_cut_custody_test.go — the S8 adjunct and its inherited domain lease
// through the exact bootstrap code the exec path runs (FromCut + DupSource).
// Real: upgradelock registration/Acquire/Adopt, handoff resource shape, the
// agentimage cut. Stubbed: the exec itself (P23 owns the real same-PID exec).
// origin: hot-update S8 plan §5.1 / §6.1 / §11.2 P17
package agentboot

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agentimage"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/handoff/handofftest"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradecut"
	"github.com/LinZiyang666/tether/internal/upgradelock"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"golang.org/x/sys/unix"
)

// Adjunct and lease must reach the activation owner before closeAuxiliary or
// the source's close can release the reservation (M08), and an import that has
// not verified the required adjunct must not take a business descriptor.
// origin: hot-update S8 plan §5.1 / §6.1 / IL05 / DC05 / §11.2 P17
func TestBootAdoptsUpgradeLeaseBeforeActivationCleanup(t *testing.T) {
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(strconv.Itoa(round), func(t *testing.T) { exerciseBootUpgradeLease(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "boot upgrade lease x5", beforeG)
	testharness.AssertNoFDLeak(t, "boot upgrade lease x5", beforeFD, 0)
}

func exerciseBootUpgradeLease(t *testing.T) {
	f := newCutFixture(t, "upgrade-cut-instance")
	key, control, exporter := acquireBootUpgradeLease(t)
	state := bootUpgradeState(t, key)
	img, frozen, src := cutWithUpgradeAdjunct(t, f, state, exporter.FD())
	// Model the exec: the exporting wrapper is gone, only the handoff copy
	// still holds the original OFD and therefore the reservation.
	if err := exporter.Close(); err != nil {
		t.Fatal(err)
	}
	requireDomainHeld(t, control, key, true)
	files := src.Remaining()
	if _, err := img.Restore(f.target()); !errors.Is(err, errUpgradeCutNotImported) || img.Phase() != PhaseHeld || src.Remaining() != files {
		t.Fatalf("restore without the verified adjunct: err=%v phase=%q taken=%d", err, img.Phase(), files-src.Remaining())
	}
	if cut, err := img.ImportUpgradeCut(nil); cut != nil || err == nil {
		t.Fatal("adjunct accepted without the caller's expectations")
	}
	if cut, err := img.ImportUpgradeCut(func(upgradecut.State) error { return errors.New("journal names another cut") }); cut != nil || err == nil || img.UpgradeCut() != nil {
		t.Fatal("adjunct accepted against the caller's journal")
	}
	cut, err := img.ImportUpgradeCut(func(got upgradecut.State) error {
		if got.Attempt != state.Attempt || got.TargetSHA != state.TargetSHA {
			return errors.New("unexpected cut")
		}
		got.Authorities[0].Incarnation = strings.Repeat("0", 64) // a copy cannot alter the image
		return nil
	})
	if err != nil || cut == nil || img.UpgradeCut().Authorities[0] != state.Authorities[0] {
		t.Fatalf("import verified adjunct: %v", err)
	}
	if again, err := img.ImportUpgradeCut(func(upgradecut.State) error { return nil }); again != nil || err == nil {
		t.Fatal("adjunct imported twice")
	}
	imp, err := img.Restore(f.target())
	if err != nil {
		t.Fatal(err)
	}
	if img.CutStage() != upgradetx.CutPristine || img.TakeUpgradeLease() != nil {
		t.Fatal("pristine import released its lease or claimed activation")
	}
	if err := frozen.Discard(); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := img.Activate(); err != nil {
		t.Fatal(err)
	}
	for _, resource := range src.Borrow() {
		if resource.ID == upgradecut.ResourceID {
			t.Fatal("activation left the lease in the handoff source")
		}
	}
	if img.CutStage() != upgradetx.CutActive {
		t.Fatalf("completed activation stage %q", img.CutStage())
	}
	lease := img.TakeUpgradeLease()
	if lease == nil || lease.DomainID() != key.DomainID || img.TakeUpgradeLease() != nil {
		t.Fatal("activation did not transfer exactly one adopted lease")
	}
	requireDomainHeld(t, control, key, true)
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	requireDomainHeld(t, control, key, false)
	ctx, cancel := context.WithTimeout(context.Background(), imageTestBudget)
	defer cancel()
	if err := imp.Runtime.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

// A refused Take is final and keeps custody: activation cannot retry with a
// second owner of the FD, pristine redispatch carries it onward, and only the
// terminal abandon releases it.
// origin: hot-update S8 plan §5.1 / §8.1 / M08 / §11.2 P17
func TestBootRefusedUpgradeLeaseKeepsPristineCustody(t *testing.T) {
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(strconv.Itoa(round), func(t *testing.T) { exerciseBootRefusedUpgradeLease(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "boot refused lease x5", beforeG)
	testharness.AssertNoFDLeak(t, "boot refused lease x5", beforeFD, 0)
}

func exerciseBootRefusedUpgradeLease(t *testing.T) {
	f := newCutFixture(t, "refused-lease-instance")
	// A domain nobody registered: Adopt must refuse, after the FD left the source.
	key := upgradetx.Key{DomainID: strings.Repeat("7", 32), Component: upgradetx.ComponentAgent, SID: "lab", ConfiguredNID: "node", UID: uint32(os.Getuid())}
	read, write, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = write.Close() }()
	img, frozen, _ := cutWithUpgradeAdjunct(t, f, bootUpgradeState(t, key), read.Fd())
	if err := read.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := img.ImportUpgradeCut(func(upgradecut.State) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if _, err := img.Restore(f.target()); err != nil {
		t.Fatal(err)
	}
	for attempt := 0; attempt < 2; attempt++ {
		var unsettled *upgradetx.Error
		if _, _, _, err := img.Activate(); !errors.As(err, &unsettled) || unsettled.Code != upgradetx.CodeUnsettled {
			t.Fatalf("attempt %d activated without an adopted lease: %v", attempt, err)
		}
		if img.Phase() != PhaseImported || img.CutStage() != upgradetx.CutPristine || img.TakeUpgradeLease() != nil {
			t.Fatalf("refused take left phase %q stage %q", img.Phase(), img.CutStage())
		}
	}
	if readers := pipeReaders(t, write); readers != 1 {
		t.Fatalf("refused take kept %d readers, want the one retained custody", readers)
	}
	prepared, err := img.Repack()
	if err != nil {
		t.Fatalf("pristine repack after a refused take: %v", err)
	}
	// The redispatch carries the same pipe (the lease's OFD) onward: Prepare
	// owns one more reader until it is closed or executed.
	if readers := pipeReaders(t, write); readers != 2 {
		_ = prepared.Close()
		t.Fatalf("redispatch carried %d readers of the retained custody, want 2", readers)
	}
	if err := prepared.Close(); err != nil {
		t.Fatal(err)
	}
	img.Abandon(nil, CrashWindowNoRollbackTarget, "refused lease")
	if readers := pipeReaders(t, write); readers != 0 {
		t.Fatalf("terminal abandon kept %d readers of the lease custody", readers)
	}
	if err := frozen.Thaw(); err != nil {
		t.Fatal(err)
	}
}

// Committing PhaseActive before the first pump moves is partial_active, not
// pristine: a recovery that consulted CutStage must export the current owners
// and must never be able to repack the original snapshot.
// origin: hot-update S8 plan §4.1 / §8.1 / §11.2 P17 mutant "partial uses old snapshot"
func TestBootPartialActivationIsNeverPristine(t *testing.T) {
	for _, tc := range []struct {
		phase     Phase
		activated bool
		stage     upgradetx.CutStage
		repack    bool
	}{
		{PhaseNone, false, upgradetx.CutNone, false},
		{PhaseHeld, false, upgradetx.CutPristine, true},
		{PhaseRefused, false, upgradetx.CutPristine, true},
		{PhaseImported, false, upgradetx.CutPristine, true},
		{PhaseActive, false, upgradetx.CutPartialActive, false},
		{PhaseActive, true, upgradetx.CutActive, false},
		{PhaseReady, true, upgradetx.CutActive, false},
		{PhaseLost, false, upgradetx.CutNone, false},
	} {
		img := &Image{phase: tc.phase, activated: tc.activated}
		if got := img.CutStage(); got != tc.stage {
			t.Errorf("%s activated=%t: stage %q, want %q", tc.phase, tc.activated, got, tc.stage)
		}
		// Pristine phases may still be refused for want of a bundle here; the
		// invariant is that no non-pristine stage ever reaches a repack.
		if !tc.repack {
			if prepared, err := img.Repack(); err == nil || prepared != nil {
				t.Errorf("%s activated=%t repacked the original snapshot", tc.phase, tc.activated)
			}
		}
	}
	var none *Image
	if none.CutStage() != upgradetx.CutNone {
		t.Error("absent image reported a cut")
	}
}

// An adopted lease stays with the image in every pristine phase (a failed
// preactivation must still repack the same OFD); only committed activation
// transfers it, once.
// origin: hot-update S8 plan §5.1 / M08 / §11.2 P17
func TestBootKeepsAdoptedLeaseUntilActivationCommits(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(strconv.Itoa(round), func(t *testing.T) { exerciseBootLeaseRelease(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "boot lease release x5", beforeG)
	testharness.AssertNoFDLeak(t, "boot lease release x5", beforeFD, 0)
}

func exerciseBootLeaseRelease(t *testing.T) {
	key, control, exporter := acquireBootUpgradeLease(t)
	state := bootUpgradeState(t, key)
	duplicate, err := unix.FcntlInt(exporter.FD(), unix.F_DUPFD_CLOEXEC, 3)
	if err != nil {
		t.Fatal(err)
	}
	inherited := os.NewFile(uintptr(duplicate), "inherited-lease")
	if err := exporter.Close(); err != nil {
		t.Fatal(err)
	}
	resource, err := upgradecut.Pack(state, map[uint32]*os.File{state.LockFD: inherited})
	if err != nil {
		t.Fatal(err)
	}
	custody, err := upgradecut.Admit([]handoff.Resource{resource}, upgradetx.ComponentAgent, func(upgradecut.State) error { return nil })
	if err != nil || custody == nil {
		t.Fatalf("admit: %v", err)
	}
	if err := custody.Take(func(string, string) (*os.File, error) { return inherited, nil }); err != nil {
		t.Fatal(err)
	}
	img := &Image{upgrade: custody}
	for _, phase := range []Phase{PhaseHeld, PhaseRefused, PhaseImported, PhaseLost, PhaseNone} {
		img.phase = phase
		if img.TakeUpgradeLease() != nil {
			t.Fatalf("%s image released its adopted lease", phase)
		}
	}
	requireDomainHeld(t, control, key, true)
	img.phase = PhaseActive
	lease := img.TakeUpgradeLease()
	if lease == nil || img.TakeUpgradeLease() != nil {
		t.Fatal("committed activation did not transfer the lease exactly once")
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	requireDomainHeld(t, control, key, false)
}

func cutWithUpgradeAdjunct(t *testing.T, f *cutFixture, state upgradecut.State, leaseFD uintptr) (*Image, *agentimage.Frozen, *handofftest.DupSource) {
	t.Helper()
	frozen, err := agentimage.Freeze(agentimage.Owners{Runtime: f.rt, Manager: f.manager, Lineage: agentimage.Lineage{Generation: 2}}, time.Now().Add(imageTestBudget))
	if err != nil {
		t.Fatal(err)
	}
	_, resources, err := f.rt.Export()
	if err != nil {
		t.Fatal(err)
	}
	duplicate, err := unix.FcntlInt(leaseFD, unix.F_DUPFD_CLOEXEC, 3)
	if err != nil {
		t.Fatal(err)
	}
	borrowed := os.NewFile(uintptr(duplicate), "exported-lease")
	defer func() { _ = borrowed.Close() }()
	adjunct, err := upgradecut.Pack(state, map[uint32]*os.File{state.LockFD: borrowed})
	if err != nil {
		t.Fatal(err)
	}
	all := append(append([]handoff.Resource(nil), resources...), adjunct)
	src := handofftest.DupResources(t, all)
	b := frozen.Bundle()
	return FromCut(b, b.Processes.Records, src, all), frozen, src
}

func acquireBootUpgradeLease(t *testing.T) (upgradetx.Key, string, upgradelock.Lease) {
	t.Helper()
	control, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(control, "tether")
	if err := os.WriteFile(binary, []byte("fixture image"), 0o700); err != nil {
		t.Fatal(err)
	}
	key := upgradetx.Key{Component: upgradetx.ComponentAgent, SID: "lab", ConfiguredNID: "node", UID: uint32(os.Getuid())}
	id, err := upgradelock.RegisterDomain(upgradelock.Registration{CanonicalPath: binary, ControlDir: control, DeploymentID: "boot-upgrade-cut",
		Participants: []upgradelock.Participant{{Key: key, ServiceID: "agent-fixture"}}})
	if err != nil {
		t.Fatal(err)
	}
	key.DomainID = id
	ctx, cancel := context.WithTimeout(t.Context(), imageTestBudget)
	defer cancel()
	lease, err := upgradelock.Acquire(ctx, key, "")
	if err != nil {
		t.Fatal(err)
	}
	return key, control, lease
}

func bootUpgradeState(t *testing.T, key upgradetx.Key) upgradecut.State {
	t.Helper()
	boot, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		t.Fatal(err)
	}
	stat, err := os.ReadFile("/proc/self/stat")
	if err != nil {
		t.Fatal(err)
	}
	line := string(stat)
	ticks, err := strconv.ParseUint(strings.Fields(line[strings.LastIndexByte(line, ')')+1:])[19], 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	owner := upgradetx.Owner{BootID: strings.TrimSpace(string(boot)), PID: os.Getpid(), StartTicks: ticks, InstanceID: strings.Repeat("8", 32)}
	return upgradecut.State{
		Attempt:   upgradetx.Handle{Key: key, TxID: strings.Repeat("3", 32), CutID: strings.Repeat("4", 32), Generation: 2, Revision: 1},
		SourceSHA: strings.Repeat("a", 64), TargetSHA: strings.Repeat("b", 64),
		Authorities: []upgradecut.Authority{{SID: key.SID, Incarnation: strings.Repeat("c", 64)}}, Owner: owner,
		Scope: upgradetx.Scope{ID: strings.Repeat("6", 32), Participants: []upgradetx.Key{key},
			Domains: []upgradetx.Domain{upgradetx.DomainAuthority, upgradetx.DomainControl, upgradetx.DomainExec, upgradetx.DomainUpgrade}, Expected: []upgradetx.Resource{}},
	}
}

// requireDomainHeld uses an independent OFD: it neither borrows the custody
// nor re-flocks through it, so it detects both an early unlock and a leak.
func requireDomainHeld(t *testing.T, control string, key upgradetx.Key, held bool) {
	t.Helper()
	contender, err := os.OpenFile(filepath.Join(control, key.DomainID+".lease"), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = contender.Close() }()
	err = unix.Flock(int(contender.Fd()), unix.LOCK_EX|unix.LOCK_NB)
	if blocked := errors.Is(err, unix.EWOULDBLOCK); blocked != held || (!blocked && err != nil) {
		t.Fatalf("domain reservation held=%t, want %t: %v", blocked, held, err)
	}
}

// pipeReaders counts this process's descriptors naming the pipe's inode,
// excluding the write end itself.
func pipeReaders(t *testing.T, write *os.File) int {
	t.Helper()
	var st syscall.Stat_t
	if err := syscall.Fstat(int(write.Fd()), &st); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	target := fmt.Sprintf("pipe:[%d]", st.Ino)
	count := 0
	for _, entry := range entries {
		if link, err := os.Readlink("/proc/self/fd/" + entry.Name()); err == nil && link == target {
			count++
		}
	}
	return count - 1
}
