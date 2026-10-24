package upgradecut

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradelock"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"golang.org/x/sys/unix"
)

// origin: hot-update S8 plan §6.1 / P07
func TestAdjunctTakeValidatesBeforeConsumingFD(t *testing.T) {
	for _, defect := range stateDefects() {
		t.Run(defect.name, func(t *testing.T) {
			state := fixtureState()
			defect.mutate(&state)
			calls := 0
			lease, err := Take(state, func(uint32) (uintptr, error) {
				calls++
				return ^uintptr(0), nil
			})
			requireAdjunctRefusal(t, err)
			if calls != 0 || lease != nil {
				t.Fatal("invalid adjunct consumed auxiliary custody")
			}
		})
	}
	lease, err := Take(fixtureState(), nil)
	requireAdjunctRefusal(t, err)
	if lease != nil {
		t.Fatal("nil source fabricated a lease")
	}
	for _, fd := range []uintptr{0, 1, 2, ^uintptr(0), 1 << 31} {
		calls := 0
		lease, err := Take(fixtureState(), func(index uint32) (uintptr, error) {
			calls++
			if index != 0 {
				t.Fatal("auxiliary index zero was treated as a missing FD")
			}
			return fd, nil
		})
		requireAdjunctRefusal(t, err)
		if lease != nil || calls != 1 {
			t.Fatal("invalid OS descriptor fabricated custody or retried Take")
		}
	}
}

// origin: hot-update S8 plan §5.1 / §8.1 / P07 / M08
func TestAdjunctTakeKeepsLeaseThroughAuxiliaryCleanup(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("the inherited domain OFD and /proc witness are Linux contracts")
	}
	beforeG := runtime.NumGoroutine()
	beforeFD := testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseAdjunctTakeCleanup(t)
	}
	testharness.AssertNoGoroutineLeak(t, "adjunct Take/Adopt cleanup", beforeG)
	testharness.AssertNoFDLeak(t, "adjunct Take/Adopt cleanup", beforeFD, 0)
}

func exerciseAdjunctTakeCleanup(t *testing.T) {
	t.Helper()
	state, original := acquireFixtureLease(t)
	// Model handoff's inherited duplicate, then retire the exporting wrapper
	// before adoption. The importer cannot take a descriptor still registered
	// to the exporting lease in this same process. Both descriptors share the
	// original locked OFD; P23 owns the real exec and commit-under-lease test.
	duplicate, err := unix.FcntlInt(original.FD(), unix.F_DUPFD_CLOEXEC, 0)
	if err != nil {
		if closeErr := original.Close(); closeErr != nil {
			t.Error(closeErr)
		}
		t.Fatal(err)
	}
	if err := original.Close(); err != nil {
		if closeErr := unix.Close(duplicate); closeErr != nil {
			t.Error(closeErr)
		}
		t.Fatal(err)
	}
	fd := uintptr(duplicate)
	state.LockFD = 7
	auxiliary := map[uint32]uintptr{7: fd}
	calls := 0
	lease, err := Take(state, func(index uint32) (uintptr, error) {
		calls++
		taken, ok := auxiliary[index]
		if !ok {
			return ^uintptr(0), errors.New("auxiliary FD already consumed")
		}
		delete(auxiliary, index)
		return taken, nil
	})
	if err != nil || lease == nil {
		if lease != nil {
			_ = lease.Close()
		}
		_ = original.Close()
		t.Fatalf("adopt original OFD: %v", err)
	}
	if lease.FD() != fd || lease.DomainID() != state.Attempt.Key.DomainID || calls != 1 {
		_ = lease.Close()
		_ = original.Close()
		t.Fatal("Take reopened, substituted or consumed the lease twice")
	}
	// This is the real auxiliary closer. It may close every remaining descriptor;
	// the lock must already have been removed by Take rather than duplicated and
	// left in this list until ACTIVE/Thaw.
	for _, remaining := range auxiliary {
		_ = unix.Close(int(remaining))
	}
	if _, err := unix.FcntlInt(fd, unix.F_GETFD, 0); err != nil {
		_ = lease.Close()
		_ = original.Close()
		t.Fatal("auxiliary cleanup closed the adopted lease before commit")
	}
	// Opening /proc/self/fd makes a distinct OFD, so this witness catches both
	// an early unlock and an adoption that re-flocks the original inode. It is
	// opened only while the tested lease is still held and is closed every round.
	contender, err := os.OpenFile("/proc/self/fd/"+strconv.FormatUint(uint64(fd), 10), os.O_RDWR, 0)
	if err != nil {
		_ = lease.Close()
		_ = original.Close()
		t.Fatal(err)
	}
	blocked := unix.Flock(int(contender.Fd()), unix.LOCK_EX|unix.LOCK_NB)
	if !errors.Is(blocked, unix.EWOULDBLOCK) && !errors.Is(blocked, unix.EAGAIN) {
		_ = contender.Close()
		_ = lease.Close()
		_ = original.Close()
		t.Fatalf("independent contender acquired reservation before commit: %v", blocked)
	}
	closeErr := lease.Close()
	// The exporting wrapper is already retired; terminal close releases the
	// inherited duplicate without leaving a finalizer to close a reused number.
	if closeErr != nil {
		_ = contender.Close()
		t.Fatal(closeErr)
	}
	if err := unix.Flock(int(contender.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = contender.Close()
		t.Fatal("final reservation owner did not release the lease")
	}
	if err := contender.Close(); err != nil {
		t.Fatal(err)
	}
}

func acquireFixtureLease(t *testing.T) (State, upgradelock.Lease) {
	t.Helper()
	state := brokerFixtureState()
	sandbox := t.TempDir()
	// Both namespace paths are private to this exercise. P04 must provide the
	// real domain resolver/registration; no installed binary or user state is
	// consulted to manufacture a lease for this test.
	canonical, err := filepath.EvalSymlinks(sandbox)
	if err != nil {
		t.Fatal(err)
	}
	state.Attempt.Key.DomainID = "" // Registration supplies the canonical domain ID.
	state.Attempt.Key.ConfigPath = filepath.Join(sandbox, "broker.yaml")
	state.Attempt.Key.UID = uint32(os.Getuid())
	state.ParentToken = "" // This lane owns a standalone lease, not a parent reservation.
	state.Scope.Participants = []upgradetx.Key{state.Attempt.Key}
	if err := os.WriteFile(state.Attempt.Key.ConfigPath, []byte("mode: single\n"), 0600); err != nil {
		t.Fatal(err)
	}
	// origin: hot-update S8 plan §5.1 / LINT-s8 — acquisition needs
	// the real deployment registration, not a pathname-shaped domain ID.
	binaryPath := filepath.Join(canonical, "candidate")
	if err := os.WriteFile(binaryPath, []byte("fixture source image"), 0700); err != nil {
		t.Fatal(err)
	}
	registration := upgradelock.Registration{
		CanonicalPath: binaryPath, ControlDir: canonical, DeploymentID: "adjunct-fixture",
		Participants: []upgradelock.Participant{{Key: state.Attempt.Key, ServiceID: "broker-fixture"}},
	}
	id, err := upgradelock.RegisterDomain(registration)
	if err != nil {
		t.Fatal(err)
	}
	state.Attempt.Key.DomainID = id
	state.Scope.Participants = []upgradetx.Key{state.Attempt.Key}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	lease, err := upgradelock.Acquire(ctx, state.Attempt.Key, state.ParentToken)
	if err != nil || lease == nil {
		if lease != nil {
			_ = lease.Close()
		}
		t.Fatalf("real P04 domain lease required: %v", err)
	}
	return state, lease
}

// origin: hot-update S8 plan §5.1 / §8.1 / P07 / M08
func TestAdjunctTakePreservesFailedCustody(t *testing.T) {
	beforeG := runtime.NumGoroutine()
	beforeFD := testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseAdjunctTakeRefusal(t)
	}
	testharness.AssertNoGoroutineLeak(t, "adjunct partial Take custody", beforeG)
	testharness.AssertNoFDLeak(t, "adjunct partial Take custody", beforeFD, 0)
}

func exerciseAdjunctTakeRefusal(t *testing.T) {
	t.Helper()
	for _, failAt := range []string{"take", "adopt"} {
		readEnd, writeEnd, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		fd, err := unix.Dup(int(readEnd.Fd()))
		_ = readEnd.Close()
		if err != nil {
			_ = writeEnd.Close()
			t.Fatal(err)
		}
		calls := 0
		lease, err := Take(fixtureState(), func(index uint32) (uintptr, error) {
			calls++
			if index != 0 {
				t.Fatal("wrong auxiliary descriptor index")
			}
			if failAt == "take" {
				return uintptr(fd), errors.New("private-take-input-must-not-escape")
			}
			// A pipe cannot be adopted as a domain lock. The real P04 validator
			// must reject it; returning empty success from a stub cannot pass.
			return uintptr(fd), nil
		})
		var unsettled *upgradetx.Error
		if !errors.As(err, &unsettled) || unsettled.Code != upgradetx.CodeUnsettled || !unsettled.Accepted ||
			strings.Contains(err.Error(), "private-take-input") || lease == nil || lease.FD() != uintptr(fd) || calls != 1 {
			if lease != nil {
				_ = lease.Close()
			} else {
				_ = unix.Close(fd)
			}
			_ = writeEnd.Close()
			t.Fatalf("%s lost partial FD custody, retried or leaked diagnostics: %v", failAt, err)
		}
		if _, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0); err != nil {
			_ = lease.Close()
			_ = writeEnd.Close()
			t.Fatal("refusal closed a descriptor needed by pristine salvage")
		}
		if err := writeEnd.Close(); err != nil {
			_ = lease.Close()
			t.Fatal(err)
		}
		var byte [1]byte
		if n, err := unix.Read(fd, byte[:]); n != 0 || err != nil {
			_ = lease.Close()
			t.Fatal("salvaged FD no longer refers to the original pipe")
		}
		if err := lease.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0); !errors.Is(err, unix.EBADF) {
			t.Fatal("terminal cleanup leaked the retained descriptor")
		}
		// A second closer is an ownership error, not fake success or a raw OS
		// diagnostic that bypasses the frozen upgrade error/exit classification.
		var closeFailure *upgradetx.Error
		if err := lease.Close(); !errors.As(err, &closeFailure) || closeFailure.Code != upgradetx.CodeUnsettled || !closeFailure.Accepted {
			t.Fatalf("closed custody lost its error classification: %v", err)
		}
	}
}

// origin: hot-update S8 plan §5.1 / P07
func TestAdjunctFailedTakeCannotUnlockParent(t *testing.T) {
	beforeG := runtime.NumGoroutine()
	beforeFD := testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseAdjunctBorrowedCleanup(t)
	}
	testharness.AssertNoGoroutineLeak(t, "adjunct borrowed lease cleanup", beforeG)
	testharness.AssertNoFDLeak(t, "adjunct borrowed lease cleanup", beforeFD, 0)
}

func exerciseAdjunctBorrowedCleanup(t *testing.T) {
	t.Helper()
	parent, err := os.CreateTemp(t.TempDir(), "parent-lease-")
	if err != nil {
		t.Fatal(err)
	}
	parentOpen := true
	defer func() {
		if parentOpen {
			if err := parent.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	if err := unix.Flock(int(parent.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	duplicate, err := unix.Dup(int(parent.Fd()))
	if err != nil {
		t.Fatal(err)
	}
	lease, err := Take(fixtureState(), func(uint32) (uintptr, error) {
		return uintptr(duplicate), errors.New("take interrupted after custody moved")
	})
	if err == nil || lease == nil {
		_ = unix.Close(duplicate)
		t.Fatal("partial borrowed Take must retain custody with an error")
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	contender, err := os.OpenFile(parent.Name(), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := contender.Close(); err != nil {
			t.Error(err)
		}
	}()
	if err := unix.Flock(int(contender.Fd()), unix.LOCK_EX|unix.LOCK_NB); !errors.Is(err, unix.EWOULDBLOCK) && !errors.Is(err, unix.EAGAIN) {
		t.Fatalf("closing borrowed custody unlocked the live parent's OFD: %v", err)
	}
	parentOpen = false
	if err := parent.Close(); err != nil {
		t.Fatal(err)
	}
	if err := unix.Flock(int(contender.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		t.Fatal("parent's final close did not release reservation")
	}
}
