// Real flock/OFD and journal-guard tests, plus a supervised test-binary exec.
// No lock is mocked. The CLI, adjunct importer and full runtime commit remain
// P23 integration coverage; this leaf commits a real file through WithGuard.
package upgradelock

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"golang.org/x/sys/unix"
)

func assertContender(t *testing.T, key upgradetx.Key, held bool) {
	t.Helper()
	d, err := resolveDomain(key)
	if err != nil {
		t.Fatal(err)
	}
	f, err := openLock(d, ".lease")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	err = flock(f, unix.LOCK_EX|unix.LOCK_NB)
	if held && !errors.Is(err, syscall.EWOULDBLOCK) && !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("contender obtained a continuously held reservation: %v", err)
	}
	if !held && err != nil {
		t.Fatalf("final release retained reservation: %v", err)
	}
}

// origin: hot-update S8 plan §5.1 / P04 / M08
func TestBorrowedLeaseClosePreservesParentReservation(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseBorrowedLease(t)
	}
	testharness.AssertNoGoroutineLeak(t, "borrowed domain reservation", g)
	testharness.AssertNoFDLeak(t, "borrowed domain reservation", fd, 0)
}

func exerciseBorrowedLease(t *testing.T) {
	t.Helper()
	reg, key := domainFixture(t)
	sibling := key
	sibling.ConfiguredNID = "sibling"
	sibling.UID++ // Multi-UID identities are separate participants in one domain.
	reg.Participants = append(reg.Participants, Participant{Key: sibling, ServiceID: "sibling-unit"})
	key = registerFixture(t, reg, key)
	sibling.DomainID = key.DomainID
	parent, err := Acquire(t.Context(), key, "parent-token")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = parent.Close() }()
	for _, token := range []string{"", "competing-parent"} {
		_, err := Acquire(t.Context(), sibling, token)
		requireCode(t, err, upgradetx.CodeDeferred)
	}
	child, err := Acquire(t.Context(), sibling, "parent-token")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = child.Close() }()
	if child.FD() == parent.FD() || child.DomainID() != parent.DomainID() {
		t.Fatal("child did not receive separate descriptor custody in the same domain")
	}
	assertContender(t, key, true)
	if err := child.Close(); err != nil {
		t.Fatal(err)
	}
	assertContender(t, key, true)
	child, err = Acquire(t.Context(), sibling, "parent-token")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = child.Close() }()
	if err := parent.Close(); err != nil {
		t.Fatal(err)
	}
	assertContender(t, key, true)
	if err := child.Close(); err != nil {
		t.Fatal(err)
	}
	assertContender(t, key, false)
	if parent.FD() != ^uintptr(0) || parent.Close() != nil || child.Close() != nil {
		t.Fatal("closed custody was reused")
	}
}

// origin: hot-update S8 plan §5.1 / P04
func TestJournalGuardUsesSeparateInodeAndSerializes(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseJournalGuard(t)
	}
	testharness.AssertNoGoroutineLeak(t, "short journal guard", g)
	testharness.AssertNoFDLeak(t, "short journal guard", fd, 0)
}

func exerciseJournalGuard(t *testing.T) {
	t.Helper()
	reg, key := domainFixture(t)
	key = registerFixture(t, reg, key)
	parent, err := Acquire(t.Context(), key, "")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = parent.Close() }()
	// A same-inode guard defect would block forever. Let final cleanup close
	// the lease before joining the callback, so mutation runs terminate safely.
	result := make(chan error, 1)
	go func() { result <- WithGuard(key, func() error { return nil }) }()
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		_ = parent.Close()
		<-result
		t.Fatal("short guard self-locked behind the long domain lease")
	}
	long, err := os.Stat(filepath.Join(reg.ControlDir, key.DomainID+".lease"))
	if err != nil {
		t.Fatal(err)
	}
	short, err := os.Stat(filepath.Join(reg.ControlDir, key.DomainID+".guard"))
	if err != nil || os.SameFile(long, short) {
		t.Fatalf("long lease and short guard share an inode: %v", err)
	}
	var wg sync.WaitGroup
	errorsOut := make(chan error, 8)
	journal := filepath.Join(reg.ControlDir, "guarded-counter")
	if err := os.WriteFile(journal, []byte("0"), 0o600); err != nil {
		t.Fatal(err)
	}
	for range 8 {
		wg.Go(func() {
			errorsOut <- WithGuard(key, func() error {
				data, err := os.ReadFile(journal)
				if err != nil {
					return err
				}
				previous, err := strconv.Atoi(string(data))
				if err != nil {
					return err
				}
				runtime.Gosched()
				return os.WriteFile(journal, []byte(strconv.Itoa(previous+1)), 0o600)
			})
		})
	}
	wg.Wait()
	close(errorsOut)
	for err := range errorsOut {
		if err != nil {
			t.Fatal(err)
		}
	}
	value, err := os.ReadFile(journal)
	if err != nil || string(value) != "8" {
		t.Fatalf("guard did not serialize journal updates: %q, %v", value, err)
	}
	sentinel := errors.New("local RMW failed")
	if err := WithGuard(key, func() error { return sentinel }); err != sentinel {
		t.Fatalf("guard lost callback result: %v", err)
	}
	func() {
		defer func() {
			if recover() != sentinel {
				t.Error("guard changed callback panic")
			}
		}()
		_ = WithGuard(key, func() error { panic(sentinel) })
	}()
	if err := WithGuard(key, func() error { return nil }); err != nil {
		t.Fatalf("callback failure retained guard: %v", err)
	}
	assertContender(t, key, true)
}

// origin: hot-update S8 plan §5.1 / P04 / M08
func TestAdoptRequiresOriginalOFDAndPreservesRefusedCustody(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("kernel OFD adoption proof requires Linux")
	}
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseAdoptLease(t)
	}
	testharness.AssertNoGoroutineLeak(t, "original domain OFD adoption", g)
	testharness.AssertNoFDLeak(t, "original domain OFD adoption", fd, 0)
}

func exerciseAdoptLease(t *testing.T) {
	t.Helper()
	reg, key := domainFixture(t)
	key = registerFixture(t, reg, key)
	parent, err := Acquire(t.Context(), key, "parent-token")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = parent.Close() }()
	d, err := resolveDomain(key)
	if err != nil {
		t.Fatal(err)
	}
	fresh, err := openLock(d, ".lease")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = fresh.Close() }()
	_, err = Adopt(fresh.Fd(), key, "parent-token")
	requireCode(t, err, upgradetx.CodeRejected)
	if _, err := fresh.Stat(); err != nil {
		t.Fatalf("refusal consumed caller descriptor: %v", err)
	}
	_, err = Adopt(parent.FD(), key, "parent-token")
	requireCode(t, err, upgradetx.CodeRejected)
	dup, err := unix.FcntlInt(parent.FD(), unix.F_DUPFD_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	owned := false
	defer func() {
		if !owned {
			_ = unix.Close(dup)
		}
	}()
	_, err = Adopt(uintptr(dup), key, "wrong-parent")
	requireCode(t, err, upgradetx.CodeRejected)
	unknown := key
	unknown.ConfiguredNID = "unknown"
	_, err = Adopt(uintptr(dup), unknown, "parent-token")
	requireCode(t, err, upgradetx.CodeRejected)
	if _, err := unix.FcntlInt(uintptr(dup), unix.F_SETFD, 0); err != nil {
		t.Fatal(err)
	}
	_, err = Adopt(uintptr(dup), key, "parent-token")
	requireCode(t, err, upgradetx.CodeRejected)
	if _, err := unix.FcntlInt(uintptr(dup), unix.F_SETFD, unix.FD_CLOEXEC); err != nil {
		t.Fatal(err)
	}
	adopted, err := Adopt(uintptr(dup), key, "parent-token")
	if err != nil {
		t.Fatal(err)
	}
	owned = true
	defer func() { _ = adopted.Close() }()
	if adopted.FD() != uintptr(dup) {
		t.Fatal("adoption reopened the lease instead of consuming the inherited OFD")
	}
	if err := parent.Close(); err != nil {
		t.Fatal(err)
	}
	assertContender(t, key, true)
	if err := WithGuard(key, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := adopted.Close(); err != nil {
		t.Fatal(err)
	}
	_, err = Adopt(fresh.Fd(), key, "parent-token")
	requireCode(t, err, upgradetx.CodeRejected)
	assertContender(t, key, false)
}

// origin: hot-update S8 plan §5.1 / P04
func TestLeaseRejectsAliasedLockInodes(t *testing.T) {
	for _, kind := range []string{"hardlink", "symlink", "directory"} {
		t.Run(kind, func(t *testing.T) {
			reg, key := domainFixture(t)
			key = registerFixture(t, reg, key)
			parent, err := Acquire(t.Context(), key, "")
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = parent.Close() }()
			long := filepath.Join(reg.ControlDir, key.DomainID+".lease")
			short := filepath.Join(reg.ControlDir, key.DomainID+".guard")
			switch kind {
			case "hardlink":
				err = os.Link(long, short)
			case "symlink":
				err = os.Symlink(long, short)
			case "directory":
				err = os.Mkdir(short, 0o700)
			default:
				t.Fatal("unknown test case")
			}
			if err != nil {
				t.Fatal(err)
			}
			called := false
			result := make(chan error, 1)
			go func() { result <- WithGuard(key, func() error { called = true; return nil }) }()
			select {
			case err = <-result:
			case <-time.After(3 * time.Second):
				_ = parent.Close()
				<-result
				t.Fatal("guard blocked behind an aliased reservation inode")
			}
			if err == nil || called {
				t.Fatalf("guard accepted aliased or invalid lock: %v", err)
			}
		})
	}
}

// origin: hot-update S8 plan §5.1 / P04
func TestAdoptRejectsChangedDeclarationAndForeignInode(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("kernel OFD adoption proof requires Linux")
	}
	reg, key := domainFixture(t)
	key = registerFixture(t, reg, key)
	parent, err := Acquire(t.Context(), key, "parent-token")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = parent.Close() }()
	dup, err := unix.FcntlInt(parent.FD(), unix.F_DUPFD_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = unix.Close(dup) }()
	path := filepath.Join(reg.ControlDir, key.DomainID+".lease")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, malformed := range [][]byte{
		[]byte("{}"),
		append(append([]byte(nil), data[:len(data)-1]...), []byte(",\"unknown\":true}")...),
		append(append([]byte(nil), data[:len(data)-1]...), []byte(",\"version\":1}")...),
	} {
		if err := os.WriteFile(path, malformed, 0o600); err != nil {
			t.Fatal(err)
		}
		_, err := Adopt(uintptr(dup), key, "parent-token")
		requireCode(t, err, upgradetx.CodeRejected)
		if _, err := unix.FcntlInt(uintptr(dup), unix.F_GETFD, 0); err != nil {
			t.Fatal("malformed declaration consumed salvage custody")
		}
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	foreign, err := os.CreateTemp(reg.ControlDir, "foreign-lock-")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = foreign.Close() }()
	if _, err := foreign.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := flock(foreign, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	_, err = Adopt(foreign.Fd(), key, "parent-token")
	requireCode(t, err, upgradetx.CodeRejected)
	assertContender(t, key, true)
}

// Real kernel contention, rather than the in-process registration map, proves
// that publication is serialized against an external process as well.
// origin: hot-update S8 plan §5.1 / P04
func TestRegistryGuardSerializesDeclarationPublication(t *testing.T) {
	reg, _ := domainFixture(t)
	d, err := normalizeDomain(reg)
	if err != nil {
		t.Fatal(err)
	}
	guard, err := lockRegistry(d)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = guard.Close() }()
	contender, err := os.OpenFile(filepath.Join(reg.ControlDir, "domain-registry.guard"), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = contender.Close() }()
	if err := flock(contender, unix.LOCK_EX|unix.LOCK_NB); !errors.Is(err, syscall.EWOULDBLOCK) && !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("registry publication guard admitted a contender: %v", err)
	}
	if err := guard.Close(); err != nil {
		t.Fatal(err)
	}
	if err := flock(contender, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		t.Fatalf("registry publication retained guard: %v", err)
	}
}

// origin: hot-update S8 plan §2.2 / §5.1 / P04
func TestLockAPIsRejectMissingInputs(t *testing.T) {
	reg, key := domainFixture(t)
	key = registerFixture(t, reg, key)
	_, err := Acquire(nil, key, "") //nolint:staticcheck // Nil is the adversarial input for the missing-context rejection guard.
	requireCode(t, err, upgradetx.CodeRejected)
	_, err = Acquire(t.Context(), key, "invalid\nparent")
	requireCode(t, err, upgradetx.CodeRejected)
	requireCode(t, WithGuard(key, nil), upgradetx.CodeRejected)
	_, err = Adopt(^uintptr(0), key, "")
	requireCode(t, err, upgradetx.CodeRejected)
}

// This is a leaf-level same-PID syscall.Exec test. P23 additionally exercises
// real tether/aux Take/Adopt/runtime commit; this receipt does not replace it.
// Every spawned process uses the shared external FIX-ORPHAN supervisor.
// origin: hot-update S8 plan §9.1 / §5.1 / P04
func TestExecPreservesDomainLeaseThroughCommit(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("same-PID reservation adoption requires Linux")
	}
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseLeaseExec(t)
	}
	testharness.AssertNoGoroutineLeak(t, "domain reservation through exec", g)
	testharness.AssertNoFDLeak(t, "domain reservation through exec", fd, 0)
}

func exerciseLeaseExec(t *testing.T) {
	t.Helper()
	reg, _ := domainFixture(t)
	config := filepath.Join(reg.ControlDir, "deployment.json")
	encoded, err := json.Marshal(reg)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(config, encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := contextWithExecDeadline(t)
	defer cancel()
	cmd := exec.CommandContext(ctx, self, "-test.run=^TestDomainLeaseExecHelper$")
	cmd.Env = append(os.Environ(), "S8_LOCK_TEST_ROLE=dispatch", "S8_LOCK_TEST_CONFIG="+config)
	out, err := testharness.CombinedOutput(cmd)
	if err != nil {
		t.Fatalf("supervised same-PID lease exec: %v\n%s", err, out)
	}
	proof, err := os.ReadFile(config + ".commit")
	var receipt leaseExecReceipt
	if err != nil || json.Unmarshal(proof, &receipt) != nil || receipt.PID != cmd.Process.Pid || !receipt.Committed {
		t.Fatalf("missing same-PID commit receipt: %q, %v", proof, err)
	}
	t.Logf("S8.P04 exec receipt: %s", proof)
}

type leaseExecReceipt struct {
	PID            int
	DomainID       string
	Device, Inode  uint64
	ExecutablePath string
	ExecutableSHA  string
	Committed      bool
}

func leaseExecProof(t *testing.T, fd uintptr, key upgradetx.Key) leaseExecReceipt {
	t.Helper()
	var stat unix.Stat_t
	if err := unix.Fstat(int(fd), &stat); err != nil {
		t.Fatal(err)
	}
	path, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(data)
	return leaseExecReceipt{PID: os.Getpid(), DomainID: key.DomainID, Device: uint64(stat.Dev), Inode: stat.Ino, ExecutablePath: path, ExecutableSHA: hex.EncodeToString(digest[:])}
}

// A separate process-local root is not needed: each helper retains t.Context.
func contextWithExecDeadline(t *testing.T) (context.Context, context.CancelFunc) {
	return context.WithTimeout(t.Context(), 20*time.Second)
}

// origin: hot-update S8 plan §9.1 / P04
func TestDomainLeaseExecHelper(t *testing.T) {
	role := os.Getenv("S8_LOCK_TEST_ROLE")
	if role == "" {
		t.Skip("supervised subprocess helper")
	}
	config := os.Getenv("S8_LOCK_TEST_CONFIG")
	data, err := os.ReadFile(config)
	if err != nil {
		t.Fatal(err)
	}
	var reg Registration
	if err := json.Unmarshal(data, &reg); err != nil {
		t.Fatal(err)
	}
	key := registerFixture(t, reg, reg.Participants[0].Key)
	if role == "dispatch" {
		owner, err := Acquire(t.Context(), key, "exec-parent")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = owner.Close() }()
		proof, err := json.Marshal(leaseExecProof(t, owner.FD(), key))
		if err != nil || os.WriteFile(config+".dispatch", proof, 0o600) != nil {
			t.Fatal("dispatch identity receipt unavailable")
		}
		if _, err := unix.FcntlInt(owner.FD(), unix.F_SETFD, 0); err != nil {
			t.Fatal(err)
		}
		self, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		env := append(os.Environ(), "S8_LOCK_TEST_ROLE=receive", "S8_LOCK_TEST_FD="+strconv.FormatUint(uint64(owner.FD()), 10))
		// Explicitly replace the existing mode; duplicate environment entries
		// would let the next test process dispatch recursively.
		for i := 0; i < len(env)-2; i++ {
			if env[i] == "S8_LOCK_TEST_ROLE=dispatch" {
				env[i] = "S8_LOCK_TEST_ROLE=receive"
			}
		}
		if err := syscall.Exec(self, []string{self, "-test.run=^TestDomainLeaseExecHelper$"}, env); err != nil {
			t.Fatal(err)
		}
		t.Fatal("exec unexpectedly returned")
	}
	if role != "receive" {
		t.Fatal("unknown exec test role")
	}
	fd, err := strconv.ParseUint(os.Getenv("S8_LOCK_TEST_FD"), 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := unix.FcntlInt(uintptr(fd), unix.F_SETFD, unix.FD_CLOEXEC); err != nil {
		t.Fatal(err)
	}
	owner, err := Adopt(uintptr(fd), key, "exec-parent")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = owner.Close() }()
	proof := leaseExecProof(t, owner.FD(), key)
	data, err = os.ReadFile(config + ".dispatch")
	var dispatched leaseExecReceipt
	if err != nil || json.Unmarshal(data, &dispatched) != nil || dispatched != proof {
		t.Fatalf("exec changed PID, binary or reservation identity: %v", err)
	}
	assertContender(t, key, true)
	err = WithGuard(key, func() error {
		proof.Committed = true
		encoded, err := json.Marshal(proof)
		if err != nil {
			return err
		}
		file, err := os.OpenFile(config+".commit", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
		if err != nil {
			return err
		}
		defer func() { _ = file.Close() }()
		if _, err := file.Write(encoded); err != nil {
			return err
		}
		return file.Sync()
	})
	if err != nil {
		t.Fatal(err)
	}
	assertContender(t, key, true)
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	assertContender(t, key, false)
}
