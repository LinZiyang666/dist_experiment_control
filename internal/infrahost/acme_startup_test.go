//go:build linux

package infrahost

// Real private filesystem trees, X.509 renewals, supervised writer processes,
// pidfds and flock OFDs run here. Policy/domain authorization is P04's boundary.
// Stock Caddy, real ACME protocol and systemd restart are P29/P31/86.I evidence.

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/infracontract"
	"golang.org/x/sys/unix"
)

// origin: hot-update S9 plan §5.3 / T21 / M11
func TestACMEStartupFenceOutlivesRestartDelay(t *testing.T) {
	t.Run("copy_outlives_two_second_restart", checkACMESlowCopy)
	t.Run("failures_keep_fence", checkACMEFailures)
	t.Run("checkpoint_recovery", checkACMECheckpoint)
	t.Run("checkpoint_reuses_verified_inode", checkACMEReuse)
	t.Run("durability_and_owner", checkACMEDurability)
	t.Run("corrupt_selector_fails_closed", checkACMESelector)
	t.Run("startup_budget_and_cancellation", checkACMEStartupBudget)
	t.Run("ownership_failure", checkACMEOwnerFailure)
}

func checkACMESlowCopy(t *testing.T) {
	f := newACMEFixture(t)
	f.writer.renew(t)
	f.writer.close()
	replacement, err := NewACMEStorage(f.config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = replacement.Close() }()
	entered, proceed := make(chan struct{}), make(chan struct{})
	var once sync.Once
	f.storage.copyStep = func() { once.Do(func() { close(entered); <-proceed }) }
	result := make(chan error, 1)
	started := time.Now()
	deadline := acmeDeadline(t, 8*time.Second)
	go func() { _, err := f.storage.Do(f.request, deadline, nil); result <- err }()
	var resume sync.Once
	unblock := func() { resume.Do(func() { close(proceed) }) }
	joined := false
	defer func() {
		unblock()
		if !joined {
			<-result
		}
	}()
	<-entered
	// sleep-fixture: the stock RestartSec=2 failure window is the subject. This
	// timer deliberately keeps the real final-copy action in flight past it.
	timer := time.NewTimer(2200 * time.Millisecond)
	defer timer.Stop()
	if _, err := ClaimACMEWriter(replacement, f.request.Tx, f.request.Domain, f.fenceRevision+2, acmeDeadline(t, 150*time.Millisecond), nil); !errors.Is(err, errACMEBudget) {
		t.Fatal("automatic restart bypassed an in-progress durable fence")
	}
	<-timer.C
	if _, err := ClaimACMEWriter(replacement, f.request.Tx, f.request.Domain, f.fenceRevision+2, acmeDeadline(t, 150*time.Millisecond), nil); !errors.Is(err, errACMEBudget) {
		t.Fatal("startup fence expired with RestartSec")
	}
	unblock()
	err = <-result
	joined = true
	if err != nil {
		t.Fatal(err)
	}
	if time.Since(started) <= 2*time.Second {
		t.Fatal("final-copy restart race was empty")
	}
	state := acmeStateFixture(t, replacement)
	if _, err := ClaimACMEWriter(replacement, f.request.Tx, f.request.Domain, state.Revision, acmeDeadline(t, 50*time.Millisecond), nil); !errors.Is(err, errACMEBudget) {
		t.Fatal("final selector committed without ReleaseStart")
	}
	if _, err := ReleaseACMEStart(replacement, f.request.Tx, f.request.Domain, f.fenceRevision, acmeDeadline(t, time.Second), nil); err == nil {
		t.Fatal("stale finalize revision released startup")
	}
	revision, err := ReleaseACMEStart(replacement, f.request.Tx, f.request.Domain, state.Revision, acmeDeadline(t, time.Second), nil)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := ClaimACMEWriter(replacement, f.request.Tx, f.request.Domain, revision, acmeDeadline(t, time.Second), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = writer.Lease.Close() }()
	defer func() { _ = writer.Directory.Close() }()
	assertACMERevision(t, writer.Directory, "B", false)
}

func checkACMEFailures(t *testing.T) {
	for _, stage := range []string{"pending_intent", "file_sync", "directory_sync", "selector_sync"} {
		t.Run(stage, func(t *testing.T) {
			f := newACMEFixture(t)
			f.writer.close()
			calls := 0
			failed := false
			f.storage.syncFile = func(file *os.File) error {
				calls++
				stat, err := file.Stat()
				if err != nil {
					return err
				}
				var finalSelector bool
				if stage == "selector_sync" && !stat.IsDir() {
					body, err := os.ReadFile(fmt.Sprintf("/proc/self/fd/%d", file.Fd()))
					finalSelector = err == nil && bytes.Contains(body, []byte(`"Purpose":"tether-acme-storage-v1"`)) && bytes.Contains(body, []byte(`"Finalized":true`))
				}
				if (stage == "pending_intent" && calls == 1) || (stage == "file_sync" && calls == 3) || (stage == "directory_sync" && calls > 3 && stat.IsDir()) || finalSelector {
					failed = true
					return errors.New("cannot sync " + acmeCanary)
				}
				return file.Sync()
			}
			if receipt, err := f.storage.Do(f.request, acmeDeadline(t, 3*time.Second), nil); err == nil || receipt.Applied || !failed {
				t.Fatalf("did not reject injected %s failure", stage)
			} else {
				assertACMENoSecret(t, err)
			}
			state := acmeStateFixture(t, f.storage)
			if !state.Fenced || state.Current != "" {
				t.Fatal("failed final copy replaced the current selector")
			}
			if _, err := ReleaseACMEStart(f.storage, f.request.Tx, f.request.Domain, state.Revision, acmeDeadline(t, time.Second), nil); err == nil {
				t.Fatal("copy failure released startup")
			}
		})
	}
}

func checkACMEDurability(t *testing.T) {
	f := newACMEFixture(t)
	f.writer.close()
	type identity struct{ dev, ino uint64 }
	synced := make(map[identity]bool)
	finalSelectorSynced := false
	var rootStat unix.Stat_t
	if err := unix.Fstat(int(f.storage.root.Fd()), &rootStat); err != nil {
		t.Fatal(err)
	}
	syncOrdinal, finalSelectorOrdinal, rootOrdinal := 0, 0, 0
	f.storage.syncFile = func(file *os.File) error {
		syncOrdinal++
		var stat unix.Stat_t
		if err := unix.Fstat(int(file.Fd()), &stat); err != nil {
			return err
		}
		synced[identity{stat.Dev, stat.Ino}] = true
		if stat.Dev == rootStat.Dev && stat.Ino == rootStat.Ino {
			rootOrdinal = syncOrdinal
		}
		if stat.Mode&unix.S_IFMT == unix.S_IFREG {
			body, err := os.ReadFile(fmt.Sprintf("/proc/self/fd/%d", file.Fd()))
			if err == nil && bytes.Contains(body, []byte(`"Purpose":"tether-acme-storage-v1"`)) && bytes.Contains(body, []byte(`"Finalized":true`)) {
				finalSelectorSynced = true
				finalSelectorOrdinal = syncOrdinal
			}
		}
		return file.Sync()
	}
	if _, err := ReleaseACMEStart(f.storage, f.request.Tx, f.request.Domain, f.fenceRevision, acmeDeadline(t, time.Second), nil); err == nil {
		t.Fatal("startup release accepted an unfinalized tree")
	}
	if _, err := f.storage.Do(f.request, acmeDeadline(t, 5*time.Second), nil); err != nil {
		t.Fatal(err)
	}
	state := acmeStateFixture(t, f.storage)
	if !finalSelectorSynced {
		t.Fatal("final selector was not fsynced")
	}
	if rootOrdinal <= finalSelectorOrdinal {
		t.Fatal("selector rename was not followed by a directory fsync")
	}
	if err := filepath.WalkDir(filepath.Join(f.root, state.Current), func(path string, _ os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		var stat unix.Stat_t
		if err := unix.Lstat(path, &stat); err != nil {
			return err
		}
		if !synced[identity{stat.Dev, stat.Ino}] {
			t.Fatal("selected a tree containing an unsynced inode")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	changed := f.request
	changed.Payload = infracontract.Digest{7}
	if _, err := f.storage.Do(changed, acmeDeadline(t, time.Second), nil); err == nil {
		t.Fatal("a different finalization payload reused its receipt")
	}
	for _, field := range []string{"tx", "domain", "step", "expected"} {
		changed := f.request
		switch field {
		case "tx":
			changed.Tx = infracontract.ID{12}
		case "domain":
			changed.Domain = infracontract.ID{12}
		case "step":
			changed.Step++
		case "expected":
			changed.Expected.StartTicks++
		}
		if _, err := f.storage.Do(changed, acmeDeadline(t, time.Second), nil); !errors.Is(err, errACMENotReady) {
			t.Fatal("another transaction/incarnation reused the storage fence")
		}
	}
}

func checkACMECheckpoint(t *testing.T) {
	f := newACMEFixture(t)
	f.writer.renew(t)
	f.writer.close()
	stop := make(chan struct{})
	steps := 0
	f.storage.copyStep = func() {
		steps++
		if steps == 2 {
			close(stop)
		}
	}
	if _, err := f.storage.Do(f.request, acmeDeadline(t, time.Second), stop); !errors.Is(err, errACMEBudget) {
		t.Fatal("copy ignored cancellation between checkpoints")
	}
	state := acmeStateFixture(t, f.storage)
	if state.Pending == "" || state.Current != "" || !state.Fenced {
		t.Fatal("partial tree became current or lost its checkpoint")
	}
	// An interrupted tree may contain an obsolete file from an earlier copy
	// attempt. Final delta must remove it, as well as refreshing current bytes.
	acmeWrite(t, filepath.Join(f.root, state.Pending, "obsolete"), []byte("obsolete-checkpoint"))
	// Recovery must re-read bytes even for an already checkpointed, same-size
	// account record; a completed warm/pending file is not a current-state oracle.
	writeACMERevision(t, f.initial, "C")
	f.request.Sequence++
	f.request.Nonce = infracontract.ID{13}
	replacement, err := NewACMEStorage(f.config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = replacement.Close() }()
	if _, err := replacement.Do(f.request, acmeDeadline(t, 3*time.Second), nil); err != nil {
		t.Fatal(err)
	}
	state = acmeStateFixture(t, replacement)
	selected, err := replacement.current(state)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = selected.Close() }()
	assertACMERevision(t, selected, "C", false)
}

func checkACMEReuse(t *testing.T) {
	f := newACMEFixture(t)
	f.writer.renew(t)
	f.writer.close()
	stop := make(chan struct{})
	steps := 0
	f.storage.copyStep = func() {
		steps++
		if steps == 2 {
			close(stop)
		}
	}
	if _, err := f.storage.Do(f.request, acmeDeadline(t, time.Second), stop); !errors.Is(err, errACMEBudget) {
		t.Fatal("checkpoint cancellation was not exercised")
	}
	state := acmeStateFixture(t, f.storage)
	var before unix.Stat_t
	if err := unix.Fstatat(int(f.storage.root.Fd()), state.Pending+"/account.key", &before, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		t.Fatal("no complete file existed at the bounded checkpoint")
	}
	f.storage.copyStep = nil
	f.request.Sequence++
	f.request.Nonce = infracontract.ID{13}
	if _, err := f.storage.Do(f.request, acmeDeadline(t, 3*time.Second), nil); err != nil {
		t.Fatal(err)
	}
	state = acmeStateFixture(t, f.storage)
	var after unix.Stat_t
	if err := unix.Fstatat(int(f.storage.root.Fd()), state.Current+"/account.key", &after, unix.AT_SYMLINK_NOFOLLOW); err != nil || after.Dev != before.Dev || after.Ino != before.Ino {
		t.Fatal("next bounded action recopied an already verified checkpoint")
	}
}

func checkACMESelector(t *testing.T) {
	for _, corrupt := range []string{"duplicate", "unknown", "tail", "version", "purpose", "slot", "uid", "gid", "pending_slot", "garbage_slot"} {
		t.Run(corrupt, func(t *testing.T) {
			f := newACMEFixture(t)
			body, err := os.ReadFile(filepath.Join(f.root, "selector.json"))
			if err != nil {
				t.Fatal(err)
			}
			switch corrupt {
			case "duplicate":
				body = append([]byte(`{"Fenced":false,`), body[1:]...)
			case "unknown":
				body = append([]byte(`{"Other":false,`), body[1:]...)
			case "tail":
				body = append(body, ' ')
			case "version":
				body = bytes.Replace(body, []byte(`"Version":1`), []byte(`"Version":2`), 1)
			case "purpose":
				body = bytes.Replace(body, []byte("tether-acme-storage-v1"), []byte("unrelated-record-v1"), 1)
			case "slot":
				body = bytes.Replace(body, []byte(`"Current":""`), []byte(`"Current":"../outside"`), 1)
			case "uid":
				body = bytes.Replace(body, []byte(fmt.Sprintf(`"UID":%d`, os.Geteuid())), []byte(fmt.Sprintf(`"UID":%d`, os.Geteuid()+1)), 1)
			case "gid":
				body = bytes.Replace(body, []byte(fmt.Sprintf(`"GID":%d`, os.Getegid())), []byte(fmt.Sprintf(`"GID":%d`, os.Getegid()+1)), 1)
			case "pending_slot":
				body = bytes.Replace(body, []byte(`"Pending":""`), []byte(`"Pending":"../outside"`), 1)
			case "garbage_slot":
				body = bytes.Replace(body, []byte(`"Garbage":""`), []byte(`"Garbage":"../outside"`), 1)
			}
			acmeWrite(t, filepath.Join(f.root, "selector.json"), body)
			if _, err := ClaimACMEWriter(f.storage, f.request.Tx, f.request.Domain, f.fenceRevision, acmeDeadline(t, time.Second), nil); !errors.Is(err, infracontract.ErrIncomplete) {
				t.Fatal("corrupt selector became a startup permit")
			}
		})
	}
}

func checkACMEStartupBudget(t *testing.T) {
	f := newACMEFixture(t)
	stop := make(chan struct{})
	close(stop)
	canceledAt := time.Now()
	if _, err := ClaimACMEWriter(f.storage, f.request.Tx, f.request.Domain, f.fenceRevision, acmeDeadline(t, 3*time.Second), stop); !errors.Is(err, errACMEBudget) {
		t.Fatal("canceled launcher waited or received a permit")
	}
	if time.Since(canceledAt) >= time.Second {
		t.Fatal("canceled launcher consumed its remaining boot budget")
	}
	if _, err := f.storage.Do(f.request, 0, nil); !errors.Is(err, errACMEBudget) {
		t.Fatal("zero budget changed storage")
	}
	if state := acmeStateFixture(t, f.storage); !state.Fenced || state.Finalized {
		t.Fatal("budget exhaustion opened startup")
	}
	foreign := f.request
	foreign.Tx = infracontract.ID{20}
	foreign.Op = infracontract.FenceStart
	if _, err := FenceACME(f.storage, foreign, acmeDeadline(t, time.Second), nil); !errors.Is(err, errACMEBusy) {
		t.Fatal("another transaction replaced the durable fence")
	}
}

func checkACMEOwnerFailure(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("unprivileged ownership failure requires a non-root test process")
	}
	f := newACMEFixture(t)
	f.writer.close()
	// A rejected chown must never be ignored. No privileged action succeeds here:
	// the private target UID is deliberately outside this process's credentials.
	f.storage.config.UID = os.Geteuid() + 1
	if err := f.storage.atomicFile(f.storage.root, "ownership-probe", nil, f.storage.config.UID, os.Getegid(), 0600); err == nil {
		t.Fatal("file chown failure was ignored")
	}
	emptyPath := t.TempDir()
	if err := os.Chmod(emptyPath, 0700); err != nil {
		t.Fatal(err)
	}
	empty, err := os.Open(emptyPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = empty.Close() }()
	slot, err := acmeRandomSlot("tree-")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.storage.copyTree(empty, slot, false, acmeDeadline(t, time.Second), nil); err == nil {
		t.Fatal("directory chown failure was ignored")
	}
	state := acmeStateFixtureBeforeOwnerChange(t, f.storage)
	state.UID = f.storage.config.UID
	if err := f.storage.writeState(state); err != nil {
		t.Fatal(err)
	}
	if result, err := f.storage.Do(f.request, acmeDeadline(t, time.Second), nil); err == nil || result.Applied {
		t.Fatal("ownership failure finalized storage")
	}
	state = acmeStateFixture(t, f.storage)
	if !state.Fenced || state.Current != "" {
		t.Fatal("ownership failure selected a partially migrated tree")
	}
}
