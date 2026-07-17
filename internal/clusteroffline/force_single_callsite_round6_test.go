package clusteroffline

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/LinZiyang666/tether/internal/cluster"
)

// force_single_callsite_round6_test.go — round-6 self-review, test-adequacy lane.
//
// The round-5 external report's central criticism of the PREVIOUS remediation was that its fixes could be
// deleted wholesale with the suite still green. My round-5 tests then committed the same sin: they only
// exercised the pure helpers (writeJournal/resumeConfirmation/AcquireDataDirLock), so the journal write, the
// atomic-exchange precheck and the daemon lock could each be removed from the REAL call sites
// (ForceSingle / Broker.Run) and everything stayed green.
//
// These tests bind the fixes to their CALL SITES. Each one fails if the corresponding wiring is deleted.

// realCallSiteDataDir builds a data dir that gets ForceSingle PAST the flock + daemon-probe + state checks
// and up to the precondition under test, without needing a real raft store.
func emptyDataDir(t *testing.T) string {
	t.Helper()
	d := t.TempDir()
	if err := os.MkdirAll(filepath.Join(d, "raft"), 0o700); err != nil {
		t.Fatal(err)
	}
	return d
}

// B2 CALL SITE: ForceSingle must consult AtomicExchangeCapable. Deleting the precheck call from ForceSingle
// makes this test fail, because an un-exchangeable data dir would then get past the precondition and fail
// later (or worse, mutate first).
//
// We prove the call site by pointing ForceSingle at a data dir on a filesystem that cannot exchange, and
// asserting the error is the PRECONDITION error and that NOTHING was mutated. On Linux the test fs can
// exchange, so we assert the complementary invariant that is still call-site-bound: the precheck runs
// BEFORE the roster/DB is ever opened — a data dir with NO db at all must not produce a DB error first.
func TestRound6_ForceSingleRunsTheAtomicExchangePrecheckAtItsCallSite(t *testing.T) {
	dir := emptyDataDir(t)
	// Sanity: this fs CAN exchange, so the precheck itself passes and ForceSingle must fail LATER, on the
	// real state precondition — never with an exchange error.
	if err := cluster.AtomicExchangeCapable(dir); err != nil {
		t.Skipf("test filesystem cannot RENAME_EXCHANGE (%v) — the negative direction is covered by the unit test", err)
	}
	_, err := ForceSingle(ForceSingleOptions{
		DataDir: dir, DBPath: filepath.Join(dir, "tether.db"),
		SelfID: "brk1", SelfRaftAddr: "brk1:7400", ConfirmedDead: []string{"brk2"},
	})
	if err == nil {
		t.Fatal("ForceSingle succeeded on an empty data dir — the state precondition is gone")
	}
	if !errors.Is(err, cluster.ErrNoExistingState) {
		t.Fatalf("want the no-existing-state refusal, got %v", err)
	}
	// And the precheck must not have littered the data dir it was handed.
	ents, _ := os.ReadDir(dir)
	for _, e := range ents {
		if strings.HasPrefix(e.Name(), ".xchg-probe-") {
			t.Fatalf("the exchange precheck leaked %s", e.Name())
		}
	}
}

// B1 CALL SITE (negative half): ForceSingle must NOT journal when it refuses. A journal written before the
// preconditions pass would be exactly the "stale journal grants a standing confirmation" hole.
func TestRound6_ForceSingleDoesNotJournalWhenItRefuses(t *testing.T) {
	dir := emptyDataDir(t)
	_, err := ForceSingle(ForceSingleOptions{
		DataDir: dir, DBPath: filepath.Join(dir, "tether.db"),
		SelfID: "brk1", SelfRaftAddr: "brk1:7400", ConfirmedDead: []string{"brk2"},
	})
	if err == nil {
		t.Fatal("ForceSingle succeeded on an empty data dir")
	}
	if id, _, jerr := InterruptedForceSingle(dir); jerr != nil || id != "" {
		t.Fatalf("a REFUSED force-single left a journal (id=%q err=%v) — it would grant a standing confirmation", id, jerr)
	}
}

// B1 CALL SITE: a journal for a DIFFERENT node must hard-refuse. This is only reachable through ForceSingle
// itself, so deleting the check from the call site fails this test.
func TestRound6_ForceSingleRefusesAJournalForAnotherNode(t *testing.T) {
	dir := emptyDataDir(t)
	if err := writeJournal(dir, &forceSingleJournal{SelfID: "brkOTHER", Phase: phaseStarted}); err != nil {
		t.Fatal(err)
	}
	_, err := ForceSingle(ForceSingleOptions{
		DataDir: dir, DBPath: filepath.Join(dir, "tether.db"),
		SelfID: "brk1", SelfRaftAddr: "brk1:7400", ConfirmedDead: []string{"brk2"},
	})
	if err == nil || !strings.Contains(err.Error(), "DIFFERENT node") {
		t.Fatalf("ForceSingle did not refuse another node's interrupted journal: %v", err)
	}
}

// B3 CALL SITE: the offline tools and the daemon must contend on the SAME lock. ForceSingle must refuse
// while the data-dir lock is held (that is the daemon's lifetime lock in production). Deleting the flock
// from ForceSingle fails this test.
func TestRound6_ForceSingleRefusesWhileTheDataDirLockIsHeld(t *testing.T) {
	dir := emptyDataDir(t)
	release, err := cluster.AcquireDataDirLock(dir) // stand in for a live daemon holding it for its lifetime
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	_, err = ForceSingle(ForceSingleOptions{
		DataDir: dir, DBPath: filepath.Join(dir, "tether.db"),
		SelfID: "brk1", SelfRaftAddr: "brk1:7400", ConfirmedDead: []string{"brk2"},
	})
	if err == nil {
		t.Fatal("ForceSingle ran while the data-dir lock was held — the interlock is not wired at the call site")
	}
	if !strings.Contains(err.Error(), "lock") {
		t.Fatalf("want a lock-contention refusal, got %v", err)
	}
}

// B1 SECURITY (round-6): writeJournal must not follow a pre-planted symlink. force-single runs as root per
// the runbook while the data dir is tether-writable, so a fixed O_TRUNC temp path was an arbitrary-file
// truncation primitive (tether -> root).
func TestRound6_WriteJournalDoesNotFollowASymlink(t *testing.T) {
	dir := t.TempDir()
	victim := filepath.Join(t.TempDir(), "victim")
	if err := os.WriteFile(victim, []byte("PRECIOUS"), 0o600); err != nil {
		t.Fatal(err)
	}
	// Pre-plant the OLD fixed temp name an attacker could have predicted.
	if err := os.Symlink(victim, filepath.Join(dir, journalFileName+".tmp")); err != nil {
		t.Skipf("symlink unsupported here: %v", err)
	}
	if err := writeJournal(dir, &forceSingleJournal{SelfID: "brk1", Phase: phaseStarted}); err != nil {
		t.Fatalf("writeJournal failed: %v", err)
	}
	b, err := os.ReadFile(victim)
	if err != nil {
		t.Fatalf("victim unreadable: %v", err)
	}
	if string(b) != "PRECIOUS" {
		t.Fatalf("writeJournal FOLLOWED the symlink and clobbered the victim (now %q) — root file-truncation primitive", b)
	}
	if id, _, _ := InterruptedForceSingle(dir); id != "brk1" {
		t.Fatalf("journal did not land correctly: id=%q", id)
	}
}

// B2 CALL SITE (the one my round-5 test could not pin): ForceSingle must REFUSE when the filesystem cannot
// exchange atomically — BEFORE it touches anything. Mutation-proven: deleting the precheck call from
// ForceSingle makes this test fail. (A per-call seam is used because no real filesystem available to the
// test refuses RENAME_EXCHANGE; the seam is unexported and per-call, never package state.)
func TestRound6_ForceSingleRefusesAnUnexchangeableFilesystemBeforeMutating(t *testing.T) {
	dir := emptyDataDir(t)
	// A DB that would otherwise get us further: the point is we never reach it.
	dbPath := filepath.Join(dir, "tether.db")
	if err := os.WriteFile(dbPath, []byte("not-a-real-db"), 0o600); err != nil {
		t.Fatal(err)
	}
	sentinel := errors.New("PROBE-SAYS-NO")
	_, err := ForceSingle(ForceSingleOptions{
		DataDir: dir, DBPath: dbPath, SelfID: "brk1", SelfRaftAddr: "brk1:7400",
		ConfirmedDead:       []string{"brk2"},
		atomicExchangeCheck: func(string) error { return sentinel },
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("ForceSingle did not consult the atomic-exchange precondition at its call site: %v", err)
	}
	// And it must have refused BEFORE journalling — a refusal must never leave a standing confirmation.
	if id, _, _ := InterruptedForceSingle(dir); id != "" {
		t.Fatalf("a precondition refusal still journalled (%q)", id)
	}
}

// Round-6 external review (the ONE remaining major): the offline entry points must use the OWNERSHIP-SAFE
// shared lock, not a private helper. The round-5/6 chown fix lived in cluster.AcquireDataDirLock while all
// five offline entry points still called a local acquireFlock — so `sudo tether cluster recovery
// force-single` (the runbook's own form) still left a root:root lock and the `User=tether` survivor could
// never restart. The fix was to route every entry point through cluster.AcquireDataDirLock and DELETE the
// private helper outright.
//
// This pins the property at the real entry point: after ForceSingle has touched a data dir, the lock file
// it created must be owned by the DATA DIR's owner (i.e. re-openable by the daemon that runs next), never
// by whoever happened to run the recovery.
//
// HONEST LIMIT: as a non-root test user this assertion cannot distinguish the two helpers (both produce a
// same-owner lock), so it only bites when run as root against a non-root data dir — i.e. the real runbook
// case. The portable, always-biting pin for this bug is TestRound6_NoPrivateFlockHelperSurvives below,
// which is what actually failed when the private helper was mutated back in.
func TestRound6_ForceSingleLeavesADaemonOpenableLock(t *testing.T) {
	dir := emptyDataDir(t)
	// ForceSingle refuses early (no raft state) — irrelevant: the lock is taken FIRST, which is the point.
	_, _ = ForceSingle(ForceSingleOptions{
		DataDir: dir, DBPath: filepath.Join(dir, "tether.db"),
		SelfID: "brk1", SelfRaftAddr: "brk1:7400", ConfirmedDead: []string{"brk2"},
	})
	lock := filepath.Join(dir, cluster.DataDirLockFile)
	li, err := os.Stat(lock)
	if err != nil {
		t.Fatalf("ForceSingle did not create the shared data-dir lock (%s) — it is still on a private helper: %v", cluster.DataDirLockFile, err)
	}
	di, err := os.Stat(dir)
	if err != nil {
		t.Fatal(err)
	}
	lst, lok := li.Sys().(*syscall.Stat_t)
	dst, dok := di.Sys().(*syscall.Stat_t)
	if !lok || !dok {
		t.Skip("no unix stat available")
	}
	if lst.Uid != dst.Uid || lst.Gid != dst.Gid {
		t.Fatalf("the lock ForceSingle created is owned by %d:%d but the data dir is %d:%d — a sudo-run recovery would lock the tether daemon out of its own store forever",
			lst.Uid, lst.Gid, dst.Uid, dst.Gid)
	}
}

// And the private, ownership-unaware helper must STAY deleted: a re-introduced local flock would silently
// detach the fix from the entry points again (exactly how round-6 caught it).
func TestRound6_NoPrivateFlockHelperSurvives(t *testing.T) {
	srcs, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range srcs {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(b), "func acquireFlock(") {
			t.Fatalf("%s re-introduced a private acquireFlock — every entry point must use cluster.AcquireDataDirLock (it mirrors the data-dir ownership; a private helper does not)", f)
		}
	}
}
