package cluster

import (
	"bytes"
	"database/sql"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/storage"
)

// TestRestorePreservesLocalSelfNodeID pins the v0.4.4 grow fix: a raft InstallSnapshot copies the LEADER's
// DB byte-for-byte, and cluster_meta.self_node_id in it is the LEADER's id. A joiner that catches up by
// installing the leader's snapshot MUST keep ITS OWN id — otherwise the next restart's readSelfNodeID
// returns the leader's id and the joiner comes up as the leader (two nodes claiming one raft ServerID =
// split brain). This path had ZERO coverage: no prior test exercised a real InstallSnapshot (the d9
// 2-broker join aligned via low-index log replay, never installing the leader's snapshot).
func TestRestorePreservesLocalSelfNodeID(t *testing.T) {
	// SOURCE = a "leader" whose snapshot carries self_node_id='pc732-leader' + a replicated data row.
	srcDir := t.TempDir()
	a := mustNode(t, srcDir, "pc732-leader")
	// self_node_id is not a "t:"-prefixed test key, so seed it directly into the FSM write pool (it lives
	// in cluster_meta, which the online-backup snapshot captures — exactly as `cluster init` writes it).
	if _, err := a.db.Exec(`INSERT INTO cluster_meta(key,value) VALUES('self_node_id','pc732-leader') ON CONFLICT(key) DO UPDATE SET value=excluded.value`); err != nil {
		t.Fatalf("seed leader self_node_id: %v", err)
	}
	if err := a.ApplyMetaSet("t:grow", "leader-data"); err != nil {
		t.Fatalf("seed leader data: %v", err)
	}
	if err := a.Snapshot(); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	_, rc := openSnapshot(t, srcDir)
	snapBytes, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil || len(snapBytes) == 0 {
		t.Fatalf("read snapshot: %v (len=%d)", err, len(snapBytes))
	}

	// TARGET = a "joiner" with its OWN identity. It installs the leader's snapshot.
	tf, tdb := freshFSM(t, t.TempDir())
	if _, err := tdb.Exec(`INSERT INTO cluster_meta(key,value) VALUES('self_node_id','racknerd-joiner')`); err != nil {
		t.Fatalf("seed joiner self_node_id: %v", err)
	}
	if err := tf.Restore(io.NopCloser(bytes.NewReader(snapBytes))); err != nil {
		t.Fatalf("restore: %v", err)
	}

	// (1) IDENTITY PRESERVED — the joiner keeps its own id, NOT the leader's (the fix; RED before it).
	var self string
	if err := tdb.QueryRow(`SELECT value FROM cluster_meta WHERE key='self_node_id'`).Scan(&self); err != nil {
		t.Fatalf("read self_node_id: %v", err)
	}
	if self != "racknerd-joiner" {
		t.Fatalf("self_node_id=%q after InstallSnapshot — the joiner ADOPTED THE LEADER's identity "+
			"(split brain on next restart); must preserve 'racknerd-joiner'", self)
	}

	// (2) DATA TRANSFERRED — the joiner did receive the leader's replicated state (snapshot install worked).
	var data string
	if err := tdb.QueryRow(`SELECT value FROM cluster_meta WHERE key='t:grow'`).Scan(&data); err != nil || data != "leader-data" {
		t.Fatalf("joiner missing the leader's snapshot data: got %q err=%v", data, err)
	}
}

// TestRestoreStripsRestoreInProgressMarker pins R16 B1: `restore_in_progress` is a SOURCE-NODE-LOCAL
// recovery flag. A grow-ready snapshot is taken (correctly) while the source still carries it (restore.go's
// fail-closed crash window), so the snapshot DB has restore_in_progress='1'. A fresh joiner that
// InstallSnapshots it MUST NOT inherit the marker — otherwise its next restart FATALs
// (assertNoInterruptedRestore) on a restore it never ran. fsm.restoreFrom strips it on every install, like
// self_node_id. RED before the fix (the marker rode the snapshot into the joiner).
func TestRestoreStripsRestoreInProgressMarker(t *testing.T) {
	srcDir := t.TempDir()
	a := mustNode(t, srcDir, "restored-survivor")
	// The survivor's live DB carries the marker at snapshot time (grow-ready snapshot precedes the clear).
	if _, err := a.db.Exec(`INSERT INTO cluster_meta(key,value) VALUES('restore_in_progress','1') ON CONFLICT(key) DO UPDATE SET value='1'`); err != nil {
		t.Fatalf("seed restore_in_progress: %v", err)
	}
	if err := a.ApplyMetaSet("t:grow", "survivor-data"); err != nil {
		t.Fatalf("seed data: %v", err)
	}
	if err := a.Snapshot(); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	_, rc := openSnapshot(t, srcDir)
	snapBytes, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil || len(snapBytes) == 0 {
		t.Fatalf("read snapshot: %v (len=%d)", err, len(snapBytes))
	}

	tf, tdb := freshFSM(t, t.TempDir())
	if _, err := tdb.Exec(`INSERT INTO cluster_meta(key,value) VALUES('self_node_id','fresh-joiner')`); err != nil {
		t.Fatalf("seed joiner id: %v", err)
	}
	if err := tf.Restore(io.NopCloser(bytes.NewReader(snapBytes))); err != nil {
		t.Fatalf("restore: %v", err)
	}

	// The marker MUST be stripped on install (else the joiner bricks on its next restart).
	var n int
	if err := tdb.QueryRow(`SELECT COUNT(*) FROM cluster_meta WHERE key='restore_in_progress'`).Scan(&n); err != nil {
		t.Fatalf("count restore_in_progress: %v", err)
	}
	if n != 0 {
		t.Fatal("restore_in_progress rode the grow-ready snapshot into the joiner — it FATALs on next restart " +
			"(assertNoInterruptedRestore) on a restore it never ran; fsm.restoreFrom must strip it")
	}
	// The real data still transferred.
	var data string
	if err := tdb.QueryRow(`SELECT value FROM cluster_meta WHERE key='t:grow'`).Scan(&data); err != nil || data != "survivor-data" {
		t.Fatalf("joiner missing the survivor's snapshot data: got %q err=%v", data, err)
	}
}

// restore_identity_test.go — what an InstallSnapshot may and may not carry between nodes.
//
// origin: prerelease audit cluster-fsm/L3-F1.
//
// A raft InstallSnapshot copies the LEADER's database byte for byte, so two node-local
// values in it are wrong for the receiver: cluster_meta.self_node_id (the leader's raft
// ServerID) and restore_in_progress (the source's own recovery flag, which makes the
// daemon FATAL until `recovery restore` completes).
//
// These used to be corrected AFTER the install, in two separate transactions on the
// live database, with the identity read out of that same database beforehand and the
// read error discarded. Anything interrupting that sequence left the LEADER's id in the
// joiner's cluster_meta — and the damage was self-confirming, because the next
// InstallSnapshot attempt re-read the id from there, found the leader's, and faithfully
// preserved it. The joiner's own identity was gone, and its next restart brought it up
// as a second server claiming the leader's ServerID.

// stagedSnapshot builds a file shaped like a leader's snapshot: migrated schema,
// somebody else's self_node_id, and the source's restore_in_progress marker.
func stagedSnapshot(t *testing.T, leaderID string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "snap.db")
	db, err := storage.Open("file:" + path)
	if err != nil {
		t.Fatalf("open staging: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO cluster_meta(key,value) VALUES('self_node_id',?)`, leaderID); err != nil {
		t.Fatalf("seed self_node_id: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO cluster_meta(key,value) VALUES('restore_in_progress','1')`); err != nil {
		t.Fatalf("seed restore_in_progress: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

func metaValue(t *testing.T, path, key string) (string, bool) {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer func() { _ = db.Close() }()
	var v string
	switch err := db.QueryRow(`SELECT value FROM cluster_meta WHERE key=?`, key).Scan(&v); err {
	case nil:
		return v, true
	case sql.ErrNoRows:
		return "", false
	default:
		t.Fatalf("read %s: %v", key, err)
		return "", false
	}
}

// TestTheStagingFileIsCorrectedBeforeItIsInstalled is the whole of the fix: after
// prepareRestoreStaging the file on disk is already right, so the install has nothing
// left to fix up and therefore nothing left to be interrupted between.
func TestTheStagingFileIsCorrectedBeforeItIsInstalled(t *testing.T) {
	path := stagedSnapshot(t, "leader-1")

	if err := prepareRestoreStaging(path, "joiner-2"); err != nil {
		t.Fatalf("prepare staging: %v", err)
	}

	got, ok := metaValue(t, path, "self_node_id")
	if !ok || got != "joiner-2" {
		t.Fatalf("staged self_node_id=%q present=%v, want joiner-2.\n\n"+
			"If the identity is written to the LIVE database after the install instead, then every "+
			"interruption of that window leaves the leader's id in the joiner's cluster_meta — and "+
			"the next InstallSnapshot re-reads it from there and preserves it, so retrying makes it "+
			"permanent rather than fixing it.", got, ok)
	}
	if _, present := metaValue(t, path, "restore_in_progress"); present {
		t.Fatal("the source's restore_in_progress survived into the staging file.\n\n" +
			"A grow-ready snapshot is taken BEFORE the source clears its marker, so it always carries " +
			"one. A joiner that installs it FATALs on its next restart, on a restore it never ran.")
	}
}

// TestAnUnknownIdentityLeavesTheSnapshotsOwnValue pins the fallback. A construction
// site with no id to give must not silently blank the column — that would be a
// different way to lose the identity.
func TestAnUnknownIdentityLeavesTheSnapshotsOwnValue(t *testing.T) {
	path := stagedSnapshot(t, "leader-1")

	if err := prepareRestoreStaging(path, ""); err != nil {
		t.Fatalf("prepare staging: %v", err)
	}
	got, ok := metaValue(t, path, "self_node_id")
	if !ok || got != "leader-1" {
		t.Fatalf("an empty id must leave the value alone, got %q present=%v", got, ok)
	}
	// The marker strip is unconditional and must still have happened.
	if _, present := metaValue(t, path, "restore_in_progress"); present {
		t.Fatal("restore_in_progress survived an empty-id prepare; the strip is not conditional on the id")
	}
}

// TestBothStagingCorrectionsRideOneTransaction is the atomicity claim itself. If the
// second statement fails, the first must not be visible — otherwise the staging file
// is a new half-corrected artefact and we have moved the window rather than closed it.
//
// THE FAILURE HAS TO LAND ON THE SECOND STATEMENT, and the first version of this test
// did not manage that — origin: prerelease audit round 2, H-1. It chmod-ed the staging
// file to 0400 and described that as making "the COMMIT fail after the statements ran".
// It does no such thing: sqlite cannot open a read-only file read-write at all, so the
// FIRST write failed and neither statement ever ran. self_node_id was therefore still
// "leader-1" for the trivial reason that nothing had touched it, and the test passed
// identically against the exact defect it names — two bare db.Exec calls with no
// transaction and no rollback.
//
// A BEFORE DELETE trigger is the fixture that actually models it: the identity write
// commits inside the transaction, and then the strip aborts. A non-transactional
// implementation leaves the joiner's id stamped on a staging file that still carries
// restore_in_progress — half-corrected, and installable.
func TestBothStagingCorrectionsRideOneTransaction(t *testing.T) {
	path := stagedSnapshot(t, "leader-1")
	armFailingStrip(t, path)

	if err := prepareRestoreStaging(path, "joiner-2"); err == nil {
		t.Fatal("premise broken: the strip was supposed to abort, so this test is not exercising " +
			"the rollback it claims to. A guard that cannot fail is worse than no guard.")
	}

	got, ok := metaValue(t, path, "self_node_id")
	if got != "leader-1" {
		t.Fatalf("a failed prepare left self_node_id=%q — the two corrections did not roll back "+
			"together, so a staging file carrying the JOINER's identity AND the source's "+
			"restore_in_progress marker can still be installed", got)
	}
	// And the marker the first statement's partner would have removed is still there,
	// which is the other half of "nothing was applied".
	if _, present := metaValue(t, path, "restore_in_progress"); !present {
		t.Fatal("restore_in_progress is gone even though the strip aborted")
	}
	if !ok {
		t.Fatal("self_node_id vanished entirely — the rollback undid more than the transaction did")
	}
}

// armFailingStrip installs a trigger that makes exactly the SECOND staging
// correction — the restore_in_progress strip — abort, while leaving the first one
// free to succeed. That asymmetry is the whole point: a fixture that breaks the
// first statement proves nothing about whether the two ride one transaction.
func armFailingStrip(t *testing.T, path string) {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatalf("open staging: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.Exec(`CREATE TRIGGER refuse_strip BEFORE DELETE ON cluster_meta
		WHEN OLD.key='restore_in_progress'
		BEGIN SELECT RAISE(ABORT, 'staged strip refused by the test fixture'); END`); err != nil {
		t.Fatalf("arm the failing strip: %v", err)
	}
}

// TestRestoreCorrectsTheStagingFileBeforeInstallingIt is the WIRING half, and it exists
// because the behavioural tests above do not need it.
//
// They call prepareRestoreStaging directly, so they stay green against a restoreFrom
// that never calls it — which is the same vacuity the repo has been bitten by before
// (a test of the mapping function passing while the handler still hard-codes the old
// path). What has to be true is an ORDER: the staging file is corrected, and only then
// installed. Asserting that in the source is cheap and is exactly what a future edit
// would undo.
func TestRestoreCorrectsTheStagingFileBeforeInstallingIt(t *testing.T) {
	src, err := os.ReadFile("snapshot.go")
	if err != nil {
		t.Fatalf("read source: %v", err)
	}
	fset := token.NewFileSet()
	f, perr := parser.ParseFile(fset, "snapshot.go", src, 0)
	if perr != nil {
		t.Fatalf("parse: %v", perr)
	}
	var body string
	for _, d := range f.Decls {
		fn, ok := d.(*ast.FuncDecl)
		if !ok || fn.Body == nil || fn.Name.Name != "restoreFrom" {
			continue
		}
		body = string(src[fset.Position(fn.Body.Pos()).Offset:fset.Position(fn.Body.End()).Offset])
	}
	if body == "" {
		t.Fatal("SELF-CHECK FAILED: restoreFrom not found in snapshot.go — this guard is scanning " +
			"for a function that no longer exists, so it can never report anything")
	}

	prepAt := strings.Index(body, "prepareRestoreStaging(")
	installAt := strings.Index(body, "restoreInPlace(")
	if installAt < 0 {
		t.Fatal("SELF-CHECK FAILED: restoreFrom no longer calls restoreInPlace; the scanner does not " +
			"match its real shape")
	}
	if prepAt < 0 {
		t.Fatal("restoreFrom never corrects the staging file.\n\n" +
			"Whatever prepareRestoreStaging does, it does not happen — so the leader's self_node_id " +
			"and the source's restore_in_progress ride the snapshot into the live database.")
	}
	if prepAt > installAt {
		t.Fatal("restoreFrom corrects the staging file AFTER installing it.\n\n" +
			"That is the original defect with extra steps: any interruption between the install and " +
			"the correction leaves the LEADER's id in this node's cluster_meta, and the next " +
			"InstallSnapshot re-reads it from there and preserves it, so retrying cements it.")
	}

	// And the live database must not be patched afterwards. Two writes to f.db after the
	// install is precisely the shape that was replaced.
	after := body[installAt:]
	if strings.Contains(after, "f.db.Exec(`INSERT INTO cluster_meta(key,value) VALUES('self_node_id'") ||
		strings.Contains(after, "f.db.Exec(`DELETE FROM cluster_meta WHERE key='restore_in_progress'") {
		t.Error("restoreFrom still patches the LIVE database after the install.\n\n" +
			"The staging corrections and a post-install fixup are not complementary: whichever one " +
			"runs last defines the window, and the point of correcting the staging file is that " +
			"there is no window at all.")
	}
}

// origin: prerelease audit round 2, H-3 / CC-4.
//
// f.localID IS THE FIX, and nothing exercised it.
//
// L3-F1's headline change is that restoreFrom prefers the id raft itself knows over
// re-reading the identity from the database it is about to overwrite. Every test that
// drove restoreFrom went through the `selfID == ""` DB-fallback branch, because the
// helper that builds the fsm never set localID — so mutating `selfID := f.localID` to
// `selfID := ""`, i.e. reverting to exactly the pre-fix identity source the comment
// blames for split brain, left internal/cluster and test/cluster both green.
func TestRestorePrefersTheIdentityRaftKnowsOverTheOneOnDisk(t *testing.T) {
	// A staging file carrying the LEADER's id, and a live DB that has ALREADY been
	// corrupted to hold the leader's id too — the self-confirming state the comment
	// describes, where re-reading from disk faithfully preserves the wrong answer.
	path := stagedSnapshot(t, "leader-1")
	f, db := freshFSM(t, t.TempDir())
	if _, err := db.Exec(
		`INSERT INTO cluster_meta(key,value) VALUES('self_node_id','leader-1')
		 ON CONFLICT(key) DO UPDATE SET value=excluded.value`); err != nil {
		t.Fatal(err)
	}
	f.localID = "joiner-2"

	// THE WIRING FIRST. Reproducing the resolution inline here and asserting on it would
	// pass against a restoreFrom that reads the DB — which is precisely the mutation this
	// guard exists to catch, and the first version of this test did exactly that and
	// stayed green under it.
	src, rerr := os.ReadFile("snapshot.go")
	if rerr != nil {
		t.Fatal(rerr)
	}
	if !strings.Contains(string(src), "selfID := f.localID") {
		t.Fatal("restoreFrom no longer resolves the identity from f.localID.\n\n" +
			"Reading it out of the live database is the pre-fix source the comment blames for " +
			"split brain: an interrupted restore leaves the LEADER's id there, and the next " +
			"attempt faithfully 'preserves' it, so retrying cements the damage instead of " +
			"repairing it.")
	}

	// And the resolution it performs, given that shape: localID wins.
	selfID := f.localID
	if selfID == "" {
		var v string
		_ = f.db.QueryRow(`SELECT value FROM cluster_meta WHERE key='self_node_id'`).Scan(&v)
		selfID = v
	}
	if selfID != "joiner-2" {
		t.Fatalf("resolved identity %q, want joiner-2.\n\n"+
			"The live DB says leader-1 because a previous interrupted restore put it there. "+
			"Re-reading from disk 'preserves' that, which is why the damage was self-confirming "+
			"and retries cemented it. raft's own id is the only source the restore cannot have "+
			"corrupted.", selfID)
	}

	if err := prepareRestoreStaging(path, selfID); err != nil {
		t.Fatalf("prepare staging: %v", err)
	}
	if got, ok := metaValue(t, path, "self_node_id"); !ok || got != "joiner-2" {
		t.Fatalf("staged self_node_id=%q present=%v, want joiner-2", got, ok)
	}
}

// TestEveryFSMConstructionSiteSetsLocalID is the other half of CC-4: the field is only
// as good as the sites that populate it, and a fifth `&fsm{}` that forgets it would
// silently reinstate the DB-read fallback for whichever path it serves.
func TestEveryFSMConstructionSiteSetsLocalID(t *testing.T) {
	// AST, not a substring count: node.go also carries a `localID:` on the Node struct,
	// so counting the token would compare two unrelated things and pass by luck.
	total := 0
	for _, file := range []string{"node.go", "offline.go"} {
		src, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("read %s: %v", file, err)
		}
		fset := token.NewFileSet()
		f, perr := parser.ParseFile(fset, file, src, 0)
		if perr != nil {
			t.Fatalf("parse %s: %v", file, perr)
		}
		ast.Inspect(f, func(node ast.Node) bool {
			cl, ok := node.(*ast.CompositeLit)
			if !ok {
				return true
			}
			id, ok := cl.Type.(*ast.Ident)
			if !ok || id.Name != "fsm" {
				return true
			}
			total++
			for _, e := range cl.Elts {
				if kv, ok := e.(*ast.KeyValueExpr); ok {
					if k, ok := kv.Key.(*ast.Ident); ok && k.Name == "localID" {
						return true
					}
				}
			}
			t.Errorf("%s:%d builds an fsm without setting localID.\n\n"+
				"That site falls back to reading the identity out of the database the restore is "+
				"about to overwrite — the exact source L3-F1 exists to stop trusting.",
				file, fset.Position(cl.Pos()).Line)
			return true
		})
	}
	if total < 3 {
		t.Fatalf("SELF-CHECK FAILED: found only %d fsm construction site(s); the scanner no longer "+
			"matches the real shape, and a scan that finds nothing reports no offenders", total)
	}
}
