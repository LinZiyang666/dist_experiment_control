package session

import (
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/storage"
)

// creators_test.go — the session-create allow-list.
//
// origin: prerelease audit increment 2 internal review. creators.go shipped with NO test
// file at all (test-blast-radius/F4), while four behavioural promises lived only in its
// comments: idempotent re-admission, "removing a fingerprint does not touch its sessions",
// a read error refusing rather than reading as an empty table, and the single-mode and
// replicated writes producing the same row. Every one of them was reported by a lane that
// had to read the code to find out whether it was true.

func creatorsDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := storage.Open(":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

const (
	fpA = "SHA256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	fpB = "SHA256:BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"
)

func TestAdmissionAdmitsOnlyWhatWasAdmitted(t *testing.T) {
	db := creatorsDB(t)
	now := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)

	if ok, err := MayCreateSession(db, fpA); err != nil || ok {
		t.Fatalf("an empty allow-list admitted %s (ok=%v err=%v) — a fresh broker must admit nobody", fpA, ok, err)
	}
	if _, err := AllowCreator(db, fpA, "admin", "first operator", now); err != nil {
		t.Fatal(err)
	}
	if ok, err := MayCreateSession(db, fpA); err != nil || !ok {
		t.Fatalf("an admitted fingerprint was refused (ok=%v err=%v)", ok, err)
	}
	if ok, err := MayCreateSession(db, fpB); err != nil || ok {
		t.Fatalf("admitting %s also admitted %s — the check is not keyed on the fingerprint", fpA, fpB)
	}
	// The empty fingerprint is not a wildcard. FingerprintFromActor cannot produce it, so
	// reaching this with "" means something upstream failed to derive one, and treating
	// that as a match would admit exactly the caller whose identity could not be read.
	if ok, err := MayCreateSession(db, ""); err != nil || ok {
		t.Fatalf("the empty fingerprint was admitted (ok=%v err=%v)", ok, err)
	}
}

// The idempotency promise, and the half of it that was silently untrue: a re-admission
// keeps the original row, INCLUDING its note, so a --note passed the second time does
// nothing. That is the intended behaviour; what was wrong is that nothing said so.
func TestReAdmittingIsIdempotentAndSaysItChangedNothing(t *testing.T) {
	db := creatorsDB(t)
	t0 := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)

	added, err := AllowCreator(db, fpA, "admin", "first note", t0)
	if err != nil || !added {
		t.Fatalf("first admission: added=%v err=%v", added, err)
	}
	added, err = AllowCreator(db, fpA, "someone-else", "second note", t0.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if added {
		t.Fatal("re-admitting an existing fingerprint reported that it ADDED a row.\n\n" +
			"The caller uses this to tell the operator whether anything changed; reporting true " +
			"here is how a --note that was silently ignored gets reported as applied.")
	}
	list, err := ListCreators(db)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 {
		t.Fatalf("re-admission produced %d rows, want 1", len(list))
	}
	if list[0].AddedBy != "admin" || list[0].Note != "first note" {
		t.Fatalf("re-admission rewrote the row: added_by=%q note=%q, want the ORIGINAL admin/\"first note\" "+
			"— who admitted whom must not be rewritable by re-running the command",
			list[0].AddedBy, list[0].Note)
	}
}

// origin: increment 2 internal review, empirical mut-admission/F3 — "removing an fp does
// not touch its sessions" is promised in three places and was guarded nowhere.
//
// The promise matters because it is what makes the verb safe to use: an operator revoking
// somebody's ability to CREATE sessions must not be silently deleting the sessions that
// person's team is currently working in.
func TestRemovingACreatorDoesNotTouchItsSessions(t *testing.T) {
	db := creatorsDB(t)
	now := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	if _, err := db.Exec(
		`INSERT INTO sessions(sid, name, owner_pubkey_fp, pin_hash) VALUES (?,?,?,?)`,
		"lab", "lab", fpA, "hash"); err != nil {
		t.Fatal(err)
	}
	if _, err := AllowCreator(db, fpA, "admin", "", now); err != nil {
		t.Fatal(err)
	}
	removed, err := DenyCreator(db, fpA)
	if err != nil || !removed {
		t.Fatalf("DenyCreator: removed=%v err=%v", removed, err)
	}
	var sessions int
	if err := db.QueryRow(`SELECT COUNT(*) FROM sessions WHERE owner_pubkey_fp=?`, fpA).Scan(&sessions); err != nil {
		t.Fatal(err)
	}
	if sessions != 1 {
		t.Fatalf("removing the creator deleted %d of its sessions.\n\n"+
			"Revoking the ability to create is not the same as deleting somebody's work, and an "+
			"operator who believes otherwise will not use this verb when they should.", 1-sessions)
	}
	// And removing something that is not there is reported honestly, not as a removal.
	if removed, err := DenyCreator(db, fpB); err != nil || removed {
		t.Fatalf("removing an absent fingerprint reported removed=%v err=%v", removed, err)
	}
}

// origin: increment 2 internal review, raft-op/F4 ≡ repo-invariants/F4 ≡
// admission-product/L8-F9 ≡ empirical mut-admission/F8.
//
// THE TWO WRITE PATHS MUST PRODUCE THE SAME ROW. Single mode writes through AllowCreator;
// cluster mode bakes SQL in PlanSetCreator and the FSM applies it. §13.2/DIFF-1 requires
// the results to be equivalent, and they were not: the Plan baked RFC3339Nano while the
// direct path let the driver encode a time.Time. Two text encodings in one column, in a
// column ListCreators sorts by and documents as "oldest first".
func TestTheDirectAndReplicatedAdmissionWritesAgree(t *testing.T) {
	now := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)

	direct := creatorsDB(t)
	if _, err := AllowCreator(direct, fpA, "admin", "n", now); err != nil {
		t.Fatal(err)
	}
	replicated := creatorsDB(t)
	cmd, err := PlanSetCreator(fpA, "admin", "n", true, now)
	if err != nil {
		t.Fatal(err)
	}
	applyCommand(t, replicated, cmd)

	got, want := readCreatorRow(t, replicated, fpA), readCreatorRow(t, direct, fpA)
	if got != want {
		t.Fatalf("the replicated write stored %q but the direct write stored %q.\n\n"+
			"Same operator action, same inputs, two different rows — so a fingerprint's added_at "+
			"depends on whether the broker it was typed at is clustered. ListCreators sorts on that "+
			"column and calls the result \"oldest first\".", got, want)
	}
}

// The backfill: the whole "upgrade needs no operator action" promise, which had no test
// at all (empirical migration-0019/F1, test-blast-radius/F3 — deleting it left six suites
// green), and whose migration form resurrected revocations on every re-run.
func TestTheUpgradeBackfillRunsOnceAndDoesNotResurrectRevocations(t *testing.T) {
	db := creatorsDB(t)
	now := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	for _, s := range []struct{ sid, owner string }{{"lab", fpA}, {"gpu", fpB}, {"old", fpA}} {
		if _, err := db.Exec(
			`INSERT INTO sessions(sid, name, owner_pubkey_fp, pin_hash) VALUES (?,?,?,?)`,
			s.sid, s.sid, s.owner, "hash"); err != nil {
			t.Fatal(err)
		}
	}
	// A session with no owner must not produce a row: "" is not a fingerprint and would
	// be admitted as a literal empty string.
	if _, err := db.Exec(
		`INSERT INTO sessions(sid, name, owner_pubkey_fp, pin_hash) VALUES (?,?,?,?)`,
		"orphan", "orphan", "", "hash"); err != nil {
		t.Fatal(err)
	}

	if seeded, err := CreatorsSeeded(db); err != nil || seeded {
		t.Fatalf("a DB that has never been seeded reports seeded=%v err=%v", seeded, err)
	}
	fps, err := OwnersNeedingAdmission(db)
	if err != nil {
		t.Fatal(err)
	}
	if len(fps) != 2 || fps[0] != fpA || fps[1] != fpB {
		t.Fatalf("owners needing admission = %v, want exactly [%s %s] (distinct, no empty owner)", fps, fpA, fpB)
	}
	if err := SeedCreatorsLocally(db, fps, now); err != nil {
		t.Fatal(err)
	}
	for _, fp := range fps {
		if ok, merr := MayCreateSession(db, fp); merr != nil || !ok {
			t.Fatalf("%s owned a session before the upgrade and was not grandfathered in", fp)
		}
	}
	if seeded, serr := CreatorsSeeded(db); serr != nil || !seeded {
		t.Fatalf("the backfill did not record its marker (seeded=%v err=%v) — without it every "+
			"boot re-derives the allow-list from the sessions table", seeded, serr)
	}

	// THE RESURRECTION CASE, which is the entire reason this moved out of the migration.
	// Revoke one, then run the backfill logic again exactly as a later upgrade would.
	if _, err := DenyCreator(db, fpA); err != nil {
		t.Fatal(err)
	}
	if seeded, serr := CreatorsSeeded(db); serr != nil || !seeded {
		t.Fatal("the marker did not survive a revocation")
	}
	// A caller that honours the marker does nothing; prove the marker is what stops it by
	// showing the fingerprint stays revoked while its sessions still exist.
	var stillOwned int
	if err := db.QueryRow(`SELECT COUNT(*) FROM sessions WHERE owner_pubkey_fp=?`, fpA).Scan(&stillOwned); err != nil {
		t.Fatal(err)
	}
	if stillOwned == 0 {
		t.Fatal("the fixture is wrong: the revoked fingerprint must still own sessions, or this " +
			"test cannot detect the resurrection it exists for")
	}
	if ok, merr := MayCreateSession(db, fpA); merr != nil || ok {
		t.Fatal("a revoked fingerprint is admitted again while it still owns sessions.\n\n" +
			"That is the migration-time backfill's failure mode: it re-derived the allow-list from " +
			"the sessions table, and `--remove` deliberately does not delete sessions — so every " +
			"upgrade undid every revocation.")
	}
}

// The replicated backfill must carry its marker in the SAME command as its rows: a
// leadership change between two entries would leave the rows applied and the marker
// missing, and the next boot would re-derive — the resurrection again, through a narrower
// window.
func TestTheReplicatedBackfillCarriesItsMarkerInTheSameEntry(t *testing.T) {
	now := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	cmd, err := PlanSeedCreators([]string{fpA, fpB}, now)
	if err != nil {
		t.Fatal(err)
	}
	db := creatorsDB(t)
	applyCommand(t, db, cmd)
	for _, fp := range []string{fpA, fpB} {
		if ok, merr := MayCreateSession(db, fp); merr != nil || !ok {
			t.Fatalf("%s was not admitted by the replicated backfill", fp)
		}
	}
	if seeded, serr := CreatorsSeeded(db); serr != nil || !seeded {
		t.Fatal("the replicated backfill applied its rows without its marker — a re-run would " +
			"re-derive the allow-list and undo any revocation made in between")
	}
	// An empty fleet still records the decision, or every boot re-asks it against a
	// sessions table that has since grown.
	empty := creatorsDB(t)
	emptyCmd, err := PlanSeedCreators(nil, now)
	if err != nil {
		t.Fatal(err)
	}
	applyCommand(t, empty, emptyCmd)
	if seeded, serr := CreatorsSeeded(empty); serr != nil || !seeded {
		t.Fatal("a backfill with nothing to grandfather did not record that it ran")
	}
}

// applyCommand runs a planned Command's statements the way the FSM does: in one
// transaction, in order.
func applyCommand(t *testing.T, db *sql.DB, cmd *cluster.Command) {
	t.Helper()
	if cmd == nil {
		t.Fatal("nil command")
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	for _, st := range cmd.Body {
		if _, err := tx.Exec(st.SQL); err != nil {
			t.Fatalf("apply %q: %v", st.SQL, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

// readCreatorRow returns the row as STORED, not as read back.
//
// quote(added_at) rather than added_at, and that is the whole point of the helper. The
// driver PARSES a TIMESTAMP column on the way out and re-renders it, so scanning into a
// string normalises two different stored encodings into one value — a comparison that
// cannot see the defect it is checking for. `ORDER BY added_at`, which ListCreators
// documents as "oldest first", compares the raw text. So does anything else SQLite does
// with the column.
//
// Measured while writing this: the modernc driver stores a time.Time parameter as
// "2026-09-03 12:00:00 +0000 UTC" (Go's time.Time.String, which is exactly what
// cluster.LitTime renders), while scanning it back into a string yields
// "2026-09-03T12:00:00Z". Both write paths agreeing on the SECOND of those proves nothing.
func readCreatorRow(t *testing.T, db *sql.DB, fp string) string {
	t.Helper()
	var addedAt, addedBy, note string
	if err := db.QueryRow(
		`SELECT quote(added_at), added_by, COALESCE(note,'') FROM session_creators WHERE fp=?`, fp,
	).Scan(&addedAt, &addedBy, &note); err != nil {
		t.Fatalf("read creator row %s: %v", fp, err)
	}
	return strings.Join([]string{addedAt, addedBy, note}, "|")
}

// TestAdmissionGrandfathersExistingOwnersOnlyUntilTheMarkerLands covers the window the
// upgrade backfill has not closed yet.
//
// origin: prerelease audit external review M-2. A rolling upgrade is
// followers-first/leader-last, so every new broker starts while the old leader still
// serves and the leader-last node returns as a follower — nobody ran the backfill, and
// each new follower began enforcing an EMPTY allow-list against owners who had been
// creating sessions for months. The retry pass fixes who runs it; this covers what the
// brokers answer in the meantime, which is a separate question with a separate wrong
// answer (refuse everyone).
//
// The three cases are the whole contract: an existing owner is admitted, a stranger is
// NOT (the fallback must not widen to "anyone"), and once the marker is committed the
// table is the only authority — so a revoked owner stays revoked instead of being
// resurrected by the very fallback that was supposed to be temporary.
func TestAdmissionGrandfathersExistingOwnersOnlyUntilTheMarkerLands(t *testing.T) {
	db := creatorsDB(t)
	now := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	if _, err := db.Exec(
		`INSERT INTO sessions(sid, name, owner_pubkey_fp, pin_hash) VALUES (?,?,?,?)`,
		"lab", "lab", fpA, "hash"); err != nil {
		t.Fatalf("seed session: %v", err)
	}

	// --- before the marker: the owner is admitted, the stranger is not ---
	if ok, err := MayCreateSession(db, fpA); err != nil || !ok {
		t.Fatalf("an existing session OWNER was refused before the backfill marker committed "+
			"(ok=%v err=%v). During a rolling upgrade that is every one of them.", ok, err)
	}
	if ok, err := MayCreateSession(db, fpB); err != nil || ok {
		t.Fatalf("a fingerprint that owns nothing was admitted before the marker (ok=%v err=%v); "+
			"the pre-marker fallback must admit exactly the set the backfill would, not everyone", ok, err)
	}

	// --- the backfill commits: rows for the owners plus the marker ---
	if err := SeedCreatorsLocally(db, []string{fpA}, now); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if seeded, err := CreatorsSeeded(db); err != nil || !seeded {
		t.Fatalf("marker not committed after the backfill (seeded=%v err=%v)", seeded, err)
	}

	// --- after the marker: the table is the ONLY authority ---
	if _, err := AllowCreator(db, fpA, "operator", "", now); err != nil {
		t.Fatalf("re-admit: %v", err)
	}
	if _, err := DenyCreator(db, fpA); err != nil {
		t.Fatalf("revoke: %v", err)
	}
	if ok, err := MayCreateSession(db, fpA); err != nil || ok {
		t.Fatalf("a REVOKED owner was re-admitted by the pre-marker fallback (ok=%v err=%v).\n\n"+
			"Revocation deliberately does not delete the owner's existing sessions, so 'owns a "+
			"session' stays true forever afterwards. If the fallback outlived the marker it "+
			"would silently undo every revocation an operator ever performed.", ok, err)
	}
}
