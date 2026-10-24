// Session writers depend on upgradeauthority; keep their integration fixtures
// in the external test package so the tests cannot introduce the reverse edge.
// origin: S8 internal review FIX-VETTAGS F2
package upgradeauthority_test

import (
	"bytes"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	clusterstate "github.com/LinZiyang666/tether/internal/cluster/state"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradeauthority"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// origin: hot-update S8 plan §6.2 / P08
func TestReadCreationObservesTransactionWrites(t *testing.T) {
	db, creation, guard := authorityDatabase(t)
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	newHash := authorityPHC(bytes.Repeat([]byte{42}, 16), 2)
	if _, err := tx.Exec(`UPDATE sessions SET pin_hash=? WHERE sid=?`, newHash, guard.SID); err != nil {
		t.Fatal(err)
	}
	if got, err := upgradeauthority.ReadCreation(tx, guard); got != (upgradeauthority.Creation{}) {
		t.Fatal("mismatch returned local SQL material")
	} else {
		assertAuthorityError(t, err, upgradetx.CodeSessionIncarnationMismatch, newHash)
	}
	// Empty expected is reserved for ordinary first register; this must see the
	// uncommitted row through the supplied transaction, not a different pool.
	got, err := upgradeauthority.ReadCreation(tx, upgradeauthority.Guard{SID: guard.SID})
	if err != nil || got.EncodedPINHash != newHash {
		t.Fatal("ReadCreation did not use the caller's transaction")
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	got, err = upgradeauthority.ReadCreation(db, guard)
	if err != nil || got != creation {
		t.Fatal("transaction rollback changed the original creation")
	}
}

// origin: hot-update S8 plan §6.2 / P08
func TestReadCreationRejectsRecreatedSession(t *testing.T) {
	db, creation, guard := authorityDatabase(t)
	if _, err := db.Exec(`DELETE FROM sessions WHERE sid=?`, guard.SID); err != nil {
		t.Fatal(err)
	}
	if got, err := upgradeauthority.ReadCreation(db, guard); got != (upgradeauthority.Creation{}) {
		t.Fatal("missing session returned comparison material")
	} else {
		assertAuthorityError(t, err, upgradetx.CodeSessionIncarnationMismatch)
	}
	// Reuse every other field, so only the creation salt distinguishes the
	// recreated SID. No delivered deletion event is required to detect it.
	creation.EncodedPINHash = authorityPHC(bytes.Repeat([]byte{99}, 16), 1)
	insertAuthorityCreation(t, db, creation)
	if got, err := upgradeauthority.ReadCreation(db, guard); got != (upgradeauthority.Creation{}) {
		t.Fatal("recreated session returned material to the old image")
	} else {
		assertAuthorityError(t, err, upgradetx.CodeSessionIncarnationMismatch, creation.EncodedPINHash)
	}
	learned, err := upgradeauthority.ReadCreation(db, upgradeauthority.Guard{SID: guard.SID})
	if err != nil || learned != creation {
		t.Fatal("ordinary first register could not learn the recreated session")
	}
	// Cleanup compares creation independently of lifecycle. The actual session
	// delete writer must add its DELETING predicate; node register adds ACTIVE.
	if _, err := db.Exec(`UPDATE sessions SET state='DELETING' WHERE sid=?`, guard.SID); err != nil {
		t.Fatal(err)
	}
	if got, err := upgradeauthority.ReadCreation(db, upgradeauthority.Guard{SID: guard.SID}); err != nil || got != creation {
		t.Fatal("creation comparison incorrectly excluded tombstoned cleanup")
	}
}

// origin: hot-update S8 plan §6.2 / P08
func TestReadCreationPreservesStoredTextAndDecodedSalt(t *testing.T) {
	db, creation, guard := authorityDatabase(t)
	instant := authorityInstant()
	for _, text := range []string{
		instant.String(), instant.Format(time.RFC3339Nano),
		instant.In(time.FixedZone("CDT", -5*3600)).String(),
		instant.In(time.FixedZone("", 330*60)).String(),
		instant.String() + " m=+0.123456789",
		instant.Format("2006-01-02 15:04:05.999999999"),
		instant.Format("2006-01-02T15:04:05.999999999"),
		instant.Format("2006-01-02 15:04:05.999999999-07:00"),
	} {
		if _, err := db.Exec(`UPDATE sessions SET created_at=? WHERE sid=?`, text, guard.SID); err != nil {
			t.Fatal(err)
		}
		got, err := upgradeauthority.ReadCreation(db, guard)
		if err != nil || got.CreatedAtText != text {
			t.Fatalf("timestamp spelling %q changed the digest or local SQL binding: %v", text, err)
		}
	}
	// A re-encoded hash with identical salt and different hash bytes is not a
	// new creation. Numeric PHC spellings accepted by auth retain that property.
	reencoded := strings.Replace(authorityPHC(authoritySalt(), 9), "v=19$m=65536", "v=019$m=065536", 1)
	if _, err := db.Exec(`UPDATE sessions SET pin_hash=? WHERE sid=?`, reencoded, guard.SID); err != nil {
		t.Fatal(err)
	}
	got, err := upgradeauthority.ReadCreation(db, guard)
	if err != nil || got.EncodedPINHash != reencoded {
		t.Fatal("incarnation hashed the PHC text instead of the decoded salt")
	}
	encoded, err := json.Marshal(got)
	if err != nil || string(encoded) != "{}" {
		t.Fatal("local SQL material entered JSON")
	}
	// Exercise auth's real writer and session.Create's real time binding once,
	// rather than proving compatibility only with a hand-written PHC fixture.
	phc, err := auth.HashPIN("authority-fixture")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	if _, err := session.Create(db, "real-auth", "real-auth", creation.Owner, phc, now); err != nil {
		t.Fatal(err)
	}
	salt, err := base64.RawStdEncoding.DecodeString(strings.Split(phc, "$")[4])
	if err != nil {
		t.Fatal("auth produced malformed creation salt")
	}
	expected, err := upgradeauthority.Derive("real-auth", creation.Owner, now.UnixNano(), salt)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := upgradeauthority.ReadCreation(db, upgradeauthority.Guard{SID: "real-auth", Incarnation: expected}); err != nil {
		t.Fatal(err)
	}
	// SQLite's DEFAULT and the older short forms have lower precision; their
	// decoded instant still agrees with an independently constructed timestamp.
	for _, text := range []string{"2026-10-12 16:00:00", "2026-10-12 16:00", "2026-10-12T16:00", "2026-10-12"} {
		layout := "2006-01-02 15:04:05"
		if strings.Contains(text, "T") {
			layout = "2006-01-02T15:04"
		} else if len(text) == 16 {
			layout = "2006-01-02 15:04"
		} else if len(text) == 10 {
			layout = "2006-01-02"
		}
		at, err := time.Parse(layout, text)
		if err != nil {
			t.Fatal(err)
		}
		expected, err := upgradeauthority.Derive(guard.SID, creation.Owner, at.UnixNano(), authoritySalt())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`UPDATE sessions SET created_at=? WHERE sid=?`, text, guard.SID); err != nil {
			t.Fatal(err)
		}
		if _, err := upgradeauthority.ReadCreation(db, upgradeauthority.Guard{SID: guard.SID, Incarnation: expected}); err != nil {
			t.Fatal(err)
		}
	}
}

// origin: hot-update S8 plan §6.2 / P08
func TestCreationPredicateRejectsEveryRecreatedIdentity(t *testing.T) {
	for _, field := range []string{"sid", "owner", "timestamp", "salt", "hash encoding", "timestamp encoding"} {
		t.Run(field, func(t *testing.T) {
			db, original, guard := authorityDatabase(t)
			creation, err := upgradeauthority.ReadCreation(db, guard)
			if err != nil {
				t.Fatal(err)
			}
			predicate, args, err := upgradeauthority.CreationPredicate(creation)
			if err != nil {
				t.Fatal(err)
			}
			assertPredicateEffect(t, db, guard.SID, predicate, args, 1)
			if _, err := db.Exec(`DELETE FROM sessions WHERE sid=?`, guard.SID); err != nil {
				t.Fatal(err)
			}
			assertPredicateEffect(t, db, guard.SID, predicate, args, 0)
			recreated := original
			switch field {
			case "sid":
				recreated.SID = "new-lab"
			case "owner":
				recreated.Owner = "new-owner"
			case "timestamp":
				recreated.CreatedAtText = authorityInstant().Add(time.Nanosecond).String()
			case "salt":
				recreated.EncodedPINHash = authorityPHC(bytes.Repeat([]byte{22}, 16), 1)
			case "hash encoding":
				recreated.EncodedPINHash = authorityPHC(authoritySalt(), 2)
			case "timestamp encoding":
				recreated.CreatedAtText = authorityInstant().Format(time.RFC3339Nano)
			default:
				t.Fatal("unhandled creation identity field")
			}
			insertAuthorityCreation(t, db, recreated)
			assertPredicateEffect(t, db, recreated.SID, predicate, args, 0)
			fresh, err := upgradeauthority.ReadCreation(db, upgradeauthority.Guard{SID: recreated.SID})
			if err != nil {
				t.Fatal(err)
			}
			predicate, args, err = upgradeauthority.CreationPredicate(fresh)
			if err != nil {
				t.Fatal(err)
			}
			assertPredicateEffect(t, db, recreated.SID, predicate, args, 1)
		})
	}
}

// origin: hot-update S8 plan §6.2 / P08
func TestCreationPredicateKeepsMaterialInSQLBindings(t *testing.T) {
	db, original, guard := authorityDatabase(t)
	original.Owner = "owner' OR 1=1 --"
	if _, err := db.Exec(`UPDATE sessions SET owner_pubkey_fp=? WHERE sid=?`, original.Owner, guard.SID); err != nil {
		t.Fatal(err)
	}
	creation, err := upgradeauthority.ReadCreation(db, upgradeauthority.Guard{SID: guard.SID})
	if err != nil || creation != original {
		t.Fatal("parameterized identity lookup changed quoted material")
	}
	predicate, args, err := upgradeauthority.CreationPredicate(creation)
	if err != nil {
		t.Fatal(err)
	}
	if len(args) != 4 || strings.Count(predicate, "?") != 4 {
		t.Fatal("creation predicate lost its four SQL bindings")
	}
	for i, want := range []string{creation.SID, creation.Owner, creation.CreatedAtText, creation.EncodedPINHash} {
		if args[i] != want || strings.Contains(predicate, want) {
			t.Fatal("creation material was interpolated or bound to the wrong column")
		}
	}
	assertPredicateEffect(t, db, guard.SID, predicate, args, 1)
	// The leader adapter uses the existing audited literal renderer. This
	// exercises actual SQLite with its output, including an injection-shaped
	// owner; no new placeholder format is needed by the replicated command.
	parts := strings.Split(predicate, "?")
	var baked strings.Builder
	for i, arg := range args {
		baked.WriteString(parts[i])
		literal, err := clusterstate.LitText(arg.(string))
		if err != nil {
			t.Fatal("valid creation material failed the existing literal renderer")
		}
		baked.WriteString(literal)
	}
	baked.WriteString(parts[len(parts)-1])
	assertPredicateEffect(t, db, guard.SID, baked.String(), nil, 1)
	if _, err := db.Exec(`UPDATE sessions SET owner_pubkey_fp='replacement' WHERE sid=?`, guard.SID); err != nil {
		t.Fatal(err)
	}
	assertPredicateEffect(t, db, guard.SID, baked.String(), nil, 0)
}

// origin: hot-update S8 plan §6.2 / P08
func TestCreationReadersRejectInvalidMaterialWithoutDisclosure(t *testing.T) {
	base := upgradeauthority.Creation{"lab", "owner", authorityInstant().String(), authorityPHC(authoritySalt(), 1)}
	for _, test := range []struct {
		name  string
		alter func(*upgradeauthority.Creation)
	}{
		{"empty owner", func(c *upgradeauthority.Creation) { c.Owner = "" }},
		{"missing PHC", func(c *upgradeauthority.Creation) { c.EncodedPINHash = "private-material" }},
		{"bad PHC prefix", func(c *upgradeauthority.Creation) { c.EncodedPINHash = "private-material" + c.EncodedPINHash }},
		{"wrong algorithm", func(c *upgradeauthority.Creation) {
			c.EncodedPINHash = strings.Replace(c.EncodedPINHash, "argon2id", "argon2i", 1)
		}},
		{"wrong version", func(c *upgradeauthority.Creation) {
			c.EncodedPINHash = strings.Replace(c.EncodedPINHash, "v=19", "v=20", 1)
		}},
		{"wrong memory", func(c *upgradeauthority.Creation) {
			c.EncodedPINHash = strings.Replace(c.EncodedPINHash, "m=65536", "m=65537", 1)
		}},
		{"wrong iterations", func(c *upgradeauthority.Creation) {
			c.EncodedPINHash = strings.Replace(c.EncodedPINHash, "t=3", "t=4", 1)
		}},
		{"wrong parallelism", func(c *upgradeauthority.Creation) {
			c.EncodedPINHash = strings.Replace(c.EncodedPINHash, "p=2", "p=3", 1)
		}},
		{"trailing parameters", func(c *upgradeauthority.Creation) {
			c.EncodedPINHash = strings.Replace(c.EncodedPINHash, "p=2", "p=2garbage", 1)
		}},
		{"missing salt", func(c *upgradeauthority.Creation) { c.EncodedPINHash = authorityPHC(nil, 1) }},
		{"truncated salt", func(c *upgradeauthority.Creation) { c.EncodedPINHash = authorityPHC(authoritySalt()[:15], 1) }},
		{"invalid base64", func(c *upgradeauthority.Creation) {
			c.EncodedPINHash = strings.Replace(c.EncodedPINHash, base64.RawStdEncoding.EncodeToString(authoritySalt()), "private-material!", 1)
		}},
		{"truncated hash", func(c *upgradeauthority.Creation) { c.EncodedPINHash = c.EncodedPINHash[:len(c.EncodedPINHash)-2] }},
		{"ignored newline", func(c *upgradeauthority.Creation) { c.EncodedPINHash += "\n" }},
		{"missing timestamp", func(c *upgradeauthority.Creation) { c.CreatedAtText = "" }},
		{"invalid timestamp", func(c *upgradeauthority.Creation) { c.CreatedAtText = "private-material" }},
		{"zero timestamp", func(c *upgradeauthority.Creation) { c.CreatedAtText = "1970-01-01T00:00:00Z" }},
		{"wrapping timestamp", func(c *upgradeauthority.Creation) { c.CreatedAtText = "2500-01-01T00:00:00Z" }},
		{"positive wrapping timestamp", func(c *upgradeauthority.Creation) { c.CreatedAtText = "2600-01-01T00:00:00Z" }},
		{"invalid monotonic suffix", func(c *upgradeauthority.Creation) { c.CreatedAtText += " m=private-material" }},
		{"nonfinite monotonic suffix", func(c *upgradeauthority.Creation) { c.CreatedAtText += " m=+Inf" }},
		{"nondecimal monotonic suffix", func(c *upgradeauthority.Creation) { c.CreatedAtText += " m=+1e3.123456789" }},
		{"short monotonic suffix", func(c *upgradeauthority.Creation) { c.CreatedAtText += " m=+0.123" }},
		{"conflicting numeric zone", func(c *upgradeauthority.Creation) { c.CreatedAtText = "2026-10-12 21:30:00.123456789 +0530 +0545" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			creation := base
			test.alter(&creation)
			predicate, args, err := upgradeauthority.CreationPredicate(creation)
			if predicate != "" || args != nil {
				t.Fatal("invalid creation produced a mutation predicate")
			}
			assertAuthorityError(t, err, upgradetx.CodeDeferred, "private-material", creation.EncodedPINHash)
			db, _, _ := authorityDatabase(t)
			if _, err := db.Exec(`UPDATE sessions SET owner_pubkey_fp=?, created_at=?, pin_hash=? WHERE sid=?`, creation.Owner, creation.CreatedAtText, creation.EncodedPINHash, creation.SID); err != nil {
				t.Fatal(err)
			}
			if got, err := upgradeauthority.ReadCreation(db, upgradeauthority.Guard{SID: creation.SID}); got != (upgradeauthority.Creation{}) {
				t.Fatal("invalid creation returned local comparison material")
			} else {
				assertAuthorityError(t, err, upgradetx.CodeDeferred, "private-material", creation.EncodedPINHash)
			}
		})
	}
}

// origin: hot-update S8 plan §6.2 / P08
func TestReadCreationRejectsForeignRowsAndSanitizesSQLErrors(t *testing.T) {
	db, creation, guard := authorityDatabase(t)
	creation.SID = "other"
	insertAuthorityCreation(t, db, creation)
	reader := authorityReader{db: db, query: `SELECT sid, owner_pubkey_fp, CAST(created_at AS TEXT), pin_hash FROM sessions WHERE sid='other'`}
	if got, err := upgradeauthority.ReadCreation(reader, upgradeauthority.Guard{SID: guard.SID}); got != (upgradeauthority.Creation{}) {
		t.Fatal("foreign row returned comparison material")
	} else {
		assertAuthorityError(t, err, upgradetx.CodeSessionIncarnationMismatch)
	}
	reader.query = `SELECT sid, owner_pubkey_fp, CAST(created_at AS TEXT), NULL FROM sessions WHERE sid='lab'`
	if got, err := upgradeauthority.ReadCreation(reader, guard); got != (upgradeauthority.Creation{}) {
		t.Fatal("partial Scan failure returned comparison material")
	} else {
		assertAuthorityError(t, err, upgradetx.CodeDeferred)
	}
	reader.query = `SELECT private_material_sql_error`
	row := reader.QueryRow("")
	var ignored string
	if err := row.Scan(&ignored); err == nil || !strings.Contains(err.Error(), "private_material_sql_error") {
		t.Fatal("SQL error fixture did not exercise sensitive driver diagnostics")
	}
	if got, err := upgradeauthority.ReadCreation(reader, guard); got != (upgradeauthority.Creation{}) {
		t.Fatal("SQL failure returned material")
	} else {
		assertAuthorityError(t, err, upgradetx.CodeDeferred, "private_material_sql_error")
	}
	queries := 0
	reader.query = `SELECT sid, owner_pubkey_fp, CAST(created_at AS TEXT), pin_hash FROM sessions WHERE sid='lab'`
	reader.queries = &queries
	for _, badGuard := range []upgradeauthority.Guard{{}, {SID: "lab", Incarnation: "invalid"}} {
		if _, err := upgradeauthority.ReadCreation(reader, badGuard); err == nil {
			t.Fatal("invalid guard reached an authorized creation")
		}
	}
	if queries != 0 {
		t.Fatal("invalid guard reached the SQL reader")
	}
	if _, err := upgradeauthority.ReadCreation(nil, guard); err == nil {
		t.Fatal("nil reader produced a proof")
	}
	if _, err := upgradeauthority.ReadCreation(authorityReader{}, guard); err == nil {
		t.Fatal("nil row produced a proof")
	}
}

// The leaf starts no loops or processes. Its SQL test fixtures do own database
// readers/FDs, so five on-disk transaction cycles directly check their cleanup.
// origin: hot-update S8 plan §11.2 / P08
func TestCreationSQLReadersReleaseResources(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseCreationSQL(t)
	}
	testharness.AssertNoGoroutineLeak(t, "authority SQL readers", beforeG)
	testharness.AssertNoFDLeak(t, "authority SQL readers", beforeFD, 0)
}

func exerciseCreationSQL(t *testing.T) {
	t.Helper()
	db, err := storage.Open(filepath.Join(t.TempDir(), "authority.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	}()
	creation := upgradeauthority.Creation{"lab", "owner", authorityInstant().String(), authorityPHC(authoritySalt(), 1)}
	insertAuthorityCreation(t, db, creation)
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	got, err := upgradeauthority.ReadCreation(tx, upgradeauthority.Guard{SID: creation.SID})
	if err != nil {
		t.Fatal(err)
	}
	predicate, args, err := upgradeauthority.CreationPredicate(got)
	if err != nil {
		t.Fatal(err)
	}
	var known int
	if err := tx.QueryRow("SELECT "+predicate, args...).Scan(&known); err != nil || known != 1 {
		t.Fatal("transaction lost the read creation's identity")
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := upgradeauthority.ReadCreation(db, upgradeauthority.Guard{SID: creation.SID, Incarnation: strings.Repeat("f", 64)}); err == nil {
		t.Fatal("mismatch failed to exercise refusal cleanup")
	}
}

func authorityDatabase(t *testing.T) (*sql.DB, upgradeauthority.Creation, upgradeauthority.Guard) {
	t.Helper()
	db := testharness.OpenDB(t)
	creation := upgradeauthority.Creation{"lab", "owner", authorityInstant().String(), authorityPHC(authoritySalt(), 1)}
	insertAuthorityCreation(t, db, creation)
	expected, err := upgradeauthority.Derive(creation.SID, creation.Owner, authorityInstant().UnixNano(), authoritySalt())
	if err != nil {
		t.Fatal(err)
	}
	return db, creation, upgradeauthority.Guard{SID: creation.SID, Incarnation: expected, InstanceID: "aaaaaaaaaaaaaaaaaaaaaaaaaa", ConfiguredNID: "lab-1"}
}

func insertAuthorityCreation(t *testing.T, db *sql.DB, creation upgradeauthority.Creation) {
	t.Helper()
	if _, err := db.Exec(`INSERT INTO sessions(sid, name, owner_pubkey_fp, created_at, pin_hash) VALUES (?, 'unchanged', ?, ?, ?)`,
		creation.SID, creation.Owner, creation.CreatedAtText, creation.EncodedPINHash); err != nil {
		t.Fatal("could not seed session creation")
	}
}

func assertPredicateEffect(t *testing.T, db *sql.DB, sid, predicate string, args []any, want int64) {
	t.Helper()
	// Deliberately different alias from the predicate: unqualified sid inside
	// EXISTS must never capture the outer row and authorize another session.
	res, err := db.Exec(`UPDATE sessions AS target SET name='changed' WHERE target.sid=? AND `+predicate, append([]any{sid}, args...)...)
	if err != nil {
		t.Fatal("creation predicate failed in the actual mutation statement")
	}
	changed, err := res.RowsAffected()
	if err != nil || changed != want {
		t.Fatalf("creation predicate affected %d rows, want %d", changed, want)
	}
}

func authoritySalt() []byte { return bytes.Repeat([]byte{17}, 16) }

func authorityInstant() time.Time {
	return time.Date(2026, time.October, 12, 16, 0, 0, 123456789, time.UTC)
}

func authorityPHC(salt []byte, hashByte byte) string {
	return "$argon2id$v=19$m=65536,t=3,p=2$" + base64.RawStdEncoding.EncodeToString(salt) + "$" +
		base64.RawStdEncoding.EncodeToString(bytes.Repeat([]byte{hashByte}, 32))
}

// The negative port still returns real SQLite rows: it can demonstrate a wrong
// SID, sensitive driver diagnostics, or a missing row without a fake authority.
type authorityReader struct {
	db      *sql.DB
	query   string
	queries *int
}

func (r authorityReader) QueryRow(string, ...any) *sql.Row {
	if r.queries != nil {
		*r.queries++
	}
	if r.db == nil {
		return nil
	}
	return r.db.QueryRow(r.query)
}
