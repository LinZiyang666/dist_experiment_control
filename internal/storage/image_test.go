// These tests use real SQLite files, pools, checkpoints and online backups.
// Only the optional registry input is blinded to prove it cannot replace the
// actual census; same-PID exec and Raft replay remain P04/P26 integration.
package storage_test

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"syscall"
	"testing"

	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/testharness"
	sqlite "modernc.org/sqlite"
)

func imageStore(t *testing.T, wal bool) (*sql.DB, storage.StoreStamp) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "state.db")
	opener := storage.Open
	if wal {
		opener = storage.OpenWAL
	}
	db, err := opener(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	id, err := storage.CanonicalStoreIdentity(path, filepath.Dir(path))
	if err != nil {
		t.Fatal(err)
	}
	stamp, err := storage.ReadStoreStamp(db, id)
	if err != nil {
		t.Fatal(err)
	}
	return db, stamp
}

func imageConfig(stamp storage.StoreStamp) storage.OpenImageConfig {
	return storage.OpenImageConfig{Stamp: stamp, Registry: storage.DefaultDriverRegistry()}
}

func requireCounts(t *testing.T, id storage.DBIdentity, writers, readers uint32) {
	t.Helper()
	w, r := storage.DefaultDriverRegistry().Connections(id)
	if w != writers || r != readers {
		t.Fatalf("driver census = (%d,%d), want (%d,%d)", w, r, writers, readers)
	}
}

func alterImageStore(t *testing.T, path, statement string) {
	t.Helper()
	// A raw registered pool deliberately avoids the cold migration runner: it
	// cannot repair the defect that the current opener is supposed to reject.
	db, err := storage.OpenExisting(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	}()
	if statement == "unmigrated" {
		rows, err := db.Query("SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name!='schema_migrations'")
		if err != nil {
			t.Fatal(err)
		}
		var tables []string
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				t.Fatal(err)
			}
			tables = append(tables, name)
		}
		if err := errors.Join(rows.Err(), rows.Close()); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec("PRAGMA foreign_keys=OFF"); err != nil {
			t.Fatal(err)
		}
		for _, name := range tables {
			quoted := strings.ReplaceAll(name, "\"", "\"\"")
			if _, err := db.Exec("DROP TABLE \"" + quoted + "\""); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := db.Exec("DELETE FROM schema_migrations"); err != nil {
			t.Fatal(err)
		}
		return
	}
	if _, err := db.Exec(statement); err != nil {
		t.Fatal(err)
	}
}

// origin: hot-update S6 plan §3.1 / T05
func TestBrokerCanonicalStoreLock(t *testing.T) {
	db, stamp := imageStore(t, false)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	path := stamp.Identity.Path
	for _, dsn := range []string{path, "file:" + path, (&url.URL{Scheme: "file", Path: path}).String(), "file:" + path + "?mode=rw"} {
		id, err := storage.CanonicalStoreIdentity(dsn, stamp.Identity.DataDir)
		if err != nil || id != stamp.Identity {
			t.Fatalf("binding %q = %+v, %v", dsn, id, err)
		}
	}
	if stamp.Identity.LockPath != filepath.Join(filepath.Dir(path), "tether.lock") {
		t.Fatal("wrong lock domain")
	}
	for _, dsn := range []string{":memory:", "file::memory:", "file:" + path + "?mode=memory", "file:" + path + "?cache=shared", "file:" + path + "?_pragma=synchronous(NORMAL)", "file://remote" + path, "file:" + path + "?mode=rw&mode=ro", "file:" + path + "?bad=%xx", path + "?mode=rw"} {
		if _, err := storage.CanonicalStoreIdentity(dsn, ""); err == nil {
			t.Fatalf("unsafe URI accepted: %q", dsn)
		}
	}
	if _, err := storage.CanonicalStoreIdentity(path, t.TempDir()); err == nil {
		t.Fatal("independent DataDir accepted")
	}
	if err := os.Symlink(path, path+".symlink"); err != nil {
		t.Fatal(err)
	}
	if _, err := storage.CanonicalStoreIdentity(path+".symlink", ""); err == nil {
		t.Fatal("symlink alias accepted")
	}
	parentAlias := filepath.Join(t.TempDir(), "parent")
	if err := os.Symlink(filepath.Dir(path), parentAlias); err != nil {
		t.Fatal(err)
	}
	if _, err := storage.CanonicalStoreIdentity(filepath.Join(parentAlias, "state.db"), ""); err == nil {
		t.Fatal("parent symlink alias accepted")
	}
	if err := os.Link(path, path+".hardlink"); err != nil {
		t.Fatal(err)
	}
	for _, alias := range []string{path, path + ".hardlink"} {
		if _, err := storage.CanonicalStoreIdentity(alias, ""); err == nil {
			t.Fatal("hardlink alias accepted")
		}
	}
	if err := os.Remove(path + ".hardlink"); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(path, stamp.Identity.LockPath); err != nil {
		t.Fatal(err)
	}
	if _, err := storage.CanonicalStoreIdentity(path, ""); err == nil {
		t.Fatal("symlink lock accepted")
	}
	if err := os.Remove(stamp.Identity.LockPath); err != nil {
		t.Fatal(err)
	}
	lockPath := stamp.Identity.LockPath
	if err := os.WriteFile(lockPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(lockPath, lockPath+".alias"); err != nil {
		t.Fatal(err)
	}
	if _, err := storage.CanonicalStoreIdentity(path, ""); err == nil {
		t.Fatal("hardlink lock accepted")
	}
	fresh := filepath.Join(t.TempDir(), "absent.db")
	id, err := storage.CanonicalStoreIdentity(fresh, "")
	if err != nil || id.Inode != 0 {
		t.Fatalf("cold binding: %+v, %v", id, err)
	}
	if _, err := os.Stat(fresh); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("binding created the DB")
	}
	memory, err := storage.Open(":memory:")
	if err != nil {
		t.Fatal("embedding memory opener was restricted", err)
	}
	if err := memory.Close(); err != nil {
		t.Fatal(err)
	}
}

// origin: hot-update S6 plan §3.3 / T07
func TestOpenCurrentExactSchema(t *testing.T) {
	for _, tc := range []struct {
		name, sql string
		change    func(*storage.StoreStamp)
	}{
		{name: "unmigrated_current", sql: "unmigrated"},
		{name: "missing_early", sql: "DELETE FROM schema_migrations WHERE version='0001_init.sql'"},
		{name: "unknown_name", sql: "INSERT INTO schema_migrations(version) VALUES('9999_unknown.sql')"},
		{name: "same_count_wrong_name", sql: "UPDATE schema_migrations SET version='0000_unknown.sql' WHERE version='0001_init.sql'"},
		{name: "tracker_intact_table_deleted", sql: "DROP TABLE alert_acks"},
		{name: "same_set_different_content", change: func(s *storage.StoreStamp) { s.Schema.Supported[0].SHA256 = strings.Repeat("a", 64) }},
		{name: "applied_content", change: func(s *storage.StoreStamp) { s.Schema.Applied[0].SHA256 = strings.Repeat("b", 64) }},
		{name: "null_applied", change: func(s *storage.StoreStamp) { s.Schema.Applied = nil }},
		{name: "null_supported", change: func(s *storage.StoreStamp) { s.Schema.Supported = nil }},
		{name: "empty_applied", change: func(s *storage.StoreStamp) { s.Schema.Applied = []storage.MigrationDigest{} }},
		{name: "schema_digest", change: func(s *storage.StoreStamp) { s.Schema.Digest = strings.Repeat("c", 64) }},
		{name: "object_digest", change: func(s *storage.StoreStamp) { s.Schema.ObjectDigest = strings.Repeat("d", 64) }},
		{name: "unordered", change: func(s *storage.StoreStamp) {
			s.Schema.Applied[0], s.Schema.Applied[1] = s.Schema.Applied[1], s.Schema.Applied[0]
		}},
		{name: "wrong_inode", change: func(s *storage.StoreStamp) { s.Identity.Inode++ }},
		{name: "wrong_lock", change: func(s *storage.StoreStamp) { s.Identity.LockPath += ".other" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, stamp := imageStore(t, false)
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			if tc.sql != "" {
				alterImageStore(t, stamp.Identity.Path, tc.sql)
			}
			if tc.change != nil {
				tc.change(&stamp)
			}
			switch tc.name {
			case "same_set_different_content", "applied_content", "null_applied", "null_supported", "empty_applied", "schema_digest", "unordered":
				if err := storage.ValidateSchemaStamp(stamp.Schema); err == nil {
					t.Fatal("pure schema predicate accepted drift")
				}
			}

			before, err := os.ReadFile(stamp.Identity.Path)
			if err != nil {
				t.Fatal(err)
			}
			current, err := storage.OpenCurrent(imageConfig(stamp))
			if current != nil {
				_ = current.Close()
			}
			if err == nil || current != nil {
				t.Fatal("invalid current schema was accepted")
			}
			after, err := os.ReadFile(stamp.Identity.Path)
			if err != nil || sha256.Sum256(before) != sha256.Sum256(after) {
				t.Fatal("rejected open mutated the current DB", err)
			}
			requireCounts(t, stamp.Identity, 0, 0)
		})
	}
	t.Run("current_rows_before_replay", func(t *testing.T) {
		db, stamp := imageStore(t, false)
		if _, err := db.Exec("INSERT INTO cluster_meta(key,value) VALUES('ack-witness','current-bytes'),('applied_index','17'),('applied_term','3')"); err != nil {
			t.Fatal(err)
		}
		stamp, err := storage.ReadStoreStamp(db, stamp.Identity)
		if err != nil {
			t.Fatal(err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		current, err := storage.OpenCurrent(imageConfig(stamp))
		if err != nil {
			t.Fatal(err)
		}
		var witness string
		if err := current.QueryRow("SELECT value FROM cluster_meta WHERE key='ack-witness'").Scan(&witness); err != nil || witness != "current-bytes" {
			t.Fatal("current ACK missing before replay", witness, err)
		}
		if err := current.Close(); err != nil {
			t.Fatal(err)
		}
		alterImageStore(t, stamp.Identity.Path, "UPDATE cluster_meta SET value='16' WHERE key='applied_index'")
		current, err = storage.OpenCurrent(imageConfig(stamp))
		if current != nil {
			_ = current.Close()
		}
		if err == nil {
			t.Fatal("durable floor regression accepted")
		}
	})
	t.Run("missing_file", func(t *testing.T) {
		db, stamp := imageStore(t, false)
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(stamp.Identity.Path); err != nil {
			t.Fatal(err)
		}
		if current, err := storage.OpenCurrent(imageConfig(stamp)); err == nil {
			_ = current.Close()
			t.Fatal("missing DB recreated")
		}
		if _, err := os.Stat(stamp.Identity.Path); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("missing DB was created")
		}
	})
}

type blindDriverRegistry struct{}

func (blindDriverRegistry) Connections(storage.DBIdentity) (uint32, uint32) { return 0, 0 }
func (blindDriverRegistry) AssertExclusive(storage.DBIdentity) error        { return nil }

// origin: hot-update S6 plan §3.2 / T08
func TestBrokerWriterRegistryExclusive(t *testing.T) {
	t.Run("raw_database_fixture_interfaces", func(t *testing.T) {
		source, err := storage.Open(":memory:")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = source.Close() }()
		if _, err := source.Exec("INSERT INTO cluster_meta(key,value) VALUES('fixture','serialized-bytes')"); err != nil {
			t.Fatal(err)
		}
		conn, err := source.Conn(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		var data []byte
		err = conn.Raw(func(raw any) error {
			serializer, ok := raw.(interface{ Serialize() ([]byte, error) })
			if !ok {
				t.Fatal("database/sql Raw serialization interface lost")
			}
			var err error
			data, err = serializer.Serialize()
			return err
		})
		if err := errors.Join(err, conn.Close()); err != nil {
			t.Fatal(err)
		}
		if len(data) == 0 {
			t.Fatal("empty serialized database")
		}
		destination, err := storage.Open(":memory:")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = destination.Close() }()
		conn, err = destination.Conn(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		err = conn.Raw(func(raw any) error {
			deserializer, ok := raw.(interface{ Deserialize([]byte) error })
			if !ok {
				t.Fatal("database/sql Raw deserialization interface lost")
			}
			return deserializer.Deserialize(data)
		})
		if err := errors.Join(err, conn.Close()); err != nil {
			t.Fatal(err)
		}
		var value string
		if err := destination.QueryRow("SELECT value FROM cluster_meta WHERE key='fixture'").Scan(&value); err != nil || value != "serialized-bytes" {
			t.Fatal("raw fixture round-trip lost bytes", value, err)
		}
	})

	db, stamp := imageStore(t, false)
	requireCounts(t, stamp.Identity, 1, 0)
	second, err := storage.Open(stamp.Identity.Path)
	if err != nil {
		t.Fatal(err)
	}
	defer func(pool *sql.DB) { _ = pool.Close() }(second)
	if db.Stats().MaxOpenConnections != 1 || second.Stats().MaxOpenConnections != 1 {
		t.Fatal("pool control absent")
	}
	requireCounts(t, stamp.Identity, 2, 0)
	if err := storage.DefaultDriverRegistry().AssertExclusive(stamp.Identity); err == nil {
		t.Fatal("second pool evaded registry")
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	requireCounts(t, stamp.Identity, 1, 0)
	alias := stamp.Identity.Path + ".hardlink"
	if err := os.Link(stamp.Identity.Path, alias); err != nil {
		t.Fatal(err)
	}
	second, err = storage.Open(alias)
	if err != nil {
		t.Fatal(err)
	}
	requireCounts(t, stamp.Identity, 2, 0)
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(alias); err != nil {
		t.Fatal(err)
	}
	// A sql.OpenDB pool owns no connection until Ping; the registry must count
	// the real lazy reader once, and remove it only after actual driver Close.
	lazy := sql.OpenDB(driverConnector{driver: db.Driver(), dsn: currentDSN(stamp.Identity.Path, "ro")})
	defer func() { _ = lazy.Close() }()
	requireCounts(t, stamp.Identity, 1, 0)
	if err := lazy.Ping(); err != nil {
		t.Fatal(err)
	}
	requireCounts(t, stamp.Identity, 1, 1)
	declared, err := storage.OpenReadOnly("file:" + stamp.Identity.Path + "?mode=rw")
	if err != nil {
		t.Fatal(err)
	}
	requireCounts(t, stamp.Identity, 1, 2)
	if _, err := declared.Exec("INSERT INTO cluster_meta(key,value) VALUES('forbidden','writer')"); err == nil {
		t.Fatal("declared reader became a writer")
	}
	if err := declared.Close(); err != nil {
		t.Fatal(err)
	}
	cfg := imageConfig(stamp)
	cfg.Registry = blindDriverRegistry{}
	if current, err := storage.OpenCurrent(cfg); err == nil {
		_ = current.Close()
		t.Fatal("injected registry bypassed actual connections")
	}
	if err := lazy.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	requireCounts(t, stamp.Identity, 0, 0)
	current, err := storage.OpenCurrent(imageConfig(stamp))
	if err != nil {
		t.Fatal(err)
	}
	requireCounts(t, stamp.Identity, 1, 0)
	exclusiveDriver := current.Driver()
	if err := current.Close(); err != nil {
		t.Fatal(err)
	}
	requireCounts(t, stamp.Identity, 0, 0)
	// A constructor cannot momentarily open an uncounted second writer.
	live, err := storage.Open(stamp.Identity.Path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = live.Close() }()
	connector := driverConnector{dsn: currentDSN(stamp.Identity.Path, "rw"), driver: exclusiveDriver}
	if raw, err := connector.Connect(t.Context()); err == nil {
		_ = raw.Close()
		t.Fatal("exclusive connector allowed existing owner")
	}
}

// origin: hot-update S6 plan §3.2 / T11
func TestBrokerJournalPreserved(t *testing.T) {
	for _, wal := range []bool{false, true} {
		name := "delete"
		if wal {
			name = "wal"
		}
		t.Run(name, func(t *testing.T) {
			db, stamp := imageStore(t, wal)
			if stamp.Journal != name || stamp.Synchronous != 2 {
				t.Fatal("incorrect durability", stamp.Journal, stamp.Synchronous)
			}
			busy, log, done, err := storage.CheckpointForImage(db)
			if err != nil {
				t.Fatal(err)
			}
			want := -1
			if wal {
				want = 0
			}
			if busy != want || log != want || done != want {
				t.Fatalf("checkpoint = %d,%d,%d", busy, log, done)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			current, err := storage.OpenCurrent(imageConfig(stamp))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = current.Close() }()
			actual, err := storage.ReadStoreStamp(current, stamp.Identity)
			if err != nil || actual.Journal != name || actual.Synchronous != 2 {
				t.Fatal("journal changed across current open", actual.Journal, actual.Synchronous, err)
			}
			tx, err := current.Begin()
			if err != nil {
				t.Fatal(err)
			}
			if _, _, _, err := storage.CheckpointForImage(current); err == nil {
				t.Fatal("active transaction checkpoint accepted")
			}
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			if _, err := current.Exec("PRAGMA synchronous=NORMAL"); err != nil {
				t.Fatal(err)
			}
			if _, err := storage.ReadStoreStamp(current, stamp.Identity); err == nil {
				t.Fatal("effective NORMAL accepted")
			}
			if _, _, _, err := storage.CheckpointForImage(current); err == nil {
				t.Fatal("NORMAL checkpoint accepted")
			}
		})
	}
	t.Run("busy_checkpoint", func(t *testing.T) {
		db, stamp := imageStore(t, true)
		if _, err := db.Exec("INSERT INTO cluster_meta(key,value) VALUES('pinned','old')"); err != nil {
			t.Fatal(err)
		}
		ro, err := storage.OpenReadOnly(stamp.Identity.Path)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = ro.Close() }()
		tx, err := ro.Begin()
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback() }()
		var old string
		if err := tx.QueryRow("SELECT value FROM cluster_meta WHERE key='pinned'").Scan(&old); err != nil || old != "old" {
			t.Fatal(err)
		}
		if _, err := db.Exec("UPDATE cluster_meta SET value='new' WHERE key='pinned'"); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec("PRAGMA busy_timeout=0"); err != nil {
			t.Fatal(err)
		}
		busy, _, _, err := storage.CheckpointForImage(db)
		if busy != 1 || err == nil {
			t.Fatalf("busy checkpoint accepted: busy=%d err=%v", busy, err)
		}
		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
		if err := ro.Close(); err != nil {
			t.Fatal(err)
		}
		if busy, log, done, err := storage.CheckpointForImage(db); err != nil || busy != 0 || log != 0 || done != 0 {
			t.Fatalf("joined reader checkpoint: %d,%d,%d %v", busy, log, done, err)
		}
	})
	t.Run("wrong_journal", func(t *testing.T) {
		db, stamp := imageStore(t, false)
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		alterImageStore(t, stamp.Identity.Path, "PRAGMA journal_mode=WAL")
		if current, err := storage.OpenCurrent(imageConfig(stamp)); err == nil {
			_ = current.Close()
			t.Fatal("changed journal accepted")
		}
	})
}

// origin: hot-update S6 plan §12.1 P03 / T55 storage prerequisite
func TestBrokerStorageLeakCycles(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	beforeIdentity := storageFDIdentities(t)
	for round := 0; round < 5; round++ {
		exerciseStorageCloseFailure(t)
	}
	testharness.AssertNoGoroutineLeak(t, "storage current-open failures", beforeG)
	// No sockets are used: every driver and online-backup descriptor must close.
	testharness.AssertNoFDLeak(t, "storage current-open failures", beforeFD, 0)
	if after := storageFDIdentities(t); !reflect.DeepEqual(beforeIdentity, after) {
		t.Fatalf("FD identity multiset changed: before=%v after=%v", beforeIdentity, after)
	}
}

func exerciseStorageCloseFailure(t *testing.T) {
	t.Helper()
	db, stamp := imageStore(t, true)
	ro, err := storage.OpenReadOnly(stamp.Identity.Path)
	if err != nil {
		t.Fatal(err)
	}
	requireCounts(t, stamp.Identity, 1, 1)
	// Exercise the real modernc Raw interface, not an inert fake backup owner.
	conn, err := ro.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	backupPath := filepath.Join(t.TempDir(), "backup.db")
	err = conn.Raw(func(raw any) error {
		backup, err := raw.(interface {
			NewBackup(string) (*sqlite.Backup, error)
		}).NewBackup(backupPath)
		if err != nil {
			return err
		}
		for {
			more, err := backup.Step(1)
			if err != nil {
				return errors.Join(err, backup.Finish())
			}
			if !more {
				break
			}
		}
		return backup.Finish()
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if err := ro.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	requireCounts(t, stamp.Identity, 0, 0)
	// This digest is syntactically valid, so rejection happens after opening
	// and actually reading the mode=ro connection, exercising partial cleanup.
	bad := stamp
	bad.Schema.ObjectDigest = strings.Repeat("e", 64)
	if current, err := storage.OpenCurrent(imageConfig(bad)); err == nil {
		_ = current.Close()
		t.Fatal("bad object stamp accepted")
	}
	requireCounts(t, stamp.Identity, 0, 0)
	current, err := storage.OpenCurrent(imageConfig(stamp))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := storage.CheckpointForImage(current); err != nil {
		t.Fatal(err)
	}
	if err := current.Close(); err != nil {
		t.Fatal(err)
	}
	requireCounts(t, stamp.Identity, 0, 0)
	// Both contenders start at one barrier. Only one may own a current
	// writer; the loser must close every partial validation connection.
	type openResult struct {
		db  *sql.DB
		err error
	}
	start := make(chan struct{})
	results := make(chan openResult, 2)
	for worker := 0; worker < 2; worker++ {
		go func() { <-start; db, err := storage.OpenCurrent(imageConfig(stamp)); results <- openResult{db, err} }()
	}
	close(start)
	var owners []*sql.DB
	for worker := 0; worker < 2; worker++ {
		result := <-results
		if result.err == nil {
			owners = append(owners, result.db)
		} else if result.db != nil {
			_ = result.db.Close()
			t.Error("failed current constructor returned owner")
		}
	}
	requireCounts(t, stamp.Identity, uint32(len(owners)), 0)
	for _, owner := range owners {
		if err := owner.Close(); err != nil {
			t.Error(err)
		}
	}
	if len(owners) != 1 {
		t.Fatalf("concurrent current writer owners = %d, want 1", len(owners))
	}
	requireCounts(t, stamp.Identity, 0, 0)
	contents, err := os.ReadFile(backupPath)
	if err != nil || len(contents) == 0 {
		t.Fatal("empty real backup", err)
	}
}

// Keep the ctx-free frozen function signature explicit in the owner package.
var _ storage.OpenCurrentFunc = storage.OpenCurrent

type driverConnector struct {
	driver driver.Driver
	dsn    string
}

func (c driverConnector) Driver() driver.Driver { return c.driver }
func (c driverConnector) Connect(ctx context.Context) (driver.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return c.driver.Open(c.dsn)
}
func currentDSN(path, mode string) string {
	u := url.URL{Scheme: "file", Path: path}
	u.RawQuery = (url.Values{"mode": {mode}, "_pragma": {"synchronous(FULL)"}}).Encode()
	return u.String()
}

// Counts cannot detect closing an old FD while leaking a replacement. Compare
// the inode/device multiset as well; a ReadDir's own transient FD is already
// closed when os.Stat runs, and is the only permitted missing entry.
func storageFDIdentities(t *testing.T) map[string]int {
	t.Helper()
	if runtime.GOOS != "linux" {
		return nil
	}
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	result := make(map[string]int)
	for _, entry := range entries {
		info, err := os.Stat(filepath.Join("/proc/self/fd", entry.Name()))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok {
			t.Fatal("FD stat identity unavailable")
		}
		result[fmt.Sprintf("%d:%d:%d", uint64(stat.Dev), uint64(stat.Ino), stat.Mode)]++
	}
	return result
}
