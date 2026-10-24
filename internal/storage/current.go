package storage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
)

// StoreLockFile is shared with the cluster lock owner without importing cluster
// into storage (cluster already imports storage). Both single and cluster stores
// must acquire this basename in the canonical database parent.
const StoreLockFile = "tether.lock"

// origin: hot-update S6 plan §3.1 / T05
// CanonicalStoreIdentity binds every production opener to the database parent's
// tether.lock. Absent files are allowed for cold/offline creation, but aliases
// are rejected before Acquire or SQLite can create anything in the wrong domain.
// Embedding tests keep storage.Open's in-memory support; this binding is their
// explicit image eligibility boundary.
func CanonicalStoreIdentity(dsn, dataDir string) (DBIdentity, error) {
	path, err := localDatabasePath(dsn)
	if err != nil {
		return DBIdentity{}, err
	}
	path, err = filepath.Abs(path)
	if err != nil {
		return DBIdentity{}, err
	}
	parent := filepath.Dir(path)
	if dataDir == "" {
		dataDir = parent
	}
	dir, err := filepath.Abs(dataDir)
	if err != nil {
		return DBIdentity{}, err
	}
	if dir != parent {
		return DBIdentity{}, errors.New("storage: database and DataDir do not share a lock domain")
	}
	if err := rejectPathAliases(path); err != nil {
		return DBIdentity{}, err
	}
	lock := filepath.Join(dir, StoreLockFile)
	if err := rejectPathAliases(lock); err != nil {
		return DBIdentity{}, err
	}
	id := DBIdentity{Path: path, DataDir: dir, LockPath: lock}
	info, err := os.Lstat(path)
	// A cold/offline destination has a lock domain before it has an inode. Hot
	// callers separately require a nonzero existing inode before any SQL open.
	if errors.Is(err, os.ErrNotExist) {
		return id, nil
	}
	if err != nil {
		return DBIdentity{}, err
	}
	if !info.Mode().IsRegular() {
		return DBIdentity{}, errors.New("storage: database is not a regular file")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return DBIdentity{}, errors.New("storage: database identity unavailable")
	}
	id.Device, id.Inode = uint64(stat.Dev), uint64(stat.Ino)
	return id, nil
}

func localDatabasePath(dsn string) (string, error) {
	if dsn == "" || strings.ContainsRune(dsn, 0) || dsn == ":memory:" {
		return "", errors.New("storage: image-ineligible database path")
	}
	if !strings.HasPrefix(dsn, "file:") {
		if strings.ContainsAny(dsn, "?#") {
			return "", errors.New("storage: database path contains URI options")
		}
		return dsn, nil
	}
	u, err := url.Parse(dsn)
	// No remote authorities, URI cache sharing or caller-supplied locking and
	// synchronous pragmas are part of the production store identity. The image
	// opener builds its own DSN after validation instead of forwarding options.
	if err != nil || u.User != nil || u.Host != "" || u.Fragment != "" {
		return "", errors.New("storage: database URI must name a local file")
	}
	query, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return "", errors.New("storage: invalid database URI options")
	}
	for key, values := range query {
		if key != "mode" || len(values) != 1 || (values[0] != "rw" && values[0] != "rwc" && values[0] != "ro") {
			return "", errors.New("storage: unsupported database URI options")
		}
	}
	path := u.Path
	if u.Opaque != "" {
		// file:relative.db uses Opaque; file:///absolute.db uses Path. Decode
		// once so percent-encoded filenames cannot select a second lock domain.
		path, err = url.PathUnescape(u.Opaque)
		if err != nil {
			return "", errors.New("storage: invalid database URI path")
		}
	}
	if path == "" || path == ":memory:" || strings.ContainsRune(path, 0) {
		return "", errors.New("storage: image-ineligible database URI")
	}
	return path, nil
}

func rejectPathAliases(path string) error {
	// Inspect every existing ancestor, not only the final basename. Resolving a
	// directory symlink would otherwise authorize a different spelling of the
	// same live DB while an offline tool locks an independently chosen path.
	for current := path; ; current = filepath.Dir(current) {
		info, err := os.Lstat(current)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if err == nil {
			if info.Mode()&os.ModeSymlink != 0 {
				return errors.New("storage: symbolic link store alias refused")
			}
			if info.Mode().IsRegular() {
				stat, ok := info.Sys().(*syscall.Stat_t)
				if !ok || stat.Nlink != 1 {
					return errors.New("storage: hard link store alias refused")
				}
			} else if !info.IsDir() || current == path {
				return errors.New("storage: invalid store or lock file")
			}
		}
		if current == filepath.Dir(current) {
			break
		}
	}
	return nil
}

// SupportedMigrations is pure metadata for candidate smoke/preflight. Full
// filenames and content hashes are compared, never a MAX(version) or count.
func SupportedMigrations() ([]MigrationDigest, error) {
	entries, err := migrationsFS.ReadDir("migrations")
	if err != nil {
		return nil, err
	}
	result := make([]MigrationDigest, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		body, err := migrationsFS.ReadFile("migrations/" + entry.Name())
		if err != nil {
			return nil, err
		}
		digest := sha256.Sum256(body)
		result = append(result, MigrationDigest{Name: entry.Name(), SHA256: hex.EncodeToString(digest[:])})
	}
	if len(result) == 0 {
		return nil, errors.New("storage: empty supported migration set")
	}
	return result, nil // embed.ReadDir returns filename order.
}

func schemaDigest(applied, supported []MigrationDigest) string {
	// The two labeled ordered lists are independently present in the receipt:
	// the installed schema and the reader's embedded support are different
	// claims even when a fully migrated S6 database makes them equal.
	body, _ := json.Marshal(struct{ Applied, Supported []MigrationDigest }{applied, supported})
	digest := sha256.Sum256(body)
	return hex.EncodeToString(digest[:])
}

// ValidateSchemaStamp is pure: a reader never probes a live store during smoke.
// origin: hot-update S6 plan §3.1 / T07
func ValidateSchemaStamp(stamp SchemaStamp) error {
	supported, err := SupportedMigrations()
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(stamp.Supported, supported) || !reflect.DeepEqual(stamp.Applied, supported) ||
		stamp.Digest != schemaDigest(stamp.Applied, stamp.Supported) || !validDigest(stamp.ObjectDigest) {
		return errors.New("storage: exact schema stamp mismatch")
	}
	return nil
}

func validDigest(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size && value == strings.ToLower(value)
}

type storeReader interface {
	Query(string, ...any) (*sql.Rows, error)
	QueryRow(string, ...any) *sql.Row
}

func readSchemaStamp(db storeReader) (SchemaStamp, error) {
	supported, err := SupportedMigrations()
	if err != nil {
		return SchemaStamp{}, err
	}
	rows, err := db.Query("SELECT version FROM schema_migrations ORDER BY version")
	if err != nil {
		return SchemaStamp{}, err
	}
	applied := make([]MigrationDigest, 0, len(supported))
	// The existing tracker persists filenames, not content hashes. Resolve
	// each exact name against the embedded body, then compare the exported
	// applied/support hashes with the importing reader in ValidateSchemaStamp.
	// No new persistent column or migration is introduced for instrumentation.
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return SchemaStamp{}, errors.Join(err, rows.Close())
		}
		if len(applied) >= len(supported) || supported[len(applied)].Name != name {
			return SchemaStamp{}, errors.Join(errors.New("storage: exact applied migration set mismatch"), rows.Close())
		}
		applied = append(applied, supported[len(applied)])
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return SchemaStamp{}, err
	}
	if len(applied) != len(supported) {
		return SchemaStamp{}, errors.New("storage: missing applied migration")
	}

	// A tracker row cannot prove its table/index still exists. Hash the full
	// sqlite_schema SQL, including columns, constraints and index predicates.
	rows, err = db.Query("SELECT type,name,tbl_name,sql FROM sqlite_schema ORDER BY type,name")
	if err != nil {
		return SchemaStamp{}, err
	}
	hash := sha256.New()
	for rows.Next() {
		var typ, name, table string
		var definition sql.NullString
		if err := rows.Scan(&typ, &name, &table, &definition); err != nil {
			return SchemaStamp{}, errors.Join(err, rows.Close())
		}
		body, _ := json.Marshal([]any{typ, name, table, definition.Valid, definition.String})
		_, _ = hash.Write(body)
		_, _ = hash.Write([]byte{'\n'})
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return SchemaStamp{}, err
	}
	return SchemaStamp{Applied: applied, Supported: supported, Digest: schemaDigest(applied, supported), ObjectDigest: hex.EncodeToString(hash.Sum(nil))}, nil
}

// ReadStoreStamp captures SQL-owned fields after producers join; the cluster
// owner supplies its separate Raft log/config receipt. This reads no payload
// into the image and performs no migration or journal-mode change.
func ReadStoreStamp(db *sql.DB, identity DBIdentity) (StoreStamp, error) {
	return readStoreStamp(db, identity, false)
}

func readStoreStamp(db *sql.DB, identity DBIdentity, observe bool) (StoreStamp, error) {
	id, err := CanonicalStoreIdentity(identity.Path, identity.DataDir)
	if err != nil || id.Inode == 0 || id != identity {
		return StoreStamp{}, errors.Join(errors.New("storage: store identity changed"), err)
	}
	// Verify the caller's DB actually owns this file, not just its pathname.
	var opened connectionKey
	// ctx-none: the image owner API has no caller context; this inspects its pinned connection.
	conn, err := db.Conn(context.Background())
	if err != nil {
		return StoreStamp{}, err
	}
	err = conn.Raw(func(raw any) error {
		c, ok := raw.(*registeredConnection)
		if !ok {
			return errors.New("storage: unregistered store connection")
		}
		opened = c.key
		return nil
	})
	if err := errors.Join(err, conn.Close()); err != nil {
		return StoreStamp{}, err
	}
	if opened != (connectionKey{id.Device, id.Inode}) {
		return StoreStamp{}, errors.New("storage: store handle identity mismatch")
	}
	tx, err := db.Begin()
	if err != nil {
		return StoreStamp{}, err
	}
	// Schema and cursor belong to one SQL snapshot. Capturing the two cursor
	// fields across separate autocommit reads could combine different commits
	// and invent a floor that the current database never actually contained.
	stamp, readErr := readSQLStamp(tx, id)
	// origin: hot-update S10 plan §5.3 / P08
	// ACK rows and the durable cursor must share this RO snapshot. Observing
	// after returning the writer would let Raft replay conceal lost ACKs.
	if observeStamp := stampObserver.Load(); readErr == nil && observe && observeStamp != nil {
		writers, readers := DefaultDriverRegistry().Connections(id)
		(*observeStamp)(tx, ObservedStamp{
			Device: id.Device, Inode: id.Inode,
			SchemaDigest: sha256.Sum256([]byte(stamp.Schema.Digest + stamp.Schema.ObjectDigest)),
			Journal:      stamp.Journal, Synchronous: stamp.Synchronous,
			AppliedIndex: stamp.AppliedIndex, AppliedTerm: stamp.AppliedTerm,
			Writers: writers, Readers: readers,
		})
	}
	return stamp, errors.Join(readErr, tx.Rollback())
}

// ObservedStamp is the reader's actual snapshot and driver census, handed to
// the S10 store witness inside the same RO transaction. storage must not
// import the witness: it depends on drillobserve and sealedstate, and
// testharness → storage would put every test package that uses the harness
// into an import cycle. The witness installs itself through SetStampObserver.
// origin: hot-update S10 plan §5.3 / P08 (lead fix: storage→storewitness cycle)
type ObservedStamp struct {
	Device, Inode             uint64
	SchemaDigest              [32]byte
	Journal                   string
	Synchronous               int
	AppliedIndex, AppliedTerm uint64
	Writers, Readers          uint32
}

var stampObserver atomic.Pointer[func(*sql.Tx, ObservedStamp)]

// SetStampObserver installs the store-witness capture, or removes it with nil.
func SetStampObserver(fn func(*sql.Tx, ObservedStamp)) {
	if fn == nil {
		stampObserver.Store(nil)
		return
	}
	stampObserver.Store(&fn)
}

func readSQLStamp(db storeReader, id DBIdentity) (StoreStamp, error) {
	stamp := StoreStamp{Identity: id}
	var err error
	stamp.Schema, err = readSchemaStamp(db)
	if err != nil {
		return StoreStamp{}, err
	}
	if err := db.QueryRow("PRAGMA journal_mode").Scan(&stamp.Journal); err != nil {
		return StoreStamp{}, err
	}
	if err := db.QueryRow("PRAGMA synchronous").Scan(&stamp.Synchronous); err != nil {
		return StoreStamp{}, err
	}
	if stamp.Journal != "delete" && stamp.Journal != "wal" {
		return StoreStamp{}, errors.New("storage: image-ineligible journal mode")
	}
	if stamp.Synchronous != 2 {
		return StoreStamp{}, errors.New("storage: effective synchronous must be FULL")
	}
	for _, item := range []struct {
		key    string
		target *uint64
	}{{"applied_index", &stamp.AppliedIndex}, {"applied_term", &stamp.AppliedTerm}} {
		var value string
		err := db.QueryRow("SELECT value FROM cluster_meta WHERE key=?", item.key).Scan(&value)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		} // Fresh single store has no Raft cursor.
		if err != nil {
			return StoreStamp{}, err
		}
		*item.target, err = strconv.ParseUint(value, 10, 64)
		if err != nil {
			return StoreStamp{}, errors.New("storage: invalid SQL apply cursor")
		}
	}
	if stamp.AppliedIndex > 0 && stamp.AppliedTerm == 0 {
		// An acknowledged command has a Raft term. Missing rows are legitimate
		// only at the fresh single-store floor; a partial nonzero pair is not.
		return StoreStamp{}, errors.New("storage: missing SQL apply term")
	}
	return stamp, nil
}

func currentDSN(path, mode string) string {
	// mode=rw cannot recreate a lost current file. The journal pragma is
	// deliberately absent: restoring the writer must preserve DELETE/WAL.
	u := url.URL{Scheme: "file", Path: path}
	query := url.Values{"mode": {mode}, "_pragma": {"foreign_keys(1)", "busy_timeout(5000)", "synchronous(FULL)"}}
	u.RawQuery = query.Encode()
	return u.String()
}

// OpenExisting opens a registered existing-file writer without migrations. It
// is for offline checkpoints and staging edits whose schema may predate the
// current release. Production hot owners use OpenCurrent's stronger stamp and
// all-closed checks; offline callers hold the canonical data-dir lock first.
func OpenExisting(dsn string) (*sql.DB, error) {
	id, err := CanonicalStoreIdentity(dsn, "")
	if err != nil || id.Inode == 0 {
		return nil, errors.Join(errors.New("storage: existing database required"), err)
	}
	db := openRegistered(currentDSN(id.Path, "rw"), false)
	db.SetMaxOpenConns(1)
	if err := db.Ping(); err != nil {
		return nil, errors.Join(err, db.Close())
	}
	return db, nil
}

// origin: hot-update S6 plan §3.3 / T07 / T08 / T11
// OpenCurrent validates through a real mode=ro connection, closes that reader,
// then opens the existing inode mode=rw. Failure never repairs schema or creates
// a database. The inherited flock remains the external lifetime interlock.
func OpenCurrent(cfg OpenImageConfig) (*sql.DB, error) {
	if cfg.Registry == nil {
		return nil, errors.New("storage: current open requires driver registry")
	}
	if err := ValidateSchemaStamp(cfg.Stamp.Schema); err != nil {
		return nil, err
	}
	id, err := CanonicalStoreIdentity(cfg.Stamp.Identity.Path, cfg.Stamp.Identity.DataDir)
	if err != nil || id.Inode == 0 || id != cfg.Stamp.Identity {
		return nil, errors.Join(errors.New("storage: current database identity mismatch"), err)
	}
	if cfg.Stamp.Synchronous != 2 || (cfg.Stamp.Journal != "delete" && cfg.Stamp.Journal != "wal") {
		return nil, errors.New("storage: invalid current durability stamp")
	}
	for _, registry := range []DriverRegistry{DefaultDriverRegistry(), cfg.Registry} {
		// Even one still-live writer, reader or unknown-close tombstone blocks
		// reopening. AssertExclusive alone permits the lawful serving writer and
		// therefore cannot prove the stronger all-closed precondition here.
		if err := registry.AssertExclusive(id); err != nil {
			return nil, err
		}
		w, r := registry.Connections(id)
		if w != 0 || r != 0 {
			return nil, errors.New("storage: current store connections not closed")
		}
	}
	ro := openRegistered(currentDSN(id.Path, "ro"), false)
	ro.SetMaxOpenConns(1)
	current, readErr := readStoreStamp(ro, id, true)
	if err := errors.Join(readErr, ro.Close()); err != nil {
		return nil, err
	}
	// Growth beyond the sealed SQL floor is legal for a current-state rollback;
	// regression or changing the term of the same command index is not. Raft
	// log/config metadata is checked by the cluster owner, not inferred here.
	if !reflect.DeepEqual(current.Schema, cfg.Stamp.Schema) || current.Journal != cfg.Stamp.Journal ||
		current.Synchronous != cfg.Stamp.Synchronous || current.AppliedIndex < cfg.Stamp.AppliedIndex ||
		current.AppliedTerm < cfg.Stamp.AppliedTerm || (current.AppliedIndex == cfg.Stamp.AppliedIndex && current.AppliedTerm != cfg.Stamp.AppliedTerm) {
		return nil, errors.New("storage: current schema, journal or durable floor mismatch")
	}
	db := openRegistered(currentDSN(id.Path, "rw"), true)
	db.SetMaxOpenConns(1)
	if err := db.Ping(); err != nil {
		return nil, errors.Join(err, db.Close())
	}
	// Recheck after the writer opens: a changed pathname or a racing pool must
	// not turn validation of one inode into activation of another.
	active, err := ReadStoreStamp(db, id)
	if err == nil && (!reflect.DeepEqual(active.Schema, current.Schema) || active.Journal != current.Journal || active.Synchronous != 2 || active.AppliedIndex < current.AppliedIndex || active.AppliedTerm < current.AppliedTerm) {
		err = errors.New("storage: current store changed during open")
	}
	if err == nil {
		err = cfg.Registry.AssertExclusive(id)
	}
	if err != nil {
		return nil, errors.Join(err, db.Close())
	}
	return db, nil
}

// CheckpointForImage preserves DELETE/WAL. DELETE has a (-1,-1,-1) N/A
// receipt after proving there is no admitted transaction; WAL requires a real
// completed TRUNCATE checkpoint. The caller must fence all SQL users first.
// origin: hot-update S6 plan §3.2 / T11
func CheckpointForImage(db *sql.DB) (busy, log, done int, err error) {
	// The admission fence makes this snapshot stable. Waiting behind an
	// unjoined transaction would hide a missing producer join in the cut.
	if db.Stats().InUse != 0 {
		err = errors.New("storage: active SQL connection blocks checkpoint")
		return
	}
	// ctx-none: the image owner has no caller context; pin the fenced checkpoint connection.
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		return
	}
	defer func() { err = errors.Join(err, conn.Close()) }()
	var journal string
	var synchronous int
	if err = conn.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&journal); err != nil {
		return
	}
	if err = conn.QueryRowContext(ctx, "PRAGMA synchronous").Scan(&synchronous); err != nil {
		return
	}
	if synchronous != 2 {
		err = errors.New("storage: checkpoint requires effective FULL")
		return
	}
	switch journal {
	case "delete":
		busy, log, done = -1, -1, -1
		if _, err = conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
			return
		}
		_, err = conn.ExecContext(ctx, "ROLLBACK")
	case "wal":
		err = conn.QueryRowContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)").Scan(&busy, &log, &done)
		if err == nil && (busy != 0 || log != 0 || done != 0) {
			err = errors.New("storage: incomplete WAL checkpoint")
		}
	default:
		err = errors.New("storage: image-ineligible checkpoint journal")
	}
	return
}
