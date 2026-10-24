package storage

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"sync"
	"syscall"

	sqlite "modernc.org/sqlite"
)

// origin: hot-update S6 plan §3.2 / T08
// The census lives below sql.DB: two pools with MaxOpenConns(1) still own two
// writers. Keeping entries until a successful driver Close also makes an
// ambiguous close fail closed rather than authorize a replacement writer.
// Pathnames are diagnostic metadata, never census keys: a second pool through
// a hardlink must still appear under the same underlying database identity.
type connectionKey struct{ device, inode uint64 }
type connectionCounts struct{ writers, readers uint32 }
type connectionRegistry struct {
	mu   sync.Mutex
	live map[connectionKey]connectionCounts
}

var actualConnections = connectionRegistry{live: make(map[connectionKey]connectionCounts)}

// DefaultDriverRegistry is the process census shared by cold, current, read-only
// and probe openers. An injected image registry cannot replace this census.
func DefaultDriverRegistry() DriverRegistry { return &actualConnections }

func (r *connectionRegistry) Connections(id DBIdentity) (uint32, uint32) {
	r.mu.Lock()
	defer r.mu.Unlock()
	c := r.live[connectionKey{id.Device, id.Inode}]
	return c.writers, c.readers
}

func (r *connectionRegistry) AssertExclusive(id DBIdentity) error {
	if id.Inode == 0 || id.Path == "" {
		return errors.New("storage: image-ineligible database identity")
	}
	w, _ := r.Connections(id)
	if w > 1 {
		return fmt.Errorf("storage: multiple live driver writers: %d", w)
	}
	return nil
}

// Use the driver's registered instance so existing UDF/collation registrations
// remain effective. sql.Open is lazy here: this creates no SQLite connection.
var sqliteDriver = registeredSQLiteDriver()

func registeredSQLiteDriver() driver.Driver {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		panic(err) // The pinned dependency must register its driver during init.
	}
	d := db.Driver()
	_ = db.Close()
	return d
}

type registeredDriver struct{ exclusive bool }
type registeredConnector struct {
	dsn    string
	driver registeredDriver
}

func openRegistered(dsn string, exclusive bool) *sql.DB {
	return sql.OpenDB(registeredConnector{dsn: dsn, driver: registeredDriver{exclusive: exclusive}})
}

func (c registeredConnector) Driver() driver.Driver { return c.driver }
func (c registeredConnector) Connect(ctx context.Context) (driver.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return c.driver.Open(c.dsn)
}

// These optional interfaces are pinned by modernc and used by database/sql and
// the cluster's Raw online-backup path. Embedding preserves context cancellation
// and NewBackup/NewRestore rather than silently falling back to legacy calls.
type sqliteConnection interface {
	driver.Conn
	driver.ConnBeginTx
	driver.ConnPrepareContext
	driver.ExecerContext
	driver.QueryerContext
	// Preserve the raw connection's legacy method set without embedding the
	// interfaces database/sql/driver retired; database/sql uses the context
	// variants above.
	Exec(string, []driver.Value) (driver.Result, error)
	Query(string, []driver.Value) (driver.Rows, error)
	driver.Pinger
	driver.SessionResetter
	driver.Validator
	sqlite.HookRegisterer
	ColumnInfo(string) ([]sqlite.ColumnInfo, error)
	IsReadOnly(string) (bool, error)
	Serialize() ([]byte, error)
	Deserialize([]byte) error
	NewBackup(string) (*sqlite.Backup, error)
	NewRestore(string) (*sqlite.Backup, error)
}

type registeredConnection struct {
	sqliteConnection
	key       connectionKey
	reader    bool
	closeOnce sync.Once
	closeErr  error
}

func (d registeredDriver) Open(dsn string) (driver.Conn, error) {
	// Hold the census lock through Open and registration. A competing current
	// constructor cannot both observe zero and authorize a second live writer.
	actualConnections.mu.Lock()
	defer actualConnections.mu.Unlock()
	if d.exclusive {
		// Reject before sqlite3_open, not after an uncounted second writer
		// has briefly existed. currentDSN is generated internally.
		path, err := registeredDatabasePath(dsn)
		if err != nil {
			return nil, err
		}
		id, err := CanonicalStoreIdentity(path, "")
		if err != nil || id.Inode == 0 {
			return nil, errors.Join(errors.New("storage: current database missing"), err)
		}
		counts := actualConnections.live[connectionKey{id.Device, id.Inode}]
		if counts.writers != 0 || counts.readers != 0 {
			return nil, errors.New("storage: current store still has live driver connections")
		}
	}
	conn, err := sqliteDriver.Open(dsn)
	if err != nil {
		return nil, err
	}
	c, ok := conn.(sqliteConnection)
	if !ok {
		return nil, errors.Join(errors.New("storage: SQLite driver interface drift"), conn.Close())
	}
	key, err := driverConnectionIdentity(c)
	if err != nil {
		return nil, errors.Join(err, c.Close())
	}
	reader, err := c.IsReadOnly("main")
	if err != nil {
		return nil, errors.Join(err, c.Close())
	}
	// Inspect the real driver's open mode rather than trusting a pool's name
	// or a DSN hint. SQLite URI precedence can otherwise classify a writable
	// connection as a read-only backup and hide a second writer from the seal.
	counts := actualConnections.live[key]
	if d.exclusive && (key.inode == 0 || counts.writers != 0 || counts.readers != 0) {
		return nil, errors.Join(errors.New("storage: current store still has live driver connections"), c.Close())
	}
	if key.inode != 0 {
		if reader {
			counts.readers++
		} else {
			counts.writers++
		}
		actualConnections.live[key] = counts
	}
	return &registeredConnection{sqliteConnection: c, key: key, reader: reader}, nil
}

func driverConnectionIdentity(c sqliteConnection) (connectionKey, error) {
	// SQLite reports the file that this connection actually opened. Inferring
	// it from the unparsed caller DSN would confuse percent escaping, in-memory
	// pools and file URI spellings before the census ever saw the real inode.
	// ctx-none: driver Open has no caller context; this reads connection metadata.
	rows, err := c.QueryContext(context.Background(), "PRAGMA database_list", nil)
	if err != nil {
		return connectionKey{}, err
	}
	defer func() { _ = rows.Close() }()
	values := make([]driver.Value, 3)
	for {
		err := rows.Next(values)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return connectionKey{}, err
		}
		if values[1] != "main" {
			continue
		}
		path, ok := values[2].(string)
		if !ok {
			return connectionKey{}, errors.New("storage: invalid SQLite filename")
		}
		if path == "" {
			return connectionKey{}, nil
		} // cold in-memory, image-ineligible
		info, err := os.Stat(path)
		if err != nil {
			return connectionKey{}, err
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok {
			return connectionKey{}, errors.New("storage: database identity unavailable")
		}
		return connectionKey{uint64(stat.Dev), uint64(stat.Ino)}, nil
	}
	return connectionKey{}, errors.New("storage: main database missing")
}

func (c *registeredConnection) Close() error {
	// sql.DB can call Close after error eviction as well as explicit shutdown.
	// Count exactly once, and keep a failed close visible forever: only the
	// owner knows whether a failed driver close has released its descriptors.
	c.closeOnce.Do(func() {
		c.closeErr = c.sqliteConnection.Close()
		if c.closeErr != nil || c.key.inode == 0 {
			return
		}
		actualConnections.mu.Lock()
		defer actualConnections.mu.Unlock()
		counts := actualConnections.live[c.key]
		if c.reader {
			counts.readers--
		} else {
			counts.writers--
		}
		if counts.writers == 0 && counts.readers == 0 {
			delete(actualConnections.live, c.key)
		} else {
			actualConnections.live[c.key] = counts
		}
	})
	return c.closeErr
}

// Only internal connectors use this parser; public production binding rejects
// all externally supplied pragmas before constructing a registered DSN.
func registeredDatabasePath(dsn string) (string, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return "", err
	}
	if u.Opaque != "" {
		return url.PathUnescape(u.Opaque)
	}
	return u.Path, nil
}
