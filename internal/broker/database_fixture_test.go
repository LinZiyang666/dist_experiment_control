package broker

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"github.com/LinZiyang666/tether/internal/storage"
)

// origin: hot-update S5 plan §8 / docs/testing-standards.md T4
// Broker tests exercise queries against the current schema, not migration
// replay. Race/checkptr instrumentation makes the repeated SQLite DDL dominate
// their fixtures. Build the empty schema through the real migration runner once,
// then copy its bytes, never a live connection or mutable database. Storage's
// own tests remain responsible for migration history and restart behavior.
var brokerFixtureSchema struct {
	sync.Once
	data []byte
	err  error
}

func brokerSchemaBytes(t *testing.T) []byte {
	t.Helper()
	brokerFixtureSchema.Do(func() {
		db, err := storage.Open(":memory:")
		if err != nil {
			brokerFixtureSchema.err = err
			return
		}
		defer func() { _ = db.Close() }()
		conn, err := db.Conn(t.Context())
		if err != nil {
			brokerFixtureSchema.err = err
			return
		}
		defer func() { _ = conn.Close() }()
		brokerFixtureSchema.err = conn.Raw(func(raw any) error {
			brokerFixtureSchema.data, err = raw.(interface{ Serialize() ([]byte, error) }).Serialize()
			return err
		})
	})
	if brokerFixtureSchema.err != nil {
		t.Fatalf("build broker fixture schema: %v", brokerFixtureSchema.err)
	}
	return brokerFixtureSchema.data
}

func openBrokerFixtureDB(t *testing.T) *sql.DB {
	t.Helper()
	data := brokerSchemaBytes(t)
	// Keep the production connection-local policies, including on replacement
	// connections. The contract test below compares them with storage.Open.
	db, err := sql.Open("sqlite", "file::memory:?_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	conn, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	// Deserialize allocates its own SQLite buffer: writes in one fixture cannot
	// modify either the cached bytes or a second fixture's schema/data pages.
	if err := conn.Raw(func(raw any) error {
		return raw.(interface{ Deserialize([]byte) error }).Deserialize(data)
	}); err != nil {
		t.Fatalf("copy broker fixture schema: %v", err)
	}
	return db
}

func seedBrokerFixtureDBFile(t *testing.T, path string) {
	t.Helper()
	// Exclusive creation prevents accidentally replacing a fixture that already
	// contains test data. OpenWAL/cluster.New still install their real policies
	// and run the production migration check on this fully migrated empty file.
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(brokerSchemaBytes(t)); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

// origin: hot-update S5 plan §8 / docs/testing-standards.md T4
func TestBrokerFixtureDatabasesKeepSchemaPoliciesAndIsolation(t *testing.T) {
	production, err := storage.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = production.Close() }()
	readSchema := func(db *sql.DB) []string {
		rows, err := db.Query(`SELECT type,name,tbl_name,coalesce(sql,'') FROM sqlite_schema ORDER BY type,name`)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = rows.Close() }()
		var schema []string
		for rows.Next() {
			var kind, name, table, ddl string
			if err := rows.Scan(&kind, &name, &table, &ddl); err != nil {
				t.Fatal(err)
			}
			schema = append(schema, fmt.Sprint(kind, "\n", name, "\n", table, "\n", ddl))
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		return schema
	}
	wantSchema := readSchema(production)
	for _, mode := range []string{"memory", "wal"} {
		t.Run(mode, func(t *testing.T) {
			open := func() *sql.DB {
				if mode == "memory" {
					return openBrokerFixtureDB(t)
				}
				path := filepath.Join(t.TempDir(), "fixture.db")
				seedBrokerFixtureDBFile(t, path)
				db, err := storage.OpenWAL(path)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = db.Close() })
				return db
			}
			first, second := open(), open()
			for _, db := range []*sql.DB{first, second} {
				if got := readSchema(db); !reflect.DeepEqual(got, wantSchema) {
					t.Fatal("fixture differs from the production tables, indexes or triggers")
				}
				for _, pragma := range []string{"foreign_keys", "busy_timeout"} {
					var got, want int
					if err := db.QueryRow("PRAGMA " + pragma).Scan(&got); err != nil {
						t.Fatal(err)
					}
					if err := production.QueryRow("PRAGMA " + pragma).Scan(&want); err != nil {
						t.Fatal(err)
					}
					if got != want {
						t.Fatalf("fixture %s=%d, production=%d", pragma, got, want)
					}
				}
				if got := db.Stats().MaxOpenConnections; got != 1 {
					t.Fatalf("fixture pool limit=%d, want production single writer", got)
				}
				if _, err := db.Exec(`INSERT INTO nodes(sid,nid,status) VALUES('absent','orphan','ONLINE')`); err == nil {
					t.Fatal("fixture accepted a node without its parent session")
				}
			}
			if _, err := first.Exec(`INSERT INTO sessions(sid,name,owner_pubkey_fp,pin_hash) VALUES('first','first','owner','hash')`); err != nil {
				t.Fatal(err)
			}
			var count int
			if err := second.QueryRow(`SELECT count(*) FROM sessions`).Scan(&count); err != nil || count != 0 {
				t.Fatalf("fixture data leaked to another database: count=%d error=%v", count, err)
			}
			if _, err := first.Exec(`DROP TABLE processes`); err != nil {
				t.Fatal(err)
			}
			if got := readSchema(second); !reflect.DeepEqual(got, wantSchema) {
				t.Fatal("fixture DDL changed another database")
			}
			if got := readSchema(open()); !reflect.DeepEqual(got, wantSchema) {
				t.Fatal("fixture DDL corrupted the cached schema")
			}
		})
	}
}
