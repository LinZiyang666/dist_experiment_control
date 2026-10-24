package port

import (
	"database/sql"
	"reflect"
	"sync"
	"testing"

	"github.com/LinZiyang666/tether/internal/storage"
)

// Allocation tests need isolated current-schema databases, not hundreds of
// repeated migration runs. Race-instrumented DDL alone exhausted the D2 budget.
// Cache only immutable bytes produced by the real migrator, never a connection.
// origin: hot-update S6 plan §9.3 / T42 / TESTFIX
var portFixtureSchema struct {
	sync.Once
	data []byte
	err  error
}

func openPortFixtureDB(t *testing.T) *sql.DB {
	t.Helper()
	portFixtureSchema.Do(func() {
		db, err := storage.Open(":memory:")
		if err != nil {
			portFixtureSchema.err = err
			return
		}
		defer func() { _ = db.Close() }()
		conn, err := db.Conn(t.Context())
		if err != nil {
			portFixtureSchema.err = err
			return
		}
		defer func() { _ = conn.Close() }()
		portFixtureSchema.err = conn.Raw(func(raw any) error {
			portFixtureSchema.data, err = raw.(interface{ Serialize() ([]byte, error) }).Serialize()
			return err
		})
	})
	if portFixtureSchema.err != nil {
		t.Fatal(portFixtureSchema.err)
	}
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
	// Deserialize allocates private SQLite pages; DDL and rows cannot mutate
	// either the cached bytes or another fixture's database.
	if err := conn.Raw(func(raw any) error {
		return raw.(interface{ Deserialize([]byte) error }).Deserialize(portFixtureSchema.data)
	}); err != nil {
		t.Fatal(err)
	}
	return db
}

// origin: hot-update S6 plan §9.3 / T42 / TESTFIX
func TestPortFixtureDatabasesKeepSchemaPoliciesAndIsolation(t *testing.T) {
	production, err := storage.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = production.Close() }()
	schema := func(db *sql.DB) []string {
		t.Helper()
		rows, err := db.Query(`SELECT type||':'||name||':'||tbl_name||':'||coalesce(sql,'') FROM sqlite_schema ORDER BY type,name`)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = rows.Close() }()
		var result []string
		for rows.Next() {
			var row string
			if err := rows.Scan(&row); err != nil {
				t.Fatal(err)
			}
			result = append(result, row)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		return result
	}
	want := schema(production)
	first, second := openDB(t), openDB(t)
	for _, db := range []*sql.DB{first, second} {
		if !reflect.DeepEqual(schema(db), want) || db.Stats().MaxOpenConnections != 1 {
			t.Fatal("copied fixture changed the production schema or writer policy")
		}
		for _, pragma := range []string{"foreign_keys", "busy_timeout"} {
			var got, expected int
			if err := db.QueryRow("PRAGMA " + pragma).Scan(&got); err != nil {
				t.Fatal(err)
			}
			if err := production.QueryRow("PRAGMA " + pragma).Scan(&expected); err != nil || got != expected {
				t.Fatalf("fixture %s=%d production=%d: %v", pragma, got, expected, err)
			}
		}
		if _, err := db.Exec(`INSERT INTO nodes(sid,nid,status) VALUES('absent','orphan','ONLINE')`); err == nil {
			t.Fatal("copied fixture lost foreign-key enforcement")
		}
	}
	seedSessionAndNode(t, first, "first", "node")
	var count int
	if err := second.QueryRow(`SELECT count(*) FROM sessions`).Scan(&count); err != nil || count != 0 {
		t.Fatal("copied fixtures share rows", err)
	}
	if _, err := first.Exec(`DROP TABLE processes`); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(schema(second), want) || !reflect.DeepEqual(schema(openDB(t)), want) {
		t.Fatal("fixture DDL changed another database or the cached schema")
	}
}
