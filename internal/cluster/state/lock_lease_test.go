package state

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"testing"
)

// origin: hot-update S6 plan §7.1 / T40
func TestUpgradeLockPreservesRowCloseFailure(t *testing.T) {
	for _, value := range []driver.Value{"", nil} {
		failure := errors.New("injected lock rows close failure")
		db := sql.OpenDB(lockRowsConnector{value: value, closeErr: failure})
		func() {
			defer func() {
				if err := db.Close(); err != nil {
					t.Error(err)
				}
			}()
			_, err := ReadUpgradeLock(db)
			// An early corrupt-value or Scan refusal must retain both failures:
			// a failed rows close is not a successful lock-read receipt.
			if !errors.Is(err, failure) {
				t.Fatalf("lock read discarded rows close failure: %v", err)
			}
			if value == "" && !errors.Is(err, ErrUpgradeHolder) {
				t.Fatalf("rows close replaced the invalid holder error: %v", err)
			}
			if value == nil && !strings.Contains(err.Error(), "converting NULL to string") {
				t.Fatalf("rows close replaced the Scan error: %v", err)
			}
		}()
	}
}

// This driver exposes the database/sql Close seam directly. SQLite normally
// closes exhausted rows successfully, which cannot exercise early-return errors.
type lockRowsConnector struct {
	value    driver.Value
	closeErr error
}

func (c lockRowsConnector) Connect(context.Context) (driver.Conn, error) {
	return lockRowsConn{c}, nil
}
func (c lockRowsConnector) Driver() driver.Driver { return lockRowsDriver{c} }

type lockRowsDriver struct{ connector lockRowsConnector }

func (d lockRowsDriver) Open(string) (driver.Conn, error) {
	return lockRowsConn(d), nil
}

type lockRowsConn struct{ connector lockRowsConnector }

func (lockRowsConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("lock rows fixture does not prepare statements")
}
func (lockRowsConn) Close() error { return nil }
func (lockRowsConn) Begin() (driver.Tx, error) {
	return nil, errors.New("lock rows fixture does not begin transactions")
}
func (c lockRowsConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	return &lockRows{connector: c.connector}, nil
}

type lockRows struct {
	connector lockRowsConnector
	delivered bool
}

func (*lockRows) Columns() []string { return []string{"key", "value"} }
func (r *lockRows) Close() error    { return r.connector.closeErr }
func (r *lockRows) Next(values []driver.Value) error {
	if r.delivered {
		return io.EOF
	}
	r.delivered = true
	values[0], values[1] = MetaKeyUpgradeActive, r.connector.value
	return nil
}
