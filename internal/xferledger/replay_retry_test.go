package xferledger

import (
	"errors"
	"os"
	"reflect"
	"testing"

	x "github.com/LinZiyang666/tether/internal/xfercontract"
)

// A retained imported row can have a lost ACK even when the ledger regards
// that revision as clean. Replaying identical bytes must really Sync again;
// equality is never permission to ACK a failed file/directory barrier.
// origin: S7 internal review rv-recovery F05
func TestTransferLedgerExactRevisionRetryRequiresFreshSync(t *testing.T) {
	s := newStore(t, testConfig(t))
	row := update(t, s, RowV1{}, startRow(t), x.MutationAdmit)
	mutation := x.LedgerMutation{Kind: x.MutationAdmit, EntryNonce: row.EntryNonce, Row: row}
	concrete := s.(*ledger)
	syncs := 0
	concrete.syncFile = func(f *os.File) error { syncs++; return f.Sync() }
	retried, err := s.Update(rowKey(row), row.Revision-1, mutation, testDeadline(t))
	if err != nil || !reflect.DeepEqual(row, retried) || syncs != 2 {
		t.Fatalf("identical retained revision did not repeat file+directory Sync: syncs=%d err=%v", syncs, err)
	}
	failure := errors.New("injected fresh Sync failure")
	concrete.syncFile = func(*os.File) error { return failure }
	if _, err := s.Update(rowKey(row), row.Revision-1, mutation, testDeadline(t)); !errors.Is(err, ErrDurability) || !errors.Is(err, failure) {
		t.Fatalf("retry acknowledged equality without fresh Sync: %v", err)
	}
	census, err := s.Scan(testDeadline(t))
	if err == nil || census.Ready || census.Complete || census.Code != x.CodeDurabilityUnknown || len(census.Rows) != 1 || !reflect.DeepEqual(row, census.Rows[0]) {
		t.Fatal("failed fresh Sync lost or acknowledged the original revision")
	}
	concrete.syncFile = (*os.File).Sync
	if retried, err := s.Update(rowKey(row), row.Revision-1, mutation, testDeadline(t)); err != nil || !reflect.DeepEqual(row, retried) {
		t.Fatal("exact retained retry cannot recover", err)
	}
	changed := clone(t, row)
	changed.Intent.Path = "changed"
	mutation.Row = changed
	if _, err := s.Update(rowKey(row), row.Revision-1, mutation, testDeadline(t)); !errors.Is(err, ErrConflict) {
		t.Fatalf("equality retry admitted a different proposal: %v", err)
	}
}
