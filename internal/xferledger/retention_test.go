package xferledger

import (
	"testing"
	"time"

	x "github.com/LinZiyang666/tether/internal/xfercontract"
)

// origin: hot-update S7 plan §4 / §5.1 / T39
func TestTransferResultRetentionStartsOnceAfterExactHistory(t *testing.T) {
	cfg := testConfig(t)
	store := newStore(t, cfg)
	row := startRow(t)
	row.Retention.ResultUntil = time.Time{}
	row = update(t, store, RowV1{}, row, x.MutationAdmit)
	row = update(t, store, row, grantedRow(t, row), x.MutationReserve)
	row = update(t, store, row, decidedRow(t, row), x.MutationDecide)
	confirmed := confirmedRow(t, row)
	confirmed.Retention.ResultUntil = row.StartedAt.Add(time.Hour)
	confirmed.Retention.Deadline = x.BootDeadline{BootID: row.Origin.BootID, Deadline: testDeadline(t)}
	for _, fault := range []string{"before_history", "identity", "creation", "boot"} {
		bad := clone(t, confirmed)
		switch fault {
		case "before_history":
			bad = clone(t, row)
			bad.Retention = confirmed.Retention
		case "identity":
			bad.Retention.Identity.EntryNonce = "foreign"
		case "creation":
			bad.Retention.CreateUntil = bad.Retention.CreateUntil.Add(time.Hour)
		case "boot":
			bad.Retention.Deadline.BootID = "other"
		}
		if _, err := store.Update(rowKey(row), row.Revision, x.LedgerMutation{Kind: x.MutationHistory, EntryNonce: row.EntryNonce, Row: bad}, testDeadline(t)); err == nil {
			t.Fatalf("%s authorized retention", fault)
		}
	}
	row = update(t, store, row, confirmed, x.MutationHistory)
	reopened := newStore(t, cfg)
	census, err := reopened.Scan(testDeadline(t))
	if err != nil || !census.Complete || len(census.Rows) != 1 || census.Rows[0].Retention != row.Retention {
		t.Fatalf("retention not durable: %+v %v", census, err)
	}
	for _, field := range []string{"wall", "boot"} {
		next := clone(t, row)
		if field == "wall" {
			next.Retention.ResultUntil = next.Retention.ResultUntil.Add(time.Hour)
		} else {
			next.Retention.Deadline.Deadline++
		}
		if _, err := reopened.Update(rowKey(row), row.Revision, x.LedgerMutation{Kind: x.MutationHistory, EntryNonce: row.EntryNonce, Row: next}, testDeadline(t)); err == nil {
			t.Fatalf("%s retention renewed", field)
		}
	}
}

// Reconciliation may learn either exact history slot first. Only the second
// receipt starts retention, even when settlement is later than the byte budget.
// origin: hot-update S7 plan §1.2 / §4 / §5.1 / INTEG-R retention reconciliation
func TestTransferResultRetentionAcceptsEitherHistoryReceiptOrder(t *testing.T) {
	for _, terminalFirst := range []bool{false, true} {
		store := newStore(t, testConfig(t))
		row := startRow(t)
		row.Retention.ResultUntil = time.Time{}
		row = update(t, store, RowV1{}, row, x.MutationAdmit)
		row = update(t, store, row, grantedRow(t, row), x.MutationReserve)
		row = update(t, store, row, decidedRow(t, row), x.MutationDecide)
		confirmed := confirmedRow(t, row)
		partial := clone(t, row)
		index := 0
		if terminalFirst {
			index = 1
			partial.TerminalDelivery = x.DeliveryHistoryConfirmed
		} else {
			partial.StartDelivery = x.DeliveryHistoryConfirmed
		}
		partial.HistoryReceipts = []x.HistoryReceipt{confirmed.HistoryReceipts[index]}
		row = update(t, store, row, partial, x.MutationHistory)
		confirmed = confirmedRow(t, row)
		confirmed.Retention.ResultUntil = row.StartedAt.Add(24 * time.Hour)
		confirmed.Retention.Deadline = x.BootDeadline{BootID: row.Origin.BootID, Deadline: testDeadline(t)}
		row = update(t, store, row, confirmed, x.MutationHistory)
		if row.Retention != confirmed.Retention {
			t.Fatal("second exact receipt lost original result retention")
		}
	}
}
