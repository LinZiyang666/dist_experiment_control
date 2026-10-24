// reconcile_started_unacked_test.go — the "neither accepted nor dropped" half
// of C-5 that reconcile_marks_test.go's TestReconcileDefersOrphanKillForStartedUnackedRow
// does not pin: a StartedUnacked pid the broker has NO row for must stay out
// of AcceptedProcesses as well. The agent's courier deletes a pending started
// on `accepted && row.State == "running"` (proc_delivery.go onRegisterSuccess),
// so accepting such a pid would make the started never land, the broker never
// grow the row, and the NEXT register — unflagged, because the courier let go —
// orphan-kill the live child. A flagged pid the broker DOES have a row for is
// the opposite case: it is accepted, which is exactly how the courier settles a
// started whose ACK was lost.
package broker

import (
	"slices"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proc"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/storage"
)

// origin: hot-update S1 internal review (protocol lane) — mutation
// `accepted = append(accepted, pid)` in the live StartedUnacked skip left every
// broker test green.
func TestReconcileNeverAcceptsStartedUnackedPidWithoutARow(t *testing.T) {
	now := time.Date(2026, 6, 21, 13, 14, 15, 0, time.UTC)
	db, err := storage.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.Exec(`INSERT INTO sessions(sid,name,owner_pubkey_fp,pin_hash,state,created_at) VALUES('lab','lab','o','p','ACTIVE',?)`, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO nodes(nid,sid,status,registered_at) VALUES('lab-1','lab','ONLINE',?)`, now); err != nil {
		t.Fatal(err)
	}
	// A known RUNNING row: history exists, so the fail-closed "no history" guard
	// cannot mask the decision; it is also the "flagged but the broker has the
	// row" case (started landed, its ACK was lost).
	if err := proc.Insert(db, proc.Process{PID: "01known", SID: "lab", NID: "lab-1", Argv: []string{"x"}, StartedAt: now, BootID: "boot2", StartTimeTicks: 100}); err != nil {
		t.Fatal(err)
	}
	b := &Broker{cfg: Config{DB: db, Logger: slogDiscard(), Now: func() time.Time { return now }}}
	req := proto.NodeRegisterReq{BootID: "boot2", LocalProcesses: []proto.LocalProcess{
		{PID: "01known", State: "running", StartTimeTicks: 100, StartedUnacked: true},
		{PID: "01new", State: "running", StartTimeTicks: 700, StartedUnacked: true},
	}}
	accepted, reconciled, _, _, dropProcesses := b.reconcileOnRegister("lab", "lab-1", req)
	if slices.Contains(accepted, "01new") {
		t.Fatalf("a started-unacked pid the broker has no row for was ACCEPTED — the courier would drop its started and the next register would orphan-kill it: accepted=%v", accepted)
	}
	if slices.Contains(dropProcesses, "01new") {
		t.Fatalf("a started-unacked pid was orphan-killed: %v", dropProcesses)
	}
	if !slices.Contains(accepted, "01known") {
		t.Fatalf("a started-unacked pid whose row exists must be accepted so the courier can settle the started: accepted=%v", accepted)
	}
	if len(reconciled) != 0 {
		t.Fatalf("no row should have been reconciled: %+v", reconciled)
	}
}
