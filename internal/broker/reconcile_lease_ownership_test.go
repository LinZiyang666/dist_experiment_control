package broker

import (
	"database/sql"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// Ownership lens on the reconcile side of the cloned-credential lease:
// rowsOwnedBy(nid, PreviousNID) + the sawAnyRow fail-closed orphan gate.

func leaseReconcileBroker(t *testing.T) (*Broker, *sql.DB) {
	t.Helper()
	db := testharness.OpenDB(t)
	if _, err := db.Exec(
		`INSERT INTO sessions(sid,name,owner_pubkey_fp,pin_hash) VALUES('lab','lab','SHA256:o','h')`); err != nil {
		t.Fatal(err)
	}
	for _, nid := range []string{"gpu1", "gpu1-02"} {
		if _, err := db.Exec(`INSERT INTO nodes(sid,nid,status) VALUES('lab',?,'ONLINE')`, nid); err != nil {
			t.Fatal(err)
		}
	}
	b := &Broker{}
	b.cfg.DB = db
	b.cfg.Logger = testharness.SilentLog()
	b.cfg.Now = time.Now
	return b, db
}

func seedRunningProc(t *testing.T, db *sql.DB, pid, nid string) {
	t.Helper()
	if _, err := db.Exec(
		`INSERT INTO processes(pid,sid,nid,argv,started_at,status,started_by_fp)
		 VALUES(?,'lab',?,'["train.py"]',?,'RUNNING','SHA256:u')`, pid, nid, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
}

func procStatus(t *testing.T, db *sql.DB, pid string) (string, sql.NullInt64) {
	t.Helper()
	var st string
	var rc sql.NullInt64
	if err := db.QueryRow(`SELECT status, exit_code FROM processes WHERE pid=?`, pid).Scan(&st, &rc); err != nil {
		t.Fatal(err)
	}
	return st, rc
}

// origin: cloned-credential-instances external review, orphan-kill lane —
// docs/reviews/cloned-credential-instances-review.md §3b B3.
//
// PreviousNID CLAIMS ROWS THAT BELONG TO THE INSTANCE THAT NOW HOLDS THAT NAME.
//
// Interleaving (all four steps are ordinary):
//  1. A holds `gpu1` and is running the operator's job p1.
//  2. A's link blips. Clone C registers as `gpu1`; the probe finds no interest,
//     so C is GRANTED the bare name. C's own reconcile closes p1's row as a
//     missed-exit (it is not in C's LocalProcesses) — p1 keeps running on A.
//  3. C runs a job of its own: row q1 filed under `gpu1`, RUNNING, live on C.
//  4. A reconnects, is contested, adopts `gpu1-02` and registers ONCE with
//     PreviousNID="gpu1".
//
// rowsOwnedBy makes every row under `gpu1` A's, including q1 — a row A has
// never heard of. Two things break at once: q1 is closed as a missed-exit while
// C is still running it, and its mere existence flips the fail-closed
// sawAnyRow gate to true, so A's own live p1 is ordered killed.
func TestPreviousNIDMustNotClaimRowsOfTheInstanceNowHoldingThatName(t *testing.T) {
	b, db := leaseReconcileBroker(t)
	seedRunningProc(t, db, "q1", "gpu1") // the CLONE's live job, filed under the name A gave up

	_, reconciled, _, _, dropProcesses := b.reconcileOnRegister("lab", "gpu1-02", proto.NodeRegisterReq{
		PreviousNID:    "gpu1",
		LocalProcesses: []proto.LocalProcess{{PID: "p1", State: "running"}},
	})

	if len(reconciled) != 0 {
		st, rc := procStatus(t, db, "q1")
		t.Errorf("registering under an adopted lease name closed %v — a process row that belongs to "+
			"the OTHER instance now holding the previous name (q1 is now %s rc=%v while it is still "+
			"running on that instance). PreviousNID may only rescue rows this agent RE-PRESENTS; "+
			"used as a blanket ownership claim it turns one rename into a remote kill of another "+
			"instance's bookkeeping.", reconciled, st, rc)
	}
	if len(dropProcesses) != 0 {
		t.Errorf("the first register after adoption ordered the agent to SIGKILL its own live "+
			"process(es) %v. The fail-closed gate did not hold: anyRowMatches was satisfied by the "+
			"OTHER instance's row q1 (matched only through previousNID), so 'this nid has process "+
			"history' was true for a nid that has none.", dropProcesses)
	}
}

// origin: cloned-credential-instances external review, orphan-kill lane —
// docs/reviews/cloned-credential-instances-review.md §3b B3.
//
// THE SECOND REGISTER AFTER AN ADOPTION KILLS THE WORK THE FIRST ONE SAVED.
//
// PreviousNID rides exactly one register (previousNIDOnce Swaps it away) and
// NOTHING re-files the rows: after the rescue they are still filed under the
// old name. So the next register — a NATS blip, a further rebuild, anything —
// arrives with PreviousNID="" and the pre-adoption pids are strangers again.
//
// The fail-closed gate is supposed to be the belt here ("a nid with no process
// history never issues an orphan kill"), but the leased instance is a full
// citizen: the moment the operator runs ANYTHING under the lease name there is
// a row under it, sawAnyRow is true, and every surviving pre-adoption pid is
// ordered killed.
func TestSecondRegisterAfterAdoptionDoesNotKillPreAdoptionProcesses(t *testing.T) {
	b, db := leaseReconcileBroker(t)
	seedRunningProc(t, db, "p1", "gpu1")    // started before the rename, still running
	seedRunningProc(t, db, "p2", "gpu1-02") // started after it, under the lease name

	_, _, _, _, dropProcesses := b.reconcileOnRegister("lab", "gpu1-02", proto.NodeRegisterReq{
		// PreviousNID deliberately empty: it was consumed by the FIRST register.
		LocalProcesses: []proto.LocalProcess{
			{PID: "p1", State: "running"},
			{PID: "p2", State: "running"},
		},
	})

	if len(dropProcesses) != 0 {
		t.Errorf("a routine re-register ordered the agent to kill %v — its own processes, still "+
			"running, rescued by the PREVIOUS register. One hop of memory is not enough: the rows "+
			"are never re-filed under the adopted name, so every register after the first sees them "+
			"as orphans, and one row under the lease name is all it takes to unlock the kill.",
			dropProcesses)
	}
	if st, _ := procStatus(t, db, "p1"); st != "RUNNING" {
		t.Errorf("p1 row is %s", st)
	}
}
