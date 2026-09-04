package broker

import (
	"database/sql"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proc"
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

// origin: prerelease audit broker-proc/L3-F1.
//
// A REFILE THAT MOVED NOTHING MUST NOT COUNT AS AN ADOPTION.
//
// adoptRowsCarriedAcrossARename returns the number of rows it carried across the
// rename, and reconcileOnRegister feeds that number straight into sawAnyRow — the
// fail-closed gate that refuses to order an orphan-kill on a node name with no
// process history. The refile itself is `UPDATE ... WHERE sid AND pid AND nid AND
// status='RUNNING'`, and SQL calls a zero-row UPDATE a success, so the old code
// counted a move that never happened. One phantom adoption is all it takes: the
// gate opens and every other pid the agent reports is ordered SIGKILLed.
//
// The staging is deterministic rather than a race, and it is the real interleaving:
// procs is a SNAPSHOT read before the refile, so the test hands the function a
// snapshot that says RUNNING while the row in the DB has since exited — exactly
// what a proc.exit arriving between the SELECT and the UPDATE produces.
func TestAPhantomRefileIsNotCountedAsAnAdoptedRow(t *testing.T) {
	b, db := leaseReconcileBroker(t)
	seedRunningProc(t, db, "p1", "gpu1")

	// The snapshot the caller would have taken.
	procs, err := proc.ListBySessionFiltered(db, "lab", proc.ListBySessionOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(procs) != 1 || procs[0].NID != "gpu1" {
		t.Fatalf("fixture: %+v", procs)
	}

	// ...and the exit that lands before the refile runs.
	if err := proc.MarkExited(db, "p1", "lab", 0, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}

	agentByPID := map[string]proto.LocalProcess{"p1": {PID: "p1", State: "running"}}
	adopted := adoptRowsCarriedAcrossARename(b, "lab", "gpu1-02",
		proto.NodeRegisterReq{PreviousNID: "gpu1"}, procs, agentByPID)

	if adopted != 0 {
		t.Errorf("adopted=%d after a refile that moved no row.\n\n"+
			"That count IS the sawAnyRow evidence. Claiming history for a node name that has "+
			"none opens the fail-closed orphan gate, and what comes through it is SIGTERM+SIGKILL "+
			"on the operator's running work.", adopted)
	}
	if _, still := agentByPID["p1"]; !still {
		t.Error("the pid was removed from agentByPID even though its row did not move.\n\n" +
			"Deleting it hides the pid from the orphan pass instead of accounting for it. The row " +
			"is still in procs under the old name, so livePIDsByRow already protects it by the " +
			"honest route — a row that exists.")
	}
}

// origin: prerelease audit broker-proc/L3-F1.
//
// A REAL adoption must update the snapshot, not just the database.
//
// The rest of reconcileOnRegister reads `procs`, so a row that moved on disk while
// the slice still says PreviousNID stays invisible for a whole register cycle —
// which is why the old code had to compensate by deleting the pid from agentByPID.
// Once the slice is corrected the row simply takes the ordinary same-name path,
// PID-reuse check included.
func TestAnAdoptedRowIsVisibleToTheRestOfReconcileImmediately(t *testing.T) {
	b, db := leaseReconcileBroker(t)
	seedRunningProc(t, db, "p1", "gpu1")

	procs, err := proc.ListBySessionFiltered(db, "lab", proc.ListBySessionOpts{})
	if err != nil {
		t.Fatal(err)
	}
	agentByPID := map[string]proto.LocalProcess{"p1": {PID: "p1", State: "running"}}
	adopted := adoptRowsCarriedAcrossARename(b, "lab", "gpu1-02",
		proto.NodeRegisterReq{PreviousNID: "gpu1"}, procs, agentByPID)

	if adopted != 1 {
		t.Fatalf("adopted=%d, want 1 — the row was RUNNING under the previous name and the agent "+
			"re-presented it", adopted)
	}
	if procs[0].NID != "gpu1-02" {
		t.Errorf("the snapshot still says nid=%q after a successful adoption.\n\n"+
			"Everything downstream reads this slice. Left stale, the adopted row is skipped by the "+
			"same-name arm for a full cycle and the only thing keeping its pid out of the orphan "+
			"pass is a deletion from agentByPID — bookkeeping by concealment.", procs[0].NID)
	}
	var nid string
	if err := db.QueryRow(`SELECT nid FROM processes WHERE pid='p1'`).Scan(&nid); err != nil {
		t.Fatal(err)
	}
	if nid != "gpu1-02" {
		t.Errorf("row nid=%q on disk, want gpu1-02", nid)
	}
}
