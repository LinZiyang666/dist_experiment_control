package proc

import (
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
)

// exit_session_fence_test.go — a process exit may only be written by the session
// that owns the process.
//
// origin: prerelease audit broker-core #27, raised to BLOCKER by the main process.
// MarkExited's predicate was `WHERE pid=? AND status='RUNNING'` — no session, no
// node. PIDs are ULIDs printed by `tether ps`, so any member of session A could
// publish an exit for session B's pid ON ITS OWN LEGAL SUBJECT and choose the exit
// code. The consequence is not a wrong column: the victim's next register finds no
// RUNNING row for a process that IS running, G.1 reconcile calls it an orphan, and
// the agent SIGTERMs then SIGKILLs the operator's live job.
//
// Everything here is written to fail if the fence is removed from EITHER writer.
// Both exist and which one runs is a deployment property (single vs cluster), so an
// attacker picks the unfenced one if only one is guarded.

// seedTwoSessions returns a DB holding one RUNNING process in each of two
// sessions, so "the other session's row" is a real row rather than an absence —
// the distinction the unscoped predicate could not make.
func seedTwoSessions(t *testing.T) (db *sql.DB, victimPID string) {
	t.Helper()
	db = openDB(t) // seeds session "lab" + node "lab-1"
	if _, err := db.Exec(
		`INSERT INTO sessions(sid, name, owner_pubkey_fp, pin_hash) VALUES (?,?,?,?)`,
		"other", "other", "SHA256:attacker", "phc",
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`INSERT INTO nodes(sid, nid, status) VALUES (?,?,?)`,
		"other", "other-1", "ONLINE",
	); err != nil {
		t.Fatal(err)
	}
	victim := sample() // SID "lab", NID "lab-1"
	victim.PID = "01victimrunningproc0000000"
	if err := Insert(db, victim); err != nil {
		t.Fatal(err)
	}
	attacker := sample()
	attacker.PID = "01attackerownproc000000000"
	attacker.SID, attacker.NID = "other", "other-1"
	if err := Insert(db, attacker); err != nil {
		t.Fatal(err)
	}
	return db, victim.PID
}

func statusOf(t *testing.T, db *sql.DB, pid string) string {
	t.Helper()
	var st string
	if err := db.QueryRow(`SELECT status FROM processes WHERE pid=?`, pid).Scan(&st); err != nil {
		t.Fatalf("read status of %s: %v", pid, err)
	}
	return st
}

// TestMarkExitedRefusesAForeignSessionsProcess is the direct-writer half (single
// mode). The assertion is deliberately in THREE parts, because two of them pass
// against the vulnerable code:
//
//	the row must still be RUNNING     — the actual damage
//	the error must be ErrNotFound     — so the caller acks terminally and publishes
//	                                    NO audit.proc{exit} for a live process
//	the owner must still be able to   — a fence that also blocked the legitimate
//	exit it afterwards                  writer would be a self-inflicted outage
func TestMarkExitedRefusesAForeignSessionsProcess(t *testing.T) {
	db, victimPID := seedTwoSessions(t)
	now := time.Now().UTC().Truncate(time.Second)

	err := MarkExited(db, victimPID, "other", 137, now)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-session MarkExited returned %v, want ErrNotFound.\n\n"+
			"ErrNotFound specifically, not just any error: exec.go turns it into the ONE terminal ack "+
			"(unknown_pid) and publishes no audit. Any other outcome either tells the sender the pid "+
			"exists or leaves the courier retrying forever.", err)
	}
	if got := statusOf(t, db, victimPID); got != string(StateRunning) {
		t.Fatalf("session 'other' moved session 'lab' process %s to %s.\n\n"+
			"That is the cross-session write escape: the victim's next register then sees no RUNNING "+
			"row for a process that is running, G.1 calls it an orphan, and the agent SIGKILLs the "+
			"operator's job.", victimPID, got)
	}

	// The fence must not cost the legitimate writer anything.
	if err := MarkExited(db, victimPID, "lab", 0, now); err != nil {
		t.Fatalf("the OWNING session could not exit its own process: %v", err)
	}
	if got := statusOf(t, db, victimPID); got != string(StateExited) {
		t.Fatalf("owner's MarkExited left status %s", got)
	}
}

// TestPlanMarkExitedRefusesAForeignSessionsProcess is the cluster half. The leader
// renders SQL instead of executing it, so the fence has to be in the rendered text
// AND in the ErrNotFound decision made before proposing — the same two halves as
// the direct writer, in a shape where forgetting one is easy.
func TestPlanMarkExitedRefusesAForeignSessionsProcess(t *testing.T) {
	db, victimPID := seedTwoSessions(t)
	now := time.Now().UTC().Truncate(time.Second)

	if _, err := PlanMarkExited(db, victimPID, "other", 137, now); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-session PlanMarkExited returned %v, want ErrNotFound — the leader must refuse "+
			"to PROPOSE the write, not merely render SQL that happens to match nothing", err)
	}

	cmd, err := PlanMarkExited(db, victimPID, "lab", 0, now)
	if err != nil {
		t.Fatalf("owner's PlanMarkExited: %v", err)
	}
	// The rendered statement is what every replica applies, so the fence has to be
	// IN it — a leader-side ErrNotFound decision alone would leave the committed
	// entry itself unscoped, and entries are replayed on restore by replicas that
	// never saw the decision.
	var text string
	for _, st := range cmd.Body {
		text += st.SQL + "\n"
	}
	if !strings.Contains(text, "sid=") {
		t.Fatalf("the baked ProcMarkExited statement carries no sid predicate:\n%s\n\n"+
			"Replicas apply this text, and a restore replays it. If the scoping lives only in the "+
			"leader's pre-check then the committed entry is still the unscoped UPDATE.", text)
	}

	// And it must actually work when applied.
	if err := cluster.ExecCommand(db, cmd); err != nil {
		t.Fatalf("apply owner's ProcMarkExited: %v", err)
	}
	if got := statusOf(t, db, victimPID); got != string(StateExited) {
		t.Fatalf("after applying the owner's baked command, status is %s", got)
	}
}

// TestMarkExitedFenceHoldsIdenticallyInBothWriters pins the property that made this
// a BLOCKER rather than a bug in one function: single mode and cluster mode must
// refuse the SAME set of writes. Which one is running is chosen by the deployment,
// i.e. by the attacker, so a fence on one of them is not a fence.
func TestMarkExitedFenceHoldsIdenticallyInBothWriters(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	cases := []struct {
		name, sid string
		wantErr   bool
	}{
		{"foreign session", "other", true},
		{"unknown session", "no-such-session", true},
		{"owning session", "lab", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			directDB, pid := seedTwoSessions(t)
			planDB, pid2 := seedTwoSessions(t)
			if pid != pid2 {
				t.Fatalf("fixture drift: %s vs %s", pid, pid2)
			}

			directErr := MarkExited(directDB, pid, tc.sid, 0, now)
			_, planErr := PlanMarkExited(planDB, pid, tc.sid, 0, now)

			if (directErr != nil) != tc.wantErr {
				t.Errorf("MarkExited err=%v, wantErr=%v", directErr, tc.wantErr)
			}
			if (planErr != nil) != tc.wantErr {
				t.Errorf("PlanMarkExited err=%v, wantErr=%v", planErr, tc.wantErr)
			}
			if errors.Is(directErr, ErrNotFound) != errors.Is(planErr, ErrNotFound) {
				t.Errorf("the two writers disagree on ErrNotFound: direct=%v plan=%v.\n\n"+
					"They must refuse the same set: single vs cluster mode is a deployment "+
					"property, so an attacker picks whichever one is unfenced.", directErr, planErr)
			}
		})
	}
}

// TestMarkExitedRefusesAnUnscopedExitOnBothWriters pins that NEITHER writer will
// retire a process without being told which session it belongs to.
//
// origin: prerelease audit external review B-2. This test previously asserted the
// OPPOSITE for the forwarded writer, under the name
// TestMarkExitedAcceptsEmptySidForTheNMinusOneWindow [deleted]: an empty sid rendered the
// pre-fence `WHERE pid=?` predicate so that an N-1 broker's exits would still land
// during a rolling upgrade, and the test's own comment called that safe "because no
// AGENT can produce it: exec.go takes sid from the subject".
//
// THE PREMISE WAS ABOUT THE WRONG HOP. exec.go pins the sid on the hop from the agent
// to the broker that RECEIVES the exit. The empty sid appears one hop later, when an
// N-1 broker FORWARDS it, because the old payload has no Sid field to carry what that
// broker knew. Its input was still an agent-chosen PID. So the old broker — a genuine
// peer inside the mTLS/system-account boundary — laundered an agent's request into an
// unscoped write, and the new leader had no way to tell it from a trustworthy one.
//
// Refusing is the only available fix: the old payload carries no session evidence and
// the old binary cannot be taught to add any. The exits are not lost — see
// ErrUnscopedExit for why reconcileOnRegister recovers them.
func TestMarkExitedRefusesAnUnscopedExitOnBothWriters(t *testing.T) {
	db, victimPID := seedTwoSessions(t)
	now := time.Now().UTC().Truncate(time.Second)

	// The DIRECT writer must refuse an empty sid LOUDLY. It is single-mode only, so
	// nothing forwards to it and every call site has the session; an empty one there
	// could only be a future call site that forgot, and silently accepting it would
	// remove the fence from that path while every other test here still passed.
	// It must NOT be ErrNotFound either — that is the "row isn't yours / isn't there"
	// answer, and exec.go turns it into a success ack.
	err := MarkExited(db, victimPID, "", 0, now)
	if err == nil {
		t.Fatal("the direct writer accepted an empty sid; that silently disables the fence for any " +
			"call site that forgets to pass one")
	}
	if errors.Is(err, ErrNotFound) {
		t.Fatalf("empty sid reported ErrNotFound (%v), which exec.go acks as terminal success — a "+
			"programming mistake must not be reported as 'nothing to do'", err)
	}
	if got := statusOf(t, db, victimPID); got != string(StateRunning) {
		t.Fatalf("empty-sid MarkExited changed status to %s", got)
	}

	// The FORWARDED writer must refuse it too, and with a DISTINGUISHABLE error:
	// ErrNotFound is the "row isn't yours / isn't there" answer that callers ack as
	// terminal success, so reporting the refusal that way would drop the exit silently
	// AND tell the reporter it was handled.
	planDB, pid := seedTwoSessions(t)
	cmd, perr := PlanMarkExited(planDB, pid, "", 0, now)
	if perr == nil {
		t.Fatalf("PlanMarkExited planned an unscoped exit for pid %q; an N-1 broker forwards an "+
			"agent-chosen PID with no session, so this is a cross-session write primitive", pid)
	}
	if !errors.Is(perr, ErrUnscopedExit) {
		t.Fatalf("empty-sid PlanMarkExited returned %v, want ErrUnscopedExit", perr)
	}
	if cmd != nil {
		t.Fatalf("PlanMarkExited returned a command alongside its refusal: %+v", cmd)
	}
	if got := statusOf(t, planDB, pid); got != string(StateRunning) {
		t.Fatalf("refused empty-sid plan still changed status to %s", got)
	}

	// And the shared renderer refuses independently of either caller — it is the last
	// place the unscoped predicate could come back.
	if _, serr := markExitedSQL(pid, "", 0, now); !errors.Is(serr, ErrUnscopedExit) {
		t.Fatalf("markExitedSQL rendered an unscoped UPDATE (err=%v); a third caller could then "+
			"reintroduce the fleet-wide predicate with every other test still green", serr)
	}
}
