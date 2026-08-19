package node

import (
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"
)

func readNatsServer(t *testing.T, db *sql.DB) string {
	t.Helper()
	var v string
	if err := db.QueryRow(`SELECT nats_server FROM nodes WHERE sid='lab' AND nid='lab-1'`).Scan(&v); err != nil {
		t.Fatalf("read nats_server: %v", err)
	}
	return v
}

// origin: d6_review_test.go (renamed in B6) — docs/reviews/d6-review.md
//
// TestD6RegisterNatsServerDiff1 (review A2 M2 / A6 M3): the D6 nats_server
// double-write is DIFF-1 consistent for a NON-EMPTY value — the live node.Register
// direct mutator and the FSM node.PlanRegister bake the SAME nats_server, on both
// the INSERT and the ON-CONFLICT update path (the equiv harness only exercised the
// vacuous empty default).
func TestD6RegisterNatsServerDiff1(t *testing.T) {
	now := time.Date(2026, 6, 23, 0, 0, 0, 0, time.UTC)
	in := sampleInput()
	in.NatsServer = "tether-1"

	live := openDB(t)
	if err := Register(live, in, now); err != nil {
		t.Fatalf("live register: %v", err)
	}

	fsm := openDB(t)
	cmd, err := PlanRegister(fsm, in, now)
	if err != nil {
		t.Fatalf("plan register: %v", err)
	}
	if _, err := fsm.Exec(cmd.Body[0].SQL); err != nil {
		t.Fatalf("apply plan register: %v", err)
	}

	if a, b := readNatsServer(t, live), readNatsServer(t, fsm); a != "tether-1" || b != "tether-1" || a != b {
		t.Fatalf("nats_server DIFF-1 broke on INSERT: live=%q fsm=%q", a, b)
	}

	// ON-CONFLICT update path: re-register with a DIFFERENT server_name (a reconnect
	// to another nats-server) — both paths must refresh nats_server identically.
	in2 := in
	in2.NatsServer = "tether-3"
	if err := Register(live, in2, now); err != nil {
		t.Fatalf("live re-register: %v", err)
	}
	cmd2, err := PlanRegister(fsm, in2, now)
	if err != nil {
		t.Fatalf("plan re-register: %v", err)
	}
	if _, err := fsm.Exec(cmd2.Body[0].SQL); err != nil {
		t.Fatalf("apply plan re-register: %v", err)
	}
	if a, b := readNatsServer(t, live), readNatsServer(t, fsm); a != "tether-3" || b != "tether-3" || a != b {
		t.Fatalf("nats_server DIFF-1 broke on ON CONFLICT: live=%q fsm=%q", a, b)
	}
}

// TestD6NatsServerDefaultsEmpty (review A2 m3): a register that reports NO
// server_name (the production / single-node case) stores nats_server=” (the
// NOT NULL DEFAULT), never NULL — so the column reads back cleanly + the home
// resolution treats it as "no binding".
func TestD6NatsServerDefaultsEmpty(t *testing.T) {
	db := openDB(t)
	if err := Register(db, sampleInput(), time.Now()); err != nil { // sampleInput has no NatsServer
		t.Fatalf("register: %v", err)
	}
	if v := readNatsServer(t, db); v != "" {
		t.Fatalf("unreported nats_server must default to '' (not NULL), got %q", v)
	}
}

func TestD9PlanRegisterIdentityInsertStartsOffline(t *testing.T) {
	db := openDB(t)
	now := time.Date(2026, 6, 23, 0, 0, 0, 0, time.UTC)
	in := sampleInput()
	cmd, err := PlanRegister(db, in, now)
	if err != nil {
		t.Fatalf("plan register: %v", err)
	}
	if _, err := db.Exec(cmd.Body[0].SQL); err != nil {
		t.Fatalf("apply plan register: %v", err)
	}
	var status string
	var hb sql.NullTime
	if err := db.QueryRow(`SELECT status, last_heartbeat_at FROM nodes WHERE sid=? AND nid=?`, in.SID, in.NID).Scan(&status, &hb); err != nil {
		t.Fatalf("read node: %v", err)
	}
	if status != string(StateOffline) || hb.Valid {
		t.Fatalf("identity-only register must not create a live heartbeat: status=%q hb=%v", status, hb.Valid)
	}
	if err := Heartbeat(db, in.SID, in.NID, now.Add(time.Second)); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	in.ReleaseVersion = "v2"
	cmd, err = PlanRegister(db, in, now.Add(2*time.Second))
	if err != nil {
		t.Fatalf("plan re-register: %v", err)
	}
	if _, err := db.Exec(cmd.Body[0].SQL); err != nil {
		t.Fatalf("apply re-register: %v", err)
	}
	if err := db.QueryRow(`SELECT status, last_heartbeat_at FROM nodes WHERE sid=? AND nid=?`, in.SID, in.NID).Scan(&status, &hb); err != nil {
		t.Fatalf("read node after conflict: %v", err)
	}
	if status != string(StateOnline) || !hb.Valid {
		t.Fatalf("identity update must preserve liveness: status=%q hb=%v", status, hb.Valid)
	}
}

func TestD9PlanRegisterRejectsDeletingSession(t *testing.T) {
	db := openDB(t)
	if _, err := db.Exec(
		`UPDATE sessions SET state='DELETING' WHERE sid='lab'`,
	); err != nil {
		t.Fatal(err)
	}
	_, err := PlanRegister(db, RegisterInput{
		SID: "lab", NID: "lab-1", ProtoVersion: 2, ReleaseVersion: "v2",
	}, time.Date(2026, 6, 24, 0, 0, 0, 0, time.UTC))
	if !errors.Is(err, ErrSessionNotActive) {
		t.Fatalf("PlanRegister deleting session err = %v, want ErrSessionNotActive", err)
	}
	if err := Register(db, RegisterInput{
		SID: "lab", NID: "lab-1", ProtoVersion: 2, ReleaseVersion: "v2",
	}, time.Date(2026, 6, 24, 0, 0, 0, 0, time.UTC)); !errors.Is(err, ErrSessionNotActive) {
		t.Fatalf("Register deleting session err = %v, want ErrSessionNotActive", err)
	}
}

// origin: docs/reviews/cloned-credential-instances-external-review-tasklist.md F3
//
// RegisterInput is the shared wire-to-storage command used by both single-node
// and clustered brokers. Its Leased classification is operationally
// load-bearing: fleet upgrades and lease allocation query the stored column.
// The replicated PlanRegister path must therefore preserve it on both INSERT
// and conflict exactly as the direct Register path does.
func TestLeaseClassificationIsNotSchemaAndCannotDivergeBetweenModes(t *testing.T) {
	// origin: external review F1 — REWRITTEN, not deleted, by the main process.
	//
	// F1 caught a real divergence: single-mode Register wrote a `nodes.leased`
	// column that the replicated PlanRegister never rendered, so a clustered
	// broker classified every lease wrong. F1's OTHER half is why the fix is not
	// "render the column in both": the column arrived via migration 0019, and a
	// same-proto rolling release MUST NOT add migrations (g5-plan OQ-2) — an
	// un-migrated follower would fail to Apply a register command naming a
	// column it does not have. Filling the column in both writers would have
	// traded a data bug for a raft-Apply compatibility incident.
	//
	// ADJUDICATION: the column is gone. Lease-ness is not schema. It is derived
	// where it is needed from the agent_provisioning rows, which BOTH modes
	// already replicate through the same path — so the divergence F1 found
	// cannot recur by construction rather than by a second writer remembering to
	// keep up.
	//
	// What this test now pins is exactly that: the two register paths write the
	// SAME COLUMN SET, so neither can grow a private classification again.
	now := time.Date(2026, 8, 19, 0, 0, 0, 0, time.UTC)
	in := sampleInput()

	live := openDB(t)
	if err := Register(live, in, now); err != nil {
		t.Fatalf("direct register: %v", err)
	}
	fsm := openDB(t)
	cmd, err := PlanRegister(fsm, in, now)
	if err != nil {
		t.Fatalf("plan register: %v", err)
	}
	if _, err := fsm.Exec(cmd.Body[0].SQL); err != nil {
		t.Fatalf("apply plan register: %v", err)
	}

	if _, err := live.Exec(`SELECT leased FROM nodes LIMIT 1`); err == nil {
		t.Fatal("nodes.leased is back. A same-proto rolling release must not add a migration " +
			"(g5-plan OQ-2): a follower that has not run it fails to Apply a register command " +
			"naming the column, which is a cluster-wide outage rather than a display bug. " +
			"Derive lease-ness from the provisioning rows instead.")
	}

	// Neither writer may carry a classification the other lacks.
	if d, r := nodeColumns(t, live), nodeColumns(t, fsm); d != r {
		t.Fatalf("the two register paths disagree on the node row's shape:\n direct=%s\n replicated=%s", d, r)
	}
}

// nodeColumns renders the non-null columns of the single node row, so a
// classification written by one register path and not the other shows up.
func nodeColumns(t *testing.T, db *sql.DB) string {
	t.Helper()
	rows, err := db.Query(`SELECT * FROM nodes`)
	if err != nil {
		t.Fatalf("select nodes: %v", err)
	}
	defer func() { _ = rows.Close() }()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("columns: %v", err)
	}
	if !rows.Next() {
		t.Fatal("no node row")
	}
	vals := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	if err := rows.Scan(ptrs...); err != nil {
		t.Fatalf("scan: %v", err)
	}
	var out strings.Builder
	for i, c := range cols {
		// LIVENESS is deliberately single-mode: PlanRegister renders the IDENTITY
		// half only, and the local mutator stamps the heartbeat (D9 §3). That
		// split is by design and is not the divergence this guard is about.
		if c == "last_heartbeat_at" {
			continue
		}
		if vals[i] != nil {
			out.WriteString(c)
			out.WriteByte(' ')
		}
	}
	return out.String()
}
