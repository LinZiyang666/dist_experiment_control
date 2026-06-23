package node

import (
	"database/sql"
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
// server_name (the production / single-node case) stores nats_server='' (the
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
