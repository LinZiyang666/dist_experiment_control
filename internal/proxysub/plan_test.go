package proxysub

import (
	"testing"
	"time"
)

// plan_test.go (C5) — the leader-baked proxy-subscriber Plan helpers: the guarded INSERT is a no-op on
// a duplicate (never a partial-unique violation that would fsm-panic), and the keyset-epoch bump is
// change-gated (no bump on a no-op create / already-revoked).

func TestPlanCreateGuardedAndEpochGated(t *testing.T) {
	db := newDB(t)
	now := time.Date(2026, 6, 25, 12, 0, 0, 0, time.UTC)
	readEpoch := func() int64 {
		var e int64
		if err := db.QueryRow(`SELECT proxy_epoch FROM sessions WHERE sid='lab'`).Scan(&e); err != nil {
			t.Fatalf("read epoch: %v", err)
		}
		return e
	}
	run := func(cmdSQLs []string) {
		for _, s := range cmdSQLs {
			if _, err := db.Exec(s); err != nil {
				t.Fatalf("exec: %v\n%s", err, s)
			}
		}
	}
	bodySQL := func(opID string) []string {
		_, tokenHash, psk, subID, err := MintSubscriber()
		if err != nil {
			t.Fatal(err)
		}
		cmd, err := PlanCreate("lab", subID, "alice", tokenHash, psk, "SHA256:owner", now)
		if err != nil {
			t.Fatal(err)
		}
		var out []string
		for _, st := range cmd.Body {
			out = append(out, st.SQL)
		}
		return out
	}

	e0 := readEpoch()
	run(bodySQL("c1"))
	if e1 := readEpoch(); e1 != e0+1 {
		t.Fatalf("a real create must bump the keyset epoch once: %d -> %d", e0, e1)
	}
	var n int
	if err := db.QueryRow(`SELECT COUNT(1) FROM proxy_subscribers WHERE sid='lab' AND name='alice' AND state='ACTIVE'`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("subscriber not inserted: n=%d err=%v", n, err)
	}

	// A SECOND create for the same ACTIVE (sid,name) is a guarded no-op (no duplicate row, no epoch bump).
	e1 := readEpoch()
	run(bodySQL("c2"))
	if e2 := readEpoch(); e2 != e1 {
		t.Fatalf("a duplicate-name create must NOT bump the epoch (guarded no-op): %d -> %d", e1, e2)
	}
	if err := db.QueryRow(`SELECT COUNT(1) FROM proxy_subscribers WHERE sid='lab' AND name='alice' AND state='ACTIVE'`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("a duplicate create must not insert a 2nd ACTIVE row: n=%d", n)
	}

	// Revoke bumps the epoch + flips state; a 2nd revoke is a no-op (no double bump).
	revoke := func() []string {
		cmd, err := PlanRevoke("lab", "alice", now)
		if err != nil {
			t.Fatal(err)
		}
		var out []string
		for _, st := range cmd.Body {
			out = append(out, st.SQL)
		}
		return out
	}
	e2 := readEpoch()
	run(revoke())
	if e3 := readEpoch(); e3 != e2+1 {
		t.Fatalf("a real revoke must bump the epoch: %d -> %d", e2, e3)
	}
	if err := db.QueryRow(`SELECT COUNT(1) FROM proxy_subscribers WHERE sid='lab' AND name='alice' AND state='REVOKED'`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("subscriber not revoked: n=%d", n)
	}
	e3 := readEpoch()
	run(revoke())
	if e4 := readEpoch(); e4 != e3 {
		t.Fatalf("an already-revoked revoke must NOT bump the epoch: %d -> %d", e3, e4)
	}
}
