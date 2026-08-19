package p2_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agent"
	"github.com/LinZiyang666/tether/internal/broker"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
)

// Cloned-credential instances, end to end.
//
// origin: docs/reviews/cloned-credential-instances-plan.md §0.1
//
// THE DEFECT THIS PINS, observed on the live fleet before the fix: a VM/
// container image with a baked agent, launched twice, produced ONE row in
// `node ls` while BOTH processes executed every command addressed to that name.
// It was proved by encoding each pod's IP into the exit code — the audit log
// showed rc=20 and rc=120 for every single invocation, three runs in a row.
//
// The mechanism is that both clones present the same credential (which is the
// credential model working as designed) and the same nid, so both authenticate
// and both subscribe to the same forwarded subject.

func cloneStartBroker(t *testing.T, url string, db *sql.DB) func() {
	t.Helper()
	b, err := broker.New(broker.Config{
		NATSURL:           url,
		DB:                db,
		Logger:            testharness.SilentLog(),
		ReconcileInterval: 50 * time.Millisecond,
		StaleAfter:        2 * time.Second,
		OfflineAfter:      10 * time.Second,
		// Keep the grace window wide enough that a second instance arriving
		// milliseconds after the first is unambiguously CONTESTED rather than
		// racing the clock; the arithmetic itself is unit-tested.
		LeaseGrace: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- b.Run(ctx) }()
	// Crude readiness probe, same shape the other phase harnesses use.
	for i := 0; i < 30; i++ {
		if nc, err := nats.Connect(url); err == nil {
			nc.Close()
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	return func() {
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("broker did not exit")
		}
	}
}

func cloneStartAgent(t *testing.T, url, sid, nid string) func() {
	t.Helper()
	a, err := agent.New(agent.Config{
		NATSURL:           url,
		SID:               sid,
		NID:               nid,
		Logger:            testharness.SilentLog(),
		HeartbeatInterval: 100 * time.Millisecond,
		RegisterTimeout:   2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	return func() {
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("agent did not exit")
		}
	}
}

func nodeNIDs(t *testing.T, db *sql.DB, sid string) []string {
	t.Helper()
	rows, err := db.Query(`SELECT nid FROM nodes WHERE sid = ? ORDER BY nid`, sid)
	if err != nil {
		t.Fatalf("query nodes: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var nid string
		if err := rows.Scan(&nid); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out = append(out, nid)
	}
	return out
}

func waitForNodeCount(t *testing.T, db *sql.DB, sid string, want int, timeout time.Duration) []string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var got []string
	for time.Now().Before(deadline) {
		got = nodeNIDs(t, db, sid)
		if len(got) == want {
			return got
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d node row(s); got %v", want, got)
	return nil
}

// TestTwoInstancesOfOneImageBecomeTwoDevices is the headline assertion: two
// processes sharing one credential and one configured nid must end up as two
// separately addressable devices, not one.
//
// MUTATION: remove the contested-register branch from handleRegister (let it
// fall through to registerNode) and this fails with a single row — which is
// exactly the pre-fix production state.
func TestTwoInstancesOfOneImageBecomeTwoDevices(t *testing.T) {
	url := startNATS(t)
	db := openDB(t) // p2's openDB already seeds the "lab" session
	defer cloneStartBroker(t, url, db)()

	// The image's configured identity. Both instances present it.
	const baked = "jupyter"

	stopA := cloneStartAgent(t, url, "lab", baked)
	defer stopA()
	waitForNodeCount(t, db, "lab", 1, 5*time.Second)

	stopB := cloneStartAgent(t, url, "lab", baked)
	defer stopB()
	got := waitForNodeCount(t, db, "lab", 2, 8*time.Second)

	wantLease := proto.LeaseNameFor(baked, proto.FirstLeaseSuffix)
	if got[0] != baked || got[1] != wantLease {
		t.Fatalf("got node rows %v, want [%q %q] — the first instance keeps the configured name and "+
			"the second is leased a suffix", got, baked, wantLease)
	}

	// I2: the leased row is an ordinary, addressable node — its name passes the
	// same validator every subject parser re-applies, so an un-upgraded ctl can
	// target it with no changes at all.
	if err := proto.ValidateNID(got[1]); err != nil {
		t.Fatalf("leased name %q is not addressable: %v", got[1], err)
	}
}

// TestLoneInstanceKeepsItsConfiguredName is the I1 counterpart, and it is the
// one that protects every device that will only ever run one agent: a single
// instance must produce exactly one row, under exactly the configured name.
//
// MUTATION: make the adjudicator contest unconditionally (drop the
// heartbeat-age test) and this fails with a suffixed row.
func TestLoneInstanceKeepsItsConfiguredName(t *testing.T) {
	url := startNATS(t)
	db := openDB(t) // p2's openDB already seeds the "lab" session
	defer cloneStartBroker(t, url, db)()

	stop := cloneStartAgent(t, url, "lab", "solo")
	defer stop()

	got := waitForNodeCount(t, db, "lab", 1, 5*time.Second)
	if got[0] != "solo" {
		t.Fatalf("a lone agent must register under its configured name; got %q", got[0])
	}
	// And it must STAY that way across re-registers: an agent re-registers on
	// every reconnect, and a non-idempotent claim would burn a fresh suffix
	// each time while the same live process still held the previous one.
	time.Sleep(600 * time.Millisecond)
	if got := nodeNIDs(t, db, "lab"); len(got) != 1 || got[0] != "solo" {
		t.Fatalf("repeated registers from one instance must not create new rows; got %v", got)
	}
}

// TestRestartedInstanceReclaimsItsNameRatherThanBeingSuffixed pins the case
// three separate reviewers flagged as the design's sharpest wrong result: an
// ordinary agent restart must NOT rename the device.
//
// Under D2 a restarted process is a NEW instance (nothing is persisted, which
// is what stops a cloned image from carrying an id), so it cannot prove it is
// the previous holder. What saves it is that the previous holder's
// subscription is gone the moment its connection dropped, so the interest
// probe answers ErrNoResponders instantly and the name is granted back.
//
// MUTATION: replace the probe with a pure timer and this fails whenever the
// restart lands inside LeaseGrace — which, with systemd's RestartSec=5 and a
// 5s heartbeat interval, is roughly a fifth of real restarts.
func TestRestartedInstanceReclaimsItsNameRatherThanBeingSuffixed(t *testing.T) {
	url := startNATS(t)
	db := openDB(t) // p2's openDB already seeds the "lab" session
	defer cloneStartBroker(t, url, db)()

	stop := cloneStartAgent(t, url, "lab", "durable")
	waitForNodeCount(t, db, "lab", 1, 5*time.Second)

	// Stop it and bring it back well inside LeaseGrace, so the heartbeat clock
	// ALONE would call this contested — the probe is what must save it.
	//
	// The pause models a real process restart: systemd's RestartSec is 5s, and
	// even a bare re-exec costs more than this. It is deliberately LONGER than
	// leaseGrantWindow, which exists only to cover the milliseconds between a
	// grant and that agent installing its subscription — a window that must
	// never be wide enough to swallow a restart, or a durable device gets
	// renamed for restarting.
	stop()
	time.Sleep(1200 * time.Millisecond)
	stop2 := cloneStartAgent(t, url, "lab", "durable")
	defer stop2()

	// Give the successor time to register and to be (wrongly) suffixed if the
	// probe were not consulted.
	time.Sleep(1500 * time.Millisecond)
	got := nodeNIDs(t, db, "lab")
	if len(got) != 1 || got[0] != "durable" {
		t.Fatalf("a restart must reclaim the same name, not be suffixed; got %v.\n"+
			"This is the case a clock-only rule gets wrong: the dead predecessor's heartbeat is "+
			"only a second old, but its subscription is already gone.", got)
	}
}
