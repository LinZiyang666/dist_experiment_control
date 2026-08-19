package p2_test

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	natsserver "github.com/nats-io/nats-server/v2/server"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
)

// origin: docs/reviews/cloned-credential-instances-plan.md §1.1 I1
//
// I1, the case the existing e2e never reaches. TestLoneInstanceKeepsItsConfiguredName
// claims to cover "repeated registers from one instance", but an agent registers
// ONCE per session and thereafter only from onNATSReconnect — so an idle sleep
// exercises no second register at all, and the assertion is about an event that
// never happened.
//
// This drives the real one: the broker process restarts (leaseHolder is
// leader-local memory and comes back EMPTY), the bus bounces, and the single
// agent on the device re-registers on the SAME connection — whose forwarded
// subscription nats.go has already replayed. Adjudication then finds a fresh
// heartbeat, an unknown holder, and an interest probe answered by the
// registrant's OWN subscription, and contests.
//
// The assertion is product-level, not log-level: release_version is written
// ONLY by registerNode (the heartbeat updater touches last_heartbeat_at and
// status alone), so a contested verdict — which returns before registerNode —
// leaves the cleared column cleared. A register COUNTER rules out the
// alternative explanation that no re-register happened at all.
func TestLoneAgentReRegisterAfterABrokerRestartStillRegisters(t *testing.T) {
	port := reserveTCPPort(t)
	url := fmt.Sprintf("nats://127.0.0.1:%d", port)
	ns := runNATSOn(t, port)
	db := openDB(t)

	// The spy is built first so it survives the bounce the same way the agent
	// does (nats.go replays its subscription on reconnect).
	spy, err := nats.Connect(url, nats.MaxReconnects(-1), nats.ReconnectWait(200*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer spy.Close()
	var registers int64
	rsub, err := spy.Subscribe(proto.SubjNodeRegister("lab", "solo"), func(*nats.Msg) {
		atomic.AddInt64(&registers, 1)
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rsub.Unsubscribe() }()
	if err := spy.Flush(); err != nil {
		t.Fatal(err)
	}

	stopBroker1 := cloneStartBroker(t, url, db)
	stopAgent := cloneStartAgent(t, url, "lab", "solo")
	defer stopAgent()
	waitForNodeCount(t, db, "lab", 1, 5*time.Second)

	// The broker restarts. Its in-memory lease registry does not survive.
	stopBroker1()
	before := atomic.LoadInt64(&registers)

	// Clear the one column only registerNode writes, so a re-register is observable.
	if _, err := db.Exec(`UPDATE nodes SET release_version = '' WHERE sid='lab' AND nid='solo'`); err != nil {
		t.Fatalf("clear release_version: %v", err)
	}

	// Bounce the bus so the agent reconnects and re-registers on the SAME conn.
	ns.Shutdown()
	ns.WaitForShutdown()
	ns2 := runNATSOn(t, port)
	defer func() {
		ns2.Shutdown()
		ns2.WaitForShutdown()
	}()

	stopBroker2 := cloneStartBroker(t, url, db)
	defer stopBroker2()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var rv string
		if err := db.QueryRow(
			`SELECT COALESCE(release_version,'') FROM nodes WHERE sid='lab' AND nid='solo'`).
			Scan(&rv); err != nil {
			t.Fatalf("read back: %v", err)
		}
		if rv != "" {
			return // the re-register landed: uncontested, as a lone device requires
		}
		time.Sleep(100 * time.Millisecond)
	}
	got := atomic.LoadInt64(&registers) - before
	if got == 0 {
		t.Fatalf("INCONCLUSIVE: no re-register was observed at all (node rows %v)",
			nodeNIDs(t, db, "lab"))
	}
	t.Fatalf("the only agent on this device re-registered %d time(s) and the broker never ran "+
		"registerNode (release_version still empty; node rows %v). Its own replayed subscription "+
		"answered the interest probe, so the lone agent contested itself.",
		got, nodeNIDs(t, db, "lab"))
}

func runNATSOn(t *testing.T, port int) *natsserver.Server {
	t.Helper()
	opts := natstest.DefaultTestOptions
	opts.Host = "127.0.0.1"
	opts.Port = port
	ns := natstest.RunServer(&opts)
	if !ns.ReadyForConnections(3 * time.Second) {
		t.Fatal("embedded nats-server not ready")
	}
	return ns
}
