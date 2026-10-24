package agent

// proc_delivery_test.go — the h1 C courier's contract, driven directly
// (deliverRound/attempt are called synchronously; the goroutine wrapper is
// exercised by the p2/p5 e2e suites).
// origin: docs/reviews/h1-plan.md workstream C (2026-08-04 incident, zombie
// class a).

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"runtime"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agentimage"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
)

func courierAgent(t *testing.T, url string) *Agent {
	t.Helper()
	a, err := New(Config{NATSURL: url, SID: "lab", NID: "n1"})
	if err != nil {
		t.Fatal(err)
	}
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	a.ncBox.Store(nc)
	return a
}

// ackResponder subscribes the broker half: it decodes each couriered event,
// records it, and (mode-dependent) acks, drops, or nacks.
type ackResponder struct {
	exits    atomic.Int32
	starts   atomic.Int32
	mode     atomic.Value // "ack" | "silent" | "nack"
	lastExit atomic.Value // proto.ProcExitEvent
}

func startAckResponder(t *testing.T, url string) *ackResponder {
	t.Helper()
	r := &ackResponder{}
	r.mode.Store("ack")
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	if _, err := nc.Subscribe(proto.SubjectPrefix+".s.*.ev.node.*.proc.*.*", func(msg *nats.Msg) {
		_, _, _, kind, ok := proto.ParseEvProc(msg.Subject)
		if !ok {
			return
		}
		switch kind {
		case "exit":
			var ev proto.ProcExitEvent
			_ = json.Unmarshal(msg.Data, &ev)
			r.lastExit.Store(ev)
			r.exits.Add(1)
		case "started":
			r.starts.Add(1)
		}
		switch r.mode.Load().(string) {
		case "silent":
			return // the pre-h1 broker shape: processes, never answers
		case "nack":
			body, _ := json.Marshal(proto.ProcEventAck{OK: false, Code: "store_error"})
			_ = msg.Respond(body)
		default:
			body, _ := json.Marshal(proto.ProcEventAck{OK: true, Code: "recorded"})
			_ = msg.Respond(body)
		}
	}); err != nil {
		t.Fatal(err)
	}
	_ = nc.Flush()
	return r
}

// TestCourierDeliversExitOnCurrentConn is the class-a regression: the exit is
// enqueued while ncBox holds a DIFFERENT conn than the one the run was
// spawned with (the spawn conn is closed — the incident's exact shape). The
// courier must deliver on the CURRENT conn with the real rc.
// Mutation check: publishing on a captured spawn conn (the pre-h1 code)
// makes the delivery vanish and this test red.
func TestCourierDeliversExitOnCurrentConn(t *testing.T) {
	url := startNATS(t)
	r := startAckResponder(t, url)
	a := courierAgent(t, url)

	// The "spawn conn" the pre-h1 code would have captured — closed mid-run.
	spawnConn, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	spawnConn.Close()

	a.courier.enqueueExit("pid-1", 42)
	a.courier.deliverRound(context.Background())

	deadline := time.Now().Add(3 * time.Second)
	for r.exits.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("exit never delivered on the current conn")
		}
		a.courier.deliverRound(context.Background())
		time.Sleep(20 * time.Millisecond)
	}
	ev := r.lastExit.Load().(proto.ProcExitEvent)
	if ev.ExitCode != 42 {
		t.Fatalf("delivered rc=%d, want the REAL 42 (not G.1's -1)", ev.ExitCode)
	}
	a.courier.mu.Lock()
	n := len(a.courier.pending)
	a.courier.mu.Unlock()
	if n != 0 {
		t.Fatalf("acked entry not cleared: %d pending", n)
	}
}

// TestCourierStartedBeforeExitSamePID pins per-PID ordering: with both events
// queued, the round must deliver started first and hold the exit back until
// the started clears.
func TestCourierStartedBeforeExitSamePID(t *testing.T) {
	url := startNATS(t)
	r := startAckResponder(t, url)
	r.mode.Store("silent") // hold both pending
	a := courierAgent(t, url)
	old := courierRequestTimeout
	courierRequestTimeout = 150 * time.Millisecond
	t.Cleanup(func() { courierRequestTimeout = old })

	a.courier.enqueueStarted("pid-1", []string{"x"}, "SHA256:u", "", 0, time.Now().UTC())
	a.courier.enqueueExit("pid-1", 0)
	a.courier.deliverRound(context.Background())

	if s, e := r.starts.Load(), r.exits.Load(); s != 1 || e != 0 {
		t.Fatalf("ordering violated: started=%d exit=%d (want 1/0 — exit must wait for started to clear)", s, e)
	}
}

// TestCourierParksAgainstSilentBroker is the plan critique-2 BLOCKER
// regression: a responder that PROCESSES every event but never answers (the
// pre-h1 broker on the wire) must park the entry after 3 connected-timeouts —
// NOT retry forever, which would re-deliver (and re-audit) the same exit
// every backoff interval for the rest of time. nc.Close() does NOT exercise
// this hole: the conn must stay CONNECTED while the acks never come.
func TestCourierParksAgainstSilentBroker(t *testing.T) {
	url := startNATS(t)
	r := startAckResponder(t, url)
	r.mode.Store("silent")
	a := courierAgent(t, url)
	old := courierRequestTimeout
	courierRequestTimeout = 100 * time.Millisecond
	t.Cleanup(func() { courierRequestTimeout = old })

	a.courier.enqueueExit("pid-1", 7)
	ev := a.courier.pending[courierKey("pid-1", procEventExit)]
	nc := a.ncBox.Load()
	for i := 0; i < courierParkAfter; i++ {
		ev.next = time.Time{} // bypass backoff waits — the belt under test is the park counter
		a.courier.attempt(context.Background(), nc, ev)
	}
	if !ev.parked {
		t.Fatalf("entry not parked after %d silent timeouts (silent=%d)", courierParkAfter, ev.silent)
	}
	delivered := r.exits.Load()
	// Parked = register-replay-only: further rounds must not send it again.
	for i := 0; i < 3; i++ {
		a.courier.deliverRound(context.Background())
	}
	time.Sleep(50 * time.Millisecond)
	if got := r.exits.Load(); got != delivered {
		t.Fatalf("parked entry was re-sent: %d -> %d deliveries", delivered, got)
	}
}

// TestCourierNeverParksStarted is the internal-review (wire lens) regression:
// a STARTED must keep retrying no matter how many acks go missing. Parking it
// would leave the broker with no row for a LIVE process, and the very next
// register's G.1 orphan pass would then tell the agent to KILL the user's
// process. The retry is harmless in the other direction (INSERT OR IGNORE +
// the broker's dedup pre-read), so the asymmetry with exits is deliberate.
func TestCourierNeverParksStarted(t *testing.T) {
	url := startNATS(t)
	startAckResponder(t, url).mode.Store("silent")
	a := courierAgent(t, url)
	old := courierRequestTimeout
	courierRequestTimeout = 100 * time.Millisecond
	t.Cleanup(func() { courierRequestTimeout = old })

	a.courier.enqueueStarted("pid-1", []string{"x"}, "SHA256:u", "", 0, time.Now().UTC())
	ev := a.courier.pending[courierKey("pid-1", procEventStarted)]
	nc := a.ncBox.Load()
	for i := 0; i < courierParkAfter+3; i++ {
		ev.next = time.Time{}
		a.courier.attempt(context.Background(), nc, ev)
	}
	if ev.parked {
		t.Fatalf("STARTED parked after %d silent timeouts — a parked started has no replay channel, "+
			"so G.1's orphan pass would kill the live process", ev.silent)
	}
}

// TestCourierParkedStartedDoesNotBlockExit pins the ordering carve-out: even
// if a started somehow ends up parked, its pid's EXIT must still be
// couriered. Holding the exit behind a never-retried started is strictly
// worse than pre-h1 fire-and-forget — the exit would never be sent at all.
func TestCourierParkedStartedDoesNotBlockExit(t *testing.T) {
	url := startNATS(t)
	r := startAckResponder(t, url)
	a := courierAgent(t, url)

	a.courier.enqueueStarted("pid-1", []string{"x"}, "SHA256:u", "", 0, time.Now().UTC())
	a.courier.enqueueExit("pid-1", 9)
	// Force the started into the parked state directly (the production path
	// no longer does this for starteds — the point is that the EXIT survives
	// it regardless).
	a.courier.mu.Lock()
	a.courier.pending[courierKey("pid-1", procEventStarted)].parked = true
	a.courier.mu.Unlock()

	deadline := time.Now().Add(3 * time.Second)
	for r.exits.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("exit never delivered — a parked started blocked it forever")
		}
		a.courier.deliverRound(context.Background())
		time.Sleep(20 * time.Millisecond)
	}
}

// TestCourierMaxLifetimeReleasesParkedEntries pins that the "nothing waits
// forever" belt reaches PARKED entries — they are the ones that can sit for
// days, so exempting them would make the bound unreachable for its most
// likely subject.
func TestCourierMaxLifetimeReleasesParkedEntries(t *testing.T) {
	url := startNATS(t)
	startAckResponder(t, url)
	a := courierAgent(t, url)

	a.courier.enqueueExit("ancient", 0)
	a.courier.mu.Lock()
	ev := a.courier.pending[courierKey("ancient", procEventExit)]
	ev.parked = true
	ev.enqueuedAt = time.Now().Add(-courierMaxLifetime - time.Hour)
	a.courier.mu.Unlock()

	a.courier.deliverRound(context.Background())

	a.courier.mu.Lock()
	_, still := a.courier.pending[courierKey("ancient", procEventExit)]
	a.courier.mu.Unlock()
	if still {
		t.Fatal("a parked entry older than courierMaxLifetime was not released")
	}
}

// TestCourierRegisterClearance pins the C4 settlement rules — including the
// critique-2 fix: an EXIT clears when its pid is ABSENT from
// AcceptedProcesses (a broker that already marked it EXITED reports it in
// NEITHER list; a ReconciledProcesses-membership rule would strand the entry
// forever). A STARTED clears only when the pid IS accepted.
func TestCourierRegisterClearance(t *testing.T) {
	url := startNATS(t)
	a := courierAgent(t, url)

	a.courier.enqueueExit("done-pid", 0)          // broker no longer lists it → clear
	a.courier.enqueueExit("still-running-pid", 0) // broker still believes RUNNING → keep (retry)
	a.courier.enqueueStarted("live-pid", []string{"x"}, "SHA256:u", "", 0, time.Now().UTC())
	a.courier.enqueueStarted("unheard-pid", []string{"x"}, "SHA256:u", "", 0, time.Now().UTC())

	rows := append(a.courier.pendingExitSnapshot(),
		proto.LocalProcess{PID: "live-pid", State: "running"},
		proto.LocalProcess{PID: "unheard-pid", State: "running"})
	a.courier.onRegisterSuccess(a.courier.registerReceipt(), rows, proto.NodeRegisterResp{
		OK:                true,
		AcceptedProcesses: []string{"still-running-pid", "live-pid"},
	})

	a.courier.mu.Lock()
	defer a.courier.mu.Unlock()
	if _, ok := a.courier.pending[courierKey("done-pid", procEventExit)]; ok {
		t.Error("exit absent from AcceptedProcesses must clear (the broker settled it)")
	}
	if _, ok := a.courier.pending[courierKey("still-running-pid", procEventExit)]; !ok {
		t.Error("exit still listed as accepted-RUNNING must be kept for retry")
	}
	if _, ok := a.courier.pending[courierKey("live-pid", procEventStarted)]; ok {
		t.Error("started with accepted pid must clear")
	}
	if _, ok := a.courier.pending[courierKey("unheard-pid", procEventStarted)]; !ok {
		t.Error("started the broker never acknowledged must be kept")
	}
}

// TestCourierKeepsExitWhenBrokerStillCallsItRunning is the agent half of the
// h1 external review's F1 (Blocker). The broker now reports a pid whose exit
// WRITE FAILED as still-accepted (the row really is still RUNNING), and this
// pins that the courier reads that correctly: the pending exit — the only
// copy of the real rc — must SURVIVE for another attempt. Before F1 the
// broker said nothing in that case and this entry was deleted.
// origin: docs/reviews/h1-external-review.md F1.
func TestCourierKeepsExitWhenBrokerStillCallsItRunning(t *testing.T) {
	url := startNATS(t)
	a := courierAgent(t, url)
	a.courier.enqueueExit("write-failed-pid", 42)

	a.courier.onRegisterSuccess(a.courier.registerReceipt(), a.courier.pendingExitSnapshot(), proto.NodeRegisterResp{
		OK: true,
		// The broker's exit write failed, so it still believes the pid runs.
		AcceptedProcesses: []string{"write-failed-pid"},
	})

	a.courier.mu.Lock()
	ev, still := a.courier.pending[courierKey("write-failed-pid", procEventExit)]
	a.courier.mu.Unlock()
	if !still {
		t.Fatal("courier discarded the pending exit for a pid the broker still reports RUNNING — " +
			"the real exit code is now unrecoverable and the row stays RUNNING forever (F1)")
	}
	if ev.rc != 42 {
		t.Fatalf("retained entry lost its rc: %d, want 42", ev.rc)
	}
}

// TestCourierSnapshotCarriesPendingExits pins the replay channel's send half:
// pending exits ride the register snapshot as State:"exited" rows with the
// real rc and the true end instant.
func TestCourierSnapshotCarriesPendingExits(t *testing.T) {
	url := startNATS(t)
	a := courierAgent(t, url)
	before := time.Now().UTC().Add(-time.Second)
	a.courier.enqueueExit("pid-9", 3)

	rows := a.courier.pendingExitSnapshot()
	if len(rows) != 1 {
		t.Fatalf("snapshot rows = %d, want 1", len(rows))
	}
	row := rows[0]
	if row.PID != "pid-9" || row.State != "exited" || row.RC == nil || *row.RC != 3 {
		t.Fatalf("snapshot row wrong: %+v", row)
	}
	if row.EndedAt == nil || row.EndedAt.Before(before) || row.EndedAt.After(time.Now().UTC().Add(time.Second)) {
		t.Fatalf("snapshot ended_at wrong: %v", row.EndedAt)
	}
}

// origin: hot-update S2 plan §3.5 / §5.1 — TestCourierOutboxSurvivesImage:
// the outbox crosses an image switch with every entry's bookkeeping, the
// register-facing views (started/exit snapshots, receipt keys) unchanged,
// backoff reset so the first round retries, per-pid ordering intact
// (started still delivered before the same pid's exit) and a parked exit
// still never re-sent. The M2 mutation (ExportOutbox skipping exits) makes
// the exit snapshot differ.
func TestCourierOutboxSurvivesImage(t *testing.T) {
	url := startNATS(t)
	r := startAckResponder(t, url)
	r.mode.Store("silent")
	old := courierRequestTimeout
	courierRequestTimeout = 150 * time.Millisecond
	t.Cleanup(func() { courierRequestTimeout = old })
	a := courierAgent(t, url)
	startedAt := time.Now().UTC().Truncate(time.Microsecond)
	a.courier.enqueueStarted("pid-a", []string{"x"}, "SHA256:u", "boot", 77, startedAt)
	a.courier.enqueueExit("pid-a", 4)
	a.courier.enqueueExit("pid-b", 5)
	a.courier.enqueueExit("pid-c", 6)
	a.courier.mu.Lock()
	a.courier.pending[courierKey("pid-b", procEventExit)].parked = true
	a.courier.pending[courierKey("pid-b", procEventExit)].silent = courierParkAfter
	a.courier.pending[courierKey("pid-c", procEventExit)].fails = 2
	a.courier.pending[courierKey("pid-c", procEventExit)].next = time.Now().Add(time.Hour)
	a.courier.mu.Unlock()
	if MaxCourier := agentimage.MaxCourierEvents; MaxCourier != courierMaxPending {
		t.Fatalf("agentimage.MaxCourierEvents=%d must equal courierMaxPending=%d", MaxCourier, courierMaxPending)
	}
	if agentimage.MaxCourierSilent != courierParkAfter {
		t.Fatalf("agentimage.MaxCourierSilent=%d must equal courierParkAfter=%d", agentimage.MaxCourierSilent, courierParkAfter)
	}
	// Check is the pure half: it refuses what ImportOutbox would, without a courier.
	if err := NewCourierImage(a).Check(agentimage.CourierState{Version: agentimage.CourierStateVersion,
		Events: []agentimage.CourierEvent{{PID: "pid-z", Kind: "exit", Payload: json.RawMessage(`{"pid":"pid-z","exit_code":1}`), EnqueuedAtUnixNano: 1, RC: 2}}}); err == nil {
		t.Fatal("Check accepted an exit whose payload disagrees with its rc")
	}
	exported := NewCourierImage(a)
	st, err := exported.ExportOutbox()
	if err != nil {
		t.Fatal(err)
	}
	if len(st.Events) != 4 {
		t.Fatalf("exported %d events, want 4", len(st.Events))
	}
	data, err := json.Marshal(st)
	if err != nil {
		t.Fatal(err)
	}
	var back agentimage.CourierState
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatal(err)
	}
	next := courierAgent(t, url)
	if err := NewCourierImage(next).ImportOutbox(back); err != nil {
		t.Fatal(err)
	}
	// Register-facing views are identical.
	sortRows := func(rows []proto.LocalProcess) []proto.LocalProcess {
		sort.Slice(rows, func(i, j int) bool { return rows[i].PID < rows[j].PID })
		return rows
	}
	if got, want := sortRows(next.courier.pendingExitSnapshot()), sortRows(a.courier.pendingExitSnapshot()); !reflect.DeepEqual(got, want) {
		t.Fatalf("exit snapshot differs after the image:\n got %+v\nwant %+v", got, want)
	}
	if got, want := sortRows(next.courier.pendingStartedSnapshot()), sortRows(a.courier.pendingStartedSnapshot()); !reflect.DeepEqual(got, want) {
		t.Fatalf("started snapshot differs after the image:\n got %+v\nwant %+v", got, want)
	}
	if got, want := next.courier.registerReceipt(), a.courier.registerReceipt(); len(got) != len(want) {
		t.Fatalf("receipt keys differ: %d vs %d", len(got), len(want))
	} else {
		for key, ev := range want {
			g, ok := got[key]
			if !ok || g.fails != ev.fails || g.silent != ev.silent || g.parked != ev.parked || g.rc != ev.rc ||
				!g.endedAt.Equal(ev.endedAt) || !g.startedAt.Equal(ev.startedAt) || g.startTimeTicks != ev.startTimeTicks ||
				!bytes.Equal(g.payload, ev.payload) || !g.enqueuedAt.Equal(ev.enqueuedAt) {
				t.Fatalf("entry %s differs after the image:\n got %+v\nwant %+v", key, g, ev)
			}
			if g.next.After(time.Now().Add(time.Second)) {
				t.Fatalf("entry %s kept its old backoff instant across the image: %v", key, g.next)
			}
		}
	}
	// Delivery after the switch: started(a) goes first, exit(a) waits for it,
	// exit(c) is due at once (backoff reset), exit(b) is parked and never sent.
	next.courier.deliverRound(context.Background())
	if s, e := r.starts.Load(), r.exits.Load(); s != 1 || e != 1 {
		t.Fatalf("first round after the image: started=%d exits=%d, want 1 (pid-a started) / 1 (pid-c exit)", s, e)
	}
	// A second import into a non-empty courier is refused and changes nothing.
	if err := NewCourierImage(next).ImportOutbox(back); err == nil {
		t.Fatal("import into a non-empty outbox succeeded")
	}
	if len(next.courier.registerReceipt()) != 4 {
		t.Fatal("refused import changed the outbox")
	}
}

// origin: hot-update S2 plan §3.5 — TestCourierImportRejectsInconsistentEntries:
// the derived fields the register snapshot reads must agree with the
// payload the broker will decode; every disagreement and every malformed
// shape is refused with the outbox untouched.
func TestCourierImportRejectsInconsistentEntries(t *testing.T) {
	url := startNATS(t)
	a := courierAgent(t, url)
	endedAt := time.Now().UTC()
	a.courier.enqueueExit("pid-x", 9)
	a.courier.enqueueStarted("pid-y", []string{"x"}, "SHA256:u", "boot", 5, endedAt)
	good, err := NewCourierImage(a).ExportOutbox()
	if err != nil {
		t.Fatal(err)
	}
	for name, edit := range map[string]func(*agentimage.CourierState){
		"rc-disagrees":     func(s *agentimage.CourierState) { s.Events[0].RC = 10 },
		"ended-disagrees":  func(s *agentimage.CourierState) { s.Events[0].EndedAtUnixNano++ },
		"pid-disagrees":    func(s *agentimage.CourierState) { s.Events[0].PID = "pid-z" },
		"ticks-disagree":   func(s *agentimage.CourierState) { s.Events[1].StartTimeTicks = 6 },
		"parked-started":   func(s *agentimage.CourierState) { s.Events[1].Parked = true },
		"duplicate-key":    func(s *agentimage.CourierState) { s.Events = append(s.Events, s.Events[0]) },
		"unknown-kind":     func(s *agentimage.CourierState) { s.Events[0].Kind = "reaped" },
		"payload-not-json": func(s *agentimage.CourierState) { s.Events[0].Payload = json.RawMessage(`{`) },
		"too-many": func(s *agentimage.CourierState) {
			for i := 0; i <= courierMaxPending; i++ {
				s.Events = append(s.Events, agentimage.CourierEvent{PID: fmt.Sprintf("p%d", i), Kind: "exit", Payload: json.RawMessage(`{}`), EnqueuedAtUnixNano: 1})
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			bad := good
			bad.Events = append([]agentimage.CourierEvent(nil), good.Events...)
			edit(&bad)
			target := courierAgent(t, url)
			if err := NewCourierImage(target).ImportOutbox(bad); err == nil {
				t.Fatalf("%s imported", name)
			}
			if n := len(target.courier.registerReceipt()); n != 0 {
				t.Fatalf("%s: refused import left %d entries", name, n)
			}
		})
	}
	// The intact export imports.
	target := courierAgent(t, url)
	if err := NewCourierImage(target).ImportOutbox(good); err != nil {
		t.Fatal(err)
	}
}

// origin: hot-update S3 plan §3.4 T10. An imported exit and still-pending
// start can arrive in the same register. An unknown pid is not a settled exit;
// only a receipt that carried an acknowledged start can retire its exit.
func TestRegisterReceiptSettlesOnlyCarriedEvents(t *testing.T) {
	a, err := New(Config{NATSURL: "nats://127.0.0.1:1", SID: "lab", NID: "node"})
	if err != nil {
		t.Fatal(err)
	}
	a.courier.enqueueStarted("unknown", []string{"work"}, "SHA256:u", "boot", 17, time.Now().UTC())
	a.courier.enqueueExit("unknown", 23)
	a.courier.enqueueExit("settled", 7)
	state := exportCourierState(a.courier)
	c := newProcCourier(a)
	if err := importCourierState(c, state); err != nil {
		t.Fatal(err)
	}
	receipt := c.registerReceipt()
	rows := c.pendingExitSnapshot()
	// Enqueued after request construction: absent from this receipt even
	// though the next request will represent it.
	c.enqueueExit("late", 41)
	c.onRegisterSuccess(receipt, rows, proto.NodeRegisterResp{OK: true})
	if receiptPending(t, c, "unknown", procEventExit) == nil || receiptPending(t, c, "unknown", procEventStarted) == nil {
		t.Fatal("register erased an exit whose start the broker never acknowledged")
	}
	if receiptPending(t, c, "late", procEventExit) == nil {
		t.Fatal("register settled an event it never carried")
	}
	if receiptPending(t, c, "settled", procEventExit) != nil {
		t.Fatal("register failed to settle the represented, already-known exit")
	}
	// A positive started ACK removes that obstacle; the next register can
	// then settle exactly the original rc/ended-at pair.
	c.mu.Lock()
	delete(c.pending, courierKey("unknown", procEventStarted))
	c.mu.Unlock()
	c.onRegisterSuccess(c.registerReceipt(), c.pendingExitSnapshot(), proto.NodeRegisterResp{OK: true})
	if receiptPending(t, c, "unknown", procEventExit) != nil {
		t.Fatal("known exit did not settle after its start was acknowledged")
	}
}

// origin: hot-update S3 plan §3.4 / concurrency review CC-3. A start ACK
// overtaking a register reply cannot make that older request know the PID.
// Real NATS requests fix the ordering; broker reconciliation is represented
// by its empty reply for an unknown exited row (reconcile.go).
func TestRegisterReceiptRetainsExitWhenStartAckArrivesAfterSnapshot(t *testing.T) {
	gBefore, fdBefore := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		for _, settlement := range []string{"exit-ack", "next-register"} {
			t.Run(fmt.Sprintf("%s/%d", settlement, round), func(t *testing.T) {
				f := newReceiptFixture(t)
				c := f.a.courier
				const pid = "receipt-race"
				c.enqueueStarted(pid, []string{"work"}, "SHA256:u", "boot", 17, time.Now().UTC())
				c.enqueueExit(pid, 23)
				exit := receiptPending(t, c, pid, procEventExit)
				done := receiptLaunchRegister(t, f)
				register, req := receiptNextRegister(t, f)
				if len(req.LocalProcesses) != 1 || req.LocalProcesses[0].PID != pid ||
					req.LocalProcesses[0].State != "exited" || req.LocalProcesses[0].RC == nil ||
					*req.LocalProcesses[0].RC != 23 || req.LocalProcesses[0].EndedAt == nil ||
					!req.LocalProcesses[0].EndedAt.Equal(exit.endedAt) {
					t.Fatalf("register did not carry the real exit: %+v", req.LocalProcesses)
				}
				if receiptPending(t, c, pid, procEventStarted) == nil {
					t.Fatal("start was already acknowledged before the register snapshot")
				}
				// The broker evaluated the exited row while the PID was unknown,
				// then its reply was delayed until after the started ACK landed.
				oldReply := proto.NodeRegisterResp{OK: true}
				receiptDeliverOne(t, f, pid, procEventStarted, 0)
				receiptRespond(t, register, oldReply)
				receiptWaitRegister(t, done)
				if ev := receiptPending(t, c, pid, procEventExit); ev != exit || ev.rc != 23 {
					t.Fatal("stale register reply erased or altered exit rc=23 after a later started ACK")
				}
				if settlement == "exit-ack" {
					// Observe the retained exit's actual payload and positive ACK.
					receiptDeliverOne(t, f, pid, procEventExit, 23)
					return
				}
				// A new request knows the start landed. Its otherwise identical
				// empty reply can now settle the exact same exit.
				done = receiptLaunchRegister(t, f)
				register, next := receiptNextRegister(t, f)
				if !reflect.DeepEqual(next.LocalProcesses, req.LocalProcesses) {
					t.Fatalf("next register changed the retained exit: %+v", next.LocalProcesses)
				}
				receiptRespond(t, register, proto.NodeRegisterResp{OK: true})
				receiptWaitRegister(t, done)
				if receiptPending(t, c, pid, procEventExit) != nil {
					t.Fatal("next register failed to settle the exit after the start was acknowledged")
				}
			})
		}
	}
	testharness.AssertNoGoroutineLeak(t, "register start ACK ordering x5", gBefore)
	testharness.AssertNoFDLeak(t, "register start ACK ordering x5", fdBefore, 0)
}
