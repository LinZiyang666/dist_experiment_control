//go:build linux && infrabusiness_integration

// Copy into test/infrabusiness, or add this file with go test -overlay.
package infrabusiness_test

import (
	"fmt"
	"github.com/LinZiyang666/tether/internal/testharness"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
)

// Keep the oracle's positive fixture within the real product's rate budget.
// The original fixture uses 64 KiB/s for 1 GiB, which admission rejects.
func reviewBusinessReceipt(t *testing.T) businessReceipt {
	t.Helper()
	r := transferControls()
	for n := range r.Required {
		id := &r.Required[n]
		if id.Role != "transfer" {
			continue
		}
		id.RateLimitBPS = 1 << 20
		budget, err := proto.TransferBudget("b", id.Size, id.RateLimitBPS)
		if err != nil {
			t.Fatal(err)
		}
		id.DeadlineNS = id.StartedNS + int64(budget)
		for k := range r.Transfers {
			row := &r.Transfers[k]
			if row.TransferID == id.TransferID {
				row.RateLimitBPS, row.OriginalRate = id.RateLimitBPS, id.RateLimitBPS
				row.ByteDeadlineNS, row.OriginalDeadlineNS = id.DeadlineNS, id.DeadlineNS
			}
		}
	}
	a := authControls()
	r.Auth, r.Faults = a.Auth, a.Faults
	if err := checkMixed(r); err != nil {
		t.Fatalf("positive mixed control: %v", err)
	}
	if err := checkTransfers(r); err != nil {
		t.Fatalf("positive transfer control: %v", err)
	}
	if err := checkAuth(r); err != nil {
		t.Fatalf("positive auth control: %v", err)
	}
	return r
}

// origin: S9 internal review rv-business F6
// CIDs are scoped to a NATS server incarnation.
func TestBusinessOracleAcceptsDistinctServersWithEqualCID(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseDistinctServersWithEqualCID(t)
	}
	testharness.AssertNoGoroutineLeak(t, "distinct servers with equal CID x5", g)
	testharness.AssertNoFDLeak(t, "distinct servers with equal CID x5", fd, 0)
}

func exerciseDistinctServersWithEqualCID(t *testing.T) {
	a, b := embedded(t), embedded(t)
	defer func() { a.Shutdown(); a.WaitForShutdown() }()
	defer func() { b.Shutdown(); b.WaitForShutdown() }()
	left, err := nats.Connect(a.ClientURL(), nats.NoReconnect(), nats.Timeout(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer left.Close()
	right, err := nats.Connect(b.ClientURL(), nats.NoReconnect(), nats.Timeout(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer right.Close()
	leftCID, err := left.GetClientID()
	if err != nil {
		t.Fatal(err)
	}
	rightCID, err := right.GetClientID()
	if err != nil {
		t.Fatal(err)
	}
	if leftCID != rightCID || left.ConnectedServerId() == right.ConnectedServerId() {
		t.Fatalf("unexpected real-server control: left=%d right=%d", leftCID, rightCID)
	}
	r := reviewBusinessReceipt(t)
	row := &r.Attachments[0]
	row.Old.ServerID, row.New.ServerID = left.ConnectedServerId(), right.ConnectedServerId()
	row.Old.CID, row.New.CID = leftCID, rightCID
	row.Returned.CID = rightCID + 100
	// The stricter trace carries this observed return into the next cut.
	// Keep the original equal-CID assertion and every physical proof intact.
	r.Attachments[1].Old = row.Returned
	t.Logf("two real servers allocated the same CID=%d; raw identities and generations differ", leftCID)
	if err := checkAttachments(r); err != nil {
		t.Fatalf("healthy server migration rejected: %v", err)
	}
	routes := routeControls()
	routes[1].Old.ServerID, routes[1].New.ServerID = left.ConnectedServerId(), right.ConnectedServerId()
	routes[1].Old.CID, routes[1].New.CID = leftCID, rightCID
	if err := checkRoutes(routes); err != nil {
		t.Fatalf("healthy equal-CID route rejected: %v", err)
	}
}

// origin: S9 internal review rv-business F4
// freeze all four actual offsets for sixty seconds.
func TestBusinessOracleRejectsFrozenTransferProgress(t *testing.T) {
	r := reviewBusinessReceipt(t)
	for n := range r.Hops {
		r.Hops[n].StartNS *= 10
		r.Hops[n].CutNS *= 10
		r.Hops[n].EndNS *= 10
	}
	templates := r.Slots
	r.Slots = nil
	r.WindowNS = int64(60 * time.Second)
	for n := 0; n < 600; n++ {
		slot := templates[n%len(templates)]
		slot.Index, slot.ID = n, fmt.Sprint(n)
		slot.ScheduledNS = int64(n) * int64(100*time.Millisecond)
		slot.DispatchedNS = slot.ScheduledNS
		slot.TerminalNS = slot.ScheduledNS + int64(time.Millisecond)
		slot.Attempts = []attemptReceipt{{StartNS: slot.DispatchedNS, EndNS: slot.TerminalNS, Code: "ok", Sent: true}}
		r.Slots = append(r.Slots, slot)
	}
	for n := range r.Transfers {
		r.Transfers[n].Durable, r.Transfers[n].PreviousDurable = 32768, 32768
		// A cached summary cannot override the contradictory raw offset series.
		r.Transfers[n].MaxSilenceNS = 0
	}
	if err := checkMixed(r); err != nil {
		t.Fatalf("unrelated mixed check failed: %v", err)
	}
	if err := checkTransfers(r); err == nil {
		t.Fatal("accepted four frozen durable offsets through all six hops / 60 seconds")
	}
}

// origin: S9 internal review rv-business F5
// both sizes must run in both directions.
func TestBusinessOracleRejectsMissingTransferSizeQuadrants(t *testing.T) {
	r := reviewBusinessReceipt(t)
	for n := range r.Transfers {
		if r.Transfers[n].Size == 66549440 {
			r.Transfers[n].Direction = "push"
		} else {
			r.Transfers[n].Direction = "pull"
		}
	}
	if err := checkTransfers(r); err == nil {
		t.Fatal("accepted two small pushes and two large pulls; small pull and large push never ran")
	}
}

// origin: S9 internal review rv-business F3
// auth evidence must belong to actual cut steps.
func TestBusinessOracleRejectsForeignAuthSteps(t *testing.T) {
	r := reviewBusinessReceipt(t)
	for n := range r.Auth {
		r.Auth[n].Step = 999
	}
	if err := checkAuth(r); err == nil {
		t.Fatal("accepted auth proofs for nonexistent step 999, with none for the six actual hops")
	}
}

// origin: S9 internal review rv-business F3
// changing a row label cannot refresh physical proof.
func TestBusinessOracleRejectsReplayedAttachmentHops(t *testing.T) {
	r := reviewBusinessReceipt(t)
	for n := range r.Attachments {
		row := &r.Attachments[n]
		if row.Step <= 1 {
			continue
		}
		for _, first := range r.Attachments {
			if first.Step == 1 && first.Identity == row.Identity {
				step := row.Step
				*row = first
				row.Step = step
				break
			}
		}
	}
	if err := checkAttachments(r); err == nil {
		t.Fatal("accepted one physical move/return proof relabeled as every later hop")
	}
}
