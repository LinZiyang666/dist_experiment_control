package infrarepair

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/drillobserve"
	"github.com/nats-io/nats.go"
)

// origin: hot-update S9 internal review s9-instr4 Request 2
func TestRepairObservationExportsFourOriginalPrefixesBeforeRelease(t *testing.T) {
	f := newRecovery(t)
	previous := drillobserve.Hook
	drillobserve.Hook = func(drillobserve.Event) {}
	defer func() { drillobserve.Hook = previous }()
	for i := range f.original.ACKs {
		f.original.ACKs[i].NUID = "" // Existing fixture audit rows are not transfer chunks.
	}
	for transfer := 0; transfer < 4; transfer++ {
		for index := 0; index < 2; index++ {
			id := fmt.Sprintf("original-%d", transfer)
			msg := &nats.Msg{Subject: fmt.Sprintf("chunks.%s.%d", id, index), Data: []byte(fmt.Sprintf("original bytes %s/%d", id, index))}
			ack, err := f.js.PublishMsg(t.Context(), msg)
			if err != nil {
				t.Fatal(err)
			}
			f.original.ACKs = append(f.original.ACKs, ACK{Account: "business", Stream: ack.Stream, Subject: msg.Subject,
				TransferID: id, NUID: "object-" + id, Index: uint64(index), Sequence: ack.Sequence, Message: MessageDigest(msg.Header, msg.Data)})
		}
	}
	cp, err := f.gate.Seal(f.plan, recoveryDeadline(t))
	if err != nil {
		t.Fatal(err)
	}
	var events []drillobserve.Event
	drillobserve.Hook = func(e drillobserve.Event) {
		if e.Kind == "infra-prefix" {
			if _, err := f.gate.Enter("transfer"); err != ErrFenced {
				t.Error("repair reopened before prefix export")
			}
			events = append(events, e)
		}
	}
	if err := f.gate.VerifyBeforeReplay(cp, recoveryDeadline(t), nil); err != nil {
		t.Fatal(err)
	}
	if len(events) != 4 {
		t.Fatalf("four original ACKed prefixes were not exported under the fence: got %d", len(events))
	}
	for _, e := range events {
		if e.Actor != f.plan.Tx || e.Values[2] != uint64(f.plan.Step) {
			t.Fatal("prefix escaped its actual transaction step")
		}
		var row struct {
			TransferID       string `json:"transfer_id"`
			At               uint64 `json:"at_ns"`
			Before           uint64 `json:"before_repair_ns"`
			Expected, Actual []struct {
				Index, Length uint64
				SHA256        string
			}
		}
		if err := json.Unmarshal(e.Data, &row); err != nil {
			t.Fatal(err)
		}
		if !strings.HasPrefix(row.TransferID, "original-") || row.At == 0 || row.At > row.Before || len(row.Expected) != 2 || len(row.Actual) != 2 {
			t.Fatalf("incomplete original prefix: %s", e.Data)
		}
		for i := range row.Expected {
			if row.Expected[i] != row.Actual[i] || row.Expected[i].Index != uint64(i) || row.Expected[i].Length == 0 || len(row.Expected[i].SHA256) != 64 {
				t.Fatalf("original prefix bytes or chunk order lost: %s", e.Data)
			}
		}
	}
	if err := f.gate.Release(cp, recoveryDeadline(t)); err != nil {
		t.Fatal(err)
	}
}

// origin: hot-update S9 plan §4.3 / T40 / M20; s9-instr4 Request 2
func TestRepairObservationKeepsOriginalExpectedBytesAfterLoss(t *testing.T) {
	f := newRecovery(t)
	var events []drillobserve.Event
	previous := drillobserve.Hook
	drillobserve.Hook = func(e drillobserve.Event) { events = append(events, e) }
	defer func() { drillobserve.Hook = previous }()
	for index := 0; index < 3; index++ {
		f.original.ACKs[index].TransferID = "original-transfer"
	}
	cp := sealRecovery(t, f)
	stream, err := f.js.Stream(t.Context(), "chunks")
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.DeleteMsg(t.Context(), f.original.ACKs[1].Sequence); err != nil {
		t.Fatal(err)
	}
	if err := f.gate.VerifyBeforeReplay(cp, recoveryDeadline(t), nil); !errors.Is(err, ErrReceipt) {
		t.Fatalf("lost ACKed chunk accepted: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("first independent loss read missing: %d", len(events))
	}
	var row prefixObservation
	if err := json.Unmarshal(events[0].Data, &row); err != nil {
		t.Fatal(err)
	}
	expected := sha256.Sum256([]byte("acknowledged chunks.original.2"))
	if row.TransferID != "original-transfer" || len(row.Expected) != 3 || row.Expected[1].SHA256 != hex.EncodeToString(expected[:]) || len(row.Actual) != 2 || row.Actual[0].Index != 0 || row.Actual[1].Index != 2 {
		t.Fatalf("post-loss data replaced the original prefix: %s", events[0].Data)
	}
	// Even an external replay cannot alter the first exported expected row or
	// clear the sticky loss. Repair through the product remains fenced.
	if _, err := f.js.Publish(t.Context(), f.original.ACKs[1].Subject, []byte("acknowledged chunks.original.2")); err != nil {
		t.Fatal(err)
	}
	if err := f.gate.VerifyBeforeReplay(cp, recoveryDeadline(t), nil); !errors.Is(err, ErrReceipt) {
		t.Fatalf("replay hid original loss: %v", err)
	}
	if err := f.gate.Release(cp, recoveryDeadline(t)); !errors.Is(err, ErrFenced) {
		t.Fatalf("lost prefix opened repair: %v", err)
	}
	if len(events) != 1 {
		t.Fatal("a post-repair observation replaced the first read")
	}
}

// origin: hot-update S9 plan §4.3 / T40 / M20; s9-instr4 Request 2
func TestRepairObservationHoldsOperationUntilExportReturns(t *testing.T) {
	f := newRecovery(t)
	for index := 0; index < 3; index++ {
		f.original.ACKs[index].TransferID = "original-transfer"
	}
	entered, release := make(chan struct{}), make(chan struct{})
	previous := drillobserve.Hook
	drillobserve.Hook = func(e drillobserve.Event) {
		if e.Kind == drillobserve.InfrastructurePrefixKind {
			close(entered)
			<-release
		}
	}
	defer func() { drillobserve.Hook = previous }()
	cp := sealRecovery(t, f)
	done := make(chan error, 1)
	go func() { done <- f.gate.VerifyBeforeReplay(cp, recoveryDeadline(t), nil) }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		close(release)
		<-done
		t.Fatal("prefix export did not run")
	}
	d, err := controlbudget.After(25 * time.Millisecond)
	if err != nil {
		close(release)
		<-done
		t.Fatal(err)
	}
	err = f.gate.Release(cp, d)
	close(release)
	verifyErr := <-done
	if !errors.Is(err, controlbudget.ErrExpired) {
		t.Fatalf("Release did not queue behind the export fence: %v", err)
	}
	if verifyErr != nil {
		t.Fatal(verifyErr)
	}
	if err := f.gate.Release(cp, recoveryDeadline(t)); err != nil {
		t.Fatal(err)
	}
}
