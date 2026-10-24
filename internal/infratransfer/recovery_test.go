package infratransfer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"testing"

	"github.com/LinZiyang666/tether/internal/drillobserve"
	i "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/infrarepair"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go/jetstream"
)

// origin: S9 internal review NC-EXPORTS handoff P18/P19; plan §4.3 / T40 / T42
func TestTransferRecoveryCapturesOriginalACKsBeforeSeal(t *testing.T) {
	goroutines, fds := runtime.NumGoroutine(), testharness.FDCount()
	defer testharness.AssertNoGoroutineLeak(t, "original transfer ACKs", goroutines)
	defer testharness.AssertNoFDLeak(t, "original transfer ACKs", fds, 0)
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseOriginalACKs(t) })
	}
}

func exerciseOriginalACKs(t *testing.T) {
	s := newServer(t)
	js := connect(t, s)
	body := bytes.Repeat([]byte("original"), 7000)
	spec := specification(t, js, body)
	domain, tx, d := i.ID{2}, i.ID{3}, i.Digest{4}
	required := []string{"transfer:original0", "transfer:original1", "transfer:original2", "transfer:original3"}
	gate, err := infrarepair.New(infrarepair.Config{Domain: domain, Required: required, Accounts: map[string]jetstream.JetStream{"$G": js},
		Exclusive: func(i.Deadline, i.Stop) error { return nil },
		Assets: func(i.Deadline, i.Stop) (i.AssetSeal, error) {
			return i.AssetSeal{Revision: 1, Accounts: d, Assets: d, Participants: d, Complete: true}, nil
		},
		Transfers: func(i.Deadline) (i.TransferSeal, error) {
			return i.TransferSeal{Version: 1, SourceSHA256: d, SchemaSHA256: d, InvocationSet: d, ReceiptSet: d, ConsumerSet: d, Complete: true}, nil
		}})
	if err != nil {
		t.Fatal(err)
	}
	// Custody records are emitted by the actual Gate.VerifyBeforeReplay, not
	// by the test's management reads. The fixture owns all embedded writers.
	oldHook := drillobserve.Hook
	var events []drillobserve.Event
	drillobserve.Hook = func(e drillobserve.Event) { events = append(events, e) }
	defer func() { drillobserve.Hook = oldHook }()
	var first uint64
	var originalUnknown jetstream.JetStream
	var bound []jetstream.JetStream
	for index := 0; index < 4; index++ {
		original := spec
		original.Identity.TransferID = fmt.Sprint("original", index)
		original.Identity.ObjectVersion = fmt.Sprint("NUID", index)
		initial := connect(t, s)
		backend := initial
		if index == 0 {
			originalUnknown = initial
			backend = &lostACK{JetStream: initial, subject: "$O." + original.Bucket + ".C." + original.Identity.ObjectVersion}
		}
		port, err := RecoveryWriterJS(backend, original, gate, "$G", func(i.Deadline, i.Stop) error { initial.Conn().Close(); return nil })
		if err != nil {
			t.Fatal(err)
		}
		bound = append(bound, port)
		writer, err := jsstream.NewTransferWriter(port, original)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := writer.Stop(deadline(t)); err != nil {
				t.Error(err)
			}
		})
		for chunk := uint32(0); chunk < 2; chunk++ {
			start, end := int(chunk*original.ChunkSize), min(len(body), int((chunk+1)*original.ChunkSize))
			receipt, err := writer.Append(chunk, body[start:end], deadline(t))
			if index == 0 && chunk == 0 {
				if err == nil {
					t.Fatal("lost-reply lane did not lose the original ACK")
				}
				// S7 joins its speculative original-message read before Append
				// returns, so that read may already have confirmed the lost ACK.
				// The no-read boundary is independently pinned by
				// TestTransferRecoveryDistinguishesUnknownPublishFromReadback.
				// origin: S10 internal review E2E-PARALLEL-REAL-FAILS F2; S9 plan §4.3
				var found bool
				receipt, found, err = writer.ResolvePending(deadline(t))
				if err != nil || !found {
					t.Fatalf("original lost ACK was not resolved: %v", err)
				}
				captured, e := port.(*recoveryWriterJS).capture(deadline(t))
				if e != nil || len(captured.ACKs) != 1 || captured.ACKs[0].Sequence != receipt.StreamSeq {
					t.Fatal("definite original readback never entered the ACK ledger")
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			// Cancelling an SDK read belongs to that operation. It cannot
			// retire the original writer, lose its ACKs or poison later reads.
			// origin: S10 internal review ARCH-REDS-2 F2
			stream, err := port.Stream(t.Context(), original.Stream)
			if err != nil {
				t.Fatal(err)
			}
			cancelled, cancel := context.WithCancel(t.Context())
			cancel()
			if _, err = stream.GetMsg(cancelled, receipt.StreamSeq); !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled read did not retain operation cancellation: %v", err)
			}
			if _, err = stream.GetMsg(t.Context(), receipt.StreamSeq); err != nil {
				t.Fatalf("operation cancellation retired the original writer: %v", err)
			}
			if index == 0 && chunk == 0 {
				first = receipt.StreamSeq
			}
			if chunk == 0 {
				// Rebind to a genuinely different data NC while retaining the
				// producer registered against the original ACK ledger.
				state, err := writer.Freeze(deadline(t))
				if err != nil {
					t.Fatal(err)
				}
				next := connect(t, s)
				current, err := RecoveryWriterCurrent(port, func(i.Deadline) (jetstream.JetStream, error) { return next, nil })
				if err != nil {
					t.Fatal(err)
				}
				successor, err := current(deadline(t))
				if err != nil {
					t.Fatal(err)
				}
				oldCID, _ := port.Conn().GetClientID()
				newCID, _ := successor.Conn().GetClientID()
				if oldCID == newCID {
					t.Fatal("successor reused original data NC")
				}
				if err = writer.Stop(deadline(t)); err != nil {
					t.Fatal(err)
				}
				writer, err = jsstream.ImportTransferWriter(successor, state)
				if err != nil {
					t.Fatal(err)
				}
				if err = jsstream.ThawTransferWriter(writer, deadline(t)); err != nil {
					t.Fatal(err)
				}
			}
		}
		if _, err = writer.Finalize(deadline(t)); err != nil {
			t.Fatal(err)
		}
		responsibilities, err := port.(*recoveryWriterJS).capture(deadline(t))
		if err != nil || len(responsibilities.ACKs) != 2 {
			t.Fatalf("original chunk ACK inventory: %v %#v", err, responsibilities)
		}
		for chunk, ack := range responsibilities.ACKs {
			if ack.TransferID != original.Identity.TransferID || ack.NUID != original.Identity.ObjectVersion || ack.Index != uint64(chunk) || ack.Message == (i.Digest{}) {
				t.Fatal("original ACK lost invocation/object/message binding")
			}
		}
		responsibilities.ACKs[0].TransferID = "caller-mutated"
	}
	_, err = gate.Seal(i.Plan{Tx: tx, Step: 7, Domain: domain, Component: i.NATS}, deadline(t))
	if !errors.Is(err, infrarepair.ErrUnknown) {
		t.Fatalf("lost ACK cleared transport fence without retirement: %v", err)
	}
	if err = infrarepair.RetireUnknownProducers(gate, deadline(t), nil); err != nil {
		t.Fatal(err)
	}
	if !originalUnknown.Conn().IsClosed() {
		t.Fatal("sent-unknown transport was not retired")
	}
	cp, err := gate.Seal(i.Plan{Tx: tx, Step: 7, Domain: domain, Component: i.NATS}, deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	if err = gate.VerifyBeforeReplay(cp, deadline(t), nil); err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, event := range events {
		if event.Kind != drillobserve.InfrastructurePrefixKind {
			continue
		}
		var row struct {
			TransferID       string `json:"transfer_id"`
			Expected, Actual []json.RawMessage
		}
		if err = json.Unmarshal(event.Data, &row); err != nil {
			t.Fatal(err)
		}
		if event.Actor != tx || event.Values[2] != 7 || row.TransferID == "" || len(row.Expected) != 2 || !bytes.Equal(mustJSON(t, row.Expected), mustJSON(t, row.Actual)) {
			t.Fatal("four original prefixes did not reach custody")
		}
		count++
	}
	if count != 4 {
		t.Fatalf("four original prefixes: got %d", count)
	}
	blocked, err := bound[1].Stream(t.Context(), spec.Stream)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = blocked.GetMsg(t.Context(), first); !errors.Is(err, infrarepair.ErrFenced) {
		t.Fatalf("sealed writer read could register repaired evidence: %v", err)
	}
	// Delete a definite original ACK while repair admission is closed. A later
	// metadata ACK or stream-wide LastSeq cannot make the missing chunk pass.
	stream, err := js.Stream(t.Context(), spec.Stream)
	if err != nil {
		t.Fatal(err)
	}
	if err = stream.DeleteMsg(t.Context(), first); err != nil {
		t.Fatal(err)
	}
	if err = gate.VerifyBeforeReplay(cp, deadline(t), nil); !errors.Is(err, infrarepair.ErrReceipt) {
		t.Fatalf("lost original ACK accepted: %v", err)
	}
	_, err = bound[0].PublishMsg(t.Context(), nil)
	if err == nil {
		t.Fatal("nil publish accepted")
	}
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
