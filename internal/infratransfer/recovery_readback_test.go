package infratransfer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"runtime"
	"testing"

	i "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/infrarepair"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/xferbucket"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats.go/jetstream"
)

// The capable writer overlaps publish with readback. Exercise the recovery port
// directly to keep the original zero-ACK assertion at the actual no-read boundary,
// then require a verified original read to add exactly that obligation.
// origin: S10 internal review E2E-PARALLEL-REAL-FAILS F2; S9 plan §4.3 / T40 / T42
func TestTransferRecoveryDistinguishesUnknownPublishFromReadback(t *testing.T) {
	goroutines, fds := runtime.NumGoroutine(), testharness.FDCount()
	defer testharness.AssertNoGoroutineLeak(t, "unknown transfer ACK readback", goroutines)
	defer testharness.AssertNoFDLeak(t, "unknown transfer ACK readback", fds, 0)
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseUnknownACKReadback(t) })
	}
}

func exerciseUnknownACKReadback(t *testing.T) {
	s := newServer(t)
	js := connect(t, s)
	body := bytes.Repeat([]byte("original"), 4096)
	spec := specification(t, js, body)
	// This test exercises admission and readback, not infrastructure sealing.
	// Any accidental attempt to seal must fail rather than invent asset evidence.
	gate, err := infrarepair.New(infrarepair.Config{
		Domain: i.ID{2}, Required: []string{"transfer:" + spec.Identity.TransferID},
		Accounts:  map[string]jetstream.JetStream{"$G": js},
		Exclusive: func(i.Deadline, i.Stop) error { return i.ErrUnsupported },
		Assets: func(i.Deadline, i.Stop) (i.AssetSeal, error) {
			return i.AssetSeal{}, i.ErrUnsupported
		},
		Transfers: func(i.Deadline) (i.TransferSeal, error) {
			return i.TransferSeal{}, i.ErrUnsupported
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	port, err := RecoveryWriterJS(&lostACK{JetStream: js, subject: xferbucket.ChunkSubject(spec)}, spec, gate, "$G",
		func(i.Deadline, i.Stop) error { js.Conn().Close(); return nil })
	if err != nil {
		t.Fatal(err)
	}
	attempt := x.PendingAttempt{Bytes: body, Length: uint32(len(body)), SHA256: sha256.Sum256(body), AttachmentGeneration: 1}
	message := xferbucket.ChunkMessage(spec, attempt)
	if ack, err := port.PublishMsg(t.Context(), message, jetstream.WithRetryAttempts(0)); ack != nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("original publish ACK was not lost: ack=%+v err=%v", ack, err)
	}
	captured, err := port.(*recoveryWriterJS).capture(deadline(t))
	if err != nil || len(captured.ACKs) != 0 {
		t.Fatal("unknown request forged a definite ACK")
	}
	// The management read proves the request really landed, but cannot update
	// the producer's ledger. Only the original port's verified read can do that.
	stream, err := js.Stream(t.Context(), spec.Stream)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := stream.GetMsg(t.Context(), 1)
	if err != nil || xferbucket.CheckChunk(spec, xferbucket.PendingReceipt(attempt, 1), raw) != nil {
		t.Fatalf("original effect is missing: %v", err)
	}
	captured, err = port.(*recoveryWriterJS).capture(deadline(t))
	if err != nil || len(captured.ACKs) != 0 {
		t.Fatal("management read forged an original ACK")
	}
	original, err := port.Stream(t.Context(), spec.Stream)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = original.GetMsg(t.Context(), raw.Sequence); err != nil {
		t.Fatal(err)
	}
	captured, err = port.(*recoveryWriterJS).capture(deadline(t))
	want := infrarepair.ACK{TransferID: spec.Identity.TransferID, Account: "$G", Stream: spec.Stream, Subject: raw.Subject,
		NUID: spec.Identity.ObjectVersion, Sequence: raw.Sequence, Message: infrarepair.MessageDigest(message.Header, message.Data)}
	if err != nil || len(captured.ACKs) != 1 || captured.ACKs[0] != want {
		t.Fatalf("original readback lost its exact ACK obligation: err=%v got=%+v want=%+v", err, captured.ACKs, want)
	}
}
