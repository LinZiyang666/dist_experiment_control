package cli

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats.go"
)

// origin: S7 internal review rv-terminal / FIX-TERM request 3
func TestTransferWriterRetirementRequiresExactAuthenticatedACK(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseWriterRetirementACK(t)
	}
	testharness.AssertNoGoroutineLeak(t, t.Name(), g)
	testharness.AssertNoFDLeak(t, t.Name(), fd, 0)
}

func exerciseWriterRetirementACK(t *testing.T) {
	t.Helper()
	s, nc, id, stop := transferConnectionFixture(t)
	defer stop()
	transport, err := NewTransferTransport(nc, s.ClientURL(), id, &CanonicalTransferWire{Actor: id.PublicKey}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := transport.Close(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	producer, ok := any(transport).(interface {
		RetireWriter(x.Identity, x.Origin, x.WriterState, x.Deadline) error
	})
	if !ok {
		t.Fatal("production transport has no writer retirement channel")
	}
	identity := x.Identity{SID: "session", NID: "node", ActorFP: id.Fingerprint, AgentInstance: "agent", TransferID: "original", EntryNonce: "entry", IntentDigest: "intent", ObjectVersion: "version"}
	origin := x.Origin{BrokerID: "broker", InstanceID: "original", BootID: "boot", Generation: 1}
	state := x.WriterState{ObjectSpec: x.ObjectSpec{Identity: identity, Bucket: "xfer-session", Stream: "OBJ_xfer-session", StreamCreated: time.Now().Format(time.RFC3339Nano)}, AttachmentGeneration: 3,
		RetirementFence: x.RetirementFence{WriterRetired: true, ExpectedStreamSeq: 11}, Receipts: []x.ChunkReceipt{{StreamSeq: 19}}, PendingAttempt: &x.PendingAttempt{ExpectedStreamSeq: 23},
		MetaReceipt: &x.ObjectReceipt{MetadataSeq: 21}}
	for _, defect := range []string{"digest", "nonce", "origin", "revision", "call", "generic", "code", "oversized", "valid", "receipt_max", "metadata_max"} {
		sending, expected, metadataSeq := state, uint64(23), uint64(21)
		if defect == "receipt_max" {
			sending.Receipts = []x.ChunkReceipt{{StreamSeq: 29}}
			expected = 29
		}
		if defect == "metadata_max" {
			metadata := *state.MetaReceipt
			metadata.MetadataSeq = 31
			sending.MetaReceipt = &metadata
			expected, metadataSeq = 31, 31
		}
		sub, err := nc.Subscribe(proto.SubjCtrlTransferFinalize(id.PublicKey, identity.SID, identity.TransferID), func(msg *nats.Msg) {
			var req proto.TransferWriterRetirementReq
			if err := proto.DecodeTransferWire(msg.Data, &req); err != nil {
				t.Error(err)
				return
			}
			if req.Writer.ExpectedStreamSeq != expected || req.Writer.PreviousMetadataSeq != metadataSeq || req.Writer.AttachmentGeneration != 3 {
				t.Error("writer retirement lost original pending/receipt/metadata expectations")
			}
			sum := sha256.Sum256(msg.Data)
			ack := proto.TransferWriterRetirementResp{CallID: req.CallID, Identity: req.Identity, Origin: req.Origin, Revision: 4, WitnessDigest: hex.EncodeToString(sum[:])}
			switch defect {
			case "digest":
				ack.WitnessDigest = "foreign"
			case "nonce":
				ack.Identity.EntryNonce = "foreign"
			case "origin":
				ack.Origin.InstanceID = "foreign"
			case "revision":
				ack.Revision = 0
			case "call":
				ack.CallID = "foreign"
			case "code":
				ack.Code = x.CodeOwnerRecovering
			}
			body, err := json.Marshal(ack)
			if err != nil {
				t.Error(err)
				return
			}
			if defect == "generic" {
				body = []byte(`{"ok":true}`)
			}
			if defect == "oversized" {
				body = append(body, []byte(strings.Repeat(" ", 4096))...)
			}
			if err = msg.Respond(body); err != nil {
				t.Error(err)
			}
		})
		if err != nil {
			t.Fatal(err)
		}
		if err = nc.FlushTimeout(time.Second); err != nil {
			t.Fatal(err)
		}
		err = producer.RetireWriter(identity, origin, sending, retryDeadline(t, time.Second))
		if unsubscribeErr := sub.Unsubscribe(); unsubscribeErr != nil {
			t.Fatal(unsubscribeErr)
		}
		if (err == nil) != (defect == "valid" || defect == "receipt_max" || defect == "metadata_max") {
			t.Fatalf("%s witness ACK validation: %v", defect, err)
		}
	}
}
