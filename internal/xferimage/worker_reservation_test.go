package xferimage

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/xfercontract"
)

// A Tier A image retains its inline bytes once beside the entry records; the
// entry's single reservation must make that image representable. Before
// FIX-INLINE the Tier A non-replay share was a fixed 4096 bytes, so any push
// payload above about one kilobyte and every pull copy refused the cut, and a
// pull's prepared reservation (size unknown) still cannot cover its copy.
// origin: hot-update S7 plan §8.1 / §8.3 / T17 / FIX-INLINE
func TestTransferInlineReservationCoversRetainedCopy(t *testing.T) {
	for _, size := range []int{0, 1, 4096, 65537, 1 << 20, proto.XferTierAMaxBytes} {
		data := bytes.Repeat([]byte{'p'}, size)
		pull := inlinePullImage(data)
		prepared := WorkerPreparationReservation(pull.Entries[0].Prepare)
		pull.CapacityReservations[0].Reservation = WorkerInlineReservation(pull.Entries[0].Prepare, int64(size))
		if _, err := EncodeWorker(pull); err != nil {
			t.Fatalf("pull copy of %d bytes is not representable under its grown reservation: %v", size, err)
		}
		if size > 8192 { // beyond the sealed floor the pre-seal share cannot hold the copy
			pull.CapacityReservations[0].Reservation = prepared
			if _, err := EncodeWorker(pull); !errors.Is(err, ErrWorkerCapacity) {
				t.Fatalf("pull copy of %d bytes fit the pre-seal reservation: %v", size, err)
			}
		}
		push := inlinePushImage(data)
		push.CapacityReservations[0].Reservation = WorkerPreparationReservation(push.Entries[0].Prepare)
		if _, err := EncodeWorker(push); err != nil {
			t.Fatalf("push payload of %d bytes is not representable under its reservation: %v", size, err)
		}
		if got := WorkerInlineReservation(pull.Entries[0].Prepare, int64(size)); got.ReplayBytes != int64(size) || got.SealedBytes < int64(3*size) {
			t.Fatalf("inline copy is not charged to replay/sealed accounts: %+v", got)
		}
	}
	// Tier B reserves its pending chunk and codec copies of all receipts;
	// charging inline replay would account for bytes it never retains.
	b := workerFixture("sender").Entries[0].Prepare
	want := xfercontract.Reservation{NonReplayBytes: 2*(8192+2*192) + 6*int64(b.Intent.ChunkSize), SealedBytes: 8192 + int64(b.Intent.ChunkSize) + 2*192, HandoffBytes: 4096, FDs: 9, Resources: 3}
	if got := WorkerPreparationReservation(b); got != want {
		t.Fatalf("tier B reservation changed: got %+v want %+v", got, want)
	}
}

// inlinePullImage is the codec's real Tier A pull sender shape for data.
func inlinePullImage(data []byte) xfercontract.WorkerState {
	s := workerFixture("inline-sender")
	seal := s.Senders[0].SourceSeal
	seal.Size, seal.SHA256 = int64(len(data)), codecDigest(data)
	e, a := &s.Entries[0], &s.Senders[0]
	e.Prepare.Source = &seal
	e.Prepare.Budget.Size, e.Status.Budget.Size = seal.Size, seal.Size
	a.Budget, a.SourceSeal, a.Offset, a.SHA256State, a.Inline = e.Prepare.Budget, seal, seal.Size, codecHash(data), bytes.Clone(data)
	return s
}

// inlinePushImage is a Tier A push whose receiver has synced every byte.
func inlinePushImage(data []byte) xfercontract.WorkerState {
	s := workerFixture("inline")
	e, r := &s.Entries[0], &s.Receivers[0]
	e.Prepare.Inline, e.Prepare.Intent.Size, e.Prepare.Intent.SHA256 = bytes.Clone(data), int64(len(data)), codecDigest(data)
	e.Prepare.Budget.Size, e.Status.Budget.Size = int64(len(data)), int64(len(data))
	e.Prepare.Source = nil
	r.Budget = e.Prepare.Budget
	r.Checkpoint.DurableOffset, r.Checkpoint.SHA256State, r.InlineOffset = int64(len(data)), codecHash(data), int64(len(data))
	r.Atomic.DurableOffset = int64(len(data))
	e.Status.DurableOffset = int64(len(data))
	return s
}

// A Tier B pull sender's first cut retains one confirmed receipt and its one
// pending attempt: a whole chunk of bytes. The registry sizes the entry from
// the sealed geometry before that chunk (WorkerPreparationReservation at the
// sealed size, xferworker reserveSealed). The old formula omitted the pending
// chunk from the sealed share and kept a receipt-blind non-replay share, so
// every Tier B pull's first cut was refused as over capacity. At the largest
// legal object every receipt plus the pending chunk must still fit, within
// the shared whole-cut caps, at the smallest and largest chunk geometry.
// origin: S7 internal review CUT-CAPACITY F1 / hot-update S7 plan §2.1 / §8.3
func TestTransferPullReservationCoversPendingChunkAndReceiptGeometry(t *testing.T) {
	for _, chunk := range []int{32 << 10, 128 << 10, 1 << 20} {
		t.Run(fmt.Sprint(chunk), func(t *testing.T) {
			s := workerFixture("sender")
			e, a := &s.Entries[0], &s.Senders[0]
			data := bytes.Repeat([]byte{'a'}, 2*chunk)
			e.Prepare.Intent.Verb, e.Prepare.Intent.Size, e.Prepare.Intent.SHA256 = "pull", 0, ""
			e.Prepare.Intent.ChunkSize = uint32(chunk)
			a.Intent = e.Prepare.Intent
			prepared := WorkerPreparationReservation(e.Prepare)
			seal := xfercontract.SourceSeal{Size: int64(len(data)), SHA256: codecDigest(data), ChunkSize: uint32(chunk), Chunks: 2, SourceFingerprint: "pinned"}
			e.Prepare.Source, e.Prepare.Budget.Size = &seal, seal.Size
			e.Status.Budget = e.Prepare.Budget
			a.Budget, a.SourceSeal = e.Prepare.Budget, seal
			a.Offset, a.SHA256State = int64(chunk), codecHash(data[:chunk])
			a.Writer.ObjectSpec.Size, a.Writer.ObjectSpec.SHA256, a.Writer.ObjectSpec.ChunkSize = seal.Size, seal.SHA256, seal.ChunkSize
			a.Writer.Receipts[0].Length = uint32(chunk)
			a.Writer.Receipts[0].SHA256 = sha256.Sum256(data[:chunk])
			pending := a.Writer.PendingAttempt
			pending.Offset, pending.Length, pending.Bytes, pending.SHA256 = int64(chunk), uint32(chunk), data[chunk:], sha256.Sum256(data[chunk:])
			sized := e.Prepare.Intent
			sized.Size = seal.Size
			grown := e.Prepare
			grown.Intent = sized
			reservation := WorkerPreparationReservation(grown)
			s.CapacityReservations[0].Reservation = reservation
			encoded, err := EncodeWorker(s)
			if err != nil {
				t.Fatalf("first pull cut cannot retain its pending chunk under its sealed reservation: %v", err)
			}
			decoded, err := DecodeWorker(encoded)
			if err != nil || !reflect.DeepEqual(s, decoded) {
				t.Fatalf("pending chunk or synced sender checkpoint lost: %v", err)
			}
			// Even the prepared share holds that first pending chunk; it cannot
			// hold the receipts of an unknown size, which is why it grows.
			s.CapacityReservations[0].Reservation = prepared
			if _, err := EncodeWorker(s); err != nil {
				t.Fatalf("prepared reservation cannot hold the pending chunk: %v", err)
			}
			// The largest legal object, with its last chunk pending.
			chunks := (2 << 30) / chunk
			receipts := make([]xfercontract.ChunkReceipt, chunks-1)
			size := workerHeaderBytes + 8192 + chunk
			if err := measureWorkerValue(reflect.ValueOf(receipts), &size); err != nil {
				t.Fatal(err)
			}
			sized.Size = 2 << 30
			grown.Intent = sized
			largest := WorkerPreparationReservation(grown)
			if largest.SealedBytes < int64(size) || largest.NonReplayBytes < 2*int64(size)+4*int64(chunk) {
				t.Fatalf("sealed geometry underreserved its receipts: %+v encoded=%d", largest, size)
			}
			if prepared.SealedBytes >= int64(size) {
				t.Fatalf("an unknown-size pull's prepared share already covers the largest object: %+v", prepared)
			}
			if largest.SealedBytes > MaxWorkerStateBytes || largest.NonReplayBytes > 64<<20 {
				t.Fatal("reservation exceeds the shared cut caps")
			}
		})
	}
}

// A push whose prepare reply was lost to its admission deadline, or whose
// commit never came, stays an unstarted entry. The deciding broker's failed
// terminal then settles it without a courier (the exact receipt with its
// bound canonical decision), and a cancel of such an entry leaves only a
// not-entered courier. The old validator refused any unstarted entry beyond
// prepared, so the agent could not cut again until the result's retention
// expired. A gate left open, an entered outcome or an absent settlement owner
// still cannot describe an unstarted entry.
// origin: S7 internal review CUT-CAPACITY F2 / hot-update S7 plan §4 / §8.2 / T14
func TestTransferRefusedPrepareRetainsExportableSettlement(t *testing.T) {
	notEntered := xfercontract.ReceiverOutcome{Outcome: xfercontract.OutcomeNotEntered, Code: xfercontract.CodeCanceled}
	refused := func(shape string) xfercontract.WorkerState {
		s := workerFixture("courier")
		e, c := &s.Entries[0], &s.Couriers[0]
		e.Started = false
		c.Outcome, c.Canonical.Kind = notEntered, "failed"
		c.Canonical.Digest = codecDigest(c.Canonical.Bytes)
		s.CanonicalOutcomes[0].Outcome, s.CanonicalOutcomes[0].Canonical = c.Outcome, c.Canonical
		switch shape {
		case "courier-pending":
			// The cancel's courier waits for the broker's canonical bytes.
			c.Canonical, s.CanonicalOutcomes = xfercontract.CanonicalRecord{}, nil
		case "courier-acknowledged", "broker-settled":
			receipt := codecTerminal(s.CanonicalOutcomes[0])
			e.Status.State, e.Status.Phase, e.Status.Terminal = xfercontract.StatusTerminal, xfercontract.PhaseSettled, &receipt
			s.CanonicalOutcomes[0].Receipt, c.Receipt, c.Stopped = &receipt, &receipt, true
			c.Cleanup.PrimaryRetirePending = false
			if shape == "broker-settled" {
				s.Couriers = nil
			}
		}
		return s
	}
	for _, shape := range []string{"courier-pending", "courier-decided", "courier-acknowledged", "broker-settled"} {
		t.Run(shape, func(t *testing.T) {
			s := refused(shape)
			encoded, err := EncodeWorker(s)
			if err != nil {
				t.Fatalf("refused unstarted prepare blocks later cuts (%s): %v", shape, err)
			}
			decoded, err := DecodeWorker(encoded)
			if err != nil || !reflect.DeepEqual(s, decoded) {
				t.Fatalf("refused prepare lost its exact settlement responsibility: %v", err)
			}
			for name, forge := range map[string]func(*xfercontract.WorkerState){
				"open-gate": func(f *xfercontract.WorkerState) { f.Entries[0].GateClosed, f.Revocations = false, nil },
				"entered-courier": func(f *xfercontract.WorkerState) {
					if len(f.Couriers) == 0 {
						f.CanonicalOutcomes[0].Outcome.Outcome = xfercontract.OutcomeInflight
						return
					}
					landed := workerFixture("courier").Couriers[0].Outcome
					f.Couriers[0].Outcome = landed
					for i := range f.CanonicalOutcomes {
						f.CanonicalOutcomes[i].Outcome = landed
					}
				},
				"no-settlement-owner": func(f *xfercontract.WorkerState) { f.Couriers, f.CanonicalOutcomes = nil, nil },
				// A phase that names no courier or terminal still needs an owner.
				"ownerless-decision": func(f *xfercontract.WorkerState) {
					f.Couriers, f.CanonicalOutcomes = nil, nil
					f.Entries[0].Status.State, f.Entries[0].Status.Phase, f.Entries[0].Status.Terminal = xfercontract.StatusLive, xfercontract.PhaseDecided, nil
					f.ResultRetention[0].ResultUntil = time.Time{}
				},
			} {
				forged := refused(shape)
				forge(&forged)
				if _, err := EncodeWorker(forged); err == nil {
					t.Fatalf("%s: an unstarted entry accepted %s", shape, name)
				}
			}
		})
	}
	landed := workerFixture("courier")
	landed.Entries[0].Started = false
	if _, err := EncodeWorker(landed); err == nil {
		t.Fatal("an unstarted prepare claimed a landed primitive")
	}
}
