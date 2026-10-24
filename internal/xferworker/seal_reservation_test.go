package xferworker

import (
	"errors"
	"io"
	"testing"
	"time"

	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xferimage"
)

// sealedSenderProbe is a sender whose pinned source seals to a fixed geometry.
// Its Step records whether byte work started; it never touches IO.
type sealedSenderProbe struct {
	seal         x.SourceSeal
	seals, steps int
}

func (s *sealedSenderProbe) Seal(x.Deadline) (x.SourceSeal, error) { s.seals++; return s.seal, nil }
func (s *sealedSenderProbe) Step(x.Deadline) error                 { s.steps++; return io.EOF }
func (s *sealedSenderProbe) Freeze(x.Deadline) (x.SenderState, error) {
	return x.SenderState{}, errors.New("probe sender exports no state")
}
func (s *sealedSenderProbe) Stop(x.Deadline) error { return nil }

// A Tier B pull's intent predates its source size, so its prepared
// reservation covers only the pending chunk. The registry must grow the one
// entry reservation from the sealed geometry before the first chunk, and the
// image must carry the grown reservation the next process adopts. Without
// the growth every first cut of a Tier B pull was refused as over capacity.
// A refused growth keeps the prepared reservation and starts no byte work.
// origin: S7 internal review CUT-CAPACITY F1 / hot-update S7 plan §2.1 / §8.3
func TestTransferWorkerPullReservationGrowsFromItsSourceSeal(t *testing.T) {
	const size = 9 << 20
	for _, refuse := range []bool{false, true} {
		f := newWorkerFixture()
		p := f.prepare("b", "grow")
		p.Intent.Verb = "pull"
		seal := x.SourceSeal{Size: size, SHA256: p.Intent.SHA256, ChunkSize: p.Intent.ChunkSize, Chunks: size / (32 << 10), SourceFingerprint: "pinned"}
		sender := &sealedSenderProbe{seal: seal}
		f.cfg.MakeSender = func(x.SendConfig) (x.Sender, error) { return sender, nil }
		o := newOwner(t, f)
		if _, err := o.Prepare(p, deadline(t, time.Second)); err != nil {
			t.Fatal(err)
		}
		prepared := xferimage.WorkerPreparationReservation(p)
		sized := p
		sized.Intent.Size = size
		grown := xferimage.WorkerPreparationReservation(sized)
		if grown.SealedBytes <= prepared.SealedBytes || grown.NonReplayBytes <= prepared.NonReplayBytes {
			t.Fatalf("sealed geometry does not grow the prepared reservation: prepared=%+v grown=%+v", prepared, grown)
		}
		f.capacity.mu.Lock()
		f.capacity.fail = refuse
		f.capacity.mu.Unlock()
		if err := o.Start(p.Identity); err != nil {
			t.Fatal(err)
		}
		e := o.entries[key(p.Identity)]
		await(t, e.done)
		f.capacity.mu.Lock()
		var live []x.Reservation
		for _, r := range f.capacity.live {
			live = append(live, r)
		}
		f.capacity.mu.Unlock()
		o.mu.Lock()
		recorded := o.state.CapacityReservations
		held := e.value.Reservation
		o.mu.Unlock()
		want := grown
		if refuse {
			want = prepared
		}
		if len(live) != 1 || live[0] != want || len(recorded) != 1 || recorded[0].ID != held || recorded[0].Reservation != want {
			t.Fatalf("refuse=%t: account %+v, image %+v, want one %+v", refuse, live, recorded, want)
		}
		if sender.seals != 1 || (sender.steps == 0) != refuse {
			t.Fatalf("refuse=%t: seals=%d steps=%d; byte work must follow only a granted growth", refuse, sender.seals, sender.steps)
		}
	}
}
