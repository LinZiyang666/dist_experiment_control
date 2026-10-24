package xferclient

import (
	"errors"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
)

type retirementTransport struct {
	x.Transport
	calls  int
	state  x.WriterState
	id     x.Identity
	origin x.Origin
	fail   bool
}

type retirementWriter struct {
	x.ObjectWriter
	failStop, failFreeze, unjoined, wrongIdentity bool
}

func (w *retirementWriter) Stop(d x.Deadline) error {
	if w.failStop {
		if err := w.ObjectWriter.Stop(d); err != nil {
			return err
		}
		return errors.New("writer join failed")
	}
	return w.ObjectWriter.Stop(d)
}
func (w *retirementWriter) Freeze(d x.Deadline) (x.WriterState, error) {
	if w.failFreeze {
		state, err := w.ObjectWriter.Freeze(d)
		if err != nil {
			return state, err
		}
		return state, errors.New("writer freeze failed")
	}
	state, err := w.ObjectWriter.Freeze(d)
	if w.unjoined {
		state.RetirementFence.WriterRetired = false
	}
	if w.wrongIdentity {
		state.ObjectSpec.Identity.EntryNonce = "replacement"
	}
	return state, err
}

// The orchestration double checks the new seam; real delivery and durability
// are exercised by the authenticated broker regression, not credited here.
func (p *clientTransport) RetireWriter(id x.Identity, origin x.Origin, state x.WriterState, _ x.Deadline) error {
	if id != p.identity || origin != p.origin || state.ObjectSpec.Identity != id || !state.RetirementFence.WriterRetired {
		return errors.New("wrong original writer witness")
	}
	return nil
}

func (p *retirementTransport) RetireWriter(id x.Identity, origin x.Origin, state x.WriterState, _ x.Deadline) error {
	p.calls++
	p.id, p.origin, p.state = id, origin, state
	if p.fail {
		return errors.New("retirement ACK lost")
	}
	return nil
}

// origin: S7 internal review rv-terminal / FIX-TERM request 3
func TestTransferWriterStopRetainsWitnessUntilAuthenticatedACK(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseWriterWitnessDelivery(t)
	}
	testharness.AssertNoGoroutineLeak(t, t.Name(), g)
	testharness.AssertNoFDLeak(t, t.Name(), fd, 0)
}

func exerciseWriterWitnessDelivery(t *testing.T) {
	t.Helper()
	f := newClientFixture(t, "push")
	defer f.close()
	c := f.client
	c.identity, c.origin = f.transport.identity, f.transport.origin
	c.identity.ObjectVersion = "A0123456789012345678901"
	c.identity.IntentDigest = "original-intent"
	f.transport.identity = c.identity
	stream, err := f.js.Stream(t.Context(), "OBJ_"+proto.XferBucketName(c.identity.SID))
	if err != nil {
		t.Fatal(err)
	}
	spec := x.ObjectSpec{Identity: c.identity, Bucket: proto.XferBucketName(c.identity.SID), Stream: stream.CachedInfo().Config.Name,
		StreamCreated: stream.CachedInfo().Created.UTC().Format(time.RFC3339Nano), Size: int64(len(f.data)), SHA256: digestOf(f.data), ChunkSize: 32768, Chunks: 3}
	writer, err := jsstream.NewTransferWriter(f.js, spec)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = writer.Append(0, f.data[:32768], testDeadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	wrapped := &retirementWriter{ObjectWriter: writer, failStop: true}
	c.writer, c.dataOpen, c.js = wrapped, true, f.js
	c.reservation, err = f.capacity.Reserve(x.Reservation{NonReplayBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	p := &retirementTransport{Transport: f.transport, fail: true}
	c.cfg.Transport = p
	for _, defect := range []string{"join", "freeze", "unjoined", "identity"} {
		wrapped.failStop, wrapped.failFreeze, wrapped.unjoined = defect == "join", defect == "freeze", defect == "unjoined"
		wrapped.wrongIdentity = defect == "identity"
		if err = c.Stop(testDeadline(t, time.Second)); err == nil || p.calls != 0 || c.writer == nil || c.stopped || !c.dataOpen || c.reservation == "" {
			t.Fatalf("%s failure invented witness delivery or released original custody", defect)
		}
	}
	wrapped.unjoined, wrapped.wrongIdentity = false, false
	// A frozen witness remains custody when the authenticated channel has not
	// been installed. The optional seam must refuse, never return fake success.
	c.cfg.Transport = struct{ x.Transport }{f.transport}
	if err = c.Stop(testDeadline(t, time.Second)); err == nil || c.stopped || c.reservation == "" || c.writer == nil {
		t.Fatal("missing retirement channel discharged writer custody")
	}
	c.cfg.Transport = p
	if err = c.Stop(testDeadline(t, time.Second)); err == nil {
		t.Fatal("Stop discarded the original writer without delivering its authenticated witness")
	}
	if p.calls != 1 || !p.state.RetirementFence.WriterRetired || p.state.ObjectSpec != spec || len(p.state.Receipts) != 1 ||
		p.id != c.identity || p.origin != c.origin || c.writer == nil || c.stopped || !c.dataOpen || c.reservation == "" {
		t.Fatalf("lost retirement ACK discarded original evidence or custody: calls=%d state=%+v", p.calls, p.state)
	}
	first := p.state
	p.fail = false
	// Retry consumes the retained evidence even if its old frozen leaf is now
	// unavailable; it cannot construct another snapshot or attachment.
	wrapped.failFreeze = true
	if err = c.Stop(testDeadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	if p.calls != 2 || !reflect.DeepEqual(first, p.state) || c.writer != nil || c.dataOpen || c.reservation != "" {
		t.Fatal("retirement retry changed its original frozen witness or released before ACK")
	}
}
