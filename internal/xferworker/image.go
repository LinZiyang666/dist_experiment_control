// image.go owns worker image validation/adoption and retained child ports.
// Keeping these beside the lifecycle owner leaves admission/controls focused.
// origin: hot-update S7 plan §8.2 / S7 internal review rv-lifecycle F1–F5
package xferworker

import (
	"errors"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/transferpath"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xfercourier"
	"github.com/LinZiyang666/tether/internal/xferimage"
	"github.com/LinZiyang666/tether/internal/xferreceive"
	"github.com/nats-io/nats.go/jetstream"
	"reflect"
	"slices"
	"time"
)

func cloneResources(resources []handoff.Resource) []handoff.Resource {
	return xferimage.CloneWorkerResources(resources)
}

// receiverState observes a live receiver between steps. A real receiver's
// Freeze is a cut (it freezes the atomic sink and closes the reader), so the
// registry uses its non-cutting Progress view whenever one exists.
// origin: hot-update S7 plan §4 / T08 — S7 production activation
func receiverState(r x.Receiver) (x.ReceiverState, error) {
	if p, ok := r.(interface{ Progress() x.ReceiverState }); ok {
		return p.Progress(), nil
	}
	// A missing progress port cannot authorize a lifecycle cut.
	// origin: S7 internal review rv-terminal F1
	return x.ReceiverState{}, workerError(x.CodeOwnerRecovering)
}

// stopBytes copies actor handles under the registry lock, then lets their own
// Stop gates interrupt IO without holding the registry or entry token.
func (o *owner) stopBytes(e *entry, deadline x.Deadline) error {
	o.mu.Lock()
	sender, receiver := e.sender, e.receiver
	o.mu.Unlock()
	var errs []error
	if sender != nil {
		errs = append(errs, sender.Stop(deadline))
	}
	if receiver != nil {
		errs = append(errs, receiver.Stop(deadline))
	}
	return errors.Join(errs...)
}

// Wrapped actors own their reconnect adapters. A bare receiver exposes its
// retained ports for the same dependency-ordered rollback; no path is reopened.
// origin: S7 internal review rv-recovery F01
func thawActor(actor any, deadline x.Deadline) error {
	if thaw, ok := actor.(interface{ Thaw(x.Deadline) error }); ok {
		return thaw.Thaw(deadline)
	}
	receiver, ok := actor.(x.Receiver)
	if !ok {
		return nil
	}
	reader, atomic, err := xferreceive.Ports(receiver)
	if err != nil {
		return err
	}
	if err := transferpath.ThawAtomic(atomic, deadline); err != nil {
		return err
	}
	if thaw, ok := reader.(interface{ Thaw(x.Deadline) error }); ok {
		if err := thaw.Thaw(deadline); err != nil {
			return err
		}
	} else {
		return workerError(x.CodeOwnerRecovering)
	}
	return xferreceive.Thaw(receiver)
}

// RebindConsumerCleanup joins and retries every retained consumer duty through
// the authenticated current management binding. Hosts call it outside mutexes.
// origin: S7 internal review rv-jsnats F3 / s7-p9jsn2 Requests 1
func RebindConsumerCleanup(manager Manager, js jetstream.JetStream, deadline x.Deadline) error {
	o, ok := manager.(*owner)
	if !ok {
		return workerError(x.CodeOwnerRecovering)
	}
	o.mu.Lock()
	duties := append([]*jsstream.ConsumerCleanupOwner(nil), o.cleanup...)
	o.mu.Unlock()
	var errs []error
	released := make(map[string]bool)
	for _, duty := range duties {
		if err := duty.Bind(js, deadline); err != nil {
			errs = append(errs, err)
			continue
		}
		if err := duty.Wait(deadline); err != nil {
			errs = append(errs, err)
		} else {
			released[duty.ID()] = true
		}
	}
	o.mu.Lock()
	o.cleanup = slices.DeleteFunc(o.cleanup, func(duty *jsstream.ConsumerCleanupOwner) bool { return released[duty.ID()] })
	o.mu.Unlock()
	return errors.Join(errs...)
}

// RebindCurrentConsumerCleanup applies the fixed management attempt budget
// when authentication installs a new connection without a transfer deadline.
func RebindCurrentConsumerCleanup(manager Manager, js jetstream.JetStream) error {
	d, err := controlbudget.After(5 * time.Second)
	if err != nil {
		return err
	}
	return RebindConsumerCleanup(manager, js, d)
}

// StopWithConsumerCleanup keeps the terminal host call outside host mutexes.
// Stopping readers may create detached deletion duties; current management
// settles them and a repeated Stop releases the retained actor ownership.
// origin: S7 internal review rv-jsnats F3 / s7-p9jsn2 Requests 1
func StopWithConsumerCleanup(manager Manager, stop func() error, js jetstream.JetStream) error {
	stopErr := stop()
	bindErr := RebindCurrentConsumerCleanup(manager, js)
	if bindErr == nil && stopErr != nil {
		stopErr = stop()
	}
	return errors.Join(stopErr, bindErr)
}

// Actors is a set of original actors restored by the outer composition. P00's
// creation factories stay unchanged; restoration never calls those factories
// with a pathname in place of an inherited descriptor.
// origin: hot-update S7 plan §8.2 / T35
type Actors struct {
	Identity x.Identity
	Sender   x.Sender
	Receiver x.Receiver
	Courier  x.Courier
}

// AdoptActors installs sleeping actors before Thaw. The registry retains their
// borrowed resource census for pristine detach; after activation each actor is
// the sole closer of its own original descriptors.
func AdoptActors(manager Manager, actors []Actors) error {
	o, ok := manager.(*owner)
	if !ok {
		return workerError(x.CodeOwnerRecovering)
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if !o.pristine || !o.closed || o.actorsImported {
		return workerError(x.CodeOwnerRecovering)
	}
	expected := map[x.Identity][3]bool{}
	for _, e := range o.entries {
		expected[e.value.Prepare.Identity] = [3]bool{}
	}
	for _, a := range o.state.Senders {
		mask := expected[a.Identity]
		mask[0] = true
		expected[a.Identity] = mask
	}
	for _, a := range o.state.Receivers {
		mask := expected[a.Identity]
		mask[1] = true
		expected[a.Identity] = mask
	}
	for _, a := range o.state.Couriers {
		mask := expected[a.Identity]
		mask[2] = true
		expected[a.Identity] = mask
	}
	if len(actors) != len(expected) {
		return workerError(x.CodeIntentConflict)
	}
	seen := map[x.Identity]bool{}
	for _, a := range actors {
		e := o.entries[key(a.Identity)]
		if e == nil || e.value.Prepare.Identity != a.Identity || seen[a.Identity] || expected[a.Identity] != [3]bool{a.Sender != nil, a.Receiver != nil, a.Courier != nil} {
			return workerError(x.CodeIntentConflict)
		}
		seen[a.Identity] = true
	}
	for _, a := range actors {
		e := o.entries[key(a.Identity)]
		e.sender, e.receiver, e.courier = a.Sender, a.Receiver, a.Courier
		e.sealed = a.Sender != nil && e.value.Prepare.Budget.SourceSealed
	}
	o.actorsImported = true
	return nil
}

// PreparedInline borrows the authenticated Tier A push payload an entry
// retained, and reserved, at Prepare. The receiver factory reads it without a
// second copy: nothing writes the retained slice after Prepare (image copies
// are separate clones), and the slice outlives entry removal while borrowed.
// origin: hot-update S7 plan §8.1 / FIX-INLINE
func PreparedInline(manager Manager, id x.Identity) ([]byte, bool) {
	o, ok := manager.(*owner)
	if !ok {
		return nil, false
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	e := o.entries[key(id)]
	if e == nil || e.value.Prepare.Identity != id || e.value.Prepare.Intent.Verb != "push" || e.value.Prepare.Intent.Tier != "a" {
		return nil, false
	}
	return e.value.Prepare.Inline, true
}

// BindOutcome retains the exact broker decision before a sender-only entry
// accepts its receipt. The receipt alone cannot reconstruct canonical bytes.
// origin: hot-update S7 plan §4 / §5.1 / T16
func BindOutcome(manager Manager, outcome x.CanonicalOutcome, d x.Deadline) error {
	o, ok := manager.(*owner)
	if !ok || outcome.Receipt == nil {
		return workerError(x.CodeOwnerRecovering)
	}
	if err := xfercourier.ValidateEvidence(outcome); err != nil {
		return err
	}
	if err := o.enter(d); err != nil {
		return err
	}
	defer o.leave()
	o.mu.Lock()
	defer o.mu.Unlock()
	e := o.entries[key(outcome.Identity)]
	if e == nil || e.value.Prepare.Identity != outcome.Identity {
		return workerError(x.CodeIntentConflict)
	}
	for _, old := range o.state.CanonicalOutcomes {
		if old.Identity == outcome.Identity && !reflect.DeepEqual(old, outcome) {
			return workerError(x.CodeTerminalConflict)
		}
	}
	xferimage.RetainWorkerOutcome(&o.state, outcome)
	return nil
}

// RetainConsumerCleanup adopts the original attachment's independent duty.
// A shared process can host multiple accounts; other readers are not ours.
// origin: S7 internal review rv-jsnats F3 / s7-p9jsn2 Requests 1–2
func RetainConsumerCleanup(manager Manager, duty *jsstream.ConsumerCleanupOwner) error {
	o, ok := manager.(*owner)
	if !ok {
		return workerError(x.CodeOwnerRecovering)
	}
	if duty == nil {
		return nil
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, held := range o.cleanup {
		if held.ID() == duty.ID() {
			return nil
		}
	}
	o.cleanup = append(o.cleanup, duty)
	return nil
}

// reserveSealed grows a pull entry's single reservation to cover what its
// sealed source makes the sender retain. A Tier A pull needs its source copy,
// the sealed image of that copy and the cut codec's copies
// (xferimage.WorkerInlineReservation); a Tier B pull needs one receipt per
// sealed chunk beside its pending chunk (xferimage.WorkerPreparationReservation
// at the sealed size). It is idempotent and reserves before releasing: a
// refusal keeps the original reservation and no copy or chunk is allocated.
// The entry's recorded reservation is the one an image carries and the next
// process adopts, so the cut capacity check sees these bytes.
// origin: hot-update S7 plan §8.1 / §8.3 / FIX-INLINE
// origin: S7 internal review CUT-CAPACITY F1 / hot-update S7 plan §2.1
func (o *owner) reserveSealed(e *entry, seal x.SourceSeal) error {
	want := xferimage.WorkerInlineReservation(e.value.Prepare, seal.Size)
	if prepare := e.value.Prepare; prepare.Intent.Tier == "b" {
		prepare.Intent.Size = seal.Size
		want = xferimage.WorkerPreparationReservation(prepare)
	}
	o.mu.Lock()
	old, held := e.value.Reservation, false
	for _, r := range o.state.CapacityReservations {
		if r.ID == old {
			held = r.Reservation == want
		}
	}
	o.mu.Unlock()
	if held {
		return nil
	}
	id, err := o.cfg.Capacity.Reserve(want)
	if err != nil || id == "" {
		return errors.Join(workerError(x.CodeCapacityExceeded), err)
	}
	o.mu.Lock()
	for i, r := range o.state.CapacityReservations {
		if r.ID == old {
			o.state.CapacityReservations[i] = x.CapacityReservation{ID: id, Reservation: want}
		}
	}
	e.value.Reservation = id
	o.mu.Unlock()
	return o.cfg.Capacity.Release(old)
}
