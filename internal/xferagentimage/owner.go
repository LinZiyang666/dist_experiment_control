// Package xferagentimage adapts the agent's continuation worker to the
// NATS-free agent image ports (transfercontract.TransferWorker/TransferImporter).
// transfercontract stays NATS-free and the byte-work leaves (filetransfer,
// xferagent) never reach the image's other domains; only internal/agent
// imports this composition.
// origin: hot-update S7 plan §8.1–§8.3 / P21 — R9 NATS-free image ports
package xferagentimage

import (
	"errors"

	"github.com/LinZiyang666/tether/internal/agentimage/transfercontract"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/filetransfer"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/xferagent"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xfercontract/xfervalue"
	"github.com/LinZiyang666/tether/internal/xferimage"
	"github.com/LinZiyang666/tether/internal/xferworker"
)

// Worker is the agent image owner of rt's bound worker, or nil when none is
// bound or it retains no entry: the cut then carries (and the
// candidate must read) no transfer domain. A creation admitted after this
// check is refused by the closed cut gate; a late one fails the final
// bundle-capability check instead of crossing unread.
func Worker(rt *xferagent.Runtime) transfercontract.TransferWorker {
	if rt == nil || !xferworker.Retained(rt.BoundWorker()) {
		return nil
	}
	return &worker{rt: rt}
}

type worker struct{ rt *xferagent.Runtime }

func (w *worker) manager() (x.WorkerManager, error) {
	if m := w.rt.BoundWorker(); m != nil {
		return m, nil
	}
	return nil, errors.New(x.CodeOwnerRecovering + ": transfer worker is not bound")
}

func (w *worker) Freeze(d controlbudget.Deadline) (transfercontract.TransferFrozen, error) {
	m, err := w.manager()
	if err != nil {
		return nil, err
	}
	frozen, err := m.Freeze(d)
	if errors.Is(err, xferworker.ErrActorCustody) {
		// A live byte actor's descriptors cannot cross yet: the same deferral
		// the legacy transfer census reports, never a failed upgrade. The
		// rollback thaws every actor the snapshot froze.
		return nil, &transfercontract.DeferredError{Reason: transfercontract.ReasonTransfersInFlight, Detail: "capable transfer in flight", Cause: err}
	}
	if frozen == nil {
		return nil, err
	}
	return imageFrozen{frozen}, err
}

// Thaw binds the backend current at this call (xferagent resolves it).
func (w *worker) Thaw(d controlbudget.Deadline) error {
	m, err := w.manager()
	if err != nil {
		return err
	}
	return m.Thaw(w.rt.Bindings(), d)
}

type imageFrozen struct{ frozen x.WorkerFrozen }

func (f imageFrozen) Export() (transfercontract.TransferState, []handoff.Resource, error) {
	state, resources, err := f.frozen.Export()
	if err != nil {
		return transfercontract.TransferState{}, resources, err
	}
	data, err := xferimage.EncodeWorker(state)
	if err != nil {
		return transfercontract.TransferState{}, resources, err
	}
	return Project(state, data), resources, nil
}

func (f imageFrozen) DetachPristine() (xfervalue.Salvage, error) { return f.frozen.DetachPristine() }

// Importer is the agent image's transfer importer: Import adopts the sealed
// worker as owner's only handler worker, before activation and before any
// handler admission (filetransfer.WorkerOwner.Adopt).
func Importer(owner *filetransfer.WorkerOwner, deps WorkerAdopter, build func() *filetransfer.Handler) transfercontract.TransferImporter {
	return &importer{owner: owner, deps: deps, build: build}
}

// WorkerAdopter exposes adoption without importing the legacy handler's
// control-context callbacks into the image lifetime boundary.
// origin: S7 internal review GATES-PROD F1
type WorkerAdopter interface {
	AdoptWorker(*filetransfer.WorkerOwner, func() *filetransfer.Handler, func(*xferagent.Runtime, xferworker.Config) (xferworker.Manager, error)) (*filetransfer.Handler, error)
}

type importer struct {
	owner *filetransfer.WorkerOwner
	deps  WorkerAdopter
	build func() *filetransfer.Handler
}

func (i *importer) Decode(data []byte) (transfercontract.TransferState, error) {
	state, err := xferimage.DecodeWorker(data)
	if err != nil {
		return transfercontract.TransferState{}, err
	}
	return Project(state, data), nil
}

func (i *importer) Import(p transfercontract.TransferState, files transfercontract.TransferFileSource) (transfercontract.TransferWorker, xfervalue.Salvage, error) {
	state, err := xferimage.DecodeWorker(p.Encoded)
	if err != nil {
		return nil, xfervalue.Salvage{}, err
	}
	var salvage xfervalue.Salvage
	h, err := i.deps.AdoptWorker(i.owner, i.build, func(rt *xferagent.Runtime, cfg xferworker.Config) (xferworker.Manager, error) {
		manager, taken, importErr := rt.ImportWorker(cfg, state, files)
		salvage = taken
		return manager, importErr
	})
	if h == nil || h.Runtime == nil || h.Worker == nil {
		return nil, salvage, errors.Join(err, errors.New(x.CodeOwnerRecovering+": imported transfer worker is not installed"))
	}
	return &worker{rt: h.Runtime}, salvage, err
}

// Project is the custody projection the image validates against its bundle,
// taken from the same decoded value the sealed bytes encode.
func Project(state x.WorkerState, data []byte) transfercontract.TransferState {
	p := transfercontract.TransferState{Encoded: data, Schema: state.Schema, BootID: state.Lineage.BootID,
		InstanceID: state.Lineage.InstanceID, Generation: state.Lineage.Generation, AdmissionClosed: state.Admission.Closed,
		Inflight: state.Admission.Inflight, Entries: uint32(len(state.Entries))}
	for _, reservation := range state.CapacityReservations {
		p.Reservations = append(p.Reservations, reservation.Reservation)
	}
	for _, sender := range state.Senders {
		p.Refs = append(p.Refs, sender.Source)
	}
	for _, receiver := range state.Receivers {
		p.Refs = append(p.Refs, receiver.Atomic.Tmp, receiver.Atomic.Parent, receiver.Atomic.Source)
	}
	return p
}
