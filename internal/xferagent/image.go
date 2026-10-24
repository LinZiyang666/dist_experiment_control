package xferagent

import (
	"errors"
	"os"

	"github.com/LinZiyang666/tether/internal/transferpath"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xfercourier"
	"github.com/LinZiyang666/tether/internal/xferreceive"
	"github.com/LinZiyang666/tether/internal/xfersend"
	"github.com/LinZiyang666/tether/internal/xferworker"
)

// imageOwner tracks the owner generation the bound worker's next Thaw
// installs. Both fields are guarded by the Runtime's mutex. The NATS-free
// agent image ports are adapted in xferagentimage, so this byte-work leaf
// (imported by filetransfer) never reaches the agent image's domains.
// origin: hot-update S7 plan §8.1–§8.3 / P21 — agent image cut continuity
type imageOwner struct {
	worker     x.WorkerManager
	generation uint64
}

// BoundWorker returns the worker bound by Bind (nil before). A worker the
// runtime did not import starts at lineage generation one.
func (r *Runtime) BoundWorker() x.WorkerManager {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.worker != nil && r.image.worker != r.worker {
		r.image.worker, r.image.generation = r.worker, max(r.image.generation, 1)
	}
	return r.worker
}

// Bindings resolves the worker's backend when a thaw needs it: this
// runtime's stable transport and authority and the session's CURRENT
// JetStream. A returning exec or a rebuilt control session therefore never
// thaws a stale backend.
func (r *Runtime) Bindings() x.Bindings {
	r.mu.Lock()
	generation := r.image.generation
	r.mu.Unlock()
	return x.Bindings{JS: r.host.JetStream(), Transport: r, Authority: r, OwnerGeneration: generation}
}

// ImportWorker imports a sleeping worker from a sealed image state with this
// runtime's configuration; its first Thaw installs the next owner generation.
// The caller binds the returned manager as the handler's only worker.
func (r *Runtime) ImportWorker(cfg x.WorkerConfig, state x.WorkerState, files x.FileSource) (x.WorkerManager, x.Salvage, error) {
	// Import and actor constructors emit before Bind/Thaw. Stamp them with the
	// known successor now; late binding would split one image's ordinal stream.
	// origin: S7 internal review INTEG-O F5
	r.mu.Lock()
	r.image.generation = state.Lineage.Generation + 1
	r.mu.Unlock()
	var ids []x.ReservationID
	var reservations []x.Reservation
	for _, reservation := range state.CapacityReservations {
		ids = append(ids, reservation.ID)
		reservations = append(reservations, reservation.Reservation)
	}
	if err := r.capacity.Adopt(ids, reservations); err != nil {
		return nil, x.Salvage{}, err
	}
	manager, salvage, err := xferworker.ImportWorker(cfg, state, files)
	if manager == nil {
		return nil, salvage, errors.Join(err, errors.New(x.CodeOwnerRecovering+": worker import returned no owner"))
	}
	if err != nil {
		return manager, salvage, err
	}
	borrowed := imageFiles{}
	for _, resource := range salvage.Resources {
		for name, file := range resource.Files {
			borrowed[resource.ID+"/"+name] = file
		}
	}
	actors, err := r.restoreActors(cfg, state, borrowed)
	if err == nil {
		err = xferworker.AdoptActors(manager, actors)
	}
	r.mu.Lock()
	r.image.worker, r.image.generation = manager, state.Lineage.Generation+1
	r.mu.Unlock()
	return manager, salvage, err
}

// imageFiles borrows descriptors the sleeping registry already took exactly
// once. Actor constructors only inspect them; pristine failure returns that
// registry's complete salvage, never a closed or reopened namesake file.
type imageFiles map[string]*os.File

func (f imageFiles) TakeFile(id, name string) (*os.File, error) {
	if file := f[id+"/"+name]; file != nil {
		return file, nil
	}
	return nil, errors.New(x.CodeOwnerRecovering + ": missing inherited transfer file")
}

// restoreActors is network-free: imported writers/readers open their new
// attachment on first byte work, after the agent re-establishes its session.
// origin: hot-update S7 plan §8.2 / T35
func (r *Runtime) restoreActors(cfg x.WorkerConfig, state x.WorkerState, files imageFiles) ([]xferworker.Actors, error) {
	sets := map[x.Identity]*xferworker.Actors{}
	payloads := map[x.Identity][]byte{}
	for _, entry := range state.Entries {
		id := entry.Prepare.Identity
		sets[id] = &xferworker.Actors{Identity: id}
		payloads[id] = entry.Prepare.Inline
		r.recordIntent(id, entry.Prepare.Intent)
	}
	for _, s := range state.Senders {
		source, err := files.TakeFile(s.Source.ResourceID, s.Source.Name)
		if err != nil {
			return nil, err
		}
		// A Tier A pull imports its retained inline copy and no object writer;
		// it rejoins the replay index so a pending prepare still gets the
		// original bytes after the cut, never a re-read of the source path.
		// origin: hot-update S7 plan §8.1–§8.2 / FIX-INLINE
		if s.Intent.Tier == "a" {
			actor, err := xfersend.Import(x.SendConfig{Base: cfg.Base, Identity: s.Identity, Intent: s.Intent, Budget: s.Budget, Source: source, Transport: r}, s)
			if err != nil {
				return nil, err
			}
			live := &liveSender{Sender: actor, source: source, ref: s.Source, rt: r, id: s.Identity}
			r.retainInline(live)
			sets[s.Identity].Sender = live
			continue
		}
		writer := &sealedWriter{rt: r, id: s.Identity}
		if s.Budget.SourceSealed {
			writer.imported = &s.Writer
		}
		actor, err := xfersend.Import(x.SendConfig{Base: cfg.Base, Identity: s.Identity, Intent: s.Intent, Budget: s.Budget, Source: source, Writer: writer, Transport: r}, s)
		if err != nil {
			return nil, err
		}
		sets[s.Identity].Sender = &liveSender{Sender: actor, writer: writer, source: source, ref: s.Source}
		r.mu.Lock()
		r.sources[x.Key{SID: s.Identity.SID, TransferID: s.Identity.TransferID}] = x.Request{Op: x.OpSource, CallID: s.Identity.TransferID + "-source", Identity: s.Identity, Source: &s.SourceSeal}
		r.mu.Unlock()
	}
	for _, s := range state.Receivers {
		atomic, _, err := transferpath.ImportAtomic(s.Atomic, files)
		if err != nil {
			return nil, err
		}
		// A Tier A push receiver imports the reader-free state its Freeze
		// exported: the entry's retained payload resumes at the durable tmp
		// prefix, with no object, consumer or data attachment. Building the
		// object reader here left every active Tier A receiver unimportable.
		// origin: hot-update S7 plan §8.1–§8.2 / S7 FIX-TIERA2
		if s.Identity.ObjectVersion == "" {
			inline, err := importInlineReader(payloads[s.Identity], s)
			if err != nil {
				return nil, err
			}
			actor, err := xferreceive.Import(x.ReceiveConfig{Base: cfg.Base, Identity: s.Identity, Budget: s.Budget, Reader: inline, Atomic: atomic, Transport: r}, s)
			if err != nil {
				return nil, err
			}
			sets[s.Identity].Receiver = &liveReceiver{Receiver: actor, atomic: atomic}
			continue
		}
		reader := &dataReader{rt: r, id: s.Identity, spec: s.Reader.ObjectSpec, retained: &s.Reader, frozen: true}
		actor, err := xferreceive.Import(x.ReceiveConfig{Base: cfg.Base, Identity: s.Identity, Budget: s.Budget, Reader: reader, Atomic: atomic, Transport: r}, s)
		if err != nil {
			return nil, err
		}
		sets[s.Identity].Receiver = &liveReceiver{Receiver: actor, reader: reader, atomic: atomic}
	}
	for _, s := range state.Couriers {
		transport := &courierTransport{Transport: r, rt: r}
		actor, err := xfercourier.Import(x.CourierConfig{Base: cfg.Base, Identity: s.Identity, Outcome: s.Outcome, Capsule: s.Capsule, Transport: transport}, s)
		if err != nil {
			return nil, err
		}
		transport.courier, sets[s.Identity].Courier = actor, actor
	}
	var actors []xferworker.Actors
	for _, set := range sets {
		actors = append(actors, *set)
	}
	return actors, nil
}
