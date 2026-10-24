package xferimage

import (
	"bytes"
	"crypto/sha256"
	"encoding"
	"encoding/binary"
	"encoding/hex"
	"reflect"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xferid"
	"github.com/LinZiyang666/tether/internal/xferreceive"
)

// Validation is pure: actual fstat/OFD custody, file Sync and grant signatures
// remain the lifecycle owners' obligations. Here the whole image must agree
// before any imported actor can acquire authority or touch a shared OFD.
// origin: hot-update S7 plan §3 / §8.2–§8.3 / T17
func validateWorker(s xfercontract.WorkerState) error {
	if s.Schema != xfercontract.StateSchema || !workerID(s.Lineage.InstanceID) || !workerID(s.Lineage.BootID) ||
		s.Lineage.Generation == 0 || !s.Admission.Closed || s.Admission.Inflight != 0 ||
		s.Admission.Generation != s.Lineage.Generation || len(s.Entries) > 1024 || !ValidWorkerConsumerCleanup(s) || !workerVocabulary(reflect.ValueOf(s)) {
		return ErrWorkerState
	}
	reservations, totals, err := workerReservations(s)
	if err != nil {
		return err
	}
	entries, err := workerEntries(s, reservations)
	if err != nil {
		return err
	}

	// Collection identity is independent of position. Every child has one
	// registry parent, and aliases of resource IDs cannot create second owners.
	resources := make(map[string]workerResourceOwner)
	senders, err := workerSenders(s, entries, resources)
	if err != nil {
		return err
	}
	receivers, err := workerReceivers(s, entries, resources)
	if err != nil {
		return err
	}
	outcomes, err := workerOutcomes(s, entries)
	if err != nil {
		return err
	}
	couriers, err := workerCouriers(s, entries, outcomes)
	if err != nil {
		return err
	}
	if err := workerEntryActors(s, entries, senders, receivers, outcomes, couriers); err != nil {
		return err
	}
	if err := workerRetention(s, entries); err != nil {
		return err
	}
	return workerCutCapacity(s, entries, resources, totals)
}

func workerReservations(s xfercontract.WorkerState) (map[xfercontract.ReservationID]xfercontract.Reservation, xfercontract.Reservation, error) {
	reservations := make(map[xfercontract.ReservationID]xfercontract.Reservation)
	var totals xfercontract.Reservation
	// Reservations precede all actor effects. Repeated reservation IDs would
	// otherwise make import release somebody else's budget or count it twice.
	for _, r := range s.CapacityReservations {
		if !workerID(string(r.ID)) || reservations[r.ID] != (xfercontract.Reservation{}) || !workerReservation(r.Reservation, &totals) {
			return nil, totals, ErrWorkerCapacity
		}
		reservations[r.ID] = r.Reservation
	}
	return reservations, totals, nil
}

func workerEntries(s xfercontract.WorkerState, reservations map[xfercontract.ReservationID]xfercontract.Reservation) (map[xfercontract.Identity]xfercontract.WorkerEntry, error) {
	entries := make(map[xfercontract.Identity]xfercontract.WorkerEntry)
	keys := make(map[xfercontract.Key]bool)
	usedReservations := make(map[xfercontract.ReservationID]bool)
	for _, e := range s.Entries {
		if err := workerEntry(s, e, keys, usedReservations, reservations); err != nil {
			return nil, err
		}
		id := e.Prepare.Identity
		key := xfercontract.Key{SID: id.SID, TransferID: id.TransferID}
		entries[id], keys[key], usedReservations[e.Reservation] = e, true, true
	}
	if len(usedReservations) != len(reservations) {
		return nil, ErrWorkerCapacity
	}
	return entries, nil
}

func workerEntry(s xfercontract.WorkerState, e xfercontract.WorkerEntry, keys map[xfercontract.Key]bool,
	usedReservations map[xfercontract.ReservationID]bool, reservations map[xfercontract.ReservationID]xfercontract.Reservation) error {
	p := e.Prepare
	id := p.Identity
	key := xfercontract.Key{SID: id.SID, TransferID: id.TransferID}
	// Permit expiration is deliberately absent here: it limits creation,
	// while an already retained entry survives that deadline and cuts.
	if !workerIdentity(id) || id.AgentInstance != s.Lineage.InstanceID || keys[key] || !workerID(p.CallID) || !workerOrigin(p.Origin) ||
		p.Permit.Identity != id || p.Permit.Origin != p.Origin || !workerID(p.Permit.PermitNonce) || p.Permit.Signature == "" ||
		p.Permit.IssuedAt.IsZero() || !p.Permit.CreateUntil.After(p.Permit.IssuedAt) ||
		e.OwnerGeneration != s.Lineage.Generation || e.Status.Identity != id || e.Status.Origin != p.Origin ||
		e.Status.State == xfercontract.StatusUnknown || e.Status.State == "" || !workerPhase(e.Status.Phase) ||
		!workerBudget(p.Budget) || e.Status.Budget != p.Budget || !workerIntent(p.Intent) ||
		usedReservations[e.Reservation] {
		return ErrWorkerState
	}
	if _, ok := reservations[e.Reservation]; !ok {
		return ErrWorkerCapacity
	}
	if p.Intent.Tier == "a" && (int64(len(p.Inline)) != p.Intent.Size || len(p.Inline) > 8<<20) {
		return ErrWorkerState
	}
	// Inline bytes are the whole retained source. Unlike a pinned file's
	// prefix, their advertised digest is verifiable without filesystem IO.
	// origin: hot-update S7 plan §4 / §8.3 / T17
	if p.Intent.Tier == "a" && p.Intent.SHA256 != "" && p.Intent.SHA256 != workerDigest(p.Inline) {
		return ErrWorkerState
	}
	if p.Intent.Tier == "b" && (len(p.Inline) != 0 || !workerID(id.ObjectVersion)) {
		return ErrWorkerState
	}
	if p.Source != nil && !workerSourceSeal(*p.Source) {
		return ErrWorkerState
	}
	if p.Settlement != nil && !workerCapsule(*p.Settlement, id) {
		return ErrWorkerState
	}
	if e.Status.Terminal != nil && (e.Status.State != xfercontract.StatusTerminal || !workerReceipt(*e.Status.Terminal)) {
		return ErrWorkerState
	}
	if e.Status.State == xfercontract.StatusTerminal && e.Status.Terminal == nil {
		return ErrWorkerState
	}
	if e.Status.DurableOffset < 0 || e.Status.DurableOffset > p.Budget.Size || e.Status.ChunkIndex > workerMaxItems {
		return ErrWorkerState
	}
	return nil
}

func workerSenders(s xfercontract.WorkerState, entries map[xfercontract.Identity]xfercontract.WorkerEntry,
	resources map[string]workerResourceOwner) (map[xfercontract.Identity]bool, error) {
	actorSeen := make(map[xfercontract.Identity]bool)
	senders := make(map[xfercontract.Identity]bool)
	for _, a := range s.Senders {
		e, ok := entries[a.Identity]
		if !ok || actorSeen[a.Identity] || (!e.Started && e.Status.Phase != xfercontract.PhasePrepared) || a.Intent != e.Prepare.Intent || a.Budget != e.Prepare.Budget ||
			!workerDeadline(a.ByteDeadline, s.Lineage.BootID) {
			return nil, ErrWorkerState
		}
		// A prepared pull may own its pinned source before stat/hash seals it.
		// Preserve that owner without authorizing any cursor or object effect.
		// origin: hot-update S7 plan §1.2 / §4 / T17
		unsealed := e.Status.Phase == xfercontract.PhasePrepared && !a.Budget.SourceSealed && a.SourceSeal == (xfercontract.SourceSeal{})
		if unsealed {
			if a.Offset != 0 || len(a.SHA256State) != 0 || e.Prepare.Source != nil || !reflect.DeepEqual(a.Writer, xfercontract.WriterState{}) {
				return nil, ErrWorkerState
			}
		} else if !workerSourceSeal(a.SourceSeal) || a.Offset < 0 || a.Offset > a.SourceSeal.Size ||
			!workerHash(a.SHA256State, a.Offset, a.SourceSeal.Size, a.SourceSeal.SHA256) || !a.Budget.SourceSealed ||
			a.Budget.Size != a.SourceSeal.Size || (e.Prepare.Source != nil && *e.Prepare.Source != a.SourceSeal) {
			return nil, ErrWorkerState
		}
		if !unsealed && a.Intent.Tier == "b" {
			if !workerWriter(a.Writer, a.Identity) || a.Writer.ObjectSpec.Size != a.SourceSeal.Size ||
				a.Writer.ObjectSpec.SHA256 != a.SourceSeal.SHA256 || a.Writer.ObjectSpec.ChunkSize != a.SourceSeal.ChunkSize ||
				a.Offset != workerReceiptOffset(a.Writer.Receipts) {
				return nil, ErrWorkerState
			}
		} else if !reflect.DeepEqual(a.Writer, xfercontract.WriterState{}) {
			return nil, ErrWorkerState
		}
		// A Tier A pull has no object cursor: its cursor is zero until the
		// retained copy exists, then the whole sealed source in that copy. A
		// stopped sender has released it; Tier B never carries inline bytes.
		// origin: hot-update S7 plan §8.1 / §8.3 / FIX-INLINE
		if !workerResource(resources, a.Source, a.SourceIdentity, xfercontract.ResourceSource, a.Identity, !a.Stopped) || a.Inline == nil && a.Intent.Tier == "a" && !a.Stopped && a.Offset != 0 ||
			a.Inline != nil && (unsealed || a.Intent.Tier != "a" || a.Stopped || int64(len(a.Inline)) != a.SourceSeal.Size || a.Offset != a.SourceSeal.Size || workerDigest(a.Inline) != a.SourceSeal.SHA256) {
			return nil, ErrWorkerState
		}
		actorSeen[a.Identity] = true
		senders[a.Identity] = true
	}
	return senders, nil
}

func workerReceivers(s xfercontract.WorkerState, entries map[xfercontract.Identity]xfercontract.WorkerEntry,
	resources map[string]workerResourceOwner) (map[xfercontract.Identity]bool, error) {
	actorSeen := make(map[xfercontract.Identity]bool)
	receivers := make(map[xfercontract.Identity]bool)
	for _, a := range s.Receivers {
		e, ok := entries[a.Identity]
		if !ok || actorSeen[a.Identity] || (!e.Started && e.Status.Phase != xfercontract.PhasePrepared) || a.Budget != e.Prepare.Budget || !workerDeadline(a.ByteDeadline, s.Lineage.BootID) ||
			a.InlineOffset < 0 || a.InlineOffset > e.Prepare.Intent.Size || !workerAtomic(a.Atomic, a.Identity, a.GateClosed, resources) ||
			a.Atomic.DurableOffset != a.Checkpoint.DurableOffset || !workerAtomicGrant(a) ||
			(a.GateClosed && !e.GateClosed) || (a.Grant != (xfercontract.CommitGrant{}) && !workerGrant(a.Grant, a.Identity)) ||
			!workerOutcome(a.Outcome, a.Identity, a.GateClosed) {
			return nil, ErrWorkerState
		}
		if !e.Started && (a.Atomic.Phase != xfercontract.PhasePrepared || a.Checkpoint.DurableOffset != 0 || a.Grant != (xfercontract.CommitGrant{})) {
			return nil, ErrWorkerState
		}
		if e.Prepare.Intent.Tier == "b" {
			if !workerReader(a.Reader, a.Identity) || a.Reader.ObjectSpec.Size != a.Budget.Size ||
				!reflect.DeepEqual(a.Checkpoint, a.Reader.SinkCheckpoint) || a.InlineOffset != 0 {
				return nil, ErrWorkerState
			}
		} else if !xferreceive.ValidInlineState(a, e.Prepare.Inline) {
			// The receiver's own definition, bound to the entry's retained payload:
			// a second rule here disagreed with its export and refused every cut.
			// origin: hot-update S7 plan §8.1 / §8.3 / S7 FIX-TIERA2
			return nil, ErrWorkerState
		}
		if e.Status.DurableOffset != a.Checkpoint.DurableOffset || e.Status.ChunkIndex != a.Checkpoint.ChunkIndex ||
			e.Status.LastConsumedStreamSeq != a.Checkpoint.LastConsumedStreamSeq {
			return nil, ErrWorkerState
		}
		actorSeen[a.Identity] = true
		receivers[a.Identity] = true
	}
	return receivers, nil
}

func workerOutcomes(s xfercontract.WorkerState, entries map[xfercontract.Identity]xfercontract.WorkerEntry) (map[xfercontract.Identity]xfercontract.CanonicalOutcome, error) {
	actorSeen := make(map[xfercontract.Identity]bool)
	outcomes := make(map[xfercontract.Identity]xfercontract.CanonicalOutcome)
	// Decisions and canonical bytes form a single frozen projection. Neither
	// a matching transfer ID nor a terminal-shaped message is enough to ACK it.
	for _, o := range s.CanonicalOutcomes {
		e, ok := entries[o.Identity]
		if !ok || actorSeen[o.Identity] || !workerOutcome(o.Outcome, o.Identity, e.GateClosed) ||
			o.Outcome.Outcome == "" || !workerCanonical(o.Canonical, o.Identity) ||
			(o.Receipt != nil && !workerExactReceipt(*o.Receipt, o.Canonical, o.Outcome)) {
			return nil, ErrWorkerState
		}
		outcomes[o.Identity], actorSeen[o.Identity] = o, true
	}
	return outcomes, nil
}

func workerCouriers(s xfercontract.WorkerState, entries map[xfercontract.Identity]xfercontract.WorkerEntry,
	outcomes map[xfercontract.Identity]xfercontract.CanonicalOutcome) (map[xfercontract.Identity]bool, error) {
	actorSeen := make(map[xfercontract.Identity]bool)
	couriers := make(map[xfercontract.Identity]bool)
	for _, c := range s.Couriers {
		o, ok := outcomes[c.Identity]
		// A courier started from a real receiver outcome first waits for the
		// deciding broker's canonical bytes (and may not have attempted yet).
		// xfercourier.Import carries it; refusing it here refused every cut in
		// that round trip as invalid state instead of carrying the courier.
		// An unstarted entry never had a receiver, so its courier can only
		// carry the not-entered outcome of its closed gate.
		// origin: hot-update S7 plan §4 / §8.2 / T14 / FIX-INLINE (agent cut push flake)
		// origin: S7 internal review CUT-CAPACITY F2
		undecided := !ok && reflect.DeepEqual(c.Canonical, xfercontract.CanonicalRecord{}) && c.Receipt == nil && !c.Stopped && c.Outcome.Outcome != "" && workerOutcome(c.Outcome, c.Identity, true) &&
			(entries[c.Identity].Started || c.Outcome.Outcome == xfercontract.OutcomeNotEntered)
		if !ok && !undecided || actorSeen[c.Identity] || !undecided && (!reflect.DeepEqual(c.Outcome, o.Outcome) || !reflect.DeepEqual(c.Canonical, o.Canonical)) ||
			!workerCapsule(c.Capsule, c.Identity) || c.Cleanup.Identity != c.Identity ||
			!workerDeadline(c.NextAttempt, s.Lineage.BootID) && (!undecided || c.NextAttempt != (xfercontract.BootDeadline{})) || (c.Stopped && c.Receipt == nil) ||
			(c.Receipt != nil && !workerExactReceipt(*c.Receipt, c.Canonical, c.Outcome)) {
			return nil, ErrWorkerState
		}
		for _, reader := range c.Cleanup.Consumers {
			if !workerReader(reader, c.Identity) {
				return nil, ErrWorkerState
			}
		}
		if !workerFence(c.Cleanup.WriterFence) {
			return nil, ErrWorkerState
		}
		actorSeen[c.Identity] = true
		couriers[c.Identity] = true
	}
	// A retained decision with no exact ACK still owns publication. Dropping
	// the courier must not turn a terminal-pending cut into a settled cache.
	// origin: hot-update S7 plan §4 / §8.3 / T17
	for id, o := range outcomes {
		if o.Receipt == nil && !couriers[id] {
			return nil, ErrWorkerState
		}
	}
	return couriers, nil
}

// workerEntryActors closes the registry against its child slices: every phase
// keeps the actor it needs, and a settled entry keeps none.
func workerEntryActors(s xfercontract.WorkerState, entries map[xfercontract.Identity]xfercontract.WorkerEntry, senders, receivers map[xfercontract.Identity]bool,
	outcomes map[xfercontract.Identity]xfercontract.CanonicalOutcome, couriers map[xfercontract.Identity]bool) error {
	for id, e := range entries {
		// Cancelling or settling a prepare before its byte work started (a push
		// whose commit never came, or whose prepare reply was lost to its
		// admission deadline) closes its gate with no actor. Its not-entered
		// settlement still owns the slot through the exact ACK, and every cut
		// until then must carry it: Started names byte work, not courier duty.
		// Senders and receivers of an unstarted entry stay prepared-only.
		// origin: S7 internal review CUT-CAPACITY F2 / hot-update S7 plan §4 / §8.2 / T14
		decision, decided := outcomes[id]
		if !e.Started && e.Status.Phase != xfercontract.PhaseOpen && e.Status.Phase != xfercontract.PhasePrepared &&
			(!e.GateClosed || !couriers[id] && !decided || decided && decision.Outcome.Outcome != xfercontract.OutcomeNotEntered) {
			return ErrWorkerState
		}
		// Forward references alone cannot detect dropping a whole child slice.
		// Byte phases must retain their actor, even at the empty synced prefix.
		// origin: hot-update S7 plan §4 / §8.3 / T17
		if (e.Status.Phase == xfercontract.PhaseSending && !senders[id]) ||
			((e.Status.Phase == xfercontract.PhaseReceiving || e.Status.Phase == xfercontract.PhaseCommitReserved) && !receivers[id]) {
			return ErrWorkerState
		}
		if e.Status.Terminal != nil {
			o, ok := outcomes[id]
			if !ok || o.Receipt == nil || !workerExactReceipt(*e.Status.Terminal, o.Canonical, o.Outcome) ||
				!reflect.DeepEqual(e.Status.Terminal, o.Receipt) {
				return ErrWorkerState
			}
		}
		if e.Status.Phase == xfercontract.PhaseTerminalPending && !couriers[id] {
			return ErrWorkerState
		}
		if e.Status.Phase == xfercontract.PhaseSettled {
			if e.Status.State != xfercontract.StatusTerminal {
				return ErrWorkerState
			}
			for _, a := range s.Senders {
				if a.Identity == id {
					return ErrWorkerState
				}
			}
			for _, a := range s.Receivers {
				if a.Identity == id {
					return ErrWorkerState
				}
			}
			for _, c := range s.Couriers {
				if c.Identity == id && (!c.Stopped || c.Cleanup.TmpUnlinkPending || c.Cleanup.DirectorySyncPending ||
					c.Cleanup.ObjectPurgePending || c.Cleanup.PrimaryRetirePending || c.Cleanup.OutboxRetirePending) {
					return ErrWorkerState
				}
			}
		}
	}
	return nil
}

func workerRetention(s xfercontract.WorkerState, entries map[xfercontract.Identity]xfercontract.WorkerEntry) error {
	// origin: S7 internal review rv-lifecycle F5
	if len(s.Revocations) > len(entries) {
		return ErrWorkerState
	}
	actorSeen := make(map[xfercontract.Identity]bool)
	for _, r := range s.Revocations {
		e, ok := entries[r.Identity]
		if !ok || actorSeen[r.Identity] || r.Reason == "" || !e.GateClosed || !workerDeadline(r.At, s.Lineage.BootID) {
			return ErrWorkerState
		}
		actorSeen[r.Identity] = true
	}
	clear(actorSeen)
	for _, r := range s.ResultRetention {
		e, ok := entries[r.Identity]
		// The worker fixes a result's retention when the result exists: a
		// live entry carries a zero ResultUntil (setting a floor there would
		// shorten the result's own retention later). A decided result must
		// outlive its creation window.
		// origin: hot-update S7 plan §1.2 / T17 — S7 production activation
		undecided := e.Status.Terminal == nil && e.Status.Phase != xfercontract.PhaseSettled && r.ResultUntil.IsZero()
		if !ok || actorSeen[r.Identity] || r.CreateUntil != e.Prepare.Permit.CreateUntil ||
			(!undecided && r.ResultUntil.Before(r.CreateUntil)) || !workerDeadline(r.Deadline, s.Lineage.BootID) {
			return ErrWorkerState
		}
		actorSeen[r.Identity] = true
	}
	return nil
}

func workerCutCapacity(s xfercontract.WorkerState, entries map[xfercontract.Identity]xfercontract.WorkerEntry,
	resources map[string]workerResourceOwner, totals xfercontract.Reservation) error {
	// Count actual framing/copies and original+Prepared descriptor peaks. These
	// lower bounds cannot replace the owner's reservations for other cut domains,
	// reader windows, custody witnesses and salvage, but prevent understated cuts.
	// origin: hot-update S7 plan §8.3 / T17
	size := workerHeaderBytes
	if err := measureWorkerValue(reflect.ValueOf(s), &size); err != nil {
		return err
	}
	if len(entries) != 0 && (int64(size) > totals.SealedBytes || 2*int64(size) > totals.NonReplayBytes) {
		return ErrWorkerCapacity
	}
	if uint64(len(resources)) > uint64(totals.Resources) || 2*uint64(len(resources)) > uint64(totals.FDs) {
		return ErrWorkerCapacity
	}
	return nil
}

// Counters describe complete synced chunks, never a fetched suffix. SHA-256's
// marshaled length is independently bound to that offset; at EOF its digest
// must also equal the sealed source. Unused buffer bytes must be canonical.
// origin: hot-update S7 plan §3 / T17 / M08
func workerHash(state []byte, offset, size int64, digest string) bool {
	if offset < 0 || offset > size || len(state) != 108 || binary.BigEndian.Uint64(state[100:]) != uint64(offset) {
		return false
	}
	h := sha256.New()
	unmarshaler, canRestore := h.(encoding.BinaryUnmarshaler)
	marshaler, canExport := h.(encoding.BinaryMarshaler)
	if !canRestore || !canExport || unmarshaler.UnmarshalBinary(state) != nil {
		return false
	}
	b, err := marshaler.MarshalBinary()
	return err == nil && bytes.Equal(b, state) && (offset != size || hex.EncodeToString(h.Sum(nil)) == digest)
}

func workerCheckpoint(c xfercontract.SinkCheckpoint, spec xfercontract.ObjectSpec) bool {
	if c.ChunkIndex > spec.Chunks || c.DurableOffset < 0 || c.DurableOffset > spec.Size {
		return false
	}
	expected := int64(c.ChunkIndex) * int64(spec.ChunkSize)
	if expected > spec.Size {
		expected = spec.Size
	}
	return c.DurableOffset == expected && (c.ChunkIndex == 0) == (c.LastConsumedStreamSeq == 0) &&
		workerHash(c.SHA256State, c.DurableOffset, spec.Size, spec.SHA256)
}

func workerReader(r xfercontract.ReaderState, id xfercontract.Identity) bool {
	// An unopened prepared reader legitimately has no connection identity.
	// Dirty/delete obligations keep their original identities even at refs=0;
	// their phase must not silently turn that responsibility into stopped.
	// An open reader on its attachment with no consumer yet (or after a
	// refused cut retired it, before the next fetch) owes no deletion; the
	// jsstream reader exports exactly that shape, which this validator refused,
	// so a cut landing in that window was refused as invalid state.
	// origin: hot-update S7 plan §3 / §8.2 / T17 / FIX-INLINE (agent cut push flake)
	return workerObject(r.ObjectSpec, id) && r.Phase != "" && workerCheckpoint(r.SinkCheckpoint, r.ObjectSpec) &&
		(r.Phase != xfercontract.ReaderDirty || r.DirtyBarrier) &&
		(r.Phase != xfercontract.ReaderDeletePending || r.DeletePending) &&
		(r.Phase == xfercontract.ReaderStopped || (r.Phase == xfercontract.ReaderOpen && r.ConsumerIdentity == "" && (r.ConnectionIdentity == "" || workerID(r.ConnectionIdentity))) ||
			(workerID(r.ConsumerIdentity) && workerID(r.ConnectionIdentity)))
}

func workerWriter(w xfercontract.WriterState, id xfercontract.Identity) bool {
	spec := w.ObjectSpec
	if !workerObject(spec, id) || w.Phase == "" || len(w.Receipts) > int(spec.Chunks) || w.AttachmentGeneration == 0 || !workerFence(w.RetirementFence) {
		return false
	}
	var offset int64
	var seq uint64
	// Physical stream holes are permitted; logical chunk holes are not. Each
	// ACK advances exactly one complete geometry-sized chunk in this version.
	for i, r := range w.Receipts {
		n := int64(spec.ChunkSize)
		if n > spec.Size-offset {
			n = spec.Size - offset
		}
		if r.Index != uint32(i) || r.Offset != offset || int64(r.Length) != n || n <= 0 || r.StreamSeq <= seq {
			return false
		}
		offset, seq = offset+n, r.StreamSeq
	}
	if p := w.PendingAttempt; p != nil {
		// Pending bytes, both CAS fences and attachment generation are immutable
		// together. Adopting only the cursor would duplicate an ACK-lost chunk.
		n := int64(spec.ChunkSize)
		if n > spec.Size-offset {
			n = spec.Size - offset
		}
		if p.Index != uint32(len(w.Receipts)) || p.Index >= spec.Chunks || p.Offset != offset || int64(p.Length) != n ||
			int64(len(p.Bytes)) != n || sha256.Sum256(p.Bytes) != p.SHA256 || p.PreviousSubjectSeq != seq ||
			p.ExpectedStreamSeq < seq || p.AttachmentGeneration != w.AttachmentGeneration || w.Phase != xfercontract.WritePending {
			return false
		}
	} else if w.Phase == xfercontract.WritePending {
		return false
	}
	if w.Phase == xfercontract.WriteMetadataPending || w.Phase == xfercontract.WriteFinalized {
		if offset != spec.Size || len(w.Receipts) != int(spec.Chunks) || w.PendingAttempt != nil {
			return false
		}
	}
	if m := w.MetaReceipt; m != nil {
		if m.Spec != spec || m.MetadataSeq <= seq || m.MetadataDigest != workerDigest(m.Metadata) || len(m.Metadata) == 0 {
			return false
		}
	} else if w.Phase == xfercontract.WriteFinalized {
		return false
	}
	return w.Phase != xfercontract.WriteRetired || (w.RetirementFence.Purged && w.PendingAttempt == nil)
}

func workerReceiptOffset(receipts []xfercontract.ChunkReceipt) int64 {
	var n int64
	for _, r := range receipts {
		n += int64(r.Length)
	}
	return n
}

func workerFence(f xfercontract.RetirementFence) bool {
	// Purge is the last step, after a definite fence ACK later than all old
	// attempts. A retired local writer alone says nothing about late HPUB.
	if f.Receipt != nil && (f.Receipt.Sequence <= f.ExpectedStreamSeq || f.Receipt.Sequence <= f.PreviousMetadataSeq ||
		f.Receipt.Stream == "" || !workerSHA(f.Receipt.RecordDigest)) {
		return false
	}
	return !f.Purged || (f.WriterRetired && f.ConsumersRetired && f.Receipt != nil)
}

type workerResourceOwner struct {
	ref      xfercontract.ResourceRef
	identity xfercontract.FileIdentity
	owner    xfercontract.Identity
}

// References carry both ID and basename. No integer descriptor or pathname
// reopen can replace the leaf's two-argument TakeFile closure.
// origin: hot-update S7 plan §3 / §8.3 / T17
func workerResource(seen map[string]workerResourceOwner, ref xfercontract.ResourceRef, fingerprint xfercontract.FileIdentity, kind string, owner xfercontract.Identity, required bool) bool {
	if ref == (xfercontract.ResourceRef{}) {
		return !required && fingerprint == (xfercontract.FileIdentity{})
	}
	name, mode, access := xfercontract.SourceFileName, uint32(0o100000), int32(0)
	if kind == xfercontract.ResourceTmp {
		name, access = xfercontract.TmpFileName, 1
	}
	if kind == xfercontract.ResourceParent {
		name, mode = xfercontract.ParentFileName, 0o040000
	}
	if !workerID(ref.ResourceID) || ref.Name != name || ref.Kind != kind || fingerprint.Dev == 0 || fingerprint.Ino == 0 ||
		fingerprint.Mode&0o170000 != mode || fingerprint.Flags&3 != access {
		return false
	}
	value := workerResourceOwner{ref, fingerprint, owner}
	if previous, ok := seen[ref.ResourceID]; ok {
		return previous == value
	}
	seen[ref.ResourceID] = value
	return true
}

func workerAtomic(a xfercontract.AtomicState, id xfercontract.Identity, closed bool, refs map[string]workerResourceOwner) bool {
	if !workerPhase(a.Phase) || a.DurableOffset < 0 || !workerOutcome(a.PrimitiveOutcome, id, closed) ||
		(a.CommitGrant != (xfercontract.CommitGrant{}) && !workerGrant(a.CommitGrant, id)) ||
		(a.PrimitiveOutcome.Grant != (xfercontract.CommitGrant{}) && a.PrimitiveOutcome.Grant != a.CommitGrant) {
		return false
	}
	// The primitive itself is an indivisible local cut. Serializing inflight
	// would let a new owner repeat an already entered rename/link operation.
	if a.PrimitiveOutcome.Outcome == xfercontract.OutcomeInflight {
		return false
	}
	landed := a.PrimitiveOutcome.Outcome == xfercontract.OutcomeLanded
	if a.DirSyncPending != (landed && !a.PrimitiveOutcome.ParentSynced) || a.UnlinkPending != a.PrimitiveOutcome.TmpCleanupPending {
		return false
	}
	parent, tmp, source := false, false, false
	switch a.Phase {
	case xfercontract.PhaseOpen, xfercontract.PhasePrepared:
		parent = true
	case xfercontract.PhaseSending:
		source = true
	case xfercontract.PhaseReceiving, xfercontract.PhaseCommitReserved:
		parent, tmp = true, true
	case xfercontract.PhaseDecided, xfercontract.PhaseTerminalPending, xfercontract.PhaseHistoryConfirmed, xfercontract.PhaseCleanupPending:
		parent, tmp = a.DirSyncPending || a.UnlinkPending, a.UnlinkPending
	case xfercontract.PhaseSettled:
		if a.Tmp != (xfercontract.ResourceRef{}) || a.Parent != (xfercontract.ResourceRef{}) || a.Source != (xfercontract.ResourceRef{}) || a.DirSyncPending || a.UnlinkPending {
			return false
		}
	default:
		return false
	}
	if (a.Phase == xfercontract.PhaseCommitReserved || landed) && !workerGrant(a.CommitGrant, id) {
		return false
	}
	// The phase names the complete live FD closure. Extra references in a
	// courier-only or prepared cut are as invalid as missing receiving files.
	// origin: hot-update S7 plan §3 / T17
	if (!tmp && a.Tmp != (xfercontract.ResourceRef{})) || (!parent && a.Parent != (xfercontract.ResourceRef{})) ||
		(!source && a.Source != (xfercontract.ResourceRef{})) {
		return false
	}
	if a.Tmp != (xfercontract.ResourceRef{}) && (!workerBasename(a.TmpName) || !workerBasename(a.Destination)) {
		return false
	}
	return workerResource(refs, a.Tmp, a.TmpIdentity, xfercontract.ResourceTmp, id, tmp) &&
		workerResource(refs, a.Parent, a.ParentIdentity, xfercontract.ResourceParent, id, parent) &&
		workerResource(refs, a.Source, a.SourceIdentity, xfercontract.ResourceSource, id, source)
}

func workerGrant(g xfercontract.CommitGrant, id xfercontract.Identity) bool {
	return g.Identity == id && workerID(g.GrantNonce) && workerID(g.ReceiverInstance) && g.TerminalID == workerTerminalID(id)
}

func workerTerminalID(id xfercontract.Identity) string {
	return workerDigest([]byte("xfer-terminal-v1\n" + id.SID + "\n" + id.TransferID + "\n" + id.EntryNonce))
}

func workerOutcome(o xfercontract.ReceiverOutcome, id xfercontract.Identity, closed bool) bool {
	// Absence is unresolved, not a failed commit. In particular an empty grant
	// can certify a closed gate but cannot authorize a landed filesystem effect.
	if o == (xfercontract.ReceiverOutcome{}) {
		return true
	}
	if o.Bytes < 0 || (o.SHA256 != "" && !workerSHA(o.SHA256)) {
		return false
	}
	if o.Grant != (xfercontract.CommitGrant{}) && !workerGrant(o.Grant, id) {
		return false
	}
	switch o.Outcome {
	case xfercontract.OutcomeNotEntered:
		return closed && !o.ParentSynced && !o.TmpCleanupPending
	case xfercontract.OutcomeInflight:
		return workerGrant(o.Grant, id) && !o.ParentSynced
	case xfercontract.OutcomeLanded:
		return workerGrant(o.Grant, id) && o.Primitive != "" && workerSHA(o.SHA256)
	default:
		return false
	}
}

func workerCanonical(c xfercontract.CanonicalRecord, id xfercontract.Identity) bool {
	return c.Identity == id && (c.Kind == "complete" || c.Kind == "failed") && workerID(c.ReqID) && len(c.Bytes) > 0 && c.Digest == workerDigest(c.Bytes)
}

func workerReceipt(r xfercontract.TerminalReceipt) bool {
	return workerSHA(r.TerminalID) && workerSHA(r.CanonicalDigest) && workerID(r.ReqID) && r.HistoryStream != "" && r.HistorySeq > 0 &&
		r.Bytes >= 0 && (r.Kind == "complete" || r.Kind == "failed") && (!r.OutcomeUnknown || (r.Kind == "failed" && r.Code == xfercontract.ReasonSettlementAbandoned))
}

func workerExactReceipt(r xfercontract.TerminalReceipt, c xfercontract.CanonicalRecord, o xfercontract.ReceiverOutcome) bool {
	return workerReceipt(r) && r.TerminalID == workerTerminalID(c.Identity) && r.Kind == c.Kind && r.ReqID == c.ReqID &&
		r.CanonicalDigest == c.Digest && r.Bytes == o.Bytes && r.SHA256 == o.SHA256 && r.Code == o.Code &&
		(r.Kind != "complete" || (o.Outcome == xfercontract.OutcomeLanded && o.ParentSynced))
}

func workerCapsule(c xfercontract.SettlementCapsule, id xfercontract.Identity) bool {
	return c.Identity == id && workerOrigin(c.Origin) && c.ReceiverNkey != "" && workerID(c.Issuer) && workerID(c.Nonce) && c.Signature != ""
}

func workerDeadline(d xfercontract.BootDeadline, boot string) bool {
	return d.BootID == boot && d.Deadline > 0
}

func workerBudget(b xfercontract.BudgetSeal) bool {
	// Preserve the original UTC start and rate-derived seal. The codec never
	// reads a clock or extends an expired byte budget to cover settlement work.
	return !b.StartedAt.IsZero() && b.Size >= 0 && b.Size <= 2<<30 && b.RateLimitBPS >= 0 && b.EffectiveBudgetNS > 0 &&
		b.EffectiveBudgetNS <= int64(2108*time.Second) && b.RemainingNS >= 0 && b.RemainingNS <= b.EffectiveBudgetNS
}

func workerIntent(i xfercontract.Intent) bool {
	return (i.Verb == "push" || i.Verb == "pull") && (i.Tier == "a" || i.Tier == "b") && i.Path != "" &&
		i.Size >= 0 && i.Size <= 2<<30 && i.RateLimitBPS >= 0 && (i.SHA256 == "" || workerSHA(i.SHA256))
}

func workerSourceSeal(s xfercontract.SourceSeal) bool {
	geometry := workerGeometry(s.Size, s.ChunkSize, s.Chunks) || (s.Size >= 0 && s.Size <= 8<<20 && s.ChunkSize == 0 && s.Chunks == 0)
	return geometry && workerSHA(s.SHA256) && s.SourceFingerprint != "" && s.RateLimitBPS >= 0
}

func workerObject(s xfercontract.ObjectSpec, id xfercontract.Identity) bool {
	return s.Identity == id && workerID(id.ObjectVersion) && s.Bucket != "" && s.Stream != "" && s.StreamCreated != "" &&
		workerSHA(s.SHA256) && workerGeometry(s.Size, s.ChunkSize, s.Chunks)
}

func workerGeometry(size int64, chunk, chunks uint32) bool {
	return size >= 0 && size <= 2<<30 && chunk >= 32<<10 && chunk <= 1<<20 && chunks <= workerMaxItems &&
		int64(chunks) == (size+int64(chunk)-1)/int64(chunk)
}

func workerReservation(r xfercontract.Reservation, totals *xfercontract.Reservation) bool {
	values := []int64{r.ReplayBytes, r.NonReplayBytes, r.SealedBytes, r.PublicBytes, r.HandoffBytes}
	sums := []*int64{&totals.ReplayBytes, &totals.NonReplayBytes, &totals.SealedBytes, &totals.PublicBytes, &totals.HandoffBytes}
	caps := []int64{256 << 20, 64 << 20, MaxWorkerStateBytes, 16 << 20, 64 << 20}
	if r == (xfercontract.Reservation{}) {
		return false
	}
	for i, n := range values {
		if n < 0 || n > caps[i]-*sums[i] {
			return false
		}
		*sums[i] += n
	}
	if r.FDs > 16384-totals.FDs || r.Resources > 16384-totals.Resources {
		return false
	}
	totals.FDs += r.FDs
	totals.Resources += r.Resources
	return true
}

func workerIdentity(id xfercontract.Identity) bool {
	// The actor is a canonical fingerprint ("SHA256:" + base64), not an ID token.
	// origin: S7 production activation — real actors made every cut unrepresentable
	return workerID(id.SID) && workerID(id.NID) && (workerID(id.ActorFP) || xferid.ActorFP(id.ActorFP)) && workerID(id.AgentInstance) &&
		workerID(id.TransferID) && workerID(id.EntryNonce) && workerSHA(id.IntentDigest)
}

func workerOrigin(o xfercontract.Origin) bool {
	return workerID(o.BrokerID) && workerID(o.InstanceID) && workerID(o.BootID) && o.Generation > 0
}
func workerDigest(b []byte) string { d := sha256.Sum256(b); return hex.EncodeToString(d[:]) }
func workerSHA(s string) bool {
	if len(s) != 64 {
		return false
	}
	for _, c := range s {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}
func workerID(s string) bool {
	return len(s) > 0 && len(s) <= 256 && strings.IndexFunc(s, func(c rune) bool {
		return (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9') && c != '.' && c != '_' && c != '-'
	}) < 0
}
func workerBasename(s string) bool {
	return s != "" && s != "." && s != ".." && !strings.ContainsAny(s, "/\x00")
}
func workerPhase(p xfercontract.Phase) bool {
	switch p {
	case xfercontract.PhaseOpen, xfercontract.PhasePrepared, xfercontract.PhaseSending, xfercontract.PhaseReceiving,
		xfercontract.PhaseCommitReserved, xfercontract.PhaseDecided, xfercontract.PhaseTerminalPending,
		xfercontract.PhaseHistoryConfirmed, xfercontract.PhaseCleanupPending, xfercontract.PhaseSettled:
		return true
	default:
		return false
	}
}

// Optional actors contain zero enum fields. Nonzero values still belong to the
// frozen vocabulary; required phases are checked by their record validators.
func workerVocabulary(v reflect.Value) bool {
	if v.Type() == reflect.TypeFor[time.Time]() {
		return true
	}
	if v.Kind() == reflect.String && v.String() != "" {
		s := v.String()
		switch v.Type() {
		case reflect.TypeFor[xfercontract.Phase]():
			return workerPhase(xfercontract.Phase(s))
		case reflect.TypeFor[xfercontract.StatusState]():
			return s == "unknown" || s == "live" || s == "terminal"
		case reflect.TypeFor[xfercontract.ObjectWritePhase]():
			return s == "open" || s == "pending" || s == "metadata_pending" || s == "finalized" || s == "retiring" || s == "retired"
		case reflect.TypeFor[xfercontract.ReaderPhase]():
			return s == "open" || s == "fetching" || s == "dirty" || s == "delete_pending" || s == "stopped"
		case reflect.TypeFor[xfercontract.CommitPrimitive]():
			return s == "rename" || s == "link"
		case reflect.TypeFor[xfercontract.PrimitiveOutcome]():
			return s == "not_entered" || s == "inflight" || s == "landed"
		case reflect.TypeFor[xfercontract.CancelReason]():
			return s == "user" || s == "deadline" || s == "revoked" || s == "stopped"
		case reflect.TypeFor[controlbudget.AttemptPhase]():
			return controlbudget.ValidPhase(controlbudget.AttemptPhase(s))
		}
	}
	switch v.Kind() {
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			if !workerVocabulary(v.Field(i)) {
				return false
			}
		}
	case reflect.Pointer:
		if !v.IsNil() {
			return workerVocabulary(v.Elem())
		}
	case reflect.Slice, reflect.Array:
		if v.Type().Elem().Kind() == reflect.Uint8 {
			return true
		}
		for i := 0; i < v.Len(); i++ {
			if !workerVocabulary(v.Index(i)) {
				return false
			}
		}
	case reflect.Bool, reflect.Uint8, reflect.Uint32, reflect.Int32, reflect.Uint64, reflect.Int64, reflect.String:
		// Scalar leaves; a nonempty string vocabulary was checked above.
	case reflect.Invalid, reflect.Int, reflect.Int8, reflect.Int16, reflect.Uint, reflect.Uint16, reflect.Uintptr,
		reflect.Float32, reflect.Float64, reflect.Complex64, reflect.Complex128, reflect.Chan, reflect.Func,
		reflect.Interface, reflect.Map, reflect.UnsafePointer:
		// The binary codec refuses these kinds, so no valid image carries one.
		return false
	}
	return true
}

// Reserve belongs to the receiver. The atomic sink learns that immutable grant
// only at Commit, after streaming; its empty grant before that call is real
// state, not a missing decision. A entered primitive must carry the exact grant.
// origin: hot-update S7 plan §3 / §4 / T17
func workerAtomicGrant(a xfercontract.ReceiverState) bool {
	if a.Grant == a.Atomic.CommitGrant {
		return true
	}
	return a.Atomic.CommitGrant == (xfercontract.CommitGrant{}) && a.Atomic.PrimitiveOutcome == (xfercontract.ReceiverOutcome{}) &&
		a.Outcome == (xfercontract.ReceiverOutcome{}) && (a.Atomic.Phase == xfercontract.PhasePrepared || a.Atomic.Phase == xfercontract.PhaseReceiving)
}

// Validate the immutable control shape before any registry side effect.
// origin: hot-update S7 plan §4 / §8.1
func ValidWorkerControl(c xfercontract.Control) bool {
	switch c.Kind {
	case xfercontract.ControlStart:
		return c.Grant == nil && c.Outcome == nil && c.Receipt == nil && c.Reason == ""
	case xfercontract.ControlGrant:
		return c.Grant != nil && c.Outcome == nil && c.Receipt == nil && c.Reason == "" && c.Grant.Identity == c.Identity && c.Grant.GrantNonce != "" && c.Grant.ReceiverInstance != "" && c.Grant.TerminalID != ""
	case xfercontract.ControlCancel, xfercontract.ControlRevoke:
		return c.Grant == nil && c.Outcome == nil && c.Receipt == nil && validWorkerCancelReason(c.Reason)
	case xfercontract.ControlReceipt:
		return c.Grant == nil && c.Outcome == nil && c.Receipt != nil && c.Reason == ""
	default:
		return false
	}
}

// Validate the immutable control shape before any registry side effect.
// origin: hot-update S7 plan §4 / §8.1
func validWorkerCancelReason(reason xfercontract.CancelReason) bool {
	switch reason {
	case xfercontract.CancelUser, xfercontract.CancelDeadline, xfercontract.CancelRevoked, xfercontract.CancelStopped:
		return true
	default:
		return false
	}
}

// The entry's single reservation is carried in the sealed image and adopted on
// import, so it must cover everything that image can retain for the entry.
func WorkerPreparationReservation(p xfercontract.Prepare) xfercontract.Reservation {
	return WorkerInlineReservation(p, int64(len(p.Inline)))
}

// WorkerInlineReservation sizes an entry that retains inline Tier A bytes: a
// push's authenticated payload, or a pull's copy of its sealed source, whose
// size the registry learns only from that seal. Tier A has no chunk window,
// so the per-chunk term cannot cover the cut's codec working set: the codec
// holds the whole retained state twice (value and encoded output), and that
// state carries the inline bytes once beside the entry/actor records the
// sealed floor bounds. Without this a Tier A image was never representable.
// origin: hot-update S7 plan §8.1 / §8.3 / FIX-INLINE
func WorkerInlineReservation(p xfercontract.Prepare, inline int64) xfercontract.Reservation {
	// Reserve both inline copies and worst-case per-chunk receipt/codec bytes
	// before actors allocate. The shared account enforces the whole-cut limits.
	// Tier B always has a validated 32KiB–1MiB geometry; Tier A has no chunks.
	// A Tier B sender's image also retains its one pending attempt (a whole
	// chunk of bytes, whenever its ACK is unknown at freeze), and the codec
	// holds that and every receipt twice; a reader window is four chunks. A
	// pull's intent has no size, so its sealed share was the bare 8KiB floor
	// and its non-replay share ignored receipts: a 9MiB pull sender with its
	// 72 receipts exported 8596 bytes, and its cut was refused as over
	// capacity. The registry now grows this reservation from the sealed
	// geometry before the first chunk (registry reserveSealed).
	// origin: S7 internal review CUT-CAPACITY F1 / hot-update S7 plan §2.1 / §8.3
	chunks, pending := int64(0), int64(0)
	if p.Intent.Tier != "a" {
		pending, chunks = int64(p.Intent.ChunkSize), (p.Intent.Size+int64(p.Intent.ChunkSize)-1)/max(int64(p.Intent.ChunkSize), 1)
	}
	return xfercontract.Reservation{ReplayBytes: inline, NonReplayBytes: 2*(8192+inline+pending+chunks*192) + 4*pending, SealedBytes: 8192 + 3*inline + pending + chunks*192, HandoffBytes: 4096, FDs: 9, Resources: 3}
}
