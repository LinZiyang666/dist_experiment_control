package infratransfer

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"sync/atomic"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	i "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/jsstream"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats.go/jetstream"
)

// ObservationSchema fingerprints the actual S7 typed observation closure.
// Source and fixture receipts are separate: a schema hash cannot certify that
// a supplied executable ran, or that the four original transfers stayed live.
// origin: hot-update S9 plan §8 / §9.1 / P19
func ObservationSchema() i.Digest {
	var fields []string
	seen := make(map[reflect.Type]bool)
	var visit func(reflect.Type)
	visit = func(t reflect.Type) {
		if seen[t] {
			return
		}
		seen[t] = true
		switch t.Kind() {
		case reflect.Pointer, reflect.Slice, reflect.Array:
			visit(t.Elem())
		case reflect.Invalid, reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
			reflect.Float32, reflect.Float64, reflect.Complex64, reflect.Complex128, reflect.Chan,
			reflect.Func, reflect.Interface, reflect.Map, reflect.String, reflect.UnsafePointer:
			// These leaf kinds have no fields in the observation record closure.
		case reflect.Struct:
			for n := 0; n < t.NumField(); n++ {
				f := t.Field(n)
				fields = append(fields, t.String()+"."+f.Name+":"+f.Type.String()+":"+string(f.Tag))
				visit(f.Type)
			}
		}
	}
	visit(reflect.TypeFor[x.Observation]())
	return digest("s7-observation-v1", fields)
}

// These bytes are temporary private hash input, never a bundle, log or file.
// The existing S7 state codecs remain unchanged; S9 adds no image state.
func digest(domain string, v any) i.Digest {
	b, err := json.Marshal(v)
	if err != nil {
		return i.Digest{}
	}
	return sha256.Sum256(append([]byte(domain+"\n"), b...))
}

func retired(r x.ReaderState) bool {
	// ConsumerIdentity names the old consumer whose deletion Freeze confirmed.
	// It is evidence, not permission to adopt that name on the successor.
	return !r.DirtyBarrier && !r.DeletePending &&
		(r.Phase == x.ReaderOpen || r.Phase == x.ReaderStopped || r.ObjectSpec == (x.ObjectSpec{}))
}

// checkpoint covers whole private values, including sent-unknown publish bytes,
// canonical terminal/courier retry state and original deadlines. Only hashes
// escape. Never substitute final SHA or a global stream cursor for these rows.
// origin: hot-update S9 plan §4.3 / §8 / T42–T43
func checkpoint(a *Adapter, d i.Deadline) (i.TransferSeal, error) {
	var invocations, receipts, consumers []i.Digest
	writers, readers := make(map[x.Identity]x.WriterState), make(map[x.Identity]x.ReaderState)
	coveredWriters, coveredReaders := make(map[x.Identity]bool), make(map[x.Identity]bool)
	for _, w := range a.cfg.Writers {
		if _, exists := writers[w.state.ObjectSpec.Identity]; exists {
			return i.TransferSeal{}, i.ErrIncomplete
		}
		writers[w.state.ObjectSpec.Identity] = w.state
	}
	for _, r := range a.cfg.Readers {
		if _, exists := readers[r.state.ObjectSpec.Identity]; exists {
			return i.TransferSeal{}, i.ErrIncomplete
		}
		readers[r.state.ObjectSpec.Identity] = r.state
	}
	for _, c := range a.workers {
		invocations = append(invocations, digest("worker-invocations", c.state.Entries))
		receipts = append(receipts, digest("worker-receipts", c.state))
		for _, s := range c.state.Senders {
			// Worker.Thaw replaces the control transport only. Every tier-B
			// child must also hold our registered stable data port; otherwise
			// a complete seal would leave it attached to the retired backend.
			// origin: hot-update S9 plan §8 / T42
			if s.Writer.ObjectSpec != (x.ObjectSpec{}) && !reflect.DeepEqual(writers[s.Identity], s.Writer) {
				return i.TransferSeal{}, i.ErrIncomplete
			}
			coveredWriters[s.Identity] = true
		}
		for _, r := range c.state.Receivers {
			if r.Reader.ObjectSpec != (x.ObjectSpec{}) && !reflect.DeepEqual(readers[r.Identity], r.Reader) {
				return i.TransferSeal{}, i.ErrIncomplete
			}
			coveredReaders[r.Identity] = true
			if !retired(r.Reader) {
				return i.TransferSeal{}, i.ErrIncomplete
			}
			consumers = append(consumers, digest("worker-consumer", r.Reader))
		}
		for _, courier := range c.state.Couriers {
			for _, r := range courier.Cleanup.Consumers {
				if !retired(r) {
					return i.TransferSeal{}, i.ErrIncomplete
				}
				consumers = append(consumers, digest("courier-consumer", r))
			}
		}
	}
	for _, c := range a.trackers {
		invocations = append(invocations, digest("tracker-invocations", c.state.Entries))
		receipts = append(receipts, digest("tracker-receipts", c.state))
		for _, cleanup := range c.state.Cleanup {
			for _, r := range cleanup.Consumers {
				if !retired(r) {
					return i.TransferSeal{}, i.ErrIncomplete
				}
				consumers = append(consumers, digest("tracker-consumer", r))
			}
		}
	}
	for _, w := range a.cfg.Writers {
		if a.cfg.FenceData == nil && !coveredWriters[w.state.ObjectSpec.Identity] {
			return i.TransferSeal{}, i.ErrUnsupported
		}
		invocations = append(invocations, digest("writer-invocation", w.state.ObjectSpec.Identity))
		receipts = append(receipts, digest("writer-receipts", w.state))
	}
	for _, r := range a.cfg.Readers {
		if a.cfg.FenceData == nil && !coveredReaders[r.state.ObjectSpec.Identity] {
			return i.TransferSeal{}, i.ErrUnsupported
		}
		if !retired(r.state) {
			return i.TransferSeal{}, i.ErrIncomplete
		}
		invocations = append(invocations, digest("reader-invocation", r.state.ObjectSpec.Identity))
		receipts = append(receipts, digest("reader-checkpoint", r.state))
		consumers = append(consumers, digest("reader-consumer", r.state))
	}
	// Registration order is not authority. Bind each seal to its transaction
	// as well, so an identical empty cohort cannot release another transaction.
	for _, set := range [][]i.Digest{invocations, receipts, consumers} {
		for _, row := range set {
			if row == (i.Digest{}) {
				return i.TransferSeal{}, i.ErrIncomplete
			}
		}
		sort.Slice(set, func(n, m int) bool { return string(set[n][:]) < string(set[m][:]) })
	}
	if _, err := controlbudget.Remaining(d); err != nil {
		return i.TransferSeal{}, err
	}
	return i.TransferSeal{Version: 1, SourceSHA256: a.cfg.SourceSHA256, SchemaSHA256: a.cfg.SchemaSHA256,
		InvocationSet: digest("infra-invocations", struct {
			Tx   i.ID
			Rows []i.Digest
		}{a.tx, invocations}),
		ReceiptSet: digest("infra-receipts", receipts), ConsumerSet: digest("infra-consumers", consumers), Complete: true}, nil
}

// DataCurrent resolves a registered independent transfer data connection. P15
// must include it in the same cohort as control NCs; sharing a control client
// here would bypass the physical attachment proof and writer no-buffer policy.
type DataCurrent func(i.Deadline) (jetstream.JetStream, error)

type objectSlot struct {
	serial                chan struct{}
	current               DataCurrent
	fenced, cached, bound bool
	stopped               atomic.Bool
}

// Stable ports let S7 actors keep their original source/sink and retry loops
// while replacing only the JS backend. There is deliberately no Put or ID
// factory, and Rebind never enters a commit primitive or ACKs a courier.
type Writer struct {
	objectSlot
	owner   x.ObjectWriter
	state   x.WriterState
	observe func(x.Observation)
}
type Reader struct {
	objectSlot
	owner   x.ObjectReader
	state   x.ReaderState
	pending x.ObjectReader // A constructor still owns its window if rollback Stop expires.
}

var _ x.ObjectWriter = (*Writer)(nil)
var _ x.ObjectReader = (*Reader)(nil)

// WrapWriter/WrapReader take lifecycle ownership of data ports on success.
// Business owners still own Stop. The adapter borrows these stable wrappers.
func WrapWriter(owner x.ObjectWriter, current DataCurrent) (*Writer, error) {
	if absent(owner) || current == nil {
		return nil, i.ErrIncomplete
	}
	return &Writer{objectSlot: objectSlot{serial: make(chan struct{}, 1), current: current}, owner: owner}, nil
}
func WrapReader(owner x.ObjectReader, current DataCurrent) (*Reader, error) {
	if absent(owner) || current == nil {
		return nil, i.ErrIncomplete
	}
	return &Reader{objectSlot: objectSlot{serial: make(chan struct{}, 1), current: current}, owner: owner}, nil
}

// SetWriterObserver retains the actual S7 emitter across backend replacement.
// Installing it only on the old concrete writer would silently lose custody
// samples after a cut, even though the business transfer still made progress.
// origin: hot-update S9 plan §8 / §9.1 / T42
func SetWriterObserver(w *Writer, observe func(x.Observation), d i.Deadline) error {
	if w == nil {
		return i.ErrIncomplete
	}
	if err := take(w.serial, d); err != nil {
		return err
	}
	defer func() { <-w.serial }()
	if w.stopped.Load() {
		return i.ErrUnsupported
	}
	if err := jsstream.SetTransferWriterObserver(w.owner, observe, d); err != nil {
		return err
	}
	w.observe = observe
	return nil
}

var errFenced = errors.New(x.CodeOwnerRecovering)

func call[T any](s *objectSlot, d i.Deadline, fn func() (T, error)) (v T, err error) {
	if err = take(s.serial, d); err != nil {
		return v, err
	}
	defer func() { <-s.serial }()
	if s.fenced || s.stopped.Load() {
		return v, errFenced
	}
	return fn()
}

func (w *Writer) Append(idx uint32, b []byte, d x.Deadline) (x.ChunkReceipt, error) {
	return call(&w.objectSlot, d, func() (x.ChunkReceipt, error) { return w.owner.Append(idx, b, d) })
}
func (w *Writer) ResolvePending(d x.Deadline) (x.ChunkReceipt, bool, error) {
	type result struct {
		receipt x.ChunkReceipt
		found   bool
	}
	r, err := call(&w.objectSlot, d, func() (result, error) {
		r, found, err := w.owner.ResolvePending(d)
		return result{r, found}, err
	})
	return r.receipt, r.found, err
}
func (w *Writer) Finalize(d x.Deadline) (x.ObjectReceipt, error) {
	return call(&w.objectSlot, d, func() (x.ObjectReceipt, error) { return w.owner.Finalize(d) })
}

// S7 sender admission inspects a writer through this additive package seam.
// Inspection must remain read-only: substituting Freeze would close the first
// append before the sender had published any bytes.
// origin: hot-update S9 plan §8 / T42
func (w *Writer) Snapshot(d x.Deadline) (x.WriterState, error) {
	return call(&w.objectSlot, d, func() (x.WriterState, error) {
		port, ok := w.owner.(interface {
			Snapshot(x.Deadline) (x.WriterState, error)
		})
		if !ok {
			return x.WriterState{}, i.ErrUnsupported
		}
		return port.Snapshot(d)
	})
}
func (w *Writer) Freeze(d x.Deadline) (x.WriterState, error) {
	if err := take(w.serial, d); err != nil {
		return x.WriterState{}, err
	}
	defer func() { <-w.serial }()
	w.fenced = true
	if w.stopped.Load() {
		return x.WriterState{}, i.ErrUnsupported
	}
	if !w.cached {
		state, err := w.owner.Freeze(d)
		if err != nil {
			return x.WriterState{}, err
		}
		w.state, w.cached = state, true
	}
	return copyState(w.state)
}
func (w *Writer) Stop(d x.Deadline) error {
	// origin: hot-update S9 plan §8 / T42 — timeout does not undo a
	// business Stop. Close future admission before waiting for the current IO.
	w.stopped.Store(true)
	if err := take(w.serial, d); err != nil {
		return err
	}
	defer func() { <-w.serial }()
	w.fenced = true
	return w.owner.Stop(d)
}
func (w *Writer) rebind(d i.Deadline) error {
	if err := take(w.serial, d); err != nil {
		return err
	}
	defer func() { <-w.serial }()
	if !w.fenced || !w.cached || w.stopped.Load() {
		return i.ErrIncomplete
	}
	if w.bound {
		return nil
	}
	js, err := w.current(d)
	if err != nil {
		return err
	}
	// origin: hot-update S9 plan §8 / T42 — use the original spec, receipts,
	// pending attempt and metadata. NewTransferWriter would lose ACK authority.
	next, err := jsstream.ImportTransferWriter(js, w.state)
	if err != nil {
		return err
	}
	if err = jsstream.SetTransferWriterObserver(next, w.observe, d); err != nil {
		return errors.Join(err, next.Stop(d))
	}
	if err = w.owner.Stop(d); err != nil {
		return errors.Join(err, next.Stop(d))
	}
	w.owner, w.bound = next, true
	return nil
}
func (w *Writer) release(d i.Deadline) error {
	if err := take(w.serial, d); err != nil {
		return err
	}
	defer func() { <-w.serial }()
	if !w.fenced {
		return nil
	} // A retry after another owner refused Release.
	if !w.bound || w.stopped.Load() {
		return i.ErrIncomplete
	}
	if err := jsstream.ThawTransferWriter(w.owner, d); err != nil {
		return err
	}
	w.fenced, w.cached, w.bound = false, false, false
	return nil
}

func (r *Reader) ReadChunk(d x.Deadline) (x.Chunk, error) {
	return call(&r.objectSlot, d, func() (x.Chunk, error) { return r.owner.ReadChunk(d) })
}
func (r *Reader) ConfirmSynced(cp x.SinkCheckpoint) error {
	// This frozen signature has no deadline. Never spawn a background waiter;
	// callers must finish their write/Sync/confirm section before cohort sealing.
	select {
	case r.serial <- struct{}{}:
		defer func() { <-r.serial }()
	default:
		return errFenced
	}
	if r.fenced || r.stopped.Load() {
		return errFenced
	}
	return r.owner.ConfirmSynced(cp)
}
func (r *Reader) Freeze(d x.Deadline) (x.ReaderState, error) {
	if err := take(r.serial, d); err != nil {
		return x.ReaderState{}, err
	}
	defer func() { <-r.serial }()
	r.fenced = true
	if r.stopped.Load() {
		return x.ReaderState{}, i.ErrUnsupported
	}
	if !r.cached {
		state, err := r.owner.Freeze(d)
		if err != nil {
			return x.ReaderState{}, err
		}
		// origin: hot-update S9 plan §8 / T43 — UNSUB/PONG and confirmed
		// deletion both precede a seal. Socket closure alone cannot sign it.
		if !retired(state) {
			return x.ReaderState{}, i.ErrIncomplete
		}
		r.state, r.cached = state, true
	}
	return copyState(r.state)
}
func (r *Reader) Stop(d x.Deadline) error {
	r.stopped.Store(true)
	if err := take(r.serial, d); err != nil {
		return err
	}
	defer func() { <-r.serial }()
	r.fenced = true
	err := r.owner.Stop(d)
	if r.pending != nil {
		cleanupErr := r.pending.Stop(d)
		if cleanupErr == nil {
			r.pending = nil
		}
		err = errors.Join(err, cleanupErr)
	}
	return err
}
func (r *Reader) rebind(d i.Deadline) error {
	if err := take(r.serial, d); err != nil {
		return err
	}
	defer func() { <-r.serial }()
	if !r.fenced || !r.cached || !retired(r.state) || r.stopped.Load() {
		return i.ErrIncomplete
	}
	if r.bound {
		return nil
	}
	if r.pending != nil {
		if err := r.pending.Stop(d); err != nil {
			return err
		}
		r.pending = nil
	}
	js, err := r.current(d)
	if err != nil {
		return err
	}
	// Consumer delivery ordinal is intentionally absent: only the real synced
	// raw sequence can place the successor at the original object suffix.
	next, err := jsstream.NewTransferReader(js, r.state.ObjectSpec, r.state.SinkCheckpoint)
	if err != nil {
		return err
	}
	r.pending = next
	if err = r.owner.Stop(d); err != nil {
		cleanupErr := next.Stop(d)
		if cleanupErr == nil {
			r.pending = nil
		}
		return errors.Join(err, cleanupErr)
	}
	r.owner, r.bound = next, true
	r.pending = nil
	return nil
}
func (r *Reader) release(d i.Deadline) error {
	if err := take(r.serial, d); err != nil {
		return err
	}
	defer func() { <-r.serial }()
	if !r.fenced {
		return nil
	}
	if !r.bound || r.stopped.Load() {
		return i.ErrIncomplete
	}
	r.fenced, r.cached, r.bound = false, false, false
	return nil
}

func copyState[T any](v T) (T, error) {
	var copy T
	b, err := json.Marshal(v)
	if err != nil {
		return copy, i.ErrIncomplete
	}
	if err := json.Unmarshal(b, &copy); err != nil {
		return copy, i.ErrIncomplete
	}
	return copy, nil
}
