package xferimage

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"github.com/LinZiyang666/tether/internal/handoff"
	"maps"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/LinZiyang666/tether/internal/xfercontract"
)

// These are ceilings within the shared cut budget, not a separate pool. The
// lifecycle owner reserves simultaneous snapshots, output and Prepared copies.
// This codec opens no descriptors and starts no background work.
// origin: hot-update S7 plan §8.3 / T17 / T53
const MaxWorkerStateBytes = 32 << 20

const (
	workerHeaderBytes  = 80
	workerCodecVersion = 1
	workerMaxString    = 65536
	workerMaxItems     = 65536
	workerNilSlice     = ^uint32(0)
)

// Refusals intentionally exclude private values, paths and settlement material.
var ErrWorkerState = errors.New("xferimage: invalid worker state")
var ErrWorkerCapacity = errors.New("xferimage: worker state capacity exceeded")

// WorkerFieldDescriptor describes an actual field visited by the binary codec.
// Type and JSONTag are inventory metadata; private binary values always include
// zero decisions, even when their public message tag specifies omitempty.
type WorkerFieldDescriptor struct {
	Record, Name, Type, JSONTag, Encoding string
}

// WorkerDescriptors returns the complete reachable contract closure, excluding
// time.Time's implementation internals. A fresh result cannot mutate the schema
// used by subsequent readers. P34/P35 compare this union with runtime builders.
// origin: hot-update S7 plan §8.3 / §11.2 / T17
func WorkerDescriptors() []WorkerFieldDescriptor {
	var fields []WorkerFieldDescriptor
	seen := make(map[reflect.Type]bool)
	var visit func(reflect.Type)
	visit = func(typ reflect.Type) {
		if typ == reflect.TypeFor[time.Time]() || seen[typ] {
			return
		}
		seen[typ] = true
		switch typ.Kind() {
		case reflect.Pointer, reflect.Slice, reflect.Array:
			visit(typ.Elem())
		case reflect.Struct:
			for i := 0; i < typ.NumField(); i++ {
				f := typ.Field(i)
				fields = append(fields, WorkerFieldDescriptor{contractTypeName(typ), f.Name, contractTypeName(f.Type), string(f.Tag), "binary-v1"})
				visit(f.Type)
			}
		case reflect.Invalid, reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
			reflect.Float32, reflect.Float64, reflect.Complex64, reflect.Complex128, reflect.Chan, reflect.Func,
			reflect.Interface, reflect.Map, reflect.String, reflect.UnsafePointer:
			// Leaves: the parent struct's descriptor already names this field's
			// type, and the codec itself refuses the kinds it cannot encode.
		}
	}
	visit(reflect.TypeFor[xfercontract.WorkerState]())
	sort.Slice(fields, func(i, j int) bool {
		return fields[i].Record+"."+fields[i].Name < fields[j].Record+"."+fields[j].Name
	})
	return fields
}

// contractPackageTree is the import path of the neutral transfer vocabulary
// and every subpackage that hosts part of it behind identity-preserving aliases.
const contractPackageTree = "github.com/LinZiyang666/tether/internal/xfercontract"

// contractTypeName names a type by its contract vocabulary, never by the Go
// package that happens to host its declaration. P00fix moved Identity,
// CommitGrant, ReceiverOutcome, AtomicState and friends into the std-only
// xfercontract/xferport subpackage behind aliases: no field, type, tag or
// encoding changed, yet reflect.Type.String() flipped "xfercontract." to
// "xferport." and with it the sealed schema digest that every reader compares.
// A layering refactor inside the contract tree must not become an image epoch.
// origin: hot-update S7 plan §8.3 / T17 / M08 — P00fix alias move regression
func contractTypeName(t reflect.Type) string {
	if t.Name() != "" {
		if path := t.PkgPath(); path == contractPackageTree || strings.HasPrefix(path, contractPackageTree+"/") {
			return "xfercontract." + t.Name()
		}
		return t.String()
	}
	switch t.Kind() {
	case reflect.Pointer:
		return "*" + contractTypeName(t.Elem())
	case reflect.Slice:
		return "[]" + contractTypeName(t.Elem())
	case reflect.Array:
		return "[" + strconv.Itoa(t.Len()) + "]" + contractTypeName(t.Elem())
	case reflect.Map:
		return "map[" + contractTypeName(t.Key()) + "]" + contractTypeName(t.Elem())
	default:
		return t.String()
	}
}

var workerSchemaDigest = workerDescriptorDigest()

func workerDescriptorDigest() [32]byte {
	h := sha256.New()
	for _, f := range WorkerDescriptors() {
		for _, s := range []string{f.Record, f.Name, f.Type, f.JSONTag, f.Encoding} {
			_, _ = h.Write(binary.BigEndian.AppendUint32(nil, uint32(len(s))))
			_, _ = h.Write([]byte(s))
		}
	}
	var digest [32]byte
	copy(digest[:], h.Sum(nil))
	return digest
}

var _ WorkerEncoder = EncodeWorker
var _ WorkerDecoder = DecodeWorker

// EncodeWorker measures before allocating output. In particular, many valid
// inline prefixes or receipt arrays cannot grow a buffer past the sealed cap.
// Strings are exact UTF-8 bytes, integers are big endian, pointers have explicit
// presence, and slice counts distinguish nil from empty. Structs consume every
// declared field in declaration order, bound by the descriptor fingerprint.
// Returned bytes stay private; only sealed resources carry them across exec.
// origin: hot-update S7 plan §8.1–§8.3 / T17
func EncodeWorker(state xfercontract.WorkerState) ([]byte, error) {
	size := workerHeaderBytes
	if err := measureWorkerValue(reflect.ValueOf(state), &size); err != nil {
		return nil, err
	}
	heap := MaxWorkerStateBytes - int(reflect.TypeFor[xfercontract.WorkerState]().Size())
	if err := measureWorkerHeap(reflect.ValueOf(state), &heap); err != nil {
		return nil, err
	}
	if err := validateWorker(state); err != nil {
		return nil, err
	}
	data := make([]byte, workerHeaderBytes, size)
	copy(data[:8], "TETHXW7\x00")
	binary.BigEndian.PutUint32(data[8:12], workerCodecVersion)
	binary.BigEndian.PutUint32(data[12:16], uint32(size-workerHeaderBytes))
	copy(data[48:80], workerSchemaDigest[:])
	data, err := appendWorkerValue(data, reflect.ValueOf(state))
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(data[workerHeaderBytes:])
	copy(data[16:48], digest[:])
	return data, nil
}

// DecodeWorker rejects envelopes before allocation and charges decoded heap
// separately, preventing small forged counts from amplifying into large slices.
// It returns no partial state and copies all bytes: a caller may scrub its sealed
// input immediately. Integrity here detects corrupt state; the outer sealed
// resource and public ImageRef bind custody and authenticity.
// origin: hot-update S7 plan §8.3 / T17 / T53
func DecodeWorker(data []byte) (xfercontract.WorkerState, error) {
	var state xfercontract.WorkerState
	if len(data) > MaxWorkerStateBytes {
		return state, ErrWorkerCapacity
	}
	if len(data) < workerHeaderBytes || string(data[:8]) != "TETHXW7\x00" ||
		binary.BigEndian.Uint32(data[8:12]) != workerCodecVersion ||
		uint64(binary.BigEndian.Uint32(data[12:16])) != uint64(len(data)-workerHeaderBytes) ||
		!bytes.Equal(data[48:80], workerSchemaDigest[:]) {
		return state, ErrWorkerState
	}
	digest := sha256.Sum256(data[workerHeaderBytes:])
	if !bytes.Equal(data[16:48], digest[:]) {
		return state, ErrWorkerState
	}
	remaining := data[workerHeaderBytes:]
	heap := MaxWorkerStateBytes - int(reflect.TypeFor[xfercontract.WorkerState]().Size())
	if readWorkerValue(&remaining, reflect.ValueOf(&state).Elem(), &heap) != nil || len(remaining) != 0 {
		return xfercontract.WorkerState{}, ErrWorkerState
	}
	if err := validateWorker(state); err != nil {
		return xfercontract.WorkerState{}, err
	}
	return state, nil
}

func addWorkerSize(size *int, n int) error {
	if n < 0 || n > MaxWorkerStateBytes-*size {
		return ErrWorkerCapacity
	}
	*size += n
	return nil
}

func measureWorkerValue(v reflect.Value, size *int) error {
	if v.Type() == reflect.TypeFor[time.Time]() {
		tm, ok := v.Interface().(time.Time)
		if !ok {
			return ErrWorkerState
		}
		if _, err := tm.MarshalBinary(); err != nil {
			return ErrWorkerState
		}
		return addWorkerSize(size, 17) // length plus fixed seconds/nanoseconds/zone
	}
	switch v.Kind() {
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			if err := measureWorkerValue(v.Field(i), size); err != nil {
				return err
			}
		}
	case reflect.Pointer:
		if err := addWorkerSize(size, 1); err != nil {
			return err
		}
		if !v.IsNil() {
			return measureWorkerValue(v.Elem(), size)
		}
	case reflect.Slice, reflect.String:
		if v.Kind() == reflect.String && (v.Len() > workerMaxString || !utf8.ValidString(v.String())) {
			return ErrWorkerState
		}
		if err := addWorkerSize(size, 4); err != nil {
			return err
		}
		if v.Kind() == reflect.String || v.Type().Elem().Kind() == reflect.Uint8 {
			return addWorkerSize(size, v.Len())
		}
		if v.Len() > workerMaxItems {
			return ErrWorkerCapacity
		}
		for i := 0; i < v.Len(); i++ {
			if err := measureWorkerValue(v.Index(i), size); err != nil {
				return err
			}
		}
	case reflect.Array:
		for i := 0; i < v.Len(); i++ {
			if err := measureWorkerValue(v.Index(i), size); err != nil {
				return err
			}
		}
	case reflect.Bool, reflect.Uint8:
		return addWorkerSize(size, 1)
	case reflect.Uint32, reflect.Int32:
		return addWorkerSize(size, 4)
	case reflect.Uint64, reflect.Int64:
		return addWorkerSize(size, 8)
	default:
		return ErrWorkerState
	}
	return nil
}

// appendWorkerValue runs only after measureWorkerValue accepted the same value,
// so a refusal here is an internal inconsistency. EncodeWorker then fails
// closed instead of emitting a frame whose body disagrees with its header.
func appendWorkerValue(data []byte, v reflect.Value) ([]byte, error) {
	if v.Type() == reflect.TypeFor[time.Time]() {
		return appendWorkerTime(data, v)
	}
	switch v.Kind() {
	case reflect.Struct:
		return appendWorkerEach(data, v.NumField(), v.Field)
	case reflect.Pointer:
		if v.IsNil() {
			return append(data, 0), nil
		}
		return appendWorkerValue(append(data, 1), v.Elem())
	case reflect.Slice:
		if v.IsNil() {
			return binary.BigEndian.AppendUint32(data, workerNilSlice), nil
		}
		data = binary.BigEndian.AppendUint32(data, uint32(v.Len()))
		if v.Type().Elem().Kind() == reflect.Uint8 {
			return append(data, v.Bytes()...), nil
		}
		return appendWorkerEach(data, v.Len(), v.Index)
	case reflect.String:
		data = binary.BigEndian.AppendUint32(data, uint32(v.Len()))
		data = append(data, v.String()...)
	case reflect.Array:
		return appendWorkerEach(data, v.Len(), v.Index)
	case reflect.Bool:
		if v.Bool() {
			return append(data, 1), nil
		}
		data = append(data, 0)
	case reflect.Uint8:
		data = append(data, byte(v.Uint()))
	case reflect.Uint32:
		data = binary.BigEndian.AppendUint32(data, uint32(v.Uint()))
	case reflect.Int32:
		data = binary.BigEndian.AppendUint32(data, uint32(v.Int()))
	case reflect.Uint64:
		data = binary.BigEndian.AppendUint64(data, v.Uint())
	case reflect.Int64:
		data = binary.BigEndian.AppendUint64(data, uint64(v.Int()))
	case reflect.Invalid, reflect.Int, reflect.Int8, reflect.Int16, reflect.Uint, reflect.Uint16, reflect.Uintptr,
		reflect.Float32, reflect.Float64, reflect.Complex64, reflect.Complex128, reflect.Chan, reflect.Func,
		reflect.Interface, reflect.Map, reflect.UnsafePointer:
		// measureWorkerValue refuses these kinds before any byte is appended.
		return nil, ErrWorkerState
	}
	return data, nil
}

func appendWorkerTime(data []byte, v reflect.Value) ([]byte, error) {
	tm, ok := v.Interface().(time.Time)
	if !ok {
		return nil, ErrWorkerState
	}
	// time.MarshalBinary's fixed 15-byte v1 form preserves the UTC/offset
	// distinction without process-local monotonic clock state. Its v2 form
	// adds one zone second; reserve and encode it explicitly as well.
	b, err := tm.MarshalBinary()
	if err != nil {
		return nil, ErrWorkerState
	}
	data = append(data, byte(len(b)))
	data = append(data, b...)
	if len(b) == 15 {
		data = append(data, 0)
	}
	return data, nil
}

// appendWorkerEach appends struct fields or slice/array elements in order.
func appendWorkerEach(data []byte, n int, item func(int) reflect.Value) ([]byte, error) {
	var err error
	for i := 0; i < n; i++ {
		if data, err = appendWorkerValue(data, item(i)); err != nil {
			return nil, err
		}
	}
	return data, nil
}

func takeWorkerBytes(data *[]byte, n int) ([]byte, error) {
	if n < 0 || n > len(*data) {
		return nil, ErrWorkerState
	}
	b := (*data)[:n]
	*data = (*data)[n:]
	return b, nil
}

func chargeWorkerHeap(heap *int, n uint64) error {
	if n > uint64(*heap) {
		return ErrWorkerCapacity
	}
	*heap -= int(n)
	return nil
}

// Apply the reader's allocation charges at export as well. Empty nested
// structs can occupy much more heap than their small binary representation;
// accepting such an export would create an image this reader cannot restore.
// origin: hot-update S7 plan §8.3 / T17 / T53
func measureWorkerHeap(v reflect.Value, heap *int) error {
	if v.Type() == reflect.TypeFor[time.Time]() {
		return nil
	}
	switch v.Kind() {
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			if err := measureWorkerHeap(v.Field(i), heap); err != nil {
				return err
			}
		}
	case reflect.Pointer:
		if !v.IsNil() {
			if err := chargeWorkerHeap(heap, uint64(v.Type().Elem().Size())); err != nil {
				return err
			}
			return measureWorkerHeap(v.Elem(), heap)
		}
	case reflect.Slice:
		if err := chargeWorkerHeap(heap, uint64(v.Len())*uint64(v.Type().Elem().Size())); err != nil {
			return err
		}
		if v.Type().Elem().Kind() == reflect.Uint8 {
			return nil
		}
		for i := 0; i < v.Len(); i++ {
			if err := measureWorkerHeap(v.Index(i), heap); err != nil {
				return err
			}
		}
	case reflect.String:
		return chargeWorkerHeap(heap, uint64(v.Len()))
	case reflect.Array:
		// Inline in its parent like a struct: only its elements can own heap,
		// and readWorkerValue charges them element by element.
		for i := 0; i < v.Len(); i++ {
			if err := measureWorkerHeap(v.Index(i), heap); err != nil {
				return err
			}
		}
	case reflect.Bool, reflect.Uint8, reflect.Uint32, reflect.Int32, reflect.Uint64, reflect.Int64:
		// Fixed-width scalars occupy their parent's already charged storage.
	case reflect.Invalid, reflect.Int, reflect.Int8, reflect.Int16, reflect.Uint, reflect.Uint16, reflect.Uintptr,
		reflect.Float32, reflect.Float64, reflect.Complex64, reflect.Complex128, reflect.Chan, reflect.Func,
		reflect.Interface, reflect.Map, reflect.UnsafePointer:
		// measureWorkerValue refuses these kinds before the heap is charged.
		return ErrWorkerState
	}
	return nil
}

func readWorkerValue(data *[]byte, v reflect.Value, heap *int) error {
	if v.Type() == reflect.TypeFor[time.Time]() {
		b, err := takeWorkerBytes(data, 17)
		if err != nil || (b[0] != 15 && b[0] != 16) || (b[0] == 15 && b[16] != 0) {
			return ErrWorkerState
		}
		var tm time.Time
		if tm.UnmarshalBinary(b[1:1+int(b[0])]) != nil {
			return ErrWorkerState
		}
		canonical, err := tm.MarshalBinary()
		if err != nil || !bytes.Equal(canonical, b[1:1+int(b[0])]) {
			return ErrWorkerState
		}
		v.Set(reflect.ValueOf(tm))
		return nil
	}
	switch v.Kind() {
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			if err := readWorkerValue(data, v.Field(i), heap); err != nil {
				return err
			}
		}
	case reflect.Pointer:
		b, err := takeWorkerBytes(data, 1)
		if err != nil || b[0] > 1 {
			return ErrWorkerState
		}
		if b[0] == 0 {
			return nil
		}
		if err := chargeWorkerHeap(heap, uint64(v.Type().Elem().Size())); err != nil {
			return err
		}
		v.Set(reflect.New(v.Type().Elem()))
		return readWorkerValue(data, v.Elem(), heap)
	case reflect.String, reflect.Slice:
		b, err := takeWorkerBytes(data, 4)
		if err != nil {
			return err
		}
		n := binary.BigEndian.Uint32(b)
		if v.Kind() == reflect.Slice && n == workerNilSlice {
			return nil
		}
		// Every non-byte element consumes at least one encoded byte. Check the
		// remaining input and heap BEFORE MakeSlice, including nested pointers.
		if uint64(n) > uint64(len(*data)) {
			return ErrWorkerState
		}
		if v.Kind() == reflect.String || v.Type().Elem().Kind() == reflect.Uint8 {
			if v.Kind() == reflect.String && n > workerMaxString {
				return ErrWorkerState
			}
			if err := chargeWorkerHeap(heap, uint64(n)); err != nil {
				return err
			}
			b, _ = takeWorkerBytes(data, int(n))
			if v.Kind() == reflect.String {
				if !utf8.Valid(b) {
					return ErrWorkerState
				}
				v.SetString(string(b))
			} else {
				v.SetBytes(bytes.Clone(b))
			}
			return nil
		}
		if n > workerMaxItems {
			return ErrWorkerCapacity
		}
		if err := chargeWorkerHeap(heap, uint64(n)*uint64(v.Type().Elem().Size())); err != nil {
			return err
		}
		v.Set(reflect.MakeSlice(v.Type(), int(n), int(n)))
		for i := 0; i < v.Len(); i++ {
			if err := readWorkerValue(data, v.Index(i), heap); err != nil {
				return err
			}
		}
	case reflect.Array:
		for i := 0; i < v.Len(); i++ {
			if err := readWorkerValue(data, v.Index(i), heap); err != nil {
				return err
			}
		}
	case reflect.Bool, reflect.Uint8, reflect.Uint32, reflect.Int32, reflect.Uint64, reflect.Int64:
		return readWorkerScalar(data, v)
	default:
		return ErrWorkerState
	}
	return nil
}

// readWorkerScalar decodes one fixed-width kind written by appendWorkerValue;
// each occupies exactly its Go type's size.
func readWorkerScalar(data *[]byte, v reflect.Value) error {
	b, err := takeWorkerBytes(data, int(v.Type().Size()))
	if err != nil {
		return err
	}
	switch v.Kind() {
	case reflect.Bool:
		if b[0] > 1 {
			return ErrWorkerState
		}
		v.SetBool(b[0] == 1)
	case reflect.Uint8:
		v.SetUint(uint64(b[0]))
	case reflect.Uint32:
		v.SetUint(uint64(binary.BigEndian.Uint32(b)))
	case reflect.Int32:
		v.SetInt(int64(int32(binary.BigEndian.Uint32(b))))
	case reflect.Uint64:
		v.SetUint(binary.BigEndian.Uint64(b))
	case reflect.Int64:
		v.SetInt(int64(binary.BigEndian.Uint64(b)))
	case reflect.Invalid, reflect.Int, reflect.Int8, reflect.Int16, reflect.Uint, reflect.Uint16, reflect.Uintptr,
		reflect.Float32, reflect.Float64, reflect.Complex64, reflect.Complex128, reflect.Array, reflect.Chan,
		reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice, reflect.String,
		reflect.Struct, reflect.UnsafePointer:
		// readWorkerValue routes only the six encoded scalar kinds here.
		return ErrWorkerState
	}
	return nil
}

// Boundary copies retain private state without lending mutable evidence to callers.
// origin: hot-update S7 plan §8.2 / T16
func CloneTransferReceipt(receipt *xfercontract.TerminalReceipt) *xfercontract.TerminalReceipt {
	if receipt == nil {
		return nil
	}
	value := *receipt
	return &value
}
func CloneTransferStatus(s xfercontract.Status) xfercontract.Status {
	s.Terminal = CloneTransferReceipt(s.Terminal)
	return s
}
func CloneWorkerPrepare(p xfercontract.Prepare) xfercontract.Prepare {
	p.Inline = append([]byte(nil), p.Inline...)
	if p.Source != nil {
		value := *p.Source
		p.Source = &value
	}
	if p.Settlement != nil {
		value := *p.Settlement
		p.Settlement = &value
	}
	return p
}
func CloneWorkerEntry(e xfercontract.WorkerEntry) xfercontract.WorkerEntry {
	e.Prepare = CloneWorkerPrepare(e.Prepare)
	e.Status = CloneTransferStatus(e.Status)
	return e
}
func CloneWorkerState(s xfercontract.WorkerState) xfercontract.WorkerState {
	// Copy the private reachable closure, including pending payload/hash bytes
	// and evidence pointers. Resource descriptors themselves remain borrowed.
	s.ConsumerCleanup = append([]xfercontract.ConsumerCleanupState(nil), s.ConsumerCleanup...)
	for i := range s.ConsumerCleanup {
		s.ConsumerCleanup[i].Deletes = append([]xfercontract.ConsumerDelete(nil), s.ConsumerCleanup[i].Deletes...)
	}
	s.Entries = append([]xfercontract.WorkerEntry(nil), s.Entries...)
	for i, e := range s.Entries {
		s.Entries[i] = CloneWorkerEntry(e)
	}
	s.Revocations = append([]xfercontract.Revocation(nil), s.Revocations...)
	s.ResultRetention = append([]xfercontract.RetentionState(nil), s.ResultRetention...)
	s.CapacityReservations = append([]xfercontract.CapacityReservation(nil), s.CapacityReservations...)
	s.Senders = append([]xfercontract.SenderState(nil), s.Senders...)
	for i := range s.Senders {
		r := &s.Senders[i]
		// bytes.Clone keeps a not-yet-copied inline source (nil) distinct from
		// an empty source's retained copy (non-nil, length zero).
		r.SHA256State, r.Inline = append([]byte(nil), r.SHA256State...), bytes.Clone(r.Inline)
		r.Writer.Receipts = append([]xfercontract.ChunkReceipt(nil), r.Writer.Receipts...)
		if r.Writer.PendingAttempt != nil {
			value := *r.Writer.PendingAttempt
			value.Bytes = append([]byte(nil), value.Bytes...)
			r.Writer.PendingAttempt = &value
		}
		if r.Writer.MetaReceipt != nil {
			value := *r.Writer.MetaReceipt
			value.Metadata = append([]byte(nil), value.Metadata...)
			r.Writer.MetaReceipt = &value
		}
		if r.Writer.RetirementFence.Receipt != nil {
			value := *r.Writer.RetirementFence.Receipt
			r.Writer.RetirementFence.Receipt = &value
		}
	}
	s.Receivers = append([]xfercontract.ReceiverState(nil), s.Receivers...)
	for i := range s.Receivers {
		r := &s.Receivers[i]
		r.Checkpoint.SHA256State = append([]byte(nil), r.Checkpoint.SHA256State...)
		r.Reader.SinkCheckpoint.SHA256State = append([]byte(nil), r.Reader.SinkCheckpoint.SHA256State...)
	}
	s.Couriers = append([]xfercontract.CourierState(nil), s.Couriers...)
	for i := range s.Couriers {
		c := &s.Couriers[i]
		c.Canonical.Bytes = append([]byte(nil), c.Canonical.Bytes...)
		c.Receipt = CloneTransferReceipt(c.Receipt)
		c.Cleanup.Consumers = append([]xfercontract.ReaderState(nil), c.Cleanup.Consumers...)
		for j := range c.Cleanup.Consumers {
			r := &c.Cleanup.Consumers[j]
			r.SinkCheckpoint.SHA256State = append([]byte(nil), r.SinkCheckpoint.SHA256State...)
		}
		if c.Cleanup.WriterFence.Receipt != nil {
			value := *c.Cleanup.WriterFence.Receipt
			c.Cleanup.WriterFence.Receipt = &value
		}
	}
	s.CanonicalOutcomes = append([]xfercontract.CanonicalOutcome(nil), s.CanonicalOutcomes...)
	for i := range s.CanonicalOutcomes {
		c := &s.CanonicalOutcomes[i]
		c.Canonical.Bytes = append([]byte(nil), c.Canonical.Bytes...)
		c.Receipt = CloneTransferReceipt(c.Receipt)
	}
	return s
}

func RetainWorkerOutcome(s *xfercontract.WorkerState, outcome xfercontract.CanonicalOutcome) {
	for i, old := range s.CanonicalOutcomes {
		if old.Identity == outcome.Identity {
			s.CanonicalOutcomes[i] = outcome
			return
		}
	}
	s.CanonicalOutcomes = append(s.CanonicalOutcomes, outcome)
}

// WorkerResourceRefs projects the exact original-descriptor closure; no files are taken here.
// origin: hot-update S7 plan §8.2 / T17
func WorkerResourceRefs(state xfercontract.WorkerState) []xfercontract.ResourceRef {
	var refs []xfercontract.ResourceRef
	seen := make(map[xfercontract.ResourceRef]bool)
	add := func(ref xfercontract.ResourceRef) {
		if ref != (xfercontract.ResourceRef{}) && !seen[ref] {
			refs = append(refs, ref)
			seen[ref] = true
		}
	}
	for _, s := range state.Senders {
		add(s.Source)
	}
	for _, r := range state.Receivers {
		add(r.Atomic.Source)
		add(r.Atomic.Tmp)
		add(r.Atomic.Parent)
	}
	return refs
}

// ValidWorkerConsumerCleanup validates detached duties without opening a socket
// or admitting readers. The original stream incarnation binds every deletion.
// origin: S7 internal review rv-jsnats F3 / s7-p9jsn2 Requests 2
func ValidWorkerConsumerCleanup(s xfercontract.WorkerState) bool {
	if len(s.ConsumerCleanup) > 1024 {
		return false
	}
	owners, deletes := make(map[string]bool), make(map[string]bool)
	count := 0
	for _, state := range s.ConsumerCleanup {
		if !workerID(state.OwnerID) || strings.ContainsAny(state.OwnerID, "._-") || len(state.OwnerID) > 128 || owners[state.OwnerID] {
			return false
		}
		owners[state.OwnerID] = true
		for _, duty := range state.Deletes {
			count++
			created, err := time.Parse(time.RFC3339Nano, duty.StreamCreated)
			identity := duty.Stream + "/" + duty.Consumer
			if count > 1024 || !strings.HasPrefix(duty.Stream, "OBJ_") || len(duty.Stream) <= 4 || len(duty.Stream) > 255 ||
				!workerID(duty.Stream[4:]) || strings.Contains(duty.Stream[4:], ".") || !workerID(duty.Consumer) || strings.ContainsAny(duty.Consumer, "._-") || len(duty.Consumer) > 128 ||
				err != nil || created.IsZero() || created.UTC().Format(time.RFC3339Nano) != duty.StreamCreated || deletes[identity] {
				return false
			}
			deletes[identity] = true
		}
	}
	return true
}

// WorkerRegistryClosureCode checks the registry's private value graph before
// any descriptor is taken. Creation scope remains the worker's responsibility.
// origin: hot-update S7 plan §8.2 / S7 internal review rv-lifecycle F5
func WorkerRegistryClosureCode(state xfercontract.WorkerState) string {
	ids := make(map[xfercontract.Identity]bool)
	keys := make(map[xfercontract.Key]bool)
	reservations := make(map[xfercontract.ReservationID]bool)
	for _, r := range state.CapacityReservations {
		if r.ID == "" || reservations[r.ID] {
			return xfercontract.CodeCapacityExceeded
		}
		reservations[r.ID] = true
	}
	used := make(map[xfercontract.ReservationID]bool)
	for _, e := range state.Entries {
		if keys[xfercontract.Key{SID: e.Prepare.Identity.SID, TransferID: e.Prepare.Identity.TransferID}] || e.OwnerGeneration != state.Lineage.Generation || e.Status.Identity != e.Prepare.Identity || e.Status.Origin != e.Prepare.Origin || e.Status.Budget != e.Prepare.Budget || !reservations[e.Reservation] || used[e.Reservation] || !workerPhase(e.Status.Phase) {
			return xfercontract.CodeIntentConflict
		}
		keys[xfercontract.Key{SID: e.Prepare.Identity.SID, TransferID: e.Prepare.Identity.TransferID}], ids[e.Prepare.Identity], used[e.Reservation] = true, true, true
	}
	// Spare reservations are not harmless padding: without an entry to release
	// them they become permanent capacity leaks in the new process account.
	if len(used) != len(reservations) {
		return xfercontract.CodeCapacityExceeded
	}
	retained := make(map[xfercontract.Identity]bool)
	for _, r := range state.ResultRetention {
		if !ids[r.Identity] || retained[r.Identity] || r.Deadline.BootID != state.Lineage.BootID || r.Deadline.Deadline <= 0 {
			return xfercontract.CodeOwnerRecovering
		}
		retained[r.Identity] = true
	}
	if len(retained) != len(ids) {
		return xfercontract.CodeOwnerRecovering
	}
	check := func(list []xfercontract.Identity) string {
		seen := make(map[xfercontract.Identity]bool)
		for _, id := range list {
			if !ids[id] || seen[id] {
				return xfercontract.CodeIntentConflict
			}
			seen[id] = true
		}
		return ""
	}
	var senders, receivers, couriers, outcomes, revocations []xfercontract.Identity
	for _, s := range state.Senders {
		senders = append(senders, s.Identity)
		if s.ByteDeadline.BootID != state.Lineage.BootID {
			return xfercontract.CodeOwnerRecovering
		}
	}
	for _, r := range state.Receivers {
		receivers = append(receivers, r.Identity)
		if r.ByteDeadline.BootID != state.Lineage.BootID {
			return xfercontract.CodeOwnerRecovering
		}
	}
	for _, c := range state.Couriers {
		couriers = append(couriers, c.Identity)
	}
	for _, c := range state.CanonicalOutcomes {
		outcomes = append(outcomes, c.Identity)
	}
	for _, r := range state.Revocations {
		revocations = append(revocations, r.Identity)
	}
	for _, list := range [][]xfercontract.Identity{senders, receivers, couriers, outcomes, revocations} {
		if err := check(list); err != "" {
			return err
		}
	}
	// File names include the resource ID. Reducing them to a singleton name
	// would let one transfer consume another transfer's tmp or parent OFD.
	files := make(map[string]bool)
	for _, ref := range WorkerResourceRefs(state) {
		valid := ref.Kind == xfercontract.ResourceSource && ref.Name == xfercontract.SourceFileName || ref.Kind == xfercontract.ResourceTmp && ref.Name == xfercontract.TmpFileName || ref.Kind == xfercontract.ResourceParent && ref.Name == xfercontract.ParentFileName
		name := ref.ResourceID + "/" + ref.Name
		if !valid || ref.ResourceID == "" || files[name] {
			return xfercontract.CodeOwnerRecovering
		}
		files[name] = true
	}
	return ""
}

// CloneWorkerResources copies metadata and file maps without duplicating the
// borrowed originals. Export callers cannot rewrite the rollback owner's graph.
// origin: hot-update S7 plan §8.2 / T14
func CloneWorkerResources(resources []handoff.Resource) []handoff.Resource {
	out := make([]handoff.Resource, len(resources))
	copy(out, resources)
	for i := range out {
		out[i].Data = bytes.Clone(out[i].Data)
		out[i].Files = maps.Clone(out[i].Files)
	}
	return out
}

func SameWorkerPrepare(a, b xfercontract.Prepare) bool {
	// CallID and received remaining budget are attempt details. Neither can
	// rewrite the immutable bytes, original start, intent or permit on replay.
	// A retained pull learns its seal from its own pinned source (recorded at
	// every cut); the broker's replay of the original prepare carries none and
	// cannot supply or change one. Comparing them refused every pending
	// prepare replay after any freeze, including a refused cut's rollback.
	// origin: hot-update S7 plan §6.1 / §8.2 / FIX-INLINE
	return a.Identity == b.Identity && a.Origin == b.Origin && a.Intent == b.Intent && sha256.Sum256(a.Inline) == sha256.Sum256(b.Inline) && len(a.Inline) == len(b.Inline) && a.Budget.StartedAt.Equal(b.Budget.StartedAt) && (reflect.DeepEqual(a.Source, b.Source) || a.Intent.Verb == "pull" && b.Source == nil) && reflect.DeepEqual(a.Settlement, b.Settlement)
}
