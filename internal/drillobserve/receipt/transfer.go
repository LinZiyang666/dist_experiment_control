package receipt

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
	"strings"
	"sync"
)

const TransferSchemaVersion = 1
const TransferEventKind = "transfer-v1"
const TransferPoisonKind = "transfer-poison-v1"
const TransferSetupRC = 2

// Every observation failure has one secret-free classification. In particular,
// permission-denied kernel evidence cannot become a continuity success.
// origin: hot-update S7 plan §10.4 / T41 / T47
var ErrTransferCustody = errors.New("drillobserve: transfer custody incomplete (SETUP-RED2)")

// TransferPendingBoundary requires the producing owner's pending proof. A
// phase copied onto a later receipt cannot select an earlier live window, and
// a successful send alone says nothing about whether its reply was lost.
// This supplements the identity/OFD/sequence checks of the independent reader.
// origin: S7 internal review INTEG-O F5
func TransferPendingBoundary(window string, value any) bool {
	var o TransferObservation
	if CopyTransferValue(reflect.ValueOf(&o).Elem(), reflect.ValueOf(value)) != nil {
		return false
	}
	switch window {
	case "grant-reply-lost":
		a := o.Attempt
		return o.Kind == "reserve" && o.Phase == "commit_reserved" && o.GrantNonce != "" &&
			a.LogicalID != "" && a.Ordinal > 0 && a.Sent && a.Phase == "request" && a.Class == "reply" &&
			a.StartedMonoNS > 0 && a.FinishedMonoNS >= a.StartedMonoNS
	case "terminal-pre-history-ACK":
		return o.Kind == "finish" && o.Phase == "terminal_pending" && o.AdmissionClosed &&
			o.CanonicalDigest != "" && o.HistorySeq == 0
	case "cleanup-pending":
		return o.Kind == "finish" && o.Phase == "cleanup_pending" && o.AdmissionClosed &&
			o.CanonicalDigest != "" && o.HistorySeq > 0 && o.RetirementDigest == ""
	default:
		return true
	}
}

// TransferProcess binds all leaf producers to one process incarnation. The
// outer adapter constructs one observer per generation and shares it through
// Base.Observe; leaves neither read /proc nor mint their own ordinal streams.
type TransferProcess struct {
	ProcessStart uint64
	BootID       string
	PID          int
	Generation   uint64
	CutID        string
}

// This ordered list is the v1 wire schema, including zero-valued evidence.
// Absence never means false/zero: a reader must reject a producer that omitted
// a field. Nested records follow the same rule and have no extensible payload.
// origin: hot-update S7 plan §10.4 / T41
var transferFields = [...]string{
	"kind", "process_start", "boot_id", "pid", "generation", "cut_id",
	"transfer_id", "entry_nonce", "object_version", "leg", "phase", "event_seq", "boottime_ns",
	"index", "offset", "length", "sha256", "expected_subject_seq", "expected_stream_seq", "actual_ack_seq",
	"attachment_generation", "fetched_offset", "durable_offset", "last_consumed_stream_seq", "hash_state_digest",
	"consumer_identity", "connection_identity", "server_identity", "stream_identity", "dropped", "pong_confirmed", "consumer_deleted",
	"primitive", "outcome", "errno", "parent_synced", "tmp", "parent", "source", "ofd_witness_digest",
	"admission_closed", "grant_nonce", "canonical_digest", "stop_nonce", "original_deadline", "revision",
	"primary_revision", "outbox_revision", "file_synced", "directory_synced", "raft_index", "history_seq",
	"publisher_checkpoint", "retirement_digest", "reader_sha", "active", "installed_before_reaper", "attempt", "native_outcome",
}

func transferNames(t reflect.Type) []string {
	switch t {
	case reflect.TypeFor[TransferObservation]():
		return transferFields[:]
	case reflect.TypeFor[TransferResource]():
		return []string{"resource_id", "name", "kind"}
	case reflect.TypeFor[TransferDeadline]():
		return []string{"boot_id", "deadline"}
	case reflect.TypeFor[TransferAttempt]():
		return []string{"logical_id", "ordinal", "phase", "started_mono_ns", "finished_mono_ns", "sent", "class", "backoff_ns"}
	default:
		return nil
	}
}

// EncodeTransfer copies only the frozen typed values. Event's existing format
// and JSON redaction protects the private envelope from accidental logging.
func EncodeTransfer(value any) (Event, error) {
	var o TransferObservation
	if err := CopyTransferValue(reflect.ValueOf(&o).Elem(), reflect.ValueOf(value)); err != nil {
		return Event{}, err
	}
	if !validTransfer(o) {
		return Event{}, ErrTransferCustody
	}
	fields, err := TransferObject(reflect.ValueOf(o))
	if err != nil {
		return Event{}, err
	}
	fields["schema"] = TransferSchemaVersion
	data, err := json.Marshal(fields)
	if err != nil || len(data) > MaxData {
		return Event{}, ErrTransferCustody
	}
	return Event{Kind: TransferEventKind, Domain: "transfer", At: uint64(o.BoottimeNS), Digest: sha256.Sum256(data), Data: data}, nil
}

func TransferObject(v reflect.Value) (map[string]any, error) {
	names := transferNames(v.Type())
	if len(names) != v.NumField() {
		return nil, ErrTransferCustody
	}
	fields := make(map[string]any, len(names))
	for i, name := range names {
		value := v.Field(i)
		if value.Kind() == reflect.Struct {
			nested, err := TransferObject(value)
			if err != nil {
				return nil, err
			}
			fields[name] = nested
		} else {
			fields[name] = value.Interface()
		}
	}
	return fields, nil
}

// DecodeTransfer is strict about presence, type, bounds and digest. It does
// not convert damaged raw evidence to product-red or to zero-valued success.
// origin: hot-update S7 plan §10.4 / T41
func DecodeTransfer(e Event) (TransferObservation, error) {
	var o TransferObservation
	if e.Kind != TransferEventKind || e.Domain != "transfer" || len(e.Data) > MaxData || sha256.Sum256(e.Data) != e.Digest {
		return o, ErrTransferCustody
	}
	fields, err := TransferRawObject(e.Data)
	if err != nil || len(fields) != len(transferFields)+1 || string(fields["schema"]) != "1" {
		return o, ErrTransferCustody
	}
	delete(fields, "schema")
	if err := DecodeTransferObject(fields, reflect.ValueOf(&o).Elem()); err != nil || !validTransfer(o) || e.At < uint64(o.BoottimeNS) {
		return TransferObservation{}, ErrTransferCustody
	}
	return o, nil
}

// encoding/json's map decoder accepts duplicate keys and null scalars. Both
// erase evidence, so reject them before any typed assignment, at every level.
func TransferRawObject(data []byte) (map[string]json.RawMessage, error) {
	d := json.NewDecoder(bytes.NewReader(data))
	first, err := d.Token()
	if err != nil || first != json.Delim('{') {
		return nil, ErrTransferCustody
	}
	fields := make(map[string]json.RawMessage)
	for d.More() {
		key, err := d.Token()
		name, ok := key.(string)
		if err != nil || !ok || fields[name] != nil {
			return nil, ErrTransferCustody
		}
		var value json.RawMessage
		if d.Decode(&value) != nil || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return nil, ErrTransferCustody
		}
		fields[name] = value
	}
	if last, err := d.Token(); err != nil || last != json.Delim('}') {
		return nil, ErrTransferCustody
	}
	if _, err := d.Token(); err != io.EOF {
		return nil, ErrTransferCustody
	}
	return fields, nil
}

func DecodeTransferObject(fields map[string]json.RawMessage, v reflect.Value) error {
	names := transferNames(v.Type())
	if len(names) != v.NumField() || len(fields) != len(names) {
		return ErrTransferCustody
	}
	for i, name := range names {
		data, present := fields[name]
		if !present {
			return ErrTransferCustody
		}
		value := v.Field(i)
		if value.Kind() == reflect.Struct {
			nested, err := TransferRawObject(data)
			if err != nil || DecodeTransferObject(nested, value) != nil {
				return ErrTransferCustody
			}
		} else if json.Unmarshal(data, value.Addr().Interface()) != nil {
			return ErrTransferCustody
		}
	}
	return nil
}

func validTransfer(o TransferObservation) bool {
	if o.ProcessStart == 0 || o.PID <= 0 || o.EventSeq == 0 || o.BoottimeNS <= 0 || o.Offset < 0 || o.FetchedOffset < 0 || o.DurableOffset < 0 || o.DurableOffset > o.FetchedOffset || o.Errno < 0 || o.Dropped != 0 {
		return false
	}
	if !TransferToken(o.BootID, true) || !TransferToken(o.TransferID, true) || !TransferToken(o.EntryNonce, true) || !TransferToken(o.Leg, true) || !TransferToken(o.ObjectVersion, false) || !TransferToken(o.CutID, false) {
		return false
	}
	switch o.Kind {
	case "append", "resolve", "finalize",
		"fetched", "synced", "reader_stop",
		"primitive_enter", "primitive_exit", "directory_sync",
		"freeze", "import", "control", "acknowledge",
		"reserve", "finish", "callback_enter", "callback_join",
		"ledger_sync", "history", "retirement",
		"install", "rollback", "attempt", "drain":
	default:
		return false
	}
	switch o.Phase {
	case "open", "prepared", "sending", "receiving",
		"commit_reserved", "decided", "terminal_pending",
		"history_confirmed", "cleanup_pending", "settled":
	default:
		return false
	}
	if o.Primitive != "" && o.Primitive != "rename" && o.Primitive != "link" {
		return false
	}
	if o.Outcome != "" && o.Outcome != "not_entered" && o.Outcome != "inflight" && o.Outcome != "landed" {
		return false
	}
	for _, digest := range []string{o.SHA256, o.HashStateDigest, o.OFDWitnessDigest, o.CanonicalDigest, o.RetirementDigest, o.ReaderSHA} {
		if digest != "" && !TransferDigest(digest) {
			return false
		}
	}
	for _, id := range []string{o.ConsumerIdentity, o.ConnectionIdentity, o.ServerIdentity, o.StreamIdentity, o.GrantNonce, o.StopNonce} {
		if !TransferToken(id, false) {
			return false
		}
	}
	for _, ref := range []TransferResource{o.Tmp, o.Parent, o.Source} {
		if !ValidTransferResource(ref, false) {
			return false
		}
	}
	if o.OriginalDeadline.Deadline < 0 || (o.OriginalDeadline.Deadline != 0 && o.OriginalDeadline.BootID != o.BootID) || (o.OriginalDeadline.Deadline == 0 && o.OriginalDeadline.BootID != "") {
		return false
	}
	a := o.Attempt
	if a == (TransferAttempt{}) {
		return true
	}
	// Entry is emitted before the transport finishes. Only that boundary may
	// carry an unknown finish time; join/result still require a real timestamp.
	// origin: S7 internal review INTEG-M F1
	finished := a.FinishedMonoNS >= a.StartedMonoNS || o.Kind == "callback_enter" && a.FinishedMonoNS == 0
	return TransferToken(a.LogicalID, true) && a.Ordinal > 0 && validTransferAttemptPhase(a.Phase) && a.StartedMonoNS > 0 && finished && a.BackoffNS >= 0 && TransferToken(a.Class, false)
}

func TransferToken(s string, required bool) bool {
	if len(s) > 256 || (required && s == "") {
		return false
	}
	for _, c := range s {
		if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9') && c != '-' && c != '_' && c != '.' && c != ':' {
			return false
		}
	}
	return true
}

func TransferDigest(s string) bool {
	b, err := hex.DecodeString(s)
	return err == nil && len(b) == sha256.Size && s == strings.ToLower(s)
}

func ValidTransferResource(r TransferResource, required bool) bool {
	if r == (TransferResource{}) {
		return !required
	}
	if !TransferToken(r.ResourceID, true) {
		return false
	}
	return r.Name == "tmp" && r.Kind == "xfer-tmp-v1" || r.Name == "parent" && r.Kind == "xfer-parent-v1" || r.Name == "source" && r.Kind == "xfer-source-v1"
}

// NewTransferObserver returns nil in an ordinary release. The adapter stamps
// actual local BOOTTIME and allocates ordinals under the same lock as enqueue,
// so racing producers cannot reorder a complete stream or hide an invalid one.
// Hook is the startup-installed, nonblocking custody hook, never product IO.
// origin: hot-update S7 plan §10.4 / T41
func NewTransferObserver[T any](process TransferProcess, hook func(Event), nowBoot func() (int64, error), poison func()) func(T) {
	if hook == nil {
		return nil
	}
	var mu sync.Mutex
	var sequence uint64
	broken := false
	return func(value T) {
		var o TransferObservation
		copyErr := CopyTransferValue(reflect.ValueOf(&o).Elem(), reflect.ValueOf(value))
		mu.Lock()
		defer mu.Unlock()
		if broken {
			return
		}
		if copyErr != nil || sequence == math.MaxUint64 || (o.ProcessStart != 0 && o.ProcessStart != process.ProcessStart) || (o.PID != 0 && o.PID != process.PID) || (o.BootID != "" && o.BootID != process.BootID) || (o.Generation != 0 && o.Generation != process.Generation) {
			broken = true
			transferPoison(hook, poison)
			return
		}
		sequence++
		o.ProcessStart, o.BootID, o.PID, o.Generation = process.ProcessStart, process.BootID, process.PID, process.Generation
		// A new cut is observed before exec, within the same process generation.
		// Preserve its producer tag without resetting the process ordinal stream.
		// origin: hot-update S7 plan §10.4 / T41
		if o.CutID == "" {
			o.CutID = process.CutID
		}
		o.EventSeq = sequence
		now, err := nowBoot()
		o.BoottimeNS = now
		e, encodeErr := EncodeTransfer(o)
		if err != nil || encodeErr != nil {
			broken = true
			transferPoison(hook, poison)
			return
		}
		hook(e)
	}
}

// TransferSequence belongs to an independent reader, one per process/image
// incarnation. Even a well-formed record cannot forgive a missing/repeated
// event or a changed process identity. Poison is irreversible.
// origin: hot-update S7 plan §10.4 / T41
type TransferSequence struct {
	process TransferProcess
	last    uint64
	broken  bool
}

func (s *TransferSequence) Accept(e Event) (TransferObservation, error) {
	o, err := DecodeTransfer(e)
	p := TransferProcess{o.ProcessStart, o.BootID, o.PID, o.Generation, o.CutID}
	if s.broken || err != nil || s.last == math.MaxUint64 || o.EventSeq != s.last+1 || (s.last != 0 && !sameTransferProcess(p, s.process)) {
		s.broken = true
		return TransferObservation{}, ErrTransferCustody
	}
	s.process, s.last = p, o.EventSeq
	return o, nil
}

// No formatter exposes paths, references or private identity records.
func (TransferSequence) String() string               { return "drillobserve.TransferSequence(redacted)" }
func (s TransferSequence) Format(w fmt.State, _ rune) { _, _ = io.WriteString(w, s.String()) }

// TDP2 adds a challenge to the existing TDP1 barrier. TDA2 carries that exact
// nonce, the sealed batch watermark and the first custody poison cause. A
// missing/stale ACK never closes a judge window, even when all history arrived.
// origin: hot-update S7 plan §10.4 / T41
func TransferDrainRequest(nonce [16]byte) ([]byte, error) {
	if nonce == ([16]byte{}) {
		return nil, ErrTransferCustody
	}
	return append([]byte("TDP2"), nonce[:]...), nil
}

func ValidateTransferDrainACK(data []byte, nonce [16]byte, sealedSequence uint64) error {
	if nonce == ([16]byte{}) || len(data) != 36 || string(data[:4]) != "TDA2" || !bytes.Equal(data[4:20], nonce[:]) || sealedSequence == 0 || binary.LittleEndian.Uint64(data[20:28]) != sealedSequence || binary.LittleEndian.Uint64(data[28:36]) != 0 {
		return ErrTransferCustody
	}
	return nil
}

// TransferOFDReceipt identifies custody and its resource cost. Its witness
// digest is an opaque label: continuity requires Compare's kernel result.
type TransferOFDReceipt struct {
	Schema        uint32           `json:"schema"`
	ProcessStart  uint64           `json:"process_start"`
	BootID        string           `json:"boot_id"`
	PID           int              `json:"pid"`
	Resource      TransferResource `json:"resource"`
	WitnessDigest string           `json:"witness_digest"`
	OwnedFDs      uint32           `json:"owned_fds"`
	PeakFDs       uint32           `json:"peak_fds"`
}

// TransferCustodyReader validates the existing sealed-batch framing. The
// collector verifies the memfd seals before passing its bytes here and creates
// a new reader only from an independently established image incarnation.
// origin: hot-update S7 plan §10.4 / T41
type TransferCustodyReader struct {
	Identity      [32]byte
	incarnation   [16]byte
	sequence      uint64
	drainSequence uint64
	broken        bool
}

func (r *TransferCustodyReader) Accept(data []byte) ([]Event, error) {
	fail := func() ([]Event, error) { r.broken = true; return nil, ErrTransferCustody }
	if r.broken || r.Identity == ([32]byte{}) || len(data) < 76 || len(data) > 76+MaxBatch*(MaxData+328) || string(data[:8]) != "TDRILL1\x00" || !bytes.Equal(data[8:40], r.Identity[:]) {
		return fail()
	}
	var incarnation [16]byte
	copy(incarnation[:], data[40:56])
	sequence := binary.LittleEndian.Uint64(data[56:64])
	// The legacy frame has its sequence at 56 and poison at 64. Keep explicit
	// offsets rather than treating a missing scalar as a harmless zero.
	if binary.LittleEndian.Uint64(data[64:72]) != 0 || incarnation == ([16]byte{}) || r.sequence == math.MaxUint64 || sequence != r.sequence+1 || (r.sequence != 0 && incarnation != r.incarnation) {
		return fail()
	}
	count := binary.LittleEndian.Uint32(data[72:76])
	if count > MaxBatch {
		return fail()
	}
	data = data[76:]
	events := make([]Event, 0, count)
	for i := uint32(0); i < count; i++ {
		var e Event
		if len(data) < 1 || int(data[0]) > 63 || data[0] == 0 || len(data) < 1+int(data[0]) {
			return fail()
		}
		e.Kind, data = string(data[1:1+int(data[0])]), data[1+int(data[0]):]
		if len(data) < 1 || int(data[0]) > 31 || len(data) < 1+int(data[0]) {
			return fail()
		}
		e.Domain, data = string(data[1:1+int(data[0])]), data[1+int(data[0]):]
		if len(data) < 232 {
			return fail()
		}
		copy(e.Endpoint[:], data[:16])
		copy(e.Flow[:], data[16:32])
		copy(e.Actor[:], data[32:48])
		copy(e.Digest[:], data[48:80])
		copy(e.Nonce[:], data[80:92])
		e.At = binary.LittleEndian.Uint64(data[92:100])
		for index := range e.Values {
			e.Values[index] = binary.LittleEndian.Uint64(data[100+index*8 : 108+index*8])
		}
		length := binary.LittleEndian.Uint32(data[228:232])
		data = data[232:]
		if length > MaxData || uint64(length) > uint64(len(data)) || e.Kind == TransferPoisonKind {
			return fail()
		}
		e.Data = append([]byte(nil), data[:length]...)
		data = data[length:]
		if e.Kind == TransferEventKind {
			if _, err := DecodeTransfer(e); err != nil {
				return fail()
			}
		}
		events = append(events, e)
	}
	if len(data) != 0 {
		return fail()
	}
	r.incarnation, r.sequence = incarnation, sequence
	return events, nil
}

func (r *TransferCustodyReader) Finish(nonce [16]byte, ack []byte) error {
	if r.broken || r.sequence <= r.drainSequence || ValidateTransferDrainACK(ack, nonce, r.sequence) != nil {
		r.broken = true
		return ErrTransferCustody
	}
	// Consuming the watermark prevents one ACK from closing both quiet-window
	// barriers. The second drain must include a newly sealed product batch.
	// origin: hot-update S7 plan §10.4 / T41
	r.drainSequence = r.sequence
	return nil
}

func sameTransferProcess(a, b TransferProcess) bool {
	return a.ProcessStart == b.ProcessStart && a.BootID == b.BootID && a.PID == b.PID && a.Generation == b.Generation
}

type TransferObservation struct {
	Kind                                                                 string
	ProcessStart                                                         uint64
	BootID                                                               string
	PID                                                                  int
	Generation                                                           uint64
	CutID                                                                string
	TransferID, EntryNonce, ObjectVersion, Leg                           string
	Phase                                                                string
	EventSeq                                                             uint64
	BoottimeNS                                                           int64
	Index                                                                uint32
	Offset                                                               int64
	Length                                                               uint32
	SHA256                                                               string
	ExpectedSubjectSeq, ExpectedStreamSeq, ActualACKSeq                  uint64
	AttachmentGeneration                                                 uint64
	FetchedOffset, DurableOffset                                         int64
	LastConsumedStreamSeq                                                uint64
	HashStateDigest                                                      string
	ConsumerIdentity, ConnectionIdentity, ServerIdentity, StreamIdentity string
	Dropped                                                              uint64
	PONGConfirmed, ConsumerDeleted                                       bool
	Primitive                                                            string
	Outcome                                                              string
	Errno                                                                int32
	ParentSynced                                                         bool
	Tmp, Parent, Source                                                  TransferResource
	OFDWitnessDigest                                                     string
	AdmissionClosed                                                      bool
	GrantNonce, CanonicalDigest, StopNonce                               string
	OriginalDeadline                                                     TransferDeadline
	Revision, PrimaryRevision, OutboxRevision                            uint64
	FileSynced, DirectorySynced                                          bool
	RaftIndex, HistorySeq, PublisherCheckpoint                           uint64
	RetirementDigest, ReaderSHA                                          string
	Active, InstalledBeforeReaper                                        bool
	Attempt                                                              TransferAttempt
	NativeOutcome                                                        int
}

// This projection is private custody schema, not another business DTO. Product
// contracts include NATS ports; importing them here would pull an SDK into all
// existing flow/SS leaves. Copying requires exact exported field names and
// scalar kinds, so drift in the frozen product record fails closed.
// origin: hot-update S7 plan §10.4 / §11.1 / T41
type TransferResource struct{ ResourceID, Name, Kind string }
type TransferDeadline struct {
	BootID   string
	Deadline int64
}
type TransferAttempt struct {
	LogicalID                     string
	Ordinal                       uint32
	Phase                         string
	StartedMonoNS, FinishedMonoNS int64
	Sent                          bool
	Class                         string
	BackoffNS                     int64
}

func CopyTransferValue(dst, src reflect.Value) error {
	if !src.IsValid() || src.Kind() != reflect.Struct || dst.Kind() != reflect.Struct || src.NumField() != dst.NumField() {
		return ErrTransferCustody
	}
	for i := 0; i < dst.NumField(); i++ {
		if dst.Type().Field(i).Name != src.Type().Field(i).Name {
			return ErrTransferCustody
		}
		from, to := src.Field(i), dst.Field(i)
		if !from.CanInterface() || !to.CanSet() || from.Kind() != to.Kind() {
			return ErrTransferCustody
		}
		if to.Kind() == reflect.Struct {
			if err := CopyTransferValue(to, from); err != nil {
				return err
			}
		} else {
			if !from.CanConvert(to.Type()) {
				return ErrTransferCustody
			}
			to.Set(from.Convert(to.Type()))
		}
	}
	return nil
}

func validTransferAttemptPhase(phase string) bool {
	switch phase {
	case "dns", "proxy", "tls", "ws", "connect", "inbox_probe", "inbox_fallback", "discovery", "banner", "probe", "create", "request", "backoff", "cancel", "result", "freeze", "exec", "import", "auth", "ready", "maintenance":
		return true
	default:
		return false
	}
}

// Sequence returns the last validated sealed batch watermark. It is evidence
// for a drain/release request, never an independently supplied progress value.
func (r *TransferCustodyReader) Sequence() uint64 { return r.sequence }

// The producer owns sticky channel failure; this pure encoder also emits the
// semantic marker so an independent reader cannot treat later records as clean.
func transferPoison(hook func(Event), poison func()) {
	hook(Event{Kind: TransferPoisonKind, Domain: "transfer", Values: [16]uint64{TransferSetupRC}})
	poison()
}
