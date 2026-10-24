package brokerimage

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/LinZiyang666/tether/internal/brokermaint"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/imageref"
	"github.com/LinZiyang666/tether/internal/sealedstate"
	"github.com/LinZiyang666/tether/internal/storage"
)

var ErrImage = errors.New("brokerimage: invalid image")
var ErrActive = errors.New("brokerimage: current runtime cut required")

// RuntimeState duplicates the public lineage in sealed storage. ACTIVE and CutID
// also survive a new cut; a pristine image cannot be relabeled after a constructor.
// origin: hot-update S6 plan §2.2 / §4.2
// All fields are explicit: a missing zero decision is not an implicit default.
type RuntimeState struct {
	Role            string   `json:"role"`
	Mode            Mode     `json:"mode"`
	Args            []string `json:"args"`
	ConfigDigest    string   `json:"config_digest"`
	Generation      uint64   `json:"generation"`
	BootID          string   `json:"boot_id"`
	InstanceID      string   `json:"instance_id"`
	ExporterSHA256  string   `json:"exporter_sha256"`
	CandidateSHA256 string   `json:"candidate_sha256"`
	PreviousSHA256  string   `json:"previous_sha256"`
	RequiredDomains []string `json:"required_domains"`
	Active          bool     `json:"active"`
	CutID           string   `json:"cut_id"`
}

// ControlState contains decisions, never a NATS connection or serialized heap.
// Historical baseline bytes are canonical decisions from the maintenance owner;
// neither a new clock nor a fresh roster may reset planned return history.
type ControlState struct {
	AdmissionGeneration uint64                   `json:"admission_generation"`
	RequiredHandlers    []string                 `json:"required_handlers"`
	RequiredLoops       []string                 `json:"required_loops"`
	AuthReady           bool                     `json:"auth_ready"`
	ControlReady        bool                     `json:"control_ready"`
	Maintenance         []brokermaint.LocalLease `json:"maintenance"`
	HomeBaselines       []HomeBaseline           `json:"home_baselines"`
	// The broker owns the allocation identities and original cache challenges.
	// Keep their full projection sealed; a lease alone cannot restore nonce/t0
	// or historical dwell after exec without extending a maintenance deadline.
	// origin: hot-update S6 plan §2.2 / §7.2 / T43
	HomeHistory []byte `json:"home_history"`
}
type HomeBaseline struct {
	Home          string `json:"home"`
	DwellBootNS   int64  `json:"dwell_boot_ns"`
	PlannedDown   bool   `json:"planned_down"`
	PlannedReturn bool   `json:"planned_return"`
}

// ResourceOwnership records one terminal owner. Prepared copies are borrowed
// aliases of that owner; they must never appear as a second terminal resource.
type ResourceOwnership struct {
	ID       string `json:"id"`
	Data     []byte `json:"data"`
	Kind     string `json:"kind"`
	Name     string `json:"name"`
	Purpose  string `json:"purpose"`
	Identity string `json:"identity"`
	Owner    string `json:"owner"`
	CutID    string `json:"cut_id"`
}
type PrivateState struct {
	Runtime   RuntimeState
	Store     storage.StoreSeal
	Control   ControlState
	Resources []ResourceOwnership
}

// CodecRecordDescriptors describes the actual four closed records consumed by
// this codec. P32 unions the fields with P03's store projection; the P00 skeleton
// stays unchanged. JSON values are length-framed inside the sealed binary record,
// never put in the handoff's public Data. These records contain no SS key material.
// origin: hot-update S6 plan §2.2 / T47
var CodecRecordDescriptors = []PrivateRecordDescriptor{
	{Kind: "broker-runtime", Version: 1, StateRecord: PrivateStateRecord, Owner: "P02", CompatibleReader: Domain, Fields: privateFields(reflect.TypeFor[RuntimeState]())},
	{Kind: "control", Version: 1, StateRecord: PrivateStateRecord, Owner: "P02", CompatibleReader: Domain, Fields: privateFields(reflect.TypeFor[ControlState]())},
	{Kind: "resources", Version: 1, StateRecord: PrivateStateRecord, Owner: "P02", CompatibleReader: Domain, Fields: []PrivateFieldDescriptor{{Name: "owners", Type: "[]brokerimage.ResourceOwnership", Encoding: "canonical-json", MaxBytes: MaxBundleBytes, CompatibleReader: Domain}}},
	{Kind: "store", Version: 1, StateRecord: PrivateStateRecord, Owner: "P03", CompatibleReader: Domain, Fields: privateFields(reflect.TypeFor[storage.StoreSeal]())},
}

func privateFields(typ reflect.Type) []PrivateFieldDescriptor {
	fields := make([]PrivateFieldDescriptor, typ.NumField())
	for i := range fields {
		field := typ.Field(i)
		fields[i] = PrivateFieldDescriptor{Name: field.Tag.Get("json"), Type: field.Type.String(), Encoding: "canonical-json", MaxBytes: MaxBundleBytes, CompatibleReader: Domain}
	}
	sort.Slice(fields, func(i, j int) bool { return fields[i].Name < fields[j].Name })
	return fields
}

// Encode validates at the final cut, not merely at preflight, since required
// domains and resource ownership may have changed while fencing producers.
// origin: hot-update S6 plan §2.1 / §4.1
func Encode(b Bundle) ([]byte, error) {
	if err := Validate(b); err != nil {
		return nil, err
	}
	data, err := json.Marshal(b)
	if err != nil || len(data) > MaxBundleBytes {
		return nil, ErrImage
	}
	return data, nil
}
func Decode(state handoff.State) (Bundle, error) {
	var b Bundle
	if len(state.Data) > MaxBundleBytes || decodeCanonical(state.Data, &b) != nil {
		return b, ErrImage
	}
	if state.Role != ImageRole || state.Instance != b.InstanceID || state.Generation != b.Generation {
		return b, ErrImage
	}
	return b, Validate(b)
}
func Validate(b Bundle) error {
	if b.Schema != Domain || b.SchemaVersion != BundleVersion || b.Role != ImageRole || b.Generation == 0 ||
		(b.Mode != ModeSingle && b.Mode != ModeSoleVoter && b.Mode != ModeQuorum) ||
		!validID(b.InstanceID) || !validID(b.BootID) || !validID(b.StateResource) || !validID(b.LockResource) || b.StateResource == b.LockResource ||
		!validSHA(b.ConfigDigest) || !validSHA(b.ExporterSHA256) || !validSHA(b.CandidateSHA256) || !validSHA(b.PreviousSHA256) ||
		(b.PreviousSHA256 != b.ExporterSHA256 && b.PreviousSHA256 != b.CandidateSHA256) ||
		len(b.Args) == 0 || len(b.Args) > 4096 || !canonicalStrings(b.RequiredDomains, true) || !contains(b.RequiredDomains, Domain) ||
		b.Listeners == nil || b.Maintenance == nil || !reflect.DeepEqual(b.Store, b.Seal.Stamp) {
		return ErrImage
	}
	for _, arg := range b.Args {
		if len(arg) > 65536 || strings.IndexByte(arg, 0) >= 0 || !utf8.ValidString(arg) {
			return ErrImage
		}
	}
	identity := b.Store.Identity
	if !filepath.IsAbs(identity.Path) || filepath.Clean(identity.Path) != identity.Path || filepath.Dir(identity.Path) != identity.DataDir || identity.LockPath != filepath.Join(identity.DataDir, "tether.lock") || identity.Device == 0 || identity.Inode == 0 {
		return ErrImage
	}
	if validateSeal(b.Seal) != nil {
		return ErrImage
	}
	// A public ref cannot excuse skipping the sealed tracker. Validate all
	// bounds and reader-domain obligations before taking a single resource.
	// origin: hot-update S7 plan §8.2 / §8.3 / P24
	if ref := b.Transfers; ref != nil {
		if ref.Schema != "transfer-v1" || !validID(ref.ResourceID) || !validSHA(ref.Digest) || ref.Count > 1024 || ref.EncodedBytes == 0 || ref.EncodedBytes > sealedstate.MaxStateBytes-16 || ref.ResourceID == b.StateResource || ref.ResourceID == b.LockResource || !contains(b.RequiredDomains, "transfer-v1") {
			return ErrImage
		}
	}
	// Keep the final cut's original reader-domain bounds at the shared codec
	// boundary; validation must not diverge between import and Prepare.
	// origin: hot-update S7 plan §8.3 / P24
	for _, domain := range b.RequiredDomains {
		if len(domain) > 64 {
			return ErrImage
		}
	}
	if retainedFlow(b.Tunnel) && !contains(b.RequiredDomains, "flow-v1") {
		return ErrImage
	}
	// Original keepers are always protected. A false advertised flag would
	// turn an otherwise pure listener refusal into a post-open ACTIVE failure.
	// origin: hot-update S6 plan §4.1 / T27
	for i, d := range b.Listeners {
		if !d.CloseOnExec || !validID(d.ID) || (i > 0 && b.Listeners[i-1].ID >= d.ID) || d.ID == b.StateResource || d.ID == b.LockResource {
			return ErrImage
		}
	}
	for i, lease := range b.Maintenance {
		if !validID(lease.Record.CutID) || (i > 0 && b.Maintenance[i-1].Record.Home >= lease.Record.Home) {
			return ErrImage
		}
	}
	return nil
}
func validateSeal(s storage.StoreSeal) error {
	if !StoreSealClosed(s) || s.Stamp.Synchronous != 2 {
		return ErrImage
	}
	return validateSchema(s.Stamp.Schema)
}
func validateSchema(s storage.SchemaStamp) error {
	if !validSHA(s.Digest) || !validSHA(s.ObjectDigest) || s.Applied == nil || s.Supported == nil || len(s.Applied) == 0 ||
		!reflect.DeepEqual(s.Applied, s.Supported) || migrationDigest(s.Applied) != s.Digest {
		return ErrImage
	}
	for i, m := range s.Applied {
		if !validID(m.Name) || !strings.HasSuffix(m.Name, ".sql") || !validSHA(m.SHA256) || (i > 0 && s.Applied[i-1].Name >= m.Name) {
			return ErrImage
		}
	}
	return nil
}

// Match storage's applied/support projection exactly. Distinct digest formats
// made every real P03 receipt fail the P02 reader while synthetic probes passed.
// origin: hot-update S6 plan §3.1 / T06 / T07
func migrationDigest(ms []storage.MigrationDigest) string {
	body, _ := json.Marshal(struct{ Applied, Supported []storage.MigrationDigest }{ms, ms})
	digest := sha256.Sum256(body)
	return hex.EncodeToString(digest[:])
}

func validSHA(s string) bool { return imageref.ValidSHA(s) }
func validID(s string) bool  { return imageref.ValidID(s) }
func canonicalStrings(ss []string, required bool) bool {
	return imageref.CanonicalStrings(ss, required)
}
func contains(ss []string, s string) bool {
	return slices.Contains(ss, s)
}

func decodeCanonical(data []byte, target any) error {
	if !imageref.DecodeCanonical(data, target) {
		return ErrImage
	}
	return nil
}

func runtimeState(b Bundle, cutID string) RuntimeState {
	return RuntimeState{Role: b.Role, Mode: b.Mode, Args: b.Args, ConfigDigest: b.ConfigDigest, Generation: b.Generation, BootID: b.BootID, InstanceID: b.InstanceID, ExporterSHA256: b.ExporterSHA256, CandidateSHA256: b.CandidateSHA256, PreviousSHA256: b.PreviousSHA256, RequiredDomains: b.RequiredDomains, CutID: cutID}
}

// EncodePrivateState uses a bounded binary envelope with exactly four records.
// Each named field has an explicit length. The descriptor order is the encoding
// order; an unknown, duplicate, missing or reordered field cannot be skipped.
// origin: hot-update S6 plan §2.2 / FuzzBrokerPrivateState
func EncodePrivateState(state PrivateState) ([]byte, error) {
	if err := validatePrivateCodec(); err != nil {
		return nil, err
	}
	values := []any{state.Runtime, state.Control, struct {
		Owners []ResourceOwnership `json:"owners"`
	}{state.Resources}, state.Store}
	var out bytes.Buffer
	out.WriteString("TETHBR6\x00")
	_ = binary.Write(&out, binary.BigEndian, uint32(PrivateDescriptorVersion))
	for i, descriptor := range CodecRecordDescriptors {
		data, err := json.Marshal(values[i])
		if err != nil {
			return nil, ErrImage
		}
		var fields map[string]json.RawMessage
		if json.Unmarshal(data, &fields) != nil {
			return nil, ErrImage
		}
		writePrivatePart(&out, []byte(descriptor.Kind))
		for _, field := range descriptor.Fields {
			value, ok := fields[field.Name]
			if !ok || len(value) > field.MaxBytes {
				return nil, ErrImage
			}
			writePrivatePart(&out, []byte(field.Name))
			writePrivatePart(&out, value)
		}
	}
	if out.Len() > sealedstate.MaxStateBytes-16 {
		return nil, ErrImage
	}
	return out.Bytes(), nil
}
func writePrivatePart(out *bytes.Buffer, value []byte) {
	_ = binary.Write(out, binary.BigEndian, uint32(len(value)))
	out.Write(value)
}
func DecodePrivateState(data []byte) (PrivateState, error) {
	var state PrivateState
	if err := validatePrivateCodec(); err != nil {
		return state, err
	}
	if len(data) < 12 || len(data) > sealedstate.MaxStateBytes-16 || string(data[:8]) != "TETHBR6\x00" || binary.BigEndian.Uint32(data[8:12]) != PrivateDescriptorVersion {
		return state, ErrImage
	}
	input := bytes.NewReader(data[12:])
	owners := struct {
		Owners []ResourceOwnership `json:"owners"`
	}{}
	targets := []any{&state.Runtime, &state.Control, &owners, &state.Store}
	for i, descriptor := range CodecRecordDescriptors {
		kind, err := readPrivatePart(input, 256)
		if err != nil || string(kind) != descriptor.Kind {
			return state, ErrImage
		}
		fields := make(map[string]json.RawMessage, len(descriptor.Fields))
		for _, field := range descriptor.Fields {
			name, err := readPrivatePart(input, 256)
			if err != nil || string(name) != field.Name {
				return state, ErrImage
			}
			value, err := readPrivatePart(input, field.MaxBytes)
			if err != nil || !json.Valid(value) {
				return state, ErrImage
			}
			fields[field.Name] = value
		}
		// Reorder according to the explicit Go projection before exact validation.
		blob, err := json.Marshal(fields)
		if err != nil {
			return state, ErrImage
		}
		dec := json.NewDecoder(bytes.NewReader(blob))
		dec.DisallowUnknownFields()
		if dec.Decode(targets[i]) != nil {
			return state, ErrImage
		}
	}
	if input.Len() != 0 {
		return state, ErrImage
	}
	state.Resources = owners.Owners
	canonical, err := EncodePrivateState(state)
	if err != nil || !bytes.Equal(data, canonical) {
		return state, ErrImage
	}
	return state, nil
}
func readPrivatePart(in *bytes.Reader, limit int) ([]byte, error) {
	var size uint32
	if binary.Read(in, binary.BigEndian, &size) != nil || uint64(size) > uint64(limit) || uint64(size) > uint64(in.Len()) {
		return nil, ErrImage
	}
	data := make([]byte, int(size))
	if _, err := io.ReadFull(in, data); err != nil {
		return nil, ErrImage
	}
	return data, nil
}

// CheckPrivateState binds sealed lineage and every required zero decision to the
// public envelope. No private contents appear in a diagnostic on refusal.
func CheckPrivateState(state PrivateState, b Bundle) error {
	expected := runtimeState(b, state.Runtime.CutID)
	expected.Active = state.Runtime.Active
	if !reflect.DeepEqual(state.Runtime, expected) || !validID(state.Runtime.CutID) || !reflect.DeepEqual(state.Store, b.Seal) ||
		state.Control.AdmissionGeneration != b.Generation || !canonicalStrings(state.Control.RequiredHandlers, false) || !canonicalStrings(state.Control.RequiredLoops, false) ||
		!reflect.DeepEqual(state.Control.Maintenance, b.Maintenance) || state.Control.HomeBaselines == nil || state.Resources == nil || len(state.Resources) > 16384 {
		return ErrImage
	}
	previous := ""
	transferState := b.Transfers == nil
	for _, owner := range state.Resources {
		key := owner.ID + "/" + owner.Name
		if !validID(owner.ID) || !validID(owner.Kind) || !validID(owner.Name) || !validID(owner.Purpose) || !validID(owner.Owner) || owner.CutID != state.Runtime.CutID || len(owner.Identity) == 0 || len(owner.Identity) > 512 || key <= previous || owner.Kind == "xfer-state-v1" && (b.Transfers == nil || owner.ID != b.Transfers.ResourceID) {
			return ErrImage
		}
		previous = key
		if b.Transfers != nil && owner.ID == b.Transfers.ResourceID {
			if transferState || owner.Name != "state" || owner.Kind != "xfer-state-v1" || len(owner.Data) != 0 {
				return ErrImage
			}
			transferState = true
		}
	}
	if !transferState {
		return ErrImage
	}
	for i, h := range state.Control.HomeBaselines {
		if !validID(h.Home) || h.DwellBootNS < 0 || (i > 0 && state.Control.HomeBaselines[i-1].Home >= h.Home) {
			return ErrImage
		}
	}
	return nil
}

// Descriptor metadata is part of the private reader contract, including fields
// encoded through reflection. Refuse a table that would frame a different type,
// reader or secret class instead of allowing its declaration to drift silently.
// origin: hot-update S6 plan §2.2 / T47
func validatePrivateCodec() error {
	projections := []reflect.Type{reflect.TypeFor[RuntimeState](), reflect.TypeFor[ControlState](), reflect.TypeFor[struct {
		Owners []ResourceOwnership `json:"owners"`
	}](), reflect.TypeFor[storage.StoreSeal]()}
	kinds := []string{"broker-runtime", "control", "resources", "store"}
	if len(CodecRecordDescriptors) != len(projections) {
		return ErrImage
	}
	for i, record := range CodecRecordDescriptors {
		if record.Kind != kinds[i] || record.Version != PrivateDescriptorVersion || record.StateRecord != PrivateStateRecord || record.CompatibleReader != Domain || len(record.Fields) != projections[i].NumField() {
			return ErrImage
		}
		// One field projection drives both encoding and validation. Exact
		// equality still rejects extra/missing fields, order, type, encoding,
		// bound, secret flag and compatible-reader drift.
		// origin: hot-update S7 plan §8.3 / P24
		if !reflect.DeepEqual(record.Fields, privateFields(projections[i])) {
			return ErrImage
		}
	}
	return nil
}
