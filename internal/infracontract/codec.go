package infracontract

import (
	"bytes"
	"crypto/ed25519"
	"encoding"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"slices"
	"strings"
	"unicode/utf8"
)

var (
	errInvalidRecord = errors.New("invalid infra record")
	errSignature     = errors.New("infra_permission_denied")
)

// FieldDescriptor records the exact Go wire spelling and type, including fixed
// array lengths. Records are versioned separately from public NATS messages.
// The codec checks these rows against the real struct before using JSON; a DTO
// edit alone cannot silently change the private protocol.
type FieldDescriptor struct {
	Name string
	Type string
}
type RecordDescriptor struct {
	Name     string
	Version  uint16
	MaxBytes int
	Fields   []FieldDescriptor
}

type recordShape struct {
	typ    reflect.Type
	fields string
}

// ScalarDescriptor closes named integer families and fixed byte arrays. Zero
// remains available inside absent optional structs; each root's admission rules
// decide where it is legal. Values excludes zero because it is not an enum member.
type ScalarDescriptor struct {
	Name   string
	Kind   string
	Length int
	Values []uint64
}

// ScalarDescriptors supplies the actual codec checks and the inventory reader.
// Literals are independent of enum constants so renumbering cannot silently move
// both the implementation and its protocol descriptor together.
// origin: hot-update S9 plan §2.1 / §2.2 / T01 / T02
func ScalarDescriptors() []ScalarDescriptor {
	return []ScalarDescriptor{
		{"ID", "array", 16, nil}, {"Digest", "array", 32, nil},
		{"Component", "uint8", 0, []uint64{1, 2, 3}},
		{"Phase", "uint8", 0, []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}},
		{"Action", "uint8", 0, []uint64{1, 2, 3, 4}},
		{"ArtifactKind", "uint8", 0, []uint64{1, 2, 3, 4}},
		{"RoutePurpose", "uint8", 0, []uint64{1, 2}},
		{"RouteDirection", "uint8", 0, []uint64{1, 2}},
		{"HostOp", "uint8", 0, []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18}},
	}
}

// CodecDescriptors returns a copy of the very shapes enforced by the codec.
// Independent literal goldens in the tests freeze these rows in both directions.
// origin: hot-update S9 plan §2.1 / §2.2 / T01
func CodecDescriptors() []RecordDescriptor {
	rows := make([]RecordDescriptor, 0, len(recordShapes))
	for _, shape := range recordShapes {
		row := RecordDescriptor{Name: shape.typ.Name(), Version: 1, MaxBytes: MaxRequestBytes}
		for _, entry := range strings.Split(shape.fields, ";") {
			name, typ, _ := strings.Cut(entry, "|")
			row.Fields = append(row.Fields, FieldDescriptor{Name: name, Type: typ})
		}
		rows = append(rows, row)
	}
	return rows
}

// EncodeRequest validates operation operands before emitting the one canonical
// spelling. An empty signature becomes null so unsigned requests can be signed.
// This does not authorize the request or resolve any root-owned digest.
// origin: hot-update S9 plan §2.2 / T02
func EncodeRequest(r HostRequest) ([]byte, error) {
	if err := validateRequest(r); err != nil {
		return nil, err
	}
	return encodeRecord(r)
}

// DecodeRequest accepts only canonical bytes. Re-encoding and exact comparison
// rejects duplicate keys (including nested keys), aliases/case folding, omitted
// fields, unknown fields, trailing whitespace/data, short arrays and lossy UTF-8.
// Do not replace this check with json.Unmarshal or DisallowUnknownFields alone.
// origin: hot-update S9 plan §2.2 / T02
func DecodeRequest(data []byte) (HostRequest, error) {
	var r HostRequest
	if err := decodeRecord(data, &r); err != nil {
		return HostRequest{}, err
	}
	if err := validateRequest(r); err != nil {
		return HostRequest{}, err
	}
	return r, nil
}

// SigningBytes binds every field, including forbidden/optional zero operands,
// to a domain that cannot be confused with a receipt, route proof or journal.
// origin: hot-update S9 plan §2.2 / T02
func SigningBytes(r HostRequest) ([]byte, error) {
	r.Signature = nil
	data, err := EncodeRequest(r)
	if err != nil {
		return nil, err
	}
	return append([]byte(RequestSigningDomain), data...), nil
}

// VerifyRequest uses a raw 32-byte Ed25519 public key supplied by the trusted
// policy owner. Callers receiving bytes must first use DecodeRequest. Identity,
// boot, policy revision, sequence and domain CAS are additionally rechecked by
// the host; a valid signature alone never authorizes an effect.
// origin: hot-update S9 plan §2.2 / T02
func VerifyRequest(r HostRequest, publicKey []byte) error {
	if len(publicKey) != ed25519.PublicKeySize || len(r.Signature) != ed25519.SignatureSize {
		return errSignature
	}
	data, err := SigningBytes(r)
	if err != nil {
		return err
	}
	if !ed25519.Verify(ed25519.PublicKey(publicKey), data, r.Signature) {
		return errSignature
	}
	return nil
}

// A receipt has the frozen §2.1 shape, without an in-band Version field. It is
// bound to the versioned request by RequestDigest; root IPC/journal owners must
// preserve that association. No codec may add a field to that frozen shape.
func EncodeReceipt(r HostReceipt) ([]byte, error) {
	if r.Tx == (ID{}) || r.Sequence == 0 || r.Revision == 0 || r.Op < Inspect || r.Op > VerifyIngress || r.RequestDigest == (Digest{}) || r.Journal == (Digest{}) {
		return nil, errInvalidRecord
	}
	return encodeRecord(r)
}

func DecodeReceipt(data []byte) (HostReceipt, error) {
	var r HostReceipt
	if err := decodeRecord(data, &r); err != nil {
		return HostReceipt{}, err
	}
	if _, err := EncodeReceipt(r); err != nil {
		return HostReceipt{}, err
	}
	return r, nil
}

// Route proofs use their own purpose and version, and never share the host
// request signing domain. P16 owns signing, authority and the original expiry.
func EncodeRouteProof(r RouteProof) ([]byte, error) {
	if r.Version != 1 || !slices.Contains([]RoutePurpose{RoutePrepare, RouteBind}, r.Purpose) || !slices.Contains([]RouteDirection{RouteOutbound, RouteReturn}, r.Direction) || r.Tx == (ID{}) || r.Participant == (ID{}) || r.Nonce == (ID{}) || r.HomeDigest == (Digest{}) || r.AuthorityRevision == 0 || r.PolicyRevision == 0 || r.RemainingNS == 0 || r.RemainingNS > math.MaxInt64 || r.KeyID == "" || (len(r.Signature) != 0 && len(r.Signature) != ed25519.SignatureSize) {
		return nil, errInvalidRecord
	}
	if r.Target.Component != NATS && r.Target.Component != Caddy {
		return nil, errInvalidRecord
	}
	if r.Target.URL == "" || r.Target.NodeID == "" || !validAttachment(r.Old) || r.Old.Participant != r.Participant {
		return nil, errInvalidRecord
	}
	// A not-yet-running N1 successor has Before+Candidate; an already-running
	// survivor/warm target has Ingress. Neither variant may invent a future CID.
	if r.Target.Before != (Process{}) && !validProcess(r.Target.Before) {
		return nil, errInvalidRecord
	}
	if r.Target.Ingress != (Process{}) && !validProcess(r.Target.Ingress) {
		return nil, errInvalidRecord
	}
	if r.Target.Candidate != (Artifact{}) && !validArtifact(r.Target.Candidate, r.Target.Component) {
		return nil, errInvalidRecord
	}
	if r.Target.Ingress == (Process{}) && (r.Target.Before == (Process{}) || r.Target.Candidate == (Artifact{})) {
		return nil, errInvalidRecord
	}
	if r.Purpose == RoutePrepare && r.New != (Attachment{}) {
		return nil, errInvalidRecord
	}
	if r.Purpose == RouteBind && (!validAttachment(r.New) || r.New.Participant != r.Participant || r.New.Generation <= r.Old.Generation || r.New.Raw == r.Old.Raw) {
		return nil, errInvalidRecord
	}
	return encodeRecord(r)
}

func DecodeRouteProof(data []byte) (RouteProof, error) {
	var r RouteProof
	if err := decodeRecord(data, &r); err != nil {
		return RouteProof{}, err
	}
	if _, err := EncodeRouteProof(r); err != nil {
		return RouteProof{}, err
	}
	return r, nil
}

// ValidateObservation rejects structurally empty producers and unknown kinds.
// It cannot establish that evidence came from the kernel: P02/P32 provide that
// observation, and P35 checks the emitter set rather than trusting Status logs.
func ValidateObservation(o Observation) error {
	if o.Version != 1 || o.Tx == (ID{}) || o.Step == 0 || !slices.Contains(ObservationKinds(), o.Kind) || o.BootID == "" || o.BootNS <= 0 || o.Subject == (ID{}) || o.Generation == 0 || o.Evidence == (Digest{}) {
		return errInvalidRecord
	}
	_, err := encodeRecord(o)
	return err
}

// ValidatePlan keeps HostSupport out of the public component surface. Platform
// support is a before-effect backend decision, not a guess from these DTOs.
func ValidatePlan(p Plan) error {
	if p.Tx == (ID{}) || p.Step == 0 || p.Domain == (ID{}) || (p.Component != NATS && p.Component != Caddy) || p.Action < Upgrade || p.Action > Recover || !validArtifact(p.Candidate, p.Component) || !validArtifact(p.Previous, p.Component) || !validProcess(p.Before) || p.ConfigSHA256 == (Digest{}) || p.PolicyRevision == 0 {
		return errInvalidRecord
	}
	if p.Store != (Store{}) && !validStore(p.Store) {
		return errInvalidRecord
	}
	if p.Component == NATS && !validStore(p.Store) {
		return errInvalidRecord
	}
	_, err := encodeRecord(p)
	return err
}

func validateRequest(r HostRequest) error {
	if r.Version != RequestVersion || r.Op < Inspect || r.Op > VerifyIngress || r.Tx == (ID{}) || r.Step == 0 || r.Domain == (ID{}) || r.Sequence == 0 || r.PolicyRevision == 0 || r.KeyID == "" || r.HostBootID == "" || r.Nonce == (ID{}) || r.RemainingNS == 0 || r.RemainingNS > math.MaxInt64 || (len(r.Signature) != 0 && len(r.Signature) != ed25519.SignatureSize) {
		return errInvalidRecord
	}
	rule := HostOpDescriptors()[int(r.Op)-1]
	if !slices.Contains(rule.Components, r.Component) {
		return errInvalidRecord
	}
	value := reflect.ValueOf(r)
	for _, field := range []string{"Expected", "Candidate", "Store", "ExpectedJournal", "Config", "BeforeCensus", "WarmCensus", "ReturnProof", "Payload"} {
		present := !value.FieldByName(field).IsZero()
		if slices.Contains(rule.Required, field) {
			if !present {
				return errInvalidRecord
			}
		} else if present && !slices.Contains(rule.Optional, field) {
			return errInvalidRecord
		}
	}
	if r.Expected != (Process{}) && (!validProcess(r.Expected) || r.Expected.BootID != r.HostBootID) {
		return errInvalidRecord
	}
	if r.Candidate != (Artifact{}) && !validArtifact(r.Candidate, r.Component) {
		return errInvalidRecord
	}
	if r.Store != (Store{}) && !validStore(r.Store) {
		return errInvalidRecord
	}
	return nil
}

func validProcess(p Process) bool {
	return p.BootID != "" && p.PID > 0 && p.StartTicks != 0 && p.InvocationID != "" && p.ExeDevice != 0 && p.ExeInode != 0 && p.ExeSHA256 != (Digest{}) && p.Cgroup != ""
}

func validStore(s Store) bool {
	return s.Device != 0 && s.Inode != 0 && s.MountID != 0 && s.LockDevice != 0 && s.LockInode != 0
}

func validArtifact(a Artifact, component Component) bool {
	kindOK := (component == NATS && a.Kind == NATSBinary) || (component == Caddy && a.Kind == CaddyBinary) || (component == HostSupport && (a.Kind == HelperBinary || a.Kind == LauncherBinary))
	return kindOK && a.Version != "" && a.GOOS != "" && a.GOARCH != "" && a.ArchiveSHA256 != (Digest{}) && a.BinarySHA256 != (Digest{}) && a.CatalogSHA256 != (Digest{})
}

func validAttachment(a Attachment) bool {
	s := a.Raw
	return a.Participant != (ID{}) && a.Generation != 0 && a.ServerID != "" && a.CID != 0 && a.URL != "" && a.HealthGeneration != 0 && s.NetNS != 0 && s.Device != 0 && s.Inode != 0 && s.Cookie != 0 && s.Local != "" && s.Remote != "" && validProcess(s.Owner) && validProcess(a.Ingress)
}

// The generic boundary remains a closed set, not an arbitrary JSON extension.
type codecRecord interface {
	HostRequest | HostReceipt | RouteProof | Observation | Plan
}

func encodeRecord[T codecRecord](record T) ([]byte, error) {
	value, err := normalizeRecord(reflect.ValueOf(record))
	if err != nil {
		return nil, err
	}
	data, err := json.Marshal(value.Interface())
	if err != nil || len(data) > MaxRequestBytes {
		return nil, errInvalidRecord
	}
	return data, nil
}

func decodeRecord[T codecRecord](data []byte, target *T) error {
	if len(data) == 0 || len(data) > MaxRequestBytes || !utf8.Valid(data) {
		return errInvalidRecord
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return errInvalidRecord
	}
	canonical, err := encodeRecord(*target)
	if err != nil || !bytes.Equal(data, canonical) {
		return errInvalidRecord
	}
	return nil
}

// Normalization is copy-on-encode; signing never edits a caller's slices. Shape
// checks are in the encoding path, not an unused table that scanners alone see.
// Only the closed struct/array/slice/primitive graph is admitted. No maps,
// pointers, interfaces, custom JSON methods or arbitrary extension bags cross it.
// origin: hot-update S9 plan §2.1 / §2.2 / T02
func normalizeRecord(v reflect.Value) (reflect.Value, error) {
	// A future MarshalJSON/MarshalText method must not bypass the frozen fields.
	for _, marshaler := range []reflect.Type{reflect.TypeFor[json.Marshaler](), reflect.TypeFor[encoding.TextMarshaler]()} {
		if v.Type().Implements(marshaler) || reflect.PointerTo(v.Type()).Implements(marshaler) {
			return reflect.Value{}, errInvalidRecord
		}
	}
	for _, scalar := range ScalarDescriptors() {
		if v.Type().PkgPath() != reflect.TypeFor[HostRequest]().PkgPath() || v.Type().Name() != scalar.Name {
			continue
		}
		if v.Kind().String() != scalar.Kind {
			return reflect.Value{}, errInvalidRecord
		}
		if scalar.Kind == "array" {
			if v.Len() != scalar.Length || v.Type().Elem().Kind() != reflect.Uint8 {
				return reflect.Value{}, errInvalidRecord
			}
		} else if v.Uint() != 0 && !slices.Contains(scalar.Values, v.Uint()) {
			return reflect.Value{}, errInvalidRecord
		}
	}
	switch v.Kind() {
	case reflect.Struct:
		fields := ""
		for _, shape := range recordShapes {
			if shape.typ == v.Type() {
				fields = shape.fields
				break
			}
		}
		if fields == "" {
			return reflect.Value{}, errInvalidRecord
		}
		expected := strings.Split(fields, ";")
		if len(expected) != v.NumField() {
			return reflect.Value{}, errInvalidRecord
		}
		copyValue := reflect.New(v.Type()).Elem()
		for i, row := range expected {
			name, typ, _ := strings.Cut(row, "|")
			f := v.Type().Field(i)
			if f.Name != name || f.Type.String() != typ || f.Tag != "" || f.Anonymous || !f.IsExported() {
				return reflect.Value{}, errInvalidRecord
			}
			field, err := normalizeRecord(v.Field(i))
			if err != nil {
				return reflect.Value{}, err
			}
			copyValue.Field(i).Set(field)
		}
		return copyValue, nil
	case reflect.Slice, reflect.Array:
		if v.Len() > MaxRequestBytes {
			return reflect.Value{}, errInvalidRecord
		}
		copyValue := reflect.New(v.Type()).Elem()
		if v.Kind() == reflect.Slice && v.Len() > 0 {
			copyValue = reflect.MakeSlice(v.Type(), v.Len(), v.Len())
		}
		for i := 0; i < v.Len(); i++ {
			item, err := normalizeRecord(v.Index(i))
			if err != nil {
				return reflect.Value{}, err
			}
			copyValue.Index(i).Set(item)
		}
		return copyValue, nil
	case reflect.String:
		if !utf8.ValidString(v.String()) || len(v.String()) > MaxRequestBytes {
			return reflect.Value{}, errInvalidRecord
		}
	case reflect.Bool, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Int32, reflect.Int64:
	default:
		return reflect.Value{}, errInvalidRecord
	}
	return v, nil
}

// Declaration order and untagged Go names are the private version-one ABI.
var recordShapes = []recordShape{
	{reflect.TypeFor[Process](), "BootID|string;PID|int32;StartTicks|uint64;InvocationID|string;ExeDevice|uint64;ExeInode|uint64;ExeSHA256|infracontract.Digest;Cgroup|string"},
	{reflect.TypeFor[Socket](), "NetNS|uint64;Device|uint64;Inode|uint64;Cookie|uint64;Local|string;Remote|string;Owner|infracontract.Process"},
	{reflect.TypeFor[Attachment](), "Participant|infracontract.ID;Generation|uint64;ServerID|string;CID|uint64;URL|string;Raw|infracontract.Socket;Ingress|infracontract.Process;HealthGeneration|uint64"},
	{reflect.TypeFor[Store](), "Device|uint64;Inode|uint64;MountID|uint64;LockDevice|uint64;LockInode|uint64"},
	{reflect.TypeFor[Artifact](), "Kind|infracontract.ArtifactKind;Version|string;GOOS|string;GOARCH|string;ArchiveSHA256|infracontract.Digest;BinarySHA256|infracontract.Digest;CatalogSHA256|infracontract.Digest"},
	{reflect.TypeFor[Plan](), "Tx|infracontract.ID;Step|uint32;Domain|infracontract.ID;Component|infracontract.Component;Action|infracontract.Action;Candidate|infracontract.Artifact;Previous|infracontract.Artifact;Before|infracontract.Process;ConfigSHA256|infracontract.Digest;Store|infracontract.Store;PolicyRevision|uint64"},
	{reflect.TypeFor[Census](), "Complete|bool;Generation|uint64;Processes|[]infracontract.Process;Sockets|[]infracontract.Socket;LiveBusiness|uint32;OldOwnerPending|uint32;Digest|infracontract.Digest"},
	{reflect.TypeFor[Status](), "Tx|infracontract.ID;Step|uint32;Phase|infracontract.Phase;Revision|uint64;Code|string;Detail|string;Before|infracontract.Process;Current|infracontract.Process;Census|infracontract.Census;Proof|infracontract.Digest"},
	{reflect.TypeFor[AssetSeal](), "Revision|uint64;Accounts|infracontract.Digest;Assets|infracontract.Digest;Participants|infracontract.Digest;Complete|bool"},
	{reflect.TypeFor[Readiness](), "Topology|infracontract.Digest;Seal|infracontract.AssetSeal;Auth|infracontract.Digest;WSS|infracontract.Digest;Running|infracontract.Process;MetaLeader|string;Groups|infracontract.Digest;Complete|bool"},
	{reflect.TypeFor[RouteTarget](), "URL|string;NodeID|string;Component|infracontract.Component;Before|infracontract.Process;Candidate|infracontract.Artifact;Ingress|infracontract.Process"},
	{reflect.TypeFor[MoveResult](), "Census|infracontract.Census;Attachments|[]infracontract.Attachment"},
	{reflect.TypeFor[RouteProof](), "Purpose|infracontract.RoutePurpose;Direction|infracontract.RouteDirection;Target|infracontract.RouteTarget;Version|uint16;Tx|infracontract.ID;Participant|infracontract.ID;Old|infracontract.Attachment;New|infracontract.Attachment;HomeDigest|infracontract.Digest;AuthorityRevision|uint64;PolicyRevision|uint64;RemainingNS|uint64;Nonce|infracontract.ID;KeyID|string;Signature|[]uint8"},
	{reflect.TypeFor[TransferSeal](), "Version|uint16;SourceSHA256|infracontract.Digest;SchemaSHA256|infracontract.Digest;InvocationSet|infracontract.Digest;ReceiptSet|infracontract.Digest;ConsumerSet|infracontract.Digest;Complete|bool"},
	{reflect.TypeFor[Checkpoint](), "Cohort|infracontract.ID;Assets|infracontract.AssetSeal;Transfers|infracontract.TransferSeal;Proof|infracontract.Digest"},
	{reflect.TypeFor[Observation](), "Version|uint16;Tx|infracontract.ID;Step|uint32;Kind|string;BootID|string;BootNS|int64;Subject|infracontract.ID;Generation|uint64;Evidence|infracontract.Digest"},
	{reflect.TypeFor[HostRequest](), "Version|uint16;Op|infracontract.HostOp;Component|infracontract.Component;Tx|infracontract.ID;Step|uint32;Domain|infracontract.ID;Sequence|uint64;PolicyRevision|uint64;KeyID|string;HostBootID|string;Nonce|infracontract.ID;Expected|infracontract.Process;Candidate|infracontract.Artifact;Store|infracontract.Store;ExpectedJournal|infracontract.Digest;Config|infracontract.Digest;BeforeCensus|infracontract.Digest;WarmCensus|infracontract.Digest;ReturnProof|infracontract.Digest;Payload|infracontract.Digest;RemainingNS|uint64;Signature|[]uint8"},
	{reflect.TypeFor[HostReceipt](), "Tx|infracontract.ID;Sequence|uint64;Revision|uint64;Op|infracontract.HostOp;Applied|bool;RequestDigest|infracontract.Digest;Journal|infracontract.Digest;Current|infracontract.Process;Census|infracontract.Census;Code|string"},
	{reflect.TypeFor[Peer](), "PID|int32;UID|uint32;GID|uint32;BootID|string;StartTicks|uint64"},
}
