package ssstate

import (
	"encoding/binary"
	"math"
	"sort"
	"unicode/utf8"

	"github.com/LinZiyang666/tether/internal/sealedstate"
)

// The private envelope is TSSS, version:u16, reserved:u16, records:u32.
// Integers are big endian. Each record is kind-length:u16, kind, fields:u16;
// each field is name-length:u16, name, type:u8, value-length:u32, value.
// Even empty values carry their declared name/type/length: absence must never
// reset a nonce, consumed SO_ERROR, revoke decision or pending byte suffix.
// Only callers that seal the result may persist these bytes. Errors deliberately
// carry neither field values nor attacker-controlled names.
// origin: hot-update S5 plan §5.1 / §6.6 / §12.1
const stateHeaderBytes = 12

func encodeState(records []sealedstate.Record) ([]byte, error) {
	// Account before allocating secret output: individually valid buffers can
	// exceed the total cut cap. No partial output escapes a failed validation.
	size := stateHeaderBytes
	identities := make(map[stateRecordIdentity]struct{})
	for _, record := range records {
		// Kind matching is exact; case folding would create a second identity
		// for the same owner and could bypass duplicate-record rejection.
		d, known := stateRecordDescriptor(record.Kind)
		if !known {
			return nil, sealedstate.ErrInvalid
		}
		id, err := validateStateRecord(record, d)
		if err != nil {
			return nil, err
		}
		if err := addStateIdentity(identities, record.Kind, id); err != nil {
			return nil, err
		}
		if !addStateSize(&size, 4+len(record.Kind)) {
			return nil, sealedstate.ErrCapacity
		}
		for _, field := range record.Fields {
			if !addStateSize(&size, 7+len(field.Name)+stateValueSize(field)) {
				return nil, sealedstate.ErrCapacity
			}
		}
	}
	// All records have passed validation before any output is emitted. The
	// exact size prevents reallocation copies of secret state during encoding.
	data := make([]byte, 0, size)
	data = append(data, 'T', 'S', 'S', 'S')
	data = binary.BigEndian.AppendUint16(data, sealedstate.Version)
	data = binary.BigEndian.AppendUint16(data, 0)
	data = binary.BigEndian.AppendUint32(data, uint32(len(records)))
	for _, record := range records {
		data = appendStateName(data, record.Kind)
		data = binary.BigEndian.AppendUint16(data, uint16(len(record.Fields)))
		// Sort a copy so canonical encoding cannot mutate the owner's snapshot.
		fields := append([]sealedstate.Field(nil), record.Fields...)
		sort.Slice(fields, func(i, j int) bool { return fields[i].Name < fields[j].Name })
		for _, field := range fields {
			data = appendStateName(data, field.Name)
			data = append(data, field.Type)
			data = binary.BigEndian.AppendUint32(data, uint32(stateValueSize(field)))
			switch field.Type {
			case sealedstate.FieldU64, sealedstate.FieldNonnegativeI64:
				data = binary.BigEndian.AppendUint64(data, field.U64)
			case sealedstate.FieldBool:
				data = append(data, byte(field.U64))
			// Name every byte representation; validation above rejects unknown types.
			// origin: hot-update S5 plan §5.1 / §10.2 / T23
			case sealedstate.FieldBytes, sealedstate.FieldUTF8, sealedstate.FieldID16, sealedstate.FieldNonce12, sealedstate.FieldSalt32:
				data = append(data, field.Bytes...)
			default:
				data = append(data, field.Bytes...)
			}
		}
	}
	return data, nil
}

func decodeState(data []byte) ([]sealedstate.Record, error) {
	// Reject the envelope before trusting counts. Slices use remaining bytes,
	// so attacker-controlled u32 lengths cannot overflow an index.
	// origin: hot-update S5 plan §5.1 / T23 / T24
	if len(data) > sealedstate.MaxStateBytes {
		return nil, sealedstate.ErrCapacity
	}
	if len(data) < stateHeaderBytes || string(data[:4]) != "TSSS" ||
		binary.BigEndian.Uint16(data[4:6]) != sealedstate.Version || binary.BigEndian.Uint16(data[6:8]) != 0 {
		return nil, sealedstate.ErrInvalid
	}
	count := binary.BigEndian.Uint32(data[8:12])
	remaining := data[stateHeaderBytes:]
	// An ID16 alone costs 25 bytes. Do not preallocate from an untrusted count;
	// append records only after their full descriptor and identity validate.
	if uint64(count) > uint64(len(remaining))/25 {
		return nil, sealedstate.ErrInvalid
	}
	var records []sealedstate.Record
	identities := make(map[stateRecordIdentity]struct{})
	for n := uint32(0); n < count; n++ {
		kind, ok := takeStateName(&remaining)
		if !ok || len(remaining) < 2 {
			return nil, sealedstate.ErrInvalid
		}
		d, known := stateRecordDescriptor(kind)
		if !known {
			return nil, sealedstate.ErrInvalid
		}
		fieldCount := int(binary.BigEndian.Uint16(remaining[:2]))
		remaining = remaining[2:]
		// The descriptor bounds metadata too: a forged field count cannot
		// allocate a field slice sized from untrusted input.
		if fieldCount > len(d.Fields) {
			return nil, sealedstate.ErrInvalid
		}
		record := sealedstate.Record{Kind: kind}
		for j := 0; j < fieldCount; j++ {
			field, err := takeStateField(kind, d, &remaining)
			if err != nil {
				return nil, err
			}
			record.Fields = append(record.Fields, field)
		}
		id, err := validateStateRecord(record, d)
		if err != nil {
			return nil, err
		}
		if err := addStateIdentity(identities, kind, id); err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	// A valid prefix is insufficient: accepting its suffix would hide a
	// second record/decision or a version mismatch behind a valid envelope.
	if len(remaining) != 0 {
		return nil, sealedstate.ErrInvalid
	}
	return records, nil
}

// Identity includes kind: independent owners may use the same typed ID.
type stateRecordIdentity struct {
	kind string
	id   [16]byte
}

// A repeated kind/ID would otherwise let runtime restore choose first or last
// ownership independently. Neither choice can preserve a unique cut owner.
// origin: hot-update S5 plan §12.1 / T23
func addStateIdentity(seen map[stateRecordIdentity]struct{}, kind string, id [16]byte) error {
	key := stateRecordIdentity{kind: kind, id: id}
	if _, duplicate := seen[key]; duplicate {
		return sealedstate.ErrInvalid
	}
	seen[key] = struct{}{}
	return nil
}

// The P0 table is the only schema. Linear lookup avoids a separately cached
// table whose required/type decisions could drift from the actual descriptor.
func stateRecordDescriptor(kind string) (sealedstate.RecordDescriptor, bool) {
	for _, d := range sealedstate.RecordDescriptors {
		if d.Kind == kind {
			return d, true
		}
	}
	return sealedstate.RecordDescriptor{}, false
}

// Names are the declared wire IDs; aliases and unknown extensions require a
// codec version decision rather than being silently discarded during import.
func stateFieldDescriptor(d sealedstate.RecordDescriptor, name string) (sealedstate.FieldDescriptor, bool) {
	for _, field := range d.Fields {
		if field.Name == name {
			return field, true
		}
	}
	return sealedstate.FieldDescriptor{}, false
}

func validateStateRecord(record sealedstate.Record, d sealedstate.RecordDescriptor) ([16]byte, error) {
	var id [16]byte
	seen := make(map[string]struct{}, len(d.Fields))
	for _, field := range record.Fields {
		descriptor, known := stateFieldDescriptor(d, field.Name)
		if !known {
			return id, sealedstate.ErrInvalid
		}
		if _, duplicate := seen[field.Name]; duplicate {
			return id, sealedstate.ErrInvalid
		}
		seen[field.Name] = struct{}{}
		if err := validateStateField(record.Kind, field, descriptor); err != nil {
			return id, err
		}
		if field.Name == "id" {
			copy(id[:], field.Bytes)
		}
	}
	// Required includes zero/empty decisions; a duplicate or alias cannot stand
	// in for an omitted nonce, FIFO, pending buffer or irreversible receipt.
	// origin: hot-update S5 plan §6.1 / §6.3 / T23
	for _, field := range d.Fields {
		if _, present := seen[field.Name]; field.Required && !present {
			return id, sealedstate.ErrInvalid
		}
	}
	return id, nil
}

// Primitive validity is shared by encode/decode so a runtime cannot export a
// cut that this reader would reject, or import dual value representations.
func validateStateField(kind string, field sealedstate.Field, d sealedstate.FieldDescriptor) error {
	if field.Type != d.Type {
		return sealedstate.ErrInvalid
	}
	switch field.Type {
	case sealedstate.FieldU64, sealedstate.FieldNonnegativeI64, sealedstate.FieldBool:
		// Reject dual representations instead of silently dropping Bytes/U64.
		if len(field.Bytes) != 0 || (field.Type == sealedstate.FieldNonnegativeI64 && field.U64 > math.MaxInt64) ||
			(field.Type == sealedstate.FieldBool && field.U64 > 1) || !validStateEnum(kind, field.Name, field.U64) {
			return sealedstate.ErrInvalid
		}
	case sealedstate.FieldBytes, sealedstate.FieldUTF8, sealedstate.FieldID16, sealedstate.FieldNonce12, sealedstate.FieldSalt32:
		if field.U64 != 0 || (d.Width != 0 && len(field.Bytes) != d.Width) ||
			(field.Type == sealedstate.FieldUTF8 && !utf8.Valid(field.Bytes)) {
			return sealedstate.ErrInvalid
		}
		if d.Width == 0 && len(field.Bytes) > d.MaxBytes {
			return sealedstate.ErrCapacity
		}
	default:
		return sealedstate.ErrInvalid
	}
	return nil
}

// Frozen closed wire ranges are not a second field schema. Layering forbids
// importing flow/tunnel here. Zero connect/open/reason is an unset/success
// decision; phase=0 is invalid. Owners check cross-field semantics before IO.
// origin: hot-update S5 plan §2.1 / §2.2 / §6.1 / T23
func validStateEnum(kind, name string, value uint64) bool {
	switch name {
	case "phase":
		switch kind {
		case sealedstate.KindSSActor:
			return value >= 1 && value <= 9
		case sealedstate.KindAuthority:
			return value >= 1 && value <= 3
		case sealedstate.KindFlow:
			return value >= 1 && value <= 5
		case sealedstate.KindEndpoint:
			return value >= 1 && value <= 6
		}
	case "authority_phase":
		return value >= 1 && value <= 3
	case "connect_state", "close_mode", "open_status":
		return value <= 3
	case "verdict":
		return value >= 1 && value <= 3
	case "purpose":
		return value >= 1 && value <= 2
	case "reason", "open_reason", "close_reason":
		return value <= 16
	case "fin_flags":
		// The frozen schema declares u8 storage; the owning flow codec checks
		// the bit meanings when constructing/consuming its RESUME entries.
		return value <= math.MaxUint8
	}
	return true
}

func stateValueSize(field sealedstate.Field) int {
	switch field.Type {
	case sealedstate.FieldU64, sealedstate.FieldNonnegativeI64:
		return 8
	case sealedstate.FieldBool:
		return 1
	// Size accounting must name the same closed types as encode/decode.
	// origin: hot-update S5 plan §5.1 / §10.2 / T23
	case sealedstate.FieldBytes, sealedstate.FieldUTF8, sealedstate.FieldID16, sealedstate.FieldNonce12, sealedstate.FieldSalt32:
		return len(field.Bytes)
	default:
		return len(field.Bytes)
	}
}

// Subtract from the fixed limit before adding, so even aggregate size arithmetic
// cannot wrap an int and convert an oversized cut into a small allocation.
func addStateSize(size *int, extra int) bool {
	if extra > sealedstate.MaxStateBytes-*size {
		return false
	}
	*size += extra
	return true
}

func appendStateName(data []byte, name string) []byte {
	data = binary.BigEndian.AppendUint16(data, uint16(len(name)))
	return append(data, name...)
}

// Names cannot be empty. The remaining-byte check precedes conversion/slicing
// and leaves every malformed prefix on the same redacted invalid-state path.
func takeStateName(data *[]byte) (string, bool) {
	if len(*data) < 2 {
		return "", false
	}
	n := int(binary.BigEndian.Uint16((*data)[:2]))
	*data = (*data)[2:]
	if n == 0 || n > len(*data) {
		return "", false
	}
	name := string((*data)[:n])
	*data = (*data)[n:]
	return name, true
}

func takeStateField(kind string, descriptor sealedstate.RecordDescriptor, data *[]byte) (sealedstate.Field, error) {
	name, ok := takeStateName(data)
	if !ok || len(*data) < 5 {
		return sealedstate.Field{}, sealedstate.ErrInvalid
	}
	d, known := stateFieldDescriptor(descriptor, name)
	field := sealedstate.Field{Name: name, Type: (*data)[0]}
	n := uint64(binary.BigEndian.Uint32((*data)[1:5]))
	*data = (*data)[5:]
	// Enforce width before scalar reads and capacity before copying byte values.
	// A hostile length never drives allocation or reaches an unsafe slice.
	// origin: hot-update S5 plan §5.1 / T23 / T24
	if !known || field.Type != d.Type || n > uint64(len(*data)) ||
		(d.Width != 0 && n != uint64(d.Width)) {
		return sealedstate.Field{}, sealedstate.ErrInvalid
	}
	if d.Width == 0 && n > uint64(d.MaxBytes) {
		return sealedstate.Field{}, sealedstate.ErrCapacity
	}
	value := (*data)[:int(n)]
	*data = (*data)[int(n):]
	switch field.Type {
	case sealedstate.FieldU64, sealedstate.FieldNonnegativeI64:
		field.U64 = binary.BigEndian.Uint64(value)
	case sealedstate.FieldBool:
		field.U64 = uint64(value[0])
	// Decoded byte values never alias input, for every declared byte type.
	// origin: hot-update S5 plan §5.1 / §10.2 / T23
	case sealedstate.FieldBytes, sealedstate.FieldUTF8, sealedstate.FieldID16, sealedstate.FieldNonce12, sealedstate.FieldSalt32:
		field.Bytes = append([]byte(nil), value...)
	default:
		// Import may scrub/reuse its sealed input: no decoded buffer may alias it.
		field.Bytes = append([]byte(nil), value...)
	}
	if err := validateStateField(kind, field, d); err != nil {
		return sealedstate.Field{}, err
	}
	return field, nil
}

// Encode validates the complete cut before emitting any secret bytes.
func Encode(records []sealedstate.Record) ([]byte, error) { return encodeState(records) }

// Decode rejects malformed cuts without returning partial private state.
func Decode(data []byte) ([]sealedstate.Record, error) { return decodeState(data) }

// RecordDescriptor resolves the sole frozen schema for direct runtime restore.
func RecordDescriptor(kind string) (sealedstate.RecordDescriptor, bool) {
	return stateRecordDescriptor(kind)
}

// ValidateRecord applies the codec's primitive guards to a direct private cut.
func ValidateRecord(r sealedstate.Record, d sealedstate.RecordDescriptor) ([16]byte, error) {
	return validateStateRecord(r, d)
}
