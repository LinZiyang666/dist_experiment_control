package xferimage

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"time"
	"unicode/utf8"

	"github.com/LinZiyang666/tether/internal/xfercontract"
)

// This is the shared sealed ceiling, not a separate transfer pool. The outer
// cut must reserve these bytes together with all other domains, input/output
// copies, Prepared duplicates and salvage before creating the sealed resource.
// origin: hot-update S7 plan §8.3 / T20
const trackerMaxBytes = 32 << 20
const trackerHeaderBytes = 16
const trackerMaxEntries = 1024
const trackerMagic = "TETHXTR\x00"
const trackerCodecVersion = 1

var errTrackerImage = errors.New("xferimage: invalid tracker state")

var _ TrackerEncoder = EncodeTracker
var _ TrackerDecoder = DecodeTracker

// EncodeTracker serializes the whole frozen private projection. Canonical
// records are opaque bytes: re-creating their timestamps or request IDs here
// would turn a pending publication into a different history decision.
// origin: hot-update S7 plan §5.1 / §8.1–§8.3 / T20
func EncodeTracker(state xfercontract.TrackerState) ([]byte, error) {
	// Measure before Marshal allocates. In particular, a huge hostile slice
	// must fail before an encoder copy exists alongside the original state.
	remaining := int64(trackerMaxBytes - trackerHeaderBytes)
	if !trackerMeasure(reflect.ValueOf(state), &remaining) {
		return nil, errTrackerImage
	}
	if validateTracker(state) != nil {
		return nil, errTrackerImage
	}
	body, err := json.Marshal(state)
	if err != nil || len(body) > trackerMaxBytes-trackerHeaderBytes {
		return nil, errTrackerImage
	}
	encoded := make([]byte, trackerHeaderBytes+len(body))
	copy(encoded, trackerMagic)
	binary.BigEndian.PutUint32(encoded[8:12], trackerCodecVersion)
	binary.BigEndian.PutUint32(encoded[12:16], uint32(len(body)))
	copy(encoded[trackerHeaderBytes:], body)
	return encoded, nil
}

// DecodeTracker has no IO, clock read or side effect. Boot deadlines are bound
// to the retained origin boot; the outer importer also compares that origin
// with its public image lineage before installing the sleeping owner.
// origin: hot-update S7 plan §8.1–§8.2 / T20 / T53
func DecodeTracker(encoded []byte) (xfercontract.TrackerState, error) {
	var state xfercontract.TrackerState
	if !trackerValidFrame(encoded) {
		return state, errTrackerImage
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded[trackerHeaderBytes:]))
	// Reject oversized collections before the typed decoder can allocate their
	// backing arrays. JSON length alone cannot bound a slice of large structs.
	// Include the input and canonical comparison copy in the shared 64MiB peak.
	allocation := int64(64<<20) - 2*int64(len(encoded))
	decoder.UseNumber()
	if !trackerScan(decoder, reflect.TypeFor[xfercontract.TrackerState](), &allocation) || allocation < 0 {
		return state, errTrackerImage
	}
	decoder = json.NewDecoder(bytes.NewReader(encoded[trackerHeaderBytes:]))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&state) != nil || decoder.Decode(new(any)) != io.EOF {
		return xfercontract.TrackerState{}, errTrackerImage
	}
	// Exact re-encoding rejects duplicate, omitted and case-folded fields,
	// including zero-valued retirement decisions that DisallowUnknownFields
	// alone would silently accept. Never return partially decoded ownership.
	remaining := int64(trackerMaxBytes - trackerHeaderBytes)
	if !trackerMeasure(reflect.ValueOf(state), &remaining) || validateTracker(state) != nil {
		return xfercontract.TrackerState{}, errTrackerImage
	}
	canonical, err := json.Marshal(state)
	if err != nil || !bytes.Equal(encoded[trackerHeaderBytes:], canonical) {
		return xfercontract.TrackerState{}, errTrackerImage
	}
	return state, nil
}

// Keep the framing guard separate from JSON semantics so an oversized body is
// refused before the scanner can allocate a token or decoder read buffer.
// origin: hot-update S7 plan §8.3 / T53
func trackerValidFrame(encoded []byte) bool {
	return len(encoded) >= trackerHeaderBytes && len(encoded) <= trackerMaxBytes &&
		string(encoded[:8]) == trackerMagic && binary.BigEndian.Uint32(encoded[8:12]) == trackerCodecVersion &&
		uint64(binary.BigEndian.Uint32(encoded[12:16])) == uint64(len(encoded)-trackerHeaderBytes)
}

// Scan against the actual type closure: no arbitrary nesting, unknown fields
// or unbounded arrays are allocated before their lengths have been checked.
// Null presence and required zero fields are finally checked by exact encoding.
// origin: hot-update S7 plan §8.3 / T53
func trackerScan(decoder *json.Decoder, typ reflect.Type, allocation *int64) bool {
	token, err := decoder.Token()
	if err != nil {
		return false
	}
	if token == nil {
		return typ.Kind() == reflect.Pointer || typ.Kind() == reflect.Slice
	}
	for typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
		*allocation -= int64(typ.Size())
	}
	if *allocation < 0 {
		return false
	}
	if typ == reflect.TypeFor[time.Time]() {
		_, ok := token.(string)
		return ok
	}
	switch typ.Kind() {
	case reflect.Struct:
		if token != json.Delim('{') {
			return false
		}
		fields := make(map[string]reflect.Type, typ.NumField())
		for i := 0; i < typ.NumField(); i++ {
			field := typ.Field(i)
			name := field.Name
			if tag := field.Tag.Get("json"); tag != "" {
				for j, c := range tag {
					if c == ',' {
						tag = tag[:j]
						break
					}
				}
				name = tag
			}
			fields[name] = field.Type
		}
		for decoder.More() {
			key, err := decoder.Token()
			name, ok := key.(string)
			field, found := fields[name]
			if err != nil || !ok || !found || !trackerScan(decoder, field, allocation) {
				return false
			}
			delete(fields, name)
		}
		end, err := decoder.Token()
		return err == nil && end == json.Delim('}')
	case reflect.Slice:
		if typ.Elem().Kind() == reflect.Uint8 {
			value, ok := token.(string)
			*allocation -= int64(len(value))
			return ok && *allocation >= 0
		}
		if token != json.Delim('[') {
			return false
		}
		limit := 16384
		if typ.Elem() == reflect.TypeFor[xfercontract.LedgerRow]() || typ.Elem() == reflect.TypeFor[xfercontract.CanonicalRecord]() ||
			typ.Elem() == reflect.TypeFor[xfercontract.ReapedEntry]() || typ.Elem() == reflect.TypeFor[xfercontract.WatchdogDeadline]() ||
			typ.Elem() == reflect.TypeFor[xfercontract.GrantSlot]() || typ.Elem() == reflect.TypeFor[xfercontract.Protection]() ||
			typ.Elem() == reflect.TypeFor[xfercontract.CleanupState]() || typ.Elem() == reflect.TypeFor[xfercontract.ConfigAttempt]() {
			limit = trackerMaxEntries
		}
		count := 0
		for decoder.More() {
			count++
			// Go's append may retain nearly twice the live backing storage.
			*allocation -= 2 * int64(typ.Elem().Size())
			if count > limit || *allocation < 0 || !trackerScan(decoder, typ.Elem(), allocation) {
				return false
			}
		}
		end, err := decoder.Token()
		return err == nil && end == json.Delim(']')
	case reflect.String:
		value, ok := token.(string)
		*allocation -= int64(len(value))
		return ok && len(value) <= 65536 && *allocation >= 0
	case reflect.Bool:
		_, ok := token.(bool)
		return ok
	case reflect.Int, reflect.Int32, reflect.Int64, reflect.Uint32, reflect.Uint64:
		_, ok := token.(json.Number)
		return ok
	default:
		return false
	}
}

// JSON visits every exported field of this P00 projection, recursively. The
// measurement uses the same closure; time.Time is its sole opaque scalar.
// Byte slices use base64, strings reserve worst-case JSON escaping, and field
// names/commas/numbers also count. There are no maps or runtime handles here.
// origin: hot-update S7 plan §8.3 / T53
func trackerMeasure(value reflect.Value, remaining *int64) bool {
	*remaining -= 8
	if *remaining < 0 {
		return false
	}
	if value.Type() == reflect.TypeFor[time.Time]() {
		*remaining -= 64
		return *remaining >= 0
	}
	switch value.Kind() {
	case reflect.Pointer:
		return value.IsNil() || trackerMeasure(value.Elem(), remaining)
	case reflect.Struct:
		for i := 0; i < value.NumField(); i++ {
			*remaining -= int64(len(value.Type().Field(i).Name) + 4)
			if !trackerMeasure(value.Field(i), remaining) {
				return false
			}
		}
	case reflect.Slice:
		if value.Type().Elem().Kind() == reflect.Uint8 {
			*remaining -= 4 * ((int64(value.Len()) + 2) / 3)
			return *remaining >= 0
		}
		if value.Len() > 16384 {
			return false
		}
		for i := 0; i < value.Len(); i++ {
			if !trackerMeasure(value.Index(i), remaining) {
				return false
			}
		}
	case reflect.String:
		if value.Len() > 65536 || !utf8.ValidString(value.String()) {
			return false
		}
		*remaining -= 6 * int64(value.Len())
	case reflect.Bool:
	case reflect.Int, reflect.Int32, reflect.Int64, reflect.Uint32, reflect.Uint64:
		*remaining -= 24
	default:
		return false
	}
	return *remaining >= 0
}
