package flowstate

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"
	"net/netip"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/LinZiyang666/tether/internal/rawsocket"
	"github.com/LinZiyang666/tether/internal/sealedstate"
)

// The descriptor shape stays local because brokerimage's public Bundle imports
// tunnel. P32 collects this literal table without adding a reverse dependency.
// origin: hot-update S6 plan §2.2 / T24
type PrivateFieldDescriptor struct {
	Name             string
	Type             string
	Encoding         string
	MaxBytes         int
	Secret           bool
	CompatibleReader string
}

type PrivateRecordDescriptor struct {
	Kind             string
	Version          uint32
	StateRecord      string
	Owner            string
	CompatibleReader string
	Fields           []PrivateFieldDescriptor
}

var ServerPrivateDescriptors = []PrivateRecordDescriptor{
	{Kind: "server", Version: 1, StateRecord: "broker-tunnel-state-v1", Owner: "P07", CompatibleReader: "broker-image-v1", Fields: []PrivateFieldDescriptor{
		{Name: "id", Type: "id16", Encoding: "id16be", MaxBytes: 16, Secret: true, CompatibleReader: "broker-image-v1"},
		{Name: "boot_id", Type: "utf8", Encoding: "utf8be", MaxBytes: 4096, Secret: true, CompatibleReader: "broker-image-v1"},
		{Name: "instance_id", Type: "utf8", Encoding: "utf8be", MaxBytes: 4096, Secret: true, CompatibleReader: "broker-image-v1"},
		{Name: "owner_gen", Type: "u64", Encoding: "u64be", MaxBytes: 8, Secret: true, CompatibleReader: "broker-image-v1"},
		{Name: "generation", Type: "u64", Encoding: "u64be", MaxBytes: 8, Secret: true, CompatibleReader: "broker-image-v1"},
		{Name: "ticket_key", Type: "bytes", Encoding: "bytesbe", MaxBytes: 32, Secret: true, CompatibleReader: "broker-image-v1"},
		{Name: "replay_limit", Type: "i64", Encoding: "i64be", MaxBytes: 8, Secret: true, CompatibleReader: "broker-image-v1"},
		{Name: "replay_used", Type: "i64", Encoding: "i64be", MaxBytes: 8, Secret: true, CompatibleReader: "broker-image-v1"},
		{Name: "pending_slots", Type: "u64", Encoding: "u64be", MaxBytes: 8, Secret: true, CompatibleReader: "broker-image-v1"},
		{Name: "waiters", Type: "bytes", Encoding: "bytesbe", MaxBytes: 2048, Secret: true, CompatibleReader: "broker-image-v1"},
		{Name: "control_descriptor", Type: "bytes", Encoding: "bytesbe", MaxBytes: 8192, Secret: true, CompatibleReader: "broker-image-v1"},
		{Name: "control_listener", Type: "utf8", Encoding: "utf8be", MaxBytes: 256, Secret: true, CompatibleReader: "broker-image-v1"},
		{Name: "flows_digest", Type: "bytes", Encoding: "bytesbe", MaxBytes: 32, Secret: true, CompatibleReader: "broker-image-v1"},
		{Name: "sessions", Type: "bytes", Encoding: "bytesbe", MaxBytes: 1048576, Secret: true, CompatibleReader: "broker-image-v1"},
	}},
	{Kind: "endpoint", Version: 1, StateRecord: "broker-tunnel-state-v1", Owner: "P07", CompatibleReader: "broker-image-v1", Fields: []PrivateFieldDescriptor{
		{Name: "id", Type: "id16", Encoding: "id16be", MaxBytes: 16, Secret: true, CompatibleReader: "broker-image-v1"},
		{Name: "allocation_sid", Type: "utf8", Encoding: "utf8be", MaxBytes: 4096, Secret: true, CompatibleReader: "broker-image-v1"},
		{Name: "allocation_nid", Type: "utf8", Encoding: "utf8be", MaxBytes: 4096, Secret: true, CompatibleReader: "broker-image-v1"},
		{Name: "allocation_port", Type: "u64", Encoding: "u64be", MaxBytes: 8, Secret: true, CompatibleReader: "broker-image-v1"},
		{Name: "allocation_token_hash", Type: "utf8", Encoding: "utf8be", MaxBytes: 4096, Secret: true, CompatibleReader: "broker-image-v1"},
		{Name: "allocation_epoch", Type: "i64", Encoding: "i64be", MaxBytes: 8, Secret: true, CompatibleReader: "broker-image-v1"},
		{Name: "phase", Type: "u64", Encoding: "u64be", MaxBytes: 8, Secret: true, CompatibleReader: "broker-image-v1"},
		{Name: "owner_gen", Type: "u64", Encoding: "u64be", MaxBytes: 8, Secret: true, CompatibleReader: "broker-image-v1"},
		{Name: "carrier_gen", Type: "u64", Encoding: "u64be", MaxBytes: 8, Secret: true, CompatibleReader: "broker-image-v1"},
		{Name: "target_port", Type: "u64", Encoding: "u64be", MaxBytes: 8, Secret: true, CompatibleReader: "broker-image-v1"},
		{Name: "ticket", Type: "bytes", Encoding: "bytesbe", MaxBytes: 8192, Secret: true, CompatibleReader: "broker-image-v1"},
		{Name: "flow_ids", Type: "bytes", Encoding: "bytesbe", MaxBytes: 65536, Secret: true, CompatibleReader: "broker-image-v1"},
		{Name: "listener_ref", Type: "utf8", Encoding: "utf8be", MaxBytes: 256, Secret: true, CompatibleReader: "broker-image-v1"},
		{Name: "listener_descriptor", Type: "bytes", Encoding: "bytesbe", MaxBytes: 8192, Secret: true, CompatibleReader: "broker-image-v1"},
		{Name: "pending_ref", Type: "utf8", Encoding: "utf8be", MaxBytes: 256, Secret: true, CompatibleReader: "broker-image-v1"},
		{Name: "pending_dev", Type: "u64", Encoding: "u64be", MaxBytes: 8, Secret: true, CompatibleReader: "broker-image-v1"},
		{Name: "pending_ino", Type: "u64", Encoding: "u64be", MaxBytes: 8, Secret: true, CompatibleReader: "broker-image-v1"},
		{Name: "pending_type", Type: "u64", Encoding: "u64be", MaxBytes: 8, Secret: true, CompatibleReader: "broker-image-v1"},
		{Name: "pending_tuple", Type: "bytes", Encoding: "bytesbe", MaxBytes: 4096, Secret: true, CompatibleReader: "broker-image-v1"},
		{Name: "deadline_boot_ns", Type: "i64", Encoding: "i64be", MaxBytes: 8, Secret: true, CompatibleReader: "broker-image-v1"},
		{Name: "fired", Type: "bool", Encoding: "boolbe", MaxBytes: 1, Secret: true, CompatibleReader: "broker-image-v1"},
		{Name: "cleanup_pending", Type: "bool", Encoding: "boolbe", MaxBytes: 1, Secret: true, CompatibleReader: "broker-image-v1"},
		{Name: "fence_port", Type: "i64", Encoding: "i64be", MaxBytes: 8, Secret: true, CompatibleReader: "broker-image-v1"},
		{Name: "fence_session", Type: "i64", Encoding: "i64be", MaxBytes: 8, Secret: true, CompatibleReader: "broker-image-v1"},
		{Name: "fence_allocation", Type: "i64", Encoding: "i64be", MaxBytes: 8, Secret: true, CompatibleReader: "broker-image-v1"},
		{Name: "lineage", Type: "bytes", Encoding: "bytesbe", MaxBytes: 32, Secret: true, CompatibleReader: "broker-image-v1"},
		{Name: "created_unix_ns", Type: "i64", Encoding: "i64be", MaxBytes: 8, Secret: true, CompatibleReader: "broker-image-v1"},
		{Name: "next_seq", Type: "u64", Encoding: "u64be", MaxBytes: 8, Secret: true, CompatibleReader: "broker-image-v1"},
	}},
	{Kind: "permit", Version: 1, StateRecord: "broker-tunnel-state-v1", Owner: "P07", CompatibleReader: "broker-image-v1", Fields: []PrivateFieldDescriptor{
		{Name: "id", Type: "id16", Encoding: "id16be", MaxBytes: 16, Secret: true, CompatibleReader: "broker-image-v1"},
		{Name: "request_id", Type: "id16", Encoding: "id16be", MaxBytes: 16, Secret: true, CompatibleReader: "broker-image-v1"},
		{Name: "cut_id", Type: "id16", Encoding: "id16be", MaxBytes: 16, Secret: true, CompatibleReader: "broker-image-v1"},
		{Name: "owner_gen", Type: "u64", Encoding: "u64be", MaxBytes: 8, Secret: true, CompatibleReader: "broker-image-v1"},
		{Name: "next_owner_gen", Type: "u64", Encoding: "u64be", MaxBytes: 8, Secret: true, CompatibleReader: "broker-image-v1"},
		{Name: "deadline_boot_ns", Type: "i64", Encoding: "i64be", MaxBytes: 8, Secret: true, CompatibleReader: "broker-image-v1"},
		{Name: "image_digest", Type: "bytes", Encoding: "bytesbe", MaxBytes: 32, Secret: true, CompatibleReader: "broker-image-v1"},
		{Name: "consumed", Type: "bool", Encoding: "boolbe", MaxBytes: 1, Secret: true, CompatibleReader: "broker-image-v1"},
		{Name: "abort", Type: "bool", Encoding: "boolbe", MaxBytes: 1, Secret: true, CompatibleReader: "broker-image-v1"},
	}},
	{Kind: "receipt", Version: 1, StateRecord: "broker-tunnel-state-v1", Owner: "P07", CompatibleReader: "broker-image-v1", Fields: []PrivateFieldDescriptor{
		{Name: "id", Type: "id16", Encoding: "id16be", MaxBytes: 16, Secret: true, CompatibleReader: "broker-image-v1"},
		{Name: "request", Type: "bytes", Encoding: "bytesbe", MaxBytes: 1048576, Secret: true, CompatibleReader: "broker-image-v1"},
		{Name: "response", Type: "bytes", Encoding: "bytesbe", MaxBytes: 1048576, Secret: true, CompatibleReader: "broker-image-v1"},
	}},
	{Kind: "fences", Version: 1, StateRecord: "broker-tunnel-state-v1", Owner: "P07", CompatibleReader: "broker-image-v1", Fields: []PrivateFieldDescriptor{
		{Name: "id", Type: "id16", Encoding: "id16be", MaxBytes: 16, Secret: true, CompatibleReader: "broker-image-v1"},
		{Name: "kill_gen_port", Type: "bytes", Encoding: "bytesbe", MaxBytes: 1048576, Secret: true, CompatibleReader: "broker-image-v1"},
		{Name: "kill_gen_session", Type: "bytes", Encoding: "bytesbe", MaxBytes: 1048576, Secret: true, CompatibleReader: "broker-image-v1"},
		{Name: "kill_gen_allocation", Type: "bytes", Encoding: "bytesbe", MaxBytes: 1048576, Secret: true, CompatibleReader: "broker-image-v1"},
		{Name: "forgotten", Type: "bytes", Encoding: "bytesbe", MaxBytes: 1048576, Secret: true, CompatibleReader: "broker-image-v1"},
		{Name: "closed_allocation", Type: "bytes", Encoding: "bytesbe", MaxBytes: 1048576, Secret: true, CompatibleReader: "broker-image-v1"},
		{Name: "inflight_by_sid", Type: "bytes", Encoding: "bytesbe", MaxBytes: 1048576, Secret: true, CompatibleReader: "broker-image-v1"},
		{Name: "inflight_by_allocation", Type: "bytes", Encoding: "bytesbe", MaxBytes: 1048576, Secret: true, CompatibleReader: "broker-image-v1"},
	}},
}

func serverDescriptor(kind string) *PrivateRecordDescriptor {
	for i := range ServerPrivateDescriptors {
		if ServerPrivateDescriptors[i].Kind == kind {
			return &ServerPrivateDescriptors[i]
		}
	}
	return nil
}

func serverFieldType(d PrivateFieldDescriptor) uint8 {
	switch d.Type {
	case "u64":
		return sealedstate.FieldU64
	case "i64":
		return sealedstate.FieldNonnegativeI64
	case "bool":
		return sealedstate.FieldBool
	case "bytes":
		return sealedstate.FieldBytes
	case "utf8":
		return sealedstate.FieldUTF8
	case "id16":
		return sealedstate.FieldID16
	}
	return 0
}

// Required empty fields prevent an omitted fence, cursor or consumed decision
// from silently becoming its permissive zero value. Both directions use the table.
// origin: hot-update S6 plan §2.2 / T24
func ServerRecord(kind string, values map[string]sealedstate.Field) (sealedstate.Record, error) {
	d := serverDescriptor(kind)
	r := sealedstate.Record{Kind: kind}
	if d == nil || len(values) != len(d.Fields) {
		return r, sealedstate.ErrInvalid
	}
	for _, fd := range d.Fields {
		f, ok := values[fd.Name]
		if !ok || !fd.Secret || fd.CompatibleReader != "broker-image-v1" || fd.Encoding != fd.Type+"be" {
			return r, sealedstate.ErrInvalid
		}
		f.Name = fd.Name
		f.Type = serverFieldType(fd)
		if f.Type == 0 || f.Type <= sealedstate.FieldBool && (len(f.Bytes) != 0 || f.Type == sealedstate.FieldBool && f.U64 > 1 || f.Type == sealedstate.FieldNonnegativeI64 && f.U64 > math.MaxInt64) || f.Type >= sealedstate.FieldBytes && (f.U64 != 0 || len(f.Bytes) > fd.MaxBytes || f.Type == sealedstate.FieldID16 && len(f.Bytes) != 16 || f.Type == sealedstate.FieldUTF8 && !utf8.Valid(f.Bytes)) {
			return r, sealedstate.ErrInvalid
		}
		f.Bytes = bytes.Clone(f.Bytes)
		r.Fields = append(r.Fields, f)
	}
	return r, nil
}

func ValidateServerRecords(records []sealedstate.Record) error {
	seen := map[string]bool{}
	counts := map[string]int{}
	for _, r := range records {
		values := ClientFields(r)
		if len(values) != len(r.Fields) {
			return sealedstate.ErrInvalid
		}
		want, err := ServerRecord(r.Kind, values)
		if err != nil {
			return err
		}
		for i, f := range r.Fields {
			if f.Name != want.Fields[i].Name || f.Type != want.Fields[i].Type {
				return sealedstate.ErrInvalid
			}
		}
		id := values["id"].Bytes
		key := r.Kind + string(id)
		if len(id) != 16 || bytes.Equal(id, make([]byte, 16)) || seen[key] {
			return sealedstate.ErrInvalid
		}
		seen[key] = true
		counts[r.Kind]++
	}
	if counts["server"] != 1 || counts["fences"] != 1 || counts["endpoint"] > MaxEndpoints || counts["permit"] > MaxEndpoints || counts["receipt"] > MaxEndpoints {
		return sealedstate.ErrInvalid
	}
	return nil
}

func EncodeServerCut(cut sealedstate.CutState, validateServerCut func(sealedstate.CutState) error) ([]byte, error) {
	if err := validateServerCut(cut); err != nil {
		return nil, err
	}
	return encodeCutState(cut, "tether-broker-tunnel-cut\x01")
}

func DecodeServerCut(data []byte, validateServerCut func(sealedstate.CutState) error) (sealedstate.CutState, error) {
	cut, err := decodeCutState(data, "tether-broker-tunnel-cut\x01", 3*MaxEndpoints+2, MaxEndpoints*2+2)
	if err != nil {
		return cut, err
	}
	if err := validateServerCut(cut); err != nil {
		return sealedstate.CutState{}, err
	}
	return cut, nil
}

// Maps are canonical sorted key/value sequences, not Go map iteration or JSON.
// Allocation keys preserve SID/NID/token as well as the reused public port.
// origin: hot-update S6 plan §2.2 / T24 / T26
func ServerMapBytes(values map[string]uint64) []byte {
	var b bytes.Buffer
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	_ = binary.Write(&b, binary.BigEndian, uint32(len(keys)))
	for _, key := range keys {
		_ = binary.Write(&b, binary.BigEndian, uint32(len(key)))
		b.WriteString(key)
		_ = binary.Write(&b, binary.BigEndian, values[key])
	}
	return b.Bytes()
}

func ServerMapRead(data []byte) (map[string]uint64, error) {
	out := map[string]uint64{}
	r := bytes.NewReader(data)
	var n uint32
	if binary.Read(r, binary.BigEndian, &n) != nil || uint64(n) > uint64(r.Len()/12) {
		return nil, sealedstate.ErrInvalid
	}
	last := ""
	for i := uint32(0); i < n; i++ {
		var size uint32
		var value uint64
		if binary.Read(r, binary.BigEndian, &size) != nil || uint64(size) > uint64(r.Len()) {
			return nil, sealedstate.ErrInvalid
		}
		key := make([]byte, size)
		if _, err := io.ReadFull(r, key); err != nil {
			return nil, sealedstate.ErrInvalid
		}
		if binary.Read(r, binary.BigEndian, &value) != nil || value > math.MaxInt64 || i > 0 && string(key) <= last {
			return nil, sealedstate.ErrInvalid
		}
		last = string(key)
		out[last] = value
	}
	if r.Len() != 0 {
		return nil, sealedstate.ErrInvalid
	}
	return out, nil
}

func ServerTupleBytes(t rawsocket.Tuple) []byte {
	return []byte(t.Local.String() + "\n" + t.Remote.String())
}

func ServerTupleRead(p []byte) (rawsocket.Tuple, error) {
	fields := strings.Split(string(p), "\n")
	if len(fields) != 2 {
		return rawsocket.Tuple{}, sealedstate.ErrInvalid
	}
	local, err := netip.ParseAddrPort(fields[0])
	if err != nil {
		return rawsocket.Tuple{}, sealedstate.ErrInvalid
	}
	remote, err := netip.ParseAddrPort(fields[1])
	if err != nil {
		return rawsocket.Tuple{}, sealedstate.ErrInvalid
	}
	return rawsocket.Tuple{Local: local, Remote: remote}, nil
}
