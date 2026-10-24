package flowstate

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"math"
	"sort"
	"unicode/utf8"

	"github.com/LinZiyang666/tether/internal/flow"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/sealedstate"
	"github.com/LinZiyang666/tether/internal/workload"
)

func SetClientStateRef(im *ClientImage, ref string) {
	im.StateRef, im.Flows.StateRef = ref, ref
	for i := range im.Endpoints {
		im.Endpoints[i].StateRef = ref
	}
}

func ClientStateRef(ref string) bool {
	if len(ref) != len("flow-state.")+32 || ref[:len("flow-state.")] != "flow-state." {
		return false
	}
	id, err := hex.DecodeString(ref[len("flow-state."):])
	return err == nil && hex.EncodeToString(id) == ref[len("flow-state."):]
}

func ClientFields(rec sealedstate.Record) map[string]sealedstate.Field {
	out := make(map[string]sealedstate.Field, len(rec.Fields))
	for _, field := range rec.Fields {
		out[field.Name] = field
	}
	return out
}

// The public image is derived from typed private records, never independently
// assembled metadata. This makes swapped endpoints and omitted flow FDs fail
// before restore transfers any business ownership.
// origin: hot-update S5 plan §5.1 / T32
func ClientImageFromCut(cut sealedstate.CutState, ref string) (ClientImage, error) {
	im := ClientImage{Version: flow.Version, Flows: flow.Image{Version: flow.Version}}
	if err := ValidateClientRecords(cut.Records); err != nil {
		return ClientImage{}, err
	}
	endpoints := make(map[flow.EndpointID]map[string]sealedstate.Field)
	flows := make(map[flow.ID]map[string]sealedstate.Field)
	for _, rec := range cut.Records {
		f := ClientFields(rec)
		var id [16]byte
		copy(id[:], f["id"].Bytes)
		switch rec.Kind {
		case sealedstate.KindEndpoint:
			if f["owner_gen"].U64 == 0 || f["phase"].U64 < uint64(EndpointPhaseBinding) || f["phase"].U64 > uint64(EndpointPhaseClosed) || len(f["flow_ids"].Bytes)%16 != 0 {
				return ClientImage{}, sealedstate.ErrInvalid
			}
			// Client cuts do not own the broker's public listener/pending accepts.
			if len(f["listener_ref"].Bytes) != 0 || len(f["pending_raw_refs"].Bytes) != 0 {
				return ClientImage{}, sealedstate.ErrInvalid
			}
			endpoints[id] = f
			ep := EndpointImage{ID: id, OwnerGen: f["owner_gen"].U64, CarrierGen: f["carrier_gen"].U64}
			for ids := f["flow_ids"].Bytes; len(ids) > 0; ids = ids[16:] {
				var flowID flow.ID
				copy(flowID[:], ids[:16])
				ep.FlowIDs = append(ep.FlowIDs, flowID)
			}
			im.Endpoints = append(im.Endpoints, ep)
		case sealedstate.KindFlow:
			if f["phase"].U64 < uint64(flow.FlowPhaseOpening) || f["phase"].U64 > uint64(flow.FlowPhaseTerminal) || f["fin_flags"].U64 > math.MaxUint8 || f["tx_base"].U64 > f["tx_end"].U64 || f["rx_applied"].U64 > f["rx_received"].U64 {
				return ClientImage{}, sealedstate.ErrInvalid
			}
			flows[id] = f
			ri := flow.RecordImage{ID: id, OpenSeq: f["open_seq"].U64, Phase: flow.Phase(f["phase"].U64), TxBase: f["tx_base"].U64, TxEnd: f["tx_end"].U64, RxApplied: f["rx_applied"].U64, RxReceived: f["rx_received"].U64, TxFIN: f["tx_fin"].U64, RxFIN: f["rx_fin"].U64, FINFlags: uint8(f["fin_flags"].U64), RawRef: string(f["raw_ref"].Bytes), ReplayRef: string(f["replay_ref"].Bytes)}
			copy(ri.Intent[:], f["intent"].Bytes)
			// A live journal entry cannot omit an already-owned business FD.
			// origin: hot-update S5 plan §3.2 / §5.1 / T32
			if ri.OpenSeq == 0 || (ri.Phase != flow.FlowPhaseTerminal && ri.ReplayRef == "") || (ri.Phase >= flow.FlowPhaseConnecting && ri.Phase < flow.FlowPhaseTerminal && ri.RawRef == "") {
				return ClientImage{}, sealedstate.ErrInvalid
			}
			if ri.ReplayRef != "" {
				im.Flows.ReservedBytes += workload.DefaultReplayCapacity
			}
			im.Flows.Records = append(im.Flows.Records, ri)
		case sealedstate.KindRetirement:
			if len(f["holes"].Bytes)%8 != 0 || f["next_open_seq"].U64 == 0 || f["retired_through"].U64 >= f["next_open_seq"].U64 {
				return ClientImage{}, sealedstate.ErrInvalid
			}
			var epID flow.EndpointID
			copy(epID[:], f["endpoint_id"].Bytes)
			r := flow.RetirementImage{EndpointID: epID, NextOpenSeq: f["next_open_seq"].U64, RetiredThrough: f["retired_through"].U64}
			for holes := f["holes"].Bytes; len(holes) > 0; holes = holes[8:] {
				r.Holes = append(r.Holes, binary.BigEndian.Uint64(holes[:8]))
			}
			im.Flows.Retirement = append(im.Flows.Retirement, r)
		case sealedstate.KindPermit, sealedstate.KindReceipt, sealedstate.KindAuthority, sealedstate.KindBinding, sealedstate.KindTimer:
			// These remain private and are restored unchanged by the runtime hook.
		// SS records cannot be spliced into a client flow cut.
		// origin: hot-update S5 plan §5.1 / §10.2 / T32
		case sealedstate.KindMaterial, sealedstate.KindSaltRing, sealedstate.KindSSActor, sealedstate.KindSSServer:
			return ClientImage{}, sealedstate.ErrInvalid
		default:
			return ClientImage{}, sealedstate.ErrInvalid
		}
	}
	if len(endpoints) > MaxEndpoints || ActiveClientImageFlows(im) > flow.MaxFlows || len(flows)-ActiveClientImageFlows(im) > flow.MaxTerminalMetadata || len(cut.Records) > MaxClientCutRecords {
		return ClientImage{}, sealedstate.ErrCapacity
	}
	SetClientStateRef(&im, ref)
	if err := ValidateClientClosure(cut, im, endpoints, flows); err != nil {
		return ClientImage{}, err
	}
	return im, nil
}

// Every auxiliary record is bounded separately from terminal flow receipts.
// origin: hot-update S5 internal review rv-image-2
const MaxClientCutRecords = flow.MaxFlows + 2*flow.MaxTerminalMetadata + 7*MaxEndpoints

func ActiveClientImageFlows(im ClientImage) int {
	active := 0
	for _, record := range im.Flows.Records {
		if record.Phase != flow.FlowPhaseTerminal {
			active++
		}
	}
	return active
}

func ValidateClientClosure(cut sealedstate.CutState, im ClientImage, endpoints map[flow.EndpointID]map[string]sealedstate.Field, flows map[flow.ID]map[string]sealedstate.Field) error {
	listed := make(map[flow.ID]bool)
	for _, ep := range im.Endpoints {
		for _, id := range ep.FlowIDs {
			f, exists := flows[id]
			// Terminal decisions retain their original generation when a later
			// handoff has no live entry to rebind. Only active owners require the
			// current generation; a future or zero receipt owner is still invalid.
			// origin: hot-update S5 internal review PRIVATE-STATE-1
			owner := f["owner_gen"].U64
			wrongOwner := owner == 0 || owner > ep.OwnerGen || f["phase"].U64 != uint64(flow.FlowPhaseTerminal) && owner != ep.OwnerGen
			if !exists || listed[id] || !bytes.Equal(f["endpoint_id"].Bytes, ep.ID[:]) || wrongOwner {
				return sealedstate.ErrInvalid
			}
			listed[id] = true
		}
	}
	if len(listed) != len(flows) {
		return sealedstate.ErrInvalid
	}
	permits := make(map[flow.EndpointID]bool)
	for _, rec := range cut.Records {
		f := ClientFields(rec)
		var epID flow.EndpointID
		copy(epID[:], f["endpoint_id"].Bytes)
		switch rec.Kind {
		case sealedstate.KindPermit:
			ep, exists := endpoints[epID]
			owner := f["owner_gen"].U64
			if !exists || permits[epID] || owner == 0 || owner == math.MaxUint64 || f["next_owner_gen"].U64 != owner+1 || f["purpose"].U64 != uint64(TicketPurposeHandoff) || bytes.Equal(f["cut_id"].Bytes, make([]byte, 16)) {
				return sealedstate.ErrInvalid
			}
			want := owner
			if f["consumed"].U64 != 0 {
				want++
			}
			if ep["owner_gen"].U64 != want {
				return sealedstate.ErrInvalid
			}
			if ref := im.StateRef; ref != "" && ref != "flow-state."+hex.EncodeToString(f["cut_id"].Bytes) {
				return sealedstate.ErrInvalid
			}
			permits[epID] = true
		case sealedstate.KindReceipt, sealedstate.KindRetirement:
			if _, exists := endpoints[epID]; !exists {
				return sealedstate.ErrInvalid
			}
		// These kinds add no permit/receipt edge; the outer cut validates them.
		// origin: hot-update S5 plan §5.1 / §10.2 / T32
		case sealedstate.KindAuthority, sealedstate.KindBinding, sealedstate.KindEndpoint, sealedstate.KindFlow, sealedstate.KindMaterial, sealedstate.KindSaltRing, sealedstate.KindSSActor, sealedstate.KindSSServer, sealedstate.KindTimer:
		default:
		}
	}
	resources := make(map[string]handoff.Resource)
	for _, res := range cut.Resources {
		if !ClientImageName(res.ID, 256) || !ClientImageName(res.Kind, 64) || resources[res.ID].ID != "" || len(res.Data) != 0 {
			return sealedstate.ErrInvalid
		}
		resources[res.ID] = res
	}
	for _, ri := range im.Flows.Records {
		id := "flow." + hex.EncodeToString(ri.ID[:])
		names := make(map[string]bool)
		if ri.RawRef != "" {
			if ri.RawRef != id {
				return sealedstate.ErrInvalid
			}
			names["raw"] = true
		}
		if ri.ReplayRef != "" {
			if ri.ReplayRef != id {
				return sealedstate.ErrInvalid
			}
			names["replay"] = true
		}
		if len(names) == 0 {
			continue
		}
		res, exists := resources[id]
		if !exists || len(res.Files) != len(names) {
			return sealedstate.ErrInvalid
		}
		for name := range res.Files {
			if !names[name] {
				return sealedstate.ErrInvalid
			}
		}
		delete(resources, id)
	}
	if len(resources) != 0 {
		return sealedstate.ErrInvalid
	}
	return nil
}

func ClientImageName(s string, limit int) bool {
	if len(s) == 0 || len(s) > limit {
		return false
	}
	for _, c := range s {
		if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9') && c != '-' && c != '_' && c != '.' {
			return false
		}
	}
	return true
}

// Each encoded field is checked against the frozen descriptor table. Empty
// required buffers and false decisions are still present; omission must never
// reset a consumed permit, nonce, pending suffix or irreversible revoke.
// origin: hot-update S5 plan §12.1 / T32
func ValidateClientRecords(records []sealedstate.Record) error {
	seen := make(map[string]bool)
	for _, rec := range records {
		var descriptor *sealedstate.RecordDescriptor
		for i := range sealedstate.RecordDescriptors {
			if sealedstate.RecordDescriptors[i].Kind == rec.Kind {
				descriptor = &sealedstate.RecordDescriptors[i]
				break
			}
		}
		if descriptor == nil || len(rec.Fields) != len(descriptor.Fields) {
			return sealedstate.ErrInvalid
		}
		fields := make(map[string]bool)
		for _, f := range rec.Fields {
			var d *sealedstate.FieldDescriptor
			for i := range descriptor.Fields {
				if descriptor.Fields[i].Name == f.Name {
					d = &descriptor.Fields[i]
					break
				}
			}
			if d == nil || fields[f.Name] || f.Type != d.Type {
				return sealedstate.ErrInvalid
			}
			fields[f.Name] = true
			switch f.Type {
			case sealedstate.FieldU64, sealedstate.FieldNonnegativeI64, sealedstate.FieldBool:
				if len(f.Bytes) != 0 || (f.Type == sealedstate.FieldNonnegativeI64 && f.U64 > math.MaxInt64) || (f.Type == sealedstate.FieldBool && f.U64 > 1) {
					return sealedstate.ErrInvalid
				}
			case sealedstate.FieldBytes, sealedstate.FieldUTF8, sealedstate.FieldID16, sealedstate.FieldNonce12, sealedstate.FieldSalt32:
				if f.U64 != 0 || (d.Width != 0 && len(f.Bytes) != d.Width) || (d.MaxBytes != 0 && len(f.Bytes) > d.MaxBytes) || (f.Type == sealedstate.FieldUTF8 && !utf8.Valid(f.Bytes)) {
					return sealedstate.ErrInvalid
				}
			default:
				return sealedstate.ErrInvalid
			}
		}
		id := ClientFields(rec)["id"].Bytes
		key := rec.Kind + string(id)
		if len(id) != 16 || bytes.Equal(id, make([]byte, 16)) || seen[key] {
			return sealedstate.ErrInvalid
		}
		seen[key] = true
	}
	return nil
}

func ClientFileNames(res handoff.Resource) []string {
	names := make([]string, 0, len(res.Files))
	for name := range res.Files {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// The private envelope is binary: version, record count, length-prefixed kind,
// field count, then name/type/value; a resource directory follows the records.
// P9's codec is package-private to ssproxy; P7 uses the same frozen descriptors
// without creating a reverse dependency on the SS owner.
func EncodeClientCut(cut sealedstate.CutState) ([]byte, error) {
	if _, err := ClientImageFromCut(cut, ""); err != nil {
		return nil, err
	}
	return encodeCutState(cut, "tether-client-cut\x01")
}

func DecodeClientCut(data []byte) (sealedstate.CutState, error) {
	cut, err := decodeCutState(data, "tether-client-cut\x01", MaxClientCutRecords, flow.MaxFlows)
	if err != nil {
		return cut, err
	}
	if _, err := ClientImageFromCut(cut, ""); err != nil {
		return sealedstate.CutState{}, err
	}
	return cut, nil
}
