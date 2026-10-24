package flowstate

import (
	"encoding/hex"
	"encoding/json"
	"math"

	"github.com/LinZiyang666/tether/internal/flow"
	"github.com/LinZiyang666/tether/internal/listenerimage"
	"github.com/LinZiyang666/tether/internal/sealedstate"
)

func ServerStateRef(ref string) bool {
	const prefix = "broker-tunnel-state."
	if len(ref) != len(prefix)+32 || ref[:len(prefix)] != prefix {
		return false
	}
	id, err := hex.DecodeString(ref[len(prefix):])
	return err == nil && hex.EncodeToString(id) == ref[len(prefix):] && string(id) != string(make([]byte, 16))
}

func EndpointListenerRef(id flow.EndpointID) string {
	return "flow-endpoint." + hex.EncodeToString(id[:])
}

func ValidateServerCut(cut sealedstate.CutState, restoreServerRecords func(sealedstate.CutState) error) error {
	if err := ValidateServerRecords(cut.Records); err != nil {
		return err
	}
	expected := map[string]map[string]bool{}
	endpoints := map[string]bool{}
	ports := map[uint64]bool{}
	for _, r := range cut.Records {
		v := ClientFields(r)
		if r.Kind == "server" {
			if len(v["ticket_key"].Bytes) != 32 || len(v["sessions"].Bytes) != 0 || v["pending_slots"].U64 > flow.MaxPendingFDs || len(v["waiters"].Bytes)%16 != 0 {
				return sealedstate.ErrInvalid
			}
			if ref := string(v["control_listener"].Bytes); ref != "" {
				expected[ref] = map[string]bool{"listener": true}
			}
		}
		if r.Kind != "endpoint" {
			continue
		}
		id := string(v["id"].Bytes)
		endpoints[id] = true
		if v["allocation_port"].U64 == 0 || v["allocation_port"].U64 > math.MaxUint16 || ports[v["allocation_port"].U64] || v["phase"].U64 < 1 || v["phase"].U64 > 6 || v["next_seq"].U64 == 0 || v["target_port"].U64 > math.MaxUint16 || len(v["lineage"].Bytes) != 32 {
			return sealedstate.ErrInvalid
		}
		ports[v["allocation_port"].U64] = true
		if (len(v["listener_descriptor"].Bytes) == 0) != (len(v["listener_ref"].Bytes) == 0) {
			return sealedstate.ErrInvalid
		}
		if len(v["listener_descriptor"].Bytes) != 0 {
			var d listenerimage.Descriptor
			if json.Unmarshal(v["listener_descriptor"].Bytes, &d) != nil || d.ID != string(v["listener_ref"].Bytes) {
				return sealedstate.ErrInvalid
			}
		}
		if len(v["pending_ref"].Bytes) != 0 {
			if _, err := ServerTupleRead(v["pending_tuple"].Bytes); err != nil {
				return err
			}
			if v["pending_dev"].U64 == 0 || v["pending_ino"].U64 == 0 || v["pending_type"].U64 > math.MaxUint32 {
				return sealedstate.ErrInvalid
			}
		}
		if len(v["flow_ids"].Bytes)%16 != 0 {
			return sealedstate.ErrInvalid
		}
		for _, pair := range []struct{ field, name string }{{"listener_ref", "listener"}, {"pending_ref", "raw"}} {
			ref := string(v[pair.field].Bytes)
			if ref == "" {
				continue
			}
			if ref != EndpointListenerRef(flow.EndpointID(v["id"].Bytes)) {
				return sealedstate.ErrInvalid
			}
			if expected[ref] == nil {
				expected[ref] = map[string]bool{}
			}
			expected[ref][pair.name] = true
		}
	}
	for _, r := range cut.Records {
		v := ClientFields(r)
		if r.Kind == "permit" || r.Kind == "receipt" {
			if !endpoints[string(v["id"].Bytes)] {
				return sealedstate.ErrInvalid
			}
		}
		if r.Kind == "permit" && (v["owner_gen"].U64 == 0 || v["owner_gen"].U64 == math.MaxUint64 || v["next_owner_gen"].U64 != v["owner_gen"].U64+1 || len(v["image_digest"].Bytes) != 32) {
			return sealedstate.ErrInvalid
		}
	}
	if len(cut.Resources) != len(expected) {
		return sealedstate.ErrInvalid
	}
	seen := map[string]bool{}
	for _, res := range cut.Resources {
		names := expected[res.ID]
		if names == nil || seen[res.ID] || len(res.Files) != len(names) || (res.Kind != "listener" && res.Kind != "raw-socket-v1") {
			return sealedstate.ErrInvalid
		}
		seen[res.ID] = true
		for name := range res.Files {
			if !names[name] {
				return sealedstate.ErrInvalid
			}
		}
	}
	return restoreServerRecords(cut)
}
