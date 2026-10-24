package tunnel

import (
	"math"
	"strconv"

	"github.com/LinZiyang666/tether/internal/rawsocket"
	"github.com/LinZiyang666/tether/internal/sealedstate"
	"github.com/LinZiyang666/tether/internal/tunnel/flowstate"
)

// Each fence keeps its identity-qualified map entry, including tombstones
// without a live Endpoint. Closing references survive until real cleanup joins.
// origin: hot-update S6 plan §2.2 / §6.2 / T24
func snapshotServerFences(s *Server) (sealedstate.Record, error) {
	maps := map[string]map[string]uint64{}
	for _, name := range []string{"kill_gen_port", "kill_gen_session", "kill_gen_allocation", "forgotten", "closed_allocation", "inflight_by_sid", "inflight_by_allocation"} {
		maps[name] = map[string]uint64{}
	}
	for k, v := range s.killGen {
		maps["kill_gen_port"][strconv.Itoa(k)] = uint64(v)
	}
	for k, v := range s.killGenSession {
		maps["kill_gen_session"][k] = uint64(v)
	}
	for k, v := range s.killGenAllocation {
		maps["kill_gen_allocation"][serverAllocationKey(k)] = uint64(v)
	}
	for k, v := range s.forgotten {
		maps["forgotten"][k] = boolNumber(v)
	}
	for k, v := range s.closedAllocation {
		maps["closed_allocation"][serverAllocationKey(k)] = boolNumber(v)
	}
	for k, v := range s.inflightBySID {
		maps["inflight_by_sid"][k] = uint64(v)
	}
	for k, v := range s.inflightByAllocation {
		maps["inflight_by_allocation"][serverAllocationKey(k)] = uint64(v)
	}
	id := [16]byte{1}
	values := map[string]sealedstate.Field{"id": endpointBytes(id[:])}
	for name, m := range maps {
		values[name] = endpointBytes(serverMapBytes(m))
	}
	return serverRecord("fences", values)
}
func restoreServerFences(s *Server, rec sealedstate.Record) error {
	for name, f := range clientFields(rec) {
		if name == "id" {
			continue
		}
		values, err := serverMapRead(f.Bytes)
		if err != nil {
			return err
		}
		for key, v := range values {
			switch name {
			case "kill_gen_port":
				port, err := strconv.Atoi(key)
				if err != nil || port <= 0 || port > math.MaxUint16 || strconv.Itoa(port) != key {
					return sealedstate.ErrInvalid
				}
				s.killGen[port] = int64(v)
			case "kill_gen_session":
				s.killGenSession[key] = int64(v)
			case "forgotten":
				if v > 1 {
					return sealedstate.ErrInvalid
				}
				s.forgotten[key] = v == 1
			case "inflight_by_sid":
				s.inflightBySID[key] = int(v)
			case "kill_gen_allocation", "closed_allocation", "inflight_by_allocation":
				k, err := serverAllocationRead(key)
				if err != nil {
					return err
				}
				switch name {
				case "kill_gen_allocation":
					s.killGenAllocation[k] = int64(v)
				case "closed_allocation":
					if v > 1 {
						return sealedstate.ErrInvalid
					}
					s.closedAllocation[k] = v == 1
				case "inflight_by_allocation":
					s.inflightByAllocation[k] = int(v)
				}
			default:
				return sealedstate.ErrInvalid
			}
		}
	}
	return nil
}

// The runtime retains its API while the leaf owns this implementation.
// origin: hot-update S6 plan §9.3 / TUNNEL-SLIM
type PrivateFieldDescriptor = flowstate.PrivateFieldDescriptor
type PrivateRecordDescriptor = flowstate.PrivateRecordDescriptor

var serverPrivateDescriptors = flowstate.ServerPrivateDescriptors

func serverRecord(kind string, values map[string]sealedstate.Field) (sealedstate.Record, error) {
	return flowstate.ServerRecord(kind, values)
}

func encodeServerCut(cut sealedstate.CutState) ([]byte, error) {
	return flowstate.EncodeServerCut(cut, validateServerCut)
}
func decodeServerCut(data []byte) (sealedstate.CutState, error) {
	return flowstate.DecodeServerCut(data, validateServerCut)
}

func serverMapBytes(values map[string]uint64) []byte       { return flowstate.ServerMapBytes(values) }
func serverMapRead(data []byte) (map[string]uint64, error) { return flowstate.ServerMapRead(data) }
func serverTupleBytes(t rawsocket.Tuple) []byte            { return flowstate.ServerTupleBytes(t) }
func serverTupleRead(p []byte) (rawsocket.Tuple, error)    { return flowstate.ServerTupleRead(p) }

// Keep the runtime fence key private while the leaf owns its canonical bytes.
// origin: hot-update S6 plan §2.2 / §9.3 / TUNNEL-SLIM
func serverAllocationKey(k sessionFenceKey) string {
	return flowstate.ServerAllocationKey(Allocation{Port: k.publicPort, SID: k.sid, NID: k.nid, TokenHash: k.tokenHash})
}
func serverAllocationRead(key string) (sessionFenceKey, error) {
	k, err := flowstate.ServerAllocationRead(key)
	return sessionFenceKey{publicPort: k.Port, sid: k.SID, nid: k.NID, tokenHash: k.TokenHash}, err
}
