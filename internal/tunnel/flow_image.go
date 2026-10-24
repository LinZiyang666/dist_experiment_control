package tunnel

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"reflect"
	"sync"
	"time"

	"github.com/LinZiyang666/tether/internal/flow"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/rawsocket"
	"github.com/LinZiyang666/tether/internal/sealedstate"
	"github.com/LinZiyang666/tether/internal/tunnel/flowstate"
	"github.com/LinZiyang666/tether/internal/workload"
)

// A cut owns its sealed descriptor; the runtime hooks own business descriptors.
// Keeping this side table here avoids depending on P1's private runtime layout.
// P1 calls closeClientImageSnapshot on both Thaw and Discard, after fencing Export.
// origin: hot-update S5 plan §5.3 / §12.1
var clientImageSnapshots = struct {
	sync.Mutex
	cuts map[*ClientFrozen]*sealedstate.Snapshot
}{cuts: make(map[*ClientFrozen]*sealedstate.Snapshot)}

func closeClientImageSnapshot(f *ClientFrozen) error {
	clientImageSnapshots.Lock()
	s := clientImageSnapshots.cuts[f]
	delete(clientImageSnapshots.cuts, f)
	clientImageSnapshots.Unlock()
	if s == nil {
		return nil
	}
	return s.Close()
}

// Export takes no network action. In particular it preserves a consumed permit
// and cached receipt rather than manufacturing a second activation after a lost reply.
// origin: hot-update S5 plan §4.4 / §5.3 / T32
func (f *ClientFrozen) Export() (ClientImage, *sealedstate.Snapshot, error) {
	if f == nil || f.state == nil {
		return ClientImage{}, nil, sealedstate.ErrInvalid
	}
	// Private state and its sealed descriptor form one transaction with Thaw
	// and Discard. A failed seal must leave the previous successful cut intact.
	// origin: hot-update S5 plan §5.3 / T32
	f.state.mu.Lock()
	defer f.state.mu.Unlock()
	cut, err := snapshotClientCutLocked(f.state)
	if err != nil {
		return ClientImage{}, nil, err
	}
	im, err := clientImageFromCut(cut, "")
	if err != nil {
		return ClientImage{}, nil, err
	}
	var cutID [16]byte
	for _, rec := range cut.Records {
		if rec.Kind == sealedstate.KindPermit {
			id := clientFields(rec)["cut_id"].Bytes
			if cutID != ([16]byte{}) && !bytes.Equal(cutID[:], id) {
				return ClientImage{}, nil, sealedstate.ErrInvalid
			}
			copy(cutID[:], id)
		}
	}
	if cutID == ([16]byte{}) {
		if _, err := rand.Read(cutID[:]); err != nil {
			return ClientImage{}, nil, errors.New("tunnel: cut identity unavailable")
		}
	}
	ref := "flow-state." + hex.EncodeToString(cutID[:])
	setClientStateRef(&im, ref)
	for _, res := range cut.Resources {
		for _, file := range res.Files {
			if file == nil {
				return ClientImage{}, nil, sealedstate.ErrInvalid
			}
		}
	}
	data, err := encodeClientCut(cut)
	if err != nil {
		return ClientImage{}, nil, err
	}
	snapshot, err := sealedstate.New(ref, data)
	if err != nil {
		return ClientImage{}, nil, err
	}
	// Merge before replacing the last good cut. A failed re-export must retain
	// that cut, and must never close any borrowed raw/replay original.
	snapshot.Resources, err = handoff.MergeResources(cut.Resources, snapshot.Resources)
	if err != nil {
		_ = snapshot.Close()
		return ClientImage{}, nil, sealedstate.ErrInvalid
	}
	clientImageSnapshots.Lock()
	old := clientImageSnapshots.cuts[f]
	clientImageSnapshots.cuts[f] = snapshot
	clientImageSnapshots.Unlock()
	if old != nil {
		_ = old.Close()
	}
	return im, snapshot, nil
}

// RestoreClient validates sealed records and the public projection before taking
// any business file. It never activates pumps, consumes a permit or dials a target.
// All transferred descriptors remain available for redispatch on refusal.
// origin: hot-update S5 plan §5.1 / §5.4 / §12.1 / T32
func RestoreClient(cfg FlowConfig, im ClientImage, src rawsocket.FileSource) (*Client, []handoff.Resource, error) {
	if src == nil || im.Version != flow.Version || !clientStateRef(im.StateRef) {
		return nil, nil, sealedstate.ErrInvalid
	}
	data, stateFile, err := sealedstate.Read(src, im.StateRef, "state", sealedstate.MaxStateBytes)
	var salvage []handoff.Resource
	if stateFile != nil {
		salvage = append(salvage, handoff.Resource{ID: im.StateRef, Kind: "sealed-state-v1", Files: map[string]*os.File{"state": stateFile}})
	}
	if err != nil {
		return nil, salvage, err
	}
	cut, err := decodeClientCut(data)
	if err != nil {
		return nil, salvage, err
	}
	want, err := clientImageFromCut(cut, im.StateRef)
	if err != nil || !reflect.DeepEqual(im, want) {
		return nil, salvage, sealedstate.ErrInvalid
	}
	taken, err := takeClientFiles(&cut, src)
	salvage = append(salvage, taken...)
	if err != nil {
		return nil, salvage, err
	}
	client, _, err := restoreCutState(cfg, cut)
	if err != nil {
		// The hook leaves failed imports frozen and must relinquish wrappers;
		// P7 retains the complete Take ledger, including prior successful actors.
		return nil, salvage, err
	}
	if client == nil {
		return nil, salvage, sealedstate.ErrInvalid
	}
	if err := flow.RetainImportMetadata(client.flowState.manager, salvage[0]); err != nil {
		_, _ = client.flowState.manager.Detach()
		return nil, salvage, err
	}
	return client, nil, nil
}

// The directory is sealed with the state, so missing files cannot silently
// become empty owners. Take once even if a FileSource would allow a second take.
// origin: hot-update S5 plan §5.3 / T32
func takeClientFiles(cut *sealedstate.CutState, src rawsocket.FileSource) ([]handoff.Resource, error) {
	var salvage []handoff.Resource
	for i := range cut.Resources {
		res := &cut.Resources[i]
		for _, name := range clientFileNames(*res) {
			file, err := src.TakeFile(res.ID, name)
			if file != nil {
				res.Files[name] = file
				salvage = appendClientSalvage(salvage, *res, name, file)
			}
			if err != nil || file == nil {
				return salvage, sealedstate.ErrInvalid
			}
		}
	}
	return salvage, nil
}

func appendClientSalvage(out []handoff.Resource, res handoff.Resource, name string, file *os.File) []handoff.Resource {
	for i := range out {
		if out[i].ID == res.ID {
			out[i].Files[name] = file
			return out
		}
	}
	return append(out, handoff.Resource{ID: res.ID, Kind: res.Kind, Data: res.Data, Files: map[string]*os.File{name: file}})
}

// Detach is restricted to imports that have never activated or changed state.
// Manager owns the full original business/sealed closure and its reservations;
// transferring that closure keeps the original bundle safe for redispatch.
// origin: hot-update S5 internal review rv-image-1
func (c *Client) Detach() ([]handoff.Resource, error) {
	if c == nil || c.flowState == nil {
		return nil, errFlowState
	}
	st := c.flowState
	st.mu.Lock()
	defer st.mu.Unlock()
	if !st.frozen || st.admission != 0 {
		return nil, errFlowState
	}
	for _, endpoint := range st.endpoints {
		if endpoint.attachment != nil || endpoint.supervisorDone != nil {
			return nil, errFlowState
		}
	}
	resources, err := st.manager.Detach()
	if err != nil {
		return nil, err
	}
	clear(st.endpoints)
	st.generation++
	return resources, nil
}

// The runtime retains its API while the leaf owns this implementation.
// origin: hot-update S6 plan §9.3 / TUNNEL-SLIM
// Keep the frozen public type identity at the tunnel boundary. Leaf images
// have identical fields; only their defining package differs.
// origin: hot-update S6 plan §2.1 / §9.3 / TUNNEL-SLIM
func setClientStateRef(im *ClientImage, ref string) {
	im.StateRef, im.Flows.StateRef = ref, ref
	for i := range im.Endpoints {
		im.Endpoints[i].StateRef = ref
	}
}

func clientStateRef(ref string) bool { return flowstate.ClientStateRef(ref) }
func clientFields(rec sealedstate.Record) map[string]sealedstate.Field {
	return flowstate.ClientFields(rec)
}

func clientImageFromCut(cut sealedstate.CutState, ref string) (ClientImage, error) {
	im, err := flowstate.ClientImageFromCut(cut, ref)
	out := ClientImage{Version: im.Version, Flows: im.Flows, StateRef: im.StateRef}
	if im.Endpoints != nil {
		out.Endpoints = make([]EndpointImage, len(im.Endpoints))
		for i, ep := range im.Endpoints {
			out.Endpoints[i] = EndpointImage(ep)
		}
	}
	return out, err
}

func clientImageName(s string, limit int) bool { return flowstate.ClientImageName(s, limit) }
func validateClientRecords(records []sealedstate.Record) error {
	return flowstate.ValidateClientRecords(records)
}
func clientFileNames(res handoff.Resource) []string            { return flowstate.ClientFileNames(res) }
func encodeClientCut(cut sealedstate.CutState) ([]byte, error) { return flowstate.EncodeClientCut(cut) }
func decodeClientCut(data []byte) (sealedstate.CutState, error) {
	return flowstate.DecodeClientCut(data)
}

// origin: hot-update S8 plan §4.2 / S8 internal review TAGGED-SUITE-SWEEP-A F2
// CensusClient names every retained identity without PREPARE, detaching a
// carrier, exporting secrets or allocating descriptors. Unknown/legacy owners
// refuse even when they claim no installed sessions.
func CensusClient(c *Client, deadline time.Time) (ClientImage, error) {
	if c == nil || !flowLockBefore(&c.mu, deadline) {
		return ClientImage{}, errFlowState
	}
	st := c.flowState
	sessionPorts := make([]int, 0, len(c.sessions))
	for port := range c.sessions {
		sessionPorts = append(sessionPorts, port)
	}
	c.mu.Unlock()
	if st == nil || !flowLockBefore(&st.mu, deadline) {
		return ClientImage{}, errFlowState
	}
	defer st.mu.Unlock()
	if st.admission != 0 || len(st.allocationOps) != 0 {
		return ClientImage{}, errFlowState
	}
	states, retained, err := flow.Census(st.manager, deadline)
	if err != nil {
		return ClientImage{}, err
	}
	im := ClientImage{Version: flow.Version, Flows: flow.Image{Version: flow.Version, Retirement: retained}}
	for _, e := range st.endpoints {
		ep := EndpointImage{ID: e.id, OwnerGen: e.owner, CarrierGen: e.carrier}
		for _, row := range states {
			if row.EndpointID == e.id {
				ep.FlowIDs = append(ep.FlowIDs, row.ID)
			}
		}
		im.Endpoints = append(im.Endpoints, ep)
	}
	// Every legacy session must be backed by a typed endpoint, just as the
	// ordinary FlowCensus verifies; a foreign legacy slot is never an empty row.
	for _, port := range sessionPorts {
		if st.endpoints[port] == nil {
			return ClientImage{}, errFlowState
		}
	}
	for _, row := range states {
		im.Flows.Records = append(im.Flows.Records, flow.RecordImage{ID: row.ID, OpenSeq: row.OpenSeq,
			Intent: row.Intent, Phase: row.Phase})
		if row.Phase != flow.FlowPhaseTerminal {
			im.Flows.ReservedBytes += workload.DefaultReplayCapacity
		}
	}
	return im, nil
}
