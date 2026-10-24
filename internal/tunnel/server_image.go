package tunnel

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/flow"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/listenerimage"
	"github.com/LinZiyang666/tether/internal/rawsocket"
	"github.com/LinZiyang666/tether/internal/sealedstate"
	"github.com/LinZiyang666/tether/internal/tunnel/flowstate"
	"github.com/LinZiyang666/tether/internal/workload"
)

// freezeServerRuntime is P06's joined runtime hook (server_pause.go); it never
// returns a successful cut lacking real worker receipts.
// origin: hot-update S6 plan §2.1 / §12.1 P07

type serverFrozenImage struct {
	mu       sync.Mutex
	cut      serverRuntimeCut
	state    *sealedstate.Snapshot
	released bool
}

// FreezeServer keeps carrier and raw lifetimes with P06. Only the private image
// and its replacement sealed FD belong to this adapter.
// origin: hot-update S6 plan §6.2 / T24
func (s *Server) FreezeServer(deadline controlbudget.Deadline) (ServerFrozen, error) {
	cut, err := freezeServerRuntime(s, deadline)
	if err != nil {
		return nil, err
	}
	if cut == nil {
		return nil, sealedstate.ErrInvalid
	}
	return &serverFrozenImage{cut: cut}, nil
}
func FreezeServer(s *Server, deadline controlbudget.Deadline) (ServerFrozen, error) {
	if s == nil {
		return nil, sealedstate.ErrInvalid
	}
	return s.FreezeServer(deadline)
}
func (f *serverFrozenImage) Export() (ServerImage, []handoff.Resource, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.released {
		return ServerImage{}, nil, sealedstate.ErrInvalid
	}
	state, err := f.cut.snapshot()
	if err != nil {
		return ServerImage{}, nil, err
	}
	flows, fs, err := f.cut.flowExport()
	if err != nil {
		return ServerImage{}, nil, err
	}
	if fs == nil {
		return ServerImage{}, nil, sealedstate.ErrInvalid
	}
	cut := sealedstate.CutState{Records: state.Records, Resources: state.Resources}
	if err = bindServerFlowImage(&cut, flows); err != nil {
		return ServerImage{}, nil, err
	}
	im, err := serverImageFromCut(cut, flows, "")
	if err != nil {
		return ServerImage{}, nil, err
	}
	data, err := encodeServerCut(cut)
	if err != nil {
		return ServerImage{}, nil, err
	}
	var id [16]byte
	if _, err = rand.Read(id[:]); err != nil {
		return ServerImage{}, nil, sealedstate.ErrInvalid
	}
	ref := "broker-tunnel-state." + hex.EncodeToString(id[:])
	next, err := sealedstate.New(ref, data)
	if err != nil {
		return ServerImage{}, nil, err
	}
	resources, err := handoff.MergeResources(state.Resources, fs.Resources, next.Resources)
	if err != nil {
		_ = next.Close()
		return ServerImage{}, nil, sealedstate.ErrInvalid
	}
	// Commit replacement only after closure validation. Borrowed business files
	// and P06's flow snapshot remain outside the cut-owned close list.
	old := f.state
	f.state = next
	if old != nil {
		_ = old.Close()
	}
	im.StateResource = ref
	for i := range im.Endpoints {
		im.Endpoints[i].StateRef = ref
	}
	return im, resources, nil
}
func (f *serverFrozenImage) Thaw(d controlbudget.Deadline) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.released {
		return sealedstate.ErrInvalid
	}
	if err := f.cut.thaw(d); err != nil {
		return err
	}
	f.released = true
	return f.state.Close()
}
func (f *serverFrozenImage) Discard() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.released {
		return sealedstate.ErrInvalid
	}
	f.released = true
	return errors.Join(f.cut.discard(), f.state.Close())
}

// snapshot is called by P06 only after accept/timer/carrier joins. Listener
// resources are keeper borrows, never captured by closing a net.Listener here.
// Fired/cleanup decisions and the original BOOTTIME deadline are projected
// here, so the runtime safe point and codec cannot choose different field sets.
// origin: hot-update S6 plan §2.2 / §6.2 / T24
func snapshot(s *Server, resources []handoff.Resource) (serverStateSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return snapshotServerLocked(s, resources)
}

func snapshotServerLocked(s *Server, resources []handoff.Resource) (serverStateSnapshot, error) {
	st := s.flowState
	if st == nil || s.closed {
		return serverStateSnapshot{}, sealedstate.ErrInvalid
	}
	var out serverStateSnapshot
	out.Resources = resources
	// Legacy sessions have no transferable carrier owner, even if idle.
	s.hsMu.Lock()
	handshakes := len(s.hsPerIP)
	s.hsMu.Unlock()
	if handshakes != 0 {
		return out, rawsocket.ErrBusy
	}
	if len(s.sessions) != 0 {
		return out, errors.New("tunnel: legacy session busy")
	}
	waiters := serverWaiterIDs(st.waiters)
	control := ""
	var controlDescriptor []byte
	for _, r := range resources {
		if r.ID == "tunnel-control" {
			control = r.ID
			controlDescriptor = r.Data
		}
	}
	limit := st.cfg.Budget.Limit
	if limit == 0 {
		limit = workload.DefaultReplayBudget
	}
	id := [16]byte{1}
	rec, err := serverRecord("server", map[string]sealedstate.Field{
		"id": endpointBytes(id[:]), "boot_id": endpointText(st.cfg.BootID), "instance_id": endpointText(st.cfg.InstanceID), "owner_gen": endpointNum(st.cfg.OwnerGen), "generation": endpointNum(1), "ticket_key": endpointBytes(st.ticketKey[:]), "replay_limit": endpointNum(uint64(limit)), "replay_used": endpointNum(uint64(st.cfg.Budget.Used())), "pending_slots": endpointNum(uint64(len(st.pending))), "waiters": endpointBytes(waiters), "control_descriptor": endpointBytes(controlDescriptor), "control_listener": endpointText(control), "flows_digest": endpointBytes(make([]byte, 32)), "sessions": endpointBytes(nil),
	})
	if err != nil {
		return out, err
	}
	out.Records = append(out.Records, rec)
	for _, e := range st.endpoints {
		records, err := snapshotServerEndpoint(s, e, resources)
		if err != nil {
			return out, err
		}
		out.Records = append(out.Records, records...)
	}
	rec, err = snapshotServerFences(s)
	if err != nil {
		return out, err
	}
	out.Records = append(out.Records, rec)
	sort.Slice(out.Records, func(i, j int) bool {
		a, b := out.Records[i], out.Records[j]
		if a.Kind != b.Kind {
			return a.Kind < b.Kind
		}
		return string(clientFields(a)["id"].Bytes) < string(clientFields(b)["id"].Bytes)
	})
	return out, nil
}

func snapshotServerEndpoint(s *Server, e *flowEndpoint, resources []handoff.Resource) ([]sealedstate.Record, error) {
	if e.attachment != nil || e.timer != nil {
		return nil, rawsocket.ErrBusy
	}
	var desc []byte
	listener := ""
	if e.listener != nil || e.keeper != nil {
		listener = endpointListenerRef(e.id)
		for _, res := range resources {
			if res.ID == listener && res.Kind == "listener" {
				desc = res.Data
			}
		}
		if len(desc) == 0 {
			return nil, sealedstate.ErrInvalid
		}
	}
	pending := ""
	var fp rawsocket.Fingerprint
	var tuple []byte
	if e.pendingRaw != nil {
		pending = endpointListenerRef(e.id)
		fp = e.pendingRaw.Fingerprint()
		_, _, t, _, err := rawsocket.ConnectReceipt(e.pendingRaw)
		if err != nil {
			return nil, err
		}
		tuple = serverTupleBytes(t)
	}
	deadline := e.detachBootNS
	var err error
	if deadline == 0 {
		deadline, err = clientDeadlineBoot(e.detachDeadline)
	}
	if err != nil {
		return nil, err
	}
	created := uint64(0)
	if !e.createdAt.IsZero() {
		created = uint64(e.createdAt.UnixNano())
	}
	var flowIDs []byte
	for _, state := range flow.States(s.flowState.manager, e.id) {
		flowIDs = append(flowIDs, state.ID[:]...)
	}
	r, err := serverRecord("endpoint", map[string]sealedstate.Field{
		"id": endpointBytes(e.id[:]), "allocation_sid": endpointText(e.allocation.SID), "allocation_nid": endpointText(e.allocation.NID), "allocation_port": endpointNum(uint64(e.allocation.Port)), "allocation_token_hash": endpointText(e.allocation.TokenHash), "allocation_epoch": endpointNum(uint64(e.allocation.Epoch)), "phase": endpointNum(uint64(e.phase)), "owner_gen": endpointNum(e.ownerGen), "carrier_gen": endpointNum(e.carrierGen), "target_port": endpointNum(uint64(e.targetPort)), "ticket": endpointBytes(e.ticket), "flow_ids": endpointBytes(flowIDs), "listener_ref": endpointText(listener), "listener_descriptor": endpointBytes(desc), "pending_ref": endpointText(pending), "pending_dev": endpointNum(fp.Dev), "pending_ino": endpointNum(fp.Ino), "pending_type": endpointNum(uint64(fp.Type)), "pending_tuple": endpointBytes(tuple), "deadline_boot_ns": endpointNum(deadline), "fired": endpointNum(boolNumber(e.fired)), "cleanup_pending": endpointNum(boolNumber(e.cleanupPending || e.phase >= EndpointPhaseClosing)), "fence_port": endpointNum(uint64(e.fence.port)), "fence_session": endpointNum(uint64(e.fence.sess)), "fence_allocation": endpointNum(uint64(e.fence.alloc)), "lineage": endpointBytes(e.lineage[:]), "created_unix_ns": endpointNum(created), "next_seq": endpointNum(e.nextSeq),
	})
	if err != nil {
		return nil, err
	}
	out := []sealedstate.Record{r}
	if p := e.permit; p != nil {
		deadline, err = clientDeadlineBoot(p.deadline)
		if err != nil {
			return nil, err
		}
		r, err = serverRecord("permit", map[string]sealedstate.Field{"id": endpointBytes(e.id[:]), "request_id": endpointBytes(p.requestID[:]), "cut_id": endpointBytes(p.cutID[:]), "owner_gen": endpointNum(p.owner), "next_owner_gen": endpointNum(p.next), "deadline_boot_ns": endpointNum(deadline), "image_digest": endpointBytes(p.digest[:]), "consumed": endpointNum(boolNumber(p.consumed)), "abort": endpointNum(boolNumber(p.abort))})
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	if receipt := e.receipt; receipt != nil {
		r, err = serverRecord("receipt", map[string]sealedstate.Field{"id": endpointBytes(e.id[:]), "request": endpointBytes(encodeResumeRequest(receipt.request)), "response": endpointBytes(encodeResumeResponse(receipt.response))})
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, nil
}

func serverImageFromCut(cut sealedstate.CutState, flows flow.Image, ref string) (ServerImage, error) {
	im := ServerImage{Schema: "broker-tunnel-state-v1", SchemaVersion: 1, StateResource: ref, Flows: flows}
	if err := validateServerCut(cut); err != nil {
		return im, fmt.Errorf("tunnel: server cut validation: %w", err)
	}
	for _, rec := range cut.Records {
		v := clientFields(rec)
		switch rec.Kind {
		case "server":
			public, err := json.Marshal(flows)
			if err != nil {
				return im, sealedstate.ErrInvalid
			}
			digest := sha256.Sum256(public)
			if !bytes.Equal(v["flows_digest"].Bytes, digest[:]) {
				return im, sealedstate.ErrInvalid
			}
			im.Generation = v["generation"].U64
			im.IssuerBootID = string(v["boot_id"].Bytes)
			im.InstanceID = string(v["instance_id"].Bytes)
			im.OwnerGeneration = v["owner_gen"].U64
			if len(v["control_descriptor"].Bytes) != 0 {
				var d listenerimage.Descriptor
				if json.Unmarshal(v["control_descriptor"].Bytes, &d) != nil || d.ID != string(v["control_listener"].Bytes) {
					return im, sealedstate.ErrInvalid
				}
				im.Listeners = append(im.Listeners, d)
			}
			im.ReplayLimit = int64(v["replay_limit"].U64)
			im.ReplayUsed = int64(v["replay_used"].U64)
		case "endpoint":
			e := EndpointImage{ID: flow.EndpointID(v["id"].Bytes), OwnerGen: v["owner_gen"].U64, CarrierGen: v["carrier_gen"].U64, StateRef: ref}
			for ids := v["flow_ids"].Bytes; len(ids) > 0; ids = ids[16:] {
				e.FlowIDs = append(e.FlowIDs, flow.ID(ids[:16]))
			}
			im.Endpoints = append(im.Endpoints, e)
			if len(v["listener_descriptor"].Bytes) != 0 {
				var d listenerimage.Descriptor
				if err := json.Unmarshal(v["listener_descriptor"].Bytes, &d); err != nil {
					return im, sealedstate.ErrInvalid
				}
				im.Listeners = append(im.Listeners, d)
			}
		}
	}
	// Flow endpoint membership is checked from the sealed flow codec during Import;
	// public retirement entries also bind empty endpoints to their original owner.
	if im.ReplayUsed < flows.ReservedBytes || im.ReplayUsed > im.ReplayLimit || im.ReplayLimit <= 0 || im.Generation == 0 || im.OwnerGeneration == 0 || im.IssuerBootID == "" || im.InstanceID == "" {
		return im, sealedstate.ErrInvalid
	}
	if err := validateServerFlowClosure(im); err != nil {
		return im, fmt.Errorf("tunnel: server flow membership: %w", err)
	}
	sort.Slice(im.Endpoints, func(i, j int) bool { return string(im.Endpoints[i].ID[:]) < string(im.Endpoints[j].ID[:]) })
	sort.Slice(im.Listeners, func(i, j int) bool { return im.Listeners[i].ID < im.Listeners[j].ID })
	return im, nil
}

// Bind the exact S5 public projection inside the sealed broker record. Swapping
// another cut's flow state or editing an outer offset cannot borrow its authority.
// origin: hot-update S6 plan §2.2 / T24
func bindServerFlowImage(cut *sealedstate.CutState, im flow.Image) error {
	data, err := json.Marshal(im)
	if err != nil {
		return sealedstate.ErrInvalid
	}
	digest := sha256.Sum256(data)
	for i := range cut.Records {
		if cut.Records[i].Kind == "server" {
			v := clientFields(cut.Records[i])
			v["flows_digest"] = endpointBytes(digest[:])
			cut.Records[i], err = serverRecord("server", v)
			return err
		}
	}
	return sealedstate.ErrInvalid
}

// A retained S5 journal entry must belong to one preserved Endpoint. Terminal
// metadata remains owned by S5, while every live entry appears in this closure.
// origin: hot-update S6 plan §2.2 / T24
func validateServerFlowClosure(im ServerImage) error {
	endpoints := map[flow.EndpointID]bool{}
	listed := map[flow.ID]bool{}
	flows := map[flow.ID]bool{}
	for _, r := range im.Flows.Records {
		flows[r.ID] = r.Phase != flow.FlowPhaseTerminal
	}
	for _, e := range im.Endpoints {
		endpoints[e.ID] = true
		for _, id := range e.FlowIDs {
			if listed[id] || !flows[id] {
				return sealedstate.ErrInvalid
			}
			listed[id] = true
		}
	}
	for id, live := range flows {
		if live && !listed[id] {
			return sealedstate.ErrInvalid
		}
	}
	for _, r := range im.Flows.Retirement {
		if !endpoints[r.EndpointID] {
			return sealedstate.ErrInvalid
		}
	}
	return nil
}

// Pure value operations stay below the runtime owner.
// origin: hot-update S6 plan §9.3 / TUNNEL-SLIM
func serverStateRef(ref string) bool                { return flowstate.ServerStateRef(ref) }
func endpointListenerRef(id flow.EndpointID) string { return flowstate.EndpointListenerRef(id) }

func validateServerCut(cut sealedstate.CutState) error {
	return flowstate.ValidateServerCut(cut, func(cut sealedstate.CutState) error {
		// Structural validity alone cannot accept a corrupt cached response or map.
		// Decode the typed runtime projection before any resource can be taken; the
		// temporary owner has no manager, listener, timer or worker side effects.
		// origin: hot-update S6 plan §2.2 / T24
		temporary := newServer("", "", nil, nil, nil)
		temporary.flowState = &serverFlowState{endpoints: map[int]*flowEndpoint{}, pending: make(chan struct{}, flow.MaxPendingFDs)}
		return restoreServerRecords(temporary, cut)

	})
}
