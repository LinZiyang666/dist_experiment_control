package tunnel

import (
	"bytes"
	"encoding/json"
	"errors"
	"math"
	"os"
	"reflect"
	"sync"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/flow"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/listenerimage"
	"github.com/LinZiyang666/tether/internal/rawsocket"
	"github.com/LinZiyang666/tether/internal/sealedstate"
	"github.com/LinZiyang666/tether/internal/workload"
)

// Imported owners use the same resumed Accept/FIFO/timer path as a failed
// current cut. Mark ACTIVE before any pump/view can advance the inherited state.
// origin: hot-update S6 plan §6.3; internal review rv-fd-3
func activateServerImage(i *serverImportedImage, d controlbudget.Deadline) error {
	cut := &serverPausedCut{server: i.server, complete: true, dormant: i}
	return cut.thaw(d)
}

type serverImportedImage struct {
	mu               sync.Mutex
	server           *Server
	image            ServerImage
	stateFile        *os.File
	state            serverStateSnapshot
	keepers          map[string]listenerimage.Keeper
	resources        []handoff.Resource
	active, detached bool
}

func (i *serverImportedImage) Server() *Server { return i.server }
func (i *serverImportedImage) Activate(d controlbudget.Deadline) error {
	i.mu.Lock()
	if i.active || i.detached {
		i.mu.Unlock()
		return sealedstate.ErrInvalid
	}
	now, err := rawsocket.BootNow()
	if err != nil {
		i.mu.Unlock()
		return err
	}
	if int64(d) <= now {
		i.mu.Unlock()
		return os.ErrDeadlineExceeded
	}
	i.active = true
	i.mu.Unlock()
	return activateServerImage(i, d)
}

// The single-string source uses ID/purpose, matching brokerimage and Restored.
// Resource IDs contain dots; the slash separates the file purpose unambiguously.
// Every attempted Take, including a nonnil+error result, enters the salvage
// ledger before its error is observed. Never close a failed pristine import.
// origin: hot-update S6 plan §4.2 / T04 / T56
type serverImageSource struct {
	source   FileSource
	taken    []handoff.Resource
	seen     map[string]bool
	metadata map[string]handoff.Resource
}

func (s *serverImageSource) TakeFile(id, name string) (*os.File, error) {
	key := id + "/" + name
	if s.seen[key] {
		return nil, sealedstate.ErrInvalid
	}
	res, ok := s.metadata[id]
	if !ok {
		return nil, sealedstate.ErrInvalid
	}
	s.seen[key] = true
	f, err := s.source.TakeFile(key)
	if f != nil {
		s.taken = appendClientSalvage(s.taken, res, name, f)
	}
	return f, err
}

func ImportServer(cfg ServerImportConfig, im ServerImage, source FileSource) (ServerImported, Salvage, error) {
	if source == nil || im.Schema != "broker-tunnel-state-v1" || im.SchemaVersion != 1 || !serverStateRef(im.StateResource) {
		return nil, Salvage{}, sealedstate.ErrInvalid
	}
	if err := validateFlowConfig(cfg.Flow, true); err != nil {
		return nil, Salvage{}, err
	}
	src := &serverImageSource{source: source, seen: map[string]bool{}, metadata: map[string]handoff.Resource{
		im.StateResource:  {ID: im.StateResource, Kind: "sealed-state-v1"},
		im.Flows.StateRef: {ID: im.Flows.StateRef, Kind: "sealed-state-v1"},
	}}
	for _, r := range im.Flows.Records {
		for _, ref := range []string{r.RawRef, r.ReplayRef} {
			if ref != "" {
				src.metadata[ref] = handoff.Resource{ID: ref, Kind: "flow-v1"}
			}
		}
	}
	data, stateFile, err := sealedstate.Read(src, im.StateResource, "state", sealedstate.MaxStateBytes)
	salvage := func() Salvage { return Salvage{Resources: src.taken} }
	if err != nil {
		return nil, salvage(), err
	}
	cut, err := decodeServerCut(data)
	if err != nil {
		return nil, salvage(), err
	}
	// Populate metadata before taking any business file, including nonnil+error
	// refusals. Decoded rows retain kind/names; listener Data is the exact public
	// descriptor emitted by the keeper, never a private socket tuple.
	// origin: hot-update S6 plan §4.2; internal review rv-fd-5
	for _, res := range cut.Resources {
		src.metadata[res.ID] = handoff.Resource{ID: res.ID, Kind: res.Kind}
	}
	for _, descriptor := range im.Listeners {
		res := src.metadata[descriptor.ID]
		res.Data, err = json.Marshal(descriptor)
		if err != nil {
			return nil, salvage(), sealedstate.ErrInvalid
		}
		src.metadata[descriptor.ID] = res
	}
	want, err := serverImageFromCut(cut, im.Flows, im.StateResource)
	if err != nil || !reflect.DeepEqual(want, im) {
		return nil, salvage(), sealedstate.ErrInvalid
	}
	limit := cfg.Flow.Budget.Limit
	if limit == 0 {
		limit = workload.DefaultReplayBudget
	}
	if cfg.Flow.BootID != im.IssuerBootID || cfg.Flow.InstanceID != im.InstanceID || cfg.Flow.OwnerGen != im.OwnerGeneration || limit != im.ReplayLimit || im.Flows.ReservedBytes > limit-cfg.Flow.Budget.Used() {
		return nil, salvage(), sealedstate.ErrInvalid
	}
	// Construction deliberately bypasses SetFlowConfig's cold random ticket key
	// and manager factory. All flow logs reserve against this one caller budget.
	s := newServer("", cfg.PublicHost, cfg.Lookup, cfg.Certificate, cfg.Logger)
	s.flowState = &serverFlowState{cfg: cfg.Flow, endpoints: map[int]*flowEndpoint{}, pending: make(chan struct{}, flow.MaxPendingFDs)}
	if err = restoreServerRecords(s, cut); err != nil {
		return nil, salvage(), err
	}
	imported := &serverImportedImage{server: s, image: im, stateFile: stateFile, state: serverStateSnapshot{Records: cut.Records, Resources: cut.Resources}, keepers: map[string]listenerimage.Keeper{}}
	// Flow Restore owns its own partial-wrapper salvage transaction. Adapters
	// record all files, even when its error suffix is incomplete.
	manager, _, err := flow.Restore(flow.Config{Budget: cfg.Flow.Budget}, im.Flows, src)
	if err != nil {
		return nil, salvage(), err
	}
	// The shared S5 decoder defaults to the agent half. Broker originals send in
	// the opposite direction; retaining the default closes every resumed carrier.
	// origin: hot-update S6 plan §6.3 / T24
	if err = flow.RestoreDirection(manager, 0); err != nil {
		_, detachErr := manager.Detach()
		return nil, salvage(), errors.Join(err, detachErr)
	}

	s.flowState.manager = manager
	rollback := func(err error) error {
		_, detachErr := manager.Detach()
		for _, k := range imported.keepers {
			_, e := k.Detach()
			detachErr = errors.Join(detachErr, e)
		}
		return errors.Join(err, detachErr)
	}
	// Validate S5 flow membership against the imported endpoint owner; a daemon
	// cut cannot increment the agent's owner generation or manufacture a permit.
	for _, d := range im.Listeners {
		f, err := src.TakeFile(d.ID, "listener")
		if err != nil || f == nil {
			return nil, salvage(), rollback(sealedstate.ErrInvalid)
		}
		k, err := listenerimage.Adopt(d, f)
		if err != nil {
			return nil, salvage(), rollback(err)
		}
		imported.keepers[d.ID] = k
		// Terminal stop and the next cut read the Server's keeper fields.
		// Dormant View placeholders cannot serve as either terminal owners or
		// Accept views; thaw creates the tracked native views at activation.
		// origin: hot-update S6 plan §6.1 / §6.3; internal review rv-fd-3
		matched := d.ID == "tunnel-control"
		if matched {
			s.controlKeeper = k
		}
		for _, e := range s.flowState.endpoints {
			if endpointListenerRef(e.id) == d.ID {
				e.keeper = k
				matched = true
			}
		}
		if !matched {
			return nil, salvage(), rollback(sealedstate.ErrInvalid)
		}
	}
	for _, r := range cut.Records {
		if r.Kind != "endpoint" {
			continue
		}
		v := clientFields(r)
		ref := string(v["pending_ref"].Bytes)
		if ref == "" {
			continue
		}
		f, err := src.TakeFile(ref, "raw")
		if err != nil || f == nil {
			return nil, salvage(), rollback(sealedstate.ErrInvalid)
		}
		raw, err := rawsocket.Adopt(f, rawsocket.Fingerprint{Dev: v["pending_dev"].U64, Ino: v["pending_ino"].U64, Type: uint32(v["pending_type"].U64)})
		if err != nil {
			return nil, salvage(), rollback(err)
		}
		tuple, err := serverTupleRead(v["pending_tuple"].Bytes)
		if err != nil {
			return nil, salvage(), rollback(err)
		}
		if err = rawsocket.RestoreConnectReceipt(raw, rawsocket.ConnectConnected, true, 0, tuple, time.Time{}); err != nil {
			return nil, salvage(), rollback(err)
		}
		s.flowState.endpoints[int(v["allocation_port"].U64)].pendingRaw = raw
	}
	imported.resources = src.taken
	s.importedImage = imported
	return imported, Salvage{}, nil
}

// Detach only relinquishes wrappers. SHUT_RDWR/Close/normal keeper cleanup
// would irreversibly destroy originals needed by same-PID previous dispatch.
// origin: hot-update S6 plan §4.2 / T56
func (i *serverImportedImage) DetachPristine() (Salvage, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.active || i.detached {
		return Salvage{}, sealedstate.ErrInvalid
	}
	if _, err := i.server.flowState.manager.Detach(); err != nil {
		return Salvage{}, err
	}
	var err error
	for _, k := range i.keepers {
		_, e := k.Detach()
		err = errors.Join(err, e)
	}
	if err != nil {
		return Salvage{Resources: i.resources}, err
	}
	i.server.mu.Lock()
	for _, e := range i.server.flowState.endpoints {
		e.pendingRaw = nil
		e.listener = nil
		e.keeper = nil
	}
	i.server.controlKeeper = nil
	i.server.ln = nil
	i.server.importedImage = nil
	i.stateFile = nil
	i.detached = true
	i.server.closed = true
	i.server.mu.Unlock()
	return Salvage{Resources: i.resources}, nil
}

func restoreServerRecords(s *Server, cut sealedstate.CutState) error {
	st := s.flowState
	byID := map[string]*flowEndpoint{}
	var serverValues map[string]sealedstate.Field
	for _, rec := range cut.Records {
		v := clientFields(rec)
		switch rec.Kind {
		case "server":
			serverValues = v
			copy(st.ticketKey[:], v["ticket_key"].Bytes)
		case "fences":
			if err := restoreServerFences(s, rec); err != nil {
				return err
			}
		case "endpoint":
			e, err := restoreServerEndpoint(v)
			if err != nil {
				return err
			}
			st.endpoints[e.allocation.Port] = e
			byID[string(e.id[:])] = e
		}
	}
	for _, rec := range cut.Records {
		v := clientFields(rec)
		switch rec.Kind {
		case "permit":
			e := byID[string(v["id"].Bytes)]
			if e == nil {
				return sealedstate.ErrInvalid
			}
			p := &flowPermit{owner: v["owner_gen"].U64, next: v["next_owner_gen"].U64, consumed: v["consumed"].U64 != 0, abort: v["abort"].U64 != 0}
			copy(p.requestID[:], v["request_id"].Bytes)
			copy(p.cutID[:], v["cut_id"].Bytes)
			copy(p.digest[:], v["image_digest"].Bytes)
			var err error
			p.deadline, err = clientBootDeadline(v["deadline_boot_ns"].U64)
			if err != nil {
				return err
			}
			if p.requestID == ([16]byte{}) || p.cutID == ([16]byte{}) || (!p.consumed && e.ownerGen != p.owner || p.consumed && e.ownerGen != p.next) {
				return sealedstate.ErrInvalid
			}
			e.permit = p
		case "receipt":
			e := byID[string(v["id"].Bytes)]
			if e == nil {
				return sealedstate.ErrInvalid
			}
			request, err := decodeResumeRequest(v["request"].Bytes)
			if err != nil {
				return sealedstate.ErrInvalid
			}
			response, err := serverReceiptResponse(v["response"].Bytes)
			if err != nil {
				return err
			}
			// INITIAL establishes the Endpoint, so its canonical request carries
			// the zero EndpointID. The response binds the newly allocated ID;
			// rejecting this cached pair makes every real attached export fail.
			// origin: hot-update S6 plan §2.2 / T24
			requestBound := request.Endpoint == e.id
			if request.Mode == flow.ResumeModeInitial {
				requestBound = request.Endpoint == (flow.EndpointID{}) && len(request.Ticket) == 0 && request.Owner == response.Owner && request.Target == e.targetPort
			}
			if !requestBound || response.Endpoint != e.id || request.RequestID != response.RequestID {
				return sealedstate.ErrInvalid
			}
			e.receipt = &resumeReceipt{request: request, response: response}
		}
	}
	seen := map[string]bool{}
	waiters := serverValues["waiters"].Bytes
	for len(waiters) != 0 {
		key := string(waiters[:16])
		waiters = waiters[16:]
		e := byID[key]
		if e == nil || seen[key] {
			return sealedstate.ErrInvalid
		}
		seen[key] = true
		st.waiters = append(st.waiters, e)
	}
	slots := serverValues["pending_slots"].U64
	if slots < uint64(len(st.waiters)) {
		return sealedstate.ErrInvalid
	}
	for range slots {
		st.pending <- struct{}{}
	}
	return nil
}
func restoreServerEndpoint(v map[string]sealedstate.Field) (*flowEndpoint, error) {
	e := &flowEndpoint{id: flow.EndpointID(v["id"].Bytes), allocation: Allocation{SID: string(v["allocation_sid"].Bytes), NID: string(v["allocation_nid"].Bytes), Port: int(v["allocation_port"].U64), TokenHash: string(v["allocation_token_hash"].Bytes), Epoch: int64(v["allocation_epoch"].U64)}, phase: EndpointPhase(v["phase"].U64), ownerGen: v["owner_gen"].U64, carrierGen: v["carrier_gen"].U64, targetPort: uint16(v["target_port"].U64), ticket: bytes.Clone(v["ticket"].Bytes), fence: fenceSnap{port: int64(v["fence_port"].U64), sess: int64(v["fence_session"].U64), alloc: int64(v["fence_allocation"].U64)}, nextSeq: v["next_seq"].U64, stop: make(chan struct{}), done: make(chan struct{})}
	// No accept worker exists during pristine import. P06 replaces done before
	// launching the next generation, so terminal close cannot wait on no owner.
	// origin: hot-update S6 plan §6.3 / T26
	close(e.done)
	copy(e.lineage[:], v["lineage"].Bytes)
	if n := v["created_unix_ns"].U64; n != 0 {
		e.createdAt = time.Unix(0, int64(n))
	}
	deadline, err := clientBootDeadline(v["deadline_boot_ns"].U64)
	if err != nil {
		return nil, err
	}
	e.detachDeadline = deadline
	e.detachBootNS = v["deadline_boot_ns"].U64
	e.fired = v["fired"].U64 != 0
	e.cleanupPending = v["cleanup_pending"].U64 != 0
	e.paused = true
	if v["pending_type"].U64 > math.MaxUint32 {
		return nil, sealedstate.ErrInvalid
	}
	// Fired/cleanup and the original absolute deadline are installed before any
	// timer or Accept launch; activation must not renew a detached owner.
	return e, nil
}

// Reuse the S5 wire codec and normalize its refusal at the private boundary.
// wireCursor retains a bad-input flag and pads primitive reads, so malformed
// cached responses are ordinary errors and cannot become an empty success.
// origin: hot-update S6 plan §2.2 / T24
func serverReceiptResponse(data []byte) (resumeResponse, error) {
	response, err := decodeResumeResponse(data)
	if err != nil {
		return resumeResponse{}, sealedstate.ErrInvalid
	}
	return response, nil
}

// Caller holds the Server table lock. ACTIVE or terminal stop consumes the
// decode-only sealed FD once; pristine Detach transfers it without closing.
// origin: hot-update S6 plan §4.2; internal review rv-fd-8
func takeServerImportStateLocked(s *Server) *os.File {
	if s.importedImage == nil {
		return nil
	}
	file := s.importedImage.stateFile
	s.importedImage.stateFile = nil
	return file
}
