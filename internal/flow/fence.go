package flow

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"math"
	"os"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/rawsocket"
	"github.com/LinZiyang666/tether/internal/sealedstate"
	"github.com/LinZiyang666/tether/internal/workload"
)

type frozenState struct {
	mu       sync.Mutex
	manager  *Manager
	released bool
	snapshot *sealedstate.Snapshot
}

// Export replaces only cut-owned sealed state; raw/replay resources remain
// borrowed. Keeping the swap under the lifecycle guard excludes Thaw/Discard.
// origin: hot-update S5 plan §5.3 / T31
func (st *frozenState) replaceImageSnapshot(snapshot *sealedstate.Snapshot) error {
	if st.released {
		return ErrClosed
	}
	old := st.snapshot
	st.snapshot = snapshot
	if old != nil {
		_ = old.Close()
	}
	return nil
}

// lockBefore charges lock contention to the caller's one absolute budget.
// This is used only for lifecycle locks; no raw IO runs under these locks.
func lockBefore(mu *sync.Mutex, deadline time.Time) bool {
	for !mu.TryLock() {
		if !deadline.IsZero() && !time.Now().Before(deadline) {
			return false
		}
		timer := time.NewTimer(time.Millisecond)
		<-timer.C
	}
	if !deadline.IsZero() && !time.Now().Before(deadline) {
		mu.Unlock()
		return false
	}
	return true
}
func waitBefore(done <-chan struct{}, deadline time.Time) bool {
	if done == nil {
		return true
	}
	if deadline.IsZero() {
		<-done
		return true
	}
	timer := time.NewTimer(max(time.Until(deadline), 0))
	defer timer.Stop()
	select {
	case <-done:
		return true
	case <-timer.C:
		return false
	}
}

// Freeze fences admission before stopping both directions together. Deadline
// wakeups interrupt only poll waits; the business socket and its OFD survive.
// origin: hot-update S5 plan §3.5 / T10
func (m *Manager) Freeze(deadline time.Time) (*Frozen, error) {
	if m == nil || m.state == nil {
		return nil, ErrClosed
	}
	st := m.state
	if !lockBefore(&st.mu, deadline) {
		return nil, os.ErrDeadlineExceeded
	}
	if st.closed {
		st.mu.Unlock()
		return nil, ErrClosed
	}
	if st.frozen {
		st.mu.Unlock()
		return nil, ErrFrozen
	}
	st.frozen = true
	records := make([]*record, 0, len(st.records))
	for _, r := range st.records {
		records = append(records, r)
		r.mu.Lock()
		r.desired = false
		stopPumpsLocked(r)
		r.mu.Unlock()
	}
	st.mu.Unlock()
	for _, r := range records {
		r.mu.Lock()
		done := r.done
		r.mu.Unlock()
		if !waitBefore(done, deadline) {
			_ = m.ActivateRaw()
			return nil, os.ErrDeadlineExceeded
		}
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.closed {
		return nil, ErrClosed
	}
	for _, r := range records {
		r.mu.Lock()
		if !r.closed {
			var err error
			if r.raw != nil {
				err = rawsocket.Hold(r.raw)
			}
			if err == nil {
				_, r.replayFile, err = r.replay.Freeze()
			}
			if err != nil {
				r.mu.Unlock()
				st.frozen = false
				for _, old := range records {
					old.mu.Lock()
					old.desired = true
					startPumpsLocked(old)
					old.mu.Unlock()
				}
				return nil, err
			}
		}
		r.mu.Unlock()
	}
	return &Frozen{state: &frozenState{manager: m}}, nil
}
func (f *Frozen) Thaw() error {
	if f == nil || f.state == nil {
		return ErrClosed
	}
	st := f.state
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.released {
		return ErrClosed
	}
	st.released = true
	if st.snapshot != nil {
		_ = st.snapshot.Close()
		st.snapshot = nil
	}
	return st.manager.ActivateRaw()
}
func (f *Frozen) Discard() error {
	if f == nil || f.state == nil {
		return ErrClosed
	}
	st := f.state
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.released {
		return ErrClosed
	}
	st.released = true
	if st.snapshot != nil {
		_ = st.snapshot.Close()
		st.snapshot = nil
	}
	return st.manager.Close()
}

// Record values are selected by name from the frozen descriptor table. Missing
// runtime fields are an error, never silently filled with a codec default.
func privateRecord(kind string, values map[string]sealedstate.Field) (sealedstate.Record, error) {
	rec := sealedstate.Record{Kind: kind}
	for _, d := range sealedstate.RecordDescriptors {
		if d.Kind == kind {
			if len(values) != len(d.Fields) {
				return rec, failure(ReasonStateInvalid)
			}
			for _, fd := range d.Fields {
				v, ok := values[fd.Name]
				if !ok {
					return rec, failure(ReasonStateInvalid)
				}
				v.Name = fd.Name
				v.Type = fd.Type
				if fd.Type >= sealedstate.FieldBytes {
					if fd.Width > 0 && len(v.Bytes) != fd.Width || fd.Width == 0 && len(v.Bytes) > fd.MaxBytes {
						return rec, failure(ReasonStateInvalid)
					}
					v.Bytes = append([]byte(nil), v.Bytes...)
				} else if fd.Type == sealedstate.FieldBool && v.U64 > 1 || fd.Type == sealedstate.FieldNonnegativeI64 && v.U64 > math.MaxInt64 {
					return rec, failure(ReasonStateInvalid)
				}
				rec.Fields = append(rec.Fields, v)
			}
			return rec, nil
		}
	}
	return rec, failure(ReasonStateInvalid)
}
func number(v uint64) sealedstate.Field { return sealedstate.Field{U64: v} }
func buffer(v []byte) sealedstate.Field { return sealedstate.Field{Bytes: v} }
func boolean(v bool) sealedstate.Field {
	if v {
		return number(1)
	}
	return number(0)
}
func textField(v string) sealedstate.Field { return buffer([]byte(v)) }
func pairBytes(v [2]uint64) []byte {
	b := binary.BigEndian.AppendUint64(nil, v[0])
	return binary.BigEndian.AppendUint64(b, v[1])
}
func deadlineBoot(t time.Time) (uint64, error) {
	if t.IsZero() {
		return 0, nil
	}
	now, err := rawsocket.BootNow()
	if err != nil {
		return 0, err
	}
	return uint64(max(1, now+int64(time.Until(t)))), nil
}
func bootDeadline(v uint64) (time.Time, error) {
	if v == 0 {
		return time.Time{}, nil
	}
	now, err := rawsocket.BootNow()
	if err != nil {
		return time.Time{}, err
	}
	return time.Now().Add(time.Duration(int64(v) - now)), nil
}

func snapshotRecord(r *record) (sealedstate.Record, handoff.Resource, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	tx := replayStateLocked(r)
	ref := "flow." + hex.EncodeToString(r.open.ID[:])
	resource := handoff.Resource{ID: ref, Kind: "flow-v1", Files: make(map[string]*os.File)}
	var fp rawsocket.Fingerprint
	var tuple rawsocket.Tuple
	var phase rawsocket.ConnectState
	var consumed bool
	var connectErr error
	if r.raw != nil && !r.closed {
		raw, err := rawsocket.BorrowHeldFile(r.raw)
		if err != nil {
			return sealedstate.Record{}, resource, err
		}
		resource.Files["raw"] = raw
		fp = r.raw.Fingerprint()
		phase, consumed, tuple, _, connectErr = rawsocket.ConnectReceipt(r.raw)
	}
	rawRef, replayRef := "", ""
	if resource.Files["raw"] != nil {
		rawRef = ref
	}
	if !r.closed {
		resource.Files["replay"] = r.replayFile
		replayRef = ref
	}
	code := uint64(0)
	var errno syscall.Errno
	if errors.As(connectErr, &errno) {
		code = uint64(errno)
	}
	if connectErr != nil && code == 0 {
		return sealedstate.Record{}, resource, failure(ReasonStateInvalid)
	}
	boot := r.connectDeadlineBootNS
	tuples := []byte(nil)
	if tuple.Local.IsValid() {
		a, _ := tuple.Local.MarshalBinary()
		b, _ := tuple.Remote.MarshalBinary()
		tuples = append([]byte{byte(len(a))}, a...)
		tuples = append(tuples, b...)
	}
	state := stateLocked(r)
	rec, err := privateRecord(sealedstate.KindFlow, map[string]sealedstate.Field{
		"id": buffer(r.open.ID[:]), "endpoint_id": buffer(r.open.EndpointID[:]), "open_seq": number(r.open.OpenSeq), "intent": buffer(r.open.Intent[:]), "bind_nonce": buffer(r.open.BindNonce[:]), "target_port": number(uint64(r.open.TargetPort)),
		"carrier_gen": number(r.carrierGen), "owner_gen": number(r.ownerGen), "phase": number(uint64(r.phase)), "open_status": number(uint64(r.openStatus)), "open_reason": number(uint64(r.openReason)),
		"tx_base": number(tx.Base), "tx_end": number(tx.End), "tx_fin": number(r.txFIN), "tx_fin_receipt": boolean(r.txFINReceipt), "tx_source_ended": boolean(r.sourceEnded), "tx_pending": buffer(r.txPending), "tx_pending_error": textField(r.txError),
		"rx_applied": number(r.rxApplied), "rx_received": number(r.rxReceived), "rx_pending": buffer(r.rxPending), "rx_fin": number(r.rxFIN), "rx_fin_receipt": boolean(r.rxCloseWrite), "rx_close_write": boolean(r.rxCloseWrite), "fin_flags": number(uint64(state.FINFlags)),
		"raw_dev": number(fp.Dev), "raw_ino": number(fp.Ino), "raw_type": number(uint64(fp.Type)), "raw_tuple": buffer(tuples), "raw_ref": textField(rawRef), "replay_ref": textField(replayRef), "raw_read_pending": buffer(nil), "raw_read_error": textField(""),
		"connect_state": number(uint64(phase)), "connect_so_error": number(code), "connect_so_error_consumed": boolean(consumed), "connect_deadline_boot_ns": number(boot),
		"writer_error": textField(r.writerError), "close_mode": number(uint64(r.closeMode)), "close_reason": number(uint64(r.reason)), "close_receipt": boolean(r.closeReceipt), "final_end": buffer(pairBytes(r.finalEnd)), "final_applied": buffer(pairBytes(r.finalApplied)), "revoked": boolean(r.revoked), "cleanup_pending": boolean(r.cleanupPending),
	})
	return rec, resource, err
}
func (f *Frozen) SnapshotState() (sealedstate.CutState, error) {
	var out sealedstate.CutState
	if f == nil || f.state == nil {
		return out, ErrClosed
	}
	fs := f.state
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return snapshotFlowCutLocked(fs)
}

// Caller holds the frozen lifecycle guard through the complete cut transaction.
// origin: hot-update S5 plan §5.3 / T31
func snapshotFlowCutLocked(fs *frozenState) (sealedstate.CutState, error) {
	var out sealedstate.CutState
	if fs.released {
		return out, ErrClosed
	}
	st := fs.manager.state
	st.mu.Lock()
	defer st.mu.Unlock()
	if !st.frozen || st.closed {
		return out, ErrFrozen
	}
	ids := make([]ID, 0, len(st.records))
	for id := range st.records {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return string(ids[i][:]) < string(ids[j][:]) })
	for _, id := range ids {
		rec, res, err := snapshotRecord(st.records[id])
		if err != nil {
			return out, err
		}
		out.Records = append(out.Records, rec)
		if len(res.Files) > 0 {
			out.Resources = append(out.Resources, res)
		}
	}
	endpoints := make([]EndpointID, 0, len(st.retirement))
	for id := range st.retirement {
		endpoints = append(endpoints, id)
	}
	sort.Slice(endpoints, func(i, j int) bool { return string(endpoints[i][:]) < string(endpoints[j][:]) })
	for _, id := range endpoints {
		rt := retirementImage(id, st.retirement[id])
		var holes []byte
		for _, h := range rt.Holes {
			holes = binary.BigEndian.AppendUint64(holes, h)
		}
		rec, err := privateRecord(sealedstate.KindRetirement, map[string]sealedstate.Field{"id": buffer(id[:]), "endpoint_id": buffer(id[:]), "next_open_seq": number(rt.NextOpenSeq), "retired_through": number(rt.RetiredThrough), "holes": buffer(holes), "terminal_metadata": buffer(nil)})
		if err != nil {
			return out, err
		}
		out.Records = append(out.Records, rec)
	}
	return out, nil
}

func readPrivate(rec sealedstate.Record) (map[string]sealedstate.Field, error) {
	values := make(map[string]sealedstate.Field, len(rec.Fields))
	for _, f := range rec.Fields {
		if _, ok := values[f.Name]; ok {
			return nil, failure(ReasonStateInvalid)
		}
		values[f.Name] = f
	}
	checked, err := privateRecord(rec.Kind, values)
	if err != nil {
		return nil, err
	}
	for _, f := range checked.Fields {
		if values[f.Name].Type != f.Type {
			return nil, failure(ReasonStateInvalid)
		}
	}
	return values, nil
}
func restoreFlowRecords(cfg Config, cut sealedstate.CutState) (*Manager, []handoff.Resource, error) {
	m, err := New(cfg)
	if err != nil {
		return nil, cut.Resources, err
	}
	m.state.frozen = true
	m.state.pristine = true
	// On refusal the caller still owns every taken FD, including ones wrapped
	// before a later validation failed. Detach releases only replay reservations.
	fail := func(e error) (*Manager, []handoff.Resource, error) {
		for _, r := range m.state.records {
			if r.replay != nil && !r.closed {
				_, _, _ = r.replay.Detach()
			}
		}
		return nil, cut.Resources, e
	}
	resources := make(map[string]handoff.Resource)
	for _, res := range cut.Resources {
		if _, ok := resources[res.ID]; ok {
			return fail(failure(ReasonStateInvalid))
		}
		resources[res.ID] = res
	}
	for _, rec := range cut.Records {
		v, e := readPrivate(rec)
		if e != nil {
			return fail(e)
		}
		switch rec.Kind {
		case sealedstate.KindRetirement:
			var id EndpointID
			copy(id[:], v["endpoint_id"].Bytes)
			if id == (EndpointID{}) || m.state.retirement[id] != nil || string(v["id"].Bytes) != string(id[:]) {
				return fail(failure(ReasonStateInvalid))
			}
			rt := &retirement{next: v["next_open_seq"].U64, through: v["retired_through"].U64, holes: make(map[uint64]bool), live: make(map[uint64]ID)}
			holes := v["holes"].Bytes
			if rt.next == 0 || rt.through >= rt.next || len(holes)%8 != 0 || len(holes) > MaxRetirementHoles*8 || len(v["terminal_metadata"].Bytes) != 0 {
				return fail(failure(ReasonStateInvalid))
			}
			for len(holes) > 0 {
				h := binary.BigEndian.Uint64(holes)
				holes = holes[8:]
				if h <= rt.through || h >= rt.next || rt.holes[h] {
					return fail(failure(ReasonStateInvalid))
				}
				rt.holes[h] = true
			}
			m.state.retirement[id] = rt
		case sealedstate.KindFlow:
			r, e := restoreRecord(cfg, v, resources)
			if e != nil {
				return fail(e)
			}
			if m.state.records[r.open.ID] != nil {
				if !r.closed {
					_, _, _ = r.replay.Detach()
				}
				return fail(failure(ReasonStateInvalid))
			}
			r.settle = func(join bool) { settleRecordJoining(m, r, join) }
			m.state.records[r.open.ID] = r
			delete(resources, "flow."+hex.EncodeToString(r.open.ID[:]))
		// A flow cut cannot take ownership of another domain's records.
		// origin: hot-update S5 plan §5.1 / §10.2
		case sealedstate.KindAuthority, sealedstate.KindBinding, sealedstate.KindEndpoint, sealedstate.KindMaterial, sealedstate.KindPermit, sealedstate.KindReceipt, sealedstate.KindSaltRing, sealedstate.KindSSActor, sealedstate.KindSSServer, sealedstate.KindTimer:
			return fail(failure(ReasonStateInvalid))
		default:
			return fail(failure(ReasonStateInvalid))
		}
	}
	if len(resources) != 0 {
		return fail(failure(ReasonStateInvalid))
	}
	active := 0
	for _, r := range m.state.records {
		rt := m.state.retirement[r.open.EndpointID]
		if rt == nil || r.open.OpenSeq >= rt.next {
			return fail(failure(ReasonStateInvalid))
		}
		if !r.closed {
			active++
			if retired(rt, r.open.OpenSeq) {
				return fail(failure(ReasonStateInvalid))
			}
			if _, ok := rt.live[r.open.OpenSeq]; ok {
				return fail(failure(ReasonStateInvalid))
			}
			rt.live[r.open.OpenSeq] = r.open.ID
		}
	}
	if active > m.state.cfg.MaxFlows || len(m.state.records)-active > MaxTerminalMetadata {
		return fail(failure(ReasonCapacity))
	}
	m.state.importedResources = append([]handoff.Resource(nil), cut.Resources...)
	return m, nil, nil
}
func restoreRecord(cfg Config, v map[string]sealedstate.Field, resources map[string]handoff.Resource) (*record, error) {
	bad := func() (*record, error) { return nil, failure(ReasonStateInvalid) }
	if v["phase"].U64 < 1 || v["phase"].U64 > 5 || v["connect_state"].U64 > 3 || v["raw_type"].U64 > math.MaxUint32 {
		return bad()
	}
	r := &record{change: make(chan struct{}), dial: cfg.Dial, phase: Phase(v["phase"].U64), ownerGen: v["owner_gen"].U64, carrierGen: v["carrier_gen"].U64}
	copy(r.open.ID[:], v["id"].Bytes)
	copy(r.open.EndpointID[:], v["endpoint_id"].Bytes)
	copy(r.open.Intent[:], v["intent"].Bytes)
	copy(r.open.BindNonce[:], v["bind_nonce"].Bytes)
	r.open.OpenSeq = v["open_seq"].U64
	r.open.TargetPort = uint16(v["target_port"].U64)
	r.open.Direction = FlagDirection
	r.open.OwnerGen = r.ownerGen
	r.open.CarrierGen = r.carrierGen
	if r.open.ID == (ID{}) || r.open.EndpointID == (EndpointID{}) || r.open.OpenSeq == 0 || v["target_port"].U64 > 65535 || r.open.TargetPort == 0 || r.ownerGen == 0 || r.open.Intent != OpenIntent(r.open.EndpointID, r.open.ID, r.open.OpenSeq, r.open.TargetPort) || r.phase < FlowPhaseOpening || r.phase > FlowPhaseTerminal {
		return bad()
	}
	flags := v["fin_flags"].U64
	if flags > 15 {
		return bad()
	}
	r.txFINSet = flags&1 != 0
	r.rxFINSet = flags&2 != 0
	r.rxCloseWrite = v["rx_close_write"].U64 != 0
	r.txFINReceipt = v["tx_fin_receipt"].U64 != 0
	r.rxApplied = v["rx_applied"].U64
	r.rxReceived = v["rx_received"].U64
	r.rxPending = append([]byte(nil), v["rx_pending"].Bytes...)
	r.txPending = append([]byte(nil), v["tx_pending"].Bytes...)
	r.txFIN = v["tx_fin"].U64
	r.rxFIN = v["rx_fin"].U64
	r.sourceEnded = v["tx_source_ended"].U64 != 0
	r.txError = string(v["tx_pending_error"].Bytes)
	r.writerError = string(v["writer_error"].Bytes)
	r.revoked = v["revoked"].U64 != 0
	r.cleanupPending = v["cleanup_pending"].U64 != 0
	r.closeMode = CloseMode(v["close_mode"].U64)
	r.reason = Reason(v["close_reason"].U64)
	r.closeReceipt = v["close_receipt"].U64 != 0
	r.openStatus = uint8(v["open_status"].U64)
	r.openReason = Reason(v["open_reason"].U64)
	if (flags&4 != 0) != r.rxCloseWrite || (flags&8 != 0) != r.txFINReceipt || v["rx_fin_receipt"].U64 != v["rx_close_write"].U64 || r.txFINSet && (!r.sourceEnded || r.txError != "eof" || len(r.txPending) != 0) || r.rxApplied > r.rxReceived || r.rxReceived-r.rxApplied != uint64(len(r.rxPending)) || r.rxFINSet && r.rxFIN < r.rxReceived || r.rxCloseWrite && (!r.rxFINSet || r.rxApplied != r.rxFIN) || v["close_mode"].U64 > 3 || v["close_reason"].U64 > 16 || v["open_reason"].U64 > 16 || v["open_status"].U64 < 1 || v["open_status"].U64 > 3 || r.txError != "" && r.txError != "eof" && r.txError != "io_failure" || r.writerError != "" && r.writerError != "io_failure" || len(v["raw_read_pending"].Bytes) != 0 || len(v["raw_read_error"].Bytes) != 0 {
		return bad()
	}
	if len(v["final_end"].Bytes) != 16 || len(v["final_applied"].Bytes) != 16 {
		return bad()
	}
	for i := range 2 {
		r.finalEnd[i] = binary.BigEndian.Uint64(v["final_end"].Bytes[i*8:])
		r.finalApplied[i] = binary.BigEndian.Uint64(v["final_applied"].Bytes[i*8:])
		if r.finalApplied[i] > r.finalEnd[i] {
			return bad()
		}
	}
	deadline, err := bootDeadline(v["connect_deadline_boot_ns"].U64)
	if err != nil {
		return nil, err
	}
	r.open.Deadline = deadline
	r.connectDeadlineBootNS = v["connect_deadline_boot_ns"].U64
	ref := "flow." + hex.EncodeToString(r.open.ID[:])
	res := resources[ref]
	tx := workload.ReplayState{Version: 1, Capacity: workload.DefaultReplayCapacity, Base: v["tx_base"].U64, End: v["tx_end"].U64, FIN: r.txFINSet}
	if tx.Base > tx.End || tx.End-tx.Base > uint64(tx.Capacity) || r.txFINSet && r.txFIN != tx.End {
		return bad()
	}
	if r.phase == FlowPhaseTerminal && len(v["replay_ref"].Bytes) == 0 {
		if res.ID != "" || len(v["raw_ref"].Bytes) != 0 {
			return bad()
		}
		// Terminal receipts retain offsets without reserving a new replay capacity.
		r.closed = true
		r.terminalTx = tx
		return r, nil
	}
	if string(v["replay_ref"].Bytes) != ref || res.Kind != "flow-v1" || res.Files["replay"] == nil {
		return bad()
	}
	expectedFiles := 1
	if len(v["raw_ref"].Bytes) != 0 {
		expectedFiles++
	}
	if len(res.Files) != expectedFiles {
		return bad()
	}
	if err := restoreRecordSocket(r, v, res, ref, deadline); err != nil {
		return nil, err
	}
	replay, err := workload.RestoreReplay(cfg.Budget, tx, res.Files["replay"])
	if err != nil {
		return nil, err
	}
	r.replay = replay
	r.replayFile = res.Files["replay"]
	return r, nil
}

// restoreRecordSocket validates the original socket without consuming SO_ERROR.
// origin: hot-update S5 plan §3.2 / T09
func restoreRecordSocket(r *record, v map[string]sealedstate.Field, res handoff.Resource, ref string, deadline time.Time) error {
	if rawRef := string(v["raw_ref"].Bytes); rawRef != "" {
		if rawRef != ref || res.Files["raw"] == nil {
			return failure(ReasonStateInvalid)
		}
		fp := rawsocket.Fingerprint{Dev: v["raw_dev"].U64, Ino: v["raw_ino"].U64, Type: uint32(v["raw_type"].U64)}
		raw, err := rawsocket.Adopt(res.Files["raw"], fp)
		if err != nil {
			return err
		}
		b := v["raw_tuple"].Bytes
		var tuple rawsocket.Tuple
		if len(b) < 1 || int(b[0])+1 >= len(b) {
			return failure(ReasonStateInvalid)
		}
		n := int(b[0])
		if tuple.Local.UnmarshalBinary(b[1:1+n]) != nil || tuple.Remote.UnmarshalBinary(b[1+n:]) != nil {
			return failure(ReasonStateInvalid)
		}
		if !tuple.Local.IsValid() || !tuple.Remote.IsValid() {
			return failure(ReasonStateInvalid)
		}
		if err := rawsocket.RestoreConnectReceipt(raw, rawsocket.ConnectState(v["connect_state"].U64), v["connect_so_error_consumed"].U64 != 0, v["connect_so_error"].U64, tuple, deadline); err != nil {
			return err
		}
		r.raw = raw
	} else if r.phase != FlowPhaseOpening && r.phase != FlowPhaseConnecting {
		return failure(ReasonStateInvalid)
	}
	return nil
}

// RetainImportMetadata gives the pristine owner its original sealed descriptor.
// The client and standalone codecs both use this owner so activation, discard
// and shutdown release metadata through the same lifecycle as business files.
// origin: hot-update S5 internal review rv-image-1
func RetainImportMetadata(m *Manager, resource handoff.Resource) error {
	if m == nil || m.state == nil {
		return ErrClosed
	}
	st := m.state
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.closed || !st.frozen || !st.pristine || resource.Kind != "sealed-state-v1" || !validStateRef(resource.ID) || len(resource.Files) != 1 || resource.Files["state"] == nil || len(resource.Data) != 0 {
		return ErrFrozen
	}
	for _, old := range st.importedResources {
		if old.ID == resource.ID {
			return ErrFrozen
		}
	}
	st.importedResources = append(st.importedResources, resource)
	return nil
}

// Caller holds the manager guard. Only sealed metadata ends here; business
// descriptors remain with their wrappers until their normal close boundary.
// origin: hot-update S5 internal review rv-image-1
func releaseImportMetadataLocked(st *managerState) {
	for _, resource := range st.importedResources {
		if resource.Kind == "sealed-state-v1" {
			for _, file := range resource.Files {
				_ = file.Close()
			}
		}
	}
	st.importedResources = nil
}

// Detach transfers the exact pristine closure without shutdown of any OFD.
// Validate all wrappers before transferring the first one: a later refusal
// must not strand files already detached from earlier records.
// origin: hot-update S5 internal review rv-image-1
func (m *Manager) Detach() ([]handoff.Resource, error) {
	if m == nil || m.state == nil {
		return nil, ErrClosed
	}
	st := m.state
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.closed || !st.frozen || !st.pristine {
		return nil, ErrFrozen
	}
	records := make([]*record, 0, len(st.records))
	for _, r := range st.records {
		r.mu.Lock()
		records = append(records, r)
	}
	defer func() {
		for _, r := range records {
			r.mu.Unlock()
		}
	}()
	for _, r := range records {
		if r.running {
			return nil, ErrFrozen
		}
		if !r.closed {
			if r.raw != nil {
				if _, err := rawsocket.BorrowHeldFile(r.raw); err != nil {
					return nil, err
				}
			}
			if r.replay == nil {
				return nil, ErrFrozen
			}
			if _, _, err := r.replay.Freeze(); !errors.Is(err, workload.ErrFrozen) {
				return nil, ErrFrozen
			}
		}
	}
	resources := st.importedResources
	for _, r := range records {
		if !r.closed {
			// With all record locks held, no revoke/activation can change these logs.
			if _, _, err := r.replay.Detach(); err != nil {
				return nil, err
			}
			r.raw, r.replay, r.replayFile = nil, nil, nil
			r.closed, r.desired = true, false
		}
	}
	st.importedResources = nil
	st.closed, st.pristine = true, false
	managerChanged(st)
	return resources, nil
}
