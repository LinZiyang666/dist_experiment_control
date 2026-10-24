package flow

import (
	"sort"

	"github.com/LinZiyang666/tether/internal/drillobserve"
)

type retirement struct {
	next    uint64
	through uint64
	holes   map[uint64]bool
	live    map[uint64]ID
}

func retired(r *retirement, seq uint64) bool { return r != nil && (seq <= r.through || r.holes[seq]) }

// Never evict a hole to make room: it is the only evidence that a delayed OPEN
// must not create a second target connection after its receipt has been GC'd.
// origin: hot-update S5 plan §3.2 / T05
func retire(r *retirement, seq uint64) error {
	if r == nil || seq == 0 {
		return failure(ReasonStateInvalid)
	}
	if retired(r, seq) {
		return nil
	}
	if seq != r.through+1 && len(r.holes) >= MaxRetirementHoles {
		return failure(ReasonCapacity)
	}
	delete(r.live, seq)
	if seq == r.through+1 {
		r.through = seq
		for r.holes[r.through+1] {
			delete(r.holes, r.through+1)
			r.through++
		}
	} else {
		r.holes[seq] = true
	}
	return nil
}
func Retirement(m *Manager, endpoint EndpointID) RetirementImage {
	if m == nil || m.state == nil {
		return RetirementImage{}
	}
	st := m.state
	st.mu.Lock()
	defer st.mu.Unlock()
	return retirementImage(endpoint, st.retirement[endpoint])
}
func retirementImage(id EndpointID, r *retirement) RetirementImage {
	im := RetirementImage{EndpointID: id, NextOpenSeq: 1}
	if r == nil {
		return im
	}
	if drillobserve.Hook != nil {
		observeRetirement(id, r)
	}
	im.NextOpenSeq = r.next
	im.RetiredThrough = r.through
	for seq := range r.holes {
		im.Holes = append(im.Holes, seq)
	}
	sort.Slice(im.Holes, func(i, j int) bool { return im.Holes[i] < im.Holes[j] })
	return im
}
func CollectReceipt(m *Manager, id ID) error {
	if m == nil || m.state == nil {
		return ErrClosed
	}
	st := m.state
	st.mu.Lock()
	defer st.mu.Unlock()
	r := st.records[id]
	if r == nil {
		return failure(ReasonUnknownFlow)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.closed {
		return failure(ReasonStateInvalid)
	}
	if err := retire(st.retirement[r.open.EndpointID], r.open.OpenSeq); err != nil {
		return err
	}
	delete(st.records, id)
	managerChanged(st)
	return nil
}

// ForgetEndpoint releases retirement and settled history only after the
// allocation owner has fenced every possible attachment and closed live flows.
func ForgetEndpoint(m *Manager, id EndpointID) error {
	if m == nil || m.state == nil {
		return ErrClosed
	}
	st := m.state
	st.mu.Lock()
	defer st.mu.Unlock()
	for _, r := range st.records {
		if r.open.EndpointID == id {
			r.mu.Lock()
			closed := r.closed
			r.mu.Unlock()
			if !closed {
				return failure(ReasonStateInvalid)
			}
		}
	}
	// Terminal receipts are intentionally absent from States. Allocation
	// teardown owns their final GC as well as the permanent retirement proof.
	// origin: hot-update S5 internal review rv-flow-1
	for key, r := range st.records {
		if r.open.EndpointID == id {
			delete(st.records, key)
		}
	}
	delete(st.retirement, id)
	managerChanged(st)
	return nil
}

// RebindReceipt moves only retained terminal control evidence to an authorized
// carrier. It never allocates raw IO or permits DATA binding of a retired flow.
// origin: hot-update S5 internal review rv-flow-V1
func RebindReceipt(m *Manager, id ID, endpoint EndpointID, intent [32]byte, owner, carrier uint64) error {
	r, err := lookup(m, id)
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.open.EndpointID != endpoint || r.open.Intent != intent {
		return failure(ReasonIntentConflict)
	}
	if !r.closed || owner < r.ownerGen || carrier < r.carrierGen || owner == 0 || carrier == 0 {
		return failure(ReasonOwnerConflict)
	}
	r.ownerGen, r.carrierGen = owner, carrier
	changed(r)
	return nil
}

func observeRetirement(id EndpointID, r *retirement) {
	drillobserve.Hook(drillobserve.Event{Kind: "flow-retirement", Domain: "flow", Endpoint: [16]byte(id), Values: [16]uint64{r.next, r.through, uint64(len(r.holes)), uint64(len(r.live))}})
}
