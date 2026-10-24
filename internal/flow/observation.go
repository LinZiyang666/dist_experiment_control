package flow

import (
	"encoding/binary"

	"github.com/LinZiyang666/tether/internal/drillobserve"
	"github.com/LinZiyang666/tether/internal/workload"
)

// Snapshot only owner state, after its committed syscall result. The hook
// queues a copy without writing or acquiring any data-plane owner lock.
// origin: hot-update S5 plan §3.1 / §9.1 / DRILL-OBSERVER
func observeRecordLocked(r *record) {
	e := drillobserve.Event{Kind: "flow-state", Domain: "flow", Endpoint: [16]byte(r.open.EndpointID), Flow: [16]byte(r.open.ID)}
	e.Values[0] = r.ownerGen
	e.Values[1] = r.carrierGen
	e.Values[2] = r.open.OpenSeq
	e.Values[3] = uint64(r.phase)
	e.Values[4] = uint64(r.openStatus)
	if r.raw != nil {
		fp := r.raw.Fingerprint()
		e.Values[5] = fp.Dev
		e.Values[6] = fp.Ino
		e.Values[7] = uint64(fp.Type)
		if !r.closed {
			e.Values[14] = 1
		}
	}
	tx := replayStateLocked(r)
	e.Values[8] = tx.Base
	e.Values[9] = tx.End
	e.Values[10] = r.rxApplied
	e.Values[11] = r.rxReceived
	if r.replay != nil && !r.closed {
		e.Values[12] = uint64(tx.Capacity)
		e.Values[13] = tx.End - tx.Base
		e.Values[15] = 1
	}
	e.Data = append(e.Data, r.open.BindNonce[:]...)
	_, dev, ino, live := workload.ReplayObservation(r.replay)
	if !live {
		dev = 0
		ino = 0
	}
	e.Data = binary.LittleEndian.AppendUint64(e.Data, dev)
	e.Data = binary.LittleEndian.AppendUint64(e.Data, ino)
	drillobserve.Hook(e)
}
