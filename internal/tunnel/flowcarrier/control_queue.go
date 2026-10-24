package flowcarrier

import (
	"sort"

	"github.com/LinZiyang666/tether/internal/flow"
)

// Control ownership includes queued, shared-writer-waiting and in-flight frames.
// Coalescing changes only the reservation delta; close releases queued frames,
// while joined writers release their own batches. DATA never takes this quota.
// origin: hot-update S5 plan §3.4 / §3.5 / T11
func reserveControlLocked(a *Carrier, bytes int64) bool {
	if a.Closed || a.ControlBytes+bytes > 64<<10 {
		return false
	}
	if a.ControlBudget.Add(bytes) > 4<<20 {
		a.ControlBudget.Add(-bytes)
		return false
	}
	a.ControlBytes += bytes
	return true
}

func releaseControlLocked(a *Carrier, bytes int64) {
	a.ControlBytes -= bytes
	a.ControlBudget.Add(-bytes)
}

func releaseControl(a *Carrier, bytes int64) {
	a.Mu.Lock()
	releaseControlLocked(a, bytes)
	a.Mu.Unlock()
}

func controlBatchBytes(batch map[ControlKey]flow.Frame) int64 {
	var bytes int64
	for _, frame := range batch {
		bytes += int64(flow.HeaderBytes + len(frame.Body))
	}
	return bytes
}

func FlushControls(a *Carrier, batch map[ControlKey]flow.Frame) error {
	defer releaseControl(a, controlBatchBytes(batch))
	keys := make([]ControlKey, 0, len(batch))
	for key := range batch {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i].Kind < keys[j].Kind })
	for _, key := range keys {
		if err := writeCarrierFrame(a, a.Control, batch[key], true); err != nil {
			return err
		}
		// Queueing a terminal decision is not delivery. Keep its data stream
		// until CLOSE has crossed the shared writer, so peer EOF cannot win
		// against the authorization event and strand the original public FD.
		// A revoke CLOSE keeps the stream until the peer closes it instead
		// (the data reader releases it at that EOF).
		// origin: hot-update S5 internal review HOTUPDATE-T40
		// origin: hot-update S5 internal review SS-REVOKE-1
		if key.Kind == flow.KindClose && (len(batch[key].Body) == 0 || flow.CloseMode(batch[key].Body[0]) != flow.CloseModeRevoke) {
			if state, _, err := flow.Observe(a.Manager, key.ID); err == nil && state.Phase == flow.FlowPhaseTerminal {
				a.ReleaseFlowStream(key.ID)
			}
		}
	}
	return nil
}
