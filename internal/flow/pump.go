package flow

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"os"
	"sync"
	"time"

	"github.com/LinZiyang666/tether/internal/drillobserve"
	"github.com/LinZiyang666/tether/internal/rawsocket"
	"github.com/LinZiyang666/tether/internal/workload"
)

// Receive commits control decisions without performing raw writes. A full
// pending buffer rejects the entire frame so its reader can wait and retry;
// control processing and other flows remain independently schedulable.
func Receive(m *Manager, f Frame) error {
	r, err := lookup(m, f.FlowID)
	if err != nil {
		return err
	}
	var revoke Reason
	r.mu.Lock()
	defer func() {
		r.mu.Unlock()
		if revoke != 0 {
			_ = m.RevokeFlow(f.FlowID, revoke)
		}
		if r.settle != nil {
			r.settle(true)
		}
	}()
	if f.CarrierGen != r.carrierGen || f.CarrierGen == 0 {
		return failure(ReasonOwnerConflict)
	}
	if f.Version != Version || int(f.BodyLen) != len(f.Body) || f.Flags&FlagDirection == r.open.Direction {
		return failure(ReasonMalformed)
	}
	if r.closed {
		return receiveClosedLocked(r, f)
	}
	if r.revoked {
		return receiveRevokedLocked(r, f)
	}

	switch f.Kind {
	case KindData:
		return receiveDataLocked(r, f)
	case KindACK:
		if len(f.Body) != 0 || f.Flags & ^(FlagDirection|FlagFINReceipt) != 0 {
			return failure(ReasonMalformed)
		}
		return ackLocked(r, f.Offset, f.Flags&FlagFINReceipt != 0)
	case KindFIN:
		if len(f.Body) != 0 || f.Flags & ^FlagDirection != 0 || f.Offset < r.rxReceived || (r.rxFINSet && r.rxFIN != f.Offset) {
			return failure(ReasonMalformed)
		}
		r.rxFIN = f.Offset
		r.rxFINSet = true
		changed(r)
	case KindClose:
		revoke, err = receiveCloseLocked(r, f)
		return err
	// Journal and attachment frames belong to the carrier, never the raw pump.
	// origin: hot-update S5 plan §2.2 / §10.2
	case KindOpen, KindOpenACK, KindResume, KindResumeOK, KindReject:
		return failure(ReasonMalformed)
	default:
		return failure(ReasonMalformed)
	}
	return nil
}

// Terminal receipts may acknowledge retained decisions without advancing raw bytes.
// origin: hot-update S5 plan §3.3 / T07.
func receiveClosedLocked(r *record, f Frame) error {
	if f.Kind == KindClose {
		mode, reason, ends, applied, err := closeFields(f, r.open.Direction)
		if err != nil {
			return err
		}
		if (mode == CloseModeNormal) != (r.closeMode == CloseModeNormal) || reason != r.reason || ends != r.finalEnd || (mode == CloseModeNormal && applied != ends) {
			return failure(ReasonStateInvalid)
		}
		if !r.closeReceipt {
			r.closeReceipt = true
			changed(r)
		}
		return nil
	}
	if f.Kind == KindACK && len(f.Body) == 0 && f.Flags & ^(FlagDirection|FlagFINReceipt) == 0 && f.Offset <= r.finalEnd[0] {
		return nil
	}
	if f.Kind == KindFIN && len(f.Body) == 0 && f.Flags & ^FlagDirection == 0 && r.rxFINSet && f.Offset == r.rxFIN {
		return nil
	}
	return failure(ReasonRetired)
}

func receiveRevokedLocked(r *record, f Frame) error {
	// Authorization is closed before raw workers join. Valid controls already
	// in flight must not fence the shared carrier and hide our final CLOSE;
	// they cannot advance offsets or admit any DATA during this boundary.
	// Revocation also precedes the shutdown/join that fixes terminal
	// counters, so already queued ACKs remain harmless receipts.
	// origin: hot-update S5 internal review SS-REVOKE-1 / plan §6.5
	// origin: hot-update S5 internal review HOTUPDATE-T40
	if f.Kind == KindACK && len(f.Body) == 0 && f.Flags & ^(FlagDirection|FlagFINReceipt) == 0 && f.Offset <= replayStateLocked(r).End {
		return nil
	}
	if f.Kind == KindFIN && len(f.Body) == 0 && f.Flags & ^FlagDirection == 0 && f.Offset >= r.rxReceived && (!r.rxFINSet || f.Offset == r.rxFIN) {
		return nil
	}
	return failure(ReasonRetired)
}

// Abort/revoke fixes the final byte decisions before carrier receipts can replay them.
// origin: hot-update S5 plan §3.3 / T07.
func receiveCloseLocked(r *record, f Frame) (Reason, error) {
	var revoke Reason
	mode, reason, ends, applied, err := closeFields(f, r.open.Direction)
	if err != nil {
		return 0, err
	}
	if mode == CloseModeNormal && (!r.rxCloseWrite || !r.txFINReceipt || applied != ends) {
		return 0, failure(ReasonStateInvalid)
	}
	if mode == CloseModeReadAbort || mode == CloseModeWriteAbort {
		if ends[1] < r.rxReceived || r.rxFINSet && r.rxFIN != ends[1] {
			return 0, failure(ReasonMalformed)
		}
		if applied[0] >= r.replay.Snapshot().Base {
			if err := ackLocked(r, applied[0], false); err != nil {
				return 0, err
			}
		}
		// A peer abort stops this source, then each side drains the other's
		// fixed end or explicitly acknowledges a partial discard. Source n+err
		// still enters replay before publishing the final source end.
		// origin: hot-update S5 plan §3.3 / T07
		r.rxFIN, r.rxFINSet = ends[1], true
		r.finalEnd[1] = ends[1]
		r.sourceEnded, r.txError = true, "io_failure"
		if r.raw != nil {
			_ = r.raw.SetReadDeadline(time.Now())
		}
		if r.txFINSet && len(r.txPending) == 0 {
			abortLocked(r, CloseModeReadAbort)
		}
		r.closeReceipt = f.Flags&FlagCloseReceipt != 0 && len(r.txPending) == 0 && ends[0] == r.replay.Snapshot().End
	} else {
		r.finalEnd, r.finalApplied = ends, applied
		r.closeMode, r.reason = mode, reason
		r.closeReceipt = f.Flags&FlagCloseReceipt != 0
		if mode == CloseModeRevoke {
			revoke = reason
		}
	}
	changed(r)
	return revoke, nil
}

// receiveDataLocked validates overlap before retaining any new socket bytes.
// origin: hot-update S5 plan §3.1 / T03
func receiveDataLocked(r *record, f Frame) error {
	if f.Flags & ^FlagDirection != 0 || len(f.Body) == 0 || len(f.Body) > MaxDataBytes || f.Offset > math.MaxUint64-uint64(len(f.Body)) {
		return failure(ReasonMalformed)
	}
	end := f.Offset + uint64(len(f.Body))
	if f.Offset > r.rxReceived {
		return failure(ReasonOffsetGap)
	}
	if r.rxFINSet && end > r.rxFIN {
		return failure(ReasonMalformed)
	}
	// Already applied history is deliberately not compared: it is no longer
	// retained. Strip it BEFORE any raw Write or pending overlap comparison.
	// origin: hot-update S5 plan §3.1 / T03
	if end <= r.rxApplied {
		return nil
	}
	offset, payload := f.Offset, f.Body
	if offset < r.rxApplied {
		payload = payload[r.rxApplied-offset:]
		offset = r.rxApplied
	}
	overlap := min(uint64(len(payload)), r.rxReceived-offset)
	if !bytes.Equal(payload[:overlap], r.rxPending[offset-r.rxApplied:offset-r.rxApplied+overlap]) {
		return failure(ReasonIntentConflict)
	}
	payload = payload[overlap:]
	if len(payload) > MaxDataBytes-len(r.rxPending) {
		return workload.ErrBackpressure
	}
	r.rxPending = append(r.rxPending, payload...)
	r.rxReceived += uint64(len(payload))
	if r.reason != 0 && !r.rxFINSet {
		r.finalEnd[1] = r.rxReceived
	}
	changed(r)
	return nil
}

func ackLocked(r *record, applied uint64, fin bool) error {
	tx := r.replay.Snapshot()
	if applied < tx.Base {
		return failure(ReasonOffsetExpired)
	}
	// A previous carrier may have applied bytes whose ACK was lost. Sent is an
	// attachment cursor, never an upper bound on the peer's applied receipt.
	// origin: hot-update S5 plan §3.1 / T02
	if applied > tx.End {
		return failure(ReasonMalformed)
	}
	if fin && (!r.txFINSet || applied != r.txFIN) {
		return failure(ReasonMalformed)
	}
	if applied == tx.Base && (!fin || r.txFINReceipt) {
		return nil
	}
	if err := r.replay.Ack(applied); err != nil {
		return err
	}
	if r.reason != 0 {
		r.finalApplied[0] = applied
	}
	if fin {
		r.txFINReceipt = true
	}
	changed(r)
	return nil
}

// NextData reads retained source bytes without acknowledging them. Each call
// gives one <=32 KiB dispatch, so a carrier scheduler can yield between flows.
func NextData(m *Manager, id ID, carrier, offset uint64) (Frame, <-chan struct{}, error) {
	r, err := lookup(m, id)
	if err != nil {
		return Frame{}, nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.carrierGen != carrier {
		return Frame{}, nil, failure(ReasonOwnerConflict)
	}
	if r.closed {
		return Frame{}, nil, ErrClosed
	}
	tx := r.replay.Snapshot()
	if offset < tx.Base {
		return Frame{}, nil, failure(ReasonOffsetExpired)
	}
	if offset > tx.End {
		return Frame{}, nil, failure(ReasonMalformed)
	}
	if offset == tx.End {
		return Frame{}, r.change, nil
	}
	body := make([]byte, min(uint64(MaxDataBytes), tx.End-offset))
	n, err := r.replay.Read(offset, body)
	if err != nil && !errors.Is(err, io.EOF) {
		return Frame{}, nil, err
	}
	body = body[:n]
	r.sent = offset + uint64(n)
	return Frame{Version: Version, Kind: KindData, FlowID: id, CarrierGen: carrier, Offset: offset, Flags: r.open.Direction, BodyLen: uint32(n), Body: body}, r.change, nil
}
func Controls(m *Manager, id ID, carrier uint64) ([]Frame, <-chan struct{}, error) {
	r, err := lookup(m, id)
	if err != nil {
		return nil, nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.carrierGen != carrier {
		return nil, nil, failure(ReasonOwnerConflict)
	}
	// Hard revoke cannot advertise its final ends until shutdown has joined raw
	// workers and captured their last syscall results. The wake survives the join.
	// origin: hot-update S5 internal review SS-REVOKE-1
	if r.revoked && !r.closed {
		return nil, r.change, nil
	}
	flags := r.open.Direction
	if r.rxCloseWrite && r.reason == 0 && r.txError != "io_failure" {
		flags |= FlagFINReceipt
	}
	out := []Frame{{Version: Version, Kind: KindACK, FlowID: id, CarrierGen: carrier, Offset: r.rxApplied, Flags: flags}}
	if r.txFINSet && !r.txFINReceipt && r.reason == 0 {
		out = append(out, Frame{Version: Version, Kind: KindFIN, FlowID: id, CarrierGen: carrier, Offset: r.txFIN, Flags: r.open.Direction})
	}
	// Revoke marks responsibility before waking in-flight raw syscalls. Their
	// final progress belongs in CLOSE; an earlier snapshot has unset counters
	// and cannot serve as the peer's irreversible discard decision.
	// origin: hot-update S5 internal review HOTUPDATE-T40
	if (r.reason != 0 && !r.revoked) || r.closed {
		body := []byte{byte(r.closeMode)}
		body = binary.BigEndian.AppendUint16(body, uint16(r.reason))
		ends, applied := r.finalEnd, r.finalApplied
		if r.open.Direction != 0 {
			ends[0], ends[1] = ends[1], ends[0]
			applied[0], applied[1] = applied[1], applied[0]
		}
		for _, v := range ends {
			body = binary.BigEndian.AppendUint64(body, v)
		}
		for _, v := range applied {
			body = binary.BigEndian.AppendUint64(body, v)
		}
		closeFlags := r.open.Direction
		if r.closeReceipt || r.reason != 0 && r.rxFINSet && (r.rxCloseWrite || r.writerError != "") {
			closeFlags |= FlagCloseReceipt
		}
		out = append(out, Frame{Version: Version, Kind: KindClose, FlowID: id, CarrierGen: carrier, Flags: closeFlags, BodyLen: uint32(len(body)), Body: body})
	}
	// A nil notification marks a complete terminal snapshot. Carrier watchers
	// must decide to stop from this same snapshot, rather than observing a later
	// terminal state after enqueueing an older ACK without its FIN receipt.
	// origin: hot-update S5 internal review rv-flow-1 / rv-flow-V1
	if r.closed {
		return out, nil, nil
	}
	return out, r.change, nil
}

func startPumpsLocked(r *record) {
	if r.running || r.closed || !r.desired || r.revoked {
		return
	}
	if r.raw != nil {
		_ = rawsocket.Release(r.raw)
		_ = r.raw.SetReadDeadline(time.Time{})
		_ = r.raw.SetWriteDeadline(time.Time{})
	}
	r.replay.Thaw()
	r.replayFile = nil
	r.stop = make(chan struct{})
	r.done = make(chan struct{})
	r.running = true
	go runPumps(r, r.stop, r.done)
}
func stopPumpsLocked(r *record) {
	if !r.running {
		return
	}
	select {
	case <-r.stop:
	default:
		close(r.stop)
	}
	if r.raw != nil {
		_ = r.raw.SetReadDeadline(time.Now())
		_ = r.raw.SetWriteDeadline(time.Now())
	}
}
func runPumps(r *record, stop <-chan struct{}, done chan struct{}) {
	defer func() {
		r.mu.Lock()
		// A revoked established flow also has a failed OPEN receipt. Its revoke
		// caller owns terminal capture after join, rather than connector cleanup.
		// origin: hot-update S5 internal review SS-REVOKE-1
		if r.openStatus == 3 && !r.closed && !r.revoked {
			r.closed = true
			r.phase = FlowPhaseTerminal
			r.finalEnd = [2]uint64{r.replay.Snapshot().End, r.rxReceived}
			r.finalApplied = [2]uint64{r.replay.Snapshot().Base, r.rxApplied}
			if r.raw != nil {
				_ = r.raw.Close()
			}
			_ = r.replay.Close()
			changed(r)
		}
		r.running = false
		// A failed Freeze may request thaw before the last old syscall returns.
		// Restart only here, after both old workers joined, never alongside them.
		if r.desired && !r.closed {
			select {
			case <-stop:
				startPumpsLocked(r)
			default:
			}
		}
		r.mu.Unlock()
		if r.settle != nil {
			r.settle(false)
		}
		// A join includes retirement publication and its observer callback.
		// Closing earlier lets the next owner replace callback state while this
		// old pump still writes it.
		// origin: hot-update S6 plan §6.2 / T23 / T56; hot-update S5 DRILL-OBSERVER
		close(done)
	}()
	r.mu.Lock()
	raw := r.raw
	needDial := raw == nil
	r.phase = FlowPhaseConnecting
	r.mu.Unlock()
	if needDial {
		var err error
		raw, err = r.dial(r.open.ID, r.open.TargetPort, r.open.Deadline, stop)
		r.mu.Lock()
		r.raw = raw
		if err != nil {
			if raw == nil && stopped(stop) {
				r.mu.Unlock()
				return
			}
			r.openStatus = 3
			r.openReason = ReasonIOFailure
			r.reason = ReasonIOFailure
			r.phase = FlowPhaseTerminal
			r.desired = false
			changed(r)
			r.mu.Unlock()
			return
		}
		// origin: hot-update S5 plan §9.1 / DRILL-RUN. Publish the
		// committed original raw FD before waiting for SO_ERROR; the
		// Opening receipt had no FD and an Active receipt arrives too late
		// to prove a blocked EINPROGRESS connect. The normal path stays inert.
		if drillobserve.Hook != nil {
			observeRecordLocked(r)
		}
		r.mu.Unlock()
	}
	if err := raw.CompleteConnect(r.open.Deadline, stop); err != nil {
		select {
		case <-stop:
			return
		default:
		}
		r.mu.Lock()
		r.openStatus = 3
		r.openReason = ReasonIOFailure
		r.reason = ReasonIOFailure
		r.phase = FlowPhaseTerminal
		r.desired = false
		changed(r)
		r.mu.Unlock()
		return
	}
	r.mu.Lock()
	r.openStatus = 2
	r.phase = FlowPhaseActive
	changed(r)
	r.mu.Unlock()
	var workers sync.WaitGroup
	workers.Add(2)
	go func() { defer workers.Done(); sourcePump(r, stop) }()
	go func() { defer workers.Done(); sinkPump(r, stop) }()
	workers.Wait()
}
func stopped(stop <-chan struct{}) bool {
	select {
	case <-stop:
		return true
	default:
		return false
	}
}
func waitChange(stop <-chan struct{}, change <-chan struct{}) bool {
	select {
	case <-stop:
		return false
	case <-change:
		return true
	}
}
func transientIO(err error) bool {
	return errors.Is(err, os.ErrDeadlineExceeded) || errors.Is(err, rawsocket.ErrBusy)
}

func sourcePump(r *record, stop <-chan struct{}) {
	buf := make([]byte, MaxDataBytes)
	for !stopped(stop) {
		r.mu.Lock()
		if len(r.txPending) > 0 {
			n, err := r.replay.Append(r.txPending)
			r.txPending = r.txPending[n:]
			changed(r)
			if err != nil && !errors.Is(err, workload.ErrBackpressure) {
				r.txError = "io_failure"
				r.sourceEnded = true
			}
			if len(r.txPending) > 0 {
				ch := r.change
				r.mu.Unlock()
				if !waitChange(stop, ch) {
					return
				}
				continue
			}
		}
		if r.sourceEnded {
			if r.txError == "eof" {
				tx := r.replay.Snapshot()
				r.txFIN = tx.End
				r.txFINSet = true
				_ = r.replay.Finish()
				r.phase = FlowPhaseDraining
			} else {
				abortLocked(r, CloseModeReadAbort)
			}
			changed(r)
			r.mu.Unlock()
			return
		}
		tx, ch := r.replay.Observe()
		if tx.End-tx.Base >= uint64(tx.Capacity) {
			r.mu.Unlock()
			if !waitChange(stop, ch) {
				return
			}
			continue
		}
		raw := r.raw
		r.mu.Unlock()
		n, err := raw.Read(buf)
		r.mu.Lock()
		// n+error is legal. Preserve the returned bytes before interpreting EOF,
		// abort or the freeze deadline; the cut includes this exact pending suffix.
		// origin: hot-update S5 plan §3.1 / §3.3 / T07
		recordSourceResult(r, buf, n, err)
		changed(r)
		r.mu.Unlock()
		if err != nil && transientIO(err) && stopped(stop) {
			return
		}
	}
}
func sinkPump(r *record, stop <-chan struct{}) {
	for !stopped(stop) {
		r.mu.Lock()
		if r.writerError != "" {
			r.mu.Unlock()
			return
		}
		if len(r.rxPending) == 0 {
			if r.rxFINSet && !r.rxCloseWrite && r.rxApplied == r.rxFIN {
				// FIN can overtake DATA on the control stream. CloseWrite and its ACK
				// are delayed until the original socket accepted every byte before F.
				// origin: hot-update S5 plan §3.3 / T06
				raw := r.raw
				r.mu.Unlock()
				err := raw.CloseWrite()
				r.mu.Lock()
				if err == nil {
					r.rxCloseWrite = true
				} else if !transientIO(err) {
					r.writerError = "io_failure"
					abortLocked(r, CloseModeWriteAbort)
				}
				changed(r)
			}
			ch := r.change
			finished := r.rxCloseWrite
			r.mu.Unlock()
			if finished || !waitChange(stop, ch) {
				return
			}
			continue
		}
		pending := append([]byte(nil), r.rxPending...)
		raw := r.raw
		r.mu.Unlock()
		n, err := raw.Write(pending)
		r.mu.Lock()
		// Only actual syscall progress advances A. The unaccepted suffix remains
		// pending even when the same syscall also returns an error.
		r.rxApplied += uint64(n)
		if r.reason != 0 {
			r.finalApplied[1] = r.rxApplied
		}
		r.rxPending = r.rxPending[n:]
		if err != nil && !transientIO(err) {
			r.writerError = "io_failure"
			abortLocked(r, CloseModeWriteAbort)
		}
		changed(r)
		r.mu.Unlock()
		if err != nil {
			return
		}
	}
}
func abortLocked(r *record, mode CloseMode) {
	// Shutdown wakes in-flight raw writes with an error. That local syscall
	// outcome cannot replace the authorization decision that caused it.
	// origin: hot-update S5 internal review HOTUPDATE-T40
	if r.revoked {
		return
	}
	if mode == CloseModeWriteAbort {
		r.closeMode = mode
		r.sourceEnded = true
		r.txError = "io_failure"
		if r.raw != nil {
			_ = r.raw.SetReadDeadline(time.Now())
		}
		return
	}
	if r.reason != 0 {
		return
	}
	r.txFINSet, r.txFINReceipt = false, false
	r.closeReceipt = false
	tx := r.replay.Snapshot()
	r.finalEnd = [2]uint64{tx.End, r.rxReceived}
	if r.rxFINSet {
		r.finalEnd[1] = r.rxFIN
	}
	r.finalApplied = [2]uint64{tx.Base, r.rxApplied}
	if r.writerError != "" {
		r.closeMode = CloseModeWriteAbort
	} else {
		r.closeMode = mode
	}
	r.reason = ReasonIOFailure
	r.phase = FlowPhaseDraining
}

func recordSourceResult(r *record, buf []byte, n int, err error) {
	if n > 0 {
		r.txPending = append(r.txPending, buf[:n]...)
	}
	if err != nil && !transientIO(err) {
		r.sourceEnded = true
		if errors.Is(err, io.EOF) && r.txError != "io_failure" {
			r.txError = "eof"
		} else {
			r.txError = "io_failure"
		}
	}
}

// CLOSE uses global wire directions; each actor keeps local source first.
// Even retired receipts validate the whole body, so malformed late control
// cannot turn into a successful discard acknowledgment.
// origin: hot-update S5 plan §2.2 / §3.3 / T06 / T07
func closeFields(f Frame, direction uint16) (CloseMode, Reason, [2]uint64, [2]uint64, error) {
	var ends, applied [2]uint64
	if len(f.Body) != 35 || f.Offset != 0 || f.Flags & ^(FlagDirection|FlagCloseReceipt) != 0 {
		return 0, 0, ends, applied, failure(ReasonMalformed)
	}
	mode, reason := CloseMode(f.Body[0]), Reason(binary.BigEndian.Uint16(f.Body[1:3]))
	if mode > CloseModeRevoke || reason > ReasonStateInvalid || (mode != CloseModeNormal && reason == 0) || (mode == CloseModeNormal && reason != 0) {
		return 0, 0, ends, applied, failure(ReasonMalformed)
	}
	for i := range 2 {
		ends[i] = binary.BigEndian.Uint64(f.Body[3+i*8:])
		applied[i] = binary.BigEndian.Uint64(f.Body[19+i*8:])
		if applied[i] > ends[i] {
			return 0, 0, ends, applied, failure(ReasonMalformed)
		}
	}
	if direction != 0 {
		ends[0], ends[1] = ends[1], ends[0]
		applied[0], applied[1] = applied[1], applied[0]
	}
	return mode, reason, ends, applied, nil
}
