// transfer_audit_forward.go (D8a §9) routes transfer audit (start/complete/failed) through
// leader Apply as a re-derivable OpTransferAudit entry. Post-D9 cutover this is LIVE in
// CLUSTER mode: wireClusterLate calls AttachTransferAuditSink so b.transferAuditSink routes
// each record through the §4.1 broker→leader forwarder. In SINGLE mode the sink stays nil
// and emitTransferAudit (transfer.go) falls through to the byte-identical best-effort
// pubAuditTransfer. (The per-phase TestD8ProductionWiresNoCluster guard was a build-and-prove
// scaffold and was removed at the D9 cutover — see test/d9; production now legitimately
// constructs the cluster.Node and wires this seam.)
package broker

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/schema"
)

// transferAuditForward* bound the async forward retry so the goroutine ALWAYS terminates
// (the transferAuditWG drains within attempts*backoff). A transient not_leader (election /
// just-stepped-down leader) is retried; a permanent error or success returns immediately.
// The audit is re-derivable ONLY once committed, so a forward that never reaches a leader is
// a bounded best-effort loss — the SAME posture as today's pubAuditTransfer.
const (
	transferAuditForwardAttempts = 5
	transferAuditForwardBackoff  = 100 * time.Millisecond
)

// AttachTransferAuditSink wires the D8a build-and-prove sink (TEST/HARNESS ONLY). After
// this call emitTransferAudit routes each record through fwd (the §4.1 broker→leader
// forwarder) as an OpTransferAudit commit instead of the live best-effort pubAuditTransfer.
// The forward runs in a tracked goroutine so it NEVER blocks the NATS handler (OQ9-A: start
// must not block the agent-forward); complete/failed take the same async path (they are
// terminal events off the data path). Call before Run. Production never calls it.
func (b *Broker) AttachTransferAuditSink(fwd *Forwarder) {
	b.attachTransferAuditSinkWith(func(payload []byte) error {
		return fwd.Forward(VerbTransferAudit, "", payload)
	})
}

// attachTransferAuditSinkWith is the testable core of AttachTransferAuditSink: it wires the
// sink to an arbitrary forward function (a fake in unit tests; fwd.Forward in production-harness
// wiring). The forward runs in a tracked goroutine (transferAuditWG, drained by
// WaitTransferAudit) so emitTransferAudit never blocks the NATS handler (OQ9-A); a transient
// ErrForwardNotLeader is retried (the derived reqID dedups a double-commit), a permanent error
// returns at once, and the loop ALWAYS terminates (bounded attempts×backoff) — not a leak.
func (b *Broker) attachTransferAuditSinkWith(forward func(payload []byte) error) {
	runForward := func(payload []byte, rec schema.AuditTransfer) {
		for attempt := 0; attempt < transferAuditForwardAttempts; attempt++ {
			ferr := forward(payload)
			if ferr == nil {
				return
			}
			if !errors.Is(ferr, cluster.ErrForwardNotLeader) {
				b.cfg.Logger.Warn("d8: transfer audit forward failed (best-effort)",
					"err", ferr, "tid", rec.TransferID, "kind", rec.Kind)
				return
			}
			time.Sleep(transferAuditForwardBackoff) // transient: retry (reqID dedups a double-commit)
		}
		b.cfg.Logger.Warn("d8: transfer audit forward gave up after retries (best-effort)",
			"tid", rec.TransferID, "kind", rec.Kind)
	}
	b.transferAuditSink = func(rec schema.AuditTransfer) {
		payload, err := json.Marshal(rec)
		if err != nil {
			return
		}
		// D9 round-1 MAJOR: during the ordered shutdown forward SYNCHRONOUSLY in the NATS
		// handler so nc.Drain (which waits for handler callbacks) drains this audit too — the
		// async goroutine could otherwise be spawned AFTER WaitTransferAudit returned and lost
		// on Drain. Steady state stays async (OQ9-A: never block the agent-forward handler).
		//
		// audit M7 / xx-concurrency F1: the {check draining + WaitGroup.Add(1)} MUST be
		// indivisible w.r.t. clusterShutdownOrdered's {set draining}, else a callback that read
		// draining==false could be preempted, the shutdown could set draining + WaitTransferAudit
		// (counter 0, returns), and the callback could THEN Add(1)+spawn an un-waited goroutine
		// (leak + lost audit). transferAuditMu guards both sides; a bare atomic cannot.
		b.transferAuditMu.Lock()
		if b.transferAuditDraining.Load() {
			b.transferAuditMu.Unlock()
			runForward(payload, rec)
			return
		}
		b.transferAuditWG.Add(1)
		b.transferAuditMu.Unlock()
		go func() {
			defer b.transferAuditWG.Done()
			runForward(payload, rec)
		}()
	}
}

// WaitTransferAudit blocks until every in-flight async transfer-audit forward has returned.
// TEST-ONLY: the d8 harness calls it before its NumGoroutine/fd leak assertion so a forward
// goroutine in flight is never miscounted as a leak.
func (b *Broker) WaitTransferAudit() { b.transferAuditWG.Wait() }
