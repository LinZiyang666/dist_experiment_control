// transfer_audit_forward.go is the D8a (§9) BUILD-AND-PROVE mechanism file for routing
// transfer audit (start/complete/failed) through leader Apply as a re-derivable
// OpTransferAudit entry. Everything here is INERT in production: serve.go never calls
// AttachTransferAuditSink, so b.transferAuditSink stays nil and emitTransferAudit
// (transfer.go) is the byte-identical best-effort pubAuditTransfer (cutover=D9).
//
// This file is EXCLUDED from the TestD8ProductionWiresNoCluster guard scan (like home.go
// for D6, audit_publisher.go for D5): it is where the `b.transferAuditSink =` write lives,
// which the guard bans from the SCANNED production files so the seam can never be wired
// there.
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
		if b.transferAuditDraining.Load() {
			runForward(payload, rec)
			return
		}
		b.transferAuditWG.Add(1)
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
