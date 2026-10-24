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
	"errors"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
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

// WaitTransferAudit blocks until every in-flight async transfer-audit forward has returned.
// TEST-ONLY: the d8 harness calls it before its NumGoroutine/fd leak assertion so a forward
// goroutine in flight is never miscounted as a leak.
func (b *Broker) WaitTransferAudit() { b.transferAuditWG.Wait() }

// JoinTransferAudit fences future forward borrowers and waits for actual exit
// under the cut's original deadline. On refusal the caller keeps the current
// backend alive; neither a Raft commit nor timeout retires a v1 history duty.
// Single mode has no cluster audit forwards. Legacy test-only sinks have no
// admission owner and cannot be used to certify an image cut.
// origin: hot-update S7 plan §8.2 / T39
func JoinTransferAudit(b *Broker, deadline controlbudget.Deadline) error {
	if _, err := controlbudget.Remaining(deadline); err != nil {
		return err
	}
	if b == nil {
		return controlbudget.ErrInvalidDeadline
	}
	if b.cl == nil {
		if b.transferAuditSink != nil {
			return errors.New("broker: transfer audit join owner is missing")
		}
		return nil
	}
	return b.cl.transferAuditUsers.Fence(deadline)
}

// ResumeTransferAudit reopens the retained owner after a refused planned cut.
// It must be called only after the caller has restored its current bindings.
// origin: hot-update S7 plan §8.2 / T39
func ResumeTransferAudit(b *Broker) {
	if b != nil && b.cl != nil {
		b.cl.transferAuditUsers.Resume()
	}
}

// ResumeStoreUsers is the optional image-thaw hook for retained transfer RPC and
// audit borrowers. It runs after current backend rebinding and before business.
// origin: hot-update S7 plan §8.2 / T39
func (c *brokerImageControl) ResumeStoreUsers() error {
	ResumeTransferAudit(c.broker)
	if c.broker.cl != nil {
		ResumeTransferClusterForward(c.broker.cl.transferRPC)
	}
	return nil
}
