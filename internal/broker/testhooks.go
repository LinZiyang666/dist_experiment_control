package broker

// auditTapForTest, when non-nil, is invoked at the TOP of publishAudit with every
// audit subject + marshaled payload, regardless of the JetStream/core/disconnected
// branch. It is a TEST-ONLY seam (nil in production, negligible nil-check cost;
// mirrors internal/cluster/testhooks.go's applyCommitGate pattern), used by the D4
// ReconcileBatch live-vs-op audit-equivalence test to capture the EXACT bytes the
// live reconcileOnRegister emits — NATS-free — and compare them (as a set) against
// proc.ReplayReconcileAudit over the op-path entry. Unexported, so only in-package
// (internal/broker) tests can set it; never set in production.
var auditTapForTest func(subject string, payload []byte)
