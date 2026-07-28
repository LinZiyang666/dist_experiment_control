package broker

import "testing"

// megaaudit_test.go — regression pins for the C1–C8 mega-audit MAJOR fixes in internal/broker.

// TestMegaAuditMAJ2ProxySubCreateRevokeAreBroadcast: the C5 proxy sub create/revoke leaves are leader-
// only writes and MUST be classified broadcast (so a ≥2-node cluster reaches the leader, not a silent
// follower). list is a queue-grouped RODB read. The original bug registered the WILDCARD
// `.proxy.sub.*.req`, which never matches this per-leaf HasSuffix check → queue-grouped → follower
// silent-drop; the fix registers the concrete leaves below (which DO match).
func TestMegaAuditMAJ2ProxySubCreateRevokeAreBroadcast(t *testing.T) {
	for _, subj := range []string{
		"tether.v2.ctrl.by.actor.s.sid.proxy.sub.create.req",
		"tether.v2.ctrl.by.actor.s.sid.proxy.sub.revoke.req",
	} {
		if !isBroadcastClusterSubject(subj) {
			t.Errorf("%s must be broadcast+leader-only (the leak-once token is minted on the leader)", subj)
		}
	}
	// list stays a queue-grouped read (any broker can answer from RODB).
	if isBroadcastClusterSubject("tether.v2.ctrl.by.actor.s.sid.proxy.sub.list.req") {
		t.Error("proxy.sub.list must remain queue-grouped (RODB read), not broadcast")
	}
	// The WILDCARD must NOT be classified broadcast — proving why registering it (the original bug)
	// silently queue-grouped the whole subscription. The fix is to register the concrete leaves.
	if isBroadcastClusterSubject("tether.v2.ctrl.by.actor.s.sid.proxy.sub.*.req") {
		t.Error("the wildcard subject classifies as broadcast — but cluster registration would never have, the fix is concrete leaves")
	}
}
