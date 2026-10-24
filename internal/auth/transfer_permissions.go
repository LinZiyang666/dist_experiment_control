package auth

import "github.com/nats-io/jwt/v2"

// SessionRemovalToken narrows an ordinary ctl CONNECT to owner-only removal.
// It is a public intent marker, not a credential: the NKey and SQL owner row
// still authorize it. v0.6.0 ignores the token for an existing ACTIVE member,
// preserving old ordinary rm without interpreting an unknown connection role.
// origin: S7 internal review rv-n1compat F4
const SessionRemovalToken = "session-removal"

// Transfer ACL construction stays below auth: cluster imports auth and must
// never acquire the NATS client through xferauth's runtime contracts.
// origin: hot-update S7 plan §6.3 / T31
func transferMemberPermissions(p jwt.Permissions, actor, sid string) jwt.Permissions {
	p.Pub.Allow = append(p.Pub.Allow, jwt.Permission{Allow: []string{
		// Census/config belong to brokers. Keep each grant explicit so the
		// ACL/subscriber scanner can reconcile the actual operation set.
		// origin: hot-update S7 plan §6.3 / T31
		// Sequence readback uses the stream root; .> covers only last-by-subject.
		// origin: hot-update S7 plan §6.3 / S7 INTEG-O authenticated chunk receipt
		"$JS.API.DIRECT.GET.OBJ_xfer-" + sid,
		subjectPrefix + ".ctrl.by." + actor + ".s." + sid + ".node.*.transfer.*.probe.req",
		subjectPrefix + ".ctrl.by." + actor + ".s." + sid + ".node.*.transfer.*.permit.req",
		subjectPrefix + ".ctrl.by." + actor + ".s." + sid + ".node.*.transfer.*.status.req",
		subjectPrefix + ".ctrl.by." + actor + ".s." + sid + ".node.*.transfer.*.source.req",
		subjectPrefix + ".ctrl.by." + actor + ".s." + sid + ".node.*.transfer.*.reserve.req",
		subjectPrefix + ".ctrl.by." + actor + ".s." + sid + ".node.*.transfer.*.terminal.req",
		subjectPrefix + ".ctrl.by." + actor + ".s." + sid + ".node.*.transfer.*.cancel.req",
	}}.Allow...)
	return p
}

func transferAgentEvents(sid, nid string) []string {
	return []string{
		// Retain legacy event depths while closing the new request namespace.
		// The old trailing > also authorized broker-only census/permit verbs.
		// origin: hot-update S7 plan §6.3 / T31
		// Sequence readback uses the stream root; .> covers only last-by-subject.
		// origin: hot-update S7 plan §6.3 / S7 INTEG-O authenticated chunk receipt
		"$JS.API.DIRECT.GET.OBJ_xfer-" + sid,
		subjectPrefix + ".s." + sid + ".ev.node." + nid + ".state",
		subjectPrefix + ".s." + sid + ".ev.node." + nid + ".proc.*.*",
		subjectPrefix + ".s." + sid + ".ev.node." + nid + ".proxy.*",
		subjectPrefix + ".s." + sid + ".ev.node." + nid + ".transfer.*.*",
		subjectPrefix + ".s." + sid + ".ev.node." + nid + ".transfer.*.source.req",
		subjectPrefix + ".s." + sid + ".ev.node." + nid + ".transfer.*.reserve.req",
		subjectPrefix + ".s." + sid + ".ev.node." + nid + ".transfer.*.terminal.req",
		subjectPrefix + ".s." + sid + ".ev.node." + nid + ".transfer.*.cancel.req",
	}
}

// PermissionsForTransferSettlement grants only one retained entry and the
// receiver's private reply subtree. Legacy/shared inbox access is never part
// of this new role, even if a client omits the modern inbox marker.
// origin: hot-update S7 plan §6.3–§6.4 / T31
func PermissionsForTransferSettlement(receiver, sid, nid, transferID string) jwt.Permissions {
	prefix := subjectPrefix + ".ctrl.by." + receiver + ".s." + sid + ".node." + nid + ".transfer." + transferID + "."
	return jwt.Permissions{
		Pub: jwt.Permission{Allow: []string{prefix + "status.req", prefix + "terminal.req", prefix + "cancel.req"}},
		Sub: inboxPermission(receiver, nil, false),
	}
}

// A fresh rm process needs to reach the tombstone owner without regaining
// ordinary membership. The health probe preserves rm's existing alert gate.
// origin: hot-update S7 plan §6.4 / lead LQ1 / T31
func PermissionsForSessionRemoval(actor, sid string) jwt.Permissions {
	return jwt.Permissions{
		Pub: jwt.Permission{Allow: []string{
			subjectPrefix + ".ctrl.by." + actor + ".session." + sid + ".rm.req",
			subjectPrefix + ".ctrl.by." + actor + ".cluster-health.req",
		}},
		Sub: inboxPermission(actor, nil, false),
	}
}
