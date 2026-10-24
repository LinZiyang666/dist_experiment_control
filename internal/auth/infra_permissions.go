package auth

import (
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
)

// PermissionsForBroker returns the permissions tetherd's own NATS connection
// uses (broad subscribe for routing + pub for forwarded/ev/audit + auth
// reply on `$SYS._INBOX.>`).
//
// Note the wildcards here are intentional — only tetherd is allowed `s.*.>`
// reach (the broker has cross-session authority). The static guard test
// allow-lists this template for that reason.
func PermissionsForBroker() jwt.Permissions {
	return jwt.Permissions{
		Pub: jwt.Permission{Allow: []string{
			subjectPrefix + ".s.*.cmd.node.*.*.req.forwarded",
			subjectPrefix + ".s.*.ev.>",
			subjectPrefix + ".s.*.audit.>",
			subjectPrefix + ".ctrl.version.announce",
			subjectPrefix + ".sys.events",
			// RF1 broker-only cluster ACL (distributed-broker §6.2). Granted ONLY
			// to broker nkey AuthUsers; the D4 follower publishes a forwarded write
			// here, so the broker needs PUB. Version-prefixed, SSOT in
			// proto.SubjClusterApplyWildcard / proto.SubjClusterWildcard.
			subjectPrefix + ".cluster.apply.>",
			subjectPrefix + ".cluster.>",
			// BOTH INBOX ROOTS — the broker answers pre-cutover and upgraded requesters
			// alike; see the same pair in PermissionsForAgent for why neither may be
			// conditioned on the responder's own vintage.
			//
			// FOR A CLUSTERED BROKER THIS LIST IS ALSO A FILE. natsconf.Render writes it
			// into nats.conf as the static nkey user's permissions block, and only the
			// topology reconciler rewrites that file — a single broker's conf is
			// hand-written per broker-ops §3.4 and carries no permissions block at all,
			// so it is unaffected. The gap that remains is a broker whose conf WAS
			// rendered and then went single (force-single): its file is frozen with the
			// old list and nothing re-renders it. natsinbox.Connect's probe is what turns
			// that from "the broker's replies silently vanish" into a logged, named
			// failure, and TestEveryResponderCanPublishToBothInboxRoots is what stops the
			// list itself from drifting.
			"_INBOX.>",
			InboxRoot + ".>",
			// auth_callout responses (msg.Respond → $SYS._INBOX.<server>.<rand>).
			"$SYS._INBOX.>",
			// JetStream API surface — broker manages history/events
			// streams + xfer Object Stores. `>` here is a real
			// wildcard (matches any tail), and broker has
			// cross-session authority by design (see template
			// docstring).
			"$JS.API.>",
			"$JS.FC.>",
			"$O.>",
		}},
		Sub: jwt.Permission{Allow: []string{
			subjectPrefix + ".ctrl.by.*.>",
			subjectPrefix + ".ctrl.s.*.node.*.register.req",
			subjectPrefix + ".ctrl.s.*.node.*.unregister.req",
			subjectPrefix + ".ctrl.s.*.node.*.heartbeat",
			subjectPrefix + ".s.*.cmd.by.*.node.*.*.req",
			subjectPrefix + ".s.*.ev.>",
			subjectPrefix + ".s.*.pty.*.failed",
			// RF1 broker-only cluster ACL (distributed-broker §6.2). The D4 leader
			// SUBSCRIBES here to receive forwarded writes. Granted ONLY to broker
			// nkey AuthUsers.
			subjectPrefix + ".cluster.apply.>",
			subjectPrefix + ".cluster.>",
			"$SYS.REQ.USER.AUTH",
			// `_INBOX.>` is the broker's LEGACY reply space and it stays: replies to
			// requests the broker itself made before it opted into a private inbox, and
			// anything a pre-cutover peer addresses back to it, still land there.
			// InboxRoot is where its own replies land now — brokerConnectOptions asks
			// nats.go for `_TINBOX.<hash(broker nkey)>.…` — and without this line the
			// broker could not subscribe to its own inbox.
			"_INBOX.>",
			InboxRoot + ".>",
		}},
	}
}

// Infra management is a separate, account-authorized connection role. An
// ordinary member's existing cluster-upgrade grant must never imply this grant.
// The auth owner must establish account authority before selecting this template;
// the targeted handler additionally verifies the signed request and reply route.
// No HostSupport/root IPC subject or wildcard is granted to this connection.
// origin: hot-update S9 plan §7.1 / T47 / M23
func PermissionsForInfraManager(actor, nodeID string) jwt.Permissions {
	if !nkeys.IsValidPublicUserKey(actor) || proto.ValidateClusterNodeID(nodeID) != nil {
		return deniedInfraPermissions()
	}
	return jwt.Permissions{
		Pub: jwt.Permission{Allow: []string{proto.SubjClusterInfra(nodeID)}},
		Sub: inboxPermission(actor, nil, false),
	}
}

// Production may select these templates only when the corresponding direct
// subscription is installed from proto.InfraSubscriptions. Keeping the base
// templates unchanged avoids minting a future agent grant before its subscriber
// has landed; T47 exercises real direct subscriptions and both grant directions.
// origin: hot-update S9 plan §7.1 / T47
func PermissionsForInfraAgent(sid, nid, actor string, legacyInbox bool) jwt.Permissions {
	if proto.ValidateSID(sid) != nil || proto.ValidateNID(nid) != nil || !nkeys.IsValidPublicUserKey(actor) {
		return deniedInfraPermissions()
	}
	p := PermissionsForAgent(sid, nid, actor, legacyInbox)
	p.Sub.Allow = append(p.Sub.Allow, proto.SubjInfraMove(sid, nid))
	return p
}

func PermissionsForInfraBroker() jwt.Permissions {
	p := PermissionsForBroker()
	p.Pub.Allow = append(p.Pub.Allow, subjectPrefix+".ctrl.s.*.node.*.infra-move.req")
	return p
}

// An empty NATS allow list means unrestricted. Malformed selectors therefore
// need explicit deny-all permissions rather than the zero Permissions value.
// origin: hot-update S9 plan §7.1 / T47
func deniedInfraPermissions() jwt.Permissions {
	return jwt.Permissions{Pub: jwt.Permission{Deny: []string{">"}}, Sub: jwt.Permission{Deny: []string{">"}}}
}
