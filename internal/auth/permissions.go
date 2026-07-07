package auth

import "github.com/nats-io/jwt/v2"

// subjectPrefix is duplicated from internal/proto.SubjectPrefix to avoid
// pulling proto into internal/auth (and through it the ed25519 / jwt
// chain into proto's identifier validation). Kept in sync via the static
// guard test in this package (TestSubjectPrefixInSyncWithProto). This is the
// one legitimate off-SSOT copy of the version prefix (the D0 tripwire
// whitelists it).
const subjectPrefix = "tether.v2"

// PermissionsForUnactivated returns the permissions a CLI gets after
// authenticating but before activating any session. Architecture B.2.
//
// `actor` must be the NATS user nkey public key bound to this connection;
// callers (auth_callout in P3+) write this directly into the JWT so the
// `by.<actor>` segment of every pub allow is locked to the connection's
// real identity — that's the unforgeability invariant in B.2 / C.4.
func PermissionsForUnactivated(actor string) jwt.Permissions {
	return jwt.Permissions{
		Pub: jwt.Permission{Allow: []string{
			subjectPrefix + ".ctrl.by." + actor + ".session.create.req",
			subjectPrefix + ".ctrl.by." + actor + ".session.list.req",
			// G3 #17: roster-pull. refreshCtlEndpoints fires on EVERY expandable connect — including
			// unactivated ones (session list / login) — so an unactivated ctl must be able to pub it or
			// the refresh would silently fail on a NATS permission violation. The reply is discovery-only
			// public topology (zero secrets) served from the broker's O(1) pre-signed manifestBytes()
			// cache, so the widened surface is a memcpy, not a signing amplifier. Actor-scoped
			// (unforgeable) and under ctrl.by.<actor>.* — §13.8 (member denied cluster.*) stays green.
			subjectPrefix + ".ctrl.by." + actor + ".cluster-roster.req",
			"_INBOX.>",
		}},
		Sub: jwt.Permission{Allow: []string{
			subjectPrefix + ".ctrl.version.announce",
			subjectPrefix + ".sys.events",
			"_INBOX.>",
		}},
	}
}

// PermissionsForActivatedMember returns permissions for a CLI that has
// activated session sid as either owner or member. Owner-only operations
// (session rm / kick / rotate-pin) are pub-allowed at the NATS layer for
// every member; tetherd performs the owner check on the application side
// and replies admin_denied for non-owners (see architecture B.2 note).
//
// JetStream API permissions are scoped to this session's `history-<sid>`
// stream only. Members need to publish to `$JS.API.STREAM.INFO.<stream>`,
// `$JS.API.CONSUMER.CREATE/INFO/DELETE/MSG.NEXT.<stream>.>` so the
// nats-go jetstream client can look up the stream and create an
// ephemeral OrderedConsumer for `tether history`. Without these the
// client gets "permissions violation" before any business logic runs.
//
// File transfer (P11): adds the literal OBJ_xfer-<sid> JS subjects (no
// STREAM.CREATE/DELETE/PURGE — broker owns bucket lifecycle), the
// caps.req probe, and the pull-receiver finalize subject. Wildcards
// on the transfer-id segment are sid-bound by the JWT and
// transfer-id-bound by broker application logic
// (`internal/broker/transfer_finalize.go`); see file-transfer-plan
// §Auth.
func PermissionsForActivatedMember(actor, sid string) jwt.Permissions {
	return jwt.Permissions{
		Pub: jwt.Permission{Allow: []string{
			subjectPrefix + ".ctrl.by." + actor + ".session.create.req",
			subjectPrefix + ".ctrl.by." + actor + ".session.list.req",
			// D8b (§10) member-reachable cluster health + alert RPCs. The SUBJECTS are
			// cluster-wide (actor-scoped, no sid — a member queries any reachable broker), but
			// the GRANT lives in the ACTIVATED-member template: a member operates within a
			// session, and every destructive op the gate guards (session rm / expose rm / …)
			// itself requires a session, so an activated-member scope is sufficient (and
			// tighter than granting an unactivated CLI). Deliberately UNDER ctrl.by.<actor>.* —
			// NOT broker-only tether.v2.cluster.* (the §13.8 negative test that a member cannot
			// pub cluster.apply.* stays green; a positive test asserts member reach to these).
			subjectPrefix + ".ctrl.by." + actor + ".cluster-health.req",
			subjectPrefix + ".ctrl.by." + actor + ".cluster-roster.req", // G3 #17 roster-pull (same rationale as unactivated)
			subjectPrefix + ".ctrl.by." + actor + ".alert.ls.req",
			subjectPrefix + ".ctrl.by." + actor + ".alert.ack.req",
			subjectPrefix + ".ctrl.by." + actor + ".session." + sid + ".rm.req",
			subjectPrefix + ".ctrl.by." + actor + ".session." + sid + ".kick.req",
			subjectPrefix + ".ctrl.by." + actor + ".session." + sid + ".rotate-pin.req",
			subjectPrefix + ".ctrl.by." + actor + ".s." + sid + ".ps.req",
			subjectPrefix + ".ctrl.by." + actor + ".s." + sid + ".node.list.req",
			subjectPrefix + ".ctrl.by." + actor + ".s." + sid + ".node.*.tag.req",
			subjectPrefix + ".ctrl.by." + actor + ".s." + sid + ".caps.req",
			subjectPrefix + ".ctrl.by." + actor + ".s." + sid + ".transfer.*.finalize.req",
			// P13 proxy subscription control (owner-only enforced at the
			// app layer; member-readable status). Fixed-token literals,
			// pinned to this actor + sid. The keyset push to agents rides
			// the existing s.<sid>.cmd.node.*.*.req.forwarded wildcards
			// (broker-pub / agent-sub) — no ctl pub permission for it.
			subjectPrefix + ".ctrl.by." + actor + ".s." + sid + ".proxy.set.req",
			subjectPrefix + ".ctrl.by." + actor + ".s." + sid + ".proxy.status.req",
			subjectPrefix + ".ctrl.by." + actor + ".s." + sid + ".proxy.sub.create.req",
			subjectPrefix + ".ctrl.by." + actor + ".s." + sid + ".proxy.sub.list.req",
			subjectPrefix + ".ctrl.by." + actor + ".s." + sid + ".proxy.sub.revoke.req",
			subjectPrefix + ".s." + sid + ".cmd.by." + actor + ".node.*.*.req",
			subjectPrefix + ".s." + sid + ".pty.*.in",
			subjectPrefix + ".s." + sid + ".pty.*.resize",
			subjectPrefix + ".s." + sid + ".pty.*.attach",
			"$JS.API.STREAM.INFO.history-" + sid,
			"$JS.API.CONSUMER.CREATE.history-" + sid,
			"$JS.API.CONSUMER.CREATE.history-" + sid + ".>",
			"$JS.API.CONSUMER.INFO.history-" + sid + ".>",
			"$JS.API.CONSUMER.DELETE.history-" + sid + ".>",
			"$JS.API.CONSUMER.MSG.NEXT.history-" + sid + ".>",
			// File transfer Tier-B (Object Store) — single per-session
			// bucket xfer-<sid>; per-transfer scoping is done via
			// object key (transfer_id). Subjects below are all
			// literal stream names + valid `>` tail wildcards;
			// no partial-token wildcards (which NATS does not
			// support — see proto.XferBucketName comment).
			"$JS.API.STREAM.INFO.OBJ_xfer-" + sid,
			"$JS.API.STREAM.MSG.GET.OBJ_xfer-" + sid,
			"$JS.API.DIRECT.GET.OBJ_xfer-" + sid + ".>",
			"$JS.FC.OBJ_xfer-" + sid + ".>",
			"$JS.API.CONSUMER.CREATE.OBJ_xfer-" + sid,
			"$JS.API.CONSUMER.CREATE.OBJ_xfer-" + sid + ".>",
			"$JS.API.CONSUMER.INFO.OBJ_xfer-" + sid + ".>",
			"$JS.API.CONSUMER.DELETE.OBJ_xfer-" + sid + ".>",
			"$JS.API.CONSUMER.MSG.NEXT.OBJ_xfer-" + sid + ".>",
			"$O.xfer-" + sid + ".M.>",
			"$O.xfer-" + sid + ".C.>",
			"_INBOX.>",
		}},
		Sub: jwt.Permission{Allow: []string{
			subjectPrefix + ".ctrl.version.announce",
			subjectPrefix + ".s." + sid + ".ev.>",
			subjectPrefix + ".s." + sid + ".audit.>",
			subjectPrefix + ".s." + sid + ".pty.*.out",
			subjectPrefix + ".s." + sid + ".pty.*.ready",
			// Node heartbeat — read-only liveness signal used by
			// `tether run` to detect agent disappearance during an
			// interactive PTY (otherwise raw mode + no exit chunk
			// = terminal wedged forever). Wildcard nid because a
			// ctl session may run against multiple nodes.
			subjectPrefix + ".ctrl.s." + sid + ".node.*.heartbeat",
			subjectPrefix + ".sys.events",
			// Object-store data subjects: ctl Get reads from these.
			"$O.xfer-" + sid + ".M.>",
			"$O.xfer-" + sid + ".C.>",
			"_INBOX.>",
		}},
	}
}

// PermissionsForAgent returns permissions for an agent in session sid with
// node id nid. agents have NO access to `audit.*` (audit is tetherd-single-
// writer per C.1 §4) and only see their own node's `cmd.*.req.forwarded`.
//
// File transfer (P11): adds the literal OBJ_xfer-<sid> JS subjects so agent can
// Put (push receiver: Get from bucket; pull sender: Put into bucket).
// No STREAM.CREATE/DELETE/PURGE — broker owns bucket lifecycle. The
// existing `ev.node.<nid>.>` wildcard already covers
// `ev.node.<nid>.transfer.<id>.<kind>` (push receiver-side finalize),
// no addition needed.
func PermissionsForAgent(sid, nid string) jwt.Permissions {
	return jwt.Permissions{
		Pub: jwt.Permission{Allow: []string{
			subjectPrefix + ".ctrl.s." + sid + ".node." + nid + ".register.req",
			subjectPrefix + ".ctrl.s." + sid + ".node." + nid + ".unregister.req",
			subjectPrefix + ".ctrl.s." + sid + ".node." + nid + ".heartbeat",
			subjectPrefix + ".s." + sid + ".ev.node." + nid + ".>",
			subjectPrefix + ".s." + sid + ".pty.*.out",
			subjectPrefix + ".s." + sid + ".pty.*.ready",
			subjectPrefix + ".s." + sid + ".pty.*.failed",
			// File transfer Tier-B Object Store — single per-session
			// bucket; same shape as the activated-member template.
			"$JS.API.STREAM.INFO.OBJ_xfer-" + sid,
			"$JS.API.STREAM.MSG.GET.OBJ_xfer-" + sid,
			"$JS.API.DIRECT.GET.OBJ_xfer-" + sid + ".>",
			"$JS.FC.OBJ_xfer-" + sid + ".>",
			"$JS.API.CONSUMER.CREATE.OBJ_xfer-" + sid,
			"$JS.API.CONSUMER.CREATE.OBJ_xfer-" + sid + ".>",
			"$JS.API.CONSUMER.INFO.OBJ_xfer-" + sid + ".>",
			"$JS.API.CONSUMER.DELETE.OBJ_xfer-" + sid + ".>",
			"$JS.API.CONSUMER.MSG.NEXT.OBJ_xfer-" + sid + ".>",
			"$O.xfer-" + sid + ".M.>",
			"$O.xfer-" + sid + ".C.>",
			"_INBOX.>",
		}},
		Sub: jwt.Permission{Allow: []string{
			subjectPrefix + ".s." + sid + ".cmd.node." + nid + ".*.req.forwarded",
			subjectPrefix + ".s." + sid + ".pty.*.in",
			subjectPrefix + ".s." + sid + ".pty.*.resize",
			subjectPrefix + ".s." + sid + ".pty.*.attach",
			// Agent must receive sys.events to react to admin evict
			// (architecture P9 / I.2b — agent self-shutdown on
			// agent_evicted), session_deleting (refuse calls in
			// the deleting window), and disk_pressure broadcasts.
			// Without this NATS rejects the subscribe and those
			// runtime signals never reach the agent.
			subjectPrefix + ".sys.events",
			// Object-store data subjects: agent Get reads from these
			// (push receiver: get the file ctl uploaded; pull sender:
			// not needed for sub but nats.go ObjectStore Watch path
			// expects sub on metadata).
			"$O.xfer-" + sid + ".M.>",
			"$O.xfer-" + sid + ".C.>",
			"_INBOX.>",
		}},
	}
}

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
			"_INBOX.>",
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
			"_INBOX.>",
		}},
	}
}
