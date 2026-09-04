package auth

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"

	"github.com/nats-io/jwt/v2"
)

// InboxPrefixFor derives this connection's PRIVATE reply-inbox prefix from the NATS
// user nkey bound to it. Clients pass the same value to nats.CustomInboxPrefix, and
// auth_callout computes it independently from the connection's real nkey — so the
// prefix cannot be claimed by anyone else, without a single new wire field.
//
// WHY THIS EXISTS. origin: prerelease audit proto-auth-acl/L1-F1 ≡
// broker-proxy-http/L3-F1, found independently by two lanes and reproduced
// end-to-end. Every client template granted a BARE `Sub "_INBOX.>"`, nothing in the
// tree ever set a custom inbox prefix, and all principals share one NATS account —
// so any connection could subscribe to the whole reply space and read every other
// connection's replies. What flows there is not metadata: the register reply carries
// ProxyDirective{Token, Keys[].Secret} (a tunnel token plus every subscriber's
// Shadowsocks PSK), ProxySubCreateResp carries the print-once /sub bearer token, and
// `tether exec` streams its stdout/stderr straight to the caller's inbox. The
// repository had already written this fact down once, in internal/broker's
// home_delivery.go ("a token is DISCLOSED on the shared bus"), and worked around it
// locally instead of closing it.
//
// THE SHAPE IS LOAD-BEARING, AND IT IS A SEPARATE ROOT — `_TINBOX.`, not a subtree of
// `_INBOX`. The first cut of this function did put it inside `_INBOX` precisely to
// avoid touching any responder's ACL, and that turned out to be unimplementable.
//
// origin: prerelease audit increment 2 internal review, ops-upgrade/L16-F1, reproduced
// by the main process against two real servers. Keeping the modern prefix inside
// `_INBOX` forces a compatibility grant that spans three tokens (`allow _INBOX.*.*`),
// because a pre-cutover nats.go client's response multiplexer subscribes
// `_INBOX.<nuid>.*`. But nats-server admits a SUBSCRIBE by matching its subject against
// the allow list with the subscription's own `*`/`>` treated as literal tokens, so the
// same grant also admits `_INBOX.<victim-hash>.>` — three tokens, wildcarding
// everything below one specific victim. The only bound left was `deny _INBOX.*.*.>`,
// and that deny is installed LAZILY, under a predicate that changed upstream:
//
//	<= v2.12.x  server/client.go  subjectIsSubsetMatch(denyClause, subscription)
//	>= v2.14.0  server/client.go  SubjectsCollide(denyClause, subscription)
//
// `subjectIsSubsetMatch("_INBOX.*.*.>", "_INBOX.<lit>.>")` is FALSE — token 2 is `*` on
// the deny side and a literal on the subscription side — so on every server the fleet
// has ever run the filter was never installed and every four-token reply under that
// hash was delivered. Measured, with these exact lists: 2.10.22 / 2.11.0 / 2.11.9 /
// 2.12.0 DELIVERED, 2.14.0 not-delivered. No deny list fixes this, because a finite
// deny cannot be a subset of `_INBOX.<arbitrary literal>.>`; and no allow list fixes it,
// because the escape shape and the legacy multiplexer's own subject are both three
// tokens and the server cannot tell `*` from `>` in that position. Inside `_INBOX`, on
// those servers, the property is not merely unguarded — it is unachievable.
//
// A separate root makes it a property of the SUBJECT SPACE instead of a property of the
// server's deny bookkeeping: `_INBOX.>` and `_TINBOX.>` are disjoint at token 1, on
// every nats-server that has ever existed, with no deny clause at all. That is why
// the narrowed LegacyInboxAllow and its deny are gone, and a legacy client is simply handed back
// the plain `_INBOX.>` it had before any of this.
//
// THE COST IS REAL AND IT IS PAID IN TWO PLACES, both of them mechanical:
//
//  1. RESPONDERS NEED A MATCHING Pub GRANT. msg.Respond publishes to the requester's
//     inbox, so every principal that answers a request needs `Pub _TINBOX.>` next to
//     its `Pub _INBOX.>`: the agent template below, PermissionsForBroker, and — for a
//     clustered broker — the static nkey user natsconf.Render writes into nats.conf
//     from that same template. A miss here does NOT fail loudly (the server refuses
//     the publish and the reply just vanishes), which is why TestEveryResponderCanPublishToBothInboxRoots
//     pins it rather than leaving it to review.
//  2. AN OLD BROKER DOES NOT GRANT IT. A pre-cutover broker mints `Sub _INBOX.>` and
//     nothing else, so a modern client's `_TINBOX.<a>.<b>.>` subscription is REFUSED
//     there — the one N-1 quadrant this shape costs, and the reason
//     natsinbox.Connect probes its own inbox once and falls back to the shallow
//     prefix when the answer is no. Loud, measured, and self-healing; see that
//     function for why the probe is ordered rather than timed.
//
// 16 hex chars = 64 bits of the nkey's digest. This is an ISOLATION key, not a
// secret: it is derived from a public key and appears in every subject the
// connection publishes a request on. Its job is to be unguessable-enough to not
// collide and, far more importantly, to be UNGRANTABLE to anyone else — which is
// enforced by auth_callout deriving it, never by the client asserting it.
//
// THE ISOLATION UNIT IS THE NKEY, NOT THE MACHINE — origin: prerelease audit round 2,
// A-F5. That distinction is invisible in the common case and load-bearing in one real
// one: the cloned-credential lease feature exists precisely so that many machines can
// run from ONE image with ONE nkey. Every instance of such a clone set therefore
// derives the SAME prefix and shares one inbox subtree, and can read each other's
// replies. That is not a regression this function introduces — those instances already
// share a credential, so they can already impersonate one another outright — but it is
// the reason this comment must not be read as "one connection, one private inbox".
// Two connections are isolated from each other exactly when their nkeys differ.
//
// DEPTH IS NO LONGER LOAD-BEARING, and deleting that dependency is most of the point.
// An earlier draft split `_INBOX.` by token count — "at most three" for legacy, "at
// least four" for modern — and every argument in this file rested on that partition
// holding. It did not hold on the shipped server (see above), and a partition that
// depends on the server's deny bookkeeping is a partition that can be un-held by a
// dependency bump. Under `_TINBOX.` the two spaces differ in their FIRST token, so
// nothing here cares how deep either side goes and no future nats.go inbox shape can
// erode it. The two remaining tokens are hash material, not structure.
//
// THE PREFIX IS PUBLIC. It is sha256 of a public key, so anyone who knows the actor can
// compute it; what they cannot do is get it GRANTED to them, because auth_callout
// derives it from the connection's own nkey rather than from anything the client says.
// That is the entire isolation argument, and it is only as strong as the callout's
// binding of `ConnectOptions.Nkey` to a proven identity — see authcallout.verifyClientNkey,
// which exists because nats-server does NOT verify that field for a key it has no
// static user for.
func InboxPrefixFor(actor string) string {
	sum := sha256.Sum256([]byte(actor))
	h := hex.EncodeToString(sum[:])
	return InboxRoot + "." + h[:8] + "." + h[8:16]
}

// InboxRoot is the top-level subject the per-identity reply inboxes live under. It is
// deliberately NOT `_INBOX`: see InboxPrefixFor for the measurement that forced a
// separate root, and LegacyInboxAllow for what the other root is used for now.
//
// Exported because three things outside this package must agree on it and each of them
// would otherwise hard-code it: natsinbox (which builds the connect options), the
// responder Pub grants below, and the architecture gate that reconciles the two.
const InboxRoot = "_TINBOX"

// inboxSubjectFor is the subscribe grant matching InboxPrefixFor: nats.go's response
// multiplexer subscribes `<prefix>.<nuid>.*`, and JetStream ordered consumers and
// ObjectStore watches derive their delivery subjects from the same prefix, so one
// `>` covers all of them.
func inboxSubjectFor(actor string) string { return InboxPrefixFor(actor) + ".>" }

// IsPrivateInboxSubject reports whether subject lies in the per-identity inbox root, i.e.
// in a subtree that only the connection whose nkey derives it can subscribe to.
//
// origin: prerelease audit external review B-1. It exists so that a RESPONDER can decide,
// per reply, whether the space it is about to publish into is readable by anybody else.
// The N-1 compatibility grant (LegacyInboxAllow) hands `Sub _INBOX.>` to any connection
// that declines to send InboxCapableMarker, and no ACL can narrow that without breaking
// the pre-cutover clients it exists for — an old client picks its own random reply
// subject after connecting, which a per-connection permission issued at CONNECT time
// cannot name. So the shared space cannot be made private, and the only remaining lever
// is to keep long-lived secrets OUT of it.
//
// DELIBERATELY A WHITELIST, NOT `!strings.HasPrefix(subject, "_INBOX.")`. A reply subject
// that is neither root — a future inbox scheme, a service subject someone reuses as a
// reply-to — must read as "not private", because the failure direction of guessing wrong
// is publishing a tunnel token into a space with an unknown reader set.
func IsPrivateInboxSubject(subject string) bool {
	return strings.HasPrefix(subject, InboxRoot+".")
}

// LegacyInboxAllow is the compatibility grant for a client that predates InboxPrefixFor
// and therefore uses nats.go's default `_INBOX.<nuid>` — i.e. exactly what every
// principal held before the per-identity inbox existed.
//
// IT IS DELIBERATELY THE WHOLE `_INBOX.>` SUBTREE, AND THERE IS NO DENY. An earlier
// draft narrowed it to `_INBOX.*` + `_INBOX.*.*` and bounded it with
// `deny _INBOX.*.*.>` so that the modern inbox could live inside `_INBOX` too. That
// construction did not work on any server the fleet runs (InboxPrefixFor documents the
// measurement), and once the modern inbox moved to its own root the narrowing bought
// nothing: everything reachable through this grant is traffic that a pre-cutover binary
// chose to put in the shared space. Narrowing it further would only break the old
// clients it exists for, which is why it is written as the one shape they all work with.
//
// WHAT THIS GRANT COSTS, STATED PLAINLY: within the N-1 window, a connection that
// declines to send auth.InboxCapableMarker can read the replies of every principal that
// has not yet upgraded. That is the residue requirements §6.7 records. It is bounded by
// each principal's own upgrade — not by anybody else's — because a principal that HAS
// upgraded receives its replies under InboxRoot, which this grant cannot name.
var LegacyInboxAllow = []string{"_INBOX.>"}

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
func PermissionsForUnactivated(actor string, legacyInbox bool) jwt.Permissions {
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
			// NO `Pub "_INBOX.>"` — origin: prerelease audit round 2, the ctl half of the
			// L1-F1 surface. A ctl never publishes INTO an inbox: nc.PublishRequest sends
			// to the SERVICE subject and merely names its inbox in the reply field, which
			// needs no permission on the inbox at all. The grant was therefore pure
			// surplus on the one template handed to ANY connection presenting a
			// syntactically valid nkey — i.e. it let the internet publish into every
			// other connection's reply space.
			//
			// Not a live exploit today: with per-identity inboxes the target subject is
			// `_TINBOX.<a>.<b>.<nuid>.<seq>` and the nuid is unguessable. It is
			// removed because it is free to remove, and because "anonymous may publish
			// anywhere in the reply space" becomes live the moment anything makes an
			// inbox predictable — which is exactly how L1-F1 itself arose.
		}},
		Sub: inboxPermission(actor, []string{
			subjectPrefix + ".ctrl.version.announce",
			// NO sys.events, and NO bare `_INBOX.>`. This template is handed to ANY
			// connection presenting a syntactically valid user nkey with the CONNECT
			// name "tether-cli" — no PIN, no membership, no DB lookup at all
			// (authcallout's roleCtlUnactivated arm returns immediately). Whatever it
			// grants is therefore granted to the internet, since the control plane is
			// reachable at wss://<broker>:443 by design.
			//
			//   - sys.events carried session_created{sid, owner_fp, actor},
			//     member_joined{sid, fp}, pin_failed{sid, fp}, agent_evicted{sid, nid}
			//     — i.e. a live feed enumerating every session and owner fingerprint on
			//     the deployment, to an anonymous subscriber. No ctl code path in the
			//     tree ever subscribed to it; only agents do, and they keep their grant
			//     below. origin: prerelease audit proto-auth-acl/L1-F2.
			//   - `_INBOX.>` was the anonymous half of L1-F1: it made every other
			//     connection's replies readable. Replaced by this connection's OWN
			//     inbox subtree, which is derived from its nkey and cannot be pointed
			//     at anybody else.
		}, legacyInbox),
	}
}

// PermissionsForActivatedMember returns permissions for a CLI that has
// activated session sid as either owner or member. Owner-only operations
// (session rm) are pub-allowed at the NATS layer for every member; tetherd
// performs the owner check on the application side (see architecture B.2 note).
//
// Batch-A A4: this doc used to say the same of `kick` / `rotate-pin` and to
// promise that tetherd "replies admin_denied for non-owners". Both halves are
// false — those two verbs have NO handler at all, so nothing can reply
// anything. internal/broker/audit.go:36-39 already records the same finding
// ("session kick/rotate-pin are not implemented"); this doc had not caught up.
// A7 then removed the grants themselves, and TestACLGrantsHaveSubscribers now
// reconciles this template against the broker's live subscription table in both
// directions so the pair cannot drift apart again.
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
func PermissionsForActivatedMember(actor, sid string, legacyInbox bool) jwt.Permissions {
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
			// G5 #13 remote reload/transfer trigger. Publish-only ACL is NOT the real gate — the broker
			// verifies the request's ACCOUNT SIGNATURE against its pinned account_pub before acting, so
			// only the operator holding the cluster account seed is honored. Hyphen-leaf (not cluster.*)
			// keeps §13.8 green; a member that reaches it without a valid signature is refused.
			subjectPrefix + ".ctrl.by." + actor + ".cluster-upgrade.req",
			// G4 §B remote grow trigger. Same rationale as cluster-upgrade: publish-only ACL is defence in
			// depth, NOT the gate — the broker verifies the ACCOUNT SIGNATURE + TargetNode==self + replay
			// skew before acting. Hyphen-leaf keeps §13.8 green.
			subjectPrefix + ".ctrl.by." + actor + ".cluster-grow.req",
			subjectPrefix + ".ctrl.by." + actor + ".alert.ls.req",
			subjectPrefix + ".ctrl.by." + actor + ".alert.ack.req",
			subjectPrefix + ".ctrl.by." + actor + ".session." + sid + ".rm.req",
			// Batch-A A7 removed three grants here whose verbs do not exist:
			// session.<sid>.kick.req, session.<sid>.rotate-pin.req and
			// s.<sid>.node.*.tag.req. No broker ever subscribed to any of them
			// (TestACLGrantsHaveSubscribers now reconciles both directions), and
			// there is no pin rotation anywhere in the repo — `pin_hash` has zero
			// UPDATE sites.
			//
			// The risk was never the unused strings. It was that the next person
			// to implement node tagging would find both the subject AND the grant
			// already in place, read that as "the design was done", and inherit an
			// authorisation nobody reviewed for the feature they were building.
			//
			// Reversible: grants live in issued user JWTs (24h TTL), so this
			// converges as clients re-authenticate and a revert converges back the
			// same way. These subjects match no JetStream stream filter, so an
			// unsubscribed publish was dropped by core NATS, never persisted.
			subjectPrefix + ".ctrl.by." + actor + ".s." + sid + ".ps.req",
			subjectPrefix + ".ctrl.by." + actor + ".s." + sid + ".node.list.req",
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
			// NODE-SCOPED `.in` / `.resize` — origin: prerelease audit round 2, I-F6.
			// A member may address any node in its own session, so the nid stays a
			// wildcard HERE; the narrowing that matters is on the agent's Sub side,
			// which is granted only its own nid.
			subjectPrefix + ".s." + sid + ".node.*.pty.*.in",
			subjectPrefix + ".s." + sid + ".node.*.pty.*.resize",
			// h1 D1: the ctl-liveness keepalive beat (ctl → agent direct).
			subjectPrefix + ".s." + sid + ".pty.*.ka",
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
			// NO `Pub "_INBOX.>"` here either — see PermissionsForUnactivated. The agent
			// and broker templates KEEP it, and legitimately: they are the responders,
			// and msg.Respond publishes to the requester's inbox.
		}},
		Sub: inboxPermission(actor, []string{
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
			// THE INBOX GRANT IS ADDED BY inboxPermission, and which one depends on
			// this connection: this identity's own subtree under InboxRoot for a client
			// carrying auth.InboxCapableMarker, the shared pre-cutover `_INBOX.>` for one
			// that predates it. The two ROOTS are disjoint, which is what lets the legacy
			// grant be handed out on nothing but the client's own say-so.
			//
			// origin: prerelease audit round 2, A-F1. It used to be here, on the
			// reasoning that this template "requires session membership — so what
			// survives is cross-session reading BY AN AUTHORIZED PRINCIPAL, not by a
			// stranger". That reasoning was wrong, and a verifier reproduced the
			// counter-example against a real nats-server: membership costs a stranger
			// ONE unauthenticated request. PermissionsForUnactivated grants
			// `Pub …session.create.req`, handleSessionCreate HAD no admission control (it
			// does now — session.MayCreateSession — which is the fix that came out of this
			// same finding), creating a session makes you its owner, and the next CONNECT
			// mints THIS template. So the wildcard was reachable by anybody on the internet, and
			// with it every other connection's replies: each agent's register reply
			// (tunnel token + every subscriber PSK), the print-once /sub bearer token,
			// and all `tether exec` output.
			//
			// AND THERE IS NO N-1 COST. An earlier draft of this template removed the
			// compatibility grant outright and recorded a deliberate exemption in
			// requirements §6.7, on the reasoning that a ctl picks its own inbox so any
			// version signal it offers is self-asserted and an attacker simply claims to
			// be old. That reasoning assumed the legacy grant is a SUPERSET of the
			// modern one — true only while the modern inbox lived inside the legacy
			// wildcard. It does not any more, so "claims to be old" buys a different
			// space rather than a larger one, and the exemption is withdrawn.
		}, legacyInbox),
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
// legacyInbox says whether this agent predates the per-identity inbox and therefore
// needs the shared pre-cutover `_INBOX.>` instead of a subtree under InboxRoot. The
// caller reads it from auth.InboxCapableMarker on the CONNECT; see inboxPermission for
// why a self-reported answer is safe once the two ROOTS are disjoint. Note this governs
// the Sub side only — the agent's responder Pub grants cover both roots unconditionally.
func PermissionsForAgent(sid, nid, actor string, legacyInbox bool) jwt.Permissions {
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
			// BOTH INBOX ROOTS, and neither is conditioned on `legacyInbox`. An agent is a
			// RESPONDER: msg.Respond publishes to whatever inbox the REQUESTER chose, and
			// the requester's vintage is independent of this agent's. A pre-cutover agent
			// talking to an upgraded broker must still be able to answer into
			// `_TINBOX.…`, and an upgraded agent must still be able to answer a
			// pre-cutover ctl in `_INBOX.…`. Conditioning either one on this connection's
			// own marker is the mistake that would break N-1 in both directions at once —
			// silently, because a refused Respond simply drops the reply.
			"_INBOX.>",
			InboxRoot + ".>",
		}},
		Sub: inboxPermission(actor, []string{
			subjectPrefix + ".s." + sid + ".cmd.node." + nid + ".*.req.forwarded",
			// THE NODE'S OWN nid, not a wildcard — origin: prerelease audit round 2,
			// I-F6. This is the grant that actually removes the fan-out: with it the
			// server delivers a keystroke only to the agent it is addressed to, the
			// same way the forwarded command subject above has always worked.
			subjectPrefix + ".s." + sid + ".node." + nid + ".pty.*.in",
			subjectPrefix + ".s." + sid + ".node." + nid + ".pty.*.resize",
			// The SESSION-scoped forms, kept for the N-1 window so an OLD ctl (which
			// publishes only these) still reaches this agent. They are the fan-out;
			// retiring them is a one-line change once no ctl older than the cutoff is
			// in the fleet, exactly like LegacyInboxAllow above.
			subjectPrefix + ".s." + sid + ".pty.*.in",
			subjectPrefix + ".s." + sid + ".pty.*.resize",
			subjectPrefix + ".s." + sid + ".pty.*.attach",
			// h1 D3: the agent's session-scoped keepalive intake.
			subjectPrefix + ".s." + sid + ".pty.*.ka",
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
			// THE INBOX GRANT IS ADDED BY inboxPermission — this identity's own subtree
			// under InboxRoot (which carries this agent's register reply, i.e. the tunnel
			// token and every subscriber PSK) for a modern agent, the shared pre-cutover
			// `_INBOX.>` for one that predates the prefix.
			//
			// origin: prerelease audit round 2, A-F1. An earlier draft decided this from
			// the release the agent reported on its LAST register, on the reasoning that
			// an attacker cannot influence that row without already holding this
			// (sid,nid)'s provisioning credential. Two things were wrong with it. The
			// row is absent on a first-ever join and the lookup fails TOWARD the legacy
			// grant, which is exactly a stranger's state; and a stranger reaches this
			// template in three steps anyway (create a session — handleSessionCreate had
			// no admission control — then provision an agent against the PIN they just
			// chose). The DB lookup is gone: with the two spaces disjoint it bought
			// nothing that the client's own claim does not.
		}, legacyInbox),
	}
}

// inboxPermission builds the Sub permission for a client template: this connection's
// own subtree under InboxRoot for a modern connection, or the shared pre-cutover
// `_INBOX.>` for one that predates the prefix.
//
// THE TWO ROOTS ARE DISJOINT, which is what makes `legacy` safe to take from the client
// itself (auth.InboxCapableMarker, self-reported). Claiming to be old buys the shared
// space and nothing under InboxRoot; claiming to be modern buys a subtree derived from
// the caller's own nkey and nothing under `_INBOX`. Neither claim reaches an upgraded
// principal's replies.
//
// THERE IS NO Deny CLAUSE ON EITHER BRANCH, and there must not be one. The draft this
// replaced bounded a narrowed legacy allow with `deny _INBOX.*.*.>`; that deny is
// installed lazily by nats-server under a predicate that changed between v2.12 and
// v2.14, so on every server the fleet runs it was never installed at all. Disjoint
// roots need no bookkeeping to stay disjoint. If a future change reintroduces a deny
// here, it is reintroducing that dependency.
//
// `rest` is COPIED rather than appended to in place. Every caller passes a fresh slice
// literal today, so this is not a live bug — it is written this way because an append
// that escapes into its caller's backing array is how one template silently acquires
// another template's grants, and that class of bug is invisible in review.
func inboxPermission(actor string, rest []string, legacy bool) jwt.Permission {
	allow := make([]string, 0, len(rest)+1)
	allow = append(allow, rest...)
	if legacy {
		return jwt.Permission{Allow: append(allow, LegacyInboxAllow...)}
	}
	return jwt.Permission{Allow: append(allow, inboxSubjectFor(actor))}
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

// InboxCapableMarker is the CONNECT-time signal that this client takes its replies in a
// per-identity subtree under InboxRoot and therefore must NOT be given `_INBOX.>`.
//
// It rides ConnectOptions.Username, which is otherwise unused on this deployment: the
// only static `users:` entry a broker's nats.conf carries is the broker's own nkey, and
// every tether principal authenticates by nkey (plus a PIN in Token). A pre-cutover
// broker does not read Username at all, so setting it is invisible to one — it does not
// make a new client WORK against an old broker (the old broker grants only `_INBOX.>`,
// so the deep subscription is refused), it makes the marker HARMLESS there, and
// natsinbox.Connect's probe is what turns that refusal into a fallback.
//
// SELF-REPORTED, AND THAT IS SAFE HERE — which is the whole point of the design and the
// one thing to check before touching any of this. An attacker chooses between two
// DISJOINT ROOTS (see LegacyInboxAllow): claim to be modern and get a subtree derived
// from their own nkey, or claim to be legacy and get `_INBOX.>`, whose only traffic
// belongs to binaries that put their replies there by their own choice. Neither claim
// reaches an upgraded principal's replies. This holds because the roots differ in their
// FIRST token, not because of any allow/deny arithmetic — the previous design's
// arithmetic is exactly what failed on the shipped server.
//
// THE VALUE IS WIRE. It is compared verbatim by a broker against a string chosen by a
// client that may be a different release, so it is frozen by
// internal/proto's wire inventory the same way a message field is: changing it is a
// compatibility break, not a rename.
const InboxCapableMarker = "tether-inbox-v2"
