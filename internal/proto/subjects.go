package proto

import (
	"fmt"
	"strings"
)

// Subjects with no actor segment (broker-pub or agent-self).
const (
	SubjVersionAnnounce = SubjectPrefix + ".ctrl.version.announce"
	SubjSysEvents       = SubjectPrefix + ".sys.events"
)

// Cluster control plane (distributed-broker, D3+). These are the SSOT for the
// broker-only inter-broker subjects: the D4 follower→leader write-forwarding verb
// space (cluster.apply.<verb>) and the broader cluster.> namespace. Per §6.2 RF1
// the pub/sub permission for these is granted ONLY to broker nkey AuthUsers
// (auth.PermissionsForBroker); member/agent/unactivated user JWTs never carry them.
// D3 wires the ACL; the actual forwarding publish/subscribe is D4.
const (
	// SubjClusterPrefix is the root of the broker-only cluster namespace.
	SubjClusterPrefix = SubjectPrefix + ".cluster"
	// SubjClusterApplyPrefix is the root of the D4 write-forwarding verb space.
	SubjClusterApplyPrefix = SubjClusterPrefix + ".apply"
	// SubjClusterApplyWildcard / SubjClusterWildcard are the NATS ACL grants.
	SubjClusterApplyWildcard = SubjClusterApplyPrefix + ".>"
	SubjClusterWildcard      = SubjClusterPrefix + ".>"
	// SubjClusterCursor is the BROKER-ONLY (§17) cursor/health probe the leader scatters to
	// all brokers for the broker_down/raft_lag observability. It lives under cluster.> (NOT
	// the member-facing ctrl.by.<actor>.cluster-health.req) because PermissionsForBroker
	// grants the broker nkey pub+sub on cluster.> but NOT on ctrl.by.* — using the member
	// subject would get the leader's publish DENIED (round-1 BLOCKER: empty replies ⇒ a
	// false broker_down for every voter every tick).
	SubjClusterCursor = SubjClusterPrefix + ".cursor.req"
	// SubjClusterTunnelClose is a broker-only best-effort broadcast used after a
	// committed port free/revoke. Any broker may be the old expose home, so every
	// broker tears down its local public listener for the port.
	SubjClusterTunnelClose = SubjClusterPrefix + ".tunnel.close"
)

// SubjClusterApply returns "tether.v2.cluster.apply.<verb>" — the broker-only
// subject a follower publishes a forwarded session-control write to (D4). §4.1.
func SubjClusterApply(verb string) string {
	return fmt.Sprintf("%s.%s", SubjClusterApplyPrefix, verb)
}

// SubjCtrlBy returns "tether.v2.ctrl.by.<actor>.<leaf>".
// Used for actor-scoped global control messages (session.create/list/...).
func SubjCtrlBy(actor, leaf string) string {
	return fmt.Sprintf("%s.ctrl.by.%s.%s", SubjectPrefix, actor, leaf)
}

// SubjCmdBy returns "tether.v2.s.<sid>.cmd.by.<actor>.node.<nid>.<verb>.req".
// Used for ctl-originated per-session commands targeting a node.
// `actor` is the NATS user nkey public key; tetherd extracts it from the
// subject (B.1) rather than trusting message headers.
func SubjCmdBy(sid, actor, nid, verb string) string {
	return fmt.Sprintf("%s.s.%s.cmd.by.%s.node.%s.%s.req",
		SubjectPrefix, sid, actor, nid, verb)
}

// SubjCmdForwarded returns the `.req.forwarded` subject pubbed by tetherd
// after ACL/audit, subscribed by the target agent. ctl is denied pub by JWT
// permissions (architecture C.4).
func SubjCmdForwarded(sid, nid, verb string) string {
	return fmt.Sprintf("%s.s.%s.cmd.node.%s.%s.req.forwarded",
		SubjectPrefix, sid, nid, verb)
}

// Node lifecycle subjects (agent pub).
func SubjNodeRegister(sid, nid string) string {
	return fmt.Sprintf("%s.ctrl.s.%s.node.%s.register.req", SubjectPrefix, sid, nid)
}
func SubjNodeUnregister(sid, nid string) string {
	return fmt.Sprintf("%s.ctrl.s.%s.node.%s.unregister.req", SubjectPrefix, sid, nid)
}
func SubjNodeHeartbeat(sid, nid string) string {
	return fmt.Sprintf("%s.ctrl.s.%s.node.%s.heartbeat", SubjectPrefix, sid, nid)
}

// Per-session events (tetherd or agent pub depending on event kind; see C.1 §5).
func SubjEvNodeState(sid, nid string) string {
	return fmt.Sprintf("%s.s.%s.ev.node.%s.state", SubjectPrefix, sid, nid)
}

// SubjEvProc kind ∈ {started, exit}. Pubbed by agent.
func SubjEvProc(sid, nid, pid, kind string) string {
	return fmt.Sprintf("%s.s.%s.ev.node.%s.proc.%s.%s",
		SubjectPrefix, sid, nid, pid, kind)
}

// SubjEvPort kind ∈ {allocated, revoked, freed}. Pubbed by tetherd.
func SubjEvPort(sid string, port int, kind string) string {
	return fmt.Sprintf("%s.s.%s.ev.port.%d.%s", SubjectPrefix, sid, port, kind)
}

// Per-session audit subjects (tetherd single-writer; architecture C.1 §4).
func SubjAuditCall(sid string) string {
	return fmt.Sprintf("%s.s.%s.audit.call", SubjectPrefix, sid)
}
func SubjAuditProc(sid string) string {
	return fmt.Sprintf("%s.s.%s.audit.proc", SubjectPrefix, sid)
}
func SubjAuditPort(sid string) string {
	return fmt.Sprintf("%s.s.%s.audit.port", SubjectPrefix, sid)
}
func SubjAuditTransfer(sid string) string {
	return fmt.Sprintf("%s.s.%s.audit.transfer", SubjectPrefix, sid)
}

// SubjEvTransfer kind ∈ {complete, failed}. Pubbed by agent (push receiver
// finalization, both tiers). file-transfer-plan §Audit table row "push".
func SubjEvTransfer(sid, nid, transferID, kind string) string {
	return fmt.Sprintf("%s.s.%s.ev.node.%s.transfer.%s.%s",
		SubjectPrefix, sid, nid, transferID, kind)
}

// SubjCtrlTransferFinalize — ctl pub on
// ctrl.by.<actor>.s.<sid>.transfer.<id>.finalize.req. Pull receiver
// finalization (both tiers). file-transfer-plan §Audit row "pull".
// JWT pub allow scopes the wildcard to (sid, actor); broker enforces
// transfer_id ownership application-side.
func SubjCtrlTransferFinalize(actor, sid, transferID string) string {
	return fmt.Sprintf("%s.ctrl.by.%s.s.%s.transfer.%s.finalize.req",
		SubjectPrefix, actor, sid, transferID)
}

// SubjCtrlCaps — ctl pub on ctrl.by.<actor>.s.<sid>.caps.req. Pre-flight
// JetStream-readiness + MaxPayload probe before chooseTier.
func SubjCtrlCaps(actor, sid string) string {
	return fmt.Sprintf("%s.ctrl.by.%s.s.%s.caps.req", SubjectPrefix, actor, sid)
}

// XferBucketName returns the per-SESSION JetStream Object Store bucket
// name "xfer-<sid>". One bucket holds every in-flight transfer for the
// session; per-transfer scoping is done via the object key (= transfer
// id). The backing stream is "OBJ_<bucket>" = "OBJ_xfer-<sid>" per
// nats.go convention. Used by ctl, broker, and agent.
//
// v0.2.2 design change (audit-fix P11 F4): v0.2.0/v0.2.1 used a
// per-transfer bucket name `xfer-<sid>-<id>`. That can't be wildcarded
// in NATS pub/sub permissions because NATS `*` only matches a whole
// dot-separated token, and bucket names disallow dots — so JWT perms
// like `$JS.API.STREAM.INFO.OBJ_xfer-<sid>-*` had a literal `*` and
// matched zero buckets. Per-session buckets fix this: the perm is a
// single literal `$JS.API.STREAM.INFO.OBJ_xfer-<sid>` (or `.>`-suffix
// for consumer paths) — no wildcard needed.
func XferBucketName(sid string) string {
	return "xfer-" + sid
}

// ParseTransferFinalize extracts (actor, sid, transfer_id) from a
// `tether.v2.ctrl.by.<actor>.s.<sid>.transfer.<id>.finalize.req` subject.
// Returns ok=false on layout mismatch.
func ParseTransferFinalize(subject string) (actor, sid, transferID string, ok bool) {
	parts := strings.Split(subject, ".")
	// 0:tether 1:v2 2:ctrl 3:by 4:<actor> 5:s 6:<sid> 7:transfer 8:<id> 9:finalize 10:req
	if len(parts) != 11 ||
		parts[0] != "tether" || parts[1] != SubjectVersionToken ||
		parts[2] != "ctrl" || parts[3] != "by" ||
		parts[5] != "s" || parts[7] != "transfer" ||
		parts[9] != "finalize" || parts[10] != "req" {
		return "", "", "", false
	}
	if ValidateSID(parts[6]) != nil ||
		ValidateActorToken(parts[4]) != nil {
		return "", "", "", false
	}
	return parts[4], parts[6], parts[8], true
}

// ParseEvTransfer extracts (sid, nid, transfer_id, kind) from a
// `tether.v2.s.<sid>.ev.node.<nid>.transfer.<id>.<kind>` subject.
// kind ∈ {complete, failed}.
// origin: line-2 review M14 (moved here from .golangci.yml, where the exemption was pinned to line
// ranges a comment edit could invalidate). This file holds ~40 named subject builders and parsers. They
// ARE structurally identical, and merging them into one parameterised builder would cost grep-ability
// and type safety for nothing.
//
// The directive covers THIS declaration and its partner only, never the file: a third parser matching
// one of them still reports. Getting that property was why the config-file form pinned line ranges.
//
//nolint:dupl // paired with ParseEvProc; argument above.
func ParseEvTransfer(subject string) (sid, nid, transferID, kind string, ok bool) {
	parts := strings.Split(subject, ".")
	// 0:tether 1:v2 2:s 3:<sid> 4:ev 5:node 6:<nid> 7:transfer 8:<id> 9:<kind>
	if len(parts) != 10 ||
		parts[0] != "tether" || parts[1] != SubjectVersionToken ||
		parts[2] != "s" || parts[4] != "ev" ||
		parts[5] != "node" || parts[7] != "transfer" {
		return "", "", "", "", false
	}
	if ValidateSID(parts[3]) != nil || ValidateNID(parts[6]) != nil {
		return "", "", "", "", false
	}
	return parts[3], parts[6], parts[8], parts[9], true
}

// PTY subjects (architecture C.5).
func SubjPtyOut(sid, pid string) string {
	return fmt.Sprintf("%s.s.%s.pty.%s.out", SubjectPrefix, sid, pid)
}
func SubjPtyIn(sid, pid string) string {
	return fmt.Sprintf("%s.s.%s.pty.%s.in", SubjectPrefix, sid, pid)
}
func SubjPtyResize(sid, pid string) string {
	return fmt.Sprintf("%s.s.%s.pty.%s.resize", SubjectPrefix, sid, pid)
}

// NODE-SCOPED `.in` / `.resize` — origin: prerelease audit round 2, I-F6.
//
// The session-scoped forms above are a SESSION wildcard, and every agent in the session
// subscribes to them. So every agent receives a copy of every other node's raw keystroke
// stream and drops it on the pid lookup. That stream is not metadata: it is whatever the
// operator types into an interactive program, which on this fleet demonstrably includes
// passwords typed at a jump host.
//
// The `.ka` analogy the intake's comment draws does not carry — a keepalive is an empty
// beat, `.in` has a payload.
//
// ADDITIVE, and both ends must move for it to help. See PtyNodeHeader for how a new ctl
// keeps an OLD agent working without a new agent processing the same keystroke twice.
func SubjPtyInNode(sid, nid, pid string) string {
	return fmt.Sprintf("%s.s.%s.node.%s.pty.%s.in", SubjectPrefix, sid, nid, pid)
}
func SubjPtyResizeNode(sid, nid, pid string) string {
	return fmt.Sprintf("%s.s.%s.node.%s.pty.%s.resize", SubjectPrefix, sid, nid, pid)
}

// PtyNodeHeader marks a LEGACY-subject pty message that also went out on the
// node-scoped subject. origin: prerelease audit round 2, I-F6.
//
// It is what makes the transition N-1 safe in BOTH directions, which neither subject
// alone can be:
//
//	old ctl → new agent   no header; the legacy subscription handles it, as today.
//	new ctl → old agent   the legacy copy arrives and is processed; the old agent has
//	                      never heard of the header and ignores it.
//	new ctl → new agent   BOTH copies arrive. The node-scoped one is processed; the
//	                      legacy one carries this header and is DROPPED, so a keystroke
//	                      is never delivered twice.
//
// Dropping on the header rather than on "did I also get a node-scoped copy" is
// deliberate: the two subscriptions have no ordering guarantee between them, and a
// keystroke stream cannot be de-duplicated after the fact.
//
// It is NOT a security control — a client could omit it — and it does not need to be:
// omitting it costs the sender nothing but a double keystroke on its own session.
const PtyNodeHeader = "Tether-Pty-Node"

func SubjPtyAttach(sid, pid string) string {
	return fmt.Sprintf("%s.s.%s.pty.%s.attach", SubjectPrefix, sid, pid)
}
func SubjPtyReady(sid, pid string) string {
	return fmt.Sprintf("%s.s.%s.pty.%s.ready", SubjectPrefix, sid, pid)
}
func SubjPtyFailed(sid, pid string) string {
	return fmt.Sprintf("%s.s.%s.pty.%s.failed", SubjectPrefix, sid, pid)
}

// SubjPtyKeepalive (h1 D1) is the ctl-liveness beat for one interactive run:
// ctl → agent direct (the broker is not in the path), empty body, published
// every RunReq.KAIntervalMS. Its ABSENCE is the signal — see the agent's
// ctlLivenessReaper.
func SubjPtyKeepalive(sid, pid string) string {
	return fmt.Sprintf("%s.s.%s.pty.%s.ka", SubjectPrefix, sid, pid)
}

// SubjCtrlPs returns the per-actor, per-session ps subject.
//
// `tether ps` is read-only (no agent forwarding) — the broker answers
// directly out of the SQLite `processes` table. Therefore lives under
// `ctrl.by.<A>.s.<S>.ps.req`, not under `cmd.by.<A>.node.<N>...`.
func SubjCtrlPs(actor, sid string) string {
	return fmt.Sprintf("%s.ctrl.by.%s.s.%s.ps.req", SubjectPrefix, actor, sid)
}

// SubjCtrlNodeList returns the per-actor, per-session node-list
// subject (architecture B.1). Read-only enumeration of the
// `nodes` table — same pattern as SubjCtrlPs, used by
// `tether node upgrade --all` to see ONLINE agents that haven't
// run any process yet (PsReq would miss those).
func SubjCtrlNodeList(actor, sid string) string {
	return fmt.Sprintf("%s.ctrl.by.%s.s.%s.node.list.req", SubjectPrefix, actor, sid)
}

// Session-management subjects (ctl pub, broker handle).
func SubjCtrlSessionCreate(actor string) string {
	return fmt.Sprintf("%s.ctrl.by.%s.session.create.req", SubjectPrefix, actor)
}
func SubjCtrlSessionList(actor string) string {
	return fmt.Sprintf("%s.ctrl.by.%s.session.list.req", SubjectPrefix, actor)
}
func SubjCtrlSessionRm(actor, sid string) string {
	return fmt.Sprintf("%s.ctrl.by.%s.session.%s.rm.req", SubjectPrefix, actor, sid)
}

// D8b (§10) member-reachable, actor-scoped cluster health + alert RPCs. They live UNDER
// ctrl.by.<actor>.* — NOT broker-only tether.v2.cluster.* — so a member JWT can reach them
// (the banner is for everyone; client-synth gating queries any reachable broker). The §13.8
// negative test (member denied cluster.apply.*) stays green; a positive test asserts member
// reach to these. The broker subscribes the wildcards below.
func SubjCtrlClusterHealth(actor string) string {
	return fmt.Sprintf("%s.ctrl.by.%s.cluster-health.req", SubjectPrefix, actor)
}
func SubjCtrlAlertLs(actor string) string {
	return fmt.Sprintf("%s.ctrl.by.%s.alert.ls.req", SubjectPrefix, actor)
}
func SubjCtrlAlertAck(actor string) string {
	return fmt.Sprintf("%s.ctrl.by.%s.alert.ack.req", SubjectPrefix, actor)
}

// SubjCtrlClusterRoster (G3 #17) — the member-reachable, actor-scoped roster-pull: a ctl connected to
// ANY broker requests that broker's signed cluster manifest (roster + seeds) on the live conn, so
// discovery converges from the actually-connected survivor instead of the pinned floor/bootstrap host.
// Deliberately UNDER ctrl.by.<actor>.* (actor-locked, unforgeable) — NOT broker-only tether.v2.cluster.*
// — so the §13.8 negative test (member denied cluster.apply.*) stays green (the leaf token is
// ".cluster-roster." not ".cluster."). The reply is the discovery-only, account-signed manifest (zero
// secrets); the broker serves its pre-signed, rate-limited manifestBytes() cache (no per-request sign).
func SubjCtrlClusterRoster(actor string) string {
	return fmt.Sprintf("%s.ctrl.by.%s.cluster-roster.req", SubjectPrefix, actor)
}

// SubjCtrlClusterUpgrade (G5 #13 W2b) is the account-signed remote-trigger subject the `cluster upgrade`
// orchestrator uses to reach each broker's local reload op + transfer-leader over NATS (so the roll runs
// external to all brokers and re-resolves the leader after each restart). Hyphen-leaf "cluster-upgrade."
// (not ".cluster.") keeps §13.8 (member denied cluster.*) green. The request is account-seed-signed and
// each broker verifies it against its pinned account_pub before acting (the operator's root authority).
func SubjCtrlClusterUpgrade(actor string) string {
	return fmt.Sprintf("%s.ctrl.by.%s.cluster-upgrade.req", SubjectPrefix, actor)
}

// SubjCtrlClusterGrow (G4 §B) is the account-signed remote-trigger subject the `cluster add` grow
// orchestrator uses to reach a broker's lock/approve-join/mesh-cutover/rebalance steps over NATS (so the
// grow runs external to all brokers and re-resolves the leader after each restart the grow causes).
// Hyphen-leaf "cluster-grow." (not ".cluster.") keeps §13.8 (member denied cluster.*) green. The request is
// account-seed-signed and each broker verifies it against its pinned account_pub before acting.
func SubjCtrlClusterGrow(actor string) string {
	return fmt.Sprintf("%s.ctrl.by.%s.cluster-grow.req", SubjectPrefix, actor)
}

// Broker-side wildcard subscriptions for the D8b ctl RPCs (any actor segment). cluster-health
// is broadcast (every broker answers, ctl corroborates); alert.ls uses a queue group (any one
// broker serves the bounded-stale replicated read); alert.ack any broker forwards to leader.
const (
	SubjCtrlClusterHealthWildcard  = SubjectPrefix + ".ctrl.by.*.cluster-health.req"
	SubjCtrlClusterRosterWildcard  = SubjectPrefix + ".ctrl.by.*.cluster-roster.req"  // G3 #17 roster-pull
	SubjCtrlClusterUpgradeWildcard = SubjectPrefix + ".ctrl.by.*.cluster-upgrade.req" // G5 #13 remote reload/transfer trigger
	SubjCtrlClusterGrowWildcard    = SubjectPrefix + ".ctrl.by.*.cluster-grow.req"    // G4 §B remote grow trigger
	SubjCtrlAlertLsWildcard        = SubjectPrefix + ".ctrl.by.*.alert.ls.req"
	SubjCtrlAlertAckWildcard       = SubjectPrefix + ".ctrl.by.*.alert.ack.req"
)

// ParseEvProc extracts (sid, nid, pid, kind) from a process-lifecycle
// event subject `tether.v2.s.<sid>.ev.node.<nid>.proc.<pid>.<kind>`.
// kind ∈ {"started", "exit"}.
// origin: line-2 review M14. ParseEvTransfer above carries the argument for this pair.
//
//nolint:dupl // paired with ParseEvTransfer; ~40 parsers kept named on purpose.
func ParseEvProc(subject string) (sid, nid, pid, kind string, ok bool) {
	parts := strings.Split(subject, ".")
	// 0:tether 1:v2 2:s 3:<sid> 4:ev 5:node 6:<nid> 7:proc 8:<pid> 9:<kind>
	if len(parts) != 10 ||
		parts[0] != "tether" || parts[1] != SubjectVersionToken || parts[2] != "s" ||
		parts[4] != "ev" || parts[5] != "node" || parts[7] != "proc" {
		return "", "", "", "", false
	}
	if ValidateSID(parts[3]) != nil || ValidateNID(parts[6]) != nil {
		return "", "", "", "", false
	}
	return parts[3], parts[6], parts[8], parts[9], true
}

// ParseCmdBy extracts (sid, actor, nid, verb) from any
// `tether.v2.s.<sid>.cmd.by.<actor>.node.<nid>.<verb>.req` subject.
// Returns ok=false when the subject doesn't match this layout OR
// when sid / nid / actor fail their architecture B.5 syntax check
// (audit shard 03 F5 — defense in depth so a malformed token
// doesn't reach handlers as opaque strings).
//
// Same NATS-proven authority for `actor` as ParseCtrlBy (B.2): the JWT
// permissions pin the `by.<A>` segment to the connection's real nkey.
func ParseCmdBy(subject string) (sid, actor, nid, verb string, ok bool) {
	parts := strings.Split(subject, ".")
	// 0:tether 1:v2 2:s 3:<sid> 4:cmd 5:by 6:<actor> 7:node 8:<nid> 9:<verb> 10:req
	if len(parts) != 11 ||
		parts[0] != "tether" || parts[1] != SubjectVersionToken ||
		parts[2] != "s" || parts[4] != "cmd" || parts[5] != "by" ||
		parts[7] != "node" || parts[10] != "req" {
		return "", "", "", "", false
	}
	if ValidateSID(parts[3]) != nil || ValidateNID(parts[8]) != nil ||
		ValidateActorToken(parts[6]) != nil {
		return "", "", "", "", false
	}
	return parts[3], parts[6], parts[8], parts[9], true
}

// ParseCtrlBy extracts the actor segment from any "tether.v2.ctrl.by.<actor>.<rest...>"
// subject. Returns leaf = the dot-joined remainder after the actor segment.
//
// Authority: with the P3 auth_callout in place, the JWT permissions
// pin every connection's allowed `by.<A>` segment to its own real nkey
// (architecture B.2). Therefore an actor parsed out of a subject the
// broker actually receives is NATS-proven — no second-guessing needed
// at the broker layer.
func ParseCtrlBy(subject string) (actor, leaf string, ok bool) {
	parts := strings.Split(subject, ".")
	// 0:tether 1:v2 2:ctrl 3:by 4:<actor> 5+:leaf
	if len(parts) < 6 ||
		parts[0] != "tether" || parts[1] != SubjectVersionToken ||
		parts[2] != "ctrl" || parts[3] != "by" {
		return "", "", false
	}
	return parts[4], strings.Join(parts[5:], "."), true
}

// ParseSidNidFromCtrl extracts (sid, nid) from any ctrl-tree subject of the
// shape "tether.v2.ctrl.s.<sid>.node.<nid>.<rest...>". Returns ok=false when
// the subject doesn't match this layout.
//
// Used by the broker's wildcard subscription handlers
// (".ctrl.s.*.node.*.register.req" / ".heartbeat") to recover the session
// and node identifiers without trusting the message body.
func ParseSidNidFromCtrl(subject string) (sid, nid string, ok bool) {
	parts := strings.Split(subject, ".")
	// 0:tether 1:v2 2:ctrl 3:s 4:<sid> 5:node 6:<nid> 7:<verb> [8:req]
	if len(parts) < 8 ||
		parts[0] != "tether" || parts[1] != SubjectVersionToken || parts[2] != "ctrl" ||
		parts[3] != "s" || parts[5] != "node" {
		return "", "", false
	}
	if ValidateSID(parts[4]) != nil || ValidateNID(parts[6]) != nil {
		return "", "", false
	}
	return parts[4], parts[6], true
}

// ---------------------------------------------------------------------------
// P13 — session-scoped proxy subscription subjects.
//
// Owner/member commands live under the ctrl.by.<actor>.s.<sid> tree (same
// shape as SubjCtrlCaps / SubjCtrlPs — broker answers directly, NOT forwarded
// to an agent). The per-(sid,nid) keyset push to agents reuses the existing
// SubjCmdForwarded builder with verb "proxy-keys" (rides the broker-pub /
// agent-sub .req.forwarded wildcards — zero JWT edits). The proxy-ready ACK
// rides the agent's existing s.<sid>.ev.node.<nid>.> pub permission.
// ---------------------------------------------------------------------------

func SubjCtrlProxySet(actor, sid string) string {
	return fmt.Sprintf("%s.ctrl.by.%s.s.%s.proxy.set.req", SubjectPrefix, actor, sid)
}
func SubjCtrlProxyStatus(actor, sid string) string {
	return fmt.Sprintf("%s.ctrl.by.%s.s.%s.proxy.status.req", SubjectPrefix, actor, sid)
}
func SubjCtrlProxySubCreate(actor, sid string) string {
	return fmt.Sprintf("%s.ctrl.by.%s.s.%s.proxy.sub.create.req", SubjectPrefix, actor, sid)
}
func SubjCtrlProxySubList(actor, sid string) string {
	return fmt.Sprintf("%s.ctrl.by.%s.s.%s.proxy.sub.list.req", SubjectPrefix, actor, sid)
}
func SubjCtrlProxySubRevoke(actor, sid string) string {
	return fmt.Sprintf("%s.ctrl.by.%s.s.%s.proxy.sub.revoke.req", SubjectPrefix, actor, sid)
}

// SubjEvNodeProxyReady is the agent's SS-bind ACK (kind "ready" or "unready").
// Broker subscribes to set/clear nodes.proxy_ready.
func SubjEvNodeProxyReady(sid, nid, kind string) string {
	return fmt.Sprintf("%s.s.%s.ev.node.%s.proxy.%s", SubjectPrefix, sid, nid, kind)
}

// ParseCtrlProxy extracts (actor, sid, action) from a proxy ctrl subject.
// action ∈ {"set","status","sub.create","sub.list","sub.revoke"}. Exact-length
// + token validation per the handlePsReq/handleNodeListReq precedent; callers
// reject ok=false as subject_malformed BEFORE any DB/owner work.
func ParseCtrlProxy(subject string) (actor, sid, action string, ok bool) {
	parts := strings.Split(subject, ".")
	// base: 0:tether 1:v2 2:ctrl 3:by 4:<A> 5:s 6:<sid> 7:proxy ...
	if len(parts) < 10 ||
		parts[0] != "tether" || parts[1] != SubjectVersionToken ||
		parts[2] != "ctrl" || parts[3] != "by" ||
		parts[5] != "s" || parts[7] != "proxy" {
		return "", "", "", false
	}
	if ValidateActorToken(parts[4]) != nil || ValidateSID(parts[6]) != nil {
		return "", "", "", false
	}
	switch {
	case len(parts) == 10 && parts[9] == "req" && (parts[8] == "set" || parts[8] == "status"):
		return parts[4], parts[6], parts[8], true
	case len(parts) == 11 && parts[8] == "sub" && parts[10] == "req" &&
		(parts[9] == "create" || parts[9] == "list" || parts[9] == "revoke"):
		return parts[4], parts[6], "sub." + parts[9], true
	}
	return "", "", "", false
}
