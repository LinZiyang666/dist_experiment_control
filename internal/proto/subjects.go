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

// SubjCtrlBy returns "tether.v1.ctrl.by.<actor>.<leaf>".
// Used for actor-scoped global control messages (session.create/list/...).
func SubjCtrlBy(actor, leaf string) string {
	return fmt.Sprintf("%s.ctrl.by.%s.%s", SubjectPrefix, actor, leaf)
}

// SubjCmdBy returns "tether.v1.s.<sid>.cmd.by.<actor>.node.<nid>.<verb>.req".
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
func SubjPtyAttach(sid, pid string) string {
	return fmt.Sprintf("%s.s.%s.pty.%s.attach", SubjectPrefix, sid, pid)
}
func SubjPtyReady(sid, pid string) string {
	return fmt.Sprintf("%s.s.%s.pty.%s.ready", SubjectPrefix, sid, pid)
}
func SubjPtyFailed(sid, pid string) string {
	return fmt.Sprintf("%s.s.%s.pty.%s.failed", SubjectPrefix, sid, pid)
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

// SubjCtrlSessionJoin returns the per-actor, per-session join subject.
//
// P3 transitional: this subject is here to give first-time PIN join an RPC
// path in v1 without auth_callout. P3.5+ will replace it with the standard
// NATS auth_callout flow over `$SYS.REQ.USER.AUTH`, and this subject will
// be removed (architecture B.1 / E.2 — login is NOT a business subject).
func SubjCtrlSessionJoin(actor, sid string) string {
	return fmt.Sprintf("%s.ctrl.by.%s.session.%s.join.req", SubjectPrefix, actor, sid)
}

// ParseCtrlBy extracts the actor segment from any "tether.v1.ctrl.by.<actor>.<rest...>"
// subject. Returns leaf = the dot-joined remainder after the actor segment.
//
// Trust note: in P3 (no auth_callout) the actor returned here is not yet
// proven to be the connection's identity — NATS doesn't verify it. Callers
// must treat the returned actor as a routing label only, not authoritative
// identity. P3.5 + auth_callout makes this trustworthy by pinning the
// actor segment in the connection's JWT.
func ParseCtrlBy(subject string) (actor, leaf string, ok bool) {
	parts := strings.Split(subject, ".")
	// 0:tether 1:v1 2:ctrl 3:by 4:<actor> 5+:leaf
	if len(parts) < 6 ||
		parts[0] != "tether" || parts[1] != "v1" ||
		parts[2] != "ctrl" || parts[3] != "by" {
		return "", "", false
	}
	return parts[4], strings.Join(parts[5:], "."), true
}

// ParseSidNidFromCtrl extracts (sid, nid) from any ctrl-tree subject of the
// shape "tether.v1.ctrl.s.<sid>.node.<nid>.<rest...>". Returns ok=false when
// the subject doesn't match this layout.
//
// Used by the broker's wildcard subscription handlers
// (".ctrl.s.*.node.*.register.req" / ".heartbeat") to recover the session
// and node identifiers without trusting the message body.
func ParseSidNidFromCtrl(subject string) (sid, nid string, ok bool) {
	parts := strings.Split(subject, ".")
	// 0:tether 1:v1 2:ctrl 3:s 4:<sid> 5:node 6:<nid> 7:<verb> [8:req]
	if len(parts) < 8 ||
		parts[0] != "tether" || parts[1] != "v1" || parts[2] != "ctrl" ||
		parts[3] != "s" || parts[5] != "node" {
		return "", "", false
	}
	return parts[4], parts[6], true
}
