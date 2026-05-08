package proto

import "fmt"

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
