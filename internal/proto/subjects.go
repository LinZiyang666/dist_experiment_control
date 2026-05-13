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

// XferBucketName returns the per-transfer JetStream Object Store bucket
// name "xfer-<sid>-<transfer_id>". The backing stream is "OBJ_<bucket>"
// per nats.go convention. Used by ctl, broker, and agent.
func XferBucketName(sid, transferID string) string {
	return "xfer-" + sid + "-" + transferID
}

// XferObjectKey is the single object name inside every xfer bucket. The
// design is one-object-per-bucket so a bucket name uniquely identifies
// the transfer; we only need a stable key for nats.go's ObjectStore.Put
// / .Get calls. file-transfer-plan v0.2.0.
const XferObjectKey = "object"

// ParseTransferFinalize extracts (actor, sid, transfer_id) from a
// `tether.v1.ctrl.by.<actor>.s.<sid>.transfer.<id>.finalize.req` subject.
// Returns ok=false on layout mismatch.
func ParseTransferFinalize(subject string) (actor, sid, transferID string, ok bool) {
	parts := strings.Split(subject, ".")
	// 0:tether 1:v1 2:ctrl 3:by 4:<actor> 5:s 6:<sid> 7:transfer 8:<id> 9:finalize 10:req
	if len(parts) != 11 ||
		parts[0] != "tether" || parts[1] != "v1" ||
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
// `tether.v1.s.<sid>.ev.node.<nid>.transfer.<id>.<kind>` subject.
// kind ∈ {complete, failed}.
func ParseEvTransfer(subject string) (sid, nid, transferID, kind string, ok bool) {
	parts := strings.Split(subject, ".")
	// 0:tether 1:v1 2:s 3:<sid> 4:ev 5:node 6:<nid> 7:transfer 8:<id> 9:<kind>
	if len(parts) != 10 ||
		parts[0] != "tether" || parts[1] != "v1" ||
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
func SubjPtyAttach(sid, pid string) string {
	return fmt.Sprintf("%s.s.%s.pty.%s.attach", SubjectPrefix, sid, pid)
}
func SubjPtyReady(sid, pid string) string {
	return fmt.Sprintf("%s.s.%s.pty.%s.ready", SubjectPrefix, sid, pid)
}
func SubjPtyFailed(sid, pid string) string {
	return fmt.Sprintf("%s.s.%s.pty.%s.failed", SubjectPrefix, sid, pid)
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

// ParseEvProc extracts (sid, nid, pid, kind) from a process-lifecycle
// event subject `tether.v1.s.<sid>.ev.node.<nid>.proc.<pid>.<kind>`.
// kind ∈ {"started", "exit"}.
func ParseEvProc(subject string) (sid, nid, pid, kind string, ok bool) {
	parts := strings.Split(subject, ".")
	// 0:tether 1:v1 2:s 3:<sid> 4:ev 5:node 6:<nid> 7:proc 8:<pid> 9:<kind>
	if len(parts) != 10 ||
		parts[0] != "tether" || parts[1] != "v1" || parts[2] != "s" ||
		parts[4] != "ev" || parts[5] != "node" || parts[7] != "proc" {
		return "", "", "", "", false
	}
	if ValidateSID(parts[3]) != nil || ValidateNID(parts[6]) != nil {
		return "", "", "", "", false
	}
	return parts[3], parts[6], parts[8], parts[9], true
}

// ParseCmdBy extracts (sid, actor, nid, verb) from any
// `tether.v1.s.<sid>.cmd.by.<actor>.node.<nid>.<verb>.req` subject.
// Returns ok=false when the subject doesn't match this layout OR
// when sid / nid / actor fail their architecture B.5 syntax check
// (audit shard 03 F5 — defense in depth so a malformed token
// doesn't reach handlers as opaque strings).
//
// Same NATS-proven authority for `actor` as ParseCtrlBy (B.2): the JWT
// permissions pin the `by.<A>` segment to the connection's real nkey.
func ParseCmdBy(subject string) (sid, actor, nid, verb string, ok bool) {
	parts := strings.Split(subject, ".")
	// 0:tether 1:v1 2:s 3:<sid> 4:cmd 5:by 6:<actor> 7:node 8:<nid> 9:<verb> 10:req
	if len(parts) != 11 ||
		parts[0] != "tether" || parts[1] != "v1" ||
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

// ParseCtrlBy extracts the actor segment from any "tether.v1.ctrl.by.<actor>.<rest...>"
// subject. Returns leaf = the dot-joined remainder after the actor segment.
//
// Authority: with the P3 auth_callout in place, the JWT permissions
// pin every connection's allowed `by.<A>` segment to its own real nkey
// (architecture B.2). Therefore an actor parsed out of a subject the
// broker actually receives is NATS-proven — no second-guessing needed
// at the broker layer.
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
	if ValidateSID(parts[4]) != nil || ValidateNID(parts[6]) != nil {
		return "", "", false
	}
	return parts[4], parts[6], true
}
