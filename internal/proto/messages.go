package proto

import "time"

// SessionCreateReq — ctl pub on ctrl.by.<A>.session.create.req.
type SessionCreateReq struct {
	Name string `json:"name"`
	// PIN is required (must be ASCII-printable). Empty PIN is rejected by
	// the broker with code "pin_invalid". Server-side random PIN generation
	// is a future feature; for v1 the caller supplies it.
	PIN string `json:"pin,omitempty"`
}

// SessionCreateResp — tetherd reply.
type SessionCreateResp struct {
	SID       string    `json:"sid"`
	OwnerFP   string    `json:"owner_fp"`
	CreatedAt time.Time `json:"created_at"`
	Error     string    `json:"error,omitempty"`
}

// NodeRegisterReq — agent pub on ctrl.s.<S>.node.<N>.register.req.
// First field is proto_version per J.2 strict same-version handshake.
// More reconciliation fields (boot_id, local_processes[], local_ports[]) land in P8.
type NodeRegisterReq struct {
	ProtoVersion   int    `json:"proto_version"`
	ReleaseVersion string `json:"release_version"`
	NID            string `json:"nid"`
	OS             string `json:"os"`
	Arch           string `json:"arch"`
	BootID         string `json:"boot_id,omitempty"`
}

// NodeRegisterResp — tetherd reply with reconciliation directives (G.1).
type NodeRegisterResp struct {
	OK    bool   `json:"ok"`
	Code  string `json:"code,omitempty"`  // e.g. "proto_mismatch", "session_not_found_or_deleting"
	Error string `json:"error,omitempty"` // human-readable; populated when OK=false
	// Reconciliation arrays land in P8.
}

// HeartbeatPayload — agent core pub on ctrl.s.<S>.node.<N>.heartbeat (no reply).
type HeartbeatPayload struct {
	Ts time.Time `json:"ts"`
}

// ErrorReply is the canonical shape for any req that responds with an error
// instead of its normal success body. Code is machine-readable; Message is for
// humans.
type ErrorReply struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// SessionListReq — ctl pub on ctrl.by.<A>.session.list.req. Empty body.
type SessionListReq struct{}

// SessionEntry is the read-side projection of one session row in list/info
// responses (no pin_hash, never).
type SessionEntry struct {
	SID       string    `json:"sid"`
	Name      string    `json:"name"`
	OwnerFP   string    `json:"owner_fp"`
	State     string    `json:"state"`
	CreatedAt time.Time `json:"created_at"`
	IsOwner   bool      `json:"is_owner"`
}

type SessionListResp struct {
	Sessions []SessionEntry `json:"sessions"`
	Code     string         `json:"code,omitempty"`
	Error    string         `json:"error,omitempty"`
}

// SessionRmReq — ctl pub on ctrl.by.<A>.session.<sid>.rm.req. Empty body
// (sid is in subject; owner check is server-side).
type SessionRmReq struct{}

type SessionRmResp struct {
	OK    bool   `json:"ok"`
	Code  string `json:"code,omitempty"`  // not_found | already_deleting | not_owner | store_error
	Error string `json:"error,omitempty"`
}

// ExecReq — ctl pub on s.<sid>.cmd.by.<actor>.node.<nid>.exec.req.
//
// Non-interactive remote command. PTY mode (`run`) lands in P5; this is
// the simpler `exec` from P4: no terminal allocation, agent runs argv
// via os/exec and streams stdout/stderr chunks back via the request's
// reply inbox.
type ExecReq struct {
	Argv  []string          `json:"argv"`            // [program, arg1, ...]
	Env   map[string]string `json:"env,omitempty"`   // extra environment for the child
	Cwd   string            `json:"cwd,omitempty"`   // working dir; empty = agent's
	Stdin []byte            `json:"stdin,omitempty"` // small one-shot stdin payload
}

// ExecChunk is what the agent publishes (one or many) on the reply
// inbox of an ExecReq:
//
//   1. exactly one  Kind="started"  with the assigned PID;
//   2. zero or more Kind="stdout" / "stderr" with byte chunks;
//   3. exactly one  Kind="exit"    with the process exit code,
//      OR exactly one Kind="error" if the agent failed to start the
//      process (the Error field is set; ExitCode is meaningless).
//
// CTL streams these to the local terminal until "exit" / "error" arrives.
type ExecChunk struct {
	Kind     string `json:"kind"`               // started | stdout | stderr | exit | error
	PID      string `json:"pid,omitempty"`      // populated on "started"
	Data     []byte `json:"data,omitempty"`     // stdout/stderr bytes
	ExitCode int    `json:"exit_code,omitempty"` // populated on "exit"
	Error    string `json:"error,omitempty"`    // populated on "error"
}

// ProcStartedEvent is what agents publish on
// `s.<sid>.ev.node.<nid>.proc.<pid>.started`. Broker subscribes and
// transcribes to `audit.proc{kind:start}`. (architecture C.1 §5)
type ProcStartedEvent struct {
	PID         string    `json:"pid"`
	Argv        []string  `json:"argv"`
	StartedAt   time.Time `json:"started_at"`
	StartedByFP string    `json:"started_by_fp"` // who via the originating ctl req
}

// ProcExitEvent is what agents publish on
// `s.<sid>.ev.node.<nid>.proc.<pid>.exit`. Broker transcribes to
// `audit.proc{kind:exit}`.
type ProcExitEvent struct {
	PID      string    `json:"pid"`
	ExitCode int       `json:"exit_code"`
	EndedAt  time.Time `json:"ended_at"`
}

// PsReq — empty body. Lives in `ctrl.by.<actor>.s.<sid>.ps.req`.
type PsReq struct{}

// PsEntry describes one running/exited process for `tether ps`.
type PsEntry struct {
	PID         string    `json:"pid"`
	NID         string    `json:"nid"`
	Argv        []string  `json:"argv"`
	StartedAt   time.Time `json:"started_at"`
	EndedAt     time.Time `json:"ended_at,omitempty"`
	Status      string    `json:"status"` // RUNNING | EXITED | LOST
	ExitCode    int       `json:"exit_code,omitempty"`
	StartedByFP string    `json:"started_by_fp,omitempty"`
}

type PsResp struct {
	Processes []PsEntry `json:"processes"`
	Code      string    `json:"code,omitempty"`
	Error     string    `json:"error,omitempty"`
}

