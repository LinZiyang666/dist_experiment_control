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
// LocalProcesses / LocalPorts implement architecture G.1: agent's view of
// "what should be live right now" so the broker can converge SQLite
// against the agent's reality on (re)connect.
type NodeRegisterReq struct {
	ProtoVersion   int    `json:"proto_version"`
	ReleaseVersion string `json:"release_version"`
	NID            string `json:"nid"`
	OS             string `json:"os"`
	Arch           string `json:"arch"`
	BootID         string `json:"boot_id,omitempty"`

	LocalProcesses []LocalProcess `json:"local_processes,omitempty"`
	LocalPorts     []LocalPort    `json:"local_ports,omitempty"`
}

// LocalProcess is one entry in NodeRegisterReq.LocalProcesses — the
// agent's view of a managed process's live state.
//
// RC is populated only when State=="exited" and the agent observed
// the rc (otherwise nil → broker treats as missed-exit with rc=-1).
//
// StartedAt + StartTimeTicks make up the (boot_id, pid,
// start_time_ticks) triple per architecture G.1 PID-reuse defense.
// The agent captures /proc/<os_pid>/stat field 22 at fork time and
// echoes it back here. Broker compares against
// processes.start_time_ticks: mismatch → original row treated as
// missed-exit + new pid handled as orphan. Both fields are omitempty
// because exec-style children (sync, not held in a.procs) have no
// way to report them — broker treats those as missed-exit when not
// reported, which is the same outcome the triple check would yield.
type LocalProcess struct {
	PID            string    `json:"pid"`
	State          string    `json:"state"` // "running" | "exited"
	RC             *int      `json:"rc,omitempty"`
	StartedAt      time.Time `json:"started_at,omitempty"`
	StartTimeTicks int64     `json:"start_time_ticks,omitempty"`
}

// LocalPort is one entry in NodeRegisterReq.LocalPorts — the agent's
// view of a live tunnel proxy. TokenHash is SHA256 hex (per F.4
// rule: raw token is agent-only after the initial expose forward;
// what the agent re-presents on register is the SAME hash the broker
// already has in port_allocations.token_hash).
type LocalPort struct {
	Port      int    `json:"port"`
	Name      string `json:"name"`
	LocalPort int    `json:"local_port"`
	TokenHash string `json:"token_hash"`
}

// NodeRegisterResp — tetherd reply with G.1 reconciliation directives.
//
// Agent applies in this order:
//  1. RevokePorts — close tunnel sessions + prune state.json.
//  2. DropProcesses — SIGTERM + 5s + SIGKILL the orphans.
//
// AcceptedProcesses / ReconciledProcesses / KeepPorts are
// informational; the agent may log but isn't required to act.
type NodeRegisterResp struct {
	OK    bool   `json:"ok"`
	Code  string `json:"code,omitempty"`  // e.g. "proto_mismatch", "session_not_found_or_deleting"
	Error string `json:"error,omitempty"` // human-readable; populated when OK=false

	AcceptedProcesses   []string         `json:"accepted_processes,omitempty"`
	ReconciledProcesses []ReconciledProc `json:"reconciled_processes,omitempty"`
	KeepPorts           []int            `json:"keep_ports,omitempty"`
	RevokePorts         []int            `json:"revoke_ports,omitempty"`
	DropProcesses       []string         `json:"drop_processes,omitempty"`
}

// ReconciledProc reports one PID the broker just transitioned away
// from RUNNING/LOST as part of the register reconciliation. NewState
// is always "EXITED" in v1 (architecture G.1 reply payload).
type ReconciledProc struct {
	PID      string `json:"pid"`
	NewState string `json:"new_state"`
	RC       int    `json:"rc"`
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

	// ActorFP is broker-stamped at forward time (broker re-marshals the
	// payload after C.1 §6 / membership checks pass; whatever ctl puts
	// here is discarded). Agent reads it back into ProcStartedEvent so
	// the resulting `processes.started_by_fp` row reflects the
	// broker-parsed `by.<actor>` segment, not agent-supplied data.
	// Architecture C.1 §4 (broker is the audit single-writer) — actor
	// attribution must originate at the broker.
	ActorFP string `json:"actor_fp,omitempty"`
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
//
// BootID + StartTimeTicks land in `processes` so the next G.1 reconcile
// can verify (boot_id, pid, start_time_ticks) without re-querying the
// agent. Both omitempty because non-PTY exec children don't capture
// them in v1 (sync lifecycle, no /proc inspection on the fast path).
type ProcStartedEvent struct {
	PID            string    `json:"pid"`
	Argv           []string  `json:"argv"`
	StartedAt      time.Time `json:"started_at"`
	StartedByFP    string    `json:"started_by_fp"`
	BootID         string    `json:"boot_id,omitempty"`
	StartTimeTicks int64     `json:"start_time_ticks,omitempty"`
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
	Processes []PsEntry      `json:"processes"`
	Ports     []PsPortEntry  `json:"ports,omitempty"`
	Code      string         `json:"code,omitempty"`
	Error     string         `json:"error,omitempty"`
}

// PsPortEntry is the read-side projection of one port_allocations row
// for `tether ps`. Architecture F.8 — the unified view shows both
// processes and ports on the same screen.
type PsPortEntry struct {
	Port        int       `json:"port"`
	Name        string    `json:"name"`
	NID         string    `json:"nid"`
	LocalPort   int       `json:"local_port"`
	State       string    `json:"state"` // ALLOCATED | REVOKED | FREED
	CreatedByFP string    `json:"created_by_fp,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
}

// RunReq — ctl pub on s.<sid>.cmd.by.<actor>.node.<nid>.run.req.
//
// Interactive PTY mode. Architecture C.5 / P5: agent allocates a PTY,
// replies RunChunk{Kind:ready,PID,Cols,Rows}, waits for ctl to publish a
// PtyAttachEvent on s.<sid>.pty.<pid>.attach within 3s, only THEN
// fork+exec's the child with PTY slave bound to its stdio.
//
// Cols/Rows here are ctl's terminal size at run.req time; ctl sends the
// authoritative initial size again in PtyAttachEvent.
type RunReq struct {
	Argv []string          `json:"argv"`
	Env  map[string]string `json:"env,omitempty"`
	Cwd  string            `json:"cwd,omitempty"`
	Cols int               `json:"cols,omitempty"`
	Rows int               `json:"rows,omitempty"`

	// ActorFP — broker-stamped at forward time; same semantics as
	// ExecReq.ActorFP. Whatever ctl supplies is discarded.
	ActorFP string `json:"actor_fp,omitempty"`
}

// RunChunk is what the agent / broker streams back on the run.req reply
// inbox over the lifetime of one run:
//
//   1. exactly one Kind="ready" with PID + initial Cols/Rows;
//   2. exactly one Kind="started" once attach was received and exec succeeded;
//   3. exactly one terminal chunk:
//        - Kind="exit" with ExitCode (normal end, including non-zero exit),
//        - Kind="failed" with Reason (attach_timeout / exec_failed / ...).
//
// PTY byte streams travel on a SEPARATE subject (`pty.<pid>.out`); this
// reply inbox carries only lifecycle events. Two channels keep the byte
// stream untouched by lifecycle bookkeeping (and let broker write
// `audit.proc{kind:attach_timeout}` without sitting in the byte path).
type RunChunk struct {
	Kind     string `json:"kind"`
	PID      string `json:"pid,omitempty"`
	Cols     int    `json:"cols,omitempty"`
	Rows     int    `json:"rows,omitempty"`
	ExitCode int    `json:"exit_code,omitempty"`
	Reason   string `json:"reason,omitempty"`
}

// PtyAttachEvent — ctl pub on s.<sid>.pty.<pid>.attach. Architecture
// C.5.1 step 5: ctl has subscribed to .out / .in / .resize and is now
// telling the agent "you may exec; here is my authoritative initial
// terminal size". Agent fork+exec's only after receiving this.
type PtyAttachEvent struct {
	Cols int `json:"cols"`
	Rows int `json:"rows"`
}

// PtyResizeEvent — ctl pub on s.<sid>.pty.<pid>.resize. Architecture
// C.5.2: ctl SIGWINCH handler emits this whenever the local terminal
// resizes; agent ioctl(TIOCSWINSZ) on the PTY master.
type PtyResizeEvent struct {
	Cols int `json:"cols"`
	Rows int `json:"rows"`
}

// PtyFailedEvent — agent pub on s.<sid>.pty.<pid>.failed. Architecture
// C.5.1: when attach doesn't arrive within 3s, OR when fork+exec itself
// fails after attach. Reason is machine-readable (attach_timeout,
// exec_failed, pty_alloc_failed). Broker subscribes to write
// audit.proc{kind:reason} (the audit kind mirrors the failure shape).
type PtyFailedEvent struct {
	PID    string `json:"pid"`
	Reason string `json:"reason"`
	Detail string `json:"detail,omitempty"`
}

// KillReq — ctl pub on s.<sid>.cmd.by.<actor>.node.<nid>.kill.req.
//
// Used for Ctrl-C semantics in `tether run`: ctl captures local Ctrl-C
// (raw mode swallowed it before the kernel could deliver it to the
// PTY's foreground process group), and sends one of these instead. The
// agent forwards the signal to the child's process group so SIGINT
// propagates to the whole job (e.g. shell + child).
//
// Signal is the conventional UNIX signal NUMBER (2 = SIGINT, 15 = SIGTERM,
// 9 = SIGKILL). v1 only sends SIGINT but the field is open for SIGTERM /
// SIGKILL escalation (P-future).
type KillReq struct {
	PID     string `json:"pid"`
	Signal  int    `json:"signal"`
	ActorFP string `json:"actor_fp,omitempty"` // broker-stamped, same convention as ExecReq.ActorFP
}

// KillResp — agent pub on the kill.req reply inbox.
type KillResp struct {
	OK    bool   `json:"ok"`
	Code  string `json:"code,omitempty"`  // pid_unknown | signal_failed | not_a_member | ...
	Error string `json:"error,omitempty"`
}

// ExposeReq — ctl pub on s.<sid>.cmd.by.<actor>.node.<nid>.expose.req.
//
// `tether expose --local 8888 --name jupyter` packages this and sends.
// broker allocates a public port from the [14000-14999] band, generates
// a 32-byte URL-safe token, persists (port, sha256(token), state=
// ALLOCATED) into port_allocations, and replies with ExposeResp{Port,
// Token, PublicHost, Name}. The same token is then forwarded to the
// agent inside ExposeForwardedReq so frpc can present it to frps.
//
// Architecture D.4 / F.3 / F.4.
type ExposeReq struct {
	Name      string `json:"name"`
	LocalPort int    `json:"local_port"`

	// ActorFP — broker-stamped at forward time; same convention as
	// ExecReq.ActorFP. ctl-supplied value is discarded.
	ActorFP string `json:"actor_fp,omitempty"`
}

// ExposeResp — broker pub on the expose.req reply inbox.
//
// Architecture F.4 storage rule: only the agent ever holds the raw
// tunnel token (in state.json); the broker keeps SHA256 in
// port_allocations. This struct deliberately has no Token field —
// were ctl to receive the raw token, any ctl-side code path could
// register a competing tunnel for the same public port.
type ExposeResp struct {
	Port       int    `json:"port,omitempty"`        // public port assigned (14000-14999)
	PublicHost string `json:"public_host,omitempty"` // operator-friendly URL host (e.g. broker.example.com)
	Name       string `json:"name,omitempty"`        // echoed back

	Code  string `json:"code,omitempty"`  // not_a_member | session_not_found_or_deleting | name_taken | port_exhausted | ...
	Error string `json:"error,omitempty"`
}

// ExposeForwardedReq — broker pub on
// s.<sid>.cmd.node.<nid>.expose.req.forwarded. Agent uses the supplied
// (Port, Token, LocalPort, Name) to add a proxy to its frpc instance
// and persist the entry to ~/.tether/agent/<sid>/state.json so frpc
// can auto-reconnect on agent restart (architecture F.4 storage rule).
type ExposeForwardedReq struct {
	Name      string `json:"name"`
	Port      int    `json:"port"`
	LocalPort int    `json:"local_port"`
	Token     string `json:"token"`
	ActorFP   string `json:"actor_fp"`
}

// ExposeForwardedResp — agent pub on the expose.req.forwarded reply
// inbox. broker waits for this so it can fold "agent agreed to start
// frpc proxy" into the original expose.req reply latency. OK=false means
// agent rejected (already-have-proxy / frpc start failed / etc.) and
// the broker should mark the row FREED (since no traffic will flow).
type ExposeForwardedResp struct {
	OK    bool   `json:"ok"`
	Code  string `json:"code,omitempty"`  // already_exposed | frpc_failed | local_port_unreachable | ...
	Error string `json:"error,omitempty"`
}

// ExposeRmReq — ctl pub on s.<sid>.cmd.by.<actor>.node.<nid>.expose-rm.req.
//
// `tether expose rm --name jupyter` packages this. broker looks up the
// ALLOCATED row by (sid, name), marks it FREED, returns the port to
// the pool, forwards a drop instruction to the agent, and replies OK.
type ExposeRmReq struct {
	Name string `json:"name"`

	ActorFP string `json:"actor_fp,omitempty"` // broker-stamped
}

// ExposeRmResp — broker pub on the expose-rm.req reply inbox.
type ExposeRmResp struct {
	OK    bool   `json:"ok"`
	Port  int    `json:"port,omitempty"` // the port that was just freed (informational)
	Code  string `json:"code,omitempty"` // not_found | not_a_member | ...
	Error string `json:"error,omitempty"`
}

// ExposeRmForwardedReq — broker pub on
// s.<sid>.cmd.node.<nid>.expose-rm.req.forwarded. Agent removes the
// proxy from frpc and prunes the corresponding state.json entry.
type ExposeRmForwardedReq struct {
	Name string `json:"name"`
	Port int    `json:"port"`
}

// PortEvent — broker pub on s.<sid>.ev.port.<port>.<kind>.
// kind ∈ {allocated, revoked, freed}. Members subscribe to react to
// port-state changes (e.g. update local CLI ps cache).
type PortEvent struct {
	Port      int       `json:"port"`
	Name      string    `json:"name,omitempty"`
	NID       string    `json:"nid,omitempty"`
	LocalPort int       `json:"local_port,omitempty"`
	Kind      string    `json:"kind"` // allocated | revoked | freed
	Ts        time.Time `json:"ts"`
}

