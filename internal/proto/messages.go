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

	// Capabilities (P13) is the explicit feature set this agent build
	// implements (e.g. "proxy-v1"). The broker gates proxy allocation on the
	// presence of CapProxyV1 here rather than guessing from ReleaseVersion —
	// every source build shares "v0.0.0-dev", so a version string can't prove
	// P13 support (external review round-2 F5). Additive/omitempty.
	Capabilities []string `json:"capabilities,omitempty"`
}

// CapProxyV1 is the capability token an agent advertises when it implements the
// P13 proxy subscription. Absence (any pre-P13 agent, any version) means the
// broker won't allocate a proxy port or push a directive to it.
const CapProxyV1 = "proxy-v1"

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

	// Proxy (P13), when non-nil, is the authoritative per-node proxy
	// directive delivered at register/reconnect time. A POINTER so a
	// proxy-disabled response marshals with NO "proxy" key and stays
	// byte-identical to a pre-P13 NodeRegisterResp (proto stays v1).
	Proxy *ProxyDirective `json:"proxy,omitempty"`
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
	// ProxyGeneration + ProxyEpoch (P13 round-4) report the agent's last
	// SUCCESSFULLY-applied ordering pair. The broker considers a node converged
	// only when BOTH match the current (broker generation, session epoch); on
	// any mismatch — including the SAME epoch under a different generation (two
	// DB snapshots can share an epoch with different keysets) — it re-pushes the
	// current directive. Epoch alone is insufficient to fence that case.
	ProxyGeneration int64 `json:"proxy_generation,omitempty"`
	ProxyEpoch      int64 `json:"proxy_epoch,omitempty"`
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
	Code  string `json:"code,omitempty"` // not_found | already_deleting | not_owner | store_error
	Error string `json:"error,omitempty"`
}

// ExecReq — ctl pub on s.<sid>.cmd.by.<actor>.node.<nid>.exec.req.
//
// Non-interactive remote command: no terminal allocation, agent runs
// argv via os/exec and streams stdout/stderr chunks back via the
// request's reply inbox. For interactive PTY mode use RunReq.
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
//  1. exactly one  Kind="started"  with the assigned PID;
//  2. zero or more Kind="stdout" / "stderr" with byte chunks;
//  3. exactly one  Kind="exit"    with the process exit code,
//     OR exactly one Kind="error" if the agent failed to start the
//     process (the Error field is set; ExitCode is meaningless).
//
// CTL streams these to the local terminal until "exit" / "error" arrives.
type ExecChunk struct {
	Kind     string `json:"kind"`                // started | stdout | stderr | exit | error
	PID      string `json:"pid,omitempty"`       // populated on "started"
	Data     []byte `json:"data,omitempty"`      // stdout/stderr bytes
	ExitCode int    `json:"exit_code,omitempty"` // populated on "exit"
	Error    string `json:"error,omitempty"`     // populated on "error"
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

// NodeListReq — ctl pub on ctrl.by.<actor>.s.<sid>.node.list.req
// (architecture B.1 line 129). Empty body; sid in subject.
//
// Different from PsReq: NodeList returns the nodes themselves
// (independent of whether they've ever run a process), so callers
// like `tether node upgrade --all` can target ALL ONLINE agents
// even on a fresh install. PsReq's process list misses any agent
// that hasn't exec'd anything yet.
type NodeListReq struct{}

// NodeListEntry projects one row of the SQLite `nodes` table.
type NodeListEntry struct {
	NID             string    `json:"nid"`
	Status          string    `json:"status"` // ONLINE | STALE | OFFLINE
	LastHeartbeatAt time.Time `json:"last_heartbeat_at"`
	BootID          string    `json:"boot_id,omitempty"`
	ReleaseVersion  string    `json:"release_version,omitempty"`
	ProtoVersion    int       `json:"proto_version,omitempty"`
}

type NodeListResp struct {
	Nodes []NodeListEntry `json:"nodes"`
	Code  string          `json:"code,omitempty"` // not_a_member | session_not_found_or_deleting | actor_invalid | store_error
	Error string          `json:"error,omitempty"`
}

// PsReq is the JSON body for `ctrl.by.<actor>.s.<sid>.ps.req`.
//
// All fields are optional; older clients (v0.2.7-) that send the
// empty body `{}` decode into the zero value, which on a v0.2.8+
// broker means "active processes only (storage RUNNING rows; the
// handler derives LOST per row from node status), server-side row
// cap". The cap is fixed at the broker (currently 500) — clients
// cannot raise it via Limit, only lower it.
//
// IncludeExited maps to the ctl `-a` flag.
type PsReq struct {
	IncludeExited bool `json:"include_exited,omitempty"`
	Limit         int  `json:"limit,omitempty"`
}

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
	Processes []PsEntry     `json:"processes"`
	Ports     []PsPortEntry `json:"ports,omitempty"`
	Code      string        `json:"code,omitempty"`
	Error     string        `json:"error,omitempty"`
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
//  1. exactly one Kind="ready" with PID + initial Cols/Rows;
//  2. exactly one Kind="started" once attach was received and exec succeeded;
//  3. exactly one terminal chunk:
//     - Kind="exit" with ExitCode (normal end, including non-zero exit),
//     - Kind="failed" with Reason (attach_timeout / exec_failed / ...).
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
	Code  string `json:"code,omitempty"` // pid_unknown | signal_failed | not_a_member | ...
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

	// RemotePort — P12 `--remote-port`: request a SPECIFIC public port
	// instead of the auto lowest-free. 0/omitted = lowest-free (the
	// pre-P12 behavior). `omitempty` is load-bearing: an unset value
	// serializes away entirely, so an old ctl (no field) and a new ctl
	// that omits the flag send byte-identical bodies, and an old broker
	// (which ignores the unknown key) keeps auto-allocating. A nonzero
	// value must fall in the broker's configured band, else the broker
	// rejects with port_out_of_band; if the port is already ALLOCATED
	// the broker rejects with port_taken (hard fail, no auto fallback).
	RemotePort int `json:"remote_port,omitempty"`

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

	Code  string `json:"code,omitempty"` // not_a_member | session_not_found_or_deleting | name_taken | port_exhausted | port_taken | port_out_of_band | ...
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
	Code  string `json:"code,omitempty"` // already_exposed | frpc_failed | local_port_unreachable | ...
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

// UpgradeReq — ctl pub on s.<sid>.cmd.by.<actor>.node.<nid>.upgrade.req
// (architecture J.4 (b) "主动触发"). Only the session owner can issue
// this; the broker rejects non-owner callers before forwarding.
//
// URL is the absolute https:// pointer to the new tether binary
// tarball; SHA256 is the hex digest of the tarball (NOT of the
// extracted binary — agent verifies before extraction). ProtoVersion
// MUST match the current tetherd ProtoVersion: J.4 § proto bump
// requires tetherd-side reinstall (J.3 scenario 2), not this verb.
type UpgradeReq struct {
	URL          string `json:"url"`
	SHA256       string `json:"sha256"`
	ProtoVersion int    `json:"proto_version"`

	// ActorFP — broker-stamped at forward time (same convention
	// as ExecReq.ActorFP). ctl-supplied value is discarded.
	ActorFP string `json:"actor_fp,omitempty"`
}

// UpgradeResp — broker pub on the upgrade.req reply inbox.
// OK=true means tetherd accepted the request and forwarded it to
// the agent; the agent's actual download/verify/restart happens
// asynchronously and surfaces via audit.proc / sys.events.
type UpgradeResp struct {
	OK    bool   `json:"ok"`
	Code  string `json:"code,omitempty"`
	Error string `json:"error,omitempty"`
}

// UpgradeForwardedReq — broker pub on
// s.<sid>.cmd.node.<nid>.upgrade.req.forwarded. Agent uses (URL,
// SHA256) to fetch + verify + atomically replace its own binary.
type UpgradeForwardedReq struct {
	URL     string `json:"url"`
	SHA256  string `json:"sha256"`
	ActorFP string `json:"actor_fp"`
}

// UpgradeForwardedResp — agent pub on the forwarded inbox. OK=true
// means the binary has been verified and atomically renamed into
// place; the agent will exit shortly so its supervisor (systemd
// unit / setsid nohup wrapper) can launch the new binary.
type UpgradeForwardedResp struct {
	OK             bool   `json:"ok"`
	Code           string `json:"code,omitempty"`
	Error          string `json:"error,omitempty"`
	NewVersion     string `json:"new_version,omitempty"`     // best-effort, parsed from tarball
	BinaryReplaced bool   `json:"binary_replaced,omitempty"` // true when chmod+rename succeeded
}

// ─── File transfer (P11 / file-transfer-plan v0.2.0) ───────────────────────
//
// Two tiers (file-transfer-plan §Tier selection):
//   - Tier A (≤ 8 MiB):  InlineData embedded in PushPrepareReq /
//     PullPrepareResp. No JetStream needed; round-trip ends in one
//     request/reply.
//   - Tier B (≤ 2 GiB):  ObjectStore bucket created by broker before
//     forwarding; the receiver Get's the object, the sender Put's it.
//
// Finalization invariant (plan §Audit): broker writes audit.transfer
// {start} from its accepted prepare; broker writes audit.transfer
// {complete|failed} from a signal sent by whichever party RECEIVED
// the bytes. Push: agent emits SubjEvTransfer. Pull: ctl emits
// SubjCtrlTransferFinalize. Both subjects are sid-scoped + ownership-
// checked broker-side so the audit triplet stays correct under
// crashes / forgery attempts.

// PushPrepareReq — ctl pub on s.<sid>.cmd.by.<actor>.node.<nid>.push.req.
// One shape covers both tiers; agent reads Tier and dispatches.
//
// Tier A: InlineData populated; agent SHA-verifies, writes via tmp+rename
// into Path, replies OK; agent then publishes ev.transfer.<id>.complete
// (receiver-finalization). Size MUST equal len(InlineData).
//
// Tier B: InlineData empty; broker has already created bucket Bucket and
// granted the actor Put rights (not in this struct — broker owns
// lifecycle). Agent validates Path against allow_roots, replies OK +
// echoes Bucket/ObjectKey/ChunkSize. ctl then ObjectStore.Puts to Bucket
// keyed by ObjectKey, then sends TransferCommitReq.
type PushPrepareReq struct {
	TransferID string `json:"transfer_id"` // ULID, ctl-generated; broker echoes into audit
	Path       string `json:"path"`        // absolute remote path under one of agent's allow_roots
	Size       int64  `json:"size"`
	SHA256     string `json:"sha256"`                // hex; expected digest after upload
	Force      bool   `json:"force,omitempty"`       // overwrite existing regular file
	Tier       string `json:"tier"`                  // "a" | "b" — chosen by ctl, validated by agent
	InlineData []byte `json:"inline_data,omitempty"` // tier A only (base64 over JSON)
	Bucket     string `json:"bucket,omitempty"`      // tier B only; broker-assigned
	ObjectKey  string `json:"object_key,omitempty"`  // tier B only; broker-assigned
	ChunkSize  int    `json:"chunk_size,omitempty"`  // tier B hint

	// ActorFP — broker-stamped at forward time (same convention as ExecReq.ActorFP).
	// ctl-supplied value is discarded.
	ActorFP string `json:"actor_fp,omitempty"`
}

// PushPrepareResp — agent pub on the push.req reply inbox. For tier A
// this means "file is on disk + SHA verified + renamed into place"; for
// tier B it means "I validated Path + ready for you to Put".
type PushPrepareResp struct {
	OK    bool   `json:"ok"`
	Tier  string `json:"tier,omitempty"` // echoed for paranoia
	Code  string `json:"code,omitempty"` // dst_exists | path_outside_roots | not_a_regular_file | path_parent_missing | sha_mismatch | too_large | tier_invalid | transfer_disabled | jetstream_unavailable | io_error | ...
	Error string `json:"error,omitempty"`
}

// PullPrepareReq — ctl pub on s.<sid>.cmd.by.<actor>.node.<nid>.pull.req.
// Single shape; agent decides tier based on size vs MaxInline. Broker
// pre-creates the tier-B bucket only AFTER seeing the agent's Stat reply,
// because pull-direction ctl is the receiver and the broker must know
// the file size to decide tier. To avoid two round-trips per pull, the
// broker forwards pull.req synchronously and creates the bucket in
// between when agent.Stat indicates tier B (see broker.transfer.go).
type PullPrepareReq struct {
	TransferID string `json:"transfer_id"`
	Path       string `json:"path"`            // absolute remote path
	MaxInline  int64  `json:"max_inline"`      // ctl's tier-A budget (server max_payload aware)
	Force      bool   `json:"force,omitempty"` // overwrite existing local file

	ActorFP string `json:"actor_fp,omitempty"`
}

// PullPrepareResp — agent pub on the pull.req reply inbox.
//
// Tier A: InlineData populated with the entire file contents; ctl
// SHA-verifies, writes locally via tmp+rename, then sends
// SubjCtrlTransferFinalize{kind:complete}.
//
// Tier B: agent has Put the file into Bucket/ObjectKey (broker created
// the bucket synchronously between subscribe and forward); ctl Gets it,
// SHA-verifies, writes locally, then sends finalize.
type PullPrepareResp struct {
	OK         bool   `json:"ok"`
	Tier       string `json:"tier,omitempty"` // a | b
	Size       int64  `json:"size,omitempty"`
	SHA256     string `json:"sha256,omitempty"`      // hex
	InlineData []byte `json:"inline_data,omitempty"` // tier A only
	Bucket     string `json:"bucket,omitempty"`      // tier B only
	ObjectKey  string `json:"object_key,omitempty"`  // tier B only
	Code       string `json:"code,omitempty"`        // path_not_found | path_outside_roots | not_a_regular_file | too_large | transfer_disabled | jetstream_unavailable | io_error | ...
	Error      string `json:"error,omitempty"`
}

// TransferCommitReq — ctl pub on s.<sid>.cmd.by.<actor>.node.<nid>.push-commit.req.
// Tier B push only. ctl signals "ObjectStore.Put completed; please Get,
// SHA-verify, rename into place, then publish ev.transfer.<id>.<kind>".
// Reply is TransferCommitResp{OK} — the bytes-on-disk outcome flows via
// the ev.transfer subject so the broker (subscribed) can write audit
// and delete the bucket without sitting in the request path.
type TransferCommitReq struct {
	TransferID string `json:"transfer_id"`
	Bucket     string `json:"bucket"`
	ObjectKey  string `json:"object_key"`

	ActorFP string `json:"actor_fp,omitempty"`
}

// TransferCommitResp acknowledges that the agent picked up the commit
// and is starting Get; final outcome flows separately on ev.transfer.
type TransferCommitResp struct {
	OK    bool   `json:"ok"`
	Code  string `json:"code,omitempty"` // bucket_unknown | jetstream_unavailable | not_owner | ...
	Error string `json:"error,omitempty"`
}

// TransferEvent — agent pub on s.<sid>.ev.node.<nid>.transfer.<id>.<kind>
// (kind ∈ complete|failed). Push receiver-side finalization for both
// tiers. Broker subscribes to s.*.ev.node.*.transfer.> and writes the
// matching audit.transfer{kind} + deletes the tier-B bucket.
type TransferEvent struct {
	Kind       string `json:"kind"` // complete | failed
	Verb       string `json:"verb"` // push (v1 only emits for push; pull uses TransferFinalize)
	TransferID string `json:"transfer_id"`
	Tier       string `json:"tier,omitempty"`
	Bucket     string `json:"bucket,omitempty"`
	Path       string `json:"path,omitempty"`
	Bytes      int64  `json:"bytes,omitempty"`
	DurationMs int64  `json:"duration_ms,omitempty"`
	Code       string `json:"code,omitempty"` // failed only: sha_mismatch | io_error | object_get_failed | path_outside_roots | ...
	Error      string `json:"error,omitempty"`
}

// TransferFinalize — ctl pub on ctrl.by.<actor>.s.<sid>.transfer.<id>.finalize.req.
// Pull receiver-side finalization for both tiers. Broker validates
// (transfer_id ↔ actor) and (sid via NATS-layer ACL), writes
// audit.transfer{kind}, and (tier B only) deletes the bucket.
type TransferFinalize struct {
	Kind       string `json:"kind"` // complete | failed
	TransferID string `json:"transfer_id"`
	Tier       string `json:"tier,omitempty"` // a | b — broker uses to decide bucket cleanup
	Bucket     string `json:"bucket,omitempty"`
	Path       string `json:"path,omitempty"`
	Bytes      int64  `json:"bytes,omitempty"`
	DurationMs int64  `json:"duration_ms,omitempty"`
	Code       string `json:"code,omitempty"` // failed only: sha_mismatch | object_get_failed | io_error | ...
	Error      string `json:"error,omitempty"`
}

// TransferFinalizeResp — broker ack on the finalize.req reply inbox.
// OK=true means audit was written and bucket cleanup (if tier B) is
// scheduled; OK=false codes ∈ {transfer_unknown, not_owner_or_creator,
// already_finalized, store_error}.
type TransferFinalizeResp struct {
	OK    bool   `json:"ok"`
	Code  string `json:"code,omitempty"`
	Error string `json:"error,omitempty"`
}

// CapsReq — ctl pub on ctrl.by.<actor>.s.<sid>.caps.req. Pre-flight
// probe before chooseTier so the CLI can refuse a tier-B request when
// the broker has no JetStream, or trim a tier-A request to a smaller
// max_payload than the 8 MiB design ceiling. Empty body.
type CapsReq struct{}

// CapsResp — broker pub on the caps.req reply inbox.
type CapsResp struct {
	OK             bool   `json:"ok"`
	JetStreamReady bool   `json:"jetstream_ready"`
	MaxPayload     int64  `json:"max_payload"` // bytes; nc.MaxPayload() at broker side
	BrokerRelease  string `json:"broker_release,omitempty"`
	BrokerProto    int    `json:"broker_proto,omitempty"`
	Code           string `json:"code,omitempty"` // not_a_member | session_not_found_or_deleting | actor_invalid | ...
	Error          string `json:"error,omitempty"`
}

// ---------------------------------------------------------------------------
// P13 — session-scoped proxy subscription. All additive; ProtoVersion stays 1.
// ---------------------------------------------------------------------------

// ProxyKey is one ACTIVE subscriber's Shadowsocks credential pushed to an
// agent. SubID is the subscriber's stable internal id (== ssproxy KeyID, used
// for targeted revocation); Secret is the SS password string.
type ProxyKey struct {
	SubID  string `json:"sub_id"`
	Secret string `json:"secret"`
}

// ProxyDirective is the authoritative per-node proxy state delivered to an
// agent via the register response (join/reconnect) or the per-(sid,nid)
// proxy-keys.req.forwarded push (live delta). Epoch is the broker-DB-monotonic
// keyset version; the agent applies on Epoch != last-applied (not >) so a
// broker DB restore that rewinds Epoch still converges.
//
// SECURITY: this message carries the raw tunnel Token (once) and the SS PSKs.
// It travels ONLY over the register-reply _INBOX and the Agent-only
// .req.forwarded channel — NEVER over sys.events (which members can read).
type ProxyDirective struct {
	Enabled    bool       `json:"enabled"`
	PublicPort int        `json:"public_port,omitempty"`
	Token      string     `json:"token,omitempty"`  // tunnel token (raw, agent-only)
	Cipher     string     `json:"cipher,omitempty"` // locked "chacha20-ietf-poly1305"
	Keys       []ProxyKey `json:"keys,omitempty"`

	// Generation + Epoch form the ORDERING PAIR (round-3). Generation is the
	// broker incarnation id (its process boot time); it increases across broker
	// restarts INCLUDING a DB restore (which requires a restart). Epoch is the
	// per-session keyset version. Every directive source — register reply, live
	// keyset push, and heartbeat repair — carries the SAME pair, and the agent
	// applies on lexicographic (Generation, Epoch) >:
	//   - a higher Generation applies even with a LOWER epoch (DB-restore
	//     convergence);
	//   - within one Generation, a lower epoch is stale (no resurrect-after-OFF).
	// A scalar epoch alone can't distinguish a stale lower reply from an
	// authoritative restore; the generation does.
	Generation int64 `json:"generation,omitempty"`
	Epoch      int64 `json:"epoch,omitempty"`
}

// ProxySetReq — ctl pub on ctrl.by.<A>.s.<sid>.proxy.set.req. Owner-only.
// One message carries both on (Enabled:true) and off (Enabled:false).
type ProxySetReq struct {
	Enabled bool `json:"enabled"`
}

// ProxySetResp — broker reply. AffectedNodes counts the ONLINE P13-capable
// nodes the directive was pushed to (actual readiness shows in proxy status).
type ProxySetResp struct {
	OK            bool   `json:"ok"`
	Enabled       bool   `json:"enabled"`
	AffectedNodes int    `json:"affected_nodes"`
	Code          string `json:"code,omitempty"` // not_owner | not_a_member | session_not_found_or_deleting | subject_malformed | store_error
	Error         string `json:"error,omitempty"`
}

// ProxyStatusReq — ctl pub on ctrl.by.<A>.s.<sid>.proxy.status.req.
// Member-readable (status reveals NO secrets).
type ProxyStatusReq struct{}

// ProxyNodeEntry is one node row in proxy status (NEVER carries a token/psk).
type ProxyNodeEntry struct {
	NID        string `json:"nid"`
	Status     string `json:"status"` // ONLINE | STALE | OFFLINE
	Ready      bool   `json:"ready"`  // agent ACKed SS bind (rendered in /sub)
	PublicHost string `json:"public_host,omitempty"`
	PublicPort int    `json:"public_port,omitempty"`
}

// ProxySubEntry is one subscriber row in status/list (NEVER carries token/psk).
type ProxySubEntry struct {
	Name      string     `json:"name"`
	State     string     `json:"state"` // ACTIVE | REVOKED
	CreatedAt time.Time  `json:"created_at"`
	RevokedAt *time.Time `json:"revoked_at,omitempty"`
}

type ProxyStatusResp struct {
	Enabled      bool             `json:"enabled"`
	Nodes        []ProxyNodeEntry `json:"nodes,omitempty"`
	Subscribers  []ProxySubEntry  `json:"subscribers,omitempty"`
	SubURLPrefix string           `json:"sub_url_prefix,omitempty"` // e.g. "https://broker/sub/"
	Code         string           `json:"code,omitempty"`
	Error        string           `json:"error,omitempty"`
}

// ProxySubCreateReq — ctl pub on ctrl.by.<A>.s.<sid>.proxy.sub.create.req. Owner-only.
type ProxySubCreateReq struct {
	Name string `json:"name"`
}

// ProxySubCreateResp — broker reply. SubURL is printed exactly once (the raw
// token only ever leaves the broker here).
type ProxySubCreateResp struct {
	OK     bool   `json:"ok"`
	Name   string `json:"name,omitempty"`
	SubURL string `json:"sub_url,omitempty"`
	Code   string `json:"code,omitempty"` // sub_name_invalid | sub_name_taken | not_owner | ...
	Error  string `json:"error,omitempty"`
}

// ProxySubListReq — ctl pub on ctrl.by.<A>.s.<sid>.proxy.sub.list.req. Owner-only.
type ProxySubListReq struct{}

type ProxySubListResp struct {
	Subs  []ProxySubEntry `json:"subs,omitempty"`
	Code  string          `json:"code,omitempty"`
	Error string          `json:"error,omitempty"`
}

// ProxySubRevokeReq — ctl pub on ctrl.by.<A>.s.<sid>.proxy.sub.revoke.req. Owner-only.
type ProxySubRevokeReq struct {
	Name string `json:"name"`
}

type ProxySubRevokeResp struct {
	OK    bool   `json:"ok"`
	Name  string `json:"name,omitempty"`
	Code  string `json:"code,omitempty"` // sub_not_found | already_revoked | not_owner | ...
	Error string `json:"error,omitempty"`
}
