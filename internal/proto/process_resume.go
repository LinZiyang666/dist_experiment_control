package proto

// ProcessResumeVerb keeps reliable creation away from legacy exec handlers,
// which decode and re-encode ExecReq and can discard a new deduplication key.
const ProcessResumeVerb = "process-resume"

const ProcessResumeVersion = 1

// PTYVersion advertises the complete reliable terminal protocol. The task
// kind is separate from the reply's capability/status/error discriminator.
// origin: hot-update S4 plan §2.1 / T45
const (
	PTYVersion         = 1
	ProcessKindPTY     = "pty"
	ProcessOpPTYProbe  = "pty-probe"
	ProcessOpInput     = "input"
	ProcessOpResize    = "resize"
	ProcessOpSignal    = "signal"
	ProcessOpKeepalive = "keepalive"
	ProcessOpHangup    = "hangup"
)

// Stable machine codes carried in ProcessResumeReply.Code on a Kind "error"
// reply. The client classifies by Code and never sniffs prose; an empty Code
// means the peer predates this field (a pre-release dev build) and the Error
// text prefix is the fallback. Broker-side refusals reuse the admission codes
// (not_a_member, node_offline, ...) unchanged; the constants below are the
// process runtime's own. origin: docs/reviews/hot-update-reliable-exec-plan.md §2.1.
const (
	CodeProcessBadRequest      = "bad_request"                  // broker: body/exec not an object, call_id not an unsigned integer
	CodeProcessTooLarge        = "process_request_too_large"    // broker: body over the 1 MiB request limit
	CodeProcessUnsupported     = "unsupported_process_protocol" // version mismatch or agent without a runtime: the client's legacy-fallback signal
	CodeProcessUnsupportedOp   = "unsupported_process_operation"
	CodeProcessIdentity        = "process_identity"      // procrequest.ErrIdentity / ErrUnknown
	CodeProcessInstanceLost    = "process_instance_lost" // lease issued by another runtime instance: continuity lost
	CodeProcessExpired         = "process_expired"
	CodeProcessConflict        = "process_conflict" // procrequest.ErrConflict / workload.ErrReplayOffset
	CodeProcessCapacity        = "process_capacity"
	CodeProcessStale           = "process_stale"
	CodeProcessBusy            = "process_busy" // genuine admission refusal: procrequest.ErrBusy / ErrFrozen
	CodeProcessRevoked         = "process_revoked"
	CodeProcessOutputDiscarded = "process_output_discarded"
	CodeProcessRuntimeClosed   = "process_runtime_closed"
)

// Admission exceeded RequestLatency while recovery retains its owners.
// N-1 clients surface this unknown refusal without fallback or re-creation.
// origin: hot-update S4 CLOSE admission contract / plan §4.1
const CodeProcessRecovering = "process_recovering"

// ProcessRequestMaxBytes is the request body limit of the process-resume
// verb, shared by the broker (which refuses a larger body with
// CodeProcessTooLarge and CallID 0 — unparseable-request boundary) and the
// client (which refuses to publish one at all, so a deterministic usage
// error never turns into a resend loop: S1 internal review ctl-V1). It
// matches the registry's intent-hash limit and NATS' default max_payload.
const ProcessRequestMaxBytes = 1 << 20

// ProcessLease authorizes the admission of a new request only until a boot
// clock deadline. It is not a substitute for current session authorization.
// A retry keeps this entire lease, including LeaseID, unchanged.
type ProcessLease struct {
	Version        int    `json:"version"`
	LeaseID        string `json:"lease_id"`
	TargetInstance string `json:"target_instance"`
	BootID         string `json:"boot_id"`
	SID            string `json:"sid"`
	NID            string `json:"nid"`
	ActorFP        string `json:"actor_fp"`
	ClientLineage  string `json:"client_lineage"`
	IssuedBootNS   int64  `json:"issued_boot_ns"`
	ExpiresBootNS  int64  `json:"expires_boot_ns"`
	MAC            string `json:"mac"`
}

type ProcessHandle struct {
	Lease     ProcessLease `json:"lease"`
	RequestID string       `json:"request_id"`
}

// ProcessResumeReq is authenticated and forwarded by the broker for every
// operation. ActorFP is overwritten there; reply destinations come only from
// the validated NATS Reply, never from a payload field.
//
// StdoutFIN/StderrFIN echo a TRUE source EOF only, at StdoutACK/StderrACK ==
// End. An aborted stream (see ProcessOutput.Error) is acknowledged at its
// final offset with the flag false; the agent refuses a FIN claim on a
// non-FIN log, so a client cannot rewrite an abort into a clean EOF.
type ProcessResumeReq struct {
	Version       int            `json:"version"`
	CallID        uint64         `json:"call_id"`
	Op            string         `json:"op"`
	ClientLineage string         `json:"client_lineage,omitempty"`
	Handle        *ProcessHandle `json:"handle,omitempty"`
	AttachEpoch   uint64         `json:"attach_epoch,omitempty"`
	Exec          *ExecReq       `json:"exec,omitempty"`
	StdoutACK     uint64         `json:"stdout_ack,omitempty"`
	StderrACK     uint64         `json:"stderr_ack,omitempty"`
	StdoutFIN     bool           `json:"stdout_fin,omitempty"`
	StderrFIN     bool           `json:"stderr_fin,omitempty"`
	TerminalID    string         `json:"terminal_id,omitempty"`
	ActorFP       string         `json:"actor_fp,omitempty"`
	// Empty Kind retains the exec contract; all terminal payloads disappear
	// at zero so an old exec peer receives byte-identical JSON.
	Kind       string          `json:"kind,omitempty"`
	Run        *RunReq         `json:"run,omitempty"`
	Input      []byte          `json:"input,omitempty"`
	InputSeq   uint64          `json:"input_seq,omitempty"`
	InputFIN   bool            `json:"input_fin,omitempty"`
	Resize     *PtyResizeEvent `json:"resize,omitempty"`
	Signal     int             `json:"signal,omitempty"`
	ControlSeq uint64          `json:"control_seq,omitempty"`
}

// ProcessResumeReply carries each output stream as its own bounded batch in
// Stdout/Stderr, with independent absolute offsets and no global ordering
// between the two. Error under Kind "error" is the refusal; under Kind
// "status" it is the task's own setup or wait failure
// (procrequest.Terminal.Error), never a stream abort — those travel in the
// batches. (A single-stream Stream/Offset/Data/FIN form that nothing ever
// wrote or read was removed before this verb's first release — S0 review P-3.)
type ProcessResumeReply struct {
	Version        int           `json:"version"`
	CallID         uint64        `json:"call_id"`
	Kind           string        `json:"kind"`
	RequestID      string        `json:"request_id,omitempty"`
	PID            string        `json:"pid,omitempty"`
	TargetInstance string        `json:"target_instance,omitempty"`
	AttachEpoch    uint64        `json:"attach_epoch,omitempty"`
	Lease          *ProcessLease `json:"lease,omitempty"`
	ExitCode       int           `json:"exit_code,omitempty"`
	Signal         int           `json:"signal,omitempty"`
	TerminalID     string        `json:"terminal_id,omitempty"`
	// Code is the stable machine code of a Kind "error" reply (CodeProcess*
	// or a broker admission code); empty from a peer that predates it.
	Code   string         `json:"code,omitempty"`
	Error  string         `json:"error,omitempty"`
	Phase  string         `json:"phase,omitempty"`
	Stdout *ProcessOutput `json:"stdout,omitempty"`
	Stderr *ProcessOutput `json:"stderr,omitempty"`
	// InputACK releases byte responsibility; InputAccepted only grants
	// credit. InputEnded with an error may therefore retain ACK < Accepted.
	TaskKind      string `json:"task_kind,omitempty"`
	PTYVersion    int    `json:"pty_version,omitempty"`
	InputACK      uint64 `json:"input_ack,omitempty"`
	InputAccepted uint64 `json:"input_accepted,omitempty"`
	InputWindow   uint64 `json:"input_window,omitempty"`
	InputFIN      bool   `json:"input_fin,omitempty"`
	InputEnded    bool   `json:"input_ended,omitempty"`
	InputError    string `json:"input_error,omitempty"`
	ControlACK    uint64 `json:"control_ack,omitempty"`
	ControlError  string `json:"control_error,omitempty"`
}

// ProcessOutput is one bounded replay batch starting at an absolute offset.
// FIN and Error both apply at Offset+len(Data) and only in the batch that
// reaches the stream's end; a batch short of the end carries neither. FIN
// means a true source EOF. Error means the stream ABORTED there: every byte
// before it was retained and delivered (in this or earlier batches), nothing
// follows, and FIN stays false — an abort never fabricates EOF.
// origin: docs/reviews/hot-update-stages-plan.md §-1 OQ5.
type ProcessOutput struct {
	Offset uint64 `json:"offset"`
	Data   []byte `json:"data,omitempty"`
	FIN    bool   `json:"fin,omitempty"`
	Error  string `json:"error,omitempty"`
}
