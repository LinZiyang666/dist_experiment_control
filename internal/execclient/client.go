// Package execclient implements the receiving half of reliable exec. Only
// bytes actually written to the caller's destinations are acknowledged.
package execclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/proc"
	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
)

// ErrUnsupported is returned only before a reliable create was attempted.
// It is the only error for which a caller may choose a legacy exec path.
var ErrUnsupported = errors.New("reliable process protocol unavailable")

// ProtocolError is a Kind "error" reply from the broker or the agent. Code is
// the stable wire code (empty from a peer that predates it); Message is the
// prose, which Error returns verbatim so text classifiers keep working.
// AfterCreate records whether the create had already been sent when the
// refusal arrived: after that point the task's fate is unknown to the caller.
type ProtocolError struct {
	Code        string
	Message     string
	AfterCreate bool
}

func (e *ProtocolError) Error() string { return e.Message }

// isUnsupported reports the one refusal that may send a caller to the legacy
// path: the agent (or broker) has no reliable process runtime. Anything else,
// permission and session errors included, must surface as itself.
func isUnsupported(err error) bool {
	var pe *ProtocolError
	if !errors.As(err, &pe) {
		return false
	}
	// origin: hot-update S6 plan §8.2 / T31
	// Either explicit protocol/operation refusal proves this probe unavailable;
	// transport silence and unrelated admission failures never prove legacy.
	return pe.Code == proto.CodeProcessUnsupported || pe.Code == proto.CodeProcessUnsupportedOp ||
		(pe.Code == "" && pe.Message == "unsupported process protocol")
}

// isExpiredSettlement reports the agent telling us the settled result was
// already collected: only a client that holds every stream end and the
// terminal may read that as completion.
func isExpiredSettlement(err error) bool {
	var pe *ProtocolError
	if !errors.As(err, &pe) {
		return false
	}
	return pe.Code == proto.CodeProcessExpired || (pe.Code == "" && pe.Message == procrequest.ErrExpired.Error())
}

// SettledError is returned by Run only AFTER the task settled on the agent:
// every delivered byte was acknowledged and the terminal collected, but the
// outcome is not a clean run. Result carries the child's real exit code and
// signal. A transport or identity failure is never wrapped in it.
// origin: docs/reviews/hot-update-stages-plan.md §-1 OQ5.
type SettledError struct {
	Result Result
}

func (e *SettledError) Error() string {
	parts := make([]string, 0, 4)
	if e.Result.TerminalError != "" {
		// First and verbatim: the CLI classifies by the text before the
		// first colon (remote_fs_spawn_timeout and friends).
		parts = append(parts, e.Result.TerminalError)
	}
	for _, s := range []struct {
		name  string
		bytes uint64
		abort string
	}{{"stdout", e.Result.StdoutBytes, e.Result.StdoutAbort}, {"stderr", e.Result.StderrBytes, e.Result.StderrAbort}} {
		if s.abort == "" {
			continue
		}
		msg := fmt.Sprintf("%s aborted after %d byte(s)", s.name, s.bytes)
		if s.abort != e.Result.TerminalError {
			msg += ": " + s.abort
		}
		parts = append(parts, msg)
	}
	if e.Result.InputAbort != "" {
		parts = append(parts, fmt.Sprintf("%s (applied %d, accepted %d byte(s))",
			e.Result.InputAbort, e.Result.InputACK, e.Result.InputAccepted))
	}
	return strings.Join(parts, "; ")
}

type receiver struct {
	writer io.Writer
	offset uint64
	fin    bool
	abort  string
}

// ended reports that no byte can follow: a true EOF or an abort was
// accepted at offset. Only fin is ever echoed to the agent.
func (r *receiver) ended() bool { return r.fin || r.abort != "" }

type transport struct {
	nc        *nats.Conn
	sub       *nats.Subscription
	subject   string
	inbox     string
	callID    uint64
	created   bool             // the create request reached the transport at least once
	sent      bool             // the current attempt reached PublishRequest
	admission *AdmissionConfig // cleared only after validated true start
}

// Run negotiates before creating anything. After sending create it retries
// the original handle only, including when the response containing its PID
// was lost. A terminal result is returned after output and settlement. A
// settled but unclean result is returned as *SettledError together with the
// acknowledged Result.
func Run(ctx context.Context, nc *nats.Conn, subject string, spec proto.ExecReq, stdout, stderr io.Writer) (Result, error) {
	return runExec(ctx, nc, subject, spec, stdout, stderr, nil)
}

func runExec(ctx context.Context, nc *nats.Conn, subject string, spec proto.ExecReq, stdout, stderr io.Writer, cfg *AdmissionConfig) (Result, error) {
	var result Result
	sid, actor, nid, verb, ok := proto.ParseCmdBy(subject)
	if nc == nil || !ok || verb != proto.ProcessResumeVerb || stdout == nil || stderr == nil {
		return result, fmt.Errorf("exec client: invalid request")
	}
	fp, err := auth.FingerprintFromActor(actor)
	if err != nil {
		return result, err
	}
	if cfg != nil {
		if _, err := admissionRemaining(ctx, *cfg); err != nil {
			return result, err
		}
		if cfg.LegacyConfirmed {
			return result, ErrUnsupported
		}
	}
	inbox := nc.NewInbox()
	if !strings.HasPrefix(inbox, auth.InboxPrefixFor(actor)+".") {
		if cfg != nil {
			return result, fmt.Errorf("exec client: private inbox unavailable")
		}
		return result, ErrUnsupported
	}
	sub, err := nc.SubscribeSync(inbox)
	if err != nil {
		return result, err
	}
	defer func() { _ = sub.Unsubscribe() }()
	t := &transport{nc: nc, sub: sub, subject: subject, inbox: inbox, admission: cfg}
	lineage := proc.NewPID()
	probeBudget := 2 * time.Second
	if cfg != nil {
		probeBudget, err = admissionRemaining(ctx, *cfg)
		if err != nil {
			return result, err
		}
	}
	probeCtx, cancel := context.WithTimeout(ctx, probeBudget)
	capability, err := t.call(probeCtx, proto.ProcessResumeReq{Version: proto.ProcessResumeVersion,
		Op: "probe", ClientLineage: lineage})
	cancel()
	if err != nil {
		if ctx.Err() != nil {
			return result, ctx.Err()
		}
		// origin: hot-update S6 plan §8.2 / T31
		// Silence proves no capability: a hot-cut blackout must not create
		// a legacy child while the reliable owner is coming back. An explicit
		// no-responder reply is positive evidence under lead D1.
		if isUnsupported(err) || errors.Is(err, nats.ErrNoResponders) {
			return result, ErrUnsupported
		}
		return result, err
	}
	l := capability.Lease
	if !validCapability(capability, sid, nid, fp, lineage) {
		return result, fmt.Errorf("exec client: invalid capability response")
	}
	result.Handle = proto.ProcessHandle{Lease: *l, RequestID: proc.NewPID()}
	req := proto.ProcessResumeReq{Version: proto.ProcessResumeVersion, Op: "create", Handle: &result.Handle,
		AttachEpoch: 1, Exec: &spec}
	out, errOut := receiver{writer: stdout}, receiver{writer: stderr}
	idle := idlePollMin
	for {
		reply, err := t.call(ctx, req)
		result.CreateSent = t.created
		if err != nil {
			return result, err
		}
		if err := validateReply(&result, req, reply); err != nil {
			return result, err
		}
		// origin: hot-update S6 plan §8.2 / T32
		// Accepted/starting is still admission. Once the original task truly
		// started (or finished), its lifetime returns to the user context.
		if t.admission != nil && (reply.Phase != "starting") {
			t.admission = nil
		}
		if reply.Phase == "running" || reply.Signal != 0 || (reply.TerminalID != "" && reply.ExitCode >= 0) {
			result.Executed = true
		}
		before := out.offset + errOut.offset
		if err := out.accept(reply.Stdout); err != nil {
			acknowledgePartial(ctx, t, req, out, errOut)
			return result, err
		}
		if err := errOut.accept(reply.Stderr); err != nil {
			acknowledgePartial(ctx, t, req, out, errOut)
			return result, err
		}
		if out.offset+errOut.offset > before {
			result.Executed = true
		}
		req.Op, req.Exec = "ack", nil
		// Only a true EOF is echoed as FIN; an aborted stream is acknowledged
		// at its final offset with the flag false.
		req.StdoutACK, req.StderrACK, req.StdoutFIN, req.StderrFIN = out.offset, errOut.offset, out.fin, errOut.fin
		if reply.TerminalID != "" && out.ended() && errOut.ended() {
			result.ExitCode, result.Signal = reply.ExitCode, reply.Signal
			result.TerminalError = reply.Error
			result.StdoutAbort, result.StderrAbort = out.abort, errOut.abort
			result.StdoutBytes, result.StderrBytes = out.offset, errOut.offset
			req.Op, req.TerminalID = "terminal-ack", reply.TerminalID
			settled, err := t.call(ctx, req)
			if err != nil {
				// Only a client already holding every stream's end and the
				// terminal may interpret expired settlement as a completed result.
				if !isExpiredSettlement(err) {
					return result, err
				}
			} else if err := validateReply(&result, req, settled); err != nil {
				return result, err
			} else if settled.Phase != "settled" || settled.TerminalID != reply.TerminalID {
				return result, fmt.Errorf("exec client: terminal was not settled")
			}
			if result.TerminalError != "" || result.StdoutAbort != "" || result.StderrAbort != "" {
				return result, &SettledError{Result: result}
			}
			return result, nil
		}
		if before == out.offset+errOut.offset {
			// An idle source has no data to drive a subsequent ACK. Keep
			// the poll bounded without tying business lifetime to a carrier,
			// and back it off while nothing arrives: every idle ACK is a full
			// ctl→broker→agent→ctl round trip with three SQLite reads in the
			// broker's admission, so a quiet long command polling every 20 ms
			// cost the fleet broker ~48 requests/s per exec (S1 review C4).
			// Bytes reset the interval, so a burst after silence is picked up
			// within idlePollMax plus one round trip.
			timer := time.NewTimer(idle)
			select {
			case <-ctx.Done():
				timer.Stop()
				return result, ctx.Err()
			case <-timer.C:
			}
			idle = min(idle*2, idlePollMax)
		} else {
			idle = idlePollMin
		}
	}
}

// Idle ACK cadence: fast enough that the first byte after a pause is seen
// promptly, slow enough that a silent command is not a load on the broker.
// The ceiling stays well under the 500 ms attempt timeout so an idle ACK is
// never mistaken for a lost reply.
const (
	idlePollMin = 20 * time.Millisecond
	idlePollMax = 250 * time.Millisecond
)

// Each attempt yields within this bound, including a reconnect/no-responder
// wait. PTY scheduling relies on that return; exec retains its retry wrapper.
// origin: hot-update S4 plan §7.2 / T40
const processAttemptTimeout = 500 * time.Millisecond

func (t *transport) call(ctx context.Context, req proto.ProcessResumeReq) (proto.ProcessResumeReply, error) {
	if t.admission != nil {
		return admissionCall(ctx, t, req, *t.admission)
	}
	t.callID++
	req.CallID = t.callID
	for {
		reply, err := t.attempt(ctx, req)
		if ctx.Err() != nil {
			return proto.ProcessResumeReply{}, ctx.Err()
		}
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, nats.ErrTimeout) ||
			errors.Is(err, nats.ErrNoResponders) || errors.Is(err, nats.ErrConnectionReconnecting) {
			continue
		}
		// origin: hot-update S4 CLOSE admission contract / plan §4.1
		// Retry only retained recovery, with the same CallID and handle. The
		// caller's ctx remains the exec recovery budget; an immediate refusal
		// must yield rather than turn recovery into a publish storm.
		var refusal *ProtocolError
		if errors.As(err, &refusal) && refusal.Code == proto.CodeProcessRecovering {
			timer := time.NewTimer(idlePollMax)
			select {
			case <-ctx.Done():
				timer.Stop()
				return proto.ProcessResumeReply{}, ctx.Err()
			case <-timer.C:
			}
			continue
		}
		return reply, err
	}
}

// attempt publishes at most once. A zero CallID asks for a fresh correlation;
// the exec wrapper supplies one stable ID across its existing retry loop.
// Business identities and byte/control sequences never depend on CallID.
func (t *transport) attempt(ctx context.Context, req proto.ProcessResumeReq) (proto.ProcessResumeReply, error) {
	t.sent = false
	if err := ctx.Err(); err != nil {
		return proto.ProcessResumeReply{}, err
	}
	if req.CallID == 0 {
		t.callID++
		req.CallID = t.callID
	}
	body, err := json.Marshal(req)
	if err != nil {
		return proto.ProcessResumeReply{}, err
	}
	if len(body) > proto.ProcessRequestMaxBytes {
		// The broker would refuse this with CallID 0 (it cannot parse a body
		// it will not read), which no retry can change: refuse it here, before
		// a byte is published, as the same typed refusal the broker sends.
		// origin: S1 internal review ctl-V1.
		return proto.ProcessResumeReply{}, &ProtocolError{Code: proto.CodeProcessTooLarge,
			Message: fmt.Sprintf("process request too large: %d bytes over the %d-byte limit (argv, env and --stdin together)",
				len(body)-proto.ProcessRequestMaxBytes, proto.ProcessRequestMaxBytes), AfterCreate: t.created}
	}
	attempt, cancel := context.WithTimeout(ctx, processAttemptTimeout)
	defer cancel()
	if t.nc.IsReconnecting() {
		// nats.go would buffer the publish and flush every copy after the
		// reconnect: a 25 s outage became ~50 forwarded copies of the same
		// CallID (and as many create audit rows). Wait for the carrier
		// instead; the attempt timeout below resends once it is back.
		// Waiting inside one attempt also exposes the retry to the PTY owner,
		// so a transport wait cannot monopolize its keepalive scheduler.
		<-attempt.Done()
		return proto.ProcessResumeReply{}, nats.ErrConnectionReconnecting
	}
	if err := t.nc.PublishRequest(t.subject, t.inbox, body); err != nil {
		return proto.ProcessResumeReply{}, err
	}
	t.sent = true
	if req.Op == "create" {
		t.created = true
	}
	reply, err := t.receive(attempt, req.CallID)
	if errors.Is(err, nats.ErrNoResponders) {
		// An immediate 503 must not turn either owner into a publish storm.
		<-attempt.Done()
	}
	return reply, err
}

func (t *transport) receive(ctx context.Context, callID uint64) (proto.ProcessResumeReply, error) {
	for {
		msg, err := t.sub.NextMsgWithContext(ctx)
		if err != nil {
			return proto.ProcessResumeReply{}, err
		}
		// origin: hot-update S6 internal review rv-ctl-1 / lead D1
		// Manual inbox reads expose the server 503 as a message, unlike Request.
		if len(msg.Data) == 0 && msg.Header.Get("Status") == "503" {
			return proto.ProcessResumeReply{}, nats.ErrNoResponders
		}
		var reply proto.ProcessResumeReply
		if err := json.Unmarshal(msg.Data, &reply); err != nil {
			return reply, fmt.Errorf("exec client: invalid response: %w", err)
		}
		if reply.CallID != callID && !unparseableRefusal(reply) {
			continue
		}
		if reply.Version != proto.ProcessResumeVersion {
			return reply, fmt.Errorf("exec client: unsupported response version")
		}
		if reply.Kind == "error" {
			return reply, &ProtocolError{Code: reply.Code, Message: reply.Error, AfterCreate: t.created}
		}
		return reply, nil
	}
}

// unparseableRefusal recognises the two refusals the broker sends with
// CallID 0 because it could not (too large) or would not (malformed) read the
// request far enough to learn the CallID (architecture §22.4 item 2 ③). This
// client only ever has one call in flight on its private inbox, so such a
// reply can only answer that call; dropping it as "not mine" turned a
// deterministic refusal into a resend loop that ended at --timeout as 74
// (S1 internal review ctl-V1).
func unparseableRefusal(reply proto.ProcessResumeReply) bool {
	return reply.CallID == 0 && reply.Kind == "error" &&
		(reply.Code == proto.CodeProcessTooLarge || reply.Code == proto.CodeProcessBadRequest)
}

// Capability identity is shared by exec and the first PTY probe. Check it
// before interpreting optional capability fields, so malformed old replies
// cannot authorize fallback or mint a handle for another scope.
// origin: hot-update S4 plan §2.2
func validCapability(reply proto.ProcessResumeReply, sid, nid, fp, lineage string) bool {
	l := reply.Lease
	return reply.Kind == "capability" && l != nil && l.Version == proto.ProcessResumeVersion &&
		l.TargetInstance != "" && l.TargetInstance == reply.TargetInstance && l.SID == sid && l.NID == nid &&
		l.ActorFP == fp && l.ClientLineage == lineage
}

func validateReply(result *Result, req proto.ProcessResumeReq, reply proto.ProcessResumeReply) error {
	if reply.Kind != "status" || reply.RequestID != result.Handle.RequestID ||
		reply.TargetInstance != result.Handle.Lease.TargetInstance || reply.AttachEpoch != req.AttachEpoch || reply.PID == "" {
		return fmt.Errorf("exec client: response identity mismatch")
	}
	if reply.ExitCode < -1 || reply.ExitCode > 255 || reply.Signal < 0 || reply.Signal > 64 ||
		(reply.Signal != 0 && reply.ExitCode != -1) {
		return fmt.Errorf("exec client: invalid terminal status")
	}
	if result.PID == "" {
		result.PID = reply.PID
	} else if result.PID != reply.PID {
		return fmt.Errorf("exec client: original task changed")
	}
	switch reply.Phase {
	case "starting", "running", "terminal", "settled":
	default:
		return fmt.Errorf("exec client: invalid task state")
	}
	return nil
}

func (r *receiver) accept(batch *proto.ProcessOutput) error {
	if batch == nil {
		return nil
	}
	if batch.FIN && batch.Error != "" {
		return fmt.Errorf("exec client: conflicting stream end")
	}
	if batch.Offset > r.offset || uint64(len(batch.Data)) > ^uint64(0)-batch.Offset {
		return fmt.Errorf("exec client: output gap or invalid offset")
	}
	end := batch.Offset + uint64(len(batch.Data))
	if r.fin && end > r.offset {
		return fmt.Errorf("exec client: bytes after FIN")
	}
	if r.abort != "" && end > r.offset {
		return fmt.Errorf("exec client: bytes after abort")
	}
	if end > r.offset {
		data := batch.Data[r.offset-batch.Offset:]
		n, err := r.writer.Write(data)
		if n < 0 || n > len(data) {
			return fmt.Errorf("exec client: invalid destination write count")
		}
		r.offset += uint64(n)
		if err != nil {
			return err
		}
		if n != len(data) {
			return io.ErrShortWrite
		}
	}
	if batch.FIN || batch.Error != "" {
		// Either end marker applies exactly at Offset+len(Data), and a stream
		// that ended one way can never be reported as having ended the other.
		if end != r.offset {
			return fmt.Errorf("exec client: conflicting final offset")
		}
		if (batch.FIN && r.abort != "") || (batch.Error != "" && r.fin) {
			return fmt.Errorf("exec client: conflicting stream end")
		}
		if batch.FIN {
			r.fin = true
		}
		if batch.Error != "" && r.abort == "" {
			// An abort is a stream fact to settle, not a transport failure:
			// returning here left the task unsettled on the agent forever.
			r.abort = batch.Error
		}
	}
	return nil
}

func acknowledgePartial(ctx context.Context, t *transport, req proto.ProcessResumeReq, stdout, stderr receiver) {
	req.Op, req.Exec, req.TerminalID = "ack", nil, ""
	req.StdoutACK, req.StderrACK, req.StdoutFIN, req.StderrFIN = stdout.offset, stderr.offset, stdout.fin, stderr.fin
	deadline, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	_, _ = t.call(deadline, req)
}
