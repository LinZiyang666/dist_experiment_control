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
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
)

// RunWithin carries the caller's absolute admission budget through negotiation
// and true start. Output, settlement and long-running work keep the user ctx.
// origin: hot-update S6 plan §8.2 / T31 / T32
func RunWithin(ctx context.Context, nc *nats.Conn, subject string, spec proto.ExecReq, stdout, stderr io.Writer, cfg AdmissionConfig) (Result, error) {
	return runExec(ctx, nc, subject, spec, stdout, stderr, &cfg)
}

// ProbeWithin never infers legacy support from silence.
// Only authenticated prior capability or an explicit refusal allows fallback.
// origin: hot-update S6 plan §8.2 / T31
func ProbeWithin(ctx context.Context, nc *nats.Conn, subject string, req proto.ProcessResumeReq, cfg AdmissionConfig) (proto.ProcessResumeReply, error) {
	if req.Op != "probe" || req.Handle != nil || req.Exec != nil || req.Run != nil {
		return proto.ProcessResumeReply{}, fmt.Errorf("exec client: invalid probe")
	}
	return admissionExchange(ctx, nc, subject, req, cfg)
}

// CreateWithin keeps the supplied handle and intent even when the original
// create reply was lost. Starting is an admission receipt, not true start.
// origin: hot-update S6 plan §8.2 / T32
func CreateWithin(ctx context.Context, nc *nats.Conn, subject string, req proto.ProcessResumeReq, cfg AdmissionConfig) (proto.ProcessResumeReply, error) {
	if req.Op != "create" || req.Handle == nil || req.Handle.RequestID == "" || req.Exec == nil {
		return proto.ProcessResumeReply{}, fmt.Errorf("exec client: invalid create")
	}
	return admissionExchange(ctx, nc, subject, req, cfg)
}

func admissionExchange(ctx context.Context, nc *nats.Conn, subject string, req proto.ProcessResumeReq, cfg AdmissionConfig) (proto.ProcessResumeReply, error) {
	if _, err := admissionRemaining(ctx, cfg); err != nil {
		return proto.ProcessResumeReply{}, err
	}
	sid, actor, nid, verb, ok := proto.ParseCmdBy(subject)
	if nc == nil || !ok || verb != proto.ProcessResumeVerb {
		return proto.ProcessResumeReply{}, fmt.Errorf("exec client: invalid request")
	}
	fp, err := auth.FingerprintFromActor(actor)
	if err != nil {
		return proto.ProcessResumeReply{}, err
	}
	if req.Op == "probe" && cfg.LegacyConfirmed {
		return proto.ProcessResumeReply{}, ErrUnsupported
	}
	inbox := nc.NewInbox()
	if !strings.HasPrefix(inbox, auth.InboxPrefixFor(actor)+".") {
		return proto.ProcessResumeReply{}, fmt.Errorf("exec client: private inbox unavailable")
	}
	sub, err := nc.SubscribeSync(inbox)
	if err != nil {
		return proto.ProcessResumeReply{}, err
	}
	defer func() { _ = sub.Unsubscribe() }()
	tx := &transport{nc: nc, sub: sub, subject: subject, inbox: inbox}
	if req.Op == "probe" {
		reply, err := admissionCall(ctx, tx, req, cfg)
		if isUnsupported(err) {
			return reply, ErrUnsupported
		}
		if err == nil && !validCapability(reply, sid, nid, fp, req.ClientLineage) {
			return reply, fmt.Errorf("exec client: invalid capability response")
		}
		return reply, err
	}
	// Copy the handle before any asynchronous reply. CallID belongs to the
	// transport; lease/RequestID belong to the task and can never be renewed.
	result := Result{Handle: *req.Handle}
	req.Handle = &result.Handle
	for {
		reply, err := admissionCall(ctx, tx, req, cfg)
		if err != nil {
			return reply, err
		}
		if err := validateReply(&result, req, reply); err != nil {
			return reply, err
		}
		if reply.Phase != "starting" {
			return reply, nil
		}
		// A receipt for an accepted create permits querying the same task;
		// it cannot justify either a second handle or legacy execution.
		req.Op, req.Exec, req.CallID = "resume", nil, 0
		if err := admissionBackoff(ctx, cfg, idlePollMin); err != nil {
			return reply, err
		}
	}
}

// The existing lease clock is already a ctx-free, platform-split BOOTTIME
// leaf. Reuse it while P18 owns the shared clock API, rather than add a Linux
// syscall to this common file or interpret an absolute stamp as wall time.
// origin: hot-update S6 plan §8.2 / T31 / T32
func admissionRemaining(ctx context.Context, cfg AdmissionConfig) (time.Duration, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if cfg.Deadline <= 0 || cfg.LogicalID == "" {
		return 0, fmt.Errorf("exec client: invalid admission budget")
	}
	now, err := procrequest.BootNow()
	if err != nil {
		return 0, err
	}
	if int64(cfg.Deadline) <= now {
		return 0, errors.Join(controlbudget.ErrExpired, context.DeadlineExceeded)
	}
	remaining := time.Duration(int64(cfg.Deadline) - now)
	if end, ok := ctx.Deadline(); ok {
		remaining = min(remaining, time.Until(end))
	}
	if remaining <= 0 {
		return 0, errors.Join(controlbudget.ErrExpired, context.DeadlineExceeded)
	}
	return remaining, nil
}

func admissionCall(ctx context.Context, tx *transport, req proto.ProcessResumeReq, cfg AdmissionConfig) (proto.ProcessResumeReply, error) {
	// origin: hot-update S6 plan §8.2 / T32
	// Allocate once, outside the retry loop. Late replies correlate with the
	// original operation, whose immutable payload is reused for every send.
	if req.CallID == 0 {
		tx.callID++
		req.CallID = tx.callID
	} else {
		tx.callID = max(tx.callID, req.CallID)
	}
	phase := controlbudget.PhaseRequest
	switch req.Op {
	case "probe":
		phase = controlbudget.PhaseProbe
	case "create":
		phase = controlbudget.PhaseCreate
	default:
	}
	var lastRefusal error
	probeStarted := time.Now()
	liveLegacySurface := false
	for {
		if _, err := admissionRemaining(ctx, cfg); err != nil {
			return proto.ProcessResumeReply{}, errors.Join(lastRefusal, err)
		}
		attempt, err := admissionBegin(cfg, phase)
		if err != nil {
			return proto.ProcessResumeReply{}, err
		}
		// Observer admission shares the same budget. Never publish using a
		// remaining duration calculated before an observer call consumed it.
		remaining, err := admissionRemaining(ctx, cfg)
		if err != nil {
			if recordErr := admissionRecord(cfg, attempt, err); recordErr != nil {
				return proto.ProcessResumeReply{}, recordErr
			}
			return proto.ProcessResumeReply{}, errors.Join(lastRefusal, err)
		}
		bounded, cancel := context.WithTimeout(ctx, remaining)
		reply, callErr := tx.attempt(bounded, req)
		cancel()
		// origin: hot-update S6 plan §8.2 / T31
		// A late response (including time consumed across suspend) cannot turn
		// an expired admission into success merely because bytes were queued.
		if callErr == nil {
			_, callErr = admissionRemaining(ctx, cfg)
		}
		if callErr != nil && controlbudget.IsExpired(callErr) {
			callErr = errors.Join(lastRefusal, callErr)
		}
		attempt.Sent = tx.sent
		if err := admissionRecord(cfg, attempt, callErr); err != nil {
			return reply, err
		}
		if _, err := admissionRemaining(ctx, cfg); err != nil {
			return reply, errors.Join(lastRefusal, err)
		}
		// origin: hot-update S6 plan §8.2 / T31 / FIX-BLACKOUT
		// A 503 names a missing subscription, including during a modern cut.
		// Preserve S1's positive old-broker path only when a fresh authenticated
		// node read witnessed a live control surface between two explicit 503s.
		// A timeout/reconnect breaks that proof; an earlier 503 cannot be kept
		// as evidence after the modern subscription has returned but is busy.
		noResponder := errors.Is(callErr, nats.ErrNoResponders)
		if req.Op == "probe" && (noResponder || errors.Is(callErr, context.DeadlineExceeded) || errors.Is(callErr, nats.ErrTimeout)) && time.Since(probeStarted) >= 2*time.Second {
			if noResponder && liveLegacySurface {
				return reply, ErrUnsupported
			}
			old, live, proofErr := legacyPeerEvidence(ctx, tx, cfg)
			if proofErr != nil {
				return reply, proofErr
			}
			if old {
				return reply, ErrUnsupported
			}
			liveLegacySurface = noResponder && live
		} else {
			liveLegacySurface = false
		}

		if callErr == nil || !admissionRetryable(callErr) {
			return reply, callErr
		}
		// attempt already paces timeout/no-responder waits. Immediate typed
		// recovery refuses need a bounded pause to avoid a publish storm.
		var refusal *ProtocolError
		if errors.As(callErr, &refusal) {
			// origin: hot-update S6 internal review rv-ctl-7
			// Budget exhaustion must preserve the last peer refusal and its CLI class.
			lastRefusal = callErr
			if err := admissionBackoff(ctx, cfg, idlePollMax); err != nil {
				return reply, errors.Join(lastRefusal, err)
			}
		}
	}
}

// legacyNodeProof is a successful authenticated read of the target's registered
// old-version metadata. A missing reply, missing node, refusal or modern version
// cannot authorize fallback. Missing version fields are unknown, even in a
// completed read; only an explicit known old release supplies positive evidence.
// origin: hot-update S6 internal review rv-ctl-1 / lead D1
func legacyNodeProof(ctx context.Context, tx *transport, cfg AdmissionConfig) (bool, error) {
	old, _, err := legacyPeerEvidence(ctx, tx, cfg)
	return old, err
}

// legacyPeerEvidence keeps version proof separate from current broker liveness.
// The latter cannot authorize fallback by itself: its caller must observe a
// subsequent explicit missing-verb reply, within the original admission budget.
func legacyPeerEvidence(ctx context.Context, tx *transport, cfg AdmissionConfig) (old, live bool, err error) {
	sid, actor, nid, _, ok := proto.ParseCmdBy(tx.subject)
	_, err = admissionRemaining(ctx, cfg)
	if !ok || err != nil {
		return false, false, err
	}
	a, err := admissionBegin(cfg, controlbudget.PhaseDiscovery)
	if err != nil {
		return false, false, err
	}
	// The observer and the optional proof use the original admission deadline.
	// A late old-peer response must not authorize a new legacy execution.
	// origin: hot-update S6 plan §8.2 / T31 / FIX-BLACKOUT
	remaining, err := admissionRemaining(ctx, cfg)
	if err != nil {
		return false, false, errors.Join(err, admissionRecord(cfg, a, err))
	}
	readCtx, cancel := context.WithTimeout(ctx, min(remaining, 250*time.Millisecond))
	defer cancel()
	msg, err := tx.nc.RequestWithContext(readCtx, proto.SubjCtrlNodeList(actor, sid), nil)
	a.Sent = true
	if recordErr := admissionRecord(cfg, a, err); recordErr != nil {
		return false, false, recordErr
	}
	if _, budgetErr := admissionRemaining(ctx, cfg); budgetErr != nil {
		return false, false, budgetErr
	}
	if err != nil {
		return false, false, nil // A failed optional read supplies no legacy evidence.
	}
	var nodes proto.NodeListResp
	if json.Unmarshal(msg.Data, &nodes) != nil || nodes.Code != "" || nodes.Error != "" {
		return false, false, nil
	}
	for _, node := range nodes.Nodes {
		if node.NID == nid && node.Status == "ONLINE" {
			return proto.SameRelease(node.ReleaseVersion, "v0.6.0"), true, nil
		}
	}
	return false, false, nil
}

func admissionRetryable(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, nats.ErrTimeout) ||
		errors.Is(err, nats.ErrNoResponders) || errors.Is(err, nats.ErrConnectionReconnecting) {
		return true
	}
	var refusal *ProtocolError
	if !errors.As(err, &refusal) {
		return false
	}
	// ACL, identity, signature, unsupported and genuine busy stay terminal.
	// A sent create retains ownership even when a later peer refuses it.
	switch refusal.Code {
	case proto.CodeProcessRecovering, proto.CodeNodeOffline, proto.CodeAgentNoResponders, "agent_upgrading", "forward_failed":
		return true
	default:
		return false
	}
}

func admissionBegin(cfg AdmissionConfig, phase controlbudget.AttemptPhase) (controlbudget.Attempt, error) {
	if cfg.Observer == nil {
		return controlbudget.Attempt{}, nil
	}
	return cfg.Observer.Begin(cfg.LogicalID, phase)
}

func admissionRecord(cfg AdmissionConfig, attempt controlbudget.Attempt, err error) error {
	if cfg.Observer == nil {
		return nil
	}
	now, clockErr := procrequest.BootNow()
	if clockErr != nil {
		return clockErr
	}
	attempt.FinishedMonoNS = now
	// Only closed, secret-free classes enter receipts. Peer error text may
	// contain identity or payload material and must never reach the sink.
	switch {
	case err == nil:
		attempt.Class = "success"
	case errors.Is(err, context.Canceled):
		attempt.Class = "canceled"
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, nats.ErrTimeout):
		attempt.Class = "timeout"
	case errors.Is(err, nats.ErrNoResponders):
		attempt.Class = "no_responders"
	case admissionRetryable(err):
		attempt.Class = "retryable"
	default:
		attempt.Class = "terminal"
	}
	return cfg.Observer.Record(attempt)
}

func admissionBackoff(ctx context.Context, cfg AdmissionConfig, pause time.Duration) error {
	if _, err := admissionRemaining(ctx, cfg); err != nil {
		return err
	}
	attempt, err := admissionBegin(cfg, controlbudget.PhaseBackoff)
	if err != nil {
		return err
	}
	// origin: hot-update S6 plan §8.2 / T32
	// Observing a retry cannot renew its wait or authorize a timer after the
	// original deadline. The timeout is recorded even when no wait begins.
	remaining, err := admissionRemaining(ctx, cfg)
	if err != nil {
		if recordErr := admissionRecord(cfg, attempt, err); recordErr != nil {
			return recordErr
		}
		return err
	}
	pause = min(pause, remaining)
	attempt.BackoffNS = int64(pause)
	timer := time.NewTimer(pause)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		err = ctx.Err()
	case <-timer.C:
	}
	if recordErr := admissionRecord(cfg, attempt, err); recordErr != nil {
		return recordErr
	}
	return err
}
