package cli

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
)

const (
	ControlHealthWindow = 600 * time.Millisecond
	ControlBannerWindow = 500 * time.Millisecond
)

// RequestWithin keeps the transport's request wait, retry and backoff inside
// the original admission deadline. The subject/body are captured once, so a
// stable-process caller cannot accidentally generate a new payload per retry.
// Application refusals must be validated by the caller before claiming success.
// origin: hot-update S6 plan §8.2 / T30 / T36
func RequestWithin(ctx context.Context, nc *nats.Conn, subject string, body []byte, cfg RetryConfig) (reply *nats.Msg, err error) {
	if nc == nil {
		return nil, errors.New("cli: nil request connection")
	}
	requestCtx, cancel, err := AdmissionContext(ctx, cfg.Deadline)
	if err != nil {
		return nil, err
	}
	defer cancel()
	payload := append([]byte(nil), body...)
	err = DoPhaseWithin(requestCtx, cfg, controlbudget.PhaseRequest, func(time.Duration) error {
		var requestErr error
		reply, requestErr = nc.RequestWithContext(requestCtx, subject, payload)
		return requestErr
	})
	if err != nil {
		reply = nil
	}
	return reply, err
}

// origin: hot-update S6 plan §8.2 / T30
// ProbeClusterHealthWithin broadcasts a cluster-health request and collects ALL broker replies
// within a window (a destructive op must corroborate every reachable view, §10.4).
//
// IT DISTINGUISHES "NOBODY IS THERE" FROM "I COULD NOT ASK", and that distinction is the
// whole safety property — origin: prerelease audit increment 2 internal review,
// pairing-sweep/IMPACT-F1.
//
//	(nil, nil)   nobody answered. Production N=1, or a cluster with no health responder.
//	             The gate legitimately does not fire.
//	(nil, err)   the probe could not be MADE: the subscribe or the publish was refused.
//	             The caller has learned nothing, and a destructive operation must not
//	             read that as "healthy".
//
// It used to return a bare slice and collapse both into nil. A ctl whose reply inbox was
// refused therefore reported an empty cluster, EvalDestructiveGate(nil) is an unblocked
// gate, and `tether session rm` proceeded — the safety gate failing OPEN on exactly the
// misconfiguration it was written to survive. An earlier external review (H3/H-4) saw
// the collapse and worked around it at ONE call site by counting responders; this is the
// root fix the workaround was standing in for.
// ProbeClusterHealthWithin preserves the broadcast's corroboration window,
// bounded by what remains of RunE admission. Failure to ask remains an error;
// it cannot masquerade as an empty, healthy cluster before a destructive verb.
// origin: hot-update S6 plan §8.2 / T30
func ProbeClusterHealthWithin(ctx context.Context, nc *nats.Conn, actor string, cfg RetryConfig) (replies []proto.ClusterHealthResp, err error) {
	if nc == nil || actor == "" || !validLogicalID(cfg.LogicalID) {
		return nil, errors.New("cli: invalid health probe")
	}
	probeCtx, cancel, err := AdmissionContext(ctx, cfg.Deadline)
	if err != nil {
		return nil, err
	}
	defer cancel()
	a, err := beginAttempt(cfg.Observer, cfg.LogicalID, controlbudget.PhaseDiscovery)
	if err != nil {
		return nil, err
	}
	sent := false
	defer func() { err = errors.Join(err, completeAttempt(cfg.Observer, a, sent, errorClass(err), 0)) }()
	// nc.NewInbox(), NOT the package-level nats.NewInbox(): only the method honours
	// this connection's CustomInboxPrefix. With per-identity inbox subtrees
	// (auth.InboxPrefixFor) the package-level helper mints `_INBOX.<nuid>`, which
	// this connection is no longer granted, so the SubscribeSync below would fail.
	// origin: prerelease audit proto-auth-acl/L1-F1 verifier.
	inbox := nc.NewInbox()
	sub, err := nc.SubscribeSync(inbox)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, sub.Unsubscribe()) }()
	// A REFUSED SUBSCRIBE DOES NOT COME BACK FROM SubscribeSync. nats-server answers
	// `-ERR 'Permissions Violation for Subscription to "…"'` asynchronously, so the call
	// above returns nil and the subscription is simply dead. Flush and look: the server
	// processes SUB before the PING that Flush sends, and nats.go's readLoop sets
	// LastError() synchronously in processPermissionsViolation, so by the time Flush
	// returns a refusal is already observable. No sleep, no polling.
	//
	// Without this the whole error return below is decorative — which the guard in
	// alerts_test.go demonstrated on the first attempt at this fix.
	if err = nc.FlushWithContext(probeCtx); err != nil {
		return nil, err
	}
	if last := nc.LastError(); last != nil && strings.Contains(last.Error(), inbox) {
		return nil, nats.ErrPermissionViolation
	}
	if _, err = controlbudget.Remaining(cfg.Deadline); err != nil {
		return nil, err
	}
	if err = nc.PublishRequest(proto.SubjCtrlClusterHealth(actor), inbox, nil); err != nil {
		return nil, err
	}
	sent = true
	windowCtx, windowCancel := context.WithTimeout(probeCtx, ControlHealthWindow)
	defer windowCancel()
	for {
		msg, nextErr := sub.NextMsgWithContext(windowCtx)
		if nextErr != nil {
			if probeCtx.Err() != nil {
				return nil, probeCtx.Err()
			}
			if errors.Is(nextErr, nats.ErrNoResponders) {
				return replies, nil
			}
			if errors.Is(nextErr, context.DeadlineExceeded) {
				return replies, nil // the full corroboration window was collected
			}
			return nil, nextErr
		}
		// No-responders fast-path (M1): a manual SubscribeSync+PublishRequest does NOT get
		// Conn.Request's ErrNoResponders, so at N=1 (no broker subscribes the health subject)
		// the server's 503 sentinel arrives as a successful empty message — detect it and
		// return at once, else `tether session rm` would block the full window every time
		// (breaking the build-and-prove "byte-identical at N=1" wall-clock).
		if len(msg.Data) == 0 && msg.Header.Get("Status") == "503" {
			return replies, nil
		}
		var reply proto.ClusterHealthResp
		if json.Unmarshal(msg.Data, &reply) != nil || reply.NodeID == "" {
			return nil, errors.New("cli: invalid health response")
		}
		replies = append(replies, reply)
	}
}

// BannerWithin is an optional pre-result request. If the whole banner window
// no longer fits, record a deliberate skip; never tack another timeout onto a
// validated primary result. Callers may ignore its network error, but must
// propagate observer/Close failure before claiming a complete receipt.
// origin: hot-update S6 plan §8.2 / T35
func BannerWithin(ctx context.Context, cfg RetryConfig, fetch func(context.Context) error) (err error) {
	if fetch == nil || !validLogicalID(cfg.LogicalID) {
		return errors.New("cli: invalid banner request")
	}
	a, err := beginAttempt(cfg.Observer, cfg.LogicalID, controlbudget.PhaseBanner)
	if err != nil {
		return err
	}
	remaining, deadlineErr := controlbudget.Remaining(cfg.Deadline)
	if deadlineErr != nil || remaining < ControlBannerWindow || ctx.Err() != nil {
		return completeAttempt(cfg.Observer, a, false, "skipped", 0)
	}
	bannerCtx, cancel := context.WithTimeout(ctx, min(remaining, ControlBannerWindow))
	defer cancel()
	err = fetch(bannerCtx) // synchronous: all callback work belongs to this phase
	if err == nil {
		err = bannerCtx.Err()
	}
	return errors.Join(err, completeAttempt(cfg.Observer, a, true, errorClass(err), 0))
}
