package broker

import (
	"context"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/xferbucket"
	"github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xfercontrol"
)

// The legacy provisioning state machine remains separate from v1 grants. Its
// existing diagnostics and retry budgets live with the control owner; this
// adapter fences provisioning and resolves the broker's current backend.
// origin: hot-update S7 plan §7 / §12.2 / P22
const xferSizingTimeout = xfercontrol.LegacySizingTimeout
const xferCreateAttemptTO = xfercontrol.LegacyCreateAttemptTO
const xferProvisionMaxTries = xfercontrol.LegacyProvisionMaxTries

const xferProvisionBudget = xfercontrol.LegacyProvisionBudget
const xferProvisionMinSlack = xfercontrol.LegacyProvisionMinSlack
const codeXferJSNotReady = xfercontrol.CodeLegacyJSNotReady
const codeXferStoreTooSmall = xfercontrol.CodeLegacyStoreTooSmall
const codeXferBrokerRestarting = xfercontrol.CodeLegacyBrokerRestarting
const xferNoJetStreamMsg = xfercontrol.LegacyNoJetStreamMsg

type xferProvisionErr = xfercontrol.LegacyProvisionError
type xferTooLarge = xfercontrol.LegacyTooLarge

func xferProvisionRefusal(perr *xferProvisionErr) (string, string) {
	return xfercontrol.LegacyProvisionRefusal(perr)
}
func provisionXferBucketWithLimit(parent context.Context, b *Broker, sid string, size int64) (string, int64, *xferTooLarge, *xferProvisionErr) {
	// Direct provision callers share the prepare fence, including the period before
	// an OBJ store exists or a transferTracker entry has been published.
	// origin: hot-update S6 plan §5 / T45
	if !beginBrokerTransfer(b) {
		return "", 0, nil, &xferProvisionErr{Err: context.Canceled, Aborted: true, AbortCause: context.Canceled}
	}
	defer endBrokerTransfer(b)
	if parent == nil {
		parent = context.Background() // ctx-none: standalone legacy provisioning retains its bounded calls.
	}
	return xfercontrol.ProvisionLegacyBucket(xfercontrol.LegacyProvisionConfig{
		Done: parent.Done(), Canceled: parent.Err, PayloadLimit: xferPayloadLimit, Replicas: b.xferTargetReplicas, Logger: b.cfg.Logger,
		Size: func(timeout time.Duration) (int64, error) {
			ctx, cancel := context.WithTimeout(parent, timeout)
			defer cancel()
			return b.xferBucketMaxBytes(ctx)
		},
		Ensure: func(timeout time.Duration, sid string, replicas int, maxBytes int64) (string, int64, error) {
			ctx, cancel := context.WithTimeout(parent, timeout)
			defer cancel()
			return ensureXferBucketSizedWithLimit(ctx, b, sid, replicas, maxBytes)
		},
	}, sid, size)
}

// provisionXferBucket preserves the established push/test call shape. Pull uses the WithLimit form
// because only the agent learns the file size and therefore needs the actual payload ceiling.
func (b *Broker) provisionXferBucket(parent context.Context, sid string, size int64) (string, *xferTooLarge, *xferProvisionErr) {
	bucket, _, tooLarge, perr := provisionXferBucketWithLimit(parent, b, sid, size)
	return bucket, tooLarge, perr
}

// Shrink lets the legacy small-disk repair (convergeOversizedXferBucket) reach
// the leader's proof-gated coordinator through the same shared router.
// origin: hot-update S7 plan §2.1 / lead decision on authoritative shrink proof
func (r *TransferBucketRouter) Shrink(p xfercontract.ConfigPatch, d controlbudget.Deadline) (xfercontract.BucketSeal, error) {
	return xferbucket.ShrinkVia(r.Local, r.Node == nil || r.Node.IsLeader(), p, d)
}

// brokerBucketCensus is the single-broker configuration owner's own census for
// xferbucket.NewWithCensus: the legacy tracker's admitted entries plus every
// unsettled row of the installed continuation ledger. Anything it cannot see
// completely is an error, never an empty set.
// origin: hot-update S7 plan §2.1 / lead decision on authoritative shrink proof
type brokerBucketCensus struct{ b *Broker }

func (c brokerBucketCensus) Responsible(bucket string, d controlbudget.Deadline) (int, error) {
	b := c.b
	// A leader's local trackers do not see transfers retained by other homes.
	if b.clusterMode || b.transfers == nil {
		return 0, &xferbucket.Refusal{Code: xfercontract.CodeOwnerRecovering, Detail: "bucket census is incomplete on this broker"}
	}
	live := 0
	if _, busy := b.transfers.ActiveOBJStreams()["OBJ_"+bucket]; busy {
		live++
	}
	current := b.transferRuntime.Load()
	if current == nil && b.cfg.TransferFactory == nil {
		return live, nil // no continuation owner is configured: the legacy tracker is the whole set
	}
	var ledger xfercontract.LedgerPort
	if current != nil {
		ledger = current.Ledger
	}
	rows, err := xferbucket.LedgerResponsibility(ledger, bucket, d)
	if err != nil {
		return 0, err
	}
	return live + rows, nil
}
