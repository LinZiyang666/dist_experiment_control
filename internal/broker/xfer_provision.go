package broker

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/LinZiyang666/tether/internal/jsstream"
)

// ── #67 tier-B provisioning: separate deadlines + a bounded, classified retry ─────────────────────
//
// The defect this replaces (docs/deploy-tier-gotchas.md #67) was NOT "tether refused while JetStream
// had no quorum" — an R=2 asset genuinely cannot be written then, so refusing is correct. It was that
// the user-facing path had no representation of "transient": two independent JetStream round trips
// shared ONE 5s deadline, ran ONCE, and any error became a terminal `bucket_create_failed` whose text
// gave the operator no reason to retry. Measured on the deploy tier: the create itself costs ~57ms on
// a healthy cluster, yet a freshly-grown N=2 cluster's FIRST tier-B push intermittently hard-failed
// with `create_bucket: context deadline exceeded` — and a plain immediate retry succeeded.
//
// Budget sizing is constrained by a fact that is easy to miss: `.push.req` and `.pull.req` are in
// isBroadcastClusterSubject (clusterwrite.go:59-61), so they use a plain nc.Subscribe (broker.go:1025-1028)
// and their handlers are registered directly as nats.go async callbacks with no goroutine wrapper.
// nats.go delivers one message at a time per subscription, so EVERY push on this broker is serialised
// behind whatever the current one is doing. Time spent blocking here is head-of-line blocking for all
// other transfers, which is why the budget is deliberately tighter than "retry until it works":
// worst case is ~1.5s + 8s = ~9.5s (1.9x the old 5s ceiling) and stays well under transferTimeoutTierA.
const (
	// xferSizingTimeout bounds the BEST-EFFORT sizing probe. jsStoreCeiling swallows its error and
	// falls back to statfs (transfer.go), so a stalled AccountInfo previously produced no failure at
	// all — it just silently ate the load-bearing create's budget. Mirrors the broker's own boot JS
	// probe (broker.go:1057).
	xferSizingTimeout = 1500 * time.Millisecond
	// xferCreateAttemptTO bounds ONE create attempt. Each attempt gets a fresh context; an attempt
	// that is silently dropped (nats-server returns without a reply when it holds no JetStream
	// leadership) costs the full value, so the arithmetic above assumes the expensive regime.
	xferCreateAttemptTO = 2500 * time.Millisecond
	// xferProvisionMaxTries caps attempts. A PERMANENT classification returns after exactly one, so a
	// misconfigured broker never pays the budget per request.
	xferProvisionMaxTries = 3
	// xferProvisionBackoff is the first inter-attempt wait; it grows x3 with +/-25% jitter.
	xferProvisionBackoff = 300 * time.Millisecond
	// xferProvisionBudget is the wall-clock ceiling for the whole create loop (excludes sizing).
	xferProvisionBudget = 8 * time.Second
	// xferProvisionMinSlack refuses to START an attempt that cannot run to completion. Internal review
	// (adopted): the first version set this to 1s — SMALLER than xferCreateAttemptTO — and only checked
	// it before the backoff sleep, so a third attempt could start with 1.68s of budget and fail with
	// DeadlineExceeded caused by OUR wall clock rather than by JetStream. The operator was then told
	// "JetStream did not accept the request" about an attempt JetStream was never given time to answer.
	// It must be at least one full attempt.
	xferProvisionMinSlack = xferCreateAttemptTO
)

// codeXferJSNotReady is the TRANSIENT tier-B provisioning refusal. It is a new value of the existing
// free-form `Code` string on PushPrepareResp/PullPrepareResp — purely ADDITIVE, no struct change, no
// proto.ProtoVersion bump: an older ctl prints an unknown code through its default branch exactly as
// before, and a newer ctl talking to an older broker simply never receives it.
//
// It exists so the permanent case can keep `bucket_create_failed` with its wording BYTE-UNCHANGED.
// That separation is the point: the permanent text deliberately contains none of retry/transient/
// temporary, so "retry shortly" cannot spread onto refusals where retrying is useless — the single
// most likely dishonest version of this fix.
const codeXferJSNotReady = "jetstream_not_ready"

// codeXferStoreTooSmall carves the G6 #21 disk-sizing refusal out of bucket_create_failed. G67 plan Q2
// deferred this split; the internal review (M5) made it load-bearing, because the exit taxonomy
// contradicted the prose: docs/usage.md §9.13 tells automation to treat 70 as RETRYABLE with backoff
// and only 64/77 as terminal, while bucket_create_failed maps to 70 (EX_SOFTWARE, "unclassified") and
// G67's own doc row calls it 永久态/重试无用. A monitor following the documented rule would retry a
// too-small disk forever, each attempt paying a full-file SHA-256.
//
// With the split each code means what it says: jetstream_not_ready = 75 (transient, retry),
// tier_b_store_too_small = 64 (terminal, the operator must give this broker more disk),
// bucket_create_failed = 70 (genuinely unclassified — retry with backoff AND report it).
// Additive: a new Code string value, no struct change, ProtoVersion unmoved.
const codeXferStoreTooSmall = "tier_b_store_too_small"

// codeXferBrokerRestarting is the refusal for "this broker's own lifetime ended while it was
// provisioning". Internal review (adopted): it used to reply bucket_create_failed => exit 70, i.e. a
// restarting broker — the single most retriable condition there is — was reported as an unclassified
// tether fault. Additive, like the others.
const codeXferBrokerRestarting = "broker_restarting"

// xferNoJetStreamMsg replaces the EMPTY Error string the nil-JS refusal used to carry. It must name
// BOTH possible causes: the broker's JetStream handle is established by a single 1s AccountInfo probe
// at boot (broker.go) and is never re-probed, so "JetStream is disabled" is not a claim this broker is
// entitled to make — the probe may simply have timed out on a busy host.
const xferNoJetStreamMsg = "this broker has no JetStream client, so tier-B transfers cannot be served. " +
	"Either JetStream is disabled in this broker's nats.conf, or the broker's one-shot JetStream probe " +
	"failed at boot; restarting the broker re-runs the probe"

// xferProvisionRefusal maps a provisioning failure to the reply code and the operator-visible text.
//
// The transient text states an observed FACT (n attempts over d) and names likely causes as likely —
// it never asserts quorum loss, because a permanently departed peer produces the identical
// DeadlineExceeded and this broker cannot tell the two apart. It ends with an escalation path so the
// operator is not left retrying forever against a genuinely broken cluster.
func xferProvisionRefusal(perr *xferProvisionErr) (string, string) {
	switch {
	case perr.Aborted:
		cause := "the broker is shutting down"
		if errors.Is(perr.AbortCause, context.DeadlineExceeded) {
			cause = "the broker's run context expired"
		}
		return codeXferBrokerRestarting, "tier-B provisioning was aborted before it completed (" + cause +
			"); this is not a JetStream verdict — retry once the broker is back"
	case perr.Transient:
		return codeXferJSNotReady, fmt.Sprintf(
			"tier-B object store could not be provisioned after %d attempt(s) over %s: %v — JetStream did not "+
				"accept the request. This class of failure is usually transient (a broker restart, a leader "+
				"election, or a cluster grow in progress); retry the same command shortly. If it persists, "+
				"run `tether cluster status`",
			perr.Attempts, perr.Elapsed.Round(100*time.Millisecond), perr.Err)
	case errors.Is(perr.Err, errXferStoreTooSmall):
		// PERMANENT and OPERATOR-ACTIONABLE. Text byte-unchanged; only the code (and therefore the
		// exit class) is sharpened, so the operator sees the same words and automation stops retrying.
		return codeXferStoreTooSmall, perr.Err.Error()
	default:
		// PERMANENT but UNCLASSIFIED: byte-identical to what this path replied before #67.
		return "bucket_create_failed", perr.Err.Error()
	}
}

// xferTooLarge is the G6 #21 admission refusal, returned as a TYPED value rather than an error so it
// cannot silently degrade into a provisioning failure. The caller replies with the pre-existing
// `too_large` code and its pre-existing wording.
type xferTooLarge struct{ MaxBytes int64 }

// xferProvisionErr carries everything the refusal text and the operator log need.
type xferProvisionErr struct {
	Err        error
	Transient  bool  // classified by jsstream.IsTransientProvisionErr; drives the reply code
	Aborted    bool  // the broker's own lifetime ended mid-provisioning — NOT a JetStream verdict
	AbortCause error // context.Canceled (shutdown) vs DeadlineExceeded (the parent carried a deadline)
	Attempts   int
	Elapsed    time.Duration
	SizingMs   int64 // #67 plan Q1: measure, never assume — these two settle which round trip stalls
	CreateMs   int64
}

func (e *xferProvisionErr) Error() string { return e.Err.Error() }

// provisionXferBucket provisions the per-session tier-B object store with SEPARATE deadlines for the
// best-effort sizing probe and the load-bearing create, plus a bounded classified retry of the create.
//
// PRECONDITION (load-bearing — see the R7a ordering guard in transfer.go): this runs BEFORE
// transfers.put(), so it touches neither the tracker, the #57 in-flight ledger, the audit start row,
// the watchdog, nor the orphan reaper. Nothing here may be moved after put(): no tracker slot is held
// while provisioning (so the 1024-entry cap is unaffected), the transfer's own watchdog budget is not
// consumed, and a crash mid-provisioning leaves no ledger file and no dangling start row.
//
// parent MUST be b.runCtx, not context.Background(): with Background() the shutdown branch is dead
// code AND ordinary budget exhaustion would be reported as "the broker is shutting down" — a brand
// new false statement of exactly the species #67 exists to remove.
func (b *Broker) provisionXferBucket(parent context.Context, sid string, size int64) (string, *xferTooLarge, *xferProvisionErr) {
	if parent == nil {
		parent = context.Background()
	}
	start := time.Now()

	// ── 1. sizing, on its OWN short deadline ────────────────────────────────────────────────
	sctx, scancel := context.WithTimeout(parent, xferSizingTimeout)
	sizingStart := time.Now()
	maxBytes, sizeErr := b.xferBucketMaxBytes(sctx)
	sizingMs := time.Since(sizingStart).Milliseconds()
	scancel()

	// ── 2. admission (G6 #21), semantics unchanged ──────────────────────────────────────────
	if sizeErr == nil && size > maxBytes {
		return "", &xferTooLarge{MaxBytes: maxBytes}, nil
	}

	// ── 3. a too-small store is PERMANENT: zero create attempts ─────────────────────────────
	// ensureXferBucketSized already short-circuits on sizeErr without touching JetStream; making it
	// explicit here keeps the "no attempts were made" fact out of the attempt counter.
	if sizeErr != nil {
		return "", nil, &xferProvisionErr{
			Err: fmt.Errorf("xfer bucket sizing: %w", sizeErr), Transient: false,
			Attempts: 0, Elapsed: time.Since(start), SizingMs: sizingMs,
		}
	}

	// ── 4. create loop ──────────────────────────────────────────────────────────────────────
	deadline := time.Now().Add(xferProvisionBudget)
	wait := xferProvisionBackoff
	var lastErr error
	var createMs int64
	attempts := 0

	for attempts < xferProvisionMaxTries {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		attemptTO := xferCreateAttemptTO
		if remaining < attemptTO {
			attemptTO = remaining
		}
		attempts++

		actx, acancel := context.WithTimeout(parent, attemptTO)
		createStart := time.Now()
		bucket, err := b.ensureXferBucketSized(actx, sid, b.xferTargetReplicas(), maxBytes, nil)
		createMs += time.Since(createStart).Milliseconds()
		acancel() // NOT deferred: this is a loop.

		if err == nil {
			if attempts > 1 {
				b.cfg.Logger.Info("broker: tier-B bucket provisioned after retry",
					"sid", sid, "attempts", attempts, "elapsed_ms", time.Since(start).Milliseconds(),
					"sizing_ms", sizingMs, "create_ms", createMs)
			}
			return bucket, nil, nil
		}
		lastErr = err

		// The broker's own lifetime ended: stop immediately and say so honestly. Internal review
		// (adopted, two findings): (1) this used to set Aborted only for context.Canceled, so a parent
		// that ended by DEADLINE fell through to the permanent branch and the operator was told
		// "create_bucket: context deadline exceeded" under the PERMANENT code — a JetStream verdict
		// about a JetStream that was never asked; (2) a broker that is shutting down is the most
		// RETRIABLE condition there is, so it must not carry a terminal class either.
		if parent.Err() != nil {
			return "", nil, &xferProvisionErr{
				Err: lastErr, Transient: false, Aborted: true,
				AbortCause: parent.Err(),
				Attempts:   attempts, Elapsed: time.Since(start), SizingMs: sizingMs, CreateMs: createMs,
			}
		}
		if !jsstream.IsTransientProvisionErr(err) {
			break // permanent: one attempt is all a misconfiguration ever gets
		}
		if attempts >= xferProvisionMaxTries {
			break
		}
		jittered := jitter(wait)
		if time.Until(deadline) < jittered+xferProvisionMinSlack {
			break // no room left to run another attempt to completion
		}
		b.cfg.Logger.Warn("broker: tier-B bucket provisioning retried",
			"sid", sid, "attempt", attempts, "backoff_ms", jittered.Milliseconds(), "err", err)
		if !sleepCtx(parent, jittered) {
			return "", nil, &xferProvisionErr{
				Err: lastErr, Transient: false, Aborted: true, AbortCause: parent.Err(),
				Attempts: attempts, Elapsed: time.Since(start), SizingMs: sizingMs, CreateMs: createMs,
			}
		}
		wait *= 3
	}

	transient := jsstream.IsTransientProvisionErr(lastErr)
	b.cfg.Logger.Warn("broker: tier-B bucket provisioning gave up",
		"sid", sid, "attempts", attempts, "elapsed_ms", time.Since(start).Milliseconds(),
		"sizing_ms", sizingMs, "create_ms", createMs, "transient", transient, "err", lastErr)
	return "", nil, &xferProvisionErr{
		Err: lastErr, Transient: transient, Attempts: attempts,
		Elapsed: time.Since(start), SizingMs: sizingMs, CreateMs: createMs,
	}
}

// jitter spreads retries by +/-25% so a fleet coming out of a rolling broker restart does not
// re-converge on the same instants.
func jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	delta := int64(d) / 2 // full width = 50% of d, i.e. +/-25%
	return time.Duration(int64(d) - delta/2 + rand.Int64N(delta+1))
}

// sleepCtx waits d, or returns false if ctx ends first. time.NewTimer + Stop rather than time.After,
// which pins a timer for its full duration and shows up in the repo's own goroutine/fd leak gate.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
