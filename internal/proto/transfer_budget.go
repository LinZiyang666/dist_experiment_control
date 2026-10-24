package proto

import (
	"errors"
	"time"
)

// Storage keeps the worst accepted byte deadline plus the late-publish margin.
// This is a minimum, never permission to shorten an operator's existing TTL.
// origin: hot-update S7 plan §1.2 / §2.1 / lead LQ3 / P01
const (
	ObjectRecoveryHorizon     = XferTierBMaxBudget + 120*time.Second
	TransferMinChunkSize      = 32 * 1024
	TransferMaxChunkSize      = 1024 * 1024
	TransferMaxChunks         = XferMaxBytes / TransferMinChunkSize
	TransferResultRetention   = 10 * time.Minute
	TransferSettlementAttempt = 5 * time.Second
)

// TransferBudget is admission for a sealed, known source. Unlike the legacy
// XferBudget helper, invalid values are refused rather than clamped. A requested
// slow rate that needs more than 2108s cannot be made safe by truncating it.
// origin: hot-update S7 plan §1.2 / lead LQ2 / P01
func TransferBudget(tier string, size, rateLimitBPS int64) (time.Duration, error) {
	if (tier != "a" && tier != "b") || size < 0 || size > XferMaxBytes || rateLimitBPS < 0 {
		return 0, errors.New(CodeTransferBudgetExceeded)
	}
	if tier == "a" {
		if size > XferTierAMaxBytes {
			return 0, errors.New(CodeTransferBudgetExceeded)
		}
		if rateLimitBPS == 0 {
			return XferTimeoutTierA, nil
		}
		// Explicit pacing is part of the sealed admission budget for inline
		// bytes too. A slow cap cannot outlive the original 2108-second fence.
		// origin: hot-update S7 plan lead LQ2 / INTEG-D
		// Inline pacing must fit both byte legs inside the frozen 30s total.
		// The tier-B overhead margin cannot extend an inline invocation.
		// origin: S7 internal review rv-wiring F8 / hot-update S7 plan §1.2
		seconds := size / rateLimitBPS
		if size%rateLimitBPS != 0 {
			seconds++
		}
		if seconds > int64(XferTimeoutTierA/time.Second)/XferPushLegs {
			return 0, errors.New(CodeTransferBudgetExceeded)
		}
		return XferTimeoutTierA, nil
	}
	leg, err := TransferLegBudget(size, rateLimitBPS)
	if err != nil {
		return 0, err
	}
	budget := XferPushLegs*leg + XferOverheadMargin
	if budget < XferTimeoutTierBFloor {
		budget = XferTimeoutTierBFloor
	}
	if budget > XferTierBMaxBudget {
		return 0, errors.New(CodeTransferBudgetExceeded)
	}
	return budget, nil
}

// Check seconds before multiplying by time.Second. Even damaged int64 ledger
// fields cannot wrap the duration and turn a refusal into the five-minute floor.
// Division plus remainder avoids an overflowing size+rate-1 ceiling expression.
// origin: hot-update S7 plan §1.2 / lead LQ2 / P01
func TransferLegBudget(size, rateLimitBPS int64) (time.Duration, error) {
	if size < 0 || size > XferMaxBytes || rateLimitBPS < 0 {
		return 0, errors.New(CodeTransferBudgetExceeded)
	}
	rate := int64(XferMinThroughput)
	if rateLimitBPS != 0 && rateLimitBPS < rate {
		rate = rateLimitBPS
	}
	seconds := size / rate
	if size%rate != 0 {
		seconds++
	}
	if seconds > int64((XferTierBMaxBudget-XferOverheadMargin)/XferPushLegs/time.Second) {
		return 0, errors.New(CodeTransferBudgetExceeded)
	}
	return time.Duration(seconds) * time.Second, nil
}

// NewTransferBudgetSeal receives elapsed local BOOTTIME, not a comparison of
// wire wall clocks. Unsealed pull sources get the provisional ceiling; source
// discovery must call SealTransferSourceBudget with elapsed since this start.
// origin: hot-update S7 plan §1.2 / P01
func NewTransferBudgetSeal(tier string, size, rateLimitBPS int64, startedAt time.Time, elapsed time.Duration, sourceSealed bool) (BudgetSeal, error) {
	budget, err := TransferBudget(tier, size, rateLimitBPS)
	if err != nil {
		return BudgetSeal{}, err
	}
	if !sourceSealed && tier == "b" {
		budget = XferTierBMaxBudget
	}
	if startedAt.IsZero() || elapsed < 0 || elapsed >= budget {
		return BudgetSeal{}, errors.New(CodeTransferBudgetExceeded)
	}
	return BudgetSeal{StartedAt: startedAt, Size: size, RateLimitBPS: rateLimitBPS,
		EffectiveBudgetNS: int64(budget), RemainingNS: int64(budget - elapsed), SourceSealed: sourceSealed}, nil
}

// Source facts tighten the original invocation. They never replace StartedAt,
// the requested rate, or a smaller remaining deadline already consumed in transit.
// elapsed is total time since original admission, supplied by the local owner.
// origin: hot-update S7 plan §1.2 / P01
func SealTransferSourceBudget(previous BudgetSeal, tier string, source SourceSeal, elapsed time.Duration) (BudgetSeal, error) {
	if previous.SourceSealed || previous.StartedAt.IsZero() || source.RateLimitBPS != previous.RateLimitBPS ||
		previous.RemainingNS <= 0 || previous.RemainingNS > previous.EffectiveBudgetNS || previous.EffectiveBudgetNS > int64(XferTierBMaxBudget) {
		return BudgetSeal{}, errors.New(CodeTransferIntentConflict)
	}
	seal, err := NewTransferBudgetSeal(tier, source.Size, previous.RateLimitBPS, previous.StartedAt, elapsed, true)
	if err != nil {
		return BudgetSeal{}, err
	}
	if seal.RemainingNS > previous.RemainingNS {
		seal.RemainingNS = previous.RemainingNS
	}
	return seal, nil
}

// Each request receipt consumes the entire measured request/queue interval.
// No remote BOOTTIME value is compared with the receiver's local clock.
// origin: hot-update S7 plan §1.2 / P01
func ConsumeTransferBudget(seal BudgetSeal, elapsed time.Duration) (BudgetSeal, error) {
	if elapsed < 0 || seal.StartedAt.IsZero() || seal.EffectiveBudgetNS <= 0 ||
		seal.EffectiveBudgetNS > int64(XferTierBMaxBudget) || seal.RemainingNS <= 0 ||
		seal.RemainingNS > seal.EffectiveBudgetNS || int64(elapsed) >= seal.RemainingNS {
		return BudgetSeal{}, errors.New(CodeTransferBudgetExceeded)
	}
	seal.RemainingNS -= int64(elapsed)
	return seal, nil
}

// Callers supply the actual encoded longest headers, including both CAS guards
// and MsgID. streamMaxMsgSize<=0 means the stream has no separate message cap;
// the three connection MaxPayload witnesses must always be positive.
// origin: hot-update S7 plan §2.1 / P01 (integration pressure test T09 owned by P02)
func TransferChunkGeometry(size, ctlMaxPayload, agentMaxPayload, brokerMaxPayload, streamMaxMsgSize, headerBytes int64) (chunkSize, chunks uint32, err error) {
	if size < 0 || size > XferMaxBytes || ctlMaxPayload <= 0 || agentMaxPayload <= 0 || brokerMaxPayload <= 0 || headerBytes <= 0 {
		return 0, 0, errors.New(CodeTransferPayloadUnsupported)
	}
	limit := min(ctlMaxPayload, agentMaxPayload, brokerMaxPayload)
	if streamMaxMsgSize > 0 {
		limit = min(limit, streamMaxMsgSize)
	}
	if headerBytes >= limit {
		return 0, 0, errors.New(CodeTransferPayloadUnsupported)
	}
	geometry := min(limit-headerBytes, int64(TransferMaxChunkSize))
	if geometry < TransferMinChunkSize {
		return 0, 0, errors.New(CodeTransferPayloadUnsupported)
	}
	count := size / geometry
	if size%geometry != 0 {
		count++
	}
	if count > TransferMaxChunks {
		return 0, 0, errors.New(CodeTransferCapacityExceeded)
	}
	return uint32(geometry), uint32(count), nil
}
