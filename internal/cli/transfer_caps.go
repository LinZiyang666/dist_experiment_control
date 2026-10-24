package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
	"time"
)

// ProbeTransferCaps issues `ctrl.by.<actor>.s.<sid>.caps.req` and returns the broker's reported
// capabilities. Used by ChooseTransferTier so we don't shoot a tier-B request at a broker without JetStream,
// or a tier-A request that exceeds the server max_payload.
//
// #67 face B: its ERROR IS LOAD-BEARING and callers must not discard it. Failing this probe means we
// learned NOTHING; it does not mean the broker has no JetStream. Go through ProbeTransferCapsClassified,
// which encodes that distinction in the type. The parent context makes Ctrl-C interrupt the probe
// instead of waiting out its timeout.
func ProbeTransferCaps(parent context.Context, nc *nats.Conn, actor, sid string, timeout time.Duration) (proto.CapsResp, error) {
	body, _ := json.Marshal(proto.CapsReq{})
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	subj := proto.SubjCtrlCaps(actor, sid)
	msg, err := nc.RequestWithContext(ctx, subj, body)
	if err != nil {
		return proto.CapsResp{}, err
	}
	var resp proto.CapsResp
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		return proto.CapsResp{}, fmt.Errorf("caps: parse: %w", err)
	}
	return resp, nil
}

// TransferCapsStatus classifies what a caps probe actually established. #67 face B existed because these
// three collapsed into one zero-value CapsResp, making "no answer" indistinguishable from
// "authoritatively: no JetStream".
type TransferCapsStatus int

const (
	TransferCapsUndetermined TransferCapsStatus = iota // transport or parse failure — NO usable answer
	TransferCapsRefused                                // the broker answered but declined (OK=false)
	TransferCapsOK                                     // authoritative
)

type TransferCapsProbe struct {
	Status TransferCapsStatus
	Resp   proto.CapsResp
	Err    error
}

// warning returns the stderr note for a probe that produced no authoritative answer, or "" when the
// answer was authoritative. It is a WARNING, never an error: the broker is one RPC away and is the
// single adjudicator of whether tier B can be served.
func (p TransferCapsProbe) Warning() string {
	switch p.Status {
	case TransferCapsUndetermined:
		return fmt.Sprintf("warning: could not read broker capabilities (%v); proceeding and letting the broker decide", p.Err)
	case TransferCapsRefused:
		return fmt.Sprintf("warning: the broker declined to report its capabilities (%s); that says nothing about JetStream, so proceeding and letting the broker decide", p.Resp.Code)
	case TransferCapsOK:
		return "" // the probe answered; nothing to warn about
	default:
		return ""
	}
}

// ProbeTransferCapsClassified runs the caps probe and CLASSIFIES the outcome instead of discarding the error.
// It deliberately does NOT retry: once a failed probe means "let the broker decide", a retry buys
// nothing but latency on every transfer.
func ProbeTransferCapsClassified(ctx context.Context, nc *nats.Conn, actor, sid string, timeout time.Duration) TransferCapsProbe {
	return ClassifyTransferCaps(ProbeTransferCaps(ctx, nc, actor, sid, timeout))
}

// ClassifyTransferCaps is the pure half of the probe, split out so the three-way classification is
// directly testable. Internal review B1: with it folded into the RPC wrapper, flipping the
// `!resp.OK` arm to TransferCapsOK left every Go test green — and that flip alone reinstates #67 face B,
// because a broker that merely DECLINED to answer (`not_a_member`) would be treated as authoritative
// and ChooseTransferTier would refuse with a permanent capability claim the broker never made.
func ClassifyTransferCaps(resp proto.CapsResp, err error) TransferCapsProbe {
	switch {
	case err != nil:
		return TransferCapsProbe{Status: TransferCapsUndetermined, Err: err}
	case !resp.OK:
		return TransferCapsProbe{Status: TransferCapsRefused, Resp: resp}
	default:
		return TransferCapsProbe{Status: TransferCapsOK, Resp: resp}
	}
}

// TransferInlineCeiling clamps the inline (tier-A) budget by EVERY measurement actually available and
// NEVER by one that is missing.
//
// #67 face B, second half: the old code clamped only `if caps.MaxPayload > 0`, so a zero-value
// CapsResp from a FAILED probe silently RAISED the ceiling back to the 8 MiB design default and moved
// the tier-A/B boundary without telling anyone. nc.MaxPayload() is ground truth for what THIS client
// may publish and is populated from server INFO on any connected conn, so it is always available;
// the broker's own caps.MaxPayload is honoured only when the probe was authoritative.
func TransferInlineCeiling(connMaxPayload int64, probe TransferCapsProbe) int64 {
	ceiling := int64(proto.XferTierAMaxBytes)
	clamp := func(p int64) {
		if p <= 0 {
			return
		}
		half := p/2 - 1024
		if half < 0 {
			half = 0
		}
		if half < ceiling {
			ceiling = half
		}
	}
	clamp(connMaxPayload)
	if probe.Status == TransferCapsOK {
		clamp(probe.Resp.MaxPayload)
	}
	return ceiling
}

// ChooseTransferTier decides tier A vs B for a given file size and a CLASSIFIED caps probe.
//
//   - size <= the inline ceiling            -> tier A. A failed probe must never block a tier-A
//     transfer, so this is decided first.
//   - authoritative + JetStream ready       -> tier B.
//   - authoritative + JetStream NOT ready   -> refuse. This is the ONLY case where the broker really
//     told us tier B cannot be served, so it is the only case entitled to say so.
//   - no authoritative answer               -> tier B, with a stderr warning. The broker adjudicates
//     (handlePushReq refuses with real prose if it has no JetStream). Refusing locally would mean
//     inventing a second claim — which is exactly what #67 was.
//
// Returns ("a"|"b", err). It used to also return maxInline, which no caller -- production or test --
// ever read.
func ChooseTransferTier(size int64, probe TransferCapsProbe, connMaxPayload int64) (string, error) {
	if size > proto.XferMaxBytes {
		return "", fmt.Errorf("too_large: file size %d > %d (use `tether expose` + rsync)", size, proto.XferMaxBytes)
	}
	maxInline := TransferInlineCeiling(connMaxPayload, probe)
	if size <= maxInline {
		return "a", nil
	}
	if probe.Status == TransferCapsOK && !probe.Resp.JetStreamReady {
		// The max_payload clause is offered ONLY when raising it would actually help — i.e. when the
		// file could travel inline. For a file above the 8 MiB tier-A design ceiling it never can,
		// and offering it (as the pre-#67 message did unconditionally) is misdirection.
		if size <= proto.XferTierAMaxBytes {
			return "", usageErr("jetstream_unavailable: this broker reports JetStream is not available, so tier B cannot be served, and this %d-byte file does not fit the current tier-A inline budget of %d bytes. Raise the broker's nats max_payload to at least %d bytes so this file travels inline as tier A, enable JetStream on the broker (docs/broker-ops.md), or use `tether expose` + rsync",
				size, maxInline, size*2+2048)
		}
		return "", usageErr("jetstream_unavailable: this broker reports JetStream is not available, so tier B (files larger than %d bytes) cannot be served. Enable JetStream on the broker (docs/broker-ops.md), or use `tether expose` + rsync",
			maxInline)
	}
	return "b", nil
}
