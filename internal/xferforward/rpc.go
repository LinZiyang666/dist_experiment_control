// Package xferforward owns transfer-only cluster RPC proofs and deadlines.
// Broker adapters retain subscription admission and resolve current backends.
// origin: hot-update S7 plan §6.3 / §8.2 / T39
package xferforward

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats.go"
	"golang.org/x/time/rate"
)

const (
	VerbTransferBucketConfig = "transfer-bucket-config"
	VerbTransferCensus       = "transfer-census"
	VerbTransferProbe        = "transfer-probe"
	fwdStatusError           = "error"
	fwdStatusOK              = "ok"
)

type Envelope struct {
	ReqID   string          `json:"req_id"`
	Verb    string          `json:"verb"`
	Payload json.RawMessage `json:"payload"`
}
type Config struct {
	Origin  xfercontract.Origin
	Buckets xfercontract.BucketPort
	Census  func(xfercontract.Origin, controlbudget.Deadline) (xfercontract.LedgerCensus, error)
	Probe   func(controlbudget.Deadline) (xfercontract.ProbeResult, error)
}

// Client borrows only this binding's transport and limiter. It retains no store.
type Client struct {
	NC      *nats.Conn
	Timeout time.Duration
	Limiter *rate.Limiter
	Observe func(string, error)
}
type Refusal struct{ Kind, Msg string }

func (e *Refusal) Error() string        { return e.Msg }
func (e *Refusal) TransferCode() string { return e.Kind }

// A relative budget crosses hosts; CLOCK_BOOTTIME timestamps never do. The
// receiver bounds it again to a control attempt, rather than minting five new
// seconds for each lookup/update inside the callback.
type Request struct {
	Origin   xfercontract.Origin       `json:"origin"`
	Patch    *xfercontract.ConfigPatch `json:"patch,omitempty"`
	BudgetNS int64                     `json:"budget_ns"`
}
type Reply struct {
	Status string                    `json:"status"`
	Code   string                    `json:"code,omitempty"`
	Error  string                    `json:"error,omitempty"`
	Seal   *xfercontract.BucketSeal  `json:"seal,omitempty"`
	Census *CensusProof              `json:"census,omitempty"`
	Probe  *xfercontract.ProbeResult `json:"probe,omitempty"`
}

const Budget = 5 * time.Second

// Census transports protection, never ledger rows. Rows carry receiver
// settlement authentication and canonical publications and belong to sealed
// state/local disk; an absence proof needs neither of those payloads.
// origin: hot-update S7 plan §6.2 / §6.3 / T39
type CensusProof struct {
	Origin   xfercontract.Origin       `json:"origin"`
	Ready    bool                      `json:"ready"`
	Complete bool                      `json:"complete"`
	Entries  []xfercontract.Protection `json:"entries"`
	Digest   string                    `json:"digest"`
	Code     string                    `json:"code,omitempty"`
}

func IsVerb(verb string) bool {
	switch verb {
	case VerbTransferBucketConfig, VerbTransferCensus, VerbTransferProbe:
		return true
	default:
		return false
	}
}

func Dispatch(cfg Config, env Envelope, req Request) Reply {
	fail := func(code string) Reply {
		return Reply{Status: fwdStatusError, Code: code}
	}
	if env.ReqID != "" || req.BudgetNS <= 0 || req.BudgetNS > int64(Budget) {
		return fail("bad_request")
	}
	deadline, err := controlbudget.After(time.Duration(req.BudgetNS))
	if err != nil {
		return fail(xfercontract.CodeResultUnknown)
	}
	if env.Verb == VerbTransferCensus && req.Origin != cfg.Origin {
		return fail(xfercontract.CodeOwnerRecovering)
	}
	reply := Reply{Status: fwdStatusOK}
	switch env.Verb {
	case VerbTransferBucketConfig:
		if req.Patch == nil || req.Origin != (xfercontract.Origin{}) {
			return fail("bad_request")
		}
		if cfg.Buckets == nil {
			return fail(xfercontract.CodeResultUnknown)
		}
		seal, applyErr := cfg.Buckets.Apply(*req.Patch, deadline)
		if applyErr != nil {
			failure := fail(ErrorCode(applyErr))
			var coded interface{ TransferCode() string }
			if errors.As(applyErr, &coded) && coded.TransferCode() != "" {
				// Only deliberately public refusals cross the bus. Horizon detail
				// includes required Duplicates/MaxAge, while arbitrary SQL/SDK
				// diagnostics never become client error text.
				// origin: hot-update S7 plan lead LQ3 / §2.1 / T39
				failure.Error = applyErr.Error()
			}
			return failure
		}
		reply.Seal = &seal
	case VerbTransferCensus:
		if req.Patch != nil {
			return fail("bad_request")
		}
		if cfg.Census == nil {
			return fail(xfercontract.CodeOwnerRecovering)
		}
		census, censusErr := cfg.Census(req.Origin, deadline)
		if censusErr != nil || census.Origin != req.Origin || !census.Ready || !census.Complete || census.Digest == "" || census.Code != "" {
			return fail(xfercontract.CodeOwnerRecovering)
		}
		reply.Census = &CensusProof{Origin: census.Origin, Ready: census.Ready, Complete: census.Complete, Entries: census.Entries, Digest: census.Digest}
	case VerbTransferProbe:
		if req.Patch != nil {
			return fail("bad_request")
		}
		if cfg.Probe == nil {
			return fail(xfercontract.CodeOwnerRecovering)
		}
		probe, probeErr := cfg.Probe(deadline)
		if probeErr != nil {
			return fail(ErrorCode(probeErr))
		}
		reply.Probe = &probe
	default:
		return fail("bad_request")
	}
	// A late successful operation is still sent-unknown to the caller whose
	// budget ended. It cannot become first-byte or retirement permission.
	// origin: hot-update S7 plan §6.3 / §8.2 / T39
	if _, err := controlbudget.Remaining(deadline); err != nil {
		return fail(xfercontract.CodeResultUnknown)
	}
	return reply
}

// Preserve stable refusals without serializing arbitrary store/SDK errors.
// A failed capability/config call supplies no downgrade or absence evidence.
func ErrorCode(err error) string {
	var coded interface{ TransferCode() string }
	if errors.As(err, &coded) && coded.TransferCode() != "" {
		return coded.TransferCode()
	}
	return xfercontract.CodeResultUnknown
}

// forwardTransferRPC shares one absolute budget between limiter and request.
// An unavailable original owner is never translated to an empty census, and
// a config timeout does not authorize another coordinator or an automatic retry.
// origin: hot-update S7 plan §6.3 / §8.2 / T39
func RPC(f *Client, verb string, req Request, deadline controlbudget.Deadline) (reply Reply, err error) {
	defer func() {
		if f != nil && f.Observe != nil {
			f.Observe(verb, err)
		}
	}()
	left, err := controlbudget.Remaining(deadline)
	if err != nil {
		return reply, err
	}
	if f == nil || f.NC == nil || !IsVerb(verb) {
		return reply, errors.New("broker: transfer forwarding connection is missing")
	}
	if f.Timeout <= 0 {
		return reply, controlbudget.ErrInvalidDeadline
	}
	left = min(left, f.Timeout, Budget)
	// ctx-none: broker forwarding ports carry an absolute deadline, not a context.
	ctx, cancel := context.WithTimeout(context.Background(), left)
	defer cancel()
	if f.Limiter != nil {
		if err := f.Limiter.Wait(ctx); err != nil {
			return reply, err
		}
	}
	end, _ := ctx.Deadline()
	req.BudgetNS = int64(time.Until(end))
	if req.BudgetNS <= 0 {
		return reply, controlbudget.ErrExpired
	}
	payload, err := json.Marshal(req)
	if err != nil {
		return reply, err
	}
	raw, err := json.Marshal(Envelope{Verb: verb, Payload: payload})
	if err != nil {
		return reply, err
	}
	msg, err := f.NC.RequestWithContext(ctx, proto.SubjClusterApply(verb), raw)
	if err != nil {
		return reply, fmt.Errorf("broker: transfer owner reply unavailable: %w", err)
	}
	if err := json.Unmarshal(msg.Data, &reply); err != nil {
		return reply, fmt.Errorf("broker: invalid transfer forwarding reply: %w", err)
	}
	if reply.Status != fwdStatusOK || reply.Code != "" || reply.Error != "" {
		code := reply.Code
		if code == "" {
			code = xfercontract.CodeResultUnknown
		}
		message := reply.Error
		if message == "" {
			message = "broker: transfer forwarding refused: " + code
		}
		return reply, &Refusal{Kind: code, Msg: message}
	}
	return reply, nil
}

func Apply(f *Client, patch xfercontract.ConfigPatch, deadline controlbudget.Deadline) (xfercontract.BucketSeal, error) {
	reply, err := RPC(f, VerbTransferBucketConfig, Request{Patch: &patch}, deadline)
	if err != nil {
		return xfercontract.BucketSeal{}, err
	}
	if reply.Seal == nil || reply.Census != nil || reply.Probe != nil || reply.Seal.Bucket != patch.Bucket || reply.Seal.ConfigDigest == "" {
		return xfercontract.BucketSeal{}, errors.New("broker: transfer config reply has no matching seal")
	}
	return *reply.Seal, nil
}

// TransferOwnerResolver implements the neutral census port over the broker bus.
// It holds only the current connection; the complete requested Origin travels
// unchanged to its original responder, including a follower after rehome.
type OwnerResolver struct{ Client *Client }

var _ xfercontract.OwnerResolver = OwnerResolver{}

func (r OwnerResolver) Census(origin xfercontract.Origin, deadline controlbudget.Deadline) (xfercontract.LedgerCensus, error) {
	if origin.BrokerID == "" {
		return xfercontract.LedgerCensus{}, errors.New("broker: transfer census origin is missing")
	}
	reply, err := RPC(r.Client, VerbTransferCensus, Request{Origin: origin}, deadline)
	if err != nil {
		return xfercontract.LedgerCensus{}, err
	}
	if reply.Census == nil || reply.Seal != nil || reply.Probe != nil || reply.Census.Origin != origin || !reply.Census.Ready || !reply.Census.Complete || reply.Census.Digest == "" || reply.Census.Code != "" {
		return xfercontract.LedgerCensus{}, errors.New("broker: transfer census reply is incomplete")
	}
	return xfercontract.LedgerCensus{Origin: reply.Census.Origin, Ready: reply.Census.Ready, Complete: reply.Census.Complete, Entries: reply.Census.Entries, Digest: reply.Census.Digest}, nil
}

// ProbeTransferBroker is deliberately directed. Current-home capability alone
// cannot certify all eligible publishers/config writers in an N3 admission.
func Probe(f *Client, origin xfercontract.Origin, deadline controlbudget.Deadline) (xfercontract.ProbeResult, error) {
	if origin.BrokerID == "" {
		return xfercontract.ProbeResult{}, errors.New("broker: transfer probe origin is missing")
	}
	reply, err := RPC(f, VerbTransferProbe, Request{Origin: origin}, deadline)
	if err != nil {
		return xfercontract.ProbeResult{}, err
	}
	if reply.Probe == nil || reply.Seal != nil || reply.Census != nil || reply.Probe.InstanceID == "" || (origin.InstanceID != "" && reply.Probe.InstanceID != origin.InstanceID) || reply.Probe.Code != "" || reply.Probe.Error != "" {
		return xfercontract.ProbeResult{}, errors.New("broker: transfer probe reply has no matching owner")
	}
	return *reply.Probe, nil
}
