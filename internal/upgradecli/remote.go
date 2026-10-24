package upgradecli

import (
	"context"
	"errors"
	"reflect"

	"github.com/LinZiyang666/tether/internal/upgradeartifact"
	"github.com/LinZiyang666/tether/internal/upgradeport"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/internal/upgradewire"
)

// Exchange performs exactly one request/reply round trip for one strict
// transaction envelope. It never retries: a lost reply to a mutation may follow
// an accepted effect, so only a fresh Query of the original ID can settle it.
// A failure that provably happened before any byte reached the target must be
// wrapped in NotSentError; every other failure is treated as possibly sent.
// origin: hot-update S8 plan §3.1 / §7.1 / §11.2 P19
type Exchange func(ctx context.Context, request upgradewire.Request) (upgradewire.Response, error)

// NotSentError marks a transport refusal that cannot have delivered the
// request (encoding failure, closed connection, no subscriber). It is the only
// evidence that lets a mutation be reported as retryable instead of unknown.
type NotSentError struct{ Err error }

func (e *NotSentError) Error() string { return "upgrade transaction was not sent: " + e.Err.Error() }
func (e *NotSentError) Unwrap() error { return e.Err }

// Remote is the ctl-side upgradeport.Endpoint. The target daemon remains the
// only journal writer and the only issuer of IDs; this adapter only encodes the
// frozen per-operation envelope and classifies what came back. It never fills
// in a missing field, never converts an undecodable reply into an empty View,
// and never turns a mutation's lost reply into a retryable refusal.
// origin: hot-update S8 plan §2.2 / §7.1 / §11.2 P19
type Remote struct {
	Exchange Exchange
}

var _ upgradeport.Endpoint = Remote{}

func (r Remote) Prepare(ctx context.Context, key upgradetx.Key, req upgradetx.PrepareRequest) (upgradetx.Grant, error) {
	view, err := r.call(ctx, upgradewire.Request{Op: upgradewire.OpPrepare, Key: key, Prepare: req})
	if err != nil {
		return upgradetx.Grant{}, err
	}
	grant := view.Historical.Grant
	// The target issues the execution identity; every caller-supplied binding
	// must come back unchanged. A grant naming another component, actor, nonce
	// or SHA pair is not authorization for this request, whatever its shape.
	// origin: hot-update S8 plan §3.1 / §5.2 / M16
	if grant.Handle.Key != key || view.Historical.Key != key || !hexValue(grant.Handle.TxID, 32) || grant.ClientNonce != req.ClientNonce ||
		grant.ActorFP != req.ActorFP || grant.Mode != req.Mode || grant.SourceSHA != req.SourceSHA || grant.TargetSHA != req.TargetSHA ||
		grant.ParentTxID != req.ParentTxID || grant.ParentSlot != req.ParentSlot || grant.CapabilityDigest != req.CapabilityDigest {
		return upgradetx.Grant{}, unsettled("prepare reply does not bind the requested authorization")
	}
	return grant, nil
}

func (r Remote) Start(ctx context.Context, grant upgradetx.Grant, artifact upgradeartifact.Receipt) (upgradetx.View, error) {
	return r.call(ctx, upgradewire.Request{Op: upgradewire.OpStart, Key: grant.Handle.Key, Grant: grant, Artifact: artifact})
}

func (r Remote) Query(ctx context.Context, key upgradetx.Key, txID, nonce string) (upgradetx.View, error) {
	return r.call(ctx, upgradewire.Request{Op: upgradewire.OpQuery, Key: key, TxID: txID, QueryNonce: nonce})
}

func (r Remote) Rollback(ctx context.Context, expected upgradetx.Fence, req upgradetx.ReverseRequest) (upgradetx.View, error) {
	return r.call(ctx, upgradewire.Request{Op: upgradewire.OpRollback, Key: expected.Current.Key, Expected: expected, Reverse: req})
}

func (r Remote) Cancel(ctx context.Context, expected upgradetx.Handle) (upgradetx.View, error) {
	return r.call(ctx, upgradewire.Request{Op: upgradewire.OpCancel, Key: expected.Key, Expected: upgradetx.Fence{Current: expected}})
}

func (r Remote) Archive(ctx context.Context, expected upgradetx.Handle) (upgradetx.View, error) {
	return r.call(ctx, upgradewire.Request{Op: upgradewire.OpArchive, Key: expected.Key, Expected: upgradetx.Fence{Current: expected}})
}

func (r Remote) CloseWindow(ctx context.Context, expected upgradetx.Handle) (upgradetx.View, error) {
	return r.call(ctx, upgradewire.Request{Op: upgradewire.OpCloseWindow, Key: expected.Key, Expected: upgradetx.Fence{Current: expected}})
}

// call classifies one exchange. Prepare and Query carry no execution effect:
// an unanswered Prepare is retried with the same client nonce and receives the
// same inert grant, so a lost reply is a deferral. Every other operation can
// have been accepted before its reply was lost; that is upgrade_unsettled with
// Accepted=true, which no caller may map to a retryable class (§7.1).
// origin: hot-update S8 plan §3.1 / §7.1 / §11.2 P19
func (r Remote) call(ctx context.Context, request upgradewire.Request) (upgradetx.View, error) {
	response, err := r.exchange(ctx, request)
	return response.View, err
}

// exchange is call's classification, shared with Describe. A describe, like a
// query, has no effect: its lost reply is asked again with a fresh nonce,
// never reported as an accepted unknown.
// origin: hot-update S8 plan §3.1 / §7.1 / §11.2 P19; S8 FIX-DESCRIBE
func (r Remote) exchange(ctx context.Context, request upgradewire.Request) (upgradewire.Response, error) {
	if r.Exchange == nil {
		return upgradewire.Response{}, &upgradetx.Error{Code: upgradetx.CodeNotWired, Detail: "upgrade transaction route is unavailable"}
	}
	if ctx == nil {
		return upgradewire.Response{}, rejected("caller context is required")
	}
	if err := ctx.Err(); err != nil {
		return upgradewire.Response{}, err
	}
	// Encode locally first: a malformed request is the caller's error and
	// provably never reached the transport.
	if _, err := upgradewire.EncodeRequest(request); err != nil {
		return upgradewire.Response{}, err
	}
	mutation := request.Op != upgradewire.OpPrepare && request.Op != upgradewire.OpQuery && request.Op != upgradewire.OpDescribe
	response, err := r.Exchange(ctx, request)
	if err != nil {
		var notSent *NotSentError
		if errors.As(err, &notSent) {
			return upgradewire.Response{}, &upgradetx.Error{Code: upgradetx.CodeDeferred, Detail: "upgrade transaction route is unavailable"}
		}
		if mutation {
			return upgradewire.Response{}, &upgradetx.Error{Code: upgradetx.CodeUnsettled, Accepted: true,
				Detail: "transaction reply was lost; query the original transaction instead of retrying"}
		}
		if request.Op == upgradewire.OpQuery {
			return upgradewire.Response{}, unsettled("transaction query reply was lost")
		}
		if request.Op == upgradewire.OpDescribe {
			return upgradewire.Response{}, &upgradetx.Error{Code: upgradetx.CodeDeferred, Detail: "target description reply was lost; describe again"}
		}
		return upgradewire.Response{}, &upgradetx.Error{Code: upgradetx.CodeDeferred, Detail: "prepare reply was lost; retry with the same client nonce"}
	}
	if response.Code != "" {
		// The target already classified the failure, including an accepted
		// result it projected to upgrade_unsettled. A deferral the target
		// returned precedes acceptance; nothing here upgrades it to accepted.
		failure := &upgradetx.Error{Code: response.Code, Detail: response.Error, Accepted: mutation && response.Code == upgradetx.CodeUnsettled}
		if _, known := upgradewire.ErrorExit(response.Code); !known && !legacyCode(response.Code) {
			failure = &upgradetx.Error{Code: upgradetx.CodeUnsettled, Detail: "target returned an unregistered transaction code", Accepted: mutation}
		}
		return response, failure
	}
	return response, nil
}

// DescribeTarget asks the target for the identity a Prepare must name. The
// reply must answer this selector and this fresh nonce with a well-formed
// description; anything else is refused before any candidate is downloaded,
// so a guessed or replayed identity can never reach a grant.
// origin: hot-update S8 plan §2.1 / §3.1; S8 FIX-DESCRIBE (P19 request R1)
func (r Remote) DescribeTarget(ctx context.Context, selector upgradetx.Key) (Target, *upgradeport.CandidateDescription, error) {
	nonce, err := randomHex()
	if err != nil {
		return Target{}, nil, err
	}
	response, err := r.exchange(ctx, upgradewire.Request{Op: upgradewire.OpDescribe, Key: selector, QueryNonce: nonce})
	if err != nil {
		return Target{}, nil, err
	}
	d := response.Description
	if d == nil || !reflect.DeepEqual(response.View, upgradetx.View{}) {
		return Target{}, nil, &upgradetx.Error{Code: upgradetx.CodeDeferred, Detail: upgradewire.DescriptionUnavailable}
	}
	if err := upgradewire.ValidateDescription(*d, selector, nonce); err != nil {
		return Target{}, nil, err
	}
	return Target{Key: d.Key, SourceSHA: d.RunningSHA, SourceRelease: d.SourceRelease, Scope: d.Scope}, d.Candidate, nil
}

// legacyCode admits the refusal namespace an older broker or the shared
// legacy policy already returns on this route. Anything else is unregistered.
func legacyCode(code string) bool {
	switch code {
	case "bad_request", "try_again", "not_owner", "node_not_found", "node_offline", "session_not_found_or_deleting", "actor_invalid":
		return true
	default:
		return false
	}
}
