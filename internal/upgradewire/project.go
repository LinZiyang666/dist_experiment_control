package upgradewire

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"reflect"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/upgradeport"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// Access is supplied by the authenticated transport, never decoded from the
// request. The local admin socket supplies Administrator only after its Unix
// permissions have admitted the connection. Remote handlers bind ActorFP to
// the authenticated subject and Key to the actual target's configuration.
// origin: hot-update S8 plan §2.2 / §7.1 / P13
type Access struct {
	Key           upgradetx.Key
	ActorFP       string
	Administrator bool
}

// Handle is shared by local admin and forwarded agent adapters. Every request
// crosses the same strict decoder and exact target guard. It cannot dispatch
// the old upgrade verb, ReExecOnly, or implicit interrupting execution.
func Handle(ctx context.Context, endpoint upgradeport.Endpoint, body []byte, access Access) Response {
	r, err := DecodeRequest(body, access.Key.Component)
	if err != nil {
		return ErrorResponse(err)
	}
	// A describe names this component by the selector a ctl can know; every
	// other operation names the exact Key the target itself issued.
	// origin: S8 FIX-DESCRIBE (P19 request R1)
	want := access.Key
	if r.Op == OpDescribe {
		want = DescribeSelector(access.Key)
	}
	if r.Key != want || (!access.Administrator && !validText(access.ActorFP, 256)) {
		return denied()
	}
	// An interface holding a nil endpoint is missing wiring, not an empty
	// successful transaction result. It must refuse before calling a method.
	// origin: hot-update S8 plan §2.2 / P13
	if endpoint == nil || reflect.ValueOf(endpoint).Kind() == reflect.Ptr && reflect.ValueOf(endpoint).IsNil() {
		return ErrorResponse(&upgradetx.Error{Code: upgradetx.CodeDeferred, Detail: "upgrade endpoint is unavailable"})
	}
	if ctx == nil {
		return ErrorResponse(wireError("caller context is required"))
	}
	if err := ctx.Err(); err != nil {
		return ErrorResponse(err)
	}
	if r.Op == OpDescribe {
		return describe(ctx, endpoint, r, access.Key)
	}
	actor := r.Prepare.ActorFP
	if r.Op == OpStart {
		actor = r.Grant.ActorFP
	}
	if r.Op == OpRollback {
		actor = r.Reverse.ActorFP
	}
	if (r.Op == OpPrepare || r.Op == OpStart || r.Op == OpRollback) && !access.Administrator && actor != access.ActorFP {
		return denied()
	}
	// Query the original actor before exposing a private view or mutating an
	// accepted transaction. A caller-supplied reverse actor is not ownership of
	// the historical execution. Admin still uses the same target and CAS guards.
	if r.Op != OpPrepare && r.Op != OpStart && !access.Administrator {
		id, nonce := r.TxID, r.QueryNonce
		if r.Op != OpQuery {
			id = r.Expected.Current.TxID
			if r.Op == OpRollback {
				id = r.Reverse.HistoricalTxID
			}
			var random [16]byte
			if _, err := rand.Read(random[:]); err != nil {
				return ErrorResponse(err)
			}
			nonce = hex.EncodeToString(random[:])
		}
		view, err := endpoint.Query(ctx, r.Key, id, nonce)
		if err != nil {
			return ErrorResponse(err)
		}
		if view.Historical.Key != r.Key || view.Historical.Grant.ActorFP != access.ActorFP {
			return denied()
		}
		if proof := queryResponse(view, nonce, r.Key, id); proof.Code != "" {
			return proof
		}
		if r.Op == OpQuery {
			return Response{View: view}
		}
	}
	var view upgradetx.View
	switch r.Op {
	case OpPrepare:
		var grant upgradetx.Grant
		grant, err = endpoint.Prepare(ctx, r.Key, r.Prepare)
		// Prepare issues authorization, not a completed execution. The frozen
		// response has one View slot; its historical grant carries this result.
		view.Historical.Key, view.Historical.Grant = r.Key, grant
	case OpStart:
		view, err = endpoint.Start(ctx, r.Grant, r.Artifact)
	case OpQuery:
		view, err = endpoint.Query(ctx, r.Key, r.TxID, r.QueryNonce)
	case OpRollback:
		view, err = endpoint.Rollback(ctx, r.Expected, r.Reverse)
	case OpCancel:
		view, err = endpoint.Cancel(ctx, r.Expected.Current)
	case OpArchive:
		view, err = endpoint.Archive(ctx, r.Expected.Current)
	case OpCloseWindow:
		view, err = endpoint.CloseWindow(ctx, r.Expected.Current)
	}
	if err != nil {
		response := ErrorResponse(err)
		response.View = view
		return response
	}
	if r.Op == OpQuery {
		return queryResponse(view, r.QueryNonce, r.Key, r.TxID)
	}
	return Response{View: view}
}

func denied() Response {
	return Response{Code: proto.CodeNotOwner, Error: "upgrade actor or target is not authorized"}
}

// describe carries no actor field. Like Prepare it is authorized by its route:
// the broker admits only the session owner and stamps the authenticated actor
// (Handle refuses a forwarded request without one), and the admin socket's
// Unix permissions admit an administrator. The answer must describe exactly
// this configured component for this request's nonce; an endpoint that
// cannot describe itself refuses before the ctl downloads anything.
// origin: S8 FIX-DESCRIBE (P19 request R1)
func describe(ctx context.Context, endpoint upgradeport.Endpoint, r Request, key upgradetx.Key) Response {
	describer, ok := endpoint.(upgradeport.Describer)
	if !ok {
		return ErrorResponse(&upgradetx.Error{Code: upgradetx.CodeNotWired, Detail: DescriptionUnavailable})
	}
	d, err := describer.Describe(ctx, r.Key, r.QueryNonce)
	if err != nil {
		return ErrorResponse(err)
	}
	if err := ValidateDescription(d, r.Key, r.QueryNonce); err != nil || d.Key != key {
		return ErrorResponse(&upgradetx.Error{Code: upgradetx.CodeDeferred, Detail: DescriptionUnavailable})
	}
	return Response{Description: &d}
}

// DescriptionUnavailable is the public reason of every describe refusal that
// is not a typed runtime deferral: no exception names a path or identity.
const DescriptionUnavailable = "target_description_unavailable"

// ValidateDescription is shared by the target (before replying) and the ctl
// (before building a Prepare from it). The description must answer this
// selector and nonce, name a registered Key, a running SHA and release, and an
// original scope with exactly this one participant and every frozen domain: a
// description that omitted a domain could hide retained work from the grant.
// The domain union is the sealed-scope rule itself (upgradetx.CanonicalScope
// requires exactly the eleven frozen domains), checked here before a grant.
// A candidate is a broker statement only; an agent's candidate is the ctl's.
// origin: hot-update S8 plan §2.1 / §4.2 / M06; S8 FIX-DESCRIBE (P19 request R1)
func ValidateDescription(d upgradeport.Description, selector upgradetx.Key, nonce string) error {
	invalid := !validSelector(selector) || DescribeSelector(d.Key) != selector || !validKey(d.Key) || upgradetx.ValidateKey(d.Key) != nil ||
		!hexValue(nonce, 32) || d.Nonce != nonce || d.At <= 0 || !hexValue(d.RunningSHA, 64) || !validText(d.SourceRelease, 128) ||
		!validScope(d.Scope, d.Key) || len(d.Scope.Participants) != 1
	if !invalid {
		_, err := upgradetx.CanonicalScope(d.Scope)
		invalid = err != nil
	}
	if c := d.Candidate; c != nil {
		invalid = invalid || d.Key.Component != upgradetx.ComponentBroker || !hexValue(c.SHA, 64) || !validText(c.Release, 128) || !hexValue(c.CapabilityDigest, 64)
	}
	if invalid {
		return &upgradetx.Error{Code: upgradetx.CodeDeferred, Detail: DescriptionUnavailable}
	}
	return nil
}

// A fresh nonce cannot authenticate the wrong historical transaction. Check
// both bindings before the caller sees any private view; unknown current owner
// remains representable inside a successfully read view and is not completion.
// origin: hot-update S8 plan §4.2 / §7.1 / P13
func queryResponse(view upgradetx.View, nonce string, key upgradetx.Key, id string) Response {
	if view.Observation.Nonce != nonce || view.Historical.Key != key || view.Historical.Grant.Handle.Key != key || view.Historical.Grant.Handle.TxID != id {
		return ErrorResponse(&upgradetx.Error{Code: upgradetx.CodeUnsettled, Detail: "query response identity or nonce mismatch", Accepted: true})
	}
	return Response{View: view}
}

// DecodeClusterRequest is called only AFTER the existing account signature,
// target-node and replay checks. The signed actor must also equal the subject's
// authenticated actor; a valid signature does not authorize a different route.
func DecodeClusterRequest(r *proto.ClusterUpgradeReq, targetNode, actor string) (Request, error) {
	if err := proto.ValidateUpgradeReq(r); err != nil {
		return Request{}, wireError("invalid signed upgrade request")
	}
	if r.Op != "upgrade-tx" || r.TargetNode != targetNode || r.ActorFP != actor || actor == "" {
		return Request{}, &upgradetx.Error{Code: proto.CodeNotOwner, Detail: "signed upgrade route mismatch"}
	}
	request, err := DecodeRequest(r.Upgrade, upgradetx.ComponentBroker)
	if err != nil {
		return Request{}, err
	}
	if request.Op == OpPrepare && request.Prepare.ActorFP != actor || request.Op == OpStart && request.Grant.ActorFP != actor || request.Op == OpRollback && request.Reverse.ActorFP != actor {
		return Request{}, &upgradetx.Error{Code: proto.CodeNotOwner, Detail: "signed upgrade actor mismatch"}
	}
	return request, nil
}

// Summary is an explicit allowlist projection. Public health/register never
// carries resource IDs, actor, SID, artifact paths or free-form private errors.
// RunningSHA comes only from current observation, never an effective disk path.
func Summary(view upgradetx.View) *proto.UpgradeSummary {
	a := view.Historical.Forward
	if view.Observation.Current.Handle.TxID != "" {
		a = view.Observation.Current
	}
	if a.Handle.TxID == "" {
		return nil
	}
	reason := ""
	if _, known := ErrorExit(view.Code); known {
		reason = view.Code
	}
	return &proto.UpgradeSummary{TxID: a.Handle.TxID, CutID: a.Handle.CutID,
		Generation: a.Handle.Generation, Revision: a.Handle.Revision,
		Phase: string(a.Phase), Outcome: string(a.Outcome), RunningSHA: view.Observation.RunningSHA, Reason: reason}
}

// ErrorResponse keeps unknown errors opaque: filesystem and SQL errors can
// contain paths or credential material. Accepted failures must never invite a
// new Start by being mislabeled as a retryable admission deferral.
func ErrorResponse(err error) Response {
	var failure *upgradetx.Error
	if errors.As(err, &failure) {
		code := failure.Code
		if _, known := ErrorExit(code); !known {
			code = upgradetx.CodeUnsettled
		}
		if failure.Accepted && code != proto.CodeNotOwner && code != upgradetx.CodeSessionIncarnationMismatch {
			code = upgradetx.CodeUnsettled
		}
		return Response{Code: code, Error: failure.Detail}
	}
	return Response{Code: upgradetx.CodeUnsettled, Error: "upgrade result is unknown; query the original transaction"}
}

// ErrorExit is the S8 classification seam for CLI adapters; the bool lets the
// existing classifier retain all older namespaces without a new default 70.
func ErrorExit(code string) (int, bool) {
	switch code {
	case upgradetx.CodeUnsupportedComponent, upgradetx.CodeGrantUnknown, upgradetx.CodeGrantExpired, upgradetx.CodeRejected, upgradetx.CodeWindowClosed:
		return 64, true
	case upgradetx.CodeDeferred, upgradetx.CodeNotWired:
		return 75, true
	case upgradetx.CodeUnsettled, upgradetx.CodeStateCorrupt:
		return 74, true
	case upgradetx.CodeSessionIncarnationMismatch, proto.CodeNotOwner:
		return 77, true
	default:
		return 0, false
	}
}

// LegacyError preserves codes the real N-1 ctl already classifies. It cannot
// turn an accepted unknown into try_again or claim a legacy interrupt was hot.
func LegacyError(err error) proto.UpgradeResp {
	r := ErrorResponse(err)
	switch r.Code {
	case upgradetx.CodeRejected, upgradetx.CodeUnsupportedComponent, upgradetx.CodeGrantUnknown, upgradetx.CodeGrantExpired, upgradetx.CodeWindowClosed:
		r.Code = proto.CodeBadRequest
	case upgradetx.CodeDeferred, upgradetx.CodeNotWired:
		r.Code = "try_again"
	case upgradetx.CodeSessionIncarnationMismatch:
		r.Code = proto.CodeNotOwner
	}
	return proto.UpgradeResp{Code: r.Code, Error: r.Error}
}

// Value-struct omission is explicit on responses too. A failure must not
// accidentally serialize a zero View that a consumer could read as evidence.
// origin: hot-update S8 plan §2.2 / P13
func (r Response) MarshalJSON() ([]byte, error) { return EncodeResponse(r) }

func EncodeResponse(response Response) ([]byte, error) {
	fields := map[string]any{}
	if !reflect.ValueOf(response.View).IsZero() {
		fields["view"] = response.View
	}
	if response.Code != "" {
		fields["code"] = response.Code
	}
	if response.Error != "" {
		fields["error"] = response.Error
	}
	if response.Description != nil {
		fields["description"] = response.Description
	}
	body, err := json.Marshal(fields)
	if err == nil {
		err = checkJSON(body)
	}
	return body, err
}
func DecodeResponse(body []byte) (Response, error) {
	var response Response
	if len(bytes.TrimSpace(body)) == 0 || bytes.TrimSpace(body)[0] != '{' {
		return response, wireError("response must be an object")
	}
	err := StrictDecode(body, &response)
	return response, err
}

// WriteLine retains the admin handler's full-write receipt. Short writes must
// reach the image reply barrier; otherwise exec could erase an armed response.
func WriteLine(writer io.Writer, value any) error {
	body, err := json.Marshal(value)
	if err != nil {
		return err
	}
	body = append(body, '\n')
	n, err := writer.Write(body)
	if err != nil || n != len(body) {
		return errors.Join(err, errors.New("adminsock: incomplete reply write"))
	}
	return nil
}

// InconsistencyDetail retains the existing admin status projection in the wire
// leaf so new transaction fields do not grow the legacy protocol file.
func InconsistencyDetail(reason string) string {
	switch reason {
	case "draining_without_marker":
		return "phase DRAINING but its broker_draining marker is gone and no operation is driving it — " +
			"a drain died between its own steps; run `tether cluster drain --abort <node>` to return it " +
			"to VOTER, or `tether cluster retire <node>` to finish removing it"
	default:
		return "roster phase and the committed raft configuration disagree — run `tether cluster status` " +
			"and, if a live node is missing from raft, `tether cluster recovery node remove <id> --manual`"
	}
}

// EncodeForwarded is used after the broker verifies the original command's
// authenticated subject. The envelope binds that actor to the unchanged strict
// request; actor authorization is checked again by the target before dispatch.
// origin: hot-update S8 plan §2.2 / §7.1 / P13
func EncodeForwarded(request Request, actor string) ([]byte, error) {
	if !validText(actor, 256) {
		return nil, wireError("forwarded actor is required")
	}
	body, err := EncodeRequest(request)
	if err != nil {
		return nil, err
	}
	return json.Marshal(proto.UpgradeTxForwardedReq{ActorFP: actor, Request: body})
}

// DecodeForwarded must be reached only from the broker-only forwarded subject.
// It cannot infer an actor from a grant: query/cancel/archive have no actor field
// in the frozen request and still require the original owner's authorization.
// origin: hot-update S8 plan §2.2 / P13
func DecodeForwarded(body []byte, key upgradetx.Key) (Request, Access, error) {
	var envelope proto.UpgradeTxForwardedReq
	if err := StrictDecode(body, &envelope); err != nil {
		return Request{}, Access{}, err
	}
	if !validText(envelope.ActorFP, 256) {
		return Request{}, Access{}, wireError("forwarded actor is required")
	}
	request, err := DecodeRequest(envelope.Request, key.Component)
	if err != nil {
		return Request{}, Access{}, err
	}
	want := key
	if request.Op == OpDescribe {
		want = DescribeSelector(key)
	}
	if request.Key != want {
		return Request{}, Access{}, &upgradetx.Error{Code: proto.CodeNotOwner, Detail: "forwarded upgrade target mismatch"}
	}
	return request, Access{Key: key, ActorFP: envelope.ActorFP}, nil
}

func HandleForwarded(ctx context.Context, endpoint upgradeport.Endpoint, key upgradetx.Key, body []byte) Response {
	request, access, err := DecodeForwarded(body, key)
	if err != nil {
		return ErrorResponse(err)
	}
	requestBody, err := EncodeRequest(request)
	if err != nil {
		return ErrorResponse(err)
	}
	return Handle(ctx, endpoint, requestBody, access)
}
