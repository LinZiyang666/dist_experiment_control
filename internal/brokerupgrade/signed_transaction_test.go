package brokerupgrade

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/upgradeartifact"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/internal/upgradewire"
)

// recordingEndpoint stands in for the bound broker engine: it proves which
// operation the trigger dispatched and with which key, never a successful cut.
type recordingEndpoint struct {
	calls []string
	key   upgradetx.Key
	actor string
}

func (e *recordingEndpoint) Prepare(_ context.Context, key upgradetx.Key, req upgradetx.PrepareRequest) (upgradetx.Grant, error) {
	e.calls = append(e.calls, "prepare")
	e.key = key
	return upgradetx.Grant{Handle: upgradetx.Handle{Key: key, TxID: strings.Repeat("1", 32)}, ActorFP: req.ActorFP}, nil
}
func (e *recordingEndpoint) Start(context.Context, upgradetx.Grant, upgradeartifact.Receipt) (upgradetx.View, error) {
	e.calls = append(e.calls, "start")
	return upgradetx.View{}, errors.New("not a cut")
}
func (e *recordingEndpoint) Query(_ context.Context, key upgradetx.Key, txID, nonce string) (upgradetx.View, error) {
	e.calls = append(e.calls, "query")
	return upgradetx.View{Historical: upgradetx.Record{Key: key, Grant: upgradetx.Grant{Handle: upgradetx.Handle{Key: key, TxID: txID}, ActorFP: e.actor}},
		Observation: upgradetx.Observation{Nonce: nonce}}, nil
}
func (e *recordingEndpoint) Rollback(context.Context, upgradetx.Fence, upgradetx.ReverseRequest) (upgradetx.View, error) {
	e.calls = append(e.calls, "rollback")
	return upgradetx.View{}, errors.New("not a cut")
}
func (e *recordingEndpoint) Cancel(context.Context, upgradetx.Handle) (upgradetx.View, error) {
	e.calls = append(e.calls, "cancel")
	return upgradetx.View{}, nil
}
func (e *recordingEndpoint) Archive(context.Context, upgradetx.Handle) (upgradetx.View, error) {
	e.calls = append(e.calls, "archive")
	return upgradetx.View{}, nil
}
func (e *recordingEndpoint) CloseWindow(context.Context, upgradetx.Handle) (upgradetx.View, error) {
	e.calls = append(e.calls, "close_window")
	return upgradetx.View{}, nil
}

func signedTransaction(t *testing.T, actor string, request upgradewire.Request) *proto.ClusterUpgradeReq {
	t.Helper()
	body, err := upgradewire.EncodeRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	return &proto.ClusterUpgradeReq{Op: SignedTransactionOp, TargetNode: "brk-a", ActorFP: actor, Upgrade: body, IssuedAt: time.Now().UTC().Format(time.RFC3339)}
}

// A valid account signature is operator authority, not a transaction grant:
// the signed actor must equal the subject's authenticated actor, the target
// key must be this broker's, and every mutation re-checks the live roll holder.
// origin: hot-update S8 plan §2.2 / §6.3 / §7.1 / §11.2 P15
func TestSignedTransactionBindsSubjectActorKeyAndHolder(t *testing.T) {
	key := upgradetx.Key{Component: upgradetx.ComponentBroker, DomainID: strings.Repeat("d", 32), ConfigPath: "/etc/tether/broker.yaml"}
	prepare := upgradewire.Request{Op: upgradewire.OpPrepare, Key: key, Prepare: upgradetx.PrepareRequest{ClientNonce: "nonce", ActorFP: "actor-a",
		SourceRelease: "v1", TargetRelease: "v2", Mode: upgradetx.ModeHot, SourceSHA: strings.Repeat("a", 64), TargetSHA: strings.Repeat("b", 64),
		CapabilityDigest: strings.Repeat("c", 64), ExpiresAt: time.Now().Add(time.Minute).UnixNano(),
		Scope: upgradetx.Scope{ID: "scope", Participants: []upgradetx.Key{key}, Domains: []upgradetx.Domain{upgradetx.DomainBrokerRuntime}}}}
	handle := upgradetx.Handle{Key: key, TxID: strings.Repeat("1", 32), CutID: strings.Repeat("2", 32), Generation: 2, Revision: 3}
	cancel := upgradewire.Request{Op: upgradewire.OpCancel, Key: key, Expected: upgradetx.Fence{Current: handle}}
	query := upgradewire.Request{Op: upgradewire.OpQuery, Key: key, TxID: handle.TxID, QueryNonce: strings.Repeat("3", 32)}
	holderOK, holderLost := func() error { return nil }, func() error { return errors.New("holder moved") }
	foreign := key
	foreign.ConfigPath = "/etc/tether/other.yaml"
	foreignPrepare := prepare
	foreignPrepare.Key = foreign
	foreignPrepare.Prepare.Scope.Participants = []upgradetx.Key{foreign}
	for name, arm := range map[string]struct {
		request  upgradewire.Request
		signed   string
		subject  string
		holder   func() error
		unwired  bool
		code     string
		dispatch []string
	}{
		"prepare_by_subject_actor":   {request: prepare, signed: "actor-a", subject: "actor-a", holder: holderOK, dispatch: []string{"prepare"}},
		"signed_actor_not_subject":   {request: prepare, signed: "actor-a", subject: "actor-b", holder: holderOK, code: proto.CodeNotOwner},
		"unauthenticated_route":      {request: prepare, signed: "actor-a", holder: holderOK, code: proto.CodeNotOwner},
		"another_brokers_key":        {request: foreignPrepare, signed: "actor-a", subject: "actor-a", holder: holderOK, code: proto.CodeNotOwner},
		"mutation_after_holder_lost": {request: cancel, signed: "actor-a", subject: "actor-a", holder: holderLost, code: upgradetx.CodeDeferred},
		"mutation_without_holder_fn": {request: cancel, signed: "actor-a", subject: "actor-a", code: upgradetx.CodeDeferred},
		"query_reads_during_a_roll":  {request: query, signed: "actor-a", subject: "actor-a", holder: holderLost, dispatch: []string{"query"}},
		"unwired_endpoint_defers":    {request: prepare, signed: "actor-a", subject: "actor-a", holder: holderOK, unwired: true, code: upgradetx.CodeDeferred},
	} {
		t.Run(name, func(t *testing.T) {
			endpoint := &recordingEndpoint{actor: "actor-a"}
			req := signedTransaction(t, arm.signed, arm.request)
			var resp *proto.ClusterUpgradeResp
			if arm.unwired {
				resp = HandleSignedTransaction(t.Context(), nil, key, req, "brk-a", arm.subject, arm.holder)
			} else {
				resp = HandleSignedTransaction(t.Context(), endpoint, key, req, "brk-a", arm.subject, arm.holder)
			}
			if resp.Code != arm.code || resp.OK != (arm.code == "") || strings.Join(endpoint.calls, ",") != strings.Join(arm.dispatch, ",") {
				t.Fatalf("%s: resp=%+v calls=%v", name, resp, endpoint.calls)
			}
			response, err := upgradewire.DecodeResponse(resp.Upgrade)
			if err != nil || response.Code != resp.Code {
				t.Fatalf("%s: raw transaction reply %q does not carry the same outcome: %v", name, resp.Upgrade, err)
			}
			if arm.code == "" && arm.request.Op == upgradewire.OpPrepare && (endpoint.key != key || response.View.Historical.Grant.ActorFP != "actor-a") {
				t.Fatalf("%s: dispatched with key %+v / grant %+v", name, endpoint.key, response.View.Historical.Grant)
			}
		})
	}
	// The trigger target is part of the signed route: a request naming another
	// broker can never be served here, even with a valid envelope.
	endpoint := &recordingEndpoint{}
	resp := HandleSignedTransaction(t.Context(), endpoint, key, signedTransaction(t, "actor-a", prepare), "brk-b", "actor-a", holderOK)
	if resp.Code != proto.CodeNotOwner || len(endpoint.calls) != 0 {
		t.Fatalf("wrong target served: %+v %v", resp, endpoint.calls)
	}
}
