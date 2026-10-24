package brokerupgrade

import (
	"context"

	"github.com/LinZiyang666/tether/internal/brokerupgrade/rollcontrol"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/upgradeport"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/internal/upgradewire"
)

// SignedTransactionOp is the additive account-signed trigger action. Its
// envelope and actor are covered by the upgrade-tx-v1 signing suffix.
const SignedTransactionOp = "upgrade-tx"

// HandleSignedTransaction runs only after the existing account-signature,
// target-node and replay checks. The subject's authenticated actor must equal
// the signed actor; Handle then binds the transaction actor to the original
// grant and the request key to this broker's registered key. A valid account
// signature is operator authority, not a substitute for the target's grant.
// holder is the live roll-lock check, applied to every mutation (Query is read
// only): a remote cut cannot bypass a live roll with an old signature.
// origin: hot-update S8 plan §2.2 / §6.3 / §7.1 / §11.2 P15
func HandleSignedTransaction(ctx context.Context, endpoint upgradeport.Endpoint, key upgradetx.Key, req *proto.ClusterUpgradeReq, target, actorFP string, holder func() error) *proto.ClusterUpgradeResp {
	if req != nil && rollcontrol.IsRequest(req.Upgrade) {
		return signedRoll(ctx, endpoint, req, target, actorFP)
	}
	response := signedResponse(ctx, endpoint, key, req, target, actorFP, holder)
	raw, err := upgradewire.EncodeResponse(response)
	if err != nil {
		response = upgradewire.ErrorResponse(err)
		raw = nil
	}
	return &proto.ClusterUpgradeResp{OK: response.Code == "", Code: response.Code, Error: response.Error, Upgrade: raw}
}

func signedResponse(ctx context.Context, endpoint upgradeport.Endpoint, key upgradetx.Key, req *proto.ClusterUpgradeReq, target, actorFP string, holder func() error) upgradewire.Response {
	if actorFP == "" {
		return upgradewire.ErrorResponse(&upgradetx.Error{Code: proto.CodeNotOwner, Detail: "signed upgrade route has no authenticated actor"})
	}
	request, err := upgradewire.DecodeClusterRequest(req, target, actorFP)
	if err != nil {
		return upgradewire.ErrorResponse(err)
	}
	if request.Op != upgradewire.OpQuery && (holder == nil || holder() != nil) {
		return upgradewire.ErrorResponse(refusal(upgradetx.CodeDeferred, "upgrade holder is not current"))
	}
	body, err := upgradewire.EncodeRequest(request)
	if err != nil {
		return upgradewire.ErrorResponse(err)
	}
	return upgradewire.Handle(ctx, endpoint, body, upgradewire.Access{Key: key, ActorFP: actorFP})
}
