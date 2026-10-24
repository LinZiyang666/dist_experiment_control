package brokerupgrade

import (
	"context"
	"encoding/json"
	"reflect"

	"github.com/LinZiyang666/tether/internal/brokerupgrade/rollcontrol"
	"github.com/LinZiyang666/tether/internal/brokerupgradeport"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/upgradeport"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

type rollEndpoint struct {
	upgradeport.Endpoint
	deps  *Dependencies
	node  *cluster.Node
	seed  []byte
	homes func() error
}

// WithRoll binds the authenticated route to THIS broker's current journal,
// cluster runtime and publication authority. It adds no competing endpoint.
func WithRoll(endpoint upgradeport.Endpoint, deps *Dependencies, node *cluster.Node, seed []byte, homes func() error) upgradeport.Endpoint {
	// A decorator must preserve missing wiring so the shared wire gate can
	// return a typed deferral before invoking any promoted endpoint method.
	// origin: S8 internal review upgrade-maketest F1
	if endpoint == nil || reflect.ValueOf(endpoint).Kind() == reflect.Ptr && reflect.ValueOf(endpoint).IsNil() {
		return nil
	}
	return rollEndpoint{Endpoint: endpoint, deps: deps, node: node, seed: seed, homes: homes}
}
func (e rollEndpoint) Describe(ctx context.Context, key upgradetx.Key, nonce string) (upgradeport.Description, error) {
	d, ok := e.Endpoint.(upgradeport.Describer)
	if !ok {
		return upgradeport.Description{}, refusal(upgradetx.CodeNotWired, "broker cannot describe its runtime")
	}
	return d.Describe(ctx, key, nonce)
}
func (e rollEndpoint) Roll(ctx context.Context, r rollcontrol.Request, actor string) rollcontrol.Response {
	c := rollcontrol.Config{}
	if e.deps != nil {
		c = rollcontrol.Config{Key: e.deps.Key, Owner: brokerupgradeport.RuntimeOwner(e.deps.Runtime), Store: e.deps.Store, Runtime: e.deps.Runtime, Endpoint: e.Endpoint, Stage: e.deps.Stage, Homes: e.homes, ReleaseLease: func() error { return ReleaseParentLease(e.deps) }}
	}
	return rollcontrol.Handle(ctx, c, e.node, e.seed, r, actor)
}
func signedRoll(ctx context.Context, endpoint upgradeport.Endpoint, req *proto.ClusterUpgradeReq, target, actor string) *proto.ClusterUpgradeResp {
	response := rollcontrol.Response{}
	if proto.ValidateUpgradeReq(req) != nil || req.Op != SignedTransactionOp || req.TargetNode != target || req.ActorFP != actor || actor == "" {
		response.Code, response.Detail = proto.CodeNotOwner, "signed roll route mismatch"
	} else if port, ok := endpoint.(interface {
		Roll(context.Context, rollcontrol.Request, string) rollcontrol.Response
	}); ok {
		request, err := rollcontrol.Decode(req.Upgrade)
		if err != nil {
			response.Code, response.Detail = upgradetx.CodeRejected, "invalid roll envelope"
		} else {
			response = port.Roll(ctx, request, actor)
		}
	} else {
		response.Code, response.Detail = upgradetx.CodeNotWired, "signed roll journal route is unavailable"
	}
	raw, _ := json.Marshal(response)
	return &proto.ClusterUpgradeResp{OK: response.Code == "", Code: response.Code, Error: response.Detail, Upgrade: raw}
}

// CheckHomes preserves the actual leader-owned home ACK verdict.
func CheckHomes(pending []string, err error) error {
	if err != nil {
		return err
	}
	if len(pending) > 0 {
		return refusal(upgradetx.CodeDeferred, "homes have not reconverged")
	}
	return nil
}
