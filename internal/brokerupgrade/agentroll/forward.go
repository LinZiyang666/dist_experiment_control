package agentroll

import (
	"database/sql"
	"encoding/json"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/brokerupgrade/rollcontrol"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/internal/upgradewire"
)

// admitAgentRoll keeps orchestration on the existing owner/capability/private
// inbox route. The broker stamps the actor itself and authenticates fresh
// agent evidence; a ctl can neither forge a sample nor substitute its own view.
// origin: S8 internal review SIGNED-ROLL-FIXTURES F3; S8 plan §3.1 / §7.2
func Admit(r Route) (Forward, error) {
	request, err := rollcontrol.Decode(r.Body)
	if err != nil {
		return Forward{}, err
	}
	if r.Actor == "" || !authPrivateReply(r) || request.Parent.ActorFP != r.ActorFP || request.Key.Component != upgradetx.ComponentAgent || request.Key.SID != r.SID || request.Key.ConfiguredNID != r.NID {
		return Forward{}, refusal(upgradetx.CodeRejected, "agent roll route or actor mismatch")
	}
	// Parent binding needs fleet administrator authority, not just session
	// ownership. Otherwise an actor could borrow another roll's holder for an
	// extra unplanned agent cut after issuing its own inert host binding.
	// origin: S8 internal review SIGNED-ROLL-FIXTURES F3; S8 plan §3.1 / §6.3
	if r.VerifyRoll == nil {
		return Forward{}, refusal(upgradetx.CodeRejected, "agent roll signature verifier unavailable")
	}
	if err = r.VerifyRoll(request); err != nil {
		return Forward{}, err
	}
	switch request.Op {
	case "roll.describe", "roll.bind", "roll.prepare_step":
	case "roll.sample", "roll.reserve", "roll.stage", "roll.release_host":
		if request.Plan.Parent.TxID != request.Parent.Handle.TxID {
			return Forward{}, refusal(upgradetx.CodeRejected, "agent roll parent changed")
		}
		var saved string
		if r.DB == nil || r.DB.QueryRow("SELECT value FROM cluster_meta WHERE key=?", "upgrade_roll_reserved."+request.Parent.Handle.TxID).Scan(&saved) != nil || saved != rollcontrol.Hash(request.Plan) {
			return Forward{}, refusal(upgradetx.CodeUnsettled, "agent roll has no original cluster reservation")
		}
		// Observation after final release is still legal. Execution/publication
		// can borrow authority only while the original holder remains live.
		if request.Op != "roll.sample" && request.Op != "roll.release_host" {
			if err = ParentHeld(r.DB, request.Parent.Handle.TxID); err != nil {
				return Forward{}, err
			}
		}
	default:
		return Forward{}, refusal(upgradetx.CodeRejected, "unsupported agent roll operation")
	}
	if !r.Capable {
		return Forward{}, refusal(upgradetx.CodeDeferred, "agent transaction capability unavailable")
	}
	raw, err := json.Marshal(proto.UpgradeTxForwardedReq{ActorFP: r.ActorFP, Request: r.Body})
	if err != nil {
		return Forward{}, err
	}
	return Forward{Subject: proto.SubjCmdForwarded(r.SID, r.NID, "upgrade-tx"), Envelope: raw, Read: request.Op == "roll.describe" || request.Op == "roll.sample" || request.Op == "roll.prepare_step", Roll: &request}, nil
}
func authPrivateReply(r Route) bool {
	return len(r.Reply) > len(auth.InboxPrefixFor(r.Actor))+1 && r.Reply[:len(auth.InboxPrefixFor(r.Actor))+1] == auth.InboxPrefixFor(r.Actor)+"."
}
func ParentHeld(db *sql.DB, parent string) error {
	if db == nil {
		return refusal(upgradetx.CodeDeferred, "cluster reservation unavailable")
	}
	state, err := cluster.ReadUpgradeLock(db)
	marker, e := cluster.UpgradeHolderMarker(parent)
	expiry, x := time.Parse(time.RFC3339Nano, state.Lease)
	if err != nil || e != nil || x != nil || state.Marker != marker || !time.Now().Before(expiry) {
		return refusal(upgradetx.CodeUnsettled, "original agent parent holder is not live")
	}
	return nil
}
func Response(r Route, request rollcontrol.Request, body []byte) ([]byte, string, error) {
	var out rollcontrol.Response
	if err := upgradewire.StrictDecode(body, &out); err != nil {
		return nil, "", err
	}
	if out.Code != "" {
		return body, out.Code, nil
	}
	if request.Op == "roll.sample" {
		s := out.Sample
		if s.ParentTxID != request.Parent.Handle.TxID || s.Nonce != request.Nonce || s.View.Historical.Grant != request.Step.Grant || s.View.Observation.Nonce != request.Nonce || r.SignSample == nil {
			return nil, "", refusal(upgradetx.CodeUnsettled, "agent sample binding unavailable")
		}
		s.Holder = ParentHeld(r.DB, s.ParentTxID) == nil
		var err error
		out.Sample, err = r.SignSample(s)
		if err != nil {
			return nil, "", err
		}
	}
	raw, err := json.Marshal(out)
	return raw, out.Code, err
}

type Route struct {
	VerifyRoll                      func(rollcontrol.Request) error
	Capable                         bool
	DB                              *sql.DB
	SID, NID, Actor, ActorFP, Reply string
	Body                            []byte
	SignSample                      func(rollcontrol.Sample) (rollcontrol.Sample, error)
}
type Forward struct {
	Subject  string
	Envelope []byte
	Read     bool
	Roll     *rollcontrol.Request
}

func refusal(code, detail string) error { return &upgradetx.Error{Code: code, Detail: detail} }
