// Package rollcontrol carries the separate, strict orchestration envelope on
// the existing account-signed targeted transaction route. Component envelopes
// and their frozen operations remain owned by upgradewire.
// origin: hot-update S8 plan §2.2 / §3.1 / §7.1; S8 internal review cluster-roll F4
package rollcontrol

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/upgradeartifact"
	"github.com/LinZiyang666/tether/internal/upgraderoll"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/internal/upgradewire"
)

type Request struct {
	AccountSignature string                   `json:"account_signature,omitempty"`
	Op               string                   `json:"op"`
	Route            string                   `json:"route,omitempty"`
	Parent           upgradetx.Grant          `json:"parent"`
	Scope            upgradetx.Scope          `json:"scope"`
	Plan             upgradetx.Plan           `json:"plan"`
	Key              upgradetx.Key            `json:"key"`
	Prepare          upgradetx.PrepareRequest `json:"prepare"`
	Expected         upgradetx.Handle         `json:"expected"`
	Event            upgradetx.Event          `json:"event"`
	Step             upgradetx.Step           `json:"step"`
	Nonce            string                   `json:"nonce"`
	Samples          []Sample                 `json:"samples"`
}
type Sample struct {
	ParentTxID string         `json:"parent_tx_id"`
	Nonce      string         `json:"nonce"`
	At         int64          `json:"at"`
	View       upgradetx.View `json:"view"`
	Release    string         `json:"release"`
	Holder     bool           `json:"holder"`
	Released   bool           `json:"released"`
	Signature  string         `json:"signature"`
}
type Response struct {
	Description *upgradetx.Description  `json:"description,omitempty"`
	Grant       upgradetx.Grant         `json:"grant"`
	Plan        upgradetx.Plan          `json:"plan"`
	View        upgradetx.View          `json:"view"`
	Artifact    upgradeartifact.Receipt `json:"artifact"`
	Sample      Sample                  `json:"sample"`
	Fleet       Fleet                   `json:"fleet"`
	Observation upgradetx.Observation   `json:"observation"`
	Receipt     upgradetx.Receipt       `json:"receipt"`
	Code        string                  `json:"code"`
	Detail      string                  `json:"detail"`
	Accepted    bool                    `json:"accepted"`
}
type Exchange func(context.Context, string, Request) (Response, error)

func IsRequest(raw []byte) bool {
	var op struct {
		Op string `json:"op"`
	}
	return json.Unmarshal(raw, &op) == nil && strings.HasPrefix(op.Op, "roll.")
}
func Decode(raw []byte) (Request, error) {
	var r Request
	err := upgradewire.StrictDecode(raw, &r)
	if err == nil && !operation(r.Op) {
		err = Error(upgradetx.CodeRejected, "unknown roll operation", false)
	}
	return r, err
}
func Hash(v any) string {
	b, _ := json.Marshal(v)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
func Error(code, detail string, accepted bool) error {
	return &upgradetx.Error{Code: code, Detail: detail, Accepted: accepted}
}
func Unknown(detail string) error { return Error(upgradetx.CodeUnsettled, detail, true) }

// Action commits to an original step, including its dependency list. This is
// shared with the driver so remote Apply cannot authorize a different action.
func Action(plan upgradetx.Plan, step upgradetx.Step) upgradetx.Action {
	kind := upgradetx.ActionDispatch
	if step.Kind == upgradetx.StepHomeConverge {
		kind = upgradetx.ActionControl
	}
	if step.Kind == upgradetx.StepRelease {
		kind = upgradetx.ActionRelease
	}
	return upgradetx.Action{ID: Hash(step)[:32], Handle: plan.Parent, Kind: kind, IntentDigest: upgraderoll.PlanDigest(plan), State: upgradetx.ActionIntent, Nonce: step.Grant.ClientNonce}
}

// Fleet is an explicit wire projection; the in-process collector interfaces
// intentionally carry no JSON contract and must not leak Go field spellings.
type Fleet struct {
	Parent       upgradetx.Handle `json:"parent"`
	Nonce        string           `json:"nonce"`
	Known        bool             `json:"known"`
	Complete     bool             `json:"complete"`
	Participants []upgradetx.Key  `json:"participants"`
	Members      []Member         `json:"members"`
	Census       upgradetx.Census `json:"census"`
	HolderHeld   bool             `json:"holder_held"`
}
type Member struct {
	Participant upgradetx.Key   `json:"participant"`
	Owner       upgradetx.Owner `json:"owner"`
	Release     string          `json:"release"`
	Generation  uint64          `json:"generation"`
	Known       bool            `json:"known"`
}

func WireFleet(p upgraderoll.FleetProof) Fleet {
	out := Fleet{Parent: p.Parent, Nonce: p.Nonce, Known: p.Known, Complete: p.Complete, Participants: p.Participants, Census: p.Census, HolderHeld: p.HolderHeld}
	for _, m := range p.Members {
		out.Members = append(out.Members, Member{Participant: m.Participant, Owner: m.Owner, Release: m.Release, Generation: m.Generation, Known: m.Known})
	}
	return out
}
func (p Fleet) Proof() upgraderoll.FleetProof {
	out := upgraderoll.FleetProof{Parent: p.Parent, Nonce: p.Nonce, Known: p.Known, Complete: p.Complete, Participants: p.Participants, Census: p.Census, HolderHeld: p.HolderHeld}
	for _, m := range p.Members {
		out.Members = append(out.Members, upgraderoll.FleetMember{Participant: m.Participant, Owner: m.Owner, Release: m.Release, Generation: m.Generation, Known: m.Known})
	}
	return out
}

func operation(op string) bool {
	switch op {
	case "roll.describe", "roll.check_idle", "roll.prepare_parent", "roll.accept_parent", "roll.bind", "roll.prepare_step", "roll.load", "roll.seal", "roll.plan", "roll.apply", "roll.sample", "roll.reserve_cluster", "roll.release_cluster", "roll.check_cluster", "roll.reserve", "roll.release_host", "roll.stage", "roll.preflight", "roll.home", "roll.fleet", "roll.observe", "roll.complete":
		return true
	default:
		return false
	}
}

// SignAgentRequest binds the whole-host administrator's authority to the exact
// parent operation before it enters the ordinary owner-admitted agent route.
// The agent route authenticates the session actor as well; owning a session
// alone cannot invent a parent reservation during somebody else's fleet roll.
// origin: S8 internal review SIGNED-ROLL-FIXTURES F3; S8 plan §3.1 / §7.2
func SignAgentRequest(seed []byte, r Request) ([]byte, error) {
	r.AccountSignature = ""
	body, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	signature, err := auth.SignWithSeed(seed, body)
	if err != nil {
		return nil, err
	}
	r.AccountSignature = hex.EncodeToString(signature)
	return json.Marshal(r)
}
func VerifyAgentRequest(seed []byte, r Request) error {
	signature, err := hex.DecodeString(r.AccountSignature)
	if err != nil {
		return Error(upgradetx.CodeRejected, "agent roll signature is invalid", false)
	}
	r.AccountSignature = ""
	body, err := json.Marshal(r)
	if err != nil {
		return err
	}
	pub, err := auth.PublicKeyFromSeed(seed)
	if err != nil {
		return err
	}
	if auth.VerifySignature(pub, body, signature) != nil {
		return Error(upgradetx.CodeRejected, "agent roll requires the account administrator signature", false)
	}
	return nil
}
