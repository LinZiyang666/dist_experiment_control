package agentroll

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"time"

	"github.com/LinZiyang666/tether/internal/agentupgradeport"
	"github.com/LinZiyang666/tether/internal/brokerupgrade/rollcontrol"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/upgradehost"
	"github.com/LinZiyang666/tether/internal/upgradelock"
	"github.com/LinZiyang666/tether/internal/upgradeport"
	"github.com/LinZiyang666/tether/internal/upgraderoll"
	"github.com/LinZiyang666/tether/internal/upgradestage"
	"github.com/LinZiyang666/tether/internal/upgradestore"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/internal/upgradewire"
)

// The broker authenticates the original actor before stamping this envelope.
// Orchestration is distinct from the frozen component wire: its only effects
// are original-plan reservation/publication/release, never a legacy re-exec.
// origin: S8 internal review SIGNED-ROLL-FIXTURES F3; S8 plan §3.1 / §5.1
func IsForwarded(body []byte) bool {
	var f proto.UpgradeTxForwardedReq
	return upgradewire.StrictDecode(body, &f) == nil && rollcontrol.IsRequest(f.Request)
}
func Reply(ctx context.Context, d *Config, endpoint upgradeport.Endpoint, body []byte) ([]byte, error) {
	var f proto.UpgradeTxForwardedReq
	out := rollcontrol.Response{}
	err := upgradewire.StrictDecode(body, &f)
	if err == nil {
		var r rollcontrol.Request
		r, err = rollcontrol.Decode(f.Request)
		if err == nil && (f.ActorFP == "" || r.Parent.ActorFP != f.ActorFP) {
			err = refusal(upgradetx.CodeRejected, "agent roll actor mismatch", false)
		}
		if err == nil {
			out, err = handleRoll(ctx, d, endpoint, r)
		}
	}
	if err != nil {
		out.Code, out.Detail = upgradetx.CodeUnsettled, "agent roll evidence unavailable"
		out.Accepted = true
		var tx *upgradetx.Error
		if errors.As(err, &tx) {
			out.Code, out.Detail, out.Accepted = tx.Code, tx.Detail, tx.Accepted
		}
	}
	return json.Marshal(out)
}
func handleRoll(ctx context.Context, d *Config, endpoint upgradeport.Endpoint, r rollcontrol.Request) (out rollcontrol.Response, err error) {
	if ctx == nil || d == nil || endpoint == nil || d.Store == nil {
		return out, refusal(upgradetx.CodeNotWired, "agent roll ports unavailable", false)
	}
	if err = ctx.Err(); err != nil {
		return
	}
	if r.Key != d.Key && (r.Op != "roll.describe" || r.Key != upgradewire.DescribeSelector(d.Key)) {
		return out, refusal(upgradetx.CodeRejected, "agent roll key mismatch", false)
	}
	if r.Op == "roll.describe" {
		description, e := agentupgradeport.DescribeRoll(d.Runtime, r.Nonce)
		out.Description = &description
		return out, e
	}
	owner, reg, err := agentupgradeport.RollRegistration(d.Runtime)
	if err != nil {
		return out, err
	}
	if r.Parent.Mode != upgradetx.ModeHot || r.Parent.Handle.TxID == "" || r.Parent.ActorFP == "" {
		return out, refusal(upgradetx.CodeRejected, "agent roll parent unavailable", false)
	}
	if r.Op == "roll.bind" {
		if !slices.Contains(r.Scope.Participants, d.Key) {
			return out, refusal(upgradetx.CodeRejected, "agent omitted from parent scope", false)
		}
		return out, upgradehost.BindRollParent(d.Key, owner, r.Parent, r.Scope, r.Route)
	}
	if r.Op == "roll.prepare_step" {
		if r.Prepare.ParentTxID != r.Parent.Handle.TxID || r.Prepare.ParentSlot != upgradehost.RollSlot(r.Parent, d.Key, r.Route) || r.Prepare.ExpiresAt != r.Parent.ExpiresAt || r.Prepare.ActorFP != r.Parent.ActorFP {
			return out, refusal(upgradetx.CodeRejected, "agent child authorization changed", false)
		}
		out.Grant, err = endpoint.Prepare(ctx, d.Key, r.Prepare)
		return
	}
	if err = validateRollPlan(d.Key, r); err != nil {
		return
	}
	released, err := upgradehost.HostReleased(d.Key, owner, r.Parent, r.Plan)
	if err != nil {
		return out, err
	}
	if !released {
		if _, err = upgradehost.ParentDeadline(d.Key, owner, r.Parent.Handle.TxID, upgradehost.RollSlot(r.Parent, d.Key, r.Route)); err != nil {
			return
		}
	}
	switch r.Op {
	case "roll.reserve":
		if released {
			return out, nil
		}
		if err = upgradehost.RecordHost(d.Key, owner, r.Parent, r.Plan, "reserve"); err == nil {
			err = upgradehost.HoldRoll(ctx, d.Key, r.Parent.Handle.TxID)
		}
	case "roll.stage":
		return stageRollStep(ctx, d, r, reg, released)
	case "roll.sample":
		return sampleRollStep(ctx, d, endpoint, r, released)
	case "roll.release_host":
		return releaseRollHost(ctx, d, endpoint, r, owner)
	default:
		err = refusal(upgradetx.CodeRejected, "unsupported agent roll operation", false)
	}
	return
}

// Parent authority is checked before any operation can use the sealed plan.
// origin: hot-update S8 plan §3.1 / §5.1
func validateRollPlan(key upgradetx.Key, r rollcontrol.Request) error {
	if err := upgraderoll.ValidatePlan(r.Plan); err != nil {
		return err
	}
	if !upgradetx.SameCut(r.Plan.Parent, r.Parent.Handle) || r.Plan.Deadline != r.Parent.ExpiresAt || r.Parent.ScopeDigest != upgraderoll.ScopeDigest(r.Plan.Scope) {
		return refusal(upgradetx.CodeRejected, "agent roll plan differs from parent", false)
	}
	member := false
	for _, s := range r.Plan.Steps {
		if s.Participant == key && s.Grant.ParentSlot == upgradehost.RollSlot(r.Parent, key, r.Route) {
			member = true
		}
	}
	if !member {
		return refusal(upgradetx.CodeRejected, "agent outside sealed parent", false)
	}
	return nil
}

// Publication uses the registered installation and the sole durable journal.
func stageRollStep(ctx context.Context, d *Config, r rollcontrol.Request, reg upgradelock.Registration, released bool) (out rollcontrol.Response, err error) {
	if released || !savedRollStep(r) {
		return out, refusal(upgradetx.CodeRejected, "agent stage is outside original plan", false)
	}
	store, ok := d.Store.(*upgradestore.Store)
	if !ok {
		return out, refusal(upgradetx.CodeNotWired, "agent publication journal unavailable", false)
	}
	out.Artifact, err = upgradestage.PublishBroker(ctx, d.Key, reg.CanonicalPath, reg.ControlDir, store, r.Step.Grant, agentupgradeport.RollPublisher(d.Runtime))
	return
}

// Sampling keeps the original grant and reservation proof in the same response.
func sampleRollStep(ctx context.Context, d *Config, endpoint upgradeport.Endpoint, r rollcontrol.Request, released bool) (out rollcontrol.Response, err error) {
	if !savedRollStep(r) {
		return out, refusal(upgradetx.CodeRejected, "agent sample is outside original plan", false)
	}
	out.Sample.View, err = endpoint.Query(ctx, d.Key, r.Step.Grant.Handle.TxID, r.Nonce)
	if err != nil {
		return
	}
	if out.Sample.View.Historical.Grant != r.Step.Grant {
		return out, refusal(upgradetx.CodeUnsettled, "agent sample grant changed", true)
	}
	out.Sample.ParentTxID, out.Sample.Nonce, out.Sample.At = r.Parent.Handle.TxID, r.Nonce, time.Now().UnixNano()
	// An imported parent cut retains its inherited OFD until this host's
	// observed release. Component completion never unlocks the host early.
	// origin: S8 internal review SIGNED-ROLL-FIXTURES F3; S8 plan §5.1
	if !released && out.Sample.View.Historical.Forward.Phase == upgradetx.PhaseCompleted && (d.HasLease == nil || !d.HasLease()) {
		return out, refusal(upgradetx.CodeUnsettled, "agent parent reservation was released before its host", true)
	}
	out.Sample.Release = r.Step.Grant.SourceRelease
	if out.Sample.View.Observation.RunningSHA == r.Step.Grant.TargetSHA {
		out.Sample.Release = r.Step.Grant.TargetRelease
	}
	out.Sample.Released = released
	return
}

// Release follows fresh proof for every original child before dropping custody.
func releaseRollHost(ctx context.Context, d *Config, endpoint upgradeport.Endpoint, r rollcontrol.Request, owner upgradetx.Owner) (out rollcontrol.Response, err error) {
	if !savedRollStep(r) || r.Step.Kind != upgradetx.StepRelease {
		return out, refusal(upgradetx.CodeRejected, "agent release outside original plan", false)
	}
	for _, step := range r.Plan.Steps {
		if step.Participant != d.Key || step.Kind != upgradetx.StepAgentCut {
			continue
		}
		v, e := endpoint.Query(ctx, d.Key, step.Grant.Handle.TxID, r.Nonce)
		if e != nil {
			return out, e
		}
		if v.Historical.Grant != step.Grant || upgradetx.Completed(v.Historical.Forward, v.Observation) != nil {
			return out, refusal(upgradetx.CodeUnsettled, "agent child has not completed", true)
		}
	}
	if d.ReleaseLease != nil {
		err = d.ReleaseLease()
	}
	if err == nil {
		err = upgradehost.ReleaseRoll(d.Key)
	}
	if err == nil {
		err = upgradehost.RecordHost(d.Key, owner, r.Parent, r.Plan, "release")
	}
	if err == nil {
		out.Receipt = rollcontrol.Barrier(r.Step, r.Nonce, owner, rollcontrol.Hash(r.Plan))
	}
	return
}

func savedRollStep(r rollcontrol.Request) bool {
	for _, s := range r.Plan.Steps {
		if reflect.DeepEqual(s, r.Step) {
			return true
		}
	}
	return false
}

type Config struct {
	Key          upgradetx.Key
	Store        upgradetx.Store
	Runtime      upgradeport.RuntimePort
	ReleaseLease func() error
	HasLease     func() bool
}

func refusal(code, detail string, accepted bool) error {
	return &upgradetx.Error{Code: code, Detail: detail, Accepted: accepted}
}
