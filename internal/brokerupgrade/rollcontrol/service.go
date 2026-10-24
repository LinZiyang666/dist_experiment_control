package rollcontrol

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"time"

	"github.com/LinZiyang666/tether/internal/brokerupgradeport"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/upgradeartifact"
	"github.com/LinZiyang666/tether/internal/upgradelock"
	"github.com/LinZiyang666/tether/internal/upgradeport"
	"github.com/LinZiyang666/tether/internal/upgraderoll"
	"github.com/LinZiyang666/tether/internal/upgradestore"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// Config comes only from the running broker. No journal pathname, publication
// directory, owner identity or release function comes from the envelope.
// Operations open the canonical daemon journal; no CLI side journal exists.
type Config struct {
	Key          upgradetx.Key
	Owner        upgradetx.Owner
	Store        upgradetx.Store
	Endpoint     upgradeport.Endpoint
	Runtime      upgradeport.RuntimePort
	Stage        func(context.Context, upgradetx.Grant) (upgradeartifact.Receipt, error)
	Homes        func() error
	ReleaseLease func() error
}

func Handle(ctx context.Context, c Config, node *cluster.Node, seed []byte, r Request, actor string) Response {
	out, err := handle(ctx, c, node, seed, r, actor)
	if err != nil {
		var typed *upgradetx.Error
		if errors.As(err, &typed) {
			out.Code, out.Detail, out.Accepted = typed.Code, typed.Detail, typed.Accepted
		} else {
			out.Code, out.Detail, out.Accepted = upgradetx.CodeUnsettled, "roll control operation could not be observed", true
		}
	}
	return out
}
func handle(ctx context.Context, c Config, node *cluster.Node, seed []byte, r Request, actor string) (out Response, err error) {
	if c.Store == nil || c.Endpoint == nil || c.Owner.PID == 0 || actor == "" || node == nil {
		return out, Error(upgradetx.CodeNotWired, "cluster roll ports are unavailable", false)
	}
	if err = ctx.Err(); err != nil {
		return
	}
	journal, err := brokerupgradeport.RollJournal(c.Key, c.Owner, false, node.SelfID())
	if err != nil {
		return out, err
	}
	if r.Op == "roll.check_idle" {
		return out, checkIdleRoll(ctx, c, node, journal)
	}
	if r.Op == "roll.prepare_parent" {
		if r.Prepare.ActorFP != actor || r.Prepare.ParentTxID != "" {
			return out, Error(proto.CodeNotOwner, "parent actor mismatch", false)
		}
		out.Grant, err = journal.Prepare(parentKey(c.Key, node.SelfID()), r.Prepare)
		return
	}
	if r.Parent.ActorFP != actor || r.Parent.Mode != upgradetx.ModeHot || r.Parent.Handle.TxID == "" {
		return out, Error(proto.CodeNotOwner, "roll actor mismatch", false)
	}
	switch r.Op {
	case "roll.bind":
		err = brokerupgradeport.BindRollParent(c.Key, c.Owner, r.Parent, r.Scope, node.SelfID())
		return
	case "roll.prepare_step":
		if r.Prepare.ParentTxID != r.Parent.Handle.TxID || r.Prepare.ParentSlot != brokerupgradeport.RollSlot(r.Parent, c.Key, node.SelfID()) || r.Prepare.ExpiresAt != r.Parent.ExpiresAt || r.Prepare.ActorFP != actor {
			return out, Unknown("child authorization differs from its parent")
		}
		if r.Key == c.Key {
			out.Grant, err = c.Endpoint.Prepare(ctx, c.Key, r.Prepare)
		} else if r.Key == parentKey(c.Key, node.SelfID()) {
			out.Grant, err = journal.Prepare(r.Key, r.Prepare)
		} else {
			err = Error(proto.CodeNotOwner, "step key is not local", false)
		}
		return
	}
	// Parent operations are bound to the original issuer's logical key, even
	// after leadership moves. A different broker never creates a replacement.
	parentLocal := r.Parent.Handle.Key == parentKey(c.Key, node.SelfID())
	if parentLocal && r.Op == "roll.accept_parent" {
		fence := upgradetx.Fence{Owner: c.Owner, RunningSHA: r.Parent.SourceSHA}
		if old, e := journal.Load(r.Parent.Handle.Key, ""); e == nil {
			fence.Current = old.Historical.Forward.Handle
			fence.PreviousSHA = old.Historical.RecordedPreviousSHA
		}
		out.View.Historical, err = journal.Start(r.Parent, fence)
		return
	}
	if r.Op == "roll.home" {
		if upgraderoll.ValidatePlan(r.Plan) != nil || r.Plan.Parent.TxID != r.Parent.Handle.TxID || c.Homes == nil {
			return out, Unknown("home barrier has no original plan or collector")
		}
		err = node.VerifyLeaderRead(func(*sql.DB) error { return c.Homes() })
		if err == nil {
			out.Receipt = barrier(r.Step, r.Nonce, c.Owner, Hash(r.Plan))
			out.Receipt.Kind = upgradetx.ReceiptControlReady
		}
		return
	}
	if r.Op == "roll.sample" {
		out.Sample, err = sample(ctx, c, node, seed, r)
		return
	}
	if r.Op == "roll.reserve_cluster" {
		err = reserveCluster(node, r)
		return
	}
	if r.Op == "roll.release_cluster" {
		out.Receipt, err = releaseCluster(c, node, seed, r)
		return
	}
	if r.Op == "roll.check_cluster" {
		out.Receipt, err = clusterReceipt(c, node, r)
		return
	}
	if r.Op == "roll.reserve" || r.Op == "roll.release_host" || r.Op == "roll.stage" || r.Op == "roll.preflight" {
		return handleLocalRoll(ctx, c, node, r)
	}
	if !parentLocal {
		return out, Error(proto.CodeNotOwner, "parent journal belongs to another broker", false)
	}
	return handleParentRoll(c, node, seed, r, journal)
}

// checkIdleRoll requires both the cluster reservation and the original parent
// to be settled before proving that the component lock is available.
// origin: hot-update S8 plan §7.1; S10 LINT-L4 responsibility split
func checkIdleRoll(ctx context.Context, c Config, node *cluster.Node, journal *upgradestore.Store) error {
	state, e := cluster.ReadUpgradeLock(node.RODB())
	if e != nil || state.Marker != "" {
		return Unknown("cluster still holds an upgrade reservation")
	}
	v, e := journal.Load(parentKey(c.Key, node.SelfID()), "")
	var typed *upgradetx.Error
	if e != nil && (!errors.As(e, &typed) || typed.Code != upgradetx.CodeGrantUnknown) {
		return e
	}
	if e == nil && v.Historical.Forward.Phase != upgradetx.PhaseCompleted && !upgradetx.CancellationSettled(v.Historical) {
		return Unknown("an original parent is still unresolved")
	}
	lease, e := upgradelock.Acquire(ctx, c.Key, "")
	if e != nil {
		return e
	}
	return lease.Close()
}

// handleLocalRoll keeps host reservation, stage and preflight behind the same
// sealed-plan membership guard; none may acquire another parent's authority.
// origin: hot-update S8 plan §7.1; S10 LINT-L4 responsibility split
func handleLocalRoll(ctx context.Context, c Config, node *cluster.Node, r Request) (out Response, err error) {
	if err = localPlan(c, node.SelfID(), r); err != nil {
		return
	}
	switch r.Op {
	case "roll.reserve":
		h, e := brokerupgradeport.RollJournal(c.Key, c.Owner, true)
		if e != nil {
			return out, e
		}
		v, e := h.Load(hostKey(c.Key), "")
		if e != nil {
			return out, e
		}
		for _, a := range v.Historical.Actions {
			if a.ID == Hash("release:" + r.Parent.Handle.TxID)[:32] && a.IntentDigest == Hash(r.Plan) && a.State == upgradetx.ActionDurable {
				return out, nil
			}
		}
		if err = recordHost(c, r, "reserve"); err != nil {
			return
		}
		err = brokerupgradeport.HoldRoll(ctx, c.Key, r.Parent.Handle.TxID)
	case "roll.release_host":
		for _, step := range r.Plan.Steps {
			if step.Participant != c.Key || step.Kind != upgradetx.StepBrokerCut {
				continue
			}
			v, e := c.Endpoint.Query(ctx, c.Key, step.Grant.Handle.TxID, r.Nonce)
			if e != nil {
				return out, e
			}
			if v.Historical.Grant != step.Grant || (upgraderoll.ObservationOnly(step.Grant) && upgraderoll.ObserveUnchanged(v, step.Grant, r.Nonce) != nil) || (!upgraderoll.ObservationOnly(step.Grant) && upgradetx.Completed(v.Historical.Forward, v.Observation) != nil) {
				return out, Unknown("host child is not complete")
			}
		}
		if c.ReleaseLease != nil {
			err = c.ReleaseLease()
		}
		if err == nil {
			err = brokerupgradeport.ReleaseRoll(c.Key)
		}
		if err == nil {
			err = recordHost(c, r, "release")
		}
		if err == nil {
			out.Receipt = barrier(r.Step, r.Nonce, c.Owner, Hash(r.Plan))
		}
	case "roll.preflight":
		if c.Stage == nil || c.Runtime == nil {
			return out, Unknown("preflight ports are unavailable")
		}
		receipt, e := c.Stage(ctx, r.Step.Grant)
		if e != nil {
			return out, e
		}
		_, err = c.Runtime.Preflight(ctx, r.Step.Grant, receipt)
	case "roll.stage":
		if c.Stage == nil {
			err = Error(upgradetx.CodeDeferred, "publish both grant images on the target host, then resume", false)
		} else {
			out.Artifact, err = c.Stage(ctx, r.Step.Grant)
		}
	}
	return
}

// handleParentRoll mutates only the original issuer's journal after handle has
// checked locality. Fleet completion still requires independent signed proof.
// origin: hot-update S8 plan §7.1; S10 LINT-L4 responsibility split
func handleParentRoll(c Config, node *cluster.Node, seed []byte, r Request, journal *upgradestore.Store) (out Response, err error) {
	view, e := journal.Load(r.Parent.Handle.Key, r.Parent.Handle.TxID)
	if e != nil {
		return out, e
	}
	if view.Historical.Grant != r.Parent {
		return out, Error(proto.CodeNotOwner, "parent grant mismatch", false)
	}
	switch r.Op {
	case "roll.load":
		out.View = view
	case "roll.seal":
		out.Plan, err = journal.SealPlan(r.Plan)
	case "roll.plan":
		out.Plan, err = journal.LoadPlan(r.Expected)
	case "roll.apply":
		saved, e := journal.LoadPlan(r.Parent.Handle)
		if e != nil {
			return out, e
		}
		valid := false
		for _, step := range saved.Steps {
			wanted := Action(saved, step)
			a := r.Event.Action
			if wanted.ID == a.ID && wanted.Kind == a.Kind && wanted.IntentDigest == a.IntentDigest && wanted.Nonce == a.Nonce && upgradetx.SameCut(a.Handle, saved.Parent) {
				valid = true
			}
		}
		if !valid || !reflect.DeepEqual(r.Event, upgradetx.Event{Action: r.Event.Action}) {
			return out, Unknown("parent action is outside its sealed plan")
		}
		out.View.Historical, err = journal.Apply(r.Expected, r.Event)
	case "roll.fleet", "roll.observe", "roll.complete":
		plan, e := journal.LoadPlan(r.Parent.Handle)
		if e != nil {
			return out, e
		}
		var proof upgraderoll.FleetProof
		proof, out.Observation, err = aggregate(c, node, seed, view, plan, r)
		out.Fleet = WireFleet(proof)
		if err == nil && r.Op == "roll.complete" {
			out.View, err = journal.CompletePlan(r.Expected, out.Observation)
		}
	default:
		err = Error(upgradetx.CodeRejected, "unknown roll operation", false)
	}
	return
}

func localPlan(c Config, nodeID string, r Request) error {
	if err := upgraderoll.ValidatePlan(r.Plan); err != nil {
		return err
	}
	if !upgradetx.SameCut(r.Plan.Parent, r.Parent.Handle) || r.Plan.Deadline != r.Parent.ExpiresAt || r.Parent.ScopeDigest != upgraderoll.ScopeDigest(r.Plan.Scope) {
		return Unknown("plan does not bind its original parent")
	}
	if _, err := brokerupgradeport.ParentDeadline(c.Key, c.Owner, r.Parent.Handle.TxID, brokerupgradeport.RollSlot(r.Parent, c.Key, nodeID)); err != nil {
		if r.Op != "roll.reserve" && r.Op != "roll.release_host" {
			return err
		}
		h, e := brokerupgradeport.RollJournal(c.Key, c.Owner, true)
		if e != nil {
			return err
		}
		v, e := h.Load(hostKey(c.Key), "")
		if e != nil || v.Historical.Grant.ClientNonce != r.Parent.Handle.TxID {
			return err
		}
		released := false
		for _, a := range v.Historical.Actions {
			if a.ID == Hash("release:" + r.Parent.Handle.TxID)[:32] && a.IntentDigest == Hash(r.Plan) && a.State == upgradetx.ActionDurable {
				released = true
			}
		}
		if !released {
			return err
		}
	}
	for _, step := range r.Plan.Steps {
		if step.Participant == c.Key && step.Grant.ParentSlot == brokerupgradeport.RollSlot(r.Parent, c.Key, nodeID) {
			return nil
		}
	}
	return Error(proto.CodeNotOwner, "broker is outside the sealed plan", false)
}
func recordHost(c Config, r Request, kind string) error {
	s, err := brokerupgradeport.RollJournal(c.Key, c.Owner, true)
	if err != nil {
		return err
	}
	v, err := s.Load(hostKey(c.Key), "")
	if err != nil {
		return err
	}
	id := Hash(kind + ":" + r.Parent.Handle.TxID)[:32]
	a := upgradetx.Action{ID: id, Handle: v.Historical.Forward.Handle, Kind: upgradetx.ActionRelease, IntentDigest: Hash(r.Plan), Nonce: r.Parent.ClientNonce}
	for _, old := range v.Historical.Actions {
		if old.ID == id {
			if old.IntentDigest != a.IntentDigest {
				return Unknown("host plan changed")
			}
			if old.State == upgradetx.ActionDurable {
				return finishHostRelease(s, c.Key, kind)
			}
			a = old
		}
	}
	for _, state := range []upgradetx.ActionState{upgradetx.ActionIntent, upgradetx.ActionObserved, upgradetx.ActionDurable} {
		v, err = s.Load(hostKey(c.Key), "")
		if err != nil {
			return err
		}
		if a.State == upgradetx.ActionDurable {
			break
		}
		if a.State == upgradetx.ActionObserved && state == upgradetx.ActionIntent {
			continue
		}
		a.State = state
		if state != upgradetx.ActionIntent {
			a.ResultDigest = Hash(r.Plan)
		}
		if _, err = s.Apply(v.Historical.Forward.Handle, upgradetx.Event{Action: a}); err != nil {
			return err
		}
	}
	return finishHostRelease(s, c.Key, kind)
}
func hostKey(k upgradetx.Key) upgradetx.Key { k.ConfiguredNID += "@host"; return k }
func barrier(step upgradetx.Step, nonce string, owner upgradetx.Owner, evidence string) upgradetx.Receipt {
	stamp := upgradetx.Stamp{Attempt: step.Grant.Handle, Producer: owner, Nonce: nonce, Sequence: 1, At: time.Now().UnixNano(), ScopeDigest: step.Grant.ScopeDigest}
	return upgradetx.Receipt{Kind: upgradetx.ReceiptOldGenerationReleased, Stamp: stamp, Known: true, Complete: true, EvidenceDigest: evidence}
}

var _ upgraderoll.PlanStore = (*upgradestore.Store)(nil)

func parentKey(key upgradetx.Key, nodeID string) upgradetx.Key {
	key.ConfiguredNID = nodeID + "@roll"
	return key
}

func finishHostRelease(s *upgradestore.Store, key upgradetx.Key, kind string) error {
	if kind != "release" {
		return nil
	}
	v, err := s.Load(hostKey(key), "")
	if err != nil {
		return err
	}
	if v.Historical.Forward.Phase == upgradetx.PhaseCancelled {
		return nil
	}
	_, err = s.Apply(v.Historical.Forward.Handle, upgradetx.Event{Phase: upgradetx.PhaseCancelled, CutStage: upgradetx.CutNone, Outcome: upgradetx.OutcomeCancelled})
	return err
}
