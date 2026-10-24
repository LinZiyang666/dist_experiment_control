package rollcontrol

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/brokerupgradeport"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/upgraderoll"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

func sample(ctx context.Context, c Config, node *cluster.Node, seed []byte, r Request) (out Sample, err error) {
	if r.Step.Participant != c.Key || r.Step.Grant.ParentTxID != r.Parent.Handle.TxID {
		return out, Unknown("sample participant differs from the saved child")
	}
	out.View, err = c.Endpoint.Query(ctx, c.Key, r.Step.Grant.Handle.TxID, r.Nonce)
	if err != nil {
		return
	}
	if out.View.Historical.Grant != r.Step.Grant {
		return out, Unknown("sample grant changed")
	}
	out.ParentTxID, out.Nonce, out.At = r.Parent.Handle.TxID, r.Nonce, time.Now().UnixNano()
	out.Release = r.Step.Grant.SourceRelease
	if out.View.Observation.RunningSHA == r.Step.Grant.TargetSHA {
		out.Release = r.Step.Grant.TargetRelease
	}
	out.Holder = holder(node, r.Parent.Handle.TxID) == nil
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
			out.Released = true
		}
	}
	b, _ := json.Marshal(out)
	sig, e := auth.SignWithSeed(seed, b)
	if e != nil {
		return out, e
	}
	out.Signature = hex.EncodeToString(sig)
	return
}

// Samples are couriers of authenticated live facts. The parent writer checks
// their signatures, original grant, challenge and sampling window itself;
// it never signs a CLI-invented empty census or counts phases as completion.
// origin: hot-update S8 plan §4.2 / §5.1; S8 internal review cluster-roll F7
func collect(seed []byte, plan upgradetx.Plan, r Request, completed bool) ([]upgradetx.Resource, []upgraderoll.FleetMember, error) {
	pub, err := auth.PublicKeyFromSeed(seed)
	if err != nil {
		return nil, nil, err
	}
	byID := map[string]Sample{}
	for _, sample := range r.Samples {
		sig, e := hex.DecodeString(sample.Signature)
		sample.Signature = ""
		b, _ := json.Marshal(sample)
		if e != nil || auth.VerifySignature(pub, b, sig) != nil || sample.ParentTxID != plan.Parent.TxID || sample.Nonce != r.Nonce || len(r.Nonce) != 32 || time.Since(time.Unix(0, sample.At)) > 5*time.Second || sample.At > time.Now().UnixNano() {
			return nil, nil, Unknown("fleet sample is stale or unauthenticated")
		}
		tx := sample.View.Historical.Grant.Handle.TxID
		if _, ok := byID[tx]; ok {
			return nil, nil, Unknown("duplicate fleet sample")
		}
		byID[tx] = sample
	}
	var rows []upgradetx.Resource
	var members []upgraderoll.FleetMember
	for _, step := range plan.Steps {
		if step.Kind != upgradetx.StepBrokerCut && step.Kind != upgradetx.StepAgentCut {
			continue
		}
		sample, ok := byID[step.Grant.Handle.TxID]
		if !ok {
			return nil, nil, Unknown("fleet omits an original participant")
		}
		delete(byID, step.Grant.Handle.TxID)
		v, o := sample.View, sample.View.Observation
		if v.Code != "" || v.Historical.Grant != step.Grant || o.Nonce != r.Nonce || !o.OwnerKnown || !o.Census.Known || !o.Census.Complete || o.Census.Admitting != 0 || len(o.Census.OldOwners) != 0 || !upgradetx.SameCut(o.Current.Handle, step.Grant.Handle) || (o.RunningSHA != step.Grant.SourceSHA && o.RunningSHA != step.Grant.TargetSHA) {
			return nil, nil, Unknown("fleet owner or running image is unknown")
		}
		expected, _ := upgradetx.ExpectedDigest(o.Current.Scope)
		release := step.Grant.SourceRelease
		if o.RunningSHA == step.Grant.TargetSHA {
			release = step.Grant.TargetRelease
		}
		if sample.Release != release || o.At <= 0 || o.Census.Stamp.At != o.At || o.Census.Stamp.Nonce != r.Nonce || o.Census.Stamp.Attempt != o.Current.Handle || o.Census.Stamp.ScopeDigest != step.Grant.ScopeDigest || o.Census.ExpectedDigest != expected || upgraderoll.ScopeDigest(o.Current.Scope) != step.Grant.ScopeDigest || !upgradetx.CensusCovers(o.Current.Scope, o.Census) {
			return nil, nil, Unknown("fleet sample does not bind its original census")
		}
		if upgraderoll.ObservationOnly(step.Grant) && upgraderoll.ObserveUnchanged(v, step.Grant, r.Nonce) != nil {
			return nil, nil, Unknown("observation-only participant changed")
		}
		if completed && ((!upgraderoll.ObservationOnly(step.Grant) && upgradetx.Completed(v.Historical.Forward, o) != nil) || !sample.Released) {
			return nil, nil, Unknown("participant or host reservation is unsettled")
		}
		owners := map[upgradetx.Owner]uint64{}
		for _, row := range o.Census.Resources {
			rows = append(rows, upgraderoll.ParentResource(step.Participant, row))
			owners[row.Owner] = row.Generation
		}
		if len(owners) == 0 {
			generation := o.Current.SourceGeneration
			if o.RunningSHA == step.Grant.TargetSHA && !upgraderoll.ObservationOnly(step.Grant) {
				generation = o.Current.Handle.Generation
			}
			owners[o.Census.Stamp.Producer] = generation
		}
		for owner, generation := range owners {
			members = append(members, upgraderoll.FleetMember{Participant: step.Participant, Owner: owner, Generation: generation, Release: sample.Release, Known: true})
		}
	}
	if len(byID) != 0 {
		return nil, nil, Unknown("fleet contains an unplanned participant")
	}
	sort.Slice(rows, func(i, j int) bool {
		return string(rows[i].Domain)+"\x00"+rows[i].ID+"\x00"+rows[i].Origin < string(rows[j].Domain)+"\x00"+rows[j].ID+"\x00"+rows[j].Origin
	})
	return rows, members, nil
}
func aggregate(c Config, node *cluster.Node, seed []byte, view upgradetx.View, plan upgradetx.Plan, r Request) (fleet upgraderoll.FleetProof, o upgradetx.Observation, err error) {
	complete := r.Op != "roll.fleet"
	rows, members, err := collect(seed, plan, r, complete)
	if err != nil {
		return fleet, o, err
	}
	held := holder(node, plan.Parent.TxID) == nil
	if complete {
		if _, err = clusterReceipt(c, node, Request{Parent: r.Parent, Plan: plan, Step: plan.Steps[len(plan.Steps)-1], Nonce: r.Nonce}); err != nil {
			return
		}
	}
	handle := view.Historical.Forward.Handle
	if !complete {
		handle = plan.Parent
	} // Fleet authority names the immutable plan, not a later journal CAS.
	stamp := upgradetx.Stamp{Attempt: handle, Producer: c.Owner, Nonce: r.Nonce, Sequence: 1, At: time.Now().UnixNano(), ScopeDigest: upgraderoll.ScopeDigest(plan.Scope)}
	expected, _ := upgradetx.ExpectedDigest(plan.Scope)
	census := upgradetx.Census{Stamp: stamp, Known: true, Complete: true, ExpectedDigest: expected, Resources: rows}
	fleet = upgraderoll.FleetProof{Parent: plan.Parent, Nonce: r.Nonce, Known: true, Complete: true, Participants: plan.Scope.Participants, Members: members, Census: census, HolderHeld: held}
	if !complete {
		return
	}
	a := view.Historical.Forward
	a.Phase, a.CutStage, a.Outcome = upgradetx.PhaseCompleted, upgradetx.CutReleased, upgradetx.OutcomeSuccess
	receipts := []*upgradetx.Receipt{&a.Control, &a.Resources, &a.Released}
	kinds := []upgradetx.ReceiptKind{upgradetx.ReceiptControlReady, upgradetx.ReceiptResourcesMigrated, upgradetx.ReceiptOldGenerationReleased}
	for i, p := range receipts {
		*p = upgradetx.Receipt{Kind: kinds[i], Stamp: stamp, Known: true, Complete: true, RunningSHA: a.TargetSHA, Census: census, EvidenceDigest: Hash(struct {
			Kind    upgradetx.ReceiptKind
			Samples []Sample
		}{kinds[i], r.Samples})}
	}
	o = upgradetx.Observation{Nonce: r.Nonce, At: stamp.At, Current: a, Census: census, OwnerKnown: true, RunningSHA: a.TargetSHA, EffectiveSHA: a.TargetSHA}
	return
}
func holder(node *cluster.Node, tx string) error {
	state, err := cluster.ReadUpgradeLock(node.RODB())
	if err != nil {
		return err
	}
	marker, err := cluster.UpgradeHolderMarker(tx)
	if err != nil {
		return err
	}
	expiry, e := time.Parse(time.RFC3339Nano, state.Lease)
	if state.Marker != marker || e != nil || !time.Now().Before(expiry) {
		return Unknown("original cluster holder is no longer live")
	}
	return nil
}

// The release tombstone and expected-holder delete commit in ONE replicated
// command. Expiry, a different holder, or a lost reply cannot become a release
// receipt. Replays read the immutable tombstone; they never reacquire a lease.
// origin: hot-update S8 plan §3.2 / §5.1; S8 internal review cluster-roll F8
func releaseCluster(c Config, node *cluster.Node, seed []byte, r Request) (upgradetx.Receipt, error) {
	if err := upgraderoll.ValidatePlan(r.Plan); err != nil {
		return upgradetx.Receipt{}, err
	}
	if !upgradetx.SameCut(r.Plan.Parent, r.Parent.Handle) || !reflect.DeepEqual(r.Step, r.Plan.Steps[len(r.Plan.Steps)-1]) {
		return upgradetx.Receipt{}, Unknown("release is outside its parent plan")
	}
	if _, _, err := collect(seed, r.Plan, r, true); err != nil {
		return upgradetx.Receipt{}, err
	}
	err := node.Propose(func(db *sql.DB) (*cluster.Command, error) {
		var saved string
		e := db.QueryRow("SELECT value FROM cluster_meta WHERE key=?", releaseKey(r.Parent)).Scan(&saved)
		if e == nil {
			if saved != Hash(r.Plan) {
				return nil, Unknown("release tombstone differs from the plan")
			}
			return nil, nil
		}
		if !errors.Is(e, sql.ErrNoRows) {
			return nil, e
		}
		cmd, e := cluster.PlanClearUpgradeActiveExpected(db, r.Parent.Handle.TxID, time.Now())
		if e != nil {
			return nil, e
		}
		state, e := cluster.ReadUpgradeLock(db)
		if e != nil {
			return nil, e
		}
		statement := cluster.Stmt("INSERT INTO cluster_meta(key,value) SELECT " + cluster.MustLitText(releaseKey(r.Parent)) + "," + cluster.MustLitText(Hash(r.Plan)) + " WHERE EXISTS(SELECT 1 FROM cluster_meta WHERE key=" + cluster.MustLitText(cluster.MetaKeyUpgradeActive) + " AND value=" + cluster.MustLitText(state.Marker) + ") AND EXISTS(SELECT 1 FROM cluster_meta WHERE key=" + cluster.MustLitText(cluster.MetaKeyUpgradeLease) + " AND value=" + cluster.MustLitText(state.Lease) + ")")
		cmd.Body = append([]cluster.Statement{statement}, cmd.Body...)
		return cmd, nil
	})
	if err != nil {
		return upgradetx.Receipt{}, err
	}
	return clusterReceipt(c, node, r)
}
func releaseKey(g upgradetx.Grant) string { return "upgrade_roll_released." + g.Handle.TxID }
func clusterReceipt(c Config, node *cluster.Node, r Request) (upgradetx.Receipt, error) {
	var saved string
	err := node.RODB().QueryRow("SELECT value FROM cluster_meta WHERE key=?", releaseKey(r.Parent)).Scan(&saved)
	state, e := cluster.ReadUpgradeLock(node.RODB())
	if err != nil || e != nil || saved != Hash(r.Plan) || state.Marker != "" {
		return upgradetx.Receipt{}, Unknown("original cluster release has no committed readback")
	}
	return barrier(r.Step, r.Nonce, c.Owner, saved), nil
}

// SignSample authenticates a freshly collected sample at the broker courier.
// The actor never gets authority to sign or replace the forwarded agent view.
// origin: S8 internal review SIGNED-ROLL-FIXTURES F3; S8 plan §4.2
func SignSample(seed []byte, sample Sample) (Sample, error) {
	sample.Signature = ""
	b, _ := json.Marshal(sample)
	sig, err := auth.SignWithSeed(seed, b)
	if err != nil {
		return sample, err
	}
	sample.Signature = hex.EncodeToString(sig)
	return sample, nil
}

// Barrier is the same host-release projection for either component owner.
func Barrier(step upgradetx.Step, n string, o upgradetx.Owner, e string) upgradetx.Receipt {
	return barrier(step, n, o, e)
}
