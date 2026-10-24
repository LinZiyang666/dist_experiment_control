package upgradehost

import (
	"github.com/LinZiyang666/tether/internal/upgradestore"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// Host actions outlive the courier and the component exec. A release closes
// only this binding, after all original children have fresh completion proof.
// origin: S8 internal review SIGNED-ROLL-FIXTURES F3; S8 plan §5.1
func RecordHost(key upgradetx.Key, owner upgradetx.Owner, parent upgradetx.Grant, plan upgradetx.Plan, kind string) error {
	s, err := RollJournal(key, owner, true)
	if err != nil {
		return err
	}
	v, err := s.Load(hostKey(key), "")
	if err != nil {
		return err
	}
	id := digest(kind + ":" + parent.Handle.TxID)[:32]
	a := upgradetx.Action{ID: id, Handle: v.Historical.Forward.Handle, Kind: upgradetx.ActionRelease, IntentDigest: digest(plan), Nonce: parent.ClientNonce}
	for _, old := range v.Historical.Actions {
		if old.ID == id {
			if old.IntentDigest != a.IntentDigest {
				return &upgradetx.Error{Code: upgradetx.CodeUnsettled, Detail: "host plan changed", Accepted: true}
			}
			if old.State == upgradetx.ActionDurable {
				return finishHostRelease(s, key, kind)
			}
			a = old
		}
	}
	for _, state := range []upgradetx.ActionState{upgradetx.ActionIntent, upgradetx.ActionObserved, upgradetx.ActionDurable} {
		v, err = s.Load(hostKey(key), "")
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
			a.ResultDigest = digest(plan)
		}
		if _, err = s.Apply(v.Historical.Forward.Handle, upgradetx.Event{Action: a}); err != nil {
			return err
		}
	}
	return finishHostRelease(s, key, kind)
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

func HostReleased(key upgradetx.Key, owner upgradetx.Owner, parent upgradetx.Grant, plan upgradetx.Plan) (bool, error) {
	s, err := RollJournal(key, owner, true)
	if err != nil {
		return false, err
	}
	v, err := s.Load(hostKey(key), "")
	if err != nil {
		return false, err
	}
	if v.Historical.Grant.ClientNonce != parent.Handle.TxID {
		return false, &upgradetx.Error{Code: upgradetx.CodeUnsettled, Detail: "host binding changed", Accepted: true}
	}
	for _, a := range v.Historical.Actions {
		if a.ID == digest("release:" + parent.Handle.TxID)[:32] && a.IntentDigest == digest(plan) && a.State == upgradetx.ActionDurable {
			return true, nil
		}
	}
	return false, nil
}
