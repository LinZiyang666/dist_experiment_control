package brokerupgrade

import (
	"database/sql"

	"github.com/LinZiyang666/tether/internal/brokerupgrade/agentroute"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/upgradeauthority"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/internal/upgradewire"
)

// RegisterAuthority compares a register's inherited expectation with the
// session's actual creation identity before any register, lease or farewell
// write, and returns the actual incarnation for the reply. An ordinary first
// register (empty expectation) learns the value; if the creation identity is
// unreadable it is simply omitted, which keeps legacy replies byte-identical.
// A non-empty expectation that cannot be proven equal always refuses: an image
// carried across exec must never be re-bound to a recreated session.
//
// This read is the broker handler's admission. P16 repeats the comparison in
// the actual writer transaction / leader planner, because a lookup here cannot
// by itself prevent a delete/recreate race before the write commits.
// origin: hot-update S8 plan §6.2 / §11.2 P15 (adjudicateLease and farewell calls)
func RegisterAuthority(db *sql.DB, sid, expected string) (string, error) {
	if db == nil {
		if expected == "" {
			return "", nil
		}
		return "", refusal(upgradetx.CodeDeferred, "session creation identity is unavailable")
	}
	creation, err := upgradeauthority.ReadCreation(db, upgradeauthority.Guard{SID: sid, Incarnation: expected})
	if err != nil {
		if expected == "" {
			return "", nil
		}
		return "", err
	}
	return upgradeauthority.CreationIncarnation(creation)
}

// Family authority shares the route leaf's clone-family classification.
// origin: hot-update S8 plan §12.1; S8 BROKERUPGRADE-SLIM
func ConfiguredBasename(db *sql.DB, sid, nid string, req *proto.NodeRegisterReq) string {
	return agentroute.ConfiguredBasename(db, sid, nid, req)
}

func TrustedPreviousNID(db *sql.DB, sid, nid string, req *proto.NodeRegisterReq) string {
	return agentroute.TrustedPreviousNID(db, sid, nid, req)
}

// IncarnationMismatch distinguishes a proven different creation (revocation)
// from an unreadable one, which the register loop may retry.
func IncarnationMismatch(err error) bool {
	return hasCode(err, upgradetx.CodeSessionIncarnationMismatch)
}

// HealthSummary projects the broker's Current journal record into broadcast
// health. It is a disk read, never a running proof: RunningSHA stays empty
// because no fresh observation was taken, and the public summary omits SID,
// actor and resource sets. A missing or unreadable journal reports nothing.
// origin: hot-update S8 plan §2.2 / §7.1 / §11.2 P15
func HealthSummary(deps *Dependencies) *proto.UpgradeSummary {
	if deps == nil || deps.Store == nil {
		return nil
	}
	view, err := deps.Store.Load(deps.Key, "")
	if err != nil {
		return nil
	}
	view.Observation = upgradetx.Observation{Current: active(view.Historical)}
	return upgradewire.Summary(view)
}
