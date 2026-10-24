// classifyRehomeErr maps a Propose failure to the closed reason enum (never the raw error string).
package brokercontrol

import (
	"errors"

	"github.com/LinZiyang666/tether/internal/cluster"
)

// rehome event kinds + closed reason enum.
const (
	EvHomeReassignStarted   = "home_reassign_started"
	EvHomeReassignSucceeded = "home_reassign_succeeded"
	EvHomeReassignFailed    = "home_reassign_failed"
	EvRehomeStalled         = "rehome_stalled"
	EvBrokerDownRehomeSum   = "broker_down_rehome_summary"

	ReasonNoEligibleTarget = "no_eligible_target"
	ReasonTargetNotReady   = "target_not_ready"
	ReasonNotLeader        = "not_leader"
	ReasonStoreError       = "store_error"
)

// ClassifyRehomeErr maps a Propose failure to the closed reason enum (never the raw error string).
func ClassifyRehomeErr(err error) string {
	if errors.Is(err, cluster.ErrForwardNotLeader) || cluster.IsNotLeader(err) {
		return ReasonNotLeader
	}
	return ReasonStoreError
}
