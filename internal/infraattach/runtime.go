package infraattach

import "github.com/LinZiyang666/tether/internal/drillobserve"

// HealthContactStale uses current leader evidence even across a host restart;
// a new process cannot turn quorum loss into a fresh grace period.
func HealthContactStale(fenced, stateLeader, writable bool, leaderID, selfID string) bool {
	if writable {
		return false
	}
	if stateLeader || fenced {
		return true
	}
	return leaderID == "" || leaderID == selfID
}

// ProcessFDCount shares the observer's descriptor census and unknown result.
func ProcessFDCount() int { return drillobserve.ProcessFDCount() }
