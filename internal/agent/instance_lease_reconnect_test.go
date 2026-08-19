package agent

import (
	"testing"

	"github.com/LinZiyang666/tether/internal/proto"
)

// origin: docs/reviews/cloned-credential-instances-plan.md §3.1 step 4/5
//
// A CONTESTED register reply is NOT a successful register: the broker returned
// before registerNode, so it never looked at LocalProcesses and never accepted
// a single pid. Agent.session() honours that by returning early — but
// onNATSReconnect (proxy.go) re-registers on the live conn and feeds whatever
// comes back straight into the courier, with no Lease check.
//
// onRegisterSuccess reads "pid absent from AcceptedProcesses" as "the broker
// already has this exit", so a contested reply — which carries no accepted
// pids at all — DELETES every pending exit. That is the 2026-08-04 zombie-row
// class, re-armed: the ctl waiting on the exit never gets it and `tether ps`
// keeps the row RUNNING.
//
// This is reachable on a device that runs exactly ONE agent: after a broker
// restart the leaseHolder map is empty, the node's heartbeat is seconds old,
// and the interest probe finds the agent's own replayed subscription — see
// the lone-agent probe test in internal/broker/instance_lease_probe_test.go.
func TestContestedRegisterReplyMustNotClearPendingExits(t *testing.T) {
	c := newProcCourier(nil)
	c.enqueueExit("pid-alpha", 7)

	c.onRegisterSuccess(proto.NodeRegisterResp{
		OK:    true,
		Lease: &proto.NodeLease{AssignedNID: "gpu1-02", Basename: "gpu1"},
	})

	c.mu.Lock()
	n := len(c.pending)
	c.mu.Unlock()
	if n == 0 {
		t.Fatal("a contested register reply cleared the pending exit queue: " +
			"the broker never processed the snapshot, so rc=7 is now lost forever " +
			"and the ps row stays RUNNING")
	}
}
