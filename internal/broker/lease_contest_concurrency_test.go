package broker

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
)

// Concurrency lens on the lease contest. Both tests here drive adjudicateLease
// against a REAL bus, because the decision it makes depends on a request/reply
// round trip whose timing is the whole point.

// leaseBrokerOnBus builds a lease-capable Broker wired to a live NATS server.
func leaseBrokerOnBus(t *testing.T) (*Broker, string) {
	t.Helper()
	url := testharness.StartNATS(t)
	b := leaseBroker(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("broker conn: %v", err)
	}
	t.Cleanup(nc.Close)
	b.nc.Store(nc)
	return b, url
}

// claimProbeResponder installs the agent side of the claim probe: a subscriber
// on the forwarded tree that answers with `id` after `delay`.
func claimProbeResponder(t *testing.T, url, sid, nid, id string, delay time.Duration) {
	t.Helper()
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("agent conn: %v", err)
	}
	t.Cleanup(nc.Close)
	sub, err := nc.Subscribe(proto.SubjCmdForwarded(sid, nid, "*"), func(m *nats.Msg) {
		if m.Reply == "" {
			return
		}
		time.Sleep(delay)
		payload, _ := json.Marshal(proto.ClaimProbeResp{InstanceID: id})
		_ = nc.Publish(m.Reply, payload)
	})
	if err != nil {
		t.Fatalf("agent subscribe: %v", err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
}

// origin: docs/reviews/cloned-credential-instances-plan.md §3.2
//
// THE SELF-IDENTIFYING ANSWER IS ONLY AS GOOD AS THE BUDGET IT MUST BEAT.
//
// probeNameHeldByOther exists to tell "a DIFFERENT process holds this name"
// from "the thing answering is the very process that is re-registering" — the
// latter is what every nats.go reconnect looks like, because subscriptions are
// replayed before the reconnect handler runs. The distinction is carried in the
// reply body, so it is lost the moment the reply misses claimProbeBudget: a
// timeout and a foreign holder produce the SAME verdict.
//
// claimProbeBudget is a fixed 200ms with no relation to the actual broker↔agent
// RTT. The live fleet's broker is a public VPS with agents on other continents,
// where 200ms is an ordinary round trip — so a healthy LONE agent whose answer
// is merely slow is renamed, on the one path (reconnect with a cold holder map)
// the identity answer was added to protect.
func TestAProbeAnswerSlowerThanTheBudgetSuffixesTheAnsweringInstance(t *testing.T) {
	b, url := leaseBrokerOnBus(t)
	// The incumbent IS the registrant: same instance id, answering correctly,
	// just further away than the budget allows.
	claimProbeResponder(t, url, "lab", "gpu1", testInstanceA, claimProbeBudget+80*time.Millisecond)
	seedBeat(t, b, "gpu1", 0)

	lease, code, err := adjudicated(t, b, "lab", "gpu1", &proto.NodeRegisterReq{InstanceID: testInstanceA})
	if err != nil || code != "" {
		t.Fatalf("code=%q err=%v", code, err)
	}
	if lease != nil {
		t.Fatalf("a lone agent that answered the claim probe with its OWN instance id was "+
			"suffixed to %q because the answer arrived after claimProbeBudget (%v). The budget is "+
			"a constant, not a function of the fleet's RTT, so a cross-continent agent is renamed "+
			"on any reconnect that lands inside LeaseGrace with a cold holder map — and the rename "+
			"strands every process row it is running under the old name.",
			lease.AssignedNID, claimProbeBudget)
	}
}

// origin: docs/reviews/cloned-credential-instances-plan.md §3.3
//
// AN ISSUED SUFFIX IS NOT RESERVED. The contested branch deliberately writes
// nothing (that ordering is what protects the incumbent), and it also records
// nothing in the leader-local holder map — so LowestFreeSuffix keeps returning
// the same free name to every challenger until one of them finishes a full
// session rebuild and registers under it.
//
// A herd of N clones therefore costs O(N^2) rebuilds, each one a fresh TCP
// dial plus an auth_callout JWT mint, with no backoff anywhere in Run's rebuild
// loop.
func TestConsecutiveChallengersAreNotAllOfferedTheSameSuffix(t *testing.T) {
	b, url := leaseBrokerOnBus(t)
	// A live incumbent that answers honestly with a DIFFERENT id: every
	// challenger below is genuinely contested.
	claimProbeResponder(t, url, "lab", "gpu1", testInstanceA, 0)
	seedBeat(t, b, "gpu1", 0)

	ids := []string{
		"bbbbbbbbbbbbbbbbbbbbbbbbbb",
		"cccccccccccccccccccccccccc",
		"dddddddddddddddddddddddddd",
		"eeeeeeeeeeeeeeeeeeeeeeeeee",
	}
	seen := map[string]int{}
	for _, id := range ids {
		lease, code, err := adjudicated(t, b, "lab", "gpu1", &proto.NodeRegisterReq{InstanceID: id})
		if err != nil || code != "" || lease == nil {
			t.Fatalf("id %q: lease=%v code=%q err=%v", id, lease, code, err)
		}
		seen[lease.AssignedNID]++
	}
	for name, n := range seen {
		if n > 1 {
			t.Fatalf("%d of %d challengers were all offered %q. Each loser discovers the "+
				"collision only by rebuilding its whole NATS session and registering again, so a "+
				"Deployment scaled to N converges in O(N^2) connect+auth_callout rounds with no "+
				"backoff between them (verdicts: %v)", n, len(ids), name, seen)
		}
	}
}

// origin: docs/reviews/cloned-credential-instances-plan.md §0.3
//
// A MID-LIFE RENAME ORDERS THE AGENT TO KILL ITS OWN WORK.
//
// Adoption is now reachable on the RECONNECT register path (onNATSReconnect →
// requestLeaseRebuild), i.e. on a process that has already been running for a
// while and may be running the operator's jobs. The rebuilt session registers
// under the ASSIGNED name and reports its LocalProcesses — but
// reconcileOnRegister scopes the broker's side to `p.NID != nid`, so every pid
// this process is running is a pid the broker has no row for under the new
// name, and G.1's orphan pass pushes all of them into DropProcesses. The agent
// then SIGTERM+5s+SIGKILLs them (applyReconciliation → killOrphanProcess).
//
// The rows under the OLD name stay RUNNING, so the same rename also produces
// the zombie `ps` rows on the other side.
func TestAdoptingALeaseNameDoesNotOrphanTheInstancesOwnRunningProcesses(t *testing.T) {
	db := testharness.OpenDB(t)
	if _, err := db.Exec(
		`INSERT INTO sessions(sid,name,owner_pubkey_fp,pin_hash) VALUES('lab','lab','SHA256:o','h')`); err != nil {
		t.Fatal(err)
	}
	for _, nid := range []string{"gpu1", "gpu1-02"} {
		if _, err := db.Exec(`INSERT INTO nodes(sid,nid,status) VALUES('lab',?,'ONLINE')`, nid); err != nil {
			t.Fatal(err)
		}
	}
	// The job this instance started while it still held the basename.
	if _, err := db.Exec(
		`INSERT INTO processes(pid,sid,nid,argv,started_at,status,started_by_fp)
		 VALUES('p1','lab','gpu1','["train.py"]',?,'RUNNING','SHA256:u')`, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}

	b := &Broker{}
	b.cfg.DB = db
	b.cfg.Logger = testharness.SilentLog()
	b.cfg.Now = time.Now

	// The FIRST register after the rebuild: same process, same pid, new name.
	_, _, _, _, dropProcesses := b.reconcileOnRegister("lab", "gpu1-02", proto.NodeRegisterReq{
		LocalProcesses: []proto.LocalProcess{{PID: "p1", State: "running"}},
	})
	if len(dropProcesses) != 0 {
		t.Fatalf("the register that follows a lease adoption ordered the agent to kill its own "+
			"running processes %v. A rename is not a restart: the pids are the same OS processes, "+
			"and the only thing that changed is the name the broker files them under.",
			dropProcesses)
	}
}
