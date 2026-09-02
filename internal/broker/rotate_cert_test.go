package broker

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/hashicorp/raft"
)

// rotate_cert_test.go (formerly r11_rotate_cert_test.go; R11 P11/#56) — rotate-tunnel-cert is a SELF-ONLY verb. On a FOLLOWER it
// must NOT get the generic mutating-verb leader-redirect ("re-run on the leader host"); it must get
// the self-only guidance ("this IS the target broker but it is a FOLLOWER — transfer leadership to it
// first"). Sending an operator to the leader host to rotate a follower's cert is a dead end (#56's
// "back-and-forth"), because the TARGET must BE the leader.

// rotate2NodeFollower brings up a REAL two-node inmem raft, waits for a leader, and returns the
// FOLLOWER's admin backend + node + both ids. Uses AUTO inmem addresses (prevote_test.go's proven
// pattern) so parallel/sequential tests never collide on a shared address.
//
// EXTERNAL REVIEW B4 — what this used to claim and did not do. The comment said "the follower is
// healthy … so its IsLeader() is a stable false", and the loop below broke out of its poll after ONE
// instantaneous pair of IsLeader() observations. Neither the stability nor the follower's KNOWLEDGE
// OF THE LEADER was established, and both callers' assertions depend on the latter: the generic
// mutating-verb gate answers "no leader (election in progress); retry" whenever LeaderWithID() is
// empty. Under `make e2e-parallel` load, with many raft fixtures competing for CPU, that window is
// wide enough to hit — the internal review diagnosed it (plan §16.3) and left it unfixed, which is
// what this finding is.
//
// The fix has two halves, because either alone is still a snapshot:
//
//  1. here: require the split AND a known leader id to hold across consecutive observations, so the
//     fixture's own claim is true when it returns;
//  2. at the assertion: re-derive the state FROM THE RESPONSE and retry through the legitimate
//     leaderless window (followerResponse below). testing-standards §T3 — an observation of a
//     distributed state does not survive to the next step, so the only durable check is one made on
//     the same value being asserted.
func rotate2NodeFollower(t *testing.T) (fb *clusterAdminBackend, followerID, leaderID string) {
	t.Helper()
	ids := []raft.ServerID{"brk-a", "brk-b"}
	addrs := make([]raft.ServerAddress, len(ids))
	transports := make([]*raft.InmemTransport, len(ids))
	servers := make([]raft.Server, len(ids))
	for i, id := range ids {
		addr, tr := raft.NewInmemTransport("") // auto address — no cross-test collision
		addrs[i], transports[i] = addr, tr
		servers[i] = raft.Server{Suffrage: raft.Voter, ID: id, Address: addr}
	}
	// Fully connect every transport to every peer (two-way over both (i,j) and (j,i)).
	for i := range transports {
		for j := range transports {
			if i != j {
				transports[i].Connect(addrs[j], transports[j])
			}
		}
	}
	nodes := make([]*cluster.Node, len(ids))
	for i, id := range ids {
		dir := t.TempDir()
		n, err := cluster.New(cluster.Config{
			LocalID:            id,
			DataDir:            dir,
			DBPath:             filepath.Join(dir, "state.db"),
			Transport:          transports[i],
			BootstrapPeers:     servers,
			HeartbeatTimeout:   cluster.MultinodeHeartbeatTimeout,
			ElectionTimeout:    cluster.MultinodeElectionTimeout,
			LeaderLeaseTimeout: cluster.MultinodeLeaderLeaseTimeout,
			ApplyTimeout:       5 * time.Second,
		})
		if err != nil {
			t.Fatalf("new node %s: %v", id, err)
		}
		t.Cleanup(func() { _ = n.Shutdown() })
		nodes[i] = n
	}
	// Poll for a leader/follower split that is STABLE and in which the follower KNOWS the leader —
	// the precondition both callers assert against. "Stable" means the same split observed on
	// consecutive polls: a single observation can be the instant before an election completes or the
	// instant after one starts.
	const wantConsecutive = 3
	var follower, leader *cluster.Node
	streak := 0
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		var f, l *cluster.Node
		switch {
		case nodes[0].IsLeader() && !nodes[1].IsLeader():
			l, f = nodes[0], nodes[1]
		case nodes[1].IsLeader() && !nodes[0].IsLeader():
			l, f = nodes[1], nodes[0]
		}
		// The follower must also be able to NAME the leader, or the product's (correct) answer is
		// "no leader (election in progress)" rather than the leader-host redirect under test.
		if f != nil {
			if _, leaderID := f.LeaderWithID(); leaderID == "" {
				f, l = nil, nil
			}
		}
		if f != nil && f == follower && l == leader {
			streak++
		} else {
			streak = 1
		}
		follower, leader = f, l
		if follower != nil && streak >= wantConsecutive {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if follower == nil || streak < wantConsecutive {
		t.Fatalf("no leader/follower split held for %d consecutive observations with a known leader "+
			"id (streak=%d) — the two-node cluster never settled, so neither caller's precondition "+
			"was ever true", wantConsecutive, streak)
	}
	admin := NewClusterAdmin(follower, nil)
	return &clusterAdminBackend{admin: admin}, follower.SelfID(), leader.SelfID()
}

// followerResponse issues req against the follower backend and returns the first response that
// reflects the state the caller's assertion presupposes, as decided by accept.
//
// It exists because the fixture's split is a SNAPSHOT no matter how carefully it is taken (external
// review B4). Leadership can move between the fixture returning and HandleCluster running, and it can
// move again before the next line. So the state is derived from the RESPONSE ITSELF — the same value
// being asserted — rather than from a second observation of the node, which would only add a second
// race.
//
// transient names the responses that are the product being CORRECT about a state other than the one
// under test; each is retried, and each is validated on the way past so a genuine regression cannot
// hide inside the retry loop. Anything neither accepted nor transient fails immediately: that is a
// real wrong answer, not a timing window.
func followerResponse(t *testing.T, fb *clusterAdminBackend, req adminsock.Request,
	accept func(adminsock.Response) bool, transient func(adminsock.Response) string) adminsock.Response {
	t.Helper()
	return retryUntilStateUnderTest(t, func() adminsock.Response { return fb.HandleCluster(req) }, accept, transient)
}

// retryUntilStateUnderTest is followerResponse's loop with the request source injected, so the loop
// itself is unit-testable (TestRetryUntilStateUnderTest). Without that, the only evidence the retry
// works would be "the flaky test stopped failing", which is indistinguishable from luck.
func retryUntilStateUnderTest(t *testing.T, do func() adminsock.Response,
	accept func(adminsock.Response) bool, transient func(adminsock.Response) string) adminsock.Response {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	var last adminsock.Response
	var lastWhy string
	for {
		last = do()
		if accept(last) {
			return last
		}
		why := transient(last)
		if why == "" {
			return last // not a known transient state — hand it back so the caller's own assertion reports it
		}
		lastWhy = why
		if time.Now().After(deadline) {
			t.Fatalf("the two-node cluster never reached the state under test within the deadline; "+
				"last response was the legitimate transient %q (NotLeader=%v err=%q). The product is "+
				"answering correctly for the state it is in — the fixture could not hold the "+
				"precondition.", lastWhy, last.NotLeader, last.Error)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// TestRetryUntilStateUnderTest pins the B4 retry machinery deterministically. The two fixture tests
// above cannot prove it works: under isolation the leaderless window they retry through hardly ever
// occurs, so they would pass with the retry removed — which is exactly the trap that let the original
// snapshot fixture look healthy for as long as it did.
func TestRetryUntilStateUnderTest(t *testing.T) {
	leaderless := adminsock.Response{NotLeader: true, Code: adminsock.CodeNotLeader,
		Error: "no leader (election in progress); retry"}
	redirect := adminsock.Response{NotLeader: true, Code: adminsock.CodeNotLeader,
		LeaderHost: "brk-a", Error: "not leader; re-run on the leader host brk-a"}

	acceptRedirect := func(r adminsock.Response) bool {
		return r.NotLeader && strings.Contains(r.Error, "re-run on the leader host")
	}

	t.Run("retries through the leaderless window and returns the state under test", func(t *testing.T) {
		script := []adminsock.Response{leaderless, leaderless, redirect}
		i := 0
		got := retryUntilStateUnderTest(t, func() adminsock.Response {
			r := script[i]
			i++
			return r
		}, acceptRedirect, func(r adminsock.Response) string { return leaderlessIsWellFormed(t, r) })
		if !acceptRedirect(got) {
			t.Fatalf("did not retry to the accepted response, got %+v after %d calls", got, i)
		}
		if i != len(script) {
			t.Fatalf("consumed %d of %d scripted responses — the loop is not retrying", i, len(script))
		}
	})

	t.Run("a response that is neither accepted nor transient is handed straight back", func(t *testing.T) {
		wrong := adminsock.Response{Error: "some other refusal"}
		got := retryUntilStateUnderTest(t, func() adminsock.Response { return wrong },
			acceptRedirect, func(r adminsock.Response) string { return leaderlessIsWellFormed(t, r) })
		if got.Error != wrong.Error {
			t.Fatalf("a genuine wrong answer must be returned for the caller's own assertion to "+
				"report, not retried until the deadline; got %+v", got)
		}
	})

	// And the leaderless validator must be a real check, not a rubber stamp: the retry loop is the
	// one place a malformed leaderless answer could slip past unexamined.
	t.Run("the leaderless validator rejects a malformed leaderless answer", func(t *testing.T) {
		for _, bad := range []adminsock.Response{
			{Error: "no leader (election in progress); retry"},                                                               // NotLeader unset
			{NotLeader: true, Error: "no leader (election in progress); retry"},                                              // Code unset
			{NotLeader: true, Code: adminsock.CodeNotLeader, LeaderHost: "brk-a", Error: "no leader (election in progress)"}, // names a leader that does not exist
			{NotLeader: true, Code: adminsock.CodeNotLeader, OK: true, Error: "no leader (election in progress)"},            // reports success
		} {
			if leaderlessProblem(bad) == "" {
				t.Errorf("the validator accepted a malformed leaderless response, so a genuine "+
					"regression in the leaderless answer could hide inside the retry loop: %+v", bad)
			}
		}
		if p := leaderlessProblem(leaderless); p != "" {
			t.Errorf("the validator rejected the WELL-FORMED leaderless response (%s) — it would "+
				"turn every legitimate election window into a hard failure", p)
		}
	})
}

// leaderlessIsWellFormed validates the response the generic gate gives while an election is in
// flight. It is the "separately validate the legitimate leaderless response" half of the B4 fix: the
// retry loop must not become a way for a malformed leaderless answer to pass unnoticed.
func leaderlessIsWellFormed(t *testing.T, resp adminsock.Response) string {
	t.Helper()
	if !strings.Contains(resp.Error, "no leader (election in progress)") {
		return ""
	}
	if p := leaderlessProblem(resp); p != "" {
		t.Fatalf("the leaderless answer the retry loop was about to skip past is malformed: %s (%+v)", p, resp)
	}
	return "no leader (election in progress)"
}

// leaderlessProblem describes what is wrong with a leaderless answer, or "" when it is well-formed.
// Split out from leaderlessIsWellFormed so the validation is a pure function the tests can drive
// directly — a validator wired only into a rarely-taken retry branch is a validator nobody has run.
func leaderlessProblem(resp adminsock.Response) string {
	switch {
	case !resp.NotLeader:
		return "NotLeader is unset — a ctl branches on it to decide retry vs. hard failure"
	case resp.Code != adminsock.CodeNotLeader:
		return "Code is " + resp.Code + ", want " + adminsock.CodeNotLeader
	case resp.LeaderHost != "":
		return "it names leader host " + resp.LeaderHost + " while reporting that there is no leader"
	case resp.OK:
		return "it reports OK for a mutating verb that was refused"
	}
	return ""
}

// TestRotateCertOnFollowerGivesSelfOnlyGuidance (P11/#56) — the load-bearing test: OpClusterRotateCrt
// dispatched to a FOLLOWER must reach RotateTunnelCert and return the self-only guidance, NOT the
// generic leader-redirect.
func TestRotateCertOnFollowerGivesSelfOnlyGuidance(t *testing.T) {
	fb, followerID, _ := rotate2NodeFollower(t)

	// The state under test is "this node is a FOLLOWER". The one transient that is the product being
	// correct about a different state is leadership having moved ONTO this node, where rotate legally
	// succeeds — retry through it rather than asserting on it.
	resp := followerResponse(t, fb,
		adminsock.Request{Op: adminsock.OpClusterRotateCrt, NodeID: followerID, CertFP: "sha256:NEW"},
		func(r adminsock.Response) bool { return !r.OK && strings.Contains(r.Error, "FOLLOWER") },
		func(r adminsock.Response) string {
			if r.OK {
				return "leadership moved onto the node under test (rotate legally succeeds there)"
			}
			return leaderlessIsWellFormed(t, r)
		})
	if resp.OK {
		t.Fatalf("rotate-tunnel-cert on a follower must be refused, got OK")
	}
	// It MUST NOT be routed through the generic mutating-verb redirect.
	if resp.NotLeader {
		t.Fatalf("self-only rotate-cert must BYPASS the generic leader-redirect; got NotLeader=true err=%q", resp.Error)
	}
	if strings.Contains(resp.Error, "re-run on the leader host") {
		t.Fatalf("follower rotate-cert must NOT tell the operator to re-run on the leader host (that dead-ends #56); got %q", resp.Error)
	}
	// It MUST give the self-only "make THIS node the leader" guidance.
	if !strings.Contains(resp.Error, "FOLLOWER") || !strings.Contains(resp.Error, "transfer") {
		t.Fatalf("follower rotate-cert must give self-only guidance (transfer leadership to the target itself); got %q", resp.Error)
	}
	if !strings.Contains(resp.Error, followerID) {
		t.Fatalf("guidance must name the target broker %q; got %q", followerID, resp.Error)
	}
}

// TestGenericMutatingVerbStillRedirectsOnFollower is the CONTRAST / mutation guard: a DIFFERENT
// mutating verb (drain) on the SAME follower must STILL get the generic "re-run on the leader host"
// redirect. This proves the #56 bypass is SPECIFIC to the self-only verb and did not disable the
// generic gate for every verb (which would silently break every other command's leader-redirect UX).
func TestGenericMutatingVerbStillRedirectsOnFollower(t *testing.T) {
	fb, followerID, _ := rotate2NodeFollower(t)

	// The state under test is "follower, and the leader is known". Two transients are the product
	// being correct about something else: an in-flight election (leaderless), and leadership having
	// moved onto this node (drain then proceeds and NotLeader is legitimately false).
	resp := followerResponse(t, fb,
		adminsock.Request{Op: adminsock.OpClusterDrain, NodeID: followerID},
		func(r adminsock.Response) bool {
			return r.NotLeader && strings.Contains(r.Error, "re-run on the leader host")
		},
		func(r adminsock.Response) string {
			if !r.NotLeader {
				return "leadership moved onto the node under test (drain is no longer redirected)"
			}
			return leaderlessIsWellFormed(t, r)
		})
	if !resp.NotLeader {
		t.Fatalf("a generic mutating verb (drain) on a follower must STILL redirect to the leader; got NotLeader=false err=%q", resp.Error)
	}
	if !strings.Contains(resp.Error, "re-run on the leader host") {
		t.Fatalf("drain on a follower must give the generic leader-redirect; got %q", resp.Error)
	}
}

// TestRotateTunnelCertGuidanceNeverPointsAtLeaderHost pins BOTH RotateTunnelCert failure modes'
// wording at the unit level (no raft needed for the wrong-host case): neither may say "re-run on the
// leader host", and both must point at making the TARGET itself the leader.
func TestRotateTunnelCertGuidanceNeverPointsAtLeaderHost(t *testing.T) {
	// Wrong-host: a single-node LEADER asked to rotate a DIFFERENT node's cert.
	n, addr := d7SingleNode(t, "single-1")
	admin := NewClusterAdmin(n, nil)
	in := d7JoinInput(t, "single-1", addr)
	caughtUp := func(b uint64) (bool, error) { c, e := n.AppliedIndex(); return c >= b, e }
	if err := admin.AddNode(in, addr, caughtUp, 5*time.Second); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	err := admin.RotateTunnelCert("other-1", "sha256:NEW", time.Hour)
	if err == nil {
		t.Fatal("rotating a remote target must be refused")
	}
	if strings.Contains(err.Error(), "re-run on the leader host") {
		t.Fatalf("wrong-host guidance must not point at the leader host; got %q", err)
	}
	if !strings.Contains(err.Error(), "transfer") || !strings.Contains(err.Error(), "other-1") {
		t.Fatalf("wrong-host guidance must say run it ON the target (other-1) and make it leader; got %q", err)
	}
}
