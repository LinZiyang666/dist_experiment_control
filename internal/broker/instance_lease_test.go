package broker

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/node"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
)

// leaseBroker builds the smallest Broker that adjudicateLease needs.
//
// It deliberately leaves b.nc nil. probeNameInUse then reports "in use", which
// is the FAIL-SAFE direction, so these tests exercise the adjudication logic
// with the probe pinned to its conservative answer. The probe's own behaviour
// (ErrNoResponders ⇒ grant) needs a real bus and is covered separately.
func leaseBroker(t *testing.T) *Broker {
	t.Helper()
	db := testharness.OpenDB(t)
	if _, err := db.Exec(
		`INSERT INTO sessions(sid, name, owner_pubkey_fp, pin_hash) VALUES (?,?,?,?)`,
		"lab", "lab", "SHA256:test-owner", "test-hash"); err != nil {
		t.Fatalf("seed session: %v", err)
	}
	return &Broker{cfg: Config{DB: db, Logger: testharness.SilentLog()}}
}

func TestClusterRegisterAtomicallyRefilesAdoptedProcesses(t *testing.T) {
	b := leaseBroker(t)
	seedBeat(t, b, "gpu1", 0)
	if _, err := b.cfg.DB.Exec(
		`INSERT INTO processes(pid,sid,nid,argv,started_at,status,started_by_fp)
		 VALUES('p1','lab','gpu1','["train"]',?,'RUNNING','SHA256:u')`, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	in := node.RegisterInput{SID: "lab", NID: "gpu1-02", ProtoVersion: proto.ProtoVersion}
	cmd, err := planRegisterWithRefiles(b.cfg.DB, in, "gpu1",
		[]proto.LocalProcess{{PID: "p1", State: "running"}}, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if len(cmd.Body) != 2 {
		t.Fatalf("cluster register has %d statements, want identity + refile", len(cmd.Body))
	}
	if err := cluster.ExecCommand(b.cfg.DB, cmd); err != nil {
		t.Fatal(err)
	}
	var nid string
	if err := b.cfg.DB.QueryRow(`SELECT nid FROM processes WHERE pid='p1'`).Scan(&nid); err != nil {
		t.Fatal(err)
	}
	if nid != "gpu1-02" {
		t.Fatalf("committed cluster register left p1 under %s", nid)
	}
}

func TestCloneFamilyCannotEnterAPerInstanceRemoteUpgrade(t *testing.T) {
	b := leaseBroker(t)
	for _, nid := range []string{"gpu1", "gpu1-02"} {
		seedBeat(t, b, nid, 0)
	}
	if _, err := b.cfg.DB.Exec(
		`INSERT INTO agent_provisioning(sid,nid,agent_fp,joined_at) VALUES('lab','gpu1','SHA256:image',datetime('now'))`); err != nil {
		t.Fatal(err)
	}
	for _, target := range []string{"gpu1", "gpu1-02"} {
		if !cloneFamilyUpgradeConflict(b, "lab", target) {
			t.Fatalf("clone-family target %s was allowed to mutate a potentially shared binary", target)
		}
	}
	if cloneFamilyUpgradeConflict(b, "lab", "fixed-device") {
		t.Fatal("an unrelated fixed device was rejected")
	}

	// A broker without auth-callout bindings cannot prove whether a shaped
	// row is a lease. Replacing an executable is the destructive direction, so
	// upgrade must conservatively refuse both the row and its possible root.
	unbound := leaseBroker(t)
	for _, nid := range []string{"cpu1", "cpu1-02"} {
		seedBeat(t, unbound, nid, 0)
	}
	for _, target := range []string{"cpu1", "cpu1-02"} {
		if !cloneFamilyUpgradeConflict(unbound, "lab", target) {
			t.Fatalf("unbound clone-shaped target %s was allowed to mutate an unproved upgrade domain", target)
		}
	}
}

func seedBeat(t *testing.T, b *Broker, nid string, age time.Duration) {
	t.Helper()
	if _, err := b.cfg.DB.Exec(
		`INSERT INTO nodes(sid, nid, status, last_heartbeat_at) VALUES (?,?,?,?)`,
		"lab", nid, "ONLINE", time.Now().UTC().Add(-age)); err != nil {
		t.Fatalf("seed beat for %q: %v", nid, err)
	}
}

const testInstanceA = "aaaaaaaaaaaaaaaaaaaaaaaaaa"
const testInstanceB = "bbbbbbbbbbbbbbbbbbbbbbbbbb"
const testInstanceC = "cccccccccccccccccccccccccc"

// adjudicated drives adjudicateLease to a DECISION the way a real agent does.
//
// The interest probe runs off the register handler — inline it would serialize
// every other node's register behind it, and its budget would have to be too
// small for an intercontinental round trip. So a register that needs a probe is
// answered with a transient code and the agent retries; this helper is that
// retry loop, and calling adjudicateLease once in a test would only ever see
// the transient.
func adjudicated(t *testing.T, b *Broker, sid, nid string, req *proto.NodeRegisterReq) (*proto.NodeLease, string, error) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		lease, code, err := adjudicateLease(b, sid, nid, req, time.Now().UTC())
		if code != leaseReasonProbePending {
			return lease, code, err
		}
		if time.Now().After(deadline) {
			t.Fatalf("adjudication never left %q for (%s,%s)", leaseReasonProbePending, sid, nid)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// origin: docs/reviews/cloned-credential-instances-plan.md §6 N-1
//
// An agent that predates this feature advertises no instance id, and MUST get
// exactly today's behaviour — including the clone fan-out, which is the
// pre-upgrade baseline rather than a regression. If contest could fire without
// both ids, a new broker would start renaming an un-upgraded fleet.
//
// MUTATION: drop the `req.InstanceID == ""` early return and this goes red.
func TestAdjudicateLeaseNeverFiresForAPreFeatureAgent(t *testing.T) {
	b := leaseBroker(t)
	seedBeat(t, b, "gpu1", 0) // a live holder exists…
	lease, code, err := adjudicated(t, b, "lab", "gpu1", &proto.NodeRegisterReq{})
	if lease != nil || code != "" || err != nil {
		t.Fatalf("a register with no instance id must be uncontested; got lease=%v code=%q err=%v",
			lease, code, err)
	}
}

// origin: docs/reviews/cloned-credential-instances-plan.md §2 Q5
//
// TAKE-OVER IS THE DEFAULT. A name with no live holder — no row at all, or a
// holder silent past LeaseGrace — is granted outright, which is what
// node.Register's existing upsert already does. This is the path every ordinary
// agent restart takes, and it must stay byte-identical to today.
func TestAdjudicateLeaseGrantsTheNameWhenNoLiveHolderExists(t *testing.T) {
	for _, c := range []struct {
		name string
		seed func(*Broker)
	}{
		{"no row at all", func(*Broker) {}},
		{"holder silent past the grace window", func(b *Broker) { seedBeat(t, b, "gpu1", time.Minute) }},
	} {
		t.Run(c.name, func(t *testing.T) {
			// A real bus with NO agent subscribed: the stale-beat arm now PROBES
			// before granting (a heartbeat clock cannot tell a dead predecessor
			// from a gotcha #72 one whose socket and subscriptions are still
			// live), and with nobody subscribed the probe returns
			// ErrNoResponders instantly — which is the take-over evidence.
			b, _, _ := leaseBrokerWithBus(t)
			c.seed(b)
			lease, code, err := adjudicated(t, b, "lab", "gpu1", &proto.NodeRegisterReq{InstanceID: testInstanceA})
			if lease != nil || code != "" || err != nil {
				t.Fatalf("expected an uncontested grant; got lease=%v code=%q err=%v", lease, code, err)
			}
		})
	}
}

// origin: docs/reviews/cloned-credential-instances-plan.md §3.2
//
// CLAIM IDEMPOTENCY. Agents re-register on every NATS reconnect. Without
// matching on the instance id, each reconnect would burn a fresh suffix
// (gpu1 → gpu1-02 → gpu1-03 …) while the SAME live process still held the
// previous ones.
//
// MUTATION: remove the leaseHolder fast-path comparison and this goes red.
func TestAdjudicateLeaseIsIdempotentAcrossReconnectsOfTheSameInstance(t *testing.T) {
	b := leaseBroker(t)
	req := &proto.NodeRegisterReq{InstanceID: testInstanceA}

	// First register claims the name.
	if lease, code, err := adjudicateLease(b, "lab", "gpu1", req, time.Now().UTC()); lease != nil || code != "" || err != nil {
		t.Fatalf("first register: lease=%v code=%q err=%v", lease, code, err)
	}
	// A fresh beat now exists and would look "contested" to anyone else.
	seedBeat(t, b, "gpu1", 0)

	// Twenty reconnects must all return the same (unsuffixed) name.
	for i := 0; i < 20; i++ {
		lease, code, err := adjudicateLease(b, "lab", "gpu1", req, time.Now().UTC())
		if lease != nil || code != "" || err != nil {
			t.Fatalf("reconnect %d was treated as contested (lease=%v code=%q err=%v); "+
				"every reconnect would burn a new suffix", i, lease, code, err)
		}
	}
}

// origin: docs/reviews/cloned-credential-instances-plan.md §3.2, §0.1 E2
//
// THE HEADLINE CASE: a second process presenting the same baked credential,
// while the incumbent is live, must be given a different name — otherwise both
// subscribe to one forwarded subject and every command runs twice.
//
// MUTATION: make contest require a recorded in-memory holder (`holder != ""`)
// and this still passes here but breaks after a broker restart — which is why
// TestAdjudicateLeaseContestsFromDurableStateAfterARestart exists alongside it.
func TestAdjudicateLeaseSuffixesASecondLiveInstance(t *testing.T) {
	b := leaseBroker(t)
	// Incumbent registers and is beating.
	if _, _, err := adjudicated(t, b, "lab", "gpu1", &proto.NodeRegisterReq{InstanceID: testInstanceA}); err != nil {
		t.Fatalf("incumbent register: %v", err)
	}
	seedBeat(t, b, "gpu1", 0)

	lease, code, err := adjudicated(t, b, "lab", "gpu1", &proto.NodeRegisterReq{InstanceID: testInstanceB})
	if err != nil || code != "" {
		t.Fatalf("challenger: code=%q err=%v", code, err)
	}
	if lease == nil {
		t.Fatal("a second live instance must be contested; got no lease, which means both would " +
			"register under one name and every command would execute twice")
	}
	if lease.AssignedNID != "gpu1-02" || lease.Basename != "gpu1" {
		t.Fatalf("got %+v, want AssignedNID=gpu1-02 Basename=gpu1", lease)
	}
}

// origin: docs/reviews/cloned-credential-instances-plan.md §3.2 (completeness B1)
//
// THE COLD-REGISTRY HOLE. The in-memory holder map is empty after a broker
// restart or a leader election. A contest rule that required a recorded holder
// would read as UNCONTESTED for both live clones and hand them the same name in
// sequence — restoring the fan-out with no clone arrival and no death involved.
// Contest is therefore driven by nodes.last_heartbeat_at, which survives both.
//
// MUTATION: add `holder != ""` to the contest condition and this goes red.
func TestAdjudicateLeaseContestsFromDurableStateAfterARestart(t *testing.T) {
	b := leaseBroker(t)
	// A live holder exists in SQLite, but the broker has NO memory of it —
	// exactly the state after a restart or a leadership change.
	seedBeat(t, b, "gpu1", 0)

	lease, code, err := adjudicated(t, b, "lab", "gpu1", &proto.NodeRegisterReq{InstanceID: testInstanceB})
	if err != nil || code != "" {
		t.Fatalf("code=%q err=%v", code, err)
	}
	if lease == nil {
		t.Fatal("with an empty in-memory registry the adjudicator must still contest from " +
			"nodes.last_heartbeat_at; otherwise a broker restart silently restores the fan-out")
	}
}

// origin: docs/reviews/cloned-credential-instances-plan.md §2 D7
//
// REFUSE — do NOT degrade — once a different live holder has been PROVEN.
//
// The first version of this test pinned the opposite ("degrade, never refuse"),
// and internal review showed that rule is destructive exactly here: the probe
// has already established that another live process holds the name, so falling
// through to registerNode would overwrite the incumbent's row, clear its
// proxy_ready and close every process it is running. Degrading is the safe
// answer when the broker does NOT know there is an incumbent; it is the
// destructive one when it does.
//
// The contract this now pins: a non-nil lease with an EMPTY AssignedNID — a
// refusal the challenger can act on, which still short-circuits handleRegister
// before it touches anything — plus a code so the operator learns why.
func TestAdjudicateLeaseRefusesWhenAProvenHolderExistsAndNoNameCanBeIssued(t *testing.T) {
	b := leaseBroker(t)
	long := strings.Repeat("b", proto.MaxLeaseBasenameLen+1)
	seedBeat(t, b, long, 0)

	lease, code, _ := adjudicated(t, b, "lab", long, &proto.NodeRegisterReq{InstanceID: testInstanceB})
	if lease == nil {
		t.Fatal("a proven live holder plus no issuable name must REFUSE, not fall through to " +
			"registerNode — falling through overwrites the incumbent's row and closes its processes")
	}
	if lease.AssignedNID != "" {
		t.Fatalf("no name could be issued, so AssignedNID must be empty; got %q", lease.AssignedNID)
	}
	if code != leaseReasonUnavailable {
		t.Fatalf("got code %q, want %q so the operator learns why", code, leaseReasonUnavailable)
	}
}

// origin: docs/reviews/cloned-credential-instances-plan.md §2 D2
//
// The instance id is client-supplied, so a malformed one is rejected
// fail-closed before it can reach a decision — but as a DEGRADE (grant the
// presented name and log), never as a register refusal.
func TestAdjudicateLeaseRejectsAMalformedInstanceIDWithoutRefusingTheRegister(t *testing.T) {
	b := leaseBroker(t)
	for _, bad := range []string{"short", strings.Repeat("A", 26), strings.Repeat("a", 25) + "*"} {
		lease, code, err := adjudicated(t, b, "lab", "gpu1", &proto.NodeRegisterReq{InstanceID: bad})
		if lease != nil {
			t.Fatalf("malformed id %q produced an assignment %+v", bad, lease)
		}
		if code != leaseReasonBadInstanceID || err == nil {
			t.Fatalf("malformed id %q: code=%q err=%v, want %q and a non-nil error",
				bad, code, err, leaseReasonBadInstanceID)
		}
	}
}

// origin: docs/reviews/cloned-credential-instances-review.md §3d — the p4
// regression (TestAgentReconnectsWithoutPINAfterBootstrap went STALE).
//
// THE RESTART-VS-CLONE AMBIGUITY, and why the farewell exists.
//
// A quick restart and a simultaneous second clone are IDENTICAL from the
// broker's side: warm heartbeat, a holder granted moments ago, and — until the
// server reaps the dead process's subscription — live interest that nobody
// answers. The grant window resolves that tie toward "clone", which is right
// for clones and WRONG for a restart: the device gets renamed for restarting
// and its bare name goes STALE while the user keeps addressing it.
//
// A cleanly stopping agent therefore hands the name back, and its successor is
// adjudicated as a first arrival.
//
// MUTATION: drop the leaseHolder.Delete in the ReleasingName arm of
// replyLeaseVerdict and this goes red — the successor is suffixed.
func TestAGracefulFarewellLetsTheNextProcessKeepTheBareName(t *testing.T) {
	b, _, _ := leaseBrokerWithBus(t)

	// The incumbent claims the name.
	if lease, code, err := adjudicated(t, b, "lab", "gpu1",
		&proto.NodeRegisterReq{InstanceID: testInstanceA}); lease != nil || code != "" || err != nil {
		t.Fatalf("incumbent: lease=%v code=%q err=%v", lease, code, err)
	}
	seedBeat(t, b, "gpu1", 0)

	// It stops cleanly and says so.
	releaseName(t, b, "lab", "gpu1", testInstanceA)

	// The successor — a NEW process, so a new instance id, arriving well inside
	// the grant window, which without the farewell is exactly the shape that
	// gets suffixed.
	lease, code, err := adjudicated(t, b, "lab", "gpu1", &proto.NodeRegisterReq{InstanceID: testInstanceB})
	if err != nil || code != "" {
		t.Fatalf("successor: code=%q err=%v", code, err)
	}
	if lease != nil {
		t.Fatalf("a restart was renamed to %q. The predecessor released the name, so this is a "+
			"SUCCESSOR, not a clone: the device keeps its identity and the operator keeps "+
			"addressing it the way they always did.", lease.AssignedNID)
	}
}

// origin: docs/reviews/cloned-credential-instances-review.md §3d
//
// A farewell is an ASSERTION FROM THE NETWORK, so it must not be usable to take
// a name away from the process that currently holds it. Only the current holder
// can release; a straggler clone saying goodbye must change nothing.
//
// MUTATION: drop the `g.instanceID == req.InstanceID` guard and this goes red.
func TestAStragglerFarewellCannotReleaseTheNameItsSiblingHolds(t *testing.T) {
	b, _, _ := leaseBrokerWithBus(t)

	if _, _, err := adjudicated(t, b, "lab", "gpu1", &proto.NodeRegisterReq{InstanceID: testInstanceA}); err != nil {
		t.Fatalf("holder register: %v", err)
	}
	seedBeat(t, b, "gpu1", 0)

	// A DIFFERENT instance — one that was suffixed earlier, or a forgery — says
	// goodbye on the holder's name.
	releaseName(t, b, "lab", "gpu1", testInstanceB)

	// Assert the HOLDER ENTRY directly.
	//
	// An earlier version of this test asserted only through adjudication
	// outcomes, and it survived the mutation it was written to catch: with the
	// ownership guard removed the stranger DID evict the holder, but the very
	// next adjudication re-granted the name to A (nobody is subscribed, so the
	// probe reports the name free) and re-stamped a fresh grant, which then
	// suffixed the third process through the grant window. Every observable the
	// test looked at came out the same as if the guard were intact. The state
	// the guard actually protects is this map entry, so read it.
	v, ok := b.leaseHolder.Load(leaseKey("lab", "gpu1"))
	if !ok {
		t.Fatal("a stranger's farewell evicted the holder entry: any party that can publish a " +
			"register can now discard a live agent's claim by announcing ITS OWN departure")
	}
	if g, _ := v.(leaseGrant); g.instanceID != testInstanceA {
		t.Fatalf("the holder entry now names %q; only the CURRENT holder may release a name",
			g.instanceID)
	}
	// And the holder's own reconnect is still idempotent.
	if lease, code, err := adjudicated(t, b, "lab", "gpu1",
		&proto.NodeRegisterReq{InstanceID: testInstanceA}); lease != nil || code != "" || err != nil {
		t.Fatalf("the real holder lost its claim to a stranger's farewell: lease=%v code=%q err=%v",
			lease, code, err)
	}
	// ...and a third process is still contested rather than being handed the name.
	lease, _, _ := adjudicated(t, b, "lab", "gpu1", &proto.NodeRegisterReq{InstanceID: testInstanceC})
	if lease == nil {
		t.Fatal("a stranger's farewell freed the name: any party that can publish a register can " +
			"now displace a live agent by claiming to be leaving")
	}
}

// releaseName drives the farewell arm of replyLeaseVerdict the way a stopping
// agent does (internal/agent, releaseLeaseName).
func releaseName(t *testing.T, b *Broker, sid, nid, iid string) {
	t.Helper()
	body, err := json.Marshal(proto.NodeRegisterReq{
		ProtoVersion: proto.ProtoVersion, NID: nid, InstanceID: iid, ReleasingName: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !replyLeaseVerdict(b, &nats.Msg{Subject: proto.SubjNodeRegister(sid, nid), Data: body},
		sid, nid, &proto.NodeRegisterReq{
			ProtoVersion: proto.ProtoVersion, NID: nid, InstanceID: iid, ReleasingName: true,
		}) {
		t.Fatal("a farewell must be handled and stop there, not fall through to the ordinary " +
			"register — falling through stamps a FRESH heartbeat for a process that is leaving")
	}
}

// origin: docs/reviews/cloned-credential-instances-external-review.md F5
//
// THE CO-LOCATED AGENT IS NEVER LEASED — a safety interlock, not a courtesy.
//
// A cluster broker upgrade drives its own host's agent by publishing to the
// STATIC nid the broker was configured with. Suffix that agent and the upgrade
// leg is addressed to a name nobody is subscribed to: the host stops with the
// broker on the new binary and the agent on the old, and behind NAT there is no
// remote path out of that state.
//
// MUTATION: drop the ColocatedAgentNID arm in adjudicateLease and this goes red.
func TestTheColocatedAgentIsExemptFromLeaseAdjudication(t *testing.T) {
	b := leaseBroker(t)
	b.cfg.ColocatedAgentNID = "brk1-agent"
	// Everything about this register SCREAMS contest: a live incumbent row, a
	// different instance id, well inside the grace window.
	seedBeat(t, b, "brk1-agent", 0)
	if _, _, err := adjudicated(t, b, "lab", "brk1-agent",
		&proto.NodeRegisterReq{InstanceID: testInstanceA}); err != nil {
		t.Fatalf("incumbent: %v", err)
	}
	lease, code, err := adjudicated(t, b, "lab", "brk1-agent",
		&proto.NodeRegisterReq{InstanceID: testInstanceB})
	if err != nil || code != "" {
		t.Fatalf("code=%q err=%v", code, err)
	}
	if lease != nil {
		t.Fatalf("the co-located agent was assigned %q. The broker's own upgrade trigger publishes "+
			"to the STATIC configured nid, so this host will stop half-upgraded — broker new, agent "+
			"old — with no remote recovery path.", lease.AssignedNID)
	}
	// A DIFFERENT node on the same broker is still adjudicated normally: the
	// exemption is one name, not a switch that disables the feature.
	seedBeat(t, b, "gpu1", 0)
	if _, _, err := adjudicated(t, b, "lab", "gpu1", &proto.NodeRegisterReq{InstanceID: testInstanceA}); err != nil {
		t.Fatalf("gpu1 incumbent: %v", err)
	}
	if l, _, _ := adjudicated(t, b, "lab", "gpu1", &proto.NodeRegisterReq{InstanceID: testInstanceC}); l == nil {
		t.Fatal("exempting the co-located agent must not exempt every other node on that broker")
	}
}

// origin: docs/reviews/cloned-credential-instances-external-review.md F11
//
// A RELEASED LEASE NAME IS FREE AT ONCE — the restarting instance gets its own
// name back rather than the next suffix.
//
// Clearing only the in-memory grant left the nodes row ONLINE for the whole
// OfflineAfter window, and the allocator reads that row. So an instance that
// said goodbye and came back was issued the NEXT suffix, and the one after that
// the one after — -02, -03, -04 on every bounce — while the operator's saved
// commands, exposes and scripts kept pointing at ghosts that still showed
// ONLINE for a minute.
//
// MUTATION: drop the markNodeOfflineOnRelease call in the farewell arm and this
// goes red with gpu1-03.
func TestAReleasedLeaseNameIsImmediatelyReusableByItsOwnRestart(t *testing.T) {
	b, _, url := leaseBrokerWithBus(t)
	// The incumbent holds the basename and is LIVE — subscribed and answering
	// claim-probe — so the restart below is genuinely contested rather than
	// simply inheriting a free name.
	seedBeat(t, b, "gpu1", 0)
	seedBeat(t, b, "gpu1-02", 0)
	subscribeClaimProbeAs(t, url, "lab", "gpu1", testInstanceA)

	// The leased clone stops cleanly, naming the lease it actually holds.
	releaseLeasedName(t, b, "lab", "gpu1-02", testInstanceB)

	// It restarts — a NEW process, so a new instance id — and contests again.
	lease, code, err := adjudicated(t, b, "lab", "gpu1", &proto.NodeRegisterReq{
		InstanceID: testInstanceC, ConfiguredNID: "gpu1"})
	if err != nil || code != "" {
		t.Fatalf("code=%q err=%v", code, err)
	}
	if lease == nil {
		t.Fatal("the restart should still be contested — the basename holder is live")
	}
	if lease.AssignedNID != "gpu1-02" {
		t.Fatalf("the restarting instance was issued %q instead of reclaiming the name it just "+
			"released. Every bounce walks another suffix, addresses the operator saved go stale, "+
			"and the suffix space eventually runs out.", lease.AssignedNID)
	}
}

// origin: docs/reviews/cloned-credential-instances-external-review.md F11 (external re-review)
//
// The release contract is mode-independent. A clustered broker must not report
// success while leaving the row ONLINE: the allocator reads that replicated
// row and advances to the next suffix exactly as it did before F11's alleged
// fix. This seam-level guard deliberately enables clusterMode on a writable
// fixture; the current implementation returns nil without proposing any write,
// which is the divergence under review.
func TestClusterReleaseDoesNotLeaveTheLeaseRowOnline(t *testing.T) {
	b := leaseBroker(t)
	seedBeat(t, b, "gpu1-02", 0)
	b.clusterMode = true

	if err := markNodeOfflineOnRelease(b, "lab", "gpu1-02"); err != nil {
		t.Fatalf("cluster release: %v", err)
	}
	var status string
	if err := b.cfg.DB.QueryRow(
		`SELECT status FROM nodes WHERE sid='lab' AND nid='gpu1-02'`,
	).Scan(&status); err != nil {
		t.Fatalf("read released row: %v", err)
	}
	if status != string(node.StateOffline) {
		t.Fatalf("cluster release returned success but left the lease row %s; the allocator still "+
			"counts gpu1-02 as occupied and the restart drifts to gpu1-03", status)
	}
}

// releaseLeasedName drives the farewell arm for an agent running UNDER a lease.
func releaseLeasedName(t *testing.T, b *Broker, sid, nid, iid string) {
	t.Helper()
	req := &proto.NodeRegisterReq{
		ProtoVersion: proto.ProtoVersion, NID: nid, InstanceID: iid,
		ReleasingName: true, LeasedNID: true, RosterRefreshOnly: true,
	}
	body, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	if !replyLeaseVerdict(b, &nats.Msg{Subject: proto.SubjNodeRegister(sid, nid), Data: body}, sid, nid, req) {
		t.Fatal("a farewell must be handled and stop there")
	}
}

// releaseAsTheProductDoes drives the farewell arm with EXACTLY the message the
// agent constructs — nothing the harness knows that the product does not.
//
// origin: prerelease audit broker-core/BC-F1. releaseLeasedName above sets
// LeasedNID:true, and for the whole v0.5.1 line the agent never did. The broker's
// release-the-row gate required that field, so it was dead in production while
// these tests were green: they asserted a message shape the product never emits.
// This helper exists so that can never be true again silently.
func releaseAsTheProductDoes(t *testing.T, b *Broker, sid, nid, iid string, leasedNID bool) {
	t.Helper()
	req := &proto.NodeRegisterReq{
		ProtoVersion: proto.ProtoVersion, NID: nid, InstanceID: iid,
		ReleasingName: true, RosterRefreshOnly: true, LeasedNID: leasedNID,
	}
	body, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	if !replyLeaseVerdict(b, &nats.Msg{Subject: proto.SubjNodeRegister(sid, nid), Data: body}, sid, nid, req) {
		t.Fatal("a farewell must be handled and stop there")
	}
}

func nodeStatus(t *testing.T, b *Broker, nid string) string {
	t.Helper()
	var st string
	if err := b.cfg.DB.QueryRow(`SELECT status FROM nodes WHERE sid='lab' AND nid=?`, nid).Scan(&st); err != nil {
		t.Fatalf("read status of %s: %v", nid, err)
	}
	return st
}

func seedProvisioning(t *testing.T, b *Broker, nid string) {
	t.Helper()
	if _, err := b.cfg.DB.Exec(
		`INSERT INTO agent_provisioning(sid, nid, agent_fp, joined_at)
		 VALUES ('lab', ?, 'SHA256:dev-'||?, ?)`, nid, nid, time.Now().UTC()); err != nil {
		t.Fatalf("seed provisioning for %s: %v", nid, err)
	}
}

// origin: prerelease audit broker-core/BC-F1.
//
// THE RELEASE MUST WORK FOR A FLEET THAT NEVER SENDS THE FLAG.
//
// Leases shipped in v0.5.1. Every agent already in the field builds its farewell
// without LeasedNID, so a gate that requires it is a gate that never fires — and
// F11's suffix drift, which the code comments describe as fixed, kept happening.
// The broker therefore decides from agent_provisioning: a suffixed name with no
// binding of its own IS a lease, whatever the sender says.
func TestAFarewellReleasesTheRowWithoutTheClientSayingItIsALease(t *testing.T) {
	b := leaseBroker(t)
	seedBeat(t, b, "gpu1-02", 0)
	seedProvisioning(t, b, "gpu1") // the configured device; the lease name has no binding

	releaseAsTheProductDoes(t, b, "lab", "gpu1-02", testInstanceB, false)

	if got := nodeStatus(t, b, "gpu1-02"); got != string(node.StateOffline) {
		t.Fatalf("the released lease row is still %s.\n\n"+
			"The farewell carried no LeasedNID, which is what every fielded agent sends. A gate "+
			"that trusts the client's flag is dead code against the deployed fleet, and the name "+
			"stays occupied for the whole OfflineAfter window so the agent's own restart is issued "+
			"the next suffix.", got)
	}
}

// origin: prerelease audit broker-core/BC-F1.
//
// The other direction, and the reason the test is authoritative rather than
// syntactic: a CONFIGURED device's row is not ours to take offline on a farewell.
// Its liveness is the heartbeat's business. The name here is suffix-shaped, so
// only the agent_provisioning binding distinguishes the two cases.
func TestAFarewellNeverTakesAConfiguredDevicesRowOffline(t *testing.T) {
	b := leaseBroker(t)
	seedBeat(t, b, "gpu1-02", 0)
	seedProvisioning(t, b, "gpu1-02") // THIS suffixed name is the operator's configured nid

	releaseAsTheProductDoes(t, b, "lab", "gpu1-02", testInstanceB, true) // even claiming to be a lease

	if got := nodeStatus(t, b, "gpu1-02"); got != "ONLINE" {
		t.Fatalf("a configured device's row was taken %s by a farewell.\n\n"+
			"The client asked for it, and that is exactly why the server must not ask the client: "+
			"a row with its own agent_provisioning binding is a real device whose liveness belongs "+
			"to the heartbeat.", got)
	}
}

// origin: prerelease audit broker-core/BC-F3.
//
// A NON-HOLDER'S FAREWELL MUST NOT TOUCH THE INCUMBENT.
//
// Sibling clones of one image can all authenticate as <base>-NN (the auth suffix
// fallback), so "some process that can legally publish on this register subject"
// is NOT the same party as "the process holding this name". Before BC-F1 the
// row write was dead code and this only cost a cache entry; with the write live,
// a straggler's goodbye knocks the incumbent OFFLINE — which fails its admitACL
// node-ONLINE check, drops it from /sub, and frees its name for re-issue to
// another clone. That is the exact double-holder the whole lease feature exists
// to prevent.
func TestANonHolderFarewellCannotTakeTheIncumbentOffline(t *testing.T) {
	b := leaseBroker(t)
	seedBeat(t, b, "gpu1-02", 0)
	seedProvisioning(t, b, "gpu1")
	// B is the recorded holder of the lease name.
	b.leaseHolder.Store(leaseKey("lab", "gpu1-02"), leaseGrant{instanceID: testInstanceB})
	b.probeCache.Store(leaseKey("lab", "gpu1-02"), true)

	// C — a straggler sibling — says goodbye for a name it does not hold.
	releaseAsTheProductDoes(t, b, "lab", "gpu1-02", testInstanceC, false)

	if got := nodeStatus(t, b, "gpu1-02"); got != "ONLINE" {
		t.Fatalf("a farewell from a non-holder took the live incumbent's row %s.\n\n"+
			"The incumbent now fails admitACL's node-ONLINE check — exec, run, kill and expose all "+
			"refused — vanishes from /sub, and its name is re-issuable to another clone before the "+
			"next heartbeat. Heartbeat restores ONLINE but NOT proxy_ready.", got)
	}
	if _, still := b.probeCache.Load(leaseKey("lab", "gpu1-02")); !still {
		t.Error("a non-holder invalidated the probe evidence.\n\n" +
			"Cheap to send, expensive for everyone else: every subsequent register has to re-probe, " +
			"so an unauthorised sender can hold the name in permanent re-probe by repeating it.")
	}
	if v, ok := b.leaseHolder.Load(leaseKey("lab", "gpu1-02")); ok {
		if g, _ := v.(leaseGrant); g.released {
			t.Error("a non-holder's farewell flagged the holder's grant released")
		}
	}
}

// origin: prerelease audit broker-core/BC-F3, verifier note ①.
//
// THE COLD-MAP PASS IS LOAD-BEARING, not an oversight. After a broker restart or
// a leader election leaseHolder is empty, and the first agent to exit cleanly is
// then unknown to it. Refusing there would mean no row is ever released after a
// restart — reintroducing F11's suffix drift through a different door.
func TestAFarewellStillReleasesWhenTheHolderMapIsCold(t *testing.T) {
	b := leaseBroker(t)
	seedBeat(t, b, "gpu1-02", 0)
	seedProvisioning(t, b, "gpu1")
	// leaseHolder deliberately left empty: this broker just restarted.

	releaseAsTheProductDoes(t, b, "lab", "gpu1-02", testInstanceB, false)

	if got := nodeStatus(t, b, "gpu1-02"); got != string(node.StateOffline) {
		t.Fatalf("a farewell after a broker restart left the row %s.\n\n"+
			"Nothing is recorded in leaseHolder after a restart, so a holder check that fails "+
			"closed here means no lease is ever released again until something re-populates the "+
			"map — which is F11's drift with extra steps.", got)
	}
}

// origin: prerelease audit round 2, E1.
//
// A READABLE-BUT-EMPTY agent_provisioning is not "I could not read the table".
//
// ProvisionedNIDs used to return len(out) > 0 as its second value, so an empty table was
// indistinguishable from a failed read — and this gate, whose entire purpose is to stop
// trusting the client's LeasedNID flag, fell straight back to trusting it on exactly that
// input. An empty table is the STRONGEST evidence the gate can get: nothing on this
// deployment is a configured device, so a suffix-shaped name is certainly a lease.
func TestAnEmptyProvisioningTableStillDecidesTheLeaseServerSide(t *testing.T) {
	b := leaseBroker(t)
	seedBeat(t, b, "gpu1-02", 0)
	// Deliberately NO agent_provisioning rows at all.

	// The client says nothing — the shape every fielded agent sends.
	releaseAsTheProductDoes(t, b, "lab", "gpu1-02", testInstanceB, false)

	if got := nodeStatus(t, b, "gpu1-02"); got != string(node.StateOffline) {
		t.Fatalf("the released lease row is still %s.\n\n"+
			"agent_provisioning was readable and empty, which means NO device on this deployment "+
			"is configured — so a suffixed name is certainly a lease. Reading that as 'cannot "+
			"read' hands the decision back to a client flag no fielded agent sends, which is the "+
			"exact fallback BC-F1 established cannot repair a deployed fleet.", got)
	}
}

// origin: prerelease audit round 2, E2.
//
// A COLD MAP IS NOT A BLANK CHEQUE.
//
// TestAFarewellStillReleasesWhenTheHolderMapIsCold above pins the other half —
// the cold path must stay open — and the two are in tension only if "cold" is
// read as "no evidence". It is not. The map is what THIS broker process
// remembers; the fleet is still there to be asked. Before this, `mayRelease`
// defaulted true whenever leaseHolder missed, so the holder check that BC-F3
// added was unenforced for the whole window after every broker restart,
// re-exec or leader election — and `broker upgrade` re-execs on every roll, so
// that window is on the ordinary operational path, not a corner. Inside it a
// sibling clone's farewell still knocked the live incumbent OFFLINE.
func TestAColdMapStillRefusesAFarewellContradictedByTheLiveHolder(t *testing.T) {
	b, nc, _ := leaseBrokerWithBus(t)
	seedBeat(t, b, "gpu1-02", 0)
	seedProvisioning(t, b, "gpu1")
	// leaseHolder deliberately empty: this broker just re-execed under `broker upgrade`.

	// B is alive under the name and answering claim-probe, exactly as a running
	// lease-aware agent does (internal/agent, replyClaimProbe).
	sub, err := nc.Subscribe(proto.SubjCmdForwarded("lab", "gpu1-02", proto.ClaimProbeVerb), func(m *nats.Msg) {
		payload, merr := json.Marshal(proto.ClaimProbeResp{InstanceID: testInstanceB})
		if merr != nil {
			return
		}
		_ = m.Respond(payload)
	})
	if err != nil {
		t.Fatalf("incumbent subscribe: %v", err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// C — a straggler sibling — says goodbye for a name B is holding.
	releaseAsTheProductDoes(t, b, "lab", "gpu1-02", testInstanceC, false)

	if got := nodeStatus(t, b, "gpu1-02"); got != "ONLINE" {
		t.Fatalf("a non-holder's farewell took the live incumbent's row %s while leaseHolder was cold.\n\n"+
			"The recorded-holder check is only half the rule: with an empty map the sender was "+
			"trusted by default, so every broker restart re-opened the BC-F3 hole for as long as "+
			"the incumbent's connection stayed up. The incumbent is RIGHT THERE answering "+
			"claim-probe with a different instance id — that is a positive contradiction, not an "+
			"absence of evidence.", got)
	}
}

// origin: prerelease audit round 2, E2.
//
// The evidence rule must not fire on the sender ITSELF. An agent that is still
// subscribed when it says goodbye answers its own probe, and reading that as a
// contradiction would refuse every clean exit that has not yet unsubscribed —
// which is most of them, since the farewell is published before teardown.
func TestAHolderStillSubscribedCanReleaseItsOwnNameOnAColdMap(t *testing.T) {
	b, nc, _ := leaseBrokerWithBus(t)
	seedBeat(t, b, "gpu1-02", 0)
	seedProvisioning(t, b, "gpu1")

	sub, err := nc.Subscribe(proto.SubjCmdForwarded("lab", "gpu1-02", proto.ClaimProbeVerb), func(m *nats.Msg) {
		payload, merr := json.Marshal(proto.ClaimProbeResp{InstanceID: testInstanceB})
		if merr != nil {
			return
		}
		_ = m.Respond(payload)
	})
	if err != nil {
		t.Fatalf("incumbent subscribe: %v", err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	releaseAsTheProductDoes(t, b, "lab", "gpu1-02", testInstanceB, false)

	if got := nodeStatus(t, b, "gpu1-02"); got != string(node.StateOffline) {
		t.Fatalf("the holder's own farewell left its row %s.\n\n"+
			"It answered its own claim probe — the responder id EQUALS the sender's — which is "+
			"agreement, not contradiction. Refusing here would leave every cleanly-exiting lease "+
			"row ONLINE for the full OfflineAfter window and re-issue the agent a fresh suffix on "+
			"restart, which is F11's drift.", got)
	}
}
