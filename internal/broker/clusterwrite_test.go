package broker

import (
	"database/sql"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/authcallout"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/clusternodes"
	"github.com/LinZiyang666/tether/internal/node"
	"github.com/LinZiyang666/tether/internal/proc"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/hashicorp/raft"
	"github.com/nats-io/nats.go"
)

// origin: external review 2026-09-03, mixed-version process-exit fence finding.
//
// An N-1 broker does not put SID in ProcMarkExitedPayload. That does not make the
// event trusted: the old broker built it from an agent-published subject whose SID is
// scoped but whose PID is attacker-selected. If the new leader treats the missing SID
// as authority to update by PID alone, the old broker becomes a confused deputy and
// rolling upgrade reopens the cross-session process-kill primitive.
func TestLegacyForwardedProcExitCannotBypassTheSessionFence(t *testing.T) {
	leader, _ := d7SingleNode(t, "leader-legacy-exit-fence")
	now := time.Now().UTC()
	if err := leader.Propose(func(db *sql.DB) (*cluster.Command, error) {
		return session.PlanCreate(db, "victim", "victim", "SHA256:owner", "pin-hash", now)
	}); err != nil {
		t.Fatalf("seed victim session: %v", err)
	}
	if err := leader.Propose(func(db *sql.DB) (*cluster.Command, error) {
		return node.PlanRegister(db, node.RegisterInput{SID: "victim", NID: "n1", ProtoVersion: proto.ProtoVersion}, now)
	}); err != nil {
		t.Fatalf("seed victim node: %v", err)
	}
	if err := leader.Propose(func(db *sql.DB) (*cluster.Command, error) {
		return proc.PlanInsert(db, proc.Process{
			PID: "victim-pid", SID: "victim", NID: "n1", Argv: []string{"train"},
			StartedAt: now, StartedByFP: "SHA256:owner",
		})
	}); err != nil {
		t.Fatalf("seed victim process: %v", err)
	}

	payload, err := json.Marshal(ProcMarkExitedPayload{
		Pid: "victim-pid", ExitCode: 137, EndedAt: now,
		// Sid is deliberately absent: this is the exact payload an N-1 broker forwards.
	})
	if err != nil {
		t.Fatal(err)
	}
	// forwardDeps{} carries NO PIN verifier: this verb runs no Argon2, and a nil verifier
	// is the fail-closed default rather than a fallback (external review M-1).
	err = dispatchForward(leader, func() time.Time { return now }, forwardDeps{}, forwardEnvelope{
		Verb: VerbProcMarkExited, Payload: payload,
	})

	var status string
	if qerr := leader.RODB().QueryRow(
		`SELECT status FROM processes WHERE pid='victim-pid'`).Scan(&status); qerr != nil {
		t.Fatalf("read victim process: %v", qerr)
	}
	if err == nil || status != string(proc.StateRunning) {
		t.Fatalf("legacy payload crossed the session fence: dispatch err=%v, status=%s; "+
			"a session-A agent can name session B's pid through an N-1 broker", err, status)
	}
}

func TestTunnelCertMatchesPinnedHonorsPreviousWindow(t *testing.T) {
	now := time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)
	valid := now.Add(time.Hour)
	expired := now.Add(-time.Nanosecond)

	if !tunnelCertMatchesPinned("sha256:new", &clusternodes.HomeNode{CertFP: "sha256:new"}, now) {
		t.Fatal("current pin should match")
	}
	if !tunnelCertMatchesPinned("sha256:old", &clusternodes.HomeNode{CertFP: "sha256:new", CertFPPrev: "sha256:old", CertValid: &valid}, now) {
		t.Fatal("previous pin should match inside rotation window")
	}
	if tunnelCertMatchesPinned("sha256:old", &clusternodes.HomeNode{CertFP: "sha256:new", CertFPPrev: "sha256:old", CertValid: &expired}, now) {
		t.Fatal("expired previous pin must not match")
	}
	if tunnelCertMatchesPinned("sha256:old", &clusternodes.HomeNode{CertFP: "sha256:new", CertFPPrev: "sha256:old"}, now) {
		t.Fatal("previous pin without valid_until must not match")
	}
}

// TestTunnelCertPinMismatchErrorPointsAtFileRestore (R11 P12/DOC-23): the wireClusterEarly
// pin-mismatch error runs BEFORE the admin socket is up, so its remedy must be a reachable FILE
// restore — it must NOT point at `tether cluster rotate-tunnel-cert` (which needs that socket).
func TestTunnelCertPinMismatchErrorPointsAtFileRestore(t *testing.T) {
	self := &clusternodes.HomeNode{CertFP: "sha256:pinned", CertFPPrev: "sha256:older"}
	err := tunnelCertPinMismatchError("sha256:ondisk", self, "brk-a", "/etc/tether/secrets")
	msg := err.Error()

	// The old, dead-ending remedy must be gone.
	if strings.Contains(msg, "rotate-tunnel-cert") {
		t.Fatalf("bricked-state error must NOT point at rotate-tunnel-cert (admin socket is down); got %q", msg)
	}
	// It must name the reachable file restore: the previous cert + key files under the secrets dir.
	for _, want := range []string{secretTunnelCert, secretTunnelKey, "/etc/tether/secrets", "restore", "restart"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("recovery guidance must mention %q; got %q", want, msg)
		}
	}
}

// nonLeaderNode returns a raft node that can never win an election: it is bootstrapped
// into a TWO-voter configuration whose peer does not exist, so it never reaches quorum.
// That is what makes proposeOrForward take its FORWARD branch.
func nonLeaderNode(t *testing.T, id string) *cluster.Node {
	t.Helper()
	_, trans := raft.NewInmemTransport(raft.ServerAddress(id + ":7400"))
	dir := t.TempDir()
	n, err := cluster.New(cluster.Config{
		LocalID:   raft.ServerID(id),
		DataDir:   dir,
		DBPath:    filepath.Join(dir, "state.db"),
		Transport: trans,
		BootstrapPeers: []raft.Server{
			{Suffrage: raft.Voter, ID: raft.ServerID(id), Address: trans.LocalAddr()},
			{Suffrage: raft.Voter, ID: raft.ServerID("absent-peer"), Address: raft.ServerAddress("absent-peer:7400")},
		},
		ApplyTimeout:       5 * time.Second,
		HeartbeatTimeout:   cluster.MultinodeHeartbeatTimeout,
		ElectionTimeout:    cluster.MultinodeElectionTimeout,
		LeaderLeaseTimeout: cluster.MultinodeLeaderLeaseTimeout,
	})
	if err != nil {
		t.Fatalf("new non-leader node: %v", err)
	}
	t.Cleanup(func() { _ = n.Shutdown() })
	// THE PREMISE, asserted by what it MEANS rather than by reading leadership.
	//
	// What this fixture has to guarantee is that proposeOrForward takes its FORWARD
	// branch, and the property that guarantees it is "this node cannot commit". Reading
	// IsLeader() would say the same thing one statement earlier and be stale by the time
	// it mattered — docs/testing-standards.md T3, which test/determinism enforces.
	if err := n.Propose(func(*sql.DB) (*cluster.Command, error) {
		return cluster.NewCommand(cluster.OpClusterMetaSet,
			cluster.Stmt(`INSERT OR IGNORE INTO cluster_meta(key,value) VALUES('premise','1')`)), nil
	}); err == nil {
		t.Fatal("premise broken: this node COMMITTED a write, so it can win elections and " +
			"proposeOrForward will take its leader-local branch — the forward path this fixture " +
			"exists to reach is then never exercised")
	}
	return n
}

// origin: prerelease audit round 2, L-F1.
//
// THE FENCE HAS TO HOLD ON THE PATH THE FLEET ACTUALLY USES.
//
// proc.MarkExited is sid-scoped, and internal/proc has a guard for it. But in CLUSTER
// mode — which is what the live fleet runs — the writer is the LEADER, and what reaches
// it is a marshalled ProcMarkExitedPayload. Deleting `Sid: sid` from that struct literal
// reopened the cross-session escape on the dominant path and left every gate green: the
// single-mode guard still passed, because single mode never builds the payload.
//
// So this drives the real thing end to end: the real marshal in markProcExited, the real
// Forwarder over a real bus, the real SubscribeClusterApply responder, and a real leader
// applying a real PlanMarkExited. The only thing constructed for the test is a node that
// cannot win an election, which is how the forward branch is reached at all.
func TestAForwardedProcExitCarriesTheSessionFence(t *testing.T) {
	leader, _ := d7SingleNode(t, "leader-1")
	now := time.Now().UTC()
	if err := leader.Propose(func(db *sql.DB) (*cluster.Command, error) {
		return session.PlanCreate(db, "lab", "lab", "SHA256:owner", "pin-hash", now)
	}); err != nil {
		t.Fatalf("seed session: %v", err)
	}
	if err := leader.Propose(func(db *sql.DB) (*cluster.Command, error) {
		return node.PlanRegister(db, node.RegisterInput{SID: "lab", NID: "n1", ProtoVersion: proto.ProtoVersion}, now)
	}); err != nil {
		t.Fatalf("seed node: %v", err)
	}
	if err := leader.Propose(func(db *sql.DB) (*cluster.Command, error) {
		return proc.PlanInsert(db, proc.Process{
			PID: "p1", SID: "lab", NID: "n1", Argv: []string{"train"},
			StartedAt: now, StartedByFP: "SHA256:u",
		})
	}); err != nil {
		t.Fatalf("seed process: %v", err)
	}

	url := testharness.StartNATS(t)
	lnc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(lnc.Close)
	sub, err := SubscribeClusterApply(lnc, leader, func() time.Time { return now }, silentLogger(),
		(&authcallout.Handler{Now: time.Now}).VerifyPINWithBudget)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	if err := lnc.Flush(); err != nil {
		t.Fatal(err)
	}

	fnc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(fnc.Close)
	b := &Broker{cfg: Config{Logger: silentLogger(), Now: func() time.Time { return now }}}
	b.clusterMode = true
	b.cl = &clusterRuntime{node: nonLeaderNode(t, "follower-1"), forwarder: NewForwarder(fnc, 3*time.Second)}

	// A member of session `other` publishes an exit for a pid that belongs to `lab`.
	_ = b.markProcExited("p1", "other", 0, now)

	var status string
	if err := leader.RODB().QueryRow(`SELECT status FROM processes WHERE pid='p1'`).Scan(&status); err != nil {
		t.Fatalf("read the victim's process row: %v", err)
	}
	if status != string(proc.StateRunning) {
		t.Fatalf("a cross-session exit marked another session's process %s.\n\n"+
			"The sid rides the forwarded payload precisely so the LEADER applies the same fence "+
			"the single-node writer does. A fence that only holds on the mode the attacker did "+
			"not happen to hit is not one — and single mode is not what the fleet runs.", status)
	}

	// POSITIVE CONTROL: the OWNING session's exit must still commit through the same
	// path, or the assertion above is satisfied by a forward that never works at all.
	if err := b.markProcExited("p1", "lab", 7, now); err != nil {
		t.Fatalf("the owning session's forwarded exit failed: %v", err)
	}
	if err := leader.RODB().QueryRow(`SELECT status FROM processes WHERE pid='p1'`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != string(proc.StateExited) {
		t.Fatalf("the owning session's exit did not commit (status=%s); this test's negative "+
			"assertion proves nothing if the forward is simply broken", status)
	}
}
