package cluster

import (
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/hashicorp/raft"
)

// membership_test.go (formerly d7_membership_test.go) — D7 §8.1 cheap unit tests (make test): Plan/applier join-PoP
// (forged → errAppliedRejected poison-skip, valid → exec), phase-predecessor CAS
// no-op, removal only on terminal phases, migration 0013 columns. The multi-node
// forged-sig-read-on-FOLLOWER drill is gated d7_integration (test/d7 TestD7Matrix).

func d7GenKey(t *testing.T) (seed []byte, pub string) {
	t.Helper()
	s, err := auth.GenerateUserSeed()
	if err != nil {
		t.Fatalf("gen seed: %v", err)
	}
	p, err := auth.PublicKeyFromSeed(s)
	if err != nil {
		t.Fatalf("pub from seed: %v", err)
	}
	return s, p
}

func d7Sign(t *testing.T, seed, msg []byte) string {
	t.Helper()
	sig, err := auth.SignWithSeed(seed, msg)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	return hex.EncodeToString(sig)
}

func d7UpsertInput(nodeID, pub, nonce, sigHex string) ClusterNodeUpsertInput {
	return ClusterNodeUpsertInput{
		NodeID:       nodeID,
		Name:         nodeID,
		NodeIdentPub: pub,
		NatsServerID: "tether-" + nodeID,
		RaftAddr:     "10.0.0.9:7400",
		NatsRoute:    "nats://10.0.0.9:6222",
		TunnelAddr:   "10.0.0.9:7000",
		PublicHost:   "host.example",
		CertFP:       "sha256:deadbeef",
		JoinNonce:    nonce,
		JoinSigHex:   sigHex,
		Now:          time.Date(2026, 6, 23, 1, 2, 3, 0, time.UTC),
	}
}

// applyOne encodes + applies a single command at the given index and returns the
// FSM result sentinel (or fails on a panic-equivalent error).
func d7Apply(t *testing.T, f *fsm, idx uint64, cmd *Command) any {
	t.Helper()
	data, err := cmd.encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	return f.Apply(&raft.Log{Index: idx, Term: 1, Type: raft.LogCommand, Data: data})
}

func d7CountNode(t *testing.T, f *fsm, nodeID string) int {
	t.Helper()
	var n int
	if err := f.db.QueryRow(`SELECT COUNT(*) FROM cluster_nodes WHERE node_id=?`, nodeID).Scan(&n); err != nil {
		t.Fatalf("count node: %v", err)
	}
	return n
}

func d7Phase(t *testing.T, f *fsm, nodeID string) string {
	t.Helper()
	var p string
	if err := f.db.QueryRow(`SELECT phase FROM cluster_nodes WHERE node_id=?`, nodeID).Scan(&p); err != nil {
		t.Fatalf("read phase: %v", err)
	}
	return p
}

func d7Col(t *testing.T, f *fsm, nodeID, col string) string {
	t.Helper()
	var v string
	if err := f.db.QueryRow(`SELECT COALESCE(`+col+`,'') FROM cluster_nodes WHERE node_id=?`, nodeID).Scan(&v); err != nil {
		t.Fatalf("read %s: %v", col, err)
	}
	return v
}

// TestD7UpsertEmptyReadmitPreservesIdentity (v0.4.4 review F2): an idempotent grow-retry re-approve that
// carries an EMPTY bus_nkey / cert_fp must NOT clobber a previously-good value back to "" via the ON
// CONFLICT DO UPDATE — that silently re-arms the learner self-backfill DEADLOCK (bus_nkey) and the joiner
// crash-loop (cert_fp), the exact failures audit A removes. The upsert must be empty-PRESERVING.
func TestD7UpsertEmptyReadmitPreservesIdentity(t *testing.T) {
	f, _ := freshFSM(t, t.TempDir())
	seed, pub := d7GenKey(t)
	const node = "n1"

	// (1) admit with a GOOD bus_nkey + cert_fp.
	nonce1 := "nonce-good"
	in1 := d7UpsertInput(node, pub, nonce1, d7Sign(t, seed, JoinSignBytes(node, pub, nonce1)))
	// A REAL user public key: the admission planner now validates a non-empty bus_nkey
	// the same way PlanClusterBusNkeySet always has (prerelease audit cluster-fsm/L3-F2),
	// so a placeholder here would test the validator instead of the empty-preserve rule
	// this test is about.
	_, goodBusNkey := d7GenKey(t)
	in1.BusNkey = goodBusNkey
	in1.CertFP = "sha256:goodfp"
	cmd1, err := PlanClusterNodeUpsert(in1)
	if err != nil {
		t.Fatalf("plan admit: %v", err)
	}
	if _, ok := d7Apply(t, f, 1, cmd1).(appliedOK); !ok {
		t.Fatal("admit: want appliedOK")
	}
	if bn := d7Col(t, f, node, "bus_nkey_pub"); bn != goodBusNkey {
		t.Fatalf("after admit bus_nkey_pub=%q, want %q", bn, goodBusNkey)
	}

	// (2) re-admit (still PENDING → the DO UPDATE fires) carrying an EMPTY bundle.
	nonce2 := "nonce-empty"
	in2 := d7UpsertInput(node, pub, nonce2, d7Sign(t, seed, JoinSignBytes(node, pub, nonce2)))
	in2.BusNkey = ""
	in2.CertFP = ""
	cmd2, err := PlanClusterNodeUpsert(in2)
	if err != nil {
		t.Fatalf("plan re-admit: %v", err)
	}
	if _, ok := d7Apply(t, f, 2, cmd2).(appliedOK); !ok {
		t.Fatal("re-admit: want appliedOK")
	}

	// (3) the good values must be PRESERVED, not clobbered to ''.
	if bn := d7Col(t, f, node, "bus_nkey_pub"); bn != goodBusNkey {
		t.Fatalf("empty re-admit CLOBBERED bus_nkey_pub to %q — must preserve the admitted key (re-arms the deadlock)", bn)
	}
	if fp := d7Col(t, f, node, "cert_fp"); fp != "sha256:goodfp" {
		t.Fatalf("empty re-admit CLOBBERED cert_fp to %q — must preserve sha256:goodfp (re-arms the crash-loop)", fp)
	}
}

// TestD7PlanUpsertVerifiesLeaderSide: the leader refuses to PROPOSE an entry whose
// join PoP does not verify (fail-closed pre-propose), and produces a well-formed
// command (with Aux) for a valid signature.
func TestD7PlanUpsertVerifiesLeaderSide(t *testing.T) {
	seed, pub := d7GenKey(t)
	nonce := "nonce-abc"
	msg := JoinSignBytes("n1", pub, nonce)
	goodSig := d7Sign(t, seed, msg)

	cmd, err := PlanClusterNodeUpsert(d7UpsertInput("n1", pub, nonce, goodSig))
	if err != nil {
		t.Fatalf("valid upsert should plan: %v", err)
	}
	if cmd.Op != OpClusterNodeUpsert || len(cmd.Body) != 1 || len(cmd.Aux) == 0 {
		t.Fatalf("malformed planned command: %+v", cmd)
	}

	// A signature over the message but with the WRONG key must not plan.
	advSeed, _ := d7GenKey(t)
	badSig := d7Sign(t, advSeed, msg) // valid sig, wrong key for pub
	if _, err := PlanClusterNodeUpsert(d7UpsertInput("n1", pub, nonce, badSig)); err == nil {
		t.Fatal("leader planned an entry whose join PoP does not verify")
	}
}

// TestD7ForgedSigPoisonSkips is the load-bearing FSM unit: a committed
// OpClusterNodeUpsert whose signature does not verify (a compromised/buggy leader)
// must POISON-SKIP — advance applied_index, write NO roster row, NEVER panic — and
// the FSM must keep applying the next legitimate entry.
func TestD7ForgedSigPoisonSkips(t *testing.T) {
	f, _ := freshFSM(t, t.TempDir())

	victimSeed, victimPub := d7GenKey(t)
	advSeed, _ := d7GenKey(t)
	nonce := "nonce-xyz"
	msg := JoinSignBytes("victim", victimPub, nonce)
	victimSigHex := d7Sign(t, victimSeed, msg)
	advSigHex := d7Sign(t, advSeed, msg) // verifies under advPub, NOT under victimPub

	// Build a well-formed command (passes the leader pre-verify), then FORGE it:
	// swap the victim sig for the adversary sig in BOTH the baked row literal and
	// the Aux (so the Aux-vs-Body cross-check passes and we reach the real verify,
	// which fails). This is exactly "a committed entry whose PoP does not verify".
	cmd, err := PlanClusterNodeUpsert(d7UpsertInput("victim", victimPub, nonce, victimSigHex))
	if err != nil {
		t.Fatalf("seed command: %v", err)
	}
	cmd.Body[0].SQL = strings.ReplaceAll(cmd.Body[0].SQL, victimSigHex, advSigHex)
	auxBytes, _ := json.Marshal(clusterNodeUpsertAux{
		NodeID: "victim", Name: "victim", NodeIdentPub: victimPub, JoinNonce: nonce, JoinSigHex: advSigHex,
	})
	cmd.Aux = auxBytes

	res := d7Apply(t, f, 1, cmd) // must NOT panic
	if _, ok := res.(appliedRejected); !ok {
		t.Fatalf("forged sig: want appliedRejected, got %T", res)
	}
	if got := f.rejectCount.Load(); got != 1 {
		t.Fatalf("rejectCount = %d, want 1 (poison-skip branch must fire)", got)
	}
	if mustApplied(t, f) != 1 {
		t.Fatalf("applied_index must advance past the rejected entry, got %d", mustApplied(t, f))
	}
	if n := d7CountNode(t, f, "victim"); n != 0 {
		t.Fatalf("forged sig wrote a roster row (%d) — must write NONE", n)
	}

	// The FSM stays live: a subsequent legitimate entry applies.
	seed2, pub2 := d7GenKey(t)
	nonce2 := "nonce-ok"
	good := d7Sign(t, seed2, JoinSignBytes("n2", pub2, nonce2))
	cmd2, err := PlanClusterNodeUpsert(d7UpsertInput("n2", pub2, nonce2, good))
	if err != nil {
		t.Fatalf("legit command: %v", err)
	}
	if res := d7Apply(t, f, 2, cmd2); func() bool { _, ok := res.(appliedOK); return !ok }() {
		t.Fatalf("legit entry after a rejection: want appliedOK, got %T", res)
	}
	if d7CountNode(t, f, "n2") != 1 || d7Phase(t, f, "n2") != "JOIN_VERIFIED_PENDING_VOTER" {
		t.Fatal("legit admission did not land at JOIN_VERIFIED_PENDING_VOTER")
	}
}

// TestD7UpsertAppliesAndCrossCheckRejectsSplice: a valid signature applies and
// writes the row; an Aux/Body splice (Aux pubkey != baked pubkey) is rejected even
// though the Aux signature self-verifies.
func TestD7UpsertAppliesAndCrossCheckRejectsSplice(t *testing.T) {
	f, _ := freshFSM(t, t.TempDir())
	seed, pub := d7GenKey(t)
	nonce := "n-splice"
	good := d7Sign(t, seed, JoinSignBytes("ok", pub, nonce))
	cmd, err := PlanClusterNodeUpsert(d7UpsertInput("ok", pub, nonce, good))
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	if res := d7Apply(t, f, 1, cmd); func() bool { _, ok := res.(appliedOK); return !ok }() {
		t.Fatalf("valid upsert: want appliedOK, got %T", res)
	}
	if d7CountNode(t, f, "ok") != 1 {
		t.Fatal("valid upsert wrote no row")
	}

	// Splice: Aux carries a DIFFERENT (self-consistent) pub+sig than the baked row.
	advSeed, advPub := d7GenKey(t)
	advNonce := "n-adv"
	advSig := d7Sign(t, advSeed, JoinSignBytes("victim2", advPub, advNonce))
	victimSeed, victimPub := d7GenKey(t)
	vSig := d7Sign(t, victimSeed, JoinSignBytes("victim2", victimPub, advNonce))
	spliced, err := PlanClusterNodeUpsert(d7UpsertInput("victim2", victimPub, advNonce, vSig))
	if err != nil {
		t.Fatalf("seed splice cmd: %v", err)
	}
	// Aux says (advPub, advSig) — self-verifies — but Body bakes (victimPub, vSig) at
	// the node_ident_pub column. The POSITIONAL cross-check (review M8) must reject:
	// even with Name matching, advPub is not at the node_ident_pub column slot.
	aux, _ := json.Marshal(clusterNodeUpsertAux{NodeID: "victim2", Name: "victim2", NodeIdentPub: advPub, JoinNonce: advNonce, JoinSigHex: advSig})
	spliced.Aux = aux
	if res := d7Apply(t, f, 2, spliced); func() bool { _, ok := res.(appliedRejected); return !ok }() {
		t.Fatalf("Aux/Body splice: want appliedRejected, got %T", res)
	}
	if d7CountNode(t, f, "victim2") != 0 {
		t.Fatal("spliced admission wrote a roster row")
	}
}

// TestD7UpsertConstraintViolationPoisonSkips (review B2): a committed, validly-signed
// OpClusterNodeUpsert whose baked SQL violates a constraint (UNIQUE(name)) must
// POISON-SKIP (errAppliedRejected), NOT return a plain error — a plain error would
// feed the fsm retry loop and PANIC every replica on every log replay (never-wedge).
func TestD7UpsertConstraintViolationPoisonSkips(t *testing.T) {
	f, _ := freshFSM(t, t.TempDir())
	// nodeA with name "shared" — applies.
	sA, pA := d7GenKey(t)
	inA := d7UpsertInput("nodeA", pA, "n-a", d7Sign(t, sA, JoinSignBytes("nodeA", pA, "n-a")))
	inA.Name = "shared"
	cmdA, err := PlanClusterNodeUpsert(inA)
	if err != nil {
		t.Fatalf("plan A: %v", err)
	}
	if _, ok := d7Apply(t, f, 1, cmdA).(appliedOK); !ok {
		t.Fatal("nodeA should apply")
	}
	// nodeB with the SAME name "shared" — different node_id, so ON CONFLICT(node_id)
	// does NOT fire; the UNIQUE(name) constraint does.
	sB, pB := d7GenKey(t)
	inB := d7UpsertInput("nodeB", pB, "n-b", d7Sign(t, sB, JoinSignBytes("nodeB", pB, "n-b")))
	inB.Name = "shared"
	cmdB, err := PlanClusterNodeUpsert(inB)
	if err != nil {
		t.Fatalf("plan B: %v", err)
	}
	res := d7Apply(t, f, 2, cmdB) // must NOT panic
	if _, ok := res.(appliedRejected); !ok {
		t.Fatalf("UNIQUE(name) violation: want appliedRejected (poison-skip), got %T", res)
	}
	if mustApplied(t, f) != 2 {
		t.Fatalf("applied_index must advance past the rejected entry, got %d", mustApplied(t, f))
	}
	if d7CountNode(t, f, "nodeB") != 0 {
		t.Fatal("nodeB row must NOT exist (constraint rejected)")
	}
	// FSM stays live: a trivial valid op still applies after the rejection.
	probe, _ := newClusterMetaSet("t:probe", "ok")
	if _, ok := d7Apply(t, f, 3, probe).(appliedOK); !ok {
		t.Fatal("FSM wedged after constraint rejection")
	}
}

// TestD7PhaseCASPredecessorGuard: a transition whose predecessor set does not
// include the row's current phase is a deterministic RowsAffected==0 no-op.
func TestD7PhaseCASPredecessorGuard(t *testing.T) {
	f, _ := freshFSM(t, t.TempDir())
	seed, pub := d7GenKey(t)
	nonce := "n-phase"
	good := d7Sign(t, seed, JoinSignBytes("p1", pub, nonce))
	cmd, _ := PlanClusterNodeUpsert(d7UpsertInput("p1", pub, nonce, good))
	d7Apply(t, f, 1, cmd) // phase = JOIN_VERIFIED_PENDING_VOTER

	now := time.Date(2026, 6, 23, 2, 0, 0, 0, time.UTC)
	// Legit: PENDING -> CATCHING_UP.
	c1, err := PlanClusterNodePhase("p1", "CATCHING_UP", []string{"JOIN_VERIFIED_PENDING_VOTER"}, "", now)
	if err != nil {
		t.Fatalf("plan phase: %v", err)
	}
	d7Apply(t, f, 2, c1)
	if d7Phase(t, f, "p1") != "CATCHING_UP" {
		t.Fatalf("legit transition did not apply, phase=%s", d7Phase(t, f, "p1"))
	}
	// Stale: a transition from PENDING applied to a CATCHING_UP row is a no-op.
	c2, _ := PlanClusterNodePhase("p1", "VOTER", []string{"JOIN_VERIFIED_PENDING_VOTER"}, "", now)
	d7Apply(t, f, 3, c2)
	if d7Phase(t, f, "p1") != "CATCHING_UP" {
		t.Fatalf("disallowed-predecessor transition was NOT a no-op, phase=%s", d7Phase(t, f, "p1"))
	}
}

// TestD7RemoveOnlyTerminalPhases: a live VOTER cannot be removed; a RETIRING row can.
func TestD7RemoveOnlyTerminalPhases(t *testing.T) {
	f, _ := freshFSM(t, t.TempDir())
	seed, pub := d7GenKey(t)
	nonce := "n-rm"
	good := d7Sign(t, seed, JoinSignBytes("r1", pub, nonce))
	cmd, _ := PlanClusterNodeUpsert(d7UpsertInput("r1", pub, nonce, good))
	d7Apply(t, f, 1, cmd)
	now := time.Date(2026, 6, 23, 3, 0, 0, 0, time.UTC)
	// Walk to VOTER.
	d7Apply(t, f, 2, mustPhase(t, "r1", "CATCHING_UP", []string{"JOIN_VERIFIED_PENDING_VOTER"}, now))
	d7Apply(t, f, 3, mustPhase(t, "r1", "VOTER", []string{"CATCHING_UP"}, now))

	// Remove on a VOTER is a no-op.
	rm, _ := PlanClusterNodeRemove("r1", now)
	d7Apply(t, f, 4, rm)
	if d7CountNode(t, f, "r1") != 1 {
		t.Fatal("a live VOTER was removed by OpClusterNodeRemove")
	}
	// Walk to RETIRING, then remove succeeds.
	d7Apply(t, f, 5, mustPhase(t, "r1", "DRAINING", []string{"VOTER"}, now))
	d7Apply(t, f, 6, mustPhase(t, "r1", "RETIRING", []string{"DRAINING"}, now))
	d7Apply(t, f, 7, rm)
	if d7CountNode(t, f, "r1") != 0 {
		t.Fatal("a RETIRING node was not removed")
	}
}

func mustPhase(t *testing.T, nodeID, to string, preds []string, now time.Time) *Command {
	t.Helper()
	c, err := PlanClusterNodePhase(nodeID, to, preds, "", now)
	if err != nil {
		t.Fatalf("plan phase %s: %v", to, err)
	}
	return c
}

// TestD7Migration0013Columns asserts the join-PoP bookkeeping columns exist.
func TestD7Migration0013Columns(t *testing.T) {
	f, _ := freshFSM(t, t.TempDir())
	rows, err := f.db.Query(`PRAGMA table_info(cluster_nodes)`)
	if err != nil {
		t.Fatalf("table_info: %v", err)
	}
	defer func() { _ = rows.Close() }()
	have := map[string]bool{}
	for rows.Next() {
		var cid int
		var name, typ string
		var notnull, pk int
		var dflt any
		if err := rows.Scan(&cid, &name, &typ, &notnull, &dflt, &pk); err != nil {
			t.Fatalf("scan: %v", err)
		}
		have[name] = true
	}
	for _, c := range []string{"join_nonce", "join_sig", "voter_add_error", "phase_changed_at"} {
		if !have[c] {
			t.Errorf("migration 0013 missing column %q", c)
		}
	}
}

// origin: prerelease audit cluster-fsm/L3-F2.
//
// THE ADMISSION PATH VALIDATES WHAT THE REWRITE PATHS VALIDATE.
//
// Each of nats_route, raft_addr and bus_nkey_pub has a dedicated rewrite planner in
// membership_ops.go that validates it and states in its own comment what breaks when it
// does not: a garbage route renders a broken cluster{} block on every replica, a garbage
// nkey breaks every replica's `nats-server -t`, a garbage raft_addr desynchronises
// status and the :7400 liveness probe. The planner that writes those columns for the
// FIRST time checked none of them, so a node had to be admitted before its own values
// could be validated.
func TestUpsertRefusesTheValuesItsRewritePlannersRefuse(t *testing.T) {
	seed, pub := d7GenKey(t)
	const node = "n-valid"
	nonce := "nonce-1"
	sig := d7Sign(t, seed, JoinSignBytes(node, pub, nonce))
	_, goodNkey := d7GenKey(t)

	base := func() ClusterNodeUpsertInput {
		in := d7UpsertInput(node, pub, nonce, sig)
		in.BusNkey = goodNkey
		return in
	}

	// The fixture itself must be admissible, or every negative case below is vacuous.
	if _, err := PlanClusterNodeUpsert(base()); err != nil {
		t.Fatalf("the well-formed fixture was refused: %v", err)
	}

	cases := []struct {
		name    string
		mutate  func(*ClusterNodeUpsertInput)
		mention string
	}{
		{"route with no scheme", func(in *ClusterNodeUpsertInput) { in.NatsRoute = "10.0.0.9:6222" }, "nats route"},
		{"route carrying credentials", func(in *ClusterNodeUpsertInput) {
			in.NatsRoute = "nats://user:pw@10.0.0.9:6222"
		}, "credentials"},
		{"raft addr with no port", func(in *ClusterNodeUpsertInput) { in.RaftAddr = "brk-b" }, "raft addr"},
		{"bus nkey that is not a key", func(in *ClusterNodeUpsertInput) { in.BusNkey = "UNOTAREALKEY" }, "bus nkey"},
		{"node id that is not a node id", func(in *ClusterNodeUpsertInput) { in.NodeID = "Not A Node" }, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := base()
			tc.mutate(&in)
			// The node-id case has to re-sign, or it would be refused by the PoP check
			// instead and prove nothing about the id validator.
			if in.NodeID != node {
				in.JoinNonce = "nonce-2"
				in.JoinSigHex = d7Sign(t, seed, JoinSignBytes(in.NodeID, pub, in.JoinNonce))
			}
			_, err := PlanClusterNodeUpsert(in)
			if err == nil {
				t.Fatalf("the admission planner accepted %s.\n\n"+
					"The rewrite planner for this very column refuses it. Admitting it means the "+
					"cluster carries a value that can only be corrected by a later operation, and "+
					"every replica has to live with it in the meantime.", tc.name)
			}
			if tc.mention != "" && !strings.Contains(err.Error(), tc.mention) {
				t.Errorf("refusal %q does not name the offending field (%q)", err, tc.mention)
			}
		})
	}

	// An EMPTY bus nkey stays legal: the conflict path is empty-PRESERVE on purpose, so
	// an idempotent grow-retry carrying an empty bundle must still be admissible.
	in := base()
	in.BusNkey = ""
	if _, err := PlanClusterNodeUpsert(in); err != nil {
		t.Fatalf("an empty bus nkey must remain legal (the ON CONFLICT path is empty-preserving): %v", err)
	}
}

// origin: prerelease audit cluster-fsm/L3-F3.
//
// TWO CONCURRENT ROLLS MUST NOT BOTH HOLD THE UPGRADE LOCK.
//
// markerAcquireStmts is INSERT-OR-IGNORE then UPDATE, and the guard was `NOT
// growMarkerExists()` alone — so when an upgrade marker was already present the UPDATE
// fired and overwrote the timestamp. A second `cluster upgrade` "acquired" a lock the
// first was holding, and both read-backs returned true because upgradeActive() only
// asks whether the key exists. The grow lock had been given exactly this treatment by
// external review H1/M1; this one was left behind.
//
// The lock is what keeps join/retire from crossing a rolling restart, so two holders
// means the second roll runs its whole second half with no lock at all.
func TestASecondRollCannotTakeTheUpgradeLockFromTheFirst(t *testing.T) {
	f, _ := freshFSM(t, t.TempDir())
	first := time.Date(2026, 9, 2, 3, 0, 0, 0, time.UTC)
	second := first.Add(time.Minute)

	cmd, err := PlanSetUpgradeActive(first)
	if err != nil {
		t.Fatalf("plan first acquire: %v", err)
	}
	if _, ok := d7Apply(t, f, 1, cmd).(appliedOK); !ok {
		t.Fatal("first acquire: want appliedOK")
	}
	if got := UpgradeMarkerValue(f.db); got != UpgradeMarkerStamp(first) {
		t.Fatalf("after the first acquire the marker is %q, want %q", got, UpgradeMarkerStamp(first))
	}
	// The lease MUST land in the same command. A marker with no lease is the
	// never-expires bucket, and it is exactly what an existence-shaped guard produces:
	// the marker's own INSERT makes the guard false for the lease statements behind it.
	var lease string
	if err := f.db.QueryRow(`SELECT value FROM cluster_meta WHERE key=?`, MetaKeyUpgradeLease).Scan(&lease); err != nil {
		t.Fatalf("the acquire did not stamp a lease (%v) — the lock would never expire, and an "+
			"interrupted roll would fence join/retire permanently", err)
	}

	// A SECOND roll acquires with its own stamp. It must change nothing.
	cmd2, err := PlanSetUpgradeActive(second)
	if err != nil {
		t.Fatalf("plan second acquire: %v", err)
	}
	if _, ok := d7Apply(t, f, 2, cmd2).(appliedOK); !ok {
		t.Fatal("second acquire: want appliedOK (a refused acquire is a no-op, not an error)")
	}
	if got := UpgradeMarkerValue(f.db); got != UpgradeMarkerStamp(first) {
		t.Fatalf("the second roll overwrote the marker: %q, want the FIRST roll's %q.\n\n"+
			"The acquirer proves it won by comparing the marker value against the stamp it "+
			"proposed. If a second roll can overwrite that value, both rolls read back their own "+
			"stamp and both believe they hold a topology-stable window.",
			got, UpgradeMarkerStamp(first))
	}
	var lease2 string
	if err := f.db.QueryRow(`SELECT value FROM cluster_meta WHERE key=?`, MetaKeyUpgradeLease).Scan(&lease2); err != nil {
		t.Fatalf("the refused acquire destroyed the holder's lease: %v", err)
	}
	if lease2 != lease {
		t.Errorf("the refused acquire extended the holder's lease from %q to %q — a second roll "+
			"must not be able to keep the first one's lock alive", lease, lease2)
	}
}

// upgradeLeaseOf reads the roll lock's lease, or "" when there is none.
func upgradeLeaseOf(t *testing.T, f *fsm) string {
	t.Helper()
	var v string
	if err := f.db.QueryRow(`SELECT value FROM cluster_meta WHERE key=?`, MetaKeyUpgradeLease).Scan(&v); err != nil {
		return ""
	}
	return v
}

// acquireUpgradeAt proposes an acquire stamped `at` and reports whether it WON —
// which, as the acquirer itself determines, means the marker now holds its stamp.
func acquireUpgradeAt(t *testing.T, f *fsm, idx uint64, at time.Time) bool {
	t.Helper()
	cmd, err := PlanSetUpgradeActive(at)
	if err != nil {
		t.Fatalf("plan acquire: %v", err)
	}
	if _, ok := d7Apply(t, f, idx, cmd).(appliedOK); !ok {
		t.Fatal("acquire: want appliedOK (a refused acquire is a no-op, not an error)")
	}
	return UpgradeMarkerValue(f.db) == UpgradeMarkerStamp(at)
}

// origin: prerelease audit round 2, H-2.
//
// A LOCK NOBODY IS RENEWING IS NOT HELD.
//
// L3-F3 made the acquire refuse a marker it does not own, which is right against a
// LIVE competitor and wrong against the operator's own interrupted roll: a HALT
// deliberately leaves the marker set, so `cluster upgrade` re-run — the recovery
// its own HALT message instructs — was refused for LockLeaseTTL plus a reap
// interval, and so was the emergency `--to-version <older>` rollback an operator
// reaches for while the fleet sits mixed-version.
//
// The fix needs no holder identity (the protocol has none, and adding a roll id to
// CanonicalUpgradeReqBytes would break every not-yet-upgraded verifier mid-roll):
// a roll that is still driving renews every LockLeaseRenewInterval, so its lease is
// never in the past.
func TestAnExpiredLeaseStopsBlockingTheNextRoll(t *testing.T) {
	f, _ := freshFSM(t, t.TempDir())
	first := time.Date(2026, 9, 2, 3, 0, 0, 0, time.UTC)

	if !acquireUpgradeAt(t, f, 1, first) {
		t.Fatal("the first acquire did not win an unheld lock")
	}
	// A LIVE holder is still exclusive: one renew interval in, its lease has
	// plenty left and a second roll must be refused.
	if acquireUpgradeAt(t, f, 2, first.Add(LockLeaseRenewInterval)) {
		t.Fatal("a second roll took the lock from a holder whose lease was still live.\n\n" +
			"That is the H1 mutual exclusion: two rolls would each believe they own a " +
			"topology-stable window, and join/retire could cross a rolling restart.")
	}
	// Past the TTL the holder has demonstrably stopped renewing.
	if !acquireUpgradeAt(t, f, 3, first.Add(LockLeaseTTL+time.Minute)) {
		t.Fatal("a re-run was refused by the lease of a roll that stopped renewing.\n\n" +
			"A HALT leaves the marker set on purpose, so this is the ordinary recovery path — " +
			"`cluster upgrade` HALTs, the operator fixes the cause and re-runs, and the roll's " +
			"own abandoned marker refuses it. The emergency rollback is blocked the same way.")
	}
	// AND THE MEMBERSHIP FENCE SURVIVES: what the re-run may take is the
	// roll-versus-roll exclusion, never the partial-roll fence on join/retire.
	if UpgradeMarkerValue(f.db) == "" {
		t.Fatal("the upgrade marker is gone — `cluster join`/`cluster retire` would now be " +
			"admitted while the cluster sits mid-roll")
	}
}

// origin: prerelease audit round 2, H-2.
//
// FAIL CLOSED ON EVERY UNKNOWN. A marker with no lease is a lock acquired by a
// pre-R7b broker, and lock_lease.go's own contract is that it NEVER expires on its
// own — `tether cluster unlock` is the out. An unparseable lease is the same case
// arrived at differently. Either must keep blocking, or an upgrade during exactly
// the mixed-version window this whole mechanism serves would steal a live lock.
func TestAnUnexpirableLeaseKeepsBlockingTheNextRoll(t *testing.T) {
	for _, tc := range []struct {
		name    string
		corrupt func(t *testing.T, f *fsm)
	}{
		{"no lease row at all (a pre-R7b lock)", func(t *testing.T, f *fsm) {
			if _, err := f.db.Exec(`DELETE FROM cluster_meta WHERE key=?`, MetaKeyUpgradeLease); err != nil {
				t.Fatal(err)
			}
		}},
		{"an unparseable expiry", func(t *testing.T, f *fsm) {
			if _, err := f.db.Exec(`UPDATE cluster_meta SET value='not-a-time' WHERE key=?`, MetaKeyUpgradeLease); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, _ := freshFSM(t, t.TempDir())
			first := time.Date(2026, 9, 2, 3, 0, 0, 0, time.UTC)
			if !acquireUpgradeAt(t, f, 1, first) {
				t.Fatal("the first acquire did not win an unheld lock")
			}
			tc.corrupt(t, f)

			// Far past any TTL: only a lease that PARSES and has expired may open the door.
			if acquireUpgradeAt(t, f, 2, first.Add(100*LockLeaseTTL)) {
				t.Fatal("a second roll took a lock whose lease could not be evaluated.\n\n" +
					"An unreadable lease is not evidence the holder is dead. Reading it as " +
					"expiry hands the lock away during a mixed-version roll — the one window " +
					"where a pre-R7b broker's marker is exactly what is on disk.")
			}
		})
	}
}

// origin: prerelease audit round 2, H-2.
//
// THE ORCHESTRATOR CAN HAND THE LEASE BACK EXPLICITLY, so the re-run its HALT
// message promises does not have to wait out a 15-minute TTL. The MARKER must
// survive: the partial-roll fence on join/retire is the reason a HALT does not
// simply release the lock, and giving that up here would trade one bug for a worse
// one.
func TestExpiringTheLeaseAdmitsARerunButKeepsTheMembershipFence(t *testing.T) {
	f, _ := freshFSM(t, t.TempDir())
	first := time.Date(2026, 9, 2, 3, 0, 0, 0, time.UTC)
	if !acquireUpgradeAt(t, f, 1, first) {
		t.Fatal("the first acquire did not win an unheld lock")
	}
	before := upgradeLeaseOf(t, f)

	cmd, err := PlanExpireUpgradeLease(first.Add(time.Minute))
	if err != nil {
		t.Fatalf("plan expire: %v", err)
	}
	if _, ok := d7Apply(t, f, 2, cmd).(appliedOK); !ok {
		t.Fatal("expire-lease: want appliedOK")
	}
	if after := upgradeLeaseOf(t, f); after == before {
		t.Fatalf("the lease was not moved (still %q) — a HALTing orchestrator could not hand it back", after)
	}
	if UpgradeMarkerValue(f.db) != UpgradeMarkerStamp(first) {
		t.Fatal("expiring the lease also dropped the MARKER.\n\n" +
			"That unfences `cluster join`/`cluster retire` across a partial roll, which is the " +
			"exact hazard the lock exists for — and it is why a HALT keeps the marker instead " +
			"of releasing the lock outright.")
	}

	// Seconds later, not fifteen minutes: the whole point.
	if !acquireUpgradeAt(t, f, 3, first.Add(2*time.Minute)) {
		t.Fatal("the re-run was still refused after the previous roll handed its lease back")
	}

	// A LATE hand-back from an orchestrator that already released must not
	// resurrect anything — leaseRenewStmts is guarded on the marker existing.
	clear, err := PlanClearUpgradeActive()
	if err != nil {
		t.Fatalf("plan clear: %v", err)
	}
	if _, ok := d7Apply(t, f, 4, clear).(appliedOK); !ok {
		t.Fatal("release: want appliedOK")
	}
	late, err := PlanExpireUpgradeLease(first.Add(3 * time.Minute))
	if err != nil {
		t.Fatalf("plan late expire: %v", err)
	}
	if _, ok := d7Apply(t, f, 5, late).(appliedOK); !ok {
		t.Fatal("late expire-lease: want appliedOK")
	}
	if got := upgradeLeaseOf(t, f); got != "" {
		t.Fatalf("a late hand-back created a lease row (%q) against a released lock", got)
	}
}
