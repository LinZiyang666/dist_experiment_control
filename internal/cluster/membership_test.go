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

// d7_membership_test.go — D7 §8.1 cheap unit tests (make test): Plan/applier join-PoP
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
	in1.BusNkey = "UGOODBUSNKEY"
	in1.CertFP = "sha256:goodfp"
	cmd1, err := PlanClusterNodeUpsert(in1)
	if err != nil {
		t.Fatalf("plan admit: %v", err)
	}
	if _, ok := d7Apply(t, f, 1, cmd1).(appliedOK); !ok {
		t.Fatal("admit: want appliedOK")
	}
	if bn := d7Col(t, f, node, "bus_nkey_pub"); bn != "UGOODBUSNKEY" {
		t.Fatalf("after admit bus_nkey_pub=%q, want UGOODBUSNKEY", bn)
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
	if bn := d7Col(t, f, node, "bus_nkey_pub"); bn != "UGOODBUSNKEY" {
		t.Fatalf("empty re-admit CLOBBERED bus_nkey_pub to %q — must preserve UGOODBUSNKEY (re-arms the deadlock)", bn)
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
