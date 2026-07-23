package broker

import (
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/storage"
)

// g4_grow_trigger_test.go (G4 §B) — the account-signed grow trigger is a cluster-admin auth surface (a verify
// bug = a bypass), so the signature / target / replay / dispatch gates are pinned exhaustively, mirroring the
// G5 upgrade-trigger tests.

func newGrowTriggerTestBroker(selfID string, seed []byte, now time.Time, handle func(adminsock.Request) adminsock.Response) *Broker {
	b := &Broker{selfID: selfID}
	b.cfg.AuthCallout = &AuthCalloutConfig{AccountSeed: seed}
	b.cfg.Now = func() time.Time { return now }
	b.clusterAdminHandle = handle
	return b
}

func signGrowReq(t *testing.T, seed []byte, req *proto.ClusterGrowReq) []byte {
	t.Helper()
	sig, err := auth.SignWithSeed(seed, proto.CanonicalGrowReqBytes(req))
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	req.Sig = hex.EncodeToString(sig)
	data, _ := json.Marshal(req)
	return data
}

var growNow = time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)

func TestGrowTriggerApproveJoinRouted(t *testing.T) {
	seed, _ := auth.GenerateUserSeed()
	var routed []adminsock.Request
	b := newGrowTriggerTestBroker("brk-a", seed, growNow, func(r adminsock.Request) adminsock.Response {
		routed = append(routed, r)
		return adminsock.Response{OK: true, OpID: "op-xyz"}
	})
	req := &proto.ClusterGrowReq{Op: "approve-join", TargetNode: "brk-a", JoinerNode: "brk-b", JoinBundle: "tether-join:v1:...", IssuedAt: growNow.Format(time.RFC3339)}
	resp := b.handleGrowTrigger(signGrowReq(t, seed, req), growNow)
	if resp == nil || !resp.OK || resp.OpID != "op-xyz" {
		t.Fatalf("valid approve-join must route + return the op_id: %+v", resp)
	}
	if len(routed) != 1 || routed[0].Op != adminsock.OpClusterJoinApprove || routed[0].JoinBundle != "tether-join:v1:..." {
		t.Fatalf("must route OpClusterJoinApprove carrying the bundle: %+v", routed)
	}
}

func TestGrowTriggerRebalanceRouted(t *testing.T) {
	seed, _ := auth.GenerateUserSeed()
	var routed []adminsock.Request
	b := newGrowTriggerTestBroker("brk-a", seed, growNow, func(r adminsock.Request) adminsock.Response {
		routed = append(routed, r)
		return adminsock.Response{OK: true}
	})
	req := &proto.ClusterGrowReq{Op: "rebalance-proxy", TargetNode: "brk-a", IssuedAt: growNow.Format(time.RFC3339)}
	resp := b.handleGrowTrigger(signGrowReq(t, seed, req), growNow)
	if resp == nil || !resp.OK {
		t.Fatalf("valid rebalance-proxy must route + succeed: %+v", resp)
	}
	if len(routed) != 1 || routed[0].Op != adminsock.OpClusterRebalanceProxy {
		t.Fatalf("must route OpClusterRebalanceProxy: %+v", routed)
	}
}

func TestGrowTriggerWrongTargetSilent(t *testing.T) {
	seed, _ := auth.GenerateUserSeed()
	b := newGrowTriggerTestBroker("brk-a", seed, growNow, nil)
	req := &proto.ClusterGrowReq{Op: "approve-join", TargetNode: "brk-OTHER", IssuedAt: growNow.Format(time.RFC3339)}
	if resp := b.handleGrowTrigger(signGrowReq(t, seed, req), growNow); resp != nil {
		t.Fatalf("a request for another broker must be SILENT (nil), got %+v", resp)
	}
}

func TestGrowTriggerBadSignatureRefused(t *testing.T) {
	seed, _ := auth.GenerateUserSeed()
	b := newGrowTriggerTestBroker("brk-a", seed, growNow, nil)
	req := &proto.ClusterGrowReq{Op: "approve-join", TargetNode: "brk-a", IssuedAt: growNow.Format(time.RFC3339), Sig: "deadbeef"}
	data, _ := json.Marshal(req)
	resp := b.handleGrowTrigger(data, growNow)
	if resp == nil || resp.Code != growTriggerCodeUnauthorized {
		t.Fatalf("a bad signature must be refused unauthorized, got %+v", resp)
	}
}

func TestGrowTriggerWrongAccountRefused(t *testing.T) {
	seed, _ := auth.GenerateUserSeed()
	other, _ := auth.GenerateUserSeed()
	b := newGrowTriggerTestBroker("brk-a", seed, growNow, nil)
	req := &proto.ClusterGrowReq{Op: "approve-join", TargetNode: "brk-a", IssuedAt: growNow.Format(time.RFC3339)}
	// Signed with a DIFFERENT account seed than the broker's pinned one.
	resp := b.handleGrowTrigger(signGrowReq(t, other, req), growNow)
	if resp == nil || resp.Code != growTriggerCodeUnauthorized {
		t.Fatalf("a request signed by a non-cluster account must be refused, got %+v", resp)
	}
}

func TestGrowTriggerStaleIssuedAtRefused(t *testing.T) {
	seed, _ := auth.GenerateUserSeed()
	b := newGrowTriggerTestBroker("brk-a", seed, growNow, nil)
	stale := growNow.Add(-10 * time.Minute) // beyond the 5-min skew
	req := &proto.ClusterGrowReq{Op: "approve-join", TargetNode: "brk-a", IssuedAt: stale.Format(time.RFC3339)}
	resp := b.handleGrowTrigger(signGrowReq(t, seed, req), growNow)
	if resp == nil || resp.Code != adminsock.CodeBadRequest {
		t.Fatalf("a stale issued_at must be refused (replay guard), got %+v", resp)
	}
}

func TestGrowTriggerMalformedRefused(t *testing.T) {
	seed, _ := auth.GenerateUserSeed()
	b := newGrowTriggerTestBroker("brk-a", seed, growNow, nil)
	if resp := b.handleGrowTrigger([]byte("{not json"), growNow); resp == nil || resp.Code != adminsock.CodeBadRequest {
		t.Fatalf("malformed request must be bad_request, got %+v", resp)
	}
}

func TestGrowTriggerUnknownOpRefused(t *testing.T) {
	seed, _ := auth.GenerateUserSeed()
	b := newGrowTriggerTestBroker("brk-a", seed, growNow, func(adminsock.Request) adminsock.Response { return adminsock.Response{} })
	req := &proto.ClusterGrowReq{Op: "frobnicate", TargetNode: "brk-a", IssuedAt: growNow.Format(time.RFC3339)}
	resp := b.handleGrowTrigger(signGrowReq(t, seed, req), growNow)
	if resp == nil || resp.Code != adminsock.CodeBadRequest {
		t.Fatalf("an unknown op must be bad_request, got %+v", resp)
	}
}

func TestGrowTriggerLockOpsNeedCluster(t *testing.T) {
	seed, _ := auth.GenerateUserSeed()
	b := newGrowTriggerTestBroker("brk-a", seed, growNow, nil) // b.cl is nil (cluster not enabled)
	for _, op := range []string{"acquire-lock", "release-lock"} {
		req := &proto.ClusterGrowReq{Op: op, TargetNode: "brk-a", JoinerNode: "brk-b", IssuedAt: growNow.Format(time.RFC3339)}
		resp := b.handleGrowTrigger(signGrowReq(t, seed, req), growNow)
		if resp == nil || resp.Code != adminsock.CodeClusterNotEnabled {
			t.Fatalf("%s without a cluster must be cluster_not_enabled, got %+v", op, resp)
		}
	}
}

// --- cutover JS-store move-aside gating -----------------------------------------------------------------

func newCutoverTestBroker(t *testing.T) *Broker {
	t.Helper()
	b := &Broker{}
	b.cfg.ClusterDataDir = t.TempDir()
	b.cfg.Now = func() time.Time { return growNow }
	return b
}

// TestMeshPeerTriples (review m6): the §12.8 mesh-peers query returns one "server,route,bus_nkey" per
// route-mesh broker, EXCLUDING rows in a non-mesh phase or missing a bus_nkey_pub (identity not replicated).
func TestMeshPeerTriples(t *testing.T) {
	db, err := storage.Open(":memory:") // applies migrations → the real cluster_nodes schema
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	// {node_id, nats_server_id, nats_route, bus_nkey_pub, phase} — the query's inputs; other NOT NULL columns
	// get dummies.
	rows := [][5]string{
		{"brk1", "brk1", "nats://brk1:6222", "Ubrk1", "VOTER"},       // included
		{"brk2", "brk2", "nats://brk2:6222", "Ubrk2", "CATCHING_UP"}, // included (a joining member is in the mesh)
		{"brk3", "brk3", "nats://brk3:6222", "", "VOTER"},            // EXCLUDED: bus_nkey not replicated yet
		{"brk4", "brk4", "nats://brk4:6222", "Ubrk4", "RETIRING"},    // EXCLUDED: not a mesh phase
	}
	for _, r := range rows {
		if _, err := db.Exec(`INSERT INTO cluster_nodes(node_id,name,node_ident_pub,nats_server_id,raft_addr,nats_route,tunnel_addr,public_host,cert_fp,phase,added_at,bus_nkey_pub) `+
			`VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`, r[0], r[0], "Ni"+r[0], r[1], r[0]+":7400", r[2], r[0]+":7000", r[0], "fp", r[4], "2026-07-08T00:00:00Z", r[3]); err != nil {
			t.Fatal(err)
		}
	}
	peers, err := meshPeerTriples(db)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"brk1,nats://brk1:6222,Ubrk1", "brk2,nats://brk2:6222,Ubrk2"}
	if len(peers) != len(want) {
		t.Fatalf("expected exactly the 2 eligible mesh triples, got %v", peers)
	}
	for i := range want {
		if peers[i] != want[i] {
			t.Fatalf("triple %d: got %q want %q", i, peers[i], want[i])
		}
	}
}

// TestPerformGrowCutoverRefusesRecoveredResidue pins the A5-min WIRING, not just its discriminator. The
// internal review (M7) found that NO test invoked performGrowCutover at all, so deleting the guard reddened
// nothing. Shape: the survivor's conf is CLUSTERED on disk, no nats is live (the probe fails ⇒ not
// live-clustered), and THIS grow epoch has no reset evidence — i.e. recovered clustered residue (the
// online-force-single / racknerd shape). The cutover must REFUSE LOUDLY and name the de-cluster remedy; it
// must never report AlreadyDone nor fall through to the Stage-B restart, either of which would carry a stale
// single-node JS meta into the 1->2 grow and wedge it.
func TestPerformGrowCutoverRefusesRecoveredResidue(t *testing.T) {
	b := newCutoverTestBroker(t)
	b.cfg.Logger = silentLogger()
	n, _ := d7SingleNode(t, "brk-a")
	b.cl = &clusterRuntime{node: n}
	b.cfg.NatsConfPath = writeConfFile(t, g2ClusteredConf)

	resp := b.performGrowCutover(&proto.ClusterGrowReq{Op: "mesh-cutover", TargetNode: "brk-a", GrowEpoch: "op-epoch1"})
	if resp == nil || resp.OK || resp.AlreadyDone {
		t.Fatalf("recovered clustered residue must be REFUSED — never OK/AlreadyDone (that silently absorbs the "+
			"stale meta the guard exists to catch): %+v", resp)
	}
	if resp.Code != adminsock.CodeBadRequest {
		t.Fatalf("residue refusal Code = %q, want %q (err=%s)", resp.Code, adminsock.CodeBadRequest, resp.Error)
	}
	for _, want := range []string{"residue", "--to-standalone", "--reset-js"} {
		if !strings.Contains(resp.Error, want) {
			t.Fatalf("the refusal must be ACTIONABLE and name the remedy (missing %q): %s", want, resp.Error)
		}
	}

	// The with-evidence direction is deliberately NOT driven through performGrowCutover here: past this guard
	// the call enters restartAndVerifyClustered, whose deadline loop reads b.cfg.Now(), and this fixture's
	// clock is FROZEN (newCutoverTestBroker) — it would spin forever. That direction is pinned one layer down
	// by TestGrowCutoverThisEpochEvidence (evidence present ⇒ discriminator true ⇒ this `if` cannot fire).
}

// TestGrowCutoverThisEpochEvidence pins the R16 A5-min residue discriminator (M2): evidence that THIS grow
// epoch reset the store must be EPOCH-SPECIFIC. A store carrying only a DIFFERENT epoch's grow-bak (every
// ex-joiner/ex-former-N1 carries one forever) is NOT evidence for this grow — otherwise the guard silently
// absorbs recovered residue. Evidence forms: the per-epoch .grow-cutover-<epoch>.done sentinel OR the
// <store>.grow-bak.<epoch> backup for THIS epoch.
func TestGrowCutoverThisEpochEvidence(t *testing.T) {
	b := newCutoverTestBroker(t)
	store := filepath.Join(t.TempDir(), "jetstream")
	if err := os.MkdirAll(store, 0o700); err != nil {
		t.Fatal(err)
	}
	if b.growCutoverThisEpochEvidence(store, "epochA") {
		t.Fatal("no evidence for epochA yet ⇒ must read false (a clustered survivor here = residue → refuse)")
	}
	if b.growCutoverThisEpochEvidence(store, "") {
		t.Fatal("an empty epoch can never be proven ⇒ false (refuse loudly)")
	}
	// A grow cutover moved it aside under epochA (creates the epochA backup + sentinel).
	if _, err := b.moveAsideJetStreamStore(store, "epochA", false); err != nil {
		t.Fatalf("move aside: %v", err)
	}
	if !b.growCutoverThisEpochEvidence(store, "epochA") {
		t.Fatal("after epochA's cutover, epochA must read as evidenced")
	}
	// M2 KEYSTONE: a DIFFERENT epoch (epochB) sees NO this-epoch evidence despite epochA's lingering backup —
	// so a recovered residue survivor whose store carries only an ancient grow-bak is still refused.
	if b.growCutoverThisEpochEvidence(store, "epochB") {
		t.Fatal("epochA's backup must NOT count as evidence for epochB (else A5-min silently absorbs residue)")
	}
	// The absent-store sentinel path: markGrowCutoverEpoch alone (no backup) is sufficient evidence.
	b.markGrowCutoverEpoch("epochC")
	if !b.growCutoverThisEpochEvidence(store, "epochC") {
		t.Fatal("the per-epoch sentinel alone must count (the absent-store grow's resume must not be falsely refused)")
	}
}

func TestMoveAsideJetStreamStore_EmptyMoves(t *testing.T) {
	b := newCutoverTestBroker(t)
	store := filepath.Join(t.TempDir(), "jetstream")
	if err := os.MkdirAll(store, 0o700); err != nil {
		t.Fatal(err)
	}
	backup, err := b.moveAsideJetStreamStore(store, "epoch1", false)
	if err != nil {
		t.Fatalf("an empty store must move without an ack: %v", err)
	}
	if backup == "" {
		t.Fatal("expected a backup path")
	}
	if _, serr := os.Stat(backup); serr != nil {
		t.Fatalf("backup dir must exist at %s: %v", backup, serr)
	}
	if _, serr := os.Stat(store); serr != nil {
		t.Fatal("a fresh empty store dir must be recreated for the clustered boot")
	}
}

func TestMoveAsideJetStreamStore_NonEmptyRefusesWithoutAck(t *testing.T) {
	b := newCutoverTestBroker(t)
	store := filepath.Join(t.TempDir(), "jetstream")
	if err := os.MkdirAll(filepath.Join(store, "$G"), 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := b.moveAsideJetStreamStore(store, "epoch1", false); err == nil {
		t.Fatal("a NON-EMPTY (data-bearing) store must REFUSE without --reset-former-js (no silent data loss)")
	}
	// The store must be untouched by the refusal.
	if _, serr := os.Stat(filepath.Join(store, "$G")); serr != nil {
		t.Fatal("a refused reset must not touch the data-bearing store")
	}
	// With the ack it moves.
	if _, err := b.moveAsideJetStreamStore(store, "epoch1", true); err != nil {
		t.Fatalf("with --reset-former-js the non-empty store must move: %v", err)
	}
}

func TestMoveAsideJetStreamStore_IdempotentPerEpoch(t *testing.T) {
	b := newCutoverTestBroker(t)
	store := filepath.Join(t.TempDir(), "jetstream")
	if err := os.MkdirAll(store, 0o700); err != nil {
		t.Fatal(err)
	}
	backup1, err := b.moveAsideJetStreamStore(store, "epochA", false)
	if err != nil {
		t.Fatal(err)
	}
	// A second call for the SAME epoch must be a no-op (sentinel present) — never double-move a fresh store —
	// but C3: it must still REPORT the backup path (was "", which dropped resp.BackupPath's restore hint on
	// the common SIGKILL-lost-reply resume). Same path, no new move.
	backup2, err := b.moveAsideJetStreamStore(store, "epochA", false)
	if err != nil {
		t.Fatalf("a same-epoch re-run must be an idempotent no-op, got err=%v", err)
	}
	if backup2 != backup1 {
		t.Fatalf("C3: the idempotent re-run must report the SAME backup path %q, got %q", backup1, backup2)
	}
}

// TestMoveAsideJetStreamStore_CrashWindowBackupExists (review M3): a crash between the rename+recreate and the
// sentinel write leaves the backup dir holding the DATA + an empty recreated store + NO sentinel. On resume,
// keying idempotency on the DURABLE backup dir (not the lost sentinel) must be a no-op — NOT a second rename
// (which would ENOTEMPTY-wedge the leader forever on the data-bearing racknerd-N=1 case).
func TestMoveAsideJetStreamStore_CrashWindowBackupExists(t *testing.T) {
	b := newCutoverTestBroker(t)
	dir := t.TempDir()
	store := filepath.Join(dir, "jetstream")
	backup := store + ".grow-bak.epochX"
	// Simulate the post-crash state: backup has the original data, store recreated empty, sentinel ABSENT.
	if err := os.MkdirAll(filepath.Join(backup, "$G"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(store, 0o700); err != nil {
		t.Fatal(err)
	}
	got, err := b.moveAsideJetStreamStore(store, "epochX", false)
	if err != nil {
		t.Fatalf("resume with a durable backup present must be a no-op, not error (M3 ENOTEMPTY wedge): %v", err)
	}
	// C3: a no-op resume must REPORT the existing backup path (was "" — which dropped the operator's
	// restore-source hint on the common SIGKILL-lost-reply cutover resume), without touching the backup.
	if got != backup {
		t.Fatalf("C3: no-op resume must report the existing backup path %q, got %q", backup, got)
	}
	if _, serr := os.Stat(filepath.Join(backup, "$G")); serr != nil {
		t.Fatal("the data-bearing backup must NOT be touched on a no-op resume")
	}
}

// TestMoveAsideJetStreamStore_ReadDirErrorFailsClosed (review m4): a store whose contents cannot be enumerated
// must be treated as POTENTIALLY data-bearing (require ack), never silently reset as empty. Simulated by
// making storeDir a NON-directory the ReadDir path rejects... but the Stat-isDir guard catches that first, so
// instead make the store unreadable. Skips as root (root ignores the mode).
func TestMoveAsideJetStreamStore_ReadDirErrorFailsClosed(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory read permissions — cannot exercise the ReadDir-error path")
	}
	b := newCutoverTestBroker(t)
	store := filepath.Join(t.TempDir(), "jetstream")
	if err := os.MkdirAll(store, 0o300); err != nil { // write+exec, NOT read → ReadDir fails
		t.Fatal(err)
	}
	defer func() { _ = os.Chmod(store, 0o700) }()
	if _, err := b.moveAsideJetStreamStore(store, "epochR", false); err == nil {
		t.Fatal("a store that cannot be enumerated must REFUSE without ack (fail-closed), not reset silently")
	}
}
