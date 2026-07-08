package broker

import (
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/proto"
)

// g5_upgrade_trigger_test.go — G5 #13 W2b: the account-signed remote-trigger is the security crux (a verify
// bug = a cluster-admin auth bypass), so the signature / target / replay / tamper / wrong-account gates are
// pinned exhaustively here.

func newTriggerTestBroker(selfID string, seed []byte, now time.Time, routed *[]adminsock.Request) *Broker {
	b := &Broker{selfID: selfID}
	b.cfg.AuthCallout = &AuthCalloutConfig{AccountSeed: seed}
	b.cfg.Now = func() time.Time { return now }
	b.clusterAdminHandle = func(r adminsock.Request) adminsock.Response {
		*routed = append(*routed, r)
		return adminsock.Response{OK: true, Reloading: true}
	}
	return b
}

func signUpgradeReq(t *testing.T, seed []byte, req *proto.ClusterUpgradeReq) []byte {
	t.Helper()
	sig, err := auth.SignWithSeed(seed, proto.CanonicalUpgradeReqBytes(req))
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	req.Sig = hex.EncodeToString(sig)
	data, _ := json.Marshal(req)
	return data
}

var triggerNow = time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)

func TestUpgradeTriggerValidReloadRouted(t *testing.T) {
	seed, _ := auth.GenerateUserSeed()
	var routed []adminsock.Request
	b := newTriggerTestBroker("brk-a", seed, triggerNow, &routed)
	req := &proto.ClusterUpgradeReq{Op: "reload", TargetNode: "brk-a", ToVersion: "v2", ExpectSHA256: "abc", IssuedAt: triggerNow.Format(time.RFC3339)}
	resp := b.handleUpgradeTrigger(signUpgradeReq(t, seed, req), triggerNow)
	if resp == nil || !resp.OK {
		t.Fatalf("a valid signed reload must route + succeed: %+v", resp)
	}
	if len(routed) != 1 || routed[0].Op != adminsock.OpBrokerUpgradeReload || routed[0].ToVersion != "v2" || routed[0].ExpectSHA256 != "abc" {
		t.Fatalf("must route a reload carrying the version + sha: %+v", routed)
	}
}

func TestUpgradeTriggerTransferRouted(t *testing.T) {
	seed, _ := auth.GenerateUserSeed()
	var routed []adminsock.Request
	b := newTriggerTestBroker("brk-a", seed, triggerNow, &routed)
	req := &proto.ClusterUpgradeReq{Op: "transfer-leader", TargetNode: "brk-a", TransferTo: "brk-b", IssuedAt: triggerNow.Format(time.RFC3339)}
	resp := b.handleUpgradeTrigger(signUpgradeReq(t, seed, req), triggerNow)
	if resp == nil || !resp.OK || resp.NewLeader != "brk-b" {
		t.Fatalf("a valid transfer must route + report the new leader: %+v", resp)
	}
	if len(routed) != 1 || routed[0].Op != adminsock.OpClusterTransfer || routed[0].NodeID != "brk-b" {
		t.Fatalf("must route OpClusterTransfer to brk-b: %+v", routed)
	}
}

func TestUpgradeTriggerWrongTargetSilent(t *testing.T) {
	seed, _ := auth.GenerateUserSeed()
	var routed []adminsock.Request
	b := newTriggerTestBroker("brk-a", seed, triggerNow, &routed)
	req := &proto.ClusterUpgradeReq{Op: "reload", TargetNode: "brk-OTHER", IssuedAt: triggerNow.Format(time.RFC3339)}
	if resp := b.handleUpgradeTrigger(signUpgradeReq(t, seed, req), triggerNow); resp != nil {
		t.Fatalf("a request for another broker must be SILENT (nil), got %+v", resp)
	}
	if len(routed) != 0 {
		t.Fatal("a wrong-target request must not be dispatched")
	}
}

func TestUpgradeTriggerBadSignatureRefused(t *testing.T) {
	seed, _ := auth.GenerateUserSeed()
	var routed []adminsock.Request
	b := newTriggerTestBroker("brk-a", seed, triggerNow, &routed)
	req := &proto.ClusterUpgradeReq{Op: "reload", TargetNode: "brk-a", IssuedAt: triggerNow.Format(time.RFC3339), Sig: "deadbeef"}
	data, _ := json.Marshal(req)
	resp := b.handleUpgradeTrigger(data, triggerNow)
	if resp == nil || resp.Code != upgradeTriggerCodeUnauthorized {
		t.Fatalf("a bad signature must be refused unauthorized: %+v", resp)
	}
	if len(routed) != 0 {
		t.Fatal("an unauthorized request must never be dispatched")
	}
}

func TestUpgradeTriggerWrongAccountRefused(t *testing.T) {
	brokerSeed, _ := auth.GenerateUserSeed()
	attackerSeed, _ := auth.GenerateUserSeed()
	var routed []adminsock.Request
	b := newTriggerTestBroker("brk-a", brokerSeed, triggerNow, &routed) // broker pins brokerSeed's pub
	req := &proto.ClusterUpgradeReq{Op: "reload", TargetNode: "brk-a", IssuedAt: triggerNow.Format(time.RFC3339)}
	data := signUpgradeReq(t, attackerSeed, req) // signed by a DIFFERENT account
	resp := b.handleUpgradeTrigger(data, triggerNow)
	if resp == nil || resp.Code != upgradeTriggerCodeUnauthorized {
		t.Fatalf("a request signed by a non-account key must be refused: %+v", resp)
	}
	if len(routed) != 0 {
		t.Fatal("a wrong-account request must never be dispatched")
	}
}

func TestUpgradeTriggerTamperedFieldRefused(t *testing.T) {
	seed, _ := auth.GenerateUserSeed()
	var routed []adminsock.Request
	b := newTriggerTestBroker("brk-a", seed, triggerNow, &routed)
	req := &proto.ClusterUpgradeReq{Op: "reload", TargetNode: "brk-a", ToVersion: "v2", IssuedAt: triggerNow.Format(time.RFC3339)}
	signUpgradeReq(t, seed, req) // stamps req.Sig over ToVersion=v2
	req.ToVersion = "v3-TAMPERED" // mutate AFTER signing; keep the old Sig
	data, _ := json.Marshal(req)
	resp := b.handleUpgradeTrigger(data, triggerNow)
	if resp == nil || resp.Code != upgradeTriggerCodeUnauthorized {
		t.Fatalf("a field tampered after signing must invalidate the signature: %+v", resp)
	}
}

func TestUpgradeTriggerStaleIssuedAtRefused(t *testing.T) {
	seed, _ := auth.GenerateUserSeed()
	var routed []adminsock.Request
	b := newTriggerTestBroker("brk-a", seed, triggerNow, &routed)
	old := triggerNow.Add(-time.Hour) // well outside upgradeTriggerSkew
	req := &proto.ClusterUpgradeReq{Op: "reload", TargetNode: "brk-a", IssuedAt: old.Format(time.RFC3339)}
	resp := b.handleUpgradeTrigger(signUpgradeReq(t, seed, req), triggerNow)
	if resp == nil || resp.Code != adminsock.CodeBadRequest {
		t.Fatalf("a stale issued_at must be refused (replay guard): %+v", resp)
	}
	if len(routed) != 0 {
		t.Fatal("a replayed request must never be dispatched")
	}
}

func TestUpgradeTriggerReexecAgentNoNID(t *testing.T) {
	seed, _ := auth.GenerateUserSeed()
	var routed []adminsock.Request
	b := newTriggerTestBroker("brk-a", seed, triggerNow, &routed) // no ColocatedAgentNID configured
	req := &proto.ClusterUpgradeReq{Op: "reexec-agent", TargetNode: "brk-a", SID: "lab", IssuedAt: triggerNow.Format(time.RFC3339)}
	resp := b.handleUpgradeTrigger(signUpgradeReq(t, seed, req), triggerNow)
	if resp == nil || resp.Code != adminsock.CodeBadRequest {
		t.Fatalf("reexec-agent without colocated_agent_nid must refuse bad_request: %+v", resp)
	}
	if len(routed) != 0 {
		t.Fatal("reexec-agent must NOT route through the admin backend (it forwards to the co-located agent)")
	}
}

func TestUpgradeTriggerNoAccountKeyRefused(t *testing.T) {
	seed, _ := auth.GenerateUserSeed()
	var routed []adminsock.Request
	b := newTriggerTestBroker("brk-a", seed, triggerNow, &routed)
	b.cfg.AuthCallout = nil // no account key to verify against
	req := &proto.ClusterUpgradeReq{Op: "reload", TargetNode: "brk-a", IssuedAt: triggerNow.Format(time.RFC3339)}
	resp := b.handleUpgradeTrigger(signUpgradeReq(t, seed, req), triggerNow)
	if resp == nil || resp.Code != adminsock.CodeClusterNotEnabled {
		t.Fatalf("no account key must fail-closed (not-enabled), not act: %+v", resp)
	}
}

// --- External-review B2: the cluster-scoped roll-lock trigger ops (acquire-lock / release-lock). ---

// TestUpgradeTriggerAcquireLockRequiresCluster: acquire-lock needs a raft node to Propose the marker; on a
// non-cluster broker it fail-closes (not-enabled) and never routes through the admin backend.
func TestUpgradeTriggerAcquireLockRequiresCluster(t *testing.T) {
	seed, _ := auth.GenerateUserSeed()
	var routed []adminsock.Request
	b := newTriggerTestBroker("brk-a", seed, triggerNow, &routed) // b.cl nil (no cluster wired)
	req := &proto.ClusterUpgradeReq{Op: "acquire-lock", TargetNode: "brk-a", IssuedAt: triggerNow.Format(time.RFC3339)}
	resp := b.handleUpgradeTrigger(signUpgradeReq(t, seed, req), triggerNow)
	if resp == nil || resp.Code != adminsock.CodeClusterNotEnabled {
		t.Fatalf("acquire-lock on a non-cluster broker must fail-closed not-enabled: %+v", resp)
	}
	if len(routed) != 0 {
		t.Fatal("acquire-lock must not route through the admin backend")
	}
}

// TestUpgradeTriggerAcquireLockBadSigRefused: the roll lock is a membership freeze — an attacker taking it
// without the account key would DoS grow/retire, so it must be signature-gated like every other op.
func TestUpgradeTriggerAcquireLockBadSigRefused(t *testing.T) {
	seed, _ := auth.GenerateUserSeed()
	var routed []adminsock.Request
	b := newTriggerTestBroker("brk-a", seed, triggerNow, &routed)
	req := &proto.ClusterUpgradeReq{Op: "acquire-lock", TargetNode: "brk-a", IssuedAt: triggerNow.Format(time.RFC3339), Sig: "deadbeef"}
	data, _ := json.Marshal(req)
	resp := b.handleUpgradeTrigger(data, triggerNow)
	if resp == nil || resp.Code != upgradeTriggerCodeUnauthorized {
		t.Fatalf("an unsigned acquire-lock must be refused unauthorized (no unauthenticated membership freeze): %+v", resp)
	}
}

// TestUpgradeTriggerAcquireLockRefusesInflightOp (round2 B2): a roll must not START while a membership op
// is already in flight — acquire-lock refuses (so the roll never overlaps an ongoing join/retire), and the
// refusal must NOT set the marker.
func TestUpgradeTriggerAcquireLockRefusesInflightOp(t *testing.T) {
	seed, _ := auth.GenerateUserSeed()
	var routed []adminsock.Request
	b := newTriggerTestBroker("brk-a", seed, triggerNow, &routed)
	n, _ := d7SingleNode(t, "brk-a")
	b.cl = &clusterRuntime{node: n}
	if err := n.Propose(func(_ *sql.DB) (*cluster.Command, error) {
		return cluster.PlanClusterOpStart(cluster.OpStartInput{
			OpID: "op-join-x", Kind: cluster.OpKindJoin, TargetNode: "brk-new",
			InitState: cluster.OpStateRosterCommitted, Timeline: `[{"s":"ROSTER_COMMITTED"}]`,
		}, triggerNow)
	}); err != nil {
		t.Fatalf("seed in-flight op: %v", err)
	}
	req := &proto.ClusterUpgradeReq{Op: "acquire-lock", TargetNode: "brk-a", IssuedAt: triggerNow.Format(time.RFC3339)}
	resp := b.handleUpgradeTrigger(signUpgradeReq(t, seed, req), triggerNow)
	if resp == nil || resp.OK || !strings.Contains(resp.Error, "in flight") {
		t.Fatalf("acquire-lock must refuse while a membership op is in flight: %+v", resp)
	}
	if upgradeActive(n.RODB()) {
		t.Fatal("a refused acquire-lock must NOT set the cluster_upgrade_active marker")
	}
}

// TestUpgradeTriggerLockOpsBypassHandleNilGate (M1 interaction): acquire-lock/release-lock do NOT need the
// admin backend, so a nil clusterAdminHandle (the re-exec startup window) must NOT reject them with the
// retriable cluster_not_ready — only reload/transfer-leader require the handle. With a real leader node
// wired, release-lock proposes the clear and succeeds (round2 B1: it is NOT best-effort — it surfaces
// Propose failures — but against a live leader it commits and replies OK).
func TestUpgradeTriggerLockOpsBypassHandleNilGate(t *testing.T) {
	seed, _ := auth.GenerateUserSeed()
	var routed []adminsock.Request
	b := newTriggerTestBroker("brk-a", seed, triggerNow, &routed)
	b.clusterAdminHandle = nil // admin backend not wired yet (post re-exec window)
	n, _ := d7SingleNode(t, "brk-a")
	b.cl = &clusterRuntime{node: n}
	req := &proto.ClusterUpgradeReq{Op: "release-lock", TargetNode: "brk-a", IssuedAt: triggerNow.Format(time.RFC3339)}
	resp := b.handleUpgradeTrigger(signUpgradeReq(t, seed, req), triggerNow)
	if resp == nil || resp.Code == upgradeTriggerCodeNotReady {
		t.Fatalf("release-lock must not be gated on the admin handle: %+v", resp)
	}
	if !resp.OK {
		t.Fatalf("release-lock against a live leader must commit and reply OK: %+v", resp)
	}
}
