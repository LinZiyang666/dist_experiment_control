package broker

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/brokerupgrade"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/node"
	"github.com/LinZiyang666/tether/internal/proc"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradeartifact"
	"github.com/LinZiyang666/tether/internal/upgradeport"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/internal/upgradewire"
	"github.com/nats-io/nats.go"
)

// upgrade_trigger_test.go (formerly g5_upgrade_trigger_test.go) — G5 #13 W2b: the account-signed remote-trigger is the security crux (a verify
// bug = a cluster-admin auth bypass), so the signature / target / replay / tamper / wrong-account gates are
// pinned exhaustively here. The S8 entries (signed transaction action, agent transaction hop, register and
// farewell authority, live cut role) are pinned at the bottom against the real broker handlers.

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
	// origin: S8 internal review rv3-leaves F2
	// A valid account signature authenticates the caller, not an S8 cut grant.
	if resp == nil || resp.OK || resp.Code != "try_again" || resp.Error != "legacy_quiescence_unproved" {
		t.Fatalf("a valid unsigned-cut reload must pass legacy policy: %+v", resp)
	}
	if len(routed) != 0 {
		t.Fatalf("unfenced legacy reload reached execution: %+v", routed)
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
	signUpgradeReq(t, seed, req)  // stamps req.Sig over ToVersion=v2
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
	// A12: an UNSET colocated_agent_nid no longer HARD-REFUSES at the config gate — it falls back to
	// b.selfID (the node_id==nid convention detect/plan already use) so the one-command roll keeps working
	// on a host that left the field unset. With no live nats.Conn in this unit broker, the fallback path
	// then stops at the "broker not connected" check — proving it got PAST the old bad_request gate.
	// Assert on the Error too: CodeClusterNotEnabled is ALSO the account-key-missing gate's code, so keying
	// only on the code could false-pass if signature/seed setup broke. "broker not connected" is specific to
	// the reexec-agent nil-nc guard reached AFTER the selfID fallback — proving the config gate was passed.
	if resp == nil || resp.Code != adminsock.CodeClusterNotEnabled || !strings.Contains(resp.Error, "broker not connected") {
		t.Fatalf("reexec-agent without colocated_agent_nid must fall back to selfID and reach the nc check (cluster_not_enabled / broker not connected), not refuse at the config gate: %+v", resp)
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

// --- S8 broker upgrade entries. origin: hot-update S8 plan §11.2 P15 ---------------------------------

// upgradeEndpointSpy stands in for the bound broker engine. It proves which operation an entry
// dispatched and with which key; it never signs a cut.
type upgradeEndpointSpy struct {
	calls atomic.Int64
	key   upgradetx.Key
}

func (e *upgradeEndpointSpy) Prepare(_ context.Context, key upgradetx.Key, req upgradetx.PrepareRequest) (upgradetx.Grant, error) {
	e.calls.Add(1)
	if key != e.key {
		return upgradetx.Grant{}, errors.New("dispatched with a foreign key")
	}
	return upgradetx.Grant{Handle: upgradetx.Handle{Key: key, TxID: strings.Repeat("1", 32)}, ActorFP: req.ActorFP}, nil
}
func (e *upgradeEndpointSpy) Start(context.Context, upgradetx.Grant, upgradeartifact.Receipt) (upgradetx.View, error) {
	e.calls.Add(1)
	return upgradetx.View{}, errors.New("spy never cuts")
}
func (e *upgradeEndpointSpy) Query(context.Context, upgradetx.Key, string, string) (upgradetx.View, error) {
	e.calls.Add(1)
	return upgradetx.View{}, errors.New("spy has no history")
}
func (e *upgradeEndpointSpy) Rollback(context.Context, upgradetx.Fence, upgradetx.ReverseRequest) (upgradetx.View, error) {
	e.calls.Add(1)
	return upgradetx.View{}, errors.New("spy never cuts")
}
func (e *upgradeEndpointSpy) Cancel(context.Context, upgradetx.Handle) (upgradetx.View, error) {
	e.calls.Add(1)
	return upgradetx.View{}, nil
}
func (e *upgradeEndpointSpy) Archive(context.Context, upgradetx.Handle) (upgradetx.View, error) {
	e.calls.Add(1)
	return upgradetx.View{}, nil
}
func (e *upgradeEndpointSpy) CloseWindow(context.Context, upgradetx.Handle) (upgradetx.View, error) {
	e.calls.Add(1)
	return upgradetx.View{}, nil
}

var _ upgradeport.Endpoint = (*upgradeEndpointSpy)(nil)

func s8Actor(t *testing.T) (actor, fp string) {
	t.Helper()
	seed, err := auth.GenerateUserSeed()
	if err != nil {
		t.Fatal(err)
	}
	if actor, err = auth.PublicKeyFromSeed(seed); err != nil {
		t.Fatal(err)
	}
	if fp, err = auth.FingerprintFromActor(actor); err != nil {
		t.Fatal(err)
	}
	return actor, fp
}

func s8PrepareRequest(key upgradetx.Key, actorFP string) upgradewire.Request {
	return upgradewire.Request{Op: upgradewire.OpPrepare, Key: key, Prepare: upgradetx.PrepareRequest{ClientNonce: "nonce", ActorFP: actorFP,
		SourceRelease: "v1", TargetRelease: "v2", Mode: upgradetx.ModeHot, SourceSHA: strings.Repeat("a", 64), TargetSHA: strings.Repeat("b", 64),
		CapabilityDigest: strings.Repeat("c", 64), ExpiresAt: time.Now().Add(time.Minute).UnixNano(),
		Scope: upgradetx.Scope{ID: "scope", Participants: []upgradetx.Key{key}, Domains: []upgradetx.Domain{upgradetx.DomainControl}}}}
}

func s8SignedTransaction(t *testing.T, seed []byte, target, actorFP string, request upgradewire.Request) []byte {
	t.Helper()
	body, err := upgradewire.EncodeRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	return signUpgradeReq(t, seed, &proto.ClusterUpgradeReq{Op: brokerupgrade.SignedTransactionOp, TargetNode: target, ActorFP: actorFP,
		Upgrade: body, IssuedAt: triggerNow.Format(time.RFC3339)})
}

// The signed transaction action passes the same signature/target/replay gate as every trigger op, then
// binds the SUBJECT's authenticated actor and this broker's registered key. An account signature is
// operator authority, not a grant: a different subject actor, a stale signature, an unwired endpoint or a
// lost roll holder never reaches the endpoint. The signature-only entry cannot serve the action at all.
// origin: hot-update S8 plan §2.2 / §6.3 / §7.1 / §11.2 P15
func TestUpgradeTriggerServesSignedTransactionOnlyForTheSubjectActor(t *testing.T) {
	seed, _ := auth.GenerateUserSeed()
	actor, fp := s8Actor(t)
	other, _ := s8Actor(t)
	key := upgradetx.Key{Component: upgradetx.ComponentBroker, DomainID: strings.Repeat("d", 32), ConfigPath: "/etc/tether/broker.yaml"}
	var routed []adminsock.Request
	for name, arm := range map[string]struct {
		subject string
		data    func() []byte
		unwired bool
		code    string
		calls   int64
		silent  bool
	}{
		"subject_actor": {subject: actor, data: func() []byte { return s8SignedTransaction(t, seed, "brk-a", fp, s8PrepareRequest(key, fp)) }, calls: 1},
		"other_subject": {subject: other, data: func() []byte { return s8SignedTransaction(t, seed, "brk-a", fp, s8PrepareRequest(key, fp)) }, code: proto.CodeNotOwner},
		"other_target":  {subject: actor, data: func() []byte { return s8SignedTransaction(t, seed, "brk-b", fp, s8PrepareRequest(key, fp)) }, silent: true},
		"forged_signature": {subject: actor, data: func() []byte {
			attacker, _ := auth.GenerateUserSeed()
			return s8SignedTransaction(t, attacker, "brk-a", fp, s8PrepareRequest(key, fp))
		}, code: upgradeTriggerCodeUnauthorized},
		"unwired_endpoint": {subject: actor, data: func() []byte { return s8SignedTransaction(t, seed, "brk-a", fp, s8PrepareRequest(key, fp)) }, unwired: true, code: upgradetx.CodeDeferred},
		"unrouted_subject": {subject: "", data: func() []byte { return s8SignedTransaction(t, seed, "brk-a", fp, s8PrepareRequest(key, fp)) }, code: proto.CodeNotOwner},
		"holder_not_current": {subject: actor, data: func() []byte {
			body, _ := upgradewire.EncodeRequest(upgradewire.Request{Op: upgradewire.OpCancel, Key: key, Expected: upgradetx.Fence{Current: upgradetx.Handle{
				Key: key, TxID: strings.Repeat("1", 32), CutID: strings.Repeat("2", 32), Generation: 2, Revision: 3}}})
			return signUpgradeReq(t, seed, &proto.ClusterUpgradeReq{Op: brokerupgrade.SignedTransactionOp, TargetNode: "brk-a", ActorFP: fp,
				Upgrade: body, Holder: "roll-holder", IssuedAt: triggerNow.Format(time.RFC3339)})
		}, code: upgradetx.CodeDeferred},
	} {
		t.Run(name, func(t *testing.T) {
			b := newTriggerTestBroker("brk-a", seed, triggerNow, &routed)
			spy := &upgradeEndpointSpy{key: key}
			b.upgradeKey = key
			if !arm.unwired {
				b.upgradeEndpoint = spy
			}
			subject := proto.SubjCtrlClusterUpgrade(arm.subject)
			if arm.subject == "" {
				subject = "tether.v2.ctrl.cluster-upgrade.req"
			}
			resp := handleUpgradeTxTrigger(b, subject, arm.data(), triggerNow)
			if arm.silent {
				if resp != nil || spy.calls.Load() != 0 {
					t.Fatalf("another broker's transaction was answered: %+v", resp)
				}
				return
			}
			if resp == nil || resp.Code != arm.code || resp.OK != (arm.code == "") || spy.calls.Load() != arm.calls {
				t.Fatalf("%s: resp=%+v calls=%d", name, resp, spy.calls.Load())
			}
			if arm.code == "" {
				response, err := upgradewire.DecodeResponse(resp.Upgrade)
				if err != nil || response.View.Historical.Grant.ActorFP != fp || response.View.Historical.Key != key {
					t.Fatalf("transaction reply lost its grant: %s %v", resp.Upgrade, err)
				}
			}
		})
	}
	if len(routed) != 0 {
		t.Fatalf("transactions reached the legacy admin backend: %+v", routed)
	}
	// The signature-only entry has no authenticated actor; it must not serve the action.
	b := newTriggerTestBroker("brk-a", seed, triggerNow, &routed)
	spy := &upgradeEndpointSpy{key: key}
	b.upgradeEndpoint, b.upgradeKey = spy, key
	if resp := b.handleUpgradeTrigger(s8SignedTransaction(t, seed, "brk-a", fp, s8PrepareRequest(key, fp)), triggerNow); resp == nil || resp.OK || spy.calls.Load() != 0 {
		t.Fatalf("actor-less trigger entry served a transaction: %+v", resp)
	}
}

// s8TxBus wires the real agent transaction hop on an embedded NATS server: the broker handler on the
// ctl subject, a fake agent on the forwarded subject (counting deliveries) and the owning actor's ctl.
type s8TxBus struct {
	b              *Broker
	actor, fp      string
	forwarded      atomic.Int64
	ctl, sharedCtl *nats.Conn
	key            upgradetx.Key
	url            string
}

func newS8TxBus(t *testing.T) *s8TxBus {
	t.Helper()
	bus := &s8TxBus{url: testharness.StartNATS(t)}
	bus.actor, bus.fp = s8Actor(t)
	db := openDB(t)
	if _, err := session.Create(db, "lab", "lab", bus.fp, "pin-hash", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := node.Register(db, node.RegisterInput{SID: "lab", NID: "node", ProtoVersion: proto.ProtoVersion}, time.Now()); err != nil {
		t.Fatal(err)
	}
	bus.b = &Broker{cfg: Config{DB: db, Logger: silentLogger(), Now: time.Now, UpgradeForwardTimeoutDur: 2 * time.Second}}
	bus.key = upgradetx.Key{Component: upgradetx.ComponentAgent, DomainID: strings.Repeat("d", 32), SID: "lab", ConfiguredNID: "node"}
	connect := func(opts ...nats.Option) *nats.Conn {
		nc, err := nats.Connect(bus.url, opts...)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(nc.Close)
		return nc
	}
	brokerNC, agentNC := connect(), connect()
	bus.b.nc.Store(brokerNC)
	bus.ctl, bus.sharedCtl = connect(nats.CustomInboxPrefix(auth.InboxPrefixFor(bus.actor))), connect()
	if _, err := brokerNC.Subscribe(proto.SubjectPrefix+".s.*.cmd.by.*.node.*.upgrade-tx.req", func(m *nats.Msg) { handleUpgradeTxReq(bus.b, brokerNC, m) }); err != nil {
		t.Fatal(err)
	}
	reply, _ := upgradewire.EncodeResponse(upgradewire.Response{View: upgradetx.View{Historical: upgradetx.Record{Key: bus.key}}})
	if _, err := agentNC.Subscribe(proto.SubjCmdForwarded("lab", "node", brokerupgrade.SignedTransactionOp), func(m *nats.Msg) {
		bus.forwarded.Add(1)
		_ = m.Respond(reply)
	}); err != nil {
		t.Fatal(err)
	}
	for _, nc := range []*nats.Conn{brokerNC, agentNC} {
		if err := nc.FlushTimeout(time.Second); err != nil {
			t.Fatal(err)
		}
	}
	return bus
}

func (bus *s8TxBus) send(t *testing.T, ctl *nats.Conn, actor, actorFP string) upgradewire.Response {
	t.Helper()
	body, err := upgradewire.EncodeRequest(s8PrepareRequest(bus.key, actorFP))
	if err != nil {
		t.Fatal(err)
	}
	msg, err := ctl.Request(proto.SubjCmdBy("lab", actor, "node", brokerupgrade.SignedTransactionOp), body, 5*time.Second)
	if err != nil {
		t.Fatalf("transaction hop did not answer: %v", err)
	}
	response, err := upgradewire.DecodeResponse(msg.Data)
	if err != nil {
		t.Fatalf("hop reply is not a transaction envelope: %q %v", msg.Data, err)
	}
	return response
}

// The agent transaction hop is an upgrade entry like the old wire: owner admission first, then the shared
// legacy policy for any target that has not advertised the S8 route (busy 64 / unfenced 75), and only a
// capable target on the owner's own private reply root ever receives a forwarded request.
// origin: hot-update S8 plan §7.1 / §7.2 / §11.2 P15 (old-request direct-forward mutant)
func TestAgentUpgradeTransactionHopAdmitsOwnerThenPolicy(t *testing.T) {
	exerciseAgentUpgradeTransactionHop(t)
}

func exerciseAgentUpgradeTransactionHop(t *testing.T) {
	t.Helper()
	bus := newS8TxBus(t)
	if got := bus.send(t, bus.ctl, bus.actor, bus.fp); got.Code != upgradetx.CodeDeferred || bus.forwarded.Load() != 0 {
		t.Fatalf("legacy idle target: %+v forwarded=%d", got, bus.forwarded.Load())
	}
	if err := proc.Insert(bus.b.cfg.DB, proc.Process{PID: "pid", SID: "lab", NID: "node", Argv: []string{"sleep", "60"}, StartedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if got := bus.send(t, bus.ctl, bus.actor, bus.fp); got.Code != upgradetx.CodeRejected || got.Error != "legacy_busy" || bus.forwarded.Load() != 0 {
		t.Fatalf("legacy busy target: %+v forwarded=%d", got, bus.forwarded.Load())
	}
	if err := brokerupgrade.RememberAgentTransaction(bus.b.read().SQL(), func(plan func(*sql.DB) (*cluster.Command, error)) error {
		return maintenanceWrite(bus.b, plan)
	}, "lab", "node", []string{proto.CapUpgradeTXV1}); err != nil {
		t.Fatal(err)
	}
	if got := bus.send(t, bus.ctl, bus.actor, bus.fp); got.Code != "" || got.View.Historical.Key != bus.key || bus.forwarded.Load() != 1 {
		t.Fatalf("capable target: %+v forwarded=%d", got, bus.forwarded.Load())
	}
	if got := bus.send(t, bus.sharedCtl, bus.actor, bus.fp); got.Code != upgradetx.CodeRejected || bus.forwarded.Load() != 1 {
		t.Fatalf("shared-inbox requester was forwarded: %+v forwarded=%d", got, bus.forwarded.Load())
	}
	stranger, strangerFP := s8Actor(t)
	strangerCtl, err := nats.Connect(bus.url, nats.CustomInboxPrefix(auth.InboxPrefixFor(stranger)))
	if err != nil {
		t.Fatal(err)
	}
	defer strangerCtl.Close()
	if got := bus.send(t, strangerCtl, stranger, strangerFP); got.Code != "not_owner" || bus.forwarded.Load() != 1 {
		t.Fatalf("non-owner was forwarded: %+v forwarded=%d", got, bus.forwarded.Load())
	}
}

// Register and farewell carry the session's creation authority: an image whose inherited incarnation
// names another creation can neither register nor release (offline) a row, while the ordinary register
// reply teaches the actual incarnation. P16 adds the same comparison inside the writer transactions.
// origin: hot-update S8 plan §6.2 / §11.2 P15 (adjudicateLease and farewell call sites)
func TestRegisterAndFarewellCarryTheSessionCreationAuthority(t *testing.T) {
	url := testharness.StartNATS(t)
	db := openDB(t)
	hash, err := auth.HashPIN("pin")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.Create(db, "lab", "lab", "SHA256:owner", hash, time.Now()); err != nil {
		t.Fatal(err)
	}
	actual, err := brokerupgrade.RegisterAuthority(db, "lab", "")
	if err != nil || actual == "" {
		t.Fatalf("fixture creation authority: %q %v", actual, err)
	}
	stale := strings.Repeat("e", 64)
	b := &Broker{cfg: Config{DB: db, Logger: silentLogger(), Now: time.Now}}
	brokerNC, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer brokerNC.Close()
	b.nc.Store(brokerNC)
	if _, err := brokerNC.Subscribe(proto.SubjectPrefix+".ctrl.s.*.node.*.register.req", b.handleRegister); err != nil {
		t.Fatal(err)
	}
	agentNC, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer agentNC.Close()
	if err := brokerNC.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	register := func(req proto.NodeRegisterReq) proto.NodeRegisterResp {
		t.Helper()
		req.ProtoVersion, req.NID = proto.ProtoVersion, "gpu-02"
		body, _ := json.Marshal(req)
		msg, err := agentNC.Request(proto.SubjNodeRegister("lab", "gpu-02"), body, 5*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		var resp proto.NodeRegisterResp
		if err := json.Unmarshal(msg.Data, &resp); err != nil {
			t.Fatal(err)
		}
		return resp
	}
	status := func() string {
		var s string
		if err := db.QueryRow(`SELECT status FROM nodes WHERE sid='lab' AND nid='gpu-02'`).Scan(&s); err != nil {
			return "absent"
		}
		return s
	}
	if resp := register(proto.NodeRegisterReq{Incarnation: stale}); resp.OK || resp.Code != proto.CodeSessionIncarnationMismatch || status() != "absent" {
		t.Fatalf("stale image registered: %+v row=%s", resp, status())
	}
	if resp := register(proto.NodeRegisterReq{}); !resp.OK || resp.Incarnation != actual || status() != string(node.StateOnline) {
		t.Fatalf("ordinary register did not learn the creation: %+v row=%s", resp, status())
	}
	if resp := register(proto.NodeRegisterReq{Incarnation: actual}); !resp.OK || resp.Incarnation != actual {
		t.Fatalf("matching image refused: %+v", resp)
	}
	farewell := proto.NodeRegisterReq{ReleasingName: true, RosterRefreshOnly: true, InstanceID: testInstanceA}
	farewell.Incarnation = stale
	if resp := register(farewell); !resp.OK || status() != string(node.StateOnline) {
		t.Fatalf("a recreated session's process released the live row: %+v row=%s", resp, status())
	}
	farewell.Incarnation = actual
	if resp := register(farewell); !resp.OK || status() != string(node.StateOffline) {
		t.Fatalf("the holder's own farewell did not release its lease row: %+v row=%s", resp, status())
	}
}

// The cut role is read from the CURRENT cluster runtime at every gated step: an admission sample is not
// authority after a membership operation starts, and an image activation that replaces b.cl must be seen.
// origin: hot-update S8 plan §6.3 / §8.2 F8 / §11.2 P15 (cached-role mutant)
func TestBrokerCutRoleIsReadFromTheCurrentClusterRuntime(t *testing.T) {
	// origin: hot-update S10 plan §11.1 / TEST-BUDGET
	if runIsolatedBrokerTest(t) {
		return
	}
	b := &Broker{}
	if err := freshBrokerRole(b); err != nil {
		t.Fatalf("single broker refused: %v", err)
	}
	n, _ := d7SingleNode(t, "brk-a")
	b.cl = &clusterRuntime{node: n}
	inner := &cutRuntimeSpy{}
	gated := brokerupgrade.GateRuntime(inner, func() error { return freshBrokerRole(b) })
	if _, err := gated.Freeze(upgradetx.Handle{}); err != nil || inner.freezes != 1 {
		t.Fatalf("sole-voter leader could not cut: %v freezes=%d", err, inner.freezes)
	}
	if err := n.Propose(func(_ *sql.DB) (*cluster.Command, error) {
		return cluster.PlanClusterOpStart(cluster.OpStartInput{OpID: "op-join-s8", Kind: cluster.OpKindJoin, TargetNode: "brk-new",
			InitState: cluster.OpStateRosterCommitted, Timeline: `[{"s":"ROSTER_COMMITTED"}]`}, triggerNow)
	}); err != nil {
		t.Fatal(err)
	}
	var refused *upgradetx.Error
	if _, err := gated.Activate(upgradeport.Cut{}); !errors.As(err, &refused) || refused.Code != upgradetx.CodeDeferred || inner.activations != 0 {
		t.Fatalf("membership change after admission still activated: %v activations=%d", err, inner.activations)
	}
	if _, err := gated.Preflight(t.Context(), upgradetx.Grant{}, upgradeartifact.Receipt{}); err == nil || inner.preflights != 0 {
		t.Fatalf("membership change after admission still preflighted: %v", err)
	}
	clean, _ := d7SingleNode(t, "brk-b")
	b.cl = &clusterRuntime{node: clean}
	if _, err := gated.Freeze(upgradetx.Handle{}); err != nil || inner.freezes != 2 {
		t.Fatalf("the gate kept answering for the replaced runtime: %v freezes=%d", err, inner.freezes)
	}
}

type cutRuntimeSpy struct {
	upgradeport.RuntimePort
	preflights, freezes, activations int
}

func (s *cutRuntimeSpy) Preflight(context.Context, upgradetx.Grant, upgradeartifact.Receipt) (upgradeport.PreflightProof, error) {
	s.preflights++
	return upgradeport.PreflightProof{}, nil
}
func (s *cutRuntimeSpy) Freeze(upgradetx.Handle) (upgradeport.Cut, error) {
	s.freezes++
	return upgradeport.Cut{}, nil
}
func (s *cutRuntimeSpy) Activate(upgradeport.Cut) (upgradeport.Activation, error) {
	s.activations++
	return upgradeport.Activation{}, nil
}

// The broker's S8 entries start no goroutine or descriptor that outlives a request: five rounds of the
// real agent hop plus the signed transaction action over the real trigger subscription.
// origin: hot-update S8 plan §11.2 P15 (broker leak anchor)
func TestBrokerUpgradeEntriesLeakNothing(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseBrokerUpgradeEntries(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "broker upgrade entries x5", g)
	testharness.AssertNoFDLeak(t, "broker upgrade entries x5", fd, 0)
}

func exerciseBrokerUpgradeEntries(t *testing.T) {
	t.Helper()
	exerciseAgentUpgradeTransactionHop(t)
	seed, _ := auth.GenerateUserSeed()
	actor, fp := s8Actor(t)
	key := upgradetx.Key{Component: upgradetx.ComponentBroker, DomainID: strings.Repeat("d", 32), ConfigPath: "/etc/tether/broker.yaml"}
	var routed []adminsock.Request
	b := newTriggerTestBroker("brk-a", seed, time.Now(), &routed)
	b.cfg.Now = time.Now
	spy := &upgradeEndpointSpy{key: key}
	b.upgradeEndpoint, b.upgradeKey = spy, key
	url := testharness.StartNATS(t)
	brokerNC, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer brokerNC.Close()
	b.nc.Store(brokerNC)
	sub, err := b.SubscribeClusterUpgradeTrigger(brokerNC)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	ctl, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer ctl.Close()
	body, _ := upgradewire.EncodeRequest(s8PrepareRequest(key, fp))
	data := signUpgradeReq(t, seed, &proto.ClusterUpgradeReq{Op: brokerupgrade.SignedTransactionOp, TargetNode: "brk-a", ActorFP: fp,
		Upgrade: body, IssuedAt: time.Now().UTC().Format(time.RFC3339)})
	if err := brokerNC.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	msg, err := ctl.Request(proto.SubjCtrlClusterUpgrade(actor), data, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var resp proto.ClusterUpgradeResp
	if err := json.Unmarshal(msg.Data, &resp); err != nil || !resp.OK || spy.calls.Load() != 1 {
		t.Fatalf("signed transaction over the trigger subscription: %+v calls=%d err=%v", resp, spy.calls.Load(), err)
	}
}

type journalSummaryStore struct {
	upgradetx.Store
	view upgradetx.View
}

func (s journalSummaryStore) Load(upgradetx.Key, string) (upgradetx.View, error) { return s.view, nil }

// The local admin socket (cold Run and the restored graph) serves the one bound transaction endpoint,
// and broadcast health carries only the public summary of the Current journal record — never a running
// proof, actor or resource set. origin: hot-update S8 plan §2.2 / §7.1 / §11.2 P15
func TestBrokerAdminAndHealthExposeTheBoundTransaction(t *testing.T) {
	// origin: hot-update S10 plan §11.1 / TEST-BUDGET
	if runIsolatedBrokerTest(t) {
		return
	}
	key := upgradetx.Key{Component: upgradetx.ComponentBroker, DomainID: strings.Repeat("d", 32), ConfigPath: "/etc/tether/broker.yaml"}
	spy := &upgradeEndpointSpy{key: key}
	b := &Broker{cfg: Config{DB: openDB(t), Logger: silentLogger(), Now: time.Now}}
	b.upgradeEndpoint, b.upgradeKey = spy, key
	backend := brokerImageAdminBackend(b)
	if backend.UpgradeEndpoint == nil || backend.UpgradeKey != key {
		t.Fatalf("restored admin backend lost the bound endpoint: %+v", backend.UpgradeKey)
	}
	// The ctx-free socket port must still dispatch to this exact engine/key.
	// Testing the actual call preserves the identity assertion across adaptation.
	// origin: S7 internal review GATE-CONVERGE-layering F2
	body, err := upgradewire.EncodeRequest(s8PrepareRequest(key, "admin-actor"))
	if err != nil {
		t.Fatal(err)
	}
	deadline, err := controlbudget.After(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	response := backend.UpgradeEndpoint(body, upgradewire.Access{Key: key, Administrator: true}, deadline, nil)
	if spy.calls.Load() != 1 || response.Code != "" || response.View.Historical.Grant.Handle.Key != key {
		t.Fatalf("restored admin port lost engine/key: calls=%d response=%+v", spy.calls.Load(), response)
	}
	// Cold Run builds its own literal (the clustered-seam guard pins it there); it must name both fields.
	fields := map[string]string{}
	ast.Inspect(findFuncDecl(t, "broker.go", "Run").Body, func(n ast.Node) bool {
		if kv, ok := n.(*ast.KeyValueExpr); ok {
			if k, ok := kv.Key.(*ast.Ident); ok {
				value := kv.Value
				if k.Name == "UpgradeEndpoint" {
					if call, ok := value.(*ast.CallExpr); ok && len(call.Args) == 1 {
						if target, ok := call.Fun.(*ast.SelectorExpr); ok && target.Sel.Name == "BindAdminUpgrade" {
							value = call.Args[0]
						}
					}
				}
				if v, ok := value.(*ast.SelectorExpr); ok {
					if x, ok := v.X.(*ast.Ident); ok && x.Name == "b" {
						fields[k.Name] = v.Sel.Name
					}
				}
			}
		}
		return true
	})
	if fields["UpgradeEndpoint"] != "upgradeEndpoint" || fields["UpgradeKey"] != "upgradeKey" {
		t.Fatalf("cold Run's admin backend does not serve the bound endpoint: %v", fields)
	}

	n, _ := d7SingleNode(t, "brk-h")
	attempt := upgradetx.Attempt{Handle: upgradetx.Handle{Key: key, TxID: strings.Repeat("1", 32), CutID: strings.Repeat("2", 32), Generation: 2, Revision: 4},
		Phase: upgradetx.PhaseMigrating, Outcome: upgradetx.OutcomePending, TargetSHA: strings.Repeat("b", 64)}
	owner := &Broker{cl: &clusterRuntime{node: n}, cfg: Config{Upgrade: &brokerupgrade.Dependencies{Key: key,
		Store: journalSummaryStore{view: upgradetx.View{Historical: upgradetx.Record{Key: key, Forward: attempt, Grant: upgradetx.Grant{ActorFP: "private-actor"}}}}}}}
	bindBrokerImageHealth(owner)
	defer unbindBrokerImageHealth(owner)
	url := testharness.StartNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	sub, err := nc.Subscribe(proto.SubjCtrlClusterHealthWildcard, clusterHealthResponder(nc, n, n.RODB(), time.Now, nil, nil, "", nil, silentLogger()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	msg, err := nc.Request(proto.SubjCtrlClusterHealth("anyone"), nil, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var resp proto.ClusterHealthResp
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Upgrade == nil || resp.Upgrade.TxID != attempt.Handle.TxID || resp.Upgrade.Phase != string(upgradetx.PhaseMigrating) || resp.Upgrade.RunningSHA != "" {
		t.Fatalf("health summary = %+v", resp.Upgrade)
	}
	if strings.Contains(string(msg.Data), "private-actor") {
		t.Fatal("public health leaked the transaction actor")
	}
}
