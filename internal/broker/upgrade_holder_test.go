package broker

import (
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/brokermaint"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
)

// origin: hot-update S6 plan §7.1 / §9.1 / T40
func TestBrokerUpgradeHolderSignedCAS(t *testing.T) { exerciseHolderLostLease(t) }

// origin: hot-update S6 plan §7.1 / T40
func TestBrokerHolderLeakCycles(t *testing.T) {
	// origin: hot-update S10 plan §11.1 / TEST-BUDGET
	if runIsolatedBrokerTest(t) {
		return
	}
	beforeG := runtime.NumGoroutine()
	beforeFD := testharness.FDCount()
	identities := holderFDIdentities(t)
	for round := 0; round < 5; round++ {
		t.Run("round-"+strconv.Itoa(round), func(t *testing.T) { exerciseHolderLostLease(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "upgrade holder", beforeG)
	testharness.AssertNoFDLeak(t, "upgrade holder", beforeFD, 0)
	if !reflect.DeepEqual(identities, holderFDIdentities(t)) {
		t.Fatal("holder teardown changed original FD identity multiset")
	}
}

func holderFDIdentities(t *testing.T) map[string]int {
	t.Helper()
	if runtime.GOOS != "linux" {
		return nil
	}
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	result := map[string]int{}
	for _, entry := range entries {
		target, err := os.Readlink("/proc/self/fd/" + entry.Name())
		if err == nil {
			result[target]++
		}
	}
	return result
}

func exerciseHolderLostLease(t *testing.T) {
	t.Helper()
	n, _ := d7SingleNode(t, "holder-home")
	seed, err := auth.GenerateUserSeed()
	if err != nil {
		t.Fatal(err)
	}
	var routed []adminsock.Request
	b := newTriggerTestBroker("holder-home", seed, triggerNow, &routed)
	b.cl = &clusterRuntime{node: n}
	b.clusterMode = true
	b.cfg.Logger = silentLogger()
	invoke := func(op, holder string, now time.Time) *proto.ClusterUpgradeResp {
		t.Helper()
		req := &proto.ClusterUpgradeReq{Op: op, Holder: holder, TargetNode: b.selfID, IssuedAt: now.Format(time.RFC3339)}
		return b.handleUpgradeTrigger(signUpgradeReq(t, seed, req), now)
	}
	if resp := invoke("acquire-lock", "A", triggerNow); !resp.OK {
		state, _ := cluster.ReadUpgradeLock(n.RODB())
		writer, _ := cluster.ReadUpgradeLock(n.DB())
		t.Fatalf("acquire A: %+v state=%+v writer=%+v", resp, state, writer)
	}
	sameTime := invoke("acquire-lock", "B", triggerNow)
	if sameTime.OK || sameTime.Code != adminsock.CodeBadRequest {
		t.Fatalf("same-timestamp B: %+v", sameTime)
	}
	read := func() cluster.UpgradeLockState {
		t.Helper()
		state, err := cluster.ReadUpgradeLock(n.RODB())
		if err != nil {
			t.Fatal(err)
		}
		return state
	}
	ownedA := read()
	for _, op := range []string{"acquire-lock", "renew-lock", "expire-lock-lease", "release-lock", "reload", "transfer-leader", "reexec-agent"} {
		resp := invoke(op, "", triggerNow)
		if resp.OK || resp.Code != adminsock.CodeBadRequest || read() != ownedA {
			t.Fatalf("holder-less %s touched new lock: %+v", op, resp)
		}
	}
	later := triggerNow.Add(cluster.LockLeaseTTL + time.Second)
	if resp := invoke("acquire-lock", "B", later); !resp.OK {
		t.Fatalf("takeover B: %+v", resp)
	}
	ownedB := read()
	for _, op := range []string{"renew-lock", "expire-lock-lease", "release-lock", "reload", "transfer-leader", "reexec-agent"} {
		resp := invoke(op, "A", later)
		if resp.OK || resp.Code != adminsock.CodeBadRequest || read() != ownedB {
			t.Fatalf("late A %s touched B: %+v", op, resp)
		}
	}
	if resp := invoke("renew-lock", "B", later.Add(time.Second)); !resp.OK {
		t.Fatalf("renew B: %+v", resp)
	}
	record := brokermaint.Record{CutID: "cut", Holder: "B", Home: "holder-home", InstanceID: "instance", IssuerBootID: "boot", Mode: "quorum", Generation: 1, Phase: brokermaint.MaintPrepared, IssuedBootNS: 1, ExpiresBootNS: 1 + int64(8*time.Second), Term: 1, ConfigIndex: 1, VoterDigest: "voters", BaselineDigest: "baseline", RequiredCapability: brokermaint.Capability}
	var currentTerm string
	if err := n.RODB().QueryRow(`SELECT value FROM cluster_meta WHERE key='applied_term'`).Scan(&currentTerm); err != nil {
		t.Fatal(err)
	}
	record.Term, err = strconv.ParseUint(currentTerm, 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	maintenance := &proto.ClusterUpgradeReq{Op: "maintenance", TargetNode: b.selfID, Holder: "B", CutID: "cut", Maintenance: &record, IssuedAt: later.Format(time.RFC3339)}
	changed := record
	changed.Term++
	stale := *maintenance
	stale.Maintenance = &changed
	if resp := b.handleUpgradeTrigger(signUpgradeReq(t, seed, &stale), later); resp.OK {
		t.Fatal("maintenance stale term accepted")
	}
	resp := b.handleUpgradeTrigger(signUpgradeReq(t, seed, maintenance), later)
	if !resp.OK {
		t.Fatalf("maintenance commit: %+v", resp)
	}
	actual, err := cluster.ReadBrokerMaintenance(n.RODB(), record.Home)
	if err != nil || actual != record {
		t.Fatalf("maintenance readback: %v", err)
	}
	if resp := invoke("release-lock", "B", later.Add(2*time.Second)); !resp.OK || read() != (cluster.UpgradeLockState{}) {
		t.Fatalf("B release not complete: %+v", resp)
	}
	// The reaper must use the writer's renewed pair, not an expired RODB hint.
	if resp := invoke("acquire-lock", "C", later); !resp.OK {
		t.Fatal(resp)
	}
	hintTime := later.Add(cluster.LockLeaseTTL + time.Second)
	if clear, err := upgradeLockDecision(n.RODB(), hintTime); err != nil || !clear {
		t.Fatal("missing expired hint")
	}
	if resp := invoke("renew-lock", "C", later.Add(time.Second*2)); !resp.OK {
		t.Fatal(resp)
	}
	if err := n.Propose(func(db *sql.DB) (*cluster.Command, error) { return cluster.PlanReapUpgradeLease(db, hintTime) }); err != nil {
		t.Fatal(err)
	}
	marker, _ := cluster.UpgradeHolderMarker("C")
	if read().Marker != marker {
		t.Fatal("stale hint reaped renewed C")
	}
	if err := b.reconcileUpgradeLock(t.Context(), hintTime.Add(cluster.LockLeaseTTL)); err != nil || read() != (cluster.UpgradeLockState{}) {
		t.Fatalf("expired current reaper: %v", err)
	}
}

// origin: hot-update S6 plan §9.1 / T40
func TestBrokerUpgradeHolderRejectsUnsignedExtension(t *testing.T) {
	seed, err := auth.GenerateUserSeed()
	if err != nil {
		t.Fatal(err)
	}
	var routed []adminsock.Request
	b := newTriggerTestBroker("home", seed, triggerNow, &routed)
	for _, change := range []func(*proto.ClusterUpgradeReq){
		func(r *proto.ClusterUpgradeReq) { r.Holder = "changed" }, func(r *proto.ClusterUpgradeReq) { r.Scope = "broker-daemon" }, func(r *proto.ClusterUpgradeReq) { r.CutID = "changed" }, func(r *proto.ClusterUpgradeReq) {
			r.Maintenance = &brokermaint.Record{CutID: r.CutID, Holder: "changed"}
		},
	} {
		req := &proto.ClusterUpgradeReq{Op: "release-lock", TargetNode: "home", IssuedAt: triggerNow.Format(time.RFC3339), Holder: "holder", Scope: "whole-host", CutID: "cut"}
		signUpgradeReq(t, seed, req)
		change(req)
		data, _ := json.Marshal(req)
		resp := b.handleUpgradeTrigger(data, triggerNow)
		if resp.Code != upgradeTriggerCodeUnauthorized {
			t.Fatalf("unsigned extension dispatched: %+v", resp)
		}
		legacy := &proto.ClusterUpgradeReq{Op: "release-lock", TargetNode: "home", IssuedAt: triggerNow.Format(time.RFC3339)}
		signUpgradeReq(t, seed, legacy)
		change(legacy)
		data, _ = json.Marshal(legacy)
		resp = b.handleUpgradeTrigger(data, triggerNow)
		if resp.Code != upgradeTriggerCodeUnauthorized {
			t.Fatalf("unsigned extension dispatched: %+v", resp)
		}
	}
	invalid := &proto.ClusterUpgradeReq{Op: "reload", TargetNode: "home", IssuedAt: triggerNow.Format(time.RFC3339), Scope: "unknown"}
	// A signature over the nil failure sentinel must still be rejected BEFORE verification.
	data := signUpgradeReq(t, seed, invalid)
	if resp := b.handleUpgradeTrigger(data, triggerNow); resp.Code != adminsock.CodeBadRequest {
		t.Fatalf("invalid signed identifiers: %+v", resp)
	}
	if len(routed) != 0 {
		t.Fatal("rejected extension reached admin backend")
	}
}

// origin: hot-update S6 plan §7.1 / §7.3
func TestBrokerMaintenanceChallengeBindings(t *testing.T) {
	nc, err := nats.Connect(startNATS(t))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	req := proto.BrokerMaintenanceReq{Record: brokermaint.Record{CutID: "cut", InstanceID: "instance", RequiredCapability: brokermaint.Capability, ExpiresBootNS: int64(8 * time.Second)}, Proof: brokermaint.Proof{Nonce: "nonce", RecordDigest: "record", ResponseBootNS: int64(time.Second)}, NID: "node", ConnectedBrokerID: "home", IngressServerID: "ingress", RouteDigest: "route", Sig: "signed"}
	good := proto.BrokerMaintenanceAck{CutID: "cut", Nonce: "nonce", NID: "node", ConnectedBrokerID: "home", IngressServerID: "ingress", InstanceID: "instance", RouteDigest: "route", ProofDigest: "proof", Accepted: true, RemainingNS: int64(6 * time.Second), Sig: "signed"}
	for _, tc := range []struct {
		name      string
		mutate    func(*proto.BrokerMaintenanceAck)
		verifyErr bool
	}{
		{name: "valid"}, {name: "rejected", mutate: func(a *proto.BrokerMaintenanceAck) { a.Accepted = false }},
		{name: "nonce", mutate: func(a *proto.BrokerMaintenanceAck) { a.Nonce = "wrong" }},
		{name: "cut", mutate: func(a *proto.BrokerMaintenanceAck) { a.CutID = "wrong" }},
		{name: "node", mutate: func(a *proto.BrokerMaintenanceAck) { a.NID = "wrong" }},
		{name: "broker", mutate: func(a *proto.BrokerMaintenanceAck) { a.ConnectedBrokerID = "wrong" }},
		{name: "ingress", mutate: func(a *proto.BrokerMaintenanceAck) { a.IngressServerID = "wrong" }},
		{name: "instance", mutate: func(a *proto.BrokerMaintenanceAck) { a.InstanceID = "wrong" }},
		{name: "route", mutate: func(a *proto.BrokerMaintenanceAck) { a.RouteDigest = "wrong" }},
		{name: "digest", mutate: func(a *proto.BrokerMaintenanceAck) { a.ProofDigest = "" }},
		{name: "expired", mutate: func(a *proto.BrokerMaintenanceAck) { a.RemainingNS = 0 }},
		{name: "renewed", mutate: func(a *proto.BrokerMaintenanceAck) { a.RemainingNS = int64(8 * time.Second) }},
		{name: "unsigned", mutate: func(a *proto.BrokerMaintenanceAck) { a.Sig = "" }}, {name: "bad-signature", verifyErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ack := good
			if tc.mutate != nil {
				tc.mutate(&ack)
			}
			sub, err := nc.Subscribe(proto.SubjCmdForwarded("session", "node", proto.BrokerMaintenanceVerb), func(msg *nats.Msg) {
				var got proto.BrokerMaintenanceReq
				if json.Unmarshal(msg.Data, &got) != nil || got != req {
					return
				}
				body, _ := json.Marshal(ack)
				_ = msg.Respond(body)
			})
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := sub.Unsubscribe(); err != nil {
					t.Errorf("unsubscribe: %v", err)
				}
			}()
			if err := nc.FlushTimeout(time.Second); err != nil {
				t.Fatal(err)
			}
			called := false
			actual, err := deliverMaintenanceChallenge(nc, "session", req, time.Second, func(a proto.BrokerMaintenanceAck) error {
				called = true
				if tc.verifyErr {
					return errors.New("bad signature")
				}
				return nil
			})
			if tc.name == "valid" {
				if err != nil || actual != good || !called {
					t.Fatalf("valid challenge: %v", err)
				}
			} else if err == nil {
				t.Fatal("invalid ACK admitted")
			}
		})
	}
	if _, err := deliverMaintenanceChallenge(nc, "session", req, time.Second, nil); err == nil {
		t.Fatal("missing verifier accepted")
	}
}
