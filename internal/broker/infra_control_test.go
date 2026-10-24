package broker

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/infraattach"
	"github.com/LinZiyang666/tether/internal/natsinbox"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	ns "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// origin: hot-update S9 plan §7.1 / T47 / WIRE-2 R14
func TestBrokerInfraControlUsesAccountAuthorityAndDirectSubscription(t *testing.T) {
	// origin: hot-update S10 plan §11.1 / TEST-BUDGET
	if runIsolatedBrokerTest(t) {
		return
	}
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseBrokerInfraControl(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "broker infra control x5", beforeG)
	testharness.AssertNoFDLeak(t, "broker infra control x5", beforeFD, 0)
}

func exerciseBrokerInfraControl(t *testing.T) {
	t.Helper()
	f := newBrokerInfraFixture(t, false, false)
	config := f.config(false)
	descriptor := config.Infra.Auth
	config.Infra = &infraattach.BrokerInfra{Identities: &infraattach.IdentitySupply{SID: infraSID, NID: infraNID, AccountPublicKey: descriptor.AccountPublicKey,
		Member: infraattach.OrdinaryProbe{PublicKey: descriptor.Member.PublicKey, Sign: descriptor.Member.Sign, Subject: descriptor.Member.RPCSubject, Request: descriptor.Member.RPCRequest, Response: descriptor.Member.RPCResponse, Handle: descriptor.Business[0].Handle},
		Agent:  infraattach.OrdinaryProbe{PublicKey: descriptor.Agent.PublicKey, Sign: descriptor.Agent.Sign, Subject: descriptor.Agent.RPCSubject, Request: descriptor.Agent.RPCRequest, Response: descriptor.Agent.RPCResponse, Handle: descriptor.Business[1].Handle}}}
	b, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	b.selfID, b.runCtx = "b1", t.Context()
	if err := brokerInfraConnect(b, infraCutDeadline(t)); err != nil {
		t.Fatal(err)
	}
	nc := b.nc.Load()
	owner := newBrokerAuthOwner(b, infraCutDeadline(t))
	if _, err := b.installAuthCallout(nc, owner); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := b.infra.Close(infraCutDeadline(t)); err != nil {
			t.Error(err)
		}
		nc.Close()
		if err := stopBrokerImageAuth(owner, infraCutDeadline(t)); err != nil {
			t.Error(err)
		}
	})
	if !b.infra.Configured() {
		t.Fatal("production cold root ignored the supplied ordinary identities")
	}
	// The shared cold/restored table must select a DIRECT exact-host owner even
	// when all ordinary control verbs use the cluster's queue group.
	b.clusterMode = true
	if err := subscribeBrokerImageBusiness(b, nc); err != nil {
		t.Fatal(err)
	}
	if err := nc.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	rows := brokerBusinessSubscriptions(b, nc)
	found := false
	for _, row := range rows {
		if row.subj == proto.SubjClusterInfra("b1") {
			found = true
		}
	}
	if !found {
		t.Fatal("cold/hot business table omitted the infra owner")
	}
	// The independent server census is the wire-level source for whether
	// the real hot binding used a queue.
	connections, err := f.server.Connz(&ns.ConnzOptions{SubscriptionsDetail: true, Limit: 1024})
	if err != nil {
		t.Fatal(err)
	}
	live := false
	for _, connection := range connections.Conns {
		for _, sub := range connection.SubsDetail {
			if sub.Subject == proto.SubjClusterInfra("b1") {
				live = true
				if sub.Queue != "" {
					t.Fatal("infra owner was queue grouped on the wire")
				}
			}
		}
	}
	if !live {
		t.Fatal("infra SUB never reached the server")
	}
	request := proto.InfraUpgradeReq{Request: proto.InfraCommand{Version: 1, Purpose: proto.InfraRequestPurpose, Op: "probe", RemainingNS: uint64(time.Second)},
		TargetNode: "b1", Actor: f.pubs["member"], IssuedAt: time.Now().UTC().Format(time.RFC3339Nano), Nonce: "01000000000000000000000000000000"}
	data, err := proto.InfraUpgradeSigningBytes(request)
	if err != nil {
		t.Fatal(err)
	}
	sig, err := f.keys["account"].Sign(data)
	if err != nil {
		t.Fatal(err)
	}
	request.Sig = hex.EncodeToString(sig)
	body, err := proto.EncodeInfraUpgradeRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	options := append([]nats.Option{nats.Name("tether-infra-manager:b1"), nats.Token(string(body)), nats.Nkey(request.Actor, f.keys["member"].Sign), nats.NoReconnect()}, natsinbox.InboxConnectOptions(request.Actor)...)
	manager, err := nats.Connect(f.server.ClientURL(), options...)
	if err != nil {
		t.Fatal("production normal queue did not admit the account-authorized manager:", err)
	}
	defer manager.Close()
	response, err := manager.Request(proto.SubjClusterInfra("b1"), body, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var reply proto.InfraUpgradeResp
	if err := json.Unmarshal(response.Data, &reply); err != nil || reply.OK || reply.Code != proto.CodeInfraUnsupported {
		t.Fatalf("uncomposed runtime did not explicitly refuse: %s %v", response.Data, err)
	}
	// CONNECT authorization cannot substitute for verification of each command.
	unsigned := request
	unsigned.Sig = ""
	badBody, err := proto.EncodeInfraUpgradeRequest(unsigned)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Request(proto.SubjClusterInfra("b1"), badBody, 200*time.Millisecond); err == nil {
		t.Fatal("subscriber accepted an unsigned command on an authorized manager connection")
	}
	expired := request
	expired.IssuedAt = time.Now().Add(-2 * time.Second).UTC().Format(time.RFC3339Nano)
	expired.Sig = ""
	expiredBytes, err := proto.InfraUpgradeSigningBytes(expired)
	if err != nil {
		t.Fatal(err)
	}
	expiredSig, err := f.keys["account"].Sign(expiredBytes)
	if err != nil {
		t.Fatal(err)
	}
	expired.Sig = hex.EncodeToString(expiredSig)
	expiredBody, err := proto.EncodeInfraUpgradeRequest(expired)
	if err != nil {
		t.Fatal(err)
	}
	expiredResponse, err := manager.Request(proto.SubjClusterInfra("b1"), expiredBody, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(expiredResponse.Data, &reply); err != nil || reply.Code != proto.CodeInfraBudgetExceeded {
		t.Fatalf("queue/transit time renewed the signed budget: %s %v", expiredResponse.Data, err)
	}
	for _, scenario := range []string{"unsigned", "other-actor", "other-target"} {
		invalid := request
		name := "tether-infra-manager:b1"
		if scenario == "unsigned" {
			invalid.Sig = ""
		}
		if scenario == "other-actor" {
			invalid.Actor = f.pubs["agent"]
		}
		if scenario == "other-target" {
			name = "tether-infra-manager:b2"
		}
		bad, err := proto.EncodeInfraUpgradeRequest(invalid)
		if err != nil {
			t.Fatal(err)
		}
		denied, err := nats.Connect(f.server.ClientURL(), nats.Name(name), nats.Token(string(bad)), nats.Nkey(request.Actor, f.keys["member"].Sign), nats.NoReconnect())
		if denied != nil {
			denied.Close()
		}
		if err == nil || !strings.Contains(err.Error(), "Authorization") {
			t.Fatalf("%s manager admitted: %v", scenario, err)
		}
	}
	// Shared predicates must preserve the existing normal queue decision too.
	if infraattach.QueueBusiness(true, proto.SubjClusterInfra("b1"), false) || !infraattach.QueueBusiness(true, proto.SubjCtrlNodeList(request.Actor, infraSID), false) {
		t.Fatal("infra direct binding changed normal queue semantics")
	}
}
