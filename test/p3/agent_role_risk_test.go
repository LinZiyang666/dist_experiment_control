package p3_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
)

func TestAuthCalloutRejectsWildcardAgentRole(t *testing.T) {
	url, brokerSeed, accountSeed := startAuthNATS(t)
	db := openDB(t)
	defer startBrokerWithAuth(t, url, db, brokerSeed, accountSeed)()

	owner := admittedIdentity(t, db)
	mustCreate(t, url, owner, "lab", "owner-pin")

	attacker := freshIdentity(t)
	nc, err := cli.ConnectNATSWithNkey(url, attacker, nats.Name(cli.AgentName("*", "*")))
	if err != nil {
		return
	}
	defer nc.Close()

	payload, _ := json.Marshal(proto.NodeRegisterReq{
		ProtoVersion:   proto.ProtoVersion,
		ReleaseVersion: "risk-test",
		NID:            "evil",
	})
	msg, reqErr := nc.Request(proto.SubjNodeRegister("lab", "evil"), payload, 2*time.Second)
	if reqErr != nil {
		t.Fatalf("wildcard agent role CONNECT succeeded; register was denied later: %v", reqErr)
	}
	var resp proto.NodeRegisterResp
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		t.Fatal(err)
	}
	if resp.OK {
		t.Fatalf("wildcard agent role was accepted: attacker connected as tether-agent:*:* and registered node evil in session lab")
	}
	t.Fatalf("wildcard agent role CONNECT succeeded; broker response: %+v", resp)
}

func TestAuthCalloutRejectsUnprovisionedAgentRole(t *testing.T) {
	url, brokerSeed, accountSeed := startAuthNATS(t)
	db := openDB(t)
	defer startBrokerWithAuth(t, url, db, brokerSeed, accountSeed)()

	owner := admittedIdentity(t, db)
	mustCreate(t, url, owner, "lab", "owner-pin")

	attacker := freshIdentity(t)
	nc, err := cli.ConnectNATSWithNkey(url, attacker, nats.Name(cli.AgentName("lab", "evil")))
	if err != nil {
		return
	}
	defer nc.Close()

	payload, _ := json.Marshal(proto.NodeRegisterReq{
		ProtoVersion:   proto.ProtoVersion,
		ReleaseVersion: "risk-test",
		NID:            "evil",
	})
	msg, reqErr := nc.Request(proto.SubjNodeRegister("lab", "evil"), payload, 2*time.Second)
	if reqErr != nil {
		t.Fatalf("unprovisioned agent CONNECT succeeded; register was denied later: %v", reqErr)
	}
	var resp proto.NodeRegisterResp
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		t.Fatal(err)
	}
	if resp.OK {
		t.Fatalf("unprovisioned agent role was accepted: attacker connected as tether-agent:lab:evil and registered in session lab")
	}
	t.Fatalf("unprovisioned agent role CONNECT succeeded; broker response: %+v", resp)
}
