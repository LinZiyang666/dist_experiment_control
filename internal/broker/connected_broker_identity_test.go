package broker

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
)

// origin: control-connection-recovery plan, current broker identity on register.
// Exercise the real handler over NATS. The handler is labelled as the leader,
// while its request names a different broker's custom INFO.Name. SQLite handles
// the ordinary registration write here; this is not a raft-forwarding fixture.
func TestRegisterReplyIdentifiesTheConnectedBroker(t *testing.T) {
	url := startNATS(t)
	db := openDBWithSession(t, "lab")
	if _, err := db.Exec(`INSERT INTO cluster_nodes
	    (node_id,name,node_ident_pub,nats_server_id,raft_addr,nats_route,tunnel_addr,public_host,cert_fp,phase,added_at)
	    VALUES ('edge-node','edge-node','pub-edge','custom-nats-name','203.0.113.2:7400',
	    'nats://203.0.113.2:6222','203.0.113.2:7000','edge.example','sha256:edge','VOTER',?)`,
		time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	b, err := New(Config{NATSURL: url, DB: db, Logger: silentLogger()})
	if err != nil {
		t.Fatal(err)
	}
	b.selfID = "leader-node"
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	b.nc.Store(nc)
	sub, err := nc.Subscribe(proto.SubjectPrefix+".ctrl.s.lab.node.*.register.req", b.handleRegister)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	for _, refresh := range []bool{false, true} {
		for _, serverName := range []string{"custom-nats-name", "unmapped-nats-name", ""} {
			name := "full"
			if refresh {
				name = "refresh"
			}
			t.Run(name+"/"+serverName, func(t *testing.T) {
				nid := "identity-agent"
				if refresh {
					nid = "unregistered-refresh-agent"
				}
				request := proto.NodeRegisterReq{ProtoVersion: proto.ProtoVersion,
					NID: nid, ServerID: serverName, RosterRefreshOnly: refresh}
				payload, err := json.Marshal(request)
				if err != nil {
					t.Fatal(err)
				}
				message, err := nc.Request(proto.SubjNodeRegister("lab", nid), payload, 3*time.Second)
				if err != nil {
					t.Fatal(err)
				}
				var response proto.NodeRegisterResp
				if err := json.Unmarshal(message.Data, &response); err != nil {
					t.Fatal(err)
				}
				if !response.OK {
					t.Fatalf("register rejected: %+v", response)
				}
				var fields map[string]json.RawMessage
				if err := json.Unmarshal(message.Data, &fields); err != nil {
					t.Fatal(err)
				}
				if serverName == "custom-nats-name" {
					if response.ConnectedBrokerID != "edge-node" {
						t.Fatalf("reply identifies %q, want edge-node, not leader-node or the custom NATS name", response.ConnectedBrokerID)
					}
					// An N-1 reader ignores the additive field and keeps the successful reply.
					var oldResponse struct {
						OK bool `json:"ok"`
					}
					if err := json.Unmarshal(message.Data, &oldResponse); err != nil || !oldResponse.OK {
						t.Fatalf("legacy response reader rejected the additive identity: %v", err)
					}
				} else {
					if response.ConnectedBrokerID != "" {
						t.Fatalf("unmapped server manufactured broker identity %q", response.ConnectedBrokerID)
					}
					if _, present := fields["connected_broker_id"]; present {
						t.Fatal("unknown identity must omit the field for N-1 compatibility")
					}
				}
				if refresh {
					var count int
					if err := db.QueryRow(`SELECT COUNT(*) FROM nodes WHERE sid='lab' AND nid=?`, nid).Scan(&count); err != nil {
						t.Fatal(err)
					}
					if count != 0 {
						t.Fatal("identity resolution made a roster-only refresh write a node row")
					}
				}
			})
		}
	}
}

func TestConnectedBrokerIdentityAbsentOnLegacyReply(t *testing.T) {
	var response proto.NodeRegisterResp
	if err := json.Unmarshal([]byte(`{"ok":true}`), &response); err != nil {
		t.Fatal(err)
	}
	if !response.OK || response.ConnectedBrokerID != "" {
		t.Fatalf("legacy reply must remain successful with unknown connected identity: %+v", response)
	}
	payload, err := json.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(payload, &fields); err != nil {
		t.Fatal(err)
	}
	if _, present := fields["connected_broker_id"]; present {
		t.Fatal("zero connected identity must be omitted")
	}
}
