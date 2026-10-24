// transfer_cluster_fixture_test.go holds the shared continuation authority fixture.
// origin: S10 internal review GATE-FINAL F2
package broker

// Three real Raft/SQLite owners and routed NATS ingresses use the production
// transfer factory. A shared embedded JetStream isolates authority handoff
// from storage placement; the agent intake reports only preparation facts.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/natsinbox"
	"github.com/LinZiyang666/tether/internal/proto"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xferproof"
	"github.com/LinZiyang666/tether/test/clusterharness"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nkeys"
)

type continuationCluster struct {
	*transferRouteCluster
	js            jetstream.JetStream
	actor, agent  string
	agentKey      nkeys.KeyPair
	actorKey      nkeys.KeyPair
	account       nkeys.KeyPair
	capsules      chan x.SettlementCapsule
	pendingConfig atomic.Bool
}

func newContinuationCluster(t *testing.T, overlays ...func(int, jetstream.JetStream) jetstream.JetStream) *continuationCluster {
	t.Helper()
	c := &continuationCluster{capsules: make(chan x.SettlementCapsule, 8)}
	var err error
	c.actorKey, err = nkeys.CreateUser()
	requireWiring(t, err)
	t.Cleanup(c.actorKey.Wipe)
	c.actor, _ = c.actorKey.PublicKey()
	c.agentKey, err = nkeys.CreateUser()
	requireWiring(t, err)
	t.Cleanup(c.agentKey.Wipe)
	c.agent, _ = c.agentKey.PublicKey()
	c.account, err = nkeys.CreateAccount()
	requireWiring(t, err)
	t.Cleanup(c.account.Wipe)
	accountPub, _ := c.account.PublicKey()
	var brokerKeys [3]nkeys.KeyPair
	var brokerPubs []string
	for i := range brokerKeys {
		brokerKeys[i], err = nkeys.CreateUser()
		requireWiring(t, err)
		t.Cleanup(brokerKeys[i].Wipe)
		pub, _ := brokerKeys[i].PublicKey()
		brokerPubs = append(brokerPubs, pub)
	}
	c.transferRouteCluster = newTransferRouteClusterWithStorage(t, true, func(i int, opts *natsserver.Options) []nats.Option {
		opts.AlwaysEnableNonce = true
		for _, pub := range brokerPubs {
			opts.Nkeys = append(opts.Nkeys, &natsserver.NkeyUser{Nkey: pub})
		}
		opts.AuthCallout = &natsserver.AuthCallout{Issuer: accountPub, Account: "$G", AuthUsers: brokerPubs}
		return []nats.Option{nats.Nkey(brokerPubs[i], brokerKeys[i].Sign)}
	})
	c.js, err = jetstream.New(c.conns[0])
	requireWiring(t, err)
	mustWait(t, 15*time.Second, "routed JetStream authority ready", func() bool {
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		_, err := c.js.AccountInfo(ctx)
		return err == nil
	})
	fp, _ := auth.FingerprintFromActor(c.actor)
	agentFP, _ := auth.FingerprintFromActor(c.agent)
	queries := []string{
		"INSERT INTO sessions(sid,name,owner_pubkey_fp,pin_hash) VALUES('lab','lab','" + fp + "','pin')",
		"INSERT INTO members(sid,pubkey_fp,role,via) VALUES('lab','" + fp + "','owner','pin')",
		"INSERT INTO nodes(sid,nid,nats_server,status) VALUES('lab','node','server-0','ONLINE')",
		"INSERT INTO agent_provisioning(sid,nid,agent_fp,joined_at) VALUES('lab','node','" + agentFP + "','2026-10-13T00:00:00Z')",
	}
	for i := range c.brokers {
		queries = append(queries, fmt.Sprintf("INSERT INTO cluster_nodes(node_id,name,node_ident_pub,nats_server_id,raft_addr,nats_route,tunnel_addr,public_host,cert_fp,phase,added_at) VALUES('broker-%d','broker-%d','pub-%d','server-%d','127.0.0.1:7400','127.0.0.1:6222','127.0.0.1:7000','host','sha','VOTER','2026-10-13T00:00:00Z')", i, i, i, i))
	}
	routePropose(t, c.transferRouteCluster, queries)
	seed, _ := c.account.Seed()
	for i, b := range c.brokers {
		b.cfg.DBPath = filepath.Join(t.TempDir(), "state.db")
		b.cfg.AuthCallout = &AuthCalloutConfig{AccountSeed: seed}
		b.cl.forwarder = NewForwarderWithLimit(c.conns[i], time.Second, 0)
		b.AttachXferReplicas(func() int { return 3 })
		js, err := jetstream.New(c.conns[i])
		requireWiring(t, err)
		for _, overlay := range overlays {
			js = overlay(i, js)
		}
		setBrokerJS(b, js)
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		err = startBrokerTransfers(ctx, b, c.conns[i])
		cancel()
		requireWiring(t, err)
		t.Cleanup(func() {
			err := stopBrokerTransfers(b)
			if c.pendingConfig.Load() && err != nil && strings.Contains(err.Error(), "cannot retire sent-unknown configuration request") {
				return
			}
			requireWiring(t, err)
		})
		authSub, err := b.installAuthCallout(c.conns[i])
		requireWiring(t, err)
		t.Cleanup(func() { _ = authSub.Unsubscribe() })
		c.origins[i] = b.transferRuntime.Load().Origin
	}
	agentConn, err := nats.Connect(c.conns[2].ConnectedUrl(), nats.NoReconnect(), nats.Nkey(c.agent, c.agentKey.Sign), nats.Name("tether-agent:lab:node"), nats.CustomInboxPrefix(auth.InboxPrefixFor(c.agent)))
	requireWiring(t, err)
	t.Cleanup(agentConn.Close)
	sub, err := agentConn.Subscribe(proto.SubjCmdForwarded("lab", "node", proto.TransferLinkVerb), func(m *nats.Msg) {
		var envelope proto.TransferCommitReq
		if json.Unmarshal(m.Data, &envelope) != nil || envelope.Link == nil {
			return
		}
		req := *envelope.Link
		result := proto.TransferLinkResult{Version: x.Version, InstanceID: "agent", AgentNkey: c.agent, ReceiverNkey: c.agent, MaxPayload: c.conns[2].MaxPayload()}
		if req.Op == proto.TransferLinkProbe {
			result.BindingSignature, _ = xferproof.Sign(c.agentKey.Sign, xferproof.Binding(req.CallID, "lab", "node", "agent", c.agent))
		} else {
			if req.Settlement != nil {
				wc := req.Settlement
				c.capsules <- x.SettlementCapsule{Identity: x.Identity(wc.Identity), Origin: x.Origin(wc.Origin), ReceiverNkey: wc.ReceiverNkey, Issuer: wc.Issuer, Nonce: wc.Nonce, Signature: wc.Signature}
			}
			result.Status = &proto.TransferStatusResp{State: proto.StatusState(x.StatusLive), Phase: proto.Phase(x.PhasePrepared), Identity: req.Identity, Origin: req.Origin, Budget: *req.Budget}
		}
		body, _ := json.Marshal(proto.TransferCommitResp{Link: &result})
		_ = m.Respond(body)
	})
	requireWiring(t, err)
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	requireWiring(t, agentConn.FlushTimeout(time.Second))
	return c
}

func (c *continuationCluster) leader(t *testing.T) int {
	t.Helper()
	probes := make([]clusterharness.LeaderProbe, 3)
	for i := range probes {
		probes[i] = c.nodes[i]
	}
	var i int
	_, err := clusterharness.WithLeader(t, probes, 5*time.Second, func(leader int) error { i = leader; return nil })
	requireWiring(t, err)
	return i
}

func (c *continuationCluster) permit(t *testing.T, owner int, tid string) (x.CreationPermit, x.Intent) {
	t.Helper()
	resp, intent := c.callPermit(t, owner, tid)
	if resp.Code != "" || resp.Permit == nil {
		t.Fatalf("production cluster permit unavailable: code=%s error=%s", resp.Code, resp.Error)
	}
	p := resp.Permit
	return x.CreationPermit{Identity: x.Identity(p.Identity), Origin: x.Origin(p.Origin), IssuedAt: p.IssuedAt, CreateUntil: p.CreateUntil, PermitNonce: p.PermitNonce, Signature: p.Signature}, intent
}

func (c *continuationCluster) callPermit(t *testing.T, owner int, tid string) (proto.TransferPermitResp, x.Intent) {
	t.Helper()
	remaining := *c.transferRouteCluster
	remaining.nodes = nil
	members, err := c.nodes[c.leader(t)].RaftConfiguration()
	requireWiring(t, err)
	for _, n := range c.nodes {
		for _, member := range members {
			if member.NodeID == n.SelfID() {
				remaining.nodes = append(remaining.nodes, n)
			}
		}
	}
	routePropose(t, &remaining, []string{fmt.Sprintf("UPDATE nodes SET nats_server='server-%d' WHERE sid='lab' AND nid='node'", owner)})
	empty := sha256.Sum256(nil)
	intent := x.Intent{Verb: "push", Tier: "a", Path: "destination", SHA256: hex.EncodeToString(empty[:])}
	nc, _, err := natsinbox.Connect(c.conns[(owner+1)%3].ConnectedUrl(), c.actor, []nats.Option{nats.Nkey(c.actor, c.actorKey.Sign), nats.Name("tether-cli:lab"), nats.NoReconnect()})
	requireWiring(t, err)
	defer nc.Close()
	subject, err := proto.SubjCtrlTransferRequest(c.actor, "lab", "node", tid, x.OpPermit)
	requireWiring(t, err)
	body, _ := json.Marshal(proto.TransferPermitReq{CallID: "permit", Intent: proto.Intent(intent), TransferID: tid, AgentInstance: "agent"})
	msg, err := nc.Request(subject, body, 10*time.Second)
	requireWiring(t, err)
	var resp proto.TransferPermitResp
	requireWiring(t, proto.DecodeTransferWire(msg.Data, &resp))
	return resp, intent
}
