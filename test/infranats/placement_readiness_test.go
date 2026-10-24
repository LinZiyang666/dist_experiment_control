//go:build linux && amd64 && infranats_integration

package infranats_test

import (
	"encoding/json"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	server "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// Account INFO can succeed while the last server is still joining the meta
// group. Creating R3 then fails with no suitable peers, before any cut. Query
// every exact server through the existing read-only SYS principal; neither a
// sleep nor a successful account request establishes placement readiness.
// origin: S9 internal review INFRA-SUITE-SUPPLIES F8
func stockPlacementCurrent(nodes []*stockNode) bool {
	members := make(map[string]bool, len(nodes))
	for _, node := range nodes {
		members[node.name] = true
	}
	leaderCurrent := false
	for _, node := range nodes {
		pub, err := node.readKey.PublicKey()
		if err != nil {
			return false
		}
		nc, err := nats.Connect(node.url, nats.Nkey(pub, node.readKey.Sign), nats.NoReconnect(),
			nats.Timeout(100*time.Millisecond), nats.CustomInboxPrefix("_INBOX.tether.infra.sys."+pub))
		if err != nil {
			return false
		}
		msg, err := nc.Request("$SYS.REQ.SERVER."+nc.ConnectedServerId()+".JSZ", []byte(`{"raft":true}`), 200*time.Millisecond)
		nc.Close()
		var reply struct {
			Data server.JSInfo `json:"data"`
		}
		if err != nil || json.Unmarshal(msg.Data, &reply) != nil || reply.Data.Disabled {
			return false
		}
		meta := reply.Data.Meta
		if meta == nil || meta.Size != len(nodes) || !members[meta.Leader] {
			return false
		}
		if meta.Leader == node.name {
			if len(meta.Replicas) != len(nodes)-1 {
				return false
			}
			seen := map[string]bool{node.name: true}
			for _, peer := range meta.Replicas {
				if peer == nil || !members[peer.Name] || seen[peer.Name] || !peer.Current || peer.Offline || peer.Lag != 0 {
					return false
				}
				seen[peer.Name] = true
			}
			leaderCurrent = true
		}
	}
	return leaderCurrent
}

func waitStockPlacement(t *testing.T, nodes []*stockNode) {
	t.Helper()
	poll(t, 15*time.Second, "all actual stock meta peers current before placement", func() bool {
		return stockPlacementCurrent(nodes)
	})
}

// LDM closes the old JetStream before returning its leadership. The first
// post-LDM raw read must use independently observed surviving groups, rather
// than treating the pre-LDM currentness check as a lease through that change.
// origin: hot-update S9 plan §4.2 / T61; S9 internal review INFRA-SUITE-SUPPLIES F8
func waitStockSurvivors(t *testing.T, nodes []*stockNode, retiring *stockNode) {
	t.Helper()
	var survivors []*stockNode
	for _, n := range nodes {
		if n != retiring {
			survivors = append(survivors, n)
		}
	}
	waitStockGroups(t, survivors, nodes, retiring)
}

func waitStockGroups(t *testing.T, observers, members []*stockNode, retiring *stockNode) {
	t.Helper()
	var clients []nats.JetStreamContext
	for _, n := range observers {
		client, err := dataConnection(t, n.url).Conn().JetStream(nats.MaxWait(200 * time.Millisecond))
		if err != nil {
			t.Fatal(err)
		}
		clients = append(clients, client)
	}
	limit := 15 * time.Second
	if retiring != nil {
		limit = upgradebudget.RequestLatency
	}
	poll(t, limit, "every actual stream and consumer group current", func() bool {
		for _, client := range clients {
			for _, stream := range []string{"OBJ_payload", "AUDIT"} {
				info, err := client.StreamInfo(stream)
				if err != nil || !stockGroupCurrent(info.Cluster, members, retiring) {
					return false
				}
			}
			consumer, err := client.ConsumerInfo("OBJ_payload", "original")
			if err != nil || !stockGroupCurrent(consumer.Cluster, members, retiring) {
				return false
			}
		}
		return true
	})
}

func stockGroupCurrent(group *nats.ClusterInfo, members []*stockNode, retiring *stockNode) bool {
	if !currentGroupSize(group, len(members)) {
		return false
	}
	known := make(map[string]bool, len(members))
	for _, n := range members {
		known[n.name] = true
	}
	if !known[group.Leader] || retiring != nil && group.Leader == retiring.name {
		return false
	}
	seen := map[string]bool{group.Leader: true}
	current := 1
	for _, peer := range group.Replicas {
		if peer == nil || !known[peer.Name] || seen[peer.Name] {
			return false
		}
		seen[peer.Name] = true
		if retiring != nil && peer.Name == retiring.name {
			continue
		}
		if !peer.Current || peer.Offline || peer.Lag != 0 {
			return false
		}
		current++
	}
	return current >= len(members)/2+1
}

func currentGroupSize(group *nats.ClusterInfo, size int) bool {
	return group != nil && group.Leader != "" && len(group.Replicas) == size-1
}

// This counterexample starts with a real missing member. A leader/account
// response from the two survivors must not sign the three-node prerequisite.
// origin: S9 internal review INFRA-SUITE-SUPPLIES F8
func TestNATSStockPlacementRequiresEveryPeer(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseStockPlacement(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "stock placement", g)
	testharness.AssertNoFDLeak(t, "stock placement", fd, 0)
}

func exerciseStockPlacement(t *testing.T) {
	t.Helper()
	nodes := []*stockNode{newStockNode(t, "placement1"), newStockNode(t, "placement2"), newStockNode(t, "placement3")}
	for idx, node := range nodes {
		node.writeConfig(t, nodes, true)
		if idx < 2 {
			node.start(t, "2.14.5")
		}
	}
	if stockPlacementCurrent(nodes) {
		t.Fatal("missing third stock server was admitted for R3 placement")
	}
	nodes[2].start(t, "2.14.5")
	waitStockPlacement(t, nodes)
	baseline := seedCurrentStore(t, nodes[0], len(nodes))
	waitCurrent(t, nodes)
	for _, node := range nodes {
		verifyRaw(t, dataConnection(t, node.url), baseline.object)
	}
	t.Log(fmt.Sprint("all ", len(nodes), " genuine stock peers admitted R3 without a setup retry"))
}
