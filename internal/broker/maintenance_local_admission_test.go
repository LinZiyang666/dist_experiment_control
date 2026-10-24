//go:build linux

package broker

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/agent"
	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/brokercontrol"
	"github.com/LinZiyang666/tether/internal/brokerimage"
	"github.com/LinZiyang666/tether/internal/brokermaint"
	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/clusterroster"
	"github.com/LinZiyang666/tether/internal/node"
	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

// origin: S6 internal review rv-final F4
func TestLocalReloadRefusesFleetWithoutMaintenanceCapability(t *testing.T) {
	// origin: hot-update S10 plan §11.1 / TEST-BUDGET
	if runIsolatedBrokerTest(t) {
		return
	}
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseLocalReloadRefusesFleetWithoutMaintenanceCapability(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "TestLocalReloadRefusesFleetWithoutMaintenanceCapability x5", g)
	testharness.AssertNoFDLeak(t, "TestLocalReloadRefusesFleetWithoutMaintenanceCapability x5", fd, 0)
}

func exerciseLocalReloadRefusesFleetWithoutMaintenanceCapability(t *testing.T) {
	t.Helper()
	t.Run("single", func(t *testing.T) {
		db := openDBWithSession(t, "legacy-session")
		b, err := New(Config{DB: db, NATSURL: "nats://127.0.0.1:1", Logger: silentLogger()})
		if err != nil {
			t.Fatal(err)
		}
		if err := node.Register(db, node.RegisterInput{SID: "legacy-session", NID: "legacy-agent", ProtoVersion: proto.ProtoVersion}, b.cfg.Now()); err != nil {
			t.Fatal(err)
		}
		d, err := brokerCutDeadline()
		if err != nil {
			t.Fatal(err)
		}
		if err := captureBrokerReloadAdmission(b, adminsock.Request{Op: adminsock.OpBrokerUpgradeReload}, d); err == nil {
			t.Fatal("single admitted a planned hot cut with a registered legacy agent and no signed installation ACK")
		}
	})
	t.Run("sole-voter", func(t *testing.T) {
		b, n := newClusterProxyBroker(t, "legacy-session", "owner")
		if err := n.Propose(func(db *sql.DB) (*cluster.Command, error) {
			return node.PlanRegister(db, node.RegisterInput{SID: "legacy-session", NID: "legacy-agent", ProtoVersion: proto.ProtoVersion}, b.cfg.Now())
		}); err != nil {
			t.Fatal(err)
		}
		d, err := brokerCutDeadline()
		if err != nil {
			t.Fatal(err)
		}
		if err := captureBrokerReloadAdmission(b, adminsock.Request{Op: adminsock.OpBrokerUpgradeReload}, d); err == nil {
			t.Fatal("sole voter admitted a planned hot cut with a registered legacy agent and no signed installation ACK")
		}
	})
}

// origin: S6 internal review rv-final F4
func TestLocalReloadRequiresSignedInstalledAgentAck(t *testing.T) {
	// origin: hot-update S10 plan §11.1 / TEST-BUDGET
	if runIsolatedBrokerTest(t) {
		return
	}
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseLocalInstalledAgentAck(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "local installed maintenance ACK x5", g)
	testharness.AssertNoFDLeak(t, "local installed maintenance ACK x5", fd, 0)
}

func exerciseLocalInstalledAgentAck(t *testing.T) {
	t.Helper()
	for _, single := range []bool{true, false} {
		t.Run(fmt.Sprint(single), func(t *testing.T) {
			var b *Broker
			if single {
				var err error
				b, err = New(Config{DB: openDBWithSession(t, "local"), NATSURL: "nats://127.0.0.1:1", Logger: silentLogger()})
				if err != nil {
					t.Fatal(err)
				}
			} else {
				b, _ = newClusterProxyBroker(t, "local", "owner")
			}
			issuer, err := auth.GenerateUserSeed()
			if err != nil {
				t.Fatal(err)
			}
			seed, err := auth.GenerateUserSeed()
			if err != nil {
				t.Fatal(err)
			}
			pub, _ := auth.PublicKeyFromSeed(seed)
			issuerPub, _ := auth.PublicKeyFromSeed(issuer)
			ns, err := natsserver.NewServer(&natsserver.Options{Host: "127.0.0.1", Port: -1, ServerName: "local-maintenance", Nkeys: []*natsserver.NkeyUser{{Nkey: pub}, {Nkey: issuerPub}}})
			if err != nil {
				t.Fatal(err)
			}
			ns.Start()
			t.Cleanup(func() { ns.Shutdown(); ns.WaitForShutdown() })
			if !ns.ReadyForConnections(5 * time.Second) {
				t.Fatal("NATS not ready")
			}
			nc, err := nats.Connect(ns.ClientURL(), nats.Nkey(issuerPub, func(nonce []byte) ([]byte, error) { return auth.SignWithSeed(issuer, nonce) }))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(nc.Close)
			b.nc.Store(nc)
			b.cfg.AuthCallout = &AuthCalloutConfig{AccountSeed: issuer}
			if !single {
				seedClusterNode(t, &Broker{cfg: Config{DB: b.cl.node.DB()}}, b.selfID, ns.Name(), "127.0.0.1:7443", "sha256:fixture", "VOTER")
			}
			fp, _ := auth.FingerprintFromActor(pub)
			if err := maintenanceWrite(b, func(*sql.DB) (*cluster.Command, error) {
				return cluster.NewCommand(cluster.OpAgentProvision, cluster.Stmt(`INSERT INTO agent_provisioning(sid,nid,agent_fp) VALUES('local','capable',`+cluster.MustLitText(fp)+`)`)), nil
			}); err != nil {
				t.Fatal(err)
			}
			if err := maintenanceWrite(b, func(db *sql.DB) (*cluster.Command, error) {
				return node.PlanRegister(db, node.RegisterInput{SID: "local", NID: "capable", ProtoVersion: proto.ProtoVersion, NatsServer: ns.Name()}, b.cfg.Now())
			}); err != nil {
				t.Fatal(err)
			}
			rememberAgentMaintenance(b, "local", "capable", proto.NodeRegisterReq{BrokerImageMaintenance: brokermaint.Capability, BrokerImageMaintenanceKey: pub})
			if err := InitializeBrokerImageStatus(b, 3, ""); err != nil {
				t.Fatal(err)
			}
			d, _ := brokerCutDeadline()
			if err := captureBrokerReloadAdmission(b, adminsock.Request{Op: adminsock.OpBrokerUpgradeReload}, d); !errors.Is(err, brokerimage.ErrMaintenanceObserverUnsupported) {
				t.Fatalf("no installed ACK admitted: %v", err)
			}
			var silent atomic.Bool
			misses := make(chan struct{}, 16)
			register, err := nc.Subscribe(proto.SubjectPrefix+".ctrl.s.*.node.*.register.req", func(msg *nats.Msg) {
				var req proto.NodeRegisterReq
				_ = json.Unmarshal(msg.Data, &req)
				if silent.Load() && req.RosterRefreshOnly {
					select {
					case misses <- struct{}{}:
					default:
					}
					return
				}
				b.handleRegister(msg)
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = register.Unsubscribe() })
			if err := nc.FlushTimeout(time.Second); err != nil {
				t.Fatal(err)
			}
			agentHome := t.TempDir()
			if single {
				// A retained signed roster starts the real refresh loop even after
				// returning to single mode; the current nil-roster route stays single.
				roster, err := clusterroster.Build(issuer, issuerPub, 1, []proto.RosterBroker{{NodeID: "former", PublicHost: "127.0.0.1", Phase: proto.RosterPhaseVoter}}, time.Now().UTC().Format(time.RFC3339Nano), time.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano))
				if err != nil {
					t.Fatal(err)
				}
				if err := agent.WriteRosterCache(agentHome, "local", &agent.RosterCache{PinAccountPub: issuerPub, Generation: 1, Roster: roster}); err != nil {
					t.Fatal(err)
				}
			}
			a, err := agent.New(agent.Config{NATSURL: ns.ClientURL(), SID: "local", NID: "capable", Home: agentHome, ProxyOptOut: true, RemoteFSMode: "off", AccountPub: issuerPub, Identity: &cli.Identity{Seed: seed, PublicKey: pub}, Logger: silentLogger(), RegisterTimeout: 200 * time.Millisecond, RosterRefreshInterval: time.Hour})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan error, 1)
			go func() { done <- a.Run(ctx) }()
			t.Cleanup(func() {
				cancel()
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Error("agent did not join")
				}
			})
			until := time.Now().Add(5 * time.Second)
			for {
				_, err := nc.Request(proto.SubjCmdForwarded("local", "capable", proto.ClaimProbeVerb), nil, 50*time.Millisecond)
				if err == nil {
					break
				}
				if time.Now().After(until) {
					t.Fatalf("agent not ready: %v", err)
				}
			}
			d, _ = brokerCutDeadline()
			if err := captureBrokerReloadAdmission(b, adminsock.Request{Op: adminsock.OpBrokerUpgradeReload}, d); err != nil {
				t.Fatalf("real signed installed ACK refused: %v", err)
			}
			lease := b.imageControl.localMaintenance
			if lease.Record.Phase != brokermaint.MaintActive || lease.Proof.Nonce == "" || lease.ExpiresLocalBootNS > lease.Record.ExpiresBootNS {
				t.Fatal("missing fixed local lease")
			}
			if single && (lease.Record.IngressServerID != nc.ConnectedServerId() || lease.Record.Home != "") {
				t.Fatal("single was not bound to actual ingress")
			}
			if err := validateBrokerReloadRole(b, false); err != nil {
				t.Fatal(err)
			}
			projection, err := projectBrokerTransferCut(b.imageControl)
			if err != nil || len(projection.Maintenance) != 1 || projection.Maintenance[0] != lease {
				t.Fatalf("local lease lost from current cut: %v", err)
			}
			bound, err := brokercontrol.BindLocalMaintenance(b.read().SQL(), lease)
			if err != nil || bound != lease {
				t.Fatalf("local owner failed to rebind: %v", err)
			}
			if err := maintenanceWrite(b, func(*sql.DB) (*cluster.Command, error) {
				return cluster.NewCommand(cluster.OpClusterMetaClear, cluster.Stmt("DELETE FROM cluster_meta WHERE key="+cluster.MustLitText("broker_image_local."+lease.Record.Home))), nil
			}); err != nil {
				t.Fatal(err)
			}
			bound, err = brokercontrol.BindLocalMaintenance(b.read().SQL(), lease)
			if err != nil || bound.Record.CutID != "" {
				t.Fatalf("holder prefix claimed a remote owner: %v", err)
			}
			if err := maintenanceWrite(b, func(*sql.DB) (*cluster.Command, error) {
				return cluster.NewCommand(cluster.OpClusterMetaSet, cluster.Stmt("INSERT INTO cluster_meta(key,value) VALUES("+cluster.MustLitText("broker_image_local."+lease.Record.Home)+","+cluster.MustLitText(lease.Record.CutID)+")")), nil
			}); err != nil {
				t.Fatal(err)
			}
			before := localAgentConnection(t, ns, pub)
			silent.Store(true)
			for wake := 0; wake < 3; wake++ {
				if err := nc.Publish(proto.SubjSysEvents, []byte(`{"type":"nats_topology_changed"}`)); err != nil {
					t.Fatal(err)
				}
				select {
				case <-misses:
				case <-time.After(time.Second):
					t.Fatal("topology wake did not refresh")
				}
				// The prior failed attempt finishes before the next wake is queued.
				time.Sleep(250 * time.Millisecond)
			}
			if after := localAgentConnection(t, ns, pub); after != before {
				t.Fatalf("maintenance changed original connection: %d to %d", before, after)
			}
			if !testharness.WaitFor(t, 9*time.Second, 10*time.Millisecond, func() bool { now, _ := procrequest.BootNow(); return now >= lease.Record.ExpiresBootNS }) {
				t.Fatal("fixed expiry did not arrive")
			}
			if err := validateBrokerReloadRole(b, false); !errors.Is(err, brokermaint.ErrExpired) {
				t.Fatalf("expiry refreshed: %v", err)
			}
			if b.imageControl.localMaintenance.ExpiresLocalBootNS != lease.ExpiresLocalBootNS {
				t.Fatal("wake renewed issuer lifetime")
			}
			// Expiry restores admission for a new logical cut without renewing
			// the original challenge/hold or retaining an expired local holder.
			d, _ = brokerCutDeadline()
			if err := captureBrokerReloadAdmission(b, adminsock.Request{Op: adminsock.OpBrokerUpgradeReload}, d); err != nil {
				t.Fatalf("expired local owner stranded the next cut: %v", err)
			}
			currentLease := b.imageControl.localMaintenance
			if currentLease.Record.CutID == lease.Record.CutID {
				t.Fatal("new admission renewed the old cut")
			}
			var n *cluster.Node
			if b.cl != nil {
				n = b.cl.node
			}
			if err := brokercontrol.FinishLocalMaintenance(b.cfg.DB, n, currentLease, b.cfg.Now()); err != nil {
				t.Fatal(err)
			}
			if !single {
				lock, err := cluster.ReadUpgradeLock(n.RODB())
				if err != nil || lock.Marker != "" {
					t.Fatalf("local holder retained: %+v %v", lock, err)
				}
			}
		})
	}
}

func localAgentConnection(t *testing.T, ns *natsserver.Server, pub string) uint64 {
	t.Helper()
	info, err := ns.Connz(&natsserver.ConnzOptions{Username: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, conn := range info.Conns {
		if conn.AuthorizedUser == pub {
			return conn.Cid
		}
	}
	t.Fatal("original agent connection missing")
	return 0
}
