//go:build linux && amd64 && infranats_integration

package infranats_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/infracontract/contractstub"
	"github.com/LinZiyang666/tether/internal/infrahost"
	"github.com/LinZiyang666/tether/internal/infratransport"
	"github.com/LinZiyang666/tether/internal/natsconf"
	"github.com/LinZiyang666/tether/test/infraharness"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
	"golang.org/x/sys/unix"
)

type stockNode struct {
	name, root, config, store, url string
	port, route                    int
	proc                           *infraharness.Process
	current                        c.Process
	artifact                       c.Artifact
	log                            *os.File
	readKey, stepKey               nkeys.KeyPair
	principals                     natsconf.SystemPrincipals
	peers                          []*stockNode
}

func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	return port
}

func newStockNode(t *testing.T, name string) *stockNode {
	t.Helper()
	root := t.TempDir()
	n := &stockNode{name: name, root: root, config: filepath.Join(root, "nats.conf"), store: filepath.Join(root, "store"), port: freePort(t), route: freePort(t)}
	n.url = fmt.Sprintf("nats://127.0.0.1:%d", n.port)
	if err := os.Mkdir(n.store, 0700); err != nil {
		t.Fatal(err)
	}
	var err error
	n.readKey, err = nkeys.CreateUser()
	if err != nil {
		t.Fatal(err)
	}
	n.stepKey, err = nkeys.CreateUser()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(n.readKey.Wipe)
	t.Cleanup(n.stepKey.Wipe)
	n.principals.ReadNkey, err = n.readKey.PublicKey()
	if err != nil {
		t.Fatal(err)
	}
	n.principals.StepNkey, err = n.stepKey.PublicKey()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cleanupStockNode(t, n) })
	return n
}

func (n *stockNode) writeConfig(t *testing.T, peers []*stockNode, principals bool) {
	t.Helper()
	n.peers = append([]*stockNode(nil), peers...)
	text := fmt.Sprintf("server_name: %q\nlisten: 127.0.0.1:%d\njetstream { store_dir: %q }\n", n.name, n.port, n.store)
	if len(peers) > 1 {
		var routes []string
		for _, peer := range peers {
			if peer != n {
				routes = append(routes, fmt.Sprintf("nats://127.0.0.1:%d", peer.route))
			}
		}
		encoded, err := json.Marshal(routes)
		if err != nil {
			t.Fatal(err)
		}
		text += fmt.Sprintf("cluster { name: infra, listen: 127.0.0.1:%d, routes: %s }\nlame_duck_duration: 30s\nlame_duck_grace_period: 5s\n", n.route, encoded)
	}
	if principals {
		block, err := natsconf.SystemPrincipalBlock(n.principals)
		if err != nil {
			t.Fatal(err)
		}
		text += block
	}
	if err := os.WriteFile(n.config, []byte(text), 0600); err != nil {
		t.Fatal(err)
	}
}

func (n *stockNode) start(t *testing.T, version string) {
	t.Helper()
	if n.proc != nil {
		t.Fatal("attempt to double-open a live stock store")
	}
	binary, artifact := stockArtifact(t, version)
	log, err := os.CreateTemp(n.root, "stock-log-")
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(t.Context(), binary, "--config", n.config)
	cmd.Dir = n.root
	cmd.Env = append(privateEnvironment(), "INVOCATION_ID="+invocation(n.name, version))
	cmd.Stdout, cmd.Stderr = log, log
	proc, err := infraharness.StartProcess(cmd)
	if err != nil {
		_ = log.Close()
		t.Fatal(err)
	}
	n.proc, n.log, n.artifact = proc, log, artifact
	t.Cleanup(func() {
		if t.Failed() {
			if output, err := os.ReadFile(log.Name()); err == nil {
				t.Logf("stock fixture process log: %s", output)
			}
		}
	})
	t.Cleanup(func() {
		if err := log.Close(); err != nil {
			t.Error(err)
		}
	})
	poll(t, 10*time.Second, "stock INFO and running image", func() bool {
		nc, err := nats.Connect(n.url, nats.NoReconnect(), nats.Timeout(100*time.Millisecond))
		if err != nil {
			return false
		}
		defer nc.Close()
		if nc.ConnectedServerVersion() != version {
			t.Fatal("INFO version does not match the official candidate")
		}
		current, err := infrahost.ReadProcess(int32(proc.Identity().PID), budget(t, time.Second))
		if err != nil {
			t.Fatalf("stock running identity: %v", err)
		}
		if current.ExeSHA256 != artifact.BinarySHA256 || current.BootID != proc.Identity().BootID || current.StartTicks != proc.Identity().StartTicks {
			t.Fatal("disk candidate differs from actual running image")
		}
		n.current = current
		return true
	})
}

func invocation(name, version string) string {
	sum := sha256.Sum256([]byte(name + "/" + version))
	return hex.EncodeToString(sum[:16])
}

func poll(t *testing.T, duration time.Duration, detail string, ready func() bool) {
	t.Helper()
	deadline := time.NewTimer(duration)
	defer deadline.Stop()
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		if ready() {
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("deadline waiting for %s", detail)
		case <-tick.C:
		case <-t.Context().Done():
			t.Fatal(t.Context().Err())
		}
	}
}

func (n *stockNode) stop(t *testing.T, op c.HostOp) {
	t.Helper()
	if n.proc == nil {
		return
	}
	pin, err := infrahost.PinProcess(n.current, budget(t, time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := pin.Close(); err != nil {
			t.Error(err)
		}
	}()
	if err := pin.Signal(op, budget(t, time.Second), nil); err != nil {
		t.Fatal(err)
	}
	if op == c.SignalLDM {
		return
	}
	if err := n.proc.Wait(); err != nil {
		var exit *exec.ExitError
		if !errorsAsKilled(err, &exit) {
			t.Fatalf("unexpected supervised stop: %v", err)
		}
	}
	poll(t, time.Second, "exact old pidfd exit", func() bool {
		exited, err := pin.Exited(budget(t, time.Second))
		if err != nil {
			t.Fatal(err)
		}
		return exited
	})
	if exists, err := infraharness.ProcessExists(n.proc.Identity()); err != nil || exists {
		t.Fatal("old stock process or zombie survived joined retirement")
	}
	n.proc = nil
}

func (n *stockNode) reload(t *testing.T) {
	t.Helper()
	fd, err := unix.PidfdOpen(int(n.current.PID), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := unix.Close(fd); err != nil {
			t.Error(err)
		}
	}()
	before, err := infrahost.ReadProcess(n.current.PID, budget(t, time.Second))
	if err != nil || before != n.current {
		t.Fatal("reload lost its exact running incarnation")
	}
	if err := unix.PidfdSendSignal(fd, unix.SIGHUP, nil, 0); err != nil {
		t.Fatal(err)
	}
	poll(t, 3*time.Second, "least-privilege SYS reload", func() bool {
		pub, _ := n.readKey.PublicKey()
		nc, err := nats.Connect(n.url, nats.Nkey(pub, n.readKey.Sign), nats.NoReconnect(), nats.Timeout(100*time.Millisecond), nats.CustomInboxPrefix("_INBOX.tether.infra.sys."+pub))
		if err != nil {
			return false
		}
		defer nc.Close()
		_, err = nc.Request("$SYS.REQ.SERVER."+nc.ConnectedServerId()+".VARZ", nil, time.Second)
		return err == nil
	})
	after, err := infrahost.ReadProcess(n.current.PID, budget(t, time.Second))
	if err != nil || after != before {
		t.Fatal("SYS retrofit secretly restarted or replaced NATS")
	}
}

// This is L2 same-boot stock compatibility. RestartSec/systemd, sole-voter
// broker operation and global pre-repair admission need the production cell
// below; L2 observations alone cannot sign those responsibilities.
// origin: hot-update S9 plan §4.3 / §9.2 / T60
func TestNATSSingleAdjacentCycle(t *testing.T) {
	for _, mode := range []string{"default_single", "sole_voter_standalone_JS"} {
		t.Run(mode, func(t *testing.T) {
			n := newStockNode(t, mode)
			n.writeConfig(t, nil, false)
			n.start(t, "2.14.5")
			baseline := seedCurrentStore(t, n, 1)
			n.writeConfig(t, nil, true)
			n.reload(t)
			for hop, version := range []string{"2.14.6", "2.14.5", "2.14.6"} {
				before := n.current
				n.stop(t, c.SignalStop)
				n.start(t, version)
				if n.current.PID == before.PID || n.current.ExeSHA256 == before.ExeSHA256 {
					t.Fatal("stock cut left the old running image")
				}
				verifyCurrentStore(t, n, baseline, hop+1)
			}
			t.Run("product_recovery_and_transaction", func(t *testing.T) {
				productService(t) // Fail before creating a product fixture when its binding is absent.
				original := newStockNode(t, mode+"product")
				original.writeConfig(t, nil, false)
				original.start(t, "2.14.5")
				exerciseProductCycle(t, original, seedCurrentStore(t, original, 1), false)
			})
		})
	}
}

func waitCurrent(t *testing.T, nodes []*stockNode) {
	t.Helper()
	waitStockGroups(t, nodes, nodes, nil)
}

// The three stock processes use separate stores and observed IDs. They share
// this L2 host, so their currentness proves cross-version JS compatibility,
// while the product multi-host readiness/cut cell stays explicitly pending.
// origin: hot-update S9 plan §4.2 / §9.2 / T61 / M04/M15/M17
func TestNATSThreeServerAdjacentCycle(t *testing.T) {
	t.Run("stock_adjacent_cycle", func(t *testing.T) {
		nodes := []*stockNode{newStockNode(t, "n1"), newStockNode(t, "n2"), newStockNode(t, "n3")}
		for _, n := range nodes {
			n.writeConfig(t, nodes, true)
			n.start(t, "2.14.5")
		}
		poll(t, 15*time.Second, "stock JS meta readiness", func() bool {
			js := dataConnection(t, nodes[0].url)
			defer js.Conn().Close()
			_, err := js.AccountInfo(t.Context())
			return err == nil
		})
		baseline := seedCurrentStore(t, nodes[0], 3)
		waitCurrent(t, nodes)
		hop := 0
		for _, version := range []string{"2.14.6", "2.14.5", "2.14.6"} {
			for idx, n := range nodes {
				survivor := nodes[(idx+1)%len(nodes)]
				// An actual successor CONNECT/PONG and private request precede LDM.
				before := dataConnection(t, n.url).Conn()
				oldCID, err := before.GetClientID()
				if err != nil {
					t.Fatal(err)
				}
				oldServer := before.ConnectedServerId()
				transport, err := infratransport.New(uint64(hop+1), budget(t, time.Second), nil, nil)
				if err != nil {
					t.Fatal(err)
				}
				if err := infratransport.Move(transport, before, budget(t, time.Second), nil); err != nil {
					t.Fatal(err)
				}
				successor := dataConnection(t, survivor.url).Conn()
				if err := successor.FlushTimeout(time.Second); err != nil {
					t.Fatal(err)
				}
				cid, err := successor.GetClientID()
				if err != nil || cid == 0 || oldCID == 0 || successor.ConnectedServerId() == oldServer {
					t.Fatal("pool edit was substituted for a successor attachment")
				}
				if _, err := successor.Request("$JS.API.INFO", nil, time.Second); err != nil {
					t.Fatal("successor business RPC missing before LDM")
				}
				n.stop(t, c.SignalLDM)
				waitStockSurvivors(t, nodes, n)
				// LDM disables old JS. The survivor must still read original receipts.
				verifyRaw(t, dataConnection(t, survivor.url), baseline.object)
				n.stop(t, c.SignalStop)
				n.start(t, version)
				waitCurrent(t, nodes)
				hop++
				t.Logf("stock hop=%d node=%s version=%s pid=%d sha=%x", hop, n.name, version, n.current.PID, n.current.ExeSHA256)
				// Read the raw current multiset on every actual host after every hop.
				for _, peer := range nodes {
					verifyRaw(t, dataConnection(t, peer.url), baseline.object)
				}
				legacy, err := dataConnection(t, n.url).Conn().JetStream()
				if err != nil {
					t.Fatal(err)
				}
				if _, err := legacy.Publish("audit.current", []byte(fmt.Sprint("current-hop-", hop))); err != nil {
					t.Fatal(err)
				}
				for seq := uint64(3); seq <= uint64(2+hop); seq++ {
					if _, err := legacy.GetMsg("AUDIT", seq); err != nil {
						t.Fatal("current rollback lost new-generation audit writes")
					}
				}
			}
		}
	})
	t.Run("product_multi_host_cut", func(t *testing.T) {
		productService(t) // P22/P40 must supply the real multi-host fixture binding.
		originals := []*stockNode{newStockNode(t, "productn1"), newStockNode(t, "productn2"), newStockNode(t, "productn3")}
		for _, node := range originals {
			node.writeConfig(t, originals, true)
			node.start(t, "2.14.5")
		}
		state := seedCurrentStore(t, originals[0], 3)
		waitCurrent(t, originals)
		exerciseProductCycle(t, originals[0], state, true)
	})
}

// The full three-hop assertions consume only frozen contracts. The host owner
// must independently return current process/census data, and the product
// Recovery binding must verify originals before any producer can repair them.
// origin: hot-update S9 plan §4.2–§4.6 / §7.2 / T60/T61
func exerciseProductCycle(t *testing.T, n *stockNode, b *storeBaseline, clustered bool) {
	witness := &recoveryWitness{t: t, node: n, baseline: b}
	service, deps := productService(t, func(deps *c.Dependencies) {
		witness.Recovery = deps.Recovery
		deps.Recovery = witness
	})
	for hop, version := range []string{"2.14.6", "2.14.5", "2.14.6"} {
		action := c.Upgrade
		if hop == 1 {
			action = c.Rollback
		}
		plan := stockPlan(t, n, byte(20+hop), version, action)
		deadline := budget(t, 120*time.Second)
		before := n.current
		status, err := service.Start(plan, deadline, nil)
		if err != nil || status.Phase != c.Completed || status.Tx != plan.Tx || status.Step != plan.Step || status.Before != before || status.Revision == 0 {
			t.Fatalf("real product adjacent hop not completed: phase=%d code=%s error=%v", status.Phase, status.Code, err)
		}
		current, err := infrahost.ReadProcess(status.Current.PID, budget(t, time.Second))
		if err != nil || current != status.Current || current.ExeSHA256 != plan.Candidate.BinarySHA256 || current.PID == before.PID || current.BootID != before.BootID {
			t.Fatal("product completed from disk-only or old-alive evidence")
		}
		pin, err := infrahost.PinProcess(before, budget(t, time.Second))
		if err == nil {
			_ = pin.Close()
			t.Fatal("completed step still has its before process")
		}
		census, err := deps.Host.Inspect(plan.Domain, budget(t, time.Second))
		if err != nil || !census.Complete || census.OldOwnerPending != 0 || len(census.Processes) == 0 || census.Digest == (c.Digest{}) {
			t.Fatal("completed step lacks independent retirement census")
		}
		if err := infrahost.VerifyRetired([]c.Process{before}, census, false); err != nil {
			t.Fatal(err)
		}
		if witness.verified != hop+1 {
			t.Fatal("product completed before invoking its actual pre-repair verifier")
		}
		n.current, n.artifact = current, plan.Candidate
		verifyCurrentStore(t, n, b, hop+1)
		loaded, err := service.Status(plan.Tx, budget(t, time.Second))
		rootStatus, rootErr := deps.Tx.Load(plan.Tx, budget(t, time.Second))
		if err != nil || rootErr != nil || loaded.Phase != c.Completed || rootStatus.Phase != loaded.Phase || rootStatus.Proof != loaded.Proof {
			t.Fatal("S8/root status diverged from the actual completed process")
		}
		if clustered && status.Census.LiveBusiness == 0 {
			t.Fatal("clustered cut lost its required live cohort")
		}
	}
}

// hashOpenFile reads bytes through an already opened descriptor, without changing
// its OFD cursor. The launcher tests use it for executable and config custody.
func hashOpenFile(t *testing.T, f *os.File) c.Digest {
	t.Helper()
	stat, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	h := sha256.New()
	if _, err := io.Copy(h, io.NewSectionReader(f, 0, stat.Size())); err != nil {
		t.Fatal(err)
	}
	var digest c.Digest
	copy(digest[:], h.Sum(nil))
	return digest
}

// P00 is the only frozen service construction seam in this candidate. This
// failure adapter deliberately prevents the L2 stock observations from signing
// an assembled S8/root/broker cut. P20/P22/P23/P40 must supply a real sandbox
// Dependencies binding here, including P18's producer/replay closure, before
// the dedicated job can pass. No in-test transaction or successful stub is used.
func productService(t *testing.T, decorate ...func(*c.Dependencies)) (c.Service, c.Dependencies) {
	t.Helper()
	deps := contractstub.NewDependencies()
	for _, apply := range decorate {
		apply(&deps)
	}
	service, err := contractstub.NewService(deps, nil, nil)
	if err != nil {
		t.Fatalf("NOT_RUN pending P18/P20/P22/P23/P40: sandbox product service binding: %v", err)
	}
	if service == nil {
		t.Fatal("product service factory returned no owner")
	}
	return service, deps
}
