//go:build linux

package cli_e2e_test

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/agentprov"
	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/brokercontrol"
	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradecli"
	natsserver "github.com/nats-io/nats-server/v2/server"
)

// The BUILT ctl drives two real single-mode broker cuts through the authorized
// transaction route over the admin socket: prepare → root stage → start →
// wait reaches completed both ways (A→B, then B→A as a second transaction).
// The broker keeps its PID and kernel start tick, /proc/<pid>/exe is the
// requested image each time, the S6 image generation advances once per cut,
// and status reports each original transaction completed with all three
// receipts and the requested effective image. The Start reply of an armed cut
// is written before the exec: accepted, its outcome left to wait (74).
// origin: hot-update S8 plan §3.1 / §4.1 / §6.1 / §11.2 P15 P17; S8 BROKER-EXPORTER R-11
func TestTransactionCLIPrepareStartWaitCompletesBrokerCut(t *testing.T) {
	// origin: hot-update S10 plan L17 / §11.1 / POLISH-B
	if runIsolatedBinaryTest(t, binaryFixtureTestBudget) {
		return
	}
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))
	bin := buildTether(t)
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) { exerciseBrokerTransactionCut(t, bin) })
	}
	testharness.AssertNoGoroutineLeak(t, "broker transaction cut", g)
	testharness.AssertNoFDLeak(t, "broker transaction cut", fd, 0)
}

func exerciseBrokerTransactionCut(t *testing.T, bin string) {
	t.Helper()
	root := t.TempDir()
	url, seeds, accountPub := startBrokerTransactionMaintenanceNATS(t, root)
	s := &realStack{t: t, bin: bin, sid: "lab", nid: "a100", ctlHome: t.TempDir(), url: url, callout: true, pin: realStackPIN}
	var err error
	if s.ctlID, err = cli.EnsureIdentity(s.ctlHome); err != nil {
		t.Fatal(err)
	}
	s.dbPath = filepath.Join(root, "state.db")
	if s.db, err = storage.Open(s.dbPath); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.db.Close() })
	// The cut carries the complete session authority map; a real PHC creation
	// salt gives the session a creation incarnation.
	realStackPINOnce.Do(func() { realStackPINHash, realStackPINErr = auth.HashPIN(realStackPIN) })
	if realStackPINErr != nil {
		t.Fatal(realStackPINErr)
	}
	if _, err := session.Create(s.db, s.sid, s.sid, s.ctlID.Fingerprint, realStackPINHash, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	prepareBrokerMaintenanceAgent(t, s, accountPub)
	brokerBin, original := filepath.Join(root, "broker"), filepath.Join(root, "broker-a")
	for _, path := range []string{brokerBin, brokerBin + ".prev", original} {
		copyTransferBinary(t, bin, path, nil)
	}
	configPath := filepath.Join(root, "broker.yaml")
	if err := os.WriteFile(configPath, []byte("broker:\n  storage:\n    db: "+s.dbPath+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	registerTransactionDomain(t, upgradecli.BrokerBootRole(root, configPath, "", uint32(os.Geteuid())), brokerBin, "tether-broker")
	admin := &adminsock.Client{Path: filepath.Join(root, "admin.sock"), Timeout: 5 * time.Second}
	brokerCmd, brokerLog := startTransferDaemon(t, brokerBin, root, "serve", "--config", configPath, "--db", s.dbPath, "--nats-url", s.url,
		"--admin-socket", admin.Path, "--tunnel-addr", "127.0.0.1:0", "--tunnel-public-host", "127.0.0.1", "--log-file", "-", "--auth-callout-seeds-dir", seeds)
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("serve log:\n%s", brokerLog.bytes())
		}
	})
	runtimeImage := func() (string, uint64, bool) {
		r, e := admin.Call(adminsock.Request{Op: adminsock.OpRuntime})
		if e != nil || !r.OK || r.Runtime.BrokerImage == nil {
			return "", 0, false
		}
		return r.Runtime.BrokerImage.RunningSHA256, r.Runtime.BrokerImage.Generation, true
	}
	if !testharness.WaitFor(t, 10*time.Second, 20*time.Millisecond, func() bool { _, _, ok := runtimeImage(); return ok }) {
		t.Fatalf("serve did not become ready: %s", brokerLog.bytes())
	}
	s.startAgent()
	t.Cleanup(s.stopAgent)
	awaitBrokerMaintenanceAgent(t, s)
	pid := brokerCmd.Process.Pid
	ticks, err := upgradeProcessIdentity(pid)
	if err != nil {
		t.Fatal(err)
	}
	for index, step := range []struct {
		name    string
		trailer []byte
		from    string
	}{{"forward", []byte("\nS8-broker-image-B\n"), brokerBin}, {"reverse", nil, original}} {
		// The previous slot is the running image; the candidate is staged at
		// the registered canonical path, as the installer stages it.
		copyTransferBinary(t, brokerBin, brokerBin+".prev", nil)
		copyTransferBinary(t, step.from, brokerBin+".candidate", step.trailer)
		if err := os.Rename(brokerBin+".candidate", brokerBin); err != nil {
			t.Fatal(err)
		}
		sha := upgradeFileSHA(t, brokerBin)
		tx, start, err := s.brokerTransactionCut(admin.Path, brokerBin, sha, pid)
		if err != nil {
			t.Fatalf("%s broker cut: %s%v", step.name, transactionDependency(err), err)
		}
		if start.rc != 74 || !strings.Contains(start.stdout+start.stderr, "broker_activation_dispatched") {
			t.Fatalf("%s Start reply is not the armed dispatch: rc=%d %s%s", step.name, start.rc, start.stdout, start.stderr)
		}
		if got, generation, ok := runtimeImage(); !ok || got != sha || generation != uint64(index+2) {
			t.Fatalf("%s image after wait: sha=%s generation=%d ok=%t (want %s/%d)", step.name, got, generation, ok, sha, index+2)
		}
		if got, e := upgradeProcessIdentity(pid); e != nil || got != ticks {
			t.Fatalf("%s: broker process identity changed: %s/%s %v", step.name, got, ticks, e)
		}
		if got := upgradeFileSHA(t, fmt.Sprintf("/proc/%d/exe", pid)); got != sha {
			t.Fatalf("%s: /proc/%d/exe is %s, want %s", step.name, pid, got, sha)
		}
		status := s.transactionCtl(30*time.Second, nil, "broker", "upgrade", "status", "--socket", admin.Path, "--tx-id", tx, "--json", "--home", s.ctlHome)
		if status.rc != 0 {
			t.Fatalf("%s status %s rc=%d: %s%s", step.name, tx, status.rc, status.stdout, status.stderr)
		}
		for _, want := range []string{`"phase":"completed"`, `"outcome":"success"`, `"control_ready"`, `"resources_migrated"`, `"old_generation_released"`, `"effective_sha":"` + sha + `"`} {
			if !strings.Contains(status.stdout, want) {
				t.Fatalf("%s status %s lacks %s:\n%s", step.name, tx, want, status.stdout)
			}
		}
	}
}

// Use the product issuer and agent callout ACLs for the observer command. The
// account pin lets the agent authenticate the signed challenge and proof.
// origin: S6 lead decision D-C / plan §7.1
func startBrokerTransactionMaintenanceNATS(t *testing.T, root string) (url, seeds, accountPub string) {
	t.Helper()
	brokerSeed, accountSeed := mintRealStackSeeds(t)
	brokerPub, err := auth.PublicKeyFromSeed(brokerSeed)
	if err != nil {
		t.Fatal(err)
	}
	accountPub, err = auth.PublicKeyFromSeed(accountSeed)
	if err != nil {
		t.Fatal(err)
	}
	seeds = filepath.Join(root, "seeds")
	if err := os.Mkdir(seeds, 0700); err != nil {
		t.Fatal(err)
	}
	for name, seed := range map[string][]byte{"broker.nk": brokerSeed, "account.nk": accountSeed} {
		if err := os.WriteFile(filepath.Join(seeds, name), seed, 0600); err != nil {
			t.Fatal(err)
		}
	}
	ns, err := natsserver.NewServer(&natsserver.Options{Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: filepath.Join(root, "js"), NoLog: true, NoSigs: true,
		Nkeys:       []*natsserver.NkeyUser{{Nkey: brokerPub}},
		AuthCallout: &natsserver.AuthCallout{Issuer: accountPub, Account: "$G", AuthUsers: []string{brokerPub}}})
	if err != nil {
		t.Fatal(err)
	}
	ns.Start()
	t.Cleanup(func() { ns.Shutdown(); ns.WaitForShutdown() })
	if !ns.ReadyForConnections(5 * time.Second) {
		t.Fatal("broker transaction NATS did not start")
	}
	return ns.ClientURL(), seeds, accountPub
}

func prepareBrokerMaintenanceAgent(t *testing.T, s *realStack, accountPub string) {
	t.Helper()
	// A planned single cut has the same signed installed-hold prerequisite as
	// a clustered cut. Keep a real observer alive through both transactions.
	// origin: S6 lead decision D-C / plan §7.1
	if s.agentHome == "" {
		s.agentHome = t.TempDir()
	}
	agentID, err := cli.EnsureAgentIdentity(s.agentHome, s.sid)
	if err != nil {
		t.Fatal(err)
	}
	if err := agentprov.Provision(s.db, s.sid, s.nid, agentID.Fingerprint, time.Now()); err != nil {
		t.Fatal(err)
	}
	agentConfig := filepath.Join(s.agentHome, "agent", s.sid, "agent.yaml")
	if err := os.WriteFile(agentConfig, []byte("account_pub: "+accountPub+"\nproxy:\n  participate: false\n"), 0600); err != nil {
		t.Fatal(err)
	}
}

func awaitBrokerMaintenanceAgent(t *testing.T, s *realStack) {
	t.Helper()
	if !testharness.WaitFor(t, 15*time.Second, 10*time.Millisecond, func() bool {
		peers, err := brokercontrol.MaintenanceAgents(s.db, "", true)
		return err == nil && len(peers) == 1 && peers[0].NID == s.nid
	}) {
		t.Fatal("single broker transaction agent did not install maintenance capability")
	}
}
