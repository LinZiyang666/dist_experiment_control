//go:build linux

package cli_e2e_test

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/natsinbox"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradecli"
	"github.com/LinZiyang666/tether/internal/upgradelock"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/internal/xferclient"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats.go"
)

// Both production entrypoints own their defaults. No transfer factory, actor,
// controller, or transport is injected. The original ctl invocation stays
// alive while both daemon PIDs exec, then verifies exact bytes and history ACK.
// origin: hot-update S7 plan §8.2 / §11 / T35 / T36 / T37
func TestTransferServeAndAgentImagesKeepCapablePushPull(t *testing.T) {
	// origin: hot-update S10 plan §11.1 / TEST-BUDGET
	if runIsolatedBinaryTest(t, binaryFixtureTestBudget) {
		return
	}
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))
	a := []upgradeArtifact{buildUpgradeArtifact(t, "v0.0.0-s7-activation"), buildUpgradeArtifact(t, "v0.0.0-s7-activation-next")}
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) { exerciseTransferServeImages(t, a) })
	}
	testharness.AssertNoGoroutineLeak(t, "serve transfer images", g)
	testharness.AssertNoFDLeak(t, "serve transfer images", fd, 0)
}

func exerciseTransferServeImages(t *testing.T, artifacts []upgradeArtifact) {
	t.Helper()
	srv := serveUpgradeArtifacts(t, artifacts...)
	root := t.TempDir()
	url, seeds, accountPub := startBrokerTransactionMaintenanceNATS(t, root)
	s := &realStack{t: t, bin: buildTether(t), sid: "lab", nid: "a100", ctlHome: t.TempDir(), agentHome: t.TempDir(),
		url: url, callout: true, pin: realStackPIN, agentArgs: []string{"--upgrade-url-allow", srv.URL + "/"}}
	var err error
	s.ctlID, err = cli.EnsureIdentity(s.ctlHome)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(s.ctlHome, "current_session"), []byte("lab\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	s.dbPath = filepath.Join(root, "state.db")
	s.db, err = storage.Open(s.dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.db.Close() })
	// Both S8 cuts carry the session's incarnation, derived from its real PHC
	// creation salt; a placeholder PIN hash has none, and every authorized cut
	// would defer as authority_incarnation_unknown. origin: S8 BROKER-EXPORTER R-11
	realStackPINOnce.Do(func() { realStackPINHash, realStackPINErr = auth.HashPIN(realStackPIN) })
	if realStackPINErr != nil {
		t.Fatal(realStackPINErr)
	}
	if _, err := session.Create(s.db, s.sid, s.sid, s.ctlID.Fingerprint, realStackPINHash, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	// origin: S6 lead decision D-C / plan §7.1
	prepareBrokerMaintenanceAgent(t, s, accountPub)
	brokerBin, agentBin := filepath.Join(root, "broker"), filepath.Join(root, "agent")
	for _, path := range []string{brokerBin, brokerBin + ".prev", agentBin} {
		copyTransferBinary(t, s.bin, path, nil)
	}
	s.agentBin = agentBin
	// The authorized S8 routes need both daemons' deployment registrations
	// before their first start (installer retrofit), and the broker's config
	// file names its data directory for the root stage.
	// origin: S8 FIX-DESCRIBE (P19 request R7)
	configPath := filepath.Join(root, "broker.yaml")
	if err := os.WriteFile(configPath, []byte("broker:\n  storage:\n    db: "+s.dbPath+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	registerTransactionDomain(t, upgradecli.BrokerBootRole(root, configPath, "", uint32(os.Geteuid())), brokerBin, "tether-broker")
	registerTransactionDomain(t, upgradecli.AgentBootRole(s.agentHome, s.sid, s.nid, uint32(os.Geteuid())), agentBin, "tether-agent@"+s.sid)
	admin := &adminsock.Client{Path: filepath.Join(root, "admin.sock"), Timeout: 5 * time.Second}
	brokerCmd, brokerLog := startTransferDaemon(t, brokerBin, root, "serve", "--config", configPath, "--db", s.dbPath, "--nats-url", s.url, "--admin-socket", admin.Path,
		"--tunnel-addr", "127.0.0.1:0", "--tunnel-public-host", "127.0.0.1", "--log-file", "-", "--upgrade-url-allow", srv.URL+"/", "--auth-callout-seeds-dir", seeds)
	if !testharness.WaitFor(t, 10*time.Second, 20*time.Millisecond, func() bool {
		r, e := admin.Call(adminsock.Request{Op: adminsock.OpRuntime})
		return e == nil && r.OK && r.Runtime.BrokerImage != nil
	}) {
		t.Fatalf("serve did not become ready: %s", brokerLog.bytes())
	}
	s.startAgent()
	t.Cleanup(s.stopAgent)
	t.Cleanup(func() {
		if t.Failed() {
			body, _ := os.ReadFile(s.agentOut)
			t.Logf("serve log:\n%s\nagent log:\n%s", brokerLog.bytes(), body)
		}
	})
	testharness.WaitNodeOnline(t, s.db, s.sid, s.nid, 20*time.Second)
	awaitBrokerMaintenanceAgent(t, s)
	nc, _, err := natsinbox.Connect(s.url, s.ctlID.PublicKey, []nats.Option{
		nats.Name(cli.CtlNameForSession(s.sid)),
		nats.Nkey(s.ctlID.PublicKey, func(nonce []byte) ([]byte, error) { return auth.SignWithSeed(s.ctlID.Seed, nonce) }),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	for index, verb := range []string{"pull", "push"} {
		payload := make([]byte, 8<<20+777)
		if _, err := rand.Read(payload); err != nil {
			t.Fatal(err)
		}
		src, dst := filepath.Join(root, verb+"-source"), filepath.Join(root, verb+"-destination")
		if err := os.WriteFile(src, payload, 0o600); err != nil {
			t.Fatal(err)
		}
		cfg, closeData, err := cli.TransferClientConfig(nc, s.url, s.ctlID, s.sid)
		if err != nil {
			t.Fatal(err)
		}
		cfg.Identity.SID, cfg.Identity.NID, cfg.Identity.TransferID = s.sid, s.nid, verb+"image"
		cfg.Intent = x.Intent{Verb: verb, Path: src, Force: true, RateLimitBPS: 512 << 10}
		if verb == "push" {
			cfg.Intent.Path = dst
			cfg.Intent.Size = int64(len(payload))
			cfg.Source, err = os.Open(src)
		} else {
			cfg.Destination, err = cli.NewTransferReceiver(dst, true)
		}
		if err != nil {
			t.Fatal(err)
		}
		d, _ := controlbudget.After(100 * time.Second)
		legacy, err := cli.SelectTransferClient(&cfg, d)
		if err != nil || legacy {
			t.Fatalf("serve capability: legacy=%t err=%v", legacy, err)
		}
		prepared := make(chan struct{})
		var once sync.Once
		cfg.Observe = func(o x.Observation) {
			if o.Attempt.LogicalID == cfg.Identity.TransferID+"-"+string(x.OpPrepare) {
				once.Do(func() { close(prepared) })
			}
		}
		client, err := xferclient.NewClient(cfg)
		if err != nil {
			t.Fatal(err)
		}
		type result struct {
			receipt x.TerminalReceipt
			err     error
		}
		done := make(chan result, 1)
		joined := make(chan struct{})
		go func() { defer close(joined); r, e := client.Run(d); done <- result{r, e} }()
		t.Cleanup(func() {
			stop, _ := controlbudget.After(10 * time.Second)
			_ = closeData()
			if err := client.Stop(stop); err != nil {
				t.Error(err)
			}
			select {
			case <-joined:
			case <-time.After(10 * time.Second):
				t.Error("original transfer runner did not join")
			}
		})
		select {
		case <-prepared:
		case r := <-done:
			t.Fatalf("transfer ended before cut: %+v %v", r.receipt, r.err)
		case <-time.After(20 * time.Second):
			t.Fatal("prepare did not finish")
		}
		brokerPID, agentPID := brokerCmd.Process.Pid, s.agentPID()
		brokerTicks, _ := upgradeProcessIdentity(brokerPID)
		agentTicks, _ := upgradeProcessIdentity(agentPID)
		// Replace only our private staged executable. The admin operation uses
		// the same real preflight, closed-store cut and exec as tether serve.
		copyTransferBinary(t, brokerBin, brokerBin+".prev", nil)
		copyTransferBinary(t, brokerBin, brokerBin+".candidate", []byte(fmt.Sprintf("\nS7-image-%d\n", index)))
		if err := os.Rename(brokerBin+".candidate", brokerBin); err != nil {
			t.Fatal(err)
		}
		sha := upgradeFileSHA(t, brokerBin)
		// origin: S8 FIX-DESCRIBE (P19 request R7). The old admin reload is
		// refused by the shared legacy policy; the cut takes the authorized
		// broker transaction route over the same admin socket.
		if _, _, err := s.brokerTransactionCut(admin.Path, brokerBin, sha, brokerPID); err != nil {
			t.Fatalf("serve transaction cut: %s%v", transactionDependency(err), err)
		}
		if !testharness.WaitFor(t, 25*time.Second, 50*time.Millisecond, func() bool {
			r, e := admin.Call(adminsock.Request{Op: adminsock.OpRuntime})
			return e == nil && r.OK && r.Runtime.BrokerImage != nil && r.Runtime.BrokerImage.RunningSHA256 == sha && r.Runtime.BrokerImage.Generation == uint64(index+2)
		}) {
			t.Fatal("serve image did not activate")
		}
		// Pull carries a live sender; push carries its prepared receiver entry.
		// A different private artifact on the second hop forces another exec.
		// origin: S8 FIX-DESCRIBE (P19 request R7) — the agent cut takes the
		// authorized transaction route; the old `node upgrade --url` dispatch
		// is refused by the shared legacy policy.
		candidate := filepath.Join(root, fmt.Sprintf("agent-candidate-%d", index))
		writeArtifactBinary(t, artifacts[index], candidate)
		if err := s.agentTransactionCut(fmt.Sprintf("%s/%d.tar.gz", srv.URL, index), artifacts[index].tarSHA, candidate, agentPID); err != nil {
			t.Fatalf("agent transaction cut: %s%v", transactionDependency(err), err)
		}
		for pid, want := range map[int]string{brokerPID: brokerTicks, agentPID: agentTicks} {
			got, e := upgradeProcessIdentity(pid)
			if e != nil || got != want {
				t.Fatalf("daemon process identity changed: %d %s/%s %v", pid, got, want, e)
			}
		}
		select {
		case r := <-done:
			if r.err != nil || r.receipt.Kind != "complete" || r.receipt.HistorySeq == 0 || r.receipt.Bytes != int64(len(payload)) {
				t.Fatalf("%s after image cuts: %+v %v", verb, r.receipt, r.err)
			}
		case <-time.After(90 * time.Second):
			t.Fatal("original invocation did not resume")
		}
		stop, _ := controlbudget.After(5 * time.Second)
		if err := client.Stop(stop); err != nil {
			t.Fatal(err)
		}
		if err := closeData(); err != nil {
			t.Fatal(err)
		}
		if got, e := os.ReadFile(dst); e != nil || !bytes.Equal(got, payload) {
			t.Fatalf("%s bytes after cuts: %v", verb, e)
		}
	}
}

func copyTransferBinary(t *testing.T, from, to string, trailer []byte) {
	t.Helper()
	data, err := os.ReadFile(from)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(to, append(data, trailer...), 0o755); err != nil {
		t.Fatal(err)
	}
}

func startTransferDaemon(t *testing.T, bin, home string, args ...string) (*exec.Cmd, *ptyCapture) {
	t.Helper()
	cmd := exec.Command(bin, args...)
	cmd.Env = append(ctlEnv(), "TETHER_HOME="+home, "HOME="+home)
	cmd.Dir = home
	cmd.SysProcAttr = agentProcessAttributes()
	log := &ptyCapture{}
	cmd.Stdout, cmd.Stderr = log, log
	wait, err := testharness.StartProcess(cmd)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- wait() }()
	t.Cleanup(func() {
		_ = cmd.Process.Signal(syscall.SIGTERM)
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("daemon exit: %v\n%s", err, log.bytes())
			}
		case <-time.After(10 * time.Second):
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
			<-done
			t.Error("daemon stop did not join")
		}
	})
	return cmd, log
}

// registerTransactionDomain writes one component's deployment registration as
// an installer retrofit would: canonical entry, owner-only control directory
// with its artifacts slot, one participant. Production has no writer yet (R5).
// origin: S8 FIX-DESCRIBE (P19 requests R5/R7)
func registerTransactionDomain(t *testing.T, role upgradecli.BootRole, canonical, service string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(role.ControlDir, "artifacts"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(role.ControlDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := upgradelock.RegisterDomain(upgradelock.Registration{CanonicalPath: canonical, ControlDir: role.ControlDir,
		DeploymentID: "transfer-serve-images", Participants: []upgradelock.Participant{{Key: role.Want, ServiceID: service}}}); err != nil {
		t.Fatal(err)
	}
}

// writeArtifactBinary extracts the release archive's single `tether` entry:
// the exact candidate bytes the root stage publishes for the grant.
func writeArtifactBinary(t *testing.T, a upgradeArtifact, path string) {
	t.Helper()
	gz, err := gzip.NewReader(bytes.NewReader(a.tarball))
	if err != nil {
		t.Fatal(err)
	}
	tr := tar.NewReader(gz)
	if _, err := tr.Next(); err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(tr)
	if err != nil || fmt.Sprintf("%x", sha256.Sum256(body)) != a.binSHA {
		t.Fatalf("artifact binary does not match its archive: %v", err)
	}
	if err := os.WriteFile(path, body, 0o755); err != nil {
		t.Fatal(err)
	}
}

// transactionCtl runs the BUILT ctl with exactly the given arguments (the
// broker and stage verbs take neither --home nor --nats-url positionally).
func (s *realStack) transactionCtl(budget time.Duration, env []string, args ...string) ctlResult {
	s.t.Helper()
	ctx, cancel := context.WithTimeout(s.t.Context(), budget)
	defer cancel()
	cmd := exec.CommandContext(ctx, s.bin, args...)
	cmd.Env = append(ctlEnv(), env...)
	var out, errOut bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errOut
	res := ctlResult{}
	if err := testharness.RunProcess(cmd); err != nil {
		var exitErr *exec.ExitError
		if !errorsAs(err, &exitErr) {
			s.t.Fatalf("run ctl %v: %v", args, err)
		}
		res.rc = exitErr.ExitCode()
	}
	res.stdout, res.stderr = out.String(), errOut.String()
	return res
}

// transactionDependency names the by-design refusals of a component whose S8
// exporter or reader advertisement does not exist yet, so the red is the
// exact missing dependency rather than an unexplained cut failure. It never
// turns a refusal into a pass: the caller still fails.
// origin: S8 FIX-DESCRIBE (P19 request R7)
func transactionDependency(err error) string {
	switch text := err.Error(); {
	case strings.Contains(text, "broker_cut_unavailable") || strings.Contains(text, "broker") && strings.Contains(text, "domain_census_unavailable"):
		return "NOT_RUN pending the S8 broker cut exporter and identity census (P15/P17; broker describe/preflight refuse by design): "
	case strings.Contains(text, "reader_capability_missing") || strings.Contains(text, "reader_domain_missing"):
		return "NOT_RUN pending production advertisement of upgrade-tx-v1/authority-incarnation-v1/upgrade-cut-v1 (P13/P17; off until every exporter exists): "
	}
	return ""
}

// grantTxID reads the target-issued transaction ID from a --grant-out file.
func grantTxID(path string) (string, error) {
	body, err := os.ReadFile(path)
	var grant upgradetx.Grant
	if err == nil {
		err = json.Unmarshal(body, &grant)
	}
	if err != nil || len(grant.Handle.TxID) != 32 {
		return "", fmt.Errorf("no target-issued grant in %s: %v", path, err)
	}
	return grant.Handle.TxID, nil
}

// brokerTransactionCut is `broker upgrade reload --prepare-only` → root stage
// → `--tx-id --artifact-receipt` → `broker upgrade wait`, all over the admin
// socket. The candidate is the one staged at the registered canonical path.
// origin: S8 FIX-DESCRIBE (P19 request R7)
// It returns the target-issued transaction ID and the Start result (an armed
// broker cut replies before its exec: accepted, outcome left to wait).
func (s *realStack) brokerTransactionCut(socket, candidate, sha string, brokerPID int) (string, ctlResult, error) {
	// Even a short version probe must join the shared supervisor's descendants.
	// origin: S10 internal review MAKETEST-GATES F3 / S6 FIX-ORPHAN
	cmd := exec.CommandContext(s.t.Context(), candidate, "version")
	var version bytes.Buffer
	cmd.Stdout = &version
	err := testharness.RunProcess(cmd)
	fields := strings.Fields(version.String())
	if err != nil || len(fields) < 2 {
		return "", ctlResult{}, fmt.Errorf("candidate version probe: %v", err)
	}
	dir := s.t.TempDir()
	grantPath, receiptPath := filepath.Join(dir, "broker-grant.json"), filepath.Join(dir, "broker-receipt.json")
	if r := s.transactionCtl(60*time.Second, nil, "broker", "upgrade", "reload", "--socket", socket, "--to-version", fields[1], "--expect-sha256", sha,
		"--prepare-only", "--grant-out", grantPath, "--home", s.ctlHome); r.rc != 0 {
		return "", r, fmt.Errorf("broker prepare rc=%d: %s%s", r.rc, r.stdout, r.stderr)
	}
	tx, err := grantTxID(grantPath)
	if err != nil {
		return "", ctlResult{}, err
	}
	if r := s.transactionCtl(60*time.Second, nil, "upgrade", "stage", "--grant", grantPath, "--source", fmt.Sprintf("/proc/%d/exe", brokerPID),
		"--candidate", candidate, "--receipt-out", receiptPath); r.rc != 0 {
		return tx, r, fmt.Errorf("broker stage rc=%d: %s%s", r.rc, r.stdout, r.stderr)
	}
	start := s.transactionCtl(90*time.Second, nil, "broker", "upgrade", "reload", "--socket", socket, "--tx-id", tx, "--artifact-receipt", receiptPath, "--home", s.ctlHome)
	if start.rc != 0 && start.rc != 74 {
		return tx, start, fmt.Errorf("broker start rc=%d: %s%s", start.rc, start.stdout, start.stderr)
	}
	if r := s.transactionCtl(150*time.Second, nil, "broker", "upgrade", "wait", "--socket", socket, "--tx-id", tx, "--timeout", "120s", "--home", s.ctlHome); r.rc != 0 {
		return tx, start, fmt.Errorf("broker wait rc=%d: %s%s", r.rc, r.stdout, r.stderr)
	}
	return tx, start, nil
}

// agentTransactionCut is `node upgrade --prepare-only` → root stage (in the
// agent's own home) → `node upgrade --tx-id --artifact-receipt` → wait.
// origin: S8 FIX-DESCRIBE (P19 request R7)
func (s *realStack) agentTransactionCut(url, sha, candidate string, agentPID int) error {
	dir := s.t.TempDir()
	grantPath, receiptPath := filepath.Join(dir, "agent-grant.json"), filepath.Join(dir, "agent-receipt.json")
	ctx, cancel := context.WithTimeout(s.t.Context(), 150*time.Second)
	defer cancel()
	if r := s.ctlCommand(ctx, "node", "upgrade", s.nid, "--url", url, "--sha256", sha, "--prepare-only", "--grant-out", grantPath); r.rc != 0 {
		return fmt.Errorf("agent prepare rc=%d: %s%s", r.rc, r.stdout, r.stderr)
	}
	tx, err := grantTxID(grantPath)
	if err != nil {
		return err
	}
	if r := s.transactionCtl(60*time.Second, []string{"TETHER_HOME=" + s.agentHome}, "upgrade", "stage", "--grant", grantPath,
		"--source", fmt.Sprintf("/proc/%d/exe", agentPID), "--candidate", candidate, "--receipt-out", receiptPath); r.rc != 0 {
		return fmt.Errorf("agent stage rc=%d: %s%s", r.rc, r.stdout, r.stderr)
	}
	if r := s.ctlCommand(ctx, "node", "upgrade", s.nid, "--tx-id", tx, "--artifact-receipt", receiptPath); r.rc != 0 && r.rc != 74 {
		return fmt.Errorf("agent start rc=%d: %s%s", r.rc, r.stdout, r.stderr)
	}
	if r := s.ctlCommand(ctx, "node", "upgrade-wait", s.nid, "--tx-id", tx, "--timeout", "120s"); r.rc != 0 {
		return fmt.Errorf("agent wait rc=%d: %s%s", r.rc, r.stdout, r.stderr)
	}
	return nil
}
