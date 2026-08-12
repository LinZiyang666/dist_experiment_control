package main

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/broker"
	"github.com/LinZiyang666/tether/internal/clusteroffline"
	"github.com/LinZiyang666/tether/internal/natsconf"
	"github.com/LinZiyang666/tether/internal/serveconf"
	"github.com/LinZiyang666/tether/internal/storage"
)

// r10_restore_test.go — R10 P2 / P4 / #53, asserted END-TO-END through the real cobra command.
//
// Why end-to-end and not a unit test on applyClusterSeam: this repo has been bitten THREE times by
// "half-wiring" — a capability that compiles, has tests, passes `go vet`, passes `make lint`, and is
// never actually reached (R5 `_b_migrate_forensics`, R7b's lease keeper with no client, R8's
// `forgetHomeAck`). applyClusterSeam already existed and already had unit tests; the defect was
// precisely that `cluster recovery restore` had no --config flag and therefore never called it. A
// test that calls applyClusterSeam directly would have been green throughout the entire defect.
//
// So every assertion below drives `newClusterRestoreCmd().Execute()` and then reads the RESULT off
// disk with the same predicate the daemon uses (broker.DetectClusterMode).

// --- fixture -----------------------------------------------------------------------------------

type drBox struct {
	root       string
	bundleDir  string
	secretsDir string
	dataDir    string
	dbPath     string
	configPath string
	natsConf   string
	selfID     string
	raftAddr   string
}

func writeTestTunnelCert(t *testing.T, certPath, keyPath string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "tether-r10-test"},
		NotBefore:    time.Unix(0, 0),
		NotAfter:     time.Unix(1<<31, 0),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatal(err)
	}
}

// newDRBox builds a FRESH disaster-recovery host: a restorable cluster bundle, this node's secrets,
// an empty data dir (no DB — the box is new), and the broker.yaml `install.sh` actually ships, whose
// entire `cluster:` block is COMMENTED OUT. That commented block is the P2 root cause: without a
// --config on restore, nothing ever uncomments it, and the daemon FATALs at boot.
func newDRBox(t *testing.T) *drBox {
	t.Helper()
	root := t.TempDir()
	b := &drBox{
		root:       root,
		secretsDir: filepath.Join(root, "secrets"),
		dataDir:    filepath.Join(root, "lib"),
		configPath: filepath.Join(root, "etc", "broker.yaml"),
		natsConf:   filepath.Join(root, "etc", "nats.d", "nats.conf"),
		selfID:     "brk-a",
		raftAddr:   "10.0.0.1:7400",
	}
	b.dbPath = filepath.Join(b.dataDir, "tether.db")
	for _, d := range []string{b.secretsDir, b.dataDir, filepath.Dir(b.configPath), filepath.Dir(b.natsConf)} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	writeTestTunnelCert(t, filepath.Join(b.secretsDir, "tunnel-cert.pem"), filepath.Join(b.secretsDir, "tunnel-key.pem"))
	for _, f := range []string{"cluster-ca.pem", "route-cert.pem"} {
		if err := os.WriteFile(filepath.Join(b.secretsDir, f), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	for _, f := range []string{"route-key.pem", "node-ident.nk", "broker.nk", "account.nk"} {
		if err := os.WriteFile(filepath.Join(b.secretsDir, f), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	fp, err := clusteroffline.TunnelCertFingerprint(b.secretsDir)
	if err != nil {
		t.Fatal(err)
	}

	// The SOURCE cluster (3 voters) that later died, backed up into a bundle.
	srcDir := filepath.Join(root, "src")
	if err := os.MkdirAll(srcDir, 0o700); err != nil {
		t.Fatal(err)
	}
	srcDB := filepath.Join(srcDir, "tether.db")
	db, err := storage.OpenWAL("file:" + srcDB)
	if err != nil {
		t.Fatal(err)
	}
	ins := func(id, name, addr, certfp string) {
		if _, err := db.Exec(
			`INSERT INTO cluster_nodes
			 (node_id,name,node_ident_pub,nats_server_id,raft_addr,nats_route,tunnel_addr,public_host,cert_fp,phase,added_at,join_nonce,join_sig)
			 VALUES(?,?,?,?,?,?,?,?,?,'VOTER','2026-07-18 00:00:00 +0000 UTC',?,?)`,
			id, name, "Ident-"+id, id, addr, "nats://"+strings.Split(addr, ":")[0]+":6222",
			strings.Split(addr, ":")[0]+":7000", "h."+id, certfp, "NONCE-"+id, "SIG-"+id); err != nil {
			t.Fatal(err)
		}
	}
	ins(b.selfID, "broker-a", b.raftAddr, fp)
	ins("brk-b", "broker-b", "10.0.0.2:7400", "sha256:cert-b")
	ins("brk-c", "broker-c", "10.0.0.3:7400", "sha256:cert-c")
	for _, kv := range [][2]string{{"self_node_id", b.selfID}, {"applied_index", "9001"}} {
		if _, err := db.Exec(`INSERT INTO cluster_meta(key,value) VALUES(?,?)`, kv[0], kv[1]); err != nil {
			t.Fatal(err)
		}
	}
	_ = db.Close()

	b.bundleDir = filepath.Join(root, "bundle")
	if _, err := clusteroffline.OfflineBackup(clusteroffline.BackupOptions{
		DataDir: srcDir, DBPath: srcDB, SecretsDir: b.secretsDir, OutDir: b.bundleDir,
	}); err != nil {
		t.Fatalf("OfflineBackup: %v", err)
	}

	// The broker.yaml install.sh ships: `cluster:` fully commented out.
	// #75: the fixture must be schema-legal (strict decoder) — the old copy
	// used `nats_url` (never a real key; the template writes nats.url) and a
	// top-level `storage:`, shapes only the tolerant decoder let pass.
	if err := os.WriteFile(b.configPath, []byte(
		"broker:\n"+
			"  nats:\n"+
			"    url: nats://127.0.0.1:4222\n"+
			"  # cluster:\n"+
			"  #   data_dir: /var/lib/tether\n"+
			"  #   raft_addr: 10.0.0.1:7400\n"+
			"  #   secrets_dir: /etc/tether/secrets\n"+
			"  storage:\n"+
			"    db: /var/lib/tether/tether.db\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	return b
}

// runRestore drives the real cobra command. extra lets a case override/add flags.
func (b *drBox) runRestore(t *testing.T, extra ...string) (stdout, stderr string, err error) {
	t.Helper()
	cmd := newClusterRestoreCmd()
	var out, errb bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errb)
	cmd.SetIn(strings.NewReader(b.selfID + "\n")) // the Tier-2 typed confirm
	args := append([]string{
		b.bundleDir,
		"--confirm-node-id", b.selfID,
		"--data-dir", b.dataDir,
		"--db", b.dbPath,
		"--secrets-dir", b.secretsDir,
		"--config", b.configPath,
		"--nats-conf", b.natsConf,
	}, extra...)
	cmd.SetArgs(args)
	err = cmd.Execute()
	return out.String(), errb.String(), err
}

// --- P2 ----------------------------------------------------------------------------------------

// TestRestoreAppliesTheClusterSeamAndTheHostBootsClustered is exit assertion 1.
//
// The chain proven here is the WHOLE P2 defect, link by link:
//
//	restore --config  →  broker.yaml gains a broker.cluster seam  →  serveconf decodes all 5 fields
//	                  →  broker.DetectClusterMode (the REAL serve-time predicate) says CLUSTER
//	                  →  the `!clusterMode && seeded` boot FATAL is unreachable.
//
// The last link's other half — that a seeded DB with clusterMode=false really does FATAL — is pinned
// in internal/broker (TestSeededDBWithoutTheSeamIsTheBootFatal), because assertClusterDBConsistent
// is unexported. Together they close the loop without either test asserting its own oracle.
func TestRestoreAppliesTheClusterSeamAndTheHostBootsClustered(t *testing.T) {
	b := newDRBox(t)

	stdout, _, err := b.runRestore(t)
	if err != nil {
		t.Fatalf("restore failed: %v\nstdout:\n%s", err, stdout)
	}
	if !strings.Contains(stdout, "restore complete") {
		t.Fatalf("no completion line:\n%s", stdout)
	}

	cfg, lerr := serveconf.Load(b.configPath)
	if lerr != nil {
		t.Fatalf("broker.yaml no longer decodes after the seam write: %v", lerr)
	}
	c := cfg.Broker.Cluster
	// FIVE fields, named individually — the runbook used to list three, and a partial seam boots
	// SINGLE mode (serve keys cluster mode on data_dir), which is the failure this whole item is about.
	for _, f := range []struct{ name, got, want string }{
		{"data_dir", c.DataDir, b.dataDir},
		{"raft_addr", c.RaftAddr, b.raftAddr},
		{"secrets_dir", c.SecretsDir, b.secretsDir},
		// N-4c: the seam now records the ACTUAL --nats-conf the restore was given (b.natsConf), not the
		// hardcoded default — that is the whole fix (a custom-conf deploy no longer drifts to the default).
		{"nats_conf_path", c.NatsConfPath, b.natsConf},
	} {
		if f.got != f.want {
			t.Errorf("seam field %s = %q, want %q", f.name, f.got, f.want)
		}
	}
	if c.NatsServerBin == "" {
		t.Error("seam field nats_server_bin is empty (the 5th field)")
	}

	// The raft_addr must be the address the voter was ACTUALLY bootstrapped at, not a guess.
	if c.RaftAddr != b.raftAddr {
		t.Errorf("seam raft_addr %q must equal the bootstrapped raft addr %q", c.RaftAddr, b.raftAddr)
	}

	// The real serve-time predicate, on the real restored data dir.
	clustered, derr := broker.DetectClusterMode(c.DataDir)
	if derr != nil {
		t.Fatalf("DetectClusterMode: %v", derr)
	}
	if !clustered {
		t.Fatal("after restore --config the host must boot in CLUSTER mode; it would boot SINGLE and FATAL")
	}
}

// TestRestoreWithoutTheSeamStillProducesTheFatalPrecondition is the NON-VACUITY arm: with the seam
// suppressed (--config ""), the very same box lands in exactly the pre-R10 state. Without this, the
// test above could pass on a host that was somehow already clustered.
func TestRestoreWithoutTheSeamStillProducesTheFatalPrecondition(t *testing.T) {
	b := newDRBox(t)

	if _, _, err := b.runRestore(t, "--config", ""); err != nil {
		t.Fatalf("restore --config \"\" should still succeed (explicit opt-out): %v", err)
	}
	cfg, err := serveconf.Load(b.configPath)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Broker.Cluster.DataDir != "" {
		t.Fatal("--config \"\" must NOT write a seam (it is the documented hand-edit opt-out)")
	}
	clustered, err := broker.DetectClusterMode(cfg.Broker.Cluster.DataDir)
	if err != nil {
		t.Fatal(err)
	}
	if clustered {
		t.Fatal("with no seam the host must NOT be cluster-mode — the P2 assertion would be vacuous")
	}
	// And the DB really is cluster-seeded, so `!clusterMode && seeded` — the boot FATAL — holds.
	ro, err := storage.OpenReadOnly("file:" + b.dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ro.Close() }()
	var n int
	if err := ro.QueryRow(`SELECT count(*) FROM cluster_meta WHERE key='applied_index'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatal("the restored DB must be cluster-seeded (that is what makes the missing seam FATAL)")
	}
}

// TestRestoreFailsClosedWhenTheSeamCannotBeApplied: a restored host whose broker.yaml did not get a
// seam CANNOT start, so exiting 0 would report success for a box that is down. The restore itself is
// irreversible and must still be reported — the completion line stays, the exit code does not.
func TestRestoreFailsClosedWhenTheSeamCannotBeApplied(t *testing.T) {
	b := newDRBox(t)
	missing := filepath.Join(b.root, "etc", "nope.yaml")

	stdout, _, err := b.runRestore(t, "--config", missing)
	if err == nil {
		t.Fatal("a missing --config must FAIL the command (a silent skip is the exact half-done state R10 removes)")
	}
	if classifyExit(err) == exitOK {
		t.Fatalf("exit class must be nonzero, got %d", classifyExit(err))
	}
	if !strings.Contains(stdout, "restore complete") {
		t.Error("the irreversible restore must still be REPORTED even though the command exits nonzero")
	}
	if !strings.Contains(err.Error(), "REFUSE to start") {
		t.Errorf("the error must say what breaks, got: %v", err)
	}
}

// --- P4 (#64) ----------------------------------------------------------------------------------

// TestRestoreNextStepsAreExecutableInBothSituations is exit assertion 2.
//
// Pre-R10 the completion text was "NEXT: start tether-broker, then `cluster join approve`", and
// following it verbatim crash-looped the broker: restore prunes the roster to ONE voter but never
// touches nats.conf, and a lone node cannot reach the clustered JetStream meta quorum. The product
// printed the missing step — at the crash, from broker.Run. It knew what to say and said it late.
func TestRestoreNextStepsAreExecutableInBothSituations(t *testing.T) {
	clusteredConf := `
listen: 0.0.0.0:4222
server_name: brk-a
jetstream { store_dir: "/var/lib/nats/js" }
cluster {
  name: tether
  listen: 0.0.0.0:6222
  routes: [ nats://10.0.0.2:6222, nats://10.0.0.3:6222 ]
}
`
	standaloneConf := `
listen: 0.0.0.0:4222
server_name: brk-a
jetstream { store_dir: "/var/lib/nats/js" }
`
	cases := []struct {
		name    string
		conf    string // "" = do not create the file (a fresh DR box)
		wantWhy string // the situation-specific DIAGNOSIS
	}{
		{"A-clustered-conf", clusteredConf, "is CLUSTERED"},
		{"B-fresh-box-no-conf", "", "is MISSING"}, // N-4b: fresh box is now diagnosed as MISSING (honest 2-step)
		{"B-stock-standalone-conf", standaloneConf, "is standalone"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := newDRBox(t)
			if tc.conf != "" {
				if err := os.WriteFile(b.natsConf, []byte(tc.conf), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			stdout, _, err := b.runRestore(t)
			if err != nil {
				t.Fatalf("restore: %v\n%s", err, stdout)
			}

			// (1) the situation is DIAGNOSED, not guessed at.
			if !strings.Contains(stdout, tc.wantWhy) {
				t.Errorf("completion text must diagnose this situation (%q), got:\n%s", tc.wantWhy, stdout)
			}
			// (2) a step the operator can run AS PRINTED — fully substituted, no placeholders left
			// in the parts the product knows (conf path, secrets dir, server name, route url).
			if !strings.Contains(stdout, "tether cluster reconcile nats --manual") {
				t.Fatalf("no runnable nats.conf step:\n%s", stdout)
			}
			for _, tok := range []string{
				"--conf " + b.natsConf,
				"--secrets-dir " + b.secretsDir,
				"--server-name " + b.selfID,
				"--route-url nats://10.0.0.1:6222",
			} {
				if !strings.Contains(stdout, tok) {
					t.Errorf("the printed command is not copy-paste-ready: missing %q\n%s", tok, stdout)
				}
			}
			// (3) the ordering that actually works: conf BEFORE the daemon start.
			iConf := strings.Index(stdout, "reconcile nats --manual")
			iStart := strings.Index(stdout, "systemctl start tether-broker")
			if iConf < 0 || iStart < 0 || iConf > iStart {
				t.Errorf("nats.conf must be fixed BEFORE tether-broker is started (that ordering IS the fix):\n%s", stdout)
			}
			// (4) the pre-R10 text — start the daemon first — must be gone.
			if strings.Contains(stdout, "NEXT: start tether-broker") {
				t.Errorf("the pre-R10 one-liner is back; following it verbatim crash-loops the broker:\n%s", stdout)
			}

			// (5) situation A additionally carries the shared de-cluster SSOT (the same sentence the
			// boot FATAL and the status banner emit) plus the reason the ONLINE verb cannot be used here.
			if tc.name == "A-clustered-conf" {
				if !strings.Contains(stdout, natsconf.DeClusterRemedyCmd) {
					t.Errorf("clustered conf must surface the shared de-cluster remedy:\n%s", stdout)
				}
				if !strings.Contains(stdout, natsconf.DeClusterRemedyOfflineNote) {
					t.Errorf("must explain why the ONLINE verb is unreachable with the daemon stopped:\n%s", stdout)
				}
			}
		})
	}
}

// TestRestoreNextStepsFreshBoxIsAnHonestTwoStep (external review N-4b) proves the fresh-box remediation
// no longer prints a command that cannot run: a genuinely-missing conf is diagnosed as MISSING with an
// explicit "create the base conf FIRST" step and the install.sh-clobber warning. The premise is real —
// the render command it points at reads the conf via Preflight, which fails on a missing file.
func TestRestoreNextStepsFreshBoxIsAnHonestTwoStep(t *testing.T) {
	b := newDRBox(t)
	// b.natsConf is never created → a fresh DR box.
	stdout, _, err := b.runRestore(t)
	if err != nil {
		t.Fatalf("restore: %v\n%s", err, stdout)
	}
	for _, want := range []string{
		"is MISSING",
		"CANNOT run yet",
		"FIRST create the base conf",
		"Do NOT re-run install.sh now",
	} {
		if !strings.Contains(stdout, want) {
			t.Errorf("fresh-box next-steps must be an honest two-step; missing %q\n%s", want, stdout)
		}
	}
	// Premise: the render command reads the conf via natsconf.Preflight, which fails on a missing file —
	// so "create the base conf FIRST" is necessary, not gratuitous (this is exactly the N-4b bug).
	if _, e := natsconf.Preflight(b.natsConf); e == nil {
		t.Fatal("premise broken: a missing conf must fail Preflight (else the fresh-box two-step would be unnecessary)")
	}
}

// TestRestoreNextStepsDistinguishBrokenConfFromMissing (external review N-4b) proves a conf that EXISTS
// but is Preflight-refused (an `include` directive) is diagnosed as exists-but-unreadable, not "MISSING".
func TestRestoreNextStepsDistinguishBrokenConfFromMissing(t *testing.T) {
	b := newDRBox(t)
	if err := os.WriteFile(b.natsConf, []byte("include \"other.conf\"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	stdout, _, err := b.runRestore(t)
	if err != nil {
		t.Fatalf("restore: %v\n%s", err, stdout)
	}
	if !strings.Contains(stdout, "exists but cannot be taken over as-is") {
		t.Errorf("a Preflight-refused conf must be diagnosed as exists-but-unreadable:\n%s", stdout)
	}
	if strings.Contains(stdout, "is MISSING") {
		t.Errorf("an existing (broken) conf must not be reported as MISSING:\n%s", stdout)
	}
}

// --- #53 ---------------------------------------------------------------------------------------

// TestBundleScopeIsStatedAtBothEnds is exit assertion 4 for the chosen option (b): the bundle stays
// state.db-only, and BOTH the backup and the restore say so. "Silently losing history/audit" was the
// unacceptable part, not the scope.
func TestBundleScopeIsStatedAtBothEnds(t *testing.T) {
	b := newDRBox(t)

	// restore end.
	_, stderr, err := b.runRestore(t)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"HISTORY/AUDIT NOT RESTORED", "nats stream restore"} {
		if !strings.Contains(stderr, want) {
			t.Errorf("restore must warn about the missing JetStream (%q):\n%s", want, stderr)
		}
	}

	// backup end (offline path — same command surface the runbook prints).
	out := filepath.Join(b.root, "bundle2")
	cmd := newClusterBackupCmd(new(string))
	var sout, serr bytes.Buffer
	cmd.SetOut(&sout)
	cmd.SetErr(&serr)
	cmd.SetArgs([]string{"--offline", "--out", out, "--db", b.dbPath, "--data-dir", b.dataDir, "--secrets-dir", b.secretsDir})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("offline backup: %v\n%s", err, serr.String())
	}
	for _, want := range []string{"BUNDLE SCOPE", "JetStream is NOT in it", "nats stream backup"} {
		if !strings.Contains(serr.String(), want) {
			t.Errorf("backup must state the bundle scope (%q):\n%s", want, serr.String())
		}
	}
}
