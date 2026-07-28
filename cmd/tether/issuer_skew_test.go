package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/clusteroffline"
	"github.com/nats-io/nkeys"
)

// stockConfNoAuth is a stock pre-cutover nats.conf shape (what install.sh writes): readable + valid,
// but with NO authorization{} block, so AuthIdentity() yields "" and the issuer cross-check cannot run.
func stockConfNoAuth() string {
	return "host: \"127.0.0.1\"\nport: 4222\njetstream { store_dir: \"/var/lib/tether/js\" }\n"
}

// multiUserConf is a grown MULTI-user conf: two nkey users → AuthIdentity yields confBroker=="" (the
// documented-normal grown-cluster shape) while auth_callout.issuer is still read. Used to pin that no
// BrokerUnverified false alarm is produced on every grown cluster (external review N-3 F5).
func multiUserConf(issuer, brokerNkey string) string {
	otherKP, _ := nkeys.CreateUser()
	other, _ := otherKP.PublicKey()
	return `host: "127.0.0.1"
port: 4222
jetstream { store_dir: "/var/lib/tether/js" }
authorization {
  users: [{ nkey: "` + brokerNkey + `" }, { nkey: "` + other + `" }]
  auth_callout {
    issuer: "` + issuer + `"
    auth_users: [ "` + brokerNkey + `", "` + other + `" ]
  }
}
`
}

// TestReadClusterPublicIdentitiesUnverifiedIssuer (external review N-3) pins the fail-open fix: when
// account.nk is readable but the conf issuer cannot be cross-checked (missing conf / stock pre-cutover
// conf), the value is STILL substituted (init/restore need it precisely then) but flagged
// IssuerUnverified with a note — never a silent false all-clear. A matching conf clears everything.
// Mutation: reverting the default-case split (silent substitute) drops IssuerUnverified and this reds.
func TestReadClusterPublicIdentitiesUnverifiedIssuer(t *testing.T) {
	// (a) missing conf → substituted + unverified.
	dir, conf, acctPub, _ := skewFixture(t, "PLACEHOLDER", "PLACEHOLDER")
	_ = os.Remove(conf)
	ids := readClusterPublicIdentities(dir, conf)
	if ids.AccountIssuer != acctPub {
		t.Fatalf("missing conf: account issuer must still be substituted, got %q", ids.AccountIssuer)
	}
	if !ids.IssuerUnverified || ids.IssuerSkew {
		t.Fatalf("missing conf: want IssuerUnverified=true IssuerSkew=false, got unv=%v skew=%v", ids.IssuerUnverified, ids.IssuerSkew)
	}
	if !strings.Contains(ids.Note, "UNVERIFIED") {
		t.Fatalf("missing conf: note must flag UNVERIFIED, got %q", ids.Note)
	}

	// (b) stock conf (no authorization block) → substituted + unverified (the mainline pre-cutover state).
	dir2, conf2, acctPub2, _ := skewFixture(t, "PLACEHOLDER", "PLACEHOLDER")
	_ = os.WriteFile(conf2, []byte(stockConfNoAuth()), 0o600)
	ids2 := readClusterPublicIdentities(dir2, conf2)
	if ids2.AccountIssuer != acctPub2 || !ids2.IssuerUnverified || ids2.IssuerSkew {
		t.Fatalf("stock conf: want substituted+unverified+no-skew, got issuer=%q unv=%v skew=%v", ids2.AccountIssuer, ids2.IssuerUnverified, ids2.IssuerSkew)
	}

	// (c) matching conf → all clear.
	dir3, conf3, acctPub3, brkPub3 := skewFixture(t, "PLACEHOLDER", "PLACEHOLDER")
	_ = os.WriteFile(conf3, []byte(confWith(acctPub3, brkPub3)), 0o600)
	ids3 := readClusterPublicIdentities(dir3, conf3)
	if ids3.IssuerUnverified || ids3.IssuerSkew || ids3.Note != "" {
		t.Fatalf("matching conf: want clean, got unv=%v skew=%v note=%q", ids3.IssuerUnverified, ids3.IssuerSkew, ids3.Note)
	}

	// (d) F5: a MULTI-user conf with a matching issuer yields confBroker=="" (documented-normal) and must
	// produce NO advisory row (no BrokerUnverified false alarm on every grown cluster).
	dir4, conf4, acctPub4, brkPub4 := skewFixture(t, "PLACEHOLDER", "PLACEHOLDER")
	_ = os.WriteFile(conf4, []byte(multiUserConf(acctPub4, brkPub4)), 0o600)
	if got := clusterAuthIssuerSkewChecks(dir4, conf4); len(got) != 0 {
		t.Fatalf("F5: a matching-issuer multi-user conf must yield zero advisory rows, got %+v", got)
	}
}

// TestClusterDoctorAdvisoryWhenIssuerCheckSkipped (external review N-3, F1) pins that a skipped issuer
// cross-check surfaces as an ADVISORY (not FATAL) — proportionality: a stock pre-init box must not fail
// doctor on its mainline. Pinned via clusterAuthIssuerSkewChecks directly (a full Doctor() would FATAL
// on skewFixture's incomplete §15 secret set, so it could never reach the ADVISORY assertion).
func TestClusterDoctorAdvisoryWhenIssuerCheckSkipped(t *testing.T) {
	dir, conf, _, _ := skewFixture(t, "PLACEHOLDER", "PLACEHOLDER")
	_ = os.WriteFile(conf, []byte(stockConfNoAuth()), 0o600)
	got := clusterAuthIssuerSkewChecks(dir, conf)
	if len(got) != 1 || got[0].Name != "auth-issuer-unverified" || got[0].Status != clusteroffline.DoctorAdvisory {
		t.Fatalf("skipped verification must yield ONE ADVISORY auth-issuer-unverified check (not FATAL), got %+v", got)
	}
	// Control: a matching conf → no unverified row.
	dir2, conf2, acctPub2, brkPub2 := skewFixture(t, "PLACEHOLDER", "PLACEHOLDER")
	_ = os.WriteFile(conf2, []byte(confWith(acctPub2, brkPub2)), 0o600)
	if got := clusterAuthIssuerSkewChecks(dir2, conf2); len(got) != 0 {
		t.Fatalf("a matching conf must yield no unverified row, got %+v", got)
	}
}

// TestReconcileNatsWarnsNotRefusesOnSkippedVerification (external review N-3, F2/F3) proves reconcile
// nats WARNS (never refuses) when the issuer could not be cross-checked: on a missing/stock conf it
// exits 0 with a stderr "verification SKIPPED", not a hard error (a refuse would break the fresh-box /
// init mainline). A matching conf produces no warning.
func TestReconcileNatsWarnsNotRefusesOnSkippedVerification(t *testing.T) {
	sock := "/nonexistent/admin.sock"
	run := func(dir, conf string) (string, error) {
		cmd := newClusterReconcileNatsCmd(&sock)
		var buf bytes.Buffer
		cmd.SetOut(&buf)
		cmd.SetErr(&buf)
		cmd.SetArgs([]string{"--secrets-dir", dir, "--conf", conf}) // no --wait: runs before the (unreachable) socket poll
		err := cmd.Execute()
		return buf.String(), err
	}
	// Missing conf (fresh box) → WARN, not refuse.
	dir, conf, _, _ := skewFixture(t, "PLACEHOLDER", "PLACEHOLDER")
	_ = os.Remove(conf)
	out, err := run(dir, conf)
	if err != nil {
		t.Fatalf("reconcile nats on an UNVERIFIABLE conf must WARN, not refuse; got err=%v out=%s", err, out)
	}
	if !strings.Contains(out, "verification SKIPPED") {
		t.Fatalf("reconcile nats must warn 'verification SKIPPED' on an unverifiable conf; out=%s", out)
	}
	// Control: matching conf → no warning.
	dir2, conf2, acctPub2, brkPub2 := skewFixture(t, "PLACEHOLDER", "PLACEHOLDER")
	_ = os.WriteFile(conf2, []byte(confWith(acctPub2, brkPub2)), 0o600)
	out2, err2 := run(dir2, conf2)
	if err2 != nil {
		t.Fatalf("reconcile nats with a matching conf must not error; got %v", err2)
	}
	if strings.Contains(out2, "verification SKIPPED") {
		t.Fatalf("a matching conf must not warn about skipped verification; out=%s", out2)
	}
}

// TestReconcileConvergedPrintIsQualifiedWhenUnverified (external review N-3, F2) pins the --wait
// converged path: when the issuer could not be cross-checked, the converged line keeps its leading
// "all voters converged" substring (drills grep it) but is QUALIFIED with SKIPPED + a stderr warning —
// the exact false all-clear N-3 names. A matching conf prints the unqualified line.
func TestReconcileConvergedPrintIsQualifiedWhenUnverified(t *testing.T) {
	converged := &adminsock.ClusterStatusReport{
		TopoDesired: 3,
		Nodes:       []adminsock.ClusterNodeStatus{{NodeID: "n1", Phase: "VOTER", Reachable: true, TopoReported: true, TopoObserved: 3, TopoApplied: 3}},
	}
	stubFetch(t, func(string) (*adminsock.ClusterStatusReport, error) { return converged, nil })
	sock := "/nonexistent/admin.sock"
	run := func(dir, conf string) (string, string, error) {
		cmd := newClusterReconcileNatsCmd(&sock)
		var out, errBuf bytes.Buffer
		cmd.SetOut(&out)
		cmd.SetErr(&errBuf)
		cmd.SetArgs([]string{"--secrets-dir", dir, "--conf", conf, "--wait"})
		err := cmd.Execute()
		return out.String(), errBuf.String(), err
	}
	// Unverifiable (stock) conf + converged → exit 0, qualified converged line + stderr warning.
	dir, conf, _, _ := skewFixture(t, "PLACEHOLDER", "PLACEHOLDER")
	_ = os.WriteFile(conf, []byte(stockConfNoAuth()), 0o600)
	out, errOut, err := run(dir, conf)
	if err != nil {
		t.Fatalf("converged + unverified must exit 0 (warn, not refuse); err=%v out=%s errOut=%s", err, out, errOut)
	}
	if !strings.Contains(out, "all voters converged to topology generation") {
		t.Fatalf("must keep the 'all voters converged' substring (drills grep it); out=%s", out)
	}
	if !strings.Contains(out, "SKIPPED") || !strings.Contains(errOut, "verification SKIPPED") {
		t.Fatalf("converged print must be QUALIFIED + stderr warned when unverified; out=%s errOut=%s", out, errOut)
	}
	// Control: matching conf → unqualified converged line, no warning.
	dir2, conf2, acctPub2, brkPub2 := skewFixture(t, "PLACEHOLDER", "PLACEHOLDER")
	_ = os.WriteFile(conf2, []byte(confWith(acctPub2, brkPub2)), 0o600)
	out2, errOut2, err2 := run(dir2, conf2)
	if err2 != nil {
		t.Fatalf("converged + verified must exit 0; got %v", err2)
	}
	if strings.Contains(out2, "SKIPPED") || strings.Contains(errOut2, "SKIPPED") {
		t.Fatalf("a verified converged print must be unqualified; out=%s errOut=%s", out2, errOut2)
	}
}

// r11_issuer_skew_test.go (R11 P6/#54) — a rotated account.nk/broker.nk that has NOT been re-rendered
// into the live nats.conf is an auth_callout identity skew. Facet 2: `cluster doctor` must report it
// FATAL (it used to be blind). Facet 1: `reconcile nats` must fail-closed (it used to print a false
// all-clear). Both consume readClusterPublicIdentities' structured skew flags.

// skewFixture writes account.nk + broker.nk to a temp dir and a nats.conf whose auth_callout issuer /
// broker-nkey are the CALLER'S choice, so tests can construct a match or a skew at will. Returns the
// dir + conf path + the on-disk seeds' derived public keys.
func skewFixture(t *testing.T, confIssuer, confBroker string) (dir, confPath, acctPub, brkPub string) {
	t.Helper()
	dir = t.TempDir()
	acctKP, _ := nkeys.CreateAccount()
	acctSeed, _ := acctKP.Seed()
	brkKP, _ := nkeys.CreateUser()
	brkSeed, _ := brkKP.Seed()
	if err := os.WriteFile(filepath.Join(dir, "account.nk"), acctSeed, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "broker.nk"), brkSeed, 0o600); err != nil {
		t.Fatal(err)
	}
	acctPub, _ = acctKP.PublicKey()
	brkPub, _ = brkKP.PublicKey()
	confPath = filepath.Join(dir, "nats.conf")
	if err := os.WriteFile(confPath, []byte(confWith(confIssuer, confBroker)), 0o600); err != nil {
		t.Fatal(err)
	}
	return dir, confPath, acctPub, brkPub
}

// TestClusterAuthIssuerSkewChecks pins the doctor-facing skew detector directly (facet 2 unit).
func TestClusterAuthIssuerSkewChecks(t *testing.T) {
	// No skew: conf renders the SAME issuer + broker the on-disk seeds derive.
	dir, conf, acctPub, brkPub := skewFixture(t, "PLACEHOLDER_A", "PLACEHOLDER_B")
	// Re-render the conf to match the real on-disk pubs (skewFixture wrote placeholders).
	_ = os.WriteFile(conf, []byte(confWith(acctPub, brkPub)), 0o600)
	if got := clusterAuthIssuerSkewChecks(dir, conf); len(got) != 0 {
		t.Fatalf("no skew must yield no checks, got %+v", got)
	}

	// Issuer skew: conf issuer is a DIFFERENT account key than the on-disk account.nk.
	otherAcct, _ := nkeys.CreateAccount()
	otherAcctPub, _ := otherAcct.PublicKey()
	dir2, conf2, _, brkPub2 := skewFixture(t, otherAcctPub, "PLACEHOLDER")
	_ = os.WriteFile(conf2, []byte(confWith(otherAcctPub, brkPub2)), 0o600)
	got := clusterAuthIssuerSkewChecks(dir2, conf2)
	if len(got) != 1 || got[0].Name != "auth-issuer-skew" || got[0].Status != clusteroffline.DoctorFatal {
		t.Fatalf("issuer skew must yield one FATAL auth-issuer-skew check, got %+v", got)
	}

	// Unreadable account.nk is NOT a skew (it is the separate secrets FATAL): no skew check.
	dir3 := t.TempDir()
	_ = os.WriteFile(filepath.Join(dir3, "account.nk"), []byte("garbage"), 0o600)
	conf3 := filepath.Join(dir3, "nats.conf")
	_ = os.WriteFile(conf3, []byte(confWith(otherAcctPub, brkPub2)), 0o600)
	if got := clusterAuthIssuerSkewChecks(dir3, conf3); len(got) != 0 {
		t.Fatalf("an unreadable seed is not a skew (secrets check owns it), got %+v", got)
	}
}

// TestClusterDoctorReportsIssuerSkew is the END-TO-END facet-2 test: `cluster doctor --offline`
// against a rotated-but-not-re-rendered account.nk exits non-zero AND prints the FATAL skew row. The
// mutation half (a matching conf) proves the row is driven by the actual skew, not always emitted.
func TestClusterDoctorReportsIssuerSkew(t *testing.T) {
	runDoctor := func(dir, conf string) (string, error) {
		cmd := newClusterDoctorCmd()
		var buf bytes.Buffer
		cmd.SetOut(&buf)
		cmd.SetErr(&buf)
		cmd.SetArgs([]string{"--offline", "--secrets-dir", dir, "--conf", conf, "--db", filepath.Join(dir, "nonexistent.db")})
		err := cmd.Execute()
		return buf.String(), err
	}

	// SKEW: on-disk account.nk differs from the conf issuer.
	otherAcct, _ := nkeys.CreateAccount()
	otherAcctPub, _ := otherAcct.PublicKey()
	dir, conf, _, brkPub := skewFixture(t, otherAcctPub, "PLACEHOLDER")
	_ = os.WriteFile(conf, []byte(confWith(otherAcctPub, brkPub)), 0o600)
	out, err := runDoctor(dir, conf)
	if err == nil {
		t.Fatalf("doctor on a known issuer skew must exit non-zero; out=%s", out)
	}
	if !strings.Contains(out, "auth-issuer-skew") || !strings.Contains(out, "FATAL") {
		t.Fatalf("doctor must print the FATAL auth-issuer-skew row on a skewed conf; out=%s", out)
	}

	// MUTATION: match the conf issuer to the on-disk account.nk — the skew row must DISAPPEAR.
	dir2, conf2, acctPub2, brkPub2 := skewFixture(t, "PLACEHOLDER", "PLACEHOLDER")
	_ = os.WriteFile(conf2, []byte(confWith(acctPub2, brkPub2)), 0o600)
	out2, _ := runDoctor(dir2, conf2)
	if strings.Contains(out2, "auth-issuer-skew") {
		t.Fatalf("a matching conf must NOT report an issuer skew; out=%s", out2)
	}
}

// TestReconcileNatsFailsClosedOnIssuerSkew is the END-TO-END facet-1 test: `reconcile nats` against a
// rotated-but-not-re-rendered account.nk returns a non-zero error naming the skew, instead of the
// old false all-clear. Mutation: a matching conf lets it proceed (nil error).
func TestReconcileNatsFailsClosedOnIssuerSkew(t *testing.T) {
	sock := "/nonexistent/admin.sock"
	runReconcile := func(dir, conf string) (string, error) {
		cmd := newClusterReconcileNatsCmd(&sock)
		var buf bytes.Buffer
		cmd.SetOut(&buf)
		cmd.SetErr(&buf)
		// No --wait: the skew check runs BEFORE the (unreachable) socket poll.
		cmd.SetArgs([]string{"--secrets-dir", dir, "--conf", conf})
		err := cmd.Execute()
		return buf.String(), err
	}

	// SKEW → non-zero error naming the skew.
	otherAcct, _ := nkeys.CreateAccount()
	otherAcctPub, _ := otherAcct.PublicKey()
	dir, conf, _, brkPub := skewFixture(t, otherAcctPub, "PLACEHOLDER")
	_ = os.WriteFile(conf, []byte(confWith(otherAcctPub, brkPub)), 0o600)
	_, err := runReconcile(dir, conf)
	if err == nil || !strings.Contains(err.Error(), "skew") {
		t.Fatalf("reconcile nats on a known issuer skew must fail-closed naming the skew, got %v", err)
	}

	// MUTATION: matching conf → no skew → the command proceeds (nil error, prints the auto message).
	dir2, conf2, acctPub2, brkPub2 := skewFixture(t, "PLACEHOLDER", "PLACEHOLDER")
	_ = os.WriteFile(conf2, []byte(confWith(acctPub2, brkPub2)), 0o600)
	if _, err := runReconcile(dir2, conf2); err != nil {
		t.Fatalf("reconcile nats with a matching conf must not error on skew, got %v", err)
	}
}

// TestClusterDoctorJSONStreamStaysParseableOnFatal is the DEPLOY-TIER regression (drill 52 B3/55c):
// `cluster doctor --json` correctly DETECTS the skew, but the process main sink used to print an
// "error: ..." line to stderr on the non-zero exit — and a caller that merges the streams
// (`... --json 2>&1 | jq`) then got the JSON FOLLOWED BY prose and could not parse it, hiding the
// FATAL from every machine consumer. This reproduces the exact merge the earlier hermetic test
// missed (it never combined stderr). MUTATION: revert renderDoctor's JSON FATAL to a non-quiet
// usageErr and this goes RED (json.Unmarshal fails on the trailing prose).
func TestClusterDoctorJSONStreamStaysParseableOnFatal(t *testing.T) {
	// A REAL skew: on-disk account.nk derives a different issuer than the conf renders.
	otherAcct, _ := nkeys.CreateAccount()
	otherAcctPub, _ := otherAcct.PublicKey()
	dir, conf, _, brkPub := skewFixture(t, otherAcctPub, "PLACEHOLDER")
	_ = os.WriteFile(conf, []byte(confWith(otherAcctPub, brkPub)), 0o600)

	// Invoke through the ROOT command exactly as production does: the root sets
	// SilenceUsage/SilenceErrors, so cobra itself prints nothing on a RunE error — the ONLY stderr
	// writer is the main sink (renderTerminalError). Invoking the subcommand standalone would instead
	// trigger cobra's own usage/error dump, which never happens in the real binary.
	root := newRootCmd()
	var stdout, stderr bytes.Buffer
	root.SetOut(&stdout)
	root.SetErr(&stderr)
	root.SetArgs([]string{"cluster", "doctor", "--offline", "--secrets-dir", dir, "--conf", conf, "--db", filepath.Join(dir, "nope.db"), "--json"})
	err := root.Execute()

	// Simulate the process main sink (renderTerminalError = what main() prints to stderr) + the shell's
	// 2>&1 merge — exactly what the drill's `... --json 2>&1 | jq` receives.
	renderTerminalError(&stderr, err)
	merged := stdout.String() + stderr.String()

	// The merged stream MUST be a single parseable JSON value. json.Unmarshal (like jq) rejects any
	// trailing non-whitespace, so an "error: ..." line appended after the JSON fails here.
	var doc struct {
		Checks []struct {
			Name   string `json:"name"`
			Status string `json:"status"`
		} `json:"checks"`
		Summary struct {
			Fatal int `json:"fatal"`
		} `json:"summary"`
	}
	if e := json.Unmarshal([]byte(merged), &doc); e != nil {
		t.Fatalf("cluster doctor --json 2>&1 stream must be parseable JSON even on FATAL; got parse error %v\nstream=%q", e, merged)
	}
	// The skew must be visible as a FATAL check, and the summary must count it.
	var sawSkew bool
	for _, c := range doc.Checks {
		if c.Name == "auth-issuer-skew" && c.Status == "FATAL" {
			sawSkew = true
		}
	}
	if !sawSkew {
		t.Fatalf("JSON must carry a FATAL auth-issuer-skew check; stream=%q", merged)
	}
	if doc.Summary.Fatal < 1 {
		t.Fatalf("summary.fatal must be >=1; stream=%q", merged)
	}
	// And it must still be a non-zero exit (the drill asserts exit != 0 too).
	if classifyExit(err) == exitOK {
		t.Fatal("a FATAL doctor must still exit non-zero")
	}
}
