//go:build linux && hotupdate_integration

package hotupdate_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agentimage"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/upgradecli"
	"github.com/LinZiyang666/tether/internal/upgradelock"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/test/transferharness"
)

// A broker cut needs the same explicit deployment identity as the agent cut.
// Registration precedes boot; the fixture never writes the daemon's journal.
// origin: S8 internal review activation-and-route-migration F3
func registerTransferBroker(t *testing.T, s *transferStack, binary, dataDir, db string, clustered bool) string {
	t.Helper()
	config := filepath.Join(dataDir, "broker-upgrade.yaml")
	body := fmt.Sprintf("broker:\n  storage:\n    db: %q\n", db)
	if clustered {
		body += fmt.Sprintf("  cluster:\n    data_dir: %q\n", dataDir)
	}
	transferMust(s.t, os.WriteFile(config, []byte(body), 0600))
	role := upgradecli.BrokerBootRole(dataDir, config, "", uint32(os.Geteuid()))
	transferMust(s.t, os.MkdirAll(filepath.Join(role.ControlDir, "artifacts"), 0700))
	_, err := upgradelock.RegisterDomain(upgradelock.Registration{CanonicalPath: binary, ControlDir: role.ControlDir, DeploymentID: "transfer-broker-fixture", Participants: []upgradelock.Participant{{Key: role.Want, ServiceID: "tether-broker"}}})
	transferMust(s.t, err)
	return config
}

// transferPendingCut owns one target-issued grant and its supervised Start.
// Fault arms inspect that original transaction; an uncertain reply authorizes
// only wait/query of this ID and never a replacement Start.
// origin: hot-update S8 plan §3.1 / §5.2 / §7.1
type transferPendingCut struct {
	tx     string
	start  *transferharness.Process
	broker bool
}

func (s *transferStack) startBrokerTransaction(candidate, expected string) transferPendingCut {
	s.t.Helper()
	// S6's local describe reads the candidate at the canonical entry. Supplying
	// those bytes is inert: Start still requires target authorization and root
	// publication of both the /proc source and this candidate. No .prev is made.
	staged := filepath.Join(s.h.Root, "broker-candidate")
	transferCopy(s.t, candidate, staged)
	transferMust(s.t, os.Rename(staged, s.brokerPath))
	nonce := fmt.Sprint(time.Now().UnixNano())
	grantPath, receiptPath := filepath.Join(s.h.Root, "broker-grant-"+nonce), filepath.Join(s.h.Root, "broker-receipt-"+nonce)
	r, out, err := s.ctlRun(s.home, 60*time.Second, "broker", "upgrade", "reload", "--socket", s.admin, "--to-version", proto.ReleaseVersion, "--expect-sha256", expected, "--prepare-only", "--grant-out", grantPath)
	if err != nil || r.NativeRC != 0 {
		s.t.Fatalf("broker prepare: native rc=%d: %v: %s", r.NativeRC, err, out)
	}
	var grant upgradetx.Grant
	body, err := os.ReadFile(grantPath)
	transferMust(s.t, err)
	transferMust(s.t, json.Unmarshal(body, &grant))
	before, err := s.broker.SampleIdentity()
	transferMust(s.t, err)
	if grant.Handle.TxID == "" || grant.SourceSHA != before.RunningSHA || grant.TargetSHA != expected {
		s.t.Fatal("broker grant names other running or candidate bytes")
	}
	// The registered config, carried in Key, resolves the broker's own control
	// directory even when its selected origin is a different cluster peer.
	r, out, err = s.ctlRun(s.home, 60*time.Second, "upgrade", "stage", "--grant", grantPath, "--source", fmt.Sprintf("/proc/%d/exe", before.PID), "--candidate", candidate, "--receipt-out", receiptPath)
	if err != nil || r.NativeRC != 0 {
		s.t.Fatalf("broker stage: native rc=%d: %v: %s", r.NativeRC, err, out)
	}
	p := s.launch("command", filepath.Join(s.h.Root, "ctl"), s.home, []string{"broker", "upgrade", "reload", "--socket", s.admin, "--tx-id", grant.Handle.TxID, "--artifact-receipt", receiptPath}, false)
	return transferPendingCut{tx: grant.Handle.TxID, start: p, broker: true}
}

func (s *transferStack) awaitTransactionStart(cut transferPendingCut) {
	s.t.Helper()
	r, out, err := s.ctlWait(cut.start, 90*time.Second, "upgrade start")
	if err != nil || r.NativeRC != 0 && r.NativeRC != 74 {
		s.t.Fatalf("transaction start %s: native rc=%d: %v: %s", cut.tx, r.NativeRC, err, out)
	}
	s.t.Logf("transaction start %s: native rc=%d: %s", cut.tx, r.NativeRC, out)
}

func (s *transferStack) waitBrokerTransaction(tx string) {
	s.t.Helper()
	r, out, err := s.ctlRun(s.home, 150*time.Second, "broker", "upgrade", "wait", "--socket", s.admin, "--tx-id", tx, "--timeout", "120s")
	if err != nil || r.NativeRC != 0 {
		s.t.Fatalf("broker wait %s: native rc=%d: %v: %s", tx, r.NativeRC, err, out)
	}
	r, out, err = s.ctlRun(s.home, 30*time.Second, "broker", "upgrade", "status", "--socket", s.admin, "--tx-id", tx, "--json")
	if err != nil || r.NativeRC != 0 {
		s.t.Fatalf("broker status %s: native rc=%d: %v: %s", tx, r.NativeRC, err, out)
	}
	var view upgradetx.View
	body, err := os.ReadFile(r.StdoutPath)
	transferMust(s.t, err)
	transferMust(s.t, json.Unmarshal(body, &view))
	transferMust(s.t, upgradetx.Completed(view.Observation.Current, view.Observation))
	if view.Observation.Current.Handle.TxID != tx {
		s.t.Fatal("broker completion belongs to a different transaction")
	}
}

// The selected executable becomes visible before the reverse imports owners.
// Wait on that same accepted transaction before reading custody; a SHA alone
// cannot prove that the original transfer has activated in the returned image.
// The original forward goal missed its target, so native Wait must report64,
// while the independent current reverse must have all three completed receipts.
// origin: S8 internal review activation-and-route-migration F10; S8 plan §4.1
func (s *transferStack) waitAgentRollback(tx, sourceSHA string) {
	r, out, err := s.ctlRun(s.home, 150*time.Second, "node", "upgrade-wait", s.nid, "--tx-id", tx, "--timeout", "120s", "--home", s.home, "--nats-url", s.url)
	if err != nil || r.NativeRC != 64 {
		s.t.Fatalf("automatic reverse wait %s: native rc=%d: %v: %s", tx, r.NativeRC, err, out)
	}
	r, out, err = s.ctlRun(s.home, 30*time.Second, "node", "upgrade-status", s.nid, "--tx-id", tx, "--json", "--home", s.home, "--nats-url", s.url)
	if err != nil || r.NativeRC != 0 {
		s.t.Fatalf("automatic reverse status %s: native rc=%d: %v: %s", tx, r.NativeRC, err, out)
	}
	var view upgradetx.View
	body, err := os.ReadFile(r.StdoutPath)
	transferMust(s.t, err)
	transferMust(s.t, json.Unmarshal(body, &view))
	transferMust(s.t, upgradetx.Completed(view.Observation.Current, view.Observation))
	a := view.Observation.Current
	if a.Direction != upgradetx.DirectionReverse || a.Handle.TxID != tx || a.TargetSHA != sourceSHA || view.Observation.RunningSHA != sourceSHA || view.Historical.Forward.Outcome != upgradetx.OutcomeRolledBack {
		s.t.Fatal("watchdog did not complete the current reverse of the original transaction")
	}
}

// The resource-baseline artifacts intentionally have distinct release strings.
// Probe the actual candidate instead of borrowing the task fixture's label.
// origin: S8 internal review activation-and-route-migration F3
func transferArtifactRelease(s *transferStack, path string) string {
	s.t.Helper()
	local := filepath.Join(s.h.Root, "candidate-version")
	transferCopy(s.t, path, local)
	p := s.launch("candidate-version", local, s.home, []string{"version"}, false)
	r, out, err := s.ctlWait(p, 10*time.Second, "candidate version")
	if err != nil || r.NativeRC != 0 {
		s.t.Fatalf("candidate version: native rc=%d: %v: %s", r.NativeRC, err, out)
	}
	release, _, err := agentimage.ParseCapabilities(out)
	transferMust(s.t, err)
	return release
}
