//go:build linux && hotupdate_integration

package hotupdate_test

// Real: workspace-built serve/agent/ctl, the private-inbox ctl connection, the
// broker's owner admission and upgrade-tx forwarding, the agent's journal,
// runtime port, adjunct, same-PID exec and completion receipts, the root stage
// publication. Simulated: loopback infrastructure, an httptest artifact host
// and the deployment registration an installer retrofit would write (R5 has
// no production writer yet).

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradecli"
	"github.com/LinZiyang666/tether/internal/upgradelock"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/test/transferharness"
)

// registerTransferAgent registers the fixture agent's deployment domain before
// the agent's first start, as an installer retrofit would: canonical entry,
// owner-only control directory with its artifacts slot, one participant Key.
// Without it the agent has no journal, advertises no upgrade-tx capability and
// the broker answers every transaction with the shared legacy refusal.
// origin: S8 FIX-DESCRIBE (P19 requests R3/R5/R7)
func registerTransferAgent(t *testing.T, s *transferStack) {
	t.Helper()
	role := upgradecli.AgentBootRole(s.agentHome, s.sid, s.nid, uint32(os.Geteuid()))
	transferMust(t, os.MkdirAll(filepath.Join(role.ControlDir, "artifacts"), 0o700))
	transferMust(t, os.Chmod(role.ControlDir, 0o700))
	_, err := upgradelock.RegisterDomain(upgradelock.Registration{CanonicalPath: s.agentPath, ControlDir: role.ControlDir,
		DeploymentID: "transfer-fixture", Participants: []upgradelock.Participant{{Key: role.Want, ServiceID: "tether-agent@" + s.sid}}})
	transferMust(t, err)
}

// ctlRun runs the BUILT ctl once under the shared supervisor and returns its
// native receipt and combined output. home is TETHER_HOME: the ctl identity for
// node verbs, the component's own home for the root stage (whose control
// directory is resolved from the configured component, never the grant file).
func (s *transferStack) ctlRun(home string, budget time.Duration, args ...string) (transferharness.Receipt, string, error) {
	return s.ctlWait(s.launch("command", filepath.Join(s.h.Root, "ctl"), home, args, false), budget, args[0])
}

// ctlWait joins one launched ctl within its budget (ctlRun's second half).
func (s *transferStack) ctlWait(p *transferharness.Process, budget time.Duration, verb string) (transferharness.Receipt, string, error) {
	ctx, cancel := context.WithTimeout(s.t.Context(), budget)
	defer cancel()
	r, err := p.Wait(ctx)
	stdout, _ := os.ReadFile(r.StdoutPath)
	output := string(stdout) + s.output(p)
	if err != nil && r.NativeRC <= 0 {
		return r, output, fmt.Errorf("ctl %s: native rc=%d: %w: %s", verb, r.NativeRC, err, output)
	}
	return r, output, nil
}

// awaitFreezeIntent returns once the target's own journal records this
// transaction's freeze intent: the engine persists it immediately before it
// freezes the business owners, so it is the cut's entry. Reading the durable
// record only observes; it decides nothing for the product. It also returns
// when the Start ended first (the cut never reached its freeze).
// origin: S8 FIX-DESCRIBE (P19 request R7; deterministic cut-entry release)
func (s *transferStack) awaitFreezeIntent(start *transferharness.Process, tx string) bool {
	plan, err := upgradecli.RegisteredJournal(upgradecli.AgentBootRole(s.agentHome, s.sid, s.nid, uint32(os.Geteuid())))
	transferMust(s.t, err)
	entered := false
	testharness.WaitFor(s.t, 60*time.Second, 2*time.Millisecond, func() bool {
		if body, err := os.ReadFile(plan.Journal); err == nil {
			entered = journalHasAction(body, tx, string(upgradetx.ActionFreeze))
		}
		return entered || start.Receipt().NativeRC != -1
	})
	return entered
}

// journalHasAction finds an action object of this kind for this transaction
// anywhere in the journal's JSON, without depending on its envelope layout.
func journalHasAction(body []byte, tx, kind string) bool {
	var doc any
	if json.Unmarshal(body, &doc) != nil {
		return false
	}
	var walk func(any) bool
	walk = func(v any) bool {
		switch value := v.(type) {
		case map[string]any:
			if value["kind"] == kind {
				if handle, ok := value["handle"].(map[string]any); ok && handle["tx_id"] == tx {
					return true
				}
			}
			for _, child := range value {
				if walk(child) {
					return true
				}
			}
		case []any:
			for _, child := range value {
				if walk(child) {
					return true
				}
			}
		}
		return false
	}
	return walk(doc)
}

// transactionCut drives the authorized S8 route an operator uses, with the
// BUILT ctl: the target describes itself and issues an inert grant (prepare),
// the stage command publishes the running source and the candidate under that
// exact grant, the original actor starts that grant by TxID, and wait settles
// it. The old `node upgrade --url` dispatch is never used; the broker's shared
// legacy policy refuses it by design. Start may legitimately lose its reply to
// the exec (74, accepted); only wait's rc0 counts as completion.
// origin: S8 FIX-DESCRIBE (P19 request R7)
func (s *transferStack) startAgentTransaction(candidate, url string, archive []byte) (transferPendingCut, error) {
	nonce := fmt.Sprintf("%d", time.Now().UnixNano())
	grantPath, receiptPath := filepath.Join(s.h.Root, "grant-"+nonce+".json"), filepath.Join(s.h.Root, "receipt-"+nonce+".json")
	// Per-step wall time is part of the receipt: a live transfer's producer
	// stays parked across the whole route, so a slow step is a finding.
	began, steps := time.Now(), []string{}
	step := func(name string) {
		steps = append(steps, fmt.Sprintf("%s=%s", name, time.Since(began).Round(time.Millisecond)))
		began = time.Now()
	}
	defer func() { s.t.Logf("transaction cut steps: %s", strings.Join(steps, " ")) }()
	r, out, err := s.ctlRun(s.home, 60*time.Second, "node", "upgrade", s.nid, "--url", url, "--sha256", transferDigest(archive),
		"--prepare-only", "--grant-out", grantPath, "--home", s.home, "--nats-url", s.url)
	step("prepare")
	if err != nil || r.NativeRC != 0 {
		return transferPendingCut{}, fmt.Errorf("prepare: native rc=%d: %v: %s", r.NativeRC, err, out)
	}
	var grant upgradetx.Grant
	body, err := os.ReadFile(grantPath)
	if err == nil {
		err = json.Unmarshal(body, &grant)
	}
	if err != nil || len(grant.Handle.TxID) != 32 {
		return transferPendingCut{}, fmt.Errorf("prepare wrote no target-issued grant: %v", err)
	}
	tx := grant.Handle.TxID
	agent := s.agent.Receipt()
	r, out, err = s.ctlRun(s.agentHome, 60*time.Second, "upgrade", "stage", "--grant", grantPath,
		"--source", fmt.Sprintf("/proc/%d/exe", agent.PID), "--candidate", candidate, "--receipt-out", receiptPath)
	step("stage")
	if err != nil || r.NativeRC != 0 {
		return transferPendingCut{tx: tx}, fmt.Errorf("stage: native rc=%d: %v: %s", r.NativeRC, err, out)
	}
	startArgs := []string{"node", "upgrade", s.nid, "--tx-id", tx, "--artifact-receipt", receiptPath, "--home", s.home, "--nats-url", s.url}
	startCtl := s.launch("command", filepath.Join(s.h.Root, "ctl"), s.home, startArgs, false)
	// The cut enters at its journaled freeze intent: a producer parked in the
	// process being cut is released only then — not before the describe,
	// candidate download, root stage and preflight that freeze nothing, during
	// which a released producer finished a whole transfer.
	// origin: S8 FIX-DESCRIBE (P19 request R7)
	if entry := s.cutEntry; entry != nil {
		s.cutEntry = nil
		if !s.awaitFreezeIntent(startCtl, tx) {
			s.t.Logf("transaction %s ended before its freeze intent; releasing the parked producer", tx)
		}
		entry()
		step("cut-entry-release")
	}
	r, out, err = s.ctlWait(startCtl, 90*time.Second, "node")
	step("start")
	if err != nil || r.NativeRC != 0 && r.NativeRC != 74 {
		return transferPendingCut{tx: tx}, fmt.Errorf("start: native rc=%d: %v: %s", r.NativeRC, err, out)
	}
	start, startRC := out, r.NativeRC
	s.t.Logf("start %s: native rc=%d: %s", tx, startRC, start)
	return transferPendingCut{tx: tx, start: startCtl}, nil
}

func (s *transferStack) transactionCut(candidate, url string, archive []byte) (string, error) {
	cut, err := s.startAgentTransaction(candidate, url, archive)
	if err != nil {
		return cut.tx, err
	}
	tx, begin := cut.tx, time.Now()
	r, out, err := s.ctlRun(s.home, 150*time.Second, "node", "upgrade-wait", s.nid, "--tx-id", tx, "--timeout", "120s",
		"--home", s.home, "--nats-url", s.url)
	s.t.Logf("transaction wait %s: %s", tx, time.Since(begin).Round(time.Millisecond))
	if err != nil || r.NativeRC != 0 {
		// The target's own record names why it settled elsewhere (the
		// journaled detail of a cancellation or deferral); keep it in the receipt.
		_, status, _ := s.ctlRun(s.home, 30*time.Second, "node", "upgrade-status", s.nid, "--tx-id", tx, "--json", "--home", s.home, "--nats-url", s.url)
		return tx, fmt.Errorf("wait: native rc=%d: %v: %s\nstatus: %s", r.NativeRC, err, out, status)
	}

	// Start's early pending notice is distinct from target completion. Keep a
	// fresh native View so a later original-ctl failure cannot be misreported
	// as an activating transaction that never completed.
	// origin: S8 internal review activation-and-route-migration F3
	r, status, err := s.ctlRun(s.home, 30*time.Second, "node", "upgrade-status", s.nid, "--tx-id", tx, "--json", "--home", s.home, "--nats-url", s.url)
	if err != nil || r.NativeRC != 0 {
		return tx, fmt.Errorf("current agent status: native rc=%d: %v: %s", r.NativeRC, err, status)
	}
	var view upgradetx.View
	body, err := os.ReadFile(r.StdoutPath)
	if err == nil {
		err = json.Unmarshal(body, &view)
	}
	if err == nil {
		err = upgradetx.Completed(view.Observation.Current, view.Observation)
	}
	if err != nil || view.Observation.Current.Handle.TxID != tx {
		return tx, fmt.Errorf("agent current completion differs: %v", err)
	}
	s.t.Logf("agent current completion tx_id=%s phase=%s running_sha=%s control=%t resources=%t released=%t", tx, view.Observation.Current.Phase, view.Observation.RunningSHA, view.Observation.Current.Control.Complete, view.Observation.Current.Resources.Complete, view.Observation.Current.Released.Complete)
	return tx, nil
}

// buildAdvertisingArtifacts builds the product twice-identical (A, and B = A
// plus trailing bytes) with ONE overlay edit: the version reader declarations
// add `image-features upgrade-tx-v1 authority-incarnation-v1 upgrade-cut-v1`
// and the `upgrade-cut-v1` domain, exactly the line the lead will single-source
// once every exporter exists. Production does not advertise them, so a real
// cut is otherwise refused at preflight (reader_capability_missing). An outer
// GOFLAGS overlay (a production mutation) still reaches both artifacts.
// origin: S8 FIX-DESCRIBE (acceptance: real agent cut through the CLI)
// Since S8 BROKER-EXPORTER R-11 production emits exactly that line, and the
// edit is skipped: the artifacts are the production declarations unchanged.
func buildAdvertisingArtifacts(t *testing.T) transferArtifacts {
	t.Helper()
	root, err := filepath.Abs("../..")
	transferMust(t, err)
	dir := testharness.GuardProcessDirectory(t, t.TempDir())
	// The transfer harness's own observation/fault overlay (which already
	// merges any outer GOFLAGS overlay) is the base; one replacement is added.
	replacements := map[string]string{}
	var kept []string
	for _, flag := range strings.Fields(transferFaultOverlay(t, root, dir)) {
		name, ok := strings.CutPrefix(flag, "-overlay=")
		if !ok {
			kept = append(kept, flag)
			continue
		}
		data, err := os.ReadFile(name)
		transferMust(t, err)
		var prior struct{ Replace map[string]string }
		transferMust(t, json.Unmarshal(data, &prior))
		for from, to := range prior.Replace {
			replacements[from] = to
		}
	}
	codec := filepath.Join(root, "internal/upgradeprotocol/codec.go")
	read := codec
	if replacement, ok := replacements[codec]; ok {
		read = replacement
	}
	data, err := os.ReadFile(read)
	transferMust(t, err)
	anchor := `return []string{domains, "image-read-epochs 1", abi}`
	advertised := `return []string{domains + " upgrade-cut-v1", "image-read-epochs 1", "image-features upgrade-tx-v1 authority-incarnation-v1 upgrade-cut-v1", abi}`
	// origin: S8 BROKER-EXPORTER R-11 — production now advertises exactly these
	// declarations (every exporter exists), so the overlay has nothing to add.
	switch {
	case strings.Count(string(data), advertised) == 1:
	case strings.Count(string(data), anchor) == 1:
		patched := filepath.Join(dir, "upgradeprotocol-codec.go")
		transferMust(t, os.WriteFile(patched, []byte(strings.Replace(string(data), anchor, advertised, 1)), 0o600))
		replacements[codec] = patched
	default:
		t.Fatal("NOT_RUN: the version declaration anchor moved")
	}
	overlay, err := json.Marshal(map[string]map[string]string{"Replace": replacements})
	transferMust(t, err)
	overlayPath := filepath.Join(dir, "advertising-overlay.json")
	transferMust(t, os.WriteFile(overlayPath, overlay, 0o600))
	a := transferArtifacts{a: filepath.Join(dir, "original"), b: filepath.Join(dir, "candidate")}
	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "go", "build", "-race", "-trimpath", "-o", a.a, "./cmd/tether")
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "GOFLAGS="+strings.Join(append(kept, "-overlay="+overlayPath), " "))
	buildTransferCandidate(t, cmd, a.a)
	body, err := os.ReadFile(a.a)
	transferMust(t, err)
	transferMust(t, os.WriteFile(a.b, append(body, []byte("\nS8 describe candidate\n")...), 0o700))
	a.shaA, err = transferharness.FileSHA(a.a)
	transferMust(t, err)
	a.shaB, err = transferharness.FileSHA(a.b)
	transferMust(t, err)
	if a.shaA == a.shaB {
		t.Fatal("transaction artifacts have identical SHA")
	}
	// The receipt line names the same source identity as buildTransferArtifacts.
	head := exec.CommandContext(ctx, "git", "rev-parse", "HEAD")
	head.Dir = root
	out, err := testharness.Output(head)
	transferMust(t, err)
	a.source, a.dirty = strings.TrimSpace(string(out)), hotUpdateSourceDigest(t, root)
	a.archiveA, a.archiveB = transferArchive(t, a.a), transferArchive(t, a.b)
	t.Logf("source_sha=%s dirty_digest=%s artifact_A=%s artifact_B=%s overlay=image-features", a.source, a.dirty, a.shaA, a.shaB)
	return a
}

// The BUILT ctl drives two real agent cuts on one real broker through the
// authorized transaction route: describe → prepare → root stage → start →
// wait reaches completed both ways (A→B, then B→A as a second transaction),
// the agent keeps its original PID and kernel start tick, and /proc/<pid>/exe
// is the requested image each time. status reports the completed original
// transaction with all three receipts. Only the reader declaration line is an
// overlay (see buildAdvertisingArtifacts).
// origin: S8 FIX-DESCRIBE (P19 requests R1/R3/R4/R7 acceptance)
func TestTransactionCLIPrepareStartWaitCompletesAgentCut(t *testing.T) {
	a := buildAdvertisingArtifacts(t)
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	t.Run("rounds", func(t *testing.T) {
		for round := 0; round < 5; round++ {
			t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) {
				t.Parallel()
				exerciseTransactionAgentCut(t, a)
			})
		}
	})
	testharness.AssertNoGoroutineLeak(t, "transaction agent cut", g)
	testharness.AssertNoFDLeak(t, "transaction agent cut", fd, 0)
}

func exerciseTransactionAgentCut(t *testing.T, a transferArtifacts) {
	s := newTransferStack(t, a)
	for _, step := range []struct {
		candidate, path, sha string
		archive              []byte
	}{{a.b, "/candidate.tar.gz", a.shaB, a.archiveB}, {a.a, "/original.tar.gz", a.shaA, a.archiveA}} {
		before, err := s.agent.SampleIdentity()
		transferMust(t, err)
		tx, err := s.transactionCut(step.candidate, s.artifact.URL+step.path, step.archive)
		if err != nil {
			t.Fatalf("authorized cut to %s: %v\nagent:\n%s", step.sha, err, s.output(s.agent))
		}
		after, err := s.agent.SampleIdentity()
		transferMust(t, err)
		if err := validateTransferProcess(before, after, step.sha); err != nil {
			t.Fatalf("cut %s: %v (before %+v after %+v)", tx, err, before, after)
		}
		r, out, err := s.ctlRun(s.home, 30*time.Second, "node", "upgrade-status", s.nid, "--tx-id", tx, "--json", "--home", s.home, "--nats-url", s.url)
		if err != nil || r.NativeRC != 0 {
			t.Fatalf("status %s: rc=%d %v: %s", tx, r.NativeRC, err, out)
		}
		for _, want := range []string{`"phase":"completed"`, `"control_ready"`, `"resources_migrated"`, `"old_generation_released"`, `"effective_sha":"` + step.sha + `"`} {
			if !strings.Contains(out, want) {
				t.Fatalf("status %s lacks %s:\n%s", tx, want, out)
			}
		}
	}
}
