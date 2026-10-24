//go:build linux

package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agentupgrade"
	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradeartifact"
	"github.com/LinZiyang666/tether/internal/upgradecli"
	"github.com/LinZiyang666/tether/internal/upgradelock"
	"github.com/LinZiyang666/tether/internal/upgradeobserve"
	"github.com/LinZiyang666/tether/internal/upgradeport"
	"github.com/LinZiyang666/tether/internal/upgradestore"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/internal/upgradewire"
	"github.com/nats-io/nats.go"
)

// coldBootRuntime supplies only the target's local runtime facts. Journal,
// transitions, publication, registry and the agent engine are production code;
// this runtime certifies neither the agent's real RuntimePort nor a same-PID
// cut (those are P14/P17/P23). It exists so the BUILT ctl's stage → original-tx
// resume → ordinary cold boot chain runs against a real accepted journal.
type coldBootRuntime struct {
	store     *upgradestore.Store
	registry  upgradeobserve.Registry
	owner     upgradetx.Owner
	scope     upgradetx.Scope
	running   string
	resources []upgradetx.Resource
	effects   int
}

func (r *coldBootRuntime) current() upgradetx.Attempt {
	v, err := r.store.Load(r.scope.Participants[0], "")
	if err != nil {
		return upgradetx.Attempt{}
	}
	if v.Historical.Reverse.Handle.TxID != "" {
		return v.Historical.Reverse
	}
	return v.Historical.Forward
}

func (r *coldBootRuntime) census(s upgradetx.Stamp, scope upgradetx.Scope) upgradetx.Census {
	s.Producer, s.Sequence, s.At = r.owner, 1, time.Now().UnixNano()
	s.ScopeDigest, _ = upgradetx.ScopeDigest(scope)
	expected, _ := upgradetx.ExpectedDigest(scope)
	return upgradetx.Census{Stamp: s, Known: true, Complete: true, ExpectedDigest: expected, Resources: append([]upgradetx.Resource{}, r.resources...)}
}

func (r *coldBootRuntime) Preflight(_ context.Context, g upgradetx.Grant, _ upgradeartifact.Receipt) (upgradeport.PreflightProof, error) {
	c := r.census(upgradetx.Stamp{Attempt: g.Handle, Nonce: strings.Repeat("e", 32)}, r.scope)
	return upgradeport.PreflightProof{Grant: g, ReadersDigest: strings.Repeat("d", 64), ConfigDigest: strings.Repeat("e", 64), Census: c}, nil
}
func (r *coldBootRuntime) cut(h upgradetx.Handle) (upgradeport.Cut, error) {
	return upgradeport.Cut{Attempt: h, Owner: r.owner, Stage: upgradetx.CutPristine, Descriptor: []byte("state"), Scope: r.current().Scope, Sequence: 1}, nil
}
func (r *coldBootRuntime) Freeze(h upgradetx.Handle) (upgradeport.Cut, error)        { return r.cut(h) }
func (r *coldBootRuntime) ExportCurrent(h upgradetx.Handle) (upgradeport.Cut, error) { return r.cut(h) }
func (r *coldBootRuntime) Activate(c upgradeport.Cut) (upgradeport.Activation, error) {
	r.effects++
	a := r.current()
	r.running = a.TargetSHA
	for i := range r.resources {
		r.resources[i].Owner, r.resources[i].Generation, r.resources[i].Custody = r.owner, a.Handle.Generation, "target"
	}
	return upgradeport.Activation{Attempt: c.Attempt, Owner: r.owner, Stage: upgradetx.CutActive, RunningSHA: r.running}, nil
}
func (r *coldBootRuntime) Observe(s upgradetx.Stamp) (upgradetx.Observation, error) {
	a := r.current()
	scope := r.scope
	if a.Handle.TxID != "" {
		scope = a.Scope
	}
	c := r.census(s, scope)
	if a.Handle.TxID != "" && r.running == a.TargetSHA && a.CutStage == upgradetx.CutPartialActive {
		a.CutStage = upgradetx.CutActive
	}
	if r.running == a.TargetSHA {
		a.Control = upgradetx.Receipt{Kind: upgradetx.ReceiptControlReady, Stamp: c.Stamp, Known: true, Complete: true, RunningSHA: r.running, Census: c, EvidenceDigest: strings.Repeat("e", 64)}
	}
	return upgradetx.Observation{Current: a, Nonce: s.Nonce, At: c.Stamp.At, OwnerKnown: true, RunningSHA: r.running, EffectiveSHA: r.running, Census: c}, nil
}
func (r *coldBootRuntime) Retire(h upgradetx.Handle) (upgradetx.Receipt, error) {
	a := r.current()
	c := r.census(upgradetx.Stamp{Attempt: h, Nonce: strings.Repeat("f", 32)}, a.Scope)
	return upgradetx.Receipt{Kind: upgradetx.ReceiptOldGenerationReleased, Stamp: c.Stamp, Known: true, Complete: true, RunningSHA: a.TargetSHA, Census: c, EvidenceDigest: strings.Repeat("e", 64)}, nil
}
func (r *coldBootRuntime) Thaw(upgradetx.Handle) error { return nil }
func (r *coldBootRuntime) Incarnation(upgradetx.Handle) (string, error) {
	return strings.Repeat("f", 64), nil
}

// installRegistry binds the production admission registry to the prepared attempt.
func (r *coldBootRuntime) installRegistry(t *testing.T, a upgradetx.Attempt) {
	t.Helper()
	var observers []upgradeobserve.DomainObserver
	for _, d := range a.Scope.Domains {
		domain := d
		observers = append(observers, upgradeobserve.DomainObserver{Domain: domain, CapabilityDigest: strings.Repeat("c", 64), Observe: func(s upgradetx.Stamp) (upgradetx.Census, error) {
			scope := r.current().Scope
			if scope.ID == "" {
				scope = a.Scope
			}
			c := r.census(s, scope)
			c.Stamp, c.Resources = s, nil
			for _, row := range r.resources {
				if row.Domain == domain {
					c.Resources = append(c.Resources, row)
				}
			}
			return c, nil
		}})
	}
	var err error
	r.registry, err = upgradeobserve.New(upgradeobserve.Config{Attempt: a, Producer: r.owner, Observers: observers, Probe: func(s upgradetx.Stamp) (upgradetx.Observation, error) {
		o, err := r.Observe(s)
		o.Census.Stamp, o.At = s, s.At
		return o, err
	}})
	if err != nil {
		t.Fatal(err)
	}
}

// startColdAgent launches an ordinary `tether agent` from path and waits for
// its canonical boot line; it never reaches a NATS server.
func startColdAgent(t *testing.T, path, home string) (*exec.Cmd, func() string, func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	cmd := exec.CommandContext(ctx, path, "agent", "--session", "lab", "--nid", "n1", "--nats-url", "nats://127.0.0.1:1", "--tunnel-addr", "", "--log-file", "-")
	cmd.Env = append(os.Environ(), "TETHER_HOME="+home, "HOME="+home, cli.DevNoAuthEnv+"=1")
	logPath := filepath.Join(t.TempDir(), "agent.log")
	log, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stdout, cmd.Stderr = log, log
	wait, err := testharness.StartProcess(cmd)
	_ = log.Close()
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	stopped := false
	stop := func() {
		if !stopped {
			stopped = true
			cancel()
			_ = cmd.Process.Kill()
			_ = wait()
		}
	}
	t.Cleanup(stop)
	output := func() string { body, _ := os.ReadFile(logPath); return string(body) }
	if !testharness.WaitFor(t, 20*time.Second, 20*time.Millisecond, func() bool { return strings.Contains(output(), "upgrade boot: running selected image") }) {
		t.Fatalf("agent never reported its canonical boot selection:\n%s", output())
	}
	return cmd, output, stop
}

// The BUILT ctl drives the readonly product chain end to end: the root stage
// publishes the inert target-issued grant's two images, the original actor
// starts exactly that grant by TxID with the receipt, a retry is a query, and
// an ordinary cold boot of the registered canonical entry exec's the durable
// effective selector (same PID) instead of the newest canonical bytes.
// origin: hot-update S8 plan §5.2 / IL13 / DB09 / §11.2 P19 (stage → original tx resume → cold boot)
func TestTransactionCLIStageResumeColdBoot(t *testing.T) {
	bin := transactionCLIBinary(t)
	url := testharness.StartNATS(t)
	root := t.TempDir()
	home, ctl := transactionCtlHome(t)
	canonical, candidate := filepath.Join(root, "install", "tether"), filepath.Join(root, "candidate", "tether")
	image, err := os.ReadFile(bin)
	if err != nil {
		t.Fatal(err)
	}
	for path, body := range map[string][]byte{canonical: image, candidate: append(append([]byte{}, image...), []byte("\nS8-P19-candidate\n")...)} {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, body, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	role := upgradecli.AgentBootRole(home, "lab", "n1", uint32(os.Geteuid()))
	if err := os.MkdirAll(filepath.Join(role.ControlDir, "artifacts"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(role.ControlDir, 0o700); err != nil {
		t.Fatal(err)
	}
	id, err := upgradelock.RegisterDomain(upgradelock.Registration{CanonicalPath: canonical, ControlDir: role.ControlDir, DeploymentID: "p19-cold-boot",
		Participants: []upgradelock.Participant{{Key: role.Want, ServiceID: "tether-agent@lab"}}})
	if err != nil {
		t.Fatal(err)
	}
	key := role.Want
	key.DomainID = id

	// Issuer: the canonical agent. With no journal it runs the canonical bytes.
	issuer, _, stopIssuer := startColdAgent(t, canonical, home)
	owner, err := upgradecli.ProcessOwner(issuer.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	owner.InstanceID = strings.Repeat("a", 32)
	sourceRelease, sourceCaps, err := probeUpgradeImage(canonical)
	if err != nil {
		t.Fatal(err)
	}
	targetRelease, targetCaps, err := probeUpgradeImage(candidate)
	if err != nil || targetCaps.SHA256 == sourceCaps.SHA256 {
		t.Fatalf("candidate probe: %v", err)
	}

	// The target's sole journal writer issues the inert grant (what --prepare-only obtains).
	journal := role.Journal(key, upgradelock.Registration{})
	if err := os.MkdirAll(filepath.Dir(journal), 0o700); err != nil {
		t.Fatal(err)
	}
	store, err := upgradestore.Open(upgradestore.Options{Path: journal, GuardPath: filepath.Join(role.ControlDir, id+".guard"), Key: key, Owner: owner, Transition: upgradetx.Transition})
	if err != nil {
		t.Fatal(err)
	}
	resource := upgradetx.Resource{Domain: upgradetx.DomainExec, ID: "job", Origin: "original", Generation: 1, Owner: owner, State: "active", Custody: "source"}
	scope := upgradetx.Scope{ID: strings.Repeat("d", 32), Participants: []upgradetx.Key{key}, Expected: []upgradetx.Resource{resource},
		Domains: []upgradetx.Domain{upgradetx.DomainAuthority, upgradetx.DomainBrokerRuntime, upgradetx.DomainBrokerStore, upgradetx.DomainControl, upgradetx.DomainExec,
			upgradetx.DomainFlow, upgradetx.DomainPTY, upgradetx.DomainSS, upgradetx.DomainTransferTracker, upgradetx.DomainTransferWorker, upgradetx.DomainUpgrade}}
	req, err := upgradecli.BuildPrepare(upgradecli.Target{Key: key, SourceSHA: sourceCaps.SHA256, SourceRelease: sourceRelease, Scope: scope},
		upgradecli.Candidate{SHA: targetCaps.SHA256, Release: targetRelease, CapabilityDigest: upgradecli.CapabilityDigest(upgradetx.ComponentAgent, targetCaps)}, ctl.Fingerprint, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	grant, err := store.Prepare(key, req)
	if err != nil {
		t.Fatal(err)
	}
	rt := &coldBootRuntime{store: store, owner: owner, scope: req.Scope, running: sourceCaps.SHA256, resources: []upgradetx.Resource{resource}}
	rt.installRegistry(t, upgradetx.Attempt{Handle: grant.Handle, SourceGeneration: grant.Handle.Generation - 1, SourceSHA: grant.SourceSHA, TargetSHA: grant.TargetSHA,
		Scope: req.Scope, Direction: upgradetx.DirectionForward, Phase: upgradetx.PhasePrepared, CutStage: upgradetx.CutNone, Outcome: upgradetx.OutcomePending})
	grantPath, receiptPath := filepath.Join(root, "grant.json"), filepath.Join(root, "receipt.json")
	if err := upgradecli.WriteGrant(grantPath, grant); err != nil {
		t.Fatal(err)
	}
	if err := upgradecli.RememberGrant(home, grant); err != nil {
		t.Fatal(err)
	}

	// Root stage through the built binary: publication only, no journal write.
	before, _ := store.Load(key, "")
	stage := runTransactionCLI(t, bin, home, "upgrade", "stage", "--grant", grantPath, "--source", fmt.Sprintf("/proc/%d/exe", issuer.Process.Pid), "--candidate", candidate, "--receipt-out", receiptPath)
	if stage.rc != 0 {
		t.Fatalf("product stage failed: %+v", stage)
	}
	if after, _ := store.Load(key, ""); after.Historical.Revision != before.Historical.Revision || rt.effects != 0 {
		t.Fatal("stage wrote the journal or started execution")
	}

	// The target answers the agent transaction subject with the production endpoint.
	endpoint := agentupgrade.New(store, rt, rt.registry)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	sub, err := nc.Subscribe(proto.SubjCmdBy("lab", ctl.PublicKey, "n1", "upgrade-tx"), func(m *nats.Msg) {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second) // ctx-root: test target responder
		defer cancel()
		body, _ := upgradewire.EncodeResponse(upgradewire.Handle(ctx, endpoint, m.Data, upgradewire.Access{Key: key, ActorFP: ctl.Fingerprint}))
		_ = m.Respond(body)
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	tx := grant.Handle.TxID
	for attempt := 0; attempt < 2; attempt++ {
		resume := runTransactionCLI(t, bin, home, "node", "upgrade", "n1", "--tx-id", tx, "--artifact-receipt", receiptPath, "--home", home, "--nats-url", url)
		if resume.rc != 0 || rt.effects != 1 {
			t.Fatalf("resume %d: rc=%d effects=%d (a retry must be a query of the same execution): %+v", attempt, resume.rc, rt.effects, resume)
		}
	}
	tamper := runTransactionCLI(t, bin, home, "node", "upgrade", "n1", "--tx-id", strings.Repeat("9", 32), "--artifact-receipt", receiptPath, "--home", home, "--nats-url", url)
	if tamper.rc != 64 || rt.effects != 1 {
		t.Fatalf("a receipt started another transaction ID: %+v", tamper)
	}
	status := runTransactionCLI(t, bin, home, "node", "upgrade-status", "n1", "--tx-id", tx, "--json", "--home", home, "--nats-url", url)
	if status.rc != 0 || !strings.Contains(status.stdout, `"effective_sha":"`+targetCaps.SHA256+`"`) || !strings.Contains(status.stdout, `"phase":"completed"`) {
		t.Fatalf("status did not show the completed original transaction: %+v", status)
	}

	// Ordinary cold boot of the registered canonical entry follows the durable selector.
	stopIssuer()
	cold, output, stopCold := startColdAgent(t, canonical, home)
	defer stopCold()
	text := output()
	exec := regexp.MustCompile(`upgrade boot: exec selected image.*pid=(\d+)`).FindStringSubmatch(text)
	running := regexp.MustCompile(`upgrade boot: running selected image"? effective_sha=([0-9a-f]{64}) pid=(\d+)`).FindStringSubmatch(text)
	if exec == nil || running == nil || running[1] != targetCaps.SHA256 || exec[1] != fmt.Sprint(cold.Process.Pid) || running[2] != exec[1] {
		t.Fatalf("cold boot did not exec the effective selector in place (pid %d, exec %v, running %v):\n%s", cold.Process.Pid, exec, running, text)
	}
	if sha, err := upgradeartifact.FileSHA(fmt.Sprintf("/proc/%d/exe", cold.Process.Pid)); err != nil || sha != targetCaps.SHA256 {
		t.Fatalf("cold-booted process runs %s, want the selected %s (%v)", sha, targetCaps.SHA256, err)
	}
	if canonicalSHA, _ := upgradeartifact.FileSHA(canonical); canonicalSHA != sourceCaps.SHA256 {
		t.Fatal("the readonly canonical entry was replaced")
	}
}
