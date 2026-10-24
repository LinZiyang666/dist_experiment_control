package upgradecli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agentimage"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradeartifact"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

type stageFixture struct {
	port     *StagePort
	grant    upgradetx.Grant
	opts     StageOptions
	control  string
	issuer   int
	probes   atomic.Int64
	targetOK agentimage.Capabilities
}

// newStageFixture registers an agent, starts a live supervised issuer whose
// running bytes are the grant's source, and writes the grant file. The issuer
// is /bin/sleep: only its kernel identity and /proc exe digest matter here.
func newStageFixture(t *testing.T) *stageFixture {
	t.Helper()
	role, key, _ := bootFixture(t)
	if err := os.Mkdir(filepath.Join(role.ControlDir, "artifacts"), 0o755); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	issuer := exec.CommandContext(ctx, "/bin/sleep", "300")
	wait, err := testharness.StartProcess(issuer)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	t.Cleanup(func() { cancel(); _ = issuer.Process.Kill(); _ = wait() })
	pid := issuer.Process.Pid
	owner, err := ProcessOwner(pid)
	if err != nil {
		t.Fatal(err)
	}
	owner.InstanceID = strings.Repeat("e", 32)
	source, err := upgradeartifact.FileSHA(fmt.Sprintf("/proc/%d/exe", pid))
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	candidate := filepath.Join(dir, "candidate")
	if err := os.WriteFile(candidate, []byte("candidate tether "+dir), 0o755); err != nil {
		t.Fatal(err)
	}
	target, err := upgradeartifact.FileSHA(candidate)
	if err != nil {
		t.Fatal(err)
	}
	f := &stageFixture{control: role.ControlDir, issuer: pid,
		targetOK: agentimage.Capabilities{ABI: 1, SHA256: target, ReadEpochs: []uint64{1}, Domains: []string{"exec-v1", "pty-v1"}}}
	f.grant = upgradetx.Grant{Handle: upgradetx.Handle{Key: key, TxID: strings.Repeat("a", 32), CutID: strings.Repeat("b", 32), Generation: 2, Revision: 1},
		Issuer: owner, ActorFP: "SHA256:actor", ClientNonce: strings.Repeat("c", 32), Mode: upgradetx.ModeHot, SourceRelease: "v1", TargetRelease: "v2",
		SourceSHA: source, TargetSHA: target, ScopeDigest: strings.Repeat("d", 64),
		CapabilityDigest: CapabilityDigest(upgradetx.ComponentAgent, f.targetOK), ExpiresAt: time.Now().Add(time.Minute).UnixNano()}
	f.opts = StageOptions{Grant: filepath.Join(dir, "grant.json"), Source: fmt.Sprintf("/proc/%d/exe", pid), Candidate: candidate, ReceiptOut: filepath.Join(dir, "receipt.json")}
	if err := writeJSONFile(f.opts.Grant, f.grant); err != nil {
		t.Fatal(err)
	}
	f.port = &StagePort{ControlDir: role.ControlDir, Probe: f.probe}
	return f
}

// probe stands in for the isolated version probe: the source reports the
// grant's source release and the candidate its exact reader declarations.
func (f *stageFixture) probe(path string) (string, agentimage.Capabilities, error) {
	f.probes.Add(1)
	sha, err := upgradeartifact.FileSHA(path)
	if err != nil {
		return "", agentimage.Capabilities{}, err
	}
	if sha == f.grant.SourceSHA {
		return "v1", agentimage.Capabilities{ABI: 1, SHA256: sha}, nil
	}
	caps := f.targetOK
	caps.SHA256 = sha
	return "v2", caps, nil
}

func (f *stageFixture) stage(t *testing.T) (upgradeartifact.Receipt, error) {
	t.Helper()
	ctx := upgradeartifact.WithPublicationPort(t.Context(), f.port)
	return Stage(ctx, f.opts, upgradeartifact.Publish)
}

// The production stage port publishes only for a registered component whose
// live issuer still runs the grant's source bytes, under the domain
// reservation, with the candidate's reader declarations matching the grant.
// It writes no journal and starts nothing; the receipt alone binds the grant.
// origin: hot-update S8 plan §5.2 / LQ1 / M16 / §11.2 P19
func TestStagePortPublishesOnlyForTheLiveIssuer(t *testing.T) {
	f := newStageFixture(t)
	receipt, err := f.stage(t)
	if err != nil {
		t.Fatal(err)
	}
	if err := upgradeartifact.Verify(receipt, f.grant); err != nil {
		t.Fatalf("published receipt does not verify: %v", err)
	}
	if filepath.Dir(receipt.TargetPath) != filepath.Join(f.control, "artifacts") || filepath.Base(receipt.TargetPath) != f.grant.TargetSHA || f.probes.Load() < 2 {
		t.Fatalf("publication escaped the registered artifact store or skipped the probes: %+v probes=%d", receipt, f.probes.Load())
	}
	if entries, _ := filepath.Glob(filepath.Join(f.control, "*", "*", "upgrade-state.json")); len(entries) != 0 {
		t.Fatalf("stage wrote a daemon journal: %v", entries)
	}
	for name, mutate := range map[string]func(*stageFixture){
		"expired":      func(f *stageFixture) { f.grant.ExpiresAt = time.Now().Add(-time.Second).UnixNano() },
		"issuer_ticks": func(f *stageFixture) { f.grant.Issuer.StartTicks++ },
		"issuer_boot":  func(f *stageFixture) { f.grant.Issuer.BootID = "another-boot" },
		"source_sha":   func(f *stageFixture) { f.grant.SourceSHA = strings.Repeat("0", 64) },
		// Bytes matching the grant are not enough: they must be what the issuer
		// is running now, or the recorded rollback target would be fiction.
		"source_not_running": func(f *stageFixture) {
			f.opts.Source = filepath.Join(filepath.Dir(f.opts.Candidate), "not-running")
			if err := os.WriteFile(f.opts.Source, []byte("bytes the issuer is not running"), 0o755); err != nil {
				t.Fatal(err)
			}
			sha, err := upgradeartifact.FileSHA(f.opts.Source)
			if err != nil {
				t.Fatal(err)
			}
			f.grant.SourceSHA = sha
		},
		"unregistered":  func(f *stageFixture) { f.grant.Handle.Key.ConfiguredNID = "someone-else" },
		"capabilities":  func(f *stageFixture) { f.targetOK.Domains = append(f.targetOK.Domains, "flow-v1") },
		"release":       func(f *stageFixture) { f.grant.TargetRelease = "v3" },
		"other_control": func(f *stageFixture) { f.port.ControlDir = t.TempDir() },
	} {
		t.Run(name, func(t *testing.T) {
			f := newStageFixture(t)
			mutate(f)
			if err := writeJSONFile(f.opts.Grant, f.grant); err != nil {
				t.Fatal(err)
			}
			if _, err := f.stage(t); err == nil || ExitCode(err) == 0 {
				t.Fatalf("%s: stage published without its authority fact", name)
			}
			if _, err := os.Lstat(f.opts.ReceiptOut); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("%s: a refused stage left a receipt", name)
			}
		})
	}
	other := newStageFixture(t)
	other.port.grant = f.grant
	if _, err := other.stage(t); err == nil {
		t.Fatal("one stage port published a second grant")
	}
}

// Five complete stage lifetimes leave no goroutine or descriptor behind:
// the reservation, both image readers and the issuer probe are all closed.
// origin: hot-update S8 plan §5.2 / §11.2 P19 (direct five-round leak shape)
func TestStagePortLifecycleNoLeaks(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseStagePort(t)
	}
	testharness.AssertNoGoroutineLeak(t, "stage port x5", g)
	testharness.AssertNoFDLeak(t, "stage port x5", fd, 0)
}

func exerciseStagePort(t *testing.T) {
	t.Helper()
	t.Run("round", func(t *testing.T) {
		f := newStageFixture(t)
		if _, err := f.stage(t); err != nil {
			t.Fatal(err)
		}
	})
}
