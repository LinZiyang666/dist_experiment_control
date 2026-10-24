//go:build linux && hotupdate_integration

package hotupdate_test

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"debug/buildinfo"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/test/fullstackharness"
	c "github.com/LinZiyang666/tether/test/hotupdate/acceptance/contract"
)

// Supply is explicit and local. The assembly-host must be a workspace-built
// Go executable implementing P19's line protocol, with hermetic workload ports.
// No deployment entry, executable from PATH, or command decoded from evidence
// is a fallback. Each round has distinct profile/input/output directories.
// origin: hot-update S10 plan §5.1 / §10.2 P25 / lead L15–L17
var fullstackLocalSupply = flag.String("fullstack-local-supply", "", "absolute private P25 hermetic assembly supply directory")

type fullstackCLIInput struct {
	Key             c.Key
	StepID, Action  string
	BindingID, Home string
	StampRoot       string
	BinarySHA       c.Digest
	Values          map[string]string
}

type fullstackRollout struct {
	profile c.Profile
	fixture c.Fixture
	stderr  *os.File
}

type fullstackRolloutCapture struct {
	Evidence c.Evidence
	Work     []fullstackMixedWork
}

func newFullstackRollout(t *testing.T, round int) *fullstackRollout {
	t.Helper()
	if !filepath.IsAbs(*fullstackLocalSupply) {
		t.Fatal("SETUP-RED: NOT_RUN pending S10 P25 test/hotupdate/assemblycmd and hermetic workload bindings; S10 P18 native stage/infra/query-nonce adapters (supply via -fullstack-local-supply)")
	}
	root := filepath.Join(*fullstackLocalSupply, fmt.Sprintf("round-%d", round))
	var p c.Profile
	transferMust(t, fullstackReadContract(filepath.Join(root, "profile.json"), "profile", &p))
	if p.Tier != "hermetic" || p.Topology != "single" || p.Regime != "quiet" || !filepath.IsAbs(p.EvidenceRoot) {
		t.Fatal("SETUP-RED: local mixed rollout requires an isolated hermetic single profile")
	}
	manifest, err := fullstackharness.BuildFixtureStepManifest(p)
	transferMust(t, err)
	if !reflect.DeepEqual(manifest.Steps, p.Steps) {
		t.Fatal("SETUP-RED: supplied rollout differs from P19's frozen seven-row manifest")
	}
	fixture, err := fullstackharness.NewFixture(p)
	transferMust(t, err)
	driver, err := fullstackharness.NewDriver(p)
	transferMust(t, err)
	body, err := os.ReadFile(filepath.Join(root, "commands.json"))
	transferMust(t, err)
	var commands []fullstackCLIInput
	transferMust(t, fullstackDecode(body, reflect.ValueOf(&commands).Elem()))
	if len(commands) == 0 {
		t.Fatal("SETUP-RED: product command binding set is empty")
	}
	for _, input := range commands {
		path := input.Values["{binary}"]
		transferMust(t, fullstackLocalExecutable(*fullstackLocalSupply, path, input.BinarySHA, "github.com/LinZiyang666/tether/cmd/tether"))
		command := fullstackProductCommand(fullstackQuerySupply{BindingID: input.BindingID, Values: input.Values,
			Home: input.Home, BinarySHA: input.BinarySHA, StampRoot: input.StampRoot})
		transferMust(t, fullstackharness.BindProductCLICommand(driver, input.Key, input.StepID, input.Action, command))
	}
	// Artifacts are supplied once before READY. The SHA read below authenticates
	// the local host input; P04 independently checks each running product image.
	host := filepath.Join(*fullstackLocalSupply, "assembly-host")
	digest, err := os.ReadFile(host + ".sha256")
	transferMust(t, err)
	decoded, err := hex.DecodeString(strings.TrimSpace(string(digest)))
	if err != nil || len(decoded) != 32 {
		t.Fatal("SETUP-RED: invalid assembly host digest")
	}
	transferMust(t, fullstackLocalExecutable(*fullstackLocalSupply, host, c.Digest(decoded), ""))
	transferMust(t, os.MkdirAll(p.EvidenceRoot, 0700))
	home := filepath.Join(p.EvidenceRoot, "host-home")
	transferMust(t, os.Mkdir(home, 0700))
	stderr, err := os.OpenFile(filepath.Join(p.EvidenceRoot, "assembly.stderr"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	transferMust(t, err)
	run := &fullstackRollout{profile: p, fixture: fixture, stderr: stderr}
	cmd := exec.CommandContext(t.Context(), host, "--profile", filepath.Join(root, "profile.json"), "--inputs", filepath.Join(root, "inputs.json"))
	cmd.Dir, cmd.Env, cmd.Stderr = root, flowSandboxEnvironment(home), stderr
	if err := fullstackharness.BindFixtureCommand(fixture, cmd, driver); err != nil {
		transferMust(t, errors.Join(err, stderr.Close()))
	}
	return run
}

func fullstackLocalExecutable(root, path string, want c.Digest, module string) error {
	if !filepath.IsAbs(root) || !filepath.IsAbs(path) || want == (c.Digest{}) {
		return fmt.Errorf("local executable requires an absolute supply path and digest")
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil || resolved != path || !strings.HasPrefix(path, filepath.Clean(root)+string(filepath.Separator)) {
		return fmt.Errorf("executable escapes the private local supply")
	}
	info, err := buildinfo.ReadFile(path)
	if err != nil || !strings.HasPrefix(info.Path, "github.com/LinZiyang666/tether/") || module != "" && info.Path != module {
		return fmt.Errorf("supply is not the required workspace Go executable")
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	h := sha256.New()
	_, readErr := io.Copy(h, f)
	err = errors.Join(readErr, f.Close())
	if err != nil || !slices.Equal(h.Sum(nil), want[:]) {
		return fmt.Errorf("local executable digest changed: %w", err)
	}
	return nil
}

func (r *fullstackRollout) close(t *testing.T) {
	t.Helper()
	begin := time.Now()
	ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 15*time.Second)
	defer cancel()
	stopErr := r.fixture.Stop(ctx)
	joinErr := r.fixture.Join(ctx)
	if err := errors.Join(stopErr, joinErr, r.stderr.Close()); err != nil {
		t.Errorf("fixture Stop/Join failed; original native failure retained: %v", err)
	}
	t.Logf("supervisor-join=%s", time.Since(begin))
}

func (r *fullstackRollout) capture(t *testing.T, phase string) fullstackRolloutCapture {
	t.Helper()
	nonce := fullstackRolloutNonce()
	e, err := r.fixture.Sample(t.Context(), nonce)
	transferMust(t, err)
	if !reflect.DeepEqual(e.Profile, r.profile) || len(e.Raw) == 0 {
		t.Fatal("fresh sample changed the predeclared profile or lacks raw bytes")
	}
	for _, key := range r.profile.Required {
		last, ok := fullstackLatestSample(e, key)
		if !ok || !last.Complete || last.Stamp.QueryNonce != nonce {
			t.Fatal("fresh sample omitted a required runtime or replayed its nonce")
		}
	}
	for _, ref := range e.Raw {
		transferMust(t, fullstackRolloutRaw(r.profile.EvidenceRoot, ref))
	}
	var work []fullstackMixedWork
	name := fmt.Sprintf("workload-%x.json", sha256.Sum256([]byte(nonce)))
	body, err := os.ReadFile(filepath.Join(r.profile.EvidenceRoot, name))
	transferMust(t, err)
	transferMust(t, fullstackDecode(body, reflect.ValueOf(&work).Elem()))
	if !slices.ContainsFunc(e.Raw, func(ref c.RawRef) bool {
		return ref.Path == filepath.Join(r.profile.EvidenceRoot, name) && ref.SHA == sha256.Sum256(body)
	}) {
		t.Fatal("workload checkpoint is not bound to the challenged raw archive")
	}
	t.Logf("sample=%s nonce=%s", phase, nonce)
	return fullstackRolloutCapture{e, work}
}

func (r *fullstackRollout) sealed(t *testing.T) fullstackRolloutCapture {
	t.Helper()
	var e c.Evidence
	transferMust(t, fullstackReadContract(filepath.Join(r.profile.EvidenceRoot, "evidence.json"), "evidence", &e))
	if !reflect.DeepEqual(e.Profile, r.profile) {
		t.Fatal("sealed evidence changed its profile")
	}
	for _, ref := range e.Raw {
		transferMust(t, fullstackRolloutRaw(r.profile.EvidenceRoot, ref))
	}
	body, err := os.ReadFile(filepath.Join(r.profile.EvidenceRoot, "workload-sealed.json"))
	transferMust(t, err)
	var work []fullstackMixedWork
	transferMust(t, fullstackDecode(body, reflect.ValueOf(&work).Elem()))
	if !slices.ContainsFunc(e.Raw, func(ref c.RawRef) bool {
		return ref.Path == filepath.Join(r.profile.EvidenceRoot, "workload-sealed.json") && ref.SHA == sha256.Sum256(body)
	}) {
		t.Fatal("final workload bytes are absent from the sealed archive")
	}
	return fullstackRolloutCapture{e, work}
}

func fullstackReadContract(path, schema string, target any) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	body, readErr := io.ReadAll(io.LimitReader(f, (16<<20)+1))
	if err := errors.Join(readErr, f.Close()); err != nil {
		return err
	}
	if len(body) > 16<<20 {
		return fmt.Errorf("fullstack contract exceeds archive bound")
	}
	return c.DecodeJSON(schema, body, target)
}

func fullstackRolloutRaw(root string, ref c.RawRef) error {
	if !filepath.IsAbs(ref.Path) || ref.Schema == "" || ref.Bytes <= 0 || ref.Bytes > 16<<20 || ref.SHA == (c.Digest{}) {
		return fmt.Errorf("invalid fullstack raw reference")
	}
	rel, err := filepath.Rel(root, ref.Path)
	if err != nil {
		return err
	}
	dir, err := os.OpenRoot(root)
	if err != nil {
		return err
	}
	f, err := dir.Open(rel)
	if err != nil {
		return errors.Join(err, dir.Close())
	}
	body, readErr := io.ReadAll(io.LimitReader(f, ref.Bytes+1))
	err = errors.Join(readErr, f.Close(), dir.Close())
	if err != nil || int64(len(body)) != ref.Bytes || sha256.Sum256(body) != ref.SHA {
		return fmt.Errorf("fullstack raw reference differs from original bytes: %w", err)
	}
	return nil
}

func fullstackRolloutNonce() string {
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(nonce[:])
}

func fullstackLatestSample(e c.Evidence, key c.Key) (c.Sample, bool) {
	for i := len(e.Samples) - 1; i >= 0; i-- {
		if e.Samples[i].Stamp.Process.Key == key {
			return e.Samples[i], true
		}
	}
	return c.Sample{}, false
}

// M07 must be rejected for EVERY role. Same-PID exec is required for tether;
// infra may change process, but the old owner's actual retirement is mandatory.
// origin: hot-update S10 plan §2.1 / §8 M07A/M07B/M07N/M07C
func fullstackCutIdentity(step c.Step, p c.Profile, before, after c.Evidence) error {
	a, foundA := fullstackLatestSample(before, step.Target)
	b, foundB := fullstackLatestSample(after, step.Target)
	from := slices.IndexFunc(p.Artifacts, func(a c.Artifact) bool { return a.ID == step.From && a.Role == step.Target.Role })
	to := slices.IndexFunc(p.Artifacts, func(a c.Artifact) bool { return a.ID == step.To && a.Role == step.Target.Role })
	if !foundA || !foundB || from < 0 || to < 0 || !a.Complete || !b.Complete || a.ExeSHA != p.Artifacts[from].Binary ||
		b.ExeSHA != p.Artifacts[to].Binary || a.ExeSHA == b.ExeSHA || b.Stamp.Generation != a.Stamp.Generation+1 ||
		b.Stamp.CutID == a.Stamp.CutID || b.Stamp.QueryNonce == a.Stamp.QueryNonce || b.Stamp.Direction != step.Direction {
		return fmt.Errorf("M07: disk target was not activated by the required current owner")
	}
	if step.Target.Role == "agent" || step.Target.Role == "broker" {
		if a.Stamp.Process != b.Stamp.Process {
			return fmt.Errorf("tether handoff restarted the original process")
		}
	} else {
		if a.Stamp.Process == b.Stamp.Process || len(b.OldImages) != 0 {
			return fmt.Errorf("old infra image remains responsible")
		}
		if !slices.ContainsFunc(after.Intervals, func(i c.Interval) bool {
			return i.Event == "retire" && i.Stamp.Process == a.Stamp.Process && i.AtNS >= a.Stamp.AtBootNS && i.AtNS <= b.Stamp.AtBootNS && i.Raw.Bytes > 0
		}) {
			return fmt.Errorf("infra cut lacks the actual old process retirement")
		}
	}
	return nil
}

func (r *fullstackRollout) executing(t *testing.T, step c.Step, e c.Evidence) {
	t.Helper()
	sample, ok := fullstackLatestSample(e, step.Target)
	if !ok {
		t.Fatal("executing image sample missing")
	}
	read, err := fullstackharness.ReadExecutable(t.Context(), sample.Stamp, func(ctx context.Context) (c.Stamp, error) {
		// P04 revalidates ONE challenge while its executable FD is held.
		// A second CLI query would change that nonce (and P18 correctly
		// rejects replaying a consumed query nonce). P16 owns this live cursor.
		if err := ctx.Err(); err != nil {
			return c.Stamp{}, err
		}
		key, err := c.EncodeJSON("key", step.Target)
		if err != nil {
			return c.Stamp{}, err
		}
		var current c.Stamp
		path := filepath.Join(r.profile.EvidenceRoot, fmt.Sprintf("current-%x.json", sha256.Sum256(key)))
		if err := fullstackReadContract(path, "stamp", &current); err != nil {
			return current, err
		}
		if current.Sequence <= sample.Stamp.Sequence || current.AtBootNS <= sample.Stamp.AtBootNS {
			return current, fmt.Errorf("P16 live image cursor did not advance during kernel sampling")
		}
		return current, nil
	})
	transferMust(t, err)
	defer func() {
		if err := read.Close(); err != nil {
			t.Error(err)
		}
	}()
	if read.Current.SHA != sample.ExeSHA {
		t.Fatal("M07: kernel executable disagrees with claimed executing image")
	}
}

// Synthetic controls below exercise only local predicates; none is passed to
// NewFixture or used as a live product/retirement receipt.
func fullstackControlRaw() c.RawRef {
	return c.RawRef{Path: "/control/raw", SHA: sha256.Sum256([]byte("control")), Bytes: 7, Schema: "control"}
}

func fullstackControlProfile() c.Profile {
	p := c.Profile{ID: "control", ProbeRate: 10, StartNS: 1, EndNS: 1_000_000_001}
	for i := 0; i < 4; i++ {
		p.Required = append(p.Required, c.Key{Role: "agent", Host: "local", Unit: fmt.Sprintf("unit%d", i), SID: fmt.Sprintf("sid%d", i),
			NID: fmt.Sprintf("a%d", i), Domain: fmt.Sprintf("domain%d", i), UID: uint32(i + 1000)})
	}
	return p
}

func fullstackControlWork(p c.Profile) []fullstackMixedWork {
	var rows []fullstackMixedWork
	for _, key := range p.Required {
		kinds := []string{"exec", "transfer", "tcp"}
		if key.NID == "a2" || key.NID == "a3" {
			kinds[2] = "ssh"
		}
		if key.NID == "a0" || key.NID == "a2" {
			kinds = append(kinds, "pty")
		}
		for _, kind := range kinds {
			id := key.NID + "/" + kind
			process := c.Process{Key: key, PID: len(rows) + 1, StartTicks: 20, BootID: "boot", PIDNS: "ns", InstanceID: id}
			digest := fmt.Sprintf("%x", sha256.Sum256([]byte(id)))
			rows = append(rows, fullstackMixedWork{ID: id, Kind: kind, NID: key.NID, Handle: id, Nonce: id, Cohort: "C0", Process: process, Controller: process,
				Sent: [2]uint64{10, 10}, Received: [2]uint64{10, 10}, SentDigest: [2]string{digest, digest}, ReceivedDigest: [2]string{digest, digest},
				Accepts: 1, DeadlineNS: 1000, AtNS: 1})
		}
	}
	return rows
}

func fullstackControlCut(role string) (c.Profile, c.Step, c.Evidence, c.Evidence) {
	key := c.Key{Role: role, Host: "local", Unit: role, Domain: role}
	stamp := c.Stamp{Process: c.Process{Key: key, PID: 1, StartTicks: 1}, Generation: 1, CutID: "old", QueryNonce: "before", AtBootNS: 1}
	a := c.Sample{Stamp: stamp, ExeSHA: sha256.Sum256([]byte("old")), Complete: true}
	b := a
	b.ExeSHA, b.Stamp.Generation, b.Stamp.CutID, b.Stamp.QueryNonce, b.Stamp.Direction, b.Stamp.AtBootNS = sha256.Sum256([]byte("new")), 2, "new", "after", "forward", 3
	p := c.Profile{Artifacts: []c.Artifact{{ID: "old", Role: role, Binary: a.ExeSHA}, {ID: "new", Role: role, Binary: b.ExeSHA}}}
	step := c.Step{ID: "step", Target: key, From: "old", To: "new", Direction: "forward"}
	before, after := c.Evidence{Samples: []c.Sample{a}}, c.Evidence{Samples: []c.Sample{b}}
	if role == "nats" || role == "caddy" {
		after.Samples[0].Stamp.Process.PID++
		after.Intervals = []c.Interval{{Event: "retire", Stamp: a.Stamp, AtNS: 2, Raw: fullstackControlRaw()}}
	}
	return p, step, before, after
}
