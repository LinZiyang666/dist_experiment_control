//go:build linux && hotupdate_integration

package hotupdate_test

// The live arm consumes P19's supervised host and P00 profiles for five private
// hermetic stacks. The host must bind fullstack_soak.SoakAssembly to real product
// processes, custody readers and fault operations. Controls below only exercise
// rejection predicates; neither they nor missing supply sign a product soak.

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/test/fullstackharness"
	"github.com/LinZiyang666/tether/test/hotupdate/acceptance"
	"github.com/LinZiyang666/tether/test/hotupdate/acceptance/contract"
	"golang.org/x/sys/unix"
)

const fullstackSoakCycles = 6

type fullstackSoakSupply struct {
	host, profilePath string
	profile           contract.Profile
	survivors         []contract.Key
}

// Each round includes startup, baseline, six real effect/settle cycles, final
// live census, natural drain, Stop and supervisor Join. No upgrade may reset a
// survivor heap. Cost target: five parallel rounds <=420s; matrix ceiling 600s.
// origin: hot-update S10 plan §4.3 / §10.2 P26 / §11.2
func TestFullstackSoakRejectsOneLeakPerCycle(t *testing.T) {
	t.Run("contracts", fullstackSoakControls)
	var supply []fullstackSoakSupply
	if !t.Run("supply", func(t *testing.T) { supply = fullstackLoadSoakSupply(t) }) {
		return
	}
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	live, started := false, time.Now()
	t.Run("rounds", func(t *testing.T) {
		live = true
		for round := 0; round < 5; round++ {
			t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) {
				t.Parallel()
				exerciseFixedImageCycle(t, supply[round])
			})
		}
	})
	if !live {
		return // An explicitly selected controls arm is not a live receipt.
	}
	testharness.AssertNoGoroutineLeak(t, "fullstack fixed-image soak parent", g)
	testharness.AssertNoFDLeak(t, "fullstack fixed-image soak parent", fd, 0)
	t.Logf("parent five-round lifecycle including supervisor join=%s", time.Since(started))
}

// Supply is explicit, prebuilt and inert. The manifest is an execution input,
// never an argv read from product evidence. Each entry has profile (P00 profile
// path) and survivors (two P00 Keys); SIM_FULLSTACK_HOST is P19's host artifact.
// A missing host is setup-red, and cannot fall back to tether from PATH.
func fullstackLoadSoakSupply(t *testing.T) []fullstackSoakSupply {
	t.Helper()
	host, input := os.Getenv("SIM_FULLSTACK_HOST"), os.Getenv("SIM_FULLSTACK_SOAK_INPUT")
	if !filepath.IsAbs(host) || !filepath.IsAbs(input) {
		t.Fatal("SETUP-RED2: NOT_RUN pending S10 P25 test/hotupdate/assemblycmd and P26 five sealed hermetic K soak profiles (SIM_FULLSTACK_HOST/SIM_FULLSTACK_SOAK_INPUT)")
	}
	info, err := os.Stat(host)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&0111 == 0 {
		t.Fatal("SETUP-RED2: real soak host is not an executable artifact")
	}
	body, err := os.ReadFile(input)
	fullstackSoakMust(t, err)
	var entries []struct {
		Profile   string            `json:"profile"`
		Survivors []json.RawMessage `json:"survivors"`
	}
	decoder := json.NewDecoder(strings.NewReader(string(body)))
	decoder.DisallowUnknownFields()
	fullstackSoakMust(t, decoder.Decode(&entries))
	if decoder.Decode(new(any)) != io.EOF || len(entries) != 5 {
		t.Fatal("SETUP-RED2: five independent hermetic soak supplies required")
	}
	roots, domains := make(map[string]bool), make(map[string]bool)
	var result []fullstackSoakSupply
	for _, entry := range entries {
		if !filepath.IsAbs(entry.Profile) {
			t.Fatal("SETUP-RED2: absolute profile path required")
		}
		raw, err := os.ReadFile(entry.Profile)
		fullstackSoakMust(t, err)
		var p contract.Profile
		fullstackSoakMust(t, contract.DecodeJSON("profile", raw, &p))
		if p.ID != "99-hot-update-fullstack.K" || p.Tier != "hermetic" || p.Topology != "quorum" ||
			!filepath.IsAbs(p.EvidenceRoot) || p.EvidenceRoot == "/" || roots[p.EvidenceRoot] {
			t.Fatal("SETUP-RED2: private hermetic 99.K profile required")
		}
		roots[p.EvidenceRoot] = true
		for _, k := range p.Required {
			id := k.Host + "/" + k.Domain
			if domains[id] {
				t.Fatal("SETUP-RED2: parallel fixtures share a mutable product domain")
			}
			domains[id] = true
		}
		s := fullstackSoakSupply{host: host, profilePath: entry.Profile, profile: p}
		for _, raw := range entry.Survivors {
			var k contract.Key
			fullstackSoakMust(t, contract.DecodeJSON("key", raw, &k))
			s.survivors = append(s.survivors, k)
		}
		fullstackSoakMust(t, fullstackSoakPopulation(p, s.survivors))
		result = append(result, s)
	}
	return result
}

func fullstackSoakPopulation(p contract.Profile, survivors []contract.Key) error {
	roles := make(map[string]bool)
	for _, k := range survivors {
		if !slices.Contains(p.Required, k) || (k.Role != "agent" && k.Role != "broker") || roles[k.Role] {
			return errors.New("soak survivor is missing, duplicated or not a product owner")
		}
		roles[k.Role] = true
	}
	if len(survivors) != 2 || !roles["agent"] || !roles["broker"] {
		return errors.New("soak requires both a fixed agent and a fixed broker")
	}
	return nil
}

// Fixed-image checking is incremental, before any teardown. P03 remains the
// sole authority for non-FD data and legitimate lease/tombstone retention.
// origin: hot-update S10 plan §4.3 / M08E/M08P/M08A/M08B/M08D/M08X
type fullstackSoakSeries struct {
	base, last map[contract.Key]contract.Sample
	seen       map[string]contract.Sample
	failed     bool
}

func newFullstackSoakSeries() *fullstackSoakSeries {
	return &fullstackSoakSeries{base: make(map[contract.Key]contract.Sample),
		last: make(map[contract.Key]contract.Sample), seen: make(map[string]contract.Sample)}
}

func fullstackSoakImage(a, b contract.Sample) bool {
	x, y := a.Stamp, b.Stamp
	return a.ExeSHA == b.ExeSHA && x.Process == y.Process && x.Generation == y.Generation &&
		x.CutID == y.CutID && x.TxID == y.TxID && x.Direction == y.Direction && x.ScopeDigest == y.ScopeDigest
}

func fullstackSoakFDs(s contract.Sample) map[contract.Resource]int {
	bag := make(map[contract.Resource]int)
	for _, r := range s.Resources {
		if r.FD >= 0 {
			// Observation fields advance; ownership and OFD identity do not.
			r.Owner.QueryNonce, r.Owner.Sequence, r.Owner.Revision, r.Owner.AtBootNS = "", 0, 0, 0
			bag[r]++
		}
	}
	return bag
}

func (s *fullstackSoakSeries) observe(e contract.Evidence, survivors []contract.Key, nonce string) (err error) {
	if s.failed {
		return errors.New("soak series is poisoned; resetting the baseline is forbidden")
	}
	defer func() { s.failed = s.failed || err != nil }()
	if err := fullstackSoakPopulation(e.Profile, survivors); err != nil {
		return err
	}
	latest := make(map[contract.Key]contract.Sample)
	for _, sample := range e.Samples {
		k, stamp := sample.Stamp.Process.Key, sample.Stamp
		if !slices.Contains(survivors, k) {
			continue
		}
		if !sample.Complete || sample.Goroutines <= 0 || sample.FDs < 0 || sample.ExeSHA == (contract.Digest{}) ||
			stamp.Process.PID <= 0 || stamp.Process.StartTicks == 0 || stamp.Process.BootID == "" ||
			stamp.Process.PIDNS == "" || stamp.Process.InstanceID == "" || stamp.CutID == "" ||
			stamp.TxID == "" || stamp.ScopeDigest == "" || stamp.Generation == 0 || stamp.Sequence == 0 ||
			stamp.Revision == 0 || stamp.AtBootNS <= 0 || stamp.QueryNonce == "" {
			return errors.New("soak sample lacks live product identity or counters")
		}
		if base, ok := s.base[k]; ok && !fullstackSoakImage(base, sample) {
			return errors.New("soak survivor image changed")
		}
		id := fmt.Sprintf("%v/%d", k, stamp.Sequence)
		if prior, ok := s.seen[id]; ok {
			if !reflect.DeepEqual(prior, sample) {
				return errors.New("soak historical observation changed")
			}
			continue
		}
		if last, ok := s.last[k]; ok && (stamp.Sequence <= last.Stamp.Sequence ||
			stamp.AtBootNS <= last.Stamp.AtBootNS || stamp.Revision < last.Stamp.Revision || stamp.QueryNonce == last.Stamp.QueryNonce) {
			return errors.New("soak observation replay")
		}
		// Preserve sealed values even if a caller reuses its decode buffers.
		sample.Resources = slices.Clone(sample.Resources)
		sample.Consumers, sample.Temps = slices.Clone(sample.Consumers), slices.Clone(sample.Temps)
		sample.Locks, sample.OldImages = slices.Clone(sample.Locks), slices.Clone(sample.OldImages)
		s.seen[id], s.last[k], latest[k] = sample, sample, sample
	}
	if len(latest) != len(survivors) {
		return errors.New("soak sample is missing a fresh survivor")
	}
	for k, sample := range latest {
		if nonce == "" || sample.Stamp.QueryNonce != nonce {
			return errors.New("soak sample did not answer its challenge")
		}
		if len(sample.Consumers) != 0 || len(sample.Temps) != 0 || len(sample.OldImages) != 0 {
			return errors.New("soak retains a consumer, temp or old image")
		}
		if _, ok := s.base[k]; !ok {
			s.base[k] = sample
		}
		base := s.base[k]
		if sample.Goroutines > base.Goroutines+testharness.GoroutineLeakTolerance {
			return errors.New("soak same-image goroutine floor exceeds baseline +2")
		}
		if sample.FDs != base.FDs || !reflect.DeepEqual(fullstackSoakFDs(base), fullstackSoakFDs(sample)) ||
			!fullstackSoakSetEqual(sample.Locks, base.Locks) {
			return errors.New("soak exact owned resource census changed")
		}
	}
	return nil
}

func fullstackSoakSetEqual(a, b []string) bool {
	a, b = slices.Clone(a), slices.Clone(b)
	slices.Sort(a)
	slices.Sort(b)
	return slices.Equal(a, b)
}

func exerciseFixedImageCycle(t *testing.T, supply fullstackSoakSupply) {
	t.Helper()
	started := time.Now()
	ctx, cancel := context.WithTimeout(t.Context(), 400*time.Second)
	defer cancel()
	p := supply.profile
	fullstackSoakMust(t, os.MkdirAll(p.EvidenceRoot, 0700))
	home := filepath.Join(p.EvidenceRoot, "soak-host-home")
	fullstackSoakMust(t, os.Mkdir(home, 0700))
	f, err := fullstackharness.NewFixture(p)
	fullstackSoakMust(t, err)
	d, err := fullstackharness.NewDriver(p)
	fullstackSoakMust(t, err)
	stderr, err := os.OpenFile(filepath.Join(p.EvidenceRoot, "soak-host.stderr"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	fullstackSoakMust(t, err)
	defer func() { fullstackSoakMust(t, stderr.Close()) }()
	cmd := exec.Command(supply.host, "--profile", supply.profilePath, "--arm", "K")
	cmd.Dir, cmd.Stderr = p.EvidenceRoot, stderr
	cmd.Env = []string{"HOME=" + home, "PATH=/usr/bin:/bin", "LANG=C", "LC_ALL=C", "GORACE=atexit_sleep_ms=0"}
	fullstackSoakMust(t, fullstackharness.BindFixtureCommand(f, cmd, d))
	defer func() {
		// Cancellation cannot bypass Stop/Join or turn a reclaimed orphan green.
		cleanup, end := context.WithTimeout(context.WithoutCancel(t.Context()), 20*time.Second)
		defer end()
		stopErr := f.Stop(cleanup)
		joinErr := f.Join(cleanup)
		if err := errors.Join(stopErr, joinErr); err != nil {
			t.Errorf("soak native cleanup failure: %v", err)
		}
	}()
	_, err = f.Start(ctx, p)
	fullstackSoakMust(t, err)
	t.Logf("startup=%s", time.Since(started))
	series := newFullstackSoakSeries()
	for sample := 0; sample < fullstackSoakCycles+2; sample++ {
		before := time.Now()
		nonce := fullstackSoakNonce(t)
		e, err := f.Sample(ctx, nonce)
		fullstackSoakMust(t, err)
		if !reflect.DeepEqual(e.Profile, p) {
			t.Fatal("soak host changed the sealed profile")
		}
		fullstackSoakMust(t, series.observe(e, supply.survivors, nonce))
		for _, k := range supply.survivors {
			fullstackSoakMust(t, fullstackSoakLiveOwner(series.last[k]))
			fullstackSoakMust(t, fullstackSoakRaw(p.EvidenceRoot, series.last[k].Raw))
		}
		t.Logf("sample=%d effect/IO/settle/live-read=%s", sample, time.Since(before))
	}
	// The eighth sample is still before natural drain and fixture teardown.
	// SoakAssembly records the exact same sequence and rejects early Stop.
	drainStarted := time.Now()
	_, err = f.Drain(ctx, fullstackSoakNonce(t))
	fullstackSoakMust(t, err)
	fullstackSoakPython(t, "--check-archive", filepath.Join(p.EvidenceRoot, "fullstack-soak-series.json"))
	// P19's finish owner publishes the final P00 archive used by its independent
	// judge. Read it here too: Python fault effects cannot replace P03 retention.
	raw, err := os.ReadFile(filepath.Join(p.EvidenceRoot, "evidence.json"))
	if err != nil {
		t.Fatalf("SETUP-RED2: NOT_RUN pending P19/P25 final P00 evidence archive: %v", err)
	}
	var final contract.Evidence
	fullstackSoakMust(t, contract.DecodeJSON("evidence", raw, &final))
	if !reflect.DeepEqual(final.Profile, p) {
		t.Fatal("final soak evidence changed the sealed profile")
	}
	last := make(map[contract.Key]contract.Sample)
	for _, sample := range final.Samples {
		if base, ok := series.base[sample.Stamp.Process.Key]; ok && !fullstackSoakImage(base, sample) {
			t.Fatal("survivor image changed during natural drain")
		}
		last[sample.Stamp.Process.Key] = sample
	}
	for _, k := range supply.survivors {
		before, after := series.last[k], last[k]
		if after.Stamp.Sequence <= before.Stamp.Sequence || after.Stamp.AtBootNS <= before.Stamp.AtBootNS ||
			after.Stamp.QueryNonce == before.Stamp.QueryNonce {
			t.Fatal("natural drain lacks a fresh live survivor census")
		}
		fullstackSoakMust(t, fullstackSoakLiveOwner(after))
	}
	result, err := acceptance.JudgeResources(fullstackSoakResourceEvidence(final, supply.survivors))
	fullstackSoakMust(t, err)
	if result.RC != 0 || result.Verdict != "GREEN" || len(result.Failures) != 0 || len(result.Missing) != 0 {
		t.Fatalf("live soak resource judge: %+v", result)
	}
	for _, ref := range result.Inputs {
		fullstackSoakMust(t, fullstackSoakRaw(p.EvidenceRoot, ref))
	}
	for _, k := range supply.survivors {
		fullstackSoakMust(t, fullstackSoakLiveOwner(series.last[k]))
	}
	t.Logf("final natural drain/judge=%s; six-cycle live duration=%s; cuts=0", time.Since(drainStarted), time.Since(started))
}

// Victims deliberately restart; applying P03's same-PID image transition rule
// to them would contradict 99.K. Keep the immutable fleet archive unchanged,
// and judge only the predeclared survivors plus the independent harness axis.
func fullstackSoakResourceEvidence(e contract.Evidence, survivors []contract.Key) contract.Evidence {
	out := e
	out.Profile.Required = slices.Clone(survivors)
	wanted := func(k contract.Key) bool { return k.Role == "fixture" || slices.Contains(survivors, k) }
	out.Samples, out.Intervals, out.Views = nil, nil, nil
	for _, s := range e.Samples {
		if wanted(s.Stamp.Process.Key) {
			out.Samples = append(out.Samples, s)
		}
	}
	for _, event := range e.Intervals {
		if wanted(event.Stamp.Process.Key) {
			out.Intervals = append(out.Intervals, event)
		}
	}
	for _, view := range e.Views {
		if wanted(view.Stamp.Process.Key) {
			out.Views = append(out.Views, view)
		}
	}
	return out
}

// This extra local hermetic check is independent of the host's self-report.
// P04/P16 retain full cut/OFD witnesses; a pidfd prevents a dead or zombie owner
// from passing the last sample merely because /proc still has its executable.
func fullstackSoakLiveOwner(s contract.Sample) (err error) {
	p := s.Stamp.Process
	fd, err := unix.PidfdOpen(p.PID, 0)
	if err != nil {
		return err
	}
	// origin: hot-update S10 lint L3a / errcheck
	defer func() { err = errors.Join(err, unix.Close(fd)) }()
	before, err := fullstackharness.ReadKernelProcess(p.Key, p.PID, p.InstanceID)
	if err != nil || before != p {
		return errors.Join(err, errors.New("soak survivor kernel identity changed"))
	}
	file, err := os.Open(fmt.Sprintf("/proc/%d/exe", p.PID))
	if err != nil {
		return err
	}
	hash := sha256.New()
	_, readErr := io.Copy(hash, file)
	if err := errors.Join(readErr, file.Close()); err != nil {
		return err
	}
	after, err := fullstackharness.ReadKernelProcess(p.Key, p.PID, p.InstanceID)
	poll := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}
	_, pollErr := unix.Poll(poll, 0)
	if err != nil || pollErr != nil || after != p || poll[0].Revents != 0 ||
		hex.EncodeToString(hash.Sum(nil)) != hex.EncodeToString(s.ExeSHA[:]) {
		return errors.Join(err, pollErr, errors.New("soak survivor died or executing SHA changed before cleanup"))
	}
	return nil
}

func fullstackSoakRaw(root string, ref contract.RawRef) (err error) {
	name, err := filepath.Rel(root, ref.Path)
	if err != nil || !filepath.IsAbs(ref.Path) || ref.Bytes <= 0 || ref.Bytes > 16<<20 || ref.Schema == "" {
		return errors.New("invalid soak raw reference")
	}
	dir, err := os.OpenRoot(root)
	if err != nil {
		return err
	}
	// origin: hot-update S10 lint L3a / errcheck
	defer func() { err = errors.Join(err, dir.Close()) }()
	file, err := dir.Open(name)
	if err != nil {
		return err
	}
	// origin: hot-update S10 lint L3a / errcheck
	defer func() { err = errors.Join(err, file.Close()) }()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return errors.New("soak raw is not a regular file")
	}
	body, err := io.ReadAll(io.LimitReader(file, ref.Bytes+1))
	if err != nil || int64(len(body)) != ref.Bytes || sha256.Sum256(body) != ref.SHA {
		return errors.Join(err, errors.New("soak immutable raw digest changed"))
	}
	return nil
}

func fullstackSoakNonce(t *testing.T) string {
	t.Helper()
	var value [16]byte
	_, err := rand.Read(value[:])
	fullstackSoakMust(t, err)
	return hex.EncodeToString(value[:])
}

func fullstackSoakPython(t *testing.T, args ...string) {
	t.Helper()
	fullstackSoakPythonStatus(t, 0, args...)
}

func fullstackSoakPythonStatus(t *testing.T, want int, args ...string) {
	t.Helper()
	script, err := filepath.Abs("../simcluster/drills/lib/fullstack_soak.py")
	fullstackSoakMust(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "/usr/bin/python3", append([]string{"-I", "-B", script}, args...)...)
	cmd.Dir = t.TempDir()
	cmd.Env = []string{"HOME=" + cmd.Dir, "PATH=/usr/bin:/bin", "LANG=C", "LC_ALL=C"}
	output, err := fullstackharness.CombinedOutput(cmd)
	if rc := fullstackharness.ProcessExitCode(err); rc != want {
		t.Fatalf("soak read-only native status=%d, want=%d: %v\n%s", rc, want, err, output)
	}
	t.Logf("%s", output)
}

func fullstackSoakMust(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// origin: hot-update S10 plan §10.2 P26 / fixed SHA, +1/cycle, cleanup order
func fullstackSoakControls(t *testing.T) {
	t.Run("python-lifecycle", func(t *testing.T) { fullstackSoakPython(t, "--selftest") })
	t.Run("missing-archive", func(t *testing.T) {
		fullstackSoakPythonStatus(t, 2, "--check-archive", filepath.Join(t.TempDir(), "absent-series.json"))
	})
	t.Run("independent-live-owner", func(t *testing.T) {
		// Kernel-reader control only: this is the parent test process, and is
		// never included in a product resource or deployment success receipt.
		k := contract.Key{Role: "fixture", UID: uint32(os.Getuid())}
		p, err := fullstackharness.ReadKernelProcess(k, os.Getpid(), "kernel-control")
		fullstackSoakMust(t, err)
		body, err := os.ReadFile("/proc/self/exe")
		fullstackSoakMust(t, err)
		s := contract.Sample{Stamp: contract.Stamp{Process: p}, ExeSHA: sha256.Sum256(body)}
		fullstackSoakMust(t, fullstackSoakLiveOwner(s))
		s.ExeSHA[0]++
		if fullstackSoakLiveOwner(s) == nil {
			t.Fatal("independent executable check accepted a false SHA")
		}
		s.ExeSHA[0]--
		s.Stamp.Process.StartTicks++
		if fullstackSoakLiveOwner(s) == nil {
			t.Fatal("independent process check accepted PID reuse")
		}
	})
	t.Run("immutable-raw", func(t *testing.T) {
		root := t.TempDir()
		body := []byte("immutable public resource receipt")
		ref := contract.RawRef{Path: filepath.Join(root, "sample"), SHA: sha256.Sum256(body), Bytes: int64(len(body)), Schema: "sample"}
		fullstackSoakMust(t, os.WriteFile(ref.Path, body, 0600))
		fullstackSoakMust(t, fullstackSoakRaw(root, ref))
		ref.SHA[0]++
		if fullstackSoakRaw(root, ref) == nil {
			t.Fatal("mutated raw accepted")
		}
		ref.SHA[0]--
		if fullstackSoakRaw(filepath.Join(root, "absent-root"), ref) == nil {
			t.Fatal("raw outside the evidence root accepted")
		}
	})
	keys := []contract.Key{{Role: "agent", Host: "local", Unit: "agent", Domain: "a"},
		{Role: "broker", Host: "local", Unit: "broker", Domain: "b"}}
	makeEvidence := func(sequence uint64) contract.Evidence {
		e := contract.Evidence{Profile: contract.Profile{Required: keys}}
		for i, k := range keys {
			s := contract.Sample{Stamp: contract.Stamp{Process: contract.Process{Key: k, PID: i + 100,
				StartTicks: 1, BootID: "boot", PIDNS: "ns", InstanceID: "instance"}, TxID: "tx", CutID: "cut",
				ScopeDigest: "scope", Generation: 1, Revision: 1, Sequence: sequence,
				AtBootNS: int64(sequence), QueryNonce: fmt.Sprint(sequence), Direction: "forward"},
				ExeSHA: contract.Digest{1}, Goroutines: 10, FDs: 1, Complete: true}
			s.Resources = []contract.Resource{{ID: "original", Kind: "raw-socket", Domain: "flow", FD: 3,
				Flags: 1, OFD: "original-OFD", Owner: s.Stamp, State: "live"}}
			e.Samples = append(e.Samples, s)
		}
		return e
	}
	t.Run("survivor-resource-scope", func(t *testing.T) {
		e := makeEvidence(1)
		victim, harness := e.Samples[0], e.Samples[1]
		victim.Stamp.Process.Key.Unit = "victim"
		harness.Stamp.Process.Key.Role = "fixture"
		e.Samples = append(e.Samples, victim, harness)
		e.Profile.Required = append(slices.Clone(keys), victim.Stamp.Process.Key)
		for _, s := range e.Samples {
			e.Intervals = append(e.Intervals, contract.Interval{Stamp: s.Stamp})
			e.Views = append(e.Views, contract.View{Stamp: s.Stamp})
		}
		out := fullstackSoakResourceEvidence(e, keys)
		if len(out.Samples) != 3 || len(out.Intervals) != 3 || len(out.Views) != 3 ||
			!slices.Equal(out.Profile.Required, keys) || len(e.Profile.Required) != 3 || len(e.Samples) != 4 ||
			out.Samples[2].Stamp.Process.Key.Role != "fixture" {
			t.Fatal("survivor projection lost the harness axis or mutated the fleet archive")
		}
		if fullstackSoakPopulation(e.Profile, keys[:1]) == nil || fullstackSoakPopulation(e.Profile, []contract.Key{keys[0], keys[0]}) == nil {
			t.Fatal("missing or duplicated survivor accepted")
		}
	})
	t.Run("six-fixed-image-rounds", func(t *testing.T) {
		series := newFullstackSoakSeries()
		for sequence := uint64(1); sequence <= 8; sequence++ {
			fullstackSoakMust(t, series.observe(makeEvidence(sequence), keys, fmt.Sprint(sequence)))
		}
	})
	for _, tc := range []struct {
		name string
		edit func(*contract.Sample, uint64)
	}{
		{"fixed-sha", func(s *contract.Sample, _ uint64) { s.ExeSHA[0]++ }},
		{"fixed-pid", func(s *contract.Sample, _ uint64) { s.Stamp.Process.PID++ }},
		{"fixed-ticks", func(s *contract.Sample, _ uint64) { s.Stamp.Process.StartTicks++ }},
		{"fixed-cut", func(s *contract.Sample, _ uint64) { s.Stamp.CutID += "replacement" }},
		{"M08B-one-goroutine", func(s *contract.Sample, n uint64) { s.Goroutines += int64(n - 1) }},
		{"M08D-one-fd", func(s *contract.Sample, n uint64) { s.FDs += int64(n - 1) }},
		{"balanced-fd-substitution", func(s *contract.Sample, _ uint64) { s.Resources[0].OFD = "replacement" }},
		{"M08X-consumer", func(s *contract.Sample, _ uint64) { s.Consumers = []string{"leak"} }},
		{"temp", func(s *contract.Sample, _ uint64) { s.Temps = []string{"leak"} }},
		{"unknown-census", func(s *contract.Sample, _ uint64) { s.Complete = false }},
		{"replayed-challenge", func(s *contract.Sample, _ uint64) { s.Stamp.QueryNonce = "1" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for role := range keys {
				series := newFullstackSoakSeries()
				fullstackSoakMust(t, series.observe(makeEvidence(1), keys, "1"))
				var killed bool
				for n := uint64(2); n <= 7; n++ {
					e := makeEvidence(n)
					tc.edit(&e.Samples[role], n)
					if series.observe(e, keys, fmt.Sprint(n)) != nil {
						killed = true
						break
					}
				}
				if !killed || series.observe(makeEvidence(9), keys, "9") == nil {
					t.Fatal("defect escaped or was erased by resetting the floor")
				}
			}
		})
	}
}
