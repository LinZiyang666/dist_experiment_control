package testharness

// Release candidates are timing witnesses, while race candidates remain the
// functional witnesses. Inspect the actual executable rather than trusting a
// harness flag: copied, appended and /proc/PID/exe artifacts retain build info.
// origin: hot-update S10 lead decision D-A
import (
	"context"
	"crypto/sha256"
	"debug/buildinfo"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/debug"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// CandidateBuildModes keeps the harness race-enabled in both arms. Production
// SLOs belong only to the explicit release arm; functional coverage still runs
// the instrumented product whenever the invoking harness is instrumented.
func CandidateBuildModes() []string {
	modes := []string{"release-budget"}
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range info.Settings {
			if setting.Key == "-race" && setting.Value == "true" {
				modes = append(modes, "race-functional")
			}
		}
	}
	return modes
}

// ReleaseBudgetRun recognizes the explicit measurement arm at any subtest depth.
// Ordinary race fixtures retain their original builders and functional checks.
func ReleaseBudgetRun(t *testing.T) bool {
	return slices.Contains(strings.Split(t.Name(), "/"), "release-budget")
}

// ReleaseBudgetReceipt binds the sample to bytes and metadata from one open
// executable inode. Linker flags may be absent with Go's -trimpath setting;
// BuildReleaseCandidate records its fixed release flags separately.
type ReleaseBudgetReceipt struct {
	Path       string
	SHA256     string
	Toolchain  string
	Race       bool
	CGOEnabled string
	Trimpath   string
	LDFlags    string
	Elapsed    time.Duration
	Budget     time.Duration
}

// CheckReleaseBudget authenticates the build before comparing a duration. A
// fast race sample must fail just as surely as an over-budget release sample.
// origin: hot-update S10 lead decision D-A / RELEASE-MODE F1
func CheckReleaseBudget(path string, elapsed, budget time.Duration) (ReleaseBudgetReceipt, error) {
	r := ReleaseBudgetReceipt{Path: path, Elapsed: elapsed, Budget: budget}
	// Pin one executable inode through metadata and digest reads. A concurrent
	// exec or pathname replacement must not pair release metadata with race bytes.
	file, err := os.Open(path)
	if err != nil {
		return r, err
	}
	defer func() { _ = file.Close() }()
	info, err := buildinfo.Read(file)
	if err != nil {
		return r, fmt.Errorf("budget executable build info: %w", err)
	}
	r.Toolchain = info.GoVersion
	for _, setting := range info.Settings {
		switch setting.Key {
		case "-race", "-msan", "-asan":
			r.Race = r.Race || setting.Value == "true"
		case "CGO_ENABLED":
			r.CGOEnabled = setting.Value
		case "-trimpath":
			r.Trimpath = setting.Value
		case "-ldflags":
			r.LDFlags = setting.Value
		}
	}
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return r, err
	}
	r.SHA256 = fmt.Sprintf("%x", hash.Sum(nil))
	if r.Race || r.CGOEnabled != "0" || r.Trimpath != "true" {
		return r, fmt.Errorf("budget requires release build: race=%t CGO_ENABLED=%q trimpath=%q ldflags=%q", r.Race, r.CGOEnabled, r.Trimpath, r.LDFlags)
	}
	if elapsed < 0 || budget <= 0 || elapsed > budget {
		return r, fmt.Errorf("release candidate duration %s exceeds budget %s or is invalid", elapsed, budget)
	}
	return r, nil
}

// RequireCandidateBudget records every sample and enforces the unchanged limit
// only in the release arm. A functional arm never emits an SLO verdict.
func RequireCandidateBudget(t *testing.T, path string, elapsed, budget time.Duration) {
	t.Helper()
	if !ReleaseBudgetRun(t) {
		t.Logf("functional sample: measured=%s budget=%s (no SLO verdict)", elapsed, budget)
		return
	}
	r, err := CheckReleaseBudget(path, elapsed, budget)
	t.Logf("release budget receipt: path=%s sha256=%s toolchain=%s race=%t CGO_ENABLED=%s trimpath=%s ldflags=%q measured=%s budget=%s", r.Path, r.SHA256, r.Toolchain, r.Race, r.CGOEnabled, r.Trimpath, r.LDFlags, r.Elapsed, r.Budget)
	if err != nil {
		t.Fatal(err)
	}
}

var releaseCandidates = struct {
	sync.Mutex
	data map[string][]byte
}{data: make(map[string][]byte)}

// BuildReleaseCandidate matches Makefile's release flags and CGO setting. Cache
// bytes, not fixture-owned paths or FDs. Both source and overlay contents enter
// the key, so an on-disk mutant can never borrow an earlier healthy artifact.
// GOFLAGS (including composed observer/mutation overlays) still reaches go build.
// origin: hot-update S10 lead decision D-A / testing-standards §0.4–§0.6
func BuildReleaseCandidate(t *testing.T, root, binary, version, flags string) {
	t.Helper()
	if version == "" || strings.ContainsAny(version, " \t\r\n") {
		t.Fatal("invalid release candidate version")
	}
	linkerFlags := "-s -w -X github.com/LinZiyang666/tether/internal/proto.ReleaseVersion=" + version
	key, err := releaseCandidateKey(root, version, flags)
	if err != nil {
		t.Fatal(err)
	}
	releaseCandidates.Lock()
	defer releaseCandidates.Unlock()
	if body, ok := releaseCandidates.data[key]; ok {
		t.Logf("release candidate build: path=%s inputs=%s CGO_ENABLED=0 race=false trimpath=true linker_flags=%q cache_hit=true", binary, key, linkerFlags)
		if err := os.WriteFile(binary, body, 0700); err != nil {
			t.Fatal(err)
		}
		return
	}
	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Minute)
	defer cancel()
	// Explicit -race=false overrides even an inherited GOFLAGS=-race. Keeping
	// the override on the command line preserves every unrelated mutation flag.
	cmd := exec.CommandContext(ctx, "go", "build", "-race=false", "-trimpath", "-ldflags", linkerFlags, "-o", binary, "./cmd/tether")
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0", "GOFLAGS="+flags)
	t.Logf("release candidate build: path=%s inputs=%s CGO_ENABLED=0 race=false trimpath=true linker_flags=%q cache_hit=false", binary, key, linkerFlags)
	if output, err := CombinedOutput(cmd); err != nil {
		t.Fatalf("release candidate build: %v\n%s", err, output)
	}
	if _, err := CheckReleaseBudget(binary, 0, time.Second); err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	releaseCandidates.data[key] = body
}

func releaseCandidateKey(root, version, flags string) (string, error) {
	h := sha256.New()
	add := func(name string, body []byte) {
		_, _ = fmt.Fprintf(h, "%d:%s:%d:", len(name), name, len(body))
		_, _ = h.Write(body)
	}
	add("root", []byte(root))
	add("version", []byte(version))
	for _, name := range []string{"GOOS", "GOARCH", "GOAMD64", "GOARM", "GOTOOLCHAIN", "GOEXPERIMENT"} {
		add(name, []byte(os.Getenv(name)))
	}
	// Pins and embedded assets affect executable bytes as much as Go source.
	// Test source is excluded because it never enters the product build.
	paths := []string{"go.mod", "go.sum"}
	for _, dir := range []string{"cmd", "internal"} {
		if err := filepath.WalkDir(filepath.Join(root, dir), func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !entry.IsDir() && !strings.HasSuffix(path, "_test.go") {
				paths = append(paths, path)
			}
			return nil
		}); err != nil {
			return "", err
		}
	}
	slices.Sort(paths)
	for _, path := range paths {
		name := path
		if !filepath.IsAbs(path) {
			path = filepath.Join(root, path)
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return "", err
		}
		add(name, body)
	}
	// Normalize overlay paths but retain replacement names and bytes. Fixture
	// directories change between rounds even when their effective sources do not.
	for _, flag := range strings.Fields(flags) {
		path, overlay := strings.CutPrefix(flag, "-overlay=")
		if !overlay {
			add("flag", []byte(flag))
			continue
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return "", err
		}
		var o struct{ Replace map[string]string }
		if err := json.Unmarshal(data, &o); err != nil {
			return "", err
		}
		names := make([]string, 0, len(o.Replace))
		for name := range o.Replace {
			names = append(names, name)
		}
		slices.Sort(names)
		for _, name := range names {
			to := o.Replace[name]
			if to == "" {
				add("delete:"+name, nil)
				continue
			}
			body, err := os.ReadFile(to)
			if err != nil {
				return "", err
			}
			add("overlay:"+name, body)
		}
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}
