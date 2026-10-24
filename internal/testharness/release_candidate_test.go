package testharness

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func releaseBuildFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	for name, body := range map[string]string{
		"go.mod":                    "module github.com/LinZiyang666/tether\n\ngo 1.26.0\n",
		"internal/proto/version.go": "package proto\nvar ReleaseVersion = \"unlinked\"\n",
		"cmd/tether/main.go":        "package main\nimport (\"fmt\"; \"github.com/LinZiyang666/tether/internal/proto\")\nfunc main(){fmt.Print(proto.ReleaseVersion, \":\", payload)}\n",
		"cmd/tether/payload.go":     "package main\nconst payload = \"original\"\n",
		"go.sum":                    "",
	} {
		path := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

// A short race duration cannot acquire a production SLO verdict. Each exact
// budget boundary is accepted; one nanosecond beyond it must fail unchanged.
// origin: hot-update S10 lead decision D-A / RELEASE-MODE F1
func TestReleaseCandidateBudgetRejectsRaceBinary(t *testing.T) {
	root := releaseBuildFixture(t)
	release, race := filepath.Join(root, "release"), filepath.Join(root, "race")
	BuildReleaseCandidate(t, root, release, "v0.0.0-budget", "")
	cmd := exec.CommandContext(t.Context(), "go", "build", "-race", "-trimpath", "-o", race, "./cmd/tether")
	cmd.Dir, cmd.Env = root, append(os.Environ(), "CGO_ENABLED=1", "GOFLAGS=")
	if out, err := CombinedOutput(cmd); err != nil {
		t.Fatalf("race control build: %v\n%s", err, out)
	}
	g, fd := runtime.NumGoroutine(), FDCount()
	for round := 0; round < 5; round++ {
		exerciseReleaseCandidateBudget(t, release, race)
	}
	AssertNoGoroutineLeak(t, "release budget build identity", g)
	AssertNoFDLeak(t, "release budget build identity", fd, 0)
}

func exerciseReleaseCandidateBudget(t *testing.T, release, race string) {
	t.Helper()
	for _, budget := range []time.Duration{500 * time.Millisecond, 5 * time.Second, 30 * time.Second, 10 * time.Second} {
		if r, err := CheckReleaseBudget(release, budget, budget); err != nil || r.SHA256 == "" || r.Race || r.CGOEnabled != "0" {
			t.Fatalf("valid release boundary: %+v %v", r, err)
		}
		if _, err := CheckReleaseBudget(release, budget+1, budget); err == nil {
			t.Fatalf("accepted duration above %s", budget)
		}
		if _, err := CheckReleaseBudget(race, 0, budget); err == nil || !strings.Contains(err.Error(), "requires release build") {
			t.Fatalf("race-built candidate acquired a budget verdict: %v", err)
		}
	}
	for _, pair := range [][2]time.Duration{{-1, time.Second}, {0, 0}} {
		if _, err := CheckReleaseBudget(release, pair[0], pair[1]); err == nil {
			t.Fatal("accepted invalid duration/budget")
		}
	}
}

// Nested mutation overlays and on-disk changes are part of the cache identity.
// Keeping cached bytes after the first output is removed proves path ownership
// is independent of the fixture that first requested a build.
// origin: hot-update S10 lead decision D-A / testing-standards §0.4
func TestReleaseCandidateCacheTracksSourceAndOverlay(t *testing.T) {
	root := releaseBuildFixture(t)
	BuildReleaseCandidate(t, root, filepath.Join(root, "warm"), "v0.0.0-budget", "")
	g, fd := runtime.NumGoroutine(), FDCount()
	for round := 0; round < 5; round++ {
		exerciseReleaseCandidateCache(t, root, round)
	}
	AssertNoGoroutineLeak(t, "release candidate cache/overlay", g)
	AssertNoFDLeak(t, "release candidate cache/overlay", fd, 0)
}

func exerciseReleaseCandidateCache(t *testing.T, root string, round int) {
	t.Helper()
	dir := t.TempDir()
	shadow := filepath.Join(dir, "payload.go")
	manifest := filepath.Join(dir, "overlay.json")
	data, err := json.Marshal(map[string]any{"Replace": map[string]string{filepath.Join(root, "cmd/tether/payload.go"): shadow}})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(manifest, data, 0600); err != nil {
		t.Fatal(err)
	}
	for _, payload := range []string{"first", "second", "second"} {
		if err := os.WriteFile(shadow, []byte("package main\nconst payload = \""+payload+"\"\n"), 0600); err != nil {
			t.Fatal(err)
		}
		binary := filepath.Join(dir, "candidate")
		BuildReleaseCandidate(t, root, binary, "v0.0.0-budget", "-race -overlay="+manifest)
		out, err := Output(exec.CommandContext(t.Context(), binary))
		if err != nil || string(out) != "v0.0.0-budget:"+payload {
			t.Fatalf("cached source/overlay ignored: %s %v", out, err)
		}
		if err := os.Remove(binary); err != nil {
			t.Fatal(err)
		}
	}
	before, err := releaseCandidateKey(root, "v0.0.0-budget", "")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "cmd/tether/payload.go")
	if err := os.WriteFile(path, []byte(strings.Repeat("\n", round+1)+"package main\nconst payload = \"changed\"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	after, err := releaseCandidateKey(root, "v0.0.0-budget", "")
	if err != nil || before == after {
		t.Fatalf("on-disk source change reused cache key: %v", err)
	}
}
