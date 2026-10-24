//go:build linux

package clusterharness

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/testharness"
)

// Identical observations may share an immutable build, but changed overlay
// bytes must reach the compiler even when their pathname and source tree match.
// origin: hot-update S6 plan §11 / T48 / T52
func TestBrokerObservationBuildRejectsChangedInputs(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	_, file, _, _ := runtime.Caller(0)
	repo := filepath.Dir(filepath.Dir(filepath.Dir(file)))
	digest, err := storeFixtureSourceDigest(repo)
	if err != nil {
		t.Fatal(err)
	}
	for round := 0; round < 5; round++ {
		exerciseBrokerObservationBuildInputs(t, repo, digest)
	}
	testharness.AssertNoGoroutineLeak(t, "observation build inputs", g)
	testharness.AssertNoFDLeak(t, "observation build inputs", fd, 0)
}

func exerciseBrokerObservationBuildInputs(t *testing.T, repo, digest string) {
	t.Helper()
	root := testharness.GuardProcessDirectory(t, t.TempDir())
	overlay := productObservationOverlay(t, repo, root, 12345)
	binary := filepath.Join(root, "broker-product")
	if err := productObservationBuild(t.Context(), repo, binary, digest, overlay); err != nil {
		t.Fatal(err)
	}
	output, err := testharness.CombinedOutput(exec.CommandContext(t.Context(), binary, "version"))
	if err != nil || !strings.Contains(string(output), "broker-image-v1") {
		t.Fatalf("real observation artifact: %s: %v", output, err)
	}
	data, err := os.ReadFile(overlay)
	if err != nil {
		t.Fatal(err)
	}
	var replacements struct{ Replace map[string]string }
	if err := json.Unmarshal(data, &replacements); err != nil {
		t.Fatal(err)
	}
	path := replacements.Replace[filepath.Join(repo, "internal/broker/reexec.go")]
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, writeErr := file.WriteString("\ninvalid go source\n")
	closeErr := file.Close()
	if writeErr != nil || closeErr != nil {
		t.Fatalf("mutate observation: %v / %v", writeErr, closeErr)
	}
	if err := productObservationBuild(t.Context(), repo, filepath.Join(root, "mutated-product"), digest, overlay); err == nil {
		t.Fatal("changed observation inputs reused the unmutated artifact")
	}
}

// origin: hot-update S6 plan §9.3 / T55 / T56
func TestBrokerProductFixtureCloseJoinsResources(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	initial := productFixtureFDLinks(t)
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) { exerciseBrokerProductFixtureClose(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "product fixture close", g)
	if testharness.FDCount() != fd {
		t.Logf("FD identities before=%v after=%v", initial, productFixtureFDLinks(t))
	}
	testharness.AssertNoFDLeak(t, "product fixture close", fd, 0)
}
func exerciseBrokerProductFixtureClose(t *testing.T) {
	t.Helper()
	f, err := StartBrokerRunFixture(t, BrokerImageFixtureConfig{Topology: "single"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.Checkpoint("before-reload"); err != nil {
		t.Fatal(err)
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
}
func productFixtureFDLinks(t *testing.T) map[string]string {
	t.Helper()
	files, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	links := map[string]string{}
	for _, file := range files {
		if link, err := os.Readlink(filepath.Join("/proc/self/fd", file.Name())); err == nil {
			links[file.Name()] = link
		}
	}
	return links
}
