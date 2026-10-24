//go:build linux

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"

	"github.com/LinZiyang666/tether/internal/testharness"
)

// The cache is private to one TestMain invocation, so source, toolchain and
// inherited GOFLAGS (including mutations) cannot cross invocation boundaries.
// Separate overlay keys keep the refusal port out of every ordinary product.
// A kernel lock also joins concurrent builders in isolated test processes.
// origin: S10 internal review CMD-TETHER-BUDGET F1; hot-update S10 plan §11.1
func signedRollBuild(t *testing.T, keeper ...bool) string {
	t.Helper()
	root := os.Getenv(signedRollArtifactRootKey)
	if root == "" {
		t.Fatal("signed roll artifact directory is not owned by TestMain")
	}
	name := "product"
	if len(keeper) > 0 && keeper[0] {
		name = "keeper"
	}
	binary := filepath.Join(root, name)
	lock, err := os.OpenFile(binary+".lock", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := lock.Close(); err != nil {
			t.Errorf("close artifact build lock: %v", err)
		}
	}()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(binary); os.IsNotExist(err) {
		built := buildSignedRollArtifact(t, keeper...)
		sha := signedRollSHA(t, built)
		if err := os.Chmod(built, 0555); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(built, binary); err != nil {
			t.Fatal(err)
		}
		signedRollWrite(t, binary+".sha256", []byte(sha), 0600)
	} else if err != nil {
		t.Fatal(err)
	}
	expected, err := os.ReadFile(binary + ".sha256")
	if err != nil {
		t.Fatal(err)
	}
	actual := signedRollSHA(t, binary)
	if actual != string(expected) {
		t.Fatal("shared signed roll artifact changed after publication")
	}
	t.Logf("shared product path=%s sha256=%s", binary, actual)
	return binary
}

type signedRollArtifactPair struct {
	paths, hashes [2]string
}

// A round owns mutable daemons, but its immutable artifacts must outlive that
// round and be reused by every signed-roll root in this package invocation.
// origin: S10 internal review CMD-TETHER-BUDGET F1; hot-update S10 plan §11.1
func TestSignedRollBuildReusesPackageArtifacts(t *testing.T) {
	if runIsolatedBinaryTest(t) {
		return
	}
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	var artifacts signedRollArtifactPair
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseSignedRollArtifactReuse(t, &artifacts) })
	}
	testharness.AssertNoGoroutineLeak(t, "signed roll artifact reuse", beforeG)
	testharness.AssertNoFDLeak(t, "signed roll artifact reuse", beforeFD, 0)
}

func exerciseSignedRollArtifactReuse(t *testing.T, artifacts *signedRollArtifactPair) {
	t.Helper()
	for i, keeper := range []bool{false, true} {
		if artifacts.paths[i] != "" {
			if _, err := os.Stat(artifacts.paths[i]); err != nil {
				t.Fatalf("a round retired the package artifact: %v", err)
			}
		}
		binary := signedRollBuild(t, keeper)
		sha := signedRollSHA(t, binary)
		if artifacts.paths[i] == "" {
			artifacts.paths[i], artifacts.hashes[i] = binary, sha
		} else if binary != artifacts.paths[i] || sha != artifacts.hashes[i] {
			t.Fatalf("immutable artifact was rebuilt: path=%s sha=%s, want path=%s sha=%s", binary, sha, artifacts.paths[i], artifacts.hashes[i])
		}
	}
	if artifacts.paths[0] == artifacts.paths[1] || artifacts.hashes[0] == artifacts.hashes[1] {
		t.Fatal("keeper overlay reused the ordinary product artifact")
	}
}
