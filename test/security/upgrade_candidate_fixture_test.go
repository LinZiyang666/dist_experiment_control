package security_test

import (
	"archive/tar"
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/LinZiyang666/tether/internal/agentimage"
	"github.com/LinZiyang666/tether/internal/binaryupgrade"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradecli"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// Candidate verification now belongs to ctl, before Prepare or root stage.
// Real HTTP, digest checking, extraction and a supervised version process keep
// a legacy-policy refusal from masquerading as tarball safety coverage.
// origin: hot-update S8 plan §5.2 / §6.1 / §7.1
func fetchUpgradeTarball(t *testing.T, entries []tarEntry) (upgradecli.Candidate, int, error) {
	t.Helper()
	// Keep a broken candidate cleanup inside test-owned storage too. The Close
	// assertion below still runs before testing removes this outer directory.
	t.Setenv("TMPDIR", t.TempDir())
	tarball, sum := buildTarball(t, entries)
	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		_, _ = w.Write(tarball)
	}))
	defer srv.Close()
	probes := 0
	candidate, err := upgradecli.FetchCandidate(upgradetx.ComponentAgent, srv.URL+"/tether.tar.gz", sum, binaryupgrade.FetchURL,
		func(path string) (string, agentimage.Capabilities, error) {
			probes++
			return agentimage.ProbeCapabilities(path, testharness.RunProcess)
		})
	t.Cleanup(func() {
		if err := candidate.Close(); err != nil {
			t.Error(err)
		}
	})
	if requests.Load() != 1 {
		t.Fatalf("candidate archive was not fetched exactly once: %d", requests.Load())
	}
	return candidate, probes, err
}

// A probe failure is later than extraction: it cannot certify that a path or
// link was refused before untrusted executable bytes became usable.
// origin: hot-update S8 plan §5.2 / §6.1
func exerciseUpgradeTarballRejected(t *testing.T, entries []tarEntry) {
	t.Helper()
	candidate, probes, err := fetchUpgradeTarball(t, entries)
	var refusal *upgradetx.Error
	if !errors.As(err, &refusal) || refusal.Code != upgradetx.CodeRejected || upgradecli.ExitCode(err) != 64 ||
		!strings.Contains(refusal.Detail, "candidate archive has no usable tether executable") || candidate.Path != "" || probes != 0 {
		t.Fatalf("unsafe archive must fail extraction before probing: candidate=%+v err=%v probes=%d", candidate, err, probes)
	}
}

func exerciseUpgradeTarballAccepted(t *testing.T) {
	t.Helper()
	// Executable AND smoke-gate-compatible: FetchCandidate runs version before
	// Prepare. This script proves extraction/probing, never hot activation.
	binBody := []byte("#!/bin/sh\necho 'tether v9.9.9-test (proto v2)'\n")
	candidate, probes, err := fetchUpgradeTarball(t, []tarEntry{
		{name: "README", typeflag: tar.TypeReg, body: []byte("hi")},
		{name: "LICENSE", typeflag: tar.TypeReg, body: []byte("MIT")},
		{name: "tether", typeflag: tar.TypeReg, body: binBody, mode: 0o755},
	})
	if err != nil {
		t.Fatalf("legitimate multi-entry tarball rejected: %v", err)
	}
	body, err := os.ReadFile(candidate.Path)
	info, statErr := os.Lstat(candidate.Path)
	if err != nil || statErr != nil || !bytes.Equal(body, binBody) || !info.Mode().IsRegular() || info.Mode().Perm() != 0o755 ||
		candidate.SHA != fmt.Sprintf("%x", sha256.Sum256(binBody)) || candidate.Release != "v9.9.9-test" || candidate.CapabilityDigest == "" || probes != 1 {
		t.Fatalf("candidate did not preserve the sole regular executable: candidate=%+v read=%v stat=%v probes=%d", candidate, err, statErr, probes)
	}
	if err := candidate.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(candidate.Path); !os.IsNotExist(err) {
		t.Fatalf("candidate workspace %s survived Close: %v", candidate.Path, err)
	}
}
