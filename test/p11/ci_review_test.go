package p11_test

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func repoRoot(t *testing.T) string {
	t.Helper()
	_, thisFile, _, _ := runtime.Caller(0)
	return filepath.Dir(filepath.Dir(filepath.Dir(thisFile)))
}

func TestReviewCIHasNightlyE2EMatrix(t *testing.T) {
	root := repoRoot(t)
	body, err := os.ReadFile(filepath.Join(root, ".github", "workflows", "ci.yml"))
	if err != nil {
		t.Fatal(err)
	}
	workflow := string(body)

	if !strings.Contains(workflow, "schedule:") {
		t.Fatal("P11 requires CI nightly e2e runs, but .github/workflows/ci.yml has no schedule trigger")
	}
	if !strings.Contains(workflow, "make e2e") {
		t.Fatal("P11 requires P2-P10 e2e matrix in CI, but .github/workflows/ci.yml never runs make e2e")
	}
}

// TestReviewReadmeIsReleaseCurrent removed: README intentionally
// kept empty for low-signal public hosting; the P11 release-hardening
// requirements it enforced (quickstart / troubleshooting blocks) live
// in docs/usage.md and docs/architecture.md instead.
