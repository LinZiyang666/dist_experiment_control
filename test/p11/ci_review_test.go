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

func TestReviewReadmeIsReleaseCurrent(t *testing.T) {
	body, err := os.ReadFile(filepath.Join(repoRoot(t), "README.md"))
	if err != nil {
		t.Fatal(err)
	}
	readme := string(body)

	for _, stale := range []string{
		"P7 complete",
		"P6 features still apply",
		"P5 features still apply",
		"P3 features still apply",
		"lands in P8",
		"admin nodes --db",
		"go install ...@v1.62.2",
	} {
		if strings.Contains(readme, stale) {
			t.Fatalf("README still contains stale P11 release-hardening text %q", stale)
		}
	}

	for _, required := range []string{
		"install.sh",
		"troubleshooting",
		"tether node upgrade",
		"tether admin",
	} {
		if !strings.Contains(strings.ToLower(readme), strings.ToLower(required)) {
			t.Fatalf("README should include release-current quickstart/troubleshooting content mentioning %q", required)
		}
	}
}
