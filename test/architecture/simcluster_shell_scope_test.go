package architecture

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"
)

var (
	drillFunctionDefinition = regexp.MustCompile(`(?m)^([A-Za-z_][A-Za-z0-9_]*)\(\)[[:space:]]*\{`)
	deferredFunctionCall    = regexp.MustCompile(`sh -c[^\n]*\\\$\(([A-Za-z_][A-Za-z0-9_]*)\)`)
)

// origin: docs/reviews/g75-g78-deploy-defaults-external-review.md F5
func TestSimclusterDrillsDoNotDeferLocalFunctionsToFreshShell(t *testing.T) {
	root := filepath.Join(repoRoot(t), simDrillsDir)
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".sh" {
			continue
		}
		path := filepath.Join(root, entry.Name())
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		defined := map[string]bool{}
		for _, m := range drillFunctionDefinition.FindAllSubmatch(body, -1) {
			defined[string(m[1])] = true
		}
		for _, m := range deferredFunctionCall.FindAllSubmatch(body, -1) {
			name := string(m[1])
			if defined[name] {
				t.Errorf("%s defers local function %s to `sh -c`; the fresh shell cannot see it and an empty command substitution can make the oracle vacuously pass", entry.Name(), name)
			}
		}
	}
}
