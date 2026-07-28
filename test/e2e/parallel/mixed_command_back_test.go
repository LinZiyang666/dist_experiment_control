package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// origin: external_review_test.go (renamed in B6) — the parallel runner's own external review; see
// docs/reviews/parallel-flake-rootcause.md.
//
// TestExternalReviewNonSplitModeCanPass exercises the runner's advertised
// default (without -split). A passing test must produce an ALL PASS runner
// result; comparing those results with len(units), which is zero in this mode,
// turns every successful default run into a false failure.
func TestExternalReviewNonSplitModeCanPass(t *testing.T) {
	root, err := repoRoot()
	if err != nil {
		t.Fatalf("repo root: %v", err)
	}
	cmd := exec.Command("go", "run", "./test/e2e/parallel",
		"-workers", "1",
		"-run", "^TestTransferDefaultsMatrix$",
		"-timeout", "2m",
		"-no-avoid-busy",
	)
	cmd.Dir = root
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("a passing non-split run exited non-zero: %v\n%s", err, out)
	}
	if !strings.Contains(string(out), "ALL PASS") {
		t.Fatalf("passing non-split run did not report ALL PASS:\n%s", out)
	}
}

// TestExternalReviewMixedCommandShapesFallBackWhole proves that parsing one
// command in a matrix cannot hide a second command whose shape is dynamic. A
// matrix-level fallback is the only safe representation unless every command
// was parsed; running only the parseable half is silent under-coverage.
func TestExternalReviewMixedCommandShapesFallBackWhole(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "test", "e2e")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	src := `package e2e
import (
	"os/exec"
	"testing"
)
func TestMixedMatrix(t *testing.T) {
	_ = exec.Command("go", "test", "-race", "./first/...")
	args := []string{"test", "-race", "./second/..."}
	_ = exec.Command("go", args...)
}`
	if err := os.WriteFile(filepath.Join(dir, "all_phases_test.go"), []byte(src), 0o644); err != nil {
		t.Fatal(err)
	}
	units, unparsed, err := splitMatrices(root)
	if err != nil {
		t.Fatalf("split: %v", err)
	}
	if len(units) != 0 || len(unparsed) != 1 || unparsed[0] != "TestMixedMatrix" {
		t.Fatalf("partially parsed matrix must fall back whole; units=%+v unparsed=%v", units, unparsed)
	}
}

// TestExternalReviewHelperCommandFallsBackWhole covers the less obvious mixed
// form: one command is in the matrix body and another is hidden in a local
// helper. Looking only at the Test function body accepts the literal half and
// silently drops the helper half.
func TestExternalReviewHelperCommandFallsBackWhole(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "test", "e2e")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	src := `package e2e
import (
	"os/exec"
	"testing"
)
func runDynamic(args []string) { _ = exec.Command("go", args...) }
func TestHelperMatrix(t *testing.T) {
	_ = exec.Command("go", "test", "-race", "./first/...")
	runDynamic([]string{"test", "-race", "./second/..."})
}`
	if err := os.WriteFile(filepath.Join(dir, "all_phases_test.go"), []byte(src), 0o644); err != nil {
		t.Fatal(err)
	}
	units, unparsed, err := splitMatrices(root)
	if err != nil {
		t.Fatalf("split: %v", err)
	}
	if len(units) != 0 || len(unparsed) != 1 || unparsed[0] != "TestHelperMatrix" {
		t.Fatalf("helper command must force whole fallback; units=%+v unparsed=%v", units, unparsed)
	}
}

// TestExternalReviewParserPreservesOriginalRunFilter pins a semantic part of
// the serial command, not merely its package/tag/race shell. Dropping -run
// executes a different suite and lets later name sharding replace rather than
// intersect the matrix's intended selection.
func TestExternalReviewParserPreservesOriginalRunFilter(t *testing.T) {
	units := parseGoTestArgs("TestFilteredMatrix", []string{
		"go", "test", "-race", "-run", "TestOne|TestTwo", "./internal/example/...",
	})
	if len(units) != 1 {
		t.Fatalf("got %d units, want 1", len(units))
	}
	if units[0].runFilter != "TestOne|TestTwo" {
		t.Fatalf("serial -run filter was lost: got %q", units[0].runFilter)
	}
}

func TestExternalReviewParserFailsClosedOnUnknownOrDynamicArgs(t *testing.T) {
	for name, args := range map[string][]string{
		"unknown flag":    {"go", "test", "-shuffle=on", "./internal/example/..."},
		"dynamic timeout": {"go", "test", "-timeout", placeholderArg, "./internal/example/..."},
		"not go test":     {"go", "run", "./internal/example/..."},
	} {
		t.Run(name, func(t *testing.T) {
			if got := parseGoTestArgs("TestMatrix", args); len(got) != 0 {
				t.Fatalf("unsupported argv must fall back whole, got %+v", got)
			}
		})
	}
}

// TestExternalReviewAllocateHandlesUnevenNUMANodes models the state produced
// after busy-core exclusion. There are enough total cores for three workers,
// but a fixed even worker count per node asks the one-core node for two and
// fails instead of assigning one worker there and two to the larger node.
func TestExternalReviewAllocateHandlesUnevenNUMANodes(t *testing.T) {
	topo := &topology{
		nodes: map[int][]physCore{
			0: {{id: 0, siblings: []int{0}, node: 0}},
			1: {
				{id: 1, siblings: []int{1}, node: 1},
				{id: 2, siblings: []int{2}, node: 1},
				{id: 3, siblings: []int{3}, node: 1},
			},
		},
		total: 4,
	}
	plan, err := allocate(topo, 3)
	if err != nil {
		t.Fatalf("four available cores should support three isolated workers: %v", err)
	}
	if len(plan) != 3 {
		t.Fatalf("got %d assignments, want 3", len(plan))
	}
}

func TestExternalReviewAutoWorkersFitsSmallCIAndCapsLargeHosts(t *testing.T) {
	for _, tc := range []struct {
		cores, work, want int
	}{
		{cores: 2, work: 100, want: 1},
		{cores: 4, work: 100, want: 2},
		{cores: 44, work: 107, want: 20},
		{cores: 44, work: 3, want: 3},
	} {
		if got := autoWorkerCount(tc.cores, tc.work); got != tc.want {
			t.Errorf("autoWorkerCount(%d, %d)=%d, want %d", tc.cores, tc.work, got, tc.want)
		}
	}
}

func TestExternalReviewHeavyWorkerUsesCapableNUMANode(t *testing.T) {
	plan := []assignment{
		{worker: 0, node: 0, cores: []physCore{{id: 0}}},
		{worker: 1, node: 0, cores: []physCore{{id: 1}}},
		{worker: 2, node: 0, cores: []physCore{{id: 2}}},
		{worker: 3, node: 1, cores: []physCore{{id: 3}}},
	}
	rest, heavy := reserveHeavyWorker(plan, 3)
	if heavy == nil {
		t.Fatal("node 0 can supply three slices; the smaller last node must not disable the heavy worker")
	}
	if heavy.node != 0 || len(rest) != 1 {
		t.Fatalf("heavy=%+v rest=%+v, want node 0 merged with one assignment left", heavy, rest)
	}
}
