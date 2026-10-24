package main

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/testharness"
)

// Every leaf retains the five original rounds and one unchanged 600s process.
// The denominator is independent of the planner, including tier-A's inline
// substitution and the existing transport applicability exclusions.
// origin: S7 internal review TRANSFER-OVERLAY-REPAIR F5 / hot-update S7 plan lead L17
func TestTransferBoundaryPartitionsKeepEveryCellAndRound(t *testing.T) {
	declaration := unit{matrix: "TestHotUpdateIntegrationMatrix", pkg: "./test/hotupdate/...", tags: "hotupdate_integration", race: true, timeo: "600s", name: "integration"}
	names := []string{"TestTransferAgentExecAcrossEveryBoundary", "TestTransferBrokerExecKeepsTrackerBeforeReaper", "TestTransferCarrierAndJSRestartReceiptsAreDistinct"}
	for _, shards := range []int{0, 1, 8} {
		planned, err := shardPackagesWithList([]unit{declaration}, shards, func(unit) ([]string, error) { return names, nil })
		if err != nil {
			t.Fatal(err)
		}
		if len(planned) != 226 {
			t.Fatalf("boundary matrices have %d units, want 44 agent + 132 broker + 50 restart", len(planned))
		}
		expected := transferPartitionPaths()
		counts := make(map[string]int)
		labels := make(map[string]bool)
		for _, u := range planned {
			if !u.race || u.tags != declaration.tags || u.timeo != "600s" || u.whole || u.extra != "" {
				t.Fatalf("boundary partition changed its budget/build: %+v", u)
			}
			if labels[u.name] {
				t.Fatalf("duplicate receipt identity: %s", u.name)
			}
			labels[u.name] = true
			matched := 0
			filters := transferSelectorExpressions(t, u.runFilter)
			for _, path := range expected {
				if !transferSelectorMatches(filters, path) {
					continue
				}
				counts[path]++
				matched++
			}
			if matched != 5 {
				t.Fatalf("selector %s executes %d rounds/cells, want exactly five rounds of one cell", u.runFilter, matched)
			}
		}
		for _, path := range expected {
			if counts[path] != 1 {
				t.Fatalf("%s has %d owners", path, counts[path])
			}
		}
	}
}

func transferPartitionPaths() []string {
	var paths []string
	for _, test := range []string{"TestTransferAgentExecAcrossEveryBoundary", "TestTransferBrokerExecKeepsTrackerBeforeReaper", "TestTransferCarrierAndJSRestartReceiptsAreDistinct"} {
		prefixes := []string{""}
		if strings.Contains(test, "BrokerExec") {
			prefixes = []string{"single/", "sole-voter/", "quorum/"}
		}
		if strings.Contains(test, "Restart") {
			prefixes = []string{"carrier/", "data-connection/", "same-store-js/"}
		}
		for _, prefix := range prefixes {
			for _, tier := range []string{"a", "b"} {
				for _, direction := range []string{"push", "pull"} {
					for _, window := range []string{"prepare-accepted", "source-pinned", "sender-chunk-pending", "receiver-write", "Sync-pending", "synced-prefix", "grant-reply-lost", "pre-primitive", "post-primitive-pre-outcome", "terminal-pre-history-ACK", "cleanup-pending"} {
						if tier == "a" && window == "sender-chunk-pending" {
							window = "inline-send-unconfirmed"
						}
						if strings.Contains(test, "Restart") && (prefix != "same-store-js/" && window != "synced-prefix" || prefix == "data-connection/" && tier == "a") {
							continue
						}
						for round := 0; round < 5; round++ {
							paths = append(paths, fmt.Sprintf("%s/rounds/round-%d/%stier-%s/%s/%s", test, round, prefix, tier, direction, window))
						}
					}
				}
			}
		}
	}
	return paths
}

func transferSelectorExpressions(t *testing.T, selector string) []*regexp.Regexp {
	t.Helper()
	var filters []*regexp.Regexp
	for _, filter := range strings.Split(selector, "/") {
		re, err := regexp.Compile(filter)
		if err != nil {
			t.Fatal(err)
		}
		filters = append(filters, re)
	}
	return filters
}

func transferSelectorMatches(filters []*regexp.Regexp, path string) bool {
	names := strings.Split(path, "/")
	if len(filters) != len(names) {
		return false
	}
	for i, filter := range filters {
		if !filter.MatchString(names[i]) {
			return false
		}
	}
	return true
}

// Only command selection is simulated here. The built matrix entry point must
// issue every exact tagged race command; this does not certify transfer cuts.
// origin: S7 internal review TRANSFER-OVERLAY-REPAIR F5 / hot-update S7 plan lead L17
func TestTransferBoundaryPartitionsAlsoCoverDirectMatrix(t *testing.T) {
	root, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	binary := filepath.Join(dir, "matrix.test")
	cmd := exec.CommandContext(t.Context(), "go", "test", "-c", "-race", "-tags", "e2e_matrix", "-o", binary, "./test/e2e")
	cmd.Dir = root
	var output bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &output
	if err := testharness.RunProcess(cmd); err != nil {
		t.Fatalf("build direct matrix: %v\n%s", err, output.Bytes())
	}
	const recorder = `#!/bin/sh
printf '%s\n' "$*" >> "$TRANSFER_MATRIX_ARGS"
case " $* " in
 *" -list ^Test "*)
 printf 'TestTransferAgentExecAcrossEveryBoundary\nTestTransferBrokerExecKeepsTrackerBeforeReaper\nTestTransferCarrierAndJSRestartReceiptsAreDistinct\nok github.com/LinZiyang666/tether/test/hotupdate\n'
 ;;
 *" -list "*) printf 'ok github.com/LinZiyang666/tether/test/hotupdate\n' ;;
esac
`
	if err := os.WriteFile(filepath.Join(dir, "go"), []byte(recorder), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseTransferDirectPartitions(t, binary)
	}
	testharness.AssertNoGoroutineLeak(t, "direct transfer partitions", g)
	testharness.AssertNoFDLeak(t, "direct transfer partitions", fd, 0)
}

func exerciseTransferDirectPartitions(t *testing.T, binary string) {
	t.Helper()
	log := filepath.Join(t.TempDir(), "argv")
	t.Setenv("TRANSFER_MATRIX_ARGS", log)
	cmd := exec.CommandContext(t.Context(), binary, "-test.run=^TestHotUpdateIntegrationMatrix$", "-test.timeout=60s")
	var output bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &output
	if err := testharness.RunProcess(cmd); err != nil {
		t.Fatalf("direct transfer matrix: %v\n%s", err, output.Bytes())
	}
	data, err := os.ReadFile(log)
	if err != nil {
		t.Fatal(err)
	}
	expected := transferPartitionPaths()
	counts := make(map[string]int)
	units := 0
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		if strings.Contains(line, " -list ") {
			continue
		}
		prefix := "test -race -count=1 -tags hotupdate_integration -timeout 600s ./test/hotupdate -run "
		if !strings.HasPrefix(line, prefix) {
			t.Fatalf("direct partition changed its build/budget: %s", line)
		}
		selector := strings.TrimPrefix(line, prefix)
		filters := transferSelectorExpressions(t, selector)
		matched := 0
		for _, path := range expected {
			if transferSelectorMatches(filters, path) {
				counts[path]++
				matched++
			}
		}
		if matched != 5 {
			t.Fatalf("direct selector %s owns %d cells/rounds", selector, matched)
		}
		units++
	}
	if units != 226 {
		t.Fatalf("direct matrix has %d units, want 226", units)
	}
	for _, path := range expected {
		if counts[path] != 1 {
			t.Fatalf("direct %s has %d owners", path, counts[path])
		}
	}
}
