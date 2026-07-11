package main

// command_tree_inventory_test.go — the COMMITTED command-tree structural gate for the simcluster
// deploy-tier coverage program (S0-ledger, landed by batch S1). It enumerates the Cobra command tree —
// every command path, its Hidden bit, and its full local+persistent+inherited flag set with per-flag
// Hidden bits — and asserts ZERO DIFF against checked-in golden dumps. When the product's CLI surface
// changes (a new command, a new flag, a Hidden toggle), this test FAILS, forcing the simcluster coverage
// inventory (docs/reviews/simcluster-coverage-inventory.md §2) to be re-reconciled — every new surface
// must map to an S-batch drill or an explicit NOT-COVERED before the golden is regenerated. This is the
// inventory §3 generation-law made permanent + runnable at every S-batch's wrap gate.
//
// TWO trees are gated (roadmap round-6 "94 / 99" convention):
//   - the CONSTRUCTED tree (94 paths) authored in newRootCmd(), before any cobra runtime injection; and
//   - the RUNTIME tree (99 paths) after cobra's InitDefaultCompletionCmd() injects the `completion <shell>`
//     subtree + its `--no-descriptions` flag. Gating the runtime tree keeps the completion path/flag/Hidden
//     surface under the structural gate too, not merely a loose `completion bash` smoke in drill 60.
//
// Regenerate BOTH goldens after an INTENTIONAL, inventory-reconciled CLI change:
//   go test ./cmd/tether -run TestCommandTreeInventory -update-command-tree-golden

import (
	"flag"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

var updateCommandTreeGolden = flag.Bool("update-command-tree-golden", false,
	"regenerate the command-tree golden dumps instead of asserting against them")

const (
	commandTreeGolden        = "testdata/command_tree_golden.txt"
	commandTreeGoldenRuntime = "testdata/command_tree_golden_runtime.txt"
)

func TestCommandTreeInventory(t *testing.T) {
	// Legacy hygiene: older revisions wrote a `testdata/*.actual` diagnostic INTO the source tree on drift.
	// Remove any such residue so a project `git add -A` can never stage a stale diagnostic (MINOR-2).
	_ = os.Remove(commandTreeGolden + ".actual")
	_ = os.Remove(commandTreeGoldenRuntime + ".actual")

	// Constructed tree — the CLI surface authored in newRootCmd, before cobra's runtime injection.
	construct := dumpCommandTree(newRootCmd())
	checkCommandTreeGolden(t, commandTreeGolden, construct)

	// Runtime tree — cobra's InitDefaultCompletionCmd injects `completion` + its per-shell subcommands
	// (bash/fish/powershell/zsh) and the `--no-descriptions` flag. Walk it so that surface is gated too.
	rt := newRootCmd()
	rt.InitDefaultCompletionCmd()
	runtime := dumpCommandTree(rt)
	checkCommandTreeGolden(t, commandTreeGoldenRuntime, runtime)

	// The "94 / 99" convention, made explicit and drift-proof: the runtime tree is exactly the constructed
	// tree plus the 5 completion paths (`completion` + 4 shells). This documents the invariant in code and
	// fails loudly if a future cobra bump changes how many paths completion injects.
	if got, want := strings.Count(runtime, "\n"), strings.Count(construct, "\n")+5; got != want {
		t.Errorf("runtime tree = %d paths, want constructed+5 completion paths = %d "+
			"(roadmap 94/99 convention — a cobra-version change altered the injected completion subtree)",
			got, want)
	}
}

// checkCommandTreeGolden asserts `got` against the golden at `path`, or regenerates it under
// -update-command-tree-golden. On drift it writes the actual dump to a TEMP dir (never the source tree,
// MINOR-2) and points at it, so `go test ./...` on a legitimate CLI change leaves no untracked residue.
func checkCommandTreeGolden(t *testing.T, path, got string) {
	t.Helper()
	if *updateCommandTreeGolden {
		if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
			t.Fatalf("write golden %s: %v", path, err)
		}
		t.Logf("regenerated %s (%d bytes)", path, len(got))
		return
	}

	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden %s: %v\n"+
			"If this is the first run, generate it with:\n"+
			"  go test ./cmd/tether -run TestCommandTreeInventory -update-command-tree-golden",
			path, err)
	}

	if string(want) != got {
		actual := filepath.Join(t.TempDir(), filepath.Base(path)+".actual")
		_ = os.WriteFile(actual, []byte(got), 0o644)
		t.Errorf("command tree drifted from %s (actual dump written to %s for diffing).\n"+
			"The product CLI command/flag surface changed. Before regenerating the golden, RECONCILE\n"+
			"docs/reviews/simcluster-coverage-inventory.md §2: map every added command/flag to an S-batch\n"+
			"drill or an explicit NOT-COVERED (the deploy-tier no-omission gate). Then regenerate with:\n"+
			"  go test ./cmd/tether -run TestCommandTreeInventory -update-command-tree-golden",
			path, actual)
	}
}

// dumpCommandTree renders a deterministic, line-per-command view of the tree: each command's full path +
// [hidden] bit, then its flag set (local ∪ persistent ∪ inherited, deduped, sorted) with a (hidden) tag on
// Hidden flags (MarkHidden / registerYesRejector). Children are recursed in name order.
func dumpCommandTree(root *cobra.Command) string {
	var lines []string
	var walk func(c *cobra.Command)
	walk = func(c *cobra.Command) {
		seen := map[string]bool{}
		var flags []string
		add := func(fs *pflag.FlagSet) {
			if fs == nil {
				return
			}
			fs.VisitAll(func(f *pflag.Flag) {
				if seen[f.Name] {
					return
				}
				seen[f.Name] = true
				name := "--" + f.Name
				if f.Hidden {
					name += "(hidden)"
				}
				flags = append(flags, name)
			})
		}
		// LocalFlags merges own persistent flags; InheritedFlags walks ancestors' persistent flags.
		add(c.LocalFlags())
		add(c.InheritedFlags())
		sort.Strings(flags)

		hidden := ""
		if c.Hidden {
			hidden = " [hidden]"
		}
		// MINOR-4: emit `flags:` with NO trailing whitespace on an empty flag set — a trailing space made
		// `git diff --cached --check` fail on every flagless command. Append " <flags>" only when non-empty.
		line := c.CommandPath() + hidden + "\tflags:"
		if len(flags) > 0 {
			line += " " + strings.Join(flags, " ")
		}
		lines = append(lines, line)

		subs := append([]*cobra.Command(nil), c.Commands()...)
		sort.Slice(subs, func(i, j int) bool { return subs[i].Name() < subs[j].Name() })
		for _, s := range subs {
			walk(s)
		}
	}
	walk(root)
	return strings.Join(lines, "\n") + "\n"
}
