package architecture

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// nolint_directive_test.go — every nolint directive must name a linter this repo actually runs.
//
// origin: plan §8 C4, and then line-2 external review F4/GI-3/PF-4/IDG-8 — four lanes found the same
// thing, which is that this file did not exist while `.golangci.yml` already said it did:
//
//	# KNOWN LIMIT: nolintlint does NOT report a directive naming a linter that is not enabled, so a
//	# directive can name a linter this repo never runs and never be examined by anything.
//	# test/architecture/nolint_directive_test.go covers that gap.
//
// So the gate file itself carried a promised guard — the exact class of defect
// test/determinism/promised_guard_test.go exists to stop, in the one file that is itself a gate, and
// invisible to that guard because it only harvests Test identifiers from GO comments and cannot read
// a .yml. Writing it is cheaper than the sentence explaining why it is missing.
//
// WHAT THE GAP IS
// ---------------
// nolintlint's allow-unused:false catches a directive whose linter IS enabled and no longer fires.
// It cannot catch a directive naming `gosec` when gosec is not enabled at all: from its point of view nothing was
// suppressed because nothing was going to be reported. Such a directive is pure noise that reads as a
// considered exemption — and this repo has some, because gosec was reviewed by hand (plan §12) and
// never turned on.
//
// The rule is deliberately "must be enabled", not "must currently suppress something": a directive for
// an enabled linter that stops firing is nolintlint's job, and duplicating it here would make two gates
// disagree about who owns that case.

// nolintDirectiveRe captures the linter list of a nolint directive. golangci-lint accepts a
// comma-separated list of linter names, an optional trailing reason, and (forbidden here by
// require-specific) a bare directive naming nothing.
var nolintDirectiveRe = regexp.MustCompile(`//nolint:([a-zA-Z0-9_,-]+)`)

// enabledLinterRe pulls the enable list out of .golangci.yml. The config is YAML but the shape needed
// here is narrow enough that a line scanner beats adding a YAML dependency to the test tree: entries
// look like `    - bodyclose        # comment` inside the `enable:` block.
func enabledLinters(t *testing.T, root string) map[string]bool {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(root, ".golangci.yml"))
	if err != nil {
		t.Fatalf("read .golangci.yml: %v", err)
	}
	out := map[string]bool{}
	// `default: standard` brings these in without naming them. Hardcoded because golangci-lint defines
	// the set, not this repo.
	//
	// origin: line-2 external review 疑惑 #3. This list used to be injected UNCONDITIONALLY, under a
	// comment claiming that a switch to `default: none` would be wrong "in the SAFE direction". That was
	// backwards. This gate's job is to REJECT directives naming linters the repo does not run; believing
	// five extra linters are enabled makes it more PERMISSIVE, which is the unsafe direction for a gate.
	// Under `default: none`, five stale `//nolint:staticcheck` directives would keep reading as considered
	// exemptions and nothing would say otherwise.
	//
	// So the default mode is now read from the config rather than assumed, and an unrecognised mode is a
	// hard failure rather than a guess. `all` is refused on purpose: this gate cannot enumerate every
	// linter golangci-lint ships, and silently treating `all` as `standard` would be the same permissive
	// mistake in a new costume.
	const standardMode, noneMode, allMode = "standard", "none", "all"
	mode := ""
	for _, line := range strings.Split(string(b), "\n") {
		// Scoped to the `linters:` block's own indentation (two spaces) so a `default:` inside a nested
		// settings map cannot be mistaken for the linter-set selector.
		if rest, ok := strings.CutPrefix(line, "  default:"); ok {
			if i := strings.Index(rest, "#"); i >= 0 {
				rest = rest[:i]
			}
			mode = strings.Trim(strings.TrimSpace(rest), `"'`)
			break
		}
	}
	switch mode {
	case standardMode:
		for _, l := range []string{"errcheck", "govet", "ineffassign", "staticcheck", "unused"} {
			out[l] = true
		}
	case noneMode:
		// Nothing implicit: `enable:` is the whole set.
	case allMode:
		t.Fatalf(".golangci.yml sets `default: all`. This gate cannot enumerate golangci-lint's full " +
			"linter set, so it cannot tell an enabled linter from a typo, and every directive would look " +
			"valid. Either go back to `standard`/`none`, or replace this function with a parser for " +
			"`golangci-lint linters` output — do NOT weaken the check to make it pass.")
	default:
		t.Fatalf(".golangci.yml has no recognised `default:` under `linters:` (parsed %q). golangci-lint's "+
			"implicit linter set is what tells this gate which names are legitimate; guessing it would make "+
			"the gate permissive about exactly the directives it exists to catch.", mode)
	}

	inEnable := false
	for _, line := range strings.Split(string(b), "\n") {
		trimmed := strings.TrimSpace(line)
		switch {
		case trimmed == "enable:":
			inEnable = true
			continue
		case inEnable && strings.HasPrefix(trimmed, "- "):
			name := strings.TrimSpace(strings.TrimPrefix(trimmed, "- "))
			if i := strings.Index(name, "#"); i >= 0 {
				name = strings.TrimSpace(name[:i])
			}
			if name != "" {
				out[name] = true
			}
		case inEnable && trimmed != "" && !strings.HasPrefix(trimmed, "#") &&
			!strings.HasPrefix(line, "     "):
			// Dedented to a sibling key (`settings:`, `exclusions:`) — the enable block is over.
			inEnable = false
		}
	}
	if len(out) < 10 {
		t.Fatalf("only parsed %d enabled linters from .golangci.yml; the parser is broken and every "+
			"directive would look valid", len(out))
	}
	return out
}

// TestEnabledLinterSetIsParsedNotAssumed pins the two things this gate's verdict rests on: the default
// mode actually came from the file, and the enabled set is the size it is.
//
// origin: line-2 external review 疑惑 #3. State plainly what each half does and does not prove, because
// the review's objection was that an exact number "未必能证明安全强度没有下降" — which is correct, and
// pretending otherwise is worse than the gap:
//
//   - The MODE assertion does prove something: `standard` vs `none` decides whether five linter names are
//     legitimate, and reading it from the file removes the assumption that was wrong before.
//   - The COUNT proves only VISIBILITY. Swapping one linter for another leaves it unchanged, so it cannot
//     show that suppression strength held. What it does is force any change to the enabled set to be typed
//     here too, in the same commit, where a human is looking at it. That is the same bargain every ledger
//     in this repo makes, and it is worth stating rather than overselling.
func TestEnabledLinterSetIsParsedNotAssumed(t *testing.T) {
	root := repoRoot(t)
	b, err := os.ReadFile(filepath.Join(root, ".golangci.yml"))
	if err != nil {
		t.Fatalf("read .golangci.yml: %v", err)
	}
	if !strings.Contains(string(b), "\n  default: standard\n") {
		t.Fatalf("`linters.default` is no longer the literal `  default: standard`.\n\n" +
			"enabledLinters() injects golangci-lint's five standard linters ONLY in that mode. If the mode " +
			"genuinely changed, update both this assertion and the switch in enabledLinters — do not delete " +
			"this check, because a silently-wrong mode makes the nolint gate permissive about the exact " +
			"directives it exists to catch.")
	}

	enabled := enabledLinters(t, root)
	// Sanity: the five standard names must be present under `standard` mode, and they must come from the
	// injection rather than from `enable:` (golangci-lint rejects re-enabling a default linter).
	for _, l := range []string{"errcheck", "govet", "ineffassign", "staticcheck", "unused"} {
		if !enabled[l] {
			t.Errorf("standard linter %q missing from the parsed set — the mode switch did not fire", l)
		}
	}

	const expectedEnabled = 22
	if len(enabled) != expectedEnabled {
		names := make([]string, 0, len(enabled))
		for n := range enabled {
			names = append(names, n)
		}
		sort.Strings(names)
		t.Errorf("the enabled linter set has %d entries, expected %d:\n  %s\n\n"+
			"This count does NOT prove suppression strength held — a one-for-one swap leaves it unchanged. "+
			"It proves the change was VISIBLE: update the constant in the same commit that edits "+
			"`.golangci.yml`, and say in the message why the new set is not weaker.",
			len(enabled), expectedEnabled, strings.Join(names, "\n  "))
	}
}

func TestNolintDirectivesNameEnabledLinters(t *testing.T) {
	root := repoRoot(t)
	enabled := enabledLinters(t, root)

	var offenders []string
	scanned := 0
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "vendor", "node_modules", "testdata", "bin":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(d.Name(), ".go") {
			return nil
		}
		src, rerr := os.ReadFile(p)
		if rerr != nil {
			return rerr
		}
		rel, _ := filepath.Rel(root, p)
		rel = filepath.ToSlash(rel)
		for i, line := range strings.Split(string(src), "\n") {
			loc := nolintDirectiveRe.FindStringSubmatchIndex(line)
			if loc == nil {
				continue
			}
			// A directive is either TRAILING (code to its left) or OWN-LINE (`//nolint:...` starting the
			// line, which golangci-lint applies to the declaration or block that follows).
			//
			// The first version of this gate counted only the trailing form, with a comment arguing that
			// requiring code to the left "separates a directive from a sentence about directives
			// structurally". It does — and it also discarded 25 of the tree's 30 real directives, including
			// every one of the 16 `//nolint:dupl` that this same increment created by moving dupl
			// exemptions out of .golangci.yml. A gate built to check exemptions was blind to 83% of them,
			// and the change that was supposed to make those exemptions MORE visible moved them into the
			// blind spot. origin: line-2 closure verification C3.
			//
			// The prose problem the first version was solving is real, so it is solved narrowly instead:
			// an own-line directive counts only when the `//nolint:` is the very first thing on the line
			// (after indentation). Prose that mentions the token mid-sentence — "there used to be a
			// //nolint:unused here" — never satisfies that, and neither does a token inside a regex
			// literal or an error string, because those have code to their left and are caught by the
			// trailing branch's own check below.
			leftOfDirective := strings.TrimSpace(line[:loc[0]])
			isOwnLine := leftOfDirective == ""
			isTrailing := leftOfDirective != "" && !strings.HasPrefix(leftOfDirective, "//")
			if !isOwnLine && !isTrailing {
				continue // a mention inside a comment's prose, not a directive
			}
			m := []string{line[loc[0]:loc[1]], line[loc[2]:loc[3]]}
			scanned++
			for _, name := range strings.Split(m[1], ",") {
				if name = strings.TrimSpace(name); name != "" && !enabled[name] {
					offenders = append(offenders,
						rel+":"+strconv.Itoa(i+1)+"  //nolint:"+name)
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}

	sort.Strings(offenders)
	if len(offenders) > 0 {
		t.Errorf("%d //nolint directive(s) name a linter this repo does not run:\n  %s\n\n"+
			"nolintlint cannot see these: with the linter disabled, nothing was going to be reported, so\n"+
			"from its point of view nothing was suppressed. The directive is pure noise that reads to the\n"+
			"next person as a considered exemption. Delete it, or enable the linter.",
			len(offenders), strings.Join(offenders, "\n  "))
	}

	// Non-vacuity as an EXACT count, in both directions.
	//
	// origin: line-2 closure verification C3, which flagged `scanned == 0` as the weakest possible floor —
	// the same defect IDG-9 had already fixed in the sibling TLS gate during this increment, left in place
	// here. `== 0` only catches TOTAL failure; it was satisfied by 5 of 31 directives, which is exactly how
	// the trailing-only bug survived.
	//
	// The number is the enforcement. A new directive must be typed here as well, which is the friction that
	// makes the suppression surface countable — plan §14.1 enumerates all 30 by name for the same reason.
	// 30 → 34: upgrade-safety external-review fix round added four nilerr
	// exemptions inside the host-wide flock closures (their error return
	// carries LOCK failures only; a bad marker/self-hash is contractually
	// "nothing to do", not an error). Enumerated in line2-plan §14.1.
	const expectedDirectives = 34
	if scanned != expectedDirectives {
		t.Errorf("the scan found %d //nolint directive(s), expected exactly %d.\n\n"+
			"MORE: a new exemption was added. Add it to plan §14.1's enumeration and bump this constant in "+
			"the same commit — an exemption nobody counted is an exemption nobody reviewed.\n"+
			"FEWER: either an exemption was removed (bump the constant DOWN and lock the win in) or the "+
			"scanner stopped recognising a form it used to see, which would make every 'no offenders' "+
			"verdict above meaningless. The trailing-only bug this count replaced was exactly that: it saw "+
			"5 of 30 and reported success.", scanned, expectedDirectives)
	}
}
