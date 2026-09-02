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

// gate_registry_test.go — the hand-copied lists that describe the gates must match the gates.
//
// origin: line-2 external review m3 and m24. Everything this increment added carries a reverse
// assertion except the lists that describe the machinery itself:
//
//	m3   .golangci.yml holds a register of function NAMES (10 maintidx sites, 22 unparam sites). Rename
//	     or delete any of them and the exemption stays, excusing nothing, while still reading to the next
//	     person as "this was looked at and accepted". Every other ledger in the repo fails on a stale
//	     entry; these were the exception.
//	m24  the `gates:` target's package list is a hand-typed constant with no reconciliation. Three gates
//	     were outside it, one of which CLAUDE.md's own gate table lists AS a gate — so `make gates`, whose
//	     entire promise is "re-runs every mechanical guard in one shot", did not run it.
//
// Both are the same shape as the defect this whole increment is about: a declaration that nothing checks.

var (
	// Any parenthesised alternation on a `text:` line. Deliberately loose: the config spells these
	// several ways (with and without a receiver-prefix group, with and without a `Function name:`
	// preamble), and a regex tuned to today's exact spelling would go quietly blind the next time one of
	// them is edited — which is the whole failure mode this file exists to prevent. Non-identifier
	// tokens are filtered afterwards instead.
	golangciAltGroupRe = regexp.MustCompile(`\(([A-Za-z_][A-Za-z0-9_|$\\d]*)\)`)
	golangciTextLineRe = regexp.MustCompile(`(?m)^\s*text: ".*"$`)
	// A rule's own path scope. origin: external review M4 — the register lookup must honour it.
	golangciPathLineRe = regexp.MustCompile(`^- path: (.+)$`)
	makeGoTestPkgsRe   = regexp.MustCompile(`(?m)^\tgo test ((?:\./[^\s]+\s*)+)$`)
	// A shell gate set run as its own recipe line (`\tsh test/simcluster/tests/run-all.sh`). Covered by
	// EXACT path, never by directory: the table names the script, and a directory prefix would let any
	// other script under tests/ claim to be run. origin: docs/reviews/test-system-overhaul-plan.md B1.
	makeShLineRe     = regexp.MustCompile(`(?m)^\tsh (test/\S+)$`)
	claudeGatePathRe = regexp.MustCompile("`((?:test|cmd|internal)/[A-Za-z0-9_/.-]+)`")
	funcDeclNameRe   = regexp.MustCompile(`(?m)^func (?:\([^)]*\) )?([A-Za-z_][A-Za-z0-9_]*)`)
	identRe          = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
)

// TestGolangciNameRegistersNameLiveFunctions is m3's reverse assertion: every function named in an
// exclusion rule's alternation must still exist somewhere in the tree.
//
// It deliberately checks EXISTENCE, not that the linter still reports the site. Proving the latter means
// running golangci-lint with the exclusion removed, once per site — and golangci-lint takes a global lock,
// so that is not something a test in this package may do (see the note on `make gates` in CLAUDE.md). What
// it does catch is the rot that actually happens: a rename or a deletion leaving an exemption behind.
func TestGolangciNameRegistersNameLiveFunctions(t *testing.T) {
	root := repoRoot(t)

	cfg, err := os.ReadFile(filepath.Join(root, ".golangci.yml"))
	if err != nil {
		t.Fatalf("read .golangci.yml: %v", err)
	}

	// PARSED AS {path, names} PAIRS. origin: line-2 INDEPENDENT EXTERNAL REVIEW M4.
	//
	// The first version flattened every register into one bag of names and asked only "does a function with
	// this name exist ANYWHERE in the tree". `Run` exists in five packages. So renaming the exemption's
	// actual target — `(*Broker).Run` in internal/broker — left the exemption green forever, because some
	// other package still had a `Run`. The exclusion is scoped by `path:` in the config and the check threw
	// that scope away, which made the check weakest exactly where the config was most careful.
	//
	// Rules are read as blocks: a `- path:` line opens one and its `text:` lines carry the names.
	type register struct {
		path  *regexp.Regexp
		names []string
		line  int
	}
	var registers []register
	var cur *register
	for i, line := range strings.Split(string(cfg), "\n") {
		trimmed := strings.TrimSpace(line)
		if m := golangciPathLineRe.FindStringSubmatch(trimmed); m != nil {
			re, cerr := regexp.Compile(m[1])
			if cerr != nil {
				t.Errorf(".golangci.yml:%d: unparseable path regex %q: %v", i+1, m[1], cerr)
				cur = nil
				continue
			}
			registers = append(registers, register{path: re, line: i + 1})
			cur = &registers[len(registers)-1]
			continue
		}
		if strings.HasPrefix(trimmed, "- ") && !strings.HasPrefix(trimmed, "- path:") {
			cur = nil // a rule with no path: (global text rule) — names in it are not path-scoped
		}
		if !golangciTextLineRe.MatchString(line) {
			continue
		}
		for _, g := range golangciAltGroupRe.FindAllStringSubmatch(line, -1) {
			parts := strings.Split(g[1], "|")
			if len(parts) < 2 {
				continue // not an alternation of names; e.g. a lone capture group
			}
			for _, name := range parts {
				name = strings.TrimSpace(name)
				if !identRe.MatchString(name) {
					continue
				}
				if cur == nil {
					// Unscoped rule: keep the old whole-tree semantics rather than inventing a scope.
					registers = append(registers, register{path: nil, names: []string{name}, line: i + 1})
					continue
				}
				cur.names = append(cur.names, name)
			}
		}
	}
	total := 0
	for _, r := range registers {
		total += len(r.names)
	}
	if total < 20 {
		t.Fatalf("only %d names parsed out of .golangci.yml's exclusion registers — the parser is broken, "+
			"so a clean result below would mean nothing. Expected the maintidx register (10) plus the two "+
			"unparam lists (13 + 9).", total)
	}
	scoped := 0
	for _, r := range registers {
		if r.path != nil {
			scoped += len(r.names)
		}
	}
	if scoped == 0 {
		t.Fatal("no register was parsed WITH a path scope — the block parser is broken and this check has " +
			"silently degraded back to the whole-tree name lookup external review M4 rejected")
	}

	// declaredIn[name] is the set of repo-relative FILES declaring a function of that name, so a register's
	// path scope can be applied to the lookup.
	declaredIn := map[string][]string{}
	err = filepath.WalkDir(root, func(p string, d fs.DirEntry, werr error) error {
		if werr != nil {
			return werr
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "vendor", "node_modules", "testdata":
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
		for _, m := range funcDeclNameRe.FindAllStringSubmatch(string(src), -1) {
			declaredIn[m[1]] = append(declaredIn[m[1]], rel)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}

	seen := map[string]bool{}
	var dead []string
	for _, r := range registers {
		for _, name := range r.names {
			key := name
			if r.path != nil {
				key = r.path.String() + "::" + name
			}
			if seen[key] {
				continue
			}
			seen[key] = true

			files := declaredIn[name]
			if r.path == nil {
				if len(files) == 0 {
					dead = append(dead, name+"  (unscoped rule at .golangci.yml:"+strconv.Itoa(r.line)+")")
				}
				continue
			}
			// The declaration must be inside the rule's own path scope. A same-named function elsewhere
			// does not keep the exemption alive — that was M4 exactly.
			inScope := false
			for _, f := range files {
				if r.path.MatchString(f) {
					inScope = true
					break
				}
			}
			if !inScope {
				detail := "no function of that name anywhere"
				if len(files) > 0 {
					sort.Strings(files)
					detail = "declared only OUTSIDE the scope, in " + strings.Join(slicesCompact(files), ", ")
				}
				dead = append(dead, name+"  (rule path `"+r.path.String()+"` at .golangci.yml:"+
					strconv.Itoa(r.line)+" — "+detail+")")
			}
		}
	}
	sort.Strings(dead)
	if len(dead) > 0 {
		t.Errorf("%d name(s) in .golangci.yml's exclusion registers no longer name any function in the "+
			"tree:\n  %s\n\n"+
			"A dead exemption excuses nothing while still reading as a considered decision. Delete the "+
			"name in the same commit as the rename — that is the rule every other ledger in this repo "+
			"already enforces on itself.", len(dead), strings.Join(dead, "\n  "))
	}
}

// TestGatesTargetCoversEveryGateCLAUDEMdNames is m24's reverse assertion.
//
// CLAUDE.md's gate table is the user-facing answer to "what are this repo's gates". `make gates` is the
// command that claims to run all of them. Nothing connected the two, and they had diverged.
func TestGatesTargetCoversEveryGateCLAUDEMdNames(t *testing.T) {
	root := repoRoot(t)

	mk, err := os.ReadFile(filepath.Join(root, "Makefile"))
	if err != nil {
		t.Fatalf("read Makefile: %v", err)
	}
	// The gates recipe's `go test` line, isolated from the rest of the Makefile.
	gatesIdx := strings.Index(string(mk), "\ngates:")
	if gatesIdx < 0 {
		t.Fatal("the Makefile has no `gates:` target — CLAUDE.md tells people to run it")
	}
	recipe := string(mk)[gatesIdx:]
	if end := strings.Index(recipe[1:], "\n\n"); end > 0 {
		recipe = recipe[:end+1]
	}
	pm := makeGoTestPkgsRe.FindStringSubmatch(recipe)
	if pm == nil {
		t.Fatal("could not parse the `go test` package list out of the gates recipe")
	}
	var covered []string
	for _, p := range strings.Fields(pm[1]) {
		p = strings.TrimPrefix(p, "./")
		p = strings.TrimSuffix(p, "/...")
		covered = append(covered, strings.TrimSuffix(p, "/"))
	}
	if len(covered) < 4 {
		t.Fatalf("parsed only %d packages from the gates recipe — parser broken", len(covered))
	}
	for _, m := range makeShLineRe.FindAllStringSubmatch(recipe, -1) {
		covered = append(covered, strings.TrimSuffix(m[1], "/"))
	}

	claude, err := os.ReadFile(filepath.Join(root, "CLAUDE.md"))
	if err != nil {
		t.Fatalf("read CLAUDE.md: %v", err)
	}
	// The gate table, bounded by its header row and the first line after it that is not a table row.
	body := string(claude)
	hdr := strings.Index(body, "| 闸门 | 位置 | 管什么 |")
	if hdr < 0 {
		t.Fatal("CLAUDE.md no longer has the gate table this test reconciles against")
	}
	table := body[hdr:]
	var rows []string
	for _, line := range strings.Split(table, "\n") {
		// The table is indented inside a bullet, so trim before testing for a row.
		if !strings.HasPrefix(strings.TrimSpace(line), "|") {
			break
		}
		rows = append(rows, line)
	}
	if len(rows) < 6 {
		t.Fatalf("the gate table parsed to %d rows — too few to be the real table", len(rows))
	}

	var uncovered []string
	for _, row := range rows {
		// The LOCATION column only. The table is `| 闸门 | 位置 | 管什么 |`, and the third column routinely
		// names files a gate READS (a scanned document, a golden, a config) rather than a place a gate LIVES.
		//
		// origin: line-2 closure verification. Scanning the whole row made this gate demand that
		// `make gates` run `test/simcluster` because the 确定性 lint row mentions
		// test/simcluster/README.md as part of the docs wire-version SCAN SURFACE. That is a false
		// positive of this gate's own making, and it fired on a documentation edit — which is exactly the
		// friction that teaches people to delete the documentation instead.
		cells := strings.Split(strings.Trim(strings.TrimSpace(row), "|"), "|")
		if len(cells) < 2 {
			continue
		}
		location := cells[1]
		for _, m := range claudeGatePathRe.FindAllStringSubmatch(location, -1) {
			path := m[1]
			// Reduce a file path to its package directory.
			dir := path
			if strings.HasSuffix(path, ".go") || strings.HasSuffix(path, ".md") {
				dir = filepath.Dir(path)
			}
			dir = strings.TrimSuffix(dir, "/")
			ok := false
			for _, c := range covered {
				if dir == c || strings.HasPrefix(dir+"/", c+"/") {
					ok = true
					break
				}
			}
			if !ok {
				uncovered = append(uncovered, dir+"  (from "+path+")")
			}
		}
	}
	sort.Strings(uncovered)
	uncovered = slicesCompact(uncovered)
	if len(uncovered) > 0 {
		t.Errorf("CLAUDE.md's gate table names %d location(s) that `make gates` does not run:\n  %s\n\n"+
			"gates says it \"re-runs every mechanical guard in one shot\". Either add the package to the "+
			"recipe, or take the row out of the table — a gate listed but not run is worse than one that "+
			"is neither, because the table is what people check instead of looking.",
			len(uncovered), strings.Join(uncovered, "\n  "))
	}
}
