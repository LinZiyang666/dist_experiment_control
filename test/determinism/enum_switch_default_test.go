package determinism

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// enum_switch_default_test.go — a switch over one of THIS repo's enums may not carry a `default:`.
//
// origin: line-2 §7 / plan §1 订正 2. `.golangci.yml` sets exhaustive's
// `default-signifies-exhaustive: true`, which is necessary — without it every default-carrying switch
// in the tree reports and the linter becomes noise. It also opens a one-line escape hatch, and the
// escape hatch was measured rather than imagined:
//
//	1. baseline: exhaustive reports cmd/tether/cluster_doctor_online.go missing TopoUnreported/TopoConverged
//	2. delete `case TopoBehind:` and `case TopoUnknownAction:` — i.e. re-inject the batch-C external
//	   review MAJOR, where the doctor reported PASS on a cluster that was not converging — and
//	   exhaustive catches it immediately
//	3. now add a bare `default:` to that switch. exhaustive goes quiet. Re-inject the SAME defect from
//	   step 2 and exhaustive stays quiet: 0 issues, gate green, defect shipped.
//
// So the cheapest way to clear an exhaustive violation is also the way to permanently blind the check
// on that switch. The whole value argument for enabling exhaustive rests on it catching that class of
// defect, which makes this guard the other half of the linter rather than an extra.
//
// WHY IT MATCHES ON CONSTANT NAMES
// --------------------------------
// Resolving a switch tag to its declared type needs full type information (go/packages over every
// build tag), which is slow and, worse, gives a different answer depending on which tags are on. This
// scans for switches whose CASES name a constant from one of the enum families below — the same thing
// a reader does — and it is deliberately conservative: a family has to be registered here to be
// guarded, so the failure mode is a missed switch rather than a false accusation. enumFamilies is the
// list to grow when a new enum appears.
//
// A `default:` that is genuinely required (a wire-facing switch that must tolerate an unknown value
// from a newer peer, say) goes in allowedEnumDefaults with a reason, and that ledger has the same
// reverse assertion as every other one here: an entry that no longer names a real site fails.

// enumFamilies maps a family label to the constant-name prefixes that belong to it. A switch is
// considered "over this enum" when at least one of its case expressions is a selector or identifier
// whose name starts with one of these prefixes.
// Every closed enum in the tree, enumerated from `= iota` declarations. Prefixes only — a family is
// recognised when a case names a constant starting with one of them.
//
// origin: line-2 external review IDG-3 / PI-13. The first version listed THREE families and the tree
// has thirteen, so the `default:` escape hatch stayed open on ten enums while the gate reported green —
// and a review lane, after completing the list, found five production switches still carrying one.
// A registry that covers 3/13 is worse than none, because its green is read as coverage.
//
// TestEnumFamiliesCoverEveryIotaEnum below re-derives this list from source and fails when an enum
// appears without an entry, so the 3/13 state cannot recur silently.
var enumFamilies = map[string][]string{
	"agent.openOutcome":            {"openState", "openPort", "opened"},
	"agent.transferMode":           {"mode"},
	"authcallout.role":             {"role"},
	"broker.admitRole":             {"admit"},
	"broker.cutoverAction":         {"cutover"},
	"broker.ledgerDisposition":     {"ledgerLeave", "ledgerReplay", "ledgerSynthesize", "ledgerSite"},
	"broker.passAuthority":         {"authority"},
	"cli.Source":                   {"Source"},
	"clusterupgrade.AgentPresence": {"Agent"},
	"main.capsStatus":              {"caps"},
	"natsconf.RenderIntent":        {"Intent"},
	"natsconf.TopoState":           {"Topo"},
	"spawnsafe.fsKind":             {"kind"},
	"spawnsafe.probeState":         {"st"},
	"spawnsafe.Mode":               {"Mode"},
}

// allowedEnumDefaults are the sites where a `default:` is correct, keyed by "file:ENCLOSING FUNCTION".
// Empty, and the failure text is written expecting it to stay that way: within one binary an enum is
// closed, so a default is a way of not deciding.
//
// KEYED BY FUNCTION, NOT BY LINE. origin: line-2 closure verification §6 B4. This shipped keyed by
// "file:line" — the identical scheme that review M15 had just finished REMOVING from the TLS ledger in this
// same increment, for the identical reason: inserting one comment line above the site invalidates the key,
// so the gate fires with a message claiming the ledgered site "no longer exists" when it is right there.
// The map being empty is why nobody noticed: a broken key scheme in a ledger with no entries has no
// symptoms until the first entry, at which point the person adding it inherits a trap.
//
// Fixing an empty ledger looks like wasted work and is the opposite: the cost of the wrong scheme is paid
// by whoever adds entry number one, and they will have no reason to suspect the mechanism.
var allowedEnumDefaults = map[string]string{}

// enumConstPrefixes flattens enumFamilies for lookup, remembering which family each prefix came from.
func enumConstPrefixes() map[string]string {
	out := map[string]string{}
	for family, prefixes := range enumFamilies {
		for _, p := range prefixes {
			out[p] = family
		}
	}
	return out
}

// caseEnumFamily reports which enum family a case clause's expressions belong to ("" = none).
func caseEnumFamily(cc *ast.CaseClause, prefixes map[string]string) string {
	for _, e := range cc.List {
		var name string
		switch v := e.(type) {
		case *ast.SelectorExpr: // natsconf.TopoStuck
			name = v.Sel.Name
		case *ast.Ident: // ledgerLeave
			name = v.Name
		default:
			continue
		}
		for p, family := range prefixes {
			if strings.HasPrefix(name, p) {
				return family
			}
		}
	}
	return ""
}

// enumMembers scans the tree for `= iota` blocks and returns, per family label, the full set of member
// names. A family is matched to a block by prefix, the same way cases are.
//
// origin: line-2 external review IDG-3, second pass. The first rule here was "a switch over a repo enum
// may not carry a `default:` at all", and applying it exposed five production switches whose default was
// CORRECT — each carried a real semantic (the zero value IS the owner role; IntentPreserve IS the
// fallback; deny IS the safe answer for an unknown auth role). Enumerating their missing cases left them
// with both full coverage AND a defensive default, which is the best available shape — and the gate
// still reddened, because the rule was measuring the wrong thing.
//
// The risk was never `default:`. It is a `default:` that HIDES a missing case: with
// default-signifies-exhaustive:true, adding one silences exhaustive on that switch forever, so the next
// enum member joins by inheriting whatever the fallback happens to do. A switch that names every member
// and then keeps a default has nothing hidden — the next member added still reddens exhaustive, because
// exhaustive counts cases, not defaults.
//
// So the rule is now: a default is allowed IFF every member of the enum appears as a case.
func enumMembers(t *testing.T, root string, prefixes map[string]string) map[string]map[string]bool {
	t.Helper()
	out := map[string]map[string]bool{}
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "vendor", "node_modules", "testdata", "test":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(d.Name(), ".go") || strings.HasSuffix(d.Name(), "_test.go") {
			return nil
		}
		fset := token.NewFileSet()
		f, perr := parser.ParseFile(fset, p, nil, 0)
		if perr != nil {
			return nil
		}
		ast.Inspect(f, func(n ast.Node) bool {
			gd, ok := n.(*ast.GenDecl)
			if !ok || gd.Tok != token.CONST {
				return true
			}
			isIota := false
			var names []string
			for _, spec := range gd.Specs {
				vs, ok := spec.(*ast.ValueSpec)
				if !ok {
					continue
				}
				for _, v := range vs.Values {
					if id, ok := v.(*ast.Ident); ok && id.Name == "iota" {
						isIota = true
					}
				}
				for _, nm := range vs.Names {
					if nm.Name != "_" {
						names = append(names, nm.Name)
					}
				}
			}
			if !isIota || len(names) == 0 {
				return true
			}
			// Attribute the block to a family by matching its FIRST member against the prefix table.
			for _, nm := range names {
				for pre, family := range prefixes {
					if strings.HasPrefix(nm, pre) {
						if out[family] == nil {
							out[family] = map[string]bool{}
						}
						for _, member := range names {
							out[family][member] = true
						}
						return true
					}
				}
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walk for enum members: %v", err)
	}
	return out
}

func TestNoDefaultOnRepoEnumSwitch(t *testing.T) {
	root := repoRoot(t)
	prefixes := enumConstPrefixes()
	members := enumMembers(t, root, prefixes)

	type site struct {
		key, family string
		uncovered   []string
	}
	var offenders []site
	seen := map[string]bool{}
	guarded := 0
	hitsByFamily := map[string]int{}

	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "vendor", "node_modules", "testdata", "test":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(d.Name(), ".go") || strings.HasSuffix(d.Name(), "_test.go") {
			return nil
		}
		rel, rerr := filepath.Rel(root, p)
		if rerr != nil {
			return rerr
		}
		rel = filepath.ToSlash(rel)

		fset := token.NewFileSet()
		f, perr := parser.ParseFile(fset, p, nil, 0)
		if perr != nil {
			t.Fatalf("parse %s: %v", rel, perr)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			sw, ok := n.(*ast.SwitchStmt)
			if !ok || sw.Body == nil {
				return true
			}
			family := ""
			hasDefault := false
			named := map[string]bool{}
			for _, stmt := range sw.Body.List {
				cc, ok := stmt.(*ast.CaseClause)
				if !ok {
					continue
				}
				if cc.List == nil { // `default:` has a nil expression list
					hasDefault = true
					continue
				}
				if family == "" {
					family = caseEnumFamily(cc, prefixes)
				}
				for _, e := range cc.List {
					switch v := e.(type) {
					case *ast.SelectorExpr:
						named[v.Sel.Name] = true
					case *ast.Ident:
						named[v.Name] = true
					}
				}
			}
			if family == "" {
				return true
			}
			guarded++
			hitsByFamily[family]++
			// Keyed by enclosing function, not by line — see allowedEnumDefaults. A line-keyed ledger is
			// invalidated by a comment inserted above the site, which is review M15's finding applied here
			// before this ledger has its first entry.
			key := rel + ":" + enclosingFuncName(f, sw.Pos())
			seen[key] = true
			// A default is fine when every member is also named: exhaustive still reddens on the NEXT
			// member, because it counts cases. It is only a problem when it is covering for a gap.
			var uncovered []string
			for member := range members[family] {
				if !named[member] {
					uncovered = append(uncovered, member)
				}
			}
			sort.Strings(uncovered)
			if hasDefault && len(uncovered) > 0 && allowedEnumDefaults[key] == "" {
				offenders = append(offenders, site{key: key, family: family, uncovered: uncovered})
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}

	sort.Slice(offenders, func(i, j int) bool { return offenders[i].key < offenders[j].key })
	if len(offenders) > 0 {
		var lines []string
		for _, o := range offenders {
			lines = append(lines, o.key+"  (switch over "+o.family+"; unnamed: "+strings.Join(o.uncovered, ", ")+")")
		}
		t.Errorf("%d switch(es) over a repo enum rely on `default:` to cover a member they never name:\n  %s\n\n"+
			"With exhaustive's default-signifies-exhaustive:true, that default makes the linter stop "+
			"checking this switch FOREVER — measured: with a default in place, re-injecting the batch-C\n"+
			"doctor defect (missing TopoBehind/TopoUnknownAction, reporting PASS on a non-converging\n"+
			"cluster) produces zero reports. Enumerate the missing cases instead. If a default really is\n"+
			"required, add the site to allowedEnumDefaults WITH a reason.",
			len(offenders), strings.Join(lines, "\n  "))
	}

	// Reverse assertion: an allow-list entry that no longer names a guarded switch is dead weight, and
	// a dead entry is how an allow-list turns into an allow-everything.
	var stale []string
	for key := range allowedEnumDefaults {
		if !seen[key] {
			stale = append(stale, key)
		}
	}
	sort.Strings(stale)
	if len(stale) > 0 {
		t.Errorf("%d allowedEnumDefaults entr(y/ies) no longer name a switch over a repo enum "+
			"(moved, deleted, or the default was removed):\n  %s\n\nDelete them.",
			len(stale), strings.Join(stale, "\n  "))
	}

	// Non-vacuity PER FAMILY, not one global floor.
	//
	// origin: line-2 closure verification. The floor was `guarded == 0` plus `guarded < 5`, over a scan
	// that reaches ~18 production switches across 15 registered families. Both are satisfied by a prefix
	// list that has rotted down to one working family: five switches over natsconf.TopoState alone clear
	// them, and the other fourteen families could stop being recognised entirely with the gate still
	// green. A global count cannot distinguish "the walker works" from "one family's prefixes happen to
	// still match".
	//
	// Per-family is the assertion that means what the global one was pretending to: every family in
	// enumFamilies must be reached by at least one real switch, so a prefix that stops matching is named.
	if guarded == 0 {
		t.Fatal("the scan found NO switch over any registered enum — the prefix list or the walker is " +
			"broken, and every default would pass unnoticed")
	}
	// The families that HAVE switches today, pinned by name. A family with no switch is not a defect —
	// TestEnumFamiliesCoverEveryIotaEnum requires every iota enum to be REGISTERED, and most of this repo's
	// enums are consumed with `if`/`==` rather than `switch`. Registering one before its first switch is
	// correct: the coverage is then already in place when somebody writes it.
	//
	// So the floor cannot be "every family must be hit" — that assertion and the coverage assertion together
	// would demand a switch for every enum in the tree. (It was written that way first and immediately
	// reported 7 of 15 families "unreached", all of which were fine.)
	//
	// What IS assertable is the set that is hit TODAY. If one of these stops being hit, either its prefixes
	// rotted or its switches were deleted, and both need a person. The reverse half matters just as much: a
	// family NOT in this list that starts being hit means somebody wrote the first switch over that enum, and
	// the gate says so — which is the moment its coverage becomes load-bearing.
	familiesWithSwitches := map[string]bool{
		"agent.openOutcome":        true,
		"authcallout.role":         true,
		"broker.admitRole":         true,
		"broker.cutoverAction":     true,
		"broker.ledgerDisposition": true,
		"main.capsStatus":          true,
		"natsconf.RenderIntent":    true,
		"natsconf.TopoState":       true,
	}
	var lostCoverage, newlySwitched []string
	for family := range enumFamilies {
		hit := hitsByFamily[family] > 0
		switch {
		case familiesWithSwitches[family] && !hit:
			lostCoverage = append(lostCoverage, family)
		case !familiesWithSwitches[family] && hit:
			newlySwitched = append(newlySwitched, family)
		}
	}
	sort.Strings(lostCoverage)
	sort.Strings(newlySwitched)
	if len(lostCoverage) > 0 {
		t.Errorf("%d family(ies) that had switches now match NONE:\n  %s\n\n"+
			"Either the constant-name prefixes in enumFamilies no longer match how the constants are spelled "+
			"— in which case those switches are UNGUARDED while this gate reports green — or the switches "+
			"were deleted. The global floor this replaced (`guarded >= 5`) was satisfied by "+
			"natsconf.TopoState's five switches on its own, so it could not see this at all.",
			len(lostCoverage), strings.Join(lostCoverage, "\n  "))
	}
	if len(newlySwitched) > 0 {
		t.Errorf("%d family(ies) are now matched by a switch but are not in familiesWithSwitches:\n  %s\n\n"+
			"Somebody wrote the first switch over that enum. Add it to the list in the same commit: from now "+
			"on its coverage is load-bearing, and this is the assertion that will notice if it stops.",
			len(newlySwitched), strings.Join(newlySwitched, "\n  "))
	}
}

// TestEnumFamiliesCoverEveryIotaEnum keeps the family table from silently under-covering the tree.
//
// origin: line-2 external review IDG-3 / PI-13. enumFamilies shipped with THREE entries while the tree
// had thirteen closed enums, so the escape hatch stayed open on ten of them while the gate reported
// green. The count is the whole problem: a registry at 3/13 is worse than no registry, because its green
// is read as coverage. Re-deriving the denominator from source is the only way that cannot drift.
func TestEnumFamiliesCoverEveryIotaEnum(t *testing.T) {
	root := repoRoot(t)
	prefixes := enumConstPrefixes()
	members := enumMembers(t, root, prefixes)

	// Every `= iota` const block in prod code, by the name of its first member.
	var unregistered []string
	total := 0
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "vendor", "node_modules", "testdata", "test":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(d.Name(), ".go") || strings.HasSuffix(d.Name(), "_test.go") {
			return nil
		}
		fset := token.NewFileSet()
		f, perr := parser.ParseFile(fset, p, nil, 0)
		if perr != nil {
			return nil
		}
		rel, _ := filepath.Rel(root, p)
		ast.Inspect(f, func(n ast.Node) bool {
			gd, ok := n.(*ast.GenDecl)
			if !ok || gd.Tok != token.CONST {
				return true
			}
			isIota, first := false, ""
			for _, spec := range gd.Specs {
				vs, ok := spec.(*ast.ValueSpec)
				if !ok {
					continue
				}
				for _, v := range vs.Values {
					if id, ok := v.(*ast.Ident); ok && id.Name == "iota" {
						isIota = true
					}
				}
				if first == "" && len(vs.Names) > 0 {
					first = vs.Names[0].Name
				}
			}
			if !isIota || first == "" {
				return true
			}
			total++
			for pre := range prefixes {
				if strings.HasPrefix(first, pre) {
					return true
				}
			}
			unregistered = append(unregistered,
				filepath.ToSlash(rel)+":"+strconv.Itoa(fset.Position(gd.Pos()).Line)+
					"  (first member "+first+")")
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}

	sort.Strings(unregistered)
	if len(unregistered) > 0 {
		t.Errorf("%d of %d iota enum(s) have no entry in enumFamilies:\n  %s\n\n"+
			"An unregistered enum is invisible to TestNoDefaultOnRepoEnumSwitch, so a `default:` on a "+
			"switch over it silences exhaustive permanently and the next member added inherits the "+
			"fallback. Add a prefix to enumFamilies.",
			len(unregistered), total, strings.Join(unregistered, "\n  "))
	}
	if total < 10 {
		t.Errorf("only found %d iota enums in prod code; the scan is broken (measured: 13)", total)
	}
	if len(members) < 10 {
		t.Errorf("enumMembers resolved only %d families; a family with no members makes every switch "+
			"over it pass vacuously", len(members))
	}
}

// enclosingFuncName returns the name of the function containing pos, or "<file-level>" for a switch inside
// a package-level declaration. Methods are spelled "Recv.Method" so two same-named methods on different
// types do not collide.
//
// origin: line-2 closure verification §6 B4 — the key scheme for allowedEnumDefaults, chosen to be immune
// to a comment inserted above the site. Same shape as the sibling helper in
// test/architecture/tls_verify_pairing_test.go; the two ledgers are in different packages, so sharing the
// function would mean a third package existing only to hold it.
func enclosingFuncName(f *ast.File, pos token.Pos) string {
	for _, decl := range f.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Pos() > pos || fn.End() < pos {
			continue
		}
		name := fn.Name.Name
		if fn.Recv != nil && len(fn.Recv.List) > 0 {
			t := fn.Recv.List[0].Type
			if star, isStar := t.(*ast.StarExpr); isStar {
				t = star.X
			}
			if id, isIdent := t.(*ast.Ident); isIdent {
				name = id.Name + "." + name
			}
		}
		return name
	}
	return "<file-level>"
}
