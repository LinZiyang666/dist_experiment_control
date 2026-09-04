package architecture

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// inbox_prefix_pairing_test.go — the private inbox prefix and its CONNECT marker travel
// together, always.
//
// origin: prerelease audit round 2, A-F1 (the disjoint-roots redesign); both halves and
// the failure-mode description corrected by increment 2 internal review
// (pairing-sweep/F2+F3, doc-truth/L12-F1+F2, marker-channel/F3+F5, empirical
// mut-admission/F5+F7).
//
// A connection that sets one without the other is broken:
//
//   - prefix without marker → auth_callout hands it the pre-cutover `_INBOX.>` grant,
//     which does not cover auth.InboxRoot at all, so every subscription to its own inbox
//     is REFUSED and every request times out;
//   - marker without prefix → it asks for the per-identity grant and then subscribes
//     shallow, outside what it was granted.
//
// THE FAILURE IS LOUD. Four places in this repository — including the previous version
// of this very comment — claimed a half-wired connection "times out with nothing on the
// wire and nothing in any log". Measured, it does not: the server refuses the SUBSCRIBE,
// answers `-ERR 'Permissions Violation for Subscription to "…"'`, logs it, and nats.go
// hands it to the async error handler. The correction matters because that false premise
// was the stated reason for guarding this with a source-level gate INSTEAD of a runtime
// check — and natsinbox.Connect now does the runtime check too, precisely because the
// condition is observable.
//
// This has already happened once in this repo for the weaker single-option version of
// the same rule: `internal/cli/completion_transport.go` is a SECOND production connection
// builder that was missed when the derived prefix landed, and every shell completion
// silently returned nothing (round 2, A-F2). One helper, one gate.

// gate-control: TestInboxPrefixPairingGateCatchesEitherHalf

// inboxHalf names which of the two coupled options a call site set.
type inboxHalf struct {
	file string
	half string // "prefix" | "marker"
}

// inboxHalfSites finds every production site that sets EITHER half of the pairing.
//
// BOTH HALVES ARE SCANNED — origin: increment 2 internal review, pairing-sweep/F2. The
// first version looked only for nats.CustomInboxPrefix, so deleting the marker line from
// the helper left `make gates` entirely green while every upgraded client silently
// dropped back to the shared inbox. CLAUDE.md's gate table said this row guarded the
// PAIRING; it guarded one side of it.
//
// The prefix scan matches on the SELECTOR NAME rather than on `nats.` — an aliased or
// dot import defeated the package-name check, which is the cheapest possible way around
// a gate (mut-admission/F6). A false positive from some unrelated `x.CustomInboxPrefix`
// is a loud, one-line fix; a false negative is a silent disclosure.
//
// One switch arm per syntactic shape the gate must see; the enumeration IS the point,
// so it stays in one place rather than being split for length.
func inboxHalfSites(t *testing.T, root string) []inboxHalf {
	t.Helper()
	var sites []inboxHalf
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, werr error) error {
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
		if !strings.HasSuffix(d.Name(), ".go") || strings.HasSuffix(d.Name(), "_test.go") {
			return nil
		}
		src, rerr := os.ReadFile(p)
		if rerr != nil {
			return rerr
		}
		rel, _ := filepath.Rel(root, p)
		rel = filepath.ToSlash(rel)
		f, perr := parser.ParseFile(token.NewFileSet(), p, src, 0)
		if perr != nil {
			return nil // not our business; the build gate owns syntax
		}
		ast.Inspect(f, func(n ast.Node) bool {
			switch node := n.(type) {
			case *ast.SelectorExpr:
				// `<anything>.CustomInboxPrefix` — as a call, a value, anything.
				if node.Sel.Name == "CustomInboxPrefix" {
					sites = append(sites, inboxHalf{rel, "prefix"})
				}
				// `<anything>.InboxCapableMarker` — the marker half.
				if node.Sel.Name == "InboxCapableMarker" {
					sites = append(sites, inboxHalf{rel, "marker"})
				}
			case *ast.Ident:
				// A dot-import, or a file INSIDE the declaring package referring to it
				// unqualified. The declaration itself lands here too, which is why
				// internal/auth/permissions.go is a named markerReader rather than a
				// special case in the scanner: an exemption someone can read beats a
				// silent carve-out in the matcher.
				if node.Name == "CustomInboxPrefix" || node.Name == "InboxCapableMarker" {
					half := "prefix"
					if node.Name == "InboxCapableMarker" {
						half = "marker"
					}
					sites = append(sites, inboxHalf{rel, half})
				}
			case *ast.KeyValueExpr:
				// `nats.Options{InboxPrefix: …}` — the struct field behind the option.
				if k, ok := node.Key.(*ast.Ident); ok && k.Name == "InboxPrefix" {
					sites = append(sites, inboxHalf{rel, "prefix"})
				}
			case *ast.AssignStmt:
				// `o.InboxPrefix = …` — the same field, written directly.
				for _, lhs := range node.Lhs {
					if s, ok := lhs.(*ast.SelectorExpr); ok && s.Sel.Name == "InboxPrefix" {
						sites = append(sites, inboxHalf{rel, "prefix"})
					}
				}
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	sort.Slice(sites, func(i, j int) bool {
		if sites[i].file != sites[j].file {
			return sites[i].file < sites[j].file
		}
		return sites[i].half < sites[j].half
	})
	return sites
}

// theOneHelper is the single production file allowed to SET either half.
const theOneHelper = "internal/natsinbox/natsinbox.go"

// markerReaders are the production files allowed to NAME the marker without setting it.
// Each entry is a reason, not a waiver: reading the marker is the broker side of the
// same contract, and there is exactly one place that decides on it.
// A DEAD ENTRY IS A FAILURE, not a harmless leftover: an exemption nobody needs any
// more reads as "this was considered and allowed" to the next person, which is exactly
// how a real violation gets waved through later.
var markerReaders = map[string]string{
	"internal/auth/permissions.go":    "declares the constant and documents it",
	"internal/authcallout/handler.go": "the broker side: compares the CONNECT Username against it",
}

func TestInboxPairingIsSetOnlyByItsHelper(t *testing.T) {
	sites := inboxHalfSites(t, repoRoot(t))
	var prefixes, markers int
	for _, s := range sites {
		switch s.half {
		case "prefix":
			prefixes++
		case "marker":
			markers++
		}
	}
	if prefixes == 0 || markers == 0 {
		t.Fatalf("found %d prefix sites and %d marker sites — the scan has gone blind, or the "+
			"private inbox has been removed and this gate now asserts nothing", prefixes, markers)
	}
	// No dead exemptions: every markerReaders entry must still name a file that names
	// the marker. See the map's doc comment for why this is a failure and not tidying.
	seen := map[string]bool{}
	for _, s := range sites {
		if s.half == "marker" {
			seen[s.file] = true
		}
	}
	for file, why := range markerReaders {
		if !seen[file] {
			t.Errorf("markerReaders lists %s (%q) but that file no longer names "+
				"auth.InboxCapableMarker. Remove the entry — a stale exemption reads as "+
				"\"considered and allowed\" to whoever adds the next real violation.", file, why)
		}
	}
	// THE HELPER MUST SET BOTH HALVES. Without this the gate only answers "who else sets
	// one", and deleting a line from the helper itself — the single change that silently
	// drops every upgraded client back to the shared inbox — passes cleanly.
	//
	// origin: increment 2 internal review, mut-admission/F5, and then again on the
	// rewritten gate: the first rewrite added the marker SCAN but still exempted the
	// helper wholesale, so the mutation it was written for still came out green.
	helperHalves := map[string]bool{}
	for _, s := range sites {
		if s.file == theOneHelper {
			helperHalves[s.half] = true
		}
	}
	for _, half := range []string{"prefix", "marker"} {
		if !helperHalves[half] {
			t.Errorf("%s does not set the %q half of the inbox pairing.\n\n"+
				"Both options must come from this one function, because a connection that carries "+
				"only one of them is broken: prefix without marker is refused its own inbox by the "+
				"pre-cutover grant, marker without prefix subscribes outside what it was granted. "+
				"Every caller in the tree trusts this function to return the pair.",
				theOneHelper, half)
		}
	}

	for _, s := range sites {
		if s.file == theOneHelper {
			continue
		}
		if s.half == "marker" {
			if _, ok := markerReaders[s.file]; ok {
				continue // named, with a reason, above
			}
			t.Errorf("%s names auth.InboxCapableMarker.\n\n"+
				"Setting the marker without the matching prefix asks auth_callout for the "+
				"per-identity grant and then subscribes outside it. Go through "+
				"natsinbox.InboxConnectOptions (or natsinbox.Connect), which sets both halves. If "+
				"this file legitimately only READS the marker, add it to markerReaders with the "+
				"reason.", s.file)
			continue
		}
		t.Errorf("%s sets the private inbox prefix directly.\n\n"+
			"Use natsinbox.InboxConnectOptions — note the package: it is NOT in internal/auth, "+
			"because internal/cluster imports auth and layering rule L-2 keeps the raft layer "+
			"NATS-free. The helper returns the prefix AND the CONNECT marker together. A "+
			"connection with the prefix but no marker is handed the pre-cutover `_INBOX.>` "+
			"grant, which does not cover auth.InboxRoot at all, so every subscription to its "+
			"own inbox is refused and every request times out.", s.file)
	}
}

// gate-control for the gate above. It drives a synthetic tree so it cannot depend on the
// repo being broken, and it checks the two failure modes the previous control could not:
// an ALIASED import, and the marker half.
//
// The previous negative control was an identity test — origin: increment 2 internal
// review, pairing-sweep/F5, empty-guard/F6. Its "correct" fixture contained no prefix
// call at all, so an empty file (or a scanner that always returned nothing) passed it
// just as well. The negative fixture below therefore MENTIONS both halves in positions
// that must not count — a comment and a string literal — so it can only pass if the
// scanner is actually distinguishing syntax from text.
func TestInboxPrefixPairingGateCatchesEitherHalf(t *testing.T) {
	write := func(t *testing.T, dir, body string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, "rogue.go"), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	positives := []struct{ name, half, body string }{
		{"plain prefix call", "prefix", `package rogue

import "github.com/nats-io/nats.go"

func dial() []nats.Option { return []nats.Option{nats.CustomInboxPrefix("_TINBOX.a.b")} }
`},
		{"aliased import", "prefix", `package rogue

import n "github.com/nats-io/nats.go"

func dial() []n.Option { return []n.Option{n.CustomInboxPrefix("_TINBOX.a.b")} }
`},
		{"options struct literal", "prefix", `package rogue

import "github.com/nats-io/nats.go"

func dial() nats.Options { return nats.Options{InboxPrefix: "_TINBOX.a.b"} }
`},
		{"field assignment", "prefix", `package rogue

import "github.com/nats-io/nats.go"

func dial(o *nats.Options) { o.InboxPrefix = "_TINBOX.a.b" }
`},
		{"marker half alone", "marker", `package rogue

import (
	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/nats-io/nats.go"
)

func dial() []nats.Option { return []nats.Option{nats.UserInfo(auth.InboxCapableMarker, "")} }
`},
	}
	for _, p := range positives {
		t.Run(p.name, func(t *testing.T) {
			dir := t.TempDir()
			write(t, dir, p.body)
			got := inboxHalfSites(t, dir)
			if len(got) == 0 {
				t.Fatalf("the scan found nothing in a file that plainly sets the %s half; it "+
					"cannot detect the thing it exists to detect", p.half)
			}
			for _, g := range got {
				if g.half == p.half {
					return
				}
			}
			t.Fatalf("the scan saw %v, none of them the %s half", got, p.half)
		})
	}

	// NEGATIVE: a file that goes through the helper is not a site — and it names both
	// halves in a comment and a string, which must not count. An always-empty scanner
	// would pass the first clause of this and fail the positives above; a scanner that
	// grepped text would pass the positives and fail here.
	t.Run("helper caller with textual mentions", func(t *testing.T) {
		dir := t.TempDir()
		write(t, dir, `package rogue

import "github.com/LinZiyang666/tether/internal/natsinbox"

// This file must not be flagged even though it says CustomInboxPrefix and
// InboxCapableMarker in prose.
func dial(pub string) []any {
	_ = "CustomInboxPrefix InboxCapableMarker"
	return []any{natsinbox.InboxConnectOptions(pub)}
}
`)
		if got := inboxHalfSites(t, dir); len(got) != 0 {
			t.Fatalf("the scan flagged %v, which uses the helper and only MENTIONS the names in "+
				"a comment and a string — it would fail every correct call site", got)
		}
	})
}

// gate-control: TestPackageLevelInboxGateSeesABareCall

// packageLevelInboxSites finds production calls to the PACKAGE-LEVEL nats.NewInbox().
//
// origin: prerelease audit increment 2 internal review, broker-self-inbox/L6-F2 and
// repo-invariants/F3.
//
// nats.NewInbox() mints `_INBOX.<nuid>` from a package-level counter and knows nothing
// about any connection, so it IGNORES that connection's CustomInboxPrefix. On a
// connection that opted into the per-identity inbox the result is a subject the
// connection is not granted — but the two failure modes are not the same, and only one
// of them is loud:
//
//   - cmd/tether: the ctl's grant does not cover `_INBOX.>`, so the SUBSCRIBE is refused
//     and something visibly breaks. That is the site the previous guard watched.
//   - internal/broker: PermissionsForBroker still carries `_INBOX.>` — legitimately, for
//     pre-cutover peers — so a broker using the package-level helper keeps WORKING and
//     silently publishes its replies into the shared space. Nothing fails. That is the
//     site the previous guard did not watch, and it is the one that matters.
//
// So the rule is repo-wide rather than per-file, and it lives here rather than in a
// cmd/tether test named after something else (it was in exit_class_family_test.go, which
// is about exit codes).
func packageLevelInboxSites(t *testing.T, root string) []string {
	t.Helper()
	var sites []string
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, werr error) error {
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
		if !strings.HasSuffix(d.Name(), ".go") || strings.HasSuffix(d.Name(), "_test.go") {
			return nil
		}
		src, rerr := os.ReadFile(p)
		if rerr != nil {
			return rerr
		}
		rel, _ := filepath.Rel(root, p)
		rel = filepath.ToSlash(rel)
		f, perr := parser.ParseFile(token.NewFileSet(), p, src, 0)
		if perr != nil {
			return nil
		}
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "NewInbox" {
				return true
			}
			// A CONNECTION method call is the correct form; only a call on the imported
			// PACKAGE is a finding. Both parse to a SelectorExpr, so the receiver decides:
			// AST, not a substring scan, because the comment above every one of these
			// sites names nats.NewInbox() as the thing not to use.
			recv, ok := sel.X.(*ast.Ident)
			if !ok {
				return true
			}
			if recv.Obj == nil && recv.Name != "nc" && recv.Name != "conn" {
				// An unresolved identifier that is not a known connection variable: the
				// package qualifier. (recv.Obj is non-nil for locals declared in the file.)
				sites = append(sites, rel)
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	sort.Strings(sites)
	return sites
}

func TestNoProductionCodeUsesThePackageLevelInbox(t *testing.T) {
	if sites := packageLevelInboxSites(t, repoRoot(t)); len(sites) != 0 {
		t.Errorf("these production files mint an inbox with the package-level nats.NewInbox(): %v\n\n"+
			"Use the CONNECTION's method, nc.NewInbox(), which honours the private prefix this "+
			"connection asked for. In cmd/tether the package-level form is refused and something "+
			"breaks visibly; in internal/broker it KEEPS WORKING and quietly publishes the "+
			"broker's replies into the shared inbox space that every agent may read.", sites)
	}
}

// gate-control for the gate above.
func TestPackageLevelInboxGateSeesABareCall(t *testing.T) {
	dir := t.TempDir()
	write := func(body string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, "rogue.go"), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write(`package rogue

import "github.com/nats-io/nats.go"

func mint() string { return nats.NewInbox() }
`)
	if got := packageLevelInboxSites(t, dir); len(got) != 1 {
		t.Fatalf("the scan found %v in a file that plainly calls the package-level nats.NewInbox()", got)
	}
	// NEGATIVE, and not an identity test: this fixture DOES call NewInbox — on the
	// connection, which is the correct form — plus it names the forbidden form in a
	// comment. An empty file would pass this clause; only a scanner that distinguishes
	// the receiver passes both clauses.
	write(`package rogue

import "github.com/nats-io/nats.go"

// Deliberately NOT nats.NewInbox(): see the inbox pairing gate.
func mint(nc *nats.Conn) string { return nc.NewInbox() }
`)
	if got := packageLevelInboxSites(t, dir); len(got) != 0 {
		t.Fatalf("the scan flagged %v, which uses the CONNECTION method — it would fail every "+
			"correct site in the tree", got)
	}
}
