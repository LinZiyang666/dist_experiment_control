package auth

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// Batch-A A7: reconcile the ctl-facing pub ACL against the broker's live
// subscription table, in BOTH directions.
//
// Neither direction had any check, and both had drifted:
//
//   - grant with no subscriber. `session kick` and `session rotate-pin` are
//     pub-allowed for every activated member, and no broker ever subscribed to
//     them. internal/broker/audit.go:36-39 records the same fact from the audit
//     side ("session kick/rotate-pin are not implemented"), and DOC-12 already
//     corrected architecture H.1 — the ACL was simply never part of that sweep.
//
//     The cost is not "an unused string". It is that the next person to build
//     version announce, or node tagging, finds the subject AND the grant already
//     present, concludes the design was done, and inherits an authorisation
//     nobody ever reviewed for that feature.
//
//   - subscriber with no grant would be the opposite failure: a handler that
//     can never be reached by a member, discovered only when someone reports
//     "permissions violation" in production.
//
// Revert semantics (checked before writing this — it is the one item in batch A
// whose effect `git revert` does not immediately undo): grants live in issued
// user JWTs with a 24h TTL (authcallout.defaultJWTTTL), so removing a grant
// takes effect as clients re-authenticate, and restoring it likewise converges
// within one TTL. Old and new clients coexist meanwhile. Nothing is persisted.
// And these subjects match no JetStream stream filter (jsstream.go creates
// exactly three: sys events, per-session history, placement canary), so an
// unsubscribed publish is dropped by core NATS rather than written to disk.

// tokenMatch reports whether a NATS subscription pattern matches a concrete
// subject. `*` matches exactly one token; `>` matches one or more trailing
// tokens.
func tokenMatch(pattern, subject string) bool {
	p := strings.Split(pattern, ".")
	s := strings.Split(subject, ".")
	for i, pt := range p {
		if pt == ">" {
			return len(s) > i
		}
		if i >= len(s) {
			return false
		}
		if pt != "*" && pt != s[i] {
			return false
		}
	}
	return len(p) == len(s)
}

// grantSubjects extracts the ctl-facing pub-allow subjects from permissions.go
// by parsing the string-concatenation expressions, substituting `*` for the
// actor/sid variables so they can be compared against wildcard subscriptions.
func grantSubjects(t *testing.T, file string) []string {
	t.Helper()
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, file, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", file, err)
	}
	var out []string
	var flatten func(e ast.Expr) (string, bool)
	flatten = func(e ast.Expr) (string, bool) {
		switch v := e.(type) {
		case *ast.BasicLit:
			if v.Kind != token.STRING {
				return "", false
			}
			s, err := strconv.Unquote(v.Value)
			return s, err == nil
		case *ast.Ident:
			switch v.Name {
			case "subjectPrefix":
				return "PREFIX", true
			case "actor", "sid":
				return "*", true // wildcard-equivalent for matching purposes
			}
			return "", false
		case *ast.BinaryExpr:
			if v.Op != token.ADD {
				return "", false
			}
			l, ok1 := flatten(v.X)
			r, ok2 := flatten(v.Y)
			if !ok1 || !ok2 {
				return "", false
			}
			return l + r, true
		}
		return "", false
	}
	ast.Inspect(f, func(n ast.Node) bool {
		kv, ok := n.(*ast.KeyValueExpr)
		if !ok {
			return true
		}
		id, ok := kv.Key.(*ast.Ident)
		if !ok || id.Name != "Allow" {
			return true
		}
		cl, ok := kv.Value.(*ast.CompositeLit)
		if !ok {
			return true
		}
		for _, el := range cl.Elts {
			if s, ok := flatten(el); ok && strings.HasPrefix(s, "PREFIX.ctrl.by.") {
				out = append(out, s)
			}
		}
		return true
	})
	return out
}

// subscribedSubjects extracts subscription subject literals from every
// non-test file under internal/broker. Scanning only broker.go would report
// handlers registered elsewhere (alerts, cluster health, grow/upgrade triggers)
// as unsubscribed — a false positive that would train readers to ignore this
// test, which is exactly the failure mode it exists to prevent.
func subscribedSubjects(t *testing.T, dirs ...string) []string {
	t.Helper()
	// Pass 1: subject constants across ALL scanned packages. Subscriptions
	// routinely reference them cross-package (nc.Subscribe(proto.SubjCtrlXWildcard,
	// ...)), so a per-file view resolves nothing and reports every such handler
	// as absent — the false-negative direction, which reads as "this grant has no
	// subscriber" and would have had us delete live authorisations.
	globalConsts := map[string]string{}
	collect := func(dir string) error {
		return filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() || !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
				return nil
			}
			f, perr := parser.ParseFile(token.NewFileSet(), p, nil, 0)
			if perr != nil {
				return nil
			}
			for k, v := range subjectConsts(f) {
				globalConsts[k] = v
			}
			return nil
		})
	}
	for _, d := range dirs {
		if err := collect(d); err != nil {
			t.Fatalf("collect consts %s: %v", d, err)
		}
	}

	var out []string
	fset := token.NewFileSet()
	walk := func(dir string) error {
		return filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() || !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
				return nil
			}
			f, perr := parser.ParseFile(fset, p, nil, 0)
			if perr != nil {
				return nil
			}
			out = append(out, subjectLiteralsWith(f, globalConsts)...)
			return nil
		})
	}
	for _, d := range dirs {
		if err := walk(d); err != nil {
			t.Fatalf("walk %s: %v", d, err)
		}
	}
	return out
}

// subjectLiterals extracts subjects the broker actually SUBSCRIBES to.
//
// External review B4: this used to collect every `SubjectPrefix + "..."`
// expression in the tree, so a dead constant — a subject builder left behind
// after its handler was deleted — counted as a live subscriber. That makes the
// grant→subscriber direction pass for exactly the case it exists to catch: an
// authorisation whose consumer is gone.
//
// Evidence now required, in order of strength:
//   - the subject is an argument to Subscribe / QueueSubscribe / ChanSubscribe
//     (directly, or via a variable/const resolved below), or
//   - it appears as a `subj:` field in a subscription-table composite literal,
//     which is how broker.go registers most handlers.
//
// A bare declaration is NOT evidence. Dynamic subjects that neither form can
// resolve are simply not reported — this direction fails closed towards "no
// subscriber found", which produces a loud reconciliation error rather than a
// silent pass.
// subjectLiterals keeps the single-argument form the external-review counter-
// example calls. It resolves only what one file can see.
func subjectLiterals(f *ast.File) []string {
	return subjectLiteralsWith(f, nil)
}

func subjectLiteralsWith(f *ast.File, globalConsts map[string]string) []string {
	// Local const/var name -> subject value, so `Subscribe(subjFoo)`
	// resolves. Values are only USED when referenced by a subscribe call below.
	local := map[string]string{}
	for k, v := range globalConsts {
		local[k] = v
	}
	var flatten func(ast.Expr) (string, bool)
	flatten = func(e ast.Expr) (string, bool) {
		switch v := e.(type) {
		case *ast.BasicLit:
			if v.Kind != token.STRING {
				return "", false
			}
			s, err := strconv.Unquote(v.Value)
			return s, err == nil
		case *ast.Ident:
			if v.Name == "SubjectPrefix" {
				return "PREFIX", true
			}
			if s, ok := local[v.Name]; ok {
				return s, true
			}
			return "", false
		case *ast.SelectorExpr:
			if v.Sel.Name == "SubjectPrefix" {
				return "PREFIX", true
			}
			if s, ok := local[v.Sel.Name]; ok {
				return s, true
			}
			return "", false
		case *ast.BinaryExpr:
			if v.Op != token.ADD {
				return "", false
			}
			l, ok1 := flatten(v.X)
			r, ok2 := flatten(v.Y)
			if !ok1 || !ok2 {
				return "", false
			}
			return l + r, true
		}
		return "", false
	}
	ast.Inspect(f, func(n ast.Node) bool {
		vs, ok := n.(*ast.ValueSpec)
		if !ok {
			return true
		}
		for i, nm := range vs.Names {
			if i >= len(vs.Values) {
				continue
			}
			if s, ok := flatten(vs.Values[i]); ok {
				local[nm.Name] = s
			}
		}
		return true
	})

	var out []string
	record := func(e ast.Expr) {
		if s, ok := flatten(e); ok && strings.HasPrefix(s, "PREFIX") {
			out = append(out, s)
		}
	}

	ast.Inspect(f, func(n ast.Node) bool {
		switch v := n.(type) {
		case *ast.CallExpr:
			var name string
			switch fn := v.Fun.(type) {
			case *ast.Ident:
				name = fn.Name
			case *ast.SelectorExpr:
				name = fn.Sel.Name
			default:
				return true
			}
			if !subscribeFuncs[name] || len(v.Args) == 0 {
				return true
			}
			record(v.Args[0])
		case *ast.CompositeLit:
			// broker.go registers most handlers through a POSITIONAL table:
			//
			//   for _, ss := range []struct {
			//       subj    string
			//       handler nats.MsgHandler
			//   }{
			//       {proto.SubjectPrefix + ".ctrl.by.*.session.create.req", b.handleSessionCreate},
			//       ...
			//
			// The elements carry no field names, and the subject reaches
			// nc.Subscribe as a loop variable, so neither the call-site rule nor
			// the keyed rule sees it. Recognising the table by its FIELD NAMES
			// keeps the "real subscription required" property: an anonymous
			// struct declaring `subj` plus a nats.MsgHandler is a subscription
			// table, not a stray declaration.
			at, ok := v.Type.(*ast.ArrayType)
			if !ok {
				return true
			}
			st, ok := at.Elt.(*ast.StructType)
			if !ok || st.Fields == nil {
				return true
			}
			subjIdx, isTable := -1, false
			idx := 0
			for _, fl := range st.Fields.List {
				for _, nm := range fl.Names {
					if nm.Name == "subj" || nm.Name == "subject" {
						subjIdx = idx
					}
					if sel, ok := fl.Type.(*ast.SelectorExpr); ok && sel.Sel.Name == "MsgHandler" {
						isTable = true
					}
					idx++
				}
			}
			if subjIdx < 0 || !isTable {
				return true
			}
			for _, el := range v.Elts {
				row, ok := el.(*ast.CompositeLit)
				if !ok {
					continue
				}
				// Rows may be positional ({subject, handler}) or keyed
				// ({subj: ..., handler: ...}). The keyed form used to be matched by
				// a free-standing KeyValueExpr rule that fired on ANY `subj:` field
				// anywhere in the file, so `config{subj: SubjectPrefix + ".dead"}` —
				// a plain struct with no handler in sight — was reported as a live
				// subscription (external review R2). Reading keys only INSIDE a
				// recognised subscription table keeps the handler requirement.
				keyed := false
				for _, e := range row.Elts {
					kv, ok := e.(*ast.KeyValueExpr)
					if !ok {
						continue
					}
					keyed = true
					if id, ok := kv.Key.(*ast.Ident); ok && (id.Name == "subj" || id.Name == "subject") {
						record(kv.Value)
					}
				}
				if !keyed && subjIdx < len(row.Elts) {
					record(row.Elts[subjIdx])
				}
			}
		}
		return true
	})
	return out
}

// subjectConsts returns this file's const/var declarations whose value is a
// SubjectPrefix-rooted string, for cross-package resolution.
func subjectConsts(f *ast.File) map[string]string {
	out := map[string]string{}
	var flat func(ast.Expr) (string, bool)
	flat = func(e ast.Expr) (string, bool) {
		switch v := e.(type) {
		case *ast.BasicLit:
			if v.Kind != token.STRING {
				return "", false
			}
			s, err := strconv.Unquote(v.Value)
			return s, err == nil
		case *ast.Ident:
			if v.Name == "SubjectPrefix" {
				return "PREFIX", true
			}
			if s, ok := out[v.Name]; ok {
				return s, true
			}
			return "", false
		case *ast.SelectorExpr:
			if v.Sel.Name == "SubjectPrefix" {
				return "PREFIX", true
			}
			if s, ok := out[v.Sel.Name]; ok {
				return s, true
			}
			return "", false
		case *ast.BinaryExpr:
			if v.Op != token.ADD {
				return "", false
			}
			l, ok1 := flat(v.X)
			r, ok2 := flat(v.Y)
			if !ok1 || !ok2 {
				return "", false
			}
			return l + r, true
		}
		return "", false
	}
	// Two passes so declarations that reference earlier ones in the same block resolve.
	for i := 0; i < 2; i++ {
		ast.Inspect(f, func(n ast.Node) bool {
			vs, ok := n.(*ast.ValueSpec)
			if !ok {
				return true
			}
			for j, nm := range vs.Names {
				if j >= len(vs.Values) {
					continue
				}
				if s, ok := flat(vs.Values[j]); ok && strings.HasPrefix(s, "PREFIX") {
					out[nm.Name] = s
				}
			}
			return true
		})
	}
	return out
}

// subscribeFuncs are the calls that actually establish a subscription. A helper
// that wraps one of these must be added here, or its subjects go unseen — which
// fails towards a noisy reconciliation error, not a silent pass.
var subscribeFuncs = map[string]bool{
	"Subscribe":      true,
	"QueueSubscribe": true,
	"ChanSubscribe":  true,
	"SubscribeSync":  true,
}

// knownUnsubscribedGrants are grants deliberately kept without a subscriber,
// each with the reason. Anything NOT in here that has no subscriber fails.
// knownUnsubscribedGrants holds grants deliberately kept without a subscriber,
// each with its reason. It is EMPTY: A7 removed the three that existed
// (session kick / rotate-pin / node tag) rather than exempting them.
//
// The mechanism stays because the legitimate case will recur — a grant landing
// one release ahead of its handler. Adding an entry must be a conscious act
// with a written justification, not the path of least resistance.
var knownUnsubscribedGrants = map[string]string{}

func TestACLGrantsHaveSubscribers(t *testing.T) {
	root, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	root = filepath.Dir(filepath.Dir(root)) // internal/auth -> repo root

	grants := grantSubjects(t, filepath.Join(root, "internal", "auth", "permissions.go"))
	// internal/proto is scanned too: many subscriptions reference a named
	// constant (proto.SubjCtrlClusterHealthWildcard and friends) rather than
	// concatenating the subject inline, and those constants are defined there.
	subs := subscribedSubjects(t,
		filepath.Join(root, "internal", "broker"),
		filepath.Join(root, "internal", "proto"))

	if len(grants) == 0 || len(subs) == 0 {
		t.Fatalf("extraction degenerated (grants=%d subs=%d); a green result would be meaningless",
			len(grants), len(subs))
	}

	seen := map[string]bool{}
	for _, g := range grants {
		if seen[g] || strings.HasSuffix(g, ".>") {
			// A trailing `>` is a template-level blanket grant (broker / node
			// identities), not a per-verb ctl grant; reconciling it against a
			// verb subscription list is a category error.
			continue
		}
		seen[g] = true
		matched := false
		for _, s := range subs {
			if tokenMatch(s, g) {
				matched = true
				break
			}
		}
		if matched {
			if _, listed := knownUnsubscribedGrants[g]; listed {
				t.Errorf("%s now HAS a subscriber but is still listed in knownUnsubscribedGrants; "+
					"remove the exemption", g)
			}
			continue
		}
		if reason, listed := knownUnsubscribedGrants[g]; listed {
			if reason == "" {
				t.Errorf("exemption for %s has no reason", g)
			}
			continue
		}
		t.Errorf("ACL grants %s but no broker subscription matches it.\n"+
			"Either wire the handler, drop the grant, or add it to knownUnsubscribedGrants WITH a reason. "+
			"An unreviewed grant that outlives its feature is how the next author inherits an "+
			"authorisation nobody designed.", g)
	}
}

// TestACLReconcilerIsNotVacuous proves the matcher can actually fail — without
// this, a broken extractor reports a clean reconciliation forever.
func TestACLReconcilerIsNotVacuous(t *testing.T) {
	if tokenMatch("PREFIX.ctrl.by.*.session.create.req", "PREFIX.ctrl.by.*.session.nope.req") {
		t.Error("matcher accepted a subject it must reject")
	}
	if !tokenMatch("PREFIX.ctrl.by.*.session.*.rm.req", "PREFIX.ctrl.by.*.session.*.rm.req") {
		t.Error("matcher rejected an exact wildcard match")
	}
	if !tokenMatch("PREFIX.s.>", "PREFIX.s.abc.def") {
		t.Error("matcher mishandled `>`")
	}
	if tokenMatch("PREFIX.s.*", "PREFIX.s.abc.def") {
		t.Error("`*` must match exactly one token, not many")
	}
}

// knownUngrantedSubscriptions are ctl-facing subscriptions deliberately served
// without a member grant, each with its reason.
var knownUngrantedSubscriptions = map[string]string{}

// TestACLSubscribersHaveGrants is the SECOND direction, and the reason this file
// may call itself bidirectional.
//
// Batch-A review F-12: four places (this file's doc, permissions.go twice, and
// the progress log) claimed the reconciliation ran both ways while only
// grant→subscriber existed. That is the same defect class as the missing
// TestWireCodeNamespacesAgree — a promised guard that was never written — and it
// is the second occurrence inside one batch. The direction is now real.
//
// What it catches: a broker handler that members can never reach. That failure
// is invisible in tests (the broker subscribes fine) and surfaces in production
// as "permissions violation" from a client that looks correctly configured.
// unresolvedSubscriptionSites reports Subscribe call sites whose subject this
// scanner cannot resolve to a literal.
//
// External review R2: those sites used to be dropped in silence. In the
// grant->subscriber direction that is merely conservative, but in the
// subscriber->grant direction it is a hole in exactly the property being
// asserted — a NEW subscription with NO grant is invisible if its subject is
// built dynamically. "Fails closed" was true of one direction and claimed for
// both. Requiring each such site to be declared makes it true of both.
func unresolvedSubscriptionSites(t *testing.T, dirs ...string) []string {
	t.Helper()
	var out []string
	for _, dir := range dirs {
		fset := token.NewFileSet()
		_ = filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() || !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
				return nil
			}
			f, perr := parser.ParseFile(fset, p, nil, 0)
			if perr != nil {
				return nil
			}
			// Values proven to be static subject expressions in this file. Keep
			// this deliberately narrower than "has a familiar AST shape":
			// `SubjectPrefix + suffix` is a BinaryExpr too, but remains dynamic
			// when suffix is a function parameter.
			locals := subjectConsts(f)
			var resolvable func(ast.Expr) bool
			resolvable = func(e ast.Expr) bool {
				switch v := e.(type) {
				case *ast.BasicLit:
					return v.Kind == token.STRING
				case *ast.Ident:
					if v.Name == "SubjectPrefix" {
						return true
					}
					_, ok := locals[v.Name]
					return ok
				case *ast.SelectorExpr:
					return v.Sel.Name == "SubjectPrefix" || strings.HasPrefix(v.Sel.Name, "Subj")
				case *ast.BinaryExpr:
					return v.Op == token.ADD && resolvable(v.X) && resolvable(v.Y)
				}
				return false
			}
			ast.Inspect(f, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok || len(call.Args) == 0 {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok || !subscribeFuncs[sel.Sel.Name] {
					return true
				}
				if resolvable(call.Args[0]) {
					return true
				}
				rel, _ := filepath.Rel(filepath.Dir(filepath.Dir(dir)), p)
				out = append(out, filepath.ToSlash(rel)+":"+
					strconv.Itoa(fset.Position(call.Pos()).Line))
				return true
			})
			return nil
		})
	}
	sort.Strings(out)
	return out
}

// dynamicSubscriptionExemptions records why a subscription subject that cannot
// be read statically is nonetheless not an ACL gap. Each entry is a decision.
var dynamicSubscriptionExemptions = map[string]string{
	"internal/broker/observability.go:81": "SubscribeSync(inbox) — a per-request random _INBOX reply " +
		"subject, not a served endpoint. Reply subjects are granted by NATS to the requester, " +
		"never by the ctl permission template, so no grant can or should exist.",
	"internal/broker/home_delivery.go:197": "Subscribe(inbox+\".>\", ...) — a broker-owned _INBOX ack " +
		"channel for agent home-delivery acknowledgements, not a ctl-served endpoint; the random " +
		"inbox is intentionally runtime-generated and does not belong in member pub grants.",
	"internal/broker/broker.go:1029": "QueueSubscribe(ss.subj, ...) — the loop variable of the positional " +
		"subscription table; every row's subject IS extracted, from the table literal above.",
	"internal/broker/broker.go:1031": "Subscribe(ss.subj, ...) — same loop variable as :1029.",
}

// TestACLDynamicSubscriptionsAreDeclared closes the second direction of R2: a
// subscription whose subject the scanner cannot read must be an explicit,
// justified entry, not an accident that reads as coverage.
func TestACLDynamicSubscriptionsAreDeclared(t *testing.T) {
	root, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	root = filepath.Dir(filepath.Dir(root))
	sites := unresolvedSubscriptionSites(t, filepath.Join(root, "internal", "broker"))
	live := map[string]bool{}
	for _, s := range sites {
		live[s] = true
	}
	var undeclared []string
	for _, s := range sites {
		if _, ok := dynamicSubscriptionExemptions[s]; !ok {
			undeclared = append(undeclared, s)
		}
	}
	if len(undeclared) > 0 {
		t.Errorf("%d subscription(s) build their subject dynamically and are not declared in "+
			"dynamicSubscriptionExemptions:\n  %s\n"+
			"An unreadable subject is a subscription this reconciliation cannot check for a grant. "+
			"Declare why it needs none, or make the subject a resolvable constant.",
			len(undeclared), strings.Join(undeclared, "\n  "))
	}
	for site, reason := range dynamicSubscriptionExemptions {
		if reason == "" {
			t.Errorf("dynamicSubscriptionExemptions entry %q has no reason", site)
		}
		if !live[site] {
			t.Errorf("dynamicSubscriptionExemptions entry %q no longer names an unresolved Subscribe site; "+
				"re-read and remove or update it", site)
		}
	}
}

func TestACLSubscribersHaveGrants(t *testing.T) {
	root, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	root = filepath.Dir(filepath.Dir(root))

	grants := grantSubjects(t, filepath.Join(root, "internal", "auth", "permissions.go"))
	subs := subscribedSubjects(t,
		filepath.Join(root, "internal", "broker"),
		filepath.Join(root, "internal", "proto"))

	if len(grants) == 0 || len(subs) == 0 {
		t.Fatalf("extraction degenerated (grants=%d subs=%d); a green result would be meaningless",
			len(grants), len(subs))
	}

	// Only ctl-facing subjects are in scope: `ctrl.by.<actor>.*` is the member
	// namespace. Agent subjects (s.<sid>.cmd…) and broker-only ones
	// (tether.v2.cluster.>) are reached with different identities entirely.
	seen := map[string]bool{}
	for _, sub := range subs {
		if !strings.HasPrefix(sub, "PREFIX.ctrl.by.") || seen[sub] {
			continue
		}
		seen[sub] = true
		granted := false
		for _, g := range grants {
			// Blanket `>` grants are template-level (broker / node identities),
			// not per-verb member grants. Counting them here would make EVERY
			// ctl subscription look granted — which is exactly how the first
			// version of this direction came out vacuous: a mutation adding an
			// ungranted `ctrl.by.*.mutation.probe.req` subscription stayed green
			// because `ctrl.by.*.>` swallowed it. The forward direction already
			// skipped them; this one has to as well.
			if strings.HasSuffix(g, ".>") {
				continue
			}
			// A grant covers a subscription when either side's pattern matches
			// the other; both carry `*` in the actor/sid positions.
			if tokenMatch(sub, g) || tokenMatch(g, sub) {
				granted = true
				break
			}
		}
		if granted {
			if _, listed := knownUngrantedSubscriptions[sub]; listed {
				t.Errorf("%s now HAS a grant but is still exempted; remove the entry", sub)
			}
			continue
		}
		if reason, listed := knownUngrantedSubscriptions[sub]; listed {
			if reason == "" {
				t.Errorf("exemption for %s has no reason", sub)
			}
			continue
		}
		t.Errorf("the broker subscribes %s but no member ACL grants it.\n"+
			"A handler members cannot reach fails as a NATS permissions violation in production, from a "+
			"client that looks correctly configured. Grant it, stop subscribing it, or exempt it WITH a reason.",
			sub)
	}
}

// TestACLExtractorsAreNotVacuous proves both AST extractors actually see
// something structured, not that they merely returned a non-empty slice.
// TestACLReconcilerIsNotVacuous covers tokenMatch; these cover the parsers,
// which is where a silent degeneration would hide.
func TestACLExtractorsAreNotVacuous(t *testing.T) {
	root, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	root = filepath.Dir(filepath.Dir(root))

	grants := grantSubjects(t, filepath.Join(root, "internal", "auth", "permissions.go"))
	subs := subscribedSubjects(t,
		filepath.Join(root, "internal", "broker"),
		filepath.Join(root, "internal", "proto"))

	// Anchors that must be present: if the extractor degenerates to matching
	// nothing structured, these disappear and every reconciliation goes green.
	mustGrant := "PREFIX.ctrl.by.*.session.create.req"
	mustSub := "PREFIX.ctrl.by.*.session.create.req"
	if !contains(grants, mustGrant) {
		t.Errorf("grant extractor did not find %q; it has degenerated and every reconciliation above "+
			"is now vacuously green. Found %d grants.", mustGrant, len(grants))
	}
	if !contains(subs, mustSub) {
		t.Errorf("subscription extractor did not find %q; same problem. Found %d subscriptions.",
			mustSub, len(subs))
	}
	// And the deleted grants must stay deleted.
	for _, gone := range []string{
		"PREFIX.ctrl.by.*.session.*.kick.req",
		"PREFIX.ctrl.by.*.session.*.rotate-pin.req",
		"PREFIX.ctrl.by.*.s.*.node.*.tag.req",
	} {
		if contains(grants, gone) {
			t.Errorf("%s is granted again — A7 removed it because no handler exists and the grant "+
				"would let the next author inherit an authorisation nobody designed", gone)
		}
	}
}
