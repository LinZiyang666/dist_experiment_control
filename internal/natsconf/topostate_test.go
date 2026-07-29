package natsconf

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"sort"
	"strings"
	"testing"
)

// topostate_test.go — the classifier that both status renderers now share. The three defects it
// exists to remove are the three things these tests pin; each case names the mutation that reddens it.

const (
	// Verbatim Reasons from ReconcileOnce, so a reword there breaks these tests rather than silently
	// breaking the mixed-version fallback (which matches on exactly these strings).
	reasonUnknownDirective = "nats.conf has an unrecognized directive — fix it, or `cluster reconcile nats --manual`: boom"
	reasonRenderFailed     = "render (nats.conf could not be assembled — fix the conf/secrets, or `cluster reconcile nats --manual`): boom"
	reasonDryRunFailed     = "rendered conf failed `nats-server -t` (not swapping; .bak intact): boom"
	reasonApplyFailed      = "apply: no space left on device"
	reasonCutoverWithheld  = "standalone→clustered cutover rendered + validated but WITHHELD — a reconciler SIGHUP cannot safely cross it; run `tether cluster add <this-broker>` to perform the coordinated restart"
	reasonReloadPending    = "conf is current but the live server has not loaded it — a restart will pick it up"
	reasonAwaitingConfirm  = "conf swapped+validated; awaiting the live server's reload confirmation (re-checked each tick; a non-reloadable delta needs a restart)"
	reasonUnresolvable     = "no mesh peers known yet (converging)"
)

// TestClassifyTopoMapsEveryAction pins the whole action->state table. TopoUnknownAction is reserved
// for an action this binary has never heard of; every constant the package can EMIT must classify.
//
// Mutation: drop any case from ClassifyTopo's switch — that action falls through to
// TopoUnknownAction and this reddens.
func TestClassifyTopoMapsEveryAction(t *testing.T) {
	const desired = uint64(7)
	cases := []struct {
		action   string
		reason   string
		observed uint64
		want     TopoState
	}{
		{ActionNoop, "", 7, TopoConverged},
		{ActionNoop, "", 3, TopoBehind},
		{ActionReloaded, "", 7, TopoConverged},
		{ActionReloaded, "", 0, TopoBehind},
		{ActionSwappedReloadPending, reasonReloadPending, 3, TopoBehind},
		{ActionSwappedReloadPending, reasonAwaitingConfirm, 7, TopoBehind}, // never "converged" on an unconfirmed load
		{ActionUnresolvable, reasonUnresolvable, 0, TopoBehind},
		{ActionRejected, reasonRenderFailed, 3, TopoStuck},
		{ActionRejected, reasonDryRunFailed, 3, TopoStuck},
		{ActionRejected, reasonApplyFailed, 3, TopoStuck},
		{ActionUnknownDirective, reasonUnknownDirective, 3, TopoStuck},
		{ActionAwaitingClusteredCutover, reasonCutoverWithheld, 3, TopoHeld},
	}
	for _, c := range cases {
		if got := ClassifyTopo(c.action, c.reason, c.observed, desired, true); got != c.want {
			t.Errorf("ClassifyTopo(%q, observed=%d) = %s, want %s", c.action, c.observed, got, c.want)
		}
	}
	for _, a := range AllActions() {
		if got := ClassifyTopo(a, "", 0, desired, true); got == TopoUnknownAction {
			t.Errorf("action %q produced by this package classifies as TopoUnknownAction — every "+
				"emitted action must have a case, or a new reconcile outcome silently reads as "+
				"\"I cannot judge\" on every status surface", a)
		}
	}
	if got := ClassifyTopo("a_future_action_this_binary_predates", "", 0, desired, true); got != TopoUnknownAction {
		t.Errorf("an unrecognized action must be TopoUnknownAction (fail-closed), got %s", got)
	}
}

// TestAllActionsListsEveryActionConstant is what makes the exhaustiveness check above non-vacuous:
// it reads THIS package's own source and asserts AllActions() names every Action* constant. Without
// it, adding an eighth Action and forgetting AllActions() would leave the loop iterating over seven
// and passing.
//
// Mutation: add `const ActionFoo = "foo"` to reconcile.go without touching AllActions() — reddens.
func TestAllActionsListsEveryActionConstant(t *testing.T) {
	declared := actionConstantsInDir(t, ".")
	if len(declared) == 0 {
		t.Fatal("SELF-CHECK FAILED: the scanner found no Action* constants in this package, so its " +
			"agreement with AllActions() below is meaningless")
	}
	listed := map[string]bool{}
	for _, a := range AllActions() {
		listed[a] = true
	}
	var missing []string
	for name, val := range declared {
		if !listed[val] {
			missing = append(missing, name+" = "+val)
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Errorf("AllActions() does not list %d declared Action constant(s): %s\n"+
			"Add them AND give each a case in ClassifyTopo — an unlisted action makes the "+
			"exhaustiveness test vacuous for exactly the value nobody classified.",
			len(missing), strings.Join(missing, ", "))
	}
	if len(declared) != len(listed) {
		t.Errorf("AllActions() has %d entries but the package declares %d Action constants", len(listed), len(declared))
	}
}

// actionConstantsInDir returns {constName: stringValue} for every `Action*` const in the .go files
// of dir (test files excluded).
func actionConstantsInDir(t *testing.T, dir string) map[string]string {
	t.Helper()
	out := map[string]string{}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("readdir %s: %v", dir, err)
	}
	fset := token.NewFileSet()
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, perr := parser.ParseFile(fset, dir+"/"+name, nil, 0)
		if perr != nil {
			t.Fatalf("parse %s: %v", name, perr)
		}
		for _, d := range f.Decls {
			gd, ok := d.(*ast.GenDecl)
			if !ok || gd.Tok != token.CONST {
				continue
			}
			for _, s := range gd.Specs {
				vs, ok := s.(*ast.ValueSpec)
				if !ok {
					continue
				}
				for i, id := range vs.Names {
					if !strings.HasPrefix(id.Name, "Action") || i >= len(vs.Values) {
						continue
					}
					lit, ok := vs.Values[i].(*ast.BasicLit)
					if !ok || lit.Kind != token.STRING {
						continue
					}
					out[id.Name] = strings.Trim(lit.Value, `"`)
				}
			}
		}
	}
	return out
}

// TestStuckIsNotGatedOnGeneration pins defect (a). ActionUnknownDirective/ActionRejected return
// applied/observed UNCHANGED, so a broker that converged and was THEN wedged reports
// observed == desired. The pre-batch-C broker predicate nested its whole STUCK test inside
// `observed < desired`, so that state reported HEALTHY_HA with a permanently fail-closed reconciler.
//
// Mutation: gate the Stuck/Held cases in ClassifyTopo on observed < desired — reddens.
func TestStuckIsNotGatedOnGeneration(t *testing.T) {
	const desired = uint64(7)
	for _, tc := range []struct {
		name   string
		action string
		reason string
	}{
		{"unknown_directive", ActionUnknownDirective, reasonUnknownDirective},
		{"rejected_render", ActionRejected, reasonRenderFailed},
		{"rejected_dryrun", ActionRejected, reasonDryRunFailed},
		{"rejected_apply", ActionRejected, reasonApplyFailed},
	} {
		// observed == desired: the wedged-after-converging shape.
		if got := ClassifyTopo(tc.action, tc.reason, desired, desired, true); got != TopoStuck {
			t.Errorf("%s at observed==desired: got %s, want stuck — a reconcile wedged AT the "+
				"converged generation is invisible if this is gated on generations", tc.name, got)
		}
		// and the same verdict via the legacy (no-action) fallback.
		if got := ClassifyTopo("", tc.reason, desired, desired, true); got != TopoStuck {
			t.Errorf("%s at observed==desired via legacy fallback: got %s, want stuck", tc.name, got)
		}
	}
}

// TestAwaitingCutoverIsHeldAndNeverRecommendsManual pins defect (b), the one that steered an
// operator into data damage: the withhold Reason contains "rendered", the pre-batch-C broker matched
// the substring "render", so a DELIBERATE hold was classified STUCK — and STUCK's next-step
// recommends `reconcile nats --manual`, which is precisely the command ReconcileOnce documents as
// forming a clustered-alone JS meta (G4 #10) or orphaning the standalone store (#4).
//
// Mutations: (1) classify ActionAwaitingClusteredCutover as TopoStuck; (2) reorder
// classifyLegacyReason so the render marker is tested before legacyCutoverMarker; (3) reword
// TopoHeld.NextStep() to the generic STUCK remedy. Each reddens.
func TestAwaitingCutoverIsHeldAndNeverRecommendsManual(t *testing.T) {
	const desired = uint64(3)
	for _, tc := range []struct {
		name   string
		action string
	}{
		{"action path", ActionAwaitingClusteredCutover},
		{"legacy fallback (pre-batch-C broker sends no action)", ""},
	} {
		got := ClassifyTopo(tc.action, reasonCutoverWithheld, 1, desired, true)
		if got != TopoHeld {
			t.Fatalf("%s: got %s, want held — a withheld cutover is neither broken (STUCK) nor "+
				"self-healing (BEHIND)", tc.name, got)
		}
	}
	next := TopoHeld.NextStep()
	if !strings.Contains(next, "cluster add") {
		t.Errorf("TopoHeld.NextStep() must name `cluster add`, the only command that performs the "+
			"coordinated restart; got %q", next)
	}
	if !strings.Contains(next, "do NOT run") || !strings.Contains(next, "--manual") {
		t.Errorf("TopoHeld.NextStep() must EXPLICITLY warn against `reconcile nats --manual`. Naming "+
			"the wrong command is the runtime defence against re-classifying this as STUCK, whose "+
			"remedy recommends exactly that; got %q", next)
	}
	if strings.Contains(TopoHeld.Banner(), "cannot be rendered") {
		t.Errorf("TopoHeld.Banner() must not claim the conf cannot be rendered — it rendered AND "+
			"validated; only the swap was withheld; got %q", TopoHeld.Banner())
	}
}

// TestLegacyFallbackNeverGreenlightsAFailureReason is the mixed-version guard. Both ends share ONE
// fallback now; before batch C they had different substring sets and the CLI's was missing the
// render marker. Any reason carrying a failure/hold marker must classify as a degrading state
// through the fallback, at BOTH generation relations.
//
// Mutation: drop any marker from classifyLegacyReason — the corresponding row reddens.
func TestLegacyFallbackNeverGreenlightsAFailureReason(t *testing.T) {
	const desired = uint64(5)
	rows := []struct {
		name   string
		reason string
		want   TopoState
	}{
		{"unrecognized directive", reasonUnknownDirective, TopoStuck},
		{"nats-server -t", reasonDryRunFailed, TopoStuck},
		{"render failure", reasonRenderFailed, TopoStuck},
		{"apply failure", reasonApplyFailed, TopoStuck},
		{"withheld cutover", reasonCutoverWithheld, TopoHeld},
	}
	for _, r := range rows {
		for _, observed := range []uint64{0, desired - 1, desired, desired + 1} {
			got := ClassifyTopo("", r.reason, observed, desired, true)
			if got != r.want {
				t.Errorf("legacy %s at observed=%d/desired=%d: got %s, want %s",
					r.name, observed, desired, got, r.want)
			}
			if !got.Degrades() {
				t.Errorf("legacy %s classified as a non-degrading state (%s) — a mixed-version "+
					"window that reports HEALTHY on a wedged broker is the whole defect", r.name, got)
			}
		}
	}
}

// TestUnreportedAndNoDesiredNeverDegrade: an older broker that does not report topology, and a
// cluster with nothing desired yet, must never be punished by the HEALTHY-HA gate.
func TestUnreportedAndNoDesiredNeverDegrade(t *testing.T) {
	if got := ClassifyTopo(ActionRejected, reasonRenderFailed, 0, 7, false); got != TopoUnreported {
		t.Errorf("a non-reporting node must be TopoUnreported regardless of action, got %s", got)
	}
	if got := ClassifyTopo(ActionRejected, reasonRenderFailed, 0, 0, true); got != TopoUnreported {
		t.Errorf("desired==0 (nothing managed) must be TopoUnreported, got %s", got)
	}
	if TopoUnreported.Degrades() || TopoConverged.Degrades() {
		t.Error("only unreported and converged may be non-degrading")
	}
	for _, s := range []TopoState{TopoBehind, TopoHeld, TopoStuck, TopoUnknownAction} {
		if !s.Degrades() {
			t.Errorf("%s must degrade — folding it into healthy is the false-green direction", s)
		}
	}
}

// TestWorstTopoStatePrecedence: the node needing hands NOW must win the cluster banner over the one
// that only needs patience, and "I cannot read that broker's action" must not be silently outranked
// by a merely-behind peer.
//
// Mutation: swap any two severity() values — reddens.
func TestWorstTopoStatePrecedence(t *testing.T) {
	order := []TopoState{TopoUnreported, TopoConverged, TopoBehind, TopoUnknownAction, TopoHeld, TopoStuck}
	for i := 0; i < len(order); i++ {
		for j := i + 1; j < len(order); j++ {
			if got := WorstTopoState(order[i], order[j]); got != order[j] {
				t.Errorf("WorstTopoState(%s, %s) = %s, want %s", order[i], order[j], got, order[j])
			}
			if got := WorstTopoState(order[j], order[i]); got != order[j] {
				t.Errorf("WorstTopoState(%s, %s) = %s, want %s (must be order-independent)", order[j], order[i], got, order[j])
			}
		}
	}
	if got := WorstTopoState(); got != TopoUnreported {
		t.Errorf("WorstTopoState() over nothing = %s, want unreported", got)
	}
}

// TestEveryDegradingStateHasBannerAndNextStep: a degrading state with an empty banner would make the
// broker return DEGRADED with no explanation, and an empty next-step leaves the operator with a
// verdict and no command.
func TestEveryDegradingStateHasBannerAndNextStep(t *testing.T) {
	for _, s := range []TopoState{TopoUnreported, TopoConverged, TopoBehind, TopoHeld, TopoStuck, TopoUnknownAction} {
		banner, next, cell := s.Banner(), s.NextStep(), s.Cell()
		if s.Degrades() {
			if banner == "" || next == "" {
				t.Errorf("%s degrades but banner=%q next=%q — a verdict with no explanation or no command", s, banner, next)
			}
		} else if banner != "" || next != "" {
			t.Errorf("%s does not degrade but carries banner=%q next=%q", s, banner, next)
		}
		if cell == "" {
			t.Errorf("%s has an empty TOPO cell token", s)
		}
	}
}

// TestLegacyFallbackAgreesWithTheActionPathOnEveryProducedOutcome.
//
// origin: batch-c internal review C3-F2. The two paths disagreed on the same broker state: the legacy
// fallback fell through to a plain generation comparison for any reason carrying no failure marker,
// so swapped_reload_pending and unresolvable read as CONVERGED at observed>=desired while the action
// path calls them BEHIND. Two ends of one mixed-version window giving opposite polarity for one
// broker is the exact defect this classifier was built to remove, reproduced inside its own fallback.
//
// The table is every (action, reason) pair ReconcileOnce actually emits, taken verbatim from
// reconcile.go, at BOTH generation relations.
//
// Mutation: drop the `case reason != ""` arm from classifyLegacyReason — the swapped/unresolvable rows
// at observed>=desired redden.
func TestLegacyFallbackAgreesWithTheActionPathOnEveryProducedOutcome(t *testing.T) {
	const desired = uint64(5)
	rows := []struct {
		name   string
		action string
		reason string
	}{
		{"noop, truly converged", ActionNoop, ""},
		{"reloaded, probe confirmed", ActionReloaded, ""},
		{"swapped, reload not confirmed", ActionSwappedReloadPending, reasonReloadPending},
		{"swapped, awaiting confirmation", ActionSwappedReloadPending, reasonAwaitingConfirm},
		{"unresolvable, peers not replicated", ActionUnresolvable, reasonUnresolvable},
		{"rejected: render", ActionRejected, reasonRenderFailed},
		{"rejected: nats-server -t", ActionRejected, reasonDryRunFailed},
		{"rejected: apply", ActionRejected, reasonApplyFailed},
		{"unknown directive", ActionUnknownDirective, reasonUnknownDirective},
		{"withheld cutover", ActionAwaitingClusteredCutover, reasonCutoverWithheld},
	}
	for _, r := range rows {
		for _, observed := range []uint64{0, desired - 1, desired, desired + 1} {
			withAction := ClassifyTopo(r.action, r.reason, observed, desired, true)
			legacy := ClassifyTopo("", r.reason, observed, desired, true)
			if withAction != legacy {
				t.Errorf("%s at observed=%d/desired=%d: the action path says %s but a pre-batch-C broker "+
					"reporting the SAME state classifies as %s — the two ends of the mixed-version window "+
					"disagree about one broker", r.name, observed, desired, withAction, legacy)
			}
			// And neither may be the false-green direction for a state that is not converged.
			if r.reason != "" && (withAction == TopoConverged || legacy == TopoConverged) {
				t.Errorf("%s at observed=%d: a pass that reported a reason classified as converged "+
					"(action=%s legacy=%s)", r.name, observed, withAction, legacy)
			}
		}
	}
}
