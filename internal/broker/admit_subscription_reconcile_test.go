package broker

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
	"strings"
	"testing"
)

// admit_subscription_reconcile_test.go (batch B, B1) — the reconciliation the plan asked for
// and the first pass did not deliver.
//
// WHAT NOTHING ELSE COVERS
//
// Every other test in this batch starts from a list the TEST wrote down. admit_test.go compares
// the spec table against verb strings typed into itself; admit_ordering_test.go parses the
// handler files; ingress_characterization_test.go builds its own subjects and subscribes its own
// callback, bypassing broker.go's subscription table entirely.
//
// So the one thing none of them can see is a verb that is SUBSCRIBED but never gated. Rename the
// `.expose-rm.req` subscription, or add a new session-scoped ctl verb whose handler forgets
// admit() altogether, and every test in this increment stays green — while a wire-reachable verb
// runs with no session, membership or node check at all.
//
// That is not hypothetical. admit.go's own header cites broker.go:1320, where `register` is
// recorded as having SHIPPED without this gate: "Without this, a tombstoned session could get a
// fresh nodes row." The gate is the system's only runtime revocation point for 24h auth_callout
// JWTs, so a verb that skips it has no other backstop.
//
// This test therefore derives the truth from broker.go's actual subscription literal and requires
// every session-scoped ctl verb to be either gated by a verbSpec or explicitly listed below with
// a reason.

// ungatedSubscriptionLeaves are the subscribed verbs that do NOT go through a verbSpec, each with
// the reason. An entry here is a decision; the absence of an entry is a bug.
var ungatedSubscriptionLeaves = map[string]string{
	// Deferred by docs/reviews/batch-b-plan.md §1 — they already share a gate helper, and folding
	// them into admit() was judged the smallest-benefit, largest-cost part of B1.
	"push":        "transfer family: gated by transferGate (transfer.go), deferred by plan §1.",
	"pull":        "transfer family: gated by transferGate, deferred by plan §1.",
	"push-commit": "transfer family: gated by transferGate AFTER the tracker lookup — the position is load-bearing (§9 D8 continuation routing), deferred by plan §1.",
	"finalize":    "transfer family: gated by transferGate after the tracker preview, deferred by plan §1.",
	"caps":        "transfer family: gated by transferGate with nid=\"\" (batch-A A2), deferred by plan §1.",

	"proxy.set": "proxy-owner family: gated by proxyActiveOwnerGate (proxy.go), deferred by plan §1.",
	"proxy.sub": "proxy-owner family: gated by proxyActiveOwnerGate per action, deferred by plan §1.",
	// ps / node.list / proxy.status were converted when plan §15.2's deferral was completed, but
	// they are gated by ctrlVerbSpec (three subject families, one shared admitACL) rather than by
	// verbSpec, so they are still not in the `gated` set this test builds from the verbSpec table.
	// They are exempted with the spec that covers them named, so the exemption is checkable.
	"ps":           "gated by admitCtrl(psSpec) — ctrl.by family, ParseCtrlBy + leaf tail, no nid.",
	"node.list":    "gated by admitCtrl(nodeListSpec) — same family as ps.",
	"proxy.status": "gated by admitACL directly — ctrl.<sid>.proxy.<action> family (ParseCtrlProxy), no nid.",

	// Deliberately NOT session-member-gated — folding these into admit() would CHANGE the
	// authorization model, not preserve it. Each is argued in plan §1.
	"session.create": "no session exists yet; the actor is becoming the owner.",
	"session.list":   "fp-scoped listing, not session-scoped.",
	"session.rm":     "owner-only and deliberately WITHOUT IsActive — it must work on a DELETING session, which is where already_deleting comes from.",
	"register":       "agent, not a session member: IsActive is the FIFTH check and runs AFTER proto_mismatch. Hoisting it would reclassify a proto-skewed agent as session_not_found_or_deleting and hide the one string that says 'reinstall, do not upgrade'.",
}

// TestEverySubscribedVerbIsGatedOrExempt reconciles broker.go's subscription table against the
// verbSpec table.
func TestEverySubscribedVerbIsGatedOrExempt(t *testing.T) {
	leaves := subscribedCtlVerbs(t)
	if len(leaves) < 12 {
		t.Fatalf("extracted only %d session-scoped subscription leaf/leaves from broker.go — the "+
			"scan is broken, and a broken scan reports every verb gated", len(leaves))
	}

	gated := map[string]bool{}
	for _, s := range []verbSpec{execSpec, runSpec, killSpec, exposeSpec, exposeRmSpec, upgradeSpec} {
		gated[s.verb] = true
	}

	for _, leaf := range leaves {
		if gated[leaf] {
			continue
		}
		reason, exempt := ungatedSubscriptionLeaves[leaf]
		if !exempt {
			t.Errorf("broker.go subscribes %q but nothing gates it: there is no verbSpec for it and "+
				"no entry in ungatedSubscriptionLeaves.\n"+
				"A session-scoped verb reachable on the wire with no session/membership check has no "+
				"backstop — auth_callout JWTs live 24h with no revocation list, and this gate is the "+
				"only runtime revocation point. Either route it through admit() or add it here with "+
				"the reason it is safe.", leaf)
			continue
		}
		if reason == "" {
			t.Errorf("%q is exempted with an empty reason — an exemption without a reason is just a "+
				"slower way of having no check", leaf)
		}
	}

	// The exemption list must not rot: an entry naming a verb nobody subscribes any more hides
	// the next real one.
	present := map[string]bool{}
	for _, l := range leaves {
		present[l] = true
	}
	for leaf := range ungatedSubscriptionLeaves {
		if !present[leaf] {
			t.Errorf("ungatedSubscriptionLeaves exempts %q, which broker.go no longer subscribes — "+
				"delete it", leaf)
		}
	}

	// And every gated verb must actually BE subscribed, else a spec is pointing at nothing.
	for verb := range gated {
		if !present[verb] {
			t.Errorf("verbSpec %q has no subscription in broker.go — either the subject was renamed "+
				"(in which case the verb is now unreachable) or the spec is dead", verb)
		}
	}
}

// subscribedCtlVerbs extracts the session-scoped ctl verb from every subscription subject in
// broker.go's table. Two shapes carry them:
//
//	…s.*.cmd.by.*.node.*.<verb>.req      — the per-node command family
//	…ctrl.by.*.s.*.<verb…>.req           — the session-scoped ctl family (verb may be dotted)
//
// Event subjects (`.ev.`), the pty stream and the non-session `ctrl.by.*.session.*` leaves are
// not ingress commands and are skipped.
func subscribedCtlVerbs(t *testing.T) []string {
	t.Helper()
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "broker.go", nil, 0)
	if err != nil {
		t.Fatalf("parse broker.go: %v", err)
	}

	var out []string
	seen := map[string]bool{}
	add := func(v string) {
		if v != "" && !seen[v] {
			seen[v] = true
			out = append(out, v)
		}
	}

	ast.Inspect(f, func(n ast.Node) bool {
		lit, ok := n.(*ast.BasicLit)
		if !ok || lit.Kind != token.STRING {
			return true
		}
		s, err := strconv.Unquote(lit.Value)
		if err != nil || !strings.HasSuffix(s, ".req") {
			return true
		}
		body := strings.TrimSuffix(s, ".req")
		switch {
		case strings.Contains(body, ".cmd.by.*.node.*."):
			add(body[strings.LastIndex(body, ".node.*.")+len(".node.*."):])
		case strings.Contains(body, ".ctrl.by.*.s.*."):
			leaf := body[strings.Index(body, ".ctrl.by.*.s.*.")+len(".ctrl.by.*.s.*."):]
			// `transfer.*.finalize` -> `finalize`; `proxy.sub.create` -> `proxy.sub`.
			leaf = strings.ReplaceAll(leaf, "*.", "")
			if strings.HasPrefix(leaf, "transfer") {
				leaf = leaf[strings.LastIndex(leaf, ".")+1:]
			} else if strings.HasPrefix(leaf, "proxy.sub") {
				leaf = "proxy.sub"
			}
			add(leaf)
		case strings.Contains(body, ".node.*.register"):
			add("register")
		case strings.Contains(body, ".ctrl.by.*.session."):
			// `session.create` / `session.list` / `session.*.rm`
			leaf := body[strings.Index(body, ".ctrl.by.*.session.")+len(".ctrl.by.*."):]
			add(strings.ReplaceAll(leaf, "*.", ""))
		}
		return true
	})
	return out
}

// TestSubscriptionScanSelfCheck proves the extractor reads the shapes it claims to. Without it,
// an extractor that silently matched nothing would report every subscribed verb gated.
func TestSubscriptionScanSelfCheck(t *testing.T) {
	leaves := subscribedCtlVerbs(t)
	got := map[string]bool{}
	for _, l := range leaves {
		got[l] = true
	}
	// One from each shape the extractor handles. If any disappears, the extractor stopped seeing
	// that shape and its whole family silently left the reconciliation.
	for _, want := range []string{
		"exec",           // .cmd.by.*.node.*.<verb>.req
		"expose-rm",      // same shape, hyphenated verb
		"ps",             // .ctrl.by.*.s.*.<verb>.req
		"caps",           // same shape
		"finalize",       // .ctrl.by.*.s.*.transfer.*.<verb>.req
		"proxy.status",   // dotted leaf
		"proxy.sub",      // dotted leaf collapsed from three actions
		"register",       // node register
		"session.create", // non-session ctl family
		"session.rm",     // wildcard in the middle
	} {
		if !got[want] {
			t.Errorf("the subscription scan did not find %q — that shape is no longer extracted, so "+
				"every verb using it dropped out of the reconciliation silently. Found: %v", want, leaves)
		}
	}
	if got["failed"] || got["complete"] || got["started"] {
		t.Errorf("the scan picked up an EVENT subject as a command verb: %v", leaves)
	}
}
