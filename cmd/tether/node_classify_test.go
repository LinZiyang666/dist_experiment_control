package main

import (
	"errors"
	"sort"
	"strings"
	"testing"
)

// brokerErr builds the error the fleet loop actually receives: dispatchUpgrade routes every non-OK
// broker reply through brokerErrorMessage, which is what attaches the wire code STRUCTURALLY.
//
// origin: line-2 external review 疑惑 #2. These tests used to hand-build `errors.New("... code ...")`,
// which quietly made them assertions about the PROSE — they passed for a classifier that sniffed
// substrings and would have kept passing if the classifier were wrong for every real error. Going
// through the production constructor is the difference between testing the contract and testing the
// test's own string.
func brokerErr(code string) error {
	return brokerErrorMessage("upgrade lab/n1", code, "some detail")
}

// TestIsTransientError covers the codes `--all` skips with a
// warning rather than aborting. Reviewer's recommendation in P10
// round-1 was to keep fleet rollouts moving past one OFFLINE box;
// these are the codes architecture J.4 / G considers retryable.
func TestIsTransientError(t *testing.T) {
	for _, code := range []string{
		"node_offline", "node_not_found", "agent_no_responders", "agent_malformed_resp",
	} {
		if !isTransientError(brokerErr(code)) {
			t.Errorf("transient: %q misclassified as non-transient", code)
		}
	}
	// Transport/context failures carry NO wire code — dispatchUpgrade wraps them with a plain
	// fmt.Errorf. They are the only thing the prose fallback exists for.
	for _, s := range []string{
		"upgrade lab/n1: context deadline exceeded",
		"upgrade lab/n1: context canceled",
	} {
		if !isTransientError(errors.New(s)) {
			t.Errorf("codeless transient: %q misclassified as non-transient", s)
		}
	}
	for _, code := range []string{"not_owner", "url_not_allowed", "sha256_invalid"} {
		if isTransientError(brokerErr(code)) {
			t.Errorf("non-transient: %q misclassified as transient", code)
		}
	}
}

// origin: upgrade-safety plan §4 + internal review S7. smoke_failed aborts
// the fleet (a sha-verified artifact that cannot exec is bad on EVERY node);
// upgrade_in_progress is skipped (a prior upgrade on THIS node self-resolves
// within its register deadline). Both arrive agent_rejected:-wrapped on the
// wire (internal/broker/upgrade.go); brokerErrorMessage strips the wrapper.
func TestUpgradeSafetyCodesFleetClassification(t *testing.T) {
	for _, code := range []string{"smoke_failed", "agent_rejected:smoke_failed"} {
		if !isConfigError(brokerErr(code)) || isTransientError(brokerErr(code)) {
			t.Errorf("%q must abort --all (config), never be skipped", code)
		}
	}
	for _, code := range []string{"upgrade_in_progress", "agent_rejected:upgrade_in_progress"} {
		if !isTransientError(brokerErr(code)) || isConfigError(brokerErr(code)) {
			t.Errorf("%q must be skipped (transient), never abort the fleet", code)
		}
	}
}

// TestIsConfigError covers the codes that abort `--all` because
// the request itself is wrong; no other node will accept it
// either, so dispatching the rest is wasted broker work.
func TestIsConfigError(t *testing.T) {
	for _, code := range []string{
		"not_owner", "url_not_allowed", "sha256_invalid",
		"proto_bump_requires_reinstall", "actor_invalid", "session_not_found_or_deleting",
	} {
		if !isConfigError(brokerErr(code)) {
			t.Errorf("config error: %q misclassified as non-config", code)
		}
	}
	for _, code := range []string{
		"node_offline",
		// The agent_rejected: wrapper must be stripped before the code reaches the classifier, or every
		// agent-rejected code would land in "neither set" purely because of the prefix.
		"agent_rejected:install_failed",
	} {
		if isConfigError(brokerErr(code)) {
			t.Errorf("non-config: %q misclassified as config", code)
		}
	}
}

// TestFleetClassifiersIgnoreCodeNamesAppearingInProse is the property the structural rewrite bought,
// asserted directly.
//
// origin: line-2 external review 疑惑 #2 — "this allows a human error text that happens to contain a
// code to change fleet control flow". The prose these classifiers used to read is brokerCodeHints,
// operator-facing text that exists to be reworded. Two ways it bites, both silent:
//
//	a hint about a bad URL that mentions download_failed  →  a permanently-broken rollout retries forever
//	a hint about an offline box that mentions not_owner   →  one offline box aborts the whole fleet
//
// Adversarial by construction: every sample below carries a code name in its message and a DIFFERENT
// code (or none) structurally. A classifier that reads prose gets every one of them wrong.
//
// The first two samples REWORD THE HINT rather than passing a detail string, because brokerErrorMessage
// REPLACES the broker's detail with the hint whenever one exists — a sample built the naive way would
// have had its planted code name silently discarded and would have proven nothing. The plantedCode
// non-vacuity check below is what caught that; it stays so the next edit cannot re-hollow the test.
func TestFleetClassifiersIgnoreCodeNamesAppearingInProse(t *testing.T) {
	rewordHint := func(t *testing.T, code, hint string) {
		t.Helper()
		orig, had := brokerCodeHints[code]
		t.Cleanup(func() {
			if had {
				brokerCodeHints[code] = orig
				return
			}
			delete(brokerCodeHints, code)
		})
		brokerCodeHints[code] = hint
	}

	tests := []struct {
		name        string
		err         error
		plantedCode string // must literally appear in err.Error(); "" = nothing planted
		transient   bool
		config      bool
	}{
		{
			// The literal review scenario: a reworded operator-facing hint that names a config code, on a
			// failure whose real code is transient.
			name: "transient code, hint prose names a config code",
			err: func() error {
				rewordHint(t, "node_offline", "the agent is OFFLINE. (This is not a not_owner problem.)")
				return brokerErrorMessage("upgrade lab/n1", "node_offline", "detail")
			}(),
			plantedCode: "not_owner",
			transient:   true, config: false,
		},
		{
			name: "config code, hint prose names a transient code",
			err: func() error {
				rewordHint(t, "url_not_allowed", "URL not whitelisted; unrelated to download_failed.")
				return brokerErrorMessage("upgrade lab/n1", "url_not_allowed", "detail")
			}(),
			plantedCode: "download_failed",
			transient:   false, config: true,
		},
		{
			// A codeless transport error whose prose mentions a config code must NOT abort the fleet:
			// the fallback list holds transport needles only, and isConfigError has no fallback at all.
			name:        "no wire code, prose names a config code",
			err:         errors.New("upgrade lab/n1: dial tcp: connection refused (not url_not_allowed)"),
			plantedCode: "url_not_allowed",
			transient:   false, config: false,
		},
		{
			// Same, but the prose also carries a transport needle: the fallback fires on the needle, and
			// the config name in the prose still changes nothing.
			name:        "no wire code, transport needle plus a config code in prose",
			err:         errors.New("upgrade lab/n1: context deadline exceeded while checking not_owner"),
			plantedCode: "not_owner",
			transient:   true, config: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// NON-VACUITY. Without this the samples can silently stop containing the foreign code name —
			// which is exactly what happened on the first draft, where brokerErrorMessage replaced the
			// planted detail with the stock hint and two "adversarial" cases tested nothing at all.
			if tc.plantedCode != "" && !strings.Contains(tc.err.Error(), tc.plantedCode) {
				t.Fatalf("sample does not actually contain the planted code %q; the prose is %q.\n"+
					"A prose-sniffing classifier would pass this case, so it proves nothing.",
					tc.plantedCode, tc.err)
			}
			if got := isTransientError(tc.err); got != tc.transient {
				t.Errorf("isTransientError = %v, want %v\nerr: %v\ncode: %q\n\n"+
					"Classification must come from the structurally-carried wire code, never from the "+
					"operator-facing prose — the prose is written to be reworded.",
					got, tc.transient, tc.err, wireCodeOf(tc.err))
			}
			if got := isConfigError(tc.err); got != tc.config {
				t.Errorf("isConfigError = %v, want %v\nerr: %v\ncode: %q\n\n"+
					"A false config error halts a whole fleet rollout over one node.",
					got, tc.config, tc.err, wireCodeOf(tc.err))
			}
		})
	}
}

// TestBrokerErrorMessageCarriesTheStrippedCode pins the seam the two classifiers now depend on. Without
// it they would silently classify nothing at all: every code would read as "" and every node would count
// as a plain per-node failure — a regression with no error message anywhere.
func TestBrokerErrorMessageCarriesTheStrippedCode(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"not_owner", "not_owner"},
		{"agent_rejected:install_failed", "install_failed"},
		{"a_code_with_no_hint_entry", "a_code_with_no_hint_entry"},
	} {
		if got := wireCodeOf(brokerErrorMessage("upgrade lab/n1", tc.in, "detail")); got != tc.want {
			t.Errorf("wireCodeOf(brokerErrorMessage(%q)) = %q, want %q", tc.in, got, tc.want)
		}
	}
	// The raw code stays in the PROSE even when the carried code is stripped — an operator greps the
	// broker's logs for what the broker actually said.
	if msg := brokerErrorMessage("upgrade lab/n1", "agent_rejected:install_failed", "d").Error(); !strings.Contains(msg, "agent_rejected:install_failed") {
		t.Errorf("prose lost the raw code: %q", msg)
	}
	// wireCodeOf must not invent a code for errors that have none.
	if got := wireCodeOf(errors.New("plain error mentioning not_owner")); got != "" {
		t.Errorf("wireCodeOf(plain error) = %q, want \"\"", got)
	}
}

// TestFleetClassifiersKnowTheCauseSplitCodes pins the line-2 §12 Y2 codes in BOTH fleet classifiers.
//
// origin: line-2 closure verification §6 B1, which the review called its most substantive item. Y2 split
// four codes out of two catch-alls so that automation could tell "retry" from "stop". It updated the exit
// classes and the hint text and missed THIS pair of lists — a third classification of the same codes, in
// the same package, on the one command Y2 exists to rescue.
//
// The failure was concrete: `download_http_status` (a typo'd --url) matched neither list, so
// `node upgrade --all` scored the node "✗ failed" and kept going, fanning the known-bad URL out to every
// remaining node in the fleet. Aborting on exactly that is what isConfigError's call site says it is for.
func TestFleetClassifiersKnowTheCauseSplitCodes(t *testing.T) {
	tests := []struct {
		code      string
		transient bool // --all skips this node and continues
		config    bool // --all aborts the whole fleet
		why       string
	}{
		{"download_http_status", false, true,
			"a typo'd --url returns the same non-2xx on every node; fanning it out is the waste isConfigError prevents"},
		{"download_too_large", false, true,
			"the artifact is over the ceiling on node 2..N as well"},
		{"download_failed", true, false,
			"a transport blip really does clear — skip this node, keep the rollout moving"},
		{"pty_alloc_failed", true, false,
			"fd or pty-count exhaustion frees as sessions close (see internal/agent/run.go ptyTransientErrnos)"},
		{"pty_unavailable", false, false,
			"NEITHER: a host with no /dev/ptmx is not transient, but it is not a bad CALL either — one " +
				"misconfigured box must not abort the fleet, so it stays a per-node failure"},
	}
	for _, tc := range tests {
		t.Run(tc.code, func(t *testing.T) {
			err := brokerErr(tc.code)
			if got := isTransientError(err); got != tc.transient {
				t.Errorf("isTransientError(%s) = %v, want %v — %s", tc.code, got, tc.transient, tc.why)
			}
			if got := isConfigError(err); got != tc.config {
				t.Errorf("isConfigError(%s) = %v, want %v — %s", tc.code, got, tc.config, tc.why)
			}
		})
	}
}

// targetScopedUsageCodes are the codes for which "exit 64" and "skip this node, keep the fleet moving" are
// BOTH correct, because the two mechanisms answer different questions.
//
// The exit class answers "should this PROCESS exit with a retryable code?" — for a single explicit `--nid`
// the answer is no, a human has to start that agent. The fleet classifier answers "should this ONE node
// abort the rollout for all the others?" — and for a code that describes the TARGET rather than the CALL,
// the answer is also no.
//
// Found by the reconciliation below on its first run: both entries predate line 2. They are recorded rather
// than silenced so that the next code added to both mechanisms has to be reasoned about in this frame.
var targetScopedUsageCodes = map[string]string{
	"node_offline": "describes the target box (no recent heartbeat), not the request. Exit 64 for an " +
		"explicit --nid is right (start the agent); skipping it during --all is also right (one offline " +
		"box must not stop a fleet rollout).",
	"node_not_found": "same shape: no agent registered under that nid in this session. Wrong nid on a " +
		"single call is a usage error; a stale nid in a fleet list is one node to skip.",
}

// TestFleetClassifiersReconcileWithTheExitClassTable is the reverse assertion B1 asked for implicitly: the
// thing that let this rot was that node.go's two hand-written substring lists answer the same question as
// brokerCodeExitClasses and nothing compared them.
//
// The rule is deliberately narrow, because the two mechanisms are NOT redundant — a code can legitimately
// be in neither fleet list (pty_unavailable above). What must never happen again is a code the exit-class
// table calls TRANSIENT while the fleet loop treats it as a hard failure, or a code the table calls
// terminal-because-a-human-must-act while the fleet keeps fanning it out. Those two are the states that
// make `--all` contradict `tether`'s own documented retry rule.
func TestFleetClassifiersReconcileWithTheExitClassTable(t *testing.T) {
	// The codes `node upgrade` can actually receive. Scoped to that command on purpose: the exit-class
	// table covers ~90 codes for every command in the CLI, and demanding a fleet disposition for
	// `port_exhausted` would be noise.
	upgradeCodes := []string{
		"download_http_status", "download_too_large", "download_failed",
		"url_not_allowed", "url_not_allowed_local", "sha256_invalid", "sha256_mismatch",
		"proto_bump_requires_reinstall", "node_offline", "node_not_found",
		"agent_no_responders", "agent_malformed_resp", "not_owner", "version_skew",
	}
	stillNeeded := map[string]bool{}
	for _, code := range upgradeCodes {
		class, classified := brokerCodeExitClasses[code]
		if !classified {
			t.Errorf("%s is reachable from `node upgrade` but has no entry in brokerCodeExitClasses — it "+
				"would exit 70 (unclassified) while the fleet loop makes its own decision about it", code)
			continue
		}
		err := brokerErr(code)
		transient, config := isTransientError(err), isConfigError(err)

		if transient && config {
			t.Errorf("%s is in BOTH fleet lists — isTransientError is consulted first, so isConfigError's "+
				"entry is dead and the abort it was added for never happens", code)
		}
		if class == exitTransient && config {
			t.Errorf("%s is exitTransient (75, 'retry this') in brokerCodeExitClasses but aborts the whole "+
				"fleet via isConfigError. One of the two is wrong: a retryable code must not stop a rollout.",
				code)
		}
		if class == exitUsage && transient {
			if why, excused := targetScopedUsageCodes[code]; excused {
				if why == "" {
					t.Errorf("targetScopedUsageCodes[%q] has an empty reason — an exemption that does not "+
						"argue for itself is just a slower way of having no check", code)
				}
				stillNeeded[code] = true
				continue
			}
			t.Errorf("%s is exitUsage (64, 'a human must change something') in brokerCodeExitClasses but the "+
				"fleet loop SKIPS it and keeps going — so `--all` goes on dispatching a request that cannot "+
				"succeed anywhere. This is the §6 B1 defect exactly.\n\n"+
				"If the code describes the TARGET rather than the CALL, the two verdicts are answering "+
				"different questions and the pair is legitimate — add it to targetScopedUsageCodes with the "+
				"reason. If it describes the call, one of the two classifications is wrong.", code)
		}
	}
	// GROWTH CAP + REVERSE ASSERTION. origin: line-2 closure verification Q4. This ledger shipped with an
	// empty-reason check and nothing else — no cap, so it could grow silently, and no reverse assertion, so
	// an entry whose code stopped being exitUsage-and-transient would sit here forever reading as a
	// considered decision. That is the exact shape this increment spent its whole length removing from other
	// ledgers, introduced by the change that fixed the last one.
	const targetScopedCap = 2
	if n := len(targetScopedUsageCodes); n != targetScopedCap {
		t.Errorf("targetScopedUsageCodes has %d entries, expected %d.\n\n"+
			"Each entry silences a real contradiction (exit 64 says 'a human must act', the fleet loop says "+
			"'skip and continue'). Growth must be a visible edit: raise the cap in the same commit and say "+
			"why the code describes the TARGET rather than the CALL. If it went DOWN, lower the cap and lock "+
			"the win in.", n, targetScopedCap)
	}
	var stale []string
	for code := range targetScopedUsageCodes {
		if !stillNeeded[code] {
			stale = append(stale, code)
		}
	}
	sort.Strings(stale)
	if len(stale) > 0 {
		t.Errorf("%d targetScopedUsageCodes entr(y/ies) no longer excuse anything:\n  %s\n\n"+
			"The contradiction they were added for is gone — either the exit class changed, the fleet "+
			"classification changed, or the code left upgradeCodes. Delete the entry in the same commit: an "+
			"exemption that excuses nothing still reads to the next person as a considered decision.",
			len(stale), strings.Join(stale, "\n  "))
	}

}
