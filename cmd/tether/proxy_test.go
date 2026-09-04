package main

import (
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

// TestProxyCredentialRotationRunbookUsesARealCommand keeps the incident
// remediation executable. The leaked credentials remain valid until every
// subscriber is revoked, so a typo in the exact command block is a security
// failure rather than a documentation nicety.
//
// It is the cheap regression pin for the exact spelling that shipped;
// TestEveryProxyCommandShownToOperatorsExists below is the general form, and is
// the one that would have caught the other two sites.
func TestProxyCredentialRotationRunbookUsesARealCommand(t *testing.T) {
	body, err := os.ReadFile("../../docs/broker-ops.md")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(body), "tether proxy sub rm") {
		t.Fatal("credential-rotation runbook calls `tether proxy sub rm`, but the CLI exposes only " +
			"`proxy sub revoke`; step 2 aborts before any leaked subscriber PSK/bearer is revoked")
	}
}

// `tether proxy on` without --yes in a non-interactive context must ABORT
// (and therefore never send a NATS request that would flip the switch).
func TestProxyOnAbortsWithoutYesNonInteractive(t *testing.T) {
	root := newProxyCmd()
	root.SetArgs([]string{"on"})
	root.SetOut(io.Discard)
	root.SetErr(io.Discard)
	err := root.Execute()
	if err == nil {
		t.Fatal("proxy on without --yes (non-interactive) must error, not proceed")
	}
	if !strings.Contains(err.Error(), "aborted") {
		t.Fatalf("expected an abort error, got: %v", err)
	}
}

// `proxy sub create` requires --name.
func TestProxySubCreateRequiresName(t *testing.T) {
	root := newProxyCmd()
	root.SetArgs([]string{"sub", "create"})
	root.SetOut(io.Discard)
	root.SetErr(io.Discard)
	if err := root.Execute(); err == nil || !strings.Contains(err.Error(), "name") {
		t.Fatalf("expected a --name-required error, got: %v", err)
	}
}

// Every P13 broker code maps to a human hint (no raw-code fallthrough).
func TestProxyErrorHintsMapped(t *testing.T) {
	for _, code := range []string{
		"not_owner", "sub_name_invalid", "sub_name_taken", "sub_not_found",
		"already_revoked", "subject_malformed", "proxy_disabled", "name_reserved",
	} {
		if _, ok := brokerCodeHints[code]; !ok {
			t.Errorf("proxy code %q has no operator hint", code)
		}
		msg := brokerErrorMessage("proxy x", code, "raw")
		if msg == nil || !strings.Contains(msg.Error(), brokerCodeHints[code]) {
			t.Errorf("code %q: error message does not include its hint", code)
		}
	}
}

// ─── every proxy command we spell out to an operator must exist ──────────────
//
// origin: prerelease audit external review round 3, the `proxy sub rm` finding.
//
// THE INCIDENT: three places told an operator to run `tether proxy sub rm` — the
// credential-rotation runbook in broker-ops.md, the N-1 exception in requirements.md,
// and the broker's OWN reply string in internal/broker/proxy.go. The CLI has never had
// that verb; it has `revoke`. Step ② of the rotation procedure therefore aborts, and the
// leaked bearer token that the whole procedure exists to invalidate stays valid.
//
// WHY THIS GUARD IS SHAPED LIKE THIS. The three sites were fixed by hand, one at a time,
// because nothing mechanical related them — the same failure mode this repo's memory
// already records as recurring ("改一个命令的契约后必须全局扫所有调用点，含产品打印给运维
// 的手抄文案"). A grep for the literal `tether proxy sub rm` in one file does not relate
// them either: a denylist answers only the question somebody already asked, so spelling
// the mistake `proxy sub delete`, or making it in usage.md, or in a product string, walks
// straight past it. So the legal verbs are DERIVED from the cobra tree, and every citation
// is checked — documents and product strings alike, because the operator reads both.
func TestEveryProxyCommandShownToOperatorsExists(t *testing.T) {
	proxyCmd := newProxyCmd()
	sources := operatorFacingProxySources(t)
	cited := 0
	for path, body := range sources {
		for _, words := range proxyCitationsIn(body) {
			cited++
			if bad := resolveProxyVerbs(proxyCmd, words); bad != "" {
				t.Errorf("%s cites `proxy %s`, but %q is not a subcommand there; the CLI offers %v.\n\n"+
					"An operator who copies this line gets an error instead of the effect the text "+
					"promises. If this is prose rather than a command, it still reads as one — rewrite "+
					"it so it does not.",
					path, strings.Join(words, " "), bad, proxyVerbsUnder(proxyCmd, words))
			}
		}
	}
	// NON-VACUITY. A regex that quietly stopped matching would pass this test for every
	// spelling, including the one it was written for. The tree really is cited dozens of
	// times across the runbook, the manual and the CLI's own output.
	if cited < 15 {
		t.Fatalf("only %d proxy command citations found across %d sources — the scanner is not "+
			"reading the text it is supposed to police", cited, len(sources))
	}
}

// TestProxyCommandCitationGuardCanActuallyFail is the positive/negative control for the
// guard above: it must reject the spelling that shipped and accept the real ones.
// gate-control: TestEveryProxyCommandShownToOperatorsExists
func TestProxyCommandCitationGuardCanActuallyFail(t *testing.T) {
	proxyCmd := newProxyCmd()

	const shipped = "② 逐个作废：`tether proxy sub rm -s <sid> --name <each>`"
	bad := proxyCitationsIn(shipped)
	if len(bad) == 0 {
		t.Fatal("the citation regexes no longer see a real command line at all")
	}
	rejected := false
	for _, words := range bad {
		if resolveProxyVerbs(proxyCmd, words) == "rm" {
			rejected = true
		}
	}
	if !rejected {
		t.Fatal("the guard no longer rejects `proxy sub rm` — that is the exact spelling the " +
			"external review found in three places, and it must stay rejected")
	}

	// A leaf's positional argument and an alias are NOT missing verbs; if they were, the
	// guard would be red on correct text and somebody would delete it.
	for _, ok := range []string{
		"`tether proxy sub revoke alice`", "`tether proxy sub list`", "`tether proxy sub ls -s x`",
		"`tether proxy on --yes`", "run `proxy sub create` first",
	} {
		for _, words := range proxyCitationsIn(ok) {
			if v := resolveProxyVerbs(proxyCmd, words); v != "" {
				t.Fatalf("guard rejects the legitimate line %q at verb %q", ok, v)
			}
		}
	}
}

var (
	// Two citation shapes. "tether proxy …" is the operator-copyable form used by runbooks
	// and by the CLI's own hints; "proxy sub …" is the shorter form the broker prints back
	// at a client that is already inside this command family (that is where the incident's
	// third site lived).
	//
	// Both stop at the first token that is not a bare lowercase word, so flags (`-s`),
	// placeholders (`<sid>`), format verbs (`%s`) and CJK prose end a citation instead of
	// being mistaken for a subcommand.
	tetherProxyCitationRe = regexp.MustCompile(`tether proxy((?: +[a-z][a-z0-9-]*)+)`)
	proxySubCitationRe    = regexp.MustCompile(`proxy sub((?: +[a-z][a-z0-9-]*)+)`)
)

// proxyCitationsIn returns each cited verb chain below `proxy`, e.g. ["sub", "revoke"].
func proxyCitationsIn(body string) [][]string {
	var out [][]string
	for _, m := range tetherProxyCitationRe.FindAllStringSubmatch(body, -1) {
		out = append(out, strings.Fields(m[1]))
	}
	for _, m := range proxySubCitationRe.FindAllStringSubmatch(body, -1) {
		out = append(out, append([]string{"sub"}, strings.Fields(m[1])...))
	}
	return out
}

// resolveProxyVerbs walks words down the proxy command tree and returns the first one
// that does not exist. A word that a LEAF does not recognise is a positional argument
// (`proxy sub revoke alice`), not a missing verb, so only a command that still has
// children can be said to be missing one.
func resolveProxyVerbs(proxyCmd *cobra.Command, words []string) string {
	cur := proxyCmd
	for _, w := range words {
		next := proxyChildNamed(cur, w)
		if next == nil {
			if len(cur.Commands()) == 0 {
				return ""
			}
			return w
		}
		cur = next
	}
	return ""
}

// proxyChildNamed resolves one word against a command's children, ALIASES INCLUDED —
// `proxy sub list` is the documented alias of `ls` and must not read as a defect.
func proxyChildNamed(c *cobra.Command, name string) *cobra.Command {
	for _, sub := range c.Commands() {
		if sub.Name() == name {
			return sub
		}
		for _, alias := range sub.Aliases {
			if alias == name {
				return sub
			}
		}
	}
	return nil
}

// proxyVerbsUnder lists what the failing citation's parent actually offers, so the
// failure message names the fix instead of only the mistake.
func proxyVerbsUnder(proxyCmd *cobra.Command, words []string) []string {
	cur := proxyCmd
	for _, w := range words {
		next := proxyChildNamed(cur, w)
		if next == nil {
			break
		}
		cur = next
	}
	var out []string
	for _, sub := range cur.Commands() {
		out = append(out, sub.Name())
	}
	return out
}

// operatorFacingProxySources returns every file whose text an operator is expected to act
// on: the ACTIVE documents, plus the non-test Go sources that print commands back at them.
func operatorFacingProxySources(t *testing.T) map[string]string {
	t.Helper()
	out := map[string]string{}

	// TRACKED top-level docs only — the same rule, and the same reason, as
	// test/architecture/docs_layout_test.go: docs/reviews/ holds FROZEN reports that
	// legitimately quote the wrong command as evidence of the finding, and
	// docs/cluster-ha-realmachine-test-plan.md is a deliberately untracked local document.
	listed, err := gitListFiles(t, "docs")
	if err != nil {
		t.Fatalf("git ls-files docs: %v", err)
	}
	docs := 0
	for _, p := range listed {
		if filepath.Dir(p) != "docs" || filepath.Ext(p) != ".md" {
			continue
		}
		out[p] = readRepoFile(t, p)
		docs++
	}
	if docs < 8 {
		t.Fatalf("only %d tracked markdown files at docs/ top level — `git ls-files` is not "+
			"returning the active documents this guard is supposed to read", docs)
	}

	// PRODUCT STRINGS. The broker's replies and the CLI's own hints reach the same operator
	// as the runbook does, and the incident had one site in each.
	for _, dir := range []string{"cmd/tether", "internal/broker"} {
		entries, rerr := os.ReadDir(filepath.Join("..", "..", dir))
		if rerr != nil {
			t.Fatal(rerr)
		}
		for _, e := range entries {
			name := e.Name()
			if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
				continue
			}
			p := filepath.ToSlash(filepath.Join(dir, name))
			out[p] = readRepoFile(t, p)
		}
	}
	return out
}

func gitListFiles(t *testing.T, dir string) ([]string, error) {
	t.Helper()
	cmd := exec.Command("git", "ls-files", dir)
	cmd.Dir = filepath.Join("..", "..")
	body, err := cmd.Output()
	if err != nil {
		return nil, err
	}
	return strings.Fields(string(body)), nil
}

func readRepoFile(t *testing.T, rel string) string {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("..", "..", rel))
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}
