package main

import (
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// origin: error_class_external_review_test.go (renamed in B6)
//
// TestExternalReviewProtoMismatchClassesAreTerminal pins the shared operational
// remedy of the two cross-proto refusal codes: reinstall a matching binary.
// Retrying the same request cannot change either outcome.
func TestExternalReviewProtoMismatchClassesAreTerminal(t *testing.T) {
	for _, code := range []string{"proto_mismatch", "proto_bump_requires_reinstall"} {
		if got := brokerCodeExitClass(code); got != exitUsage {
			t.Errorf("%s exits %d, want %d: docs instruct a full reinstall, so blind retry cannot succeed",
				code, got, exitUsage)
		}
	}
}

// TestExternalReviewErrorCodeGateSeesVariableHelperArguments exercises the
// plan's required form 3: a code flowing through a local variable into a known
// code-carrying helper must not disappear without either classification or an
// explicit unresolved-site exemption.
func TestExternalReviewErrorCodeGateSeesVariableHelperArguments(t *testing.T) {
	dir := t.TempDir()
	src := `package sample
func replyExposeErr(msg any, code string, detail string) {}
func emit() {
	code := "external_review_unclassified"
	replyExposeErr(nil, code, "")
}
`
	if err := os.WriteFile(filepath.Join(dir, "sample.go"), []byte(src), 0o600); err != nil {
		t.Fatal(err)
	}
	codes, unresolved := scanTree(t, dir, []string{"."})
	if len(codes) == 0 && len(unresolved) == 0 {
		t.Fatal("variable helper argument vanished from both emitted and unresolved results; the coverage gate is blind")
	}
}

// TestExternalReviewErrorCodeGateReportsEveryDynamicSite prevents a first
// unresolved call from hiding every later dynamic call in the same file. An
// exemption is site-scoped only if the scanner returns every site; storing one
// map entry per file keeps the old blanket-exemption behavior under a new
// file:line-shaped key.
//
// origin: p-b2 internal review NIT n4. The comment above has always said "in the
// same FILE", but the fixture put both sites in the same FUNCTION — the one
// arrangement the file:FUNCTION#ordinal key handled correctly even while it was
// collapsing two sites into one key across functions (M1/M2). The test that
// existed to prove sites cannot hide each other was structurally blind to the way
// they actually did. Both arrangements are now rows.
func TestExternalReviewErrorCodeGateReportsEveryDynamicSite(t *testing.T) {
	for _, tc := range []struct {
		name string
		src  string
	}{
		{"same function", `package sample
func replyExposeErr(msg any, code string, detail string) {}
func emit(first, second string) {
	replyExposeErr(nil, first, "")
	replyExposeErr(nil, second, "")
}
`},
		{"different functions", `package sample
func replyExposeErr(msg any, code string, detail string) {}
func emitFirst(first string) { replyExposeErr(nil, first, "") }
func emitSecond(second string) { replyExposeErr(nil, second, "") }
`},
		{"different receivers, same method name", `package sample
func replyExposeErr(msg any, code string, detail string) {}
type backendA struct{}
type backendB struct{}
func (a *backendA) Handle(code string) { replyExposeErr(nil, code, "") }
func (b *backendB) Handle(code string) { replyExposeErr(nil, code, "") }
`},
		{"file scope, separated by a func", `package sample
type resp struct{ Code string }
var dyn string
var one = resp{Code: dyn}
func spacer() {}
var two = resp{Code: dyn}
`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, "sample.go"), []byte(tc.src), 0o600); err != nil {
				t.Fatal(err)
			}
			_, unresolved := scanTree(t, dir, []string{"."})
			if len(unresolved) != 2 {
				t.Fatalf("scanner returned %d unresolved site(s), want 2; two sites in one file must not "+
					"collapse into one key, whatever their arrangement: %v", len(unresolved), unresolved)
			}
		})
	}
}

// TestUnresolvedSiteKeysAreInjective is the M1/M2 guard, and the reason the site key carries a receiver.
//
// origin: p-b2 internal review M1 + M2. A site key that is not injective does not merely rot — it
// SILENTLY ABSORBS a brand-new unresolved site into an exemption written for a different one, with every
// gate green and nothing visible in the diff. The old key had two independent ways to do that: it used
// the bare method name (so two receivers collided), and it zeroed the ordinal counter on every FuncDecl
// exit (so the <file-scope> bucket restarted after each func, no name collision needed).
//
// This asserts the whole key set, not just its size, because the shape is the claim: if the receiver
// ever stops appearing, the collision comes straight back and only an exact-set assertion notices.
func TestUnresolvedSiteKeysAreInjective(t *testing.T) {
	dir := t.TempDir()
	// Five unresolved sites, arranged as the three shapes that can collide:
	// two same-named methods on different receivers, a plain function of that same name, and two
	// package-level sites separated by a func.
	src := `package sample

type resp struct{ Code string }

type backendA struct{}
type backendB struct{}

var dyn string

var fileScopeOne = resp{Code: dyn}

func spacer() {}

var fileScopeTwo = resp{Code: dyn}

func (a *backendA) HandleCluster(code string) resp { return resp{Code: code} }

func (b *backendB) HandleCluster(code string) resp { return resp{Code: code} }

func HandleCluster(code string) resp { return resp{Code: code} }
`
	if err := os.WriteFile(filepath.Join(dir, "sample.go"), []byte(src), 0o600); err != nil {
		t.Fatal(err)
	}
	_, unresolved := scanTree(t, dir, []string{"."})

	want := map[string]bool{
		"sample.go:(*backendA).HandleCluster#1": true,
		"sample.go:(*backendB).HandleCluster#1": true,
		"sample.go:HandleCluster#1":             true,
		"sample.go:<file-scope>#1":              true,
		"sample.go:<file-scope>#2":              true,
	}
	var got []string
	for k := range unresolved {
		got = append(got, k)
	}
	sort.Strings(got)
	if len(unresolved) != len(want) {
		t.Fatalf("scanner produced %d key(s) for 5 unresolved sites — keys COLLIDED, so one exemption "+
			"would silently cover two sites:\n  %s", len(unresolved), strings.Join(got, "\n  "))
	}
	for k := range unresolved {
		if !want[k] {
			t.Errorf("unexpected site key %q; the key must be file:RECEIVER-QUALIFIED-FUNC#ordinal so that "+
				"two receivers cannot share a sequence. Got:\n  %s", k, strings.Join(got, "\n  "))
		}
	}
}

func TestExternalReviewUnresolvedCodeExemptionsAreSiteScopedAndLive(t *testing.T) {
	root := repoRoot(t)
	_, unresolved := scanTree(t, root, scannedTrees)
	for key := range unresolvedCodeSites {
		// The key is file:FUNCTION#ordinal. Both halves of the shape are load-bearing and are checked
		// separately, because they fail for different reasons:
		//
		//	no "#ordinal"  -> the key names a FUNCTION, and a function-wide exemption hides every future
		//	                  dynamic code added anywhere in it. clusterstatus.go's HandleCluster alone
		//	                  holds ten unresolved sites, so this is not a theoretical concern.
		//	no ":"         -> the key names a FILE, the original defect (external review R1).
		i := strings.LastIndexByte(key, ':')
		if i < 0 {
			t.Errorf("file-wide unresolved exemption %q can hide every future dynamic code in that file", key)
			continue
		}
		h := strings.LastIndexByte(key, '#')
		if h < i {
			t.Errorf("unresolved exemption %q has no #ordinal, so it covers the whole function rather than "+
				"one site — a second dynamic code added to that function would inherit the exemption "+
				"silently. Key it as file:FUNCTION#N.", key)
			continue
		}
		if n, err := strconv.Atoi(key[h+1:]); err != nil || n < 1 {
			t.Errorf("unresolved exemption %q has a malformed ordinal (%q); it must be a 1-based site index "+
				"within the enclosing function", key, key[h+1:])
			continue
		}
		if _, ok := unresolved[key]; !ok {
			t.Errorf("unresolved exemption %q no longer names an unresolved code site", key)
		}
	}
}
