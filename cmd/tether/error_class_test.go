package main

import (
	"os"
	"path/filepath"
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
func TestExternalReviewErrorCodeGateReportsEveryDynamicSite(t *testing.T) {
	dir := t.TempDir()
	src := `package sample
func replyExposeErr(msg any, code string, detail string) {}
func emit(first, second string) {
	replyExposeErr(nil, first, "")
	replyExposeErr(nil, second, "")
}
`
	if err := os.WriteFile(filepath.Join(dir, "sample.go"), []byte(src), 0o600); err != nil {
		t.Fatal(err)
	}
	_, unresolved := scanTree(t, dir, []string{"."})
	if len(unresolved) != 2 {
		t.Fatalf("scanner returned %d unresolved site(s), want 2; sites in one file must not hide each other: %v",
			len(unresolved), unresolved)
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
