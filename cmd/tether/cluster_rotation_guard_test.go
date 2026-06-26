package main

import (
	"os"
	"strings"
	"testing"
)

// cluster_rotation_guard_test.go (C7 §3.1) — rejection #2 STRUCTURAL guard: the credential-rotation
// flow must NEVER read a private key and write/copy/publish/exec it elsewhere. ALLOW-LIST token-scan
// over cluster_rotation.go: the ONLY secret-touching helpers permitted are the four public/hash/perm-
// only readers; any key-exfil primitive (write/copy/publish/remote-exec of key material) is forbidden.
func TestC7RotationNoPrivateKeyExfilStatic(t *testing.T) {
	src, err := os.ReadFile("cluster_rotation.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(src)

	// Forbid any key-exfil primitive in the rotation file (it is a printer + alert text only). A drill
	// harness legitimately writes per-node leaf keys to its own dir — but this guard is scoped to the
	// production rotation CLI file, which must move NOTHING.
	// Comprehensive forbidden set across the READ / WRITE / TRANSPORT / EXEC families (M1: the prior
	// 8-token denylist had holes — os.Open+io.ReadAll, os.Create/.Write, http.Post, net.Dial). The
	// rotation file must touch secrets ONLY through the sanctioned public/hash readers; it may do
	// fmt.Fprintf over an io.Writer + callAdmin (alert text), nothing else fs/net/exec.
	forbidden := []string{
		// READ — the file must not read raw bytes itself; only the sanctioned helpers do.
		".ReadFile(", "os.Open(", "os.OpenFile(", "io.ReadAll(", "ioutil.ReadFile", "ioutil.ReadAll", "os.ReadDir(",
		// WRITE — no writing a key anywhere.
		"os.WriteFile", "ioutil.WriteFile", "os.Create(", "io.Copy(",
		// TRANSPORT — no streaming key material off-box.
		".Publish(", ".Request(", "http.Post", "http.NewRequest", "http.Get", "net.Dial",
		// EXEC — no scp/ssh/rsync shell-out.
		"os/exec", "exec.Command",
	}
	for _, tok := range forbidden {
		if strings.Contains(text, tok) {
			t.Errorf("cluster_rotation.go contains forbidden key-exfil/raw-read primitive %q — the rotation "+
				"flow must be a printer that moves NO private key material (rejection #2)", tok)
		}
	}

	// Forbid importing the network/exec packages outright (a transport primitive must not even be
	// reachable from the rotation file).
	for _, imp := range []string{`"net/http"`, `"net"`, `"os/exec"`} {
		if strings.Contains(text, imp) {
			t.Errorf("cluster_rotation.go imports %s — the rotation flow must not reach the network/exec families (rejection #2)", imp)
		}
	}

	// Positive: the secret-derived values it DOES show must come only through the sanctioned public/hash
	// readers (it derives the public account key + the cluster-ca sha256 — never a private seed).
	for _, want := range []string{"readClusterPublicIdentities", "AccountFingerprint"} {
		if !strings.Contains(text, want) {
			t.Errorf("expected the rotation guide to source fingerprints via the sanctioned reader %q", want)
		}
	}
}
