package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nats-io/nkeys"
)

// cluster_rotation_test.go (C7) — the guided rotation printer's secret-free + content invariants +
// the tracking-key SSOT.

// TestC7RotationFlagsRequireCompromised: --require-credential-rotation without --compromised is a
// usage error, caught BEFORE any admin socket call.
func TestC7RotationFlagsRequireCompromised(t *testing.T) {
	sock := "/nonexistent/admin.sock"
	cmd := newClusterRetireCmd(&sock)
	var b strings.Builder
	cmd.SetOut(&b)
	cmd.SetErr(&b)
	cmd.SetArgs([]string{"brk-b", "--require-credential-rotation"})
	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "require-credential-rotation requires --compromised") {
		t.Fatalf("expected a usage error for require-without-compromised, got %v\n%s", err, b.String())
	}
}

func TestC7RotationTrackingKeySSOT(t *testing.T) {
	if got := rotationTrackingKey("brk-b"); got != "manual:credrot:brk-b" {
		t.Fatalf("tracking key SSOT: %q", got)
	}
}

// TestC7RotationGuidePrintsNoSecrets seeds a secrets dir with a REAL account.nk seed (+ canary files)
// and asserts the printed guide carries the PUBLIC account key + ca-sha256 but NONE of the private
// seed bytes / no PEM private-key marker.
func TestC7RotationGuidePrintsNoSecrets(t *testing.T) {
	dir := t.TempDir()
	// A real ed25519 account seed — derivePublicKey returns its PUBLIC A… form; the seed must never print.
	kp, err := nkeys.CreateAccount()
	if err != nil {
		t.Fatal(err)
	}
	seed, err := kp.Seed()
	if err != nil {
		t.Fatal(err)
	}
	pub, err := kp.PublicKey()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "account.nk"), seed, 0o600); err != nil {
		t.Fatal(err)
	}
	canary := "-----BEGIN PRIVATE KEY-----\nCANARYROUTEKEYDEADBEEF\n-----END PRIVATE KEY-----\n"
	if err := os.WriteFile(filepath.Join(dir, "route-key.pem"), []byte(canary), 0o600); err != nil {
		t.Fatal(err)
	}

	var b strings.Builder
	printCredentialRotationGuide(&b, "brk-b", dir, filepath.Join(dir, "nonexistent.conf"), true)
	out := b.String()

	if strings.Contains(out, string(seed)) {
		t.Error("guide leaked the account.nk SEED bytes")
	}
	if strings.Contains(out, "CANARYROUTEKEYDEADBEEF") || strings.Contains(out, "BEGIN PRIVATE KEY") {
		t.Error("guide leaked private key material")
	}
	if !strings.Contains(out, pub) {
		t.Errorf("guide should show the PUBLIC account key %s for verification", pub)
	}
	// The non-boundary + rejection-#2 messaging must be present.
	for _, want := range []string{"NOT A TRUST REVOCATION", "NEVER copies private keys", rotationTrackingKey("brk-b")} {
		if !strings.Contains(out, want) {
			t.Errorf("guide missing required text %q", want)
		}
	}
}

func TestC7RotationGuideEnumeratesExclusionsAndCaveats(t *testing.T) {
	var b strings.Builder
	printCredentialRotationGuide(&b, "brk-b", t.TempDir(), "", true)
	out := b.String()
	for _, want := range []string{
		"broker.nk", "node-ident.nk", // exclusions named
		"TTL",                // old-JWT-TTL caveat
		"proxy subscription", // proxy-PSK note
		"rotate-tunnel-cert", // optional tunnel-cert DiD step
		"reconcile nats",     // account.nk reseed step
	} {
		if !strings.Contains(out, want) {
			t.Errorf("guide missing %q", want)
		}
	}
}

func TestC7RotationClearIsExistingAlertClear(t *testing.T) {
	var b strings.Builder
	printCredentialRotationGuide(&b, "brk-b", t.TempDir(), "", true)
	if !strings.Contains(b.String(), "tether alert clear manual:credrot:brk-b") {
		t.Error("the 'when done' step must use the existing `tether alert clear` (no new completion command)")
	}
}

// TestC7AdvisoryGuideDoesNotPromiseUnraisedAlert (M2): the advisory (--compromised-only) guide
// (raised=false) must NOT tell the operator a severe alert is live nor to `alert clear` — that path
// raises no alert. The require path (raised=true) keeps those lines.
func TestC7AdvisoryGuideDoesNotPromiseUnraisedAlert(t *testing.T) {
	var adv strings.Builder
	printCredentialRotationGuide(&adv, "brk-b", t.TempDir(), "", false)
	a := adv.String()
	if strings.Contains(a, "alert clear "+rotationTrackingKey("brk-b")) {
		t.Error("advisory guide must NOT instruct `alert clear` for an alert it never raised")
	}
	if strings.Contains(a, "severe alert "+rotationTrackingKey("brk-b")) {
		t.Error("advisory guide must NOT claim a live severe alert exists")
	}
	// But it must still loudly say NOT SAFE + name the require flag to get the tracking alert.
	if !strings.Contains(a, "NOT SAFE") || !strings.Contains(a, "require-credential-rotation") {
		t.Errorf("advisory guide must still warn NOT SAFE + point at --require-credential-rotation:\n%s", a)
	}
	// The require path DOES promise + instruct the alert.
	var req strings.Builder
	printCredentialRotationGuide(&req, "brk-b", t.TempDir(), "", true)
	if !strings.Contains(req.String(), "alert clear "+rotationTrackingKey("brk-b")) {
		t.Error("require guide must instruct `alert clear`")
	}
}

func TestC7NotSafeBannerNotGreen(t *testing.T) {
	var b strings.Builder
	printNotSafeBanner(&b, "brk-b")
	out := b.String()
	if !strings.Contains(out, "NOT SAFE") || !strings.Contains(out, "NOT yet rotated") {
		t.Errorf("banner must say NOT SAFE / NOT yet rotated:\n%s", out)
	}
	// No POSITIVE safety claim (rejection #5). "complete the rotation" is an imperative (do the work),
	// not a "rotation complete" claim — so forbid only the affirmative phrasings.
	low := strings.ToLower(out)
	for _, forbidden := range []string{"secure", "is safe", "now safe", "rotation complete", "rotation done", "credentials rotated"} {
		if strings.Contains(low, forbidden) {
			t.Errorf("banner must not imply safety with %q:\n%s", forbidden, out)
		}
	}
}
