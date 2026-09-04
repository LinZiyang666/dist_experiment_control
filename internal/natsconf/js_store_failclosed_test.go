package natsconf

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// origin: r16_g67_g69_external_review_test.go (renamed in B6) — docs/reviews/r16-g67-g69-external-review.md
//
// TestExternalReviewJSStoreRootStatErrorFailsClosed pins the function's documented
// fail-closed contract at the root itself. A returning joiner's store path can be a
// symlink; ELOOP (or EACCES/I/O failure in production) must not be interpreted as
// "absent/empty", because that silently skips the grow-time stale-store reset.
func TestExternalReviewJSStoreRootStatErrorFailsClosed(t *testing.T) {
	store := filepath.Join(t.TempDir(), "jetstream")
	if err := os.Symlink(store, store); err != nil {
		t.Fatal(err)
	}
	// Signature adapted to the (hasData, err) API this review asked for; the assertion is unchanged.
	hasData, _ := JSStoreHasData(store)
	if !hasData {
		t.Fatal("an unstatable JetStream store must fail closed as potentially data-bearing; false means the joiner reset is silently skipped")
	}
}

func TestExternalReviewJSStoreSymlinkDoesNotHideData(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "real-store")
	store := filepath.Join(root, "jetstream")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(target, "meta.db"), []byte("clustered metadata"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, store); err != nil {
		t.Fatal(err)
	}
	hasData, err := JSStoreHasData(store)
	if err != nil {
		t.Fatalf("a resolvable symlinked store must not error: %v", err)
	}
	if !hasData {
		t.Fatal("a symlinked JetStream store hid data in its target; WalkDir must inspect the directory that os.Stat accepted")
	}
}

// TestExternalReviewStaleSentinelCannotDisarmADataBearingReset models a
// successful move whose backup was archived by the operator, followed by new
// writes into the recreated store before the operation completed. The sentinel
// proves only that an earlier move happened; it cannot prove that the current
// store is still empty. Treating it as an unconditional no-op boots stale/live
// JS state through the very cutover the reset is meant to protect.
func TestExternalReviewStaleSentinelCannotDisarmADataBearingReset(t *testing.T) {
	root := t.TempDir()
	store := filepath.Join(root, "jetstream")
	backup := filepath.Join(root, "jetstream.grow-bak.op")
	sentinel := filepath.Join(root, ".grow-reset.done")
	if err := os.Mkdir(store, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(store, "new-meta"), []byte("live clustered state"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(sentinel, []byte("an earlier move happened"), 0o600); err != nil {
		t.Fatal(err)
	}

	moved, err := MoveAsideJSStore(store, backup, sentinel, true)
	if err != nil {
		t.Fatal(err)
	}
	if moved == "" {
		t.Fatal("a stale sentinel suppressed reset of a currently data-bearing store")
	}
}

// origin: line-2 C2. MoveAsideJSStore's stat of the store root was the one fail-OPEN left in a
// function whose two other disk checks both fail closed:
//
//	if fi, serr := os.Stat(storeDir); serr != nil || !fi.IsDir() {
//	    return "", nil // no store dir on disk yet — nothing to reset
//	}
//
// ANY stat error meant "nothing to reset". os.Stat fails on EACCES, a broken symlink and I/O errors as
// well as on absence, and in each of those the store may exist and hold data. Worse, the early return
// jumps PAST the m4 ReadDir branch three lines below, so the "a store we cannot enumerate is
// potentially data-bearing" rule never got to apply — the one guard written for exactly this case was
// unreachable from this path.
//
// The two halves are asserted separately because collapsing them is how the bug was written in the
// first place: absence and unreadability are different facts and only one of them is safe to ignore.
func TestJSStoreRootStatErrorDoesNotSilentlySkipTheReset(t *testing.T) {
	root := t.TempDir()
	parent := filepath.Join(root, "locked")
	if err := os.Mkdir(parent, 0o700); err != nil {
		t.Fatal(err)
	}
	store := filepath.Join(parent, "jetstream")
	if err := os.Mkdir(store, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(store, "meta"), []byte("live data"), 0o600); err != nil {
		t.Fatal(err)
	}
	// Remove search permission on the parent so stat(store) fails with EACCES while the store very much
	// still exists and holds data. Running as root defeats this, so skip rather than assert a false pass.
	if err := os.Chmod(parent, 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(parent, 0o700) })
	if _, err := os.Stat(store); err == nil {
		t.Skip("stat still succeeds on an unsearchable parent (running as root?) — the premise does not hold here")
	}

	moved, err := MoveAsideJSStore(store, filepath.Join(root, "bak"), "", true)
	if err == nil {
		t.Errorf("an unstatable store returned (%q, nil) — the caller reads that as "+
			"'no store, nothing to reset' and proceeds to start a nats-server over data it never "+
			"inspected", moved)
	}
	if moved != "" {
		t.Errorf("no move can have happened, but the function reported backup path %q", moved)
	}
}

// TestJSStoreAbsentRootIsStillANoOp is the other half: a store that genuinely does not exist must
// remain a silent no-op. Without this, "fail closed on stat errors" could be satisfied by failing on
// EVERY stat error including ENOENT, which would make a first-ever grow return an error.
func TestJSStoreAbsentRootIsStillANoOp(t *testing.T) {
	root := t.TempDir()
	moved, err := MoveAsideJSStore(filepath.Join(root, "nope"), filepath.Join(root, "bak"), "", true)
	if err != nil {
		t.Fatalf("an absent store must be a no-op, got error: %v", err)
	}
	if moved != "" {
		t.Errorf("an absent store reported a backup path %q", moved)
	}
}

// origin: line-2 independent external review. A configured JetStream store path with the wrong
// filesystem shape is corruption/misconfiguration, not evidence that no store exists. Returning
// ("", nil) lets a destructive lifecycle command continue after inspecting neither data nor ownership.
func TestMoveAsideJSStoreRejectsNonDirectoryStorePath(t *testing.T) {
	root := t.TempDir()
	store := filepath.Join(root, "jetstream")
	if err := os.WriteFile(store, []byte("not a directory"), 0o600); err != nil {
		t.Fatal(err)
	}

	moved, err := MoveAsideJSStore(store, filepath.Join(root, "backup"), "", true)
	if err == nil {
		t.Fatalf("a non-directory JetStream store path returned (%q, nil); only an absent path may be "+
			"treated as 'nothing to reset'", moved)
	}
	if moved != "" {
		t.Fatalf("the invalid store path was not moved, but the function reported backup %q", moved)
	}
}

// origin: prerelease audit round 2, G6 (the guard) and cli-serve-agent-cluster/L4-F1
// (the defect it closes).
//
// A CLIENT BIND MUST NOT VANISH IN A RENDER.
//
// Render omits the bind when Config.ClientListen is empty, and nats-server then
// defaults to 0.0.0.0:4222 — so a broker whose conf bound clients to a private
// address silently becomes PUBLICLY bound, on a file `nats-server -t` accepts, so
// the DryRun gate waves it through. Three of the five assembly paths reach
// BuildMergedConf without an explicit ClientListen: the topology reconciler's loop
// and the grow cutover among them.
//
// Round 2's objection was not that the guard is wrong but that nothing exercised it,
// and by construction nothing could: every existing test threads own.ClientListen()
// into the Config, which is precisely the case the guard lets through.
func TestARenderThatDropsTheClientBindIsRefused(t *testing.T) {
	own, err := Preflight(writeConf(t, reconcileStandaloneConf))
	if err != nil {
		t.Fatalf("fixture must preflight clean: %v", err)
	}
	if own.ClientListen() == "" {
		t.Fatal("premise broken: the fixture conf declares no client bind, so this test cannot " +
			"tell a dropped bind from an absent one")
	}
	base := Config{
		Local: Broker{ServerName: "brk-a", NkeyPub: "UBUSNKEYA", RouteURL: "nats://10.0.0.1:6222"},
		Peers: []Broker{
			{ServerName: "brk-a", NkeyPub: "UBUSNKEYA", RouteURL: "nats://10.0.0.1:6222"},
			{ServerName: "brk-b", NkeyPub: "UBUSNKEYB", RouteURL: "nats://10.0.0.2:6222"},
		},
		AccountIssuer: "ABROKERACCOUNTPUB",
		JSStoreDir:    own.JSStoreDir(),
	}
	// The first-grow fallback, exactly as TestSecretsDirFallbackRendersThroughTheSharedHelper
	// drives it: without it the render is refused by the routes-mTLS harvest long before it
	// reaches the client-bind check this test is about.
	base.ApplySecretsDirIdentity("/etc/tether/secrets", "nats://10.0.0.1:6222")

	// ClientListen deliberately left empty — what an assembly path that forgot to
	// carry it forward produces.
	dropped := base
	if _, err := BuildMergedConf(own, dropped); err == nil {
		t.Fatal("a render carrying NO client listen was accepted over a conf that binds one.\n\n" +
			"nats-server defaults the missing bind to 0.0.0.0:4222, so a broker bound to a private " +
			"address is silently re-bound to every interface — and `nats-server -t` accepts the " +
			"file, so the DryRun gate cannot catch it either.")
	} else if !strings.Contains(err.Error(), "client listen") {
		t.Fatalf("refused, but for some other reason: %v", err)
	}

	// The POSITIVE control: carrying it forward must still render.
	kept := base
	kept.ClientListen = own.ClientListen()
	if _, err := BuildMergedConf(own, kept); err != nil {
		t.Fatalf("a render that DOES carry the client bind was refused: %v", err)
	}
}

// origin: prerelease audit round 2, G6.
//
// The other half of the predicate, and the reason it is not simply "ClientListen must
// be non-empty": a fixture that declares no client bind has none to preserve, and
// failing it would break every conf that only carries routes or auth_callout.
func TestARenderWithNoClientBindToPreserveIsStillAccepted(t *testing.T) {
	const noBindConf = `server_name: "brk-a"
jetstream {
  store_dir: "/var/lib/tether/js"
}
`
	own, err := Preflight(writeConf(t, noBindConf))
	if err != nil {
		t.Fatalf("fixture must preflight clean: %v", err)
	}
	if own.ClientListen() != "" {
		t.Fatalf("premise broken: this fixture was supposed to declare no client bind, got %q",
			own.ClientListen())
	}
	cfg := Config{
		Local: Broker{ServerName: "brk-a", NkeyPub: "UBUSNKEYA", RouteURL: "nats://10.0.0.1:6222"},
		Peers: []Broker{
			{ServerName: "brk-a", NkeyPub: "UBUSNKEYA", RouteURL: "nats://10.0.0.1:6222"},
			{ServerName: "brk-b", NkeyPub: "UBUSNKEYB", RouteURL: "nats://10.0.0.2:6222"},
		},
		AccountIssuer: "ABROKERACCOUNTPUB",
		JSStoreDir:    own.JSStoreDir(),
	}
	cfg.ApplySecretsDirIdentity("/etc/tether/secrets", "nats://10.0.0.1:6222")
	if _, err := BuildMergedConf(own, cfg); err != nil {
		t.Fatalf("a conf with NO client bind to preserve was refused for not preserving one: %v", err)
	}
}
