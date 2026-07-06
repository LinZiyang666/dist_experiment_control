package natsconf

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestApplyPermDeniedInReadOnlyDir pins the G1 #22 error contract. The User=tether in-broker
// topology reconciler shares takeover.Apply, which writes a one-time pristine `.bak` and a sibling
// `os.CreateTemp` in the conf's DIRECTORY, then atomically renames — all of which need a WRITABLE
// parent dir. A pre-G1 root-owned /etc/tether therefore perm-denies with a "natsconf:" +
// "permission denied" message, the exact tokens drills/13-inbroker-reconcile-perm.sh greps. This is
// why G1 relocates the reconciler's conf into the tether-owned /etc/tether/nats.d/ subdir. Arm (b)
// is root-gated: root ignores directory permission bits, so it would false-pass under root CI.
func TestApplyPermDeniedInReadOnlyDir(t *testing.T) {
	t.Run("writable_dir_succeeds", func(t *testing.T) {
		dir := t.TempDir()
		conf := filepath.Join(dir, "nats.conf")
		if err := os.WriteFile(conf, []byte("port: 4222\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := Apply(conf, "port: 4223\n"); err != nil {
			t.Fatalf("Apply into a writable dir must succeed, got: %v", err)
		}
		got, _ := os.ReadFile(conf)
		if string(got) != "port: 4223\n" {
			t.Fatalf("Apply did not swap the conf; got %q", got)
		}
	})

	t.Run("readonly_dir_perm_denies", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("root ignores directory permission bits; the #22 perm-deny only manifests as non-root")
		}
		dir := t.TempDir()
		conf := filepath.Join(dir, "nats.conf")
		if err := os.WriteFile(conf, []byte("port: 4222\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		// 0555 = r-x: the conf is still stat/readable (Apply reads the source) but the DIRECTORY is
		// not writable, so the .bak / temp create fails — exactly the pre-G1 root-owned /etc/tether.
		if err := os.Chmod(dir, 0o555); err != nil {
			t.Fatal(err)
		}
		defer func() { _ = os.Chmod(dir, 0o755) }() // restore so t.TempDir cleanup can remove it

		err := Apply(conf, "port: 4223\n")
		if err == nil {
			t.Fatal("Apply into a read-only dir must perm-deny (the #22 in-broker reconciler failure)")
		}
		msg := err.Error()
		if !strings.Contains(msg, "natsconf:") || !strings.Contains(msg, "permission denied") {
			t.Fatalf("error must carry the drill-13 signature tokens (natsconf: … permission denied); got: %v", err)
		}
	})
}
