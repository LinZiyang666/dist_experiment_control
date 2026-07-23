package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

// TestResetForceSingleJSStore_MovesAndDemandsRestart pins R16 A3 + the M5 fix: the offline force-single JS
// reset moves a data-bearing clustered store aside (never deletes) AND the operator-facing note demands the
// mandatory FULL nats-server restart (following the printed steps without it lands in a 503).
func TestResetForceSingleJSStore_MovesAndDemandsRestart(t *testing.T) {
	dataDir := t.TempDir()
	store := filepath.Join(t.TempDir(), "jetstream")
	if err := os.MkdirAll(filepath.Join(store, "$G", "streams", "history-x"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(store, "$G", "streams", "history-x", "1.blk"), []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}

	cmd := &cobra.Command{}
	var stderr bytes.Buffer
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&stderr)

	// Without --reset-js a NON-EMPTY store must refuse (no silent data loss) BEFORE the journal is cleared.
	if err := resetForceSingleJSStore(cmd, store, dataDir, "epoch1", false); err == nil {
		t.Fatal("a non-empty clustered JS store must refuse without --reset-js")
	}
	if _, e := os.Stat(filepath.Join(store, "$G", "streams", "history-x", "1.blk")); e != nil {
		t.Fatal("a refused reset must not touch the data-bearing store")
	}

	// With --reset-js it moves aside AND demands the FULL nats restart (the M5 fix).
	if err := resetForceSingleJSStore(cmd, store, dataDir, "epoch1", true); err != nil {
		t.Fatalf("with --reset-js the store must move: %v", err)
	}
	out := stderr.String()
	if !strings.Contains(out, "systemctl restart nats-server") {
		t.Fatalf("the JS-reset note MUST demand a FULL `systemctl restart nats-server` (else the operator boots the broker into a 503), got:\n%s", out)
	}
	if !strings.Contains(out, "moved aside") {
		t.Fatalf("the note must say the store was MOVED aside (never deleted), got:\n%s", out)
	}
	if _, e := os.Stat(store + ".force-single-bak.epoch1"); e != nil {
		t.Fatalf("the clustered store must be moved to the per-incident backup: %v", e)
	}
}
