package main

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func writeAgentYAML(t *testing.T, home, body string) {
	t.Helper()
	dir := filepath.Join(home, "agent", "lab")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "agent.yaml"), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}

// TestAgentYAML_remoteFS pins review m4: the remote_fs block round-trips, an
// absent block yields the zero value (⇒ mode auto downstream), and a typo'd key
// (nested or top-level) is rejected loud by KnownFields rather than silently
// dropped.
func TestAgentYAML_remoteFS(t *testing.T) {
	t.Run("absent ⇒ zero", func(t *testing.T) {
		home := t.TempDir()
		writeAgentYAML(t, home, "broker_url: wss://b:443\nsession: lab\nnid: lab-1\n")
		ay, err := loadAgentYAML(home, "lab")
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(ay.RemoteFS, remoteFSConfig{}) {
			t.Errorf("absent remote_fs must be zero, got %+v", ay.RemoteFS)
		}
	})

	t.Run("full block round-trips", func(t *testing.T) {
		home := t.TempDir()
		writeAgentYAML(t, home, "nid: lab-1\nremote_fs:\n  mode: off\n  safe_dir: /var/tmp\n"+
			"  probe_timeout: 2s\n  spawn_timeout: 30s\n  wedge_ceiling: 32\n")
		ay, err := loadAgentYAML(home, "lab")
		if err != nil {
			t.Fatal(err)
		}
		want := remoteFSConfig{Mode: "off", SafeDir: "/var/tmp", ProbeTimeout: "2s", SpawnTimeout: "30s", WedgeCeiling: 32}
		if ay.RemoteFS != want {
			t.Errorf("round-trip: got %+v want %+v", ay.RemoteFS, want)
		}
	})

	t.Run("nested typo rejected", func(t *testing.T) {
		home := t.TempDir()
		writeAgentYAML(t, home, "nid: lab-1\nremote_fs:\n  moed: auto\n")
		if _, err := loadAgentYAML(home, "lab"); err == nil {
			t.Error("a typo'd nested key must be rejected by KnownFields")
		}
	})

	t.Run("top-level typo rejected", func(t *testing.T) {
		home := t.TempDir()
		writeAgentYAML(t, home, "nid: lab-1\nremotefs:\n  mode: auto\n")
		if _, err := loadAgentYAML(home, "lab"); err == nil {
			t.Error("a misspelled top-level remotefs: must be rejected by KnownFields")
		}
	})
}

// TestParseOptDuration pins review m4: empty ⇒ 0, valid durations parse, and a
// malformed or negative value fails loud with the field name in the error.
func TestParseOptDuration(t *testing.T) {
	cases := []struct {
		in      string
		want    time.Duration
		wantErr bool
	}{
		{"", 0, false},
		{"2s", 2 * time.Second, false},
		{"30s", 30 * time.Second, false},
		{"-5s", 0, true}, // negative
		{"2sx", 0, true}, // malformed
		{"30", 0, true},  // bare int — needs a unit
		{"abc", 0, true}, // garbage
	}
	for _, c := range cases {
		got, err := parseOptDuration(c.in, "remote_fs.probe_timeout")
		if (err != nil) != c.wantErr {
			t.Errorf("%q: err=%v wantErr=%v", c.in, err, c.wantErr)
			continue
		}
		if err != nil {
			if !strings.Contains(err.Error(), "remote_fs.probe_timeout") {
				t.Errorf("%q: error must name the field: %v", c.in, err)
			}
			continue
		}
		if got != c.want {
			t.Errorf("%q: got %v want %v", c.in, got, c.want)
		}
	}
}

// TestExecSafeFlagOrdering pins review m6: --safe is parsed only BEFORE the node
// positional (SetInterspersed(false)). A trailing --safe is captured as a remote
// argv token, NOT parsed as our flag — the documented footgun.
func TestExecSafeFlagOrdering(t *testing.T) {
	cases := []struct {
		args     []string
		wantSafe bool
	}{
		{[]string{"--safe", "node", "--", "whoami"}, true},
		{[]string{"node", "--safe", "whoami"}, false},
		{[]string{"node", "--", "--safe", "x"}, false},
	}
	for _, c := range cases {
		cmd := newExecCmd()
		if err := cmd.Flags().Parse(c.args); err != nil {
			t.Fatalf("%v: parse: %v", c.args, err)
		}
		safe, _ := cmd.Flags().GetBool("safe")
		if safe != c.wantSafe {
			t.Errorf("%v: safe=%v want %v", c.args, safe, c.wantSafe)
		}
		// First positional is always the node, never consumed by --safe parsing.
		if got := cmd.Flags().Args(); len(got) == 0 || got[0] != "node" {
			t.Errorf("%v: first positional must be the node, got %v", c.args, got)
		}
	}
}

// TestRunSafeFlagOrdering: same contract for `tether run`.
func TestRunSafeFlagOrdering(t *testing.T) {
	cmd := newRunCmd()
	if err := cmd.Flags().Parse([]string{"--safe", "node", "--", "bash"}); err != nil {
		t.Fatal(err)
	}
	if safe, _ := cmd.Flags().GetBool("safe"); !safe {
		t.Error("leading --safe must parse to safe=true for run")
	}
	cmd2 := newRunCmd()
	_ = cmd2.Flags().Parse([]string{"node", "--safe", "bash"})
	if safe, _ := cmd2.Flags().GetBool("safe"); safe {
		t.Error("trailing --safe must NOT parse as our flag for run")
	}
}
