package main

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"
)

// agent_upgrade_allow_test.go — #28. The agent has always re-checked an upgrade URL against its OWN
// allowlist (internal/agent/upgrade.go), but that allowlist was a hardcoded constant: `--upgrade-url-allow`
// existed only on `tether serve`. Both cmd/tether/error_hints.go and docs/usage.md nevertheless told
// operators to "check the agent's --upgrade-url-allow flag". An operator self-hosting release artifacts
// could open the BROKER's allowlist and still be refused url_not_allowed_local by every agent in the
// fleet, with no configuration to change and a hint pointing at a flag that did not exist.
//
// Fixed by giving the agent the flag for real (plus the agent.yaml `upgrade.url_allow` block, since
// install.sh-managed agents are started by a systemd unit whose ExecStart an operator does not normally
// edit), on the same flag > yaml > built-in-default precedence `tether serve` already uses.

// agentCmdForTest returns the REAL `tether agent` command out of the real root tree, with args parsed.
// Using the real tree is the point: a test that registers its own flag would pass even if the product
// never declared one.
func agentCmdForTest(t *testing.T, args ...string) *cobra.Command {
	t.Helper()
	root := newRootCmd()
	agent, _, err := root.Find([]string{"agent"})
	if err != nil || agent == nil || agent.Name() != "agent" {
		t.Fatalf("`tether agent` not found in the command tree: %v", err)
	}
	if err := agent.Flags().Parse(args); err != nil {
		t.Fatalf("parse %v: %v", args, err)
	}
	return agent
}

func TestAgentUpgradeAllowPrecedence(t *testing.T) {
	yaml := []string{"https://mirror.internal.example/tether/"}

	t.Run("flag wins over yaml", func(t *testing.T) {
		cmd := agentCmdForTest(t, "--upgrade-url-allow", "https://flag.example/rel/")
		got, err := resolveAgentUpgradeAllow(cmd, mustSliceFlag(t, cmd), yaml)
		if err != nil {
			t.Fatal(err)
		}
		if want := []string{"https://flag.example/rel/"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("got %v, want %v", got, want)
		}
	})

	t.Run("yaml is used when the flag is absent", func(t *testing.T) {
		cmd := agentCmdForTest(t)
		got, err := resolveAgentUpgradeAllow(cmd, mustSliceFlag(t, cmd), yaml)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, yaml) {
			t.Fatalf("got %v, want the yaml value %v — an agent started from a systemd unit can only be "+
				"configured through agent.yaml, so the yaml path is the one operators will actually use", got, yaml)
		}
	})

	t.Run("neither set leaves the agent on its built-in default", func(t *testing.T) {
		cmd := agentCmdForTest(t)
		got, err := resolveAgentUpgradeAllow(cmd, mustSliceFlag(t, cmd), nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 0 {
			t.Fatalf("got %v, want empty — internal/agent maps empty to defaultAgentURLAllowlist, which is how "+
				"every pre-#28 install keeps its exact behaviour", got)
		}
	})

	t.Run("multiple prefixes survive as a list", func(t *testing.T) {
		cmd := agentCmdForTest(t, "--upgrade-url-allow", "https://a.example/,https://b.example/")
		got, err := resolveAgentUpgradeAllow(cmd, mustSliceFlag(t, cmd), nil)
		if err != nil {
			t.Fatal(err)
		}
		if want := []string{"https://a.example/", "https://b.example/"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("got %v, want %v", got, want)
		}
	})

	t.Run("a non-URL entry is refused at startup, not silently ignored", func(t *testing.T) {
		for _, bad := range []string{"mirror.example.com/tether/", "ftp://mirror.example/", "githib.com/x/releases/"} {
			cmd := agentCmdForTest(t, "--upgrade-url-allow", bad)
			if _, err := resolveAgentUpgradeAllow(cmd, mustSliceFlag(t, cmd), nil); err == nil {
				t.Errorf("%q was accepted — the allowlist is a strings.HasPrefix match, so this entry can never "+
					"match a download URL; accepting it turns a typo into 'upgrades mysteriously refused'", bad)
			}
		}
	})

	t.Run("a blank yaml entry is refused too", func(t *testing.T) {
		// pflag collapses `--upgrade-url-allow ""` to an empty LIST (= "use the default"), but YAML can
		// really carry a blank item, and urlAllowed silently skips those. A blank line in the operator's
		// list must not be a silent no-op.
		cmd := agentCmdForTest(t)
		if _, err := resolveAgentUpgradeAllow(cmd, mustSliceFlag(t, cmd), []string{"https://ok.example/", ""}); err == nil {
			t.Error("a blank upgrade.url_allow entry was accepted; urlAllowed skips \"\" entries, so it is dead config")
		}
	})
}

// mustSliceFlag reads the parsed --upgrade-url-allow value off the REAL command. It fails if the product
// never declared the flag, which is the whole point of #28.
func mustSliceFlag(t *testing.T, cmd *cobra.Command) []string {
	t.Helper()
	v, err := cmd.Flags().GetStringSlice("upgrade-url-allow")
	if err != nil {
		t.Fatalf("`tether agent` has no --upgrade-url-allow flag (%v) — this is #28: error_hints.go and "+
			"docs/usage.md both tell operators to use it", err)
	}
	return v
}

// TestAgentUpgradeAllowIsDeclaredOnTheAgentCommand is the blunt existence + shape check, independent of
// the resolver: the capability must be a NORMAL, visible, documented CLI flag — not an env var, a build
// tag, or a hidden path.
func TestAgentUpgradeAllowIsDeclaredOnTheAgentCommand(t *testing.T) {
	cmd := agentCmdForTest(t)
	f := cmd.Flags().Lookup("upgrade-url-allow")
	if f == nil {
		t.Fatal("`tether agent` does not declare --upgrade-url-allow")
	}
	if f.Hidden {
		t.Fatal("the flag is Hidden — an operator capability that does not appear in `tether agent --help` is a back door, not a feature")
	}
	if f.Value.Type() != "stringSlice" {
		t.Errorf("flag type = %q, want stringSlice (an allowlist is a list, and it must match `serve`'s shape)", f.Value.Type())
	}
	if !strings.Contains(f.Usage, "agent.yaml") {
		t.Errorf("the flag help does not mention agent.yaml; a systemd-managed agent is configured through the "+
			"yaml, so the help must say so. got: %q", f.Usage)
	}
}

// TestAgentDaemonReachesTheAllowlistResolver closes the HALF-WIRING hazard this repo has now hit twice
// (R5's _b_migrate_forensics, R7b's unrenewed lease): every piece can exist and pass its own unit test
// while the daemon never calls any of it. It runs the REAL `tether agent` RunE with a deliberately
// malformed allowlist and asserts the daemon refuses to start — which can only happen if RunE actually
// resolved the flag.
//
// Mutation: bypass agentDaemonConfig in RunE and this test reports the bypass within its bound instead of
// silently blocking on a broker that will never answer.
func TestAgentDaemonReachesTheAllowlistResolver(t *testing.T) {
	home := t.TempDir()
	t.Setenv("TETHER_HOME", home)

	root := newRootCmd()
	root.SetOut(&bytes.Buffer{})
	root.SetErr(&bytes.Buffer{})
	// --nid keeps us past the earlier required-arg gates; the allowlist check must fire BEFORE any
	// identity/NATS work, so this must never touch the network.
	root.SetArgs([]string{"agent", "--session", "s-28", "--nid", "n1",
		"--upgrade-url-allow", "not-a-url-prefix"})

	// BOUNDED on purpose. If RunE stops consulting the allowlist, the daemon sails past this point and
	// blocks forever trying to reach a broker; an unbounded Execute() would then hang the whole package
	// instead of reporting the regression. A wiring test that can only fail by timing out is a bad
	// wiring test.
	done := make(chan error, 1)
	go func() { done <- root.Execute() }()
	var err error
	select {
	case err = <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("the agent daemon did not reject a malformed upgrade allowlist and is now blocked on startup " +
			"(NATS/identity) — RunE never consulted the flag, so --upgrade-url-allow is decorative: it parses " +
			"and is then thrown away (half-wiring)")
	}
	if err == nil {
		t.Fatal("the agent daemon STARTED with a malformed upgrade allowlist — RunE never consulted the flag, " +
			"so --upgrade-url-allow is decorative: it parses and is then thrown away (half-wiring)")
	}
	if !strings.Contains(err.Error(), "upgrade URL allowlist entry") {
		t.Fatalf("the daemon failed for an unrelated reason (%v) — this test proves nothing unless the failure "+
			"is the allowlist validation", err)
	}
}

// TestResolvedAllowlistReachesTheAgentConfig closes the LAST link of the chain. The two tests above
// prove (1) the flag parses and the resolver is right, and (2) RunE calls the resolver — but neither
// would notice if the resolved value were then dropped on the floor instead of assigned to
// agent.Config.UpgradeURLAllowlist, which is the field internal/agent actually consults.
//
// Mutation: delete `UpgradeURLAllowlist: upgradeAllow` from agentDaemonConfig → this test fails while
// every other test in this file still passes. That asymmetry is the reason it exists.
func TestResolvedAllowlistReachesTheAgentConfig(t *testing.T) {
	t.Run("from the flag", func(t *testing.T) {
		cmd := agentCmdForTest(t, "--upgrade-url-allow", "https://mirror.example/tether/")
		cfg, err := agentDaemonConfig(cmd, agentDaemonInputs{
			Home: t.TempDir(), SID: "s-28", NID: "n1",
			UpgradeURLAllowFlag: mustSliceFlag(t, cmd),
		})
		if err != nil {
			t.Fatal(err)
		}
		if want := []string{"https://mirror.example/tether/"}; !reflect.DeepEqual(cfg.UpgradeURLAllowlist, want) {
			t.Fatalf("agent.Config.UpgradeURLAllowlist = %v, want %v — the flag is parsed but never reaches the "+
				"daemon, so the capability does not exist at runtime", cfg.UpgradeURLAllowlist, want)
		}
	})

	t.Run("from agent.yaml", func(t *testing.T) {
		cmd := agentCmdForTest(t)
		cfg, err := agentDaemonConfig(cmd, agentDaemonInputs{
			Home: t.TempDir(), SID: "s-28", NID: "n1",
			YAML:                agentYAML{Upgrade: agentUpgradeConfig{URLAllow: []string{"https://yamlhost.example/r/"}}},
			UpgradeURLAllowFlag: mustSliceFlag(t, cmd),
		})
		if err != nil {
			t.Fatal(err)
		}
		if want := []string{"https://yamlhost.example/r/"}; !reflect.DeepEqual(cfg.UpgradeURLAllowlist, want) {
			t.Fatalf("agent.Config.UpgradeURLAllowlist = %v, want %v", cfg.UpgradeURLAllowlist, want)
		}
	})

	t.Run("unset stays empty so the agent keeps its built-in default", func(t *testing.T) {
		cmd := agentCmdForTest(t)
		cfg, err := agentDaemonConfig(cmd, agentDaemonInputs{
			Home: t.TempDir(), SID: "s-28", NID: "n1", UpgradeURLAllowFlag: mustSliceFlag(t, cmd),
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(cfg.UpgradeURLAllowlist) != 0 {
			t.Fatalf("an unconfigured agent got %v — #28 must not change the behaviour of any existing install",
				cfg.UpgradeURLAllowlist)
		}
	})
}

// TestAgentYAMLCarriesTheUpgradeBlock proves the yaml key is really parsed by the real loader. agentYAML
// is decoded with KnownFields(true), so an unknown key is a hard error — meaning a documented
// `upgrade.url_allow` that the struct does not declare would make agent.yaml unloadable entirely.
func TestAgentYAMLCarriesTheUpgradeBlock(t *testing.T) {
	home := t.TempDir()
	dir := filepath.Join(home, "agent", "s-28")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	body := "broker_url: nats://127.0.0.1:4222\nsession: s-28\nnid: n1\ntunnel_addr: 127.0.0.1:7000\n" +
		"upgrade:\n  url_allow:\n    - https://mirror.internal.example/tether/\n    - https://backup.example/rel/\n"
	if err := os.WriteFile(filepath.Join(dir, "agent.yaml"), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	ay, err := loadAgentYAML(home, "s-28")
	if err != nil {
		t.Fatalf("agent.yaml with an `upgrade:` block failed to load: %v", err)
	}
	want := []string{"https://mirror.internal.example/tether/", "https://backup.example/rel/"}
	if !reflect.DeepEqual(ay.Upgrade.URLAllow, want) {
		t.Fatalf("upgrade.url_allow = %v, want %v", ay.Upgrade.URLAllow, want)
	}
}

// TestUpgradeHintPointsAtSomethingThatExists guards the ORIGINAL #28 report: the hint named a flag that
// did not exist. Now that it does, keep the hint honest about both surfaces.
func TestUpgradeHintPointsAtSomethingThatExists(t *testing.T) {
	hint, ok := brokerCodeHints["url_not_allowed_local"]
	if !ok {
		t.Fatal("no hint for url_not_allowed_local")
	}
	if !strings.Contains(hint, "--upgrade-url-allow") {
		t.Error("the hint no longer names the flag")
	}
	if !strings.Contains(hint, "upgrade.url_allow") {
		t.Error("the hint should also name the agent.yaml key — a systemd-managed agent cannot easily take a new flag")
	}
	// And the flag it names must be real.
	if agentCmdForTest(t).Flags().Lookup("upgrade-url-allow") == nil {
		t.Fatal("the hint names an agent flag that does not exist — this is #28 verbatim")
	}
}
