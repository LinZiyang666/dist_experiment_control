package upgradecli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/upgradelock"
	"github.com/LinZiyang666/tether/internal/upgradestore"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// allTestDomains is the frozen full domain list every sealed scope names.
func allTestDomains() []upgradetx.Domain {
	return []upgradetx.Domain{upgradetx.DomainAuthority, upgradetx.DomainBrokerRuntime, upgradetx.DomainBrokerStore,
		upgradetx.DomainControl, upgradetx.DomainExec, upgradetx.DomainFlow, upgradetx.DomainPTY, upgradetx.DomainSS,
		upgradetx.DomainTransferTracker, upgradetx.DomainTransferWorker, upgradetx.DomainUpgrade}
}

// bootFixture registers one agent component under the agent role's real
// control directory and returns its role, registered key and canonical entry.
func bootFixture(t *testing.T) (BootRole, upgradetx.Key, string) {
	t.Helper()
	root := t.TempDir()
	home := filepath.Join(root, "home")
	role := AgentBootRole(home, "lab", "n1", uint32(os.Geteuid()))
	if err := os.MkdirAll(role.ControlDir, 0o700); err != nil {
		t.Fatal(err)
	}
	canonical := filepath.Join(root, "bin", "tether")
	if err := os.MkdirAll(filepath.Dir(canonical), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(canonical, []byte("canonical tether bytes "+root), 0o755); err != nil {
		t.Fatal(err)
	}
	id, err := upgradelock.RegisterDomain(upgradelock.Registration{CanonicalPath: canonical, ControlDir: role.ControlDir, DeploymentID: "boot-" + filepath.Base(root),
		Participants: []upgradelock.Participant{{Key: role.Want, ServiceID: "agent"}}})
	if err != nil {
		t.Fatal(err)
	}
	key := role.Want
	key.DomainID = id
	return role, key, canonical
}

// An unregistered component (no control directory, or no participant naming
// it) runs exactly as before: no selection, no exec, no capability.
// origin: hot-update S8 plan §5.2 / §11.2 P19
func TestCanonicalBootLeavesUnregisteredComponentsUnchanged(t *testing.T) {
	missing := AgentBootRole(filepath.Join(t.TempDir(), "nowhere"), "lab", "n1", uint32(os.Geteuid()))
	if plan, err := PlanBoot(missing); err != nil || plan.Registered || plan.Exec {
		t.Fatalf("missing control directory changed boot: %+v %v", plan, err)
	}
	role, _, _ := bootFixture(t)
	other := AgentBootRole(filepath.Dir(filepath.Dir(filepath.Dir(role.ControlDir))), "lab", "another-node", uint32(os.Geteuid()))
	other.ControlDir = role.ControlDir
	if plan, err := PlanBoot(other); err != nil || plan.Registered {
		t.Fatalf("a registration for another node was adopted: %+v %v", plan, err)
	}
}

// A registered component without an accepted transaction runs its registered
// canonical bytes — even when its journal already holds an inert prepared
// grant, because a Prepare never moves the selector. The running image (this
// test binary) differs, so boot must exec the canonical entry exactly once.
// origin: hot-update S8 plan §5.2 / IL13 / DB09 / §11.2 P19
func TestCanonicalBootRunsCanonicalUntilATransactionIsAccepted(t *testing.T) {
	role, key, canonical := bootFixture(t)
	plan, err := PlanBoot(role)
	if err != nil || !plan.Registered || plan.Key != key || !plan.Exec || plan.Effective != canonical {
		t.Fatalf("registered cold boot did not select the canonical entry: %+v %v", plan, err)
	}
	owner := upgradetx.Owner{BootID: "boot", PID: 1, StartTicks: 1, InstanceID: strings.Repeat("a", 32)}
	if err := os.MkdirAll(filepath.Dir(plan.Journal), 0o700); err != nil {
		t.Fatal(err)
	}
	store, err := upgradestore.Open(upgradestore.Options{Path: plan.Journal, GuardPath: plan.Guard, Key: key, Owner: owner, Transition: upgradetx.Transition})
	if err != nil {
		t.Fatal(err)
	}
	scope := upgradetx.Scope{ID: strings.Repeat("d", 32), Participants: []upgradetx.Key{key}, Domains: allTestDomains()}
	if _, err := store.Prepare(key, upgradetx.PrepareRequest{ClientNonce: strings.Repeat("c", 32), ActorFP: "SHA256:actor", Mode: upgradetx.ModeHot,
		SourceRelease: "v1", TargetRelease: "v2", SourceSHA: strings.Repeat("1", 64), TargetSHA: strings.Repeat("2", 64),
		CapabilityDigest: strings.Repeat("3", 64), Scope: scope}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(plan.Journal); err != nil {
		t.Fatalf("prepare did not create the journal under test: %v", err)
	}
	again, err := PlanBoot(role)
	if err != nil || again.Effective != canonical || again.EffectiveSHA != plan.EffectiveSHA {
		t.Fatalf("an inert prepared grant moved the boot selector: %+v %v", again, err)
	}
	var execed string
	var env []string
	if _, err := SelectBootImage(role, func(path string, e []string) error { execed, env = path, e; return nil }); err != nil || execed != canonical {
		t.Fatalf("selection did not exec the canonical entry: %q %v", execed, err)
	}
	found := false
	for _, entry := range env {
		found = found || entry == BootDepthEnv+"="+plan.EffectiveSHA
	}
	if !found {
		t.Fatal("selected exec carries no depth bound")
	}
	// After one selection exec the image must already be the selected one; a
	// second mismatch refuses instead of looping between images.
	t.Setenv(BootDepthEnv, plan.EffectiveSHA)
	if _, err := SelectBootImage(role, func(string, []string) error { t.Fatal("selector exec'ed twice"); return nil }); ExitCode(err) != 74 {
		t.Fatalf("a second selection exec was not refused: %v", err)
	}
	if os.Getenv(BootDepthEnv) != "" {
		t.Fatal("the depth bound leaked into the daemon environment")
	}
}

// A component registered twice, or a malformed registration, names two
// journals or none: boot refuses rather than guessing one.
func TestCanonicalBootRefusesAmbiguousOrCorruptRegistration(t *testing.T) {
	role, key, _ := bootFixture(t)
	reg, err := os.ReadFile(filepath.Join(role.ControlDir, key.DomainID+".domain"))
	if err != nil {
		t.Fatal(err)
	}
	twin := strings.Repeat("f", 32)
	if err := os.WriteFile(filepath.Join(role.ControlDir, twin+".domain"), reg, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := PlanBoot(role); ExitCode(err) != 74 {
		t.Fatalf("a duplicated registration was accepted: %v", err)
	}
	if err := os.WriteFile(filepath.Join(role.ControlDir, twin+".domain"), []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := PlanBoot(role); ExitCode(err) != 74 {
		t.Fatalf("a malformed registration was ignored: %v", err)
	}
}

// The broker data directory is the store's directory; an in-memory or
// relative store has no stable registration.
func TestBrokerDataDirFollowsTheConfiguredStore(t *testing.T) {
	for _, tc := range []struct{ cluster, db, want string }{
		{"/var/lib/tether/cluster", "/var/lib/tether/tether.db", "/var/lib/tether/cluster"},
		{"", "/var/lib/tether/tether.db", "/var/lib/tether"},
		{"", "file:/srv/tether/state.db?_busy=5000", "/srv/tether"},
		{"", ":memory:", ""}, {"", "./tether.db", ""}, {"", "", ""},
	} {
		if got := BrokerDataDir(tc.cluster, tc.db); got != tc.want {
			t.Fatalf("BrokerDataDir(%q, %q) = %q, want %q", tc.cluster, tc.db, got, tc.want)
		}
	}
}
