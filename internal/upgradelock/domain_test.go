// These tests use real temporary files and deployment declarations. No runtime
// is stubbed; service discovery and the CLI/installer adapter are outside this
// leaf's coverage. All paths live under testing's private temporary directory.
package upgradelock

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

func domainFixture(t *testing.T) (Registration, upgradetx.Key) {
	t.Helper()
	base := t.TempDir()
	canonical, err := filepath.EvalSymlinks(base)
	if err != nil {
		t.Fatal(err)
	}
	bin := filepath.Join(canonical, "bin")
	control := filepath.Join(canonical, "control")
	for _, dir := range []string{bin, control} {
		if err := os.Mkdir(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	path := filepath.Join(bin, "candidate")
	if err := os.WriteFile(path, []byte("source"), 0o700); err != nil {
		t.Fatal(err)
	}
	key := upgradetx.Key{Component: upgradetx.ComponentAgent, SID: "session", ConfiguredNID: "node", UID: uint32(os.Getuid())}
	reg := Registration{CanonicalPath: path, ControlDir: control, DeploymentID: "deployment", Participants: []Participant{{Key: key, ServiceID: "agent-unit"}}}
	return reg, key
}

func registerFixture(t *testing.T, reg Registration, key upgradetx.Key) upgradetx.Key {
	t.Helper()
	id, err := RegisterDomain(reg)
	if err != nil {
		t.Fatal(err)
	}
	key.DomainID = id
	t.Cleanup(func() {
		domains.Lock()
		delete(domains.byID, id)
		domains.Unlock()
	})
	return key
}

func requireCode(t *testing.T, err error, code string) {
	t.Helper()
	var refusal *upgradetx.Error
	if !errors.As(err, &refusal) || refusal.Code != code || refusal.Accepted {
		t.Fatalf("want unaccepted %s, got %v", code, err)
	}
}

// origin: hot-update S8 plan §5.1 / P04
func TestDomainIdentitySurvivesBinaryReplacement(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseDomainReplacement(t)
	}
	testharness.AssertNoGoroutineLeak(t, "canonical domain replacement", g)
	testharness.AssertNoFDLeak(t, "canonical domain replacement", fd, 0)
}

func exerciseDomainReplacement(t *testing.T) {
	t.Helper()
	reg, key := domainFixture(t)
	key = registerFixture(t, reg, key)
	owner, err := Acquire(t.Context(), key, "")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = owner.Close() }()
	replacement := reg.CanonicalPath + ".replacement"
	if err := os.WriteFile(replacement, []byte("target"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(replacement, reg.CanonicalPath); err != nil {
		t.Fatal(err)
	}
	id, err := RegisterDomain(reg)
	if err != nil || id != key.DomainID {
		t.Fatalf("binary rename split the domain: %s, %v", id, err)
	}
	_, err = Acquire(t.Context(), key, "")
	requireCode(t, err, upgradetx.CodeDeferred)
	changed := reg
	changed.DeploymentID = "another-deployment"
	d, err := normalizeDomain(changed)
	if err != nil || d.id == key.DomainID {
		t.Fatalf("deployment identity omitted: %v", err)
	}
	other, _ := domainFixture(t)
	other.DeploymentID = reg.DeploymentID
	d, err = normalizeDomain(other)
	if err != nil || d.id == key.DomainID {
		t.Fatalf("installation parent omitted: %v", err)
	}
	bin := filepath.Dir(reg.CanonicalPath)
	if err := os.Rename(bin, bin+".old"); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(bin, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(reg.CanonicalPath, []byte("target"), 0o700); err != nil {
		t.Fatal(err)
	}
	d, err = normalizeDomain(reg)
	if err != nil || d.id == key.DomainID {
		t.Fatalf("recreated installation parent reused the old domain: %v", err)
	}
	_, err = Acquire(t.Context(), key, "")
	requireCode(t, err, upgradetx.CodeRejected)
}

// origin: hot-update S8 plan §5.1 / P04
func TestDomainAliasesRequireCompleteRegistration(t *testing.T) {
	reg, key := domainFixture(t)
	symlink := reg.CanonicalPath + ".symlink"
	hardlink := reg.CanonicalPath + ".hardlink"
	if err := os.Symlink(reg.CanonicalPath, symlink); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(reg.CanonicalPath, hardlink); err != nil {
		t.Fatal(err)
	}
	_, err := RegisterDomain(reg)
	requireCode(t, err, upgradetx.CodeRejected)
	reg.Aliases = []string{hardlink, symlink}
	key = registerFixture(t, reg, key)
	aliasReg := reg
	aliasReg.CanonicalPath = symlink
	id, err := RegisterDomain(aliasReg)
	if err != nil || id != key.DomainID {
		t.Fatalf("declared symlink split the domain: %v", err)
	}
	clone := reg.CanonicalPath + ".clone"
	if err := os.WriteFile(clone, []byte("source"), 0o700); err != nil {
		t.Fatal(err)
	}
	reg.Aliases = append(reg.Aliases, clone)
	_, err = normalizeDomain(reg)
	requireCode(t, err, upgradetx.CodeRejected)
	_, err = RegisterDomain(reg)
	requireCode(t, err, upgradetx.CodeRejected)
	// A symlink to the clone does not alter the canonical hard-link count.
	// This isolates the same-file check from the independent link-count guard.
	cloneAlias := clone + ".symlink"
	if err := os.Symlink(clone, cloneAlias); err != nil {
		t.Fatal(err)
	}
	reg.Aliases[len(reg.Aliases)-1] = cloneAlias
	_, err = normalizeDomain(reg)
	requireCode(t, err, upgradetx.CodeRejected)
	unknown := reg.CanonicalPath + ".unknown"
	if err := os.Link(reg.CanonicalPath, unknown); err != nil {
		t.Fatal(err)
	}
	_, err = Acquire(t.Context(), key, "")
	requireCode(t, err, upgradetx.CodeRejected)
}

// origin: hot-update S8 plan §3.1 / §5.1 / P04
func TestDomainRejectsUnknownSiblingAndChangedRegistry(t *testing.T) {
	reg, key := domainFixture(t)
	_, err := Acquire(t.Context(), key, "")
	requireCode(t, err, upgradetx.CodeRejected)
	key = registerFixture(t, reg, key)
	for _, change := range []func(*upgradetx.Key){
		func(k *upgradetx.Key) { k.SID = "unknown-session" },
		func(k *upgradetx.Key) { k.ConfiguredNID = "unknown-node" },
		func(k *upgradetx.Key) { k.UID++ },
		func(k *upgradetx.Key) {
			k.Component = upgradetx.ComponentBroker
			k.SID = ""
			k.ConfigPath = "/config.yaml"
		},
	} {
		unknown := key
		change(&unknown)
		_, err = Acquire(t.Context(), unknown, "")
		requireCode(t, err, upgradetx.CodeRejected)
	}
	path := filepath.Join(reg.ControlDir, key.DomainID+".domain")
	if err := os.WriteFile(path, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err = Acquire(t.Context(), key, "")
	requireCode(t, err, upgradetx.CodeStateCorrupt)
	_, err = RegisterDomain(reg)
	requireCode(t, err, upgradetx.CodeStateCorrupt)
}

// origin: hot-update S8 plan §5.1 / P04
func TestDomainRejectsDirectoryReplacementAndInvalidKeys(t *testing.T) {
	reg, key := domainFixture(t)
	key = registerFixture(t, reg, key)
	if err := os.Rename(reg.ControlDir, reg.ControlDir+".old"); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(reg.ControlDir, 0o700); err != nil {
		t.Fatal(err)
	}
	_, err := Acquire(t.Context(), key, "")
	requireCode(t, err, upgradetx.CodeRejected)
	for _, k := range []upgradetx.Key{
		{}, {DomainID: key.DomainID, Component: upgradetx.ComponentAgent},
		{DomainID: key.DomainID, Component: upgradetx.ComponentBroker, ConfigPath: "relative"},
		{DomainID: key.DomainID, Component: "unknown"},
	} {
		requireCode(t, validateKey(k), upgradetx.CodeRejected)
	}
	for _, component := range []upgradetx.Component{upgradetx.ComponentNATS, upgradetx.ComponentCaddy} {
		requireCode(t, validateKey(upgradetx.Key{DomainID: key.DomainID, Component: component}), upgradetx.CodeRejected)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = Acquire(ctx, key, "")
	requireCode(t, err, upgradetx.CodeDeferred)
}

// origin: hot-update S8 plan §3.1 / §5.1 / P04
func TestDomainRejectsOverlappingDeploymentAfterColdRegistration(t *testing.T) {
	reg, key := domainFixture(t)
	key = registerFixture(t, reg, key)
	changed := reg
	changed.DeploymentID = "different-deployment"
	_, err := RegisterDomain(changed)
	requireCode(t, err, upgradetx.CodeRejected)
	// Cold startup has only the declared control directory, not live map state.
	domains.Lock()
	delete(domains.byID, key.DomainID)
	domains.Unlock()
	_, err = RegisterDomain(changed)
	requireCode(t, err, upgradetx.CodeRejected)
	id, err := RegisterDomain(reg)
	if err != nil || id != key.DomainID {
		t.Fatalf("cold registration lost the original domain: %v", err)
	}
}

// origin: hot-update S8 plan §2.1 / §5.1 / P04
func TestDomainRejectsUnboundedRegistration(t *testing.T) {
	reg, _ := domainFixture(t)
	for _, change := range []func(*Registration){
		func(r *Registration) { r.DeploymentID = "" },
		func(r *Registration) { r.Participants = nil },
		func(r *Registration) { r.Aliases = make([]string, 1025) },
		func(r *Registration) { r.Participants = make([]Participant, 1025) },
		func(r *Registration) {
			p := r.Participants[0]
			p.ServiceID = "another-service"
			r.Participants = append(r.Participants, p)
		},
		func(r *Registration) {
			p := r.Participants[0]
			p.Key.ConfiguredNID = "another-node"
			r.Participants = append(r.Participants, p)
		},
	} {
		candidate := reg
		candidate.Participants = append([]Participant(nil), reg.Participants...)
		change(&candidate)
		_, err := normalizeDomain(candidate)
		requireCode(t, err, upgradetx.CodeRejected)
	}
	// Each broker identity is individually bounded; the combined declaration
	// must also fit the same ceiling used when enumerating sibling registries.
	large := reg
	large.Participants = nil
	for i := 0; i < 1024; i++ {
		large.Participants = append(large.Participants, Participant{
			Key:       upgradetx.Key{Component: upgradetx.ComponentBroker, ConfigPath: "/" + strings.Repeat("a", 1500) + "/" + strconv.Itoa(i)},
			ServiceID: strconv.Itoa(i),
		})
	}
	_, err := normalizeDomain(large)
	requireCode(t, err, upgradetx.CodeRejected)
	if err := os.Chmod(reg.ControlDir, 0o777); err != nil {
		t.Fatal(err)
	}
	_, err = normalizeDomain(reg)
	requireCode(t, err, upgradetx.CodeRejected)
}

// origin: hot-update S8 plan §2.1 / §5.1; S9 plan §8 / T44
func TestDomainRegistersInfrastructureParticipants(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseInfrastructureDomain(t)
	}
	testharness.AssertNoGoroutineLeak(t, "infrastructure domain registration", g)
	testharness.AssertNoFDLeak(t, "infrastructure domain registration", fd, 0)
}

func exerciseInfrastructureDomain(t *testing.T) {
	t.Helper()
	reg, agent := domainFixture(t)
	nats := upgradetx.Key{Component: upgradetx.ComponentNATS, ConfiguredNID: "node", ConfigPath: filepath.Join(reg.ControlDir, "nats.conf"), UID: agent.UID}
	caddy := nats
	caddy.Component, caddy.ConfigPath = upgradetx.ComponentCaddy, filepath.Join(reg.ControlDir, "caddy.conf")
	reg.Participants = append(reg.Participants, Participant{Key: nats, ServiceID: "nats-unit"}, Participant{Key: caddy, ServiceID: "caddy-unit"})
	agent = registerFixture(t, reg, agent)
	nats.DomainID, caddy.DomainID = agent.DomainID, agent.DomainID
	for _, key := range []upgradetx.Key{nats, caddy} {
		owner, err := Acquire(t.Context(), key, "host-parent")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = owner.Close() }()
		for _, sibling := range []upgradetx.Key{agent, nats, caddy} {
			_, err = Acquire(t.Context(), sibling, "")
			requireCode(t, err, upgradetx.CodeDeferred)
		}
		// A registered child borrows the host OFD, while a substituted role,
		// UID, config or unknown component cannot borrow the registered key.
		borrowed, err := Acquire(t.Context(), agent, "host-parent")
		if err != nil {
			t.Fatal(err)
		}
		if err := borrowed.Close(); err != nil {
			t.Fatal(err)
		}
		for _, change := range []func(*upgradetx.Key){
			func(k *upgradetx.Key) { k.Component = "unknown" },
			func(k *upgradetx.Key) { k.Component = upgradetx.ComponentBroker },
			func(k *upgradetx.Key) {
				if k.Component == upgradetx.ComponentNATS {
					k.Component = upgradetx.ComponentCaddy
				} else {
					k.Component = upgradetx.ComponentNATS
				}
			},
			func(k *upgradetx.Key) { k.UID++ },
			func(k *upgradetx.Key) { k.ConfiguredNID = "another-node" },
		} {
			foreign := key
			change(&foreign)
			_, err = Acquire(t.Context(), foreign, "host-parent")
			requireCode(t, err, upgradetx.CodeRejected)
		}
		if err := WithGuard(key, func() error { return nil }); err != nil {
			t.Fatal(err)
		}
		_, err = Acquire(t.Context(), caddy, "")
		requireCode(t, err, upgradetx.CodeDeferred)
		if err := owner.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

// origin: hot-update S8 plan §2.1 / §5.1; S9 plan §8 / T44
func TestDomainRejectsMalformedInfrastructureKeys(t *testing.T) {
	for _, component := range []upgradetx.Component{upgradetx.ComponentNATS, upgradetx.ComponentCaddy} {
		for _, defect := range []string{"unknown", "sid", "node", "long_node", "relative", "unclean", "long_path", "path_control"} {
			t.Run(string(component)+"/"+defect, func(t *testing.T) {
				key := upgradetx.Key{DomainID: strings.Repeat("a", 32), Component: component, ConfiguredNID: "node", ConfigPath: "/managed/config"}
				switch defect {
				case "unknown":
					key.Component = "unknown"
				case "sid":
					key.SID = "agent-session"
				case "node":
					key.ConfiguredNID = ""
				case "long_node":
					key.ConfiguredNID = strings.Repeat("n", 257)
				case "relative":
					key.ConfigPath = "config"
				case "unclean":
					key.ConfigPath = "/managed/../config"
				case "long_path":
					key.ConfigPath = "/" + strings.Repeat("p", 4096)
				case "path_control":
					key.ConfigPath = "/managed/config\n"
				}
				requireCode(t, validateKey(key), upgradetx.CodeRejected)
			})
		}
	}
}
