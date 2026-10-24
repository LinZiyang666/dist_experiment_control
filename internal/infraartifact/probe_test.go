package infraartifact

import (
	"archive/tar"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// The generated ELF fixtures are never executed. This supervised shell models
// the version adapter's report contract; stock two-version IO belongs to T60/
// T65/T67. It does not claim to be a NATS/Caddy process or its running identity.
type versionFixture struct {
	calls   int
	edit    func(*VersionReport)
	process bool
	fail    bool
}

func (s *versionFixture) Version(v *Verified, d infracontract.Deadline, _ infracontract.Stop) (VersionReport, error) {
	s.calls++
	output := "nats-server: v" + v.artifact.Version
	if v.artifact.Kind == infracontract.CaddyBinary {
		output = "v" + v.artifact.Version + " fixture"
	}
	if s.process {
		remaining, err := controlbudget.Remaining(d)
		if err != nil {
			return VersionReport{}, err
		}
		// ctx-root: hermetic supervised fixture command owns its cancellation scope.
		ctx, cancel := context.WithTimeout(context.Background(), remaining)
		defer cancel()
		cmd := exec.CommandContext(ctx, "/bin/sh", "-c", "printf '%s' \"$1\"", "fixture", output)
		cmd.Env = []string{"PATH=/usr/bin:/bin"}
		cmd.Dir = "/tmp"
		if s.fail {
			cmd = exec.CommandContext(ctx, "/bin/sh", "-c", "exit 23")
			cmd.Env = []string{"PATH=/usr/bin:/bin"}
		}
		raw, err := testharness.Output(cmd)
		if err != nil {
			return VersionReport{}, errors.New("private-canary")
		}
		output = string(raw)
	}
	report := VersionReport{v.artifact, sha256.Sum256(v.Bytes()), 1000, true, true, output}
	if s.edit != nil {
		s.edit(&report)
	}
	return report, nil
}

type pairSource struct {
	entries []Entry
	sources []*memorySource
}

func (s *pairSource) Open(e Entry, d infracontract.Deadline, stop infracontract.Stop) (io.ReadCloser, io.ReadCloser, error) {
	for i, entry := range s.entries {
		if entry == e {
			return s.sources[i].Open(e, d, stop)
		}
	}
	return nil, nil, ErrArtifactInvalid
}

type compatibilityFixture struct {
	calls int
	edit  func(*CompatibilityReport)
	fail  bool
}

func (p *compatibilityFixture) Check(r CompatibilityRequest, current, previous *Verified, _ infracontract.Deadline) (CompatibilityReport, error) {
	p.calls++
	if current.Artifact() != r.Candidate || previous.Artifact() != r.Previous {
		return CompatibilityReport{}, ErrIncompatible
	}
	if p.fail {
		return CompatibilityReport{}, errors.New("private-canary")
	}
	report := CompatibilityReport{1, r, sha256.Sum256([]byte("nonempty-current-fixture-state")), true, true, true, true}
	if p.edit != nil {
		p.edit(&report)
	}
	return report, nil
}

func pairFixture(t *testing.T) (*Catalog, *compatibilityFixture) {
	t.Helper()
	e1, s1 := fixtureEntry(t, "2.14.6", 1, []archiveMember{{"binary", tar.TypeReg, 0755, fixtureELF()}})
	e2, s2 := fixtureEntry(t, "2.14.5", 1, []archiveMember{{"binary", tar.TypeReg, 0755, append(fixtureELF(), 1)}})
	p := &compatibilityFixture{}
	return &Catalog{entries: []Entry{e1, e2}, digest: sha256.Sum256([]byte("pair-catalog")), config: Config{
		Source: &pairSource{[]Entry{e1, e2}, []*memorySource{s1, s2}}, Supervisor: &versionFixture{}, Compatibility: p,
	}}, p
}

// origin: hot-update S9 plan §3.1 / T04; report-binding interface only.
func TestInfraPreviousReadsCurrentState(t *testing.T) {
	c, p := pairFixture(t)
	candidate, previous := c.artifact(c.entries[0]), c.artifact(c.entries[1])
	schema, store, d := sha256.Sum256([]byte("schema")), infracontract.Store{Device: 1, Inode: 2, MountID: 3, LockDevice: 4, LockInode: 5}, testDeadline(t)
	for _, pair := range [][2]infracontract.Artifact{{candidate, previous}, {previous, candidate}} {
		if err := c.Compatible(pair[0], pair[1], schema, store, d); err != nil {
			t.Fatal(err)
		}
	}
	if p.calls != 2 {
		t.Fatal("compatibility producer not consulted in both directions")
	}
	edits := []struct {
		name string
		edit func(*CompatibilityReport)
	}{
		{"missing-version", func(r *CompatibilityReport) { r.Version = 0 }},
		{"missing-state", func(r *CompatibilityReport) { r.CurrentState = infracontract.Digest{} }},
		{"missing-write", func(r *CompatibilityReport) { r.Written = false }},
		{"missing-previous-read", func(r *CompatibilityReport) { r.PreviousRead = false }},
		{"missing-reupgrade", func(r *CompatibilityReport) { r.Reupgraded = false }},
		{"incomplete", func(r *CompatibilityReport) { r.Complete = false }},
		{"wrong-current", func(r *CompatibilityReport) { r.Request.Candidate.BinarySHA256[0]++ }},
		{"wrong-previous", func(r *CompatibilityReport) { r.Request.Previous.BinarySHA256[0]++ }},
		{"wrong-schema", func(r *CompatibilityReport) { r.Request.Schema[0]++ }},
		{"wrong-store-device", func(r *CompatibilityReport) { r.Request.Store.Device++ }},
		{"wrong-store-inode", func(r *CompatibilityReport) { r.Request.Store.Inode++ }},
		{"wrong-store-mount", func(r *CompatibilityReport) { r.Request.Store.MountID++ }},
		{"wrong-lock-device", func(r *CompatibilityReport) { r.Request.Store.LockDevice++ }},
		{"wrong-lock-inode", func(r *CompatibilityReport) { r.Request.Store.LockInode++ }},
	}
	for _, tc := range edits {
		t.Run(tc.name, func(t *testing.T) {
			p.edit = tc.edit
			if err := c.Compatible(candidate, previous, schema, store, d); err != ErrIncompatible {
				t.Fatalf("bad evidence accepted: %v", err)
			}
		})
	}
	p.edit = nil
	p.fail = true
	if err := c.Compatible(candidate, previous, schema, store, d); err != ErrIncompatible || stringsContainCanary(err) {
		t.Fatal("producer error leaked/accepted")
	}
	p.fail = false
	for _, bad := range []infracontract.Artifact{candidate, {}, func() infracontract.Artifact { a := previous; a.BinarySHA256[0]++; return a }()} {
		before := p.calls
		if err := c.Compatible(candidate, bad, schema, store, d); err == nil || p.calls != before {
			t.Fatal("invalid previous reached probe")
		}
	}
	// A legitimate catalog entry from the other component also fails before IO.
	other := c.entries[1]
	other.Kind = infracontract.CaddyBinary
	other.Version = "2.7.5"
	c.entries = append(c.entries, other)
	if c.Compatible(candidate, c.artifact(other), schema, store, d) != ErrIncompatible {
		t.Fatal("foreign component previous admitted")
	}
	if c.Compatible(candidate, previous, infracontract.Digest{}, store, d) != ErrIncompatible {
		t.Fatal("missing schema admitted")
	}
	for field := 0; field < 5; field++ {
		bad := store
		switch field {
		case 0:
			bad.Device = 0
		case 1:
			bad.Inode = 0
		case 2:
			bad.MountID = 0
		case 3:
			bad.LockDevice = 0
		case 4:
			bad.LockInode = 0
		}
		if c.Compatible(candidate, previous, schema, bad, d) != ErrIncompatible {
			t.Fatal("incomplete store admitted")
		}
	}
	c.config.Compatibility = nil
	if c.Compatible(candidate, previous, schema, store, d) != infracontract.ErrIncomplete {
		t.Fatal("missing compatibility adapter signed success")
	}
	c.config.Compatibility = p
	c.config.Supervisor = nil
	if c.Compatible(candidate, previous, schema, store, d) != infracontract.ErrIncomplete {
		t.Fatal("missing supervisor signed success")
	}
	c.config.Source = nil
	if c.Verify(candidate, d, nil) != infracontract.ErrIncomplete {
		t.Fatal("missing source signed success")
	}
}

// origin: hot-update S9 plan §3.1 / T03
func exerciseVersionBinding(t *testing.T) {
	t.Helper()
	e, s := fixtureEntry(t, "2.14.6", 1, []archiveMember{{"binary", tar.TypeReg, 0755, fixtureELF()}})
	for i, edit := range []func(*VersionReport){
		func(r *VersionReport) { r.Artifact.BinarySHA256[0]++ },
		func(r *VersionReport) { r.RunningSHA256[0]++ },
		func(r *VersionReport) { r.UID = 0 },
		func(r *VersionReport) { r.Isolated = false },
		func(r *VersionReport) { r.Joined = false },
		func(r *VersionReport) { r.Output = "nats-server: v2.14.5" },
		func(r *VersionReport) { r.Output = "version 2.14.6" },
		func(r *VersionReport) { r.Output = fmt.Sprintf("%05000d", 1) },
	} {
		c := fixtureCatalog(e, s, &versionFixture{edit: edit})
		if err := c.Verify(c.artifact(e), testDeadline(t), nil); err != ErrArtifactInvalid {
			t.Fatalf("execution evidence %d accepted: %v", i, err)
		}
	}
}

// origin: hot-update S9 plan §12 P01 / L:TestInfraArtifactLifecycle
func TestInfraArtifactLifecycle(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("shared supervisor exercise requires Linux")
	}
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseArtifact(t)
	}
	testharness.AssertNoGoroutineLeak(t, "artifact", beforeG)
	testharness.AssertNoFDLeak(t, "artifact", beforeFD, 0)
}
func exerciseArtifact(t *testing.T) {
	t.Helper()
	e, s := fixtureEntry(t, "2.14.6", 1, []archiveMember{{"binary", tar.TypeReg, 0755, fixtureELF()}})
	runner := &versionFixture{process: true}
	c := fixtureCatalog(e, s, runner)
	if err := c.Verify(c.artifact(e), testDeadline(t), nil); err != nil {
		t.Fatal(err)
	}
	runner.fail = true
	if err := c.Verify(c.artifact(e), testDeadline(t), nil); err != ErrArtifactInvalid || stringsContainCanary(err) {
		t.Fatalf("supervised failure accepted/leaked: %v", err)
	}
	if s.opens != 2 || s.closes != 4 {
		t.Fatal("source lifetime leaked")
	}
	// Cancel an actual tree, including the shell's sleep descendant, and join the
	// supervisor. The parent-death lease is installed before either target starts.
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	cmd := exec.CommandContext(ctx, "/bin/sh", "-c", "sleep 30")
	cmd.Env = []string{"PATH=/usr/bin:/bin"}
	if err := testharness.RunProcess(cmd); err == nil || ctx.Err() == nil {
		t.Fatal("blocked fixture was not cancelled and joined")
	}
}

// origin: hot-update S9 plan §3.1 / T03; actual downloaded stock commands.
func exerciseOfficialVersion(t *testing.T, v *Verified) {
	t.Helper()
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
		t.Skip("stock command receipt requires Linux amd64")
	}
	if os.Geteuid() == 0 {
		t.Fatal("stock validation command must not execute as root")
	}
	dir := t.TempDir()
	executable := filepath.Join(dir, "candidate")
	if err := os.WriteFile(executable, v.Bytes(), 0500); err != nil {
		t.Fatal(err)
	}
	a := v.Artifact()
	args := []string{"-v"}
	if a.Kind == infracontract.CaddyBinary {
		args = []string{"version"}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, executable, args...)
	cmd.Dir = dir
	cmd.Env = []string{"PATH=/usr/bin:/bin", "HOME=" + dir, "XDG_DATA_HOME=" + dir, "XDG_CONFIG_HOME=" + dir}
	output, err := testharness.Output(cmd)
	if err != nil {
		t.Fatalf("stock version command failed: %v", err)
	}
	text := strings.TrimSpace(string(output))
	if a.Kind == infracontract.NATSBinary && text != "nats-server: v"+a.Version {
		t.Fatalf("stock NATS version mismatch: %q", text)
	}
	if a.Kind == infracontract.CaddyBinary && !strings.HasPrefix(text, "v"+a.Version+" ") {
		t.Fatalf("stock Caddy version mismatch: %q", text)
	}
	t.Logf("stock version command joined: kind=%d version=%s uid=%d binary=%x", a.Kind, a.Version, os.Geteuid(), a.BinarySHA256)
}
