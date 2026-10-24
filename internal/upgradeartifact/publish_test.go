// These unit tests use real files, fsync, immutable slots and supervised shell
// processes. Local authorization, decoder proofs and host reservations are
// test ports; production P02/P04/P17/P19/P20 wiring is not certified here.
// origin: hot-update S8 plan §5.2 / §11.2 / P05
package upgradeartifact

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradelock"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// Keep the concrete implementations on P00's frozen function signatures.
// origin: hot-update S8 plan §2.2 / P05
var (
	_ PublishFunc = Publish
	_ VerifyFunc  = Verify
	_ ResolveFunc = Resolve
)

type publicationFixture struct {
	grant    upgradetx.Grant
	location Location
	source   []byte
	target   []byte
	port     *publicationTestPort
}

type publicationTestPort struct {
	grant          upgradetx.Grant
	location       Location
	lease          publicationTestLease
	authorizations int
	probes         int
	deny           bool
	failProbe      bool
	changeLocation bool
	waitExpiry     bool
	supervise      bool
}

type publicationTestLease struct {
	domain string
	held   bool
	closed int
	err    error
}

func (l *publicationTestLease) FD() uintptr      { return 7 }
func (l *publicationTestLease) DomainID() string { return l.domain }
func (l *publicationTestLease) Close() error {
	l.held = false
	l.closed++
	return l.err
}

func (p *publicationTestPort) Authorize(ctx context.Context, grant upgradetx.Grant) (Location, error) {
	p.authorizations++
	if err := ctx.Err(); err != nil {
		return Location{}, err
	}
	if p.deny || grant != p.grant {
		return Location{}, artifactError(upgradetx.CodeGrantUnknown, "fixture grant is not registered")
	}
	if p.authorizations > 1 && p.changeLocation {
		return Location{CanonicalPath: p.location.CanonicalPath + "-changed", Directory: p.location.Directory}, nil
	}
	return p.location, nil
}

func (p *publicationTestPort) Reserve(ctx context.Context, _ upgradetx.Grant) (upgradelock.Lease, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.lease.held = true
	return &p.lease, nil
}

func (p *publicationTestPort) CheckImage(ctx context.Context, path, sha, release, capabilities string) (string, error) {
	p.probes++
	if !p.lease.held || capabilities != p.grant.CapabilityDigest || p.failProbe {
		return "", artifactError(upgradetx.CodeRejected, "fixture decoder or reservation refuses publication")
	}
	if err := verifyImage(path, sha, p.grant.Handle.Key); err != nil {
		return "", err
	}
	if p.supervise {
		output, err := testharness.Output(exec.CommandContext(ctx, path))
		if err != nil || string(output) != release+"\n" {
			return "", fmt.Errorf("fixture image execution failed: %w", err)
		}
	}
	if p.waitExpiry {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(time.Until(time.Unix(0, p.grant.ExpiresAt)) + time.Millisecond):
		}
	}
	return digest([]byte("fixture decoder proof:" + sha)), nil
}

func imageBytes(release string) []byte {
	return []byte("#!/bin/sh\nprintf '%s\\n' '" + release + "'\n")
}

func newPublicationFixture(t *testing.T) publicationFixture {
	t.Helper()
	base, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	canonical := filepath.Join(base, "canonical")
	source, target := imageBytes("source"), imageBytes("target")
	if err := os.WriteFile(canonical, target, 0555); err != nil {
		t.Fatal(err)
	}
	directory := filepath.Join(base, "artifacts")
	if err := os.Mkdir(directory, 0755); err != nil {
		t.Fatal(err)
	}
	key := upgradetx.Key{Component: upgradetx.ComponentAgent,
		SID: "session", ConfiguredNID: "node", ConfigPath: filepath.Join(base, "agent.yaml"), UID: uint32(os.Getuid())}
	agentArgv := key
	agentArgv.ConfigPath = ""
	broker := key
	broker.Component = upgradetx.ComponentBroker
	broker.SID = ""
	broker.ConfiguredNID = ""
	id, err := upgradelock.RegisterDomain(upgradelock.Registration{CanonicalPath: canonical, ControlDir: t.TempDir(), DeploymentID: "publication-test", Participants: []upgradelock.Participant{{Key: key, ServiceID: "agent"}, {Key: agentArgv, ServiceID: "agent-argv"}, {Key: broker, ServiceID: "broker"}}})
	if err != nil {
		t.Fatal(err)
	}
	key.DomainID = id
	grant := upgradetx.Grant{Handle: upgradetx.Handle{Key: key, TxID: strings.Repeat("a", 32), CutID: strings.Repeat("b", 32), Generation: 1, Revision: 1},
		ClientNonce: "client-nonce", Issuer: upgradetx.Owner{BootID: "fixture-boot", PID: os.Getpid(), StartTicks: 1, InstanceID: "fixture-instance"},
		ActorFP: "actor", Mode: upgradetx.ModeHot, SourceRelease: "source", TargetRelease: "target", SourceSHA: digest(source), TargetSHA: digest(target),
		ScopeDigest: strings.Repeat("c", 64), CapabilityDigest: strings.Repeat("e", 64), ExpiresAt: time.Now().Add(120 * time.Second).UnixNano()}
	location := Location{CanonicalPath: canonical, Directory: directory}
	port := &publicationTestPort{grant: grant, location: location, lease: publicationTestLease{domain: key.DomainID}}
	return publicationFixture{grant: grant, location: location, source: source, target: target, port: port}
}

func publishFixture(t *testing.T, f publicationFixture) Receipt {
	t.Helper()
	receipt, err := Publish(WithPublicationPort(t.Context(), f.port), f.grant, bytes.NewReader(f.source), bytes.NewReader(f.target))
	if err != nil {
		t.Fatal(err)
	}
	return receipt
}

func requireRefusal(t *testing.T, err error) {
	t.Helper()
	var refusal *upgradetx.Error
	if !errors.As(err, &refusal) || refusal.Code == "" || refusal.Accepted {
		t.Fatalf("expected a typed unaccepted refusal, got %v", err)
	}
}

// Both objects must survive publication and the canonical entry must be untouched.
// origin: hot-update S8 plan §3.2 / §5.2 / P05
func TestPublicationPreservesCanonicalAndBothSHAObjects(t *testing.T) {
	f := newPublicationFixture(t)
	receipt := publishFixture(t, f)
	if err := Verify(receipt, f.grant); err != nil {
		t.Fatal(err)
	}
	for path, want := range map[string][]byte{receipt.SourcePath: f.source, receipt.TargetPath: f.target, f.location.CanonicalPath: f.target} {
		got, err := os.ReadFile(path)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("bytes changed at %s: %v", path, err)
		}
	}
	if f.port.authorizations != 2 || f.port.probes != 2 || f.port.lease.held || f.port.lease.closed != 1 {
		t.Fatalf("publication skipped authority/decoder/reservation: %+v", f.port)
	}
	entries, err := os.ReadDir(filepath.Dir(f.location.CanonicalPath))
	if err != nil || len(entries) != 2 {
		t.Fatalf("stage wrote outside the artifact store: %v, %v", entries, err)
	}
	for _, path := range Pins(receipt) {
		info, err := os.Lstat(path)
		if err != nil || info.Mode().Perm()&0222 != 0 {
			t.Fatalf("published pin is writable or missing: %s: %v", path, err)
		}
	}
}

// No authorization means no reads, no probes and no immutable publication.
// origin: hot-update S8 plan §3.1 / §5.2 / P05
func TestPublicationRequiresTrustedAuthorityBeforeReading(t *testing.T) {
	for _, mode := range []string{"missing", "denied", "expired", "wrong_domain"} {
		t.Run(mode, func(t *testing.T) {
			f := newPublicationFixture(t)
			ctx := t.Context()
			switch mode {
			case "missing":
			case "denied":
				f.port.deny = true
				ctx = WithPublicationPort(ctx, f.port)
			case "expired":
				f.grant.ExpiresAt = 1
				ctx = WithPublicationPort(ctx, f.port)
			case "wrong_domain":
				f.port.lease.domain = "another-domain"
				ctx = WithPublicationPort(ctx, f.port)
			}
			reader := &countingReader{Reader: bytes.NewReader(f.source)}
			receipt, err := Publish(ctx, f.grant, reader, bytes.NewReader(f.target))
			requireRefusal(t, err)
			if reader.calls != 0 || receipt != (Receipt{}) || f.port.probes != 0 {
				t.Fatal("unauthorized stage consumed bytes or minted a receipt")
			}
		})
	}
}

type countingReader struct {
	io.Reader
	calls int
}

func (r *countingReader) Read(data []byte) (int, error) { r.calls++; return r.Reader.Read(data) }

// Refused probes, wrong bytes, alias paths and a changed installation cannot
// leave a usable receipt. Immutable objects may remain safely unclaimed.
// origin: hot-update S8 plan §5.2 / P05
func TestPublicationRejectsUnsafeInputsAndLateAuthorityChange(t *testing.T) {
	for _, mode := range []string{"source_sha", "target_sha", "decoder", "alias", "writable_dir", "late_location", "nil_reader", "cancelled", "foreign_uid"} {
		t.Run(mode, func(t *testing.T) {
			f := newPublicationFixture(t)
			var source, target io.Reader = bytes.NewReader(f.source), bytes.NewReader(f.target)
			ctx := WithPublicationPort(t.Context(), f.port)
			switch mode {
			case "source_sha":
				source = strings.NewReader("wrong source")
			case "target_sha":
				target = strings.NewReader("wrong target")
			case "decoder":
				f.port.failProbe = true
			case "alias":
				alias := f.location.Directory + "-alias"
				if err := os.Symlink(f.location.Directory, alias); err != nil {
					t.Fatal(err)
				}
				f.port.location.Directory = alias
			case "writable_dir":
				if err := os.Chmod(f.location.Directory, 0777); err != nil {
					t.Fatal(err)
				}
			case "late_location":
				f.port.changeLocation = true
			case "foreign_uid":
				if os.Getuid() == 0 {
					t.Skip("root-owned storage is valid for every registered component UID")
				}
				f.grant.Handle.Key.UID++
				f.port.grant = f.grant
			case "nil_reader":
				source = nil
			case "cancelled":
				cancelled, cancel := context.WithCancel(ctx)
				cancel()
				ctx = cancelled
			}
			receipt, err := Publish(ctx, f.grant, source, target)
			if err == nil || receipt != (Receipt{}) || f.port.lease.held {
				t.Fatalf("unsafe publication returned receipt or kept reservation: %v", err)
			}
		})
	}
}

// A retry reuses immutable inodes. A conflicting existing SHA name cannot be
// overwritten even when a fresh reader contains the correct authorized bytes.
// origin: hot-update S8 plan §3.2 / §5.2 / P05
func TestPublicationNeverReplacesPinnedObjects(t *testing.T) {
	f := newPublicationFixture(t)
	receipt := publishFixture(t, f)
	before, err := os.Stat(receipt.SourcePath)
	if err != nil {
		t.Fatal(err)
	}
	f.port.authorizations = 0
	if retry := publishFixture(t, f); retry != receipt {
		t.Fatal("retry changed receipt")
	}
	after, err := os.Stat(receipt.SourcePath)
	if err != nil || !os.SameFile(before, after) {
		t.Fatal("retry replaced pinned inode")
	}
	if err := os.Chmod(receipt.SourcePath, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(receipt.SourcePath, []byte("corrupt pin"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(receipt.SourcePath, 0555); err != nil {
		t.Fatal(err)
	}
	f.port.authorizations = 0
	_, err = Publish(WithPublicationPort(t.Context(), f.port), f.grant, bytes.NewReader(f.source), bytes.NewReader(f.target))
	requireRefusal(t, err)
	got, err := os.ReadFile(receipt.SourcePath)
	if err != nil || string(got) != "corrupt pin" {
		t.Fatal("stage replaced a conflicting pinned object")
	}
}

// A grant that was live before a slow decoder cannot receive a late receipt.
// origin: hot-update S8 plan §3.1 / §5.2 / P05
func TestPublicationRechecksExpiryAfterDecoder(t *testing.T) {
	f := newPublicationFixture(t)
	f.grant.ExpiresAt = time.Now().Add(time.Second).UnixNano()
	f.port.grant = f.grant
	f.port.waitExpiry = true
	receipt, err := Publish(WithPublicationPort(t.Context(), f.port), f.grant, bytes.NewReader(f.source), bytes.NewReader(f.target))
	requireRefusal(t, err)
	var refusal *upgradetx.Error
	if !errors.As(err, &refusal) || refusal.Code != upgradetx.CodeGrantExpired || receipt != (Receipt{}) || f.port.probes != 2 || f.port.lease.held {
		t.Fatalf("expired decoder completion was accepted or was not exercised: %v", err)
	}
}

// Immutable objects do not turn a failed host-reservation release into success.
// origin: hot-update S8 plan §5.1 / §5.2 / P05
func TestPublicationReleaseFailureDoesNotCertifySuccess(t *testing.T) {
	f := newPublicationFixture(t)
	f.port.lease.err = errors.New("injected reservation release failure")
	receipt, err := Publish(WithPublicationPort(t.Context(), f.port), f.grant, bytes.NewReader(f.source), bytes.NewReader(f.target))
	requireRefusal(t, err)
	if receipt != (Receipt{}) || f.port.lease.closed != 1 {
		t.Fatal("failed release certified publication")
	}
	if _, err := os.Stat(filepath.Join(f.location.Directory, f.grant.SourceSHA)); err != nil {
		t.Fatal("failed release destroyed original pin")
	}
}

// Each file must be sealed and synced before its name is published, and each
// publication must sync the directory. No failed durability boundary certifies
// a receipt or releases a reservation before the attempted publication ends.
// origin: hot-update S8 plan §3.2 / §5.2 / P05
func TestPublicationDurabilityFailuresNeverMintReceipt(t *testing.T) {
	for failAt := 0; failAt <= 6; failAt++ {
		t.Run(fmt.Sprintf("sync_%d", failAt), func(t *testing.T) {
			f := newPublicationFixture(t)
			var observed []string
			observe := func(file *os.File) error {
				info, err := file.Stat()
				if err != nil {
					return err
				}
				kind := "file"
				if info.IsDir() {
					kind = "directory"
				} else if info.Mode().Perm()&0222 != 0 {
					t.Fatal("Sync preceded permission sealing")
				}
				if !f.port.lease.held {
					t.Fatal("Sync occurred without the host reservation")
				}
				observed = append(observed, kind)
				if len(observed) == failAt {
					return errors.New("injected sync failure")
				}
				return file.Sync()
			}
			ctx := WithPublicationPort(t.Context(), f.port)
			receipt, err := publish(ctx, f.grant, bytes.NewReader(f.source), bytes.NewReader(f.target), observe)
			if failAt == 0 {
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(observed, []string{"file", "directory", "file", "directory", "file", "directory"}) {
					t.Fatalf("durability order: %v", observed)
				}
			} else {
				requireRefusal(t, err)
				if receipt != (Receipt{}) || len(observed) != failAt {
					t.Fatalf("failed Sync minted proof or continued: %v", observed)
				}
			}
			if f.port.lease.held || f.port.lease.closed != 1 {
				t.Fatal("publication left its reservation open")
			}
		})
	}
}

// Every round includes refused publication plus successful two-image probing.
// origin: hot-update S8 plan §11.2 / P05 child+FD five-round obligation
func TestPublicationReleasesFilesAndSupervisedChildren(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exercisePublicationCleanup(t)
	}
	testharness.AssertNoGoroutineLeak(t, "artifact publication", beforeG)
	testharness.AssertNoFDLeak(t, "artifact publication", beforeFD, 0)
}

func exercisePublicationCleanup(t *testing.T) {
	t.Helper()
	f := newPublicationFixture(t)
	f.port.supervise = true
	publishFixture(t, f)
	f.port.failProbe = true
	f.port.authorizations = 0
	receipt, err := Publish(WithPublicationPort(t.Context(), f.port), f.grant, bytes.NewReader(f.source), bytes.NewReader(f.target))
	if err == nil || !reflect.DeepEqual(receipt, Receipt{}) || f.port.lease.held {
		t.Fatal("failed publication left a usable receipt or a reservation")
	}
	f.port.failProbe = false
	f.port.authorizations = 0
	_, err = Publish(WithPublicationPort(t.Context(), f.port), f.grant, strings.NewReader("bad source bytes"), bytes.NewReader(f.target))
	requireRefusal(t, err)
	entries, err := os.ReadDir(f.location.Directory)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".stage-") {
			t.Fatal("publication kept a temporary file")
		}
	}
}
