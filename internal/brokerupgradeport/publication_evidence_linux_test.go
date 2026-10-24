package brokerupgradeport

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/upgradeartifact"
	"github.com/LinZiyang666/tether/internal/upgradelock"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// Decoder declarations are a unit-test port; publication, hashing, modes,
// provenance, reservations and all later Verify calls use the real owners.
// The CLI counterexamples supply the actual product binary/decoder coverage.
type evidencePublisher struct{ location upgradeartifact.Location }

func (p evidencePublisher) Authorize(context.Context, upgradetx.Grant) (upgradeartifact.Location, error) {
	return p.location, nil
}
func (p evidencePublisher) Reserve(ctx context.Context, g upgradetx.Grant) (upgradelock.Lease, error) {
	return upgradelock.Acquire(ctx, g.Handle.Key, "")
}
func (p evidencePublisher) CheckImage(_ context.Context, path, sha, release, caps string) (string, error) {
	got, err := upgradeartifact.FileSHA(path)
	if err != nil || got != sha {
		return "", fmt.Errorf("fixture image digest differs")
	}
	return evidenceSHA([]byte(sha + release + caps)), nil
}

func publicationEvidenceFixture(t *testing.T, ttl ...time.Duration) upgradeartifact.Receipt {
	t.Helper()
	base := t.TempDir()
	canonical := filepath.Join(base, "canonical")
	if err := os.WriteFile(canonical, []byte("source"), 0555); err != nil {
		t.Fatal(err)
	}
	key := upgradetx.Key{Component: upgradetx.ComponentBroker, ConfigPath: filepath.Join(base, "broker.yaml"), UID: uint32(os.Getuid())}
	domain, err := upgradelock.RegisterDomain(upgradelock.Registration{CanonicalPath: canonical, ControlDir: t.TempDir(), DeploymentID: "reader-evidence", Participants: []upgradelock.Participant{{Key: key, ServiceID: "broker"}}})
	if err != nil {
		t.Fatal(err)
	}
	key.DomainID = domain
	directory := filepath.Join(base, "artifacts")
	if err := os.Mkdir(directory, 0700); err != nil {
		t.Fatal(err)
	}
	lifetime := time.Minute
	if len(ttl) != 0 {
		lifetime = ttl[0]
	}
	grant := upgradetx.Grant{Handle: upgradetx.Handle{Key: key, TxID: strings.Repeat("1", 32), CutID: strings.Repeat("2", 32), Generation: 2, Revision: 1},
		ClientNonce: "client", Issuer: upgradetx.Owner{BootID: "boot", PID: os.Getpid(), StartTicks: 1, InstanceID: "instance"}, ActorFP: "actor", Mode: upgradetx.ModeHot,
		SourceRelease: "source", TargetRelease: "target", SourceSHA: evidenceSHA([]byte("source")), TargetSHA: evidenceSHA([]byte("target")),
		ScopeDigest: strings.Repeat("3", 64), CapabilityDigest: strings.Repeat("4", 64), ExpiresAt: time.Now().Add(lifetime).UnixNano()}
	receipt, err := upgradeartifact.Publish(upgradeartifact.WithPublicationPort(t.Context(), evidencePublisher{upgradeartifact.Location{CanonicalPath: canonical, Directory: directory}}), grant, bytes.NewBufferString("source"), bytes.NewBufferString("target"))
	if err != nil {
		t.Fatal(err)
	}
	return receipt
}

func evidenceSHA(b []byte) string { sum := sha256.Sum256(b); return hex.EncodeToString(sum[:]) }

// Accepted custody retains only the exact previously verified publication;
// a matching target pathname cannot excuse changed bytes or authority.
// origin: S8 internal review upgrade-maketest F5
func TestBrokerPublicationDoesNotReuseAChangedAcceptedSlot(t *testing.T) {
	for _, name := range []string{"target", "source", "manifest", "authority"} {
		t.Run(name, func(t *testing.T) {
			r := publicationEvidenceFixture(t)
			p := NewRuntime(RuntimeHost{}).(*brokerRuntime)
			if _, _, err := p.paths(r.Grant, r, upgradetx.View{}, false); err != nil {
				t.Fatal(err)
			}
			p.ready = map[string]preflighted{r.Grant.Handle.TxID: {target: r.TargetPath}}
			path := r.TargetPath
			switch name {
			case "source":
				path = r.SourcePath
			case "manifest":
				path = filepath.Join(filepath.Dir(r.SourcePath), "manifest-"+r.ManifestDigest+".json")
			case "authority":
				r.Grant.ActorFP = "foreign actor"
			}
			if name != "authority" {
				info, err := os.Stat(path)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.Chmod(path, 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, bytes.Repeat([]byte{'!'}, int(info.Size())), 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.Chmod(path, info.Mode()); err != nil {
					t.Fatal(err)
				}
				if err := os.Chtimes(path, info.ModTime(), info.ModTime()); err != nil {
					t.Fatal(err)
				}
			}
			if _, _, err := p.paths(r.Grant, r, upgradetx.View{}, true); err == nil {
				t.Fatal("changed publication borrowed accepted pins")
			}
		})
	}
}

// origin: S8 internal review upgrade-maketest F5
// Count actual Verify operations, including file hashes, rather than timing a
// tiny fixture. Unchanged evidence is reused; unrelated journal renames must
// not force the expensive read back into the freeze window.
func TestBrokerPublicationVerificationStaysOutsideTheFreezeWindow(t *testing.T) {
	r := publicationEvidenceFixture(t)
	port := NewRuntime(RuntimeHost{}).(*brokerRuntime)
	calls := 0
	verify := func(r upgradeartifact.Receipt, g upgradetx.Grant) error { calls++; return upgradeartifact.Verify(r, g) }
	for gate := 0; gate < 3; gate++ {
		// This changes an ancestor's mtime/ctime, just like a journal CAS.
		if err := os.WriteFile(filepath.Join(filepath.Dir(r.CanonicalPath), fmt.Sprintf("journal-%d", gate)), nil, 0600); err != nil {
			t.Fatal(err)
		}
		if err := verifyPublicationUsing(port, r, r.Grant, false, verify); err != nil {
			t.Fatal(err)
		}
	}
	if calls != 1 {
		t.Fatalf("full publication verification ran %d times, want 1 before freeze", calls)
	}
	foreign := r.Grant
	foreign.ActorFP = "another actor"
	if err := port.verifyPublication(r, foreign, true); err == nil {
		t.Fatal("another grant borrowed the cached publication")
	}
	// Expiry does not revoke an accepted cut's exact, already-verified pins.
	// An unaccepted expired authorization still has no execution authority.
	expiring := publicationEvidenceFixture(t, 2*time.Second)
	if err := port.verifyPublication(expiring, expiring.Grant, false); err != nil {
		t.Fatal(err)
	}
	<-time.After(time.Until(time.Unix(0, expiring.Grant.ExpiresAt)))
	if err := port.verifyPublication(expiring, expiring.Grant, true); err != nil {
		t.Fatalf("accepted evidence lost its pins at expiry: %v", err)
	}
	if err := port.verifyPublication(expiring, expiring.Grant, false); err == nil {
		t.Fatal("expired unaccepted evidence authorized a new Start")
	}

}

// origin: S8 internal review upgrade-maketest F5
// The same path/SHA strings never certify changed bytes or provenance. The
// final gate must reject even when size and mtime were restored after a write.
func TestBrokerPublicationEvidenceRejectsChangedBytesAndAuthority(t *testing.T) {
	for _, name := range []string{"target_bytes", "source_bytes", "manifest", "mode", "inode", "directory_mode", "directory_alias", "actor", "nonce", "parent", "issuer", "digest"} {
		t.Run(name, func(t *testing.T) {
			r := publicationEvidenceFixture(t)
			port := NewRuntime(RuntimeHost{}).(*brokerRuntime)
			if err := port.verifyPublication(r, r.Grant, false); err != nil {
				t.Fatal(err)
			}
			switch name {
			case "target_bytes", "source_bytes", "manifest":
				path := r.TargetPath
				if name == "source_bytes" {
					path = r.SourcePath
				}
				if name == "manifest" {
					path = filepath.Join(filepath.Dir(r.SourcePath), "manifest-"+r.ManifestDigest+".json")
				}
				info, err := os.Stat(path)
				if err != nil {
					t.Fatal(err)
				}
				if err = os.Chmod(path, 0600); err != nil {
					t.Fatal(err)
				}
				if err = os.WriteFile(path, bytes.Repeat([]byte{'!'}, int(info.Size())), 0600); err != nil {
					t.Fatal(err)
				}
				if err = os.Chmod(path, info.Mode()); err != nil {
					t.Fatal(err)
				}
				if err = os.Chtimes(path, info.ModTime(), info.ModTime()); err != nil {
					t.Fatal(err)
				}
			case "mode":
				if err := os.Chmod(r.TargetPath, 0755); err != nil {
					t.Fatal(err)
				}
			case "inode":
				if err := os.Remove(r.TargetPath); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(r.TargetPath, []byte("wrong!"), 0555); err != nil {
					t.Fatal(err)
				}
			case "directory_mode":
				if err := os.Chmod(filepath.Dir(r.TargetPath), 0777); err != nil {
					t.Fatal(err)
				}
			case "directory_alias":
				dir := filepath.Dir(r.TargetPath)
				if err := os.Rename(dir, dir+"-moved"); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(dir+"-moved", dir); err != nil {
					t.Fatal(err)
				}
			case "actor":
				r.Grant.ActorFP = "another actor"
			case "nonce":
				r.Grant.ClientNonce = "another nonce"
			case "parent":
				r.Grant.ParentTxID, r.Grant.ParentSlot = strings.Repeat("a", 32), "another slot"
			case "issuer":
				r.Grant.Issuer.StartTicks++
			case "digest":
				r.ManifestDigest = strings.Repeat("a", 64)
			}
			// Passing the changed grant too ensures equality against the verified
			// receipt matters, independently of artifact.Grant == grant.
			if _, _, err := port.paths(r.Grant, r, upgradetx.View{}, true); err == nil {
				t.Fatal("changed publication borrowed verified evidence")
			}
		})
	}
}

// origin: S8 internal review upgrade-maketest F5
func TestBrokerPublicationEvidenceDoesNotSurviveAWriteDuringVerification(t *testing.T) {
	r := publicationEvidenceFixture(t)
	p := NewRuntime(RuntimeHost{}).(*brokerRuntime)
	err := verifyPublicationUsing(p, r, r.Grant, false, func(r upgradeartifact.Receipt, g upgradetx.Grant) error {
		if err := upgradeartifact.Verify(r, g); err != nil {
			return err
		}
		if err := os.Chmod(r.TargetPath, 0600); err != nil {
			return err
		}
		return os.WriteFile(r.TargetPath, []byte("wrong!"), 0600)
	})
	if err == nil || p.publication.receipt != (upgradeartifact.Receipt{}) {
		t.Fatal("raced verification certified changed bytes")
	}
}
