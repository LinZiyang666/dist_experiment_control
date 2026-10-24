package upgradecli

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agentimage"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

func releaseArchive(t *testing.T, name string, body []byte) ([]byte, string) {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o755, Size: int64(len(body)), Typeflag: tar.TypeReg}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write(body); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(buf.Bytes())
	return buf.Bytes(), hex.EncodeToString(sum[:])
}

// The candidate named in a grant is the extracted executable the archive
// digest vouches for, bound to its own reader declarations. A digest mismatch,
// a missing entry or a failed probe refuses before any request is built, and
// the private copy never outlives the command.
// origin: hot-update S8 plan §5.2 / §6.1 / §11.2 P19
func TestFetchCandidateBindsArchiveDigestToProbedBytes(t *testing.T) {
	body := []byte("candidate executable bytes")
	archive, digest := releaseArchive(t, "tether", body)
	fetch := func(string, time.Duration) ([]byte, error) { return archive, nil }
	caps := agentimage.Capabilities{ABI: 1, ReadEpochs: []uint64{1}, Domains: []string{"exec-v1"}}
	var probed string
	probe := func(path string) (string, agentimage.Capabilities, error) {
		probed = path
		got, err := os.ReadFile(path)
		if err != nil || !bytes.Equal(got, body) {
			return "", agentimage.Capabilities{}, errors.New("probe saw other bytes")
		}
		c := caps
		sum := sha256.Sum256(got)
		c.SHA256 = hex.EncodeToString(sum[:])
		return "v9", c, nil
	}
	c, err := FetchCandidate(upgradetx.ComponentAgent, "https://example.invalid/t.tar.gz", digest, fetch, probe)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(body)
	if c.SHA != hex.EncodeToString(sum[:]) || c.Release != "v9" || c.CapabilityDigest != CapabilityDigest(upgradetx.ComponentAgent, caps) {
		t.Fatalf("candidate is not bound to the probed executable: %+v", c)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(probed); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("candidate copy outlived the command")
	}
	wrong, _ := releaseArchive(t, "not-tether", body)
	for name, run := range map[string]func() error{
		"digest": func() error {
			_, err := FetchCandidate(upgradetx.ComponentAgent, "u", strings.Repeat("0", 64), fetch, probe)
			return err
		},
		"entry": func() error {
			_, err := FetchCandidate(upgradetx.ComponentAgent, "u", hexSHA(wrong), func(string, time.Duration) ([]byte, error) { return wrong, nil }, probe)
			return err
		},
		"probe": func() error {
			_, err := FetchCandidate(upgradetx.ComponentAgent, "u", digest, fetch, func(string) (string, agentimage.Capabilities, error) {
				return "", agentimage.Capabilities{}, errors.New("not executable")
			})
			return err
		},
	} {
		if err := run(); ExitCode(err) != 64 {
			t.Fatalf("%s: unverified candidate accepted: %v", name, err)
		}
	}
	if _, err := FetchCandidate(upgradetx.ComponentAgent, "u", digest, func(string, time.Duration) ([]byte, error) {
		return nil, errors.New("mirror down")
	}, probe); ExitCode(err) != 75 {
		t.Fatalf("an unavailable mirror must be a retryable deferral: %v", err)
	}
}

func hexSHA(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// The capability digest is order-insensitive but changes with any reader
// declaration, so a candidate rebuilt with another reader set cannot be
// published under a grant prepared for the first one.
func TestCapabilityDigestTracksEveryReaderDeclaration(t *testing.T) {
	base := agentimage.Capabilities{ABI: 1, ReadEpochs: []uint64{1, 2}, Domains: []string{"exec-v1", "pty-v1"}, Features: []string{"a", "b"}}
	reordered := agentimage.Capabilities{ABI: 1, ReadEpochs: []uint64{2, 1}, Domains: []string{"pty-v1", "exec-v1"}, Features: []string{"b", "a"}, SHA256: "ignored"}
	want := CapabilityDigest(upgradetx.ComponentAgent, base)
	if CapabilityDigest(upgradetx.ComponentAgent, reordered) != want || !hexValue(want, 64) {
		t.Fatal("capability digest depends on declaration order or the executable SHA")
	}
	for name, mutate := range map[string]func(*agentimage.Capabilities){
		"abi":     func(c *agentimage.Capabilities) { c.ABI = 2 },
		"epochs":  func(c *agentimage.Capabilities) { c.ReadEpochs = []uint64{1} },
		"domains": func(c *agentimage.Capabilities) { c.Domains = append(c.Domains, "flow-v1") },
		"feature": func(c *agentimage.Capabilities) { c.Features = nil },
	} {
		c := base
		c.ReadEpochs, c.Domains, c.Features = append([]uint64(nil), base.ReadEpochs...), append([]string(nil), base.Domains...), append([]string(nil), base.Features...)
		mutate(&c)
		if CapabilityDigest(upgradetx.ComponentAgent, c) == want {
			t.Fatalf("%s change did not change the capability digest", name)
		}
	}
	if CapabilityDigest(upgradetx.ComponentBroker, base) == want {
		t.Fatal("capability digest does not bind the component")
	}
}

// Prepare names only the described participant and a candidate that differs
// from the running image; the ctl chooses nothing but a fresh client nonce.
func TestBuildPrepareUsesOnlyDescribedIdentity(t *testing.T) {
	key := remoteKey()
	target := Target{Key: key, SourceSHA: strings.Repeat("1", 64), SourceRelease: "v1",
		Scope: upgradetx.Scope{ID: strings.Repeat("d", 32), Participants: []upgradetx.Key{key}, Domains: allTestDomains()}}
	candidate := Candidate{SHA: strings.Repeat("2", 64), Release: "v2", CapabilityDigest: strings.Repeat("3", 64)}
	now := time.Unix(1000, 0)
	req, err := BuildPrepare(target, candidate, "SHA256:actor", now)
	if err != nil {
		t.Fatal(err)
	}
	if req.Mode != upgradetx.ModeHot || req.SourceSHA != target.SourceSHA || req.TargetSHA != candidate.SHA || req.TargetRelease != "v2" ||
		req.CapabilityDigest != candidate.CapabilityDigest || req.ParentTxID != "" || !hexValue(req.ClientNonce, 32) || req.ExpiresAt <= now.UnixNano() ||
		len(req.Scope.Participants) != 1 || req.Scope.Participants[0] != key {
		t.Fatalf("prepare request is not bound to the described target: %+v", req)
	}
	again, _ := BuildPrepare(target, candidate, "SHA256:actor", now)
	if again.ClientNonce == req.ClientNonce {
		t.Fatal("two prepares reused one client nonce")
	}
	for name, mutate := range map[string]func(*Target, *Candidate){
		"same_image":   func(_ *Target, c *Candidate) { c.SHA = strings.Repeat("1", 64) },
		"other_scope":  func(tg *Target, _ *Candidate) { tg.Scope.Participants[0].ConfiguredNID = "other" },
		"no_release":   func(tg *Target, _ *Candidate) { tg.SourceRelease = "" },
		"bad_digest":   func(_ *Target, c *Candidate) { c.CapabilityDigest = "short" },
		"domain_short": func(tg *Target, _ *Candidate) { tg.Scope.Domains = tg.Scope.Domains[1:] },
	} {
		tg, c := target, candidate
		tg.Scope.Participants = append([]upgradetx.Key(nil), target.Scope.Participants...)
		tg.Scope.Domains = append([]upgradetx.Domain(nil), target.Scope.Domains...)
		mutate(&tg, &c)
		if _, err := BuildPrepare(tg, c, "SHA256:actor", now); ExitCode(err) != 64 {
			t.Fatalf("%s: prepare accepted an undescribed identity: %v", name, err)
		}
	}
	if _, err := UndescribedTarget(t.Context()); ExitCode(err) != 75 {
		t.Fatalf("the production describer must refuse before any effect: %v", err)
	}
}
