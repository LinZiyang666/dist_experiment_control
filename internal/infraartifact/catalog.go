// Package infraartifact admits only the official adjacent S9 component releases.
// It neither installs files nor executes a caller-supplied path.
// origin: hot-update S9 plan §3.1 / T03
package infraartifact

import (
	"bytes"
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"strings"

	"github.com/LinZiyang666/tether/internal/infracontract"
)

var (
	ErrArtifactInvalid = errors.New("infra_artifact_invalid")
	ErrIncompatible    = errors.New("infra_incompatible")
	// Cancellation stays a local error; it does not extend the closed wire codes.
	ErrStopped = context.Canceled
)

//go:embed testdata/catalog.json
var catalogBytes []byte

// Entry records both the locally measured SHA256 identities and the original
// official checksum algorithm. Caddy's SHA512 must not be replaced by SHA256.
type Entry struct {
	Kind              infracontract.ArtifactKind
	Version           string
	GOOS              string
	GOARCH            string
	ArchiveName       string
	ArchiveURL        string
	ArchiveSHA256     string
	BinarySHA256      string
	ChecksumURL       string
	ChecksumSHA256    string
	ChecksumAlgorithm string
	OfficialChecksum  string
	BinaryMember      string
}

// Source must bound and cancel its reads using the supplied deadline and stop.
// Open returns independent streams; the catalog owns and closes both, including
// partially successful opens. URLs are from the embedded allowlist only.
type Source interface {
	Open(Entry, infracontract.Deadline, infracontract.Stop) (archive, checksums io.ReadCloser, err error)
}

type Config struct {
	Source        Source
	Supervisor    Supervisor
	Compatibility CompatibilityProbe
}

type Catalog struct {
	entries []Entry
	digest  infracontract.Digest
	config  Config
}

var _ infracontract.Artifacts = (*Catalog)(nil)

// New uses the embedded catalog rather than a writable caller manifest. Missing
// adapters permit resolution but cannot verify or authorize a compatibility cut.
func New(config Config) (*Catalog, error) {
	var entries []Entry
	decoder := json.NewDecoder(bytes.NewReader(catalogBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&entries); err != nil {
		return nil, ErrArtifactInvalid
	}
	if decoder.Decode(new(json.RawMessage)) != io.EOF || len(entries) != 4 {
		return nil, ErrArtifactInvalid
	}
	seen := make(map[string]bool)
	for _, entry := range entries {
		if err := validEntry(entry); err != nil {
			return nil, err
		}
		key := entry.ArchiveURL
		if seen[key] {
			return nil, ErrArtifactInvalid
		}
		seen[key] = true
	}
	return &Catalog{entries: entries, digest: sha256.Sum256(catalogBytes), config: config}, nil
}

func validEntry(e Entry) error {
	if e.GOOS != "linux" || e.GOARCH != "amd64" {
		return infracontract.ErrUnsupported
	}
	var base, name, member, sumName, algorithm string
	switch e.Kind {
	case infracontract.NATSBinary:
		if e.Version != "2.14.5" && e.Version != "2.14.6" {
			return ErrArtifactInvalid
		}
		base = "https://github.com/nats-io/nats-server/releases/download/v" + e.Version + "/"
		name = "nats-server-v" + e.Version + "-linux-amd64.tar.gz"
		member = strings.TrimSuffix(name, ".tar.gz") + "/nats-server"
		sumName, algorithm = "SHA256SUMS", "sha256"
	case infracontract.CaddyBinary:
		if e.Version != "2.7.5" && e.Version != "2.7.6" {
			return ErrArtifactInvalid
		}
		base = "https://github.com/caddyserver/caddy/releases/download/v" + e.Version + "/"
		name, member = "caddy_"+e.Version+"_linux_amd64.tar.gz", "caddy"
		sumName, algorithm = "caddy_"+e.Version+"_checksums.txt", "sha512"
	case infracontract.HelperBinary, infracontract.LauncherBinary:
		// P26 supplies release-bound support catalogs; no placeholder authorizes them.
		return infracontract.ErrUnsupported
	default:
		return ErrArtifactInvalid
	}
	if e.ArchiveName != name || e.ArchiveURL != base+name || e.BinaryMember != member || e.ChecksumURL != base+sumName || e.ChecksumAlgorithm != algorithm {
		return ErrArtifactInvalid
	}
	for _, value := range []string{e.ArchiveSHA256, e.BinarySHA256, e.ChecksumSHA256} {
		if _, err := digest(value); err != nil {
			return err
		}
	}
	checksumSize := 64
	if algorithm == "sha512" {
		checksumSize = 128
	}
	sum, err := hex.DecodeString(e.OfficialChecksum)
	if err != nil || len(sum)*2 != checksumSize || strings.ToLower(e.OfficialChecksum) != e.OfficialChecksum {
		return ErrArtifactInvalid
	}
	return nil
}

func digest(value string) (infracontract.Digest, error) {
	var result infracontract.Digest
	b, err := hex.DecodeString(value)
	if err != nil || len(b) != len(result) || strings.ToLower(value) != value {
		return result, ErrArtifactInvalid
	}
	copy(result[:], b)
	if result == (infracontract.Digest{}) {
		return result, ErrArtifactInvalid
	}
	return result, nil
}

func (c *Catalog) artifact(e Entry) infracontract.Artifact {
	archive, _ := digest(e.ArchiveSHA256)
	binary, _ := digest(e.BinarySHA256)
	return infracontract.Artifact{Kind: e.Kind, Version: e.Version, GOOS: e.GOOS, GOARCH: e.GOARCH, ArchiveSHA256: archive, BinarySHA256: binary, CatalogSHA256: c.digest}
}

func (c *Catalog) Resolve(kind infracontract.ArtifactKind, version, goos, goarch string) (infracontract.Artifact, error) {
	if goos != "linux" || goarch != "amd64" {
		return infracontract.Artifact{}, infracontract.ErrUnsupported
	}
	for _, e := range c.entries {
		if e.Kind == kind && e.Version == version {
			return c.artifact(e), nil
		}
	}
	return infracontract.Artifact{}, ErrArtifactInvalid
}

func (c *Catalog) entry(a infracontract.Artifact) (Entry, error) {
	for _, e := range c.entries {
		if c.artifact(e) == a {
			return e, nil
		}
	}
	return Entry{}, ErrArtifactInvalid
}
