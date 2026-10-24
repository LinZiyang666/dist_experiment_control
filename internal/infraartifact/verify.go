package infraartifact

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"crypto/sha512"
	"debug/elf"
	"encoding/hex"
	"errors"
	"io"
	"path"
	"strings"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/infracontract"
)

const (
	maxArchiveBytes  = 64 << 20
	maxBinaryBytes   = 128 << 20
	maxChecksumBytes = 1 << 20
	maxExpandedBytes = 256 << 20
	maxMembers       = 128
)

// Verified owns a private copy of the extracted bytes. There is no caller path
// or writable source FD to execute after verification. Root promotion still
// copies to its private slot and calls VerifyFinal before selecting that slot.
type Verified struct {
	artifact infracontract.Artifact
	binary   []byte
}

func (v *Verified) Artifact() infracontract.Artifact { return v.artifact }
func (v *Verified) Bytes() []byte                    { return bytes.Clone(v.binary) }

func admission(deadline infracontract.Deadline, stop infracontract.Stop) error {
	select {
	case <-stop:
		return ErrStopped
	default:
	}
	_, err := controlbudget.Remaining(deadline)
	return err
}

type boundedReader struct {
	reader   io.Reader
	deadline infracontract.Deadline
	stop     infracontract.Stop
}

func (r boundedReader) Read(p []byte) (int, error) {
	if err := admission(r.deadline, r.stop); err != nil {
		return 0, err
	}
	return r.reader.Read(p)
}

func readBounded(r io.Reader, limit int64, d infracontract.Deadline, stop infracontract.Stop) ([]byte, error) {
	if r == nil {
		return nil, ErrArtifactInvalid
	}
	b, err := io.ReadAll(io.LimitReader(boundedReader{r, d, stop}, limit+1))
	if err != nil {
		// A source read error may embed credentials. Preserve the local admission
		// failure, but never carry opaque reader diagnostics into product status.
		// origin: hot-update S9 plan §2.2 / T03
		if admissionErr := admission(d, stop); admissionErr != nil {
			return nil, admissionErr
		}
		return nil, ErrArtifactInvalid
	}
	if len(b) == 0 || int64(len(b)) > limit {
		return nil, ErrArtifactInvalid
	}
	if err := admission(d, stop); err != nil {
		return nil, err
	}
	return b, nil
}

// ReadVerified checks the official list, compressed archive, member structure,
// ELF target and final extracted bytes before returning anything executable.
// origin: hot-update S9 plan §3.1 / T03
func (c *Catalog) ReadVerified(a infracontract.Artifact, d infracontract.Deadline, stop infracontract.Stop) (*Verified, error) {
	e, err := c.entry(a)
	if err != nil {
		return nil, err
	}
	if err := admission(d, stop); err != nil {
		return nil, err
	}
	if c.config.Source == nil {
		return nil, infracontract.ErrIncomplete
	}
	archive, sums, err := c.config.Source.Open(e, d, stop)
	if archive != nil {
		defer func() { _ = archive.Close() }()
	}
	if sums != nil {
		defer func() { _ = sums.Close() }()
	}
	if err != nil {
		if admissionErr := admission(d, stop); admissionErr != nil {
			return nil, admissionErr
		}
		return nil, ErrArtifactInvalid
	}
	checksumBytes, err := readBounded(sums, maxChecksumBytes, d, stop)
	if err != nil {
		return nil, err
	}
	archiveBytes, err := readBounded(archive, maxArchiveBytes, d, stop)
	if err != nil {
		return nil, err
	}
	binary, err := verifyArchive(e, archiveBytes, checksumBytes, d, stop)
	if err != nil {
		return nil, err
	}
	return &Verified{a, binary}, nil
}

func verifyArchive(e Entry, archive, sums []byte, d infracontract.Deadline, stop infracontract.Stop) ([]byte, error) {
	expectedSums, err := digest(e.ChecksumSHA256)
	if err != nil || sha256.Sum256(sums) != expectedSums {
		return nil, ErrArtifactInvalid
	}
	if err := uniqueChecksum(e, sums); err != nil {
		return nil, err
	}
	archiveDigest, err := digest(e.ArchiveSHA256)
	if err != nil || sha256.Sum256(archive) != archiveDigest {
		return nil, ErrArtifactInvalid
	}
	switch e.ChecksumAlgorithm {
	case "sha256":
		sum := sha256.Sum256(archive)
		if hex.EncodeToString(sum[:]) != e.OfficialChecksum {
			return nil, ErrArtifactInvalid
		}
	case "sha512":
		sum := sha512.Sum512(archive)
		if hex.EncodeToString(sum[:]) != e.OfficialChecksum {
			return nil, ErrArtifactInvalid
		}
	default:
		return nil, ErrArtifactInvalid
	}
	gz, err := gzip.NewReader(bytes.NewReader(archive))
	if err != nil {
		return nil, ErrArtifactInvalid
	}
	defer func() { _ = gz.Close() }()
	// Consume through the gzip EOF so a valid tar prefix cannot hide a broken
	// gzip trailer or a second archive. Zero tar padding alone is permitted.
	expanded, err := readBounded(gz, maxExpandedBytes, d, stop)
	if err != nil {
		return nil, err
	}
	reader := bytes.NewReader(expanded)
	tr := tar.NewReader(reader)
	seen := make(map[string]bool)
	var binary []byte
	for members := 0; ; members++ {
		if err := admission(d, stop); err != nil {
			return nil, err
		}
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil || members >= maxMembers {
			return nil, ErrArtifactInvalid
		}
		name := strings.TrimSuffix(h.Name, "/")
		if name == "" || strings.Contains(name, "\\") || path.IsAbs(name) || path.Clean(name) != name || name == ".." || strings.HasPrefix(name, "../") || seen[name] {
			return nil, ErrArtifactInvalid
		}
		seen[name] = true
		if h.Typeflag != tar.TypeReg && h.Typeflag != tar.TypeDir {
			return nil, ErrArtifactInvalid
		}
		if h.Size < 0 || h.Size > maxBinaryBytes {
			return nil, ErrArtifactInvalid
		}
		if name == e.BinaryMember {
			if h.Typeflag != tar.TypeReg || h.Mode&0111 == 0 {
				return nil, ErrArtifactInvalid
			}
			binary, err = readBounded(tr, maxBinaryBytes, d, stop)
			if err != nil {
				return nil, err
			}
		}
	}
	remainder := expanded[len(expanded)-reader.Len():]
	if len(bytes.Trim(remainder, "\x00")) != 0 {
		return nil, ErrArtifactInvalid
	}
	if err := finalBytes(e, binary); err != nil {
		return nil, err
	}
	return binary, nil
}

func uniqueChecksum(e Entry, data []byte) error {
	count := 0
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			if strings.TrimSpace(line) != "" {
				return ErrArtifactInvalid
			}
			continue
		}
		name := strings.TrimPrefix(fields[1], "*")
		if name == e.ArchiveName {
			count++
			if fields[0] != e.OfficialChecksum {
				return ErrArtifactInvalid
			}
		}
	}
	// origin: hot-update S9 plan §3.1 / T03; duplicates are ambiguous even when equal.
	if count != 1 {
		return ErrArtifactInvalid
	}
	return nil
}

func finalBytes(e Entry, binary []byte) error {
	expected, err := digest(e.BinarySHA256)
	// origin: hot-update S9 plan §3.1 / T03; archive identity cannot sign executable bytes.
	if err != nil || sha256.Sum256(binary) != expected {
		return ErrArtifactInvalid
	}
	f, err := elf.NewFile(bytes.NewReader(binary))
	if err != nil {
		return ErrArtifactInvalid
	}
	defer func() { _ = f.Close() }()
	if e.GOOS != "linux" || e.GOARCH != "amd64" || f.Class != elf.ELFCLASS64 || f.Data != elf.ELFDATA2LSB || f.Machine != elf.EM_X86_64 || (f.Type != elf.ET_EXEC && f.Type != elf.ET_DYN) || (f.OSABI != elf.ELFOSABI_NONE && f.OSABI != elf.ELFOSABI_LINUX) {
		return ErrArtifactInvalid
	}
	return nil
}

// VerifyFinal rehashes the immutable destination after copying, not the caller
// descriptor. Its owner must fsync and remove write references before selection.
func (c *Catalog) VerifyFinal(a infracontract.Artifact, final io.Reader, d infracontract.Deadline, stop infracontract.Stop) error {
	e, err := c.entry(a)
	if err != nil {
		return err
	}
	b, err := readBounded(final, maxBinaryBytes, d, stop)
	if err != nil {
		return err
	}
	return finalBytes(e, b)
}

func (c *Catalog) Verify(a infracontract.Artifact, d infracontract.Deadline, stop infracontract.Stop) error {
	v, err := c.ReadVerified(a, d, stop)
	if err != nil {
		return err
	}
	return c.probeVersion(v, d, stop)
}
