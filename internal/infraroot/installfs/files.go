package installfs

import (
	"crypto/sha256"
	"io"
	"os"
	"path/filepath"
	"strings"

	c "github.com/LinZiyang666/tether/internal/infracontract"
)

// Files pins the installation namespace. Production uses / and owner
// zero; hermetic tests supply a private namespace and their own numeric owner.
// No IPC field or environment variable selects this root.
// origin: S9 internal review rv-infra F01
type Files struct {
	root   *os.File
	prefix string
	owner  uint32
}

// Path projects fixed composition paths into the pinned installation namespace.
func (f *Files) Path(name string) string {
	return filepath.Join(f.prefix, strings.TrimPrefix(name, "/"))
}

// Owner is fixed by the trusted composition input, never by an IPC request.
func (f *Files) Owner() uint32 { return f.owner }

// HashOpened hashes the supplied immutable descriptor without changing its offset or reopening its path.
func HashOpened(file *os.File) (c.Digest, error) {
	stat, err := file.Stat()
	if err != nil {
		return c.Digest{}, err
	}
	hash := sha256.New()
	if _, err := io.Copy(hash, io.NewSectionReader(file, 0, stat.Size())); err != nil {
		return c.Digest{}, err
	}
	var digest c.Digest
	copy(digest[:], hash.Sum(nil))
	return digest, nil
}
