package infraartifact

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/infracontract"
)

type memorySource struct {
	archive, sums []byte
	opens, closes int
	fail          bool
	readFail      bool
}
type countedStream struct {
	io.Reader
	owner *memorySource
}

func (s *countedStream) Read(p []byte) (int, error) {
	if s.owner.readFail {
		return 0, errors.New("private-canary")
	}
	return s.Reader.Read(p)
}
func (s *countedStream) Close() error { s.owner.closes++; return nil }
func (s *memorySource) Open(_ Entry, _ infracontract.Deadline, _ infracontract.Stop) (io.ReadCloser, io.ReadCloser, error) {
	s.opens++
	a := &countedStream{bytes.NewReader(s.archive), s}
	b := &countedStream{bytes.NewReader(s.sums), s}
	if s.fail {
		return a, b, errors.New("private-canary")
	}
	return a, b, nil
}

type archiveMember struct {
	name string
	kind byte
	mode int64
	body []byte
}

func fixtureELF() []byte {
	b := make([]byte, 64)
	copy(b, []byte{0x7f, 'E', 'L', 'F', 2, 1, 1})
	binary.LittleEndian.PutUint16(b[16:], 2)
	binary.LittleEndian.PutUint16(b[18:], 62)
	binary.LittleEndian.PutUint32(b[20:], 1)
	binary.LittleEndian.PutUint16(b[52:], 64)
	return b
}
func archiveFixture(t *testing.T, members []archiveMember) []byte {
	t.Helper()
	var b bytes.Buffer
	gz := gzip.NewWriter(&b)
	tr := tar.NewWriter(gz)
	for _, m := range members {
		if err := tr.WriteHeader(&tar.Header{Name: m.name, Typeflag: m.kind, Mode: m.mode, Size: int64(len(m.body)), Linkname: "outside"}); err != nil {
			t.Fatal(err)
		}
		if len(m.body) > 0 {
			if _, err := tr.Write(m.body); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := tr.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}
func fixtureEntry(t *testing.T, version string, kind infracontract.ArtifactKind, members []archiveMember) (Entry, *memorySource) {
	t.Helper()
	e := Entry{Kind: kind, Version: version, GOOS: "linux", GOARCH: "amd64", ArchiveName: "fixture.tar.gz", BinaryMember: "binary", ChecksumAlgorithm: "sha256"}
	archive := archiveFixture(t, members)
	a := sha256.Sum256(archive)
	// Different fixture generations really have different bytes, never only labels.
	b := sha256.Sum256(members[0].body)
	e.ArchiveSHA256, e.BinarySHA256 = hex.EncodeToString(a[:]), hex.EncodeToString(b[:])
	e.OfficialChecksum = e.ArchiveSHA256
	s := &memorySource{archive: archive, sums: []byte(e.OfficialChecksum + "  " + e.ArchiveName + "\n")}
	sealSums(&e, s)
	return e, s
}
func sealSums(e *Entry, s *memorySource) {
	sum := sha256.Sum256(s.sums)
	e.ChecksumSHA256 = hex.EncodeToString(sum[:])
}
func fixtureCatalog(e Entry, s Source, supervisor Supervisor) *Catalog {
	return &Catalog{entries: []Entry{e}, digest: sha256.Sum256([]byte("fixture-catalog")), config: Config{Source: s, Supervisor: supervisor}}
}
func testDeadline(t *testing.T) infracontract.Deadline {
	t.Helper()
	d, err := controlbudget.After(time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

// origin: hot-update S9 plan §3.1 / T03
func TestInfraArtifactBindsFinalBytes(t *testing.T) {
	exerciseVersionBinding(t)
	verifyOfficialArtifacts(t)
	good := archiveMember{"binary", tar.TypeReg, 0755, fixtureELF()}
	e, s := fixtureEntry(t, "2.14.6", 1, []archiveMember{good})
	supervisor := &versionFixture{}
	c := fixtureCatalog(e, s, supervisor)
	a, d := c.artifact(e), testDeadline(t)
	if err := c.Verify(a, d, nil); err != nil || supervisor.calls != 1 || s.closes != 2 {
		t.Fatalf("verified fixture: %v", err)
	}
	v, err := c.ReadVerified(a, d, nil)
	if err != nil {
		t.Fatal(err)
	}
	exposed := v.Bytes()
	exposed[0]++
	if !bytes.Equal(v.Bytes(), good.body) {
		t.Fatal("writable reference changed verified bytes")
	}
	if err := c.VerifyFinal(a, bytes.NewReader(good.body), d, nil); err != nil {
		t.Fatal(err)
	}
	if err := c.VerifyFinal(a, bytes.NewReader(exposed), d, nil); err == nil {
		t.Fatal("modified final copy accepted")
	}
	if err := c.VerifyFinal(a, bytes.NewReader(s.archive), d, nil); err == nil {
		t.Fatal("archive signed as executable")
	}
	for _, mutate := range []func(*infracontract.Artifact){
		func(a *infracontract.Artifact) { a.BinarySHA256 = a.ArchiveSHA256 },
		func(a *infracontract.Artifact) { a.CatalogSHA256[0]++ },
		func(a *infracontract.Artifact) { a.Version = "2.14.5" },
	} {
		bad := a
		mutate(&bad)
		if c.Verify(bad, d, nil) == nil {
			t.Fatal("changed artifact identity accepted")
		}
	}

	cases := []struct {
		name    string
		edit    func(*Entry, *memorySource)
		members []archiveMember
	}{
		{name: "archive-tampered", edit: func(_ *Entry, s *memorySource) { s.archive[0]++ }},
		{name: "list-valid-extra-row", edit: func(_ *Entry, s *memorySource) { s.sums = append(s.sums, []byte("aabb  irrelevant.tar.gz\n")...) }},
		{name: "archive-wrong-pin", edit: func(e *Entry, _ *memorySource) {
			sum := sha256.Sum256([]byte("wrong-archive"))
			e.ArchiveSHA256 = hex.EncodeToString(sum[:])
		}},
		{name: "list-tampered", edit: func(_ *Entry, s *memorySource) { s.sums = append(s.sums, 'x') }},
		{name: "binary-not-archive", edit: func(e *Entry, _ *memorySource) { e.BinarySHA256 = e.ArchiveSHA256 }},
		{name: "binary-tampered", members: []archiveMember{{"binary", tar.TypeReg, 0755, append(fixtureELF(), 1)}}, edit: func(e *Entry, _ *memorySource) {
			sum := sha256.Sum256(fixtureELF())
			e.BinarySHA256 = hex.EncodeToString(sum[:])
		}},
		{name: "duplicate-checksum", edit: func(e *Entry, s *memorySource) { s.sums = append(s.sums, s.sums...); sealSums(e, s) }},
		{name: "conflicting-checksum", edit: func(e *Entry, s *memorySource) {
			s.sums = append(s.sums, []byte("bad  "+e.ArchiveName+"\n")...)
			sealSums(e, s)
		}},
		{name: "missing-checksum", edit: func(e *Entry, s *memorySource) {
			s.sums = []byte(e.OfficialChecksum + "  another.tar.gz\n")
			sealSums(e, s)
		}},
		{name: "official-sha512", edit: func(e *Entry, s *memorySource) {
			e.ChecksumAlgorithm = "sha512"
			e.OfficialChecksum = fmt.Sprintf("%0128d", 1)
			s.sums = []byte(e.OfficialChecksum + "  " + e.ArchiveName + "\n")
			sealSums(e, s)
		}},
		{name: "traversal", members: []archiveMember{good, {"../escape", tar.TypeReg, 0644, []byte("x")}}},
		{name: "absolute", members: []archiveMember{good, {"/escape", tar.TypeReg, 0644, []byte("x")}}},
		{name: "symlink", members: []archiveMember{good, {"link", tar.TypeSymlink, 0777, nil}}},
		{name: "hardlink", members: []archiveMember{good, {"link", tar.TypeLink, 0777, nil}}},
		{name: "device", members: []archiveMember{good, {"device", tar.TypeChar, 0600, nil}}},
		{name: "duplicate-member", members: []archiveMember{good, good}},
		{name: "missing-binary", members: []archiveMember{{"not-binary", tar.TypeReg, 0755, good.body}}},
		{name: "not-executable", members: []archiveMember{{"binary", tar.TypeReg, 0644, good.body}}},
		{name: "not-elf", members: []archiveMember{{"binary", tar.TypeReg, 0755, []byte("untrusted")}}},
		{name: "wrong-machine", members: []archiveMember{{"binary", tar.TypeReg, 0755, func() []byte { b := fixtureELF(); b[18] = 183; return b }()}}},

		{name: "gzip-trailer", edit: func(e *Entry, s *memorySource) { s.archive[len(s.archive)-1]++; repinArchive(e, s) }},
		{name: "nonzero-tar-suffix", edit: func(e *Entry, s *memorySource) {
			var buf bytes.Buffer
			gz := gzip.NewWriter(&buf)
			raw, err := gzip.NewReader(bytes.NewReader(s.archive))
			if err != nil {
				t.Fatal(err)
			}
			expanded, err := io.ReadAll(raw)
			if err != nil {
				t.Fatal(err)
			}
			_ = raw.Close()
			_, _ = gz.Write(append(expanded, 1))
			_ = gz.Close()
			s.archive = buf.Bytes()
			repinArchive(e, s)
		}},
		{name: "wrong-elf-type", members: []archiveMember{{"binary", tar.TypeReg, 0755, func() []byte { b := fixtureELF(); b[16] = 1; return b }()}}},
		{name: "wrong-elf-abi", members: []archiveMember{{"binary", tar.TypeReg, 0755, func() []byte { b := fixtureELF(); b[7] = 9; return b }()}}},
		{name: "noncanonical-member", members: []archiveMember{good, {"a/../escape", tar.TypeReg, 0644, []byte("x")}}},
		{name: "backslash-member", members: []archiveMember{good, {"a\\escape", tar.TypeReg, 0644, []byte("x")}}},
		{name: "checksum-algorithm", edit: func(e *Entry, _ *memorySource) { e.ChecksumAlgorithm = "md5" }},
		{name: "checksum-value", edit: func(e *Entry, s *memorySource) { s.sums = []byte("bad  " + e.ArchiveName + "\n"); sealSums(e, s) }},
		{name: "malformed-checksum", edit: func(e *Entry, s *memorySource) { s.sums = append(s.sums, []byte("bad\n")...); sealSums(e, s) }},

		{name: "private-read-error", edit: func(_ *Entry, s *memorySource) { s.readFail = true }},
		{name: "partial-open", edit: func(_ *Entry, s *memorySource) { s.fail = true }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			members := tc.members
			if members == nil {
				members = []archiveMember{good}
			}
			e, s := fixtureEntry(t, "2.14.6", 1, members)
			if tc.edit != nil {
				tc.edit(&e, s)
			}
			runner := &versionFixture{}
			c := fixtureCatalog(e, s, runner)
			err := c.Verify(c.artifact(e), d, nil)
			if err == nil || stringsContainCanary(err) || runner.calls != 0 || s.closes != 2 {
				t.Fatalf("invalid input executed/leaked: err=%v calls=%d closes=%d", err, runner.calls, s.closes)
			}
		})
	}
	// Caddy's original SHA512 has a separate positive arm.
	e, s = fixtureEntry(t, "2.7.6", 2, []archiveMember{good})
	sum := sha512.Sum512(s.archive)
	e.ChecksumAlgorithm = "sha512"
	e.OfficialChecksum = hex.EncodeToString(sum[:])
	s.sums = []byte(e.OfficialChecksum + " *" + e.ArchiveName + "\n")
	sealSums(&e, s)
	c = fixtureCatalog(e, s, &versionFixture{})
	if err := c.Verify(c.artifact(e), d, nil); err != nil {
		t.Fatal(err)
	}
	oversized := fixtureCatalog(e, s, &versionFixture{edit: func(r *VersionReport) { r.Output = "v2.7.6 " + strings.Repeat("x", 5000) }})
	if err := oversized.Verify(oversized.artifact(e), d, nil); err != ErrArtifactInvalid {
		t.Fatal("unbounded Caddy version output accepted")
	}
	for _, limit := range []int64{1, 32} {
		if _, err := readBounded(bytes.NewReader(make([]byte, limit+1)), limit, d, nil); err == nil {
			t.Fatal("oversize input admitted")
		}
	}
	stop := make(chan struct{})
	close(stop)
	opens := s.opens
	if c.Verify(c.artifact(e), d, stop) != ErrStopped || c.Verify(c.artifact(e), 1, nil) == nil || s.opens != opens {
		t.Fatal("expired/stopped operation opened source")
	}
}
func stringsContainCanary(err error) bool {
	return err != nil && bytes.Contains([]byte(err.Error()), []byte("private-canary"))
}

func repinArchive(e *Entry, s *memorySource) {
	a := sha256.Sum256(s.archive)
	e.ArchiveSHA256 = hex.EncodeToString(a[:])
	e.OfficialChecksum = e.ArchiveSHA256
	s.sums = []byte(e.OfficialChecksum + "  " + e.ArchiveName + "\n")
	sealSums(e, s)
}

type fileSource struct{ dir string }

func (s fileSource) Open(e Entry, _ infracontract.Deadline, _ infracontract.Stop) (io.ReadCloser, io.ReadCloser, error) {
	archive, err := os.Open(filepath.Join(s.dir, e.ArchiveName))
	if err != nil {
		return nil, nil, err
	}
	sums, err := os.Open(filepath.Join(s.dir, fmt.Sprintf("%d-%s.checksums", e.Kind, e.Version)))
	return archive, sums, err
}
func verifyOfficialArtifacts(t *testing.T) {
	t.Helper()
	directory := os.Getenv("S9_ARTIFACT_DIR")
	if directory == "" {
		t.Log("official content receipts NOT_RUN: S9_ARTIFACT_DIR unset")
		return
	}
	c, err := New(Config{Source: fileSource{directory}})
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range c.entries {
		v, err := c.ReadVerified(c.artifact(e), testDeadline(t), nil)
		if err != nil {
			t.Fatalf("official %s content rejected: %v", e.ArchiveName, err)
		}
		if err := c.VerifyFinal(v.Artifact(), bytes.NewReader(v.Bytes()), testDeadline(t), nil); err != nil {
			t.Fatal(err)
		}
		exerciseOfficialVersion(t, v)
		t.Logf("official %s archive=%s binary=%s checksum_list=%s algorithm=%s", e.ArchiveName, e.ArchiveSHA256, e.BinarySHA256, e.ChecksumSHA256, e.ChecksumAlgorithm)
	}
}
