package infraartifact

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/infracontract"
)

// origin: hot-update S9 plan §3.1 / T03
func TestInfraCatalogPinsOfficialReleases(t *testing.T) {
	c, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	// Independent frozen identities prevent the catalog from signing itself.
	want := []struct {
		kind                                                infracontract.ArtifactKind
		version, archive, binary, sums, algorithm, official string
	}{
		{1, "2.14.5", "5e3b603d47c447bda1f77f9ac16dbf91c90aac4ff3681f8fbbc7201e4ed99355", "e1a2f9ba25077f4cf753bee829483bd68fbf0a4eec9b6645a1e5785a6de0c0d1", "2e842f2670eb0eed2c65f2a58ac3f0c439256d607e9654ebd6999859c18ed0e0", "sha256", "5e3b603d47c447bda1f77f9ac16dbf91c90aac4ff3681f8fbbc7201e4ed99355"},
		{1, "2.14.6", "61c3d55f69f61ec616b75782250936445f2819e9e5f2ae6159b10a31abd2200c", "2d0dc83e0547185a6820377114215d6a7c645f607aeb31728fffaf3d84e77c10", "cc6e36a66bd91aead06d5a1bfcf49ea90354917eb35ae44319b92925279d4523", "sha256", "61c3d55f69f61ec616b75782250936445f2819e9e5f2ae6159b10a31abd2200c"},
		{2, "2.7.5", "13653e34d194d6737268e391c40a72a5d55e9f741e2ebbeebfa675e91a27223a", "f7706787b843d375d1326cc451d53f48900b7a14f3a9c606d49d7751b390609f", "a2667595ce868bfee693413f6b4f1e463cc8d856a99a743b7dbc1b06dae020c8", "sha512", "4afdf50ccf3a8f32344dbac46006ca2b5d90c2ef656c53e8617f1c3b81c5f9e44bd3a9e0b62975f73c85451c354d3d9b5292f5247d18a62d95ab19c8b0a5dba7"},
		{2, "2.7.6", "99587cf77c595f0bf62cc23c9ea101f9879fa016c7b689d498ce054904821f22", "db3bfc85bb93160f60fa6df9c3ebf2340dc11740e9a52c717d88a14c0430f229", "0ca6538623d93747a104ce7c135c7f5261f08c155cd80187c2918d390e14830d", "sha512", "b74311ec8263f30f6d36e5c8be151e8bc092b377789a55300d5671238b9043de5bd6db2bcefae32aa1e6fe94c47bbf02982c44a7871e5777b2596fdb20907cbf"},
	}
	for i, w := range want {
		e := c.entries[i]
		a, err := c.Resolve(w.kind, w.version, "linux", "amd64")
		if err != nil || e.ChecksumSHA256 != w.sums || e.ChecksumAlgorithm != w.algorithm || e.OfficialChecksum != w.official || hex.EncodeToString(a.ArchiveSHA256[:]) != w.archive || hex.EncodeToString(a.BinarySHA256[:]) != w.binary || a.CatalogSHA256 != sha256.Sum256(catalogBytes) {
			t.Fatalf("catalog entry %d changed: %v", i, err)
		}
		if err := validEntry(e); err != nil {
			t.Fatal(err)
		}
		for _, mutate := range []func(*Entry){
			func(e *Entry) { e.ArchiveURL = "https://example.invalid/caller" },
			func(e *Entry) { e.ChecksumURL += "/caller" },
			func(e *Entry) { e.BinaryMember = "../caller" },
			func(e *Entry) { e.ChecksumAlgorithm = "md5" },
			func(e *Entry) { e.Version = "9.9.9" },
			func(e *Entry) { e.BinarySHA256 = strings.Repeat("0", 64) },
		} {
			bad := e
			mutate(&bad)
			if validEntry(bad) == nil {
				t.Fatal("untrusted catalog metadata accepted")
			}
		}
	}
	original := bytes.Clone(catalogBytes)
	defer func() { catalogBytes = original }()
	for _, bad := range [][]byte{
		[]byte("null"), []byte("[]"), append(bytes.Clone(original), []byte(" {}")...),
		bytes.Replace(original, []byte(`"GOARCH": "amd64"`), []byte(`"GOARCH": "arm64"`), 1),
		bytes.Replace(original, []byte(`"Kind": 1`), []byte(`"Kind": 255`), 1),
		bytes.Replace(original, []byte(`"Kind": 1`), []byte(`"unknown": 1, "Kind": 1`), 1),
		func() []byte {
			entries := append([]Entry(nil), c.entries...)
			entries[1] = entries[0]
			b, err := json.Marshal(entries)
			if err != nil {
				t.Fatal(err)
			}
			return b
		}(),
	} {
		catalogBytes = bad
		if _, err := New(Config{}); err == nil {
			t.Fatal("malformed/duplicate catalog accepted")
		}
	}
	catalogBytes = original

	for _, target := range [][2]string{{"darwin", "arm64"}, {"linux", "arm64"}, {"windows", "amd64"}} {
		if _, err := c.Resolve(1, "2.14.6", target[0], target[1]); err != infracontract.ErrUnsupported {
			t.Fatal("unsupported target admitted")
		}
	}
	for _, kind := range []infracontract.ArtifactKind{0, 3, 4, 255} {
		if _, err := c.Resolve(kind, "2.14.6", "linux", "amd64"); err == nil {
			t.Fatal("unknown/support artifact admitted")
		}
	}
	for _, pin := range []struct{ path, token string }{
		{"../../scripts/install.sh", `NATS_SERVER_VERSION="${TETHER_NATS_SERVER_VERSION:-v2.14.6}"`},
		{"../../scripts/install.sh", `CADDY_VERSION="${TETHER_CADDY_VERSION:-2.7.6}"`},
		{"../../Makefile", "NATS_SERVER_VERSION ?= v2.14.6"},
		{"../../go.mod", "github.com/nats-io/nats-server/v2 v2.14.6"},
	} {
		b, err := os.ReadFile(pin.path)
		if err != nil || !strings.Contains(string(b), pin.token) {
			t.Fatalf("catalog drifted from pin in %s", pin.path)
		}
	}
}
