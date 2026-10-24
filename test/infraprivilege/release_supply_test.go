//go:build linux && amd64 && infraprivilege_integration

package infraprivilege

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"debug/buildinfo"
	"encoding/hex"
	"encoding/json"
	"fmt"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/test/infraharness"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// The producer must build this workspace once, retain genuine executable source
// identities, and supply inner race images independently of the outer test.
// origin: S9 internal review INFRA-SUITE-SUPPLIES F4
func TestInfraCurrentReleaseSupplyHasRaceAndSource(t *testing.T) {
	_, file, _, _ := runtime.Caller(0)
	repo := filepath.Clean(filepath.Join(filepath.Dir(file), "../.."))
	root := t.TempDir()
	cmd := exec.CommandContext(t.Context(), "/usr/bin/python3", "-I", "-B", filepath.Join(repo, "test/infraharness/release_supply.py"),
		"current", "--repo", repo, "--output", root)
	var output bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &output
	process, err := infraharness.StartProcess(cmd)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = process.Stop() }()
	if err := process.Wait(); err != nil {
		t.Fatalf("current release producer failed: %v: %s", err, output.Bytes())
	}
	var release struct {
		Version                                 string
		Archive, RaceArchive, Checksums, Source struct{ Path, SHA256 string }
		Binaries                                []struct {
			Name, SHA256 string
			Race         struct{ Path, SHA256 string }
		}
	}
	data, err := os.ReadFile(filepath.Join(root, "release.json"))
	if err != nil || json.Unmarshal(data, &release) != nil || len(release.Binaries) != 3 {
		t.Fatal("current release lacks its complete manifest")
	}
	if filepath.Base(release.Archive.Path) != "tether_"+strings.TrimPrefix(release.Version, "v")+"_linux_amd64.tar.gz" {
		t.Fatal("archive name differs from the actual installer download contract")
	}
	for _, row := range release.Binaries {
		if row.Name != "tether-infra-helper" && row.Name != "tether-infra-launch" && row.Name != "tether" {
			t.Fatal("unexpected current executable")
		}
		digest, err := hex.DecodeString(row.Race.SHA256)
		if err != nil || len(digest) != 32 {
			t.Fatal("invalid race executable hash")
		}
		if err := infraharness.VerifyArtifact(root, filepath.Join(root, row.Race.Path), c.Digest(digest), true); err != nil {
			t.Fatal(err)
		}
		info, err := buildinfo.ReadFile(filepath.Join(root, row.Race.Path))
		if err != nil || info.Path != "github.com/LinZiyang666/tether/cmd/"+row.Name {
			t.Fatal("race binary is not the real product entry")
		}
	}
	for _, row := range []struct{ Path, SHA256 string }{release.Archive, release.RaceArchive, release.Checksums, release.Source} {
		data, err := os.ReadFile(filepath.Join(root, row.Path))
		if err != nil || fmt.Sprintf("%x", sha256.Sum256(data)) != row.SHA256 {
			t.Fatal("current archive/source custody is not bound")
		}
	}
	var source struct {
		Binaries []json.RawMessage `json:"binaries"`
		Race     []struct {
			Name   string                          `json:"component"`
			SHA    string                          `json:"source_sha256"`
			Inputs []struct{ Path, SHA256 string } `json:"source_inputs"`
		} `json:"race_binaries"`
	}
	sourceBytes, err := os.ReadFile(filepath.Join(root, release.Source.Path))
	if err != nil || json.Unmarshal(sourceBytes, &source) != nil || len(source.Binaries) != 2 || len(source.Race) != 3 {
		t.Fatal("source manifest omitted a genuine executable closure")
	}
	for _, record := range source.Race {
		found := false
		for _, input := range record.Inputs {
			if input.Path == "cmd/"+record.Name+"/main.go" {
				found = true
			}
			data, err := os.ReadFile(filepath.Join(repo, input.Path))
			if err != nil || fmt.Sprintf("%x", sha256.Sum256(data)) != input.SHA256 {
				t.Fatal("source manifest does not describe this workspace")
			}
		}
		if !found || len(record.SHA) != 64 {
			t.Fatal("source manifest lacks its entry or source digest")
		}
	}
	for _, race := range []bool{false, true} {
		archive := release.Archive
		if race {
			archive = release.RaceArchive
		}
		f, err := os.Open(filepath.Join(root, archive.Path))
		if err != nil {
			t.Fatal(err)
		}
		z, err := gzip.NewReader(f)
		if err != nil {
			_ = f.Close()
			t.Fatal(err)
		}
		reader, seen := tar.NewReader(z), make(map[string]bool)
		for {
			h, err := reader.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			if h.Typeflag != tar.TypeReg || seen[h.Name] {
				t.Fatal("archive has a duplicate or nonregular member")
			}
			seen[h.Name] = true
			data, err := io.ReadAll(reader)
			if err != nil {
				t.Fatal(err)
			}
			for _, binary := range release.Binaries {
				if h.Name != binary.Name {
					continue
				}
				expected := binary.SHA256
				if race {
					expected = binary.Race.SHA256
				}
				if h.Mode != 0755 || fmt.Sprintf("%x", sha256.Sum256(data)) != expected {
					t.Fatal("archive executable differs from manifest")
				}
			}
			if h.Name == "infra-support.manifest" && !strings.Contains(string(data), "release "+release.Version+"\n") {
				t.Fatal("archive support manifest lost release binding")
			}
		}
		if err := z.Close(); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		if len(seen) != 5 || !seen["infra-catalog.json"] || !seen["infra-support.manifest"] || !seen["tether"] || !seen["tether-infra-helper"] || !seen["tether-infra-launch"] {
			t.Fatal("release archive omitted an input")
		}
	}

}
