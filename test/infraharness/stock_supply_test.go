package infraharness

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/infraartifact"
	c "github.com/LinZiyang666/tether/internal/infracontract"
)

// origin: hot-update S9 plan §9.1 / §12 P26; S9 INFRA-SUITE-SUPPLIES
func TestInfraStockSupplyKeepsExplicitInputAuthority(t *testing.T) {
	repo := t.TempDir()
	t.Setenv("S9_ARTIFACT_DIR", "")
	if got := StockSource(repo, ""); got != filepath.Join(repo, "test/simcluster/vendor/infra") {
		t.Fatalf("unstaged suite did not select its own workspace: %q", got)
	}
	t.Setenv("S9_ARTIFACT_DIR", "/shared-official-inputs")
	if got := StockSource(repo, ""); got != "/shared-official-inputs" {
		t.Fatalf("shared official inputs were ignored: %q", got)
	}
	if got := StockSource(repo, "/broken-explicit-inputs"); got != "/broken-explicit-inputs" {
		t.Fatalf("explicit broken supply fell back to unrelated bytes: %q", got)
	}
}

// These bytes only test the offline Source interface. They are deliberately
// invalid official artifacts and must never pass PrepareStockArtifact below.
// origin: hot-update S9 plan §3.1 / §9.1 / T55 / M28
func TestInfraStockSupplyRejectsUnverifiedInputs(t *testing.T) {
	d, err := controlbudget.After(time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	source, root := t.TempDir(), t.TempDir()
	e := infraartifact.Entry{Kind: c.NATSBinary, Version: "2.14.5", ArchiveName: "official.tar.gz", ChecksumURL: "https://invalid.test/SHA256SUMS"}
	write := func(name, body string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(source, name), []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	write(e.ArchiveName, "unverified archive")
	write("nats-server-2.14.5-checksums.txt", "previous list")
	write("1-2.14.5.checksums", "P01 list")
	write("SHA256SUMS", "current list")
	read := func(want string) {
		t.Helper()
		a, sums, err := StockArchiveSource(source).Open(e, d, nil)
		if a != nil {
			defer func() { _ = a.Close() }()
		}
		if sums != nil {
			defer func() { _ = sums.Close() }()
		}
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(sums)
		if err != nil || string(body) != want {
			t.Fatalf("version-specific checksum selection: %q %v", body, err)
		}
	}
	read("previous list")
	if err := os.Remove(filepath.Join(source, "nats-server-2.14.5-checksums.txt")); err != nil {
		t.Fatal(err)
	}
	read("P01 list")
	if err := os.Remove(filepath.Join(source, "1-2.14.5.checksums")); err != nil {
		t.Fatal(err)
	}
	read("current list")
	// A corrupt preferred file cannot be rescued by a valid-looking alternative.
	if err := os.Symlink(filepath.Join(source, "SHA256SUMS"), filepath.Join(source, "nats-server-2.14.5-checksums.txt")); err != nil {
		t.Fatal(err)
	}
	a, sums, err := StockArchiveSource(source).Open(e, d, nil)
	if a != nil {
		_ = a.Close()
	}
	if sums != nil {
		_ = sums.Close()
	}
	if err == nil {
		t.Fatal("nonregular preferred supply silently fell back")
	}
	stop := make(chan struct{})
	close(stop)
	a, sums, err = StockArchiveSource(source).Open(e, d, stop)
	if a != nil {
		_ = a.Close()
	}
	if sums != nil {
		_ = sums.Close()
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled supply opened inputs: %v", err)
	}
	// The real immutable catalog must reject these fabricated archive bytes and
	// publish neither an executable nor a successful private input manifest.
	if err := os.Remove(filepath.Join(source, "nats-server-2.14.5-checksums.txt")); err != nil {
		t.Fatal(err)
	}
	write("nats-server-2.14.5-checksums.txt", "unverified checksums")
	write("nats-server-v2.14.5-linux-amd64.tar.gz", "unverified archive")
	if _, err := PrepareStockArtifact(root, source, c.NATSBinary, "2.14.5", d, nil); err == nil {
		t.Fatal("unverified bytes became executable supply")
	}
	files, err := os.ReadDir(root)
	if err != nil || len(files) != 0 {
		t.Fatal("failed verification published fixture state")
	}
	for _, path := range []string{"/", os.TempDir(), "relative"} {
		if _, err := PrepareStockArtifact(path, source, c.NATSBinary, "2.14.5", d, nil); err == nil {
			t.Fatalf("unsafe output root accepted: %q", path)
		}
	}
}
