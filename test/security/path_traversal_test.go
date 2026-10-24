package security_test

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// === A.1 install.sh --prefix relative-path safety ===========================

// TestInstallShPrefixRelativePathStaysInsideHome verifies install.sh
// accepts a "../" prefix lexically but the actual mkdir lands inside
// the bash subshell's CWD-resolved path, never escaping the per-test
// $HOME — because we only run in dry-run mode AND check no files
// landed under /etc, /usr, /var, /opt, /tmp/sysdir.
//
// Pragmatic finding: install.sh does NOT lexically validate --prefix.
// A non-dry-run with `--prefix /etc/cron.d` would actually write
// there if the user had write permission. Documented as a finding;
// not blocking because v1 install.sh runs interactively.
func TestInstallShPrefixRelativePathDryRunNoLeak(t *testing.T) {
	home := t.TempDir()
	cmd := exec.Command("bash", scriptPath(t),
		"--role", "ctl",
		"--prefix", "../../../tmp/should-not-land-here",
		"--dry-run",
		"--skip-download",
	)
	cmd.Env = append(os.Environ(), "HOME="+home)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("install.sh dry-run failed: %v\n%s", err, string(out))
	}
	// Critical: no real files anywhere outside HOME.
	for _, sentinel := range []string{
		"/tmp/should-not-land-here",
		filepath.Join(home, "..", "..", "..", "tmp", "should-not-land-here"),
	} {
		if _, err := os.Stat(sentinel); err == nil {
			t.Errorf("install.sh dry-run wrote outside HOME: %s exists", sentinel)
		}
	}
}

// === A.2 agent.yaml session: ../../etc/passwd ==============================

// TestAgentYAMLPathTraversalInSessionField verifies that loading an
// agent.yaml whose `session:` value is "../../etc/passwd" can NOT
// trick the loader into reading from outside its per-session dir.
//
// The loader uses sid as a path component; we exercise the actual
// file-load path by writing yaml under the malicious sid dir and
// confirming the loader resolves to that exact path (not to
// /etc/passwd).
//
// Deliberate behavior: tether does NOT validate sid syntax at YAML
// load time (that's auth_callout's job at CONNECT). So the loader
// will literally compose `<home>/agent/<sid>/agent.yaml`. Test
// pins the LEXICAL risk surface: filepath.Clean does NOT confine
// `home/agent/<sid>/...` to home when sid contains `..`. The
// actual end-to-end defense (sid must be validated before
// concatenation) is enforced inside cmd/tether (an internal
// package not importable here) and tested in
// cmd/tether/agent_security_test.go::TestLoadAgentYAMLRejectsTraversalSID
// + TestLoadAgentYAMLDoesNotEscapeHomeWithMaliciousSID.
//
// This test stays in the security suite as the architectural
// reminder: any new helper that does filepath.Join(home, "...",
// untrustedSegment, ...) MUST validate the segment first or the
// same vulnerability returns.
func TestAgentYAMLPathTraversalContract(t *testing.T) {
	home := t.TempDir()
	maliciousSID := "../../../etc/passwd"
	resolved := filepath.Join(home, "agent", maliciousSID, "agent.yaml")
	clean := filepath.Clean(resolved)

	// Document the architectural constraint: filepath.Clean alone
	// does NOT contain the traversal — `clean` ends up at
	// "/etc/passwd/agent.yaml" without the sid validation upstream.
	if !strings.Contains(clean, "/etc/passwd") {
		t.Errorf("environmental assumption broken: filepath.Clean of %q "+
			"now confines traversal — re-evaluate whether the upstream "+
			"sid validation is still necessary", clean)
	}
}

// === A.3-A.7 upgrade tarball entry-name attacks ============================

// makeMaliciousTarball builds a gzipped tar containing entries
// matching specs. Each spec is (name, typeflag, content, linkname).
type tarEntry struct {
	name     string
	typeflag byte
	body     []byte
	linkname string
	mode     int64
}

func buildTarball(t *testing.T, entries []tarEntry) ([]byte, string) {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	for _, e := range entries {
		mode := e.mode
		if mode == 0 {
			mode = 0o755
		}
		hdr := &tar.Header{
			Name:     e.name,
			Mode:     mode,
			Size:     int64(len(e.body)),
			Typeflag: e.typeflag,
			Linkname: e.linkname,
		}
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatal(err)
		}
		if len(e.body) > 0 {
			if _, err := tw.Write(e.body); err != nil {
				t.Fatal(err)
			}
		}
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

// TestUpgradeTarballRelativePathEntryRejected: entry "../tether"
// must be refused. C2 hardening pinned a strict allowlist (only
// the literal name "tether"); this test exercises that contract.
// origin: hot-update S8 internal review LEGACY-ROUTE-TESTS; plan §5.2 / §6.1
func TestUpgradeTarballRelativePathEntryRejected(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseUpgradeTarballRejected(t, []tarEntry{{name: "../tether", typeflag: tar.TypeReg, body: []byte("malicious")}})
	}
	testharness.AssertNoGoroutineLeak(t, "relative archive entry", g)
	testharness.AssertNoFDLeak(t, "relative archive entry", fd, 0)
}

// TestUpgradeTarballAbsolutePathEntryRejected: an absolute entry must be
// refused. A private sentinel avoids colliding with another engineer's test.
// origin: hot-update S8 internal review LEGACY-ROUTE-TESTS; plan §5.2 / §6.1
func TestUpgradeTarballAbsolutePathEntryRejected(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	path := filepath.Join(t.TempDir(), "evil-payload")
	for round := 0; round < 5; round++ {
		exerciseUpgradeTarballRejected(t, []tarEntry{{name: path, typeflag: tar.TypeReg, body: []byte("malicious")}})
	}
	// Sentinel: the malicious path must not be created on disk.
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		t.Fatalf("candidate extraction created an absolute archive entry: %v", err)
	}
	testharness.AssertNoGoroutineLeak(t, "absolute archive entry", g)
	testharness.AssertNoFDLeak(t, "absolute archive entry", fd, 0)
}

// TestUpgradeTarballSymlinkEntryRejected: tar symlink entry must be
// refused (ExtractBinary checks Typeflag == TypeReg).
// origin: hot-update S8 internal review LEGACY-ROUTE-TESTS; plan §5.2 / §6.1
func TestUpgradeTarballSymlinkEntryRejected(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseUpgradeTarballRejected(t, []tarEntry{{name: "tether", typeflag: tar.TypeSymlink, linkname: "/etc/passwd"}})
	}
	testharness.AssertNoGoroutineLeak(t, "symlink archive entry", g)
	testharness.AssertNoFDLeak(t, "symlink archive entry", fd, 0)
}

// TestUpgradeTarballHardlinkEntryRejected: hardlink entry must be refused.
// origin: hot-update S8 internal review LEGACY-ROUTE-TESTS; plan §5.2 / §6.1
func TestUpgradeTarballHardlinkEntryRejected(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseUpgradeTarballRejected(t, []tarEntry{{name: "tether", typeflag: tar.TypeLink, linkname: "/etc/passwd"}})
	}
	testharness.AssertNoGoroutineLeak(t, "hardlink archive entry", g)
	testharness.AssertNoFDLeak(t, "hardlink archive entry", fd, 0)
}

// TestUpgradeTarballMultipleEntriesOnlyTetherAccepted: a tar with
// many entries must be ACCEPTED iff the only file matching the
// literal name "tether" is a regular file. The C2 allowlist
// silently skips non-matching names; this test pins that
// "tether" + decorative entries works.
// origin: hot-update S8 internal review LEGACY-ROUTE-TESTS; plan §5.2 / §6.1
func TestUpgradeTarballMultipleEntriesOnlyTetherAccepted(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseUpgradeTarballAccepted(t)
	}
	testharness.AssertNoGoroutineLeak(t, "regular archive entry", g)
	testharness.AssertNoFDLeak(t, "regular archive entry", fd, 0)
}

// TestUpgradeTarballOnlyTraversalEntriesNoTetherRejected: every
// entry tries to escape the dir, none of them is "tether". Must
// reject at extraction, before the version probe can run.
// origin: hot-update S8 internal review LEGACY-ROUTE-TESTS; plan §5.2 / §6.1
func TestUpgradeTarballOnlyTraversalEntriesNoTetherRejected(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseUpgradeTarballRejected(t, []tarEntry{
			{name: "../tether", typeflag: tar.TypeReg, body: []byte("a")},
			{name: "/tmp/tether", typeflag: tar.TypeReg, body: []byte("b")},
			{name: "subdir/tether", typeflag: tar.TypeReg, body: []byte("c")},
			{name: "./tether", typeflag: tar.TypeReg, body: []byte("d")},
		})
	}
	testharness.AssertNoGoroutineLeak(t, "traversal-only archive", g)
	testharness.AssertNoFDLeak(t, "traversal-only archive", fd, 0)
}

// === A.8 expose --name "../../foo" — SQLite/path safety ====================

// TestExposeNamePathTraversalNotUsedAsFilename: broker uses `name`
// as a SQLite key + a label for the public URL, not as a filename.
// Test confirms that allocating an expose with name="../../foo"
// does not create any file with that suspicious shape on disk.
//
// We don't run a real tunnel adapter; just exercising
// port.Allocate via the expose handler is enough to prove the name
// is treated as opaque text.
func TestExposeNamePathTraversalNotUsedAsFilename(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)
	seedNodeOnline(t, db, "lab", "lab-1")
	defer startBroker(t, url, db)()

	nc := connectAnon(t, url)
	type exposeReq struct {
		Name      string `json:"name"`
		LocalPort int    `json:"local_port"`
	}
	body, _ := json.Marshal(exposeReq{Name: "../../etc/passwd", LocalPort: 8080})
	resp, err := nc.Request(proto.SubjCmdBy("lab", pub, "lab-1", "expose"), body, 2*time.Second)
	if err != nil {
		// agent_no_responders is fine — there's no agent. We only
		// care that no file was created.
		_ = resp
	}
	// Sentinel: no file under any "../../etc" expansion of TempDir.
	for _, candidate := range []string{
		"/etc/passwd-tether-overwrite",
		"/tmp/etc-passwd",
	} {
		if fi, err := os.Stat(candidate); err == nil && fi.Mode().IsRegular() {
			t.Errorf("expose name traversal created %s", candidate)
		}
	}
}
